package follow

import (
	"context"
	"fmt"
	"time"

	"github.com/ipfs/go-cid"

	"github.com/blobarchive/bloar/metrics"
	"github.com/blobarchive/bloar/schema"
	"github.com/blobarchive/bloar/server"
)

// maxManifestWalk bounds the manifest-ancestry walk (spec 11.3). A head's chain
// grows one link per operator upgrade, which is rare, so a legitimate walk is a
// handful of hops and reaches its floor almost at once. The bound is here for the
// other case: a writer who replaced the chain publishes a tip whose lineage never
// contains the floor, and the walk would otherwise follow the replacement all the
// way to its own genesis over the network. A chain this long that still does not
// contain the floor is refused as unprovable rather than chased further.
const maxManifestWalk = 4096

// First adoption has no retained manifest floor to stop a recursive retention
// walk at. These limits turn that otherwise-open boundary into a finite
// admission proof. A Manifest is ordinarily a few hundred bytes and a source
// schedule changes rarely; the byte ceilings leave orders of magnitude of
// headroom without allowing a signed first tip to consume unbounded network,
// decoder memory, or cached disk before it is checkpointed.
const (
	maxFirstManifestBlocks     = maxManifestWalk
	maxManifestBlockBytes      = 1 << 20
	maxFirstManifestChainBytes = 16 << 20
	maxFirstManifestWalkTime   = docTimeout
)

type manifestAdmissionLimits struct {
	maxHops       int
	maxBlocks     int
	maxBlockBytes int64
	maxChainBytes int64
	maxDuration   time.Duration
}

func firstManifestAdmissionLimits() manifestAdmissionLimits {
	return manifestAdmissionLimits{
		maxHops:       maxManifestWalk,
		maxBlocks:     maxFirstManifestBlocks,
		maxBlockBytes: maxManifestBlockBytes,
		maxChainBytes: maxFirstManifestChainBytes,
		maxDuration:   maxFirstManifestWalkTime,
	}
}

type manifestBlockLoader func(context.Context, cid.Cid) ([]byte, error)

// parseManifestTip reads the manifest tip a document entry carries (spec 8,
// 10.5): the CID in its manifest field, or cid.Undef when the field is absent (a
// head with no chain). A present-but-unparseable tip is a malformed document.
func parseManifestTip(e server.HeadEntry) (cid.Cid, error) {
	if e.Manifest == "" {
		return cid.Undef, nil
	}
	tip, err := cid.Decode(e.Manifest)
	if err != nil {
		return cid.Undef, fmt.Errorf("follow: head %q has an undecodable manifest tip %q: %w", e.Name, e.Manifest, err)
	}
	return tip, nil
}

// checkManifestAncestry enforces the manifest-ancestry floor of spec 11.3: a
// newly published tip is accepted only if the floor this node already holds
// appears in its prev-lineage. It is a pure hash-chain walk -- follow prev links
// from newTip by the same generic traversal that fetches and pins the chain,
// never decoding a manifest (spec 15) -- fetching blocks it does not hold as it
// goes.
//
// floor is the tip this node has already accepted -- from the head's checkpoint,
// or the retained legacy floor before its first one -- and hasFloor is false for a
// head that has never accepted a tip. The cases, in the order they are cheap to
// decide:
//
//   - no floor yet: the head has never had a tip, so there is nothing to descend
//     from. Before a first tip can become the floor, its complete chain is
//     schema-validated to genesis under finite hop/block/byte/time budgets. This
//     is the boundary which prevents the later recursive retention pin from
//     walking an arbitrary DAG.
//   - equal tip: the document republished the same tip, common when only the root
//     moved. No walk.
//   - floor held but the document dropped the chain (newTip undefined): a head
//     cannot un-attest its filter, so this is a regression. Refused.
//   - a different tip: walk prev from it and require the floor to appear. If the
//     walk reaches genesis, or the bound, without meeting the floor, the chain was
//     rewritten and the tip is refused.
//
// A refusal returns an error and counts a FollowRefusal, blocking the adoption the
// caller would otherwise make; the head goes on serving its last good state, the
// same as a synced_to regression (spec 11.3). A fetch failure mid-walk is a
// transient error, not a refusal: the writer may be reachable next poll.
func (f *Follower) checkManifestAncestry(ctx context.Context, head string, newTip, floor cid.Cid, hasFloor bool) error {
	if !hasFloor {
		if !newTip.Defined() {
			return nil // no floor and no chain: nothing to validate.
		}
		err := validateFirstManifestChain(ctx, head, newTip, firstManifestAdmissionLimits(),
			func(ctx context.Context, c cid.Cid) ([]byte, error) {
				if c.Prefix().Codec != cid.DagCBOR {
					return nil, fmt.Errorf("manifest tip %s is not a dag-cbor block", c)
				}
				blk, err := f.blocks.Get(ctx, c)
				if err != nil {
					return nil, err
				}
				return blk.RawData(), nil
			})
		if err != nil {
			return fmt.Errorf("follow: validating the first manifest chain of head %q from %s: %w", head, newTip, err)
		}
		return nil
	}
	if newTip == floor {
		return nil // unchanged tip: no walk.
	}
	if !newTip.Defined() {
		f.cfg.Metrics.FollowRefusal(metrics.RefusalManifestAncestry)
		return fmt.Errorf("follow: head %q dropped its manifest chain (floor %s); a head cannot un-attest its filter "+
			"(spec 10.5), refusing to regress", head, floor)
	}

	c := newTip
	for hops := 0; hops < maxManifestWalk; hops++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		if c == floor {
			return nil // the floor is an ancestor of the new tip: a legal extension.
		}
		prev, ok, err := f.manifestPrev(ctx, c)
		if err != nil {
			// A block the walk could not fetch. Transient: not a refusal, and the
			// floor stands, so the next poll tries again.
			return fmt.Errorf("follow: walking the manifest chain of head %q from %s: %w", head, newTip, err)
		}
		if !ok {
			break // reached genesis (prev absent) without meeting the floor.
		}
		c = prev
	}

	f.cfg.Metrics.FollowRefusal(metrics.RefusalManifestAncestry)
	return fmt.Errorf("follow: head %q published manifest tip %s does not descend from the accepted tip %s; "+
		"refusing a rewritten filter history (spec 10.5, 11.3)", head, newTip, floor)
}

// validateFirstManifestChain proves that tip is a bounded, linear chain of
// manifests for head, ending at a genesis Manifest. Unlike later ancestry
// checks, first adoption cannot rely on a previously validated floor: every
// block it would make reachable from the first recursive manifest pin is
// therefore decoded here and only the schema's Prev field is followed.
//
// The aggregate encoded-byte ceiling conservatively bounds decoder input and
// the bytes a completely cold follower can fetch and cache. Seen CIDs make the
// unique-block bound explicit and reject cycles independently of the hop limit.
// The caller performs this proof in preflight, before retention Prepare,
// checkpoint, mirror, or reconciler mutation.
func validateFirstManifestChain(ctx context.Context, head string, tip cid.Cid, limits manifestAdmissionLimits,
	load manifestBlockLoader) error {
	if limits.maxHops < 1 || limits.maxBlocks < 1 || limits.maxBlockBytes < 1 ||
		limits.maxChainBytes < 1 || limits.maxDuration <= 0 {
		return fmt.Errorf("invalid first-manifest admission limits")
	}

	walkCtx, cancel := context.WithTimeout(ctx, limits.maxDuration)
	defer cancel()

	seen := make(map[string]struct{}, min(limits.maxBlocks, limits.maxHops))
	var totalBytes int64
	current := tip
	for hops := 0; hops < limits.maxHops; hops++ {
		if err := walkCtx.Err(); err != nil {
			return fmt.Errorf("manifest walk exceeded its %s wall-time budget: %w", limits.maxDuration, err)
		}
		if current.Prefix().Codec != cid.DagCBOR {
			return fmt.Errorf("manifest %s is not a dag-cbor block", current)
		}
		key := current.KeyString()
		if _, duplicate := seen[key]; duplicate {
			return fmt.Errorf("manifest chain contains a cycle at %s", current)
		}
		if len(seen) >= limits.maxBlocks {
			return fmt.Errorf("manifest chain exceeds %d unique blocks", limits.maxBlocks)
		}
		seen[key] = struct{}{}

		raw, err := load(walkCtx, current)
		if err != nil {
			return fmt.Errorf("reading manifest %s: %w", current, err)
		}
		blockBytes := int64(len(raw))
		if blockBytes > limits.maxBlockBytes {
			return fmt.Errorf("manifest %s is %d bytes, exceeds the %d-byte per-block limit",
				current, blockBytes, limits.maxBlockBytes)
		}
		if blockBytes > limits.maxChainBytes-totalBytes {
			return fmt.Errorf("manifest chain exceeds the %d-byte aggregate limit", limits.maxChainBytes)
		}
		totalBytes += blockBytes

		manifest, err := schema.DecodeManifest(raw)
		if err != nil {
			return fmt.Errorf("decoding manifest %s: %w", current, err)
		}
		if manifest.Head != head {
			return fmt.Errorf("manifest %s names head %q, want %q", current, manifest.Head, head)
		}
		if !manifest.Prev.Defined() {
			return nil
		}
		current = manifest.Prev
	}
	return fmt.Errorf("manifest chain from %s exceeds %d hops without reaching genesis", tip, limits.maxHops)
}

// manifestPrev returns the prev link of the manifest at c, fetching the block if
// this node does not hold it. It reads links generically -- a manifest's only
// link is its prev (spec 2, 10.5) -- so it never decodes the manifest and an
// unknown manifest version is not its concern (spec 15). ok is false for a
// genesis manifest, which has no prev.
func (f *Follower) manifestPrev(ctx context.Context, c cid.Cid) (cid.Cid, bool, error) {
	if c.Prefix().Codec != cid.DagCBOR {
		return cid.Undef, false, fmt.Errorf("manifest tip %s is not a dag-cbor block", c)
	}
	blk, err := f.blocks.Get(ctx, c)
	if err != nil {
		return cid.Undef, false, err
	}
	kids, err := links(blk.RawData(), c)
	if err != nil {
		return cid.Undef, false, err
	}
	switch len(kids) {
	case 0:
		return cid.Undef, false, nil // genesis: no prev.
	case 1:
		return kids[0], true, nil
	default:
		// A manifest carries exactly one link, prev (spec 10.5). More than one is a
		// block shaped like nothing the schema describes.
		return cid.Undef, false, fmt.Errorf("manifest %s carries %d links, want at most 1 (prev)", c, len(kids))
	}
}

// cidOrNone renders a CID for a log line, or "none" for the undefined one.
func cidOrNone(c cid.Cid) string {
	if !c.Defined() {
		return "none"
	}
	return c.String()
}
