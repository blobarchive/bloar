package follow

import (
	"context"
	"errors"
	"fmt"

	"github.com/ipfs/boxo/blockstore"
	blocksformat "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	format "github.com/ipfs/go-ipld-format"

	"github.com/blobarchive/bloar/archive"
	"github.com/blobarchive/bloar/server"
)

// ClaimRelation is the partial-order relationship between two independently
// signed claims about one finalized head. The order is intentionally not a
// freshness order: signer-local revisions, updated_at, and source iteration
// order have no meaning across authorities.
type ClaimRelation uint8

const (
	// ClaimRelationInvalid is the zero value and is never returned by
	// ClassifyFinalizedClaims.
	ClaimRelationInvalid ClaimRelation = iota
	// ClaimsEquivalent means both claims commit to the same coverage and filter
	// history.
	ClaimsEquivalent
	// LeftClaimDominates means the left claim is a proven append-only advance of
	// the right claim (or has the same root and a descendant manifest).
	LeftClaimDominates
	// RightClaimDominates is LeftClaimDominates with the operands reversed.
	RightClaimDominates
	// ClaimsIncomparable means neither claim is proven to include the other, but
	// there is not cryptographic evidence of conflicting history. A caller must
	// hold its last good generation rather than choosing either claim.
	ClaimsIncomparable
)

func (r ClaimRelation) String() string {
	switch r {
	case ClaimsEquivalent:
		return "equivalent"
	case LeftClaimDominates:
		return "left-dominates"
	case RightClaimDominates:
		return "right-dominates"
	case ClaimsIncomparable:
		return "incomparable"
	default:
		return "invalid"
	}
}

// ConflictReason is the closed set of cryptographic contradictions which can
// durably latch a finalized head. It deliberately excludes ordinary proof
// unavailability and partial-order incomparability: neither proves that two
// authorized writers committed incompatible history.
type ConflictReason uint8

const (
	ConflictReasonInvalid ConflictReason = iota
	ConflictReasonEqualCoverageRootMismatch
	ConflictReasonPrefixProjectionMismatch
	ConflictReasonManifestBranch
)

func (r ConflictReason) String() string {
	switch r {
	case ConflictReasonEqualCoverageRootMismatch:
		return "equal_coverage_root_mismatch"
	case ConflictReasonPrefixProjectionMismatch:
		return "prefix_projection_mismatch"
	case ConflictReasonManifestBranch:
		return "manifest_branch"
	default:
		return "invalid"
	}
}

// ArchiveConflictError is cryptographic evidence that two validly signed
// authorities assigned incompatible content histories to the same logical
// archive head. It is deliberately distinct from authorityEquivocationError:
// this is a disagreement between keys, not one key assigning two claims to one
// revision. A multi-writer arbiter holds its last good generation on this error;
// it must not route it through the mutable-head quarantine path.
type ArchiveConflictError struct {
	ArchiveID server.ArchiveID
	Head      string

	LeftRoot  cid.Cid
	RightRoot cid.Cid

	LeftSyncedTo  uint64
	RightSyncedTo uint64
	LeftCovered   bool
	RightCovered  bool

	LeftManifest  cid.Cid
	RightManifest cid.Cid

	ReasonCode ConflictReason
	Reason     string
}

// claimEvidenceError attributes an ordinary proof failure to the claim whose
// content-addressed evidence could not be evaluated. It is deliberately not an
// ArchiveConflictError: an unavailable or malformed block proves nothing about
// disagreement. Multi-source arbitration may omit the affected observation for
// this poll while still considering independently proven claims. The side bits
// refer to the left/right arguments of ClassifyFinalizedClaims.
type claimEvidenceError struct {
	left  bool
	right bool
	err   error
}

func (e *claimEvidenceError) Error() string { return e.err.Error() }
func (e *claimEvidenceError) Unwrap() error { return e.err }

func attributedClaimEvidence(err error, left, right bool) error {
	if err == nil {
		return nil
	}
	return &claimEvidenceError{left: left, right: right, err: err}
}

func (e *ArchiveConflictError) Error() string {
	return fmt.Sprintf("follow: logical archive %s head %q has conflicting writer claims: %s "+
		"(left root=%s synced_to=%s manifest=%s; right root=%s synced_to=%s manifest=%s)",
		e.ArchiveID, e.Head, e.Reason,
		e.LeftRoot, coverageString(e.LeftSyncedTo, e.LeftCovered), cidOrNone(e.LeftManifest),
		e.RightRoot, coverageString(e.RightSyncedTo, e.RightCovered), cidOrNone(e.RightManifest))
}

// ClassifyFinalizedClaims proves the partial-order relationship between the
// named head in two independently signed publication documents.
//
// Both documents must use the logical-archive (v3) contract. Their signatures,
// contracts, root CIDs, and the root blocks' publication fields are validated
// before any comparison. Signature verification proves only that each document
// was signed by its self-declared key; the caller must authenticate both keys
// against its local source policy before calling this function. archive_id never
// grants that authority. Claims with different archive identities, networks, or
// immutable head parameters are unrelated and therefore incomparable.
// Mutable heads are refused: honest optimistic writers may disagree without
// either one equivocating, so the initial multi-authority contract is finalized
// only.
//
// Unequal coverage is ordered only after projecting the higher root to the
// lower synced_to and obtaining the exact lower root. Projection confines its
// deterministic, content-addressed output to a private in-memory overlay; it
// never writes the caller's blockstore or changes a serving head. Missing root
// or manifest blocks and other I/O failures are ordinary transient errors,
// never ArchiveConflictError. A present manifest on only one side is
// conservatively incomparable until an explicit migration contract exists.
func ClassifyFinalizedClaims(ctx context.Context, blocks blockstore.Blockstore, head string, left, right server.Doc) (ClaimRelation, error) {
	if blocks == nil {
		return ClaimRelationInvalid, errors.New("follow: classifying finalized claims with a nil blockstore")
	}
	if head == "" {
		return ClaimRelationInvalid, errors.New("follow: classifying finalized claims for an empty head name")
	}

	l, err := loadFinalizedClaim(ctx, blocks, head, left)
	if err != nil {
		return ClaimRelationInvalid, attributedClaimEvidence(
			fmt.Errorf("follow: validating left finalized claim: %w", err), true, false)
	}
	r, err := loadFinalizedClaim(ctx, blocks, head, right)
	if err != nil {
		return ClaimRelationInvalid, attributedClaimEvidence(
			fmt.Errorf("follow: validating right finalized claim: %w", err), false, true)
	}

	if !sameClaimIdentity(l, r) {
		return ClaimsIncomparable, nil
	}

	rootRelation, err := compareClaimRoots(ctx, blocks, l, r)
	if err != nil {
		return ClaimRelationInvalid, err
	}
	manifestRelation, err := compareClaimManifests(ctx, blocks, l, r)
	if err != nil {
		return ClaimRelationInvalid, err
	}

	switch {
	case rootRelation == ClaimsEquivalent:
		// Equal coverage and root can still have a later, compatible filter
		// schedule. In that case the descendant manifest is the stronger claim.
		return manifestRelation, nil
	case manifestRelation == ClaimsEquivalent:
		return rootRelation, nil
	case rootRelation == manifestRelation:
		return rootRelation, nil
	default:
		// One writer has more archive coverage while the other has a later
		// manifest. Neither includes all of the other's claim.
		return ClaimsIncomparable, nil
	}
}

type finalizedClaim struct {
	archiveID server.ArchiveID
	net       string
	entry     server.HeadEntry
	root      cid.Cid
	manifest  cid.Cid
}

func loadFinalizedClaim(ctx context.Context, blocks blockstore.Blockstore, name string, doc server.Doc) (*finalizedClaim, error) {
	claim, _, err := loadFinalizedClaimWithHead(ctx, blocks, name, doc, func(ctx context.Context, root cid.Cid) (*archive.Head, error) {
		return archive.Load(ctx, archive.Config{Blocks: blocks}, root)
	})
	return claim, err
}

// loadFinalizedClaimWithHead keeps the document/Head contract checks in one
// place while allowing the source-set admission path to supply its stricter
// bounded-DAG loader. Ordinary comparison callers retain the cheap Head-only
// load above; a live source-set caller supplies Follower.loadWithPointer, which
// enumerates the complete canonical index before the claim may become
// arbitration or continuity evidence.
func loadFinalizedClaimWithHead(
	ctx context.Context,
	blocks blockstore.Blockstore,
	name string,
	doc server.Doc,
	load func(context.Context, cid.Cid) (*archive.Head, error),
) (*finalizedClaim, *archive.Head, error) {
	if load == nil {
		return nil, nil, errors.New("follow: finalized claim loader is nil")
	}
	if err := doc.Verify(); err != nil {
		return nil, nil, fmt.Errorf("document signature: %w", err)
	}
	if err := doc.ValidateContract(); err != nil {
		return nil, nil, fmt.Errorf("document contract: %w", err)
	}
	if doc.V != server.LogicalArchiveDocVersion || doc.ArchiveID == nil {
		return nil, nil, fmt.Errorf("document version %d has no logical archive identity; multi-writer comparison requires version %d",
			doc.V, server.LogicalArchiveDocVersion)
	}

	var entry *server.HeadEntry
	for i := range doc.Heads {
		if doc.Heads[i].Name == name {
			entry = &doc.Heads[i]
			break
		}
	}
	if entry == nil {
		return nil, nil, fmt.Errorf("document does not publish head %q", name)
	}
	if entry.EffectiveKind() != server.FinalizedMonotonic {
		return nil, nil, fmt.Errorf("head %q is %s; multi-writer comparison supports finalized-monotonic heads only",
			name, entry.EffectiveKind())
	}

	root, err := cid.Decode(entry.Root)
	if err != nil {
		return nil, nil, fmt.Errorf("head %q has an undecodable root %q: %w", name, entry.Root, err)
	}
	loaded, err := load(ctx, root)
	if err != nil {
		return nil, nil, fmt.Errorf("loading head %q root %s: %w", name, root, err)
	}
	info := loaded.Info()
	if err := matchPublishedRoot(doc.Net, *entry, info); err != nil {
		return nil, nil, err
	}
	manifest, err := parseManifestTip(*entry)
	if err != nil {
		return nil, nil, err
	}

	return &finalizedClaim{
		archiveID: *doc.ArchiveID,
		net:       doc.Net,
		entry:     *entry,
		root:      root,
		manifest:  manifest,
	}, loaded, nil
}

func matchPublishedRoot(net string, entry server.HeadEntry, info archive.Info) error {
	if info.Name != entry.Name || info.Net != net || info.Root.String() != entry.Root ||
		info.OriginSlot != entry.OriginSlot || info.SegBits != entry.SegBits ||
		info.FanoutBits != entry.FanoutBits || info.DirDepth != entry.DirDepth ||
		!sameCoverage(info.SyncedTo, entry.SyncedTo) {
		return fmt.Errorf("head %q root %s does not reproduce its signed publication fields: "+
			"root has net=%q name=%q origin_slot=%d synced_to=%s seg_bits=%d fanout_bits=%d dir_depth=%d; "+
			"document has net=%q name=%q origin_slot=%d synced_to=%s seg_bits=%d fanout_bits=%d dir_depth=%d",
			entry.Name, entry.Root,
			info.Net, info.Name, info.OriginSlot, pointerCoverageString(info.SyncedTo), info.SegBits, info.FanoutBits, info.DirDepth,
			net, entry.Name, entry.OriginSlot, pointerCoverageString(entry.SyncedTo), entry.SegBits, entry.FanoutBits, entry.DirDepth)
	}
	return nil
}

func sameClaimIdentity(a, b *finalizedClaim) bool {
	return a.archiveID == b.archiveID && a.net == b.net &&
		a.entry.Name == b.entry.Name &&
		a.entry.EffectiveKind() == b.entry.EffectiveKind() &&
		a.entry.OriginSlot == b.entry.OriginSlot &&
		a.entry.SegBits == b.entry.SegBits &&
		a.entry.FanoutBits == b.entry.FanoutBits
}

func compareClaimRoots(ctx context.Context, blocks blockstore.Blockstore, left, right *finalizedClaim) (ClaimRelation, error) {
	cmp := compareCoverage(left.entry.SyncedTo, right.entry.SyncedTo)
	if cmp == 0 {
		if left.root != right.root {
			return ClaimRelationInvalid, newArchiveConflict(left, right, ConflictReasonEqualCoverageRootMismatch,
				"equal coverage commits to different roots")
		}
		return ClaimsEquivalent, nil
	}

	higher, lower := left, right
	relation := LeftClaimDominates
	if cmp < 0 {
		higher, lower = right, left
		relation = RightClaimDominates
	}

	// Truncate is the reference projection algorithm, but it writes the derived
	// segment/directory/root blocks. Run it over a read-through scratch layer so
	// classification cannot leak unpinned proof artifacts into the live store or
	// race its collector. The source store is read-only through this boundary.
	scratch := newProjectionBlockstore(blocks)
	projectedHead, err := archive.Load(ctx, archive.Config{Blocks: scratch}, higher.root)
	if err != nil {
		return ClaimRelationInvalid, attributedClaimEvidence(
			fmt.Errorf("follow: loading higher-coverage root %s for isolated projection: %w", higher.root, err),
			higher == left, higher == right)
	}
	var projected cid.Cid
	if lower.entry.SyncedTo == nil {
		projected, err = projectedHead.TruncateToEmpty(ctx)
	} else {
		projected, err = projectedHead.Truncate(ctx, *lower.entry.SyncedTo)
	}
	if err != nil {
		return ClaimRelationInvalid, attributedClaimEvidence(
			fmt.Errorf("follow: projecting higher-coverage root %s to synced_to %s: %w",
				higher.root, pointerCoverageString(lower.entry.SyncedTo), err),
			higher == left, higher == right)
	}
	if projected != lower.root {
		return ClaimRelationInvalid, newArchiveConflict(left, right, ConflictReasonPrefixProjectionMismatch,
			fmt.Sprintf("higher-coverage root projects to %s at synced_to %s, not the lower root %s",
				projected, pointerCoverageString(lower.entry.SyncedTo), lower.root))
	}
	return relation, nil
}

// projectionBlockstore is a read-through/write-isolated blockstore. Archive
// projection can read the source DAG, while every block it derives is confined
// to memory for the lifetime of one comparison. It deliberately never mutates
// base.
type projectionBlockstore struct {
	base    blockstore.Blockstore
	scratch blockstore.Blockstore
}

func newProjectionBlockstore(base blockstore.Blockstore) *projectionBlockstore {
	return &projectionBlockstore{
		base:    base,
		scratch: blockstore.NewBlockstore(dssync.MutexWrap(datastore.NewMapDatastore())),
	}
}

func (p *projectionBlockstore) DeleteBlock(ctx context.Context, c cid.Cid) error {
	return p.scratch.DeleteBlock(ctx, c)
}

func (p *projectionBlockstore) Has(ctx context.Context, c cid.Cid) (bool, error) {
	has, err := p.scratch.Has(ctx, c)
	if err != nil || has {
		return has, err
	}
	return p.base.Has(ctx, c)
}

func (p *projectionBlockstore) Get(ctx context.Context, c cid.Cid) (blocksformat.Block, error) {
	blk, err := p.scratch.Get(ctx, c)
	if err == nil || !format.IsNotFound(err) {
		return blk, err
	}
	return p.base.Get(ctx, c)
}

func (p *projectionBlockstore) GetSize(ctx context.Context, c cid.Cid) (int, error) {
	size, err := p.scratch.GetSize(ctx, c)
	if err == nil || !format.IsNotFound(err) {
		return size, err
	}
	return p.base.GetSize(ctx, c)
}

func (p *projectionBlockstore) Put(ctx context.Context, block blocksformat.Block) error {
	return p.scratch.Put(ctx, block)
}

func (p *projectionBlockstore) PutMany(ctx context.Context, blocks []blocksformat.Block) error {
	return p.scratch.PutMany(ctx, blocks)
}

func (p *projectionBlockstore) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	// The archive engine does not enumerate during projection, but implementing
	// a real union keeps this a correct Blockstore rather than a test-shaped one.
	base, err := p.base.AllKeysChan(ctx)
	if err != nil {
		return nil, err
	}
	scratch, err := p.scratch.AllKeysChan(ctx)
	if err != nil {
		return nil, err
	}
	out := make(chan cid.Cid)
	go func() {
		defer close(out)
		seen := make(map[string]struct{})
		forward := func(in <-chan cid.Cid) bool {
			for c := range in {
				key := c.KeyString()
				if _, duplicate := seen[key]; duplicate {
					continue
				}
				seen[key] = struct{}{}
				select {
				case out <- c:
				case <-ctx.Done():
					return false
				}
			}
			return true
		}
		if !forward(scratch) {
			return
		}
		forward(base)
	}()
	return out, nil
}

func compareClaimManifests(ctx context.Context, blocks blockstore.Blockstore, left, right *finalizedClaim) (ClaimRelation, error) {
	switch {
	case !left.manifest.Defined() && !right.manifest.Defined():
		return ClaimsEquivalent, nil
	case left.manifest.Defined() != right.manifest.Defined():
		return ClaimsIncomparable, nil
	case left.manifest == right.manifest:
		return ClaimsEquivalent, nil
	}

	leftDescends, leftErr := manifestDescends(ctx, blocks, left.manifest, right.manifest)
	if leftDescends {
		return LeftClaimDominates, nil
	}
	rightDescends, rightErr := manifestDescends(ctx, blocks, right.manifest, left.manifest)
	if rightDescends {
		return RightClaimDominates, nil
	}
	if leftErr != nil || rightErr != nil {
		return ClaimRelationInvalid, attributedClaimEvidence(
			fmt.Errorf("follow: manifest ancestry for logical archive %s head %q is not yet proven: %w",
				left.archiveID, left.entry.Name, errors.Join(leftErr, rightErr)),
			leftErr != nil, rightErr != nil)
	}
	return ClaimRelationInvalid, newArchiveConflict(left, right, ConflictReasonManifestBranch,
		"manifest tips have non-descendant histories")
}

// manifestDescends proves descendant -> ... -> ancestor using the generic
// manifest link contract. It intentionally does not decode a manifest schema:
// link ancestry is version-agnostic, matching the existing accepted-floor walk.
// It stops at ancestor without loading it because the descendant's link is the
// cryptographic proof of ancestry. A structurally invalid block or unavailable
// link is an unproven source claim, not evidence of cross-authority conflict.
func manifestDescends(ctx context.Context, blocks blockstore.Blockstore, descendant, ancestor cid.Cid) (bool, error) {
	for current, hops := descendant, 0; hops < maxManifestWalk; hops++ {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		if current == ancestor {
			return true, nil
		}
		if current.Prefix().Codec != cid.DagCBOR {
			return false, fmt.Errorf("manifest %s is not a dag-cbor block", current)
		}
		blk, err := blocks.Get(ctx, current)
		if err != nil {
			return false, fmt.Errorf("reading manifest %s: %w", current, err)
		}
		kids, err := links(blk.RawData(), current)
		if err != nil {
			return false, err
		}
		switch len(kids) {
		case 0:
			return false, nil
		case 1:
			current = kids[0]
		default:
			return false, fmt.Errorf("manifest %s carries %d links, want at most 1 (prev)", current, len(kids))
		}
	}
	return false, fmt.Errorf("manifest chain from %s exceeds %d hops without proving ancestor %s",
		descendant, maxManifestWalk, ancestor)
}

func newArchiveConflict(left, right *finalizedClaim, reasonCode ConflictReason, reason string) *ArchiveConflictError {
	lSynced, lCovered := coverage(left.entry.SyncedTo)
	rSynced, rCovered := coverage(right.entry.SyncedTo)
	return &ArchiveConflictError{
		ArchiveID:     left.archiveID,
		Head:          left.entry.Name,
		LeftRoot:      left.root,
		RightRoot:     right.root,
		LeftSyncedTo:  lSynced,
		RightSyncedTo: rSynced,
		LeftCovered:   lCovered,
		RightCovered:  rCovered,
		LeftManifest:  left.manifest,
		RightManifest: right.manifest,
		ReasonCode:    reasonCode,
		Reason:        reason,
	}
}

func compareCoverage(left, right *uint64) int {
	switch {
	case left == nil && right == nil:
		return 0
	case left == nil:
		return -1
	case right == nil:
		return 1
	case *left < *right:
		return -1
	case *left > *right:
		return 1
	default:
		return 0
	}
}

func sameCoverage(left, right *uint64) bool { return compareCoverage(left, right) == 0 }

func coverage(p *uint64) (uint64, bool) {
	if p == nil {
		return 0, false
	}
	return *p, true
}

func pointerCoverageString(p *uint64) string {
	slot, covered := coverage(p)
	return coverageString(slot, covered)
}

func coverageString(slot uint64, covered bool) string {
	if !covered {
		return "null"
	}
	return fmt.Sprintf("%d", slot)
}
