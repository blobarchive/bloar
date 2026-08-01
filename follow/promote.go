package follow

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/cockroachdb/pebble/v2"
	"github.com/ipfs/boxo/blockstore"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"

	"github.com/blobarchive/bloar/archive"
	"github.com/blobarchive/bloar/core"
	"github.com/blobarchive/bloar/pinning"
	"github.com/blobarchive/bloar/server"
)

// PromotionConfig is what ReconcileWriterPromotion needs: the KV the follower
// checkpoint lives in, the compatibility mirrors to reconcile, the local
// blockstore to load and prove the checkpoint's generation against, and the
// immutable params OpenHead will validate the promoted head with.
type PromotionConfig struct {
	// KV is store.KV(), where the follower checkpoint ('f' keyspace) is, and the
	// KV the compatibility mirrors ('h', 'm') are also on -- one Pebble store, so
	// the handoff batch below spans all three. Required.
	KV *pebble.DB
	// Roots is the RootStore mirror ('h' keyspace) to reconcile. Required.
	Roots *server.RootStore
	// Manifests is the ManifestStore mirror ('m' keyspace) to reconcile. Optional;
	// nil is a node with no manifest chains, on which a checkpoint that carries a
	// tip is an inconsistency ReconcileWriterPromotion fails closed on.
	Manifests *server.ManifestStore
	// Blocks is the local blockstore the checkpoint's generation is loaded and
	// proved complete against. Required. Every read here is local -- a promotion
	// resumes from disk and never the network, and an offline read of a block this
	// node does not hold is what fails an incompletely-synced checkpoint closed.
	Blocks blockstore.Blockstore
	// Cache is the decoded-node cache. Optional.
	Cache *core.NodeCache
	// Params is the full set of immutable head parameters OpenHead will validate
	// the promoted head against (spec 3.1): name, net, origin_slot, seg_bits,
	// fanout_bits. It comes from the same config OpenHead reads, so the promotion
	// preflight and the OpenHead check cannot drift and a mismatch is caught before
	// any handoff write rather than after.
	Params archive.Params
	// Policy is the retention policy the promoted head will run as a writer (the
	// heads.<name>.pin config). It scopes the completeness preflight: the
	// index and manifest chain must be complete for ANY policy, but a blob leaf may be
	// absent where the policy does not retain it -- a window follower promotes with a
	// knowingly incomplete blob history (spec 9, operations 7.2), and that hole is not
	// an inconsistency. The completeness walk is the policy's own Desired set, so it
	// permits exactly the holes the retention policy explains.
	Policy pinning.Policy
	// Logger receives the one line a reconciliation writes. Optional.
	Logger *slog.Logger
}

// promotionBeforeCommit, when set, is called immediately before the handoff batch
// commits. It exists only for the crash-point tests (see export_test.go): a
// non-nil hook that returns an error simulates a crash after the batch is fully
// staged but before it is made durable, so a test can prove the handoff left
// nothing changed and a rerun reconciles. Nil in production.
var promotionBeforeCommit func() error

// ReconcileWriterPromotion promotes a followed head to a writer by materializing
// its RootStore and ManifestStore compatibility mirrors from its authoritative
// follower checkpoint and retiring the checkpoint, as ONE crash-idempotent handoff,
// for a head an operator has moved from follow.heads to heads (spec 11.3, audit
// the follow-up hardening, the transition invariant). It returns whether a follower checkpoint was
// found (for the caller's log), and must run before the head is opened as a writer.
//
// # Why the mirrors need reconciling at all
//
// A followed head commits its authoritative generation as an atomic checkpoint and
// only then exposes it, which is what writes the 'h'/'m' mirrors. A crash between
// the two leaves the mirrors older than -- or absent beside -- the checkpoint, the
// authoritative generation. OpenHead resumes a writer from the mirrors, so opening
// from a stale one would republish a generation the checkpoint already superseded,
// regressing or splitting the head.
//
// # One-way, crash-idempotent handoff
//
// The previous design rewrote the mirrors but left the checkpoint in place, so a
// promoted writer that then advanced durably had its NEWER root and manifest rewound
// on its next restart, when the still-present checkpoint reconciled the mirrors
// backward again. The fix is retire-by-delete in the same
// batch: the mirrors are materialized and the checkpoint is DELETED together, one
// synced Pebble batch over the shared KV. A crash before the batch commits changes
// nothing -- the checkpoint stands, the rerun reconciles again; a crash after it
// leaves no checkpoint, so every later startup finds none and is a no-op, and the
// writer's own advancing state is never touched. The handoff cannot run twice
// against a promoted, advanced writer.
//
// # Fail closed before any write
//
// Before the batch, the checkpoint's generation is fully validated against the local
// store, and every validation aborts the promotion with no mirror mutation and no
// checkpoint retirement: all of the immutable params OpenHead checks (so
// a params mismatch cannot rewrite the mirrors before startup fails), the coverage
// the root encodes against the checkpoint floor, and -- because a checkpoint
// guarantees only that the Head block is local -- that the retained DAG and the
// manifest chain are COMPLETE on disk, walked offline. The follower serving path
// stays lazy (spec 11.3); completeness is a promotion-time requirement, because a
// promotion removes the fetch path and opens a local-only writer, which must hold
// everything it will serve and publish.
//
// It is a no-op for a head with no follower checkpoint -- a genuine writer, or a
// followed head that never committed one.
func ReconcileWriterPromotion(ctx context.Context, cfg PromotionConfig, head string) (found bool, err error) {
	log := cfg.Logger
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}

	st := &state{kv: cfg.KV}
	cp, ok, err := st.checkpoint(head)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, nil // no follower checkpoint: a genuine writer, nothing to reconcile.
	}
	if cp.version == checkpointVersionV3 && !cp.selected {
		return true, fmt.Errorf("follow: refusing to promote withdrawn head %q: its v3 checkpoint is an authenticated tombstone, not a selected writer generation", head)
	}
	if cp.kind == server.UnfinalizedMutable {
		return true, fmt.Errorf("follow: refusing to promote mutable head %q: a bounded optimistic generation is replaceable authority state, not an append-only writer history", head)
	}

	// Load the Head block from the local store (durable by the adoption ordering).
	// archive.Load reads only the Head block; the rest of the generation is proved
	// complete by the offline walk below.
	h, err := archive.Load(ctx, archive.Config{Blocks: cfg.Blocks, Cache: cfg.Cache}, cp.root)
	if err != nil {
		return true, fmt.Errorf("follow: promoting head %q from its follower checkpoint: loading root %s: %w", head, cp.root, err)
	}

	// Every immutable param OpenHead validates, checked here against the same config
	// so the two cannot drift. A mismatch is fatal (spec 3.1) and must abort BEFORE
	// any handoff write, so a misconfigured promotion never rewrites the durable
	// mirrors only for OpenHead to fail immediately after.
	if got := h.Params(); got != cfg.Params {
		return true, &server.ParamsMismatchError{Name: head, Want: cfg.Params, Got: got}
	}

	// Coverage consistency (spec 11.3): the coverage the durable root encodes must be
	// at least the floor the checkpoint records, the same fail-closed rule Resume
	// applies. A checkpoint floor above its root's coverage is an inconsistent local
	// state, refused rather than promoted.
	derived, covered := h.SyncedTo()
	if !covered || derived < cp.syncedTo {
		return true, fmt.Errorf("follow: promoting head %q: its checkpoint floor %d is above the coverage its root %s "+
			"encodes (%d, covered=%t); refusing to promote an inconsistent generation", head, cp.syncedTo, cp.root, derived, covered)
	}

	// A checkpoint attests only that the Head block is local; the index below it and
	// the manifest chain may still be unfetched after a crash right after checkpointing
	// (the fetch pass runs later, and first-tip ancestry checking does not fetch the
	// tip). A promotion opens a local-only writer, so prove the generation usable on
	// disk before handing it off, offline. The index and manifest chain must be
	// COMPLETE and CID-valid for any policy; a blob leaf may be absent only where the
	// promoted head's retention policy does not retain it -- a window follower promotes
	// with a knowingly incomplete blob history (spec 9, operations 7.2). The walk is
	// the policy's Desired set, so it permits exactly the holes the policy explains.
	if err := checkPromotionCompleteness(ctx, cfg.Blocks, cfg.Policy, h); err != nil {
		return true, fmt.Errorf("follow: promoting head %q: its checkpoint root %s is not completely synced locally; "+
			"refusing to promote an incomplete generation: %w", head, cp.root, err)
	}
	if cp.manifestTip.Defined() {
		if cfg.Manifests == nil {
			return true, fmt.Errorf("follow: promoting head %q: its checkpoint carries manifest tip %s but this node has "+
				"no ManifestStore configured", head, cp.manifestTip)
		}
		if err := walkManifestChainLocal(ctx, cfg.Blocks, cp.manifestTip); err != nil {
			return true, fmt.Errorf("follow: promoting head %q: its checkpoint manifest tip %s is not completely synced "+
				"locally; refusing to promote an incomplete manifest chain: %w", head, cp.manifestTip, err)
		}
	}

	// The handoff: materialize the mirrors EXACTLY to the checkpoint's generation and
	// retire the checkpoint, in one synced batch (see the type comment). The
	// RootStore is set unconditionally; the ManifestStore is set for a defined tip and
	// CLEARED for an undefined one, so no stale tip survives; the checkpoint is
	// deleted, which is what makes every later startup a no-op.
	b := cfg.KV.NewBatch()
	defer b.Close()
	if err := cfg.Roots.StagePut(b, head, cp.root); err != nil {
		return true, fmt.Errorf("follow: promoting head %q: staging its root mirror: %w", head, err)
	}
	switch {
	case cp.manifestTip.Defined():
		if err := cfg.Manifests.StagePut(b, head, cp.manifestTip); err != nil {
			return true, fmt.Errorf("follow: promoting head %q: staging its manifest mirror: %w", head, err)
		}
	case cfg.Manifests != nil:
		if err := cfg.Manifests.StageDelete(b, head); err != nil {
			return true, fmt.Errorf("follow: promoting head %q: staging the clearing of its manifest mirror: %w", head, err)
		}
	}
	if err := st.stageRetireCheckpoint(b, head); err != nil {
		return true, fmt.Errorf("follow: promoting head %q: %w", head, err)
	}
	if promotionBeforeCommit != nil {
		if err := promotionBeforeCommit(); err != nil {
			return true, err
		}
	}
	if err := b.Commit(pebble.Sync); err != nil {
		return true, fmt.Errorf("follow: promoting head %q: committing the handoff batch: %w", head, err)
	}

	log.Info("promoted a followed head to writer: materialized its root and manifest mirrors from the authoritative "+
		"follower checkpoint and retired the checkpoint in one synced batch before opening it",
		"head", head, "root", cp.root, "synced_to", cp.syncedTo, "manifest", cidOrNone(cp.manifestTip))
	return true, nil
}

// checkPromotionCompleteness proves the promoted writer's generation is usable on
// disk, offline, against its retention policy. It walks the
// SAME Desired pin set the fetch pass materializes for this policy (pinning.Desired),
// which is what distinguishes an inconsistency from a policy-consistent hole:
//
//   - The index is complete under every policy (spec 9 pins all of it), so every
//     Desired pin -- the root, every directory page, every sealed and the open
//     Segment -- names an index block, and each must be present AND CID-valid.
//   - A blob leaf is reached only through a RECURSIVE Desired pin (a full head's root,
//     or a window's retained Segments); those blobs the policy retains, so they must
//     be present. A blob under a DIRECT (index-only) Segment pin is one the policy
//     does not retain -- an out-of-window hole in a window follower -- and is never
//     walked, so its absence does not fail the promotion.
//
// Enumerate reads the directory over the LOCAL store, so a missing directory page
// fails here before the pin walk even begins. The manifest chain is checked
// separately (walkManifestChainLocal); it is not part of the head's Desired set.
func checkPromotionCompleteness(ctx context.Context, bs blockstore.Blockstore, policy pinning.Policy, h *archive.Head) error {
	enum, err := h.Enumerate(ctx)
	if err != nil {
		return fmt.Errorf("enumerating the index of root %s offline: %w", h.Root(), err)
	}
	desired, err := pinning.Desired(policy, enum)
	if err != nil {
		return err
	}
	for _, pin := range desired {
		if err := ctx.Err(); err != nil {
			return err
		}
		if pin.Recursive {
			// The pin's whole subtree the policy retains: index blocks present and
			// CID-valid, and every blob under them present.
			if err := walkRetainedSubtree(ctx, bs, pin.CID); err != nil {
				return fmt.Errorf("the %s pin %s: %w", pin.Purpose, pin.CID, err)
			}
			continue
		}
		// A direct pin: an index block the policy holds without its blobs (spec 9). It
		// alone must be present and CID-valid; the blobs it names are policy-consistent
		// holes and are not walked.
		if _, err := requireIndexBlock(ctx, bs, pin.CID); err != nil {
			return fmt.Errorf("the %s pin %s: %w", pin.Purpose, pin.CID, err)
		}
	}
	return nil
}

// walkRetainedSubtree proves every block reachable from root is present in bs and
// every index block CID-valid, offline. It is the recursive-pin case of the
// completeness walk: index nodes (dag-cbor) are read, CID-verified and expanded;
// blob leaves (raw) are required present. It reads links generically, exactly as the
// fetch pass and GC's mark do (see links).
func walkRetainedSubtree(ctx context.Context, bs blockstore.Blockstore, root cid.Cid) error {
	seen := map[string]bool{}
	stack := []cid.Cid{root}
	for len(stack) > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		c := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if seen[c.KeyString()] {
			continue
		}
		seen[c.KeyString()] = true

		switch c.Prefix().Codec {
		case cid.Raw:
			// A retained blob leaf: presence is the requirement. No links to follow.
			has, err := bs.Has(ctx, c)
			if err != nil {
				return fmt.Errorf("checking local block %s: %w", c, err)
			}
			if !has {
				return fmt.Errorf("retained blob %s is not held locally", c)
			}
		case cid.DagCBOR:
			blk, err := requireIndexBlock(ctx, bs, c)
			if err != nil {
				return err
			}
			kids, err := links(blk.RawData(), c)
			if err != nil {
				return err
			}
			stack = append(stack, kids...)
		default:
			return fmt.Errorf("block %s has codec 0x%x; bloar's DAG is raw blobs and dag-cbor index nodes (spec 2)",
				c, c.Prefix().Codec)
		}
	}
	return nil
}

// requireIndexBlock reads a dag-cbor index block from bs, offline, and proves it
// CID-valid: the block a promotion opens a writer from must be present AND hash to
// the CID that names it, so a locally corrupted index cannot be promoted (the local
// store does not re-hash on read). It returns the block for the caller to expand.
func requireIndexBlock(ctx context.Context, bs blockstore.Blockstore, c cid.Cid) (blocks.Block, error) {
	if c.Prefix().Codec != cid.DagCBOR {
		return nil, fmt.Errorf("index block %s has codec 0x%x, want dag-cbor (spec 2)", c, c.Prefix().Codec)
	}
	blk, err := bs.Get(ctx, c)
	if err != nil {
		return nil, fmt.Errorf("reading local index block %s: %w", c, err)
	}
	if err := verifyCID(c, blk.RawData()); err != nil {
		return nil, err
	}
	return blk, nil
}

// verifyCID recomputes the multihash of data under c's prefix and checks it names c.
// It is what makes the completeness preflight prove CID-validity, not just presence.
func verifyCID(c cid.Cid, data []byte) error {
	got, err := c.Prefix().Sum(data)
	if err != nil {
		return fmt.Errorf("hashing the local block for %s: %w", c, err)
	}
	if got != c {
		return fmt.Errorf("the local block stored for %s hashes to %s; it is corrupt", c, got)
	}
	return nil
}

// walkManifestChainLocal proves the manifest chain from tip is present in bs to
// genesis and CID-valid, offline. It is the promotion completeness check
// for the manifest history: a hash-chain walk following prev links generically (never
// decoding a manifest, spec 15), over the LOCAL store, so a dangling tip -- a defined
// manifest CID whose block a crash left unfetched -- or a corrupt manifest block fails
// the promotion closed rather than opening a writer that will republish a tip it
// cannot serve. The chain is bounded exactly as the ancestry floor's walk is
// (maxManifestWalk).
func walkManifestChainLocal(ctx context.Context, bs blockstore.Blockstore, tip cid.Cid) error {
	c := tip
	for hops := 0; hops < maxManifestWalk; hops++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		blk, err := requireIndexBlock(ctx, bs, c)
		if err != nil {
			return err
		}
		kids, err := links(blk.RawData(), c)
		if err != nil {
			return err
		}
		switch len(kids) {
		case 0:
			return nil // genesis: no prev, the whole chain is local.
		case 1:
			c = kids[0]
		default:
			return fmt.Errorf("manifest %s carries %d links, want at most 1 (prev)", c, len(kids))
		}
	}
	return fmt.Errorf("manifest chain from %s exceeds %d hops without reaching genesis", tip, maxManifestWalk)
}
