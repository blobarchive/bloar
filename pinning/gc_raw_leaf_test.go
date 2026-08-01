package pinning_test

import (
	"errors"
	"strings"
	"testing"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"

	"github.com/blobarchive/bloar/pinning"
	"github.com/blobarchive/bloar/store"
)

// TestGCMissingRawLeafIsNotChecked is the flipped regression reproducer. A raw
// leaf reached by a recursive full pin used to be marked terminal WITHOUT a read,
// so a swept-out blob was reported retained. The mark now reads every raw leaf
// through the same fail-closed/self-heal path an index node takes: a writer fails
// the run closed on the missing blob, and a follower heals it through its fetcher
// and counts the refetch.
func TestGCMissingRawLeafIsNotChecked(t *testing.T) {
	setup := func(t *testing.T) (*fixture, blocks.Block) {
		t.Helper()
		f := newFixture(t)
		h := f.head("archive", pinning.Full())
		f.apply(h, 11, f.row(8, 1))
		f.reconcileAll()

		missing := f.blobCID(1)
		saved, err := f.bs.Get(f.ctx, missing)
		if err != nil {
			t.Fatalf("Get(%s): %v", missing, err)
		}
		if err := f.bs.DeleteBlock(f.ctx, missing); err != nil {
			t.Fatalf("DeleteBlock(%s): %v", missing, err)
		}
		return f, saved
	}

	t.Run("written head fails closed", func(t *testing.T) {
		f, saved := setup(t)
		missing := saved.Cid()

		before := len(f.blockSet())
		stats, err := f.gc.Run(f.ctx)
		if err == nil {
			t.Fatal("gc.Run: want a fail-closed error for a missing raw leaf under a written head, got nil")
		}
		if !strings.Contains(err.Error(), "archive") || !strings.Contains(err.Error(), missing.String()) {
			t.Errorf("error %q does not name the written head and the missing raw leaf", err)
		}
		if stats.Refetched != 0 {
			t.Errorf("gc refetched %d blocks under a written head; a writer has nothing to fetch from", stats.Refetched)
		}
		if after := len(f.blockSet()); after != before {
			t.Errorf("gc deleted %d blocks despite failing the mark; a run that cannot read the DAG must not sweep it", before-after)
		}
	})

	t.Run("followed head heals", func(t *testing.T) {
		f, saved := setup(t)
		missing := saved.Cid()
		fetcher := &fakeFetcher{
			store: f.bs,
			have:  map[string]blocks.Block{missing.KeyString(): saved},
		}
		gc, err := pinning.NewGC(pinning.GCConfig{
			Blocks:     f.bs,
			Reconciler: f.rec,
			Fetch:      followAll(fetcher),
		})
		if err != nil {
			t.Fatalf("NewGC: %v", err)
		}

		stats, err := gc.Run(f.ctx)
		if err != nil {
			t.Fatalf("gc.Run: a followed head's missing raw leaf should heal, but the run failed: %v", err)
		}
		if stats.Refetched != 1 || fetcher.calls != 1 {
			t.Fatalf("missing raw leaf was not healed once: refetched=%d fetch_calls=%d, want 1/1", stats.Refetched, fetcher.calls)
		}
		if has, err := f.bs.Has(f.ctx, missing); err != nil || !has {
			t.Fatalf("missing raw leaf after GC: has=%t err=%v, want it healed back", has, err)
		}
	})
}

// TestGCDirectDagCBORPinRetainsWithoutChildren pins the direct-pin
// semantics the safety boundary fix must preserve (ModeNone, spec 9): a directly-pinned
// dag-cbor index node is read and retained, but its links are NEVER enqueued, so
// its blob children are still collected. The read is the fix; not expanding is the
// invariant the fix must not break -- "index blocks retained without blob
// descendants".
func TestGCDirectDagCBORPinRetainsWithoutChildren(t *testing.T) {
	f := newFixture(t)
	none := f.head("none", pinning.None())
	f.apply(none, 11, f.row(8, 1), f.row(9, 2))
	f.apply(none, 20, f.row(12, 3), f.row(20, 4))
	f.reconcileAll()

	// The sealed and open Segments are dag-cbor nodes, each a DIRECT pin under
	// ModeNone, and each links to the blobs of its rows. The GC reads them (so a
	// missing one would fail) and marks them, but expands none of them.
	enum := f.enumerate(none)
	if len(enum.Sealed) == 0 {
		t.Fatal("the none head sealed no segments; the fixture is meant to build one")
	}

	f.runGC()

	// Every index block survives -- the direct pins were read and kept -- and not one
	// blob does: a direct pin's links are never followed.
	f.expect().index(none).check()
}

// TestGCMissingDirectTarget is the direct-pin half of the safety boundary's fail-closed /
// self-heal rule. A directly-pinned dag-cbor Segment (ModeNone) is deleted; the
// mark reads it (it is a direct pin, not a walked descendant), so a writer fails
// the run closed and a follower heals it -- exactly the raw-leaf rule, on the other
// class the old mark skipped.
func TestGCMissingDirectTarget(t *testing.T) {
	setup := func(t *testing.T) (*fixture, blocks.Block) {
		t.Helper()
		f := newFixture(t, withStableCollectionGeneration())
		none := f.head("none", pinning.None())
		f.apply(none, 11, f.row(8, 1))
		f.apply(none, 20, f.row(12, 2))
		f.reconcileAll()

		// A sealed Segment: a direct pin under ModeNone, and not a block the reconcile
		// flush reads (it walks pages, not segments), so the mark is the first thing to
		// meet its absence.
		sealed := f.enumerate(none).Sealed
		if len(sealed) == 0 {
			t.Fatal("the none head sealed no segments")
		}
		missing := sealed[0].CID
		saved, err := f.bs.Get(f.ctx, missing)
		if err != nil {
			t.Fatalf("Get(%s): %v", missing, err)
		}
		if err := f.bs.DeleteBlock(f.ctx, missing); err != nil {
			t.Fatalf("DeleteBlock: %v", err)
		}
		return f, saved
	}

	t.Run("written head fails closed", func(t *testing.T) {
		f, saved := setup(t)
		missing := saved.Cid()

		before := len(f.blockSet())
		stats, err := f.gc.Run(f.ctx)
		if err == nil {
			t.Fatal("gc.Run: want a fail-closed error for a missing direct pin under a written head, got nil")
		}
		if !strings.Contains(err.Error(), "none") || !strings.Contains(err.Error(), missing.String()) {
			t.Errorf("error %q does not name the written head and the missing direct target", err)
		}
		if stats.Refetched != 0 {
			t.Errorf("gc refetched %d blocks under a written head; a writer must fail closed, not heal", stats.Refetched)
		}
		if after := len(f.blockSet()); after != before {
			t.Errorf("gc deleted %d blocks despite failing the mark; it must sweep nothing", before-after)
		}
	})

	t.Run("followed head heals", func(t *testing.T) {
		f, saved := setup(t)
		missing := saved.Cid()
		fetcher := &fakeFetcher{store: f.bs, have: map[string]blocks.Block{missing.KeyString(): saved}}
		gc, err := pinning.NewGC(pinning.GCConfig{Blocks: f.bs, Reconciler: f.rec, Fetch: followAll(fetcher)})
		if err != nil {
			t.Fatalf("NewGC: %v", err)
		}
		stats, err := gc.Run(f.ctx)
		if err != nil {
			t.Fatalf("gc.Run: a followed head's missing direct target should heal, but the run failed: %v", err)
		}
		if stats.Refetched != 1 || fetcher.calls != 1 {
			t.Fatalf("missing direct target was not healed once: refetched=%d fetch_calls=%d, want 1/1", stats.Refetched, fetcher.calls)
		}
		if has, _ := f.bs.Has(f.ctx, missing); !has {
			t.Error("the healed direct target is not back in the store; a self-heal that does not restore it re-wedges next run")
		}
	})
}

// TestGCMarkUsesValidatingGet proves the mark reads through the store's
// validating Get, not a presence-only shortcut,
// and that corruption is NOT a class the follower self-heal covers (the safety boundary
// follow-up). A raw leaf whose bytes are rewritten under its own CID -- present, but
// corrupt -- fails the mark with store.ErrCorruptBlock, which is not ipld.NotFound,
// so it fails the run for a followed head exactly as for a writer: the fetcher would
// mask the corruption by overwriting it from a peer, so repair is out of band
// (bloard fsck), never a silent refetch during the mark. There is no GetSize probe
// and no corruption registry: the mark reads the bytes it retains.
func TestGCMarkUsesValidatingGet(t *testing.T) {
	// corrupt sets up a covered full pin whose one blob leaf is present but corrupt,
	// and returns the fixture, the leaf CID, and the honest block a fetcher could
	// offer. The reconcile flush walks pages, not the leaf, so the mark is the first
	// read to meet the corruption.
	corrupt := func(t *testing.T) (*fixture, cid.Cid, blocks.Block) {
		t.Helper()
		f := newFixture(t)
		h := f.head("archive", pinning.Full())
		f.apply(h, 11, f.row(8, 1))
		f.reconcileAll()

		leaf := f.blobCID(1)
		honest, err := f.bs.Get(f.ctx, leaf)
		if err != nil {
			t.Fatalf("Get(%s): %v", leaf, err)
		}
		// Rewrite the blob's bytes under its own CID: the wrong bytes under the right
		// (multihash) key, the safety boundary's exact shape. A plain Put would no-op -- the
		// blockstore keys by multihash and skips a key it already has -- so delete the
		// honest block first, then Put the corrupt one (NewBlockWithCid does not verify).
		if err := f.bs.DeleteBlock(f.ctx, leaf); err != nil {
			t.Fatalf("DeleteBlock(%s): %v", leaf, err)
		}
		bad, err := blocks.NewBlockWithCid([]byte("blob 1's bytes rewritten under its own key"), leaf)
		if err != nil {
			t.Fatalf("framing corrupt leaf: %v", err)
		}
		if err := f.bs.Put(f.ctx, bad); err != nil {
			t.Fatalf("storing corrupt leaf: %v", err)
		}
		return f, leaf, honest
	}

	t.Run("writer fails closed", func(t *testing.T) {
		f, _, _ := corrupt(t)
		// A GC whose blockstore validates on Get, the way store.Blocks() hands it out
		// in production.
		gc, err := pinning.NewGC(pinning.GCConfig{Blocks: store.Validating(f.bs), Reconciler: f.rec})
		if err != nil {
			t.Fatalf("NewGC: %v", err)
		}
		before := len(f.blockSet())
		if _, err := gc.Run(f.ctx); !errors.Is(err, store.ErrCorruptBlock) {
			t.Fatalf("gc.Run over a corrupt raw leaf: got %v, want store.ErrCorruptBlock", err)
		}
		if after := len(f.blockSet()); after != before {
			t.Errorf("gc deleted %d blocks despite a corrupt leaf failing the mark; it must sweep nothing", before-after)
		}
	})

	t.Run("followed head does not heal corruption", func(t *testing.T) {
		f, leaf, honest := corrupt(t)
		// A fetcher that CAN supply the honest block: the run must still fail, proving
		// corruption is not routed to the self-heal (which covers NotFound only) and
		// the fetcher is never even consulted.
		fetcher := &fakeFetcher{store: f.bs, have: map[string]blocks.Block{leaf.KeyString(): honest}}
		gc, err := pinning.NewGC(pinning.GCConfig{Blocks: store.Validating(f.bs), Reconciler: f.rec, Fetch: followAll(fetcher)})
		if err != nil {
			t.Fatalf("NewGC: %v", err)
		}
		before := len(f.blockSet())
		stats, err := gc.Run(f.ctx)
		if !errors.Is(err, store.ErrCorruptBlock) {
			t.Fatalf("gc.Run over a corrupt raw leaf under a followed head: got %v, want store.ErrCorruptBlock (no heal)", err)
		}
		if stats.Refetched != 0 || fetcher.calls != 0 {
			t.Errorf("corruption was routed to the fetcher: refetched=%d fetch_calls=%d, want 0/0 (corruption is not NotFound)", stats.Refetched, fetcher.calls)
		}
		if after := len(f.blockSet()); after != before {
			t.Errorf("gc deleted %d blocks despite a corrupt leaf failing the mark; it must sweep nothing", before-after)
		}
	})
}

// TestGCValidatedReadsCountsActualReads pins the capacity contract: the mark's
// work is what it reads -- and the bytes it hashes --
// not the size of the marked set. A block direct-pinned under several heads is one
// marked entry but one read (and one copy of its bytes) PER head, each markHead row
// read before the shared set dedups, so GCStats.ValidatedReads and ValidatedBytes
// exceed GCStats.Marked. The reads are the read-amplification signal; the bytes are the
// cost signal (sized as validated_bytes / T + validated_reads x O), never the marked
// count. Two ModeNone heads that share a sealed segment demonstrate it: under None
// every pin is a direct read and nothing is recursively expanded, so the reads equal
// the total pin rows across both heads and the shared segment's bytes are counted
// twice, while the marked set dedups the shared block.
func TestGCValidatedReadsCountsActualReads(t *testing.T) {
	f := newFixture(t)
	h1 := f.head("h1", pinning.None())
	h2 := f.head("h2", pinning.None())

	// Identical first window (row over blob 1) -> both heads seal the SAME segment by
	// CID; they diverge in the second window, so only the first segment is shared.
	f.apply(h1, 11, f.row(8, 1))
	f.apply(h1, 20, f.row(12, 2))
	f.apply(h2, 11, f.row(8, 1))
	f.apply(h2, 20, f.row(12, 3))
	f.reconcileAll()

	stats := f.runGC()

	// None mode has no recursive pins, so every pin row is one direct validating read
	// and there is no expansion: the reads equal the total pin rows exactly.
	if stats.ValidatedReads != stats.Pins {
		t.Fatalf("ValidatedReads = %d, want == Pins %d (None mode: one validating read per direct pin row, no expansion)",
			stats.ValidatedReads, stats.Pins)
	}
	// And the shared segment makes the reads exceed the marked set: it is read under
	// both heads but marked once. This is exactly why the old "reads = Marked" size
	// derivation undercounts.
	if stats.ValidatedReads <= stats.Marked {
		t.Fatalf("ValidatedReads %d <= Marked %d; a block shared by two heads must read once per head but mark once",
			stats.ValidatedReads, stats.Marked)
	}

	// Bytes accumulate PER READ, never deduped: None retains no leaves (raw bytes 0),
	// and the node bytes are each head's index bytes SUMMED -- the shared segment's
	// bytes counted once under each head.
	if stats.ValidatedRawBytes != 0 {
		t.Fatalf("ValidatedRawBytes = %d, want 0 (None mode retains no leaves)", stats.ValidatedRawBytes)
	}
	h1Bytes := sumBlockBytes(t, f, indexBlockCIDs(h1, f)...)
	h2Bytes := sumBlockBytes(t, f, indexBlockCIDs(h2, f)...)
	if stats.ValidatedNodeBytes != h1Bytes+h2Bytes {
		t.Fatalf("ValidatedNodeBytes = %d, want %d (each head's index bytes summed; the shared block counts twice)",
			stats.ValidatedNodeBytes, h1Bytes+h2Bytes)
	}
	// Prove the double-count is real: the two heads DO share an index block, so the
	// summed total above genuinely counts the shared block's bytes twice (had it been
	// deduped like the marked set, the total would be smaller by those bytes).
	inH1 := map[cid.Cid]bool{}
	for _, c := range indexBlockCIDs(h1, f) {
		inH1[c] = true
	}
	var sharedBytes int64
	for _, c := range indexBlockCIDs(h2, f) {
		if inH1[c] {
			sharedBytes += sumBlockBytes(t, f, c)
		}
	}
	if sharedBytes == 0 {
		t.Fatal("the two heads share no index block; the fixture must build one so the double-count is exercised")
	}
}
