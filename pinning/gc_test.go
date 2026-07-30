package pinning_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/ipfs/boxo/blockstore"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"

	"github.com/blobarchive/bloar/archive"
	"github.com/blobarchive/bloar/catalog"
	"github.com/blobarchive/bloar/pinning"
	"github.com/blobarchive/bloar/store"
)

// TestGCWindowSlidesPastSharedBlob is the test spec 13.5 asks for, and the one
// the whole design is for: two heads over one blockstore, one keeping
// everything and one keeping a window, with a blob they both reference.
//
// The window slides past that blob. The window head's pins release it; the full
// head's do not; the blob stays. The window head's own out-of-window blob, which
// nothing else references, goes. Both heads keep every index block either way,
// so both can still answer "was there a blob at that slot" for ground whose
// blobs are gone (spec 9).
func TestGCWindowSlidesPastSharedBlob(t *testing.T) {
	f := newFixture(t)
	// 8 slots of retention. Windows are 4 slots, so this is two of them plus
	// the open one.
	full := f.head("full", pinning.Full())
	win := f.head("win", pinning.Window(slotsDur(8), testSecondsPerSlot))

	// Window 2 is slots 8..11, window 3 is 12..15, window 4 is 16..19, window 5
	// (20..23) stays open at synced_to 20.
	//
	//   blob 1: slot 8, BOTH heads     -- shared, out of win's window
	//   blob 2: slot 9, win only       -- unshared, out of win's window
	//   blob 5: slot 9, full only      -- same window as blob 2, kept by full
	//   blob 3: slot 12, win only      -- in window
	//   blob 6: slot 12, full only     -- in full
	//   blob 4: slot 20, win only      -- open segment
	f.apply(full, 11, f.row(8, 1), f.row(9, 5))
	f.apply(full, 20, f.row(12, 6))
	f.apply(win, 11, f.row(8, 1), f.row(9, 2))
	f.apply(win, 20, f.row(12, 3), f.row(20, 4))

	// synced_to 20, retention 8 slots -> the range is [12, 20]. Window 2 ends at
	// slot 11 and so falls out; windows 3 and 4 and the open window stay.
	f.reconcileAll()
	f.runGC()

	f.expect().
		index(full).index(win).
		// 1: dropped by win, held by full. The one that matters.
		// 5, 6: full's own, held by its recursive root pin.
		// 3: win's, in window. 4: win's, in the open segment.
		blobs(1, 5, 6, 3, 4).
		check() // blob 2 is gone: out of win's window and nothing else has it.

	// The index survived, so the window head still answers for slot 9 -- with
	// the row, whose blob is now unfetchable. That is what "the index stays
	// complete under every mode" buys.
	res, err := win.Lookup(f.ctx, 9)
	if err != nil {
		t.Fatalf("Lookup(9) after gc: %v", err)
	}
	if res.Status != archive.StatusFound || len(res.Entries) != 1 {
		t.Fatalf("win.Lookup(9) = %v with %d entries, want found with 1", res.Status, len(res.Entries))
	}
}

// TestGCNoneMode: the index survives in full, every blob goes.
func TestGCNoneMode(t *testing.T) {
	f := newFixture(t)
	none := f.head("none", pinning.None())

	f.apply(none, 11, f.row(8, 1), f.row(9, 2))
	f.apply(none, 20, f.row(12, 3), f.row(20, 4))

	f.reconcileAll()
	f.runGC()

	// Not even the open segment's blob: none is none.
	f.expect().index(none).check()
}

// TestGCFullModeRootSwap is spec 13.3's old-root orphaning, seen from the pin
// side: after a swap, reconcile and GC, the previous root's blocks are gone and
// the new root is whole. Structural sharing means "the previous root's blocks"
// is only the rewritten spine -- every segment and page the update did not touch
// is byte-identical and still live under the new root.
func TestGCFullModeRootSwap(t *testing.T) {
	f := newFixture(t)
	full := f.head("full", pinning.Full())

	f.apply(full, 11, f.row(8, 1))
	f.reconcileAll()
	oldRoot := full.Root()
	oldOpen := f.enumerate(full).Open

	f.apply(full, 20, f.row(12, 2))
	newRoot := full.Root()
	if oldRoot == newRoot {
		t.Fatal("root did not change across an apply")
	}

	// Before reconciliation the old root is still pinned: the ledger is what
	// pins, and nothing has told it about the swap.
	if !hasPin(f.pins("full"), oldRoot) {
		t.Fatal("the old root is not pinned before reconciliation; the ledger has lost track of what it pinned")
	}
	f.reconcileAll()
	if hasPin(f.pins("full"), oldRoot) {
		t.Error("the old root is still pinned after reconciliation")
	}
	if !hasPin(f.pins("full"), newRoot) {
		t.Error("the new root is not pinned after reconciliation")
	}

	f.runGC()
	f.expect().index(full).blobs(1, 2).check()

	if has, _ := f.bs.Has(f.ctx, oldRoot); has {
		t.Error("the old root's block survived gc")
	}
	if has, _ := f.bs.Has(f.ctx, oldOpen); has {
		t.Error("the old open segment survived gc; the apply rewrote it, so it is an orphan")
	}
}

// TestGCIsIdempotent: a second run with nothing changed deletes nothing, and a
// second reconcile writes nothing. Both are the same claim -- that the desired
// set is a function of the head and not of history.
func TestGCIsIdempotent(t *testing.T) {
	f := newFixture(t)
	win := f.head("win", pinning.Window(slotsDur(8), testSecondsPerSlot))
	f.apply(win, 20, f.row(8, 1), f.row(12, 2), f.row(20, 3))

	f.reconcileAll()
	if delta := f.reconcileAll(); delta != (pinning.Delta{}) {
		t.Errorf("second reconcile = %+v, want no ledger churn", delta)
	}

	first := f.runGC()
	if first.Swept == 0 {
		t.Fatal("the first gc swept nothing; the fixture is meant to leave orphans")
	}
	second := f.runGC()
	if second.Swept != 0 {
		t.Errorf("second gc swept %d blocks, want 0", second.Swept)
	}
	if second.Marked != first.Marked {
		t.Errorf("second gc marked %d blocks, first marked %d; the mark set must not move on its own",
			second.Marked, first.Marked)
	}
}

// TestGCFlushesReconciliation is why GC owns the flush: an apply that nothing
// reconciled leaves the new root's blocks unpinned, and a GC that marked from
// the ledger as it found it would sweep the spine of a root the head is
// serving.
func TestGCFlushesReconciliation(t *testing.T) {
	f := newFixture(t)
	full := f.head("full", pinning.Full())
	f.apply(full, 11, f.row(8, 1))
	f.reconcileAll()

	// Swap the root and tell nobody, which is exactly the state between a
	// mutation and its reconciliation pass.
	f.apply(full, 20, f.row(12, 2))
	f.runGC()

	f.expect().index(full).blobs(1, 2).check()
}

// TestGCMarksThroughDagCBORGenerically: the mark set is built by reading links
// out of the data model, so a dag-cbor block nothing in this build understands
// still has its children marked.
func TestGCMarksThroughDagCBORGenerically(t *testing.T) {
	f := newFixture(t)
	full := f.head("full", pinning.Full())
	f.apply(full, 11, f.row(8, 1))
	f.reconcileAll()
	f.runGC()

	// Everything reachable from the root: the blob is only reachable through a
	// Segment, whose rows nest the link two levels down inside lists.
	f.expect().index(full).blobs(1).check()
}

// TestGCEmptyHead: a head with no refs is a Head block and nothing else.
func TestGCEmptyHead(t *testing.T) {
	f := newFixture(t)
	empty := f.head("empty", pinning.Full())
	f.reconcileAll()
	f.runGC()
	f.expect().cid(empty.Root(), "the empty head's root").check()
}

// TestGCStaleLedgerRowsAreFlushed: a row no policy asks for is gone before the
// mark, because the mark is preceded by a reconcile. Rows cannot accumulate into
// retention nobody asked for.
func TestGCStaleLedgerRowsAreFlushed(t *testing.T) {
	f := newFixture(t)
	full := f.head("full", pinning.Full())
	f.apply(full, 11, f.row(8, 1))

	// A pin on a blob no head references: an old policy's leftover, or a bug's.
	f.putBlob(99)
	if err := f.led.Add(f.ctx, "full", pinning.PurposeIndex, f.blobCID(99), false); err != nil {
		t.Fatalf("Add: %v", err)
	}
	f.runGC()

	f.expect().index(full).blobs(1).check() // blob 99 is not retained by a row nothing asks for.
	if len(f.pins("full")) != 1 {
		t.Errorf("the ledger holds %v, want only the full head's root pin", f.pins("full"))
	}
}

// TestGCMissingBlockFailsClosed: a block that is reachable but absent stops the
// run. The ledger and the blockstore already disagree, and the one thing a
// collector must not do with a DAG it cannot fully read is delete the rest of
// it.
//
// This is the writer's semantics, and the fixture's GC is a writer's: it has no
// GCConfig.Fetch, so a missing pinned block is real divergence and not a block
// to go and get (the follower self-heal, tested below, is the other seam).
func TestGCMissingBlockFailsClosed(t *testing.T) {
	f := newFixture(t)
	full := f.head("full", pinning.Full())
	f.apply(full, 11, f.row(8, 1))
	f.apply(full, 20, f.row(12, 2))
	f.reconcileAll()

	// Delete a sealed segment: reachable from the root, and not something the
	// reconcile pass reads (it walks pages, not segments), so the run gets as
	// far as the mark.
	sealed := f.enumerate(full).Sealed
	if len(sealed) == 0 {
		t.Fatal("the fixture sealed no segments")
	}
	if err := f.bs.DeleteBlock(f.ctx, sealed[0].CID); err != nil {
		t.Fatalf("DeleteBlock: %v", err)
	}

	before := len(f.blockSet())
	stats, err := f.gc.Run(f.ctx)
	if err == nil {
		t.Fatal("gc.Run: want an error when a reachable block is absent, got nil")
	}
	if stats.Refetched != 0 {
		t.Errorf("a fetcher-less GC refetched %d blocks; a writer has nothing to fetch from", stats.Refetched)
	}
	// Nothing was swept: the failure was in the mark, and a partial mark set is
	// not a licence to delete.
	if after := len(f.blockSet()); after != before {
		t.Errorf("gc deleted %d blocks despite failing to mark; a run that cannot read the DAG must not sweep it", before-after)
	}
}

// fakeFetcher is a pinning.BlockFetcher for the self-heal tests. It returns the
// blocks it was handed and writes them through to the store, the way
// p2p.FetchingBlockstore does over bitswap (so a healed block is present for the
// sweep and every run after), or fails every fetch.
type fakeFetcher struct {
	store blockstore.Blockstore
	have  map[string]blocks.Block // by cid.KeyString
	fail  bool
	calls int
}

func (f *fakeFetcher) Get(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	f.calls++
	if f.fail {
		return nil, fmt.Errorf("fakeFetcher: no peer offered %s", c)
	}
	blk, ok := f.have[c.KeyString()]
	if !ok {
		return nil, fmt.Errorf("fakeFetcher: %s was not on offer", c)
	}
	if err := f.store.Put(ctx, blk); err != nil {
		return nil, err
	}
	return blk, nil
}

// followAll is the per-head fetch resolver of a pure follower (GCConfig.Fetch):
// it heals every head through fetch, which is what a node that follows
// everything it retains does. The mixed-node tests below scope a resolver to
// only some heads instead.
func followAll(fetch pinning.BlockFetcher) func(string) pinning.BlockFetcher {
	return func(string) pinning.BlockFetcher { return fetch }
}

// followOnly is a per-head fetch resolver for a mixed node: it heals only the
// named heads through fetch and fails every other head closed. This is the shape
// a node that writes some heads and follows others hands its GC (spec 9's
// per-head self-heal scope).
func followOnly(fetch pinning.BlockFetcher, followed ...string) func(string) pinning.BlockFetcher {
	set := make(map[string]bool, len(followed))
	for _, n := range followed {
		set[n] = true
	}
	return func(head string) pinning.BlockFetcher {
		if set[head] {
			return fetch
		}
		return nil
	}
}

// TestGCMixedNodeScopesSelfHealToFollowedHead is the mixed-scope GC regression: on a
// node that writes one head and follows another over one blockstore, the
// self-heal is scoped to the followed head. A missing pinned block under the
// written head fails the run closed -- the operator's signal that the writer's
// own store lost data -- while the same kind of miss under the followed head is
// healed, exactly as a pure follower's would be. A global fetcher (the bug)
// would refetch the written head's loss too and mask real corruption as a
// bloar_gc_refetched_blocks_total tick.
func TestGCMixedNodeScopesSelfHealToFollowedHead(t *testing.T) {
	// A mixed node: head "written" is written here, head "followed" is followed.
	// Distinct blobs, so the two heads' loaded segments do not share a block and
	// a deleted segment is reachable from exactly one head.
	setup := func(t *testing.T) (*fixture, *archive.Head, *archive.Head) {
		t.Helper()
		f := newFixture(t, withStableCollectionGeneration())
		written := f.head("written", pinning.Full())
		followed := f.head("followed", pinning.Full())
		f.apply(written, 11, f.row(8, 1))
		f.apply(written, 20, f.row(12, 2))
		f.apply(followed, 11, f.row(8, 3))
		f.apply(followed, 20, f.row(12, 4))
		f.reconcileAll()
		return f, written, followed
	}

	t.Run("written head miss fails closed", func(t *testing.T) {
		f, written, _ := setup(t)

		sealed := f.enumerate(written).Sealed
		if len(sealed) == 0 {
			t.Fatal("the written head sealed no segments")
		}
		missing := sealed[0].CID
		saved, err := f.bs.Get(f.ctx, missing)
		if err != nil {
			t.Fatalf("Get(%s): %v", missing, err)
		}
		if err := f.bs.DeleteBlock(f.ctx, missing); err != nil {
			t.Fatalf("DeleteBlock: %v", err)
		}

		// The fetcher can supply the block: the run must still fail, which is what
		// proves the failure is the per-head scope and not an empty fetcher.
		fetcher := &fakeFetcher{store: f.bs, have: map[string]blocks.Block{missing.KeyString(): saved}}
		gc, err := pinning.NewGC(pinning.GCConfig{Blocks: f.bs, Reconciler: f.rec, Fetch: followOnly(fetcher, "followed")})
		if err != nil {
			t.Fatalf("NewGC: %v", err)
		}

		before := len(f.blockSet())
		stats, err := gc.Run(f.ctx)
		if err == nil {
			t.Fatal("gc.Run: want a fail-closed error for a missing block under the written head, got nil")
		}
		if !strings.Contains(err.Error(), "written") || !strings.Contains(err.Error(), missing.String()) {
			t.Errorf("error %q does not name the written head and the missing block", err)
		}
		if stats.Refetched != 0 {
			t.Errorf("gc refetched %d blocks under a written head; a written head must fail closed, not heal", stats.Refetched)
		}
		if fetcher.calls != 0 {
			t.Errorf("the fetcher was consulted %d times for a written head's miss; the self-heal must not reach it", fetcher.calls)
		}
		if after := len(f.blockSet()); after != before {
			t.Errorf("gc deleted %d blocks despite failing the mark; a run that cannot read the DAG must not sweep it", before-after)
		}
	})

	t.Run("followed head miss heals", func(t *testing.T) {
		f, written, followed := setup(t)

		sealed := f.enumerate(followed).Sealed
		if len(sealed) == 0 {
			t.Fatal("the followed head sealed no segments")
		}
		missing := sealed[0].CID
		saved, err := f.bs.Get(f.ctx, missing)
		if err != nil {
			t.Fatalf("Get(%s): %v", missing, err)
		}
		if err := f.bs.DeleteBlock(f.ctx, missing); err != nil {
			t.Fatalf("DeleteBlock: %v", err)
		}

		fetcher := &fakeFetcher{store: f.bs, have: map[string]blocks.Block{missing.KeyString(): saved}}
		gc, err := pinning.NewGC(pinning.GCConfig{Blocks: f.bs, Reconciler: f.rec, Fetch: followOnly(fetcher, "followed")})
		if err != nil {
			t.Fatalf("NewGC: %v", err)
		}
		stats, err := gc.Run(f.ctx)
		if err != nil {
			t.Fatalf("a followed head's miss should heal, but the run failed: %v", err)
		}
		if stats.Refetched != 1 {
			t.Errorf("gc refetched %d blocks, want 1 (the deleted followed-head segment)", stats.Refetched)
		}
		if fetcher.calls != 1 {
			t.Errorf("the fetcher was called %d times, want 1", fetcher.calls)
		}
		if has, _ := f.bs.Has(f.ctx, missing); !has {
			t.Error("the healed block is not back in the store; a self-heal that does not restore it re-wedges next run")
		}
		// Both heads' indexes survive: the followed head's on its healed block, the
		// written head's on its own intact one.
		f.expect().index(written).index(followed).blobs(1, 2, 3, 4).check()
	})
}

// TestGCSharedIndexBlockFailsClosedUnderWrittenHead pins the walk-order behavior
// of the mixed-node self-heal (spec 9). An index block shared by multihash
// between a written head and a followed head, missing locally, is verified under
// the written head's fail-closed rule -- not silently refetched -- because the
// mark walks every written head before any followed one. The written head's own
// store has lost a block it pins, and that the block also lives under a followed
// head does not make the loss any less real; healing it through the followed
// head's walk first would launder the miss, which the ordering forbids.
func TestGCSharedIndexBlockFailsClosedUnderWrittenHead(t *testing.T) {
	f := newFixture(t, withStableCollectionGeneration())
	written := f.head("written", pinning.Full())
	followed := f.head("followed", pinning.Full())

	// Both heads seal an identical segment 0 (same Slot0, same single row over
	// blob 1), so it is one block by CID reached from both roots; they diverge
	// only in a later segment (blob 2 vs blob 3), so the shared block is a real
	// index node with a link, not a leaf.
	f.apply(written, 11, f.row(8, 1))
	f.apply(written, 20, f.row(12, 2))
	f.apply(followed, 11, f.row(8, 1))
	f.apply(followed, 20, f.row(12, 3))
	f.reconcileAll()

	inWritten := map[cid.Cid]bool{}
	for _, s := range f.enumerate(written).Sealed {
		inWritten[s.CID] = true
	}
	shared := cid.Undef
	for _, s := range f.enumerate(followed).Sealed {
		if inWritten[s.CID] {
			shared = s.CID
			break
		}
	}
	if !shared.Defined() {
		t.Fatal("the two heads share no sealed segment; the fixture is meant to build one")
	}

	saved, err := f.bs.Get(f.ctx, shared)
	if err != nil {
		t.Fatalf("Get(%s): %v", shared, err)
	}
	if err := f.bs.DeleteBlock(f.ctx, shared); err != nil {
		t.Fatalf("DeleteBlock: %v", err)
	}

	// The fetcher can supply the shared block, and the followed head's walk would
	// heal it -- but the written head is walked first and must fail closed on it.
	fetcher := &fakeFetcher{store: f.bs, have: map[string]blocks.Block{shared.KeyString(): saved}}
	gc, err := pinning.NewGC(pinning.GCConfig{Blocks: f.bs, Reconciler: f.rec, Fetch: followOnly(fetcher, "followed")})
	if err != nil {
		t.Fatalf("NewGC: %v", err)
	}

	before := len(f.blockSet())
	stats, err := gc.Run(f.ctx)
	if err == nil {
		t.Fatal("gc.Run: a block shared with a followed head but missing under the written head must fail closed, got nil")
	}
	if !strings.Contains(err.Error(), "written") || !strings.Contains(err.Error(), shared.String()) {
		t.Errorf("error %q does not name the written head and the shared block", err)
	}
	if stats.Refetched != 0 {
		t.Errorf("gc refetched %d blocks; a shared block missing under the written head must not be laundered into a heal", stats.Refetched)
	}
	if fetcher.calls != 0 {
		t.Errorf("the fetcher was consulted %d times; the written head's walk must fail before any followed-head fetch", fetcher.calls)
	}
	if after := len(f.blockSet()); after != before {
		t.Errorf("gc deleted %d blocks despite failing the mark; it must sweep nothing", before-after)
	}
}

// TestGCSelfHealsADanglingPin is spec 9's follower self-heal: a block a
// recursive pin reaches is absent from the store, and a fetch-capable node
// fetches it back and completes the run rather than failing closed on it. This
// is what un-wedges a follower whose fetch window left the dangling pin.
func TestGCSelfHealsADanglingPin(t *testing.T) {
	f := newFixture(t, withStableCollectionGeneration())
	full := f.head("full", pinning.Full())
	f.apply(full, 11, f.row(8, 1))
	f.apply(full, 20, f.row(12, 2))
	f.reconcileAll()

	// A sealed segment, reachable from the recursive root pin and not read by
	// the reconcile flush (which walks pages, not segments), so the mark is the
	// first thing to meet its absence. Saved before it is deleted, so the
	// fetcher can offer it back.
	sealed := f.enumerate(full).Sealed
	if len(sealed) == 0 {
		t.Fatal("the fixture sealed no segments")
	}
	missing := sealed[0].CID
	saved, err := f.bs.Get(f.ctx, missing)
	if err != nil {
		t.Fatalf("Get(%s): %v", missing, err)
	}
	if err := f.bs.DeleteBlock(f.ctx, missing); err != nil {
		t.Fatalf("DeleteBlock: %v", err)
	}

	fetcher := &fakeFetcher{store: f.bs, have: map[string]blocks.Block{missing.KeyString(): saved}}
	gc, err := pinning.NewGC(pinning.GCConfig{Blocks: f.bs, Reconciler: f.rec, Fetch: followAll(fetcher)})
	if err != nil {
		t.Fatalf("NewGC: %v", err)
	}
	stats, err := gc.Run(f.ctx)
	if err != nil {
		t.Fatalf("a self-healing GC failed on a block it could refetch: %v", err)
	}
	if stats.Refetched != 1 {
		t.Errorf("gc refetched %d blocks, want 1 (the deleted segment)", stats.Refetched)
	}
	if fetcher.calls != 1 {
		t.Errorf("the fetcher was called %d times, want 1", fetcher.calls)
	}
	if has, _ := f.bs.Has(f.ctx, missing); !has {
		t.Error("the healed block is not back in the store; a self-heal that does not restore it re-wedges next run")
	}
	// The run actually completed: the whole index is kept and the blobs with it.
	f.expect().index(full).blobs(1, 2).check()

	// And a second run is clean: the block is back, so there is nothing to heal.
	second, err := gc.Run(f.ctx)
	if err != nil {
		t.Fatalf("the second GC failed: %v", err)
	}
	if second.Refetched != 0 {
		t.Errorf("the second gc refetched %d blocks, want 0: the first run restored the block", second.Refetched)
	}
}

// TestGCSelfHealFailureFailsClosed: if the missing block cannot be fetched
// either, the run fails exactly as a fetcher-less one would, and sweeps nothing.
// A follower whose writer is unreachable is no worse off than before the heal
// existed, and its failure is loud (the error, and gc_runs_total{outcome=error}).
func TestGCSelfHealFailureFailsClosed(t *testing.T) {
	f := newFixture(t, withStableCollectionGeneration())
	full := f.head("full", pinning.Full())
	f.apply(full, 11, f.row(8, 1))
	f.apply(full, 20, f.row(12, 2))
	f.reconcileAll()

	sealed := f.enumerate(full).Sealed
	if len(sealed) == 0 {
		t.Fatal("the fixture sealed no segments")
	}
	if err := f.bs.DeleteBlock(f.ctx, sealed[0].CID); err != nil {
		t.Fatalf("DeleteBlock: %v", err)
	}

	before := len(f.blockSet())
	fetcher := &fakeFetcher{store: f.bs, fail: true}
	gc, err := pinning.NewGC(pinning.GCConfig{Blocks: f.bs, Reconciler: f.rec, Fetch: followAll(fetcher)})
	if err != nil {
		t.Fatalf("NewGC: %v", err)
	}
	stats, err := gc.Run(f.ctx)
	if err == nil {
		t.Fatal("gc.Run: want an error when the missing block cannot be refetched, got nil")
	}
	if stats.Refetched != 0 {
		t.Errorf("a failed heal counted %d refetches, want 0: a fetch that errored healed nothing", stats.Refetched)
	}
	if after := len(f.blockSet()); after != before {
		t.Errorf("gc deleted %d blocks after a mark it could not complete; it must sweep nothing", before-after)
	}
}

// TestGCExcludesReconcile is spec 9's "MUST NOT run concurrently". The sweep is
// held open by a blockstore that blocks; a reconcile pass started meanwhile must
// not proceed until the sweep is done.
func TestGCExcludesReconcile(t *testing.T) {
	f := newFixture(t)
	full := f.head("full", pinning.Full())
	f.apply(full, 11, f.row(8, 1))
	f.reconcileAll()

	release := make(chan struct{})
	entered := make(chan struct{})
	blocking := &blockingStore{Blockstore: f.bs, release: release, entered: entered}
	gc, err := pinning.NewGC(pinning.GCConfig{Blocks: blocking, Reconciler: f.rec})
	if err != nil {
		t.Fatalf("NewGC: %v", err)
	}

	gcDone := make(chan error, 1)
	go func() {
		_, err := gc.Run(context.Background())
		gcDone <- err
	}()
	<-entered // the sweep is in progress and the gate is held.

	recDone := make(chan struct{})
	go func() {
		defer close(recDone)
		if _, err := f.rec.ReconcileAll(context.Background()); err != nil {
			t.Errorf("ReconcileAll: %v", err)
		}
	}()

	select {
	case <-recDone:
		t.Fatal("a reconcile pass ran while gc was sweeping; spec 9 forbids the two overlapping")
	case <-time.After(50 * time.Millisecond):
	}

	close(release)
	if err := <-gcDone; err != nil {
		t.Fatalf("gc.Run: %v", err)
	}
	select {
	case <-recDone:
	case <-time.After(5 * time.Second):
		t.Fatal("the reconcile pass never ran after gc finished; the gate did not release")
	}
}

// TestOnlineGCReleasesTheGateAfterItsCut is the behavioral difference from the
// legacy compatibility path above: once pins are snapshotted and the epoch is
// active, a long sweep must not stop reconciliation (or any other gated writer).
func TestOnlineGCReleasesTheGateAfterItsCut(t *testing.T) {
	f := newFixture(t)
	full := f.head("full", pinning.Full())
	f.apply(full, 11, f.row(8, 1))
	f.reconcileAll()

	release := make(chan struct{})
	entered := make(chan struct{})
	blocking := &blockingStore{Blockstore: f.bs, release: release, entered: entered}
	epochs := testEpochs(store.Validating(blocking))
	gc, err := pinning.NewGC(pinning.GCConfig{Epochs: epochs, Reconciler: f.rec, SeparateScrub: true})
	if err != nil {
		t.Fatalf("NewGC: %v", err)
	}

	gcDone := make(chan error, 1)
	go func() {
		_, err := gc.Run(context.Background())
		gcDone <- err
	}()
	<-entered // sweep enumeration is blocked, after the pin cut and epoch Begin.

	recDone := make(chan error, 1)
	go func() {
		_, err := f.rec.ReconcileAll(context.Background())
		recDone <- err
	}()
	select {
	case err := <-recDone:
		if err != nil {
			t.Fatalf("ReconcileAll during online sweep: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("reconciliation remained blocked during online sweep; the long writer pause was not removed")
	}

	close(release)
	if err := <-gcDone; err != nil {
		t.Fatalf("gc.Run: %v", err)
	}
}

func TestNewGCRejectsEpochsWithoutCompleteEnumeration(t *testing.T) {
	f := newFixture(t)
	epochs := store.NewBlockstoreEpochs(store.Validating(f.bs))
	if epochs.CompleteEnumeration() {
		t.Fatal("generic epoch coordinator unexpectedly advertises complete enumeration")
	}
	_, err := pinning.NewGC(pinning.GCConfig{Epochs: epochs, Reconciler: f.rec})
	if err == nil || !strings.Contains(err.Error(), "complete error-preserving block enumeration") {
		t.Fatalf("NewGC with incomplete enumeration = %v, want fail-closed error", err)
	}
}

// TestOnlineGCProtectsAnApplicationTouch proves the M union T rule at the GC
// boundary, not only in the store coordinator's focused tests. The blob is an
// orphan at T0, but an application Get during the epoch protects its multihash,
// so this sweep retains it. With no touch in the next epoch, it is collected.
func TestOnlineGCProtectsAnApplicationTouch(t *testing.T) {
	f := newFixture(t)
	full := f.head("full", pinning.Full())
	f.apply(full, 11, f.row(8, 1))
	f.reconcileAll()
	f.putBlob(99)
	orphan := f.blobCID(99)

	release := make(chan struct{})
	entered := make(chan struct{})
	blocking := &blockingStore{Blockstore: f.bs, release: release, entered: entered}
	epochs := testEpochs(store.Validating(blocking))
	gc, err := pinning.NewGC(pinning.GCConfig{Epochs: epochs, Reconciler: f.rec, SeparateScrub: true})
	if err != nil {
		t.Fatalf("NewGC: %v", err)
	}

	type result struct {
		stats pinning.GCStats
		err   error
	}
	done := make(chan result, 1)
	go func() {
		stats, err := gc.Run(context.Background())
		done <- result{stats: stats, err: err}
	}()
	<-entered
	if _, err := epochs.Application().Get(f.ctx, orphan); err != nil {
		t.Fatalf("application Get during epoch: %v", err)
	}
	close(release)
	first := <-done
	if first.err != nil {
		t.Fatalf("first gc.Run: %v", first.err)
	}
	if first.stats.Protected != 1 || first.stats.ProtectedSkips != 1 {
		t.Fatalf("first protection stats = distinct %d skips %d, want 1/1",
			first.stats.Protected, first.stats.ProtectedSkips)
	}
	if present, err := f.bs.Has(f.ctx, orphan); err != nil || !present {
		t.Fatalf("touched orphan after first sweep: present=%t err=%v", present, err)
	}

	second, err := gc.Run(f.ctx)
	if err != nil {
		t.Fatalf("second gc.Run: %v", err)
	}
	if second.Protected != 0 || second.ProtectedSkips != 0 {
		t.Fatalf("second protection stats = distinct %d skips %d, want 0/0",
			second.Protected, second.ProtectedSkips)
	}
	if present, err := f.bs.Has(f.ctx, orphan); err != nil {
		t.Fatalf("Has orphan after second sweep: %v", err)
	} else if present {
		t.Fatal("orphan survived an epoch in which no application operation protected it")
	}
}

// TestOnlineGCRawPresenceAndScrubSplit pins the performance/integrity split.
// Reachability GC does not hash a retained raw blob and therefore stays online;
// the full scrub still detects the same on-disk corruption.
func TestOnlineGCRawPresenceAndScrubSplit(t *testing.T) {
	f := newFixture(t)
	full := f.head("full", pinning.Full())
	f.apply(full, 11, f.row(8, 1))
	f.reconcileAll()
	c := f.blobCID(1)
	if err := f.bs.DeleteBlock(f.ctx, c); err != nil {
		t.Fatalf("deleting honest blob: %v", err)
	}
	bad, err := blocks.NewBlockWithCid([]byte("corrupt bytes under the retained blob key"), c)
	if err != nil {
		t.Fatalf("framing corrupt block: %v", err)
	}
	if err := f.bs.Put(f.ctx, bad); err != nil {
		t.Fatalf("storing corrupt block: %v", err)
	}

	epochs := testEpochs(store.Validating(f.bs))
	gc, err := pinning.NewGC(pinning.GCConfig{
		Epochs: epochs, Reconciler: f.rec, SeparateScrub: true,
	})
	if err != nil {
		t.Fatalf("NewGC: %v", err)
	}
	stats, err := gc.Run(f.ctx)
	if err != nil {
		t.Fatalf("online reachability GC unexpectedly hashed the corrupt raw leaf: %v", err)
	}
	if stats.ValidatedRawReads != 0 || stats.ValidatedRawBytes != 0 {
		t.Fatalf("online GC validated raw reads/bytes = %d/%d, want 0/0",
			stats.ValidatedRawReads, stats.ValidatedRawBytes)
	}
	if _, err := gc.Scrub(f.ctx); !errors.Is(err, store.ErrCorruptBlock) {
		t.Fatalf("Scrub corrupt raw block: got %v, want ErrCorruptBlock", err)
	}
}

func TestOnlineGCAndScrubReportCancelledEnumeration(t *testing.T) {
	for _, tc := range []struct {
		name string
		run  func(context.Context, *pinning.GC) error
	}{
		{name: "gc", run: func(ctx context.Context, gc *pinning.GC) error { _, err := gc.Run(ctx); return err }},
		{name: "scrub", run: func(ctx context.Context, gc *pinning.GC) error { _, err := gc.Scrub(ctx); return err }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			entered := make(chan struct{})
			base := &cancelEnumerationStore{Blockstore: f.bs, entered: entered}
			epochs := testEpochs(store.Validating(base))
			gc, err := pinning.NewGC(pinning.GCConfig{Epochs: epochs, Reconciler: f.rec, SeparateScrub: true})
			if err != nil {
				t.Fatalf("NewGC: %v", err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() { done <- tc.run(ctx, gc) }()
			<-entered
			cancel()
			if err := <-done; !errors.Is(err, context.Canceled) {
				t.Fatalf("cancelled %s = %v, want context.Canceled", tc.name, err)
			}
		})
	}
}

// TestGateExcludesMutation is the other half: a writer inside the gate holds GC
// off until it leaves. This is what the daemon wraps its mutating requests in,
// so that a sweep cannot start between a mutation's block writes and its root
// swap.
func TestGateExcludesMutation(t *testing.T) {
	f := newFixture(t)
	f.head("full", pinning.Full())

	gate := f.rec.Gate()
	gate.Enter()

	done := make(chan struct{})
	go func() {
		defer close(done)
		if _, err := f.gc.Run(context.Background()); err != nil {
			t.Errorf("gc.Run: %v", err)
		}
	}()

	select {
	case <-done:
		t.Fatal("gc ran while a writer was inside the gate")
	case <-time.After(50 * time.Millisecond):
	}

	gate.Leave()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("gc never ran after the writer left the gate")
	}
}

// blockingStore holds the sweep open at AllKeysChan.
type blockingStore struct {
	blockstore.Blockstore
	entered chan struct{}
	release chan struct{}
	once    bool
}

type cancelEnumerationStore struct {
	blockstore.Blockstore
	entered chan struct{}
}

// testEpochs supplies the in-memory/blocking fixtures with an explicit iterator
// contract. These stores have no asynchronous query-error path; production gets
// its error-preserving iterator directly from FlatFS in store.Open.
func testEpochs(bs blockstore.Blockstore) *store.BlockstoreEpochs {
	return store.NewBlockstoreEpochs(bs, store.WithKeyIterator(func(ctx context.Context) (<-chan cid.Cid, <-chan error, error) {
		keys, err := bs.AllKeysChan(ctx)
		if err != nil {
			return nil, nil, err
		}
		errs := make(chan error)
		close(errs)
		return keys, errs, nil
	}))
}

func (s *cancelEnumerationStore) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	out := make(chan cid.Cid)
	close(s.entered)
	go func() {
		<-ctx.Done()
		close(out)
	}()
	return out, nil
}

func (b *blockingStore) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	if !b.once {
		b.once = true
		close(b.entered)
		<-b.release
	}
	return b.Blockstore.AllKeysChan(ctx)
}

func hasPin(entries []catalog.PinEntry, c cid.Cid) bool {
	for _, e := range entries {
		if e.CID == c {
			return true
		}
	}
	return false
}
