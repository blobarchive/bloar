package follow_test

import (
	"testing"

	"github.com/blobarchive/bloar/pinning"
)

// This file is the follower half of spec 9's window (a), end to end over a real
// writer and a real follower: the fetch pass stages what it makes durable and
// drops it when the head's pins take over, and a follower's GC self-heals a pin
// left dangling instead of wedging.

// TestFetchPassStagesThenDropsItsPins checks the handoff. The pass takes a
// staging pin on every block it fetches -- so a GC in the fetch-then-pin gap
// cannot sweep it -- and drops those pins once it finishes, because the adopted
// root is registered and a GC reconciles every head before it marks, so the
// head's own pins retain the blocks from then on. The tell is that the pass
// leaves no staging rows behind and a reconcile-and-GC keeps everything it
// fetched.
func TestFetchPassStagesThenDropsItsPins(t *testing.T) {
	w := newWriter(t)
	w.ingestSlot(testOrigin, 1, 2)

	f := newFollower(t, w)
	if idx, blobs := f.countBlocks(); idx != 0 || blobs != 0 {
		t.Fatalf("the follower holds %d index blocks and %d blobs before its first poll, want none", idx, blobs)
	}

	f.poll()

	// The pass fetched the head's DAG...
	idx, blobs := f.countBlocks()
	if idx == 0 || blobs != 2 {
		t.Fatalf("after a poll the follower holds %d index blocks and %d blobs, want a full index and 2 blobs", idx, blobs)
	}
	// ...and dropped every staging pin it took on the way through it.
	rows, err := f.staging.List(f.t.Context())
	if err != nil {
		t.Fatalf("staging.List: %v", err)
	}
	if len(rows) != 0 {
		t.Errorf("the fetch pass left %d staging rows behind, want 0: it drops them once the head's pins retain the blocks", len(rows))
	}

	// A reconcile-and-GC keeps everything, on the head's own pins now: nothing
	// swept, no wedge.
	f.reconcile()
	if stats := f.gc(); stats.Swept != 0 {
		t.Errorf("GC swept %d blocks after a clean poll, want 0", stats.Swept)
	}
	if idx, blobs := f.countBlocks(); idx == 0 || blobs != 2 {
		t.Errorf("after GC the follower holds %d index blocks and %d blobs, want a full index and 2 blobs", idx, blobs)
	}
}

// TestGCSelfHealsAFollowerDanglingPin is the follower self-heal end to end. A
// block the follower's pins reach is deleted, and its GC -- which, being a
// follower's, carries a fetcher (its own fetching path) -- fetches the block
// back from the writer over bitswap and completes, where a writer's fail-closed
// GC would have wedged on it. This is what un-wedges a deployment the fetch
// window already broke.
func TestGCSelfHealsAFollowerDanglingPin(t *testing.T) {
	w := newWriter(t)
	archiveWindows(t, w) // seals three segments; a full follower pins them all

	f := newFollower(t, w) // full policy: one recursive root pin over the whole DAG
	f.poll()
	f.reconcile()

	// A sealed segment: reachable from the recursive root pin and backed by a
	// current-generation structure proof, so the reconcile flush need not read
	// it again. The mark is the first thing to meet its absence.
	head, ok := f.heads.Get(testHead)
	if !ok {
		t.Fatal("the follower does not serve the head it adopted")
	}
	enum, err := head.Enumerate(f.t.Context())
	if err != nil {
		t.Fatalf("Enumerate: %v", err)
	}
	if len(enum.Sealed) == 0 {
		t.Fatal("the fixture sealed no segments")
	}
	missing := enum.Sealed[0].CID
	if !f.hasLocally(missing) {
		t.Fatalf("the follower never fetched the segment %s it is supposed to pin", missing)
	}
	if err := f.store.Blocks().DeleteBlock(f.t.Context(), missing); err != nil {
		t.Fatalf("DeleteBlock: %v", err)
	}

	// The follower's GC carries its fetching path, so the mark fetches the block
	// back rather than failing on the dangling pin.
	gc, err := pinning.NewGC(pinning.GCConfig{
		Blocks:     f.store.Blocks(),
		Reconciler: f.rec,
		Fetch:      f.f.GCFetch(),
	})
	if err != nil {
		t.Fatalf("NewGC: %v", err)
	}
	stats, err := gc.Run(f.t.Context())
	if err != nil {
		t.Fatalf("a follower's GC failed on a block it could refetch: %v", err)
	}
	if stats.Refetched != 1 {
		t.Errorf("GC refetched %d blocks, want 1 (the deleted segment)", stats.Refetched)
	}
	if !f.hasLocally(missing) {
		t.Error("the healed segment is not back in the follower's store")
	}
}
