package archive_test

import (
	"context"
	"testing"

	"github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/go-cid"

	"github.com/blobarchive/bloar/archive"
	"github.com/blobarchive/bloar/schema"
)

// readHead decodes the Head block at c straight from the blockstore, so a test
// can assert on the DAG rather than on what the engine says about it.
func readHead(t *testing.T, bs blockstore.Blockstore, c cid.Cid) *schema.Head {
	t.Helper()
	blk, err := bs.Get(context.Background(), c)
	if err != nil {
		t.Fatalf("reading head %s: %v", c, err)
	}
	h, err := schema.DecodeHead(blk.RawData())
	if err != nil {
		t.Fatalf("decoding head %s: %v", c, err)
	}
	return h
}

func readDir(t *testing.T, bs blockstore.Blockstore, c cid.Cid) *schema.DirNode {
	t.Helper()
	blk, err := bs.Get(context.Background(), c)
	if err != nil {
		t.Fatalf("reading dirnode %s: %v", c, err)
	}
	d, err := schema.DecodeDirNode(blk.RawData())
	if err != nil {
		t.Fatalf("decoding dirnode %s: %v", c, err)
	}
	return d
}

// TestLookupVHsFilter: a filtered lookup answers in request order, one blob per
// requested vh (spec 7.1).
func TestLookupVHsFilter(t *testing.T) {
	hs := newHarness(t, testParams())
	hs.apply([]archive.RefRow{hs.row(41, 1, 2, 3)}, 44)

	wantBlobs(t, hs.lookupVHs(41, 3, 1), "request order 3,1", 3, 1)
	wantBlobs(t, hs.lookupVHs(41, 2), "single vh", 2)
	wantBlobs(t, hs.lookupVHs(41, 1, 2, 3), "all three in stored order", 1, 2, 3)
	// The same vh twice is one blob per request entry, not a dedup: the caller
	// asked for two and indexes the answer positionally.
	wantBlobs(t, hs.lookupVHs(41, 2, 2), "repeated vh", 2, 2)
}

// TestLookupVHsMissing: one absent vh fails the whole request and names the
// first offender (spec 7.1). A caller that asked for N blobs cannot use N-1.
func TestLookupVHsMissing(t *testing.T) {
	hs := newHarness(t, testParams())
	hs.apply([]archive.RefRow{hs.row(41, 1, 2)}, 44)

	res := hs.lookupVHs(41, 1, 99, 2)
	wantStatus(t, res, archive.StatusAbsent, "filtered lookup with one absent vh")
	if res.Entries != nil {
		t.Errorf("a failed filtered lookup returned %d entries, want none", len(res.Entries))
	}
	if res.MissingVH == nil {
		t.Fatalf("MissingVH is nil; spec 7.1 wants the missing vh named")
	}
	if want := mkVH(99); *res.MissingVH != want {
		t.Errorf("MissingVH = 0x%x, want 0x%x", res.MissingVH[:], want[:])
	}

	// A covered slot carrying nothing at all is absent for any vh.
	res = hs.lookupVHs(42, 1)
	wantStatus(t, res, archive.StatusAbsent, "filtered lookup at a covered blobless slot")
	if res.MissingVH == nil || *res.MissingVH != mkVH(1) {
		t.Errorf("MissingVH = %v, want the requested vh", res.MissingVH)
	}
}

// TestLookupUncoveredNotFiltered: coverage is checked before the filter. A slot
// past synced_to is 503 whatever vhs were asked for, not 404.
func TestLookupUncoveredNotFiltered(t *testing.T) {
	hs := newHarness(t, testParams())
	hs.apply([]archive.RefRow{hs.row(41, 1)}, 44)

	wantStatus(t, hs.lookupVHs(45, 1), archive.StatusNotYetCovered, "filtered lookup past synced_to")
	wantStatus(t, hs.lookupVHs(39, 1), archive.StatusBeforeOrigin, "filtered lookup before origin")
}

// TestLookupBeforeOriginOnEmptyHead pins down where this implementation departs
// from the spec 4 pseudocode, which tests NOT_YET_COVERED first and so would
// answer 503 + Retry-After here.
//
// A slot below origin_slot is never coming: spec 7.1 gives it a permanent 404
// ("404 if slot < origin_slot") and reserves 503 for "not yet archived". An
// empty head is the only state where the two orders disagree, and answering
// "retry" to a question whose answer can never change would have Nitro retry
// forever.
func TestLookupBeforeOriginOnEmptyHead(t *testing.T) {
	hs := newHarness(t, testParams())
	wantStatus(t, hs.lookup(testOrigin-1), archive.StatusBeforeOrigin, "below origin on an empty head")
	wantStatus(t, hs.lookup(0), archive.StatusBeforeOrigin, "slot 0 on an empty head")
}

// TestEmptyWindowHasNoObject: spec 3.2 says a fully-empty window seals to no
// object at all. Check the DAG, not just the lookup: the directory entry must
// be a literal null.
func TestEmptyWindowHasNoObject(t *testing.T) {
	hs := newHarness(t, testParams())
	// Windows 5 and 7 carry rows; window 6 carries none. Three seals at fanout
	// 4 keep the tree at depth 1, so the root's kids are the entries.
	hs.apply([]archive.RefRow{hs.row(41, 410), hs.row(57, 570)}, 63)

	head := readHead(t, hs.bs, hs.h.Root())
	if head.DirDepth != 1 {
		t.Fatalf("dir_depth = %d, want 1", head.DirDepth)
	}
	dir := readDir(t, hs.bs, head.Dir)
	if len(dir.Kids) != 3 {
		t.Fatalf("directory has %d kids, want 3", len(dir.Kids))
	}
	if !dir.Kids[0].Defined() {
		t.Errorf("entry 0 (window 5, has rows) is null")
	}
	if dir.Kids[1].Defined() {
		t.Errorf("entry 1 (window 6, empty) links to %s; an empty window seals to no object", dir.Kids[1])
	}
	if !dir.Kids[2].Defined() {
		t.Errorf("entry 2 (window 7, has rows) is null")
	}
}

// TestLookupIsLazy: serving one slot loads the spine and one segment, nothing
// else (spec 13.3). The directory stores no keys, so a lookup that searched
// would be a lookup that read the whole tree.
func TestLookupIsLazy(t *testing.T) {
	bs := newCountingStore()
	cat := newFakeCatalog(bs)
	hs := newHarnessOver(t, testParams(), bs, cat)

	// 20 sealed windows: capacity(2) is 16 at fanout 4, so the tree is depth 3.
	for sealed := uint64(1); sealed <= 20; sealed++ {
		syncedTo := testOrigin + sealed*8 - 1
		hs.apply([]archive.RefRow{hs.row(syncedTo-1, syncedTo*10)}, syncedTo)
	}
	if got := hs.h.Info().DirDepth; got != 3 {
		t.Fatalf("dir_depth = %d, want 3", got)
	}

	// Reload so nothing is held from the writes, and read one sealed slot.
	fresh := hs.reload(t, hs.h.Root())
	bs.reset()

	slot := uint64(testOrigin + 8 - 2) // a row in the very first window
	wantBlobs(t, fresh.lookup(slot), "one sealed row", (testOrigin+8-1)*10)

	// head + 3 dir pages + 1 segment. The blob block itself is not read: a
	// lookup returns its CID, and only the server fetches bytes.
	if got, want := bs.totalGets(), 5; got != want {
		t.Errorf("lookup read %d blocks, want %d (head + 3 dir pages + 1 segment)", got, want)
	}
}

// TestLookupSnapshotIsStable: a reader holds the state it started from. A
// mutation that lands mid-read publishes a new root and leaves the old one
// intact, so the reader sees a consistent head either way -- never half of one.
func TestLookupSnapshotIsStable(t *testing.T) {
	hs := newHarness(t, testParams())
	hs.apply([]archive.RefRow{hs.row(41, 410)}, 44)
	oldRoot := hs.h.Root()

	old := hs.reload(t, oldRoot)

	hs.apply([]archive.RefRow{hs.row(45, 450)}, 47)
	if hs.h.Root() == oldRoot {
		t.Fatalf("the second batch did not change the root")
	}

	// The head pinned to the old root still answers exactly as it did.
	wantBlobs(t, old.lookup(41), "old root, slot 41", 410)
	wantStatus(t, old.lookup(45), archive.StatusNotYetCovered, "old root, slot 45")
	if synced, _ := old.h.SyncedTo(); synced != 44 {
		t.Errorf("old root synced_to = %d, want 44", synced)
	}

	// The live head sees both.
	wantBlobs(t, hs.lookup(41), "new root, slot 41", 410)
	wantBlobs(t, hs.lookup(45), "new root, slot 45", 450)
}

// TestConcurrentReadsDuringApply: readers run against a moving writer. Every
// answer must come from some published state -- a slot that reads Found must
// carry the right blob, and one that reads NotYetCovered must genuinely be past
// that reader's synced_to.
func TestConcurrentReadsDuringApply(t *testing.T) {
	hs := newHarness(t, testParams())
	hs.apply([]archive.RefRow{hs.row(41, 410)}, 44)

	const batches = 40
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := uint64(0); i < batches; i++ {
			slot := 45 + i*2
			hs.apply([]archive.RefRow{hs.row(slot, slot*10)}, slot)
		}
	}()

	ctx := context.Background()
	for range 500 {
		// Slot 41 is in every published state, so it must always read Found.
		res, err := hs.h.Lookup(ctx, 41)
		if err != nil {
			t.Errorf("Lookup(41) during apply: %v", err)
			break
		}
		if res.Status != archive.StatusFound || len(res.Entries) != 1 {
			t.Errorf("Lookup(41) during apply: status %v with %d entries, want found with 1", res.Status, len(res.Entries))
			break
		}
		// A slot the writer is walking through reads as one or the other, never
		// as a torn state: covered means the row is there.
		slot := uint64(45 + batches)
		res, err = hs.h.Lookup(ctx, slot)
		if err != nil {
			t.Errorf("Lookup(%d) during apply: %v", slot, err)
			break
		}
		if res.Status == archive.StatusFound && len(res.Entries) != 1 {
			t.Errorf("Lookup(%d) found %d entries, want 1", slot, len(res.Entries))
			break
		}
	}
	<-done

	wantBlobs(t, hs.lookup(41), "slot 41 after the writer finished", 410)
	for i := uint64(0); i < batches; i++ {
		slot := 45 + i*2
		wantBlobs(t, hs.lookup(slot), "row written under concurrent reads", slot*10)
	}
}
