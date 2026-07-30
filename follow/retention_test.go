package follow_test

import (
	"encoding/hex"
	"net/http"
	"testing"
	"time"

	"github.com/ipfs/go-cid"

	"github.com/blobarchive/bloar/follow"
	"github.com/blobarchive/bloar/pinning"
	"github.com/blobarchive/bloar/schema"
)

// The fixture these tests share: four windows of the tiny test head (8 slots
// each, from origin 96), one blob in each, and a window policy that retains the
// last one and the open one.
//
//	w12 [96,103]  blob at 97   sealed, outside the window
//	w13 [104,111] blob at 105  sealed, outside the window
//	w14 [112,119] blob at 113  sealed, inside the window
//	w15 [120,127] blob at 121  open
//
// windowSlots is chosen to put the boundary between w13 and w14: synced_to is
// 121, so a low of 113 keeps w14 (its last slot, 119, is inside) and releases
// w13 (its last slot, 111, is not).
const windowSlots = 8

type fixture struct {
	blobs map[uint64][]byte               // by slot
	vhs   map[uint64]schema.VersionedHash // by slot
	cids  map[uint64]cid.Cid              // blob CID by slot
}

// archiveWindows ingests the fixture into w.
func archiveWindows(t *testing.T, w *writer) *fixture {
	t.Helper()
	f := &fixture{
		blobs: map[uint64][]byte{},
		vhs:   map[uint64]schema.VersionedHash{},
		cids:  map[uint64]cid.Cid{},
	}
	for i, slot := range []uint64{97, 105, 113, 121} {
		blobs, vhs := w.ingestSlot(slot, uint64(i+1)*1000)
		f.blobs[slot] = blobs[0]
		f.vhs[slot] = vhs[0]
		f.cids[slot] = blobCID(t, blobs[0])
	}
	return f
}

// windowFollower follows the fixture head under a window policy that retains
// the last windowSlots slots of blobs.
func windowFollower(t *testing.T, w *writer, opts ...func(*follow.Config)) *follower {
	t.Helper()
	policy := pinning.Window(time.Duration(windowSlots*secondsPerSlot)*time.Second, secondsPerSlot)
	all := append([]func(*follow.Config){
		func(c *follow.Config) { c.Heads = map[string]pinning.Policy{testHead: policy} },
	}, opts...)
	return newFollower(t, w, all...)
}

// TestWindowPolicyFetchesOnlyItsWindow is spec 11.3's central claim: the pin
// policy decides exactly what a follower fetches, because reconciliation over a
// fetching blockstore is the sync. A window follower pulls its window's blobs
// and no others -- and pulls the entire index regardless, which is what lets it
// answer 404-vs-503 exactly like the writer while holding almost no data.
func TestWindowPolicyFetchesOnlyItsWindow(t *testing.T) {
	w := newWriter(t)
	fx := archiveWindows(t, w)

	f := windowFollower(t, w)
	f.poll()

	// The blobs the window covers, and only those.
	for _, slot := range []uint64{113, 121} {
		if !f.hasLocally(fx.cids[slot]) {
			t.Errorf("the follower did not fetch the blob at slot %d, which its window retains", slot)
		}
	}
	for _, slot := range []uint64{97, 105} {
		if f.hasLocally(fx.cids[slot]) {
			t.Errorf("the follower fetched the blob at slot %d, which is outside its window", slot)
		}
	}

	// The whole index: the root, every dir page, every sealed segment, the open
	// segment. Spec 11.3: fetched eagerly under every policy.
	enum, err := w.head.Enumerate(t.Context())
	if err != nil {
		t.Fatalf("Enumerate: %v", err)
	}
	if len(enum.Sealed) != 3 {
		t.Fatalf("the fixture sealed %d segments, want 3", len(enum.Sealed))
	}
	index := append([]cid.Cid{enum.Root, enum.Open}, enum.DirPages...)
	for _, s := range enum.Sealed {
		index = append(index, s.CID)
	}
	for _, c := range index {
		if !f.hasLocally(c) {
			t.Errorf("the follower is missing the index block %s", c)
		}
	}

	// And nothing else at all: two blobs, and the index blocks named above.
	gotIndex, gotBlobs := f.countBlocks()
	if gotBlobs != 2 {
		t.Errorf("the follower holds %d blobs, want exactly the 2 in its window", gotBlobs)
	}
	if gotIndex != len(index) {
		t.Errorf("the follower holds %d index blocks, want the %d the head has", gotIndex, len(index))
	}
}

// TestWindowFollowerAnswersLikeTheWriter: holding no blob for a slot does not
// change what the archive says about it. The index is complete, so a slot
// outside the window is a 200 (fetched on demand), a covered slot with no such
// blob is a 404, and an unarchived slot is a 503 -- the same three answers the
// writer gives.
func TestWindowFollowerAnswersLikeTheWriter(t *testing.T) {
	w := newWriter(t)
	fx := archiveWindows(t, w)

	f := windowFollower(t, w)
	f.serveHTTP(nil)
	f.poll()

	// Covered, blob outside the window: fetched on demand (spec 11.4).
	status, data, _ := f.blobsAt(97, fx.vhs[97])
	if status != http.StatusOK {
		t.Fatalf("GET slot 97 (outside the window): status = %d, want 200", status)
	}
	if data[0] != "0x"+hex.EncodeToString(fx.blobs[97]) {
		t.Error("the on-demand fetch did not return the archived bytes")
	}

	// Covered, no such blob: 404, decided by an index this node has in full.
	if status, _, _ := f.blobsAt(97, fx.vhs[113]); status != http.StatusNotFound {
		t.Errorf("GET slot 97 for a blob it does not carry: status = %d, want 404", status)
	}
	// Covered, no blobs at all at that slot: 200 with an empty list.
	if status, data, _ := f.blobsAt(98); status != http.StatusOK || len(data) != 0 {
		t.Errorf("GET a covered blobless slot: status = %d, %d blobs, want 200 and none", status, len(data))
	}
	// Not archived: 503.
	if status, _, _ := f.blobsAt(200); status != http.StatusServiceUnavailable {
		t.Errorf("GET an unarchived slot: status = %d, want 503", status)
	}
	// Before origin_slot: 404, forever.
	if status, _, _ := f.blobsAt(10); status != http.StatusNotFound {
		t.Errorf("GET a slot below origin_slot: status = %d, want 404", status)
	}
}

// TestOnDemandFetchIsCachedAndUnpinned is spec 11.4's second sentence: a blob
// fetched to answer a read is cached, so the next read is local, and unpinned,
// so GC takes it back. What a follower retains is its policy's business, not its
// clients'.
func TestOnDemandFetchIsCachedAndUnpinned(t *testing.T) {
	w := newWriter(t)
	fx := archiveWindows(t, w)

	f := windowFollower(t, w)
	f.serveHTTP(nil)
	f.poll()

	if f.hasLocally(fx.cids[97]) {
		t.Fatal("the blob at slot 97 is local before anything asked for it")
	}
	if status, _, _ := f.blobsAt(97, fx.vhs[97]); status != http.StatusOK {
		t.Fatalf("GET slot 97: status = %d, want 200", status)
	}
	if !f.hasLocally(fx.cids[97]) {
		t.Error("an on-demand fetch did not cache the blob")
	}

	// Unpinned: the sweep that keeps the window's blobs takes this one.
	f.reconcile()
	stats := f.gc()
	if stats.Swept != 1 {
		t.Errorf("GC swept %d blocks, want exactly the on-demand blob", stats.Swept)
	}
	if f.hasLocally(fx.cids[97]) {
		t.Error("GC kept a blob no policy pins")
	}
	if !f.hasLocally(fx.cids[113]) {
		t.Error("GC swept a blob the window policy pins")
	}

	// The index survives the sweep, so the head still answers -- and asking
	// again fetches it again, which is what makes an unpinned cache safe.
	status, data, _ := f.blobsAt(97, fx.vhs[97])
	if status != http.StatusOK {
		t.Fatalf("GET slot 97 after GC: status = %d, want 200", status)
	}
	if data[0] != "0x"+hex.EncodeToString(fx.blobs[97]) {
		t.Error("the re-fetched blob is not the archived bytes")
	}
}

// TestSlidingWindowReleasesBlobs: the window is a moving claim, so a segment
// that falls out of it stops being retained -- on a follower, by the same
// reconcile-and-sweep the writer uses. The block that ages out is a blob; the
// segment that referenced it stays, because the index never slides.
func TestSlidingWindowReleasesBlobs(t *testing.T) {
	w := newWriter(t)
	fx := archiveWindows(t, w)

	f := windowFollower(t, w)
	f.poll()
	f.reconcile()
	if !f.hasLocally(fx.cids[113]) {
		t.Fatal("the follower did not fetch the blob its window covers")
	}

	// The writer moves on by two windows. Slot 113's segment is now outside the
	// follower's window, and slot 121's is on its way out too.
	w.ingestSlot(137, 9000)
	f.poll()
	f.reconcile()
	f.gc()

	if f.hasLocally(fx.cids[113]) {
		t.Error("the follower still holds a blob its window has slid past")
	}
	enum, err := w.head.Enumerate(t.Context())
	if err != nil {
		t.Fatalf("Enumerate: %v", err)
	}
	for _, s := range enum.Sealed {
		if !f.hasLocally(s.CID) {
			t.Errorf("the follower swept the sealed segment %s; the index does not slide (spec 9)", s.CID)
		}
	}
	// And the new window's blob is here.
	if !f.hasLocally(blobCID(t, makeBlob(9000))) {
		t.Error("the follower did not fetch the blob of the window it just adopted")
	}
}

// TestFetchTimeoutOnAnUnreachableWriter is spec 11.4's bound: a read that misses
// locally and cannot be fetched is a 503 within follow.fetch_timeout, not a hang
// and not a 404. The 404 would be a lie -- the index says the blob exists -- and
// the hang would be a client waiting on a peer that is not coming back.
func TestFetchTimeoutOnAnUnreachableWriter(t *testing.T) {
	w := newWriter(t)
	fx := archiveWindows(t, w)

	const timeout = 300 * time.Millisecond
	f := windowFollower(t, w, func(c *follow.Config) { c.FetchTimeout = timeout })
	f.serveHTTP(nil)
	f.poll()

	// The writer goes away: the connection drops and nothing else has the
	// blocks. Its HTTP is irrelevant here -- this is the block path.
	if err := w.host.Close(); err != nil {
		t.Fatalf("closing the writer's host: %v", err)
	}

	start := time.Now()
	status, _, header := f.blobsAt(97, fx.vhs[97])
	elapsed := time.Since(start)

	if status != http.StatusServiceUnavailable {
		t.Fatalf("GET an out-of-window slot with the writer gone: status = %d, want 503", status)
	}
	if got := header.Get("Retry-After"); got != "12" {
		t.Errorf("Retry-After = %q, want 12 (spec 7.1)", got)
	}
	if got := header.Get("Cache-Control"); got != "no-store" {
		t.Errorf("Cache-Control = %q, want no-store: a cached 503 is a client that never gets the blob", got)
	}
	// The bound is the point. Generous against a loaded CI box, and still far
	// under anything that would look like a hang.
	if elapsed > 20*timeout {
		t.Errorf("the read took %s to fail, with a %s fetch timeout", elapsed, timeout)
	}

	// A slot inside the window is unaffected: it is already here.
	if status, _, _ := f.blobsAt(113, fx.vhs[113]); status != http.StatusOK {
		t.Errorf("GET an in-window slot with the writer gone: status = %d, want 200", status)
	}
}

// TestNonePolicyHoldsTheIndexAndNoBlobs: spec 9's third mode, on a follower.
// It fetches every index block and not one blob, and still answers every
// question about coverage -- which is the mode's whole purpose.
func TestNonePolicyHoldsTheIndexAndNoBlobs(t *testing.T) {
	w := newWriter(t)
	fx := archiveWindows(t, w)

	f := newFollower(t, w, func(c *follow.Config) {
		c.Heads = map[string]pinning.Policy{testHead: pinning.None()}
	})
	f.serveHTTP(nil)
	f.poll()

	_, blobs := f.countBlocks()
	if blobs != 0 {
		t.Errorf("a none-policy follower fetched %d blobs, want none", blobs)
	}
	if status, _, _ := f.blobsAt(97, fx.vhs[113]); status != http.StatusNotFound {
		t.Errorf("GET a blob the slot does not carry: status = %d, want 404 from the index alone", status)
	}
	status, data, _ := f.blobsAt(97, fx.vhs[97])
	if status != http.StatusOK {
		t.Fatalf("GET slot 97 on demand: status = %d, want 200", status)
	}
	if data[0] != "0x"+hex.EncodeToString(fx.blobs[97]) {
		t.Error("the on-demand fetch did not return the archived bytes")
	}
}
