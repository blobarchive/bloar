package follow_test

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/boxo/exchange"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"

	"github.com/blobarchive/bloar/follow"
	"github.com/blobarchive/bloar/p2p"
)

// These tests are about the fetch pass's memo (follow/sync.go's walked): the
// record of which index subtrees a follower has already made local, which is
// what keeps a pass after a batch from re-walking the whole archive. The memo is
// only sound if a subtree is recorded done once it is known complete, and the
// bug they pin down is a pass that recorded ancestors done the moment their
// children were listed -- so a pass that then died mid-walk left the root marked
// done with blobs still missing, and the next pass took the head for synced
// without fetching them.
//
// The seam is Config.Sessions: a wrapper that fails one CID's bitswap fetch on
// demand injects "a block did not arrive" (spec 11.4) exactly where the real
// fetch would raise it, and counting the fetches that do go out is how a test
// tells a pass that re-walked from one that short-circuited.

// faultySessions wraps a SessionSource to fail the fetch of one chosen CID until
// it is cleared, and to count the fetches that succeed.
type faultySessions struct {
	inner p2p.SessionSource

	mu      sync.Mutex
	fail    cid.Cid // cid.Undef fails nothing.
	fetched int
}

func (s *faultySessions) NewSession(ctx context.Context) exchange.Fetcher {
	return &faultyFetcher{inner: s.inner.NewSession(ctx), s: s}
}

func (s *faultySessions) failOn(c cid.Cid) { s.mu.Lock(); s.fail = c; s.mu.Unlock() }
func (s *faultySessions) clear()           { s.mu.Lock(); s.fail = cid.Undef; s.mu.Unlock() }
func (s *faultySessions) resetCount()      { s.mu.Lock(); s.fetched = 0; s.mu.Unlock() }
func (s *faultySessions) count() int       { s.mu.Lock(); defer s.mu.Unlock(); return s.fetched }

type faultyFetcher struct {
	inner exchange.Fetcher
	s     *faultySessions
}

func (f *faultyFetcher) GetBlock(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	f.s.mu.Lock()
	fail := f.s.fail.Defined() && f.s.fail == c
	f.s.mu.Unlock()
	if fail {
		return nil, fmt.Errorf("injected fetch failure for %s", c)
	}
	blk, err := f.inner.GetBlock(ctx, c)
	if err != nil {
		return nil, err
	}
	f.s.mu.Lock()
	f.s.fetched++
	f.s.mu.Unlock()
	return blk, nil
}

// GetBlocks is here only to satisfy exchange.Fetcher: the fetch pass fetches one
// block at a time (fetchingBlockstore.Get -> GetBlock), so nothing under test
// takes this path.
func (f *faultyFetcher) GetBlocks(ctx context.Context, ks []cid.Cid) (<-chan blocks.Block, error) {
	return f.inner.GetBlocks(ctx, ks)
}

// countingBlocks wraps a blockstore to record how many times each CID is read
// with Get. The fetch pass reads an index block with Get exactly when it expands
// it (walk.block), and Enumerate reads dir pages but never a sealed segment, so a
// sealed segment's Get count over a pass is whether that pass re-expanded it or
// the memo stopped the walk at it.
type countingBlocks struct {
	blockstore.Blockstore
	mu   sync.Mutex
	gets map[string]int
}

// Preserve the production store's optional online-GC capabilities. Wrapping a
// blockstore without forwarding these would deliberately select follow's
// conservative legacy path, which disables the cross-root memo because it has
// no generation token with which to invalidate it.
func (b *countingBlocks) CollectionGeneration() uint64 {
	return b.Blockstore.(interface{ CollectionGeneration() uint64 }).CollectionGeneration()
}

func (b *countingBlocks) ActiveEpoch() uint64 {
	return b.Blockstore.(interface{ ActiveEpoch() uint64 }).ActiveEpoch()
}

func (b *countingBlocks) Get(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	b.mu.Lock()
	b.gets[c.KeyString()]++
	b.mu.Unlock()
	return b.Blockstore.Get(ctx, c)
}

func (b *countingBlocks) getCount(c cid.Cid) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.gets[c.KeyString()]
}

func (b *countingBlocks) reset() {
	b.mu.Lock()
	b.gets = map[string]int{}
	b.mu.Unlock()
}

// TestFailedFetchPassIsRewalkedNextPass is the regression: a pass that dies
// partway through the DAG must leave nothing behind that stops the next pass from
// finishing the walk. Before the fix the walk recorded the root done as soon as
// its children were listed, so a mid-walk failure left the root marked with a
// blob still missing, and the following pass -- reasoning off that memo -- fetched
// nothing and served the head with a hole in it.
func TestFailedFetchPassIsRewalkedNextPass(t *testing.T) {
	w := newWriter(t)
	fx := archiveWindows(t, w)

	var sess *faultySessions
	f := newFollower(t, w, func(c *follow.Config) {
		sess = &faultySessions{inner: c.Sessions}
		c.Sessions = sess
	})

	// A blob deep under the root. The full-policy walk marks the root and the dir
	// pages above this blob before it ever reaches it, which is what made the old
	// memo record them done while the pass then failed here.
	missing := fx.cids[113]
	sess.failOn(missing)
	if err := f.pollErr(); err == nil {
		t.Fatal("a fetch pass whose block fetch failed returned no error")
	}
	if f.hasLocally(missing) {
		t.Fatal("the blob whose fetch was made to fail is somehow local")
	}

	// The fault clears. The next pass must walk the head again and fetch what the
	// first pass could not, rather than take the head for synced on a memo the
	// failed pass had no business leaving behind.
	sess.clear()
	sess.resetCount()
	f.poll()

	if !f.hasLocally(missing) {
		t.Error("the pass after the failure did not re-fetch the block the failure skipped; " +
			"the head is being served with a hole in it")
	}
	if sess.count() == 0 {
		t.Error("the pass after the failure fetched nothing: it short-circuited on the failed pass's marks " +
			"instead of re-walking from the root")
	}
}

// TestFailedPassNeverMarksHeadSynced holds the fault across two passes. A pass
// that gave up on a memo the first failure left behind would walk nothing the
// second time, find it fetched nothing, and quietly mark the head synced --
// returning no error while the blob is still missing. Both passes must instead
// fail on the block that is failing, and neither may announce the head synced.
func TestFailedPassNeverMarksHeadSynced(t *testing.T) {
	w := newWriter(t)
	fx := archiveWindows(t, w)

	var l logs
	var sess *faultySessions
	f := newFollower(t, w, func(c *follow.Config) {
		sess = &faultySessions{inner: c.Sessions}
		c.Sessions = sess
		c.Logger = capturingLogger(t, &l)
	})

	sess.failOn(fx.cids[113])
	if err := f.pollErr(); err == nil {
		t.Fatal("the first failing pass returned no error")
	}
	if err := f.pollErr(); err == nil {
		t.Fatal("the second failing pass returned no error; the head was taken for synced on the first pass's marks")
	}
	if l.has("followed head synced") {
		t.Error("a pass logged the head synced while a block fetch was failing")
	}
	if f.hasLocally(fx.cids[113]) {
		t.Error("the failing blob is present despite every fetch of it failing")
	}
}

// TestSecondPassOverUnchangedRootFetchesNothing is the other half of the memo's
// job: once a pass has completed, a pass over the same root must not re-fetch. It
// is the guard that the fix's buffering did not turn every poll into a re-walk.
func TestSecondPassOverUnchangedRootFetchesNothing(t *testing.T) {
	w := newWriter(t)
	archiveWindows(t, w)

	var sess *faultySessions
	f := newFollower(t, w, func(c *follow.Config) {
		sess = &faultySessions{inner: c.Sessions}
		c.Sessions = sess
	})

	f.poll() // fetches the whole head under the full policy.

	sess.resetCount()
	f.poll() // the root has not moved.

	if n := sess.count(); n != 0 {
		t.Errorf("a second pass over an unchanged root fetched %d blocks, want none", n)
	}
}

// TestMemoSkipsSharedSubtreeAcrossRoots is the memo earning its keep: when the
// root moves within one collection generation, neither structural admission nor
// the fetch pass must re-read segments the batch did not touch. They keep their
// CIDs across roots (spec 5), so the generation-bound structure proof and fetch
// memo both stop at them -- which is what makes a pass after a batch cost the
// changed spine rather than the whole archive.
func TestMemoSkipsSharedSubtreeAcrossRoots(t *testing.T) {
	w := newWriter(t)
	archiveWindows(t, w)

	var counter *countingBlocks
	var sess *faultySessions
	f := newFollower(t, w, func(c *follow.Config) {
		counter = &countingBlocks{Blockstore: c.Local, gets: map[string]int{}}
		c.Local = counter
		sess = &faultySessions{inner: c.Sessions}
		c.Sessions = sess
	})

	f.poll() // pass 1: walks and marks the whole head, every sealed segment included.

	// A sealed segment the next batch will not touch: its content does not change
	// when a later window is written, so it keeps its CID and is shared across the
	// two roots.
	enum, err := w.head.Enumerate(t.Context())
	if err != nil {
		t.Fatalf("Enumerate: %v", err)
	}
	if len(enum.Sealed) == 0 {
		t.Fatal("the fixture sealed no segments")
	}
	shared := enum.Sealed[0].CID
	if !f.hasLocally(shared) {
		t.Fatalf("the first pass did not fetch the sealed segment %s", shared)
	}

	// The writer opens a new window, moving the root. The new root shares every
	// segment the batch did not touch, the one above among them.
	w.ingestSlot(137, 9000)

	counter.reset()
	sess.resetCount()
	f.poll() // pass 2: over the moved root.

	if n := counter.getCount(shared); n != 0 {
		t.Errorf("the pass over the moved root read the shared segment %s %d times; unchanged-generation "+
			"structure and fetch memos should both have stopped at it", shared, n)
	}
	if !f.hasLocally(blobCID(t, makeBlob(9000))) {
		t.Error("the pass over the moved root did not fetch the blob of the window it just adopted")
	}
	if sess.count() == 0 {
		t.Error("the pass over the moved root fetched nothing; it had a new window's blocks to fetch")
	}
}
