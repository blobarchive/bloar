package beacon_test

// focused regressions for two fail-open boundaries in anchored mode. These
// deliberately use the package's end-to-end fakes so the observations include
// the real block/upstream clients, indexer, archive client, and refs POST.

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/blobarchive/bloar/index/archclient"
	"github.com/blobarchive/bloar/index/beacon"
	"github.com/blobarchive/bloar/index/upstream"
)

func TestContinuitySeedAtZeroCommitsUnanchoredLeadingMiss(t *testing.T) {
	feed := newFakeBlockFeed(t, 2)
	src := newFakeSource(t)
	a := newFakeArchive(t, "all")
	a.origin = 1

	// A partially backfilled block feed: headers 0 and 1 return 404 even though slot
	// 1 exists and carries a blob. Slot 2 is present and its parent_root names the
	// hidden slot-1 block. The seed walk reaches slot zero without an anchor -- and
	// with no checkpoint, that yields no usable seed, so the run WAITS rather than
	// bootstrapping on slot 2 and committing the hidden slot 1 as a proven miss
	//. The wait is retryable indefinitely: it never becomes absence.
	hidden := slotBlobs(1, 1)
	src.serve(1, hidden)
	feed.present(2, deriveRoot(1), nil)

	ix := newAnchoredIndexer(t, feed, a, 8, 1, nil, src)
	advanced, err := ix.Step(t.Context())
	if err != nil {
		t.Fatalf("Step errored; a zero-boundary walk with no anchor must wait, not fail: %v", err)
	}
	if advanced {
		t.Fatal("Step advanced; without an anchor the leading 404s must not commit as absence")
	}
	if got, ok := a.coverage(); ok {
		t.Fatalf("coverage advanced to %d; the unseeded zero-boundary walk must record nothing", got)
	}
	if src.wasRequested(1) {
		t.Fatal("the hidden slot's blob was requested; the run should have waited before touching a source")
	}

	// The wait persists across retries: the feed is still unbackfilled, so a second
	// pass waits again and coverage is never recorded.
	advanced, err = ix.Step(t.Context())
	if err != nil || advanced {
		t.Fatalf("second Step = (advanced %v, err %v); the wait must persist, never advance", advanced, err)
	}
	if _, ok := a.coverage(); ok {
		t.Fatal("coverage advanced on a retry; a zero-boundary wait must never become committed absence")
	}
}

// newAnchoredIndexerCheckpoint builds an anchored indexer with a configured
// continuity_checkpoint. It mirrors newAnchoredIndexer with the
// one extra Config field, since beacon.New takes the checkpoint at construction.
func newAnchoredIndexerCheckpoint(t *testing.T, feed *fakeBlockFeed, a *fakeArchive, batch uint64, cp *beacon.ContinuityCheckpoint, src *fakeSource) *beacon.Indexer {
	t.Helper()
	blocks, err := upstream.NewBlockClient(upstream.Config{BaseURL: feed.url, MaxAttempts: 1, Backoff: time.Millisecond})
	if err != nil {
		t.Fatalf("NewBlockClient: %v", err)
	}
	arch, err := archclient.New(archclient.Config{BaseURL: a.url, Token: "t", MaxAttempts: 1, Backoff: time.Millisecond})
	if err != nil {
		t.Fatalf("archclient.New: %v", err)
	}
	ix, err := beacon.New(beacon.Config{
		Sources: beaconSources(t, nil, src), Blocks: blocks, Archive: arch, Head: a.head,
		BatchSize: batch, MaxPutBlobs: 64, FetchConcurrency: 1, PollInterval: time.Hour,
		ContinuityCheckpoint: cp,
	})
	if err != nil {
		t.Fatalf("beacon.New: %v", err)
	}
	return ix
}

// TestContinuityCheckpointMatchAnchors: the seed walk reaches the configured
// checkpoint slot, whose feed header is present and matches the configured root, and
// anchors to it -- coverage then advances over a genuine leading miss the checkpoint
// proves.
func TestContinuityCheckpointMatchAnchors(t *testing.T) {
	feed := newFakeBlockFeed(t, 3)
	src := newFakeSource(t)
	a := newFakeArchive(t, "all")
	a.origin = 3

	// slot 1 (the checkpoint) is present with root deriveRoot(1); slot 2 is a genuine
	// miss (404); slot 3 is present and chains straight to the checkpoint, carrying a
	// blob. The walk from slot 2 down 404s slot 2 then reaches the checkpoint at slot 1.
	feed.present(1, deriveRoot(0), nil)
	blobs := slotBlobs(3, 1)
	feed.present(3, deriveRoot(1), blobs)
	src.serve(3, blobs)

	cp := &beacon.ContinuityCheckpoint{Slot: 1, Root: deriveRoot(1)}
	ix := newAnchoredIndexerCheckpoint(t, feed, a, 8, cp, src)
	if err := drain(t, ix); err != nil {
		t.Fatalf("drain: a matching checkpoint should anchor the walk, but: %v", err)
	}
	if got, ok := a.coverage(); !ok || got != 3 {
		t.Fatalf("coverage = %d (%v), want 3", got, ok)
	}
}

// TestContinuityCheckpointMismatchIsFatal: the feed's header at the
// checkpoint slot is present but its root disagrees with the configured root. The
// feed and the operator disagree about the anchor everything chains to, so nothing
// may advance -- a fatal configuration error.
func TestContinuityCheckpointMismatchIsFatal(t *testing.T) {
	feed := newFakeBlockFeed(t, 3)
	src := newFakeSource(t)
	a := newFakeArchive(t, "all")
	a.origin = 3

	feed.present(1, deriveRoot(0), nil) // the feed's real root here is deriveRoot(1)
	feed.present(3, deriveRoot(1), nil)

	cp := &beacon.ContinuityCheckpoint{Slot: 1, Root: deriveRoot(99)} // configured root disagrees
	ix := newAnchoredIndexerCheckpoint(t, feed, a, 8, cp, src)
	err := drain(t, ix)
	if err == nil || !strings.Contains(err.Error(), "continuity_checkpoint mismatch") {
		t.Fatalf("want a fatal checkpoint mismatch, got %v", err)
	}
	if _, ok := a.coverage(); ok {
		t.Fatal("coverage advanced despite a fatal checkpoint mismatch")
	}
}

// TestContinuityCheckpointRescuesLeading404s is the reproducer's feed made
// safe. The feed 404s slots 0 and 1 (still backfilling near genesis), but a
// checkpoint at slot 0 supplies the trusted anchor, and slot 2 chains straight to it
// -- proving slot 1 a genuine miss -- so coverage advances where the un-checkpointed
// run waits.
func TestContinuityCheckpointRescuesLeading404s(t *testing.T) {
	feed := newFakeBlockFeed(t, 2)
	src := newFakeSource(t)
	a := newFakeArchive(t, "all")
	a.origin = 1

	blobs := slotBlobs(2, 1)
	feed.present(2, deriveRoot(0), blobs) // chains to the checkpoint root, not to a hidden slot 1
	src.serve(2, blobs)

	cp := &beacon.ContinuityCheckpoint{Slot: 0, Root: deriveRoot(0)}
	ix := newAnchoredIndexerCheckpoint(t, feed, a, 8, cp, src)
	if err := drain(t, ix); err != nil {
		t.Fatalf("drain: the checkpoint should anchor a near-genesis walk, but: %v", err)
	}
	if got, ok := a.coverage(); !ok || got != 2 {
		t.Fatalf("coverage = %d (%v), want 2", got, ok)
	}
}

// TestContinuityCheckpointCatchesHiddenLeadingBlock is the reproducer's exact
// feed -- slot 2 chains to a HIDDEN slot 1 -- but with a checkpoint at slot 0. The
// checkpoint anchors to deriveRoot(0); slot 2's parent names deriveRoot(1), which
// does not match, so the hidden block is a fatal continuity break, never the silent
// committed absence the safety boundary exposed.
func TestContinuityCheckpointCatchesHiddenLeadingBlock(t *testing.T) {
	feed := newFakeBlockFeed(t, 2)
	src := newFakeSource(t)
	a := newFakeArchive(t, "all")
	a.origin = 1

	hidden := slotBlobs(1, 1)
	src.serve(1, hidden)
	feed.present(2, deriveRoot(1), nil)

	cp := &beacon.ContinuityCheckpoint{Slot: 0, Root: deriveRoot(0)}
	ix := newAnchoredIndexerCheckpoint(t, feed, a, 8, cp, src)
	err := drain(t, ix)
	if err == nil || !strings.Contains(err.Error(), "continuity broken") {
		t.Fatalf("want a fatal continuity break at the hidden leading block, got %v", err)
	}
	if _, ok := a.coverage(); ok {
		t.Fatal("coverage advanced over a hidden leading block")
	}
	if src.wasRequested(1) {
		t.Fatal("the hidden slot's blob was requested; the continuity break must precede any source read")
	}
}

// TestContinuityCheckpointMustPrecedeCoverage: a checkpoint at or after the
// first slot the run covers could advance coverage itself, which it must never do.
// It is rejected before any walk.
func TestContinuityCheckpointMustPrecedeCoverage(t *testing.T) {
	// Both the == and the > boundary are fatal, no-fetch, no-coverage. The == case
	// catches mutating the `>=` guard to `>`; the > case catches mutating it to `==`
	//. A checkpoint at or after the first covered slot could
	// advance coverage itself, which it must never do.
	for _, cpSlot := range []uint64{3, 4} { // origin (resume) is 3: == and > it
		t.Run(fmt.Sprintf("checkpoint_slot_%d", cpSlot), func(t *testing.T) {
			feed := newFakeBlockFeed(t, 8)
			src := newFakeSource(t)
			a := newFakeArchive(t, "all")
			a.origin = 3

			cp := &beacon.ContinuityCheckpoint{Slot: cpSlot, Root: deriveRoot(cpSlot)}
			ix := newAnchoredIndexerCheckpoint(t, feed, a, 8, cp, src)
			err := drain(t, ix)
			if err == nil || !strings.Contains(err.Error(), "strictly before") {
				t.Fatalf("want a fatal 'not strictly before' error for checkpoint.slot %d vs resume 3, got %v", cpSlot, err)
			}
			if _, ok := a.coverage(); ok {
				t.Fatal("coverage advanced with a checkpoint that does not precede it")
			}
			if src.wasRequested(cpSlot) {
				t.Fatal("a source was queried despite a fatal checkpoint config")
			}
		})
	}
}

// TestContinuityGenesisLeadingMissWaits: a genesis run (origin 0) whose slot
// 0 has not backfilled. With no slot before 0 to anchor to, an unanchored batch may
// bootstrap ONLY on a present slot 0; a present slot 2 over leading 404s is a leading
// miss that cannot be proven, so the run waits rather than committing it (audit
// the safety boundary).
func TestContinuityGenesisLeadingMissWaits(t *testing.T) {
	feed := newFakeBlockFeed(t, 2)
	src := newFakeSource(t)
	a := newFakeArchive(t, "all") // origin 0

	feed.present(2, deriveRoot(1), nil) // slots 0 and 1 are 404

	ix := newAnchoredIndexer(t, feed, a, 8, 1, nil, src)
	advanced, err := ix.Step(t.Context())
	if err != nil {
		t.Fatalf("a genesis leading miss must wait, not fail: %v", err)
	}
	if advanced {
		t.Fatal("Step advanced over a genesis leading miss")
	}
	if _, ok := a.coverage(); ok {
		t.Fatal("coverage advanced over a genesis leading miss")
	}
}

// TestContinuityGenesisPresentSlotZeroBootstraps is the rider's other side: a
// genuinely present slot-0 header bootstraps the anchor with no checkpoint and no
// walk, because genesis has no parent to chain to.
func TestContinuityGenesisPresentSlotZeroBootstraps(t *testing.T) {
	feed := newFakeBlockFeed(t, 2)
	src := newFakeSource(t)
	a := newFakeArchive(t, "all") // origin 0

	r0 := feed.present(0, [32]byte{}, nil)
	r1 := feed.present(1, r0, nil)
	blobs := slotBlobs(2, 1)
	feed.present(2, r1, blobs)
	src.serve(2, blobs)

	ix := newAnchoredIndexer(t, feed, a, 8, 1, nil, src)
	if err := drain(t, ix); err != nil {
		t.Fatalf("drain: a present slot 0 must bootstrap, but: %v", err)
	}
	if got, ok := a.coverage(); !ok || got != 2 {
		t.Fatalf("coverage = %d (%v), want 2", got, ok)
	}
}

// TestDuplicateIndexerAnchorlessStartAdvance is the follow-up blocker (audit
// the safety boundary). An indexer left in the genesis anchorless wait state (seeded, rootless)
// must not reuse it once a DUPLICATE writer has advanced the shared archive under
// it: it has to reseed from the new resume point, so a wrong parent_root at the new
// leading slot is refused, never accepted as a bootstrap.
//
// Sequence: A runs at origin 0 with slot 0 not yet backfilled -> A waits, anchorless.
// A duplicate advances the archive to slot 1. A replans at slot 2, whose block names
// a parent that does not chain to slot 1. A must refuse (continuity break), never
// advance. This isolates the seed-side fix (a): reverting it leaves A reusing the
// stale anchorless state and waiting silently (no continuity break), failing the
// error assertion below even with the reassembly guard present.
func TestDuplicateIndexerAnchorlessStartAdvance(t *testing.T) {
	feed := newFakeBlockFeed(t, 2)
	src := newFakeSource(t)
	a := newFakeArchive(t, "all") // origin 0

	// slot 0 absent (still backfilling); slot 1 present; slot 2 present but its
	// parent_root (deriveRoot(99)) does not chain to slot 1 -- a hidden or wrong
	// block at the new leading edge.
	feed.present(1, deriveRoot(0), nil)
	feed.present(2, deriveRoot(99), nil)

	ix := newAnchoredIndexer(t, feed, a, 8, 1, nil, src)

	// Pass 1: nothing to anchor to at genesis, so A waits without advancing.
	advanced, err := ix.Step(t.Context())
	if err != nil {
		t.Fatalf("pass 1: %v", err)
	}
	if advanced {
		t.Fatal("pass 1 advanced; a genesis run whose slot 0 is absent must wait")
	}
	if _, ok := a.coverage(); ok {
		t.Fatal("pass 1 advanced coverage over an unbootstrapped genesis")
	}

	// A duplicate indexer advances the shared archive to slot 1 under A.
	one := uint64(1)
	a.mu.Lock()
	a.syncedTo = &one
	a.mu.Unlock()

	// Pass 2: A replans at slot 2. It must reseed (the stale anchorless state is
	// discarded), find slot 1 as its anchor, and refuse slot 2's wrong parent_root.
	advanced, err = ix.Step(t.Context())
	if advanced {
		t.Fatal("pass 2 advanced; a stale anchorless state bootstrapped a later batch")
	}
	if err == nil || !strings.Contains(err.Error(), "continuity broken") {
		t.Fatalf("pass 2 must refuse with a continuity break after reseeding, got %v", err)
	}
	if got, ok := a.coverage(); !ok || got != 1 {
		t.Fatalf("coverage = %d (%v), want it left at 1 -- A must not have advanced over the wrong parent", got, ok)
	}
}

// TestDuplicateIndexerHeldRootStartAdvance is the follow-up blocker (audit
// the safety boundary) as a PAIR. The same duplicate-writer transition -- A commits slot 0 and
// caches its root r0 (expecting to resume at slot 1), then a duplicate advances the
// shared archive through slot 1, so A observes resume 2 -- is exercised with slot 2
// named two ways. The fix binds the cached anchor to its expected resume, so at
// resume 2 != the expected 1 A must RESEED from slot 1 (whose real root is r1)
// rather than reuse r0:
//
//   - invalid: slot 2's parent is r0 (skipping present slot 1). The reseed's anchor
//     r1 does not match, so A continuity-breaks and never advances.
//   - valid: slot 2's parent is r1 (chaining to slot 1). The reseed's anchor r1
//     matches, so A advances NORMALLY.
//
// The pair is the point: the repair RE-ANCHORS to the new resume, it does not reject
// every shared-coverage movement. Reusing r0 would wrongly accept the invalid slot 2
// (bypass) and would also mis-validate the valid one against the wrong anchor.
func TestDuplicateIndexerHeldRootStartAdvance(t *testing.T) {
	// setup commits slot 0 (caching r0), then a duplicate advances the archive
	// through slot 1, leaving A observing resume 2. slot 2's parent is caller-chosen.
	setup := func(t *testing.T, slot2Parent [32]byte) (*beacon.Indexer, *fakeArchive) {
		t.Helper()
		feed := newFakeBlockFeed(t, 2)
		src := newFakeSource(t)
		a := newFakeArchive(t, "all") // origin 0

		r0 := feed.present(0, [32]byte{}, nil)
		feed.present(1, r0, nil) // slot 1 present, root r1 = deriveRoot(1), chains to slot 0
		feed.present(2, slot2Parent, nil)

		// batch_size 1: pass 1 covers exactly [0,0], so A commits and caches r0 alone.
		ix := newAnchoredIndexer(t, feed, a, 1, 1, nil, src)
		advanced, err := ix.Step(t.Context())
		if err != nil || !advanced {
			t.Fatalf("pass 1 (commit slot 0): advanced=%v err=%v", advanced, err)
		}
		if got, ok := a.coverage(); !ok || got != 0 {
			t.Fatalf("after pass 1 coverage = %d (%v), want 0", got, ok)
		}
		one := uint64(1)
		a.mu.Lock()
		a.syncedTo = &one
		a.mu.Unlock()
		return ix, a
	}

	t.Run("invalid parent reseeds and breaks", func(t *testing.T) {
		// slot 2's parent is r0 (deriveRoot(0)): it skips present slot 1, and matches
		// exactly the stale root A holds -- the bypass's bait.
		ix, a := setup(t, deriveRoot(0))
		advanced, err := ix.Step(t.Context())
		if advanced {
			t.Fatal("advanced slot 2 on the stale held root r0")
		}
		if err == nil || !strings.Contains(err.Error(), "continuity broken") {
			t.Fatalf("must reseed and break on slot 2's wrong parent, got %v", err)
		}
		if got, ok := a.coverage(); !ok || got != 1 {
			t.Fatalf("coverage = %d (%v), want it left at 1 (the duplicate's advance, never past it)", got, ok)
		}
	})

	t.Run("valid parent reseeds and advances", func(t *testing.T) {
		// slot 2's parent is r1 (deriveRoot(1)): it chains correctly to present slot 1.
		// A must reseed to r1 and advance -- the repair re-anchors, it does not reject
		// a legitimate shared-coverage movement.
		ix, a := setup(t, deriveRoot(1))
		advanced, err := ix.Step(t.Context())
		if err != nil {
			t.Fatalf("valid slot 2 after a reseed must advance, got err %v", err)
		}
		if !advanced {
			t.Fatal("did not advance over a valid slot 2 after reseeding to r1")
		}
		if got, ok := a.coverage(); !ok || got != 2 {
			t.Fatalf("coverage = %d (%v), want 2 (reseeded to r1 and advanced normally)", got, ok)
		}
	})
}

// TestDuplicateIndexerArchiveRewindClearsStaleRoot pins the follow-up
// refinement's BOTH-clear: a mismatch clears the cached ROOT,
// not just seed readiness. A commits a canonical chain 0..3 holding r3, then the
// shared archive REWINDS to empty (a twin truncated it), so A next observes resume 0.
// The feed near genesis is unbackfilled -- slots 0 and 1 are absent -- and a later
// present slot 2 names A's PRE-REWIND cached root r3 as its parent. A must clear the
// stale root on the resume mismatch and WAIT: an unanchored genesis batch whose slot
// 0 is absent proves nothing, and validating slot 2 against r3 would commit genesis
// absence out of pre-rewind history. Reverting the clear to seeded-only (leaving
// haveLastRoot set) makes A carry r3 into the genesis batch and advance -- this test
// catches that.
func TestDuplicateIndexerArchiveRewindClearsStaleRoot(t *testing.T) {
	feed := newFakeBlockFeed(t, 3)
	src := newFakeSource(t)
	a := newFakeArchive(t, "all") // origin 0

	// Phase 1: A commits the canonical chain 0..3, caching slot 3's root r3.
	r0 := feed.present(0, [32]byte{}, nil)
	r1 := feed.present(1, r0, nil)
	r2 := feed.present(2, r1, nil)
	feed.present(3, r2, nil)
	ix := newAnchoredIndexer(t, feed, a, 8, 1, nil, src)
	if err := drain(t, ix); err != nil {
		t.Fatalf("phase 1 drain: %v", err)
	}
	if got, ok := a.coverage(); !ok || got != 3 {
		t.Fatalf("phase 1 coverage = %d (%v), want 3", got, ok)
	}

	// The archive rewinds to empty; the feed loses slots 0,1 (and 3), and slot 2 now
	// names A's stale r3 as its parent.
	a.mu.Lock()
	a.syncedTo = nil
	a.mu.Unlock()
	feed.absent(0, 1, 3)
	feed.present(2, deriveRoot(3), nil) // slot 2's parent = r3, the pre-rewind cached root

	advanced, err := ix.Step(t.Context())
	if err != nil {
		t.Fatalf("the rewind pass must wait, not fail: %v", err)
	}
	if advanced {
		t.Fatal("advanced over a genesis leading miss using the pre-rewind root r3")
	}
	if got, ok := a.coverage(); ok {
		t.Fatalf("coverage recorded (%d) after a rewind; A proved genesis absence from stale pre-rewind history", got)
	}
}

// TestLocalBoundaryNotRedefinedByResponse pins the local-boundary rule (audit
// the safety boundary follow-up): post() advances expectedResume to A's OWN reassembly-validated
// boundary (*fb.last+1), never to the archive's response. The dangerous shape is an
// idempotent no-op: while A's [0,3] POST is gated, a twin advances the shared archive
// to slot 5, so A's POST returns a no-op carrying the HIGHER synced_to 5. If A took
// its next expected resume from that response (res.SyncedTo+1 = 6), it would pair its
// stale [0,3] out-root r3 with resume 6 and validate slot 6 (whose parent is r3, the
// bait) against it -- committing slots 4,5 it never saw. Keeping *fb.last+1 = 4
// instead makes the next plan a mismatch that reseeds from slot 5 and breaks on slot
// 6. The plain fake archive echoes the request, so only this no-op-with-higher-tip
// response exposes the difference.
func TestLocalBoundaryNotRedefinedByResponse(t *testing.T) {
	feed := newFakeBlockFeed(t, 3)
	src := newFakeSource(t)
	a := newFakeArchive(t, "all") // origin 0
	a.idempotent = true
	a.refsGate = make(chan struct{})

	r0 := feed.present(0, [32]byte{}, nil)
	r1 := feed.present(1, r0, nil)
	r2 := feed.present(2, r1, nil)
	feed.present(3, r2, nil) // clean chain 0..3

	ix := newAnchoredIndexer(t, feed, a, 8, 1, nil, src)

	// The twin: once A's [0,3] POST is gated, advance the archive to slot 5 and grow
	// the feed to slot 6 (whose parent is r3, A's [0,3] out-root), then release.
	twinDone := make(chan struct{})
	go func() {
		defer close(twinDone)
		deadline := time.Now().Add(5 * time.Second)
		for {
			a.mu.Lock()
			gated := a.refsGated
			a.mu.Unlock()
			if gated || time.Now().After(deadline) {
				break
			}
			time.Sleep(time.Millisecond)
		}
		feed.present(4, deriveRoot(3), nil)
		feed.present(5, deriveRoot(4), nil)
		feed.present(6, deriveRoot(3), nil) // trap: parent = r3, A's [0,3] out-root
		feed.finalized.Store(6)
		five := uint64(5)
		a.mu.Lock()
		a.syncedTo = &five
		a.mu.Unlock()
		close(a.refsGate)
	}()

	// Step 1 commits [0,3] through the gated, idempotent no-op POST.
	advanced, err := ix.Step(t.Context())
	<-twinDone
	if err != nil || !advanced {
		t.Fatalf("commit of [0,3] through the no-op POST: advanced=%v err=%v", advanced, err)
	}

	// Step 2: the next plan sees resume 6. With the local boundary (expectedResume 4)
	// that is a mismatch -> reseed from slot 5 -> break on slot 6's r3 parent. A
	// response-derived resume (6) would instead carry r3 and advance.
	advanced, err = ix.Step(t.Context())
	if advanced {
		t.Fatal("advanced slot 6 on the stale [0,3] anchor via a response-derived resume (local-boundary rule)")
	}
	if err == nil || !strings.Contains(err.Error(), "continuity broken") {
		t.Fatalf("the replan must reseed and break on slot 6, got %v", err)
	}
	if got, ok := a.coverage(); !ok || got != 5 {
		t.Fatalf("coverage = %d (%v), want it left at 5 (the twin's tip, never past it)", got, ok)
	}
}

// TestPipelineBoundaryReseedsGenuine is the REAL pipelined boundary regression
// . It drives a genuinely pipelined Run that creates AND
// consumes lookahead, then falls back to a fresh plan at a finality boundary where a
// twin's advance is caught -- not a serial warmup feeding Run's first plan.
//
// batch 2, finalized 3, concurrency 6: Run plans [0,1], PREFETCHES [2,3] against
// [0,1]'s out-root, then posts [0,1] (gated). While that post is gated a twin advances
// the archive to slot 5 and finality to 6, with slot 6 trapping on r3 (A's [2,3]
// out-root). The pipeline consumes the ALREADY-fetched [2,3] and posts it (a no-op
// against the twin's higher tip), and only then -- [4..] exceeding the finality [2,3]
// was planned against -- falls back to a fresh plan. That plan's BOUNDARY comparison
// sees resume 6 != its expected 4 and reseeds from slot 5, breaking on slot 6.
//
// Two properties are asserted: the run reseeds and breaks (the fix), and it posted
// [2,3] BEFORE the boundary -- proof the pipeline genuinely prefetched and consumed a
// batch a serial run would never have fetched (serial replans after [0,1] sees the
// twin's resume and jumps straight to slot 6). So forcing runSerial fails the second
// assertion; reverting the follow-up fast path fails the first (the run carries r3 onto
// slot 6, advances, and idles caught-up instead of breaking).
func TestPipelineBoundaryReseedsGenuine(t *testing.T) {
	feed := newFakeBlockFeed(t, 3)
	src := newFakeSource(t)
	a := newFakeArchive(t, "all") // origin 0
	a.idempotent = true
	a.refsGate = make(chan struct{})

	r0 := feed.present(0, [32]byte{}, nil)
	r1 := feed.present(1, r0, nil)
	r2 := feed.present(2, r1, nil)
	feed.present(3, r2, nil) // clean chain 0..3, finalized 3

	// The twin: once A's [0,1] POST is gated (its [2,3] prefetch already in flight),
	// advance coverage to slot 5 and finality to 6, with slot 6's parent = r3.
	twinDone := make(chan struct{})
	go func() {
		defer close(twinDone)
		deadline := time.Now().Add(5 * time.Second)
		for {
			a.mu.Lock()
			gated := a.refsGated
			a.mu.Unlock()
			if gated || time.Now().After(deadline) {
				break
			}
			time.Sleep(time.Millisecond)
		}
		feed.present(4, deriveRoot(3), nil)
		feed.present(5, deriveRoot(4), nil)
		feed.present(6, deriveRoot(3), nil) // trap: parent = r3, A's [2,3] out-root
		five := uint64(5)
		a.mu.Lock()
		a.syncedTo = &five
		a.mu.Unlock()
		feed.finalized.Store(6)
		close(a.refsGate)
	}()

	ix := newAnchoredIndexer(t, feed, a, 2, 6, nil, src)
	done := make(chan error, 1)
	runCtx, runCancel := context.WithCancel(t.Context())
	defer runCancel()
	go func() { done <- ix.Run(runCtx) }()
	select {
	case err := <-done:
		<-twinDone
		if err == nil || !strings.Contains(err.Error(), "continuity broken") {
			t.Fatalf("the pipelined boundary must reseed and break on slot 6, got %v", err)
		}
	case <-time.After(15 * time.Second):
		// Only the reverted fast path reaches here (it advances slot 6 and idles).
		runCancel()
		<-done
		<-twinDone
		if got, _ := a.coverage(); got > 5 {
			t.Fatalf("the pipelined run advanced to %d on the stale anchor", got)
		}
		t.Fatal("the pipelined run did not reseed-and-break within the deadline")
	}

	// The pipeline must have prefetched and posted [2,3] before the boundary: two refs
	// POSTs. A serial run posts only [0,1] (it replans after [0,1] and jumps to slot 6),
	// so this fails under runSerial-forced.
	posts := 0
	for _, w := range a.recordedWrites() {
		if strings.HasPrefix(w, "refs:") {
			posts++
		}
	}
	if posts < 2 {
		t.Fatalf("%d refs POSTs; the pipeline must prefetch and post [2,3] before the boundary (a serial run would not)", posts)
	}
}

// TestContinuityZeroBoundaryWaitSkipsFetch makes the seed-side zero-boundary
// wait independently load-bearing. When the
// walk reaches slot 0 unanchored, the run must wait BEFORE fetching the batch -- so
// a blob source in the batch range is never even asked. This distinguishes the seed
// wait (plan short-circuits to caught-up) from the reassembly guard (which would
// still have fetched the batch and only then declined to commit). Reverting the
// seed's not-ready return makes plan fetch the batch and query the source, failing
// the no-fetch assertion even with the reassembly guard present.
func TestContinuityZeroBoundaryWaitSkipsFetch(t *testing.T) {
	feed := newFakeBlockFeed(t, 2)
	src := newFakeSource(t)
	a := newFakeArchive(t, "all")
	a.origin = 1

	// Slots 0 and 1 are 404 (the walk reaches zero unanchored). Slot 2 is present and
	// carries a blob the source can serve -- so if the batch were fetched, the source
	// WOULD be asked for slot 2.
	blobs := slotBlobs(2, 1)
	feed.present(2, deriveRoot(1), blobs)
	src.serve(2, blobs)

	ix := newAnchoredIndexer(t, feed, a, 8, 1, nil, src)
	advanced, err := ix.Step(t.Context())
	if err != nil {
		t.Fatalf("the zero-boundary walk must wait, not fail: %v", err)
	}
	if advanced {
		t.Fatal("advanced without an anchor")
	}
	if _, ok := a.coverage(); ok {
		t.Fatal("coverage advanced over an unanchored zero-boundary walk")
	}
	if src.wasRequested(2) {
		t.Fatal("the batch's blob source was queried; the seed wait must short-circuit before any batch fetch")
	}
}

// TestMirrorModeRejectsCheckpoint pins correction 1: beacon.New rejects a
// ContinuityCheckpoint in mirror mode (Blocks nil) at the package boundary, so the
// misconfiguration cannot reach an indexer even if a caller bypasses the cmd config
// validation.
func TestMirrorModeRejectsCheckpoint(t *testing.T) {
	src := newFakeSource(t)
	a := newFakeArchive(t, "all")
	client, err := upstream.New(upstream.Config{BaseURL: src.url, Head: "all", MaxAttempts: 1, Backoff: time.Millisecond})
	if err != nil {
		t.Fatalf("upstream.New: %v", err)
	}
	arch, err := archclient.New(archclient.Config{BaseURL: a.url, Token: "t", MaxAttempts: 1, Backoff: time.Millisecond})
	if err != nil {
		t.Fatalf("archclient.New: %v", err)
	}
	_, err = beacon.New(beacon.Config{
		Sources:              []beacon.Source{{Client: client}},
		Blocks:               nil, // mirror mode
		ContinuityCheckpoint: &beacon.ContinuityCheckpoint{Slot: 1, Root: deriveRoot(1)},
		Archive:              arch,
		Head:                 "all",
	})
	if err == nil || !strings.Contains(err.Error(), "ContinuityCheckpoint") {
		t.Fatalf("beacon.New accepted a checkpoint in mirror mode; want a rejection, got %v", err)
	}
}

func TestMissingCommitmentsFieldAdvancesAsBlobless(t *testing.T) {
	feed := newFakeBlockFeed(t, 1)
	src := newFakeSource(t)
	a := newFakeArchive(t, "all")

	r0 := feed.present(0, [32]byte{}, nil)
	blobs := slotBlobs(1, 1)
	feed.present(1, r0, blobs)
	src.serve(1, blobs)

	// Keep the header truthful but replace the blinded-block body with a
	// syntactically valid response whose required blob_kzg_commitments member is
	// absent. With the presence-aware decoder this is no longer read as an empty
	// array (a blobless block); it is a malformed answer that fails the slot closed,
	// so coverage never advances over slot 1's real blob.
	hc := &http.Client{Transport: auditBodyTransport{
		base: http.DefaultTransport,
		path: "/eth/v1/beacon/blinded_blocks/1",
		body: blindedBody(`"data":{"message":{"body":{}}}`),
	}}
	ix := newAnchoredIndexer(t, feed, a, 8, 1, hc, src)
	err := drain(t, ix)
	if err == nil {
		t.Fatal("drain succeeded; an omitted blob_kzg_commitments must fail the slot closed, not advance as blobless")
	}
	if !strings.Contains(err.Error(), "blob_kzg_commitments") {
		t.Errorf("error does not name the offending member: %v", err)
	}
	if got, ok := a.coverage(); ok {
		t.Fatalf("coverage advanced to %d over a slot with an omitted commitments member", got)
	}
	t.Log("a present blob-carrying block with an omitted commitments field failed closed rather than advancing as blobless")
}

func TestMirrorMissingDataFieldAdvancesAsEmpty(t *testing.T) {
	up := newFakeMirrorUpstream(t, "all", 0, 1)
	up.blobs[1] = slotBlobs(1, 1)
	a := newFakeArchive(t, "all")

	// The mirror really has a blob at slot 1, but its syntactically valid 200
	// omits the required data member. With the presence-aware decoder this is no
	// longer read as a nil slice (a covered empty slot); it is a malformed answer
	// that fails closed as a retryable transport error, so coverage never advances
	// over slot 1's real blob.
	hc := &http.Client{Transport: auditBodyTransport{
		base: http.DefaultTransport,
		path: "/all/eth/v1/beacon/blobs/1",
		body: `{}`,
	}}
	src, err := upstream.New(upstream.Config{
		BaseURL: up.url, Head: up.head, HTTPClient: hc,
		MaxAttempts: 1, Backoff: time.Millisecond,
	})
	if err != nil {
		t.Fatalf("upstream.New: %v", err)
	}
	arch, err := archclient.New(archclient.Config{
		BaseURL: a.url, Token: "audit", MaxAttempts: 1, Backoff: time.Millisecond,
	})
	if err != nil {
		t.Fatalf("archclient.New: %v", err)
	}
	ix, err := beacon.New(beacon.Config{
		Sources:          []beacon.Source{{Client: src}},
		Archive:          arch,
		Head:             a.head,
		BatchSize:        8,
		MaxPutBlobs:      64,
		FetchConcurrency: 1,
		PollInterval:     time.Hour,
	})
	if err != nil {
		t.Fatalf("beacon.New: %v", err)
	}
	if err := drain(t, ix); err == nil {
		t.Fatal("drain succeeded; a mirror 200 that omits data must fail closed, not advance as a covered empty slot")
	}
	if got, ok := a.coverage(); ok {
		t.Fatalf("coverage advanced to %d over a mirror slot whose data member was omitted", got)
	}
	t.Log("a mirror 200 with an omitted data field failed closed rather than advancing over a real blob as covered-empty")
}

func TestNegativePollIntervalRejectedByNew(t *testing.T) {
	feed := newFakeBlockFeed(t, 0)
	src := newFakeSource(t)
	a := newFakeArchive(t, "all")

	hc := &http.Client{Transport: http.DefaultTransport}
	blocks, err := upstream.NewBlockClient(upstream.Config{
		BaseURL: feed.url, HTTPClient: hc, MaxAttempts: 1, Backoff: time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewBlockClient: %v", err)
	}
	arch, err := archclient.New(archclient.Config{
		BaseURL: a.url, Token: "audit", MaxAttempts: 1, Backoff: time.Millisecond,
	})
	if err != nil {
		t.Fatalf("archclient.New: %v", err)
	}
	// A non-positive poll interval is now rejected at construction
	// rather than turning Run's caught-up wait into an immediate finalized-read loop.
	_, err = beacon.New(beacon.Config{
		Sources:          beaconSources(t, hc, src),
		Blocks:           blocks,
		Archive:          arch,
		Head:             a.head,
		BatchSize:        8,
		MaxPutBlobs:      64,
		FetchConcurrency: 1,
		PollInterval:     -time.Second,
	})
	if err == nil {
		t.Fatal("beacon.New accepted a negative poll_interval; it must be rejected")
	}
	if !strings.Contains(err.Error(), "Config.PollInterval is -1s, must be positive") {
		t.Fatalf("error = %q, want it to name the non-positive poll interval", err)
	}
}

// auditBodyTransport answers a fixed 200 JSON body for one path and passes every
// other request through to base. It lets a table drive the exact malformed shape a
// coverage-bearing decoder must reject at a single slot while the rest of the feed
// stays truthful.
type auditBodyTransport struct {
	base http.RoundTripper
	path string
	body string
}

func (tr auditBodyTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.URL.Path == tr.path {
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(strings.NewReader(tr.body)),
			Request:    r,
		}, nil
	}
	return tr.base.RoundTrip(r)
}

// blindedBody wraps a blinded-block data fragment in the safety metadata a slot
// within the finalized bound must carry (execution_optimistic:false, finalized:true,
// per blinded_block.yaml), so a commitments-path table exercises the
// blob_kzg_commitments structure rather than tripping the metadata checks the
// addendum added. An empty fragment omits data entirely.
func blindedBody(dataFragment string) string {
	if dataFragment == "" {
		return `{"execution_optimistic":false,"finalized":true}`
	}
	return `{"execution_optimistic":false,"finalized":true,` + dataFragment + `}`
}

// TestCommitmentsPathPresenceRule walks every parent level of
// blob_kzg_commitments: data, message, body, and the member
// itself, each omitted and each nulled, must fail the slot closed rather than
// decode to a nil slice read as a blobless block. The single legitimate shape --
// an explicitly present empty array -- is the control that advances coverage over
// a genuinely blobless slot. Both invariants in one table: presence is required
// everywhere on the path, and only the explicit empty array carries the blobless
// meaning.
func TestCommitmentsPathPresenceRule(t *testing.T) {
	run := func(t *testing.T, body string, wantFail bool) {
		t.Helper()
		feed := newFakeBlockFeed(t, 1)
		src := newFakeSource(t)
		a := newFakeArchive(t, "all")

		r0 := feed.present(0, [32]byte{}, nil)
		blobs := slotBlobs(1, 1)
		feed.present(1, r0, blobs) // slot 1 really carries a blob
		src.serve(1, blobs)

		hc := &http.Client{Transport: auditBodyTransport{
			base: http.DefaultTransport, path: "/eth/v1/beacon/blinded_blocks/1", body: body,
		}}
		ix := newAnchoredIndexer(t, feed, a, 8, 1, hc, src)
		err := drain(t, ix)

		if wantFail {
			if err == nil {
				t.Fatal("drain succeeded; a missing or null element on the commitments path must fail closed")
			}
			if got, ok := a.coverage(); ok {
				t.Fatalf("coverage advanced to %d over a malformed commitments body", got)
			}
			return
		}
		// Control: an explicitly present empty array is a verifiably blobless slot.
		if err != nil {
			t.Fatalf("drain over an explicit empty commitments array: %v", err)
		}
		if got, ok := a.coverage(); !ok || got != 1 {
			t.Fatalf("coverage = %d (%v), want 1 for an explicitly blobless slot", got, ok)
		}
		if src.wasRequested(1) {
			t.Fatal("an explicitly blobless slot asked a source for blobs")
		}
		if refs := lastRefs(t, a); !strings.Contains(refs, `"rows":[]`) {
			t.Fatalf("an explicitly blobless slot produced a non-empty refs: %s", refs)
		}
	}

	for _, tc := range []struct {
		name     string
		body     string
		wantFail bool
	}{
		{"data omitted", blindedBody(""), true},
		{"data null", blindedBody(`"data":null`), true},
		{"message omitted", blindedBody(`"data":{}`), true},
		{"message null", blindedBody(`"data":{"message":null}`), true},
		{"body omitted", blindedBody(`"data":{"message":{}}`), true},
		{"body null", blindedBody(`"data":{"message":{"body":null}}`), true},
		{"commitments omitted", blindedBody(`"data":{"message":{"body":{}}}`), true},
		{"commitments null", blindedBody(`"data":{"message":{"body":{"blob_kzg_commitments":null}}}`), true},
		{"commitments explicit empty advances blobless", blindedBody(`"data":{"message":{"body":{"blob_kzg_commitments":[]}}}`), false},
	} {
		t.Run(tc.name, func(t *testing.T) { run(t, tc.body, tc.wantFail) })
	}
}

// TestMirrorDataPresenceRule is the mirror counterpart: a 200 whose data
// member is omitted or nulled fails closed as a retryable transport error, while
// an explicitly present empty array is the spec's covered-empty slot and advances
// coverage. data is a top-level member, so those are every level on
// its path.
func TestMirrorDataPresenceRule(t *testing.T) {
	run := func(t *testing.T, body string, wantFail bool) {
		t.Helper()
		up := newFakeMirrorUpstream(t, "all", 0, 1)
		up.blobs[1] = slotBlobs(1, 1) // the mirror really has a blob at slot 1
		a := newFakeArchive(t, "all")

		hc := &http.Client{Transport: auditBodyTransport{
			base: http.DefaultTransport, path: "/all/eth/v1/beacon/blobs/1", body: body,
		}}
		src, err := upstream.New(upstream.Config{
			BaseURL: up.url, Head: up.head, HTTPClient: hc, MaxAttempts: 1, Backoff: time.Millisecond,
		})
		if err != nil {
			t.Fatalf("upstream.New: %v", err)
		}
		arch, err := archclient.New(archclient.Config{
			BaseURL: a.url, Token: "audit", MaxAttempts: 1, Backoff: time.Millisecond,
		})
		if err != nil {
			t.Fatalf("archclient.New: %v", err)
		}
		ix, err := beacon.New(beacon.Config{
			Sources: []beacon.Source{{Client: src}}, Archive: arch, Head: a.head,
			BatchSize: 8, MaxPutBlobs: 64, FetchConcurrency: 1, PollInterval: time.Hour,
		})
		if err != nil {
			t.Fatalf("beacon.New: %v", err)
		}
		err = drain(t, ix)

		if wantFail {
			if err == nil {
				t.Fatal("drain succeeded; a mirror 200 with a missing or null data must fail closed")
			}
			if got, ok := a.coverage(); ok {
				t.Fatalf("coverage advanced to %d over a malformed mirror body", got)
			}
			return
		}
		// Control: an explicitly present empty array is a covered empty slot.
		if err != nil {
			t.Fatalf("drain over an explicit empty data array: %v", err)
		}
		if got, ok := a.coverage(); !ok || got != 1 {
			t.Fatalf("coverage = %d (%v), want 1 for an explicitly covered-empty slot", got, ok)
		}
		if refs := lastRefs(t, a); !strings.Contains(refs, `"rows":[]`) {
			t.Fatalf("a covered-empty slot produced a non-empty refs: %s", refs)
		}
	}

	for _, tc := range []struct {
		name     string
		body     string
		wantFail bool
	}{
		{"data omitted", `{}`, true},
		{"data null", `{"data":null}`, true},
		{"data explicit empty advances covered-empty", `{"data":[]}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) { run(t, tc.body, tc.wantFail) })
	}
}
