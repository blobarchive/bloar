package follow_test

import (
	"context"
	"encoding/hex"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	blocks "github.com/ipfs/go-block-format"

	"github.com/blobarchive/bloar/archive"
	"github.com/blobarchive/bloar/follow"
	"github.com/blobarchive/bloar/metrics"
	"github.com/blobarchive/bloar/server"
)

// TestRunAfterResumeVerifyFullCoalescesFinalizedRotationAndMovingMutableTip
// models the production filtered follower's hardest ordinary transition:
//
//   - the retained finalized frontier advances by exactly one 32-slot window;
//   - the mutable window rotates forward by the same 32 slots; and
//   - another publication advances the mutable tip while the old generation's
//     verify:full retained-closure walk is still completing.
//
// Admission must expose each signed pair atomically without waiting for the
// old walk. The one dirty-bit worker must then discard the old completion,
// skip the superseded intermediate mutable root, and converge on the latest
// finalized/mutable pair with full blob verification.
func TestRunAfterResumeVerifyFullCoalescesFinalizedRotationAndMovingMutableTip(t *testing.T) {
	w := newWriter(t)
	docs := newDocServer(t)
	mx := metrics.New()

	// These are real KZG-valid blobs. The slot-159 row begins in mutable A and
	// becomes part of finalized B, while slots 164 and 166 move the next mutable
	// tip twice. That makes the fixture a real 32-slot boundary rotation, not
	// merely two arbitrary roots with changed metadata.
	_, finalizedAVHs := w.ingestSlot(127, 91_001)
	_, rotatesIntoFinalizedVHs := w.ingestSlot(159, 91_002)
	_, mutableBVHs := w.ingestSlot(164, 91_003)
	mutableCBlobs, mutableCVHs := w.ingestSlot(166, 91_004)

	finalizedA := buildOverlayHeadWithRows(t, w, overlayFilteredHead, 96, 127, []archive.RefRow{
		{Slot: 127, VHs: finalizedAVHs},
	})
	mutableA := buildOverlayHeadWithRows(t, w, testHead, 128, 159, []archive.RefRow{
		{Slot: 159, VHs: rotatesIntoFinalizedVHs},
	})
	witnessA := buildOverlayHeadWithRows(t, w, testHandoffHead, 96, 127, nil)

	finalizedB := buildOverlayHeadWithRows(t, w, overlayFilteredHead, 96, 159, []archive.RefRow{
		{Slot: 127, VHs: finalizedAVHs},
		{Slot: 159, VHs: rotatesIntoFinalizedVHs},
	})
	mutableB := buildOverlayHeadWithRows(t, w, testHead, 160, 164, []archive.RefRow{
		{Slot: 164, VHs: mutableBVHs},
	})
	mutableC := buildOverlayHeadWithRows(t, w, testHead, 160, 166, []archive.RefRow{
		{Slot: 164, VHs: mutableBVHs},
		{Slot: 166, VHs: mutableCVHs},
	})
	witnessB := buildOverlayHeadWithRows(t, w, testHandoffHead, 96, 159, nil)

	if finalizedA.Root() == finalizedB.Root() || mutableA.Root() == mutableB.Root() ||
		mutableB.Root() == mutableC.Root() {
		t.Fatal("rotation fixture did not create distinct finalized and mutable generations")
	}

	admitted := make(chan uint64, 4)
	f := newFollower(t, w, func(cfg *follow.Config) {
		configureFilteredOverlayFollower(cfg, docs, mx)
		cfg.Verify = follow.VerifyFull
		cfg.OnAdmittedDocument = func(_ blocks.Block, doc server.Doc) error {
			if doc.Revision != nil {
				admitted <- *doc.Revision
			}
			return nil
		}
	})
	f.serveHTTP(nil)

	// Names are sorted, so the first pass is mutable A. Hold it after the real
	// verify:full walk but before its completion CAS. The second hook invocation
	// is finalized B: reaching it proves the old A completion returned without
	// stamping, and gives us a deterministic point to inspect that fact.
	oldMutableAtCommit := make(chan struct{})
	releaseOldMutable := make(chan struct{})
	currentFinalizedAtCommit := make(chan struct{})
	releaseCurrentFinalized := make(chan struct{})
	var hookCalls atomic.Int32
	follow.SetBeforeSyncCommitHook(func() {
		switch hookCalls.Add(1) {
		case 1:
			close(oldMutableAtCommit)
			<-releaseOldMutable
		case 2:
			close(currentFinalizedAtCommit)
			<-releaseCurrentFinalized
		}
	})

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	ticks := make(chan time.Time)
	runDone := make(chan error, 1)
	docs.set(sign(t, w.key, filteredOverlayDocument(t, w, finalizedA, mutableA, witnessA, 1)))
	go func() { runDone <- follow.RunAfterResumeTicks(f.f, ctx, ticks) }()

	var releaseOldOnce, releaseCurrentOnce sync.Once
	t.Cleanup(func() {
		releaseOldOnce.Do(func() { close(releaseOldMutable) })
		releaseCurrentOnce.Do(func() { close(releaseCurrentFinalized) })
		cancel()
		select {
		case err := <-runDone:
			if err != nil {
				t.Errorf("RunAfterResumeTicks: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("RunAfterResumeTicks did not join its sync worker")
		}
		follow.SetBeforeSyncCommitHook(nil)
	})

	if got := waitAdmittedRevision(t, ctx, admitted); got != 1 {
		t.Fatalf("initial admitted revision = %d, want 1", got)
	}
	select {
	case <-oldMutableAtCommit:
	case <-ctx.Done():
		t.Fatalf("mutable A verify:full sync did not reach its completion boundary: %v", ctx.Err())
	}
	requireFilteredOverlaySelection(t, f, finalizedA, mutableA, 1)
	if got := follow.HeadFetched(f.f, testHead); got.Defined() {
		t.Fatalf("blocked mutable A sync already stamped fetched root %s", got)
	}

	// Rotate the finalized frontier and mutable window together while A's
	// retained sync remains blocked.
	docs.set(sign(t, w.key, filteredOverlayDocument(t, w, finalizedB, mutableB, witnessB, 2)))
	select {
	case ticks <- time.Now():
	case <-ctx.Done():
		t.Fatalf("triggering 32-slot rotation admission: %v", ctx.Err())
	}
	if got := waitAdmittedRevision(t, ctx, admitted); got != 2 {
		t.Fatalf("rotation admitted revision = %d, want 2", got)
	}
	requireFilteredOverlaySelection(t, f, finalizedB, mutableB, 2)

	// Before the worker can finish A, move the mutable tip again. This wake must
	// coalesce with revision 2's pending wake; no per-revision backlog exists.
	docs.set(sign(t, w.key, filteredOverlayDocument(t, w, finalizedB, mutableC, witnessB, 3)))
	select {
	case ticks <- time.Now():
	case <-ctx.Done():
		t.Fatalf("triggering moving-tip admission: %v", ctx.Err())
	}
	if got := waitAdmittedRevision(t, ctx, admitted); got != 3 {
		t.Fatalf("moving-tip admitted revision = %d, want 3", got)
	}
	requireFilteredOverlaySelection(t, f, finalizedB, mutableC, 3)
	current := checkpointPublishedEntry(t, f, testHead)
	if current.WindowStart == nil || *current.WindowStart != 160 ||
		current.SyncedTo == nil || *current.SyncedTo != 166 ||
		current.HandoffRoot != witnessB.Root().String() ||
		current.HandoffSyncedTo == nil || *current.HandoffSyncedTo != 159 {
		t.Fatalf("revision 3 exposed an incoherent finalized/mutable pair: %#v", current)
	}

	releaseOldOnce.Do(func() { close(releaseOldMutable) })
	select {
	case <-currentFinalizedAtCommit:
	case <-ctx.Done():
		t.Fatalf("current finalized sync did not reach its completion boundary: %v", ctx.Err())
	}

	// The old mutable walk has now returned. Its completion CAS must not stamp
	// A (stale) or C (never walked). Finalized B is itself paused before its
	// stamp, so both fields must still be unset at this exact boundary.
	if got := follow.HeadFetched(f.f, testHead); got.Defined() {
		t.Fatalf("superseded mutable walk stamped fetched root %s; want no stale completion", got)
	}
	if got := follow.HeadFetched(f.f, overlayFilteredHead); got.Defined() {
		t.Fatalf("current finalized walk stamped before its blocked completion boundary: %s", got)
	}

	releaseCurrentOnce.Do(func() { close(releaseCurrentFinalized) })
	waitFetchedRoot(t, ctx, f.f, overlayFilteredHead, finalizedB.Root())
	waitFetchedRoot(t, ctx, f.f, testHead, mutableC.Root())

	// A current-only exact-hash read proves the final coalesced pass fetched the
	// newest generation and that verify:full accepted its KZG binding.
	status, data, _ := f.blobsAt(166, mutableCVHs[0])
	if status != 200 {
		t.Fatalf("GET latest mutable blob after coalesced full sync: status = %d, want 200", status)
	}
	want := "0x" + hex.EncodeToString(mutableCBlobs[0])
	if len(data) != 1 || data[0] != want {
		t.Fatalf("latest mutable blob differs after coalesced full sync: blobs=%d", len(data))
	}
	if _, ok := f.heads.Quarantined(testHead); ok {
		t.Fatal("honest current mutable generation was quarantined under verify: full")
	}
}
