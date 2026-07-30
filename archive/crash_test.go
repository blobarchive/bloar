package archive_test

import (
	"testing"

	"github.com/ipfs/go-cid"

	"github.com/blobarchive/bloar/archive"
)

// TestCrashBeforeRootSwap is spec 13.6's "kill the daemon between block writes
// and root swap".
//
// The engine writes every block of a batch and only then publishes the new
// root, so a crash in that gap is simulated exactly by discarding the engine
// and reloading from the root that was still published. The blocks are on disk
// either way: what makes them harmless is that nothing points at them.
func TestCrashBeforeRootSwap(t *testing.T) {
	hs := newHarness(t, testParams())
	first := []archive.RefRow{hs.row(41, 410), hs.row(43, 430, 431)}
	hs.apply(first, 47) // seals window 5
	oldRoot := hs.h.Root()

	// The daemon writes the whole of the next batch, then dies before the
	// publication document moves.
	second := []archive.RefRow{hs.row(49, 490), hs.row(52, 520, 521)}
	hs.apply(second, 55)
	newRoot := hs.h.Root()
	if newRoot == oldRoot {
		t.Fatalf("the second batch did not change the root")
	}

	// Restart from the root that was actually published.
	crashed := hs.reload(t, oldRoot)

	// The old root is intact and serves exactly what it did before the crash.
	if synced, covered := crashed.h.SyncedTo(); !covered || synced != 47 {
		t.Fatalf("recovered head reports synced_to (%d, %t), want (47, true)", synced, covered)
	}
	wantBlobs(t, crashed.lookup(41), "recovered: slot 41", 410)
	wantBlobs(t, crashed.lookup(43), "recovered: slot 43", 430, 431)
	wantBlobs(t, crashed.lookup(42), "recovered: covered blobless slot 42")
	// The half-applied batch left no trace in the old root.
	wantStatus(t, crashed.lookup(49), archive.StatusNotYetCovered, "recovered: a slot from the lost batch")
	wantStatus(t, crashed.lookup(52), archive.StatusNotYetCovered, "recovered: a slot from the lost batch")

	// The indexer resumes from synced_to and replays the batch it lost. It
	// succeeds, and it converges on exactly the root the crash threw away:
	// the orphaned blocks are simply adopted again, because identical content
	// has identical CIDs.
	res, err := crashed.h.ApplyRefs(crashed.ctx, second, 55)
	if err != nil {
		t.Fatalf("replaying the lost batch: %v", err)
	}
	if res.NoOp {
		t.Errorf("replaying a batch the crash lost reported NoOp; it was never applied")
	}
	if res.Root != newRoot {
		t.Errorf("replayed root %s, want the root the crash lost, %s", res.Root, newRoot)
	}
	wantBlobs(t, crashed.lookup(49), "replayed: slot 49", 490)
	wantBlobs(t, crashed.lookup(52), "replayed: slot 52", 520, 521)
	wantBlobs(t, crashed.lookup(41), "replayed: the older row survives", 410)
}

// TestCrashMidSequenceReplays: crash after every batch of a sequence, each time
// resuming from the last published root, and the head still converges on the
// root an uninterrupted run reaches.
func TestCrashMidSequenceReplays(t *testing.T) {
	rng := newRNG(t)
	ref := newHarness(t, testParams())
	batches := generateBatches(ref, rng, 12)
	for _, b := range batches {
		ref.apply(b.rows, b.syncedTo)
	}
	want := ref.h.Root()

	for crashAt := range batches {
		hs := newHarnessOver(t, testParams(), ref.bs, ref.cat)
		roots := []archive.ApplyResult{}
		for i, b := range batches {
			res := hs.apply(b.rows, b.syncedTo)
			roots = append(roots, res)
			if i != crashAt {
				continue
			}
			// Die here: reload from the root published one batch ago, or from
			// an empty head if the very first batch is the one lost.
			var resume cid.Cid
			if i > 0 {
				resume = roots[i-1].Root
			} else {
				resume = newHarnessOver(t, testParams(), ref.bs, ref.cat).h.Root()
			}
			hs = hs.reload(t, resume)
			// Replay the batch that was lost, then carry on.
			hs.apply(b.rows, b.syncedTo)
		}
		if got := hs.h.Root(); got != want {
			t.Fatalf("crash after batch %d: final root %s, want %s", crashAt, got, want)
		}
	}
}

// TestReplayEveryBatch is spec 13.6's "replay every ingest call": apply every
// batch of a random sequence twice. Each second call must be a verified no-op,
// and the final root must equal the one a single pass reaches.
func TestReplayEveryBatch(t *testing.T) {
	rng := newRNG(t)
	ref := newHarness(t, testParams())
	batches := generateBatches(ref, rng, 20)

	for _, b := range batches {
		ref.apply(b.rows, b.syncedTo)
	}
	want := ref.h.Root()

	hs := newHarnessOver(t, testParams(), ref.bs, ref.cat)
	for i, b := range batches {
		first := hs.apply(b.rows, b.syncedTo)
		if first.NoOp {
			t.Fatalf("batch %d reported NoOp on its first application", i)
		}
		second := hs.apply(b.rows, b.syncedTo)
		if !second.NoOp {
			t.Fatalf("batch %d replayed: NoOp = false, want true", i)
		}
		if second.Root != first.Root {
			t.Fatalf("batch %d replayed: root %s, want the unchanged %s", i, second.Root, first.Root)
		}
	}
	if got := hs.h.Root(); got != want {
		t.Errorf("final root after replaying every batch = %s, want %s", got, want)
	}
}

// TestReplayAfterReload: a replay is verified against the DAG, not against
// anything the process remembers, so it survives a restart.
func TestReplayAfterReload(t *testing.T) {
	hs := newHarness(t, testParams())
	batch := []archive.RefRow{hs.row(41, 410), hs.row(49, 490, 491)}
	hs.apply(batch, 55)
	root := hs.h.Root()

	fresh := hs.reload(t, root)
	res, err := fresh.h.ApplyRefs(fresh.ctx, batch, 55)
	if err != nil {
		t.Fatalf("replaying after a reload: %v", err)
	}
	if !res.NoOp || res.Root != root {
		t.Errorf("replay after reload: NoOp = %t root = %s, want true and %s", res.NoOp, res.Root, root)
	}

	// A batch that contradicts the DAG is still caught after a reload.
	bad := []archive.RefRow{fresh.row(41, 999)}
	wantConflict(t, fresh.applyErr(bad, 55), "contradicting replay after reload")
	if fresh.h.Root() != root {
		t.Errorf("a rejected replay changed the root")
	}
}

// TestTruncateThenReplayInterleaved: a truncate lands between replays. The
// batch that was truncated away replays as a real apply (it is new ground
// again), and the head converges on the original root.
func TestTruncateThenReplayInterleaved(t *testing.T) {
	hs := newHarness(t, testParams())
	b1 := []archive.RefRow{hs.row(41, 410), hs.row(45, 450)}
	b2 := []archive.RefRow{hs.row(49, 490), hs.row(53, 530, 531)}
	b3 := []archive.RefRow{hs.row(57, 570)}

	hs.apply(b1, 47)
	hs.apply(b2, 55)
	hs.apply(b3, 63)
	want := hs.h.Root()

	// Replaying now is a no-op.
	if res := hs.apply(b2, 55); !res.NoOp {
		t.Errorf("replaying b2 before the truncate: NoOp = false")
	}

	// Rewind past b2 and b3.
	if _, err := hs.h.Truncate(hs.ctx, 47); err != nil {
		t.Fatalf("Truncate(47): %v", err)
	}
	wantBlobs(t, hs.lookup(41), "after truncate: b1 survives", 410)
	wantStatus(t, hs.lookup(49), archive.StatusNotYetCovered, "after truncate: b2 is gone")

	// b1 is still covered, so replaying it is still a verified no-op.
	if res := hs.apply(b1, 47); !res.NoOp {
		t.Errorf("replaying b1 after the truncate: NoOp = false, want true")
	}

	// b2 is new ground again: the same call that was a no-op a moment ago now
	// really applies.
	if res := hs.apply(b2, 55); res.NoOp {
		t.Errorf("re-applying the truncated-away b2 reported NoOp")
	}
	hs.apply(b3, 63)

	if got := hs.h.Root(); got != want {
		t.Errorf("root after truncate and replay = %s, want the original %s", got, want)
	}
	wantBlobs(t, hs.lookup(53), "rebuilt: slot 53", 530, 531)
	wantBlobs(t, hs.lookup(57), "rebuilt: slot 57", 570)
}

// TestOldRootSurvivesTruncate: truncation is copy-on-write like everything
// else, so the pre-truncate root still loads and still serves the rows the
// truncate dropped. That is what makes an emergency truncate recoverable.
func TestOldRootSurvivesTruncate(t *testing.T) {
	hs := newHarness(t, testParams())
	d := hs.spread(4)
	hs.apply(d.rows, d.syncedTo)
	before := hs.h.Root()

	if _, err := hs.h.Truncate(hs.ctx, 45); err != nil {
		t.Fatalf("Truncate(45): %v", err)
	}
	wantStatus(t, hs.lookup(57), archive.StatusNotYetCovered, "a row the truncate dropped")

	old := hs.reload(t, before)
	if synced, _ := old.h.SyncedTo(); synced != d.syncedTo {
		t.Errorf("the pre-truncate root reports synced_to %d, want %d", synced, d.syncedTo)
	}
	wantBlobs(t, old.lookup(57), "pre-truncate root still serves the dropped row", 561)
}
