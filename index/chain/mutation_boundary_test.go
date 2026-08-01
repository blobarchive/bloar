package chain

// These tests cover the index/chain mutation boundary. The exported mutation
// boundary must fail closed until the configured schedule is verified against
// the head's published manifest tip, and a
// running indexer rereads that tip every poll and revalidates on any change. The
// server-side pieces (the presence-aware expected_manifest decode) live in
// server/.

import (
	"context"
	"errors"
	"math/big"
	"strings"
	"testing"
	"time"
)

// newBatchChain builds blocks 0..21 with one inbox batch at slot 21 -- the fixture
// the boundary tests scan. With the fake archive's synced_to at 10 and its all head
// at 21, a scan of [11, 21] finds the batch and posts one refs row, so a mutation
// is observable whenever the guard under test is absent.
func newBatchChain(t *testing.T) *fakeChain {
	b := newChainBuilder(t)
	for n := uint64(0); n < 21; n++ {
		b.addBlock(n)
	}
	tx := blobTx(t, keyA, testInbox, 0, hashes(1))
	b.addBlock(21, txEntry{tx: tx, logAddr: testInbox, logTopic: testTopic})
	return b.chain()
}

// mismatchSchedule is a structurally valid schedule that differs from the fixtures'
// published tip (which attests inboxOpen(testInbox, 0)), so CheckSchedule refuses it
// by exact-equality.
func mismatchSchedule() []Source {
	return []Source{inboxRange(testInbox, 0, 20), inboxOpen(otherAddr, 21)}
}

// TestStepFailsClosedUntilVerified verifies that Step, the exported mutation
// boundary, and it must refuse to scan or write until CheckSchedule has bound the
// configured schedule to the head's published tip. A direct Step -- an embedding
// caller reaching past Run -- otherwise builds an unattested chain: on a chainless
// head it omits expected_manifest, which the head accepts, and commits refs no
// manifest ever attested.
func TestStepFailsClosedUntilVerified(t *testing.T) {
	fc := newBatchChain(t)

	t.Run("chainless head", func(t *testing.T) {
		state, client := newAuditChainlessArchive(t)
		ix := newAuditBoundaryIndexer(t, fc, client, []Source{inboxOpen(testInbox, 0)})

		if _, err := ix.Step(context.Background()); !errors.Is(err, errScheduleUnverified) {
			t.Fatalf("Step on an unverified indexer = %v, want errScheduleUnverified", err)
		}
		if rows, _ := state.posted(); rows != 0 {
			t.Fatalf("a guarded Step recorded %d refs rows against a chainless head, want 0", rows)
		}
	})

	t.Run("schedule mismatches the published tip", func(t *testing.T) {
		state, client := newAuditManifestArchive(t, []Source{inboxOpen(testInbox, 0)})
		ix := newAuditBoundaryIndexer(t, fc, client, mismatchSchedule())

		if _, err := ix.Step(context.Background()); !errors.Is(err, errScheduleUnverified) {
			t.Fatalf("Step on an unverified indexer = %v, want errScheduleUnverified", err)
		}
		if rows, _ := state.posted(); rows != 0 {
			t.Fatalf("a guarded Step recorded %d refs rows under a mismatched schedule, want 0", rows)
		}
	})
}

// TestRunFailsClosedWithoutVerification verifies that Run performs the
// schedule check itself, so an embedding caller that drives Run directly -- with no
// prior CheckSchedule -- still fails closed. A chainless head has nothing to attest
// the schedule; a schedule that does not equal the published tip must not run.
// Either way Run returns the refusal and records nothing.
func TestRunFailsClosedWithoutVerification(t *testing.T) {
	fc := newBatchChain(t)

	t.Run("chainless head", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		state, client := newAuditChainlessArchive(t)
		ix := newAuditBoundaryIndexer(t, fc, client, []Source{inboxOpen(testInbox, 0)})

		err := ix.Run(ctx)
		if err == nil || !strings.Contains(err.Error(), "no published manifest chain") {
			t.Fatalf("Run against a chainless head = %v, want a no-manifest-chain refusal", err)
		}
		if rows, _ := state.posted(); rows != 0 {
			t.Fatalf("Run recorded %d refs rows against a chainless head, want 0", rows)
		}
	})

	t.Run("schedule mismatches the published tip", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		state, client := newAuditManifestArchive(t, []Source{inboxOpen(testInbox, 0)})
		ix := newAuditBoundaryIndexer(t, fc, client, mismatchSchedule())

		err := ix.Run(ctx)
		if err == nil || !strings.Contains(err.Error(), "does not equal its published manifest tip") {
			t.Fatalf("Run under a mismatched schedule = %v, want a schedule-inequality refusal", err)
		}
		if rows, _ := state.posted(); rows != 0 {
			t.Fatalf("Run recorded %d refs rows under a mismatched schedule, want 0", rows)
		}
	})
}

// TestPerPollManifestTipRereadAdopts covers the adopt path: the per-poll reread
// detects a tip that changed under a running indexer -- without waiting for a refs
// write -- and, when the new tip still attests the configured schedule, adopts it.
// verified stays set, the carried expected_manifest advances to the new CID, and
// subsequent refs bind to it.
func TestPerPollManifestTipRereadAdopts(t *testing.T) {
	fc := newBatchChain(t)
	sched := []Source{inboxOpen(testInbox, 0)}
	state, client := newAuditManifestArchive(t, sched)
	ix := newAuditBoundaryIndexer(t, fc, client, sched)

	if err := ix.CheckSchedule(context.Background()); err != nil {
		t.Fatalf("initial CheckSchedule: %v", err)
	}
	firstTip := ix.manifestTip

	// The operator re-encodes the manifest -- same schedule, new tip CID. No refs
	// have been written, so the reread alone must notice.
	state.republish()
	newTip := state.tip()
	if newTip == firstTip {
		t.Fatal("republish did not change the tip CID; the reread would be vacuous")
	}

	if err := ix.reconcileManifestTip(context.Background()); err != nil {
		t.Fatalf("reread of an equal-schedule tip should adopt, got %v", err)
	}
	if !ix.verified {
		t.Fatal("adoption cleared the verified state")
	}
	if ix.manifestTip != newTip {
		t.Fatalf("adopted tip = %q, want the republished tip %q", ix.manifestTip, newTip)
	}
	if rows, _ := state.posted(); rows != 0 {
		t.Fatalf("the reread wrote %d refs rows; detection must be a read, not a write", rows)
	}

	// The next scan binds its refs to the adopted tip.
	advanced, err := ix.Step(context.Background())
	if err != nil || !advanced {
		t.Fatalf("Step after adoption: advanced = %v, err = %v", advanced, err)
	}
	if got := state.lastExpectedManifest(); got != newTip {
		t.Fatalf("refs after adoption carried expected_manifest %q, want the adopted tip %q", got, newTip)
	}
}

// TestPerPollManifestTipRereadFailsClosed covers the fail-closed path: a reread
// that finds a tip whose schedule no longer equals the config exits loudly and
// clears the verified state, so nothing downstream can commit against the stale tip
// -- and a subsequent Step refuses on the verification guard, having no further mutation.
func TestPerPollManifestTipRereadFailsClosed(t *testing.T) {
	fc := newBatchChain(t)
	sched := []Source{inboxOpen(testInbox, 0)}
	state, client := newAuditManifestArchive(t, sched)
	ix := newAuditBoundaryIndexer(t, fc, client, sched)

	if err := ix.CheckSchedule(context.Background()); err != nil {
		t.Fatalf("initial CheckSchedule: %v", err)
	}

	// A real manifest upgrade to a different schedule lands under the running process.
	state.setManifest(mismatchSchedule())

	err := ix.reconcileManifestTip(context.Background())
	if err == nil || !strings.Contains(err.Error(), "changed under this running indexer") {
		t.Fatalf("reread of a mismatched tip = %v, want a loud change-under-us refusal", err)
	}
	if ix.verified {
		t.Fatal("a failed revalidation left the verified state set")
	}
	if rows, _ := state.posted(); rows != 0 {
		t.Fatalf("the reread wrote %d refs rows before failing; detection must be a read", rows)
	}

	// The cleared state now bars Step too: no further mutation.
	if _, err := ix.Step(context.Background()); !errors.Is(err, errScheduleUnverified) {
		t.Fatalf("Step after a failed reread = %v, want errScheduleUnverified", err)
	}
	if rows, _ := state.posted(); rows != 0 {
		t.Fatalf("Step after a failed reread recorded %d rows, want 0", rows)
	}
}

// TestRunRereadsManifestTipEachPoll drives the check through the real Run loop:
// a caught-up process with no new finalized work, which
// the commit-time binding alone never catches because it never writes. The head
// starts at its synced_to with nothing left to scan; a manifest upgrade to a
// different schedule lands while Run idles, and the per-poll reread must make Run
// exit loudly rather than idle forever on the superseded schedule.
func TestRunRereadsManifestTipEachPoll(t *testing.T) {
	// block n lands in slot n, so with the fake's synced_to at slot 10 and the
	// finalized tag pinned to block 10, resume finds nothing past coverage: caught up.
	fc := buildLinearChain(t, 10)
	sched := []Source{inboxOpen(testInbox, 0)}
	state, client := newAuditManifestArchive(t, sched)
	ix := newAuditBoundaryIndexer(t, fc, client, sched)
	ix.finalized = big.NewInt(10)

	done := make(chan error, 1)
	go func() { done <- ix.Run(context.Background()) }()

	// Wait until Run is past its startup CheckSchedule and inside the poll loop --
	// two manifest GETs: the startup check, then the first reread -- so the upgrade
	// below is detected by the reread, not the startup check.
	waitForCount(t, "manifest GETs", state.manifestGetCount, 2)

	state.setManifest(mismatchSchedule())

	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "changed under this running indexer") {
			t.Fatalf("Run exit = %v, want a loud per-poll change refusal", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not exit after a caught-up manifest upgrade; a stale process idled on")
	}
	if rows, _ := state.posted(); rows != 0 {
		t.Fatalf("a caught-up Run wrote %d refs rows, want 0", rows)
	}
}

// newSenderScanChain builds blocks 0..21 with one blob tx from senderA to otherAddr
// at slot 21. A blob-txs source reads block bodies (no log), so this is the fixture
// for a schedule that selects on address + sender allowlist: schedule A selects
// exactly this one tx, and any drift in the source's address or its allowlist drops
// it -- which makes a mutation of the verified schedule observable as a lost row.
func newSenderScanChain(t *testing.T) *fakeChain {
	b := newChainBuilder(t)
	for n := uint64(0); n < 21; n++ {
		b.addBlock(n)
	}
	b.addBlock(21, txEntry{tx: blobTx(t, keyA, otherAddr, 0, hashes(1))})
	return b.chain()
}

// TestVerifiedScheduleIsImmutableSnapshot verifies that New deep-copies the
// schedule, so a caller that retains and mutates the
// slices it passed cannot change what a verified indexer scans. Schedule A is a
// blob-txs source allowlisting senderA over otherAddr, selecting the fixture's one
// matching tx. After CheckSchedule verifies A, the caller mutates the slices it
// still holds -- both the outer source element and the nested Senders backing -- and
// Step must still scan A (one row), bound to A's tip.
func TestVerifiedScheduleIsImmutableSnapshot(t *testing.T) {
	fc := newSenderScanChain(t)

	// verify wires a fresh indexer whose configured schedule and whose published tip
	// are INDEPENDENT slices, so mutating the caller's copy below cannot reach into
	// the fake's stored manifest. It returns the caller's retained schedule slice.
	verify := func(t *testing.T) (*auditManifestArchive, *Indexer, []Source) {
		t.Helper()
		state, client := newAuditManifestArchive(t, []Source{blobTxs(otherAddr, 0, senderA)})
		callerSources := []Source{blobTxs(otherAddr, 0, senderA)}
		ix := newAuditBoundaryIndexer(t, fc, client, callerSources)
		if err := ix.CheckSchedule(context.Background()); err != nil {
			t.Fatalf("CheckSchedule against schedule A: %v", err)
		}
		return state, ix, callerSources
	}

	assertScansA := func(t *testing.T, state *auditManifestArchive, ix *Indexer) {
		t.Helper()
		advanced, err := ix.Step(context.Background())
		if err != nil || !advanced {
			t.Fatalf("Step after verification: advanced = %v, err = %v", advanced, err)
		}
		if rows, _ := state.posted(); rows != 1 {
			t.Fatalf("Step recorded %d rows; schedule A selects exactly 1, so the verified schedule "+
				"changed under the caller", rows)
		}
		if got := state.lastExpectedManifest(); got != state.tip() {
			t.Fatalf("refs carried expected_manifest %q, want the verified tip %q", got, state.tip())
		}
	}

	t.Run("outer element and nested sender both mutated", func(t *testing.T) {
		state, ix, callerSources := verify(t)
		callerSources[0].Address = addrInbox  // outer source element field
		callerSources[0].Senders[0] = addrEOA // nested Senders backing
		assertScansA(t, state, ix)
	})

	t.Run("nested sender allowlist mutated alone", func(t *testing.T) {
		// Guards specifically against an outer-only copy: the source element is
		// left alone, only the Senders backing is swapped, and A must still hold.
		state, ix, callerSources := verify(t)
		callerSources[0].Senders[0] = addrEOA
		assertScansA(t, state, ix)
	})
}

// TestFailedCheckScheduleReclosesBoundary verifies that a direct CheckSchedule
// failure re-closes the mutation boundary rather than leaving an
// earlier success active. After verifying schedule A, the published tip moves to a
// schedule the config no longer equals; the failed recheck must clear the verified
// state, so a direct Step refuses rather than committing against the superseded tip.
func TestFailedCheckScheduleReclosesBoundary(t *testing.T) {
	fc := newBatchChain(t)
	sched := []Source{inboxOpen(testInbox, 0)}
	state, client := newAuditManifestArchive(t, sched)
	ix := newAuditBoundaryIndexer(t, fc, client, sched)

	if err := ix.CheckSchedule(context.Background()); err != nil {
		t.Fatalf("initial CheckSchedule: %v", err)
	}
	// The tip moves to a schedule the config does not equal; a direct recheck fails.
	state.setManifest(mismatchSchedule())
	if err := ix.CheckSchedule(context.Background()); err == nil {
		t.Fatal("CheckSchedule accepted a tip whose schedule differs from the config")
	}
	// The failed recheck re-closed the boundary: Step refuses, and records nothing.
	if _, err := ix.Step(context.Background()); !errors.Is(err, errScheduleUnverified) {
		t.Fatalf("Step after a failed recheck = %v, want errScheduleUnverified", err)
	}
	if rows, _ := state.posted(); rows != 0 {
		t.Fatalf("Step after a failed recheck recorded %d rows, want 0", rows)
	}
}

// waitForCount blocks until count() reaches want, failing the test if it does not
// in time.
func waitForCount(t *testing.T, what string, count func() int, want int) {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for count() < want {
		select {
		case <-deadline:
			t.Fatalf("%s reached %d, want >= %d", what, count(), want)
		case <-time.After(time.Millisecond):
		}
	}
}
