package server

// Internal unit tests for the response-memory admission of the safety boundary. They
// live in package server because the weighted semaphore and its per-entry
// weights are unexported: they are an implementation detail of the read path,
// not API.

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/blobarchive/bloar/schema"
)

// reserveResult carries a reserve's outcome out of a goroutine, so the test body
// -- not the goroutine -- is the only thing that touches *testing.T.
type reserveResult struct {
	weight int64
	err    error
}

// TestAdmissionReserveBlocksUntilRelease is the saturation half of the safety boundary's
// budget: a reservation the budget cannot currently grant waits, and is admitted
// the moment an outstanding one is released.
func TestAdmissionReserveBlocksUntilRelease(t *testing.T) {
	// A budget of exactly one JSON entry, so a single reserve saturates it.
	a := newAdmission(weightPerEntryJSON)
	first, err := a.reserve(context.Background(), 1, false)
	if err != nil {
		t.Fatalf("first reserve: %v", err)
	}

	got := make(chan reserveResult, 1)
	go func() {
		w, err := a.reserve(context.Background(), 1, false)
		got <- reserveResult{w, err}
	}()

	// The second reserve cannot be granted while the first is outstanding.
	select {
	case <-got:
		t.Fatal("second reserve was granted while the budget was fully held")
	case <-time.After(50 * time.Millisecond):
	}

	// Releasing the first unblocks it.
	a.release(first)
	select {
	case res := <-got:
		if res.err != nil {
			t.Fatalf("second reserve after release: %v", res.err)
		}
		a.release(res.weight)
	case <-time.After(time.Second):
		t.Fatal("second reserve did not proceed after the first was released")
	}
}

// TestAdmissionReserveCancelReturnsContextError is the context-sensitivity half:
// a waiter whose context ends while it waits returns that error promptly and
// leaves the queue holding no budget.
func TestAdmissionReserveCancelReturnsContextError(t *testing.T) {
	a := newAdmission(weightPerEntryJSON)
	held, err := a.reserve(context.Background(), 1, false)
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan reserveResult, 1)
	go func() {
		w, err := a.reserve(ctx, 1, false)
		done <- reserveResult{w, err}
	}()

	// Let the waiter park on the saturated budget, then cancel it.
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case res := <-done:
		if res.err == nil {
			a.release(res.weight)
			t.Fatal("cancelled reserve was granted; want a context error")
		}
		if !errors.Is(res.err, context.Canceled) {
			t.Fatalf("cancelled reserve returned %v, want context.Canceled", res.err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancelled reserve did not return promptly")
	}

	// The cancelled waiter took nothing: releasing the original frees the whole
	// budget, so a fresh full-budget reserve succeeds without blocking.
	a.release(held)
	w, err := a.reserve(context.Background(), 1, false)
	if err != nil {
		t.Fatalf("reserve after cancel: %v", err)
	}
	a.release(w)
}

// TestReserveZeroEntriesIsFree covers the blobless response: it reserves nothing
// and needs no budget, so it can never be the thing a saturated budget blocks.
func TestReserveZeroEntriesIsFree(t *testing.T) {
	a := newAdmission(1) // a budget too small for any real entry
	w, err := a.reserve(context.Background(), 0, false)
	if err != nil || w != 0 {
		t.Fatalf("reserve(0) = (%d, %v), want (0, nil)", w, err)
	}
	a.release(w)
}

// TestMaxResponseWeight pins the per-entry weights and the budget floor they
// imply, so a change to either is a change a reviewer has to see.
func TestMaxResponseWeight(t *testing.T) {
	if weightPerEntryOctet != 2*schema.BlobSize {
		t.Errorf("octet weight = %d, want 2*BlobSize", weightPerEntryOctet)
	}
	// The JSON weight is the exact single-entry peak: one raw blob plus the
	// one-entry rendered buffer. That is what makes it a provable bound rather
	// than a guessed constant; blobsJSONSize, the renderer's own
	// size function, is the authority on the buffer term.
	if want := int64(schema.BlobSize + blobsJSONSize(1)); weightPerEntryJSON != want {
		t.Errorf("json weight = %d, want the single-entry peak %d (BlobSize + blobsJSONSize(1))", weightPerEntryJSON, want)
	}
	// The floor is the heavier (JSON) encoding of the most entries a response can
	// carry. The query cap is enforced at most the stored-row ceiling, so an
	// unfiltered read's ceiling is always the larger and the floor sits at it.
	ceilingFloor := int64(schema.MaxBlobsPerSlotCeiling) * weightPerEntryJSON
	if got := MaxResponseWeight(schema.MaxBlobsPerSlotCeiling); got != ceilingFloor {
		t.Errorf("MaxResponseWeight(ceiling) = %d, want %d", got, ceilingFloor)
	}
	// A cap below the ceiling cannot lower the floor: an unfiltered response
	// still reads up to the ceiling.
	if got := MaxResponseWeight(1); got != ceilingFloor {
		t.Errorf("MaxResponseWeight(1) = %d, want the ceiling floor %d", got, ceilingFloor)
	}
}

// TestMaxResponseWeightDoesNotOverflow: no input may wrap int64 into a small or
// negative floor that a tiny budget could then pass. Above the
// enforced query ceiling the value only grows, and an extreme input clamps to
// MaxInt64 rather than overflowing.
func TestMaxResponseWeightDoesNotOverflow(t *testing.T) {
	floor := int64(schema.MaxBlobsPerSlotCeiling) * weightPerEntryJSON
	for _, n := range []int{1, schema.MaxBlobsPerSlotCeiling, 1 << 30, math.MaxInt} {
		if got := MaxResponseWeight(n); got < floor {
			t.Errorf("MaxResponseWeight(%d) = %d, below the ceiling floor %d (overflow?)", n, got, floor)
		}
	}
	if got := MaxResponseWeight(math.MaxInt); got != math.MaxInt64 {
		t.Errorf("MaxResponseWeight(MaxInt) = %d, want it clamped to MaxInt64", got)
	}
}
