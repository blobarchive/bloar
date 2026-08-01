package pinning_test

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/blobarchive/bloar/archive"
	"github.com/blobarchive/bloar/pinning"
)

func newBatchHead(t *testing.T, f *fixture, name string, origin, syncedTo, blobID uint64) *archive.Head {
	t.Helper()
	head, err := archive.New(f.ctx, archive.Config{Blocks: f.bs, Resolver: f.cat}, archive.Params{
		Name: name, Net: "testnet", OriginSlot: origin,
		SegBits: testSegBits, FanoutBits: testFanoutBits,
	})
	if err != nil {
		t.Fatalf("archive.New(%q): %v", name, err)
	}
	f.apply(head, syncedTo, f.row(origin, blobID))
	return head
}

func TestPrepareSetBatchValidatesBeforeAnyMutation(t *testing.T) {
	f := newFixture(t)
	a := f.head("a", pinning.Full())
	f.apply(a, 11, f.row(8, 1))
	f.reconcileAll()
	baselineNames := f.rec.Names()
	baselinePins := f.pins("a")
	mismatched := newBatchHead(t, f, "other", 8, 11, 2)

	tests := []struct {
		name          string
		registrations []pinning.Registration
	}{
		{"duplicate", []pinning.Registration{{Name: "a", Policy: pinning.Full()}, {Name: "a", Policy: pinning.Full()}}},
		{"empty name", []pinning.Registration{{Name: "", Policy: pinning.Full()}}},
		{"invalid name", []pinning.Registration{{Name: "bad_name", Policy: pinning.Full()}}},
		{"reserved staging name", []pinning.Registration{{Name: pinning.StagingHead, Policy: pinning.Full()}}},
		{"invalid policy", []pinning.Registration{{Name: "a", Policy: pinning.Policy{Mode: pinning.Mode(99)}}}},
		{"head name mismatch", []pinning.Registration{{Name: "a", Head: mismatched, Policy: pinning.Full()}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			apply, err := f.rec.PrepareSetBatch(tc.registrations)
			if err == nil || apply != nil {
				t.Fatalf("PrepareSetBatch = (apply_non_nil=%t, err=%v), want nil apply and validation error", apply != nil, err)
			}
			if got := f.rec.Names(); !slices.Equal(got, baselineNames) {
				t.Fatalf("failed preparation changed Names: got %v want %v", got, baselineNames)
			}
			if got := f.pins("a"); !slices.Equal(got, baselinePins) {
				t.Fatalf("failed preparation changed ledger pins: got %#v want %#v", got, baselinePins)
			}
		})
	}

	// A successful preparation is still side-effect free until its returned
	// visibility closure is explicitly applied.
	replacement := newBatchHead(t, f, "a", 12, 15, 3)
	apply, err := f.rec.PrepareSetBatch([]pinning.Registration{{Name: "a", Head: replacement, Policy: pinning.Full()}})
	if err != nil {
		t.Fatalf("valid PrepareSetBatch: %v", err)
	}
	if got := f.rec.Names(); !slices.Equal(got, baselineNames) {
		t.Fatalf("successful preparation changed Names before apply: got %v want %v", got, baselineNames)
	}
	if got := f.pins("a"); !slices.Equal(got, baselinePins) {
		t.Fatalf("successful preparation changed pins before apply: got %#v want %#v", got, baselinePins)
	}
	_ = apply
}

func TestPreparedSetBatchPatchesNamedRegistrationsAndDrainsWithdrawal(t *testing.T) {
	f := newFixture(t)
	a := f.head("a", pinning.Full())
	b := f.head("b", pinning.Full())
	c := f.head("c", pinning.Full())
	f.apply(a, 11, f.row(8, 11))
	f.apply(b, 11, f.row(8, 12))
	f.apply(c, 11, f.row(8, 13))
	f.reconcileAll()
	oldA, oldB, rootC := a.Root(), b.Root(), c.Root()

	replacementA := newBatchHead(t, f, "a", 12, 15, 14)
	apply, err := f.rec.PrepareSetBatch([]pinning.Registration{
		{Name: "a", Head: replacementA, Policy: pinning.Full()},
		{Name: "b", Head: nil, Policy: pinning.Full()},
	})
	if err != nil {
		t.Fatalf("PrepareSetBatch: %v", err)
	}
	// Preparation has no effect; apply performs one in-memory patch but does not
	// edit the ledger itself.
	if got := f.rec.Names(); !slices.Equal(got, []string{"a", "b", "c"}) {
		t.Fatalf("Names before apply = %v", got)
	}
	apply()
	if got := f.rec.Names(); !slices.Equal(got, []string{"a", "b", "c"}) {
		t.Fatalf("Names after apply = %v; withdrawal must remain as a tombstone", got)
	}
	if !hasPinAt(f.pins("a"), pinning.PurposeRoot, oldA, true) ||
		!hasPinAt(f.pins("b"), pinning.PurposeRoot, oldB, true) ||
		!hasPinAt(f.pins("c"), pinning.PurposeRoot, rootC, true) {
		t.Fatal("apply mutated ledger rows before reconciliation")
	}

	f.reconcileAll()
	if got := f.rec.Names(); !slices.Equal(got, []string{"a", "c"}) {
		t.Fatalf("Names after reconciliation = %v, want [a c]", got)
	}
	if pins := f.pins("a"); len(pins) != 1 || !hasPinAt(pins, pinning.PurposeRoot, replacementA.Root(), true) {
		t.Fatalf("replacement pins for a = %#v", pins)
	}
	if pins := f.pins("b"); len(pins) != 0 {
		t.Fatalf("withdrawn b retained pins: %#v", pins)
	}
	if pins := f.pins("c"); len(pins) != 1 || !hasPinAt(pins, pinning.PurposeRoot, rootC, true) {
		t.Fatalf("unrelated c registration/pins changed: %#v", pins)
	}
}

func TestPreparedSetBatchWakesTheReconciler(t *testing.T) {
	f := newFixture(t)
	old := f.head("wake", pinning.Full())
	f.apply(old, 11, f.row(8, 21))
	f.reconcileAll()
	replacement := newBatchHead(t, f, "wake", 12, 15, 22)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := f.rec.Run(ctx); err != nil {
			t.Errorf("Reconciler.Run: %v", err)
		}
	}()
	apply, err := f.rec.PrepareSetBatch([]pinning.Registration{{Name: "wake", Head: replacement, Policy: pinning.Full()}})
	if err != nil {
		t.Fatalf("PrepareSetBatch: %v", err)
	}
	apply()
	waitFor(t, "prepared batch wake to reconcile replacement", func() bool {
		pins := f.pins("wake")
		return len(pins) == 1 && hasPinAt(pins, pinning.PurposeRoot, replacement.Root(), true)
	})
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Reconciler.Run did not stop")
	}
}
