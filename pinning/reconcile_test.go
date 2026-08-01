package pinning_test

import (
	"context"
	"testing"
	"time"

	"github.com/blobarchive/bloar/archive"
	"github.com/blobarchive/bloar/metrics"
	"github.com/blobarchive/bloar/pinning"
)

// TestReconcileIsIdempotent: a pass over an unchanged head writes nothing. The
// ledger is a sync write per row, so churn here is disk traffic proportional to
// the index on every timer tick.
func TestReconcileIsIdempotent(t *testing.T) {
	f := newFixture(t)
	win := f.head("win", pinning.Window(slotsDur(8), testSecondsPerSlot))
	f.apply(win, 20, f.row(8, 1), f.row(12, 2), f.row(20, 3))

	first := f.reconcileAll()
	if first.Added == 0 {
		t.Fatal("the first pass added no pins")
	}
	if first.Removed != 0 {
		t.Errorf("the first pass removed %d pins from an empty ledger", first.Removed)
	}
	for i := range 3 {
		if delta := f.reconcileAll(); delta != (pinning.Delta{}) {
			t.Fatalf("pass %d = %+v, want no churn", i+2, delta)
		}
	}
}

// TestReconcileWindowSlide is the purpose vocabulary doing its job: a sealed
// segment that falls out of the window is rewritten from a recursive window pin
// to a direct index pin. The block does not move; its lifetime does.
func TestReconcileWindowSlide(t *testing.T) {
	f := newFixture(t)
	win := f.head("win", pinning.Window(slotsDur(8), testSecondsPerSlot))

	f.apply(win, 11, f.row(8, 1))
	f.reconcileAll()

	sealed := f.enumerate(win).Sealed
	if len(sealed) != 1 {
		t.Fatalf("expected one sealed segment, got %d", len(sealed))
	}
	seg := sealed[0].CID
	if !hasPinAt(f.pins("win"), pinning.PurposeWindow, seg, true) {
		t.Fatal("the sealed segment is not pinned under window/recursive while it is in range")
	}

	// Slide: synced_to 20 puts window 2 (8..11) outside [12, 20].
	f.apply(win, 20, f.row(20, 2))
	f.reconcileAll()

	if hasPinAt(f.pins("win"), pinning.PurposeWindow, seg, true) {
		t.Error("the sealed segment is still pinned under window/recursive after the window slid past it")
	}
	if !hasPinAt(f.pins("win"), pinning.PurposeIndex, seg, false) {
		t.Error("the sealed segment is not pinned under index/direct after the window slid past it; the index must stay complete")
	}
}

// TestReconcilePolicyChange: a head whose policy changed converges to the new
// one and leaves no rows from the old. Purposes are not ledger garbage that
// accumulates.
func TestReconcilePolicyChange(t *testing.T) {
	f := newFixture(t)
	h := f.head("h", pinning.Full())
	f.apply(h, 20, f.row(8, 1), f.row(20, 2))
	f.reconcileAll()

	// A second reconciler over the same ledger, under a different policy: what
	// a restart with an edited config does.
	rec, err := pinning.NewReconciler(pinning.Config{Ledger: f.led})
	if err != nil {
		t.Fatalf("NewReconciler: %v", err)
	}
	if err := rec.Add(h, pinning.None()); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if _, err := rec.ReconcileAll(f.ctx); err != nil {
		t.Fatalf("ReconcileAll: %v", err)
	}

	for _, e := range f.pins("h") {
		if e.Recursive {
			t.Errorf("pin %s/%s is still recursive under a none policy", e.Purpose, e.CID)
		}
	}
	if hasPinAt(f.pins("h"), pinning.PurposeRoot, h.Root(), true) {
		t.Error("the old policy's recursive root pin survived the change")
	}
}

func TestReplaceRetargetsHeadAndPreservesPolicy(t *testing.T) {
	f := newFixture(t)
	old := f.head("mutable", pinning.Full())
	f.apply(old, 11, f.row(8, 1))
	f.reconcileAll()
	oldRoot := old.Root()

	replacement, err := archive.New(f.ctx, archive.Config{Blocks: f.bs, Resolver: f.cat}, archive.Params{
		Name: "mutable", Net: "testnet", OriginSlot: 12, SegBits: testSegBits, FanoutBits: testFanoutBits,
	})
	if err != nil {
		t.Fatalf("archive.New replacement: %v", err)
	}
	f.apply(replacement, 15, f.row(12, 2))
	if err := f.rec.Replace(replacement); err != nil {
		t.Fatalf("Replace: %v", err)
	}
	if _, err := f.rec.ReconcileHead(f.ctx, "mutable"); err != nil {
		t.Fatalf("ReconcileHead after Replace: %v", err)
	}

	pins := f.pins("mutable")
	if !hasPinAt(pins, pinning.PurposeRoot, replacement.Root(), true) {
		t.Fatal("replacement root is not recursively pinned under the preserved full policy")
	}
	if hasPinAt(pins, pinning.PurposeRoot, oldRoot, true) {
		t.Fatal("prior generation root remains pinned after replacement reconciliation")
	}
}

func TestReplaceValidatesRegisteredIdentityBeforeMutation(t *testing.T) {
	f := newFixture(t)
	current := f.head("mutable", pinning.Full())

	if err := f.rec.Replace(nil); err == nil {
		t.Fatal("Replace(nil) succeeded")
	}
	unknown, err := archive.New(f.ctx, archive.Config{Blocks: f.bs, Resolver: f.cat}, archive.Params{
		Name: "unknown", Net: "testnet", OriginSlot: 12, SegBits: testSegBits, FanoutBits: testFanoutBits,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := f.rec.Replace(unknown); err == nil {
		t.Fatal("Replace of an unregistered name succeeded")
	}
	changed, err := archive.New(f.ctx, archive.Config{Blocks: f.bs, Resolver: f.cat}, archive.Params{
		Name: "mutable", Net: "othernet", OriginSlot: 12, SegBits: testSegBits, FanoutBits: testFanoutBits,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := f.rec.Replace(changed); err == nil {
		t.Fatal("Replace with changed immutable parameters succeeded")
	}
	if err := f.rec.Replace(current); err != nil {
		t.Fatalf("failed validation mutated the registered pointer: %v", err)
	}
}

func TestBoundReplacementIsInfallibleAfterStartupValidation(t *testing.T) {
	f := newFixture(t)
	old := f.head("mutable", pinning.Full())
	if _, err := f.rec.BindReplacement("missing"); err == nil {
		t.Fatal("BindReplacement accepted an unregistered name")
	}
	bind, err := f.rec.BindReplacement("mutable")
	if err != nil {
		t.Fatalf("BindReplacement: %v", err)
	}
	replacement, err := archive.New(f.ctx, archive.Config{Blocks: f.bs, Resolver: f.cat}, archive.Params{
		Name: "mutable", Net: "testnet", OriginSlot: 12, SegBits: testSegBits, FanoutBits: testFanoutBits,
	})
	if err != nil {
		t.Fatal(err)
	}
	bind(replacement)
	if err := f.rec.Replace(old); err != nil {
		t.Fatalf("bound replacement lost the registration or policy: %v", err)
	}
}

// TestReconcileUnknownHead is an error, not a silent no-op: a caller asking to
// reconcile a head that was never added has lost track of its own wiring, and
// the pass that would have pinned nothing is exactly the one nobody notices.
func TestReconcileUnknownHead(t *testing.T) {
	f := newFixture(t)
	if _, err := f.rec.ReconcileHead(f.ctx, "nope"); err == nil {
		t.Fatal("ReconcileHead of an unregistered head: want an error, got nil")
	}
}

// TestNotifyTriggersReconcile is spec 9's push trigger: a root swap reconciles
// without waiting for the timer.
func TestNotifyTriggersReconcile(t *testing.T) {
	f := newFixture(t)
	// An interval long enough that a pass within the test's patience can only
	// be the push.
	rec, err := pinning.NewReconciler(pinning.Config{Ledger: f.led, Interval: time.Hour})
	if err != nil {
		t.Fatalf("NewReconciler: %v", err)
	}
	h := f.head("h", pinning.Full())
	if err := rec.Add(h, pinning.Full()); err != nil {
		t.Fatalf("Add: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := rec.Run(ctx); err != nil {
			t.Errorf("Run: %v", err)
		}
	}()

	f.apply(h, 11, f.row(8, 1))
	rec.Notify("h")

	waitFor(t, "the pushed root to be pinned", func() bool {
		return hasPinAt(f.pins("h"), pinning.PurposeRoot, h.Root(), true)
	})

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return on cancellation")
	}
}

// TestNotifyCoalesces: several notifications between two passes are one pass,
// and the pass reconciles the head as it is then -- not once per root that went
// by.
func TestNotifyCoalesces(t *testing.T) {
	f := newFixture(t)
	mx := metrics.New()
	rec, err := pinning.NewReconciler(pinning.Config{Ledger: f.led, Interval: time.Hour, Metrics: mx})
	if err != nil {
		t.Fatalf("NewReconciler: %v", err)
	}
	h := f.head("h", pinning.Full())
	if err := rec.Add(h, pinning.Full()); err != nil {
		t.Fatalf("Add: %v", err)
	}

	// Queue the whole burst before the sole worker starts. This makes the
	// coalescing boundary deterministic: every historical root is pending at
	// once, and the worker must reconcile only the current one.
	for slot := uint64(8); slot <= 20; slot++ {
		f.apply(h, slot, f.row(slot, slot))
		rec.Notify("h")
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- rec.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("Run: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("Run did not return on cancellation")
		}
	})

	waitFor(t, "the queued notifications to complete reconciliation", func() bool {
		return reconcilePasses(t, mx, "h") >= 1
	})

	if got := reconcilePasses(t, mx, "h"); got != 1 {
		t.Errorf("the notification burst ran %d reconciliation passes, want exactly 1", got)
	}
	// Completion means the intentionally ordered add-then-remove pass is over,
	// so the final ledger must contain the current root and no historical row.
	pins := f.pins("h")
	if len(pins) != 1 || !hasPinAt(pins, pinning.PurposeRoot, h.Root(), true) {
		t.Errorf("the completed pass left ledger pins %v, want exactly the current root", pins)
	}
}

// TestRunTimerReconciles: the timer alone repairs a head nobody pushed, which
// is what makes a dropped notification a delay rather than a leak.
func TestRunTimerReconciles(t *testing.T) {
	f := newFixture(t)
	rec, err := pinning.NewReconciler(pinning.Config{Ledger: f.led, Interval: 5 * time.Millisecond})
	if err != nil {
		t.Fatalf("NewReconciler: %v", err)
	}
	h := f.head("h", pinning.Full())
	if err := rec.Add(h, pinning.Full()); err != nil {
		t.Fatalf("Add: %v", err)
	}
	f.apply(h, 11, f.row(8, 1))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		if err := rec.Run(ctx); err != nil {
			t.Errorf("Run: %v", err)
		}
	}()

	waitFor(t, "the timer to pin the root without a push", func() bool {
		return hasPinAt(f.pins("h"), pinning.PurposeRoot, h.Root(), true)
	})
}

// TestNotifyUnknownHeadIsHarmless: Notify is called from the mutation path and
// has nowhere to report an error, so an unknown name must be a no-op.
func TestNotifyUnknownHeadIsHarmless(t *testing.T) {
	f := newFixture(t)
	rec, err := pinning.NewReconciler(pinning.Config{Ledger: f.led, Interval: time.Hour})
	if err != nil {
		t.Fatalf("NewReconciler: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := rec.Run(ctx); err != nil {
			t.Errorf("Run: %v", err)
		}
	}()

	rec.Notify("nobody")
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run wedged on a notification for a head it does not have")
	}
}

func TestReconcilerAddRejects(t *testing.T) {
	f := newFixture(t)
	h := f.head("h", pinning.Full()) // already added by the fixture

	if err := f.rec.Add(h, pinning.Full()); err == nil {
		t.Error("Add of a duplicate head: want an error, got nil")
	}
	if err := f.rec.Add(nil, pinning.Full()); err == nil {
		t.Error("Add of a nil head: want an error, got nil")
	}
	if err := f.rec.Add(h, pinning.Window(0, 12)); err == nil {
		t.Error("Add with an invalid policy: want an error, got nil")
	}
}

// waitFor polls cond until it holds, or fails the test.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func reconcilePasses(t *testing.T, mx *metrics.Metrics, head string) uint64 {
	t.Helper()
	families, err := mx.Registry().Gather()
	if err != nil {
		t.Fatalf("gathering metrics: %v", err)
	}
	for _, family := range families {
		if family.GetName() != "bloar_pin_reconcile_duration_seconds" {
			continue
		}
		for _, sample := range family.GetMetric() {
			for _, label := range sample.GetLabel() {
				if label.GetName() == "head" && label.GetValue() == head {
					return sample.GetHistogram().GetSampleCount()
				}
			}
		}
	}
	return 0
}
