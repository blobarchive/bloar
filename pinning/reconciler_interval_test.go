package pinning_test

// focused regression for the reconciler's half of the safety boundary: a
// non-positive resolved reconcile interval is rejected at construction, rather
// than accepted and left to panic Run's time.NewTicker. A zero is still the
// documented default.

import (
	"strings"
	"testing"
	"time"

	"github.com/blobarchive/bloar/pinning"
)

func TestReconcilerNegativeIntervalRejected(t *testing.T) {
	f := newFixture(t)
	_, err := pinning.NewReconciler(pinning.Config{Ledger: f.led, Interval: -time.Second})
	if err == nil {
		t.Fatal("NewReconciler accepted a negative Interval; it must be rejected")
	}
	if !strings.Contains(err.Error(), "Config.Interval is -1s, must be positive") {
		t.Fatalf("error = %q, want it to name the non-positive interval", err)
	}
	// Zero still resolves to the default rather than being rejected.
	if _, err := pinning.NewReconciler(pinning.Config{Ledger: f.led, Interval: 0}); err != nil {
		t.Fatalf("a zero Interval was rejected; it must default: %v", err)
	}
}
