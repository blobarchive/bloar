package main

// focused regression for the GC readiness gate of the safety boundary. The gate is
// raised at the runner's ENTRY -- a start handshake -- so `gc` being met means the
// scheduler is actually running, not that serve() optimistically set it before the
// goroutine launched. A clean stop on context cancellation leaves
// it met; a terminal scheduler error (which RunEvery does only for a non-positive
// interval, the state the config boundary now rejects before startup) withdraws it.

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/blobarchive/bloar/catalog"
	"github.com/blobarchive/bloar/metrics"
	"github.com/blobarchive/bloar/pinning"
	"github.com/blobarchive/bloar/store"
)

func TestGCSchedulerReadinessLifecycle(t *testing.T) {
	st, err := store.Open(t.TempDir(), store.WithPebbleLogger(quietPebble{}))
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() {
		if err := st.Close(); err != nil {
			t.Errorf("store.Close: %v", err)
		}
	})
	rec, err := pinning.NewReconciler(pinning.Config{Ledger: catalog.NewLedger(st.KV())})
	if err != nil {
		t.Fatalf("NewReconciler: %v", err)
	}
	gc, err := pinning.NewGC(pinning.GCConfig{Blocks: st.Blocks(), Reconciler: rec})
	if err != nil {
		t.Fatalf("NewGC: %v", err)
	}
	log := newLogger()

	t.Run("runner start raises the gate; clean stop leaves it", func(t *testing.T) {
		health := metrics.NewHealth(metrics.GateGC) // starts unmet (red)
		if ready, _ := health.Ready(); ready {
			t.Fatal("GateGC was met before the runner started")
		}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() {
			runGCScheduler(ctx, gc, time.Hour, health, log)
			close(done)
		}()
		// The runner raises the gate as it starts.
		deadline := time.Now().Add(5 * time.Second)
		for {
			if ready, _ := health.Ready(); ready {
				break
			}
			if time.Now().After(deadline) {
				cancel()
				<-done
				t.Fatal("the runner did not raise GateGC at entry")
			}
			time.Sleep(2 * time.Millisecond)
		}
		cancel() // a clean stop: RunEvery returns nil, readiness untouched
		<-done
		if ready, unmet := health.Ready(); !ready {
			t.Fatalf("a clean shutdown withdrew readiness; unmet = %v", unmet)
		}
	})

	t.Run("terminal error withdraws the gate", func(t *testing.T) {
		health := metrics.NewHealth(metrics.GateGC)
		// A non-positive interval is the unreachable state RunEvery guards: it raises
		// the gate at entry, then RunEvery returns an error and it is withdrawn.
		runGCScheduler(context.Background(), gc, -time.Second, health, log)
		ready, unmet := health.Ready()
		if ready {
			t.Fatal("readiness stayed green after the GC scheduler stopped with an error")
		}
		if !slices.Contains(unmet, metrics.GateGC) {
			t.Fatalf("unmet gates = %v, want the gc gate among them", unmet)
		}
	})
}
