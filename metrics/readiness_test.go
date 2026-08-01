package metrics_test

// Acceptance coverage for the safety boundary's daemon-side plumbing: a configured
// followed head is its own readiness gate that holds global readiness red -- and
// names itself in /readyz -- until the follower raises it, and the follow_head_ready
// metric reflects the same per-head state.

import (
	"slices"
	"strings"
	"testing"

	"github.com/blobarchive/bloar/metrics"
)

func TestFollowedHeadGateHoldsReadiness(t *testing.T) {
	gate := metrics.FollowedHeadGate("all")
	if !strings.Contains(gate, "all") {
		t.Fatalf("followed-head gate %q does not name its head, so /readyz cannot say which head is not ready", gate)
	}

	// Every non-followed gate met, but the followed head is still unregistered.
	health := metrics.NewHealth(metrics.GateStore, gate)
	health.Set(metrics.GateStore, true)

	ready, unmet := health.Ready()
	if ready {
		t.Fatal("readiness went green with a followed head still unregistered")
	}
	if !slices.Contains(unmet, gate) {
		t.Fatalf("unmet gates = %v, want the followed-head gate %q among them", unmet, gate)
	}

	// The follower registers the head: readiness clears.
	health.Set(gate, true)
	if ready, unmet := health.Ready(); !ready {
		t.Fatalf("readiness stayed red after every gate was met; unmet = %v", unmet)
	}
}

func TestFollowHeadReadyMetric(t *testing.T) {
	m := metrics.New()
	// Initialised red at startup: 0 rather than absent, so a stuck head is visible.
	m.FollowHeadReady("all", false)
	out := scrape(t, m)
	if !strings.Contains(out, `bloar_follow_head_ready{head="all"} 0`) {
		t.Fatalf("follow_head_ready did not read 0 for an unregistered head:\n%s", out)
	}
	// Raised once the head registers.
	m.FollowHeadReady("all", true)
	out = scrape(t, m)
	if !strings.Contains(out, `bloar_follow_head_ready{head="all"} 1`) {
		t.Fatalf("follow_head_ready did not read 1 for a registered head:\n%s", out)
	}
}
