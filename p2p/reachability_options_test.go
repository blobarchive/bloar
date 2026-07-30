package p2p

import (
	"bytes"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/event"
	"github.com/libp2p/go-libp2p/core/network"

	bmetrics "github.com/blobarchive/bloar/metrics"
)

// TestReachabilityOptions checks the option plan directly: it proves AutoNAT
// v2 is enabled in both modes, the port mapper is a genuine opt-out, and Bloar
// never turns a reachability guess into ForceReachabilityPublic.
func TestReachabilityOptions(t *testing.T) {
	for _, tc := range []struct {
		name       string
		portMap    bool
		wantMapper bool
	}{
		{name: "mapping enabled", portMap: true, wantMapper: true},
		{name: "mapping disabled", portMap: false, wantMapper: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var cfg libp2p.Config
			if err := cfg.Apply(reachabilityOptions(tc.portMap)...); err != nil {
				t.Fatalf("applying reachability options: %v", err)
			}
			if !cfg.EnableAutoNATv2 {
				t.Error("AutoNAT v2 is disabled")
			}
			if got := cfg.NATManager != nil; got != tc.wantMapper {
				t.Errorf("NAT manager configured = %v, want %v", got, tc.wantMapper)
			}
			if cfg.AutoNATConfig.ForceReachability != nil {
				t.Errorf("reachability was forced to %s; want AutoNAT observation", *cfg.AutoNATConfig.ForceReachability)
			}
		})
	}
}

func TestReachabilityObserverIsBoundedAndLogsTransitions(t *testing.T) {
	var output lockedBuffer
	mx := bmetrics.New()
	h, err := NewHost(t.Context(), HostConfig{
		IdentityKeyFile: filepath.Join(t.TempDir(), "p2p.key"),
		Metrics:         mx,
		Logger:          slog.New(slog.NewTextHandler(&output, nil)),
	})
	if err != nil {
		t.Fatalf("building host: %v", err)
	}
	t.Cleanup(func() {
		if err := h.Close(); err != nil {
			t.Errorf("closing host: %v", err)
		}
	})
	if got := cap(h.reachabilitySub.Out()); got != 4 {
		t.Fatalf("reachability event buffer = %d, want bounded capacity 4", got)
	}

	emitter, err := h.h.EventBus().Emitter(new(event.EvtLocalReachabilityChanged))
	if err != nil {
		t.Fatalf("creating reachability test emitter: %v", err)
	}
	t.Cleanup(func() { _ = emitter.Close() })
	for _, state := range []network.Reachability{network.ReachabilityPrivate, network.ReachabilityPrivate, network.ReachabilityPublic} {
		if err := emitter.Emit(event.EvtLocalReachabilityChanged{Reachability: state}); err != nil {
			t.Fatalf("emitting %s reachability: %v", state, err)
		}
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		logs := output.String()
		if strings.Contains(logs, "to=Public") && strings.Contains(logs, "Private") {
			if got := strings.Count(logs, "to=Private"); got > 1 {
				t.Errorf("duplicate Private observation logged as %d transitions:\n%s", got, logs)
			}
			if got := reachabilityGauge(t, mx, "public"); got != 1 {
				t.Errorf("public reachability gauge = %g, want 1", got)
			}
			if got := reachabilityGauge(t, mx, "private"); got != 0 {
				t.Errorf("private reachability gauge = %g, want 0 after Public", got)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("reachability transitions were not logged:\n%s", output.String())
}

func reachabilityGauge(t *testing.T, mx *bmetrics.Metrics, state string) float64 {
	t.Helper()
	families, err := mx.Registry().Gather()
	if err != nil {
		t.Fatalf("gathering metrics: %v", err)
	}
	for _, family := range families {
		if family.GetName() != "bloar_p2p_reachability" {
			continue
		}
		for _, metric := range family.Metric {
			for _, label := range metric.Label {
				if label.GetName() == "state" && label.GetValue() == state {
					return metric.GetGauge().GetValue()
				}
			}
		}
	}
	t.Fatalf("reachability metric for state %q is absent", state)
	return 0
}

type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.String()
}
