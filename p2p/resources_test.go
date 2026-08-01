package p2p_test

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/blobarchive/bloar/metrics"
	"github.com/blobarchive/bloar/p2p"
)

// TestHostConnectionManagerKeepsConfiguredLow proves that watermarks are a
// post-connection pruning policy, not an admission cap. Four connections are
// admitted at high=4. BasicConnMgr excludes the protected static peer from
// pruning and retains the configured low=2 prunable peers alongside it.
func TestHostConnectionManagerKeepsConfiguredLow(t *testing.T) {
	static := newTestHost(t)
	others := []*p2p.Host{newTestHost(t), newTestHost(t), newTestHost(t)}

	h := newTestHost(t, func(c *p2p.HostConfig) {
		c.Peers = []string{static.AnnounceAddrs()[0]}
		c.ConnectionManager = p2p.ConnectionManagerConfig{
			LowWatermark:  2,
			HighWatermark: 4,
			GracePeriod:   time.Nanosecond,
		}
	})
	waitFor(t, "the protected static peer connection", func() bool {
		return h.Libp2p().Network().Connectedness(static.ID()) == network.Connected
	})
	for _, target := range others {
		connect(t, h, target)
	}
	if got := len(h.Libp2p().Network().Conns()); got != 4 {
		t.Fatalf("connections before trim = %d, want the burst above high watermark to reach 4", got)
	}
	if !h.Libp2p().ConnManager().IsProtected(static.ID(), "") {
		t.Fatal("configured static peer is not protected from pruning")
	}

	// Make every non-static connection older than the deliberately tiny grace.
	time.Sleep(time.Millisecond)
	h.Libp2p().ConnManager().TrimOpenConns(t.Context())
	waitFor(t, "connection-manager trim to configured low plus the protected peer", func() bool {
		return len(h.Libp2p().Network().Conns()) == 3
	})
	if h.Libp2p().Network().Connectedness(static.ID()) != network.Connected {
		t.Fatal("the configured static peer was pruned")
	}
}

// TestHostResourceManagerHardRefusal distinguishes the resource manager from
// connmgr pruning. The first allocation succeeds; the second is synchronously
// refused at the hard system connection limit without waiting for a trim.
func TestHostResourceManagerHardRefusal(t *testing.T) {
	h := newTestHost(t, func(c *p2p.HostConfig) {
		c.ConnectionManager = p2p.ConnectionManagerConfig{
			LowWatermark:  1,
			HighWatermark: 1,
		}
		c.ResourceManager = oneConnectionResourcePolicy()
	})
	endpoint := ma.StringCast("/ip4/127.0.0.1/tcp/1")
	first, err := h.Libp2p().Network().ResourceManager().OpenConnection(network.DirInbound, false, endpoint)
	if err != nil {
		t.Fatalf("opening first connection allocation: %v", err)
	}
	defer first.Done()

	second, err := h.Libp2p().Network().ResourceManager().OpenConnection(network.DirInbound, false, endpoint)
	if err == nil {
		second.Done()
		t.Fatal("second connection allocation exceeded the hard limit but was accepted")
	}
}

func TestHostResourceControlValidation(t *testing.T) {
	tests := []struct {
		name      string
		configure func(*p2p.HostConfig)
		want      string
	}{
		{
			name: "low above high",
			configure: func(c *p2p.HostConfig) {
				c.ConnectionManager = p2p.ConnectionManagerConfig{LowWatermark: 3, HighWatermark: 2}
			},
			want: "exceeds high_watermark",
		},
		{
			name: "negative grace",
			configure: func(c *p2p.HostConfig) {
				c.ConnectionManager.GracePeriod = -time.Second
			},
			want: "grace_period must be positive",
		},
		{
			name: "negative resource",
			configure: func(c *p2p.HostConfig) {
				c.ResourceManager.Streams = -1
			},
			want: "resource_manager.streams must be positive",
		},
		{
			name: "direction above total",
			configure: func(c *p2p.HostConfig) {
				c.ResourceManager.Streams = 4
				c.ResourceManager.InboundStreams = 5
				c.ResourceManager.OutboundStreams = 4
				c.ResourceManager.PeerStreams = 1
				c.ResourceManager.PeerInboundStreams = 1
				c.ResourceManager.PeerOutboundStreams = 1
			},
			want: "streams (4) must be at least",
		},
		{
			name: "per peer above system",
			configure: func(c *p2p.HostConfig) {
				c.ResourceManager.Streams = 4
				c.ResourceManager.InboundStreams = 4
				c.ResourceManager.OutboundStreams = 4
				c.ResourceManager.PeerStreams = 5
				c.ResourceManager.PeerInboundStreams = 4
				c.ResourceManager.PeerOutboundStreams = 4
			},
			want: "per-peer stream limits",
		},
		{
			name: "per peer memory above system",
			configure: func(c *p2p.HostConfig) {
				c.ResourceManager.MemoryBytes = 64 << 20
				c.ResourceManager.PeerMemoryBytes = 65 << 20
			},
			want: "peer_memory_bytes",
		},
		{
			name: "per peer file descriptors above system",
			configure: func(c *p2p.HostConfig) {
				c.ResourceManager.FileDescriptors = 256
				c.ResourceManager.PeerFileDescriptors = 257
			},
			want: "peer_file_descriptors",
		},
		{
			name: "fd below connections",
			configure: func(c *p2p.HostConfig) {
				c.ResourceManager.FileDescriptors = 7
				c.ResourceManager.Connections = 8
				c.ResourceManager.InboundConnections = 8
				c.ResourceManager.OutboundConnections = 8
				c.ResourceManager.PeerConnections = 1
				c.ResourceManager.PeerInboundConnections = 1
				c.ResourceManager.PeerOutboundConnections = 1
				c.ResourceManager.PeerFileDescriptors = 1
				c.ConnectionManager = p2p.ConnectionManagerConfig{LowWatermark: 4, HighWatermark: 8}
			},
			want: "file_descriptors (7) is below connections (8)",
		},
		{
			name: "pruning high above hard cap",
			configure: func(c *p2p.HostConfig) {
				c.ResourceManager = oneConnectionResourcePolicy()
				c.ConnectionManager = p2p.ConnectionManagerConfig{LowWatermark: 1, HighWatermark: 2}
			},
			want: "high_watermark (2) exceeds",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := p2p.HostConfig{
				Listen:          []string{"/ip4/127.0.0.1/tcp/0"},
				IdentityKeyFile: filepath.Join(t.TempDir(), "p2p.key"),
			}
			tt.configure(&cfg)
			h, err := p2p.NewHost(t.Context(), cfg)
			if h != nil {
				_ = h.Close()
			}
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error = %v, want substring %q", err, tt.want)
			}
		})
	}
}

func TestHostRejectsStaticPeerBudgetWithoutHardHeadroom(t *testing.T) {
	static := newTestHost(t)
	_, err := p2p.NewHost(t.Context(), p2p.HostConfig{
		Listen:          []string{"/ip4/127.0.0.1/tcp/0"},
		Peers:           []string{static.AnnounceAddrs()[0]},
		IdentityKeyFile: filepath.Join(t.TempDir(), "p2p.key"),
		ConnectionManager: p2p.ConnectionManagerConfig{
			LowWatermark:  1,
			HighWatermark: 3,
		},
		ResourceManager: p2p.ResourceManagerConfig{
			Connections:         p2p.DefaultResourceStaticPeerConnectionHeadroom,
			InboundConnections:  p2p.DefaultResourceStaticPeerConnectionHeadroom,
			OutboundConnections: p2p.DefaultResourceStaticPeerConnectionHeadroom,
		},
	})
	if err == nil || !strings.Contains(err.Error(), "control-plane headroom") {
		t.Fatalf("error = %v, want hard static-peer headroom rejection", err)
	}
}

func TestHostLibp2pMetricsUseConfiguredRegistererWithBoundedGlobalException(t *testing.T) {
	defaultRegisterer := prometheus.DefaultRegisterer
	defaultGatherer := prometheus.DefaultGatherer
	globalBefore := gatheredMetricNames(t, prometheus.DefaultGatherer)

	mx := metrics.New()
	first := newTestHost(t, func(c *p2p.HostConfig) { c.Metrics = mx })
	private := gatheredMetricNames(t, mx.Registry())
	if !containsMetricPrefix(private, "libp2p_rcmgr_") {
		t.Fatalf("private metric names have no libp2p resource-manager metrics: %v", private)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("closing first metrics-enabled host: %v", err)
	}
	// A restart reusing the daemon registry must not duplicate-panic either.
	newTestHost(t, func(c *p2p.HostConfig) { c.Metrics = mx })

	// Registrations also land in a second daemon's selected registry. This is
	// registration isolation only: libp2p uses package-level collectors, so the
	// values exposed through the two registries remain process-global.
	second := metrics.New()
	newTestHost(t, func(c *p2p.HostConfig) { c.Metrics = second })
	if !containsMetricPrefix(gatheredMetricNames(t, second.Registry()), "libp2p_rcmgr_") {
		t.Fatal("second daemon's private registry has no libp2p resource-manager metrics")
	}

	// A metrics-disabled daemon explicitly disables libp2p/rcmgr metrics. Bloar
	// never replaces the process defaults as a construction workaround.
	newTestHost(t)
	if prometheus.DefaultRegisterer != defaultRegisterer || prometheus.DefaultGatherer != defaultGatherer {
		t.Fatal("host construction replaced Prometheus process defaults")
	}
	globalAfter := gatheredMetricNames(t, prometheus.DefaultGatherer)
	if !containsMetricPrefix(globalAfter, "libp2p_eventbus_") {
		t.Fatal("AutoNATv2 auxiliary eventbus metrics are absent from the process registry")
	}
	// Libp2p v0.48 does not forward the parent registerer to AutoNATv2's
	// auxiliary event bus. Accept precisely its fixed collector set; every
	// supported collector remains private (or disabled) and no process-global
	// registerer substitution is used to hide this upstream limitation.
	allowedAutoNATv2Globals := map[string]struct{}{
		"libp2p_eventbus_events_emitted_total":    {},
		"libp2p_eventbus_subscriber_event_queued": {},
		"libp2p_eventbus_subscriber_queue_full":   {},
		"libp2p_eventbus_subscriber_queue_length": {},
		"libp2p_eventbus_subscribers_total":       {},
	}
	for name := range globalAfter {
		if !strings.HasPrefix(name, "libp2p_") {
			continue
		}
		if _, existed := globalBefore[name]; existed {
			continue
		}
		if _, allowed := allowedAutoNATv2Globals[name]; !allowed {
			t.Fatalf("unexpected libp2p metric %q registered process-globally", name)
		}
	}
}

func gatheredMetricNames(t *testing.T, gatherer prometheus.Gatherer) map[string]struct{} {
	t.Helper()
	families, err := gatherer.Gather()
	if err != nil {
		t.Fatalf("gathering metrics: %v", err)
	}
	out := make(map[string]struct{}, len(families))
	for _, family := range families {
		out[family.GetName()] = struct{}{}
	}
	return out
}

func containsMetricPrefix(names map[string]struct{}, prefix string) bool {
	for name := range names {
		if strings.HasPrefix(name, prefix) {
			return true
		}
	}
	return false
}

func oneConnectionResourcePolicy() p2p.ResourceManagerConfig {
	return p2p.ResourceManagerConfig{
		Connections:             1,
		InboundConnections:      1,
		OutboundConnections:     1,
		PeerConnections:         1,
		PeerInboundConnections:  1,
		PeerOutboundConnections: 1,
	}
}
