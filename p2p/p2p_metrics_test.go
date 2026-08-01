package p2p_test

import (
	"context"
	"testing"
	"time"

	"github.com/blobarchive/bloar/metrics"
	"github.com/blobarchive/bloar/p2p"
)

func TestLivePeerMetricsReconcileConnectAndDisconnect(t *testing.T) {
	mx := metrics.New()
	observed := newTestHost(t, func(cfg *p2p.HostConfig) { cfg.Metrics = mx })
	remote := newTestHost(t)

	connect(t, observed, remote)
	waitFor(t, "outbound TCP peer gauge to become live", func() bool {
		return gatheredMetricValue(t, mx, "bloar_p2p_live_peers", map[string]string{
			"direction": metrics.P2PDirectionOutbound,
			"transport": metrics.P2PTransportTCP,
		}) == 1
	})

	if err := observed.Libp2p().Network().ClosePeer(remote.ID()); err != nil {
		t.Fatalf("closing observed peer: %v", err)
	}
	waitFor(t, "outbound TCP peer gauge to reconcile after disconnect", func() bool {
		return gatheredMetricValue(t, mx, "bloar_p2p_live_peers", map[string]string{
			"direction": metrics.P2PDirectionOutbound,
			"transport": metrics.P2PTransportTCP,
		}) == 0
	})
}

func TestBitswapScheduledBytesCountsRealPayload(t *testing.T) {
	mx := metrics.New()
	server := newTestHost(t, func(cfg *p2p.HostConfig) { cfg.Metrics = mx })
	client := newTestHost(t)
	connect(t, client, server)

	serverBlocks, clientBlocks := memBlocks(), memBlocks()
	want := rawBlock(t, []byte("a real block whose raw bytes cross Bitswap"))
	putBlock(t, serverBlocks, want)
	serverExchange, err := p2p.NewExchange(t.Context(), p2p.ExchangeConfig{
		Host:    server,
		Blocks:  serverBlocks,
		Metrics: mx,
	})
	if err != nil {
		t.Fatalf("building metrics-enabled serving exchange: %v", err)
	}
	t.Cleanup(func() {
		if err := serverExchange.Close(); err != nil {
			t.Errorf("closing metrics-enabled serving exchange: %v", err)
		}
	})
	clientExchange := newTestExchange(t, client, clientBlocks)

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	got, err := clientExchange.NewSession(ctx).GetBlock(ctx, want.Cid())
	if err != nil {
		t.Fatalf("fetching real block payload: %v", err)
	}
	if got.Cid() != want.Cid() {
		t.Fatalf("fetched block = %s, want %s", got.Cid(), want.Cid())
	}

	if scheduled := gatheredMetricValue(t, mx, "bloar_bitswap_scheduled_bytes_total", map[string]string{
		"peer_class": metrics.BitswapPeerOther,
	}); scheduled != float64(len(want.RawData())) {
		t.Fatalf("scheduled payload bytes = %g, want exact RawData length %d", scheduled, len(want.RawData()))
	}
}

func gatheredMetricValue(t *testing.T, mx *metrics.Metrics, familyName string, labels map[string]string) float64 {
	t.Helper()
	families, err := mx.Registry().Gather()
	if err != nil {
		t.Fatalf("gathering metrics: %v", err)
	}
	for _, family := range families {
		if family.GetName() != familyName {
			continue
		}
		for _, sample := range family.Metric {
			matched := len(sample.Label) == len(labels)
			for _, label := range sample.Label {
				if labels[label.GetName()] != label.GetValue() {
					matched = false
					break
				}
			}
			if !matched {
				continue
			}
			switch family.GetType().String() {
			case "GAUGE":
				return sample.GetGauge().GetValue()
			case "COUNTER":
				return sample.GetCounter().GetValue()
			default:
				t.Fatalf("metric %q has unsupported type %s", familyName, family.GetType())
			}
		}
	}
	t.Fatalf("metric %q with labels %v is absent", familyName, labels)
	return 0
}
