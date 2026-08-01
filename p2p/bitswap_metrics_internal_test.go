package p2p

import (
	"testing"

	bsmsg "github.com/ipfs/boxo/bitswap/message"
	pb "github.com/ipfs/boxo/bitswap/message/pb"
	blocks "github.com/ipfs/go-block-format"
	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/blobarchive/bloar/metrics"
)

func TestBitswapMetricsTracerCountsOnlyRawBlockPayload(t *testing.T) {
	mx := metrics.New()
	tracer := &bitswapMetricsTracer{
		mx: mx,
		classify: func(peer.ID) string {
			return metrics.BitswapPeerStatic
		},
	}
	wantBlock := blocks.NewBlock([]byte("wanted only as metadata"))
	haveBlock := blocks.NewBlock([]byte("announced as HAVE only"))
	dontHaveBlock := blocks.NewBlock([]byte("announced as DONT_HAVE only"))

	metadata := bsmsg.New(false)
	metadata.AddEntry(wantBlock.Cid(), 10, pb.Message_Wantlist_Block, true)
	metadata.AddBlockPresence(haveBlock.Cid(), pb.Message_Have)
	metadata.AddBlockPresence(dontHaveBlock.Cid(), pb.Message_DontHave)
	metadata.SetPendingBytes(1 << 20)
	tracer.MessageSent(peer.ID("remote"), metadata)
	if got := p2pMetricCounter(t, mx, "bloar_bitswap_scheduled_bytes_total", "peer_class", metrics.BitswapPeerStatic); got != 0 {
		t.Fatalf("metadata-only envelope counted %g scheduled bytes, want 0", got)
	}

	payloadA := blocks.NewBlock([]byte("first real raw block payload"))
	payloadB := blocks.NewBlock([]byte("second real raw payload"))
	envelope := bsmsg.New(false)
	// Metadata in the same envelope must not inflate the raw payload sum.
	envelope.AddEntry(wantBlock.Cid(), 10, pb.Message_Wantlist_Have, true)
	envelope.AddBlockPresence(haveBlock.Cid(), pb.Message_Have)
	envelope.SetPendingBytes(1 << 20)
	envelope.AddBlock(payloadA)
	envelope.AddBlock(payloadB)
	tracer.MessageSent(peer.ID("remote"), envelope)

	want := float64(len(payloadA.RawData()) + len(payloadB.RawData()))
	if got := p2pMetricCounter(t, mx, "bloar_bitswap_scheduled_bytes_total", "peer_class", metrics.BitswapPeerStatic); got != want {
		t.Fatalf("scheduled bytes = %g, want raw block payload sum %g", got, want)
	}
}

func TestBitswapPeerClassifierWithoutPeerStateFallsBackToOther(t *testing.T) {
	for _, h := range []*Host{nil, &Host{}} {
		if got := bitswapPeerClassifier(h)(peer.ID("must-not-become-a-label")); got != metrics.BitswapPeerOther {
			t.Fatalf("classifier without peer state = %q, want other", got)
		}
	}
}

func p2pMetricCounter(t *testing.T, mx *metrics.Metrics, familyName, labelName, labelValue string) float64 {
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
			for _, label := range sample.Label {
				if label.GetName() == labelName && label.GetValue() == labelValue {
					return sample.GetCounter().GetValue()
				}
			}
		}
	}
	t.Fatalf("metric %s{%s=%q} is absent", familyName, labelName, labelValue)
	return 0
}
