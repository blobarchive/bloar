package p2p

import (
	bsmsg "github.com/ipfs/boxo/bitswap/message"
	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/blobarchive/bloar/metrics"
)

// bitswapMetricsTracer observes Boxo's server-side envelopes. Boxo invokes
// MessageSent immediately before sendBlocks, so these bytes are scheduled /
// attempted payload and are not evidence that the remote peer received them.
type bitswapMetricsTracer struct {
	mx       *metrics.Metrics
	classify func(peer.ID) string
}

func (t *bitswapMetricsTracer) MessageReceived(peer.ID, bsmsg.BitSwapMessage) {}

func (t *bitswapMetricsTracer) MessageSent(id peer.ID, message bsmsg.BitSwapMessage) {
	if t == nil || t.mx == nil || message == nil {
		return
	}
	payloadBytes := 0
	for _, block := range message.Blocks() {
		payloadBytes += len(block.RawData())
	}
	if payloadBytes == 0 {
		return
	}
	class := metrics.BitswapPeerOther
	if t.classify != nil {
		class = t.classify(id)
	}
	t.mx.BitswapScheduled(class, payloadBytes)
}
