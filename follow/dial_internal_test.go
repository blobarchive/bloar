package follow

import (
	"context"
	"crypto/rand"
	"io"
	"log/slog"
	"testing"
	"time"

	libp2pcrypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
)

func TestPublicationDialSharesOneTotalTimeout(t *testing.T) {
	addresses := make([]string, 0, 2)
	for range 2 {
		privateKey, _, err := libp2pcrypto.GenerateEd25519Key(rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		id, err := peer.IDFromPrivateKey(privateKey)
		if err != nil {
			t.Fatal(err)
		}
		addresses = append(addresses, "/ip4/127.0.0.1/tcp/1/p2p/"+id.String())
	}

	const total = 100 * time.Millisecond
	deadlines := make([]time.Time, 0, len(addresses))
	follower := &Follower{
		cfg: Config{
			FetchTimeout: total,
			DialPeer: func(ctx context.Context, _ peer.AddrInfo) error {
				deadline, ok := ctx.Deadline()
				if !ok {
					t.Fatal("dial attempt has no deadline")
				}
				deadlines = append(deadlines, deadline)
				<-ctx.Done()
				return ctx.Err()
			},
		},
		log: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	started := time.Now()
	follower.dial(t.Context(), addresses)
	if len(deadlines) != len(addresses) {
		t.Fatalf("dial attempts = %d, want %d", len(deadlines), len(addresses))
	}
	// Each attempt may receive a different fair slice, but neither deadline may
	// extend beyond the one publication-hint budget established at entry.
	outerLimit := started.Add(total + 20*time.Millisecond)
	for i, deadline := range deadlines {
		if deadline.After(outerLimit) {
			t.Fatalf("dial %d deadline %s exceeds total budget ending near %s", i, deadline, started.Add(total))
		}
	}
}
