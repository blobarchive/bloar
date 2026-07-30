package main

import (
	"context"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ipfs/go-cid"

	"github.com/blobarchive/bloar/kubo"
	"github.com/blobarchive/bloar/p2p"
)

// Run explicitly against a disposable stock Kubo 0.42 daemon:
//
//	BLOAR_KUBO_INTEGRATION_URL=http://127.0.0.1:5001 go test ./cmd/bloar-kubo-replica -run TestKubo042RendezvousProvideContract
//
// The ordinary suite remains hermetic. This test exists because embedded DHT
// routing permits arbitrary provider keys while Kubo's provide/once command
// rejects a CID that has not first been stored locally.
func TestKubo042RendezvousProvideContract(t *testing.T) {
	baseURL := os.Getenv("BLOAR_KUBO_INTEGRATION_URL")
	if baseURL == "" {
		t.Skip("set BLOAR_KUBO_INTEGRATION_URL for the stock-Kubo conformance test")
	}
	client, err := kubo.New(kubo.Config{
		BaseURL: baseURL, AllowUnauthenticated: true,
		RequestTimeout: 10 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	info, err := client.CheckReplicaCompatibility(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(strings.TrimPrefix(info.Version, "v"), "0.42.") {
		t.Fatalf("Kubo version = %s, want 0.42.x", info.Version)
	}

	network := "bloar-kubo-contract"
	head := "rendezvous-" + time.Now().UTC().Format("20060102t150405.000000000")
	block, err := p2p.RendezvousBlock(network, head)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_ = client.PinRemove(cleanup, block.Cid(), kubo.PinTypeDirect)
		_ = client.BlockRemove(cleanup, block.Cid())
	})

	missingCtx, missingCancel := context.WithTimeout(ctx, 15*time.Second)
	err = client.ProvideOnce(missingCtx, []cid.Cid{block.Cid()}, kubo.ListLimits{MaxItems: 1, MaxBytes: 4096})
	missingCancel()
	if err == nil {
		t.Fatal("stock Kubo provided a rendezvous CID absent from its blockstore")
	}

	announcer := newAnnouncer(client, "contract-replica", network, time.Hour,
		newReplicaMetrics([]string{head}), slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err := announcer.Initialize(ctx, []string{head}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.BlockGetLocal(ctx, block.Cid()); err != nil {
		t.Fatalf("materialized rendezvous block is not locally readable: %v", err)
	}
	status, err := client.PinStatus(ctx, block.Cid(), kubo.PinTypeDirect)
	if err != nil || !status.CID.Equals(block.Cid()) {
		t.Fatalf("rendezvous direct pin = %+v, %v", status, err)
	}
	provideCtx, provideCancel := context.WithTimeout(ctx, 60*time.Second)
	err = client.ProvideOnce(provideCtx, []cid.Cid{block.Cid()}, kubo.ListLimits{MaxItems: 1, MaxBytes: 4096})
	provideCancel()
	if err != nil {
		t.Fatalf("stock Kubo rejected materialized/direct-pinned rendezvous provide: %v", err)
	}
}
