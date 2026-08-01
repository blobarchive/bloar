package p2p_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/ipfs/boxo/blockstore"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/blobarchive/bloar/p2p"
)

func TestProbePeerIsolatesTargetAndClassifiesPartialArchive(t *testing.T) {
	current := blocks.NewBlock([]byte("current authenticated root"))
	historical := blocks.NewBlock([]byte("historical archive challenge"))

	fullHost, fullExchange, fullStore := newProbeServer(t)
	if err := fullStore.PutMany(t.Context(), []blocks.Block{current, historical}); err != nil {
		t.Fatal(err)
	}
	if err := fullExchange.NotifyNewBlocks(t.Context(), current, historical); err != nil {
		t.Fatal(err)
	}
	full := probe(t, peer.AddrInfo{ID: fullHost.ID(), Addrs: fullHost.Libp2p().Addrs()}, []cid.Cid{current.Cid(), historical.Cid()})
	if !full.Reachable || full.Path != p2p.ProbePathDirect || len(full.Blocks) != 2 || !full.Blocks[0].Success || !full.Blocks[1].Success {
		t.Fatalf("full probe = %+v", full)
	}

	partialHost, partialExchange, partialStore := newProbeServer(t)
	if err := partialStore.Put(t.Context(), current); err != nil {
		t.Fatal(err)
	}
	if err := partialExchange.NotifyNewBlocks(t.Context(), current); err != nil {
		t.Fatal(err)
	}
	partial := probe(t, peer.AddrInfo{ID: partialHost.ID(), Addrs: partialHost.Libp2p().Addrs()}, []cid.Cid{current.Cid(), historical.Cid()})
	if !partial.Reachable || len(partial.Blocks) == 0 || !partial.Blocks[0].Success {
		t.Fatalf("partial current proof = %+v", partial)
	}
	if len(partial.Blocks) > 1 && partial.Blocks[1].Success {
		t.Fatalf("partial peer falsely proved historical block: %+v", partial)
	}
}

func TestProbePeerRequiresDeadlineAndBoundsInput(t *testing.T) {
	_, err := p2p.ProbePeer(context.Background(), peer.AddrInfo{}, []cid.Cid{blocks.NewBlock(nil).Cid()}, p2p.ProbeLimits{})
	if err == nil {
		t.Fatal("deadline-free probe accepted")
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	_, err = p2p.ProbePeer(ctx, peer.AddrInfo{ID: peer.ID("bad")}, make([]cid.Cid, p2p.MaximumProbeCIDs+1), p2p.ProbeLimits{MaxCIDs: p2p.MaximumProbeCIDs})
	if err == nil {
		t.Fatal("oversized CID set accepted")
	}
}

func probe(t *testing.T, provider peer.AddrInfo, targets []cid.Cid) p2p.PeerProbe {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	result, err := p2p.ProbePeer(ctx, provider, targets, p2p.ProbeLimits{MaxCIDs: len(targets), MaxBytes: 4 << 20})
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func newProbeServer(t *testing.T) (*p2p.Host, *p2p.Exchange, blockstore.Blockstore) {
	t.Helper()
	host, err := p2p.NewHost(t.Context(), p2p.HostConfig{
		Listen:          []string{"/ip4/127.0.0.1/tcp/0"},
		IdentityKeyFile: filepath.Join(t.TempDir(), "identity.key"),
	})
	if err != nil {
		t.Fatal(err)
	}
	store := blockstore.NewBlockstore(dssync.MutexWrap(datastore.NewMapDatastore()))
	exchange, err := p2p.NewExchange(t.Context(), p2p.ExchangeConfig{Host: host, Blocks: store})
	if err != nil {
		_ = host.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := exchange.Close(); err != nil {
			t.Error(err)
		}
		if err := host.Close(); err != nil {
			t.Error(err)
		}
	})
	return host, exchange, store
}
