package pointerhint_test

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/ipfs/boxo/blockstore"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multihash"

	"github.com/blobarchive/bloar/p2p"
	"github.com/blobarchive/bloar/p2p/pointerhint"
)

type staticProviders struct {
	mu        sync.Mutex
	providers []peer.AddrInfo
	queries   []cid.Cid
}

func (providers *staticProviders) FindProvidersAsync(ctx context.Context, query cid.Cid, count int) <-chan peer.AddrInfo {
	providers.mu.Lock()
	providers.queries = append(providers.queries, query)
	items := append([]peer.AddrInfo(nil), providers.providers...)
	providers.mu.Unlock()
	ch := make(chan peer.AddrInfo)
	go func() {
		defer close(ch)
		for i, provider := range items {
			if i >= count {
				return
			}
			select {
			case <-ctx.Done():
				return
			case ch <- provider:
			}
		}
	}()
	return ch
}

func (providers *staticProviders) querySnapshot() []cid.Cid {
	providers.mu.Lock()
	defer providers.mu.Unlock()
	return append([]cid.Cid(nil), providers.queries...)
}

func integrationHost(t *testing.T) *p2p.Host {
	t.Helper()
	host, err := p2p.NewHost(t.Context(), p2p.HostConfig{
		Listen:          []string{"/ip4/127.0.0.1/tcp/0"},
		IdentityKeyFile: filepath.Join(t.TempDir(), "identity.key"),
	})
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	t.Cleanup(func() { _ = host.Close() })
	return host
}

func integrationBlocks() blockstore.Blockstore {
	return blockstore.NewBlockstore(dssync.MutexWrap(datastore.NewMapDatastore()))
}

func integrationBlock(t *testing.T, value string) blocks.Block {
	t.Helper()
	hash, err := multihash.Sum([]byte(value), multihash.SHA2_256, -1)
	if err != nil {
		t.Fatalf("multihash.Sum: %v", err)
	}
	block, err := blocks.NewBlockWithCid([]byte(value), cid.NewCidV1(cid.Raw, hash))
	if err != nil {
		t.Fatalf("NewBlockWithCid: %v", err)
	}
	return block
}

func integrationExchange(t *testing.T, host *p2p.Host, blocks blockstore.Blockstore) *p2p.Exchange {
	t.Helper()
	exchange, err := p2p.NewExchange(t.Context(), p2p.ExchangeConfig{Host: host, Blocks: blocks})
	if err != nil {
		t.Fatalf("NewExchange: %v", err)
	}
	t.Cleanup(func() { _ = exchange.Close() })
	return exchange
}

func TestStaleProviderRecordIsHarmlessToKnownCIDFetch(t *testing.T) {
	staleHost := integrationHost(t)
	freshHost := integrationHost(t)
	clientHost := integrationHost(t)
	staleBlocks := integrationBlocks()
	freshBlocks := integrationBlocks()
	clientBlocks := integrationBlocks()
	target := integrationBlock(t, "the exact known document bytes")
	if err := freshBlocks.Put(t.Context(), target); err != nil {
		t.Fatalf("fresh Put: %v", err)
	}
	_ = integrationExchange(t, staleHost, staleBlocks)
	_ = integrationExchange(t, freshHost, freshBlocks)
	clientExchange := integrationExchange(t, clientHost, clientBlocks)

	router := &staticProviders{providers: []peer.AddrInfo{
		{ID: staleHost.ID(), Addrs: staleHost.Libp2p().Addrs()},
		{ID: freshHost.ID(), Addrs: freshHost.Libp2p().Addrs()},
	}}
	finder, err := pointerhint.NewFinder(pointerhint.FinderConfig{
		Router:      router,
		Host:        clientHost.Libp2p(),
		FindTimeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewFinder: %v", err)
	}
	result, err := finder.FindAndDial(t.Context(), pointerhint.Pointer{Kind: pointerhint.Document, CID: target.Cid()})
	if err != nil {
		t.Fatalf("FindAndDial: %v", err)
	}
	if result.Connected != 2 {
		t.Fatalf("connected providers = %d, want stale and fresh leads both connected", result.Connected)
	}
	queries := router.querySnapshot()
	if len(queries) != 1 || !queries[0].Equals(target.Cid()) {
		t.Fatalf("provider queries = %v, want exact known document CID %s", queries, target.Cid())
	}

	// The stale record merely connected a peer that does not have target. It
	// did not select content or mutate state: Bitswap still asks for the exact
	// known CID and succeeds from the other connected provider.
	fetching := p2p.FetchingBlockstore(t.Context(), clientBlocks, clientExchange)
	fetchCtx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	got, err := fetching.Get(fetchCtx, target.Cid())
	if err != nil {
		t.Fatalf("known-CID fetch with stale lead present: %v", err)
	}
	if string(got.RawData()) != string(target.RawData()) {
		t.Fatalf("fetched bytes = %q, want %q", got.RawData(), target.RawData())
	}
}
