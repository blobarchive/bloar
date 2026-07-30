package follow_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/ipfs/boxo/exchange"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/blobarchive/bloar/follow"
	"github.com/blobarchive/bloar/p2p"
	"github.com/blobarchive/bloar/p2p/pointerhint"
)

// exactPointerProviders is the smallest deterministic content-router seam for
// a real Finder. The follower and writer still use real loopback libp2p hosts
// and a real Bitswap exchange; only the DHT's provider-record lookup is made
// hermetic.
type exactPointerProviders struct {
	mu       sync.Mutex
	provider peer.AddrInfo
	queries  []cid.Cid
}

// exactPointerGateSessions makes the first read of one known root fail even if
// background DHT maintenance happens to reconnect the peers first. Real
// Kademlia nodes are intentionally nondeterministic about those maintenance
// connections; gating the data-plane read keeps the exact-provider retry
// load-bearing without weakening any of the real DHT, Finder, dial, or Bitswap
// work exercised after the miss.
type exactPointerGateSessions struct {
	inner  p2p.SessionSource
	target cid.Cid

	mu     sync.Mutex
	open   bool
	misses int
}

func (s *exactPointerGateSessions) NewSession(ctx context.Context) exchange.Fetcher {
	return &exactPointerGateFetcher{inner: s.inner.NewSession(ctx), gate: s}
}

func (s *exactPointerGateSessions) allowTarget() {
	s.mu.Lock()
	s.open = true
	s.mu.Unlock()
}

func (s *exactPointerGateSessions) missCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.misses
}

type exactPointerGateFetcher struct {
	inner exchange.Fetcher
	gate  *exactPointerGateSessions
}

func (f *exactPointerGateFetcher) GetBlock(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	f.gate.mu.Lock()
	if c.Equals(f.gate.target) && !f.gate.open {
		f.gate.misses++
		f.gate.mu.Unlock()
		return nil, errors.New("injected cold-root miss before exact pointer discovery")
	}
	f.gate.mu.Unlock()
	return f.inner.GetBlock(ctx, c)
}

func (f *exactPointerGateFetcher) GetBlocks(ctx context.Context, cids []cid.Cid) (<-chan blocks.Block, error) {
	return f.inner.GetBlocks(ctx, cids)
}

func (r *exactPointerProviders) FindProvidersAsync(ctx context.Context, query cid.Cid, count int) <-chan peer.AddrInfo {
	r.mu.Lock()
	r.queries = append(r.queries, query)
	r.mu.Unlock()

	results := make(chan peer.AddrInfo, 1)
	if count > 0 && ctx.Err() == nil {
		results <- r.provider
	}
	close(results)
	return results
}

func (r *exactPointerProviders) snapshot() []cid.Cid {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]cid.Cid(nil), r.queries...)
}

func TestExactPointerFinderConnectsColdFollowerAndEnablesRootRetry(t *testing.T) {
	w := newWriter(t)
	w.ingestSlot(testOrigin, 1)
	router := &exactPointerProviders{provider: peerInfo(w)}

	f := newLoneFollower(t, w, func(cfg *follow.Config) {
		// Save the real host for Finder, then disable the publication-document
		// multiaddr convenience path. This leaves the follower genuinely cold
		// until the first known-root fetch misses and invokes exact discovery.
		coldHost := cfg.Host
		cfg.Host = nil
		cfg.FetchTimeout = 50 * time.Millisecond
		finder, err := pointerhint.NewFinder(pointerhint.FinderConfig{
			Router:      router,
			Host:        coldHost.Libp2p(),
			FindTimeout: 2 * time.Second,
		})
		if err != nil {
			t.Fatalf("NewFinder: %v", err)
		}
		cfg.FindPointer = func(ctx context.Context, pointer pointerhint.Pointer) error {
			_, err := finder.FindAndDial(ctx, pointer)
			return err
		}
	})

	if got := f.host.Libp2p().Network().Connectedness(w.host.ID()); got == network.Connected {
		t.Fatal("test follower was connected before exact-pointer discovery")
	}
	if err := f.pollErr(); err != nil {
		t.Fatalf("Poll after exact root discovery: %v", err)
	}
	if got := f.host.Libp2p().Network().Connectedness(w.host.ID()); got != network.Connected {
		t.Fatalf("writer connectedness after Finder = %s, want connected", got)
	}
	queries := router.snapshot()
	if len(queries) != 1 || !queries[0].Equals(w.head.Root()) {
		t.Fatalf("provider queries = %v, want exactly current root %s", queries, w.head.Root())
	}
	if got, ok := f.heads.Get(testHead); !ok || !got.Root().Equals(w.head.Root()) {
		t.Fatalf("adopted root after exact discovery = %v, present=%t; want %s", got, ok, w.head.Root())
	}
}

// TestPrivateDHTPointerProviderEnablesRetryAfterColdRootMiss is the private
// swarm acceptance path end to end. Unlike the router-seam test above, the
// provider record is written and found through three real loopback Kademlia
// nodes: a bootstrap node, the writer, and a follower which knows only the
// bootstrap node. The DHT is still never installed as generic Bitswap routing.
func TestPrivateDHTPointerProviderEnablesRetryAfterColdRootMiss(t *testing.T) {
	w := newWriter(t)
	w.ingestSlot(testOrigin, 1)

	var finder *pointerhint.Finder
	var gate *exactPointerGateSessions
	findCalls := 0
	f := newLoneFollower(t, w, func(cfg *follow.Config) {
		// Suppress the authenticated document-multiaddr shortcut. The only path
		// from this cold follower to the writer is the exact root provider record.
		cfg.Host = nil
		cfg.FetchTimeout = 75 * time.Millisecond
		gate = &exactPointerGateSessions{inner: cfg.Sessions, target: w.head.Root()}
		cfg.Sessions = gate
		cfg.FindPointer = func(ctx context.Context, pointer pointerhint.Pointer) error {
			findCalls++
			if pointer.Kind != pointerhint.Root || !pointer.CID.Equals(w.head.Root()) {
				return &unexpectedPointerError{got: pointer, want: w.head.Root()}
			}
			result, err := finder.FindAndDial(ctx, pointer)
			if err != nil {
				return err
			}
			if result.Results == 0 || result.Connected+result.AlreadyConnected == 0 {
				return errors.New("private DHT returned no usable exact-root provider")
			}
			gate.allowTarget()
			return nil
		}
	})
	bootstrap := newNode(t)

	bootstrapDHT := newPrivatePointerDHT(t, bootstrap.host)
	writerDHT := newPrivatePointerDHT(t, w.host)
	followerDHT := newPrivatePointerDHT(t, f.host)
	if err := w.host.Libp2p().Connect(t.Context(), peer.AddrInfo{
		ID: bootstrap.host.ID(), Addrs: bootstrap.host.Libp2p().Addrs(),
	}); err != nil {
		t.Fatalf("connect writer to private bootstrap: %v", err)
	}
	if err := f.host.Libp2p().Connect(t.Context(), peer.AddrInfo{
		ID: bootstrap.host.ID(), Addrs: bootstrap.host.Libp2p().Addrs(),
	}); err != nil {
		t.Fatalf("connect follower to private bootstrap: %v", err)
	}
	waitPointerCondition(t, "private DHT routing tables to populate", func() bool {
		return bootstrapDHT.RoutingTable().Size() >= 2 &&
			writerDHT.RoutingTable().Size() >= 1 && followerDHT.RoutingTable().Size() >= 1
	})
	for _, routing := range []*dht.IpfsDHT{bootstrapDHT, writerDHT, followerDHT} {
		if err := routing.Bootstrap(t.Context()); err != nil {
			t.Fatalf("bootstrap private DHT: %v", err)
		}
	}

	provider, err := pointerhint.NewProvider(t.Context(), pointerhint.ProviderConfig{
		Router:            writerDHT,
		Serving:           w.docs,
		ReprovideInterval: time.Hour,
		ReprovideJitter:   time.Minute,
		MinWriteInterval:  time.Millisecond,
		RetryMin:          25 * time.Millisecond,
		RetryMax:          100 * time.Millisecond,
		AttemptTimeout:    5 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewProvider: %v", err)
	}
	t.Cleanup(func() {
		if err := provider.Close(); err != nil {
			t.Errorf("close pointer provider: %v", err)
		}
	})
	if err := provider.Update(pointerhint.Set{Root: w.head.Root()}); err != nil {
		t.Fatalf("schedule current root pointer: %v", err)
	}
	waitPointerCondition(t, "root provider record to reach private bootstrap", func() bool {
		providers, err := bootstrapDHT.ProviderStore().GetProviders(t.Context(), w.head.Root().Hash())
		if err != nil {
			return false
		}
		for _, provider := range providers {
			if provider.ID == w.host.ID() {
				return true
			}
		}
		return false
	})

	// Provider writes and DHT maintenance are allowed to create transient
	// control-plane connections. Remove one if present so the data-plane proof
	// starts cold and the Finder's dial is load-bearing.
	_ = f.host.Libp2p().Network().ClosePeer(w.host.ID())
	_ = w.host.Libp2p().Network().ClosePeer(f.host.ID())
	waitPointerCondition(t, "writer and follower to be disconnected before Poll", func() bool {
		return f.host.Libp2p().Network().Connectedness(w.host.ID()) != network.Connected
	})
	finder, err = pointerhint.NewFinder(pointerhint.FinderConfig{
		Router:          followerDHT,
		Host:            f.host.Libp2p(),
		MaxResults:      8,
		DialConcurrency: 2,
		DialTimeout:     2 * time.Second,
		FindTimeout:     5 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewFinder: %v", err)
	}

	if err := f.pollErr(); err != nil {
		t.Fatalf("Poll through private-DHT exact discovery: %v", err)
	}
	if findCalls != 1 {
		t.Fatalf("exact root finder calls = %d, want 1", findCalls)
	}
	if misses := gate.missCount(); misses != 1 {
		t.Fatalf("injected cold-root misses = %d, want 1", misses)
	}
	if got := f.host.Libp2p().Network().Connectedness(w.host.ID()); got != network.Connected {
		t.Fatalf("writer connectedness after private-DHT Finder = %s, want connected", got)
	}
	if got, ok := f.heads.Get(testHead); !ok || !got.Root().Equals(w.head.Root()) {
		t.Fatalf("adopted root after private-DHT discovery = %v, present=%t; want %s", got, ok, w.head.Root())
	}
}

type unexpectedPointerError struct {
	got  pointerhint.Pointer
	want cid.Cid
}

func (e *unexpectedPointerError) Error() string {
	return "unexpected exact pointer " + e.got.Kind.String() + " " + e.got.CID.String() + ", want root " + e.want.String()
}

func newPrivatePointerDHT(t *testing.T, host *p2p.Host) *dht.IpfsDHT {
	t.Helper()
	routing, err := dht.New(t.Context(), host.Libp2p(), dht.Mode(dht.ModeServer))
	if err != nil {
		t.Fatalf("new private pointer DHT: %v", err)
	}
	t.Cleanup(func() {
		if err := routing.Close(); err != nil {
			t.Errorf("close private pointer DHT: %v", err)
		}
	})
	return routing
}

func waitPointerCondition(t *testing.T, description string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", description)
}
