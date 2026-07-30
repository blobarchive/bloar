package p2p_test

import (
	"context"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/network"

	"github.com/blobarchive/bloar/p2p"
)

// TestRendezvousDiscoveryConnectsBitswapPeers is the private-network
// rendezvous data-plane acceptance test. The serving and joining nodes have no
// direct/static relationship: each knows only a third DHT node. The provider
// record connects them after both Bitswap exchanges are already running, and a
// real block then crosses that discovered connection. The DHT is never handed
// to Bitswap as a generic content router.
func TestRendezvousDiscoveryConnectsBitswapPeers(t *testing.T) {
	bootstrap, serving, joining := newTestHost(t), newTestHost(t), newTestHost(t)
	bootstrapDHT := newTestDHT(t, bootstrap)
	servingDHT := newTestDHT(t, serving)
	joiningDHT := newTestDHT(t, joining)

	connect(t, serving, bootstrap)
	connect(t, joining, bootstrap)
	waitFor(t, "the private DHT routing tables to populate", func() bool {
		return bootstrapDHT.RoutingTable().Size() >= 2 &&
			servingDHT.RoutingTable().Size() >= 1 && joiningDHT.RoutingTable().Size() >= 1
	})
	for _, d := range []interface{ Bootstrap(context.Context) error }{bootstrapDHT, servingDHT, joiningDHT} {
		if err := d.Bootstrap(t.Context()); err != nil {
			t.Fatalf("bootstrapping private DHT: %v", err)
		}
	}

	servingBlocks, joiningBlocks := memBlocks(), memBlocks()
	want := rawBlock(t, []byte("fetched from a rendezvous-discovered Bitswap peer"))
	putBlock(t, servingBlocks, want)
	newTestExchange(t, serving, servingBlocks)
	joiningExchange := newTestExchange(t, joining, joiningBlocks)

	target := p2p.RendezvousTarget{Network: "mainnet", Head: "arbitrum-one"}
	key, err := p2p.RendezvousCID(target.Network, target.Head)
	if err != nil {
		t.Fatalf("deriving rendezvous key: %v", err)
	}
	servingRendezvous, err := p2p.NewRendezvousService(t.Context(), p2p.RendezvousConfig{
		Host:         serving,
		Router:       servingDHT,
		Targets:      []p2p.RendezvousTarget{target},
		Interval:     time.Hour,
		Jitter:       time.Minute,
		RoundTimeout: 10 * time.Second,
	})
	if err != nil {
		t.Fatalf("starting serving rendezvous: %v", err)
	}
	t.Cleanup(func() {
		if err := servingRendezvous.Close(); err != nil {
			t.Errorf("closing serving rendezvous: %v", err)
		}
	})

	// Wait on the third node's local provider store rather than issuing a
	// second network lookup. This proves the service's startup Provide reached
	// the DHT while leaving the joining service to perform the actual lookup.
	waitFor(t, "the rendezvous provider record to propagate", func() bool {
		providers, err := bootstrapDHT.ProviderStore().GetProviders(t.Context(), key.Hash())
		if err != nil {
			return false
		}
		for _, provider := range providers {
			if provider.ID == serving.ID() {
				return true
			}
		}
		return false
	})
	// A DHT lookup is itself allowed to open transient connections to nodes it
	// learns from the routing table. Remove any such control-plane connection
	// so the joining rendezvous round demonstrably starts without a data peer.
	if err := joining.Libp2p().Network().ClosePeer(serving.ID()); err != nil {
		t.Fatalf("clearing pre-discovery DHT connection from joining node: %v", err)
	}
	if err := serving.Libp2p().Network().ClosePeer(joining.ID()); err != nil {
		t.Fatalf("clearing pre-discovery DHT connection from serving node: %v", err)
	}
	waitFor(t, "the nodes to have no pre-discovery connection", func() bool {
		return joining.Libp2p().Network().Connectedness(serving.ID()) != network.Connected
	})

	joiningRendezvous, err := p2p.NewRendezvousService(t.Context(), p2p.RendezvousConfig{
		Host:               joining,
		Router:             joiningDHT,
		Targets:            []p2p.RendezvousTarget{target},
		DisableProviding:   true,
		Interval:           time.Hour,
		Jitter:             time.Minute,
		MaxProviderResults: 8,
		DialConcurrency:    2,
		DialTimeout:        5 * time.Second,
		RoundTimeout:       10 * time.Second,
	})
	if err != nil {
		t.Fatalf("starting joining rendezvous: %v", err)
	}
	t.Cleanup(func() {
		if err := joiningRendezvous.Close(); err != nil {
			t.Errorf("closing joining rendezvous: %v", err)
		}
	})
	waitFor(t, "joining node to dial the discovered provider", func() bool {
		return joining.Libp2p().Network().Connectedness(serving.ID()) == network.Connected
	})

	fetching := p2p.FetchingBlockstore(t.Context(), joiningBlocks, joiningExchange)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	got, err := fetching.Get(ctx, want.Cid())
	if err != nil {
		t.Fatalf("fetching from rendezvous-discovered peer: %v", err)
	}
	if got.Cid() != want.Cid() || string(got.RawData()) != string(want.RawData()) {
		t.Fatalf("fetched block (%s, %q), want (%s, %q)", got.Cid(), got.RawData(), want.Cid(), want.RawData())
	}
}
