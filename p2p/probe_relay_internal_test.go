package p2p

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/ipfs/boxo/blockstore"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/libp2p/go-libp2p/core/peer"
	circuitclient "github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/client"
	ma "github.com/multiformats/go-multiaddr"
)

// A relay is necessarily another connected Bitswap peer. This adversarial
// topology gives the challenge only to the relay, not to the addressed target,
// and proves traced block provenance prevents the relay's response from being
// credited to the target.
func TestProbePeerRejectsChallengeServedByRelay(t *testing.T) {
	relayHost := newRelayServiceTestHost(t, nil)
	target := newRelayEndpointTestHost(t)
	relayInfo := peer.AddrInfo{ID: relayHost.ID(), Addrs: relayHost.Addrs()}

	reserveCtx, cancelReserve := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancelReserve()
	if err := target.Connect(reserveCtx, relayInfo); err != nil {
		t.Fatalf("target connecting to relay: %v", err)
	}
	if _, err := circuitclient.Reserve(reserveCtx, target, relayInfo); err != nil {
		t.Fatalf("target reserving relay slot: %v", err)
	}

	challenge := blocks.NewBlock([]byte("available from relay but absent on target"))
	relayStore := blockstore.NewBlockstore(dssync.MutexWrap(datastore.NewMapDatastore()))
	if err := relayStore.Put(t.Context(), challenge); err != nil {
		t.Fatal(err)
	}
	relayExchange, err := NewExchange(t.Context(), ExchangeConfig{Host: &Host{h: relayHost}, Blocks: relayStore})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := relayExchange.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := relayExchange.NotifyNewBlocks(t.Context(), challenge); err != nil {
		t.Fatal(err)
	}

	relayPeer, err := ma.NewMultiaddr("/p2p/" + relayHost.ID().String())
	if err != nil {
		t.Fatal(err)
	}
	circuit := relayHost.Addrs()[0].Encapsulate(relayPeer).Encapsulate(ma.StringCast("/p2p-circuit"))
	probeCtx, cancelProbe := context.WithTimeout(t.Context(), 4*time.Second)
	defer cancelProbe()
	result, err := ProbePeer(probeCtx, peer.AddrInfo{ID: target.ID(), Addrs: []ma.Multiaddr{circuit}},
		[]cid.Cid{challenge.Cid()}, ProbeLimits{MaxCIDs: 1, MaxBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Reachable || len(result.Blocks) != 1 {
		t.Fatalf("relay probe = %+v", result)
	}
	if result.Blocks[0].Success || result.Blocks[0].Err == nil || !strings.Contains(result.Blocks[0].Err.Error(), "not target") {
		t.Fatalf("relay response was not failed closed: %+v", result.Blocks[0])
	}
}
