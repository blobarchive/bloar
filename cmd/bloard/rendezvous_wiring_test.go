package main

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/libp2p/go-libp2p/core/peer"
)

func TestSetupP2PBuildsPrivateRendezvousDHTWithoutIPNS(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	cfg := loadString(t, fmt.Sprintf(`
net: mainnet
beacon: {genesis_time: 1}
store: {path: %s}
server: {auth_token_file: /t}
heads: {all: {}}
p2p:
  listen: ["/ip4/127.0.0.1/tcp/0"]
  nat_port_map: false
  dht: {bootstrap: private}
`, filepath.Join(dir, "store")))
	st := openStore(t, cfg.Store.Path)
	defer st.Close()

	publicCalled := false
	n, err := setupP2PWithDeps(t.Context(), cfg, st, nil, nil, newLogger(), p2pSetupDeps{
		publicBootstrapPeers: func() []peer.AddrInfo {
			publicCalled = true
			return nil
		},
	})
	if err != nil {
		t.Fatalf("setupP2PWithDeps: %v", err)
	}
	defer n.close(newLogger())
	if publicCalled {
		t.Fatal("private bootstrap mode read the public bootstrap set")
	}
	if n.host == nil || n.exchange == nil || n.dht == nil || n.rendezvous == nil {
		t.Fatalf("private rendezvous stack incomplete: host=%v exchange=%v dht=%v rendezvous=%v",
			n.host != nil, n.exchange != nil, n.dht != nil, n.rendezvous != nil)
	}
	if n.publisher != nil {
		t.Fatal("rendezvous-only node unexpectedly built an IPNS publisher")
	}
}

func TestSetupP2PPublicDefaultUsesInjectedBootstrapSource(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	cfg := loadString(t, fmt.Sprintf(`
net: mainnet
beacon: {genesis_time: 1}
store: {path: %s}
server: {auth_token_file: /t}
heads: {all: {}}
p2p:
  listen: ["/ip4/127.0.0.1/tcp/0"]
  nat_port_map: false
`, filepath.Join(dir, "store")))
	st := openStore(t, cfg.Store.Path)
	defer st.Close()

	publicCalls := 0
	n, err := setupP2PWithDeps(t.Context(), cfg, st, nil, nil, newLogger(), p2pSetupDeps{
		publicBootstrapPeers: func() []peer.AddrInfo {
			publicCalls++
			return nil // Hermetic: prove selection without public network access.
		},
	})
	if err != nil {
		t.Fatalf("setupP2PWithDeps: %v", err)
	}
	defer n.close(newLogger())
	if publicCalls != 1 {
		t.Fatalf("public bootstrap source calls = %d, want 1", publicCalls)
	}
	if n.dht == nil || n.rendezvous == nil {
		t.Fatal("default public embedded swarm did not build DHT+rendezvous without IPNS")
	}
}

func TestSetupP2PRendezvousOptOutKeepsStaticBitswapWithoutDHT(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	cfg := loadString(t, fmt.Sprintf(`
net: mainnet
beacon: {genesis_time: 1}
store: {path: %s}
server: {auth_token_file: /t}
heads: {all: {}}
p2p:
  listen: ["/ip4/127.0.0.1/tcp/0"]
  nat_port_map: false
  dht: {bootstrap: private}
  rendezvous: {enabled: false}
`, filepath.Join(dir, "store")))
	st := openStore(t, cfg.Store.Path)
	defer st.Close()

	n, err := setupP2PWithDeps(t.Context(), cfg, st, nil, nil, newLogger(), p2pSetupDeps{
		publicBootstrapPeers: func() []peer.AddrInfo {
			t.Fatal("rendezvous-disabled private stack consulted public bootstrap peers")
			return nil
		},
	})
	if err != nil {
		t.Fatalf("setupP2PWithDeps: %v", err)
	}
	defer n.close(newLogger())
	if n.host == nil || n.exchange == nil {
		t.Fatal("rendezvous opt-out disabled the static host or Bitswap exchange")
	}
	if n.dht != nil || n.rendezvous != nil || n.publisher != nil {
		t.Fatalf("rendezvous opt-out built routing state: dht=%v rendezvous=%v publisher=%v",
			n.dht != nil, n.rendezvous != nil, n.publisher != nil)
	}
}
