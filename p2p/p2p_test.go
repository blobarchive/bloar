package p2p_test

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/protocol/holepunch"

	"github.com/blobarchive/bloar/p2p"
)

func TestHostConfigWiresDCUtRIntoRealHost(t *testing.T) {
	h := newTestHost(t, func(cfg *p2p.HostConfig) {
		cfg.Relay = p2p.DefaultRelayConfig()
		// Hole-punch protocol registration waits for a public address. This
		// documentation-only address is an explicit test reachability claim; the
		// real listener remains loopback and no packet is sent to it.
		cfg.Announce = []string{"/ip4/1.1.1.1/tcp/4001"}
	})
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		for _, candidate := range h.Libp2p().Mux().Protocols() {
			if candidate == holepunch.Protocol {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("real host protocols %v do not include %s", h.Libp2p().Mux().Protocols(), holepunch.Protocol)
}

// TestHostAnnounceDerived: with no p2p.announce, the document's multiaddrs are
// the addresses the host actually bound, each naming the peer. A follower has
// to be able to dial what it reads there, so the PeerID is not optional.
func TestHostAnnounceDerived(t *testing.T) {
	h := newTestHost(t)

	addrs := h.AnnounceAddrs()
	if len(addrs) == 0 {
		t.Fatal("a listening host announced no addresses")
	}
	for _, a := range addrs {
		if !strings.Contains(a, "/p2p/"+h.ID().String()) {
			t.Errorf("announce addr %q does not name the peer %s", a, h.ID())
		}
		if !strings.Contains(a, "127.0.0.1") {
			t.Errorf("announce addr %q is not the address the host bound", a)
		}
		if strings.Contains(a, "/tcp/0") {
			t.Errorf("announce addr %q has the wildcard port rather than the bound one", a)
		}
	}
}

// TestHostAnnounceConfiguredCompletesPeerID: reachability is the operator's
// claim (a NAT, a load balancer, a DNS name), so a configured addr is published
// as written -- except that a follower dials the /p2p/<peerid> it names, and an
// addr that omits one is completed with this host's own peer ID rather than
// published as something no follower can use.
func TestHostAnnounceConfiguredCompletesPeerID(t *testing.T) {
	h := newTestHost(t, func(c *p2p.HostConfig) {
		c.Announce = []string{"/ip4/198.51.100.10/tcp/4001"}
	})

	want := "/ip4/198.51.100.10/tcp/4001/p2p/" + h.ID().String()
	got := h.AnnounceAddrs()
	if len(got) != 1 || got[0] != want {
		t.Errorf("announce = %v, want the configured addr completed with the peer ID %q", got, want)
	}
	if got := h.Libp2p().Addrs(); len(got) != 1 || got[0].String() != "/ip4/198.51.100.10/tcp/4001" {
		t.Errorf("identify addrs = %v, want the configured transport address without /p2p", got)
	}
}

// TestHostAnnounceConfiguredKeepsOwnPeerID: an operator who wrote the full form
// is right, and the addr is published exactly as written. The peer ID has to be
// this node's, so it is derived from the same key file the host is built with.
func TestHostAnnounceConfiguredKeepsOwnPeerID(t *testing.T) {
	keyFile := filepath.Join(t.TempDir(), "p2p.key")
	key, _, err := p2p.LoadOrCreateIdentity(keyFile)
	if err != nil {
		t.Fatalf("creating identity: %v", err)
	}
	id, err := peer.IDFromPrivateKey(key)
	if err != nil {
		t.Fatalf("deriving PeerID: %v", err)
	}

	want := "/ip4/198.51.100.10/tcp/4001/p2p/" + id.String()
	h := newTestHost(t, func(c *p2p.HostConfig) {
		c.IdentityKeyFile = keyFile
		c.Announce = []string{want}
	})

	got := h.AnnounceAddrs()
	if len(got) != 1 || got[0] != want {
		t.Errorf("announce = %v, want the configured addr kept as written %q", got, want)
	}
	if got := h.Libp2p().Addrs(); len(got) != 1 || got[0].String() != "/ip4/198.51.100.10/tcp/4001" {
		t.Errorf("identify addrs = %v, want terminal /p2p/%s stripped", got, id)
	}
}

// TestHostAnnounceRejectsForeignPeerID: an announce addr that names a different
// node is not an address but a config error -- announcing another node's
// identity -- and it stops startup rather than being published.
func TestHostAnnounceRejectsForeignPeerID(t *testing.T) {
	_, err := p2p.NewHost(t.Context(), p2p.HostConfig{
		Listen:          []string{"/ip4/127.0.0.1/tcp/0"},
		Announce:        []string{"/ip4/198.51.100.10/tcp/4001/p2p/12D3KooWA6Wm6iNn2LcAwPHnLhSCE6dSyRPmFRDyPvL9dcJZuJfF"},
		IdentityKeyFile: filepath.Join(t.TempDir(), "p2p.key"),
	})
	if err == nil {
		t.Fatal("a p2p.announce naming another node's identity was accepted")
	}
	if !strings.Contains(err.Error(), "p2p.announce") {
		t.Errorf("error = %q, want it to blame p2p.announce", err)
	}
}

// TestHostAnnounceRejectsBadMultiaddr: a typo in p2p.announce is a startup
// failure, not a document nobody can use.
func TestHostAnnounceRejectsBadMultiaddr(t *testing.T) {
	_, err := p2p.NewHost(t.Context(), p2p.HostConfig{
		Listen:          []string{"/ip4/127.0.0.1/tcp/0"},
		Announce:        []string{"not-a-multiaddr"},
		IdentityKeyFile: filepath.Join(t.TempDir(), "p2p.key"),
	})
	if err == nil {
		t.Fatal("an unparseable p2p.announce was accepted")
	}
	if !strings.Contains(err.Error(), "p2p.announce") {
		t.Errorf("error = %q, want it to blame p2p.announce", err)
	}
}

func TestHostAnnounceRejectsBarePeerID(t *testing.T) {
	keyFile := filepath.Join(t.TempDir(), "p2p.key")
	key, _, err := p2p.LoadOrCreateIdentity(keyFile)
	if err != nil {
		t.Fatalf("creating identity: %v", err)
	}
	id, err := peer.IDFromPrivateKey(key)
	if err != nil {
		t.Fatalf("deriving PeerID: %v", err)
	}

	_, err = p2p.NewHost(t.Context(), p2p.HostConfig{
		Listen:          []string{"/ip4/127.0.0.1/tcp/0"},
		Announce:        []string{"/p2p/" + id.String()},
		IdentityKeyFile: keyFile,
	})
	if err == nil || !strings.Contains(err.Error(), "no transport address") {
		t.Fatalf("bare peer ID error = %v, want no transport address", err)
	}
}

// TestHostNoListenAddrs: an empty p2p.listen binds nothing. libp2p's own
// default is to listen on :4001 when it is told nothing, so this asserts we do
// not fall into it -- a follower behind a NAT asked for no listener and must
// get none.
func TestHostNoListenAddrs(t *testing.T) {
	h := newTestHost(t, func(c *p2p.HostConfig) { c.Listen = nil })

	if addrs := h.Libp2p().Addrs(); len(addrs) != 0 {
		t.Errorf("a host with no p2p.listen bound %v", addrs)
	}
	if addrs := h.AnnounceAddrs(); len(addrs) != 0 {
		t.Errorf("a host with no p2p.listen announced %v", addrs)
	}
}

// TestHostListensOnTCPAndQUIC proves the two multiaddrs selected by the command
// are backed by real transports. Ephemeral ports keep the test hermetic; the
// command-level default test separately asserts both defaults use port 4001.
func TestHostListensOnTCPAndQUIC(t *testing.T) {
	h := newTestHost(t, func(c *p2p.HostConfig) {
		c.Listen = []string{
			"/ip4/127.0.0.1/tcp/0",
			"/ip4/127.0.0.1/udp/0/quic-v1",
		}
	})

	var tcp, quic bool
	for _, addr := range h.Libp2p().Network().ListenAddresses() {
		s := addr.String()
		tcp = tcp || strings.Contains(s, "/tcp/")
		quic = quic || strings.Contains(s, "/udp/") && strings.Contains(s, "/quic-v1")
	}
	if !tcp || !quic {
		t.Errorf("listen addresses = %v, want both TCP and QUIC-v1", h.Libp2p().Network().ListenAddresses())
	}
}

func TestHostConnectsOverQUIC(t *testing.T) {
	listener := newTestHost(t, func(c *p2p.HostConfig) {
		c.Listen = []string{"/ip4/127.0.0.1/udp/0/quic-v1"}
	})
	dialer := newTestHost(t)

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	if err := dialer.Libp2p().Connect(ctx, peer.AddrInfo{
		ID:    listener.ID(),
		Addrs: listener.Libp2p().Addrs(),
	}); err != nil {
		t.Fatalf("connecting over QUIC-v1: %v", err)
	}

	connections := dialer.Libp2p().Network().ConnsToPeer(listener.ID())
	if len(connections) == 0 || !strings.Contains(connections[0].RemoteMultiaddr().String(), "/quic-v1") {
		t.Fatalf("connections = %v, want a QUIC-v1 connection", connections)
	}
}

// TestHostPeersRejectsAddrWithoutPeerID: a static peer is an identity at an
// address, so an address alone is a config error rather than something to dial
// and hope about.
func TestHostPeersRejectsAddrWithoutPeerID(t *testing.T) {
	_, err := p2p.NewHost(t.Context(), p2p.HostConfig{
		Listen:          []string{"/ip4/127.0.0.1/tcp/0"},
		Peers:           []string{"/ip4/127.0.0.1/tcp/4001"},
		IdentityKeyFile: filepath.Join(t.TempDir(), "p2p.key"),
	})
	if err == nil {
		t.Fatal("a static peer with no /p2p/ component was accepted")
	}
	if !strings.Contains(err.Error(), "p2p.peers") {
		t.Errorf("error = %q, want it to blame p2p.peers", err)
	}
}

// TestHostDialsStaticPeers is spec 11.2's static peering: the peers in the
// config are connected at startup, without anything else asking.
func TestHostDialsStaticPeers(t *testing.T) {
	target := newTestHost(t)
	addr := target.AnnounceAddrs()[0]

	dialer := newTestHost(t, func(c *p2p.HostConfig) { c.Peers = []string{addr} })

	waitFor(t, "the static peer to be connected", func() bool {
		return dialer.Libp2p().Network().Connectedness(target.ID()) == network.Connected
	})
}

// TestHostIdentityIsStableAcrossRestart: the same key file is the same node.
// This is what makes a published multiaddr survive a restart, and it is the
// reason the identity is a file rather than something generated per process.
func TestHostIdentityIsStableAcrossRestart(t *testing.T) {
	keyFile := filepath.Join(t.TempDir(), "p2p.key")
	opt := func(c *p2p.HostConfig) { c.IdentityKeyFile = keyFile }

	first := newTestHost(t, opt)
	firstID := first.ID()
	if err := first.Close(); err != nil {
		t.Fatalf("closing first host: %v", err)
	}

	second := newTestHost(t, opt)
	if second.ID() != firstID {
		t.Errorf("PeerID moved across a restart: %s -> %s", firstID, second.ID())
	}
}

// TestHostCloseIsIdempotent: bloard closes in a defer, and a test closes early.
func TestHostCloseIsIdempotent(t *testing.T) {
	h := newTestHost(t)
	if err := h.Close(); err != nil {
		t.Fatalf("first close: %v", err)
	}
	if err := h.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
}

// waitFor polls cond until it holds, and fails the test if it does not. The
// things it waits on here are network events on loopback: they happen in
// milliseconds, and the generous ceiling is so that a loaded CI machine reports
// a real failure rather than a timing one.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}
