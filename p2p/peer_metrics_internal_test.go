package p2p

import (
	"sync"
	"testing"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"

	"github.com/blobarchive/bloar/metrics"
)

func TestLivePeerCountsDeduplicatesWithinEachCell(t *testing.T) {
	first := peer.ID("first-peer")
	second := peer.ID("second-peer")
	conns := []network.Conn{
		newPeerMetricTestConn("first-tcp-1", first, network.DirInbound, false, "/ip4/127.0.0.1/tcp/1"),
		newPeerMetricTestConn("first-tcp-2", first, network.DirInbound, false, "/ip4/127.0.0.1/tcp/2"),
		newPeerMetricTestConn("second-tcp", second, network.DirInbound, false, "/ip4/127.0.0.1/tcp/3"),
		newPeerMetricTestConn("first-quic", first, network.DirOutbound, false, "/ip4/127.0.0.1/udp/4/quic-v1"),
		newPeerMetricTestConn("relay", peer.ID("relay-peer"), network.DirInbound, true, "/ip4/127.0.0.1/tcp/5"),
		newPeerMetricTestConn("unknown", peer.ID("unknown-peer"), network.DirUnknown, false, "/ip4/127.0.0.1/tcp/6"),
	}

	counts := livePeerCounts(conns)
	want := map[peerMetricCell]int{
		{direction: metrics.P2PDirectionInbound, transport: metrics.P2PTransportTCP}:   2,
		{direction: metrics.P2PDirectionOutbound, transport: metrics.P2PTransportQUIC}: 1,
		{direction: metrics.P2PDirectionInbound, transport: metrics.P2PTransportRelay}: 1,
	}
	if len(counts) != len(want) {
		t.Fatalf("live peer cells = %v, want %v", counts, want)
	}
	for cell, n := range want {
		if got := counts[cell]; got != n {
			t.Errorf("live peers in %+v = %d, want %d", cell, got, n)
		}
	}
}

func TestPeerMetricTransportClassesStayClosed(t *testing.T) {
	tests := []struct {
		name    string
		limited bool
		local   string
		remote  string
		want    string
	}{
		{name: "limited is relay", limited: true, remote: "/ip4/127.0.0.1/tcp/1", want: metrics.P2PTransportRelay},
		{name: "circuit is relay", remote: "/p2p-circuit", want: metrics.P2PTransportRelay},
		{name: "quic v1", remote: "/ip4/127.0.0.1/udp/1/quic-v1", want: metrics.P2PTransportQUIC},
		{name: "webtransport", remote: "/ip4/127.0.0.1/udp/1/quic-v1/webtransport", want: metrics.P2PTransportQUIC},
		{name: "tcp", local: "/ip4/127.0.0.1/tcp/1", want: metrics.P2PTransportTCP},
		{name: "other", remote: "/ip4/127.0.0.1/udp/1", want: metrics.P2PTransportOther},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var local, remote ma.Multiaddr
			if tt.local != "" {
				local = ma.StringCast(tt.local)
			}
			if tt.remote != "" {
				remote = ma.StringCast(tt.remote)
			}
			if got := peerMetricTransportFor(tt.limited, local, remote); got != tt.want {
				t.Fatalf("transport = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestBitswapPeerClassPrecedence(t *testing.T) {
	for _, tt := range []struct {
		name                      string
		static, rendezvous, relay bool
		want                      string
	}{
		{name: "static over both", static: true, rendezvous: true, relay: true, want: metrics.BitswapPeerStatic},
		{name: "rendezvous over relay", rendezvous: true, relay: true, want: metrics.BitswapPeerRendezvous},
		{name: "relay", relay: true, want: metrics.BitswapPeerRelay},
		{name: "other", want: metrics.BitswapPeerOther},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := prioritizedBitswapPeerClass(tt.static, tt.rendezvous, tt.relay); got != tt.want {
				t.Fatalf("class = %q, want %q", got, tt.want)
			}
		})
	}

	// Exercise the real classifier as well as its precedence helper: a static
	// peer remains static even when rendezvous marked and connected by relay.
	n := newPeerMetricTestNetwork()
	id := peer.ID("static-rendezvous-relay-peer")
	state := newHostPeerState(n, nil, []peer.AddrInfo{{ID: id}})
	t.Cleanup(state.close)
	n.connect(newPeerMetricTestConn("relay", id, network.DirOutbound, true, "/ip4/127.0.0.1/tcp/1"))
	if !state.markRendezvous(id) {
		t.Fatal("connected static relay peer could not be rendezvous marked")
	}
	if got := state.bitswapPeerClass(id); got != metrics.BitswapPeerStatic {
		t.Fatalf("real classifier class = %q, want static", got)
	}
}

func TestRendezvousClassRetainedUntilLastConnectionCloses(t *testing.T) {
	n := newPeerMetricTestNetwork()
	state := newHostPeerState(n, nil, nil)
	t.Cleanup(state.close)
	id := peer.ID("rendezvous-peer")

	if state.markRendezvous(id) {
		t.Fatal("disconnected rendezvous peer was retained")
	}
	first := newPeerMetricTestConn("first", id, network.DirOutbound, false, "/ip4/127.0.0.1/tcp/1")
	second := newPeerMetricTestConn("second", id, network.DirInbound, false, "/ip4/127.0.0.1/tcp/2")
	n.connect(first)
	n.connect(second)
	if !state.markRendezvous(id) {
		t.Fatal("connected rendezvous peer was not retained")
	}
	if got := state.bitswapPeerClass(id); got != metrics.BitswapPeerRendezvous {
		t.Fatalf("class with two live connections = %q, want rendezvous", got)
	}

	n.disconnect(first)
	if got := state.bitswapPeerClass(id); got != metrics.BitswapPeerRendezvous {
		t.Fatalf("class after one of two connections closed = %q, want rendezvous", got)
	}
	n.disconnect(second)
	if got := state.bitswapPeerClass(id); got != metrics.BitswapPeerOther {
		t.Fatalf("class after last connection closed = %q, want other", got)
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if len(state.rendezvous) != 0 {
		t.Fatalf("retained rendezvous state after last disconnect: %v", state.rendezvous)
	}
}

type peerMetricTestConn struct {
	network.Conn
	id         string
	remote     peer.ID
	stats      network.ConnStats
	localAddr  ma.Multiaddr
	remoteAddr ma.Multiaddr
}

func newPeerMetricTestConn(id string, remote peer.ID, direction network.Direction, limited bool, addr string) *peerMetricTestConn {
	maddr := ma.StringCast(addr)
	return &peerMetricTestConn{
		id:         id,
		remote:     remote,
		stats:      network.ConnStats{Stats: network.Stats{Direction: direction, Limited: limited}},
		localAddr:  maddr,
		remoteAddr: maddr,
	}
}

func (c *peerMetricTestConn) ID() string                    { return c.id }
func (c *peerMetricTestConn) RemotePeer() peer.ID           { return c.remote }
func (c *peerMetricTestConn) Stat() network.ConnStats       { return c.stats }
func (c *peerMetricTestConn) LocalMultiaddr() ma.Multiaddr  { return c.localAddr }
func (c *peerMetricTestConn) RemoteMultiaddr() ma.Multiaddr { return c.remoteAddr }

type peerMetricTestNetwork struct {
	network.Network
	mu       sync.Mutex
	conns    map[peer.ID][]network.Conn
	notifiee network.Notifiee
}

func newPeerMetricTestNetwork() *peerMetricTestNetwork {
	return &peerMetricTestNetwork{conns: make(map[peer.ID][]network.Conn)}
}

func (n *peerMetricTestNetwork) Notify(notifiee network.Notifiee) {
	n.mu.Lock()
	n.notifiee = notifiee
	n.mu.Unlock()
}

func (n *peerMetricTestNetwork) StopNotify(notifiee network.Notifiee) {
	n.mu.Lock()
	if n.notifiee == notifiee {
		n.notifiee = nil
	}
	n.mu.Unlock()
}

func (n *peerMetricTestNetwork) Connectedness(id peer.ID) network.Connectedness {
	n.mu.Lock()
	defer n.mu.Unlock()
	if len(n.conns[id]) > 0 {
		return network.Connected
	}
	return network.NotConnected
}

func (n *peerMetricTestNetwork) Conns() []network.Conn {
	n.mu.Lock()
	defer n.mu.Unlock()
	var out []network.Conn
	for _, conns := range n.conns {
		out = append(out, conns...)
	}
	return out
}

func (n *peerMetricTestNetwork) ConnsToPeer(id peer.ID) []network.Conn {
	n.mu.Lock()
	defer n.mu.Unlock()
	return append([]network.Conn(nil), n.conns[id]...)
}

func (n *peerMetricTestNetwork) connect(conn network.Conn) {
	n.mu.Lock()
	id := conn.RemotePeer()
	n.conns[id] = append(n.conns[id], conn)
	notifiee := n.notifiee
	n.mu.Unlock()
	if notifiee != nil {
		notifiee.Connected(n, conn)
	}
}

func (n *peerMetricTestNetwork) disconnect(conn network.Conn) {
	n.mu.Lock()
	id := conn.RemotePeer()
	conns := n.conns[id]
	for i, candidate := range conns {
		if candidate == conn {
			conns = append(conns[:i], conns[i+1:]...)
			break
		}
	}
	if len(conns) == 0 {
		delete(n.conns, id)
	} else {
		n.conns[id] = conns
	}
	notifiee := n.notifiee
	n.mu.Unlock()
	if notifiee != nil {
		notifiee.Disconnected(n, conn)
	}
}
