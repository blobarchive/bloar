package p2p

import (
	"sync"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"

	"github.com/blobarchive/bloar/metrics"
)

var (
	peerMetricDirections = [...]string{
		metrics.P2PDirectionInbound,
		metrics.P2PDirectionOutbound,
	}
	peerMetricTransports = [...]string{
		metrics.P2PTransportTCP,
		metrics.P2PTransportQUIC,
		metrics.P2PTransportRelay,
		metrics.P2PTransportOther,
	}
)

type peerMetricCell struct {
	direction string
	transport string
}

// hostPeerState owns both bounded peer classification and live-peer metric
// reconciliation. Network notifications are synchronous and may overlap, so
// one mutex serializes full snapshots: every callback publishes current network
// state instead of incrementing/decrementing counters that can drift when a
// peer has multiple connections or notifications arrive out of order.
//
// This snapshot is safe on the pinned go-libp2p v0.48 notification path:
// Swarm.addConn releases its conns lock before Connected, and Conn.doClose
// removes the connection (and releases that lock) before Disconnected.
// notifyAll itself holds only the independent notifiee read lock; Conns and
// Connectedness take only the conns read lock. Thus neither lookup tries to
// reacquire a lock held by its callback's caller.
//
// The callback never calls StopNotify or closes a connection. close first
// unregisters (waiting for in-flight callbacks), then takes mu and zeros the
// gauges, avoiding the notifiee-lock inversion that would deadlock shutdown.
type hostPeerState struct {
	network network.Network
	mx      *metrics.Metrics

	mu         sync.Mutex
	static     map[peer.ID]struct{}
	rendezvous map[peer.ID]struct{}
	closed     bool
	notifiee   *network.NotifyBundle
}

func newHostPeerState(n network.Network, mx *metrics.Metrics, configured []peer.AddrInfo) *hostPeerState {
	s := &hostPeerState{
		network:    n,
		mx:         mx,
		static:     make(map[peer.ID]struct{}, len(configured)),
		rendezvous: make(map[peer.ID]struct{}),
	}
	for _, ai := range configured {
		s.static[ai.ID] = struct{}{}
	}
	s.notifiee = &network.NotifyBundle{
		ConnectedF: func(n network.Network, _ network.Conn) {
			s.reconcile(n, "")
		},
		DisconnectedF: func(n network.Network, c network.Conn) {
			s.reconcile(n, c.RemotePeer())
		},
	}
	n.Notify(s.notifiee)
	// Notify has no replay. This snapshot covers connections established before
	// registration; a concurrent connection also emits a callback, and both
	// paths reconcile rather than apply a delta, so either ordering converges.
	s.reconcile(n, "")
	return s
}

func (s *hostPeerState) reconcile(n network.Network, disconnected peer.ID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	if disconnected != "" && !peerMetricConnectedness(n.Connectedness(disconnected)) {
		delete(s.rendezvous, disconnected)
	}
	if s.mx != nil {
		setLivePeerMetrics(s.mx, livePeerCounts(n.Conns()))
	}
}

func (s *hostPeerState) close() {
	if s == nil {
		return
	}
	// Do not hold s.mu here: StopNotify waits for callbacks, and callbacks take
	// s.mu. Once it returns no new callback can start.
	s.network.StopNotify(s.notifiee)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.closed = true
	clear(s.rendezvous)
	setLivePeerMetrics(s.mx, nil)
}

// markRendezvous retains only a peer that is connected at the same instant the
// state mutex is held. A last-connection callback takes the same mutex and
// removes it, so a disconnect racing this method cannot leave stale class
// state. The map is therefore bounded by peers with live connections.
func (s *hostPeerState) markRendezvous(id peer.ID) bool {
	if s == nil || id == "" {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || !peerMetricConnectedness(s.network.Connectedness(id)) {
		return false
	}
	s.rendezvous[id] = struct{}{}
	return true
}

func peerMetricConnectedness(connectedness network.Connectedness) bool {
	return connectedness == network.Connected || connectedness == network.Limited
}

func (s *hostPeerState) bitswapPeerClass(id peer.ID) string {
	if s == nil {
		return metrics.BitswapPeerOther
	}
	s.mu.Lock()
	_, static := s.static[id]
	_, rendezvous := s.rendezvous[id]
	closed := s.closed
	s.mu.Unlock()

	relay := false
	if !closed && !static && !rendezvous {
		for _, conn := range s.network.ConnsToPeer(id) {
			if peerMetricTransport(conn) == metrics.P2PTransportRelay {
				relay = true
				break
			}
		}
	}
	return prioritizedBitswapPeerClass(static, rendezvous, relay)
}

func prioritizedBitswapPeerClass(static, rendezvous, relay bool) string {
	switch {
	case static:
		return metrics.BitswapPeerStatic
	case rendezvous:
		return metrics.BitswapPeerRendezvous
	case relay:
		return metrics.BitswapPeerRelay
	default:
		return metrics.BitswapPeerOther
	}
}

// MarkRendezvousPeer classifies a connected peer for outbound Bitswap metrics.
// It returns false and retains nothing unless id is currently connected. The
// mark survives additional connections to the same peer and is removed after
// its last connection closes, keeping classification state bounded by live
// peers. Static classification always takes precedence.
func (p *Host) MarkRendezvousPeer(id peer.ID) bool {
	if p == nil {
		return false
	}
	return p.peerState.markRendezvous(id)
}

func livePeerCounts(conns []network.Conn) map[peerMetricCell]int {
	peers := make(map[peerMetricCell]map[peer.ID]struct{})
	for _, conn := range conns {
		direction, ok := peerMetricDirection(conn.Stat().Direction)
		if !ok {
			continue
		}
		cell := peerMetricCell{direction: direction, transport: peerMetricTransport(conn)}
		ids := peers[cell]
		if ids == nil {
			ids = make(map[peer.ID]struct{})
			peers[cell] = ids
		}
		ids[conn.RemotePeer()] = struct{}{}
	}
	counts := make(map[peerMetricCell]int, len(peers))
	for cell, ids := range peers {
		counts[cell] = len(ids)
	}
	return counts
}

func setLivePeerMetrics(mx *metrics.Metrics, counts map[peerMetricCell]int) {
	if mx == nil {
		return
	}
	for _, direction := range peerMetricDirections {
		for _, transport := range peerMetricTransports {
			mx.P2PLivePeers(direction, transport, counts[peerMetricCell{direction: direction, transport: transport}])
		}
	}
}

func peerMetricDirection(direction network.Direction) (string, bool) {
	switch direction {
	case network.DirInbound:
		return metrics.P2PDirectionInbound, true
	case network.DirOutbound:
		return metrics.P2PDirectionOutbound, true
	default:
		return "", false
	}
}

func peerMetricTransport(conn network.Conn) string {
	return peerMetricTransportFor(conn.Stat().Limited, conn.LocalMultiaddr(), conn.RemoteMultiaddr())
}

func peerMetricTransportFor(limited bool, local, remote ma.Multiaddr) string {
	switch {
	case limited || multiaddrHasProtocol(local, ma.P_CIRCUIT) || multiaddrHasProtocol(remote, ma.P_CIRCUIT):
		return metrics.P2PTransportRelay
	case multiaddrHasProtocol(local, ma.P_QUIC, ma.P_QUIC_V1, ma.P_WEBTRANSPORT) ||
		multiaddrHasProtocol(remote, ma.P_QUIC, ma.P_QUIC_V1, ma.P_WEBTRANSPORT):
		return metrics.P2PTransportQUIC
	case multiaddrHasProtocol(local, ma.P_TCP) || multiaddrHasProtocol(remote, ma.P_TCP):
		return metrics.P2PTransportTCP
	default:
		return metrics.P2PTransportOther
	}
}

func multiaddrHasProtocol(addr ma.Multiaddr, protocols ...int) bool {
	if addr == nil {
		return false
	}
	found := false
	ma.ForEach(addr, func(component ma.Component) bool {
		for _, protocol := range protocols {
			if component.Protocol().Code == protocol {
				found = true
				return false
			}
		}
		return true
	})
	return found
}
