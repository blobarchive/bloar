package p2p

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-libp2p/p2p/protocol/holepunch"
	"github.com/libp2p/go-libp2p/p2p/transport/quicreuse"
	"github.com/marcopolo/simnet"
	ma "github.com/multiformats/go-multiaddr"
)

// TestRelayDCUtRUpgradesLimitedCircuitToDirect proves the complete control
// path without depending on a public network. The simulated firewall permits
// inbound traffic only to the relay, so a later non-limited peer connection
// demonstrates an actual DCUtR simultaneous-open upgrade rather than an
// ordinary direct dial.
func TestRelayDCUtRUpgradesLimitedCircuitToDirect(t *testing.T) {
	// Remove this quarantine after the initialization order is fixed in a tagged
	// go-libp2p release.
	if raceDetectorEnabled {
		t.Skip("go-libp2p v0.48.0 registers its hole-punch network notifiee before directDialTimeout is initialized; keep this end-to-end test in the normal suite until an upstream release fixes that startup race")
	}
	router := &simnet.SimpleFirewallRouter{}

	relayOptions, err := RelayOptions(RelayConfig{EnableService: true})
	if err != nil {
		t.Fatalf("relay service options: %v", err)
	}
	relayHost := newRelaySimnetHost(t, router, true, "/ip4/1.2.0.1/udp/8000/quic-v1", append([]libp2p.Option{
		libp2p.ForceReachabilityPublic(),
	}, relayOptions...)...)
	waitRelayTestCondition(t, 3*time.Second, "relay service protocol", func() bool {
		return hostSupportsRelayHop(relayHost)
	})

	targetRelay := RelayConfig{
		EnableHolePunching: true,
		StaticRelays:       []peer.AddrInfo{{ID: relayHost.ID(), Addrs: relayHost.Addrs()}},
		AutoRelay: AutoRelayConfig{
			DesiredReservations: 1,
			MinInterval:         10 * time.Millisecond,
			BootDelay:           10 * time.Millisecond,
			Backoff:             100 * time.Millisecond,
			MaxCandidateAge:     time.Minute,
		},
	}
	targetOptions, err := RelayOptions(targetRelay)
	if err != nil {
		t.Fatalf("target relay options: %v", err)
	}
	target := newRelaySimnetHost(t, router, false, "/ip4/2.2.0.2/udp/8001/quic-v1", append([]libp2p.Option{
		libp2p.ForceReachabilityPrivate(),
	}, targetOptions...)...)

	dialerOptions, err := RelayOptions(RelayConfig{EnableHolePunching: true})
	if err != nil {
		t.Fatalf("dialer relay options: %v", err)
	}
	dialer := newRelaySimnetHost(t, router, false, "/ip4/2.2.0.1/udp/8000/quic-v1", append([]libp2p.Option{
		libp2p.ForceReachabilityPrivate(),
	}, dialerOptions...)...)

	waitRelayTestCondition(t, 3*time.Second, "target DCUtR protocol", func() bool {
		return hostSupportsProtocol(target, holepunch.Protocol)
	})
	waitRelayTestCondition(t, 3*time.Second, "dialer DCUtR protocol", func() bool {
		return hostSupportsProtocol(dialer, holepunch.Protocol)
	})
	waitRelayTestCondition(t, 5*time.Second, "target AutoRelay reservation", func() bool {
		return target.ConnManager().IsProtected(relayHost.ID(), "autorelay")
	})

	var circuitAddrs []ma.Multiaddr
	waitRelayTestCondition(t, 5*time.Second, "target circuit address", func() bool {
		circuitAddrs = circuitAddresses(target.Addrs())
		return len(circuitAddrs) > 0
	})

	var observedLimited atomic.Bool
	notifiee := &network.NotifyBundle{ConnectedF: func(_ network.Network, connection network.Conn) {
		if connection.RemotePeer() == target.ID() && connection.Stat().Limited {
			observedLimited.Store(true)
		}
	}}
	dialer.Network().Notify(notifiee)
	t.Cleanup(func() { dialer.Network().StopNotify(notifiee) })

	connectCtx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := dialer.Connect(connectCtx, peer.AddrInfo{ID: target.ID(), Addrs: circuitAddrs}); err != nil {
		t.Fatalf("dialing target through relay circuit: %v", err)
	}
	waitRelayTestCondition(t, 5*time.Second, "limited relay connection observation", observedLimited.Load)
	waitRelayTestCondition(t, 8*time.Second, "DCUtR non-limited direct connection", func() bool {
		return peersHaveDirectConnection(dialer, target)
	})
}

func newRelaySimnetHost(t *testing.T, router *simnet.SimpleFirewallRouter, publiclyReachable bool, listen string, extra ...libp2p.Option) host.Host {
	t.Helper()
	base := []libp2p.Option{
		libp2p.ListenAddrs(ma.StringCast(listen)),
		relayQUICSimnet(publiclyReachable, router),
		libp2p.DisableMetrics(),
	}
	h, err := libp2p.New(append(base, extra...)...)
	if err != nil {
		t.Fatalf("building simulated host at %s: %v", listen, err)
	}
	t.Cleanup(func() { closeRelayTestHost(t, h) })
	return h
}

type relaySourceIPSelector struct {
	ip atomic.Pointer[net.IP]
}

func (s *relaySourceIPSelector) PreferredSourceIPForDestination(*net.UDPAddr) (net.IP, error) {
	return *s.ip.Load(), nil
}

func relayQUICSimnet(publiclyReachable bool, router *simnet.SimpleFirewallRouter) libp2p.Option {
	selector := &relaySourceIPSelector{}
	return libp2p.QUICReuse(
		quicreuse.NewConnManager,
		quicreuse.OverrideSourceIPSelector(func() (quicreuse.SourceIPSelector, error) {
			return selector, nil
		}),
		quicreuse.OverrideListenUDP(func(_ string, address *net.UDPAddr) (net.PacketConn, error) {
			selector.ip.Store(&address.IP)
			if publiclyReachable {
				router.SetAddrPubliclyReachable(address)
			}
			connection := simnet.NewSimConn(address)
			connection.SetUpPacketReceiver(router)
			router.AddNode(address, connection)
			return connection, nil
		}),
	)
}

func circuitAddresses(addresses []ma.Multiaddr) []ma.Multiaddr {
	var circuits []ma.Multiaddr
	for _, address := range addresses {
		if _, err := address.ValueForProtocol(ma.P_CIRCUIT); err == nil {
			circuits = append(circuits, address)
		}
	}
	return circuits
}

func hostSupportsProtocol(h host.Host, wanted protocol.ID) bool {
	for _, available := range h.Mux().Protocols() {
		if available == wanted {
			return true
		}
	}
	return false
}

func peersHaveDirectConnection(a, b host.Host) bool {
	for _, connection := range a.Network().ConnsToPeer(b.ID()) {
		if !connection.Stat().Limited {
			return true
		}
	}
	return false
}
