package p2p

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/event"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/core/protocol"
	connmgr "github.com/libp2p/go-libp2p/p2p/net/connmgr"
	"github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/client"
	relayproto "github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/proto"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/blobarchive/bloar/schema"
)

// TestRelayServiceFollowsObservedReachability verifies go-libp2p v0.48's
// service-manager behavior rather than assuming EnableRelayService means an
// always-on public relay. Bloar never adds ForceReachabilityPublic itself.
func TestRelayServiceFollowsObservedReachability(t *testing.T) {
	cfg := DefaultRelayConfig()
	cfg.EnableHolePunching = false
	opts, err := RelayOptions(cfg)
	if err != nil {
		t.Fatalf("relay options: %v", err)
	}
	opts = append([]libp2p.Option{
		libp2p.NoListenAddrs,
		libp2p.DisableMetrics(),
		libp2p.ForceReachabilityPrivate(),
	}, opts...)
	h, err := libp2p.New(opts...)
	if err != nil {
		t.Fatalf("building host: %v", err)
	}
	t.Cleanup(func() { closeRelayTestHost(t, h) })

	if hostSupportsRelayHop(h) {
		t.Fatal("relay service started while reachability was Private")
	}
	emitter, err := h.EventBus().Emitter(new(event.EvtLocalReachabilityChanged))
	if err != nil {
		t.Fatalf("creating reachability emitter: %v", err)
	}
	t.Cleanup(func() { _ = emitter.Close() })
	if err := emitter.Emit(event.EvtLocalReachabilityChanged{Reachability: network.ReachabilityPublic}); err != nil {
		t.Fatalf("emitting Public reachability: %v", err)
	}
	waitRelayTestCondition(t, 3*time.Second, "relay service to start on Public", func() bool {
		return hostSupportsRelayHop(h)
	})
	if err := emitter.Emit(event.EvtLocalReachabilityChanged{Reachability: network.ReachabilityUnknown}); err != nil {
		t.Fatalf("emitting Unknown reachability: %v", err)
	}
	waitRelayTestCondition(t, 3*time.Second, "relay service to stop on Unknown", func() bool {
		return !hostSupportsRelayHop(h)
	})
}

// TestRelayAutoRelayMetricsAndShutdown proves a real static reservation lands,
// all three upstream subsystems register in the caller's private registry, and
// an active AutoRelay/relay-v2/DCUtR stack closes promptly.
func TestRelayAutoRelayMetricsAndShutdown(t *testing.T) {
	registry := prometheus.NewRegistry()
	relayHost := newRelayServiceTestHost(t, registry)

	cfg := DefaultRelayConfig()
	cfg.StaticRelays = []peer.AddrInfo{{ID: relayHost.ID(), Addrs: relayHost.Addrs()}}
	opts, err := RelayOptions(cfg)
	if err != nil {
		t.Fatalf("private relay options: %v", err)
	}
	connections, err := connmgr.NewConnManager(8, 16)
	if err != nil {
		t.Fatalf("building private connection manager: %v", err)
	}
	opts = append([]libp2p.Option{
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"),
		libp2p.ForceReachabilityPrivate(),
		libp2p.PrometheusRegisterer(registry),
		libp2p.ConnectionManager(connections),
	}, opts...)
	privateHost, err := libp2p.New(opts...)
	if err != nil {
		t.Fatalf("building private host: %v", err)
	}
	privateClosed := false
	t.Cleanup(func() {
		if !privateClosed {
			closeRelayTestHost(t, privateHost)
		}
	})

	waitRelayTestCondition(t, 10*time.Second, "static AutoRelay reservation", func() bool {
		// AutoRelay protects a connection only after client.Reserve succeeds.
		// The loopback relay cannot become an advertised circuit address because
		// v0.48 correctly filters private relay addresses, so the protection tag
		// is the deterministic reservation proof in this hermetic test.
		return privateHost.ConnManager().IsProtected(relayHost.ID(), "autorelay")
	})

	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("gathering private relay metrics: %v", err)
	}
	names := make(map[string]struct{}, len(families))
	for _, family := range families {
		names[family.GetName()] = struct{}{}
	}
	for _, name := range []string{
		"libp2p_relaysvc_status",
		"libp2p_autorelay_status",
		// The direct-dial CounterVec is absent until its first observation;
		// v0.48 preinitializes the bounded outcome label set at construction.
		"libp2p_holepunch_outcomes_total",
	} {
		if _, ok := names[name]; !ok {
			t.Errorf("private registry is missing %s", name)
		}
	}

	closeRelayTestHost(t, privateHost)
	privateClosed = true
	waitRelayTestCondition(t, 3*time.Second, "AutoRelay connection to close", func() bool {
		return relayHost.Network().Connectedness(privateHost.ID()) != network.Connected
	})
}

// TestRelayLimitedConnectionIsControlPlaneNotBitswap constructs the real
// circuit-v2 path. An explicit limited-connection context can open a control
// stream, while the normal context and Boxo Bitswap cannot use that circuit.
func TestRelayLimitedConnectionIsControlPlaneNotBitswap(t *testing.T) {
	relayHost := newRelayServiceTestHost(t, nil)
	server := newRelayEndpointTestHost(t)
	requester := newRelayEndpointTestHost(t)
	relayInfo := peer.AddrInfo{ID: relayHost.ID(), Addrs: relayHost.Addrs()}

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := server.Connect(ctx, relayInfo); err != nil {
		t.Fatalf("server connecting to relay: %v", err)
	}
	if _, err := client.Reserve(ctx, server, relayInfo); err != nil {
		t.Fatalf("server reserving relay slot: %v", err)
	}
	if err := requester.Connect(ctx, relayInfo); err != nil {
		t.Fatalf("requester connecting to relay: %v", err)
	}

	circuit := ma.StringCast("/p2p/" + relayHost.ID().String() + "/p2p-circuit")
	requester.Peerstore().AddAddrs(relayHost.ID(), relayHost.Addrs(), peerstore.PermanentAddrTTL)
	if err := requester.Connect(ctx, peer.AddrInfo{ID: server.ID(), Addrs: []ma.Multiaddr{circuit}}); err != nil {
		t.Fatalf("opening relay circuit: %v", err)
	}
	connections := requester.Network().ConnsToPeer(server.ID())
	if len(connections) != 1 || !connections[0].Stat().Limited {
		t.Fatalf("requester-server connections = %+v, want one limited relay circuit", connections)
	}

	const controlProtocol protocol.ID = "/bloar/relay-control-test/1.0.0"
	server.SetStreamHandler(controlProtocol, func(stream network.Stream) { _ = stream.Close() })
	limitedCtx, limitedCancel := context.WithTimeout(
		network.WithAllowLimitedConn(t.Context(), "relay control-plane test"),
		time.Second,
	)
	stream, err := requester.NewStream(limitedCtx, server.ID(), controlProtocol)
	limitedCancel()
	if err != nil {
		t.Fatalf("explicit control-plane stream over relay: %v", err)
	}
	_ = stream.Close()

	ordinaryCtx, ordinaryCancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	stream, err = requester.NewStream(ordinaryCtx, server.ID(), controlProtocol)
	ordinaryCancel()
	if stream != nil {
		_ = stream.Close()
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("ordinary stream error = %v, want limited connection excluded", err)
	}

	serverBlocks := newAdmissionTestBlockstore()
	requesterBlocks := newAdmissionTestBlockstore()
	blob := admissionTestBlock(t, schema.BlobSize, 1)
	putAdmissionTestBlock(t, serverBlocks, blob)
	serverExchange, err := NewExchange(t.Context(), ExchangeConfig{
		Host:   &Host{h: server},
		Blocks: serverBlocks,
	})
	if err != nil {
		t.Fatalf("building server Bitswap: %v", err)
	}
	t.Cleanup(func() {
		if err := serverExchange.Close(); err != nil {
			t.Errorf("closing server Bitswap: %v", err)
		}
	})
	requesterExchange, err := NewExchange(t.Context(), ExchangeConfig{
		Host:   &Host{h: requester},
		Blocks: requesterBlocks,
	})
	if err != nil {
		t.Fatalf("building requester Bitswap: %v", err)
	}
	t.Cleanup(func() {
		if err := requesterExchange.Close(); err != nil {
			t.Errorf("closing requester Bitswap: %v", err)
		}
	})

	fetchCtx, fetchCancel := context.WithTimeout(t.Context(), 400*time.Millisecond)
	fetched, err := requesterExchange.NewSession(fetchCtx).GetBlock(fetchCtx, blob.Cid())
	fetchCancel()
	if err == nil || fetched != nil {
		t.Fatalf("Bitswap fetched a blob over a limited relay circuit: block=%v err=%v", fetched, err)
	}
	for _, connection := range requester.Network().ConnsToPeer(server.ID()) {
		for _, stream := range connection.GetStreams() {
			if strings.Contains(string(stream.Protocol()), "bitswap") {
				t.Fatalf("Bitswap stream %q opened over limited relay circuit", stream.Protocol())
			}
		}
	}
}

func newRelayServiceTestHost(t *testing.T, registry prometheus.Registerer) host.Host {
	t.Helper()
	cfg := RelayConfig{EnableService: true}
	opts, err := RelayOptions(cfg)
	if err != nil {
		t.Fatalf("relay service options: %v", err)
	}
	base := []libp2p.Option{
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"),
		libp2p.ForceReachabilityPublic(),
	}
	if registry == nil {
		base = append(base, libp2p.DisableMetrics())
	} else {
		base = append(base, libp2p.PrometheusRegisterer(registry))
	}
	h, err := libp2p.New(append(base, opts...)...)
	if err != nil {
		t.Fatalf("building relay service host: %v", err)
	}
	t.Cleanup(func() { closeRelayTestHost(t, h) })
	waitRelayTestCondition(t, 3*time.Second, "public relay-v2 service", func() bool {
		return hostSupportsRelayHop(h)
	})
	return h
}

func newRelayEndpointTestHost(t *testing.T) host.Host {
	t.Helper()
	h, err := libp2p.New(libp2p.NoListenAddrs, libp2p.EnableRelay(), libp2p.DisableMetrics())
	if err != nil {
		t.Fatalf("building relay endpoint: %v", err)
	}
	t.Cleanup(func() { closeRelayTestHost(t, h) })
	return h
}

func hostSupportsRelayHop(h host.Host) bool {
	for _, candidate := range h.Mux().Protocols() {
		if candidate == relayproto.ProtoIDv2Hop {
			return true
		}
	}
	return false
}

func waitRelayTestCondition(t *testing.T, timeout time.Duration, description string, ready func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if ready() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", description)
}

func closeRelayTestHost(t *testing.T, h host.Host) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- h.Close() }()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("closing relay test host: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Error("relay test host shutdown exceeded 3 seconds")
	}
}
