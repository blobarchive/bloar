package p2p

import (
	"errors"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
)

func TestResourceControlZeroValueResolvesPinnedDefaults(t *testing.T) {
	resolved, err := resolveResourceControls(ConnectionManagerConfig{}, ResourceManagerConfig{}, nil)
	if err != nil {
		t.Fatalf("resolving zero-value controls: %v", err)
	}
	if got, want := resolved.connections.LowWatermark, DefaultConnectionLowWatermark; got != want {
		t.Errorf("low watermark = %d, want %d", got, want)
	}
	if got, want := resolved.connections.HighWatermark, DefaultConnectionHighWatermark; got != want {
		t.Errorf("high watermark = %d, want %d", got, want)
	}
	if got, want := resolved.connections.GracePeriod, DefaultConnectionGracePeriod; got != want {
		t.Errorf("grace = %s, want %s", got, want)
	}

	limiter := resourceLimiter(resolved.resources)
	system := limiter.GetSystemLimits()
	if got, want := system.GetMemoryLimit(), DefaultResourceMemoryBytes; got != want {
		t.Errorf("memory hard limit = %d, want %d", got, want)
	}
	if got, want := system.GetFDLimit(), DefaultResourceFileDescriptors; got != want {
		t.Errorf("FD hard limit = %d, want %d", got, want)
	}
	if got, want := system.GetConnTotalLimit(), DefaultResourceConnections; got != want {
		t.Errorf("connection hard limit = %d, want %d", got, want)
	}
	if got, want := system.GetConnLimit(network.DirInbound), DefaultResourceInboundConnections; got != want {
		t.Errorf("inbound connection hard limit = %d, want %d", got, want)
	}
	if got, want := system.GetConnLimit(network.DirOutbound), DefaultResourceOutboundConnections; got != want {
		t.Errorf("outbound connection hard limit = %d, want %d", got, want)
	}
	if resolved.resources.InboundConnections < resolved.connections.HighWatermark ||
		resolved.resources.OutboundConnections < resolved.connections.HighWatermark {
		t.Errorf(
			"default directional hard limits (%d inbound/%d outbound) fall below pruning high watermark %d",
			resolved.resources.InboundConnections,
			resolved.resources.OutboundConnections,
			resolved.connections.HighWatermark,
		)
	}
	if got, want := system.GetStreamTotalLimit(), DefaultResourceStreams; got != want {
		t.Errorf("stream hard limit = %d, want %d", got, want)
	}

	perPeer := limiter.GetPeerLimits(peer.ID("arbitrary-peer"))
	if got, want := perPeer.GetConnTotalLimit(), DefaultResourcePeerConnections; got != want {
		t.Errorf("per-peer connection limit = %d, want %d", got, want)
	}
	if got, want := perPeer.GetStreamTotalLimit(), DefaultResourcePeerStreams; got != want {
		t.Errorf("per-peer stream limit = %d, want %d", got, want)
	}
	if got, want := perPeer.GetMemoryLimit(), DefaultResourcePeerMemoryBytes; got != want {
		t.Errorf("per-peer memory limit = %d, want %d", got, want)
	}
	if got, want := perPeer.GetFDLimit(), DefaultResourcePeerFileDescriptors; got != want {
		t.Errorf("per-peer FD limit = %d, want %d", got, want)
	}
}

func TestResourceControlCountsUniqueStaticPeers(t *testing.T) {
	id := peer.ID("same-static-peer")
	resolved, err := resolveResourceControls(
		ConnectionManagerConfig{LowWatermark: 2, HighWatermark: 4},
		ResourceManagerConfig{},
		[]peer.AddrInfo{{ID: id}, {ID: id}},
	)
	if err != nil {
		t.Fatalf("duplicate addresses for one static peer exhausted the budget: %v", err)
	}
	if got := len(resolved.protected); got != 1 {
		t.Fatalf("protected peer count = %d, want one unique identity", got)
	}
}

func TestResourceControlRequiresStaticPeerConnectionHeadroom(t *testing.T) {
	id := peer.ID("static-peer")
	connections := ConnectionManagerConfig{LowWatermark: 1, HighWatermark: 3}
	resources := ResourceManagerConfig{
		Connections:         DefaultResourceStaticPeerConnectionHeadroom,
		InboundConnections:  DefaultResourceStaticPeerConnectionHeadroom,
		OutboundConnections: DefaultResourceStaticPeerConnectionHeadroom,
	}
	_, err := resolveResourceControls(connections, resources, []peer.AddrInfo{{ID: id}})
	if err == nil || !strings.Contains(err.Error(), "control-plane headroom") {
		t.Fatalf("error = %v, want static-peer headroom rejection", err)
	}

	resources.Connections++
	resources.InboundConnections++
	resources.OutboundConnections++
	if _, err := resolveResourceControls(connections, resources, []peer.AddrInfo{{ID: id}}); err != nil {
		t.Fatalf("exact static-peer headroom was rejected: %v", err)
	}
}

func TestResourceControlRejectsProtectedFloorAtHighWatermark(t *testing.T) {
	id := peer.ID("static-peer")
	_, err := resolveResourceControls(
		ConnectionManagerConfig{LowWatermark: 2, HighWatermark: 3},
		ResourceManagerConfig{},
		[]peer.AddrInfo{{ID: id}},
	)
	if err == nil || !strings.Contains(err.Error(), "one pruning slot") {
		t.Fatalf("error = %v, want impossible protected-floor rejection", err)
	}
}

func TestConstructLibp2pHostFailureClosesResourceControls(t *testing.T) {
	before := resourceControlGoroutines()
	opts, _, ownership, err := resourceControlOptions(HostConfig{}, nil)
	if err != nil {
		t.Fatalf("constructing resource controls: %v", err)
	}
	during, started := waitResourceControlGoroutines(func(count int) bool { return count > before })
	if !started {
		_ = ownership.close()
		t.Fatalf("resource-control goroutines while owned = %d, want more than baseline %d", during, before)
	}

	forced := errors.New("forced option failure")
	opts = append(opts, func(*libp2p.Config) error { return forced })
	h, err := constructLibp2pHost(opts, ownership)
	if h != nil {
		_ = h.Close()
		t.Fatal("failed construction returned a host")
	}
	if !errors.Is(err, forced) {
		t.Fatalf("construction error = %v, want forced failure", err)
	}
	if ownership.connections != nil || ownership.resources != nil {
		t.Fatal("failed construction retained manager ownership")
	}
	after, stopped := waitResourceControlGoroutines(func(count int) bool { return count == before })
	if !stopped {
		t.Fatalf("resource-control goroutines after failure = %d, want baseline %d", after, before)
	}
}

func waitResourceControlGoroutines(done func(int) bool) (int, bool) {
	deadline := time.Now().Add(time.Second)
	for {
		count := resourceControlGoroutines()
		if done(count) {
			return count, true
		}
		if time.Now().After(deadline) {
			return count, false
		}
		time.Sleep(time.Millisecond)
	}
}

func resourceControlGoroutines() int {
	buf := make([]byte, 1<<20)
	n := runtime.Stack(buf, true)
	stacks := string(buf[:n])
	return strings.Count(stacks, "connmgr.(*BasicConnMgr).background") +
		strings.Count(stacks, "connmgr.(*decayer).process") +
		strings.Count(stacks, "resource-manager.(*resourceManager).background")
}
