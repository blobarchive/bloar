package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/blobarchive/bloar/census"
	"github.com/blobarchive/bloar/kubo"
	"github.com/blobarchive/bloar/p2p"
)

func TestTargetProberMapsOnlyExactTargetResults(t *testing.T) {
	current := commandTestCID(t, "current-target")
	historical := []cid.Cid{commandTestCID(t, "old-a"), commandTestCID(t, "old-b")}
	provider := peer.AddrInfo{ID: peer.ID("target")}
	prober := &targetProber{maxBytes: 1024, probe: func(_ context.Context, got peer.AddrInfo, targets []cid.Cid, limits p2p.ProbeLimits) (p2p.PeerProbe, error) {
		if got.ID != provider.ID || limits.MaxCIDs != 3 || limits.MaxBytes != 1024 {
			t.Fatalf("probe args = %+v, %+v", got, limits)
		}
		return p2p.PeerProbe{
			Peer: got.ID, Reachable: true, Path: p2p.ProbePathRelay, DialLatency: 5 * time.Millisecond,
			Blocks: []p2p.BlockProbe{
				{CID: targets[0], Success: true},
				{CID: targets[1], Success: true},
				{CID: targets[2], Success: true},
			},
		}, nil
	}}
	result, err := prober.Probe(t.Context(), provider, census.ChallengeSet{Current: current, Historical: historical})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Reachable || !result.Current || len(result.Historical) != 2 || !result.Historical[0] || !result.Historical[1] {
		t.Fatalf("result = %+v", result)
	}
	if result.Path != census.PathRelay || result.DialLatency != 5*time.Millisecond || result.ProbeLatency < 0 {
		t.Fatalf("path/latency = %+v", result)
	}
}

func TestKuboTransportRejectsUnsupportedProviderBoundBeforePreflight(t *testing.T) {
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		requests.Add(1)
	}))
	defer server.Close()
	factory := &kuboTransportFactory{
		api: server.URL, allowUnauthenticated: true,
		requestTimeout: time.Second, maxProbeBytes: 1 << 20,
	}
	_, err := factory.Open(t.Context(), census.Limits{
		MaxProviders: kubo.MaximumFindProviders + 1, MaxAddressBytes: census.DefaultMaxAddressBytes,
	})
	if err == nil {
		t.Fatal("unsupported Kubo provider bound accepted")
	}
	if got := requests.Load(); got != 0 {
		t.Fatalf("invalid local bound triggered %d Kubo requests", got)
	}
}

func TestTargetProberFailsArchiveProofClosedOnMissingOrWrongResults(t *testing.T) {
	current := commandTestCID(t, "current")
	old := commandTestCID(t, "old")
	wrong := commandTestCID(t, "wrong")
	prober := &targetProber{maxBytes: 1024, probe: func(_ context.Context, _ peer.AddrInfo, _ []cid.Cid, _ p2p.ProbeLimits) (p2p.PeerProbe, error) {
		return p2p.PeerProbe{
			Reachable: true, Path: p2p.ProbePathDirect,
			Blocks: []p2p.BlockProbe{{CID: wrong, Success: true}},
		}, nil
	}}
	result, err := prober.Probe(t.Context(), peer.AddrInfo{ID: peer.ID("target")}, census.ChallengeSet{
		Current: current, Historical: []cid.Cid{old},
	})
	if err == nil || result.Current || result.Historical[0] {
		t.Fatalf("malformed target proof = %+v, %v", result, err)
	}

	prober.probe = func(_ context.Context, _ peer.AddrInfo, _ []cid.Cid, _ p2p.ProbeLimits) (p2p.PeerProbe, error) {
		return p2p.PeerProbe{}, errors.New("validation failed")
	}
	if _, err := prober.Probe(t.Context(), peer.AddrInfo{}, census.ChallengeSet{Current: current, Historical: []cid.Cid{old}}); err == nil {
		t.Fatal("transport validation failure was hidden")
	}
}

func TestTargetProberRejectsMismatchedPeerAttribution(t *testing.T) {
	current := commandTestCID(t, "current")
	target := commandTestPeerID(t, "target")
	other := commandTestPeerID(t, "other")
	prober := &targetProber{maxBytes: 1024, probe: func(_ context.Context, _ peer.AddrInfo, targets []cid.Cid, _ p2p.ProbeLimits) (p2p.PeerProbe, error) {
		return p2p.PeerProbe{
			Peer: other, Reachable: true,
			Blocks: []p2p.BlockProbe{{CID: targets[0], Success: true}},
		}, nil
	}}
	result, err := prober.Probe(t.Context(), peer.AddrInfo{ID: target}, census.ChallengeSet{
		Current: current, Historical: []cid.Cid{commandTestCID(t, "old")},
	})
	if err == nil || result.Current {
		t.Fatalf("mismatched peer attribution = %+v, %v", result, err)
	}
}
