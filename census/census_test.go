package census

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	mh "github.com/multiformats/go-multihash"
)

type fakeFinder struct {
	providers []peer.AddrInfo
	stream    <-chan peer.AddrInfo
	limit     atomic.Int64
	err       error
}

func (finder *fakeFinder) FindProviders(_ context.Context, _ cid.Cid, limit int) (<-chan peer.AddrInfo, error) {
	finder.limit.Store(int64(limit))
	if finder.err != nil {
		return nil, finder.err
	}
	if finder.stream != nil {
		return finder.stream, nil
	}
	stream := make(chan peer.AddrInfo, len(finder.providers))
	for _, provider := range finder.providers {
		stream <- provider
	}
	close(stream)
	return stream, nil
}

type fakeProbe struct {
	result ProbeResult
	err    error
}

type fakeProber struct {
	probes       map[peer.ID]fakeProbe
	defaultProbe fakeProbe
	delay        time.Duration

	active    atomic.Int64
	maxActive atomic.Int64
	calls     atomic.Int64
}

func (prober *fakeProber) Probe(ctx context.Context, provider peer.AddrInfo, challenges ChallengeSet) (ProbeResult, error) {
	prober.calls.Add(1)
	active := prober.active.Add(1)
	defer prober.active.Add(-1)
	for {
		maximum := prober.maxActive.Load()
		if active <= maximum || prober.maxActive.CompareAndSwap(maximum, active) {
			break
		}
	}
	if prober.delay > 0 {
		timer := time.NewTimer(prober.delay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return ProbeResult{Historical: make([]bool, len(challenges.Historical))}, ctx.Err()
		case <-timer.C:
		}
	}
	probe, ok := prober.probes[provider.ID]
	if !ok {
		probe = prober.defaultProbe
	}
	probe.result.Historical = append([]bool(nil), probe.result.Historical...)
	return probe.result, probe.err
}

func TestInspectorClassifiesPeerTargetedProofs(t *testing.T) {
	current := testCID(t, "current")
	history := []cid.Cid{testCID(t, "old-one"), testCID(t, "old-two")}
	addressOne := testMultiaddr(t, "/ip4/192.0.2.1/tcp/4001")
	addressTwo := testMultiaddr(t, "/ip4/192.0.2.2/tcp/4001")
	finder := &fakeFinder{providers: []peer.AddrInfo{
		{ID: testPeerID(t, "full"), Addrs: []ma.Multiaddr{addressOne}},
		{ID: testPeerID(t, "current"), Addrs: []ma.Multiaddr{addressOne}},
		{ID: testPeerID(t, "full"), Addrs: []ma.Multiaddr{addressOne, addressTwo}}, // duplicate provider and address
		{ID: testPeerID(t, "stale")},
		{ID: testPeerID(t, "unreachable")},
	}}
	prober := &fakeProber{probes: map[peer.ID]fakeProbe{
		testPeerID(t, "full"):        {result: ProbeResult{Reachable: true, Current: true, Historical: []bool{true, true}, Path: PathDirect, DialLatency: 12 * time.Millisecond, ProbeLatency: 34 * time.Millisecond}},
		testPeerID(t, "current"):     {result: ProbeResult{Reachable: true, Current: true, Historical: []bool{true, false}, Path: PathRelay, DialLatency: 56 * time.Millisecond, ProbeLatency: 78 * time.Millisecond}},
		testPeerID(t, "stale"):       {result: ProbeResult{Reachable: true, Current: false, Historical: []bool{true, true}}},
		testPeerID(t, "unreachable"): {result: ProbeResult{Historical: []bool{false, false}}},
	}}
	fixedTime := time.Date(2026, time.July, 22, 5, 0, 0, 0, time.UTC)
	inspector, err := New(Config{
		Rendezvous:   testCID(t, "rendezvous"),
		Current:      current,
		Historical:   history,
		Finder:       finder,
		Prober:       prober,
		IncludePeers: true,
		Now:          func() time.Time { return fixedTime },
	})
	if err != nil {
		t.Fatal(err)
	}

	report := inspector.Inspect(context.Background())
	if !report.Complete || report.Truncated || report.ErrorCount != 0 {
		t.Fatalf("unexpected report state: %+v", report)
	}
	if report.ObservedAt != fixedTime || report.CompletedAt != fixedTime {
		t.Fatalf("timestamps = %s, %s, want %s", report.ObservedAt, report.CompletedAt, fixedTime)
	}
	wantBounds := (LowerBounds{Observed: 4, Reachable: 3, Current: 2, SampledArchive: 1})
	if report.LowerBounds != wantBounds {
		t.Fatalf("lower bounds = %+v, want %+v", report.LowerBounds, wantBounds)
	}
	if report.ProbeAttempts != 4 || report.ProbeCompleted != 4 {
		t.Fatalf("probe counts = %d/%d, want 4/4", report.ProbeAttempts, report.ProbeCompleted)
	}
	if finder.limit.Load() != DefaultMaxProviders {
		t.Fatalf("finder limit = %d, want %d", finder.limit.Load(), DefaultMaxProviders)
	}
	states := make(map[PeerState]int)
	for _, provider := range report.Peers {
		states[provider.State]++
		if provider.PeerID == testPeerID(t, "full").String() && len(provider.Addresses) != 2 {
			t.Fatalf("deduplicated full-provider addresses = %v, want two", provider.Addresses)
		}
		if provider.PeerID == testPeerID(t, "full").String() && (provider.Path != PathDirect || provider.DialLatencyMS != 12 || provider.ProbeLatencyMS != 34) {
			t.Fatalf("full-provider path/latency = %s/%d/%d", provider.Path, provider.DialLatencyMS, provider.ProbeLatencyMS)
		}
		if provider.PeerID == testPeerID(t, "current").String() && provider.Path != PathRelay {
			t.Fatalf("current-provider path = %s, want relay", provider.Path)
		}
	}
	for state, want := range map[PeerState]int{
		PeerSampledArchive: 1, PeerCurrentOnly: 1, PeerStale: 1, PeerUnreachable: 1,
	} {
		if states[state] != want {
			t.Fatalf("state %q count = %d, want %d (all=%v)", state, states[state], want, states)
		}
	}
}

func TestInspectorEnforcesProviderAddressAndConcurrencyBounds(t *testing.T) {
	addressOne := testMultiaddr(t, "/ip4/192.0.2.1/tcp/4001")
	addressTwo := testMultiaddr(t, "/ip4/192.0.2.2/tcp/4001")
	providers := make([]peer.AddrInfo, 0, 8)
	for index := range 8 {
		providers = append(providers, peer.AddrInfo{
			ID:    testPeerID(t, string(rune('a'+index))),
			Addrs: []ma.Multiaddr{addressOne, addressTwo},
		})
	}
	finder := &fakeFinder{providers: providers}
	prober := &fakeProber{
		defaultProbe: fakeProbe{result: ProbeResult{Reachable: true, Current: true, Historical: []bool{true}}},
		delay:        5 * time.Millisecond,
	}
	inspector, err := New(Config{
		Rendezvous:   testCID(t, "rendezvous"),
		Current:      testCID(t, "current"),
		Historical:   []cid.Cid{testCID(t, "old")},
		Finder:       finder,
		Prober:       prober,
		IncludePeers: true,
		Limits: Limits{
			MaxProviders:     6,
			MaxAddressBytes:  len(addressOne.Bytes()),
			Concurrency:      2,
			MaxHistorical:    1,
			OverallTimeout:   time.Second,
			DiscoveryTimeout: time.Second,
			ProbeTimeout:     time.Second,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	report := inspector.Inspect(context.Background())
	if report.LowerBounds.Observed != 6 || prober.calls.Load() != 6 {
		t.Fatalf("observed/probed = %d/%d, want 6/6", report.LowerBounds.Observed, prober.calls.Load())
	}
	if !report.Truncated {
		t.Fatal("report did not disclose bounded provider/address truncation")
	}
	if report.AddressBytesAccepted != len(addressOne.Bytes()) {
		t.Fatalf("address bytes = %d, want %d", report.AddressBytesAccepted, len(addressOne.Bytes()))
	}
	if prober.maxActive.Load() > 2 {
		t.Fatalf("maximum probe concurrency = %d, limit 2", prober.maxActive.Load())
	}
	acceptedAddresses := 0
	for _, provider := range report.Peers {
		acceptedAddresses += len(provider.Addresses)
	}
	if acceptedAddresses != 1 {
		t.Fatalf("accepted addresses = %d, want one", acceptedAddresses)
	}
}

func TestInspectorDiscoveryTimeoutReturnsTimestampedPartialReport(t *testing.T) {
	never := make(chan peer.AddrInfo)
	inspector, err := New(Config{
		Rendezvous: testCID(t, "rendezvous"),
		Current:    testCID(t, "current"),
		Historical: []cid.Cid{testCID(t, "old")},
		Finder:     &fakeFinder{stream: never},
		Prober:     &fakeProber{},
		Limits: Limits{
			OverallTimeout:   100 * time.Millisecond,
			DiscoveryTimeout: 5 * time.Millisecond,
			ProbeTimeout:     50 * time.Millisecond,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	report := inspector.Inspect(context.Background())
	if time.Since(started) > 500*time.Millisecond {
		t.Fatal("discovery did not honor its context deadline")
	}
	if report.Complete || !report.TimedOut || report.DiscoveryComplete {
		t.Fatalf("timeout report = %+v", report)
	}
	if report.ObservedAt.IsZero() || report.CompletedAt.IsZero() {
		t.Fatalf("partial report lacks timestamps: %+v", report)
	}
}

func TestInspectorFailsArchiveClosedOnMalformedProbeResult(t *testing.T) {
	finder := &fakeFinder{providers: []peer.AddrInfo{{ID: testPeerID(t, "peer")}}}
	prober := &fakeProber{defaultProbe: fakeProbe{result: ProbeResult{Reachable: true, Current: true, Historical: []bool{true}}}}
	inspector, err := New(Config{
		Rendezvous:   testCID(t, "rendezvous"),
		Current:      testCID(t, "current"),
		Historical:   []cid.Cid{testCID(t, "old-one"), testCID(t, "old-two")},
		Finder:       finder,
		Prober:       prober,
		IncludePeers: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	report := inspector.Inspect(context.Background())
	if report.LowerBounds.SampledArchive != 0 || report.ErrorCount != 1 {
		t.Fatalf("malformed result was not failed closed: %+v", report)
	}
	if len(report.Peers) != 1 || report.Peers[0].State != PeerCurrentOnly || report.Peers[0].ProbeError == "" {
		t.Fatalf("raw malformed result = %+v", report.Peers)
	}
}

func TestInspectorFailsInvalidPathAndLatencyClosed(t *testing.T) {
	finder := &fakeFinder{providers: []peer.AddrInfo{{ID: testPeerID(t, "peer")}}}
	prober := &fakeProber{defaultProbe: fakeProbe{result: ProbeResult{
		Reachable: true, Current: true, Historical: []bool{true},
		Path: ConnectionPath("invented"), DialLatency: -time.Second,
	}}}
	inspector, err := New(Config{
		Rendezvous: testCID(t, "rendezvous"),
		Current:    testCID(t, "current"),
		Historical: []cid.Cid{testCID(t, "old")},
		Finder:     finder, Prober: prober, IncludePeers: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	report := inspector.Inspect(context.Background())
	if report.LowerBounds.SampledArchive != 0 || report.ErrorCount != 1 {
		t.Fatalf("invalid path/latency was not failed closed: %+v", report)
	}
	if got := report.Peers[0]; got.Path != PathUnknown || got.DialLatencyMS != 0 || got.State != PeerCurrentOnly {
		t.Fatalf("normalized peer result = %+v", got)
	}
}

func TestNewRejectsDuplicateAndUnboundedChallenges(t *testing.T) {
	base := Config{
		Rendezvous: testCID(t, "rendezvous"),
		Current:    testCID(t, "current"),
		Finder:     &fakeFinder{},
		Prober:     &fakeProber{},
	}
	base.Historical = []cid.Cid{base.Current}
	if _, err := New(base); err == nil {
		t.Fatal("duplicate current/historical challenge accepted")
	}
	base.Historical = []cid.Cid{testCID(t, "one"), testCID(t, "two")}
	base.Limits.MaxHistorical = 1
	if _, err := New(base); err == nil {
		t.Fatal("historical challenge set over configured bound accepted")
	}
	base.Limits = Limits{MaxProviders: hardMaxProviders + 1}
	if _, err := New(base); err == nil {
		t.Fatal("provider bound above hard maximum accepted")
	}
}

func TestInspectorRecordsFinderFailureWithoutRawDetails(t *testing.T) {
	inspector, err := New(Config{
		Rendezvous: testCID(t, "rendezvous"),
		Current:    testCID(t, "current"),
		Historical: []cid.Cid{testCID(t, "old")},
		Finder:     &fakeFinder{err: errors.New("provider router unavailable")},
		Prober:     &fakeProber{},
	})
	if err != nil {
		t.Fatal(err)
	}
	report := inspector.Inspect(context.Background())
	if !report.DiscoveryFailed || report.ErrorCount != 1 || report.Complete {
		t.Fatalf("finder failure report = %+v", report)
	}
	if report.Peers != nil {
		t.Fatalf("aggregate report contains raw peers: %+v", report.Peers)
	}
}

func testCID(t *testing.T, value string) cid.Cid {
	t.Helper()
	digest, err := mh.Sum([]byte(value), mh.SHA2_256, -1)
	if err != nil {
		t.Fatal(err)
	}
	return cid.NewCidV1(cid.Raw, digest)
}

func testPeerID(t *testing.T, value string) peer.ID {
	t.Helper()
	digest, err := mh.Sum([]byte(value), mh.SHA2_256, -1)
	if err != nil {
		t.Fatal(err)
	}
	id, err := peer.IDFromBytes(digest)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func testMultiaddr(t *testing.T, value string) ma.Multiaddr {
	t.Helper()
	address, err := ma.NewMultiaddr(value)
	if err != nil {
		t.Fatal(err)
	}
	return address
}
