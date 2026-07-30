package p2p

import (
	"context"
	"crypto/rand"
	"errors"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	ci "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/event"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"

	bmetrics "github.com/blobarchive/bloar/metrics"
)

type fakeRendezvousRouter struct {
	mu sync.Mutex

	provided []rendezvousProvideCall
	queries  []rendezvousQueryCall
	provide  func(context.Context, cid.Cid, bool) error
	find     func(context.Context, cid.Cid, int) <-chan peer.AddrInfo
}

type rendezvousProvideCall struct {
	key      cid.Cid
	announce bool
}

type rendezvousQueryCall struct {
	key   cid.Cid
	count int
}

func (r *fakeRendezvousRouter) Provide(ctx context.Context, key cid.Cid, announce bool) error {
	r.mu.Lock()
	r.provided = append(r.provided, rendezvousProvideCall{key: key, announce: announce})
	r.mu.Unlock()
	if r.provide != nil {
		return r.provide(ctx, key, announce)
	}
	return nil
}

func (r *fakeRendezvousRouter) FindProvidersAsync(ctx context.Context, key cid.Cid, count int) <-chan peer.AddrInfo {
	r.mu.Lock()
	r.queries = append(r.queries, rendezvousQueryCall{key: key, count: count})
	r.mu.Unlock()
	if r.find != nil {
		return r.find(ctx, key, count)
	}
	ch := make(chan peer.AddrInfo)
	close(ch)
	return ch
}

func (r *fakeRendezvousRouter) snapshot() ([]rendezvousProvideCall, []rendezvousQueryCall) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]rendezvousProvideCall(nil), r.provided...), append([]rendezvousQueryCall(nil), r.queries...)
}

type fakeRendezvousHost struct {
	self peer.ID

	mu          sync.Mutex
	connected   map[peer.ID]bool
	fail        map[peer.ID]error
	attempts    []peer.AddrInfo
	marked      []peer.ID
	inFlight    int
	maxInFlight int
	release     <-chan struct{}
	waitForCtx  bool
	sawDeadline bool
}

func (h *fakeRendezvousHost) ID() peer.ID { return h.self }

func (h *fakeRendezvousHost) Connectedness(id peer.ID) network.Connectedness {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.connected[id] {
		return network.Connected
	}
	return network.NotConnected
}

func (h *fakeRendezvousHost) Connect(ctx context.Context, ai peer.AddrInfo) error {
	h.mu.Lock()
	h.attempts = append(h.attempts, ai)
	h.inFlight++
	if h.inFlight > h.maxInFlight {
		h.maxInFlight = h.inFlight
	}
	if _, ok := ctx.Deadline(); ok {
		h.sawDeadline = true
	}
	err := h.fail[ai.ID]
	h.mu.Unlock()

	switch {
	case h.waitForCtx:
		<-ctx.Done()
		err = ctx.Err()
	case h.release != nil:
		select {
		case <-ctx.Done():
			err = ctx.Err()
		case <-h.release:
		}
	}

	h.mu.Lock()
	h.inFlight--
	if err == nil {
		if h.connected == nil {
			h.connected = make(map[peer.ID]bool)
		}
		h.connected[ai.ID] = true
	}
	h.mu.Unlock()
	return err
}

func (h *fakeRendezvousHost) MarkRendezvousPeer(id peer.ID) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.connected[id] {
		return false
	}
	h.marked = append(h.marked, id)
	return true
}

func (h *fakeRendezvousHost) snapshot() ([]peer.AddrInfo, int, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]peer.AddrInfo(nil), h.attempts...), h.maxInFlight, h.sawDeadline
}

func (h *fakeRendezvousHost) markedSnapshot() []peer.ID {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]peer.ID(nil), h.marked...)
}

type rendezvousTestConfig struct {
	now       func() time.Time
	wait      func(context.Context, time.Duration) bool
	jitter    func(time.Duration) time.Duration
	host      rendezvousHost
	roundDone func(rendezvousRoundStats)
	mx        *bmetrics.Metrics
}

func newTestRendezvousService(ctx context.Context, cfg RendezvousConfig, test rendezvousTestConfig) (*RendezvousService, error) {
	return newRendezvousService(ctx, cfg, rendezvousDeps(test))
}

func TestRendezvousServiceRecordsBoundedRoundMetrics(t *testing.T) {
	mx := bmetrics.New()
	clock := newManualRendezvousClock()
	failed, err := RendezvousCID("mainnet", "all")
	if err != nil {
		t.Fatal(err)
	}
	candidate := rendezvousPeerID(t)
	router := &fakeRendezvousRouter{
		provide: func(_ context.Context, key cid.Cid, _ bool) error {
			if key == failed {
				return errors.New("injected provide failure")
			}
			return nil
		},
		find: func(ctx context.Context, _ cid.Cid, _ int) <-chan peer.AddrInfo {
			ch := make(chan peer.AddrInfo, 1)
			ch <- peer.AddrInfo{ID: candidate}
			close(ch)
			return ch
		},
	}
	rounds := make(chan rendezvousRoundStats, 1)
	host := &fakeRendezvousHost{
		self:      rendezvousPeerID(t),
		connected: map[peer.ID]bool{candidate: true},
	}
	s, err := newTestRendezvousService(t.Context(), RendezvousConfig{
		Router: router,
		Targets: []RendezvousTarget{
			{Network: "mainnet", Head: "all"},
			{Network: "mainnet", Head: "arbitrum-one"},
		},
	}, rendezvousTestConfig{
		host:      host,
		now:       clock.Now,
		wait:      waitUntilCancelled,
		roundDone: func(stats rendezvousRoundStats) { rounds <- stats },
		mx:        mx,
	})
	if err != nil {
		t.Fatalf("building rendezvous service: %v", err)
	}

	stats := receiveRound(t, rounds)
	if stats.provided != 1 || stats.provideError != 1 || stats.results != 2 || stats.available != 1 {
		t.Fatalf("round stats = %+v, want one provide success, one error, two samples, one available peer", stats)
	}
	for series, want := range map[string]float64{
		`bloar_rendezvous_active{operation="provide"}`:                 1,
		`bloar_rendezvous_active{operation="discover"}`:                1,
		`bloar_rendezvous_provides_total{outcome="ok"}`:                1,
		`bloar_rendezvous_provides_total{outcome="error"}`:             1,
		`bloar_rendezvous_discovery_rounds_total{outcome="available"}`: 1,
		`bloar_rendezvous_observed_provider_samples`:                   2,
		`bloar_rendezvous_provide_last_success_timestamp_seconds`:      float64(clock.Now().Unix()),
	} {
		if got := rendezvousMetricSample(t, mx, series); got != want {
			t.Errorf("%s = %g, want %g", series, got, want)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatalf("closing rendezvous service: %v", err)
	}
	for _, operation := range []string{"provide", "discover"} {
		series := `bloar_rendezvous_active{operation="` + operation + `"}`
		if got := rendezvousMetricSample(t, mx, series); got != 0 {
			t.Errorf("closed %s = %g, want 0", series, got)
		}
	}
}

func TestRendezvousServiceProvidesOnlySyntheticKeysAtStartup(t *testing.T) {
	t.Parallel()
	router := &fakeRendezvousRouter{}
	roundDone := make(chan rendezvousRoundStats, 1)
	host := &fakeRendezvousHost{self: rendezvousPeerID(t)}

	s, err := newTestRendezvousService(t.Context(), RendezvousConfig{
		Router: router,
		Targets: []RendezvousTarget{
			{Network: "mainnet", Head: "all"},
			{Network: "mainnet", Head: "all"}, // Config duplicates coalesce.
			{Network: "mainnet", Head: "arbitrum-one"},
		},
	}, rendezvousTestConfig{
		host:      host,
		wait:      waitUntilCancelled,
		roundDone: func(stats rendezvousRoundStats) { roundDone <- stats },
	})
	if err != nil {
		t.Fatalf("building rendezvous service: %v", err)
	}
	defer s.Close()

	stats := receiveRound(t, roundDone)
	if stats.provided != 2 || stats.provideError != 0 {
		t.Fatalf("provide stats = %+v, want two successful synthetic keys", stats)
	}
	provided, queries := router.snapshot()
	if len(provided) != 2 || len(queries) != 2 {
		t.Fatalf("calls = %d provides, %d queries; want two of each", len(provided), len(queries))
	}
	for i, target := range []RendezvousTarget{{"mainnet", "all"}, {"mainnet", "arbitrum-one"}} {
		want, err := RendezvousCID(target.Network, target.Head)
		if err != nil {
			t.Fatal(err)
		}
		if provided[i].key != want || !provided[i].announce {
			t.Errorf("provide[%d] = %+v, want announced rendezvous key %s", i, provided[i], want)
		}
		if queries[i].key != want {
			t.Errorf("query[%d] key = %s, want %s", i, queries[i].key, want)
		}
	}
}

func TestRendezvousServiceConstructionDoesNotWaitForNetwork(t *testing.T) {
	t.Parallel()
	mx := bmetrics.New()
	started := make(chan struct{}, 2)
	router := &fakeRendezvousRouter{
		provide: func(ctx context.Context, _ cid.Cid, _ bool) error {
			started <- struct{}{}
			<-ctx.Done()
			return ctx.Err()
		},
		find: func(ctx context.Context, _ cid.Cid, _ int) <-chan peer.AddrInfo {
			started <- struct{}{}
			<-ctx.Done()
			ch := make(chan peer.AddrInfo)
			close(ch)
			return ch
		},
	}

	type buildResult struct {
		service *RendezvousService
		err     error
	}
	built := make(chan buildResult, 1)
	cfg := RendezvousConfig{
		Router:  router,
		Targets: []RendezvousTarget{{Network: "mainnet", Head: "all"}},
	}
	testCfg := rendezvousTestConfig{host: &fakeRendezvousHost{self: rendezvousPeerID(t)}, mx: mx}
	go func() {
		service, err := newTestRendezvousService(t.Context(), cfg, testCfg)
		built <- buildResult{service: service, err: err}
	}()
	var result buildResult
	select {
	case result = <-built:
	case <-time.After(time.Second):
		t.Fatal("constructor blocked on network work")
	}
	if result.err != nil {
		t.Fatalf("building rendezvous service: %v", result.err)
	}
	s := result.service
	if s == nil {
		t.Fatal("constructor returned a nil service")
	}
	for range 2 {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("startup network operations did not begin asynchronously")
		}
	}

	closed := make(chan struct{})
	go func() {
		_ = s.Close()
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("Close did not cancel bounded startup network work")
	}
	if got := rendezvousMetricSample(t, mx, `bloar_rendezvous_provides_total{outcome="error"}`); got != 0 {
		t.Errorf("shutdown cancellation recorded %g rendezvous provide errors, want 0", got)
	}
}

func TestRendezvousServiceRefreshWakesARegularInterval(t *testing.T) {
	t.Parallel()
	router := &fakeRendezvousRouter{}
	rounds := make(chan rendezvousRoundStats, 2)
	s, err := newTestRendezvousService(t.Context(), RendezvousConfig{
		Router:             router,
		Targets:            []RendezvousTarget{{Network: "mainnet", Head: "all"}},
		DisableProviding:   true,
		Interval:           time.Hour,
		Jitter:             time.Minute,
		MaxProviderResults: 1,
	}, rendezvousTestConfig{
		host:      &fakeRendezvousHost{self: rendezvousPeerID(t)},
		roundDone: func(stats rendezvousRoundStats) { rounds <- stats },
	})
	if err != nil {
		t.Fatalf("building rendezvous service: %v", err)
	}
	defer s.Close()
	receiveRound(t, rounds)

	s.Refresh()
	receiveRound(t, rounds)
	_, queries := router.snapshot()
	if len(queries) != 2 {
		t.Fatalf("queries after coalesced refresh = %d, want startup plus immediate refresh", len(queries))
	}
}

func TestCanonicalMultiaddrsSortsAndDeduplicates(t *testing.T) {
	t.Parallel()
	a := ma.StringCast("/ip4/127.0.0.1/tcp/1")
	b := ma.StringCast("/ip4/127.0.0.1/udp/1/quic-v1")
	got := canonicalMultiaddrs([]ma.Multiaddr{b, nil, a, b})
	want := canonicalMultiaddrs([]ma.Multiaddr{a, b})
	if !slices.Equal(got, want) {
		t.Fatalf("canonical addresses = %v, want %v", got, want)
	}
}

type fakeAddressSubscription struct {
	out chan any
}

func (s *fakeAddressSubscription) Out() <-chan any { return s.out }
func (s *fakeAddressSubscription) Name() string    { return "fake address changes" }
func (s *fakeAddressSubscription) Close() error {
	close(s.out)
	return nil
}

func TestRendezvousAddressWatcherIgnoresReplayAndRefreshesChange(t *testing.T) {
	t.Parallel()
	a := ma.StringCast("/ip4/127.0.0.1/tcp/1")
	b := ma.StringCast("/ip4/127.0.0.1/tcp/2")
	ctx, cancel := context.WithCancel(t.Context())
	sub := &fakeAddressSubscription{out: make(chan any, 2)}
	s := &RendezvousService{
		ctx:        ctx,
		cancel:     cancel,
		refresh:    make(chan struct{}, 1),
		addressSub: sub,
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.watchAddressChanges(canonicalMultiaddrs([]ma.Multiaddr{a}))
	}()

	sub.out <- event.EvtLocalAddressesUpdated{
		Current: []event.UpdatedAddress{{Address: a, Action: event.Added}},
	}
	select {
	case <-s.refresh:
		t.Fatal("stateful address replay triggered a redundant refresh")
	case <-time.After(10 * time.Millisecond):
	}
	sub.out <- event.EvtLocalAddressesUpdated{
		Current: []event.UpdatedAddress{{Address: b, Action: event.Added}},
	}
	select {
	case <-s.refresh:
	case <-time.After(time.Second):
		t.Fatal("changed local address did not trigger rendezvous refresh")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("address watcher did not stop on cancellation")
	}
}

func TestRendezvousServiceOptOutStillDiscoversAndRetriesWithJitter(t *testing.T) {
	t.Parallel()
	router := &fakeRendezvousRouter{}
	roundDone := make(chan rendezvousRoundStats, 2)
	waiting := make(chan time.Duration, 2)
	release := make(chan struct{}, 1)
	clock := newManualRendezvousClock()
	retry := time.Minute

	s, err := newTestRendezvousService(t.Context(), RendezvousConfig{
		Router:             router,
		Targets:            []RendezvousTarget{{Network: "mainnet", Head: "all"}},
		DisableProviding:   true,
		Interval:           6 * time.Hour,
		Jitter:             time.Hour,
		MaxProviderResults: 3,
	}, rendezvousTestConfig{
		host: &fakeRendezvousHost{self: rendezvousPeerID(t)},
		now:  clock.Now,
		jitter: func(bound time.Duration) time.Duration {
			return bound / 2
		},
		wait: func(ctx context.Context, delay time.Duration) bool {
			select {
			case waiting <- delay:
			case <-ctx.Done():
				return false
			}
			select {
			case <-release:
				clock.Advance(delay)
				return true
			case <-ctx.Done():
				return false
			}
		},
		roundDone: func(stats rendezvousRoundStats) { roundDone <- stats },
	})
	if err != nil {
		t.Fatalf("building rendezvous service: %v", err)
	}
	defer s.Close()

	first := receiveRound(t, roundDone)
	if first.results != 0 {
		t.Fatalf("empty first round stats = %+v", first)
	}
	select {
	case got := <-waiting:
		want := retry + (retry/defaultRendezvousRetryJitterDiv)/2
		if got != want {
			t.Fatalf("empty-round retry delay = %s, want %s", got, want)
		}
	case <-time.After(time.Second):
		t.Fatal("service did not schedule its next round")
	}
	release <- struct{}{}
	receiveRound(t, roundDone)

	provided, queries := router.snapshot()
	if len(provided) != 0 {
		t.Fatalf("DisableProviding made %d provide calls", len(provided))
	}
	if len(queries) != 2 {
		t.Fatalf("no-provider startup round was queried %d times after one retry, want 2", len(queries))
	}
	for _, query := range queries {
		if query.count != 3 {
			t.Errorf("FindProvidersAsync count = %d, want configured bound 3", query.count)
		}
	}
}

func TestRendezvousDiscoveryRetriesDoNotAccelerateProviding(t *testing.T) {
	t.Parallel()
	router := &fakeRendezvousRouter{}
	roundDone := make(chan rendezvousRoundStats, 5)
	waiting := make(chan time.Duration, 5)
	release := make(chan struct{}, 1)
	clock := newManualRendezvousClock()

	s, err := newTestRendezvousService(t.Context(), RendezvousConfig{
		Router:            router,
		Targets:           []RendezvousTarget{{Network: "mainnet", Head: "all"}},
		Interval:          10 * time.Minute,
		Jitter:            time.Minute,
		DiscoveryRetryMin: time.Minute,
		DiscoveryRetryMax: 4 * time.Minute,
	}, rendezvousTestConfig{
		host:   &fakeRendezvousHost{self: rendezvousPeerID(t)},
		now:    clock.Now,
		jitter: func(time.Duration) time.Duration { return 0 },
		wait: func(ctx context.Context, delay time.Duration) bool {
			select {
			case waiting <- delay:
			case <-ctx.Done():
				return false
			}
			select {
			case <-release:
				clock.Advance(delay)
				return true
			case <-ctx.Done():
				return false
			}
		},
		roundDone: func(stats rendezvousRoundStats) { roundDone <- stats },
	})
	if err != nil {
		t.Fatalf("building rendezvous service: %v", err)
	}
	defer s.Close()

	initial := receiveRound(t, roundDone)
	if initial.provided != 1 {
		t.Fatalf("startup stats = %+v, want one provide", initial)
	}

	// Empty discovery retries at t+1m, t+3m, and t+7m. The independent
	// provider deadline remains t+10m, so none of those retries re-advertise.
	for i, delay := range []time.Duration{time.Minute, 2 * time.Minute, 4 * time.Minute} {
		select {
		case got := <-waiting:
			if got != delay {
				t.Fatalf("wait[%d] = %s, want %s", i, got, delay)
			}
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for scheduled retry %d", i)
		}
		release <- struct{}{}
		stats := receiveRound(t, roundDone)
		if stats.provided != 0 {
			t.Fatalf("discovery retry %d also provided: %+v", i, stats)
		}
	}
	provided, queries := router.snapshot()
	if len(provided) != 1 || len(queries) != 4 {
		t.Fatalf("through t+7m calls = %d provides, %d queries; want 1 and 4", len(provided), len(queries))
	}

	select {
	case got := <-waiting:
		if got != 3*time.Minute {
			t.Fatalf("wait until independent provider deadline = %s, want 3m", got)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for provider deadline")
	}
	release <- struct{}{}
	providerOnly := receiveRound(t, roundDone)
	if providerOnly.provided != 1 || providerOnly.results != 0 {
		t.Fatalf("provider-only cadence stats = %+v", providerOnly)
	}
	provided, queries = router.snapshot()
	if len(provided) != 2 || len(queries) != 4 {
		t.Fatalf("through t+10m calls = %d provides, %d queries; want 2 and 4", len(provided), len(queries))
	}
}

func TestRendezvousFailedProvideRetriesBeforeRefreshCadence(t *testing.T) {
	t.Parallel()
	router := &fakeRendezvousRouter{provide: func(context.Context, cid.Cid, bool) error {
		return errors.New("DHT not bootstrapped")
	}}
	roundDone := make(chan rendezvousRoundStats, 2)
	waiting := make(chan time.Duration, 2)
	release := make(chan struct{}, 1)
	clock := newManualRendezvousClock()

	s, err := newTestRendezvousService(t.Context(), RendezvousConfig{
		Router:            router,
		Targets:           []RendezvousTarget{{Network: "mainnet", Head: "all"}},
		Interval:          12 * time.Hour,
		Jitter:            time.Hour,
		ProvideRetryMin:   time.Minute,
		ProvideRetryMax:   4 * time.Minute,
		DiscoveryRetryMin: time.Minute,
		DiscoveryRetryMax: 4 * time.Minute,
	}, rendezvousTestConfig{
		host:   &fakeRendezvousHost{self: rendezvousPeerID(t)},
		now:    clock.Now,
		jitter: func(time.Duration) time.Duration { return 0 },
		wait: func(ctx context.Context, delay time.Duration) bool {
			select {
			case waiting <- delay:
			case <-ctx.Done():
				return false
			}
			select {
			case <-release:
				clock.Advance(delay)
				return true
			case <-ctx.Done():
				return false
			}
		},
		roundDone: func(stats rendezvousRoundStats) { roundDone <- stats },
	})
	if err != nil {
		t.Fatalf("building rendezvous service: %v", err)
	}
	defer s.Close()

	initial := receiveRound(t, roundDone)
	if initial.provideError != 1 {
		t.Fatalf("startup stats = %+v, want one provide failure", initial)
	}
	select {
	case delay := <-waiting:
		if delay != time.Minute {
			t.Fatalf("first network retry = %s, want 1m", delay)
		}
	case <-time.After(time.Second):
		t.Fatal("failed startup provide did not schedule a retry")
	}
	release <- struct{}{}
	retry := receiveRound(t, roundDone)
	if retry.provideError != 1 {
		t.Fatalf("retry stats = %+v, want another provide attempt", retry)
	}
	provided, _ := router.snapshot()
	if len(provided) != 2 {
		t.Fatalf("failed startup provide calls after retry = %d, want 2", len(provided))
	}
}

func TestRendezvousResultLimitCancelsRouterChannel(t *testing.T) {
	t.Parallel()
	addr := ma.StringCast("/ip4/127.0.0.1/tcp/4001")
	results := make([]peer.AddrInfo, 8)
	for i := range results {
		results[i] = peer.AddrInfo{ID: rendezvousPeerID(t), Addrs: []ma.Multiaddr{addr}}
	}
	routerCancelled := make(chan struct{})
	router := &fakeRendezvousRouter{find: func(ctx context.Context, _ cid.Cid, _ int) <-chan peer.AddrInfo {
		ch := make(chan peer.AddrInfo)
		go func() {
			defer close(ch)
			for _, result := range results {
				select {
				case ch <- result:
				case <-ctx.Done():
					close(routerCancelled)
					return
				}
			}
		}()
		return ch
	}}
	roundDone := make(chan rendezvousRoundStats, 1)
	s, err := newTestRendezvousService(t.Context(), RendezvousConfig{
		Router:             router,
		Targets:            []RendezvousTarget{{Network: "mainnet", Head: "all"}},
		DisableProviding:   true,
		MaxProviderResults: 2,
	}, rendezvousTestConfig{
		host:      &fakeRendezvousHost{self: rendezvousPeerID(t)},
		wait:      waitUntilCancelled,
		roundDone: func(stats rendezvousRoundStats) { roundDone <- stats },
	})
	if err != nil {
		t.Fatalf("building rendezvous service: %v", err)
	}
	defer s.Close()
	stats := receiveRound(t, roundDone)
	if stats.results != 2 || stats.dialled != 2 {
		t.Fatalf("bounded round stats = %+v, want exactly two consumed and dialled", stats)
	}
	select {
	case <-routerCancelled:
	case <-time.After(time.Second):
		t.Fatal("round completion did not cancel a router blocked beyond the result bound")
	}
}

func TestRendezvousServiceBoundsAndFiltersProviderResults(t *testing.T) {
	t.Parallel()
	self := rendezvousPeerID(t)
	huge := rendezvousPeerID(t)
	failed := rendezvousPeerID(t)
	honest := rendezvousPeerID(t)
	already := rendezvousPeerID(t)
	extra := rendezvousPeerID(t)
	addr := ma.StringCast("/ip4/127.0.0.1/tcp/4001")

	results := []peer.AddrInfo{
		{ID: self, Addrs: []ma.Multiaddr{addr}},
		{ID: self, Addrs: []ma.Multiaddr{addr}},
		{ID: already, Addrs: []ma.Multiaddr{addr}},
		{ID: huge, Addrs: []ma.Multiaddr{addr, addr, addr}},
		{ID: failed, Addrs: []ma.Multiaddr{addr}},
		{ID: honest, Addrs: []ma.Multiaddr{addr}},
		{ID: extra, Addrs: []ma.Multiaddr{addr}},
	}
	router := &fakeRendezvousRouter{find: providerResults(results...)}
	host := &fakeRendezvousHost{
		self:      self,
		connected: map[peer.ID]bool{already: true},
		fail:      map[peer.ID]error{failed: errors.New("refused")},
	}
	roundDone := make(chan rendezvousRoundStats, 1)
	s, err := newTestRendezvousService(t.Context(), RendezvousConfig{
		Router:                  router,
		Targets:                 []RendezvousTarget{{Network: "mainnet", Head: "all"}},
		DisableProviding:        true,
		MaxProviderResults:      6,
		MaxProviderAddressBytes: 3 * len(addr.Bytes()),
	}, rendezvousTestConfig{
		host:      host,
		wait:      waitUntilCancelled,
		roundDone: func(stats rendezvousRoundStats) { roundDone <- stats },
	})
	if err != nil {
		t.Fatalf("building rendezvous service: %v", err)
	}
	defer s.Close()

	stats := receiveRound(t, roundDone)
	if stats.results != 6 || stats.dialled != 2 || stats.connected != 1 || stats.dialFailed != 1 || stats.available != 2 {
		t.Fatalf("round stats = %+v, want six consumed, two dialled, one success, one failure and two available", stats)
	}
	attempts, _, _ := host.snapshot()
	attempted := make(map[peer.ID]bool, len(attempts))
	for _, attempt := range attempts {
		attempted[attempt.ID] = true
	}
	if len(attempts) != 2 || !attempted[failed] || !attempted[honest] {
		t.Fatalf("dial attempts = %v, want failed and honest; self, duplicate, oversized and overflow results filtered", peerIDs(attempts))
	}
	marked := host.markedSnapshot()
	if len(marked) != 2 || !containsPeer(marked, already) || !containsPeer(marked, honest) {
		t.Fatalf("rendezvous marks = %v, want already-connected and successfully dialled peers", marked)
	}
}

func containsPeer(peers []peer.ID, want peer.ID) bool {
	for _, id := range peers {
		if id == want {
			return true
		}
	}
	return false
}

func TestRendezvousServiceBoundsDialConcurrencyAndDeadlines(t *testing.T) {
	t.Parallel()
	const providers = 7
	results := make([]peer.AddrInfo, 0, providers)
	addr := ma.StringCast("/ip4/127.0.0.1/tcp/4001")
	for range providers {
		results = append(results, peer.AddrInfo{ID: rendezvousPeerID(t), Addrs: []ma.Multiaddr{addr}})
	}
	release := make(chan struct{}, providers)
	host := &fakeRendezvousHost{self: rendezvousPeerID(t), release: release}
	roundDone := make(chan rendezvousRoundStats, 1)
	s, err := newTestRendezvousService(t.Context(), RendezvousConfig{
		Router:             &fakeRendezvousRouter{find: providerResults(results...)},
		Targets:            []RendezvousTarget{{Network: "mainnet", Head: "all"}},
		DisableProviding:   true,
		MaxProviderResults: providers,
		DialConcurrency:    2,
		DialTimeout:        time.Second,
		RoundTimeout:       2 * time.Second,
	}, rendezvousTestConfig{
		host:      host,
		wait:      waitUntilCancelled,
		roundDone: func(stats rendezvousRoundStats) { roundDone <- stats },
	})
	if err != nil {
		t.Fatalf("building rendezvous service: %v", err)
	}
	defer s.Close()

	waitForRendezvous(t, "two concurrent dials", func() bool {
		attempts, maxConcurrent, _ := host.snapshot()
		return len(attempts) == 2 && maxConcurrent == 2
	})
	for range providers {
		release <- struct{}{}
	}
	stats := receiveRound(t, roundDone)
	if stats.dialled != providers || stats.connected != providers {
		t.Fatalf("round stats = %+v, want all %d bounded dials to connect", stats, providers)
	}
	attempts, maxConcurrent, sawDeadline := host.snapshot()
	if len(attempts) != providers || maxConcurrent != 2 {
		t.Fatalf("attempts=%d max_concurrent=%d, want %d and 2", len(attempts), maxConcurrent, providers)
	}
	if !sawDeadline {
		t.Fatal("Connect did not receive a per-dial deadline")
	}
}

func TestRendezvousServiceDialTimeoutAndCloseCancelWork(t *testing.T) {
	t.Parallel()
	provider := peer.AddrInfo{
		ID:    rendezvousPeerID(t),
		Addrs: []ma.Multiaddr{ma.StringCast("/ip4/127.0.0.1/tcp/4001")},
	}
	host := &fakeRendezvousHost{self: rendezvousPeerID(t), waitForCtx: true}
	roundDone := make(chan rendezvousRoundStats, 1)
	s, err := newTestRendezvousService(t.Context(), RendezvousConfig{
		Router:           &fakeRendezvousRouter{find: providerResults(provider)},
		Targets:          []RendezvousTarget{{Network: "mainnet", Head: "all"}},
		DisableProviding: true,
		DialTimeout:      20 * time.Millisecond,
		RoundTimeout:     time.Second,
	}, rendezvousTestConfig{
		host:      host,
		wait:      waitUntilCancelled,
		roundDone: func(stats rendezvousRoundStats) { roundDone <- stats },
	})
	if err != nil {
		t.Fatalf("building rendezvous service: %v", err)
	}
	defer s.Close()
	stats := receiveRound(t, roundDone)
	if stats.dialFailed != 1 || stats.connected != 0 {
		t.Fatalf("round stats after dial deadline = %+v", stats)
	}
}

func TestRendezvousServiceMetricsClassifyRoundTimeout(t *testing.T) {
	t.Parallel()
	mx := bmetrics.New()
	rounds := make(chan rendezvousRoundStats, 1)
	s, err := newTestRendezvousService(t.Context(), RendezvousConfig{
		Router: &fakeRendezvousRouter{find: func(context.Context, cid.Cid, int) <-chan peer.AddrInfo {
			return make(chan peer.AddrInfo) // The round deadline must end this query.
		}},
		Targets:          []RendezvousTarget{{Network: "mainnet", Head: "all"}},
		DisableProviding: true,
		DialTimeout:      10 * time.Millisecond,
		RoundTimeout:     20 * time.Millisecond,
	}, rendezvousTestConfig{
		host:      &fakeRendezvousHost{self: rendezvousPeerID(t)},
		wait:      waitUntilCancelled,
		roundDone: func(stats rendezvousRoundStats) { rounds <- stats },
		mx:        mx,
	})
	if err != nil {
		t.Fatalf("building rendezvous service: %v", err)
	}
	defer s.Close()
	receiveRound(t, rounds)
	if got := rendezvousMetricSample(t, mx, `bloar_rendezvous_discovery_rounds_total{outcome="timeout"}`); got != 1 {
		t.Errorf("timed-out discovery rounds = %g, want 1", got)
	}
	if got := rendezvousMetricSample(t, mx, `bloar_rendezvous_observed_provider_samples`); got != 0 {
		t.Errorf("timed-out discovery observed samples = %g, want 0", got)
	}
}

func TestRendezvousMetricsDoNotBlameDiscoveryForProvideTimeout(t *testing.T) {
	t.Parallel()
	mx := bmetrics.New()
	rounds := make(chan rendezvousRoundStats, 1)
	s, err := newTestRendezvousService(t.Context(), RendezvousConfig{
		Router: &fakeRendezvousRouter{
			provide: func(ctx context.Context, _ cid.Cid, _ bool) error {
				<-ctx.Done()
				return ctx.Err()
			},
		},
		Targets:      []RendezvousTarget{{Network: "mainnet", Head: "all"}},
		DialTimeout:  10 * time.Millisecond,
		RoundTimeout: 20 * time.Millisecond,
	}, rendezvousTestConfig{
		host:      &fakeRendezvousHost{self: rendezvousPeerID(t)},
		wait:      waitUntilCancelled,
		roundDone: func(stats rendezvousRoundStats) { rounds <- stats },
		mx:        mx,
	})
	if err != nil {
		t.Fatalf("building rendezvous service: %v", err)
	}
	defer s.Close()
	receiveRound(t, rounds)
	if got := rendezvousMetricSample(t, mx, `bloar_rendezvous_discovery_rounds_total{outcome="empty"}`); got != 1 {
		t.Errorf("empty discovery rounds = %g, want 1 when only the concurrent provide timed out", got)
	}
	if got := rendezvousMetricSample(t, mx, `bloar_rendezvous_discovery_rounds_total{outcome="timeout"}`); got != 0 {
		t.Errorf("discovery timeout rounds = %g, want 0 when discovery completed before provide timeout", got)
	}
}

func TestRendezvousPeerCooldownAndExponentialBackoff(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_700_000_000, 0)
	id := rendezvousPeerID(t)
	s := &RendezvousService{
		cfg: rendezvousSettings{
			peerCooldown:    30 * time.Minute,
			dialBackoffMin:  time.Minute,
			dialBackoffMax:  4 * time.Minute,
			maxTrackedPeers: 16,
		},
		peers: make(map[peer.ID]rendezvousPeerState),
	}

	s.recordDial(id, errors.New("first"), now)
	if s.peerEligible(id, now.Add(time.Minute-time.Nanosecond)) {
		t.Fatal("peer became eligible before minimum failure backoff")
	}
	if !s.peerEligible(id, now.Add(time.Minute)) {
		t.Fatal("peer did not become eligible at minimum failure backoff")
	}
	s.recordDial(id, errors.New("second"), now.Add(time.Minute))
	if s.peerEligible(id, now.Add(3*time.Minute-time.Nanosecond)) {
		t.Fatal("peer became eligible before doubled failure backoff")
	}
	if !s.peerEligible(id, now.Add(3*time.Minute)) {
		t.Fatal("peer did not become eligible after doubled failure backoff")
	}
	s.recordDial(id, nil, now.Add(3*time.Minute))
	if s.peerEligible(id, now.Add(33*time.Minute-time.Nanosecond)) {
		t.Fatal("successful peer became eligible before cooldown")
	}
	if !s.peerEligible(id, now.Add(33*time.Minute)) {
		t.Fatal("successful peer did not become eligible after cooldown")
	}
}

func TestRendezvousPeerStateIsStrictlyBounded(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_700_000_000, 0)
	s := &RendezvousService{
		cfg: rendezvousSettings{
			dialBackoffMin:  time.Hour,
			dialBackoffMax:  time.Hour,
			maxTrackedPeers: 2,
		},
		peers: make(map[peer.ID]rendezvousPeerState),
	}
	for range 10 {
		s.recordDial(rendezvousPeerID(t), errors.New("untrusted peer"), now)
	}
	if got := len(s.peers); got != 2 {
		t.Fatalf("tracked peer states = %d, want hard bound 2", got)
	}
}

func TestRendezvousConfigRejectsUnboundedOrImpossibleSettings(t *testing.T) {
	t.Parallel()
	base := RendezvousConfig{
		Router:  &fakeRendezvousRouter{},
		Targets: []RendezvousTarget{{Network: "mainnet", Head: "all"}},
	}
	testCfg := rendezvousTestConfig{host: &fakeRendezvousHost{self: rendezvousPeerID(t)}}
	tests := []struct {
		name   string
		mutate func(*RendezvousConfig)
	}{
		{"no targets", func(c *RendezvousConfig) { c.Targets = nil }},
		{"negative results", func(c *RendezvousConfig) { c.MaxProviderResults = -1 }},
		{"negative address bytes", func(c *RendezvousConfig) { c.MaxProviderAddressBytes = -1 }},
		{"negative concurrency", func(c *RendezvousConfig) { c.DialConcurrency = -1 }},
		{"negative tracked peers", func(c *RendezvousConfig) { c.MaxTrackedPeers = -1 }},
		{"negative interval", func(c *RendezvousConfig) { c.Interval = -1 }},
		{"jitter reaches interval", func(c *RendezvousConfig) { c.Interval, c.Jitter = time.Hour, time.Hour }},
		{"dial exceeds round", func(c *RendezvousConfig) { c.DialTimeout, c.RoundTimeout = 2*time.Second, time.Second }},
		{"backoff inverted", func(c *RendezvousConfig) { c.DialBackoffMin, c.DialBackoffMax = 2*time.Hour, time.Hour }},
		{"discovery retry inverted", func(c *RendezvousConfig) {
			c.DiscoveryRetryMin, c.DiscoveryRetryMax = 2*time.Hour, time.Hour
		}},
		{"provide retry inverted", func(c *RendezvousConfig) {
			c.ProvideRetryMin, c.ProvideRetryMax = 2*time.Hour, time.Hour
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := base
			tt.mutate(&cfg)
			if service, err := newTestRendezvousService(t.Context(), cfg, testCfg); err == nil {
				service.Close()
				t.Fatal("NewRendezvousService accepted invalid configuration")
			}
		})
	}
}

func providerResults(results ...peer.AddrInfo) func(context.Context, cid.Cid, int) <-chan peer.AddrInfo {
	return func(context.Context, cid.Cid, int) <-chan peer.AddrInfo {
		ch := make(chan peer.AddrInfo, len(results))
		for _, result := range results {
			ch <- result
		}
		close(ch)
		return ch
	}
}

func waitUntilCancelled(ctx context.Context, _ time.Duration) bool {
	<-ctx.Done()
	return false
}

func rendezvousMetricSample(t *testing.T, mx *bmetrics.Metrics, series string) float64 {
	t.Helper()
	recorder := httptest.NewRecorder()
	bmetrics.Handler(mx, nil).ServeHTTP(recorder, httptest.NewRequest("GET", "/metrics", nil))
	for line := range strings.SplitSeq(recorder.Body.String(), "\n") {
		if !strings.HasPrefix(line, series+" ") {
			continue
		}
		value, err := strconv.ParseFloat(strings.TrimSpace(strings.TrimPrefix(line, series+" ")), 64)
		if err != nil {
			t.Fatalf("parsing metric sample %q: %v", line, err)
		}
		return value
	}
	t.Fatalf("metric series %s is absent", series)
	return 0
}

func rendezvousPeerID(t *testing.T) peer.ID {
	t.Helper()
	key, _, err := ci.GenerateEd25519Key(rand.Reader)
	if err != nil {
		t.Fatalf("generating peer key: %v", err)
	}
	id, err := peer.IDFromPrivateKey(key)
	if err != nil {
		t.Fatalf("deriving peer ID: %v", err)
	}
	return id
}

func receiveRound(t *testing.T, rounds <-chan rendezvousRoundStats) rendezvousRoundStats {
	t.Helper()
	select {
	case stats := <-rounds:
		return stats
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for rendezvous round")
		return rendezvousRoundStats{}
	}
}

func waitForRendezvous(t *testing.T, what string, ready func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if ready() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func peerIDs(in []peer.AddrInfo) []peer.ID {
	out := make([]peer.ID, len(in))
	for i := range in {
		out[i] = in[i].ID
	}
	return out
}

type manualRendezvousClock struct {
	mu  sync.Mutex
	now time.Time
}

func newManualRendezvousClock() *manualRendezvousClock {
	return &manualRendezvousClock{now: time.Unix(1_700_000_000, 0)}
}

func (c *manualRendezvousClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *manualRendezvousClock) Advance(delay time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(delay)
	c.mu.Unlock()
}
