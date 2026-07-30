package p2p

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	rand "math/rand/v2"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/event"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/routing"
	"github.com/libp2p/go-libp2p/p2p/host/eventbus"
	ma "github.com/multiformats/go-multiaddr"

	"github.com/blobarchive/bloar/metrics"
)

// DefaultRendezvousInterval is the provisional interval between rendezvous
// provider refreshes and discovery rounds. Provider-record lifetime and
// propagation still need the public-network field measurements before
// this becomes a deployment recommendation; twelve hours is a conservative
// library default that refreshes well inside the DHT's normal provider TTL.
const DefaultRendezvousInterval = 12 * time.Hour

const (
	defaultRendezvousMaxResults      = 32
	defaultRendezvousMaxAddressBytes = 64 << 10
	defaultRendezvousDialConcurrency = 4
	defaultRendezvousDialTimeout     = 10 * time.Second
	defaultRendezvousRoundTimeout    = 45 * time.Second
	defaultRendezvousPeerCooldown    = 30 * time.Minute
	defaultRendezvousDialBackoffMin  = 1 * time.Minute
	defaultRendezvousDialBackoffMax  = 1 * time.Hour
	defaultRendezvousRetryMin        = 1 * time.Minute
	defaultRendezvousRetryMax        = 30 * time.Minute
	defaultRendezvousMaxTrackedPeers = 1024
	defaultRendezvousJitterDivisor   = 12 // +/- 1h at the 12h default.
	defaultRendezvousRetryJitterDiv  = 10
	maxRendezvousTargets             = 256
)

// RendezvousTarget identifies one stable discovery meeting point. It is
// deliberately semantic rather than a CID in configuration: every
// implementation derives the CID with RendezvousCID's versioned wire format.
type RendezvousTarget struct {
	Network string
	Head    string
}

// RendezvousConfig controls bounded DHT rendezvous advertising and discovery.
// Zero values select the conservative defaults documented on each field.
//
// Provider results are untrusted leads. This service only connects their peer
// addresses to Host; it does not select a head, authenticate a document, or
// install the DHT as Bitswap's generic content router.
type RendezvousConfig struct {
	// Host is the already-running Bloar host that discovered providers are
	// connected to. Required.
	Host *Host
	// Router is the DHT (or another explicitly selected ContentRouting
	// implementation) used only for the synthetic rendezvous CIDs. Required.
	Router routing.ContentRouting
	// Targets is the bounded set of (network, head) namespaces this node joins.
	Targets []RendezvousTarget

	// DisableProviding opts out of advertising this node as a source while
	// retaining discovery. It does not change Bitswap's independent serving
	// setting.
	DisableProviding bool
	// Interval is the nominal time between provider refreshes and successful
	// discovery rounds. Empty/failed discovery uses its shorter retry schedule.
	// Zero uses twelve hours.
	Interval time.Duration
	// Jitter is sampled uniformly in [-Jitter,+Jitter] for regular interval
	// waits. Discovery retries use a fixed ten-percent jitter bound. Zero uses
	// Interval/12. It must be less than Interval.
	Jitter time.Duration

	// MaxProviderResults is the maximum number of channel values consumed in
	// one round across all targets. Zero uses 32. The same remaining bound is
	// also passed to every FindProvidersAsync call; a router that ignores it
	// still cannot make this service consume more.
	MaxProviderResults int
	// MaxProviderAddressBytes bounds the sum of multiaddr wire bytes admitted
	// in one round. An individual result that does not fit is skipped rather
	// than allowed to crowd out all later results. Zero uses 64 KiB.
	MaxProviderAddressBytes int
	// DialConcurrency bounds simultaneous Host.Connect calls. Zero uses four.
	DialConcurrency int
	// DialTimeout bounds one Connect. Zero uses ten seconds.
	DialTimeout time.Duration
	// RoundTimeout bounds all providing, querying, and dial completion in one
	// round. Zero uses 45 seconds.
	RoundTimeout time.Duration

	// PeerCooldown suppresses repeated successful/already-connected peers.
	// Zero uses 30 minutes.
	PeerCooldown time.Duration
	// DialBackoffMin and DialBackoffMax bound exponential retry delay after
	// failures. Zero values use one minute and one hour respectively.
	DialBackoffMin time.Duration
	DialBackoffMax time.Duration
	// DiscoveryRetryMin and DiscoveryRetryMax bound the separate exponential
	// retry schedule for a round that found no connected provider. These
	// retries query only: advertising stays on its twelve-hour cadence. Zero
	// values use one minute and 30 minutes.
	DiscoveryRetryMin time.Duration
	DiscoveryRetryMax time.Duration
	// ProvideRetryMin and ProvideRetryMax bound exponential retries when a
	// startup/refresh Provide fails (for example, while DHT bootstrap is still
	// converging). Successful advertisements return to Interval. Zero values
	// use one minute and 30 minutes.
	ProvideRetryMin time.Duration
	ProvideRetryMax time.Duration
	// MaxTrackedPeers bounds the cross-round cooldown/backoff table, so a
	// stream of fresh Sybil IDs cannot create unbounded resident state. Zero
	// uses 1024; the entry with the earliest expiry is evicted when full.
	MaxTrackedPeers int

	Logger *slog.Logger
}

// RendezvousService advertises and discovers a fixed set of rendezvous keys.
// Construction validates synchronously, then returns before any DHT or dial
// operation. The startup round and every later round run on the service's own
// goroutine until Close.
type RendezvousService struct {
	router routing.ContentRouting
	host   rendezvousHost
	keys   []cid.Cid
	cfg    rendezvousSettings
	log    *slog.Logger
	mx     *metrics.Metrics
	deps   rendezvousDeps

	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	refresh chan struct{}

	addressSub event.Subscription
	closeErr   error

	peersMu sync.Mutex
	peers   map[peer.ID]rendezvousPeerState

	closeOnce sync.Once
}

type rendezvousSettings struct {
	disableProviding  bool
	interval          time.Duration
	jitter            time.Duration
	maxResults        int
	maxAddressBytes   int
	dialConcurrency   int
	dialTimeout       time.Duration
	roundTimeout      time.Duration
	peerCooldown      time.Duration
	dialBackoffMin    time.Duration
	dialBackoffMax    time.Duration
	discoveryRetryMin time.Duration
	discoveryRetryMax time.Duration
	provideRetryMin   time.Duration
	provideRetryMax   time.Duration
	maxTrackedPeers   int
}

type rendezvousPeerState struct {
	nextAttempt time.Time
	failures    uint
}

type rendezvousHost interface {
	ID() peer.ID
	Connectedness(peer.ID) network.Connectedness
	Connect(context.Context, peer.AddrInfo) error
	MarkRendezvousPeer(peer.ID) bool
}

type libp2pRendezvousHost struct{ h *Host }

func (h libp2pRendezvousHost) ID() peer.ID { return h.h.ID() }
func (h libp2pRendezvousHost) Connectedness(id peer.ID) network.Connectedness {
	return h.h.Libp2p().Network().Connectedness(id)
}
func (h libp2pRendezvousHost) Connect(ctx context.Context, ai peer.AddrInfo) error {
	return h.h.Libp2p().Connect(ctx, ai)
}
func (h libp2pRendezvousHost) MarkRendezvousPeer(id peer.ID) bool {
	return h.h.MarkRendezvousPeer(id)
}

type rendezvousDeps struct {
	now       func() time.Time
	wait      func(context.Context, time.Duration) bool
	jitter    func(time.Duration) time.Duration
	host      rendezvousHost
	roundDone func(rendezvousRoundStats)
	mx        *metrics.Metrics
}

type rendezvousRoundStats struct {
	provided     int
	provideError int
	results      int
	dialled      int
	connected    int
	dialFailed   int
	available    int
}

// NewRendezvousService validates cfg and starts the asynchronous startup
// round. Its context bounds construction only; Close owns the service lifetime,
// matching Host and Exchange.
func NewRendezvousService(ctx context.Context, cfg RendezvousConfig) (*RendezvousService, error) {
	if cfg.Host == nil {
		return nil, errors.New("p2p: RendezvousConfig.Host must not be nil")
	}
	initialAddrs := canonicalMultiaddrs(cfg.Host.Libp2p().Addrs())
	s, err := newRendezvousService(ctx, cfg, rendezvousDeps{
		now:       time.Now,
		jitter:    rendezvousRandomJitter,
		host:      libp2pRendezvousHost{h: cfg.Host},
		roundDone: func(rendezvousRoundStats) {},
		mx:        cfg.Host.mx,
	})
	if err != nil {
		return nil, err
	}
	s.addressSub, err = cfg.Host.Libp2p().EventBus().Subscribe(
		new(event.EvtLocalAddressesUpdated),
		eventbus.Name("bloar rendezvous address refresh"),
		eventbus.BufSize(4),
	)
	if err != nil {
		_ = s.Close()
		return nil, fmt.Errorf("p2p: observing rendezvous address changes: %w", err)
	}
	s.wg.Add(1)
	go func(initial []string) {
		defer s.wg.Done()
		s.watchAddressChanges(initial)
	}(initialAddrs)
	return s, nil
}

func newRendezvousService(ctx context.Context, cfg RendezvousConfig, deps rendezvousDeps) (*RendezvousService, error) {
	if deps.host == nil {
		return nil, errors.New("p2p: RendezvousConfig.Host must not be nil")
	}
	if cfg.Router == nil {
		return nil, errors.New("p2p: RendezvousConfig.Router must not be nil")
	}
	settings, err := rendezvousConfigSettings(cfg)
	if err != nil {
		return nil, err
	}
	keys, err := rendezvousKeys(cfg.Targets)
	if err != nil {
		return nil, err
	}

	log := cfg.Logger
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	if deps.now == nil {
		deps.now = time.Now
	}
	if deps.jitter == nil {
		deps.jitter = rendezvousRandomJitter
	}
	if deps.roundDone == nil {
		deps.roundDone = func(rendezvousRoundStats) {}
	}

	serviceCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	s := &RendezvousService{
		router:  cfg.Router,
		host:    deps.host,
		keys:    keys,
		cfg:     settings,
		log:     log,
		mx:      deps.mx,
		deps:    deps,
		ctx:     serviceCtx,
		cancel:  cancel,
		refresh: make(chan struct{}, 1),
		peers:   make(map[peer.ID]rendezvousPeerState),
	}
	s.mx.RendezvousActive(metrics.RendezvousOperationDiscover, true)
	s.mx.RendezvousActive(metrics.RendezvousOperationProvide, !settings.disableProviding)
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.run()
	}()
	return s, nil
}

func rendezvousConfigSettings(cfg RendezvousConfig) (rendezvousSettings, error) {
	interval, err := positiveDuration("Interval", cfg.Interval, DefaultRendezvousInterval)
	if err != nil {
		return rendezvousSettings{}, err
	}
	jitter := cfg.Jitter
	if jitter < 0 {
		return rendezvousSettings{}, errors.New("p2p: RendezvousConfig.Jitter must not be negative")
	}
	if jitter == 0 {
		jitter = interval / defaultRendezvousJitterDivisor
	}
	if jitter >= interval {
		return rendezvousSettings{}, errors.New("p2p: RendezvousConfig.Jitter must be less than Interval")
	}
	maxResults, err := positiveInt("MaxProviderResults", cfg.MaxProviderResults, defaultRendezvousMaxResults)
	if err != nil {
		return rendezvousSettings{}, err
	}
	maxAddressBytes, err := positiveInt("MaxProviderAddressBytes", cfg.MaxProviderAddressBytes, defaultRendezvousMaxAddressBytes)
	if err != nil {
		return rendezvousSettings{}, err
	}
	dialConcurrency, err := positiveInt("DialConcurrency", cfg.DialConcurrency, defaultRendezvousDialConcurrency)
	if err != nil {
		return rendezvousSettings{}, err
	}
	dialTimeout, err := positiveDuration("DialTimeout", cfg.DialTimeout, defaultRendezvousDialTimeout)
	if err != nil {
		return rendezvousSettings{}, err
	}
	roundTimeout, err := positiveDuration("RoundTimeout", cfg.RoundTimeout, defaultRendezvousRoundTimeout)
	if err != nil {
		return rendezvousSettings{}, err
	}
	if dialTimeout > roundTimeout {
		return rendezvousSettings{}, errors.New("p2p: RendezvousConfig.DialTimeout must not exceed RoundTimeout")
	}
	peerCooldown, err := positiveDuration("PeerCooldown", cfg.PeerCooldown, defaultRendezvousPeerCooldown)
	if err != nil {
		return rendezvousSettings{}, err
	}
	backoffMin, err := positiveDuration("DialBackoffMin", cfg.DialBackoffMin, defaultRendezvousDialBackoffMin)
	if err != nil {
		return rendezvousSettings{}, err
	}
	backoffMax, err := positiveDuration("DialBackoffMax", cfg.DialBackoffMax, defaultRendezvousDialBackoffMax)
	if err != nil {
		return rendezvousSettings{}, err
	}
	if backoffMin > backoffMax {
		return rendezvousSettings{}, errors.New("p2p: RendezvousConfig.DialBackoffMin must not exceed DialBackoffMax")
	}
	retryMin, err := positiveDuration("DiscoveryRetryMin", cfg.DiscoveryRetryMin, defaultRendezvousRetryMin)
	if err != nil {
		return rendezvousSettings{}, err
	}
	retryMax, err := positiveDuration("DiscoveryRetryMax", cfg.DiscoveryRetryMax, defaultRendezvousRetryMax)
	if err != nil {
		return rendezvousSettings{}, err
	}
	if retryMin > retryMax {
		return rendezvousSettings{}, errors.New("p2p: RendezvousConfig.DiscoveryRetryMin must not exceed DiscoveryRetryMax")
	}
	provideRetryMin, err := positiveDuration("ProvideRetryMin", cfg.ProvideRetryMin, defaultRendezvousRetryMin)
	if err != nil {
		return rendezvousSettings{}, err
	}
	provideRetryMax, err := positiveDuration("ProvideRetryMax", cfg.ProvideRetryMax, defaultRendezvousRetryMax)
	if err != nil {
		return rendezvousSettings{}, err
	}
	if provideRetryMin > provideRetryMax {
		return rendezvousSettings{}, errors.New("p2p: RendezvousConfig.ProvideRetryMin must not exceed ProvideRetryMax")
	}
	maxTrackedPeers, err := positiveInt("MaxTrackedPeers", cfg.MaxTrackedPeers, defaultRendezvousMaxTrackedPeers)
	if err != nil {
		return rendezvousSettings{}, err
	}
	return rendezvousSettings{
		disableProviding:  cfg.DisableProviding,
		interval:          interval,
		jitter:            jitter,
		maxResults:        maxResults,
		maxAddressBytes:   maxAddressBytes,
		dialConcurrency:   dialConcurrency,
		dialTimeout:       dialTimeout,
		roundTimeout:      roundTimeout,
		peerCooldown:      peerCooldown,
		dialBackoffMin:    backoffMin,
		dialBackoffMax:    backoffMax,
		discoveryRetryMin: retryMin,
		discoveryRetryMax: retryMax,
		provideRetryMin:   provideRetryMin,
		provideRetryMax:   provideRetryMax,
		maxTrackedPeers:   maxTrackedPeers,
	}, nil
}

func positiveDuration(name string, value, fallback time.Duration) (time.Duration, error) {
	if value == 0 {
		return fallback, nil
	}
	if value < 0 {
		return 0, fmt.Errorf("p2p: RendezvousConfig.%s must be positive", name)
	}
	return value, nil
}

func positiveInt(name string, value, fallback int) (int, error) {
	if value == 0 {
		return fallback, nil
	}
	if value < 0 {
		return 0, fmt.Errorf("p2p: RendezvousConfig.%s must be positive", name)
	}
	return value, nil
}

func rendezvousKeys(targets []RendezvousTarget) ([]cid.Cid, error) {
	if len(targets) == 0 {
		return nil, errors.New("p2p: RendezvousConfig.Targets must not be empty")
	}
	if len(targets) > maxRendezvousTargets {
		return nil, fmt.Errorf("p2p: RendezvousConfig.Targets has %d entries, limit %d", len(targets), maxRendezvousTargets)
	}
	keys := make([]cid.Cid, 0, len(targets))
	seen := make(map[string]struct{}, len(targets))
	for i, target := range targets {
		key, err := RendezvousCID(target.Network, target.Head)
		if err != nil {
			return nil, fmt.Errorf("p2p: RendezvousConfig.Targets[%d]: %w", i, err)
		}
		if _, duplicate := seen[key.KeyString()]; duplicate {
			continue
		}
		seen[key.KeyString()] = struct{}{}
		keys = append(keys, key)
	}
	return keys, nil
}

func rendezvousRandomJitter(bound time.Duration) time.Duration {
	if bound <= 0 {
		return 0
	}
	// Int64N's argument cannot overflow: a validated jitter is less than a
	// positive time.Duration, hence at most MaxInt64-1 and 2*bound+1 is the
	// only risky form. Sampling magnitude and sign separately avoids it.
	magnitude := time.Duration(rand.Int64N(int64(bound) + 1))
	if magnitude != 0 && rand.IntN(2) == 0 {
		return -magnitude
	}
	return magnitude
}

func (s *RendezvousService) run() {
	now := s.deps.now()
	nextProvide := now
	nextDiscovery := now
	discoveryRetry := s.cfg.discoveryRetryMin
	provideRetry := s.cfg.provideRetryMin

	for {
		now = s.deps.now()
		doProvide := !s.cfg.disableProviding && !now.Before(nextProvide)
		doDiscovery := !now.Before(nextDiscovery)
		stats := s.round(doProvide, doDiscovery)
		s.deps.roundDone(stats)
		if s.ctx.Err() != nil {
			return
		}

		now = s.deps.now()
		if doProvide {
			if stats.provideError == 0 {
				provideRetry = s.cfg.provideRetryMin
				nextProvide = now.Add(s.jittered(s.cfg.interval, s.cfg.jitter))
			} else {
				delay := min(provideRetry, s.cfg.interval)
				nextProvide = now.Add(s.jittered(delay, delay/defaultRendezvousRetryJitterDiv))
				provideRetry = nextRendezvousBackoff(provideRetry, s.cfg.provideRetryMax)
			}
		}
		if doDiscovery {
			if stats.available > 0 {
				discoveryRetry = s.cfg.discoveryRetryMin
				nextDiscovery = now.Add(s.jittered(s.cfg.interval, s.cfg.jitter))
			} else {
				delay := min(discoveryRetry, s.cfg.interval)
				nextDiscovery = now.Add(s.jittered(delay, delay/defaultRendezvousRetryJitterDiv))
				discoveryRetry = nextRendezvousBackoff(discoveryRetry, s.cfg.discoveryRetryMax)
			}
		}

		next := nextDiscovery
		if !s.cfg.disableProviding && nextProvide.Before(next) {
			next = nextProvide
		}
		delay := next.Sub(s.deps.now())
		if delay <= 0 {
			delay = time.Nanosecond
		}
		alive, refreshed := s.wait(delay)
		if !alive {
			return
		}
		if refreshed {
			now = s.deps.now()
			nextDiscovery = now
			if !s.cfg.disableProviding {
				nextProvide = now
			}
		}
	}
}

// Refresh coalesces an immediate provide/discovery round. It is safe to call
// from address-event handlers and never blocks a libp2p event bus callback.
func (s *RendezvousService) Refresh() {
	if s == nil {
		return
	}
	select {
	case s.refresh <- struct{}{}:
	default:
	}
}

// wait retains the injectable clock/wait seam used by deterministic schedule
// tests. Production waits also listen for coalesced refresh signals.
func (s *RendezvousService) wait(delay time.Duration) (alive, refreshed bool) {
	if s.deps.wait != nil {
		return s.deps.wait(s.ctx, delay), false
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-s.ctx.Done():
		return false, false
	case <-s.refresh:
		return true, true
	case <-timer.C:
		return true, false
	}
}

func (s *RendezvousService) watchAddressChanges(previous []string) {
	for {
		select {
		case <-s.ctx.Done():
			return
		case raw, ok := <-s.addressSub.Out():
			if !ok {
				return
			}
			updated, ok := raw.(event.EvtLocalAddressesUpdated)
			if !ok {
				continue
			}
			current := make([]ma.Multiaddr, 0, len(updated.Current))
			for _, address := range updated.Current {
				current = append(current, address.Address)
			}
			canonical := canonicalMultiaddrs(current)
			if slices.Equal(previous, canonical) {
				continue // Stateful subscription replay, not a change.
			}
			previous = canonical
			s.Refresh()
		}
	}
}

func canonicalMultiaddrs(addrs []ma.Multiaddr) []string {
	canonical := make([]string, 0, len(addrs))
	for _, address := range addrs {
		if address != nil {
			canonical = append(canonical, string(address.Bytes()))
		}
	}
	slices.Sort(canonical)
	return slices.Compact(canonical)
}

func nextRendezvousBackoff(current, maximum time.Duration) time.Duration {
	if current >= maximum {
		return maximum
	}
	if current > maximum/2 {
		return maximum
	}
	return current * 2
}

func (s *RendezvousService) jittered(delay, bound time.Duration) time.Duration {
	offset := s.deps.jitter(bound)
	if offset > bound {
		offset = bound
	} else if offset < -bound {
		offset = -bound
	}
	delay += offset
	// An injected jitter function is test-only but still kept from turning the
	// production loop into a busy loop if it violates its [-bound,+bound]
	// contract.
	if delay <= 0 {
		return time.Nanosecond
	}
	return delay
}

func (s *RendezvousService) round(doProvide, doDiscovery bool) rendezvousRoundStats {
	ctx, cancel := context.WithTimeout(s.ctx, s.cfg.roundTimeout)
	defer cancel()
	stats := rendezvousRoundStats{}

	// Providing and discovery are independent. Running the small, bounded
	// provide pass alongside queries prevents a slow public DHT write from
	// consuming the entire round and starving discovery.
	var provideWG sync.WaitGroup
	if doProvide {
		provideWG.Add(1)
		go func() {
			defer provideWG.Done()
			for _, key := range s.keys {
				if ctx.Err() != nil {
					return
				}
				if err := s.router.Provide(ctx, key, true); err != nil {
					// Closing the service cancels its current RPCs by design; that is
					// lifecycle, not a failed rendezvous write.
					if s.ctx.Err() == nil {
						s.mx.RendezvousProvide(metrics.OutcomeError, s.deps.now())
					}
					stats.provideError++
					s.log.Debug("rendezvous provide failed", "key", key, "err", err)
					continue
				}
				s.mx.RendezvousProvide(metrics.OutcomeOK, s.deps.now())
				stats.provided++
			}
		}()
	}

	seen := map[peer.ID]struct{}{s.host.ID(): {}}
	addressBytes := 0
	dialSlots := make(chan struct{}, s.cfg.dialConcurrency)
	var dialWG sync.WaitGroup
	var dialStatsMu sync.Mutex

	queriesDone := false
	var discoveryTimedOut atomic.Bool
	for _, key := range s.keys {
		if !doDiscovery {
			break
		}
		if stats.results >= s.cfg.maxResults {
			break
		}
		if ctx.Err() != nil {
			discoveryTimedOut.Store(true)
			break
		}
		providers := s.router.FindProvidersAsync(ctx, key, s.cfg.maxResults-stats.results)
		if ctx.Err() != nil {
			discoveryTimedOut.Store(true)
			break
		}
		if providers == nil {
			continue
		}
		for !queriesDone && stats.results < s.cfg.maxResults {
			select {
			case <-ctx.Done():
				discoveryTimedOut.Store(true)
				queriesDone = true
			case ai, ok := <-providers:
				if !ok {
					queriesDone = true
					break
				}
				stats.results++
				if ai.ID == "" {
					continue
				}
				if _, duplicate := seen[ai.ID]; duplicate {
					continue
				}
				seen[ai.ID] = struct{}{}

				candidateBytes := rendezvousAddressBytes(ai)
				if candidateBytes > s.cfg.maxAddressBytes-addressBytes {
					continue
				}
				addressBytes += candidateBytes
				if s.host.Connectedness(ai.ID) == network.Connected {
					s.host.MarkRendezvousPeer(ai.ID)
					s.recordDial(ai.ID, nil, s.deps.now())
					dialStatsMu.Lock()
					stats.available++
					dialStatsMu.Unlock()
					continue
				}
				if !s.peerEligible(ai.ID, s.deps.now()) {
					continue
				}

				select {
				case dialSlots <- struct{}{}:
				case <-ctx.Done():
					discoveryTimedOut.Store(true)
					queriesDone = true
					continue
				}
				dialWG.Add(1)
				stats.dialled++
				candidate := ai
				go func() {
					defer dialWG.Done()
					defer func() { <-dialSlots }()
					dialCtx, dialCancel := context.WithTimeout(ctx, s.cfg.dialTimeout)
					err := s.host.Connect(dialCtx, candidate)
					dialCancel()
					s.recordDial(candidate.ID, err, s.deps.now())
					dialStatsMu.Lock()
					if err != nil {
						if errors.Is(ctx.Err(), context.DeadlineExceeded) {
							discoveryTimedOut.Store(true)
						}
						stats.dialFailed++
						dialStatsMu.Unlock()
						s.log.Debug("rendezvous provider dial failed", "peer", candidate.ID, "err", err)
						return
					}
					s.host.MarkRendezvousPeer(candidate.ID)
					stats.connected++
					stats.available++
					dialStatsMu.Unlock()
					s.log.Debug("rendezvous provider connected", "peer", candidate.ID)
				}()
			}
		}
		queriesDone = false
	}
	dialWG.Wait()
	provideWG.Wait()
	if doDiscovery && s.ctx.Err() == nil {
		outcome := metrics.RendezvousDiscoveryEmpty
		switch {
		case stats.available > 0:
			outcome = metrics.RendezvousDiscoveryAvailable
		case discoveryTimedOut.Load():
			outcome = metrics.RendezvousDiscoveryTimeout
		}
		s.mx.RendezvousDiscovery(outcome, stats.results)
	}
	s.log.Debug("rendezvous round complete", "provided", stats.provided,
		"provide_errors", stats.provideError, "provider_results", stats.results,
		"dials", stats.dialled, "connected", stats.connected, "dial_errors", stats.dialFailed)
	return stats
}

func rendezvousAddressBytes(ai peer.AddrInfo) int {
	total := 0
	for _, addr := range ai.Addrs {
		length := len(addr.Bytes())
		if length > int(^uint(0)>>1)-total {
			return int(^uint(0) >> 1)
		}
		total += length
	}
	return total
}

func (s *RendezvousService) peerEligible(id peer.ID, now time.Time) bool {
	s.peersMu.Lock()
	defer s.peersMu.Unlock()
	state, ok := s.peers[id]
	return !ok || !now.Before(state.nextAttempt)
}

func (s *RendezvousService) recordDial(id peer.ID, err error, now time.Time) {
	s.peersMu.Lock()
	defer s.peersMu.Unlock()
	state := s.peers[id]
	if _, exists := s.peers[id]; !exists && len(s.peers) >= s.cfg.maxTrackedPeers {
		var evict peer.ID
		var earliest time.Time
		for candidate, candidateState := range s.peers {
			if evict == "" || candidateState.nextAttempt.Before(earliest) {
				evict = candidate
				earliest = candidateState.nextAttempt
			}
		}
		delete(s.peers, evict)
	}
	if err == nil {
		state.failures = 0
		state.nextAttempt = now.Add(s.cfg.peerCooldown)
		s.peers[id] = state
		return
	}
	if state.failures < 63 {
		state.failures++
	}
	delay := s.cfg.dialBackoffMin
	for i := uint(1); i < state.failures && delay < s.cfg.dialBackoffMax; i++ {
		if delay > s.cfg.dialBackoffMax/2 {
			delay = s.cfg.dialBackoffMax
			break
		}
		delay *= 2
	}
	if delay > s.cfg.dialBackoffMax {
		delay = s.cfg.dialBackoffMax
	}
	state.nextAttempt = now.Add(delay)
	s.peers[id] = state
}

// Close cancels an in-flight round, stops future rounds, and waits for bounded
// Provide, FindProvidersAsync, and Connect operations to observe cancellation.
func (s *RendezvousService) Close() error {
	if s == nil {
		return nil
	}
	s.closeOnce.Do(func() {
		s.cancel()
		if s.addressSub != nil {
			s.closeErr = s.addressSub.Close()
		}
		s.wg.Wait()
		s.mx.RendezvousActive(metrics.RendezvousOperationProvide, false)
		s.mx.RendezvousActive(metrics.RendezvousOperationDiscover, false)
	})
	return s.closeErr
}
