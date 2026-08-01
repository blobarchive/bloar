package pointerhint

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	rand "math/rand/v2"
	"sync"
	"time"

	"github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/go-cid"

	"github.com/blobarchive/bloar/metrics"
)

const (
	// DefaultReprovideInterval refreshes current pointers well inside the
	// public DHT provider-record lifetime. It is provisional pending field
	// measurements; it is not a deployment claim about observed propagation.
	DefaultReprovideInterval = 12 * time.Hour
	// The default reprovide jitter is derived as ReprovideInterval/12, giving
	// the twelve-hour default a bounded +/- one-hour spread across nodes.
	// DefaultMinWriteInterval is a hard process-local DHT write ceiling: the
	// provider starts at most one Provide call per second, including retries.
	DefaultMinWriteInterval = time.Second
	DefaultRetryMin         = time.Minute
	DefaultRetryMax         = 30 * time.Minute
	DefaultAttemptTimeout   = 45 * time.Second
)

// ContentProvider is the only DHT capability Provider accepts. Keeping the
// interface narrower than routing.ContentRouting prevents this service from
// being reused as implicit generic block-provider routing.
type ContentProvider interface {
	Provide(context.Context, cid.Cid, bool) error
}

// ProviderConfig controls exact-current-pointer advertisement.
type ProviderConfig struct {
	Router ContentProvider
	// Serving is exactly the blockstore exposed to Bitswap. Every CID is
	// checked with Has immediately before its Provide call.
	Serving blockstore.Blockstore
	// VerifiedDocuments is additionally required for a Document pointer. A
	// document merely present in Serving is never eligible by itself.
	VerifiedDocuments VerifiedDocuments

	ReprovideInterval time.Duration
	// ReprovideJitter is sampled uniformly in [-Jitter,+Jitter]. Zero uses
	// ReprovideInterval/12; it must be less than ReprovideInterval. Failed
	// attempts independently use bounded +/-10% retry jitter.
	ReprovideJitter  time.Duration
	MinWriteInterval time.Duration
	RetryMin         time.Duration
	RetryMax         time.Duration
	AttemptTimeout   time.Duration
	// Metrics records only closed pointer kinds and outcomes in Bloar's
	// process-local registry. Nil disables instrumentation.
	Metrics *metrics.Metrics
	Logger  *slog.Logger
}

type providerSettings struct {
	reprovide time.Duration
	jitter    time.Duration
	minWrite  time.Duration
	retryMin  time.Duration
	retryMax  time.Duration
	timeout   time.Duration
}

// Provider advertises at most the latest desired pointer schedule. Update is
// the single-head API; Coordinator owns the bounded multi-head API. Both paths
// are coalescing and non-blocking, so a publication burst occupies one
// fixed-size wake slot regardless of mutation rate.
//
// One Provider represents one selected head/profile. A multi-head daemon must
// put a node-level aggregate/deduplicating coordinator in front of this core;
// constructing one independent Provider per head would duplicate the shared
// document hint and multiply the documented process write ceiling.
//
// A newly installed set is attempted immediately for discovery availability;
// its at-most-three writes are serialized by MinWriteInterval. Recurring
// reprovides and retries are jittered to avoid periodic fleet-wide herds. A
// rollout controller may additionally stagger simultaneous first starts.
//
// A pointer already inside an in-flight Provide when Update occurs may leave a
// stale remote record. That is inherent to the DHT (records cannot be
// withdrawn). Once the worker observes the update, the old pointer is removed
// from every future local retry/reprovide schedule.
type Provider struct {
	router ContentProvider
	local  blockstore.Blockstore
	docs   VerifiedDocuments
	cfg    providerSettings
	log    *slog.Logger
	mx     *metrics.Metrics

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	once   sync.Once
	wake   chan struct{}

	mu      sync.Mutex
	desired []scheduledPointer
	success map[string]time.Time
	version uint64
	closed  bool
}

type providerState struct {
	pointer    Pointer
	generation uint64
	next       time.Time
	retry      time.Duration
}

type scheduledPointer struct {
	pointer    Pointer
	generation uint64
}

// NewProvider validates cfg and starts an idle provider. Call Update to install
// its exact current set.
func NewProvider(ctx context.Context, cfg ProviderConfig) (*Provider, error) {
	if cfg.Router == nil {
		return nil, errors.New("pointerhint: ProviderConfig.Router must not be nil")
	}
	if cfg.Serving == nil {
		return nil, errors.New("pointerhint: ProviderConfig.Serving must not be nil")
	}
	settings, err := providerConfigSettings(cfg)
	if err != nil {
		return nil, err
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	serviceCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	p := &Provider{
		router:  cfg.Router,
		local:   cfg.Serving,
		docs:    cfg.VerifiedDocuments,
		cfg:     settings,
		log:     logger,
		mx:      cfg.Metrics,
		ctx:     serviceCtx,
		cancel:  cancel,
		wake:    make(chan struct{}, 1),
		success: make(map[string]time.Time),
	}
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		p.run()
	}()
	return p, nil
}

func providerConfigSettings(cfg ProviderConfig) (providerSettings, error) {
	s := providerSettings{
		reprovide: cfg.ReprovideInterval,
		jitter:    cfg.ReprovideJitter,
		minWrite:  cfg.MinWriteInterval,
		retryMin:  cfg.RetryMin,
		retryMax:  cfg.RetryMax,
		timeout:   cfg.AttemptTimeout,
	}
	if s.reprovide == 0 {
		s.reprovide = DefaultReprovideInterval
	}
	if s.jitter == 0 {
		s.jitter = s.reprovide / 12
	}
	if s.minWrite == 0 {
		s.minWrite = DefaultMinWriteInterval
	}
	if s.retryMin == 0 {
		s.retryMin = DefaultRetryMin
	}
	if s.retryMax == 0 {
		s.retryMax = DefaultRetryMax
	}
	if s.timeout == 0 {
		s.timeout = DefaultAttemptTimeout
	}
	for name, value := range map[string]time.Duration{
		"ReprovideInterval": s.reprovide,
		"ReprovideJitter":   s.jitter,
		"MinWriteInterval":  s.minWrite,
		"RetryMin":          s.retryMin,
		"RetryMax":          s.retryMax,
		"AttemptTimeout":    s.timeout,
	} {
		if value <= 0 {
			return providerSettings{}, fmt.Errorf("pointerhint: ProviderConfig.%s is %s, must be positive", name, value)
		}
	}
	if s.retryMax < s.retryMin {
		return providerSettings{}, errors.New("pointerhint: ProviderConfig.RetryMax must not be less than RetryMin")
	}
	if s.jitter >= s.reprovide {
		return providerSettings{}, errors.New("pointerhint: ProviderConfig.ReprovideJitter must be less than ReprovideInterval")
	}
	return s, nil
}

// Update replaces the desired set. Repeated updates coalesce to the latest
// value; no queue grows with publication frequency.
func (p *Provider) Update(set Set) error {
	items, err := set.pointers()
	if err != nil {
		return err
	}
	return p.updatePointers(items)
}

// updatePointers is the multi-head path used only by Coordinator. It accepts
// a flat exact-current schedule because a Set cannot represent two current
// roots of the same semantic kind. Keeping this method internal prevents an
// unbounded caller-defined slice from becoming public API; Coordinator proves
// the schedule is bounded by at most three pointers per configured head plus
// its single node-local publication document.
func (p *Provider) updatePointers(items []Pointer) error {
	if len(items) > MaxCoordinatorPointers {
		return fmt.Errorf("pointerhint: provider schedule has %d pointers, exceeds hard limit %d", len(items), MaxCoordinatorPointers)
	}
	normalized, err := normalizePointers(items)
	if err != nil {
		return err
	}
	for _, item := range normalized {
		if item.Kind == Document && p.docs == nil {
			return errors.New("pointerhint: a document pointer requires VerifiedDocuments")
		}
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return errors.New("pointerhint: provider is closed")
	}
	previous := make(map[string]scheduledPointer, len(p.desired))
	for _, item := range p.desired {
		previous[item.pointer.CID.KeyString()] = item
	}
	nextVersion := p.version + 1
	scheduled := make([]scheduledPointer, len(normalized))
	retainedSuccess := make(map[string]time.Time, len(normalized))
	for i, item := range normalized {
		key := item.CID.KeyString()
		if old, exists := previous[key]; exists && old.pointer == item {
			scheduled[i] = old
			if succeeded := p.success[key]; !succeeded.IsZero() {
				retainedSuccess[key] = succeeded
			}
		} else {
			scheduled[i] = scheduledPointer{pointer: item, generation: nextVersion}
		}
	}
	p.desired = scheduled
	p.success = retainedSuccess
	p.version = nextVersion
	for _, kind := range []Kind{Root, Manifest, Document} {
		p.updatePointerScheduleMetricLocked(kind)
	}
	p.mu.Unlock()
	select {
	case p.wake <- struct{}{}:
	default:
	}
	return nil
}

func (p *Provider) snapshot() ([]scheduledPointer, uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.desired, p.version
}

func (p *Provider) run() {
	defer p.clearPointerMetrics()
	states := make(map[string]providerState, 3)
	var applied uint64
	var nextWrite time.Time
	for {
		items, version := p.snapshot()
		if version != applied {
			p.reconcile(states, items, time.Now())
			applied = version
		}
		state, ok := nextProviderState(states)
		if !ok {
			select {
			case <-p.ctx.Done():
				return
			case <-p.wake:
				continue
			}
		}

		now := time.Now()
		if state.next.After(now) {
			if !p.waitOrUpdate(state.next.Sub(now)) {
				return
			}
			continue
		}
		key := state.pointer.CID.KeyString()
		eligible, reason, err := p.eligible(state.pointer)
		if p.ctx.Err() != nil {
			return // Provider shutdown is not an eligibility failure or retry.
		}
		if err != nil || !eligible {
			p.log.Debug("pointer not eligible for provide", "kind", state.pointer.Kind,
				"cid", state.pointer.CID, "reason", reason, "err", err)
			retryReason := metrics.PointerRetryIneligible
			if err != nil {
				retryReason = metrics.PointerRetryCheckError
			}
			p.retry(states, key, time.Now(), retryReason)
			continue
		}

		if delay := time.Until(nextWrite); delay > 0 {
			if !p.waitOrUpdate(delay) {
				return
			}
			continue
		}
		// The wake may have been coalesced without winning the timer select.
		// Reconcile at the top before starting any DHT call.
		if _, version := p.snapshot(); version != applied {
			continue
		}

		started := time.Now()
		nextWrite = started.Add(p.cfg.minWrite)
		attemptCtx, cancel := context.WithTimeout(p.ctx, p.cfg.timeout)
		err = p.router.Provide(attemptCtx, state.pointer.CID, true)
		cancel()
		completed := time.Now()
		if p.ctx.Err() != nil {
			return // A canceled shutdown RPC is not a failed provide.
		}
		// Record the completed attempt before reconciling an update that arrived
		// during it. If the new set retains this pointer, its successful
		// reprovide or failed retry cadence must survive the update; otherwise a
		// stream of unrelated updates could force this pointer back to "due now"
		// and collapse the normal cadence to MinWriteInterval. Reconcile removes
		// the state below on the next loop when the new set dropped or retyped it.
		current, exists := states[key]
		if !exists || current.pointer != state.pointer || current.generation != state.generation {
			continue
		}
		if err != nil {
			p.mx.PointerProvideOutcome(pointerMetricKind(state.pointer.Kind), metrics.OutcomeError)
			p.log.Debug("pointer provide failed", "kind", state.pointer.Kind, "cid", state.pointer.CID, "err", err)
			p.retry(states, key, completed, metrics.PointerRetryProvideError)
			continue
		}
		current.next = completed.Add(pointerJittered(p.cfg.reprovide, p.cfg.jitter))
		current.retry = p.cfg.retryMin
		states[key] = current
		p.mx.PointerProvideOutcome(pointerMetricKind(state.pointer.Kind), metrics.OutcomeOK)
		p.recordPointerSuccess(state.pointer, state.generation, completed)
		p.log.Debug("pointer provided", "kind", state.pointer.Kind, "cid", state.pointer.CID)
	}
}

func (p *Provider) waitOrUpdate(delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-p.ctx.Done():
		return false
	case <-p.wake:
		return true
	case <-timer.C:
		return true
	}
}

func (p *Provider) reconcile(states map[string]providerState, items []scheduledPointer, now time.Time) {
	desired := make(map[string]scheduledPointer, len(items))
	for _, item := range items {
		desired[item.pointer.CID.KeyString()] = item
	}
	for key := range states {
		if _, keep := desired[key]; !keep {
			delete(states, key)
		}
	}
	for key, item := range desired {
		if existing, ok := states[key]; ok && existing.pointer == item.pointer && existing.generation == item.generation {
			continue
		}
		states[key] = providerState{pointer: item.pointer, generation: item.generation, next: now, retry: p.cfg.retryMin}
	}
}

func (p *Provider) recordPointerSuccess(pointer Pointer, generation uint64, completed time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	key := pointer.CID.KeyString()
	for _, current := range p.desired {
		if current.pointer.CID.KeyString() == key && current.pointer == pointer && current.generation == generation {
			p.success[key] = completed
			p.updatePointerScheduleMetricLocked(pointer.Kind)
			return
		}
	}
}

// updatePointerScheduleMetricLocked publishes the oldest success across the
// exact desired set. updatePointers calls it before returning, so a new CID is
// visible as zero freshness even while an old Provide remains in flight;
// recordPointerSuccess's exact-pointer membership check prevents that stale
// completion from lending freshness to a dropped or retyped CID.
func (p *Provider) updatePointerScheduleMetricLocked(kind Kind) {
	present := false
	var oldest time.Time
	for _, scheduled := range p.desired {
		pointer := scheduled.pointer
		if pointer.Kind != kind {
			continue
		}
		present = true
		succeeded := p.success[pointer.CID.KeyString()]
		if succeeded.IsZero() {
			oldest = time.Time{}
			break
		}
		if oldest.IsZero() || succeeded.Before(oldest) {
			oldest = succeeded
		}
	}
	p.mx.PointerSchedule(pointerMetricKind(kind), present, oldest)
}

func nextProviderState(states map[string]providerState) (providerState, bool) {
	var best providerState
	found := false
	for _, state := range states {
		if !found || state.next.Before(best.next) ||
			(state.next.Equal(best.next) && (state.pointer.Kind < best.pointer.Kind ||
				(state.pointer.Kind == best.pointer.Kind && state.pointer.CID.String() < best.pointer.CID.String()))) {
			best, found = state, true
		}
	}
	return best, found
}

func (p *Provider) eligible(pointer Pointer) (bool, string, error) {
	ctx, cancel := context.WithTimeout(p.ctx, p.cfg.timeout)
	defer cancel()
	if pointer.Kind == Document {
		verified, err := p.docs.HasVerified(ctx, pointer.CID)
		if err != nil {
			return false, "verified-document check failed", err
		}
		if !verified {
			return false, "document is not in the verified retained set", nil
		}
	}
	present, err := p.local.Has(ctx, pointer.CID)
	if err != nil {
		return false, "serving blockstore check failed", err
	}
	if !present {
		return false, "CID is not in the serving blockstore", nil
	}
	return true, "", nil
}

func (p *Provider) retry(states map[string]providerState, key string, now time.Time, reason string) {
	state, ok := states[key]
	if !ok {
		return
	}
	p.mx.PointerRetry(pointerMetricKind(state.pointer.Kind), reason)
	state.next = now.Add(pointerJittered(state.retry, state.retry/10))
	if state.retry < p.cfg.retryMax {
		if state.retry > p.cfg.retryMax/2 {
			state.retry = p.cfg.retryMax
		} else {
			state.retry *= 2
			if state.retry > p.cfg.retryMax {
				state.retry = p.cfg.retryMax
			}
		}
	}
	states[key] = state
}

func (p *Provider) clearPointerMetrics() {
	for _, kind := range []Kind{Root, Manifest, Document} {
		p.mx.PointerCurrent(pointerMetricKind(kind), false, false)
	}
}

func pointerMetricKind(kind Kind) string {
	switch kind {
	case Root:
		return metrics.PointerKindRoot
	case Manifest:
		return metrics.PointerKindManifest
	case Document:
		return metrics.PointerKindDocument
	default:
		return ""
	}
}

func pointerJittered(delay, bound time.Duration) time.Duration {
	if bound <= 0 {
		return delay
	}
	// The inclusive span [-bound,+bound] fits exactly in a uint64 even when
	// bound is MaxInt64. Mapping one uniform unsigned sample avoids both signed
	// width overflow and the doubled probability of zero from sign+magnitude
	// sampling.
	span := uint64(bound)*2 + 1
	sample := rand.Uint64N(span)
	if sample <= uint64(bound) {
		magnitude := time.Duration(uint64(bound) - sample)
		if magnitude >= delay {
			return time.Nanosecond
		}
		return delay - magnitude
	}
	magnitude := time.Duration(sample - uint64(bound))
	const maxDuration = time.Duration(1<<63 - 1)
	if magnitude > maxDuration-delay {
		return maxDuration
	}
	return delay + magnitude
}

// Close stops retries and reprovides and waits for an in-flight bounded
// blockstore or DHT operation to observe cancellation.
func (p *Provider) Close() error {
	if p == nil {
		return nil
	}
	p.once.Do(func() {
		p.mu.Lock()
		p.closed = true
		p.mu.Unlock()
		p.cancel()
		p.wg.Wait()
	})
	return nil
}
