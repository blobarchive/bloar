// Package census implements a bounded, local-view swarm census.
//
// A census never reports a global replica count. DHT provider records are
// partial, cached observations, so every count in Report.LowerBounds is a
// timestamped lower bound from one observer. The package performs no network
// I/O of its own: callers explicitly supply the discovery and peer-targeted
// probing implementations.
package census

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
)

const (
	DefaultMaxProviders    = 64
	DefaultMaxAddressBytes = 128 << 10
	DefaultConcurrency     = 4
	DefaultMaxHistorical   = 16

	DefaultOverallTimeout   = 45 * time.Second
	DefaultDiscoveryTimeout = 15 * time.Second
	DefaultProbeTimeout     = 10 * time.Second

	hardMaxProviders    = 1024
	hardMaxAddressBytes = 8 << 20
	hardMaxConcurrency  = 64
	hardMaxHistorical   = 64
	hardMaxTimeout      = 10 * time.Minute
	maxCIDBytes         = 512
	maxPeerIDBytes      = 512
	maxAddressValues    = 4096
	maxRawErrorBytes    = 4096

	reportVersion = 1
)

// Finder returns provider records for a deterministic rendezvous namespace-block CID. It must
// honor ctx and limit. The inspector also enforces both bounds while consuming
// the returned stream, so a remote source cannot create unbounded state.
type Finder interface {
	FindProviders(ctx context.Context, rendezvous cid.Cid, limit int) (<-chan peer.AddrInfo, error)
}

// Prober obtains positive, peer-targeted challenge proofs. Implementations
// MUST NOT report a challenge as true when its block was fetched from some
// other connected peer. They must honor ctx.
type Prober interface {
	Probe(ctx context.Context, provider peer.AddrInfo, challenges ChallengeSet) (ProbeResult, error)
}

// ChallengeSet is the bounded set of blocks one provider must prove it can
// serve. Current establishes current usefulness. Historical samples establish
// archive depth without enumerating the archive.
type ChallengeSet struct {
	Current    cid.Cid
	Historical []cid.Cid
}

// ConnectionPath describes the transport path positively observed by the
// target-specific prober. Unknown is valid when an adapter cannot distinguish
// the path; it must not guess.
type ConnectionPath string

const (
	PathUnknown ConnectionPath = "unknown"
	PathDirect  ConnectionPath = "direct"
	PathRelay   ConnectionPath = "relay"
)

// ProbeResult records only positive proofs returned by a Prober. Historical
// entries correspond positionally to ChallengeSet.Historical. A missing or
// extra result is a failed archive proof.
type ProbeResult struct {
	Reachable    bool
	Current      bool
	Historical   []bool
	Path         ConnectionPath
	DialLatency  time.Duration
	ProbeLatency time.Duration
}

// Limits bounds all untrusted input, local work, and wall-clock time. Zero
// values select conservative defaults.
type Limits struct {
	MaxProviders    int
	MaxAddressBytes int
	Concurrency     int
	MaxHistorical   int

	OverallTimeout   time.Duration
	DiscoveryTimeout time.Duration
	ProbeTimeout     time.Duration
}

// Config describes one local census observation.
type Config struct {
	Rendezvous cid.Cid
	Current    cid.Cid
	Historical []cid.Cid
	Finder     Finder
	Prober     Prober
	Limits     Limits

	// IncludePeers opts in to raw per-peer output. Aggregate reports omit peer
	// IDs, addresses, and probe errors by default.
	IncludePeers bool

	// Now is an optional test seam for report timestamps.
	Now func() time.Time
}

// LowerBounds contains independently useful positive observations. Providers
// which pass every configured historical sample are also current and reachable;
// a finite sample is deliberately not called a full archive proof.
type LowerBounds struct {
	Observed       int `json:"observed"`
	Reachable      int `json:"reachable"`
	Current        int `json:"current"`
	SampledArchive int `json:"sampled_archive"`
}

// AppliedLimits makes every report self-describing without exposing remote
// addresses or peer identities.
type AppliedLimits struct {
	MaxProviders    int `json:"max_providers"`
	MaxAddressBytes int `json:"max_address_bytes"`
	Concurrency     int `json:"concurrency"`
	MaxHistorical   int `json:"max_historical"`

	OverallTimeoutMS   int64 `json:"overall_timeout_ms"`
	DiscoveryTimeoutMS int64 `json:"discovery_timeout_ms"`
	ProbeTimeoutMS     int64 `json:"probe_timeout_ms"`
}

// PeerState is a raw, opt-in classification for one observed provider.
type PeerState string

const (
	PeerSampledArchive PeerState = "sampled-archive"
	PeerCurrentOnly    PeerState = "current-only"
	PeerStale          PeerState = "stale"
	PeerUnreachable    PeerState = "unreachable"
	PeerUnprobed       PeerState = "unprobed"
)

// PeerReport is included only when Config.IncludePeers is true.
type PeerReport struct {
	PeerID             string         `json:"peer_id"`
	Addresses          []string       `json:"addresses,omitempty"`
	AddressesTruncated bool           `json:"addresses_truncated,omitempty"`
	State              PeerState      `json:"state"`
	Reachable          bool           `json:"reachable"`
	Current            bool           `json:"current"`
	HistoricalPassed   int            `json:"historical_passed"`
	HistoricalRequired int            `json:"historical_required"`
	ProbeError         string         `json:"probe_error,omitempty"`
	Path               ConnectionPath `json:"path"`
	DialLatencyMS      int64          `json:"dial_latency_ms"`
	ProbeLatencyMS     int64          `json:"probe_latency_ms"`
}

// Report is a timestamped lower-bound observation from one local vantage.
// Complete means the configured bounded sample finished; it does not turn the
// lower bounds into exact global counts.
type Report struct {
	Version     int       `json:"version"`
	ObservedAt  time.Time `json:"observed_at"`
	CompletedAt time.Time `json:"completed_at"`
	DurationMS  int64     `json:"duration_ms"`

	RendezvousCID      string        `json:"rendezvous_cid"`
	CurrentCID         string        `json:"current_cid"`
	HistoricalRequired int           `json:"historical_required"`
	Limits             AppliedLimits `json:"limits"`
	LowerBounds        LowerBounds   `json:"lower_bounds"`

	AddressBytesAccepted int `json:"address_bytes_accepted"`
	ProbeAttempts        int `json:"probe_attempts"`
	ProbeCompleted       int `json:"probe_completed"`
	ErrorCount           int `json:"error_count"`

	Complete          bool `json:"complete"`
	DiscoveryComplete bool `json:"discovery_complete"`
	ProbeComplete     bool `json:"probe_complete"`
	Truncated         bool `json:"truncated"`
	TimedOut          bool `json:"timed_out"`
	Canceled          bool `json:"canceled"`
	DiscoveryFailed   bool `json:"discovery_failed"`

	Peers []PeerReport `json:"peers,omitempty"`
}

// Inspector is immutable after construction and safe to use for one census at
// a time. Construct another Inspector when challenge CIDs advance.
type Inspector struct {
	rendezvous   cid.Cid
	current      cid.Cid
	historical   []cid.Cid
	finder       Finder
	prober       Prober
	limits       Limits
	includePeers bool
	now          func() time.Time
}

// New validates all limits and challenge CIDs before any discovery occurs.
func New(cfg Config) (*Inspector, error) {
	limits, err := normalizeLimits(cfg.Limits)
	if err != nil {
		return nil, err
	}
	if err := validateInputs(cfg.Rendezvous, cfg.Current, cfg.Historical, limits); err != nil {
		return nil, err
	}
	if cfg.Finder == nil {
		return nil, errors.New("census: Finder is required")
	}
	if cfg.Prober == nil {
		return nil, errors.New("census: Prober is required")
	}
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	return &Inspector{
		rendezvous:   cfg.Rendezvous,
		current:      cfg.Current,
		historical:   slices.Clone(cfg.Historical),
		finder:       cfg.Finder,
		prober:       cfg.Prober,
		limits:       limits,
		includePeers: cfg.IncludePeers,
		now:          now,
	}, nil
}

// ValidateInputs checks every local CID and work bound without requiring or
// opening a transport. Commands use it before Kubo preflight so malformed
// local arguments cannot cause an avoidable network request.
func ValidateInputs(rendezvous, current cid.Cid, historical []cid.Cid, limits Limits) error {
	normalized, err := normalizeLimits(limits)
	if err != nil {
		return err
	}
	return validateInputs(rendezvous, current, historical, normalized)
}

func validateInputs(rendezvous, current cid.Cid, historical []cid.Cid, limits Limits) error {
	if !rendezvous.Defined() {
		return errors.New("census: rendezvous CID is required")
	}
	if !current.Defined() {
		return errors.New("census: current CID is required")
	}
	if rendezvous.Equals(current) {
		return errors.New("census: rendezvous and current challenge CIDs must differ")
	}
	for _, value := range []struct {
		kind string
		cid  cid.Cid
	}{{"rendezvous", rendezvous}, {"current", current}} {
		if len(value.cid.Bytes()) > maxCIDBytes {
			return fmt.Errorf("census: %s CID exceeds %d wire bytes", value.kind, maxCIDBytes)
		}
	}
	if len(historical) == 0 {
		return errors.New("census: at least one historical challenge CID is required")
	}
	if len(historical) > limits.MaxHistorical {
		return fmt.Errorf("census: %d historical challenges exceed limit %d", len(historical), limits.MaxHistorical)
	}
	seen := map[string]struct{}{rendezvous.KeyString(): {}, current.KeyString(): {}}
	for index, challenge := range historical {
		if !challenge.Defined() {
			return fmt.Errorf("census: historical challenge %d is undefined", index)
		}
		if len(challenge.Bytes()) > maxCIDBytes {
			return fmt.Errorf("census: historical challenge %d exceeds %d wire bytes", index, maxCIDBytes)
		}
		key := challenge.KeyString()
		if _, ok := seen[key]; ok {
			return fmt.Errorf("census: historical challenge %d duplicates another challenge", index)
		}
		seen[key] = struct{}{}
	}
	return nil
}

func normalizeLimits(limits Limits) (Limits, error) {
	if limits.MaxProviders == 0 {
		limits.MaxProviders = DefaultMaxProviders
	}
	if limits.MaxAddressBytes == 0 {
		limits.MaxAddressBytes = DefaultMaxAddressBytes
	}
	if limits.Concurrency == 0 {
		limits.Concurrency = DefaultConcurrency
	}
	if limits.MaxHistorical == 0 {
		limits.MaxHistorical = DefaultMaxHistorical
	}
	if limits.OverallTimeout == 0 {
		limits.OverallTimeout = DefaultOverallTimeout
	}
	if limits.DiscoveryTimeout == 0 {
		limits.DiscoveryTimeout = DefaultDiscoveryTimeout
	}
	if limits.ProbeTimeout == 0 {
		limits.ProbeTimeout = DefaultProbeTimeout
	}
	checks := []struct {
		name string
		got  int
		max  int
	}{
		{"MaxProviders", limits.MaxProviders, hardMaxProviders},
		{"MaxAddressBytes", limits.MaxAddressBytes, hardMaxAddressBytes},
		{"Concurrency", limits.Concurrency, hardMaxConcurrency},
		{"MaxHistorical", limits.MaxHistorical, hardMaxHistorical},
	}
	for _, check := range checks {
		if check.got <= 0 || check.got > check.max {
			return Limits{}, fmt.Errorf("census: %s must be between 1 and %d", check.name, check.max)
		}
	}
	durations := []struct {
		name string
		got  time.Duration
	}{
		{"OverallTimeout", limits.OverallTimeout},
		{"DiscoveryTimeout", limits.DiscoveryTimeout},
		{"ProbeTimeout", limits.ProbeTimeout},
	}
	for _, check := range durations {
		if check.got <= 0 || check.got > hardMaxTimeout {
			return Limits{}, fmt.Errorf("census: %s must be between 1ns and %s", check.name, hardMaxTimeout)
		}
	}
	if limits.DiscoveryTimeout > limits.OverallTimeout {
		return Limits{}, errors.New("census: DiscoveryTimeout must not exceed OverallTimeout")
	}
	if limits.ProbeTimeout > limits.OverallTimeout {
		return Limits{}, errors.New("census: ProbeTimeout must not exceed OverallTimeout")
	}
	return limits, nil
}

type observedProvider struct {
	info               peer.AddrInfo
	addressKeys        map[string]struct{}
	addressesTruncated bool
}

type probeObservation struct {
	id        peer.ID
	attempted bool
	result    ProbeResult
	err       error
}

// Inspect performs one bounded observation. Operational failures are recorded
// in the returned report so a caller can still emit useful aggregate metrics.
func (inspector *Inspector) Inspect(ctx context.Context) Report {
	startedWall := time.Now()
	report := inspector.newReport()
	overallCtx, cancelOverall := context.WithTimeout(ctx, inspector.limits.OverallTimeout)
	defer cancelOverall()

	providers, discoveryComplete, truncated, addressBytes, discoveryErr, discoveryDeadline := inspector.discover(overallCtx)
	report.DiscoveryComplete = discoveryComplete
	report.Truncated = truncated
	report.AddressBytesAccepted = addressBytes
	if discoveryErr != nil {
		report.ErrorCount++
		report.DiscoveryFailed = true
	}
	if discoveryDeadline {
		report.TimedOut = true
	}
	report.LowerBounds.Observed = len(providers)

	observations := inspector.probeAll(overallCtx, providers)
	peerReports := make([]PeerReport, 0, len(providers))
	for _, provider := range providers {
		observation, ok := observations[provider.info.ID]
		peerReport := PeerReport{
			PeerID:             provider.info.ID.String(),
			State:              PeerUnprobed,
			Path:               PathUnknown,
			HistoricalRequired: len(inspector.historical),
			AddressesTruncated: provider.addressesTruncated,
		}
		if inspector.includePeers {
			peerReport.Addresses = make([]string, 0, len(provider.info.Addrs))
			for _, address := range provider.info.Addrs {
				peerReport.Addresses = append(peerReport.Addresses, address.String())
			}
			sort.Strings(peerReport.Addresses)
		}
		if ok && observation.attempted {
			report.ProbeAttempts++
			report.ProbeCompleted++
			peerReport = classify(peerReport, observation, len(inspector.historical))
			if observation.err != nil {
				report.ErrorCount++
			}
			if peerReport.Reachable {
				report.LowerBounds.Reachable++
			}
			if peerReport.Current {
				report.LowerBounds.Current++
			}
			if peerReport.State == PeerSampledArchive {
				report.LowerBounds.SampledArchive++
			}
		}
		if inspector.includePeers {
			peerReports = append(peerReports, peerReport)
		}
	}
	if inspector.includePeers {
		sort.Slice(peerReports, func(left, right int) bool {
			return peerReports[left].PeerID < peerReports[right].PeerID
		})
		report.Peers = peerReports
	}

	report.ProbeComplete = report.ProbeCompleted == len(providers)
	if errors.Is(overallCtx.Err(), context.DeadlineExceeded) && !report.ProbeComplete {
		report.TimedOut = true
	}
	if errors.Is(ctx.Err(), context.Canceled) {
		report.Canceled = true
	}
	report.Complete = report.DiscoveryComplete && report.ProbeComplete && !report.TimedOut && !report.Canceled && !report.DiscoveryFailed
	report.CompletedAt = inspector.now().UTC()
	report.DurationMS = time.Since(startedWall).Milliseconds()
	return report
}

func (inspector *Inspector) newReport() Report {
	limits := inspector.limits
	return Report{
		Version:            reportVersion,
		ObservedAt:         inspector.now().UTC(),
		RendezvousCID:      inspector.rendezvous.String(),
		CurrentCID:         inspector.current.String(),
		HistoricalRequired: len(inspector.historical),
		Limits: AppliedLimits{
			MaxProviders:       limits.MaxProviders,
			MaxAddressBytes:    limits.MaxAddressBytes,
			Concurrency:        limits.Concurrency,
			MaxHistorical:      limits.MaxHistorical,
			OverallTimeoutMS:   limits.OverallTimeout.Milliseconds(),
			DiscoveryTimeoutMS: limits.DiscoveryTimeout.Milliseconds(),
			ProbeTimeoutMS:     limits.ProbeTimeout.Milliseconds(),
		},
	}
}

func (inspector *Inspector) discover(ctx context.Context) ([]*observedProvider, bool, bool, int, error, bool) {
	discoveryCtx, cancel := context.WithTimeout(ctx, inspector.limits.DiscoveryTimeout)
	defer cancel()
	stream, err := inspector.finder.FindProviders(discoveryCtx, inspector.rendezvous, inspector.limits.MaxProviders)
	if err != nil {
		return nil, false, false, 0, err, errors.Is(discoveryCtx.Err(), context.DeadlineExceeded)
	}
	if stream == nil {
		return nil, false, false, 0, errors.New("census: Finder returned a nil provider stream"), false
	}

	byID := make(map[peer.ID]*observedProvider)
	ordered := make([]*observedProvider, 0, inspector.limits.MaxProviders)
	addressBytes := 0
	addressValues := 0
	truncated := false
	for {
		select {
		case <-discoveryCtx.Done():
			return ordered, false, truncated, addressBytes, nil, errors.Is(discoveryCtx.Err(), context.DeadlineExceeded)
		case provider, ok := <-stream:
			if !ok {
				return ordered, true, truncated, addressBytes, nil, false
			}
			validatedID, idErr := peer.IDFromBytes([]byte(provider.ID))
			if provider.ID == "" || len(provider.ID) > maxPeerIDBytes || idErr != nil || validatedID != provider.ID {
				truncated = true
				continue
			}
			observed, exists := byID[provider.ID]
			if !exists {
				if len(ordered) >= inspector.limits.MaxProviders {
					return ordered, true, true, addressBytes, nil, false
				}
				observed = &observedProvider{
					info:        peer.AddrInfo{ID: provider.ID},
					addressKeys: make(map[string]struct{}),
				}
				byID[provider.ID] = observed
				ordered = append(ordered, observed)
			}
			for _, address := range provider.Addrs {
				if addressValues >= maxAddressValues {
					observed.addressesTruncated = true
					truncated = true
					break
				}
				addressValues++
				if address == nil {
					continue
				}
				wire := address.Bytes()
				key := string(wire)
				if _, duplicate := observed.addressKeys[key]; duplicate {
					continue
				}
				if len(wire) > inspector.limits.MaxAddressBytes-addressBytes {
					observed.addressesTruncated = true
					truncated = true
					continue
				}
				observed.addressKeys[key] = struct{}{}
				observed.info.Addrs = append(observed.info.Addrs, address)
				addressBytes += len(wire)
			}
			if len(ordered) == inspector.limits.MaxProviders {
				// The configured sample is full. Stop without draining an
				// untrusted stream; the count remains explicitly a lower bound.
				return ordered, true, true, addressBytes, nil, false
			}
		}
	}
}

func (inspector *Inspector) probeAll(ctx context.Context, providers []*observedProvider) map[peer.ID]probeObservation {
	observations := make(map[peer.ID]probeObservation, len(providers))
	if len(providers) == 0 {
		return observations
	}
	jobs := make(chan *observedProvider, len(providers))
	results := make(chan probeObservation, len(providers))
	for _, provider := range providers {
		jobs <- provider
	}
	close(jobs)

	workers := min(inspector.limits.Concurrency, len(providers))
	var wait sync.WaitGroup
	wait.Add(workers)
	for range workers {
		go func() {
			defer wait.Done()
			for provider := range jobs {
				if ctx.Err() != nil {
					results <- probeObservation{id: provider.info.ID}
					continue
				}
				probeCtx, cancel := context.WithTimeout(ctx, inspector.limits.ProbeTimeout)
				challenge := ChallengeSet{Current: inspector.current, Historical: slices.Clone(inspector.historical)}
				result, err := inspector.prober.Probe(probeCtx, cloneAddrInfo(provider.info), challenge)
				cancel()
				if len(result.Historical) != len(inspector.historical) {
					err = errors.Join(err, fmt.Errorf("census: Prober returned %d historical results, want %d", len(result.Historical), len(inspector.historical)))
				}
				switch result.Path {
				case "", PathUnknown:
					result.Path = PathUnknown
				case PathDirect, PathRelay:
				default:
					err = errors.Join(err, fmt.Errorf("census: Prober returned unknown connection path %q", result.Path))
					result.Path = PathUnknown
				}
				if result.DialLatency < 0 || result.ProbeLatency < 0 {
					err = errors.Join(err, errors.New("census: Prober returned a negative latency"))
					result.DialLatency = max(result.DialLatency, 0)
					result.ProbeLatency = max(result.ProbeLatency, 0)
				}
				positiveWhileUnreachable := result.Current
				for index := 0; index < len(inspector.historical) && index < len(result.Historical); index++ {
					positiveWhileUnreachable = positiveWhileUnreachable || result.Historical[index]
				}
				if !result.Reachable && positiveWhileUnreachable {
					err = errors.Join(err, errors.New("census: Prober returned a positive proof for an unreachable peer"))
					result.Current = false
					result.Historical = nil
				}
				if !result.Reachable {
					result.Path = PathUnknown
				}
				results <- probeObservation{id: provider.info.ID, attempted: true, result: result, err: err}
			}
		}()
	}
	wait.Wait()
	close(results)
	for result := range results {
		observations[result.id] = result
	}
	return observations
}

func cloneAddrInfo(info peer.AddrInfo) peer.AddrInfo {
	return peer.AddrInfo{ID: info.ID, Addrs: slices.Clone(info.Addrs)}
}

func classify(report PeerReport, observation probeObservation, historicalRequired int) PeerReport {
	result := observation.result
	report.Reachable = result.Reachable
	report.Current = result.Reachable && result.Current
	report.Path = result.Path
	report.DialLatencyMS = result.DialLatency.Milliseconds()
	report.ProbeLatencyMS = result.ProbeLatency.Milliseconds()
	for index := 0; index < historicalRequired && index < len(result.Historical); index++ {
		if result.Historical[index] {
			report.HistoricalPassed++
		}
	}
	if observation.err != nil {
		report.ProbeError = boundRawString(observation.err.Error(), maxRawErrorBytes)
	}
	switch {
	case !report.Reachable:
		report.State = PeerUnreachable
	case !report.Current:
		report.State = PeerStale
	case observation.err == nil && report.HistoricalPassed == historicalRequired:
		report.State = PeerSampledArchive
	default:
		report.State = PeerCurrentOnly
	}
	return report
}

func boundRawString(value string, limit int) string {
	if len(value) <= limit {
		return value
	}
	end := limit
	for end > 0 && !utf8.ValidString(value[:end]) {
		end--
	}
	return value[:end] + "..."
}
