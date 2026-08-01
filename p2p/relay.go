package p2p

import (
	"fmt"
	"math"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/host/autorelay"
	relayv2 "github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/relay"
	ma "github.com/multiformats/go-multiaddr"
)

// Bloar pins the relay service and AutoRelay policy rather than inheriting
// go-libp2p defaults. Relay circuits are a bounded rendezvous/DCUtR control
// plane, not a fallback data plane: 128 KiB is exactly one Bloar blob's raw
// payload and therefore cannot carry that blob plus Bitswap framing.
const (
	DefaultRelayReservationTTL              = time.Hour
	DefaultRelayMaxReservations             = 32
	DefaultRelayMaxCircuitsPerPeer          = 4
	DefaultRelayBufferSizeBytes             = 2 << 10
	DefaultRelayMaxReservationsPerIP        = 8
	DefaultRelayMaxReservationsPerASN       = 16
	DefaultRelayCircuitDuration             = 2 * time.Minute
	DefaultRelayCircuitDataBytes      int64 = 128 << 10

	DefaultAutoRelayDesiredReservations = 2
	DefaultAutoRelayMinInterval         = 30 * time.Second
	DefaultAutoRelayBootDelay           = 30 * time.Second
	DefaultAutoRelayBackoff             = 5 * time.Minute
	DefaultAutoRelayMaxCandidateAge     = 30 * time.Minute
)

// RelayServiceConfig bounds the circuit-v2 service offered while libp2p
// observes this host as publicly reachable. Zero fields select Bloar's pinned
// limits; negative fields and inconsistent nested limits are rejected before
// a host is constructed.
type RelayServiceConfig struct {
	ReservationTTL        time.Duration
	MaxReservations       int
	MaxCircuitsPerPeer    int
	BufferSizeBytes       int
	MaxReservationsPerIP  int
	MaxReservationsPerASN int
	CircuitDuration       time.Duration
	CircuitDataBytes      int64
}

// AutoRelayConfig bounds reservation work against a configured, static relay
// set. It has no effect when RelayConfig.StaticRelays is empty: Bloar does not
// silently enable public candidate discovery.
type AutoRelayConfig struct {
	DesiredReservations int
	MinInterval         time.Duration
	BootDelay           time.Duration
	Backoff             time.Duration
	MaxCandidateAge     time.Duration
}

// RelayConfig is the isolated relay/DCUtR portion of a libp2p host plan.
// Its zero value is deliberately inert. StaticRelays enables AutoRelay only
// when the slice is non-empty; candidates are direct addresses and never relay
// circuits themselves.
type RelayConfig struct {
	EnableService      bool
	EnableHolePunching bool
	StaticRelays       []peer.AddrInfo
	Service            RelayServiceConfig
	AutoRelay          AutoRelayConfig
}

// DefaultRelayConfig is the daemon-friendly enabled policy: publicly
// reachable nodes offer the bounded relay-v2 service, and every node can
// participate in DCUtR. AutoRelay remains off until the caller supplies at
// least one static relay candidate.
func DefaultRelayConfig() RelayConfig {
	return RelayConfig{
		EnableService:      true,
		EnableHolePunching: true,
	}
}

type resolvedRelayConfig struct {
	serviceEnabled      bool
	holePunchingEnabled bool
	service             relayv2.Resources
	staticRelays        []peer.AddrInfo
	desiredReservations int
	minInterval         time.Duration
	bootDelay           time.Duration
	backoff             time.Duration
	maxCandidateAge     time.Duration
}

// RelayOptions validates cfg and translates it to go-libp2p v0.48 host
// options. The caller retains ownership of the surrounding host lifecycle.
//
// Metrics intentionally do not appear in this API. In v0.48 libp2p injects
// relay-service, AutoRelay, and hole-punch tracers from the host's
// PrometheusRegisterer (and suppresses them with DisableMetrics). This lets
// NewHost's existing private-registry seam govern these subsystems too,
// without an explicit tracer overriding or double-registering it.
func RelayOptions(cfg RelayConfig) ([]libp2p.Option, error) {
	resolved, err := resolveRelayConfig(cfg)
	if err != nil {
		return nil, err
	}
	if !resolved.serviceEnabled && !resolved.holePunchingEnabled && len(resolved.staticRelays) == 0 {
		return nil, nil
	}

	// Pin the relay transport on whenever any relay/DCUtR feature is active,
	// instead of depending on libp2p's current default-enabled transport.
	opts := []libp2p.Option{libp2p.EnableRelay()}
	if resolved.serviceEnabled {
		// EnableRelayService is reachability-gated inside libp2p's RelayManager:
		// it constructs the service only for EvtLocalReachabilityChanged(Public)
		// and closes it again for Private or Unknown. We never force a verdict.
		opts = append(opts, libp2p.EnableRelayService(
			relayv2.WithResources(resolved.service),
		))
	}
	if len(resolved.staticRelays) > 0 {
		opts = append(opts, libp2p.EnableAutoRelayWithStaticRelays(
			resolved.staticRelays,
			autorelay.WithNumRelays(resolved.desiredReservations),
			autorelay.WithMinInterval(resolved.minInterval),
			autorelay.WithBootDelay(resolved.bootDelay),
			autorelay.WithBackoff(resolved.backoff),
			autorelay.WithMaxCandidateAge(resolved.maxCandidateAge),
		))
	}
	if resolved.holePunchingEnabled {
		// DCUtR is the only intended consumer of the limited relay circuit.
		// Bitswap is deliberately left on libp2p's normal (non-limited)
		// connection context; RelayOptions never calls WithAllowLimitedConn.
		opts = append(opts, libp2p.EnableHolePunching())
	}
	return opts, nil
}

func resolveRelayConfig(cfg RelayConfig) (resolvedRelayConfig, error) {
	resolved := resolvedRelayConfig{
		serviceEnabled:      cfg.EnableService,
		holePunchingEnabled: cfg.EnableHolePunching,
	}

	staticRelays, err := normalizeStaticRelays(cfg.StaticRelays)
	if err != nil {
		return resolvedRelayConfig{}, err
	}
	resolved.staticRelays = staticRelays

	if cfg.EnableService {
		resources, err := resolveRelayServiceConfig(cfg.Service)
		if err != nil {
			return resolvedRelayConfig{}, err
		}
		resolved.service = resources
	}
	if len(staticRelays) > 0 {
		auto, err := resolveAutoRelayConfig(cfg.AutoRelay, len(staticRelays))
		if err != nil {
			return resolvedRelayConfig{}, err
		}
		resolved.desiredReservations = auto.desiredReservations
		resolved.minInterval = auto.minInterval
		resolved.bootDelay = auto.bootDelay
		resolved.backoff = auto.backoff
		resolved.maxCandidateAge = auto.maxCandidateAge
	}
	return resolved, nil
}

func resolveRelayServiceConfig(cfg RelayServiceConfig) (relayv2.Resources, error) {
	reservationTTL, err := relayDurationOrDefault("ReservationTTL", cfg.ReservationTTL, DefaultRelayReservationTTL)
	if err != nil {
		return relayv2.Resources{}, err
	}
	maxReservations, err := relayIntOrDefault("MaxReservations", cfg.MaxReservations, DefaultRelayMaxReservations)
	if err != nil {
		return relayv2.Resources{}, err
	}
	maxCircuits, err := relayIntOrDefault("MaxCircuitsPerPeer", cfg.MaxCircuitsPerPeer, DefaultRelayMaxCircuitsPerPeer)
	if err != nil {
		return relayv2.Resources{}, err
	}
	bufferSize, err := relayIntOrDefault("BufferSizeBytes", cfg.BufferSizeBytes, DefaultRelayBufferSizeBytes)
	if err != nil {
		return relayv2.Resources{}, err
	}
	perIP, err := relayIntOrDefault("MaxReservationsPerIP", cfg.MaxReservationsPerIP, DefaultRelayMaxReservationsPerIP)
	if err != nil {
		return relayv2.Resources{}, err
	}
	perASN, err := relayIntOrDefault("MaxReservationsPerASN", cfg.MaxReservationsPerASN, DefaultRelayMaxReservationsPerASN)
	if err != nil {
		return relayv2.Resources{}, err
	}
	circuitDuration, err := relayDurationOrDefault("CircuitDuration", cfg.CircuitDuration, DefaultRelayCircuitDuration)
	if err != nil {
		return relayv2.Resources{}, err
	}
	if circuitDuration < time.Second || circuitDuration%time.Second != 0 {
		return relayv2.Resources{}, fmt.Errorf("p2p: RelayServiceConfig.CircuitDuration must be a positive whole number of seconds")
	}
	if circuitDuration/time.Second > time.Duration(math.MaxUint32) {
		return relayv2.Resources{}, fmt.Errorf("p2p: RelayServiceConfig.CircuitDuration exceeds the circuit-v2 uint32 wire limit")
	}
	circuitBytes, err := relayInt64OrDefault("CircuitDataBytes", cfg.CircuitDataBytes, DefaultRelayCircuitDataBytes)
	if err != nil {
		return relayv2.Resources{}, err
	}
	if perIP > maxReservations {
		return relayv2.Resources{}, fmt.Errorf("p2p: RelayServiceConfig.MaxReservationsPerIP (%d) exceeds MaxReservations (%d)", perIP, maxReservations)
	}
	if perASN > maxReservations {
		return relayv2.Resources{}, fmt.Errorf("p2p: RelayServiceConfig.MaxReservationsPerASN (%d) exceeds MaxReservations (%d)", perASN, maxReservations)
	}

	return relayv2.Resources{
		Limit: &relayv2.RelayLimit{
			Duration: circuitDuration,
			Data:     circuitBytes,
		},
		ReservationTTL:         reservationTTL,
		MaxReservations:        maxReservations,
		MaxCircuits:            maxCircuits,
		BufferSize:             bufferSize,
		MaxReservationsPerPeer: 1,
		MaxReservationsPerIP:   perIP,
		MaxReservationsPerASN:  perASN,
	}, nil
}

type resolvedAutoRelayConfig struct {
	desiredReservations int
	minInterval         time.Duration
	bootDelay           time.Duration
	backoff             time.Duration
	maxCandidateAge     time.Duration
}

func resolveAutoRelayConfig(cfg AutoRelayConfig, candidates int) (resolvedAutoRelayConfig, error) {
	desired, err := relayIntOrDefault("AutoRelay.DesiredReservations", cfg.DesiredReservations, DefaultAutoRelayDesiredReservations)
	if err != nil {
		return resolvedAutoRelayConfig{}, err
	}
	if cfg.DesiredReservations == 0 && desired > candidates {
		desired = candidates
	}
	if desired > candidates {
		return resolvedAutoRelayConfig{}, fmt.Errorf("p2p: AutoRelayConfig.DesiredReservations (%d) exceeds the %d static relay candidates", desired, candidates)
	}
	minInterval, err := relayDurationOrDefault("AutoRelay.MinInterval", cfg.MinInterval, DefaultAutoRelayMinInterval)
	if err != nil {
		return resolvedAutoRelayConfig{}, err
	}
	bootDelay, err := relayDurationOrDefault("AutoRelay.BootDelay", cfg.BootDelay, DefaultAutoRelayBootDelay)
	if err != nil {
		return resolvedAutoRelayConfig{}, err
	}
	backoff, err := relayDurationOrDefault("AutoRelay.Backoff", cfg.Backoff, DefaultAutoRelayBackoff)
	if err != nil {
		return resolvedAutoRelayConfig{}, err
	}
	maxCandidateAge, err := relayDurationOrDefault("AutoRelay.MaxCandidateAge", cfg.MaxCandidateAge, DefaultAutoRelayMaxCandidateAge)
	if err != nil {
		return resolvedAutoRelayConfig{}, err
	}
	if maxCandidateAge < minInterval {
		return resolvedAutoRelayConfig{}, fmt.Errorf("p2p: AutoRelayConfig.MaxCandidateAge (%s) is below MinInterval (%s)", maxCandidateAge, minInterval)
	}
	return resolvedAutoRelayConfig{
		desiredReservations: desired,
		minInterval:         minInterval,
		bootDelay:           bootDelay,
		backoff:             backoff,
		maxCandidateAge:     maxCandidateAge,
	}, nil
}

func normalizeStaticRelays(in []peer.AddrInfo) ([]peer.AddrInfo, error) {
	if len(in) == 0 {
		return nil, nil
	}
	out := make([]peer.AddrInfo, 0, len(in))
	seenPeers := make(map[peer.ID]struct{}, len(in))
	for i, candidate := range in {
		if _, err := peer.IDFromBytes([]byte(candidate.ID)); err != nil {
			return nil, fmt.Errorf("p2p: RelayConfig.StaticRelays[%d] has an invalid peer ID: %w", i, err)
		}
		if _, exists := seenPeers[candidate.ID]; exists {
			return nil, fmt.Errorf("p2p: RelayConfig.StaticRelays[%d] duplicates peer %s", i, candidate.ID)
		}
		seenPeers[candidate.ID] = struct{}{}
		if len(candidate.Addrs) == 0 {
			return nil, fmt.Errorf("p2p: RelayConfig.StaticRelays[%d] has no direct addresses", i)
		}

		addresses := make([]ma.Multiaddr, 0, len(candidate.Addrs))
		seenAddrs := make(map[string]struct{}, len(candidate.Addrs))
		for j, address := range candidate.Addrs {
			if address == nil {
				return nil, fmt.Errorf("p2p: RelayConfig.StaticRelays[%d].Addrs[%d] is nil", i, j)
			}
			if multiaddrHasProtocol(address, ma.P_CIRCUIT, ma.P_P2P) {
				return nil, fmt.Errorf("p2p: RelayConfig.StaticRelays[%d].Addrs[%d] must be a direct transport address without /p2p or /p2p-circuit", i, j)
			}
			key := string(address.Bytes())
			if _, exists := seenAddrs[key]; exists {
				continue
			}
			seenAddrs[key] = struct{}{}
			copyAddress, err := ma.NewMultiaddrBytes(address.Bytes())
			if err != nil {
				return nil, fmt.Errorf("p2p: copying RelayConfig.StaticRelays[%d].Addrs[%d]: %w", i, j, err)
			}
			addresses = append(addresses, copyAddress)
		}
		out = append(out, peer.AddrInfo{ID: candidate.ID, Addrs: addresses})
	}
	return out, nil
}

func relayIntOrDefault(name string, value, fallback int) (int, error) {
	switch {
	case value < 0:
		return 0, fmt.Errorf("p2p: %s must be positive", name)
	case value == 0:
		return fallback, nil
	default:
		return value, nil
	}
}

func relayInt64OrDefault(name string, value, fallback int64) (int64, error) {
	switch {
	case value < 0:
		return 0, fmt.Errorf("p2p: %s must be positive", name)
	case value == 0:
		return fallback, nil
	default:
		return value, nil
	}
}

func relayDurationOrDefault(name string, value, fallback time.Duration) (time.Duration, error) {
	switch {
	case value < 0:
		return 0, fmt.Errorf("p2p: %s must be positive", name)
	case value == 0:
		return fallback, nil
	default:
		return value, nil
	}
}
