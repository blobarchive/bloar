package p2p

import (
	"errors"
	"fmt"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	rcmgr "github.com/libp2p/go-libp2p/p2p/host/resource-manager"
	connmgr "github.com/libp2p/go-libp2p/p2p/net/connmgr"
)

// These defaults deliberately pin the resource policy instead of inheriting
// host-sized libp2p defaults. Connection-manager watermarks are pruning
// thresholds, while ResourceManagerConfig is the hard admission boundary.
const (
	DefaultConnectionLowWatermark  = 160
	DefaultConnectionHighWatermark = 192
	DefaultConnectionGracePeriod   = time.Minute

	DefaultResourceMemoryBytes     int64 = 512 << 20
	DefaultResourceFileDescriptors       = 1024
	DefaultResourceConnections           = 256
	// A public DHT listener can legitimately skew almost entirely inbound or
	// outbound. Keep both directional ceilings at the total hard cap so the
	// connection manager's pruning threshold is reached before either
	// directional admission boundary.
	DefaultResourceInboundConnections  = 256
	DefaultResourceOutboundConnections = 256
	DefaultResourceStreams             = 4096
	DefaultResourceInboundStreams      = 2048
	DefaultResourceOutboundStreams     = 3072

	DefaultResourcePeerConnections               = 8
	DefaultResourcePeerInboundConnections        = 8
	DefaultResourcePeerOutboundConnections       = 8
	DefaultResourcePeerStreams                   = 512
	DefaultResourcePeerInboundStreams            = 256
	DefaultResourcePeerOutboundStreams           = 512
	DefaultResourcePeerMemoryBytes         int64 = 128 << 20
	DefaultResourcePeerFileDescriptors           = 16

	// DefaultResourceStaticPeerConnectionHeadroom keeps one complete default
	// per-peer connection allowance available after every configured static or
	// AutoRelay candidate peer has one connection. AutoNAT probes, replacement
	// dials, and other control traffic must not be locked out by that baseline.
	DefaultResourceStaticPeerConnectionHeadroom = DefaultResourcePeerOutboundConnections
)

const staticPeerProtectionTag = "bloar-static-peer"

// ConnectionManagerConfig controls opportunistic connection pruning. A zero
// value means Bloar's pinned defaults. These are not hard connection limits:
// the manager permits bursts beyond HighWatermark and then trims back toward
// LowWatermark after GracePeriod. Protected configured peers (ordinary static
// peers and static AutoRelay candidates) are excluded from that pruning target
// and remain in addition to LowWatermark.
type ConnectionManagerConfig struct {
	LowWatermark  int           `yaml:"low_watermark"`
	HighWatermark int           `yaml:"high_watermark"`
	GracePeriod   time.Duration `yaml:"grace_period"`
}

// ResourceManagerConfig is the hard libp2p admission policy. A zero field uses
// Bloar's pinned default; negative values are invalid. Directional limits and
// their total are simultaneous ceilings (the total need not equal the sum).
//
// The per-peer connection, stream, memory, and file-descriptor ceilings stop
// one remote peer from consuming the whole system budget. Protocol- and
// service-level limits retain a deterministic snapshot of libp2p v0.48's
// defaults, while every allocation is still bounded by these system and peer
// scopes. The pinned per-peer memory and FD defaults are the v0.48 scaled
// values at Bloar's pinned 512 MiB/1024 FD system baseline; they are not
// inherited implicitly at runtime.
type ResourceManagerConfig struct {
	MemoryBytes     int64 `yaml:"memory_bytes"`
	FileDescriptors int   `yaml:"file_descriptors"`

	Connections         int `yaml:"connections"`
	InboundConnections  int `yaml:"inbound_connections"`
	OutboundConnections int `yaml:"outbound_connections"`
	Streams             int `yaml:"streams"`
	InboundStreams      int `yaml:"inbound_streams"`
	OutboundStreams     int `yaml:"outbound_streams"`

	PeerConnections         int   `yaml:"peer_connections"`
	PeerInboundConnections  int   `yaml:"peer_inbound_connections"`
	PeerOutboundConnections int   `yaml:"peer_outbound_connections"`
	PeerStreams             int   `yaml:"peer_streams"`
	PeerInboundStreams      int   `yaml:"peer_inbound_streams"`
	PeerOutboundStreams     int   `yaml:"peer_outbound_streams"`
	PeerMemoryBytes         int64 `yaml:"peer_memory_bytes"`
	PeerFileDescriptors     int   `yaml:"peer_file_descriptors"`
}

type resolvedResourceControls struct {
	connections ConnectionManagerConfig
	resources   ResourceManagerConfig
	protected   map[peer.ID]struct{}
}

// resourceControlOwnership owns the managers' background goroutines until a
// libp2p host has been built successfully. A successful host owns and closes
// both managers; on every construction error Bloar must close them itself.
type resourceControlOwnership struct {
	connections *connmgr.BasicConnMgr
	resources   network.ResourceManager
}

// ValidateResourceControls validates the connection/resource portion of cfg
// without constructing a host. It is the daemon's preflight seam: the same
// parser and resolver used by NewHost reject malformed static peers,
// inconsistent watermarks, and impossible hard budgets before any listener,
// identity file, or manager goroutine is created.
func ValidateResourceControls(cfg HostConfig) error {
	peers, err := parsePeers(cfg.Peers)
	if err != nil {
		return err
	}
	// Static AutoRelay candidates become protected connections while acquiring
	// and holding reservations. Count them in the same pruning/headroom
	// invariant as ordinary configured peers; the map in the resolver dedupes a
	// peer that appears in both lists.
	peers = append(peers, cfg.Relay.StaticRelays...)
	_, err = resolveResourceControls(cfg.ConnectionManager, cfg.ResourceManager, peers)
	return err
}

func (o *resourceControlOwnership) close() error {
	if o == nil {
		return nil
	}
	connections, resources := o.connections, o.resources
	// Clear first so repeated Bloar cleanup remains idempotent. The pinned
	// manager implementations also tolerate an upstream close before this one.
	o.connections, o.resources = nil, nil

	var err error
	if connections != nil {
		err = errors.Join(err, connections.Close())
	}
	if resources != nil {
		err = errors.Join(err, resources.Close())
	}
	return err
}

func (o *resourceControlOwnership) transferToHost() {
	if o != nil {
		o.connections, o.resources = nil, nil
	}
}

// resourceControlOptions validates and constructs both independent layers:
// connmgr prunes healthy-but-excess connections, while rcmgr refuses resource
// allocations at hard ceilings. Static peers are protected only from pruning;
// they do not bypass hard limits.
func resourceControlOptions(cfg HostConfig, peers []peer.AddrInfo) ([]libp2p.Option, resolvedResourceControls, *resourceControlOwnership, error) {
	resolved, err := resolveResourceControls(cfg.ConnectionManager, cfg.ResourceManager, peers)
	if err != nil {
		return nil, resolvedResourceControls{}, nil, err
	}

	cm, err := connmgr.NewConnManager(
		resolved.connections.LowWatermark,
		resolved.connections.HighWatermark,
		connmgr.WithGracePeriod(resolved.connections.GracePeriod),
	)
	if err != nil {
		return nil, resolvedResourceControls{}, nil, fmt.Errorf("p2p: building connection manager: %w", err)
	}
	for id := range resolved.protected {
		cm.Protect(id, staticPeerProtectionTag)
	}

	limiter := resourceLimiter(resolved.resources)
	rmOpts := make([]rcmgr.Option, 0, 1)
	if cfg.Metrics == nil {
		// rcmgr otherwise installs its Prometheus trace reporter even when the
		// surrounding libp2p host has metrics disabled.
		rmOpts = append(rmOpts, rcmgr.WithMetricsDisabled())
	}
	rm, err := rcmgr.NewResourceManager(limiter, rmOpts...)
	if err != nil {
		closeErr := cm.Close()
		return nil, resolvedResourceControls{}, nil, errors.Join(
			fmt.Errorf("p2p: building resource manager: %w", err),
			closeErr,
		)
	}
	ownership := &resourceControlOwnership{connections: cm, resources: rm}

	opts := []libp2p.Option{
		libp2p.ConnectionManager(cm),
		libp2p.ResourceManager(rm),
	}
	if cfg.Metrics == nil {
		opts = append(opts, libp2p.DisableMetrics())
	} else {
		// Route collectors that honor libp2p's registerer option to the daemon
		// registry without rewriting prometheus.DefaultRegisterer. In v0.48 the
		// auxiliary AutoNATv2 host still installs eventbus's fixed five-collector
		// set in the process default registry; there is no supported option to
		// redirect it. Those collectors have package-global values, as do the
		// same collector instances when exposed through separate daemon
		// registries, so registry selection is not per-host value isolation.
		opts = append(opts, libp2p.PrometheusRegisterer(cfg.Metrics.Registry()))
	}
	return opts, resolved, ownership, nil
}

// constructLibp2pHost transfers manager cleanup to the returned host only after
// construction succeeds. libp2p may reject an option before it has installed
// lifecycle hooks, so relying on a failed constructor to close caller-created
// managers leaks their background goroutines.
func constructLibp2pHost(opts []libp2p.Option, ownership *resourceControlOwnership) (host.Host, error) {
	h, err := libp2p.New(opts...)
	if err != nil {
		return nil, errors.Join(err, ownership.close())
	}
	ownership.transferToHost()
	return h, nil
}

func resolveResourceControls(connections ConnectionManagerConfig, resources ResourceManagerConfig, peers []peer.AddrInfo) (resolvedResourceControls, error) {
	var err error
	if connections.LowWatermark, err = positiveOrDefault("p2p.connection_manager.low_watermark", connections.LowWatermark, DefaultConnectionLowWatermark); err != nil {
		return resolvedResourceControls{}, err
	}
	if connections.HighWatermark, err = positiveOrDefault("p2p.connection_manager.high_watermark", connections.HighWatermark, DefaultConnectionHighWatermark); err != nil {
		return resolvedResourceControls{}, err
	}
	switch {
	case connections.GracePeriod < 0:
		return resolvedResourceControls{}, fmt.Errorf("p2p: p2p.connection_manager.grace_period must be positive")
	case connections.GracePeriod == 0:
		connections.GracePeriod = DefaultConnectionGracePeriod
	}
	if connections.LowWatermark > connections.HighWatermark {
		return resolvedResourceControls{}, fmt.Errorf("p2p: p2p.connection_manager.low_watermark (%d) exceeds high_watermark (%d)", connections.LowWatermark, connections.HighWatermark)
	}

	if resources.MemoryBytes, err = positive64OrDefault("p2p.resource_manager.memory_bytes", resources.MemoryBytes, DefaultResourceMemoryBytes); err != nil {
		return resolvedResourceControls{}, err
	}
	if resources.PeerMemoryBytes, err = positive64OrDefault("p2p.resource_manager.peer_memory_bytes", resources.PeerMemoryBytes, DefaultResourcePeerMemoryBytes); err != nil {
		return resolvedResourceControls{}, err
	}
	fields := []struct {
		name     string
		value    *int
		fallback int
	}{
		{"file_descriptors", &resources.FileDescriptors, DefaultResourceFileDescriptors},
		{"connections", &resources.Connections, DefaultResourceConnections},
		{"inbound_connections", &resources.InboundConnections, DefaultResourceInboundConnections},
		{"outbound_connections", &resources.OutboundConnections, DefaultResourceOutboundConnections},
		{"streams", &resources.Streams, DefaultResourceStreams},
		{"inbound_streams", &resources.InboundStreams, DefaultResourceInboundStreams},
		{"outbound_streams", &resources.OutboundStreams, DefaultResourceOutboundStreams},
		{"peer_connections", &resources.PeerConnections, DefaultResourcePeerConnections},
		{"peer_inbound_connections", &resources.PeerInboundConnections, DefaultResourcePeerInboundConnections},
		{"peer_outbound_connections", &resources.PeerOutboundConnections, DefaultResourcePeerOutboundConnections},
		{"peer_streams", &resources.PeerStreams, DefaultResourcePeerStreams},
		{"peer_inbound_streams", &resources.PeerInboundStreams, DefaultResourcePeerInboundStreams},
		{"peer_outbound_streams", &resources.PeerOutboundStreams, DefaultResourcePeerOutboundStreams},
		{"peer_file_descriptors", &resources.PeerFileDescriptors, DefaultResourcePeerFileDescriptors},
	}
	for _, field := range fields {
		*field.value, err = positiveOrDefault("p2p.resource_manager."+field.name, *field.value, field.fallback)
		if err != nil {
			return resolvedResourceControls{}, err
		}
	}

	if err := validateDirectionalLimits("connections", resources.Connections, resources.InboundConnections, resources.OutboundConnections); err != nil {
		return resolvedResourceControls{}, err
	}
	if err := validateDirectionalLimits("streams", resources.Streams, resources.InboundStreams, resources.OutboundStreams); err != nil {
		return resolvedResourceControls{}, err
	}
	if err := validateDirectionalLimits("peer_connections", resources.PeerConnections, resources.PeerInboundConnections, resources.PeerOutboundConnections); err != nil {
		return resolvedResourceControls{}, err
	}
	if err := validateDirectionalLimits("peer_streams", resources.PeerStreams, resources.PeerInboundStreams, resources.PeerOutboundStreams); err != nil {
		return resolvedResourceControls{}, err
	}
	if resources.PeerConnections > resources.Connections ||
		resources.PeerInboundConnections > resources.InboundConnections ||
		resources.PeerOutboundConnections > resources.OutboundConnections {
		return resolvedResourceControls{}, fmt.Errorf("p2p: p2p.resource_manager per-peer connection limits must not exceed their system limits")
	}
	if resources.PeerStreams > resources.Streams ||
		resources.PeerInboundStreams > resources.InboundStreams ||
		resources.PeerOutboundStreams > resources.OutboundStreams {
		return resolvedResourceControls{}, fmt.Errorf("p2p: p2p.resource_manager per-peer stream limits must not exceed their system limits")
	}
	if resources.PeerMemoryBytes > resources.MemoryBytes {
		return resolvedResourceControls{}, fmt.Errorf("p2p: p2p.resource_manager.peer_memory_bytes (%d) exceeds memory_bytes (%d)", resources.PeerMemoryBytes, resources.MemoryBytes)
	}
	if resources.PeerFileDescriptors > resources.FileDescriptors {
		return resolvedResourceControls{}, fmt.Errorf("p2p: p2p.resource_manager.peer_file_descriptors (%d) exceeds file_descriptors (%d)", resources.PeerFileDescriptors, resources.FileDescriptors)
	}
	if resources.FileDescriptors < resources.Connections {
		return resolvedResourceControls{}, fmt.Errorf("p2p: p2p.resource_manager.file_descriptors (%d) is below connections (%d)", resources.FileDescriptors, resources.Connections)
	}
	if connections.HighWatermark > resources.Connections {
		return resolvedResourceControls{}, fmt.Errorf("p2p: connection-manager high_watermark (%d) exceeds the hard resource-manager connection limit (%d)", connections.HighWatermark, resources.Connections)
	}
	protected := make(map[peer.ID]struct{}, len(peers))
	for _, ai := range peers {
		protected[ai.ID] = struct{}{}
	}
	if len(protected) > 0 {
		// BasicConnMgr's low watermark counts only prunable connections;
		// protected connections survive in addition to it. Leave at least one
		// connection between that steady-state floor and the high watermark so a
		// trim can actually bring the total below the next trigger. Unique peers
		// are only a lower bound (one peer may hold several connections), but
		// rejecting the configured impossible case is still strictly safer than
		// accepting it.
		minimumHigh := connections.LowWatermark + len(protected) + 1
		if connections.HighWatermark < minimumHigh {
			return resolvedResourceControls{}, fmt.Errorf(
				"p2p: connection-manager high_watermark (%d) must be at least %d for low_watermark %d plus %d protected configured peers and one pruning slot",
				connections.HighWatermark, minimumHigh, connections.LowWatermark, len(protected),
			)
		}
		required := len(protected) + DefaultResourceStaticPeerConnectionHeadroom
		if required > resources.OutboundConnections || required > resources.Connections {
			return resolvedResourceControls{}, fmt.Errorf(
				"p2p: %d unique protected configured peers require hard outbound and system connection limits of at least %d to leave %d connections of control-plane headroom",
				len(protected), required, DefaultResourceStaticPeerConnectionHeadroom,
			)
		}
	}

	return resolvedResourceControls{connections: connections, resources: resources, protected: protected}, nil
}

func resourceLimiter(cfg ResourceManagerConfig) rcmgr.Limiter {
	// Start with a deterministic snapshot of the upstream protocol/service
	// defaults. The system and peer scopes below are Bloar-owned hard caps and
	// dominate every nested scope.
	defaults := rcmgr.DefaultLimits
	libp2p.SetDefaultServiceLimits(&defaults)
	base := defaults.Scale(DefaultResourceMemoryBytes, DefaultResourceFileDescriptors)
	partial := rcmgr.PartialLimitConfig{
		System: rcmgr.ResourceLimits{
			Memory:          rcmgr.LimitVal64(cfg.MemoryBytes),
			FD:              rcmgr.LimitVal(cfg.FileDescriptors),
			Conns:           rcmgr.LimitVal(cfg.Connections),
			ConnsInbound:    rcmgr.LimitVal(cfg.InboundConnections),
			ConnsOutbound:   rcmgr.LimitVal(cfg.OutboundConnections),
			Streams:         rcmgr.LimitVal(cfg.Streams),
			StreamsInbound:  rcmgr.LimitVal(cfg.InboundStreams),
			StreamsOutbound: rcmgr.LimitVal(cfg.OutboundStreams),
		},
		PeerDefault: rcmgr.ResourceLimits{
			Conns:           rcmgr.LimitVal(cfg.PeerConnections),
			ConnsInbound:    rcmgr.LimitVal(cfg.PeerInboundConnections),
			ConnsOutbound:   rcmgr.LimitVal(cfg.PeerOutboundConnections),
			Streams:         rcmgr.LimitVal(cfg.PeerStreams),
			StreamsInbound:  rcmgr.LimitVal(cfg.PeerInboundStreams),
			StreamsOutbound: rcmgr.LimitVal(cfg.PeerOutboundStreams),
			Memory:          rcmgr.LimitVal64(cfg.PeerMemoryBytes),
			FD:              rcmgr.LimitVal(cfg.PeerFileDescriptors),
		},
	}
	return rcmgr.NewFixedLimiter(partial.Build(base))
}

func positiveOrDefault(name string, value, fallback int) (int, error) {
	switch {
	case value < 0:
		return 0, fmt.Errorf("p2p: %s must be positive", name)
	case value == 0:
		return fallback, nil
	default:
		return value, nil
	}
}

func positive64OrDefault(name string, value, fallback int64) (int64, error) {
	switch {
	case value < 0:
		return 0, fmt.Errorf("p2p: %s must be positive", name)
	case value == 0:
		return fallback, nil
	default:
		return value, nil
	}
}

func validateDirectionalLimits(name string, total, inbound, outbound int) error {
	if total < inbound || total < outbound {
		return fmt.Errorf("p2p: p2p.resource_manager.%s (%d) must be at least its inbound (%d) and outbound (%d) limits", name, total, inbound, outbound)
	}
	return nil
}
