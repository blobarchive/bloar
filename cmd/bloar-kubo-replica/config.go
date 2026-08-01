package main

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/blobarchive/bloar/kubo"
	"github.com/blobarchive/bloar/p2p"
	"github.com/blobarchive/bloar/replica"
	"github.com/blobarchive/bloar/schema"
	"github.com/blobarchive/bloar/server"
	"github.com/ipfs/boxo/ipns"
	"golang.org/x/time/rate"
	"gopkg.in/yaml.v3"
)

const maxConfigBytes = 1 << 20

const (
	minimumAuditInterval      = 10 * time.Second
	minimumPollInterval       = 10 * time.Second
	minimumFetchTimeout       = time.Second
	minimumRequestTimeout     = time.Second
	minimumPinTimeout         = time.Minute
	minimumAnnounceInterval   = time.Minute
	maximumMutableWindowSlots = 4096

	defaultGatewayListen                    = "127.0.0.1:8550"
	defaultGatewayMaxQueryHashes            = schema.MaxBlobsPerSlotCeiling
	defaultGatewayResponseBytesInFlight     = 1 << 30
	defaultGatewayImmutableHorizonSlots     = 7200
	defaultGatewayReadHeaderTimeout         = 10 * time.Second
	defaultGatewayReadTimeout               = 15 * time.Second
	defaultGatewayWriteTimeout              = 120 * time.Second
	defaultGatewayIdleTimeout               = 60 * time.Second
	defaultGatewayMaxHeaderBytes            = 64 << 10
	defaultGatewayMaxConns                  = 1024
	defaultGatewayPublicReadGlobalRate      = 4096
	defaultGatewayPublicReadGlobalBurst     = 16384
	defaultGatewayPublicReadClientRate      = 1024
	defaultGatewayPublicReadClientBurst     = 4096
	defaultGatewayPublicReadClientBuckets   = 4096
	defaultGatewayPublicReadClientBucketTTL = 15 * time.Minute

	providerPolicyCheckRuntime  = "runtime"
	providerPolicyCheckExternal = "external"
)

type duration time.Duration

func (d *duration) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind != yaml.ScalarNode || node.Tag != "!!str" {
		return errors.New("duration must be a quoted or plain duration string such as 30s or 24h")
	}
	parsed, err := time.ParseDuration(node.Value)
	if err != nil {
		return err
	}
	*d = duration(parsed)
	return nil
}

func (d duration) value() time.Duration { return time.Duration(d) }

type configVersion int

func (v *configVersion) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind != yaml.ScalarNode || node.Tag != "!!int" {
		return errors.New("version must be an integer")
	}
	var parsed int
	if err := node.Decode(&parsed); err != nil {
		return err
	}
	*v = configVersion(parsed)
	return nil
}

type config struct {
	Version configVersion `yaml:"version"`
	Net     string        `yaml:"net"`
	Replica replicaConfig `yaml:"replica"`
	Source  sourceConfig  `yaml:"source"`
	Kubo    kuboConfig    `yaml:"kubo"`
	Gateway gatewayConfig `yaml:"gateway"`
	Metrics metricsConfig `yaml:"metrics"`
}

type replicaConfig struct {
	ID            string       `yaml:"id"`
	StatePath     string       `yaml:"state_path"`
	PinName       string       `yaml:"pin_name"`
	Heads         replicaHeads `yaml:"heads"`
	AuditInterval duration     `yaml:"audit_interval"`
}

// replicaHeadSelection is one selected publication head. Version 2 requires
// Kind explicitly so a publication cannot opt a replica into mutable revision
// semantics merely by changing a signed document. HandoffHead remains
// authenticated publication metadata unless it is also a key in replica.heads.
type replicaHeadSelection struct {
	Kind                 server.HeadKind `yaml:"kind"`
	HandoffHead          string          `yaml:"handoff_head"`
	MaxWindowSlots       uint64          `yaml:"max_window_slots"`
	OverlayFinalizedHead string          `yaml:"overlay_finalized_head"`
}

// replicaHeads accepts the legacy version-1 sequence and the version-2 map,
// but records their shapes so the top-level version can reject ambiguous
// combinations with an explicit migration error.
type replicaHeads struct {
	set      bool
	sequence bool
	entries  map[string]replicaHeadSelection
}

func (h *replicaHeads) UnmarshalYAML(node *yaml.Node) error {
	h.set = true
	h.entries = make(map[string]replicaHeadSelection)
	switch node.Kind {
	case yaml.SequenceNode:
		h.sequence = true
		for index, value := range node.Content {
			if value.Kind != yaml.ScalarNode || value.Tag != "!!str" {
				return fmt.Errorf("replica.heads[%d] must be a string", index)
			}
		}
		var names []string
		if err := node.Decode(&names); err != nil {
			return fmt.Errorf("replica.heads legacy sequence: %w", err)
		}
		for _, name := range names {
			if _, duplicate := h.entries[name]; duplicate {
				return fmt.Errorf("replica.heads repeats %q", name)
			}
			h.entries[name] = replicaHeadSelection{Kind: server.FinalizedMonotonic}
		}
		return nil
	case yaml.MappingNode:
		h.sequence = false
		for index := 0; index < len(node.Content); index += 2 {
			key, value := node.Content[index], node.Content[index+1]
			if key.Kind != yaml.ScalarNode || key.Tag != "!!str" {
				return errors.New("replica.heads names must be strings")
			}
			name := key.Value
			if _, duplicate := h.entries[name]; duplicate {
				return fmt.Errorf("replica.heads repeats %q", name)
			}
			if value.Kind != yaml.MappingNode {
				return fmt.Errorf("replica.heads.%s must be a mapping", name)
			}
			if err := strictHeadSelectionFields(name, value); err != nil {
				return err
			}
			var selection replicaHeadSelection
			if err := value.Decode(&selection); err != nil {
				return fmt.Errorf("replica.heads.%s: %w", name, err)
			}
			h.entries[name] = selection
		}
		return nil
	default:
		return errors.New("replica.heads must be a sequence in version 1 or a mapping in version 2")
	}
}

func strictHeadSelectionFields(name string, node *yaml.Node) error {
	allowed := map[string]bool{
		"kind": true, "handoff_head": true, "max_window_slots": true, "overlay_finalized_head": true,
	}
	seen := make(map[string]bool)
	for index := 0; index < len(node.Content); index += 2 {
		key, value := node.Content[index], node.Content[index+1]
		if key.Kind != yaml.ScalarNode || key.Tag != "!!str" {
			return fmt.Errorf("replica.heads.%s field names must be strings", name)
		}
		if !allowed[key.Value] {
			return fmt.Errorf("replica.heads.%s contains unknown field %q", name, key.Value)
		}
		if seen[key.Value] {
			return fmt.Errorf("replica.heads.%s repeats field %q", name, key.Value)
		}
		seen[key.Value] = true
		wantTag := "!!str"
		if key.Value == "max_window_slots" {
			wantTag = "!!int"
		}
		if value.Kind != yaml.ScalarNode || value.Tag != wantTag {
			want := "a string"
			if wantTag == "!!int" {
				want = "an integer"
			}
			return fmt.Errorf("replica.heads.%s.%s must be %s", name, key.Value, want)
		}
	}
	return nil
}

func (h replicaHeads) Names() []string {
	names := make([]string, 0, len(h.entries))
	for name := range h.entries {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}

func (h replicaHeads) Selection(name string) (replicaHeadSelection, bool) {
	selection, ok := h.entries[name]
	return selection, ok
}

type sourceConfig struct {
	URL          string   `yaml:"url"`
	IPNS         string   `yaml:"ipns"`
	DNSLink      string   `yaml:"dnslink"`
	PublicKey    string   `yaml:"pubkey"`
	PollInterval duration `yaml:"poll_interval"`
	FetchTimeout duration `yaml:"fetch_timeout"`
}

type kuboConfig struct {
	API                  string   `yaml:"api"`
	BearerTokenFile      string   `yaml:"bearer_token_file"`
	AllowUnauthenticated bool     `yaml:"allow_unauthenticated"`
	AllowInsecureHTTP    bool     `yaml:"allow_insecure_http"`
	ProviderPolicyCheck  string   `yaml:"provider_policy_check"`
	RequestTimeout       duration `yaml:"request_timeout"`
	PinTimeout           duration `yaml:"pin_timeout"`
	MaxStreamItems       int      `yaml:"max_stream_items"`
	MaxStreamBytes       int64    `yaml:"max_stream_bytes"`
	PinProgressItems     int      `yaml:"pin_progress_items"`
	PinProgressBytes     int64    `yaml:"pin_progress_bytes"`
	AnnounceInterval     duration `yaml:"announce_interval"`
}

type metricsConfig struct {
	Listen string `yaml:"listen"`
}

// gatewayConfig is the optional read-only Bloar/beacon API backed by the
// replica's committed Kubo-local generation. The process remains a replica,
// never a writer: enabling this block mounts no mutation routes.
type gatewayConfig struct {
	Enabled bool   `yaml:"enabled"`
	Listen  string `yaml:"listen"`

	Beacon    gatewayBeaconConfig              `yaml:"beacon"`
	LiveHeads map[string]gatewayLiveHeadConfig `yaml:"live_heads"`

	MaxQueryHashes           int      `yaml:"max_query_hashes"`
	MaxResponseBytesInFlight int64    `yaml:"max_response_bytes_in_flight"`
	ImmutableHorizonSlots    uint64   `yaml:"immutable_horizon_slots"`
	ReadHeaderTimeout        duration `yaml:"read_header_timeout"`
	ReadTimeout              duration `yaml:"read_timeout"`
	WriteTimeout             duration `yaml:"write_timeout"`
	IdleTimeout              duration `yaml:"idle_timeout"`
	MaxHeaderBytes           int      `yaml:"max_header_bytes"`
	MaxConns                 int      `yaml:"max_conns"`

	PublicReadAdmission gatewayPublicReadAdmissionConfig `yaml:"public_read_admission"`
}

type gatewayBeaconConfig struct {
	GenesisTime           uint64         `yaml:"genesis_time"`
	SecondsPerSlot        uint64         `yaml:"seconds_per_slot"`
	GenesisValidatorsRoot string         `yaml:"genesis_validators_root"`
	GenesisForkVersion    string         `yaml:"genesis_fork_version"`
	SpecExtra             map[string]any `yaml:"spec_extra"`
}

type gatewayLiveHeadConfig struct {
	FinalizedHead          string `yaml:"finalized_head"`
	UnfinalizedHead        string `yaml:"unfinalized_head"`
	RequireVersionedHashes bool   `yaml:"require_versioned_hashes"`
}

type gatewayPublicReadAdmissionConfig struct {
	Enabled *bool `yaml:"enabled"`

	GlobalRate  float64 `yaml:"global_rate"`
	GlobalBurst int     `yaml:"global_burst"`
	ClientRate  float64 `yaml:"client_rate"`
	ClientBurst int     `yaml:"client_burst"`

	ClientBuckets   int      `yaml:"client_buckets"`
	ClientBucketTTL duration `yaml:"client_bucket_ttl"`

	TrustedProxyHeader string   `yaml:"trusted_proxy_header"`
	TrustedProxyCIDRs  []string `yaml:"trusted_proxy_cidrs"`
}

func loadConfig(path string) (*config, error) {
	if path == "" {
		return nil, errors.New("-config is required")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("opening config: %w", err)
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, maxConfigBytes+1))
	if err != nil {
		return nil, fmt.Errorf("reading config: %w", err)
	}
	if len(data) > maxConfigBytes {
		return nil, fmt.Errorf("config exceeds the %d-byte limit", maxConfigBytes)
	}
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	var cfg config
	if err := decoder.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("decoding config: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, errors.New("config contains more than one YAML document")
		}
		return nil, fmt.Errorf("decoding trailing config: %w", err)
	}
	if err := cfg.defaultsAndValidate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func (c *config) defaultsAndValidate() error {
	if c.Version != 1 && c.Version != 2 {
		return fmt.Errorf("config version is %d, want 1 or 2", c.Version)
	}
	c.Net = strings.TrimSpace(c.Net)
	if c.Net == "" {
		return errors.New("net is required")
	}
	if c.Replica.ID == "" {
		return errors.New("replica.id is required")
	}
	if err := replica.ValidateReplicaID(c.Replica.ID); err != nil {
		return fmt.Errorf("replica.id: %w", err)
	}
	if c.Replica.PinName != "" && (len(c.Replica.PinName) > 128 || strings.IndexByte(c.Replica.PinName, 0) >= 0 || strings.TrimSpace(c.Replica.PinName) != c.Replica.PinName) {
		return errors.New("replica.pin_name must be 1-128 bytes, contain no NUL, and have no surrounding whitespace when set")
	}
	if !filepath.IsAbs(c.Replica.StatePath) {
		return errors.New("replica.state_path must be an absolute path")
	}
	if c.Replica.AuditInterval == 0 {
		c.Replica.AuditInterval = duration(time.Minute)
	}
	if c.Replica.AuditInterval.value() < minimumAuditInterval {
		return fmt.Errorf("replica.audit_interval must be at least %s", minimumAuditInterval)
	}
	if !c.Replica.Heads.set {
		return errors.New("replica.heads is required")
	}
	if c.Version == 1 && !c.Replica.Heads.sequence {
		return errors.New("config version 1 requires replica.heads as a flat sequence; use version 2 for structured selections")
	}
	if c.Version == 2 && c.Replica.Heads.sequence {
		return errors.New("config version 2 requires replica.heads as a structured mapping; keep version 1 for a legacy flat sequence")
	}
	headNames := c.Replica.Heads.Names()
	if len(headNames) == 0 || len(headNames) > 64 {
		return errors.New("replica.heads must contain between 1 and 64 names")
	}
	for _, name := range headNames {
		if strings.TrimSpace(name) != name {
			return fmt.Errorf("replica.heads.%s must have no surrounding whitespace", name)
		}
		if len(name) > 128 {
			return fmt.Errorf("replica head %q must be at most 128 bytes", name)
		}
		if err := schema.ValidateHeadName(name); err != nil {
			return fmt.Errorf("replica.heads.%s: %w", name, err)
		}
		selection, _ := c.Replica.Heads.Selection(name)
		switch selection.Kind {
		case server.FinalizedMonotonic:
			if selection.HandoffHead != "" || selection.MaxWindowSlots != 0 || selection.OverlayFinalizedHead != "" {
				return fmt.Errorf("replica.heads.%s is finalized-monotonic but carries mutable handoff/window/overlay fields", name)
			}
		case server.UnfinalizedMutable:
			if selection.HandoffHead == "" {
				return fmt.Errorf("replica.heads.%s.handoff_head is required for unfinalized-mutable", name)
			}
			if err := schema.ValidateHeadName(selection.HandoffHead); err != nil {
				return fmt.Errorf("replica.heads.%s.handoff_head: %w", name, err)
			}
			if selection.HandoffHead == name {
				return fmt.Errorf("replica.heads.%s.handoff_head cannot name itself", name)
			}
			if selection.MaxWindowSlots == 0 || selection.MaxWindowSlots > maximumMutableWindowSlots {
				return fmt.Errorf("replica.heads.%s.max_window_slots is %d, must be in [1,%d]", name, selection.MaxWindowSlots, maximumMutableWindowSlots)
			}
			if selection.OverlayFinalizedHead != "" {
				if err := schema.ValidateHeadName(selection.OverlayFinalizedHead); err != nil {
					return fmt.Errorf("replica.heads.%s.overlay_finalized_head: %w", name, err)
				}
				if selection.OverlayFinalizedHead == selection.HandoffHead {
					return fmt.Errorf("replica.heads.%s.overlay_finalized_head equals its authenticated handoff; omit the redundant overlay", name)
				}
				overlay, selected := c.Replica.Heads.Selection(selection.OverlayFinalizedHead)
				if !selected {
					return fmt.Errorf("replica.heads.%s.overlay_finalized_head %q must also be selected", name, selection.OverlayFinalizedHead)
				}
				if overlay.Kind != server.FinalizedMonotonic {
					return fmt.Errorf("replica.heads.%s.overlay_finalized_head %q must be finalized-monotonic", name, selection.OverlayFinalizedHead)
				}
			}
			handoff, handoffSelected := c.Replica.Heads.Selection(selection.HandoffHead)
			if handoffSelected && handoff.Kind != server.FinalizedMonotonic {
				return fmt.Errorf("replica.heads.%s.handoff_head %q must be finalized-monotonic when selected", name, selection.HandoffHead)
			}
			if !handoffSelected && selection.OverlayFinalizedHead == "" {
				return fmt.Errorf("replica.heads.%s requires overlay_finalized_head when handoff_head %q is metadata-only", name, selection.HandoffHead)
			}
		default:
			return fmt.Errorf("replica.heads.%s.kind is %q, must be explicitly %q or %q", name, selection.Kind,
				server.FinalizedMonotonic, server.UnfinalizedMutable)
		}
	}
	for _, head := range headNames {
		if _, err := p2p.RendezvousBlock(c.Net, head); err != nil {
			return fmt.Errorf("rendezvous namespace for replica head %q: %w", head, err)
		}
	}
	if c.Source.URL == "" && c.Source.IPNS == "" && c.Source.DNSLink == "" {
		return errors.New("source requires at least one of url, ipns, or dnslink")
	}
	if c.Source.URL != "" {
		if err := validateSourceURL(c.Source.URL); err != nil {
			return fmt.Errorf("source.url: %w", err)
		}
	}
	if c.Source.IPNS != "" && c.Source.DNSLink != "" {
		return errors.New("source.ipns and source.dnslink are mutually exclusive")
	}
	if c.Source.PublicKey == "" && c.Source.DNSLink == "" {
		return errors.New("source.pubkey is required unless DNSLink delegates the signer")
	}
	if _, err := parsePublicKey(c.Source.PublicKey); err != nil {
		return err
	}
	for _, name := range headNames {
		selection, _ := c.Replica.Heads.Selection(name)
		if selection.Kind == server.UnfinalizedMutable && c.Source.PublicKey == "" {
			return fmt.Errorf("replica.heads.%s is unfinalized-mutable but source.pubkey is empty: mutable revision order requires one pinned signing authority", name)
		}
	}
	if c.Source.IPNS != "" {
		if _, err := ipns.NameFromString(c.Source.IPNS); err != nil {
			return fmt.Errorf("source.ipns: %w", err)
		}
	}
	if c.Source.DNSLink != "" {
		if err := p2p.ValidateDNSLinkDomain(c.Source.DNSLink); err != nil {
			return fmt.Errorf("source.dnslink: %w", err)
		}
	}
	if c.Source.PollInterval == 0 {
		c.Source.PollInterval = duration(time.Minute)
	}
	if c.Source.FetchTimeout == 0 {
		c.Source.FetchTimeout = duration(30 * time.Second)
	}
	if c.Source.PollInterval.value() < minimumPollInterval || c.Source.FetchTimeout.value() < minimumFetchTimeout {
		return fmt.Errorf("source.poll_interval must be at least %s and source.fetch_timeout at least %s", minimumPollInterval, minimumFetchTimeout)
	}
	if c.Kubo.API == "" {
		return errors.New("kubo.api is required")
	}
	if c.Kubo.ProviderPolicyCheck == "" {
		c.Kubo.ProviderPolicyCheck = providerPolicyCheckRuntime
	}
	if c.Kubo.ProviderPolicyCheck != providerPolicyCheckRuntime &&
		c.Kubo.ProviderPolicyCheck != providerPolicyCheckExternal {
		return fmt.Errorf("kubo.provider_policy_check is %q, must be %q or %q",
			c.Kubo.ProviderPolicyCheck, providerPolicyCheckRuntime, providerPolicyCheckExternal)
	}
	if c.Kubo.RequestTimeout == 0 {
		c.Kubo.RequestTimeout = duration(kubo.DefaultRequestTimeout)
	}
	if c.Kubo.PinTimeout == 0 {
		c.Kubo.PinTimeout = duration(replica.DefaultPinTimeout)
	}
	if c.Kubo.AnnounceInterval == 0 {
		c.Kubo.AnnounceInterval = duration(12 * time.Hour)
	}
	if c.Kubo.RequestTimeout.value() < minimumRequestTimeout || c.Kubo.PinTimeout.value() < minimumPinTimeout || c.Kubo.AnnounceInterval.value() < minimumAnnounceInterval {
		return fmt.Errorf("kubo.request_timeout must be at least %s, kubo.pin_timeout at least %s, and kubo.announce_interval at least %s",
			minimumRequestTimeout, minimumPinTimeout, minimumAnnounceInterval)
	}
	if c.Kubo.PinProgressItems == 0 {
		c.Kubo.PinProgressItems = replica.DefaultPinProgressItems
	}
	if c.Kubo.PinProgressBytes == 0 {
		c.Kubo.PinProgressBytes = replica.DefaultPinProgressBytes
	}
	if c.Kubo.MaxStreamItems == 0 {
		c.Kubo.MaxStreamItems = c.Kubo.PinProgressItems
	}
	if c.Kubo.MaxStreamBytes == 0 {
		c.Kubo.MaxStreamBytes = c.Kubo.PinProgressBytes
	}
	if c.Kubo.PinProgressItems <= 0 || c.Kubo.PinProgressItems > c.Kubo.MaxStreamItems || c.Kubo.MaxStreamItems > kubo.MaximumStreamItems {
		return errors.New("kubo.pin_progress_items must be positive and no greater than max_stream_items and Kubo's hard limit")
	}
	if c.Kubo.PinProgressBytes <= 0 || c.Kubo.PinProgressBytes > c.Kubo.MaxStreamBytes || c.Kubo.MaxStreamBytes > kubo.MaximumStreamBytes {
		return errors.New("kubo.pin_progress_bytes must be positive and no greater than max_stream_bytes and Kubo's hard limit")
	}
	if err := c.Gateway.defaultsAndValidate(c.Replica.Heads); err != nil {
		return err
	}
	if c.Metrics.Listen == "" {
		return errors.New("metrics.listen is required so replica lag and pin progress cannot be silent")
	}
	return nil
}

func (c *gatewayConfig) defaultsAndValidate(heads replicaHeads) error {
	if !c.Enabled {
		return nil
	}
	if c.Listen == "" {
		c.Listen = defaultGatewayListen
	}
	if err := validateGatewayListen(c.Listen); err != nil {
		return fmt.Errorf("gateway.listen: %w", err)
	}
	if c.Beacon.GenesisTime == 0 {
		return errors.New("gateway.beacon.genesis_time is required when the gateway is enabled")
	}
	if c.Beacon.SecondsPerSlot == 0 {
		c.Beacon.SecondsPerSlot = schema.SecondsPerSlot
	}
	if c.Beacon.GenesisValidatorsRoot == "" {
		c.Beacon.GenesisValidatorsRoot = "0x" + strings.Repeat("0", 64)
	}
	if c.Beacon.GenesisForkVersion == "" {
		c.Beacon.GenesisForkVersion = "0x00000000"
	}
	if err := validateGatewayFixedHex(c.Beacon.GenesisValidatorsRoot, 32); err != nil {
		return fmt.Errorf("gateway.beacon.genesis_validators_root: %w", err)
	}
	if err := validateGatewayFixedHex(c.Beacon.GenesisForkVersion, 4); err != nil {
		return fmt.Errorf("gateway.beacon.genesis_fork_version: %w", err)
	}
	if _, duplicate := c.Beacon.SpecExtra["SECONDS_PER_SLOT"]; duplicate {
		return errors.New("gateway.beacon.spec_extra must not carry SECONDS_PER_SLOT; gateway.beacon.seconds_per_slot is authoritative")
	}
	if _, err := c.specMap(); err != nil {
		return err
	}
	if err := c.validateLiveHeads(heads); err != nil {
		return err
	}

	if c.MaxQueryHashes == 0 {
		c.MaxQueryHashes = defaultGatewayMaxQueryHashes
	}
	if c.MaxQueryHashes < 1 || c.MaxQueryHashes > schema.MaxBlobsPerSlotCeiling {
		return fmt.Errorf("gateway.max_query_hashes is %d, must be in [1,%d]", c.MaxQueryHashes, schema.MaxBlobsPerSlotCeiling)
	}
	if c.MaxResponseBytesInFlight == 0 {
		c.MaxResponseBytesInFlight = defaultGatewayResponseBytesInFlight
	}
	if minimum := server.MaxResponseWeight(c.MaxQueryHashes); c.MaxResponseBytesInFlight < minimum {
		return fmt.Errorf("gateway.max_response_bytes_in_flight is %d, must admit one maximum response of %d bytes",
			c.MaxResponseBytesInFlight, minimum)
	}
	if c.ImmutableHorizonSlots == 0 {
		c.ImmutableHorizonSlots = defaultGatewayImmutableHorizonSlots
	}
	if c.ReadHeaderTimeout == 0 {
		c.ReadHeaderTimeout = duration(defaultGatewayReadHeaderTimeout)
	}
	if c.ReadTimeout == 0 {
		c.ReadTimeout = duration(defaultGatewayReadTimeout)
	}
	if c.WriteTimeout == 0 {
		c.WriteTimeout = duration(defaultGatewayWriteTimeout)
	}
	if c.IdleTimeout == 0 {
		c.IdleTimeout = duration(defaultGatewayIdleTimeout)
	}
	for _, timeout := range []struct {
		name     string
		duration duration
	}{
		{"read_header_timeout", c.ReadHeaderTimeout},
		{"read_timeout", c.ReadTimeout},
		{"write_timeout", c.WriteTimeout},
		{"idle_timeout", c.IdleTimeout},
	} {
		if timeout.duration.value() <= 0 {
			return fmt.Errorf("gateway.%s must be positive", timeout.name)
		}
	}
	if c.MaxHeaderBytes == 0 {
		c.MaxHeaderBytes = defaultGatewayMaxHeaderBytes
	}
	if c.MaxHeaderBytes < 0 {
		return errors.New("gateway.max_header_bytes must not be negative")
	}
	if c.MaxConns == 0 {
		c.MaxConns = defaultGatewayMaxConns
	}
	if c.MaxConns < 0 {
		return errors.New("gateway.max_conns must not be negative")
	}

	if c.PublicReadAdmission.Enabled == nil {
		enabled := true
		c.PublicReadAdmission.Enabled = &enabled
	}
	if c.PublicReadAdmission.GlobalRate == 0 {
		c.PublicReadAdmission.GlobalRate = defaultGatewayPublicReadGlobalRate
	}
	if c.PublicReadAdmission.GlobalBurst == 0 {
		c.PublicReadAdmission.GlobalBurst = defaultGatewayPublicReadGlobalBurst
	}
	if c.PublicReadAdmission.ClientRate == 0 {
		c.PublicReadAdmission.ClientRate = defaultGatewayPublicReadClientRate
	}
	if c.PublicReadAdmission.ClientBurst == 0 {
		c.PublicReadAdmission.ClientBurst = defaultGatewayPublicReadClientBurst
	}
	if c.PublicReadAdmission.ClientBuckets == 0 {
		c.PublicReadAdmission.ClientBuckets = defaultGatewayPublicReadClientBuckets
	}
	if c.PublicReadAdmission.ClientBucketTTL == 0 {
		c.PublicReadAdmission.ClientBucketTTL = duration(defaultGatewayPublicReadClientBucketTTL)
	}
	if _, err := c.publicReadLimiterConfig(nil); err != nil {
		return err
	}
	return nil
}

func validateGatewayFixedHex(value string, size int) error {
	if !strings.HasPrefix(value, "0x") {
		return fmt.Errorf("must be 0x-prefixed %d-byte hex", size)
	}
	raw, err := hex.DecodeString(strings.TrimPrefix(value, "0x"))
	if err != nil || len(raw) != size {
		return fmt.Errorf("must be 0x-prefixed %d-byte hex", size)
	}
	return nil
}

func validateGatewayListen(value string) error {
	host, portText, err := net.SplitHostPort(value)
	if err != nil {
		return err
	}
	if strings.TrimSpace(host) != host || strings.TrimSpace(portText) != portText {
		return errors.New("must not contain surrounding whitespace")
	}
	port, err := strconv.ParseUint(portText, 10, 16)
	if err != nil || port == 0 {
		return errors.New("must contain a numeric TCP port in [1,65535]")
	}
	return nil
}

func (c gatewayConfig) validateLiveHeads(heads replicaHeads) error {
	for name, view := range c.LiveHeads {
		if err := schema.ValidateHeadName(name); err != nil {
			return fmt.Errorf("gateway.live_heads.%s: %w", name, err)
		}
		if _, collision := heads.Selection(name); collision {
			return fmt.Errorf("gateway.live_heads.%s collides with a selected physical head", name)
		}
		finalized, ok := heads.Selection(view.FinalizedHead)
		if !ok {
			return fmt.Errorf("gateway.live_heads.%s.finalized_head %q is not selected by replica.heads", name, view.FinalizedHead)
		}
		if finalized.Kind != server.FinalizedMonotonic {
			return fmt.Errorf("gateway.live_heads.%s.finalized_head %q is %q, want %q",
				name, view.FinalizedHead, finalized.Kind, server.FinalizedMonotonic)
		}
		mutable, ok := heads.Selection(view.UnfinalizedHead)
		if !ok {
			return fmt.Errorf("gateway.live_heads.%s.unfinalized_head %q is not selected by replica.heads", name, view.UnfinalizedHead)
		}
		if mutable.Kind != server.UnfinalizedMutable {
			return fmt.Errorf("gateway.live_heads.%s.unfinalized_head %q is %q, want %q",
				name, view.UnfinalizedHead, mutable.Kind, server.UnfinalizedMutable)
		}
		frontier := mutable.HandoffHead
		if mutable.OverlayFinalizedHead != "" {
			frontier = mutable.OverlayFinalizedHead
		}
		if view.FinalizedHead != frontier {
			return fmt.Errorf("gateway.live_heads.%s.finalized_head is %q, want retained frontier %q for mutable head %q",
				name, view.FinalizedHead, frontier, view.UnfinalizedHead)
		}
		if mutable.HandoffHead != view.FinalizedHead && !view.RequireVersionedHashes {
			return fmt.Errorf("gateway.live_heads.%s pairs filtered finalized head %q with mutable handoff %q; require_versioned_hashes must be true",
				name, view.FinalizedHead, mutable.HandoffHead)
		}
	}
	return nil
}

func (c gatewayConfig) specMap() (map[string]string, error) {
	result := make(map[string]string, len(c.Beacon.SpecExtra))
	for key, value := range c.Beacon.SpecExtra {
		rendered, err := gatewaySpecValue(value)
		if err != nil {
			return nil, fmt.Errorf("gateway.beacon.spec_extra.%s: %w", key, err)
		}
		result[key] = rendered
	}
	return result, nil
}

func gatewaySpecValue(value any) (string, error) {
	switch value := value.(type) {
	case string:
		return value, nil
	case bool:
		return strconv.FormatBool(value), nil
	case int:
		return strconv.Itoa(value), nil
	case int64:
		return strconv.FormatInt(value, 10), nil
	case uint64:
		return strconv.FormatUint(value, 10), nil
	case float64:
		return strconv.FormatFloat(value, 'f', -1, 64), nil
	case nil:
		return "", errors.New("value is null; the beacon spec map has no nulls")
	default:
		return "", fmt.Errorf("value is %T; only scalars can be served", value)
	}
}

func (c gatewayConfig) serverLiveHeads() map[string]server.LiveHead {
	if len(c.LiveHeads) == 0 {
		return nil
	}
	result := make(map[string]server.LiveHead, len(c.LiveHeads))
	for name, view := range c.LiveHeads {
		result[name] = server.LiveHead{
			FinalizedHead:          view.FinalizedHead,
			UnfinalizedHead:        view.UnfinalizedHead,
			RequireVersionedHashes: view.RequireVersionedHashes,
		}
	}
	return result
}

func (c gatewayConfig) publicReadLimiterConfig(mx *replicaMetrics) (*server.PublicReadLimiterConfig, error) {
	config := c.PublicReadAdmission
	prefixes, err := parseGatewayTrustedProxyCIDRs(config.TrustedProxyCIDRs)
	if err != nil {
		return nil, err
	}
	limiter := &server.PublicReadLimiterConfig{
		GlobalRate:       rate.Limit(config.GlobalRate),
		GlobalBurst:      config.GlobalBurst,
		PerClientRate:    rate.Limit(config.ClientRate),
		PerClientBurst:   config.ClientBurst,
		MaxClientBuckets: config.ClientBuckets,
		ClientBucketTTL:  config.ClientBucketTTL.value(),
		ForwardedHeader:  config.TrustedProxyHeader,
		TrustedProxies:   prefixes,
	}
	if mx != nil {
		limiter.Observe = func(outcome server.PublicReadAdmissionOutcome, cost int) {
			mx.base.PublicReadAdmission(outcome.String(), cost)
		}
	}
	if err := server.ValidatePublicReadLimiterConfig(*limiter, c.MaxQueryHashes); err != nil {
		return nil, fmt.Errorf("gateway.public_read_admission: %w", err)
	}
	if config.Enabled != nil && !*config.Enabled {
		return nil, nil
	}
	return limiter, nil
}

func parseGatewayTrustedProxyCIDRs(values []string) ([]netip.Prefix, error) {
	result := make([]netip.Prefix, 0, len(values))
	seen := make(map[netip.Prefix]struct{}, len(values))
	for index, value := range values {
		if value == "" || strings.TrimSpace(value) != value {
			return nil, fmt.Errorf("gateway.public_read_admission.trusted_proxy_cidrs[%d] must be non-empty and whitespace-free", index)
		}
		prefix, err := netip.ParsePrefix(value)
		if err != nil {
			return nil, fmt.Errorf("gateway.public_read_admission.trusted_proxy_cidrs[%d] %q: %w", index, value, err)
		}
		if prefix.Addr().Zone() != "" || prefix.Addr().Is4In6() || prefix != prefix.Masked() {
			return nil, fmt.Errorf("gateway.public_read_admission.trusted_proxy_cidrs[%d] %q must be a canonical native IPv4 or IPv6 network", index, value)
		}
		if _, duplicate := seen[prefix]; duplicate {
			return nil, fmt.Errorf("gateway.public_read_admission.trusted_proxy_cidrs[%d] %q is duplicated", index, value)
		}
		seen[prefix] = struct{}{}
		result = append(result, prefix)
	}
	return result, nil
}

func validateSourceURL(raw string) error {
	parsed, err := url.Parse(raw)
	if err != nil {
		return err
	}
	if parsed.Opaque != "" || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return errors.New("must be a hierarchical base URL without userinfo, query, or fragment")
	}
	switch strings.ToLower(parsed.Scheme) {
	case "https":
		return nil
	case "http":
		host := strings.ToLower(parsed.Hostname())
		ip := net.ParseIP(host)
		if host == "localhost" || ip != nil && ip.IsLoopback() {
			return nil
		}
		return errors.New("plain HTTP is permitted only for localhost or a literal loopback IP")
	default:
		return errors.New("scheme must be https, or http on loopback")
	}
}
