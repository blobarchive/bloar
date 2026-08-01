package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const validConfig = `version: 1
net: testnet
replica:
  id: archive-eu-1
  state_path: /var/lib/bloar-replica
  heads: [zeta, alpha]
source:
  url: https://writer.example
  pubkey: 0000000000000000000000000000000000000000000000000000000000000000
kubo:
  api: http://127.0.0.1:5001
  allow_unauthenticated: true
metrics:
  listen: 127.0.0.1:9097
`

const validMutableConfig = `version: 2
net: testnet
replica:
  id: archive-eu-1
  state_path: /var/lib/bloar-replica
  heads:
    arbitrum-one:
      kind: finalized-monotonic
    unfinalized:
      kind: unfinalized-mutable
      handoff_head: all
      max_window_slots: 64
      overlay_finalized_head: arbitrum-one
source:
  url: https://writer.example
  pubkey: 0000000000000000000000000000000000000000000000000000000000000000
kubo:
  api: http://127.0.0.1:5001
  allow_unauthenticated: true
metrics:
  listen: 127.0.0.1:9097
`

func TestLoadConfigStrictDefaults(t *testing.T) {
	cfg, err := loadConfig(writeConfig(t, validConfig))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(cfg.Replica.Heads.Names(), ",") != "alpha,zeta" {
		t.Fatalf("heads = %#v", cfg.Replica.Heads.Names())
	}
	if cfg.Source.PollInterval.value() != time.Minute || cfg.Source.FetchTimeout.value() != 30*time.Second {
		t.Fatalf("source defaults = %s, %s", cfg.Source.PollInterval.value(), cfg.Source.FetchTimeout.value())
	}
	if cfg.Replica.AuditInterval.value() != time.Minute {
		t.Fatalf("replica audit default = %s", cfg.Replica.AuditInterval.value())
	}
	if cfg.Kubo.PinTimeout.value() != 24*time.Hour || cfg.Kubo.AnnounceInterval.value() != 12*time.Hour {
		t.Fatalf("Kubo defaults = %s, %s", cfg.Kubo.PinTimeout.value(), cfg.Kubo.AnnounceInterval.value())
	}
	if cfg.Kubo.ProviderPolicyCheck != providerPolicyCheckRuntime {
		t.Fatalf("provider policy check default = %q, want %q", cfg.Kubo.ProviderPolicyCheck, providerPolicyCheckRuntime)
	}
	if cfg.Kubo.MaxStreamItems < cfg.Kubo.PinProgressItems || cfg.Kubo.MaxStreamBytes < cfg.Kubo.PinProgressBytes {
		t.Fatal("client stream ceilings do not cover progress stream ceilings")
	}
	if cfg.Gateway.Enabled || cfg.Gateway.Listen != "" {
		t.Fatalf("gateway default = enabled %t listen %q, want disabled and unconfigured", cfg.Gateway.Enabled, cfg.Gateway.Listen)
	}
}

func TestProviderPolicyCheckModeIsExplicitAndClosed(t *testing.T) {
	external := strings.Replace(validConfig, "  allow_unauthenticated: true",
		"  allow_unauthenticated: true\n  provider_policy_check: external", 1)
	cfg, err := loadConfig(writeConfig(t, external))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Kubo.ProviderPolicyCheck != providerPolicyCheckExternal {
		t.Fatalf("provider policy check = %q", cfg.Kubo.ProviderPolicyCheck)
	}
	unknown := strings.Replace(validConfig, "  allow_unauthenticated: true",
		"  allow_unauthenticated: true\n  provider_policy_check: sometimes", 1)
	if _, err := loadConfig(writeConfig(t, unknown)); err == nil ||
		!strings.Contains(err.Error(), "must be \"runtime\" or \"external\"") {
		t.Fatalf("unknown provider policy check error = %v", err)
	}
}

func TestLoadConfigGatewayDefaultsAndLiveView(t *testing.T) {
	body := withGatewayConfig(validMutableConfig, `gateway:
  enabled: true
  beacon:
    genesis_time: 1606824023
    spec_extra:
      DEPOSIT_CHAIN_ID: 1
      EIP7594_FORK_EPOCH: true
  live_heads:
    arbitrum-live:
      finalized_head: arbitrum-one
      unfinalized_head: unfinalized
      require_versioned_hashes: true
`)
	cfg, err := loadConfig(writeConfig(t, body))
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.Gateway.Enabled || cfg.Gateway.Listen != defaultGatewayListen {
		t.Fatalf("gateway enabled/listen = %t/%q", cfg.Gateway.Enabled, cfg.Gateway.Listen)
	}
	if cfg.Gateway.Beacon.SecondsPerSlot != 12 ||
		cfg.Gateway.Beacon.GenesisForkVersion != "0x00000000" ||
		cfg.Gateway.Beacon.GenesisValidatorsRoot != "0x"+strings.Repeat("0", 64) {
		t.Fatalf("gateway beacon defaults = %+v", cfg.Gateway.Beacon)
	}
	if cfg.Gateway.MaxQueryHashes != defaultGatewayMaxQueryHashes ||
		cfg.Gateway.MaxResponseBytesInFlight != defaultGatewayResponseBytesInFlight ||
		cfg.Gateway.ReadHeaderTimeout.value() != defaultGatewayReadHeaderTimeout ||
		cfg.Gateway.WriteTimeout.value() != defaultGatewayWriteTimeout ||
		cfg.Gateway.MaxConns != defaultGatewayMaxConns {
		t.Fatalf("gateway bounds were not defaulted: %+v", cfg.Gateway)
	}
	if cfg.Gateway.PublicReadAdmission.Enabled == nil || !*cfg.Gateway.PublicReadAdmission.Enabled {
		t.Fatal("gateway public read admission did not default on")
	}
	spec, err := cfg.Gateway.specMap()
	if err != nil {
		t.Fatal(err)
	}
	if spec["DEPOSIT_CHAIN_ID"] != "1" || spec["EIP7594_FORK_EPOCH"] != "true" {
		t.Fatalf("gateway spec = %#v", spec)
	}
	view := cfg.Gateway.serverLiveHeads()["arbitrum-live"]
	if view.FinalizedHead != "arbitrum-one" || view.UnfinalizedHead != "unfinalized" || !view.RequireVersionedHashes {
		t.Fatalf("gateway live view = %+v", view)
	}
}

func TestLoadConfigRejectsUnsafeGateway(t *testing.T) {
	base := withGatewayConfig(validMutableConfig, `gateway:
  enabled: true
  beacon:
    genesis_time: 1606824023
  live_heads:
    arbitrum-live:
      finalized_head: arbitrum-one
      unfinalized_head: unfinalized
      require_versioned_hashes: true
`)
	for name, body := range map[string]string{
		"missing genesis": strings.Replace(base, "    genesis_time: 1606824023\n", "", 1),
		"duplicate seconds source": strings.Replace(base, "    genesis_time: 1606824023",
			"    genesis_time: 1606824023\n    spec_extra:\n      SECONDS_PER_SLOT: 12", 1),
		"malformed validators root": strings.Replace(base, "    genesis_time: 1606824023",
			"    genesis_time: 1606824023\n    genesis_validators_root: \"0xdead\"", 1),
		"malformed fork version": strings.Replace(base, "    genesis_time: 1606824023",
			"    genesis_time: 1606824023\n    genesis_fork_version: nope", 1),
		"unselected finalized": strings.Replace(base, "finalized_head: arbitrum-one", "finalized_head: optimism", 1),
		"wrong finalized kind": strings.Replace(base, "finalized_head: arbitrum-one", "finalized_head: unfinalized", 1),
		"unselected mutable":   strings.Replace(base, "unfinalized_head: unfinalized", "unfinalized_head: optimism", 1),
		"filtered view enumerates": strings.Replace(base,
			"      require_versioned_hashes: true\n", "      require_versioned_hashes: false\n", 1),
		"physical name collision": strings.Replace(base, "    arbitrum-live:", "    arbitrum-one:", 1),
		"noncanonical listen": strings.Replace(base, "  enabled: true",
			"  enabled: true\n  listen: localhost", 1),
		"too little response memory": strings.Replace(base, "  enabled: true",
			"  enabled: true\n  max_response_bytes_in_flight: 1", 1),
		"bad read timeout": strings.Replace(base, "  enabled: true",
			"  enabled: true\n  read_timeout: -1s", 1),
		"bad proxy network": strings.Replace(base, "  enabled: true",
			"  enabled: true\n  public_read_admission:\n    trusted_proxy_header: X-Forwarded-For\n    trusted_proxy_cidrs: [192.0.2.9/24]", 1),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := loadConfig(writeConfig(t, body)); err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

func TestLoadConfigGatewayPublicReadAdmissionCanOptOut(t *testing.T) {
	body := withGatewayConfig(validMutableConfig, `gateway:
  enabled: true
  beacon:
    genesis_time: 1606824023
  public_read_admission:
    enabled: false
`)
	cfg, err := loadConfig(writeConfig(t, body))
	if err != nil {
		t.Fatal(err)
	}
	limiter, err := cfg.Gateway.publicReadLimiterConfig(nil)
	if err != nil {
		t.Fatal(err)
	}
	if limiter != nil {
		t.Fatal("explicitly disabled gateway limiter is non-nil")
	}
}

func TestLoadConfigStructuredMutableSelection(t *testing.T) {
	cfg, err := loadConfig(writeConfig(t, validMutableConfig))
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(cfg.Replica.Heads.Names(), ","); got != "arbitrum-one,unfinalized" {
		t.Fatalf("selected heads = %q", got)
	}
	selection := buildFollowerHeadSelection(cfg.Replica.Heads)
	if _, selected := selection.policies["all"]; selected {
		t.Fatal("metadata-only global handoff entered the retained generation")
	}
	if len(selection.policies) != 2 || selection.expectedKinds["unfinalized"] != "unfinalized-mutable" {
		t.Fatalf("follower selection = %#v", selection)
	}
	if selection.expectedHandoffs["unfinalized"] != "all" || selection.maxMutableWindowSlots["unfinalized"] != 64 ||
		selection.overlayFinalizedHeads["unfinalized"] != "arbitrum-one" {
		t.Fatalf("mutable policy = handoffs %#v windows %#v overlays %#v", selection.expectedHandoffs,
			selection.maxMutableWindowSlots, selection.overlayFinalizedHeads)
	}
}

func TestShippedConfigExampleLoads(t *testing.T) {
	cfg, err := loadConfig(filepath.Join("..", "..", "deploy", "examples", "kubo-replica.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Version != 2 || strings.Join(cfg.Replica.Heads.Names(), ",") != "arbitrum-one,unfinalized" {
		t.Fatalf("shipped example selection = version %d heads %v", cfg.Version, cfg.Replica.Heads.Names())
	}
	if cfg.Gateway.Enabled {
		t.Fatal("shipped example unexpectedly enables the public gateway")
	}
	enabled := cfg.Gateway
	enabled.Enabled = true
	if err := enabled.defaultsAndValidate(cfg.Replica.Heads); err != nil {
		t.Fatalf("shipped gateway example cannot be enabled as written: %v", err)
	}
}

func TestLoadConfigLegacySequencePinsFinalizedContract(t *testing.T) {
	cfg, err := loadConfig(writeConfig(t, validConfig))
	if err != nil {
		t.Fatal(err)
	}
	selection := buildFollowerHeadSelection(cfg.Replica.Heads)
	for _, name := range []string{"alpha", "zeta"} {
		if selection.expectedKinds[name] != "finalized-monotonic" {
			t.Fatalf("legacy head %q kind = %q", name, selection.expectedKinds[name])
		}
	}
	if len(selection.expectedHandoffs) != 0 || len(selection.maxMutableWindowSlots) != 0 || len(selection.overlayFinalizedHeads) != 0 {
		t.Fatalf("legacy config gained mutable policy: %#v", selection)
	}
}

func TestLoadConfigStructuredFinalizedAndPhysicalHandoffSelections(t *testing.T) {
	structuredFinalized := strings.Replace(validConfig, "version: 1", "version: 2", 1)
	structuredFinalized = strings.Replace(structuredFinalized, "  heads: [zeta, alpha]", `  heads:
    zeta:
      kind: finalized-monotonic
    alpha:
      kind: finalized-monotonic`, 1)
	if _, err := loadConfig(writeConfig(t, structuredFinalized)); err != nil {
		t.Fatalf("structured finalized-only selection: %v", err)
	}

	physicalHandoff := strings.Replace(validMutableConfig, `    arbitrum-one:
      kind: finalized-monotonic`, `    all:
      kind: finalized-monotonic`, 1)
	physicalHandoff = strings.Replace(physicalHandoff, "      overlay_finalized_head: arbitrum-one\n", "", 1)
	cfg, err := loadConfig(writeConfig(t, physicalHandoff))
	if err != nil {
		t.Fatalf("selected physical handoff: %v", err)
	}
	selection := buildFollowerHeadSelection(cfg.Replica.Heads)
	if got := strings.Join(selection.names, ","); got != "all,unfinalized" {
		t.Fatalf("physical handoff names = %q", got)
	}
	if selection.expectedHandoffs["unfinalized"] != "all" || len(selection.overlayFinalizedHeads) != 0 {
		t.Fatalf("physical handoff policy = handoffs %#v overlays %#v", selection.expectedHandoffs, selection.overlayFinalizedHeads)
	}
}

func TestLoadConfigRejectsUnsafeStructuredSelections(t *testing.T) {
	withoutOverlay := strings.Replace(validMutableConfig, "      overlay_finalized_head: arbitrum-one\n", "", 1)
	withoutMutableKind := strings.Replace(validMutableConfig, "      kind: unfinalized-mutable\n", "", 1)
	withoutMutableKey := strings.Replace(validMutableConfig,
		"  url: https://writer.example\n  pubkey: 0000000000000000000000000000000000000000000000000000000000000000",
		"  url: ''\n  dnslink: writer.example\n  pubkey: ''", 1)
	selectedMutableHandoff := strings.Replace(validMutableConfig, "    unfinalized:", `    all:
      kind: unfinalized-mutable
      handoff_head: arbitrum-one
      max_window_slots: 64
    unfinalized:`, 1)
	selectedMutableOverlay := strings.Replace(validMutableConfig, "    unfinalized:", `    optimism:
      kind: unfinalized-mutable
      handoff_head: arbitrum-one
      max_window_slots: 64
    unfinalized:`, 1)
	selectedMutableOverlay = strings.Replace(selectedMutableOverlay, "overlay_finalized_head: arbitrum-one", "overlay_finalized_head: optimism", 1)
	for name, body := range map[string]string{
		"version zero":                     strings.Replace(validMutableConfig, "version: 2", "version: 0", 1),
		"unknown version":                  strings.Replace(validMutableConfig, "version: 2", "version: 3", 1),
		"fractional version":               strings.Replace(validMutableConfig, "version: 2", "version: 2.9", 1),
		"v2 flat sequence":                 strings.Replace(validConfig, "version: 1", "version: 2", 1),
		"v1 structured map":                strings.Replace(validMutableConfig, "version: 2", "version: 1", 1),
		"boolean legacy head":              strings.Replace(validConfig, "[zeta, alpha]", "[true, alpha]", 1),
		"numeric structured head":          strings.Replace(validMutableConfig, "    arbitrum-one:", "    1:", 1),
		"missing explicit kind":            withoutMutableKind,
		"unknown kind":                     strings.Replace(validMutableConfig, "kind: unfinalized-mutable", "kind: optimistic", 1),
		"boolean kind":                     strings.Replace(validMutableConfig, "kind: unfinalized-mutable", "kind: true", 1),
		"unknown nested field":             strings.Replace(validMutableConfig, "      kind: finalized-monotonic", "      kind: finalized-monotonic\n      surprise: true", 1),
		"duplicate nested field":           strings.Replace(validMutableConfig, "      kind: finalized-monotonic", "      kind: finalized-monotonic\n      kind: finalized-monotonic", 1),
		"duplicate selected head":          strings.Replace(validMutableConfig, "    unfinalized:", "    arbitrum-one:", 1),
		"zero mutable window":              strings.Replace(validMutableConfig, "max_window_slots: 64", "max_window_slots: 0", 1),
		"oversized mutable window":         strings.Replace(validMutableConfig, "max_window_slots: 64", "max_window_slots: 4097", 1),
		"fractional mutable window":        strings.Replace(validMutableConfig, "max_window_slots: 64", "max_window_slots: 64.9", 1),
		"quoted mutable window":            strings.Replace(validMutableConfig, "max_window_slots: 64", `max_window_slots: "64"`, 1),
		"missing mutable handoff":          strings.Replace(validMutableConfig, "      handoff_head: all\n", "", 1),
		"self mutable handoff":             strings.Replace(validMutableConfig, "handoff_head: all", "handoff_head: unfinalized", 1),
		"boolean mutable handoff":          strings.Replace(validMutableConfig, "handoff_head: all", "handoff_head: true", 1),
		"malformed mutable handoff":        strings.Replace(validMutableConfig, "handoff_head: all", `handoff_head: "Bad Name"`, 1),
		"metadata handoff without overlay": withoutOverlay,
		"unselected overlay":               strings.Replace(validMutableConfig, "overlay_finalized_head: arbitrum-one", "overlay_finalized_head: optimism", 1),
		"boolean overlay":                  strings.Replace(validMutableConfig, "overlay_finalized_head: arbitrum-one", "overlay_finalized_head: true", 1),
		"malformed overlay":                strings.Replace(validMutableConfig, "overlay_finalized_head: arbitrum-one", `overlay_finalized_head: "Bad Name"`, 1),
		"overlay equals handoff":           strings.Replace(validMutableConfig, "overlay_finalized_head: arbitrum-one", "overlay_finalized_head: all", 1),
		"selected mutable handoff":         selectedMutableHandoff,
		"selected mutable overlay":         selectedMutableOverlay,
		"mutable fields on finalized":      strings.Replace(validMutableConfig, "      kind: finalized-monotonic", "      kind: finalized-monotonic\n      max_window_slots: 1", 1),
		"mutable DNS delegation":           withoutMutableKey,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := loadConfig(writeConfig(t, body)); err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

func TestLoadConfigRejectsUnsafeOrAmbiguousForms(t *testing.T) {
	for name, body := range map[string]string{
		"unknown field":      strings.Replace(validConfig, "net: testnet", "net: testnet\nsurprise: true", 1),
		"relative state":     strings.Replace(validConfig, "/var/lib/bloar-replica", "relative", 1),
		"duplicate head":     strings.Replace(validConfig, "[zeta, alpha]", "[alpha, alpha]", 1),
		"no metrics":         strings.Replace(validConfig, "  listen: 127.0.0.1:9097", "  listen: ''", 1),
		"all sources absent": strings.Replace(validConfig, "  url: https://writer.example", "  url: ''", 1),
		"numeric duration":   strings.Replace(validConfig, "  api: http://127.0.0.1:5001", "  api: http://127.0.0.1:5001\n  pin_timeout: 10", 1),
		"second document":    validConfig + "---\nversion: 1\n",
		"malformed public key": strings.Replace(validConfig,
			"  pubkey: 0000000000000000000000000000000000000000000000000000000000000000", "  pubkey: not-hex", 1),
		"replica id whitespace":        strings.Replace(validConfig, "  id: archive-eu-1", "  id: ' archive-eu-1 '", 1),
		"pin name whitespace":          strings.Replace(validConfig, "  state_path: /var/lib/bloar-replica", "  state_path: /var/lib/bloar-replica\n  pin_name: ' reserved '", 1),
		"bad audit interval":           strings.Replace(validConfig, "  state_path: /var/lib/bloar-replica", "  state_path: /var/lib/bloar-replica\n  audit_interval: -1s", 1),
		"oversized rendezvous network": strings.Replace(validConfig, "net: testnet", "net: "+strings.Repeat("n", 4097), 1),
		"hammering poll interval":      strings.Replace(validConfig, "  url: https://writer.example", "  url: https://writer.example\n  poll_interval: 1ns", 1),
		"hammering announce interval":  strings.Replace(validConfig, "  api: http://127.0.0.1:5001", "  api: http://127.0.0.1:5001\n  announce_interval: 1ns", 1),
		"escaped NUL in head":          strings.Replace(validConfig, "[zeta, alpha]", `[zeta, "alpha\0beta"]`, 1),
		"noncanonical head":            strings.Replace(validConfig, "[zeta, alpha]", `[zeta, "Alpha One"]`, 1),
		"head surrounding whitespace":  strings.Replace(validConfig, "[zeta, alpha]", `[zeta, " alpha "]`, 1),
		"remote plain HTTP source":     strings.Replace(validConfig, "https://writer.example", "http://writer.example", 1),
		"source URL userinfo":          strings.Replace(validConfig, "https://writer.example", "https://user@writer.example", 1),
		"source URL query":             strings.Replace(validConfig, "https://writer.example", "https://writer.example?endpoint=other", 1),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := loadConfig(writeConfig(t, body)); err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

func TestLoadConfigAllowsLoopbackHTTPSource(t *testing.T) {
	for _, source := range []string{"http://localhost:8550", "http://127.0.0.1:8550", "http://[::1]:8550"} {
		t.Run(source, func(t *testing.T) {
			body := strings.Replace(validConfig, "https://writer.example", source, 1)
			if _, err := loadConfig(writeConfig(t, body)); err != nil {
				t.Fatalf("loopback source rejected: %v", err)
			}
		})
	}
}

func TestLoadConfigValidatesIPNSAndDNSLinkWithoutNetwork(t *testing.T) {
	badIPNS := strings.Replace(validConfig, "  url: https://writer.example", "  url: ''\n  ipns: not-an-ipns-name", 1)
	if _, err := loadConfig(writeConfig(t, badIPNS)); err == nil {
		t.Fatal("malformed IPNS name passed configuration validation")
	}
	badDNS := strings.Replace(validConfig, "  url: https://writer.example", "  url: ''\n  dnslink: https://not-a-domain/path", 1)
	if _, err := loadConfig(writeConfig(t, badDNS)); err == nil {
		t.Fatal("malformed DNSLink domain passed configuration validation")
	}
}

func TestLoadConfigBoundsProgressStreams(t *testing.T) {
	body := strings.Replace(validConfig, "  api: http://127.0.0.1:5001", `  api: http://127.0.0.1:5001
  max_stream_items: 10
  pin_progress_items: 11`, 1)
	if _, err := loadConfig(writeConfig(t, body)); err == nil {
		t.Fatal("progress limit beyond client stream ceiling accepted")
	}
}

func writeConfig(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "replica.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func withGatewayConfig(body, gateway string) string {
	return strings.Replace(body, "metrics:\n", gateway+"metrics:\n", 1)
}
