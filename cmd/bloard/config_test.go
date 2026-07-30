package main

import (
	"bytes"
	"crypto/ed25519"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/blobarchive/bloar/follow"
	"github.com/blobarchive/bloar/server"
)

// specExample is the configuration reference of spec 12, verbatim apart from
// the paths. If this stops parsing, the config the spec documents is not the
// config this binary takes, which is a bug in one of the two.
const specExample = `
net: mainnet
beacon:
  genesis_time: 1606824023
  seconds_per_slot: 12
  genesis_validators_root: "0x4b363db94e286120d76eb905340fdd4e54bfe9f06bf33ff6cf5ad27f511bfe95"
  spec_extra: {}

store:
  path: /var/lib/bloar
  gc_interval: 24h
  scrub_interval: 168h
  node_cache_mb: 256

server:
  listen: ":8550"
  auth_token_file: /etc/bloar/token
  max_put_blobs: 64
  immutable_horizon_slots: 7200
  public_read_admission:
    enabled: true
    global_rate: 4096
    global_burst: 16384
    client_rate: 1024
    client_burst: 4096
    client_buckets: 4096
    client_bucket_ttl: 15m
    trusted_proxy_header: ""
    trusted_proxy_cidrs: []

publish:
  signing_key_file: /etc/bloar/ed25519.key
  ipns: false
  ipns_republish: 4h

p2p:
  listen: ["/ip4/0.0.0.0/tcp/4001"]
  peers: []
  announce: []
  bitswap:
    serve: true
    max_queued_wants_per_peer: 1024
    max_outstanding_bytes_per_peer: 1048576
    send_workers: 8
    engine_task_workers: 8
    blockstore_workers: 128
    max_cid_bytes: 168

heads:
  all:
    origin_slot: 8626176
    seg_bits: 9
    fanout_bits: 8
    pin: { mode: full }
  arbitrum-one:
    origin_slot: 8626176
    seg_bits: 13
    fanout_bits: 8
    pin: { mode: window, duration: 720h }
  unfinalized:
    kind: unfinalized-mutable
    handoff_head: all
    max_window_slots: 64
    origin_slot: 8626176
    seg_bits: 5
    fanout_bits: 8
    pin: { mode: full }

live_heads:
  live:
    finalized_head: all
    unfinalized_head: unfinalized
`

func TestLoadSpecExample(t *testing.T) {
	cfg := loadString(t, specExample)

	if cfg.Net != "mainnet" {
		t.Errorf("net = %q, want mainnet", cfg.Net)
	}
	if cfg.Beacon.GenesisTime != 1606824023 {
		t.Errorf("beacon.genesis_time = %d", cfg.Beacon.GenesisTime)
	}
	if cfg.Beacon.SecondsPerSlot != 12 {
		t.Errorf("beacon.seconds_per_slot = %d", cfg.Beacon.SecondsPerSlot)
	}
	if cfg.Store.GCInterval != 24*time.Hour {
		t.Errorf("store.gc_interval = %s, want 24h", cfg.Store.GCInterval)
	}
	if cfg.Store.ScrubInterval != 7*24*time.Hour {
		t.Errorf("store.scrub_interval = %s, want 168h", cfg.Store.ScrubInterval)
	}
	if cfg.Store.NodeCacheMB != 256 {
		t.Errorf("store.node_cache_mb = %d", cfg.Store.NodeCacheMB)
	}
	if cfg.Server.Listen != ":8550" {
		t.Errorf("server.listen = %q", cfg.Server.Listen)
	}
	if cfg.Server.MaxPutBlobs != 64 {
		t.Errorf("server.max_put_blobs = %d", cfg.Server.MaxPutBlobs)
	}
	if cfg.Server.ImmutableHorizonSlots != 7200 {
		t.Errorf("server.immutable_horizon_slots = %d", cfg.Server.ImmutableHorizonSlots)
	}
	if a := cfg.Server.PublicReadAdmission; a.Enabled == nil || !*a.Enabled ||
		a.GlobalRate != 4096 || a.ClientRate != 1024 || a.ClientBucketTTL != 15*time.Minute {
		t.Errorf("server.public_read_admission = %+v", a)
	}
	if cfg.Publish.IPNSRepublish != 4*time.Hour {
		t.Errorf("publish.ipns_republish = %s, want 4h", cfg.Publish.IPNSRepublish)
	}
	if b := cfg.P2P.Bitswap; b.Serve == nil || !*b.Serve || b.MaxQueuedWantsPerPeer != 1024 ||
		b.MaxOutstandingBytesPerPeer != 1<<20 || b.SendWorkers != 8 || b.EngineTaskWorkers != 8 ||
		b.BlockstoreWorkers != 128 || b.MaxCIDBytes != 168 {
		t.Errorf("p2p.bitswap = %+v", b)
	}

	if len(cfg.Heads) != 3 {
		t.Fatalf("heads has %d entries, want 3", len(cfg.Heads))
	}
	all := cfg.Heads["all"]
	if *all.OriginSlot != 8626176 || *all.SegBits != 9 || *all.FanoutBits != 8 {
		t.Errorf("heads.all = %d/%d/%d, want 8626176/9/8", *all.OriginSlot, *all.SegBits, *all.FanoutBits)
	}
	if all.Pin.Mode != "full" {
		t.Errorf("heads.all.pin.mode = %q, want full", all.Pin.Mode)
	}
	// Parsed and held: phase 6 applies it, but a policy that would be rejected
	// then should be rejected now, when an operator is still watching.
	one := cfg.Heads["arbitrum-one"]
	if one.Pin.Mode != "window" || one.Pin.Duration != 720*time.Hour {
		t.Errorf("heads.arbitrum-one.pin = %s/%s, want window/720h", one.Pin.Mode, one.Pin.Duration)
	}
	if *one.SegBits != 13 {
		t.Errorf("heads.arbitrum-one.seg_bits = %d, want 13", *one.SegBits)
	}
	tip := cfg.Heads["unfinalized"]
	if tip.effectiveKind() != "unfinalized-mutable" || tip.HandoffHead != "all" || tip.MaxWindowSlots != 64 || tip.Pin.Mode != "full" {
		t.Errorf("heads.unfinalized = %#v", tip)
	}
	if view := cfg.LiveHeads["live"]; view.FinalizedHead != "all" || view.UnfinalizedHead != "unfinalized" {
		t.Errorf("live_heads.live = %#v", view)
	}
}

// TestLoadDefaults covers the values an operator may leave out.
func TestLoadDefaults(t *testing.T) {
	cfg := loadString(t, `
net: mainnet
beacon:
  genesis_time: 1606824023
store:
  path: /var/lib/bloar
server:
  auth_token_file: /etc/bloar/token
heads:
  all: {}
`)

	if cfg.Beacon.SecondsPerSlot != 12 {
		t.Errorf("beacon.seconds_per_slot = %d, want the beacon default of 12", cfg.Beacon.SecondsPerSlot)
	}
	if cfg.Server.Listen != ":8550" {
		t.Errorf("server.listen = %q, want :8550", cfg.Server.Listen)
	}
	if cfg.Server.MaxPutBlobs != 64 {
		t.Errorf("server.max_put_blobs = %d, want the spec default of 64", cfg.Server.MaxPutBlobs)
	}
	if cfg.Server.ImmutableHorizonSlots != 7200 {
		t.Errorf("server.immutable_horizon_slots = %d, want the spec default of 7200", cfg.Server.ImmutableHorizonSlots)
	}
	if cfg.Server.MaxQueryHashes != 128 {
		t.Errorf("server.max_query_hashes = %d, want the protocol-ceiling default of 128", cfg.Server.MaxQueryHashes)
	}
	if cfg.Server.MaxResponseBytesInFlight != 1<<30 {
		t.Errorf("server.max_response_bytes_in_flight = %d, want the default of 1 GiB", cfg.Server.MaxResponseBytesInFlight)
	}
	if cfg.Store.NodeCacheMB != 256 {
		t.Errorf("store.node_cache_mb = %d, want 256", cfg.Store.NodeCacheMB)
	}
	if cfg.Store.GCInterval != 24*time.Hour {
		t.Errorf("store.gc_interval = %s, want 24h", cfg.Store.GCInterval)
	}
	if cfg.Store.ScrubInterval != 7*24*time.Hour {
		t.Errorf("store.scrub_interval = %s, want 168h", cfg.Store.ScrubInterval)
	}
	if cfg.Beacon.GenesisForkVersion != "0x00000000" {
		t.Errorf("beacon.genesis_fork_version = %q, want the zero version", cfg.Beacon.GenesisForkVersion)
	}
	if !strings.HasPrefix(cfg.Beacon.GenesisValidatorsRoot, "0x0000") {
		t.Errorf("beacon.genesis_validators_root = %q, want the zero root", cfg.Beacon.GenesisValidatorsRoot)
	}

	// Spec 1: origin_slot defaults to the network's first blob slot, which this
	// build only knows for mainnet.
	all := cfg.Heads["all"]
	if *all.OriginSlot != 8626176 {
		t.Errorf("heads.all.origin_slot = %d, want the Dencun slot", *all.OriginSlot)
	}
	if *all.SegBits != 9 || *all.FanoutBits != 8 {
		t.Errorf("heads.all seg_bits/fanout_bits = %d/%d, want 9/8", *all.SegBits, *all.FanoutBits)
	}
	if all.Pin.Mode != "full" {
		t.Errorf("heads.all.pin.mode = %q, want full", all.Pin.Mode)
	}
}

func TestLoadLiveHeads(t *testing.T) {
	cfg := loadString(t, `
net: mainnet
beacon: { genesis_time: 1606824023 }
store: { path: /var/lib/bloar }
server: { auth_token_file: /etc/bloar/token }
publish: { signing_key_file: /etc/bloar/ed25519.key }
heads:
  all: {}
  unfinalized:
    kind: unfinalized-mutable
    handoff_head: all
    max_window_slots: 64
    pin: { mode: full }
live_heads:
  live:
    finalized_head: all
    unfinalized_head: unfinalized
    require_versioned_hashes: true
`)
	view, ok := cfg.LiveHeads["live"]
	if !ok || view.FinalizedHead != "all" || view.UnfinalizedHead != "unfinalized" || !view.RequireVersionedHashes {
		t.Fatalf("live_heads.live = %#v, present=%t", view, ok)
	}
	serverView := cfg.serverLiveHeads()["live"]
	if serverView.FinalizedHead != "all" || serverView.UnfinalizedHead != "unfinalized" || !serverView.RequireVersionedHashes {
		t.Fatalf("server live view = %#v", serverView)
	}
}

func TestValidateLiveHeads(t *testing.T) {
	physical := map[string]HeadConfig{
		"all":         {},
		"unfinalized": {Kind: "unfinalized-mutable", HandoffHead: "all"},
		"other-tip":   {Kind: "unfinalized-mutable", HandoffHead: "other"},
		"other":       {},
	}
	tests := []struct {
		name     string
		viewName string
		view     LiveHeadConfig
		want     string
	}{
		{name: "empty alias", viewName: "", view: LiveHeadConfig{FinalizedHead: "all", UnfinalizedHead: "unfinalized"}, want: "empty virtual name"},
		{name: "path alias", viewName: "bad/name", view: LiveHeadConfig{FinalizedHead: "all", UnfinalizedHead: "unfinalized"}, want: "one URL path segment"},
		{name: "collision", viewName: "all", view: LiveHeadConfig{FinalizedHead: "other", UnfinalizedHead: "unfinalized"}, want: "collides"},
		{name: "missing finalized", viewName: "live", view: LiveHeadConfig{FinalizedHead: "missing", UnfinalizedHead: "unfinalized"}, want: "not a declared"},
		{name: "missing mutable", viewName: "live", view: LiveHeadConfig{FinalizedHead: "all", UnfinalizedHead: "missing"}, want: "not a declared"},
		{name: "wrong finalized kind", viewName: "live", view: LiveHeadConfig{FinalizedHead: "unfinalized", UnfinalizedHead: "other"}, want: "must be \"finalized-monotonic\""},
		{name: "wrong mutable kind", viewName: "live", view: LiveHeadConfig{FinalizedHead: "all", UnfinalizedHead: "other"}, want: "must be \"unfinalized-mutable\""},
		{name: "same physical head", viewName: "live", view: LiveHeadConfig{FinalizedHead: "all", UnfinalizedHead: "all"}, want: "both roles"},
		{name: "writer handoff mismatch without exact hashes", viewName: "live", view: LiveHeadConfig{FinalizedHead: "all", UnfinalizedHead: "other-tip"}, want: "require_versioned_hashes must be true"},
	}

	t.Run("exact-hash filtered overlay", func(t *testing.T) {
		cfg := Config{Heads: physical, LiveHeads: map[string]LiveHeadConfig{"arb1-live": {
			FinalizedHead: "all", UnfinalizedHead: "other-tip", RequireVersionedHashes: true,
		}}}
		if err := cfg.validateLiveHeads(); err != nil {
			t.Fatalf("exact-hash filtered overlay: %v", err)
		}
		if !cfg.serverLiveHeads()["arb1-live"].RequireVersionedHashes {
			t.Fatal("exact-hash option was not passed to server config")
		}
	})
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := Config{Heads: physical, LiveHeads: map[string]LiveHeadConfig{tc.viewName: tc.view}}
			err := cfg.validateLiveHeads()
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want substring %q", err, tc.want)
			}
		})
	}

	t.Run("followed mutable reference", func(t *testing.T) {
		cfg := Config{
			Heads: map[string]HeadConfig{"all": {}},
			Follow: &FollowConfig{Heads: map[string]FollowHeadConfig{
				"unfinalized": {Kind: "unfinalized-mutable", HandoffHead: "all"},
			}},
			LiveHeads: map[string]LiveHeadConfig{"live": {
				FinalizedHead: "all", UnfinalizedHead: "unfinalized",
			}},
		}
		if err := cfg.validateLiveHeads(); err != nil {
			t.Fatalf("followed mutable live view: %v", err)
		}
	})

	t.Run("followed filtered overlay contract", func(t *testing.T) {
		cfg := Config{
			Follow: &FollowConfig{Heads: map[string]FollowHeadConfig{
				"arb1-finalized": {},
				"unfinalized": {
					Kind: server.UnfinalizedMutable, HandoffHead: "all",
				},
			}},
			LiveHeads: map[string]LiveHeadConfig{"arb1-live": {
				FinalizedHead: "arb1-finalized", UnfinalizedHead: "unfinalized", RequireVersionedHashes: true,
			}},
		}
		if err := cfg.validateLiveHeads(); err != nil {
			t.Fatalf("followed filtered overlay: %v", err)
		}
		got := cfg.followedLiveOverlays()
		if len(got) != 1 || got["unfinalized"] != "arb1-finalized" {
			t.Fatalf("followed overlay contracts = %v", got)
		}
	})

	t.Run("locally written filtered frontier uses runtime boundary", func(t *testing.T) {
		cfg := Config{
			Heads: map[string]HeadConfig{"arb1-finalized": {}},
			Follow: &FollowConfig{Heads: map[string]FollowHeadConfig{
				"unfinalized": {Kind: server.UnfinalizedMutable, HandoffHead: "all"},
			}},
			LiveHeads: map[string]LiveHeadConfig{"arb1-live": {
				FinalizedHead: "arb1-finalized", UnfinalizedHead: "unfinalized", RequireVersionedHashes: true,
			}},
		}
		if err := cfg.validateLiveHeads(); err != nil {
			t.Fatalf("mixed writer/follower overlay: %v", err)
		}
		if got := cfg.followedLiveOverlays(); len(got) != 0 {
			t.Fatalf("mixed writer/follower overlay was treated as one remote document: %v", got)
		}
	})
}

// TestLoadErrors covers the configs that must not start a daemon. Each of these
// would otherwise fail later, quietly, or not at all.
func TestLoadErrors(t *testing.T) {
	tests := []struct {
		name string
		yaml string
		want string
	}{
		{"unknown key", `
net: mainnet
beacon: {genesis_time: 1}
store: {path: /x}
server: {auth_token_file: /t}
heads: {all: {}}
sever: {listen: ":1"}
`, "field sever not found"},
		{"no net", `
beacon: {genesis_time: 1}
store: {path: /x}
server: {auth_token_file: /t}
heads: {all: {origin_slot: 0}}
`, "net is required"},
		{"no genesis_time", `
net: mainnet
store: {path: /x}
server: {auth_token_file: /t}
heads: {all: {}}
`, "beacon.genesis_time is required"},
		{"no store path", `
net: mainnet
beacon: {genesis_time: 1}
server: {auth_token_file: /t}
heads: {all: {}}
`, "store.path is required"},
		{"no auth token file", `
net: mainnet
beacon: {genesis_time: 1}
store: {path: /x}
heads: {all: {}}
`, "server.auth_token_file is required"},
		{"no heads written and none followed", `
net: mainnet
beacon: {genesis_time: 1}
store: {path: /x}
server: {auth_token_file: /t}
`, "must write at least one head or follow at least one"},
		{"origin_slot off mainnet", `
net: sepolia
beacon: {genesis_time: 1}
store: {path: /x}
server: {auth_token_file: /t}
heads: {all: {}}
`, "origin_slot is required"},
		{"bad fanout_bits", `
net: mainnet
beacon: {genesis_time: 1}
store: {path: /x}
server: {auth_token_file: /t}
heads: {all: {fanout_bits: 0}}
`, "fanout_bits"},
		{"bad seg_bits", `
net: mainnet
beacon: {genesis_time: 1}
store: {path: /x}
server: {auth_token_file: /t}
heads: {all: {seg_bits: 64}}
`, "seg_bits"},
		{"bad pin mode", `
net: mainnet
beacon: {genesis_time: 1}
store: {path: /x}
server: {auth_token_file: /t}
heads: {all: {pin: {mode: sometimes}}}
`, "pin.mode"},
		{"window pin with no duration", `
net: mainnet
beacon: {genesis_time: 1}
store: {path: /x}
server: {auth_token_file: /t}
heads: {all: {pin: {mode: window}}}
`, "no duration"},
		{"SECONDS_PER_SLOT in spec_extra", `
net: mainnet
beacon: {genesis_time: 1, spec_extra: {SECONDS_PER_SLOT: "13"}}
store: {path: /x}
server: {auth_token_file: /t}
heads: {all: {}}
`, "SECONDS_PER_SLOT"},
		{"structured spec_extra", `
net: mainnet
beacon: {genesis_time: 1, spec_extra: {NESTED: {a: 1}}}
store: {path: /x}
server: {auth_token_file: /t}
heads: {all: {}}
`, "only scalars"},
		{"negative max_query_hashes", `
net: mainnet
beacon: {genesis_time: 1}
store: {path: /x}
server: {auth_token_file: /t, max_query_hashes: -1}
heads: {all: {}}
`, "max_query_hashes"},
		{"above-ceiling max_query_hashes", `
net: mainnet
beacon: {genesis_time: 1}
store: {path: /x}
server: {auth_token_file: /t, max_query_hashes: 1024}
heads: {all: {}}
`, "must be in [1, 128]"},
		{"response budget too small for one response", `
net: mainnet
beacon: {genesis_time: 1}
store: {path: /x}
server: {auth_token_file: /t, max_response_bytes_in_flight: 1024}
heads: {all: {}}
`, "must admit at least one"},
		{"ipns without p2p", `
net: mainnet
beacon: {genesis_time: 1}
store: {path: /x}
server: {auth_token_file: /t}
publish: {ipns: true}
heads: {all: {}}
`, "no p2p block to publish from"},
		{"p2p null is not an enable block", `
net: mainnet
beacon: {genesis_time: 1}
store: {path: /x}
server: {auth_token_file: /t}
p2p:
heads: {all: {}}
`, "p2p must be a mapping"},
		{"p2p listen null is not dial-only", `
net: mainnet
beacon: {genesis_time: 1}
store: {path: /x}
server: {auth_token_file: /t}
p2p: {listen: null}
heads: {all: {}}
`, "p2p.listen must be a list"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := LoadConfig(writeFile(t, "config.yaml", tc.yaml))
			if err == nil {
				t.Fatalf("config was accepted, want an error mentioning %q", tc.want)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("err = %v, want it to mention %q", err, tc.want)
			}
		})
	}
}

// TestP2PHost covers the programmatic compatibility path: explicit listeners
// or peers still imply a host even though only the YAML loader can record the
// presence of an otherwise-empty p2p block.
func TestP2PHost(t *testing.T) {
	for _, tt := range []struct {
		name string
		p2p  P2PConfig
		want bool
	}{
		{"empty", P2PConfig{}, false},
		{"listen", P2PConfig{Listen: []string{"/ip4/0.0.0.0/tcp/4001"}}, true},
		{"peers only", P2PConfig{Peers: []string{"/ip4/1.2.3.4/tcp/4001/p2p/x"}}, true},
		{"bitswap settings", P2PConfig{Bitswap: BitswapConfig{SendWorkers: 2}}, true},
		{"announce only", P2PConfig{Announce: []string{"/ip4/1.2.3.4/tcp/4001"}}, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.p2p.Host(); got != tt.want {
				t.Errorf("Host() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestP2PBlockPresenceAndDefaults is the zero-config swarm opt-in contract. An
// absent block is still HTTPS-only. A present block defaults to TCP and QUIC-v1
// on one port with NAT mapping enabled, while an explicit empty listen list is
// deliberately dial-only and an explicit NAT opt-out survives defaulting.
func TestP2PBlockPresenceAndDefaults(t *testing.T) {
	base := `
net: mainnet
beacon: {genesis_time: 1}
store: {path: /var/lib/bloar}
server: {auth_token_file: /t}
heads: {all: {}}
`

	t.Run("absent", func(t *testing.T) {
		cfg := loadString(t, base)
		if cfg.P2P.Host() {
			t.Fatal("an absent p2p block enabled the embedded swarm")
		}
		if len(cfg.P2P.Listen) != 0 || cfg.P2P.NATPortMap {
			t.Errorf("absent p2p defaults = listen %v, nat_port_map %v; want no p2p defaults",
				cfg.P2P.Listen, cfg.P2P.NATPortMap)
		}
	})

	t.Run("present defaults", func(t *testing.T) {
		cfg := loadString(t, base+"p2p: {}\n")
		if !cfg.P2P.Host() {
			t.Fatal("p2p: {} did not enable the embedded swarm")
		}
		want := []string{
			"/ip4/0.0.0.0/tcp/4001",
			"/ip4/0.0.0.0/udp/4001/quic-v1",
		}
		if strings.Join(cfg.P2P.Listen, "|") != strings.Join(want, "|") {
			t.Errorf("p2p.listen = %v, want TCP+QUIC-v1 defaults %v", cfg.P2P.Listen, want)
		}
		if !cfg.P2P.NATPortMap {
			t.Error("p2p.nat_port_map defaulted off, want on")
		}
	})

	t.Run("explicit dial only and NAT opt out", func(t *testing.T) {
		cfg := loadString(t, base+"p2p: {listen: [], nat_port_map: false}\n")
		if !cfg.P2P.Host() {
			t.Fatal("an explicit dial-only p2p block did not enable the host")
		}
		if len(cfg.P2P.Listen) != 0 {
			t.Errorf("explicit listen: [] became %v", cfg.P2P.Listen)
		}
		if cfg.P2P.NATPortMap {
			t.Error("explicit nat_port_map: false was overwritten")
		}
	})

	t.Run("announce participates", func(t *testing.T) {
		cfg := loadString(t, base+`p2p: {announce: ["/ip4/198.51.100.10/tcp/4001"]}
`)
		if !cfg.P2P.Host() {
			t.Fatal("a present announce-only p2p block did not enable the host")
		}
		if len(cfg.P2P.Listen) != 2 {
			t.Errorf("omitted listen with announce defaulted to %v, want TCP+QUIC", cfg.P2P.Listen)
		}
	})
}

// TestP2PIdentityKeyDefault: the identity has to be stable across restarts --
// it is in every multiaddr the document publishes and is the IPNS name itself
// -- so it defaults to a file rather than to a fresh key per process.
func TestP2PIdentityKeyDefault(t *testing.T) {
	cfg := loadString(t, `
net: mainnet
beacon: {genesis_time: 1}
store: {path: /var/lib/bloar}
server: {auth_token_file: /t}
p2p: {listen: ["/ip4/0.0.0.0/tcp/4001"]}
heads: {all: {}}
`)
	if want := filepath.Join("/var/lib/bloar", "p2p.key"); cfg.P2P.IdentityKeyFile != want {
		t.Errorf("p2p.identity_key_file = %q, want the default %q", cfg.P2P.IdentityKeyFile, want)
	}
}

// TestP2PIdentityKeyConfigured: an explicit path wins.
func TestP2PIdentityKeyConfigured(t *testing.T) {
	cfg := loadString(t, `
net: mainnet
beacon: {genesis_time: 1}
store: {path: /var/lib/bloar}
server: {auth_token_file: /t}
p2p:
  listen: ["/ip4/0.0.0.0/tcp/4001"]
  identity_key_file: /etc/bloar/p2p.key
heads: {all: {}}
`)
	if cfg.P2P.IdentityKeyFile != "/etc/bloar/p2p.key" {
		t.Errorf("p2p.identity_key_file = %q, want the configured path", cfg.P2P.IdentityKeyFile)
	}
}

// TestP2PIdentityKeyUnsetWithoutHost: no host, no identity. Defaulting one here
// would have a hostless writer create a key file it has no use for.
func TestP2PIdentityKeyUnsetWithoutHost(t *testing.T) {
	cfg := loadString(t, `
net: mainnet
beacon: {genesis_time: 1}
store: {path: /var/lib/bloar}
server: {auth_token_file: /t}
heads: {all: {}}
`)
	if cfg.P2P.IdentityKeyFile != "" {
		t.Errorf("p2p.identity_key_file = %q, want it unset for a node with no host", cfg.P2P.IdentityKeyFile)
	}
}

// TestIPNSWithP2P: the pair that was refused outright before phase 8 is now the
// supported deployment.
func TestIPNSWithP2P(t *testing.T) {
	cfg := loadString(t, `
net: mainnet
beacon: {genesis_time: 1}
store: {path: /var/lib/bloar}
server: {auth_token_file: /t}
publish: {ipns: true, ipns_republish: 1h}
p2p: {listen: ["/ip4/0.0.0.0/tcp/4001"]}
heads: {all: {}}
`)
	if !cfg.Publish.IPNS {
		t.Error("publish.ipns did not survive parsing")
	}
	if cfg.Publish.IPNSRepublish != time.Hour {
		t.Errorf("publish.ipns_republish = %s, want 1h", cfg.Publish.IPNSRepublish)
	}
}

// followPubkey is a syntactically valid follow.pubkey: 32 bytes of hex. The
// tests here never verify anything with it, but the config refuses a key it
// could not verify with, which is the point.
const followPubkey = "0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20"

// TestFollow covers spec 12's follow block on the deployment spec 11.1
// describes: one node writing one head and following another.
func TestFollow(t *testing.T) {
	cfg := loadString(t, `
net: mainnet
beacon: {genesis_time: 1606824023}
store: {path: /x}
server: {auth_token_file: /t}
p2p: {listen: ["/ip4/0.0.0.0/tcp/4001"]}
heads: {all: {}}
follow:
  url: https://archive.example.org
  pubkey: "`+followPubkey+`"
  poll_interval: 30s
  fetch_timeout: 2s
  verify: full
  heads:
    arbitrum-one:
      pin: {mode: window, duration: 720h}
`)
	if cfg.Follow == nil {
		t.Fatal("the follow block did not survive parsing")
	}
	if cfg.Follow.URL != "https://archive.example.org" {
		t.Errorf("follow.url = %q", cfg.Follow.URL)
	}
	if cfg.Follow.PollInterval != 30*time.Second || cfg.Follow.FetchTimeout != 2*time.Second {
		t.Errorf("follow intervals = %s/%s, want 30s/2s", cfg.Follow.PollInterval, cfg.Follow.FetchTimeout)
	}
	if cfg.Follow.Verify != "full" {
		t.Errorf("follow.verify = %q, want full", cfg.Follow.Verify)
	}
	if _, err := cfg.Follow.Key(); err != nil {
		t.Errorf("follow.pubkey: %v", err)
	}
	if got := cfg.Follow.Heads["arbitrum-one"].Pin; got.Mode != "window" || got.Duration != 720*time.Hour {
		t.Errorf("followed head pin = %+v, want a 720h window", got)
	}
	// A node that writes nothing is a whole role (spec 11.1), so the writer's
	// heads key is not required when follow.heads carries one.
	if cfg.Heads["all"].Pin.Mode != "full" {
		t.Errorf("written head pin mode = %q, want the full default", cfg.Heads["all"].Pin.Mode)
	}
}

// TestFollowDefaults covers spec 12's follow defaults, and the one value that
// deliberately has none.
func TestFollowDefaults(t *testing.T) {
	cfg := loadString(t, `
net: mainnet
beacon: {genesis_time: 1606824023}
store: {path: /x}
server: {auth_token_file: /t}
p2p: {listen: ["/ip4/0.0.0.0/tcp/4001"]}
follow:
  url: https://archive.example.org
  pubkey: "`+followPubkey+`"
  heads:
    all:
      pin: {mode: none}
`)
	if cfg.Follow.PollInterval != follow.DefaultPollInterval {
		t.Errorf("follow.poll_interval = %s, want the %s default", cfg.Follow.PollInterval, follow.DefaultPollInterval)
	}
	if cfg.Follow.FetchTimeout != follow.DefaultFetchTimeout {
		t.Errorf("follow.fetch_timeout = %s, want the %s default", cfg.Follow.FetchTimeout, follow.DefaultFetchTimeout)
	}
	if cfg.Follow.Verify != "cid" {
		t.Errorf("follow.verify = %q, want the cid default", cfg.Follow.Verify)
	}
	if len(cfg.Heads) != 0 {
		t.Errorf("heads = %v, want a pure follower to have none", cfg.Heads)
	}
}

func TestFollowDNSLinkSignerDelegationAndPin(t *testing.T) {
	base := `
net: mainnet
beacon: {genesis_time: 1606824023}
store: {path: /x}
server: {auth_token_file: /t}
p2p: {}
follow:
  url: https://archive.example.org
  dnslink: swarm.example
  heads: {all: {pin: {mode: none}}}
`
	cfg := loadString(t, base)
	if cfg.Follow.DNSLink != "swarm.example" || cfg.Follow.IPNS != "" {
		t.Fatalf("DNSLink authority = dnslink %q ipns %q", cfg.Follow.DNSLink, cfg.Follow.IPNS)
	}
	if key, err := cfg.Follow.Key(); err != nil || key != nil {
		t.Fatalf("unpinned DNSLink key = %x, err = %v", key, err)
	}

	pinned := strings.Replace(base, "  heads:", "  pubkey: \""+followPubkey+"\"\n  heads:", 1)
	cfg = loadString(t, pinned)
	if key, err := cfg.Follow.Key(); err != nil || len(key) != ed25519.PublicKeySize {
		t.Fatalf("pinned DNSLink key length = %d, err = %v", len(key), err)
	}
}

// TestFollowErrors covers the follow configs that must not start a daemon.
func TestFollowErrors(t *testing.T) {
	const head = `
net: mainnet
beacon: {genesis_time: 1606824023}
store: {path: /x}
server: {auth_token_file: /t}
p2p: {listen: ["/ip4/0.0.0.0/tcp/4001"]}
`
	for _, tc := range []struct{ name, yaml, want string }{
		{"no channel", head + `
follow:
  pubkey: "` + followPubkey + `"
  heads: {all: {pin: {mode: none}}}
`, "needs a channel"},
		{"no pubkey", head + `
follow:
  url: https://archive.example.org
  heads: {all: {pin: {mode: none}}}
`, "follow.pubkey is required"},
		{"direct IPNS and DNSLink", head + `
follow:
  ipns: k51qzi5uqu5dmc9hz7x2fd156p883lc3w1i36tu4i4r0yd7ohnd4a12j9zeun8
  dnslink: swarm.example
  pubkey: "` + followPubkey + `"
  heads: {all: {pin: {mode: none}}}
`, "mutually exclusive"},
		{"invalid DNSLink domain", head + `
follow:
  dnslink: "https://swarm.example/path"
  heads: {all: {pin: {mode: none}}}
`, "not a valid DNS name"},
		{"short pubkey", head + `
follow:
  url: https://archive.example.org
  pubkey: "deadbeef"
  heads: {all: {pin: {mode: none}}}
`, "want an ed25519 public key"},
		{"no heads to follow", head + `
follow:
  url: https://archive.example.org
  pubkey: "` + followPubkey + `"
`, "follow.heads is empty"},
		{"no pin policy", head + `
follow:
  url: https://archive.example.org
  pubkey: "` + followPubkey + `"
  heads: {all: {}}
`, "pin.mode is required"},
		{"window without duration", head + `
follow:
  url: https://archive.example.org
  pubkey: "` + followPubkey + `"
  heads: {all: {pin: {mode: window}}}
`, "no duration is set"},
		{"bad verify", head + `
follow:
  url: https://archive.example.org
  pubkey: "` + followPubkey + `"
  verify: paranoid
  heads: {all: {pin: {mode: none}}}
`, "must be one of cid, full"},
		// Spec 11.1: exactly one writer per head.
		{"written and followed", head + `
heads: {all: {}}
follow:
  url: https://archive.example.org
  pubkey: "` + followPubkey + `"
  heads: {all: {pin: {mode: none}}}
`, "exactly one writer"},
		// Spec 11.2: the whole protocol is bitswap, and there is no host.
		{"follow without p2p", `
net: mainnet
beacon: {genesis_time: 1606824023}
store: {path: /x}
server: {auth_token_file: /t}
follow:
  url: https://archive.example.org
  pubkey: "` + followPubkey + `"
  heads: {all: {pin: {mode: none}}}
`, "cannot fetch a single block without a host"},
		{"ipns without p2p", `
net: mainnet
beacon: {genesis_time: 1606824023}
store: {path: /x}
server: {auth_token_file: /t}
follow:
  ipns: k51qzi5uqu5dmc9hz7x2fd156p883lc3w1i36tu4i4r0yd7ohnd4a12j9zeun8
  pubkey: "` + followPubkey + `"
  heads: {all: {pin: {mode: none}}}
`, "no p2p block to resolve its IPNS name from"},
		{"dnslink without p2p", `
net: mainnet
beacon: {genesis_time: 1606824023}
store: {path: /x}
server: {auth_token_file: /t}
follow:
  dnslink: swarm.example
  heads: {all: {pin: {mode: none}}}
`, "no p2p block to resolve its IPNS name from"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := LoadConfig(writeFile(t, "config.yaml", tc.yaml))
			if err == nil {
				t.Fatalf("config was accepted, want an error mentioning %q", tc.want)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("err = %v, want it to mention %q", err, tc.want)
			}
		})
	}
}

// TestSpecMap covers the beacon convention that every value in a spec map is a
// string, whatever it looked like in YAML.
func TestSpecMap(t *testing.T) {
	cfg := loadString(t, `
net: mainnet
beacon:
  genesis_time: 1606824023
  spec_extra:
    DEPOSIT_CHAIN_ID: 1
    DEPOSIT_NETWORK_ID: "1"
    PRESET_BASE: mainnet
    SOMETHING_TRUE: true
store: {path: /x}
server: {auth_token_file: /t}
heads: {all: {}}
`)

	got, err := cfg.SpecMap()
	if err != nil {
		t.Fatalf("SpecMap: %v", err)
	}
	want := map[string]string{
		"DEPOSIT_CHAIN_ID":   "1",
		"DEPOSIT_NETWORK_ID": "1",
		"PRESET_BASE":        "mainnet",
		"SOMETHING_TRUE":     "true",
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("spec_extra.%s = %q, want %q", k, got[k], v)
		}
	}
}

// TestAuthTokenFile covers the token file's handling, including the trailing
// newline every editor adds.
func TestAuthTokenFile(t *testing.T) {
	cfg := &Config{}

	cfg.Server.AuthTokenFile = writeFile(t, "token", "s3cret\n")
	got, err := cfg.AuthToken()
	if err != nil {
		t.Fatalf("AuthToken: %v", err)
	}
	if got != "s3cret" {
		t.Errorf("AuthToken = %q, want %q: a trailing newline is not part of the secret", got, "s3cret")
	}

	cfg.Server.AuthTokenFile = writeFile(t, "empty", "  \n")
	if _, err := cfg.AuthToken(); err == nil {
		t.Error("an empty token file was accepted; it would authorize every empty bearer token")
	}

	cfg.Server.AuthTokenFile = filepath.Join(t.TempDir(), "absent")
	if _, err := cfg.AuthToken(); err == nil {
		t.Error("a missing token file was accepted")
	}
}

// TestResolveTokenFile covers the ${CREDENTIALS_DIRECTORY} handling that lets one
// config serve both the systemd credential handoff (deploy/systemd, audit
// the safety boundary) and a plain file path (manual, container, docker-compose).
func TestResolveTokenFile(t *testing.T) {
	// A plain path is returned untouched, whether or not a credential directory
	// happens to be in the environment.
	t.Run("plain path unchanged", func(t *testing.T) {
		t.Setenv("CREDENTIALS_DIRECTORY", "/run/credentials/bloard.service")
		got, err := resolveTokenFile("/etc/bloar/token")
		if err != nil {
			t.Fatalf("resolveTokenFile: %v", err)
		}
		if got != "/etc/bloar/token" {
			t.Fatalf("plain path changed: got %q", got)
		}
	})

	// The credential prefix expands to the delivered copy under the directory
	// systemd set for the unit.
	t.Run("credential prefix expands", func(t *testing.T) {
		t.Setenv("CREDENTIALS_DIRECTORY", "/run/credentials/bloard.service")
		got, err := resolveTokenFile("${CREDENTIALS_DIRECTORY}/token")
		if err != nil {
			t.Fatalf("resolveTokenFile: %v", err)
		}
		want := "/run/credentials/bloard.service/token"
		if got != want {
			t.Fatalf("resolved path: got %q, want %q", got, want)
		}
	})

	// The prefix with an unset directory is a hard error, never a fallthrough to a
	// literal /token: this is a unit missing LoadCredential=, or the config run
	// outside systemd, and the error must name the variable.
	t.Run("credential prefix without directory errors", func(t *testing.T) {
		t.Setenv("CREDENTIALS_DIRECTORY", "")
		got, err := resolveTokenFile("${CREDENTIALS_DIRECTORY}/token")
		if err == nil {
			t.Fatalf("a credential-style token_file resolved to %q with no CREDENTIALS_DIRECTORY", got)
		}
		if !strings.Contains(err.Error(), "CREDENTIALS_DIRECTORY") {
			t.Fatalf("error does not name the variable: %v", err)
		}
	})

	// Only the exact ${CREDENTIALS_DIRECTORY} prefix is a credential reference:
	// this is not general interpolation, so any other $-looking value is a plain
	// path and is not expanded.
	t.Run("other variables are not interpolated", func(t *testing.T) {
		t.Setenv("CREDENTIALS_DIRECTORY", "/run/credentials/x")
		t.Setenv("HOME", "/home/somebody")
		got, err := resolveTokenFile("${HOME}/token")
		if err != nil {
			t.Fatalf("resolveTokenFile: %v", err)
		}
		if got != "${HOME}/token" {
			t.Fatalf("a non-credential variable was expanded: got %q", got)
		}
	})
}

// minimalCredentialConfig is a valid bloard config whose auth_token_file is the
// systemd credential form.
const minimalCredentialConfig = `
net: mainnet
beacon:
  genesis_time: 1606824023
store:
  path: /var/lib/bloar
server:
  auth_token_file: "${CREDENTIALS_DIRECTORY}/token"
heads:
  all: {}
`

// TestAuthTokenResolvesCredentialTokenFile proves the systemd form works at the
// read: LoadConfig accepts a ${CREDENTIALS_DIRECTORY}/token config with no
// variable in the environment (so token-free offline commands load it), and
// AuthToken(), given the credential directory, resolves to the delivered copy and
// reads it.
func TestAuthTokenResolvesCredentialTokenFile(t *testing.T) {
	cfg, err := LoadConfig(writeFile(t, "config.yaml", minimalCredentialConfig))
	if err != nil {
		t.Fatalf("LoadConfig of a credential-form config: %v", err)
	}
	// Nothing is resolved at load; the credential form is left intact.
	if want := "${CREDENTIALS_DIRECTORY}/token"; cfg.Server.AuthTokenFile != want {
		t.Fatalf("auth_token_file changed at load: got %q, want %q", cfg.Server.AuthTokenFile, want)
	}

	credDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(credDir, "token"), []byte("s3cret\n"), 0o400); err != nil {
		t.Fatalf("writing the delivered token: %v", err)
	}
	t.Setenv("CREDENTIALS_DIRECTORY", credDir)
	got, err := cfg.AuthToken()
	if err != nil {
		t.Fatalf("AuthToken: %v", err)
	}
	if got != "s3cret" {
		t.Fatalf("token: got %q, want %q", got, "s3cret")
	}
}

// TestAuthTokenRejectsCredentialTokenFileWithoutDir proves resolution moved to
// the read without losing the fail-closed guarantee: LoadConfig of the credential
// form succeeds with no CREDENTIALS_DIRECTORY (offline commands load), but
// AuthToken() -- which serve() calls before it binds anything -- fails with an
// error naming both the variable and the key, never a literal /token.
func TestAuthTokenRejectsCredentialTokenFileWithoutDir(t *testing.T) {
	t.Setenv("CREDENTIALS_DIRECTORY", "")
	cfg, err := LoadConfig(writeFile(t, "config.yaml", minimalCredentialConfig))
	if err != nil {
		t.Fatalf("LoadConfig must not need the credential directory: %v", err)
	}
	if _, err := cfg.AuthToken(); err == nil {
		t.Fatal("AuthToken read a credential-style token_file with no CREDENTIALS_DIRECTORY")
	} else if !strings.Contains(err.Error(), "CREDENTIALS_DIRECTORY") {
		t.Fatalf("error does not name the variable: %v", err)
	} else if !strings.Contains(err.Error(), "server.auth_token_file") {
		t.Fatalf("error does not name the key: %v", err)
	}
}

// TestSigningKeyFile covers both accepted key encodings and the rejections.
func TestSigningKeyFile(t *testing.T) {
	cfg := &Config{}

	t.Run("unset", func(t *testing.T) {
		key, err := cfg.SigningKey()
		if err != nil || key != nil {
			t.Errorf("SigningKey with no file = (%v, %v), want (nil, nil)", key, err)
		}
	})

	t.Run("seed", func(t *testing.T) {
		cfg.Publish.SigningKeyFile = writeFile(t, "seed.key", strings.Repeat("ab", 32)+"\n")
		key, err := cfg.SigningKey()
		if err != nil {
			t.Fatalf("SigningKey: %v", err)
		}
		if len(key) != 64 {
			t.Errorf("a 32-byte seed expanded to %d bytes, want a 64-byte private key", len(key))
		}
	})

	t.Run("private key", func(t *testing.T) {
		// A consistent 64-byte expanded key: seed || the public half that derives
		// from it. An inconsistent one is now rejected; see
		// TestInconsistentExpandedSigningKeyIsRejected.
		expanded := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0xcd}, ed25519.SeedSize))
		cfg.Publish.SigningKeyFile = writeFile(t, "full.key", hex.EncodeToString(expanded))
		key, err := cfg.SigningKey()
		if err != nil {
			t.Fatalf("SigningKey: %v", err)
		}
		if len(key) != 64 {
			t.Errorf("key is %d bytes, want 64", len(key))
		}
	})

	for _, tc := range []struct{ name, content string }{
		{"not hex", "zzzz"},
		{"wrong length", strings.Repeat("ab", 20)},
		{"empty", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg.Publish.SigningKeyFile = writeFile(t, tc.name+".key", tc.content)
			if _, err := cfg.SigningKey(); err == nil {
				t.Error("a bad key file was accepted")
			}
		})
	}
}

// loadString writes yaml to a temp file and loads it.
func loadString(t *testing.T, yaml string) *Config {
	t.Helper()
	cfg, err := LoadConfig(writeFile(t, "config.yaml", yaml))
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	return cfg
}

// writeFile writes content to a uniquely-named temp file and returns its path.
func writeFile(t *testing.T, name, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("writing %s: %v", name, err)
	}
	return path
}

// TestExampleConfigsParse checks the shipped examples against the real loader.
//
// They are documentation, and documentation about a strictly-decoded config
// rots silently: a renamed key makes deploy/examples/*.yaml a file that no
// longer starts a daemon, and nothing would say so until an operator copied it.
// Decoding is KnownFields(true), so this catches a stale key as well as a
// missing required one.
func TestExampleConfigsParse(t *testing.T) {
	// The shipped examples are the systemd form (auth_token_file is
	// ${CREDENTIALS_DIRECTORY}/token), and they must load with NO credential
	// directory in the environment: resolution is deferred to AuthToken() so that
	// token-free offline commands parse the installed config unchanged. This test
	// therefore sets nothing; the credential form is resolved and exercised in
	// TestAuthTokenResolvesCredentialTokenFile.
	dir, err := filepath.Abs(filepath.Join("..", "..", "deploy", "examples"))
	if err != nil {
		t.Fatalf("resolving the examples directory: %v", err)
	}
	for _, name := range []string{"writer.yaml", "follower.yaml"} {
		t.Run(name, func(t *testing.T) {
			cfg, err := LoadConfig(filepath.Join(dir, name))
			if err != nil {
				t.Fatalf("deploy/examples/%s does not load: %v", name, err)
			}
			// These are the systemd-installed configs, so their token MUST be the
			// exact credential form (§3.1). A plain path here would reproduce the
			// the safety boundary crash loop the credential handoff fixes.
			if want := "${CREDENTIALS_DIRECTORY}/token"; cfg.Server.AuthTokenFile != want {
				t.Errorf("deploy/examples/%s: server.auth_token_file = %q, want the credential form %q",
					name, cfg.Server.AuthTokenFile, want)
			}
		})
	}
}
