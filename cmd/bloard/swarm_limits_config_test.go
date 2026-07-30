package main

import (
	"strings"
	"testing"
	"time"

	"golang.org/x/time/rate"

	"github.com/blobarchive/bloar/p2p"
)

const limitsConfigBase = `
net: mainnet
beacon: {genesis_time: 1}
store: {path: /var/lib/bloar}
server: {auth_token_file: /t}
heads: {all: {}}
`

func TestPublicReadAdmissionDefaultsOn(t *testing.T) {
	cfg := loadString(t, limitsConfigBase)
	a := cfg.Server.PublicReadAdmission
	if a.Enabled == nil || !*a.Enabled {
		t.Fatal("server.public_read_admission defaulted off")
	}
	if a.GlobalRate != defaultPublicReadGlobalRate || a.GlobalBurst != defaultPublicReadGlobalBurst {
		t.Errorf("global defaults = %g/%d, want %v/%d", a.GlobalRate, a.GlobalBurst,
			defaultPublicReadGlobalRate, defaultPublicReadGlobalBurst)
	}
	if a.ClientRate != defaultPublicReadClientRate || a.ClientBurst != defaultPublicReadClientBurst {
		t.Errorf("client defaults = %g/%d, want %v/%d", a.ClientRate, a.ClientBurst,
			defaultPublicReadClientRate, defaultPublicReadClientBurst)
	}
	if a.ClientBuckets != defaultPublicReadClientBuckets || a.ClientBucketTTL != defaultPublicReadClientTTL {
		t.Errorf("client cache defaults = %d/%s, want %d/%s", a.ClientBuckets, a.ClientBucketTTL,
			defaultPublicReadClientBuckets, defaultPublicReadClientTTL)
	}
	limiter, err := cfg.Server.publicReadLimiterConfig(nil)
	if err != nil {
		t.Fatalf("publicReadLimiterConfig: %v", err)
	}
	if limiter == nil {
		t.Fatal("default-on admission translated to nil")
	}
	if limiter.GlobalRate != rate.Limit(defaultPublicReadGlobalRate) ||
		limiter.PerClientRate != rate.Limit(defaultPublicReadClientRate) {
		t.Errorf("translated rates = %v/%v", limiter.GlobalRate, limiter.PerClientRate)
	}
}

func TestPublicReadAdmissionExplicitOptOutAndTrustedProxyMapping(t *testing.T) {
	cfg := loadString(t, `
net: mainnet
beacon: {genesis_time: 1}
store: {path: /var/lib/bloar}
server:
  auth_token_file: /t
  public_read_admission:
    enabled: false
    global_rate: 500
    global_burst: 1000
    client_rate: 250
    client_burst: 500
    client_buckets: 64
    client_bucket_ttl: 2m
    trusted_proxy_header: X-Forwarded-For
    trusted_proxy_cidrs: ["127.0.0.0/8", "2001:db8::/32"]
heads: {all: {}}
`)
	a := cfg.Server.PublicReadAdmission
	if a.Enabled == nil || *a.Enabled {
		t.Fatal("explicit enabled: false was overwritten")
	}
	if got, err := cfg.Server.publicReadLimiterConfig(nil); err != nil {
		t.Fatalf("disabled valid config: %v", err)
	} else if got != nil {
		t.Fatal("disabled admission produced a server limiter")
	}

	// Flip only the effective switch to inspect the exact library mapping.
	*a.Enabled = true
	got, err := cfg.Server.publicReadLimiterConfig(nil)
	if err != nil {
		t.Fatalf("enabled config: %v", err)
	}
	if got.GlobalRate != 500 || got.GlobalBurst != 1000 || got.PerClientRate != 250 || got.PerClientBurst != 500 {
		t.Errorf("rate mapping = %+v", got)
	}
	if got.MaxClientBuckets != 64 || got.ClientBucketTTL != 2*time.Minute {
		t.Errorf("client cache mapping = %d/%s", got.MaxClientBuckets, got.ClientBucketTTL)
	}
	if got.ForwardedHeader != "X-Forwarded-For" || len(got.TrustedProxies) != 2 ||
		got.TrustedProxies[0].String() != "127.0.0.0/8" || got.TrustedProxies[1].String() != "2001:db8::/32" {
		t.Errorf("trusted-proxy mapping = header %q prefixes %v", got.ForwardedHeader, got.TrustedProxies)
	}
}

func TestPublicReadAdmissionRejectsInvalidYAML(t *testing.T) {
	block := func(lines string) string {
		return `
net: mainnet
beacon: {genesis_time: 1}
store: {path: /var/lib/bloar}
server:
  auth_token_file: /t
  public_read_admission:
` + lines + `
heads: {all: {}}
`
	}
	for _, tc := range []struct {
		name, lines, want string
	}{
		{"negative global rate", "    global_rate: -1", "GlobalRate"},
		{"small global burst", "    global_burst: 128", "maximum request charge 129"},
		{"negative client rate", "    client_rate: -1", "PerClientRate"},
		{"small client burst", "    client_burst: 128", "maximum request charge 129"},
		{"negative bucket cap", "    client_buckets: -1", "MaxClientBuckets"},
		{"negative bucket ttl", "    client_bucket_ttl: -1s", "ClientBucketTTL"},
		{"header without CIDRs", "    trusted_proxy_header: X-Forwarded-For", "configured together"},
		{"CIDRs without header", "    trusted_proxy_cidrs: [\"127.0.0.0/8\"]", "configured together"},
		{"bare proxy address", "    trusted_proxy_header: X-Forwarded-For\n    trusted_proxy_cidrs: [\"127.0.0.1\"]", "is not a CIDR"},
		{"whitespace proxy CIDR", "    trusted_proxy_header: X-Forwarded-For\n    trusted_proxy_cidrs: [\" 127.0.0.0/8\"]", "without surrounding whitespace"},
		{"host bits proxy CIDR", "    trusted_proxy_header: X-Forwarded-For\n    trusted_proxy_cidrs: [\"192.0.2.9/24\"]", "host bits set"},
		{"mapped proxy CIDR", "    trusted_proxy_header: X-Forwarded-For\n    trusted_proxy_cidrs: [\"::ffff:192.0.2.0/120\"]", "native IPv4 or IPv6"},
		{"duplicate proxy CIDR", "    trusted_proxy_header: X-Forwarded-For\n    trusted_proxy_cidrs: [\"192.0.2.0/24\", \"192.0.2.0/24\"]", "duplicated"},
		{"invalid proxy header", "    trusted_proxy_header: \"X Forwarded For\"\n    trusted_proxy_cidrs: [\"192.0.2.0/24\"]", "not a valid HTTP field name"},
		// Disabled means bypass at runtime, not "skip validation". Otherwise a
		// one-line enable later could activate a malformed trust boundary.
		{"disabled malformed trust", "    enabled: false\n    trusted_proxy_header: X-Forwarded-For\n    trusted_proxy_cidrs: [\"192.0.2.9/24\"]", "host bits set"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := LoadConfig(writeFile(t, "config.yaml", block(tc.lines)))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("LoadConfig error = %v, want mention %q", err, tc.want)
			}
		})
	}
}

func TestBitswapDefaultsOnlyWithHost(t *testing.T) {
	hostless := loadString(t, limitsConfigBase)
	if hostless.P2P.Bitswap.Serve != nil || hostless.P2P.Bitswap.MaxQueuedWantsPerPeer != 0 ||
		hostless.P2P.Bitswap.BlockstoreWorkers != 0 {
		t.Fatalf("hostless config acquired irrelevant Bitswap defaults: %+v", hostless.P2P.Bitswap)
	}

	hosted := loadString(t, limitsConfigBase+"p2p: {}\n")
	b := hosted.P2P.Bitswap
	if b.Serve == nil || !*b.Serve {
		t.Fatal("hosted Bitswap defaulted to not serving")
	}
	if b.MaxQueuedWantsPerPeer != p2p.DefaultBitswapMaxQueuedWantlistEntriesPerPeer ||
		b.MaxOutstandingBytesPerPeer != p2p.DefaultBitswapMaxOutstandingBytesPerPeer ||
		b.SendWorkers != p2p.DefaultBitswapTaskWorkerCount ||
		b.EngineTaskWorkers != p2p.DefaultBitswapEngineTaskWorkerCount ||
		b.BlockstoreWorkers != p2p.DefaultBitswapEngineBlockstoreWorkerCount ||
		b.MaxCIDBytes != p2p.DefaultBitswapMaxCIDSize {
		t.Errorf("Bitswap defaults = %+v", b)
	}
}

func TestBitswapConfigMapsEveryCapAndServeOptOut(t *testing.T) {
	cfg := loadString(t, limitsConfigBase+`p2p:
  bitswap:
    serve: false
    max_queued_wants_per_peer: 10
    max_outstanding_bytes_per_peer: 20
    send_workers: 3
    engine_task_workers: 4
    blockstore_workers: 5
    max_cid_bytes: 36
`)
	got := cfg.P2P.Bitswap.exchangeConfig(nil, nil, nil)
	if !got.DisableServer || got.MaxQueuedWantlistEntriesPerPeer != 10 ||
		got.MaxOutstandingBytesPerPeer != 20 || got.TaskWorkerCount != 3 ||
		got.EngineTaskWorkerCount != 4 || got.EngineBlockstoreWorkerCount != 5 || got.MaxCIDSize != 36 {
		t.Fatalf("Bitswap exchange mapping = %+v", got)
	}
}

func TestBitswapConfigRejectsCIDCapBelowBloarWireFormat(t *testing.T) {
	yaml := limitsConfigBase + `p2p:
  bitswap:
    max_cid_bytes: 35
`
	if _, err := LoadConfig(writeFile(t, "config.yaml", yaml)); err == nil {
		t.Fatal("CID cap below Bloar's 36-byte wire identifiers was accepted")
	}
}

func TestBitswapConfigRejectsInvalidCapsEvenWhenServingOff(t *testing.T) {
	for _, key := range []string{
		"max_queued_wants_per_peer",
		"max_outstanding_bytes_per_peer",
		"send_workers",
		"engine_task_workers",
		"blockstore_workers",
		"max_cid_bytes",
	} {
		t.Run(key, func(t *testing.T) {
			yaml := limitsConfigBase + "p2p:\n  bitswap:\n    serve: false\n    " + key + ": -1\n"
			if _, err := LoadConfig(writeFile(t, "config.yaml", yaml)); err == nil {
				t.Fatalf("negative %s was accepted", key)
			}
		})
	}
}
