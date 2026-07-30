package main

import (
	"strings"
	"testing"

	"github.com/blobarchive/bloar/p2p"
)

func TestRendezvousAndPublicAminoDefaultsApplyOnlyToEmbeddedHost(t *testing.T) {
	t.Parallel()
	hostless := loadString(t, limitsConfigBase)
	if hostless.P2P.DHT.Bootstrap != "" || hostless.P2P.Rendezvous.Enabled != nil {
		t.Fatalf("hostless config acquired swarm defaults: dht=%+v rendezvous=%+v",
			hostless.P2P.DHT, hostless.P2P.Rendezvous)
	}

	hosted := loadString(t, limitsConfigBase+"p2p: {}\n")
	if hosted.P2P.DHT.Bootstrap != "public" {
		t.Fatalf("p2p.dht.bootstrap = %q, want default public", hosted.P2P.DHT.Bootstrap)
	}
	if hosted.P2P.Rendezvous.Enabled == nil || !*hosted.P2P.Rendezvous.Enabled {
		t.Fatal("embedded rendezvous did not default on")
	}
}

func TestPrivateBootstrapAndRendezvousOptOutAreExplicit(t *testing.T) {
	t.Parallel()
	cfg := loadString(t, limitsConfigBase+`p2p:
  dht: {bootstrap: private}
  rendezvous: {enabled: false}
`)
	if cfg.P2P.DHT.Bootstrap != "private" {
		t.Fatalf("p2p.dht.bootstrap = %q, want private", cfg.P2P.DHT.Bootstrap)
	}
	if cfg.P2P.Rendezvous.Enabled == nil || *cfg.P2P.Rendezvous.Enabled {
		t.Fatal("explicit rendezvous enabled: false was overwritten")
	}
}

func TestDHTBootstrapModeAndNestedDiscoveryYAMLAreStrict(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, block, want string
	}{
		{"unknown bootstrap mode", "  dht: {bootstrap: automatic}\n", "must be public or private"},
		{"unknown dht field", "  dht: {bootstrap: private, network: test}\n", "field network not found"},
		{"unknown rendezvous field", "  rendezvous: {enabled: true, magic: true}\n", "field magic not found"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			yaml := limitsConfigBase + "p2p:\n" + tc.block
			_, err := LoadConfig(writeFile(t, "config.yaml", yaml))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("LoadConfig error = %v, want mention %q", err, tc.want)
			}
		})
	}
}

func TestRendezvousTargetsAreSortedUnionOfWrittenAndFollowedHeads(t *testing.T) {
	t.Parallel()
	cfg := &Config{
		Net: "mainnet",
		Heads: map[string]HeadConfig{
			"z-written": {},
			"all":       {},
		},
		Follow: &FollowConfig{Heads: map[string]FollowHeadConfig{
			"arbitrum-one": {},
			"all":          {}, // Defensive dedupe; validation normally rejects overlap.
		}},
	}
	got := rendezvousTargets(cfg)
	want := []p2p.RendezvousTarget{
		{Network: "mainnet", Head: "all"},
		{Network: "mainnet", Head: "arbitrum-one"},
		{Network: "mainnet", Head: "z-written"},
	}
	if len(got) != len(want) {
		t.Fatalf("targets = %+v, want %+v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("target[%d] = %+v, want %+v", i, got[i], want[i])
		}
	}
}

func TestRendezvousAdvertisingCannotOutrunBitswapServing(t *testing.T) {
	t.Parallel()
	for _, serving := range []bool{true, false} {
		serving := serving
		cfg := &Config{
			Net:   "mainnet",
			Heads: map[string]HeadConfig{"all": {}},
			P2P:   P2PConfig{Bitswap: BitswapConfig{Serve: &serving}},
		}
		got := rendezvousConfig(cfg, nil, nil, newLogger())
		if got.DisableProviding != !serving {
			t.Fatalf("bitswap serve=%v mapped DisableProviding=%v", serving, got.DisableProviding)
		}
		if len(got.Targets) != 1 || got.Targets[0] != (p2p.RendezvousTarget{Network: "mainnet", Head: "all"}) {
			t.Fatalf("mapped rendezvous targets = %+v", got.Targets)
		}
	}
}
