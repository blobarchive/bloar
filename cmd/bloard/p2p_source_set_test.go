package main

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/libp2p/go-libp2p/core/peer"
)

func TestFollowsIPNSIncludesEverySourceSetNameChannel(t *testing.T) {
	for _, tc := range []struct {
		name string
		cfg  *Config
		want bool
	}{
		{name: "no follow", cfg: &Config{}},
		{name: "singular URL", cfg: &Config{Follow: &FollowConfig{URL: "https://writer.example"}}},
		{name: "singular IPNS", cfg: &Config{Follow: &FollowConfig{IPNS: "name"}}, want: true},
		{name: "singular DNSLink", cfg: &Config{Follow: &FollowConfig{DNSLink: "writer.example"}}, want: true},
		{name: "source URLs", cfg: &Config{Follow: &FollowConfig{Sources: map[string]FollowSourceConfig{
			"writer-a": {URL: "https://writer.example"},
		}}}},
		{name: "source IPNS", cfg: &Config{Follow: &FollowConfig{Sources: map[string]FollowSourceConfig{
			"writer-a": {URL: "https://writer.example"}, "writer-b": {IPNS: "name"},
		}}}, want: true},
		{name: "source DNSLink", cfg: &Config{Follow: &FollowConfig{Sources: map[string]FollowSourceConfig{
			"writer-a": {DNSLink: "writer.example"},
		}}}, want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.cfg.followsIPNS(); got != tc.want {
				t.Fatalf("followsIPNS() = %t, want %t", got, tc.want)
			}
		})
	}
}

func TestSourceSetIPNSBuildsDHTWhenRendezvousIsDisabled(t *testing.T) {
	t.Parallel()
	const directIPNS = "k51qzi5uqu5dmc9hz7x2fd156p883lc3w1i36tu4i4r0yd7ohnd4a12j9zeun8"
	sources := testSources()
	writerA := sources["writer-a"]
	writerA.IPNS = directIPNS
	sources["writer-a"] = writerA
	body := renderSourceSetConfig(t, sources, testSourceHeads(), 1, "", "")
	dir := t.TempDir()
	body = strings.Replace(body, "store: {path: /x}", fmt.Sprintf("store: {path: %s}", filepath.Join(dir, "store")), 1)
	body = strings.Replace(body, "p2p: {}", `p2p:
  listen: ["/ip4/127.0.0.1/tcp/0"]
  nat_port_map: false
  dht: {bootstrap: private}
  rendezvous: {enabled: false}`, 1)
	cfg := loadString(t, body)
	st := openStore(t, cfg.Store.Path)
	defer st.Close()

	n, err := setupP2PWithDeps(t.Context(), cfg, st, nil, nil, newLogger(), p2pSetupDeps{
		publicBootstrapPeers: func() []peer.AddrInfo {
			t.Fatal("private source-set DHT consulted public bootstrap peers")
			return nil
		},
	})
	if err != nil {
		t.Fatalf("setupP2PWithDeps: %v", err)
	}
	defer n.close(newLogger())
	if n.host == nil || n.exchange == nil || n.dht == nil {
		t.Fatalf("source-set IPNS routing stack incomplete: host=%t exchange=%t dht=%t",
			n.host != nil, n.exchange != nil, n.dht != nil)
	}
	if n.rendezvous != nil || n.publisher != nil {
		t.Fatalf("source-set follower built unrelated services: rendezvous=%t publisher=%t",
			n.rendezvous != nil, n.publisher != nil)
	}
}
