package main

// Every configured follow_head_ready{head} series is initialised to
// 0 in setupMetrics, in the same pass that registers the readiness gates -- so a
// scrape taken immediately after metrics setup, before the follower is wired, already
// shows each configured followed head at 0 rather than absent.

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/blobarchive/bloar/store"
)

func TestFollowHeadReadyInitialisedAtMetricsSetup(t *testing.T) {
	dir := t.TempDir()
	st, err := store.Open(filepath.Join(dir, "store"), store.WithPebbleLogger(quietPebble{}))
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	token := filepath.Join(dir, "token")
	if err := os.WriteFile(token, []byte("test-token"), 0o600); err != nil {
		t.Fatalf("writing token: %v", err)
	}
	metricsAddr := freeAddr(t)
	cfg := loadString(t, fmt.Sprintf(`
net: mainnet
beacon: {genesis_time: 1606824023, seconds_per_slot: 12}
store: {path: %s}
server: {listen: "%s", auth_token_file: %s, metrics_listen: "%s"}
p2p: {listen: ["/ip4/127.0.0.1/tcp/0"]}
follow:
  url: https://writer.example.org
  pubkey: "0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20"
  heads:
    all: {pin: {mode: none}}
    arb: {pin: {mode: none}}
`, filepath.Join(dir, "store"), freeAddr(t), token, metricsAddr))

	_, _, stop, err := setupMetrics(t.Context(), cfg, st, newLogger())
	if err != nil {
		t.Fatalf("setupMetrics: %v", err)
	}
	t.Cleanup(func() { stop(newLogger()) })

	// An immediate scrape -- no follower has been wired -- already shows both heads.
	_, body := httpGet(t, metricsAddr, "/metrics")
	for _, head := range []string{"all", "arb"} {
		want := fmt.Sprintf(`bloar_follow_head_ready{head=%q} 0`, head)
		if !strings.Contains(body, want) {
			t.Errorf("follow_head_ready for %q was not initialised to 0 at metrics setup:\n%s", head, body)
		}
	}
}
