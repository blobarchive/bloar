package main

import (
	"strings"
	"testing"

	"github.com/blobarchive/bloar/server"
)

const mutableWriterConfig = `
net: mainnet
beacon: {genesis_time: 1606824023}
store: {path: /x}
server: {auth_token_file: /t}
publish: {signing_key_file: /keys/publication.key}
heads:
  all:
    pin: {mode: full}
  unfinalized:
    kind: unfinalized-mutable
    handoff_head: all
    max_window_slots: 96
    pin: {mode: full}
`

func TestMutableWriterConfig(t *testing.T) {
	cfg := loadString(t, mutableWriterConfig)
	h := cfg.Heads["unfinalized"]
	if h.effectiveKind() != server.UnfinalizedMutable || h.HandoffHead != "all" || h.MaxWindowSlots != 96 {
		t.Fatalf("mutable writer config = kind %q, handoff %q, max %d", h.effectiveKind(), h.HandoffHead, h.MaxWindowSlots)
	}
	if cfg.Heads["all"].effectiveKind() != server.FinalizedMonotonic {
		t.Fatal("omitted writer kind did not preserve finalized-monotonic")
	}
}

func TestMutableWriterConfigRefusals(t *testing.T) {
	for _, tc := range []struct {
		name, body, want string
	}{
		{"unsigned", strings.Replace(mutableWriterConfig, "publish: {signing_key_file: /keys/publication.key}\n", "", 1), "signing_key_file is empty"},
		{"non-full retention", strings.Replace(mutableWriterConfig, "max_window_slots: 96\n    pin: {mode: full}", "max_window_slots: 96\n    pin: {mode: none}", 1), "must use pin.mode full"},
		{"self handoff", strings.Replace(mutableWriterConfig, "handoff_head: all", "handoff_head: unfinalized", 1), "cannot name itself"},
		{"unknown handoff", strings.Replace(mutableWriterConfig, "handoff_head: all", "handoff_head: missing", 1), "is not a locally written head"},
		{"zero bound", strings.Replace(mutableWriterConfig, "max_window_slots: 96", "max_window_slots: 0", 1), "must be in [1,4096]"},
		{"oversize bound", strings.Replace(mutableWriterConfig, "max_window_slots: 96", "max_window_slots: 4097", 1), "must be in [1,4096]"},
		{"unknown kind", strings.Replace(mutableWriterConfig, "unfinalized-mutable", "eventually-consistent", 1), "must be \"finalized-monotonic\" or \"unfinalized-mutable\""},
	} {
		t.Run(tc.name, func(t *testing.T) { assertConfigError(t, tc.body, tc.want) })
	}
}

func TestMutableFollowerConfig(t *testing.T) {
	body := `
net: mainnet
beacon: {genesis_time: 1606824023}
store: {path: /x}
server: {auth_token_file: /t}
p2p: {listen: []}
follow:
  url: https://archive.example.org
  pubkey: "` + followPubkey + `"
  heads:
    unfinalized:
      kind: unfinalized-mutable
      handoff_head: all
      max_window_slots: 128
      pin: {mode: full}
`
	cfg := loadString(t, body)
	h := cfg.Follow.Heads["unfinalized"]
	if h.effectiveKind() != server.UnfinalizedMutable || h.HandoffHead != "all" || h.MaxWindowSlots != 128 {
		t.Fatalf("mutable follower config = kind %q, handoff %q, max %d", h.effectiveKind(), h.HandoffHead, h.MaxWindowSlots)
	}

	assertConfigError(t, strings.Replace(body, "pubkey: \""+followPubkey+"\"", "dnslink: swarm.example", 1),
		"mutable revision order requires one pinned signing authority")
	assertConfigError(t, strings.Replace(body, "pin: {mode: full}", "pin: {mode: window, duration: 1h}", 1),
		"must use pin.mode full")
	assertConfigError(t, strings.Replace(body, "max_window_slots: 128", "max_window_slots: 0", 1),
		"must be in [1,4096]")
	assertConfigError(t, strings.Replace(body, "handoff_head: all", "handoff_head: ''", 1),
		"handoff_head is empty")
}

func assertConfigError(t *testing.T, body, want string) {
	t.Helper()
	_, err := LoadConfig(writeFile(t, "config.yaml", body))
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("LoadConfig error = %v, want text %q", err, want)
	}
}
