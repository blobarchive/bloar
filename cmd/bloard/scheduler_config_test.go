package main

// focused regression coverage for scheduler durations at the daemon's config
// boundary: a non-positive poll, GC, IPNS-republish, or fetch duration is now
// rejected at LoadConfig, rather than accepted and left to panic
// a ticker, spin a timer, or pre-expire a fetch context at runtime.

import (
	"strings"
	"testing"
)

func TestNegativeSchedulerDurationsRejectedByConfigValidation(t *testing.T) {
	for _, tc := range []struct {
		name string
		yaml string
		want string
	}{
		{
			name: "gc interval",
			yaml: `
net: mainnet
beacon: {genesis_time: 1}
store: {path: /x, gc_interval: -1s}
server: {auth_token_file: /t}
heads: {all: {}}
`,
			want: "store.gc_interval is -1s, must be positive",
		},
		{
			name: "scrub interval",
			yaml: `
net: mainnet
beacon: {genesis_time: 1}
store: {path: /x, scrub_interval: -1s}
server: {auth_token_file: /t}
heads: {all: {}}
`,
			want: "store.scrub_interval is -1s, must be positive",
		},
		{
			name: "follow poll interval",
			yaml: `
net: mainnet
beacon: {genesis_time: 1}
store: {path: /x}
server: {auth_token_file: /t}
p2p: {listen: ["/ip4/127.0.0.1/tcp/4001"]}
follow:
  url: https://archive.example.org
  pubkey: "0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20"
  poll_interval: -1s
  heads: {all: {pin: {mode: none}}}
`,
			want: "follow.poll_interval is -1s, must be positive",
		},
		{
			name: "follow fetch timeout",
			yaml: `
net: mainnet
beacon: {genesis_time: 1}
store: {path: /x}
server: {auth_token_file: /t}
p2p: {listen: ["/ip4/127.0.0.1/tcp/4001"]}
follow:
  url: https://archive.example.org
  pubkey: "0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20"
  fetch_timeout: -1s
  heads: {all: {pin: {mode: none}}}
`,
			want: "follow.fetch_timeout is -1s, must be positive",
		},
		{
			name: "ipns republish",
			yaml: `
net: mainnet
beacon: {genesis_time: 1}
store: {path: /x}
server: {auth_token_file: /t}
publish: {ipns: true, ipns_republish: -1s}
p2p: {listen: ["/ip4/127.0.0.1/tcp/4001"]}
heads: {all: {}}
`,
			want: "publish.ipns_republish is -1s, must be positive",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := LoadConfig(writeFile(t, "config.yaml", tc.yaml))
			if err == nil {
				t.Fatalf("negative %s was accepted; it must be rejected at config load", tc.name)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %q, want it to contain %q", err, tc.want)
			}
		})
	}
}
