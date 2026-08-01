package main

// Every server.* bound has its default, override, and invalid-rejection pinned
// so none can silently regress to an unbounded value.

import (
	"strings"
	"testing"
	"time"
)

// serverBoundsBase is a minimal valid daemon config; the field tests add server.*
// keys on top of it.
const serverBoundsBase = `
net: mainnet
beacon: {genesis_time: 1606824023}
store: {path: /var/lib/bloar}
server:
  auth_token_file: /etc/bloar/token
heads: {all: {}}
`

func TestServerConnectionBoundDefaults(t *testing.T) {
	cfg := loadString(t, serverBoundsBase)
	for _, c := range []struct {
		name string
		got  time.Duration
		want time.Duration
	}{
		{"read_header_timeout", cfg.Server.ReadHeaderTimeout, 10 * time.Second},
		{"read_timeout", cfg.Server.ReadTimeout, 15 * time.Second},
		{"write_timeout", cfg.Server.WriteTimeout, 120 * time.Second},
		{"idle_timeout", cfg.Server.IdleTimeout, 60 * time.Second},
		{"mutation_body_timeout", cfg.Server.MutationBodyTimeout, 60 * time.Second},
	} {
		if c.got != c.want {
			t.Errorf("server.%s default = %s, want %s", c.name, c.got, c.want)
		}
	}
	if cfg.Server.MaxHeaderBytes != 64<<10 {
		t.Errorf("server.max_header_bytes default = %d, want %d", cfg.Server.MaxHeaderBytes, 64<<10)
	}
	if cfg.Server.MaxConns != 1024 {
		t.Errorf("server.max_conns default = %d, want 1024", cfg.Server.MaxConns)
	}
}

func TestServerConnectionBoundOverrides(t *testing.T) {
	cfg := loadString(t, `
net: mainnet
beacon: {genesis_time: 1606824023}
store: {path: /var/lib/bloar}
server:
  auth_token_file: /etc/bloar/token
  read_header_timeout: 3s
  read_timeout: 7s
  write_timeout: 90s
  idle_timeout: 30s
  mutation_body_timeout: 45s
  max_header_bytes: 8192
  max_conns: 42
heads: {all: {}}
`)
	if cfg.Server.ReadHeaderTimeout != 3*time.Second || cfg.Server.ReadTimeout != 7*time.Second ||
		cfg.Server.WriteTimeout != 90*time.Second || cfg.Server.IdleTimeout != 30*time.Second ||
		cfg.Server.MutationBodyTimeout != 45*time.Second {
		t.Errorf("server timeouts not taken as written: %+v", cfg.Server)
	}
	if cfg.Server.MaxHeaderBytes != 8192 {
		t.Errorf("server.max_header_bytes override = %d, want 8192", cfg.Server.MaxHeaderBytes)
	}
	if cfg.Server.MaxConns != 42 {
		t.Errorf("server.max_conns override = %d, want 42", cfg.Server.MaxConns)
	}
}

func TestServerConnectionBoundRejects(t *testing.T) {
	tests := []struct {
		name string
		keys string
		want string
	}{
		{"negative read_timeout", "read_timeout: -1s", "read_timeout"},
		{"negative read_header_timeout", "read_header_timeout: -1s", "read_header_timeout"},
		{"negative write_timeout", "write_timeout: -1s", "write_timeout"},
		{"negative idle_timeout", "idle_timeout: -1s", "idle_timeout"},
		{"negative mutation_body_timeout", "mutation_body_timeout: -1s", "mutation_body_timeout"},
		{"mutation_body not over read", "read_timeout: 10s\n  mutation_body_timeout: 10s", "must exceed"},
		{"negative max_header_bytes", "max_header_bytes: -1", "max_header_bytes"},
		{"negative max_conns", "max_conns: -1", "max_conns"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			yaml := "net: mainnet\nbeacon: {genesis_time: 1606824023}\nstore: {path: /x}\nserver:\n  auth_token_file: /t\n  " +
				tc.keys + "\nheads: {all: {}}\n"
			_, err := LoadConfig(writeFile(t, "config.yaml", yaml))
			if err == nil {
				t.Fatalf("config with %s loaded, want rejection", tc.name)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error = %q, want it to mention %q", err, tc.want)
			}
		})
	}
}
