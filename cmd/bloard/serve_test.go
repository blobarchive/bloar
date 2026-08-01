package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestServeWithP2P brings the whole daemon up with a libp2p host and the IPNS
// channel on, and takes it down again.
//
// It is here because nothing else covers the wiring: the p2p stack is built,
// handed to the head registry and closed by serve, and every one of those is a
// line of ordering rather than a function with a return value. The specific
// things it would catch are the ones that cost an afternoon each -- a daemon
// that will not exit because a close waits on something already stopped, a host
// built after the document that was supposed to carry its addresses, an
// identity file that is not where the config says.
func TestServeWithP2P(t *testing.T) {
	dir := t.TempDir()
	cfg := serveTestConfig(t, dir, true)

	stop := startServe(t, cfg)

	// The document carries the addresses the host actually bound, which is the
	// whole point of building the host before the registry.
	doc := getDoc(t, cfg.Server.Listen)
	if !strings.Contains(doc, "/ip4/127.0.0.1/tcp/") || !strings.Contains(doc, "/p2p/12D3Koo") {
		t.Errorf("published document has no announce address for the running host: %s", doc)
	}

	// The identity landed where the config said, and is private.
	info, err := os.Stat(cfg.P2P.IdentityKeyFile)
	if err != nil {
		t.Fatalf("stat identity key: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Errorf("identity key mode = %04o, want 0600", perm)
	}

	stop(t)
}

// TestServeIdentityIsStableAcrossRestarts: the PeerID is in every multiaddr the
// document publishes and is the IPNS name itself, so a restart that changed it
// would invalidate everything this node has ever told a follower.
func TestServeIdentityIsStableAcrossRestarts(t *testing.T) {
	dir := t.TempDir()

	cfg := serveTestConfig(t, dir, true)
	stop := startServe(t, cfg)
	first := peerIDFromDoc(t, getDoc(t, cfg.Server.Listen))
	stop(t)

	// Same store, same key file, new process's worth of state.
	cfg2 := serveTestConfig(t, dir, true)
	stop2 := startServe(t, cfg2)
	second := peerIDFromDoc(t, getDoc(t, cfg2.Server.Listen))
	stop2(t)

	if first == "" {
		t.Fatal("no PeerID in the first document")
	}
	if first != second {
		t.Errorf("PeerID moved across a restart: %s -> %s", first, second)
	}
}

// TestServeWithoutP2P is the compatibility claim: a config with no p2p block
// runs the daemon it ran before this package existed. No host, no bitswap, no
// publisher, no identity file, and a document with no multiaddrs in it.
func TestServeWithoutP2P(t *testing.T) {
	dir := t.TempDir()
	cfg := serveTestConfig(t, dir, false)

	stop := startServe(t, cfg)
	doc := getDoc(t, cfg.Server.Listen)
	if strings.Contains(doc, "multiaddrs") {
		t.Errorf("a daemon with no p2p block published multiaddrs: %s", doc)
	}
	stop(t)

	if _, err := os.Stat(filepath.Join(dir, "p2p.key")); !os.IsNotExist(err) {
		t.Errorf("a daemon with no p2p block created an identity key (err=%v)", err)
	}
}

// TestServeWiresPublicReadAdmission exercises the whole daemon seam: the YAML
// adapter reaches server.Config, a public read is rejected with the retry
// contract, and the decision reaches the private metrics registry. A unit test
// of either endpoint alone would not catch serve.go dropping the config or
// observer assignment.
func TestServeWiresPublicReadAdmission(t *testing.T) {
	dir := t.TempDir()
	cfg := serveTestConfig(t, dir, false)
	cfg.Server.MetricsListen = freeAddr(t)
	cfg.Server.MaxQueryHashes = 1
	cfg.Server.PublicReadAdmission.GlobalRate = 0.0001
	cfg.Server.PublicReadAdmission.GlobalBurst = 2
	cfg.Server.PublicReadAdmission.ClientRate = 0.0001
	cfg.Server.PublicReadAdmission.ClientBurst = 2

	stop := startServe(t, cfg)
	defer stop(t)

	// waitForHTTP consumed one of the two initial metadata tokens. At most one
	// more request is admitted; a later one must be the non-cacheable 429.
	var rejected *http.Response
	for range 3 {
		resp, err := http.Get("http://" + cfg.Server.Listen + "/bloar/v1/heads")
		if err != nil {
			t.Fatalf("GET public heads: %v", err)
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			rejected = resp
			break
		}
		resp.Body.Close()
	}
	if rejected == nil {
		t.Fatal("public reads exhausted the configured two-token burst without a 429")
	}
	defer rejected.Body.Close()
	if rejected.Header.Get("Retry-After") == "" || rejected.Header.Get("Cache-Control") != "no-store" {
		t.Errorf("429 headers = Retry-After %q, Cache-Control %q", rejected.Header.Get("Retry-After"),
			rejected.Header.Get("Cache-Control"))
	}

	resp, err := http.Get("http://" + cfg.Server.MetricsListen + "/metrics")
	if err != nil {
		t.Fatalf("GET private metrics: %v", err)
	}
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatalf("read private metrics: %v", err)
	}
	if !strings.Contains(string(body), `bloar_public_read_admissions_total{outcome="rejected_global"} 1`) {
		t.Fatalf("rejected admission did not reach fixed-label metrics:\n%s", body)
	}
}

// serveTestConfig builds a config over dir, with the p2p host and IPNS either
// on or off.
func serveTestConfig(t *testing.T, dir string, withP2P bool) *Config {
	t.Helper()
	token := filepath.Join(dir, "token")
	if err := os.WriteFile(token, []byte("test-token"), 0o600); err != nil {
		t.Fatalf("writing token: %v", err)
	}

	yaml := fmt.Sprintf(`
net: mainnet
beacon: {genesis_time: 1606824023, seconds_per_slot: 12}
store: {path: %s}
server: {listen: "%s", auth_token_file: %s}
heads:
  all: {origin_slot: 8626176, seg_bits: 9, fanout_bits: 8, pin: {mode: full}}
`, filepath.Join(dir, "store"), freeAddr(t), token)
	if withP2P {
		yaml += `
publish: {ipns: true}
p2p: {listen: ["/ip4/127.0.0.1/tcp/0"], nat_port_map: false, dht: {bootstrap: private}}
`
	}
	return loadString(t, yaml)
}

// startServe runs the daemon and returns the function that stops it and asserts
// it stopped. A daemon that does not come back from a cancelled context is a
// daemon an operator has to SIGKILL, so the deadline is the assertion.
func startServe(t *testing.T, cfg *Config) func(*testing.T) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- serve(ctx, cfg) }()

	waitForHTTP(t, cfg.Server.Listen)

	var once bool
	return func(t *testing.T) {
		t.Helper()
		if once {
			return
		}
		once = true
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("serve returned %v", err)
			}
		case <-time.After(60 * time.Second):
			t.Fatal("serve did not return within 60s of its context being cancelled")
		}
	}
}

// freeAddr returns a loopback address that was free a moment ago. Racy in
// principle; the alternative is teaching serve to report the port it bound,
// which is API for a test's benefit.
func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("finding a free port: %v", err)
	}
	addr := l.Addr().String()
	if err := l.Close(); err != nil {
		t.Fatalf("closing the probe listener: %v", err)
	}
	return addr
}

func waitForHTTP(t *testing.T, addr string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get("http://" + addr + "/bloar/v1/heads")
		if err == nil {
			resp.Body.Close()
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("the daemon never served on %s", addr)
}

func getDoc(t *testing.T, addr string) string {
	t.Helper()
	resp, err := http.Get("http://" + addr + "/bloar/v1/heads")
	if err != nil {
		t.Fatalf("getting the publication document: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reading the publication document: %v", err)
	}
	return string(body)
}

// peerIDFromDoc pulls the PeerID out of the first multiaddr in the document.
func peerIDFromDoc(t *testing.T, doc string) string {
	t.Helper()
	_, rest, ok := strings.Cut(doc, "/p2p/")
	if !ok {
		return ""
	}
	id, _, ok := strings.Cut(rest, `"`)
	if !ok {
		t.Fatalf("unterminated multiaddr in %s", doc)
	}
	return id
}
