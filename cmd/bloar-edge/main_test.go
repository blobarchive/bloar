package main

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ipfs/boxo/ipns"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"

	publicationedge "github.com/blobarchive/bloar/p2p/edge"
)

func TestLoadConfigPinsExplicitPublicationContractAndDockerMetricsBind(t *testing.T) {
	path := writeEdgeConfig(t, true)
	cfg, err := loadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Metrics.Listen != "0.0.0.0:9555" {
		t.Fatalf("metrics.listen = %q", cfg.Metrics.Listen)
	}
	if got := cfg.Control.TransactionTimeout; got != publicationedge.DefaultTransactionTimeout {
		t.Fatalf("control.transaction_timeout = %s, want %s", got, publicationedge.DefaultTransactionTimeout)
	}
	if required := cfg.Control.AllowedHeads["all"].Required; required == nil || !*required {
		t.Fatalf("allowed_heads.all.required = %v, want explicit true", required)
	}
}

func TestLoadConfigRejectsTransactionTimeoutWithoutWriterMargin(t *testing.T) {
	path := writeEdgeConfig(t, true)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	raw = []byte(strings.Replace(string(raw), "  state_file:", "  transaction_timeout: 2m30s\n  state_file:", 1))
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadConfig(path); err == nil ||
		!strings.Contains(err.Error(), "shorter than the writer default request timeout") {
		t.Fatalf("loadConfig error = %v, want incoherent timeout refusal", err)
	}
}

func TestLoadConfigRejectsImplicitRequiredHeadPolicy(t *testing.T) {
	path := writeEdgeConfig(t, false)
	if _, err := loadConfig(path); err == nil || !strings.Contains(err.Error(), ".required must be explicitly true or false") {
		t.Fatalf("loadConfig error = %v, want explicit required-policy refusal", err)
	}
}

func TestLoadConfigRejectsOptionalFinalizedHead(t *testing.T) {
	path := writeEdgeConfig(t, true)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	raw = []byte(strings.Replace(string(raw), "required: true", "required: false", 1))
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadConfig(path); err == nil || !strings.Contains(err.Error(), "only mutable heads may be optional") {
		t.Fatalf("loadConfig error = %v, want optional-finalized refusal", err)
	}
}

func TestListenUnixIsExclusivePrivateAndRefusesRegularFile(t *testing.T) {
	socket := filepath.Join(t.TempDir(), "edge.sock")
	listener, err := listenUnix(socket)
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Lstat(socket)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode()&os.ModeSocket == 0 || info.Mode().Perm() != 0o600 {
		t.Fatalf("control socket mode = %v, want socket 0600", info.Mode())
	}
	if _, err := listenUnix(socket); err == nil {
		t.Fatal("a second edge acquired the same control socket")
	}
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(socket); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("socket remains after Close: %v", err)
	}

	regular := filepath.Join(t.TempDir(), "not-a-socket")
	if err := os.WriteFile(regular, []byte("do not remove"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := listenUnix(regular); err == nil || !strings.Contains(err.Error(), "refusing to remove non-socket") {
		t.Fatalf("listenUnix regular-file error = %v", err)
	}
	raw, err := os.ReadFile(regular)
	if err != nil || string(raw) != "do not remove" {
		t.Fatalf("regular control path was mutated: %q, %v", raw, err)
	}
}

func TestHealthcheckRequiresReadyStatus(t *testing.T) {
	status := http.StatusNoContent
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(status)
	}))
	defer server.Close()
	if err := runHealthcheck([]string{"-url", server.URL}); err != nil {
		t.Fatalf("ready healthcheck: %v", err)
	}
	status = http.StatusServiceUnavailable
	if err := runHealthcheck([]string{"-url", server.URL}); err == nil {
		t.Fatal("healthcheck accepted a non-ready edge")
	}
}

func TestHealthcheckReadsMetricsListenerFromConfig(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	path := writeEdgeConfig(t, true)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	listen := strings.TrimPrefix(server.URL, "http://")
	raw = []byte(strings.Replace(string(raw), "0.0.0.0:9555", listen, 1))
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := runHealthcheck([]string{"-config", path}); err != nil {
		t.Fatalf("config-derived healthcheck: %v", err)
	}
}

func TestReadinessURLFollowsConfigurableMetricsListener(t *testing.T) {
	for listen, want := range map[string]string{
		"0.0.0.0:9565":   "http://127.0.0.1:9565/readyz",
		":9555":          "http://127.0.0.1:9555/readyz",
		"[::]:9555":      "http://[::1]:9555/readyz",
		"127.0.0.2:9555": "http://127.0.0.2:9555/readyz",
	} {
		got, err := readinessURL(listen)
		if err != nil || got != want {
			t.Fatalf("readinessURL(%q) = %q, %v; want %q", listen, got, err, want)
		}
	}
}

func writeEdgeConfig(t *testing.T, required bool) string {
	t.Helper()
	_, documentKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	authorityKey, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	authority, err := peer.IDFromPrivateKey(authorityKey)
	if err != nil {
		t.Fatal(err)
	}
	requiredLine := ""
	if required {
		requiredLine = "      required: true\n"
	}
	raw := fmt.Sprintf(`
net: mainnet
store:
  blocks_path: /archive/blocks
control:
  socket: /run/bloar-edge/control.sock
  state_file: /var/lib/bloar-edge/publication.json
  ipns_name: %s
  document_public_key: %s
  archive_id: %s
  allowed_heads:
    all:
      kind: finalized-monotonic
%sp2p:
  listen: ["/ip4/0.0.0.0/tcp/4005"]
  identity_key_file: /var/lib/bloar-edge/p2p.key
  rendezvous_heads: [all]
metrics:
  listen: 0.0.0.0:9555
`, ipns.NameFromPeer(authority), hex.EncodeToString(documentKey.Public().(ed25519.PublicKey)),
		strings.Repeat("11", 32), requiredLine)
	path := filepath.Join(t.TempDir(), "edge.yaml")
	if err := os.WriteFile(path, []byte(raw), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}
