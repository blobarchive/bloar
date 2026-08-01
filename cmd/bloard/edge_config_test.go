package main

import (
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"

	publicationedge "github.com/blobarchive/bloar/p2p/edge"
)

func TestPublicationEdgeRequiredModeKeepsPrivateWriterHostless(t *testing.T) {
	edgePeer := testEdgePeer(t)
	cfg := loadString(t, edgeConfig("required", edgePeer, false))
	if cfg.P2P.Host() {
		t.Fatal("required edge mode constructed an embedded public P2P host")
	}
	if cfg.Publish.Edge == nil {
		t.Fatal("publish.edge did not survive parsing")
	}
	if got, want := cfg.Publish.Edge.IdentityKeyFile, "/var/lib/bloar/p2p.key"; got != want {
		t.Fatalf("publish.edge.identity_key_file = %q, want private authority default %q", got, want)
	}
	if got := cfg.Publish.Edge.RequestTimeout; got != publicationedge.DefaultRequestTimeout {
		t.Fatalf("publish.edge.request_timeout = %s, want %s", got, publicationedge.DefaultRequestTimeout)
	}
	if got := cfg.Publish.Edge.TransactionTimeout; got != publicationedge.DefaultTransactionTimeout {
		t.Fatalf("publish.edge.transaction_timeout = %s, want %s", got, publicationedge.DefaultTransactionTimeout)
	}
	if got := cfg.Publish.Edge.MaxDocumentBytes; got != publicationedge.DefaultMaxDocumentBytes {
		t.Fatalf("publish.edge.max_document_bytes = %d, want %d", got, publicationedge.DefaultMaxDocumentBytes)
	}
}

func TestPublicationEdgeMirrorModeRetainsIncumbentAuthority(t *testing.T) {
	edgePeer := testEdgePeer(t)
	cfg := loadString(t, edgeConfig("mirror", edgePeer, true))
	if !cfg.P2P.Host() {
		t.Fatal("mirror mode dropped the incumbent embedded P2P host")
	}
	if cfg.Publish.Edge.IdentityKeyFile != cfg.P2P.IdentityKeyFile {
		t.Fatalf("mirror authority paths differ: edge=%q p2p=%q",
			cfg.Publish.Edge.IdentityKeyFile, cfg.P2P.IdentityKeyFile)
	}
}

func TestPublicationEdgeTopologyMisconfigurationsFailClosed(t *testing.T) {
	edgePeer := testEdgePeer(t)
	otherPeer := testEdgePeer(t)
	tests := []struct {
		name string
		yaml string
		want string
	}{
		{
			name: "required with embedded host",
			yaml: edgeConfig("required", edgePeer, true),
			want: "required forbids an embedded p2p host",
		},
		{
			name: "mirror without incumbent",
			yaml: edgeConfig("mirror", edgePeer, false),
			want: "mirror requires the existing embedded p2p host",
		},
		{
			name: "edge without IPNS",
			yaml: strings.Replace(edgeConfig("required", edgePeer, false), "ipns: true", "ipns: false", 1),
			want: "publish.edge requires publish.ipns",
		},
		{
			name: "multiple edge identities",
			yaml: strings.Replace(edgeConfig("required", edgePeer, false),
				fmt.Sprintf("    multiaddrs:\n      - /ip4/203.0.113.10/tcp/4005/p2p/%s", edgePeer),
				fmt.Sprintf("    multiaddrs:\n      - /ip4/203.0.113.10/tcp/4005/p2p/%s\n      - /ip4/203.0.113.11/tcp/4005/p2p/%s",
					edgePeer, otherPeer), 1),
			want: "name multiple edge peers",
		},
		{
			name: "mirror authority path mismatch",
			yaml: strings.Replace(edgeConfig("mirror", edgePeer, true),
				"    transaction_timeout: 2m", "    identity_key_file: /other/private.key\n    transaction_timeout: 2m", 1),
			want: "must equal p2p.identity_key_file",
		},
		{
			name: "transaction and client deadlines equal",
			yaml: strings.Replace(edgeConfig("required", edgePeer, false),
				"    request_timeout: 2m30s", "    request_timeout: 2m", 1),
			want: "transaction timeout 2m0s must be shorter than request timeout 2m0s",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := LoadConfig(writeFile(t, "edge.yaml", tc.yaml))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("LoadConfig error = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestRequiredEdgeStartupDoesNotMintMissingIPNSAuthority(t *testing.T) {
	edgePeer := testEdgePeer(t)
	cfg := loadString(t, edgeConfig("required", edgePeer, false))
	missing := filepath.Join(t.TempDir(), "missing-ipns-authority.key")
	cfg.Publish.Edge.IdentityKeyFile = missing
	if _, err := setupP2PWithDeps(t.Context(), cfg, nil, nil, nil, newLogger(), p2pSetupDeps{}); err == nil ||
		!strings.Contains(err.Error(), "refusing to mint a new authority") {
		t.Fatalf("setupP2PWithDeps error = %v, want missing retained-authority refusal", err)
	}
	if _, err := os.Lstat(missing); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("required edge startup created %s: %v", missing, err)
	}
}

func edgeConfig(mode string, edgePeer peer.ID, embedded bool) string {
	p2pBlock := ""
	if embedded {
		p2pBlock = `
p2p:
  listen: ["/ip4/127.0.0.1/tcp/4001"]
`
	}
	return fmt.Sprintf(`
net: mainnet
beacon: {genesis_time: 1}
store: {path: /var/lib/bloar}
server: {auth_token_file: /run/secrets/token}
publish:
  ipns: true
  edge:
    mode: %s
    control_socket: /run/bloar-edge/control.sock
    multiaddrs:
      - /ip4/203.0.113.10/tcp/4005/p2p/%s
    transaction_timeout: 2m
    request_timeout: 2m30s
heads: {all: {}}
%s`, mode, edgePeer, p2pBlock)
}

func testEdgePeer(t *testing.T) peer.ID {
	t.Helper()
	key, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	id, err := peer.IDFromPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return id
}
