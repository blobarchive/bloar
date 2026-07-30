package p2p_test

import (
	"crypto/ed25519"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/blobarchive/bloar/p2p"
)

// TestIdentityCreatedOnceAndReused is the property everything published depends
// on: the PeerID is in every multiaddr the document carries and is the IPNS
// name itself, so an identity that changed per start would invalidate both on
// every restart.
func TestIdentityCreatedOnceAndReused(t *testing.T) {
	path := filepath.Join(t.TempDir(), "p2p.key")

	first, created, err := p2p.LoadOrCreateIdentity(path)
	if err != nil {
		t.Fatalf("creating identity: %v", err)
	}
	if !created {
		t.Error("first call reported the key as pre-existing")
	}

	second, created, err := p2p.LoadOrCreateIdentity(path)
	if err != nil {
		t.Fatalf("reloading identity: %v", err)
	}
	if created {
		t.Error("second call reported that it created the key")
	}
	if !first.Equals(second) {
		t.Error("reloading the identity file produced a different key")
	}

	firstID, err := peer.IDFromPrivateKey(first)
	if err != nil {
		t.Fatalf("deriving PeerID: %v", err)
	}
	secondID, err := peer.IDFromPrivateKey(second)
	if err != nil {
		t.Fatalf("deriving PeerID: %v", err)
	}
	if firstID != secondID {
		t.Errorf("PeerID moved across a reload: %s -> %s", firstID, secondID)
	}
}

// TestIdentityFileMode: it is a private key.
func TestIdentityFileMode(t *testing.T) {
	path := filepath.Join(t.TempDir(), "p2p.key")
	if _, _, err := p2p.LoadOrCreateIdentity(path); err != nil {
		t.Fatalf("creating identity: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Errorf("identity file mode = %04o, want 0600", perm)
	}
}

// TestIdentityFileIsSigningKeyFormat is the deliberate part of the format
// choice: spec 8.1 allows the IPNS key to be the publication signing key, and
// this is what makes that offer real -- one file, both keys, no conversion.
// Both spellings publish.signing_key_file accepts are accepted here.
func TestIdentityFileIsSigningKeyFormat(t *testing.T) {
	seed := make([]byte, ed25519.SeedSize)
	for i := range seed {
		seed[i] = byte(i)
	}
	priv := ed25519.NewKeyFromSeed(seed)

	for _, tt := range []struct {
		name    string
		content string
	}{
		{"32-byte seed", hex.EncodeToString(seed)},
		{"64-byte private key", hex.EncodeToString(priv)},
		{"trailing newline", hex.EncodeToString(seed) + "\n"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "p2p.key")
			if err := os.WriteFile(path, []byte(tt.content), 0o600); err != nil {
				t.Fatalf("writing key: %v", err)
			}
			key, created, err := p2p.LoadOrCreateIdentity(path)
			if err != nil {
				t.Fatalf("loading identity: %v", err)
			}
			if created {
				t.Error("an existing key was reported as created")
			}
			raw, err := key.Raw()
			if err != nil {
				t.Fatalf("raw key: %v", err)
			}
			if string(raw) != string(priv) {
				t.Error("the loaded key is not the one in the file")
			}
		})
	}
}

// TestIdentityGeneratedFileIsReadableHex: what the first run writes is what a
// later run, and an operator's eye, can read back.
func TestIdentityGeneratedFileIsReadableHex(t *testing.T) {
	path := filepath.Join(t.TempDir(), "p2p.key")
	if _, _, err := p2p.LoadOrCreateIdentity(path); err != nil {
		t.Fatalf("creating identity: %v", err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading key: %v", err)
	}
	decoded, err := hex.DecodeString(strings.TrimSpace(string(raw)))
	if err != nil {
		t.Fatalf("the generated key file is not hex: %v", err)
	}
	if len(decoded) != ed25519.SeedSize {
		t.Errorf("the generated key is %d bytes, want a %d-byte seed", len(decoded), ed25519.SeedSize)
	}
}

func TestIdentityErrors(t *testing.T) {
	for _, tt := range []struct {
		name    string
		content string
		want    string
	}{
		{"not hex", "nonsense", "not hex"},
		{"wrong length", hex.EncodeToString([]byte{1, 2, 3}), "want an ed25519 seed"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "p2p.key")
			if err := os.WriteFile(path, []byte(tt.content), 0o600); err != nil {
				t.Fatalf("writing key: %v", err)
			}
			_, _, err := p2p.LoadOrCreateIdentity(path)
			if err == nil {
				t.Fatal("loading a bad key file succeeded")
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("error = %q, want it to mention %q", err, tt.want)
			}
		})
	}

	if _, _, err := p2p.LoadOrCreateIdentity(""); err == nil {
		t.Error("an empty identity path was accepted")
	}
}
