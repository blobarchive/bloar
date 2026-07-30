package main

// focused regression coverage for the 64-byte signing-key format: an expanded
// key whose public half does not derive from its seed is now rejected (finding
// the safety boundary), rather than accepted into a document whose signatures fail against the
// public key it advertises.

import (
	"crypto/ed25519"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestInconsistentExpandedSigningKeyIsRejected(t *testing.T) {
	// An expanded ed25519 private key is seed || public-key. Corrupting only the
	// public half leaves the configured length valid but, unfixed, made signatures
	// fail against the public key the same PrivateKey reports and bloard publishes.
	valid := ed25519.NewKeyFromSeed(make([]byte, ed25519.SeedSize))
	bad := append(ed25519.PrivateKey(nil), valid...)
	bad[len(bad)-1] ^= 0xff

	path := filepath.Join(t.TempDir(), "signing.key")
	if err := os.WriteFile(path, []byte(hex.EncodeToString(bad)), 0o600); err != nil {
		t.Fatalf("writing key: %v", err)
	}
	_, err := (&Config{Publish: PublishConfig{SigningKeyFile: path}}).SigningKey()
	if err == nil {
		t.Fatal("an inconsistent 64-byte expanded signing key was accepted; it must be rejected")
	}
	if !strings.Contains(err.Error(), "public half does not derive from its seed") {
		t.Errorf("error does not explain the inconsistency and the fix: %v", err)
	}

	// The consistent 64-byte form and the 32-byte seed form both still load.
	for name, content := range map[string][]byte{
		"consistent expanded key": []byte(hex.EncodeToString(valid)),
		"32-byte seed":            []byte(hex.EncodeToString(valid.Seed())),
	} {
		p := filepath.Join(t.TempDir(), "signing.key")
		if err := os.WriteFile(p, content, 0o600); err != nil {
			t.Fatalf("writing %s: %v", name, err)
		}
		if _, err := (&Config{Publish: PublishConfig{SigningKeyFile: p}}).SigningKey(); err != nil {
			t.Errorf("%s was rejected: %v", name, err)
		}
	}
}
