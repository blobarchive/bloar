package p2p_test

// This focused regression covers validation of the optional 64-byte expanded
// Ed25519 identity-key form: an expanded key whose public half does not derive
// from its seed is now rejected, rather than accepted into a
// PeerID/IPNS identity whose signatures fail against the public key it reports.

import (
	"bytes"
	"crypto/ed25519"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/blobarchive/bloar/p2p"
)

func TestInconsistentExpandedIdentityKeyIsRejected(t *testing.T) {
	seed := bytes.Repeat([]byte{0x5a}, ed25519.SeedSize)
	expanded := ed25519.NewKeyFromSeed(seed)

	// An expanded key is seed || public-key. Change only the reported public
	// half, leaving a 64-byte value that is syntactically well sized but no
	// longer internally consistent.
	expanded[ed25519.SeedSize] ^= 0x80
	path := filepath.Join(t.TempDir(), "p2p.key")
	if err := os.WriteFile(path, []byte(hex.EncodeToString(expanded)), 0o600); err != nil {
		t.Fatalf("writing inconsistent identity key: %v", err)
	}

	if _, _, err := p2p.LoadOrCreateIdentity(path); err == nil {
		t.Fatal("an inconsistent 64-byte expanded identity key was accepted; it must be rejected")
	} else if !strings.Contains(err.Error(), "public half does not derive from its seed") {
		t.Errorf("error does not explain the inconsistency and the fix: %v", err)
	}

	// The consistent 64-byte form and the 32-byte seed form both still load.
	consistent := ed25519.NewKeyFromSeed(seed)
	for name, content := range map[string][]byte{
		"consistent expanded key": []byte(hex.EncodeToString(consistent)),
		"32-byte seed":            []byte(hex.EncodeToString(seed)),
	} {
		p := filepath.Join(t.TempDir(), "p2p.key")
		if err := os.WriteFile(p, content, 0o600); err != nil {
			t.Fatalf("writing %s: %v", name, err)
		}
		if _, _, err := p2p.LoadOrCreateIdentity(p); err != nil {
			t.Errorf("%s was rejected: %v", name, err)
		}
	}
}
