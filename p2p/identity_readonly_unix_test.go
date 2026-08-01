//go:build unix

package p2p

// follow-up regressions for the safety boundary: a key under a read-only
// parent -- /etc/bloar with ProtectSystem=strict, only /var/lib/bloar writable --
// must still load, because the existing-target path locks the target's own inode
// and writes no sidecar. Creating a missing key there is impossible and must say so.

import (
	"bytes"
	"crypto/ed25519"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/libp2p/go-libp2p/core/crypto"
)

// makeReadOnlyDir chmods dir to r-xr-xr-x and restores it before the test's TempDir
// cleanup runs. It skips as root, where directory permissions do not restrict writes
// and the read-only premise cannot hold.
func makeReadOnlyDir(t *testing.T, dir string) {
	t.Helper()
	if os.Geteuid() == 0 {
		t.Skip("running as root: directory permissions do not restrict writes, so a read-only parent cannot be simulated")
	}
	if err := os.Chmod(dir, 0o555); err != nil {
		t.Fatalf("making %s read-only: %v", dir, err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o755) }) // LIFO: runs before TempDir removal
}

func TestExistingTargetReadOnlyParentCreatesNoSidecar(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "p2p.key")

	// Provision a valid key directly, as an operator would under a read-only /etc --
	// so there is NO sidecar, and the existing-target path must read it without
	// trying to write one.
	seed := bytes.Repeat([]byte{0x42}, ed25519.SeedSize)
	if err := os.WriteFile(path, []byte(hex.EncodeToString(seed)+"\n"), 0o600); err != nil {
		t.Fatalf("provisioning the key: %v", err)
	}
	want, err := crypto.UnmarshalEd25519PrivateKey(ed25519.NewKeyFromSeed(seed))
	if err != nil {
		t.Fatalf("building the expected key: %v", err)
	}

	makeReadOnlyDir(t, dir)

	key, created, err := LoadOrCreateIdentity(path)
	if err != nil {
		t.Fatalf("reading a provisioned key under a read-only parent: %v", err)
	}
	if created {
		t.Error("reading an existing target reported creating it")
	}
	if !want.Equals(key) {
		t.Error("the read returned a different key than was provisioned")
	}
	if matches, _ := filepath.Glob(filepath.Join(dir, ".bloar-id-*")); len(matches) != 0 {
		t.Errorf("the existing-target read created a sidecar under a read-only parent: %v", matches)
	}
}

func TestMissingKeyReadOnlyParentClearError(t *testing.T) {
	dir := t.TempDir()
	makeReadOnlyDir(t, dir)

	_, _, err := LoadOrCreateIdentity(filepath.Join(dir, "p2p.key"))
	if err == nil {
		t.Fatal("a missing identity key in a read-only directory was created; it cannot be")
	}
	if !strings.Contains(err.Error(), "not writable") {
		t.Errorf("error does not explain that the directory is not writable: %v", err)
	}
}
