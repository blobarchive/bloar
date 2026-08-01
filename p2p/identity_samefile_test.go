package p2p

// follow-up for the safety boundary: the existing-target path reads THROUGH the
// fd it locked and confirms with os.SameFile that the fd still names the file at the
// path, retrying on a concurrent replace rather than returning a key read from the
// stale inode.

import (
	"bytes"
	"crypto/ed25519"
	"encoding/hex"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/libp2p/go-libp2p/core/crypto"
)

func TestExistingTargetRetriesOnConcurrentReplace(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "p2p.key")

	seedA := bytes.Repeat([]byte{0x11}, ed25519.SeedSize)
	seedB := bytes.Repeat([]byte{0x22}, ed25519.SeedSize)
	if err := os.WriteFile(path, []byte(hex.EncodeToString(seedA)+"\n"), 0o600); err != nil {
		t.Fatalf("provisioning key A: %v", err)
	}
	keyB, err := crypto.UnmarshalEd25519PrivateKey(ed25519.NewKeyFromSeed(seedB))
	if err != nil {
		t.Fatalf("building key B: %v", err)
	}

	var once sync.Once
	afterLockBeforeStat = func() {
		once.Do(func() {
			// A concurrent replace under the lock: install B on a NEW inode via rename,
			// so the fd we locked no longer names the file at path.
			tmp := filepath.Join(dir, "replacement")
			if err := os.WriteFile(tmp, []byte(hex.EncodeToString(seedB)+"\n"), 0o600); err != nil {
				t.Errorf("writing replacement B: %v", err)
				return
			}
			if err := os.Rename(tmp, path); err != nil {
				t.Errorf("installing replacement B: %v", err)
			}
		})
	}
	t.Cleanup(func() { afterLockBeforeStat = nil })

	key, created, err := LoadOrCreateIdentity(path)
	if err != nil {
		t.Fatalf("LoadOrCreateIdentity: %v", err)
	}
	if created {
		t.Error("reading an existing target reported creating it")
	}
	if !keyB.Equals(key) {
		t.Error("did not retry after a concurrent replace; returned a key read from the stale locked inode")
	}
}

// TestExistingTargetFinalSameFileCatchesLateReplace is the safety boundary follow-up:
// the SameFile revalidation AFTER the read, parse, and dir fsync catches a rename that
// landed after the early check -- which would otherwise return the key read from the
// now-unlinked inode. The early check stays as an optimization; this one is
// authoritative.
func TestExistingTargetFinalSameFileCatchesLateReplace(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "p2p.key")

	seedA := bytes.Repeat([]byte{0x11}, ed25519.SeedSize)
	seedB := bytes.Repeat([]byte{0x22}, ed25519.SeedSize)
	if err := os.WriteFile(path, []byte(hex.EncodeToString(seedA)+"\n"), 0o600); err != nil {
		t.Fatalf("provisioning key A: %v", err)
	}
	keyB, err := crypto.UnmarshalEd25519PrivateKey(ed25519.NewKeyFromSeed(seedB))
	if err != nil {
		t.Fatalf("building key B: %v", err)
	}

	var once sync.Once
	afterReadBeforeFinalStat = func() {
		once.Do(func() {
			// A replace that lands AFTER the early check and the read: install B on a
			// new inode, so the fd we read no longer names the file at path.
			tmp := filepath.Join(dir, "replacement")
			if err := os.WriteFile(tmp, []byte(hex.EncodeToString(seedB)+"\n"), 0o600); err != nil {
				t.Errorf("writing replacement B: %v", err)
				return
			}
			if err := os.Rename(tmp, path); err != nil {
				t.Errorf("installing replacement B: %v", err)
			}
		})
	}
	t.Cleanup(func() { afterReadBeforeFinalStat = nil })

	key, created, err := LoadOrCreateIdentity(path)
	if err != nil {
		t.Fatalf("LoadOrCreateIdentity: %v", err)
	}
	if created {
		t.Error("reading an existing target reported creating it")
	}
	if !keyB.Equals(key) {
		t.Error("the final SameFile check did not catch a replace after the read; returned a key read from the stale inode")
	}
}
