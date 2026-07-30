//go:build linux

package p2p

// follow-up for the safety boundary: the parent-directory fsync is attempted on
// every path, but its exception is narrow and path-dependent. The imported-target
// read tolerates ONLY a read-only-mount error (EROFS); EIO and an unsupported result
// are fatal there. The creation and race-winner paths, which have their own pending
// entry to make durable, tolerate NOTHING. The syscalls are injected so each errno
// is exercised without a real read-only mount.

import (
	"bytes"
	"crypto/ed25519"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
)

func lcSetFsyncFault(t *testing.T, errno error) {
	t.Helper()
	orig := fsyncFault
	t.Cleanup(func() { fsyncFault = orig })
	fsyncFault = func() error { return errno }
}

func provisionKey(t *testing.T, path string) {
	t.Helper()
	seed := bytes.Repeat([]byte{0x33}, ed25519.SeedSize)
	if err := os.WriteFile(path, []byte(hex.EncodeToString(seed)+"\n"), 0o600); err != nil {
		t.Fatalf("provisioning key: %v", err)
	}
}

func TestDirFsyncErrnoPolicy(t *testing.T) {
	t.Run("imported path tolerates EROFS", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "p2p.key")
		provisionKey(t, path)
		lcSetFsyncFault(t, unix.EROFS)
		if _, _, err := LoadOrCreateIdentity(path); err != nil {
			t.Fatalf("EROFS on the imported-target path must be tolerated: %v", err)
		}
	})

	for _, tc := range []struct {
		name  string
		errno error
	}{
		{"EIO", unix.EIO},
		{"unsupported EINVAL", unix.EINVAL},
		{"EACCES", unix.EACCES},
	} {
		t.Run("imported path fatal on "+tc.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "p2p.key")
			provisionKey(t, path)
			lcSetFsyncFault(t, tc.errno)
			if _, _, err := LoadOrCreateIdentity(path); err == nil {
				t.Fatalf("%s on the imported-target path must be fatal, not tolerated", tc.name)
			}
		})
	}

	for _, tc := range []struct {
		name  string
		errno error
	}{
		{"EROFS", unix.EROFS},
		{"EIO", unix.EIO},
		{"unsupported EINVAL", unix.EINVAL},
	} {
		t.Run("creation path fatal on "+tc.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "p2p.key") // missing: creation path
			lcSetFsyncFault(t, tc.errno)
			if _, _, err := LoadOrCreateIdentity(path); err == nil {
				t.Fatalf("%s on the creation path must be fatal (it has a pending entry to make durable)", tc.name)
			}
		})

		t.Run("EEXIST-winner path fatal on "+tc.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "p2p.key")
			validKey := []byte(hex.EncodeToString(bytes.Repeat([]byte{0x44}, ed25519.SeedSize)) + "\n")

			origRename := renameat2
			t.Cleanup(func() { renameat2 = origRename })
			renameat2 = func(_ int, _ string, _ int, newpath string, _ uint) error {
				_ = os.WriteFile(newpath, validKey, 0o600) // a race winner installs it
				return unix.EEXIST
			}
			lcSetFsyncFault(t, tc.errno)
			if _, _, err := LoadOrCreateIdentity(path); err == nil {
				t.Fatalf("%s on the EEXIST race-winner path must be fatal, not tolerated", tc.name)
			}
		})
	}
}
