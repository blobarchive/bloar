//go:build linux

package p2p

// follow-up regression for the safety boundary: on Linux, installNoReplace falls back to
// the portable link+unlink when renameat2 or its RENAME_NOREPLACE flag is
// unsupported (ENOSYS on an old kernel, EINVAL on a filesystem without the flag),
// and fails closed on any other errno. The syscall is injected so the fallback is
// driven without a real old kernel.

import (
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
)

func TestInstallNoReplaceFallsBackWhenRenameat2Unsupported(t *testing.T) {
	for _, tc := range []struct {
		name  string
		errno error
	}{
		{"ENOSYS (kernel without renameat2)", unix.ENOSYS},
		{"EINVAL (filesystem without RENAME_NOREPLACE)", unix.EINVAL},
	} {
		t.Run(tc.name, func(t *testing.T) {
			orig := renameat2
			t.Cleanup(func() { renameat2 = orig })
			var calls int
			renameat2 = func(int, string, int, string, uint) error {
				calls++
				return tc.errno
			}

			dir := t.TempDir()
			path := filepath.Join(dir, "p2p.key")
			key, created, err := LoadOrCreateIdentity(path)
			if err != nil {
				t.Fatalf("LoadOrCreateIdentity should have succeeded via the link fallback: %v", err)
			}
			if !created {
				t.Error("the identity was not reported as created")
			}
			if calls == 0 {
				t.Error("the injected renameat2 was never called, so the fallback was not exercised")
			}
			// The fallback installed a durable, reusable identity: a reload reads the
			// same key (and takes the existing-target path, no install).
			reload, createdAgain, err := LoadOrCreateIdentity(path)
			if err != nil {
				t.Fatalf("reloading the fallback-installed identity: %v", err)
			}
			if createdAgain {
				t.Error("reloading reported creating an existing identity")
			}
			if !key.Equals(reload) {
				t.Error("the fallback-installed identity did not reload to the same key")
			}
		})
	}

	t.Run("a genuine errno fails closed", func(t *testing.T) {
		orig := renameat2
		t.Cleanup(func() { renameat2 = orig })
		renameat2 = func(int, string, int, string, uint) error { return unix.EACCES }

		dir := t.TempDir()
		path := filepath.Join(dir, "p2p.key")
		if _, _, err := LoadOrCreateIdentity(path); err == nil {
			t.Fatal("a genuine renameat2 errno (EACCES) must fail closed, not fall back to link+unlink")
		}
	})
}
