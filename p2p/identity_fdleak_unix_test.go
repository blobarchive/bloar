//go:build unix

package p2p

// A parent-fsync failure after the
// install must NOT return with the target still exclusively flocked. If it did, an
// in-process retry would block on the leaked lock until the fd was finalized. The
// creator inode lock's fd is closed on every exit path, so the retry proceeds
// promptly and returns the installed key as not-created.

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/crypto"
)

func TestParentFsyncFailureReleasesLockForRetry(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "p2p.key")

	origFault := fsyncFault
	t.Cleanup(func() { fsyncFault = origFault })
	fsyncFault = func() error { return errors.New("injected directory fsync fault") }

	// The create installs the target, then the injected fault fails the parent fsync.
	if _, _, err := LoadOrCreateIdentity(path); err == nil {
		t.Fatal("expected the injected fsync fault to fail the create")
	}

	// Parse the key that IS now installed and visible on disk, so we can assert the
	// retry returns exactly it.
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("the failed create did not leave an installed target: %v", err)
	}
	installed, err := parseIdentity(raw, path)
	if err != nil {
		t.Fatalf("parsing the installed target: %v", err)
	}

	fsyncFault = nil // clear the fault

	// The retry must not block on a leaked, exclusively-flocked target fd, and must
	// return exactly the installed key as not-created.
	type result struct {
		key     crypto.PrivKey
		created bool
		err     error
	}
	done := make(chan result, 1)
	go func() {
		k, c, e := LoadOrCreateIdentity(path)
		done <- result{k, c, e}
	}()
	select {
	case r := <-done:
		if r.err != nil {
			t.Fatalf("retry after a cleared fsync fault: %v", r.err)
		}
		if r.created {
			t.Error("the retry reported creating an already-installed key")
		}
		if r.key == nil || !installed.Equals(r.key) {
			t.Error("the retry did not return exactly the installed target key")
		}
	case <-time.After(750 * time.Millisecond):
		t.Fatal("the retry blocked past 750ms; the failed create leaked the exclusively-flocked target fd")
	}
}
