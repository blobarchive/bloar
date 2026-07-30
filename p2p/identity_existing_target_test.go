package p2p

// follow-up addendum for the safety boundary: returning a valid EXISTING target must not
// be a lockless shortcut. Reintroducing the old fast path -- read the target and
// return it without a lock or the directory fsync -- leaves the rest of the suite
// green, because the barrier test load-bears only the create/cleanup locking. These
// two tests pin the existing-target path's own guarantees: it takes the target's
// own inode lock (so a reader blocks while another holder has it), and it routes
// through the common directory-fsync success path (so a fsync fault propagates out).

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/crypto"
)

func TestExistingTargetReadTakesTheLock(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "p2p.key")
	key, _, err := LoadOrCreateIdentity(path)
	if err != nil {
		t.Fatalf("creating the target: %v", err)
	}

	// Hold the TARGET's own inode lock -- the lock the existing-target path now takes
	// -- as another reader would, by opening the target and flocking its fd.
	lf, err := os.Open(path)
	if err != nil {
		t.Fatalf("opening the target: %v", err)
	}
	if err := lockExclusive(lf); err != nil {
		_ = lf.Close()
		t.Fatalf("locking: %v", err)
	}

	// Pre-lock handshake: the reader signals immediately
	// before its blocking flock, so we can assert -- with no timing sleep -- that it
	// does not return until we release the lock.
	reachedLock := make(chan struct{}, 1)
	origBefore := beforeLock
	t.Cleanup(func() { beforeLock = origBefore })
	beforeLock = func() {
		select {
		case reachedLock <- struct{}{}:
		default:
		}
	}

	done := make(chan struct{})
	var readKey crypto.PrivKey
	var readErr error
	go func() {
		k, _, e := LoadOrCreateIdentity(path)
		readKey, readErr = k, e
		close(done)
	}()

	<-reachedLock // the reader has reached the blocking flock and cannot proceed
	select {
	case <-done:
		_ = lf.Close()
		t.Fatal("a read of an existing target returned while the target inode lock was held; the existing-target " +
			"path did not take the lock")
	default:
	}

	if err := lf.Close(); err != nil { // release the lock
		t.Errorf("closing the lock file: %v", err)
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the reader did not complete after the lock was released")
	}
	if readErr != nil {
		t.Fatalf("reader: %v", readErr)
	}
	if !key.Equals(readKey) {
		t.Error("the reader returned a different key than was created")
	}
}

func TestExistingTargetReadIsMadeDurable(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "p2p.key")
	if _, _, err := LoadOrCreateIdentity(path); err != nil {
		t.Fatalf("creating the target: %v", err)
	}

	origFault := fsyncFault
	t.Cleanup(func() { fsyncFault = origFault })
	wantErr := errors.New("injected directory fsync fault")
	var calls int
	fsyncFault = func() error {
		calls++
		return wantErr
	}

	// Reading back the existing valid target must route through fsyncDir, so the
	// injected fault surfaces; a lockless read that skipped the fsync would return
	// the key with no error.
	_, _, err := LoadOrCreateIdentity(path)
	if !errors.Is(err, wantErr) {
		t.Fatalf("returning an existing target did not traverse the directory-fsync success path (the safety boundary "+
			"follow-up addendum): err = %v", err)
	}
	if calls == 0 {
		t.Fatal("fsyncDir was not called on the existing-target path")
	}
}
