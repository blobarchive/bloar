//go:build unix

package p2p

// The under-sidecar-lock re-check reads
// a racer's target THROUGH the locked-fd contract, so a target another process is
// writing (its flock held, partial bytes visible) blocks the re-check until the writer
// releases -- it never reads the partial bytes. A lockless pathname parse would read
// the partial bytes immediately.

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

func TestRecheckBlocksOnMidWriteTarget(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "p2p.key")

	seed := bytes.Repeat([]byte{0x5c}, ed25519.SeedSize)
	fullBytes := []byte(hex.EncodeToString(seed) + "\n")
	keyFull, err := crypto.UnmarshalEd25519PrivateKey(ed25519.NewKeyFromSeed(seed))
	if err != nil {
		t.Fatalf("building the completed key: %v", err)
	}

	releaseWriter := make(chan struct{})
	writerDone := make(chan struct{})
	var once sync.Once
	beforeRecheck = func() {
		once.Do(func() {
			// A racer installs the target but is mid-write: partial bytes, flock held.
			f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, identityFileMode)
			if err != nil {
				t.Errorf("opening the mid-write target: %v", err)
				close(writerDone)
				return
			}
			if _, err := f.Write([]byte("00")); err != nil { // partial: parses to nothing valid
				t.Errorf("writing partial bytes: %v", err)
			}
			if err := lockExclusive(f); err != nil {
				t.Errorf("locking the mid-write target: %v", err)
			}
			go func() {
				defer close(writerDone)
				<-releaseWriter
				_ = f.Truncate(0)
				_, _ = f.Seek(0, 0)
				_, _ = f.Write(fullBytes)
				_ = f.Sync()
				_ = f.Close() // releases the flock
			}()
		})
	}
	reachedLock := make(chan struct{}, 1)
	beforeLock = func() {
		select {
		case reachedLock <- struct{}{}:
		default:
		}
	}
	t.Cleanup(func() { beforeRecheck = nil; beforeLock = nil })

	type result struct {
		key crypto.PrivKey
		err error
	}
	resCh := make(chan result, 1)
	go func() {
		k, _, e := LoadOrCreateIdentity(path)
		resCh <- result{k, e}
	}()

	// The re-check either reaches the blocking flock (fix) or completes without ever
	// flocking (a lockless read: the mutation). These are mutually exclusive -- the
	// locked-fd re-check cannot complete without acquiring the flock the writer holds,
	// and a lockless read never signals reachedLock -- so this select is deterministic
	// with no timing sleep.
	select {
	case <-reachedLock:
		// The re-check flocked the racer's target and is blocked; it cannot have
		// returned.
		select {
		case <-resCh:
			close(releaseWriter)
			<-writerDone
			t.Fatal("the re-check returned while the writer held the target flock mid-write")
		default:
		}
		close(releaseWriter) // the racer completes its write and releases the flock
		<-writerDone
		r := <-resCh
		if r.err != nil {
			t.Fatalf("re-check after the writer completed: %v", r.err)
		}
		if r.key == nil || !keyFull.Equals(r.key) {
			t.Error("the re-check did not return the completed bytes; it read a partial or stale target")
		}
	case r := <-resCh:
		// The re-check completed without ever flocking: it read the target locklessly.
		close(releaseWriter)
		<-writerDone
		if r.err != nil {
			t.Fatalf("the re-check read the target locklessly instead of blocking on the mid-write flock "+
				": %v", r.err)
		}
		t.Fatal("the re-check completed without flocking the mid-write target; it read locklessly")
	}
}
