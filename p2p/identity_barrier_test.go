package p2p

// follow-up deterministic acceptance for the safety boundary: a cleanup pass cannot
// remove a currently-writing contender's temp. One creator is held between writing
// its temp and installing it (installBarrier) while a second creator runs; the
// directory lock keeps the second from reaching its cleanup pass (afterSweep) until
// the first finishes, so the first's live temp is never swept. With the lock
// neutralised (the no-op-lock mutation) the second creator sweeps the held temp,
// which this test detects reliably rather than only eventually.

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/crypto"
)

func TestIdentityCleanupCannotRemoveLiveCreatorTemp(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "p2p.key")

	var once sync.Once
	heldTemp := make(chan string, 1)
	release := make(chan struct{})
	swept := make(chan struct{}, 4)

	installBarrier = func(temp string) {
		first := false
		once.Do(func() { first = true })
		if first {
			// The winner: it has written its temp and is about to install. Hold it
			// here, holding the directory lock, while the loser tries to run.
			heldTemp <- temp
			<-release
		}
	}
	afterSweep = func() {
		select {
		case swept <- struct{}{}:
		default:
		}
	}
	t.Cleanup(func() { installBarrier = nil; afterSweep = nil })

	type result struct {
		key     crypto.PrivKey
		created bool
		err     error
	}
	results := make(chan result, 2)
	for range 2 {
		go func() {
			k, c, e := LoadOrCreateIdentity(path)
			results <- result{k, c, e}
		}()
	}

	held := <-heldTemp // the winner is paused mid-write, holding the lock

	// Give the loser a chance to run a cleanup pass. With the lock it blocks on the
	// lock the winner holds and never reaches the sweep -- the timeout elapses. With
	// a no-op lock it creates the target and sweeps the winner's held temp, firing
	// afterSweep promptly.
	select {
	case <-swept:
	case <-time.After(2 * time.Second):
	}

	_, statErr := os.Stat(held)
	liveTempSurvived := statErr == nil

	close(release)

	// Join both before asserting, so the hooks are still set while the goroutines run.
	got := [2]result{<-results, <-results}

	if !liveTempSurvived {
		t.Fatalf("a cleanup pass removed a live creator's temp %s while it held the lock: %v",
			held, statErr)
	}
	var first crypto.PrivKey
	created := 0
	for _, r := range got {
		if r.err != nil {
			t.Fatalf("creator failed: %v", r.err)
		}
		if r.created {
			created++
		}
		if first == nil {
			first = r.key
		} else if !first.Equals(r.key) {
			t.Fatal("the two creators diverged on the identity")
		}
	}
	if created != 1 {
		t.Fatalf("%d creators reported creating the identity, want exactly 1", created)
	}
}

// TestCreatorInodeLockBlocksReaderUntilFsync is the safety boundary edge case: the
// creator holds an inode lock on the temp -- which becomes the target at the rename
// -- through publication AND the parent fsync, so a reader that opens the just-
// installed target blocks until the entry is durable rather than returning inside
// the publish-to-fsync window. A pre-lock handshake makes the block deterministic
// , and the reader's returned key is validated.
func TestCreatorInodeLockBlocksReaderUntilFsync(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "p2p.key")

	published := make(chan struct{})
	release := make(chan struct{})
	afterPublishBeforeFsync = func() {
		close(published)
		<-release // hold here, still holding the creator inode lock, before fsync
	}
	reachedLock := make(chan struct{}, 1)
	beforeLock = func() {
		select {
		case reachedLock <- struct{}{}:
		default:
		}
	}
	t.Cleanup(func() { afterPublishBeforeFsync = nil; beforeLock = nil })

	type result struct {
		key crypto.PrivKey
		err error
	}
	createRes := make(chan result, 1)
	go func() {
		k, _, err := LoadOrCreateIdentity(path)
		createRes <- result{k, err}
	}()
	<-published // the creator installed the target and is paused before its fsync

	readRes := make(chan result, 1)
	go func() {
		k, _, err := LoadOrCreateIdentity(path)
		readRes <- result{k, err}
	}()

	<-reachedLock // the reader has reached the blocking flock on the creator's inode
	select {
	case <-readRes:
		close(release)
		t.Fatal("a reader returned inside the publish-to-fsync window; the creator inode lock did not block it " +
			"")
	default:
	}

	close(release) // the creator fsyncs and releases the inode lock
	var reader, creator result
	select {
	case reader = <-readRes:
	case <-time.After(5 * time.Second):
		t.Fatal("the reader did not complete after the creator released the inode lock")
	}
	creator = <-createRes
	if creator.err != nil {
		t.Fatalf("creator: %v", creator.err)
	}
	if reader.err != nil {
		t.Fatalf("reader: %v", reader.err)
	}
	if creator.key == nil || reader.key == nil || !creator.key.Equals(reader.key) {
		t.Error("the reader returned a different key than the creator installed")
	}
}
