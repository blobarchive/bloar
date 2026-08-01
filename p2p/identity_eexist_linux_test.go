//go:build linux

package p2p

// follow-up regression for the safety boundary: the EEXIST race-winner branch honours the
// fsync-before-any-successful-return contract. A creator that did NOT serialize on
// our lock may install the target without fsyncing its directory entry, so the
// branch that reads back that winner must route through the same durability helper
// as the existing-target path. This forces EEXIST via the injected renameat2 and
// injects a directory-fsync fault, then pins that the fault propagates out of
// LoadOrCreateIdentity -- which it can only do if the branch called fsyncDir.

import (
	"bytes"
	"crypto/ed25519"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/crypto"
	"golang.org/x/sys/unix"
)

func TestEEXISTWinnerRoutesThroughDurabilityPath(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "p2p.key")

	validKey := []byte(hex.EncodeToString(bytes.Repeat([]byte{0x11}, ed25519.SeedSize)) + "\n")

	origRename := renameat2
	t.Cleanup(func() { renameat2 = origRename })
	renameat2 = func(_ int, _ string, _ int, newpath string, _ uint) error {
		// A racing winner that installed the target without our lock: write a valid
		// target where our install would have, then report it already present.
		if err := os.WriteFile(newpath, validKey, 0o600); err != nil {
			t.Fatalf("simulating the race winner's install: %v", err)
		}
		return unix.EEXIST
	}

	origFault := fsyncFault
	t.Cleanup(func() { fsyncFault = origFault })
	wantErr := errors.New("injected directory fsync fault")
	var fsyncCalls int
	fsyncFault = func() error {
		fsyncCalls++
		return wantErr
	}

	_, _, err := LoadOrCreateIdentity(path)
	if !errors.Is(err, wantErr) {
		t.Fatalf("the EEXIST winner branch did not surface the fsync fault, so it does not route through the "+
			"durability helper: err = %v", err)
	}
	if fsyncCalls == 0 {
		t.Fatal("fsyncDir was never called, so the EEXIST winner branch skipped the durability path")
	}
}

// TestEEXISTWinnerReadThroughLockedFd is the safety boundary follow-up: the EEXIST
// race-winner is read through the SAME locked-fd + SameFile contract as the imported
// path, not locklessly by pathname. A winner (A) is installed via the forced EEXIST,
// then replaced (with B) during the winner read; the final SameFile revalidation
// catches the replace and returns the CURRENT winner. A lockless path-based read
// would return the stale A.
func TestEEXISTWinnerReadThroughLockedFd(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "p2p.key")

	seedA := bytes.Repeat([]byte{0x11}, ed25519.SeedSize)
	seedB := bytes.Repeat([]byte{0x22}, ed25519.SeedSize)
	keyB, err := crypto.UnmarshalEd25519PrivateKey(ed25519.NewKeyFromSeed(seedB))
	if err != nil {
		t.Fatalf("building key B: %v", err)
	}

	origRename := renameat2
	t.Cleanup(func() { renameat2 = origRename })
	renameat2 = func(_ int, _ string, _ int, newpath string, _ uint) error {
		if err := os.WriteFile(newpath, []byte(hex.EncodeToString(seedA)+"\n"), 0o600); err != nil {
			t.Errorf("simulating the race winner's install: %v", err)
		}
		return unix.EEXIST // a winner (A) installed the target without our lock
	}

	var once sync.Once
	origHook := afterReadBeforeFinalStat
	t.Cleanup(func() { afterReadBeforeFinalStat = origHook })
	afterReadBeforeFinalStat = func() {
		once.Do(func() {
			// The winner is replaced (A -> B) during the winner read.
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

	key, created, err := LoadOrCreateIdentity(path)
	if err != nil {
		t.Fatalf("LoadOrCreateIdentity: %v", err)
	}
	if created {
		t.Error("adopting a race winner reported creating the key")
	}
	if !keyB.Equals(key) {
		t.Error("the EEXIST winner was read locklessly: a replace during the read was not caught and a stale key returned")
	}
}

// TestEEXISTWinnerVanishedRetriesInstall is the safety boundary follow-up's absence
// control, extended in follow-up to pin the still-owned temp: when the EEXIST
// winner is REMOVED during the adopt read, the loser does not return a raw ENOENT -- it
// re-installs its OWN still-held temp and returns its key. The test captures every
// install's oldpath through the injected renameat2 and asserts the losing install and
// the successful retry pass the SAME temp path (a fresh temp per retry would fail here).
func TestEEXISTWinnerVanishedRetriesInstall(t *testing.T) {
	dir := t.TempDir()
	base := "p2p.key"
	path := filepath.Join(dir, base)
	winnerKey := []byte(hex.EncodeToString(bytes.Repeat([]byte{0x99}, ed25519.SeedSize)) + "\n")

	origRename := renameat2
	t.Cleanup(func() { renameat2 = origRename })
	var installOnce sync.Once
	var installOldpaths []string
	renameat2 = func(olddirfd int, oldpath string, newdirfd int, newpath string, flags uint) error {
		installOldpaths = append(installOldpaths, oldpath)
		first := false
		installOnce.Do(func() { first = true })
		if first {
			// A winner installs the target; our install loses with EEXIST.
			if err := os.WriteFile(newpath, winnerKey, 0o600); err != nil {
				t.Errorf("simulating the race winner's install: %v", err)
			}
			return unix.EEXIST
		}
		// Our re-install after the winner vanished: let it happen for real.
		return origRename(olddirfd, oldpath, newdirfd, newpath, flags)
	}

	origHook := afterReadBeforeFinalStat
	t.Cleanup(func() { afterReadBeforeFinalStat = origHook })
	var removeOnce sync.Once
	afterReadBeforeFinalStat = func() {
		removeOnce.Do(func() {
			if err := os.Remove(path); err != nil { // the winner vanishes during the adopt read
				t.Errorf("removing the vanished winner: %v", err)
			}
		})
	}

	key, created, err := LoadOrCreateIdentity(path)
	if err != nil {
		t.Fatalf("a vanished winner must trigger a re-install, not a raw ENOENT: %v", err)
	}
	if !created {
		t.Error("re-installing after the winner vanished should report created=true")
	}
	// The returned key is the one now on disk (ours, freshly installed).
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("the re-install did not leave a target: %v", err)
	}
	onDisk, err := parseIdentity(raw, path)
	if err != nil {
		t.Fatalf("parsing the re-installed target: %v", err)
	}
	if key == nil || !onDisk.Equals(key) {
		t.Error("the returned key is not the re-installed target on disk")
	}

	// follow-up: the losing install and the winning retry must pass the SAME
	// temp -- the one this creator has held open and inode-locked all along -- not a
	// fresh temp minted per attempt. The first install lost (EEXIST); a later one won.
	if len(installOldpaths) < 2 {
		t.Fatalf("expected at least a losing install and a winning retry, saw %d install(s): %v",
			len(installOldpaths), installOldpaths)
	}
	tempPrefix := sidecarTempPrefix(base)
	for i, oldpath := range installOldpaths {
		if oldpath != installOldpaths[0] {
			t.Errorf("install attempt %d used temp %q, but attempt 0 used %q; a retry minted a fresh temp instead of "+
				"re-installing the one it still owns", i, filepath.Base(oldpath), filepath.Base(installOldpaths[0]))
		}
		if !strings.HasPrefix(filepath.Base(oldpath), tempPrefix) {
			t.Errorf("install attempt %d oldpath %q is not the creator's hashed sidecar temp (prefix %q)",
				i, filepath.Base(oldpath), tempPrefix)
		}
	}
}

// TestEEXISTWinnerChurnHitsBound is the safety boundary follow-up: the persistent
// appear/vanish control. A winner installs on EVERY install attempt and vanishes on
// EVERY adopt read, so the loser can never adopt it and never re-install cleanly. The
// bounded retry must give up with the documented error rather than spin forever, and it
// must leave no owned temp behind (removing the bound makes this hang -- caught by the
// timeout guard -- so the error never comes).
//
// follow-up sharpens the oracle: the churn must reach the CAP, not just retry once. The
// only renameat2 in this scenario is the install loop's, so its call count is the number
// of install attempts, and it must be EXACTLY maxTargetOpenAttempts+1 -- with the
// diagnostic's attempt count derived from the constant, not a literal. That rejects a
// mutation collapsing the bound to "attempt >= 1", which would give up after 2.
func TestEEXISTWinnerChurnHitsBound(t *testing.T) {
	dir := t.TempDir()
	base := "p2p.key"
	path := filepath.Join(dir, base)
	winnerKey := []byte(hex.EncodeToString(bytes.Repeat([]byte{0x77}, ed25519.SeedSize)) + "\n")

	origRename := renameat2
	t.Cleanup(func() { renameat2 = origRename })
	// The install loop is the only renameat2 caller here (the adopt read reopens and
	// stats, it does not rename), so this counts install attempts exactly.
	var installAttempts int
	renameat2 = func(_ int, _ string, _ int, newpath string, _ uint) error {
		installAttempts++
		// A winner installs on every attempt; our install always loses with EEXIST.
		if err := os.WriteFile(newpath, winnerKey, 0o600); err != nil {
			t.Errorf("simulating the race winner's install: %v", err)
		}
		return unix.EEXIST
	}

	origHook := afterReadBeforeFinalStat
	t.Cleanup(func() { afterReadBeforeFinalStat = origHook })
	afterReadBeforeFinalStat = func() {
		// The winner vanishes on every adopt read, so it is never adopted.
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			t.Errorf("removing the churning winner: %v", err)
		}
	}

	type result struct {
		key     crypto.PrivKey
		created bool
		err     error
	}
	done := make(chan result, 1)
	go func() {
		key, created, err := LoadOrCreateIdentity(path)
		done <- result{key, created, err}
	}()

	select {
	case r := <-done:
		if r.err == nil {
			t.Fatalf("a winner that keeps appearing and vanishing must hit the retry bound, got created=%v key=%v", r.created, r.key)
		}
		if !strings.Contains(r.err.Error(), "appearing and vanishing") {
			t.Errorf("expected the documented bounded error, got: %v", r.err)
		}
		// The churn must reach the cap: exactly one install per attempt, from attempt 0
		// through attempt == maxTargetOpenAttempts (the iteration that gives up). Both the
		// count and the diagnostic's number are derived from the constant, so a bound
		// collapsed to "attempt >= 1" -- which gives up after 2 -- fails here, not passes.
		wantAttempts := maxTargetOpenAttempts + 1
		if installAttempts != wantAttempts {
			t.Errorf("churn gave up after %d install attempts, want exactly %d (maxTargetOpenAttempts+1); "+
				"the bound is not the cap", installAttempts, wantAttempts)
		}
		wantDiag := fmt.Sprintf("giving up after %d attempts", wantAttempts)
		if !strings.Contains(r.err.Error(), wantDiag) {
			t.Errorf("expected the constant-derived diagnostic %q, got: %v", wantDiag, r.err)
		}
		if r.created {
			t.Error("a bounded-out create must not report created=true")
		}
		if r.key != nil {
			t.Error("a bounded-out create must not return a key")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("LoadOrCreateIdentity did not return: the appear/vanish loop is unbounded")
	}

	// No owned temp may remain: the bounded-out create removes its own temp on the way
	// out. (The reserved sidecar lock file persists by design -- it is not a temp.)
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("reading dir after the bounded-out create: %v", err)
	}
	tempPrefix := sidecarTempPrefix(base)
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), tempPrefix) {
			t.Errorf("a creator temp %q was left behind after the bounded-out create", e.Name())
		}
	}
}
