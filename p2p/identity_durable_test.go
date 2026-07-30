package p2p_test

// Acceptance coverage for the safety boundary: first-use identity creation is durable
// and race-safe. Concurrent creators converge on ONE persisted identity; a
// cleanup pass cannot remove a live creator's temp or leave the directory without
// a valid identity; and an empty or truncated target fails closed rather than
// being silently used, overwritten, or deleted.

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/libp2p/go-libp2p/core/crypto"

	"github.com/blobarchive/bloar/p2p"
)

// sidecarTempPrefix mirrors the production hashed temp prefix for base, so a test
// can seed a crashed run's orphan in the same namespace createIdentity sweeps.
func sidecarTempPrefix(base string) string {
	sum := sha256.Sum256([]byte(base))
	return ".bloar-id-" + hex.EncodeToString(sum[:]) + ".tmp-"
}

// assertNoTemps asserts no identity temp survived a completed creation. It globs
// the reserved sidecar temp shape, which covers every target's temps in the dir.
func assertNoTemps(t *testing.T, dir string) {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(dir, ".bloar-id-*.tmp-*"))
	if err != nil {
		t.Fatalf("globbing temps: %v", err)
	}
	if len(matches) != 0 {
		t.Fatalf("identity temps survived a completed creation: %v", matches)
	}
}

// TestIdentityConcurrentCreatorsConverge is the concurrency convergence
// acceptance: many callers racing to create one identity all end up with the same
// persisted key, exactly one of them reports having created it, and no temp is
// left behind.
func TestIdentityConcurrentCreatorsConverge(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "p2p.key")

	const n = 24
	keys := make([]crypto.PrivKey, n)
	created := make([]bool, n)
	errs := make([]error, n)
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(n)
	for i := range n {
		go func(i int) {
			defer wg.Done()
			<-start // release them together, to maximise the race
			keys[i], created[i], errs[i] = p2p.LoadOrCreateIdentity(path)
		}(i)
	}
	close(start)
	wg.Wait()

	createdCount := 0
	for i := range n {
		if errs[i] != nil {
			t.Fatalf("caller %d failed: %v", i, errs[i])
		}
		if keys[i] == nil || !keys[i].Equals(keys[0]) {
			t.Fatalf("caller %d converged on a different identity than caller 0", i)
		}
		if created[i] {
			createdCount++
		}
	}
	if createdCount != 1 {
		t.Fatalf("%d callers reported creating the identity, want exactly 1", createdCount)
	}

	// The persisted identity is the one every caller returned, and reloading it
	// reports it as pre-existing.
	reloaded, createdAgain, err := p2p.LoadOrCreateIdentity(path)
	if err != nil {
		t.Fatalf("reloading the persisted identity: %v", err)
	}
	if createdAgain {
		t.Error("reloading reported creating an existing identity")
	}
	if !reloaded.Equals(keys[0]) {
		t.Error("the persisted identity differs from the one the callers converged on")
	}
	assertNoTemps(t, dir)
}

// TestIdentityCleanupDoesNotRemoveLiveCreatorTemp is the cleanup
// cleanup-vs-creator acceptance. A crashed run's orphan temp is seeded, then many
// creators race. The lock makes a cleanup pass (which runs only under the lock,
// after a valid target is installed) unable to coincide with a mid-write creator,
// so: every creator returns a valid identity (none had its own temp swept out from
// under it -- that would surface as an install ENOENT), the orphan is reclaimed,
// and the directory always ends with one valid identity.
func TestIdentityCleanupDoesNotRemoveLiveCreatorTemp(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "p2p.key")

	// A crashed run's abandoned temps, present before any creation begins.
	orphans := []string{
		filepath.Join(dir, sidecarTempPrefix("p2p.key")+"0000000001"),
		filepath.Join(dir, sidecarTempPrefix("p2p.key")+"0000000002"),
	}
	for _, o := range orphans {
		if err := os.WriteFile(o, []byte("garbage from a crashed run"), 0o600); err != nil {
			t.Fatalf("seeding orphan temp: %v", err)
		}
	}

	const n = 24
	keys := make([]crypto.PrivKey, n)
	errs := make([]error, n)
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(n)
	for i := range n {
		go func(i int) {
			defer wg.Done()
			<-start
			keys[i], _, errs[i] = p2p.LoadOrCreateIdentity(path)
		}(i)
	}
	close(start)
	wg.Wait()

	for i := range n {
		if errs[i] != nil {
			t.Fatalf("caller %d failed -- a live creator's temp may have been swept: %v", i, errs[i])
		}
		if keys[i] == nil || !keys[i].Equals(keys[0]) {
			t.Fatalf("caller %d converged on a different identity than caller 0", i)
		}
	}

	// A valid identity is installed, and every temp -- the seeded orphans and every
	// creator's own -- is gone.
	if _, _, err := p2p.LoadOrCreateIdentity(path); err != nil {
		t.Fatalf("the directory did not end with a valid identity: %v", err)
	}
	assertNoTemps(t, dir)
}

// TestIdentityEmptyTargetFailsClosed covers the sticky-partial-write half
// of the safety boundary: an empty or whitespace-only target is a corrupt-state error
// naming the file and the fix, and is neither used, overwritten, nor deleted.
func TestIdentityEmptyTargetFailsClosed(t *testing.T) {
	for _, tc := range []struct {
		name    string
		content string
	}{
		{"empty", ""},
		{"whitespace only", "   \n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "p2p.key")
			if err := os.WriteFile(path, []byte(tc.content), 0o600); err != nil {
				t.Fatalf("writing the target: %v", err)
			}
			_, _, err := p2p.LoadOrCreateIdentity(path)
			if err == nil {
				t.Fatal("an empty identity target was accepted; it must fail closed")
			}
			if !strings.Contains(err.Error(), "exists but is empty") {
				t.Errorf("error does not name the corrupt state and the fix: %v", err)
			}
			// Neither deleted nor overwritten: it might be a real key a botched write
			// truncated, and only the operator can decide.
			got, statErr := os.ReadFile(path)
			if statErr != nil {
				t.Fatalf("the corrupt target was removed: %v", statErr)
			}
			if string(got) != tc.content {
				t.Errorf("the corrupt target was rewritten: got %q, want %q", got, tc.content)
			}
			assertNoTemps(t, dir)
		})
	}
}
