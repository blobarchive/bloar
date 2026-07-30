package p2p_test

// follow-up regression for the safety boundary: the stale-temp sweep matches temps by a
// LITERAL prefix, not a glob. A target basename comes straight from the operator's
// configured path, so one containing a glob metacharacter (p2p*.key) must not sweep
// a DIFFERENT target's live temp in the same directory (p2p-other.key's), which is
// held under a different lock.

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/blobarchive/bloar/p2p"
)

func TestIdentitySweepDoesNotCrossTargetsWithGlobMeta(t *testing.T) {
	dir := t.TempDir()

	// A live temp belonging to a DIFFERENT target (p2p-other.key), as a concurrent
	// creation of that target would hold under its own lock. A glob of p2p*.key's
	// temp pattern (`.p2p*.key.tmp-*`) would match and delete this; a literal prefix
	// (`.p2p-other.key.tmp-` != `.p2p*.key.tmp-`) does not.
	victim := filepath.Join(dir, ".p2p-other.key.tmp-9999999999")
	if err := os.WriteFile(victim, []byte("another target's in-flight temp"), 0o600); err != nil {
		t.Fatalf("seeding the other target's temp: %v", err)
	}

	metaPath := filepath.Join(dir, "p2p*.key")
	if _, created, err := p2p.LoadOrCreateIdentity(metaPath); err != nil {
		t.Fatalf("creating the metacharacter-named identity: %v", err)
	} else if !created {
		t.Fatal("the metacharacter-named identity was not created")
	}

	if _, err := os.Stat(victim); err != nil {
		t.Fatalf("the sweep of %q removed a different target's live temp: %v",
			filepath.Base(metaPath), err)
	}
}
