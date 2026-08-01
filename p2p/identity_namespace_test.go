package p2p

// follow-up regressions for the safety boundary: for DISTINCT base names the sidecar namespace
// is prefix-free (under SHA-256 collision resistance) and bounded. A raw ".<base>.tmp-"
// prefix is neither -- a different valid target whose name extends another's prefix
// shares it (delimiter collision), and ".<base>.lock" overflows a filesystem-maximum
// base name. The hashed namespace fixes both.

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestIdentityReservedSidecarNamesRefused is the safety boundary follow-up's in-code
// reservation: a configured key file may not be named the reserved sidecar shape,
// because such a name could BE another key's derived lock or BEGIN its temp sweep
// prefix and be swept out from under it. Both of the tested shapes -- a target
// equal to a derived lock name, and one beginning a temp-sweep prefix -- are
// refused, as is the bare reserved prefix.
func TestIdentityReservedSidecarNamesRefused(t *testing.T) {
	dir := t.TempDir()
	victim := "p2p.key"
	for name, base := range map[string]string{
		"equal to a derived lock name":     sidecarLockName(victim),
		"beginning a temp-sweep prefix":    sidecarTempPrefix(victim) + "mykey",
		"the bare reserved prefix":         ".bloar-id-anything",
		"a plausible-looking reserved key": ".bloar-id-deadbeef.key",
	} {
		t.Run(name, func(t *testing.T) {
			_, _, err := LoadOrCreateIdentity(filepath.Join(dir, base))
			if err == nil {
				t.Fatalf("a reserved sidecar name (%s) was accepted; it must be refused", base)
			}
			if !strings.Contains(err.Error(), "reserved") {
				t.Errorf("error does not explain the reservation: %v", err)
			}
			// Nothing was written for the refused name.
			if _, err := os.Stat(filepath.Join(dir, base)); err == nil {
				t.Errorf("a file was created for the refused reserved name %s", base)
			}
		})
	}
}

// TestIdentitySidecarNamespaceIsPrefixFree pins that for DISTINCT base names one
// target's sweep never takes another's sidecars. The prefix-free property holds under
// SHA-256 collision resistance: distinct base names hash to distinct fixed-length tokens
// (barring an astronomically unlikely collision), so neither target's namespace is a
// prefix of the other's.
func TestIdentitySidecarNamespaceIsPrefixFree(t *testing.T) {
	dir := t.TempDir()

	// Target B: a DIFFERENT valid key file whose NAME extends target A's raw temp
	// prefix (.p2p.key.tmp-). Create B fully -- so its lock and target exist -- then
	// seed one of B's live temps, as a concurrent B creation would hold. Under the
	// old raw scheme A's sweep prefix ".p2p.key.tmp-" is a prefix of B's temp and
	// lock names; under the hashed scheme B's namespace is disjoint from A's (distinct
	// base names, distinct fixed-length hashes barring an astronomically unlikely
	// SHA-256 collision).
	bBase := "p2p.key.tmp-other"
	bPath := filepath.Join(dir, bBase)
	if _, _, err := LoadOrCreateIdentity(bPath); err != nil {
		t.Fatalf("creating target B: %v", err)
	}
	bLock := filepath.Join(dir, sidecarLockName(bBase))
	bLiveTemp := filepath.Join(dir, sidecarTempPrefix(bBase)+"live0001")
	if err := os.WriteFile(bLiveTemp, []byte("B's in-flight temp"), 0o600); err != nil {
		t.Fatalf("seeding B's live temp: %v", err)
	}

	// Target A. Its create installs A's target and sweeps A's namespace.
	aPath := filepath.Join(dir, "p2p.key")
	if _, created, err := LoadOrCreateIdentity(aPath); err != nil {
		t.Fatalf("creating target A: %v", err)
	} else if !created {
		t.Fatal("target A was not created")
	}

	// None of B's sidecars, and not B's target, may have been swept by A.
	for name, p := range map[string]string{
		"B's live temp": bLiveTemp,
		"B's lock":      bLock,
		"B's target":    bPath,
	} {
		if _, err := os.Stat(p); err != nil {
			t.Errorf("A's create+sweep removed %s (%s): %v", name, filepath.Base(p), err)
		}
	}
}

func TestIdentityLongBasenameSidecarsFit(t *testing.T) {
	dir := t.TempDir()
	// A filesystem-maximum base name: ".<base>.lock" would be 261 bytes and fail
	// ENAMETOOLONG, but the hashed sidecar names are short whatever the base name.
	base := strings.Repeat("k", 255)
	path := filepath.Join(dir, base)

	key, created, err := LoadOrCreateIdentity(path) // create path
	if err != nil {
		t.Fatalf("create at a 255-byte base name: %v", err)
	}
	if !created {
		t.Error("the identity was not reported as created")
	}
	reload, createdAgain, err := LoadOrCreateIdentity(path) // existing-target path
	if err != nil {
		t.Fatalf("reload at a 255-byte base name: %v", err)
	}
	if createdAgain {
		t.Error("reloading reported creating an existing identity")
	}
	if !key.Equals(reload) {
		t.Error("the identity did not reload to the same key")
	}
}
