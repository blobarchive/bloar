package catalog_test

import (
	"context"
	"crypto/sha256"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"

	"github.com/blobarchive/bloar/archive"
	"github.com/blobarchive/bloar/catalog"
	"github.com/blobarchive/bloar/schema"
	"github.com/blobarchive/bloar/store"
)

// The catalog is the read side archive depends on, and the interface takes a
// defined type: this is the assertion that keeps the two in step.
var _ archive.BlobResolver = (*catalog.Catalog)(nil)

// openStore opens the store under dir. Closing is registered as cleanup and is
// idempotent, so a test that reopens may close early itself.
func openStore(t *testing.T, dir string) *store.Store {
	t.Helper()
	s, err := store.Open(dir)
	if err != nil {
		t.Fatalf("opening store at %s: %v", dir, err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("closing store: %v", err)
		}
	})
	return s
}

// openKV opens a store under dir and returns its KV.
func openKV(t *testing.T, dir string) *pebble.DB {
	t.Helper()
	return openStore(t, dir).KV()
}

// vhOf makes a distinct, well-formed versioned hash out of a label. No KZG is
// involved: the catalog stores 32 bytes and does not care where they came from.
func vhOf(label string) schema.VersionedHash {
	vh := schema.VersionedHash(sha256.Sum256([]byte(label)))
	vh[0] = 0x01
	return vh
}

// cidOf makes a distinct blob-shaped CID out of a label.
func cidOf(t *testing.T, label string) cid.Cid {
	t.Helper()
	sum := sha256.Sum256([]byte(label))
	mh, err := multihash.Encode(sum[:], multihash.SHA2_256)
	if err != nil {
		t.Fatalf("encoding multihash: %v", err)
	}
	return cid.NewCidV1(cid.Raw, mh)
}

func TestCatalogPutAndResolve(t *testing.T) {
	ctx := context.Background()
	c := catalog.New(openKV(t, t.TempDir()))

	vh, want := vhOf("a"), cidOf(t, "a")
	if err := c.Put(ctx, vh, want); err != nil {
		t.Fatalf("Put: %v", err)
	}

	got, ok, err := c.ResolveBlob(ctx, vh)
	if err != nil {
		t.Fatalf("ResolveBlob: %v", err)
	}
	if !ok {
		t.Fatal("ResolveBlob: hit not found after Put")
	}
	if !got.Equals(want) {
		t.Fatalf("ResolveBlob = %s, want %s", got, want)
	}
}

func TestCatalogResolveMiss(t *testing.T) {
	ctx := context.Background()
	c := catalog.New(openKV(t, t.TempDir()))

	// A vh that was never put resolves to (Undef, false, nil): absence is an
	// answer, and archive relies on it not being an error.
	got, ok, err := c.ResolveBlob(ctx, vhOf("absent"))
	if err != nil {
		t.Fatalf("ResolveBlob of an absent vh: %v", err)
	}
	if ok {
		t.Fatalf("ResolveBlob of an absent vh: ok = true, cid = %s", got)
	}
	if got.Defined() {
		t.Fatalf("ResolveBlob of an absent vh: cid = %s, want undefined", got)
	}
}

func TestCatalogPutIsIdempotent(t *testing.T) {
	ctx := context.Background()
	c := catalog.New(openKV(t, t.TempDir()))

	vh, want := vhOf("a"), cidOf(t, "a")
	for i := range 3 {
		if err := c.Put(ctx, vh, want); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
		got, ok, err := c.ResolveBlob(ctx, vh)
		if err != nil || !ok || !got.Equals(want) {
			t.Fatalf("after Put %d: ResolveBlob = (%s, %t, %v), want (%s, true, nil)", i, got, ok, err, want)
		}
	}
}

func TestCatalogPutRejectsUndefinedCID(t *testing.T) {
	ctx := context.Background()
	c := catalog.New(openKV(t, t.TempDir()))

	if err := c.Put(ctx, vhOf("a"), cid.Undef); err == nil {
		t.Fatal("Put with cid.Undef: want error, got nil")
	}
	if err := c.PutBatch(ctx, []catalog.Entry{{VH: vhOf("a"), Blob: cid.Undef}}); err == nil {
		t.Fatal("PutBatch with cid.Undef: want error, got nil")
	}
}

func TestCatalogPutBatch(t *testing.T) {
	ctx := context.Background()
	c := catalog.New(openKV(t, t.TempDir()))

	want := map[string]cid.Cid{}
	var entries []catalog.Entry
	for _, label := range []string{"a", "b", "c"} {
		entries = append(entries, catalog.Entry{VH: vhOf(label), Blob: cidOf(t, label)})
		want[label] = cidOf(t, label)
	}
	if err := c.PutBatch(ctx, entries); err != nil {
		t.Fatalf("PutBatch: %v", err)
	}
	// An empty batch is a no-op, not an error: Rebuild's final flush is
	// usually empty.
	if err := c.PutBatch(ctx, nil); err != nil {
		t.Fatalf("PutBatch(nil): %v", err)
	}

	for label, blob := range want {
		got, ok, err := c.ResolveBlob(ctx, vhOf(label))
		if err != nil || !ok || !got.Equals(blob) {
			t.Errorf("ResolveBlob(%q) = (%s, %t, %v), want (%s, true, nil)", label, got, ok, err, blob)
		}
	}
}

func TestCatalogPersistsAcrossReopen(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	vh, want := vhOf("a"), cidOf(t, "a")
	s := openStore(t, dir)
	if err := catalog.New(s.KV()).Put(ctx, vh, want); err != nil {
		t.Fatalf("Put: %v", err)
	}
	// Pebble holds an exclusive lock on the directory, so the reopen needs the
	// first store gone. Close is what makes the write durable in the eyes of a
	// clean shutdown -- but the point of the sync write is that it survives
	// without one, which only a crash test can show (archive/crash_test.go).
	if err := s.Close(); err != nil {
		t.Fatalf("closing store: %v", err)
	}

	got, ok, err := catalog.New(openKV(t, dir)).ResolveBlob(ctx, vh)
	if err != nil || !ok || !got.Equals(want) {
		t.Fatalf("after reopen: ResolveBlob = (%s, %t, %v), want (%s, true, nil)", got, ok, err, want)
	}
}

func TestCatalogClearLeavesLedgerAlone(t *testing.T) {
	ctx := context.Background()
	kv := openKV(t, t.TempDir())
	c, l := catalog.New(kv), catalog.NewLedger(kv)

	if err := c.Put(ctx, vhOf("a"), cidOf(t, "a")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := l.Add(ctx, "all", "root", cidOf(t, "root"), true); err != nil {
		t.Fatalf("Add: %v", err)
	}

	if err := c.Clear(ctx); err != nil {
		t.Fatalf("Clear: %v", err)
	}

	if _, ok, err := c.ResolveBlob(ctx, vhOf("a")); err != nil || ok {
		t.Fatalf("after Clear: ResolveBlob ok = %t, err = %v, want false, nil", ok, err)
	}
	pins, err := l.ListAll(ctx, "all")
	if err != nil {
		t.Fatalf("ListAll: %v", err)
	}
	if len(pins) != 1 {
		t.Fatalf("after Clear: ledger has %d pins, want 1; Clear must not touch the ledger", len(pins))
	}
}
