package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"strings"
	"testing"

	"github.com/ipfs/go-cid"

	"github.com/blobarchive/bloar/catalog"
	"github.com/blobarchive/bloar/ingest"
	"github.com/blobarchive/bloar/schema"
	"github.com/blobarchive/bloar/store"
)

// TestRebuild covers the subcommand spec 6 requires to exist: the blob catalog
// is node-local state, and this is the walk that re-derives it from the
// blockstore.
//
// The daemon is not running here, and could not be: rebuild opens the store
// itself, and Pebble's lock makes that exclusive. That is the operational shape
// of the command, so the test has the same shape.
func TestRebuild(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{}
	cfg.Store.Path = dir

	// Ingest three blobs, then drop the catalog on the floor: what is left is a
	// blockstore with blobs in it and a KV that has never heard of them, which
	// is the state an operator runs this in.
	vhs := seedBlobs(t, dir, 3)
	clearCatalog(t, dir)

	var out bytes.Buffer
	if err := rebuild(context.Background(), cfg, false, &out); err != nil {
		t.Fatalf("rebuild: %v", err)
	}

	stats := out.String()
	if !strings.Contains(stats, "blobs:    3") {
		t.Errorf("stats do not report 3 blobs:\n%s", stats)
	}
	if !strings.Contains(stats, "upserted: 3") {
		t.Errorf("stats do not report 3 upserts:\n%s", stats)
	}

	// The catalog resolves every blob again, to the CID it had before.
	s := openStore(t, dir)
	defer s.Close()
	cat := catalog.New(s.KV())
	for vh, want := range vhs {
		got, ok, err := cat.ResolveBlob(t.Context(), vh)
		if err != nil || !ok {
			t.Fatalf("ResolveBlob(0x%x) = (_, %t, %v), want a hit", vh[:], ok, err)
		}
		if !got.Equals(want) {
			t.Errorf("ResolveBlob(0x%x) = %s, want %s", vh[:], got, want)
		}
	}
}

// TestRebuildIntactCatalog covers the answer an operator running this on a
// hunch gets: nothing was wrong, and the walk says so by upserting nothing.
func TestRebuildIntactCatalog(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{}
	cfg.Store.Path = dir
	seedBlobs(t, dir, 2)

	var out bytes.Buffer
	if err := rebuild(context.Background(), cfg, false, &out); err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	if !strings.Contains(out.String(), "upserted: 0") {
		t.Errorf("rebuilding an intact catalog upserted something:\n%s", out.String())
	}
	if !strings.Contains(out.String(), "blobs:    2") {
		t.Errorf("stats do not report 2 blobs:\n%s", out.String())
	}
}

// TestRebuildClear covers -clear: rebuild only upserts, so an entry for a blob
// the store no longer holds survives a plain walk. Clearing first is what makes
// the catalog say nothing but what the blockstore has.
func TestRebuildClear(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{}
	cfg.Store.Path = dir
	seedBlobs(t, dir, 1)

	// A catalog entry for a blob that was never stored: a dangling reference of
	// exactly the kind apply_refs has to defend against.
	s := openStore(t, dir)
	cat := catalog.New(s.KV())
	stale := schema.VersionedHash{0x01, 0xde, 0xad}
	blob, err := schema.BlobCID(makeBlob(99))
	if err != nil {
		t.Fatalf("BlobCID: %v", err)
	}
	if err := cat.Put(t.Context(), stale, blob); err != nil {
		t.Fatalf("Put: %v", err)
	}
	s.Close()

	var out bytes.Buffer
	if err := rebuild(context.Background(), cfg, true, &out); err != nil {
		t.Fatalf("rebuild -clear: %v", err)
	}

	s2 := openStore(t, dir)
	defer s2.Close()
	if _, ok, err := catalog.New(s2.KV()).ResolveBlob(t.Context(), stale); err != nil || ok {
		t.Errorf("the stale entry survived -clear: ok = %t, err = %v", ok, err)
	}
	// The real blob is back: -clear deletes, the walk restores.
	if !strings.Contains(out.String(), "upserted: 1") {
		t.Errorf("-clear did not re-derive the real blob:\n%s", out.String())
	}
}

// seedBlobs ingests n blobs into a store at dir and returns their vh -> CID
// map, closing the store afterwards so the caller can reopen it.
func seedBlobs(t *testing.T, dir string, n int) map[schema.VersionedHash]cid.Cid {
	t.Helper()
	s := openStore(t, dir)
	defer s.Close()

	i, err := ingest.New(ingest.Config{Blocks: s.Blocks(), Catalog: catalog.New(s.KV())})
	if err != nil {
		t.Fatalf("ingest.New: %v", err)
	}

	body := make([]byte, 0, n*schema.BlobSize)
	for k := range n {
		body = append(body, makeBlob(uint64(k)+1)...)
	}
	put, err := i.PutBlobs(t.Context(), body)
	if err != nil {
		t.Fatalf("PutBlobs: %v", err)
	}

	out := make(map[schema.VersionedHash]cid.Cid, len(put))
	for _, p := range put {
		out[p.VH] = p.CID
	}
	return out
}

// clearCatalog empties the blob catalog at dir.
func clearCatalog(t *testing.T, dir string) {
	t.Helper()
	s := openStore(t, dir)
	defer s.Close()
	if err := catalog.New(s.KV()).Clear(t.Context()); err != nil {
		t.Fatalf("Clear: %v", err)
	}
}

// openStore opens the store at dir with Pebble kept quiet.
func openStore(t *testing.T, dir string) *store.Store {
	t.Helper()
	s, err := store.Open(dir, store.WithPebbleLogger(quietPebble{}))
	if err != nil {
		t.Fatalf("store.Open %s: %v", dir, err)
	}
	return s
}

// quietPebble drops Pebble's internal logging.
type quietPebble struct{}

func (quietPebble) Infof(string, ...any)  {}
func (quietPebble) Errorf(string, ...any) {}
func (quietPebble) Fatalf(format string, args ...any) {
	panic(strings.TrimSpace(format))
}

// lanes is how many field elements a blob is made of.
const lanes = schema.BlobSize / 32

// makeBlob builds a valid blob: field elements well below the BLS12-381 scalar
// modulus.
func makeBlob(seed uint64) []byte {
	b := make([]byte, schema.BlobSize)
	for i := range lanes {
		binary.BigEndian.PutUint64(b[i*32+24:i*32+32], seed+uint64(i))
	}
	return b
}
