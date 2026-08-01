package ingest_test

import (
	"context"
	"testing"

	"github.com/ipfs/boxo/blockstore"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"

	"github.com/blobarchive/bloar/ingest"
	"github.com/blobarchive/bloar/schema"
)

// putBlock stores data under c.
func putBlock(t *testing.T, bs blockstore.Blockstore, data []byte, c cid.Cid) {
	t.Helper()
	blk, err := blocks.NewBlockWithCid(data, c)
	if err != nil {
		t.Fatalf("framing block %s: %v", c, err)
	}
	if err := bs.Put(context.Background(), blk); err != nil {
		t.Fatalf("storing block %s: %v", c, err)
	}
}

// putRaw stores data under its own blob-shaped CID. It is how the rebuild tests
// plant blocks the walk must skip: schema.BlobCID only asserts the length, so
// it happily addresses BlobSize bytes that are not a blob.
func putRaw(t *testing.T, bs blockstore.Blockstore, data []byte) {
	t.Helper()
	c, err := schema.BlobCID(data)
	if err != nil {
		t.Fatalf("schema.BlobCID: %v", err)
	}
	putBlock(t, bs, data, c)
}

// TestRebuildFromMixedBlockstore walks a store holding blobs, an index node,
// and bytes that are blob-sized but not a blob. Only the blobs may land in the
// catalog: the KZG commitment is the only thing that can tell them apart.
func TestRebuildFromMixedBlockstore(t *testing.T) {
	ctx := context.Background()
	i, s, cat := newIngester(t)
	cfg := ingest.Config{Blocks: s.Blocks(), Catalog: cat}

	// Two real blobs, ingested the normal way.
	put, err := i.PutBlobs(ctx, bodyOf(makeBlob(11), makeBlob(1<<30)))
	if err != nil {
		t.Fatalf("PutBlobs: %v", err)
	}

	// An index node: a real, encoded Segment referencing those blobs. It is
	// dag-cbor and nowhere near BlobSize, so the walk skips it on length.
	seg := &schema.Segment{Slot0: 8, Rows: []schema.Row{{
		Slot:    9,
		Entries: []schema.RefEntry{{VH: put[0].VH, Blob: put[0].CID}, {VH: put[1].VH, Blob: put[1].CID}},
	}}}
	segBytes, segCID, err := schema.EncodeSegment(seg)
	if err != nil {
		t.Fatalf("EncodeSegment: %v", err)
	}
	putBlock(t, s.Blocks(), segBytes, segCID)

	// The interesting skip: BlobSize bytes that are not canonical field
	// elements. Length alone would catalog this; only the commitment refuses.
	putRaw(t, s.Blocks(), makeInvalidBlob())

	// Snapshot the catalog as ingest wrote it, then drop it entirely.
	want := map[schema.VersionedHash]cid.Cid{}
	for _, r := range put {
		want[r.VH] = r.CID
	}
	if err := cat.Clear(ctx); err != nil {
		t.Fatalf("Clear: %v", err)
	}
	for _, r := range put {
		if _, ok, err := cat.ResolveBlob(ctx, r.VH); err != nil || ok {
			t.Fatalf("after Clear: vh 0x%x still resolves (ok=%t, err=%v)", r.VH, ok, err)
		}
	}

	stats, err := ingest.Rebuild(ctx, cfg)
	if err != nil {
		t.Fatalf("Rebuild: %v", err)
	}

	// 4 blocks: 2 blobs, 1 segment, 1 blob-sized non-blob.
	if stats.Scanned != 4 {
		t.Errorf("Scanned = %d, want 4", stats.Scanned)
	}
	if stats.Blobs != 2 {
		t.Errorf("Blobs = %d, want 2", stats.Blobs)
	}
	if stats.Upserted != 2 {
		t.Errorf("Upserted = %d, want 2", stats.Upserted)
	}
	if stats.Skipped != 2 {
		t.Errorf("Skipped = %d, want 2 (the segment and the non-blob)", stats.Skipped)
	}
	if stats.Scanned != stats.Blobs+stats.Skipped {
		t.Errorf("Scanned %d != Blobs %d + Skipped %d", stats.Scanned, stats.Blobs, stats.Skipped)
	}

	// The rebuilt catalog matches what ingest wrote, entry for entry. Together
	// with Upserted == 2 that is the whole claim: exactly two rows were
	// written, and they are these. Nothing the walk skipped could be
	// catalogued anyway -- a block with no commitment has no vh to file it
	// under.
	for vh, blob := range want {
		got, ok, err := cat.ResolveBlob(ctx, vh)
		if err != nil || !ok {
			t.Fatalf("rebuilt catalog misses vh 0x%x (ok=%t, err=%v)", vh, ok, err)
		}
		if !got.Equals(blob) {
			t.Errorf("rebuilt catalog resolves vh 0x%x to %s, want %s", vh, got, blob)
		}
	}
}

// TestRebuildOfAnIntactCatalogWritesNothing: Rebuild reads before it writes, so
// a catalog that already has the right answer costs no fsync and reports it.
func TestRebuildOfAnIntactCatalogWritesNothing(t *testing.T) {
	ctx := context.Background()
	i, s, cat := newIngester(t)
	cfg := ingest.Config{Blocks: s.Blocks(), Catalog: cat}

	if _, err := i.PutBlobs(ctx, bodyOf(makeBlob(3))); err != nil {
		t.Fatalf("PutBlobs: %v", err)
	}

	stats, err := ingest.Rebuild(ctx, cfg)
	if err != nil {
		t.Fatalf("Rebuild: %v", err)
	}
	if stats.Blobs != 1 {
		t.Errorf("Blobs = %d, want 1", stats.Blobs)
	}
	if stats.Upserted != 0 {
		t.Errorf("Upserted = %d, want 0: the catalog was already correct", stats.Upserted)
	}
}

// TestRebuildRepairsAWrongEntry: an upsert-only walk still fixes a row that
// names the wrong block, because it recomputes the CID from the bytes.
func TestRebuildRepairsAWrongEntry(t *testing.T) {
	ctx := context.Background()
	i, s, cat := newIngester(t)
	cfg := ingest.Config{Blocks: s.Blocks(), Catalog: cat}

	put, err := i.PutBlobs(ctx, bodyOf(makeBlob(5)))
	if err != nil {
		t.Fatalf("PutBlobs: %v", err)
	}
	wrong, err := schema.BlobCID(makeBlob(6))
	if err != nil {
		t.Fatalf("schema.BlobCID: %v", err)
	}
	if err := cat.Put(ctx, put[0].VH, wrong); err != nil {
		t.Fatalf("Put: %v", err)
	}

	stats, err := ingest.Rebuild(ctx, cfg)
	if err != nil {
		t.Fatalf("Rebuild: %v", err)
	}
	if stats.Upserted != 1 {
		t.Errorf("Upserted = %d, want 1", stats.Upserted)
	}
	got, ok, err := cat.ResolveBlob(ctx, put[0].VH)
	if err != nil || !ok || !got.Equals(put[0].CID) {
		t.Fatalf("ResolveBlob = (%s, %t, %v), want (%s, true, nil)", got, ok, err, put[0].CID)
	}
}

func TestRebuildEmptyStore(t *testing.T) {
	ctx := context.Background()
	_, s, cat := newIngester(t)

	stats, err := ingest.Rebuild(ctx, ingest.Config{Blocks: s.Blocks(), Catalog: cat})
	if err != nil {
		t.Fatalf("Rebuild: %v", err)
	}
	if stats != (ingest.RebuildStats{}) {
		t.Fatalf("Rebuild of an empty store: %+v, want zero", stats)
	}
}
