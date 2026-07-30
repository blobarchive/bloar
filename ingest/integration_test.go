package ingest_test

import (
	"context"
	"errors"
	"testing"

	"github.com/blobarchive/bloar/archive"
	"github.com/blobarchive/bloar/schema"
)

// TestIngestToArchiveRoundTrip closes the loop the two phases were built to
// meet on: real blobs go in through the ingest pipeline, the catalog it wrote
// is the resolver archive validates refs against, and a lookup comes back with
// the blob CIDs ingest derived.
//
// Everything in between -- vh derivation, catalog durability, apply_refs'
// resolve-and-check-presence step, the segment encoding -- has to agree for
// this to pass, which is the point: each package's own tests stub out the
// others.
func TestIngestToArchiveRoundTrip(t *testing.T) {
	ctx := context.Background()
	i, s, cat := newIngester(t)

	put, err := i.PutBlobs(ctx, bodyOf(makeBlob(101), makeBlob(202), makeBlob(303)))
	if err != nil {
		t.Fatalf("PutBlobs: %v", err)
	}

	h, err := archive.New(ctx, archive.Config{
		Blocks:   s.Blocks(),
		Resolver: cat, // the real catalog, not a stand-in
	}, archive.Params{
		Name:       "all",
		Net:        "testnet",
		OriginSlot: 8,
		SegBits:    3,
		FanoutBits: 2,
	})
	if err != nil {
		t.Fatalf("archive.New: %v", err)
	}

	// Two slots: one carrying two of the blobs, one carrying the third.
	rows := []archive.RefRow{
		{Slot: 9, VHs: []schema.VersionedHash{put[0].VH, put[1].VH}},
		{Slot: 11, VHs: []schema.VersionedHash{put[2].VH}},
	}
	res, err := h.ApplyRefs(ctx, rows, 12)
	if err != nil {
		t.Fatalf("ApplyRefs: %v", err)
	}
	if res.SyncedTo != 12 {
		t.Fatalf("ApplyRefs synced_to = %d, want 12", res.SyncedTo)
	}

	// The refs resolved through the catalog, so the blob CIDs the archive
	// stored must be exactly the ones ingest returned.
	got, err := h.LookupVHs(ctx, 9, []schema.VersionedHash{put[0].VH, put[1].VH})
	if err != nil {
		t.Fatalf("LookupVHs: %v", err)
	}
	if got.Status != archive.StatusFound {
		t.Fatalf("LookupVHs at slot 9: status = %s, want found", got.Status)
	}
	if len(got.Entries) != 2 {
		t.Fatalf("LookupVHs at slot 9 returned %d entries, want 2", len(got.Entries))
	}
	for k, e := range got.Entries {
		if e.VH != put[k].VH {
			t.Errorf("entry %d: vh = 0x%x, want 0x%x", k, e.VH, put[k].VH)
		}
		if !e.Blob.Equals(put[k].CID) {
			t.Errorf("entry %d: blob = %s, want %s", k, e.Blob, put[k].CID)
		}
	}

	// Request order, not stored order (spec 7.1).
	rev, err := h.LookupVHs(ctx, 9, []schema.VersionedHash{put[1].VH, put[0].VH})
	if err != nil {
		t.Fatalf("LookupVHs reversed: %v", err)
	}
	if len(rev.Entries) != 2 || !rev.Entries[0].Blob.Equals(put[1].CID) || !rev.Entries[1].Blob.Equals(put[0].CID) {
		t.Errorf("LookupVHs reversed did not answer in request order: %+v", rev.Entries)
	}

	// The blob bytes are really there, addressed by what came back.
	for _, e := range got.Entries {
		blk, err := s.Blocks().Get(ctx, e.Blob)
		if err != nil {
			t.Fatalf("blob block %s: %v", e.Blob, err)
		}
		if len(blk.RawData()) != schema.BlobSize {
			t.Errorf("blob block %s is %d bytes, want %d", e.Blob, len(blk.RawData()), schema.BlobSize)
		}
	}

	// A vh nobody ingested is refused by the same catalog, as a conflict
	// naming it (spec 5.1 step 4).
	_, err = h.ApplyRefs(ctx, []archive.RefRow{{Slot: 13, VHs: []schema.VersionedHash{wantVH(t, makeBlob(999))}}}, 13)
	var missing *archive.MissingBlobsError
	if err == nil {
		t.Fatal("ApplyRefs with an uningested vh: want a conflict, got nil")
	}
	if !errors.As(err, &missing) {
		t.Fatalf("ApplyRefs with an uningested vh: err = %v, want *archive.MissingBlobsError", err)
	}
}
