package ingest_test

import (
	"context"
	"crypto/sha256"
	"errors"
	"testing"

	"github.com/ethereum/go-ethereum/crypto/kzg4844"

	"github.com/blobarchive/bloar/catalog"
	"github.com/blobarchive/bloar/ingest"
	"github.com/blobarchive/bloar/schema"
)

func TestPutBlobsHappyPath(t *testing.T) {
	ctx := context.Background()
	i, s, cat := newIngester(t)

	blobs := [][]byte{makeBlob(1), makeBlob(1 << 20)}
	got, err := i.PutBlobs(ctx, bodyOf(blobs...))
	if err != nil {
		t.Fatalf("PutBlobs: %v", err)
	}
	if len(got) != len(blobs) {
		t.Fatalf("PutBlobs returned %d results, want %d", len(got), len(blobs))
	}

	for k, blob := range blobs {
		// Results are in body order, and every field is derived from the bytes.
		wantCID, err := schema.BlobCID(blob)
		if err != nil {
			t.Fatalf("blob %d: schema.BlobCID: %v", k, err)
		}
		if !got[k].CID.Equals(wantCID) {
			t.Errorf("blob %d: CID = %s, want %s", k, got[k].CID, wantCID)
		}
		if got[k].VH != wantVH(t, blob) {
			t.Errorf("blob %d: VH = 0x%x, want 0x%x", k, got[k].VH, wantVH(t, blob))
		}

		// The block is durable...
		blk, err := s.Blocks().Get(ctx, got[k].CID)
		if err != nil {
			t.Fatalf("blob %d: block %s not in the blockstore: %v", k, got[k].CID, err)
		}
		if len(blk.RawData()) != schema.BlobSize {
			t.Errorf("blob %d: stored block is %d bytes, want %d", k, len(blk.RawData()), schema.BlobSize)
		}

		// ...and the catalog names it.
		resolved, ok, err := cat.ResolveBlob(ctx, got[k].VH)
		if err != nil || !ok {
			t.Fatalf("blob %d: ResolveBlob = (%s, %t, %v), want a hit", k, resolved, ok, err)
		}
		if !resolved.Equals(got[k].CID) {
			t.Errorf("blob %d: catalog resolves to %s, want %s", k, resolved, got[k].CID)
		}
	}

	if got[0].VH == got[1].VH {
		t.Error("distinct blobs produced the same versioned hash")
	}
}

// wantVH recomputes the versioned hash straight from the spec 1 definition --
// 0x01 || sha256(kzg_commitment)[1:] -- rather than from the helper the
// pipeline uses, so the test pins the derivation and not just its stability.
func wantVH(t *testing.T, blob []byte) schema.VersionedHash {
	t.Helper()
	if len(blob) != schema.BlobSize {
		t.Fatalf("blob is %d bytes, want %d", len(blob), schema.BlobSize)
	}
	commitment, err := kzg4844.BlobToCommitment((*kzg4844.Blob)(blob))
	if err != nil {
		t.Fatalf("BlobToCommitment: %v", err)
	}
	vh := schema.VersionedHash(sha256.Sum256(commitment[:]))
	vh[0] = 0x01
	return vh
}

func TestPutBlobsRejectsNonDivisibleBody(t *testing.T) {
	ctx := context.Background()
	i, _, _ := newIngester(t)

	for _, n := range []int{1, schema.BlobSize - 1, schema.BlobSize + 1, 2*schema.BlobSize - 7} {
		_, err := i.PutBlobs(ctx, make([]byte, n))
		var ve *ingest.ValidationError
		if !errors.As(err, &ve) {
			t.Fatalf("PutBlobs with a %d-byte body: err = %v, want *ingest.ValidationError", n, err)
		}
		// No single blob is at fault, so there is no index to report.
		if ve.Index != -1 {
			t.Errorf("PutBlobs with a %d-byte body: Index = %d, want -1", n, ve.Index)
		}
	}
}

func TestPutBlobsEmptyBody(t *testing.T) {
	ctx := context.Background()
	i, _, _ := newIngester(t)

	// Zero blobs is a whole number of blobs. Spec 7.2 has no lower bound, and
	// an empty put is a no-op, not a 400.
	got, err := i.PutBlobs(ctx, nil)
	if err != nil {
		t.Fatalf("PutBlobs with an empty body: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("PutBlobs with an empty body returned %d results, want 0", len(got))
	}
}

// TestPutBlobsRejectsInvalidFieldElement is the spec 7.2 rule that one bad blob
// fails the whole batch, naming its index -- and, because verification happens
// before any write, leaves nothing behind. Serial and fanned out: pass 1's
// concurrency must not change which index is named or that nothing lands.
func TestPutBlobsRejectsInvalidFieldElement(t *testing.T) {
	ctx := context.Background()

	good := makeBlob(1)
	body := bodyOf(good, makeInvalidBlob(), makeBlob(2))
	goodCID, err := schema.BlobCID(good)
	if err != nil {
		t.Fatalf("schema.BlobCID: %v", err)
	}

	for _, w := range []int{1, 8} {
		i, s, cat := newIngesterC(t, w)

		_, err := i.PutBlobs(ctx, body)
		var ve *ingest.ValidationError
		if !errors.As(err, &ve) {
			t.Fatalf("verify_concurrency=%d: err = %v, want *ingest.ValidationError", w, err)
		}
		if ve.Index != 1 {
			t.Fatalf("verify_concurrency=%d: Index = %d, want 1", w, ve.Index)
		}
		if ve.Err == nil {
			t.Errorf("verify_concurrency=%d: ValidationError.Err is nil; the KZG failure should be wrapped", w)
		}

		// Blob 0 is valid and precedes the bad one, but the batch was rejected: it
		// must not have been stored or catalogued.
		if has, err := s.Blocks().Has(ctx, goodCID); err != nil {
			t.Fatalf("verify_concurrency=%d: Has: %v", w, err)
		} else if has {
			t.Errorf("verify_concurrency=%d: a rejected batch stored the blob preceding the invalid one", w)
		}
		if _, ok, err := cat.ResolveBlob(ctx, wantVH(t, good)); err != nil {
			t.Fatalf("verify_concurrency=%d: ResolveBlob: %v", w, err)
		} else if ok {
			t.Errorf("verify_concurrency=%d: a rejected batch catalogued the blob preceding the invalid one", w)
		}
	}
}

// TestPutBlobsConcurrencyIsDeterministic checks that pass 1's fan-out does not
// change what pass 1 produces: a valid multi-blob body yields the same
// PutResults in the same order whether verification runs serially or in
// parallel. A blob's identity is a pure function of its bytes, so the only thing
// concurrency could break is the mapping from body position to result -- which
// is exactly what asserting body-order equality pins.
func TestPutBlobsConcurrencyIsDeterministic(t *testing.T) {
	ctx := context.Background()

	var blobs [][]byte
	for k := range 12 {
		blobs = append(blobs, makeBlob(uint64(k)*1000+1))
	}
	body := bodyOf(blobs...)

	serialI, _, _ := newIngesterC(t, 1)
	serial, err := serialI.PutBlobs(ctx, body)
	if err != nil {
		t.Fatalf("serial PutBlobs: %v", err)
	}

	parallelI, _, _ := newIngesterC(t, 8)
	parallel, err := parallelI.PutBlobs(ctx, body)
	if err != nil {
		t.Fatalf("parallel PutBlobs: %v", err)
	}

	if len(serial) != len(parallel) {
		t.Fatalf("serial returned %d results, parallel %d", len(serial), len(parallel))
	}
	for k := range serial {
		if serial[k].VH != parallel[k].VH || !serial[k].CID.Equals(parallel[k].CID) {
			t.Errorf("blob %d: parallel = (0x%x, %s), serial = (0x%x, %s)",
				k, parallel[k].VH, parallel[k].CID, serial[k].VH, serial[k].CID)
		}
	}
}

// TestPutBlobsReportsLowestInvalidIndex is the parallel path's error
// determinism (spec 7.2): a body with more than one bad blob is rejected by the
// position of the first, never whichever verification finished first in wall
// time. The guarantee must hold serial and fanned out, so both settings are
// asserted against the same two-bad-blob body.
func TestPutBlobsReportsLowestInvalidIndex(t *testing.T) {
	ctx := context.Background()

	// Bad at 1 and 3, valid elsewhere.
	body := bodyOf(makeBlob(1), makeInvalidBlob(), makeBlob(2), makeInvalidBlob(), makeBlob(3))

	for _, w := range []int{1, 8} {
		i, s, _ := newIngesterC(t, w)

		_, err := i.PutBlobs(ctx, body)
		var ve *ingest.ValidationError
		if !errors.As(err, &ve) {
			t.Fatalf("verify_concurrency=%d: err = %v, want *ingest.ValidationError", w, err)
		}
		if ve.Index != 1 {
			t.Errorf("verify_concurrency=%d: Index = %d, want 1 (the lowest bad blob)", w, ve.Index)
		}
		if ve.Err == nil {
			t.Errorf("verify_concurrency=%d: ValidationError.Err is nil; the KZG failure should be wrapped", w)
		}

		// Rejected before any write, so the valid blob at 0 left nothing.
		good0, err := schema.BlobCID(makeBlob(1))
		if err != nil {
			t.Fatalf("schema.BlobCID: %v", err)
		}
		if has, err := s.Blocks().Has(ctx, good0); err != nil {
			t.Fatalf("verify_concurrency=%d: Has: %v", w, err)
		} else if has {
			t.Errorf("verify_concurrency=%d: a rejected batch stored blob 0", w)
		}
	}
}

func TestPutBlobsIsIdempotent(t *testing.T) {
	ctx := context.Background()
	i, s, cat := newIngester(t)

	body := bodyOf(makeBlob(7), makeBlob(9))
	first, err := i.PutBlobs(ctx, body)
	if err != nil {
		t.Fatalf("first PutBlobs: %v", err)
	}
	second, err := i.PutBlobs(ctx, body)
	if err != nil {
		t.Fatalf("second PutBlobs: %v", err)
	}

	if len(first) != len(second) {
		t.Fatalf("re-put returned %d results, want %d", len(second), len(first))
	}
	for k := range first {
		if first[k].VH != second[k].VH || !first[k].CID.Equals(second[k].CID) {
			t.Errorf("blob %d: re-put returned (0x%x, %s), want (0x%x, %s)",
				k, second[k].VH, second[k].CID, first[k].VH, first[k].CID)
		}
		if has, err := s.Blocks().Has(ctx, first[k].CID); err != nil || !has {
			t.Errorf("blob %d: block %s missing after re-put (err %v)", k, first[k].CID, err)
		}
		got, ok, err := cat.ResolveBlob(ctx, first[k].VH)
		if err != nil || !ok || !got.Equals(first[k].CID) {
			t.Errorf("blob %d: after re-put ResolveBlob = (%s, %t, %v), want (%s, true, nil)",
				k, got, ok, err, first[k].CID)
		}
	}
}

func TestVersionedHashRejectsWrongSize(t *testing.T) {
	if _, err := ingest.VersionedHash(make([]byte, schema.BlobSize-1)); err == nil {
		t.Fatal("VersionedHash of a short blob: want error, got nil")
	}
}

func TestNewRequiresConfig(t *testing.T) {
	s := openStore(t, t.TempDir())
	if _, err := ingest.New(ingest.Config{Catalog: nil, Blocks: s.Blocks()}); err == nil {
		t.Error("ingest.New without a catalog: want error, got nil")
	}
	if _, err := ingest.New(ingest.Config{Catalog: catalog.New(s.KV()), Blocks: nil}); err == nil {
		t.Error("ingest.New without a blockstore: want error, got nil")
	}
}
