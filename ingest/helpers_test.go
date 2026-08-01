package ingest_test

import (
	"encoding/binary"
	"testing"

	"github.com/blobarchive/bloar/catalog"
	"github.com/blobarchive/bloar/ingest"
	"github.com/blobarchive/bloar/schema"
	"github.com/blobarchive/bloar/store"
)

// lanes is how many field elements a blob is made of.
const lanes = schema.BlobSize / 32

// makeBlob builds a valid blob. A blob is 4096 field elements, each a 32-byte
// big-endian integer that MUST be less than the BLS12-381 scalar modulus
// (0x73ed...0001). Leaving each lane's top byte zero puts every element far
// below the modulus whatever the low bytes say, so seeding the low 8 bytes
// gives distinct valid blobs for free. A zero blob is valid too.
func makeBlob(seed uint64) []byte {
	b := make([]byte, schema.BlobSize)
	for i := range lanes {
		binary.BigEndian.PutUint64(b[i*32+24:i*32+32], seed+uint64(i))
	}
	return b
}

// makeInvalidBlob builds a blob whose first field element is 2^256-1, which is
// well above the modulus: not canonical, so it has no KZG commitment and no
// versioned hash.
func makeInvalidBlob() []byte {
	b := makeBlob(1)
	for i := range 32 {
		b[i] = 0xFF
	}
	return b
}

// bodyOf concatenates blobs into a PutBlobs body.
func bodyOf(blobs ...[]byte) []byte {
	body := make([]byte, 0, len(blobs)*schema.BlobSize)
	for _, b := range blobs {
		body = append(body, b...)
	}
	return body
}

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

// newIngester wires an Ingester over a fresh store and returns it alongside the
// pieces the tests assert against. Verify concurrency is left at its default.
func newIngester(t *testing.T) (*ingest.Ingester, *store.Store, *catalog.Catalog) {
	t.Helper()
	return newIngesterC(t, 0)
}

// newIngesterC is newIngester with an explicit VerifyConcurrency, for the tests
// that assert pass 1 behaves identically serial (1) and fanned out (>1).
func newIngesterC(t *testing.T, verifyConcurrency int) (*ingest.Ingester, *store.Store, *catalog.Catalog) {
	t.Helper()
	s := openStore(t, t.TempDir())
	cat := catalog.New(s.KV())
	i, err := ingest.New(ingest.Config{Blocks: s.Blocks(), Catalog: cat, VerifyConcurrency: verifyConcurrency})
	if err != nil {
		t.Fatalf("ingest.New: %v", err)
	}
	return i, s, cat
}
