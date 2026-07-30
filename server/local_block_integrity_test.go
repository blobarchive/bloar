package server_test

// These regression tests exercise local blockstore integrity.
// The reproducer that named this file once showed a blob corrupted on disk being
// published and served 200 under its original CID; validated reads close that,
// so the tests now assert the read is refused (500) and the ref is refused before
// it can commit.

import (
	"bytes"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"

	"github.com/blobarchive/bloar/archive"
	"github.com/blobarchive/bloar/catalog"
	"github.com/blobarchive/bloar/ingest"
	"github.com/blobarchive/bloar/metrics"
	"github.com/blobarchive/bloar/schema"
	"github.com/blobarchive/bloar/server"
	"github.com/blobarchive/bloar/store"
)

// integrityFixture is the shared setup: a store with one honest blob
// ingested and catalogued, a head over it, and the blob's flatfs path in hand so
// a test can corrupt it. The ref is not applied here -- the two tests want it
// applied at different moments relative to the corruption.
type integrityFixture struct {
	st     *store.Store
	cat    *catalog.Catalog
	ing    *ingest.Ingester
	heads  *server.Heads
	head   *archive.Head
	mx     *metrics.Metrics
	put    ingest.PutResult
	blob   []byte
	path   string // the single flatfs .data file holding the blob
	params archive.Params
}

func newIntegrityFixture(t *testing.T) *integrityFixture {
	t.Helper()
	ctx := t.Context()
	dir := t.TempDir()
	st, err := store.Open(dir, store.WithPebbleLogger(quietPebble{}))
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() {
		if err := st.Close(); err != nil {
			t.Errorf("store.Close: %v", err)
		}
	})

	cat := catalog.New(st.KV())
	ing, err := ingest.New(ingest.Config{Blocks: st.Blocks(), Catalog: cat})
	if err != nil {
		t.Fatalf("ingest.New: %v", err)
	}
	honest := makeBlob(101)
	put, err := ing.PutBlobs(ctx, honest)
	if err != nil || len(put) != 1 {
		t.Fatalf("PutBlobs: results=%d err=%v", len(put), err)
	}
	// The blob is the only block on disk right now; capture its flatfs file before
	// OpenHead writes the empty head's dag-cbor root and there are two.
	path := onlyFlatfsDataFile(t, filepath.Join(dir, "blocks"))

	params := archive.Params{Name: "audit-integrity", Net: "auditnet", OriginSlot: 8, SegBits: 3, FanoutBits: 2}
	roots := server.NewRootStore(st.KV())
	head, err := server.OpenHead(ctx, archive.Config{Blocks: st.Blocks(), Resolver: cat}, roots, params)
	if err != nil {
		t.Fatalf("OpenHead: %v", err)
	}
	mx := metrics.New()
	heads, err := server.NewHeads(server.HeadsConfig{Net: params.Net, Roots: roots, Metrics: mx})
	if err != nil {
		t.Fatalf("NewHeads: %v", err)
	}
	if err := heads.Add(head); err != nil {
		t.Fatalf("Heads.Add: %v", err)
	}

	return &integrityFixture{
		st: st, cat: cat, ing: ing, heads: heads, head: head, mx: mx,
		put: put[0], blob: honest, path: path, params: params,
	}
}

// corrupt replaces the blob's bytes in place with a different, still well-formed
// blob, leaving the file name (the original multihash key) unchanged -- the exact
// on-disk corruption the safety boundary modelled, now caught on read rather than served.
func (f *integrityFixture) corrupt(t *testing.T) []byte {
	t.Helper()
	altered := makeBlob(202)
	if bytes.Equal(altered, f.blob) {
		t.Fatal("corruption fixture did not change the blob bytes")
	}
	if err := os.WriteFile(f.path, altered, 0o644); err != nil {
		t.Fatalf("replacing flatfs blob bytes: %v", err)
	}
	// The recomputed CID must no longer be the original, or there is nothing to
	// catch.
	recomputed, err := schema.BlobCID(altered)
	if err != nil {
		t.Fatalf("recomputing altered blob CID: %v", err)
	}
	if recomputed.Equals(f.put.CID) {
		t.Fatal("altered bytes unexpectedly retain the original blob CID")
	}
	return altered
}

func (f *integrityFixture) server(t *testing.T) *server.Server {
	t.Helper()
	handler, err := server.New(server.Config{
		Heads:     f.heads,
		Blocks:    f.st.Blocks(),
		Ingester:  f.ing,
		AuthToken: "audit-token",
		Beacon:    server.Beacon{SecondsPerSlot: 12},
		Metrics:   f.mx,
	})
	if err != nil {
		t.Fatalf("server.New: %v", err)
	}
	return handler
}

// TestLocalBlobCorruptionIsPublishedAndServedUnderOriginalCID is the
// flipped reproducer. The ref is committed while the blob is honest; the blob is
// then corrupted on disk; and the read -- both the JSON and the raw variant --
// is refused with 500 rather than serving the altered bytes 200 under the
// original CID. Each refusal is counted by bloar_store_corrupt_reads_total.
func TestLocalBlobCorruptionIsPublishedAndServedUnderOriginalCID(t *testing.T) {
	ctx := t.Context()
	f := newIntegrityFixture(t)

	// Commit the ref while the blob is still honest: a valid archive, later
	// corrupted, is the realistic path to a corrupt block a live root references.
	if _, err := f.heads.ApplyRefs(ctx, f.params.Name,
		[]archive.RefRow{{Slot: 8, VHs: []schema.VersionedHash{f.put.VH}}}, 8, cid.Undef); err != nil {
		t.Fatalf("ApplyRefs on an honest blob: %v", err)
	}
	altered := f.corrupt(t)

	handler := f.server(t)

	for _, tc := range []struct {
		name   string
		accept string
	}{
		{name: "json", accept: "application/json"},
		{name: "octet-stream", accept: "application/octet-stream"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/audit-integrity/eth/v1/beacon/blobs/8", nil)
			req.Header.Set("Accept", tc.accept)
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)

			if rec.Code != http.StatusInternalServerError {
				t.Fatalf("GET corrupt blob status = %d, want 500; body = %s", rec.Code, rec.Body.Bytes())
			}
			if bytes.Contains(rec.Body.Bytes(), altered) {
				t.Fatal("the response carried the altered bytes stored under the original CID")
			}
		})
	}

	// Two reads, two refusals counted.
	if got := corruptReads(t, f.mx, f.params.Name); got != 2 {
		t.Fatalf("bloar_store_corrupt_reads_total{head=%q} = %v, want 2", f.params.Name, got)
	}
}

// TestCorruptBlobRefRefusedBeforeCommit covers the ApplyRefs half: a blob
// whose catalog entry resolves and whose key is present (Has would say true) but
// whose stored bytes no longer match its CID is refused, and no coverage advances.
// Presence of a key is not proof of content.
func TestCorruptBlobRefRefusedBeforeCommit(t *testing.T) {
	ctx := t.Context()
	f := newIntegrityFixture(t)

	// Corrupt before the ref is applied. The catalog still resolves the vh to the
	// CID, and the key is still present -- only the bytes are wrong.
	f.corrupt(t)
	if has, err := f.st.Blocks().Has(ctx, f.put.CID); err != nil || !has {
		t.Fatalf("blob key presence: has=%t err=%v, want present", has, err)
	}

	rootBefore := f.head.Root()
	_, err := f.heads.ApplyRefs(ctx, f.params.Name,
		[]archive.RefRow{{Slot: 8, VHs: []schema.VersionedHash{f.put.VH}}}, 8, cid.Undef)
	if err == nil {
		t.Fatal("ApplyRefs committed a ref to a blob whose stored bytes no longer match its CID")
	}

	// No coverage, no root swap: the batch was refused whole.
	if _, covered := f.head.SyncedTo(); covered {
		t.Error("head advanced coverage despite the refused batch")
	}
	if !f.head.Root().Equals(rootBefore) {
		t.Errorf("head root changed on a refused batch: %s -> %s", rootBefore, f.head.Root())
	}
}

// TestCorruptIndexNodeRefusedOnRead covers the other codec: a corrupt
// dag-cbor index node on the path to a slot is refused on read, mapped to 500 and
// counted, not just a corrupt raw blob leaf. The same store backs both, and a
// corrupt spine must not be walked any more than corrupt bytes must be served.
func TestCorruptIndexNodeRefusedOnRead(t *testing.T) {
	ctx := t.Context()
	f := newIntegrityFixture(t)
	if _, err := f.heads.ApplyRefs(ctx, f.params.Name,
		[]archive.RefRow{{Slot: 8, VHs: []schema.VersionedHash{f.put.VH}}}, 8, cid.Undef); err != nil {
		t.Fatalf("ApplyRefs on an honest blob: %v", err)
	}
	selected, ok := f.heads.Get(f.params.Name)
	if !ok {
		t.Fatal("advanced head is not selected")
	}

	enum, err := selected.Enumerate(ctx)
	if err != nil {
		t.Fatalf("Enumerate: %v", err)
	}
	if !enum.Open.Defined() {
		t.Fatal("covered head has no open segment to corrupt")
	}
	// Corrupt the open segment (a dag-cbor index node) in place: delete, then store
	// wrong bytes under its key -- the same on-disk corruption a blob suffers.
	if err := f.st.Blocks().DeleteBlock(ctx, enum.Open); err != nil {
		t.Fatalf("deleting the open segment: %v", err)
	}
	bad, err := blocks.NewBlockWithCid([]byte("corrupt index node bytes"), enum.Open)
	if err != nil {
		t.Fatalf("framing corrupt index node: %v", err)
	}
	if err := f.st.Blocks().Put(ctx, bad); err != nil {
		t.Fatalf("storing corrupt index node: %v", err)
	}

	// A freshly loaded head reads its spine from the store rather than from the
	// memory the mutating head still holds, so the corrupt segment is on the read
	// path. The root itself is untouched, so the load succeeds and the failure is
	// where it should be: walking to the slot.
	mx := metrics.New()
	roots := server.NewRootStore(f.st.KV())
	head, err := server.OpenHead(ctx, archive.Config{Blocks: f.st.Blocks(), Resolver: f.cat}, roots, f.params)
	if err != nil {
		t.Fatalf("OpenHead: %v", err)
	}
	heads, err := server.NewHeads(server.HeadsConfig{Net: f.params.Net, Roots: roots, Metrics: mx})
	if err != nil {
		t.Fatalf("NewHeads: %v", err)
	}
	if err := heads.Add(head); err != nil {
		t.Fatalf("Heads.Add: %v", err)
	}
	handler, err := server.New(server.Config{
		Heads: heads, Blocks: f.st.Blocks(), Ingester: f.ing,
		AuthToken: "audit-token", Beacon: server.Beacon{SecondsPerSlot: 12}, Metrics: mx,
	})
	if err != nil {
		t.Fatalf("server.New: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/audit-integrity/eth/v1/beacon/blobs/8", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("GET through a corrupt index node: status = %d, want 500; body = %s", rec.Code, rec.Body.Bytes())
	}
	if got := corruptReads(t, mx, f.params.Name); got != 1 {
		t.Fatalf("bloar_store_corrupt_reads_total{head=%q} = %v, want 1", f.params.Name, got)
	}
}

// corruptReads reads bloar_store_corrupt_reads_total for one head off the
// registry, or 0 if it has no sample yet.
func corruptReads(t *testing.T, mx *metrics.Metrics, head string) float64 {
	t.Helper()
	families, err := mx.Registry().Gather()
	if err != nil {
		t.Fatalf("gathering metrics: %v", err)
	}
	for _, fam := range families {
		if fam.GetName() != "bloar_store_corrupt_reads_total" {
			continue
		}
		for _, m := range fam.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == "head" && l.GetValue() == head {
					return m.GetCounter().GetValue()
				}
			}
		}
	}
	return 0
}

func onlyFlatfsDataFile(t *testing.T, root string) string {
	t.Helper()
	var paths []string
	if err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".data") {
			paths = append(paths, path)
		}
		return nil
	}); err != nil {
		t.Fatalf("walking flatfs data files: %v", err)
	}
	if len(paths) != 1 {
		t.Fatalf("flatfs data files after one blob put = %d, want 1: %v", len(paths), paths)
	}
	return paths[0]
}
