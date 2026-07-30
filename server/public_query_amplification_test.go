package server_test

// This focused regression file covers the safety boundary: the public blobs
// endpoint used to accept an unbounded count of repeated versioned_hashes, read
// one blob per entry, and materialize the whole response before writing it, for
// a measured 1,548x amplification from one unauthenticated request. The
// reproducer below now asserts the fix -- the count cap and the byte-weighted
// admission -- rather than the vulnerability, and the tests beside it hold the
// rest of the approved design: duplicates below the cap stay legal with their
// multiplicity and order preserved, a read that fails after admission is an
// error and never a truncated 200, and no blob is read before a request is
// admitted against the response-memory budget.

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ipfs/boxo/blockstore"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"

	"github.com/blobarchive/bloar/archive"
	"github.com/blobarchive/bloar/catalog"
	"github.com/blobarchive/bloar/ingest"
	"github.com/blobarchive/bloar/schema"
	"github.com/blobarchive/bloar/server"
	"github.com/blobarchive/bloar/store"
)

// countingBlocks is the read-path blockstore the amplification tests wire in
// front of the store. It records every blob Get so a test can assert a read did
// or did not happen, can fail a chosen blob's Get to exercise the
// failure-after-admission path, and can run a hook at the start of each Get to
// pin the ordering of admission against reads.
type countingBlocks struct {
	blockstore.Blockstore
	mu    sync.Mutex
	gets  int
	fail  map[string]error
	onGet func()
}

func (b *countingBlocks) Get(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	b.mu.Lock()
	b.gets++
	onGet, failErr := b.onGet, b.fail[c.String()]
	b.mu.Unlock()
	if onGet != nil {
		onGet()
	}
	if failErr != nil {
		return nil, failErr
	}
	return b.Blockstore.Get(ctx, c)
}

func (b *countingBlocks) getCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.gets
}

// ampOpts are the knobs the amplification tests vary.
type ampOpts struct {
	// nBlobs is how many distinct blobs are planted at the fixture's one slot, in
	// order. Zero means one.
	nBlobs int
	// maxQueryHashes and maxBytes override the two amplification bounds. Zero takes
	// the server default.
	maxQueryHashes int
	maxBytes       int64
	// failBlob is the 1-based index of the blob whose read-path Get returns an
	// error; zero fails none.
	failBlob int
	// onGet runs at the start of every read-path Get.
	onGet func()
}

// ampFixture is a server over one covered slot carrying nBlobs distinct blobs,
// wired through a countingBlocks so the read path is observable.
type ampFixture struct {
	handler http.Handler
	blocks  *countingBlocks
	vhs     []string // "0x"-prefixed, in stored order
	blobs   [][]byte // in stored order
	slot    uint64
}

// newAmpConfig plants nBlobs blobs at one slot without paying the KZG cost:
// like the pre-fix reproducer it starts after ingest and concerns only the read
// path, so it fabricates distinct versioned hashes and binds them in the
// catalog directly. It returns the server.Config those blobs are served through
// and the fixture describing them,
// with fix.handler left for the caller to fill -- so a test can either build the
// server or assert that server.New rejects the config.
func newAmpConfig(t *testing.T, opts ampOpts) (server.Config, *ampFixture) {
	t.Helper()
	ctx := t.Context()

	n := opts.nBlobs
	if n == 0 {
		n = 1
	}

	st, err := store.Open(t.TempDir(), store.WithPebbleLogger(quietPebble{}))
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() {
		if err := st.Close(); err != nil {
			t.Errorf("store.Close: %v", err)
		}
	})

	const slot = 8
	cat := catalog.New(st.KV())
	fix := &ampFixture{slot: slot}
	failCIDs := make(map[string]error, 1)
	vhList := make([]schema.VersionedHash, 0, n)
	for i := range n {
		data := makeBlob(uint64(300 + i))
		blobCID, err := schema.BlobCID(data)
		if err != nil {
			t.Fatalf("BlobCID: %v", err)
		}
		blk, err := blocks.NewBlockWithCid(data, blobCID)
		if err != nil {
			t.Fatalf("NewBlockWithCid: %v", err)
		}
		if err := st.Blocks().Put(ctx, blk); err != nil {
			t.Fatalf("storing blob: %v", err)
		}
		var vh schema.VersionedHash
		vh[0], vh[len(vh)-1] = 1, byte(i+1)
		if err := cat.Put(ctx, vh, blobCID); err != nil {
			t.Fatalf("catalog.Put: %v", err)
		}
		if opts.failBlob == i+1 {
			failCIDs[blobCID.String()] = errors.New("simulated blob read failure")
		}
		vhList = append(vhList, vh)
		fix.vhs = append(fix.vhs, "0x"+hex.EncodeToString(vh[:]))
		fix.blobs = append(fix.blobs, data)
	}

	params := archive.Params{Name: "audit-amplification", Net: "auditnet", OriginSlot: slot, SegBits: 3, FanoutBits: 2}
	roots := server.NewRootStore(st.KV())
	head, err := server.OpenHead(ctx, archive.Config{Blocks: st.Blocks(), Resolver: cat}, roots, params)
	if err != nil {
		t.Fatalf("OpenHead: %v", err)
	}
	heads, err := server.NewHeads(server.HeadsConfig{Net: params.Net, Roots: roots})
	if err != nil {
		t.Fatalf("NewHeads: %v", err)
	}
	if err := heads.Add(head); err != nil {
		t.Fatalf("Heads.Add: %v", err)
	}
	if _, err := heads.ApplyRefs(ctx, params.Name, []archive.RefRow{{Slot: slot, VHs: vhList}}, slot, cid.Undef); err != nil {
		t.Fatalf("ApplyRefs: %v", err)
	}
	ing, err := ingest.New(ingest.Config{Blocks: st.Blocks(), Catalog: cat})
	if err != nil {
		t.Fatalf("ingest.New: %v", err)
	}

	fix.blocks = &countingBlocks{Blockstore: st.Blocks(), fail: failCIDs, onGet: opts.onGet}
	cfg := server.Config{
		Heads:                    heads,
		Blocks:                   fix.blocks,
		Ingester:                 ing,
		AuthToken:                "audit-token",
		Beacon:                   server.Beacon{SecondsPerSlot: 12},
		MaxQueryHashes:           opts.maxQueryHashes,
		MaxResponseBytesInFlight: opts.maxBytes,
	}
	return cfg, fix
}

// newAmpFixture is newAmpConfig plus a server built from it, for the common case
// of a fixture that is expected to construct.
func newAmpFixture(t *testing.T, opts ampOpts) *ampFixture {
	t.Helper()
	cfg, fix := newAmpConfig(t, opts)
	handler, err := server.New(cfg)
	if err != nil {
		t.Fatalf("server.New: %v", err)
	}
	fix.handler = handler
	return fix
}

// ampURL builds a blobs request against the fixture's slot from a raw list of
// query values (which may repeat a hash to exercise multiplicity).
func (f *ampFixture) ampURL(vhs ...string) string {
	q := make(url.Values)
	for _, vh := range vhs {
		q.Add("versioned_hashes", vh)
	}
	u := "/audit-amplification/eth/v1/beacon/blobs/" + itoa(f.slot)
	if len(vhs) > 0 {
		u += "?" + q.Encode()
	}
	return u
}

// TestPublicVersionedHashQueryAmplifiesOneBlob is the flipped reproducer.
// Where it once asserted that N repeated hashes amplified one stored blob into N
// copies, it now asserts the count cap: a request naming more hashes than the
// configured maximum is refused 400 before any blob is looked up or read, so the
// amplification never begins.
func TestPublicVersionedHashQueryAmplifiesOneBlob(t *testing.T) {
	fix := newAmpFixture(t, ampOpts{maxQueryHashes: 8})

	// Sixty-four copies -- the count the original reproducer amplified -- now
	// exceeds the cap and is refused.
	const copies = 64
	dups := make([]string, copies)
	for i := range dups {
		dups[i] = fix.vhs[0]
	}
	req := httptest.NewRequest(http.MethodGet, fix.ampURL(dups...), nil)
	req.Header.Set("Accept", "application/octet-stream")
	rec := httptest.NewRecorder()
	fix.handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("over-cap query status/body = %d/%s, want 400", rec.Code, rec.Body.Bytes())
	}
	if !strings.Contains(rec.Body.String(), "too many versioned_hashes") {
		t.Errorf("400 body = %s, want it to name the count limit", rec.Body.Bytes())
	}
	if got := fix.blocks.getCount(); got != 0 {
		t.Fatalf("read path performed %d blob reads for a rejected request, want 0", got)
	}
}

// TestCommaSeparatedQueryCannotBypassCountCap holds the Base-compatible
// array encoding to the same amplification boundary as repeated query keys.
func TestCommaSeparatedQueryCannotBypassCountCap(t *testing.T) {
	fix := newAmpFixture(t, ampOpts{maxQueryHashes: 8})

	const copies = 64
	dups := make([]string, copies)
	for i := range dups {
		dups[i] = fix.vhs[0]
	}
	u := "/audit-amplification/eth/v1/beacon/blobs/" + itoa(fix.slot) +
		"?versioned_hashes=" + strings.Join(dups, ",")
	req := httptest.NewRequest(http.MethodGet, u, nil)
	rec := httptest.NewRecorder()
	fix.handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("over-cap comma query status/body = %d/%s, want 400", rec.Code, rec.Body.Bytes())
	}
	if !strings.Contains(rec.Body.String(), "64 requested") {
		t.Errorf("400 body = %s, want expanded entry count", rec.Body.Bytes())
	}
	if got := fix.blocks.getCount(); got != 0 {
		t.Fatalf("read path performed %d blob reads for a rejected comma query, want 0", got)
	}
}

// TestPublicDuplicatesPreservedBelowCap holds the spec 7.1 semantics the
// fix must not touch: a filtered query below the cap is answered in request
// order with duplicates repeated, one blob per entry -- the real
// dup-commitment case -- for both response encodings.
func TestPublicDuplicatesPreservedBelowCap(t *testing.T) {
	fix := newAmpFixture(t, ampOpts{nBlobs: 2})
	// vh1, vh0, vh1: vh1 named twice and out of stored order, so the answer must
	// echo the request, not the store.
	query := []string{fix.vhs[1], fix.vhs[0], fix.vhs[1]}
	want := [][]byte{fix.blobs[1], fix.blobs[0], fix.blobs[1]}

	t.Run("json", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, fix.ampURL(query...), nil)
		rec := httptest.NewRecorder()
		fix.handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("status/body = %d/%s, want 200", rec.Code, rec.Body.Bytes())
		}
		var body struct {
			Data []string `json:"data"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatalf("decoding response: %v", err)
		}
		if len(body.Data) != len(want) {
			t.Fatalf("data has %d entries, want %d (duplicate multiplicity preserved)", len(body.Data), len(want))
		}
		for i := range want {
			if body.Data[i] != "0x"+hex.EncodeToString(want[i]) {
				t.Errorf("entry %d is not the blob expected at that position", i)
			}
		}
	})

	t.Run("octet-stream", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, fix.ampURL(query...), nil)
		req.Header.Set("Accept", "application/octet-stream")
		rec := httptest.NewRecorder()
		fix.handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("status/body = %d/%s, want 200", rec.Code, rec.Body.Bytes())
		}
		if ct := rec.Header().Get("Content-Type"); ct != "application/octet-stream" {
			t.Fatalf("Content-Type = %q, want application/octet-stream", ct)
		}
		body := rec.Body.Bytes()
		if len(body) != len(want)*schema.BlobSize {
			t.Fatalf("octet body is %d bytes, want %d blobs = %d", len(body), len(want), len(want)*schema.BlobSize)
		}
		for i := range want {
			if !bytes.Equal(body[i*schema.BlobSize:(i+1)*schema.BlobSize], want[i]) {
				t.Errorf("octet blob %d is not the one expected at that position", i)
			}
		}
	})
}

// TestPublicReadFailureAfterAdmissionIsNotTruncated covers the
// no-streaming rider: a blob read that fails partway through a multi-blob
// response is an error status, never a 200 with some blobs already on the wire.
func TestPublicReadFailureAfterAdmissionIsNotTruncated(t *testing.T) {
	// Two blobs at the slot; the second one's read fails. An unfiltered request
	// reads them in stored order, so the first succeeds and the second faults.
	fix := newAmpFixture(t, ampOpts{nBlobs: 2, failBlob: 2})

	req := httptest.NewRequest(http.MethodGet, fix.ampURL(), nil)
	req.Header.Set("Accept", "application/octet-stream")
	rec := httptest.NewRecorder()
	fix.handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500 for a mid-response read failure", rec.Code)
	}
	// The whole response is buffered before any of it is written, so the failure
	// is a JSON error, not a partial octet-stream body with the first blob in it.
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json (the error, not a truncated body)", ct)
	}
	if len(rec.Body.Bytes()) >= schema.BlobSize {
		t.Fatalf("body is %d bytes; a blob leaked onto the wire before the failure", len(rec.Body.Bytes()))
	}
	if fix.blocks.getCount() < 2 {
		t.Fatalf("read path performed %d reads, want at least 2 (it must reach the failing blob)", fix.blocks.getCount())
	}
}

// TestPublicAdmissionPrecedesReadAndSerializes proves the two load-bearing
// properties of the byte budget at once: a request reserves before it reads (a
// blocked waiter has read nothing), and the budget serializes -- a second
// request waits while the budget is fully held and proceeds the moment it is
// released.
func TestPublicAdmissionPrecedesReadAndSerializes(t *testing.T) {
	// Block the first read-path Get, once, so the request holding the budget
	// parks mid-read while a second request tries to get in.
	firstGet := make(chan struct{})
	releaseGet := make(chan struct{})
	var once sync.Once
	onGet := func() {
		once.Do(func() {
			close(firstGet)
			<-releaseGet
		})
	}

	// A budget of exactly one maximum response. An unfiltered request reserves
	// the whole of it (the stored-row ceiling, worst case), so the second cannot
	// be admitted until the first releases.
	fix := newAmpFixture(t, ampOpts{maxBytes: server.MaxResponseWeight(schema.MaxBlobsPerSlotCeiling), onGet: onGet})

	type result struct{ code int }
	fire := func() <-chan result {
		out := make(chan result, 1)
		go func() {
			req := httptest.NewRequest(http.MethodGet, fix.ampURL(), nil) // unfiltered JSON
			rec := httptest.NewRecorder()
			fix.handler.ServeHTTP(rec, req)
			out <- result{rec.Code}
		}()
		return out
	}

	// A holds the whole budget and parks in its blocked read.
	aDone := fire()
	<-firstGet
	if got := fix.blocks.getCount(); got != 1 {
		t.Fatalf("first request performed %d reads before it was told to block, want 1", got)
	}

	// B is admitted against a saturated budget, so it must wait -- and while it
	// waits it must not have read anything, because admission precedes the read.
	bDone := fire()
	time.Sleep(50 * time.Millisecond) // let B reach the admission wait
	if got := fix.blocks.getCount(); got != 1 {
		t.Fatalf("second request read a blob (%d total) while blocked at admission; admission must precede the read", got)
	}
	select {
	case <-bDone:
		t.Fatal("second request completed while the budget was fully held by the first")
	default:
	}

	// Releasing the first's read lets it finish and hand the budget back; the
	// second is then admitted, reads, and completes.
	close(releaseGet)
	if res := <-aDone; res.code != http.StatusOK {
		t.Fatalf("first request status = %d, want 200", res.code)
	}
	if res := <-bDone; res.code != http.StatusOK {
		t.Fatalf("second request status = %d, want 200 after the budget was released", res.code)
	}
	if got := fix.blocks.getCount(); got != 2 {
		t.Fatalf("read path performed %d reads total, want 2 (one per admitted request)", got)
	}
}

// TestPublicQueryCapCannotExceedCeiling is the server.New half of the
// two-layer ceiling enforcement (cmd/bloard config validation is the other): a
// query cap above the per-slot blob ceiling would restore the amplification
// surface the safety boundary closed, so New refuses to build a server with one.
func TestPublicQueryCapCannotExceedCeiling(t *testing.T) {
	cfg, _ := newAmpConfig(t, ampOpts{maxQueryHashes: schema.MaxBlobsPerSlotCeiling + 1})
	if _, err := server.New(cfg); err == nil {
		t.Fatal("server.New accepted a query cap above the per-slot ceiling")
	} else if !strings.Contains(err.Error(), "128") {
		t.Errorf("error %v, want it to name the ceiling", err)
	}
}

// TestPublicReadFailureReleasesPermit proves the deferred release fires on
// the failure path too: a first request that faults on a later blob must hand its
// admission permit back, or a second request under a one-response budget could
// never be admitted.
func TestPublicReadFailureReleasesPermit(t *testing.T) {
	// Two blobs at the slot, the second one's read wired to fail, and a budget of
	// exactly one maximum response.
	fix := newAmpFixture(t, ampOpts{
		nBlobs:   2,
		failBlob: 2,
		maxBytes: server.MaxResponseWeight(schema.MaxBlobsPerSlotCeiling),
	})

	// First request: unfiltered, reads blob 1 then faults on blob 2 -> 500. Its
	// permit (the whole budget, reserved worst-case) must be released on the way
	// out.
	rec1 := httptest.NewRecorder()
	fix.handler.ServeHTTP(rec1, httptest.NewRequest(http.MethodGet, fix.ampURL(), nil))
	if rec1.Code != http.StatusInternalServerError {
		t.Fatalf("first request status = %d, want 500 for a mid-response read failure", rec1.Code)
	}

	// Second request under the same budget, for the one good blob. If the first
	// leaked its permit the budget is exhausted and this parks; a short deadline
	// turns that leak into a prompt 503 rather than a hang. A released permit lets
	// it be admitted and answered 200.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	req2 := httptest.NewRequest(http.MethodGet, fix.ampURL(fix.vhs[0]), nil).WithContext(ctx)
	rec2 := httptest.NewRecorder()
	fix.handler.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusOK {
		t.Fatalf("second request status = %d, want 200 (the first request's permit must be released after its read failed)", rec2.Code)
	}
}
