package beacon_test

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/crypto/kzg4844"

	"github.com/blobarchive/bloar/index/archclient"
	"github.com/blobarchive/bloar/index/beacon"
	"github.com/blobarchive/bloar/index/upstream"
	"github.com/blobarchive/bloar/ingest"
	"github.com/blobarchive/bloar/metrics"
	"github.com/blobarchive/bloar/schema"
)

// These tests drive the beacon indexer of spec 10.1 end to end against fakes,
// both anchored mode (a trusted block feed + untrusted blob sources) and mirror
// mode (a trusted archive upstream). The fakes model prysm's reality: a block
// feed that 404s a missed slot and a not-yet-backfilled one alike, a blinded
// block that carries only commitments, and a blob source that 404s or 200-empties
// a slot it has pruned.

// testBlob is a canonical, valid, distinct blob for (slot, idx). Its first field
// element holds a small number, well under the BLS modulus, so it commits.
func testBlob(slot uint64, idx int) []byte {
	b := make([]byte, schema.BlobSize)
	binary.BigEndian.PutUint32(b[24:28], uint32(slot))
	binary.BigEndian.PutUint32(b[28:32], uint32(idx))
	return b
}

// slotBlobs builds n distinct blobs for a slot.
func slotBlobs(slot uint64, n int) [][]byte {
	blobs := make([][]byte, n)
	for i := range n {
		blobs[i] = testBlob(slot, i)
	}
	return blobs
}

// vhOf is a blob's real versioned hash.
func vhOf(t *testing.T, blob []byte) schema.VersionedHash {
	t.Helper()
	vh, err := ingest.VersionedHash(blob)
	if err != nil {
		t.Fatalf("hashing a fixture blob: %v", err)
	}
	return vh
}

// deriveRoot is the synthetic chain's deterministic root for a slot.
func deriveRoot(slot uint64) [32]byte {
	var r [32]byte
	r[0] = 0xbe
	binary.BigEndian.PutUint64(r[24:], slot)
	return r
}

func rootHex(r [32]byte) string { return "0x" + hex.EncodeToString(r[:]) }

// -------------------------------------------------------------------------
// Fake block feed (anchored mode's trusted authority)
// -------------------------------------------------------------------------

type feedBlock struct {
	root       [32]byte
	parentRoot [32]byte
	blobs      [][]byte // block order; commitments derived from these
}

// fakeBlockFeed is a beacon node's block API: the finalized checkpoint, the
// per-slot header, and the block's blob commitments. A slot absent from blocks is
// a header 404 (a missed or not-yet-backfilled slot).
type fakeBlockFeed struct {
	srv *httptest.Server
	url string

	finalized  atomic.Uint64
	optimistic atomic.Bool
	syncing503 atomic.Bool

	mu                   sync.Mutex
	blocks               map[uint64]feedBlock
	headerOptimistic     map[uint64]int
	headerFinalizedFalse map[uint64]bool
	headerRequests       map[uint64]int
}

func newFakeBlockFeed(t *testing.T, finalized uint64) *fakeBlockFeed {
	t.Helper()
	f := &fakeBlockFeed{
		blocks:               map[uint64]feedBlock{},
		headerOptimistic:     map[uint64]int{},
		headerFinalizedFalse: map[uint64]bool{},
		headerRequests:       map[uint64]int{},
	}
	f.finalized.Store(finalized)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /eth/v1/beacon/headers/finalized", f.handleFinalized)
	mux.HandleFunc("GET /eth/v1/beacon/headers/{slot}", f.handleHeader)
	mux.HandleFunc("GET /eth/v1/beacon/blinded_blocks/{slot}", f.handleBlinded)
	f.srv = httptest.NewServer(mux)
	t.Cleanup(f.srv.Close)
	f.url = f.srv.URL
	return f
}

// present records a present block at slot, chaining its parent_root to parent.
func (f *fakeBlockFeed) present(slot uint64, parent [32]byte, blobs [][]byte) [32]byte {
	root := deriveRoot(slot)
	f.mu.Lock()
	f.blocks[slot] = feedBlock{root: root, parentRoot: parent, blobs: blobs}
	f.mu.Unlock()
	return root
}

// absent removes a slot's block, so its header 404s: a feed that has not (or no
// longer) backfilled it. Used to reshape a feed mid-test (e.g. a rewind).
func (f *fakeBlockFeed) absent(slots ...uint64) {
	f.mu.Lock()
	for _, s := range slots {
		delete(f.blocks, s)
	}
	f.mu.Unlock()
}

// optimisticHeader makes the next n reads of slot's per-slot header report
// execution_optimistic:true. The finalized checkpoint remains safe: this is the
// exact transient inconsistency observed during a finalized-read recovery.
func (f *fakeBlockFeed) optimisticHeader(slot uint64, n int) {
	f.mu.Lock()
	f.headerOptimistic[slot] = n
	f.mu.Unlock()
}

func (f *fakeBlockFeed) finalizedFalseHeader(slot uint64) {
	f.mu.Lock()
	f.headerFinalizedFalse[slot] = true
	f.mu.Unlock()
}

func (f *fakeBlockFeed) headerRequestCount(slot uint64) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.headerRequests[slot]
}

func (f *fakeBlockFeed) handleFinalized(w http.ResponseWriter, r *http.Request) {
	if f.syncing503.Load() {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"message": "syncing"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"execution_optimistic": f.optimistic.Load(),
		"finalized":            true,
		"data": map[string]any{
			"canonical": true,
			"header":    map[string]any{"message": map[string]any{"slot": strconv.FormatUint(f.finalized.Load(), 10)}},
		},
	})
}

func (f *fakeBlockFeed) handleHeader(w http.ResponseWriter, r *http.Request) {
	slot, _ := strconv.ParseUint(r.PathValue("slot"), 10, 64)
	f.mu.Lock()
	blk, ok := f.blocks[slot]
	f.headerRequests[slot]++
	optimistic := f.headerOptimistic[slot] > 0
	if optimistic {
		f.headerOptimistic[slot]--
	}
	finalized := !f.headerFinalizedFalse[slot]
	f.mu.Unlock()
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]any{"code": 404, "message": "no block"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"execution_optimistic": optimistic,
		"finalized":            finalized,
		"data": map[string]any{
			"root":      rootHex(blk.root),
			"canonical": true,
			"header":    map[string]any{"message": map[string]any{"parent_root": rootHex(blk.parentRoot)}},
		},
	})
}

func (f *fakeBlockFeed) handleBlinded(w http.ResponseWriter, r *http.Request) {
	slot, _ := strconv.ParseUint(r.PathValue("slot"), 10, 64)
	f.mu.Lock()
	blk, ok := f.blocks[slot]
	f.mu.Unlock()
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]any{"code": 404, "message": "no block"})
		return
	}
	commits := make([]string, 0, len(blk.blobs))
	for _, b := range blk.blobs {
		c, err := kzg4844.BlobToCommitment((*kzg4844.Blob)(b))
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]any{"message": "bad blob"})
			return
		}
		commits = append(commits, "0x"+hex.EncodeToString(c[:]))
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"execution_optimistic": false,
		"finalized":            true,
		"data":                 map[string]any{"message": map[string]any{"body": map[string]any{"blob_kzg_commitments": commits}}},
	})
}

// -------------------------------------------------------------------------
// Fake blob source (anchored mode's untrusted bytes)
// -------------------------------------------------------------------------

// fakeSource is a beacon node's blobs endpoint: it returns the bytes it was told
// to for a slot, ignoring the filter for WHAT it serves (a test sets exactly what
// a source serves), and records which slots were asked for -- and with which
// versioned_hashes -- so a test can assert zero blob traffic and that every
// source is asked for the whole slot.
type fakeSource struct {
	srv *httptest.Server
	url string

	mu        sync.Mutex
	cond      *sync.Cond
	blobs     map[uint64][][]byte // slot -> bytes served on a 200
	status    map[uint64]int      // slot -> non-200 status (overrides blobs)
	blockCtx  map[uint64]bool     // slot -> block until the request ctx cancels
	requested map[uint64]int
	filter    map[uint64][]string // slot -> the versioned_hashes query seen (nil == unfiltered)
}

func newFakeSource(t *testing.T) *fakeSource {
	t.Helper()
	s := &fakeSource{
		blobs:     map[uint64][][]byte{},
		status:    map[uint64]int{},
		blockCtx:  map[uint64]bool{},
		requested: map[uint64]int{},
		filter:    map[uint64][]string{},
	}
	s.cond = sync.NewCond(&s.mu)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /eth/v1/beacon/blobs/{slot}", s.handleBlobs)
	s.srv = httptest.NewServer(mux)
	t.Cleanup(s.srv.Close)
	s.url = s.srv.URL
	return s
}

// serve tells the source to answer slot's request with these bytes. The fake
// records but otherwise ignores an unexpected filter.
func (s *fakeSource) serve(slot uint64, blobs [][]byte) {
	s.mu.Lock()
	s.blobs[slot] = blobs
	s.mu.Unlock()
}

func (s *fakeSource) handleBlobs(w http.ResponseWriter, r *http.Request) {
	slot, _ := strconv.ParseUint(r.PathValue("slot"), 10, 64)

	s.mu.Lock()
	s.requested[slot]++
	s.filter[slot] = r.URL.Query()["versioned_hashes"]
	block := s.blockCtx[slot]
	status := s.status[slot]
	blobs, ok := s.blobs[slot]
	s.cond.Broadcast()
	s.mu.Unlock()

	if block {
		<-r.Context().Done()
		return
	}
	if status != 0 {
		writeJSON(w, status, map[string]any{"code": status, "message": "source says no"})
		return
	}
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]any{"code": 404, "message": "pruned"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"data": hexBlobs(blobs)})
}

func (s *fakeSource) wasRequested(slot uint64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.requested[slot] > 0
}

// filterFor is the versioned_hashes query the source saw for slot. The indexer
// must leave it nil and ask for the whole slot.
func (s *fakeSource) filterFor(slot uint64) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.filter[slot])
}

func (s *fakeSource) waitRequested(want ...uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for {
		all := true
		for _, slot := range want {
			if s.requested[slot] == 0 {
				all = false
				break
			}
		}
		if all {
			return
		}
		s.cond.Wait()
	}
}

// -------------------------------------------------------------------------
// Fake mirror upstream (a trusted bloar archive)
// -------------------------------------------------------------------------

// fakeMirrorUpstream is a bloar archive's read + finality surface: unfiltered
// blobs, synced_to, and origin_slot. A slot absent from blobs and below coverage
// is a 404 (a protocol violation once origin is validated); above coverage is a
// 503.
type fakeMirrorUpstream struct {
	srv  *httptest.Server
	url  string
	head string

	origin   uint64
	syncedTo uint64
	blobs    map[uint64][][]byte // covered slots with blobs; covered-empty is a 200 []
	notFound map[uint64]bool     // covered slots the archive (wrongly) 404s
}

func newFakeMirrorUpstream(t *testing.T, head string, origin, syncedTo uint64) *fakeMirrorUpstream {
	t.Helper()
	u := &fakeMirrorUpstream{
		head: head, origin: origin, syncedTo: syncedTo,
		blobs: map[uint64][][]byte{}, notFound: map[uint64]bool{},
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /bloar/v1/heads/{head}/synced_to", u.handleSyncedTo)
	mux.HandleFunc("GET /bloar/v1/heads/{head}", u.handleHead)
	mux.HandleFunc("GET /{head}/eth/v1/beacon/blobs/{slot}", u.handleBlobs)
	u.srv = httptest.NewServer(mux)
	t.Cleanup(u.srv.Close)
	u.url = u.srv.URL
	return u
}

func (u *fakeMirrorUpstream) handleHead(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"name": u.head, "origin_slot": u.origin, "synced_to": u.syncedTo})
}

func (u *fakeMirrorUpstream) handleSyncedTo(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"synced_to": u.syncedTo})
}

func (u *fakeMirrorUpstream) handleBlobs(w http.ResponseWriter, r *http.Request) {
	slot, _ := strconv.ParseUint(r.PathValue("slot"), 10, 64)
	if slot > u.syncedTo {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"code": 503, "message": "not yet covered"})
		return
	}
	if u.notFound[slot] {
		writeJSON(w, http.StatusNotFound, map[string]any{"code": 404, "message": "below origin"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"data": hexBlobs(u.blobs[slot])})
}

// -------------------------------------------------------------------------
// Fake archive (the head being written)
// -------------------------------------------------------------------------

type fakeArchive struct {
	srv  *httptest.Server
	url  string
	head string

	origin uint64

	mu       sync.Mutex
	cond     *sync.Cond
	syncedTo *uint64
	writes   []string

	refsGate    chan struct{}
	refsGated   bool
	failRefs    bool
	unavailable bool

	// idempotent models spec 5.1's idempotent replay: a refs POST whose synced_to
	// is at or below the archive's current coverage is a no-op that does NOT lower
	// it, and the response carries the CURRENT (possibly higher) synced_to, not the
	// echoed request. Off by default (the plain fake echoes the request); on for the
	// duplicate-writer tests where a twin has advanced coverage past A's batch.
	idempotent bool
}

func newFakeArchive(t *testing.T, head string) *fakeArchive {
	t.Helper()
	a := &fakeArchive{head: head}
	a.cond = sync.NewCond(&a.mu)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /bloar/v1/heads/{head}/synced_to", a.handleSyncedTo)
	mux.HandleFunc("GET /bloar/v1/heads/{head}", a.handleHead)
	mux.HandleFunc("POST /bloar/v1/blobs", a.handlePutBlobs)
	mux.HandleFunc("POST /bloar/v1/heads/{head}/refs", a.handleRefs)
	a.srv = httptest.NewServer(mux)
	t.Cleanup(a.srv.Close)
	a.url = a.srv.URL
	return a
}

func (a *fakeArchive) handleHead(w http.ResponseWriter, r *http.Request) {
	a.mu.Lock()
	syncedTo := a.syncedTo
	unavailable := a.unavailable
	a.mu.Unlock()
	if unavailable {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"code": 503, "message": "archive unavailable"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"name": a.head, "root": "bafyroot", "origin_slot": a.origin, "synced_to": syncedTo,
	})
}

func (a *fakeArchive) handleSyncedTo(w http.ResponseWriter, r *http.Request) {
	a.mu.Lock()
	syncedTo := a.syncedTo
	unavailable := a.unavailable
	a.mu.Unlock()
	if unavailable {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"code": 503, "message": "archive unavailable"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"synced_to": syncedTo})
}

func (a *fakeArchive) handlePutBlobs(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)

	a.mu.Lock()
	if a.unavailable {
		a.mu.Unlock()
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"code": 503, "message": "archive unavailable"})
		return
	}
	a.writes = append(a.writes, "put:"+hex.EncodeToString(body))
	a.mu.Unlock()

	if len(body)%schema.BlobSize != 0 {
		writeJSON(w, http.StatusBadRequest, map[string]any{"code": 400, "message": "not divisible"})
		return
	}
	n := len(body) / schema.BlobSize
	out := make([]map[string]string, 0, n)
	for i := range n {
		blob := body[i*schema.BlobSize : (i+1)*schema.BlobSize]
		vh, err := ingest.VersionedHash(blob)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"code": 400, "message": "bad blob"})
			return
		}
		out = append(out, map[string]string{"versioned_hash": archclient.VHHex(vh), "cid": fmt.Sprintf("bafyblob%d", i)})
	}
	writeJSON(w, http.StatusOK, map[string]any{"blobs": out})
}

func (a *fakeArchive) handleRefs(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)

	a.mu.Lock()
	if a.unavailable {
		a.mu.Unlock()
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"code": 503, "message": "archive unavailable"})
		return
	}
	a.writes = append(a.writes, "refs:"+string(body))
	gate := a.refsGate
	firstGated := gate != nil && !a.refsGated
	if firstGated {
		a.refsGated = true
	}
	fail := a.failRefs
	a.mu.Unlock()

	if firstGated {
		<-gate
	}
	if fail {
		writeJSON(w, http.StatusBadRequest, map[string]any{"code": 400, "message": "refs refused"})
		return
	}

	var req struct {
		SyncedTo uint64 `json:"synced_to"`
	}
	_ = json.Unmarshal(body, &req)

	a.mu.Lock()
	resp, noop := req.SyncedTo, false
	if a.idempotent && a.syncedTo != nil && req.SyncedTo <= *a.syncedTo {
		// A twin already covers at least this far: a no-op that keeps the higher
		// tip and reports it, rather than lowering coverage to A's request.
		resp, noop = *a.syncedTo, true
	} else {
		v := req.SyncedTo
		a.syncedTo = &v
	}
	a.cond.Broadcast()
	a.mu.Unlock()

	writeJSON(w, http.StatusOK, map[string]any{"synced_to": resp, "root": "bafyroot", "noop": noop})
}

func (a *fakeArchive) waitSyncedTo(target uint64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	for a.syncedTo == nil || *a.syncedTo < target {
		a.cond.Wait()
	}
}

func (a *fakeArchive) recordedWrites() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return slices.Clone(a.writes)
}

func (a *fakeArchive) coverage() (uint64, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.syncedTo == nil {
		return 0, false
	}
	return *a.syncedTo, true
}

func (a *fakeArchive) setUnavailable(unavailable bool) {
	a.mu.Lock()
	a.unavailable = unavailable
	a.mu.Unlock()
}

// -------------------------------------------------------------------------
// Indexer builders and drivers
// -------------------------------------------------------------------------

func beaconSources(t *testing.T, hc *http.Client, srcs ...*fakeSource) []beacon.Source {
	t.Helper()
	out := make([]beacon.Source, len(srcs))
	for i, src := range srcs {
		c, err := upstream.New(upstream.Config{BaseURL: src.url, HTTPClient: hc, MaxAttempts: 1, Backoff: time.Millisecond})
		if err != nil {
			t.Fatalf("upstream.New: %v", err)
		}
		out[i] = beacon.Source{Client: c}
	}
	return out
}

// newAnchoredIndexer builds an anchored indexer over whole-slot blob sources.
func newAnchoredIndexer(t *testing.T, feed *fakeBlockFeed, a *fakeArchive, batch uint64, concurrency int, hc *http.Client, srcs ...*fakeSource) *beacon.Indexer {
	t.Helper()
	return newAnchoredIndexerRuntime(t, feed, a, batch, concurrency, hc, time.Hour, nil, srcs...)
}

func newAnchoredIndexerRuntime(
	t *testing.T,
	feed *fakeBlockFeed,
	a *fakeArchive,
	batch uint64,
	concurrency int,
	hc *http.Client,
	poll time.Duration,
	m *metrics.Metrics,
	srcs ...*fakeSource,
) *beacon.Indexer {
	t.Helper()
	blocks, err := upstream.NewBlockClient(upstream.Config{BaseURL: feed.url, HTTPClient: hc, MaxAttempts: 1, Backoff: time.Millisecond})
	if err != nil {
		t.Fatalf("NewBlockClient: %v", err)
	}
	arch, err := archclient.New(archclient.Config{BaseURL: a.url, Token: "t", MaxAttempts: 1, Backoff: time.Millisecond})
	if err != nil {
		t.Fatalf("archclient.New: %v", err)
	}
	ix, err := beacon.New(beacon.Config{
		Sources: beaconSources(t, hc, srcs...), Blocks: blocks, Archive: arch, Head: a.head,
		BatchSize: batch, MaxPutBlobs: 64, FetchConcurrency: concurrency, PollInterval: poll, Metrics: m,
	})
	if err != nil {
		t.Fatalf("beacon.New: %v", err)
	}
	return ix
}

// drain Steps an indexer until it is caught up, and returns any error.
func drain(t *testing.T, ix *beacon.Indexer) error {
	t.Helper()
	for {
		advanced, err := ix.Step(t.Context())
		if err != nil {
			return err
		}
		if !advanced {
			return nil
		}
	}
}

// -------------------------------------------------------------------------
// Tests
// -------------------------------------------------------------------------

// TestAnchoredHappyPath is case 1: a block with two commitments, a source that
// serves both anchored, a row carrying the two block-derived vhs, and coverage
// that advances.
func TestAnchoredHappyPath(t *testing.T) {
	feed := newFakeBlockFeed(t, 2)
	src := newFakeSource(t)
	a := newFakeArchive(t, "all")

	// A continuous chain 0..2; slot 2 carries two blobs.
	r0 := feed.present(0, [32]byte{}, nil)
	r1 := feed.present(1, r0, nil)
	blobs := slotBlobs(2, 2)
	feed.present(2, r1, blobs)
	src.serve(2, blobs)

	ix := newAnchoredIndexer(t, feed, a, 8, 1, nil, src)
	if err := drain(t, ix); err != nil {
		t.Fatalf("drain: %v", err)
	}

	if got, ok := a.coverage(); !ok || got != 2 {
		t.Fatalf("coverage = %d (%v), want 2", got, ok)
	}
	refs := lastRefs(t, a)
	for _, b := range blobs {
		if !strings.Contains(refs, archclient.VHHex(vhOf(t, b))) {
			t.Errorf("refs row is missing a block-derived vh:\n%s", refs)
		}
	}
}

// TestAnchoredBloblessQueriesNoSource is case 2: a present block with zero
// commitments is resolved from the block feed alone -- no blob source is asked --
// and coverage advances over it.
func TestAnchoredBloblessQueriesNoSource(t *testing.T) {
	feed := newFakeBlockFeed(t, 1)
	src := newFakeSource(t)
	a := newFakeArchive(t, "all")

	r0 := feed.present(0, [32]byte{}, nil)
	feed.present(1, r0, nil) // present, blobless

	ix := newAnchoredIndexer(t, feed, a, 8, 1, nil, src)
	if err := drain(t, ix); err != nil {
		t.Fatalf("drain: %v", err)
	}
	if got, _ := a.coverage(); got != 1 {
		t.Fatalf("coverage = %d, want 1", got)
	}
	if src.wasRequested(0) || src.wasRequested(1) {
		t.Error("a blobless slot queried a blob source; anchored mode must ask none")
	}
}

// TestAnchoredMissedSlotProvenByContinuity is case 3: a header 404 whose absence
// the next present block proves by parent-root continuity -- no row, coverage
// advances.
func TestAnchoredMissedSlotProvenByContinuity(t *testing.T) {
	feed := newFakeBlockFeed(t, 2)
	src := newFakeSource(t)
	a := newFakeArchive(t, "all")

	r0 := feed.present(0, [32]byte{}, nil)
	// slot 1 missed (no block). slot 2 present, parent chains to slot 0.
	feed.present(2, r0, nil)

	ix := newAnchoredIndexer(t, feed, a, 8, 1, nil, src)
	if err := drain(t, ix); err != nil {
		t.Fatalf("drain: %v", err)
	}
	if got, _ := a.coverage(); got != 2 {
		t.Fatalf("coverage = %d, want 2 (the missed slot 1 proven and skipped)", got)
	}
}

// TestAnchoredHiddenBlockIsFatal is case 4: a header 404 for a slot whose block
// the next present slot's parent_root shows must exist -- a hidden or
// not-yet-backfilled block -- is a fatal error, and nothing is recorded.
func TestAnchoredHiddenBlockIsFatal(t *testing.T) {
	feed := newFakeBlockFeed(t, 2)
	src := newFakeSource(t)
	a := newFakeArchive(t, "all")

	feed.present(0, [32]byte{}, nil)
	// slot 1 is 404'd. slot 2 present, but its parent_root points at slot 1's
	// root, not slot 0's: the 404 hid a block that must exist.
	feed.present(2, deriveRoot(1), nil)

	ix := newAnchoredIndexer(t, feed, a, 8, 1, nil, src)
	err := drain(t, ix)
	if err == nil {
		t.Fatal("a hidden block was accepted; continuity must make it fatal")
	}
	if !strings.Contains(err.Error(), "continuity broken") || !strings.Contains(err.Error(), "slot 2") {
		t.Errorf("error does not name the continuity break at the present slot: %v", err)
	}
	if w := a.recordedWrites(); len(w) != 0 {
		t.Errorf("a continuity break recorded something: %v", w)
	}
}

// TestAnchoredPrunedPrimary is case 5, THE regression: a primary that 200-empties
// (here, 404s) a slot the block proves has a blob. With a fallback that has it,
// the slot is served anchored; with none, the batch errors -- never absence.
//
// Both providers are ordinary whole-slot sources. The fallback can be another
// beacon node or Bloar archive; neither source's claim of absence is trusted.
func TestAnchoredPrunedPrimary(t *testing.T) {
	build := func(t *testing.T, withFallback bool) (*beacon.Indexer, *fakeArchive, *fakeSource, *fakeSource) {
		feed := newFakeBlockFeed(t, 1)
		primary := newFakeSource(t)
		fallback := newFakeSource(t)
		a := newFakeArchive(t, "all")

		r0 := feed.present(0, [32]byte{}, nil)
		blobs := slotBlobs(1, 1)
		feed.present(1, r0, blobs)
		// The primary has pruned slot 1 (404); the fallback has it.
		fallback.serve(1, blobs)

		if withFallback {
			ix := newAnchoredIndexer(t, feed, a, 8, 1, nil, primary, fallback)
			return ix, a, primary, fallback
		}
		ix := newAnchoredIndexer(t, feed, a, 8, 1, nil, primary)
		return ix, a, primary, fallback
	}

	t.Run("fallback serves it", func(t *testing.T) {
		ix, a, primary, fallback := build(t, true)
		if err := drain(t, ix); err != nil {
			t.Fatalf("drain: %v", err)
		}
		if got, _ := a.coverage(); got != 1 {
			t.Fatalf("coverage = %d, want 1 (the fallback served the pruned slot)", got)
		}
		// The unfiltered primary was asked for the whole slot: no versioned_hashes.
		if f := primary.filterFor(1); f != nil {
			t.Errorf("the unfiltered primary was asked with versioned_hashes %v; it must get the whole slot", f)
		}
		if f := fallback.filterFor(1); f != nil {
			t.Errorf("the fallback was asked with versioned_hashes %v; it must get the whole slot", f)
		}
	})

	t.Run("no fallback errors", func(t *testing.T) {
		ix, a, _, _ := build(t, false)
		err := drain(t, ix)
		if err == nil {
			t.Fatal("a pruned primary with no fallback was accepted; absence must never be recorded")
		}
		if !strings.Contains(err.Error(), "no source served") {
			t.Errorf("error is not the source-exhausted one: %v", err)
		}
		if _, ok := a.coverage(); ok {
			t.Error("the batch advanced coverage despite failing to source the slot")
		}
	})
}

// TestAnchoredDupCommitmentSlot is the prysm-panic-class slot: a block with three
// IDENTICAL commitments [A, A, A]. A whole-slot source returns the blob three
// times in canonical block order, and the row carries the vh three times.
func TestAnchoredDupCommitmentSlot(t *testing.T) {
	dup := testBlob(2, 0)
	blobs := [][]byte{dup, dup, dup}
	vhHex := archclient.VHHex(vhOf(t, dup))

	feed := newFakeBlockFeed(t, 2)
	src := newFakeSource(t)
	a := newFakeArchive(t, "all")

	r0 := feed.present(0, [32]byte{}, nil)
	r1 := feed.present(1, r0, nil)
	feed.present(2, r1, blobs)
	src.serve(2, blobs)

	ix := newAnchoredIndexer(t, feed, a, 8, 1, nil, src)
	if err := drain(t, ix); err != nil {
		t.Fatalf("drain: %v", err)
	}
	if got, _ := a.coverage(); got != 2 {
		t.Fatalf("coverage = %d, want 2", got)
	}
	if f := src.filterFor(2); f != nil {
		t.Errorf("the source was asked with versioned_hashes %v", f)
	}
	if refs := lastRefs(t, a); strings.Count(refs, vhHex) != 3 {
		t.Errorf("refs row does not carry the duplicate vh three times:\n%s", refs)
	}
}

// TestAnchoredUnfilteredWrongCountFallsThrough is the acceptance rule under an
// unfiltered request: a whole-slot source that returns too few blobs for a slot
// the block attests is source-absent, never absence -- the walk falls through to
// the next whole-slot source.
func TestAnchoredUnfilteredWrongCountFallsThrough(t *testing.T) {
	feed := newFakeBlockFeed(t, 1)
	primary := newFakeSource(t)
	fallback := newFakeSource(t)
	a := newFakeArchive(t, "all")

	r0 := feed.present(0, [32]byte{}, nil)
	real := slotBlobs(1, 3) // the block attests three blobs
	feed.present(1, r0, real)
	// The unfiltered primary returns only two of the three: a wrong count, which is
	// source-absent, not a fact about the slot.
	primary.serve(1, real[:2])
	// The fallback has all three.
	fallback.serve(1, real)

	ix := newAnchoredIndexer(t, feed, a, 8, 1, nil, primary, fallback)
	if err := drain(t, ix); err != nil {
		t.Fatalf("drain: %v", err)
	}
	if got, _ := a.coverage(); got != 1 {
		t.Fatalf("coverage = %d, want 1 (the fallback served after the primary's short answer)", got)
	}
	// The short-answering primary was still asked unfiltered.
	if f := primary.filterFor(1); f != nil {
		t.Errorf("the unfiltered primary was asked with versioned_hashes %v", f)
	}
	if f := fallback.filterFor(1); f != nil {
		t.Errorf("the fallback was asked with versioned_hashes %v", f)
	}
}

// TestAnchoredWrongBytes is case 6: a primary serving bytes that do not commit to
// the block's vh is a mismatch (warned), the fallback's correct bytes are
// recorded; both wrong is an error.
func TestAnchoredWrongBytes(t *testing.T) {
	t.Run("fallback corrects it", func(t *testing.T) {
		feed := newFakeBlockFeed(t, 1)
		primary := newFakeSource(t)
		fallback := newFakeSource(t)
		a := newFakeArchive(t, "all")

		r0 := feed.present(0, [32]byte{}, nil)
		real := slotBlobs(1, 1)
		feed.present(1, r0, real)
		// The primary serves a valid but wrong blob (a different one); it fails the
		// KZG anchoring against the block's commitment. The fallback serves the real
		// one.
		primary.serve(1, [][]byte{testBlob(999, 0)})
		fallback.serve(1, real)

		ix := newAnchoredIndexer(t, feed, a, 8, 1, nil, primary, fallback)
		if err := drain(t, ix); err != nil {
			t.Fatalf("drain: %v", err)
		}
		if got, _ := a.coverage(); got != 1 {
			t.Fatalf("coverage = %d, want 1", got)
		}
	})

	t.Run("both wrong errors", func(t *testing.T) {
		feed := newFakeBlockFeed(t, 1)
		primary := newFakeSource(t)
		fallback := newFakeSource(t)
		a := newFakeArchive(t, "all")

		r0 := feed.present(0, [32]byte{}, nil)
		feed.present(1, r0, slotBlobs(1, 1))
		primary.serve(1, [][]byte{testBlob(999, 0)})
		fallback.serve(1, [][]byte{testBlob(998, 0)})

		ix := newAnchoredIndexer(t, feed, a, 8, 1, nil, primary, fallback)
		if err := drain(t, ix); err == nil {
			t.Fatal("two sources serving wrong bytes were accepted")
		}
	})
}

// TestAnchoredFinalityWaits is case 7: an optimistic head and a syncing 503 both
// make the block feed's finality ok=false, and the indexer waits -- recording
// nothing -- rather than erroring.
func TestAnchoredFinalityWaits(t *testing.T) {
	for _, tc := range []struct {
		name string
		set  func(*fakeBlockFeed)
	}{
		{"optimistic", func(f *fakeBlockFeed) { f.optimistic.Store(true) }},
		{"syncing 503", func(f *fakeBlockFeed) { f.syncing503.Store(true) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			feed := newFakeBlockFeed(t, 5)
			src := newFakeSource(t)
			a := newFakeArchive(t, "all")
			r0 := feed.present(0, [32]byte{}, nil)
			feed.present(1, r0, nil)
			tc.set(feed)

			ix := newAnchoredIndexer(t, feed, a, 8, 1, nil, src)
			advanced, err := ix.Step(t.Context())
			if err != nil {
				t.Fatalf("Step: %v", err)
			}
			if advanced {
				t.Error("the indexer advanced despite unusable finality")
			}
			if _, ok := a.coverage(); ok {
				t.Error("the indexer recorded coverage while finality said to wait")
			}
		})
	}
}

// TestAnchoredContinuitySeedAndCarry is case 8: a resume from mid-history seeds
// the anchor by walking back to a present slot, continuity carries across
// batches, and a seed that finds no present slot within the bound is a hard
// error.
func TestAnchoredContinuitySeedAndCarry(t *testing.T) {
	t.Run("seed found, continuity carries across batches", func(t *testing.T) {
		const origin = 2000
		feed := newFakeBlockFeed(t, origin+5)
		src := newFakeSource(t)
		a := newFakeArchive(t, "all")
		a.origin = origin

		// A present slot before origin for the seed, a missed slot, then the
		// covered range, all chaining. Small batch so the range spans two batches.
		seedRoot := feed.present(origin-2, deriveRoot(origin-3), nil)
		prev := seedRoot
		for slot := uint64(origin); slot <= origin+5; slot++ {
			blobs := [][]byte(nil)
			if slot%2 == 0 {
				blobs = slotBlobs(slot, 1)
				src.serve(slot, blobs)
			}
			prev = feed.present(slot, prev, blobs)
		}

		ix := newAnchoredIndexer(t, feed, a, 2, 1, nil, src)
		if err := drain(t, ix); err != nil {
			t.Fatalf("drain: %v", err)
		}
		if got, _ := a.coverage(); got != origin+5 {
			t.Fatalf("coverage = %d, want %d", got, origin+5)
		}
	})

	t.Run("seed exceeds the bound", func(t *testing.T) {
		const origin = 2000
		feed := newFakeBlockFeed(t, origin+5)
		src := newFakeSource(t)
		a := newFakeArchive(t, "all")
		a.origin = origin
		// Present slots only from origin up; everything below (where the seed walks)
		// is a 404, so the seed never finds an anchor and gives up past the bound.
		prev := feed.present(origin, [32]byte{}, nil)
		for slot := uint64(origin + 1); slot <= origin+5; slot++ {
			prev = feed.present(slot, prev, nil)
		}

		ix := newAnchoredIndexer(t, feed, a, 8, 1, nil, src)
		err := drain(t, ix)
		if err == nil {
			t.Fatal("a seed that found no present slot within the bound was accepted")
		}
		if !strings.Contains(err.Error(), "seed") {
			t.Errorf("error is not the seed-bound one: %v", err)
		}
	})
}

// trailingMissChain builds slots 0..6 where slot 3 is a genuine missed slot
// (header 404, slot 4 chains over it) and slots 2 and 6 carry a blob. Batch size
// 4 puts slot 3 as the trailing slot of the first batch, so it tests that a
// trailing 404 is not committed until the next batch proves it.
func trailingMissChain(t *testing.T) (*fakeBlockFeed, *fakeSource, *fakeArchive) {
	t.Helper()
	feed := newFakeBlockFeed(t, 6)
	src := newFakeSource(t)
	a := newFakeArchive(t, "all")

	r0 := feed.present(0, [32]byte{}, nil)
	r1 := feed.present(1, r0, nil)
	b2 := slotBlobs(2, 1)
	r2 := feed.present(2, r1, b2)
	src.serve(2, b2)
	// slot 3 missed. slot 4 chains to slot 2's root, proving it.
	r4 := feed.present(4, r2, nil)
	r5 := feed.present(5, r4, nil)
	b6 := slotBlobs(6, 1)
	feed.present(6, r5, b6)
	src.serve(6, b6)
	return feed, src, a
}

// TestAnchoredTrailingMissTrimsCoverage is the boundary rule: a batch whose walk
// ends in a header-404 commits only up to its last present slot, and the next
// batch re-walks the trailing 404 and proves it by continuity.
func TestAnchoredTrailingMissTrimsCoverage(t *testing.T) {
	feed, src, a := trailingMissChain(t)
	ix := newAnchoredIndexer(t, feed, a, 4, 1, nil, src)

	// Batch 1 is [0, 3]; slot 3 is the trailing 404, so coverage trims to slot 2,
	// NOT slot 3 -- the miss is not committed until proven.
	advanced, err := ix.Step(t.Context())
	if err != nil || !advanced {
		t.Fatalf("batch 1: advanced %v, err %v", advanced, err)
	}
	if got, _ := a.coverage(); got != 2 {
		t.Fatalf("after batch 1 coverage = %d, want 2 (the trailing 404 at slot 3 left uncommitted)", got)
	}

	// Batch 2 is [3, 6]; it re-walks slot 3 (now interior) and its continuity is
	// proven by slot 4, so coverage reaches slot 6.
	advanced, err = ix.Step(t.Context())
	if err != nil || !advanced {
		t.Fatalf("batch 2: advanced %v, err %v", advanced, err)
	}
	if got, _ := a.coverage(); got != 6 {
		t.Fatalf("after batch 2 coverage = %d, want 6", got)
	}
}

// TestAnchoredTrailingMissPipelined is the same rule under the pipelined path: a
// trimmed batch must not spawn a prefetch, and the run still covers the whole
// range correctly.
func TestAnchoredTrailingMissPipelined(t *testing.T) {
	feed, src, a := trailingMissChain(t)
	ix := newAnchoredIndexer(t, feed, a, 4, 6, nil, src)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- ix.Run(ctx) }()

	a.waitSyncedTo(6)
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got, _ := a.coverage(); got != 6 {
		t.Fatalf("coverage = %d, want 6", got)
	}
}

// TestAnchoredTrailingHiddenBlockNeverCommitted is the reason the trim matters: a
// trailing 404 that turns out to be a HIDDEN block (the next batch's first present
// slot chains to it, not to the committed anchor) is a fatal continuity break --
// and because it was never committed, no false absence was ever recorded.
func TestAnchoredTrailingHiddenBlockNeverCommitted(t *testing.T) {
	feed := newFakeBlockFeed(t, 5)
	src := newFakeSource(t)
	a := newFakeArchive(t, "all")

	r0 := feed.present(0, [32]byte{}, nil)
	r1 := feed.present(1, r0, nil)
	feed.present(2, r1, nil)
	// slot 3 is 404'd but a block exists there: slot 4 chains to slot 3's root, not
	// slot 2's. slot 3 is the trailing slot of batch [0, 3].
	feed.present(4, deriveRoot(3), nil)
	feed.present(5, deriveRoot(4), nil)

	ix := newAnchoredIndexer(t, feed, a, 4, 1, nil, src)

	// Batch 1 trims the trailing 404 at slot 3: coverage is slot 2, and slot 3 is
	// NOT recorded as absent.
	if _, err := ix.Step(t.Context()); err != nil {
		t.Fatalf("batch 1: %v", err)
	}
	if got, _ := a.coverage(); got != 2 {
		t.Fatalf("after batch 1 coverage = %d, want 2", got)
	}

	// Batch 2 re-walks slot 3 and finds slot 4 chaining over a block slot 3 must
	// have: a fatal continuity break, with nothing past slot 2 ever committed.
	err := drain(t, ix)
	if err == nil || !strings.Contains(err.Error(), "continuity broken") || !strings.Contains(err.Error(), "slot 4") {
		t.Fatalf("want a continuity break at slot 4, got %v", err)
	}
	if got, _ := a.coverage(); got != 2 {
		t.Fatalf("coverage = %d after the break, want it never past 2 (the hidden slot never committed)", got)
	}
}

// TestAnchoredAllMissedBatchWaits is the degenerate trim: a batch whose slots are
// all header-404 (a feed still backfilling blocks) commits nothing and waits, with
// no error -- it must never record a missed range.
func TestAnchoredAllMissedBatchWaits(t *testing.T) {
	const origin = 100
	feed := newFakeBlockFeed(t, origin+3)
	src := newFakeSource(t)
	a := newFakeArchive(t, "all")
	a.origin = origin

	// A present slot before origin for the seed; the covered range 100..103 is all
	// 404 (the feed has not backfilled those blocks yet).
	feed.present(origin-1, deriveRoot(origin-2), nil)

	ix := newAnchoredIndexer(t, feed, a, 8, 1, nil, src)
	advanced, err := ix.Step(t.Context())
	if err != nil {
		t.Fatalf("an all-missed batch errored instead of waiting: %v", err)
	}
	if advanced {
		t.Error("an all-missed batch advanced coverage")
	}
	if _, ok := a.coverage(); ok {
		t.Error("an all-missed batch recorded coverage; it must wait for a block")
	}
}

// TestMirrorMode is case 9: mirror mode's origin validation refusal, its
// treatment of an unfiltered 404 as a protocol violation, its 503 early stop, and
// a plain happy replay.
func TestMirrorMode(t *testing.T) {
	newMirror := func(t *testing.T, up *fakeMirrorUpstream, a *fakeArchive) *beacon.Indexer {
		src, err := upstream.New(upstream.Config{BaseURL: up.url, Head: up.head, MaxAttempts: 1, Backoff: time.Millisecond})
		if err != nil {
			t.Fatalf("upstream.New: %v", err)
		}
		arch, err := archclient.New(archclient.Config{BaseURL: a.url, Token: "t", MaxAttempts: 1, Backoff: time.Millisecond})
		if err != nil {
			t.Fatalf("archclient.New: %v", err)
		}
		ix, err := beacon.New(beacon.Config{
			Sources: []beacon.Source{{Client: src}}, Archive: arch, Head: a.head,
			BatchSize: 8, MaxPutBlobs: 64, FetchConcurrency: 1, PollInterval: time.Hour,
		})
		if err != nil {
			t.Fatalf("beacon.New: %v", err)
		}
		return ix
	}

	t.Run("refuses a higher upstream origin", func(t *testing.T) {
		up := newFakeMirrorUpstream(t, "all", 200, 210)
		a := newFakeArchive(t, "all")
		a.origin = 100 // local origin is below the upstream's: it cannot re-derive us.

		if err := drain(t, newMirror(t, up, a)); err == nil || !strings.Contains(err.Error(), "origin_slot") {
			t.Fatalf("mirror ran with a higher upstream origin: %v", err)
		}
	})

	t.Run("unfiltered 404 is a protocol violation", func(t *testing.T) {
		up := newFakeMirrorUpstream(t, "all", 100, 105)
		up.notFound[102] = true // a covered slot the archive wrongly 404s
		a := newFakeArchive(t, "all")
		a.origin = 100

		if err := drain(t, newMirror(t, up, a)); err == nil || !strings.Contains(err.Error(), "protocol violation") {
			t.Fatalf("mirror treated a 404 as absence rather than a violation: %v", err)
		}
	})

	t.Run("503 stops the batch and replays the covered range", func(t *testing.T) {
		up := newFakeMirrorUpstream(t, "all", 100, 103)
		up.blobs[100] = slotBlobs(100, 1)
		up.blobs[102] = slotBlobs(102, 2)
		// 101, 103 covered-empty; 104+ is a 503 (past coverage).
		a := newFakeArchive(t, "all")
		a.origin = 100

		if err := drain(t, newMirror(t, up, a)); err != nil {
			t.Fatalf("mirror replay: %v", err)
		}
		if got, _ := a.coverage(); got != 103 {
			t.Fatalf("coverage = %d, want the upstream's 103 (stopped at the 503)", got)
		}
	})
}

// TestFinalizedOptimisticReadRetriesInProcess reproduces a production
// finalized-read incident. The finalized checkpoint itself is safe, but one per-slot header
// remains optimistic after the request client's attempt budget. Run must retain
// the durable head and retry inside the same process; it must not post the
// optimistic batch. Both the serial reference loop and the one-batch pipeline
// use the same recovery boundary.
func TestFinalizedOptimisticReadRetriesInProcess(t *testing.T) {
	for _, concurrency := range []int{1, 6} {
		t.Run(fmt.Sprintf("concurrency_%d", concurrency), func(t *testing.T) {
			feed := newFakeBlockFeed(t, 2)
			src := newFakeSource(t)
			a := newFakeArchive(t, "all")
			m := metrics.New()

			var prev [32]byte
			for slot := uint64(0); slot <= 2; slot++ {
				prev = feed.present(slot, prev, nil)
			}
			feed.optimisticHeader(1, 1)

			ix := newAnchoredIndexerRuntime(t, feed, a, 8, concurrency, nil, 2*time.Millisecond, m, src)
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- ix.Run(ctx) }()

			for {
				if got, ok := a.coverage(); ok && got == 2 {
					break
				}
				select {
				case err := <-done:
					t.Fatalf("Run returned before recovering the optimistic slot: %v", err)
				case <-ctx.Done():
					t.Fatalf("timed out waiting for in-process recovery: %v", ctx.Err())
				case <-time.After(time.Millisecond):
				}
			}
			for !strings.Contains(scrapeMetrics(t, m), `bloar_index_outcomes_total{head="all",outcome="caught_up"}`) {
				select {
				case err := <-done:
					t.Fatalf("Run returned before recording the caught-up outcome: %v", err)
				case <-ctx.Done():
					t.Fatalf("timed out waiting for caught-up outcome: %v", ctx.Err())
				case <-time.After(time.Millisecond):
				}
			}
			cancel()
			if err := <-done; err != nil {
				t.Fatalf("Run after cancellation: %v", err)
			}
			if got := feed.headerRequestCount(1); got != 2 {
				t.Fatalf("slot 1 header requests = %d, want 2 (one rejected optimistic read, one safe retry)", got)
			}

			body := scrapeMetrics(t, m)
			if !strings.Contains(body, `bloar_index_retries_total{head="all",reason="execution_optimistic"} 1`) {
				t.Fatalf("retry metric missing from recovered run:\n%s", body)
			}
			if !strings.Contains(body, `bloar_index_outcomes_total{head="all",outcome="retry"} 1`) {
				t.Fatalf("retry outcome missing from recovered run:\n%s", body)
			}
			if !strings.Contains(body, `bloar_index_outcomes_total{head="all",outcome="caught_up"}`) {
				t.Fatalf("caught-up outcome missing from recovered run:\n%s", body)
			}
			if !strings.Contains(body, `bloar_index_last_progress_timestamp_seconds{head="all"} `) {
				t.Fatalf("last-progress metric missing from recovered run:\n%s", body)
			}
		})
	}
}

// TestFinalizedContradictionRemainsTerminal proves the retry classification is
// narrow. A per-slot finalized:false response contradicts the trusted finalized
// bound, so it remains fatal and cannot advance coverage.
func TestFinalizedContradictionRemainsTerminal(t *testing.T) {
	feed := newFakeBlockFeed(t, 1)
	src := newFakeSource(t)
	a := newFakeArchive(t, "all")

	r0 := feed.present(0, [32]byte{}, nil)
	feed.present(1, r0, nil)
	feed.finalizedFalseHeader(1)

	ix := newAnchoredIndexerRuntime(t, feed, a, 8, 1, nil, time.Millisecond, metrics.New(), src)
	err := ix.Run(t.Context())
	if err == nil || !strings.Contains(err.Error(), "finalized:false") {
		t.Fatalf("Run error = %v, want terminal finalized:false contradiction", err)
	}
	var optimistic *upstream.ExecutionOptimisticError
	if errors.As(err, &optimistic) {
		t.Fatalf("finalized:false was misclassified as optimistic retry: %v", err)
	}
	if _, ok := a.coverage(); ok {
		t.Fatal("a contradictory finalized slot advanced archive coverage")
	}
}

// TestArchiveUnavailabilityReturnsToTheProcessRetryBoundary proves the beacon
// loop does not relabel a cold publication writer as a fatal indexing outcome.
// cmd/bloar-index owns the slower in-process reconstruction loop; Run must hand
// the typed availability error back to it with durable coverage untouched.
func TestArchiveUnavailabilityReturnsToTheProcessRetryBoundary(t *testing.T) {
	feed := newFakeBlockFeed(t, 0)
	src := newFakeSource(t)
	a := newFakeArchive(t, "all")
	a.setUnavailable(true)
	m := metrics.New()

	ix := newAnchoredIndexerRuntime(t, feed, a, 1, 1, nil, time.Millisecond, m, src)
	err := ix.Run(t.Context())
	if err == nil || !archclient.IsUnavailable(err) {
		t.Fatalf("Run error = %v, want archive unavailable", err)
	}
	if _, ok := a.coverage(); ok {
		t.Fatal("an unavailable archive changed durable coverage")
	}
	body := scrapeMetrics(t, m)
	if strings.Contains(body, `bloar_index_outcomes_total{head="all",outcome="fatal"}`) {
		t.Fatalf("a classified archive outage was counted as fatal:\n%s", body)
	}
}

// TestFinalizedOptimisticRetryCancellationCleanlyStops covers cancellation
// during the outer retry cycle. The fixed backoff is deliberately very long;
// cancellation must interrupt it rather than wait for the timer.
func TestFinalizedOptimisticRetryCancellationCleanlyStops(t *testing.T) {
	feed := newFakeBlockFeed(t, 0)
	src := newFakeSource(t)
	a := newFakeArchive(t, "all")
	feed.present(0, [32]byte{}, nil)
	feed.optimisticHeader(0, 1<<20)

	ix := newAnchoredIndexerRuntime(t, feed, a, 1, 1, nil, time.Hour, metrics.New(), src)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- ix.Run(ctx) }()

	deadline := time.After(2 * time.Second)
	for feed.headerRequestCount(0) == 0 {
		select {
		case err := <-done:
			t.Fatalf("Run returned before cancellation: %v", err)
		case <-deadline:
			t.Fatal("optimistic header was not read")
		case <-time.After(time.Millisecond):
		}
	}
	// The response is immediate; let Run enter its cancelable outer backoff.
	time.Sleep(5 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run cancellation = %v, want nil", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run did not interrupt the optimistic retry backoff on cancellation")
	}
}

// TestConcurrencyDoesNotChangeWhatIsRecorded is case 10 (and spec 10.1's
// determinism): the serial and concurrent anchored walks write the archive
// byte-for-byte the same, given the same block feed and sources.
func TestConcurrencyDoesNotChangeWhatIsRecorded(t *testing.T) {
	// A varied chain: blobless slots, multi-blob slots, and a genuine missed slot.
	counts := map[uint64]int{0: 1, 1: 0, 2: 2, 4: 3, 5: 1, 7: 1, 8: 2, 9: 0, 11: 1}
	const finalized = 11
	const missed = 6 // no block; continuity proves it

	run := func(concurrency int) []string {
		feed := newFakeBlockFeed(t, finalized)
		src := newFakeSource(t)
		a := newFakeArchive(t, "all")

		var prev [32]byte
		for slot := uint64(0); slot <= finalized; slot++ {
			if slot == missed {
				continue
			}
			var blobs [][]byte
			if n := counts[slot]; n > 0 {
				blobs = slotBlobs(slot, n)
				src.serve(slot, blobs)
			}
			prev = feed.present(slot, prev, blobs)
		}

		ix := newAnchoredIndexer(t, feed, a, 4, concurrency, nil, src)
		if err := drain(t, ix); err != nil {
			t.Fatalf("drain (concurrency %d): %v", concurrency, err)
		}
		return a.recordedWrites()
	}

	serial := run(1)
	concurrent := run(6)
	if len(serial) == 0 {
		t.Fatal("the serial run recorded nothing")
	}
	if !slices.Equal(serial, concurrent) {
		t.Fatalf("concurrency changed what was recorded\nserial (%d):\n%s\nconcurrent (%d):\n%s",
			len(serial), strings.Join(serial, "\n"), len(concurrent), strings.Join(concurrent, "\n"))
	}
}

// TestMidBatchSourceFailureFailsTheBatch is the error taxonomy under concurrency:
// a slot no source can serve fails the whole batch, the first such slot by slot
// order is named, and nothing is posted.
func TestMidBatchSourceFailureFailsTheBatch(t *testing.T) {
	feed := newFakeBlockFeed(t, 7)
	src := newFakeSource(t)
	a := newFakeArchive(t, "all")

	var prev [32]byte
	for slot := uint64(0); slot <= 7; slot++ {
		blobs := slotBlobs(slot, 1)
		prev = feed.present(slot, prev, blobs)
		src.serve(slot, blobs)
	}
	// Slots 2 and 3 fail at the source; slot 2 is earlier and must be reported.
	src.status[2] = http.StatusInternalServerError
	src.status[3] = http.StatusInternalServerError

	ix := newAnchoredIndexer(t, feed, a, 4, 6, nil, src)
	_, err := ix.Step(t.Context())
	if err == nil {
		t.Fatal("a batch with an unservable slot was accepted")
	}
	if !strings.Contains(err.Error(), "slot 2") {
		t.Errorf("error does not name the first unservable slot by order: %v", err)
	}
	if strings.Contains(err.Error(), "slot 3") {
		t.Errorf("error names a later slot instead of the first: %v", err)
	}
	if w := a.recordedWrites(); len(w) != 0 {
		t.Errorf("a failed batch wrote to the archive: %v", w)
	}
}

// TestPrefetchDoesNotOutrunThePost is the one-batch bound in anchored mode: while
// a batch's refs post is held, the batch after it may be fetched, but the one
// after that must not be.
func TestPrefetchDoesNotOutrunThePost(t *testing.T) {
	feed := newFakeBlockFeed(t, 11)
	src := newFakeSource(t)
	a := newFakeArchive(t, "all")
	a.refsGate = make(chan struct{})

	var prev [32]byte
	for slot := uint64(0); slot <= 11; slot++ {
		blobs := slotBlobs(slot, 1)
		prev = feed.present(slot, prev, blobs)
		src.serve(slot, blobs)
	}

	ix := newAnchoredIndexer(t, feed, a, 4, 6, nil, src)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- ix.Run(ctx) }()

	// Batch 0's refs post is blocked; batch 1 ([4,7]) is the one batch of
	// lookahead and gets fetched.
	src.waitRequested(4, 5, 6, 7)
	// Batch 2 ([8,11]) must not have started: it requires consuming batch 1, which
	// requires batch 0's post to return, which is blocked.
	if src.wasRequested(8) {
		t.Fatal("the pipeline fetched two batches ahead of the blocked post")
	}

	close(a.refsGate)
	a.waitSyncedTo(11)
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
}

// TestPostFailureTearsDownTheLookahead is the teardown guarantee: when a batch's
// post fails while its successor is still being fetched, the fetch is cancelled
// and joined, and Run returns the post's error rather than hanging or leaking.
func TestPostFailureTearsDownTheLookahead(t *testing.T) {
	feed := newFakeBlockFeed(t, 11)
	src := newFakeSource(t)
	a := newFakeArchive(t, "all")
	a.failRefs = true // every refs post fails, so batch 0's post fails

	var prev [32]byte
	for slot := uint64(0); slot <= 11; slot++ {
		blobs := slotBlobs(slot, 1)
		prev = feed.present(slot, prev, blobs)
		src.serve(slot, blobs)
	}
	// Batch 1's slot 4 blocks forever unless its fetch context is cancelled.
	src.blockCtx[4] = true

	ct := &countingTransport{base: http.DefaultTransport}
	ix := newAnchoredIndexer(t, feed, a, 4, 6, &http.Client{Transport: ct}, src)

	done := make(chan error, 1)
	go func() { done <- ix.Run(t.Context()) }()

	err := <-done
	if err == nil {
		t.Fatal("a failing refs post did not fail the run")
	}
	if !strings.Contains(err.Error(), "posting refs") {
		t.Errorf("Run returned an unexpected error: %v", err)
	}
	if n := ct.inFlight.Load(); n != 0 {
		t.Errorf("%d requests still in flight after the run returned; the lookahead leaked", n)
	}
}

// countingTransport counts requests currently in flight, so a test can show that
// no fetch outlives the run that started it.
type countingTransport struct {
	base     http.RoundTripper
	inFlight atomic.Int64
}

func (t *countingTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	t.inFlight.Add(1)
	defer t.inFlight.Add(-1)
	return t.base.RoundTrip(r)
}

// lastRefs returns the most recent refs body the archive recorded.
func lastRefs(t *testing.T, a *fakeArchive) string {
	t.Helper()
	for _, w := range slices.Backward(a.recordedWrites()) {
		if r, ok := strings.CutPrefix(w, "refs:"); ok {
			return r
		}
	}
	t.Fatal("the archive recorded no refs post")
	return ""
}

// hexBlobs renders blobs the way the beacon read API states them.
func hexBlobs(blobs [][]byte) []string {
	out := make([]string, 0, len(blobs))
	for _, b := range blobs {
		out = append(out, "0x"+hex.EncodeToString(b))
	}
	return out
}

// writeJSON renders v as a response.
func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func scrapeMetrics(t *testing.T, m *metrics.Metrics) string {
	t.Helper()
	rec := httptest.NewRecorder()
	metrics.Handler(m, nil).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("metrics status = %d, want 200", rec.Code)
	}
	return rec.Body.String()
}
