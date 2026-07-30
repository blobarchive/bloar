package server_test

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"

	"github.com/blobarchive/bloar/archive"
	"github.com/blobarchive/bloar/catalog"
	"github.com/blobarchive/bloar/ingest"
	"github.com/blobarchive/bloar/schema"
	"github.com/blobarchive/bloar/server"
	"github.com/blobarchive/bloar/store"
)

const mutableHead = "unfinalized"

type generationFixture struct {
	t       *testing.T
	dir     string
	st      *store.Store
	cat     *catalog.Catalog
	roots   *server.RootStore
	heads   *server.Heads
	key     ed25519.PrivateKey
	archive archive.Config

	replaceCalls atomic.Int64
}

func newGenerationFixture(t *testing.T, dir string, key ed25519.PrivateKey, states server.GenerationStates, revisions server.PublicationRevisions) *generationFixture {
	t.Helper()
	if dir == "" {
		dir = t.TempDir()
	}
	st, err := store.Open(dir, store.WithPebbleLogger(quietPebble{}))
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	if key == nil {
		_, key, err = ed25519.GenerateKey(nil)
		if err != nil {
			t.Fatalf("ed25519.GenerateKey: %v", err)
		}
	}
	f := &generationFixture{t: t, dir: dir, st: st, cat: catalog.New(st.KV()), key: key}
	f.roots = server.NewRootStore(st.KV())
	f.archive = archive.Config{Blocks: st.Blocks(), Resolver: f.cat}
	if states == nil {
		states = f.roots.GenerationStore()
	}
	if revisions == nil {
		revisions = f.roots.PublicationStore()
	}
	f.heads, err = server.NewHeads(server.HeadsConfig{
		Net: testNet, Roots: f.roots, Generations: states, Publications: revisions,
		Policies: map[string]server.HeadPolicy{mutableHead: {
			Kind: server.UnfinalizedMutable, HandoffHead: testHead, MaxWindowSlots: 8,
		}},
		GenerationArchive: f.archive, SigningKey: key,
		Replacements: map[string]func(*archive.Head){testHead: func(*archive.Head) {}, mutableHead: func(*archive.Head) {
			f.replaceCalls.Add(1)
		}},
	})
	if err != nil {
		st.Close()
		t.Fatalf("server.NewHeads: %v", err)
	}

	all, err := server.OpenHead(t.Context(), f.archive, f.roots,
		archive.Params{Name: testHead, Net: testNet, OriginSlot: testOrigin, SegBits: testSegBits, FanoutBits: testFanout})
	if err != nil {
		st.Close()
		t.Fatalf("OpenHead(all): %v", err)
	}
	if err := f.heads.Add(all); err != nil {
		st.Close()
		t.Fatalf("Heads.Add(all): %v", err)
	}
	if _, err := f.heads.ApplyRefs(t.Context(), testHead, nil, 10, cid.Undef); err != nil {
		st.Close()
		t.Fatalf("ApplyRefs(all): %v", err)
	}
	tip, err := server.OpenMutableHead(t.Context(), f.archive, f.roots,
		archive.Params{Name: mutableHead, Net: testNet, OriginSlot: testOrigin, SegBits: testSegBits, FanoutBits: testFanout})
	if err != nil {
		st.Close()
		t.Fatalf("OpenMutableHead: %v", err)
	}
	if err := f.heads.Add(tip); err != nil {
		st.Close()
		t.Fatalf("Heads.Add(unfinalized): %v", err)
	}
	return f
}

func (f *generationFixture) close() { f.st.Close() }

func (f *generationFixture) addBlob(seed byte) string {
	f.t.Helper()
	var vh schema.VersionedHash
	vh[0], vh[len(vh)-1] = 1, seed
	blk := blocks.NewBlock([]byte{seed, seed + 1, seed + 2})
	if err := f.st.Blocks().Put(f.t.Context(), blk); err != nil {
		f.t.Fatalf("Blocks.Put: %v", err)
	}
	if err := f.cat.Put(f.t.Context(), vh, blk.Cid()); err != nil {
		f.t.Fatalf("Catalog.Put: %v", err)
	}
	return "0x" + hex.EncodeToString(vh[:])
}

func generationReq(expected, start, end uint64, rows []server.GenerationRow) server.GenerationRequest {
	return server.GenerationRequest{
		ExpectedGeneration: expected, WindowStart: start, SyncedTo: end, Rows: rows,
		SourceHeadRoot: "0x" + fmt.Sprintf("%064x", end+100), SourceFinalizedSlot: 10,
		SourceFinalizedRoot:     "0x" + fmt.Sprintf("%064x", uint64(99)),
		ObservedHandoffRoot:     "bafyreigy2q5oybpz2yhgs2d7zyd6y3jbcpfrck7qwr3tpfuaqaovs26tku",
		ObservedHandoffSyncedTo: 10,
	}
}

func decodeDoc(t *testing.T, heads *server.Heads) server.Doc {
	t.Helper()
	var doc server.Doc
	if err := json.Unmarshal(heads.Doc(), &doc); err != nil {
		t.Fatalf("decode publication: %v", err)
	}
	return doc
}

func generationReqAtCurrentHandoff(t *testing.T, heads *server.Heads, req server.GenerationRequest) server.GenerationRequest {
	t.Helper()
	doc := decodeDoc(t, heads)
	for _, entry := range doc.Heads {
		if entry.Name == testHead {
			if entry.SyncedTo == nil {
				t.Fatalf("handoff head %q is uncovered", testHead)
			}
			req.ObservedHandoffRoot = entry.Root
			req.ObservedHandoffSyncedTo = *entry.SyncedTo
			return req
		}
	}
	t.Fatalf("handoff head %q is absent from publication", testHead)
	return req
}

func mutablePublicationEntry(t *testing.T, finalized, mutable *archive.Head, sourceFinalized uint64) server.HeadEntry {
	t.Helper()
	handoff := finalized.Info()
	if handoff.SyncedTo == nil {
		t.Fatal("finalized handoff is uncovered")
	}
	info := mutable.Info()
	if info.SyncedTo == nil {
		t.Fatal("mutable generation is uncovered")
	}
	windowStart := info.OriginSlot
	syncedTo := *info.SyncedTo
	handoffSyncedTo := *handoff.SyncedTo
	sourceFinalizedSlot := sourceFinalized
	return server.HeadEntry{
		Name: info.Name, Root: info.Root.String(), OriginSlot: info.OriginSlot, SyncedTo: &syncedTo,
		SegBits: info.SegBits, FanoutBits: info.FanoutBits, DirDepth: info.DirDepth,
		Kind: server.UnfinalizedMutable, WindowStart: &windowStart,
		SourceHeadRoot:      "0x" + fmt.Sprintf("%064x", syncedTo+100),
		SourceFinalizedSlot: &sourceFinalizedSlot,
		SourceFinalizedRoot: "0x" + fmt.Sprintf("%064x", uint64(99)),
		HandoffHead:         handoff.Name, HandoffRoot: handoff.Root.String(), HandoffSyncedTo: &handoffSyncedTo,
	}
}

func generationHTTPServer(t *testing.T, f *generationFixture) *httptest.Server {
	t.Helper()
	ingester, err := ingest.New(ingest.Config{Blocks: f.st.Blocks(), Catalog: f.cat})
	if err != nil {
		t.Fatal(err)
	}
	handler, err := server.New(server.Config{
		Heads: f.heads, Blocks: f.st.Blocks(), Ingester: ingester, AuthToken: testToken,
		Beacon: server.Beacon{SecondsPerSlot: 12},
	})
	if err != nil {
		t.Fatal(err)
	}
	return httptest.NewServer(handler)
}

func TestMutableGenerationLifecycleCASAndPublication(t *testing.T) {
	f := newGenerationFixture(t, "", nil, nil, nil)
	defer f.close()

	state, err := f.heads.GenerationState(t.Context(), mutableHead)
	if err != nil {
		t.Fatal(err)
	}
	if state.Kind != server.UnfinalizedMutable || state.Generation != 0 {
		t.Fatalf("initial state = %#v, want mutable generation zero", state)
	}
	if _, ok := f.heads.Get(mutableHead); ok {
		t.Fatal("unbuilt mutable head is publicly servable")
	}
	if _, ok := f.heads.HeadDoc(mutableHead); ok {
		t.Fatal("unbuilt mutable head is in the publication")
	}
	if doc := decodeDoc(t, f.heads); doc.Revision != nil {
		t.Fatalf("pre-activation document revision = %v, want omitted", *doc.Revision)
	}

	vh1 := f.addBlob(1)
	req1 := generationReq(0, 10, 12, []server.GenerationRow{{Slot: 11, VersionedHashes: []string{vh1}}})
	res1, err := f.heads.ReplaceGeneration(t.Context(), mutableHead, req1)
	if err != nil {
		t.Fatalf("ReplaceGeneration(gen1): %v", err)
	}
	if res1.Generation != 1 || res1.NoOp || res1.WindowStart != 10 || res1.SyncedTo != 12 {
		t.Fatalf("gen1 response = %#v", res1)
	}
	if got := f.replaceCalls.Load(); got != 1 {
		t.Fatalf("OnReplace calls = %d, want 1", got)
	}
	root, ok, err := f.roots.Get(t.Context(), mutableHead)
	if err != nil || !ok || root.String() != res1.Root {
		t.Fatalf("root mirror = %s, ok=%t, err=%v; want %s", root, ok, err, res1.Root)
	}
	state, err = f.heads.GenerationState(t.Context(), mutableHead)
	if err != nil {
		t.Fatal(err)
	}
	if state.Root != res1.Root || state.HandoffHead != testHead || state.HandoffRoot == "" || state.HandoffSyncedTo != 10 || state.SourceHeadSlot != 12 {
		t.Fatalf("durable gen1 state = %#v", state)
	}
	if _, ok := f.heads.Get(mutableHead); !ok {
		t.Fatal("selected mutable generation is not served")
	}
	doc1 := decodeDoc(t, f.heads)
	if doc1.Revision == nil || *doc1.Revision != 1 {
		t.Fatalf("first mutable document revision = %v, want 1", doc1.Revision)
	}
	if err := doc1.Verify(); err != nil {
		t.Fatalf("revisioned document signature: %v", err)
	}
	entry := doc1.Heads[1]
	if entry.Name != mutableHead {
		entry = doc1.Heads[0]
	}
	if entry.Kind != server.UnfinalizedMutable || entry.WindowStart == nil || *entry.WindowStart != 10 {
		t.Fatalf("mutable publication entry = %#v", entry)
	}

	// The immediately preceding exact request is a no-op and does not allocate a
	// publication revision or retarget the reconciler again.
	retry, err := f.heads.ReplaceGeneration(t.Context(), mutableHead, req1)
	if err != nil {
		t.Fatalf("exact retry: %v", err)
	}
	if !retry.NoOp || retry.Generation != 1 {
		t.Fatalf("exact retry response = %#v", retry)
	}
	if got := f.replaceCalls.Load(); got != 1 {
		t.Fatalf("OnReplace calls after exact retry = %d, want 1", got)
	}
	if doc := decodeDoc(t, f.heads); doc.Revision == nil || *doc.Revision != 1 {
		t.Fatalf("exact retry moved publication revision to %v", doc.Revision)
	}

	different := req1
	different.SourceHeadRoot = "0x" + fmt.Sprintf("%064x", 999)
	if _, err := f.heads.ReplaceGeneration(t.Context(), mutableHead, different); err == nil {
		t.Fatal("same expected_generation with different request was accepted")
	} else {
		var conflict *server.GenerationConflictError
		if !errors.As(err, &conflict) || conflict.CurrentGeneration != 1 {
			t.Fatalf("different retry error = %T %v", err, err)
		}
	}

	// A real reorg replacement can move origin and rows while preserving the
	// immutable name/net/tree parameters.
	vh2 := f.addBlob(2)
	req2 := generationReq(1, 11, 13, []server.GenerationRow{{Slot: 12, VersionedHashes: []string{vh2}}})
	res2, err := f.heads.ReplaceGeneration(t.Context(), mutableHead, req2)
	if err != nil {
		t.Fatalf("ReplaceGeneration(gen2): %v", err)
	}
	if res2.Generation != 2 || res2.Root == res1.Root {
		t.Fatalf("gen2 response = %#v; gen1 root %s", res2, res1.Root)
	}
	if got := decodeDoc(t, f.heads).Revision; got == nil || *got != 2 {
		t.Fatalf("second mutable document revision = %v, want 2", got)
	}
	h2, ok := f.heads.Get(mutableHead)
	if !ok || h2.Params().OriginSlot != 11 {
		t.Fatalf("gen2 served origin = %v, ok=%t", h2, ok)
	}
}

type failOnceGenerations struct {
	server.GenerationStates
	fail atomic.Bool
}

func (f *failOnceGenerations) Commit(ctx context.Context, name string, expected uint64, root cid.Cid, next server.GenerationState) error {
	if f.fail.Swap(false) {
		return errors.New("injected generation commit failure")
	}
	return f.GenerationStates.Commit(ctx, name, expected, root, next)
}

type failOnceRevisions struct {
	server.PublicationRevisions
	fail atomic.Bool
}

func (f *failOnceRevisions) Next(ctx context.Context, signer ed25519.PublicKey) (uint64, error) {
	if f.fail.Swap(false) {
		return 0, errors.New("injected publication allocation failure")
	}
	return f.PublicationRevisions.Next(ctx, signer)
}

func TestMutableGenerationCrashRecoveryBoundaries(t *testing.T) {
	t.Run("atomic commit failure changes neither selector", func(t *testing.T) {
		f := newGenerationFixture(t, "", nil, nil, nil)
		defer f.close()
		wrapped := &failOnceGenerations{GenerationStates: f.roots.GenerationStore()}
		f.heads = rebuildGenerationRegistry(t, f, wrapped, f.roots.PublicationStore())
		before, _, _ := f.roots.Get(t.Context(), mutableHead)
		wrapped.fail.Store(true)
		req := generationReq(0, 10, 12, nil)
		if _, err := f.heads.ReplaceGeneration(t.Context(), mutableHead, req); err == nil {
			t.Fatal("injected generation commit failure was accepted")
		}
		after, _, _ := f.roots.Get(t.Context(), mutableHead)
		state, _ := f.heads.GenerationState(t.Context(), mutableHead)
		if !after.Equals(before) || state.Generation != 0 || f.replaceCalls.Load() != 0 {
			t.Fatalf("failed commit changed root/state/hook: before=%s after=%s state=%#v hooks=%d", before, after, state, f.replaceCalls.Load())
		}
		if res, err := f.heads.ReplaceGeneration(t.Context(), mutableHead, req); err != nil || res.Generation != 1 {
			t.Fatalf("retry after clean commit failure = %#v, %v", res, err)
		}
	})

	t.Run("publication allocation failure precedes selector commit", func(t *testing.T) {
		f := newGenerationFixture(t, "", nil, nil, nil)
		defer f.close()
		revisions := &failOnceRevisions{PublicationRevisions: f.roots.PublicationStore()}
		f.heads = rebuildGenerationRegistry(t, f, f.roots.GenerationStore(), revisions)
		revisions.fail.Store(true)
		req := generationReq(0, 10, 12, nil)
		if _, err := f.heads.ReplaceGeneration(t.Context(), mutableHead, req); err == nil {
			t.Fatal("injected publication allocation failure was accepted")
		}
		state, stateErr := f.heads.GenerationState(t.Context(), mutableHead)
		served, ok := f.heads.Get(mutableHead)
		if stateErr != nil || state.Generation != 0 || ok || served != nil {
			t.Fatalf("post-publication-failure state/serve = %#v, err=%v ok=%t root=%v", state, stateErr, ok, served)
		}
		res, err := f.heads.ReplaceGeneration(t.Context(), mutableHead, req)
		if err != nil || res.NoOp || res.Generation != 1 {
			t.Fatalf("retry did not perform the never-committed generation: %#v, %v", res, err)
		}
		if f.replaceCalls.Load() != 1 {
			t.Fatalf("already-exposed retry called OnReplace again: %d", f.replaceCalls.Load())
		}
	})

	t.Run("same-root successor still heals its own publication generation", func(t *testing.T) {
		f := newGenerationFixture(t, "", nil, nil, nil)
		defer f.close()
		first := generationReq(0, 10, 12, nil)
		res1, err := f.heads.ReplaceGeneration(t.Context(), mutableHead, first)
		if err != nil {
			t.Fatal(err)
		}
		revisions := &failOnceRevisions{PublicationRevisions: f.roots.PublicationStore()}
		f.heads = rebuildGenerationRegistry(t, f, f.roots.GenerationStore(), revisions)
		second := generationReq(1, 10, 12, nil)
		second.SourceHeadRoot = "0x" + fmt.Sprintf("%064x", uint64(999))
		revisions.fail.Store(true)
		if _, err := f.heads.ReplaceGeneration(t.Context(), mutableHead, second); err == nil {
			t.Fatal("injected publication failure was accepted")
		}
		state, err := f.heads.GenerationStatus(t.Context(), mutableHead)
		if err != nil {
			t.Fatal(err)
		}
		if state.Generation != 1 || !state.Exposed || !state.Published || state.Root != res1.Root {
			t.Fatalf("post-failure same-root status = %#v", state)
		}
		healed, err := f.heads.ReplaceGeneration(t.Context(), mutableHead, second)
		if err != nil || healed.NoOp || healed.Generation != 2 {
			t.Fatalf("retry failed to commit same-root successor: %#v, %v", healed, err)
		}
		state, err = f.heads.GenerationStatus(t.Context(), mutableHead)
		if err != nil || !state.Exposed || !state.Published {
			t.Fatalf("healed status = %#v, %v", state, err)
		}
	})
}

// rebuildGenerationRegistry reconstructs a registry over f's already-open
// engines so a test can inject persistence seams after fixture bootstrapping.
func rebuildGenerationRegistry(t *testing.T, f *generationFixture, states server.GenerationStates, revisions server.PublicationRevisions) *server.Heads {
	t.Helper()
	h, err := server.NewHeads(server.HeadsConfig{
		Net: testNet, Roots: f.roots, Generations: states, Publications: revisions,
		Policies:          map[string]server.HeadPolicy{mutableHead: {Kind: server.UnfinalizedMutable, HandoffHead: testHead, MaxWindowSlots: 8}},
		GenerationArchive: f.archive, SigningKey: f.key,
		Replacements: map[string]func(*archive.Head){mutableHead: func(*archive.Head) {
			f.replaceCalls.Add(1)
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	all, _ := f.heads.Get(testHead)
	if err := h.Add(all); err != nil {
		t.Fatal(err)
	}
	// The public Get intentionally hides generation zero. Reopen the bootstrap
	// engine from its durable root instead.
	opened, err := server.OpenMutableHead(t.Context(), f.archive, f.roots,
		archive.Params{Name: mutableHead, Net: testNet, OriginSlot: testOrigin, SegBits: testSegBits, FanoutBits: testFanout})
	if err != nil {
		t.Fatal(err)
	}
	if err := h.Add(opened); err != nil {
		t.Fatal(err)
	}
	return h
}

func TestMutableGenerationHTTPValidationAndConflictShape(t *testing.T) {
	f := newGenerationFixture(t, "", nil, nil, nil)
	defer f.close()
	httpd := generationHTTPServer(t, f)
	defer httpd.Close()

	for _, path := range []string{
		"/" + mutableHead + "/eth/v1/beacon/blobs/10",
		"/" + mutableHead + "/eth/v1/beacon/genesis",
		"/" + mutableHead + "/eth/v1/config/spec",
	} {
		resp, err := http.Get(httpd.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusServiceUnavailable || resp.Header.Get("Cache-Control") != "no-store" ||
			resp.Header.Get("Retry-After") == "" {
			t.Fatalf("generation-zero GET %s = %d cache=%q retry=%q, want retryable 503",
				path, resp.StatusCode, resp.Header.Get("Cache-Control"), resp.Header.Get("Retry-After"))
		}
	}

	missing := generationReq(0, 10, 12, []server.GenerationRow{{Slot: 11, VersionedHashes: []string{"0x" + fmt.Sprintf("%064x", 7)}}})
	resp := postGeneration(t, httpd.URL, missing)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("missing blob status = %d, want 409", resp.StatusCode)
	}
	var conflict server.GenerationErrorResponse
	decode(t, resp, &conflict)
	if conflict.CurrentGeneration == nil || *conflict.CurrentGeneration != 0 || len(conflict.MissingBlobs) != 1 {
		t.Fatalf("missing blob conflict = %#v", conflict)
	}

	badRoot := generationReq(0, 10, 12, nil)
	badRoot.SourceHeadRoot = fmt.Sprintf("%064x", 1) // missing required 0x prefix
	resp = postGeneration(t, httpd.URL, badRoot)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("malformed beacon root status = %d, want 400", resp.StatusCode)
	}

	body := bytes.NewBufferString(`{"expected_generation":0,"window_start":10,"synced_to":12,"source_head_root":"0x` + fmt.Sprintf("%064x", 1) + `","source_finalized_slot":10,"source_finalized_root":"0x` + fmt.Sprintf("%064x", 2) + `"}`)
	req, _ := http.NewRequest(http.MethodPost, httpd.URL+"/bloar/v1/heads/"+mutableHead+"/generation", body)
	req.Header.Set("Authorization", "Bearer "+testToken)
	var err error
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("missing rows status = %d, want 400", resp.StatusCode)
	}

	resp, err = http.Get(httpd.URL + "/bloar/v1/heads/" + mutableHead + "/generation")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK || resp.Header.Get("Cache-Control") != "no-store" {
		t.Fatalf("GET generation = %d cache=%q", resp.StatusCode, resp.Header.Get("Cache-Control"))
	}
}

func TestMutablePhysicalBlobAnswersAreNeverCached(t *testing.T) {
	f := newGenerationFixture(t, "", nil, nil, nil)
	defer f.close()
	vh := f.addBlob(1)
	if _, err := f.heads.ReplaceGeneration(t.Context(), mutableHead,
		generationReq(0, 10, 12, []server.GenerationRow{{Slot: 11, VersionedHashes: []string{vh}}})); err != nil {
		t.Fatal(err)
	}
	httpd := generationHTTPServer(t, f)
	defer httpd.Close()
	for _, tc := range []struct {
		path   string
		status int
	}{
		{"9", http.StatusNotFound},
		{"10?versioned_hashes=0x" + fmt.Sprintf("%064x", 99), http.StatusNotFound},
		{"11", http.StatusOK},
	} {
		resp, err := http.Get(fmt.Sprintf("%s/%s/eth/v1/beacon/blobs/%s", httpd.URL, mutableHead, tc.path))
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != tc.status || resp.Header.Get("Cache-Control") != "no-store" {
			t.Errorf("request %s = status %d cache %q, want %d/no-store", tc.path, resp.StatusCode,
				resp.Header.Get("Cache-Control"), tc.status)
		}
	}
}

func TestRevisionModeRestoresFromSignerFloorWithoutMutablePolicy(t *testing.T) {
	st, err := store.Open(t.TempDir(), store.WithPebbleLogger(quietPebble{}))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	_, key, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	publications := server.NewPublicationStore(st.KV())
	if revision, err := publications.Next(t.Context(), key.Public().(ed25519.PublicKey)); err != nil || revision != 1 {
		t.Fatalf("priming publication floor = %d, %v", revision, err)
	}
	heads, err := server.NewHeads(server.HeadsConfig{
		Net: testNet, Roots: server.NewRootStore(st.KV()), Publications: publications, SigningKey: key,
	})
	if err != nil {
		t.Fatal(err)
	}
	doc := decodeDoc(t, heads)
	if doc.Revision == nil || *doc.Revision != 2 {
		t.Fatalf("restored revision = %v, want 2", doc.Revision)
	}
}

func postGeneration(t *testing.T, base string, request server.GenerationRequest) *http.Response {
	t.Helper()
	body, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest(http.MethodPost, base+"/bloar/v1/heads/"+mutableHead+"/generation", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+testToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func TestHeadKindBaselineRestartAndCounterOverflow(t *testing.T) {
	t.Run("legacy root cannot become mutable", func(t *testing.T) {
		st, err := store.Open(t.TempDir(), store.WithPebbleLogger(quietPebble{}))
		if err != nil {
			t.Fatal(err)
		}
		defer st.Close()
		roots := server.NewRootStore(st.KV())
		cfg := archive.Config{Blocks: st.Blocks(), Resolver: catalog.New(st.KV())}
		params := archive.Params{Name: "legacy", Net: testNet, OriginSlot: 8, SegBits: 3, FanoutBits: 2}
		h, err := archive.New(t.Context(), cfg, params)
		if err != nil {
			t.Fatal(err)
		}
		// Simulate a pre-feature root with no 'g' baseline.
		if err := roots.Put(t.Context(), params.Name, h.Root()); err != nil {
			t.Fatal(err)
		}
		if _, err := server.OpenMutableHead(t.Context(), cfg, roots, params); err == nil {
			t.Fatal("legacy root was reinterpreted as mutable")
		} else {
			var mismatch *server.KindMismatchError
			if !errors.As(err, &mismatch) || mismatch.Got != server.FinalizedMonotonic {
				t.Fatalf("reinterpretation error = %T %v", err, err)
			}
		}
	})

	t.Run("mutable moving origin resumes", func(t *testing.T) {
		dir := t.TempDir()
		f := newGenerationFixture(t, dir, nil, nil, nil)
		key := append(ed25519.PrivateKey(nil), f.key...)
		res, err := f.heads.ReplaceGeneration(t.Context(), mutableHead, generationReq(0, 10, 12, nil))
		if err != nil {
			t.Fatal(err)
		}
		f.close()
		f2 := newGenerationFixture(t, dir, key, nil, nil)
		defer f2.close()
		h, ok := f2.heads.Get(mutableHead)
		if !ok || h.Root().String() != res.Root || h.Params().OriginSlot != 10 {
			t.Fatalf("resumed mutable head = ok=%t root=%v params=%v", ok, h, h.Params())
		}
		doc := decodeDoc(t, f2.heads)
		if doc.Revision == nil || *doc.Revision <= 1 {
			t.Fatalf("restart did not continue signer revision floor: %v", doc.Revision)
		}
	})

	t.Run("selected mutable generation rejects incompatible policy changes", func(t *testing.T) {
		dir := t.TempDir()
		f := newGenerationFixture(t, dir, nil, nil, nil)
		key := append(ed25519.PrivateKey(nil), f.key...)
		if _, err := f.heads.ReplaceGeneration(t.Context(), mutableHead, generationReq(0, 10, 12, nil)); err != nil {
			t.Fatal(err)
		}
		f.close()

		for _, tc := range []struct {
			name   string
			policy server.HeadPolicy
			match  string
		}{
			{"handoff", server.HeadPolicy{Kind: server.UnfinalizedMutable, HandoffHead: "other", MaxWindowSlots: 8}, "selected against handoff"},
			{"window", server.HeadPolicy{Kind: server.UnfinalizedMutable, HandoffHead: testHead, MaxWindowSlots: 2}, "above current max_window_slots"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				st, err := store.Open(dir, store.WithPebbleLogger(quietPebble{}))
				if err != nil {
					t.Fatal(err)
				}
				defer st.Close()
				roots := server.NewRootStore(st.KV())
				archiveCfg := archive.Config{Blocks: st.Blocks(), Resolver: catalog.New(st.KV())}
				heads, err := server.NewHeads(server.HeadsConfig{
					Net: testNet, Roots: roots, Generations: roots.GenerationStore(), Publications: roots.PublicationStore(),
					Policies: map[string]server.HeadPolicy{mutableHead: tc.policy}, GenerationArchive: archiveCfg, SigningKey: key,
					Replacements: map[string]func(*archive.Head){mutableHead: func(*archive.Head) {}},
				})
				if err != nil {
					t.Fatal(err)
				}
				tip, err := server.OpenMutableHead(t.Context(), archiveCfg, roots,
					archive.Params{Name: mutableHead, Net: testNet, OriginSlot: testOrigin, SegBits: testSegBits, FanoutBits: testFanout})
				if err != nil {
					t.Fatal(err)
				}
				if err := heads.Add(tip); err == nil || !strings.Contains(err.Error(), tc.match) {
					t.Fatalf("Heads.Add after %s policy change = %v, want error containing %q", tc.name, err, tc.match)
				}
			})
		}
	})

	t.Run("publication revision overflow fails closed", func(t *testing.T) {
		st, err := store.Open(t.TempDir(), store.WithPebbleLogger(quietPebble{}))
		if err != nil {
			t.Fatal(err)
		}
		defer st.Close()
		pub, _, _ := ed25519.GenerateKey(nil)
		key := append([]byte{'r'}, pub...)
		var max [8]byte
		binary.BigEndian.PutUint64(max[:], math.MaxUint64)
		if err := st.KV().Set(key, max[:], pebble.Sync); err != nil {
			t.Fatal(err)
		}
		if _, err := server.NewPublicationStore(st.KV()).Next(t.Context(), pub); !errors.Is(err, server.ErrPublicationRevisionOverflow) {
			t.Fatalf("Next at max = %v", err)
		}
	})
}
