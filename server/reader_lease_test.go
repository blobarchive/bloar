package server_test

import (
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/ipfs/boxo/blockstore"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/multiformats/go-multihash"

	"github.com/blobarchive/bloar/archive"
	"github.com/blobarchive/bloar/catalog"
	"github.com/blobarchive/bloar/ingest"
	"github.com/blobarchive/bloar/pinning"
	"github.com/blobarchive/bloar/schema"
	"github.com/blobarchive/bloar/server"
	"github.com/blobarchive/bloar/store"
)

// readBarrierBlockstore pauses one selected Get before it reaches the
// epoch-aware application blockstore. In the adversarial test the selected key
// is a blob in generation A: the index lookup has therefore already captured
// and walked A before the request stops here.
type readBarrierBlockstore struct {
	blockstore.Blockstore
	mu      sync.Mutex
	target  cid.Cid
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (b *readBarrierBlockstore) setTarget(c cid.Cid) {
	b.mu.Lock()
	b.target = c
	b.mu.Unlock()
}

func (b *readBarrierBlockstore) Get(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	b.mu.Lock()
	target := b.target
	b.mu.Unlock()
	if target.Defined() && c.Equals(target) {
		b.once.Do(func() { close(b.entered) })
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-b.release:
		}
	}
	return b.Blockstore.Get(ctx, c)
}

// blockingResponseWriter represents a client which stops reading immediately
// after the server commits its status. A correct reader lease is already gone
// when WriteHeader enters this wrapper, so the GC cut can proceed while the
// network write remains parked.
type blockingResponseWriter struct {
	*httptest.ResponseRecorder
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (w *blockingResponseWriter) WriteHeader(status int) {
	w.once.Do(func() { close(w.entered) })
	<-w.release
	w.ResponseRecorder.WriteHeader(status)
}

// TestHTTPReaderLeaseKeepsRetiredGenerationStableAcrossGCCut is the complete
// A -> B adversarial interleaving from the reader-lease hardening:
//
//  1. a request selects mutable generation A and pauses at A's blob;
//  2. generation B replaces A and reconciliation removes A's recursive pin;
//  3. online GC asks for T0 but cannot cut while A is being materialized;
//  4. the request reads and encodes A completely, then releases its lease;
//  5. T0 starts while the response body is deliberately stalled on the client.
//
// The final sweep removes A's now-unpinned blob. Its successful response proves
// the lease is a snapshot-stability mechanism, not accidental retention.
func TestHTTPReaderLeaseKeepsRetiredGenerationStableAcrossGCCut(t *testing.T) {
	ctx := t.Context()
	state, err := store.Open(t.TempDir(), store.WithPebbleLogger(quietPebble{}))
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()

	base := store.Validating(blockstore.NewBlockstore(dssync.MutexWrap(datastore.NewMapDatastore())))
	enumerationEntered := make(chan struct{})
	enumerationRelease := make(chan struct{})
	var enumerationOnce sync.Once
	epochs := store.NewBlockstoreEpochs(base, store.WithKeyIterator(func(ctx context.Context) (<-chan cid.Cid, <-chan error, error) {
		enumerationOnce.Do(func() { close(enumerationEntered) })
		select {
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		case <-enumerationRelease:
		}
		keys, err := base.AllKeysChan(ctx)
		if err != nil {
			return nil, nil, err
		}
		errs := make(chan error)
		close(errs)
		return keys, errs, nil
	}))
	barrier := &readBarrierBlockstore{
		Blockstore: epochs.Application(), entered: make(chan struct{}), release: make(chan struct{}),
	}
	cat := catalog.New(state.KV())
	ledger := catalog.NewLedger(state.KV())
	rec, err := pinning.NewReconciler(pinning.Config{Ledger: ledger})
	if err != nil {
		t.Fatal(err)
	}
	roots := server.NewRootStore(state.KV())
	archiveCfg := archive.Config{Blocks: barrier, Resolver: cat}

	finalized, err := server.OpenHead(ctx, archiveCfg, roots, archive.Params{
		Name: testHead, Net: testNet, OriginSlot: testOrigin, SegBits: testSegBits, FanoutBits: testFanout,
	})
	if err != nil {
		t.Fatal(err)
	}
	mutable, err := server.OpenMutableHead(ctx, archiveCfg, roots, archive.Params{
		Name: mutableHead, Net: testNet, OriginSlot: testOrigin, SegBits: testSegBits, FanoutBits: testFanout,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := rec.Add(finalized, pinning.Full()); err != nil {
		t.Fatal(err)
	}
	if err := rec.Add(mutable, pinning.Full()); err != nil {
		t.Fatal(err)
	}
	replaceMutable, err := rec.BindReplacement(mutableHead)
	if err != nil {
		t.Fatal(err)
	}
	replaceFinalized, err := rec.BindReplacement(testHead)
	if err != nil {
		t.Fatal(err)
	}
	_, signingKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	heads, err := server.NewHeads(server.HeadsConfig{
		Net: testNet, Roots: roots, Generations: roots.GenerationStore(), Publications: roots.PublicationStore(),
		Policies: map[string]server.HeadPolicy{mutableHead: {
			Kind: server.UnfinalizedMutable, HandoffHead: testHead, MaxWindowSlots: 8,
		}},
		GenerationArchive: archiveCfg, SigningKey: signingKey,
		Replacements: map[string]func(*archive.Head){testHead: replaceFinalized, mutableHead: replaceMutable},
		Gate:         rec.Gate(),
		OnRoot:       func(name string, _ cid.Cid) { rec.Notify(name) },
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := heads.Add(finalized); err != nil {
		t.Fatal(err)
	}
	if err := heads.Add(mutable); err != nil {
		t.Fatal(err)
	}
	if _, err := heads.ApplyRefs(ctx, testHead, nil, 10, cid.Undef); err != nil {
		t.Fatal(err)
	}

	putBlob := func(seed byte) (string, cid.Cid) {
		var vh schema.VersionedHash
		vh[0], vh[len(vh)-1] = 1, seed
		body := []byte{seed, seed + 1, seed + 2}
		hash, err := multihash.Sum(body, multihash.SHA2_256, -1)
		if err != nil {
			t.Fatal(err)
		}
		blk, err := blocks.NewBlockWithCid(body, cid.NewCidV1(cid.Raw, hash))
		if err != nil {
			t.Fatal(err)
		}
		if err := barrier.Put(ctx, blk); err != nil {
			t.Fatal(err)
		}
		if err := cat.Put(ctx, vh, blk.Cid()); err != nil {
			t.Fatal(err)
		}
		return "0x" + hex.EncodeToString(vh[:]), blk.Cid()
	}
	vhA, blobA := putBlob(1)
	genA, err := heads.ReplaceGeneration(ctx, mutableHead,
		generationReq(0, 10, 12, []server.GenerationRow{{Slot: 11, VersionedHashes: []string{vhA}}}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rec.ReconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	barrier.setTarget(blobA)

	ingester, err := ingest.New(ingest.Config{Blocks: barrier, Catalog: cat, Gate: rec.Gate()})
	if err != nil {
		t.Fatal(err)
	}
	handler, err := server.New(server.Config{
		Heads: heads, Blocks: barrier, Ingester: ingester, AuthToken: testToken,
		Beacon: server.Beacon{SecondsPerSlot: 12},
	})
	if err != nil {
		t.Fatal(err)
	}
	w := &blockingResponseWriter{
		ResponseRecorder: httptest.NewRecorder(), entered: make(chan struct{}), release: make(chan struct{}),
	}
	req := httptest.NewRequest(http.MethodGet, "/"+mutableHead+"/eth/v1/beacon/blobs/11", nil)
	requestDone := make(chan struct{})
	go func() {
		handler.ServeHTTP(w, req)
		close(requestDone)
	}()
	select {
	case <-barrier.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("generation A request did not reach its blob read")
	}

	// Replacement and reconciliation share the Gate read side and therefore may
	// complete while the older reader lease is active. This is intentional: only
	// the exclusive collection cut waits.
	vhB, _ := putBlob(2)
	genB, err := heads.ReplaceGeneration(ctx, mutableHead,
		generationReq(1, 11, 13, []server.GenerationRow{{Slot: 12, VersionedHashes: []string{vhB}}}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rec.ReconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	pins, err := ledger.List(ctx, mutableHead, pinning.PurposeRoot)
	if err != nil {
		t.Fatal(err)
	}
	if len(pins) != 1 || pins[0].CID.String() != genB.Root || pins[0].CID.String() == genA.Root {
		t.Fatalf("pins after A -> B = %#v; want only B root %s", pins, genB.Root)
	}

	gc, err := pinning.NewGC(pinning.GCConfig{Epochs: epochs, Reconciler: rec, SeparateScrub: true})
	if err != nil {
		t.Fatal(err)
	}
	gcDone := make(chan error, 1)
	go func() {
		_, err := gc.Run(context.Background())
		gcDone <- err
	}()

	// The iterator is called only after T0 has activated the epoch. It must not
	// be reachable while the request is paused on A's unmaterialized blob.
	select {
	case <-enumerationEntered:
		t.Fatal("online GC crossed T0 while a reader still materialized retired generation A")
	case <-time.After(150 * time.Millisecond):
	}

	close(barrier.release)
	select {
	case <-w.entered:
		// The old response is fully materialized; its first network write is now
		// blocked in the test writer.
	case <-time.After(5 * time.Second):
		t.Fatal("generation A response did not reach its response write")
	}
	select {
	case <-enumerationEntered:
		// Release-before-write is exact: T0 and the epoch started even though the
		// client is still refusing the response body.
	case err := <-gcDone:
		t.Fatalf("online GC returned before enumeration: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatalf("online GC remained behind the lease during a stalled response write (active epoch %d)", epochs.ActiveEpoch())
	}
	close(w.release)
	close(enumerationRelease)
	select {
	case <-requestDone:
	case <-time.After(5 * time.Second):
		t.Fatal("reader did not finish after response writer was released")
	}
	if w.Code != http.StatusOK || w.Body.String() != `{"data":["0x010203"]}` {
		t.Fatalf("generation A response = status %d body %q", w.Code, w.Body.String())
	}
	select {
	case err := <-gcDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("online GC did not finish")
	}
	if has, err := base.Has(ctx, blobA); err != nil || has {
		t.Fatalf("retired generation A blob retained after completed GC: has=%t err=%v", has, err)
	}
}

// probeGate and probeBlockstore make the endpoint audit independent of GC's
// implementation. Every block Get records whether the reader lease is held,
// and probeResponseWriter records whether it has been released before either
// response-writing method reaches the underlying writer.
type probeGate struct {
	mu       sync.Mutex
	depth    int
	enters   int
	maxDepth int
}

func (g *probeGate) Enter() {
	g.mu.Lock()
	g.depth++
	g.enters++
	if g.depth > g.maxDepth {
		g.maxDepth = g.depth
	}
	g.mu.Unlock()
}

func (g *probeGate) Leave() {
	g.mu.Lock()
	g.depth--
	g.mu.Unlock()
}

func (g *probeGate) held() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.depth > 0
}

func (g *probeGate) reset() {
	g.mu.Lock()
	g.enters = 0
	g.maxDepth = g.depth
	g.mu.Unlock()
}

func (g *probeGate) stats() (enters, maxDepth, depth int) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.enters, g.maxDepth, g.depth
}

type probeBlockstore struct {
	blockstore.Blockstore
	gate *probeGate

	mu          sync.Mutex
	gets        int
	allGetsHeld bool
	onGet       func()
}

func (b *probeBlockstore) reset() {
	b.mu.Lock()
	b.gets = 0
	b.allGetsHeld = true
	b.mu.Unlock()
}

func (b *probeBlockstore) setOnGet(f func()) {
	b.mu.Lock()
	b.onGet = f
	b.mu.Unlock()
}

func (b *probeBlockstore) Get(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	b.mu.Lock()
	b.gets++
	b.allGetsHeld = b.allGetsHeld && b.gate.held()
	onGet := b.onGet
	b.onGet = nil
	b.mu.Unlock()
	if onGet != nil {
		onGet()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return b.Blockstore.Get(ctx, c)
}

func (b *probeBlockstore) observations() (int, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.gets, b.allGetsHeld
}

type probeResponseWriter struct {
	*httptest.ResponseRecorder
	gate *probeGate

	mu                sync.Mutex
	writes            int
	allWritesUnleased bool
}

func newProbeResponseWriter(gate *probeGate) *probeResponseWriter {
	return &probeResponseWriter{
		ResponseRecorder: httptest.NewRecorder(), gate: gate, allWritesUnleased: true,
	}
}

func (w *probeResponseWriter) observeWrite() {
	w.mu.Lock()
	w.writes++
	w.allWritesUnleased = w.allWritesUnleased && !w.gate.held()
	w.mu.Unlock()
}

func (w *probeResponseWriter) WriteHeader(status int) {
	w.observeWrite()
	w.ResponseRecorder.WriteHeader(status)
}

func (w *probeResponseWriter) Write(p []byte) (int, error) {
	w.observeWrite()
	return w.ResponseRecorder.Write(p)
}

func (w *probeResponseWriter) observations() (int, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.writes, w.allWritesUnleased
}

type leaseProbeFixture struct {
	t       *testing.T
	state   *store.Store
	gate    *probeGate
	blocks  *probeBlockstore
	cat     *catalog.Catalog
	heads   *server.Heads
	head    *archive.Head
	handler http.Handler
}

func newLeaseProbeFixture(t *testing.T) *leaseProbeFixture {
	t.Helper()
	f := &leaseProbeFixture{t: t, gate: &probeGate{}}
	var err error
	f.state, err = store.Open(t.TempDir(), store.WithPebbleLogger(quietPebble{}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.state.Close() })
	f.blocks = &probeBlockstore{Blockstore: f.state.Blocks(), gate: f.gate, allGetsHeld: true}
	f.cat = catalog.New(f.state.KV())
	roots := server.NewRootStore(f.state.KV())
	f.heads, err = server.NewHeads(server.HeadsConfig{
		Net: testNet, Roots: roots, Manifests: server.NewManifestStore(f.state.KV()),
		Blocks: f.blocks, Gate: f.gate,
		Replacements: map[string]func(*archive.Head){testHead: func(next *archive.Head) { f.head = next }},
	})
	if err != nil {
		t.Fatal(err)
	}
	f.head, err = server.OpenHead(t.Context(), archive.Config{Blocks: f.blocks, Resolver: f.cat}, roots,
		archive.Params{Name: testHead, Net: testNet, OriginSlot: testOrigin, SegBits: testSegBits, FanoutBits: testFanout})
	if err != nil {
		t.Fatal(err)
	}
	if err := f.heads.Add(f.head); err != nil {
		t.Fatal(err)
	}
	ingester, err := ingest.New(ingest.Config{Blocks: f.blocks, Catalog: f.cat, Gate: f.gate})
	if err != nil {
		t.Fatal(err)
	}
	f.handler, err = server.New(server.Config{
		Heads: f.heads, Blocks: f.blocks, Ingester: ingester, AuthToken: testToken,
		Beacon: server.Beacon{SecondsPerSlot: 12},
	})
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func (f *leaseProbeFixture) serve(path string, ctx context.Context) (*probeResponseWriter, int, bool) {
	f.t.Helper()
	f.blocks.reset()
	f.gate.reset()
	w := newProbeResponseWriter(f.gate)
	req := httptest.NewRequest(http.MethodGet, path, nil).WithContext(ctx)
	f.handler.ServeHTTP(w, req)
	gets, held := f.blocks.observations()
	if f.gate.held() {
		f.t.Fatal("reader lease remained held after handler return")
	}
	writes, unleased := w.observations()
	if writes == 0 || !unleased {
		f.t.Fatalf("response writes = %d, all after lease release = %t", writes, unleased)
	}
	return w, gets, held
}

type probeFollowerBlobs struct {
	blocks blockstore.Blockstore
	gate   *probeGate

	mu       sync.Mutex
	calls    int
	allDepth int
}

func (b *probeFollowerBlobs) Blob(ctx context.Context, e schema.RefEntry) ([]byte, error) {
	_, _, depth := b.gate.stats()
	b.mu.Lock()
	b.calls++
	if b.calls == 1 {
		b.allDepth = depth
	} else if b.allDepth != depth {
		b.allDepth = -1
	}
	b.mu.Unlock()
	blk, err := b.blocks.Get(ctx, e.Blob)
	if err != nil {
		return nil, err
	}
	return blk.RawData(), nil
}

func (b *probeFollowerBlobs) observations() (calls, depth int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.calls, b.allDepth
}

// TestBlockMaterializingEndpointsUseReaderLease is the endpoint inventory for
// the reader-lease hardening. Blob reads cover both the archive index and raw leaves; manifest
// GET is the other public handler which follows a mutable selector to a block.
// The cancellation case exercises the deferred/error path rather than only a
// 200 response.
func TestBlockMaterializingEndpointsUseReaderLease(t *testing.T) {
	t.Run("blobs", func(t *testing.T) {
		f := newLeaseProbeFixture(t)
		ingester, err := ingest.New(ingest.Config{Blocks: f.blocks, Catalog: f.cat, Gate: f.gate})
		if err != nil {
			t.Fatal(err)
		}
		put, err := ingester.PutBlobs(t.Context(), makeBlob(901))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.heads.ApplyRefs(t.Context(), testHead, []archive.RefRow{{
			Slot: testOrigin, VHs: []schema.VersionedHash{put[0].VH},
		}}, testOrigin, cid.Undef); err != nil {
			t.Fatal(err)
		}
		w, gets, held := f.serve("/"+testHead+"/eth/v1/beacon/blobs/"+"8", t.Context())
		if w.Code != http.StatusOK || gets == 0 || !held {
			t.Fatalf("blobs response = status %d gets %d all-held %t", w.Code, gets, held)
		}
	})

	t.Run("manifest", func(t *testing.T) {
		f := newLeaseProbeFixture(t)
		manifest := &schema.Manifest{
			V: schema.ManifestVersion, Head: testHead,
			Sources: []schema.Source{{
				Type: schema.SourceInboxEvents, Address: make([]byte, schema.AddressSize),
				Topic: make([]byte, schema.TopicSize), OpenEnded: true,
			}},
		}
		body, tip, err := schema.EncodeManifest(manifest)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.heads.SetManifest(t.Context(), testHead, body, tip, cid.Undef, f.head.Root()); err != nil {
			t.Fatal(err)
		}
		w, gets, held := f.serve("/bloar/v1/heads/"+testHead+"/manifest", t.Context())
		if w.Code != http.StatusOK || gets != 1 || !held {
			t.Fatalf("manifest response = status %d gets %d all-held %t", w.Code, gets, held)
		}
	})

	t.Run("follower fetch seam does not reacquire gate", func(t *testing.T) {
		f := newLeaseProbeFixture(t)
		ingester, err := ingest.New(ingest.Config{Blocks: f.blocks, Catalog: f.cat, Gate: f.gate})
		if err != nil {
			t.Fatal(err)
		}
		put, err := ingester.PutBlobs(t.Context(), makeBlob(902))
		if err != nil {
			t.Fatal(err)
		}
		followed, err := archive.New(t.Context(), archive.Config{Blocks: f.blocks, Resolver: f.cat}, archive.Params{
			Name: "followed", Net: testNet, OriginSlot: testOrigin, SegBits: testSegBits, FanoutBits: testFanout,
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := followed.ApplyRefs(t.Context(), []archive.RefRow{{
			Slot: testOrigin, VHs: []schema.VersionedHash{put[0].VH},
		}}, testOrigin); err != nil {
			t.Fatal(err)
		}
		fetch := &probeFollowerBlobs{blocks: f.blocks, gate: f.gate}
		if err := f.heads.Adopt(t.Context(), followed, fetch, cid.Undef); err != nil {
			t.Fatal(err)
		}
		w, gets, held := f.serve("/followed/eth/v1/beacon/blobs/8", t.Context())
		calls, depth := fetch.observations()
		enters, maxDepth, finalDepth := f.gate.stats()
		if w.Code != http.StatusOK || gets == 0 || !held || calls != 1 || depth != 1 ||
			enters != 1 || maxDepth != 1 || finalDepth != 0 {
			t.Fatalf("followed response: status=%d gets=%d held=%t fetch_calls=%d fetch_depth=%d gate=%d/%d/%d",
				w.Code, gets, held, calls, depth, enters, maxDepth, finalDepth)
		}
	})

	t.Run("cancelled lookup releases", func(t *testing.T) {
		f := newLeaseProbeFixture(t)
		if _, err := f.heads.ApplyRefs(t.Context(), testHead, nil, testOrigin, cid.Undef); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		f.blocks.setOnGet(cancel)
		w, gets, held := f.serve("/"+testHead+"/eth/v1/beacon/blobs/"+"8", ctx)
		if w.Code != http.StatusInternalServerError || gets != 1 || !held {
			t.Fatalf("cancelled response = status %d gets %d all-held %t", w.Code, gets, held)
		}
	})
}
