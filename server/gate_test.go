package server_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/ipfs/boxo/blockstore"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"

	"github.com/blobarchive/bloar/archive"
	"github.com/blobarchive/bloar/catalog"
	"github.com/blobarchive/bloar/ingest"
	"github.com/blobarchive/bloar/pinning"
	"github.com/blobarchive/bloar/schema"
	"github.com/blobarchive/bloar/server"
	"github.com/blobarchive/bloar/store"
)

// This file is the regression test for the mutation/GC exclusion fix.
//
// The exclusion of spec 9 used to be an HTTP middleware in cmd/bloard, which
// meant it was a property of arriving as a POST rather than of writing to the
// archive: every stack that was not that daemon -- the conformance suite's, a
// follower's embedded registry, anything importing server directly -- mutated
// while GC swept. So the stack below is deliberately built WITHOUT cmd/bloard,
// out of the exported API and nothing else, and the exclusion still has to hold.

// gateStack is a writer: store, catalog, ledger, reconciler, GC, head registry
// and ingester, wired the way an embedder would wire them.
type gateStack struct {
	t       *testing.T
	ctx     context.Context
	store   *store.Store
	blocks  *hookedBlockstore
	heads   *server.Heads
	head    *archive.Head
	ing     *ingest.Ingester
	rec     *pinning.Reconciler
	gc      *pinning.GC
	catalog *catalog.Catalog
}

func newGateStack(t *testing.T) *gateStack {
	t.Helper()
	ctx := t.Context()

	s := &gateStack{t: t, ctx: ctx}
	st, err := store.Open(t.TempDir(), store.WithPebbleLogger(quietPebble{}))
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() {
		if err := st.Close(); err != nil {
			t.Errorf("closing store: %v", err)
		}
	})
	s.store = st
	s.blocks = &hookedBlockstore{Blockstore: st.Blocks()}
	s.catalog = catalog.New(st.KV())

	s.rec, err = pinning.NewReconciler(pinning.Config{Ledger: catalog.NewLedger(st.KV())})
	if err != nil {
		t.Fatalf("pinning.NewReconciler: %v", err)
	}
	// GC sweeps the real blockstore, not the hooked one: the hook exists to
	// pause a mutation mid-write, and GC is the thing that must not be running
	// while one is paused.
	s.gc, err = pinning.NewGC(pinning.GCConfig{Blocks: st.Blocks(), Reconciler: s.rec})
	if err != nil {
		t.Fatalf("pinning.NewGC: %v", err)
	}

	roots := server.NewRootStore(st.KV())
	s.heads, err = server.NewHeads(server.HeadsConfig{
		Net:   testNet,
		Roots: roots,
		// The whole point: the gate is configured on the registry, not wrapped
		// around a handler somewhere upstream.
		Gate:   s.rec.Gate(),
		OnRoot: func(name string, _ cid.Cid) { s.rec.Notify(name) },
		Replacements: map[string]func(*archive.Head){testHead: func(head *archive.Head) {
			if err := s.rec.Replace(head); err != nil {
				panic(err)
			}
		}},
	})
	if err != nil {
		t.Fatalf("server.NewHeads: %v", err)
	}

	s.head, err = server.OpenHead(ctx, archive.Config{Blocks: s.blocks, Resolver: s.catalog}, roots,
		archive.Params{Name: testHead, Net: testNet, OriginSlot: testOrigin, SegBits: testSegBits, FanoutBits: testFanout})
	if err != nil {
		t.Fatalf("server.OpenHead: %v", err)
	}
	if err := s.rec.Add(s.head, pinning.Full()); err != nil {
		t.Fatalf("Reconciler.Add: %v", err)
	}
	if err := s.heads.Add(s.head); err != nil {
		t.Fatalf("Heads.Add: %v", err)
	}

	s.ing, err = ingest.New(ingest.Config{Blocks: st.Blocks(), Catalog: s.catalog, Gate: s.rec.Gate()})
	if err != nil {
		t.Fatalf("ingest.New: %v", err)
	}
	return s
}

// put ingests one blob and returns the row that names it.
func (s *gateStack) put(seed uint64, slot uint64) archive.RefRow {
	s.t.Helper()
	res, err := s.ing.PutBlobs(s.ctx, makeBlob(seed))
	if err != nil {
		s.t.Fatalf("PutBlobs: %v", err)
	}
	return archive.RefRow{Slot: slot, VHs: []schema.VersionedHash{res[0].VH}}
}

// hookedBlockstore lets a test pause a mutation between two of its block
// writes. That is the moment the gate exists for: the engine writes bottom-up
// and swaps the root last (spec 5), so the blocks written so far are durable,
// unpinned, and reachable from nothing.
type hookedBlockstore struct {
	blockstore.Blockstore
	mu   sync.Mutex
	hook func(cid.Cid)
}

func (b *hookedBlockstore) setHook(f func(cid.Cid)) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.hook = f
}

func (b *hookedBlockstore) Put(ctx context.Context, blk blocks.Block) error {
	if err := b.Blockstore.Put(ctx, blk); err != nil {
		return err
	}
	b.mu.Lock()
	hook := b.hook
	b.mu.Unlock()
	if hook != nil {
		hook(blk.Cid())
	}
	return nil
}

// TestGCCannotInterleaveAMutationWithoutTheMiddleware is the regression itself.
//
// A mutation is paused after its first block write. A GC is started. Before
// the mutation/GC exclusion fix that GC ran: it reconciled from the previous root, marked its pins,
// and swept the blocks the paused mutation had just written -- so the root the
// mutation then published pointed at blocks that were gone. Now it blocks on the
// gate until the mutation finishes.
//
// The test asserts both halves. The timing half (GC does not return while the
// mutation is paused) is what says the exclusion is there at all; the data half
// (the mutation's blocks survive) is what says it was worth having.
func TestGCCannotInterleaveAMutationWithoutTheMiddleware(t *testing.T) {
	s := newGateStack(t)

	// A first batch, reconciled, so the ledger has a root to mark from and GC
	// has something to do.
	first := s.put(1, testOrigin+1)
	if _, err := s.heads.ApplyRefs(s.ctx, testHead, []archive.RefRow{first}, testOrigin+1, cid.Undef); err != nil {
		t.Fatalf("first ApplyRefs: %v", err)
	}
	if _, err := s.rec.ReconcileAll(s.ctx); err != nil {
		t.Fatalf("ReconcileAll: %v", err)
	}

	// The second batch is paused after its first block write.
	second := s.put(2, testOrigin+2)

	paused := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	var written []cid.Cid
	var writtenMu sync.Mutex
	s.blocks.setHook(func(c cid.Cid) {
		writtenMu.Lock()
		written = append(written, c)
		writtenMu.Unlock()
		once.Do(func() {
			close(paused)
			<-release
		})
	})

	applyDone := make(chan error, 1)
	go func() {
		_, err := s.heads.ApplyRefs(s.ctx, testHead, []archive.RefRow{second}, testOrigin+2, cid.Undef)
		applyDone <- err
	}()

	<-paused // the mutation is inside the gate, with unpinned blocks on disk.

	gcDone := make(chan error, 1)
	go func() {
		_, err := s.gc.Run(s.ctx)
		gcDone <- err
	}()

	// GC must not get in. It is waiting on the gate the mutation holds, and
	// there is no other way to observe "did not happen" than to give it a
	// chance to and check that it did not.
	select {
	case err := <-gcDone:
		t.Fatalf("gc.Run completed (err=%v) while a mutation was in flight; spec 9 requires exclusion, and this "+
			"stack has no cmd/bloard middleware to provide it", err)
	case <-time.After(200 * time.Millisecond):
	}

	close(release)
	if err := <-applyDone; err != nil {
		t.Fatalf("ApplyRefs: %v", err)
	}
	if err := <-gcDone; err != nil {
		t.Fatalf("gc.Run: %v", err)
	}

	// And the data half: every block the paused mutation wrote is still there.
	// This is what the old middleware-shaped exclusion failed to protect for any
	// caller that was not cmd/bloard.
	writtenMu.Lock()
	defer writtenMu.Unlock()
	for _, c := range written {
		has, err := s.store.Blocks().Has(s.ctx, c)
		if err != nil {
			t.Fatalf("Has(%s): %v", c, err)
		}
		if !has {
			t.Errorf("block %s, written by the mutation GC was excluded from, was swept anyway", c)
		}
	}
}

// TestTruncateHoldsTheGate covers the other mutation path of spec 5. It rewrites
// the spine and swaps a root exactly as ApplyRefs does, and its blocks are
// unpinned until reconciliation in exactly the same way.
func TestTruncateHoldsTheGate(t *testing.T) {
	s := newGateStack(t)
	gate := &countingGate{}

	// A registry whose gate is one this test can count, over the same head.
	heads, err := server.NewHeads(server.HeadsConfig{
		Net:          testNet,
		Roots:        server.NewRootStore(s.store.KV()),
		Gate:         gate,
		Replacements: map[string]func(*archive.Head){testHead: func(*archive.Head) {}},
	})
	if err != nil {
		t.Fatalf("server.NewHeads: %v", err)
	}
	if err := heads.Add(s.head); err != nil {
		t.Fatalf("Heads.Add: %v", err)
	}

	row := s.put(1, testOrigin+1)
	if _, err := heads.ApplyRefs(s.ctx, testHead, []archive.RefRow{row}, testOrigin+1, cid.Undef); err != nil {
		t.Fatalf("ApplyRefs: %v", err)
	}
	if got := gate.count(); got != 1 {
		t.Errorf("gate entered %d times during ApplyRefs, want 1", got)
	}

	if _, err := heads.Truncate(s.ctx, testHead, testOrigin); err != nil {
		t.Fatalf("Truncate: %v", err)
	}
	if got := gate.count(); got != 2 {
		t.Errorf("gate entered %d times after Truncate, want 2: Truncate swaps a root and its blocks are "+
			"unpinned until reconciliation, exactly like ApplyRefs", got)
	}
	if !gate.balanced() {
		t.Error("the gate was not left; a GC would wait on it forever")
	}
}

// TestMutationsWithoutAGateStillWork checks that the Gate field is optional in
// the sense the doc claims: a stack with no GC in it does not have to configure
// one, and does not panic for want of it.
func TestMutationsWithoutAGateStillWork(t *testing.T) {
	s := newGateStack(t)
	heads, err := server.NewHeads(server.HeadsConfig{
		Net:   testNet,
		Roots: server.NewRootStore(s.store.KV()),
		// No Gate.
	})
	if err != nil {
		t.Fatalf("server.NewHeads: %v", err)
	}
	if err := heads.Add(s.head); err != nil {
		t.Fatalf("Heads.Add: %v", err)
	}
	row := s.put(1, testOrigin+1)
	if _, err := heads.ApplyRefs(s.ctx, testHead, []archive.RefRow{row}, testOrigin+1, cid.Undef); err != nil {
		t.Fatalf("ApplyRefs with no gate configured: %v", err)
	}
}

// countingGate records Enter/Leave.
type countingGate struct {
	mu    sync.Mutex
	in    int
	depth int
}

func (g *countingGate) Enter() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.in++
	g.depth++
}

func (g *countingGate) Leave() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.depth--
}

func (g *countingGate) count() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.in
}

func (g *countingGate) balanced() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.depth == 0
}
