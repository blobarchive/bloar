package server_test

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/ipfs/go-cid"

	"github.com/blobarchive/bloar/archive"
	"github.com/blobarchive/bloar/catalog"
	"github.com/blobarchive/bloar/server"
	"github.com/blobarchive/bloar/store"
)

// faultyRoots wraps a RootStore so a test can make Put fail on demand. It is the
// seam commit's durability-before-announcement rule (the durability-before-announcement fix) turns on: Get
// always passes through, so OpenHead still resumes a head, while a Put made to
// fail stands in for a root write that never reached disk.
type faultyRoots struct {
	*server.RootStore
	fail atomic.Bool
}

// errRootPut is the injected failure. It is a plain error, not one of the
// mutation sentinels, so writeApplyError maps it to 500 exactly as a real Pebble
// write failure would.
var errRootPut = errors.New("server_test: injected root-store failure")

func (f *faultyRoots) Put(ctx context.Context, name string, root cid.Cid) error {
	if f.fail.Load() {
		return errRootPut
	}
	return f.RootStore.Put(ctx, name, root)
}

// TestRestart is the head lifecycle of the head-lifecycle regression: a head's root outlives the
// process. The engine hands a root back per mutation and keeps nothing on disk
// that says which one is current, so without the root store a restart would
// come up empty and re-serve archived ground as 503.
//
// The reopen goes through newStack, so it is the same OpenHead path cmd/bloard
// uses, not a test-only shortcut.
func TestRestart(t *testing.T) {
	dir := t.TempDir()
	blobs := [][]byte{makeBlob(1), makeBlob(2)}

	first := newStack(t, stackOpts{dir: dir})
	vhs := first.put(blobs...)
	first.refs([]map[string]any{row(9, vhs[0]), row(11, vhs[1])}, 12)
	wantRoot := first.headEntry().Root
	first.Close()

	second := newStack(t, stackOpts{dir: dir})

	// Same root: the head resumed rather than started over. A fresh head would
	// have the empty root here, which is the failure this is really about --
	// it is not a crash, it is silent data loss.
	if got := second.headEntry().Root; got != wantRoot {
		t.Errorf("root after restart = %s, want %s", got, wantRoot)
	}
	if got := second.syncedTo(); got == nil || *got != 12 {
		t.Errorf("synced_to after restart = %v, want 12", got)
	}

	// And it still serves: the blocks, the catalog and the index all came back.
	if got := second.getBlobs(9, vhs[0]); len(got) != 1 || got[0] != blobHex(blobs[0]) {
		t.Error("slot 9 is not served after a restart")
	}
	if got := second.getBlobs(11, vhs[1]); len(got) != 1 || got[0] != blobHex(blobs[1]) {
		t.Error("slot 11 is not served after a restart")
	}

	// Coverage continues from where it stopped rather than restarting: the new
	// batch extends the head the old process built.
	more := second.put(makeBlob(3))
	second.refs([]map[string]any{row(13, more[0])}, 14)
	if got := second.syncedTo(); got == nil || *got != 14 {
		t.Errorf("synced_to after a post-restart batch = %v, want 14", got)
	}
	if got := second.headEntry().Root; got == wantRoot {
		t.Error("the root did not change after a post-restart batch")
	}
}

// TestRestartEmptyHead covers the other resume: a head created but never
// applied to. Its root is persisted at creation, so a restart finds it and
// loads it rather than writing a second identical Head block.
func TestRestartEmptyHead(t *testing.T) {
	dir := t.TempDir()

	first := newStack(t, stackOpts{dir: dir})
	wantRoot := first.headEntry().Root
	first.Close()

	second := newStack(t, stackOpts{dir: dir})
	if got := second.headEntry().Root; got != wantRoot {
		t.Errorf("root after restart = %s, want %s", got, wantRoot)
	}
	if got := second.syncedTo(); got != nil {
		t.Errorf("synced_to = %d after restarting an empty head, want null", *got)
	}
}

// TestParamsMismatch covers the fatal case: spec 3.1 makes origin_slot,
// seg_bits and fanout_bits immutable for the life of a head, so a config that
// changes one must not open the head.
//
// Loading it anyway would be the worst outcome available. The parameters are
// arithmetic -- they decide which window a slot lands in and where the
// directory addresses it -- so a head read under the wrong ones does not fail,
// it answers wrong: 404s for blobs that are there, and a root that diverges
// from what the same data would build.
func TestParamsMismatch(t *testing.T) {
	dir := t.TempDir()
	first := newStack(t, stackOpts{dir: dir})
	vhs := first.put(makeBlob(1))
	first.refs([]map[string]any{row(9, vhs[0])}, 12)
	first.Close()

	base := archive.Params{Name: testHead, Net: testNet, OriginSlot: testOrigin, SegBits: testSegBits, FanoutBits: testFanout}
	mutate := map[string]func(archive.Params) archive.Params{
		"origin_slot": func(p archive.Params) archive.Params { p.OriginSlot = testOrigin + 8; return p },
		"seg_bits":    func(p archive.Params) archive.Params { p.SegBits = testSegBits + 1; return p },
		"fanout_bits": func(p archive.Params) archive.Params { p.FanoutBits = testFanout + 1; return p },
		"net":         func(p archive.Params) archive.Params { p.Net = "othernet"; return p },
	}

	for name, mutate := range mutate {
		t.Run(name, func(t *testing.T) {
			s, err := store.Open(dir, store.WithPebbleLogger(quietPebble{}))
			if err != nil {
				t.Fatalf("store.Open: %v", err)
			}
			defer s.Close()

			roots := server.NewRootStore(s.KV())
			cfg := archive.Config{Blocks: s.Blocks(), Resolver: catalog.New(s.KV())}

			_, err = server.OpenHead(t.Context(), cfg, roots, mutate(base))
			var mismatch *server.ParamsMismatchError
			if !errors.As(err, &mismatch) {
				t.Fatalf("OpenHead with a changed %s: err = %v, want *server.ParamsMismatchError", name, err)
			}
			// The operator reading this at 3am needs to be told what to do, not
			// merely that two structs differ.
			if !strings.Contains(mismatch.Error(), "immutable") || !strings.Contains(mismatch.Error(), "new head") {
				t.Errorf("the error does not explain the way out: %v", mismatch)
			}

			// Unchanged parameters still open, from the same store, after the
			// refusal: the refusal is a refusal, not a corruption.
			if _, err := server.OpenHead(t.Context(), cfg, roots, base); err != nil {
				t.Errorf("OpenHead with the original params: %v", err)
			}
		})
	}
}

// TestHeadsRegistry covers the registry's own rules, which the HTTP layer
// depends on but cannot state.
func TestHeadsRegistry(t *testing.T) {
	s := newStack(t, stackOpts{})

	t.Run("names", func(t *testing.T) {
		if got := s.heads.Names(); len(got) != 1 || got[0] != testHead {
			t.Errorf("Names() = %v, want [%s]", got, testHead)
		}
	})

	t.Run("duplicate", func(t *testing.T) {
		head, ok := s.heads.Get(testHead)
		if !ok {
			t.Fatal("the test head is not registered")
		}
		if err := s.heads.Add(head); err == nil {
			t.Error("registering a head twice was accepted")
		}
	})

	t.Run("unknown head mutations", func(t *testing.T) {
		// The registry, not the router, is what makes this a 404: the HTTP
		// layer checks first, but the check must hold underneath it too.
		_, err := s.heads.ApplyRefs(t.Context(), "nope", nil, 12, cid.Undef)
		if !errors.Is(err, server.ErrUnknownHead) {
			t.Errorf("ApplyRefs on an unknown head: err = %v, want ErrUnknownHead", err)
		}
		_, err = s.heads.Truncate(t.Context(), "nope", 12)
		if !errors.Is(err, server.ErrUnknownHead) {
			t.Errorf("Truncate on an unknown head: err = %v, want ErrUnknownHead", err)
		}
	})

	t.Run("wrong net", func(t *testing.T) {
		s2, err := store.Open(t.TempDir(), store.WithPebbleLogger(quietPebble{}))
		if err != nil {
			t.Fatalf("store.Open: %v", err)
		}
		defer s2.Close()

		roots := server.NewRootStore(s2.KV())
		head, err := server.OpenHead(t.Context(), archive.Config{Blocks: s2.Blocks(), Resolver: catalog.New(s2.KV())}, roots,
			archive.Params{Name: "other", Net: "othernet", OriginSlot: 0, SegBits: 3, FanoutBits: 2})
		if err != nil {
			t.Fatalf("OpenHead: %v", err)
		}
		// The document names one net for every head in it, so a head on
		// another one cannot be published from here.
		if err := s.heads.Add(head); err == nil {
			t.Error("a head on a different net was registered")
		}
	})
}

// TestHeadsAddRequiresDurableRoot covers Add's the safety boundary precondition: a head may
// be registered only when the RootStore already holds exactly its root. The
// daemon's OpenHead persists the root before Add, but Add is exported, and an
// embedded caller could hand it a freshly built or mutated head the store does
// not back. Without the check, rebuild would render and sign that head from its
// volatile root -- the cross-head half of the safety boundary, reached without any rebuild.
func TestHeadsAddRequiresDurableRoot(t *testing.T) {
	ctx := t.Context()

	openStore := func() *store.Store {
		st, err := store.Open(t.TempDir(), store.WithPebbleLogger(quietPebble{}))
		if err != nil {
			t.Fatalf("store.Open: %v", err)
		}
		t.Cleanup(func() { st.Close() })
		return st
	}
	newRegistry := func(roots server.Roots) *server.Heads {
		heads, err := server.NewHeads(server.HeadsConfig{Net: testNet, Roots: roots})
		if err != nil {
			t.Fatalf("NewHeads: %v", err)
		}
		return heads
	}
	// Built with archive.New, not OpenHead, so the root block exists but the
	// RootStore has never held it -- the test controls what the store holds.
	makeHead := func(st *store.Store, name string, origin uint64) *archive.Head {
		head, err := archive.New(ctx, archive.Config{Blocks: st.Blocks()},
			archive.Params{Name: name, Net: testNet, OriginSlot: origin, SegBits: 3, FanoutBits: 2})
		if err != nil {
			t.Fatalf("archive.New(%s): %v", name, err)
		}
		return head
	}

	t.Run("absent", func(t *testing.T) {
		st := openStore()
		heads := newRegistry(server.NewRootStore(st.KV()))
		head := makeHead(st, "absent", 0)
		if err := heads.Add(head); err == nil {
			t.Fatal("Add with no persisted root was accepted")
		}
		if got := heads.Names(); len(got) != 0 {
			t.Errorf("head registered despite the refused Add: Names() = %v", got)
		}
		if _, ok := heads.HeadDoc("absent"); ok {
			t.Error("head published despite the refused Add")
		}
	})

	t.Run("mismatch", func(t *testing.T) {
		st := openStore()
		roots := server.NewRootStore(st.KV())
		heads := newRegistry(roots)
		head := makeHead(st, "mismatch", 0)
		other := makeHead(st, "other", 1)
		if head.Root().Equals(other.Root()) {
			t.Fatal("test setup: the two heads share a root")
		}
		// The store holds a defined but different root under the head's name.
		if err := roots.Put(ctx, "mismatch", other.Root()); err != nil {
			t.Fatalf("Put: %v", err)
		}
		if err := heads.Add(head); err == nil {
			t.Fatal("Add over a mismatched persisted root was accepted")
		}
		if got := heads.Names(); len(got) != 0 {
			t.Errorf("head registered despite the refused Add: Names() = %v", got)
		}
		if _, ok := heads.HeadDoc("mismatch"); ok {
			t.Error("head published despite the refused Add")
		}
	})

	t.Run("match", func(t *testing.T) {
		st := openStore()
		roots := server.NewRootStore(st.KV())
		heads := newRegistry(roots)
		head := makeHead(st, "match", 0)
		if err := roots.Put(ctx, "match", head.Root()); err != nil {
			t.Fatalf("Put: %v", err)
		}
		if err := heads.Add(head); err != nil {
			t.Fatalf("Add over the persisted root: %v", err)
		}
		if got := heads.Names(); len(got) != 1 || got[0] != "match" {
			t.Errorf("Names() = %v, want [match]", got)
		}
		if _, ok := heads.HeadDoc("match"); !ok {
			t.Error("registered head is not in the published document")
		}
	})
}

// TestRootStore covers the persistence layer directly, including the answer an
// unwritten head gets.
func TestRootStore(t *testing.T) {
	s, err := store.Open(t.TempDir(), store.WithPebbleLogger(quietPebble{}))
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	defer s.Close()

	roots := server.NewRootStore(s.KV())
	ctx := t.Context()

	if _, ok, err := roots.Get(ctx, "absent"); err != nil || ok {
		t.Errorf("Get on an unwritten head = (_, %t, %v), want (_, false, nil)", ok, err)
	}

	head, err := archive.New(ctx, archive.Config{Blocks: s.Blocks()},
		archive.Params{Name: "a", Net: testNet, OriginSlot: 0, SegBits: 3, FanoutBits: 2})
	if err != nil {
		t.Fatalf("archive.New: %v", err)
	}
	if err := roots.Put(ctx, "a", head.Root()); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, ok, err := roots.Get(ctx, "a")
	if err != nil || !ok {
		t.Fatalf("Get = (_, %t, %v)", ok, err)
	}
	if !got.Equals(head.Root()) {
		t.Errorf("Get = %s, want %s", got, head.Root())
	}

	// A head whose name is a prefix of another's is a different key: the
	// catalog's keyspace rule applies here too.
	if err := roots.Put(ctx, "ab", head.Root()); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if _, ok, _ := roots.Get(ctx, "a"); !ok {
		t.Error(`head "a" disappeared when head "ab" was written`)
	}

	if err := roots.Put(ctx, "", head.Root()); err == nil {
		t.Error("an empty head name was accepted")
	}
}

// TestCommitRootPersistFailure is the durability-before-announcement regression: a mutation whose
// root write fails must not announce or select that root. The candidate is built
// off-side, so until it is durable the serving engine and document keep naming
// the previous root, OnRoot and OnDoc must not fire, and the swap metric must not
// move -- otherwise a follower adopts a root the writer may never bring back and
// pins its no-regression floor above where the writer can go.
//
// It then covers the recovery the fix leaves intact: on restart the node loads
// the durable previous root, and the same batch replayed against a healed store
// lands and is announced exactly once.
func TestCommitRootPersistFailure(t *testing.T) {
	dir := t.TempDir()
	s := newStack(t, stackOpts{dir: dir, instrument: true})

	// A good batch first: it gives the head a durable root to fall back to and a
	// baseline for the observations a failed commit must not move.
	vhs := s.put(makeBlob(1), makeBlob(2))
	s.refs([]map[string]any{row(9, vhs[0])}, 10)

	baseRoot := s.headEntry().Root
	if baseRoot == "" {
		t.Fatal("the first batch published no root")
	}
	docsBefore, swapsBefore := s.docCount(), s.swapCount()
	metricBefore := s.rootSwapMetric(testHead)
	if metricBefore == 0 {
		t.Fatal("the first batch recorded no root swap")
	}

	// Arm the fault and apply a batch that would advance coverage. The off-side
	// candidate is complete before the root write fails, so the mutation is a 500
	// and neither serving nor publication may move to it.
	s.roots.fail.Store(true)
	resp := s.postJSON("/bloar/v1/heads/"+testHead+"/refs",
		map[string]any{"rows": []map[string]any{row(11, vhs[1])}, "synced_to": 12}, withAuth)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("refs with a failing root store: status = %d, want 500; body %s", resp.StatusCode, readAll(t, resp))
	}

	// The document still names the previous root, and OnDoc did not fire: no
	// rebuild happened.
	if got := s.headEntry().Root; got != baseRoot {
		t.Errorf("document root after a failed commit = %s, want the previous root %s", got, baseRoot)
	}
	if got := s.docCount() - docsBefore; got != 0 {
		t.Errorf("OnDoc fired %d times over a failed commit, want 0", got)
	}
	// OnRoot did not fire: the pin reconciler must not be pointed at a root that
	// is not on disk.
	if got := s.swapCount() - swapsBefore; got != 0 {
		t.Errorf("OnRoot fired %d times over a failed commit, want 0", got)
	}
	// And a failed commit does not look like a swap on a dashboard.
	if got := s.rootSwapMetric(testHead); got != metricBefore {
		t.Errorf("root-swap metric moved to %v over a failed commit, want %v", got, metricBefore)
	}

	// Recovery mirrors the real self-heal: the writer restarts, loads the durable
	// previous root (the failed root was never persisted), and the indexer
	// replays the same batch -- now against a healed store.
	s.Close()
	s2 := newStack(t, stackOpts{dir: dir, instrument: true})

	if got := s2.headEntry().Root; got != baseRoot {
		t.Fatalf("root after restart = %s, want the durable previous root %s", got, baseRoot)
	}
	docsBefore, swapsBefore = s2.docCount(), s2.swapCount()
	metricBefore = s2.rootSwapMetric(testHead)

	resp2 := s2.postJSON("/bloar/v1/heads/"+testHead+"/refs",
		map[string]any{"rows": []map[string]any{row(11, vhs[1])}, "synced_to": 12}, withAuth)
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("replaying the batch after recovery: status = %d, want 200; body %s", resp2.StatusCode, readAll(t, resp2))
	}
	var body refsBody
	decode(t, resp2, &body)
	if body.NoOp {
		t.Fatal("the replayed batch was a no-op; recovery must actually apply and persist it")
	}

	// The document now names the applied root, and it advanced from the fallback.
	if got := s2.headEntry().Root; got != body.Root {
		t.Errorf("document root after recovery = %s, want the applied root %s", got, body.Root)
	}
	if s2.headEntry().Root == baseRoot {
		t.Error("the document root did not advance after recovery")
	}
	// OnDoc and OnRoot each fired exactly once for the recovered root.
	if got := s2.docCount() - docsBefore; got != 1 {
		t.Errorf("OnDoc fired %d times over the recovery commit, want 1", got)
	}
	if got := s2.swapCount() - swapsBefore; got != 1 {
		t.Errorf("OnRoot fired %d times over the recovery commit, want 1", got)
	}
	if sw := s2.lastSwap(); sw.name != testHead || sw.root.String() != body.Root {
		t.Errorf("OnRoot handed (%s, %s), want (%s, %s)", sw.name, sw.root, testHead, body.Root)
	}
	if got := s2.rootSwapMetric(testHead) - metricBefore; got != 1 {
		t.Errorf("root-swap metric advanced by %v over the recovery commit, want 1", got)
	}
}

// TestCommitHealsOnReplay covers COW retry: a failed commit leaves the selected
// engine, reconciler target, and document on the old durable root. Replaying the
// same batch rebuilds the candidate from that root, then persists and publishes
// it exactly once.
func TestCommitHealsOnReplay(t *testing.T) {
	s := newStack(t, stackOpts{instrument: true})

	vhs := s.put(makeBlob(1), makeBlob(2))
	s.refs([]map[string]any{row(9, vhs[0])}, 10)
	baseRoot := s.headEntry().Root

	// Fail the commit of an advancing batch before selection or announcement.
	s.roots.fail.Store(true)
	batch := map[string]any{"rows": []map[string]any{row(11, vhs[1])}, "synced_to": 12}
	resp := s.postJSON("/bloar/v1/heads/"+testHead+"/refs", batch, withAuth)
	resp.Body.Close()
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("armed refs: status = %d, want 500", resp.StatusCode)
	}
	if got := s.headEntry().Root; got != baseRoot {
		t.Fatalf("document advanced to %s despite a failed commit, want %s", got, baseRoot)
	}
	if got := s.durableRoot(testHead); got != baseRoot {
		t.Fatalf("durable root advanced to %s despite a failed commit, want %s", got, baseRoot)
	}
	docsBefore, swapsBefore := s.docCount(), s.swapCount()
	metricBefore := s.rootSwapMetric(testHead)

	// Heal the store and replay the SAME batch in-process. COW starts again from
	// the still-selected durable root and performs the mutation normally.
	s.roots.fail.Store(false)
	resp2 := s.postJSON("/bloar/v1/heads/"+testHead+"/refs", batch, withAuth)
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("replaying the batch after healing: status = %d, want 200; body %s", resp2.StatusCode, readAll(t, resp2))
	}
	var body refsBody
	decode(t, resp2, &body)
	if body.NoOp {
		t.Error("the replay should not report noop; the failed candidate was never selected")
	}

	// The document names the healed root, and it is durable.
	healed := s.headEntry().Root
	if healed == baseRoot {
		t.Fatal("the document did not advance after the healing replay")
	}
	if body.Root != healed {
		t.Errorf("replay root = %s, document root = %s; they must agree", body.Root, healed)
	}
	if got := s.durableRoot(testHead); got != healed {
		t.Errorf("durable root = %s after healing, want %s", got, healed)
	}
	// Announced exactly once, for the healed root.
	if got := s.docCount() - docsBefore; got != 1 {
		t.Errorf("OnDoc fired %d times over the healing replay, want 1", got)
	}
	if got := s.swapCount() - swapsBefore; got != 1 {
		t.Errorf("OnRoot fired %d times over the healing replay, want 1", got)
	}
	if sw := s.lastSwap(); sw.name != testHead || sw.root.String() != healed {
		t.Errorf("OnRoot handed (%s, %s), want (%s, %s)", sw.name, sw.root, testHead, healed)
	}
	if got := s.rootSwapMetric(testHead) - metricBefore; got != 1 {
		t.Errorf("root-swap metric advanced by %v over the healing replay, want 1", got)
	}

	// A plain replay on the now-healthy head does not re-announce: the heal is a
	// one-time repair, not something every idempotent replay repeats.
	docsBefore, swapsBefore = s.docCount(), s.swapCount()
	metricBefore = s.rootSwapMetric(testHead)
	resp3 := s.postJSON("/bloar/v1/heads/"+testHead+"/refs", batch, withAuth)
	defer resp3.Body.Close()
	if resp3.StatusCode != http.StatusOK {
		t.Fatalf("second replay: status = %d, want 200", resp3.StatusCode)
	}
	if got := s.docCount() - docsBefore; got != 0 {
		t.Errorf("OnDoc fired %d times over a healthy replay, want 0", got)
	}
	if got := s.swapCount() - swapsBefore; got != 0 {
		t.Errorf("OnRoot fired %d times over a healthy replay, want 0", got)
	}
	if got := s.rootSwapMetric(testHead) - metricBefore; got != 0 {
		t.Errorf("root-swap metric moved by %v over a healthy replay, want 0", got)
	}
}

// TestCatchAll covers the JSON-everywhere rule of spec 7 on an unrouted path.
func TestCatchAll(t *testing.T) {
	s := newStack(t, stackOpts{})

	resp := s.get("/nothing/here")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
	errorOf(t, resp)
}
