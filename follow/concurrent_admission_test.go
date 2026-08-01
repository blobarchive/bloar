package follow_test

// These tests cover concurrency and admission races in the follower poll path.
//
//   - Replay floors (document updated_at and IPNS record sequence)
//     are re-validated under the transition lock, atomically with the writes they
//     gate, so an older poll that resolved against a stale floor cannot overwrite a
//     newer poll's checkpoint or lower a floor it raised.
//   - The fetch pass snapshots head state under f.mu and commits
//     completion CAS-style, so it neither races a concurrent transition nor stamps a
//     generation the head has already moved past.
//   - A document is admitted or refused as a whole: one inconsistent
//     head leaves every checkpoint and the global floor untouched.

import (
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"

	"github.com/blobarchive/bloar/archive"
	"github.com/blobarchive/bloar/catalog"
	"github.com/blobarchive/bloar/follow"
	"github.com/blobarchive/bloar/metrics"
	"github.com/blobarchive/bloar/pinning"
	"github.com/blobarchive/bloar/server"
	"github.com/blobarchive/bloar/store"
)

// gateOnce releases the first goroutine that reaches it to pause, and lets every
// later one pass straight through. It is how the concurrent-poll tests hold the poll
// that resolved against the old floors while a newer poll commits, deterministically
// and without sleeps.
type gateOnce struct {
	first   atomic.Bool
	entered chan struct{}
	release chan struct{}
}

func newGateOnce() *gateOnce {
	return &gateOnce{entered: make(chan struct{}), release: make(chan struct{})}
}

// pause is the hook body: the first caller signals it arrived and blocks until
// release is closed; the rest return at once.
func (g *gateOnce) pause() {
	if g.first.CompareAndSwap(false, true) {
		close(g.entered)
		<-g.release
	}
}

// auditSignedDocEntriesAt signs a multi-head publication document, for the two-head
// document-admission test.
func auditSignedDocEntriesAt(t *testing.T, key ed25519.PrivateKey, entries []server.HeadEntry, at time.Time) []byte {
	t.Helper()
	u := server.Unsigned{
		V:         server.DocVersion,
		Net:       testNet,
		UpdatedAt: at.UTC().Format(time.RFC3339),
		Heads:     entries,
	}
	canonical, err := u.Canonical()
	if err != nil {
		t.Fatalf("Canonical: %v", err)
	}
	doc := server.Doc{
		Unsigned:  u,
		Pubkey:    hex.EncodeToString(key.Public().(ed25519.PublicKey)),
		Signature: hex.EncodeToString(ed25519.Sign(key, canonical)),
	}
	body, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	return body
}

// TestConcurrentPollDocumentFreshness verifies that when two polls resolve
// against the same old floor, the newer commits its
// generation, and the older -- held at the after-resolve gate until it has -- is then
// refused under the transition lock rather than overwriting the newer per-head
// checkpoint. Deterministic via the gate; no sleeps.
//
// Without the under-lock re-check the older document, being floor-compatible per head
// (its synced_to clears the newer checkpoint's), would reach putCheckpoint and rewrite
// the checkpoint to its own older-authorized generation.
func TestConcurrentPollDocumentFreshness(t *testing.T) {
	ctx := t.Context()
	st := openAuditStore(t)
	_, infos := coveredHead(t, st, 120, 130)
	root120 := infos[0].Root
	_, key, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("ed25519.GenerateKey: %v", err)
	}
	mx := metrics.New()

	// The newer document: generation @120, dated T_new. The older: a DIFFERENT
	// generation @130 (floor-compatible, 130 >= 120) but dated T_old < T_new -- the
	// older-but-floor-compatible document.
	tNew := time.Unix(1_700_000_000, 0).UTC()
	tOld := tNew.Add(-time.Hour)
	docNew := auditSignedDocAt(t, key, auditEntry(infos[0]), tNew)
	docOld := auditSignedDocAt(t, key, auditEntry(infos[1]), tOld)

	var body []byte
	af := newAuditFollower(t, st, key, swappableClient(&body), mx)

	gate := newGateOnce()
	follow.SetAfterResolveHook(gate.pause)
	t.Cleanup(func() { follow.SetAfterResolveHook(nil) })

	// Poll #1 resolves the OLDER document (floor still unset, so it passes), then is
	// held at the after-resolve gate before it can take the transition lock.
	body = docOld
	var oldErr error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		oldErr = af.f.Poll(ctx)
	}()
	<-gate.entered

	// Poll #2 resolves and commits the NEWER document while #1 is held: checkpoint and
	// floor become the newer generation's.
	body = docNew
	if err := af.f.Poll(ctx); err != nil {
		t.Fatalf("newer Poll: %v", err)
	}
	if got := checkpointRoot(t, st); got != root120 {
		t.Fatalf("checkpoint after the newer poll = %s, want %s", got, root120)
	}

	// Release the older poll into the locked admission. It must be refused on the
	// re-checked freshness floor, leaving the newer checkpoint and floor intact.
	close(gate.release)
	wg.Wait()

	if oldErr == nil || !strings.Contains(oldErr.Error(), "before the accepted floor") {
		t.Errorf("older Poll err = %v, want the under-lock freshness refusal", oldErr)
	}
	if got := checkpointRoot(t, st); got != root120 {
		t.Errorf("checkpoint after the older poll = %s, want it unchanged at the newer %s", got, root120)
	}
	if _, syncedTo, _, _, ok, err := follow.ReadCheckpoint(st.KV(), testHead); err != nil || !ok || syncedTo != 120 {
		t.Errorf("checkpoint synced_to = %d (ok=%t, err=%v), want it unchanged at 120", syncedTo, ok, err)
	}
	if floor, ok, err := follow.ReadUpdatedAt(st.KV()); err != nil || !ok || !floor.Equal(tNew) {
		t.Errorf("freshness floor = %s (ok=%t, err=%v), want the newer %s", floor, ok, err, tNew)
	}
	if got := refusalCount(t, mx, metrics.RefusalUpdatedAtFloor); got != 1 {
		t.Errorf("bloar_follow_refusals_total{reason=%q} = %g, want 1", metrics.RefusalUpdatedAtFloor, got)
	}
}

// TestConcurrentPollIPNSSeq is the corresponding IPNS-sequence half:
// interleaved records seq 10 and seq 20 end with the replay floor at 20, and the
// stale seq-10 record's document -- crafted to clear the freshness floor so ONLY the
// sequence guard can stop it -- is not admitted. Deterministic via the gate.
//
// Without the under-lock sequence guard the seq-10 record's document, freshness-newer
// and floor-compatible per head, would be adopted despite the record being a replay
// relative to the floor seq 20 raised.
func TestConcurrentPollIPNSSeq(t *testing.T) {
	ctx := t.Context()
	w := newIPNSWriter(t)
	// Two durable generations in the writer: @120 (the record-20 document) and @130
	// (the record-10 document). Empty batches, so nothing but index blocks to move.
	w.applyRefs(nil, 120)
	info120 := w.head.Info()
	w.applyRefs(nil, 130)
	info130 := w.head.Info()

	// The two publication documents, signed by the writer's key. The seq-20 document is
	// dated EARLIER than the seq-10 one, so the freshness re-check cannot be what
	// refuses seq 10 -- only the sequence guard can.
	tA := time.Unix(1_700_000_000, 0).UTC()
	tB := tA.Add(time.Hour)
	docA := auditSignedDocAt(t, w.key, auditEntry(info120), tA) // record seq 20
	docB := auditSignedDocAt(t, w.key, auditEntry(info130), tB) // record seq 10
	cidA := storeDocBlock(t, w, docA)
	cidB := storeDocBlock(t, w, docB)

	mx := metrics.New()
	f := ipnsFollower(t, w, func(c *follow.Config) { c.Metrics = mx })

	gate := newGateOnce()
	follow.SetAfterResolveHook(gate.pause)
	t.Cleanup(func() { follow.SetAfterResolveHook(nil) })

	// Poll #1 resolves the STALE record (seq 10) while the floor is unset, then is held
	// at the after-resolve gate.
	w.forge(t, cidB, 10)
	var staleErr error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		staleErr = f.f.Poll(ctx)
	}()
	<-gate.entered

	// Poll #2 resolves and commits the newer record (seq 20): the replay floor rises to
	// 20 and the @120 generation is adopted.
	w.forge(t, cidA, 20)
	if err := f.f.Poll(ctx); err != nil {
		t.Fatalf("seq-20 Poll: %v", err)
	}
	if got := auditSyncedTo(t, f.heads); got != 120 {
		t.Fatalf("adopted synced_to after the seq-20 poll = %d, want 120", got)
	}

	// Release the stale poll into the locked admission: seq 10 is below the floor 20,
	// so its document is refused and the floor is not lowered.
	close(gate.release)
	wg.Wait()

	if staleErr == nil || !strings.Contains(staleErr.Error(), "below the accepted floor") {
		t.Errorf("stale seq-10 Poll err = %v, want the under-lock sequence refusal", staleErr)
	}
	if seq, ok, err := follow.ReadIPNSSeq(f.store.KV()); err != nil || !ok || seq != 20 {
		t.Errorf("IPNS replay floor = %d (ok=%t, err=%v), want the newer 20", seq, ok, err)
	}
	if got := auditSyncedTo(t, f.heads); got != 120 {
		t.Errorf("adopted synced_to after the stale poll = %d, want it unchanged at 120 (seq-10 doc not admitted)", got)
	}
	if got := refusalCount(t, mx, metrics.RefusalIPNSSeqFloor); got != 1 {
		t.Errorf("bloar_follow_refusals_total{reason=%q} = %g, want 1", metrics.RefusalIPNSSeqFloor, got)
	}
}

// TestIPNSSeqRisesWhenHTTPSWins is the
// HTTPS-wins half: an authenticated IPNS record raises the replay floor as a CHANNEL
// fact even when a fresher HTTPS document wins the freshness selection and is the one
// adopted. The floor is not coupled to which document won.
func TestIPNSSeqRisesWhenHTTPSWins(t *testing.T) {
	ctx := t.Context()
	w := newIPNSWriter(t)
	w.applyRefs(nil, 120)
	info := w.head.Info()

	// Both channels carry the same generation, but the HTTPS document is fresher, so
	// it wins selection; the IPNS record (seq 20) names the older document.
	tOld := time.Unix(1_700_000_000, 0).UTC()
	tNew := tOld.Add(time.Hour)
	httpsDoc := auditSignedDocAt(t, w.key, auditEntry(info), tNew)
	ipnsDoc := auditSignedDocAt(t, w.key, auditEntry(info), tOld)
	cidIPNS := storeDocBlock(t, w, ipnsDoc)
	w.forge(t, cidIPNS, 20)

	f := newFollower(t, w.writer, func(c *follow.Config) {
		c.URL = "https://writer.invalid"
		c.HTTP = auditDocClient(httpsDoc)
		c.IPNS = w.name()
		c.Routing = w.routing
	})

	if err := f.f.Poll(ctx); err != nil {
		t.Fatalf("Poll: %v", err)
	}
	// The HTTPS document was adopted (its freshness won), dated tNew.
	if floor, ok, err := follow.ReadUpdatedAt(f.store.KV()); err != nil || !ok || !floor.Equal(tNew) {
		t.Errorf("freshness floor = %s (ok=%t, err=%v), want the HTTPS document's %s", floor, ok, err, tNew)
	}
	// And the IPNS record raised the replay floor even though its document lost.
	if seq, ok, err := follow.ReadIPNSSeq(f.store.KV()); err != nil || !ok || seq != 20 {
		t.Errorf("IPNS replay floor = %d (ok=%t, err=%v), want 20 even though HTTPS won", seq, ok, err)
	}
}

// TestIPNSSeqRisesWhenDocumentRefused is reviewed scope 1,
// the refused-document half: an authenticated IPNS record raises the replay floor
// even when its document then fails the document-level preflight. The
// seq floor is a channel fact, not a consequence of adoption, so it advances while
// the checkpoint and freshness floor do not.
func TestIPNSSeqRisesWhenDocumentRefused(t *testing.T) {
	ctx := t.Context()
	w := newIPNSWriter(t)
	w.applyRefs(nil, 120)
	info := w.head.Info()

	// A coverage-mismatched entry: the root covers 120 but the entry claims 130. The
	// document authenticates but the head preflight refuses it.
	entry := auditEntry(info)
	claimed := uint64(130)
	entry.SyncedTo = &claimed
	doc := auditSignedDocAt(t, w.key, entry, time.Unix(1_700_000_000, 0).UTC())
	cidDoc := storeDocBlock(t, w, doc)
	w.forge(t, cidDoc, 20)

	f := ipnsFollower(t, w)
	if err := f.f.Poll(ctx); err == nil {
		t.Fatal("adopted a coverage-mismatched document; want it refused")
	}
	// The record raised the replay floor even though its document was refused.
	if seq, ok, err := follow.ReadIPNSSeq(f.store.KV()); err != nil || !ok || seq != 20 {
		t.Errorf("IPNS replay floor = %d (ok=%t, err=%v), want 20 despite the refusal", seq, ok, err)
	}
	// No checkpoint and no freshness floor: the document effected no authoritative
	// state.
	if _, _, _, _, ok, err := follow.ReadCheckpoint(f.store.KV(), testHead); err != nil || ok {
		t.Errorf("checkpoint after the refusal: ok=%t err=%v, want none", ok, err)
	}
	if _, ok, err := follow.ReadUpdatedAt(f.store.KV()); err != nil || ok {
		t.Errorf("freshness floor after the refusal: ok=%t err=%v, want it unmoved", ok, err)
	}
}

// storeDocBlock stores body as the raw block a follower fetches over bitswap for an
// IPNS record naming it, and returns its CID.
func storeDocBlock(t *testing.T, w *ipnsWriter, body []byte) cid.Cid {
	t.Helper()
	c := rawCID(t, string(body))
	blk, err := blocks.NewBlockWithCid(body, c)
	if err != nil {
		t.Fatalf("NewBlockWithCid: %v", err)
	}
	if err := w.store.Blocks().Put(t.Context(), blk); err != nil {
		t.Fatalf("storing doc block: %v", err)
	}
	return c
}

// TestSyncDoesNotStampAStaleGeneration is the transition invariant: a fetch pass held
// mid-walk while a newer generation is adopted must not stamp its now-stale root as
// fetched. It runs under the suite's -race, which exercises the snapshot that lets
// the pass read the head state without racing the transition; this asserts the CAS
// that keeps the stale pass from stamping its generation.
func TestSyncDoesNotStampAStaleGeneration(t *testing.T) {
	ctx := t.Context()
	st := openAuditStore(t)
	_, infos := coveredHead(t, st, 130, 140)
	root130, root140 := infos[0].Root, infos[1].Root

	// The pass under test fetches the @130 generation; gate its open segment so the
	// pass pauses mid-walk, holding no lock. Resume now performs bounded structural
	// admission of every index block before exposing a durable checkpoint, so arm
	// the gate only after Resume has admitted @130.
	h130, err := archive.Load(ctx, archive.Config{Blocks: st.Blocks()}, root130)
	if err != nil {
		t.Fatalf("Load(root130): %v", err)
	}
	enum, err := h130.Enumerate(ctx)
	if err != nil {
		t.Fatalf("Enumerate(root130): %v", err)
	}
	if !enum.Open.Defined() {
		t.Fatal("the covered head has no open segment to gate on")
	}
	gate := &gateBlockstore{
		Blockstore: st.Blocks(), target: cid.Undef,
		entered: make(chan struct{}), release: make(chan struct{}),
	}

	_, key, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("ed25519.GenerateKey: %v", err)
	}
	roots := server.NewRootStore(st.KV())
	manifests := server.NewManifestStore(st.KV())
	registry, err := server.NewHeads(server.HeadsConfig{Net: testNet, Roots: roots, Manifests: manifests})
	if err != nil {
		t.Fatalf("server.NewHeads: %v", err)
	}
	rec, err := pinning.NewReconciler(pinning.Config{Ledger: catalog.NewLedger(st.KV()), ManifestTip: manifests.Get})
	if err != nil {
		t.Fatalf("pinning.NewReconciler: %v", err)
	}
	f, err := follow.New(follow.Config{
		Net: testNet, URL: "https://writer.invalid", PubKey: key.Public().(ed25519.PublicKey),
		Heads:        map[string]pinning.Policy{testHead: pinning.Full()},
		Local:        gate,
		Sessions:     auditNoFetchSessions{},
		Registry:     registry,
		Roots:        roots,
		Reconciler:   rec,
		KV:           st.KV(),
		FetchTimeout: 60 * time.Second,
	})
	if err != nil {
		t.Fatalf("follow.New: %v", err)
	}
	t.Cleanup(func() {
		if err := f.Close(); err != nil {
			t.Errorf("Follower.Close: %v", err)
		}
	})

	// Adopt @130 (Resume from its checkpoint): registry and adopted are @130, fetched
	// is still undefined, so the pass has @130 to fetch.
	if err := follow.WriteCheckpoint(st.KV(), testHead, root130, 130, cid.Undef, time.Unix(1_700_000_000, 0).UTC()); err != nil {
		t.Fatalf("WriteCheckpoint: %v", err)
	}
	if err := f.Resume(ctx); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	gate.target = enum.Open

	// The @130 pass, gated at @130's open segment.
	var passErr error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		passErr = follow.SyncHead(f, ctx, testHead)
	}()
	<-gate.entered

	// A newer transition moves the adopted generation on to @140 while the @130 pass is
	// paused -- the pass is now stale. Release it: the CAS must see adopted (@140) is no
	// longer its snapshot (@130) and refuse to stamp @130 as fetched.
	follow.SetHeadAdopted(f, testHead, root140)
	close(gate.release)
	wg.Wait()
	if passErr != nil {
		t.Fatalf("stale @130 pass: %v", passErr)
	}
	if got := follow.HeadFetched(f, testHead); got == root130 {
		t.Errorf("the stale @130 pass stamped fetched = %s, want the CAS to have skipped it", got)
	}
}

// TestDocumentLevelAdmissionTwoHeads is the transition invariant: with two followed
// heads, one valid and one coverage-mismatched, a later-dated document is refused
// whole -- neither head's checkpoint is written and the global floor does not move --
// and a refusal is recorded. The corrected document is then adopted normally.
//
// Without the two-phase (preflight-all, then commit-all) admission, the valid head A
// (processed first, sorted) would checkpoint and raise the global floor before head B
// could refuse.
func TestDocumentLevelAdmissionTwoHeads(t *testing.T) {
	ctx := t.Context()
	st := openAuditStore(t)
	rootA := coveredHeadNamed(t, st, "head-a", 120)
	rootB := coveredHeadNamed(t, st, "head-b", 120)
	_, key, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("ed25519.GenerateKey: %v", err)
	}
	mx := metrics.New()

	tf := newTwoHeadFollower(t, st, key, mx)

	// Head A valid at 120; head B coverage-mismatched -- its root covers 120 but the
	// entry claims 130. The document is later-dated than any floor (there is none yet).
	at := time.Unix(1_700_000_000, 0).UTC()
	entryA := namedEntry("head-a", rootA, 120)
	entryB := namedEntry("head-b", rootB, 130) // derived 120 != claimed 130: a coverage mismatch
	tf.body = auditSignedDocEntriesAt(t, key, []server.HeadEntry{entryA, entryB}, at)

	if err := tf.f.Poll(ctx); err == nil {
		t.Fatal("adopted a document with a coverage-mismatched head; want the whole document refused")
	} else if !strings.Contains(err.Error(), "contradicts its floor") {
		t.Errorf("Poll err = %v, want the coverage-mismatch refusal", err)
	}

	// Neither head checkpointed, and the global floor never moved.
	if _, _, _, _, ok, err := follow.ReadCheckpoint(st.KV(), "head-a"); err != nil || ok {
		t.Errorf("head-a checkpoint after the refusal: ok=%t err=%v, want none", ok, err)
	}
	if _, _, _, _, ok, err := follow.ReadCheckpoint(st.KV(), "head-b"); err != nil || ok {
		t.Errorf("head-b checkpoint after the refusal: ok=%t err=%v, want none", ok, err)
	}
	if _, ok, err := follow.ReadUpdatedAt(st.KV()); err != nil || ok {
		t.Errorf("global freshness floor after the refusal: ok=%t err=%v, want it unmoved (unset)", ok, err)
	}
	// No authoritative effect of any kind (clarification 3): no compatibility-mirror
	// write and no registry exposure for either head, not just no checkpoint/floor.
	for _, name := range []string{"head-a", "head-b"} {
		if _, ok, err := tf.roots.Get(ctx, name); err != nil || ok {
			t.Errorf("RootStore mirror of %q after the refusal: ok=%t err=%v, want no write", name, ok, err)
		}
		if _, ok := tf.registry.Get(name); ok {
			t.Errorf("registry exposed %q after the refusal; want it unexposed", name)
		}
	}
	if got := refusalCount(t, mx, metrics.RefusalCoverageMismatch); got != 1 {
		t.Errorf("bloar_follow_refusals_total{reason=%q} = %g, want 1", metrics.RefusalCoverageMismatch, got)
	}

	// The corrected document -- head B now claims the 120 its root covers -- is adopted
	// normally, both heads.
	entryB = namedEntry("head-b", rootB, 120)
	tf.body = auditSignedDocEntriesAt(t, key, []server.HeadEntry{entryA, entryB}, at)
	if err := tf.f.Poll(ctx); err != nil {
		t.Fatalf("corrected Poll: %v", err)
	}
	if _, syncedTo, _, _, ok, err := follow.ReadCheckpoint(st.KV(), "head-a"); err != nil || !ok || syncedTo != 120 {
		t.Errorf("head-a checkpoint after the correction: synced_to=%d ok=%t err=%v, want 120", syncedTo, ok, err)
	}
	if _, syncedTo, _, _, ok, err := follow.ReadCheckpoint(st.KV(), "head-b"); err != nil || !ok || syncedTo != 120 {
		t.Errorf("head-b checkpoint after the correction: synced_to=%d ok=%t err=%v, want 120", syncedTo, ok, err)
	}
	if floor, ok, err := follow.ReadUpdatedAt(st.KV()); err != nil || !ok || !floor.Equal(at) {
		t.Errorf("global freshness floor after the correction = %s (ok=%t, err=%v), want %s", floor, ok, err, at)
	}
}

// twoHeadFollower follows head-a and head-b over one swappable document.
type twoHeadFollower struct {
	f        *follow.Follower
	body     []byte
	roots    *server.RootStore
	registry *server.Heads
}

// newTwoHeadFollower builds a follower over st following head-a and head-b, resolving
// a document a test swaps through tf.body. Fetches are denied; the roots are local.
func newTwoHeadFollower(t *testing.T, st *store.Store, key ed25519.PrivateKey, mx *metrics.Metrics) *twoHeadFollower {
	t.Helper()
	tf := &twoHeadFollower{}
	roots := server.NewRootStore(st.KV())
	manifests := server.NewManifestStore(st.KV())
	registry, err := server.NewHeads(server.HeadsConfig{Net: testNet, Roots: roots, Manifests: manifests})
	if err != nil {
		t.Fatalf("server.NewHeads: %v", err)
	}
	tf.roots, tf.registry = roots, registry
	rec, err := pinning.NewReconciler(pinning.Config{Ledger: catalog.NewLedger(st.KV()), ManifestTip: manifests.Get})
	if err != nil {
		t.Fatalf("pinning.NewReconciler: %v", err)
	}
	f, err := follow.New(follow.Config{
		Net:        testNet,
		URL:        "https://writer.invalid",
		PubKey:     key.Public().(ed25519.PublicKey),
		Heads:      map[string]pinning.Policy{"head-a": pinning.Full(), "head-b": pinning.Full()},
		Local:      st.Blocks(),
		Sessions:   auditNoFetchSessions{},
		Registry:   registry,
		Roots:      roots,
		Reconciler: rec,
		KV:         st.KV(),
		HTTP:       swappableClient(&tf.body),
		Metrics:    mx,
	})
	if err != nil {
		t.Fatalf("follow.New: %v", err)
	}
	t.Cleanup(func() {
		if err := f.Close(); err != nil {
			t.Errorf("Follower.Close: %v", err)
		}
	})
	tf.f = f
	return tf
}

// coveredHeadNamed builds a covered head under an arbitrary name in st, all blocks
// durable, and returns its root.
func coveredHeadNamed(t *testing.T, st *store.Store, name string, coverage uint64) cid.Cid {
	t.Helper()
	head, err := archive.New(t.Context(), archive.Config{Blocks: st.Blocks(), Resolver: catalog.New(st.KV())},
		archive.Params{Name: name, Net: testNet, OriginSlot: 0, SegBits: 2, FanoutBits: 2})
	if err != nil {
		t.Fatalf("archive.New(%q): %v", name, err)
	}
	if _, err := head.ApplyRefs(t.Context(), nil, coverage); err != nil {
		t.Fatalf("ApplyRefs(%q, %d): %v", name, coverage, err)
	}
	return head.Info().Root
}

// namedEntry is a publication entry for a named head at a root, claiming syncedTo.
func namedEntry(name string, root cid.Cid, syncedTo uint64) server.HeadEntry {
	s := syncedTo
	return server.HeadEntry{
		Name:       name,
		Root:       root.String(),
		OriginSlot: 0,
		SyncedTo:   &s,
		SegBits:    2,
		FanoutBits: 2,
	}
}
