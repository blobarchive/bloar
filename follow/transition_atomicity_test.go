package follow_test

// These tests cover atomic checkpoint transitions, including a fault-injected
// path through the real adoptEntry->putCheckpoint->expose sequence.
//
//   - The already-serving fast path must not admit a coverage-mismatched
//     document, nor let one raise the global freshness floor.
//   - A follower->writer promotion opens from the authoritative checkpoint,
//     not a stale RootStore/ManifestStore mirror.
//   - The manifest mirror is exact: an undefined-tip generation clears it
//     (asserted here through the promotion path; the Heads.Adopt path is in server).
//   - Checkpoint transitions are serialized: an older transition cannot
//     lower a floor, nor overwrite a generation, a newer one committed.

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ipfs/boxo/blockstore"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"

	"github.com/blobarchive/bloar/archive"
	"github.com/blobarchive/bloar/catalog"
	"github.com/blobarchive/bloar/follow"
	"github.com/blobarchive/bloar/metrics"
	"github.com/blobarchive/bloar/pinning"
	"github.com/blobarchive/bloar/server"
)

// swappableClient serves whatever *body holds at request time, so a test can change
// the document between polls.
func swappableClient(body *[]byte) *http.Client {
	return &http.Client{Transport: auditRoundTrip(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       io.NopCloser(bytes.NewReader(*body)),
		}, nil
	})}
}

// gateBlockstore blocks the Get of one target CID until release is closed, signalling
// entered when it first does. It lets a test pause a load at a precise point so an
// interleaving can be reconstructed without racing one.
type gateBlockstore struct {
	blockstore.Blockstore
	target  cid.Cid
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (g *gateBlockstore) Get(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	if c == g.target {
		g.once.Do(func() { close(g.entered) })
		select {
		case <-g.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return g.Blockstore.Get(ctx, c)
}

func (g *gateBlockstore) CollectionGeneration() uint64 {
	return g.Blockstore.(interface{ CollectionGeneration() uint64 }).CollectionGeneration()
}

func (g *gateBlockstore) ActiveEpoch() uint64 {
	return g.Blockstore.(interface{ ActiveEpoch() uint64 }).ActiveEpoch()
}

// TestCoverageMismatchOnServingFastPath verifies that the already-serving fast
// path does not admit a document that reuses the served root and tip but claims a
// HIGHER synced_to. That is a root/floor contradiction; short-circuiting it would
// return success and let adoptDoc raise the global freshness floor for it, suppressing
// a later admissible document. The fast path now falls through to the coverage check,
// which refuses it, and the freshness floor does not move.
func TestCoverageMismatchOnServingFastPath(t *testing.T) {
	ctx := t.Context()
	st := openAuditStore(t)
	_, infos := coveredHead(t, st, 120)
	_, key, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("ed25519.GenerateKey: %v", err)
	}
	mx := metrics.New()

	// Adopt the head honestly at coverage 120, dated T0.
	t0 := time.Unix(1_700_000_000, 0).UTC()
	body := auditSignedDocAt(t, key, auditEntry(infos[0]), t0)
	af := newAuditFollower(t, st, key, swappableClient(&body), mx)
	if err := af.f.Poll(ctx); err != nil {
		t.Fatalf("first Poll: %v", err)
	}
	if got := auditSyncedTo(t, af.registry); got != 120 {
		t.Fatalf("adopted synced_to = %d, want 120", got)
	}
	floor0, ok, err := follow.ReadUpdatedAt(st.KV())
	if err != nil || !ok || !floor0.Equal(t0) {
		t.Fatalf("freshness floor after honest adoption = %s (ok=%t, err=%v), want %s", floor0, ok, err, t0)
	}

	// The SAME root and tip, but claiming synced_to 200 and dated T1 > T0. It clears
	// the synced_to floor (200 > 120) and hits the already-serving fast path, but must
	// not be admitted: 200 contradicts the coverage the root encodes (120).
	lying := auditEntry(infos[0])
	higher := uint64(200)
	lying.SyncedTo = &higher
	t1 := t0.Add(time.Hour)
	body = auditSignedDocAt(t, key, lying, t1)
	if err := af.f.Poll(ctx); err == nil {
		t.Fatal("admitted a document that reused the served root/tip with a higher synced_to")
	} else if !strings.Contains(err.Error(), "contradicts its floor") {
		t.Errorf("err = %v, want the coverage-mismatch refusal", err)
	}
	if got := refusalCount(t, mx, metrics.RefusalCoverageMismatch); got != 1 {
		t.Errorf("bloar_follow_refusals_total{reason=%q} = %g, want 1", metrics.RefusalCoverageMismatch, got)
	}
	// The freshness floor MUST NOT have risen to T1: a coverage-mismatched document is
	// not trusted to advance it.
	floor1, ok, err := follow.ReadUpdatedAt(st.KV())
	if err != nil || !ok || !floor1.Equal(t0) {
		t.Errorf("freshness floor after the lying document = %s (ok=%t, err=%v), want it to stay at %s", floor1, ok, err, t0)
	}
	if got := auditSyncedTo(t, af.registry); got != 120 {
		t.Errorf("synced_to after the lying document = %d, want it to stay at 120", got)
	}
}

// TestPromotionUsesCheckpointNotStaleMirror is the transition invariant: a head promoted
// from follower to writer must open from its authoritative follower checkpoint, not a
// RootStore mirror a crash between the checkpoint commit and the exposure that writes
// the mirror left stale.
func TestPromotionUsesCheckpointNotStaleMirror(t *testing.T) {
	ctx := t.Context()
	st := openAuditStore(t)
	_, infos := coveredHead(t, st, 100, 120)
	root100, root120 := infos[0].Root, infos[1].Root

	roots := server.NewRootStore(st.KV())
	manifests := server.NewManifestStore(st.KV())

	// The crash-after-batch/before-expose state: the checkpoint records generation
	// @120, but the RootStore mirror still holds the previous generation @100 -- expose
	// never ran to update it.
	if err := follow.WriteCheckpoint(st.KV(), testHead, root120, 120, cid.Undef, time.Unix(1_700_000_000, 0).UTC()); err != nil {
		t.Fatalf("WriteCheckpoint: %v", err)
	}
	if err := roots.Put(ctx, testHead, root100); err != nil {
		t.Fatalf("RootStore.Put(stale mirror): %v", err)
	}

	promoted, err := follow.ReconcileWriterPromotion(ctx, follow.PromotionConfig{
		KV: st.KV(), Roots: roots, Manifests: manifests, Blocks: st.Blocks(), Params: auditParams(), Policy: pinning.Full(),
	}, testHead)
	if err != nil {
		t.Fatalf("ReconcileWriterPromotion: %v", err)
	}
	if !promoted {
		t.Fatal("ReconcileWriterPromotion reported no checkpoint; want the promotion reconciled")
	}
	// The mirror is now the checkpoint's generation, not the stale one.
	if r, has, err := roots.Get(ctx, testHead); err != nil || !has || r != root120 {
		t.Fatalf("RootStore mirror after promotion = %s (has=%t, err=%v), want the checkpoint's %s", r, has, err, root120)
	}
	// And opening the head as a writer serves the checkpoint's generation, not the
	// stale mirror's.
	head, err := server.OpenHead(ctx, archive.Config{Blocks: st.Blocks(), Resolver: catalog.New(st.KV())}, roots,
		archive.Params{Name: testHead, Net: testNet, OriginSlot: 0, SegBits: 2, FanoutBits: 2})
	if err != nil {
		t.Fatalf("OpenHead after promotion: %v", err)
	}
	if got, covered := head.SyncedTo(); !covered || got != 120 {
		t.Errorf("promoted head serves synced_to = %d (covered=%t), want 120", got, covered)
	}
}

// TestPromotionClearsStaleManifestMirror is the transition invariant through the promotion
// path: when the checkpoint's generation has no manifest chain, promotion must CLEAR
// any older 'm<head>' mirror, so the promoted writer does not republish a manifest
// history the checkpoint dropped.
func TestPromotionClearsStaleManifestMirror(t *testing.T) {
	ctx := t.Context()
	st := openAuditStore(t)
	_, infos := coveredHead(t, st, 120)
	root120 := infos[0].Root

	roots := server.NewRootStore(st.KV())
	manifests := server.NewManifestStore(st.KV())

	// The checkpoint's generation has no chain (undefined tip), but a stale manifest
	// mirror survives from an earlier generation. Any defined CID stands in for the
	// stale tip -- the mirror stores a CID, it does not load the block.
	if err := follow.WriteCheckpoint(st.KV(), testHead, root120, 120, cid.Undef, time.Unix(1_700_000_000, 0).UTC()); err != nil {
		t.Fatalf("WriteCheckpoint: %v", err)
	}
	if err := roots.Put(ctx, testHead, root120); err != nil {
		t.Fatalf("RootStore.Put: %v", err)
	}
	if err := manifests.Put(ctx, testHead, root120); err != nil {
		t.Fatalf("ManifestStore.Put(stale): %v", err)
	}

	if _, err := follow.ReconcileWriterPromotion(ctx, follow.PromotionConfig{
		KV: st.KV(), Roots: roots, Manifests: manifests, Blocks: st.Blocks(), Params: auditParams(), Policy: pinning.Full(),
	}, testHead); err != nil {
		t.Fatalf("ReconcileWriterPromotion: %v", err)
	}
	if tip, has, err := manifests.Get(ctx, testHead); err != nil || has {
		t.Errorf("manifest mirror after promotion = %s (has=%t, err=%v), want it cleared", tip, has, err)
	}
}

// TestTransitionSerializesResumeAndPoll is the transition invariant: checkpoint
// transitions are serialized, so an older Resume cannot expose a stale generation, nor
// lower a floor, that a newer Poll committed while it was mid-flight.
//
// The interleaving is reconstructed deterministically: Resume is paused inside the
// gated load of the OLD generation's root (it has read the old checkpoint and holds
// the transition lock), then Poll is started against a document that adopts the NEWER
// generation. Under the transition lock Poll blocks until Resume finishes, so the final
// exposed generation and freshness floor are the newer ones. Without the lock Poll
// would commit and expose the newer generation while Resume was paused, and the
// released Resume would then overwrite it with the older one -- the regression this
// guards. The sleep gives an unserialized Poll time to do exactly that before the gate
// is released, so the test fails loudly if the lock is removed.
func TestTransitionSerializesResumeAndPoll(t *testing.T) {
	ctx := t.Context()
	st := openAuditStore(t)
	_, infos := coveredHead(t, st, 100, 120)
	root100 := infos[0].Root
	_, key, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("ed25519.GenerateKey: %v", err)
	}

	// Gate the load of the OLD generation's root so Resume pauses after reading the
	// checkpoint and before exposing.
	gate := &gateBlockstore{
		Blockstore: st.Blocks(), target: root100,
		entered: make(chan struct{}), release: make(chan struct{}),
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

	// The committed OLD generation @100, dated T0.
	t0 := time.Unix(1_700_000_000, 0).UTC()
	if err := follow.WriteCheckpoint(st.KV(), testHead, root100, 100, cid.Undef, t0); err != nil {
		t.Fatalf("WriteCheckpoint: %v", err)
	}
	// A poll document that adopts the NEWER generation @120, dated T1 > T0.
	t1 := t0.Add(time.Hour)
	doc := auditSignedDocAt(t, key, auditEntry(infos[1]), t1)

	f, err := follow.New(follow.Config{
		Net: testNet, URL: "https://writer.invalid", PubKey: key.Public().(ed25519.PublicKey),
		Heads:    map[string]pinning.Policy{testHead: pinning.Full()},
		Local:    gate,
		Sessions: auditNoFetchSessions{},
		Registry: registry, Roots: roots, Reconciler: rec, KV: st.KV(),
		HTTP: auditDocClient(doc),
		// The gate holds a load; do not let the bounded read path time it out.
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

	var wg sync.WaitGroup
	var resumeErr, pollErr error
	wg.Add(1)
	go func() {
		defer wg.Done()
		resumeErr = f.Resume(ctx)
	}()

	// Resume is now paused inside the gated load of root100, holding the transition lock.
	<-gate.entered

	wg.Add(1)
	go func() {
		defer wg.Done()
		pollErr = f.Poll(ctx)
	}()

	// An unserialized Poll would commit and expose @120 during this window; a
	// serialized one is blocked on the transition lock and does nothing yet.
	time.Sleep(100 * time.Millisecond)
	close(gate.release)
	wg.Wait()

	if resumeErr != nil {
		t.Fatalf("Resume: %v", resumeErr)
	}
	if pollErr != nil {
		t.Fatalf("Poll: %v", pollErr)
	}
	// The newer generation won: the serialized older Resume could not overwrite it.
	if got := auditSyncedTo(t, registry); got != 120 {
		t.Errorf("exposed synced_to after interleaved Resume/Poll = %d, want the newer 120", got)
	}
	floor, ok, err := follow.ReadUpdatedAt(st.KV())
	if err != nil || !ok || !floor.Equal(t1) {
		t.Errorf("freshness floor = %s (ok=%t, err=%v), want the newer %s", floor, ok, err, t1)
	}
}

// TestFaultAtPutCheckpoint drives the REAL adoptEntry->putCheckpoint->expose
// sequence and injects a failure at putCheckpoint (a document dated before 1970, which
// putCheckpoint refuses). Nothing is committed and nothing is exposed; the previous
// generation stands, and a retry with a valid date completes the sequence.
func TestFaultAtPutCheckpoint(t *testing.T) {
	ctx := t.Context()
	st := openAuditStore(t)
	_, infos := coveredHead(t, st, 120)
	_, key, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("ed25519.GenerateKey: %v", err)
	}

	body := auditSignedDocAt(t, key, auditEntry(infos[0]), time.Unix(-100, 0).UTC())
	af := newAuditFollower(t, st, key, swappableClient(&body), nil)

	if err := af.f.Poll(ctx); err == nil {
		t.Fatal("adopted a document whose putCheckpoint must fail on its pre-1970 date")
	} else if !strings.Contains(err.Error(), "dated") {
		t.Errorf("err = %v, want the putCheckpoint date refusal", err)
	}
	if _, ok := af.registry.Get(testHead); ok {
		t.Error("exposed a head whose checkpoint was never committed")
	}
	if _, _, _, _, ok, err := follow.ReadCheckpoint(st.KV(), testHead); err != nil || ok {
		t.Errorf("checkpoint after the failed putCheckpoint: (ok=%t, err=%v), want none", ok, err)
	}

	// The same generation with a valid date now goes through the real sequence to
	// completion.
	body = auditSignedDocAt(t, key, auditEntry(infos[0]), time.Unix(1_700_000_000, 0).UTC())
	if err := af.f.Poll(ctx); err != nil {
		t.Fatalf("retry Poll with a valid date: %v", err)
	}
	if got := auditSyncedTo(t, af.registry); got != 120 {
		t.Errorf("synced_to after the valid retry = %d, want 120", got)
	}
	if _, _, _, _, ok, err := follow.ReadCheckpoint(st.KV(), testHead); err != nil || !ok {
		t.Errorf("checkpoint after the valid retry: (ok=%t, err=%v), want it committed", ok, err)
	}
}

// TestFaultAtAtomicCommit proves the document-level durability
// barrier is all-or-nothing: a failure immediately before its Pebble commit leaves
// checkpoint, compatibility mirrors, reconciler and serving registry unchanged.
func TestFaultAtAtomicCommit(t *testing.T) {
	ctx := t.Context()
	st := openAuditStore(t)
	_, infos := coveredHead(t, st, 120)
	root120 := infos[0].Root
	_, key, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("ed25519.GenerateKey: %v", err)
	}

	doc := auditSignedDocAt(t, key, auditEntry(infos[0]), time.Unix(1_700_000_000, 0).UTC())

	realRoots := server.NewRootStore(st.KV())
	registry, err := server.NewHeads(server.HeadsConfig{Net: testNet, Roots: realRoots, Manifests: server.NewManifestStore(st.KV())})
	if err != nil {
		t.Fatalf("server.NewHeads: %v", err)
	}
	rec, err := pinning.NewReconciler(pinning.Config{Ledger: catalog.NewLedger(st.KV()), ManifestTip: server.NewManifestStore(st.KV()).Get})
	if err != nil {
		t.Fatalf("pinning.NewReconciler: %v", err)
	}
	f, err := follow.New(follow.Config{
		Net: testNet, URL: "https://writer.invalid", PubKey: key.Public().(ed25519.PublicKey),
		Heads:    map[string]pinning.Policy{testHead: pinning.Full()},
		Local:    st.Blocks(),
		Sessions: auditNoFetchSessions{},
		Registry: registry, Roots: realRoots, Reconciler: rec, KV: st.KV(),
		HTTP: auditDocClient(doc),
	})
	if err != nil {
		t.Fatalf("follow.New: %v", err)
	}
	t.Cleanup(func() {
		if err := f.Close(); err != nil {
			t.Errorf("Follower.Close: %v", err)
		}
	})

	follow.SetBeforeAdmissionCommitHook(func() error { return errors.New("audit: injected atomic admission commit failure") })
	t.Cleanup(func() { follow.SetBeforeAdmissionCommitHook(nil) })
	if err := f.Poll(ctx); err == nil {
		t.Fatal("Poll reported success though the atomic admission commit failed")
	}
	if _, _, _, _, ok, err := follow.ReadCheckpoint(st.KV(), testHead); err != nil || ok {
		t.Fatalf("checkpoint after failed atomic commit = (ok=%t, err=%v), want absent", ok, err)
	}
	if _, ok, err := realRoots.Get(ctx, testHead); err != nil || ok {
		t.Fatalf("root mirror after failed atomic commit = (ok=%t, err=%v), want absent", ok, err)
	}
	if _, ok := registry.Get(testHead); ok {
		t.Fatal("registry exposed a head whose atomic admission commit failed")
	}

	// The same authenticated document remains admissible and commits cleanly once
	// the transient failure clears.
	follow.SetBeforeAdmissionCommitHook(nil)
	if err := f.Poll(ctx); err != nil {
		t.Fatalf("retry Poll: %v", err)
	}
	root, syncedTo, _, _, ok, err := follow.ReadCheckpoint(st.KV(), testHead)
	if err != nil || !ok || root != root120 || syncedTo != 120 {
		t.Fatalf("checkpoint after retry = (%s, %d, ok=%t, err=%v), want (%s, 120)", root, syncedTo, ok, err, root120)
	}
}
