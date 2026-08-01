package follow_test

// These tests cover replay floors, snapshot binding, admission preflight, and
// quarantine serialization during checkpoint transitions.
//
//   - An authenticated IPNS record raises the replay floor even when its
//     document is freshness-refused (the channel observation is separable from the
//     document candidate), in both an IPNS-only and an HTTPS-winner configuration.
//   - Every deterministic Registry.Adopt refusal is caught in the
//     zero-effect preflight, so a params-mismatched entry never durably checkpoints.
//   - The fetch pass binds its walk to the snapshotted root, so an
//     A->B->A skew cannot stamp A fetched when only B was walked.
//   - Quarantine is serialized into the transition linearization, so it
//     cannot land between a poll's preflight and its commit.

import (
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ipfs/go-cid"

	"github.com/blobarchive/bloar/archive"
	"github.com/blobarchive/bloar/catalog"
	"github.com/blobarchive/bloar/follow"
	"github.com/blobarchive/bloar/metrics"
	"github.com/blobarchive/bloar/pinning"
	"github.com/blobarchive/bloar/server"
	"github.com/blobarchive/bloar/store"
)

// TestIPNSSeqSurvivesFreshnessRefusal covers the IPNS-only path:
// a record whose document authenticates against the configured key but is dated
// before the freshness floor is (correctly) refused, yet its sequence still raises
// the replay floor -- so a later, lower sequence that a lost floor would have
// admitted is refused.
func TestIPNSSeqSurvivesFreshnessRefusal(t *testing.T) {
	ctx := t.Context()
	w := newIPNSWriter(t)
	w.applyRefs(nil, 120)
	info120 := w.head.Info()
	w.applyRefs(nil, 130)
	info130 := w.head.Info()
	w.applyRefs(nil, 140)
	info140 := w.head.Info()

	t1 := time.Unix(1_700_000_000, 0).UTC() // the first document's updated_at floor
	tOld := t1.Add(-time.Hour)              // the seq-20 document: freshness-old
	t2 := t1.Add(time.Hour)                 // the seq-15 document: fresh

	doc10 := storeDocBlock(t, w, auditSignedDocAt(t, w.key, auditEntry(info120), t1))
	doc20 := storeDocBlock(t, w, auditSignedDocAt(t, w.key, auditEntry(info130), tOld))
	doc15 := storeDocBlock(t, w, auditSignedDocAt(t, w.key, auditEntry(info140), t2))

	f := ipnsFollower(t, w)

	// Poll 1: seq 10 at @120 is adopted, establishing seq floor 10 and updated_at t1.
	w.forge(t, doc10, 10)
	f.poll()
	if got := auditSyncedTo(t, f.heads); got != 120 {
		t.Fatalf("after poll 1, adopted synced_to = %d, want 120", got)
	}
	if seq, ok, err := follow.ReadIPNSSeq(f.store.KV()); err != nil || !ok || seq != 10 {
		t.Fatalf("after poll 1, IPNS floor = %d (ok=%t, err=%v), want 10", seq, ok, err)
	}

	// Poll 2: seq 20 names a correctly-signed but freshness-old document. The document
	// is refused (Poll errors), but the authenticated sequence 20 raises the floor.
	w.forge(t, doc20, 20)
	if err := f.f.Poll(ctx); err == nil {
		t.Fatal("poll 2 accepted a freshness-old document")
	} else if !strings.Contains(err.Error(), "before the accepted floor") {
		t.Errorf("poll 2 err = %v, want the freshness refusal", err)
	}
	if seq, ok, err := follow.ReadIPNSSeq(f.store.KV()); err != nil || !ok || seq != 20 {
		t.Errorf("after the freshness-refused seq-20 record, IPNS floor = %d (ok=%t, err=%v), want 20", seq, ok, err)
	}
	if got := auditSyncedTo(t, f.heads); got != 120 {
		t.Errorf("after poll 2, adopted synced_to = %d, want it unchanged at 120 (the old document was refused)", got)
	}

	// Poll 3: seq 15 names a FRESH document (dated after the floor). It must be refused
	// on the replay floor -- 15 is below the 20 the refused record established. Without
	// this check, the floor would still be 10 and this seq-15 record's @140 document would
	// be adopted.
	w.forge(t, doc15, 15)
	if err := f.f.Poll(ctx); err == nil {
		t.Fatal("poll 3 accepted a record whose sequence is below the replay floor")
	} else if !strings.Contains(err.Error(), "below the accepted floor") {
		t.Errorf("poll 3 err = %v, want the sequence-floor refusal", err)
	}
	if got := auditSyncedTo(t, f.heads); got != 120 {
		t.Errorf("after poll 3, adopted synced_to = %d, want it unchanged at 120 (the seq-15 record was refused)", got)
	}
	_ = info140
}

// TestIPNSSeqSurvivesFreshnessRefusalHTTPSWinner covers the
// HTTPS-winner path: the IPNS record's document is freshness-old and, on top of that,
// a fresher HTTPS document wins the selection and is adopted -- yet the authenticated
// IPNS sequence still raises the replay floor, so a later lower sequence is refused.
func TestIPNSSeqSurvivesFreshnessRefusalHTTPSWinner(t *testing.T) {
	ctx := t.Context()
	w := newIPNSWriter(t)
	w.applyRefs(nil, 120)
	info120 := w.head.Info()
	w.applyRefs(nil, 130)
	info130 := w.head.Info()
	w.applyRefs(nil, 140)
	info140 := w.head.Info()

	t1 := time.Unix(1_700_000_000, 0).UTC()
	tOld := t1.Add(-time.Hour)
	tNew := t1.Add(time.Hour)
	t3 := t1.Add(2 * time.Hour)

	doc120 := auditSignedDocAt(t, w.key, auditEntry(info120), t1)   // seq-10 record + HTTPS, poll 1
	docOld := auditSignedDocAt(t, w.key, auditEntry(info120), tOld) // seq-20 record's freshness-old doc
	doc130 := auditSignedDocAt(t, w.key, auditEntry(info130), tNew) // the fresh HTTPS winner, poll 2
	doc140 := auditSignedDocAt(t, w.key, auditEntry(info140), t3)   // seq-15 record's fresh doc, poll 3
	cid120 := storeDocBlock(t, w, doc120)
	cidOld := storeDocBlock(t, w, docOld)
	cid140 := storeDocBlock(t, w, doc140)

	var httpsBody []byte
	f := newFollower(t, w.writer, func(c *follow.Config) {
		c.URL = "https://writer.invalid"
		c.HTTP = swappableClient(&httpsBody)
		c.IPNS = w.name()
		c.Routing = w.routing
		c.PubKey = w.pubkey()
	})

	// Poll 1: both channels carry @120 at t1; the seq-10 record establishes the floors.
	httpsBody = doc120
	w.forge(t, cid120, 10)
	f.poll()
	if got := auditSyncedTo(t, f.heads); got != 120 {
		t.Fatalf("after poll 1, adopted synced_to = %d, want 120", got)
	}

	// Poll 2: HTTPS wins with a fresh @130; the IPNS seq-20 record names a freshness-old
	// @120 document, refused as a candidate -- but its sequence raises the floor.
	httpsBody = doc130
	w.forge(t, cidOld, 20)
	if err := f.f.Poll(ctx); err != nil {
		t.Fatalf("poll 2 (HTTPS winner): %v", err)
	}
	if got := auditSyncedTo(t, f.heads); got != 130 {
		t.Fatalf("after poll 2, adopted synced_to = %d, want the HTTPS winner's 130", got)
	}
	if seq, ok, err := follow.ReadIPNSSeq(f.store.KV()); err != nil || !ok || seq != 20 {
		t.Errorf("after poll 2, IPNS floor = %d (ok=%t, err=%v), want 20 even though HTTPS won", seq, ok, err)
	}

	// Poll 3: HTTPS republishes @130 (a no-op); the IPNS seq-15 record names a fresh
	// @140. It must be refused on the replay floor (15 < 20), so @140 is not adopted.
	httpsBody = doc130
	w.forge(t, cid140, 15)
	_ = f.f.Poll(ctx) // the seq-15 refusal surfaces as a poll error; HTTPS is a no-op.
	if got := auditSyncedTo(t, f.heads); got != 130 {
		t.Errorf("after poll 3, adopted synced_to = %d, want it unchanged at 130 (the seq-15 record was refused)", got)
	}
}

// TestSyncBindsWalkToSnapshot is the transition invariant: the fetch pass must not
// stamp a root fetched unless it actually walked that root. expose swaps the Registry
// entry before it updates headState, and an equal-floor A->B->A adoption can leave
// headState.adopted back at A while the Registry moved on to B; a pass that snapshots
// A, walks whatever the Registry holds (B), then stamps A fetched marks A complete
// though only B was walked -- and here A has a deleted descendant, so it is genuinely
// incomplete.
func TestSyncBindsWalkToSnapshot(t *testing.T) {
	ctx := t.Context()
	st := openAuditStore(t)
	headEngine, infos := coveredHead(t, st, 120, 130)
	rootA, rootB := infos[0].Root, infos[1].Root

	// Give A a genuinely missing descendant: delete @120's open segment. If a pass
	// wrongly stamps A fetched, it marks a head complete that is missing a block.
	enumA, err := archive.Load(ctx, archive.Config{Blocks: st.Blocks()}, rootA)
	if err != nil {
		t.Fatalf("Load(rootA): %v", err)
	}
	ea, err := enumA.Enumerate(ctx)
	if err != nil {
		t.Fatalf("Enumerate(rootA): %v", err)
	}
	if err := st.Blocks().DeleteBlock(ctx, ea.Open); err != nil {
		t.Fatalf("DeleteBlock(A open segment): %v", err)
	}
	_ = headEngine

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
		Local:        st.Blocks(),
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

	// Reconstruct the skew: the Registry moved through A then to B, but headState.adopted
	// is back at A (the last A of an A->B->A).
	if err := follow.ExposeHead(f, ctx, testHead, rootA); err != nil {
		t.Fatalf("expose A: %v", err)
	}
	if err := follow.ExposeHead(f, ctx, testHead, rootB); err != nil {
		t.Fatalf("expose B: %v", err)
	}
	follow.SetHeadAdopted(f, testHead, rootA)

	// The pass snapshots adopted A but the Registry holds B. It must skip rather than
	// walk B and stamp A. A retry once the two agree is the next poll's job.
	if err := follow.SyncHead(f, ctx, testHead); err != nil {
		t.Fatalf("SyncHead: %v", err)
	}
	if got := follow.HeadFetched(f, testHead); got == rootA {
		t.Errorf("the pass stamped fetched = A (%s) though the Registry held B and only B is walkable", got)
	}
}

// namedHeadInfo builds a covered head under name at the given seg_bits and coverage
// in st, all blocks durable, and returns its Info (for a document entry).
func namedHeadInfo(t *testing.T, st *store.Store, name string, segBits, coverage uint64) archive.Info {
	t.Helper()
	head, err := archive.New(t.Context(), archive.Config{Blocks: st.Blocks(), Resolver: catalog.New(st.KV())},
		archive.Params{Name: name, Net: testNet, OriginSlot: 0, SegBits: segBits, FanoutBits: 2})
	if err != nil {
		t.Fatalf("archive.New(%q, seg_bits=%d): %v", name, segBits, err)
	}
	if _, err := head.ApplyRefs(t.Context(), nil, coverage); err != nil {
		t.Fatalf("ApplyRefs(%q, %d): %v", name, coverage, err)
	}
	return head.Info()
}

// TestPreflightCatchesParamsRefusal is the transition invariant: a deterministic
// Registry.Adopt refusal -- here an immutable-params change against the already-
// followed generation -- is caught in the zero-effect preflight, so the whole document
// is refused with no durable effect. Without it, the follower would checkpoint the
// changed-params generation and raise the floor before Adopt rejected it, leaving a
// checkpoint a restart into an empty registry could resume.
func TestPreflightCatchesParamsRefusal(t *testing.T) {
	ctx := t.Context()
	st := openAuditStore(t)
	infoA := namedHeadInfo(t, st, "head-a", 2, 120)
	infoB2 := namedHeadInfo(t, st, "head-b", 2, 120) // the baseline generation
	infoB3 := namedHeadInfo(t, st, "head-b", 3, 120) // same name, changed seg_bits
	_, key, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("ed25519.GenerateKey: %v", err)
	}
	mx := metrics.New()
	tf := newTwoHeadFollower(t, st, key, mx)

	// Poll 1: adopt both heads at the baseline (head-b at seg_bits 2).
	at := time.Unix(1_700_000_000, 0).UTC()
	tf.body = auditSignedDocEntriesAt(t, key, []server.HeadEntry{auditEntry(infoA), auditEntry(infoB2)}, at)
	if err := tf.f.Poll(ctx); err != nil {
		t.Fatalf("baseline Poll: %v", err)
	}
	baseA, _, _, _, okA, _ := follow.ReadCheckpoint(st.KV(), "head-a")
	baseB, baseBSynced, _, _, okB, _ := follow.ReadCheckpoint(st.KV(), "head-b")
	if !okA || !okB {
		t.Fatalf("baseline checkpoints missing: head-a ok=%t, head-b ok=%t", okA, okB)
	}

	// Poll 2: head-a is valid; head-b is republished with a DIFFERENT seg_bits. The
	// preflight's shared Adopt validation refuses it, so the whole document is refused.
	tf.body = auditSignedDocEntriesAt(t, key, []server.HeadEntry{auditEntry(infoA), auditEntry(infoB3)}, at.Add(time.Hour))
	if err := tf.f.Poll(ctx); err == nil {
		t.Fatal("adopted a document whose head-b changed its immutable seg_bits")
	} else if !strings.Contains(err.Error(), "immutable") {
		t.Errorf("Poll err = %v, want the immutable-params refusal", err)
	}

	// Nothing durable moved: both checkpoints, the mirrors, the exposures, and the
	// global floor are exactly the baseline.
	if r, s, _, _, ok, _ := follow.ReadCheckpoint(st.KV(), "head-a"); !ok || r != baseA {
		t.Errorf("head-a checkpoint after the refusal = %s (ok=%t), want the baseline %s", r, ok, baseA)
		_ = s
	}
	if r, s, _, _, ok, _ := follow.ReadCheckpoint(st.KV(), "head-b"); !ok || r != baseB || s != baseBSynced {
		t.Errorf("head-b checkpoint after the refusal = %s/%d (ok=%t), want the baseline %s/%d", r, s, ok, baseB, baseBSynced)
	}
	if floor, ok, err := follow.ReadUpdatedAt(st.KV()); err != nil || !ok || !floor.Equal(at) {
		t.Errorf("global floor after the refusal = %s (ok=%t, err=%v), want the baseline %s", floor, ok, err, at)
	}
	for name, want := range map[string]cid.Cid{"head-a": baseA, "head-b": baseB} {
		if r, ok, err := tf.roots.Get(ctx, name); err != nil || !ok || r != want {
			t.Errorf("RootStore mirror of %q after the refusal = %s (ok=%t, err=%v), want the baseline %s", name, r, ok, err, want)
		}
		if _, ok := tf.registry.Get(name); !ok {
			t.Errorf("head %q dropped out of the registry after the refusal", name)
		}
	}
}

// TestQuarantineBeforePollNoOpPlan is the transition invariant, the no-op-plan shape:
// a head quarantined before a poll whose document would otherwise be a no-op
// (already-serving, newer updated_at) must refuse the whole document, so the backstop
// does NOT raise the freshness floor for a head this node has stopped serving.
func TestQuarantineBeforePollNoOpPlan(t *testing.T) {
	ctx := t.Context()
	st := openAuditStore(t)
	_, infos := coveredHead(t, st, 120)
	_, key, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("ed25519.GenerateKey: %v", err)
	}

	t0 := time.Unix(1_700_000_000, 0).UTC()
	var body []byte
	af := newAuditFollower(t, st, key, swappableClient(&body), nil)

	// Adopt @120 at t0.
	body = auditSignedDocAt(t, key, auditEntry(infos[0]), t0)
	if err := af.f.Poll(ctx); err != nil {
		t.Fatalf("adopt Poll: %v", err)
	}

	// The head is quarantined (a bad blob was served elsewhere).
	if err := follow.QuarantineHead(af.f, testHead, "audit: forced quarantine"); err == nil {
		t.Fatal("QuarantineHead returned nil; want the quarantine error")
	}

	// A newer re-publication of the SAME generation would be a no-op plan, and its
	// backstop would raise the freshness floor. Quarantined, it must be refused instead.
	body = auditSignedDocAt(t, key, auditEntry(infos[0]), t0.Add(time.Hour))
	if err := af.f.Poll(ctx); err == nil {
		t.Fatal("a quarantined head's re-publication was accepted")
	} else if !strings.Contains(err.Error(), "quarantine") {
		t.Errorf("Poll err = %v, want the quarantine refusal", err)
	}
	if floor, ok, err := follow.ReadUpdatedAt(st.KV()); err != nil || !ok || !floor.Equal(t0) {
		t.Errorf("freshness floor after the quarantined no-op = %s (ok=%t, err=%v), want it unchanged at %s", floor, ok, err, t0)
	}
}

// TestQuarantineBeforePollWritePlan is the transition invariant, the write-plan shape:
// a head quarantined before a poll that carries a NEW generation must refuse the whole
// document, so no checkpoint is written and the floor and mirror do not move.
func TestQuarantineBeforePollWritePlan(t *testing.T) {
	ctx := t.Context()
	st := openAuditStore(t)
	_, infos := coveredHead(t, st, 120, 130)
	root120 := infos[0].Root
	_, key, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("ed25519.GenerateKey: %v", err)
	}

	t0 := time.Unix(1_700_000_000, 0).UTC()
	var body []byte
	af := newAuditFollower(t, st, key, swappableClient(&body), nil)

	body = auditSignedDocAt(t, key, auditEntry(infos[0]), t0)
	if err := af.f.Poll(ctx); err != nil {
		t.Fatalf("adopt Poll: %v", err)
	}
	if err := follow.QuarantineHead(af.f, testHead, "audit: forced quarantine"); err == nil {
		t.Fatal("QuarantineHead returned nil; want the quarantine error")
	}

	// The new @130 generation: a write plan, refused because the head is quarantined.
	body = auditSignedDocAt(t, key, auditEntry(infos[1]), t0.Add(time.Hour))
	if err := af.f.Poll(ctx); err == nil {
		t.Fatal("a quarantined head adopted a new generation")
	}
	if r, s, _, _, ok, _ := follow.ReadCheckpoint(st.KV(), testHead); !ok || r != root120 || s != 120 {
		t.Errorf("checkpoint after the quarantined write = %s/%d (ok=%t), want it unchanged at %s/120", r, s, ok, root120)
	}
	if floor, ok, err := follow.ReadUpdatedAt(st.KV()); err != nil || !ok || !floor.Equal(t0) {
		t.Errorf("freshness floor after the quarantined write = %s (ok=%t, err=%v), want it unchanged at %s", floor, ok, err, t0)
	}
	if r, ok, err := af.roots.Get(ctx, testHead); err != nil || !ok || r != root120 {
		t.Errorf("RootStore mirror after the quarantined write = %s (ok=%t, err=%v), want it unchanged at %s", r, ok, err, root120)
	}
}

// TestQuarantineSerializedWithCommit is the corresponding core: a quarantine
// racing a poll's commit is serialized by the transition lock, so the poll's write is
// atomic -- the checkpoint and the compatibility mirror always agree. Without the
// serialization, the quarantine flips during the poll's checkpoint write and expose
// then refuses, leaving a checkpoint written but the mirror not updated (a torn
// generation a restart could resume yet never serve).
func TestQuarantineSerializedWithCommit(t *testing.T) {
	ctx := t.Context()
	st := openAuditStore(t)
	_, infos := coveredHead(t, st, 120, 130)
	root130 := infos[1].Root
	_, key, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("ed25519.GenerateKey: %v", err)
	}

	t0 := time.Unix(1_700_000_000, 0).UTC()
	var body []byte
	af := newAuditFollower(t, st, key, swappableClient(&body), nil)

	// Adopt @120.
	body = auditSignedDocAt(t, key, auditEntry(infos[0]), t0)
	if err := af.f.Poll(ctx); err != nil {
		t.Fatalf("adopt Poll: %v", err)
	}

	// Poll the @130 write plan, paused after its checkpoint is durable and before expose
	// (holding transition), and race a quarantine against the exposure. This is the
	// exact window the torn state opens in: the checkpoint is @130 on disk, and whether
	// the mirror follows depends on whether a quarantine snuck in before expose. The
	// suite runs under -race, so the un-snapshotted read the transition invariant also fixes is caught
	// here if it regresses.
	gate := newGateOnce()
	follow.SetBeforeExposeHook(gate.pause)
	t.Cleanup(func() { follow.SetBeforeExposeHook(nil) })
	body = auditSignedDocAt(t, key, auditEntry(infos[1]), t0.Add(time.Hour))

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_ = af.f.Poll(ctx)
	}()
	<-gate.entered
	go func() {
		defer wg.Done()
		_ = follow.QuarantineHead(af.f, testHead, "audit: racing quarantine")
	}()
	close(gate.release)
	wg.Wait()

	// Serialized, the quarantine cannot flip while the poll holds transition, so the
	// poll commits @130 coherently -- checkpoint AND mirror both @130 -- and the
	// quarantine applies after. The checkpoint and mirror never disagree.
	cpRoot, _, _, _, cpOK, err := follow.ReadCheckpoint(st.KV(), testHead)
	if err != nil {
		t.Fatalf("ReadCheckpoint: %v", err)
	}
	mirror, mOK, err := af.roots.Get(ctx, testHead)
	if err != nil {
		t.Fatalf("RootStore.Get: %v", err)
	}
	if !cpOK || !mOK {
		t.Fatalf("checkpoint ok=%t, mirror ok=%t, want both present", cpOK, mOK)
	}
	if cpRoot != mirror {
		t.Errorf("checkpoint root %s and mirror %s disagree: a quarantine tore the commit", cpRoot, mirror)
	}
	if cpRoot != root130 || mirror != root130 {
		t.Errorf("after the serialized commit, checkpoint=%s mirror=%s, want both the newer @130 %s (the poll wins; "+
			"the quarantine applies after)", cpRoot, mirror, root130)
	}
	if !follow.HeadQuarantined(af.f, testHead) {
		t.Error("the head is not quarantined after the race; the quarantine should have applied once the poll released")
	}
}

// namedHeadInfosAt advances one head under name through the given coverages, returning
// the Info snapshot after each (index-aligned), all blocks durable in st.
func namedHeadInfosAt(t *testing.T, st *store.Store, name string, coverages ...uint64) []archive.Info {
	t.Helper()
	head, err := archive.New(t.Context(), archive.Config{Blocks: st.Blocks(), Resolver: catalog.New(st.KV())},
		archive.Params{Name: name, Net: testNet, OriginSlot: 0, SegBits: 2, FanoutBits: 2})
	if err != nil {
		t.Fatalf("archive.New(%q): %v", name, err)
	}
	infos := make([]archive.Info, 0, len(coverages))
	for _, c := range coverages {
		if _, err := head.ApplyRefs(t.Context(), nil, c); err != nil {
			t.Fatalf("ApplyRefs(%q, %d): %v", name, c, err)
		}
		infos = append(infos, head.Info())
	}
	return infos
}

// TestQuarantineFreezesOtherHeads is the ops rider's two-head regression:
// one quarantined head freezes adoption of every OTHER head the same writer publishes,
// because a document is admitted as a whole. head-a is quarantined; a later document
// advancing both head-a and head-b is refused, and head-b does not advance.
func TestQuarantineFreezesOtherHeads(t *testing.T) {
	ctx := t.Context()
	st := openAuditStore(t)
	a := namedHeadInfosAt(t, st, "head-a", 120, 130)
	b := namedHeadInfosAt(t, st, "head-b", 120, 130)
	_, key, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("ed25519.GenerateKey: %v", err)
	}
	tf := newTwoHeadFollower(t, st, key, metrics.New())

	// Adopt both heads at @120.
	at := time.Unix(1_700_000_000, 0).UTC()
	tf.body = auditSignedDocEntriesAt(t, key, []server.HeadEntry{auditEntry(a[0]), auditEntry(b[0])}, at)
	if err := tf.f.Poll(ctx); err != nil {
		t.Fatalf("baseline Poll: %v", err)
	}

	// head-a is quarantined (its writer served a forged versioned hash somewhere).
	if err := follow.QuarantineHead(tf.f, "head-a", "audit: forced quarantine"); err == nil {
		t.Fatal("QuarantineHead returned nil; want the quarantine error")
	}

	// A later document advances BOTH heads. It is refused as a whole because head-a is
	// quarantined, so head-b -- perfectly healthy -- does not advance either.
	tf.body = auditSignedDocEntriesAt(t, key, []server.HeadEntry{auditEntry(a[1]), auditEntry(b[1])}, at.Add(time.Hour))
	if err := tf.f.Poll(ctx); err == nil {
		t.Fatal("adopted a document naming a quarantined head")
	} else if !strings.Contains(err.Error(), "quarantine") {
		t.Errorf("Poll err = %v, want the quarantine refusal", err)
	}
	if _, s, _, _, ok, _ := follow.ReadCheckpoint(st.KV(), "head-b"); !ok || s != 120 {
		t.Errorf("head-b checkpoint after the frozen poll = synced_to %d (ok=%t), want it frozen at 120", s, ok)
	}
	if floor, ok, err := follow.ReadUpdatedAt(st.KV()); err != nil || !ok || !floor.Equal(at) {
		t.Errorf("global floor after the frozen poll = %s (ok=%t, err=%v), want it unchanged at the baseline %s", floor, ok, err, at)
	}
}

// auditSignedDocRawUpdatedAt signs a single-head document whose updated_at is an
// arbitrary raw string, so a test can produce a CORRECTLY-SIGNED document with a
// malformed timestamp (the signature covers whatever bytes updated_at holds).
func auditSignedDocRawUpdatedAt(t *testing.T, key ed25519.PrivateKey, entry server.HeadEntry, updatedAt string) []byte {
	t.Helper()
	u := server.Unsigned{
		V:         server.DocVersion,
		Net:       testNet,
		UpdatedAt: updatedAt,
		Heads:     []server.HeadEntry{entry},
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

// TestIPNSSeqSurvivesMalformedTimestamp is follow-up's gap: the channel
// observation is captured at "signature verified", BEFORE updated_at is parsed, so a
// correctly-signed IPNS record whose document has a malformed updated_at still raises
// the replay floor even though its candidate is refused. Without this, the sequence
// dies with the malformed candidate and a later lower sequence naming a valid document
// is admitted -- the replay the floor exists to stop.
func TestIPNSSeqSurvivesMalformedTimestamp(t *testing.T) {
	ctx := t.Context()
	w := newIPNSWriter(t)
	w.applyRefs(nil, 120)
	info120 := w.head.Info()
	w.applyRefs(nil, 130)
	info130 := w.head.Info()

	// A correctly-signed document with a malformed updated_at (seq 20), and a later,
	// fully valid document (seq 15).
	badDoc := storeDocBlock(t, w, auditSignedDocRawUpdatedAt(t, w.key, auditEntry(info120), "not-rfc3339"))
	validDoc := storeDocBlock(t, w, auditSignedDocAt(t, w.key, auditEntry(info130), time.Unix(1_700_000_000, 0).UTC()))

	f := ipnsFollower(t, w)

	// Poll 1: seq 20's document authenticates against the configured key but its
	// updated_at is malformed. The candidate is refused, but the sequence raises the floor.
	w.forge(t, badDoc, 20)
	if err := f.f.Poll(ctx); err == nil {
		t.Fatal("poll 1 accepted a document with a malformed updated_at")
	} else if !strings.Contains(err.Error(), "unparseable updated_at") {
		t.Errorf("poll 1 err = %v, want the malformed-timestamp refusal", err)
	}
	if seq, ok, err := follow.ReadIPNSSeq(f.store.KV()); err != nil || !ok || seq != 20 {
		t.Errorf("after the malformed-timestamp seq-20 record, IPNS floor = %d (ok=%t, err=%v), want 20", seq, ok, err)
	}

	// Poll 2: seq 15 names a fully valid document. It must be refused on the replay floor
	// (15 < 20) and NOT admitted -- it must not persist floor 15 nor adopt @130.
	w.forge(t, validDoc, 15)
	if err := f.f.Poll(ctx); err == nil {
		t.Fatal("poll 2 accepted a record whose sequence is below the replay floor")
	} else if !strings.Contains(err.Error(), "below the accepted floor") {
		t.Errorf("poll 2 err = %v, want the sequence-floor refusal", err)
	}
	if seq, ok, err := follow.ReadIPNSSeq(f.store.KV()); err != nil || !ok || seq != 20 {
		t.Errorf("after poll 2, IPNS floor = %d (ok=%t, err=%v), want it still 20", seq, ok, err)
	}
	if _, ok := f.heads.Get(testHead); ok {
		t.Error("a head was adopted though both polls were refused (the seq-15 replay must not be admitted)")
	}
}

// auditDocInvalidSignature signs a single-head document and then CORRUPTS the
// signature, so the document carries the configured public key (the pubkey-equality
// check passes) but does not verify (doc.Verify fails). It is what proves the channel
// observation is captured at "signature verified", not merely "configured key matched".
func auditDocInvalidSignature(t *testing.T, key ed25519.PrivateKey, entry server.HeadEntry, at time.Time) []byte {
	t.Helper()
	u := server.Unsigned{
		V:         server.DocVersion,
		Net:       testNet,
		UpdatedAt: at.UTC().Format(time.RFC3339),
		Heads:     []server.HeadEntry{entry},
	}
	canonical, err := u.Canonical()
	if err != nil {
		t.Fatalf("Canonical: %v", err)
	}
	sig := ed25519.Sign(key, canonical)
	sig[0] ^= 0xff // corrupt: the configured key is carried, but the signature is invalid.
	doc := server.Doc{
		Unsigned:  u,
		Pubkey:    hex.EncodeToString(key.Public().(ed25519.PublicKey)),
		Signature: hex.EncodeToString(sig),
	}
	body, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	return body
}

// TestIPNSFloorNotRaisedByInvalidSignature is required
// regression that the repo lacked: the IPNS replay floor's no-raise coverage was
// wrong-KEY only, with no case using the CONFIGURED key and a non-verifying signature.
// It proves the observation-capture point is "signature verified", not "configured key
// matched": a seq-20 record whose document carries the followed key but a corrupt
// signature must not raise the floor, so a later VALID lower sequence 15 stays
// admissible and becomes the floor.
func TestIPNSFloorNotRaisedByInvalidSignature(t *testing.T) {
	ctx := t.Context()
	w := newIPNSWriter(t)
	w.applyRefs(nil, 120)
	info120 := w.head.Info()
	w.applyRefs(nil, 130)
	info130 := w.head.Info()

	// seq 20: the followed key, an invalid signature. seq 15: fully valid.
	badDoc := storeDocBlock(t, w, auditDocInvalidSignature(t, w.key, auditEntry(info120), time.Unix(1_700_000_000, 0).UTC()))
	validDoc := storeDocBlock(t, w, auditSignedDocAt(t, w.key, auditEntry(info130), time.Unix(1_700_000_500, 0).UTC()))

	f := ipnsFollower(t, w)

	// Poll 1: the seq-20 record's document carries the configured key but its signature
	// does not verify. It is rejected, and the floor is NOT raised.
	w.forge(t, badDoc, 20)
	if err := f.f.Poll(ctx); err == nil {
		t.Fatal("poll 1 accepted a document whose signature does not verify")
	} else if !strings.Contains(err.Error(), "does not verify") {
		t.Errorf("poll 1 err = %v, want the signature-verification refusal", err)
	}
	if _, ok, err := follow.ReadIPNSSeq(f.store.KV()); err != nil || ok {
		t.Errorf("IPNS floor after the invalid-signature seq-20 record: ok=%t err=%v, want it still absent", ok, err)
	}

	// Poll 2: the seq-15 record is fully valid and LOWER. Because the forged seq 20 never
	// raised the floor, 15 remains admissible -- it is adopted and becomes the floor.
	w.forge(t, validDoc, 15)
	if err := f.f.Poll(ctx); err != nil {
		t.Fatalf("poll 2 (valid seq 15): %v", err)
	}
	if got := auditSyncedTo(t, f.heads); got != 130 {
		t.Errorf("after poll 2, adopted synced_to = %d, want 130 (the valid seq-15 document)", got)
	}
	if seq, ok, err := follow.ReadIPNSSeq(f.store.KV()); err != nil || !ok || seq != 15 {
		t.Errorf("IPNS floor after the valid seq-15 record = %d (ok=%t, err=%v), want 15", seq, ok, err)
	}
}
