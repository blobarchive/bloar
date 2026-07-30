package follow_test

import (
	"crypto/ed25519"
	"io"
	"net/http"
	"strings"
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

// This file is the checkpoint-generation coverage for the safety boundary and the safety boundary
// (the atomic-checkpoint hardening): the crash-point matrix, and the coverage-consistency and freshness
// rules the approved design added. The two flipped reproducers are next door in
// crash_floor_test.go; these exercise the windows around them by staging
// a committed checkpoint directly (follow.WriteCheckpoint) and resuming or polling
// over it, so a crash at a precise point can be reconstructed without racing one.

// openAuditStore opens a throwaway store for a checkpoint test.
func openAuditStore(t *testing.T) *store.Store {
	t.Helper()
	st, err := store.Open(t.TempDir(), store.WithPebbleLogger(quietPebble{}))
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() {
		if err := st.Close(); err != nil {
			t.Errorf("store.Close: %v", err)
		}
	})
	return st
}

// auditParams is the immutable head parameters coveredHead builds with, and the
// same set a writer promotion validates the checkpoint's root against (spec 3.1). It
// is one definition so the promotion preflight and the head build cannot drift, the
// property the transition invariant turns on.
func auditParams() archive.Params {
	return archive.Params{Name: testHead, Net: testNet, OriginSlot: 0, SegBits: 2, FanoutBits: 2}
}

// coveredHead builds a fresh head in st and applies an empty batch at each
// coverage, returning the head and the Info snapshot after each step
// (index-aligned with coverages). Every root's blocks are durable in st, so a
// resume or load never has to reach a network the audit tests deliberately deny.
func coveredHead(t *testing.T, st *store.Store, coverages ...uint64) (*archive.Head, []archive.Info) {
	t.Helper()
	head, err := archive.New(t.Context(), archive.Config{Blocks: st.Blocks(), Resolver: catalog.New(st.KV())}, auditParams())
	if err != nil {
		t.Fatalf("archive.New: %v", err)
	}
	infos := make([]archive.Info, 0, len(coverages))
	for _, c := range coverages {
		if _, err := head.ApplyRefs(t.Context(), nil, c); err != nil {
			t.Fatalf("ApplyRefs(%d): %v", c, err)
		}
		infos = append(infos, head.Info())
	}
	return head, infos
}

// auditFollower is a follower and the fresh registry/reconciler it was built over,
// the way a restarted process would find them.
type auditFollower struct {
	f         *follow.Follower
	registry  *server.Heads
	roots     *server.RootStore
	manifests *server.ManifestStore
}

// newAuditFollower builds a follower over st with a fresh registry, the way a
// restart does. client serves the publication document (nil for a resume-only
// test that never polls); mx counts refusals (nil for tests that do not assert on
// them). Fetches are denied, so every load resolves against st's local blocks.
func newAuditFollower(t *testing.T, st *store.Store, key ed25519.PrivateKey, client *http.Client, mx *metrics.Metrics) *auditFollower {
	t.Helper()
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
		Net:        testNet,
		URL:        "https://writer.invalid",
		PubKey:     key.Public().(ed25519.PublicKey),
		Heads:      map[string]pinning.Policy{testHead: pinning.Full()},
		Local:      st.Blocks(),
		Sessions:   auditNoFetchSessions{},
		Registry:   registry,
		Roots:      roots,
		Reconciler: rec,
		KV:         st.KV(),
		HTTP:       client,
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
	return &auditFollower{f: f, registry: registry, roots: roots, manifests: manifests}
}

// auditDocClient serves body to every poll.
func auditDocClient(body []byte) *http.Client {
	return &http.Client{Transport: auditRoundTrip(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader(string(body))),
		}, nil
	})}
}

// TestCheckpointCrashMatrix walks the three crash points the atomic
// checkpoint is defined against: before the batch, after the batch
// but before the head is exposed, and after it is exposed. Each leaves a
// consistent state, which is the property the split floors did not have.
func TestCheckpointCrashMatrix(t *testing.T) {
	authAt := time.Unix(1_700_000_000, 0).UTC()
	_, key, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("ed25519.GenerateKey: %v", err)
	}

	t.Run("before the batch: the previous generation is intact", func(t *testing.T) {
		ctx := t.Context()
		st := openAuditStore(t)
		_, infos := coveredHead(t, st, 100, 120)
		root100 := infos[0].Root

		// A prior generation @100 is committed; the @120 batch never ran (crash
		// before it), though @120's blocks are durable in st.
		if err := follow.WriteCheckpoint(st.KV(), testHead, root100, 100, cid.Undef, authAt); err != nil {
			t.Fatalf("WriteCheckpoint: %v", err)
		}
		af := newAuditFollower(t, st, key, nil, nil)
		if err := af.roots.Put(ctx, testHead, root100); err != nil {
			t.Fatalf("RootStore.Put: %v", err)
		}
		if err := af.f.Resume(ctx); err != nil {
			t.Fatalf("Resume: %v", err)
		}
		if got := auditSyncedTo(t, af.registry); got != 100 {
			t.Fatalf("resumed synced_to = %d, want the intact prior generation at 100", got)
		}
	})

	t.Run("after the batch, before expose: Resume exposes the checkpoint", func(t *testing.T) {
		ctx := t.Context()
		st := openAuditStore(t)
		_, infos := coveredHead(t, st, 120)
		root120 := infos[0].Root

		// The checkpoint committed, but expose -- the registry swap and its mirror
		// writes -- never ran. Resume reads the checkpoint and exposes it.
		if err := follow.WriteCheckpoint(st.KV(), testHead, root120, 120, cid.Undef, authAt); err != nil {
			t.Fatalf("WriteCheckpoint: %v", err)
		}
		af := newAuditFollower(t, st, key, nil, nil)
		if err := af.f.Resume(ctx); err != nil {
			t.Fatalf("Resume: %v", err)
		}
		if got := auditSyncedTo(t, af.registry); got != 120 {
			t.Fatalf("resumed synced_to = %d, want the checkpoint's 120", got)
		}
		// Exposing the checkpoint healed the RootStore mirror it had not yet written.
		if r, has, err := af.roots.Get(ctx, testHead); err != nil || !has || r != root120 {
			t.Fatalf("RootStore mirror after resume = %s (has=%t, err=%v), want it healed to %s", r, has, err, root120)
		}
	})

	t.Run("after expose: a normal restart resumes the checkpoint", func(t *testing.T) {
		ctx := t.Context()
		st := openAuditStore(t)
		_, infos := coveredHead(t, st, 120)
		root120 := infos[0].Root

		if err := follow.WriteCheckpoint(st.KV(), testHead, root120, 120, cid.Undef, authAt); err != nil {
			t.Fatalf("WriteCheckpoint: %v", err)
		}
		af := newAuditFollower(t, st, key, nil, nil)
		if err := af.roots.Put(ctx, testHead, root120); err != nil { // mirror, as expose wrote it
			t.Fatalf("RootStore.Put: %v", err)
		}
		if err := af.f.Resume(ctx); err != nil {
			t.Fatalf("Resume: %v", err)
		}
		if got := auditSyncedTo(t, af.registry); got != 120 {
			t.Fatalf("resumed synced_to = %d, want 120", got)
		}
	})
}

// TestCheckpointGlobalTimeReplay covers the freshness half of the safety boundary: the
// authorizing updated_at rides in the checkpoint batch and raises the global
// freshness floor atomically, so a crash after the commit -- before any separate
// updated_at write -- cannot leave an older signed document still admissible.
func TestCheckpointGlobalTimeReplay(t *testing.T) {
	ctx := t.Context()
	st := openAuditStore(t)
	_, infos := coveredHead(t, st, 120)
	root120 := infos[0].Root
	_, key, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("ed25519.GenerateKey: %v", err)
	}

	authAt := time.Unix(1_700_000_000, 0).UTC()
	// Only the checkpoint batch ran; no separate global updated_at write followed
	// it. The batch is what raised the freshness floor.
	if err := follow.WriteCheckpoint(st.KV(), testHead, root120, 120, cid.Undef, authAt); err != nil {
		t.Fatalf("WriteCheckpoint: %v", err)
	}

	// An older document that the synced_to floor would ADMIT -- it claims more
	// coverage (130) -- but dated before the checkpoint's authorizing time. Only the
	// freshness floor the batch committed can reject it.
	stale := auditEntry(infos[0])
	higher := uint64(130)
	stale.SyncedTo = &higher
	body := auditSignedDocAt(t, key, stale, authAt.Add(-time.Hour))
	af := newAuditFollower(t, st, key, auditDocClient(body), nil)
	if err := af.roots.Put(ctx, testHead, root120); err != nil {
		t.Fatalf("RootStore.Put: %v", err)
	}

	if err := af.f.Resume(ctx); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if got := auditSyncedTo(t, af.registry); got != 120 {
		t.Fatalf("resumed synced_to = %d, want 120", got)
	}
	err = af.f.Poll(ctx)
	if err == nil {
		t.Fatal("admitted a document older than the committed checkpoint's authorizing time")
	}
	if !strings.Contains(err.Error(), "before the accepted floor") {
		t.Errorf("err = %v, want the freshness floor to reject the replay", err)
	}
	if got := auditSyncedTo(t, af.registry); got != 120 {
		t.Errorf("synced_to after the stale replay = %d, want it to stay at 120", got)
	}
}

// TestCheckpointLegacyRootNotResumed covers the migration rule: a root a
// pre-checkpoint follower left in the RootStore with no checkpoint is NOT resumed
// . The head
// stays unexposed until the first fresh verified publication commits a checkpoint.
func TestCheckpointLegacyRootNotResumed(t *testing.T) {
	ctx := t.Context()
	st := openAuditStore(t)
	_, infos := coveredHead(t, st, 120)
	root120 := infos[0].Root
	_, key, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("ed25519.GenerateKey: %v", err)
	}

	body := auditSignedDocAt(t, key, auditEntry(infos[0]), time.Unix(1_700_000_000, 0).UTC())
	af := newAuditFollower(t, st, key, auditDocClient(body), nil)
	// A legacy durable root, no checkpoint.
	if err := af.roots.Put(ctx, testHead, root120); err != nil {
		t.Fatalf("RootStore.Put: %v", err)
	}

	if err := af.f.Resume(ctx); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if _, ok := af.registry.Get(testHead); ok {
		t.Fatal("resumed a legacy root with no checkpoint; want the head left unexposed")
	}

	// The first fresh verified publication commits the checkpoint and exposes the head.
	if err := af.f.Poll(ctx); err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if _, ok := af.registry.Get(testHead); !ok {
		t.Fatal("the fresh publication did not expose the head")
	}
	if got := auditSyncedTo(t, af.registry); got != 120 {
		t.Fatalf("synced_to after the fresh publication = %d, want 120", got)
	}
	root, syncedTo, _, _, ok, err := follow.ReadCheckpoint(st.KV(), testHead)
	if err != nil || !ok {
		t.Fatalf("ReadCheckpoint after the fresh publication: (ok=%t, err=%v)", ok, err)
	}
	if root != root120 || syncedTo != 120 {
		t.Errorf("first checkpoint = (%s, %d), want (%s, 120)", root, syncedTo, root120)
	}
}

// TestCheckpointResumeFailsClosedOnFloorAboveCoverage covers the resume
// coverage-consistency direction: a checkpoint whose synced_to floor is above the
// coverage its own root encodes is an inconsistent local state, and Resume fails
// closed -- the head is refused, never served, never repaired down.
func TestCheckpointResumeFailsClosedOnFloorAboveCoverage(t *testing.T) {
	ctx := t.Context()
	st := openAuditStore(t)
	_, infos := coveredHead(t, st, 120)
	root120 := infos[0].Root
	_, key, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("ed25519.GenerateKey: %v", err)
	}
	mx := metrics.New()

	// The floor (200) is above the coverage the root (120) encodes.
	if err := follow.WriteCheckpoint(st.KV(), testHead, root120, 200, cid.Undef, time.Unix(1_700_000_000, 0).UTC()); err != nil {
		t.Fatalf("WriteCheckpoint: %v", err)
	}
	af := newAuditFollower(t, st, key, nil, mx)
	if err := af.f.Resume(ctx); err == nil {
		t.Fatal("resumed an inconsistent checkpoint whose floor is above its root's coverage")
	}
	if _, ok := af.registry.Get(testHead); ok {
		t.Error("exposed a head whose checkpoint floor is above its coverage; want it failed closed")
	}
	if got := refusalCount(t, mx, metrics.RefusalCoverageMismatch); got != 1 {
		t.Errorf("bloar_follow_refusals_total{reason=%q} = %g, want 1", metrics.RefusalCoverageMismatch, got)
	}
}

// TestCheckpointRepairsFloorUpToCoverage is the other resume direction: a
// checkpoint floor BELOW the coverage its root encodes is safe to serve, but the
// floor is repaired up to the root's coverage before serving, so a later document
// cannot regress into the gap.
func TestCheckpointRepairsFloorUpToCoverage(t *testing.T) {
	ctx := t.Context()
	st := openAuditStore(t)
	_, infos := coveredHead(t, st, 120)
	root120 := infos[0].Root
	_, key, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("ed25519.GenerateKey: %v", err)
	}

	// The floor (80) is below the coverage the root (120) encodes.
	if err := follow.WriteCheckpoint(st.KV(), testHead, root120, 80, cid.Undef, time.Unix(1_700_000_000, 0).UTC()); err != nil {
		t.Fatalf("WriteCheckpoint: %v", err)
	}
	af := newAuditFollower(t, st, key, nil, nil)
	if err := af.f.Resume(ctx); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if got := auditSyncedTo(t, af.registry); got != 120 {
		t.Fatalf("resumed synced_to = %d, want the root's coverage 120", got)
	}
	// The floor was repaired UP to the root's coverage and persisted.
	_, syncedTo, _, _, ok, err := follow.ReadCheckpoint(st.KV(), testHead)
	if err != nil || !ok {
		t.Fatalf("ReadCheckpoint: (ok=%t, err=%v)", ok, err)
	}
	if syncedTo != 120 {
		t.Errorf("checkpoint floor after repair = %d, want it raised to 120", syncedTo)
	}
}

// TestCheckpointRefusesCoverageMismatchAtAdoption covers the adoption
// coverage-consistency direction: a document whose root's derived coverage does
// not equal the synced_to it claims is refused before any checkpoint is written,
// so an inconsistent generation never becomes durable.
func TestCheckpointRefusesCoverageMismatchAtAdoption(t *testing.T) {
	ctx := t.Context()
	st := openAuditStore(t)
	_, infos := coveredHead(t, st, 120)
	_, key, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("ed25519.GenerateKey: %v", err)
	}
	mx := metrics.New()

	// A document whose root encodes coverage 120 but which claims synced_to 999.
	lying := auditEntry(infos[0])
	claim := uint64(999)
	lying.SyncedTo = &claim
	body := auditSignedDocAt(t, key, lying, time.Unix(1_700_000_000, 0).UTC())
	af := newAuditFollower(t, st, key, auditDocClient(body), mx)

	if err := af.f.Poll(ctx); err == nil {
		t.Fatal("adopted a document whose root coverage contradicts its synced_to")
	} else if !strings.Contains(err.Error(), "contradicts its floor") {
		t.Errorf("err = %v, want the coverage-mismatch refusal", err)
	}
	if _, ok := af.registry.Get(testHead); ok {
		t.Error("exposed a head from a coverage-contradicting document")
	}
	if _, _, _, _, ok, err := follow.ReadCheckpoint(st.KV(), testHead); err != nil || ok {
		t.Errorf("checkpoint after a refused adoption: (ok=%t, err=%v), want none written", ok, err)
	}
	if got := refusalCount(t, mx, metrics.RefusalCoverageMismatch); got != 1 {
		t.Errorf("bloar_follow_refusals_total{reason=%q} = %g, want 1", metrics.RefusalCoverageMismatch, got)
	}
}
