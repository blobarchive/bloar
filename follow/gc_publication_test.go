package follow_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ipfs/boxo/blockstore"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"

	"github.com/blobarchive/bloar/follow"
	"github.com/blobarchive/bloar/store"
)

// hidingHasStore reconstructs the gap between follower preflight and commit:
// bytes fetched during preflight remain readable, but the commit-time local
// presence check observes one publication anchor as gone.
type hidingHasStore struct {
	blockstore.Blockstore

	mu      sync.Mutex
	hidden  cid.Cid
	enabled bool
}

func (s *hidingHasStore) Has(ctx context.Context, c cid.Cid) (bool, error) {
	s.mu.Lock()
	hide := s.enabled && c == s.hidden
	s.mu.Unlock()
	if hide {
		return false, nil
	}
	return s.Blockstore.Has(ctx, c)
}

func (s *hidingHasStore) Get(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	s.mu.Lock()
	hide := s.enabled && c == s.hidden
	s.mu.Unlock()
	if hide {
		return nil, fmt.Errorf("test block %s is hidden", c)
	}
	return s.Blockstore.Get(ctx, c)
}

func (s *hidingHasStore) hide(c cid.Cid) {
	s.mu.Lock()
	s.hidden, s.enabled = c, true
	s.mu.Unlock()
}

func (s *hidingHasStore) show() {
	s.mu.Lock()
	s.enabled = false
	s.mu.Unlock()
}

func TestFollowerRetouchesRootBeforeCheckpointAndExposure(t *testing.T) {
	w := newWriter(t)
	w.ingestSlot(100, 1)

	var local *hidingHasStore
	f := newFollower(t, w, func(c *follow.Config) {
		local = &hidingHasStore{Blockstore: c.Local}
		c.Local = local
	})
	root := w.head.Root()
	follow.SetBetweenPhasesHook(func() { local.hide(root) })
	t.Cleanup(func() { follow.SetBetweenPhasesHook(nil) })

	err := f.pollErr()
	if err == nil {
		t.Fatal("Poll checkpointed a root that disappeared after preflight")
	}
	if msg := err.Error(); !strings.Contains(msg, root.String()) || !strings.Contains(msg, "closure") {
		t.Fatalf("Poll error = %q, want the vanished root and publication closure", msg)
	}
	if _, _, _, _, ok, err := follow.ReadCheckpoint(f.store.KV(), testHead); err != nil || ok {
		t.Fatalf("checkpoint after vanished-root refusal: ok=%t err=%v, want none", ok, err)
	}
	if _, ok := f.heads.Get(testHead); ok {
		t.Fatal("the vanished root was exposed despite the failed commit-time touch")
	}

	// The fault was only in the publication gap. Once the root is visible again,
	// the same verified document can commit normally.
	follow.SetBetweenPhasesHook(nil)
	local.show()
	if err := f.f.Poll(t.Context()); err != nil {
		t.Fatalf("Poll after restoring the local root: %v", err)
	}
	if got, ok := f.heads.Get(testHead); !ok || got.Root() != root {
		t.Fatalf("restored root exposure = %v (ok=%t), want %s", got, ok, root)
	}
}

func TestFollowerRetouchesManifestTipBeforeCheckpointAndExposure(t *testing.T) {
	w := newWriter(t)
	w.ingestSlot(100, 1)
	tip := w.setManifest(cid.Undef, 0)

	var local *hidingHasStore
	f := newFollower(t, w, func(c *follow.Config) {
		local = &hidingHasStore{Blockstore: c.Local}
		c.Local = local
	})
	follow.SetBetweenPhasesHook(func() { local.hide(tip) })
	t.Cleanup(func() { follow.SetBetweenPhasesHook(nil) })

	err := f.pollErr()
	if err == nil {
		t.Fatal("Poll checkpointed a manifest tip that disappeared after preflight")
	}
	if msg := err.Error(); !strings.Contains(msg, tip.String()) || !strings.Contains(msg, "manifest") {
		t.Fatalf("Poll error = %q, want the vanished manifest tip", msg)
	}
	if _, _, _, _, ok, err := follow.ReadCheckpoint(f.store.KV(), testHead); err != nil || ok {
		t.Fatalf("checkpoint after vanished-tip refusal: ok=%t err=%v, want none", ok, err)
	}
	if _, ok := f.heads.Get(testHead); ok {
		t.Fatal("the generation with the vanished manifest tip was exposed")
	}
}

func TestFollowerRefusesClosureProofAcrossCollectionGeneration(t *testing.T) {
	w := newWriter(t)
	w.ingestSlot(100, 1)
	f := newFollower(t, w)

	first, err := f.store.Epochs().Begin()
	if err != nil {
		t.Fatalf("Begin first epoch: %v", err)
	}
	var second *store.BlockstoreEpoch
	follow.SetBetweenPhasesHook(func() {
		first.End()
		second, err = f.store.Epochs().Begin()
		if err != nil {
			t.Fatalf("Begin second epoch: %v", err)
		}
	})
	t.Cleanup(func() {
		follow.SetBetweenPhasesHook(nil)
		if second != nil {
			second.End()
		}
	})

	err = f.pollErr()
	if err == nil || !strings.Contains(err.Error(), "collection generation") {
		t.Fatalf("Poll across collection generations = %v, want generation-proof refusal", err)
	}
	if _, _, _, _, ok, err := follow.ReadCheckpoint(f.store.KV(), testHead); err != nil || ok {
		t.Fatalf("checkpoint after generation refusal: ok=%t err=%v, want none", ok, err)
	}
	if _, ok := f.heads.Get(testHead); ok {
		t.Fatal("generation whose closure proof crossed a GC cut was exposed")
	}
	staged, err := f.staging.List(t.Context())
	if err != nil {
		t.Fatalf("listing staging after refusal: %v", err)
	}
	if len(staged) == 0 {
		t.Fatal("blocks fetched by the refused closure lost their staging protection")
	}

	// Once the collector is idle, the same document can prove its unchanged
	// generation under Gate and commit normally.
	second.End()
	second = nil
	follow.SetBetweenPhasesHook(nil)
	if err := f.f.Poll(t.Context()); err != nil {
		t.Fatalf("Poll after stable collection generation: %v", err)
	}
	if got, ok := f.heads.Get(testHead); !ok || got.Root() != w.head.Root() {
		t.Fatalf("stable generation exposure = %v (ok=%t), want %s", got, ok, w.head.Root())
	}
}

func TestFollowerSyncCrossingCollectionCutDoesNotStampFetched(t *testing.T) {
	w := newWriter(t)
	w.ingestSlot(100, 1)
	f := newFollower(t, w)
	if err := f.f.Poll(t.Context()); err != nil {
		t.Fatalf("initial Poll: %v", err)
	}
	root := w.head.Root()
	follow.SetHeadFetched(f.f, testHead, cid.Undef)

	var epoch *store.BlockstoreEpoch
	follow.SetBeforeSyncCommitHook(func() {
		var err error
		epoch, err = f.store.Epochs().Begin()
		if err != nil {
			t.Fatalf("Begin at sync completion boundary: %v", err)
		}
	})
	t.Cleanup(func() {
		follow.SetBeforeSyncCommitHook(nil)
		if epoch != nil {
			epoch.End()
		}
	})

	err := follow.SyncHead(f.f, t.Context(), testHead)
	if err == nil || !strings.Contains(err.Error(), "collection generation changed") {
		t.Fatalf("SyncHead across collection cut = %v, want retryable generation error", err)
	}
	if got := follow.HeadFetched(f.f, testHead); got.Defined() {
		t.Fatalf("sync crossing collection cut stamped fetched=%s; want undefined retry state (root %s)", got, root)
	}
}

// TestFollowerRootTransitionInvalidatesFetchCompletion is the exact
// completion-marker recurrence which a content-addressed root can otherwise
// hide: A completed, B was exposed but its fetch failed, a B-era collection
// removed an A-only descendant, and A later recurred. fetched=A is evidence
// about the first adoption of A, not a permanent assertion that A's closure is
// still present. The A -> B transition must clear it so the later A pass repairs
// what collection removed.
func TestFollowerRootTransitionInvalidatesFetchCompletion(t *testing.T) {
	w := newWriter(t)
	fx := archiveWindows(t, w)

	var sessions *faultySessions
	f := windowFollower(t, w, func(c *follow.Config) {
		sessions = &faultySessions{inner: c.Sessions}
		c.Sessions = sessions
	})
	f.poll()

	rootA := w.head.Root()
	aOnly := fx.cids[113]
	if got := follow.HeadFetched(f.f, testHead); got != rootA {
		t.Fatalf("initial fetched marker = %s, want A %s", got, rootA)
	}
	if !f.hasLocally(aOnly) {
		t.Fatalf("A's retained blob %s was not fetched", aOnly)
	}

	// At synced_to=129 the eight-slot window starts at 121, so A's segment
	// [112,119] is no longer recursively retained. Make B's ordinary sync fail
	// on its new retained blob after commit has already exposed B.
	bBlobs, _ := w.ingestSlot(129, 5000)
	rootB := w.head.Root()
	bOnly := blobCID(t, bBlobs[0])
	sessions.failOn(bOnly)
	if err := f.pollErr(); err == nil {
		t.Fatal("B's injected fetch failure returned no error")
	}
	if got := follow.HeadAdopted(f.f, testHead); got != rootB {
		t.Fatalf("adopted root after B's failed sync = %s, want B %s", got, rootB)
	}
	if got := follow.HeadFetched(f.f, testHead); got.Defined() {
		t.Fatalf("A -> B left stale fetched marker %s after B's sync failed; want undefined", got)
	}

	// Stand in for the B-era sweep at the exact store boundary. Beginning the
	// epoch also advances CollectionGeneration, invalidating A's old subtree
	// presence memo independently of the completion marker under test.
	epoch, err := f.store.Epochs().Begin()
	if err != nil {
		t.Fatalf("begin B-era collection: %v", err)
	}
	deleted, protected, err := epoch.DeleteCandidate(t.Context(), aOnly)
	epoch.End()
	if err != nil {
		t.Fatalf("delete A-only collection candidate: %v", err)
	}
	if !deleted || protected {
		t.Fatalf("A-only collection candidate: deleted=%t protected=%t, want true/false", deleted, protected)
	}
	if f.hasLocally(aOnly) {
		t.Fatalf("B-era collection left A-only blob %s local", aOnly)
	}

	// A signed publication cannot roll coverage back, but a root CID may recur
	// after an allowed equal/future-floor transition. ExposeHead drives that
	// registry/headState half directly so this test isolates the recurrence.
	sessions.clear()
	if err := follow.ExposeHead(f.f, t.Context(), testHead, rootA); err != nil {
		t.Fatalf("re-expose A: %v", err)
	}
	if err := follow.SyncHead(f.f, t.Context(), testHead); err != nil {
		t.Fatalf("sync recurrent A: %v", err)
	}
	if !f.hasLocally(aOnly) {
		t.Fatalf("recurrent A was considered complete without repairing blob %s", aOnly)
	}
	if got := follow.HeadFetched(f.f, testHead); got != rootA {
		t.Fatalf("repaired fetched marker = %s, want A %s", got, rootA)
	}
}

func TestFollowerResumeProtectsCheckpointClosureDuringActiveCollection(t *testing.T) {
	w := newWriter(t)
	blobs, _ := w.ingestSlot(100, 1)
	f := newFollower(t, w)
	syncedTo, covered := w.head.SyncedTo()
	if !covered {
		t.Fatal("writer fixture is not covered")
	}
	if err := follow.WriteCheckpoint(f.store.KV(), testHead, w.head.Root(), syncedTo, cid.Undef, time.Now()); err != nil {
		t.Fatalf("WriteCheckpoint: %v", err)
	}

	epoch, err := f.store.Epochs().Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	defer epoch.End()
	if err := f.f.Resume(t.Context()); err != nil {
		t.Fatalf("Resume during active collection: %v", err)
	}

	// This raw descendant was absent at the epoch's T0 cut and arrived only
	// while Resume reconstructed a checkpoint which had never been exposed or
	// reconciled. Resume must have walked it through the application view before
	// publication, otherwise the ongoing sweep could delete it immediately.
	blob := blobCID(t, blobs[0])
	if deleted, protected, err := epoch.DeleteCandidate(t.Context(), blob); err != nil {
		t.Fatalf("DeleteCandidate resumed descendant: %v", err)
	} else if deleted || !protected {
		t.Fatalf("resumed descendant: deleted=%t protected=%t, want false/true", deleted, protected)
	}
	if got, ok := f.heads.Get(testHead); !ok || got.Root() != w.head.Root() {
		t.Fatalf("resumed head exposure = %v (ok=%t), want %s", got, ok, w.head.Root())
	}
}
