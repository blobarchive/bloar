package follow_test

// These tests cover the follower-to-writer promotion handoff.
//
//   - The handoff -- materialize the mirrors, retire the checkpoint -- is
//     ONE crash-idempotent synced batch. A crash before it commits changes nothing
//     and the rerun reconciles; a crash after leaves no checkpoint, so every later
//     restart is a no-op and a promoted writer's own advancing state stands.
//   - The whole generation is validated before any handoff write -- all
//     immutable params, coverage, and the DAG and manifest chain proved complete on
//     disk offline -- and any failure leaves the mirrors AND the checkpoint untouched.

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/ipfs/boxo/blockstore"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"

	"github.com/blobarchive/bloar/archive"
	"github.com/blobarchive/bloar/follow"
	"github.com/blobarchive/bloar/pinning"
	"github.com/blobarchive/bloar/schema"
	"github.com/blobarchive/bloar/server"
	"github.com/blobarchive/bloar/store"
)

// promoCfg builds a PromotionConfig over st with the covered-head params and a full
// pin policy -- the set a correct promotion of a covered (full) head validates
// against. coveredHead builds empty batches, so the full policy's completeness walk
// reaches only index blocks, all durable.
func promoCfg(st *store.Store, roots *server.RootStore, manifests *server.ManifestStore) follow.PromotionConfig {
	return follow.PromotionConfig{
		KV: st.KV(), Roots: roots, Manifests: manifests, Blocks: st.Blocks(),
		Params: auditParams(), Policy: pinning.Full(),
	}
}

// mirrorRoot is the RootStore mirror of testHead, or cid.Undef when absent.
func mirrorRoot(t *testing.T, roots *server.RootStore) cid.Cid {
	t.Helper()
	r, ok, err := roots.Get(t.Context(), testHead)
	if err != nil {
		t.Fatalf("RootStore.Get: %v", err)
	}
	if !ok {
		return cid.Undef
	}
	return r
}

// mirrorTip is the ManifestStore mirror of testHead, or cid.Undef when absent.
func mirrorTip(t *testing.T, manifests *server.ManifestStore) cid.Cid {
	t.Helper()
	tip, ok, err := manifests.Get(t.Context(), testHead)
	if err != nil {
		t.Fatalf("ManifestStore.Get: %v", err)
	}
	if !ok {
		return cid.Undef
	}
	return tip
}

// checkpointRoot is testHead's committed checkpoint root, or cid.Undef when there is
// no checkpoint (retired or never written).
func checkpointRoot(t *testing.T, st *store.Store) cid.Cid {
	t.Helper()
	root, _, _, _, ok, err := follow.ReadCheckpoint(st.KV(), testHead)
	if err != nil {
		t.Fatalf("ReadCheckpoint: %v", err)
	}
	if !ok {
		return cid.Undef
	}
	return root
}

// TestPromotionHandoffIsCrashIdempotent covers the crash matrix around the
// one-batch handoff: a crash before it commits changes nothing and the
// rerun reconciles; a crash after it (the checkpoint retired) makes every later
// startup a no-op.
func TestPromotionHandoffIsCrashIdempotent(t *testing.T) {
	ctx := t.Context()
	st := openAuditStore(t)
	_, infos := coveredHead(t, st, 100, 120)
	root100, root120 := infos[0].Root, infos[1].Root
	roots := server.NewRootStore(st.KV())
	manifests := server.NewManifestStore(st.KV())
	at := time.Unix(1_700_000_000, 0).UTC()

	// The crash-before state: checkpoint @120 committed, the mirror still the previous
	// generation @100 (expose never ran to update it).
	if err := follow.WriteCheckpoint(st.KV(), testHead, root120, 120, cid.Undef, at); err != nil {
		t.Fatalf("WriteCheckpoint: %v", err)
	}
	if err := roots.Put(ctx, testHead, root100); err != nil {
		t.Fatalf("RootStore.Put(stale mirror): %v", err)
	}

	// Inject a crash right before the handoff batch commits. The batch is atomic, so
	// nothing must change: the checkpoint stands and the mirror stays stale.
	follow.SetPromotionBeforeCommit(func() error { return errors.New("audit: simulated crash before handoff commit") })
	if _, err := follow.ReconcileWriterPromotion(ctx, promoCfg(st, roots, manifests), testHead); err == nil {
		t.Fatal("promotion did not surface the injected pre-commit crash")
	}
	follow.SetPromotionBeforeCommit(nil)
	if got := checkpointRoot(t, st); got != root120 {
		t.Errorf("checkpoint after the pre-commit crash = %s, want it intact at %s", got, root120)
	}
	if got := mirrorRoot(t, roots); got != root100 {
		t.Errorf("root mirror after the pre-commit crash = %s, want it still the stale %s", got, root100)
	}

	// The rerun -- the restart after that crash -- reconciles the mirror to the
	// checkpoint and retires the checkpoint, one batch.
	promoted, err := follow.ReconcileWriterPromotion(ctx, promoCfg(st, roots, manifests), testHead)
	if err != nil {
		t.Fatalf("ReconcileWriterPromotion rerun: %v", err)
	}
	if !promoted {
		t.Fatal("the rerun reported no checkpoint; want the promotion reconciled")
	}
	if got := mirrorRoot(t, roots); got != root120 {
		t.Errorf("root mirror after the rerun = %s, want the checkpoint's %s", got, root120)
	}
	if got := checkpointRoot(t, st); got.Defined() {
		t.Errorf("checkpoint after the rerun = %s, want it retired", got)
	}

	// The crash-after state is exactly where the rerun left things: checkpoint retired,
	// mirror correct. Every later restart is a no-op that cannot touch the mirror.
	promoted, err = follow.ReconcileWriterPromotion(ctx, promoCfg(st, roots, manifests), testHead)
	if err != nil {
		t.Fatalf("ReconcileWriterPromotion after retirement: %v", err)
	}
	if promoted {
		t.Error("a promotion after retirement found a checkpoint; want a no-op")
	}
	if got := mirrorRoot(t, roots); got != root120 {
		t.Errorf("root mirror after the no-op = %s, want it unchanged at %s", got, root120)
	}
}

// TestPromotedWriterAdvancesAndRestartsRetainNewer is the corresponding
// central rollback repro: promote, let the writer advance durably to a newer root
// and manifest, then restart a second and third time. The retired checkpoint means
// each restart is a no-op, so the writer's newer generation is retained every time
// -- never rewound to the checkpoint's, the bug the leftover checkpoint caused.
func TestPromotedWriterAdvancesAndRestartsRetainNewer(t *testing.T) {
	ctx := t.Context()
	st := openAuditStore(t)
	_, infos := coveredHead(t, st, 120, 130)
	root120, root130 := infos[0].Root, infos[1].Root
	roots := server.NewRootStore(st.KV())
	manifests := server.NewManifestStore(st.KV())
	at := time.Unix(1_700_000_000, 0).UTC()

	// The checkpoint's generation @120 carries a manifest chain; both its Manifest
	// blocks are local so the completeness walk passes.
	genesis := auditManifest(t, st, cid.Undef, 0)
	tip1 := auditManifest(t, st, genesis, 100)
	if err := follow.WriteCheckpoint(st.KV(), testHead, root120, 120, tip1, at); err != nil {
		t.Fatalf("WriteCheckpoint: %v", err)
	}

	// First startup promotes: the mirror becomes the checkpoint's generation and the
	// checkpoint is retired.
	if _, err := follow.ReconcileWriterPromotion(ctx, promoCfg(st, roots, manifests), testHead); err != nil {
		t.Fatalf("first promotion: %v", err)
	}
	if got := mirrorRoot(t, roots); got != root120 {
		t.Fatalf("root mirror after promotion = %s, want %s", got, root120)
	}
	if got := mirrorTip(t, manifests); got != tip1 {
		t.Fatalf("manifest mirror after promotion = %s, want %s", got, tip1)
	}

	// The promoted writer advances durably: a real mutation writes the newer root and
	// manifest tip straight to the mirrors (there is no follower checkpoint any more).
	tip2 := auditManifest(t, st, tip1, 200)
	if err := roots.Put(ctx, testHead, root130); err != nil {
		t.Fatalf("writer RootStore.Put: %v", err)
	}
	if err := manifests.Put(ctx, testHead, tip2); err != nil {
		t.Fatalf("writer ManifestStore.Put: %v", err)
	}

	// Second and third restarts: promotion finds no checkpoint and is a no-op, so the
	// writer's newer root and manifest are retained both times.
	for _, restart := range []string{"second", "third"} {
		promoted, err := follow.ReconcileWriterPromotion(ctx, promoCfg(st, roots, manifests), testHead)
		if err != nil {
			t.Fatalf("%s restart promotion: %v", restart, err)
		}
		if promoted {
			t.Errorf("%s restart found a checkpoint; want a no-op after retirement", restart)
		}
		if got := mirrorRoot(t, roots); got != root130 {
			t.Errorf("root mirror after the %s restart = %s, want the writer's newer %s", restart, got, root130)
		}
		if got := mirrorTip(t, manifests); got != tip2 {
			t.Errorf("manifest mirror after the %s restart = %s, want the writer's newer %s", restart, got, tip2)
		}
	}
}

// TestPromotionFailsClosedOnMissingDescendant is the corresponding
// completeness check for the data DAG: a checkpoint whose Head block is local but a
// descendant (its open segment) is not is refused offline, before any handoff write.
func TestPromotionFailsClosedOnMissingDescendant(t *testing.T) {
	ctx := t.Context()
	st := openAuditStore(t)
	head, infos := coveredHead(t, st, 120)
	root120 := infos[0].Root
	roots := server.NewRootStore(st.KV())
	manifests := server.NewManifestStore(st.KV())

	// Delete a descendant of the root: the open segment, reachable from the Head block
	// (schema.Head.Open) but now absent, the crash-after-checkpoint state where the
	// fetch pass had not yet materialized the DAG.
	enum, err := head.Enumerate(ctx)
	if err != nil {
		t.Fatalf("Enumerate: %v", err)
	}
	if !enum.Open.Defined() {
		t.Fatal("the covered head has no open segment to delete; the test needs a descendant")
	}
	if err := st.Blocks().DeleteBlock(ctx, enum.Open); err != nil {
		t.Fatalf("DeleteBlock(open segment): %v", err)
	}

	// A sentinel stale mirror, so "untouched" is observable.
	sentinel := root120
	if err := follow.WriteCheckpoint(st.KV(), testHead, root120, 120, cid.Undef, time.Unix(1_700_000_000, 0).UTC()); err != nil {
		t.Fatalf("WriteCheckpoint: %v", err)
	}

	_, err = follow.ReconcileWriterPromotion(ctx, promoCfg(st, roots, manifests), testHead)
	if err == nil {
		t.Fatal("promoted a head whose open segment is missing; want it refused as incompletely synced")
	}
	// No mirror mutation, no checkpoint retirement.
	if got := mirrorRoot(t, roots); got.Defined() {
		t.Errorf("root mirror after the refusal = %s, want it untouched (absent)", got)
	}
	if got := checkpointRoot(t, st); got != sentinel {
		t.Errorf("checkpoint after the refusal = %s, want it intact at %s", got, sentinel)
	}
}

// TestPromotionFailsClosedOnDanglingManifestTip is the corresponding
// completeness check for the manifest chain: a checkpoint carrying a defined tip
// whose block a crash left unfetched is refused offline, mirrors and checkpoint
// untouched.
func TestPromotionFailsClosedOnDanglingManifestTip(t *testing.T) {
	ctx := t.Context()
	st := openAuditStore(t)
	_, infos := coveredHead(t, st, 120)
	root120 := infos[0].Root
	roots := server.NewRootStore(st.KV())
	manifests := server.NewManifestStore(st.KV())

	// A defined tip whose block is NOT stored: EncodeManifest yields the CID, but the
	// block is never Put, so a local walk from it dangles at the first hop.
	_, dangling, err := schema.EncodeManifest(&schema.Manifest{
		V: schema.ManifestVersion, Head: testHead,
		Sources: []schema.Source{{
			Type: schema.SourceInboxEvents, Address: bytes.Repeat([]byte{1}, schema.AddressSize),
			Topic: bytes.Repeat([]byte{2}, schema.TopicSize), FromBlock: 100, OpenEnded: true,
		}},
	})
	if err != nil {
		t.Fatalf("EncodeManifest: %v", err)
	}
	if err := follow.WriteCheckpoint(st.KV(), testHead, root120, 120, dangling, time.Unix(1_700_000_000, 0).UTC()); err != nil {
		t.Fatalf("WriteCheckpoint: %v", err)
	}

	_, err = follow.ReconcileWriterPromotion(ctx, promoCfg(st, roots, manifests), testHead)
	if err == nil {
		t.Fatal("promoted a head whose manifest tip block is absent; want it refused as a dangling tip")
	}
	if got := mirrorRoot(t, roots); got.Defined() {
		t.Errorf("root mirror after the refusal = %s, want it untouched (absent)", got)
	}
	if got := mirrorTip(t, manifests); got.Defined() {
		t.Errorf("manifest mirror after the refusal = %s, want it untouched (absent)", got)
	}
	if got := checkpointRoot(t, st); got != root120 {
		t.Errorf("checkpoint after the refusal = %s, want it intact at %s", got, root120)
	}
}

// writerParams is the immutable params the writer-fixture head is built with, the
// set a promotion of that head validates against.
func writerParams() archive.Params {
	return archive.Params{Name: testHead, Net: testNet, OriginSlot: testOrigin, SegBits: testSegBits, FanoutBits: testFanout}
}

// TestPromotionDistinguishesBlobHoleFromMissingIndex is the transition invariant under
// reviewed scope: the completeness preflight is scoped to the promoted
// head's retention policy. A window head promotes with a blob its policy does not
// retain absent (a knowingly incomplete blob history, spec 9 / operations 7.2), but a
// blob its window DOES retain, or any index block, missing fails the promotion closed.
func TestPromotionDistinguishesBlobHoleFromMissingIndex(t *testing.T) {
	windowPolicy := pinning.Window(time.Duration(windowSlots*secondsPerSlot)*time.Second, secondsPerSlot)
	at := time.Unix(1_700_000_000, 0).UTC()

	// setup builds the window fixture (blobs at slots 97 and 105 outside the window,
	// 113 inside it, 121 open) and stages a follower checkpoint for its head.
	setup := func(t *testing.T) (*writer, *fixture, follow.PromotionConfig) {
		t.Helper()
		w := newWriter(t)
		fx := archiveWindows(t, w)
		root := w.head.Info().Root
		syncedTo, _ := w.head.SyncedTo()
		if err := follow.WriteCheckpoint(w.store.KV(), testHead, root, syncedTo, cid.Undef, at); err != nil {
			t.Fatalf("WriteCheckpoint: %v", err)
		}
		cfg := follow.PromotionConfig{
			KV: w.store.KV(), Roots: w.roots, Manifests: w.manifests, Blocks: w.store.Blocks(),
			Params: writerParams(), Policy: windowPolicy,
		}
		return w, fx, cfg
	}

	t.Run("a policy-consistent blob hole does not block promotion", func(t *testing.T) {
		ctx := t.Context()
		w, fx, cfg := setup(t)
		// Slot 97's blob is in segment w12 [96,103], outside the window this policy
		// retains -- a hole a window follower knowingly carries. Delete it.
		if err := w.store.Blocks().DeleteBlock(ctx, fx.cids[97]); err != nil {
			t.Fatalf("DeleteBlock(out-of-window blob): %v", err)
		}
		promoted, err := follow.ReconcileWriterPromotion(ctx, cfg, testHead)
		if err != nil {
			t.Fatalf("window promotion refused a policy-consistent blob hole: %v", err)
		}
		if !promoted {
			t.Fatal("promotion reported no checkpoint; want it reconciled")
		}
		// The handoff completed: the checkpoint is retired.
		if _, _, _, _, ok, err := follow.ReadCheckpoint(w.store.KV(), testHead); err != nil || ok {
			t.Errorf("checkpoint after the window promotion: ok=%t err=%v, want it retired", ok, err)
		}
	})

	t.Run("a missing retained (in-window) blob fails closed", func(t *testing.T) {
		ctx := t.Context()
		w, fx, cfg := setup(t)
		// Slot 113's blob is in segment w14 [112,119], inside the window -- the policy
		// retains it, so its absence is a genuine incompleteness.
		if err := w.store.Blocks().DeleteBlock(ctx, fx.cids[113]); err != nil {
			t.Fatalf("DeleteBlock(in-window blob): %v", err)
		}
		if _, err := follow.ReconcileWriterPromotion(ctx, cfg, testHead); err == nil {
			t.Fatal("promoted a window head missing a blob its window retains")
		}
		// Fail closed: the checkpoint is NOT retired.
		if _, _, _, _, ok, err := follow.ReadCheckpoint(w.store.KV(), testHead); err != nil || !ok {
			t.Errorf("checkpoint after the failed promotion: ok=%t err=%v, want it intact", ok, err)
		}
	})

	t.Run("a missing index block fails closed", func(t *testing.T) {
		ctx := t.Context()
		w, _, cfg := setup(t)
		enum, err := w.head.Enumerate(ctx)
		if err != nil {
			t.Fatalf("Enumerate: %v", err)
		}
		if len(enum.Sealed) == 0 {
			t.Fatal("the fixture sealed no segments; the test needs an index block to delete")
		}
		// A sealed Segment block is index -- required present under EVERY policy.
		if err := w.store.Blocks().DeleteBlock(ctx, enum.Sealed[0].CID); err != nil {
			t.Fatalf("DeleteBlock(sealed segment): %v", err)
		}
		if _, err := follow.ReconcileWriterPromotion(ctx, cfg, testHead); err == nil {
			t.Fatal("promoted a head missing an index (Segment) block")
		}
		if _, _, _, _, ok, err := follow.ReadCheckpoint(w.store.KV(), testHead); err != nil || !ok {
			t.Errorf("checkpoint after the failed promotion: ok=%t err=%v, want it intact", ok, err)
		}
	})

	t.Run("a present but CID-invalid index block fails closed", func(t *testing.T) {
		ctx := t.Context()
		w, _, cfg := setup(t)
		enum, err := w.head.Enumerate(ctx)
		if err != nil {
			t.Fatalf("Enumerate: %v", err)
		}
		if len(enum.Sealed) == 0 {
			t.Fatal("the fixture sealed no segments; the test needs an index block to corrupt")
		}
		// The block is present but its bytes do not hash to its CID -- local corruption
		// the store does not catch on read. The preflight's CID check must (the transition invariant,
		// clarified: readable AND CID-valid).
		cfg.Blocks = &corruptBlockstore{Blockstore: w.store.Blocks(), target: enum.Sealed[0].CID}
		if _, err := follow.ReconcileWriterPromotion(ctx, cfg, testHead); err == nil {
			t.Fatal("promoted a head whose index block is corrupt")
		} else if !strings.Contains(err.Error(), "corrupt") {
			t.Errorf("err = %v, want the CID-validity refusal", err)
		}
		if _, _, _, _, ok, err := follow.ReadCheckpoint(w.store.KV(), testHead); err != nil || !ok {
			t.Errorf("checkpoint after the failed promotion: ok=%t err=%v, want it intact", ok, err)
		}
	})
}

// corruptBlockstore returns bytes that do not hash to the CID for one target block,
// standing in for local corruption a content-addressed store does not catch on read
// (it does not re-hash). Every other block passes through.
type corruptBlockstore struct {
	blockstore.Blockstore
	target cid.Cid
}

func (c *corruptBlockstore) Get(ctx context.Context, k cid.Cid) (blocks.Block, error) {
	if k == c.target {
		return blocks.NewBlockWithCid([]byte("audit: corrupt bytes that do not hash to this cid"), k)
	}
	return c.Blockstore.Get(ctx, k)
}

// TestPromotionFailsClosedOnParamsMismatch is the corresponding params
// preflight: an immutable-param mismatch (a different seg_bits than the root was
// built with) is caught before any handoff write, so the mirrors and checkpoint are
// left untouched and startup aborts -- rather than mutating the mirrors only for
// OpenHead to fail immediately after.
func TestPromotionFailsClosedOnParamsMismatch(t *testing.T) {
	ctx := t.Context()
	st := openAuditStore(t)
	_, infos := coveredHead(t, st, 120)
	root120 := infos[0].Root
	roots := server.NewRootStore(st.KV())
	manifests := server.NewManifestStore(st.KV())

	if err := follow.WriteCheckpoint(st.KV(), testHead, root120, 120, cid.Undef, time.Unix(1_700_000_000, 0).UTC()); err != nil {
		t.Fatalf("WriteCheckpoint: %v", err)
	}

	// The config asks for seg_bits 3; the checkpoint's root was built with 2.
	cfg := promoCfg(st, roots, manifests)
	cfg.Params = archive.Params{Name: testHead, Net: testNet, OriginSlot: 0, SegBits: 3, FanoutBits: 2}
	_, err := follow.ReconcileWriterPromotion(ctx, cfg, testHead)
	if err == nil {
		t.Fatal("promoted a head whose seg_bits disagree with the config; want a params-mismatch refusal")
	}
	var mismatch *server.ParamsMismatchError
	if !errors.As(err, &mismatch) {
		t.Errorf("err = %v, want a *server.ParamsMismatchError", err)
	}
	if got := mirrorRoot(t, roots); got.Defined() {
		t.Errorf("root mirror after the mismatch = %s, want it untouched (absent)", got)
	}
	if got := checkpointRoot(t, st); got != root120 {
		t.Errorf("checkpoint after the mismatch = %s, want it intact at %s", got, root120)
	}
}
