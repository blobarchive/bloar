package pinning_test

import (
	"slices"
	"testing"
	"time"

	"github.com/ipfs/go-cid"

	"github.com/blobarchive/bloar/archive"
	"github.com/blobarchive/bloar/pinning"
	"github.com/blobarchive/bloar/schema"
)

// This file is spec 9's known window (a): a blob is ingested by one request and
// referenced by another, and a GC in between used to sweep it.

// TestStagingSurvivesGCBeforeRefs is the window itself. A blob is put, no refs
// name it yet, and a GC runs: before the staging pins it was swept, and the
// indexer found out at its refs POST (a 409, spec 5.1 step 4).
func TestStagingSurvivesGCBeforeRefs(t *testing.T) {
	f := newFixture(t, withStaging(time.Hour))
	h := f.head("all", pinning.Full())
	f.apply(h, 11, f.row(9, 1))
	f.reconcileAll()

	// Blob 2 is put and nothing references it. This is the gap between spec
	// 7.2's two calls, which is where the whole problem lives.
	f.stage(2)

	stats := f.runGC()
	if stats.Staged != 1 {
		t.Errorf("gc marked %d staging pins, want 1: an unreferenced blob's pin is what keeps it", stats.Staged)
	}
	if stats.Expired != 0 {
		t.Errorf("gc expired %d staging pins, want 0: the TTL is an hour and no time has passed", stats.Expired)
	}

	// The blob is still there, which is the whole point.
	f.expect().blobs(1, 2).index(h).check()
}

// TestStagingDroppedWhenRefsLand is the other half: once the refs naming a blob
// are applied, the head's own pins retain it and the staging row is dead weight.
func TestStagingDroppedWhenRefsLand(t *testing.T) {
	f := newFixture(t, withStaging(time.Hour))
	h := f.head("all", pinning.Full())

	f.stage(1)
	if got := len(f.stagingPins()); got != 1 {
		t.Fatalf("staging rows after a put: %d, want 1", got)
	}

	// The refs land. This is what server.Heads does after a successful
	// ApplyRefs whose root is durable.
	row := f.row(9, 1)
	f.apply(h, 11, row)
	if err := f.staging.DropRefs(f.ctx, []archive.RefRow{row}); err != nil {
		t.Fatalf("DropRefs: %v", err)
	}

	if got := f.stagingPins(); len(got) != 0 {
		t.Errorf("staging rows after the refs landed: %d, want 0 -- the head's pins retain the blob now: %v",
			len(got), got)
	}

	// And the blob survives anyway, via the head's pins rather than its own.
	f.reconcileAll()
	stats := f.runGC()
	if stats.Staged != 0 {
		t.Errorf("gc marked %d staging pins, want 0", stats.Staged)
	}
	f.expect().blobs(1).index(h).check()
}

// TestStagingExpiredIsSwept is the leak the TTL exists to stop: a put whose refs
// never arrive must not retain its blobs forever.
func TestStagingExpiredIsSwept(t *testing.T) {
	f := newFixture(t, withStaging(time.Hour))
	h := f.head("all", pinning.Full())
	f.apply(h, 11, f.row(9, 1))
	f.reconcileAll()

	f.stage(2) // abandoned: no refs will ever name blob 2.

	// Still inside the TTL: the blob survives, because a slow indexer is not an
	// abandoned one.
	f.advance(59 * time.Minute)
	if stats := f.runGC(); stats.Expired != 0 || stats.Staged != 1 {
		t.Fatalf("gc inside the TTL: expired=%d staged=%d, want 0 and 1", stats.Expired, stats.Staged)
	}
	f.expect().blobs(1, 2).index(h).check()

	// Past it: the row goes, and the blob goes with it, in the same run.
	f.advance(2 * time.Minute)
	stats := f.runGC()
	if stats.Expired != 1 {
		t.Errorf("gc past the TTL expired %d staging pins, want 1", stats.Expired)
	}
	if stats.Staged != 0 {
		t.Errorf("gc past the TTL marked %d staging pins, want 0: the row was dropped before the mark", stats.Staged)
	}
	if stats.Swept != 1 {
		t.Errorf("gc past the TTL swept %d blocks, want 1 (blob 2)", stats.Swept)
	}
	if got := len(f.stagingPins()); got != 0 {
		t.Errorf("staging rows after expiry: %d, want 0", got)
	}
	f.expect().blobs(1).index(h).check()
}

// TestStagingExpiryIsExtendedByRePut checks that a re-put of a blob whose row is
// nearly due gets the fresh TTL rather than racing the old expiry. An indexer
// retrying a batch is the normal case, not an exotic one.
func TestStagingExpiryIsExtendedByRePut(t *testing.T) {
	f := newFixture(t, withStaging(time.Hour))
	f.head("all", pinning.Full())

	f.stage(1)
	f.advance(59 * time.Minute)
	f.stage(1) // the same blob, put again.

	f.advance(2 * time.Minute) // past the first expiry, inside the second.
	if stats := f.runGC(); stats.Expired != 0 || stats.Staged != 1 {
		t.Fatalf("after a re-put: expired=%d staged=%d, want 0 and 1 -- the row should carry the newer expiry",
			stats.Expired, stats.Staged)
	}
}

// TestReconcilerNeverTouchesStagingRows is the rule the reserved head exists to
// make enforceable. A reconciliation pass computes a head's desired pins and
// removes every row that is not in it; a pass over the staging rows would
// therefore delete all of them, and the next GC would sweep every blob that had
// been put but not yet referenced.
func TestReconcilerNeverTouchesStagingRows(t *testing.T) {
	f := newFixture(t, withStaging(time.Hour))
	h := f.head("all", pinning.Full())
	f.apply(h, 11, f.row(9, 1))
	f.stage(2)

	// A full pass over every registered head, which is what the timer and the
	// root-swap trigger both do.
	f.reconcileAll()

	if got := len(f.stagingPins()); got != 1 {
		t.Errorf("staging rows after reconciliation: %d, want 1 -- reconciliation must not see the reserved head", got)
	}

	// And the reserved head is not reachable through the reconciler at all: it
	// is not in Names(), so nothing iterating heads can reach its rows.
	if names := f.rec.Names(); slices.Contains(names, pinning.StagingHead) {
		t.Errorf("Reconciler.Names() = %v, must never contain the reserved staging head %q", names, pinning.StagingHead)
	}
}

// TestStagingHeadCannotCollideWithARealHead is the load-bearing claim behind the
// whole design: the reserved name is one spec 3.1's grammar cannot produce, so
// no head -- configured, adopted from a document, or built by any writer -- can
// ever own those ledger rows, and the reconciler can never be handed one that
// would make it walk them.
//
// It is checked against schema's actual validator rather than against a second
// copy of the regexp, because a copy is a thing that can drift.
func TestStagingHeadCannotCollideWithARealHead(t *testing.T) {
	head := &schema.Head{Name: pinning.StagingHead, Net: "mainnet", SegBits: 9, FanoutBits: 8}
	if err := head.Validate(); err == nil {
		t.Fatalf("schema.Head accepted the name %q; the staging ledger rows are only safe because spec 3.1's "+
			"grammar ([a-z0-9][a-z0-9-]*) cannot produce it, and it just did", pinning.StagingHead)
	}

	// The control: the same head under a legal name validates, so the failure
	// above is about the name and not about the rest of the object.
	head.Name = "all"
	if err := head.Validate(); err != nil {
		t.Fatalf("the same head named %q does not validate (%v); the test above proves nothing", head.Name, err)
	}
}

// TestReconcilerCannotBeHandedTheReservedHead closes the loop the test above
// opens. The reservation is only worth anything if there is no way to construct
// a *archive.Head with the reserved name, because such a head would be
// registrable, and a registered one would have its rows reconciled -- which
// means every staging row removed, and every not-yet-referenced blob swept on
// the next GC.
//
// So this asserts the engine refuses to build one. That refusal is what makes
// checkStagingName in Add and Set unreachable rather than merely correct: they
// stay as belt-and-braces for a future seam that hands a head over some other
// way, and this test is the reason they have nothing to catch today.
func TestReconcilerCannotBeHandedTheReservedHead(t *testing.T) {
	f := newFixture(t)

	_, err := archive.New(f.ctx, archive.Config{Blocks: f.bs, Resolver: f.cat}, archive.Params{
		Name:       pinning.StagingHead,
		Net:        "testnet",
		OriginSlot: testOrigin,
		SegBits:    testSegBits,
		FanoutBits: testFanoutBits,
	})
	if err == nil {
		t.Fatalf("archive.New built a head named %q; that head could be registered with the reconciler, whose "+
			"next pass would delete every staging row -- the reservation depends on this being impossible",
			pinning.StagingHead)
	}
}

// TestStagingPinsAreDirect checks the shape of the row. A blob is a leaf (spec
// 2), so a recursive staging pin would claim a traversal that has nothing to
// traverse -- and would cost GC's mark a decode of 128 KiB of blob bytes per
// staged blob to discover that.
func TestStagingPinsAreDirect(t *testing.T) {
	f := newFixture(t, withStaging(time.Hour))
	f.head("all", pinning.Full())
	f.stage(1)

	rows := f.stagingPins()
	if len(rows) != 1 {
		t.Fatalf("staging rows: %d, want 1", len(rows))
	}
	row := rows[0]
	if row.Purpose != pinning.PurposeStaging {
		t.Errorf("staging row purpose = %q, want %q", row.Purpose, pinning.PurposeStaging)
	}
	if row.Recursive {
		t.Error("staging row is recursive; a blob is a leaf and has nothing to reach")
	}
	if !row.Expires() {
		t.Error("staging row carries no expiry; an abandoned put would retain its blobs forever")
	}
	if want := epoch.Add(time.Hour); !row.Expiry.Equal(want) {
		t.Errorf("staging row expiry = %s, want %s (put at %s, TTL 1h)", row.Expiry, want, epoch)
	}
}

// TestStagingDropRefsToleratesUnknownVH checks that dropping is not a place a
// successful mutation can fail. A vh that does not resolve is skipped: the row
// it would have named does not exist, and the batch is already applied.
func TestStagingDropRefsToleratesUnknownVH(t *testing.T) {
	f := newFixture(t, withStaging(time.Hour))
	f.head("all", pinning.Full())
	f.stage(1)

	rows := []archive.RefRow{{Slot: 9, VHs: []schema.VersionedHash{mkVH(1), mkVH(99)}}}
	if err := f.staging.DropRefs(f.ctx, rows); err != nil {
		t.Fatalf("DropRefs with an unresolvable vh: %v; it must skip rather than fail", err)
	}
	if got := len(f.stagingPins()); got != 0 {
		t.Errorf("staging rows after DropRefs: %d, want 0 -- the resolvable one should still have been dropped", got)
	}
}

// TestStagingProtectsAFetchedIndexNode is the follower half of window (a) (spec
// 11.3): the fetch pass makes a block durable before the reconcile that pins it
// lands, and a GC in that gap must keep it on the staging pin alone. Unlike
// ingest, which only ever stages blobs, a follower stages dag-cbor index nodes
// too -- so this checks a staged block that is not a leaf survives the mark, and
// that Drop (the follower's by-CID handoff) releases it once the pass is done.
func TestStagingProtectsAFetchedIndexNode(t *testing.T) {
	f := newFixture(t, withStaging(time.Hour))
	h := f.head("all", pinning.Full())
	f.apply(h, 11, f.row(9, 1))
	f.reconcileAll()

	// A dag-cbor index node the fetch pass fetched and staged, of a root this
	// node has adopted but not yet reconciled: durable, and reached by no head
	// pin. Without the staging pin the GC below would sweep it, and the reconcile
	// that pins its root would then name a block that is gone -- the wedge.
	node := f.putIndexNode(2)
	if err := f.staging.Pin(f.ctx, []cid.Cid{node}); err != nil {
		t.Fatalf("staging the fetched index node: %v", err)
	}

	stats := f.runGC()
	if stats.Staged != 1 {
		t.Errorf("gc marked %d staging pins, want 1: the staged index node is what keeps it", stats.Staged)
	}
	if has, _ := f.bs.Has(f.ctx, node); !has {
		t.Fatal("a staged, freshly-fetched index node was swept in the fetch-then-pin window")
	}
	f.expect().index(h).blobs(1).cid(node, "the staged index node").check()

	// The pass finishes and drops the pin by CID -- an index node has no
	// versioned hash, so DropRefs cannot; Drop is the follower's handoff. Here
	// the node is a synthetic orphan no real head reaches, so the next GC sweeps
	// it: the point is that the drop lands and the mark stops keeping it.
	if err := f.staging.Drop(f.ctx, []cid.Cid{node}); err != nil {
		t.Fatalf("Drop: %v", err)
	}
	stats = f.runGC()
	if stats.Staged != 0 {
		t.Errorf("gc marked %d staging pins after the drop, want 0", stats.Staged)
	}
	if has, _ := f.bs.Has(f.ctx, node); has {
		t.Error("the index node survived after its staging pin was dropped and no head reaches it")
	}
	f.expect().index(h).blobs(1).check()
}

// TestStagingDisabledIsTheOldBehaviour pins what a node without staging does, so
// that "optional" means what it says: the GC runs, marks nothing extra, and the
// unreferenced blob is swept exactly as spec 9's window (a) described.
func TestStagingDisabledIsTheOldBehaviour(t *testing.T) {
	f := newFixture(t) // no withStaging.
	h := f.head("all", pinning.Full())
	f.apply(h, 11, f.row(9, 1))
	f.reconcileAll()
	f.putBlob(2) // put, unreferenced, and nothing pins it.

	stats := f.runGC()
	if stats.Staged != 0 || stats.Expired != 0 {
		t.Errorf("gc with no staging: staged=%d expired=%d, want 0 and 0", stats.Staged, stats.Expired)
	}
	f.expect().blobs(1).index(h).check()
}
