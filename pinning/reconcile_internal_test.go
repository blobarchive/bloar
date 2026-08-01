package pinning

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/ipfs/boxo/blockstore"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/multiformats/go-multihash"

	"github.com/blobarchive/bloar/archive"
	"github.com/blobarchive/bloar/catalog"
	"github.com/blobarchive/bloar/schema"
)

// resolver is a blob catalog just big enough to apply refs over.
type resolver struct {
	blobs map[schema.VersionedHash]cid.Cid
}

func (r *resolver) ResolveBlob(_ context.Context, vh schema.VersionedHash) (cid.Cid, bool, error) {
	c, ok := r.blobs[vh]
	return c, ok, nil
}

// row stores blob id's block and returns a RefRow at slot naming it.
func (r *resolver) row(t *testing.T, bs blockstore.Blockstore, slot, id uint64) archive.RefRow {
	t.Helper()
	var vh schema.VersionedHash
	vh[0] = 0x01
	binary.BigEndian.PutUint64(vh[24:], id)

	data := fmt.Appendf(nil, "blob:%d", id)
	c := cid.NewCidV1(cid.Raw, testMultihash(t, string(data)))
	blk, err := blocks.NewBlockWithCid(data, c)
	if err != nil {
		t.Fatalf("framing blob %d: %v", id, err)
	}
	if err := bs.Put(context.Background(), blk); err != nil {
		t.Fatalf("storing blob %d: %v", id, err)
	}
	r.blobs[vh] = c
	return archive.RefRow{Slot: slot, VHs: []schema.VersionedHash{vh}}
}

func testMultihash(t *testing.T, s string) multihash.Multihash {
	t.Helper()
	mh, err := multihash.Sum([]byte(s), multihash.SHA2_256, -1)
	if err != nil {
		t.Fatalf("hashing %q: %v", s, err)
	}
	return mh
}

// halfLedger is a ledger whose removals fail: a pass that reaches them dies
// there, which is where a crash between spec 6.2's add and remove steps would
// leave it.
type halfLedger struct {
	pinLedger
	failRemove bool
}

var errCrash = errors.New("crash")

func (l *halfLedger) Remove(ctx context.Context, head, purpose string, c cid.Cid) error {
	if l.failRemove {
		return errCrash
	}
	return l.pinLedger.Remove(ctx, head, purpose, c)
}

// TestReconcileCrashOrder is the crash safety of spec 6.2: pins are added, and
// only then are stale ones removed. Cut the pass at the seam and the ledger
// over-retains -- it names the old root's pin and the new root's -- which costs
// disk. The other order would cost data: a crash after the removal and before
// the add leaves live blocks unpinned, and the next GC deletes a root the head
// is serving.
func TestReconcileCrashOrder(t *testing.T) {
	ctx := context.Background()
	bs := blockstore.NewBlockstore(dssync.MutexWrap(datastore.NewMapDatastore()))
	kv, err := pebble.Open(filepath.Join(t.TempDir(), "kv"), &pebble.Options{})
	if err != nil {
		t.Fatalf("opening kv: %v", err)
	}
	defer kv.Close()

	cat := &resolver{blobs: map[schema.VersionedHash]cid.Cid{}}
	h, err := archive.New(ctx, archive.Config{Blocks: bs, Resolver: cat}, archive.Params{
		Name: "h", Net: "testnet", OriginSlot: 8, SegBits: 2, FanoutBits: 2,
	})
	if err != nil {
		t.Fatalf("archive.New: %v", err)
	}
	rec, err := NewReconciler(Config{Ledger: catalog.NewLedger(kv)})
	if err != nil {
		t.Fatalf("NewReconciler: %v", err)
	}
	if err := rec.Add(h, Full()); err != nil {
		t.Fatalf("Add: %v", err)
	}

	if _, err := h.ApplyRefs(ctx, []archive.RefRow{cat.row(t, bs, 8, 1)}, 11); err != nil {
		t.Fatalf("ApplyRefs: %v", err)
	}
	if _, err := rec.ReconcileHead(ctx, "h"); err != nil {
		t.Fatalf("ReconcileHead: %v", err)
	}
	oldRoot := h.Root()

	// Swap the root, then crash the pass at the first removal.
	if _, err := h.ApplyRefs(ctx, []archive.RefRow{cat.row(t, bs, 12, 2)}, 20); err != nil {
		t.Fatalf("ApplyRefs: %v", err)
	}
	newRoot := h.Root()
	half := &halfLedger{pinLedger: rec.ledger, failRemove: true}
	rec.ledger = half

	if _, err := rec.ReconcileHead(ctx, "h"); !errors.Is(err, errCrash) {
		t.Fatalf("ReconcileHead = %v, want the injected crash", err)
	}

	// The state a crash leaves: both roots pinned. Nothing reachable was ever
	// unpinned, which is the invariant that matters -- the cost is one stale
	// pin, and the next pass clears it.
	pins := listAll(t, ctx, rec.ledger, "h")
	if !pinned(pins, newRoot, true) {
		t.Error("the new root is not pinned after a crash at the removal step; it was reachable and unpinned, which is what the order exists to prevent")
	}
	if !pinned(pins, oldRoot, true) {
		t.Error("the old root's pin is gone even though the removal step failed")
	}

	// Re-reconcile: converges to exactly the desired set, no repair needed.
	half.failRemove = false
	delta, err := rec.ReconcileHead(ctx, "h")
	if err != nil {
		t.Fatalf("ReconcileHead after the crash: %v", err)
	}
	if delta.Added != 0 || delta.Removed != 1 {
		t.Errorf("the recovery pass = %+v, want only the stale pin removed (the add already survived the crash)", delta)
	}
	pins = listAll(t, ctx, rec.ledger, "h")
	if len(pins) != 1 || !pinned(pins, newRoot, true) {
		t.Errorf("after recovery the ledger holds %v, want exactly the current root", pins)
	}

	// And it stays converged.
	if delta, err := rec.ReconcileHead(ctx, "h"); err != nil || delta != (Delta{}) {
		t.Errorf("the next pass = %+v (err %v), want no churn", delta, err)
	}
}

// TestPlanRewritesChangedFlags: a row whose recursive bit no longer matches the
// policy is re-added rather than left alone. This is how a segment moves
// between purposes as a window slides, and how a policy change takes effect.
func TestPlanRewritesChangedFlags(t *testing.T) {
	c := cid.NewCidV1(cid.DagCBOR, testMultihash(t, "a"))
	desired := []Pin{{Purpose: PurposeWindow, CID: c, Recursive: true}}
	have := []catalog.PinEntry{{Purpose: PurposeWindow, CID: c, Recursive: false}}

	add, remove := plan(desired, have)
	if len(add) != 1 || !add[0].Recursive {
		t.Errorf("plan add = %v, want the row rewritten as recursive", add)
	}
	if len(remove) != 0 {
		t.Errorf("plan remove = %v, want nothing removed: the row is wanted, just with a different flag", remove)
	}
}

// TestPlanEmptyDesired removes everything: a head under no policy holds no pins.
func TestPlanEmptyDesired(t *testing.T) {
	c := cid.NewCidV1(cid.DagCBOR, testMultihash(t, "a"))
	add, remove := plan(nil, []catalog.PinEntry{{Purpose: PurposeRoot, CID: c}})
	if len(add) != 0 || len(remove) != 1 {
		t.Errorf("plan(nil, one row) = add %v, remove %v; want the row removed", add, remove)
	}
}

func listAll(t *testing.T, ctx context.Context, l pinLedger, head string) []catalog.PinEntry {
	t.Helper()
	entries, err := l.ListAll(ctx, head)
	if err != nil {
		t.Fatalf("ListAll: %v", err)
	}
	return entries
}

func pinned(entries []catalog.PinEntry, c cid.Cid, recursive bool) bool {
	for _, e := range entries {
		if e.CID == c && e.Recursive == recursive {
			return true
		}
	}
	return false
}
