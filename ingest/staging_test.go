package ingest_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/ipfs/go-cid"

	"github.com/blobarchive/bloar/catalog"
	"github.com/blobarchive/bloar/ingest"
	"github.com/blobarchive/bloar/pinning"
	"github.com/blobarchive/bloar/store"
)

// This file covers the ingest end of spec 9's window (a): a put pins what it
// accepts, so a GC between the blobs POST and the refs POST cannot sweep it.

// newStagingIngester wires an Ingester with real staging pins over a fresh
// store, the way cmd/bloard does.
func newStagingIngester(t *testing.T, ttl time.Duration) (*ingest.Ingester, *store.Store, *pinning.Staging) {
	t.Helper()
	s := openStore(t, t.TempDir())
	cat := catalog.New(s.KV())
	staging, err := pinning.NewStaging(pinning.StagingConfig{
		Ledger:   catalog.NewLedger(s.KV()),
		Resolver: cat,
		TTL:      ttl,
	})
	if err != nil {
		t.Fatalf("pinning.NewStaging: %v", err)
	}
	i, err := ingest.New(ingest.Config{Blocks: s.Blocks(), Catalog: cat, Staging: staging})
	if err != nil {
		t.Fatalf("ingest.New: %v", err)
	}
	return i, s, staging
}

// TestPutBlobsStagesEveryBlob is the guarantee POST /bloar/v1/blobs now makes:
// by the time it answers, everything it accepted is pinned.
func TestPutBlobsStagesEveryBlob(t *testing.T) {
	ctx := context.Background()
	i, _, staging := newStagingIngester(t, time.Hour)

	put, err := i.PutBlobs(ctx, bodyOf(makeBlob(1), makeBlob(2), makeBlob(3)))
	if err != nil {
		t.Fatalf("PutBlobs: %v", err)
	}

	rows, err := staging.List(ctx)
	if err != nil {
		t.Fatalf("Staging.List: %v", err)
	}
	if len(rows) != len(put) {
		t.Fatalf("staging rows after a 3-blob put: %d, want %d", len(rows), len(put))
	}

	staged := map[cid.Cid]catalog.PinEntry{}
	for _, r := range rows {
		staged[r.CID] = r
	}
	for k, p := range put {
		row, ok := staged[p.CID]
		if !ok {
			t.Errorf("blob %d (%s) was accepted but not staged; a GC before its refs would sweep it", k, p.CID)
			continue
		}
		if !row.Expires() {
			t.Errorf("blob %d's staging row carries no expiry", k)
		}
	}
}

// TestPutBlobsStagingIsIdempotent replays a put, as spec 7.2 requires every
// ingest call to tolerate.
func TestPutBlobsStagingIsIdempotent(t *testing.T) {
	ctx := context.Background()
	i, _, staging := newStagingIngester(t, time.Hour)

	body := bodyOf(makeBlob(1), makeBlob(2))
	for attempt := range 3 {
		if _, err := i.PutBlobs(ctx, body); err != nil {
			t.Fatalf("PutBlobs attempt %d: %v", attempt, err)
		}
		rows, err := staging.List(ctx)
		if err != nil {
			t.Fatalf("Staging.List: %v", err)
		}
		if len(rows) != 2 {
			t.Fatalf("staging rows after put attempt %d: %d, want 2 -- a replay must rewrite the rows, not "+
				"accumulate them", attempt, len(rows))
		}
	}
}

// TestPutBlobsStagesNothingOnRejection checks that a refused body leaves no
// rows. It stores nothing (the batch is verified before anything is written), so
// a staging row would be a claim that this node retains a block it never wrote,
// left behind until the TTL swept it up.
func TestPutBlobsStagesNothingOnRejection(t *testing.T) {
	ctx := context.Background()
	i, _, staging := newStagingIngester(t, time.Hour)

	if _, err := i.PutBlobs(ctx, bodyOf(makeBlob(1), makeInvalidBlob())); err == nil {
		t.Fatal("PutBlobs accepted a body with an invalid blob in it")
	}
	rows, err := staging.List(ctx)
	if err != nil {
		t.Fatalf("Staging.List: %v", err)
	}
	if len(rows) != 0 {
		t.Errorf("staging rows after a rejected put: %d, want 0 -- they would name blocks that were never "+
			"written, and GC's mark fails closed on a missing block", len(rows))
	}
}

// TestPutBlobsFailsWhenStagingFails is the deliberate choice to make a staging
// failure fatal to the request.
//
// A 200 whose staging pin did not land is a receipt for a put that a GC may
// erase, and the indexer would only find out at its refs POST -- which is the
// exact failure the pins exist to remove. Better to fail now, into a store
// where the blocks already are, and let the idempotent retry re-do it.
func TestPutBlobsFailsWhenStagingFails(t *testing.T) {
	ctx := context.Background()
	s := openStore(t, t.TempDir())
	cat := catalog.New(s.KV())
	i, err := ingest.New(ingest.Config{Blocks: s.Blocks(), Catalog: cat, Staging: failingStaging{}})
	if err != nil {
		t.Fatalf("ingest.New: %v", err)
	}

	if _, err := i.PutBlobs(ctx, bodyOf(makeBlob(1))); err == nil {
		t.Fatal("PutBlobs answered success when the staging pin failed; the caller would take that as a promise " +
			"the blob will still be there at the refs POST")
	}
}

// failingStaging is a Staging that never manages to pin.
type failingStaging struct{}

func (failingStaging) Pin(context.Context, []cid.Cid) error { return errStagingBroken }

var errStagingBroken = &stagingError{}

type stagingError struct{}

func (*stagingError) Error() string { return "staging is broken" }

// TestPutBlobsHoldsTheGate is the ingest half of the mutation/GC exclusion fix: the exclusion of spec
// 9 is a property of putting blobs, not of receiving a POST, so an Ingester
// built without cmd/bloard's middleware still takes the gate.
func TestPutBlobsHoldsTheGate(t *testing.T) {
	ctx := context.Background()
	s := openStore(t, t.TempDir())
	cat := catalog.New(s.KV())
	gate := &recordingGate{}
	i, err := ingest.New(ingest.Config{Blocks: s.Blocks(), Catalog: cat, Gate: gate})
	if err != nil {
		t.Fatalf("ingest.New: %v", err)
	}

	if _, err := i.PutBlobs(ctx, bodyOf(makeBlob(1))); err != nil {
		t.Fatalf("PutBlobs: %v", err)
	}
	if got := gate.entered(); got != 1 {
		t.Errorf("gate entered %d times during a put, want 1", got)
	}
	if !gate.balanced() {
		t.Error("the gate was not left after the put; a GC would wait on it forever")
	}

	// A rejected body takes and releases it too: the rejection happens inside
	// the gate, and an unbalanced Leave on the error path would deadlock GC.
	if _, err := i.PutBlobs(ctx, []byte("not a whole number of blobs")); err == nil {
		t.Fatal("PutBlobs accepted a misframed body")
	}
	if got := gate.entered(); got != 2 {
		t.Errorf("gate entered %d times after a rejected put, want 2", got)
	}
	if !gate.balanced() {
		t.Error("the gate was not left after a rejected put")
	}
}

// recordingGate counts Enter/Leave.
type recordingGate struct {
	mu    sync.Mutex
	in    int
	out   int
	depth int
}

func (g *recordingGate) Enter() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.in++
	g.depth++
}

func (g *recordingGate) Leave() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.out++
	g.depth--
}

func (g *recordingGate) entered() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.in
}

func (g *recordingGate) balanced() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.depth == 0
}

// TestRebuildLeavesStagingPinsAlone checks the one interaction between the two
// rebuildable structures of spec 6.
//
// A rebuild reconstructs the blob catalog from the blocks on disk (spec 6.1). It
// must not touch the pin ledger: the ledger is rebuilt by reconciliation
// instead, and the staging rows are the part of it that reconciliation does not
// own. A rebuild that dropped them would unpin every blob put but not yet
// referenced; one that invented them would retain blobs nobody asked for.
// Neither happens, because Rebuild only ever writes catalog keys -- and this is
// what says so.
func TestRebuildLeavesStagingPinsAlone(t *testing.T) {
	ctx := context.Background()
	i, s, staging := newStagingIngester(t, time.Hour)
	cat := catalog.New(s.KV())

	if _, err := i.PutBlobs(ctx, bodyOf(makeBlob(1), makeBlob(2))); err != nil {
		t.Fatalf("PutBlobs: %v", err)
	}
	before, err := staging.List(ctx)
	if err != nil {
		t.Fatalf("Staging.List: %v", err)
	}
	if len(before) != 2 {
		t.Fatalf("staging rows before the rebuild: %d, want 2", len(before))
	}

	if _, err := ingest.Rebuild(ctx, ingest.Config{Blocks: s.Blocks(), Catalog: cat}); err != nil {
		t.Fatalf("Rebuild: %v", err)
	}

	after, err := staging.List(ctx)
	if err != nil {
		t.Fatalf("Staging.List: %v", err)
	}
	if len(after) != len(before) {
		t.Fatalf("staging rows after the rebuild: %d, want %d unchanged", len(after), len(before))
	}
	for k := range after {
		if after[k].CID != before[k].CID || !after[k].Expiry.Equal(before[k].Expiry) {
			t.Errorf("staging row %d changed across the rebuild: %+v, want %+v", k, after[k], before[k])
		}
	}
}
