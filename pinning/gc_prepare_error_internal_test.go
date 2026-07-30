package pinning

import (
	"context"
	"errors"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/ipfs/boxo/blockstore"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"

	"github.com/blobarchive/bloar/archive"
	"github.com/blobarchive/bloar/catalog"
	"github.com/blobarchive/bloar/metrics"
	"github.com/blobarchive/bloar/schema"
)

var errPinSnapshot = errors.New("injected pin snapshot failure")

// failListLedger lets the reconciliation flush complete, then fails the
// separate pin snapshot which follows staging expiry in GC.prepare.
type failListLedger struct {
	pinLedger
	failAt int
	calls  int
}

func (l *failListLedger) ListAll(ctx context.Context, head string) ([]catalog.PinEntry, error) {
	l.calls++
	if l.calls == l.failAt {
		return nil, errPinSnapshot
	}
	return l.pinLedger.ListAll(ctx, head)
}

type prepareResolver struct {
	blobs map[schema.VersionedHash]cid.Cid
}

func (r *prepareResolver) ResolveBlob(_ context.Context, vh schema.VersionedHash) (cid.Cid, bool, error) {
	c, ok := r.blobs[vh]
	return c, ok, nil
}

func TestGCPrepareFailurePreservesExpiredAccounting(t *testing.T) {
	ctx := context.Background()
	bs := blockstore.NewBlockstore(dssync.MutexWrap(datastore.NewMapDatastore()))
	kv, err := pebble.Open(filepath.Join(t.TempDir(), "kv"), &pebble.Options{})
	if err != nil {
		t.Fatalf("opening kv: %v", err)
	}
	t.Cleanup(func() { _ = kv.Close() })

	ledger := catalog.NewLedger(kv)
	resolver := &prepareResolver{blobs: map[schema.VersionedHash]cid.Cid{}}
	rec, err := NewReconciler(Config{Ledger: ledger})
	if err != nil {
		t.Fatalf("NewReconciler: %v", err)
	}
	head, err := archive.New(ctx, archive.Config{Blocks: bs, Resolver: resolver}, archive.Params{
		Name: "all", Net: "testnet", OriginSlot: 8, SegBits: 2, FanoutBits: 2,
	})
	if err != nil {
		t.Fatalf("archive.New: %v", err)
	}
	if err := rec.Add(head, None()); err != nil {
		t.Fatalf("Reconciler.Add: %v", err)
	}

	now := time.Date(2026, 7, 21, 0, 0, 0, 0, time.UTC)
	staging, err := NewStaging(StagingConfig{
		Ledger: ledger, Resolver: resolver, TTL: time.Hour, Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("NewStaging: %v", err)
	}
	blob := blocks.NewBlock([]byte("abandoned staged blob"))
	if err := bs.Put(ctx, blob); err != nil {
		t.Fatalf("storing staged blob: %v", err)
	}
	if err := staging.Pin(ctx, []cid.Cid{blob.Cid()}); err != nil {
		t.Fatalf("staging blob: %v", err)
	}
	now = now.Add(2 * time.Hour)

	// prepare first flushes reconciliation (ListAll call 1), then expires the
	// staging row, then snapshots head pins (call 2). Fail exactly there.
	failing := &failListLedger{pinLedger: ledger, failAt: 2}
	rec.ledger = failing
	mx := metrics.New()
	mx.StagingPins(7) // sentinel: an unobserved zero must not replace this.
	gc, err := NewGC(GCConfig{Blocks: bs, Reconciler: rec, Staging: staging, Metrics: mx})
	if err != nil {
		t.Fatalf("NewGC: %v", err)
	}

	stats, err := gc.Run(ctx)
	if !errors.Is(err, errPinSnapshot) {
		t.Fatalf("GC.Run error = %v, want %v", err, errPinSnapshot)
	}
	if stats.Expired != 1 {
		t.Fatalf("GC.Run expired = %d, want 1 after successful expiry", stats.Expired)
	}
	if stats.stagingObserved {
		t.Fatal("failed pin snapshot reported a staging-row observation")
	}
	if rows, err := staging.List(ctx); err != nil || len(rows) != 0 {
		t.Fatalf("staging rows after expiry: len=%d err=%v, want zero/nil", len(rows), err)
	}

	body := scrapeMetrics(t, mx)
	if got := metricSample(t, body, "bloar_staging_pins"); got != 7 {
		t.Errorf("bloar_staging_pins = %v, want prior observed value 7", got)
	}
	if got := metricSample(t, body, "bloar_staging_expired_total"); got != 1 {
		t.Errorf("bloar_staging_expired_total = %v, want 1", got)
	}
}

func scrapeMetrics(t *testing.T, mx *metrics.Metrics) string {
	t.Helper()
	req := httptest.NewRequest("GET", "/metrics", nil)
	rec := httptest.NewRecorder()
	metrics.Handler(mx, nil).ServeHTTP(rec, req)
	if rec.Code != 200 {
		t.Fatalf("metrics scrape status = %d, want 200", rec.Code)
	}
	return rec.Body.String()
}

func metricSample(t *testing.T, body, name string) float64 {
	t.Helper()
	for line := range strings.SplitSeq(body, "\n") {
		fields := strings.Fields(line)
		if len(fields) != 2 || fields[0] != name {
			continue
		}
		v, err := strconv.ParseFloat(fields[1], 64)
		if err != nil {
			t.Fatalf("parsing %s sample %q: %v", name, fields[1], err)
		}
		return v
	}
	t.Fatalf("metric %s not found in scrape", name)
	return 0
}
