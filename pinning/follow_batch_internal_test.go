package pinning

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"

	"github.com/blobarchive/bloar/archive"
	"github.com/blobarchive/bloar/catalog"
	"github.com/blobarchive/bloar/schema"
)

type preparedBatchInternalFixture struct {
	ctx context.Context
	bs  blockstore.Blockstore
	cat *resolver
	rec *Reconciler
	old *archive.Head
}

func newPreparedBatchInternalFixture(t *testing.T) *preparedBatchInternalFixture {
	t.Helper()
	ctx := context.Background()
	bs := blockstore.NewBlockstore(dssync.MutexWrap(datastore.NewMapDatastore()))
	kv, err := pebble.Open(filepath.Join(t.TempDir(), "kv"), &pebble.Options{})
	if err != nil {
		t.Fatalf("opening kv: %v", err)
	}
	t.Cleanup(func() {
		if err := kv.Close(); err != nil {
			t.Errorf("closing kv: %v", err)
		}
	})
	cat := &resolver{blobs: map[schema.VersionedHash]cid.Cid{}}
	old, err := archive.New(ctx, archive.Config{Blocks: bs, Resolver: cat}, archive.Params{
		Name: "h", Net: "testnet", OriginSlot: 8, SegBits: 2, FanoutBits: 2,
	})
	if err != nil {
		t.Fatalf("archive.New: %v", err)
	}
	rec, err := NewReconciler(Config{Ledger: catalog.NewLedger(kv)})
	if err != nil {
		t.Fatalf("NewReconciler: %v", err)
	}
	if err := rec.Add(old, Full()); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if _, err := old.ApplyRefs(ctx, []archive.RefRow{cat.row(t, bs, 8, 1)}, 11); err != nil {
		t.Fatalf("ApplyRefs: %v", err)
	}
	if _, err := rec.ReconcileHead(ctx, "h"); err != nil {
		t.Fatalf("initial ReconcileHead: %v", err)
	}
	return &preparedBatchInternalFixture{ctx: ctx, bs: bs, cat: cat, rec: rec, old: old}
}

func (f *preparedBatchInternalFixture) replacement(t *testing.T) *archive.Head {
	t.Helper()
	head, err := archive.New(f.ctx, archive.Config{Blocks: f.bs, Resolver: f.cat}, archive.Params{
		Name: "h", Net: "testnet", OriginSlot: 12, SegBits: 2, FanoutBits: 2,
	})
	if err != nil {
		t.Fatalf("archive.New replacement: %v", err)
	}
	if _, err := head.ApplyRefs(f.ctx, []archive.RefRow{f.cat.row(t, f.bs, 12, 2)}, 15); err != nil {
		t.Fatalf("replacement ApplyRefs: %v", err)
	}
	return head
}

func TestPreparedWithdrawalTombstoneSurvivesRemovalFailure(t *testing.T) {
	f := newPreparedBatchInternalFixture(t)
	oldRoot := f.old.Root()
	half := &halfLedger{pinLedger: f.rec.ledger, failRemove: true}
	f.rec.ledger = half

	apply, err := f.rec.PrepareSetBatch([]Registration{{Name: "h", Policy: Full()}})
	if err != nil {
		t.Fatalf("PrepareSetBatch: %v", err)
	}
	apply()
	if names := f.rec.Names(); len(names) != 1 || names[0] != "h" {
		t.Fatalf("withdrawal disappeared before reconciliation: %v", names)
	}
	if _, err := f.rec.ReconcileHead(f.ctx, "h"); !errors.Is(err, errCrash) {
		t.Fatalf("ReconcileHead with failing removal = %v, want %v", err, errCrash)
	}
	if names := f.rec.Names(); len(names) != 1 || names[0] != "h" {
		t.Fatalf("failed drain removed its retry tombstone: %v", names)
	}
	if pins := listAll(t, f.ctx, f.rec.ledger, "h"); len(pins) != 1 || !pinned(pins, oldRoot, true) {
		t.Fatalf("failed drain changed old pins: %#v", pins)
	}

	half.failRemove = false
	delta, err := f.rec.ReconcileHead(f.ctx, "h")
	if err != nil {
		t.Fatalf("retrying withdrawal reconciliation: %v", err)
	}
	if delta.Added != 0 || delta.Removed != 1 {
		t.Fatalf("withdrawal retry delta = %+v, want one removal", delta)
	}
	if names := f.rec.Names(); len(names) != 0 {
		t.Fatalf("successful drain retained tombstone: %v", names)
	}
	if pins := listAll(t, f.ctx, f.rec.ledger, "h"); len(pins) != 0 {
		t.Fatalf("successful withdrawal retained pins: %#v", pins)
	}
}

func TestPreparedReplacementKeepsAddBeforeRemoveCrashSafety(t *testing.T) {
	f := newPreparedBatchInternalFixture(t)
	oldRoot := f.old.Root()
	replacement := f.replacement(t)
	half := &halfLedger{pinLedger: f.rec.ledger, failRemove: true}
	f.rec.ledger = half

	apply, err := f.rec.PrepareSetBatch([]Registration{{Name: "h", Head: replacement, Policy: Full()}})
	if err != nil {
		t.Fatalf("PrepareSetBatch: %v", err)
	}
	apply()
	if _, err := f.rec.ReconcileHead(f.ctx, "h"); !errors.Is(err, errCrash) {
		t.Fatalf("ReconcileHead with failing removal = %v, want %v", err, errCrash)
	}
	pins := listAll(t, f.ctx, f.rec.ledger, "h")
	if !pinned(pins, replacement.Root(), true) {
		t.Fatalf("new root was not added before old-root removal failed: %#v", pins)
	}
	if !pinned(pins, oldRoot, true) {
		t.Fatalf("old root was removed despite injected removal failure: %#v", pins)
	}

	half.failRemove = false
	delta, err := f.rec.ReconcileHead(f.ctx, "h")
	if err != nil {
		t.Fatalf("retrying replacement reconciliation: %v", err)
	}
	if delta.Added != 0 || delta.Removed != 1 {
		t.Fatalf("replacement retry delta = %+v, want one stale removal", delta)
	}
	pins = listAll(t, f.ctx, f.rec.ledger, "h")
	if len(pins) != 1 || !pinned(pins, replacement.Root(), true) {
		t.Fatalf("replacement did not converge to new root: %#v", pins)
	}
}
