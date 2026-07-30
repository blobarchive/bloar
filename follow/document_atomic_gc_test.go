package follow_test

import (
	"errors"
	"testing"

	"github.com/ipfs/go-cid"

	"github.com/blobarchive/bloar/follow"
	"github.com/blobarchive/bloar/pinning"
)

// TestRevisionedDocumentBatchSurvivesGCBeforeAndAfterExposure exercises the
// production embedded collector on both sides of one two-head AdoptBatch. A
// collection immediately before admission moves the closure-generation token;
// admission must prove the complete new pair against that stable token. The
// next collection reconciles the already-visible pair before marking, keeps both
// current roots, and reclaims roots which belong only to the superseded pair.
func TestRevisionedDocumentBatchSurvivesGCBeforeAndAfterExposure(t *testing.T) {
	w := newWriter(t)
	docs := newDocServer(t)
	finalizedA := buildDocumentHead(t, w, testHandoffHead, 96, 103, testSegBits, testFanout)
	mutableA := buildDocumentHead(t, w, testHead, 96, 103, testSegBits, testFanout)
	finalizedB := buildDocumentHead(t, w, testHandoffHead, 96, 111, testSegBits, testFanout)
	mutableB := buildDocumentHead(t, w, testHead, 104, 111, testSegBits, testFanout)
	for _, pair := range []struct {
		old cid.Cid
		new cid.Cid
	}{{finalizedA.Root(), finalizedB.Root()}, {mutableA.Root(), mutableB.Root()}} {
		if pair.old.Equals(pair.new) {
			t.Fatalf("fixture did not build distinct generations: %s", pair.old)
		}
	}

	f := documentFollower(t, w, docs, nil)
	docs.set(sign(t, w.key, documentPair(t, w, mutableA, finalizedA, 1)))
	f.poll()
	f.reconcile()

	gc, err := pinning.NewGC(pinning.GCConfig{
		Blocks: f.store.Blocks(), Reconciler: f.rec, Staging: f.staging, Fetch: f.f.GCFetch(),
	})
	if err != nil {
		t.Fatalf("constructing production-shaped follower GC: %v", err)
	}
	before, err := gc.Run(t.Context())
	if err != nil {
		t.Fatalf("GC immediately before batch exposure: %v", err)
	}
	if before.Scanned == 0 {
		t.Fatal("GC immediately before batch exposure did not perform a real mark/sweep")
	}
	for _, root := range []cid.Cid{finalizedA.Root(), mutableA.Root()} {
		if !f.hasLocally(root) {
			t.Fatalf("pre-admission GC swept a selected root %s", root)
		}
	}

	docs.set(sign(t, w.key, documentPair(t, w, mutableB, finalizedB, 2)))
	f.poll()
	requireSelectedRoot(t, f.heads, testHandoffHead, finalizedB.Root())
	requireSelectedRoot(t, f.heads, testHead, mutableB.Root())
	for _, root := range []cid.Cid{finalizedB.Root(), mutableB.Root()} {
		if !f.hasLocally(root) {
			t.Fatalf("newly exposed pair is missing current root %s before GC", root)
		}
	}
	for _, root := range []cid.Cid{finalizedA.Root(), mutableA.Root()} {
		if !f.hasLocally(root) {
			t.Fatalf("superseded root %s disappeared before the collector ran", root)
		}
	}

	after, err := gc.Run(t.Context())
	if err != nil {
		t.Fatalf("GC immediately after batch exposure: %v", err)
	}
	if after.Swept == 0 {
		t.Fatal("post-exposure GC swept no superseded blocks")
	}
	for _, root := range []cid.Cid{finalizedB.Root(), mutableB.Root()} {
		if !f.hasLocally(root) {
			t.Fatalf("post-exposure GC swept current root %s", root)
		}
	}
	for _, root := range []cid.Cid{finalizedA.Root(), mutableA.Root()} {
		if f.hasLocally(root) {
			t.Fatalf("post-exposure GC retained superseded-only root %s", root)
		}
	}
}

// TestRevisionedDocumentCrashAfterDurableBatchResumesWholeNewPair is the
// checkpoint-to-exposure crash seam. The panic is a process-crash surrogate:
// it fires after the synced Pebble batch commits but before the reconciler,
// follower state, registry, or publication pointer changes. A fresh follower
// over the same KV must resume the complete v3 group and expose new/new, never
// infer or retain the old serving pair from the stale in-memory process.
func TestRevisionedDocumentCrashAfterDurableBatchResumesWholeNewPair(t *testing.T) {
	w := newWriter(t)
	docs := newDocServer(t)
	finalizedA := buildDocumentHead(t, w, testHandoffHead, 96, 103, testSegBits, testFanout)
	mutableA := buildDocumentHead(t, w, testHead, 96, 103, testSegBits, testFanout)
	finalizedB := buildDocumentHead(t, w, testHandoffHead, 96, 111, testSegBits, testFanout)
	mutableB := buildDocumentHead(t, w, testHead, 104, 111, testSegBits, testFanout)

	f := documentFollower(t, w, docs, nil)
	docs.set(sign(t, w.key, documentPair(t, w, mutableA, finalizedA, 1)))
	f.poll()
	f.reconcile()

	crash := errors.New("injected process crash after durable document commit")
	follow.SetBeforeExposeHook(func() { panic(crash) })
	t.Cleanup(func() { follow.SetBeforeExposeHook(nil) })
	docs.set(sign(t, w.key, documentPair(t, w, mutableB, finalizedB, 2)))
	var recovered any
	func() {
		defer func() { recovered = recover() }()
		_ = f.f.Poll(t.Context())
	}()
	follow.SetBeforeExposeHook(nil)
	if recovered != crash {
		t.Fatalf("crash seam recovered %v, want sentinel %v", recovered, crash)
	}

	// The crashed process still exposes old/old, while its one durable batch is
	// wholly new. This is the only split allowed at the seam; no mixed member is
	// ever visible, and the durable group is sufficient for a fresh process.
	requireSelectedRoot(t, f.heads, testHandoffHead, finalizedA.Root())
	requireSelectedRoot(t, f.heads, testHead, mutableA.Root())
	for _, state := range []struct {
		name string
		root cid.Cid
	}{{testHandoffHead, finalizedB.Root()}, {testHead, mutableB.Root()}} {
		requireMirror(t, f.roots, state.name, state.root)
		requireCheckpointRevision(t, f, state.name, 2)
	}

	next := f.restart(t, w, func(c *follow.Config) {
		c.URL = docs.url
		configureMutableFollower(c, 32)
	})
	if err := next.Resume(t.Context()); err != nil {
		t.Fatalf("Resume after durable-batch crash: %v", err)
	}
	requireSelectedRoot(t, f.heads, testHandoffHead, finalizedB.Root())
	requireSelectedRoot(t, f.heads, testHead, mutableB.Root())
	for _, name := range []string{testHandoffHead, testHead} {
		requireCheckpointRevision(t, f, name, 2)
	}

	// Converge the stale pre-crash pin ledger and prove the recovered pair is the
	// only generation retained through a real collector cut.
	f.reconcile()
	gc, err := pinning.NewGC(pinning.GCConfig{
		Blocks: f.store.Blocks(), Reconciler: f.rec, Staging: f.staging, Fetch: next.GCFetch(),
	})
	if err != nil {
		t.Fatalf("constructing recovery GC: %v", err)
	}
	if _, err := gc.Run(t.Context()); err != nil {
		t.Fatalf("GC after crash recovery: %v", err)
	}
	for _, root := range []cid.Cid{finalizedB.Root(), mutableB.Root()} {
		if !f.hasLocally(root) {
			t.Fatalf("GC after crash recovery swept current root %s", root)
		}
	}
	for _, root := range []cid.Cid{finalizedA.Root(), mutableA.Root()} {
		if f.hasLocally(root) {
			t.Fatalf("GC after crash recovery retained superseded-only root %s", root)
		}
	}
}
