package follow_test

// Acceptance coverage for the safety boundary: a configured followed head reports
// readiness only once it is actually registered -- resumed from a durable
// checkpoint or first adopted from a verified document -- so the daemon's
// readiness gate (which Config.Ready drives) stays red, and the load balancer
// routes away, until the head can serve. A head with no checkpoint stays red
// until its first adoption; a head with a corrupt checkpoint fails closed and
// stays red rather than serving a wrong answer behind a green probe.

import (
	"context"
	"crypto/ed25519"
	"sync"
	"testing"
	"time"

	"github.com/ipfs/go-cid"

	"github.com/blobarchive/bloar/catalog"
	"github.com/blobarchive/bloar/follow"
	"github.com/blobarchive/bloar/pinning"
	"github.com/blobarchive/bloar/server"
	"github.com/blobarchive/bloar/store"
)

// readyRecorder captures the heads Config.Ready has raised.
type readyRecorder struct {
	mu    sync.Mutex
	ready map[string]bool
}

func newReadyRecorder() *readyRecorder { return &readyRecorder{ready: map[string]bool{}} }

func (r *readyRecorder) hook() func(string, bool) {
	return func(head string, ready bool) {
		r.mu.Lock()
		defer r.mu.Unlock()
		r.ready[head] = ready
	}
}

func (r *readyRecorder) isReady(head string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.ready[head]
}

// newReadinessFollower builds a follower over st with a readiness recorder, the
// way the daemon wires Config.Ready, and denies fetches so every load resolves
// against st's local blocks.
func newReadinessFollower(t *testing.T, st *store.Store, key ed25519.PrivateKey) (*follow.Follower, *readyRecorder) {
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
	rr := newReadyRecorder()
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
		Ready:      rr.hook(),
	})
	if err != nil {
		t.Fatalf("follow.New: %v", err)
	}
	t.Cleanup(func() {
		if err := f.Close(); err != nil {
			t.Errorf("Follower.Close: %v", err)
		}
	})
	return f, rr
}

func TestFollowedHeadReadinessOnResume(t *testing.T) {
	authAt := time.Unix(1_700_000_000, 0).UTC()
	_, key, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("ed25519.GenerateKey: %v", err)
	}

	t.Run("a durable checkpoint resumes ready", func(t *testing.T) {
		ctx := context.Background()
		st := openAuditStore(t)
		_, infos := coveredHead(t, st, 100)
		root := infos[0].Root
		if err := follow.WriteCheckpoint(st.KV(), testHead, root, 100, cid.Undef, authAt); err != nil {
			t.Fatalf("WriteCheckpoint: %v", err)
		}
		f, rr := newReadinessFollower(t, st, key)

		if rr.isReady(testHead) {
			t.Fatal("the head was ready before Resume; readiness must not precede registration")
		}
		if err := f.Resume(ctx); err != nil {
			t.Fatalf("Resume: %v", err)
		}
		if !rr.isReady(testHead) {
			t.Fatal("a resumed head did not report ready")
		}
	})

	t.Run("no checkpoint stays red until first adoption", func(t *testing.T) {
		ctx := context.Background()
		st := openAuditStore(t)
		f, rr := newReadinessFollower(t, st, key)

		// A fresh never-adopted head: Resume finds no checkpoint and exposes
		// nothing, so the head stays red -- the load balancer keeps routing away
		// until a poll first adopts it.
		if err := f.Resume(ctx); err != nil {
			t.Fatalf("Resume: %v", err)
		}
		if rr.isReady(testHead) {
			t.Fatal("a never-adopted head reported ready; it must stay red until first adoption")
		}
	})

	t.Run("a corrupt checkpoint fails closed and stays red", func(t *testing.T) {
		ctx := context.Background()
		st := openAuditStore(t)
		_, infos := coveredHead(t, st, 100)
		root := infos[0].Root
		// The floor claims more coverage (200) than the root encodes (100): an
		// inconsistent local state Resume must refuse rather than serve. The head
		// stays red, failing closed.
		if err := follow.WriteCheckpoint(st.KV(), testHead, root, 200, cid.Undef, authAt); err != nil {
			t.Fatalf("WriteCheckpoint: %v", err)
		}
		f, rr := newReadinessFollower(t, st, key)

		if err := f.Resume(ctx); err == nil {
			t.Fatal("Resume accepted a checkpoint whose floor exceeds its root's coverage; it must fail closed")
		}
		if rr.isReady(testHead) {
			t.Fatal("a head with a corrupt checkpoint reported ready; it must fail closed and stay red")
		}
	})
}

// Run remains the safe default for daemon callers: it restores durable heads
// before its first poll. The Kubo replica uses RunAfterResume only because it
// must insert its public-listener bind between those two steps.
func TestFollowerRunPreservesResumeContract(t *testing.T) {
	authAt := time.Unix(1_700_000_000, 0).UTC()
	_, key, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	st := openAuditStore(t)
	_, infos := coveredHead(t, st, 100)
	if err := follow.WriteCheckpoint(st.KV(), testHead, infos[0].Root, 100, cid.Undef, authAt); err != nil {
		t.Fatal(err)
	}
	f, rr := newReadinessFollower(t, st, key)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- f.Run(ctx) }()
	t.Cleanup(cancel)

	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	for !rr.isReady(testHead) {
		select {
		case err := <-done:
			t.Fatalf("Run exited before restoring its checkpoint: %v", err)
		case <-deadline.C:
			t.Fatal("Run did not restore its durable checkpoint before polling")
		case <-time.After(time.Millisecond):
		}
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run after cancellation: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run did not stop after cancellation")
	}
}

// TestFollowedHeadReadinessOnAdoption is the poll side of the safety boundary:
// a head is red until the first verified document adopts it, then ready.
func TestFollowedHeadReadinessOnAdoption(t *testing.T) {
	w := newWriter(t)
	w.ingestSlot(100, 1)

	rr := newReadyRecorder()
	f := newFollower(t, w, func(c *follow.Config) { c.Ready = rr.hook() })
	f.serveHTTP(nil)

	if rr.isReady(testHead) {
		t.Fatal("the head was ready before the first poll; readiness must not precede adoption")
	}
	f.poll()
	if !rr.isReady(testHead) {
		t.Fatal("a head adopted from a verified document did not report ready")
	}
}
