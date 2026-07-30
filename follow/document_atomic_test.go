package follow_test

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/ipfs/go-cid"

	"github.com/blobarchive/bloar/archive"
	"github.com/blobarchive/bloar/catalog"
	"github.com/blobarchive/bloar/follow"
	"github.com/blobarchive/bloar/pinning"
	"github.com/blobarchive/bloar/server"
)

// These tests exercise a revisioned publication as the unit of durability and
// visibility. The mutable fixtures are useful here because a mutable line and
// its finalized witness are a pair which must never be composed across signed
// document generations.

func buildDocumentHead(t *testing.T, w *writer, name string, origin, end, segBits, fanoutBits uint64) *archive.Head {
	t.Helper()
	h, err := archive.BuildGeneration(t.Context(), archive.Config{
		Blocks: w.store.Blocks(), Resolver: w.cat, Cache: w.cache,
	}, archive.Params{
		Name: name, Net: testNet, OriginSlot: origin, SegBits: segBits, FanoutBits: fanoutBits,
	}, nil, end)
	if err != nil {
		t.Fatalf("BuildGeneration(%s, origin=%d, end=%d, seg=%d, fanout=%d): %v",
			name, origin, end, segBits, fanoutBits, err)
	}
	return h
}

func documentPair(t *testing.T, w *writer, mutable, finalized *archive.Head, revision uint64) server.Unsigned {
	t.Helper()
	mutableEntry := entry(mutable.Info())
	finalizedEntry := entry(finalized.Info())
	if mutableEntry.SyncedTo == nil || finalizedEntry.SyncedTo == nil {
		t.Fatal("documentPair requires covered mutable and finalized heads")
	}
	start := mutableEntry.OriginSlot
	sourceFinalized := *finalizedEntry.SyncedTo
	handoffSynced := sourceFinalized
	mutableEntry.Kind = server.UnfinalizedMutable
	mutableEntry.WindowStart = &start
	mutableEntry.SourceHeadRoot = fmt.Sprintf("0x%064x", revision+0x1000)
	mutableEntry.SourceFinalizedSlot = &sourceFinalized
	mutableEntry.SourceFinalizedRoot = fmt.Sprintf("0x%064x", revision+0x2000)
	mutableEntry.HandoffHead = finalizedEntry.Name
	mutableEntry.HandoffRoot = finalizedEntry.Root
	mutableEntry.HandoffSyncedTo = &handoffSynced
	return server.Unsigned{
		V: server.DocVersion, Net: testNet, UpdatedAt: time.Unix(int64(revision), 0).UTC().Format(time.RFC3339),
		Multiaddrs: w.host.AnnounceAddrs(), Heads: []server.HeadEntry{mutableEntry, finalizedEntry}, Revision: &revision,
	}
}

func omissionDocument(w *writer, revision uint64, entries ...server.HeadEntry) server.Unsigned {
	return server.Unsigned{
		V: server.DocVersion, Net: testNet, UpdatedAt: time.Unix(int64(revision), 0).UTC().Format(time.RFC3339),
		Multiaddrs: w.host.AnnounceAddrs(), Heads: entries, Revision: &revision,
	}
}

func documentFollower(t *testing.T, w *writer, docs *docServer, ready *readyRecorder) *follower {
	t.Helper()
	return newFollower(t, w, func(c *follow.Config) {
		c.URL = docs.url
		configureMutableFollower(c, 32)
		if ready != nil {
			c.Ready = ready.hook()
		}
	})
}

func restartDocumentFollowerOnNetwork(t *testing.T, f *follower, w *writer, docs *docServer, network string) *follow.Follower {
	t.Helper()
	if err := f.f.Close(); err != nil {
		t.Fatalf("closing follower before cross-network restart: %v", err)
	}
	heads, err := server.NewHeads(server.HeadsConfig{
		Net: network, Roots: f.roots, Manifests: f.manifests,
	})
	if err != nil {
		t.Fatalf("server.NewHeads(cross-network restart): %v", err)
	}
	reconciler, err := pinning.NewReconciler(pinning.Config{
		Ledger: catalog.NewLedger(f.store.KV()), ManifestTip: f.manifests.Get,
	})
	if err != nil {
		t.Fatalf("pinning.NewReconciler(cross-network restart): %v", err)
	}
	f.heads, f.rec = heads, reconciler
	cfg := follow.Config{
		Net: network, URL: docs.url, PubKey: w.pubkey(),
		Local: f.store.Blocks(), Sessions: f.ex, Host: f.host,
		Registry: heads, Roots: f.roots, Manifests: f.manifests, Reconciler: reconciler,
		Staging: f.staging, KV: f.store.KV(), Cache: f.cache, Logger: testLogger(t),
	}
	configureMutableFollower(&cfg, 32)
	next, err := follow.New(cfg)
	if err != nil {
		t.Fatalf("follow.New(cross-network restart): %v", err)
	}
	f.f = next
	t.Cleanup(func() {
		if err := next.Close(); err != nil {
			t.Errorf("closing cross-network follower: %v", err)
		}
	})
	return next
}

func requireSelectedRoot(t *testing.T, heads *server.Heads, name string, want cid.Cid) {
	t.Helper()
	got, ok := heads.Get(name)
	if !ok || !got.Root().Equals(want) {
		t.Fatalf("selected head %q = %v ok=%t, want root %s", name, got, ok, want)
	}
}

func requireNotSelected(t *testing.T, heads *server.Heads, name string) {
	t.Helper()
	if got, ok := heads.Get(name); ok {
		t.Fatalf("head %q remains selected at %s", name, got.Root())
	}
}

func requireMirror(t *testing.T, roots *server.RootStore, name string, want cid.Cid) {
	t.Helper()
	got, ok, err := roots.Get(t.Context(), name)
	if err != nil || !ok || !got.Equals(want) {
		t.Fatalf("root mirror %q = %s ok=%t err=%v, want %s", name, got, ok, err, want)
	}
}

func requireNoMirrors(t *testing.T, f *follower, names ...string) {
	t.Helper()
	for _, name := range names {
		if root, ok, err := f.roots.Get(t.Context(), name); err != nil || ok {
			t.Fatalf("withdrawn root mirror %q = %s ok=%t err=%v", name, root, ok, err)
		}
		if tip, ok, err := f.manifests.Get(t.Context(), name); err != nil || ok {
			t.Fatalf("withdrawn manifest mirror %q = %s ok=%t err=%v", name, tip, ok, err)
		}
	}
}

func requireCheckpointRevision(t *testing.T, f *follower, name string, want uint64) {
	t.Helper()
	_, _, got, _, _, ok, err := follow.ReadRevisionedCheckpoint(f.store.KV(), name)
	if err != nil || !ok || got != want {
		t.Fatalf("checkpoint %q revision = %d ok=%t err=%v, want %d", name, got, ok, err, want)
	}
}

func checkpointBytes(t *testing.T, f *follower, name string) []byte {
	t.Helper()
	v, closer, err := f.store.KV().Get([]byte("fcheckpoint:" + name))
	if err != nil {
		t.Fatalf("reading raw checkpoint %q: %v", name, err)
	}
	defer closer.Close()
	return bytes.Clone(v)
}

func checkpointPublishedEntry(t *testing.T, f *follower, name string) server.HeadEntry {
	t.Helper()
	raw := checkpointBytes(t, f, name)
	if len(raw) < 92 || raw[0] != 3 {
		t.Fatalf("checkpoint %q is not a complete v3 record: %x", name, raw)
	}
	networkLen := int(binary.BigEndian.Uint16(raw[2:4]))
	publishedLen := int(binary.BigEndian.Uint32(raw[84:88]))
	publishedAt := 92 + networkLen
	if publishedLen == 0 || publishedAt+publishedLen > len(raw) {
		t.Fatalf("checkpoint %q has invalid published length %d in %d bytes", name, publishedLen, len(raw))
	}
	var got server.HeadEntry
	if err := json.Unmarshal(raw[publishedAt:publishedAt+publishedLen], &got); err != nil {
		t.Fatalf("decoding checkpoint %q publication entry: %v", name, err)
	}
	return got
}

func TestRevisionedDocumentVisibilitySwapsOldOldToNewNew(t *testing.T) {
	w := newWriter(t)
	docs := newDocServer(t)
	finalizedA := buildDocumentHead(t, w, testHandoffHead, 96, 103, testSegBits, testFanout)
	mutableA := buildDocumentHead(t, w, testHead, 96, 103, testSegBits, testFanout)
	finalizedB := buildDocumentHead(t, w, testHandoffHead, 96, 111, testSegBits, testFanout)
	mutableB := buildDocumentHead(t, w, testHead, 104, 111, testSegBits, testFanout)
	f := documentFollower(t, w, docs, nil)

	docs.set(sign(t, w.key, documentPair(t, w, mutableA, finalizedA, 1)))
	f.poll()

	var paused atomic.Bool
	follow.SetBeforeExposeHook(func() {
		paused.Store(true)
		// Persist has completed, but the one registry pointer has not moved: both
		// serving names must still resolve through the old document generation.
		requireSelectedRoot(t, f.heads, testHandoffHead, finalizedA.Root())
		requireSelectedRoot(t, f.heads, testHead, mutableA.Root())
		// Conversely, the durable selection fact is already wholly new.
		requireMirror(t, f.roots, testHandoffHead, finalizedB.Root())
		requireMirror(t, f.roots, testHead, mutableB.Root())
		requireCheckpointRevision(t, f, testHandoffHead, 2)
		requireCheckpointRevision(t, f, testHead, 2)
	})
	t.Cleanup(func() { follow.SetBeforeExposeHook(nil) })
	docs.set(sign(t, w.key, documentPair(t, w, mutableB, finalizedB, 2)))
	f.poll()
	follow.SetBeforeExposeHook(nil)

	if !paused.Load() {
		t.Fatal("document transition did not reach BeforeVisible")
	}
	requireSelectedRoot(t, f.heads, testHandoffHead, finalizedB.Root())
	requireSelectedRoot(t, f.heads, testHead, mutableB.Root())
	published := checkpointPublishedEntry(t, f, testHead)
	if published.HandoffRoot != finalizedB.Root().String() || published.HandoffSyncedTo == nil ||
		*published.HandoffSyncedTo != 111 {
		t.Fatalf("new mutable line was composed with the wrong finalized generation: %#v", published)
	}
}

func TestRevisionedOmissionDurablyWithdrawsAndRestartsAsOneGroup(t *testing.T) {
	w := newWriter(t)
	docs := newDocServer(t)
	ready := newReadyRecorder()
	finalized := buildDocumentHead(t, w, testHandoffHead, 96, 103, testSegBits, testFanout)
	mutable := buildDocumentHead(t, w, testHead, 96, 103, testSegBits, testFanout)
	f := documentFollower(t, w, docs, ready)

	docs.set(sign(t, w.key, documentPair(t, w, mutable, finalized, 1)))
	f.poll()
	if !ready.isReady(testHandoffHead) || !ready.isReady(testHead) {
		t.Fatal("initial selected pair was not ready")
	}

	// The finalized witness remains selected while omission authenticates a
	// mutable tombstone. Both checkpoint records still move to revision 2.
	finalizedLine := entry(finalized.Info())
	docs.set(sign(t, w.key, omissionDocument(w, 2, finalizedLine)))
	f.poll()
	requireSelectedRoot(t, f.heads, testHandoffHead, finalized.Root())
	requireNotSelected(t, f.heads, testHead)
	if !ready.isReady(testHandoffHead) || ready.isReady(testHead) {
		t.Fatalf("readiness after omission: finalized=%t mutable=%t", ready.isReady(testHandoffHead), ready.isReady(testHead))
	}
	requireCheckpointRevision(t, f, testHandoffHead, 2)
	requireCheckpointRevision(t, f, testHead, 2)
	requireMirror(t, f.roots, testHandoffHead, finalized.Root())
	if root, ok, err := f.roots.Get(t.Context(), testHead); err != nil || ok {
		t.Fatalf("mutable root mirror after omission = %s ok=%t err=%v", root, ok, err)
	}
	if tip, ok, err := f.manifests.Get(t.Context(), testHead); err != nil || ok {
		t.Fatalf("mutable manifest mirror after omission = %s ok=%t err=%v", tip, ok, err)
	}

	// Draining the desired-empty reconciler tombstone removes the withdrawn
	// name only after its old ledger rows are gone.
	f.reconcile()
	if rows, err := ledgerOf(f.node).ListAll(t.Context(), testHead); err != nil || len(rows) != 0 {
		t.Fatalf("withdrawn mutable ledger rows = %v err=%v", rows, err)
	}

	restartedReady := newReadyRecorder()
	next := f.restart(t, w, func(c *follow.Config) {
		c.URL = docs.url
		configureMutableFollower(c, 32)
		c.Ready = restartedReady.hook()
	})
	var paused atomic.Bool
	follow.SetBeforeExposeHook(func() {
		paused.Store(true)
		// A fresh registry exposes neither half until the whole v3 group has
		// passed validation and its selected/tombstone batch reaches visibility.
		requireNotSelected(t, f.heads, testHandoffHead)
		requireNotSelected(t, f.heads, testHead)
	})
	t.Cleanup(func() { follow.SetBeforeExposeHook(nil) })
	if err := next.Resume(t.Context()); err != nil {
		t.Fatalf("Resume(v3 selected+tombstone group): %v", err)
	}
	follow.SetBeforeExposeHook(nil)
	if !paused.Load() {
		t.Fatal("resume did not pass through the atomic visibility boundary")
	}
	requireSelectedRoot(t, f.heads, testHandoffHead, finalized.Root())
	requireNotSelected(t, f.heads, testHead)
	if !restartedReady.isReady(testHandoffHead) || restartedReady.isReady(testHead) {
		t.Fatalf("restart readiness: finalized=%t mutable=%t",
			restartedReady.isReady(testHandoffHead), restartedReady.isReady(testHead))
	}
}

func TestRevisionedResumeRejectsIncompleteOrMixedDocumentGroup(t *testing.T) {
	tests := []struct {
		name       string
		breakState func(*testing.T, *follower)
		want       string
	}{
		{
			name: "missing member",
			breakState: func(t *testing.T, f *follower) {
				if err := f.store.KV().Delete([]byte("fcheckpoint:"+testHead), pebble.Sync); err != nil {
					t.Fatal(err)
				}
			},
			want: "1 of 2 configured head records",
		},
		{
			name: "mixed revision member",
			breakState: func(t *testing.T, f *follower) {
				raw := checkpointBytes(t, f, testHead)
				if len(raw) < 20 || raw[0] != 3 {
					t.Fatalf("unexpected checkpoint v3 encoding: %x", raw)
				}
				// v3 layout: version/flags/reserved, updated_at, then revision.
				binary.BigEndian.PutUint64(raw[12:20], 2)
				if err := f.store.KV().Set([]byte("fcheckpoint:"+testHead), raw, pebble.Sync); err != nil {
					t.Fatal(err)
				}
			},
			want: "different authenticated document generation",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := newWriter(t)
			docs := newDocServer(t)
			finalized := buildDocumentHead(t, w, testHandoffHead, 96, 103, testSegBits, testFanout)
			mutable := buildDocumentHead(t, w, testHead, 96, 103, testSegBits, testFanout)
			f := documentFollower(t, w, docs, nil)
			docs.set(sign(t, w.key, documentPair(t, w, mutable, finalized, 1)))
			f.poll()
			tt.breakState(t, f)

			next := f.restart(t, w, func(c *follow.Config) {
				c.URL = docs.url
				configureMutableFollower(c, 32)
			})
			err := next.Resume(t.Context())
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Resume error = %v, want substring %q", err, tt.want)
			}
			// Failure of either member suppresses the complete signed generation.
			requireNotSelected(t, f.heads, testHandoffHead)
			requireNotSelected(t, f.heads, testHead)
		})
	}
}

func TestFirstDocumentTombstonesRetainNetworkAcrossRestart(t *testing.T) {
	w := newWriter(t)
	docs := newDocServer(t)
	f := documentFollower(t, w, docs, nil)

	// Neither head has ever been selected, so these records have no retained
	// HeadEntry from which a restart could infer the signed document's network.
	docs.set(sign(t, w.key, omissionDocument(w, 1)))
	f.poll()
	for _, name := range []string{testHandoffHead, testHead} {
		requireCheckpointRevision(t, f, name, 1)
		requireNotSelected(t, f.heads, name)
	}
	requireNoMirrors(t, f, testHandoffHead, testHead)

	const otherNetwork = "different-testnet"
	next := restartDocumentFollowerOnNetwork(t, f, w, docs, otherNetwork)
	err := next.Resume(t.Context())
	if err == nil || !strings.Contains(err.Error(), "checkpoint network \"testnet\" differs from configured network \""+otherNetwork+"\"") {
		t.Fatalf("cross-network tombstone Resume error = %v", err)
	}
	reappear := documentPair(t, w,
		buildDocumentHead(t, w, testHead, 96, 103, testSegBits, testFanout),
		buildDocumentHead(t, w, testHandoffHead, 96, 103, testSegBits, testFanout), 2)
	reappear.Net = otherNetwork
	docs.set(sign(t, w.key, reappear))
	err = next.Poll(t.Context())
	if err == nil || !strings.Contains(err.Error(), "checkpoint network \"testnet\" differs from authenticated document network \""+otherNetwork+"\"") {
		t.Fatalf("cross-network first selection error = %v", err)
	}
	for _, name := range []string{testHandoffHead, testHead} {
		requireCheckpointRevision(t, f, name, 1)
	}
	requireNotSelected(t, f.heads, testHandoffHead)
	requireNotSelected(t, f.heads, testHead)
}

func TestSelectedThenWithdrawnTombstonesRefuseCrossNetworkReappearance(t *testing.T) {
	w := newWriter(t)
	docs := newDocServer(t)
	finalized := buildDocumentHead(t, w, testHandoffHead, 96, 103, testSegBits, testFanout)
	mutable := buildDocumentHead(t, w, testHead, 96, 103, testSegBits, testFanout)
	f := documentFollower(t, w, docs, nil)
	docs.set(sign(t, w.key, documentPair(t, w, mutable, finalized, 1)))
	f.poll()
	docs.set(sign(t, w.key, omissionDocument(w, 2)))
	f.poll()
	requireNoMirrors(t, f, testHandoffHead, testHead)

	const otherNetwork = "different-testnet"
	next := restartDocumentFollowerOnNetwork(t, f, w, docs, otherNetwork)
	err := next.Resume(t.Context())
	if err == nil || !strings.Contains(err.Error(), "checkpoint network \"testnet\" differs from configured network \""+otherNetwork+"\"") {
		t.Fatalf("cross-network retained tombstone Resume error = %v", err)
	}

	reappear := documentPair(t, w, mutable, finalized, 3)
	reappear.Net = otherNetwork
	docs.set(sign(t, w.key, reappear))
	err = next.Poll(t.Context())
	if err == nil || !strings.Contains(err.Error(), "checkpoint network \"testnet\" differs from authenticated document network \""+otherNetwork+"\"") {
		t.Fatalf("cross-network reappearance error = %v", err)
	}
	for _, name := range []string{testHandoffHead, testHead} {
		requireCheckpointRevision(t, f, name, 2)
		requireNotSelected(t, f.heads, name)
	}
	requireNoMirrors(t, f, testHandoffHead, testHead)
}

func TestProoflessV2MutableCheckpointCannotBeWithdrawn(t *testing.T) {
	w := newWriter(t)
	docs := newDocServer(t)
	finalized := buildDocumentHead(t, w, testHandoffHead, 96, 103, testSegBits, testFanout)
	mutable := buildDocumentHead(t, w, testHead, 96, 103, testSegBits, testFanout)
	f := documentFollower(t, w, docs, nil)
	docs.set(sign(t, w.key, documentPair(t, w, mutable, finalized, 1)))
	f.poll()

	if err := follow.DowngradeCheckpointToProoflessV2(f.store.KV(), testHead); err != nil {
		t.Fatalf("downgrading mutable checkpoint: %v", err)
	}
	finalizedLine := entry(finalized.Info())
	docs.set(sign(t, w.key, omissionDocument(w, 2, finalizedLine)))
	err := f.pollErr()
	if err == nil || !strings.Contains(err.Error(), "v2 checkpoint lacks proof-aware publication metadata") {
		t.Fatalf("proofless-v2 withdrawal error = %v", err)
	}

	// Refusing one withdrawal refuses the complete revision: the finalized
	// member cannot consume revision 2 beside a mutable v2 record whose exact
	// handoff proof and immutable geometry are unavailable.
	for _, state := range []struct {
		name string
		root cid.Cid
	}{{testHandoffHead, finalized.Root()}, {testHead, mutable.Root()}} {
		requireCheckpointRevision(t, f, state.name, 1)
		requireMirror(t, f.roots, state.name, state.root)
		requireSelectedRoot(t, f.heads, state.name, state.root)
	}
	revision, _, ok, floorErr := follow.ReadAuthorityFloor(f.store.KV(), w.pubkey())
	if floorErr != nil || !ok || revision != 1 {
		t.Fatalf("proofless-v2 refusal authority floor = %d ok=%t err=%v, want 1", revision, ok, floorErr)
	}
}

func TestRevisionedReappearanceRetainsImmutableBaselinesAcrossOmission(t *testing.T) {
	tests := []struct {
		name  string
		build func(*testing.T, *writer, *archive.Head, *archive.Head) (*archive.Head, *archive.Head)
		want  string
	}{
		{
			name: "finalized origin",
			build: func(t *testing.T, w *writer, mutable, _ *archive.Head) (*archive.Head, *archive.Head) {
				return mutable, buildDocumentHead(t, w, testHandoffHead, 97, 103, testSegBits, testFanout)
			},
			want: "origin_slot across withdrawal",
		},
		{
			name: "finalized segment geometry",
			build: func(t *testing.T, w *writer, mutable, _ *archive.Head) (*archive.Head, *archive.Head) {
				return mutable, buildDocumentHead(t, w, testHandoffHead, 96, 103, testSegBits+1, testFanout)
			},
			want: "seg_bits/fanout_bits across withdrawal",
		},
		{
			name: "finalized fanout geometry",
			build: func(t *testing.T, w *writer, mutable, _ *archive.Head) (*archive.Head, *archive.Head) {
				return mutable, buildDocumentHead(t, w, testHandoffHead, 96, 103, testSegBits, testFanout+1)
			},
			want: "seg_bits/fanout_bits across withdrawal",
		},
		{
			name: "mutable segment geometry",
			build: func(t *testing.T, w *writer, _ *archive.Head, finalized *archive.Head) (*archive.Head, *archive.Head) {
				return buildDocumentHead(t, w, testHead, 96, 103, testSegBits+1, testFanout), finalized
			},
			want: "seg_bits/fanout_bits across withdrawal",
		},
		{
			name: "mutable fanout geometry",
			build: func(t *testing.T, w *writer, _ *archive.Head, finalized *archive.Head) (*archive.Head, *archive.Head) {
				return buildDocumentHead(t, w, testHead, 96, 103, testSegBits, testFanout+1), finalized
			},
			want: "seg_bits/fanout_bits across withdrawal",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := newWriter(t)
			docs := newDocServer(t)
			finalized := buildDocumentHead(t, w, testHandoffHead, 96, 103, testSegBits, testFanout)
			mutable := buildDocumentHead(t, w, testHead, 96, 103, testSegBits, testFanout)
			f := documentFollower(t, w, docs, nil)
			docs.set(sign(t, w.key, documentPair(t, w, mutable, finalized, 1)))
			f.poll()

			docs.set(sign(t, w.key, omissionDocument(w, 2)))
			f.poll()
			requireNotSelected(t, f.heads, testHandoffHead)
			requireNotSelected(t, f.heads, testHead)
			requireNoMirrors(t, f, testHandoffHead, testHead)

			changedMutable, changedFinalized := tt.build(t, w, mutable, finalized)
			docs.set(sign(t, w.key, documentPair(t, w, changedMutable, changedFinalized, 3)))
			err := f.pollErr()
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("reappearance error = %v, want substring %q", err, tt.want)
			}
			// The refused document cannot consume revision 3 or partially restore
			// the member whose immutable baseline happened to remain valid.
			for _, name := range []string{testHandoffHead, testHead} {
				requireCheckpointRevision(t, f, name, 2)
				requireNotSelected(t, f.heads, name)
			}
			requireNoMirrors(t, f, testHandoffHead, testHead)
			revision, _, ok, floorErr := follow.ReadAuthorityFloor(f.store.KV(), w.pubkey())
			if floorErr != nil || !ok || revision != 2 {
				t.Fatalf("refused reappearance authority floor = %d ok=%t err=%v, want 2", revision, ok, floorErr)
			}
		})
	}
}

func TestRevisionedExplicitEmptyFinalizedAfterCoverageRefusesWholeDocument(t *testing.T) {
	w := newWriter(t)
	docs := newDocServer(t)
	finalized := buildDocumentHead(t, w, testHandoffHead, 96, 103, testSegBits, testFanout)
	mutable := buildDocumentHead(t, w, testHead, 96, 103, testSegBits, testFanout)
	f := documentFollower(t, w, docs, nil)
	docs.set(sign(t, w.key, documentPair(t, w, mutable, finalized, 1)))
	f.poll()

	empty := entry(finalized.Info())
	empty.SyncedTo = nil
	// Mutable is simultaneously omitted. The invalid explicit finalized
	// regression must prevent that otherwise-valid withdrawal from committing.
	docs.set(sign(t, w.key, omissionDocument(w, 2, empty)))
	err := f.pollErr()
	if err == nil || !strings.Contains(err.Error(), "explicit null synced_to would regress") {
		t.Fatalf("explicit-empty error = %v", err)
	}
	for _, state := range []struct {
		name string
		root cid.Cid
	}{{testHandoffHead, finalized.Root()}, {testHead, mutable.Root()}} {
		requireSelectedRoot(t, f.heads, state.name, state.root)
		requireMirror(t, f.roots, state.name, state.root)
		requireCheckpointRevision(t, f, state.name, 1)
	}
}

func TestRevisionedSameRootRefreshesMutableProofAndWholeCheckpointGroup(t *testing.T) {
	w := newWriter(t)
	docs := newDocServer(t)
	finalized := buildDocumentHead(t, w, testHandoffHead, 96, 103, testSegBits, testFanout)
	mutable := buildDocumentHead(t, w, testHead, 96, 103, testSegBits, testFanout)
	f := documentFollower(t, w, docs, nil)
	docs.set(sign(t, w.key, documentPair(t, w, mutable, finalized, 1)))
	f.poll()

	next := documentPair(t, w, mutable, finalized, 2)
	newSourceHead := "0x" + strings.Repeat("a5", 32)
	newSourceFinalized := "0x" + strings.Repeat("5a", 32)
	next.Heads[0].SourceHeadRoot = newSourceHead
	next.Heads[0].SourceFinalizedRoot = newSourceFinalized
	docs.set(sign(t, w.key, next))
	f.poll()

	for _, name := range []string{testHandoffHead, testHead} {
		requireCheckpointRevision(t, f, name, 2)
	}
	requireSelectedRoot(t, f.heads, testHandoffHead, finalized.Root())
	requireSelectedRoot(t, f.heads, testHead, mutable.Root())
	got := checkpointPublishedEntry(t, f, testHead)
	if got.SourceHeadRoot != newSourceHead || got.SourceFinalizedRoot != newSourceFinalized {
		t.Fatalf("same-root revision retained stale proof: %#v", got)
	}
}

func TestRevisionedCommitFailureLeavesEveryConsumerOnOldGeneration(t *testing.T) {
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

	sentinel := errors.New("injected document commit failure")
	follow.SetBeforeAdmissionCommitHook(func() error { return sentinel })
	t.Cleanup(func() { follow.SetBeforeAdmissionCommitHook(nil) })
	docs.set(sign(t, w.key, documentPair(t, w, mutableB, finalizedB, 2)))
	err := f.pollErr()
	follow.SetBeforeAdmissionCommitHook(nil)
	if !errors.Is(err, sentinel) {
		t.Fatalf("Poll error = %v, want %v", err, sentinel)
	}

	for _, state := range []struct {
		name string
		old  cid.Cid
		new  cid.Cid
	}{{testHandoffHead, finalizedA.Root(), finalizedB.Root()}, {testHead, mutableA.Root(), mutableB.Root()}} {
		requireCheckpointRevision(t, f, state.name, 1)
		requireMirror(t, f.roots, state.name, state.old)
		requireSelectedRoot(t, f.heads, state.name, state.old)
		if tip, ok, mirrorErr := f.manifests.Get(t.Context(), state.name); mirrorErr != nil || ok {
			t.Fatalf("failed commit changed absent manifest mirror %q: tip=%s ok=%t err=%v", state.name, tip, ok, mirrorErr)
		}
	}
	revision, _, ok, floorErr := follow.ReadAuthorityFloor(f.store.KV(), w.pubkey())
	if floorErr != nil || !ok || revision != 1 {
		t.Fatalf("failed commit authority floor = %d ok=%t err=%v, want 1", revision, ok, floorErr)
	}

	// If the prepared reconciler batch had leaked through the failed durability
	// barrier, this pass would replace each old root pin with its new root.
	f.reconcile()
	ledger := catalog.NewLedger(f.store.KV())
	for _, state := range []struct {
		name string
		old  cid.Cid
		new  cid.Cid
	}{{testHandoffHead, finalizedA.Root(), finalizedB.Root()}, {testHead, mutableA.Root(), mutableB.Root()}} {
		pins, listErr := ledger.List(t.Context(), state.name, pinning.PurposeRoot)
		if listErr != nil {
			t.Fatal(listErr)
		}
		if len(pins) != 1 || !pins[0].CID.Equals(state.old) || pins[0].CID.Equals(state.new) {
			t.Fatalf("reconciler root pins for %q after failed commit = %v, want only old %s", state.name, pins, state.old)
		}
	}
}

func TestWithdrawnFinalizedCheckpointCannotBePromotedBackToWriter(t *testing.T) {
	w := newWriter(t)
	docs := newDocServer(t)
	finalized := buildDocumentHead(t, w, testHandoffHead, 96, 103, testSegBits, testFanout)
	mutable := buildDocumentHead(t, w, testHead, 96, 103, testSegBits, testFanout)
	f := documentFollower(t, w, docs, nil)
	docs.set(sign(t, w.key, documentPair(t, w, mutable, finalized, 1)))
	f.poll()
	docs.set(sign(t, w.key, omissionDocument(w, 2)))
	f.poll()
	requireNoMirrors(t, f, testHandoffHead, testHead)

	found, err := follow.ReconcileWriterPromotion(t.Context(), follow.PromotionConfig{
		KV: f.store.KV(), Roots: f.roots, Manifests: f.manifests, Blocks: f.store.Blocks(), Cache: f.cache,
		Params: finalized.Params(), Policy: pinning.Full(),
	}, testHandoffHead)
	if !found || err == nil || !strings.Contains(err.Error(), "withdrawn head") {
		t.Fatalf("promotion of tombstone: found=%t err=%v, want withdrawn-head refusal", found, err)
	}
	requireNoMirrors(t, f, testHandoffHead, testHead)
	requireCheckpointRevision(t, f, testHandoffHead, 2)
	requireCheckpointRevision(t, f, testHead, 2)
}
