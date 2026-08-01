package follow_test

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/ipfs/boxo/exchange"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"

	"github.com/blobarchive/bloar/archive"
	"github.com/blobarchive/bloar/catalog"
	"github.com/blobarchive/bloar/follow"
	"github.com/blobarchive/bloar/p2p"
	"github.com/blobarchive/bloar/pinning"
	"github.com/blobarchive/bloar/schema"
	"github.com/blobarchive/bloar/server"
	"github.com/blobarchive/bloar/store"
)

type auditRoundTrip func(*http.Request) (*http.Response, error)

func (f auditRoundTrip) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

type auditNoFetchSessions struct{}

func (auditNoFetchSessions) NewSession(context.Context) exchange.Fetcher { return auditNoFetcher{} }

type auditNoFetcher struct{}

func (auditNoFetcher) GetBlock(_ context.Context, c cid.Cid) (blocks.Block, error) {
	return nil, &p2p.FetchError{Cid: c, Err: errors.New("audit: unexpected network fetch")}
}

func (auditNoFetcher) GetBlocks(ctx context.Context, _ []cid.Cid) (<-chan blocks.Block, error) {
	out := make(chan blocks.Block)
	close(out)
	return out, ctx.Err()
}

func auditEntry(info archive.Info) server.HeadEntry {
	return server.HeadEntry{
		Name:       info.Name,
		Root:       info.Root.String(),
		OriginSlot: info.OriginSlot,
		SyncedTo:   info.SyncedTo,
		SegBits:    info.SegBits,
		FanoutBits: info.FanoutBits,
		DirDepth:   info.DirDepth,
	}
}

func auditSignedDocAt(t *testing.T, key ed25519.PrivateKey, entry server.HeadEntry, at time.Time) []byte {
	t.Helper()
	u := server.Unsigned{
		V:         server.DocVersion,
		Net:       testNet,
		UpdatedAt: at.UTC().Format(time.RFC3339),
		Heads:     []server.HeadEntry{entry},
	}
	canonical, err := u.Canonical()
	if err != nil {
		t.Fatalf("Canonical: %v", err)
	}
	doc := server.Doc{
		Unsigned:  u,
		Pubkey:    hex.EncodeToString(key.Public().(ed25519.PublicKey)),
		Signature: hex.EncodeToString(ed25519.Sign(key, canonical)),
	}
	body, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	return body
}

// TestCrashAfterAdoptBeforeFloors previously reproduced the safety boundary: a crash
// after the new root was made durable (through the server RootStore) but before
// adoptEntry persisted its synced_to and updated_at floors let the follower resume
// the new root and then accept a correctly signed OLDER publication -- a signed
// rollback across the restart.
//
// the atomic-checkpoint hardening makes a follower's root and its floors ONE atomic checkpoint
// (state.checkpoint), so "the root is durable" and "its floor is durable" are the
// same fact and that crash window no longer exists. This now asserts the fix: a
// committed checkpoint at coverage 120 survives the restart and REFUSES the older
// signed document at coverage 100 on the durable floor.
func TestCrashAfterAdoptBeforeFloors(t *testing.T) {
	ctx := t.Context()
	st, err := store.Open(t.TempDir(), store.WithPebbleLogger(quietPebble{}))
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() {
		if err := st.Close(); err != nil {
			t.Errorf("store.Close: %v", err)
		}
	})

	head, err := archive.New(ctx, archive.Config{Blocks: st.Blocks(), Resolver: catalog.New(st.KV())}, archive.Params{
		Name: testHead, Net: testNet, OriginSlot: 0, SegBits: 2, FanoutBits: 2,
	})
	if err != nil {
		t.Fatalf("archive.New: %v", err)
	}
	if _, err := head.ApplyRefs(ctx, nil, 100); err != nil {
		t.Fatalf("ApplyRefs(100): %v", err)
	}
	old := head.Info()
	if _, err := head.ApplyRefs(ctx, nil, 120); err != nil {
		t.Fatalf("ApplyRefs(120): %v", err)
	}
	newRoot := head.Root()

	// The durable state a COMPLETED adoption leaves under the atomic checkpoint: one
	// record naming the new root, its synced_to floor, and the document time that
	// authorized them, committed together. Under the old split this was the root
	// write with the floor writes still pending -- the window the safety boundary exploited.
	authAt := time.Unix(1_700_000_000, 0).UTC()
	if err := follow.WriteCheckpoint(st.KV(), testHead, newRoot, 120, cid.Undef, authAt); err != nil {
		t.Fatalf("WriteCheckpoint: %v", err)
	}
	// The compatibility mirror expose would have written; Resume never reads it. All
	// blocks are local, so neither Resume nor the poll can hide behind a fetch.
	roots := server.NewRootStore(st.KV())
	if err := roots.Put(ctx, testHead, newRoot); err != nil {
		t.Fatalf("RootStore.Put: %v", err)
	}
	registry, err := server.NewHeads(server.HeadsConfig{Net: testNet, Roots: roots})
	if err != nil {
		t.Fatalf("server.NewHeads: %v", err)
	}
	rec, err := pinning.NewReconciler(pinning.Config{Ledger: catalog.NewLedger(st.KV())})
	if err != nil {
		t.Fatalf("pinning.NewReconciler: %v", err)
	}
	_, key, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("ed25519.GenerateKey: %v", err)
	}
	// The rollback: the earlier root at coverage 100, signed and dated AFTER the
	// checkpoint's document so nothing but the synced_to floor can reject it.
	body := auditSignedDocAt(t, key, auditEntry(old), authAt.Add(time.Hour))
	client := &http.Client{Transport: auditRoundTrip(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader(string(body))),
		}, nil
	})}

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
		HTTP:       client,
	})
	if err != nil {
		t.Fatalf("follow.New: %v", err)
	}
	t.Cleanup(func() {
		if err := f.Close(); err != nil {
			t.Errorf("Follower.Close: %v", err)
		}
	})

	if err := f.Resume(ctx); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if got := auditSyncedTo(t, registry); got != 120 {
		t.Fatalf("the restart did not resume the committed checkpoint: synced_to = %d, want 120", got)
	}

	// The older signed document is refused on the durable floor, not adopted: the
	// rollback the crash used to enable no longer survives the restart.
	err = f.Poll(ctx)
	if err == nil {
		t.Fatal("a restarted follower adopted a rolled-back document")
	}
	if !strings.Contains(err.Error(), "below the adopted floor") {
		t.Errorf("err = %v, want it to name the synced_to floor", err)
	}
	if got := auditSyncedTo(t, registry); got != 120 {
		t.Errorf("synced_to after the rollback poll = %d, want it to stay at 120 (old root %s)", got, old.Root)
	}
	root, syncedTo, _, _, ok, err := follow.ReadCheckpoint(st.KV(), testHead)
	if err != nil || !ok {
		t.Fatalf("ReadCheckpoint after the rollback: (ok=%t, err=%v)", ok, err)
	}
	if root != newRoot || syncedTo != 120 {
		t.Errorf("checkpoint after the rollback = (%s, %d), want it untouched at (%s, 120)", root, syncedTo, newRoot)
	}
}

// TestCrashResumesNewRootWithOldManifest previously reproduced the safety boundary: a
// crash left the server RootStore at a new root and ManifestStore at a new tip
// while the follower's independently stored manifest floor was still the old tip,
// and Resume composed the new root with the OLD tip and overwrote the durable new
// tip without contacting the writer.
//
// the atomic-checkpoint hardening makes Resume read one atomic checkpoint -- root and manifest tip from
// the same generation -- and never compose from or write back the RootStore or
// ManifestStore mirrors. This asserts the fix: a committed checkpoint (new root,
// new tip) resumes as that exact pair, and the stale legacy floors left beside it,
// which are what the safety boundary composed in, are ignored.
func TestCrashResumesNewRootWithOldManifest(t *testing.T) {
	ctx := t.Context()
	st, err := store.Open(t.TempDir(), store.WithPebbleLogger(quietPebble{}))
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() {
		if err := st.Close(); err != nil {
			t.Errorf("store.Close: %v", err)
		}
	})

	head, err := archive.New(ctx, archive.Config{Blocks: st.Blocks(), Resolver: catalog.New(st.KV())}, archive.Params{
		Name: testHead, Net: testNet, OriginSlot: 0, SegBits: 2, FanoutBits: 2,
	})
	if err != nil {
		t.Fatalf("archive.New: %v", err)
	}
	if _, err := head.ApplyRefs(ctx, nil, 100); err != nil {
		t.Fatalf("ApplyRefs(100): %v", err)
	}
	if _, err := head.ApplyRefs(ctx, nil, 120); err != nil {
		t.Fatalf("ApplyRefs(120): %v", err)
	}
	newRoot := head.Root()

	oldTip := auditManifest(t, st, cid.Undef, 0)
	newTip := auditManifest(t, st, oldTip, 1)

	// The committed generation: the new root paired with the new tip, one atomic
	// checkpoint.
	authAt := time.Unix(1_700_000_000, 0).UTC()
	if err := follow.WriteCheckpoint(st.KV(), testHead, newRoot, 120, newTip, authAt); err != nil {
		t.Fatalf("WriteCheckpoint: %v", err)
	}
	// The mirrors expose writes through -- and, deliberately, the stale
	// pre-checkpoint legacy floors the old split could leave behind: an old manifest
	// floor and a lower synced_to. Resume must read the checkpoint and ignore these;
	// composing the new root with this old tip is exactly the safety boundary.
	roots := server.NewRootStore(st.KV())
	if err := roots.Put(ctx, testHead, newRoot); err != nil {
		t.Fatalf("RootStore.Put: %v", err)
	}
	manifests := server.NewManifestStore(st.KV())
	if err := manifests.Put(ctx, testHead, newTip); err != nil {
		t.Fatalf("ManifestStore.Put: %v", err)
	}
	if err := st.KV().Set([]byte("fmanifest:"+testHead), oldTip.Bytes(), pebble.Sync); err != nil {
		t.Fatalf("setting stale legacy manifest floor: %v", err)
	}
	var oldSynced [8]byte
	binary.BigEndian.PutUint64(oldSynced[:], 100)
	if err := st.KV().Set([]byte("fsynced_to:"+testHead), oldSynced[:], pebble.Sync); err != nil {
		t.Fatalf("setting stale legacy synced_to floor: %v", err)
	}

	registry, err := server.NewHeads(server.HeadsConfig{Net: testNet, Roots: roots, Manifests: manifests})
	if err != nil {
		t.Fatalf("server.NewHeads: %v", err)
	}
	rec, err := pinning.NewReconciler(pinning.Config{
		Ledger:      catalog.NewLedger(st.KV()),
		ManifestTip: manifests.Get,
	})
	if err != nil {
		t.Fatalf("pinning.NewReconciler: %v", err)
	}
	pub, _, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("ed25519.GenerateKey: %v", err)
	}
	f, err := follow.New(follow.Config{
		Net:        testNet,
		URL:        "https://writer.invalid",
		PubKey:     pub,
		Heads:      map[string]pinning.Policy{testHead: pinning.Full()},
		Local:      st.Blocks(),
		Sessions:   auditNoFetchSessions{},
		Registry:   registry,
		Roots:      roots,
		Reconciler: rec,
		KV:         st.KV(),
	})
	if err != nil {
		t.Fatalf("follow.New: %v", err)
	}
	t.Cleanup(func() {
		if err := f.Close(); err != nil {
			t.Errorf("Follower.Close: %v", err)
		}
	})

	if err := f.Resume(ctx); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if got := auditSyncedTo(t, registry); got != 120 {
		t.Fatalf("resumed synced_to = %d, want the checkpoint's 120", got)
	}
	// Root and tip came from ONE generation: the checkpoint's new tip, never the
	// stale legacy old tip.
	if got, ok := registry.ManifestTip(testHead); !ok || got != newTip {
		t.Fatalf("resumed registry tip = %s (ok=%t), want the checkpoint's %s, not the stale legacy floor %s",
			got, ok, newTip, oldTip)
	}
	// And the durable tip was not overwritten with the old one.
	if got, ok, err := manifests.Get(ctx, testHead); err != nil || !ok || got != newTip {
		t.Fatalf("durable tip after Resume = %s (ok=%t, err=%v), want it left at the checkpoint's %s",
			got, ok, err, newTip)
	}
}

func auditManifest(t *testing.T, st *store.Store, prev cid.Cid, from uint64) cid.Cid {
	t.Helper()
	raw, c, err := schema.EncodeManifest(&schema.Manifest{
		V:    schema.ManifestVersion,
		Head: testHead,
		Sources: []schema.Source{{
			Type:      schema.SourceInboxEvents,
			Address:   bytes.Repeat([]byte{1}, schema.AddressSize),
			Topic:     bytes.Repeat([]byte{2}, schema.TopicSize),
			FromBlock: from,
			OpenEnded: true,
		}},
		Prev: prev,
	})
	if err != nil {
		t.Fatalf("EncodeManifest: %v", err)
	}
	blk, err := blocks.NewBlockWithCid(raw, c)
	if err != nil {
		t.Fatalf("NewBlockWithCid: %v", err)
	}
	if err := st.Blocks().Put(t.Context(), blk); err != nil {
		t.Fatalf("storing manifest block: %v", err)
	}
	return c
}

func auditSyncedTo(t *testing.T, registry *server.Heads) uint64 {
	t.Helper()
	head, ok := registry.Get(testHead)
	if !ok {
		t.Fatal("followed head is not registered")
	}
	syncedTo, covered := head.SyncedTo()
	if !covered {
		t.Fatal("followed head is not covered")
	}
	return syncedTo
}
