package conformance

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ipfs/go-cid"

	"github.com/blobarchive/bloar/catalog"
	"github.com/blobarchive/bloar/core"
	"github.com/blobarchive/bloar/follow"
	"github.com/blobarchive/bloar/ingest"
	"github.com/blobarchive/bloar/pinning"
	"github.com/blobarchive/bloar/server"
	"github.com/blobarchive/bloar/store"
)

// httptestNewServer mounts h and closes it with the test.
func httptestNewServer(t *testing.T, h http.Handler) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return srv
}

// Spec 13.8: the follower conformance test. A writer archives the same fixtures
// test 13.1 uses; a follower on a real libp2p host tracks its published
// document, replicates the head over bitswap, and then nitro's own BlobClient
// runs the entire 13.1 suite against the follower's URL.
//
// The claim is the one spec 11.1 makes and everything in phase 8 rests on: a
// follower serves the entire public API, and a client cannot tell which end of
// the replication it is talking to. So the suite is not reimplemented here --
// it is the same test functions, pointed at a different URL. If a follower
// diverged from its writer in any way nitro can see, one of them fails.

// follower is a bloard that follows the writer's head and serves it.
type followerStack struct {
	*stack
	f *follow.Follower
}

// newFollowerStack wires the follower the way cmd/bloard does: a store, a
// registry, a reconciler, a libp2p host with bitswap, and follow.New over the
// lot, with the writer's URL and public key and a pin policy of its own.
func newFollowerStack(t *testing.T, w *stack, policy pinning.Policy) *followerStack {
	t.Helper()

	f := &followerStack{stack: newFollowerBase(t)}

	heads, err := server.NewHeads(server.HeadsConfig{
		Net:        testNet,
		Roots:      f.roots,
		Multiaddrs: f.announce(),
		OnRoot:     func(name string, _ cid.Cid) { f.rec.Notify(name) },
	})
	if err != nil {
		t.Fatalf("server.NewHeads: %v", err)
	}
	f.heads = heads

	cache, err := core.NewNodeCacheMB(1)
	if err != nil {
		t.Fatalf("core.NewNodeCacheMB: %v", err)
	}

	if f.f, err = follow.New(follow.Config{
		Net:        testNet,
		URL:        w.url,
		PubKey:     w.pubkey(),
		Verify:     follow.VerifyFull, // the strict mode, on the flagship test.
		Heads:      map[string]pinning.Policy{testHead: policy},
		Local:      f.store.Blocks(),
		Sessions:   f.ex,
		Host:       f.host,
		Registry:   f.heads,
		Roots:      f.roots,
		Reconciler: f.rec,
		KV:         f.store.KV(),
		Cache:      cache,
	}); err != nil {
		t.Fatalf("follow.New: %v", err)
	}
	t.Cleanup(func() {
		if err := f.f.Close(); err != nil {
			t.Errorf("closing follower: %v", err)
		}
	})

	f.serve()

	// One poll, synchronously: adopt the writer's root and fetch what the
	// policy retains. In a daemon this is the loop's first tick.
	//
	// Nothing has connected these two hosts. The publication document's
	// multiaddrs are how the follower finds the writer (spec 11.2), which is
	// part of what this test is for.
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()
	if err := f.f.Poll(ctx); err != nil {
		t.Fatalf("the follower's first poll: %v", err)
	}
	return f
}

// newFollowerBase is the half of newStack a follower shares with a writer: a
// store, a root store, a reconciler, and a libp2p host with bitswap. It opens no
// head of its own -- a follower's heads arrive from a document (spec 11.3) --
// which is the whole reason it is not newStack.
func newFollowerBase(t *testing.T) *stack {
	t.Helper()

	s := &stack{t: t}
	var err error
	if s.store, err = store.Open(t.TempDir(), store.WithPebbleLogger(quietPebble{})); err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() {
		if s.http != nil {
			s.http.Close()
		}
		if err := s.store.Close(); err != nil {
			t.Errorf("closing store: %v", err)
		}
	})

	s.roots = server.NewRootStore(s.store.KV())
	if s.rec, err = pinning.NewReconciler(pinning.Config{Ledger: catalog.NewLedger(s.store.KV())}); err != nil {
		t.Fatalf("pinning.NewReconciler: %v", err)
	}
	s.startP2P()
	return s
}

// serve mounts the read API over the follower's registry. It is the same
// server.New a writer builds: spec 11.1's "followers serve the read API" is not
// a second implementation of it.
func (s *stack) serve() {
	s.t.Helper()

	// A follower runs no ingest (spec 11.1), but POST /bloar/v1/blobs is always
	// mounted and server.Config requires something behind it. Nothing calls it.
	ing, err := ingest.New(ingest.Config{Blocks: s.store.Blocks(), Catalog: catalog.New(s.store.KV())})
	if err != nil {
		s.t.Fatalf("ingest.New: %v", err)
	}
	handler, err := server.New(server.Config{
		Heads:    s.heads,
		Blocks:   s.store.Blocks(),
		Ingester: ing,
		Beacon: server.Beacon{
			GenesisTime:           genesisTime,
			SecondsPerSlot:        secondsPerSlot,
			GenesisValidatorsRoot: "0x4b363db94e286120d76eb905340fdd4e54bfe9f06bf33ff6cf5ad27f511bfe95",
			GenesisForkVersion:    "0x00000000",
		},
		AuthToken: testToken,
	})
	if err != nil {
		s.t.Fatalf("server.New: %v", err)
	}
	s.http = httptestNewServer(s.t, handler)
	s.url = s.http.URL
}

// TestNitroSyncsFromAFollower is spec 13.8's flagship half: everything test
// 13.1 proves about a writer, proved again about a follower that has never been
// told anything but a URL, a public key, and a pin policy.
//
// The policy is full, so the follower holds every blob after its first poll and
// answers entirely from its own store. TestNitroFetchesOnDemandFromAFollower is
// the other end of that spectrum.
func TestNitroSyncsFromAFollower(t *testing.T) {
	w := newStack(t, withP2P)
	f := makeFixturesOn(t, w)

	follower := newFollowerStack(t, w, pinning.Full())
	suite := f.at(follower.url)

	// The writer is still up, because taking it down would prove something
	// narrower than what is claimed here. What matters is that every one of
	// these answers comes from blocks the follower fetched and holds; the
	// on-demand test below is where the writer's absence is the point.
	t.Run("initialize", func(t *testing.T) { nitroInitialize(t, suite) })
	t.Run("syncs_blobs", func(t *testing.T) { nitroSyncsBlobs(t, suite) })
	t.Run("derives_slot_from_header", func(t *testing.T) { nitroGetBlobsDerivesSlotFromHeader(t, suite) })
	t.Run("request_order_preserved", func(t *testing.T) { nitroRequestOrderPreserved(t, suite) })
	t.Run("verifies_proofs", func(t *testing.T) { nitroVerifiesProofs(t, suite) })
	t.Run("rejects_absent_blob", func(t *testing.T) { nitroRejectsAbsentBlob(t, suite) })
	t.Run("rejects_uncovered_slot", func(t *testing.T) { nitroRejectsUncoveredSlot(t, suite) })
}

// TestNitroFetchesOnDemandFromAFollower is spec 13.8's other half, and spec
// 11.4's reason to exist: a follower that retains no blobs at all still answers
// nitro, by fetching what it is asked for over bitswap while nitro waits.
//
// The pin policy is none -- the index and nothing else (spec 9) -- so every
// blob in this test is fetched from the writer at the moment nitro asks for it,
// and nitro verifies the KZG proof of each one. That is the deployment the
// window mode exists for, run at its limit: an archive that holds no data and
// serves all of it.
func TestNitroFetchesOnDemandFromAFollower(t *testing.T) {
	w := newStack(t, withP2P)
	f := makeFixturesOn(t, w)

	follower := newFollowerStack(t, w, pinning.None())

	// Nothing but the index: the claim under test is that the blobs come from
	// the writer during the requests below, not before them.
	if blobs := countBlobs(t, follower.stack); blobs != 0 {
		t.Fatalf("a none-policy follower holds %d blobs before anything asked for one, want 0", blobs)
	}

	client := newBlobClient(t, follower.url+"/"+testHead, nil)
	for _, slot := range []uint64{slotA, slotB, slotC} {
		want := f.blobsAt(slot)
		got, err := client.GetBlobsBySlot(t.Context(), slot, f.hashesAt(slot))
		if err != nil {
			t.Fatalf("slot %d: GetBlobsBySlot against a follower holding no blobs: %v", slot, err)
		}
		if len(got) != len(want) {
			t.Fatalf("slot %d: got %d blobs, want %d", slot, len(got), len(want))
		}
		for i := range want {
			if got[i] != want[i] {
				t.Errorf("slot %d: blob %d is not the bytes the writer archived", slot, i)
			}
		}
	}

	// The blobs are here now: an on-demand fetch caches (spec 11.4). They are
	// unpinned, so the next GC takes them back -- follow's own tests cover that.
	if blobs := countBlobs(t, follower.stack); blobs != fixtureBlobs {
		t.Errorf("the follower holds %d blobs after serving them all, want %d cached", blobs, fixtureBlobs)
	}

	// And the negative cases still answer from the index alone, which is what a
	// none policy retains in full: 404 for a blob no slot carries, 503 for a
	// slot the head has not reached.
	if _, err := client.GetBlobsBySlot(t.Context(), slotA, append(f.hashesAt(slotA), f.hashes[absentBlob])); err == nil {
		t.Error("the follower reported success for a blob the archive does not hold")
	}
	if _, err := client.GetBlobsBySlot(t.Context(), syncedTo+1, f.hashesAt(slotA)); err == nil {
		t.Error("the follower reported success for a slot beyond synced_to")
	}
}

// TestFollowerServesWithoutTheWriter: once a full-policy follower has synced, it
// is a complete archive. The writer's disappearance -- its HTTP, its libp2p host,
// everything -- does not change a single answer, which is spec 11.1's point that
// a writer need not be publicly reachable for reads.
func TestFollowerServesWithoutTheWriter(t *testing.T) {
	w := newStack(t, withP2P)
	f := makeFixturesOn(t, w)

	follower := newFollowerStack(t, w, pinning.Full())

	w.http.Close()
	if err := w.host.Close(); err != nil {
		t.Fatalf("closing the writer's host: %v", err)
	}

	nitroSyncsBlobs(t, f.at(follower.url))
}

// countBlobs counts the 128 KiB blocks a node holds. The blockstore is
// multihash-keyed and reports everything under a raw CID (see pinning's
// markKey), so size is what distinguishes a blob from an index block.
func countBlobs(t *testing.T, s *stack) int {
	t.Helper()

	keys, err := s.store.Blocks().AllKeysChan(t.Context())
	if err != nil {
		t.Fatalf("AllKeysChan: %v", err)
	}
	var blobs int
	for c := range keys {
		size, err := s.store.Blocks().GetSize(t.Context(), c)
		if err != nil {
			t.Fatalf("GetSize(%s): %v", c, err)
		}
		if size == blobSize {
			blobs++
		}
	}
	return blobs
}

// blobSize is EIP-4844's blob, in bytes. Spelled out rather than imported from
// bloar's schema so that this module's assertion about what a blob is does not
// come from the thing it is checking.
const blobSize = 131072
