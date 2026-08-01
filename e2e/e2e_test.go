package e2e

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"

	"github.com/blobarchive/bloar/index/beacon"
	chainidx "github.com/blobarchive/bloar/index/chain"
	"github.com/blobarchive/bloar/index/upstream"
	"github.com/blobarchive/bloar/schema"
)

// inboxSources is the shipped default filter these tests run under: one
// inbox-events source over testInbox, open-ended from the chain's start. It
// stands in for what deploy/examples/arbitrum-one.yaml configures.
func inboxSources() []chainidx.Source {
	return []chainidx.Source{{
		Type:      chainidx.SourceInboxEvents,
		Address:   testInbox,
		Topic:     chainidx.SequencerBatchDeliveredTopic,
		FromBlock: 0,
		OpenEnded: true,
	}}
}

// The synthetic chain, in one place.
//
// Eight blobs over twelve finalized slots, arranged so that every case the
// loops distinguish is in here somewhere:
//
//	slot 100  1 blob     a plain slot; also carries a CALLDATA batch (no row)
//	slot 101  -          an empty slot: the beacon 404s it, coverage advances
//	slot 102  3 blobs    a multi-blob slot; the inbox posts 2 of the 3
//	slot 103  -          a missed slot: another 404, and the end of window 12
//	slot 104  2 blobs    the inbox posts both, across two transactions, one of
//	                     which repeats a hash the other already posted (dedup)
//	slot 105  -          a gap...
//	slot 106  -
//	slot 107  -
//	slot 108  2 blobs    the inbox posts both, one per transaction (merge)
//	slot 109  -          a gap to the end of window 13, so the batch seals it
//	slot 110  -
//	slot 111  -
//
// origin_slot is 100 and seg_bits is 3, so window 12 is slots 96..103 and
// window 13 is 104..111: coverage to 111 seals both and leaves the directory
// two entries deep. That is the point of the trailing empty slots.
const (
	slotPlain     = 100
	slotMulti     = 102
	slotPair      = 104
	slotLast      = 108
	blobsInChain  = 8
	inboxBlobRefs = 6 // what the inbox actually posts: 2 at 102, 2 at 104, 2 at 108
)

// fixtures is the synthetic chain's blobs and where they live.
type fixtures struct {
	// blobs is every blob the beacon has, by index.
	blobs [][]byte
	// vhs is their versioned hashes, in the same order.
	vhs []schema.VersionedHash
	// slots is the beacon's view: slot -> blobs in block order.
	slots map[uint64][][]byte
	// inbox is the chain's view: slot -> the vhs the SequencerInbox posted
	// there, in the order it posted them.
	inbox map[uint64][]schema.VersionedHash
}

// newFixtures builds the chain described above.
func newFixtures() *fixtures {
	f := &fixtures{
		blobs: make([][]byte, blobsInChain),
		vhs:   make([]schema.VersionedHash, blobsInChain),
		slots: make(map[uint64][][]byte),
		inbox: make(map[uint64][]schema.VersionedHash),
	}
	for i := range blobsInChain {
		f.blobs[i] = makeBlob(uint64(i))
		f.vhs[i] = blobVH(f.blobs[i])
	}

	f.slots[slotPlain] = [][]byte{f.blobs[0]}
	f.slots[slotMulti] = [][]byte{f.blobs[1], f.blobs[2], f.blobs[3]}
	f.slots[slotPair] = [][]byte{f.blobs[4], f.blobs[5]}
	f.slots[slotLast] = [][]byte{f.blobs[6], f.blobs[7]}

	// The inbox posts a strict subset, and not in the beacon's order: blob 3
	// before blob 1 at slot 102, which is what makes "in-tx order" a testable
	// claim rather than a coincidence.
	f.inbox[slotMulti] = []schema.VersionedHash{f.vhs[3], f.vhs[1]}
	f.inbox[slotPair] = []schema.VersionedHash{f.vhs[4], f.vhs[5]}
	f.inbox[slotLast] = []schema.VersionedHash{f.vhs[6], f.vhs[7]}
	return f
}

// slotTime is the timestamp of an L1 block landing in slot.
func slotTime(slot uint64) uint64 { return genesisTime + slot*secondsPerSlot }

// firstChainSlot is where the fake parent chain's block 0 sits: ten slots
// before the head's origin, so that resume's binary search has somewhere to
// search and something to find. A chain that started exactly at origin_slot
// would make "find the first block at or after origin" trivially block 0.
const firstChainSlot = testOrigin - 10

// buildChain renders the fixtures as a parent chain: one L1 block per slot from
// firstChainSlot to testFinalSlot, carrying the inbox transactions.
func buildChain(t *testing.T, f *fixtures, inbox common.Address) *fakeChain {
	t.Helper()
	b := newChainBuilder(t, inbox)

	for slot := uint64(firstChainSlot); slot <= testFinalSlot; slot++ {
		blk := b.addBlock(slotTime(slot))

		switch slot {
		case slotPlain:
			// A calldata batch: spec 10.2's "non-blob batches produce no rows;
			// coverage still advances". The slot has a blob in the beacon's
			// view, so if this ever did produce a row, the arbitrum head would
			// pick up a blob the inbox never posted and the test would say so.
			b.addCalldataBatch(blk, []byte("calldata batch, not a blob batch"))

		case slotMulti:
			b.addBlobBatch(blk, hashes(f.inbox[slotMulti]))

		case slotPair:
			// Two transactions in one slot, the second repeating the first's
			// hash. The row must merge them in encounter order and dedup within
			// itself: [4, 5], not [4, 4, 5].
			b.addBlobBatch(blk, hashes(f.inbox[slotPair][:1]))
			b.addBlobBatch(blk, hashes(f.inbox[slotPair]))

		case slotLast:
			// Two transactions, one blob each: a plain merge into one row.
			b.addBlobBatch(blk, hashes(f.inbox[slotLast][:1]))
			b.addBlobBatch(blk, hashes(f.inbox[slotLast][1:]))
		}
	}
	return newFakeChain(t, b.blocks)
}

// hashes renders versioned hashes as the common.Hash a transaction carries.
func hashes(vhs []schema.VersionedHash) []common.Hash {
	out := make([]common.Hash, 0, len(vhs))
	for _, vh := range vhs {
		out = append(out, common.Hash(vh))
	}
	return out
}

// testInbox is the SequencerInbox address the fake chain's batches go to.
var testInbox = common.HexToAddress("0x1c479675ad559DC151F6Ec7ed3FbF8ceE79582B6")

// TestEndToEnd is spec 13.7.
//
// One bloard, both indexers, a fake beacon and a fake parent chain; run the
// loops until they are caught up; then assert what each head holds, and finally
// that a second bloard fed only by the first's read API arrives at the same
// root CID.
func TestEndToEnd(t *testing.T) {
	f := newFixtures()
	bn := newFakeBeacon(t, f.slots, testFinalSlot)
	chain := buildChain(t, f, testInbox)
	s := newStack(t, allHead, arbitrumHead)

	// Both loops, concurrently, against the one archive. Concurrently on
	// purpose: with fetch_blobs off, the arbitrum indexer must wait for the ALL
	// head to cover its batch before it can post refs (spec 10.2), and running
	// them in sequence would mean that wait always finds its answer already
	// there. This way the wait is a real one.
	runIndexers(t, s, bn, chain, false)

	t.Run("all head holds every beacon blob", func(t *testing.T) {
		assertAllHead(t, s, f)
	})
	t.Run("arbitrum head holds exactly the inbox refs", func(t *testing.T) {
		assertArbitrumHead(t, s, f)
	})
	t.Run("second archive re-derives the same root", func(t *testing.T) {
		assertRederivation(t, s, f)
	})
}

// runIndexers runs both loops against s until both heads are caught up.
func runIndexers(t *testing.T, s *stack, bn *fakeBeacon, chain *fakeChain, fetchBlobs bool) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	up, err := upstream.New(upstream.Config{BaseURL: bn.url, Backoff: time.Millisecond})
	if err != nil {
		t.Fatalf("upstream.New: %v", err)
	}
	blocks, err := upstream.NewBlockClient(upstream.Config{BaseURL: bn.url, Backoff: time.Millisecond})
	if err != nil {
		t.Fatalf("upstream.NewBlockClient: %v", err)
	}
	bx, err := beacon.New(beacon.Config{
		Sources: []beacon.Source{{Client: up}}, Blocks: blocks, Archive: s.client(), Head: allHead,
		// A batch size that does not divide the range, so the loop takes
		// several passes and the last one is short.
		BatchSize: 5, MaxPutBlobs: testMaxPutBlobs, PollInterval: time.Millisecond,
	})
	if err != nil {
		t.Fatalf("beacon.New: %v", err)
	}

	rpc, err := ethclient.DialContext(ctx, chain.url)
	if err != nil {
		t.Fatalf("ethclient.DialContext: %v", err)
	}
	defer rpc.Close()

	ax, err := chainidx.New(chainidx.Config{
		Chain: rpc, Archive: s.client(), Upstream: up,
		Head: arbitrumHead, AllHead: allHead,
		Sources:     inboxSources(),
		GenesisTime: genesisTime, SecondsPerSlot: secondsPerSlot,
		FetchBlobs: fetchBlobs,
		// Likewise: several scans rather than one, and a boundary that lands
		// mid-fixture.
		BlockRange: 7, MaxPutBlobs: testMaxPutBlobs, PollInterval: time.Millisecond,
	})
	if err != nil {
		t.Fatalf("chain.New: %v", err)
	}

	// A chain indexer runs only against a published manifest (spec 10.5, audit
	// the safety boundary): bootstrap the arbitrum head's genesis manifest attesting the inbox-
	// only schedule it is built with, so ax.Run's own startup CheckSchedule passes.
	if _, err := ax.PublishManifest(ctx, inboxSources()); err != nil {
		t.Fatalf("bootstrapping the arbitrum manifest: %v", err)
	}

	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for _, run := range []func(context.Context) error{bx.Run, ax.Run} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := run(ctx); err != nil {
				errs <- err
			}
		}()
	}

	// Caught up is a fact about the archive, not about the loops: both heads
	// cover the last finalized slot. The arbitrum head reaches the same 111
	// because its target is slot(latest finalized L1 block), and the fake
	// chain's last block sits in slot 111.
	waitFor(t, "both heads to reach the finalized slot", func() bool {
		all, allOK := s.syncedTo(allHead)
		arb, arbOK := s.syncedTo(arbitrumHead)
		return allOK && arbOK && all == testFinalSlot && arb == testFinalSlot
	}, errs)

	cancel()
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("indexer returned an error: %v", err)
	}
}

// waitFor blocks until cond holds, failing the test if it does not in time or
// if an indexer dies first.
func waitFor(t *testing.T, what string, cond func() bool, errs <-chan error) {
	t.Helper()
	deadline := time.After(30 * time.Second)
	for {
		if cond() {
			return
		}
		select {
		case err := <-errs:
			t.Fatalf("waiting for %s: an indexer stopped: %v", what, err)
		case <-deadline:
			t.Fatalf("timed out waiting for %s", what)
		case <-time.After(time.Millisecond):
		}
	}
}

// assertAllHead checks the ALL head against the beacon's own view: every blob
// the fake beacon has, at the slot it has it, in the order it stated it.
func assertAllHead(t *testing.T, s *stack, f *fixtures) {
	t.Helper()

	for slot := uint64(testOrigin); slot <= testFinalSlot; slot++ {
		want := f.slots[slot]
		got := getBlobs(t, s.url, allHead, slot)
		if len(got) != len(want) {
			t.Errorf("slot %d: head holds %d blobs, the beacon has %d", slot, len(got), len(want))
			continue
		}
		for i := range want {
			if got[i] != "0x"+hex.EncodeToString(want[i]) {
				t.Errorf("slot %d: blob %d does not match the beacon's bytes", slot, i)
			}
		}
	}

	// Every blob, addressed the way nitro addresses one: by slot and versioned
	// hash. This is the query that matters -- the read API's filtered path --
	// and it is a different code path from the unfiltered one above.
	for slot, blobs := range f.slots {
		for _, blob := range blobs {
			vh := blobVH(blob)
			got := getBlobs(t, s.url, allHead, slot, vh)
			if len(got) != 1 {
				t.Errorf("slot %d, vh %s: got %d blobs, want 1", slot, vhHex(vh), len(got))
				continue
			}
			if got[0] != "0x"+hex.EncodeToString(blob) {
				t.Errorf("slot %d, vh %s: bytes do not match", slot, vhHex(vh))
			}
		}
	}

	// A slot the beacon 404'd is covered and empty, not missing: the whole
	// point of advancing coverage over it. Spec 7.1 makes that a 200 with an
	// empty data array on the archive, where the beacon said 404.
	for _, slot := range []uint64{101, 103, 105, 111} {
		if got := getBlobs(t, s.url, allHead, slot); len(got) != 0 {
			t.Errorf("slot %d: the beacon has no blobs there, but the head returned %d", slot, len(got))
		}
	}

	// And one past coverage is still a 503, not a 404: the head must not claim
	// to have decided about a slot it has not reached.
	resp := get(t, blobsURL(s.url, allHead, testFinalSlot+1))
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("slot %d (past coverage): status = %d, want 503", testFinalSlot+1, resp.StatusCode)
	}
}

// assertArbitrumHead checks the chain head against the inbox's view: exactly
// the versioned hashes the SequencerInbox posted, at the slots it posted them,
// in the order it posted them -- and nothing else.
func assertArbitrumHead(t *testing.T, s *stack, f *fixtures) {
	t.Helper()

	for slot := uint64(testOrigin); slot <= testFinalSlot; slot++ {
		want := f.inbox[slot]
		got := getBlobs(t, s.url, arbitrumHead, slot)
		if len(got) != len(want) {
			t.Errorf("slot %d: the chain head holds %d blobs, the inbox posted %d", slot, len(got), len(want))
			continue
		}
		// Unfiltered, the head states its rows in stored order, which is the
		// order the refs were recorded in, which is the inbox's in-tx order.
		for i, vh := range want {
			blob, ok := findBlob(f.blobs, vh)
			if !ok {
				t.Fatalf("fixture error: no blob for %s", vhHex(vh))
			}
			if got[i] != "0x"+hex.EncodeToString(blob) {
				t.Errorf("slot %d: blob %d is not the one the inbox posted at that position", slot, i)
			}
		}
	}

	// The subset claim, stated the other way round: blob 2 is at slot 102 in
	// the ALL head, the inbox never posted it, and the chain head must 404 it
	// even though the slot itself is covered and carries other blobs.
	notPosted := f.vhs[2]
	resp := get(t, blobsURL(s.url, arbitrumHead, slotMulti, notPosted))
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("slot %d, a vh the inbox never posted: status = %d, want 404", slotMulti, resp.StatusCode)
	}

	// The fixture's own arithmetic, guarded: if this ever stops holding, the
	// assertions above are checking a chain that is not the one documented at
	// the top of this file.
	total := 0
	for _, vhs := range f.inbox {
		total += len(vhs)
	}
	if total != inboxBlobRefs {
		t.Errorf("fixture: the inbox posts %d refs, the file says %d", total, inboxBlobRefs)
	}
}

// assertRederivation is spec 11.5: a second archive, fed only by the first's
// read API, must arrive at the same root CID.
//
// This is a DETERMINISTIC-REPLICATION proof, not an independent honesty check --
// mirror mode has no block feed of its own, so the second stack copies the first's
// coverage decisions (a covered-empty answer included). The second stack shares
// nothing else with the first: its own temp store, its own blockstore, its own
// catalog. It never sees the fake beacon. Everything it knows arrives through GET
// /all/eth/v1/beacon/blobs/{slot} on the first archive -- the same endpoint nitro
// reads, the same loop pointed somewhere else. If the roots match, the head is a
// pure function of its parameters and the blobs the source covered, which is what
// makes forking, promotion and follower verification (spec 11.5, 11.4) mean
// anything; it does NOT prove the source omitted no real blob, which only anchored
// re-derivation against a trusted block feed can.
func assertRederivation(t *testing.T, s *stack, f *fixtures) {
	t.Helper()

	second := newStack(t, allHead)

	// The upstream is the first archive, in backfill mode: Head set, so its
	// finality is the first head's synced_to and its 404s are definitive.
	up, err := upstream.New(upstream.Config{BaseURL: s.url, Head: allHead, Backoff: time.Millisecond})
	if err != nil {
		t.Fatalf("upstream.New: %v", err)
	}
	if !up.IsArchive() {
		t.Fatal("an upstream with a head set must report itself as an archive")
	}

	bx, err := beacon.New(beacon.Config{
		// Mirror mode: the source archive is the one source, no block feed.
		Sources: []beacon.Source{{Client: up}}, Archive: second.client(), Head: allHead,
		// A different batch size from the first run's, deliberately: if the
		// root depended on how the coverage was chopped into batches, the two
		// would differ here and spec 11.5 would be false. Segments seal on
		// window boundaries, not batch boundaries, and this is the assertion
		// that says so.
		BatchSize: 4, MaxPutBlobs: testMaxPutBlobs, PollInterval: time.Millisecond,
	})
	if err != nil {
		t.Fatalf("beacon.New: %v", err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	for {
		advanced, err := bx.Step(ctx)
		if err != nil {
			t.Fatalf("re-deriving the all head: %v", err)
		}
		if !advanced {
			break
		}
	}

	if got, ok := second.syncedTo(allHead); !ok || got != testFinalSlot {
		t.Fatalf("the second archive's synced_to = %d (covered %v), want %d", got, ok, testFinalSlot)
	}

	want, got := s.root(allHead), second.root(allHead)
	if want != got {
		t.Errorf("spec 11.5: re-derived root\n got %s\nwant %s", got, want)
	}

	// And the blobs really are there, not merely referenced: the second archive
	// ingested every one of them itself, KZG and all.
	for slot, blobs := range f.slots {
		out := getBlobs(t, second.url, allHead, slot)
		if len(out) != len(blobs) {
			t.Errorf("second archive, slot %d: %d blobs, want %d", slot, len(out), len(blobs))
		}
	}
}

// TestEndToEndFetchBlobs is spec 10.2's other mode: the chain indexer fetching
// its own blobs rather than waiting for an ALL head.
//
// The archive here writes only the chain head -- there is no ALL head at all,
// which is the configuration the mode exists for. Everything the head holds was
// fetched, one filtered request per slot, from the beacon upstream.
func TestEndToEndFetchBlobs(t *testing.T) {
	f := newFixtures()
	bn := newFakeBeacon(t, f.slots, testFinalSlot)
	chain := buildChain(t, f, testInbox)
	s := newStack(t, arbitrumHead)

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	up, err := upstream.New(upstream.Config{BaseURL: bn.url, Backoff: time.Millisecond})
	if err != nil {
		t.Fatalf("upstream.New: %v", err)
	}
	rpc, err := ethclient.DialContext(ctx, chain.url)
	if err != nil {
		t.Fatalf("ethclient.DialContext: %v", err)
	}
	defer rpc.Close()

	ax, err := chainidx.New(chainidx.Config{
		Chain: rpc, Archive: s.client(), Upstream: up,
		Head:        arbitrumHead,
		Sources:     inboxSources(),
		GenesisTime: genesisTime, SecondsPerSlot: secondsPerSlot,
		FetchBlobs: true,
		BlockRange: 7, MaxPutBlobs: testMaxPutBlobs, PollInterval: time.Millisecond,
	})
	if err != nil {
		t.Fatalf("chain.New: %v", err)
	}

	// A chain indexer runs only against a published manifest (spec 10.5, audit
	// the safety boundary): bootstrap the genesis manifest, then verify the config against it so
	// the direct Step loop below is past the fail-closed guard.
	if _, err := ax.PublishManifest(ctx, inboxSources()); err != nil {
		t.Fatalf("bootstrapping the arbitrum manifest: %v", err)
	}
	if err := ax.CheckSchedule(ctx); err != nil {
		t.Fatalf("CheckSchedule before running the arbitrum indexer: %v", err)
	}

	for {
		advanced, err := ax.Step(ctx)
		if err != nil {
			t.Fatalf("running the arbitrum indexer: %v", err)
		}
		if !advanced {
			break
		}
	}

	if got, ok := s.syncedTo(arbitrumHead); !ok || got != testFinalSlot {
		t.Fatalf("synced_to = %d (covered %v), want %d", got, ok, testFinalSlot)
	}
	assertArbitrumHead(t, s, f)

	// The blobs the inbox did not post were never fetched and never stored:
	// fetch_blobs asks for exactly the versioned hashes the scan saw, which is
	// what keeps a chain archive the size of its chain rather than the size of
	// the beacon.
	resp := get(t, blobsURL(s.url, arbitrumHead, slotMulti, f.vhs[2]))
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("a blob the inbox never posted: status = %d, want 404", resp.StatusCode)
	}
}

// TestIndexerResumesFromTheArchive is spec 10's statelessness claim, which the
// runs above cannot show because they never stop.
//
// A fresh indexer -- new object, no memory of anything, in particular no hint
// for the arbitrum indexer's slot -> L1 block inverse -- pointed at a
// half-filled head must carry on from where the archive is, not from the start
// and not from wherever it left off. Nothing but GET synced_to tells it where
// that is.
func TestIndexerResumesFromTheArchive(t *testing.T) {
	f := newFixtures()
	bn := newFakeBeacon(t, f.slots, testFinalSlot)
	chain := buildChain(t, f, testInbox)
	s := newStack(t, allHead, arbitrumHead)

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	up, err := upstream.New(upstream.Config{BaseURL: bn.url, Backoff: time.Millisecond})
	if err != nil {
		t.Fatalf("upstream.New: %v", err)
	}
	newBeaconIndexer := func() *beacon.Indexer {
		blocks, err := upstream.NewBlockClient(upstream.Config{BaseURL: bn.url, Backoff: time.Millisecond})
		if err != nil {
			t.Fatalf("upstream.NewBlockClient: %v", err)
		}
		bx, err := beacon.New(beacon.Config{
			Sources: []beacon.Source{{Client: up}}, Blocks: blocks, Archive: s.client(), Head: allHead,
			BatchSize: 5, MaxPutBlobs: testMaxPutBlobs, PollInterval: time.Millisecond,
		})
		if err != nil {
			t.Fatalf("beacon.New: %v", err)
		}
		return bx
	}

	// One batch, then throw the indexer away.
	if advanced, err := newBeaconIndexer().Step(ctx); err != nil || !advanced {
		t.Fatalf("first batch: advanced = %v, err = %v", advanced, err)
	}
	after, ok := s.syncedTo(allHead)
	if !ok || after != testOrigin+4 {
		t.Fatalf("after one batch of 5 slots from origin %d: synced_to = %d (covered %v), want %d",
			testOrigin, after, ok, testOrigin+4)
	}

	// A brand new one finishes the job.
	for bx := newBeaconIndexer(); ; {
		advanced, err := bx.Step(ctx)
		if err != nil {
			t.Fatalf("resuming: %v", err)
		}
		if !advanced {
			break
		}
	}
	if got, _ := s.syncedTo(allHead); got != testFinalSlot {
		t.Fatalf("after resuming: synced_to = %d, want %d", got, testFinalSlot)
	}

	// The same for the arbitrum indexer, whose resume is the interesting one:
	// it has to invert slot -> L1 block by binary search, having never scanned
	// anything in this process.
	rpc, err := ethclient.DialContext(ctx, chain.url)
	if err != nil {
		t.Fatalf("ethclient.DialContext: %v", err)
	}
	defer rpc.Close()
	mkArbIndexer := func() *chainidx.Indexer {
		ax, err := chainidx.New(chainidx.Config{
			Chain: rpc, Archive: s.client(), Head: arbitrumHead, AllHead: allHead,
			Sources:     inboxSources(),
			GenesisTime: genesisTime, SecondsPerSlot: secondsPerSlot,
			BlockRange: 7, PollInterval: time.Millisecond,
		})
		if err != nil {
			t.Fatalf("chain.New: %v", err)
		}
		return ax
	}

	// A chain indexer runs only against a published manifest (spec 10.5, audit
	// the safety boundary): bootstrap the arbitrum head's genesis manifest once, then every fresh
	// indexer verifies its config against it before scanning -- which is what a
	// resumed process does too, and the point of statelessness this test is about.
	if _, err := mkArbIndexer().PublishManifest(ctx, inboxSources()); err != nil {
		t.Fatalf("bootstrapping the arbitrum manifest: %v", err)
	}
	newArbIndexer := func() *chainidx.Indexer {
		ax := mkArbIndexer()
		if err := ax.CheckSchedule(ctx); err != nil {
			t.Fatalf("CheckSchedule before running the arbitrum indexer: %v", err)
		}
		return ax
	}

	if advanced, err := newArbIndexer().Step(ctx); err != nil || !advanced {
		t.Fatalf("first scan: advanced = %v, err = %v", advanced, err)
	}
	firstTarget, ok := s.syncedTo(arbitrumHead)
	if !ok {
		t.Fatal("the arbitrum head has no coverage after a scan that advanced")
	}

	for ax := newArbIndexer(); ; {
		advanced, err := ax.Step(ctx)
		if err != nil {
			t.Fatalf("resuming the arbitrum indexer: %v", err)
		}
		if !advanced {
			break
		}
	}
	if got, _ := s.syncedTo(arbitrumHead); got != testFinalSlot {
		t.Fatalf("after resuming: synced_to = %d, want %d (first scan reached %d)", got, testFinalSlot, firstTarget)
	}
	assertArbitrumHead(t, s, f)
}

// TestBeaconIndexerStopsAtUpstreamCoverage is the 503 rule of spec 10.1: an
// archive upstream that has not covered a slot yet must stop the batch, and
// must not record the slot as empty.
//
// This is the failure the whole backfill mode turns on. A loop that read a 503
// as "no blobs here" would advance coverage over slots the upstream simply had
// not reached, and the resulting head would be permanently, silently short.
//
// # Getting the 503 to happen at all
//
// It does not happen on its own. The loop bounds every batch by F, and for an
// archive upstream F *is* that head's synced_to -- so the walk never reaches a
// slot the upstream would 503, and a test that merely half-fills an upstream
// proves nothing (it passes with the 503 case deleted; that was checked).
//
// The 503 needs the upstream's stated finality to run ahead of the coverage
// actually answering the blob reads. That is not a contrived situation: it is a
// lagging replica behind a load balancer, or a truncate (spec 5.4) landing
// between the two requests. So that is what this builds -- a real archive,
// correct in every answer it gives, behind a finality endpoint that overstates
// it.
func TestBeaconIndexerStopsAtUpstreamCoverage(t *testing.T) {
	f := newFixtures()
	bn := newFakeBeacon(t, f.slots, testFinalSlot)
	first := newStack(t, allHead)

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	// Fill the first archive only part way: up to slot 104, mid-fixture.
	const partial = 104
	beaconUp, err := upstream.New(upstream.Config{BaseURL: bn.url, Backoff: time.Millisecond})
	if err != nil {
		t.Fatalf("upstream.New: %v", err)
	}
	beaconBlocks, err := upstream.NewBlockClient(upstream.Config{BaseURL: bn.url, Backoff: time.Millisecond})
	if err != nil {
		t.Fatalf("upstream.NewBlockClient: %v", err)
	}
	fill, err := beacon.New(beacon.Config{
		Sources: []beacon.Source{{Client: beaconUp}}, Blocks: beaconBlocks, Archive: first.client(), Head: allHead,
		BatchSize: partial - testOrigin + 1, MaxPutBlobs: testMaxPutBlobs, PollInterval: time.Millisecond,
	})
	if err != nil {
		t.Fatalf("beacon.New: %v", err)
	}
	if _, err := fill.Step(ctx); err != nil {
		t.Fatalf("filling the first archive: %v", err)
	}
	if got, _ := first.syncedTo(allHead); got != partial {
		t.Fatalf("first archive synced_to = %d, want %d", got, partial)
	}

	// The lying front end: every blob read goes to the real archive, which
	// answers honestly and 503s everything past slot 104. Only synced_to is
	// overstated, to testFinalSlot.
	ahead := newStaleFinalityProxy(t, first.url, testFinalSlot)

	// Now backfill a second archive through it, with a batch that spans the
	// slots the upstream 503s.
	second := newStack(t, allHead)
	up, err := upstream.New(upstream.Config{BaseURL: ahead.url, Head: allHead, Backoff: time.Millisecond})
	if err != nil {
		t.Fatalf("upstream.New: %v", err)
	}
	bx, err := beacon.New(beacon.Config{
		// Mirror mode over the lying proxy: one trusted archive source, no block feed.
		Sources: []beacon.Source{{Client: up}}, Archive: second.client(), Head: allHead,
		BatchSize: 100, MaxPutBlobs: testMaxPutBlobs, PollInterval: time.Millisecond,
	})
	if err != nil {
		t.Fatalf("beacon.New: %v", err)
	}
	for {
		advanced, err := bx.Step(ctx)
		if err != nil {
			t.Fatalf("backfilling: %v", err)
		}
		if !advanced {
			break
		}
	}

	// It stopped exactly at the upstream's coverage. Not past it.
	got, ok := second.syncedTo(allHead)
	if !ok || got != partial {
		t.Fatalf("the backfilled archive's synced_to = %d (covered %v), want exactly the upstream's %d",
			got, ok, partial)
	}

	// And what it did record is right: the roots agree, on the same prefix.
	if want, got := first.root(allHead), second.root(allHead); want != got {
		t.Errorf("a backfill that stopped at the upstream's coverage should be the upstream, block for block:\n got %s\nwant %s", got, want)
	}

	// The slot after is not covered, so it 503s rather than 404ing: coverage
	// was never advanced over ground the upstream had not stated.
	resp := get(t, blobsURL(second.url, allHead, partial+1))
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("slot %d on the backfilled archive: status = %d, want 503 (it must not have been recorded as empty)",
			partial+1, resp.StatusCode)
	}
}

// blobsURL builds a read-API request.
func blobsURL(base, head string, slot uint64, vhs ...schema.VersionedHash) string {
	u := base + "/" + head + "/eth/v1/beacon/blobs/" + strconv.FormatUint(slot, 10)
	if len(vhs) > 0 {
		parts := make([]string, 0, len(vhs))
		for _, vh := range vhs {
			parts = append(parts, "versioned_hashes="+vhHex(vh))
		}
		u += "?" + strings.Join(parts, "&")
	}
	return u
}

// get issues a GET against a test server.
func get(t *testing.T, url string) *http.Response {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, nil)
	if err != nil {
		t.Fatalf("building GET %s: %v", url, err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	return resp
}

// getBlobs fetches a slot from a read API and asserts a 200, returning the data
// array.
func getBlobs(t *testing.T, base, head string, slot uint64, vhs ...schema.VersionedHash) []string {
	t.Helper()
	resp := get(t, blobsURL(base, head, slot, vhs...))
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("GET blobs %s slot %d: status = %d, body = %s", head, slot, resp.StatusCode, body)
	}
	var out struct {
		Data []string `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decoding blobs answer: %v", err)
	}
	return out.Data
}

// vhHex renders a versioned hash the way the API states one.
func vhHex(vh schema.VersionedHash) string { return "0x" + hex.EncodeToString(vh[:]) }
