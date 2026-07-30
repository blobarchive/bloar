package e2e

// This is the missed-source recovery drill of spec 5.4/10.4/10.5/11.3, end to
// end: a chain head synced under an incomplete filter, missing the blobs a
// source that should have been active never selected, and the mechanically
// enforced recovery -- truncate, publish the corrected manifest, resync -- with
// a follower riding through the dip on its floors and reconverging on the union
// history.
//
// It runs the whole stack the other e2e tests do (a real archive, the real chain
// indexer, a fake parent chain and beacon) and adds the two parts this scenario
// needs that they do not: a blob-txs source (spec 10.4's plain-EOA posting) in
// the fixtures, so there is a source to miss and re-derive, and a real follower
// over libp2p, so the reconvergence half of the drill is a follower built from
// the same parts a deployment runs rather than an assertion about one.

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/blobarchive/bloar/archive"
	"github.com/blobarchive/bloar/catalog"
	"github.com/blobarchive/bloar/core"
	"github.com/blobarchive/bloar/follow"
	"github.com/blobarchive/bloar/index/archclient"
	chainidx "github.com/blobarchive/bloar/index/chain"
	"github.com/blobarchive/bloar/index/upstream"
	"github.com/blobarchive/bloar/ingest"
	"github.com/blobarchive/bloar/p2p"
	"github.com/blobarchive/bloar/pinning"
	"github.com/blobarchive/bloar/schema"
	"github.com/blobarchive/bloar/server"
	"github.com/blobarchive/bloar/store"
)

// The drill's extra geometry, on top of the synthetic chain e2e_test.go
// documents. The inbox posts throughout (slots 102, 104, 108); a blob-txs
// source SHOULD have been active from L1 block missedFromBlock (slot 104)
// onward, posting the two EOA blobs -- but the initial schedule omits it, so
// history from there is short by exactly those two.
const (
	// missedFromBlock is where the missed blob-txs source begins: L1 block 14,
	// which is slot 104 on this chain (block n lands in slot firstChainSlot+n).
	// It is inside the inbox's covered ground, which is what makes the miss a
	// rewrite-of-covered-history to fix and not a mere append.
	missedFromBlock = 14
	// boundarySlot is where the recovery truncates back to: slot 103, the last
	// slot of window 12 (slots 96..103). Truncating to a window-final slot
	// exercises the seal rule of spec 5.4, and 103 sits one slot below slot 104,
	// so a resync from there re-derives every block the missed source covers.
	boundarySlot = 103
	// eoaSlotA and eoaSlotB are where the EOA posts its two blobs: slots 106 and
	// 110 (L1 blocks 16 and 20), both above the truncation boundary and both
	// otherwise-empty in the inbox-only fixture, so a baseline head covers them
	// empty and a recovered head fills them.
	eoaSlotA = 106
	eoaSlotB = 110
	// The EOA blobs' seeds, distinct from the inbox blobs' 0..7.
	eoaSeedA = 100
	eoaSeedB = 101
)

var (
	// eoaRecipient is the plain address the EOA posts its blob transactions to
	// (spec 10.4's blob-txs recipient), distinct from the SequencerInbox.
	eoaRecipient = common.HexToAddress("0x000000000000000000000000000000000000ee0a")
	// eoaSenderKey signs the EOA's blob transactions; its address is the one the
	// corrected schedule allowlists. A different key from the inbox's chainKey,
	// so the sender the scan recovers is genuinely a distinct party.
	eoaSenderKey, _ = crypto.HexToECDSA("59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d")
	eoaSender       = crypto.PubkeyToAddress(eoaSenderKey.PublicKey)
)

// correctedSources is the schedule the recovery publishes: the inbox source
// stays open (a plain ADD, not a close-and-add), and the blob-txs source is
// added from missedFromBlock. Close-and-add is spec 10.4's shape for changing a
// source that is still open -- most often a sender allowlist -- by capping it and
// appending its replacement. This is not that: the inbox source was never wrong
// and keeps governing its own blobs unchanged; what was missing is a whole second
// posting mechanism the head never described. So the inbox source is left
// untouched and the blob-txs source is appended, and union-with-dedup (spec 10.4)
// makes the two harmless to overlap.
func correctedSources() []chainidx.Source {
	return append(inboxSources(), chainidx.Source{
		Type:      chainidx.SourceBlobTxs,
		Address:   eoaRecipient,
		Senders:   []common.Address{eoaSender},
		FromBlock: missedFromBlock,
		OpenEnded: true,
	})
}

// recoveryFixtures is the base synthetic chain plus the two EOA blobs the missed
// source posts.
type recoveryFixtures struct {
	*fixtures
	eoaBlobs [][]byte
	eoaVHs   []schema.VersionedHash
	// eoa is the EOA's view: slot -> the versioned hashes it posted there.
	eoa map[uint64][]schema.VersionedHash
}

func newRecoveryFixtures() *recoveryFixtures {
	base := newFixtures()
	a, b := makeBlob(eoaSeedA), makeBlob(eoaSeedB)
	rf := &recoveryFixtures{
		fixtures: base,
		eoaBlobs: [][]byte{a, b},
		eoaVHs:   []schema.VersionedHash{blobVH(a), blobVH(b)},
		eoa:      map[uint64][]schema.VersionedHash{},
	}
	// The beacon has every blob, EOA ones included: an ALL head or a fetch_blobs
	// indexer must be able to fetch them once the corrected schedule names them.
	base.slots[eoaSlotA] = [][]byte{a}
	base.slots[eoaSlotB] = [][]byte{b}
	rf.eoa[eoaSlotA] = []schema.VersionedHash{rf.eoaVHs[0]}
	rf.eoa[eoaSlotB] = []schema.VersionedHash{rf.eoaVHs[1]}
	return rf
}

// buildRecoveryChain renders the recovery fixtures as a parent chain: the inbox
// batches buildChain already posts, plus the EOA's blob transactions at eoaSlotA
// and eoaSlotB.
func buildRecoveryChain(t *testing.T, rf *recoveryFixtures) *fakeChain {
	t.Helper()
	b := newChainBuilder(t, testInbox)
	for slot := uint64(firstChainSlot); slot <= testFinalSlot; slot++ {
		blk := b.addBlock(slotTime(slot))
		switch slot {
		case slotPlain:
			b.addCalldataBatch(blk, []byte("calldata batch, not a blob batch"))
		case slotMulti:
			b.addBlobBatch(blk, hashes(rf.inbox[slotMulti]))
		case slotPair:
			b.addBlobBatch(blk, hashes(rf.inbox[slotPair][:1]))
			b.addBlobBatch(blk, hashes(rf.inbox[slotPair]))
		case slotLast:
			b.addBlobBatch(blk, hashes(rf.inbox[slotLast][:1]))
			b.addBlobBatch(blk, hashes(rf.inbox[slotLast][1:]))
		case eoaSlotA:
			b.addBlobTxFrom(blk, eoaSenderKey, eoaRecipient, hashes(rf.eoa[eoaSlotA]))
		case eoaSlotB:
			b.addBlobTxFrom(blk, eoaSenderKey, eoaRecipient, hashes(rf.eoa[eoaSlotB]))
		}
	}
	return newFakeChain(t, b.blocks)
}

// TestMissedSourceRecovery is the capstone drill (spec 5.4, 10.4, 10.5, 11.3).
//
// The stages, each gated:
//
//	baseline        the head syncs under the inbox-only schedule; the EOA blobs
//	                are absent, and a genesis manifest attests the incomplete
//	                filter. A follower adopts this head.
//	who-refuses     the indexer refuses to run the corrected schedule while the
//	                position sits ahead of the added source (CheckSchedule), and
//	                the append-only rule is illegal at that position and legal
//	                below it (ValidateUpgrade); the server, which cannot see L1,
//	                accepts the corrected manifest regardless.
//	truncate        the head is truncated to the window-final boundary slot; its
//	                coverage drops and its root changes.
//	resync          a corrected-schedule indexer re-derives the truncated range;
//	                the EOA blobs are now covered.
//	reconverge      the follower refuses every document through the dip on its
//	                synced_to floor, then adopts the recovered root, accepts the
//	                corrected tip through the manifest-ancestry floor, serves the
//	                union, and GCs the divergent old segment.
func TestMissedSourceRecovery(t *testing.T) {
	rf := newRecoveryFixtures()
	bn := newFakeBeacon(t, rf.slots, testFinalSlot)
	chain := buildRecoveryChain(t, rf)
	w := newRecoveryWriter(t)

	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
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

	// --- baseline: bootstrap the manifest, then sync under it ---

	// A chain indexer runs only against a published manifest (spec 10.5, audit
	// the safety boundary), so the head is bootstrapped first: the append-only preflight --
	// PublishManifest, the same one `bloar-index publish-manifest` runs -- publishes
	// a genesis manifest attesting the inbox-only schedule the head is built with.
	// (An existing v1 head is bootstrapped the same way, attesting what it was
	// already built with; here the drill is a head whose filter is attested from the
	// start and then corrected.) Its incompleteness is the whole scenario.
	genesisTip, err := publishManifest(t, ctx, rpc, w.client(), up, inboxSources())
	if err != nil {
		t.Fatalf("genesis manifest preflight: %v", err)
	}
	if tip, n := getManifest(t, w.url); tip != genesisTip || n != 1 {
		t.Fatalf("genesis manifest: tip %s with %d sources, want %s with 1", tip, n, genesisTip)
	}

	// The head syncs under the inbox-only schedule, which equals the published tip,
	// so CheckSchedule passes and each batch binds to the genesis tip.
	runChainIndexer(t, ctx, newRecoveryIndexer(t, rpc, w.client(), up, inboxSources()))
	if got, ok := w.syncedTo(); !ok || got != testFinalSlot {
		t.Fatalf("baseline synced_to = %d (covered %v), want %d", got, ok, testFinalSlot)
	}
	baselineRoot := w.root()

	// The inbox blobs serve; the EOA blobs the inbox never posted are absent.
	assertInboxBlobsServe(t, w.url, rf)
	assertEOABlobsAbsent(t, w.url, rf)

	// --- baseline: a follower adopts the incomplete head ---

	f := newRecoveryFollower(t, w)
	f.poll()
	if got := f.syncedToFloor(); got != testFinalSlot {
		t.Fatalf("follower adopted synced_to floor = %d, want %d", got, testFinalSlot)
	}
	if tip, ok := f.adoptedTip(); !ok || tip.String() != genesisTip {
		t.Fatalf("follower adopted manifest tip = %s (ok=%t), want the genesis %s", tip, ok, genesisTip)
	}
	assertInboxBlobsServe(t, f.url, rf)
	assertEOABlobsAbsent(t, f.url, rf)
	baselineBlocks := f.blockSet()

	// --- who refuses what ---

	// The preflight refuses to publish the corrected schedule while the head's
	// position is ahead of the added source: the blob-txs source begins at block
	// 14, but synced_to maps to a block well past it, so adding it now would rewrite
	// covered history. PublishManifest reads the published tip (still the genesis
	// inbox-only manifest) and the live synced_to, maps it to L1, validates the
	// decoded predecessor, and refuses -- naming the recovery order, which is the
	// fix. This is the append-only guardrail, and it is the indexer's, because only
	// it sees L1.
	if _, err := publishManifest(t, ctx, rpc, w.client(), up, correctedSources()); err == nil {
		t.Fatal("preflight accepted the corrected schedule pre-truncate; it rewrites covered history")
	} else {
		for _, want := range []string{"covering L1 block", "truncate the head"} {
			if !bytes.Contains([]byte(err.Error()), []byte(want)) {
				t.Errorf("preflight refusal does not mention %q: %v", want, err)
			}
		}
	}

	// The mechanism beneath that refusal, shown pure: the corrected schedule is an
	// illegal successor of the inbox-only one at a position at or past block 14
	// (synced_to 111 maps to L1 block 21 on this chain), and a legal one at a
	// position below it (synced_to 103 maps to block 13). Truncate is what moves
	// the position from the first to the second, which is the whole reason the
	// order is truncate-first.
	if err := chainidx.ValidateUpgrade(inboxSources(), correctedSources(), 21); err == nil {
		t.Error("ValidateUpgrade accepted the added source at a position past its from_block")
	}
	if err := chainidx.ValidateUpgrade(inboxSources(), correctedSources(), 13); err != nil {
		t.Errorf("ValidateUpgrade refused the added source at a position below its from_block: %v", err)
	}

	// The running indexer refuses the corrected config for a second, independent
	// reason: startup is now exact equality against the published tip (spec 10.5,
	// the safety boundary), and the corrected config does not equal the genesis tip. There
	// is no way to get it running before its manifest is legally published, and the
	// only order that publishes it legally is truncate-first.
	if err := newRecoveryIndexer(t, rpc, w.client(), up, correctedSources()).CheckSchedule(ctx); err == nil {
		t.Fatal("CheckSchedule ran a config that does not equal the published tip")
	}

	// Nothing has been published or recovered: the tip is still the genesis
	// inbox-only manifest, and the EOA blobs are still absent.
	if tip, n := getManifest(t, w.url); tip != genesisTip || n != 1 {
		t.Fatalf("pre-truncate tip changed: tip %s with %d sources, want the genesis %s with 1", tip, n, genesisTip)
	}
	assertEOABlobsAbsent(t, w.url, rf)

	// --- truncate ---

	root := truncate(t, w.url, boundarySlot)
	if got, ok := w.syncedTo(); !ok || got != boundarySlot {
		t.Fatalf("after truncate, synced_to = %d (covered %v), want %d", got, ok, boundarySlot)
	}
	if root == baselineRoot {
		t.Fatal("truncate did not change the root")
	}

	// --- publish, now legal ---

	// With the position moved back below block 14, the corrected schedule is a legal
	// successor: the same preflight that refused now publishes it, chained onto the
	// genesis tip. This is the publish step of the recovery order, legal only now.
	correctedTip, err := publishManifest(t, ctx, rpc, w.client(), up, correctedSources())
	if err != nil {
		t.Fatalf("preflight refused the corrected schedule post-truncate: %v", err)
	}
	if tip, n := getManifest(t, w.url); tip != correctedTip || n != 2 {
		t.Fatalf("after the corrected publish: tip %s with %d sources, want %s with 2", tip, n, correctedTip)
	}

	// And the restarted indexer's CheckSchedule now passes: the config equals the tip.
	if err := newRecoveryIndexer(t, rpc, w.client(), up, correctedSources()).CheckSchedule(ctx); err != nil {
		t.Fatalf("indexer refused the corrected schedule post-truncate: %v", err)
	}

	// --- follower rides through the dip ---

	// The document is now truncated -- synced_to has dipped below the follower's
	// adopted floor -- but the resync has not yet climbed it back. The follower
	// polled here refuses: adopting a document below the floor would take back
	// slots it already served. It keeps serving its last good state. This is the
	// window an operator must NOT "fix" (spec 11.3): the refusal is the system
	// working, not a fault.
	dip := f.pollErr()
	if dip == nil {
		t.Fatal("follower adopted a truncated document below its synced_to floor")
	}
	if !bytes.Contains([]byte(dip.Error()), []byte("below the adopted floor")) {
		t.Errorf("dip refusal is not the synced_to floor's: %v", dip)
	}
	// It froze, it did not regress: still the baseline root, still serving the
	// inbox blobs, still 404ing the EOA ones.
	if got := f.adoptedRoot(); got != baselineRoot {
		t.Errorf("follower moved off the baseline root during the dip: %s", got)
	}
	assertInboxBlobsServe(t, f.url, rf)
	assertEOABlobsAbsent(t, f.url, rf)

	// --- resync ---

	runChainIndexer(t, ctx, newRecoveryIndexer(t, rpc, w.client(), up, correctedSources()))
	if got, ok := w.syncedTo(); !ok || got != testFinalSlot {
		t.Fatalf("resync synced_to = %d (covered %v), want %d", got, ok, testFinalSlot)
	}
	// The re-derived range now carries the EOA blobs the inbox-only pass missed,
	// and the inbox blobs it did have are unchanged. The root differs from the
	// baseline: the union added versioned hashes over the truncated range.
	assertInboxBlobsServe(t, w.url, rf)
	assertEOABlobsServe(t, w.url, rf)
	if w.root() == baselineRoot {
		t.Fatal("the recovered root equals the baseline root; the union added no blobs")
	}

	// --- follower reconvergence ---

	// The resync has climbed synced_to back to testFinalSlot, so the document the
	// follower now polls is at or above its floor and carries the corrected tip. It
	// adopts: the synced_to floor is cleared, and the manifest-ancestry floor
	// accepts the corrected tip because its prev-lineage walks back through the
	// genesis tip the follower holds -- a legal extension, not a rewritten history.
	f.poll()
	if got := f.adoptedRoot(); got != w.root() {
		t.Fatalf("follower did not adopt the recovered root: have %s, want %s", got, w.root())
	}
	if tip, ok := f.adoptedTip(); !ok || tip.String() != correctedTip {
		t.Fatalf("follower manifest floor = %s (ok=%t), want the corrected tip %s", tip, ok, correctedTip)
	}
	// It now serves the union: the inbox blobs and the recovered EOA blobs both.
	assertInboxBlobsServe(t, f.url, rf)
	assertEOABlobsServe(t, f.url, rf)

	// Reconvergence is complete when the divergent old segment is GC-able. The
	// truncate rebuilt the head's spine above the cut, so the pre-recovery segment
	// covering the truncated range is no longer referenced by any root the
	// follower holds; a reconcile drops its pin and a sweep collects it, while the
	// segments below the cut keep their CIDs and their pins. Assert the nearest
	// honest thing the harness can show without reaching into the archive's
	// internals: after the follower reconciles and GCs, at least one index block it
	// held at baseline is gone, and the union history still serves through the
	// sweep.
	f.reconcile()
	f.gc()
	afterBlocks := f.blockSet()
	sweptIndex := 0
	for c, size := range baselineBlocks {
		if _, ok := afterBlocks[c]; !ok && size != schema.BlobSize {
			sweptIndex++
		}
	}
	if sweptIndex == 0 {
		t.Error("no pre-recovery index block was swept; the divergent old segment was expected to become GC-able")
	}
	assertInboxBlobsServe(t, f.url, rf)
	assertEOABlobsServe(t, f.url, rf)
}

// assertInboxBlobsServe checks the inbox blobs serve at their slots, addressed by
// versioned hash the way nitro reads one.
func assertInboxBlobsServe(t *testing.T, base string, rf *recoveryFixtures) {
	t.Helper()
	for slot, vhs := range rf.inbox {
		for _, vh := range vhs {
			blob, ok := findBlob(rf.blobs, vh)
			if !ok {
				t.Fatalf("fixture error: no blob for %s", vhHex(vh))
			}
			got := getBlobs(t, base, arbitrumHead, slot, vh)
			if len(got) != 1 || got[0] != "0x"+hex.EncodeToString(blob) {
				t.Errorf("slot %d, inbox vh %s: not served correctly (got %d blobs)", slot, vhHex(vh), len(got))
			}
		}
	}
}

// assertEOABlobsServe checks the EOA blobs serve at their slots.
func assertEOABlobsServe(t *testing.T, base string, rf *recoveryFixtures) {
	t.Helper()
	for slot, vhs := range rf.eoa {
		for _, vh := range vhs {
			blob, ok := findBlob(rf.eoaBlobs, vh)
			if !ok {
				t.Fatalf("fixture error: no EOA blob for %s", vhHex(vh))
			}
			got := getBlobs(t, base, arbitrumHead, slot, vh)
			if len(got) != 1 || got[0] != "0x"+hex.EncodeToString(blob) {
				t.Errorf("slot %d, EOA vh %s: not served correctly (got %d blobs)", slot, vhHex(vh), len(got))
			}
		}
	}
}

// assertEOABlobsAbsent checks the EOA blobs are not in the head: a request for
// one at its covered slot 404s, because the slot is covered and the vh is
// definitively not there (spec 7.1).
func assertEOABlobsAbsent(t *testing.T, base string, rf *recoveryFixtures) {
	t.Helper()
	for slot, vhs := range rf.eoa {
		for _, vh := range vhs {
			resp := get(t, blobsURL(base, arbitrumHead, slot, vh))
			resp.Body.Close()
			if resp.StatusCode != http.StatusNotFound {
				t.Errorf("slot %d, EOA vh %s: status = %d, want 404 (the head must not hold it yet)",
					slot, vhHex(vh), resp.StatusCode)
			}
		}
	}
}

// --- the drill's writer: a signed, block-serving bloard over the arbitrum head ---

// recoveryWriter is newStack plus what a follower needs from a writer (spec
// 11.1): a signing key, a libp2p host that serves the head's blocks over bitswap,
// and a manifest store so the manifest chain can advance.
type recoveryWriter struct {
	t     *testing.T
	store *store.Store
	heads *server.Heads
	host  *p2p.Host
	ex    *p2p.Exchange
	url   string
	key   ed25519.PrivateKey
}

func newRecoveryWriter(t *testing.T) *recoveryWriter {
	t.Helper()
	ctx := t.Context()
	w := &recoveryWriter{t: t}

	var err error
	if w.store, err = store.Open(t.TempDir(), store.WithPebbleLogger(quietPebble{})); err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() {
		if err := w.store.Close(); err != nil {
			t.Errorf("closing writer store: %v", err)
		}
	})

	cache, err := core.NewNodeCacheMB(1)
	if err != nil {
		t.Fatalf("core.NewNodeCacheMB: %v", err)
	}
	roots := server.NewRootStore(w.store.KV())
	manifests := server.NewManifestStore(w.store.KV())
	cat := catalog.New(w.store.KV())

	if _, w.key, err = ed25519.GenerateKey(nil); err != nil {
		t.Fatalf("generating a signing key: %v", err)
	}
	if w.host, err = p2p.NewHost(ctx, p2p.HostConfig{
		Listen:          []string{"/ip4/127.0.0.1/tcp/0"},
		IdentityKeyFile: filepath.Join(t.TempDir(), "p2p.key"),
	}); err != nil {
		t.Fatalf("p2p.NewHost: %v", err)
	}
	t.Cleanup(func() {
		if err := w.host.Close(); err != nil {
			t.Errorf("closing writer host: %v", err)
		}
	})
	docs, err := p2p.NewDocBlockstore(w.store.Blocks())
	if err != nil {
		t.Fatalf("p2p.NewDocBlockstore: %v", err)
	}
	if w.ex, err = p2p.NewExchange(ctx, p2p.ExchangeConfig{Host: w.host, Blocks: docs}); err != nil {
		t.Fatalf("p2p.NewExchange: %v", err)
	}
	t.Cleanup(func() {
		if err := w.ex.Close(); err != nil {
			t.Errorf("closing writer exchange: %v", err)
		}
	})

	// Signed, and with a manifest store and blockstore wired: SetManifest needs
	// both (a tip with no block is a dangling CID), and a follower needs the
	// signature and the multiaddrs.
	if w.heads, err = server.NewHeads(server.HeadsConfig{
		Net:        testNet,
		Roots:      roots,
		Manifests:  manifests,
		Blocks:     w.store.Blocks(),
		Multiaddrs: w.host.AnnounceAddrs(),
		SigningKey: w.key,
	}); err != nil {
		t.Fatalf("server.NewHeads: %v", err)
	}

	head, err := server.OpenHead(ctx,
		archive.Config{Blocks: w.store.Blocks(), Resolver: cat, Cache: cache}, roots,
		archive.Params{
			Name: arbitrumHead, Net: testNet, OriginSlot: testOrigin,
			SegBits: testSegBits, FanoutBits: testFanoutBits,
		})
	if err != nil {
		t.Fatalf("server.OpenHead: %v", err)
	}
	if err := w.heads.Add(head); err != nil {
		t.Fatalf("Heads.Add: %v", err)
	}

	ingester, err := ingest.New(ingest.Config{Blocks: w.store.Blocks(), Catalog: cat})
	if err != nil {
		t.Fatalf("ingest.New: %v", err)
	}
	handler, err := server.New(server.Config{
		Heads:    w.heads,
		Blocks:   w.store.Blocks(),
		Ingester: ingester,
		Beacon: server.Beacon{
			GenesisTime:           genesisTime,
			SecondsPerSlot:        secondsPerSlot,
			GenesisValidatorsRoot: "0x4b363db94e286120d76eb905340fdd4e54bfe9f06bf33ff6cf5ad27f511bfe95",
			GenesisForkVersion:    "0x00000000",
		},
		AuthToken:   testToken,
		MaxPutBlobs: testMaxPutBlobs,
	})
	if err != nil {
		t.Fatalf("server.New: %v", err)
	}
	httpSrv := httptest.NewServer(handler)
	t.Cleanup(httpSrv.Close)
	w.url = httpSrv.URL
	return w
}

func (w *recoveryWriter) client() *archclient.Client {
	w.t.Helper()
	c, err := archclient.New(archclient.Config{BaseURL: w.url, Token: testToken, Backoff: time.Millisecond})
	if err != nil {
		w.t.Fatalf("archclient.New: %v", err)
	}
	return c
}

func (w *recoveryWriter) syncedTo() (uint64, bool) {
	w.t.Helper()
	head, ok := w.heads.Get(arbitrumHead)
	if !ok {
		w.t.Fatalf("writer has no head %q", arbitrumHead)
	}
	return head.SyncedTo()
}

func (w *recoveryWriter) root() string {
	w.t.Helper()
	head, ok := w.heads.Get(arbitrumHead)
	if !ok {
		w.t.Fatalf("writer has no head %q", arbitrumHead)
	}
	return head.Root().String()
}

// --- the drill's follower: a real follower over libp2p ---

// recoveryFollower is a follower of the writer's arbitrum head, built from the
// same parts follow/ tests use: its own store, a libp2p host dialled to the
// writer, a reconciler and staging for the fetch pass, and an HTTP server so its
// reads can be asserted.
type recoveryFollower struct {
	t         *testing.T
	store     *store.Store
	heads     *server.Heads
	manifests *server.ManifestStore
	rec       *pinning.Reconciler
	f         *follow.Follower
	url       string
}

func newRecoveryFollower(t *testing.T, w *recoveryWriter) *recoveryFollower {
	t.Helper()
	ctx := t.Context()
	fl := &recoveryFollower{t: t}

	var err error
	if fl.store, err = store.Open(t.TempDir(), store.WithPebbleLogger(quietPebble{})); err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() {
		if err := fl.store.Close(); err != nil {
			t.Errorf("closing follower store: %v", err)
		}
	})

	cache, err := core.NewNodeCacheMB(1)
	if err != nil {
		t.Fatalf("core.NewNodeCacheMB: %v", err)
	}
	roots := server.NewRootStore(fl.store.KV())
	fl.manifests = server.NewManifestStore(fl.store.KV())

	host, err := p2p.NewHost(ctx, p2p.HostConfig{
		Listen:          []string{"/ip4/127.0.0.1/tcp/0"},
		IdentityKeyFile: filepath.Join(t.TempDir(), "p2p.key"),
	})
	if err != nil {
		t.Fatalf("p2p.NewHost: %v", err)
	}
	t.Cleanup(func() {
		if err := host.Close(); err != nil {
			t.Errorf("closing follower host: %v", err)
		}
	})
	docs, err := p2p.NewDocBlockstore(fl.store.Blocks())
	if err != nil {
		t.Fatalf("p2p.NewDocBlockstore: %v", err)
	}
	ex, err := p2p.NewExchange(ctx, p2p.ExchangeConfig{Host: host, Blocks: docs})
	if err != nil {
		t.Fatalf("p2p.NewExchange: %v", err)
	}
	t.Cleanup(func() {
		if err := ex.Close(); err != nil {
			t.Errorf("closing follower exchange: %v", err)
		}
	})

	if fl.rec, err = pinning.NewReconciler(pinning.Config{
		Ledger:      catalog.NewLedger(fl.store.KV()),
		ManifestTip: fl.manifests.Get,
	}); err != nil {
		t.Fatalf("pinning.NewReconciler: %v", err)
	}
	if fl.heads, err = server.NewHeads(server.HeadsConfig{
		Net:        testNet,
		Roots:      roots,
		Manifests:  fl.manifests,
		Blocks:     fl.store.Blocks(),
		Multiaddrs: host.AnnounceAddrs(),
		OnRoot:     func(name string, _ cid.Cid) { fl.rec.Notify(name) },
	}); err != nil {
		t.Fatalf("server.NewHeads: %v", err)
	}
	staging, err := pinning.NewStaging(pinning.StagingConfig{
		Ledger:   catalog.NewLedger(fl.store.KV()),
		Resolver: catalog.New(fl.store.KV()),
	})
	if err != nil {
		t.Fatalf("pinning.NewStaging: %v", err)
	}

	if fl.f, err = follow.New(follow.Config{
		Net:        testNet,
		URL:        w.url,
		PubKey:     w.key.Public().(ed25519.PublicKey),
		Heads:      map[string]pinning.Policy{arbitrumHead: pinning.Full()},
		Local:      fl.store.Blocks(),
		Sessions:   ex,
		Host:       host,
		Registry:   fl.heads,
		Roots:      roots,
		Reconciler: fl.rec,
		Staging:    staging,
		KV:         fl.store.KV(),
		Cache:      cache,
	}); err != nil {
		t.Fatalf("follow.New: %v", err)
	}
	t.Cleanup(func() {
		if err := fl.f.Close(); err != nil {
			t.Errorf("closing follower: %v", err)
		}
	})

	// Dial the writer directly, the way p2p.peers would, so the test fails about
	// the protocol rather than about how long a dial took.
	if err := host.Libp2p().Connect(ctx, peer.AddrInfo{ID: w.host.ID(), Addrs: w.host.Libp2p().Addrs()}); err != nil {
		t.Fatalf("connecting the follower to the writer: %v", err)
	}

	ingester, err := ingest.New(ingest.Config{Blocks: fl.store.Blocks(), Catalog: catalog.New(fl.store.KV())})
	if err != nil {
		t.Fatalf("ingest.New (follower): %v", err)
	}
	handler, err := server.New(server.Config{
		Heads:    fl.heads,
		Blocks:   fl.store.Blocks(),
		Ingester: ingester,
		Beacon: server.Beacon{
			GenesisTime:           genesisTime,
			SecondsPerSlot:        secondsPerSlot,
			GenesisValidatorsRoot: "0x4b363db94e286120d76eb905340fdd4e54bfe9f06bf33ff6cf5ad27f511bfe95",
			GenesisForkVersion:    "0x00000000",
		},
		AuthToken: testToken,
	})
	if err != nil {
		t.Fatalf("server.New (follower): %v", err)
	}
	httpSrv := httptest.NewServer(handler)
	t.Cleanup(httpSrv.Close)
	fl.url = httpSrv.URL
	return fl
}

// poll runs one follower cycle and fails on error.
func (fl *recoveryFollower) poll() {
	fl.t.Helper()
	if err := fl.f.Poll(fl.t.Context()); err != nil {
		fl.t.Fatalf("follower Poll: %v", err)
	}
}

// pollErr runs one cycle and returns what it says.
func (fl *recoveryFollower) pollErr() error { return fl.f.Poll(fl.t.Context()) }

// syncedToFloor is the follower's persisted synced_to floor for the head.
func (fl *recoveryFollower) syncedToFloor() uint64 {
	fl.t.Helper()
	head, ok := fl.heads.Get(arbitrumHead)
	if !ok {
		fl.t.Fatal("follower has not adopted the head")
	}
	s, _ := head.SyncedTo()
	return s
}

// adoptedRoot is the root the follower currently serves.
func (fl *recoveryFollower) adoptedRoot() string {
	fl.t.Helper()
	head, ok := fl.heads.Get(arbitrumHead)
	if !ok {
		fl.t.Fatal("follower has not adopted the head")
	}
	return head.Root().String()
}

// adoptedTip is the manifest tip the follower has accepted and persisted.
func (fl *recoveryFollower) adoptedTip() (cid.Cid, bool) {
	fl.t.Helper()
	tip, ok, err := fl.manifests.Get(fl.t.Context(), arbitrumHead)
	if err != nil {
		fl.t.Fatalf("ManifestStore.Get: %v", err)
	}
	return tip, ok
}

// reconcile runs a pin pass over the follower's heads.
func (fl *recoveryFollower) reconcile() {
	fl.t.Helper()
	if _, err := fl.rec.ReconcileAll(fl.t.Context()); err != nil {
		fl.t.Fatalf("ReconcileAll: %v", err)
	}
}

// gc runs a mark-and-sweep over the follower's own blockstore.
func (fl *recoveryFollower) gc() {
	fl.t.Helper()
	gc, err := pinning.NewGC(pinning.GCConfig{Blocks: fl.store.Blocks(), Reconciler: fl.rec})
	if err != nil {
		fl.t.Fatalf("pinning.NewGC: %v", err)
	}
	if _, err := gc.Run(fl.t.Context()); err != nil {
		fl.t.Fatalf("GC.Run: %v", err)
	}
}

// blockSet is the multihash-keyed set of blocks the follower holds, with each
// block's size: a blob is exactly BlobSize, an index block never is, which is how
// the GC-able assertion tells a swept segment from a swept blob.
func (fl *recoveryFollower) blockSet() map[string]uint64 {
	fl.t.Helper()
	keys, err := fl.store.Blocks().AllKeysChan(fl.t.Context())
	if err != nil {
		fl.t.Fatalf("AllKeysChan: %v", err)
	}
	out := map[string]uint64{}
	for c := range keys {
		size, err := fl.store.Blocks().GetSize(fl.t.Context(), c)
		if err != nil {
			fl.t.Fatalf("GetSize(%s): %v", c, err)
		}
		out[c.KeyString()] = uint64(size)
	}
	return out
}

// --- indexer and admin-endpoint helpers ---

// newRecoveryIndexer builds a fetch_blobs chain indexer over the given schedule.
// fetch_blobs is on so the drill needs no ALL head: the indexer fetches exactly
// the versioned hashes it sees from the beacon upstream.
func newRecoveryIndexer(t *testing.T, rpc *ethclient.Client, arch *archclient.Client, up *upstream.Client, sources []chainidx.Source) *chainidx.Indexer {
	t.Helper()
	ix, err := chainidx.New(chainidx.Config{
		Chain: rpc, Archive: arch, Upstream: up,
		Head:        arbitrumHead,
		Sources:     sources,
		GenesisTime: genesisTime, SecondsPerSlot: secondsPerSlot,
		FetchBlobs: true,
		BlockRange: 7, MaxPutBlobs: testMaxPutBlobs, PollInterval: time.Millisecond,
	})
	if err != nil {
		t.Fatalf("chain.New: %v", err)
	}
	return ix
}

// runChainIndexer drives an indexer's Step until it stops advancing, after the
// startup schedule check the real binary runs (cmd/bloar-index): CheckSchedule
// verifies the config equals the published tip and records it, so every batch
// binds to that tip.
func runChainIndexer(t *testing.T, ctx context.Context, ix *chainidx.Indexer) {
	t.Helper()
	if err := ix.CheckSchedule(ctx); err != nil {
		t.Fatalf("CheckSchedule before running the chain indexer: %v", err)
	}
	for {
		advanced, err := ix.Step(ctx)
		if err != nil {
			t.Fatalf("running the chain indexer: %v", err)
		}
		if !advanced {
			return
		}
	}
}

// publishManifest advances the head's manifest chain the supported way (spec
// 10.5, the safety boundary): the append-only preflight -- the same PublishManifest an
// operator runs via `bloar-index publish-manifest` -- which validates the
// schedule against the decoded predecessor at the head's L1 position and binds the
// publish to the head root it read. It returns the new tip CID string, or the
// preflight's refusal for the caller to assert on.
func publishManifest(t *testing.T, ctx context.Context, rpc *ethclient.Client, arch *archclient.Client, up *upstream.Client, sources []chainidx.Source) (string, error) {
	t.Helper()
	return newRecoveryIndexer(t, rpc, arch, up, sources).PublishManifest(ctx, sources)
}

// getManifest reads the head's published manifest tip (spec 7.2): its CID and the
// number of sources it lists. A head with no chain fails the test.
func getManifest(t *testing.T, base string) (cidStr string, sourceCount int) {
	t.Helper()
	resp := get(t, base+"/bloar/v1/heads/"+arbitrumHead+"/manifest")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(resp.Body)
		t.Fatalf("GET manifest: status = %d, body = %s", resp.StatusCode, raw)
	}
	var out struct {
		Manifest struct {
			Sources []struct {
				Type string `json:"type"`
			} `json:"sources"`
		} `json:"manifest"`
		CID string `json:"cid"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decoding manifest: %v", err)
	}
	return out.CID, len(out.Manifest.Sources)
}

// truncate calls the real truncate endpoint (spec 5.4, 7.2) and returns the new
// root, failing the test on any non-200.
func truncate(t *testing.T, base string, slot uint64) string {
	t.Helper()
	body, err := json.Marshal(map[string]any{"slot": slot, "confirm": arbitrumHead})
	if err != nil {
		t.Fatalf("encoding truncate body: %v", err)
	}
	resp := authPost(t, base+"/bloar/v1/heads/"+arbitrumHead+"/truncate", body)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(resp.Body)
		t.Fatalf("POST truncate: status = %d, body = %s", resp.StatusCode, raw)
	}
	var out struct {
		Root string `json:"root"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decoding truncate response: %v", err)
	}
	return out.Root
}

// authPost issues an authenticated JSON POST against an admin/ingest endpoint.
func authPost(t *testing.T, url string, body []byte) *http.Response {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("building POST %s: %v", url, err)
	}
	req.Header.Set("Authorization", "Bearer "+testToken)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	return resp
}
