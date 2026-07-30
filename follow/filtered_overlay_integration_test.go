package follow_test

import (
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/ipfs/go-cid"

	"github.com/blobarchive/bloar/archive"
	"github.com/blobarchive/bloar/follow"
	"github.com/blobarchive/bloar/metrics"
	"github.com/blobarchive/bloar/pinning"
	"github.com/blobarchive/bloar/server"
)

const overlayFilteredHead = "arb1-finalized"

// filteredOverlayDocument carries three authenticated facts with deliberately
// different retention roles: a selected filtered finalized line, a selected
// global mutable line, and the mutable proof's global finalized witness. The
// last line licenses the proof but is metadata-only on this follower.
func filteredOverlayDocument(t *testing.T, w *writer, filtered *archive.Head, mutable *archive.Head,
	witness *archive.Head, revision uint64) server.Unsigned {
	t.Helper()
	filteredEntry := entry(filtered.Info())
	witnessEntry := entry(witness.Info())
	heads := []server.HeadEntry{filteredEntry}
	if mutable != nil {
		mutableEntry := entry(mutable.Info())
		if mutableEntry.SyncedTo == nil || witnessEntry.SyncedTo == nil {
			t.Fatal("filtered overlay requires covered mutable and witness heads")
		}
		start := mutableEntry.OriginSlot
		sourceFinalized := *witnessEntry.SyncedTo
		handoffSynced := sourceFinalized
		mutableEntry.Kind = server.UnfinalizedMutable
		mutableEntry.WindowStart = &start
		mutableEntry.SourceHeadRoot = fmt.Sprintf("0x%064x", revision+0x1000)
		mutableEntry.SourceFinalizedSlot = &sourceFinalized
		mutableEntry.SourceFinalizedRoot = fmt.Sprintf("0x%064x", revision+0x2000)
		mutableEntry.HandoffHead = witnessEntry.Name
		mutableEntry.HandoffRoot = witnessEntry.Root
		mutableEntry.HandoffSyncedTo = &handoffSynced
		heads = append(heads, mutableEntry)
	}
	heads = append(heads, witnessEntry)
	return server.Unsigned{
		V: server.DocVersion, Net: testNet,
		UpdatedAt:  time.Unix(int64(revision), 0).UTC().Format(time.RFC3339),
		Multiaddrs: w.host.AnnounceAddrs(), Heads: heads, Revision: &revision,
	}
}

func buildOverlayHeadWithRows(t *testing.T, w *writer, name string, origin, end uint64, rows []archive.RefRow) *archive.Head {
	t.Helper()
	head, err := archive.BuildGeneration(t.Context(), archive.Config{
		Blocks: w.store.Blocks(), Resolver: w.cat, Cache: w.cache,
	}, archive.Params{
		Name: name, Net: testNet, OriginSlot: origin, SegBits: testSegBits, FanoutBits: testFanout,
	}, rows, end)
	if err != nil {
		t.Fatalf("BuildGeneration(%s, [%d,%d]): %v", name, origin, end, err)
	}
	return head
}

func configureFilteredOverlayFollower(c *follow.Config, docs *docServer, mx *metrics.Metrics) {
	c.URL = docs.url
	c.Heads = map[string]pinning.Policy{
		overlayFilteredHead: pinning.Full(),
		testHead:            pinning.Full(),
	}
	c.ExpectedKinds = map[string]server.HeadKind{testHead: server.UnfinalizedMutable}
	c.ExpectedHandoffs = map[string]string{testHead: testHandoffHead}
	c.OverlayFinalizedHeads = map[string]string{testHead: overlayFilteredHead}
	c.MaxMutableWindowSlots = map[string]uint64{testHead: 32}
	c.Metrics = mx
}

func requireNoOverlayWitnessState(t *testing.T, active *follow.Follower, f *follower, witness *archive.Head) {
	t.Helper()
	if adopted := follow.HeadAdopted(active, witness.Params().Name); adopted.Defined() {
		t.Fatalf("active follower tracks metadata-only witness %q at %s", witness.Params().Name, adopted)
	}
	requireNotSelected(t, f.heads, witness.Params().Name)
	if slices.Contains(f.heads.Names(), witness.Params().Name) {
		t.Fatalf("metadata-only witness %q appears in served names %v", witness.Params().Name, f.heads.Names())
	}
	if _, ok := f.heads.HeadDoc(witness.Params().Name); ok {
		t.Fatalf("metadata-only witness %q was republished", witness.Params().Name)
	}
	if root, ok, err := f.roots.Get(t.Context(), witness.Params().Name); err != nil || ok {
		t.Fatalf("metadata-only witness root mirror = %s ok=%t err=%v", root, ok, err)
	}
	if tip, ok, err := f.manifests.Get(t.Context(), witness.Params().Name); err != nil || ok {
		t.Fatalf("metadata-only witness manifest mirror = %s ok=%t err=%v", tip, ok, err)
	}
	if _, _, _, _, _, ok, err := follow.ReadRevisionedCheckpoint(f.store.KV(), witness.Params().Name); err != nil || ok {
		t.Fatalf("metadata-only witness checkpoint: ok=%t err=%v", ok, err)
	}
	if rows, err := ledgerOf(f.node).ListAll(t.Context(), witness.Params().Name); err != nil || len(rows) != 0 {
		t.Fatalf("metadata-only witness pin rows = %v err=%v", rows, err)
	}
	if f.hasLocally(witness.Root()) {
		t.Fatalf("metadata-only witness root %s was fetched", witness.Root())
	}
}

func overlayCheckpointWitness(t *testing.T, f *follower, name string) server.HeadEntry {
	t.Helper()
	raw := checkpointBytes(t, f, name)
	if len(raw) < 92 || raw[0] != 3 {
		t.Fatalf("checkpoint %q is not v3: %x", name, raw)
	}
	networkLen := int(binary.BigEndian.Uint16(raw[2:4]))
	publishedLen := int(binary.BigEndian.Uint32(raw[84:88]))
	witnessLen := int(binary.BigEndian.Uint32(raw[88:92]))
	witnessAt := 92 + networkLen + publishedLen
	if witnessLen == 0 || witnessAt+witnessLen != len(raw) {
		t.Fatalf("checkpoint %q witness bounds: offset=%d length=%d total=%d", name, witnessAt, witnessLen, len(raw))
	}
	var witness server.HeadEntry
	if err := json.Unmarshal(raw[witnessAt:], &witness); err != nil {
		t.Fatalf("decoding checkpoint %q witness: %v", name, err)
	}
	return witness
}

func requireFilteredOverlaySelection(t *testing.T, f *follower, filtered, mutable *archive.Head, revision uint64) {
	t.Helper()
	requireSelectedRoot(t, f.heads, overlayFilteredHead, filtered.Root())
	requireSelectedRoot(t, f.heads, testHead, mutable.Root())
	requireMirror(t, f.roots, overlayFilteredHead, filtered.Root())
	requireMirror(t, f.roots, testHead, mutable.Root())
	requireCheckpointRevision(t, f, overlayFilteredHead, revision)
	requireCheckpointRevision(t, f, testHead, revision)
}

func TestFilteredOverlayFollowerKeepsGlobalHandoffMetadataOnly(t *testing.T) {
	w := newWriter(t)
	docs := newDocServer(t)
	mx := metrics.New()

	filtered := buildDocumentHead(t, w, overlayFilteredHead, 96, 103, testSegBits, testFanout)
	mutable := buildDocumentHead(t, w, testHead, 104, 111, testSegBits, testFanout)
	witness := buildDocumentHead(t, w, testHandoffHead, 96, 103, testSegBits, testFanout)
	f := newFollower(t, w, func(c *follow.Config) { configureFilteredOverlayFollower(c, docs, mx) })

	docs.set(sign(t, w.key, filteredOverlayDocument(t, w, filtered, mutable, witness, 1)))
	f.poll()
	f.reconcile()
	requireFilteredOverlaySelection(t, f, filtered, mutable, 1)
	requireNoOverlayWitnessState(t, f.f, f, witness)
	published := checkpointPublishedEntry(t, f, testHead)
	if published.HandoffHead != testHandoffHead || published.HandoffRoot != witness.Root().String() ||
		published.HandoffSyncedTo == nil || *published.HandoffSyncedTo != 103 {
		t.Fatalf("mutable checkpoint lost its metadata-only global witness: %#v", published)
	}
	checkpointWitness := overlayCheckpointWitness(t, f, testHead)
	if checkpointWitness.Name != testHandoffHead || checkpointWitness.Root != witness.Root().String() ||
		checkpointWitness.SyncedTo == nil || *checkpointWitness.SyncedTo != 103 {
		t.Fatalf("mutable checkpoint retained the wrong nested handoff witness: %#v", checkpointWitness)
	}

	// Revision 2 is globally coherent: its mutable proof meets the global
	// witness at 111. It is locally unsafe because the selected filtered frontier
	// reaches only 104 while the mutable window begins at 112. Refusal must happen
	// before any of the three candidate DAGs is fetched or any member commits.
	filteredGap := buildDocumentHead(t, w, overlayFilteredHead, 96, 104, testSegBits, testFanout)
	mutableGap := buildDocumentHead(t, w, testHead, 112, 119, testSegBits, testFanout)
	witnessGap := buildDocumentHead(t, w, testHandoffHead, 96, 111, testSegBits, testFanout)
	docs.set(sign(t, w.key, filteredOverlayDocument(t, w, filteredGap, mutableGap, witnessGap, 2)))
	err := f.pollErr()
	if err == nil || !strings.Contains(err.Error(), "window_start 112 is beyond finalized synced_to 104 plus one") {
		t.Fatalf("filtered handoff-gap Poll error = %v", err)
	}
	if got := refusalCount(t, mx, metrics.RefusalHandoffBlocked); got != 1 {
		t.Fatalf("handoff_blocked refusals = %g, want 1", got)
	}
	requireFilteredOverlaySelection(t, f, filtered, mutable, 1)
	requireNoOverlayWitnessState(t, f.f, f, witness)
	for _, candidate := range []cid.Cid{filteredGap.Root(), mutableGap.Root(), witnessGap.Root()} {
		if f.hasLocally(candidate) {
			t.Fatalf("refused gap document fetched candidate root %s", candidate)
		}
	}
	if revision, _, ok, floorErr := follow.ReadAuthorityFloor(f.store.KV(), w.pubkey()); floorErr != nil || !ok || revision != 1 {
		t.Fatalf("gap refusal authority floor = %d ok=%t err=%v, want revision 1", revision, ok, floorErr)
	}

	// A fresh process reconstructs both selected physical heads and the hidden
	// global witness carried inside the mutable v3 checkpoint. The witness must
	// license mutable service again without becoming a third retained head.
	priorFollower, priorRegistry := f.f, f.heads
	next := f.restart(t, w, func(c *follow.Config) { configureFilteredOverlayFollower(c, docs, mx) })
	if next == priorFollower || f.heads == priorRegistry {
		t.Fatal("restart fixture did not replace the follower and registry")
	}
	if err := next.Resume(t.Context()); err != nil {
		t.Fatalf("Resume(filtered overlay): %v", err)
	}
	if adopted := follow.HeadAdopted(next, testHead); !adopted.Equals(mutable.Root()) {
		t.Fatalf("restarted follower mutable root = %s, want %s", adopted, mutable.Root())
	}
	requireFilteredOverlaySelection(t, f, filtered, mutable, 1)
	requireNoOverlayWitnessState(t, next, f, witness)

	// Revision 3 keeps the filtered finalized line but omits mutable. The global
	// witness remains an authenticated extra document line, never a selection.
	// Omission must withdraw mutable, drain its old pins, and leave the filtered
	// archive serving independently.
	docs.set(sign(t, w.key, filteredOverlayDocument(t, w, filtered, nil, witness, 3)))
	if err := next.Poll(t.Context()); err != nil {
		t.Fatalf("restarted follower Poll(mutable omission): %v", err)
	}
	requireSelectedRoot(t, f.heads, overlayFilteredHead, filtered.Root())
	requireNotSelected(t, f.heads, testHead)
	requireMirror(t, f.roots, overlayFilteredHead, filtered.Root())
	if root, ok, rootErr := f.roots.Get(t.Context(), testHead); rootErr != nil || ok {
		t.Fatalf("withdrawn mutable root mirror = %s ok=%t err=%v", root, ok, rootErr)
	}
	requireCheckpointRevision(t, f, overlayFilteredHead, 3)
	requireCheckpointRevision(t, f, testHead, 3)
	f.reconcile()
	if rows, err := ledgerOf(f.node).ListAll(t.Context(), testHead); err != nil || len(rows) != 0 {
		t.Fatalf("withdrawn mutable pin rows = %v err=%v", rows, err)
	}
	requireNoOverlayWitnessState(t, next, f, witness)
	if got := refusalCount(t, mx, metrics.RefusalHandoffBlocked); got != 1 {
		t.Fatalf("handoff_blocked refusals after valid restart/withdrawal = %g, want 1", got)
	}
}

func TestFilteredOverlayGCProtectsSharedBlobAndSweepsRetiredMutableOnlyBlob(t *testing.T) {
	w := newWriter(t)
	docs := newDocServer(t)

	sharedBlobs, sharedVHs := w.ingestSlot(testOrigin, 7001)
	foreignBlobs, foreignVHs := w.ingestSlot(testOrigin+1, 7002)
	sharedCID := blobCID(t, sharedBlobs[0])
	foreignCID := blobCID(t, foreignBlobs[0])

	filteredA := buildOverlayHeadWithRows(t, w, overlayFilteredHead, 96, 103,
		[]archive.RefRow{{Slot: 103, VHs: sharedVHs}})
	mutableA := buildOverlayHeadWithRows(t, w, testHead, 104, 111, []archive.RefRow{
		{Slot: 104, VHs: sharedVHs},
		{Slot: 105, VHs: foreignVHs},
	})
	witnessA := buildOverlayHeadWithRows(t, w, testHandoffHead, 96, 103, nil)
	f := newFollower(t, w, func(c *follow.Config) { configureFilteredOverlayFollower(c, docs, nil) })
	docs.set(sign(t, w.key, filteredOverlayDocument(t, w, filteredA, mutableA, witnessA, 1)))
	f.poll()
	f.reconcile()
	for name, c := range map[string]cid.Cid{"shared": sharedCID, "mutable-only": foreignCID} {
		if !f.hasLocally(c) {
			t.Fatalf("initial follower did not fetch %s blob %s", name, c)
		}
	}

	// B retires mutable A entirely. The filtered finalized head advances while
	// retaining the shared blob; mutable B has no reference to either A blob.
	// Its global witness advances in metadata only.
	filteredB := buildOverlayHeadWithRows(t, w, overlayFilteredHead, 96, 111,
		[]archive.RefRow{{Slot: 103, VHs: sharedVHs}})
	mutableB := buildOverlayHeadWithRows(t, w, testHead, 112, 119, nil)
	witnessB := buildOverlayHeadWithRows(t, w, testHandoffHead, 96, 111, nil)
	docs.set(sign(t, w.key, filteredOverlayDocument(t, w, filteredB, mutableB, witnessB, 2)))
	gc, err := pinning.NewGC(pinning.GCConfig{
		Blocks: f.store.Blocks(), Reconciler: f.rec, Staging: f.staging, Fetch: f.f.GCFetch(),
	})
	if err != nil {
		t.Fatalf("constructing production-shaped follower GC: %v", err)
	}

	// Force a real embedded collection cut after B's closure preflight but before
	// its atomic publication. Because the cut starts after the idle-generation
	// observation, it may sweep an unexposed B root; either the generation-token
	// check or the commit-time root retouch must refuse that attempt as a whole.
	// A remains selected and its mutable-only blob remains protected. Restarting
	// with an empty decode cache models the ordinary recovery from that refused
	// attempt; Resume restores A, then a stable poll refetches B and rotates.
	var (
		racedGCStats pinning.GCStats
		racedGCErr   error
	)
	follow.SetBetweenPhasesHook(func() { racedGCStats, racedGCErr = gc.Run(t.Context()) })
	t.Cleanup(func() { follow.SetBetweenPhasesHook(nil) })
	err = f.pollErr()
	follow.SetBetweenPhasesHook(nil)
	if err == nil || !strings.Contains(err.Error(), "collection generation") && !strings.Contains(err.Error(), "before publication") {
		t.Fatalf("adoption crossing embedded GC = %v, want generation/retouch refusal", err)
	}
	if racedGCErr != nil {
		t.Fatalf("concurrent embedded GC: %v", racedGCErr)
	}
	if racedGCStats.Scanned == 0 {
		t.Fatal("concurrent embedded GC did not run a real sweep")
	}
	requireFilteredOverlaySelection(t, f, filteredA, mutableA, 1)
	if !f.hasLocally(foreignCID) {
		t.Fatalf("GC crossing refused adoption swept still-selected mutable A blob %s", foreignCID)
	}

	next := f.restart(t, w, func(c *follow.Config) {
		configureFilteredOverlayFollower(c, docs, nil)
		c.Cache = nil
		c.Staging = f.staging
	})
	if err := next.Resume(t.Context()); err != nil {
		t.Fatalf("resume after GC-raced refusal: %v", err)
	}
	requireFilteredOverlaySelection(t, f, filteredA, mutableA, 1)
	if err := next.Poll(t.Context()); err != nil {
		t.Fatalf("stable post-restart mutable rotation: %v", err)
	}
	requireFilteredOverlaySelection(t, f, filteredB, mutableB, 2)
	f.reconcile()
	if !f.hasLocally(foreignCID) {
		t.Fatalf("reconciliation deleted retired mutable-only blob %s before GC", foreignCID)
	}

	postRotationGC, err := pinning.NewGC(pinning.GCConfig{
		Blocks: f.store.Blocks(), Reconciler: f.rec, Staging: f.staging, Fetch: next.GCFetch(),
	})
	if err != nil {
		t.Fatalf("constructing post-rotation GC: %v", err)
	}
	stats, err := postRotationGC.Run(t.Context())
	if err != nil {
		t.Fatalf("post-rotation GC: %v", err)
	}
	if stats.Swept == 0 {
		t.Fatal("post-rotation GC swept no retired blocks")
	}
	if f.hasLocally(foreignCID) {
		t.Fatalf("next successful GC retained mutable-A-only blob %s", foreignCID)
	}
	if !f.hasLocally(sharedCID) {
		t.Fatalf("next successful GC swept shared filtered-finalized blob %s", sharedCID)
	}

	// Exercise the public read path after collection, not merely blockstore Has:
	// the shared blob remains recursively reachable from filtered B and is served
	// byte-for-byte even though the mutable generation which also referenced it
	// has retired.
	f.serveHTTP(nil)
	url := fmt.Sprintf("%s/%s/eth/v1/beacon/blobs/103?versioned_hashes=0x%x",
		f.url, overlayFilteredHead, sharedVHs[0][:])
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET shared filtered blob after GC: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reading shared filtered blob after GC: %v", err)
	}
	var payload struct {
		Data []string `json:"data"`
	}
	if err := json.Unmarshal(body, &payload); err != nil || resp.StatusCode != http.StatusOK || len(payload.Data) != 1 {
		t.Fatalf("shared filtered blob after GC: status=%d blobs=%d err=%v body=%q",
			resp.StatusCode, len(payload.Data), err, body)
	}
	wantBlob := "0x" + hex.EncodeToString(sharedBlobs[0])
	if payload.Data[0] != wantBlob {
		t.Fatalf("shared filtered blob body differs after GC: got %d hex chars, want %d",
			len(payload.Data[0]), len(wantBlob))
	}
}
