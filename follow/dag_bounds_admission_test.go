package follow_test

import (
	"crypto/ed25519"
	"reflect"
	"strings"
	"testing"
	"time"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"

	"github.com/blobarchive/bloar/catalog"
	"github.com/blobarchive/bloar/follow"
	"github.com/blobarchive/bloar/schema"
	"github.com/blobarchive/bloar/server"
)

func putFollowAdmissionBlock(t *testing.T, w *writer, data []byte, id cid.Cid) cid.Cid {
	t.Helper()
	blk, err := blocks.NewBlockWithCid(data, id)
	if err != nil {
		t.Fatalf("blocks.NewBlockWithCid(%s): %v", id, err)
	}
	if err := w.store.Blocks().Put(t.Context(), blk); err != nil {
		t.Fatalf("writer Blockstore.Put(%s): %v", id, err)
	}
	return id
}

func putFollowAdmissionSegment(t *testing.T, w *writer, slot0 uint64, nonempty bool) cid.Cid {
	return putFollowAdmissionSegmentSeed(t, w, slot0, nonempty, 1)
}

func putFollowAdmissionSegmentSeed(t *testing.T, w *writer, slot0 uint64, nonempty bool, seed byte) cid.Cid {
	t.Helper()
	segment := &schema.Segment{Slot0: slot0}
	if nonempty {
		var vh schema.VersionedHash
		vh[0] = seed
		segment.Rows = []schema.Row{{
			Slot: slot0,
			Entries: []schema.RefEntry{{
				VH:   vh,
				Blob: blobCID(t, makeBlob(slot0+uint64(seed))),
			}},
		}}
	}
	data, id, err := schema.EncodeSegment(segment)
	if err != nil {
		t.Fatalf("schema.EncodeSegment(slot0=%d): %v", slot0, err)
	}
	return putFollowAdmissionBlock(t, w, data, id)
}

func putFollowAdmissionDir(t *testing.T, w *writer, kids ...cid.Cid) cid.Cid {
	t.Helper()
	data, id, err := schema.EncodeDirNode(&schema.DirNode{Kids: kids})
	if err != nil {
		t.Fatalf("schema.EncodeDirNode: %v", err)
	}
	return putFollowAdmissionBlock(t, w, data, id)
}

func repeatedSubtreeHead(t *testing.T, w *writer) (cid.Cid, uint64) {
	return repeatedSubtreeHeadSeed(t, w, 1)
}

func repeatedSubtreeHeadSeed(t *testing.T, w *writer, seed byte) (cid.Cid, uint64) {
	t.Helper()
	const sealedWindows uint64 = 8
	base := uint64(testOrigin >> testSegBits)
	first := putFollowAdmissionSegmentSeed(t, w, base<<testSegBits, true, seed)
	leaf := putFollowAdmissionDir(t, w, first)
	dir := putFollowAdmissionDir(t, w, leaf, leaf)
	openOrd := base + sealedWindows
	open := putFollowAdmissionSegment(t, w, openOrd<<testSegBits, false)
	syncedTo := (openOrd << testSegBits) - 1
	obj := &schema.Head{
		Name:       testHead,
		Net:        testNet,
		OriginSlot: testOrigin,
		SyncedTo:   &syncedTo,
		SegBits:    testSegBits,
		FanoutBits: testFanout,
		DirDepth:   2,
		Dir:        dir,
		Open:       open,
	}
	data, root, err := schema.EncodeHead(obj)
	if err != nil {
		t.Fatalf("schema.EncodeHead: %v", err)
	}
	return putFollowAdmissionBlock(t, w, data, root), syncedTo
}

func followAdmissionEntry(root cid.Cid, syncedTo uint64) server.HeadEntry {
	return server.HeadEntry{
		Name:       testHead,
		Root:       root.String(),
		OriginSlot: testOrigin,
		SyncedTo:   &syncedTo,
		SegBits:    testSegBits,
		FanoutBits: testFanout,
		DirDepth:   2,
	}
}

func canonicalFollowAdmissionHead(
	t *testing.T,
	w *writer,
	sealed []cid.Cid,
	open cid.Cid,
	syncedTo uint64,
) cid.Cid {
	t.Helper()
	dir := putFollowAdmissionDir(t, w, sealed...)
	obj := &schema.Head{
		Name:       testHead,
		Net:        testNet,
		OriginSlot: testOrigin,
		SyncedTo:   &syncedTo,
		SegBits:    testSegBits,
		FanoutBits: testFanout,
		DirDepth:   1,
		Dir:        dir,
		Open:       open,
	}
	data, root, err := schema.EncodeHead(obj)
	if err != nil {
		t.Fatalf("schema.EncodeHead: %v", err)
	}
	return putFollowAdmissionBlock(t, w, data, root)
}

func TestSignedDirectoryAmplificationRejectedBeforeFollowerStateMutation(t *testing.T) {
	w := newWriter(t)
	root, syncedTo := repeatedSubtreeHead(t, w)
	docs := newDocServer(t)
	unsigned := w.unsigned(time.Now())
	unsigned.Heads = []server.HeadEntry{{
		Name:       testHead,
		Root:       root.String(),
		OriginSlot: testOrigin,
		SyncedTo:   &syncedTo,
		SegBits:    testSegBits,
		FanoutBits: testFanout,
		DirDepth:   2,
	}}
	docs.set(sign(t, w.key, unsigned))

	retention := &recordingRetention{}
	f := newFollower(t, w,
		func(cfg *follow.Config) { cfg.URL = docs.url },
		externalRetentionOption(t, retention),
	)
	retention.roots = f.roots

	err := f.pollErr()
	if err == nil || !strings.Contains(err.Error(), "shared at multiple positions") {
		t.Fatalf("Poll(repeated signed subtree) error = %v, want bounded structure refusal", err)
	}
	if retention.pending != nil || retention.current != nil || len(retention.events) != 0 {
		t.Fatalf("refused DAG touched external retention: pending=%#v current=%#v events=%v",
			retention.pending, retention.current, retention.events)
	}
	if _, _, _, _, ok, err := follow.ReadCheckpoint(f.store.KV(), testHead); err != nil || ok {
		t.Fatalf("checkpoint after refused DAG: ok=%t err=%v, want absent", ok, err)
	}
	if _, ok, err := f.roots.Get(t.Context(), testHead); err != nil || ok {
		t.Fatalf("root mirror after refused DAG: ok=%t err=%v, want absent", ok, err)
	}
	if _, ok := f.heads.Get(testHead); ok {
		t.Fatal("refused DAG became a served registry head")
	}
	if _, ok, err := follow.ReadUpdatedAt(f.store.KV()); err != nil || ok {
		t.Fatalf("document freshness floor after refused DAG: ok=%t err=%v, want absent", ok, err)
	}
	pins, err := catalog.NewLedger(f.store.KV()).ListAll(t.Context(), testHead)
	if err != nil {
		t.Fatalf("Ledger.ListAll: %v", err)
	}
	if len(pins) != 0 {
		t.Fatalf("pin ledger after refused DAG = %v, want empty", pins)
	}
}

func TestSourceSetMalformedRepeatedSubtreeCannotConflictWithHealthyPeer(t *testing.T) {
	w := newWriter(t)
	malformedRoot, syncedTo := repeatedSubtreeHeadSeed(t, w, 11)
	healthy := buildDocumentHead(t, w, testHead, testOrigin, syncedTo, testSegBits, testFanout)
	if healthy.Root() == malformedRoot {
		t.Fatal("healthy and malformed source fixtures unexpectedly share a root")
	}

	archiveID := sourceRuntimeArchiveID(t)
	badKey, goodKey := sourceRuntimeKey(t), sourceRuntimeKey(t)
	badDocs, goodDocs := newDocServer(t), newDocServer(t)
	badDocs.set(sourceRuntimeDocument(t, w, badKey, archiveID, 1, time.Unix(1, 0),
		followAdmissionEntry(malformedRoot, syncedTo)))
	goodDocs.set(sourceRuntimeDocument(t, w, goodKey, archiveID, 1, time.Unix(1, 0),
		entry(healthy.Info())))
	sources := []follow.SourceConfig{
		{ID: "writer-bad", URL: badDocs.url, PubKey: badKey.Public().(ed25519.PublicKey), AllowedHeads: []string{testHead}},
		{ID: "writer-good", URL: goodDocs.url, PubKey: goodKey.Public().(ed25519.PublicKey), AllowedHeads: []string{testHead}},
	}
	f := newFollower(t, w, func(cfg *follow.Config) {
		configureSourceRuntime(t, cfg, archiveID, sources)
	})

	if err := f.pollErr(); err != nil {
		t.Fatalf("healthy source beside malformed repeated subtree: %v", err)
	}
	if got := follow.HeadAdopted(f.f, testHead); got != healthy.Root() {
		t.Fatalf("adopted root = %s, want healthy root %s", got, healthy.Root())
	}
	cp := readSourceRuntimeV4(t, f.store.KV(), testHead)
	if !cp.selected || cp.sourceID != "writer-good" {
		t.Fatalf("checkpoint = %+v, want selected writer-good", cp)
	}
	if _, ok, err := follow.LoadConflictLatch(f.store.KV(), archiveID, testHead); err != nil || ok {
		t.Fatalf("malformed peer created conflict latch: ok=%t err=%v", ok, err)
	}
	if _, ok, err := follow.ReadSourcePublicationFloor(f.store.KV(), archiveID, "writer-bad"); err != nil || ok {
		t.Fatalf("malformed source advanced its replay floor: ok=%t err=%v", ok, err)
	}
}

func TestSourceSetTwoMalformedRootsCreateNoConflictOrSelectionMutation(t *testing.T) {
	w := newWriter(t)
	leftRoot, syncedTo := repeatedSubtreeHeadSeed(t, w, 21)
	rightRoot, rightSyncedTo := repeatedSubtreeHeadSeed(t, w, 22)
	if rightSyncedTo != syncedTo || rightRoot == leftRoot {
		t.Fatalf("malformed fixtures = (%s,%d) and (%s,%d), want differing roots at equal coverage",
			leftRoot, syncedTo, rightRoot, rightSyncedTo)
	}

	archiveID := sourceRuntimeArchiveID(t)
	leftKey, rightKey := sourceRuntimeKey(t), sourceRuntimeKey(t)
	leftDocs, rightDocs := newDocServer(t), newDocServer(t)
	leftDocs.set(sourceRuntimeDocument(t, w, leftKey, archiveID, 1, time.Unix(1, 0),
		followAdmissionEntry(leftRoot, syncedTo)))
	rightDocs.set(sourceRuntimeDocument(t, w, rightKey, archiveID, 1, time.Unix(1, 0),
		followAdmissionEntry(rightRoot, syncedTo)))
	sources := []follow.SourceConfig{
		{ID: "writer-left", URL: leftDocs.url, PubKey: leftKey.Public().(ed25519.PublicKey), AllowedHeads: []string{testHead}},
		{ID: "writer-right", URL: rightDocs.url, PubKey: rightKey.Public().(ed25519.PublicKey), AllowedHeads: []string{testHead}},
	}
	f := newFollower(t, w, func(cfg *follow.Config) {
		configureSourceRuntime(t, cfg, archiveID, sources)
	})

	err := f.pollErr()
	if err == nil || !strings.Contains(err.Error(), "shared at multiple positions") {
		t.Fatalf("Poll(two malformed source roots) error = %v, want bounded structure refusal", err)
	}
	if _, ok, err := follow.LoadConflictLatch(f.store.KV(), archiveID, testHead); err != nil || ok {
		t.Fatalf("malformed roots created conflict latch: ok=%t err=%v", ok, err)
	}
	if _, _, _, _, ok, err := follow.ReadCheckpoint(f.store.KV(), testHead); err != nil || ok {
		t.Fatalf("checkpoint after malformed arbitration: ok=%t err=%v", ok, err)
	}
	if _, ok, err := f.roots.Get(t.Context(), testHead); err != nil || ok {
		t.Fatalf("root mirror after malformed arbitration: ok=%t err=%v", ok, err)
	}
	if _, ok := f.heads.Get(testHead); ok {
		t.Fatal("malformed arbitration exposed a serving head")
	}
	pins, err := catalog.NewLedger(f.store.KV()).ListAll(t.Context(), testHead)
	if err != nil {
		t.Fatalf("Ledger.ListAll: %v", err)
	}
	if len(pins) != 0 {
		t.Fatalf("malformed arbitration changed pin ledger: %v", pins)
	}
	for _, sourceID := range []string{"writer-left", "writer-right"} {
		if _, ok, err := follow.ReadSourcePublicationFloor(f.store.KV(), archiveID, sourceID); err != nil || ok {
			t.Fatalf("malformed source %q advanced replay floor: ok=%t err=%v", sourceID, ok, err)
		}
	}
}

func TestSourceSetMalformedFreshClaimCannotConflictWithDurableCheckpoint(t *testing.T) {
	w := newWriter(t)
	const syncedTo = uint64(159)
	healthy := buildDocumentHead(t, w, testHead, testOrigin, syncedTo, testSegBits, testFanout)
	archiveID := sourceRuntimeArchiveID(t)
	key := sourceRuntimeKey(t)
	docs := newDocServer(t)
	docs.set(sourceRuntimeDocument(t, w, key, archiveID, 1, time.Unix(1, 0), entry(healthy.Info())))
	sources := []follow.SourceConfig{{
		ID: "writer-a", URL: docs.url, PubKey: key.Public().(ed25519.PublicKey), AllowedHeads: []string{testHead},
	}}
	f := newFollower(t, w, func(cfg *follow.Config) {
		configureSourceRuntime(t, cfg, archiveID, sources)
	})
	f.poll()

	beforePins, err := catalog.NewLedger(f.store.KV()).ListAll(t.Context(), testHead)
	if err != nil {
		t.Fatalf("Ledger.ListAll(before): %v", err)
	}
	malformedRoot, malformedSyncedTo := repeatedSubtreeHeadSeed(t, w, 31)
	if malformedSyncedTo != syncedTo || malformedRoot == healthy.Root() {
		t.Fatalf("malformed durable fixture = (%s,%d), healthy = (%s,%d)",
			malformedRoot, malformedSyncedTo, healthy.Root(), syncedTo)
	}
	docs.set(sourceRuntimeDocument(t, w, key, archiveID, 2, time.Unix(2, 0),
		followAdmissionEntry(malformedRoot, malformedSyncedTo)))

	err = f.pollErr()
	if err == nil || !strings.Contains(err.Error(), "shared at multiple positions") {
		t.Fatalf("Poll(malformed fresh versus durable) error = %v, want bounded structure refusal", err)
	}
	if _, ok, err := follow.LoadConflictLatch(f.store.KV(), archiveID, testHead); err != nil || ok {
		t.Fatalf("malformed fresh claim created durable conflict: ok=%t err=%v", ok, err)
	}
	root, _, _, _, ok, err := follow.ReadCheckpoint(f.store.KV(), testHead)
	if err != nil || !ok || root != healthy.Root() {
		t.Fatalf("checkpoint after malformed fresh claim = %s ok=%t err=%v, want %s",
			root, ok, err, healthy.Root())
	}
	if got := follow.HeadAdopted(f.f, testHead); got != healthy.Root() {
		t.Fatalf("served root after malformed fresh claim = %s, want %s", got, healthy.Root())
	}
	if mirrored, ok, err := f.roots.Get(t.Context(), testHead); err != nil || !ok || mirrored != healthy.Root() {
		t.Fatalf("root mirror after malformed fresh claim = %s ok=%t err=%v, want %s",
			mirrored, ok, err, healthy.Root())
	}
	afterPins, err := catalog.NewLedger(f.store.KV()).ListAll(t.Context(), testHead)
	if err != nil {
		t.Fatalf("Ledger.ListAll(after): %v", err)
	}
	if !reflect.DeepEqual(afterPins, beforePins) {
		t.Fatalf("malformed fresh claim changed pin ledger:\nbefore=%v\nafter=%v", beforePins, afterPins)
	}
	if revision, ok, err := follow.ReadSourcePublicationFloor(f.store.KV(), archiveID, "writer-a"); err != nil || !ok || revision != 1 {
		t.Fatalf("source floor after malformed revision 2 = %d ok=%t err=%v, want retained revision 1",
			revision, ok, err)
	}
}

func TestFollowerStructureCacheCannotAdmitDeletedSegmentOnLaterReuse(t *testing.T) {
	w := newWriter(t)
	segmentA := putFollowAdmissionSegmentSeed(t, w, testOrigin, true, 41)
	segmentB := putFollowAdmissionSegmentSeed(t, w, testOrigin, true, 42)
	segment13 := putFollowAdmissionSegmentSeed(t, w, testOrigin+8, true, 43)
	segment14 := putFollowAdmissionSegmentSeed(t, w, testOrigin+16, true, 44)
	open13 := putFollowAdmissionSegment(t, w, testOrigin+8, false)
	open14 := putFollowAdmissionSegment(t, w, testOrigin+16, false)
	open15 := putFollowAdmissionSegment(t, w, testOrigin+24, false)
	rootA := canonicalFollowAdmissionHead(t, w, []cid.Cid{segmentA}, open13, testOrigin+7)
	rootB := canonicalFollowAdmissionHead(t, w, []cid.Cid{segmentB, segment13}, open14, testOrigin+15)
	rootC := canonicalFollowAdmissionHead(t, w, []cid.Cid{segmentA, segment13, segment14}, open15, testOrigin+23)

	docs := newDocServer(t)
	publish := func(at time.Time, root cid.Cid, syncedTo uint64) {
		unsigned := w.unsigned(at)
		unsigned.Heads = []server.HeadEntry{{
			Name:       testHead,
			Root:       root.String(),
			OriginSlot: testOrigin,
			SyncedTo:   &syncedTo,
			SegBits:    testSegBits,
			FanoutBits: testFanout,
			DirDepth:   1,
		}}
		docs.set(sign(t, w.key, unsigned))
	}
	retention := &recordingRetention{}
	f := newFollower(t, w,
		func(cfg *follow.Config) { cfg.URL = docs.url },
		externalRetentionOption(t, retention),
	)
	retention.roots = f.roots

	publish(time.Unix(1, 0), rootA, testOrigin+7)
	f.poll()
	publish(time.Unix(2, 0), rootB, testOrigin+15)
	f.poll()
	if retention.current == nil || len(retention.current.Heads) != 1 || retention.current.Heads[0].Root != rootB {
		t.Fatalf("retained generation before deletion = %#v, want root B %s", retention.current, rootB)
	}

	segmentABlock, err := w.store.Blocks().Get(t.Context(), segmentA)
	if err != nil {
		t.Fatalf("reading Segment A before deletion: %v", err)
	}
	// Delete through a real collection epoch. Begin advances the monotonic
	// generation before a collector can remove anything, which invalidates the
	// cache's generation-zero presence proof for Segment A. This is the
	// production A -> B -> GC -> C-reuses-A failure mode.
	epoch, err := f.store.Epochs().Begin()
	if err != nil {
		t.Fatalf("beginning follower collection epoch: %v", err)
	}
	deleted, protected, deleteErr := epoch.DeleteCandidate(t.Context(), segmentA)
	epoch.End()
	if deleteErr != nil {
		t.Fatalf("deleting cached Segment A in collection epoch: %v", deleteErr)
	}
	if !deleted || protected {
		t.Fatalf("collection deletion of cached Segment A: deleted=%t protected=%t, want deleted and unprotected",
			deleted, protected)
	}
	if err := w.store.Blocks().DeleteBlock(t.Context(), segmentA); err != nil {
		t.Fatalf("deleting cached Segment A from writer: %v", err)
	}

	beforePins, err := catalog.NewLedger(f.store.KV()).ListAll(t.Context(), testHead)
	if err != nil {
		t.Fatalf("Ledger.ListAll(before rejection): %v", err)
	}
	beforeFloor, ok, err := follow.ReadUpdatedAt(f.store.KV())
	if err != nil || !ok {
		t.Fatalf("freshness floor before rejection: ok=%t err=%v", ok, err)
	}
	retention.events = nil
	retention.pending = nil
	publish(time.Unix(3, 0), rootC, testOrigin+23)
	err = f.pollErr()
	if err == nil || !strings.Contains(err.Error(), segmentA.String()) {
		t.Fatalf("Poll(reused deleted Segment) error = %v, want missing Segment %s", err, segmentA)
	}
	if retention.pending != nil || len(retention.events) != 0 ||
		retention.current == nil || retention.current.Heads[0].Root != rootB {
		t.Fatalf("rejected cached Segment changed retention: pending=%#v current=%#v events=%v",
			retention.pending, retention.current, retention.events)
	}
	if root, _, _, _, ok, err := follow.ReadCheckpoint(f.store.KV(), testHead); err != nil || !ok || root != rootB {
		t.Fatalf("checkpoint after deleted Segment refusal = %s ok=%t err=%v, want %s", root, ok, err, rootB)
	}
	if mirrored, ok, err := f.roots.Get(t.Context(), testHead); err != nil || !ok || mirrored != rootB {
		t.Fatalf("root mirror after deleted Segment refusal = %s ok=%t err=%v, want %s",
			mirrored, ok, err, rootB)
	}
	if got := follow.HeadAdopted(f.f, testHead); got != rootB {
		t.Fatalf("served root after deleted Segment refusal = %s, want %s", got, rootB)
	}
	afterPins, err := catalog.NewLedger(f.store.KV()).ListAll(t.Context(), testHead)
	if err != nil {
		t.Fatalf("Ledger.ListAll(after rejection): %v", err)
	}
	if !reflect.DeepEqual(afterPins, beforePins) {
		t.Fatalf("rejected cached Segment changed pin ledger:\nbefore=%v\nafter=%v", beforePins, afterPins)
	}
	if floor, ok, err := follow.ReadUpdatedAt(f.store.KV()); err != nil || !ok || !floor.Equal(beforeFloor) {
		t.Fatalf("freshness floor after deleted Segment refusal = %s ok=%t err=%v, want %s",
			floor, ok, err, beforeFloor)
	}

	// Restoring the block only at the writer forces the ordinary fetching
	// blockstore path to re-establish local presence. The same signed document
	// must then admit successfully without clearing the process-local cache.
	if err := w.store.Blocks().Put(t.Context(), segmentABlock); err != nil {
		t.Fatalf("restoring Segment A at writer: %v", err)
	}
	if err := f.pollErr(); err != nil {
		t.Fatalf("Poll after restoring cached Segment: %v", err)
	}
	if got := follow.HeadAdopted(f.f, testHead); got != rootC {
		t.Fatalf("served root after Segment restore = %s, want %s", got, rootC)
	}
	if !f.hasLocally(segmentA) {
		t.Fatalf("restored Segment %s was not fetched into follower storage", segmentA)
	}
	if retention.current == nil || retention.current.Heads[0].Root != rootC {
		t.Fatalf("retention after Segment restore = %#v, want root C %s", retention.current, rootC)
	}
}
