package archive_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/ipfs/boxo/blockstore"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"

	"github.com/blobarchive/bloar/archive"
	"github.com/blobarchive/bloar/schema"
)

func putSecurityBlock(t *testing.T, bs blockstore.Blockstore, data []byte, id cid.Cid) cid.Cid {
	t.Helper()
	blk, err := blocks.NewBlockWithCid(data, id)
	if err != nil {
		t.Fatalf("blocks.NewBlockWithCid(%s): %v", id, err)
	}
	if err := bs.Put(t.Context(), blk); err != nil {
		t.Fatalf("Blockstore.Put(%s): %v", id, err)
	}
	return id
}

func putSecurityDir(t *testing.T, bs blockstore.Blockstore, kids ...cid.Cid) cid.Cid {
	t.Helper()
	data, id, err := schema.EncodeDirNode(&schema.DirNode{Kids: kids})
	if err != nil {
		t.Fatalf("schema.EncodeDirNode: %v", err)
	}
	return putSecurityBlock(t, bs, data, id)
}

func putSecuritySegment(t *testing.T, bs blockstore.Blockstore, slot0 uint64, nonempty bool) cid.Cid {
	t.Helper()
	segment := &schema.Segment{Slot0: slot0}
	if nonempty {
		vh := mkVH(slot0 + 1)
		segment.Rows = []schema.Row{{
			Slot: slot0,
			Entries: []schema.RefEntry{{
				VH:   vh,
				Blob: blobCID(t, vh),
			}},
		}}
	}
	data, id, err := schema.EncodeSegment(segment)
	if err != nil {
		t.Fatalf("schema.EncodeSegment(slot0=%d): %v", slot0, err)
	}
	return putSecurityBlock(t, bs, data, id)
}

func putSecurityHead(t *testing.T, bs blockstore.Blockstore, syncedTo, segBits, fanoutBits, depth uint64, dir, open cid.Cid) cid.Cid {
	t.Helper()
	obj := &schema.Head{
		Name:       "test",
		Net:        "testnet",
		OriginSlot: 0,
		SyncedTo:   &syncedTo,
		SegBits:    segBits,
		FanoutBits: fanoutBits,
		DirDepth:   depth,
		Dir:        dir,
		Open:       open,
	}
	data, id, err := schema.EncodeHead(obj)
	if err != nil {
		t.Fatalf("schema.EncodeHead: %v", err)
	}
	return putSecurityBlock(t, bs, data, id)
}

func loadSecurityHead(t *testing.T, bs blockstore.Blockstore, root cid.Cid) *archive.Head {
	t.Helper()
	head, err := archive.Load(t.Context(), archive.Config{Blocks: bs}, root)
	if err != nil {
		t.Fatalf("archive.Load(%s): %v", root, err)
	}
	return head
}

func requireSecurityError(t *testing.T, err error, contains string) {
	t.Helper()
	if err == nil {
		t.Fatalf("operation succeeded, want an error containing %q", contains)
	}
	if !strings.Contains(err.Error(), contains) {
		t.Fatalf("error = %v, want it to contain %q", err, contains)
	}
}

func TestLoadRejectsNoncanonicalDirectoryDepth(t *testing.T) {
	bs := newBlockstore()
	segment := putSecuritySegment(t, bs, 0, true)
	dir := putSecurityDir(t, bs, segment)
	open := putSecuritySegment(t, bs, 1, false)
	root := putSecurityHead(t, bs, 0, 0, 2, 2, dir, open)

	_, err := archive.Load(t.Context(), archive.Config{Blocks: bs}, root)
	requireSecurityError(t, err, "dir_depth 2 is not canonical depth 1")
}

func TestLoadRejectsOversizedEnumerationOutputGeometry(t *testing.T) {
	bs := newBlockstore()
	dummyDir := putSecurityDir(t, bs)
	dummyOpen := putSecuritySegment(t, bs, archive.MaxEnumerationOutputs+1, false)
	// With seg_bits=0, synced_to=N means N+1 sealed positions. The geometry is
	// rejected from the authenticated Head before either linked block is read.
	root := putSecurityHead(t, bs, archive.MaxEnumerationOutputs, 0, 32, 1, dummyDir, dummyOpen)

	_, err := archive.Load(t.Context(), archive.Config{Blocks: bs}, root)
	requireSecurityError(t, err, "exceeds the 1048576-output admission budget")
}

func TestEnumerateRejectsDirectoryOverfanoutAtItsPosition(t *testing.T) {
	bs := newBlockstore()
	segment := putSecuritySegment(t, bs, 0, true)
	// The signed head covers exactly one sealed position. Even though fanout is
	// four, canonical append geometry permits only kid 0 on this leaf.
	dir := putSecurityDir(t, bs, segment, segment, segment, segment, segment)
	open := putSecuritySegment(t, bs, 1, false)
	root := putSecurityHead(t, bs, 0, 0, 2, 1, dir, open)

	_, err := loadSecurityHead(t, bs, root).Enumerate(t.Context())
	requireSecurityError(t, err, "canonical position allows 1")
}

func TestEnumerateRejectsDirectoryLevelTypeConfusion(t *testing.T) {
	t.Run("leaf names a DirNode as a Segment", func(t *testing.T) {
		bs := newBlockstore()
		notSegment := putSecurityDir(t, bs)
		dir := putSecurityDir(t, bs, notSegment)
		open := putSecuritySegment(t, bs, 1, false)
		root := putSecurityHead(t, bs, 0, 0, 2, 1, dir, open)

		_, err := loadSecurityHead(t, bs, root).Enumerate(t.Context())
		requireSecurityError(t, err, "decoding Segment")
	})

	t.Run("internal page names a Segment as a DirNode", func(t *testing.T) {
		bs := newBlockstore()
		notDir := putSecuritySegment(t, bs, 0, true)
		emptyLeaf := putSecurityDir(t, bs)
		dir := putSecurityDir(t, bs, notDir, emptyLeaf)
		open := putSecuritySegment(t, bs, 5, false)
		root := putSecurityHead(t, bs, 4, 0, 2, 2, dir, open)

		_, err := loadSecurityHead(t, bs, root).Enumerate(t.Context())
		requireSecurityError(t, err, "decoding DirNode")
	})
}

func TestEnumerateRejectsSegmentAtWrongDirectoryPosition(t *testing.T) {
	bs := newBlockstore()
	wrong := putSecuritySegment(t, bs, 99, true)
	dir := putSecurityDir(t, bs, wrong)
	open := putSecuritySegment(t, bs, 1, false)
	root := putSecurityHead(t, bs, 0, 0, 2, 1, dir, open)

	_, err := loadSecurityHead(t, bs, root).Enumerate(t.Context())
	requireSecurityError(t, err, "path position requires 0")
}

func TestEnumerateRejectsSegmentRowsOutsidePathWindow(t *testing.T) {
	bs := newBlockstore()
	vh := mkVH(1)
	data, segment, err := schema.EncodeSegment(&schema.Segment{
		Slot0: 0,
		Rows: []schema.Row{{
			Slot:    1,
			Entries: []schema.RefEntry{{VH: vh, Blob: blobCID(t, vh)}},
		}},
	})
	if err != nil {
		t.Fatalf("schema.EncodeSegment(out-of-window row): %v", err)
	}
	putSecurityBlock(t, bs, data, segment)
	dir := putSecurityDir(t, bs, segment)
	open := putSecuritySegment(t, bs, 1, false)
	root := putSecurityHead(t, bs, 0, 0, 2, 1, dir, open)

	_, err = loadSecurityHead(t, bs, root).Enumerate(t.Context())
	requireSecurityError(t, err, "path window permits [0,0]")
}

func TestEnumerateRejectsNonBlobSegmentLink(t *testing.T) {
	bs := newBlockstore()
	notBlob := putSecurityDir(t, bs)
	var vh schema.VersionedHash
	vh[0] = 1
	data, segment, err := schema.EncodeSegment(&schema.Segment{
		Slot0: 0,
		Rows: []schema.Row{{
			Slot:    0,
			Entries: []schema.RefEntry{{VH: vh, Blob: notBlob}},
		}},
	})
	if err != nil {
		t.Fatalf("schema.EncodeSegment(non-blob link): %v", err)
	}
	putSecurityBlock(t, bs, data, segment)
	dir := putSecurityDir(t, bs, segment)
	open := putSecuritySegment(t, bs, 1, false)
	root := putSecurityHead(t, bs, 0, 0, 2, 1, dir, open)

	_, err = loadSecurityHead(t, bs, root).Enumerate(t.Context())
	requireSecurityError(t, err, "want CIDv1 raw sha2-256/32")
}

func TestEnumerateRejectsRepeatedNonemptySubtree(t *testing.T) {
	bs := newBlockstore()
	segment := putSecuritySegment(t, bs, 0, true)
	leaf := putSecurityDir(t, bs, segment)
	// Eight sealed positions at fanout four require two leaf pages. Reusing one
	// non-empty page at both positions used to multiply its output recursively.
	dir := putSecurityDir(t, bs, leaf, leaf)
	open := putSecuritySegment(t, bs, 8, false)
	root := putSecurityHead(t, bs, 7, 0, 2, 2, dir, open)

	_, err := loadSecurityHead(t, bs, root).Enumerate(t.Context())
	requireSecurityError(t, err, "shared at multiple positions")
}

func TestEnumerateAccountsSharedEmptySubtreeWithoutExpansion(t *testing.T) {
	bs := newBlockstore()
	emptyLeaf := putSecurityDir(t, bs)
	// Empty canonical pages naturally have identical bytes and CIDs. Sharing
	// them is valid, but both logical positions still consume the path budget.
	dir := putSecurityDir(t, bs, emptyLeaf, emptyLeaf)
	open := putSecuritySegment(t, bs, 8, false)
	root := putSecurityHead(t, bs, 7, 0, 2, 2, dir, open)

	enum, err := loadSecurityHead(t, bs, root).Enumerate(t.Context())
	if err != nil {
		t.Fatalf("Enumerate(shared empty pages): %v", err)
	}
	if len(enum.Sealed) != 0 {
		t.Fatalf("sealed segments = %d, want 0", len(enum.Sealed))
	}
	if len(enum.DirPages) != 2 {
		t.Fatalf("distinct directory pages = %d, want root plus one shared empty leaf", len(enum.DirPages))
	}
}

type securityOverrideStore struct {
	blockstore.Blockstore
	target cid.Cid
	data   []byte
}

func (s *securityOverrideStore) Get(ctx context.Context, id cid.Cid) (blocks.Block, error) {
	if id == s.target {
		return blocks.NewBlockWithCid(s.data, id)
	}
	return s.Blockstore.Get(ctx, id)
}

func TestEnumerateRejectsCycleShapedCorruptBlock(t *testing.T) {
	bs := newBlockstore()
	cycleID := putSecurityDir(t, bs)
	cycleData, _, err := schema.EncodeDirNode(&schema.DirNode{Kids: []cid.Cid{cycleID, cycleID}})
	if err != nil {
		t.Fatalf("schema.EncodeDirNode(cycle): %v", err)
	}
	open := putSecuritySegment(t, bs, 5, false)
	root := putSecurityHead(t, bs, 4, 0, 2, 2, cycleID, open)
	override := &securityOverrideStore{Blockstore: bs, target: cycleID, data: cycleData}

	start := time.Now()
	_, err = loadSecurityHead(t, override, root).Enumerate(t.Context())
	requireSecurityError(t, err, "it is corrupt")
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("cycle-shaped block rejection took %s, want a bounded immediate refusal", elapsed)
	}
}

func TestEnumerateRejectsOversizedIndexNodeBeforeDecode(t *testing.T) {
	bs := newBlockstore()
	dirID := putSecurityDir(t, bs)
	open := putSecuritySegment(t, bs, 1, false)
	root := putSecurityHead(t, bs, 0, 0, 2, 1, dirID, open)
	override := &securityOverrideStore{
		Blockstore: bs,
		target:     dirID,
		data:       make([]byte, archive.MaxEnumerationNodeBytes+1),
	}

	_, err := loadSecurityHead(t, override, root).Enumerate(t.Context())
	requireSecurityError(t, err, "per-node admission budget")
}

func TestEnumerateHonorsCallerCancellation(t *testing.T) {
	bs := newBlockstore()
	segment := putSecuritySegment(t, bs, 0, true)
	dir := putSecurityDir(t, bs, segment)
	open := putSecuritySegment(t, bs, 1, false)
	root := putSecurityHead(t, bs, 0, 0, 2, 1, dir, open)
	head := loadSecurityHead(t, bs, root)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := head.Enumerate(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Enumerate(canceled) error = %v, want context.Canceled", err)
	}
}

func TestEnumerationStructureCacheWithoutGenerationReestablishesSegmentPresence(t *testing.T) {
	bs := newCountingStore()
	entries := make([]cid.Cid, 8)
	for i := range entries {
		entries[i] = putSecuritySegment(t, bs, uint64(i), true)
	}
	dir := buildSecurityDirectory(t, bs, entries, 2, 2)
	open := putSecuritySegment(t, bs, 8, false)
	root := putSecurityHead(t, bs, 7, 0, 2, 2, dir, open)
	cache := archive.NewStructureCache()

	first, err := archive.Load(t.Context(), archive.Config{Blocks: bs, StructureCache: cache}, root)
	if err != nil {
		t.Fatalf("archive.Load(first): %v", err)
	}
	if _, err := first.Enumerate(t.Context()); err != nil {
		t.Fatalf("first Enumerate: %v", err)
	}

	bs.reset()
	second, err := archive.Load(t.Context(), archive.Config{Blocks: bs, StructureCache: cache}, root)
	if err != nil {
		t.Fatalf("archive.Load(second): %v", err)
	}
	if _, err := second.Enumerate(t.Context()); err != nil {
		t.Fatalf("second Enumerate: %v", err)
	}

	bs.mu.Lock()
	for _, segment := range append(entries, open) {
		if got := bs.gets[segment]; got != 1 {
			t.Errorf("cached Segment %s fetched %d times on the second admission, want 1 presence/integrity read", segment, got)
		}
	}
	bs.mu.Unlock()

	if err := bs.DeleteBlock(t.Context(), entries[0]); err != nil {
		t.Fatalf("DeleteBlock(cached Segment): %v", err)
	}
	third, err := archive.Load(t.Context(), archive.Config{Blocks: bs, StructureCache: cache}, root)
	if err != nil {
		t.Fatalf("archive.Load(third): %v", err)
	}
	if _, err := third.Enumerate(t.Context()); err == nil || !strings.Contains(err.Error(), entries[0].String()) {
		t.Fatalf("third Enumerate after cached Segment deletion = %v, want missing %s", err, entries[0])
	}
}

type generationCountingStore struct {
	*countingStore
	generation uint64
}

func (s *generationCountingStore) CollectionGeneration() uint64 { return s.generation }

type generationAdvancingStore struct {
	*generationCountingStore
	advanceOn cid.Cid
	advanced  bool
}

func (s *generationAdvancingStore) Get(ctx context.Context, id cid.Cid) (blocks.Block, error) {
	block, err := s.generationCountingStore.Get(ctx, id)
	if err == nil && id == s.advanceOn && !s.advanced {
		s.advanced = true
		s.generation++
	}
	return block, err
}

func TestEnumerationStructureCacheReusesProofWithinCollectionGeneration(t *testing.T) {
	bs := &generationCountingStore{countingStore: newCountingStore()}
	entries := make([]cid.Cid, 8)
	for i := range entries {
		entries[i] = putSecuritySegment(t, bs, uint64(i), true)
	}
	dir := buildSecurityDirectory(t, bs, entries, 2, 2)
	open := putSecuritySegment(t, bs, 8, false)
	root := putSecurityHead(t, bs, 7, 0, 2, 2, dir, open)
	cache := archive.NewStructureCache()

	admit := func(label string) error {
		t.Helper()
		head, err := archive.Load(t.Context(), archive.Config{Blocks: bs, StructureCache: cache}, root)
		if err != nil {
			t.Fatalf("archive.Load(%s): %v", label, err)
		}
		_, err = head.Enumerate(t.Context())
		return err
	}
	if err := admit("first"); err != nil {
		t.Fatalf("first Enumerate: %v", err)
	}

	bs.reset()
	if err := admit("same generation"); err != nil {
		t.Fatalf("same-generation Enumerate: %v", err)
	}
	bs.mu.Lock()
	for _, segment := range append(entries, open) {
		if got := bs.gets[segment]; got != 0 {
			t.Errorf("cached Segment %s fetched %d times in the unchanged generation, want 0", segment, got)
		}
	}
	bs.mu.Unlock()

	bs.generation++
	bs.reset()
	if err := admit("new generation"); err != nil {
		t.Fatalf("new-generation Enumerate: %v", err)
	}
	bs.mu.Lock()
	defer bs.mu.Unlock()
	for _, segment := range append(entries, open) {
		if got := bs.gets[segment]; got != 1 {
			t.Errorf("cached Segment %s fetched %d times after the generation advanced, want 1", segment, got)
		}
	}
}

func TestEnumerationRefusesProofAcrossConcurrentCollectionBoundary(t *testing.T) {
	counted := &generationCountingStore{countingStore: newCountingStore()}
	segment := putSecuritySegment(t, counted, 0, true)
	dir := putSecurityDir(t, counted, segment)
	open := putSecuritySegment(t, counted, 1, false)
	root := putSecurityHead(t, counted, 0, 0, 2, 1, dir, open)
	bs := &generationAdvancingStore{generationCountingStore: counted, advanceOn: segment}

	head, err := archive.Load(t.Context(), archive.Config{Blocks: bs, StructureCache: archive.NewStructureCache()}, root)
	if err != nil {
		t.Fatalf("archive.Load: %v", err)
	}
	if _, err := head.Enumerate(t.Context()); err == nil || !strings.Contains(err.Error(), "collection generation changed") {
		t.Fatalf("Enumerate across collection boundary = %v, want generation-change refusal", err)
	}
}

func buildSecurityDirectory(t *testing.T, bs blockstore.Blockstore, entries []cid.Cid, fanoutBits, depth uint64) cid.Cid {
	t.Helper()
	if depth == 1 {
		return putSecurityDir(t, bs, entries...)
	}
	childCapacity := uint64(1) << ((depth - 1) * fanoutBits)
	children := make([]cid.Cid, 0, 1+(uint64(len(entries))-1)/childCapacity)
	for first := uint64(0); first < uint64(len(entries)); first += childCapacity {
		last := first + childCapacity
		if last > uint64(len(entries)) {
			last = uint64(len(entries))
		}
		children = append(children, buildSecurityDirectory(t, bs, entries[first:last], fanoutBits, depth-1))
	}
	return putSecurityDir(t, bs, children...)
}

func TestEnumerateLargeCanonicalHeadRegression(t *testing.T) {
	if testing.Short() {
		t.Skip("large canonical-head regression")
	}
	const (
		sealed    = 32 * 1024
		fanoutBit = 8
	)
	bs := newBlockstore()
	entries := make([]cid.Cid, sealed)
	for i := range entries {
		entries[i] = putSecuritySegment(t, bs, uint64(i), true)
	}
	depth := canonicalDepth(sealed, fanoutBit)
	dir := buildSecurityDirectory(t, bs, entries, fanoutBit, depth)
	open := putSecuritySegment(t, bs, sealed, false)
	root := putSecurityHead(t, bs, sealed-1, 0, fanoutBit, depth, dir, open)

	start := time.Now()
	enum, err := loadSecurityHead(t, bs, root).Enumerate(t.Context())
	if err != nil {
		t.Fatalf("Enumerate(%d-segment canonical head): %v", sealed, err)
	}
	if len(enum.Sealed) != sealed {
		t.Fatalf("sealed segments = %d, want %d", len(enum.Sealed), sealed)
	}
	for i, got := range enum.Sealed {
		if got.Ord != uint64(i) {
			t.Fatalf("sealed[%d].Ord = %d, want %d", i, got.Ord, i)
		}
	}
	t.Logf("validated %d sealed Segments and %d distinct DirNodes in %s",
		len(enum.Sealed), len(enum.DirPages), time.Since(start))
}
