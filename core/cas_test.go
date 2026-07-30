package core_test

import (
	"context"
	"sync"
	"testing"

	"github.com/ipfs/boxo/blockstore"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/multiformats/go-multihash"

	"github.com/blobarchive/bloar/core"
	"github.com/blobarchive/bloar/schema"
)

// countingStore counts Get calls per CID so tests can assert which blocks a
// read path actually touched.
type countingStore struct {
	blockstore.Blockstore

	mu   sync.Mutex
	gets map[cid.Cid]int
}

func newCountingStore() *countingStore {
	ds := dssync.MutexWrap(datastore.NewMapDatastore())
	return &countingStore{
		Blockstore: blockstore.NewBlockstore(ds),
		gets:       make(map[cid.Cid]int),
	}
}

func (s *countingStore) Get(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	s.mu.Lock()
	s.gets[c]++
	s.mu.Unlock()
	return s.Blockstore.Get(ctx, c)
}

func (s *countingStore) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.gets = make(map[cid.Cid]int)
}

func (s *countingStore) getCount(c cid.Cid) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.gets[c]
}

func (s *countingStore) totalGets() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, v := range s.gets {
		n += v
	}
	return n
}

// blockCount returns how many blocks the store holds.
func blockCount(t *testing.T, bs blockstore.Blockstore) int {
	t.Helper()
	ch, err := bs.AllKeysChan(context.Background())
	if err != nil {
		t.Fatalf("AllKeysChan: %v", err)
	}
	n := 0
	for range ch {
		n++
	}
	return n
}

func dirCodec() core.Codec[schema.DirNode] {
	return core.Codec[schema.DirNode]{Encode: schema.EncodeDirNode, Decode: schema.DecodeDirNode}
}

func segCodec() core.Codec[schema.Segment] {
	return core.Codec[schema.Segment]{Encode: schema.EncodeSegment, Decode: schema.DecodeSegment}
}

// fakeBlobCID returns a raw-codec CID standing in for a blob block. Segments
// only require the link to be defined; no blob bytes are involved here.
func fakeBlobCID(t *testing.T, seed string) cid.Cid {
	t.Helper()
	mh, err := multihash.Sum([]byte(seed), multihash.SHA2_256, -1)
	if err != nil {
		t.Fatalf("multihash.Sum: %v", err)
	}
	return cid.NewCidV1(cid.Raw, mh)
}

func vh(t *testing.T, b byte) schema.VersionedHash {
	t.Helper()
	var v schema.VersionedHash
	v[0] = 0x01
	v[31] = b
	return v
}

func segment(t *testing.T, slot0 uint64, seed string) *schema.Segment {
	t.Helper()
	return &schema.Segment{
		Slot0: slot0,
		Rows: []schema.Row{{
			Slot:    slot0,
			Entries: []schema.RefEntry{{VH: vh(t, byte(slot0)), Blob: fakeBlobCID(t, seed)}},
		}},
	}
}

// tree is the fixture: a root DirNode over two DirNode pages, each over two
// Segments. It mirrors the real shape (dir pages down to segment leaves) and
// gives every test an untouched half to make assertions about.
//
//	root
//	├── dirA -> segA0, segA1
//	└── dirB -> segB0, segB1
type tree struct {
	root, dirA, dirB cid.Cid
	segA0, segA1     cid.Cid
	segB0, segB1     cid.Cid
	bs               *countingStore
	dirStore         *core.NodeStore[schema.DirNode]
	segStore         *core.NodeStore[schema.Segment]
}

// buildTree writes the fixture bottom-up, exactly as archive/ will: children
// commit first, their CIDs go into the parent, then the parent commits.
func buildTree(t *testing.T, cache *core.NodeCache) *tree {
	t.Helper()
	ctx := context.Background()
	bs := newCountingStore()
	tr := &tree{
		bs:       bs,
		dirStore: core.NewNodeStore(bs, dirCodec(), cache),
		segStore: core.NewNodeStore(bs, segCodec(), cache),
	}

	segs := []struct {
		dst   *cid.Cid
		slot0 uint64
		seed  string
	}{
		{&tr.segA0, 0, "a0"},
		{&tr.segA1, 512, "a1"},
		{&tr.segB0, 1024, "b0"},
		{&tr.segB1, 1536, "b1"},
	}
	for _, s := range segs {
		c, err := tr.segStore.NewNode(segment(t, s.slot0, s.seed)).Commit(ctx)
		if err != nil {
			t.Fatalf("commit segment %s: %v", s.seed, err)
		}
		*s.dst = c
	}

	var err error
	if tr.dirA, err = tr.dirStore.NewNode(&schema.DirNode{Kids: []cid.Cid{tr.segA0, tr.segA1}}).Commit(ctx); err != nil {
		t.Fatalf("commit dirA: %v", err)
	}
	if tr.dirB, err = tr.dirStore.NewNode(&schema.DirNode{Kids: []cid.Cid{tr.segB0, tr.segB1}}).Commit(ctx); err != nil {
		t.Fatalf("commit dirB: %v", err)
	}
	if tr.root, err = tr.dirStore.NewNode(&schema.DirNode{Kids: []cid.Cid{tr.dirA, tr.dirB}}).Commit(ctx); err != nil {
		t.Fatalf("commit root: %v", err)
	}
	return tr
}

// mutateLeftLeaf appends a row to segA1 and rewrites the spine above it,
// bottom-up. It returns the new segA1, dirA and root CIDs.
func (tr *tree) mutateLeftLeaf(t *testing.T) (newSegA1, newDirA, newRoot cid.Cid) {
	t.Helper()
	ctx := context.Background()

	segPtr := tr.segStore.Pointer(tr.segA1)
	seg, err := segPtr.Mutate(ctx)
	if err != nil {
		t.Fatalf("mutate segA1: %v", err)
	}
	seg.Rows = append(seg.Rows, schema.Row{
		Slot:    seg.Slot0 + 1,
		Entries: []schema.RefEntry{{VH: vh(t, 0xff), Blob: fakeBlobCID(t, "new")}},
	})
	if newSegA1, err = segPtr.Commit(ctx); err != nil {
		t.Fatalf("commit mutated segA1: %v", err)
	}

	dirPtr := tr.dirStore.Pointer(tr.dirA)
	dir, err := dirPtr.Mutate(ctx)
	if err != nil {
		t.Fatalf("mutate dirA: %v", err)
	}
	dir.Kids[1] = newSegA1
	if newDirA, err = dirPtr.Commit(ctx); err != nil {
		t.Fatalf("commit mutated dirA: %v", err)
	}

	rootPtr := tr.dirStore.Pointer(tr.root)
	root, err := rootPtr.Mutate(ctx)
	if err != nil {
		t.Fatalf("mutate root: %v", err)
	}
	root.Kids[0] = newDirA
	if newRoot, err = rootPtr.Commit(ctx); err != nil {
		t.Fatalf("commit mutated root: %v", err)
	}
	return newSegA1, newDirA, newRoot
}

// TestStructuralSharing: mutating one leaf rewrites only that leaf's spine.
// Every node off the spine keeps its exact CID, its block is neither
// duplicated nor altered, and the new root points at the old right-hand page
// by its original CID.
func TestStructuralSharing(t *testing.T) {
	ctx := context.Background()
	tr := buildTree(t, nil)

	before := make(map[cid.Cid][]byte)
	for _, c := range []cid.Cid{tr.segA0, tr.segA1, tr.segB0, tr.segB1, tr.dirA, tr.dirB, tr.root} {
		blk, err := tr.bs.Get(ctx, c)
		if err != nil {
			t.Fatalf("reading %s: %v", c, err)
		}
		before[c] = blk.RawData()
	}
	blocksBefore := blockCount(t, tr.bs)

	newSegA1, newDirA, newRoot := tr.mutateLeftLeaf(t)

	changed := []struct {
		name     string
		old, new cid.Cid
	}{
		{"segA1", tr.segA1, newSegA1},
		{"dirA", tr.dirA, newDirA},
		{"root", tr.root, newRoot},
	}
	for _, c := range changed {
		if c.old == c.new {
			t.Errorf("%s: CID unchanged after mutation: %s", c.name, c.old)
		}
	}

	untouched := []struct {
		name string
		c    cid.Cid
	}{
		{"segA0", tr.segA0},
		{"segB0", tr.segB0},
		{"segB1", tr.segB1},
		{"dirB", tr.dirB},
	}
	for _, u := range untouched {
		blk, err := tr.bs.Get(ctx, u.c)
		if err != nil {
			t.Errorf("%s: block gone after mutation: %v", u.name, err)
			continue
		}
		if got := blk.RawData(); string(got) != string(before[u.c]) {
			t.Errorf("%s: block bytes changed under its CID", u.name)
		}
		if blk.Cid() != u.c {
			t.Errorf("%s: block CID changed: got %s want %s", u.name, blk.Cid(), u.c)
		}
	}

	// The untouched half must be reachable from the new root by its original
	// CID: it was neither re-encoded nor re-hashed.
	rootNode, err := tr.dirStore.GetNode(ctx, newRoot)
	if err != nil {
		t.Fatalf("reading new root: %v", err)
	}
	if rootNode.Kids[1] != tr.dirB {
		t.Errorf("new root's right kid: got %s want the original dirB %s", rootNode.Kids[1], tr.dirB)
	}
	newDirANode, err := tr.dirStore.GetNode(ctx, newDirA)
	if err != nil {
		t.Fatalf("reading new dirA: %v", err)
	}
	if newDirANode.Kids[0] != tr.segA0 {
		t.Errorf("new dirA's left kid: got %s want the original segA0 %s", newDirANode.Kids[0], tr.segA0)
	}

	// Exactly the three rewritten nodes are new: no untouched node was
	// re-written under a second key.
	if got, want := blockCount(t, tr.bs), blocksBefore+len(changed); got != want {
		t.Errorf("block count: got %d want %d (%d before + %d rewritten)", got, want, blocksBefore, len(changed))
	}
}

// TestLaziness: reading one path loads exactly that path.
//
// The Rust prototype's version of this test reset its counters after the
// insert and so measured only the commit phase, which never loads: it asserted
// nothing. This one counts the reads themselves.
func TestLaziness(t *testing.T) {
	ctx := context.Background()
	tr := buildTree(t, nil) // no cache: every read must reach the blockstore

	tr.bs.reset()

	// Read one slot: root -> dirA -> segA0, nothing else.
	rootPtr := tr.dirStore.Pointer(tr.root)
	root, err := rootPtr.Load(ctx)
	if err != nil {
		t.Fatalf("load root: %v", err)
	}
	dirPtr := tr.dirStore.Pointer(root.Kids[0])
	dir, err := dirPtr.Load(ctx)
	if err != nil {
		t.Fatalf("load dirA: %v", err)
	}
	segPtr := tr.segStore.Pointer(dir.Kids[0])
	if _, err := segPtr.Load(ctx); err != nil {
		t.Fatalf("load segA0: %v", err)
	}

	onPath := []struct {
		name string
		c    cid.Cid
	}{
		{"root", tr.root},
		{"dirA", tr.dirA},
		{"segA0", tr.segA0},
	}
	offPath := []struct {
		name string
		c    cid.Cid
	}{
		{"dirB", tr.dirB},
		{"segA1", tr.segA1},
		{"segB0", tr.segB0},
		{"segB1", tr.segB1},
	}
	for _, n := range onPath {
		if got := tr.bs.getCount(n.c); got != 1 {
			t.Errorf("%s: got %d Gets, want 1", n.name, got)
		}
	}
	for _, n := range offPath {
		if got := tr.bs.getCount(n.c); got != 0 {
			t.Errorf("%s: got %d Gets, want 0 (off the read path)", n.name, got)
		}
	}
	if got, want := tr.bs.totalGets(), len(onPath); got != want {
		t.Errorf("total Gets: got %d want %d", got, want)
	}

	// A second touch of an already-loaded pointer must not re-read.
	if _, err := rootPtr.Load(ctx); err != nil {
		t.Fatalf("re-load root: %v", err)
	}
	if got := tr.bs.getCount(tr.root); got != 1 {
		t.Errorf("root re-Load hit the blockstore: got %d Gets, want 1", got)
	}
}

// TestOldRootOrphaning: a commit never mutates in place. The old root survives
// intact and decodes to the old tree; the new root is a different block; and
// nothing reachable from the new root references the old root or the old spine
// nodes it replaced.
func TestOldRootOrphaning(t *testing.T) {
	ctx := context.Background()
	tr := buildTree(t, nil)

	oldRootBlock, err := tr.bs.Get(ctx, tr.root)
	if err != nil {
		t.Fatalf("reading old root: %v", err)
	}
	oldRootBytes := string(oldRootBlock.RawData())

	newSegA1, newDirA, newRoot := tr.mutateLeftLeaf(t)

	if newRoot == tr.root {
		t.Fatalf("new root equals old root %s", tr.root)
	}

	has, err := tr.bs.Has(ctx, tr.root)
	if err != nil {
		t.Fatalf("Has(old root): %v", err)
	}
	if !has {
		t.Errorf("old root block was removed by the commit")
	}
	oldRoot, err := tr.dirStore.GetNode(ctx, tr.root)
	if err != nil {
		t.Fatalf("old root no longer decodes: %v", err)
	}
	if oldRoot.Kids[0] != tr.dirA || oldRoot.Kids[1] != tr.dirB {
		t.Errorf("old root was mutated in place: kids are %s,%s want %s,%s",
			oldRoot.Kids[0], oldRoot.Kids[1], tr.dirA, tr.dirB)
	}
	if blk, err := tr.bs.Get(ctx, tr.root); err != nil {
		t.Errorf("re-reading old root: %v", err)
	} else if string(blk.RawData()) != oldRootBytes {
		t.Errorf("old root block bytes changed")
	}

	reachable := reachableFrom(t, tr, newRoot)
	orphaned := []struct {
		name string
		c    cid.Cid
	}{
		{"old root", tr.root},
		{"old dirA", tr.dirA},
		{"old segA1", tr.segA1},
	}
	for _, o := range orphaned {
		if reachable[o.c] {
			t.Errorf("%s (%s) is still referenced from the new tree", o.name, o.c)
		}
	}
	shared := []struct {
		name string
		c    cid.Cid
	}{
		{"dirB", tr.dirB},
		{"segB0", tr.segB0},
		{"segB1", tr.segB1},
		{"segA0", tr.segA0},
		{"new segA1", newSegA1},
		{"new dirA", newDirA},
	}
	for _, s := range shared {
		if !reachable[s.c] {
			t.Errorf("%s (%s) is not reachable from the new tree", s.name, s.c)
		}
	}
}

// reachableFrom walks the two-level dir tree under root and returns every CID
// it references, including root itself.
func reachableFrom(t *testing.T, tr *tree, root cid.Cid) map[cid.Cid]bool {
	t.Helper()
	ctx := context.Background()
	seen := map[cid.Cid]bool{root: true}
	rootNode, err := tr.dirStore.GetNode(ctx, root)
	if err != nil {
		t.Fatalf("walking root %s: %v", root, err)
	}
	for _, page := range rootNode.Kids {
		if !page.Defined() {
			continue
		}
		seen[page] = true
		pageNode, err := tr.dirStore.GetNode(ctx, page)
		if err != nil {
			t.Fatalf("walking page %s: %v", page, err)
		}
		for _, leaf := range pageNode.Kids {
			if leaf.Defined() {
				seen[leaf] = true
			}
		}
	}
	return seen
}
