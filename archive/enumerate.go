package archive

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"

	"github.com/blobarchive/bloar/schema"
)

// Enumeration is an admission boundary for a signed remote root, not merely a
// convenience walk. These ceilings bound one head validation independently of
// the signer's declared geometry. The shipped ALL head currently has ~12,000
// sealed Segments and ~1.65 GiB of encoded Segment data, leaving substantial
// growth room without permitting a tiny repeated-child DAG to allocate or walk
// without limit.
const (
	MaxEnumerationOutputs     = 1 << 20 // non-null sealed Segment results
	MaxEnumerationPaths       = 1 << 21 // logical DirNode positions
	MaxEnumerationUniqueNodes = 1 << 22 // Head + open + dirs + sealed
	// MaxIndexNodeBytes is the shared writer and reader boundary for one
	// encoded DAG-CBOR index block. Writers refuse to store a new Head,
	// DirNode or Segment above it; readers refuse a remote block above it
	// before decoding.
	MaxIndexNodeBytes = 2 << 20
	// MaxEnumerationNodeBytes retains the reader-budget name used by callers.
	// It aliases the shared boundary so admission and publication cannot drift.
	MaxEnumerationNodeBytes    = MaxIndexNodeBytes
	MaxEnumerationDecodedBytes = uint64(4) << 30
	MaxEnumerationDuration     = time.Hour
)

// SealedSegment is one non-null entry of a head's directory: the sealed Segment
// block, the window ordinal it holds, and that window's slot bounds.
//
// The bounds are what a retention policy needs and cannot compute for itself:
// a window's range is fixed by seg_bits and the ordinal (spec 4), and the
// ordinal is not stored anywhere -- it is the entry's position in the
// directory, which only a walk from the root recovers.
type SealedSegment struct {
	CID       cid.Cid
	Ord       uint64
	FirstSlot uint64
	LastSlot  uint64
}

// Enumeration is every index block of one head, plus the coordinates a policy
// reasons about. It is the input to pin computation (spec 9), which has to name
// the Head root, every distinct DirNode block and every Segment individually,
// and otherwise has no way to find them: the directory is addressed by
// arithmetic, so nothing but a walk reports what is actually in it.
//
// Blobs are deliberately absent. A policy pins segments, not the rows inside
// them: a recursive pin on a Segment reaches its blobs by traversing the DAG,
// which is GC's job and not this walk's.
//
// It is a snapshot of one root. The head may swap roots the instant it is
// returned, so everything here describes Root and nothing after it.
type Enumeration struct {
	Params Params
	Root   cid.Cid

	// SyncedTo is meaningful only when Covered is true; an uncovered head is
	// the spec's "synced_to: null", which has no directory and no open segment
	// either.
	SyncedTo uint64
	Covered  bool

	// DirPages is every distinct DirNode block reached, root first, in
	// first-visit depth-first order. Canonical empty subtrees can have the same
	// CID at several logical positions; one physical block needs one pin.
	DirPages []cid.Cid
	// Sealed is every sealed Segment, ascending by ordinal. Windows that sealed
	// empty are not here: they seal to a null entry rather than to a block
	// (spec 5.2), so there is nothing to pin.
	Sealed []SealedSegment

	// Open is the open Segment, undefined iff !Covered. OpenOrd is its window.
	Open    cid.Cid
	OpenOrd uint64
}

// Enumerate validates and walks the current root's complete index structure.
// It never reads a blob. A successful result proves, within fixed resource
// bounds, that:
//
//   - signed coverage implies the exact canonical directory depth and page
//     geometry;
//   - every directory page respects fanout and position;
//   - every Segment is in the window its path names;
//   - repeated non-empty or cyclic subgraphs cannot multiply output;
//   - every logical path, unique node, decoded byte and output is budgeted.
//
// Callers admitting a followed root run this before checkpoint, retention or
// pin mutation. A compact content-addressed Segment proof cache makes unchanged
// history incremental across successive roots.
func (h *Head) Enumerate(parent context.Context) (*Enumeration, error) {
	ctx, cancel := context.WithTimeout(parent, MaxEnumerationDuration)
	defer cancel()

	e, err := h.enumerate(ctx)
	if err == nil {
		return e, nil
	}
	if errors.Is(err, context.DeadlineExceeded) && parent.Err() == nil {
		return nil, fmt.Errorf("archive: directory enumeration exceeded the %s time budget: %w",
			MaxEnumerationDuration, err)
	}
	return nil, err
}

func (h *Head) enumerate(ctx context.Context) (*Enumeration, error) {
	generation, generationKnown := h.collectionGeneration()
	st := h.cur.Load()
	openOrd, sealedCount, err := validateDirectoryGeometry(st)
	if err != nil {
		return nil, fmt.Errorf("archive: validating head %s directory geometry: %w", st.root, err)
	}
	expectedPaths := canonicalDirectoryPages(sealedCount, st.params.FanoutBits)

	budget := enumerationBudget{
		unique: make(map[string]struct{}),
	}
	if _, err := budget.readNode(ctx, h, st.root, "Head"); err != nil {
		return nil, err
	}

	e := &Enumeration{
		Params:   st.params,
		Root:     st.root,
		SyncedTo: st.syncedTo,
		Covered:  st.covered,
		Open:     st.open,
	}
	if !st.covered {
		return e, nil
	}
	e.OpenOrd = openOrd

	w := enumerationWalker{
		h: h, st: st, out: e, budget: &budget,
		dirNodes:        make(map[string]*schema.DirNode),
		dirActive:       make(map[string]bool),
		dirMemo:         make(map[dirMemoKey]dirSummary),
		dirContains:     make(map[string]bool),
		segments:        make(map[string]uint64),
		newProofs:       make(map[string]cachedSegmentProof),
		generation:      generation,
		generationKnown: generationKnown,
	}
	if err := w.validateSegment(ctx, st.open, openOrd, st.syncedTo, true); err != nil {
		return nil, fmt.Errorf("archive: validating open segment %s: %w", st.open, err)
	}
	if st.dirDepth > 0 {
		if _, err := w.walkDir(ctx, st.dir, st.dirDepth, 0, sealedCount); err != nil {
			return nil, err
		}
	}
	if budget.paths != expectedPaths {
		return nil, fmt.Errorf("archive: directory walk accounted %d logical pages, canonical geometry requires %d",
			budget.paths, expectedPaths)
	}
	if generationKnown {
		if current := h.cfg.CollectionGeneration.CollectionGeneration(); current != generation {
			return nil, fmt.Errorf("archive: collection generation changed from %d to %d during directory admission",
				generation, current)
		}
	}
	h.structure.rememberSegments(w.newProofs)
	return e, nil
}

func (h *Head) collectionGeneration() (uint64, bool) {
	if h.cfg.CollectionGeneration == nil {
		return 0, false
	}
	return h.cfg.CollectionGeneration.CollectionGeneration(), true
}

// validateDirectoryGeometry derives the only legal tree shape from coverage.
// It runs at Load as the cheap first defence and again at Enumerate to bind a
// walk to the immutable state snapshot it actually uses.
func validateDirectoryGeometry(st *state) (openOrd, sealedCount uint64, err error) {
	if !st.covered {
		if st.dirDepth != 0 || st.dir.Defined() || st.open.Defined() {
			return 0, 0, errors.New("an uncovered head must have no directory or open Segment")
		}
		return 0, 0, nil
	}
	if st.syncedTo == maxUint64 {
		return 0, 0, errors.New("synced_to is MaxUint64, so its required next open window is not representable")
	}
	openOrd = ord(st.syncedTo+1, st.params.SegBits)
	base := st.dirBase()
	if openOrd < base {
		return 0, 0, fmt.Errorf("open window %d precedes directory base %d", openOrd, base)
	}
	sealedCount = openOrd - base
	if sealedCount > MaxEnumerationOutputs {
		return 0, 0, fmt.Errorf("sealed-window count %d exceeds the %d-output admission budget",
			sealedCount, MaxEnumerationOutputs)
	}
	wantDepth := canonicalDepth(sealedCount, st.params.FanoutBits)
	if st.dirDepth != wantDepth {
		return 0, 0, fmt.Errorf("dir_depth %d is not canonical depth %d for %d sealed windows at fanout_bits=%d",
			st.dirDepth, wantDepth, sealedCount, st.params.FanoutBits)
	}
	paths := canonicalDirectoryPages(sealedCount, st.params.FanoutBits)
	if paths > MaxEnumerationPaths {
		return 0, 0, fmt.Errorf("canonical directory requires %d logical pages, above the %d-path admission budget",
			paths, MaxEnumerationPaths)
	}
	// Every non-null sealed position may add one unique Segment. Root and open
	// add two more. Page CIDs can only reduce uniqueness through sharing.
	if paths > MaxEnumerationUniqueNodes-2 ||
		sealedCount > MaxEnumerationUniqueNodes-paths-2 {
		return 0, 0, fmt.Errorf("canonical directory can name %d unique nodes, above the %d-node admission budget",
			sealedCount+paths+2, MaxEnumerationUniqueNodes)
	}
	return openOrd, sealedCount, nil
}

type enumerationBudget struct {
	unique      map[string]struct{}
	paths       uint64
	decodedByte uint64
	outputs     uint64
}

func (b *enumerationBudget) check(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}

func (b *enumerationBudget) addUnique(c cid.Cid) (bool, error) {
	key := c.KeyString()
	if _, ok := b.unique[key]; ok {
		return false, nil
	}
	if len(b.unique) >= MaxEnumerationUniqueNodes {
		return false, fmt.Errorf("archive: directory enumeration exceeds the %d-unique-node budget",
			MaxEnumerationUniqueNodes)
	}
	b.unique[key] = struct{}{}
	return true, nil
}

func (b *enumerationBudget) readNode(ctx context.Context, h *Head, c cid.Cid, kind string) ([]byte, error) {
	if err := b.check(ctx); err != nil {
		return nil, err
	}
	fresh, err := b.addUnique(c)
	if err != nil {
		return nil, err
	}
	if !fresh {
		return nil, fmt.Errorf("archive: internal: attempted to decode %s %s twice", kind, c)
	}
	blk, err := h.cfg.Blocks.Get(ctx, c)
	if err != nil {
		return nil, fmt.Errorf("archive: reading %s %s: %w", kind, c, err)
	}
	data := blk.RawData()
	size := len(data)
	if size < 0 || size > MaxEnumerationNodeBytes {
		return nil, fmt.Errorf("archive: %s %s is %d encoded bytes, outside the [0,%d] per-node admission budget",
			kind, c, size, MaxEnumerationNodeBytes)
	}
	n := uint64(size)
	if n > MaxEnumerationDecodedBytes-b.decodedByte {
		return nil, fmt.Errorf("archive: decoding %s %s would exceed the %d-byte enumeration budget (already %d)",
			kind, c, MaxEnumerationDecodedBytes, b.decodedByte)
	}
	if err := verifyBlockCID(c, data); err != nil {
		return nil, fmt.Errorf("archive: reading %s %s: %w", kind, c, err)
	}
	b.decodedByte += n
	return data, nil
}

func (b *enumerationBudget) addCachedUnique(ctx context.Context, c cid.Cid) error {
	if err := b.check(ctx); err != nil {
		return err
	}
	_, err := b.addUnique(c)
	return err
}

func (b *enumerationBudget) addPaths(n uint64) error {
	if n > MaxEnumerationPaths-b.paths {
		return fmt.Errorf("archive: directory enumeration exceeds the %d-logical-path budget",
			MaxEnumerationPaths)
	}
	b.paths += n
	return nil
}

func (b *enumerationBudget) addOutput() error {
	if b.outputs >= MaxEnumerationOutputs {
		return fmt.Errorf("archive: directory enumeration exceeds the %d-output budget",
			MaxEnumerationOutputs)
	}
	b.outputs++
	return nil
}

type dirMemoKey struct {
	cid     string
	depth   uint64
	entries uint64
}

type dirSummary struct {
	paths    uint64
	segments uint64
}

type enumerationWalker struct {
	h      *Head
	st     *state
	out    *Enumeration
	budget *enumerationBudget

	dirNodes    map[string]*schema.DirNode
	dirActive   map[string]bool
	dirMemo     map[dirMemoKey]dirSummary
	dirContains map[string]bool
	segments    map[string]uint64
	// newProofs is staged until the whole head passes. Rejected signed roots
	// cannot churn the cross-generation cache with unadmitted Segment CIDs.
	newProofs map[string]cachedSegmentProof

	generation      uint64
	generationKnown bool
}

// walkDir validates one logical directory-page position. depth is the number
// of page levels remaining, offset is its first directory index, and entries is
// exactly how many appended positions lie in this subtree.
func (w *enumerationWalker) walkDir(ctx context.Context, node cid.Cid, depth, offset, entries uint64) (dirSummary, error) {
	if err := w.budget.check(ctx); err != nil {
		return dirSummary{}, err
	}
	if !node.Defined() {
		return dirSummary{}, fmt.Errorf("archive: canonical directory page at depth %d offset %d is null", depth, offset)
	}
	if err := validateIndexCID(node, "directory page"); err != nil {
		return dirSummary{}, err
	}
	key := node.KeyString()
	memoKey := dirMemoKey{cid: key, depth: depth, entries: entries}
	if summary, ok := w.dirMemo[memoKey]; ok {
		if summary.segments != 0 {
			return dirSummary{}, fmt.Errorf("archive: non-empty directory subtree %s is shared at multiple positions", node)
		}
		if err := w.budget.addPaths(summary.paths); err != nil {
			return dirSummary{}, err
		}
		return summary, nil
	}
	if w.dirActive[key] {
		return dirSummary{}, fmt.Errorf("archive: directory subtree %s is cyclic", node)
	}
	if w.dirContains[key] {
		return dirSummary{}, fmt.Errorf("archive: non-empty directory subtree %s is shared at multiple positions", node)
	}

	startPaths := w.budget.paths
	if err := w.budget.addPaths(1); err != nil {
		return dirSummary{}, err
	}
	page, err := w.readDir(ctx, node)
	if err != nil {
		return dirSummary{}, err
	}
	w.dirActive[key] = true
	defer delete(w.dirActive, key)

	fanout := uint64(1) << w.st.params.FanoutBits
	var segmentCount uint64
	if depth == 1 {
		maxKids := entries
		if maxKids > fanout {
			maxKids = fanout
		}
		if uint64(len(page.Kids)) > maxKids {
			return dirSummary{}, fmt.Errorf("archive: leaf directory page %s has %d kids, canonical position allows %d",
				node, len(page.Kids), maxKids)
		}
		for digit, kid := range page.Kids {
			if err := w.budget.check(ctx); err != nil {
				return dirSummary{}, err
			}
			if !kid.Defined() {
				continue
			}
			index := offset + uint64(digit)
			window := w.st.dirBase() + index
			if err := w.validateSegment(ctx, kid, window, windowEnd(window, w.st.params.SegBits), false); err != nil {
				return dirSummary{}, fmt.Errorf("archive: directory leaf %s kid %d: %w", node, digit, err)
			}
			if err := w.budget.addOutput(); err != nil {
				return dirSummary{}, err
			}
			w.out.Sealed = append(w.out.Sealed, SealedSegment{
				CID:       kid,
				Ord:       window,
				FirstSlot: windowStart(window, w.st.params.SegBits),
				LastSlot:  windowEnd(window, w.st.params.SegBits),
			})
			segmentCount++
		}
	} else {
		childCap := capacity(depth-1, w.st.params.FanoutBits)
		expectedKids := uint64(1)
		if childCap < entries {
			expectedKids = 1 + (entries-1)/childCap
		}
		if uint64(len(page.Kids)) != expectedKids {
			return dirSummary{}, fmt.Errorf("archive: internal directory page %s has %d kids, canonical position requires %d",
				node, len(page.Kids), expectedKids)
		}
		for digit, kid := range page.Kids {
			if !kid.Defined() {
				return dirSummary{}, fmt.Errorf("archive: internal directory page %s kid %d is null; every appended range requires a page",
					node, digit)
			}
			d := uint64(digit)
			childOffset := offset + d*childCap
			childEntries := entries - d*childCap
			if childEntries > childCap {
				childEntries = childCap
			}
			summary, err := w.walkDir(ctx, kid, depth-1, childOffset, childEntries)
			if err != nil {
				return dirSummary{}, err
			}
			segmentCount += summary.segments
		}
	}

	summary := dirSummary{
		paths:    w.budget.paths - startPaths,
		segments: segmentCount,
	}
	w.dirMemo[memoKey] = summary
	w.dirContains[key] = segmentCount != 0
	return summary, nil
}

func (w *enumerationWalker) readDir(ctx context.Context, c cid.Cid) (*schema.DirNode, error) {
	key := c.KeyString()
	if page, ok := w.dirNodes[key]; ok {
		return page, nil
	}
	data, err := w.budget.readNode(ctx, w.h, c, "DirNode")
	if err != nil {
		return nil, err
	}
	page, err := schema.DecodeDirNode(data)
	if err != nil {
		return nil, fmt.Errorf("archive: decoding DirNode %s: %w", c, err)
	}
	w.dirNodes[key] = page
	w.out.DirPages = append(w.out.DirPages, c)
	return page, nil
}

type segmentProof struct {
	slot0    uint64
	firstRow uint64
	lastRow  uint64
	rows     uint64
}

func (w *enumerationWalker) validateSegment(ctx context.Context, c cid.Cid, expectedWindow, upper uint64, open bool) error {
	if err := validateIndexCID(c, "Segment"); err != nil {
		return err
	}
	if prior, duplicate := w.segments[c.KeyString()]; duplicate {
		return fmt.Errorf("segment %s is referenced at windows %d and %d", c, prior, expectedWindow)
	}
	w.segments[c.KeyString()] = expectedWindow

	key := c.KeyString()
	cachedProof, cached := w.newProofs[key]
	if !cached {
		cachedProof, cached = w.h.structure.segment(c)
	}
	proof := cachedProof.proof
	if cached {
		// A cached shape proof is also a current presence proof only while it is
		// stamped with the local store's unchanged collection generation. A new
		// generation is allocated before any GC deletion, invalidating every
		// older stamp. The first post-boundary Get re-hashes the block and, when
		// a collection is active, touches it into that epoch's protected set.
		current := w.generationKnown && cachedProof.generationKnown &&
			cachedProof.generation == w.generation
		if current {
			if err := w.budget.addCachedUnique(ctx, c); err != nil {
				return err
			}
		} else {
			if _, err := w.budget.readNode(ctx, w.h, c, "Segment"); err != nil {
				return err
			}
			w.newProofs[key] = cachedSegmentProof{
				proof: proof, generation: w.generation, generationKnown: w.generationKnown,
			}
		}
	} else {
		data, err := w.budget.readNode(ctx, w.h, c, "Segment")
		if err != nil {
			return err
		}
		segment, err := schema.DecodeSegment(data)
		if err != nil {
			return fmt.Errorf("decoding Segment %s: %w", c, err)
		}
		proof, err = segmentShape(segment)
		if err != nil {
			return fmt.Errorf("validating Segment %s links: %w", c, err)
		}
		w.newProofs[key] = cachedSegmentProof{
			proof: proof, generation: w.generation, generationKnown: w.generationKnown,
		}
	}

	wantSlot0 := windowStart(expectedWindow, w.st.params.SegBits)
	if proof.slot0 != wantSlot0 {
		return fmt.Errorf("segment %s has slot0=%d, path position requires %d",
			c, proof.slot0, wantSlot0)
	}
	if !open && proof.rows == 0 {
		return fmt.Errorf("sealed Segment %s is empty; an empty window must be a null directory entry", c)
	}
	lower := wantSlot0
	if lower < w.st.params.OriginSlot {
		lower = w.st.params.OriginSlot
	}
	if proof.rows > 0 && (proof.firstRow < lower || proof.lastRow > upper) {
		return fmt.Errorf("segment %s rows span [%d,%d], path window permits [%d,%d]",
			c, proof.firstRow, proof.lastRow, lower, upper)
	}
	return nil
}

func segmentShape(segment *schema.Segment) (segmentProof, error) {
	proof := segmentProof{slot0: segment.Slot0, rows: uint64(len(segment.Rows))}
	if len(segment.Rows) > 0 {
		proof.firstRow = segment.Rows[0].Slot
		proof.lastRow = segment.Rows[len(segment.Rows)-1].Slot
	}
	for rowIndex, row := range segment.Rows {
		for entryIndex, entry := range row.Entries {
			if err := validateBlobCID(entry.Blob); err != nil {
				return segmentProof{}, fmt.Errorf("row %d entry %d: %w", rowIndex, entryIndex, err)
			}
		}
	}
	return proof, nil
}

func validateIndexCID(c cid.Cid, what string) error {
	if !c.Defined() {
		return fmt.Errorf("archive: %s CID is undefined", what)
	}
	prefix := c.Prefix()
	if prefix.Version != 1 || prefix.Codec != cid.DagCBOR ||
		prefix.MhType != multihash.SHA2_256 || prefix.MhLength != 32 {
		return fmt.Errorf("archive: %s %s has CID prefix v%d/codec=0x%x/multihash=0x%x/%d bytes, "+
			"want CIDv1 dag-cbor sha2-256/32",
			what, c, prefix.Version, prefix.Codec, prefix.MhType, prefix.MhLength)
	}
	return nil
}

func validateBlobCID(c cid.Cid) error {
	if !c.Defined() {
		return errors.New("blob CID is undefined")
	}
	prefix := c.Prefix()
	if prefix.Version != 1 || prefix.Codec != cid.Raw ||
		prefix.MhType != multihash.SHA2_256 || prefix.MhLength != 32 {
		return fmt.Errorf("blob %s has CID prefix v%d/codec=0x%x/multihash=0x%x/%d bytes, "+
			"want CIDv1 raw sha2-256/32",
			c, prefix.Version, prefix.Codec, prefix.MhType, prefix.MhLength)
	}
	return nil
}

func verifyBlockCID(want cid.Cid, data []byte) error {
	got, err := want.Prefix().Sum(data)
	if err != nil {
		return fmt.Errorf("hashing block %s: %w", want, err)
	}
	if got != want {
		return fmt.Errorf("block stored for %s hashes to %s; it is corrupt", want, got)
	}
	return nil
}
