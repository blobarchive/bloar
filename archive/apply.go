package archive

import (
	"context"
	"errors"
	"fmt"

	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"

	"github.com/blobarchive/bloar/core"
	"github.com/blobarchive/bloar/schema"
)

// RefRow is one blob-carrying slot of a batch. VHs is order-significant: it
// becomes the row's entry order, which is part of the content and therefore of
// the CID (spec 3.2). A slot with no blobs has no RefRow.
type RefRow struct {
	Slot uint64
	VHs  []schema.VersionedHash
}

// ApplyResult reports the outcome of ApplyRefs, matching the fields spec 7.2
// returns. NoOp is true for an accepted idempotent replay, which changes
// nothing.
type ApplyResult struct {
	SyncedTo uint64
	Root     cid.Cid
	NoOp     bool
	Index    IndexApplyStats
}

// ApplyRefs adds rows and advances coverage to newSyncedTo (spec 5.1).
//
// rows must be strictly ascending by slot, every slot within
// [origin_slot, newSyncedTo]. rows may be empty, which advances coverage
// through slots that carry no blobs.
//
// A batch that lies entirely at or before synced_to is an idempotent replay: it
// is verified against what is stored and, if it matches, accepted as a no-op.
// Anything else that touches covered ground is refused. Every failure except an
// I/O error is a *ConflictError, which spec 7.2 maps to 409.
func (h *Head) ApplyRefs(ctx context.Context, rows []RefRow, newSyncedTo uint64) (ApplyResult, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	st := h.cur.Load()
	k := st.params.SegBits

	// 5.1 step 1: shape.
	if err := validateShape(st, rows, newSyncedTo); err != nil {
		return ApplyResult{}, err
	}

	// 5.1 step 2: idempotent replay. Coverage does not advance and no row is
	// new, so the batch either matches what is stored or contradicts it.
	if st.covered && newSyncedTo <= st.syncedTo && lastSlot(rows) <= st.syncedTo {
		if err := h.verifyReplay(ctx, st, rows); err != nil {
			return ApplyResult{}, err
		}
		return ApplyResult{SyncedTo: st.syncedTo, Root: st.root, NoOp: true}, nil
	}

	// 5.1 step 3: no partial overlap. Past here coverage advances, so every row
	// must be new: sealed segments are immutable and a batch that half-replays
	// is a writer that lost track of its own progress.
	if st.covered {
		for i, r := range rows {
			if r.Slot <= st.syncedTo {
				return ApplyResult{}, conflictf(
					"row %d (slot %d) is at or before synced_to %d while the batch advances coverage to %d; a batch may replay or extend, not both",
					i, r.Slot, st.syncedTo, newSyncedTo)
			}
		}
	}

	// 5.1 step 4: every vh resolves and its block is present.
	resolved, err := h.resolveRows(ctx, rows)
	if err != nil {
		return ApplyResult{}, err
	}
	stats := IndexApplyStats{}
	h.applyStats = &stats
	defer func() { h.applyStats = nil }()

	next := *st
	next.root = cid.Undef

	// The open segment is always the window ord(synced_to+1) -- an unsealed
	// window is exactly one that synced_to has not passed the end of -- so it is
	// already the window this batch starts in.
	startSlot := st.params.OriginSlot
	if st.covered {
		startSlot = st.syncedTo + 1
	}
	w0, w1 := ord(startSlot, k), ord(newSyncedTo, k)

	var open *core.Pointer[schema.Segment]
	if st.covered {
		openW, err := h.openOrd(ctx, st)
		if err != nil {
			return ApplyResult{}, err
		}
		if openW != w0 {
			return ApplyResult{}, fmt.Errorf("archive: internal: open segment is window %d but coverage resumes in window %d", openW, w0)
		}
		open = h.segs.Pointer(st.open)
	} else {
		open = h.segs.NewNode(&schema.Segment{Slot0: windowStart(w0, k)})
	}

	ri := 0
	for w := w0; w <= w1; w++ {
		var batch []schema.Row
		for ri < len(resolved) && ord(resolved[ri].Slot, k) == w {
			batch = append(batch, resolved[ri])
			ri++
		}
		if len(batch) > 0 {
			seg, err := open.Mutate(ctx)
			if err != nil {
				return ApplyResult{}, fmt.Errorf("archive: opening segment for window %d: %w", w, err)
			}
			seg.Rows = append(seg.Rows, batch...)
		}
		if windowEnd(w, k) <= newSyncedTo {
			if open, err = h.seal(ctx, &next, open, w); err != nil {
				return ApplyResult{}, err
			}
		}
	}
	if ri != len(resolved) {
		return ApplyResult{}, fmt.Errorf("archive: internal: %d rows fell outside windows [%d, %d]", len(resolved)-ri, w0, w1)
	}

	if next.open, err = h.commitSegment(ctx, open, SegmentOpen); err != nil {
		return ApplyResult{}, fmt.Errorf("archive: writing open segment: %w", err)
	}
	next.syncedTo, next.covered = newSyncedTo, true
	if err := h.publish(ctx, &next); err != nil {
		return ApplyResult{}, err
	}
	stats.Segments = append([]SegmentSample(nil), stats.Segments...)
	h.lastApplyStats = stats
	return ApplyResult{SyncedTo: next.syncedTo, Root: next.root, Index: stats}, nil
}

// seal closes window w: the open segment becomes a directory entry and a fresh
// empty segment opens over window w+1 (spec 5.2). It returns the new open
// pointer and updates st's directory in place.
//
// st here is the mutation's private next-state, never a published one.
func (h *Head) seal(ctx context.Context, st *state, open *core.Pointer[schema.Segment], w uint64) (*core.Pointer[schema.Segment], error) {
	seg, err := open.Load(ctx)
	if err != nil {
		return nil, fmt.Errorf("archive: reading segment for window %d: %w", w, err)
	}

	// An empty window seals to no object at all, a null entry (spec 3.2). The
	// null still occupies an index: the directory is addressed by arithmetic,
	// so every window must take its turn whether or not it has refs.
	var sealed cid.Cid
	if len(seg.Rows) > 0 {
		if sealed, err = h.commitSegment(ctx, open, SegmentSealed); err != nil {
			return nil, fmt.Errorf("archive: sealing segment for window %d: %w", w, err)
		}
	}

	st.dir, st.dirDepth, err = h.dirAppend(ctx, st.dir, st.dirDepth, st.params.FanoutBits, w-st.dirBase(), sealed)
	if err != nil {
		return nil, err
	}
	return h.segs.NewNode(&schema.Segment{Slot0: windowStart(w+1, st.params.SegBits)}), nil
}

// commitSegment commits a dirty Segment exactly once and records the byte
// length produced by that encode. A clean pointer at seal time is the immutable
// block written by an earlier apply; its bytes are read from the blockstore
// rather than re-encoded, preserving the seal-as-link invariant.
func (h *Head) commitSegment(ctx context.Context, segment *core.Pointer[schema.Segment], state SegmentState) (cid.Cid, error) {
	value, err := segment.Load(ctx)
	if err != nil {
		return cid.Undef, err
	}
	dirty := segment.IsDirty()
	h.segmentEncodeState = state
	h.segmentEncodedBytes = 0
	id, err := segment.Commit(ctx)
	h.segmentEncodeState = ""
	if err != nil {
		return cid.Undef, err
	}
	encodedBytes := h.segmentEncodedBytes
	if !dirty {
		block, err := h.cfg.Blocks.Get(ctx, id)
		if err != nil {
			return cid.Undef, fmt.Errorf("archive: reading committed segment %s for exact size: %w", id, err)
		}
		encodedBytes = len(block.RawData())
	}
	refs := 0
	for _, row := range value.Rows {
		refs += len(row.Entries)
	}
	if h.applyStats != nil {
		h.applyStats.Segments = append(h.applyStats.Segments, SegmentSample{
			State: state, EncodedBytes: encodedBytes, Rows: len(value.Rows), Refs: refs,
		})
	}
	return id, nil
}

// LastApplyStats returns an independent copy of the most recent successful
// ApplyRefs measurement. Complete-generation builders use it after their
// durable selector commit; ordinary callers receive the same data in
// ApplyResult.
func (h *Head) LastApplyStats() IndexApplyStats {
	h.mu.Lock()
	defer h.mu.Unlock()
	stats := h.lastApplyStats
	stats.Segments = append([]SegmentSample(nil), stats.Segments...)
	return stats
}

// OpenSegmentSample reads the current immutable open Segment and returns its
// exact stored bytes and density. It is used to initialize metrics from a
// durable root after restart without encoding or mutating anything.
func (h *Head) OpenSegmentSample(ctx context.Context) (SegmentSample, bool, error) {
	st := h.cur.Load()
	if !st.covered {
		return SegmentSample{}, false, nil
	}
	_, sample, err := h.readSegmentSample(ctx, st.open, SegmentOpen)
	return sample, err == nil, err
}

// MaxLatestSealedSampleDirNodes bounds the directory-aware reverse search used
// to restore the last-sealed metric at startup.
const MaxLatestSealedSampleDirNodes = 1024

// LatestSealedSegmentSample returns the nearest non-null sealed Segment behind
// the current open window. It searches the radix tree from its right edge,
// skipping null child ranges without scanning their window ordinals.
func (h *Head) LatestSealedSegmentSample(ctx context.Context) (SegmentSample, bool, error) {
	st := h.cur.Load()
	if !st.covered || st.dirDepth == 0 {
		return SegmentSample{}, false, nil
	}
	openW, err := h.openOrd(ctx, st)
	if err != nil {
		return SegmentSample{}, false, err
	}
	base := st.dirBase()
	sealed := openW - base
	budget := MaxLatestSealedSampleDirNodes
	id, index, ok, err := h.latestSealedInDir(ctx, st.dir, st.dirDepth, st.params.FanoutBits, sealed, &budget)
	if err != nil || !ok {
		return SegmentSample{}, false, err
	}
	segment, sample, err := h.readSegmentSample(ctx, id, SegmentSealed)
	if err != nil {
		return SegmentSample{}, false, err
	}
	wantSlot0 := windowStart(base+index, st.params.SegBits)
	if segment.Slot0 != wantSlot0 || len(segment.Rows) == 0 {
		return SegmentSample{}, false, fmt.Errorf(
			"archive: latest sealed segment %s has slot0=%d rows=%d, path requires non-empty slot0=%d",
			id, segment.Slot0, len(segment.Rows), wantSlot0)
	}
	return sample, true, nil
}

// readSegmentSample performs one validating block read. The same raw bytes are
// both the decode input and the measurement source, so startup cannot drift
// from the writer's canonical block length or decode an over-budget node.
func (h *Head) readSegmentSample(
	ctx context.Context,
	id cid.Cid,
	state SegmentState,
) (*schema.Segment, SegmentSample, error) {
	block, err := h.cfg.Blocks.Get(ctx, id)
	if err != nil {
		return nil, SegmentSample{}, fmt.Errorf("archive: reading %s segment %s: %w", state, id, err)
	}
	data := block.RawData()
	if len(data) > MaxIndexNodeBytes {
		return nil, SegmentSample{}, fmt.Errorf(
			"archive: %s segment %s is %d encoded bytes, above the %d-byte per-node admission budget",
			state, id, len(data), MaxIndexNodeBytes)
	}
	segment, err := schema.DecodeSegment(data)
	if err != nil {
		return nil, SegmentSample{}, fmt.Errorf("archive: decoding %s segment %s: %w", state, id, err)
	}
	refs := 0
	for _, row := range segment.Rows {
		refs += len(row.Entries)
	}
	return segment, SegmentSample{
		State: state, EncodedBytes: len(data), Rows: len(segment.Rows), Refs: refs,
	}, nil
}

// latestSealedInDir returns the greatest defined leaf index below limit. Empty
// subtrees are skipped by page links, so a long run of null windows costs no
// per-window reads. budget bounds even a malformed or extraordinarily sparse
// tree by logical DirNode visits.
func (h *Head) latestSealedInDir(
	ctx context.Context,
	node cid.Cid,
	depth, fanoutBits, limit uint64,
	budget *int,
) (cid.Cid, uint64, bool, error) {
	if !node.Defined() || depth == 0 || limit == 0 {
		return cid.Undef, 0, false, nil
	}
	if *budget == 0 {
		return cid.Undef, 0, false, fmt.Errorf(
			"archive: latest sealed Segment search exceeded %d DirNode visits",
			MaxLatestSealedSampleDirNodes)
	}
	*budget = *budget - 1
	page, err := h.dirs.GetNode(ctx, node)
	if err != nil {
		return cid.Undef, 0, false, fmt.Errorf("archive: reading dirnode %s: %w", node, err)
	}
	if len(page.Kids) == 0 {
		return cid.Undef, 0, false, nil
	}

	childCapacity := capacity(depth-1, fanoutBits)
	highest := (limit - 1) / childCapacity
	if highest >= uint64(len(page.Kids)) {
		highest = uint64(len(page.Kids) - 1)
	}
	for child := highest + 1; child > 0; {
		child--
		id := page.Kids[child]
		if !id.Defined() {
			continue
		}
		base := child * childCapacity
		childLimit := childCapacity
		if child == highest && limit-base < childLimit {
			childLimit = limit - base
		}
		if depth == 1 {
			return id, child, true, nil
		}
		found, offset, ok, err := h.latestSealedInDir(ctx, id, depth-1, fanoutBits, childLimit, budget)
		if err != nil {
			return cid.Undef, 0, false, err
		}
		if ok {
			return found, base + offset, true, nil
		}
	}
	return cid.Undef, 0, false, nil
}

// validateShape enforces spec 5.1 step 1.
func validateShape(st *state, rows []RefRow, newSyncedTo uint64) error {
	if newSyncedTo < st.params.OriginSlot {
		// Not spelled out in 5.1, but synced_to < origin_slot is not a state a
		// Head can even encode (schema 3.1).
		return conflictf("synced_to %d precedes origin_slot %d", newSyncedTo, st.params.OriginSlot)
	}
	for i, r := range rows {
		if i > 0 && r.Slot <= rows[i-1].Slot {
			return conflictf("rows are not strictly ascending: row %d (slot %d) follows slot %d", i, r.Slot, rows[i-1].Slot)
		}
		if r.Slot < st.params.OriginSlot {
			return conflictf("row %d (slot %d) precedes origin_slot %d", i, r.Slot, st.params.OriginSlot)
		}
		// Also enforces "new_synced_to >= last row slot".
		if r.Slot > newSyncedTo {
			return conflictf("row %d (slot %d) is past synced_to %d", i, r.Slot, newSyncedTo)
		}
		if len(r.VHs) == 0 {
			return conflictf("row %d (slot %d) has no versioned hashes; a slot with no blobs has no row", i, r.Slot)
		}
		// The mutation-time half of the safety boundary's read-path bound: no stored
		// row may exceed the protocol ceiling, so an unfiltered read of the slot
		// this row commits is bounded by construction. schema.Segment.Validate
		// enforces the same on encode/decode; catching it here gives the writer a
		// clean 409 with the offending slot before any block is written.
		if len(r.VHs) > schema.MaxBlobsPerSlotCeiling {
			return conflictf("row %d (slot %d) has %d versioned hashes, more than the %d-blob ceiling per slot",
				i, r.Slot, len(r.VHs), schema.MaxBlobsPerSlotCeiling)
		}
	}
	return nil
}

// lastSlot returns the highest row slot, or 0 for an empty batch. An empty
// batch replays vacuously, which is what 0 achieves at every call site.
func lastSlot(rows []RefRow) uint64 {
	if len(rows) == 0 {
		return 0
	}
	return rows[len(rows)-1].Slot
}

// verifyReplay checks each row against the stored row at its slot: same vhs,
// same order (spec 5.1 step 2).
func (h *Head) verifyReplay(ctx context.Context, st *state, rows []RefRow) error {
	for _, r := range rows {
		res, stored, err := h.lookupRow(ctx, st, r.Slot)
		if err != nil {
			return err
		}
		if res.Status != StatusFound || stored == nil {
			return conflictf("replayed row for slot %d contradicts the head: that slot is covered and carries no blobs", r.Slot)
		}
		if len(stored.Entries) != len(r.VHs) {
			return conflictf("replayed row for slot %d has %d versioned hashes, the stored row has %d",
				r.Slot, len(r.VHs), len(stored.Entries))
		}
		for i := range r.VHs {
			if stored.Entries[i].VH != r.VHs[i] {
				return conflictf("replayed row for slot %d differs at entry %d: got 0x%x, stored 0x%x",
					r.Slot, i, r.VHs[i][:], stored.Entries[i].VH[:])
			}
		}
	}
	return nil
}

// resolveRows turns each vh into its blob link (spec 5.1 step 4), reporting
// every unavailable vh at once rather than the first: a writer fixing a batch
// wants the whole list.
func (h *Head) resolveRows(ctx context.Context, rows []RefRow) ([]schema.Row, error) {
	if h.cfg.Resolver == nil {
		return nil, errors.New("archive: Config.Resolver is nil; this head cannot apply refs")
	}
	var missing []schema.VersionedHash
	out := make([]schema.Row, 0, len(rows))
	for _, r := range rows {
		entries := make([]schema.RefEntry, 0, len(r.VHs))
		for _, vh := range r.VHs {
			c, ok, err := h.cfg.Resolver.ResolveBlob(ctx, vh)
			if err != nil {
				return nil, fmt.Errorf("archive: resolving blob 0x%x: %w", vh[:], err)
			}
			if !ok || !c.Defined() {
				missing = append(missing, vh)
				continue
			}
			// The catalog outlives its blocks -- GC does not update it (spec 6.1) --
			// so a catalog hit is not proof the blob is here, and a key being present
			// is not proof its bytes are. Read the block through the
			// validating store rather than probing Has: a NotFound is a genuinely
			// missing blob the writer must re-put, gathered with the rest into one
			// 409; anything else -- a block whose stored bytes no longer hash to its
			// CID, an I/O error -- fails the whole batch before any coverage advances,
			// because committing a ref to a blob this archive cannot serve is exactly
			// the false success the safety boundary is about. The read pays the blob's full hash,
			// which is the price of not trusting Has (see docs/operations.md's
			// minimum-requirements note).
			if _, err := h.cfg.Blocks.Get(ctx, c); err != nil {
				if format.IsNotFound(err) {
					missing = append(missing, vh)
					continue
				}
				return nil, fmt.Errorf("archive: validating blob block %s for vh 0x%x: %w", c, vh[:], err)
			}
			entries = append(entries, schema.RefEntry{VH: vh, Blob: c})
		}
		out = append(out, schema.Row{Slot: r.Slot, Entries: entries})
	}
	if len(missing) > 0 {
		return nil, &ConflictError{Reason: "batch references blobs the archive does not hold", Err: &MissingBlobsError{VHs: missing}}
	}
	return out, nil
}
