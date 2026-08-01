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

	if next.open, err = open.Commit(ctx); err != nil {
		return ApplyResult{}, fmt.Errorf("archive: writing open segment: %w", err)
	}
	next.syncedTo, next.covered = newSyncedTo, true
	if err := h.publish(ctx, &next); err != nil {
		return ApplyResult{}, err
	}
	return ApplyResult{SyncedTo: next.syncedTo, Root: next.root}, nil
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
		if sealed, err = open.Commit(ctx); err != nil {
			return nil, fmt.Errorf("archive: sealing segment for window %d: %w", w, err)
		}
	}

	st.dir, st.dirDepth, err = h.dirAppend(ctx, st.dir, st.dirDepth, st.params.FanoutBits, w-st.dirBase(), sealed)
	if err != nil {
		return nil, err
	}
	return h.segs.NewNode(&schema.Segment{Slot0: windowStart(w+1, st.params.SegBits)}), nil
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
