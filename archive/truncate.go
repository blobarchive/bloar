package archive

import (
	"context"
	"fmt"

	"github.com/ipfs/go-cid"

	"github.com/blobarchive/bloar/schema"
)

// Truncate restores the head to coverage [origin_slot, slot] and returns the
// new root (spec 5.4). It is an emergency admin operation.
//
// Truncation past synced_to is refused: the head would claim coverage it never
// had. To reset a head entirely, use TruncateToEmpty.
func (h *Head) Truncate(ctx context.Context, slot uint64) (cid.Cid, error) {
	return h.truncate(ctx, slot, 0, false)
}

// TruncateRetainingWindow is Truncate with the additional publication barrier
// required by a writer whose pin policy retains a trailing window. Rewinding a
// window can make older sealed Segments recursive again; this method confirms
// and application-touches the blobs in exactly those newly retained Segments
// before the new root becomes visible. windowSlots is the policy duration
// converted to slots and may be zero (only the open Segment is retained).
func (h *Head) TruncateRetainingWindow(ctx context.Context, slot, windowSlots uint64) (cid.Cid, error) {
	return h.truncate(ctx, slot, windowSlots, true)
}

func (h *Head) truncate(ctx context.Context, slot, windowSlots uint64, retainWindow bool) (cid.Cid, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	st := h.cur.Load()
	k := st.params.SegBits

	if !st.covered {
		return cid.Undef, conflictf("cannot truncate to slot %d: the head is empty", slot)
	}
	if slot > st.syncedTo {
		return cid.Undef, conflictf("cannot truncate to slot %d: it is past synced_to %d", slot, st.syncedTo)
	}
	if slot < st.params.OriginSlot {
		return cid.Undef, conflictf("cannot truncate to slot %d: it precedes origin_slot %d; reset the head with TruncateToEmpty",
			slot, st.params.OriginSlot)
	}

	t := ord(slot, k)
	openW, err := h.openOrd(ctx, st)
	if err != nil {
		return cid.Undef, err
	}

	// The surviving half of window t becomes the new open segment: its rows,
	// wherever they live now, filtered to <= slot.
	segCID, ok, err := h.segmentFor(ctx, st, t)
	if err != nil {
		return cid.Undef, err
	}
	var rows []schema.Row
	if ok {
		seg, err := h.segs.GetNode(ctx, segCID)
		if err != nil {
			return cid.Undef, fmt.Errorf("archive: reading segment %s: %w", segCID, err)
		}
		rows = copyRowsUpTo(seg.Rows, slot)
		if retainWindow {
			// The rebuilt Segment is recursive under a window policy. Re-touch
			// every surviving blob at the store boundary before publishing it;
			// ModeNone deliberately permits those blobs to be absent, while a full
			// policy already had them in the old root's cut-time closure.
			if err := h.touchTruncatedBlobs(ctx, rows); err != nil {
				return cid.Undef, err
			}
		}
	}

	next := *st
	next.root = cid.Undef

	// The directory keeps entries [0, idx(slot)): everything past window t, and
	// window t itself, whose remainder is now the open segment.
	switch n := t - st.dirBase(); {
	case t == openW:
		// Nothing was sealed past window t; the directory is untouched.
	case n == 0:
		next.dir, next.dirDepth = cid.Undef, 0
	default:
		digits, ok := pathDigits(n, st.dirDepth, st.params.FanoutBits)
		if !ok {
			return cid.Undef, fmt.Errorf("archive: internal: sealed count %d does not fit a depth-%d directory", n, st.dirDepth)
		}
		if next.dir, err = h.dirTruncate(ctx, st.dir, digits); err != nil {
			return cid.Undef, err
		}
		if next.dir, next.dirDepth, err = h.dirShrink(ctx, next.dir, st.dirDepth); err != nil {
			return cid.Undef, err
		}
	}

	open := h.segs.NewNode(&schema.Segment{Slot0: windowStart(t, k), Rows: rows})

	// Spec 5.4 hands window t to the open segment unconditionally, but if slot
	// is the last slot of its window then window t is fully covered, and spec
	// 5.1 requires a fully covered window to be sealed -- the open segment is
	// always the window ord(synced_to+1), which is t+1 in that case, not t.
	// Leaving it open would strand window t's rows outside the directory, since
	// the next apply resumes at t+1 and never revisits t, and would leave the
	// truncated head structurally different from a fresh build of the same
	// data. So the seal rule runs over the rebuilt window exactly as apply
	// would.
	if windowEnd(t, k) <= slot {
		if open, err = h.seal(ctx, &next, open, t); err != nil {
			return cid.Undef, err
		}
	}

	if next.open, err = open.Commit(ctx); err != nil {
		return cid.Undef, fmt.Errorf("archive: writing open segment: %w", err)
	}
	next.syncedTo, next.covered = slot, true
	if retainWindow {
		if err := h.touchNewlyRetainedWindow(ctx, st, &next, windowSlots); err != nil {
			return cid.Undef, err
		}
	}
	if err := h.publish(ctx, &next); err != nil {
		return cid.Undef, err
	}
	return next.root, nil
}

// touchNewlyRetainedWindow protects the sealed Segment closures which a
// backwards root transition adds to a trailing-window policy. Segments already
// recursive under old were in the collector's cut-time mark set; the rebuilt
// target Segment was touched above. Only ordinals below the old window's first
// recursive Segment can be newly live, so an emergency rewind pays for the
// amount by which it expands retention rather than for the whole archive.
func (h *Head) touchNewlyRetainedWindow(ctx context.Context, old, next *state, windowSlots uint64) error {
	oldFirst := firstRetainedWindow(old, windowSlots)
	newFirst := firstRetainedWindow(next, windowSlots)
	if newFirst >= oldFirst || oldFirst == 0 {
		return nil
	}
	last := oldFirst - 1
	if end := ord(next.syncedTo, next.params.SegBits); last > end {
		last = end
	}
	if newFirst > last {
		return nil
	}

	seen := make(map[string]struct{})
	for w := newFirst; ; w++ {
		segmentCID, ok, err := h.segmentFor(ctx, next, w)
		if err != nil {
			return err
		}
		if ok {
			segment, err := h.segs.GetNode(ctx, segmentCID)
			if err != nil {
				return fmt.Errorf("archive: reading newly retained segment %s: %w", segmentCID, err)
			}
			for _, row := range segment.Rows {
				for _, entry := range row.Entries {
					blob := entry.Blob
					if !blob.Defined() || blob.Prefix().Codec != cid.Raw {
						return fmt.Errorf("archive: newly retained blob link %s from slot %d is not a defined raw CID; refusing to publish the truncated head",
							blob, row.Slot)
					}
					key := string(blob.Hash())
					if _, duplicate := seen[key]; duplicate {
						continue
					}
					seen[key] = struct{}{}
					if _, err := h.cfg.Blocks.Get(ctx, blob); err != nil {
						return fmt.Errorf("archive: validating newly retained blob %s from slot %d; refusing to publish the truncated window: %w",
							blob, row.Slot, err)
					}
				}
			}
		}
		if w == last {
			break
		}
	}
	return nil
}

func firstRetainedWindow(st *state, windowSlots uint64) uint64 {
	low := uint64(0)
	if windowSlots < st.syncedTo {
		low = st.syncedTo - windowSlots
	}
	if low < st.params.OriginSlot {
		low = st.params.OriginSlot
	}
	return ord(low, st.params.SegBits)
}

// touchTruncatedBlobs closes the publication window opened when Truncate copies
// old rows into a newly-built Segment. The blockstore handed to archive is the
// application view; during online GC its Get method validates the bytes and
// records the CID in the active epoch's protected set.
func (h *Head) touchTruncatedBlobs(ctx context.Context, rows []schema.Row) error {
	seen := make(map[string]struct{})
	for _, row := range rows {
		for _, entry := range row.Entries {
			blob := entry.Blob
			if !blob.Defined() {
				continue
			}
			if blob.Prefix().Codec != cid.Raw {
				return fmt.Errorf("archive: surviving blob link %s from slot %d has codec 0x%x, want raw; refusing to publish the truncated head",
					blob, row.Slot, blob.Prefix().Codec)
			}
			key := string(blob.Hash())
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}

			_, err := h.cfg.Blocks.Get(ctx, blob)
			if err != nil {
				return fmt.Errorf("archive: validating surviving blob %s from slot %d; refusing to publish the truncated head: %w",
					blob, row.Slot, err)
			}
		}
	}
	return nil
}

// TruncateToEmpty resets the head to cover nothing (spec 5.4's "empty"). The
// result is byte-identical to a head that has never been applied to, so the
// root returns to what New wrote.
func (h *Head) TruncateToEmpty(ctx context.Context) (cid.Cid, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	next := state{params: h.cur.Load().params}
	if err := h.publish(ctx, &next); err != nil {
		return cid.Undef, err
	}
	return next.root, nil
}

// copyRowsUpTo returns the rows at or before maxSlot, deep-copied: the source
// rows belong to the decoded-node cache, and the copy is about to be owned by a
// segment pointer.
func copyRowsUpTo(rows []schema.Row, maxSlot uint64) []schema.Row {
	out := make([]schema.Row, 0, len(rows))
	for _, r := range rows {
		if r.Slot > maxSlot {
			break // rows are ascending (schema 3.2)
		}
		out = append(out, schema.Row{Slot: r.Slot, Entries: append([]schema.RefEntry(nil), r.Entries...)})
	}
	return out
}
