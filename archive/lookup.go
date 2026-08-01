package archive

import (
	"context"
	"fmt"
	"sort"

	"github.com/ipfs/go-cid"

	"github.com/blobarchive/bloar/schema"
)

// Status is the outcome of a lookup. The three non-Found outcomes are distinct
// facts about the head, not shades of "no": the server maps them to different
// responses (spec 7.1) and only one of them is worth retrying.
type Status int

const (
	// StatusFound: the slot is covered and carries the blobs in Result.Entries.
	StatusFound Status = iota
	// StatusBeforeOrigin: the slot precedes origin_slot. This head will never
	// cover it. Spec 7.1: 404.
	StatusBeforeOrigin
	// StatusNotYetCovered: the slot is past synced_to, or the head is empty.
	// Not archived yet; ask again later. Spec 7.1: 503 + Retry-After.
	StatusNotYetCovered
	// StatusAbsent: the slot is covered and provably carries no such blob --
	// either no row at all, or a row without the requested vh. Spec 7.1: 404.
	StatusAbsent
)

func (s Status) String() string {
	switch s {
	case StatusFound:
		return "found"
	case StatusBeforeOrigin:
		return "before_origin"
	case StatusNotYetCovered:
		return "not_yet_covered"
	case StatusAbsent:
		return "absent"
	default:
		return fmt.Sprintf("Status(%d)", int(s))
	}
}

// Result is a lookup outcome. Entries is set only when Status is StatusFound,
// and carries both the versioned hash and the blob CID of each match, in order:
// stored (canonical) order for an unfiltered lookup, request order for a
// filtered one (spec 7.1).
type Result struct {
	Status  Status
	Slot    uint64
	Entries []schema.RefEntry

	// MissingVH is the first requested vh that the covered slot does not
	// carry, set only when a filtered lookup returns StatusAbsent. Spec 7.1
	// wants it named in the 404 message.
	MissingVH *schema.VersionedHash
}

// Lookup returns every blob at slot, in stored order. A covered slot with no
// blobs is StatusFound with no entries, not StatusAbsent: the archive knows the
// slot carried nothing, which is a different fact from "you asked for a blob
// that is not there" and spec 7.1 answers it with 200 {"data": []}.
func (h *Head) Lookup(ctx context.Context, slot uint64) (Result, error) {
	st := h.cur.Load()
	res, row, err := h.lookupRow(ctx, st, slot)
	if err != nil || res.Status != StatusFound {
		return res, err
	}
	if row != nil {
		res.Entries = append([]schema.RefEntry(nil), row.Entries...)
	}
	return res, nil
}

// LookupVHs returns exactly one blob per requested vh, in request order. If any
// requested vh is not at the slot, the result is StatusAbsent naming it: spec
// 7.1 fails the whole request rather than answering part of it, because a
// caller that asked for N blobs cannot use N-1.
func (h *Head) LookupVHs(ctx context.Context, slot uint64, vhs []schema.VersionedHash) (Result, error) {
	st := h.cur.Load()
	res, row, err := h.lookupRow(ctx, st, slot)
	if err != nil || res.Status != StatusFound {
		return res, err
	}

	entries := make([]schema.RefEntry, 0, len(vhs))
	for i := range vhs {
		e, ok := findVH(row, vhs[i])
		if !ok {
			missing := vhs[i]
			return Result{Status: StatusAbsent, Slot: slot, MissingVH: &missing}, nil
		}
		entries = append(entries, e)
	}
	res.Entries = entries
	return res, nil
}

// findVH returns the row's entry for vh. row may be nil (a covered slot with no
// blobs), which carries nothing.
func findVH(row *schema.Row, vh schema.VersionedHash) (schema.RefEntry, bool) {
	if row == nil {
		return schema.RefEntry{}, false
	}
	for _, e := range row.Entries {
		if e.VH == vh {
			return e, true
		}
	}
	return schema.RefEntry{}, false
}

// lookupRow walks st to the row at slot, per spec 4. It returns StatusFound
// with a nil row for a covered slot that carries no blobs.
func (h *Head) lookupRow(ctx context.Context, st *state, slot uint64) (Result, *schema.Row, error) {
	// Origin is checked before coverage, which reverses the order of the spec 4
	// pseudocode. The two disagree only for a slot below origin_slot on an
	// empty head, where the spec's order answers NOT_YET_COVERED and so 503 +
	// Retry-After (spec 7.1) for a slot this head is defined never to cover.
	// Coverage is a moving fact; preceding origin_slot is a permanent one, and
	// spec 7.1 gives it its own permanent answer: "404 if slot < origin_slot".
	if slot < st.params.OriginSlot {
		return Result{Status: StatusBeforeOrigin, Slot: slot}, nil, nil
	}
	if !st.covered || slot > st.syncedTo {
		return Result{Status: StatusNotYetCovered, Slot: slot}, nil, nil
	}

	segCID, ok, err := h.segmentFor(ctx, st, ord(slot, st.params.SegBits))
	if err != nil {
		return Result{}, nil, err
	}
	if !ok {
		// A null on the route means no refs anywhere in that range, which for a
		// covered slot is proof it carries nothing.
		return Result{Status: StatusFound, Slot: slot}, nil, nil
	}
	seg, err := h.segs.GetNode(ctx, segCID)
	if err != nil {
		return Result{}, nil, fmt.Errorf("archive: reading segment %s: %w", segCID, err)
	}
	row := searchRows(seg.Rows, slot)
	return Result{Status: StatusFound, Slot: slot}, row, nil
}

// segmentFor returns the segment holding window w: the open segment if w is the
// open window, else the sealed segment the directory addresses. ok is false
// when the window has no segment at all (a null entry, or a null anywhere on
// the route).
func (h *Head) segmentFor(ctx context.Context, st *state, w uint64) (cid.Cid, bool, error) {
	if !st.covered {
		return cid.Undef, false, nil
	}
	openW, err := h.openOrd(ctx, st)
	if err != nil {
		return cid.Undef, false, err
	}
	if w == openW {
		return st.open, true, nil
	}
	return h.sealedSegment(ctx, st, w)
}

// sealedSegment walks the directory to the sealed segment for window w (spec 4).
func (h *Head) sealedSegment(ctx context.Context, st *state, w uint64) (cid.Cid, bool, error) {
	base := st.dirBase()
	if st.dirDepth == 0 || w < base {
		return cid.Undef, false, nil
	}
	digits, ok := pathDigits(w-base, st.dirDepth, st.params.FanoutBits)
	if !ok {
		// The index does not fit the tree, so nothing was ever appended there.
		return cid.Undef, false, nil
	}

	cur := st.dir
	for _, d := range digits {
		node, err := h.dirs.GetNode(ctx, cur)
		if err != nil {
			return cid.Undef, false, fmt.Errorf("archive: reading dirnode %s: %w", cur, err)
		}
		// Out of range is null: pages omit their trailing nulls (spec 3.3).
		if d >= uint64(len(node.Kids)) {
			return cid.Undef, false, nil
		}
		kid := node.Kids[d]
		if !kid.Defined() {
			return cid.Undef, false, nil
		}
		cur = kid
	}
	return cur, true, nil
}

// searchRows binary-searches rows (ascending, no duplicate slots) for slot.
func searchRows(rows []schema.Row, slot uint64) *schema.Row {
	i := sort.Search(len(rows), func(i int) bool { return rows[i].Slot >= slot })
	if i < len(rows) && rows[i].Slot == slot {
		return &rows[i]
	}
	return nil
}
