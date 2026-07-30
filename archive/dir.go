package archive

import (
	"context"
	"fmt"

	"github.com/ipfs/go-cid"

	"github.com/blobarchive/bloar/schema"
)

// dirAppend appends kid as directory entry i and returns the new directory root
// and depth (spec 5.3). kid may be undefined: a fully-empty window seals to a
// null entry, not to an empty Segment.
//
// i is always the current sealed count. Anything else is an internal error: the
// directory is an append-only vector addressed by arithmetic, and a gap or an
// overwrite would silently misfile every later entry.
func (h *Head) dirAppend(ctx context.Context, dir cid.Cid, depth, fanoutBits, i uint64, kid cid.Cid) (cid.Cid, uint64, error) {
	if depth == 0 {
		if i != 0 {
			return cid.Undef, 0, fmt.Errorf("archive: internal: first directory append must be index 0, got %d", i)
		}
		c, err := h.dirs.NewNode(&schema.DirNode{Kids: []cid.Cid{kid}}).Commit(ctx)
		if err != nil {
			return cid.Undef, 0, fmt.Errorf("archive: writing first dirnode: %w", err)
		}
		return c, 1, nil
	}

	switch full := capacity(depth, fanoutBits); {
	case i == full:
		// The root is full. Wrap it in a new single-kid root: every existing
		// entry keeps its index, because prefixing a zero digit to every path
		// is what deepening the tree means.
		c, err := h.dirs.NewNode(&schema.DirNode{Kids: []cid.Cid{dir}}).Commit(ctx)
		if err != nil {
			return cid.Undef, 0, fmt.Errorf("archive: growing directory to depth %d: %w", depth+1, err)
		}
		dir, depth = c, depth+1
	case i > full:
		return cid.Undef, 0, fmt.Errorf("archive: internal: directory append of index %d past capacity %d at depth %d; appends must be contiguous",
			i, full, depth)
	}

	digits, ok := pathDigits(i, depth, fanoutBits)
	if !ok {
		return cid.Undef, 0, fmt.Errorf("archive: internal: index %d does not fit a depth-%d directory", i, depth)
	}
	newDir, err := h.dirSet(ctx, dir, digits, kid)
	if err != nil {
		return cid.Undef, 0, err
	}
	return newDir, depth, nil
}

// dirSet copies the spine along digits and writes kid at the leaf, returning the
// new subtree root. Untouched pages are referenced by their existing CIDs, so
// one append writes exactly len(digits) new blocks (spec 5.3, O(dir_depth)).
//
// node may be undefined, which is how pages come into existence on the way
// down.
func (h *Head) dirSet(ctx context.Context, node cid.Cid, digits []uint64, kid cid.Cid) (cid.Cid, error) {
	if len(digits) == 0 {
		return kid, nil
	}
	kids, err := h.loadKids(ctx, node)
	if err != nil {
		return cid.Undef, err
	}
	d := digits[0]

	var child cid.Cid
	if d < uint64(len(kids)) {
		child = kids[d]
	}
	newChild, err := h.dirSet(ctx, child, digits[1:], kid)
	if err != nil {
		return cid.Undef, err
	}

	kids = growTo(kids, d+1)
	kids[d] = newChild
	c, err := h.dirs.NewNode(&schema.DirNode{Kids: kids}).Commit(ctx)
	if err != nil {
		return cid.Undef, fmt.Errorf("archive: writing dirnode: %w", err)
	}
	return c, nil
}

// dirTruncate rewrites the subtree at node to address only the entries whose
// index within it is below the value of digits, and returns the new subtree
// root (spec 5.4).
//
// It requires that value(digits) > 0 and node is defined, i.e. that this
// subtree survives at all. A page whose surviving entries are all null still
// survives, as a page with no kids: it exists because entries were appended to
// it, and a fresh build of the same data would write it too. Only a page with
// no appended entries is dropped, which the allZero test below is what
// distinguishes.
func (h *Head) dirTruncate(ctx context.Context, node cid.Cid, digits []uint64) (cid.Cid, error) {
	kids, err := h.loadKids(ctx, node)
	if err != nil {
		return cid.Undef, err
	}
	d := digits[0]

	if rest := digits[1:]; len(rest) == 0 {
		// Leaf page: entries d and up go away.
		kids = clampTo(kids, d)
	} else if allZero(rest) {
		// Nothing survives inside child d, and child d itself holds no entries
		// below it either, so the whole child goes with them.
		kids = clampTo(kids, d)
	} else {
		if d >= uint64(len(kids)) || !kids[d].Defined() {
			return cid.Undef, fmt.Errorf("archive: internal: directory page has no child %d, but entries below it survive truncation", d)
		}
		child, err := h.dirTruncate(ctx, kids[d], rest)
		if err != nil {
			return cid.Undef, err
		}
		kids = growTo(clampTo(kids, d), d+1)
		kids[d] = child
	}

	c, err := h.dirs.NewNode(&schema.DirNode{Kids: kids}).Commit(ctx)
	if err != nil {
		return cid.Undef, fmt.Errorf("archive: writing truncated dirnode: %w", err)
	}
	return c, nil
}

// dirShrink drops root levels that exist only to wrap a single child (spec
// 5.4), returning the new root and depth.
//
// This reproduces exactly the depth a fresh build of the surviving entries
// would reach. Growth wraps the root in a single-kid node, so a directory of
// depth d holding n entries is the canonical depth-cd tree for n under d-cd
// such wrappers; at cd itself the root has two or more kids (n > capacity(cd-1)
// is why the tree grew), so unwrapping stops in the right place.
func (h *Head) dirShrink(ctx context.Context, dir cid.Cid, depth uint64) (cid.Cid, uint64, error) {
	for depth > 1 {
		node, err := h.dirs.GetNode(ctx, dir)
		if err != nil {
			return cid.Undef, 0, fmt.Errorf("archive: reading dirnode %s: %w", dir, err)
		}
		// Pages omit their trailing nulls (spec 3.3), so a single kid is a
		// single non-null kid and it is kids[0].
		if len(node.Kids) != 1 || !node.Kids[0].Defined() {
			break
		}
		dir, depth = node.Kids[0], depth-1
	}
	return dir, depth, nil
}

// loadKids returns a private copy of the node's kids, or nil for an undefined
// node. The copy matters: GetNode's result is shared with the decoded-node
// cache and every other pointer to that CID.
func (h *Head) loadKids(ctx context.Context, node cid.Cid) ([]cid.Cid, error) {
	if !node.Defined() {
		return nil, nil
	}
	n, err := h.dirs.GetNode(ctx, node)
	if err != nil {
		return nil, fmt.Errorf("archive: reading dirnode %s: %w", node, err)
	}
	return append([]cid.Cid(nil), n.Kids...), nil
}

// growTo null-pads kids to length n.
func growTo(kids []cid.Cid, n uint64) []cid.Cid {
	for uint64(len(kids)) < n {
		kids = append(kids, cid.Undef)
	}
	return kids
}

// clampTo drops kids from index n on.
func clampTo(kids []cid.Cid, n uint64) []cid.Cid {
	if uint64(len(kids)) > n {
		return kids[:n]
	}
	return kids
}
