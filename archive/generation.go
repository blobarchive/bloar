package archive

import "context"

// BuildGeneration constructs a complete, independent head generation from a
// snapshot of blob-bearing rows and the coverage boundary syncedTo.
//
// The snapshot follows ApplyRefs' shape rules: rows are strictly ascending,
// each row is within [params.OriginSlot, syncedTo], and slots without blobs are
// omitted. In particular, an empty rows slice is a valid fully covered
// generation when the covered range contains no blobs.
//
// Construction never mutates another Head, even when cfg shares its
// blockstore, resolver, or cache with existing heads. The new Head is not
// returned until its full snapshot has been resolved and published. A failed
// build may leave unreachable immutable blocks in the shared blockstore; they
// are safe for normal garbage collection and no existing root references them.
//
// BuildGeneration deliberately uses the same New and ApplyRefs path as normal
// archive construction. Consequently, identical parameters, rows, blob CIDs,
// and coverage produce the same content-addressed root.
func BuildGeneration(ctx context.Context, cfg Config, params Params, rows []RefRow, syncedTo uint64) (*Head, error) {
	// Validate the complete snapshot before even publishing the new empty head.
	// ApplyRefs repeats this check at its mutation boundary; keeping that check
	// there is important for callers that build incrementally.
	if err := validateShape(&state{params: params}, rows, syncedTo); err != nil {
		return nil, err
	}

	h, err := New(ctx, cfg, params)
	if err != nil {
		return nil, err
	}
	if _, err := h.ApplyRefs(ctx, rows, syncedTo); err != nil {
		return nil, err
	}
	return h, nil
}
