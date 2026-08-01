package core

import (
	"context"
	"errors"
	"fmt"

	"github.com/ipfs/go-cid"
)

// ErrDirty reports an attempt to read the CID of a node that has uncommitted
// changes. A dirty node has no CID: its bytes do not exist yet.
//
// This is the invariant that forces bottom-up commits. A parent's links are
// its children's CIDs, so building a parent block whose child is still dirty
// fails here rather than silently writing a stale or null link.
var ErrDirty = errors.New("core: node is dirty, commit it before taking its CID")

// ErrEmpty reports a pointer that references nothing: neither a CID to load
// from nor a value to commit.
var ErrEmpty = errors.New("core: pointer has neither a CID nor a value")

// Pointer is a reference to a node in one of three states:
//
//	hash:   a CID, nothing loaded. Load fetches and decodes.
//	loaded: a CID and the decoded node. The node matches the CID.
//	dirty:  a decoded node with pending changes and no CID. Commit writes it.
//
// Load upgrades hash to loaded in place. Mutate moves either to dirty; Commit
// moves dirty back to loaded. A Pointer is not safe for concurrent use.
//
// Pointers come from NodeStore.Pointer or NodeStore.NewNode; the zero Pointer
// is not usable.
type Pointer[T any] struct {
	ns    *NodeStore[T]
	cid   cid.Cid
	val   *T
	dirty bool
}

// Pointer returns a lazy pointer to the node at c. Nothing is read until Load.
func (ns *NodeStore[T]) Pointer(c cid.Cid) *Pointer[T] {
	return &Pointer[T]{ns: ns, cid: c}
}

// NewNode returns a dirty pointer to v, a node that has never been written. v
// must not be aliased by the caller after this: the pointer now owns it.
func (ns *NodeStore[T]) NewNode(v *T) *Pointer[T] {
	return &Pointer[T]{ns: ns, val: v, dirty: true}
}

// IsDirty reports whether p has uncommitted changes.
func (p *Pointer[T]) IsDirty() bool { return p.dirty }

// IsLoaded reports whether p holds a decoded node.
func (p *Pointer[T]) IsLoaded() bool { return p.val != nil }

// CID returns the CID of the node, or ErrDirty if it has uncommitted changes.
func (p *Pointer[T]) CID() (cid.Cid, error) {
	if p.dirty {
		return cid.Undef, ErrDirty
	}
	if !p.cid.Defined() {
		return cid.Undef, ErrEmpty
	}
	return p.cid, nil
}

// Load returns the decoded node, fetching and decoding it on first touch and
// caching it in the pointer.
//
// The result MUST NOT be mutated: it may be shared with the decoded-node cache
// and with every other pointer to this CID. Use Mutate to change a node.
func (p *Pointer[T]) Load(ctx context.Context) (*T, error) {
	if p.val != nil {
		return p.val, nil
	}
	if !p.cid.Defined() {
		return nil, ErrEmpty
	}
	v, err := p.ns.GetNode(ctx, p.cid)
	if err != nil {
		return nil, err
	}
	p.val = v
	return p.val, nil
}

// Mutate returns the node for in-place modification and marks p dirty,
// dropping its CID. The caller may change the returned node freely; the change
// reaches storage at the next Commit.
//
// A clean node's value may be shared (with the cache, or with other pointers
// to the same CID), so Mutate cannot hand it out for writing. It decodes a
// private copy straight from the block instead, bypassing the cache. That
// costs one decode per node actually mutated, which is rare next to reads, and
// it keeps correctness independent of whether a cache exists at all. A node
// that is already dirty is already private and is returned as-is.
func (p *Pointer[T]) Mutate(ctx context.Context) (*T, error) {
	if p.dirty {
		return p.val, nil
	}
	if !p.cid.Defined() {
		return nil, ErrEmpty
	}
	v, _, err := p.ns.decode(ctx, p.cid)
	if err != nil {
		return nil, err
	}
	p.val = v
	p.cid = cid.Undef
	p.dirty = true
	return p.val, nil
}

// Set replaces the node with v and marks p dirty. It is Mutate for callers
// that build a replacement node outright rather than editing the old one, and
// unlike Mutate it never reads the old block. v must not be aliased by the
// caller after this.
func (p *Pointer[T]) Set(v *T) {
	p.val = v
	p.cid = cid.Undef
	p.dirty = true
}

// Commit writes the node if dirty and returns its CID. It is a no-op for a
// clean pointer, which already has one.
//
// Commit is the per-node mechanism only: it does not walk children, because
// core does not know what a child is. Callers commit children first, write the
// resulting CIDs into the parent through Mutate or Set, then commit the
// parent. Getting that order wrong is an error, not a silent corruption: the
// parent's encode reads child CIDs and a dirty child has none (ErrDirty).
func (p *Pointer[T]) Commit(ctx context.Context) (cid.Cid, error) {
	if !p.dirty {
		return p.CID()
	}
	if p.val == nil {
		return cid.Undef, ErrEmpty
	}
	c, err := p.ns.PutNode(ctx, p.val)
	if err != nil {
		return cid.Undef, fmt.Errorf("core: committing node: %w", err)
	}
	p.cid = c
	p.dirty = false
	// The node is not added to the cache here: the cache is read-through, and
	// p.val is still owned (and mutable) by whoever called Mutate.
	return c, nil
}
