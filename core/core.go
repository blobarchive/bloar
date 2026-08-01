// Package core provides the content-addressed store mechanics the head engine
// is built from: a Codec binding for a node type, a NodeStore that reads and
// writes those nodes as blocks, and Pointer[T], the three-state (hash, loaded,
// dirty) reference to a node.
//
// Nodes are immutable once committed. Mutating a node means producing a new
// node with a new CID; the old block is left alone. Untouched nodes are never
// re-encoded, so they keep their CIDs and their blocks are shared between
// successive roots.
package core

import (
	"context"
	"errors"
	"fmt"

	"github.com/ipfs/boxo/blockstore"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
)

// Codec binds encode and decode for one node type. The signatures match
// schema's EncodeX/DecodeX exactly, so a codec for a schema type is a pair of
// method values:
//
//	core.Codec[schema.Segment]{Encode: schema.EncodeSegment, Decode: schema.DecodeSegment}
//
// Go methods cannot introduce type parameters, so this is how a *NodeStore[T]
// gets hold of the functions for its T.
type Codec[T any] struct {
	Encode func(*T) ([]byte, cid.Cid, error)
	Decode func([]byte) (*T, error)
}

// NodeStore reads and writes nodes of one type as blocks. It verifies nothing:
// blocks are content-addressed and the blockstore hashes them.
//
// A NodeStore is safe for concurrent use if its blockstore is.
type NodeStore[T any] struct {
	bs    blockstore.Blockstore
	codec Codec[T]
	cache *NodeCache
}

// NewNodeStore returns a NodeStore over bs for the node type bound by codec.
// cache may be nil, in which case every read decodes.
func NewNodeStore[T any](bs blockstore.Blockstore, codec Codec[T], cache *NodeCache) *NodeStore[T] {
	if bs == nil {
		panic("core: NewNodeStore with nil blockstore")
	}
	if codec.Encode == nil || codec.Decode == nil {
		panic("core: NewNodeStore with an incomplete codec")
	}
	return &NodeStore[T]{bs: bs, codec: codec, cache: cache}
}

// Blocks returns the underlying blockstore. Raw blobs need no wrapper: callers
// address them here directly.
func (ns *NodeStore[T]) Blocks() blockstore.Blockstore { return ns.bs }

// GetNode reads and decodes the node at c, consulting the decoded-node cache
// first.
//
// The returned *T may be shared with the cache and with other callers, so it
// MUST NOT be mutated. Use Pointer.Mutate to obtain a value that may be.
func (ns *NodeStore[T]) GetNode(ctx context.Context, c cid.Cid) (*T, error) {
	if !c.Defined() {
		return nil, errors.New("core: GetNode on an undefined CID")
	}
	if ns.cache != nil {
		if v, ok := ns.cache.get(c); ok {
			// A CID commits to the block bytes, so a hit can only be a T --
			// unless a caller mixed node types over one cache. Treat that as a
			// miss rather than trusting the assertion.
			if node, ok := v.(*T); ok {
				return node, nil
			}
		}
	}
	node, size, err := ns.decode(ctx, c)
	if err != nil {
		return nil, err
	}
	if ns.cache != nil {
		ns.cache.add(c, node, size)
	}
	return node, nil
}

// PutNode encodes v, writes the block, and returns its CID.
func (ns *NodeStore[T]) PutNode(ctx context.Context, v *T) (cid.Cid, error) {
	data, c, err := ns.codec.Encode(v)
	if err != nil {
		return cid.Undef, fmt.Errorf("core: encoding node: %w", err)
	}
	blk, err := blocks.NewBlockWithCid(data, c)
	if err != nil {
		return cid.Undef, fmt.Errorf("core: framing block %s: %w", c, err)
	}
	if err := ns.bs.Put(ctx, blk); err != nil {
		return cid.Undef, fmt.Errorf("core: writing block %s: %w", c, err)
	}
	return c, nil
}

// decode reads and decodes the block at c, bypassing the cache. It returns the
// decoded node and the encoded block length.
func (ns *NodeStore[T]) decode(ctx context.Context, c cid.Cid) (*T, int, error) {
	blk, err := ns.bs.Get(ctx, c)
	if err != nil {
		return nil, 0, fmt.Errorf("core: reading block %s: %w", c, err)
	}
	data := blk.RawData()
	node, err := ns.codec.Decode(data)
	if err != nil {
		return nil, 0, fmt.Errorf("core: decoding block %s: %w", c, err)
	}
	return node, len(data), nil
}
