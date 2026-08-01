package kubo

import (
	"context"
	"errors"

	"github.com/ipfs/boxo/blockstore"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
)

var (
	// ErrReplicaDeleteForbidden is returned when Bloar code attempts to delete
	// through a shared Kubo archive view. Kubo alone owns its repo GC.
	ErrReplicaDeleteForbidden = errors.New("kubo: block deletion is forbidden through the replica view")
	// ErrReplicaEnumerationForbidden prevents an accidental repository-wide
	// scan through the Bloar controller. Kubo owns its repository inventory.
	ErrReplicaEnumerationForbidden = errors.New("kubo: block enumeration is forbidden through the replica view")
)

// ReplicaBlockstore is an append-only capability wrapper for an operator-owned
// Kubo repo. Reads and verified cache writes pass through; block deletion and
// full-repository enumeration are structurally unavailable to Bloar even though
// boxo's broad Blockstore interface requires those methods to exist.
type ReplicaBlockstore struct {
	inner blockstore.Blockstore
}

var _ blockstore.Blockstore = (*ReplicaBlockstore)(nil)

func NewReplicaBlockstore(inner blockstore.Blockstore) (*ReplicaBlockstore, error) {
	if inner == nil {
		return nil, errors.New("kubo: replica blockstore requires an inner blockstore")
	}
	return &ReplicaBlockstore{inner: inner}, nil
}

func (*ReplicaBlockstore) DeleteBlock(context.Context, cid.Cid) error {
	return ErrReplicaDeleteForbidden
}

func (s *ReplicaBlockstore) Has(ctx context.Context, target cid.Cid) (bool, error) {
	return s.inner.Has(ctx, target)
}

func (s *ReplicaBlockstore) Get(ctx context.Context, target cid.Cid) (blocks.Block, error) {
	return s.inner.Get(ctx, target)
}

func (s *ReplicaBlockstore) GetSize(ctx context.Context, target cid.Cid) (int, error) {
	return s.inner.GetSize(ctx, target)
}

func (s *ReplicaBlockstore) Put(ctx context.Context, block blocks.Block) error {
	return s.inner.Put(ctx, block)
}

func (s *ReplicaBlockstore) PutMany(ctx context.Context, input []blocks.Block) error {
	return s.inner.PutMany(ctx, input)
}

func (*ReplicaBlockstore) AllKeysChan(context.Context) (<-chan cid.Cid, error) {
	return nil, ErrReplicaEnumerationForbidden
}
