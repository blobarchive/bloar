package server

import (
	"context"
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble/v2"
	"github.com/ipfs/go-cid"
)

// prefixHeadRoot is this package's byte of the KV keyspace. See the package
// comment for the layout and catalog's for the rest of it.
const prefixHeadRoot byte = 'h'

// syncWrite is the write option RootStore mutations use: the root is what a
// restart resumes from, and losing an acknowledged one to a page cache would
// silently regress a head.
var syncWrite = pebble.Sync

// RootStore persists the current root of each head, under the 'h' prefix. It is
// node-local state in the sense of spec 6, but NOT rebuildable from the DAG: a
// root is a CID, and nothing in a blockstore full of Head blocks says which one
// is current. What the entry MEANS depends on the role.
//
// On a WRITER, 'h' is the AUTHORITATIVE RESTART SELECTOR for a head: the root a
// restart resumes serving and building from. There is no supported path that
// imports a root from a publication document -- a writer that loses this KV does
// not adopt its own old publication, it re-derives each head from the chain by
// letting the indexers replay from an empty head (a correct but very long
// re-index). So it is authoritative and non-derivable, not a convenience cache.
//
// On a FOLLOWER, the head's 'h' entry is an EXACT WRITE-THROUGH COMPATIBILITY
// MIRROR of the follower's authoritative 'f' checkpoint (follow/state.go): it is
// re-derived from 'f' on each adoption purely so the read/serve path and the pin
// reconciler have a root where they expect one. It is never a resume source and
// never an authority -- Resume reads only the 'f' checkpoint, whose adoption is
// gated by the authenticated anti-replay floors, so a follower never trusts an
// arbitrary publication document to set this.
type RootStore struct {
	kv           *pebble.DB
	generations  *GenerationStore
	publications *PublicationStore
}

// NewRootStore returns a RootStore over kv, which is store.Store.KV().
func NewRootStore(kv *pebble.DB) *RootStore {
	s := &RootStore{kv: kv}
	s.generations = NewGenerationStore(kv)
	s.publications = NewPublicationStore(kv)
	return s
}

// GenerationStore returns the store which atomically owns this RootStore's
// mutable root+generation commits.
func (s *RootStore) GenerationStore() *GenerationStore { return s.generations }

// PublicationStore returns the signer-local revision allocator sharing this
// RootStore's Pebble database.
func (s *RootStore) PublicationStore() *PublicationStore { return s.publications }

// rootKey renders 'h' || name. Nothing prefix-scans this space, so the
// variable-length name needs no terminator.
func rootKey(name string) []byte {
	k := make([]byte, 0, 1+len(name))
	return append(append(k, prefixHeadRoot), name...)
}

// Get returns the persisted root of the named head. A head with no persisted
// root is (cid.Undef, false, nil): it has never been created.
func (s *RootStore) Get(ctx context.Context, name string) (cid.Cid, bool, error) {
	if err := ctx.Err(); err != nil {
		return cid.Undef, false, err
	}
	if name == "" {
		return cid.Undef, false, errors.New("server: head name must not be empty")
	}
	v, closer, err := s.kv.Get(rootKey(name))
	if errors.Is(err, pebble.ErrNotFound) {
		return cid.Undef, false, nil
	}
	if err != nil {
		return cid.Undef, false, fmt.Errorf("server: reading root of head %q: %w", name, err)
	}
	defer closer.Close()

	root, err := cid.Cast(v)
	if err != nil {
		return cid.Undef, false, fmt.Errorf("server: head %q has an undecodable root: %w", name, err)
	}
	return root, true, nil
}

// Put records root as the current root of the named head. It is called after
// every mutation the head engine publishes.
func (s *RootStore) Put(ctx context.Context, name string, root cid.Cid) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if name == "" {
		return errors.New("server: head name must not be empty")
	}
	if !root.Defined() {
		return fmt.Errorf("server: refusing to store an undefined root for head %q", name)
	}
	if err := s.kv.Set(rootKey(name), root.Bytes(), syncWrite); err != nil {
		return fmt.Errorf("server: writing root of head %q: %w", name, err)
	}
	return nil
}

// StagePut stages root as the named head's current root into b, for a caller
// that must commit this mirror atomically with writes to other keyspaces of the
// same KV -- the follower->writer promotion handoff, which materializes this
// mirror and retires the follower checkpoint in one synced batch (the safety boundary
// follow-up). It writes the same key and value Put does, so the batch and the
// synchronous path agree byte for byte; the caller owns the batch's Commit and
// its durability. It stages only -- b is not committed here.
func (s *RootStore) StagePut(b *pebble.Batch, name string, root cid.Cid) error {
	if name == "" {
		return errors.New("server: head name must not be empty")
	}
	if !root.Defined() {
		return fmt.Errorf("server: refusing to store an undefined root for head %q", name)
	}
	if err := b.Set(rootKey(name), root.Bytes(), nil); err != nil {
		return fmt.Errorf("server: staging root of head %q: %w", name, err)
	}
	return nil
}

// StageDelete stages removal of the named head's compatibility root mirror into
// b. A revisioned follower document uses it for an authenticated withdrawal,
// in the same Pebble batch as the checkpoint tombstone and manifest-tip delete.
// Deleting an absent root is idempotent. The caller owns b's commit and
// durability.
func (s *RootStore) StageDelete(b *pebble.Batch, name string) error {
	if name == "" {
		return errors.New("server: head name must not be empty")
	}
	if err := b.Delete(rootKey(name), nil); err != nil {
		return fmt.Errorf("server: staging deletion of root of head %q: %w", name, err)
	}
	return nil
}
