package server

import (
	"context"
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble/v2"
	"github.com/ipfs/go-cid"
)

// prefixManifestTip is this package's second byte of the KV keyspace, alongside
// 'h' (head roots). See the package comment for the layout and catalog's for the
// rest of it.
const prefixManifestTip byte = 'm'

// ManifestStore persists the tip of each head's manifest chain (spec 10.5): the
// CID of its newest Manifest.
//
// It is a sibling of RootStore under the 'm' prefix and carries the same role
// taxonomy (spec 6), NOT rebuildable from the DAG: the chain itself lives in the
// DAG, pinned, and travels between nodes over bitswap, but nothing in a store
// full of Manifest blocks says which one is the current tip. On a WRITER the tip
// is the AUTHORITATIVE RESTART SELECTOR for the head's published filter
// attestation -- there is no supported import path. A writer that loses it
// un-publishes its heads' filters, and recovery turns on ONE question: do you have
// the EXACT known schedule chain, and does EVERY link still pass the append-only
// preflight at the head's current position? Manifests are content-addressed
// functions of {head, sources, prev}, so re-publishing a known schedule re-mints the
// IDENTICAL CID. If yes, reconstruct in place with BOTH the external HTTPS read route
// withdrawn AND IPNS publication disabled -- each POST rebuilds the document, which is
// both handed to the IPNS publisher (OnDoc) AND stored in what unauthenticated
// GET /bloar/v1/heads serves, so an intermediate genesis/ancestor doc would otherwise
// leak over EITHER channel (operations 4.6): from an absent tip, bootstrap the original
// genesis and then replay each known successor in order; from a stale ancestor tip,
// replay forward from there. publish-manifest re-validates each schedule at the current
// L1 position (index/chain preflightManifest -> ValidateUpgrade) -- which the genesis
// bootstrap, and any successor on an empty/uncovered head, skip because no ground is
// frozen -- and re-mints the next link, up to the identical tip the followers hold,
// which they accept (equal tip: follow.checkManifestAncestry returns nil when
// newTip == floor, spec 10.5). If no --
// any link unknown, a historical change now BEHIND the append-only boundary (frozen),
// or exact replay cannot reproduce the CID -- the general path is a backup/import
// (operations 4.6). On a
// FOLLOWER it is an EXACT WRITE-THROUGH COMPATIBILITY MIRROR of the authoritative
// 'f' checkpoint's manifest tip, re-derived from 'f' on adoption for the serve
// path and pin reconciler, never a resume source and never an authority. This is
// also the reason the tip is a field in the publication document rather than a
// link in the Head (spec 8, 10.5): the head root stays a pure function of the
// filtered data, and the tip is published and pinned beside it.
type ManifestStore struct {
	kv *pebble.DB
}

// NewManifestStore returns a ManifestStore over kv, which is store.Store.KV().
func NewManifestStore(kv *pebble.DB) *ManifestStore { return &ManifestStore{kv: kv} }

// manifestKey renders 'm' || name. Nothing prefix-scans this space, so the
// variable-length name needs no terminator.
func manifestKey(name string) []byte {
	k := make([]byte, 0, 1+len(name))
	return append(append(k, prefixManifestTip), name...)
}

// Get returns the persisted manifest tip of the named head. A head with no
// chain is (cid.Undef, false, nil).
func (s *ManifestStore) Get(ctx context.Context, name string) (cid.Cid, bool, error) {
	if err := ctx.Err(); err != nil {
		return cid.Undef, false, err
	}
	if name == "" {
		return cid.Undef, false, errors.New("server: head name must not be empty")
	}
	v, closer, err := s.kv.Get(manifestKey(name))
	if errors.Is(err, pebble.ErrNotFound) {
		return cid.Undef, false, nil
	}
	if err != nil {
		return cid.Undef, false, fmt.Errorf("server: reading manifest tip of head %q: %w", name, err)
	}
	defer closer.Close()

	tip, err := cid.Cast(v)
	if err != nil {
		return cid.Undef, false, fmt.Errorf("server: head %q has an undecodable manifest tip: %w", name, err)
	}
	return tip, true, nil
}

// Put records tip as the current manifest tip of the named head. It is written
// synchronously and durably, the same as a root: the tip is what a restart
// resumes the head's published manifest field from, and losing an acknowledged
// one would republish an old tip and trip followers' ancestry floor (spec 11.3).
func (s *ManifestStore) Put(ctx context.Context, name string, tip cid.Cid) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if name == "" {
		return errors.New("server: head name must not be empty")
	}
	if !tip.Defined() {
		return fmt.Errorf("server: refusing to store an undefined manifest tip for head %q", name)
	}
	if err := s.kv.Set(manifestKey(name), tip.Bytes(), syncWrite); err != nil {
		return fmt.Errorf("server: writing manifest tip of head %q: %w", name, err)
	}
	return nil
}

// Delete removes the named head's manifest tip, so a generation with no chain
// leaves no mirror behind. It is what makes this compatibility mirror EXACT: an
// older tip left in place when a head's current generation has none would be read
// by the pin reconciler (retaining an obsolete manifest history) and could be
// republished by a later writer promotion. Deleting an
// absent tip is not an error -- the mirror is already exact. Synchronous, the same
// as Put: the exactness must survive the crash it is defending against.
func (s *ManifestStore) Delete(ctx context.Context, name string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if name == "" {
		return errors.New("server: head name must not be empty")
	}
	if err := s.kv.Delete(manifestKey(name), syncWrite); err != nil {
		return fmt.Errorf("server: deleting manifest tip of head %q: %w", name, err)
	}
	return nil
}

// StagePut stages tip as the named head's manifest tip into b, and StageDelete
// stages the tip's removal, for a caller that must commit this mirror atomically
// with writes to other keyspaces of the same KV -- the follower->writer promotion
// handoff. They write exactly the
// keys and values Put and Delete do, so the batch and the synchronous paths agree,
// and staging the mirror EXACT -- a defined tip is a StagePut, an undefined one a
// StageDelete -- in the same batch is what stops a promotion republishing a manifest
// history the checkpoint's generation dropped. They stage only; the caller commits b.
func (s *ManifestStore) StagePut(b *pebble.Batch, name string, tip cid.Cid) error {
	if name == "" {
		return errors.New("server: head name must not be empty")
	}
	if !tip.Defined() {
		return fmt.Errorf("server: refusing to store an undefined manifest tip for head %q", name)
	}
	if err := b.Set(manifestKey(name), tip.Bytes(), nil); err != nil {
		return fmt.Errorf("server: staging manifest tip of head %q: %w", name, err)
	}
	return nil
}

// StageDelete stages the removal of the named head's manifest tip into b. See
// StagePut; deleting an absent tip is not an error, the mirror is already exact.
func (s *ManifestStore) StageDelete(b *pebble.Batch, name string) error {
	if name == "" {
		return errors.New("server: head name must not be empty")
	}
	if err := b.Delete(manifestKey(name), nil); err != nil {
		return fmt.Errorf("server: staging deletion of manifest tip of head %q: %w", name, err)
	}
	return nil
}
