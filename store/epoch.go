package store

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/ipfs/boxo/blockstore"
	dshelp "github.com/ipfs/boxo/datastore/dshelp"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	flatfs "github.com/ipfs/go-ds-flatfs"
)

// epochShardCount is large enough that unrelated application writes and GC
// candidates almost never contend, while remaining small enough to allocate a
// fresh lock table for every GC run. It is a power of two so shard selection is
// a mask rather than a division in every blockstore operation.
const epochShardCount = 4096

// ErrEpochActive reports an attempt to begin a blockstore epoch while another
// one is still active. Concurrent collectors must not share a protection set:
// ending either one would otherwise make the other's deletion decisions
// unsafe.
type ErrEpochActive struct {
	ID uint64
}

func (e *ErrEpochActive) Error() string {
	return fmt.Sprintf("store: blockstore epoch %d is already active", e.ID)
}

// ErrEpochEnded is returned when a collector tries to delete through an epoch
// that is no longer active. It is a programming error, not permission to fall
// back to an uncoordinated delete.
var ErrEpochEnded = errors.New("store: blockstore epoch has ended")

// KeyIterator enumerates physical block keys and reports errors both when the
// query opens and while it is consumed. A successful close of both channels is
// a proof of complete enumeration.
type KeyIterator func(context.Context) (<-chan cid.Cid, <-chan error, error)

// EpochOption configures a blockstore epoch coordinator.
type EpochOption func(*BlockstoreEpochs)

// WithKeyIterator installs the complete, error-preserving enumeration required
// by online GC and integrity scrub. The caller must surface every asynchronous
// backend error on the returned error channel; falsely closing it successfully
// can make a scrub report incomplete coverage.
func WithKeyIterator(iterator KeyIterator) EpochOption {
	return func(e *BlockstoreEpochs) {
		if iterator != nil {
			e.allKeys = iterator
			e.completeEnumeration = true
		}
	}
}

// BlockstoreEpochs coordinates an application-facing blockstore with an
// online collector. During an active epoch, every successful application read
// and write protects its multihash. Candidate deletion takes the same sharded
// lock and rechecks that protection set immediately before deleting, so either
// ordering is safe:
//
//   - the application operation finishes first and the collector preserves the
//     block; or
//   - the collector deletes first and the later application operation observes
//     the deletion (or writes the block back).
//
// lifecycle prevents Begin or End from changing the active protection set in
// the middle of an application operation. It is held for reading by ordinary
// operations, so those operations remain concurrent; the 4096 per-epoch
// shards are the only locks shared by operations on block keys. Application
// AllKeysChan is the exception: begun while idle, it holds the lifecycle read
// lock until its channel drains or its context is cancelled, preventing Begin
// from splitting an enumeration which cannot protect atomically per key; an
// enumeration attempted during an epoch fails with ErrEpochActive. Collector
// enumeration uses the separate untracked, error-preserving AllKeys path.
type BlockstoreEpochs struct {
	base blockstore.Blockstore
	app  applicationBlockstore

	lifecycle sync.RWMutex
	active    *epochState
	nextID    uint64

	allKeys             KeyIterator
	completeEnumeration bool
}

type epochState struct {
	id        uint64
	shards    [epochShardCount]epochShard
	protected atomic.Int64
}

type epochShard struct {
	mu      sync.Mutex
	touched map[string]struct{}
}

// BlockstoreEpoch is one active collector's handle. Its Blocks and AllKeys
// views deliberately bypass application protection: marking a retained block
// must not itself protect every retained block a second time.
type BlockstoreEpoch struct {
	owner *BlockstoreEpochs
	state *epochState

	endOnce  sync.Once
	endCount int
}

// NewBlockstoreEpochs wraps base with an application protection boundary.
// base is expected to be the validating blockstore, so both the application
// and collector views retain read-time CID validation.
//
// Generic blockstores have only AllKeysChan, whose interface cannot report an
// error that happens after enumeration starts. Stores opened by Open replace
// this fallback with a direct flatfs query that preserves asynchronous errors.
func NewBlockstoreEpochs(base blockstore.Blockstore, opts ...EpochOption) *BlockstoreEpochs {
	e := &BlockstoreEpochs{base: base}
	e.app.owner = e
	e.allKeys = e.fallbackAllKeys
	for _, opt := range opts {
		opt(e)
	}
	return e
}

// Application returns the blockstore view used by all non-collector code.
func (e *BlockstoreEpochs) Application() blockstore.Blockstore { return &e.app }

// CollectorBlocks returns the validating but untracked blockstore view used by
// GC marking and integrity scrubs. Calling Get through this view does not add
// the block to the active epoch's protection set.
func (e *BlockstoreEpochs) CollectorBlocks() blockstore.Blockstore { return e.base }

// CompleteEnumeration reports whether AllKeys has an explicit asynchronous
// error channel supplied by the backing store. Online GC/scrub constructors
// fail closed when this is false; Boxo's AllKeysChan logs and hides such errors.
func (e *BlockstoreEpochs) CompleteEnumeration() bool { return e.completeEnumeration }

// Begin starts a new protection epoch. Only one epoch may be active at a time.
func (e *BlockstoreEpochs) Begin() (*BlockstoreEpoch, error) {
	e.lifecycle.Lock()
	defer e.lifecycle.Unlock()

	if e.active != nil {
		return nil, &ErrEpochActive{ID: e.active.id}
	}
	e.nextID++
	if e.nextID == 0 { // reserve zero for "no active epoch" after wraparound.
		e.nextID++
	}
	state := &epochState{id: e.nextID}
	e.active = state
	return &BlockstoreEpoch{owner: e, state: state}, nil
}

// ActiveEpoch returns the current epoch ID, or zero when collection is idle.
func (e *BlockstoreEpochs) ActiveEpoch() uint64 {
	e.lifecycle.RLock()
	defer e.lifecycle.RUnlock()
	if e.active == nil {
		return 0
	}
	return e.active.id
}

// CollectionGeneration is the most recently allocated epoch ID, including
// after that epoch has ended. A new collection increments it before any delete
// can occur. Long-lived application caches use this monotonic generation to
// invalidate presence proofs made before a completed collection.
func (e *BlockstoreEpochs) CollectionGeneration() uint64 {
	e.lifecycle.RLock()
	defer e.lifecycle.RUnlock()
	return e.nextID
}

// AllKeys enumerates collector keys without activating an epoch. Unlike
// blockstore.AllKeysChan, the second channel carries errors discovered after
// enumeration starts. Consumers must drain keys and errors concurrently (or
// select over both) until both channels close.
func (e *BlockstoreEpochs) AllKeys(ctx context.Context) (<-chan cid.Cid, <-chan error, error) {
	return e.allKeys(ctx)
}

func (e *BlockstoreEpochs) fallbackAllKeys(ctx context.Context) (<-chan cid.Cid, <-chan error, error) {
	keys, err := e.base.AllKeysChan(ctx)
	if err != nil {
		return nil, nil, err
	}
	errs := make(chan error)
	close(errs)
	return keys, errs, nil
}

// flatFSAllKeys uses the datastore query directly because Boxo's
// AllKeysChan logs and silently truncates the stream on both query-result and
// datastore-key conversion errors. A truncated GC enumeration is not safe to
// treat as complete.
func flatFSAllKeys(ds *flatfs.Datastore) KeyIterator {
	return func(ctx context.Context) (<-chan cid.Cid, <-chan error, error) {
		results, err := ds.Query(ctx, query.Query{KeysOnly: true})
		if err != nil {
			return nil, nil, fmt.Errorf("store: opening block-key enumeration: %w", err)
		}

		keys := make(chan cid.Cid, query.KeysOnlyBufSize)
		errs := make(chan error, 2)
		go func() {
			defer close(keys)
			defer close(errs)
			defer func() {
				if closeErr := results.Close(); closeErr != nil && ctx.Err() == nil {
					select {
					case errs <- fmt.Errorf("store: closing block-key enumeration: %w", closeErr):
					case <-ctx.Done():
					}
				}
			}()

			for {
				select {
				case <-ctx.Done():
					return
				case result, ok := <-results.Next():
					if !ok {
						return
					}
					if result.Error != nil {
						errs <- fmt.Errorf("store: enumerating block keys: %w", result.Error)
						return
					}
					mh, err := dshelp.DsKeyToMultihash(datastore.RawKey(result.Key))
					if err != nil {
						errs <- fmt.Errorf("store: converting block key %q to CID: %w", result.Key, err)
						return
					}
					c := cid.NewCidV1(cid.Raw, mh)
					select {
					case <-ctx.Done():
						return
					case keys <- c:
					}
				}
			}
		}()
		return keys, errs, nil
	}
}

// ID returns this epoch's nonzero, process-local sequence number.
func (e *BlockstoreEpoch) ID() uint64 { return e.state.id }

// Protected returns the number of distinct multihashes touched by successful
// application operations so far. It is an observability snapshot only;
// DeleteCandidate still performs the authoritative per-key check under lock.
func (e *BlockstoreEpoch) Protected() int { return int(e.state.protected.Load()) }

// Blocks returns the validating, untracked collector view.
func (e *BlockstoreEpoch) Blocks() blockstore.Blockstore {
	return e.owner.CollectorBlocks()
}

// AllKeys delegates to the coordinator's error-preserving enumerator.
func (e *BlockstoreEpoch) AllKeys(ctx context.Context) (<-chan cid.Cid, <-chan error, error) {
	return e.owner.AllKeys(ctx)
}

// DeleteCandidate deletes c only if no successful application operation has
// touched its multihash during this epoch. The protection check and underlying
// delete share a shard lock with application reads, writes, and deletes.
func (e *BlockstoreEpoch) DeleteCandidate(ctx context.Context, c cid.Cid) (deleted, protected bool, err error) {
	owner := e.owner
	owner.lifecycle.RLock()
	defer owner.lifecycle.RUnlock()
	if owner.active != e.state {
		return false, false, ErrEpochEnded
	}

	key := multihashKey(c)
	shard := &e.state.shards[epochShardIndex(key)]
	shard.mu.Lock()
	defer shard.mu.Unlock()

	// Recheck while holding the same key shard as application operations. The
	// lifecycle read lock keeps the epoch itself stable across this decision.
	if _, ok := shard.touched[key]; ok {
		return false, true, nil
	}
	if err := owner.base.DeleteBlock(ctx, c); err != nil {
		return false, false, err
	}
	return true, false, nil
}

// End deactivates the epoch and returns the number of distinct multihashes
// protected during it. It is idempotent.
func (e *BlockstoreEpoch) End() int {
	e.endOnce.Do(func() {
		e.owner.lifecycle.Lock()
		if e.owner.active == e.state {
			e.owner.active = nil
		}
		e.endCount = int(e.state.protected.Load())
		e.owner.lifecycle.Unlock()
	})
	return e.endCount
}

type applicationBlockstore struct {
	owner *BlockstoreEpochs
}

// CollectionGeneration forwards the coordinator generation for application
// caches that memoize successful blockstore walks.
func (a *applicationBlockstore) CollectionGeneration() uint64 {
	return a.owner.CollectionGeneration()
}

// ActiveEpoch lets mutation coordinators detect that their application-facing
// blockstore is currently protected by an online collection epoch without
// depending on this concrete wrapper type.
func (a *applicationBlockstore) ActiveEpoch() uint64 { return a.owner.ActiveEpoch() }

func (a *applicationBlockstore) DeleteBlock(ctx context.Context, c cid.Cid) error {
	return a.withKey(c, func(_ *epochState, _ *epochShard, _ string) error {
		return a.owner.base.DeleteBlock(ctx, c)
	})
}

func (a *applicationBlockstore) Has(ctx context.Context, c cid.Cid) (bool, error) {
	var present bool
	err := a.withKey(c, func(state *epochState, shard *epochShard, key string) error {
		var err error
		present, err = a.owner.base.Has(ctx, c)
		if err == nil && present {
			touch(state, shard, key)
		}
		return err
	})
	return present, err
}

func (a *applicationBlockstore) Get(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	var block blocks.Block
	err := a.withKey(c, func(state *epochState, shard *epochShard, key string) error {
		var err error
		block, err = a.owner.base.Get(ctx, c)
		if err == nil {
			touch(state, shard, key)
		}
		return err
	})
	return block, err
}

func (a *applicationBlockstore) GetSize(ctx context.Context, c cid.Cid) (int, error) {
	size := -1
	err := a.withKey(c, func(state *epochState, shard *epochShard, key string) error {
		var err error
		size, err = a.owner.base.GetSize(ctx, c)
		if err == nil {
			touch(state, shard, key)
		}
		return err
	})
	return size, err
}

func (a *applicationBlockstore) Put(ctx context.Context, block blocks.Block) error {
	c := block.Cid()
	return a.withKey(c, func(state *epochState, shard *epochShard, key string) error {
		err := a.owner.base.Put(ctx, block)
		if err == nil {
			touch(state, shard, key)
		}
		return err
	})
}

func (a *applicationBlockstore) PutMany(ctx context.Context, input []blocks.Block) error {
	owner := a.owner
	owner.lifecycle.RLock()
	defer owner.lifecycle.RUnlock()
	state := owner.active
	if state == nil || len(input) == 0 {
		return owner.base.PutMany(ctx, input)
	}

	type keyedBlock struct {
		key   string
		shard int
	}
	keyed := make([]keyedBlock, len(input))
	var used [epochShardCount]bool
	shards := make([]int, 0, min(len(input), epochShardCount))
	for i, block := range input {
		key := multihashKey(block.Cid())
		index := epochShardIndex(key)
		keyed[i] = keyedBlock{key: key, shard: index}
		if !used[index] {
			used[index] = true
			shards = append(shards, index)
		}
	}
	// The fixed scan supplies a stable ascending lock order without sorting a
	// potentially very large PutMany input, preventing deadlocks between two
	// overlapping batches.
	locked := shards[:0]
	for index := range epochShardCount {
		if used[index] {
			state.shards[index].mu.Lock()
			locked = append(locked, index)
		}
	}
	defer func() {
		for i := len(locked) - 1; i >= 0; i-- {
			state.shards[locked[i]].mu.Unlock()
		}
	}()

	err := owner.base.PutMany(ctx, input)
	// Boxo batches may fail after writing only part of the input. Protecting the
	// whole requested set is conservative and prevents collection from guessing
	// which subset became durable.
	for _, item := range keyed {
		touch(state, &state.shards[item.shard], item.key)
	}
	return err
}

func (a *applicationBlockstore) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	owner := a.owner
	owner.lifecycle.RLock()
	if owner.active != nil {
		id := owner.active.id
		owner.lifecycle.RUnlock()
		return nil, &ErrEpochActive{ID: id}
	}
	keys, err := owner.base.AllKeysChan(ctx)
	if err != nil {
		owner.lifecycle.RUnlock()
		return nil, err
	}

	// Application enumeration has no asynchronous error channel in Boxo's
	// Blockstore interface, so it cannot safely join an epoch key by key. Keep
	// the lifecycle read lock until the source closes instead: a Begin waits,
	// and an enumeration attempted after Begin fails above. Callers must drain
	// the returned channel or cancel ctx, as required by AllKeysChan generally.
	out := make(chan cid.Cid)
	go func() {
		defer owner.lifecycle.RUnlock()
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case c, ok := <-keys:
				if !ok {
					return
				}
				select {
				case <-ctx.Done():
					return
				case out <- c:
				}
			}
		}
	}()
	return out, nil
}

// withKey keeps the active epoch stable and, when one exists, holds c's shard
// across both the underlying operation and any protection touch.
func (a *applicationBlockstore) withKey(c cid.Cid, fn func(*epochState, *epochShard, string) error) error {
	owner := a.owner
	owner.lifecycle.RLock()
	defer owner.lifecycle.RUnlock()
	state := owner.active
	if state == nil {
		return fn(nil, nil, "")
	}
	key := multihashKey(c)
	shard := &state.shards[epochShardIndex(key)]
	shard.mu.Lock()
	defer shard.mu.Unlock()
	return fn(state, shard, key)
}

func touch(state *epochState, shard *epochShard, key string) {
	if state == nil {
		return
	}
	if shard.touched == nil {
		shard.touched = make(map[string]struct{})
	}
	if _, exists := shard.touched[key]; exists {
		return
	}
	shard.touched[key] = struct{}{}
	state.protected.Add(1)
}

// multihashKey deliberately omits CID version and codec: the flatfs
// blockstore uses the multihash as its physical key, so aliases must share both
// protection and exclusion locks.
func multihashKey(c cid.Cid) string { return string(c.Hash()) }

func epochShardIndex(key string) int {
	// FNV-1a is stable, allocation-free, and adequate for distributing content
	// hashes over a power-of-two lock table.
	var hash uint32 = 2166136261
	for i := range len(key) {
		hash ^= uint32(key[i])
		hash *= 16777619
	}
	return int(hash & (epochShardCount - 1))
}
