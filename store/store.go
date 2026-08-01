// Package store opens and closes the node-local on-disk state: a flatfs-backed
// blockstore for blocks (blobs and index nodes) and a Pebble KV for everything
// else (spec 6).
package store

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/ipfs/boxo/blockstore"
	flatfs "github.com/ipfs/go-ds-flatfs"
)

// ShardFunc is the flatfs shard function bloar stores are created with. It is
// fixed at creation and verified on every open (spec 6).
const ShardFunc = "/repo/flatfs/shard/v1/next-to-last/3"

// Subdirectories of the store root.
const (
	blocksDir = "blocks"
	kvDir     = "kv"
)

// ShardMismatchError reports an existing store whose flatfs shard function is
// not the one bloar writes with. The layout on disk is not interpretable under
// the wrong shard function, so opening is refused rather than attempted.
type ShardMismatchError struct {
	Path string
	Want string
	Got  string
}

func (e *ShardMismatchError) Error() string {
	return fmt.Sprintf("store: %s was created with shard function %q, this build requires %q; refusing to open",
		e.Path, e.Got, e.Want)
}

// Store is the combined on-disk state rooted at a single directory: blocks
// under <path>/blocks, node-local KV under <path>/kv.
type Store struct {
	path string
	ds   *flatfs.Datastore
	// bs is the plain blockstore over ds, keyed by multihash. epochs owns the
	// validating wrapper over bs and hands application code a protection-aware
	// view through blocks. Collector code uses Epochs instead, retaining
	// validation without recording its own mark reads as application touches.
	bs     blockstore.Blockstore
	blocks blockstore.Blockstore
	epochs *BlockstoreEpochs
	kv     *pebble.DB

	closeOnce sync.Once
	closeErr  error
}

// options is the mutable part of an Open call.
type options struct {
	pebbleLogger pebble.Logger
}

// Option configures Open.
type Option func(*options)

// WithPebbleLogger routes Pebble's internal logging to l. Pebble logs its
// compactions and flushes unconditionally, and its default logger is the Go
// standard logger, so a daemon that does not set this gets Pebble's internals
// interleaved with its own output on stdout. Passing nil (or omitting the
// option) keeps Pebble's default.
func WithPebbleLogger(l pebble.Logger) Option {
	return func(o *options) { o.pebbleLogger = l }
}

// Open opens the store at path, creating it if it does not exist. An existing
// store whose shard function differs from ShardFunc is refused.
func Open(path string, opts ...Option) (*Store, error) {
	if path == "" {
		return nil, errors.New("store: path must not be empty")
	}
	var o options
	for _, opt := range opts {
		opt(&o)
	}
	if err := os.MkdirAll(path, 0o755); err != nil {
		return nil, fmt.Errorf("store: creating root %s: %w", path, err)
	}

	blocks := filepath.Join(path, blocksDir)
	if err := checkShardFunc(blocks); err != nil {
		return nil, err
	}

	shard, err := flatfs.ParseShardFunc(ShardFunc)
	if err != nil {
		return nil, fmt.Errorf("store: parsing shard function %q: %w", ShardFunc, err)
	}
	// Sync writes: a block that is durable only in the page cache would be
	// referenced by a published root we cannot re-derive after a crash.
	ds, err := flatfs.CreateOrOpen(blocks, shard, true)
	if err != nil {
		return nil, fmt.Errorf("store: opening blockstore at %s: %w", blocks, err)
	}

	// A nil Logger is Pebble's own default, so this is the unconfigured
	// behaviour when no option was passed.
	kv, err := pebble.Open(filepath.Join(path, kvDir), &pebble.Options{Logger: o.pebbleLogger})
	if err != nil {
		if cerr := ds.Close(); cerr != nil {
			err = errors.Join(err, cerr)
		}
		return nil, fmt.Errorf("store: opening kv at %s: %w", filepath.Join(path, kvDir), err)
	}

	// NoPrefix: boxo namespaces keys under /blocks by default, for repos that
	// mount several datastores in one keyspace. flatfs takes only single-component
	// keys, and here it owns <path>/blocks outright, so the directory is already
	// the namespace.
	bs := blockstore.NewBlockstore(ds, blockstore.NoPrefix())
	validated := Validating(bs)
	epochs := NewBlockstoreEpochs(validated, WithKeyIterator(flatFSAllKeys(ds)))
	// Unlike Boxo's AllKeysChan, the direct flatfs iterator preserves errors
	// discovered after enumeration begins. GC must never mistake a truncated
	// key stream for a complete sweep.
	return &Store{
		path: path,
		ds:   ds,
		bs:   bs,
		// Every consumer reads through the validating wrapper: it is the single
		// place bloar recomputes a block's hash on the way out of the store, so
		// altered local bytes cannot be served under a CID they no longer match
		//.
		blocks: epochs.Application(),
		epochs: epochs,
		kv:     kv,
	}, nil
}

// Locked reports whether the store at path is currently held by another process
// -- a running daemon, or another offline tool that has it open. It probes the
// KV directory's Pebble lock without opening the store: Pebble takes an
// exclusive OS lock on <path>/kv for the life of an open (flatfs takes only an
// in-process mutex, so the KV lock is what enforces cross-process exclusivity),
// and this acquires and immediately releases that same lock to answer the
// question a mutating offline command must ask first -- do I have exclusive
// ownership. A held lock reports true; a fresh path with no KV yet reports
// false, there being nothing to hold.
//
// It is advisory, not a mutex: another process may take the lock between this
// returning false and the caller opening the store, in which case the open
// fails as the backstop. That is fine for a human-run tool, which is the only
// caller -- `bloard fsck --repair` refuses rather than delete blocks out from
// under a live daemon.
func Locked(path string) (bool, error) {
	if path == "" {
		return false, errors.New("store: path must not be empty")
	}
	kv := filepath.Join(path, kvDir)
	if _, err := os.Stat(kv); errors.Is(err, os.ErrNotExist) {
		return false, nil // nothing created yet, so nothing to hold.
	}
	lock, err := pebble.LockDirectory(kv, vfs.Default)
	if err != nil {
		// The lock could not be taken: another process owns it. Any error here is
		// answered "held" -- this process does not have exclusive ownership, which
		// is the only thing the caller needs to know to refuse.
		return true, nil
	}
	if cerr := lock.Close(); cerr != nil {
		return false, fmt.Errorf("store: releasing probe lock at %s: %w", kv, cerr)
	}
	return false, nil
}

// checkShardFunc refuses a blocks directory created with a different shard
// function. flatfs would also catch this, but only as an opaque string
// mismatch buried in CreateOrOpen; callers need a typed error to distinguish
// "wrong store" from "broken store".
func checkShardFunc(blocks string) error {
	got, err := flatfs.ReadShardFunc(blocks)
	switch {
	case errors.Is(err, flatfs.ErrShardingFileMissing):
		return nil // not created yet; CreateOrOpen writes SHARDING.
	case errors.Is(err, os.ErrNotExist):
		return nil
	case err != nil:
		return fmt.Errorf("store: reading shard function at %s: %w", blocks, err)
	}
	if got.String() != ShardFunc {
		return &ShardMismatchError{Path: blocks, Want: ShardFunc, Got: got.String()}
	}
	return nil
}

// Path returns the store root.
func (s *Store) Path() string { return s.path }

// Blocks returns the blockstore. Keys are multihashes, as boxo defaults to, and
// every Get revalidates the block against the CID it was requested under (audit
// the safety boundary, see Validating): presence under a multihash is not proof of content.
func (s *Store) Blocks() blockstore.Blockstore { return s.blocks }

// Epochs returns the coordinator used by online GC and integrity scrubs. Its
// collector view bypasses application-touch tracking while retaining the same
// read-time CID validation as Blocks.
func (s *Store) Epochs() *BlockstoreEpochs { return s.epochs }

// KV returns the node-local KV. It is shared by several packages behind
// single-byte prefixes (spec 6): the blob catalog and pin ledger (catalog,
// spec 6.1/6.2), the head roots and manifest tips (server), the writer IPNS
// sequence (p2p), and the follower checkpoint/floors (follow). Not all of it is
// rebuildable from the DAG: the catalog and the reconciled pin ledger are
// (see bloard rebuild and pin reconciliation), but the head roots, manifest
// tips, IPNS sequence, follower anti-replay floors, and staging-pin leases are
// current-selection, monotonic-publication, anti-replay, or time-bearing state
// that no walk of an unordered blockstore can reconstruct.
func (s *Store) KV() *pebble.DB { return s.kv }

// CountPrefix returns the number of KV keys under the single-byte prefix b. It
// is a key-only ranged scan -- no value reads or decodes -- over
// [{b}, {b+1}), so its cost is O(keys in the range).
//
// It backs the store-growth gauges, which run it at GC cadence. It
// must NOT be called on a scrape: the catalog prefix alone is tens of millions
// of rows on a full-retention writer, and an O(n) scan per scrape is exactly the
// walk the flatfs blockstore has no gauge for either (docs/operations.md §2.4).
func (s *Store) CountPrefix(b byte) (uint64, error) {
	var upper []byte
	if b < 0xFF {
		upper = []byte{b + 1}
	}
	it, err := s.kv.NewIter(&pebble.IterOptions{
		LowerBound: []byte{b},
		UpperBound: upper,
		KeyTypes:   pebble.IterKeyTypePointsOnly,
	})
	if err != nil {
		return 0, fmt.Errorf("store: opening count iterator for prefix %q: %w", string(b), err)
	}
	defer it.Close()

	var n uint64
	for valid := it.First(); valid; valid = it.Next() {
		n++
	}
	if err := it.Error(); err != nil {
		return 0, fmt.Errorf("store: counting keys under prefix %q: %w", string(b), err)
	}
	return n, nil
}

// Close closes the KV first, then the blockstore, and is safe to call more
// than once.
//
// The KV is the referrer: its catalog and pin ledger name blocks. Closing it
// first means no KV write can name a block after the blockstore is gone. The
// converse order risks the opposite, and the asymmetry matters because the two
// stores fail differently: an unreferenced block is collectable garbage, while
// a catalog entry for a block that was never written is a dangling reference
// that only a rebuild can clear.
func (s *Store) Close() error {
	s.closeOnce.Do(func() {
		var errs []error
		if err := s.kv.Close(); err != nil {
			errs = append(errs, fmt.Errorf("store: closing kv: %w", err))
		}
		if err := s.ds.Close(); err != nil {
			errs = append(errs, fmt.Errorf("store: closing blockstore: %w", err))
		}
		s.closeErr = errors.Join(errs...)
	})
	return s.closeErr
}
