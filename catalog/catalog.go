// Package catalog implements the two node-local KV structures of spec 6: the
// blob catalog (6.1), which maps a versioned hash to its blob block, and the
// pin ledger (6.2), which records the pins the daemon believes it holds.
//
// Both live in the single Pebble KV that store.Store exposes as KV(), so they
// share a keyspace and are separated by the one-byte prefixes documented below.
// Neither is a current-selection authority the way a head root is: the catalog
// is rebuilt by ingest.Rebuild and the reconciled part of the ledger by pin
// reconciliation. Not literally every byte is rebuildable, though: the ledger's
// staging rows (the reserved `_staging` head, spec 9) are time-bearing leases
// carrying a TTL expiry that reconciliation never touches, so a rebuild cannot
// reconstruct them -- losing them lapses the staging pins early rather than
// re-deriving their deadlines.
//
// # Key layout
//
// This is the only place the layout is written down. Both prefixes are single
// bytes, chosen so no key of one structure can ever be a prefix of a key of the
// other -- prefix scans of the ledger must not walk into catalog entries.
//
//	catalog  key: 'c' || vh[32]
//	         val: cid.Bytes()
//
//	ledger   key: 'p' || head || 0x00 || purpose || 0x00 || cid.Bytes()
//	         val: flags[1]                       (bit 0: recursive, bit 1: expires)
//	              || expiry[8]                   (iff bit 1; Unix seconds, big-endian)
//
// Catalog keys are fixed-width, so they need no separator. Ledger keys are
// built from variable-length components and terminate each with 0x00; head and
// purpose are rejected if they contain 0x00, which is what makes the encoding
// unambiguous and the prefix scans exact (a scan for head "a" cannot reach head
// "ab", because the former's prefix ends in the 0x00 the latter has a 'b' in).
// The CID needs no terminator: it is last, and self-delimiting besides.
//
// The expiry is optional in the encoding rather than always present because
// almost no pin has one: a value written before the bit existed is one byte
// long and decodes to a pin that does not expire, which is what it is. See
// PinEntry.Expiry.
//
// # The reserved head
//
// One head name in the ledger is not a head: pinning.StagingHead, under which
// ingest records the blobs it has accepted but nobody references yet (spec 9).
// It is spelled with a leading underscore, which spec 3.1's head-name grammar
// ([a-z0-9][a-z0-9-]*) cannot produce, so it can never collide with a real
// head's rows. This package does not know about it -- it records what it is
// told -- but the layout above is why the reservation works, and pinning
// asserts the grammar in a test rather than trusting this comment.
//
// # Durability
//
// Every write here is a Pebble sync write. The catalog is written after blob
// verification and read by apply_refs, which will refuse a batch whose blobs it
// cannot resolve; the ledger is the crash-safe record reconciliation diffs
// against. Neither can afford to lose an acknowledged write to a page cache.
package catalog

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble/v2"
	"github.com/ipfs/go-cid"

	"github.com/blobarchive/bloar/schema"
)

// Key prefixes. See the package comment for the full layout.
const (
	prefixCatalog byte = 'c'
	prefixLedger  byte = 'p'
)

// syncWrite is the write option every mutation here uses. See the package
// comment.
var syncWrite = pebble.Sync

// Catalog is the blob catalog of spec 6.1: vh -> blob CID. It implements
// archive.BlobResolver.
//
// Entries may outlive their blocks, because GC does not update the catalog.
// A resolved CID is therefore not proof the block is present, and callers that
// need the bytes (apply_refs) check the blockstore separately.
type Catalog struct {
	kv *pebble.DB
}

// New returns a Catalog over kv, which is store.Store.KV().
func New(kv *pebble.DB) *Catalog { return &Catalog{kv: kv} }

// Entry is one catalog row.
type Entry struct {
	VH   schema.VersionedHash
	Blob cid.Cid
}

// catalogKey renders 'c' || vh.
func catalogKey(vh schema.VersionedHash) []byte {
	k := make([]byte, 1+schema.VersionedHashSize)
	k[0] = prefixCatalog
	copy(k[1:], vh[:])
	return k
}

// Put upserts vh -> blob. It is idempotent: re-putting the same pair is a
// no-op in effect, and re-putting a different CID for the same vh overwrites,
// which cannot happen for honestly-derived entries (vh commits to the blob
// bytes, which the CID also commits to).
func (c *Catalog) Put(ctx context.Context, vh schema.VersionedHash, blob cid.Cid) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !blob.Defined() {
		return fmt.Errorf("catalog: refusing to store an undefined CID for vh 0x%x", vh[:])
	}
	if err := c.kv.Set(catalogKey(vh), blob.Bytes(), syncWrite); err != nil {
		return fmt.Errorf("catalog: writing vh 0x%x: %w", vh[:], err)
	}
	return nil
}

// PutBatch upserts every entry in one atomic, synced Pebble batch. It is the
// bulk form of Put, for callers with many entries in hand (ingest.Rebuild); a
// batch is one fsync rather than len(entries) of them.
func (c *Catalog) PutBatch(ctx context.Context, entries []Entry) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(entries) == 0 {
		return nil
	}
	b := c.kv.NewBatch()
	defer b.Close()
	for _, e := range entries {
		if !e.Blob.Defined() {
			return fmt.Errorf("catalog: refusing to store an undefined CID for vh 0x%x", e.VH[:])
		}
		if err := b.Set(catalogKey(e.VH), e.Blob.Bytes(), nil); err != nil {
			return fmt.Errorf("catalog: staging vh 0x%x: %w", e.VH[:], err)
		}
	}
	if err := b.Commit(syncWrite); err != nil {
		return fmt.Errorf("catalog: committing %d entries: %w", len(entries), err)
	}
	return nil
}

// ResolveBlob returns the blob CID for vh. A vh that is not in the catalog is
// (cid.Undef, false, nil): absence is an answer, not a failure.
func (c *Catalog) ResolveBlob(ctx context.Context, vh schema.VersionedHash) (cid.Cid, bool, error) {
	if err := ctx.Err(); err != nil {
		return cid.Undef, false, err
	}
	v, closer, err := c.kv.Get(catalogKey(vh))
	if errors.Is(err, pebble.ErrNotFound) {
		return cid.Undef, false, nil
	}
	if err != nil {
		return cid.Undef, false, fmt.Errorf("catalog: reading vh 0x%x: %w", vh[:], err)
	}
	defer closer.Close()

	blob, err := cid.Cast(v)
	if err != nil {
		return cid.Undef, false, fmt.Errorf("catalog: vh 0x%x has an undecodable CID: %w", vh[:], err)
	}
	return blob, true, nil
}

// Clear deletes every catalog entry, leaving the pin ledger untouched. It
// exists for the rebuild path: an operator who believes the catalog is wrong
// rather than merely incomplete wants it gone before the walk, since
// ingest.Rebuild only ever upserts and so cannot retract a bad row.
func (c *Catalog) Clear(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	lo := []byte{prefixCatalog}
	if err := c.kv.DeleteRange(lo, keyUpperBound(lo), syncWrite); err != nil {
		return fmt.Errorf("catalog: clearing: %w", err)
	}
	return nil
}

// keyUpperBound returns the exclusive end of the range of keys prefixed by
// prefix: the prefix with its last non-0xFF byte incremented and everything
// after it dropped. A prefix of all 0xFF has no upper bound, and nil means
// "unbounded" to Pebble; no prefix this package builds is all-0xFF.
func keyUpperBound(prefix []byte) []byte {
	end := bytes.Clone(prefix)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] < 0xFF {
			end[i]++
			return end[:i+1]
		}
	}
	return nil
}
