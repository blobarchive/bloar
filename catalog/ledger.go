package catalog

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/ipfs/go-cid"
)

// Ledger is the pin ledger of spec 6.2: the set of pins the daemon believes it
// holds, keyed by head and purpose.
//
// It is deliberately dumb. It records what it is told and reports it back; it
// has no opinion on which pins should exist, what a purpose means, or whether
// the pinner agrees with it. Reconciliation (spec 9) computes the desired set,
// diffs it against this ledger, and drives the pinner -- adding pins, then
// persisting here, then removing stale pins, so that a crash at any point
// leaves pins this ledger does not know about (collectable) rather than
// ledger rows naming pins that were never taken.
type Ledger struct {
	kv *pebble.DB
}

// NewLedger returns a Ledger over kv, which is store.Store.KV(). It shares the
// keyspace with Catalog; see the package comment for the layout.
func NewLedger(kv *pebble.DB) *Ledger { return &Ledger{kv: kv} }

// PinEntry is one ledger row: a pinned block, why it is pinned, whether the pin
// is recursive, and when it stops being a pin at all.
type PinEntry struct {
	Purpose   string
	CID       cid.Cid
	Recursive bool
	// Expiry is when the pin lapses, or the zero Time for a pin that does not.
	//
	// Almost every pin is the second kind: a pin exists because a policy asks
	// for it, and it goes away when the policy stops asking (spec 9). The
	// exception is the staging pin an ingest takes on a blob nobody references
	// yet -- there is no policy behind it and no reconciler pass that would ever
	// remove it, so an abandoned put would leak a pin forever. The expiry is
	// what makes such a pin self-limiting. Nothing here enforces it: this type
	// records what it is told (see Ledger), and pinning's GC is what drops the
	// rows that have lapsed.
	Expiry time.Time
}

// Expires reports whether e carries an expiry at all.
func (e PinEntry) Expires() bool { return !e.Expiry.IsZero() }

// Value flags. The remaining bits are zero and a reader must not assume they
// stay that way.
const (
	flagRecursive byte = 1 << 0
	// flagExpires marks a value that carries an 8-byte expiry after the flags.
	// A row written before this bit existed has neither the bit nor the bytes,
	// and decodes to a PinEntry with no expiry -- which is what it is.
	flagExpires byte = 1 << 1
)

// expirySize is the width of the encoded expiry: Unix seconds, big-endian.
// Seconds rather than nanoseconds because the thing it bounds is a TTL measured
// in hours (spec 12's ingest.staging_ttl), and big-endian because these bytes
// are read back by a scan that has no reason to care but every reason to be
// boring.
const expirySize = 8

// encodePinValue renders a row's value.
func encodePinValue(recursive bool, expiry time.Time) []byte {
	var flags byte
	if recursive {
		flags |= flagRecursive
	}
	if expiry.IsZero() {
		return []byte{flags}
	}
	flags |= flagExpires
	v := make([]byte, 1+expirySize)
	v[0] = flags
	binary.BigEndian.PutUint64(v[1:], uint64(expiry.Unix()))
	return v
}

// decodePinValue parses a row's value back.
func decodePinValue(val []byte) (recursive bool, expiry time.Time, err error) {
	if len(val) == 0 {
		return false, time.Time{}, nil
	}
	flags := val[0]
	recursive = flags&flagRecursive != 0
	if flags&flagExpires == 0 {
		return recursive, time.Time{}, nil
	}
	if len(val) < 1+expirySize {
		return false, time.Time{}, fmt.Errorf("catalog: pin ledger value claims an expiry but is %d bytes, want %d",
			len(val), 1+expirySize)
	}
	return recursive, time.Unix(int64(binary.BigEndian.Uint64(val[1:1+expirySize])), 0).UTC(), nil
}

// nameSep terminates the variable-length components of a ledger key.
const nameSep byte = 0x00

// checkName rejects the one thing that would make the key encoding ambiguous.
// Purposes are phase 6's vocabulary and heads are named by spec 3.1, so this
// package does not police their shape beyond what its own layout requires.
func checkName(kind, s string) error {
	if s == "" {
		return fmt.Errorf("catalog: %s must not be empty", kind)
	}
	if bytes.IndexByte([]byte(s), nameSep) >= 0 {
		return fmt.Errorf("catalog: %s %q must not contain a NUL byte", kind, s)
	}
	return nil
}

// headPrefix renders 'p' || head || 0x00: every pin of one head, any purpose.
func headPrefix(head string) []byte {
	k := make([]byte, 0, 1+len(head)+1)
	k = append(k, prefixLedger)
	k = append(k, head...)
	return append(k, nameSep)
}

// purposePrefix renders 'p' || head || 0x00 || purpose || 0x00.
func purposePrefix(head, purpose string) []byte {
	k := headPrefix(head)
	k = append(k, purpose...)
	return append(k, nameSep)
}

// ledgerKey renders a full row key.
func ledgerKey(head, purpose string, c cid.Cid) []byte {
	return append(purposePrefix(head, purpose), c.Bytes()...)
}

// Add records a pin that does not expire. It is idempotent, and re-adding a pin
// with a different recursive flag overwrites it: the ledger records the last
// thing it was told.
func (l *Ledger) Add(ctx context.Context, head, purpose string, c cid.Cid, recursive bool) error {
	return l.add(ctx, head, purpose, c, recursive, time.Time{})
}

// AddExpiring records a pin that lapses at expiry. See PinEntry.Expiry for what
// that means and who acts on it; a zero expiry is Add.
//
// It is the same write as Add, and overwrites the same way: a row re-added with
// a later expiry gets the later one. That is what makes a re-put of a blob whose
// staging pin is nearly due extend it rather than race it.
func (l *Ledger) AddExpiring(ctx context.Context, head, purpose string, c cid.Cid, recursive bool, expiry time.Time) error {
	return l.add(ctx, head, purpose, c, recursive, expiry)
}

func (l *Ledger) add(ctx context.Context, head, purpose string, c cid.Cid, recursive bool, expiry time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := checkNames(head, purpose); err != nil {
		return err
	}
	if !c.Defined() {
		return fmt.Errorf("catalog: refusing to pin an undefined CID under %q/%q", head, purpose)
	}
	if err := l.kv.Set(ledgerKey(head, purpose, c), encodePinValue(recursive, expiry), syncWrite); err != nil {
		return fmt.Errorf("catalog: recording pin %s under %q/%q: %w", c, head, purpose, err)
	}
	return nil
}

// AddBatch records every pin in one atomic, synced Pebble batch: one fsync
// rather than len(pins) of them.
//
// It exists for the ingest path, which stages a whole put's blobs at once (spec
// 9's window (a)) and is on the critical path of every indexer batch. The
// atomicity is a bonus rather than the point -- the rows are independent, and a
// caller that got half of them would be no worse off than one that got them
// one at a time -- but the fsync count is the difference between a put costing
// one disk round trip and costing 64.
func (l *Ledger) AddBatch(ctx context.Context, head string, pins []PinEntry) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(pins) == 0 {
		return nil
	}
	if err := checkName("head", head); err != nil {
		return err
	}
	b := l.kv.NewBatch()
	defer b.Close()
	for _, p := range pins {
		if err := checkName("purpose", p.Purpose); err != nil {
			return err
		}
		if !p.CID.Defined() {
			return fmt.Errorf("catalog: refusing to pin an undefined CID under %q/%q", head, p.Purpose)
		}
		if err := b.Set(ledgerKey(head, p.Purpose, p.CID), encodePinValue(p.Recursive, p.Expiry), nil); err != nil {
			return fmt.Errorf("catalog: staging pin %s under %q/%q: %w", p.CID, head, p.Purpose, err)
		}
	}
	if err := b.Commit(syncWrite); err != nil {
		return fmt.Errorf("catalog: committing %d pins under %q: %w", len(pins), head, err)
	}
	return nil
}

// RemoveBatch drops every named pin in one atomic, synced batch. It is the bulk
// form of Remove, and like it, removing a pin that is not recorded is not an
// error.
func (l *Ledger) RemoveBatch(ctx context.Context, head string, pins []PinEntry) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(pins) == 0 {
		return nil
	}
	if err := checkName("head", head); err != nil {
		return err
	}
	b := l.kv.NewBatch()
	defer b.Close()
	for _, p := range pins {
		if err := checkName("purpose", p.Purpose); err != nil {
			return err
		}
		if !p.CID.Defined() {
			return fmt.Errorf("catalog: refusing to unpin an undefined CID under %q/%q", head, p.Purpose)
		}
		if err := b.Delete(ledgerKey(head, p.Purpose, p.CID), nil); err != nil {
			return fmt.Errorf("catalog: staging removal of pin %s under %q/%q: %w", p.CID, head, p.Purpose, err)
		}
	}
	if err := b.Commit(syncWrite); err != nil {
		return fmt.Errorf("catalog: committing removal of %d pins under %q: %w", len(pins), head, err)
	}
	return nil
}

// Remove drops a pin from the ledger. Removing a pin that is not recorded is
// not an error: the caller's intent is that it be gone, and it is.
func (l *Ledger) Remove(ctx context.Context, head, purpose string, c cid.Cid) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := checkNames(head, purpose); err != nil {
		return err
	}
	if !c.Defined() {
		return fmt.Errorf("catalog: refusing to unpin an undefined CID under %q/%q", head, purpose)
	}
	if err := l.kv.Delete(ledgerKey(head, purpose, c), syncWrite); err != nil {
		return fmt.Errorf("catalog: removing pin %s under %q/%q: %w", c, head, purpose, err)
	}
	return nil
}

func checkNames(head, purpose string) error {
	if err := checkName("head", head); err != nil {
		return err
	}
	return checkName("purpose", purpose)
}

// List returns every pin recorded for one head and purpose, ordered by key.
func (l *Ledger) List(ctx context.Context, head, purpose string) ([]PinEntry, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := checkNames(head, purpose); err != nil {
		return nil, err
	}
	return l.scan(purposePrefix(head, purpose), len(headPrefix(head)))
}

// ListAll returns every pin recorded for one head across all purposes, ordered
// by key: purposes group together, and pins sort within a purpose.
func (l *Ledger) ListAll(ctx context.Context, head string) ([]PinEntry, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := checkName("head", head); err != nil {
		return nil, err
	}
	prefix := headPrefix(head)
	return l.scan(prefix, len(prefix))
}

// scan walks every key under prefix. purposeAt is the offset at which the
// purpose component begins, which both callers know statically.
func (l *Ledger) scan(prefix []byte, purposeAt int) ([]PinEntry, error) {
	it, err := l.kv.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: keyUpperBound(prefix),
	})
	if err != nil {
		return nil, fmt.Errorf("catalog: opening pin ledger iterator: %w", err)
	}
	defer it.Close()

	var out []PinEntry
	for it.First(); it.Valid(); it.Next() {
		e, err := decodePin(it.Key(), it.Value(), purposeAt)
		if err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	if err := it.Error(); err != nil {
		return nil, fmt.Errorf("catalog: scanning pin ledger: %w", err)
	}
	return out, nil
}

// decodePin parses one row back out of its key and value. The key bytes belong
// to the iterator and are invalidated by Next, so everything decoded out of
// them is copied.
func decodePin(key, val []byte, purposeAt int) (PinEntry, error) {
	rest := key[purposeAt:]
	sep := bytes.IndexByte(rest, nameSep)
	if sep < 0 {
		return PinEntry{}, fmt.Errorf("catalog: pin ledger key %q has no purpose terminator", key)
	}
	c, err := cid.Cast(rest[sep+1:])
	if err != nil {
		return PinEntry{}, fmt.Errorf("catalog: pin ledger key %q has an undecodable CID: %w", key, err)
	}
	recursive, expiry, err := decodePinValue(val)
	if err != nil {
		return PinEntry{}, fmt.Errorf("catalog: pin ledger key %q: %w", key, err)
	}
	return PinEntry{
		Purpose:   string(rest[:sep]),
		CID:       c,
		Recursive: recursive,
		Expiry:    expiry,
	}, nil
}
