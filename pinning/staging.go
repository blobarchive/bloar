package pinning

// This file closes spec 9's known window (a): a blob is ingested by one request
// and referenced by another, and a GC in between used to sweep it.

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ipfs/go-cid"

	"github.com/blobarchive/bloar/archive"
	"github.com/blobarchive/bloar/catalog"
)

// StagingHead is the ledger head name staging pins are filed under (spec 6.2's
// keyspace). It is not a head: no engine has this name, no policy covers it,
// and the reconciler refuses to register it.
//
// # Why a reserved head rather than a new keyspace
//
// The mark set is "every row of the pin ledger this node cares about" (GC.pins),
// and a staging pin is a pin: it retains a block, it is dropped when it stops
// being needed, and GC must mark from it. Giving it its own KV prefix would mean
// a second thing GC has to remember to read, which is a second thing a later
// change can forget. Filing it in the ledger makes it a row like any other, and
// the only code that has to know it is special is the code that must not touch
// it (the reconciler) and the code that expires it (GC).
//
// # Why this name cannot collide
//
// Spec 3.1's head names match [a-z0-9][a-z0-9-]*: lower-case alphanumerics and
// hyphens, and the first character cannot be a hyphen. An underscore is not in
// the grammar at all, so no configured head, no head in a publication document,
// and no head any writer could build can ever be called this. schema.Head's
// validation is what enforces that, and TestStagingHeadIsNotAValidHeadName
// asserts the two agree rather than leaving it to this comment.
const StagingHead = "_staging"

// PurposeStaging is the purpose staging pins are filed under. See the package
// comment for the other four.
//
//	staging  a blob accepted by POST /bloar/v1/blobs whose refs have not been
//	         applied yet. Always direct -- a blob is a leaf (spec 2), so there is
//	         nothing for a recursive pin to reach. Always carries an expiry, so
//	         that a put nobody ever references stops retaining its blobs.
const PurposeStaging = "staging"

// DefaultStagingTTL is how long a staging pin lives if the caller does not say.
// Spec 12's ingest.staging_ttl.
//
// A day is chosen against what it has to survive and what it costs. What it has
// to survive is the gap between an indexer's blobs POST and its refs POST, which
// is milliseconds in the normal case and, in the bad case, however long the
// indexer takes to retry after a crash between the two. What it costs is disk:
// an abandoned put retains its blobs for this long, so a beacon indexer that
// died mid-batch strands at most max_put_blobs * 128 KiB (8 MiB at the default)
// for a day. The asymmetry is the point -- the cost is bounded and small, and
// the failure it prevents is a sweep of blobs an indexer has already been told
// it can reference.
const DefaultStagingTTL = 24 * time.Hour

// Staging is the staging-pin store: ingest's Pin, the server's DropRefs, and
// GC's expiry pass, over the reserved head above.
//
// It is safe for concurrent use, and every write is idempotent: a re-put of a
// blob whose row already exists rewrites it with a fresh expiry (see
// catalog.Ledger.AddExpiring), and a drop of a row that is not there is not an
// error.
type Staging struct {
	ledger   *catalog.Ledger
	resolver archive.BlobResolver
	ttl      time.Duration
	// now is the clock, so that the TTL is testable without sleeping.
	now func() time.Time
}

// StagingConfig is what a Staging needs.
type StagingConfig struct {
	// Ledger is the pin ledger of spec 6.2. Required.
	Ledger *catalog.Ledger
	// Resolver maps a versioned hash to its blob CID (spec 6.1). Required:
	// staging rows are keyed by CID, because a pin is a statement about a block,
	// and DropRefs is handed the versioned hashes a batch named, because that is
	// what spec 7.2's refs body carries. catalog.Catalog is the one.
	Resolver archive.BlobResolver
	// TTL is spec 12's ingest.staging_ttl. Zero is DefaultStagingTTL.
	TTL time.Duration
	// Now is the clock. Optional; nil is time.Now.
	Now func() time.Time
}

// NewStaging returns a Staging over cfg.
func NewStaging(cfg StagingConfig) (*Staging, error) {
	if cfg.Ledger == nil {
		return nil, errors.New("pinning: StagingConfig.Ledger must not be nil")
	}
	if cfg.Resolver == nil {
		return nil, errors.New("pinning: StagingConfig.Resolver must not be nil")
	}
	if cfg.TTL < 0 {
		return nil, fmt.Errorf("pinning: StagingConfig.TTL is %s, must not be negative", cfg.TTL)
	}
	s := &Staging{ledger: cfg.Ledger, resolver: cfg.Resolver, ttl: cfg.TTL, now: cfg.Now}
	if s.ttl == 0 {
		s.ttl = DefaultStagingTTL
	}
	if s.now == nil {
		s.now = time.Now
	}
	return s, nil
}

// TTL returns the configured staging TTL.
func (s *Staging) TTL() time.Duration { return s.ttl }

// Pin records a direct, expiring staging pin per CID. It implements
// ingest.Staging.
//
// It returns only once the rows are durable, which is the whole contract: the
// caller is about to answer 200 to a blobs POST, and the promise that answer
// makes is that the blobs will still be there when the refs arrive.
func (s *Staging) Pin(ctx context.Context, cids []cid.Cid) error {
	if len(cids) == 0 {
		return nil
	}
	expiry := s.now().Add(s.ttl)
	pins := make([]catalog.PinEntry, 0, len(cids))
	for _, c := range cids {
		pins = append(pins, catalog.PinEntry{Purpose: PurposeStaging, CID: c, Expiry: expiry})
	}
	if err := s.ledger.AddBatch(ctx, StagingHead, pins); err != nil {
		return fmt.Errorf("pinning: staging %d blobs: %w", len(cids), err)
	}
	return nil
}

// DropRefs drops the staging pins of every blob the rows name. It implements
// server.Staging, and is called once a batch's root is durable.
//
// A versioned hash that does not resolve is skipped rather than failed on. The
// rows have just been applied, so every one of them resolved a moment ago
// inside apply_refs (spec 5.1 step 4) -- but this runs after that, outside the
// engine, and the one thing it must not do is turn a successful mutation into an
// error over a pin that was going to expire anyway.
func (s *Staging) DropRefs(ctx context.Context, rows []archive.RefRow) error {
	var pins []catalog.PinEntry
	for _, row := range rows {
		for _, vh := range row.VHs {
			c, ok, err := s.resolver.ResolveBlob(ctx, vh)
			if err != nil {
				return fmt.Errorf("pinning: resolving 0x%x to drop its staging pin: %w", vh[:], err)
			}
			if !ok {
				continue
			}
			pins = append(pins, catalog.PinEntry{Purpose: PurposeStaging, CID: c})
		}
	}
	if err := s.ledger.RemoveBatch(ctx, StagingHead, pins); err != nil {
		return fmt.Errorf("pinning: dropping %d staging pins: %w", len(pins), err)
	}
	return nil
}

// Drop removes the staging pins on cids. It is the follower's handoff (spec
// 11.3's fetch pass), the counterpart to DropRefs for ingest: a fetch pass
// stages every block it makes durable so a GC cannot sweep it before the head's
// pins land, and drops those rows once they have -- the adopted root is durable
// and registered, and a GC reconciles every head before it marks, so the head's
// own pins retain the blocks from here.
//
// It drops by CID rather than by versioned hash because a fetch pass stages
// index nodes as well as blobs, and an index node has no versioned hash. A CID
// with no staging row is not an error (RemoveBatch tolerates it).
func (s *Staging) Drop(ctx context.Context, cids []cid.Cid) error {
	if len(cids) == 0 {
		return nil
	}
	pins := make([]catalog.PinEntry, 0, len(cids))
	for _, c := range cids {
		pins = append(pins, catalog.PinEntry{Purpose: PurposeStaging, CID: c})
	}
	if err := s.ledger.RemoveBatch(ctx, StagingHead, pins); err != nil {
		return fmt.Errorf("pinning: dropping %d fetch-pass staging pins: %w", len(cids), err)
	}
	return nil
}

// List returns every staging row.
func (s *Staging) List(ctx context.Context) ([]catalog.PinEntry, error) {
	return s.ledger.ListAll(ctx, StagingHead)
}

// DropExpired removes every staging row whose expiry has passed, and returns how
// many it removed. It is GC's pre-mark pass; see GC.Run.
//
// The rows are read and then removed in one batch rather than removed as they
// are found, so that the scan is not walking an iterator over a keyspace it is
// deleting from.
func (s *Staging) DropExpired(ctx context.Context) (int, error) {
	rows, err := s.List(ctx)
	if err != nil {
		return 0, fmt.Errorf("pinning: listing staging pins: %w", err)
	}
	now := s.now()
	var expired []catalog.PinEntry
	for _, r := range rows {
		// A staging row with no expiry is not something this package writes; if
		// one is there, it is from a build that is not this one, and dropping it
		// on a guess is how a collector deletes live data. It stays, and the
		// operator has a row they can see.
		if r.Expires() && !r.Expiry.After(now) {
			expired = append(expired, r)
		}
	}
	if err := s.ledger.RemoveBatch(ctx, StagingHead, expired); err != nil {
		return 0, fmt.Errorf("pinning: dropping %d expired staging pins: %w", len(expired), err)
	}
	return len(expired), nil
}

// checkStagingName rejects the reserved head.
//
// It has nothing to catch today, and that is the point: registering a head means
// handing over a *archive.Head, archive.New and archive.Load both validate the
// name against spec 3.1's grammar, and the grammar cannot spell this one. The
// reconciler therefore cannot be given the staging rows to reconcile -- which it
// must not be, since a pass over them would compute a desired set with no
// staging pins in it and remove every row, and the next GC would sweep every
// blob that had been put and not yet referenced.
//
// So this is the braces to that belt: cheap, and here for the day someone adds a
// seam that builds a head some other way.
// TestReconcilerCannotBeHandedTheReservedHead is what asserts the belt still
// holds.
func checkStagingName(name string) error {
	if name == StagingHead {
		return fmt.Errorf("pinning: %q is the reserved staging head (spec 9's window (a)); it is not a head and "+
			"must never be registered for reconciliation", StagingHead)
	}
	return nil
}
