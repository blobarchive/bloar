package follow

import (
	"context"
	"fmt"
	"time"

	"github.com/ipfs/boxo/blockstore"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"

	"github.com/blobarchive/bloar/ingest"
	"github.com/blobarchive/bloar/schema"
	"github.com/blobarchive/bloar/server"
)

// blobs is one followed head's blob read path: server.Blobs, implemented as
// spec 11.4's read miss.
//
// A lookup that resolved in the index has produced a RefEntry, and the block it
// names is either local (fetched by a policy that retains it, or by an earlier
// read) or it is not (outside the window, or a backfill that has not reached it
// yet). Both are ordinary, and the difference is invisible from here on purpose:
// the fetching blockstore turns the second into the first, bounded by
// follow.fetch_timeout, and a fetch that does not land is a *p2p.FetchError the
// HTTP layer answers 503 -- never a 404, which would tell a client the archive
// knows there is no such blob.
//
// What is fetched here is cached and unpinned (spec 11.4): the write-through is
// the blockstore's, and nothing adds a pin, so a blob a window policy does not
// retain is served now and swept by the next GC. That is exactly right. A
// follower that pinned what it served would have a retention policy written by
// whoever queries it.
type blobs struct {
	f    *Follower
	head string
}

var _ server.Blobs = (*blobs)(nil)

// Blob returns the bytes of e.Blob, fetching them if this node has not got them.
func (b *blobs) Blob(ctx context.Context, e schema.RefEntry) ([]byte, error) {
	blk, err := b.f.blocks.Get(ctx, e.Blob)
	if err != nil {
		return nil, err
	}
	if b.f.cfg.Verify == VerifyFull {
		if err := b.f.verifyBlob(b.head, e, blk.RawData()); err != nil {
			return nil, err
		}
	}
	return blk.RawData(), nil
}

// verifyBlob is follow.verify: full (spec 11.4). It recomputes the blob's KZG
// commitment, derives the versioned hash, and checks it against the entry the
// blob was fetched to satisfy.
//
// # What this catches that content addressing does not
//
// Everything else in bloar is a hash of itself. A Segment's CID covers its
// bytes; a blob's CID covers its bytes; bitswap checks both on arrival, and a
// writer cannot forge either. The one thing not covered is the binding between
// them: a RefEntry says "versioned hash V is at blob C", and both halves can be
// perfectly well-formed while the claim is false. Nothing about C's bytes proves
// KZG(C) is V, because V comes from a commitment scheme the CID knows nothing
// about.
//
// So this is the only check in the follower that can fail on data rather than on
// structure, and a failure is not a corrupt block -- it is a writer stating
// something untrue about the chain, signed. Hence quarantine rather than a skip:
// the signature vouches for the completeness and freshness of a head (spec
// 11.4), and a head whose writer will assert a wrong vh is a head nothing about
// this node's copy makes trustworthy.
//
// # Why this runs on every serve, not only in the background sync
//
// Spec 11.4 puts the check on every blob served. The ordinary sync also verifies
// every RefEntry in a Segment and persists a successful proof for sealed Segment
// CIDs, but that is a backfill optimization, not authority to skip this boundary.
// A failed check records no proof and quarantine itself is process-local, so a
// restart can Resume a checkpoint before background sync encounters the bad
// entry again. A blob can also be local without this follower ever fetching it:
// a node that writes one head and follows another shares one blockstore. The
// response path therefore verifies the exact RefEntry it is about to serve.
//
// The cost is a commitment per blob served, which is milliseconds of CPU on a
// mode that exists to spend them: cid is the default, and a node running full is
// a node that has said it would rather pay than trust.
func (f *Follower) verifyBlob(head string, e schema.RefEntry, data []byte) error {
	got, err := ingest.VersionedHash(data)
	if err != nil {
		return f.quarantine(head, "the blob at %s, which the index binds to versioned hash %s, is not a valid KZG "+
			"blob: %v", e.Blob, vhHex(e.VH), err)
	}
	if got != e.VH {
		return f.quarantine(head, "the blob at %s has versioned hash %s, but the index entry it was fetched for "+
			"binds that block to %s. The block's bytes hash to its CID, so this is not corruption in transit: it is "+
			"the writer's index asserting a versioned hash the blob does not have",
			e.Blob, vhHex(got), vhHex(e.VH))
	}
	return nil
}

// vhHex renders a versioned hash the way the API and the logs always state one.
func vhHex(vh schema.VersionedHash) string { return fmt.Sprintf("0x%x", vh[:]) }

// bounded is a blockstore whose every read is given follow.fetch_timeout to
// answer (spec 11.4).
//
// # Why the bound is here and not at the callers
//
// It is one knob with two jobs, and both need it in the same place. On the read
// path it is spec 11.4's: a client waiting on a blob gets an answer or a 503
// within five seconds, rather than waiting for whatever the request's own
// context allows. On maintenance paths it bounds ordinary sync, the rare
// active-epoch adoption/Resume closure, reconciliation during the T0 cut, and a
// follower GC's self-heal. Online mark and closure fetches normally hold no Gate;
// a fetched-block-plus-staging operation holds it only as one cut-linearized
// transition, while the legacy fallback may hold it for a complete walk. The
// per-block deadline prevents an unreachable peer from parking any of those
// paths indefinitely.
//
// The bound is per call, which is what p2p.FetchingBlockstore's two contexts are
// about: the session lives as long as the follower, and this is the deadline on
// one block. Every call is bounded and not only the ones that miss, because a
// local read that takes five seconds is a disk that has its own problem.
type bounded struct {
	inner   blockstore.Blockstore
	timeout time.Duration
}

var _ blockstore.Blockstore = bounded{}

func (b bounded) Get(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	ctx, cancel := context.WithTimeout(ctx, b.timeout)
	defer cancel()
	return b.inner.Get(ctx, c)
}

func (b bounded) Has(ctx context.Context, c cid.Cid) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, b.timeout)
	defer cancel()
	return b.inner.Has(ctx, c)
}

func (b bounded) GetSize(ctx context.Context, c cid.Cid) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, b.timeout)
	defer cancel()
	return b.inner.GetSize(ctx, c)
}

// The write half is local and unbounded: nothing here reaches the network, and
// a deadline on a disk write would only ever fire on a disk that is already
// failing every read too.
func (b bounded) Put(ctx context.Context, blk blocks.Block) error { return b.inner.Put(ctx, blk) }

func (b bounded) PutMany(ctx context.Context, blks []blocks.Block) error {
	return b.inner.PutMany(ctx, blks)
}

func (b bounded) DeleteBlock(ctx context.Context, c cid.Cid) error {
	return b.inner.DeleteBlock(ctx, c)
}

func (b bounded) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	// Preserve the local application view's whole-enumeration lifecycle rule: an
	// enumeration begun while idle delays Begin until drained/cancelled, while one
	// attempted during an epoch is refused. Per-key read deadlines cannot safely
	// turn this channel-valued operation into independent protected calls.
	return b.inner.AllKeysChan(ctx)
}
