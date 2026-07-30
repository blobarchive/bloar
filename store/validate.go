package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/ipfs/boxo/blockstore"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
)

// ErrCorruptBlock is the sentinel a validated read returns when a stored block's
// bytes no longer hash to the CID they were requested under. It
// is deliberately distinct from every other read outcome the daemon maps: not
// ipld.ErrNotFound -- the block is present, its bytes are simply wrong -- and
// not p2p.FetchError -- nothing was fetched. The public read path answers it
// 500, where a missing blob is 404 and a failed fetch is 503, and an operator
// repairs it with `bloard fsck` (see docs/operations.md).
var ErrCorruptBlock = errors.New("store: stored block failed CID validation")

// CorruptError names a specific block whose stored bytes no longer reproduce its
// CID. Want is the CID the read asked for; Got is the CID those bytes actually
// hash to under Want's prefix. The two share a version, codec, hash function and
// length -- all taken from Want -- and differ only in the multihash digest,
// which is the whole of the evidence that the bytes changed under a fixed key.
type CorruptError struct {
	Want cid.Cid
	Got  cid.Cid
}

func (e *CorruptError) Error() string {
	return fmt.Sprintf("store: block %s failed validation: stored bytes hash to %s", e.Want, e.Got)
}

// Is reports true for ErrCorruptBlock, so callers match the class with
// errors.Is without depending on this concrete type.
func (e *CorruptError) Is(target error) bool { return target == ErrCorruptBlock }

// validatingBlockstore recomputes a block's multihash on every Get and refuses
// bytes that no longer reproduce the requested CID.
//
// flatfs keys blocks by multihash, so a Get loads whatever bytes live under the
// requested CID's multihash and boxo's ordinary blockstore returns them wrapped
// in the *requested* CID without ever re-deriving it (blockstore.Get frames the
// result with NewBlockWithCid(bytes, k), the caller's k). That is the hole audit
// the safety boundary walked through: bytes altered in place on disk are served under the CID
// they no longer match. This wrapper closes it by re-summing the returned bytes
// under the requested CID's prefix. Version, codec, hash function and length all
// come from the request, so only the digest can differ, and a difference is
// exactly a byte that changed under a fixed key. Identity CIDs (whose digest is
// the bytes) and codec-carrying CIDs both fall out correctly: the sum is taken
// under the same prefix the request named, so the codec always matches and only
// the content is under test.
//
// # Why validation sits here and nowhere higher
//
// Every local read path in the daemon -- the beacon read API, the head engine's
// index-node loads, GC's mark, offline fsck -- reads through the blockstore
// store.Blocks() hands out, so wrapping it once validates all of them. It sits
// *below* the follower's fetching blockstore (p2p.FetchingBlockstore layers over
// this one), which is the boundary that matters: a local hit is validated here,
// and a local miss that bitswap fetches is returned straight from the exchange
// -- which bitswap already verified against the requested CID on receipt --
// without a second hash through this wrapper. So blocks arriving over the
// network are not double-validated, and a corrupt local block surfaces as
// ErrCorruptBlock rather than being silently refetched: a follower's repair is
// an explicit delete, which turns the corrupt block into the miss the fetching
// store then heals from a peer.
type validatingBlockstore struct {
	blockstore.Blockstore
}

func (v validatingBlockstore) Get(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	blk, err := v.Blockstore.Get(ctx, c)
	if err != nil {
		return nil, err
	}
	got, err := c.Prefix().Sum(blk.RawData())
	if err != nil {
		return nil, fmt.Errorf("store: rehashing block %s for validation: %w", c, err)
	}
	if !got.Equals(c) {
		return nil, &CorruptError{Want: c, Got: got}
	}
	return blk, nil
}

// Validating wraps bs so that every Get revalidates the block against the CID it
// was requested under; every other method is bs's own. Only reads can serve
// bytes corrupted after they were written -- a Put is content-addressed by the
// caller and a delete removes rather than reads -- so only reads are checked.
//
// store.Blocks() returns the store's blockstore wrapped in this. It is exported
// so tests and benchmarks can wrap an arbitrary blockstore to exercise or
// measure the validated path against the plain one.
func Validating(bs blockstore.Blockstore) blockstore.Blockstore {
	return validatingBlockstore{Blockstore: bs}
}
