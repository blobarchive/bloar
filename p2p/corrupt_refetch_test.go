package p2p_test

// This regression test covers the follower half of the safety boundary's recovery: a
// corrupt local block is refused on read rather than silently refetched, and
// deleting it (the operator's repair) turns it into the miss the fetching
// blockstore heals from a peer. It lives here, in the package that owns the
// fetching blockstore, rather than in follow/.

import (
	"context"
	"errors"
	"testing"
	"time"

	blocks "github.com/ipfs/go-block-format"

	"github.com/blobarchive/bloar/p2p"
	"github.com/blobarchive/bloar/store"
)

func TestFollowerRefetchesAfterCorruptionIsDeleted(t *testing.T) {
	server, client := newTestHost(t), newTestHost(t)
	connect(t, client, server)

	// Only the server still has the honest bytes.
	honest := rawBlock(t, []byte("the honest bytes only the server still has"))
	serverBlocks := memBlocks()
	putBlock(t, serverBlocks, honest)
	newTestExchange(t, server, serverBlocks)

	// The client holds a corrupt copy under the same CID, behind the validating
	// store every local read goes through (as store.Blocks() layers it).
	clientPlain := memBlocks()
	corrupt, err := blocks.NewBlockWithCid([]byte("corrupt local bytes, wrong hash, same key"), honest.Cid())
	if err != nil {
		t.Fatalf("framing corrupt block: %v", err)
	}
	putBlock(t, clientPlain, corrupt)
	clientLocal := store.Validating(clientPlain)
	clientExchange := newTestExchange(t, client, clientLocal)

	bs := p2p.FetchingBlockstore(t.Context(), clientLocal, clientExchange)

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	// A local hit that fails validation is refused, not refetched: the fetching
	// store passes the corruption error through because it is not a not-found, so
	// corruption is surfaced rather than laundered into a silent network fetch.
	if _, err := bs.Get(ctx, honest.Cid()); !errors.Is(err, store.ErrCorruptBlock) {
		t.Fatalf("Get of a corrupt local block: got %v, want ErrCorruptBlock", err)
	}

	// The operator's repair: delete the corrupt block, turning it into a miss.
	if err := bs.DeleteBlock(ctx, honest.Cid()); err != nil {
		t.Fatalf("deleting the corrupt block: %v", err)
	}

	// Now the same read is a local miss, and the fetching store heals it from the
	// peer -- the genuine self-heal the recovery documentation claims.
	got, err := bs.Get(ctx, honest.Cid())
	if err != nil {
		t.Fatalf("refetch after deletion: %v", err)
	}
	if string(got.RawData()) != string(honest.RawData()) {
		t.Error("the refetched block is not the honest bytes")
	}
	// And it reads again locally, now validating cleanly.
	again, err := bs.Get(ctx, honest.Cid())
	if err != nil {
		t.Fatalf("re-reading the healed block: %v", err)
	}
	if string(again.RawData()) != string(honest.RawData()) {
		t.Error("the healed block did not validate on the next read")
	}
}
