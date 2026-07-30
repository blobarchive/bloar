package store_test

import (
	"context"
	"errors"
	"testing"

	"github.com/ipfs/boxo/blockstore"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	datastore "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	ipld "github.com/ipfs/go-ipld-format"
	"github.com/multiformats/go-multihash"

	"github.com/blobarchive/bloar/store"
)

// mem returns an in-memory blockstore, for exercising the validating wrapper
// without a flatfs directory.
func mem() blockstore.Blockstore {
	return blockstore.NewBlockstore(dssync.MutexWrap(datastore.NewMapDatastore()), blockstore.NoPrefix())
}

// cidOver returns the CID of data under the given codec (raw blobs and dag-cbor
// index nodes are the two bloar uses, spec 2).
func cidOver(t *testing.T, codec uint64, data []byte) cid.Cid {
	t.Helper()
	c, err := cid.Prefix{Version: 1, Codec: codec, MhType: multihash.SHA2_256, MhLength: -1}.Sum(data)
	if err != nil {
		t.Fatalf("hashing: %v", err)
	}
	return c
}

// corrupt stores bad bytes under c's key: blockstore.Put keys by multihash and
// NewBlockWithCid does not verify (the u.Debug check is off), so this is exactly
// what a byte altered in place on disk looks like -- the wrong bytes under the
// right key, which is the whole of the safety boundary.
func corrupt(t *testing.T, bs blockstore.Blockstore, c cid.Cid, bad []byte) {
	t.Helper()
	blk, err := blocks.NewBlockWithCid(bad, c)
	if err != nil {
		t.Fatalf("framing corrupt block: %v", err)
	}
	if err := bs.Put(context.Background(), blk); err != nil {
		t.Fatalf("storing corrupt block: %v", err)
	}
}

func TestValidatingGetRefusesCorruptRawBlock(t *testing.T) {
	ctx := context.Background()
	bs := store.Validating(mem())

	honest := []byte("the bytes a raw blob CID commits to")
	c := cidOver(t, cid.Raw, honest)
	corrupt(t, bs, c, []byte("different bytes entirely, same key"))

	_, err := bs.Get(ctx, c)
	if !errors.Is(err, store.ErrCorruptBlock) {
		t.Fatalf("Get of a corrupt raw block: got %v, want ErrCorruptBlock", err)
	}
	var ce *store.CorruptError
	if !errors.As(err, &ce) {
		t.Fatalf("error is not a *store.CorruptError: %v (%T)", err, err)
	}
	if !ce.Want.Equals(c) {
		t.Errorf("CorruptError.Want = %s, want %s", ce.Want, c)
	}
	if ce.Got.Equals(c) {
		t.Error("CorruptError.Got equals the requested CID; the bytes should hash to something else")
	}
	// The distinct-class requirement: corruption is neither a miss nor a fetch
	// failure, and the read path relies on telling them apart.
	if ipld.IsNotFound(err) {
		t.Error("a corrupt block must not read as not-found")
	}
}

func TestValidatingGetRefusesCorruptDagCBORBlock(t *testing.T) {
	ctx := context.Background()
	bs := store.Validating(mem())

	// The wrapper hashes bytes, not structure, so the "node" need not be valid
	// CBOR to prove a dag-cbor CID's bytes are checked the same as a raw one's.
	honest := []byte("a dag-cbor index node's committed bytes")
	c := cidOver(t, cid.DagCBOR, honest)
	corrupt(t, bs, c, []byte("index node bytes rewritten under the same key"))

	_, err := bs.Get(ctx, c)
	if !errors.Is(err, store.ErrCorruptBlock) {
		t.Fatalf("Get of a corrupt dag-cbor block: got %v, want ErrCorruptBlock", err)
	}
}

func TestValidatingGetPassesHonestBlock(t *testing.T) {
	ctx := context.Background()
	inner := mem()
	bs := store.Validating(inner)

	honest := []byte("bytes that hash to the CID they are stored under")
	c := cidOver(t, cid.Raw, honest)
	blk, err := blocks.NewBlockWithCid(honest, c)
	if err != nil {
		t.Fatalf("framing block: %v", err)
	}
	if err := bs.Put(ctx, blk); err != nil {
		t.Fatalf("Put: %v", err)
	}

	got, err := bs.Get(ctx, c)
	if err != nil {
		t.Fatalf("Get of an honest block: %v", err)
	}
	if string(got.RawData()) != string(honest) {
		t.Errorf("Get returned %q, want %q", got.RawData(), honest)
	}
}

func TestValidatingGetPassesNotFound(t *testing.T) {
	ctx := context.Background()
	bs := store.Validating(mem())

	c := cidOver(t, cid.Raw, []byte("never stored"))
	_, err := bs.Get(ctx, c)
	if !ipld.IsNotFound(err) {
		t.Fatalf("Get of an absent block: got %v, want ipld not-found", err)
	}
	if errors.Is(err, store.ErrCorruptBlock) {
		t.Error("an absent block must not read as corrupt")
	}
}

// TestStoreBlocksValidatesOnOpen is the integration check: the blockstore the
// real store hands out validates, so corruption in flatfs is caught through the
// same accessor every consumer uses.
func TestStoreBlocksValidatesOnOpen(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	honest := []byte("a block that will be corrupted under its own key")
	c := cidOver(t, cid.Raw, honest)
	corrupt(t, st.Blocks(), c, []byte("corrupted on disk"))

	if _, err := st.Blocks().Get(ctx, c); !errors.Is(err, store.ErrCorruptBlock) {
		t.Fatalf("store.Blocks().Get of a corrupt block: got %v, want ErrCorruptBlock", err)
	}
}

func TestLocked(t *testing.T) {
	dir := t.TempDir()

	// A path with no store yet holds nothing.
	locked, err := store.Locked(dir)
	if err != nil {
		t.Fatalf("Locked on a fresh path: %v", err)
	}
	if locked {
		t.Error("a path with no KV yet reported locked")
	}

	// An open store holds the KV lock.
	st, err := store.Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	locked, err = store.Locked(dir)
	if err != nil {
		t.Fatalf("Locked on an open store: %v", err)
	}
	if !locked {
		t.Error("an open store did not report locked")
	}

	// And releases it on close, so a following offline tool can take it.
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	locked, err = store.Locked(dir)
	if err != nil {
		t.Fatalf("Locked after close: %v", err)
	}
	if locked {
		t.Error("a closed store still reported locked")
	}
}
