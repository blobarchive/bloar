package pinning

import (
	"context"
	"log/slog"
	"testing"

	"github.com/ipfs/boxo/blockstore"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"

	"github.com/blobarchive/bloar/schema"
)

func testGC(t *testing.T) (*GC, blockstore.Blockstore) {
	t.Helper()
	bs := blockstore.NewBlockstore(dssync.MutexWrap(datastore.NewMapDatastore()))
	return &GC{blocks: bs, log: slog.New(slog.DiscardHandler)}, bs
}

// TestLinksRawIsALeafButIsRead: a blob is the bottom of the DAG -- it has no
// links -- but the mark now READS it to confirm it is present.
// Spec 2 fixes blobs as raw blocks, so a present leaf yields no links without a
// decode; the read is an existence-and-validation check, whose cost the mark
// deliberately pays now (the fetch pass no longer stands behind it as a silent
// re-fetcher of swept leaves). A missing leaf under a writer fails the mark
// closed rather than being marked terminal-unread.
func TestLinksRawIsALeafButIsRead(t *testing.T) {
	g, bs := testGC(t)
	ctx := context.Background()

	data := []byte("a real blob's committed bytes")
	c := cid.NewCidV1(cid.Raw, testMultihash(t, string(data)))
	blk, err := blocks.NewBlockWithCid(data, c)
	if err != nil {
		t.Fatalf("framing: %v", err)
	}
	if err := bs.Put(ctx, blk); err != nil {
		t.Fatalf("Put: %v", err)
	}

	links, err := g.links(ctx, c, "", nil)
	if err != nil {
		t.Fatalf("links of a present raw block: %v", err)
	}
	if len(links) != 0 {
		t.Errorf("links of a raw block = %v, want none", links)
	}

	// A missing raw leaf under a writer (nil fetch) is real divergence, not a leaf
	// to mark and move past: it fails the mark closed.
	missing := cid.NewCidV1(cid.Raw, testMultihash(t, "a blob nobody stored"))
	if _, err := g.links(ctx, missing, "written", nil); err == nil {
		t.Fatal("links of a missing raw leaf under a writer: want a fail-closed error, got nil")
	}
}

// TestLinksRejectsUnknownCodec: bloar's DAG is raw and dag-cbor (spec 2).
// Anything else is a block this build cannot promise to have read the links of,
// and guessing would sweep whatever it links to.
func TestLinksRejectsUnknownCodec(t *testing.T) {
	g, bs := testGC(t)
	data := []byte(`{"kids":[]}`)
	c := cid.NewCidV1(cid.DagJSON, testMultihash(t, string(data)))
	blk, err := blocks.NewBlockWithCid(data, c)
	if err != nil {
		t.Fatalf("framing: %v", err)
	}
	if err := bs.Put(context.Background(), blk); err != nil {
		t.Fatalf("Put: %v", err)
	}

	if _, err := g.links(context.Background(), c, "", nil); err == nil {
		t.Fatal("links of a dag-json block: want an error, got nil")
	}
}

// TestLinksFindsEveryLinkGenerically walks a real Segment, whose blob links sit
// two levels down inside nested lists ([[slot, [[vh, &blob], ...]], ...]).
// Nothing here knows that shape, which is the point: the traversal is over the
// data model, so a block from a schema this build does not have still has its
// children marked.
func TestLinksFindsEveryLinkGenerically(t *testing.T) {
	g, bs := testGC(t)
	ctx := context.Background()

	blobs := []cid.Cid{
		cid.NewCidV1(cid.Raw, testMultihash(t, "b1")),
		cid.NewCidV1(cid.Raw, testMultihash(t, "b2")),
		cid.NewCidV1(cid.Raw, testMultihash(t, "b3")),
	}
	seg := &schema.Segment{Slot0: 8, Rows: []schema.Row{
		{Slot: 8, Entries: []schema.RefEntry{{VH: vh(1), Blob: blobs[0]}, {VH: vh(2), Blob: blobs[1]}}},
		{Slot: 9, Entries: []schema.RefEntry{{VH: vh(3), Blob: blobs[2]}}},
	}}
	data, c, err := schema.EncodeSegment(seg)
	if err != nil {
		t.Fatalf("EncodeSegment: %v", err)
	}
	blk, err := blocks.NewBlockWithCid(data, c)
	if err != nil {
		t.Fatalf("framing: %v", err)
	}
	if err := bs.Put(ctx, blk); err != nil {
		t.Fatalf("Put: %v", err)
	}

	links, err := g.links(ctx, c, "", nil)
	if err != nil {
		t.Fatalf("links: %v", err)
	}
	got := map[cid.Cid]bool{}
	for _, l := range links {
		got[l] = true
	}
	for _, want := range blobs {
		if !got[want] {
			t.Errorf("blob %s is not among the links of a segment that references it: %v", want, links)
		}
	}
	if len(links) != len(blobs) {
		t.Errorf("links = %v, want exactly the %d blobs (a vh is a byte string, not a link)", links, len(blobs))
	}
}

func vh(n byte) schema.VersionedHash {
	var v schema.VersionedHash
	v[0], v[31] = 0x01, n
	return v
}
