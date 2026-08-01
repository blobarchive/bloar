package schema

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime/codec"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/fluent/qp"
	"github.com/ipld/go-ipld-prime/node/basicnode"
)

// fixture is one logical object shared by the round-trip and golden-vector
// tests. decoded, when non-nil, is what decoding obj's encoding must produce;
// it differs from obj only where encoding normalises (trailing null kids).
type fixture struct {
	name    string
	obj     any
	decoded any
}

func (f fixture) want() any {
	if f.decoded != nil {
		return f.decoded
	}
	return f.obj
}

func fixtureKind(obj any) string {
	switch obj.(type) {
	case *Head:
		return "head"
	case *Segment:
		return "segment"
	case *DirNode:
		return "dirnode"
	case *Manifest:
		return "manifest"
	default:
		return ""
	}
}

func encodeFixture(obj any) ([]byte, cid.Cid, error) {
	switch o := obj.(type) {
	case *Head:
		return EncodeHead(o)
	case *Segment:
		return EncodeSegment(o)
	case *DirNode:
		return EncodeDirNode(o)
	case *Manifest:
		return EncodeManifest(o)
	default:
		return nil, cid.Undef, fmt.Errorf("unknown fixture type %T", obj)
	}
}

func decodeFixture(kind string, block []byte) (any, error) {
	switch kind {
	case "head":
		return DecodeHead(block)
	case "segment":
		return DecodeSegment(block)
	case "dirnode":
		return DecodeDirNode(block)
	case "manifest":
		return DecodeManifest(block)
	default:
		return nil, fmt.Errorf("unknown fixture kind %q", kind)
	}
}

// blobPatterns are the fake blobs the fixtures reference: a full-size buffer
// of one repeated byte each. The blobs themselves stay out of testdata (they
// are 128 KiB apiece); only the CIDs they hash to are asserted.
var blobPatterns = []byte{0xaa, 0xbb, 0xcc}

func blobCIDFor(t *testing.T, pattern byte) cid.Cid {
	t.Helper()
	c, err := BlobCID(bytes.Repeat([]byte{pattern}, BlobSize))
	if err != nil {
		t.Fatalf("BlobCID(0x%02x pattern): %v", pattern, err)
	}
	return c
}

// testVH shapes a fake versioned hash like a real one: the 0x01 version byte
// followed by a repeated pattern standing in for the commitment hash.
func testVH(pattern byte) VersionedHash {
	var v VersionedHash
	for i := range v {
		v[i] = pattern
	}
	v[0] = 0x01
	return v
}

func mustEncode(t *testing.T, obj any) ([]byte, cid.Cid) {
	t.Helper()
	block, c, err := encodeFixture(obj)
	if err != nil {
		t.Fatalf("encoding %T fixture: %v", obj, err)
	}
	return block, c
}

func fixtures(t *testing.T) []fixture {
	t.Helper()

	blobA := blobCIDFor(t, 0xaa)
	blobB := blobCIDFor(t, 0xbb)
	blobC := blobCIDFor(t, 0xcc)

	segSingle := &Segment{
		Slot0: DencunMainnetSlot,
		Rows: []Row{
			{Slot: DencunMainnetSlot + 4, Entries: []RefEntry{{VH: testVH(0x11), Blob: blobA}}},
		},
	}
	segMulti := &Segment{
		Slot0: DencunMainnetSlot,
		Rows: []Row{
			{Slot: DencunMainnetSlot, Entries: []RefEntry{
				{VH: testVH(0x01), Blob: blobA},
				{VH: testVH(0x02), Blob: blobB},
			}},
			{Slot: DencunMainnetSlot + 24, Entries: []RefEntry{
				{VH: testVH(0x03), Blob: blobC},
			}},
			{Slot: DencunMainnetSlot + 511, Entries: []RefEntry{
				{VH: testVH(0x04), Blob: blobA},
				{VH: testVH(0x05), Blob: blobB},
				{VH: testVH(0x06), Blob: blobC},
			}},
		},
	}
	_, segSingleCID := mustEncode(t, segSingle)
	_, segMultiCID := mustEncode(t, segMulti)

	dirInterior := &DirNode{Kids: []cid.Cid{segMultiCID, cid.Undef, segSingleCID}}
	_, dirInteriorCID := mustEncode(t, dirInterior)

	// A genesis manifest (single open-ended inbox-events source) and an upgrade
	// that chains to it via prev, closing the inbox source and adding a blob-txs
	// era: the two shapes spec 10.5 has, including the type-specific fields and a
	// bounded until_block.
	manGenesis := &Manifest{
		V:    ManifestVersion,
		Head: "arbitrum-one",
		Sources: []Source{
			{Type: SourceInboxEvents, Address: testAddr(0x1c), Topic: testTopic(0x73), FromBlock: 0, OpenEnded: true},
		},
	}
	_, manGenesisCID := mustEncode(t, manGenesis)

	manUpgrade := &Manifest{
		V:    ManifestVersion,
		Head: "arbitrum-one",
		Sources: []Source{
			{Type: SourceInboxEvents, Address: testAddr(0x1c), Topic: testTopic(0x73), FromBlock: 0, UntilBlock: 21_000_000},
			{Type: SourceBlobTxs, Address: testAddr(0x50), Senders: [][]byte{testAddr(0xa4), testAddr(0xb5)}, FromBlock: 21_000_001, OpenEnded: true},
		},
		Prev: manGenesisCID,
	}

	syncedTo := uint64(DencunMainnetSlot + 511)

	return []fixture{
		{
			name: "head-empty",
			obj: &Head{
				Name:       "all",
				Net:        "mainnet",
				OriginSlot: DencunMainnetSlot,
				SegBits:    9,
				FanoutBits: 8,
			},
		},
		{
			name: "head-links",
			obj: &Head{
				Name:       "arbitrum-one",
				Net:        "mainnet",
				OriginSlot: DencunMainnetSlot,
				SyncedTo:   &syncedTo,
				SegBits:    13,
				FanoutBits: 8,
				DirDepth:   2,
				Dir:        dirInteriorCID,
				Open:       segSingleCID,
			},
		},
		{name: "segment-single-row", obj: segSingle},
		{name: "segment-multi-row", obj: segMulti},
		{name: "dirnode-empty", obj: &DirNode{Kids: []cid.Cid{}}},
		{name: "dirnode-interior-null", obj: dirInterior},
		{
			name:    "dirnode-trailing-nulls",
			obj:     &DirNode{Kids: []cid.Cid{segMultiCID, cid.Undef, cid.Undef}},
			decoded: &DirNode{Kids: []cid.Cid{segMultiCID}},
		},
		{name: "manifest-genesis", obj: manGenesis},
		{name: "manifest-upgrade", obj: manUpgrade},
	}
}

// testAddr shapes a fake 20-byte L1 address from a repeated pattern byte.
func testAddr(pattern byte) []byte { return bytes.Repeat([]byte{pattern}, AddressSize) }

// testTopic shapes a fake 32-byte event topic from a repeated pattern byte.
func testTopic(pattern byte) []byte { return bytes.Repeat([]byte{pattern}, TopicSize) }

// buildBlock assembles an arbitrary map and encodes it as DAG-CBOR, bypassing
// this package's constructors so tests can produce blocks a correct writer
// never would.
func buildBlock(t *testing.T, sort codec.MapSortMode, fn func(datamodel.MapAssembler)) []byte {
	t.Helper()
	n, err := qp.BuildMap(basicnode.Prototype.Map, 8, fn)
	if err != nil {
		t.Fatalf("building node: %v", err)
	}
	return encodeRaw(t, n, sort)
}

// buildListBlock encodes a top-level list, which no bloar object ever is.
func buildListBlock(t *testing.T) []byte {
	t.Helper()
	n, err := qp.BuildList(basicnode.Prototype.List, 1, func(la datamodel.ListAssembler) {
		qp.ListEntry(la, qp.Int(1))
	})
	if err != nil {
		t.Fatalf("building list node: %v", err)
	}
	return encodeRaw(t, n, codec.MapSortMode_RFC7049)
}

func encodeRaw(t *testing.T, n datamodel.Node, sort codec.MapSortMode) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := (dagcbor.EncodeOptions{AllowLinks: true, MapSortMode: sort}).Encode(n, &buf); err != nil {
		t.Fatalf("encoding node: %v", err)
	}
	return buf.Bytes()
}
