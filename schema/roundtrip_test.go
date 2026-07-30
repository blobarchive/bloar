package schema

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime/codec"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/fluent/qp"
)

func TestRoundTrip(t *testing.T) {
	for _, f := range fixtures(t) {
		t.Run(f.name, func(t *testing.T) {
			block, c, err := encodeFixture(f.obj)
			if err != nil {
				t.Fatalf("encode: %v", err)
			}

			decoded, err := decodeFixture(fixtureKind(f.obj), block)
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			if want := f.want(); !reflect.DeepEqual(decoded, want) {
				t.Errorf("decode(encode(x)) mismatch:\n got %+v\nwant %+v", decoded, want)
			}

			reblock, rec, err := encodeFixture(decoded)
			if err != nil {
				t.Fatalf("re-encode: %v", err)
			}
			if !bytes.Equal(block, reblock) {
				t.Errorf("re-encoded bytes differ:\n first %x\nsecond %x", block, reblock)
			}
			if c != rec {
				t.Errorf("re-encoded CID differs: first %s, second %s", c, rec)
			}
		})
	}
}

// TestEncodingIgnoresFieldOrder pins the property the CIDs rest on: the
// encoding is a function of logical content alone, so a block whose map keys
// arrived in some other order re-encodes to the canonical bytes.
func TestEncodingIgnoresFieldOrder(t *testing.T) {
	_, segCID := mustEncode(t, &Segment{
		Slot0: DencunMainnetSlot,
		Rows:  []Row{{Slot: DencunMainnetSlot, Entries: []RefEntry{{VH: testVH(0x01), Blob: blobCIDFor(t, 0xaa)}}}},
	})
	syncedTo := uint64(DencunMainnetSlot + 10)
	h := &Head{
		Name:       "all",
		Net:        "mainnet",
		OriginSlot: DencunMainnetSlot,
		SyncedTo:   &syncedTo,
		SegBits:    9,
		FanoutBits: 8,
		Open:       segCID,
	}

	canonical, canonicalCID, err := EncodeHead(h)
	if err != nil {
		t.Fatalf("EncodeHead: %v", err)
	}

	permuted := buildBlock(t, codec.MapSortMode_None, func(ma datamodel.MapAssembler) {
		qp.MapEntry(ma, "open", nullableLink(h.Open))
		qp.MapEntry(ma, "dir_depth", qp.Int(int64(h.DirDepth)))
		qp.MapEntry(ma, "name", qp.String(h.Name))
		qp.MapEntry(ma, "dir", nullableLink(h.Dir))
		qp.MapEntry(ma, "v", qp.Int(SchemaVersion))
		qp.MapEntry(ma, "synced_to", nullableInt(h.SyncedTo))
		qp.MapEntry(ma, "fanout_bits", qp.Int(int64(h.FanoutBits)))
		qp.MapEntry(ma, "net", qp.String(h.Net))
		qp.MapEntry(ma, "seg_bits", qp.Int(int64(h.SegBits)))
		qp.MapEntry(ma, "origin_slot", qp.Int(int64(h.OriginSlot)))
	})
	if bytes.Equal(canonical, permuted) {
		t.Fatal("permuted block is byte-identical to the canonical one; the test proves nothing")
	}

	decoded, err := DecodeHead(permuted)
	if err != nil {
		t.Fatalf("DecodeHead(permuted): %v", err)
	}
	got, gotCID, err := EncodeHead(decoded)
	if err != nil {
		t.Fatalf("EncodeHead(decoded): %v", err)
	}
	if !bytes.Equal(got, canonical) {
		t.Errorf("re-encoding a permuted head did not normalise:\n got %x\nwant %x", got, canonical)
	}
	if gotCID != canonicalCID {
		t.Errorf("CID from permuted input = %s, want %s", gotCID, canonicalCID)
	}
}

func TestDirNodeTrailingNullsTrimmed(t *testing.T) {
	_, kid := mustEncode(t, &Segment{
		Slot0: 0,
		Rows:  []Row{{Slot: 3, Entries: []RefEntry{{VH: testVH(0x07), Blob: blobCIDFor(t, 0xaa)}}}},
	})

	_, trimmedCID := mustEncode(t, &DirNode{Kids: []cid.Cid{kid}})
	_, paddedCID := mustEncode(t, &DirNode{Kids: []cid.Cid{kid, cid.Undef, cid.Undef, cid.Undef}})
	if trimmedCID != paddedCID {
		t.Errorf("trailing nulls changed the CID: padded %s, trimmed %s", paddedCID, trimmedCID)
	}

	// Interior nulls carry position and must survive.
	_, interiorCID := mustEncode(t, &DirNode{Kids: []cid.Cid{cid.Undef, kid}})
	if interiorCID == trimmedCID {
		t.Error("an interior null was dropped: [null, kid] must not encode as [kid]")
	}

	_, emptyCID := mustEncode(t, &DirNode{Kids: []cid.Cid{}})
	_, allNullCID := mustEncode(t, &DirNode{Kids: []cid.Cid{cid.Undef, cid.Undef}})
	if emptyCID != allNullCID {
		t.Errorf("an all-null dirnode did not encode as empty: %s vs %s", allNullCID, emptyCID)
	}
}

func TestBlobCID(t *testing.T) {
	tests := []struct {
		name    string
		size    int
		wantErr bool
	}{
		{name: "exact", size: BlobSize},
		{name: "empty", size: 0, wantErr: true},
		{name: "one short", size: BlobSize - 1, wantErr: true},
		{name: "one long", size: BlobSize + 1, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, err := BlobCID(make([]byte, tt.size))
			if tt.wantErr {
				if err == nil {
					t.Fatalf("BlobCID(%d bytes) = %s, want error", tt.size, c)
				}
				return
			}
			if err != nil {
				t.Fatalf("BlobCID: %v", err)
			}
			if got := c.Prefix().Codec; got != cid.Raw {
				t.Errorf("codec = %#x, want raw %#x", got, cid.Raw)
			}
			if got := c.Prefix().Version; got != 1 {
				t.Errorf("version = %d, want 1", got)
			}
		})
	}
}

func TestNodeCIDIsDagCBOR(t *testing.T) {
	block, c := mustEncode(t, &DirNode{Kids: []cid.Cid{}})
	if got := c.Prefix().Codec; got != cid.DagCBOR {
		t.Errorf("codec = %#x, want dag-cbor %#x", got, cid.DagCBOR)
	}
	if got := c.Prefix().Version; got != 1 {
		t.Errorf("version = %d, want 1", got)
	}
	direct, err := NodeCID(block)
	if err != nil {
		t.Fatalf("NodeCID: %v", err)
	}
	if direct != c {
		t.Errorf("NodeCID(block) = %s, want %s", direct, c)
	}
}
