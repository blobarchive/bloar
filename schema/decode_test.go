package schema

import (
	"errors"
	"strings"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime/codec"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/fluent/qp"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
)

func decodeHeadErr(block []byte) error    { _, err := DecodeHead(block); return err }
func decodeSegmentErr(block []byte) error { _, err := DecodeSegment(block); return err }
func decodeDirNodeErr(block []byte) error { _, err := DecodeDirNode(block); return err }

// headEntries writes a valid head, letting each case override single fields.
func headEntries(ma datamodel.MapAssembler, overrides map[string]qp.Assemble) {
	entries := []struct {
		key string
		val qp.Assemble
	}{
		{"v", qp.Int(SchemaVersion)},
		{"name", qp.String("all")},
		{"net", qp.String("mainnet")},
		{"origin_slot", qp.Int(DencunMainnetSlot)},
		{"synced_to", qp.Null()},
		{"seg_bits", qp.Int(9)},
		{"fanout_bits", qp.Int(8)},
		{"dir_depth", qp.Int(0)},
		{"dir", qp.Null()},
		{"open", qp.Null()},
	}
	for _, e := range entries {
		val := e.val
		if o, ok := overrides[e.key]; ok {
			if o == nil {
				continue // omit the field entirely
			}
			val = o
		}
		qp.MapEntry(ma, e.key, val)
	}
}

func TestDecodeRejectsMalformed(t *testing.T) {
	blobCID := blobCIDFor(t, 0xaa)
	blobLink := qp.Link(cidlink.Link{Cid: blobCID})

	head := func(overrides map[string]qp.Assemble, extra ...func(datamodel.MapAssembler)) []byte {
		return buildBlock(t, codec.MapSortMode_RFC7049, func(ma datamodel.MapAssembler) {
			headEntries(ma, overrides)
			for _, fn := range extra {
				fn(ma)
			}
		})
	}
	segment := func(rows qp.Assemble) []byte {
		return buildBlock(t, codec.MapSortMode_RFC7049, func(ma datamodel.MapAssembler) {
			qp.MapEntry(ma, "v", qp.Int(SchemaVersion))
			qp.MapEntry(ma, "slot0", qp.Int(0))
			qp.MapEntry(ma, "rows", rows)
		})
	}
	row := func(slot qp.Assemble, entries ...qp.Assemble) qp.Assemble {
		return qp.List(2, func(la datamodel.ListAssembler) {
			qp.ListEntry(la, slot)
			qp.ListEntry(la, qp.List(int64(len(entries)), func(el datamodel.ListAssembler) {
				for _, e := range entries {
					qp.ListEntry(el, e)
				}
			}))
		})
	}
	rowsOf := func(rs ...qp.Assemble) qp.Assemble {
		return qp.List(int64(len(rs)), func(la datamodel.ListAssembler) {
			for _, r := range rs {
				qp.ListEntry(la, r)
			}
		})
	}
	refEntry := func(vh qp.Assemble, blob qp.Assemble) qp.Assemble {
		return qp.List(2, func(la datamodel.ListAssembler) {
			qp.ListEntry(la, vh)
			qp.ListEntry(la, blob)
		})
	}
	vhBytes := func(pattern byte) qp.Assemble {
		v := testVH(pattern)
		return qp.Bytes(v[:])
	}

	tests := []struct {
		name    string
		block   []byte
		decode  func([]byte) error
		wantErr string
	}{
		{
			name:    "head unknown version",
			block:   head(map[string]qp.Assemble{"v": qp.Int(2)}),
			decode:  decodeHeadErr,
			wantErr: "unknown version 2",
		},
		{
			name:    "head unknown field",
			block:   head(nil, func(ma datamodel.MapAssembler) { qp.MapEntry(ma, "zzz_extra", qp.Int(1)) }),
			decode:  decodeHeadErr,
			wantErr: `unknown field "zzz_extra"`,
		},
		{
			name:    "head missing field",
			block:   head(map[string]qp.Assemble{"net": nil}),
			decode:  decodeHeadErr,
			wantErr: `missing field "net"`,
		},
		{
			name:    "head bad name",
			block:   head(map[string]qp.Assemble{"name": qp.String("Not-A-Name")}),
			decode:  decodeHeadErr,
			wantErr: "does not match",
		},
		{
			name:    "head name leading dash",
			block:   head(map[string]qp.Assemble{"name": qp.String("-all")}),
			decode:  decodeHeadErr,
			wantErr: "does not match",
		},
		{
			name:    "head negative origin_slot",
			block:   head(map[string]qp.Assemble{"origin_slot": qp.Int(-1)}),
			decode:  decodeHeadErr,
			wantErr: "must not be negative",
		},
		{
			name:    "head origin_slot not an integer",
			block:   head(map[string]qp.Assemble{"origin_slot": qp.String("8626176")}),
			decode:  decodeHeadErr,
			wantErr: "must be an integer",
		},
		{
			name: "head dir set with dir_depth 0",
			block: head(map[string]qp.Assemble{
				"dir": qp.Link(cidlink.Link{Cid: blobCID}),
			}),
			decode:  decodeHeadErr,
			wantErr: "dir must be null iff dir_depth is 0",
		},
		{
			name: "head dir null with dir_depth 1",
			block: head(map[string]qp.Assemble{
				"dir_depth": qp.Int(1),
			}),
			decode:  decodeHeadErr,
			wantErr: "dir must be null iff dir_depth is 0",
		},
		{
			name: "head synced_to before origin_slot",
			block: head(map[string]qp.Assemble{
				"synced_to": qp.Int(DencunMainnetSlot - 1),
			}),
			decode:  decodeHeadErr,
			wantErr: "precedes origin_slot",
		},
		{
			name:    "head dir is bytes not a link",
			block:   head(map[string]qp.Assemble{"dir_depth": qp.Int(1), "dir": qp.Bytes([]byte{0x01})}),
			decode:  decodeHeadErr,
			wantErr: "must be a link",
		},
		{
			name:    "head is not a map",
			block:   buildListBlock(t),
			decode:  decodeHeadErr,
			wantErr: "must be a map",
		},
		{
			name:    "head trailing bytes",
			block:   append(head(nil), 0x00),
			decode:  decodeHeadErr,
			wantErr: "decoding dag-cbor",
		},
		{
			name: "segment row wrong arity",
			block: segment(rowsOf(qp.List(3, func(la datamodel.ListAssembler) {
				qp.ListEntry(la, qp.Int(1))
				qp.ListEntry(la, qp.Int(2))
				qp.ListEntry(la, qp.Int(3))
			}))),
			decode:  decodeSegmentErr,
			wantErr: "must have exactly 2 elements",
		},
		{
			name:    "segment row not a list",
			block:   segment(rowsOf(qp.Int(5))),
			decode:  decodeSegmentErr,
			wantErr: "must be a list",
		},
		{
			name:    "segment vh too short",
			block:   segment(rowsOf(row(qp.Int(1), refEntry(qp.Bytes(make([]byte, 31)), blobLink)))),
			decode:  decodeSegmentErr,
			wantErr: "vh must be 32 bytes, got 31",
		},
		{
			name:    "segment vh too long",
			block:   segment(rowsOf(row(qp.Int(1), refEntry(qp.Bytes(make([]byte, 33)), blobLink)))),
			decode:  decodeSegmentErr,
			wantErr: "vh must be 32 bytes, got 33",
		},
		{
			name:    "segment vh is a link not bytes",
			block:   segment(rowsOf(row(qp.Int(1), refEntry(blobLink, blobLink)))),
			decode:  decodeSegmentErr,
			wantErr: "vh must be a byte string",
		},
		{
			name:    "segment blob is null",
			block:   segment(rowsOf(row(qp.Int(1), refEntry(vhBytes(0x01), qp.Null())))),
			decode:  decodeSegmentErr,
			wantErr: "must be a link",
		},
		{
			name:    "segment ref entry wrong arity",
			block:   segment(rowsOf(row(qp.Int(1), qp.List(1, func(la datamodel.ListAssembler) { qp.ListEntry(la, vhBytes(0x01)) })))),
			decode:  decodeSegmentErr,
			wantErr: "must have exactly 2 elements",
		},
		{
			name:    "segment row with no entries",
			block:   segment(rowsOf(row(qp.Int(1)))),
			decode:  decodeSegmentErr,
			wantErr: "has no entries",
		},
		{
			name: "segment rows not ascending",
			block: segment(rowsOf(
				row(qp.Int(9), refEntry(vhBytes(0x01), blobLink)),
				row(qp.Int(2), refEntry(vhBytes(0x02), blobLink)),
			)),
			decode:  decodeSegmentErr,
			wantErr: "not strictly ascending",
		},
		{
			name: "segment duplicate row slots",
			block: segment(rowsOf(
				row(qp.Int(2), refEntry(vhBytes(0x01), blobLink)),
				row(qp.Int(2), refEntry(vhBytes(0x02), blobLink)),
			)),
			decode:  decodeSegmentErr,
			wantErr: "not strictly ascending",
		},
		{
			name:    "segment rows not a list",
			block:   segment(qp.Int(0)),
			decode:  decodeSegmentErr,
			wantErr: `"rows" must be a list`,
		},
		{
			name: "segment unknown version",
			block: buildBlock(t, codec.MapSortMode_RFC7049, func(ma datamodel.MapAssembler) {
				qp.MapEntry(ma, "v", qp.Int(99))
				qp.MapEntry(ma, "slot0", qp.Int(0))
				qp.MapEntry(ma, "rows", rowsOf())
			}),
			decode:  decodeSegmentErr,
			wantErr: "unknown version 99",
		},
		{
			name: "dirnode unknown version",
			block: buildBlock(t, codec.MapSortMode_RFC7049, func(ma datamodel.MapAssembler) {
				qp.MapEntry(ma, "v", qp.Int(0))
				qp.MapEntry(ma, "kids", qp.List(0, func(datamodel.ListAssembler) {}))
			}),
			decode:  decodeDirNodeErr,
			wantErr: "unknown version 0",
		},
		{
			name: "dirnode kid is bytes",
			block: buildBlock(t, codec.MapSortMode_RFC7049, func(ma datamodel.MapAssembler) {
				qp.MapEntry(ma, "v", qp.Int(SchemaVersion))
				qp.MapEntry(ma, "kids", qp.List(1, func(la datamodel.ListAssembler) { qp.ListEntry(la, qp.Bytes([]byte{0x01})) }))
			}),
			decode:  decodeDirNodeErr,
			wantErr: "must be a link",
		},
		{
			name: "dirnode kids not a list",
			block: buildBlock(t, codec.MapSortMode_RFC7049, func(ma datamodel.MapAssembler) {
				qp.MapEntry(ma, "v", qp.Int(SchemaVersion))
				qp.MapEntry(ma, "kids", qp.Int(3))
			}),
			decode:  decodeDirNodeErr,
			wantErr: `"kids" must be a list`,
		},
		{
			name:    "dirnode missing kids",
			block:   buildBlock(t, codec.MapSortMode_RFC7049, func(ma datamodel.MapAssembler) { qp.MapEntry(ma, "v", qp.Int(SchemaVersion)) }),
			decode:  decodeDirNodeErr,
			wantErr: `missing field "kids"`,
		},
		{
			name:    "not cbor at all",
			block:   []byte("this is not cbor"),
			decode:  decodeHeadErr,
			wantErr: "decoding dag-cbor",
		},
		{
			name:    "empty block",
			block:   []byte{},
			decode:  decodeHeadErr,
			wantErr: "decoding dag-cbor",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.decode(tt.block)
			if err == nil {
				t.Fatalf("decode succeeded, want error containing %q", tt.wantErr)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("error = %q, want it to contain %q", err, tt.wantErr)
			}
		})
	}
}

// TestUnknownVersionIsTyped pins the version rejection to a typed error so
// callers can tell "newer archive" apart from "corrupt block" (spec 15).
func TestUnknownVersionIsTyped(t *testing.T) {
	block := buildBlock(t, codec.MapSortMode_RFC7049, func(ma datamodel.MapAssembler) {
		headEntries(ma, map[string]qp.Assemble{"v": qp.Int(2)})
	})
	_, err := DecodeHead(block)
	var uve *UnknownVersionError
	if !errors.As(err, &uve) {
		t.Fatalf("error = %v (%T), want *UnknownVersionError", err, err)
	}
	if uve.Got != 2 {
		t.Errorf("Got = %d, want 2", uve.Got)
	}
	if uve.Object != "head" {
		t.Errorf("Object = %q, want %q", uve.Object, "head")
	}
}

func TestEncodeRejectsInvalid(t *testing.T) {
	blobCID := blobCIDFor(t, 0xaa)

	tests := []struct {
		name    string
		obj     any
		wantErr string
	}{
		{
			name:    "head empty name",
			obj:     &Head{Name: "", Net: "mainnet"},
			wantErr: "does not match",
		},
		{
			name:    "head uppercase name",
			obj:     &Head{Name: "All", Net: "mainnet"},
			wantErr: "does not match",
		},
		{
			name:    "head underscore in name",
			obj:     &Head{Name: "arbitrum_one", Net: "mainnet"},
			wantErr: "does not match",
		},
		{
			name:    "head dir without depth",
			obj:     &Head{Name: "all", Net: "mainnet", Dir: blobCID},
			wantErr: "dir must be null iff dir_depth is 0",
		},
		{
			name:    "segment row without entries",
			obj:     &Segment{Slot0: 0, Rows: []Row{{Slot: 1}}},
			wantErr: "has no entries",
		},
		{
			name:    "segment undefined blob link",
			obj:     &Segment{Slot0: 0, Rows: []Row{{Slot: 1, Entries: []RefEntry{{VH: testVH(0x01), Blob: cid.Undef}}}}},
			wantErr: "undefined blob link",
		},
		{
			name: "segment rows descending",
			obj: &Segment{Slot0: 0, Rows: []Row{
				{Slot: 5, Entries: []RefEntry{{VH: testVH(0x01), Blob: blobCID}}},
				{Slot: 1, Entries: []RefEntry{{VH: testVH(0x02), Blob: blobCID}}},
			}},
			wantErr: "not strictly ascending",
		},
		{
			name:    "segment row before window",
			obj:     &Segment{Slot0: 10, Rows: []Row{{Slot: 4, Entries: []RefEntry{{VH: testVH(0x01), Blob: blobCID}}}}},
			wantErr: "before the window start",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := encodeFixture(tt.obj)
			if err == nil {
				t.Fatalf("encode succeeded, want error containing %q", tt.wantErr)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("error = %q, want it to contain %q", err, tt.wantErr)
			}
		})
	}
}
