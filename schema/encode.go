package schema

import (
	"bytes"
	"fmt"
	"math"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime/codec"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/fluent/qp"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
)

// EncodeHead encodes h to canonical DAG-CBOR and returns the block bytes and
// its CID.
func EncodeHead(h *Head) ([]byte, cid.Cid, error) {
	if err := h.Validate(); err != nil {
		return nil, cid.Undef, err
	}
	fields := []struct {
		name string
		v    uint64
	}{
		{"origin_slot", h.OriginSlot},
		{"seg_bits", h.SegBits},
		{"fanout_bits", h.FanoutBits},
		{"dir_depth", h.DirDepth},
	}
	if h.SyncedTo != nil {
		fields = append(fields, struct {
			name string
			v    uint64
		}{"synced_to", *h.SyncedTo})
	}
	for _, f := range fields {
		if err := checkCBORInt("head", f.name, f.v); err != nil {
			return nil, cid.Undef, err
		}
	}

	n, err := qp.BuildMap(basicnode.Prototype.Map, 10, func(ma datamodel.MapAssembler) {
		qp.MapEntry(ma, "v", qp.Int(SchemaVersion))
		qp.MapEntry(ma, "name", qp.String(h.Name))
		qp.MapEntry(ma, "net", qp.String(h.Net))
		qp.MapEntry(ma, "origin_slot", qp.Int(int64(h.OriginSlot)))
		qp.MapEntry(ma, "synced_to", nullableInt(h.SyncedTo))
		qp.MapEntry(ma, "seg_bits", qp.Int(int64(h.SegBits)))
		qp.MapEntry(ma, "fanout_bits", qp.Int(int64(h.FanoutBits)))
		qp.MapEntry(ma, "dir_depth", qp.Int(int64(h.DirDepth)))
		qp.MapEntry(ma, "dir", nullableLink(h.Dir))
		qp.MapEntry(ma, "open", nullableLink(h.Open))
	})
	if err != nil {
		return nil, cid.Undef, fmt.Errorf("schema: building head node: %w", err)
	}
	return encodeNode(n)
}

// EncodeSegment encodes s to canonical DAG-CBOR and returns the block bytes
// and its CID.
func EncodeSegment(s *Segment) ([]byte, cid.Cid, error) {
	if err := s.Validate(); err != nil {
		return nil, cid.Undef, err
	}
	if err := checkCBORInt("segment", "slot0", s.Slot0); err != nil {
		return nil, cid.Undef, err
	}
	for i, r := range s.Rows {
		if err := checkCBORInt("segment", fmt.Sprintf("rows[%d] slot", i), r.Slot); err != nil {
			return nil, cid.Undef, err
		}
	}

	n, err := qp.BuildMap(basicnode.Prototype.Map, 3, func(ma datamodel.MapAssembler) {
		qp.MapEntry(ma, "v", qp.Int(SchemaVersion))
		qp.MapEntry(ma, "slot0", qp.Int(int64(s.Slot0)))
		qp.MapEntry(ma, "rows", qp.List(int64(len(s.Rows)), func(rows datamodel.ListAssembler) {
			for _, r := range s.Rows {
				qp.ListEntry(rows, qp.List(2, func(row datamodel.ListAssembler) {
					qp.ListEntry(row, qp.Int(int64(r.Slot)))
					qp.ListEntry(row, qp.List(int64(len(r.Entries)), func(entries datamodel.ListAssembler) {
						for _, e := range r.Entries {
							qp.ListEntry(entries, qp.List(2, func(ref datamodel.ListAssembler) {
								qp.ListEntry(ref, qp.Bytes(e.VH[:]))
								qp.ListEntry(ref, qp.Link(cidlink.Link{Cid: e.Blob}))
							}))
						}
					}))
				}))
			}
		}))
	})
	if err != nil {
		return nil, cid.Undef, fmt.Errorf("schema: building segment node: %w", err)
	}
	return encodeNode(n)
}

// EncodeDirNode encodes d to canonical DAG-CBOR and returns the block bytes
// and its CID. Trailing null kids are trimmed; see trimTrailingNulls.
func EncodeDirNode(d *DirNode) ([]byte, cid.Cid, error) {
	if err := d.Validate(); err != nil {
		return nil, cid.Undef, err
	}
	kids := trimTrailingNulls(d.Kids)

	n, err := qp.BuildMap(basicnode.Prototype.Map, 2, func(ma datamodel.MapAssembler) {
		qp.MapEntry(ma, "v", qp.Int(SchemaVersion))
		qp.MapEntry(ma, "kids", qp.List(int64(len(kids)), func(la datamodel.ListAssembler) {
			for _, k := range kids {
				qp.ListEntry(la, nullableLink(k))
			}
		}))
	})
	if err != nil {
		return nil, cid.Undef, fmt.Errorf("schema: building dirnode node: %w", err)
	}
	return encodeNode(n)
}

// trimTrailingNulls drops trailing null kids. Spec 3.3 permits omitting them
// and requires readers to treat out-of-range kids as null; this writer always
// omits them, so a given set of live kids has exactly one encoding and
// therefore exactly one CID however the caller padded the slice. Interior
// nulls stay explicit -- they carry position.
func trimTrailingNulls(kids []cid.Cid) []cid.Cid {
	n := len(kids)
	for n > 0 && !kids[n-1].Defined() {
		n--
	}
	return kids[:n]
}

// encodeNode serialises n and derives its CID. The encode options are spelled
// out rather than inherited from dagcbor.Encode's defaults: canonical form is
// a conformance requirement (spec 2), not a default worth taking on trust.
func encodeNode(n datamodel.Node) ([]byte, cid.Cid, error) {
	var buf bytes.Buffer
	opts := dagcbor.EncodeOptions{
		AllowLinks:  true,
		MapSortMode: codec.MapSortMode_RFC7049,
	}
	if err := opts.Encode(n, &buf); err != nil {
		return nil, cid.Undef, fmt.Errorf("schema: encoding dag-cbor: %w", err)
	}
	block := buf.Bytes()
	c, err := NodeCID(block)
	if err != nil {
		return nil, cid.Undef, err
	}
	return block, c, nil
}

// checkCBORInt guards the one place Go's uint64 outruns the data model: IPLD
// integers are int64. Real slots are nowhere near this, so this is a
// corruption check, not a limit.
func checkCBORInt(obj, field string, v uint64) error {
	if v > math.MaxInt64 {
		return fmt.Errorf("schema: %s field %q value %d exceeds the maximum DAG-CBOR integer", obj, field, v)
	}
	return nil
}

func nullableInt(v *uint64) qp.Assemble {
	if v == nil {
		return qp.Null()
	}
	return qp.Int(int64(*v))
}

func nullableLink(c cid.Cid) qp.Assemble {
	if !c.Defined() {
		return qp.Null()
	}
	return qp.Link(cidlink.Link{Cid: c})
}
