package schema

import (
	"strings"
	"testing"

	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/fluent/qp"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
)

// segmentWithRowOf returns a one-row segment whose single row carries n
// identical entries, for exercising the per-slot blob ceiling of the safety boundary.
func segmentWithRowOf(t *testing.T, n int) *Segment {
	t.Helper()
	entry := RefEntry{VH: testVH(0x01), Blob: blobCIDFor(t, 0xaa)}
	entries := make([]RefEntry, n)
	for i := range entries {
		entries[i] = entry
	}
	return &Segment{Slot0: DencunMainnetSlot, Rows: []Row{{Slot: DencunMainnetSlot, Entries: entries}}}
}

// TestSegmentRowEntryCeiling: Segment.Validate rejects a row over the per-slot
// ceiling and accepts one exactly at it -- the read-path construction bound of
// the safety boundary.
func TestSegmentRowEntryCeiling(t *testing.T) {
	if err := segmentWithRowOf(t, MaxBlobsPerSlotCeiling+1).Validate(); err == nil {
		t.Fatal("Validate accepted a row over the per-slot ceiling")
	} else if !strings.Contains(err.Error(), "ceiling") {
		t.Errorf("error %v, want it to name the ceiling", err)
	}
	if err := segmentWithRowOf(t, MaxBlobsPerSlotCeiling).Validate(); err != nil {
		t.Errorf("Validate rejected a row exactly at the ceiling: %v", err)
	}
}

// TestSegmentRowEntryCeilingRejectedOnEncode: EncodeSegment refuses to emit an
// oversized row, so this node cannot originate one.
func TestSegmentRowEntryCeilingRejectedOnEncode(t *testing.T) {
	if _, _, err := EncodeSegment(segmentWithRowOf(t, MaxBlobsPerSlotCeiling+1)); err == nil {
		t.Fatal("EncodeSegment emitted a row over the per-slot ceiling")
	} else if !strings.Contains(err.Error(), "ceiling") {
		t.Errorf("error %v, want it to name the ceiling", err)
	}
}

// TestSegmentRowEntryCeilingRejectedOnDecode: DecodeSegment refuses an oversized
// row arriving from a peer, so another node cannot smuggle one past the read-path
// bound. The bytes are built without Segment.Validate, since no
// honest encoder would produce them.
func TestSegmentRowEntryCeilingRejectedOnDecode(t *testing.T) {
	block := encodeSegmentSkippingValidate(t, segmentWithRowOf(t, MaxBlobsPerSlotCeiling+1))
	if _, err := DecodeSegment(block); err == nil {
		t.Fatal("DecodeSegment accepted a row over the per-slot ceiling")
	} else if !strings.Contains(err.Error(), "ceiling") {
		t.Errorf("error %v, want it to name the ceiling", err)
	}
}

// encodeSegmentSkippingValidate builds segment bytes exactly as EncodeSegment
// does but without its Segment.Validate guard, so a test can hand DecodeSegment a
// row no encoder would ever emit. It mirrors EncodeSegment's node build on
// purpose: the point is well-formed DAG-CBOR that nonetheless carries an
// over-ceiling row.
func encodeSegmentSkippingValidate(t *testing.T, s *Segment) []byte {
	t.Helper()
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
		t.Fatalf("building segment node: %v", err)
	}
	block, _, err := encodeNode(n)
	if err != nil {
		t.Fatalf("encodeNode: %v", err)
	}
	return block
}
