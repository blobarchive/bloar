package schema

import (
	"bytes"
	"fmt"
	"slices"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	"github.com/ipld/go-ipld-prime/datamodel"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
)

var (
	headKeys    = []string{"v", "name", "net", "origin_slot", "synced_to", "seg_bits", "fanout_bits", "dir_depth", "dir", "open"}
	segmentKeys = []string{"v", "slot0", "rows"}
	dirNodeKeys = []string{"v", "kids"}
)

// DecodeHead decodes a Head block. It rejects any object whose version is not
// SchemaVersion (spec 15).
func DecodeHead(block []byte) (*Head, error) {
	n, err := decodeNode(block)
	if err != nil {
		return nil, err
	}
	fields, err := mapFields("head", n, headKeys)
	if err != nil {
		return nil, err
	}
	if err := checkVersion("head", fields["v"]); err != nil {
		return nil, err
	}

	h := &Head{}
	if h.Name, err = fieldString("head", "name", fields["name"]); err != nil {
		return nil, err
	}
	if h.Net, err = fieldString("head", "net", fields["net"]); err != nil {
		return nil, err
	}
	if h.OriginSlot, err = fieldUint("head", "origin_slot", fields["origin_slot"]); err != nil {
		return nil, err
	}
	if h.SyncedTo, err = fieldNullableUint("head", "synced_to", fields["synced_to"]); err != nil {
		return nil, err
	}
	if h.SegBits, err = fieldUint("head", "seg_bits", fields["seg_bits"]); err != nil {
		return nil, err
	}
	if h.FanoutBits, err = fieldUint("head", "fanout_bits", fields["fanout_bits"]); err != nil {
		return nil, err
	}
	if h.DirDepth, err = fieldUint("head", "dir_depth", fields["dir_depth"]); err != nil {
		return nil, err
	}
	if h.Dir, err = fieldNullableLink("head", "dir", fields["dir"]); err != nil {
		return nil, err
	}
	if h.Open, err = fieldNullableLink("head", "open", fields["open"]); err != nil {
		return nil, err
	}
	if err := h.Validate(); err != nil {
		return nil, err
	}
	return h, nil
}

// DecodeSegment decodes a Segment block.
func DecodeSegment(block []byte) (*Segment, error) {
	n, err := decodeNode(block)
	if err != nil {
		return nil, err
	}
	fields, err := mapFields("segment", n, segmentKeys)
	if err != nil {
		return nil, err
	}
	if err := checkVersion("segment", fields["v"]); err != nil {
		return nil, err
	}

	s := &Segment{}
	if s.Slot0, err = fieldUint("segment", "slot0", fields["slot0"]); err != nil {
		return nil, err
	}

	rowsNode := fields["rows"]
	if rowsNode.Kind() != datamodel.Kind_List {
		return nil, fmt.Errorf("schema: segment field \"rows\" must be a list, got %s", rowsNode.Kind())
	}
	s.Rows = make([]Row, 0, rowsNode.Length())
	it := rowsNode.ListIterator()
	for !it.Done() {
		idx, rowNode, err := it.Next()
		if err != nil {
			return nil, fmt.Errorf("schema: iterating segment rows: %w", err)
		}
		row, err := decodeRow(idx, rowNode)
		if err != nil {
			return nil, err
		}
		s.Rows = append(s.Rows, row)
	}
	if err := s.Validate(); err != nil {
		return nil, err
	}
	return s, nil
}

// DecodeDirNode decodes a DirNode block. Trailing nulls, which spec 3.3
// permits on the wire, are trimmed so that the decoded value is in canonical
// form.
func DecodeDirNode(block []byte) (*DirNode, error) {
	n, err := decodeNode(block)
	if err != nil {
		return nil, err
	}
	fields, err := mapFields("dirnode", n, dirNodeKeys)
	if err != nil {
		return nil, err
	}
	if err := checkVersion("dirnode", fields["v"]); err != nil {
		return nil, err
	}

	kidsNode := fields["kids"]
	if kidsNode.Kind() != datamodel.Kind_List {
		return nil, fmt.Errorf("schema: dirnode field \"kids\" must be a list, got %s", kidsNode.Kind())
	}
	kids := make([]cid.Cid, 0, kidsNode.Length())
	it := kidsNode.ListIterator()
	for !it.Done() {
		idx, kidNode, err := it.Next()
		if err != nil {
			return nil, fmt.Errorf("schema: iterating dirnode kids: %w", err)
		}
		kid, err := fieldNullableLink("dirnode", fmt.Sprintf("kids[%d]", idx), kidNode)
		if err != nil {
			return nil, err
		}
		kids = append(kids, kid)
	}
	d := &DirNode{Kids: trimTrailingNulls(kids)}
	if err := d.Validate(); err != nil {
		return nil, err
	}
	return d, nil
}

func decodeRow(idx int64, n datamodel.Node) (Row, error) {
	if n.Kind() != datamodel.Kind_List {
		return Row{}, fmt.Errorf("schema: segment rows[%d] must be a list, got %s", idx, n.Kind())
	}
	if n.Length() != 2 {
		return Row{}, fmt.Errorf("schema: segment rows[%d] must have exactly 2 elements, got %d", idx, n.Length())
	}
	slotNode, err := n.LookupByIndex(0)
	if err != nil {
		return Row{}, fmt.Errorf("schema: segment rows[%d]: %w", idx, err)
	}
	slot, err := fieldUint("segment", fmt.Sprintf("rows[%d] slot", idx), slotNode)
	if err != nil {
		return Row{}, err
	}
	entriesNode, err := n.LookupByIndex(1)
	if err != nil {
		return Row{}, fmt.Errorf("schema: segment rows[%d]: %w", idx, err)
	}
	if entriesNode.Kind() != datamodel.Kind_List {
		return Row{}, fmt.Errorf("schema: segment rows[%d] entries must be a list, got %s", idx, entriesNode.Kind())
	}

	row := Row{Slot: slot, Entries: make([]RefEntry, 0, entriesNode.Length())}
	it := entriesNode.ListIterator()
	for !it.Done() {
		entryIdx, entryNode, err := it.Next()
		if err != nil {
			return Row{}, fmt.Errorf("schema: iterating segment rows[%d] entries: %w", idx, err)
		}
		entry, err := decodeRefEntry(idx, entryIdx, entryNode)
		if err != nil {
			return Row{}, err
		}
		row.Entries = append(row.Entries, entry)
	}
	return row, nil
}

func decodeRefEntry(rowIdx, entryIdx int64, n datamodel.Node) (RefEntry, error) {
	where := fmt.Sprintf("rows[%d] entries[%d]", rowIdx, entryIdx)
	if n.Kind() != datamodel.Kind_List {
		return RefEntry{}, fmt.Errorf("schema: segment %s must be a list, got %s", where, n.Kind())
	}
	if n.Length() != 2 {
		return RefEntry{}, fmt.Errorf("schema: segment %s must have exactly 2 elements, got %d", where, n.Length())
	}
	vhNode, err := n.LookupByIndex(0)
	if err != nil {
		return RefEntry{}, fmt.Errorf("schema: segment %s: %w", where, err)
	}
	vhBytes, err := vhNode.AsBytes()
	if err != nil {
		return RefEntry{}, fmt.Errorf("schema: segment %s vh must be a byte string: %w", where, err)
	}
	if len(vhBytes) != VersionedHashSize {
		return RefEntry{}, fmt.Errorf("schema: segment %s vh must be %d bytes, got %d", where, VersionedHashSize, len(vhBytes))
	}
	blobNode, err := n.LookupByIndex(1)
	if err != nil {
		return RefEntry{}, fmt.Errorf("schema: segment %s: %w", where, err)
	}
	blob, err := fieldLink("segment", where+" blob", blobNode)
	if err != nil {
		return RefEntry{}, err
	}
	entry := RefEntry{Blob: blob}
	copy(entry.VH[:], vhBytes)
	return entry, nil
}

func decodeNode(block []byte) (datamodel.Node, error) {
	nb := basicnode.Prototype.Any.NewBuilder()
	// DecodeOptions rejects trailing bytes past the object by default, which is
	// what we want: a block is exactly one object.
	if err := (dagcbor.DecodeOptions{AllowLinks: true}).Decode(nb, bytes.NewReader(block)); err != nil {
		return nil, fmt.Errorf("schema: decoding dag-cbor: %w", err)
	}
	return nb.Build(), nil
}

// mapFields returns the object's fields, requiring the key set to be exactly
// want. Unknown keys are rejected rather than ignored: dropping them would let
// decode-then-re-encode silently change an object's CID, and CID stability is
// what the whole archive rests on. Forward-compatible additions come with a
// version bump, which checkVersion rejects anyway.
func mapFields(obj string, n datamodel.Node, want []string) (map[string]datamodel.Node, error) {
	if n.Kind() != datamodel.Kind_Map {
		return nil, fmt.Errorf("schema: %s must be a map, got %s", obj, n.Kind())
	}
	fields := make(map[string]datamodel.Node, len(want))
	it := n.MapIterator()
	for !it.Done() {
		keyNode, valNode, err := it.Next()
		if err != nil {
			return nil, fmt.Errorf("schema: iterating %s fields: %w", obj, err)
		}
		key, err := keyNode.AsString()
		if err != nil {
			return nil, fmt.Errorf("schema: %s has a non-string field key: %w", obj, err)
		}
		if !slices.Contains(want, key) {
			return nil, fmt.Errorf("schema: %s has unknown field %q", obj, key)
		}
		fields[key] = valNode
	}
	for _, key := range want {
		if _, ok := fields[key]; !ok {
			return nil, fmt.Errorf("schema: %s is missing field %q", obj, key)
		}
	}
	return fields, nil
}

func checkVersion(obj string, n datamodel.Node) error {
	v, err := n.AsInt()
	if err != nil {
		return fmt.Errorf("schema: %s field \"v\" must be an integer: %w", obj, err)
	}
	if v != SchemaVersion {
		return &UnknownVersionError{Object: obj, Got: v}
	}
	return nil
}

func fieldString(obj, key string, n datamodel.Node) (string, error) {
	s, err := n.AsString()
	if err != nil {
		return "", fmt.Errorf("schema: %s field %q must be a string: %w", obj, key, err)
	}
	return s, nil
}

func fieldUint(obj, key string, n datamodel.Node) (uint64, error) {
	i, err := n.AsInt()
	if err != nil {
		return 0, fmt.Errorf("schema: %s field %q must be an integer: %w", obj, key, err)
	}
	if i < 0 {
		return 0, fmt.Errorf("schema: %s field %q must not be negative, got %d", obj, key, i)
	}
	return uint64(i), nil
}

func fieldNullableUint(obj, key string, n datamodel.Node) (*uint64, error) {
	if n.IsNull() {
		return nil, nil
	}
	v, err := fieldUint(obj, key, n)
	if err != nil {
		return nil, err
	}
	return &v, nil
}

func fieldLink(obj, key string, n datamodel.Node) (cid.Cid, error) {
	lnk, err := n.AsLink()
	if err != nil {
		return cid.Undef, fmt.Errorf("schema: %s field %q must be a link: %w", obj, key, err)
	}
	cl, ok := lnk.(cidlink.Link)
	if !ok {
		return cid.Undef, fmt.Errorf("schema: %s field %q is not a CID link", obj, key)
	}
	if !cl.Cid.Defined() {
		return cid.Undef, fmt.Errorf("schema: %s field %q is an undefined link", obj, key)
	}
	return cl.Cid, nil
}

func fieldNullableLink(obj, key string, n datamodel.Node) (cid.Cid, error) {
	if n.IsNull() {
		return cid.Undef, nil
	}
	return fieldLink(obj, key, n)
}
