package schema

import (
	"fmt"
	"slices"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/fluent/qp"
	"github.com/ipld/go-ipld-prime/node/basicnode"
)

// ManifestVersion is the value carried in a Manifest's "v" field. It is versioned
// apart from the index schema (SchemaVersion) because the two evolve for
// different reasons and a decoder of one is not a decoder of the other: spec 15's
// reject-unknown-major rule binds whatever decodes a manifest -- the indexer
// validating an upgrade, a reviewer re-deriving a filtered head -- and not the
// followers, which replicate the chain by generic link traversal and never decode
// it at all.
const ManifestVersion = 1

// Sizes of the byte-string fields a Source carries (spec 10.5). address and each
// sender are 20-byte L1 addresses; topic is a 32-byte event topic0. They are
// plain CBOR byte strings, never links: they are not CIDs (spec 2).
const (
	AddressSize = 20
	TopicSize   = 32
)

// Source types, as the wire spells them (spec 10.4). schema keeps its own copies
// rather than importing index/chain's SourceType: the DAG layer is deliberately
// free of the L1-facing packages, and index/chain maps its go-ethereum-typed
// Source to and from this one (see its manifest conversion).
const (
	SourceInboxEvents = "inbox-events"
	SourceBlobTxs     = "blob-txs"
)

// Source is one entry of a chain head's ordered filter schedule as a Manifest
// records it (spec 10.5). It is the DAG-CBOR sibling of index/chain.Source: same
// fields, byte strings where that one has go-ethereum types, so that schema owes
// nothing to the indexer.
//
// Type decides which fields are meaningful, and the encoding omits the rest
// rather than nulling them: an inbox-events source has a Topic and no Senders, a
// blob-txs source the reverse, and a key that does not apply to a source's type
// is ABSENT on the wire (spec 10.5). OpenEnded is UntilBlock absent -- never an
// explicit null -- for the same one-encoding-per-meaning reason.
type Source struct {
	Type       string
	Address    []byte   // 20 bytes
	Topic      []byte   // 32 bytes; inbox-events only, nil otherwise
	Senders    [][]byte // 20 bytes each; blob-txs only, nil otherwise
	FromBlock  uint64
	UntilBlock uint64
	OpenEnded  bool // UntilBlock absent: the source runs open-ended
}

// Manifest is a chain block that fixes a head's source schedule (spec 10.5): a
// self-contained, content-addressed statement of the head's filter at a point in
// its history, chained to the statement it replaced through Prev.
//
// Prev is cid.Undef for the genesis manifest and a real link otherwise -- the one
// link a Manifest carries, which is what lets a recursive pin on a tip walk the
// whole chain back to genesis (spec 9). Sources is the WHOLE schedule as of this
// manifest, not a delta.
type Manifest struct {
	V       uint64
	Head    string
	Sources []Source
	Prev    cid.Cid // undef = genesis
}

// emptyAllowlistReason is spec 10.4's reason a blob-txs source MUST carry a
// non-empty allowlist, quoted where the manifest decoder enforces it so that a
// caller reads the argument and not just the rule. It mirrors the one
// index/chain states at config load: the two gates guard the same claim.
const emptyAllowlistReason = "anyone can send a blob transaction to any address, so a blob-txs source with no sender " +
	"allowlist would let any third party's blobs be recorded as this chain's history (spec 10.4)"

// Validate reports whether m is a well-formed manifest (spec 10.5). It is called
// on both encode and decode, so an in-memory Manifest and a decoded one are held
// to the same invariants.
func (m *Manifest) Validate() error {
	if m.V != ManifestVersion {
		return &UnknownVersionError{Object: "manifest", Got: int64(m.V)}
	}
	if !headNameRE.MatchString(m.Head) {
		return fmt.Errorf("schema: manifest head %q does not match [a-z0-9][a-z0-9-]*", m.Head)
	}
	if len(m.Sources) == 0 {
		// A manifest with no sources selects nothing and attests nothing; a head
		// that means "no blobs" has no manifest chain (spec 10.5), it does not have
		// an empty one.
		return fmt.Errorf("schema: manifest for head %q has no sources", m.Head)
	}
	for i := range m.Sources {
		if err := m.Sources[i].validate(); err != nil {
			return fmt.Errorf("schema: manifest source %d: %w", i, err)
		}
	}
	return nil
}

// validate holds one source to the structural rules of spec 10.4/10.5: the right
// byte-string widths, the type-specific fields present and the cross-type ones
// absent, and a non-empty sender allowlist on a blob-txs source.
func (s *Source) validate() error {
	if len(s.Address) != AddressSize {
		return fmt.Errorf("address is %d bytes, want %d", len(s.Address), AddressSize)
	}
	switch s.Type {
	case SourceInboxEvents:
		if len(s.Topic) != TopicSize {
			return fmt.Errorf("inbox-events topic is %d bytes, want %d", len(s.Topic), TopicSize)
		}
		if len(s.Senders) != 0 {
			return fmt.Errorf("inbox-events source carries %d senders; senders is a blob-txs field", len(s.Senders))
		}
	case SourceBlobTxs:
		if len(s.Senders) == 0 {
			return fmt.Errorf("blob-txs source has an empty sender allowlist: %s", emptyAllowlistReason)
		}
		for j, sender := range s.Senders {
			if len(sender) != AddressSize {
				return fmt.Errorf("sender %d is %d bytes, want %d", j, len(sender), AddressSize)
			}
		}
		if len(s.Topic) != 0 {
			return fmt.Errorf("blob-txs source carries a topic; topic is an inbox-events field")
		}
	default:
		return fmt.Errorf("unknown type %q; want %q or %q", s.Type, SourceInboxEvents, SourceBlobTxs)
	}
	if !s.OpenEnded && s.UntilBlock < s.FromBlock {
		return fmt.Errorf("until_block %d precedes from_block %d", s.UntilBlock, s.FromBlock)
	}
	return nil
}

// EncodeManifest encodes m to canonical DAG-CBOR and returns the block bytes and
// its CID. The keys the canonical form carries follow spec 10.5: type-specific
// fields are present only for their type, and until_block is present only for a
// bounded source.
func EncodeManifest(m *Manifest) ([]byte, cid.Cid, error) {
	if err := m.Validate(); err != nil {
		return nil, cid.Undef, err
	}
	if err := checkCBORInt("manifest", "v", m.V); err != nil {
		return nil, cid.Undef, err
	}
	for i := range m.Sources {
		s := &m.Sources[i]
		if err := checkCBORInt("manifest", fmt.Sprintf("sources[%d].from_block", i), s.FromBlock); err != nil {
			return nil, cid.Undef, err
		}
		if !s.OpenEnded {
			if err := checkCBORInt("manifest", fmt.Sprintf("sources[%d].until_block", i), s.UntilBlock); err != nil {
				return nil, cid.Undef, err
			}
		}
	}

	n, err := qp.BuildMap(basicnode.Prototype.Map, 4, func(ma datamodel.MapAssembler) {
		qp.MapEntry(ma, "v", qp.Int(int64(m.V)))
		qp.MapEntry(ma, "head", qp.String(m.Head))
		qp.MapEntry(ma, "sources", qp.List(int64(len(m.Sources)), func(la datamodel.ListAssembler) {
			for i := range m.Sources {
				qp.ListEntry(la, encodeSource(&m.Sources[i]))
			}
		}))
		qp.MapEntry(ma, "prev", nullableLink(m.Prev))
	})
	if err != nil {
		return nil, cid.Undef, fmt.Errorf("schema: building manifest node: %w", err)
	}
	return encodeNode(n)
}

// encodeSource assembles one source map. The keys are added here in a fixed
// order; encodeNode's RFC 7049 sort is what fixes their order on the wire, so a
// given logical source has exactly one encoding whatever order this adds them.
func encodeSource(s *Source) qp.Assemble {
	return qp.Map(6, func(ma datamodel.MapAssembler) {
		qp.MapEntry(ma, "type", qp.String(s.Type))
		qp.MapEntry(ma, "address", qp.Bytes(s.Address))
		if s.Type == SourceInboxEvents {
			qp.MapEntry(ma, "topic", qp.Bytes(s.Topic))
		}
		if s.Type == SourceBlobTxs {
			qp.MapEntry(ma, "senders", qp.List(int64(len(s.Senders)), func(la datamodel.ListAssembler) {
				for _, sender := range s.Senders {
					qp.ListEntry(la, qp.Bytes(sender))
				}
			}))
		}
		qp.MapEntry(ma, "from_block", qp.Int(int64(s.FromBlock)))
		if !s.OpenEnded {
			qp.MapEntry(ma, "until_block", qp.Int(int64(s.UntilBlock)))
		}
	})
}

var manifestKeys = []string{"v", "head", "sources", "prev"}

// sourceKeys is every key a Source map may carry. Which subset is required and
// which is forbidden depends on the type, and decodeSource enforces that; this is
// only the set outside of which a key is unknown.
var sourceKeys = []string{"type", "address", "topic", "senders", "from_block", "until_block"}

// DecodeManifest decodes a Manifest block. It rejects any object whose version is
// not ManifestVersion (spec 15) and any source whose encoding is not the single
// canonical spelling spec 10.5 admits -- a cross-type key, or an explicit null
// where until_block should be absent.
func DecodeManifest(block []byte) (*Manifest, error) {
	n, err := decodeNode(block)
	if err != nil {
		return nil, err
	}
	fields, err := mapFields("manifest", n, manifestKeys)
	if err != nil {
		return nil, err
	}
	if err := checkManifestVersion(fields["v"]); err != nil {
		return nil, err
	}

	m := &Manifest{V: ManifestVersion}
	if m.Head, err = fieldString("manifest", "head", fields["head"]); err != nil {
		return nil, err
	}

	sourcesNode := fields["sources"]
	if sourcesNode.Kind() != datamodel.Kind_List {
		return nil, fmt.Errorf("schema: manifest field \"sources\" must be a list, got %s", sourcesNode.Kind())
	}
	m.Sources = make([]Source, 0, sourcesNode.Length())
	it := sourcesNode.ListIterator()
	for !it.Done() {
		idx, srcNode, err := it.Next()
		if err != nil {
			return nil, fmt.Errorf("schema: iterating manifest sources: %w", err)
		}
		src, err := decodeSource(idx, srcNode)
		if err != nil {
			return nil, err
		}
		m.Sources = append(m.Sources, src)
	}

	if m.Prev, err = fieldNullableLink("manifest", "prev", fields["prev"]); err != nil {
		return nil, err
	}
	if err := m.Validate(); err != nil {
		return nil, err
	}
	return m, nil
}

// decodeSource decodes one source map, enforcing the exact key set its type
// allows before it reads any value. The two type-specific fields (topic,
// senders) are required for their own type and refused for the other, exactly as
// the config loader refuses a cross-type key -- a blob-txs source with a topic is
// not a source with a stray field, it is not a legal source.
func decodeSource(idx int64, n datamodel.Node) (Source, error) {
	if n.Kind() != datamodel.Kind_Map {
		return Source{}, fmt.Errorf("schema: manifest sources[%d] must be a map, got %s", idx, n.Kind())
	}
	present := make(map[string]datamodel.Node, len(sourceKeys))
	it := n.MapIterator()
	for !it.Done() {
		keyNode, valNode, err := it.Next()
		if err != nil {
			return Source{}, fmt.Errorf("schema: iterating manifest sources[%d]: %w", idx, err)
		}
		key, err := keyNode.AsString()
		if err != nil {
			return Source{}, fmt.Errorf("schema: manifest sources[%d] has a non-string field key: %w", idx, err)
		}
		if !slices.Contains(sourceKeys, key) {
			return Source{}, fmt.Errorf("schema: manifest sources[%d] has unknown field %q", idx, key)
		}
		present[key] = valNode
	}

	typeNode, ok := present["type"]
	if !ok {
		return Source{}, fmt.Errorf("schema: manifest sources[%d] is missing field \"type\"", idx)
	}
	typ, err := typeNode.AsString()
	if err != nil {
		return Source{}, fmt.Errorf("schema: manifest sources[%d] field \"type\" must be a string: %w", idx, err)
	}

	// The keys this type requires and, by omission, the ones it forbids. from_block
	// and until_block are common; the type-specific one is topic or senders.
	required := []string{"type", "address", "from_block"}
	allowed := map[string]bool{"type": true, "address": true, "from_block": true, "until_block": true}
	switch typ {
	case SourceInboxEvents:
		required, allowed["topic"] = append(required, "topic"), true
	case SourceBlobTxs:
		required, allowed["senders"] = append(required, "senders"), true
	default:
		return Source{}, fmt.Errorf("schema: manifest sources[%d] has unknown type %q; want %q or %q",
			idx, typ, SourceInboxEvents, SourceBlobTxs)
	}
	for key := range present {
		if !allowed[key] {
			return Source{}, fmt.Errorf("schema: manifest sources[%d] is type %q and carries field %q, which does not "+
				"apply to it (spec 10.5)", idx, typ, key)
		}
	}
	for _, key := range required {
		if _, ok := present[key]; !ok {
			return Source{}, fmt.Errorf("schema: manifest sources[%d] is type %q and is missing field %q", idx, typ, key)
		}
	}

	s := Source{Type: typ}
	if s.Address, err = sourceBytes(idx, "address", present["address"]); err != nil {
		return Source{}, err
	}
	if s.FromBlock, err = fieldUint("manifest", fmt.Sprintf("sources[%d] from_block", idx), present["from_block"]); err != nil {
		return Source{}, err
	}
	if ubNode, ok := present["until_block"]; ok {
		if ubNode.IsNull() {
			// Spec 10.5: an open-ended source omits until_block; a decoder MUST
			// reject an explicit null. Two spellings of "open-ended" would give one
			// source two CIDs and break the chain equality the whole scheme rests on.
			return Source{}, fmt.Errorf("schema: manifest sources[%d] has until_block: null; an open-ended source "+
				"omits the key entirely (spec 10.5)", idx)
		}
		if s.UntilBlock, err = fieldUint("manifest", fmt.Sprintf("sources[%d] until_block", idx), ubNode); err != nil {
			return Source{}, err
		}
	} else {
		s.OpenEnded = true
	}
	if typ == SourceInboxEvents {
		if s.Topic, err = sourceBytes(idx, "topic", present["topic"]); err != nil {
			return Source{}, err
		}
	}
	if typ == SourceBlobTxs {
		if s.Senders, err = decodeSenders(idx, present["senders"]); err != nil {
			return Source{}, err
		}
	}
	return s, nil
}

// sourceBytes reads a source's byte-string field. Width is checked by validate,
// once, over both encode and decode; this only insists it is bytes.
func sourceBytes(idx int64, key string, n datamodel.Node) ([]byte, error) {
	b, err := n.AsBytes()
	if err != nil {
		return nil, fmt.Errorf("schema: manifest sources[%d] field %q must be a byte string: %w", idx, key, err)
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out, nil
}

// decodeSenders reads a blob-txs source's allowlist.
func decodeSenders(idx int64, n datamodel.Node) ([][]byte, error) {
	if n.Kind() != datamodel.Kind_List {
		return nil, fmt.Errorf("schema: manifest sources[%d] field \"senders\" must be a list, got %s", idx, n.Kind())
	}
	out := make([][]byte, 0, n.Length())
	it := n.ListIterator()
	for !it.Done() {
		sIdx, sNode, err := it.Next()
		if err != nil {
			return nil, fmt.Errorf("schema: iterating manifest sources[%d] senders: %w", idx, err)
		}
		b, err := sNode.AsBytes()
		if err != nil {
			return nil, fmt.Errorf("schema: manifest sources[%d] senders[%d] must be a byte string: %w", idx, sIdx, err)
		}
		sender := make([]byte, len(b))
		copy(sender, b)
		out = append(out, sender)
	}
	return out, nil
}

// checkManifestVersion is checkVersion for the manifest's own version line (spec
// 15). It is separate because a manifest carries ManifestVersion, not
// SchemaVersion, and the two are free to move apart.
func checkManifestVersion(n datamodel.Node) error {
	v, err := n.AsInt()
	if err != nil {
		return fmt.Errorf("schema: manifest field \"v\" must be an integer: %w", err)
	}
	if v != ManifestVersion {
		return &UnknownVersionError{Object: "manifest", Got: v}
	}
	return nil
}
