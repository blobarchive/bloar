// Package schema defines the bloar DAG objects (spec section 3) together with
// their canonical DAG-CBOR encoding and CID derivation (spec section 2).
//
// # Codec decision
//
// Encoding goes through github.com/ipld/go-ipld-prime's dagcbor codec.
// The alternative considered was cbor-gen (thin generated marshallers, used
// widely in the Filecoin stack). ipld-prime wins here because CID
// stability across implementations is a conformance requirement (spec 2 and
// 13.2): its dagcbor encoder enforces the DAG-CBOR strict rules for us --
// definite lengths, map keys sorted RFC7049-style (length first, then
// bytewise), no floats, links as tag 42 -- whereas cbor-gen leaves canonical
// map ordering and link tagging to the discipline of whoever writes the
// struct definitions. We pay some encode-side allocation for that guarantee;
// index blocks are written once per update and this is not a hot path.
//
// The codec is an implementation detail. No ipld-prime type appears in this
// package's API: callers see Go structs, []byte, and cid.Cid, so the codec
// stays swappable without touching call sites.
//
// # Canonical form
//
// Encoding is a pure function of logical content: two objects that are equal
// as Go values MUST encode to identical bytes, and therefore to identical
// CIDs. Two rules beyond the DAG-CBOR strict rules are needed to hold that
// line, both enforced here:
//
//   - DirNode kids have trailing nulls trimmed on encode (see EncodeDirNode).
//   - Decoders reject unknown map keys, so that decode-then-re-encode can
//     never silently change a CID (which would break structural sharing).
//
// Decoding is deliberately more permissive than encoding in exactly one way:
// non-canonical map key order is accepted on input, and re-encoding
// normalises it. Readers accept what writers should not produce.
package schema
