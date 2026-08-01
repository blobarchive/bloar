// Package pointerhint implements bounded DHT hints for the small set of
// content identifiers that describe a Bloar head's current publication state.
//
// It is deliberately separate from Bitswap routing. Finding a provider through
// this package only connects a peer for a caller-supplied, semantically typed
// pointer; it never installs the DHT as a generic provider finder for arbitrary
// block requests.
package pointerhint

import (
	"errors"
	"fmt"

	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
)

// Kind is one of the three pointer roles Bloar is allowed to advertise.
type Kind uint8

const (
	Root Kind = iota + 1
	Manifest
	Document
)

func (k Kind) String() string {
	switch k {
	case Root:
		return "root"
	case Manifest:
		return "manifest"
	case Document:
		return "document"
	default:
		return fmt.Sprintf("Kind(%d)", uint8(k))
	}
}

// Pointer is one caller-known current root, manifest tip, or publication
// document CID. Kind is a trusted in-process assertion, not authentication:
// this package can validate Bloar's CID profile and codec but cannot prove that
// an otherwise valid DAG-CBOR block is the root or manifest selected by an
// authenticated publication document. Callers must construct Pointer values
// only from that existing trust path and must never expose this API directly to
// untrusted CID input. The narrow type makes that policy explicit; keeping the
// DHT out of Bitswap remains the enforceable no-generic-routing boundary.
type Pointer struct {
	Kind Kind
	CID  cid.Cid
}

func (p Pointer) validate() error {
	switch p.Kind {
	case Root, Manifest, Document:
	default:
		return fmt.Errorf("pointerhint: invalid pointer kind %d", p.Kind)
	}
	if !p.CID.Defined() {
		return fmt.Errorf("pointerhint: %s CID must be defined", p.Kind)
	}
	prefix := p.CID.Prefix()
	if prefix.Version != 1 || prefix.MhType != multihash.SHA2_256 || prefix.MhLength != 32 {
		return fmt.Errorf("pointerhint: %s CID %s must be CIDv1 with a 32-byte sha2-256 multihash", p.Kind, p.CID)
	}
	switch p.Kind {
	case Root, Manifest:
		if prefix.Codec != cid.DagCBOR {
			return fmt.Errorf("pointerhint: %s %s is not a dag-cbor CID", p.Kind, p.CID)
		}
	case Document:
		if prefix.Codec != cid.Raw {
			return fmt.Errorf("pointerhint: publication document %s is not a raw CID", p.CID)
		}
	}
	return nil
}

// Set is the exact current pointer set for one selected head/profile. Manifest
// and Document are optional. An empty set withdraws every future local
// advertisement; provider records already accepted by remote DHT nodes cannot
// be withdrawn and expire naturally.
type Set struct {
	Root     cid.Cid
	Manifest cid.Cid
	Document cid.Cid
}

func (s Set) pointers() ([]Pointer, error) {
	if !s.Root.Defined() && (s.Manifest.Defined() || s.Document.Defined()) {
		return nil, errors.New("pointerhint: a non-empty pointer set requires its current root")
	}
	items := make([]Pointer, 0, 3)
	if s.Root.Defined() {
		items = append(items, Pointer{Kind: Root, CID: s.Root})
	}
	if s.Manifest.Defined() {
		items = append(items, Pointer{Kind: Manifest, CID: s.Manifest})
	}
	if s.Document.Defined() {
		items = append(items, Pointer{Kind: Document, CID: s.Document})
	}

	// A CID can occupy at most one slot. Treating one identifier as two current
	// semantic objects would make retry and eligibility rules ambiguous.
	seen := make(map[string]Kind, len(items))
	for _, item := range items {
		if err := item.validate(); err != nil {
			return nil, err
		}
		key := item.CID.KeyString()
		if previous, ok := seen[key]; ok {
			return nil, fmt.Errorf("pointerhint: CID %s is both %s and %s", item.CID, previous, item.Kind)
		}
		seen[key] = item.Kind
	}
	return items, nil
}
