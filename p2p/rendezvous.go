package p2p

import (
	"encoding/binary"
	"fmt"
	"unicode/utf8"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
)

// RendezvousNamespaceVersion is the wire version of the byte string hashed by
// RendezvousCID. It is exported because non-Go peers (including stock Kubo
// tooling) need to be able to compute exactly the same meeting point.
const RendezvousNamespaceVersion uint16 = 1

// rendezvousDomain separates Bloar rendezvous keys from every other use of a
// raw sha2-256 CID. The NUL is part of the format, not string punctuation.
const rendezvousDomain = "bloar/rendezvous\x00"

// maxRendezvousComponentBytes bounds the one allocation RendezvousCID makes.
// Network and head names are operator configuration, not archive data; 4 KiB
// each is deliberately far beyond a useful name while still making the helper
// safe to expose at a config boundary.
const maxRendezvousComponentBytes = 4 << 10

// RendezvousCID returns the deterministic provider key for one (network, head).
//
// The preimage is deliberately specified as bytes rather than by an example
// string, because separators make pairs such as ("a/b", "c") and
// ("a", "b/c") ambiguous. Its exact layout is:
//
//	"bloar/rendezvous\0" || uint16be(version) ||
//	uint32be(len(net)) || utf8(net) || uint32be(len(head)) || utf8(head)
//
// The result is CIDv1(raw, sha2-256(preimage)). The preimage is also the
// canonical tiny raw block returned by RendezvousBlock. Embedded DHT clients
// may provide an arbitrary key, but stock Kubo's provide/once command requires
// the CID to exist in its local blockstore; making the namespace itself a real
// deterministic block lets both backends use the identical stable key.
func RendezvousCID(net, head string) (cid.Cid, error) {
	preimage, err := rendezvousPreimage(net, head)
	if err != nil {
		return cid.Undef, err
	}
	digest, err := mh.Sum(preimage, mh.SHA2_256, -1)
	if err != nil {
		return cid.Undef, fmt.Errorf("p2p: hashing rendezvous namespace: %w", err)
	}
	return cid.NewCidV1(cid.Raw, digest), nil
}

// RendezvousBlock returns the canonical small raw block whose CID is the
// stable rendezvous key for (net, head). A Kubo-backed replica stores and pins
// this block before advertising it so Kubo GC cannot invalidate later provider
// refreshes. The bytes contain only the public namespace components.
func RendezvousBlock(net, head string) (blocks.Block, error) {
	preimage, err := rendezvousPreimage(net, head)
	if err != nil {
		return nil, err
	}
	key, err := RendezvousCID(net, head)
	if err != nil {
		return nil, err
	}
	block, err := blocks.NewBlockWithCid(preimage, key)
	if err != nil {
		return nil, fmt.Errorf("p2p: constructing rendezvous block: %w", err)
	}
	return block, nil
}

func rendezvousPreimage(net, head string) ([]byte, error) {
	if err := validateRendezvousComponent("network", net); err != nil {
		return nil, err
	}
	if err := validateRendezvousComponent("head", head); err != nil {
		return nil, err
	}

	preimage := make([]byte, 0, len(rendezvousDomain)+2+4+len(net)+4+len(head))
	preimage = append(preimage, rendezvousDomain...)
	preimage = binary.BigEndian.AppendUint16(preimage, RendezvousNamespaceVersion)
	preimage = binary.BigEndian.AppendUint32(preimage, uint32(len(net)))
	preimage = append(preimage, net...)
	preimage = binary.BigEndian.AppendUint32(preimage, uint32(len(head)))
	preimage = append(preimage, head...)

	return preimage, nil
}

func validateRendezvousComponent(kind, value string) error {
	switch {
	case value == "":
		return fmt.Errorf("p2p: rendezvous %s must not be empty", kind)
	case !utf8.ValidString(value):
		return fmt.Errorf("p2p: rendezvous %s is not valid UTF-8", kind)
	case len(value) > maxRendezvousComponentBytes:
		return fmt.Errorf("p2p: rendezvous %s is %d bytes, limit %d", kind, len(value), maxRendezvousComponentBytes)
	default:
		return nil
	}
}
