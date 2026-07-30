package schema

import (
	"fmt"

	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
)

// blobPrefix: CIDv1, raw codec, sha2-256. One block per blob, never chunked
// (spec 2).
var blobPrefix = cid.Prefix{
	Version:  1,
	Codec:    cid.Raw,
	MhType:   multihash.SHA2_256,
	MhLength: -1,
}

// nodePrefix: CIDv1, dag-cbor codec, sha2-256, for Head, DirNode and Segment
// blocks (spec 2).
var nodePrefix = cid.Prefix{
	Version:  1,
	Codec:    cid.DagCBOR,
	MhType:   multihash.SHA2_256,
	MhLength: -1,
}

// BlobCID returns the CID of an EIP-4844 blob block. The block payload is the
// exact blob bytes, so anything that is not BlobSize bytes long is not a blob
// and is rejected rather than addressed.
func BlobCID(data []byte) (cid.Cid, error) {
	if len(data) != BlobSize {
		return cid.Undef, fmt.Errorf("schema: blob must be exactly %d bytes, got %d", BlobSize, len(data))
	}
	c, err := blobPrefix.Sum(data)
	if err != nil {
		return cid.Undef, fmt.Errorf("schema: hashing blob: %w", err)
	}
	return c, nil
}

// NodeCID returns the CID of an already-encoded index block. Prefer the CID
// returned alongside the bytes by the Encode functions; this exists for
// callers that hold block bytes read back from a store.
func NodeCID(block []byte) (cid.Cid, error) {
	c, err := nodePrefix.Sum(block)
	if err != nil {
		return cid.Undef, fmt.Errorf("schema: hashing index block: %w", err)
	}
	return c, nil
}
