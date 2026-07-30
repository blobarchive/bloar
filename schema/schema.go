package schema

import (
	"fmt"
	"regexp"

	"github.com/ipfs/go-cid"
)

// Constants fixed by EIP-4844 and the beacon chain (spec section 1).
const (
	// BlobSize is the exact size of an EIP-4844 blob, in bytes.
	BlobSize = 131072

	// VersionedHashSize is the size of a versioned hash, in bytes.
	VersionedHashSize = 32

	// MainnetGenesisTime is the unix timestamp of mainnet beacon genesis.
	MainnetGenesisTime = 1606824023

	// SecondsPerSlot is the beacon slot duration; configurable per network.
	SecondsPerSlot = 12

	// DencunMainnetSlot is the first possible blob slot on mainnet.
	DencunMainnetSlot = 8626176

	// MaxBlobsPerSlotCeiling is a conservative protocol ceiling on the number of
	// blobs a single slot can carry. It is deliberately well above any fork's
	// MAX_BLOBS_PER_BLOCK (Dencun 6, Pectra 9, and room to spare for later
	// raises), not the exact per-fork maximum: it exists to bound the read path
	// by construction, not to describe a network.
	//
	// Two invariants ride on it. A stored Segment Row may carry at most this many
	// entries, so an unfiltered read of a covered slot is bounded before a byte is
	// read (spec 7.1). And it is the default ceiling on how many versioned_hashes
	// one blobs request may name, duplicates included, so a filtered read is
	// bounded too. Both are the mitigation for the amplification of finding
	// the safety boundary.
	MaxBlobsPerSlotCeiling = 128
)

// SchemaVersion is the value carried in the "v" field of every DAG object.
// Readers reject objects carrying any other major version (spec 15).
const SchemaVersion = 1

// headNameRE is the permitted shape of a head name (spec 3.1).
var headNameRE = regexp.MustCompile(`^[a-z0-9][a-z0-9-]*$`)

// ValidateHeadName enforces the canonical public head-name grammar shared by
// schema objects and configuration boundaries.
func ValidateHeadName(name string) error {
	if !headNameRE.MatchString(name) {
		return fmt.Errorf("schema: head name %q does not match [a-z0-9][a-z0-9-]*", name)
	}
	return nil
}

// VersionedHash is 0x01 || sha256(kzg_commitment)[1:]. It is carried in the
// DAG as a plain 32-byte CBOR byte string, never as a link: it is not a CID.
type VersionedHash [VersionedHashSize]byte

// Head is the root object of one head (spec 3.1). A new Head block is written
// on every update and the latest CID published out-of-band.
//
// SyncedTo is nil when the head is empty. Dir and Open are cid.Undef when
// null; cid.Undef is this package's representation of a null link throughout.
type Head struct {
	Name       string
	Net        string
	OriginSlot uint64
	SyncedTo   *uint64
	SegBits    uint64
	FanoutBits uint64
	DirDepth   uint64
	Dir        cid.Cid
	Open       cid.Cid
}

// Segment holds every ref for the window [slot0, slot0 + 2^seg_bits) (spec
// 3.2). A fully-empty window seals to no object at all, never to an empty
// Segment.
type Segment struct {
	Slot0 uint64
	Rows  []Row
}

// Row is the refs for one blob-carrying slot. It encodes as the 2-element
// array [slot, entries].
type Row struct {
	Slot    uint64
	Entries []RefEntry
}

// RefEntry maps one versioned hash to its blob block. It encodes as the
// 2-element array [vh, blob]. Entry order within a row is part of the content
// and therefore affects the CID.
type RefEntry struct {
	VH   VersionedHash
	Blob cid.Cid
}

// DirNode is one page of the implicit radix tree of sealed segment CIDs (spec
// 3.3). A cid.Undef kid is a null: no refs in that range.
type DirNode struct {
	Kids []cid.Cid
}

// UnknownVersionError reports a DAG object carrying a schema version this
// build does not understand (spec 15).
type UnknownVersionError struct {
	Object string
	Got    int64
}

func (e *UnknownVersionError) Error() string {
	return fmt.Sprintf("schema: %s has unknown version %d, this build supports v%d", e.Object, e.Got, SchemaVersion)
}

// Validate reports whether h satisfies the invariants spec 3.1 states for a
// Head. It is called on both encode and decode.
func (h *Head) Validate() error {
	if err := ValidateHeadName(h.Name); err != nil {
		return err
	}
	// "dir: link | null (null iff dir_depth == 0)" -- spec 3.1.
	if (h.DirDepth == 0) == h.Dir.Defined() {
		return fmt.Errorf("schema: head %q has dir_depth %d but dir defined=%t, dir must be null iff dir_depth is 0",
			h.Name, h.DirDepth, h.Dir.Defined())
	}
	if h.SyncedTo != nil && *h.SyncedTo < h.OriginSlot {
		return fmt.Errorf("schema: head %q synced_to %d precedes origin_slot %d", h.Name, *h.SyncedTo, h.OriginSlot)
	}
	return nil
}

// Validate reports whether s satisfies the invariants spec 3.2 states for a
// Segment: rows strictly ascending by slot, every row non-empty, every blob
// link real. Window bounds are not checked here: seg_bits lives on the Head,
// which this package deliberately does not thread through.
func (s *Segment) Validate() error {
	for i, r := range s.Rows {
		if i > 0 && r.Slot <= s.Rows[i-1].Slot {
			return fmt.Errorf("schema: segment slot0=%d rows not strictly ascending: row %d slot %d follows slot %d",
				s.Slot0, i, r.Slot, s.Rows[i-1].Slot)
		}
		if r.Slot < s.Slot0 {
			return fmt.Errorf("schema: segment slot0=%d has row %d with slot %d before the window start", s.Slot0, i, r.Slot)
		}
		if len(r.Entries) == 0 {
			return fmt.Errorf("schema: segment slot0=%d row %d (slot %d) has no entries", s.Slot0, i, r.Slot)
		}
		// The read-path bound of the safety boundary: no covered slot may resolve to
		// more than the protocol ceiling of blobs, so an unfiltered response is
		// bounded before it is read. Enforced on both encode and decode, so a
		// segment fetched from another node cannot smuggle an oversized row in.
		if len(r.Entries) > MaxBlobsPerSlotCeiling {
			return fmt.Errorf("schema: segment slot0=%d row %d (slot %d) has %d entries, more than the %d-blob ceiling per slot",
				s.Slot0, i, r.Slot, len(r.Entries), MaxBlobsPerSlotCeiling)
		}
		for j, e := range r.Entries {
			if !e.Blob.Defined() {
				return fmt.Errorf("schema: segment slot0=%d row %d (slot %d) entry %d has an undefined blob link",
					s.Slot0, i, r.Slot, j)
			}
		}
	}
	return nil
}

// Validate reports whether d is encodable. Fanout bounds are not checked
// here: fanout_bits lives on the Head.
func (d *DirNode) Validate() error { return nil }
