// Package replica implements the crash-safe retention controller for a
// standalone Bloar archive replica backed by an operator-owned Kubo node.
//
// The controller owns no archive blockstore.  It writes one small DAG-CBOR
// generation anchor into Kubo and recursively pins that anchor.  The anchor's
// links are the current roots and manifest tips of every selected head, so one
// Kubo pin protects the complete multi-head generation while leaving every
// unrelated Kubo pin alone.
package replica

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"regexp"
	"slices"
	"strings"
	"time"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime/codec"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/fluent/qp"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/multiformats/go-multihash"
)

const (
	generationVersion  = 1
	maxReplicaIDBytes  = 64
	maxHeadNameBytes   = 128
	maxGenerationHeads = 64
)

var replicaIDPattern = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9._-]{0,62}[a-z0-9])?$`)

// Head is one authenticated, non-empty head in a replica generation.
// Manifest is undefined for a head with no manifest chain.
type Head struct {
	Name     string
	Root     cid.Cid
	Manifest cid.Cid
	SyncedTo uint64
}

// Generation is the complete all-head retention candidate authorized by one
// publication document. An empty Heads list is the canonical retention state
// for an authenticated publication which withdraws every selected head.
// UpdatedAt is part of the anchor so two publications with equal roots (or two
// empty publications) but different authority/freshness are still
// distinguishable in controller state.
type Generation struct {
	ReplicaID string
	UpdatedAt time.Time
	Heads     []Head
}

// Normalize validates g and returns a canonical copy sorted by head name.  It
// never aliases the caller's Heads slice.
func (g Generation) Normalize() (Generation, error) {
	if err := ValidateReplicaID(g.ReplicaID); err != nil {
		return Generation{}, err
	}
	if g.UpdatedAt.IsZero() || g.UpdatedAt.Unix() < 0 {
		return Generation{}, errors.New("replica: generation updated_at must be on or after the Unix epoch")
	}
	if len(g.Heads) > maxGenerationHeads {
		return Generation{}, fmt.Errorf("replica: generation must contain at most %d heads", maxGenerationHeads)
	}

	g.UpdatedAt = g.UpdatedAt.UTC().Truncate(time.Second)
	// Normalize nil and non-nil empty inputs to the same in-memory form. Block
	// encoding was already identical, but one canonical representation also
	// keeps callers and the durable JSON ledger from distinguishing them.
	g.Heads = append([]Head{}, g.Heads...)
	slices.SortFunc(g.Heads, func(a, b Head) int { return strings.Compare(a.Name, b.Name) })
	for i := range g.Heads {
		h := &g.Heads[i]
		if h.Name == "" || len(h.Name) > maxHeadNameBytes || strings.IndexByte(h.Name, 0) >= 0 {
			return Generation{}, fmt.Errorf("replica: head name must be 1-%d bytes and contain no NUL", maxHeadNameBytes)
		}
		if i > 0 && g.Heads[i-1].Name == h.Name {
			return Generation{}, fmt.Errorf("replica: generation repeats head %q", h.Name)
		}
		if !h.Root.Defined() {
			return Generation{}, fmt.Errorf("replica: head %q has an undefined root", h.Name)
		}
		if h.SyncedTo > math.MaxInt64 {
			return Generation{}, fmt.Errorf("replica: head %q synced_to %d exceeds DAG-CBOR's integer range", h.Name, h.SyncedTo)
		}
	}
	return g, nil
}

// ValidateReplicaID validates the stable identity used in anchor bytes and the
// default reserved Kubo pin name. Surrounding whitespace is rejected rather
// than normalized so those two uses can never drift across a restart.
func ValidateReplicaID(value string) error {
	if value != strings.TrimSpace(value) || len(value) == 0 || len(value) > maxReplicaIDBytes || !replicaIDPattern.MatchString(value) {
		return fmt.Errorf("replica: replica ID %q must be 1-%d lowercase ASCII letters, digits, '.', '_' or '-', without surrounding whitespace", value, maxReplicaIDBytes)
	}
	return nil
}

// Block returns the canonical DAG-CBOR generation anchor and its CID.
func (g Generation) Block() (blocks.Block, error) {
	normalized, err := g.Normalize()
	if err != nil {
		return nil, err
	}

	n, err := qp.BuildMap(basicnode.Prototype.Map, 4, func(ma datamodel.MapAssembler) {
		qp.MapEntry(ma, "v", qp.Int(generationVersion))
		qp.MapEntry(ma, "replica", qp.String(normalized.ReplicaID))
		qp.MapEntry(ma, "updated_at", qp.Int(normalized.UpdatedAt.Unix()))
		qp.MapEntry(ma, "heads", qp.List(int64(len(normalized.Heads)), func(la datamodel.ListAssembler) {
			for _, h := range normalized.Heads {
				head := h
				qp.ListEntry(la, qp.Map(4, func(hm datamodel.MapAssembler) {
					qp.MapEntry(hm, "name", qp.String(head.Name))
					qp.MapEntry(hm, "root", qp.Link(cidlink.Link{Cid: head.Root}))
					qp.MapEntry(hm, "synced_to", qp.Int(int64(head.SyncedTo)))
					if head.Manifest.Defined() {
						qp.MapEntry(hm, "manifest", qp.Link(cidlink.Link{Cid: head.Manifest}))
					}
				}))
			}
		}))
	})
	if err != nil {
		return nil, fmt.Errorf("replica: building generation anchor: %w", err)
	}

	var buf bytes.Buffer
	options := dagcbor.EncodeOptions{AllowLinks: true, MapSortMode: codec.MapSortMode_RFC7049}
	if err := options.Encode(n, &buf); err != nil {
		return nil, fmt.Errorf("replica: encoding generation anchor: %w", err)
	}
	data := buf.Bytes()
	anchor, err := cid.Prefix{
		Version:  1,
		Codec:    cid.DagCBOR,
		MhType:   multihash.SHA2_256,
		MhLength: -1,
	}.Sum(data)
	if err != nil {
		return nil, fmt.Errorf("replica: hashing generation anchor: %w", err)
	}
	block, err := blocks.NewBlockWithCid(data, anchor)
	if err != nil {
		return nil, fmt.Errorf("replica: framing generation anchor: %w", err)
	}
	return block, nil
}

// Equal reports semantic equality after normalization.
func (g Generation) Equal(other Generation) bool {
	a, err := g.Normalize()
	if err != nil {
		return false
	}
	b, err := other.Normalize()
	if err != nil {
		return false
	}
	if a.ReplicaID != b.ReplicaID || !a.UpdatedAt.Equal(b.UpdatedAt) || len(a.Heads) != len(b.Heads) {
		return false
	}
	for i := range a.Heads {
		if a.Heads[i] != b.Heads[i] {
			return false
		}
	}
	return true
}
