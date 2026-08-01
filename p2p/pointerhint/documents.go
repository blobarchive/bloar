package pointerhint

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/hashicorp/golang-lru/v2"
	"github.com/ipfs/boxo/blockstore"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
)

// DefaultVerifiedDocumentCapacity bounds the publication documents retained
// outside the archive's GC-owned blockstore when no explicit capacity is set.
const DefaultVerifiedDocumentCapacity = 16

const (
	// MaxVerifiedActiveDocuments covers one distinct authenticated source
	// document per configured head plus the daemon's one locally republished
	// document. The latter is a separate current trust path on a follower which
	// publishes its registry under its own IPNS name.
	MaxVerifiedActiveDocuments = MaxCoordinatorHeads + MaxCoordinatorExtraDocuments
	// MaxVerifiedTransitionDocuments permits one complete old and one complete
	// new active set to overlap while the provider schedule changes. That overlap
	// keeps the old schedule eligible until Coordinator.ReplaceAll succeeds.
	MaxVerifiedTransitionDocuments = 2 * MaxVerifiedActiveDocuments
)

// VerifiedDocuments is the narrow eligibility question Provider needs. A
// normal local blockstore is intentionally insufficient: content addressing
// proves bytes match a CID, not that a trusted signer published those bytes.
type VerifiedDocuments interface {
	HasVerified(context.Context, cid.Cid) (bool, error)
}

// VerifiedDocumentStore layers a bounded, GC-independent publication-document
// cache over the node's ordinary serving blockstore.
//
// RetainAfterVerification is an explicit trust boundary. This type does not
// duplicate Bloar's publication verifier: its caller must first authenticate
// and admit the exact document through the existing follower/writer trust
// path. This type independently rechecks only the facts it owns: raw codec,
// CID/content identity, bounded retention, and whether a document is eligible
// for pointer advertisement. It produces no authentication result of its own;
// Put and PutMany never grant eligibility.
type VerifiedDocumentStore struct {
	base blockstore.Blockstore
	docs *lru.Cache[string, blocks.Block]

	mu sync.RWMutex
	// active is the explicitly protected current-document set. Unlike docs it
	// is not an eviction cache: a quiet followed source must remain serveable and
	// provider-eligible while a busy local writer churns the history LRU.
	active map[string]blocks.Block
}

var _ blockstore.Blockstore = (*VerifiedDocumentStore)(nil)
var _ VerifiedDocuments = (*VerifiedDocumentStore)(nil)

// NewVerifiedDocumentStore constructs a serving view. Zero capacity selects
// DefaultVerifiedDocumentCapacity; a negative capacity is invalid.
func NewVerifiedDocumentStore(base blockstore.Blockstore, capacity int) (*VerifiedDocumentStore, error) {
	if base == nil {
		return nil, errors.New("pointerhint: verified document store base must not be nil")
	}
	if capacity < 0 {
		return nil, errors.New("pointerhint: verified document capacity must not be negative")
	}
	if capacity == 0 {
		capacity = DefaultVerifiedDocumentCapacity
	}
	docs, err := lru.New[string, blocks.Block](capacity)
	if err != nil {
		return nil, fmt.Errorf("pointerhint: building verified document cache: %w", err)
	}
	return &VerifiedDocumentStore{base: base, docs: docs, active: make(map[string]blocks.Block)}, nil
}

// RetainAfterVerification admits the exact block an upstream trust verifier
// has already accepted. Callers MUST NOT use this as a substitute for document
// schema, network, signer, freshness, or anti-replay checks.
func (s *VerifiedDocumentStore) RetainAfterVerification(block blocks.Block) error {
	owned, err := verifiedDocumentCopy(block)
	if err != nil {
		return err
	}
	s.docs.Add(owned.Cid().KeyString(), owned)
	return nil
}

// StageCurrentAfterVerification adds a complete prospective active set without
// removing the installed one. A daemon uses it before changing its provider
// schedule, so the old exact document pointers remain eligible until the new
// schedule is accepted. Every staged block also enters bounded verified history.
//
// The call is transactional: invalid bytes or a capacity violation leaves the
// active set unchanged. Call ReplaceCurrentDocuments after the matching
// Coordinator.ReplaceAll, or with the previous CID set to roll back.
// One daemon-owned transition lock must serialize this method with
// ReplaceCurrentDocuments; the two calls intentionally form a transaction with
// the separately synchronized Coordinator.
func (s *VerifiedDocumentStore) StageCurrentAfterVerification(documents []blocks.Block) error {
	staged := make(map[string]blocks.Block, len(documents))
	for _, document := range documents {
		owned, err := verifiedDocumentCopy(document)
		if err != nil {
			return err
		}
		staged[owned.Cid().KeyString()] = owned
	}
	if len(staged) > MaxVerifiedActiveDocuments {
		return fmt.Errorf("pointerhint: active document set has %d distinct blocks, exceeds limit %d", len(staged), MaxVerifiedActiveDocuments)
	}

	s.mu.Lock()
	if len(s.active)+len(staged) > MaxVerifiedTransitionDocuments {
		// Count exact union below before refusing: a document retained by both
		// generations consumes one slot, not two.
		union := len(s.active)
		for key := range staged {
			if _, exists := s.active[key]; !exists {
				union++
			}
		}
		if union > MaxVerifiedTransitionDocuments {
			s.mu.Unlock()
			return fmt.Errorf("pointerhint: active document transition has %d blocks, exceeds limit %d", union, MaxVerifiedTransitionDocuments)
		}
	}
	for key, document := range staged {
		s.active[key] = document
	}
	s.mu.Unlock()

	for _, document := range staged {
		s.docs.Add(document.Cid().KeyString(), document)
	}
	return nil
}

// ReplaceCurrentDocuments commits the exact protected active CID set. Every
// requested CID must already have been staged; this makes a typo fail without
// silently promoting an ordinary base-blockstore write to verified status.
// Removed documents remain only in bounded verified history.
// Calls must share the external transition serialization documented on
// StageCurrentAfterVerification.
func (s *VerifiedDocumentStore) ReplaceCurrentDocuments(current []cid.Cid) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	next := make(map[string]blocks.Block, len(current))
	for _, c := range current {
		if err := (Pointer{Kind: Document, CID: c}).validate(); err != nil {
			return err
		}
		key := c.KeyString()
		if _, duplicate := next[key]; duplicate {
			continue
		}
		if len(next) == MaxVerifiedActiveDocuments {
			return fmt.Errorf("pointerhint: active document set has more than %d distinct CIDs", MaxVerifiedActiveDocuments)
		}
		document, ok := s.active[key]
		if !ok {
			return fmt.Errorf("pointerhint: current publication document %s was not staged", c)
		}
		next[key] = document
	}
	s.active = next
	return nil
}

func verifiedDocumentCopy(block blocks.Block) (blocks.Block, error) {
	if block == nil {
		return nil, errors.New("pointerhint: verified publication document must have a defined CID")
	}
	c := block.Cid()
	if !c.Defined() {
		return nil, errors.New("pointerhint: verified publication document must have a defined CID")
	}
	if err := (Pointer{Kind: Document, CID: c}).validate(); err != nil {
		return nil, err
	}
	// Copy the bytes and recompute the CID explicitly. go-block-format's
	// NewBlockWithCid only performs this check in its optional debug mode, so it
	// is not itself an integrity boundary.
	data := append([]byte(nil), block.RawData()...)
	computed, err := c.Prefix().Sum(data)
	if err != nil {
		return nil, fmt.Errorf("pointerhint: hashing verified publication document %s: %w", c, err)
	}
	if !computed.Equals(c) {
		return nil, fmt.Errorf("pointerhint: verified publication document bytes do not match CID %s", c)
	}
	// The owned copy prevents a caller from changing retained bytes after
	// admission.
	owned, err := blocks.NewBlockWithCid(data, c)
	if err != nil {
		return nil, fmt.Errorf("pointerhint: verified publication document %s fails CID validation: %w", c, err)
	}
	return owned, nil
}

// HasVerified reports only explicit verified-cache admission. A matching CID
// in the base blockstore does not pass this gate.
func (s *VerifiedDocumentStore) HasVerified(ctx context.Context, c cid.Cid) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	s.mu.RLock()
	_, active := s.active[c.KeyString()]
	s.mu.RUnlock()
	if active {
		return true, nil
	}
	_, ok := s.docs.Get(c.KeyString())
	return ok, nil
}

func (s *VerifiedDocumentStore) Get(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.RLock()
	active, ok := s.active[c.KeyString()]
	if ok {
		data := append([]byte(nil), active.RawData()...)
		s.mu.RUnlock()
		return blocks.NewBlockWithCid(data, active.Cid())
	}
	s.mu.RUnlock()
	if block, ok := s.docs.Get(c.KeyString()); ok {
		// BasicBlock.RawData exposes its backing slice. Return a fresh copy so a
		// consumer cannot mutate the cache's retained verified bytes.
		return blocks.NewBlockWithCid(append([]byte(nil), block.RawData()...), block.Cid())
	}
	return s.base.Get(ctx, c)
}

func (s *VerifiedDocumentStore) Has(ctx context.Context, c cid.Cid) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	s.mu.RLock()
	_, active := s.active[c.KeyString()]
	s.mu.RUnlock()
	if active {
		return true, nil
	}
	if _, ok := s.docs.Get(c.KeyString()); ok {
		return true, nil
	}
	return s.base.Has(ctx, c)
}

func (s *VerifiedDocumentStore) GetSize(ctx context.Context, c cid.Cid) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	s.mu.RLock()
	active, ok := s.active[c.KeyString()]
	if ok {
		size := len(active.RawData())
		s.mu.RUnlock()
		return size, nil
	}
	s.mu.RUnlock()
	if block, ok := s.docs.Get(c.KeyString()); ok {
		return len(block.RawData()), nil
	}
	return s.base.GetSize(ctx, c)
}

func (s *VerifiedDocumentStore) Put(ctx context.Context, block blocks.Block) error {
	return s.base.Put(ctx, block)
}

func (s *VerifiedDocumentStore) PutMany(ctx context.Context, blocks []blocks.Block) error {
	return s.base.PutMany(ctx, blocks)
}

// DeleteBlock and AllKeysChan deliberately expose only the GC-owned base.
// Verified documents leave through bounded LRU eviction, never archive GC.
func (s *VerifiedDocumentStore) DeleteBlock(ctx context.Context, c cid.Cid) error {
	return s.base.DeleteBlock(ctx, c)
}

func (s *VerifiedDocumentStore) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	return s.base.AllKeysChan(ctx)
}
