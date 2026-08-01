package pointerhint

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/ipfs/go-cid"
)

const (
	// DefaultCoordinatorMaxHeads bounds aggregate state when the caller does
	// not choose a smaller deployment-specific limit.
	DefaultCoordinatorMaxHeads = 64
	// MaxCoordinatorHeads is the hard process safety ceiling. Each head
	// contributes at most three pointers.
	MaxCoordinatorHeads = DefaultCoordinatorMaxHeads
	// MaxCoordinatorExtraDocuments permits one node-local current publication
	// document in addition to the documents already owned by head Sets. This
	// lets a follower relay its own signed publication document without
	// consuming a configured head slot or withdrawing the upstream document.
	MaxCoordinatorExtraDocuments = 1
	// MaxCoordinatorPointers is the resulting hard bound on a flat provider
	// schedule, before same-kind shared CIDs are deduplicated.
	MaxCoordinatorPointers = MaxCoordinatorHeads*3 + MaxCoordinatorExtraDocuments
	// MaxCoordinatorHeadNameBytes bounds map-key memory even when this API is
	// accidentally reached before the schema's stricter head-name validation.
	MaxCoordinatorHeadNameBytes = 4 << 10
)

// CoordinatorConfig constructs one process-wide exact-pointer provider. A
// daemon should create one Coordinator, rather than one Provider per head, so
// every head shares Provider.MinWriteInterval and the fixed-size wake slot.
type CoordinatorConfig struct {
	Provider ProviderConfig
	MaxHeads int
}

// Coordinator combines the exact current pointers for multiple local or
// followed heads into one bounded, deduplicated Provider schedule.
//
// A CID shared by several heads is scheduled once while any head still names
// it. The same CID may never have different semantic kinds anywhere in the
// aggregate. ReplaceAll, UpdateHead, and RemoveHead are transactional: an
// invalid or conflicting candidate leaves both the accepted head map and
// active provider schedule unchanged. ReplaceAll is the preferred path when
// one publication document changes several heads, because the provider never
// observes an incoherent sequence of partially updated heads.
//
// Document eligibility is not a caller-supplied boolean. Documents still pass
// Provider's VerifiedDocuments retention check immediately before every DHT
// write, so eviction or loss of verified retention fails closed even after an
// update was accepted.
type Coordinator struct {
	provider *Provider
	maxHeads int

	mu        sync.Mutex
	heads     map[string]Set
	documents []cid.Cid
	closed    bool
	once      sync.Once
	closeErr  error
}

// NewCoordinator starts an idle aggregate provider. Zero MaxHeads selects
// DefaultCoordinatorMaxHeads; values outside [0, MaxCoordinatorHeads] are
// invalid.
func NewCoordinator(ctx context.Context, cfg CoordinatorConfig) (*Coordinator, error) {
	maxHeads := cfg.MaxHeads
	if maxHeads < 0 {
		return nil, errors.New("pointerhint: CoordinatorConfig.MaxHeads must not be negative")
	}
	if maxHeads == 0 {
		maxHeads = DefaultCoordinatorMaxHeads
	}
	if maxHeads > MaxCoordinatorHeads {
		return nil, fmt.Errorf("pointerhint: CoordinatorConfig.MaxHeads %d exceeds hard limit %d", maxHeads, MaxCoordinatorHeads)
	}
	provider, err := NewProvider(ctx, cfg.Provider)
	if err != nil {
		return nil, err
	}
	return &Coordinator{
		provider: provider,
		maxHeads: maxHeads,
		heads:    make(map[string]Set),
	}, nil
}

// ReplaceAll atomically replaces the exact pointer sets for every configured
// head. The complete candidate is validated, globally type-checked, and
// deduplicated before the provider schedule is updated exactly once. A nil or
// empty map withdraws every head and pointer.
//
// ReplaceAll clones heads before installing it; later mutation of the caller's
// map cannot change coordinator state. On any validation, cardinality,
// cross-kind, provider, or closed-state error, both the previous head map and
// provider schedule remain installed. For compatibility, it also clears every
// extra node-local document previously installed by ReplaceAllWithDocuments.
func (c *Coordinator) ReplaceAll(heads map[string]Set) error {
	return c.ReplaceAllWithDocuments(heads, nil)
}

// ReplaceAllWithDocuments atomically replaces every configured head and up
// to MaxCoordinatorExtraDocuments node-local current publication documents.
// Extra documents do not consume head slots. They are combined with the head
// Sets before one globally type-checked, same-kind-deduplicated provider
// schedule is installed.
//
// The input map and slice are cloned before installation. On any validation,
// cardinality, cross-kind, provider, or closed-state error, the previous heads,
// extra documents, and provider schedule remain installed. Call ReplaceAll to
// explicitly clear all extra documents while replacing the heads.
func (c *Coordinator) ReplaceAllWithDocuments(heads map[string]Set, documents []cid.Cid) error {
	candidate, candidateDocuments, items, err := prepareCoordinatorSnapshot(heads, documents, c.maxHeads)
	if err != nil {
		return err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return errors.New("pointerhint: coordinator is closed")
	}
	if err := c.provider.updatePointers(items); err != nil {
		return err
	}
	c.heads = candidate
	c.documents = candidateDocuments
	return nil
}

// ValidateAllWithDocuments applies every bound, CID-profile, and cross-kind
// check used by ReplaceAllWithDocuments without changing the provider
// schedule. A publication relay uses this before its load-bearing IPNS commit,
// then calls ReplaceAllWithDocuments only after that commit succeeds.
//
// Validation is deliberately repeated by ReplaceAllWithDocuments: the
// preflight result grants no authority and cannot become a stale capability.
func (c *Coordinator) ValidateAllWithDocuments(heads map[string]Set, documents []cid.Cid) error {
	if _, _, _, err := prepareCoordinatorSnapshot(heads, documents, c.maxHeads); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return errors.New("pointerhint: coordinator is closed")
	}
	return nil
}

func prepareCoordinatorSnapshot(
	heads map[string]Set,
	documents []cid.Cid,
	maxHeads int,
) (map[string]Set, []cid.Cid, []Pointer, error) {
	if len(heads) > maxHeads {
		return nil, nil, nil, fmt.Errorf("pointerhint: coordinator snapshot has %d heads, exceeds limit %d", len(heads), maxHeads)
	}
	candidate := make(map[string]Set, len(heads))
	for head, set := range heads {
		if err := validateCoordinatorHead(head); err != nil {
			return nil, nil, nil, err
		}
		if _, err := set.pointers(); err != nil {
			return nil, nil, nil, fmt.Errorf("pointerhint: head %q: %w", head, err)
		}
		candidate[head] = set
	}
	candidateDocuments, err := validateCoordinatorDocuments(documents)
	if err != nil {
		return nil, nil, nil, err
	}
	items, err := aggregateCoordinatorState(candidate, candidateDocuments)
	if err != nil {
		return nil, nil, nil, err
	}
	return candidate, candidateDocuments, items, nil
}

// UpdateHead atomically replaces one head's exact current pointer set. An
// empty Set is a valid withdrawn head and still occupies one configured head
// slot; use RemoveHead when the head itself no longer exists.
func (c *Coordinator) UpdateHead(head string, set Set) error {
	if err := validateCoordinatorHead(head); err != nil {
		return err
	}
	if _, err := set.pointers(); err != nil {
		return fmt.Errorf("pointerhint: head %q: %w", head, err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return errors.New("pointerhint: coordinator is closed")
	}
	if _, exists := c.heads[head]; !exists && len(c.heads) >= c.maxHeads {
		return fmt.Errorf("pointerhint: coordinator head limit %d reached", c.maxHeads)
	}
	candidate := cloneHeadSets(c.heads, 1)
	candidate[head] = set
	items, err := aggregateCoordinatorState(candidate, c.documents)
	if err != nil {
		return err
	}
	if err := c.provider.updatePointers(items); err != nil {
		return err
	}
	c.heads = candidate
	return nil
}

// RemoveHead atomically removes one head. It is idempotent; pointers shared by
// another head remain in the provider schedule without restarting their
// retry/reprovide cadence.
func (c *Coordinator) RemoveHead(head string) error {
	if err := validateCoordinatorHead(head); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return errors.New("pointerhint: coordinator is closed")
	}
	if _, exists := c.heads[head]; !exists {
		return nil
	}
	candidate := cloneHeadSets(c.heads, 0)
	delete(candidate, head)
	items, err := aggregateCoordinatorState(candidate, c.documents)
	if err != nil {
		// Every installed map was validated transactionally, so this is
		// unreachable unless in-process memory was corrupted.
		return fmt.Errorf("pointerhint: installed coordinator state is invalid: %w", err)
	}
	if err := c.provider.updatePointers(items); err != nil {
		return err
	}
	c.heads = candidate
	return nil
}

func cloneHeadSets(source map[string]Set, extra int) map[string]Set {
	result := make(map[string]Set, len(source)+extra)
	for head, set := range source {
		result[head] = set
	}
	return result
}

func aggregateCoordinatorState(heads map[string]Set, documents []cid.Cid) ([]Pointer, error) {
	// Sorting is not needed for correctness, but makes conflict errors and the
	// resulting first-attempt order stable across Go map iteration.
	names := make([]string, 0, len(heads))
	for head := range heads {
		names = append(names, head)
	}
	sort.Strings(names)
	items := make([]Pointer, 0, len(heads)*3+len(documents))
	for _, head := range names {
		pointers, err := heads[head].pointers()
		if err != nil {
			return nil, fmt.Errorf("pointerhint: head %q: %w", head, err)
		}
		items = append(items, pointers...)
	}
	for _, document := range documents {
		items = append(items, Pointer{Kind: Document, CID: document})
	}
	return normalizePointers(items)
}

func validateCoordinatorDocuments(documents []cid.Cid) ([]cid.Cid, error) {
	if len(documents) > MaxCoordinatorExtraDocuments {
		return nil, fmt.Errorf("pointerhint: coordinator has %d extra documents, exceeds limit %d", len(documents), MaxCoordinatorExtraDocuments)
	}
	candidate := append([]cid.Cid(nil), documents...)
	for _, document := range candidate {
		if err := (Pointer{Kind: Document, CID: document}).validate(); err != nil {
			return nil, err
		}
	}
	return candidate, nil
}

// normalizePointers validates, globally type-checks, deduplicates, and orders
// a bounded schedule. The full CID binary form is the identity: two heads may
// share it only when both assign the exact same semantic Kind.
func normalizePointers(items []Pointer) ([]Pointer, error) {
	byCID := make(map[string]Pointer, len(items))
	for _, item := range items {
		if err := item.validate(); err != nil {
			return nil, err
		}
		key := item.CID.KeyString()
		if previous, exists := byCID[key]; exists {
			if previous.Kind != item.Kind {
				return nil, fmt.Errorf("pointerhint: CID %s is both %s and %s across heads", item.CID, previous.Kind, item.Kind)
			}
			continue
		}
		byCID[key] = item
	}
	result := make([]Pointer, 0, len(byCID))
	for _, item := range byCID {
		result = append(result, item)
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Kind != result[j].Kind {
			return result[i].Kind < result[j].Kind
		}
		return result[i].CID.String() < result[j].CID.String()
	})
	return result, nil
}

func validateCoordinatorHead(head string) error {
	if head == "" {
		return errors.New("pointerhint: coordinator head name must not be empty")
	}
	if len(head) > MaxCoordinatorHeadNameBytes {
		return fmt.Errorf("pointerhint: coordinator head name is %d bytes, exceeds %d", len(head), MaxCoordinatorHeadNameBytes)
	}
	for i := 0; i < len(head); i++ {
		ch := head[i]
		if (ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9') || (i > 0 && ch == '-') {
			continue
		}
		return fmt.Errorf("pointerhint: coordinator head name %q does not match [a-z0-9][a-z0-9-]*", head)
	}
	return nil
}

// Close stops the one process-wide provider. It is idempotent.
func (c *Coordinator) Close() error {
	if c == nil {
		return nil
	}
	c.once.Do(func() {
		c.mu.Lock()
		c.closed = true
		c.mu.Unlock()
		c.closeErr = c.provider.Close()
	})
	return c.closeErr
}
