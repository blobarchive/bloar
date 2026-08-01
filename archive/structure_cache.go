package archive

import (
	"sync"

	"github.com/ipfs/go-cid"
)

// StructureCache retains compact, content-addressed Segment shape proofs across
// successive roots. A finalized root changes often while almost every sealed
// Segment stays byte-identical; remembering only its immutable slot bounds keeps
// strict pre-admission validation incremental without retaining decoded Segment
// bodies.
//
// The cache is an optimization, never authority. Every entry was produced by a
// successful bounded decode of the exact CID. With a monotonic collection
// generation, its presence proof is reusable only in that generation; without
// one, every admission re-reads and re-hashes the block. Clearing the cache
// costs a decode and changes no acceptance decision.
type StructureCache struct {
	mu       sync.RWMutex
	segments map[string]cachedSegmentProof
}

type cachedSegmentProof struct {
	proof      segmentProof
	generation uint64
	// generationKnown distinguishes an observed generation zero from a store
	// with no invalidation signal.
	generationKnown bool
}

const maxStructureCacheSegments = MaxEnumerationOutputs + 1 // sealed outputs plus the open Segment

// NewStructureCache returns an empty cache suitable for sharing by all Heads
// loaded by one writer or follower process.
func NewStructureCache() *StructureCache {
	return &StructureCache{segments: make(map[string]cachedSegmentProof)}
}

func (c *StructureCache) segment(id cid.Cid) (cachedSegmentProof, bool) {
	if c == nil {
		return cachedSegmentProof{}, false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	proof, ok := c.segments[id.KeyString()]
	return proof, ok
}

func (c *StructureCache) rememberSegments(proofs map[string]cachedSegmentProof) {
	if c == nil || len(proofs) == 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.segments)+len(proofs) > maxStructureCacheSegments {
		// A signer can make every generation name fresh Segment CIDs. Bound the
		// optimization itself rather than letting that churn become retained
		// process memory. A wholesale reset is safe because proofs are immutable
		// performance hints, not accepted state.
		clear(c.segments)
	}
	for key, proof := range proofs {
		c.segments[key] = proof
	}
}
