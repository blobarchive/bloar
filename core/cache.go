package core

import (
	"fmt"
	"math"
	"sync"

	lru "github.com/hashicorp/golang-lru/v2/simplelru"
	"github.com/ipfs/go-cid"
)

// NodeCache is an LRU of decoded nodes keyed by CID (spec 6.3). A hit skips
// both the disk read and the decode.
//
// It is purely a performance feature: it is read-through, it never holds a
// dirty node, and nothing in core is correct only because of it. Dropping
// every entry at any moment costs time, not integrity.
//
// One cache is shared by the NodeStores of every node type, matching the
// single node_cache_mb budget. Entries are keyed by CID, which commits to the
// block bytes, so two types cannot collide on a key.
//
// A nil *NodeCache is a valid empty cache: it caches nothing.
type NodeCache struct {
	mu     sync.Mutex
	lru    *lru.LRU[cid.Cid, cacheEntry]
	budget int64
	bytes  int64
}

type cacheEntry struct {
	val  any
	cost int64
}

// NewNodeCache returns a cache holding up to budgetBytes of decoded nodes.
func NewNodeCache(budgetBytes int64) (*NodeCache, error) {
	if budgetBytes <= 0 {
		return nil, fmt.Errorf("core: node cache budget must be positive, got %d", budgetBytes)
	}
	// Eviction here is by bytes, not by count, so the count bound is disabled
	// and evict is driven from add. simplelru does not preallocate for size.
	l, err := lru.NewLRU[cid.Cid, cacheEntry](math.MaxInt, nil)
	if err != nil {
		return nil, fmt.Errorf("core: creating node cache: %w", err)
	}
	return &NodeCache{lru: l, budget: budgetBytes}, nil
}

// NewNodeCacheMB returns a cache sized in MiB, as store.node_cache_mb gives it.
func NewNodeCacheMB(mb int) (*NodeCache, error) {
	return NewNodeCache(int64(mb) << 20)
}

func (c *NodeCache) get(k cid.Cid) (any, bool) {
	if c == nil {
		return nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.lru.Get(k)
	if !ok {
		return nil, false
	}
	return e.val, true
}

// add inserts a decoded node whose encoded block was encodedLen bytes.
//
// The entry's cost is approximated by that encoded length. The decoded node is
// the thing actually held, and it is bigger -- Go headers, pointers, per-field
// padding, and for Segment a slice per row -- but its true footprint is not
// something Go will tell us without walking the object. Encoded length is
// within a small constant factor, is free to obtain, and is monotone in the
// only thing that varies much (how many refs a node carries). The budget is
// therefore a floor on the memory held, not a ceiling: treat node_cache_mb as
// a knob whose absolute units are approximate.
func (c *NodeCache) add(k cid.Cid, v any, encodedLen int) {
	if c == nil {
		return
	}
	cost := int64(encodedLen)
	c.mu.Lock()
	defer c.mu.Unlock()
	if old, ok := c.lru.Peek(k); ok {
		c.bytes -= old.cost
	}
	c.lru.Add(k, cacheEntry{val: v, cost: cost})
	c.bytes += cost
	// A single node over budget is stored and then immediately evicted, which
	// is correct (a cache that refuses is still a cache that misses) and only
	// reachable with an absurdly small budget.
	for c.bytes > c.budget {
		_, e, ok := c.lru.RemoveOldest()
		if !ok {
			break
		}
		c.bytes -= e.cost
	}
}

// Len returns the number of cached nodes.
func (c *NodeCache) Len() int {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lru.Len()
}

// Bytes returns the approximate size of the cached nodes; see add.
func (c *NodeCache) Bytes() int64 {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.bytes
}

// Purge drops every entry.
func (c *NodeCache) Purge() {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lru.Purge()
	c.bytes = 0
}
