package core_test

import (
	"context"
	"testing"

	"github.com/blobarchive/bloar/core"
	"github.com/blobarchive/bloar/schema"
)

// TestCacheHitsSkipTheBlockstore: a hit costs no Get at all.
func TestCacheHitsSkipTheBlockstore(t *testing.T) {
	ctx := context.Background()
	cache, err := core.NewNodeCache(1 << 20)
	if err != nil {
		t.Fatalf("NewNodeCache: %v", err)
	}
	tr := buildTree(t, cache)
	cache.Purge()
	tr.bs.reset()

	for i := 1; i <= 3; i++ {
		if _, err := tr.dirStore.GetNode(ctx, tr.root); err != nil {
			t.Fatalf("GetNode %d: %v", i, err)
		}
		want := 1 // only the first read misses
		if got := tr.bs.getCount(tr.root); got != want {
			t.Errorf("after %d GetNodes: got %d blockstore Gets, want %d", i, got, want)
		}
	}

	// A different CID is a miss and does reach the blockstore.
	if _, err := tr.dirStore.GetNode(ctx, tr.dirB); err != nil {
		t.Fatalf("GetNode dirB: %v", err)
	}
	if got := tr.bs.getCount(tr.dirB); got != 1 {
		t.Errorf("cold dirB: got %d Gets, want 1", got)
	}
	if got := tr.dirStore.Blocks(); got != tr.bs {
		t.Errorf("Blocks() did not return the underlying blockstore")
	}
}

// TestCacheWithoutCacheAlwaysReads: a nil cache is valid and caches nothing.
func TestCacheWithoutCacheAlwaysReads(t *testing.T) {
	ctx := context.Background()
	tr := buildTree(t, nil)
	tr.bs.reset()

	for i := 1; i <= 3; i++ {
		if _, err := tr.dirStore.GetNode(ctx, tr.root); err != nil {
			t.Fatalf("GetNode %d: %v", i, err)
		}
		if got := tr.bs.getCount(tr.root); got != i {
			t.Errorf("after %d GetNodes: got %d Gets, want %d", i, got, i)
		}
	}
}

func TestNewNodeCacheBudget(t *testing.T) {
	tests := []struct {
		name    string
		budget  int64
		wantErr bool
	}{
		{name: "positive", budget: 1 << 20},
		{name: "one byte", budget: 1},
		{name: "zero", budget: 0, wantErr: true},
		{name: "negative", budget: -1, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, err := core.NewNodeCache(tt.budget)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("NewNodeCache(%d): want error, got nil", tt.budget)
				}
				return
			}
			if err != nil {
				t.Fatalf("NewNodeCache(%d): %v", tt.budget, err)
			}
			if c.Len() != 0 || c.Bytes() != 0 {
				t.Errorf("fresh cache: len=%d bytes=%d, want 0/0", c.Len(), c.Bytes())
			}
		})
	}
}

// TestCacheEviction: the cache stays inside its byte budget and evicts least
// recently used entries, and correctness survives the eviction.
func TestCacheEviction(t *testing.T) {
	ctx := context.Background()

	// Budget for roughly one node: every read of a second node evicts the
	// first. Sized from an actual encoded block so the approximation used for
	// entry cost (encoded length) is the one under test.
	// Sized from the largest node read below, in the same units the cache
	// accounts in (encoded block length), so each node fits alone and no two
	// fit together. Fixture segments differ by a byte or two: DAG-CBOR encodes
	// a small slot0 in fewer bytes than a large one.
	block := max(encodedSize(t, 0, "a0"), encodedSize(t, 512, "a1"), encodedSize(t, 1024, "b0"))
	cache, err := core.NewNodeCache(int64(block))
	if err != nil {
		t.Fatalf("NewNodeCache: %v", err)
	}
	tr := buildTree(t, cache)
	cache.Purge()

	nodes := []struct {
		name string
		read func() error
	}{
		{"segA0", func() error { _, err := tr.segStore.GetNode(ctx, tr.segA0); return err }},
		{"segA1", func() error { _, err := tr.segStore.GetNode(ctx, tr.segA1); return err }},
		{"segB0", func() error { _, err := tr.segStore.GetNode(ctx, tr.segB0); return err }},
	}
	for _, n := range nodes {
		if err := n.read(); err != nil {
			t.Fatalf("reading %s: %v", n.name, err)
		}
		if got := cache.Bytes(); got > int64(block) {
			t.Errorf("after %s: cache holds %d bytes, over the %d budget", n.name, got, block)
		}
	}
	if got := cache.Len(); got != 1 {
		t.Errorf("cache holds %d nodes, want 1 at this budget", got)
	}

	// The evicted node still reads correctly: the cache is read-through and
	// nothing depends on it.
	tr.bs.reset()
	seg, err := tr.segStore.GetNode(ctx, tr.segA0)
	if err != nil {
		t.Fatalf("re-reading evicted segA0: %v", err)
	}
	if seg.Slot0 != 0 {
		t.Errorf("evicted segA0 came back wrong: slot0=%d want 0", seg.Slot0)
	}
	if got := tr.bs.getCount(tr.segA0); got != 1 {
		t.Errorf("evicted segA0: got %d Gets, want 1 (it must be re-read)", got)
	}
}

// encodedSize returns the encoded block length of a fixture segment.
func encodedSize(t *testing.T, slot0 uint64, seed string) int {
	t.Helper()
	data, _, err := schema.EncodeSegment(segment(t, slot0, seed))
	if err != nil {
		t.Fatalf("EncodeSegment: %v", err)
	}
	return len(data)
}
