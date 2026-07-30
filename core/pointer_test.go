package core_test

import (
	"context"
	"errors"
	"testing"

	"github.com/ipfs/go-cid"

	"github.com/blobarchive/bloar/core"
	"github.com/blobarchive/bloar/schema"
)

func TestPointerStates(t *testing.T) {
	ctx := context.Background()
	tr := buildTree(t, nil)

	tests := []struct {
		name    string
		ptr     func() *core.Pointer[schema.DirNode]
		dirty   bool
		loaded  bool
		wantCID cid.Cid
		wantErr error
	}{
		{
			name:    "from CID is lazy and clean",
			ptr:     func() *core.Pointer[schema.DirNode] { return tr.dirStore.Pointer(tr.root) },
			dirty:   false,
			loaded:  false,
			wantCID: tr.root,
		},
		{
			name: "loaded keeps its CID",
			ptr: func() *core.Pointer[schema.DirNode] {
				p := tr.dirStore.Pointer(tr.root)
				if _, err := p.Load(ctx); err != nil {
					t.Fatalf("Load: %v", err)
				}
				return p
			},
			dirty:   false,
			loaded:  true,
			wantCID: tr.root,
		},
		{
			name: "new node is dirty and has no CID",
			ptr: func() *core.Pointer[schema.DirNode] {
				return tr.dirStore.NewNode(&schema.DirNode{Kids: []cid.Cid{tr.segA0}})
			},
			dirty:   true,
			loaded:  true,
			wantErr: core.ErrDirty,
		},
		{
			name: "mutated node is dirty and has no CID",
			ptr: func() *core.Pointer[schema.DirNode] {
				p := tr.dirStore.Pointer(tr.dirA)
				if _, err := p.Mutate(ctx); err != nil {
					t.Fatalf("Mutate: %v", err)
				}
				return p
			},
			dirty:   true,
			loaded:  true,
			wantErr: core.ErrDirty,
		},
		{
			name: "Set marks dirty without reading",
			ptr: func() *core.Pointer[schema.DirNode] {
				p := tr.dirStore.Pointer(tr.dirA)
				p.Set(&schema.DirNode{Kids: []cid.Cid{tr.segB0}})
				return p
			},
			dirty:   true,
			loaded:  true,
			wantErr: core.ErrDirty,
		},
		{
			name:    "undefined CID references nothing",
			ptr:     func() *core.Pointer[schema.DirNode] { return tr.dirStore.Pointer(cid.Undef) },
			dirty:   false,
			loaded:  false,
			wantErr: core.ErrEmpty,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := tt.ptr()
			if got := p.IsDirty(); got != tt.dirty {
				t.Errorf("IsDirty: got %t want %t", got, tt.dirty)
			}
			if got := p.IsLoaded(); got != tt.loaded {
				t.Errorf("IsLoaded: got %t want %t", got, tt.loaded)
			}
			c, err := p.CID()
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Errorf("CID error: got %v want %v", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("CID: %v", err)
			}
			if c != tt.wantCID {
				t.Errorf("CID: got %s want %s", c, tt.wantCID)
			}
		})
	}
}

// TestPointerCommitRoundtrip: a dirty pointer commits to a readable block and
// goes clean; committing a clean pointer is a no-op that rewrites nothing.
func TestPointerCommitRoundtrip(t *testing.T) {
	ctx := context.Background()
	tr := buildTree(t, nil)

	p := tr.dirStore.NewNode(&schema.DirNode{Kids: []cid.Cid{tr.segA0, tr.segB1}})
	c, err := p.Commit(ctx)
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if p.IsDirty() {
		t.Error("pointer still dirty after Commit")
	}
	got, err := p.CID()
	if err != nil {
		t.Fatalf("CID after Commit: %v", err)
	}
	if got != c {
		t.Errorf("CID after Commit: got %s want %s", got, c)
	}

	node, err := tr.dirStore.GetNode(ctx, c)
	if err != nil {
		t.Fatalf("reading back committed node: %v", err)
	}
	if len(node.Kids) != 2 || node.Kids[0] != tr.segA0 || node.Kids[1] != tr.segB1 {
		t.Errorf("committed node round-tripped as %v", node.Kids)
	}

	before := blockCount(t, tr.bs)
	c2, err := p.Commit(ctx)
	if err != nil {
		t.Fatalf("second Commit: %v", err)
	}
	if c2 != c {
		t.Errorf("clean re-Commit changed the CID: got %s want %s", c2, c)
	}
	if after := blockCount(t, tr.bs); after != before {
		t.Errorf("clean re-Commit wrote blocks: %d -> %d", before, after)
	}
}

// TestCommitRejectsDirtyChild pins the invariant that forces bottom-up
// commits: a parent cannot take a dirty child's CID, so the parent errors
// instead of writing a null or stale link.
func TestCommitRejectsDirtyChild(t *testing.T) {
	ctx := context.Background()
	tr := buildTree(t, nil)

	child := tr.segStore.NewNode(segment(t, 2048, "late"))
	if _, err := child.CID(); !errors.Is(err, core.ErrDirty) {
		t.Fatalf("dirty child CID: got %v want ErrDirty", err)
	}

	childCID, err := child.Commit(ctx)
	if err != nil {
		t.Fatalf("committing child: %v", err)
	}
	parent := tr.dirStore.NewNode(&schema.DirNode{Kids: []cid.Cid{childCID}})
	if _, err := parent.Commit(ctx); err != nil {
		t.Fatalf("committing parent after child: %v", err)
	}
}

// TestMutateDoesNotAliasCache is the correctness claim behind Mutate: a node
// handed out for writing is private, so mutating it cannot corrupt the cached
// copy other readers see.
func TestMutateDoesNotAliasCache(t *testing.T) {
	ctx := context.Background()
	cache, err := core.NewNodeCache(1 << 20)
	if err != nil {
		t.Fatalf("NewNodeCache: %v", err)
	}
	tr := buildTree(t, cache)

	cached, err := tr.dirStore.GetNode(ctx, tr.dirA)
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	original := append([]cid.Cid(nil), cached.Kids...)

	p := tr.dirStore.Pointer(tr.dirA)
	mutable, err := p.Mutate(ctx)
	if err != nil {
		t.Fatalf("Mutate: %v", err)
	}
	if mutable == cached {
		t.Fatal("Mutate handed out the cached node itself")
	}
	mutable.Kids[0] = tr.segB1

	again, err := tr.dirStore.GetNode(ctx, tr.dirA)
	if err != nil {
		t.Fatalf("GetNode after Mutate: %v", err)
	}
	for i, want := range original {
		if again.Kids[i] != want {
			t.Errorf("cached node kid %d was corrupted by Mutate: got %s want %s", i, again.Kids[i], want)
		}
	}
	if cached.Kids[0] != original[0] {
		t.Errorf("previously returned node was mutated underneath its reader")
	}
}
