package archive

import (
	"strings"
	"testing"

	"github.com/ipfs/boxo/blockstore"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"

	"github.com/blobarchive/bloar/schema"
)

func internalSecurityCID(t *testing.T) cid.Cid {
	t.Helper()
	data, id, err := schema.EncodeDirNode(&schema.DirNode{})
	if err != nil {
		t.Fatalf("schema.EncodeDirNode: %v", err)
	}
	_ = data
	return id
}

func TestEnumerationWalkerRejectsActiveCycle(t *testing.T) {
	id := internalSecurityCID(t)
	page := &schema.DirNode{Kids: []cid.Cid{id, id}}
	w := enumerationWalker{
		st:          &state{params: Params{OriginSlot: 0, SegBits: 0, FanoutBits: 2}},
		out:         &Enumeration{},
		budget:      &enumerationBudget{unique: make(map[string]struct{})},
		dirNodes:    map[string]*schema.DirNode{id.KeyString(): page},
		dirActive:   make(map[string]bool),
		dirMemo:     make(map[dirMemoKey]dirSummary),
		dirContains: make(map[string]bool),
		segments:    make(map[string]uint64),
		newProofs:   make(map[string]cachedSegmentProof),
	}

	_, err := w.walkDir(t.Context(), id, 2, 0, 5)
	if err == nil || !strings.Contains(err.Error(), "is cyclic") {
		t.Fatalf("walkDir(cycle) error = %v, want explicit cyclic-subgraph refusal", err)
	}
}

func TestEnumerationBudgetRejectsAggregateDecodedBytes(t *testing.T) {
	bs := blockstore.NewBlockstore(dssync.MutexWrap(datastore.NewMapDatastore()))
	data, id, err := schema.EncodeDirNode(&schema.DirNode{})
	if err != nil {
		t.Fatalf("schema.EncodeDirNode: %v", err)
	}
	blk, err := blocks.NewBlockWithCid(data, id)
	if err != nil {
		t.Fatalf("blocks.NewBlockWithCid: %v", err)
	}
	if err := bs.Put(t.Context(), blk); err != nil {
		t.Fatalf("Blockstore.Put: %v", err)
	}

	budget := &enumerationBudget{
		unique:      make(map[string]struct{}),
		decodedByte: MaxEnumerationDecodedBytes - uint64(len(data)) + 1,
	}
	head := &Head{cfg: Config{Blocks: bs}}
	if _, err := budget.readNode(t.Context(), head, id, "DirNode"); err == nil ||
		!strings.Contains(err.Error(), "byte enumeration budget") {
		t.Fatalf("readNode(over aggregate budget) error = %v, want decoded-byte refusal", err)
	}
}

func TestEnumerationBudgetRejectsLogicalPathAmplification(t *testing.T) {
	budget := &enumerationBudget{}
	if err := budget.addPaths(MaxEnumerationPaths + 1); err == nil ||
		!strings.Contains(err.Error(), "logical-path budget") {
		t.Fatalf("addPaths(over budget) error = %v, want path-budget refusal", err)
	}
}
