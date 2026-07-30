package follow

import (
	"testing"

	"github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
)

// epochBlockstore is the optional online-GC surface follow detects. The
// embedded Blockstore is deliberately nil: these tests exercise memo policy,
// not I/O.
type epochBlockstore struct {
	blockstore.Blockstore
	generation uint64
}

func (s *epochBlockstore) CollectionGeneration() uint64 { return s.generation }

// plainBlockstore intentionally erases the optional collection-generation
// capability while retaining the Blockstore method set through embedding.
type plainBlockstore struct{ blockstore.Blockstore }

func epochTestCID(t *testing.T, n byte) cid.Cid {
	t.Helper()
	mh, err := multihash.Sum([]byte{n}, multihash.SHA2_256, -1)
	if err != nil {
		t.Fatalf("multihash.Sum: %v", err)
	}
	return cid.NewCidV1(cid.DagCBOR, mh)
}

func TestWalkedMemoIsScopedToCollectionGeneration(t *testing.T) {
	local := &epochBlockstore{}
	f := &Follower{cfg: Config{Local: local}, walked: map[string]uint64{}}
	old, same, crossed := epochTestCID(t, 1), epochTestCID(t, 2), epochTestCID(t, 3)

	// Before the first collection, generation-zero proofs are reusable.
	f.walked[old.KeyString()] = 0
	if w := (&walk{f: f, generation: f.collectionGeneration()}); !w.done(old) {
		t.Fatal("a walk did not trust a completed subtree from its generation")
	}

	// Begin increments the generation. The new generation remains current even
	// after GC ends, so an old memo can never resurrect a subtree that the
	// completed sweep deleted (A -> B -> GC -> A).
	local.generation = 7
	w := &walk{f: f, generation: f.collectionGeneration()}
	if w.done(old) {
		t.Fatal("a post-GC walk trusted a subtree stamped before the collection")
	}
	w.pending = map[string]bool{same.KeyString(): true}
	w.commit()
	if stamp := f.walked[same.KeyString()]; stamp != 7 {
		t.Fatalf("a walk completed in generation 7 was stamped %d", stamp)
	}
	if !w.done(same) {
		t.Fatal("a walk did not trust a subtree completed in the same generation")
	}

	// A walk crossing a collection cut is not memoized at all: some of its reads
	// may predate the cut and prove neither post-cut presence nor protection.
	w.pending = map[string]bool{crossed.KeyString(): true}
	local.generation = 8
	w.commit()
	if _, ok := f.walked[crossed.KeyString()]; ok {
		t.Fatal("a walk crossing collection generations was memoized")
	}
	if (&walk{f: f, generation: 8}).done(crossed) {
		t.Fatal("the new generation trusted a walk completed across its boundary")
	}
}

func TestWalkedMemoIsDisabledWithoutCollectionGeneration(t *testing.T) {
	local := &plainBlockstore{}
	f := &Follower{cfg: Config{Local: local}, walked: map[string]uint64{}}
	old := epochTestCID(t, 9)
	f.walked[old.KeyString()] = 0

	// A legacy collector can complete under Gate without changing any token:
	// A was walked, the head moved A -> B, GC deleted A-only descendants, then
	// A was published again. Trusting a process-lifetime generation-zero memo
	// would skip the missing subtree. The compatibility path must rewalk it.
	w := &walk{f: f, generation: 0, pending: map[string]bool{}}
	if w.done(old) {
		t.Fatal("plain blockstore trusted a stale process-lifetime presence memo")
	}
	newCID := epochTestCID(t, 10)
	w.pending[newCID.KeyString()] = true
	w.commit()
	if _, ok := f.walked[newCID.KeyString()]; ok {
		t.Fatal("plain blockstore published a presence memo with no invalidation generation")
	}
}
