package archive_test

import (
	"testing"

	"github.com/ipfs/go-cid"

	"github.com/blobarchive/bloar/archive"
)

// enumerate is Enumerate with the error handled.
func (hs *harness) enumerate() *archive.Enumeration {
	hs.t.Helper()
	e, err := hs.h.Enumerate(hs.ctx)
	if err != nil {
		hs.t.Fatalf("Enumerate: %v", err)
	}
	return e
}

// enumOrds renders the enumerated sealed segments as their ordinals.
func enumOrds(e *archive.Enumeration) []uint64 {
	out := make([]uint64, 0, len(e.Sealed))
	for _, s := range e.Sealed {
		out = append(out, s.Ord)
	}
	return out
}

func equalUint64(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestEnumerateEmptyHead(t *testing.T) {
	hs := newHarness(t, testParams())
	e := hs.enumerate()

	if e.Root != hs.h.Root() {
		t.Errorf("root = %s, want %s", e.Root, hs.h.Root())
	}
	if e.Covered {
		t.Error("covered = true on an empty head")
	}
	if len(e.DirPages) != 0 || len(e.Sealed) != 0 || e.Open.Defined() {
		t.Errorf("empty head enumerated %d pages, %d sealed, open defined=%t; want nothing",
			len(e.DirPages), len(e.Sealed), e.Open.Defined())
	}
}

// TestEnumerateOrdinals is the arithmetic a window policy depends on: an
// entry's ordinal is its position in the directory plus dir_base, and its slot
// bounds follow from seg_bits.
func TestEnumerateOrdinals(t *testing.T) {
	hs := newHarness(t, testParams())

	// One blob per window over windows 5..9 (testOrigin 40, testSegBits 3, so
	// dir_base is 5), then coverage through the end of window 9 so that all
	// five seal and window 10 is open.
	for w := uint64(5); w <= 9; w++ {
		hs.apply([]archive.RefRow{hs.row(w<<testSegBits, w)}, (w+1)<<testSegBits-1)
	}

	e := hs.enumerate()
	if got, want := enumOrds(e), []uint64{5, 6, 7, 8, 9}; !equalUint64(got, want) {
		t.Fatalf("sealed ordinals = %v, want %v", got, want)
	}
	for _, s := range e.Sealed {
		wantFirst, wantLast := s.Ord<<testSegBits, ((s.Ord+1)<<testSegBits)-1
		if s.FirstSlot != wantFirst || s.LastSlot != wantLast {
			t.Errorf("ord %d bounds = [%d, %d], want [%d, %d]", s.Ord, s.FirstSlot, s.LastSlot, wantFirst, wantLast)
		}
	}
	if e.OpenOrd != 10 {
		t.Errorf("open ord = %d, want 10", e.OpenOrd)
	}
	if want := uint64(10<<testSegBits) - 1; !e.Covered || e.SyncedTo != want {
		t.Errorf("covered=%t synced_to=%d, want true, %d", e.Covered, e.SyncedTo, want)
	}
}

// TestEnumerateSkipsNulls is the case a policy must never see: a window with no
// blobs seals to a null entry (spec 5.2), which is not a block and cannot be
// pinned.
func TestEnumerateSkipsNulls(t *testing.T) {
	hs := newHarness(t, testParams())

	// Windows 5 and 7 carry a blob; 6 and 8 are covered but empty.
	hs.apply([]archive.RefRow{hs.row(5<<testSegBits, 1)}, (6<<testSegBits)-1)
	hs.apply(nil, (7<<testSegBits)-1)
	hs.apply([]archive.RefRow{hs.row(7<<testSegBits, 2)}, (9<<testSegBits)-1)

	e := hs.enumerate()
	if got, want := enumOrds(e), []uint64{5, 7}; !equalUint64(got, want) {
		t.Fatalf("sealed ordinals = %v, want %v (an empty window seals to null, not to a block)", got, want)
	}
	for _, s := range e.Sealed {
		if !s.CID.Defined() {
			t.Errorf("ord %d enumerated an undefined CID", s.Ord)
		}
	}
}

// TestEnumerateDeepDirectory walks a directory more than one level deep, which
// is where reconstructing the index from path digits could go wrong.
func TestEnumerateDeepDirectory(t *testing.T) {
	hs := newHarness(t, testParams())

	// Fanout 4: capacity is 4 sealed segments at depth 1, 16 at depth 2, so 20
	// windows reach depth 3.
	const windows = 20
	for w := uint64(5); w < 5+windows; w++ {
		hs.apply([]archive.RefRow{hs.row(w<<testSegBits, w)}, (w+1)<<testSegBits-1)
	}

	e := hs.enumerate()
	if got := len(e.Sealed); got != windows {
		t.Fatalf("enumerated %d sealed segments, want %d", got, windows)
	}
	for i, s := range e.Sealed {
		if want := uint64(5 + i); s.Ord != want {
			t.Errorf("sealed[%d].Ord = %d, want %d (sealed segments must come out ascending)", i, s.Ord, want)
		}
	}
	if len(e.DirPages) < 3 {
		t.Errorf("enumerated %d dir pages; a %d-entry directory at fanout %d is more than that",
			len(e.DirPages), windows, 1<<testFanoutBits)
	}

	// Nothing twice: a walk that revisited a page would pin it twice and report
	// a directory bigger than it is.
	seen := map[cid.Cid]bool{}
	for _, c := range append(append([]cid.Cid{e.Root, e.Open}, e.DirPages...), ordCIDs(e)...) {
		if seen[c] {
			t.Errorf("block %s enumerated twice", c)
		}
		seen[c] = true
	}
}

func ordCIDs(e *archive.Enumeration) []cid.Cid {
	out := make([]cid.Cid, 0, len(e.Sealed))
	for _, s := range e.Sealed {
		out = append(out, s.CID)
	}
	return out
}
