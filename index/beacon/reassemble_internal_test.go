package beacon

// Internal (white-box) regression for the follow-up blocker's reassembly half: the
// bootstrap exception belongs to literal genesis slot 0, not to whatever slot a
// batch happens to start at. reassembleAnchored is unexported, so this drives it
// directly rather than through the end-to-end fakes.

import (
	"log/slog"
	"testing"
)

// TestReassembleBootstrapsOnlyAtSlotZero isolates blocker half (b) (audit
// the safety boundary follow-up). An unanchored batch (haveIn false) may accept its first present
// slot without a parent check ONLY when that slot is the literal genesis slot 0.
// A present first slot at any higher start -- a stale-genesis batch that resumed
// past 0 because a duplicate writer advanced the shared archive, or a leading-miss
// batch -- must wait (nil last), never bootstrap on a parent nothing proves.
//
// Reverting the guard to the follow-up form `slot != start` makes the start-2 case
// bootstrap (last != nil), which fails this test; the seed-side fix (a) does not
// touch reassembleAnchored, so this half is exercised in isolation.
func TestReassembleBootstrapsOnlyAtSlotZero(t *testing.T) {
	ix := &Indexer{log: slog.New(slog.DiscardHandler)}

	root2, wrongParent := [32]byte{2}, [32]byte{0x99}

	// A start-2 batch, unanchored, whose first slot is present with an unverifiable
	// parent. It must not bootstrap: nothing anchors slot 2.
	start2 := []anchoredResult{{done: true, res: anchoredSlot{present: true, root: root2, parentRoot: wrongParent}}}
	_, _, last, _, _, err := ix.reassembleAnchored(2, 2, start2, [32]byte{}, false)
	if err != nil {
		t.Fatalf("reassembleAnchored(start=2, unanchored): unexpected error %v", err)
	}
	if last != nil {
		t.Fatalf("an unanchored start-2 batch bootstrapped at slot %d; only literal slot 0 may bootstrap", *last)
	}

	// The control: a genuine genesis batch (start 0) whose slot 0 is present DOES
	// bootstrap, so the exception still works where it is legitimate.
	start0 := []anchoredResult{{done: true, res: anchoredSlot{present: true, root: [32]byte{0xa0}, parentRoot: [32]byte{}}}}
	_, _, last0, _, _, err := ix.reassembleAnchored(0, 0, start0, [32]byte{}, false)
	if err != nil {
		t.Fatalf("reassembleAnchored(start=0, unanchored): unexpected error %v", err)
	}
	if last0 == nil || *last0 != 0 {
		t.Fatalf("a present slot 0 did not bootstrap the genesis batch (last=%v)", last0)
	}
}
