package pinning_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/ipfs/go-cid"

	"github.com/blobarchive/bloar/archive"
	"github.com/blobarchive/bloar/pinning"
)

// wantPin is one expected pin, with the block named rather than hashed.
type wantPin struct {
	purpose   string
	label     string
	recursive bool
}

// pinLabels renders a desired set as (purpose, label, recursive), where the
// label names the block the way the fixture does.
func (f *fixture) pinLabels(p pinning.Policy, h *archive.Head) []wantPin {
	f.t.Helper()
	e := f.enumerate(h)
	f.labelIndex(h, e)

	pins, err := pinning.Desired(p, e)
	if err != nil {
		f.t.Fatalf("Desired: %v", err)
	}
	out := make([]wantPin, 0, len(pins))
	for _, pin := range pins {
		out = append(out, wantPin{pin.Purpose, f.named(markKey(pin.CID)), pin.Recursive})
	}
	return out
}

// labelIndex names a head's index blocks so a failure reads as "window/sealed
// ord 3" rather than as two CIDs.
func (f *fixture) labelIndex(h *archive.Head, e *archive.Enumeration) {
	name := h.Params().Name
	f.label(e.Root, name+" root")
	for i, c := range e.DirPages {
		f.label(c, fmt.Sprintf("%s page %d", name, i))
	}
	for _, s := range e.Sealed {
		f.label(s.CID, fmt.Sprintf("%s sealed ord %d", name, s.Ord))
	}
	if e.Open.Defined() {
		f.label(e.Open, fmt.Sprintf("%s open ord %d", name, e.OpenOrd))
	}
}

func hasWant(pins []wantPin, w wantPin) bool {
	for _, p := range pins {
		if p == w {
			return true
		}
	}
	return false
}

func wantExactly(t *testing.T, got []wantPin, want []wantPin) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %d pins %v, want %d %v", len(got), got, len(want), want)
	}
	for _, w := range want {
		if !hasWant(got, w) {
			t.Fatalf("pin %v missing from %v", w, got)
		}
	}
}

// TestDesiredFull: one recursive pin, whatever the head holds.
func TestDesiredFull(t *testing.T) {
	f := newFixture(t)
	h := f.head("full", pinning.Full())
	f.apply(h, 20, f.row(8, 1), f.row(12, 2), f.row(20, 3))

	wantExactly(t, f.pinLabels(pinning.Full(), h), []wantPin{
		{pinning.PurposeRoot, "full root", true},
	})
}

// TestDesiredNone: every index block, direct, and nothing recursive -- a
// recursive pin anywhere here would retain a blob, which is the one thing this
// mode promises not to do.
func TestDesiredNone(t *testing.T) {
	f := newFixture(t)
	h := f.head("none", pinning.None())
	f.apply(h, 20, f.row(8, 1), f.row(12, 2), f.row(20, 3))

	wantExactly(t, f.pinLabels(pinning.None(), h), []wantPin{
		{pinning.PurposeRoot, "none root", false},
		{pinning.PurposeIndex, "none page 0", false},
		{pinning.PurposeIndex, "none sealed ord 2", false},
		{pinning.PurposeIndex, "none sealed ord 3", false},
		{pinning.PurposeOpen, "none open ord 5", false},
	})
}

// TestDesiredWindowBoundary is the arithmetic edge: a duration that reaches
// exactly one window's last slot. The window is [synced_to - dur/12, synced_to]
// and intersection is inclusive, so that window is in and the one before it is
// out.
func TestDesiredWindowBoundary(t *testing.T) {
	f := newFixture(t)
	h := f.head("win", pinning.Window(slotsDur(8), testSecondsPerSlot))
	// Windows 2 (8..11), 3 (12..15), 4 (16..19) seal; 5 (20..23) is open.
	f.apply(h, 20, f.row(8, 1), f.row(12, 2), f.row(16, 3), f.row(20, 4))

	// 8 slots back from 20 is slot 12: window 3 ends at 15 and so intersects;
	// window 2 ends at 11, one slot short, and does not.
	wantExactly(t, f.pinLabels(pinning.Window(slotsDur(8), testSecondsPerSlot), h), []wantPin{
		{pinning.PurposeRoot, "win root", false},
		{pinning.PurposeIndex, "win page 0", false},
		{pinning.PurposeIndex, "win sealed ord 2", false},
		{pinning.PurposeWindow, "win sealed ord 3", true},
		{pinning.PurposeWindow, "win sealed ord 4", true},
		{pinning.PurposeOpen, "win open ord 5", true},
	})

	// One slot more of retention reaches slot 11, which is window 2's last, and
	// pulls the whole window in: a window is retained if any of it is.
	wantExactly(t, f.pinLabels(pinning.Window(slotsDur(9), testSecondsPerSlot), h), []wantPin{
		{pinning.PurposeRoot, "win root", false},
		{pinning.PurposeIndex, "win page 0", false},
		{pinning.PurposeWindow, "win sealed ord 2", true},
		{pinning.PurposeWindow, "win sealed ord 3", true},
		{pinning.PurposeWindow, "win sealed ord 4", true},
		{pinning.PurposeOpen, "win open ord 5", true},
	})
}

// TestDesiredWindowLongerThanHistory: a window nothing falls out of. Every
// sealed segment is recursive and no segment is pinned as bare index.
func TestDesiredWindowLongerThanHistory(t *testing.T) {
	f := newFixture(t)
	h := f.head("win", pinning.Window(slotsDur(1_000_000), testSecondsPerSlot))
	f.apply(h, 20, f.row(8, 1), f.row(12, 2), f.row(20, 3))

	for _, p := range f.pinLabels(pinning.Window(slotsDur(1_000_000), testSecondsPerSlot), h) {
		if p.purpose == pinning.PurposeIndex && p.label != "win page 0" {
			t.Errorf("pin %v is a bare index pin; a window longer than the head's history retains every segment", p)
		}
	}
}

// TestDesiredWindowShorterThanOneWindow: the open segment is always retained,
// however short the duration -- it is the most recent window by definition.
func TestDesiredWindowShorterThanOneWindow(t *testing.T) {
	f := newFixture(t)
	h := f.head("win", pinning.Window(time.Second, testSecondsPerSlot))
	f.apply(h, 20, f.row(8, 1), f.row(20, 2))

	wantExactly(t, f.pinLabels(pinning.Window(time.Second, testSecondsPerSlot), h), []wantPin{
		{pinning.PurposeRoot, "win root", false},
		{pinning.PurposeIndex, "win page 0", false},
		{pinning.PurposeIndex, "win sealed ord 2", false},
		{pinning.PurposeOpen, "win open ord 5", true},
	})
}

// TestDesiredEmptyHead: nothing to pin but the root, under every mode.
func TestDesiredEmptyHead(t *testing.T) {
	f := newFixture(t)
	h := f.head("empty", pinning.Full())

	for _, tc := range []struct {
		policy    pinning.Policy
		recursive bool
	}{
		{pinning.Full(), true},
		{pinning.Window(slotsDur(8), testSecondsPerSlot), false},
		{pinning.None(), false},
	} {
		wantExactly(t, f.pinLabels(tc.policy, h), []wantPin{
			{pinning.PurposeRoot, "empty root", tc.recursive},
		})
	}
}

// TestWindowSlots is spec 9's dur/SECONDS_PER_SLOT.
func TestWindowSlots(t *testing.T) {
	for _, tc := range []struct {
		dur            time.Duration
		secondsPerSlot uint64
		want           uint64
	}{
		{720 * time.Hour, 12, 216_000}, // the spec 12 example
		{12 * time.Second, 12, 1},
		{11 * time.Second, 12, 0},  // less than a slot retains no sealed window
		{100 * time.Second, 12, 8}, // truncates: 8 whole slots
		{12 * time.Second, 6, 2},   // a network with a different slot time
	} {
		if got := pinning.Window(tc.dur, tc.secondsPerSlot).WindowSlots(); got != tc.want {
			t.Errorf("Window(%s, %d).WindowSlots() = %d, want %d", tc.dur, tc.secondsPerSlot, got, tc.want)
		}
	}
}

func TestPolicyValidate(t *testing.T) {
	for _, tc := range []struct {
		name   string
		policy pinning.Policy
		ok     bool
	}{
		{"full", pinning.Full(), true},
		{"none", pinning.None(), true},
		{"window", pinning.Window(time.Hour, 12), true},
		{"window without a duration", pinning.Window(0, 12), false},
		{"window without seconds_per_slot", pinning.Window(time.Hour, 0), false},
		{"unknown mode", pinning.Policy{Mode: pinning.Mode(99)}, false},
	} {
		if err := tc.policy.Validate(); (err == nil) != tc.ok {
			t.Errorf("%s: Validate() = %v, want ok=%t", tc.name, err, tc.ok)
		}
	}
}

func TestParseMode(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want pinning.Mode
		ok   bool
	}{
		{"full", pinning.ModeFull, true},
		{"window", pinning.ModeWindow, true},
		{"none", pinning.ModeNone, true},
		{"", 0, false},
		{"Full", 0, false},
	} {
		got, err := pinning.ParseMode(tc.in)
		if (err == nil) != tc.ok {
			t.Errorf("ParseMode(%q) = %v, want ok=%t", tc.in, err, tc.ok)
		}
		if err == nil && got != tc.want {
			t.Errorf("ParseMode(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}
}

// TestDesiredRejectsUnusable: a policy that cannot be evaluated is an error, not
// an empty pin set. An empty set would unpin the head.
func TestDesiredRejectsUnusable(t *testing.T) {
	f := newFixture(t)
	h := f.head("win", pinning.Full())
	e := f.enumerate(h)

	if _, err := pinning.Desired(pinning.Policy{Mode: pinning.ModeWindow}, e); err == nil {
		t.Error("Desired with an unusable window policy: want an error, got nil")
	}
	if _, err := pinning.Desired(pinning.Full(), &archive.Enumeration{Root: cid.Undef}); err == nil {
		t.Error("Desired over an enumeration with no root: want an error, got nil")
	}
}
