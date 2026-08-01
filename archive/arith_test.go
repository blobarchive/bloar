package archive

import (
	"slices"
	"testing"
)

// The ALL head's real parameters (data-structures.md).
const (
	allSegBits    = 9
	allFanoutBits = 8
	allOrigin     = 8626176
)

// TestWorkedExample is the route of data-structures.md section 4, digit for
// digit: slot 12,345,678 -> ord 24112 -> idx 7264 -> path [28, 96].
func TestWorkedExample(t *testing.T) {
	const slot = 12345678

	gotOrd := ord(slot, allSegBits)
	if want := uint64(24112); gotOrd != want {
		t.Errorf("ord(%d) = %d, want %d", slot, gotOrd, want)
	}

	base := ord(allOrigin, allSegBits)
	if want := uint64(16848); base != want {
		t.Errorf("dir_base = ord(%d) = %d, want %d", allOrigin, base, want)
	}

	idx := gotOrd - base
	if want := uint64(7264); idx != want {
		t.Errorf("idx(%d) = %d, want %d", slot, idx, want)
	}

	digits, ok := pathDigits(idx, 2, allFanoutBits)
	if !ok {
		t.Fatalf("pathDigits(%d, 2, %d) reported the index does not fit", idx, allFanoutBits)
	}
	if want := []uint64{28, 96}; !slices.Equal(digits, want) {
		t.Errorf("path = %v, want %v", digits, want)
	}
}

// TestWorkedExampleWindow: data-structures.md section 3 puts segment 24112 over
// slots 12,345,344 .. 12,345,855.
func TestWorkedExampleWindow(t *testing.T) {
	const w = 24112
	if got, want := windowStart(w, allSegBits), uint64(12345344); got != want {
		t.Errorf("windowStart(%d) = %d, want %d", w, got, want)
	}
	if got, want := windowEnd(w, allSegBits), uint64(12345855); got != want {
		t.Errorf("windowEnd(%d) = %d, want %d", w, got, want)
	}
	if got := ord(12345678, allSegBits); got != w {
		t.Errorf("slot 12345678 is in window %d, want %d", got, w)
	}
}

// TestArbitrumOneDirBase: the same origin at seg_bits 13 gives dir_base 1053
// (data-structures.md preamble), and slot 12,345,347 lands in ord 1507
// (section 6).
func TestArbitrumOneDirBase(t *testing.T) {
	if got, want := ord(allOrigin, 13), uint64(1053); got != want {
		t.Errorf("dir_base at seg_bits 13 = %d, want %d", got, want)
	}
	if got, want := ord(12345347, 13), uint64(1507); got != want {
		t.Errorf("ord(12345347) at seg_bits 13 = %d, want %d", got, want)
	}
}

func TestCapacity(t *testing.T) {
	tests := []struct {
		depth, fanoutBits, want uint64
	}{
		{0, 8, 1},
		{1, 8, 256},
		{2, 8, 65536},
		{3, 8, 16777216},
		{1, 2, 4},
		{2, 2, 16},
		{3, 2, 64},
		// Past uint64 the true capacity addresses every ordinal that can
		// exist, which is what saturation reports.
		{8, 8, maxUint64},
		{9, 8, maxUint64},
		{100, 32, maxUint64},
	}
	for _, tt := range tests {
		if got := capacity(tt.depth, tt.fanoutBits); got != tt.want {
			t.Errorf("capacity(%d, %d) = %d, want %d", tt.depth, tt.fanoutBits, got, tt.want)
		}
	}
}

func TestPathDigits(t *testing.T) {
	tests := []struct {
		name             string
		i, depth, fanout uint64
		want             []uint64
		wantOK           bool
	}{
		{name: "padded to depth", i: 0, depth: 3, fanout: 2, want: []uint64{0, 0, 0}, wantOK: true},
		{name: "most significant first", i: 6, depth: 2, fanout: 2, want: []uint64{1, 2}, wantOK: true},
		{name: "last addressable", i: 15, depth: 2, fanout: 2, want: []uint64{3, 3}, wantOK: true},
		{name: "past capacity", i: 16, depth: 2, fanout: 2, wantOK: false},
		{name: "depth zero addresses only index zero", i: 0, depth: 0, fanout: 2, wantOK: true},
		{name: "depth zero rejects anything else", i: 1, depth: 0, fanout: 2, wantOK: false},
		{name: "worked example", i: 7264, depth: 2, fanout: 8, want: []uint64{28, 96}, wantOK: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := pathDigits(tt.i, tt.depth, tt.fanout)
			if ok != tt.wantOK {
				t.Fatalf("pathDigits(%d, %d, %d) ok = %t, want %t", tt.i, tt.depth, tt.fanout, ok, tt.wantOK)
			}
			if ok && !slices.Equal(got, tt.want) {
				t.Errorf("pathDigits(%d, %d, %d) = %v, want %v", tt.i, tt.depth, tt.fanout, got, tt.want)
			}
		})
	}
}

// TestPathDigitsRoundTrip: reading the digits back as a base-2^f number returns
// the index, for every index a depth-3 fanout-4 tree can address.
func TestPathDigitsRoundTrip(t *testing.T) {
	const fanout, depth = 2, 3
	for i := uint64(0); i < capacity(depth, fanout); i++ {
		digits, ok := pathDigits(i, depth, fanout)
		if !ok {
			t.Fatalf("pathDigits(%d, %d, %d) reported the index does not fit", i, depth, fanout)
		}
		var got uint64
		for _, d := range digits {
			got = got<<fanout | d
		}
		if got != i {
			t.Fatalf("digits %v read back as %d, want %d", digits, got, i)
		}
	}
}

func TestAllZero(t *testing.T) {
	tests := []struct {
		digits []uint64
		want   bool
	}{
		{nil, true},
		{[]uint64{0}, true},
		{[]uint64{0, 0}, true},
		{[]uint64{0, 1}, false},
		{[]uint64{1, 0}, false},
	}
	for _, tt := range tests {
		if got := allZero(tt.digits); got != tt.want {
			t.Errorf("allZero(%v) = %t, want %t", tt.digits, got, tt.want)
		}
	}
}
