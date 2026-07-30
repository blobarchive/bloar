package archive

import "math/bits"

// The address arithmetic of spec 4. Every function here is pure: the directory
// stores no keys, so a slot's whole route through the tree is computed, never
// searched.

// ord returns the global segment ordinal of slot: which window of 2^segBits
// slots it belongs to.
func ord(slot, segBits uint64) uint64 { return slot >> segBits }

// windowStart returns the first slot of window w.
func windowStart(w, segBits uint64) uint64 { return w << segBits }

// windowEnd returns the last slot of window w.
func windowEnd(w, segBits uint64) uint64 { return ((w + 1) << segBits) - 1 }

// capacity returns the maximum number of sealed segments a directory of depth d
// can address, (2^fanoutBits)^d, saturating at MaxUint64.
//
// Saturation is not a limit anyone reaches: capacity is compared against a
// sealed count derived from slot numbers, so it only has to outrun uint64. A
// depth whose true capacity exceeds uint64 can hold every ordinal that exists,
// which is exactly what saturation reports.
func capacity(d, fanoutBits uint64) uint64 {
	bits := fanoutBits * d
	if fanoutBits != 0 && bits/fanoutBits != d { // uint64 multiply overflowed
		return maxUint64
	}
	if bits >= 64 {
		return maxUint64
	}
	return uint64(1) << bits
}

const maxUint64 = ^uint64(0)

// canonicalDepth returns the one directory depth a fresh append-only build has
// after n sealed windows. A signed Head is not allowed to choose a deeper tree:
// its synced_to and immutable parameters determine n, and therefore determine
// this value exactly.
//
// bits.Len64(n-1) is the number of radix bits needed for the highest occupied
// index. The n == 1 case still needs one directory page.
func canonicalDepth(n, fanoutBits uint64) uint64 {
	if n == 0 {
		return 0
	}
	needed := uint64(bits.Len64(n - 1))
	if needed == 0 {
		return 1
	}
	return (needed + fanoutBits - 1) / fanoutBits
}

// canonicalDirectoryPages is the exact number of logical DirNode positions in
// a canonical directory holding n appended entries. Content addressing may
// collapse byte-identical empty pages to one physical CID; the traversal still
// accounts for every logical position against its path budget.
func canonicalDirectoryPages(n, fanoutBits uint64) uint64 {
	if n == 0 {
		return 0
	}
	var pages uint64
	for depth := uint64(1); ; depth++ {
		cap := capacity(depth, fanoutBits)
		atDepth := uint64(1)
		if cap < n {
			atDepth = 1 + (n-1)/cap
		}
		if pages > maxUint64-atDepth {
			return maxUint64
		}
		pages += atDepth
		if cap >= n {
			return pages
		}
	}
}

// pathDigits returns i written base 2^fanoutBits, most significant first,
// padded to depth digits. ok is false if i does not fit in depth digits, which
// for a lookup means the index is past everything the directory can address and
// is therefore absent.
func pathDigits(i, depth, fanoutBits uint64) ([]uint64, bool) {
	if depth == 0 {
		return nil, i == 0
	}
	if depth > 64 { // absurd; guards the allocation below
		return nil, false
	}
	mask := (uint64(1) << fanoutBits) - 1
	digits := make([]uint64, depth)
	rest := i
	for level := int(depth) - 1; level >= 0; level-- {
		digits[level] = rest & mask
		rest >>= fanoutBits
	}
	if rest != 0 {
		return nil, false
	}
	return digits, true
}

// allZero reports whether every digit is zero, i.e. the digits address index 0
// of the subtree. A suffix of all zeros means "this subtree holds no appended
// entries yet", which is how truncation tells a page that must survive empty
// from one that must not exist at all.
func allZero(digits []uint64) bool {
	for _, d := range digits {
		if d != 0 {
			return false
		}
	}
	return true
}
