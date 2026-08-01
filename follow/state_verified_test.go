package follow

import (
	"testing"

	"github.com/cockroachdb/pebble/v2"
)

func TestSegmentVerificationProofPersistsOutsideTheHotCache(t *testing.T) {
	kv, err := pebble.Open(t.TempDir(), &pebble.Options{})
	if err != nil {
		t.Fatalf("pebble.Open: %v", err)
	}
	t.Cleanup(func() { _ = kv.Close() })
	c := epochTestCID(t, 42)

	first := &state{kv: kv}
	if ok, err := first.segmentVerified(c); err != nil || ok {
		t.Fatalf("proof before verification: ok=%t err=%v, want false/nil", ok, err)
	}
	if err := first.markSegmentVerified(c); err != nil {
		t.Fatalf("markSegmentVerified: %v", err)
	}

	// A fresh state value stands in for a new Follower process: no in-memory
	// verifiedSegments entry exists, so only the durable, versioned marker can
	// answer true.
	second := &state{kv: kv}
	if ok, err := second.segmentVerified(c); err != nil || !ok {
		t.Fatalf("persisted proof: ok=%t err=%v, want true/nil", ok, err)
	}
}

func TestSealedSegmentPromotesAnOpenSegmentHotProof(t *testing.T) {
	kv, err := pebble.Open(t.TempDir(), &pebble.Options{})
	if err != nil {
		t.Fatalf("pebble.Open: %v", err)
	}
	t.Cleanup(func() { _ = kv.Close() })
	c := epochTestCID(t, 43)

	f := &Follower{
		state:            &state{kv: kv},
		verifiedSegments: map[string]bool{c.KeyString(): false},
		verifiedOpen:     map[string]string{"all": c.KeyString()},
	}
	if ok, err := f.segmentVerified("all", c, true); err != nil || !ok {
		t.Fatalf("promoting hot proof: ok=%t err=%v, want true/nil", ok, err)
	}

	// A fresh state stands in for the next process. The proof started as an
	// open-Segment memory entry, but learning that the same CID is now sealed
	// must have promoted it to the versioned durable key.
	if ok, err := (&state{kv: kv}).segmentVerified(c); err != nil || !ok {
		t.Fatalf("promoted durable proof: ok=%t err=%v, want true/nil", ok, err)
	}
}

func TestOpenSegmentVerificationCacheIsBoundedPerHead(t *testing.T) {
	first := epochTestCID(t, 44)
	second := epochTestCID(t, 45)
	f := &Follower{
		verifiedSegments: map[string]bool{first.KeyString(): false},
		verifiedOpen:     map[string]string{"all": first.KeyString()},
	}

	if ok, err := f.segmentVerified("all", second, false); err != nil || ok {
		t.Fatalf("new open proof lookup: ok=%t err=%v, want false/nil", ok, err)
	}
	if _, ok := f.verifiedSegments[first.KeyString()]; ok {
		t.Fatal("superseded open Segment proof remained in the hot cache")
	}
	if got := f.verifiedOpen["all"]; got != second.KeyString() {
		t.Fatalf("current open proof key = %q, want %q", got, second.KeyString())
	}
}

func TestMalformedSegmentVerificationProofFailsClosed(t *testing.T) {
	kv, err := pebble.Open(t.TempDir(), &pebble.Options{})
	if err != nil {
		t.Fatalf("pebble.Open: %v", err)
	}
	t.Cleanup(func() { _ = kv.Close() })
	c := epochTestCID(t, 44)
	if err := kv.Set(verifiedSegmentKey(c), []byte("not-a-proof"), pebble.Sync); err != nil {
		t.Fatalf("writing malformed proof: %v", err)
	}
	if ok, err := (&state{kv: kv}).segmentVerified(c); err == nil || ok {
		t.Fatalf("malformed proof: ok=%t err=%v, want false/error", ok, err)
	}
}
