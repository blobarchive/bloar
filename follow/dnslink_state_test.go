package follow

import (
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/ipfs/boxo/ipns"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
)

func testIPNSName(t *testing.T) ipns.Name {
	t.Helper()
	_, public, err := crypto.GenerateEd25519Key(nil)
	if err != nil {
		t.Fatal(err)
	}
	id, err := peer.IDFromPublicKey(public)
	if err != nil {
		t.Fatal(err)
	}
	return ipns.NameFromPeer(id)
}

func TestIPNSSequenceFloorsArePerNameBoundedAndProtectDelegation(t *testing.T) {
	kv, err := pebble.Open(t.TempDir(), &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = kv.Close() })
	s := &state{kv: kv}

	names := make([]ipns.Name, maxIPNSFloorNames+2)
	for i := range names {
		names[i] = testIPNSName(t)
	}
	if err := s.commitAuthority(time.Unix(1, 0), &delegation{name: names[0], pubkey: make([]byte, 32)}); err != nil {
		t.Fatal(err)
	}
	for i, name := range names {
		if err := s.setIPNSSeq(name, uint64(i+1), false); err != nil {
			t.Fatalf("setting floor %d: %v", i, err)
		}
	}

	floors, err := s.ipnsFloors()
	if err != nil {
		t.Fatal(err)
	}
	if len(floors) != maxIPNSFloorNames {
		t.Fatalf("floor count = %d, want bound %d", len(floors), maxIPNSFloorNames)
	}
	if seq, ok, err := s.ipnsSeq(names[0], false); err != nil || !ok || seq != 1 {
		t.Fatalf("committed delegation floor = %d, ok=%t, err=%v; it was evicted", seq, ok, err)
	}
	if _, ok, err := s.ipnsSeq(names[1], false); err != nil || ok {
		t.Fatalf("oldest unprotected floor survived: ok=%t err=%v", ok, err)
	}
	if seq, ok, err := s.ipnsSeq(names[len(names)-1], false); err != nil || !ok || seq != uint64(len(names)) {
		t.Fatalf("newest floor = %d, ok=%t, err=%v", seq, ok, err)
	}
}

func TestIPNSSequenceFloorsDoNotCompareIndependentNames(t *testing.T) {
	kv, err := pebble.Open(t.TempDir(), &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = kv.Close() })
	s := &state{kv: kv}
	first, second := testIPNSName(t), testIPNSName(t)
	if err := s.setIPNSSeq(first, 1<<40, false); err != nil {
		t.Fatal(err)
	}
	if err := s.setIPNSSeq(second, 1, false); err != nil {
		t.Fatal(err)
	}
	if seq, ok, err := s.ipnsSeq(second, false); err != nil || !ok || seq != 1 {
		t.Fatalf("second name inherited first sequence: seq=%d ok=%t err=%v", seq, ok, err)
	}
}

func TestLegacyIPNSFloorMigratesOnceToConfiguredName(t *testing.T) {
	kv, err := pebble.Open(t.TempDir(), &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = kv.Close() })
	s := &state{kv: kv}
	first, second := testIPNSName(t), testIPNSName(t)
	if err := s.put(keyIPNSSeq, 40); err != nil {
		t.Fatal(err)
	}
	if seq, ok, err := s.ipnsSeq(first, true); err != nil || !ok || seq != 40 {
		t.Fatalf("legacy floor before migration = %d, ok=%t, err=%v", seq, ok, err)
	}
	if err := s.setIPNSSeq(first, 41, true); err != nil {
		t.Fatal(err)
	}
	if seq, ok, err := s.ipnsSeq(first, false); err != nil || !ok || seq != 41 {
		t.Fatalf("migrated first-name floor = %d, ok=%t, err=%v", seq, ok, err)
	}
	if _, ok, err := s.get(keyIPNSSeq); err != nil || ok {
		t.Fatalf("legacy floor survived migration: ok=%t err=%v", ok, err)
	}
	if _, ok, err := s.ipnsSeq(second, true); err != nil || ok {
		t.Fatalf("unrelated second name inherited retired legacy floor: ok=%t err=%v", ok, err)
	}
}
