package catalog_test

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/ipfs/go-cid"

	"github.com/blobarchive/bloar/catalog"
)

// pinKey renders an entry for comparison in tests.
type pinKey struct {
	purpose   string
	cid       string
	recursive bool
}

func keys(entries []catalog.PinEntry) []pinKey {
	out := make([]pinKey, 0, len(entries))
	for _, e := range entries {
		out = append(out, pinKey{e.Purpose, e.CID.String(), e.Recursive})
	}
	return out
}

func wantPins(t *testing.T, got []catalog.PinEntry, want []pinKey) {
	t.Helper()
	g := keys(got)
	if len(g) != len(want) {
		t.Fatalf("got %d pins %v, want %d %v", len(g), g, len(want), want)
	}
	for _, w := range want {
		found := false
		for _, e := range g {
			if e == w {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("pin %v missing from %v", w, g)
		}
	}
}

func TestLedgerAddListRemove(t *testing.T) {
	ctx := context.Background()
	l := catalog.NewLedger(openKV(t, t.TempDir()))

	root, seg := cidOf(t, "root"), cidOf(t, "seg")
	if err := l.Add(ctx, "all", "root", root, false); err != nil {
		t.Fatalf("Add root: %v", err)
	}
	if err := l.Add(ctx, "all", "window", seg, true); err != nil {
		t.Fatalf("Add window: %v", err)
	}

	wantPins(t, listOf(t, l, "all", "root"), []pinKey{{"root", root.String(), false}})
	wantPins(t, listOf(t, l, "all", "window"), []pinKey{{"window", seg.String(), true}})
	wantPins(t, listAllOf(t, l, "all"), []pinKey{
		{"root", root.String(), false},
		{"window", seg.String(), true},
	})

	if err := l.Remove(ctx, "all", "root", root); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	wantPins(t, listOf(t, l, "all", "root"), nil)
	wantPins(t, listAllOf(t, l, "all"), []pinKey{{"window", seg.String(), true}})

	// Removing what is already gone is the caller getting what it asked for.
	if err := l.Remove(ctx, "all", "root", root); err != nil {
		t.Fatalf("Remove of an absent pin: %v", err)
	}
}

func TestLedgerAddIsIdempotentAndOverwrites(t *testing.T) {
	ctx := context.Background()
	l := catalog.NewLedger(openKV(t, t.TempDir()))

	c := cidOf(t, "root")
	for range 2 {
		if err := l.Add(ctx, "all", "root", c, true); err != nil {
			t.Fatalf("Add: %v", err)
		}
	}
	wantPins(t, listOf(t, l, "all", "root"), []pinKey{{"root", c.String(), true}})

	// Same key, different flag: one row, last write wins.
	if err := l.Add(ctx, "all", "root", c, false); err != nil {
		t.Fatalf("Add re-flagged: %v", err)
	}
	wantPins(t, listOf(t, l, "all", "root"), []pinKey{{"root", c.String(), false}})
}

// TestLedgerPrefixIsolation is the test the key layout exists for: no scan may
// bleed across a head, a purpose, or into the catalog's half of the keyspace.
func TestLedgerPrefixIsolation(t *testing.T) {
	ctx := context.Background()
	kv := openKV(t, t.TempDir())
	c, l := catalog.New(kv), catalog.NewLedger(kv)

	// "a" is a prefix of "ab"; "root" is a prefix of "rootx". Both pairs would
	// collide under a naive concatenation.
	pins := []struct {
		head, purpose, label string
	}{
		{"a", "root", "a-root"},
		{"a", "rootx", "a-rootx"},
		{"ab", "root", "ab-root"},
		{"ab", "window", "ab-window"},
	}
	for _, p := range pins {
		if err := l.Add(ctx, p.head, p.purpose, cidOf(t, p.label), false); err != nil {
			t.Fatalf("Add %s/%s: %v", p.head, p.purpose, err)
		}
	}
	// Catalog entries in the same KV must be invisible to every ledger scan.
	for _, label := range []string{"blob1", "blob2"} {
		if err := c.Put(ctx, vhOf(label), cidOf(t, label)); err != nil {
			t.Fatalf("Put %s: %v", label, err)
		}
	}

	wantPins(t, listOf(t, l, "a", "root"), []pinKey{{"root", cidOf(t, "a-root").String(), false}})
	wantPins(t, listOf(t, l, "ab", "root"), []pinKey{{"root", cidOf(t, "ab-root").String(), false}})
	wantPins(t, listAllOf(t, l, "a"), []pinKey{
		{"root", cidOf(t, "a-root").String(), false},
		{"rootx", cidOf(t, "a-rootx").String(), false},
	})
	wantPins(t, listAllOf(t, l, "ab"), []pinKey{
		{"root", cidOf(t, "ab-root").String(), false},
		{"window", cidOf(t, "ab-window").String(), false},
	})
	wantPins(t, listAllOf(t, l, "unknown"), nil)
}

// TestLedgerSamePinUnderTwoPurposes: purpose is part of the identity of a row,
// because reconciliation diffs per purpose. Spec 9's window mode pins the open
// segment and the head root for different reasons and drops them at different
// times.
func TestLedgerSamePinUnderTwoPurposes(t *testing.T) {
	ctx := context.Background()
	l := catalog.NewLedger(openKV(t, t.TempDir()))

	c := cidOf(t, "shared")
	if err := l.Add(ctx, "all", "root", c, false); err != nil {
		t.Fatalf("Add root: %v", err)
	}
	if err := l.Add(ctx, "all", "index", c, true); err != nil {
		t.Fatalf("Add index: %v", err)
	}
	wantPins(t, listAllOf(t, l, "all"), []pinKey{
		{"index", c.String(), true},
		{"root", c.String(), false},
	})

	if err := l.Remove(ctx, "all", "root", c); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	wantPins(t, listAllOf(t, l, "all"), []pinKey{{"index", c.String(), true}})
}

func TestLedgerPersistsAcrossReopen(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	c := cidOf(t, "root")
	s := openStore(t, dir)
	if err := catalog.NewLedger(s.KV()).Add(ctx, "all", "window", c, true); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("closing store: %v", err)
	}

	l := catalog.NewLedger(openKV(t, dir))
	wantPins(t, listAllOf(t, l, "all"), []pinKey{{"window", c.String(), true}})
}

func TestLedgerRejectsUnencodableNames(t *testing.T) {
	ctx := context.Background()
	l := catalog.NewLedger(openKV(t, t.TempDir()))
	c := cidOf(t, "root")

	// A NUL in either component would make the key ambiguous; nothing else
	// about a head name or a purpose is this package's business.
	cases := []struct{ head, purpose string }{
		{"", "root"},
		{"all", ""},
		{"a\x00b", "root"},
		{"all", "ro\x00ot"},
	}
	for _, tc := range cases {
		if err := l.Add(ctx, tc.head, tc.purpose, c, false); err == nil {
			t.Errorf("Add(%q, %q): want error, got nil", tc.head, tc.purpose)
		}
		if err := l.Remove(ctx, tc.head, tc.purpose, c); err == nil {
			t.Errorf("Remove(%q, %q): want error, got nil", tc.head, tc.purpose)
		}
		if _, err := l.List(ctx, tc.head, tc.purpose); err == nil {
			t.Errorf("List(%q, %q): want error, got nil", tc.head, tc.purpose)
		}
	}
	if err := l.Add(ctx, "all", "root", cid.Undef, false); err == nil {
		t.Error("Add with cid.Undef: want error, got nil")
	}
}

func listOf(t *testing.T, l *catalog.Ledger, head, purpose string) []catalog.PinEntry {
	t.Helper()
	got, err := l.List(context.Background(), head, purpose)
	if err != nil {
		t.Fatalf("List(%q, %q): %v", head, purpose, err)
	}
	return got
}

func listAllOf(t *testing.T, l *catalog.Ledger, head string) []catalog.PinEntry {
	t.Helper()
	got, err := l.ListAll(context.Background(), head)
	if err != nil {
		t.Fatalf("ListAll(%q): %v", head, err)
	}
	return got
}

// TestPinExpiryRoundTrips checks the value encoding the staging pins of spec 9
// need: a pin that lapses, and one that does not, distinguishable in the ledger.
func TestPinExpiryRoundTrips(t *testing.T) {
	ctx := context.Background()
	l := catalog.NewLedger(openKV(t, t.TempDir()))

	// Second precision is what the encoding carries (the TTL it serves is
	// measured in hours), so the expectation is truncated to match rather than
	// carrying a monotonic reading this could never round-trip.
	expiry := time.Date(2026, 7, 16, 12, 0, 0, 0, time.UTC)
	if err := l.AddExpiring(ctx, "_staging", "staging", cidOf(t, "blob-a"), false, expiry); err != nil {
		t.Fatalf("AddExpiring: %v", err)
	}
	if err := l.Add(ctx, "_staging", "staging", cidOf(t, "blob-b"), false); err != nil {
		t.Fatalf("Add: %v", err)
	}

	entries, err := l.ListAll(ctx, "_staging")
	if err != nil {
		t.Fatalf("ListAll: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("ListAll returned %d entries, want 2", len(entries))
	}

	byCID := map[cid.Cid]catalog.PinEntry{}
	for _, e := range entries {
		byCID[e.CID] = e
	}
	got := byCID[cidOf(t, "blob-a")]
	if !got.Expires() {
		t.Error("a pin added with an expiry reports none")
	}
	if !got.Expiry.Equal(expiry) {
		t.Errorf("expiry = %s, want %s", got.Expiry, expiry)
	}
	if plain := byCID[cidOf(t, "blob-b")]; plain.Expires() {
		t.Errorf("a pin added without an expiry reports one (%s); every policy pin is this kind, and an expiry "+
			"on one would make reconciliation's rows lapse behind its back", plain.Expiry)
	}
}

// TestPinExpiryOverwrites is what makes a re-put of an almost-due blob extend
// its staging pin rather than race the old expiry.
func TestPinExpiryOverwrites(t *testing.T) {
	ctx := context.Background()
	l := catalog.NewLedger(openKV(t, t.TempDir()))
	c := cidOf(t, "blob")

	first := time.Date(2026, 7, 16, 12, 0, 0, 0, time.UTC)
	later := first.Add(time.Hour)
	for _, e := range []time.Time{first, later} {
		if err := l.AddExpiring(ctx, "_staging", "staging", c, false, e); err != nil {
			t.Fatalf("AddExpiring(%s): %v", e, err)
		}
	}

	entries, err := l.ListAll(ctx, "_staging")
	if err != nil {
		t.Fatalf("ListAll: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("re-adding one pin left %d rows, want 1", len(entries))
	}
	if !entries[0].Expiry.Equal(later) {
		t.Errorf("expiry after a re-add = %s, want the newer %s", entries[0].Expiry, later)
	}
}

// TestPinValueWithoutExpiryBitDecodes is the compatibility case: rows written
// before the expiry bit existed are one byte long, and a store upgraded in place
// is full of them.
//
// It writes the old encoding by hand rather than through Add, because Add is the
// thing that changed and using it would test nothing.
func TestPinValueWithoutExpiryBitDecodes(t *testing.T) {
	ctx := context.Background()
	kv := openKV(t, t.TempDir())
	l := catalog.NewLedger(kv)
	c := cidOf(t, "blob")

	// 'p' || "all" || 0x00 || "root" || 0x00 || cid, value flags=recursive only:
	// exactly what the ledger wrote before the expiry existed.
	key := append([]byte{'p'}, "all"...)
	key = append(key, 0x00)
	key = append(key, "root"...)
	key = append(key, 0x00)
	key = append(key, c.Bytes()...)
	if err := kv.Set(key, []byte{0x01}, pebble.Sync); err != nil {
		t.Fatalf("writing a legacy row: %v", err)
	}

	entries, err := l.ListAll(ctx, "all")
	if err != nil {
		t.Fatalf("ListAll: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("ListAll returned %d entries, want 1", len(entries))
	}
	if got := entries[0]; !got.Recursive || got.Expires() || got.Purpose != "root" || got.CID != c {
		t.Errorf("a legacy one-byte row decoded to %+v; want purpose=root, recursive, no expiry, cid=%s", got, c)
	}
}
