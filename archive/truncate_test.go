package archive_test

import (
	"context"
	"strings"
	"testing"

	"github.com/ipfs/boxo/blockstore"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"

	"github.com/blobarchive/bloar/archive"
	"github.com/blobarchive/bloar/store"
)

type getCountingBlockstore struct {
	blockstore.Blockstore
	gets map[string]int
}

func TestTruncateRetainingWindowProtectsNewlyRecursiveSegments(t *testing.T) {
	plain := newBlockstore()
	epochs := store.NewBlockstoreEpochs(store.Validating(plain))
	app := epochs.Application()
	hs := newHarnessOver(t, testParams(), app, newFakeCatalog(app))

	// Build through window 10. With a 16-slot trailing window, the old head's
	// recursive range starts in window 8. Rewinding to window 6 moves it back to
	// window 5, so that sealed Segment and its blob were not in the old M set.
	hs.apply([]archive.RefRow{
		hs.row(41, 1), // window 5: newly retained by the rewind
		hs.row(49, 2), // window 6: rebuilt target
		hs.row(65, 3), // window 8: retained before the rewind
		hs.row(73, 4), // window 9: removed by the rewind
	}, 87)

	epoch, err := epochs.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	defer epoch.End()
	if _, err := hs.h.TruncateRetainingWindow(t.Context(), 55, 16); err != nil {
		t.Fatalf("TruncateRetainingWindow: %v", err)
	}

	newlyRetained := blobCID(t, mkVH(1))
	if deleted, protected, err := epoch.DeleteCandidate(t.Context(), newlyRetained); err != nil {
		t.Fatalf("DeleteCandidate newly retained blob: %v", err)
	} else if deleted || !protected {
		t.Fatalf("newly retained blob: deleted=%t protected=%t, want false/true", deleted, protected)
	}

	removed := blobCID(t, mkVH(4))
	if deleted, protected, err := epoch.DeleteCandidate(t.Context(), removed); err != nil {
		t.Fatalf("DeleteCandidate removed blob: %v", err)
	} else if !deleted || protected {
		t.Fatalf("removed blob: deleted=%t protected=%t, want true/false", deleted, protected)
	}
}

func (s *getCountingBlockstore) Get(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	s.gets[c.KeyString()]++
	return s.Blockstore.Get(ctx, c)
}

// dataset is a head's whole logical content: the rows, and how far coverage
// reaches. Two heads holding the same dataset must have the same root CID,
// however they got there -- that is what makes a rebuild verifiable (spec
// 11.5).
type dataset struct {
	rows     []archive.RefRow
	syncedTo uint64
}

// upTo returns the dataset a head truncated to slot must hold.
func (d dataset) upTo(slot uint64) dataset {
	out := dataset{syncedTo: slot}
	for _, r := range d.rows {
		if r.Slot > slot {
			break
		}
		out.rows = append(out.rows, r)
	}
	return out
}

// buildFresh applies d to a brand-new head in one batch and returns its root.
// It shares the blockstore and catalog with hs, which is the point: identical
// content must reach identical blocks.
func (hs *harness) buildFresh(t *testing.T, params archive.Params, d dataset) cid.Cid {
	t.Helper()
	fresh := newHarnessOver(t, params, hs.bs, hs.cat)
	fresh.apply(d.rows, d.syncedTo)
	return fresh.h.Root()
}

// spread builds rows across windows 5..(5+windows-1), skipping any window in
// blank, and covers through the end of the last window.
func (hs *harness) spread(windows uint64, blank ...uint64) dataset {
	hs.t.Helper()
	skip := make(map[uint64]bool, len(blank))
	for _, w := range blank {
		skip[w] = true
	}
	d := dataset{syncedTo: testOrigin + windows*8 - 1}
	for i := uint64(0); i < windows; i++ {
		w := 5 + i
		if skip[w] {
			continue
		}
		base := testOrigin + i*8
		d.rows = append(d.rows, hs.row(base+1, base*10+1), hs.row(base+5, base*10+2, base*10+3))
	}
	return d
}

func TestTruncateRefusedPastSyncedTo(t *testing.T) {
	hs := newHarness(t, testParams())
	hs.apply([]archive.RefRow{hs.row(41, 410)}, 44)
	root := hs.h.Root()

	if _, err := hs.h.Truncate(hs.ctx, 45); err == nil {
		t.Fatalf("Truncate(45) past synced_to 44 was accepted")
	} else {
		wantConflict(t, err, "truncate past synced_to")
	}
	if hs.h.Root() != root {
		t.Errorf("a refused truncate changed the root")
	}
	if synced, _ := hs.h.SyncedTo(); synced != 44 {
		t.Errorf("synced_to = %d after a refused truncate, want 44", synced)
	}
}

func TestTruncateRefusedOnEmptyHead(t *testing.T) {
	hs := newHarness(t, testParams())
	if _, err := hs.h.Truncate(hs.ctx, testOrigin); err == nil {
		t.Fatalf("Truncate on an empty head was accepted")
	} else {
		wantConflict(t, err, "truncate an empty head")
	}
}

func TestTruncateRefusedBeforeOrigin(t *testing.T) {
	hs := newHarness(t, testParams())
	hs.apply([]archive.RefRow{hs.row(41, 410)}, 44)
	if _, err := hs.h.Truncate(hs.ctx, testOrigin-1); err == nil {
		t.Fatalf("Truncate below origin_slot was accepted")
	} else {
		wantConflict(t, err, "truncate below origin")
	}
}

// TestTruncateWithinOpenWindow: the common case, where nothing is sealed and
// only the open segment loses rows.
func TestTruncateWithinOpenWindow(t *testing.T) {
	hs := newHarness(t, testParams())
	hs.apply([]archive.RefRow{hs.row(41, 410), hs.row(43, 430), hs.row(45, 450)}, 46)

	root, err := hs.h.Truncate(hs.ctx, 43)
	if err != nil {
		t.Fatalf("Truncate(43): %v", err)
	}
	if root != hs.h.Root() {
		t.Errorf("Truncate returned %s but Root() is %s", root, hs.h.Root())
	}
	if synced, _ := hs.h.SyncedTo(); synced != 43 {
		t.Errorf("synced_to = %d, want 43", synced)
	}

	wantBlobs(t, hs.lookup(41), "kept slot 41", 410)
	wantBlobs(t, hs.lookup(43), "kept slot 43 (the truncation point)", 430)
	wantStatus(t, hs.lookup(45), archive.StatusNotYetCovered, "dropped slot 45")
	wantStatus(t, hs.lookup(44), archive.StatusNotYetCovered, "slot past the new synced_to")
}

// TestTruncateRefusesMissingSurvivingBlob is the publication-closure regression:
// copying a row from the old segment is not enough to prove its blob is still on
// disk. A window-retained Truncate must re-touch every surviving raw CID before
// it publishes the rebuilt Segment and Head, so an online GC cannot remove a
// copied child between the old read and the new root publication.
func TestTruncateRefusesMissingSurvivingBlob(t *testing.T) {
	hs := newHarness(t, testParams())
	hs.apply([]archive.RefRow{hs.row(41, 410), hs.row(43, 430), hs.row(45, 450)}, 46)
	root := hs.h.Root()
	missing := blobCID(t, mkVH(430))
	if err := hs.bs.DeleteBlock(hs.ctx, missing); err != nil {
		t.Fatalf("DeleteBlock(%s): %v", missing, err)
	}

	if _, err := hs.h.TruncateRetainingWindow(hs.ctx, 43, 16); err == nil {
		t.Fatal("Truncate published a rebuilt segment whose surviving blob is missing")
	} else if msg := err.Error(); !strings.Contains(msg, missing.String()) || !strings.Contains(msg, "refusing to publish") {
		t.Fatalf("Truncate error = %q, want the missing blob CID and publication refusal", msg)
	}
	if got := hs.h.Root(); got != root {
		t.Errorf("failed Truncate changed the published root to %s, want %s", got, root)
	}
}

func TestTruncateTouchesEachDistinctSurvivingBlobOnce(t *testing.T) {
	bs := &getCountingBlockstore{Blockstore: newBlockstore(), gets: map[string]int{}}
	hs := newHarnessOver(t, testParams(), bs, newFakeCatalog(bs))
	// The same raw CID appears twice in one row and again in another surviving
	// row. Publication protection is per physical block, so one touch suffices.
	hs.apply([]archive.RefRow{hs.row(41, 410, 410), hs.row(43, 410)}, 46)
	clear(bs.gets)

	if _, err := hs.h.TruncateRetainingWindow(hs.ctx, 43, 16); err != nil {
		t.Fatalf("Truncate(43): %v", err)
	}
	blob := blobCID(t, mkVH(410))
	if got := bs.gets[blob.KeyString()]; got != 1 {
		t.Errorf("surviving blob %s was touched %d times, want once despite duplicate edges", blob, got)
	}
}

func TestPlainTruncateAllowsAbsentBlobForNoneRetention(t *testing.T) {
	hs := newHarness(t, testParams())
	hs.apply([]archive.RefRow{hs.row(41, 410), hs.row(43, 430), hs.row(45, 450)}, 46)
	missing := blobCID(t, mkVH(430))
	if err := hs.bs.DeleteBlock(hs.ctx, missing); err != nil {
		t.Fatalf("DeleteBlock(%s): %v", missing, err)
	}

	// Plain Truncate is the ModeNone-compatible path: it rebuilds the complete
	// index but does not claim raw retention. Missing historical raw children
	// are therefore legitimate and must not prevent the rewind.
	if _, err := hs.h.Truncate(hs.ctx, 43); err != nil {
		t.Fatalf("plain Truncate with an absent non-retained blob: %v", err)
	}
	wantBlobs(t, hs.lookup(43), "index row retained under none", 430)
}

// TestTruncateToSealedWindow: truncating back into a sealed window drops every
// later entry and reopens that window from the sealed segment's surviving rows
// (spec 5.4).
func TestTruncateToSealedWindow(t *testing.T) {
	hs := newHarness(t, testParams())
	d := hs.spread(4) // windows 5..8, all sealed, synced_to 71
	hs.apply(d.rows, d.syncedTo)
	if got := hs.h.Info().DirDepth; got != 1 {
		t.Fatalf("dir_depth = %d after 4 seals, want 1", got)
	}

	// Slot 53 is mid-window-6. Window 5 stays sealed; window 6 reopens holding
	// only its rows at or before 53; windows 7 and 8 vanish.
	if _, err := hs.h.Truncate(hs.ctx, 53); err != nil {
		t.Fatalf("Truncate(53): %v", err)
	}
	if synced, _ := hs.h.SyncedTo(); synced != 53 {
		t.Errorf("synced_to = %d, want 53", synced)
	}
	if got := hs.h.Info().DirDepth; got != 1 {
		t.Errorf("dir_depth = %d, want 1 (window 5 is still sealed)", got)
	}

	wantBlobs(t, hs.lookup(41), "sealed window 5 survives", 401)
	wantBlobs(t, hs.lookup(49), "window 6 row at 49", 481)
	wantBlobs(t, hs.lookup(53), "window 6 row at 53 (the truncation point)", 482, 483)
	wantStatus(t, hs.lookup(57), archive.StatusNotYetCovered, "window 7 is gone")
	wantStatus(t, hs.lookup(65), archive.StatusNotYetCovered, "window 8 is gone")

	// It matches a head that was only ever built this far.
	if want := hs.buildFresh(t, testParams(), d.upTo(53)); hs.h.Root() != want {
		t.Errorf("truncated root %s != freshly built root %s", hs.h.Root(), want)
	}
}

// TestTruncateAtWindowEndSeals is the case where spec 5.4 read literally
// contradicts spec 5.1.
//
// 5.4 says the open segment is rebuilt from window ord(slot) unconditionally.
// But when slot is the last slot of its window, that window is fully covered,
// and 5.1 requires the open segment to be the window ord(synced_to+1) -- here
// t+1, not t. Following 5.4 to the letter would leave window t's rows in an
// open segment the next apply never revisits (it resumes at t+1), stranding
// them outside the directory forever. So the seal rule runs over the rebuilt
// window, and the result is what a fresh build produces.
func TestTruncateAtWindowEndSeals(t *testing.T) {
	hs := newHarness(t, testParams())
	d := hs.spread(4) // windows 5..8, synced_to 71
	hs.apply(d.rows, d.syncedTo)

	// Slot 47 is the last slot of window 5.
	if _, err := hs.h.Truncate(hs.ctx, 47); err != nil {
		t.Fatalf("Truncate(47): %v", err)
	}
	if synced, _ := hs.h.SyncedTo(); synced != 47 {
		t.Errorf("synced_to = %d, want 47", synced)
	}
	// Window 5 must be sealed, not left open: it is fully covered.
	if got := hs.h.Info().DirDepth; got != 1 {
		t.Errorf("dir_depth = %d, want 1: the fully covered window 5 must be sealed", got)
	}
	head := readHead(t, hs.bs, hs.h.Root())
	dir := readDir(t, hs.bs, head.Dir)
	if len(dir.Kids) != 1 || !dir.Kids[0].Defined() {
		t.Errorf("directory kids = %v, want exactly one sealed entry", dir.Kids)
	}

	wantBlobs(t, hs.lookup(41), "window 5 row at 41", 401)
	wantBlobs(t, hs.lookup(45), "window 5 row at 45", 402, 403)
	wantStatus(t, hs.lookup(48), archive.StatusNotYetCovered, "window 6 is gone")

	if want := hs.buildFresh(t, testParams(), d.upTo(47)); hs.h.Root() != want {
		t.Errorf("truncated root %s != freshly built root %s", hs.h.Root(), want)
	}

	// And the head is usable: the next batch resumes in window 6 and lands.
	hs.apply([]archive.RefRow{hs.row(49, 490)}, 55)
	wantBlobs(t, hs.lookup(49), "row applied after the truncate", 490)
	wantBlobs(t, hs.lookup(41), "window 5 still readable after resuming", 401)
}

// TestTruncateShrinksDepth: dropping entries unwraps the root levels that only
// existed to hold them (spec 5.4), back to exactly the depth a fresh build
// would have.
func TestTruncateShrinksDepth(t *testing.T) {
	ref := newHarness(t, testParams())
	d := ref.spread(20) // 20 sealed windows -> depth 3 at fanout 4
	ref.apply(d.rows, d.syncedTo)
	if got := ref.h.Info().DirDepth; got != 3 {
		t.Fatalf("dir_depth = %d, want 3", got)
	}

	// Each step truncates to the end of window (5+n-1), leaving n sealed
	// entries, and the depth must fall to the canonical depth for n: 17 keeps
	// depth 3, 16 falls to 2, 4 falls to 1.
	for _, n := range []uint64{17, 16, 5, 4, 2, 1} {
		hs := newHarnessOver(t, testParams(), ref.bs, ref.cat)
		hs.apply(d.rows, d.syncedTo)

		slot := testOrigin + n*8 - 1 // last slot of the n'th window
		if _, err := hs.h.Truncate(hs.ctx, slot); err != nil {
			t.Fatalf("Truncate(%d): %v", slot, err)
		}
		want := canonicalDepth(n, testFanoutBits)
		if got := hs.h.Info().DirDepth; got != want {
			t.Errorf("truncate to %d sealed entries: dir_depth = %d, want %d", n, got, want)
		}
		if root, fresh := hs.h.Root(), hs.buildFresh(t, testParams(), d.upTo(slot)); root != fresh {
			t.Errorf("truncate to %d sealed entries: root %s != freshly built %s", n, root, fresh)
		}
	}
}

// TestTruncateKeepsEmptyPage: a directory page whose every entry is a null is
// still a page. It exists because windows were sealed into it, and a fresh
// build writes it too -- so truncation must keep it rather than treat "no live
// kids" as "no page".
func TestTruncateKeepsEmptyPage(t *testing.T) {
	hs := newHarness(t, testParams())
	// Windows 5..11 with window 9 (entry 4) carrying no rows at all.
	d := hs.spread(7, 9)
	hs.apply(d.rows, d.syncedTo)

	// Slot 83 is mid-window-10, so entries 0..4 survive and entry 4 is the null
	// one: it lands alone in the second page at fanout 4.
	if _, err := hs.h.Truncate(hs.ctx, 83); err != nil {
		t.Fatalf("Truncate(83): %v", err)
	}
	if got, want := hs.h.Info().DirDepth, canonicalDepth(5, testFanoutBits); got != want {
		t.Errorf("dir_depth = %d, want %d", got, want)
	}

	head := readHead(t, hs.bs, hs.h.Root())
	dir := readDir(t, hs.bs, head.Dir)
	if len(dir.Kids) != 2 {
		t.Fatalf("root has %d kids, want 2 (a full first page, and a second holding the null entry)", len(dir.Kids))
	}
	page := readDir(t, hs.bs, dir.Kids[1])
	if len(page.Kids) != 0 {
		t.Errorf("the second page has %d kids, want 0: its only entry is a null", len(page.Kids))
	}

	if want := hs.buildFresh(t, testParams(), d.upTo(83)); hs.h.Root() != want {
		t.Errorf("truncated root %s != freshly built root %s", hs.h.Root(), want)
	}
}

// TestTruncateToOriginWindow: truncating into the very first window empties the
// directory entirely -- depth 0, dir null (spec 3.1 requires the pair).
func TestTruncateToOriginWindow(t *testing.T) {
	hs := newHarness(t, testParams())
	d := hs.spread(4)
	hs.apply(d.rows, d.syncedTo)

	if _, err := hs.h.Truncate(hs.ctx, 45); err != nil { // mid-window 5
		t.Fatalf("Truncate(45): %v", err)
	}
	info := hs.h.Info()
	if info.DirDepth != 0 {
		t.Errorf("dir_depth = %d, want 0", info.DirDepth)
	}
	head := readHead(t, hs.bs, info.Root)
	if head.Dir.Defined() {
		t.Errorf("dir = %s with dir_depth 0; spec 3.1 requires dir null iff dir_depth is 0", head.Dir)
	}
	wantBlobs(t, hs.lookup(41), "row at 41", 401)
	wantBlobs(t, hs.lookup(45), "row at 45", 402, 403)
	wantStatus(t, hs.lookup(46), archive.StatusNotYetCovered, "past the new synced_to")

	if want := hs.buildFresh(t, testParams(), d.upTo(45)); hs.h.Root() != want {
		t.Errorf("truncated root %s != freshly built root %s", hs.h.Root(), want)
	}
}

// TestTruncateToEmpty resets the head to the state New left it in, byte for
// byte.
func TestTruncateToEmpty(t *testing.T) {
	hs := newHarness(t, testParams())
	empty := hs.h.Root()

	d := hs.spread(6)
	hs.apply(d.rows, d.syncedTo)
	if hs.h.Root() == empty {
		t.Fatalf("applying rows did not change the root")
	}

	root, err := hs.h.TruncateToEmpty(hs.ctx)
	if err != nil {
		t.Fatalf("TruncateToEmpty: %v", err)
	}
	if root != empty {
		t.Errorf("root after reset = %s, want the original empty head %s", root, empty)
	}
	if _, covered := hs.h.SyncedTo(); covered {
		t.Errorf("a reset head reports coverage")
	}
	if got := hs.h.Info().DirDepth; got != 0 {
		t.Errorf("dir_depth = %d after reset, want 0", got)
	}
	wantStatus(t, hs.lookup(41), archive.StatusNotYetCovered, "a row that was truncated away")

	// It rebuilds from empty exactly as a new head does.
	hs.apply(d.rows, d.syncedTo)
	if want := hs.buildFresh(t, testParams(), d); hs.h.Root() != want {
		t.Errorf("rebuilt root %s != freshly built root %s", hs.h.Root(), want)
	}
}

// TestTruncateThenReapplyConverges: truncate to every slot of a head, re-apply
// what was dropped, and land on the original root every time. Truncation is a
// rewind, not a fork.
func TestTruncateThenReapplyConverges(t *testing.T) {
	ref := newHarness(t, testParams())
	d := ref.spread(6, 7) // windows 5..10, window 7 empty
	ref.apply(d.rows, d.syncedTo)
	want := ref.h.Root()

	for slot := uint64(testOrigin); slot <= d.syncedTo; slot++ {
		hs := newHarnessOver(t, testParams(), ref.bs, ref.cat)
		hs.apply(d.rows, d.syncedTo)

		if _, err := hs.h.Truncate(hs.ctx, slot); err != nil {
			t.Fatalf("Truncate(%d): %v", slot, err)
		}
		if got, fresh := hs.h.Root(), hs.buildFresh(t, testParams(), d.upTo(slot)); got != fresh {
			t.Fatalf("truncate to %d: root %s != freshly built %s", slot, got, fresh)
		}

		// Re-apply exactly what the truncate dropped.
		var rest []archive.RefRow
		for _, r := range d.rows {
			if r.Slot > slot {
				rest = append(rest, r)
			}
		}
		hs.apply(rest, d.syncedTo)
		if got := hs.h.Root(); got != want {
			t.Fatalf("truncate to %d then re-apply: root %s, want the original %s", slot, got, want)
		}
	}
}
