package archive_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/blobarchive/bloar/archive"
	"github.com/blobarchive/bloar/schema"
)

// TestNewHeadIsEmpty: a head that has never been applied to covers nothing and
// has no directory and no open segment (spec 3.1).
func TestNewHeadIsEmpty(t *testing.T) {
	hs := newHarness(t, testParams())

	if _, covered := hs.h.SyncedTo(); covered {
		t.Errorf("a new head reports coverage")
	}
	info := hs.h.Info()
	if info.SyncedTo != nil {
		t.Errorf("Info().SyncedTo = %d, want null", *info.SyncedTo)
	}
	if info.DirDepth != 0 {
		t.Errorf("Info().DirDepth = %d, want 0", info.DirDepth)
	}
	if !info.Root.Defined() {
		t.Errorf("a new head has no root CID")
	}
	wantStatus(t, hs.lookup(testOrigin), archive.StatusNotYetCovered, "lookup at origin on an empty head")
}

// TestApplySingleBatch: the plain case. Rows land, coverage advances, lookups
// answer.
func TestApplySingleBatch(t *testing.T) {
	hs := newHarness(t, testParams())

	res := hs.apply([]archive.RefRow{
		hs.row(41, 410),
		hs.row(43, 430, 431),
	}, 44)

	if res.NoOp {
		t.Errorf("a first batch reported NoOp")
	}
	if res.SyncedTo != 44 {
		t.Errorf("SyncedTo = %d, want 44", res.SyncedTo)
	}
	if res.Root != hs.h.Root() {
		t.Errorf("ApplyResult.Root %s does not match Root() %s", res.Root, hs.h.Root())
	}

	wantBlobs(t, hs.lookup(41), "slot 41", 410)
	wantBlobs(t, hs.lookup(43), "slot 43", 430, 431)
	// Covered, no blobs: the archive knows it carried nothing (spec 7.1 -> 200
	// with an empty list), which is not the same as "not archived".
	wantBlobs(t, hs.lookup(42), "covered blobless slot 42")
	wantStatus(t, hs.lookup(45), archive.StatusNotYetCovered, "slot past synced_to")
	wantStatus(t, hs.lookup(39), archive.StatusBeforeOrigin, "slot before origin")
}

// TestEntryOrderIsPreserved: entry order within a row is part of the content
// (spec 3.2), so it must survive a round trip exactly, and the two orders must
// produce different roots.
func TestEntryOrderIsPreserved(t *testing.T) {
	forward := newHarness(t, testParams())
	forward.apply([]archive.RefRow{forward.row(41, 1, 2, 3)}, 41)
	wantBlobs(t, forward.lookup(41), "stored order", 1, 2, 3)

	reversed := newHarness(t, testParams())
	reversed.apply([]archive.RefRow{reversed.row(41, 3, 2, 1)}, 41)
	wantBlobs(t, reversed.lookup(41), "reversed order", 3, 2, 1)

	if forward.h.Root() == reversed.h.Root() {
		t.Errorf("rows differing only in entry order share root %s; order must affect the CID", forward.h.Root())
	}
}

// TestSealAtWindowBoundary: a batch ending exactly on a window boundary seals
// that window and opens the next (spec 5.1, 5.2). The sealed rows stay
// readable, which means the directory took them.
func TestSealAtWindowBoundary(t *testing.T) {
	hs := newHarness(t, testParams())

	// Window 5 is slots 40..47.
	res := hs.apply([]archive.RefRow{hs.row(41, 410), hs.row(47, 470)}, 47)

	if got := hs.h.Info().DirDepth; got != 1 {
		t.Errorf("dir_depth = %d after one seal, want 1", got)
	}
	wantBlobs(t, hs.lookup(41), "sealed slot 41", 410)
	wantBlobs(t, hs.lookup(47), "sealed slot 47", 470)
	enumeration, err := hs.h.Enumerate(hs.ctx)
	if err != nil {
		t.Fatalf("Enumerate after seal: %v", err)
	}
	if len(enumeration.Sealed) != 1 || len(res.Index.Segments) != 2 {
		t.Fatalf("sealed blocks/samples = %d/%d, want 1 block and sealed+open samples", len(enumeration.Sealed), len(res.Index.Segments))
	}
	sealedBlock, err := hs.bs.Get(hs.ctx, enumeration.Sealed[0].CID)
	if err != nil {
		t.Fatalf("reading sealed Segment: %v", err)
	}
	openBlock, err := hs.bs.Get(hs.ctx, enumeration.Open)
	if err != nil {
		t.Fatalf("reading open Segment: %v", err)
	}
	sealed, open := res.Index.Segments[0], res.Index.Segments[1]
	if sealed.State != archive.SegmentSealed || sealed.EncodedBytes != len(sealedBlock.RawData()) ||
		sealed.Rows != 2 || sealed.Refs != 2 {
		t.Errorf("sealed sample = %#v, stored canonical bytes = %d", sealed, len(sealedBlock.RawData()))
	}
	if open.State != archive.SegmentOpen || open.EncodedBytes != len(openBlock.RawData()) ||
		open.Rows != 0 || open.Refs != 0 {
		t.Errorf("open sample = %#v, stored canonical bytes = %d", open, len(openBlock.RawData()))
	}

	// The next batch lands in the freshly opened window 6.
	hs.apply([]archive.RefRow{hs.row(48, 480)}, 48)
	wantBlobs(t, hs.lookup(48), "open slot 48", 480)
	wantBlobs(t, hs.lookup(41), "sealed slot 41 after the next batch", 410)
}

// TestSealPartialWindowStaysOpen: coverage that stops mid-window seals nothing.
func TestSealPartialWindowStaysOpen(t *testing.T) {
	hs := newHarness(t, testParams())
	hs.apply([]archive.RefRow{hs.row(41, 410)}, 46) // window 5 runs to 47

	if got := hs.h.Info().DirDepth; got != 0 {
		t.Errorf("dir_depth = %d with no window fully covered, want 0", got)
	}
	wantBlobs(t, hs.lookup(41), "open slot 41", 410)
}

func TestSealReusesCommittedOpenBlockAndMeasurement(t *testing.T) {
	hs := newHarness(t, testParams())
	openApply := hs.apply([]archive.RefRow{hs.row(41, 410)}, 46)
	before, err := hs.h.Enumerate(hs.ctx)
	if err != nil {
		t.Fatalf("Enumerate before seal: %v", err)
	}
	if len(openApply.Index.Segments) != 1 {
		t.Fatalf("open apply samples = %d, want 1", len(openApply.Index.Segments))
	}

	sealApply := hs.apply(nil, 47)
	after, err := hs.h.Enumerate(hs.ctx)
	if err != nil {
		t.Fatalf("Enumerate after seal: %v", err)
	}
	if len(after.Sealed) != 1 || !after.Sealed[0].CID.Equals(before.Open) {
		t.Fatalf("sealed Segment = %v, want prior open CID %s", after.Sealed, before.Open)
	}
	if len(sealApply.Index.Segments) != 2 {
		t.Fatalf("seal apply samples = %d, want sealed+open", len(sealApply.Index.Segments))
	}
	if got, want := sealApply.Index.Segments[0], openApply.Index.Segments[0]; got.State != archive.SegmentSealed ||
		got.EncodedBytes != want.EncodedBytes || got.Rows != want.Rows || got.Refs != want.Refs {
		t.Fatalf("sealed sample %#v does not preserve prior open measurement %#v", got, want)
	}
}

// TestEmptyWindowSealsToNull: a window with no rows becomes a null directory
// entry and no object at all (spec 3.2), yet its slots still read as covered.
func TestEmptyWindowSealsToNull(t *testing.T) {
	hs := newHarness(t, testParams())

	// Window 5 (40..47) has a row; window 6 (48..55) has none; window 7 does.
	hs.apply([]archive.RefRow{hs.row(41, 410), hs.row(57, 570)}, 63)

	if got, want := hs.h.Info().DirDepth, canonicalDepth(3, testFanoutBits); got != want {
		t.Errorf("dir_depth = %d after three seals, want %d", got, want)
	}
	wantBlobs(t, hs.lookup(41), "window 5", 410)
	wantBlobs(t, hs.lookup(57), "window 7", 570)
	for slot := uint64(48); slot <= 55; slot++ {
		wantBlobs(t, hs.lookup(slot), "slot in the empty window 6")
	}
	// The null entry is a fact, not an absence of one: it holds index 1 so that
	// window 7 is still addressable at index 2.
	wantStatus(t, hs.lookupVHs(50, 500), archive.StatusAbsent, "filtered lookup in the empty window")
}

func TestLatestSealedSegmentSampleSkipsLongNullRun(t *testing.T) {
	hs := newHarness(t, testParams())
	const sealedWindows = 40
	hs.apply([]archive.RefRow{hs.row(testOrigin+1, 410)}, testOrigin+sealedWindows*(1<<testSegBits)-1)

	sample, found, err := hs.h.LatestSealedSegmentSample(hs.ctx)
	if err != nil {
		t.Fatalf("LatestSealedSegmentSample: %v", err)
	}
	if !found {
		t.Fatal("directory-aware reverse search missed a Segment behind 39 null windows")
	}
	enumeration, err := hs.h.Enumerate(hs.ctx)
	if err != nil {
		t.Fatalf("Enumerate: %v", err)
	}
	if len(enumeration.Sealed) != 1 {
		t.Fatalf("sealed Segment count = %d, want 1", len(enumeration.Sealed))
	}
	block, err := hs.bs.Get(hs.ctx, enumeration.Sealed[0].CID)
	if err != nil {
		t.Fatalf("reading latest sealed Segment: %v", err)
	}
	if sample.State != archive.SegmentSealed || sample.EncodedBytes != len(block.RawData()) ||
		sample.Rows != 1 || sample.Refs != 1 {
		t.Fatalf("latest sealed sample = %#v, stored canonical bytes = %d", sample, len(block.RawData()))
	}
}

// TestCoverageOnlyAdvance: rows may be empty; coverage still advances and the
// slots it passes become provably blobless (spec 5.1 step 1).
func TestCoverageOnlyAdvance(t *testing.T) {
	hs := newHarness(t, testParams())

	res := hs.apply(nil, 60)
	if res.NoOp {
		t.Errorf("a coverage-only advance reported NoOp")
	}
	if res.SyncedTo != 60 {
		t.Errorf("SyncedTo = %d, want 60", res.SyncedTo)
	}
	for slot := uint64(40); slot <= 60; slot++ {
		wantBlobs(t, hs.lookup(slot), "covered blobless slot")
	}
	wantStatus(t, hs.lookup(61), archive.StatusNotYetCovered, "slot past synced_to")
}

// TestDirDepthGrowsAtCapacityBoundaries: depth increases when, and only when,
// an append lands exactly on the current capacity (spec 5.3). At fanout 4 that
// is after 4 segments, then after 16.
func TestDirDepthGrowsAtCapacityBoundaries(t *testing.T) {
	hs := newHarness(t, testParams())

	for sealed := uint64(1); sealed <= 20; sealed++ {
		// Cover exactly one more window: window (5 + sealed - 1) ends at
		// origin + sealed*8 - 1.
		syncedTo := testOrigin + sealed*8 - 1
		slot := syncedTo - 1
		hs.apply([]archive.RefRow{hs.row(slot, slot*10)}, syncedTo)

		want := canonicalDepth(sealed, testFanoutBits)
		if got := hs.h.Info().DirDepth; got != want {
			t.Fatalf("after %d seals: dir_depth = %d, want %d", sealed, got, want)
		}
		wantBlobs(t, hs.lookup(slot), "the row just sealed", slot*10)
	}

	// Everything sealed along the way is still there, at every depth the tree
	// passed through.
	for sealed := uint64(1); sealed <= 20; sealed++ {
		slot := testOrigin + sealed*8 - 2
		wantBlobs(t, hs.lookup(slot), "row sealed early, read at depth 3", slot*10)
	}
}

// TestUnalignedOrigin: origin_slot need not start a window. dir_base floors to
// the window containing it, and the slots below origin inside that window are
// before-origin, not absent.
func TestUnalignedOrigin(t *testing.T) {
	params := testParams()
	params.OriginSlot = 43 // window 5 is 40..47
	hs := newHarness(t, params)

	hs.apply([]archive.RefRow{hs.row(44, 440), hs.row(49, 490)}, 49)

	wantBlobs(t, hs.lookup(44), "slot 44", 440)
	wantBlobs(t, hs.lookup(49), "slot 49", 490)
	for slot := uint64(40); slot < 43; slot++ {
		wantStatus(t, hs.lookup(slot), archive.StatusBeforeOrigin, "slot inside the origin window but below origin")
	}
	// Window 5 was fully covered by synced_to 49 and so was sealed.
	if got := hs.h.Info().DirDepth; got != 1 {
		t.Errorf("dir_depth = %d, want 1", got)
	}
}

func TestApplyValidation(t *testing.T) {
	tests := []struct {
		name     string
		rows     func(hs *harness) []archive.RefRow
		syncedTo uint64
	}{
		{
			name:     "rows not ascending",
			rows:     func(hs *harness) []archive.RefRow { return []archive.RefRow{hs.row(43, 1), hs.row(41, 2)} },
			syncedTo: 44,
		},
		{
			name:     "duplicate slots",
			rows:     func(hs *harness) []archive.RefRow { return []archive.RefRow{hs.row(43, 1), hs.row(43, 2)} },
			syncedTo: 44,
		},
		{
			name:     "row before origin",
			rows:     func(hs *harness) []archive.RefRow { return []archive.RefRow{hs.row(39, 1)} },
			syncedTo: 44,
		},
		{
			name:     "row past synced_to",
			rows:     func(hs *harness) []archive.RefRow { return []archive.RefRow{hs.row(45, 1)} },
			syncedTo: 44,
		},
		{
			name:     "row with no versioned hashes",
			rows:     func(hs *harness) []archive.RefRow { return []archive.RefRow{{Slot: 41}} },
			syncedTo: 44,
		},
		{
			name:     "synced_to before origin",
			rows:     func(hs *harness) []archive.RefRow { return nil },
			syncedTo: 39,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hs := newHarness(t, testParams())
			before := hs.h.Root()
			err := hs.applyErr(tt.rows(hs), tt.syncedTo)
			wantConflict(t, err, tt.name)
			if hs.h.Root() != before {
				t.Errorf("a rejected batch changed the root")
			}
			if _, covered := hs.h.SyncedTo(); covered {
				t.Errorf("a rejected batch advanced coverage")
			}
		})
	}
}

// TestOversizedRowRejected: a refs row naming more blobs than the protocol
// ceiling is refused at apply as a conflict, before any block is written, so no
// stored row can exceed it and an unfiltered read of the slot is bounded by
// construction. A row exactly at the ceiling is still accepted.
func TestOversizedRowRejected(t *testing.T) {
	hs := newHarness(t, testParams())
	before := hs.h.Root()

	over := make([]uint64, schema.MaxBlobsPerSlotCeiling+1)
	for i := range over {
		over[i] = uint64(i + 1)
	}
	err := hs.applyErr([]archive.RefRow{hs.row(41, over...)}, 44)
	ce := wantConflict(t, err, "oversized row")
	if !strings.Contains(ce.Error(), "ceiling") {
		t.Errorf("error %v, want it to name the per-slot ceiling", ce)
	}
	if hs.h.Root() != before {
		t.Errorf("a rejected oversized batch changed the root")
	}
	if _, covered := hs.h.SyncedTo(); covered {
		t.Errorf("a rejected oversized batch advanced coverage")
	}

	// Exactly at the ceiling is legal.
	atCeiling := make([]uint64, schema.MaxBlobsPerSlotCeiling)
	for i := range atCeiling {
		atCeiling[i] = uint64(i + 1)
	}
	hs.apply([]archive.RefRow{hs.row(41, atCeiling...)}, 44)
}

// TestMissingBlobs: a vh that the catalog does not know, and a vh whose block
// GC took, are both refused, and the error names every one of them at once
// (spec 5.1 step 4).
func TestMissingBlobs(t *testing.T) {
	hs := newHarness(t, testParams())

	known := mkVH(1)
	hs.cat.add(t, known)
	unknown := mkVH(2)   // never registered
	collected := mkVH(3) // catalog entry outlived its block
	hs.cat.addCatalogOnly(t, collected)

	rows := []archive.RefRow{
		{Slot: 41, VHs: []schema.VersionedHash{known, unknown}},
		{Slot: 42, VHs: []schema.VersionedHash{collected}},
	}
	err := hs.applyErr(rows, 44)
	wantConflict(t, err, "batch with missing blobs")

	var missing *archive.MissingBlobsError
	if !errors.As(err, &missing) {
		t.Fatalf("error %v carries no *archive.MissingBlobsError", err)
	}
	want := []schema.VersionedHash{unknown, collected}
	if len(missing.VHs) != len(want) {
		t.Fatalf("MissingBlobsError lists %d vhs, want %d: %v", len(missing.VHs), len(want), missing.VHs)
	}
	for i := range want {
		if missing.VHs[i] != want[i] {
			t.Errorf("missing vh %d = 0x%x, want 0x%x", i, missing.VHs[i][:], want[i][:])
		}
	}
	if _, covered := hs.h.SyncedTo(); covered {
		t.Errorf("a batch with missing blobs advanced coverage")
	}
}

// TestResolverError: an I/O failure in the catalog is not a conflict. The batch
// is not wrong, the archive is broken, and a 409 would tell the writer to fix
// something that is not its fault.
func TestResolverError(t *testing.T) {
	hs := newHarness(t, testParams())
	row := hs.row(41, 410)

	boom := errors.New("catalog is on fire")
	hs.cat.err = boom

	err := hs.applyErr([]archive.RefRow{row}, 44)
	if !errors.Is(err, boom) {
		t.Errorf("error %v does not wrap the resolver's error", err)
	}
	var ce *archive.ConflictError
	if errors.As(err, &ce) {
		t.Errorf("a resolver I/O failure was reported as a conflict: %v", err)
	}
}

// TestIdempotentReplay: replaying a batch that is entirely at or before
// synced_to verifies against what is stored and changes nothing (spec 5.1 step
// 2).
func TestIdempotentReplay(t *testing.T) {
	hs := newHarness(t, testParams())
	rows := []archive.RefRow{hs.row(41, 410), hs.row(43, 430, 431)}
	first := hs.apply(rows, 44)

	replay := hs.apply(rows, 44)
	if !replay.NoOp {
		t.Errorf("replaying the same batch reported NoOp = false")
	}
	if replay.Root != first.Root {
		t.Errorf("replay root %s, want the unchanged %s", replay.Root, first.Root)
	}
	if replay.SyncedTo != 44 {
		t.Errorf("replay SyncedTo = %d, want 44", replay.SyncedTo)
	}

	// A replay of part of the batch, and of a lower synced_to, is still a
	// replay: it claims nothing new.
	partial := hs.apply([]archive.RefRow{rows[0]}, 42)
	if !partial.NoOp || partial.Root != first.Root {
		t.Errorf("partial replay: NoOp = %t root = %s, want true and %s", partial.NoOp, partial.Root, first.Root)
	}
}

// TestReplayAcrossSeal: a replayed row that now lives in a sealed segment is
// verified through the directory just the same.
func TestReplayAcrossSeal(t *testing.T) {
	hs := newHarness(t, testParams())
	batch := []archive.RefRow{hs.row(41, 410)}
	hs.apply(batch, 47) // seals window 5
	hs.apply([]archive.RefRow{hs.row(49, 490)}, 55)
	root := hs.h.Root()

	replay := hs.apply(batch, 47)
	if !replay.NoOp {
		t.Errorf("replaying a sealed batch reported NoOp = false")
	}
	if hs.h.Root() != root {
		t.Errorf("replaying a sealed batch changed the root")
	}
}

func TestReplayMismatch(t *testing.T) {
	tests := []struct {
		name  string
		rows  func(hs *harness) []archive.RefRow
		slot  uint64
		descr string
	}{
		{
			name: "different vh",
			rows: func(hs *harness) []archive.RefRow { return []archive.RefRow{hs.row(41, 999)} },
		},
		{
			name: "reordered vhs",
			rows: func(hs *harness) []archive.RefRow { return []archive.RefRow{hs.row(43, 431, 430)} },
		},
		{
			name: "extra vh",
			rows: func(hs *harness) []archive.RefRow { return []archive.RefRow{hs.row(41, 410, 411)} },
		},
		{
			name: "missing vh",
			rows: func(hs *harness) []archive.RefRow { return []archive.RefRow{hs.row(43, 430)} },
		},
		{
			name: "row at a slot that stored none",
			rows: func(hs *harness) []archive.RefRow { return []archive.RefRow{hs.row(42, 420)} },
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hs := newHarness(t, testParams())
			hs.apply([]archive.RefRow{hs.row(41, 410), hs.row(43, 430, 431)}, 44)
			root := hs.h.Root()

			err := hs.applyErr(tt.rows(hs), 44)
			wantConflict(t, err, tt.name)
			if hs.h.Root() != root {
				t.Errorf("a rejected replay changed the root")
			}
		})
	}
}

// TestPartialOverlapRejected: a batch that both replays covered ground and
// advances coverage is refused outright (spec 5.1 step 3). Sealed segments are
// immutable; a writer asking to edit one has lost track of its progress.
func TestPartialOverlapRejected(t *testing.T) {
	hs := newHarness(t, testParams())
	hs.apply([]archive.RefRow{hs.row(41, 410)}, 44)
	root := hs.h.Root()

	// Slot 41 is already covered; slot 45 is new.
	err := hs.applyErr([]archive.RefRow{hs.row(41, 410), hs.row(45, 450)}, 46)
	wantConflict(t, err, "partial overlap")
	if hs.h.Root() != root {
		t.Errorf("a rejected overlap changed the root")
	}
	if synced, _ := hs.h.SyncedTo(); synced != 44 {
		t.Errorf("synced_to = %d after a rejected overlap, want 44", synced)
	}
}

// TestStructuralSharingAcrossApplies: extending a head rewrites only the spine.
// Every sealed segment keeps its CID and its block, which is what makes a
// follower's incremental sync cheap (spec 13.3).
func TestStructuralSharingAcrossApplies(t *testing.T) {
	hs := newHarness(t, testParams())
	hs.apply([]archive.RefRow{hs.row(41, 410)}, 47) // seal window 5
	hs.apply([]archive.RefRow{hs.row(49, 490)}, 55) // seal window 6

	before := blockCIDs(t, hs.bs)

	hs.apply([]archive.RefRow{hs.row(57, 570)}, 63) // seal window 7

	after := blockCIDs(t, hs.bs)
	for c := range before {
		if !after[c] {
			t.Errorf("block %s disappeared: a commit rewrote history in place", c)
		}
	}
	// The rows sealed before this batch are still readable, by definition
	// through their original blocks.
	wantBlobs(t, hs.lookup(41), "window 5 after two more seals", 410)
	wantBlobs(t, hs.lookup(49), "window 6 after one more seal", 490)
}

// TestReadOnlyHeadRejectsApply: a head configured without a resolver can serve
// but not mutate. That is a programming error, not a conflict.
func TestReadOnlyHeadRejectsApply(t *testing.T) {
	bs := newBlockstore()
	cat := newFakeCatalog(bs)
	hs := newHarnessOver(t, testParams(), bs, cat)
	hs.apply([]archive.RefRow{hs.row(41, 410)}, 44)

	ctx := context.Background()
	ro, err := archive.Load(ctx, archive.Config{Blocks: bs}, hs.h.Root())
	if err != nil {
		t.Fatalf("archive.Load: %v", err)
	}
	wantBlobs(t, mustLookup(t, ro, 41), "read-only head serves", 410)

	if _, err := ro.ApplyRefs(ctx, []archive.RefRow{{Slot: 45, VHs: []schema.VersionedHash{mkVH(450)}}}, 45); err == nil {
		t.Errorf("a head with no resolver accepted a batch")
	}
}

func mustLookup(t *testing.T, h *archive.Head, slot uint64) archive.Result {
	t.Helper()
	res, err := h.Lookup(context.Background(), slot)
	if err != nil {
		t.Fatalf("Lookup(%d): %v", slot, err)
	}
	return res
}
