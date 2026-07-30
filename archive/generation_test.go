package archive_test

import (
	"context"
	"errors"
	"testing"

	"github.com/ipfs/go-cid"

	"github.com/blobarchive/bloar/archive"
	"github.com/blobarchive/bloar/schema"
)

func TestBuildGenerationEmptyRowsCoversRange(t *testing.T) {
	ctx := context.Background()
	bs := newBlockstore()
	params := testParams()

	h, err := archive.BuildGeneration(ctx, archive.Config{
		Blocks:   bs,
		Resolver: newFakeCatalog(bs),
	}, params, nil, params.OriginSlot+17)
	if err != nil {
		t.Fatalf("BuildGeneration: %v", err)
	}

	if got, covered := h.SyncedTo(); !covered || got != params.OriginSlot+17 {
		t.Fatalf("SyncedTo = (%d, %t), want (%d, true)", got, covered, params.OriginSlot+17)
	}
	for _, slot := range []uint64{params.OriginSlot, params.OriginSlot + 9, params.OriginSlot + 17} {
		res, err := h.Lookup(ctx, slot)
		if err != nil {
			t.Fatalf("Lookup(%d): %v", slot, err)
		}
		wantStatus(t, res, archive.StatusFound, "covered empty slot")
		if len(res.Entries) != 0 {
			t.Errorf("Lookup(%d) returned %d entries, want none", slot, len(res.Entries))
		}
	}

	before, err := h.Lookup(ctx, params.OriginSlot-1)
	if err != nil {
		t.Fatalf("Lookup before origin: %v", err)
	}
	wantStatus(t, before, archive.StatusBeforeOrigin, "slot before generation")
	after, err := h.Lookup(ctx, params.OriginSlot+18)
	if err != nil {
		t.Fatalf("Lookup after coverage: %v", err)
	}
	wantStatus(t, after, archive.StatusNotYetCovered, "slot after generation")
}

func TestBuildGenerationMovingOriginBoundsLookupAndEnumeration(t *testing.T) {
	ctx := context.Background()
	bs := newBlockstore()
	cat := newFakeCatalog(bs)
	cfg := archive.Config{Blocks: bs, Resolver: cat}

	oldParams := testParams()
	oldVH := mkVH(1)
	cat.add(t, oldVH)
	old, err := archive.BuildGeneration(ctx, cfg, oldParams, []archive.RefRow{
		{Slot: oldParams.OriginSlot + 2, VHs: []schema.VersionedHash{oldVH}},
	}, oldParams.OriginSlot+23)
	if err != nil {
		t.Fatalf("building old generation: %v", err)
	}
	oldRoot := old.Root()

	newParams := oldParams
	newParams.OriginSlot += 16
	newVH := mkVH(2)
	laterVH := mkVH(3)
	cat.add(t, newVH)
	cat.add(t, laterVH)
	newHead, err := archive.BuildGeneration(ctx, cfg, newParams, []archive.RefRow{
		{Slot: newParams.OriginSlot + 1, VHs: []schema.VersionedHash{newVH}},
		{Slot: newParams.OriginSlot + 9, VHs: []schema.VersionedHash{laterVH}},
	}, newParams.OriginSlot+16)
	if err != nil {
		t.Fatalf("building moved generation: %v", err)
	}

	if got := old.Root(); got != oldRoot {
		t.Fatalf("building moved generation changed old root: got %s, want %s", got, oldRoot)
	}
	oldResult, err := old.Lookup(ctx, oldParams.OriginSlot+2)
	if err != nil {
		t.Fatalf("old Lookup: %v", err)
	}
	wantBlobs(t, oldResult, "old generation remains readable", 1)

	before, err := newHead.Lookup(ctx, newParams.OriginSlot-1)
	if err != nil {
		t.Fatalf("new Lookup before origin: %v", err)
	}
	wantStatus(t, before, archive.StatusBeforeOrigin, "moved origin lower bound")
	atOrigin, err := newHead.Lookup(ctx, newParams.OriginSlot)
	if err != nil {
		t.Fatalf("new Lookup at origin: %v", err)
	}
	wantBlobs(t, atOrigin, "empty covered origin")
	withBlob, err := newHead.Lookup(ctx, newParams.OriginSlot+1)
	if err != nil {
		t.Fatalf("new Lookup blob: %v", err)
	}
	wantBlobs(t, withBlob, "new generation blob", 2)

	enum, err := newHead.Enumerate(ctx)
	if err != nil {
		t.Fatalf("Enumerate: %v", err)
	}
	if enum.Params != newParams {
		t.Errorf("Enumerate params = %+v, want %+v", enum.Params, newParams)
	}
	if !enum.Covered || enum.SyncedTo != newParams.OriginSlot+16 {
		t.Errorf("Enumerate coverage = (%d, %t), want (%d, true)", enum.SyncedTo, enum.Covered, newParams.OriginSlot+16)
	}
	wantOpenOrd := (newParams.OriginSlot + 17) >> newParams.SegBits
	if enum.OpenOrd != wantOpenOrd {
		t.Errorf("Enumerate OpenOrd = %d, want %d", enum.OpenOrd, wantOpenOrd)
	}
	if len(enum.Sealed) != 2 {
		t.Fatalf("Enumerate returned %d sealed segments, want 2: %+v", len(enum.Sealed), enum.Sealed)
	}
	windowWidth := uint64(1) << newParams.SegBits
	for i, seg := range enum.Sealed {
		wantFirst := newParams.OriginSlot + uint64(i)*windowWidth
		if seg.FirstSlot != wantFirst || seg.LastSlot != wantFirst+windowWidth-1 {
			t.Errorf("sealed segment %d bounds = [%d, %d], want [%d, %d]", i, seg.FirstSlot, seg.LastSlot, wantFirst, wantFirst+windowWidth-1)
		}
	}
}

func TestBuildGenerationMissingBlobDoesNotMutateExistingHead(t *testing.T) {
	ctx := context.Background()
	bs := newBlockstore()
	cat := newFakeCatalog(bs)
	cfg := archive.Config{Blocks: bs, Resolver: cat}
	params := testParams()

	keptVH := mkVH(10)
	cat.add(t, keptVH)
	existing, err := archive.BuildGeneration(ctx, cfg, params, []archive.RefRow{
		{Slot: params.OriginSlot, VHs: []schema.VersionedHash{keptVH}},
	}, params.OriginSlot+7)
	if err != nil {
		t.Fatalf("building existing generation: %v", err)
	}
	existingRoot := existing.Root()

	missingVH := mkVH(11)
	cat.addCatalogOnly(t, missingVH)
	failed, err := archive.BuildGeneration(ctx, cfg, params, []archive.RefRow{
		{Slot: params.OriginSlot + 1, VHs: []schema.VersionedHash{missingVH}},
	}, params.OriginSlot+7)
	if err == nil {
		t.Fatalf("BuildGeneration with missing blob returned (%v, nil), want error", failed)
	}
	if failed != nil {
		t.Errorf("failed BuildGeneration returned a head rooted at %s", failed.Root())
	}
	var missing *archive.MissingBlobsError
	if !errors.As(err, &missing) {
		t.Fatalf("BuildGeneration error %v does not contain MissingBlobsError", err)
	}
	if got := existing.Root(); got != existingRoot {
		t.Fatalf("failed build changed existing root: got %s, want %s", got, existingRoot)
	}
	res, lookupErr := existing.Lookup(ctx, params.OriginSlot)
	if lookupErr != nil {
		t.Fatalf("existing Lookup after failed build: %v", lookupErr)
	}
	wantBlobs(t, res, "existing generation after failed build", 10)
}

func TestBuildGenerationDeterministicRoot(t *testing.T) {
	ctx := context.Background()
	params := testParams()

	build := func(t *testing.T) cid.Cid {
		t.Helper()
		bs := newBlockstore()
		cat := newFakeCatalog(bs)
		vhs := []schema.VersionedHash{mkVH(20), mkVH(21), mkVH(22)}
		for _, vh := range vhs {
			cat.add(t, vh)
		}
		h, err := archive.BuildGeneration(ctx, archive.Config{Blocks: bs, Resolver: cat}, params, []archive.RefRow{
			{Slot: params.OriginSlot + 1, VHs: []schema.VersionedHash{vhs[0], vhs[1]}},
			{Slot: params.OriginSlot + 16, VHs: []schema.VersionedHash{vhs[2]}},
		}, params.OriginSlot+23)
		if err != nil {
			t.Fatalf("BuildGeneration: %v", err)
		}
		return h.Root()
	}

	first := build(t)
	second := build(t)
	if first != second {
		t.Fatalf("same generation produced roots %s and %s", first, second)
	}
}
