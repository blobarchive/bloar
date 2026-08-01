package follow_test

import (
	"errors"
	"reflect"
	"testing"

	"github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"

	"github.com/blobarchive/bloar/archive"
	"github.com/blobarchive/bloar/follow"
)

func TestFinalizedClaimArbitrationEquivalentSourcesAreDeterministic(t *testing.T) {
	params := archive.Params{Name: "all", Net: "arbitration-net", OriginSlot: 96, SegBits: 3, FanoutBits: 2}
	left := buildEquivalenceWriter(t, params,
		equivalenceStep{syncedTo: 103, rows: []equivalenceRow{{slot: 98, ids: []uint64{1}}}})
	right := buildEquivalenceWriter(t, params,
		equivalenceStep{syncedTo: 99, rows: []equivalenceRow{{slot: 98, ids: []uint64{1}}}},
		equivalenceStep{syncedTo: 103})
	if left.head.Root() != right.head.Root() {
		t.Fatalf("equivalent writer roots differ: %s != %s", left.head.Root(), right.head.Root())
	}

	archiveID := equivalenceArchiveID(70)
	leftDoc := signEquivalenceClaim(t, equivalenceKey(t), archiveID, left.head.Info(), cid.Undef, 900, "2099-01-01T00:00:00Z")
	rightDoc := signEquivalenceClaim(t, equivalenceKey(t), archiveID, right.head.Info(), cid.Undef, 1, "1990-01-01T00:00:00Z")
	blocks := copyEquivalenceStores(t, left.blocks, right.blocks)

	forward := []follow.FinalizedClaimCandidate{
		{SourceID: "writer-z", Document: leftDoc},
		{SourceID: "writer-a", Document: rightDoc},
	}
	reverse := []follow.FinalizedClaimCandidate{forward[1], forward[0]}
	for _, candidates := range [][]follow.FinalizedClaimCandidate{forward, reverse} {
		selection, err := follow.SelectFinalizedClaim(t.Context(), blocks, "all", candidates)
		if err != nil {
			t.Fatalf("selecting equivalent claims: %v", err)
		}
		if selection.Representative.SourceID != "writer-a" {
			t.Fatalf("representative = %q, want stable writer-a", selection.Representative.SourceID)
		}
		got := []string{selection.Equivalent[0].SourceID, selection.Equivalent[1].SourceID}
		if want := []string{"writer-a", "writer-z"}; !reflect.DeepEqual(got, want) {
			t.Fatalf("equivalent sources = %v, want %v", got, want)
		}
	}
}

func TestFinalizedClaimArbitrationUsesProofNotMajorityOrFreshness(t *testing.T) {
	params := archive.Params{Name: "all", Net: "arbitration-net", OriginSlot: 96, SegBits: 3, FanoutBits: 2}
	prefix := equivalenceStep{syncedTo: 103, rows: []equivalenceRow{{slot: 98, ids: []uint64{1}}}}
	lower := buildEquivalenceWriter(t, params, prefix)
	higher := buildEquivalenceWriter(t, params, prefix,
		equivalenceStep{syncedTo: 119, rows: []equivalenceRow{{slot: 107, ids: []uint64{2}}}})
	archiveID := equivalenceArchiveID(71)

	// Two lagging authorities advertise much newer wall clocks and signer-local
	// revisions. The sole advancing authority still wins by prefix proof.
	oldA := signEquivalenceClaim(t, equivalenceKey(t), archiveID, lower.head.Info(), cid.Undef, 9_000, "2099-01-01T00:00:00Z")
	oldB := signEquivalenceClaim(t, equivalenceKey(t), archiveID, lower.head.Info(), cid.Undef, 8_000, "2098-01-01T00:00:00Z")
	advanced := signEquivalenceClaim(t, equivalenceKey(t), archiveID, higher.head.Info(), cid.Undef, 1, "1990-01-01T00:00:00Z")
	candidates := []follow.FinalizedClaimCandidate{
		{SourceID: "lagging-a", Document: oldA},
		{SourceID: "lagging-b", Document: oldB},
		{SourceID: "advanced", Document: advanced},
	}
	blocks := copyEquivalenceStores(t, lower.blocks, higher.blocks)

	for _, order := range arbitrationPermutations(candidates) {
		selection, err := follow.SelectFinalizedClaim(t.Context(), blocks, "all", order)
		if err != nil {
			t.Fatalf("selecting ordered claims: %v", err)
		}
		if selection.Representative.SourceID != "advanced" || len(selection.Equivalent) != 1 {
			t.Fatalf("selection = %+v, want sole advanced claim", selection)
		}
	}
}

func TestFinalizedClaimArbitrationAllowsDominatorAboveIncomparableObservations(t *testing.T) {
	params := archive.Params{Name: "all", Net: "arbitration-net", OriginSlot: 96, SegBits: 3, FanoutBits: 2}
	prefix := equivalenceStep{syncedTo: 103, rows: []equivalenceRow{{slot: 98, ids: []uint64{1}}}}
	lower := buildEquivalenceWriter(t, params, prefix)
	higher := buildEquivalenceWriter(t, params, prefix,
		equivalenceStep{syncedTo: 119, rows: []equivalenceRow{{slot: 107, ids: []uint64{2}}}})
	genesis := putEquivalenceManifest(t, lower.blocks, cid.Undef, 10)
	if higherGenesis := putEquivalenceManifest(t, higher.blocks, cid.Undef, 10); higherGenesis != genesis {
		t.Fatalf("independent manifest genesis differs: %s != %s", higherGenesis, genesis)
	}
	tip := putEquivalenceManifest(t, higher.blocks, genesis, 20)
	archiveID := equivalenceArchiveID(72)

	// A has more root coverage while B has more manifest history, so neither
	// includes the other. C combines both advances and provably dominates each.
	candidates := []follow.FinalizedClaimCandidate{
		{SourceID: "root-ahead", Document: signEquivalenceClaim(t, equivalenceKey(t), archiveID, higher.head.Info(), genesis, 1, "2026-01-01T00:00:00Z")},
		{SourceID: "manifest-ahead", Document: signEquivalenceClaim(t, equivalenceKey(t), archiveID, lower.head.Info(), tip, 1, "2026-01-01T00:00:00Z")},
		{SourceID: "combined", Document: signEquivalenceClaim(t, equivalenceKey(t), archiveID, higher.head.Info(), tip, 1, "2026-01-01T00:00:00Z")},
	}
	blocks := copyEquivalenceStores(t, lower.blocks, higher.blocks)
	selection, err := follow.SelectFinalizedClaim(t.Context(), blocks, "all", candidates)
	if err != nil {
		t.Fatalf("selecting common dominator: %v", err)
	}
	if selection.Representative.SourceID != "combined" {
		t.Fatalf("representative = %q, want combined", selection.Representative.SourceID)
	}

	_, err = follow.SelectFinalizedClaim(t.Context(), blocks, "all", candidates[:2])
	var incomparable *follow.FinalizedClaimsIncomparableError
	if !errors.As(err, &incomparable) {
		t.Fatalf("two crossing observations error = %T (%v), want incomparable", err, err)
	}
	if len(incomparable.Incomparable) != 1 || incomparable.Incomparable[0].Relation != follow.ClaimsIncomparable {
		t.Fatalf("incomparable evidence = %+v", incomparable.Incomparable)
	}
	var conflict *follow.FinalizedClaimConflictError
	if errors.As(err, &conflict) {
		t.Fatalf("temporary incomparability was reported as durable conflict: %v", err)
	}
}

func TestFinalizedClaimArbitrationReturnsSourceAttributedConflict(t *testing.T) {
	params := archive.Params{Name: "all", Net: "arbitration-net", OriginSlot: 96, SegBits: 3, FanoutBits: 2}
	left := buildEquivalenceWriter(t, params,
		equivalenceStep{syncedTo: 111, rows: []equivalenceRow{{slot: 98, ids: []uint64{1}}}})
	right := buildEquivalenceWriter(t, params,
		equivalenceStep{syncedTo: 111, rows: []equivalenceRow{{slot: 98, ids: []uint64{99}}}})
	archiveID := equivalenceArchiveID(73)
	candidates := []follow.FinalizedClaimCandidate{
		{SourceID: "writer-z", Document: signEquivalenceClaim(t, equivalenceKey(t), archiveID, right.head.Info(), cid.Undef, 1, "2026-01-01T00:00:00Z")},
		{SourceID: "writer-a", Document: signEquivalenceClaim(t, equivalenceKey(t), archiveID, left.head.Info(), cid.Undef, 1, "2026-01-01T00:00:00Z")},
	}
	blocks := copyEquivalenceStores(t, left.blocks, right.blocks)

	for _, order := range [][]follow.FinalizedClaimCandidate{candidates, {candidates[1], candidates[0]}} {
		_, err := follow.SelectFinalizedClaim(t.Context(), blocks, "all", order)
		var conflict *follow.FinalizedClaimConflictError
		if !errors.As(err, &conflict) {
			t.Fatalf("conflicting observations error = %T (%v), want source conflict", err, err)
		}
		if len(conflict.Conflicts) != 1 {
			t.Fatalf("conflict evidence count = %d, want 1", len(conflict.Conflicts))
		}
		evidence := conflict.Conflicts[0]
		if evidence.Left.SourceID != "writer-a" || evidence.Right.SourceID != "writer-z" ||
			evidence.Relation != follow.ClaimRelationInvalid || evidence.Conflict == nil {
			t.Fatalf("conflict evidence = %+v", evidence)
		}
		var archiveConflict *follow.ArchiveConflictError
		if !errors.As(err, &archiveConflict) || archiveConflict != evidence.Conflict {
			t.Fatalf("underlying archive conflict = %p (%v), evidence = %p", archiveConflict, archiveConflict, evidence.Conflict)
		}
	}
}

func TestFinalizedClaimArbitrationKeepsClassifierFailuresOrdinary(t *testing.T) {
	params := archive.Params{Name: "all", Net: "arbitration-net", OriginSlot: 96, SegBits: 3, FanoutBits: 2}
	writer := buildEquivalenceWriter(t, params,
		equivalenceStep{syncedTo: 103, rows: []equivalenceRow{{slot: 98, ids: []uint64{1}}}})
	doc := signEquivalenceClaim(t, equivalenceKey(t), equivalenceArchiveID(74), writer.head.Info(), cid.Undef, 1, "2026-01-01T00:00:00Z")
	missing := newEquivalenceBlockstore()

	_, err := follow.SelectFinalizedClaim(t.Context(), missing, "all", []follow.FinalizedClaimCandidate{{
		SourceID: "writer-a", Document: doc,
	}})
	var evaluation *follow.FinalizedClaimEvaluationError
	if !errors.As(err, &evaluation) {
		t.Fatalf("missing proof error = %T (%v), want evaluation failure", err, err)
	}
	if len(evaluation.Failures) != 1 || evaluation.Failures[0].Left.SourceID != "writer-a" ||
		evaluation.Failures[0].Right.SourceID != "writer-a" {
		t.Fatalf("evaluation evidence = %+v", evaluation.Failures)
	}
	if !errors.Is(err, format.ErrNotFound{}) {
		t.Fatalf("evaluation error %v does not preserve block-not-found cause", err)
	}
	var conflict *follow.FinalizedClaimConflictError
	if errors.As(err, &conflict) {
		t.Fatalf("ordinary proof failure was reported as conflict: %v", err)
	}
}

func TestFinalizedClaimArbitrationOmitsUnavailableRootAndRetries(t *testing.T) {
	params := archive.Params{Name: "all", Net: "arbitration-net", OriginSlot: 96, SegBits: 3, FanoutBits: 2}
	healthy := buildEquivalenceWriter(t, params,
		equivalenceStep{syncedTo: 111, rows: []equivalenceRow{{slot: 98, ids: []uint64{1}}}})
	unavailable := buildEquivalenceWriter(t, params,
		equivalenceStep{syncedTo: 119, rows: []equivalenceRow{{slot: 98, ids: []uint64{1}}, {slot: 115, ids: []uint64{2}}}})
	archiveID := equivalenceArchiveID(75)
	candidates := []follow.FinalizedClaimCandidate{
		{SourceID: "writer-a", Document: signEquivalenceClaim(t, equivalenceKey(t), archiveID, healthy.head.Info(), cid.Undef, 1, "2026-01-01T00:00:00Z")},
		{SourceID: "writer-z", Document: signEquivalenceClaim(t, equivalenceKey(t), archiveID, unavailable.head.Info(), cid.Undef, 1, "2026-01-01T00:00:00Z")},
	}
	blocks := copyEquivalenceStores(t, healthy.blocks)
	for _, order := range [][]follow.FinalizedClaimCandidate{candidates, {candidates[1], candidates[0]}} {
		selection, err := follow.SelectFinalizedClaim(t.Context(), blocks, "all", order)
		if err != nil {
			t.Fatalf("selecting with unavailable source root: %v", err)
		}
		if selection.Representative.SourceID != "writer-a" || len(selection.Unavailable) != 1 ||
			selection.Unavailable[0].Left.SourceID != "writer-z" {
			t.Fatalf("selection with unavailable source = %+v", selection)
		}
	}

	copyEquivalenceStoreInto(t, blocks, unavailable.blocks)
	selection, err := follow.SelectFinalizedClaim(t.Context(), blocks, "all", candidates)
	if err != nil {
		t.Fatalf("retrying identical observation after proof arrival: %v", err)
	}
	if selection.Representative.SourceID != "writer-z" || len(selection.Unavailable) != 0 {
		t.Fatalf("selection after proof arrival = %+v, want writer-z", selection)
	}
}

func TestFinalizedClaimArbitrationAttributesProjectionAndManifestProofGaps(t *testing.T) {
	params := archive.Params{Name: "all", Net: "arbitration-net", OriginSlot: 96, SegBits: 3, FanoutBits: 2}
	lower := buildEquivalenceWriter(t, params,
		equivalenceStep{syncedTo: 103, rows: []equivalenceRow{{slot: 98, ids: []uint64{1}}}})
	higher := buildEquivalenceWriter(t, params,
		equivalenceStep{syncedTo: 103, rows: []equivalenceRow{{slot: 98, ids: []uint64{1}}}},
		equivalenceStep{syncedTo: 119, rows: []equivalenceRow{{slot: 115, ids: []uint64{2}}}})
	archiveID := equivalenceArchiveID(76)

	t.Run("projection descendant", func(t *testing.T) {
		blocks := copyEquivalenceStores(t, lower.blocks)
		rootBlock, err := higher.blocks.Get(t.Context(), higher.head.Root())
		if err != nil {
			t.Fatal(err)
		}
		if err := blocks.Put(t.Context(), rootBlock); err != nil {
			t.Fatal(err)
		}
		candidates := []follow.FinalizedClaimCandidate{
			{SourceID: "writer-a", Document: signEquivalenceClaim(t, equivalenceKey(t), archiveID, lower.head.Info(), cid.Undef, 1, "2026-01-01T00:00:00Z")},
			{SourceID: "writer-z", Document: signEquivalenceClaim(t, equivalenceKey(t), archiveID, higher.head.Info(), cid.Undef, 1, "2026-01-01T00:00:00Z")},
		}
		selection, err := follow.SelectFinalizedClaim(t.Context(), blocks, "all", candidates)
		if err != nil {
			t.Fatalf("selecting around unavailable projection: %v", err)
		}
		if selection.Representative.SourceID != "writer-a" || len(selection.Unavailable) != 1 {
			t.Fatalf("projection-gap selection = %+v", selection)
		}
	})

	t.Run("manifest link", func(t *testing.T) {
		genesis := putEquivalenceManifest(t, lower.blocks, cid.Undef, 10)
		missingTipStore := newEquivalenceBlockstore()
		missingTip := putEquivalenceManifest(t, missingTipStore, genesis, 20)
		blocks := copyEquivalenceStores(t, lower.blocks)
		candidates := []follow.FinalizedClaimCandidate{
			{SourceID: "writer-a", Document: signEquivalenceClaim(t, equivalenceKey(t), archiveID, lower.head.Info(), genesis, 1, "2026-01-01T00:00:00Z")},
			{SourceID: "writer-z", Document: signEquivalenceClaim(t, equivalenceKey(t), archiveID, lower.head.Info(), missingTip, 1, "2026-01-01T00:00:00Z")},
		}
		selection, err := follow.SelectFinalizedClaim(t.Context(), blocks, "all", candidates)
		if err != nil {
			t.Fatalf("selecting around unavailable manifest: %v", err)
		}
		if selection.Representative.SourceID != "writer-a" || len(selection.Unavailable) != 1 {
			t.Fatalf("manifest-gap selection = %+v", selection)
		}
	})
}

func TestFinalizedClaimArbitrationConflictOutranksUnrelatedUnavailableSource(t *testing.T) {
	params := archive.Params{Name: "all", Net: "arbitration-net", OriginSlot: 96, SegBits: 3, FanoutBits: 2}
	left := buildEquivalenceWriter(t, params,
		equivalenceStep{syncedTo: 111, rows: []equivalenceRow{{slot: 98, ids: []uint64{1}}}})
	right := buildEquivalenceWriter(t, params,
		equivalenceStep{syncedTo: 111, rows: []equivalenceRow{{slot: 98, ids: []uint64{99}}}})
	missing := buildEquivalenceWriter(t, params, equivalenceStep{syncedTo: 119})
	archiveID := equivalenceArchiveID(77)
	candidates := []follow.FinalizedClaimCandidate{
		{SourceID: "writer-a", Document: signEquivalenceClaim(t, equivalenceKey(t), archiveID, left.head.Info(), cid.Undef, 1, "2026-01-01T00:00:00Z")},
		{SourceID: "writer-b", Document: signEquivalenceClaim(t, equivalenceKey(t), archiveID, right.head.Info(), cid.Undef, 1, "2026-01-01T00:00:00Z")},
		{SourceID: "writer-z", Document: signEquivalenceClaim(t, equivalenceKey(t), archiveID, missing.head.Info(), cid.Undef, 1, "2026-01-01T00:00:00Z")},
	}
	_, err := follow.SelectFinalizedClaim(t.Context(), copyEquivalenceStores(t, left.blocks, right.blocks), "all", candidates)
	var conflict *follow.FinalizedClaimConflictError
	if !errors.As(err, &conflict) {
		t.Fatalf("conflict plus unavailable source error = %T (%v), want conflict", err, err)
	}
}

func copyEquivalenceStoreInto(t *testing.T, destination, source blockstore.Blockstore) {
	t.Helper()
	keys, err := source.AllKeysChan(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	for key := range keys {
		block, err := source.Get(t.Context(), key)
		if err != nil {
			t.Fatal(err)
		}
		if err := destination.Put(t.Context(), block); err != nil {
			t.Fatal(err)
		}
	}
}

func arbitrationPermutations(in []follow.FinalizedClaimCandidate) [][]follow.FinalizedClaimCandidate {
	values := append([]follow.FinalizedClaimCandidate(nil), in...)
	var out [][]follow.FinalizedClaimCandidate
	var visit func(int)
	visit = func(index int) {
		if index == len(values) {
			out = append(out, append([]follow.FinalizedClaimCandidate(nil), values...))
			return
		}
		for i := index; i < len(values); i++ {
			values[index], values[i] = values[i], values[index]
			visit(index + 1)
			values[index], values[i] = values[i], values[index]
		}
	}
	visit(0)
	return out
}
