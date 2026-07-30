package follow

import (
	"bytes"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/ipfs/go-cid"

	"github.com/blobarchive/bloar/server"
)

func conflictStateTestSource(seed byte, sourceID string, revision uint64, root cid.Cid, syncedTo uint64) ConflictCandidateSummary {
	return ConflictCandidateSummary{
		Role: ConflictCandidateSource, SourceID: sourceID, Revision: revision,
		Digest: sourceStateTestValue(seed), Root: root, SyncedTo: syncedTo, Covered: true,
	}
}

func conflictStateTestInput(t *testing.T, archiveID server.ArchiveID, head string, setRevision uint64,
	setDigest [32]byte,
) ConflictLatchInput {
	t.Helper()
	return ConflictLatchInput{
		ArchiveID: archiveID, Head: head, Reason: ConflictReasonEqualCoverageRootMismatch,
		SourceSetRevision: setRevision, SourceSetDigest: setDigest, PairCount: 1,
		Left:  conflictStateTestSource(11, "writer-b", 8, epochTestCID(t, 11), 120),
		Right: conflictStateTestSource(12, "writer-a", 9, epochTestCID(t, 12), 120),
	}
}

func conflictStateTestActivation(t *testing.T, s *state, archiveID server.ArchiveID, revision uint64,
	digest [32]byte,
) sourceSetActivation {
	t.Helper()
	activation := sourceStateTestActivation(archiveID, revision, digest,
		sourceBinding{sourceID: "writer-a", pubkey: sourceStateTestValue(31)},
		sourceBinding{sourceID: "writer-b", pubkey: sourceStateTestValue(32)},
	)
	if err := s.activateSourceSet(activation); err != nil {
		t.Fatalf("activating source set: %v", err)
	}
	return activation
}

func conflictStateTestCommit(t *testing.T, s *state, request ConflictLatchRequest) (ConflictRecord, bool) {
	t.Helper()
	batch := s.kv.NewBatch()
	defer batch.Close()
	record, created, err := s.stageConflictLatch(batch, request)
	if err != nil {
		t.Fatalf("staging conflict latch: %v", err)
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		t.Fatalf("committing conflict latch: %v", err)
	}
	return record, created
}

func rawSourceSetMarker(t *testing.T, s *state) []byte {
	t.Helper()
	raw, closer, err := s.kv.Get(sourceSetMarkerKey)
	if err != nil {
		t.Fatal(err)
	}
	defer closer.Close()
	return append([]byte(nil), raw...)
}

func TestConflictLatchIsAtomicAndUpgradesSourceSetMarker(t *testing.T) {
	s := openSourceStateTestDB(t)
	archiveID := sourceStateTestArchive(40)
	digest := sourceStateTestValue(41)
	activation := conflictStateTestActivation(t, s, archiveID, 1, digest)
	if raw := rawSourceSetMarker(t, s); raw[0] != sourceStateEncodingV1 {
		t.Fatalf("initial source-set marker version = %d, want v1", raw[0])
	}
	request, err := NewConflictLatchRequest(conflictStateTestInput(t, archiveID, "all", 1, digest))
	if err != nil {
		t.Fatal(err)
	}

	// Every part of the latch, including the old-binary rejection marker, is
	// invisible until the caller's batch commits.
	batch := s.kv.NewBatch()
	staged, created, err := s.stageConflictLatch(batch, request)
	if err != nil || !created || staged.Sequence != 1 {
		t.Fatalf("staged conflict = %+v created=%t err=%v", staged, created, err)
	}
	if _, ok, err := s.conflictLatch(archiveID, "all"); err != nil || ok {
		t.Fatalf("uncommitted latch visible: ok=%t err=%v", ok, err)
	}
	if marker, _, err := s.sourceSetMarker(); err != nil || marker.features != 0 {
		t.Fatalf("uncommitted feature visible: marker=%+v err=%v", marker, err)
	}
	if err := batch.Close(); err != nil {
		t.Fatal(err)
	}

	record, created := conflictStateTestCommit(t, s, request)
	if !created || record.Sequence != 1 || record.EvidenceID == ([32]byte{}) {
		t.Fatalf("committed conflict = %+v created=%t", record, created)
	}
	loaded, ok, err := LoadConflictLatch(s.kv, archiveID, "all")
	if err != nil || !ok || loaded != record {
		t.Fatalf("loaded conflict = %+v ok=%t err=%v, want %+v", loaded, ok, err, record)
	}
	marker, ok, err := s.sourceSetMarker()
	if err != nil || !ok || marker.features != sourceSetFeatureConflictLatch {
		t.Fatalf("upgraded marker = %+v ok=%t err=%v", marker, ok, err)
	}
	if raw := rawSourceSetMarker(t, s); len(raw) != 81 || raw[0] != sourceStateEncodingV2 {
		t.Fatalf("upgraded marker encoding = %x, want fixed-width v2", raw)
	}

	// Re-staging identical evidence while active is idempotent and does not
	// allocate another occurrence.
	again, created := conflictStateTestCommit(t, s, request)
	if created || again != record {
		t.Fatalf("idempotent latch = %+v created=%t, want %+v/false", again, created, record)
	}
	differentInput := conflictStateTestInput(t, archiveID, "all", 1, digest)
	differentInput.Left.Root = epochTestCID(t, 43)
	different, err := NewConflictLatchRequest(differentInput)
	if err != nil {
		t.Fatal(err)
	}
	replacement := s.kv.NewBatch()
	if _, _, err := s.stageConflictLatch(replacement, different); err == nil || !strings.Contains(err.Error(), "already has active") {
		t.Fatalf("distinct replacement error = %v", err)
	}
	_ = replacement.Close()
	if got, ok, err := s.conflictLatch(archiveID, "all"); err != nil || !ok || got != record {
		t.Fatalf("distinct evidence replaced active latch: %+v ok=%t err=%v", got, ok, err)
	}

	// An ordinary roster advance preserves the capability bit and therefore the
	// v2 marker. It cannot turn an active/conflict-aware store back into v1.
	next := activation
	next.marker.revision = 2
	next.marker.digest = sourceStateTestValue(42)
	if err := s.activateSourceSet(next); err != nil {
		t.Fatalf("advancing source set after latch: %v", err)
	}
	next.marker.features = sourceSetFeatureConflictLatch
	requireSourceSetMarker(t, s, next.marker)
	if raw := rawSourceSetMarker(t, s); raw[0] != sourceStateEncodingV2 {
		t.Fatalf("roster advance downgraded marker to version %d", raw[0])
	}
}

func TestConflictLatchRequiresExactActiveSourceSetGeneration(t *testing.T) {
	archiveID := sourceStateTestArchive(39)
	digest := sourceStateTestValue(40)
	request, err := NewConflictLatchRequest(conflictStateTestInput(t, archiveID, "all", 1, digest))
	if err != nil {
		t.Fatal(err)
	}

	withoutMarker := openSourceStateTestDB(t)
	batch := withoutMarker.kv.NewBatch()
	if _, _, err := withoutMarker.stageConflictLatch(batch, request); err == nil || !strings.Contains(err.Error(), "without an active source set") {
		t.Fatalf("missing marker error = %v", err)
	}
	_ = batch.Close()

	s := openSourceStateTestDB(t)
	conflictStateTestActivation(t, s, archiveID, 2, sourceStateTestValue(41))
	batch = s.kv.NewBatch()
	if _, _, err := s.stageConflictLatch(batch, request); err == nil || !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("mismatched generation error = %v", err)
	}
	_ = batch.Close()
	if _, ok, err := s.conflictLatch(archiveID, "all"); err != nil || ok {
		t.Fatalf("failed latch left active evidence: ok=%t err=%v", ok, err)
	}
}

func TestConflictMarkerV1V2CompatibilityAndStrictFeatures(t *testing.T) {
	base := sourceSetMarker{
		archiveID: sourceStateTestArchive(43), revision: 7, digest: sourceStateTestValue(44),
	}
	v1, err := encodeSourceSetMarker(base)
	if err != nil {
		t.Fatal(err)
	}
	if len(v1) != 73 || v1[0] != sourceStateEncodingV1 {
		t.Fatalf("v1 marker = %x", v1)
	}
	if decoded, err := decodeSourceSetMarker(v1); err != nil || decoded != base {
		t.Fatalf("decoded v1 marker = %+v err=%v", decoded, err)
	}

	aware := base
	aware.features = sourceSetFeatureConflictLatch
	v2, err := encodeSourceSetMarker(aware)
	if err != nil {
		t.Fatal(err)
	}
	if len(v2) != 81 || v2[0] != sourceStateEncodingV2 {
		t.Fatalf("v2 marker = %x", v2)
	}
	if decoded, err := decodeSourceSetMarker(v2); err != nil || decoded != aware {
		t.Fatalf("decoded v2 marker = %+v err=%v", decoded, err)
	}
	// This is the exact v1 decoder predicate used before the feature upgrade: a
	// v2 marker cannot be mistaken for its shorter predecessor.
	if len(v2) == 73 && v2[0] == sourceStateEncodingV1 {
		t.Fatal("conflict-aware marker was accepted by the v1 shape")
	}

	unknown := aware
	unknown.features |= 1 << 63
	if _, err := encodeSourceSetMarker(unknown); err == nil {
		t.Fatal("encoded unknown marker feature")
	}
	unknownRaw := append([]byte(nil), v2...)
	unknownRaw[len(unknownRaw)-1] |= 2
	if _, err := decodeSourceSetMarker(unknownRaw); err == nil {
		t.Fatal("decoded unknown marker feature")
	}
	zeroFeature := append([]byte(nil), v2...)
	clear(zeroFeature[73:])
	if _, err := decodeSourceSetMarker(zeroFeature); err == nil {
		t.Fatal("decoded a v2 marker with no feature")
	}
	for _, malformed := range [][]byte{v1[:len(v1)-1], append(v1, 0), v2[:len(v2)-1], append(v2, 0)} {
		if _, err := decodeSourceSetMarker(malformed); err == nil {
			t.Fatalf("decoded malformed marker of %d bytes", len(malformed))
		}
	}
}

func conflictStateTestCleared(t *testing.T, seed byte) (*state, server.ArchiveID, ConflictRecord) {
	t.Helper()
	s := openSourceStateTestDB(t)
	archiveID := sourceStateTestArchive(seed)
	digest := sourceStateTestValue(seed + 1)
	conflictStateTestActivation(t, s, archiveID, 1, digest)
	request, err := NewConflictLatchRequest(conflictStateTestInput(t, archiveID, "all", 1, digest))
	if err != nil {
		t.Fatal(err)
	}
	record, _ := conflictStateTestCommit(t, s, request)
	if _, err := ClearConflictLatch(s.kv, ConflictClearRequest{
		ArchiveID: archiveID, Head: "all", EvidenceID: record.EvidenceID,
		ClearedAt: time.Unix(400, 0), Operator: "operator",
	}); err != nil {
		t.Fatal(err)
	}
	return s, archiveID, record
}

func TestValidateConflictStateProtectsActiveAndClearedCapabilityFloor(t *testing.T) {
	s := openSourceStateTestDB(t)
	archiveID := sourceStateTestArchive(34)
	digest := sourceStateTestValue(35)
	conflictStateTestActivation(t, s, archiveID, 1, digest)
	if err := s.validateConflictState(archiveID); err != nil {
		t.Fatalf("empty v1 conflict state: %v", err)
	}
	request, err := NewConflictLatchRequest(conflictStateTestInput(t, archiveID, "all", 1, digest))
	if err != nil {
		t.Fatal(err)
	}
	record, _ := conflictStateTestCommit(t, s, request)
	if err := ValidateConflictState(s.kv, archiveID); err != nil {
		t.Fatalf("valid active conflict state: %v", err)
	}
	if _, err := ClearConflictLatch(s.kv, ConflictClearRequest{
		ArchiveID: archiveID, Head: "all", EvidenceID: record.EvidenceID,
		ClearedAt: time.Unix(401, 0), Operator: "operator",
	}); err != nil {
		t.Fatal(err)
	}
	if err := ValidateConflictState(s.kv, archiveID); err != nil {
		t.Fatalf("valid cleared conflict state: %v", err)
	}

	// Active is empty now, but sequence/history are permanent no-downgrade
	// facts. Replacing the marker with its v1 shape must fail startup validation.
	marker, ok, err := s.sourceSetMarker()
	if err != nil || !ok {
		t.Fatalf("reading conflict-aware marker: ok=%t err=%v", ok, err)
	}
	marker.features = 0
	downgraded, err := encodeSourceSetMarker(marker)
	if err != nil {
		t.Fatal(err)
	}
	if downgraded[0] != sourceStateEncodingV1 {
		t.Fatalf("downgraded marker version = %d", downgraded[0])
	}
	if err := s.kv.Set(sourceSetMarkerKey, downgraded, pebble.Sync); err != nil {
		t.Fatal(err)
	}
	if err := ValidateConflictState(s.kv, archiveID); err == nil || !strings.Contains(err.Error(), "conflict-aware") {
		t.Fatalf("cleared rows with v1 marker validation error = %v", err)
	}
}

func TestValidateConflictStateRejectsConflictAwareMarkerWithoutRows(t *testing.T) {
	archiveID := sourceStateTestArchive(36)

	// A never-activated store and an ordinary v1 source-set marker both
	// legitimately have no conflict rows.
	empty := openSourceStateTestDB(t)
	if err := ValidateConflictState(empty.kv, archiveID); err != nil {
		t.Fatalf("empty store conflict state: %v", err)
	}
	v1 := openSourceStateTestDB(t)
	conflictStateTestActivation(t, v1, archiveID, 1, sourceStateTestValue(37))
	if err := ValidateConflictState(v1.kv, archiveID); err != nil {
		t.Fatalf("empty v1 conflict state: %v", err)
	}
	wrongArchive := openSourceStateTestDB(t)
	otherArchive := sourceStateTestArchive(39)
	conflictStateTestActivation(t, wrongArchive, otherArchive, 1, sourceStateTestValue(40))
	if err := ValidateConflictState(wrongArchive.kv, archiveID); err == nil || !strings.Contains(err.Error(), "marker belongs to archive") {
		t.Fatalf("empty v1 wrong-archive validation error = %v", err)
	}

	// Once the irreversible feature bit exists, a valid active latch has
	// active+sequence rows and a valid clear has sequence+history rows. Losing
	// both permanent cleared rows must fail closed rather than look like a node
	// which has never observed a conflict.
	aware, awareArchive, _ := conflictStateTestCleared(t, 38)
	if err := aware.kv.Delete(conflictStateKey(keySourceConflictSequence, awareArchive, "all"), pebble.Sync); err != nil {
		t.Fatal(err)
	}
	if err := aware.kv.Delete(conflictStateKey(keySourceConflictHistory, awareArchive, "all"), pebble.Sync); err != nil {
		t.Fatal(err)
	}
	err := ValidateConflictState(aware.kv, awareArchive)
	if err == nil || !strings.Contains(err.Error(), "conflict-aware source-set marker has no durable conflict rows") {
		t.Fatalf("conflict-aware marker without rows validation error = %v", err)
	}
}

func TestValidateConflictStateRejectsCorruptClearedRows(t *testing.T) {
	for _, tc := range []struct {
		name      string
		mutate    func(*testing.T, *state, server.ArchiveID, ConflictRecord)
		want      string
		maxErrLen int
	}{
		{
			name: "missing sequence",
			mutate: func(t *testing.T, s *state, archiveID server.ArchiveID, _ ConflictRecord) {
				t.Helper()
				if err := s.kv.Delete(conflictStateKey(keySourceConflictSequence, archiveID, "all"), pebble.Sync); err != nil {
					t.Fatal(err)
				}
			},
			want: "without a sequence floor",
		},
		{
			name: "missing history",
			mutate: func(t *testing.T, s *state, archiveID server.ArchiveID, _ ConflictRecord) {
				t.Helper()
				if err := s.kv.Delete(conflictStateKey(keySourceConflictHistory, archiveID, "all"), pebble.Sync); err != nil {
					t.Fatal(err)
				}
			},
			want: "no operator history",
		},
		{
			name: "sequence ahead of history",
			mutate: func(t *testing.T, s *state, archiveID server.ArchiveID, _ ConflictRecord) {
				t.Helper()
				if err := s.kv.Set(conflictStateKey(keySourceConflictSequence, archiveID, "all"), encodeConflictSequence(2), pebble.Sync); err != nil {
					t.Fatal(err)
				}
			},
			want: "operator history ends at",
		},
		{
			name: "malformed history",
			mutate: func(t *testing.T, s *state, archiveID server.ArchiveID, _ ConflictRecord) {
				t.Helper()
				if err := s.kv.Set(conflictStateKey(keySourceConflictHistory, archiveID, "all"), []byte{1, 1}, pebble.Sync); err != nil {
					t.Fatal(err)
				}
			},
			want: "decoding conflict clear history",
		},
		{
			name: "row for another archive",
			mutate: func(t *testing.T, s *state, _ server.ArchiveID, _ ConflictRecord) {
				t.Helper()
				other := sourceStateTestArchive(250)
				if err := s.kv.Set(conflictStateKey(keySourceConflictSequence, other, "other"), encodeConflictSequence(1), pebble.Sync); err != nil {
					t.Fatal(err)
				}
			},
			want: "belongs to archive",
		},
		{
			name: "overlong row for another archive has bounded diagnostic",
			mutate: func(t *testing.T, s *state, _ server.ArchiveID, _ ConflictRecord) {
				t.Helper()
				other := sourceStateTestArchive(251)
				head := strings.Repeat("x", maxConflictHeadBytes+4096)
				if err := s.kv.Set(conflictStateKey(keySourceConflictSequence, other, head), encodeConflictSequence(1), pebble.Sync); err != nil {
					t.Fatal(err)
				}
			},
			want:      "invalid identity",
			maxErrLen: 256,
		},
		{
			name: "malformed namespace key",
			mutate: func(t *testing.T, s *state, _ server.ArchiveID, _ ConflictRecord) {
				t.Helper()
				if err := s.kv.Set(append(key(keySourceConflictSequence), 1, 2, 3), encodeConflictSequence(1), pebble.Sync); err != nil {
					t.Fatal(err)
				}
			},
			want: "malformed key",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, archiveID, record := conflictStateTestCleared(t, byte(100+len(tc.name)))
			tc.mutate(t, s, archiveID, record)
			err := ValidateConflictState(s.kv, archiveID)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("validation error = %v, want %q", err, tc.want)
			}
			if tc.maxErrLen != 0 && len(err.Error()) > tc.maxErrLen {
				t.Fatalf("validation error is %d bytes, want at most %d: %v", len(err.Error()), tc.maxErrLen, err)
			}
		})
	}
}

func TestConflictRequestRolesReasonsAndSameSourceHistory(t *testing.T) {
	archiveID := sourceStateTestArchive(45)
	digest := sourceStateTestValue(46)
	base := conflictStateTestInput(t, archiveID, "all", 1, digest)

	// A source may conflict with its own durable earlier generation. This is a
	// history fork, not a same-revision source equivocation.
	base.Right = base.Left
	base.Right.Role = ConflictCandidateDurable
	base.Right.CheckpointVersion = checkpointVersionV4
	base.Right.Revision--
	base.Right.Digest = sourceStateTestValue(47)
	base.Right.Root = epochTestCID(t, 47)
	if _, err := NewConflictLatchRequest(base); err != nil {
		t.Fatalf("same-source fresh/durable fork rejected: %v", err)
	}
	sameSourcePublications := base
	sameSourcePublications.Right.Role = ConflictCandidateSource
	sameSourcePublications.Right.CheckpointVersion = 0
	if _, err := NewConflictLatchRequest(sameSourcePublications); err == nil || !strings.Contains(err.Error(), "signer-local equivocation") {
		t.Fatalf("same-source source/source error = %v", err)
	}
	twoDurable := base
	twoDurable.Left.Role = ConflictCandidateDurable
	twoDurable.Left.CheckpointVersion = checkpointVersionV4
	if _, err := NewConflictLatchRequest(twoDurable); err == nil || !strings.Contains(err.Error(), "two durable") {
		t.Fatalf("durable/durable error = %v", err)
	}

	legacy := base.Right
	legacy.Role = ConflictCandidateDurable
	legacy.CheckpointVersion = checkpointVersionV2
	legacy.SourceID = ""
	legacy.Revision = 0
	legacy.Digest = [32]byte{}
	base.Right = legacy
	if _, err := NewConflictLatchRequest(base); err != nil {
		t.Fatalf("unattributed legacy durable baseline rejected: %v", err)
	}
	fabricatedV4 := base
	fabricatedV4.Right.CheckpointVersion = checkpointVersionV4
	if _, err := NewConflictLatchRequest(fabricatedV4); err == nil || !strings.Contains(err.Error(), "v4 durable candidate requires source provenance") {
		t.Fatalf("unattributed v4 durable error = %v", err)
	}

	invalid := base
	invalid.Right.SourceID = "invented"
	if _, err := NewConflictLatchRequest(invalid); err == nil || !strings.Contains(err.Error(), "only a v4") {
		t.Fatalf("legacy durable source invention error = %v", err)
	}
	invalid = base
	invalid.Right.CheckpointVersion = 0
	if _, err := NewConflictLatchRequest(invalid); err == nil || !strings.Contains(err.Error(), "checkpoint version") {
		t.Fatalf("missing durable checkpoint version error = %v", err)
	}
	invalid = base
	invalid.Left.Role = 99
	if _, err := NewConflictLatchRequest(invalid); err == nil || !strings.Contains(err.Error(), "role") {
		t.Fatalf("unknown role error = %v", err)
	}

	for _, tc := range []struct {
		name string
		edit func(*ConflictLatchInput)
		want string
	}{
		{"unknown reason", func(in *ConflictLatchInput) { in.Reason = 99 }, "reason"},
		{"zero pairs", func(in *ConflictLatchInput) { in.PairCount = 0 }, "pair count"},
		{"too many pairs", func(in *ConflictLatchInput) { in.PairCount = maxConflictPairs + 1 }, "pair count"},
		{"equal reason unequal coverage", func(in *ConflictLatchInput) { in.Left.SyncedTo++ }, "equal-coverage"},
		{"prefix reason equal coverage", func(in *ConflictLatchInput) { in.Reason = ConflictReasonPrefixProjectionMismatch }, "different coverage"},
		{"manifest reason missing tips", func(in *ConflictLatchInput) { in.Reason = ConflictReasonManifestBranch }, "manifest"},
		{"same endpoint", func(in *ConflictLatchInput) { in.Right = in.Left }, "same endpoint"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			in := conflictStateTestInput(t, archiveID, "all", 1, digest)
			tc.edit(&in)
			if _, err := NewConflictLatchRequest(in); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want %q", err, tc.want)
			}
		})
	}

	manifestInput := conflictStateTestInput(t, archiveID, "all", 1, digest)
	manifestInput.Reason = ConflictReasonManifestBranch
	manifestInput.Left.Root = manifestInput.Right.Root
	manifestInput.Left.Manifest = epochTestCID(t, 48)
	manifestInput.Right.Manifest = epochTestCID(t, 49)
	if _, err := NewConflictLatchRequest(manifestInput); err != nil {
		t.Fatalf("valid manifest branch rejected: %v", err)
	}
	prefixInput := conflictStateTestInput(t, archiveID, "all", 1, digest)
	prefixInput.Reason = ConflictReasonPrefixProjectionMismatch
	prefixInput.Left.SyncedTo++
	if _, err := NewConflictLatchRequest(prefixInput); err != nil {
		t.Fatalf("valid projection mismatch rejected: %v", err)
	}
}

func conflictStateTestDocument(archiveID server.ArchiveID, revision uint64) server.Doc {
	return server.Doc{Unsigned: server.Unsigned{
		V: server.LogicalArchiveDocVersion, Net: "testnet", ArchiveID: &archiveID,
		UpdatedAt: "2026-01-01T00:00:00Z", Revision: &revision,
		Heads: []server.HeadEntry{{Name: "all", Root: "unused", SyncedTo: &revision}},
	}}
}

func TestConflictRequestFromErrorChoosesOnePairDeterministically(t *testing.T) {
	archiveID := sourceStateTestArchive(50)
	setDigest := sourceStateTestValue(51)
	makePair := func(leftID, rightID string, seed byte, reason ConflictReason) FinalizedClaimPairEvidence {
		leftRevision, rightRevision := uint64(seed), uint64(seed+1)
		return FinalizedClaimPairEvidence{
			Left:     FinalizedClaimCandidate{SourceID: leftID, Document: conflictStateTestDocument(archiveID, leftRevision)},
			Right:    FinalizedClaimCandidate{SourceID: rightID, Document: conflictStateTestDocument(archiveID, rightRevision)},
			Relation: ClaimRelationInvalid,
			Conflict: &ArchiveConflictError{
				ArchiveID: archiveID, Head: "all", ReasonCode: reason, Reason: reason.String(),
				LeftRoot: epochTestCID(t, seed), RightRoot: epochTestCID(t, seed+1),
				LeftSyncedTo: 100, RightSyncedTo: 100, LeftCovered: true, RightCovered: true,
			},
		}
	}
	first := makePair("writer-z", "writer-y", 52, ConflictReasonEqualCoverageRootMismatch)
	second := makePair("writer-b", "writer-a", 54, ConflictReasonEqualCoverageRootMismatch)
	forward := &FinalizedClaimConflictError{Head: "all", Conflicts: []FinalizedClaimPairEvidence{first, second}}
	reverse := &FinalizedClaimConflictError{Head: "all", Conflicts: []FinalizedClaimPairEvidence{second, first}}
	a, err := NewConflictLatchRequestFromError(archiveID, 7, setDigest, forward)
	if err != nil {
		t.Fatal(err)
	}
	b, err := NewConflictLatchRequestFromError(archiveID, 7, setDigest, reverse)
	if err != nil {
		t.Fatal(err)
	}
	ra, err := recordFromRequest(a, 1)
	if err != nil {
		t.Fatal(err)
	}
	rb, err := recordFromRequest(b, 1)
	if err != nil {
		t.Fatal(err)
	}
	if ra != rb || ra.PairCount != 2 || ra.Left.SourceID != "writer-a" || ra.Right.SourceID != "writer-b" {
		t.Fatalf("deterministic records differ or chose wrong pair:\n%+v\n%+v", ra, rb)
	}
}

func TestConflictRecordEncodingIsCanonicalBoundedAndContentBound(t *testing.T) {
	input := conflictStateTestInput(t, sourceStateTestArchive(60), "all", 4, sourceStateTestValue(61))
	request, err := NewConflictLatchRequest(input)
	if err != nil {
		t.Fatal(err)
	}
	record, err := recordFromRequest(request, 9)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := encodeConflictRecord(record)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeConflictRecord(raw)
	if err != nil || decoded != record {
		t.Fatalf("decoded record = %+v err=%v, want %+v", decoded, err, record)
	}

	for i := range len(raw) {
		if _, err := decodeConflictRecord(raw[:i]); err == nil {
			t.Fatalf("decoded record truncated to %d/%d bytes", i, len(raw))
		}
	}
	if _, err := decodeConflictRecord(append(append([]byte(nil), raw...), 0)); err == nil {
		t.Fatal("decoded record with trailing data")
	}
	tampered := append([]byte(nil), raw...)
	tampered[len(tampered)-1] ^= 1
	if _, err := decodeConflictRecord(tampered); err == nil {
		t.Fatal("decoded record with a tampered evidence ID")
	}
	tampered = append([]byte(nil), raw...)
	// version + archive is followed by a uint16 head length.
	tampered[33], tampered[34] = 1, 0
	if _, err := decodeConflictRecord(tampered); err == nil {
		t.Fatal("decoded over-bound head length")
	}
	tampered = append([]byte(nil), raw...)
	sequenceOffset := 1 + len(server.ArchiveID{}) + 2 + len(record.Head)
	clear(tampered[sequenceOffset : sequenceOffset+8])
	if _, err := decodeConflictRecord(tampered); err == nil {
		t.Fatal("decoded zero conflict sequence")
	}

	other, err := recordFromRequest(request, 10)
	if err != nil {
		t.Fatal(err)
	}
	if other.EvidenceID == record.EvidenceID {
		t.Fatal("monotonic sequence was omitted from the evidence ID")
	}
	changed := input
	changed.Left.Digest[0] ^= 1
	changedRequest, err := NewConflictLatchRequest(changed)
	if err != nil {
		t.Fatal(err)
	}
	changedRecord, err := recordFromRequest(changedRequest, 9)
	if err != nil {
		t.Fatal(err)
	}
	if changedRecord.EvidenceID == record.EvidenceID {
		t.Fatal("candidate digest was omitted from the evidence ID")
	}
}

func TestConflictClearRequiresExactEvidenceAndPreservesSequence(t *testing.T) {
	s := openSourceStateTestDB(t)
	archiveID := sourceStateTestArchive(70)
	setDigest := sourceStateTestValue(71)
	conflictStateTestActivation(t, s, archiveID, 1, setDigest)
	request, err := NewConflictLatchRequest(conflictStateTestInput(t, archiveID, "all", 1, setDigest))
	if err != nil {
		t.Fatal(err)
	}
	checkpointBefore := checkpoint{
		root: epochTestCID(t, 74), syncedTo: 99, updatedAt: time.Unix(90, 0).UTC(),
	}
	if err := s.putCheckpoint("all", checkpointBefore); err != nil {
		t.Fatal(err)
	}
	ref := sourceRef{archiveID: archiveID, sourceID: "writer-a"}
	floorBefore := sourcePublicationFloor{revision: 5, digest: sourceStateTestValue(75)}
	commitSourceStateBatch(t, s, func(batch *pebble.Batch) error {
		return s.stageSourcePublicationFloor(batch, ref, floorBefore)
	})
	first, _ := conflictStateTestCommit(t, s, request)

	wrong := first.EvidenceID
	wrong[0] ^= 1
	if _, err := ClearConflictLatch(s.kv, ConflictClearRequest{
		ArchiveID: archiveID, Head: "all", EvidenceID: wrong,
		ClearedAt: time.Unix(100, 0), Operator: "operator-a",
	}); err == nil || !strings.Contains(err.Error(), "not") {
		t.Fatalf("wrong evidence clear error = %v", err)
	}
	if got, ok, err := s.conflictLatch(archiveID, "all"); err != nil || !ok || got != first {
		t.Fatalf("wrong-ID clear changed active record: %+v ok=%t err=%v", got, ok, err)
	}

	// A staged clear is as atomic as a latch.
	batch := s.kv.NewBatch()
	clearRequest := ConflictClearRequest{
		ArchiveID: archiveID, Head: "all", EvidenceID: first.EvidenceID,
		ClearedAt: time.Unix(101, 987), Operator: "operator-a", Note: "reviewed source divergence",
	}
	staged, err := s.stageClearConflictLatch(batch, clearRequest)
	if err != nil {
		t.Fatal(err)
	}
	if staged.ClearedAt.Nanosecond() != 0 || staged.ClearedAt.Location() != time.UTC {
		t.Fatalf("clear timestamp was not normalized: %s", staged.ClearedAt)
	}
	if _, ok, _ := s.conflictLatch(archiveID, "all"); !ok {
		t.Fatal("staged clear became visible before commit")
	}
	if history, err := s.conflictClearHistory(archiveID, "all"); err != nil || len(history) != 0 {
		t.Fatalf("staged history became visible: %v err=%v", history, err)
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		t.Fatal(err)
	}
	_ = batch.Close()
	if _, ok, err := s.conflictLatch(archiveID, "all"); err != nil || ok {
		t.Fatalf("cleared latch remains: ok=%t err=%v", ok, err)
	}
	if marker, ok, err := s.sourceSetMarker(); err != nil || !ok || marker.features&sourceSetFeatureConflictLatch == 0 ||
		rawSourceSetMarker(t, s)[0] != sourceStateEncodingV2 {
		t.Fatalf("clear downgraded conflict-aware marker: %+v ok=%t err=%v", marker, ok, err)
	}
	if checkpointAfter, ok, err := s.checkpoint("all"); err != nil || !ok || checkpointAfter.root != checkpointBefore.root ||
		checkpointAfter.syncedTo != checkpointBefore.syncedTo || !checkpointAfter.updatedAt.Equal(checkpointBefore.updatedAt) {
		t.Fatalf("clear changed checkpoint: %+v ok=%t err=%v", checkpointAfter, ok, err)
	}
	if floorAfter, ok, err := s.sourcePublicationFloor(ref); err != nil || !ok || floorAfter != floorBefore {
		t.Fatalf("clear changed replay floor: %+v ok=%t err=%v", floorAfter, ok, err)
	}
	history, err := LoadConflictClearHistory(s.kv, archiveID, "all")
	if err != nil || len(history) != 1 || history[0] != staged {
		t.Fatalf("clear history = %+v err=%v, want %+v", history, err, staged)
	}
	if _, err := ClearConflictLatch(s.kv, clearRequest); !errors.Is(err, ErrNoActiveConflictLatch) {
		t.Fatalf("second clear error = %v, want ErrNoActiveConflictLatch", err)
	}

	second, created := conflictStateTestCommit(t, s, request)
	if !created || second.Sequence != first.Sequence+1 || second.EvidenceID == first.EvidenceID {
		t.Fatalf("second occurrence = %+v created=%t, first=%+v", second, created, first)
	}
	if _, err := ClearConflictLatch(s.kv, ConflictClearRequest{
		ArchiveID: archiveID, Head: "all", EvidenceID: second.EvidenceID,
		ClearedAt: time.Unix(102, 0), Operator: "operator-b",
	}); err != nil {
		t.Fatal(err)
	}
}

func TestConflictClearCannotLaunderInvalidActiveHistory(t *testing.T) {
	type corruptHistory func(*testing.T, *state, server.ArchiveID, ConflictRecord)
	tests := []struct {
		name     string
		sequence uint64
		corrupt  corruptHistory
		want     string
	}{
		{
			name: "first occurrence has history", sequence: 1,
			corrupt: func(t *testing.T, s *state, archiveID server.ArchiveID, active ConflictRecord) {
				t.Helper()
				history, err := encodeConflictClearHistory(archiveID, active.Head, []ConflictClearRecord{{
					ArchiveID: archiveID, Head: active.Head, Sequence: 1, EvidenceID: active.EvidenceID,
					ClearedAt: time.Unix(500, 0).UTC(), Operator: "operator",
				}})
				if err != nil {
					t.Fatal(err)
				}
				if err := s.kv.Set(conflictStateKey(keySourceConflictHistory, archiveID, active.Head), history, pebble.Sync); err != nil {
					t.Fatal(err)
				}
			},
			want: "first conflict occurrence",
		},
		{
			name: "later occurrence has no history", sequence: 2,
			corrupt: func(t *testing.T, s *state, archiveID server.ArchiveID, active ConflictRecord) {
				t.Helper()
				if err := s.kv.Delete(conflictStateKey(keySourceConflictHistory, archiveID, active.Head), pebble.Sync); err != nil {
					t.Fatal(err)
				}
			},
			want: "has no prior clear history",
		},
		{
			name: "later occurrence has wrong predecessor", sequence: 2,
			corrupt: func(t *testing.T, s *state, archiveID server.ArchiveID, active ConflictRecord) {
				t.Helper()
				history, err := encodeConflictClearHistory(archiveID, active.Head, []ConflictClearRecord{{
					ArchiveID: archiveID, Head: active.Head, Sequence: active.Sequence, EvidenceID: active.EvidenceID,
					ClearedAt: time.Unix(501, 0).UTC(), Operator: "operator",
				}})
				if err != nil {
					t.Fatal(err)
				}
				if err := s.kv.Set(conflictStateKey(keySourceConflictHistory, archiveID, active.Head), history, pebble.Sync); err != nil {
					t.Fatal(err)
				}
			},
			want: "is not preceded by clear history sequence",
		},
	}

	for i, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := openSourceStateTestDB(t)
			archiveID := sourceStateTestArchive(byte(120 + i))
			setDigest := sourceStateTestValue(byte(130 + i))
			conflictStateTestActivation(t, s, archiveID, 1, setDigest)
			request, err := NewConflictLatchRequest(conflictStateTestInput(t, archiveID, "all", 1, setDigest))
			if err != nil {
				t.Fatal(err)
			}
			active, _ := conflictStateTestCommit(t, s, request)
			if tc.sequence == 2 {
				if _, err := ClearConflictLatch(s.kv, ConflictClearRequest{
					ArchiveID: archiveID, Head: active.Head, EvidenceID: active.EvidenceID,
					ClearedAt: time.Unix(400, 0), Operator: "operator",
				}); err != nil {
					t.Fatal(err)
				}
				active, _ = conflictStateTestCommit(t, s, request)
			}
			if active.Sequence != tc.sequence {
				t.Fatalf("active sequence = %d, want %d", active.Sequence, tc.sequence)
			}

			tc.corrupt(t, s, archiveID, active)
			before, err := s.conflictClearHistory(archiveID, active.Head)
			if err != nil {
				t.Fatal(err)
			}
			_, err = ClearConflictLatch(s.kv, ConflictClearRequest{
				ArchiveID: archiveID, Head: active.Head, EvidenceID: active.EvidenceID,
				ClearedAt: time.Unix(600, 0), Operator: "operator",
			})
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("clear error = %v, want %q", err, tc.want)
			}
			if got, ok, err := s.conflictLatch(archiveID, active.Head); err != nil || !ok || got != active {
				t.Fatalf("refused clear changed active latch: got=%+v ok=%t err=%v, want=%+v", got, ok, err, active)
			}
			after, err := s.conflictClearHistory(archiveID, active.Head)
			if err != nil {
				t.Fatal(err)
			}
			if len(after) != len(before) {
				t.Fatalf("refused clear changed history length from %d to %d", len(before), len(after))
			}
			for i := range before {
				if after[i] != before[i] {
					t.Fatalf("refused clear changed history record %d: before=%+v after=%+v", i, before[i], after[i])
				}
			}
		})
	}
}

func TestConflictClearHistoryIsBoundedAndSequenceCannotReset(t *testing.T) {
	s := openSourceStateTestDB(t)
	archiveID := sourceStateTestArchive(72)
	setDigest := sourceStateTestValue(73)
	conflictStateTestActivation(t, s, archiveID, 1, setDigest)
	request, err := NewConflictLatchRequest(conflictStateTestInput(t, archiveID, "all", 1, setDigest))
	if err != nil {
		t.Fatal(err)
	}
	total := maxConflictClearHistory + 4
	for i := 1; i <= total; i++ {
		record, created := conflictStateTestCommit(t, s, request)
		if !created || record.Sequence != uint64(i) {
			t.Fatalf("occurrence %d = sequence %d created=%t", i, record.Sequence, created)
		}
		if _, err := ClearConflictLatch(s.kv, ConflictClearRequest{
			ArchiveID: archiveID, Head: "all", EvidenceID: record.EvidenceID,
			ClearedAt: time.Unix(int64(200+i), 0), Operator: "operator", Note: "bounded audit",
		}); err != nil {
			t.Fatal(err)
		}
	}
	history, err := LoadConflictClearHistory(s.kv, archiveID, "all")
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != maxConflictClearHistory || history[0].Sequence != uint64(total-maxConflictClearHistory+1) ||
		history[len(history)-1].Sequence != uint64(total) {
		t.Fatalf("bounded history = %+v", history)
	}
	if sequence, ok, err := s.conflictSequence(archiveID, "all"); err != nil || !ok || sequence != uint64(total) {
		t.Fatalf("preserved sequence = %d ok=%t err=%v", sequence, ok, err)
	}

	// Losing either half of the cleared-state proof fails closed instead of
	// resetting the next occurrence to one.
	if err := s.kv.Delete(conflictStateKey(keySourceConflictSequence, archiveID, "all"), pebble.Sync); err != nil {
		t.Fatal(err)
	}
	batch := s.kv.NewBatch()
	defer batch.Close()
	if _, _, err := s.stageConflictLatch(batch, request); err == nil || !strings.Contains(err.Error(), "without a sequence") {
		t.Fatalf("missing sequence floor error = %v", err)
	}
}

func TestConflictListIsArchiveScopedAndSorted(t *testing.T) {
	s := openSourceStateTestDB(t)
	archiveID := sourceStateTestArchive(80)
	setDigest := sourceStateTestValue(81)
	conflictStateTestActivation(t, s, archiveID, 1, setDigest)
	for _, head := range []string{"zeta", "alpha"} {
		request, err := NewConflictLatchRequest(conflictStateTestInput(t, archiveID, head, 1, setDigest))
		if err != nil {
			t.Fatal(err)
		}
		conflictStateTestCommit(t, s, request)
	}
	records, err := ListConflictLatches(s.kv, archiveID)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 2 || records[0].Head != "alpha" || records[1].Head != "zeta" {
		t.Fatalf("sorted active conflicts = %+v", records)
	}
	other, err := ListConflictLatches(s.kv, sourceStateTestArchive(82))
	if err != nil || len(other) != 0 {
		t.Fatalf("other archive conflicts = %+v err=%v", other, err)
	}
}

func TestConflictStateRefusesCorruptSequenceAndHistory(t *testing.T) {
	s := openSourceStateTestDB(t)
	archiveID := sourceStateTestArchive(90)
	setDigest := sourceStateTestValue(91)
	conflictStateTestActivation(t, s, archiveID, 1, setDigest)
	request, err := NewConflictLatchRequest(conflictStateTestInput(t, archiveID, "all", 1, setDigest))
	if err != nil {
		t.Fatal(err)
	}
	record, _ := conflictStateTestCommit(t, s, request)
	sequenceKey := conflictStateKey(keySourceConflictSequence, archiveID, "all")
	if err := s.kv.Set(sequenceKey, encodeConflictSequence(record.Sequence+1), pebble.Sync); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := LoadConflictLatch(s.kv, archiveID, "all"); err == nil || ok {
		t.Fatalf("mismatched sequence accepted: ok=%t err=%v", ok, err)
	}
	if err := s.kv.Set(sequenceKey, []byte{99}, pebble.Sync); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := LoadConflictLatch(s.kv, archiveID, "all"); err == nil || ok {
		t.Fatalf("truncated sequence accepted: ok=%t err=%v", ok, err)
	}

	history := []ConflictClearRecord{{
		ArchiveID: archiveID, Head: "all", Sequence: 1, EvidenceID: record.EvidenceID,
		ClearedAt: time.Unix(300, 0).UTC(), Operator: "operator",
	}}
	raw, err := encodeConflictClearHistory(archiveID, "all", history)
	if err != nil {
		t.Fatal(err)
	}
	for i := range len(raw) {
		if _, err := decodeConflictClearHistory(archiveID, "all", raw[:i]); err == nil {
			t.Fatalf("decoded history truncated to %d/%d bytes", i, len(raw))
		}
	}
	if _, err := decodeConflictClearHistory(archiveID, "all", append(append([]byte(nil), raw...), 0)); err == nil {
		t.Fatal("decoded history with trailing data")
	}
	bad := append([]byte(nil), raw...)
	bad[1] = maxConflictClearHistory + 1
	if _, err := decodeConflictClearHistory(archiveID, "all", bad); err == nil {
		t.Fatal("decoded over-bound clear history")
	}
	if _, err := encodeConflictClearHistory(archiveID, "all", []ConflictClearRecord{history[0], history[0]}); err == nil {
		t.Fatal("encoded non-monotonic clear history")
	}
	skipped := history[0]
	skipped.Sequence = 3
	skipped.EvidenceID = sourceStateTestValue(92)
	if _, err := encodeConflictClearHistory(archiveID, "all", []ConflictClearRecord{history[0], skipped}); err == nil ||
		!strings.Contains(err.Error(), "skips") {
		t.Fatalf("gapped clear history error = %v", err)
	}
}

func TestConflictRequestBoundsAreStrict(t *testing.T) {
	archiveID := sourceStateTestArchive(92)
	digest := sourceStateTestValue(93)
	for _, tc := range []struct {
		name string
		edit func(*ConflictLatchInput)
	}{
		{"zero archive", func(in *ConflictLatchInput) { in.ArchiveID = server.ArchiveID{} }},
		{"empty head", func(in *ConflictLatchInput) { in.Head = "" }},
		{"long head", func(in *ConflictLatchInput) { in.Head = strings.Repeat("h", maxConflictHeadBytes+1) }},
		{"nul head", func(in *ConflictLatchInput) { in.Head = "bad\x00head" }},
		{"zero source-set revision", func(in *ConflictLatchInput) { in.SourceSetRevision = 0 }},
		{"zero source-set digest", func(in *ConflictLatchInput) { in.SourceSetDigest = [32]byte{} }},
		{"long source", func(in *ConflictLatchInput) { in.Left.SourceID = strings.Repeat("a", maxSourceIDBytes+1) }},
		{"zero source revision", func(in *ConflictLatchInput) { in.Left.Revision = 0 }},
		{"zero source digest", func(in *ConflictLatchInput) { in.Left.Digest = [32]byte{} }},
		{"undefined root", func(in *ConflictLatchInput) { in.Left.Root = cid.Undef }},
		{"uncovered nonzero", func(in *ConflictLatchInput) { in.Left.Covered = false }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			input := conflictStateTestInput(t, archiveID, "all", 1, digest)
			tc.edit(&input)
			if _, err := NewConflictLatchRequest(input); err == nil {
				t.Fatal("invalid conflict request accepted")
			}
		})
	}

	validRequest := ConflictClearRequest{
		ArchiveID: archiveID, Head: "all", EvidenceID: sourceStateTestValue(94),
		ClearedAt: time.Unix(1, 0), Operator: "operator",
	}
	for _, tc := range []struct {
		name string
		edit func(*ConflictClearRequest)
	}{
		{"zero evidence", func(in *ConflictClearRequest) { in.EvidenceID = [32]byte{} }},
		{"zero time", func(in *ConflictClearRequest) { in.ClearedAt = time.Time{} }},
		{"empty operator", func(in *ConflictClearRequest) { in.Operator = "" }},
		{"long operator", func(in *ConflictClearRequest) { in.Operator = strings.Repeat("a", maxConflictOperatorBytes+1) }},
		{"long note", func(in *ConflictClearRequest) { in.Note = strings.Repeat("n", maxConflictClearNoteBytes+1) }},
		{"nul note", func(in *ConflictClearRequest) { in.Note = "bad\x00note" }},
	} {
		t.Run("clear "+tc.name, func(t *testing.T) {
			request := validRequest
			tc.edit(&request)
			if _, err := normalizeConflictClearRequest(request); err == nil {
				t.Fatal("invalid clear request accepted")
			}
		})
	}
}

func TestConflictEvidenceCanonicalOrderingDoesNotDependOnPairOrientation(t *testing.T) {
	input := conflictStateTestInput(t, sourceStateTestArchive(95), "all", 3, sourceStateTestValue(96))
	forward, err := NewConflictLatchRequest(input)
	if err != nil {
		t.Fatal(err)
	}
	input.Left, input.Right = input.Right, input.Left
	reverse, err := NewConflictLatchRequest(input)
	if err != nil {
		t.Fatal(err)
	}
	a, err := recordFromRequest(forward, 1)
	if err != nil {
		t.Fatal(err)
	}
	b, err := recordFromRequest(reverse, 1)
	if err != nil {
		t.Fatal(err)
	}
	if a != b {
		t.Fatalf("pair orientation changed canonical evidence:\n%+v\n%+v", a, b)
	}
	encodedA, _ := encodeConflictRecord(a)
	encodedB, _ := encodeConflictRecord(b)
	if !bytes.Equal(encodedA, encodedB) {
		t.Fatal("pair orientation changed durable bytes")
	}
}
