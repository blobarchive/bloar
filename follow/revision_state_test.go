package follow

import (
	"bytes"
	"encoding/binary"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"

	"github.com/blobarchive/bloar/server"
)

const checkpointV3TestNet = "checkpoint-testnet"

func TestLegacyCheckpointEncodingRemainsV1ByteForByte(t *testing.T) {
	root := epochTestCID(t, 81)
	tip := epochTestCID(t, 82)
	cp := checkpoint{
		root: root, syncedTo: 1234, manifestTip: tip,
		updatedAt: time.Unix(1_700_000_000, 0).UTC(), kind: server.FinalizedMonotonic,
	}

	// Reconstruct the frozen v1 layout independently rather than round-tripping
	// through the implementation under test.
	want := []byte{1, 1}
	want = binary.BigEndian.AppendUint64(want, cp.syncedTo)
	want = binary.BigEndian.AppendUint64(want, uint64(cp.updatedAt.Unix()))
	want = binary.BigEndian.AppendUint16(want, uint16(len(root.Bytes())))
	want = append(want, root.Bytes()...)
	want = append(want, tip.Bytes()...)
	got, err := encodeCheckpoint(cp)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("legacy checkpoint bytes changed:\n got %x\nwant %x", got, want)
	}
	back, err := decodeCheckpoint(want)
	if err != nil {
		t.Fatal(err)
	}
	if back.root != root || back.syncedTo != cp.syncedTo || back.manifestTip != tip ||
		!back.updatedAt.Equal(cp.updatedAt) || back.kind != server.FinalizedMonotonic || back.revision != 0 {
		t.Fatalf("decoded legacy checkpoint = %+v", back)
	}
}

func TestRevisionedCheckpointV2RoundTrip(t *testing.T) {
	cp := checkpoint{
		root: epochTestCID(t, 83), syncedTo: 255, updatedAt: time.Unix(10, 0).UTC(),
		kind: server.UnfinalizedMutable, revision: 9, windowStart: 224,
	}
	for i := range cp.authority {
		cp.authority[i] = byte(i + 1)
		cp.digest[i] = byte(255 - i)
	}
	encoded, err := encodeCheckpoint(cp)
	if err != nil {
		t.Fatal(err)
	}
	if encoded[0] != 2 {
		t.Fatalf("revisioned checkpoint version = %d, want 2", encoded[0])
	}
	back, err := decodeCheckpoint(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if back.root != cp.root || back.syncedTo != cp.syncedTo || back.kind != cp.kind ||
		back.authority != cp.authority || back.revision != cp.revision || back.digest != cp.digest ||
		back.windowStart != cp.windowStart || !back.updatedAt.Equal(cp.updatedAt) {
		t.Fatalf("v2 checkpoint changed on round trip:\n got %+v\nwant %+v", back, cp)
	}
	if back.version != checkpointVersionV2 || !back.selected || back.published != nil || back.handoff != nil {
		t.Fatalf("decoded v2 proof state = version %d selected=%t published=%v handoff=%v; v2 must remain distinguishable from proof-complete v3",
			back.version, back.selected, back.published, back.handoff)
	}
}

func checkpointV3TestEntries(t *testing.T) (server.HeadEntry, server.HeadEntry) {
	t.Helper()
	finalizedTo := uint64(10)
	finalized := server.HeadEntry{
		Name: "arb1", Root: epochTestCID(t, 84).String(), OriginSlot: 0, SyncedTo: &finalizedTo,
		SegBits: 13, FanoutBits: 5, DirDepth: 3, Manifest: epochTestCID(t, 85).String(),
	}
	windowStart, mutableTo, sourceFinalized := uint64(11), uint64(12), uint64(10)
	handoffTo := finalizedTo
	mutable := server.HeadEntry{
		Name: "live", Root: epochTestCID(t, 86).String(), OriginSlot: windowStart, SyncedTo: &mutableTo,
		SegBits: 13, FanoutBits: 5, DirDepth: 3,
		Kind: server.UnfinalizedMutable, WindowStart: &windowStart,
		SourceHeadRoot:      "0x" + strings.Repeat("1", 64),
		SourceFinalizedSlot: &sourceFinalized,
		SourceFinalizedRoot: "0x" + strings.Repeat("2", 64),
		HandoffHead:         finalized.Name,
		HandoffRoot:         finalized.Root,
		HandoffSyncedTo:     &handoffTo,
	}
	return finalized, mutable
}

func checkpointV3TestFloor(revision uint64) authorityFloor {
	floor := authorityFloor{revision: revision}
	for i := range floor.authority {
		floor.authority[i] = byte(i + 1)
		floor.digest[i] = byte(255 - i)
	}
	return floor
}

func TestCheckpointV3SelectedMutableRoundTripRetainsExactPair(t *testing.T) {
	finalized, mutable := checkpointV3TestEntries(t)
	floor := checkpointV3TestFloor(17)
	updatedAt := time.Unix(1_800_000_000, 0).UTC()
	cp, err := makeCheckpointV3(checkpointV3TestNet, true, &mutable, &finalized, updatedAt, floor)
	if err != nil {
		t.Fatal(err)
	}
	if cp.version != checkpointVersionV3 || !cp.selected || cp.net != checkpointV3TestNet || cp.root.String() != mutable.Root ||
		cp.syncedTo != *mutable.SyncedTo || cp.kind != server.UnfinalizedMutable || cp.windowStart != *mutable.WindowStart ||
		cp.manifestTip.Defined() {
		t.Fatalf("constructed v3 projection = %+v", cp)
	}

	// Construction owns a deep copy: changing the transport document after
	// admission cannot rewrite the durable proof queued for commit.
	originalMutableTo := *mutable.SyncedTo
	originalHandoffTo := *finalized.SyncedTo
	*mutable.SyncedTo = 999
	*finalized.SyncedTo = 999
	if *cp.published.SyncedTo != originalMutableTo || *cp.handoff.SyncedTo != originalHandoffTo {
		t.Fatal("checkpoint v3 retained aliases into the caller's publication document")
	}

	encoded, err := encodeCheckpoint(cp)
	if err != nil {
		t.Fatal(err)
	}
	if encoded[0] != checkpointVersionV3 {
		t.Fatalf("checkpoint version = %d, want %d", encoded[0], checkpointVersionV3)
	}
	back, err := decodeCheckpoint(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if back.version != checkpointVersionV3 || !back.selected || back.net != checkpointV3TestNet || back.authority != floor.authority ||
		back.revision != floor.revision || back.digest != floor.digest || !back.updatedAt.Equal(updatedAt) {
		t.Fatalf("decoded v3 document identity = %+v", back)
	}
	if !reflect.DeepEqual(back.published, cp.published) || !reflect.DeepEqual(back.handoff, cp.handoff) {
		t.Fatalf("decoded v3 pair differs:\n got published=%+v handoff=%+v\nwant published=%+v handoff=%+v",
			back.published, back.handoff, cp.published, cp.handoff)
	}
}

func TestCheckpointV3PreservesExplicitZeroProofSlots(t *testing.T) {
	zero := uint64(0)
	finalized := server.HeadEntry{
		Name: "genesis", Root: epochTestCID(t, 87).String(), SyncedTo: &zero,
		SegBits: 13, FanoutBits: 5, DirDepth: 3,
	}
	mutable := server.HeadEntry{
		Name: "live-genesis", Root: epochTestCID(t, 88).String(), SyncedTo: &zero,
		SegBits: 13, FanoutBits: 5, DirDepth: 3,
		Kind: server.UnfinalizedMutable, WindowStart: &zero,
		SourceHeadRoot:      "0x" + strings.Repeat("3", 64),
		SourceFinalizedSlot: &zero,
		SourceFinalizedRoot: "0x" + strings.Repeat("4", 64),
		HandoffHead:         finalized.Name,
		HandoffRoot:         finalized.Root,
		HandoffSyncedTo:     &zero,
	}
	cp, err := makeCheckpointV3(checkpointV3TestNet, true, &mutable, &finalized, time.Unix(1, 0).UTC(), checkpointV3TestFloor(1))
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := encodeCheckpoint(cp)
	if err != nil {
		t.Fatal(err)
	}
	back, err := decodeCheckpoint(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if back.published.SyncedTo == nil || back.published.WindowStart == nil || back.published.SourceFinalizedSlot == nil ||
		back.published.HandoffSyncedTo == nil || *back.published.SyncedTo != 0 || *back.published.WindowStart != 0 ||
		*back.published.SourceFinalizedSlot != 0 || *back.published.HandoffSyncedTo != 0 || back.handoff.SyncedTo == nil ||
		*back.handoff.SyncedTo != 0 {
		t.Fatalf("explicit zero proof fields did not round-trip: published=%+v handoff=%+v", back.published, back.handoff)
	}
}

func TestCheckpointV3TombstoneRetainsFloorAndImmutableBaseline(t *testing.T) {
	finalized, mutable := checkpointV3TestEntries(t)
	selected, err := makeCheckpointV3(checkpointV3TestNet, true, &mutable, &finalized, time.Unix(10, 0).UTC(), checkpointV3TestFloor(3))
	if err != nil {
		t.Fatal(err)
	}
	withdrawalFloor := checkpointV3TestFloor(4)
	tombstone, err := makeCheckpointV3(checkpointV3TestNet, false, selected.published, selected.handoff, time.Unix(20, 0).UTC(), withdrawalFloor)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := encodeCheckpoint(tombstone)
	if err != nil {
		t.Fatal(err)
	}
	back, err := decodeCheckpoint(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if back.selected || back.revision != 4 || back.authority != withdrawalFloor.authority || back.digest != withdrawalFloor.digest {
		t.Fatalf("decoded tombstone selection/document = %+v", back)
	}
	if back.root != selected.root || back.syncedTo != selected.syncedTo || back.kind != selected.kind ||
		back.windowStart != selected.windowStart || !reflect.DeepEqual(back.published, selected.published) ||
		!reflect.DeepEqual(back.handoff, selected.handoff) {
		t.Fatalf("tombstone lost retained baseline:\n got %+v\nwant %+v", back, selected)
	}

	// A head omitted before its first selection still records the authenticated
	// document generation, but correctly has no invented root or floor.
	empty, err := makeCheckpointV3(checkpointV3TestNet, false, nil, nil, time.Unix(21, 0).UTC(), checkpointV3TestFloor(5))
	if err != nil {
		t.Fatal(err)
	}
	emptyBytes, err := encodeCheckpoint(empty)
	if err != nil {
		t.Fatal(err)
	}
	emptyBack, err := decodeCheckpoint(emptyBytes)
	if err != nil {
		t.Fatal(err)
	}
	if emptyBack.selected || emptyBack.published != nil || emptyBack.handoff != nil || emptyBack.root.Defined() || emptyBack.revision != 5 {
		t.Fatalf("empty tombstone = %+v", emptyBack)
	}
}

func TestCheckpointV3RejectsIncompleteOrComposedProofs(t *testing.T) {
	finalized, mutable := checkpointV3TestEntries(t)
	floor := checkpointV3TestFloor(9)
	tests := []struct {
		name      string
		selected  bool
		published *server.HeadEntry
		handoff   *server.HeadEntry
		floor     authorityFloor
		want      string
	}{
		{name: "selected without entry", selected: true, floor: floor, want: "no authenticated publication entry"},
		{name: "mutable without witness", selected: true, published: &mutable, floor: floor, want: "no same-document handoff witness"},
		{name: "finalized with witness", selected: true, published: &finalized, handoff: &finalized, floor: floor, want: "carries a handoff witness"},
	}
	if _, err := makeCheckpointV3("", false, nil, nil, time.Unix(1, 0).UTC(), floor); err == nil || !strings.Contains(err.Error(), "network") {
		t.Fatalf("empty network error = %v", err)
	}
	wrongWitness := finalized
	wrongWitness.Root = epochTestCID(t, 89).String()
	tests = append(tests, struct {
		name      string
		selected  bool
		published *server.HeadEntry
		handoff   *server.HeadEntry
		floor     authorityFloor
		want      string
	}{name: "composed mutable pair", selected: true, published: &mutable, handoff: &wrongWitness, floor: floor, want: "does not match finalized head"})
	zeroRevision := floor
	zeroRevision.revision = 0
	tests = append(tests, struct {
		name      string
		selected  bool
		published *server.HeadEntry
		handoff   *server.HeadEntry
		floor     authorityFloor
		want      string
	}{name: "zero revision", selected: false, floor: zeroRevision, want: "revision 0"})

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := makeCheckpointV3(checkpointV3TestNet, tt.selected, tt.published, tt.handoff, time.Unix(1, 0).UTC(), tt.floor)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error = %v, want substring %q", err, tt.want)
			}
		})
	}
}

func TestCheckpointV3DecodeFailsClosedOnFlagsLengthsAndNonCanonicalEntry(t *testing.T) {
	finalized, _ := checkpointV3TestEntries(t)
	cp, err := makeCheckpointV3(checkpointV3TestNet, true, &finalized, nil, time.Unix(1, 0).UTC(), checkpointV3TestFloor(2))
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := encodeCheckpoint(cp)
	if err != nil {
		t.Fatal(err)
	}

	badFlags := append([]byte(nil), encoded...)
	badFlags[1] |= 0x80
	if _, err := decodeCheckpoint(badFlags); err == nil || !strings.Contains(err.Error(), "unknown flags") {
		t.Fatalf("unknown flags error = %v", err)
	}
	badLength := append([]byte(nil), encoded...)
	binary.BigEndian.PutUint32(badLength[84:88], uint32(len(encoded)))
	if _, err := decodeCheckpoint(badLength); err == nil || !strings.Contains(err.Error(), "do not match") {
		t.Fatalf("bad length error = %v", err)
	}
	entryBytes, err := encodeCheckpointHeadEntry(&finalized)
	if err != nil {
		t.Fatal(err)
	}
	nonCanonical := append([]byte{' '}, entryBytes...)
	if _, err := decodeCheckpointHeadEntry(nonCanonical); err == nil || !strings.Contains(err.Error(), "canonical") {
		t.Fatalf("non-canonical entry error = %v", err)
	}
}

func TestCheckpointV3StagesWholeGenerationInCallerBatch(t *testing.T) {
	kv, err := pebble.Open(t.TempDir(), &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = kv.Close() })
	st := &state{kv: kv}
	finalized, mutable := checkpointV3TestEntries(t)
	floor := checkpointV3TestFloor(21)
	finalizedCP, err := makeCheckpointV3(checkpointV3TestNet, true, &finalized, nil, time.Unix(30, 0).UTC(), floor)
	if err != nil {
		t.Fatal(err)
	}
	mutableCP, err := makeCheckpointV3(checkpointV3TestNet, true, &mutable, &finalized, time.Unix(30, 0).UTC(), floor)
	if err != nil {
		t.Fatal(err)
	}

	b := kv.NewBatch()
	defer b.Close()
	if err := st.stageCheckpoint(b, finalized.Name, finalizedCP); err != nil {
		t.Fatal(err)
	}
	if err := st.stageCheckpoint(b, mutable.Name, mutableCP); err != nil {
		t.Fatal(err)
	}
	if err := st.stageAuthorityFloor(b, floor); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := st.checkpoint(finalized.Name); err != nil || ok {
		t.Fatalf("uncommitted finalized checkpoint visible: ok=%t err=%v", ok, err)
	}
	if _, ok, err := st.checkpoint(mutable.Name); err != nil || ok {
		t.Fatalf("uncommitted mutable checkpoint visible: ok=%t err=%v", ok, err)
	}
	if err := b.Commit(pebble.Sync); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{finalized.Name, mutable.Name} {
		got, ok, err := st.checkpoint(name)
		if err != nil || !ok || got.version != checkpointVersionV3 || !got.selected || got.revision != floor.revision {
			t.Fatalf("committed checkpoint %q = %+v, ok=%t err=%v", name, got, ok, err)
		}
	}
	gotFloor, ok, err := st.authorityFloor(floor.authority)
	if err != nil || !ok || gotFloor != floor {
		t.Fatalf("committed authority floor = %+v, ok=%t err=%v; want %+v", gotFloor, ok, err, floor)
	}
}

func TestDualCandidateRevisionOrderingIgnoresClock(t *testing.T) {
	var authority [32]byte
	authority[0] = 1
	a := &resolved{source: "https", authority: authority, revisioned: true, revision: 10,
		updatedAt: time.Unix(1000, 0), digest: [32]byte{1}}
	b := &resolved{source: "ipns", authority: authority, revisioned: true, revision: 11,
		updatedAt: time.Unix(1, 0), digest: [32]byte{2}}
	got, err := fresherCandidate(a, b)
	if err != nil || got != b {
		t.Fatalf("fresher revision = %p err=%v, want %p", got, err, b)
	}

	b.revision, b.digest = a.revision, [32]byte{3}
	if _, err := fresherCandidate(a, b); err == nil {
		t.Fatal("equal-revision different-digest candidates were selected instead of reported as equivocation")
	}

	// The legacy comparator is intentionally unchanged.
	a.revisioned, b.revisioned = false, false
	a.updatedAt, b.updatedAt = time.Unix(1, 0), time.Unix(2, 0)
	got, err = fresherCandidate(a, b)
	if err != nil || got != b {
		t.Fatalf("legacy fresher clock = %p err=%v, want %p", got, err, b)
	}
}

func TestCrossAuthorityCandidatesRequireDNSLinkHandoff(t *testing.T) {
	oldHTTPS := &resolved{
		source: "https", authority: [32]byte{1}, revisioned: true, revision: 99,
		updatedAt: time.Unix(4_000_000_000, 0), digest: [32]byte{1},
	}
	rotatedIPNS := &resolved{
		source: "ipns", authority: [32]byte{2}, revisioned: true, revision: 1,
		updatedAt: time.Unix(2_000_000_000, 0), digest: [32]byte{2},
		delegation: &delegation{pubkey: bytes.Repeat([]byte{2}, 32)},
	}

	got, err := fresherCandidate(oldHTTPS, rotatedIPNS)
	if err != nil || got != rotatedIPNS {
		t.Fatalf("DNSLink handoff selected %p err=%v, want delegated candidate %p", got, err, rotatedIPNS)
	}
	got, err = fresherCandidate(rotatedIPNS, oldHTTPS)
	if err != nil || got != rotatedIPNS {
		t.Fatalf("reverse DNSLink handoff selected %p err=%v, want delegated candidate %p", got, err, rotatedIPNS)
	}

	rotatedIPNS.delegation = nil
	if _, err := fresherCandidate(oldHTTPS, rotatedIPNS); err == nil || !strings.Contains(err.Error(), "incomparable") {
		t.Fatalf("unordered cross-authority candidates error = %v, want incomparable refusal", err)
	}
	oldHTTPS.delegation = &delegation{pubkey: bytes.Repeat([]byte{1}, 32)}
	rotatedIPNS.delegation = &delegation{pubkey: bytes.Repeat([]byte{2}, 32)}
	if _, err := fresherCandidate(oldHTTPS, rotatedIPNS); err == nil || !strings.Contains(err.Error(), "incomparable") {
		t.Fatalf("two delegated authorities error = %v, want incomparable refusal", err)
	}
	oldHTTPS.delegation = nil
	mixedLegacyHandoff := &resolved{
		source: "ipns", authority: [32]byte{5}, updatedAt: time.Unix(1, 0),
		delegation: &delegation{pubkey: bytes.Repeat([]byte{5}, 32)},
	}
	if got, err := fresherCandidate(oldHTTPS, mixedLegacyHandoff); err != nil || got != mixedLegacyHandoff {
		t.Fatalf("mixed revisioned/legacy handoff selected %p err=%v, want %p", got, err, mixedLegacyHandoff)
	}
	mixedLegacyHandoff.delegation = nil
	if _, err := fresherCandidate(oldHTTPS, mixedLegacyHandoff); err == nil || !strings.Contains(err.Error(), "incomparable") {
		t.Fatalf("unordered mixed-authority candidates error = %v, want incomparable refusal", err)
	}

	// The revisionless protocol predates signer-local revisions and deliberately
	// retains its global updated_at order across DNS signer rotation.
	legacyOld := &resolved{source: "https", authority: [32]byte{3}, updatedAt: time.Unix(1, 0)}
	legacyNew := &resolved{source: "ipns", authority: [32]byte{4}, updatedAt: time.Unix(2, 0)}
	if got, err := fresherCandidate(legacyOld, legacyNew); err != nil || got != legacyNew {
		t.Fatalf("legacy cross-authority clock selected %p err=%v, want %p", got, err, legacyNew)
	}
}
