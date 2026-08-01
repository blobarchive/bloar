package follow

import (
	"encoding/binary"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"

	"github.com/blobarchive/bloar/server"
)

func checkpointV4TestArchive(seed byte) server.ArchiveID {
	var id server.ArchiveID
	for i := range id {
		id[i] = seed + byte(i)
	}
	return id
}

func checkpointV4TestOverlay(t *testing.T) server.HeadEntry {
	t.Helper()
	syncedTo := uint64(10)
	return server.HeadEntry{
		Name: "arb1-filtered", Root: epochTestCID(t, 93).String(), OriginSlot: 0, SyncedTo: &syncedTo,
		SegBits: 13, FanoutBits: 5, DirDepth: 3, Manifest: epochTestCID(t, 94).String(),
	}
}

func TestCheckpointV4RoundTripRetainsSourceAndSignedClaim(t *testing.T) {
	finalized, mutable := checkpointV3TestEntries(t)
	floor := checkpointV3TestFloor(31)
	archiveID := checkpointV4TestArchive(11)
	updatedAt := time.Unix(1_900_000_000, 0).UTC()
	overlay := checkpointV4TestOverlay(t)
	cp, err := makeCheckpointV4(checkpointV3TestNet, archiveID, "writer-a", true, &mutable, &finalized, &overlay, updatedAt, floor)
	if err != nil {
		t.Fatal(err)
	}
	if cp.version != checkpointVersionV4 || cp.archiveID != archiveID || cp.sourceID != "writer-a" || !cp.selected {
		t.Fatalf("constructed v4 provenance = %+v", cp)
	}

	encoded, err := encodeCheckpoint(cp)
	if err != nil {
		t.Fatal(err)
	}
	if encoded[0] != checkpointVersionV4 {
		t.Fatalf("checkpoint version = %d, want %d", encoded[0], checkpointVersionV4)
	}
	back, err := decodeCheckpoint(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if back.version != checkpointVersionV4 || back.archiveID != archiveID || back.sourceID != "writer-a" ||
		back.net != checkpointV3TestNet || back.authority != floor.authority || back.revision != floor.revision ||
		back.digest != floor.digest || !back.updatedAt.Equal(updatedAt) || back.root != cp.root ||
		back.syncedTo != cp.syncedTo || back.kind != server.UnfinalizedMutable {
		t.Fatalf("decoded v4 checkpoint = %+v", back)
	}
	if !reflect.DeepEqual(back.published, cp.published) || !reflect.DeepEqual(back.handoff, cp.handoff) ||
		!reflect.DeepEqual(back.overlay, cp.overlay) {
		t.Fatalf("decoded v4 proof differs:\n got published=%+v handoff=%+v overlay=%+v\nwant published=%+v handoff=%+v overlay=%+v",
			back.published, back.handoff, back.overlay, cp.published, cp.handoff, cp.overlay)
	}
	// Construction owns the overlay witness just as it owns the signed pair.
	*overlay.SyncedTo = 999
	if *cp.overlay.SyncedTo != 10 {
		t.Fatal("checkpoint v4 retained an alias into the caller's overlay witness")
	}
}

func TestCheckpointV4WithdrawalRetainsLastSelectedSourceProof(t *testing.T) {
	finalized, mutable := checkpointV3TestEntries(t)
	overlay := checkpointV4TestOverlay(t)
	archiveID := checkpointV4TestArchive(21)
	selected, err := makeCheckpointV4(checkpointV3TestNet, archiveID, "writer-a", true, &mutable, &finalized, &overlay,
		time.Unix(10, 0).UTC(), checkpointV3TestFloor(4))
	if err != nil {
		t.Fatal(err)
	}
	withdrawalFloor := checkpointV3TestFloor(5)
	withdrawn, err := makeCheckpointV4(checkpointV3TestNet, archiveID, "writer-a", false, selected.published, selected.handoff, selected.overlay,
		time.Unix(20, 0).UTC(), withdrawalFloor)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := encodeCheckpoint(withdrawn)
	if err != nil {
		t.Fatal(err)
	}
	back, err := decodeCheckpoint(raw)
	if err != nil {
		t.Fatal(err)
	}
	if back.selected || back.archiveID != archiveID || back.sourceID != "writer-a" || back.revision != withdrawalFloor.revision ||
		back.authority != withdrawalFloor.authority || back.digest != withdrawalFloor.digest || back.root != selected.root ||
		!reflect.DeepEqual(back.published, selected.published) || !reflect.DeepEqual(back.handoff, selected.handoff) ||
		!reflect.DeepEqual(back.overlay, selected.overlay) {
		t.Fatalf("decoded v4 withdrawal lost source proof or baseline: %+v", back)
	}
}

func TestCheckpointV4RejectsMissingOrNonCanonicalProvenance(t *testing.T) {
	finalized, _ := checkpointV3TestEntries(t)
	floor := checkpointV3TestFloor(8)
	archiveID := checkpointV4TestArchive(31)

	tests := []struct {
		name      string
		archiveID server.ArchiveID
		sourceID  string
		floor     authorityFloor
		want      string
	}{
		{name: "empty archive", sourceID: "writer-a", floor: floor, want: "empty archive ID"},
		{name: "empty source", archiveID: archiveID, floor: floor, want: "invalid source ID"},
		{name: "uppercase source", archiveID: archiveID, sourceID: "Writer-A", floor: floor, want: "lowercase ASCII"},
		{name: "oversized source", archiveID: archiveID, sourceID: strings.Repeat("a", maxSourceIDBytes+1), floor: floor, want: "1..63"},
		{name: "zero revision", archiveID: archiveID, sourceID: "writer-a", floor: authorityFloor{authority: floor.authority}, want: "revision 0"},
		{name: "zero authority", archiveID: archiveID, sourceID: "writer-a", floor: authorityFloor{revision: 1}, want: "empty publication authority"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := makeCheckpointV4(checkpointV3TestNet, tt.archiveID, tt.sourceID, true, &finalized, nil, nil,
				time.Unix(1, 0).UTC(), tt.floor)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error = %v, want substring %q", err, tt.want)
			}
		})
	}
}

func TestCheckpointV4OverlayRequiresOneCoveredFinalizedClaim(t *testing.T) {
	finalized, mutable := checkpointV3TestEntries(t)
	archiveID := checkpointV4TestArchive(35)
	floor := checkpointV3TestFloor(12)
	overlay := checkpointV4TestOverlay(t)

	tests := []struct {
		name      string
		published *server.HeadEntry
		handoff   *server.HeadEntry
		overlay   server.HeadEntry
		want      string
	}{
		{name: "finalized publication", published: &finalized, overlay: overlay, want: "finalized or empty publication"},
		{name: "mutable overlay", published: &mutable, handoff: &finalized, overlay: mutable, want: "not finalized-monotonic"},
		{name: "uncovered overlay", published: &mutable, handoff: &finalized, overlay: overlay, want: "is uncovered"},
		{name: "invalid finalized contract", published: &mutable, handoff: &finalized, overlay: overlay, want: "overlay publication contract is invalid"},
		{name: "invalid root", published: &mutable, handoff: &finalized, overlay: overlay, want: "undecodable root"},
		{name: "invalid manifest", published: &mutable, handoff: &finalized, overlay: overlay, want: "undecodable manifest tip"},
		{name: "insufficient boundary coverage", published: &mutable, handoff: &finalized, overlay: overlay, want: "before mutable head"},
	}
	tests[2].overlay.SyncedTo = nil
	windowStart := uint64(0)
	tests[3].overlay.WindowStart = &windowStart
	tests[4].overlay.Root = "not-a-cid"
	tests[5].overlay.Manifest = "not-a-cid"
	lowCoverage := uint64(8)
	tests[6].overlay.SyncedTo = &lowCoverage

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := makeCheckpointV4(checkpointV3TestNet, archiveID, "writer-a", true, tt.published, tt.handoff, &tt.overlay,
				time.Unix(1, 0).UTC(), floor)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error = %v, want substring %q", err, tt.want)
			}
		})
	}
}

func TestCheckpointV4DecodeFailsClosedOnCorruptIdentityAndLayout(t *testing.T) {
	finalized, _ := checkpointV3TestEntries(t)
	cp, err := makeCheckpointV4(checkpointV3TestNet, checkpointV4TestArchive(41), "writer-a", true, &finalized, nil, nil,
		time.Unix(1, 0).UTC(), checkpointV3TestFloor(9))
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := encodeCheckpoint(cp)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name string
		edit func([]byte) []byte
		want string
	}{
		{name: "unknown flags", edit: func(b []byte) []byte { b[1] |= 0x80; return b }, want: "unknown flags"},
		{name: "reserved byte", edit: func(b []byte) []byte { b[5] = 1; return b }, want: "reserved"},
		{name: "zero archive", edit: func(b []byte) []byte { clear(b[24:56]); return b }, want: "empty archive ID"},
		{name: "invalid source", edit: func(b []byte) []byte { b[132] = 'A'; return b }, want: "invalid source ID"},
		{name: "zero source length", edit: func(b []byte) []byte { b[4] = 0; return b }, want: "do not match"},
		{name: "oversized entry", edit: func(b []byte) []byte {
			binary.BigEndian.PutUint32(b[120:124], maxCheckpointHeadEntryBytes+1)
			return b
		}, want: "oversized"},
		{name: "oversized overlay", edit: func(b []byte) []byte {
			binary.BigEndian.PutUint32(b[128:132], maxCheckpointHeadEntryBytes+1)
			return b
		}, want: "oversized"},
		{name: "overlay flag without bytes", edit: func(b []byte) []byte { b[1] |= 8; return b }, want: "flags disagree"},
		{name: "trailing byte", edit: func(b []byte) []byte { return append(b, 0) }, want: "do not match"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bad := tt.edit(append([]byte(nil), encoded...))
			if _, err := decodeCheckpoint(bad); err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error = %v, want substring %q", err, tt.want)
			}
		})
	}

	// The overlay uses the same canonical HeadEntry representation as the signed
	// line and handoff, but its boundary is independently length-delimited.
	mutableFinalized, mutable := checkpointV3TestEntries(t)
	overlay := checkpointV4TestOverlay(t)
	withOverlay, err := makeCheckpointV4(checkpointV3TestNet, checkpointV4TestArchive(42), "writer-a", true,
		&mutable, &mutableFinalized, &overlay, time.Unix(2, 0).UTC(), checkpointV3TestFloor(10))
	if err != nil {
		t.Fatal(err)
	}
	raw, err := encodeCheckpoint(withOverlay)
	if err != nil {
		t.Fatal(err)
	}
	sourceLen := int(raw[4])
	netLen := int(binary.BigEndian.Uint16(raw[2:4]))
	entryLen := int(binary.BigEndian.Uint32(raw[120:124]))
	witnessLen := int(binary.BigEndian.Uint32(raw[124:128]))
	overlayLen := int(binary.BigEndian.Uint32(raw[128:132]))
	overlayStart := 132 + sourceLen + netLen + entryLen + witnessLen
	nonCanonical := make([]byte, 0, len(raw)+1)
	nonCanonical = append(nonCanonical, raw[:overlayStart]...)
	nonCanonical = append(nonCanonical, ' ')
	nonCanonical = append(nonCanonical, raw[overlayStart:]...)
	binary.BigEndian.PutUint32(nonCanonical[128:132], uint32(overlayLen+1))
	if _, err := decodeCheckpoint(nonCanonical); err == nil || !strings.Contains(err.Error(), "canonical") {
		t.Fatalf("non-canonical overlay error = %v", err)
	}
}

func TestCheckpointV1ThroughV3NeverInventSourceProvenance(t *testing.T) {
	finalized, _ := checkpointV3TestEntries(t)
	v1 := checkpoint{
		root: epochTestCID(t, 91), syncedTo: 1, updatedAt: time.Unix(1, 0).UTC(), kind: server.FinalizedMonotonic,
	}
	v2 := checkpoint{
		root: epochTestCID(t, 92), syncedTo: 2, updatedAt: time.Unix(2, 0).UTC(), kind: server.FinalizedMonotonic,
		revision: 2, authority: checkpointV3TestFloor(2).authority, digest: checkpointV3TestFloor(2).digest,
	}
	v3, err := makeCheckpointV3(checkpointV3TestNet, true, &finalized, nil, time.Unix(3, 0).UTC(), checkpointV3TestFloor(3))
	if err != nil {
		t.Fatal(err)
	}
	legacy := []checkpoint{v1, v2, v3}
	for _, cp := range legacy {
		raw, err := encodeCheckpoint(cp)
		if err != nil {
			t.Fatal(err)
		}
		back, err := decodeCheckpoint(raw)
		if err != nil {
			t.Fatal(err)
		}
		if !back.archiveID.IsZero() || back.sourceID != "" || back.overlay != nil {
			t.Fatalf("checkpoint v%d invented source provenance: archive=%s source=%q", back.version, back.archiveID, back.sourceID)
		}
	}
	for _, cp := range legacy {
		cp.overlay = &finalized
		if _, err := encodeCheckpoint(cp); err == nil || !strings.Contains(err.Error(), "carries") {
			t.Fatalf("checkpoint v%d accepted a v4 overlay witness: %v", cp.version, err)
		}
	}

	v3.archiveID = checkpointV4TestArchive(51)
	v3.sourceID = "writer-a"
	if _, err := encodeCheckpoint(v3); err == nil || !strings.Contains(err.Error(), "carries source-set provenance") {
		t.Fatalf("v3 with v4 provenance error = %v", err)
	}
}

func TestCheckpointV4StagesAtomicSnapshotFromDifferentSources(t *testing.T) {
	kv, err := pebble.Open(t.TempDir(), &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = kv.Close() })
	st := &state{kv: kv}
	finalized, mutable := checkpointV3TestEntries(t)
	archiveID := checkpointV4TestArchive(61)
	cpA, err := makeCheckpointV4(checkpointV3TestNet, archiveID, "writer-a", true, &finalized, nil, nil,
		time.Unix(10, 0).UTC(), checkpointV3TestFloor(10))
	if err != nil {
		t.Fatal(err)
	}
	floorB := checkpointV3TestFloor(11)
	floorB.authority[0] ^= 0xff
	cpB, err := makeCheckpointV4(checkpointV3TestNet, archiveID, "writer-b", true, &mutable, &finalized, nil,
		time.Unix(11, 0).UTC(), floorB)
	if err != nil {
		t.Fatal(err)
	}

	batch := kv.NewBatch()
	defer batch.Close()
	if err := st.stageCheckpoint(batch, finalized.Name, cpA); err != nil {
		t.Fatal(err)
	}
	if err := st.stageCheckpoint(batch, mutable.Name, cpB); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := st.checkpoint(finalized.Name); err != nil || ok {
		t.Fatalf("first uncommitted checkpoint visible: ok=%t err=%v", ok, err)
	}
	if _, ok, err := st.checkpoint(mutable.Name); err != nil || ok {
		t.Fatalf("second uncommitted checkpoint visible: ok=%t err=%v", ok, err)
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		t.Fatal(err)
	}
	gotA, okA, errA := st.checkpoint(finalized.Name)
	gotB, okB, errB := st.checkpoint(mutable.Name)
	if errA != nil || errB != nil || !okA || !okB || gotA.sourceID != "writer-a" || gotB.sourceID != "writer-b" ||
		gotA.archiveID != archiveID || gotB.archiveID != archiveID || gotA.authority == gotB.authority {
		t.Fatalf("mixed-source atomic snapshot = A(%+v ok=%t err=%v), B(%+v ok=%t err=%v)", gotA, okA, errA, gotB, okB, errB)
	}
}
