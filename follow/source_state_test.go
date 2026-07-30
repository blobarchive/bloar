package follow

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/ipfs/boxo/ipns"

	"github.com/blobarchive/bloar/pinning"
	"github.com/blobarchive/bloar/server"
)

func openSourceStateTestDB(t *testing.T) *state {
	t.Helper()
	kv, err := pebble.Open(t.TempDir(), &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = kv.Close() })
	return &state{kv: kv}
}

func sourceStateTestArchive(seed byte) server.ArchiveID {
	var id server.ArchiveID
	for i := range id {
		id[i] = seed + byte(i)
	}
	return id
}

func sourceStateTestValue(seed byte) [32]byte {
	var value [32]byte
	for i := range value {
		value[i] = seed + byte(i)
	}
	return value
}

func sourceStateTestActivation(archiveID server.ArchiveID, revision uint64, digest [32]byte, bindings ...sourceBinding) sourceSetActivation {
	return sourceSetActivation{
		marker:   sourceSetMarker{archiveID: archiveID, revision: revision, digest: digest},
		bindings: bindings,
	}
}

func commitSourceStateBatch(t *testing.T, s *state, stage func(*pebble.Batch) error) {
	t.Helper()
	b := s.kv.NewBatch()
	defer b.Close()
	if err := stage(b); err != nil {
		t.Fatal(err)
	}
	if err := b.Commit(pebble.Sync); err != nil {
		t.Fatal(err)
	}
}

func requireSourceSetMarker(t *testing.T, s *state, want sourceSetMarker) {
	t.Helper()
	got, ok, err := s.sourceSetMarker()
	if err != nil || !ok || got != want {
		t.Fatalf("source-set marker = %+v ok=%t err=%v, want %+v", got, ok, err, want)
	}
}

func TestSourceIDValidationIsStrictAndBounded(t *testing.T) {
	valid := []string{"a", "0", "writer-a", strings.Repeat("a", maxSourceIDBytes)}
	for _, sourceID := range valid {
		if err := validateSourceID(sourceID); err != nil {
			t.Errorf("valid source ID %q rejected: %v", sourceID, err)
		}
	}
	invalid := []string{
		"", "-writer", "writer-", "Writer", "writer_a", "writer:a", "writer/a", "writer a",
		strings.Repeat("a", maxSourceIDBytes+1),
	}
	for _, sourceID := range invalid {
		if err := validateSourceID(sourceID); err == nil {
			t.Errorf("invalid source ID %q accepted", sourceID)
		}
	}
}

func TestSourceSetBindingValidationIsBoundedAndUnambiguous(t *testing.T) {
	archiveID := sourceStateTestArchive(20)
	digest := sourceStateTestValue(20)
	tests := []struct {
		name     string
		bindings []sourceBinding
	}{
		{"empty", nil},
		{"duplicate ID", []sourceBinding{{sourceID: "writer-a", pubkey: sourceStateTestValue(1)}, {sourceID: "writer-a", pubkey: sourceStateTestValue(2)}}},
		{"duplicate signer", []sourceBinding{{sourceID: "writer-a", pubkey: sourceStateTestValue(1)}, {sourceID: "writer-b", pubkey: sourceStateTestValue(1)}}},
	}
	tooMany := make([]sourceBinding, maxSourceSetBindings+1)
	for i := range tooMany {
		tooMany[i] = sourceBinding{sourceID: "writer-" + string(rune('a'+i)), pubkey: sourceStateTestValue(byte(i + 1))}
	}
	tests = append(tests, struct {
		name     string
		bindings []sourceBinding
	}{"too many", tooMany})
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			activation := sourceStateTestActivation(archiveID, 1, digest, tc.bindings...)
			if _, err := validateSourceSetActivation(activation); err == nil {
				t.Fatal("invalid binding set accepted")
			}
		})
	}
}

func TestSourceSetActivationIsOneAtomicBatch(t *testing.T) {
	s := openSourceStateTestDB(t)
	archiveID := sourceStateTestArchive(1)
	binding := sourceBinding{sourceID: "writer-a", pubkey: sourceStateTestValue(31)}
	activation := sourceStateTestActivation(archiveID, 1, sourceStateTestValue(61), binding)
	ref := sourceRef{archiveID: archiveID, sourceID: binding.sourceID}

	// A fully staged activation is invisible and disappears if the process dies
	// before the batch commit.
	b := s.kv.NewBatch()
	if err := s.stageSourceSetActivation(b, activation); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := s.sourceSetMarker(); err != nil || ok {
		t.Fatalf("uncommitted marker visible: ok=%t err=%v", ok, err)
	}
	if _, ok, err := s.sourceBinding(ref); err != nil || ok {
		t.Fatalf("uncommitted binding visible: ok=%t err=%v", ok, err)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := s.sourceSetMarker(); err != nil || ok {
		t.Fatalf("abandoned marker survived: ok=%t err=%v", ok, err)
	}
	if _, ok, err := s.sourceBinding(ref); err != nil || ok {
		t.Fatalf("abandoned binding survived: ok=%t err=%v", ok, err)
	}

	if err := s.activateSourceSet(activation); err != nil {
		t.Fatal(err)
	}
	requireSourceSetMarker(t, s, activation.marker)
	got, ok, err := s.sourceBinding(ref)
	if err != nil || !ok || got != binding {
		t.Fatalf("binding = %+v ok=%t err=%v, want %+v", got, ok, err, binding)
	}
	if got, ok, err := s.sourceSignerBinding(archiveID, binding.pubkey); err != nil || !ok || got != binding.sourceID {
		t.Fatalf("reverse signer binding = %q ok=%t err=%v, want %q", got, ok, err, binding.sourceID)
	}
}

func TestSourceSetActivationSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	kv, err := pebble.Open(dir, &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	archiveID := sourceStateTestArchive(21)
	binding := sourceBinding{sourceID: "writer-a", pubkey: sourceStateTestValue(24)}
	activation := sourceStateTestActivation(archiveID, 1, sourceStateTestValue(25), binding)
	s := &state{kv: kv}
	if err := s.activateSourceSet(activation); err != nil {
		t.Fatal(err)
	}
	ref := sourceRef{archiveID: archiveID, sourceID: binding.sourceID}
	floor := sourcePublicationFloor{revision: 7, digest: sourceStateTestValue(26)}
	commitSourceStateBatch(t, s, func(batch *pebble.Batch) error { return s.stageSourcePublicationFloor(batch, ref, floor) })
	if err := kv.Close(); err != nil {
		t.Fatal(err)
	}
	kv, err = pebble.Open(dir, &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = kv.Close() })
	s = &state{kv: kv}
	requireSourceSetMarker(t, s, activation.marker)
	if got, ok, err := s.sourceBinding(ref); err != nil || !ok || got != binding {
		t.Fatalf("reopened binding = %+v ok=%t err=%v", got, ok, err)
	}
	if got, ok, err := s.sourceSignerBinding(archiveID, binding.pubkey); err != nil || !ok || got != binding.sourceID {
		t.Fatalf("reopened reverse binding = %q ok=%t err=%v", got, ok, err)
	}
	if got, ok, err := s.sourcePublicationFloor(ref); err != nil || !ok || got != floor {
		t.Fatalf("reopened publication floor = %+v ok=%t err=%v", got, ok, err)
	}
}

func TestSourceBindingHalfMissingFailsClosed(t *testing.T) {
	for _, tc := range []struct {
		missing string
		want    string
	}{{"forward", "only one half"}, {"reverse", "reverse binding is missing"}} {
		t.Run(tc.missing, func(t *testing.T) {
			s := openSourceStateTestDB(t)
			archiveID := sourceStateTestArchive(24)
			binding := sourceBinding{sourceID: "writer-a", pubkey: sourceStateTestValue(31)}
			activation := sourceStateTestActivation(archiveID, 1, sourceStateTestValue(32), binding)
			if err := s.activateSourceSet(activation); err != nil {
				t.Fatal(err)
			}
			ref := sourceRef{archiveID: archiveID, sourceID: binding.sourceID}
			var k []byte
			if tc.missing == "forward" {
				k = sourceScopedKey(keySourceBinding, ref)
			} else {
				k = sourceSignerKey(archiveID, binding.pubkey)
			}
			if err := s.kv.Delete(k, pebble.Sync); err != nil {
				t.Fatal(err)
			}
			if err := s.activateSourceSet(activation); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("activation with missing %s binding half error = %v", tc.missing, err)
			}
			requireSourceSetMarker(t, s, activation.marker)
		})
	}
}

func TestSourceSetRosterFloorAndArchiveAreMonotonic(t *testing.T) {
	s := openSourceStateTestDB(t)
	archiveID := sourceStateTestArchive(2)
	digest1, digest2, digest3 := sourceStateTestValue(71), sourceStateTestValue(72), sourceStateTestValue(73)
	a := sourceBinding{sourceID: "writer-a", pubkey: sourceStateTestValue(11)}
	b := sourceBinding{sourceID: "writer-b", pubkey: sourceStateTestValue(12)}
	first := sourceStateTestActivation(archiveID, 2, digest1, a)
	if err := s.activateSourceSet(first); err != nil {
		t.Fatal(err)
	}
	if err := s.activateSourceSet(first); err != nil {
		t.Fatalf("idempotent activation: %v", err)
	}

	tests := []struct {
		name string
		act  sourceSetActivation
	}{
		{"lower revision", sourceStateTestActivation(archiveID, 1, digest1, a)},
		{"same revision different digest", sourceStateTestActivation(archiveID, 2, digest2, a)},
		{"changed digest without higher revision", sourceStateTestActivation(archiveID, 1, digest2, a)},
		{"changed archive", sourceStateTestActivation(sourceStateTestArchive(3), 3, digest2, a)},
		{"new binding with unchanged digest", sourceStateTestActivation(archiveID, 3, digest1, a, b)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if err := s.activateSourceSet(tc.act); err == nil {
				t.Fatal("activation succeeded")
			}
			requireSourceSetMarker(t, s, first.marker)
		})
	}

	// Raising a roster revision without changing its digest is permitted and
	// still establishes a rollback floor.
	raised := sourceStateTestActivation(archiveID, 3, digest1, a)
	if err := s.activateSourceSet(raised); err != nil {
		t.Fatal(err)
	}
	requireSourceSetMarker(t, s, raised.marker)
	if err := s.activateSourceSet(sourceStateTestActivation(archiveID, 2, digest1, a)); err == nil {
		t.Fatal("rolled back a revision-only roster acknowledgement")
	}

	changed := sourceStateTestActivation(archiveID, 4, digest3, a, b)
	if err := s.activateSourceSet(changed); err != nil {
		t.Fatal(err)
	}
	requireSourceSetMarker(t, s, changed.marker)
}

func TestSourceSetActivationRejectsMutableOwnershipTransferBeforeMarkerAdvance(t *testing.T) {
	s := openSourceStateTestDB(t)
	archiveID := sourceStateTestArchive(14)
	a := sourceBinding{sourceID: "writer-a", pubkey: sourceStateTestValue(41)}
	b := sourceBinding{sourceID: "writer-b", pubkey: sourceStateTestValue(42)}
	first := sourceStateTestActivation(archiveID, 1, sourceStateTestValue(43), a)
	if err := s.activateSourceSet(first); err != nil {
		t.Fatal(err)
	}
	handoff, mutable := checkpointV3TestEntries(t)
	cp, err := makeCheckpointV4(checkpointV3TestNet, archiveID, a.sourceID, true, &mutable, &handoff, nil,
		time.Unix(1, 0).UTC(), authorityFloor{authority: a.pubkey, revision: 1, digest: sourceStateTestValue(44)})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.putCheckpoint(mutable.Name, cp); err != nil {
		t.Fatal(err)
	}

	owner := sourceResumeRuntime(archiveID, b.sourceID, b.pubkey, mutable.Name)
	f := &Follower{
		cfg: Config{
			Heads:         map[string]pinning.Policy{mutable.Name: pinning.Full()},
			ExpectedKinds: map[string]server.HeadKind{mutable.Name: server.UnfinalizedMutable},
		},
		state:   s,
		sources: []*sourceRuntime{owner},
	}
	second := sourceStateTestActivation(archiveID, 2, sourceStateTestValue(45), b)
	if err := f.activateSourceSet(second); err == nil || !strings.Contains(err.Error(), "transfer durable mutable head") {
		t.Fatalf("mutable ownership transfer error = %v", err)
	}
	requireSourceSetMarker(t, s, first.marker)
}

func TestSourceSetActivationRejectsLegacyMutableAuthorityTransferBeforeUpgrade(t *testing.T) {
	s := openSourceStateTestDB(t)
	archiveID := sourceStateTestArchive(15)
	a := sourceBinding{sourceID: "writer-a", pubkey: sourceStateTestValue(46)}
	b := sourceBinding{sourceID: "writer-b", pubkey: sourceStateTestValue(47)}
	first := sourceStateTestActivation(archiveID, 1, sourceStateTestValue(48), a)
	if err := s.activateSourceSet(first); err != nil {
		t.Fatal(err)
	}
	handoff, mutable := checkpointV3TestEntries(t)
	cp, err := makeCheckpointV3(checkpointV3TestNet, true, &mutable, &handoff, time.Unix(1, 0).UTC(),
		authorityFloor{authority: a.pubkey, revision: 1, digest: sourceStateTestValue(49)})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.putCheckpoint(mutable.Name, cp); err != nil {
		t.Fatal(err)
	}

	owner := sourceResumeRuntime(archiveID, b.sourceID, b.pubkey, mutable.Name)
	f := &Follower{
		cfg: Config{
			Heads:         map[string]pinning.Policy{mutable.Name: pinning.Full()},
			ExpectedKinds: map[string]server.HeadKind{mutable.Name: server.UnfinalizedMutable},
		},
		state:   s,
		sources: []*sourceRuntime{owner},
	}
	second := sourceStateTestActivation(archiveID, 2, sourceStateTestValue(50), b)
	if err := f.activateSourceSet(second); err == nil || !strings.Contains(err.Error(), "transfer legacy mutable head") {
		t.Fatalf("legacy mutable ownership transfer error = %v", err)
	}
	requireSourceSetMarker(t, s, first.marker)
}

func TestSourceBindingsAndFloorsSurviveRemovalAndReAdd(t *testing.T) {
	s := openSourceStateTestDB(t)
	archiveID := sourceStateTestArchive(4)
	a := sourceBinding{sourceID: "writer-a", pubkey: sourceStateTestValue(21)}
	b := sourceBinding{sourceID: "writer-b", pubkey: sourceStateTestValue(22)}
	if err := s.activateSourceSet(sourceStateTestActivation(archiveID, 1, sourceStateTestValue(81), a, b)); err != nil {
		t.Fatal(err)
	}
	refB := sourceRef{archiveID: archiveID, sourceID: b.sourceID}
	floor := sourcePublicationFloor{revision: 9, digest: sourceStateTestValue(91)}
	name := testIPNSName(t)
	commitSourceStateBatch(t, s, func(batch *pebble.Batch) error {
		return s.stageSourcePublicationFloor(batch, refB, floor)
	})
	commitSourceStateBatch(t, s, func(batch *pebble.Batch) error {
		return s.stageSourceIPNSSeq(batch, refB, name, 44)
	})

	removed := sourceStateTestActivation(archiveID, 2, sourceStateTestValue(82), a)
	if err := s.activateSourceSet(removed); err != nil {
		t.Fatal(err)
	}
	if got, ok, err := s.sourceBinding(refB); err != nil || !ok || got != b {
		t.Fatalf("removed binding = %+v ok=%t err=%v", got, ok, err)
	}
	if got, ok, err := s.sourcePublicationFloor(refB); err != nil || !ok || got != floor {
		t.Fatalf("removed source floor = %+v ok=%t err=%v", got, ok, err)
	}
	if got, ok, err := s.sourceSignerBinding(archiveID, b.pubkey); err != nil || !ok || got != b.sourceID {
		t.Fatalf("retired reverse signer binding = %q ok=%t err=%v", got, ok, err)
	}

	alias := sourceBinding{sourceID: "writer-c", pubkey: b.pubkey}
	if err := s.activateSourceSet(sourceStateTestActivation(archiveID, 3, sourceStateTestValue(83), a, alias)); err == nil {
		t.Fatal("rebound a retired signer under a fresh source ID and reset its replay floors")
	}
	requireSourceSetMarker(t, s, removed.marker)

	wrongB := b
	wrongB.pubkey = sourceStateTestValue(23)
	if err := s.activateSourceSet(sourceStateTestActivation(archiveID, 3, sourceStateTestValue(83), a, wrongB)); err == nil {
		t.Fatal("reused a retired source ID with a different key")
	}
	requireSourceSetMarker(t, s, removed.marker)

	readded := sourceStateTestActivation(archiveID, 3, sourceStateTestValue(84), a, b)
	if err := s.activateSourceSet(readded); err != nil {
		t.Fatal(err)
	}
	if got, ok, err := s.sourcePublicationFloor(refB); err != nil || !ok || got != floor {
		t.Fatalf("re-added source floor = %+v ok=%t err=%v", got, ok, err)
	}
	if got, ok, err := s.sourceIPNSSeq(refB, name); err != nil || !ok || got != 44 {
		t.Fatalf("re-added IPNS floor = %d ok=%t err=%v", got, ok, err)
	}
	batch := s.kv.NewBatch()
	defer batch.Close()
	if err := s.stageSourcePublicationFloor(batch, refB, sourcePublicationFloor{revision: 8, digest: floor.digest}); err == nil {
		t.Fatal("re-added source accepted a replay below its retained floor")
	}
}

func TestRetiredForwardBindingWithoutReverseStillBlocksSignerAlias(t *testing.T) {
	s := openSourceStateTestDB(t)
	archiveID := sourceStateTestArchive(25)
	a := sourceBinding{sourceID: "writer-a", pubkey: sourceStateTestValue(33)}
	b := sourceBinding{sourceID: "writer-b", pubkey: sourceStateTestValue(34)}
	if err := s.activateSourceSet(sourceStateTestActivation(archiveID, 1, sourceStateTestValue(35), a, b)); err != nil {
		t.Fatal(err)
	}
	removed := sourceStateTestActivation(archiveID, 2, sourceStateTestValue(36), a)
	if err := s.activateSourceSet(removed); err != nil {
		t.Fatal(err)
	}
	if err := s.kv.Delete(sourceSignerKey(archiveID, b.pubkey), pebble.Sync); err != nil {
		t.Fatal(err)
	}
	alias := sourceBinding{sourceID: "writer-c", pubkey: b.pubkey}
	if err := s.activateSourceSet(sourceStateTestActivation(archiveID, 3, sourceStateTestValue(37), a, alias)); err == nil ||
		!strings.Contains(err.Error(), "reverse binding is missing") {
		t.Fatalf("alias after reverse-half loss error = %v", err)
	}
	requireSourceSetMarker(t, s, removed.marker)
}

func TestSourcePublicationAndChannelFloorsAreIsolated(t *testing.T) {
	s := openSourceStateTestDB(t)
	archiveID := sourceStateTestArchive(5)
	a := sourceBinding{sourceID: "writer-a", pubkey: sourceStateTestValue(31)}
	b := sourceBinding{sourceID: "writer-b", pubkey: sourceStateTestValue(32)}
	if err := s.activateSourceSet(sourceStateTestActivation(archiveID, 1, sourceStateTestValue(85), a, b)); err != nil {
		t.Fatal(err)
	}
	refA := sourceRef{archiveID: archiveID, sourceID: a.sourceID}
	refB := sourceRef{archiveID: archiveID, sourceID: b.sourceID}
	floorA := sourcePublicationFloor{revision: 100, digest: sourceStateTestValue(101)}
	floorB := sourcePublicationFloor{revision: 2, digest: sourceStateTestValue(102)}
	commitSourceStateBatch(t, s, func(batch *pebble.Batch) error { return s.stageSourcePublicationFloor(batch, refA, floorA) })
	commitSourceStateBatch(t, s, func(batch *pebble.Batch) error { return s.stageSourcePublicationFloor(batch, refB, floorB) })
	if got, ok, err := s.sourcePublicationFloor(refA); err != nil || !ok || got != floorA {
		t.Fatalf("source A floor = %+v ok=%t err=%v", got, ok, err)
	}
	if got, ok, err := s.sourcePublicationFloor(refB); err != nil || !ok || got != floorB {
		t.Fatalf("source B floor = %+v ok=%t err=%v", got, ok, err)
	}

	for _, bad := range []sourcePublicationFloor{
		{revision: 99, digest: floorA.digest},
		{revision: 100, digest: sourceStateTestValue(103)},
	} {
		batch := s.kv.NewBatch()
		err := s.stageSourcePublicationFloor(batch, refA, bad)
		_ = batch.Close()
		if err == nil {
			t.Fatalf("source A accepted bad publication floor %+v", bad)
		}
	}

	sharedName := testIPNSName(t)
	commitSourceStateBatch(t, s, func(batch *pebble.Batch) error { return s.stageSourceIPNSSeq(batch, refA, sharedName, 500) })
	commitSourceStateBatch(t, s, func(batch *pebble.Batch) error { return s.stageSourceIPNSSeq(batch, refB, sharedName, 3) })
	if got, ok, err := s.sourceIPNSSeq(refA, sharedName); err != nil || !ok || got != 500 {
		t.Fatalf("source A IPNS floor = %d ok=%t err=%v", got, ok, err)
	}
	if got, ok, err := s.sourceIPNSSeq(refB, sharedName); err != nil || !ok || got != 3 {
		t.Fatalf("source B IPNS floor = %d ok=%t err=%v", got, ok, err)
	}
	commitSourceStateBatch(t, s, func(batch *pebble.Batch) error { return s.stageSourceIPNSSeq(batch, refB, sharedName, 1) })
	if got, _, _ := s.sourceIPNSSeq(refB, sharedName); got != 3 {
		t.Fatalf("lower IPNS observation reduced source B floor to %d", got)
	}
}

func TestSourceIPNSFloorsAreBoundedAndProtectOwnDelegation(t *testing.T) {
	s := openSourceStateTestDB(t)
	archiveID := sourceStateTestArchive(6)
	binding := sourceBinding{sourceID: "writer-a", pubkey: sourceStateTestValue(41)}
	if err := s.activateSourceSet(sourceStateTestActivation(archiveID, 1, sourceStateTestValue(86), binding)); err != nil {
		t.Fatal(err)
	}
	ref := sourceRef{archiveID: archiveID, sourceID: binding.sourceID}
	names := make([]ipns.Name, maxIPNSFloorNames+2)
	for i := range names {
		names[i] = testIPNSName(t)
	}
	d := delegation{name: names[0], pubkey: append([]byte(nil), binding.pubkey[:]...)}
	commitSourceStateBatch(t, s, func(batch *pebble.Batch) error { return s.stageSourceDelegation(batch, ref, d) })
	for i, name := range names {
		commitSourceStateBatch(t, s, func(batch *pebble.Batch) error {
			return s.stageSourceIPNSSeq(batch, ref, name, uint64(i+1))
		})
	}
	floors, err := s.sourceIPNSFloors(ref)
	if err != nil {
		t.Fatal(err)
	}
	if len(floors) != maxIPNSFloorNames {
		t.Fatalf("IPNS floor count = %d, want %d", len(floors), maxIPNSFloorNames)
	}
	if got, ok, err := s.sourceIPNSSeq(ref, names[0]); err != nil || !ok || got != 1 {
		t.Fatalf("delegated name floor = %d ok=%t err=%v; it was evicted", got, ok, err)
	}
	if _, ok, err := s.sourceIPNSSeq(ref, names[1]); err != nil || ok {
		t.Fatalf("oldest unprotected name survived: ok=%t err=%v", ok, err)
	}
	if got, ok, err := s.sourceIPNSSeq(ref, names[len(names)-1]); err != nil || !ok || got != uint64(len(names)) {
		t.Fatalf("newest floor = %d ok=%t err=%v", got, ok, err)
	}
}

func TestSourceDelegationsAreIsolatedAndPinned(t *testing.T) {
	s := openSourceStateTestDB(t)
	archiveID := sourceStateTestArchive(7)
	a := sourceBinding{sourceID: "writer-a", pubkey: sourceStateTestValue(51)}
	b := sourceBinding{sourceID: "writer-b", pubkey: sourceStateTestValue(52)}
	if err := s.activateSourceSet(sourceStateTestActivation(archiveID, 1, sourceStateTestValue(87), a, b)); err != nil {
		t.Fatal(err)
	}
	refA := sourceRef{archiveID: archiveID, sourceID: a.sourceID}
	refB := sourceRef{archiveID: archiveID, sourceID: b.sourceID}
	dA := delegation{name: testIPNSName(t), pubkey: append([]byte(nil), a.pubkey[:]...)}
	dB := delegation{name: testIPNSName(t), pubkey: append([]byte(nil), b.pubkey[:]...)}
	commitSourceStateBatch(t, s, func(batch *pebble.Batch) error { return s.stageSourceDelegation(batch, refA, dA) })
	commitSourceStateBatch(t, s, func(batch *pebble.Batch) error { return s.stageSourceDelegation(batch, refB, dB) })
	for _, tc := range []struct {
		ref  sourceRef
		want delegation
	}{{refA, dA}, {refB, dB}} {
		got, ok, err := s.sourceDelegation(tc.ref)
		if err != nil || !ok || got.name != tc.want.name || !bytes.Equal(got.pubkey, tc.want.pubkey) {
			t.Fatalf("source %q delegation = %+v ok=%t err=%v", tc.ref.sourceID, got, ok, err)
		}
	}
	bad := dA
	bad.pubkey = append([]byte(nil), b.pubkey[:]...)
	batch := s.kv.NewBatch()
	err := s.stageSourceDelegation(batch, refA, bad)
	_ = batch.Close()
	if err == nil {
		t.Fatal("source A accepted a delegation signed by source B")
	}
	encoded, err := encodeDelegation(bad)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.kv.Set(sourceScopedKey(keySourceDelegation, refA), encoded, pebble.Sync); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := s.sourceDelegation(refA); err == nil || ok {
		t.Fatalf("read accepted a delegation whose signer differs from the immutable binding: ok=%t err=%v", ok, err)
	}
}

func TestUnknownSourceNamespaceRowPreventsFreshActivation(t *testing.T) {
	s := openSourceStateTestDB(t)
	if err := s.kv.Set(key("source_document:v99:future"), []byte{1}, pebble.Sync); err != nil {
		t.Fatal(err)
	}
	archiveID := sourceStateTestArchive(22)
	binding := sourceBinding{sourceID: "writer-a", pubkey: sourceStateTestValue(27)}
	if err := s.activateSourceSet(sourceStateTestActivation(archiveID, 1, sourceStateTestValue(28), binding)); err == nil {
		t.Fatal("activation treated an unknown future source-state row as an empty store")
	}
	if _, ok, err := s.sourceSetMarker(); err != nil || ok {
		t.Fatalf("failed orphan check left marker: ok=%t err=%v", ok, err)
	}
}

func TestSourceSetRejectsLegacyStateWithoutExplicitMigration(t *testing.T) {
	s := openSourceStateTestDB(t)
	archiveID := sourceStateTestArchive(8)
	binding := sourceBinding{sourceID: "writer-a", pubkey: sourceStateTestValue(61)}
	if err := s.put(keyIPNSSeq, 17); err != nil {
		t.Fatal(err)
	}
	activation := sourceStateTestActivation(archiveID, 1, sourceStateTestValue(88), binding)
	if err := s.activateSourceSet(activation); err == nil {
		t.Fatal("activated over legacy state without a migration source")
	}
	if _, ok, err := s.sourceSetMarker(); err != nil || ok {
		t.Fatalf("failed migration left marker: ok=%t err=%v", ok, err)
	}
	if _, ok, err := s.sourceBinding(sourceRef{archiveID: archiveID, sourceID: binding.sourceID}); err != nil || ok {
		t.Fatalf("failed migration left binding: ok=%t err=%v", ok, err)
	}
}

func TestLegacyCheckpointAndUpdatedAtRequireExplicitMigration(t *testing.T) {
	s := openSourceStateTestDB(t)
	archiveID := sourceStateTestArchive(12)
	binding := sourceBinding{sourceID: "writer-a", pubkey: sourceStateTestValue(62)}
	cp := checkpoint{
		root:      epochTestCID(t, 121),
		syncedTo:  42,
		updatedAt: sourceStateTestTime(100),
	}
	if err := s.putCheckpoint("all", cp); err != nil {
		t.Fatal(err)
	}
	activation := sourceStateTestActivation(archiveID, 1, sourceStateTestValue(93), binding)
	if err := s.activateSourceSet(activation); err == nil {
		t.Fatal("activated over a v1 checkpoint/updated_at floor without explicit migration acknowledgement")
	}
	if _, ok, err := s.sourceSetMarker(); err != nil || ok {
		t.Fatalf("failed checkpoint migration left marker: ok=%t err=%v", ok, err)
	}

	activation.legacyMigration = &sourceLegacyMigration{sourceID: binding.sourceID}
	if err := s.activateSourceSet(activation); err != nil {
		t.Fatalf("explicit checkpoint migration acknowledgement: %v", err)
	}
	if got, ok, err := s.checkpoint("all"); err != nil || !ok || got.root != cp.root || got.syncedTo != cp.syncedTo {
		t.Fatalf("legacy checkpoint changed: %+v ok=%t err=%v", got, ok, err)
	}
	if got, ok, err := s.updatedAt(); err != nil || !ok || !got.Equal(cp.updatedAt) {
		t.Fatalf("legacy updated_at changed: %s ok=%t err=%v", got, ok, err)
	}
}

func TestLegacyPreCheckpointFloorsRequireExplicitMigration(t *testing.T) {
	for _, tc := range []struct {
		name  string
		write func(*testing.T, *state)
	}{
		{
			name: "synced_to",
			write: func(t *testing.T, s *state) {
				if err := s.put(append(key(keySyncedTo), "all"...), 9); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "manifest",
			write: func(t *testing.T, s *state) {
				if err := s.kv.Set(append(key(keyManifest), "all"...), epochTestCID(t, 122).Bytes(), pebble.Sync); err != nil {
					t.Fatal(err)
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := openSourceStateTestDB(t)
			tc.write(t, s)
			archiveID := sourceStateTestArchive(13)
			binding := sourceBinding{sourceID: "writer-a", pubkey: sourceStateTestValue(63)}
			activation := sourceStateTestActivation(archiveID, 1, sourceStateTestValue(94), binding)
			if err := s.activateSourceSet(activation); err == nil {
				t.Fatal("activated over legacy pre-checkpoint state without explicit migration acknowledgement")
			}
			activation.legacyMigration = &sourceLegacyMigration{sourceID: binding.sourceID}
			if err := s.activateSourceSet(activation); err != nil {
				t.Fatalf("explicit migration acknowledgement: %v", err)
			}
		})
	}
}

func TestLegacySourceStateMigrationClonesAndRetainsEveryFloor(t *testing.T) {
	s := openSourceStateTestDB(t)
	archiveID := sourceStateTestArchive(9)
	pubkey := sourceStateTestValue(71)
	binding := sourceBinding{sourceID: "writer-a", pubkey: pubkey}
	legacyFloor := authorityFloor{authority: pubkey, revision: 19, digest: sourceStateTestValue(111)}
	commitSourceStateBatch(t, s, func(batch *pebble.Batch) error { return s.stageAuthorityFloor(batch, legacyFloor) })
	delegatedName, directName := testIPNSName(t), testIPNSName(t)
	if err := s.setIPNSSeq(delegatedName, 23, false); err != nil {
		t.Fatal(err)
	}
	if err := s.put(keyIPNSSeq, 29); err != nil {
		t.Fatal(err)
	}
	legacyDelegation := delegation{name: delegatedName, pubkey: append([]byte(nil), pubkey[:]...)}
	if err := s.commitAuthority(sourceStateTestTime(1), &legacyDelegation); err != nil {
		t.Fatal(err)
	}
	activation := sourceStateTestActivation(archiveID, 1, sourceStateTestValue(89), binding)
	activation.legacyMigration = &sourceLegacyMigration{sourceID: binding.sourceID, directIPNSName: &directName}
	if err := s.activateSourceSet(activation); err != nil {
		t.Fatal(err)
	}
	ref := sourceRef{archiveID: archiveID, sourceID: binding.sourceID}
	if got, ok, err := s.sourcePublicationFloor(ref); err != nil || !ok || got.revision != legacyFloor.revision || got.digest != legacyFloor.digest {
		t.Fatalf("migrated publication floor = %+v ok=%t err=%v", got, ok, err)
	}
	if got, ok, err := s.sourceIPNSSeq(ref, delegatedName); err != nil || !ok || got != 23 {
		t.Fatalf("migrated delegated IPNS floor = %d ok=%t err=%v", got, ok, err)
	}
	if got, ok, err := s.sourceIPNSSeq(ref, directName); err != nil || !ok || got != 29 {
		t.Fatalf("migrated unnamed IPNS floor = %d ok=%t err=%v", got, ok, err)
	}
	if got, ok, err := s.sourceDelegation(ref); err != nil || !ok || got.name != delegatedName || !bytes.Equal(got.pubkey, pubkey[:]) {
		t.Fatalf("migrated delegation = %+v ok=%t err=%v", got, ok, err)
	}

	// Migration is copy-only. Every legacy row remains available to old code,
	// while repeating the same declarative activation is idempotent.
	if got, ok, err := s.authorityFloor(pubkey); err != nil || !ok || got != legacyFloor {
		t.Fatalf("legacy authority floor changed: %+v ok=%t err=%v", got, ok, err)
	}
	if got, ok, err := s.ipnsSeq(delegatedName, false); err != nil || !ok || got != 23 {
		t.Fatalf("legacy named IPNS floor changed: %d ok=%t err=%v", got, ok, err)
	}
	if got, ok, err := s.get(keyIPNSSeq); err != nil || !ok || got != 29 {
		t.Fatalf("legacy unnamed IPNS floor changed: %d ok=%t err=%v", got, ok, err)
	}
	if got, ok, err := s.delegation(); err != nil || !ok || got.name != delegatedName || !bytes.Equal(got.pubkey, pubkey[:]) {
		t.Fatalf("legacy delegation changed: %+v ok=%t err=%v", got, ok, err)
	}
	if err := s.activateSourceSet(activation); err != nil {
		t.Fatalf("repeating migrated activation: %v", err)
	}
}

func sourceStateTestTime(seconds int64) (t time.Time) {
	return time.Unix(seconds, 0).UTC()
}

func TestLegacySourceMigrationRejectsAmbiguityAndMismatch(t *testing.T) {
	archiveID := sourceStateTestArchive(10)
	digest := sourceStateTestValue(90)
	pubkeyA, pubkeyB := sourceStateTestValue(81), sourceStateTestValue(82)
	binding := sourceBinding{sourceID: "writer-a", pubkey: pubkeyA}
	base := func(s *state, name *ipns.Name) sourceSetActivation {
		activation := sourceStateTestActivation(archiveID, 1, digest, binding)
		activation.legacyMigration = &sourceLegacyMigration{sourceID: binding.sourceID, directIPNSName: name}
		return activation
	}

	t.Run("multiple authorities", func(t *testing.T) {
		s := openSourceStateTestDB(t)
		commitSourceStateBatch(t, s, func(batch *pebble.Batch) error {
			if err := s.stageAuthorityFloor(batch, authorityFloor{authority: pubkeyA, revision: 1, digest: sourceStateTestValue(1)}); err != nil {
				return err
			}
			return s.stageAuthorityFloor(batch, authorityFloor{authority: pubkeyB, revision: 1, digest: sourceStateTestValue(2)})
		})
		if err := s.activateSourceSet(base(s, nil)); err == nil {
			t.Fatal("migrated multiple legacy publication authorities")
		}
	})

	t.Run("authority key mismatch", func(t *testing.T) {
		s := openSourceStateTestDB(t)
		commitSourceStateBatch(t, s, func(batch *pebble.Batch) error {
			return s.stageAuthorityFloor(batch, authorityFloor{authority: pubkeyB, revision: 1, digest: sourceStateTestValue(3)})
		})
		if err := s.activateSourceSet(base(s, nil)); err == nil {
			t.Fatal("migrated a legacy authority into a differently pinned source")
		}
	})

	t.Run("delegation key mismatch", func(t *testing.T) {
		s := openSourceStateTestDB(t)
		d := delegation{name: testIPNSName(t), pubkey: append([]byte(nil), pubkeyB[:]...)}
		if err := s.commitAuthority(sourceStateTestTime(1), &d); err != nil {
			t.Fatal(err)
		}
		if err := s.activateSourceSet(base(s, nil)); err == nil {
			t.Fatal("migrated a legacy delegation from another signer")
		}
	})

	t.Run("unnamed IPNS floor without name", func(t *testing.T) {
		s := openSourceStateTestDB(t)
		if err := s.put(keyIPNSSeq, 7); err != nil {
			t.Fatal(err)
		}
		if err := s.activateSourceSet(base(s, nil)); err == nil {
			t.Fatal("migrated an unnamed IPNS floor without naming it")
		}
	})
}

func TestEmptyStoreNeedsNoLegacyMigration(t *testing.T) {
	s := openSourceStateTestDB(t)
	archiveID := sourceStateTestArchive(11)
	binding := sourceBinding{sourceID: "writer-a", pubkey: sourceStateTestValue(91)}
	activation := sourceStateTestActivation(archiveID, 1, sourceStateTestValue(92), binding)
	if err := s.activateSourceSet(activation); err != nil {
		t.Fatal(err)
	}
	requireSourceSetMarker(t, s, activation.marker)
}

func TestSingularFollowerRefusesDurableSourceSetState(t *testing.T) {
	s := openSourceStateTestDB(t)
	archiveID := sourceStateTestArchive(23)
	binding := sourceBinding{sourceID: "writer-a", pubkey: sourceStateTestValue(29)}
	if err := s.activateSourceSet(sourceStateTestActivation(archiveID, 1, sourceStateTestValue(30), binding)); err != nil {
		t.Fatal(err)
	}
	if _, err := New(Config{KV: s.kv}); err == nil || !strings.Contains(err.Error(), "refusing to start the singular-source runtime") {
		t.Fatalf("singular New over source-set state error = %v", err)
	}
}
