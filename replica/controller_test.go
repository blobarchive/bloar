package replica

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
)

type fakePin struct {
	recursive bool
	name      string
}

type fakeBackend struct {
	kv         *pebble.DB
	blocks     map[string]blocks.Block
	pins       map[string]fakePin
	events     []string
	failRemove map[string]error
	failStatus error
	progress   []PinProgress
}

func newFakeBackend(kv *pebble.DB) *fakeBackend {
	return &fakeBackend{
		kv: kv, blocks: map[string]blocks.Block{}, pins: map[string]fakePin{}, failRemove: map[string]error{},
		progress: []PinProgress{{Blocks: 1, Bytes: 10}, {Blocks: 2, Bytes: 20}},
	}
}

func (f *fakeBackend) PutBlock(_ context.Context, block blocks.Block) error {
	f.events = append(f.events, "put:"+block.Cid().String())
	f.blocks[block.Cid().KeyString()] = block
	return nil
}

func (f *fakeBackend) PinStatus(_ context.Context, target cid.Cid) (PinStatus, bool, error) {
	if f.failStatus != nil {
		return PinStatus{}, false, f.failStatus
	}
	pin, ok := f.pins[target.KeyString()]
	return PinStatus{Recursive: pin.recursive, Name: pin.name}, ok, nil
}

func (f *fakeBackend) NamedRecursivePins(_ context.Context, name string) ([]cid.Cid, error) {
	var result []cid.Cid
	for key, pin := range f.pins {
		if pin.recursive && pin.name == name {
			result = append(result, cidFromKey(key))
		}
	}
	slices.SortFunc(result, func(a, b cid.Cid) int { return slices.Compare(a.Bytes(), b.Bytes()) })
	return result, nil
}

func (f *fakeBackend) PinAddRecursive(_ context.Context, target cid.Cid, name string, progress func(PinProgress)) error {
	state, err := loadControllerState(f.kv)
	if err != nil {
		return err
	}
	if state.Pending == nil || !state.Pending.Anchor.Equals(target) {
		return errors.New("pin mutation happened before pending intent was durable")
	}
	f.events = append(f.events, "add:"+target.String())
	for _, observation := range f.progress {
		progress(observation)
	}
	f.pins[target.KeyString()] = fakePin{recursive: true, name: name}
	return nil
}

func (f *fakeBackend) PinUpdateRecursive(_ context.Context, old, next cid.Cid, unpin bool) error {
	state, err := loadControllerState(f.kv)
	if err != nil {
		return err
	}
	if state.Pending == nil || !state.Pending.Anchor.Equals(next) {
		return errors.New("pin update happened before pending intent was durable")
	}
	oldPin, ok := f.pins[old.KeyString()]
	if !ok || !oldPin.recursive {
		return errors.New("old pin missing")
	}
	f.events = append(f.events, fmt.Sprintf("update:%s:%s:%t", old, next, unpin))
	f.pins[next.KeyString()] = oldPin
	if unpin {
		delete(f.pins, old.KeyString())
	}
	return nil
}

func (f *fakeBackend) PinRemoveRecursive(_ context.Context, target cid.Cid) error {
	f.events = append(f.events, "remove:"+target.String())
	if err := f.failRemove[target.KeyString()]; err != nil {
		return err
	}
	delete(f.pins, target.KeyString())
	return nil
}

func TestControllerAddUpdateCommitAndRecoverCleanup(t *testing.T) {
	kv := openReplicaKV(t)
	backend := newFakeBackend(kv)
	var progress []PinProgress
	clock := time.Unix(100, 0)
	controller := newTestController(t, kv, backend, func() time.Time { return clock }, func(p PinProgress) {
		progress = append(progress, p)
	})
	a := testGeneration("a", 1)
	b := testGeneration("b", 2)
	aAnchor := generationCID(t, a)
	bAnchor := generationCID(t, b)

	if err := controller.Prepare(context.Background(), a); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(progress, backend.progress) {
		t.Fatalf("progress = %#v, want %#v", progress, backend.progress)
	}
	status, err := controller.Status()
	if err != nil {
		t.Fatal(err)
	}
	if status.Current.Defined() || !status.Pending.Equals(aAnchor) {
		t.Fatalf("after prepare status = %+v", status)
	}
	if status.PendingOwnership != OwnershipOwned || !status.PendingAt.Equal(clock) || status.PendingGeneration == nil {
		t.Fatalf("after prepare durable metadata = %+v", status)
	}
	if err := controller.Commit(context.Background(), a); err != nil {
		t.Fatal(err)
	}
	status, err = controller.Status()
	if err != nil {
		t.Fatal(err)
	}
	if status.CurrentOwnership != OwnershipOwned || !status.CurrentAt.Equal(clock) || status.PendingAt != (time.Time{}) {
		t.Fatalf("after commit durable metadata = %+v", status)
	}

	clock = time.Unix(200, 0)
	if err := controller.Prepare(context.Background(), b); err != nil {
		t.Fatal(err)
	}
	if _, ok := backend.pins[aAnchor.KeyString()]; !ok {
		t.Fatal("pin/update removed the committed generation before checkpoint commit")
	}
	wantUpdate := fmt.Sprintf("update:%s:%s:false", aAnchor, bAnchor)
	if !slices.Contains(backend.events, wantUpdate) {
		t.Fatalf("events %#v do not contain %q", backend.events, wantUpdate)
	}
	clock = time.Unix(250, 0)
	if err := controller.Prepare(context.Background(), b); err != nil {
		t.Fatal(err)
	}
	status, err = controller.Status()
	if err != nil {
		t.Fatal(err)
	}
	if !status.PendingAt.Equal(time.Unix(200, 0)) {
		t.Fatalf("same-candidate retry reset durable transition time: %+v", status)
	}

	backend.failRemove[aAnchor.KeyString()] = errors.New("busy")
	if err := controller.Commit(context.Background(), b); err == nil {
		t.Fatal("expected cleanup failure")
	}
	status, err = controller.Status()
	if err != nil {
		t.Fatal(err)
	}
	if !status.Current.Equals(bAnchor) || status.Pending.Defined() || status.Cleanup != 1 {
		t.Fatalf("cleanup failure lost safe committed state: %+v", status)
	}
	if !status.CurrentAt.Equal(clock) || !status.CleanupOldestRetainedAt.Equal(time.Unix(100, 0)) {
		t.Fatalf("cleanup failure lost durable timestamps: %+v", status)
	}
	delete(backend.failRemove, aAnchor.KeyString())
	if err := controller.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	status, _ = controller.Status()
	if status.Cleanup != 0 {
		t.Fatalf("cleanup debt = %d, want 0", status.Cleanup)
	}
	if _, ok := backend.pins[aAnchor.KeyString()]; ok {
		t.Fatal("superseded owned anchor remains pinned")
	}
	if _, ok := backend.pins[bAnchor.KeyString()]; !ok {
		t.Fatal("current anchor was removed")
	}
}

func TestControllerAllHeadWithdrawalIsCrashSafeAndLeavesUnrelatedPinsAlone(t *testing.T) {
	ctx := context.Background()
	kv := openReplicaKV(t)
	backend := newFakeBackend(kv)
	controller := newTestController(t, kv, backend, nil, nil)

	current := testGeneration("selected", 1)
	currentAnchor := generationCID(t, current)
	withdrawn := Generation{UpdatedAt: time.Unix(2, 0)}
	withdrawnAnchor := generationCID(t, withdrawn)
	unrelated := testCID("operator-unrelated")
	unrelatedPin := fakePin{recursive: true, name: "operator/archive"}
	backend.pins[unrelated.KeyString()] = unrelatedPin

	if err := controller.Prepare(ctx, current); err != nil {
		t.Fatal(err)
	}
	if err := controller.Commit(ctx, current); err != nil {
		t.Fatal(err)
	}
	if _, err := controller.ProtectsAll(ctx, nil); !errors.Is(err, ErrGenerationUnprotected) {
		t.Fatalf("absence without an empty anchor = %v, want ErrGenerationUnprotected", err)
	}

	// Prepare protects the explicit zero-head anchor without retiring the
	// generation still named by the follower checkpoint. This is the first
	// crash boundary: a restarted process must accept both pins and recover the
	// exact pending empty generation, not infer withdrawal from missing state.
	if err := controller.Prepare(ctx, withdrawn); err != nil {
		t.Fatal(err)
	}
	if _, ok := backend.pins[currentAnchor.KeyString()]; !ok {
		t.Fatal("Prepare retired the selected generation before checkpoint commit")
	}
	if pin, ok := backend.pins[withdrawnAnchor.KeyString()]; !ok || !pin.recursive || pin.name != "bloar-replica/v1/test-replica" {
		t.Fatalf("prepared empty anchor = %+v, present=%t", pin, ok)
	}
	if pin, ok := backend.pins[unrelated.KeyString()]; !ok || pin != unrelatedPin {
		t.Fatalf("Prepare disturbed unrelated pin: %+v, present=%t", pin, ok)
	}

	restarted := newTestController(t, kv, backend, nil, nil)
	if err := restarted.Recover(ctx); err != nil {
		t.Fatalf("recovering prepared empty generation: %v", err)
	}
	protected, err := restarted.ProtectsAll(ctx, nil)
	if err != nil {
		t.Fatalf("prepared empty generation did not prove all-head withdrawal: %v", err)
	}
	if !protected.Equal(Generation{ReplicaID: "test-replica", UpdatedAt: withdrawn.UpdatedAt}) {
		t.Fatalf("protected empty generation = %+v", protected)
	}

	// Simulate a crash after Current is durably promoted but before its old pin
	// can be removed. Commit must report only safe cleanup debt, and a later
	// recovery must remove exactly the controller-owned superseded anchor.
	backend.failRemove[currentAnchor.KeyString()] = errors.New("simulated crash boundary")
	err = restarted.Commit(ctx, withdrawn)
	var cleanupErr *CleanupError
	if !errors.As(err, &cleanupErr) {
		t.Fatalf("Commit error = %v, want CleanupError", err)
	}
	status, err := restarted.Status()
	if err != nil {
		t.Fatal(err)
	}
	if !status.Current.Equals(withdrawnAnchor) || status.Pending.Defined() || status.Cleanup != 1 {
		t.Fatalf("post-commit empty generation state = %+v", status)
	}
	if status.CurrentGeneration == nil || status.CurrentGeneration.Heads == nil || len(status.CurrentGeneration.Heads) != 0 {
		t.Fatalf("durable current empty generation = %+v", status.CurrentGeneration)
	}
	if _, ok := backend.pins[currentAnchor.KeyString()]; !ok {
		t.Fatal("failed cleanup did not safely retain the old generation")
	}
	if pin, ok := backend.pins[unrelated.KeyString()]; !ok || pin != unrelatedPin {
		t.Fatalf("Commit disturbed unrelated pin: %+v, present=%t", pin, ok)
	}

	delete(backend.failRemove, currentAnchor.KeyString())
	afterCrash := newTestController(t, kv, backend, nil, nil)
	if err := afterCrash.Recover(ctx); err != nil {
		t.Fatalf("recovering committed empty generation: %v", err)
	}
	if _, ok := backend.pins[currentAnchor.KeyString()]; ok {
		t.Fatal("recovery left superseded selected generation pinned")
	}
	if _, ok := backend.pins[withdrawnAnchor.KeyString()]; !ok {
		t.Fatal("recovery removed current empty generation anchor")
	}
	if pin, ok := backend.pins[unrelated.KeyString()]; !ok || pin != unrelatedPin {
		t.Fatalf("Recovery disturbed unrelated pin: %+v, present=%t", pin, ok)
	}
	if err := afterCrash.AuditCurrent(ctx); err != nil {
		t.Fatalf("auditing recovered empty generation: %v", err)
	}
	if _, err := afterCrash.ProtectsAll(ctx, []Head{}); err != nil {
		t.Fatalf("committed empty generation did not prove withdrawal after recovery: %v", err)
	}
	status, err = afterCrash.Status()
	if err != nil {
		t.Fatal(err)
	}
	if status.Cleanup != 0 || !status.Current.Equals(withdrawnAnchor) {
		t.Fatalf("recovered empty generation state = %+v", status)
	}
}

func TestControllerBorrowedPinsAreNeverMutatedOrRemoved(t *testing.T) {
	kv := openReplicaKV(t)
	backend := newFakeBackend(kv)
	controller := newTestController(t, kv, backend, nil, nil)
	borrowed := testGeneration("borrowed", 1)
	anchor := generationCID(t, borrowed)
	backend.pins[anchor.KeyString()] = fakePin{recursive: true, name: "operator/archive"}

	if err := controller.Prepare(context.Background(), borrowed); err != nil {
		t.Fatal(err)
	}
	if err := controller.Commit(context.Background(), borrowed); err != nil {
		t.Fatal(err)
	}
	for _, event := range backend.events {
		if event == "add:"+anchor.String() || len(event) >= 7 && event[:7] == "update:" {
			t.Fatalf("borrowed pin was mutated: %q", event)
		}
	}

	next := testGeneration("next", 2)
	if err := controller.Prepare(context.Background(), next); err != nil {
		t.Fatal(err)
	}
	if err := controller.Commit(context.Background(), next); err != nil {
		t.Fatal(err)
	}
	if pin, ok := backend.pins[anchor.KeyString()]; !ok || pin.name != "operator/archive" {
		t.Fatalf("borrowed pin changed or disappeared: %+v, %t", pin, ok)
	}
}

func TestControllerFailsClosedOnAmbiguousExistingPins(t *testing.T) {
	for name, pin := range map[string]fakePin{
		"direct":          {recursive: false, name: "operator"},
		"reserved orphan": {recursive: true, name: "bloar-replica/v1/test-replica"},
	} {
		t.Run(name, func(t *testing.T) {
			kv := openReplicaKV(t)
			backend := newFakeBackend(kv)
			controller := newTestController(t, kv, backend, nil, nil)
			generation := testGeneration(name, 1)
			backend.pins[generationCID(t, generation).KeyString()] = pin
			if err := controller.Prepare(context.Background(), generation); !errors.Is(err, ErrOwnershipDrift) {
				t.Fatalf("Prepare error = %v, want ownership drift", err)
			}
		})
	}
}

func TestControllerPendingIsIdempotentAndSupersededSafely(t *testing.T) {
	kv := openReplicaKV(t)
	backend := newFakeBackend(kv)
	controller := newTestController(t, kv, backend, nil, nil)
	a := testGeneration("a", 1)
	b := testGeneration("b", 2)
	c := testGeneration("c", 3)
	if err := controller.Prepare(context.Background(), a); err != nil {
		t.Fatal(err)
	}
	if err := controller.Commit(context.Background(), a); err != nil {
		t.Fatal(err)
	}
	if err := controller.Prepare(context.Background(), b); err != nil {
		t.Fatal(err)
	}
	before := len(backend.events)
	if err := controller.Prepare(context.Background(), b); err != nil {
		t.Fatal(err)
	}
	for _, event := range backend.events[before:] {
		if len(event) >= 4 && (event[:4] == "add:" || len(event) >= 7 && event[:7] == "update:") {
			t.Fatalf("retry repeated pin mutation: %q", event)
		}
	}
	if err := controller.Prepare(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	state, err := loadControllerState(kv)
	if err != nil {
		t.Fatal(err)
	}
	if state.Pending == nil || !state.Pending.Anchor.Equals(generationCID(t, c)) || len(state.Cleanup) != 0 {
		t.Fatalf("superseded pending state = %+v", state)
	}
	if _, ok := backend.pins[generationCID(t, b).KeyString()]; ok {
		t.Fatal("superseded pending anchor was not eagerly cleaned")
	}
	if err := controller.Commit(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	if _, ok := backend.pins[generationCID(t, b).KeyString()]; ok {
		t.Fatal("superseded pending anchor leaked")
	}
}

func TestControllerReturnToCurrentCancelsOwnedPending(t *testing.T) {
	kv := openReplicaKV(t)
	backend := newFakeBackend(kv)
	controller := newTestController(t, kv, backend, nil, nil)
	a := testGeneration("a", 1)
	b := testGeneration("b", 2)
	if err := controller.Prepare(context.Background(), a); err != nil {
		t.Fatal(err)
	}
	if err := controller.Commit(context.Background(), a); err != nil {
		t.Fatal(err)
	}
	if err := controller.Prepare(context.Background(), b); err != nil {
		t.Fatal(err)
	}
	if err := controller.Prepare(context.Background(), a); err != nil {
		t.Fatal(err)
	}
	status, _ := controller.Status()
	if status.Pending.Defined() || status.Cleanup != 0 {
		t.Fatalf("return-to-current status = %+v", status)
	}
	if _, ok := backend.pins[generationCID(t, b).KeyString()]; ok {
		t.Fatal("canceled pending anchor remains pinned")
	}
}

func TestControllerProtectsAndRejectsOrphanNamedPin(t *testing.T) {
	kv := openReplicaKV(t)
	backend := newFakeBackend(kv)
	controller := newTestController(t, kv, backend, nil, nil)
	generation := testGeneration("a", 1)
	if err := controller.Prepare(context.Background(), generation); err != nil {
		t.Fatal(err)
	}
	if err := controller.Protects(context.Background(), generation.Heads[0]); err != nil {
		t.Fatalf("pending generation did not protect head: %v", err)
	}
	wrong := generation.Heads[0]
	wrong.Root = testCID("wrong")
	if err := controller.Protects(context.Background(), wrong); !errors.Is(err, ErrGenerationUnprotected) {
		t.Fatalf("wrong head error = %v", err)
	}
	orphan := testCID("orphan-anchor")
	backend.pins[orphan.KeyString()] = fakePin{recursive: true, name: "bloar-replica/v1/test-replica"}
	if err := controller.Recover(context.Background()); !errors.Is(err, ErrOwnershipDrift) {
		t.Fatalf("Recover error = %v, want ownership drift", err)
	}
}

func TestRecoverAllowsInterruptedOwnedPendingAndPrepareRetriesIt(t *testing.T) {
	kv := openReplicaKV(t)
	backend := newFakeBackend(kv)
	controller := newTestController(t, kv, backend, nil, nil)
	generation := testGeneration("interrupted", 1)
	generation.ReplicaID = "test-replica"
	normalized, err := generation.Normalize()
	if err != nil {
		t.Fatal(err)
	}
	anchor := generationCID(t, generation)
	state := controllerState{Pending: &retainedGeneration{
		Generation: normalized, Anchor: anchor, Ownership: OwnershipOwned, At: time.Unix(1, 0),
	}}
	if err := saveControllerState(kv, state); err != nil {
		t.Fatal(err)
	}

	if err := controller.Recover(context.Background()); err != nil {
		t.Fatalf("Recover rejected resumable pre-pin intent: %v", err)
	}
	if err := controller.Prepare(context.Background(), generation); err != nil {
		t.Fatalf("Prepare did not resume interrupted intent: %v", err)
	}
	if pin, ok := backend.pins[anchor.KeyString()]; !ok || !pin.recursive || pin.name != "bloar-replica/v1/test-replica" {
		t.Fatalf("resumed pin = %+v, present=%t", pin, ok)
	}
}

func TestBorrowedPendingDisappearanceNeverConvertsOwnership(t *testing.T) {
	kv := openReplicaKV(t)
	backend := newFakeBackend(kv)
	controller := newTestController(t, kv, backend, nil, nil)
	generation := testGeneration("borrowed-pending", 1)
	anchor := generationCID(t, generation)
	backend.pins[anchor.KeyString()] = fakePin{recursive: true, name: "operator/archive"}
	if err := controller.Prepare(context.Background(), generation); err != nil {
		t.Fatal(err)
	}
	delete(backend.pins, anchor.KeyString())
	before := len(backend.events)
	if err := controller.Prepare(context.Background(), generation); !errors.Is(err, ErrOwnershipDrift) {
		t.Fatalf("Prepare error = %v, want ownership drift", err)
	}
	for _, event := range backend.events[before:] {
		if event == "add:"+anchor.String() {
			t.Fatal("disappeared borrowed pin was recreated as controller-owned")
		}
	}
	if err := controller.Recover(context.Background()); err != nil {
		t.Fatalf("Recover should leave missing borrowed Pending for a live retry: %v", err)
	}
	backend.pins[anchor.KeyString()] = fakePin{recursive: true, name: "operator/archive"}
	if err := controller.Prepare(context.Background(), generation); err != nil {
		t.Fatalf("restored borrowed pin was not accepted: %v", err)
	}
}

func TestAuditCurrentDetectsAndRecoversRetentionLoss(t *testing.T) {
	kv := openReplicaKV(t)
	backend := newFakeBackend(kv)
	controller := newTestController(t, kv, backend, nil, nil)
	generation := testGeneration("audit", 1)
	anchor := generationCID(t, generation)
	if err := controller.Prepare(context.Background(), generation); err != nil {
		t.Fatal(err)
	}
	if err := controller.Commit(context.Background(), generation); err != nil {
		t.Fatal(err)
	}
	if err := controller.AuditCurrent(context.Background()); err != nil {
		t.Fatalf("healthy audit: %v", err)
	}
	delete(backend.pins, anchor.KeyString())
	if err := controller.AuditCurrent(context.Background()); !errors.Is(err, ErrGenerationUnprotected) {
		t.Fatalf("missing pin audit = %v", err)
	}
	backend.pins[anchor.KeyString()] = fakePin{recursive: true, name: "operator-takeover"}
	if err := controller.AuditCurrent(context.Background()); !errors.Is(err, ErrOwnershipDrift) {
		t.Fatalf("renamed owned pin audit = %v", err)
	}
	backend.pins[anchor.KeyString()] = fakePin{recursive: true, name: "bloar-replica/v1/test-replica"}
	if err := controller.AuditCurrent(context.Background()); err != nil {
		t.Fatalf("restored audit: %v", err)
	}
}

func TestProtectsAllRejectsMixedCrashGenerationsAndAuditsPending(t *testing.T) {
	kv := openReplicaKV(t)
	backend := newFakeBackend(kv)
	controller := newTestController(t, kv, backend, nil, nil)
	current := Generation{UpdatedAt: time.Unix(1, 0), Heads: []Head{
		{Name: "a", Root: testCID("a-1"), SyncedTo: 1},
		{Name: "b", Root: testCID("b-1"), SyncedTo: 1},
	}}
	pending := Generation{UpdatedAt: time.Unix(2, 0), Heads: []Head{
		{Name: "a", Root: testCID("a-2"), SyncedTo: 2},
		{Name: "b", Root: testCID("b-2"), SyncedTo: 2},
	}}
	if err := controller.Prepare(context.Background(), current); err != nil {
		t.Fatal(err)
	}
	if err := controller.Commit(context.Background(), current); err != nil {
		t.Fatal(err)
	}
	if err := controller.Prepare(context.Background(), pending); err != nil {
		t.Fatal(err)
	}

	mixed := []Head{current.Heads[0], pending.Heads[1]}
	if _, err := controller.ProtectsAll(context.Background(), mixed); !errors.Is(err, ErrGenerationUnprotected) {
		t.Fatalf("mixed generation proof = %v", err)
	}
	protected, err := controller.ProtectsAll(context.Background(), pending.Heads)
	if err != nil {
		t.Fatalf("pending generation proof: %v", err)
	}
	if !protected.Equal(Generation{ReplicaID: "test-replica", UpdatedAt: pending.UpdatedAt, Heads: pending.Heads}) {
		t.Fatalf("protected generation = %+v", protected)
	}
	// A resume-time upward floor repair changes no retained bytes. The same
	// all-head anchor therefore remains a valid protection proof.
	repairedFloors := slices.Clone(pending.Heads)
	repairedFloors[0].SyncedTo++
	if _, err := controller.ProtectsAll(context.Background(), repairedFloors); err != nil {
		t.Fatalf("same roots with repaired floor should remain protected: %v", err)
	}
	changedManifest := slices.Clone(pending.Heads)
	changedManifest[0].Manifest = testCID("different-manifest")
	if _, err := controller.ProtectsAll(context.Background(), changedManifest); !errors.Is(err, ErrGenerationUnprotected) {
		t.Fatalf("changed manifest proof = %v, want ErrGenerationUnprotected", err)
	}
	if err := controller.AuditGeneration(context.Background(), protected); err != nil {
		t.Fatalf("pending audit: %v", err)
	}
	delete(backend.pins, generationCID(t, pending).KeyString())
	if err := controller.AuditGeneration(context.Background(), protected); !errors.Is(err, ErrGenerationUnprotected) {
		t.Fatalf("missing pending pin audit = %v", err)
	}
}

func TestAuditGenerationDoesNotWaitForTransitionMutex(t *testing.T) {
	kv := openReplicaKV(t)
	backend := newFakeBackend(kv)
	controller := newTestController(t, kv, backend, nil, nil)
	generation := testGeneration("audit-during-pin", 1)
	if err := controller.Prepare(context.Background(), generation); err != nil {
		t.Fatal(err)
	}
	if err := controller.Commit(context.Background(), generation); err != nil {
		t.Fatal(err)
	}
	protected, err := controller.ProtectsAll(context.Background(), generation.Heads)
	if err != nil {
		t.Fatal(err)
	}

	// Prepare holds this lock across the entire recursive Kubo traversal. An
	// independent readiness audit of the active generation must not queue behind
	// it or ignore its own context deadline.
	controller.mu.Lock()
	defer controller.mu.Unlock()
	done := make(chan error, 1)
	go func() { done <- controller.AuditGeneration(context.Background(), protected) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("audit while transition lock held: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("AuditGeneration waited for the long transition mutex")
	}
}

func TestPrepareReservesCleanupSlotForCurrentGeneration(t *testing.T) {
	kv := openReplicaKV(t)
	backend := newFakeBackend(kv)
	controller := newTestController(t, kv, backend, nil, nil)
	current := testGeneration("current", 100)
	current.ReplicaID = "test-replica"
	currentNormalized, _ := current.Normalize()
	currentAnchor := generationCID(t, current)
	state := controllerState{Current: &retainedGeneration{
		Generation: currentNormalized, Anchor: currentAnchor, Ownership: OwnershipOwned, At: time.Unix(100, 0),
	}}
	backend.pins[currentAnchor.KeyString()] = fakePin{recursive: true, name: "bloar-replica/v1/test-replica"}
	for i := 0; i < maxGenerationHeads+1; i++ {
		generation := testGeneration(fmt.Sprintf("debt-%d", i), uint64(i+1))
		generation.ReplicaID = "test-replica"
		normalized, _ := generation.Normalize()
		anchor := generationCID(t, generation)
		state.Cleanup = append(state.Cleanup, retainedGeneration{
			Generation: normalized, Anchor: anchor, Ownership: OwnershipOwned, At: time.Unix(int64(i+1), 0),
		})
		backend.pins[anchor.KeyString()] = fakePin{recursive: true, name: "bloar-replica/v1/test-replica"}
		backend.failRemove[anchor.KeyString()] = errors.New("busy")
	}
	if err := saveControllerState(kv, state); err != nil {
		t.Fatal(err)
	}

	next := testGeneration("next", 101)
	before := len(backend.events)
	if err := controller.Prepare(context.Background(), next); err == nil {
		t.Fatal("Prepare did not reserve room to retire Current")
	}
	for _, event := range backend.events[before:] {
		if strings.HasPrefix(event, "add:") || strings.HasPrefix(event, "update:") {
			t.Fatalf("pin mutation crossed cleanup bound: %q", event)
		}
	}

	delete(backend.failRemove, state.Cleanup[0].Anchor.KeyString())
	if err := controller.Prepare(context.Background(), next); err != nil {
		var cleanup *CleanupError
		if !errors.As(err, &cleanup) {
			t.Fatalf("Prepare after freeing cleanup slot: %v", err)
		}
	}
	status, err := controller.Status()
	if err != nil {
		t.Fatal(err)
	}
	if !status.Pending.Equals(generationCID(t, next)) || status.Cleanup > maxGenerationHeads {
		t.Fatalf("reserved transition state = %+v", status)
	}
}

func TestCleanupBoundRetriesLiveWithoutOverflow(t *testing.T) {
	kv := openReplicaKV(t)
	backend := newFakeBackend(kv)
	controller := newTestController(t, kv, backend, nil, nil)
	state := controllerState{}
	for i := 0; i < maxGenerationHeads+1; i++ {
		generation := testGeneration(fmt.Sprintf("cleanup-%d", i), uint64(i+1))
		generation.ReplicaID = "test-replica"
		normalized, err := generation.Normalize()
		if err != nil {
			t.Fatal(err)
		}
		anchor := generationCID(t, generation)
		retained := retainedGeneration{Generation: normalized, Anchor: anchor, Ownership: OwnershipOwned, At: time.Unix(int64(i+1), 0)}
		state.Cleanup = append(state.Cleanup, retained)
		backend.pins[anchor.KeyString()] = fakePin{recursive: true, name: "bloar-replica/v1/test-replica"}
		backend.failRemove[anchor.KeyString()] = errors.New("busy")
	}
	old := testGeneration("old-pending", 100)
	old.ReplicaID = "test-replica"
	oldNormalized, _ := old.Normalize()
	oldAnchor := generationCID(t, old)
	state.Pending = &retainedGeneration{Generation: oldNormalized, Anchor: oldAnchor, Ownership: OwnershipOwned, At: time.Unix(100, 0)}
	backend.pins[oldAnchor.KeyString()] = fakePin{recursive: true, name: "bloar-replica/v1/test-replica"}
	backend.failRemove[oldAnchor.KeyString()] = errors.New("busy")
	if err := saveControllerState(kv, state); err != nil {
		t.Fatal(err)
	}

	next := testGeneration("next", 101)
	if err := controller.Prepare(context.Background(), next); err == nil {
		t.Fatal("Prepare crossed the cleanup hard bound")
	}
	status, err := controller.Status()
	if err != nil {
		t.Fatal(err)
	}
	if status.Cleanup != maxGenerationHeads+1 || !status.Pending.Equals(oldAnchor) {
		t.Fatalf("bounded refusal changed state: %+v", status)
	}

	delete(backend.failRemove, state.Cleanup[0].Anchor.KeyString())
	err = controller.Prepare(context.Background(), next)
	var cleanup *CleanupError
	if err != nil && !errors.As(err, &cleanup) {
		t.Fatalf("live retry after one cleanup slot = %v", err)
	}
	status, _ = controller.Status()
	if status.Cleanup > maxGenerationHeads+1 || !status.Pending.Equals(generationCID(t, next)) {
		t.Fatalf("live retry did not advance within bound: %+v", status)
	}
}

func TestControllerStateCorruptionFailsClosed(t *testing.T) {
	kv := openReplicaKV(t)
	if err := kv.Set(stateKey, []byte(`{"version":1,"unknown":true}`), pebble.Sync); err != nil {
		t.Fatal(err)
	}
	controller := newTestController(t, kv, newFakeBackend(kv), nil, nil)
	if _, err := controller.Status(); err == nil {
		t.Fatal("corrupt state was accepted")
	}
}

func TestControllerStateRejectsDuplicateAndBorrowedCleanup(t *testing.T) {
	kv := openReplicaKV(t)
	generation := testGeneration("duplicate", 1)
	generation.ReplicaID = "test-replica"
	normalized, _ := generation.Normalize()
	retained := retainedGeneration{
		Generation: normalized, Anchor: generationCID(t, generation), Ownership: OwnershipOwned, At: time.Unix(1, 0),
	}
	if err := saveControllerState(kv, controllerState{Current: &retained, Cleanup: []retainedGeneration{retained}}); err == nil {
		t.Fatal("duplicate current/cleanup anchor was accepted")
	}
	borrowed := retained
	borrowed.Ownership = OwnershipBorrowed
	if err := saveControllerState(kv, controllerState{Cleanup: []retainedGeneration{borrowed}}); err == nil {
		t.Fatal("borrowed cleanup anchor was accepted")
	}
}

func newTestController(t *testing.T, kv *pebble.DB, backend Backend, now func() time.Time, progress func(PinProgress)) *Controller {
	t.Helper()
	controller, err := New(Config{KV: kv, Backend: backend, ReplicaID: "test-replica", Now: now, Progress: progress})
	if err != nil {
		t.Fatal(err)
	}
	return controller
}

func openReplicaKV(t *testing.T) *pebble.DB {
	t.Helper()
	kv, err := pebble.Open(filepath.Join(t.TempDir(), "kv"), &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := kv.Close(); err != nil {
			t.Error(err)
		}
	})
	return kv
}

func testGeneration(label string, synced uint64) Generation {
	return Generation{
		UpdatedAt: time.Unix(int64(synced), 0),
		Heads:     []Head{{Name: "head", Root: testCID("root-" + label), Manifest: testCID("manifest-" + label), SyncedTo: synced}},
	}
}

func generationCID(t *testing.T, generation Generation) cid.Cid {
	t.Helper()
	generation.ReplicaID = "test-replica"
	block, err := generation.Block()
	if err != nil {
		t.Fatal(err)
	}
	return block.Cid()
}

func cidFromKey(key string) cid.Cid {
	parsed, err := cid.Cast([]byte(key))
	if err != nil {
		panic(err)
	}
	return parsed
}
