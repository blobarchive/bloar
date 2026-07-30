package server_test

import (
	"bytes"
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/ipfs/go-cid"

	"github.com/blobarchive/bloar/archive"
	"github.com/blobarchive/bloar/server"
	"github.com/blobarchive/bloar/store"
)

type adoptionBatchFixture struct {
	*generationFixture
	heads     *server.Heads
	manifests *server.ManifestStore
	docCalls  int
	finalized *archive.Head
	mutable   *archive.Head
}

func newAdoptionBatchFixture(t *testing.T) *adoptionBatchFixture {
	t.Helper()
	g := newGenerationFixture(t, "", nil, nil, nil)
	f := &adoptionBatchFixture{generationFixture: g, manifests: server.NewManifestStore(g.st.KV())}
	var err error
	f.heads, err = server.NewHeads(server.HeadsConfig{
		Net: testNet, Roots: g.roots, Manifests: f.manifests,
		SigningKey: g.key, Publications: g.roots.PublicationStore(),
		OnDoc: func([]byte) { f.docCalls++ },
	})
	if err != nil {
		g.close()
		t.Fatalf("server.NewHeads(follower): %v", err)
	}
	f.finalized, _ = g.heads.Get(testHead)
	f.mutable, err = archive.BuildGeneration(t.Context(), g.archive, archive.Params{
		Name: mutableHead, Net: testNet, OriginSlot: 10, SegBits: testSegBits, FanoutBits: testFanout,
	}, nil, 12)
	if err != nil {
		g.close()
		t.Fatalf("archive.BuildGeneration(mutable): %v", err)
	}
	initial := []server.Adoption{
		{Head: f.mutable, Published: entryPtr(mutablePublicationEntry(t, f.finalized, f.mutable, 10))},
		{Head: f.finalized},
	}
	if err := f.heads.AdoptBatch(t.Context(), initial, nil, server.AdoptionHooks{
		Persist: f.persist(initial, nil),
	}); err != nil {
		g.close()
		t.Fatalf("AdoptBatch(initial pair): %v", err)
	}
	return f
}

func entryPtr(entry server.HeadEntry) *server.HeadEntry { return &entry }

func (f *adoptionBatchFixture) persist(upserts []server.Adoption, withdrawals []string) func() error {
	return func() error {
		batch := f.st.KV().NewBatch()
		defer batch.Close()
		for _, adoption := range upserts {
			name := adoption.Head.Params().Name
			if err := f.roots.StagePut(batch, name, adoption.Head.Root()); err != nil {
				return err
			}
			if adoption.ManifestTip.Defined() {
				if err := f.manifests.StagePut(batch, name, adoption.ManifestTip); err != nil {
					return err
				}
			} else if err := f.manifests.StageDelete(batch, name); err != nil {
				return err
			}
		}
		for _, name := range withdrawals {
			if err := f.roots.StageDelete(batch, name); err != nil {
				return err
			}
			if err := f.manifests.StageDelete(batch, name); err != nil {
				return err
			}
		}
		return batch.Commit(pebble.Sync)
	}
}

func (f *adoptionBatchFixture) advancedFinalized(t *testing.T) *archive.Head {
	t.Helper()
	next, err := f.finalized.CloneAt(t.Context(), f.finalized.Root())
	if err != nil {
		t.Fatalf("CloneAt(finalized): %v", err)
	}
	if _, err := next.ApplyRefs(t.Context(), nil, 11); err != nil {
		t.Fatalf("ApplyRefs(finalized): %v", err)
	}
	return next
}

func currentEntry(t *testing.T, heads *server.Heads, name string) server.HeadEntry {
	t.Helper()
	for _, entry := range decodeDoc(t, heads).Heads {
		if entry.Name == name {
			return entry
		}
	}
	t.Fatalf("head %q absent from publication", name)
	return server.HeadEntry{}
}

func TestAdoptBatchBindsMutableProofToProspectiveFinalizedLineage(t *testing.T) {
	f := newAdoptionBatchFixture(t)
	defer f.close()

	oldDoc := bytes.Clone(f.heads.Doc())
	oldFinalized := currentEntry(t, f.heads, testHead)
	oldMutable := currentEntry(t, f.heads, mutableHead)
	finalizedB := f.advancedFinalized(t)
	mutableB := mutablePublicationEntry(t, finalizedB, f.mutable, 11)
	changes := []server.Adoption{
		// Mutable deliberately comes first. The implementation must bind it against
		// the complete prospective map, never the old registry or slice order.
		{Head: f.mutable, Published: &mutableB},
		{Head: finalizedB},
	}
	var order []string
	docsBefore := f.docCalls
	err := f.heads.AdoptBatch(t.Context(), changes, nil, server.AdoptionHooks{
		Persist: func() error {
			order = append(order, "persist")
			if got := currentEntry(t, f.heads, testHead); got.Root != oldFinalized.Root {
				t.Fatalf("new finalized root visible during Persist: %#v", got)
			}
			return f.persist(changes, nil)()
		},
		BeforeVisible: func() {
			order = append(order, "before-visible")
			if !bytes.Equal(f.heads.Doc(), oldDoc) {
				t.Fatal("prospective publication became visible before BeforeVisible returned")
			}
			persisted, ok, err := f.roots.Get(t.Context(), testHead)
			if err != nil || !ok || !persisted.Equals(finalizedB.Root()) {
				t.Fatalf("BeforeVisible ran before durable root install: root=%s ok=%t err=%v", persisted, ok, err)
			}
		},
	})
	if err != nil {
		t.Fatalf("AdoptBatch(prospective pair): %v", err)
	}
	if !slices.Equal(order, []string{"persist", "before-visible"}) {
		t.Fatalf("hook order = %v", order)
	}
	if f.docCalls-docsBefore != 1 {
		t.Fatalf("OnDoc calls for pair transition = %d, want 1", f.docCalls-docsBefore)
	}
	gotFinalized := currentEntry(t, f.heads, testHead)
	gotMutable := currentEntry(t, f.heads, mutableHead)
	if gotFinalized.Root != finalizedB.Root().String() || gotMutable.Root != oldMutable.Root ||
		gotMutable.HandoffRoot != gotFinalized.Root || gotMutable.HandoffSyncedTo == nil ||
		gotFinalized.SyncedTo == nil || *gotMutable.HandoffSyncedTo != *gotFinalized.SyncedTo {
		t.Fatalf("published prospective pair is incoherent: finalized=%#v mutable=%#v", gotFinalized, gotMutable)
	}
	if selected, ok := f.heads.Get(mutableHead); !ok || !selected.Root().Equals(f.mutable.Root()) {
		t.Fatalf("same-root mutable proof was not rebound to prospective finalized lineage: selected=%v ok=%t", selected, ok)
	}
}

func TestAdoptBatchPersistFailureHasZeroVisibleEffect(t *testing.T) {
	f := newAdoptionBatchFixture(t)
	defer f.close()

	beforeDoc := bytes.Clone(f.heads.Doc())
	beforeNames := slices.Clone(f.heads.Names())
	beforeFinalized := f.finalized.Root()
	beforeMutable := f.mutable.Root()
	finalizedB := f.advancedFinalized(t)
	mutableB, err := archive.BuildGeneration(t.Context(), f.archive, archive.Params{
		Name: mutableHead, Net: testNet, OriginSlot: 11, SegBits: testSegBits, FanoutBits: testFanout,
	}, nil, 13)
	if err != nil {
		t.Fatal(err)
	}
	entryB := mutablePublicationEntry(t, finalizedB, mutableB, 11)
	changes := []server.Adoption{{Head: finalizedB}, {Head: mutableB, Published: &entryB}}
	sentinel := errors.New("injected document persistence failure")
	beforeVisible := false
	docsBefore := f.docCalls
	if err := f.heads.AdoptBatch(t.Context(), changes, nil, server.AdoptionHooks{
		Persist: func() error { return sentinel },
		BeforeVisible: func() {
			beforeVisible = true
		},
	}); !errors.Is(err, sentinel) {
		t.Fatalf("AdoptBatch error = %v, want %v", err, sentinel)
	}
	if beforeVisible {
		t.Fatal("BeforeVisible ran after failed persistence")
	}
	if f.docCalls != docsBefore || !bytes.Equal(f.heads.Doc(), beforeDoc) || !slices.Equal(f.heads.Names(), beforeNames) {
		t.Fatalf("persist failure changed publication/registry: docs=%d/%d names=%v/%v", f.docCalls, docsBefore, f.heads.Names(), beforeNames)
	}
	if got, ok := f.heads.Get(testHead); !ok || !got.Root().Equals(beforeFinalized) {
		t.Fatalf("persist failure changed finalized selection: %v ok=%t", got, ok)
	}
	if got, ok := f.heads.Get(mutableHead); !ok || !got.Root().Equals(beforeMutable) {
		t.Fatalf("persist failure changed mutable selection: %v ok=%t", got, ok)
	}
}

func TestAdoptBatchInvalidChangeIsAllOrNothing(t *testing.T) {
	f := newAdoptionBatchFixture(t)
	defer f.close()

	beforeDoc := bytes.Clone(f.heads.Doc())
	finalizedB := f.advancedFinalized(t)
	bad := mutablePublicationEntry(t, finalizedB, f.mutable, 11)
	// A valid CID, but not the finalized root in the prospective document.
	bad.HandoffRoot = f.mutable.Root().String()
	persisted := false
	err := f.heads.AdoptBatch(t.Context(), []server.Adoption{{Head: f.mutable, Published: &bad}}, []string{testHead}, server.AdoptionHooks{
		Persist: func() error {
			persisted = true
			return nil
		},
	})
	if err == nil {
		t.Fatal("batch with a withdrawn handoff and invalid mutable proof was accepted")
	}
	if persisted || !bytes.Equal(f.heads.Doc(), beforeDoc) || len(f.heads.Names()) != 2 {
		t.Fatalf("invalid batch had effects: persisted=%t names=%v", persisted, f.heads.Names())
	}

	if err := f.heads.AdoptBatch(t.Context(), []server.Adoption{{Head: f.finalized}, {Head: f.finalized}}, nil,
		server.AdoptionHooks{Persist: func() error { persisted = true; return nil }}); err == nil {
		t.Fatal("duplicate upsert was accepted")
	}
	if persisted || !bytes.Equal(f.heads.Doc(), beforeDoc) {
		t.Fatal("duplicate upsert changed state")
	}

	writerDoc := bytes.Clone(f.generationFixture.heads.Doc())
	if err := f.generationFixture.heads.AdoptBatch(t.Context(), nil, []string{testHead}, server.AdoptionHooks{
		Persist: func() error { persisted = true; return nil },
	}); err == nil {
		t.Fatal("follower withdrawal removed a locally written head")
	}
	if persisted || !bytes.Equal(f.generationFixture.heads.Doc(), writerDoc) {
		t.Fatal("invalid written-head withdrawal had effects")
	}
}

func TestAdoptBatchWithdrawsWholePairAtomically(t *testing.T) {
	f := newAdoptionBatchFixture(t)
	defer f.close()

	withdrawals := []string{mutableHead, testHead}
	var order []string
	docsBefore := f.docCalls
	if err := f.heads.AdoptBatch(t.Context(), nil, withdrawals, server.AdoptionHooks{
		Persist: func() error {
			order = append(order, "persist")
			if _, ok := f.heads.Get(mutableHead); !ok {
				t.Fatal("withdrawal became visible during Persist")
			}
			return f.persist(nil, withdrawals)()
		},
		BeforeVisible: func() {
			order = append(order, "before-visible")
			if _, ok := f.heads.Get(testHead); !ok {
				t.Fatal("withdrawal became visible before BeforeVisible returned")
			}
		},
	}); err != nil {
		t.Fatalf("AdoptBatch(withdraw pair): %v", err)
	}
	if !slices.Equal(order, []string{"persist", "before-visible"}) || f.docCalls-docsBefore != 1 {
		t.Fatalf("withdraw transition order/calls = %v, docs=%d", order, f.docCalls-docsBefore)
	}
	if names := f.heads.Names(); len(names) != 0 {
		t.Fatalf("withdrawn names remain served: %v", names)
	}
	if _, ok := f.heads.Get(testHead); ok {
		t.Fatal("withdrawn finalized head remains selected")
	}
	if _, ok := f.heads.Get(mutableHead); ok {
		t.Fatal("withdrawn mutable head remains selected")
	}
	if doc := decodeDoc(t, f.heads); len(doc.Heads) != 0 {
		t.Fatalf("withdrawn pair remains published: %#v", doc.Heads)
	}
	for _, name := range withdrawals {
		if root, ok, err := f.roots.Get(t.Context(), name); err != nil || ok {
			t.Fatalf("root mirror %q after withdrawal = %s, ok=%t err=%v", name, root, ok, err)
		}
	}
}

func TestRootStoreStageDelete(t *testing.T) {
	st, err := store.Open(t.TempDir(), store.WithPebbleLogger(quietPebble{}))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	roots := server.NewRootStore(st.KV())
	root, err := cid.Decode("bafyreiadtfhdbbzr2jcw33xkx4xsvhurwfrjy2inxi2ozogubkxmio376i")
	if err != nil {
		t.Fatal(err)
	}
	if err := roots.Put(t.Context(), "followed", root); err != nil {
		t.Fatal(err)
	}
	batch := st.KV().NewBatch()
	defer batch.Close()
	if err := roots.StageDelete(batch, "followed"); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := roots.Get(t.Context(), "followed"); err != nil || !ok {
		t.Fatalf("StageDelete changed state before commit: ok=%t err=%v", ok, err)
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		t.Fatal(err)
	}
	if got, ok, err := roots.Get(t.Context(), "followed"); err != nil || ok {
		t.Fatalf("committed StageDelete = %s, ok=%t err=%v", got, ok, err)
	}
	invalid := st.KV().NewBatch()
	defer invalid.Close()
	if err := roots.StageDelete(invalid, ""); err == nil {
		t.Fatal("StageDelete accepted an empty head name")
	}
}

func TestAdoptBatchRejectsEmptyBatch(t *testing.T) {
	f := newAdoptionBatchFixture(t)
	defer f.close()
	if err := f.heads.AdoptBatch(t.Context(), nil, nil, server.AdoptionHooks{}); err == nil {
		t.Fatal("empty adoption batch was accepted")
	}
}

func TestAdoptBatchErrorIncludesConflictingName(t *testing.T) {
	f := newAdoptionBatchFixture(t)
	defer f.close()
	err := f.heads.AdoptBatch(t.Context(), []server.Adoption{{Head: f.finalized}}, []string{testHead},
		server.AdoptionHooks{Persist: func() error { return nil }})
	if err == nil || !bytes.Contains([]byte(err.Error()), []byte(fmt.Sprintf("%q", testHead))) {
		t.Fatalf("upsert/withdraw conflict error = %v", err)
	}
}
