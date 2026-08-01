package server_test

import (
	"slices"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/ipfs/go-cid"

	"github.com/blobarchive/bloar/archive"
	"github.com/blobarchive/bloar/server"
)

const (
	metadataMutableHead = "metadata-unfinalized"
	metadataHandoffHead = "metadata-finalized"
	metadataOtherHead   = "unrelated-finalized"
)

type metadataAdoptionFixture struct {
	*generationFixture
	heads     *server.Heads
	manifests *server.ManifestStore
	finalized *archive.Head
	mutable   *archive.Head
	published server.HeadEntry
	witness   server.HeadEntry
}

func newMetadataAdoptionFixture(t *testing.T) *metadataAdoptionFixture {
	t.Helper()
	g := newGenerationFixture(t, "", nil, nil, nil)
	f := &metadataAdoptionFixture{generationFixture: g, manifests: server.NewManifestStore(g.st.KV())}
	var err error
	f.heads, err = server.NewHeads(server.HeadsConfig{
		Net: testNet, Roots: g.roots, Manifests: f.manifests,
		SigningKey: g.key, Publications: g.roots.PublicationStore(),
	})
	if err != nil {
		g.close()
		t.Fatalf("server.NewHeads(metadata follower): %v", err)
	}
	f.finalized, err = archive.BuildGeneration(t.Context(), g.archive, archive.Params{
		Name: metadataHandoffHead, Net: testNet, OriginSlot: testOrigin, SegBits: testSegBits, FanoutBits: testFanout,
	}, nil, 10)
	if err != nil {
		g.close()
		t.Fatalf("archive.BuildGeneration(metadata finalized): %v", err)
	}
	f.mutable, err = archive.BuildGeneration(t.Context(), g.archive, archive.Params{
		Name: metadataMutableHead, Net: testNet, OriginSlot: 10, SegBits: testSegBits, FanoutBits: testFanout,
	}, nil, 12)
	if err != nil {
		g.close()
		t.Fatalf("archive.BuildGeneration(metadata mutable): %v", err)
	}
	f.published = mutablePublicationEntry(t, f.finalized, f.mutable, 10)
	info := f.finalized.Info()
	f.witness = server.HeadEntry{
		Name: metadataHandoffHead, Root: info.Root.String(), OriginSlot: info.OriginSlot, SyncedTo: info.SyncedTo,
		SegBits: info.SegBits, FanoutBits: info.FanoutBits, DirDepth: info.DirDepth,
	}
	f.published.HandoffHead = f.witness.Name
	f.published.HandoffRoot = f.witness.Root
	frontier := *f.witness.SyncedTo
	f.published.HandoffSyncedTo = &frontier
	return f
}

func (f *metadataAdoptionFixture) metadataAdoption() server.Adoption {
	return server.Adoption{
		Head: f.mutable, Published: &f.published, HandoffWitness: &f.witness,
	}
}

func (f *metadataAdoptionFixture) persist(upserts []server.Adoption) func() error {
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
		return batch.Commit(pebble.Sync)
	}
}

func TestAdoptBatchMetadataHandoffWitnessServesWithoutSelectingOrRepublishingWitness(t *testing.T) {
	f := newMetadataAdoptionFixture(t)
	defer f.close()
	adoption := f.metadataAdoption()
	if err := f.heads.AdoptBatch(t.Context(), []server.Adoption{adoption}, nil, server.AdoptionHooks{
		Persist: f.persist([]server.Adoption{adoption}),
	}); err != nil {
		t.Fatalf("AdoptBatch(metadata handoff): %v", err)
	}

	if selected, ok := f.heads.Get(metadataMutableHead); !ok || !selected.Root().Equals(f.mutable.Root()) {
		t.Fatalf("metadata-backed mutable selection = %v ok=%t, want %s", selected, ok, f.mutable.Root())
	}
	if names := f.heads.Names(); !slices.Equal(names, []string{metadataMutableHead}) {
		t.Fatalf("servable names = %v, want only %q", names, metadataMutableHead)
	}
	if selected, ok := f.heads.Get(metadataHandoffHead); ok {
		t.Fatalf("metadata witness became a physical head: %v", selected)
	}
	if _, ok := f.heads.HeadDoc(metadataHandoffHead); ok {
		t.Fatal("metadata witness became a publication line")
	}
	if _, ok := f.heads.HeadDoc(metadataMutableHead); ok {
		t.Fatal("signing follower republished a metadata-only mutable authority claim")
	}
	if doc := decodeDoc(t, f.heads); len(doc.Heads) != 0 {
		t.Fatalf("metadata-only adoption leaked into signed publication: %#v", doc.Heads)
	}
	if _, ok, err := f.roots.Get(t.Context(), metadataHandoffHead); err != nil || ok {
		t.Fatalf("metadata witness acquired a compatibility root mirror: ok=%t err=%v", ok, err)
	}
	if root, ok, err := f.roots.Get(t.Context(), metadataMutableHead); err != nil || !ok || !root.Equals(f.mutable.Root()) {
		t.Fatalf("mutable compatibility root = %s ok=%t err=%v, want %s", root, ok, err, f.mutable.Root())
	}
}

func TestAdoptBatchRejectsMismatchedMetadataWitnessBeforePersist(t *testing.T) {
	f := newMetadataAdoptionFixture(t)
	defer f.close()
	bad := f.metadataAdoption()
	witness := *bad.HandoffWitness
	witness.Root = f.mutable.Root().String() // valid CID, but not the root named by Published.
	bad.HandoffWitness = &witness
	persisted := false
	err := f.heads.AdoptBatch(t.Context(), []server.Adoption{bad}, nil, server.AdoptionHooks{
		Persist: func() error { persisted = true; return nil },
	})
	if err == nil {
		t.Fatal("AdoptBatch accepted a mismatched authenticated handoff witness")
	}
	if persisted {
		t.Fatal("mismatched handoff witness reached Persist")
	}
	if _, ok := f.heads.Get(metadataMutableHead); ok {
		t.Fatal("mismatched handoff witness changed the registry")
	}
	if _, ok, mirrorErr := f.roots.Get(t.Context(), metadataMutableHead); mirrorErr != nil || ok {
		t.Fatalf("mismatched handoff witness changed the root mirror: ok=%t err=%v", ok, mirrorErr)
	}
}

func TestMetadataHandoffProofSurvivesUnrelatedRegistryCoherence(t *testing.T) {
	f := newMetadataAdoptionFixture(t)
	defer f.close()
	adoption := f.metadataAdoption()
	if err := f.heads.AdoptBatch(t.Context(), []server.Adoption{adoption}, nil, server.AdoptionHooks{
		Persist: f.persist([]server.Adoption{adoption}),
	}); err != nil {
		t.Fatal(err)
	}

	// The immutable registry must own the exact witness, not aliases into the
	// caller's transport document. Make the caller's copy invalid before another
	// adoption forces coherence over every mutable entry.
	badRoot := f.mutable.Root().String()
	f.witness.Root = badRoot
	*f.witness.SyncedTo = 0

	unrelated, err := archive.BuildGeneration(t.Context(), f.archive, archive.Params{
		Name: metadataOtherHead, Net: testNet, OriginSlot: testOrigin, SegBits: testSegBits, FanoutBits: testFanout,
	}, nil, 10)
	if err != nil {
		t.Fatal(err)
	}
	other := server.Adoption{Head: unrelated}
	if err := f.heads.AdoptBatch(t.Context(), []server.Adoption{other}, nil, server.AdoptionHooks{
		Persist: f.persist([]server.Adoption{other}),
	}); err != nil {
		t.Fatalf("AdoptBatch(unrelated finalized): %v", err)
	}

	if selected, ok := f.heads.Get(metadataMutableHead); !ok || !selected.Root().Equals(f.mutable.Root()) {
		t.Fatalf("unrelated coherence invalidated metadata-backed mutable head: %v ok=%t", selected, ok)
	}
	if names := f.heads.Names(); !slices.Equal(names, []string{metadataMutableHead, metadataOtherHead}) {
		t.Fatalf("names after unrelated adoption = %v", names)
	}
	if _, ok := f.heads.Get(metadataHandoffHead); ok {
		t.Fatal("unrelated coherence materialized the metadata witness")
	}
	if _, ok := f.heads.HeadDoc(metadataMutableHead); ok {
		t.Fatal("unrelated coherence made metadata-only mutable state republishable")
	}
	doc := decodeDoc(t, f.heads)
	if len(doc.Heads) != 1 || doc.Heads[0].Name != metadataOtherHead {
		t.Fatalf("publication after unrelated adoption = %#v, want only %q", doc.Heads, metadataOtherHead)
	}
}

func TestMetadataHandoffBindsWhenExactPhysicalHeadIsLaterSelected(t *testing.T) {
	f := newMetadataAdoptionFixture(t)
	defer f.close()
	mutable := f.metadataAdoption()
	if err := f.heads.AdoptBatch(t.Context(), []server.Adoption{mutable}, nil, server.AdoptionHooks{
		Persist: f.persist([]server.Adoption{mutable}),
	}); err != nil {
		t.Fatal(err)
	}

	physical := server.Adoption{Head: f.finalized}
	if err := f.heads.AdoptBatch(t.Context(), []server.Adoption{physical}, nil, server.AdoptionHooks{
		Persist: f.persist([]server.Adoption{physical}),
	}); err != nil {
		t.Fatalf("AdoptBatch(exact physical handoff): %v", err)
	}
	for _, selected := range []struct {
		name string
		root cid.Cid
	}{{metadataHandoffHead, f.finalized.Root()}, {metadataMutableHead, f.mutable.Root()}} {
		got, ok := f.heads.Get(selected.name)
		if !ok || !got.Root().Equals(selected.root) {
			t.Fatalf("selection after exact physical bind %q = %v ok=%t, want %s", selected.name, got, ok, selected.root)
		}
	}
	// The original adoption was deliberately metadata-only, so selecting its
	// handoff later does not silently grant it publication authority. A fresh
	// ordinary pair adoption is the explicit transition which may do that.
	if _, ok := f.heads.HeadDoc(metadataMutableHead); ok {
		t.Fatal("later physical binding silently made the metadata adoption republishable")
	}
	doc := decodeDoc(t, f.heads)
	if len(doc.Heads) != 1 || doc.Heads[0].Name != metadataHandoffHead {
		t.Fatalf("publication after exact physical bind = %#v", doc.Heads)
	}
}

func TestPhysicalHandoffPresenceNeverFallsBackToMetadataWitness(t *testing.T) {
	t.Run("mismatched physical generation", func(t *testing.T) {
		f := newMetadataAdoptionFixture(t)
		defer f.close()
		mutable := f.metadataAdoption()
		if err := f.heads.AdoptBatch(t.Context(), []server.Adoption{mutable}, nil, server.AdoptionHooks{
			Persist: f.persist([]server.Adoption{mutable}),
		}); err != nil {
			t.Fatal(err)
		}

		mismatch, err := archive.BuildGeneration(t.Context(), f.archive, archive.Params{
			Name: metadataHandoffHead, Net: testNet, OriginSlot: testOrigin + 1,
			SegBits: testSegBits, FanoutBits: testFanout,
		}, nil, 10)
		if err != nil {
			t.Fatal(err)
		}
		physical := server.Adoption{Head: mismatch}
		if err := f.heads.AdoptBatch(t.Context(), []server.Adoption{physical}, nil, server.AdoptionHooks{
			Persist: f.persist([]server.Adoption{physical}),
		}); err != nil {
			t.Fatalf("AdoptBatch(mismatched physical handoff): %v", err)
		}
		if _, ok := f.heads.Get(metadataMutableHead); ok {
			t.Fatal("mutable proof fell back to metadata beside a mismatched physical handoff")
		}
		if got, ok := f.heads.Get(metadataHandoffHead); !ok || !got.Root().Equals(mismatch.Root()) {
			t.Fatalf("mismatched physical head itself = %v ok=%t, want %s", got, ok, mismatch.Root())
		}
	})

	t.Run("quarantined exact physical generation", func(t *testing.T) {
		f := newMetadataAdoptionFixture(t)
		defer f.close()
		mutable := f.metadataAdoption()
		physical := server.Adoption{Head: f.finalized}
		changes := []server.Adoption{mutable, physical}
		if err := f.heads.AdoptBatch(t.Context(), changes, nil, server.AdoptionHooks{
			Persist: f.persist(changes),
		}); err != nil {
			t.Fatal(err)
		}
		if _, ok := f.heads.Get(metadataMutableHead); !ok {
			t.Fatal("mutable head was not initially bound to exact physical handoff")
		}
		if err := f.heads.Quarantine(metadataHandoffHead, "test handoff quarantine"); err != nil {
			t.Fatal(err)
		}
		if _, ok := f.heads.Get(metadataMutableHead); ok {
			t.Fatal("mutable proof fell back to metadata beside a quarantined physical handoff")
		}
		if _, ok := f.heads.Get(metadataHandoffHead); ok {
			t.Fatal("quarantined physical handoff remained servable")
		}
	})
}

func TestAdoptBatchRejectsMetadataWitnessOnFinalizedAdoption(t *testing.T) {
	f := newMetadataAdoptionFixture(t)
	defer f.close()
	witness := f.witness
	persisted := false
	err := f.heads.AdoptBatch(t.Context(), []server.Adoption{{Head: f.finalized, HandoffWitness: &witness}}, nil,
		server.AdoptionHooks{Persist: func() error { persisted = true; return nil }})
	if err == nil || persisted {
		t.Fatalf("finalized adoption with metadata witness: err=%v persisted=%t", err, persisted)
	}
}

func TestAdoptBatchPhysicalHandoffWithWitnessKeepsOrdinaryRepublishSemantics(t *testing.T) {
	f := newMetadataAdoptionFixture(t)
	defer f.close()
	mutable := f.metadataAdoption()
	changes := []server.Adoption{{Head: f.finalized}, mutable}
	if err := f.heads.AdoptBatch(t.Context(), changes, nil, server.AdoptionHooks{
		Persist: f.persist(changes),
	}); err != nil {
		t.Fatalf("AdoptBatch(physical pair plus witness): %v", err)
	}
	for _, selected := range []struct {
		name string
		root cid.Cid
	}{{metadataHandoffHead, f.finalized.Root()}, {metadataMutableHead, f.mutable.Root()}} {
		got, ok := f.heads.Get(selected.name)
		if !ok || !got.Root().Equals(selected.root) {
			t.Fatalf("selected %q = %v ok=%t, want %s", selected.name, got, ok, selected.root)
		}
		if _, ok := f.heads.HeadDoc(selected.name); !ok {
			t.Fatalf("ordinary physical selection %q was not republished", selected.name)
		}
	}
	if names := f.heads.Names(); !slices.Equal(names, []string{metadataHandoffHead, metadataMutableHead}) {
		t.Fatalf("physical pair names = %v", names)
	}
	doc := decodeDoc(t, f.heads)
	if doc.V != server.DocVersion || doc.Revision == nil || len(doc.Heads) != 2 {
		t.Fatalf("physical pair publication = %#v", doc)
	}
}
