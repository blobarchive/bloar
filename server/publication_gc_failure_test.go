package server_test

import (
	"bytes"
	"crypto/ed25519"
	"encoding/hex"
	"testing"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"

	"github.com/blobarchive/bloar/archive"
	"github.com/blobarchive/bloar/catalog"
	"github.com/blobarchive/bloar/pinning"
	"github.com/blobarchive/bloar/schema"
	"github.com/blobarchive/bloar/server"
	"github.com/blobarchive/bloar/store"
)

// publicationGCFixture is a writer wired to the real pin reconciler and the
// deterministic, whole-run-excluded GC. The production daemon uses online GC;
// the epoch interleavings are covered separately. These tests need the sharper
// assertion that the very next complete collection cut after a failed
// publication still marks the last signed generation.
type publicationGCFixture struct {
	t         *testing.T
	st        *store.Store
	cat       *catalog.Catalog
	ledger    *catalog.Ledger
	roots     *server.RootStore
	manifests *server.ManifestStore
	staging   *pinning.Staging
	rec       *pinning.Reconciler
	gc        *pinning.GC
	heads     *server.Heads
	revisions *failOnceRevisions
	archive   archive.Config
	finalized *archive.Head
	mutable   *archive.Head
}

func newPublicationGCFixture(t *testing.T, withMutable bool) *publicationGCFixture {
	t.Helper()
	ctx := t.Context()
	f := &publicationGCFixture{t: t}

	var err error
	f.st, err = store.Open(t.TempDir(), store.WithPebbleLogger(quietPebble{}))
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() {
		if err := f.st.Close(); err != nil {
			t.Errorf("closing store: %v", err)
		}
	})
	f.cat = catalog.New(f.st.KV())
	f.ledger = catalog.NewLedger(f.st.KV())
	f.roots = server.NewRootStore(f.st.KV())
	f.manifests = server.NewManifestStore(f.st.KV())
	f.archive = archive.Config{Blocks: f.st.Blocks(), Resolver: f.cat}

	f.rec, err = pinning.NewReconciler(pinning.Config{
		Ledger: f.ledger, ManifestTip: f.manifests.Get,
	})
	if err != nil {
		t.Fatalf("pinning.NewReconciler: %v", err)
	}
	f.staging, err = pinning.NewStaging(pinning.StagingConfig{
		Ledger: f.ledger, Resolver: f.cat,
	})
	if err != nil {
		t.Fatalf("pinning.NewStaging: %v", err)
	}
	f.gc, err = pinning.NewGC(pinning.GCConfig{
		Blocks: f.st.Blocks(), Reconciler: f.rec, Staging: f.staging,
	})
	if err != nil {
		t.Fatalf("pinning.NewGC: %v", err)
	}

	_, key, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("ed25519.GenerateKey: %v", err)
	}
	publications := f.roots.PublicationStore()
	// Activate the signer-local v2 downgrade floor before constructing Heads.
	// This is a supported restart state and makes finalized-only mutations use
	// the same fallible revision-allocation path as a mutable publication.
	if _, err := publications.Next(ctx, key.Public().(ed25519.PublicKey)); err != nil {
		t.Fatalf("priming publication revision floor: %v", err)
	}
	f.revisions = &failOnceRevisions{PublicationRevisions: publications}

	f.finalized, err = server.OpenHead(ctx, f.archive, f.roots, archive.Params{
		Name: testHead, Net: testNet, OriginSlot: testOrigin,
		SegBits: testSegBits, FanoutBits: testFanout,
	})
	if err != nil {
		t.Fatalf("server.OpenHead: %v", err)
	}
	if err := f.rec.Add(f.finalized, pinning.Full()); err != nil {
		t.Fatalf("Reconciler.Add(%s): %v", testHead, err)
	}
	replaceFinalized, err := f.rec.BindReplacement(testHead)
	if err != nil {
		t.Fatalf("BindReplacement(%s): %v", testHead, err)
	}

	policies := map[string]server.HeadPolicy{}
	replacements := map[string]func(*archive.Head){testHead: replaceFinalized}
	if withMutable {
		f.mutable, err = server.OpenMutableHead(ctx, f.archive, f.roots, archive.Params{
			Name: mutableHead, Net: testNet, OriginSlot: testOrigin,
			SegBits: testSegBits, FanoutBits: testFanout,
		})
		if err != nil {
			t.Fatalf("server.OpenMutableHead: %v", err)
		}
		if err := f.rec.Add(f.mutable, pinning.Full()); err != nil {
			t.Fatalf("Reconciler.Add(%s): %v", mutableHead, err)
		}
		replaceMutable, err := f.rec.BindReplacement(mutableHead)
		if err != nil {
			t.Fatalf("BindReplacement(%s): %v", mutableHead, err)
		}
		policies[mutableHead] = server.HeadPolicy{
			Kind: server.UnfinalizedMutable, HandoffHead: testHead, MaxWindowSlots: 8,
		}
		replacements[mutableHead] = replaceMutable
	}

	f.heads, err = server.NewHeads(server.HeadsConfig{
		Net: testNet, Roots: f.roots, Manifests: f.manifests, Blocks: f.st.Blocks(),
		Generations: f.roots.GenerationStore(), Publications: f.revisions,
		Policies: policies, GenerationArchive: f.archive, SigningKey: key,
		Gate: f.rec.Gate(), Staging: f.staging,
		Replacements: replacements,
		OnRoot:       func(name string, _ cid.Cid) { f.rec.Notify(name) },
	})
	if err != nil {
		t.Fatalf("server.NewHeads: %v", err)
	}
	if err := f.heads.Add(f.finalized); err != nil {
		t.Fatalf("Heads.Add(%s): %v", testHead, err)
	}
	if withMutable {
		if err := f.heads.Add(f.mutable); err != nil {
			t.Fatalf("Heads.Add(%s): %v", mutableHead, err)
		}
	}
	if _, err := f.rec.ReconcileAll(ctx); err != nil {
		t.Fatalf("initial ReconcileAll: %v", err)
	}
	return f
}

// stageRawBlob makes one small, CID-valid raw leaf and protects it exactly as
// POST /blobs would. Its synthetic versioned hash is sufficient for the archive
// resolver; KZG validation belongs to ingest's tests, not publication ordering.
func (f *publicationGCFixture) stageRawBlob(seed byte) (schema.VersionedHash, cid.Cid) {
	f.t.Helper()
	ctx := f.t.Context()
	body := []byte{seed, seed + 1, seed + 2}
	hash, err := multihash.Sum(body, multihash.SHA2_256, -1)
	if err != nil {
		f.t.Fatalf("multihash.Sum: %v", err)
	}
	c := cid.NewCidV1(cid.Raw, hash)
	blk, err := blocks.NewBlockWithCid(body, c)
	if err != nil {
		f.t.Fatalf("blocks.NewBlockWithCid: %v", err)
	}
	if err := f.st.Blocks().Put(ctx, blk); err != nil {
		f.t.Fatalf("Blocks.Put(%s): %v", c, err)
	}
	var vh schema.VersionedHash
	vh[0], vh[len(vh)-1] = 1, seed
	if err := f.cat.Put(ctx, vh, c); err != nil {
		f.t.Fatalf("Catalog.Put: %v", err)
	}
	if err := f.staging.Pin(ctx, []cid.Cid{c}); err != nil {
		f.t.Fatalf("Staging.Pin: %v", err)
	}
	return vh, c
}

type publicationGCSnapshot struct {
	name string
	doc  []byte
	root cid.Cid
}

func (f *publicationGCFixture) snapshot(name string) publicationGCSnapshot {
	f.t.Helper()
	head, ok := f.heads.Get(name)
	if !ok {
		f.t.Fatalf("head %q is not selected", name)
	}
	root, ok, err := f.roots.Get(f.t.Context(), name)
	if err != nil || !ok {
		f.t.Fatalf("RootStore.Get(%q) = (_, %t, %v)", name, ok, err)
	}
	if !root.Equals(head.Root()) {
		f.t.Fatalf("selected root %s differs from durable root %s", head.Root(), root)
	}
	return publicationGCSnapshot{name: name, doc: append([]byte(nil), f.heads.Doc()...), root: root}
}

func (f *publicationGCFixture) runImmediateGC() {
	f.t.Helper()
	if _, err := f.gc.Run(f.t.Context()); err != nil {
		f.t.Fatalf("gc.Run: %v", err)
	}
}

func (f *publicationGCFixture) assertSnapshotStillAuthoritative(s publicationGCSnapshot) {
	f.t.Helper()
	ctx := f.t.Context()
	if got := f.heads.Doc(); !bytes.Equal(got, s.doc) {
		f.t.Fatalf("publication changed across failed mutation/GC:\nold %s\nnew %s", s.doc, got)
	}
	head, ok := f.heads.Get(s.name)
	if !ok || !head.Root().Equals(s.root) {
		f.t.Fatalf("selected %q after failed mutation = %v, ok=%t; want root %s", s.name, head, ok, s.root)
	}
	durable, ok, err := f.roots.Get(ctx, s.name)
	if err != nil || !ok || !durable.Equals(s.root) {
		f.t.Fatalf("durable %q root after failed mutation = %s, ok=%t err=%v; want %s", s.name, durable, ok, err, s.root)
	}
	f.assertOnlyPin(s.name, pinning.PurposeRoot, s.root)
	f.assertBlock(s.root, true)
	loaded, err := archive.Load(ctx, f.archive, s.root)
	if err != nil {
		f.t.Fatalf("archive.Load(last published root %s): %v", s.root, err)
	}
	if _, err := loaded.Enumerate(ctx); err != nil {
		f.t.Fatalf("Enumerate(last published root %s): %v", s.root, err)
	}
}

func (f *publicationGCFixture) assertOnlyPin(head, purpose string, want cid.Cid) {
	f.t.Helper()
	pins, err := f.ledger.List(f.t.Context(), head, purpose)
	if err != nil {
		f.t.Fatalf("Ledger.List(%q, %q): %v", head, purpose, err)
	}
	if len(pins) != 1 || !pins[0].CID.Equals(want) || !pins[0].Recursive {
		f.t.Fatalf("pins for %q/%q = %#v, want one recursive pin on %s", head, purpose, pins, want)
	}
}

func (f *publicationGCFixture) assertBlock(c cid.Cid, want bool) {
	f.t.Helper()
	has, err := f.st.Blocks().Has(f.t.Context(), c)
	if err != nil {
		f.t.Fatalf("Blocks.Has(%s): %v", c, err)
	}
	if has != want {
		f.t.Fatalf("Blocks.Has(%s) = %t, want %t", c, has, want)
	}
}

func (f *publicationGCFixture) assertNoStagingPins() {
	f.t.Helper()
	pins, err := f.staging.List(f.t.Context())
	if err != nil {
		f.t.Fatalf("Staging.List: %v", err)
	}
	if len(pins) != 0 {
		f.t.Fatalf("staging pins after successful retry = %#v, want none", pins)
	}
}

func publicationHeadEntry(t *testing.T, heads *server.Heads, name string) server.HeadEntry {
	t.Helper()
	for _, entry := range decodeDoc(t, heads).Heads {
		if entry.Name == name {
			return entry
		}
	}
	t.Fatalf("head %q is absent from publication", name)
	return server.HeadEntry{}
}

func TestFailedApplyRefsKeepsPublishedGenerationThroughImmediateGC(t *testing.T) {
	f := newPublicationGCFixture(t, false)
	ctx := t.Context()
	vhA, blobA := f.stageRawBlob(1)
	if _, err := f.heads.ApplyRefs(ctx, testHead, []archive.RefRow{{Slot: 9, VHs: []schema.VersionedHash{vhA}}}, 10, cid.Undef); err != nil {
		t.Fatalf("initial ApplyRefs: %v", err)
	}
	if _, err := f.rec.ReconcileAll(ctx); err != nil {
		t.Fatalf("baseline ReconcileAll: %v", err)
	}
	old := f.snapshot(testHead)

	vhB, blobB := f.stageRawBlob(2)
	batch := []archive.RefRow{{Slot: 11, VHs: []schema.VersionedHash{vhB}}}
	f.revisions.fail.Store(true)
	failed, err := f.heads.ApplyRefs(ctx, testHead, batch, 12, cid.Undef)
	if err == nil {
		t.Fatal("ApplyRefs accepted the injected publication allocation failure")
	}
	if failed.NoOp || !failed.Root.Defined() || failed.Root.Equals(old.root) {
		t.Fatalf("failed ApplyRefs did not construct a distinct candidate: %#v", failed)
	}

	f.runImmediateGC()
	f.assertSnapshotStillAuthoritative(old)
	f.assertBlock(blobA, true)
	// The failed batch has not landed, so its ingest staging pin must bridge the
	// immediate collection cut and make an exact retry possible.
	f.assertBlock(blobB, true)
	f.assertBlock(failed.Root, false)

	healed, err := f.heads.ApplyRefs(ctx, testHead, batch, 12, cid.Undef)
	if err != nil {
		t.Fatalf("retrying ApplyRefs: %v", err)
	}
	if healed.NoOp || !healed.Root.Equals(failed.Root) {
		t.Fatalf("healed ApplyRefs = %#v, want rebuilt candidate root %s", healed, failed.Root)
	}
	if bytes.Equal(f.heads.Doc(), old.doc) || publicationHeadEntry(t, f.heads, testHead).Root != healed.Root.String() {
		t.Fatal("successful retry did not install its publication")
	}
	f.runImmediateGC()
	f.assertBlock(old.root, false)
	f.assertBlock(healed.Root, true)
	f.assertBlock(blobA, true)
	f.assertBlock(blobB, true)
	f.assertOnlyPin(testHead, pinning.PurposeRoot, healed.Root)
	f.assertNoStagingPins()
}

func TestFailedTruncateKeepsPublishedGenerationThroughImmediateGC(t *testing.T) {
	f := newPublicationGCFixture(t, false)
	ctx := t.Context()
	vhA, blobA := f.stageRawBlob(11)
	vhB, blobB := f.stageRawBlob(12)
	rows := []archive.RefRow{
		{Slot: 9, VHs: []schema.VersionedHash{vhA}},
		{Slot: 11, VHs: []schema.VersionedHash{vhB}},
	}
	if _, err := f.heads.ApplyRefs(ctx, testHead, rows, 12, cid.Undef); err != nil {
		t.Fatalf("initial ApplyRefs: %v", err)
	}
	if _, err := f.rec.ReconcileAll(ctx); err != nil {
		t.Fatalf("baseline ReconcileAll: %v", err)
	}
	old := f.snapshot(testHead)

	f.revisions.fail.Store(true)
	if root, err := f.heads.Truncate(ctx, testHead, 10); err == nil {
		t.Fatalf("Truncate accepted the injected publication failure at root %s", root)
	}
	f.runImmediateGC()
	f.assertSnapshotStillAuthoritative(old)
	f.assertBlock(blobA, true)
	f.assertBlock(blobB, true)

	healed, err := f.heads.Truncate(ctx, testHead, 10)
	if err != nil {
		t.Fatalf("retrying Truncate: %v", err)
	}
	if !healed.Defined() || healed.Equals(old.root) {
		t.Fatalf("healed Truncate root = %s, want a distinct root", healed)
	}
	if bytes.Equal(f.heads.Doc(), old.doc) || publicationHeadEntry(t, f.heads, testHead).Root != healed.String() {
		t.Fatal("successful truncate retry did not install its publication")
	}
	f.runImmediateGC()
	f.assertBlock(old.root, false)
	f.assertBlock(healed, true)
	f.assertBlock(blobA, true)
	f.assertBlock(blobB, false)
	f.assertOnlyPin(testHead, pinning.PurposeRoot, healed)
}

func testPublicationManifest(t *testing.T, prev cid.Cid, marker byte) ([]byte, cid.Cid) {
	t.Helper()
	address := make([]byte, schema.AddressSize)
	topic := make([]byte, schema.TopicSize)
	address[0], topic[0] = marker, marker
	body, tip, err := schema.EncodeManifest(&schema.Manifest{
		V: schema.ManifestVersion, Head: testHead, Prev: prev,
		Sources: []schema.Source{{
			Type: schema.SourceInboxEvents, Address: address, Topic: topic,
			FromBlock: uint64(marker), OpenEnded: true,
		}},
	})
	if err != nil {
		t.Fatalf("schema.EncodeManifest: %v", err)
	}
	return body, tip
}

func TestFailedSetManifestKeepsPublishedTipThroughImmediateGC(t *testing.T) {
	f := newPublicationGCFixture(t, false)
	ctx := t.Context()
	bodyA, tipA := testPublicationManifest(t, cid.Undef, 1)
	if _, err := f.heads.SetManifest(ctx, testHead, bodyA, tipA, cid.Undef, f.finalized.Root()); err != nil {
		t.Fatalf("installing genesis manifest: %v", err)
	}
	if _, err := f.rec.ReconcileAll(ctx); err != nil {
		t.Fatalf("baseline ReconcileAll: %v", err)
	}
	old := f.snapshot(testHead)
	f.assertOnlyPin(testHead, pinning.PurposeManifest, tipA)

	bodyB, tipB := testPublicationManifest(t, tipA, 2)
	f.revisions.fail.Store(true)
	if tip, err := f.heads.SetManifest(ctx, testHead, bodyB, tipB, tipA, old.root); err == nil {
		t.Fatalf("SetManifest accepted the injected publication failure at tip %s", tip)
	}
	// Publication is prepared before the candidate block or durable tip is
	// written. A failed build therefore leaves no orphaned successor selector.
	f.assertBlock(tipB, false)
	stored, ok, err := f.manifests.Get(ctx, testHead)
	if err != nil || !ok || !stored.Equals(tipA) {
		t.Fatalf("ManifestStore after failed advance = %s, ok=%t err=%v; want %s", stored, ok, err, tipA)
	}
	if tip, ok := f.heads.ManifestTip(testHead); !ok || !tip.Equals(tipA) {
		t.Fatalf("selected manifest after failed advance = %s, ok=%t; want %s", tip, ok, tipA)
	}

	f.runImmediateGC()
	f.assertSnapshotStillAuthoritative(old)
	f.assertOnlyPin(testHead, pinning.PurposeManifest, tipA)
	f.assertBlock(tipA, true)
	f.assertBlock(tipB, false)

	healed, err := f.heads.SetManifest(ctx, testHead, bodyB, tipB, tipA, old.root)
	if err != nil {
		t.Fatalf("retrying SetManifest: %v", err)
	}
	if !healed.Equals(tipB) || publicationHeadEntry(t, f.heads, testHead).Manifest != tipB.String() {
		t.Fatalf("healed manifest = %s, publication = %#v; want %s", healed, publicationHeadEntry(t, f.heads, testHead), tipB)
	}
	if bytes.Equal(f.heads.Doc(), old.doc) {
		t.Fatal("successful manifest retry did not install its publication")
	}
	f.runImmediateGC()
	f.assertOnlyPin(testHead, pinning.PurposeManifest, tipB)
	f.assertBlock(tipB, true)
	// B links Prev=A, so the new recursive pin intentionally keeps the complete
	// attestation history. A is not a retired/collectable generation block.
	f.assertBlock(tipA, true)
	f.assertBlock(old.root, true)
}

func TestFailedReplaceGenerationKeepsPublishedGenerationThroughImmediateGC(t *testing.T) {
	f := newPublicationGCFixture(t, true)
	ctx := t.Context()
	if _, err := f.heads.ApplyRefs(ctx, testHead, nil, 10, cid.Undef); err != nil {
		t.Fatalf("advancing finalized handoff: %v", err)
	}
	vhA, blobA := f.stageRawBlob(21)
	reqA := generationReqAtCurrentHandoff(t, f.heads, generationReq(0, 10, 12, []server.GenerationRow{{
		Slot: 11, VersionedHashes: []string{"0x" + hex.EncodeToString(vhA[:])},
	}}))
	genA, err := f.heads.ReplaceGeneration(ctx, mutableHead, reqA)
	if err != nil {
		t.Fatalf("selecting generation A: %v", err)
	}
	if _, err := f.rec.ReconcileAll(ctx); err != nil {
		t.Fatalf("baseline ReconcileAll: %v", err)
	}
	old := f.snapshot(mutableHead)
	if old.root.String() != genA.Root {
		t.Fatalf("generation A root = %s, snapshot root = %s", genA.Root, old.root)
	}

	vhB, blobB := f.stageRawBlob(22)
	reqB := generationReqAtCurrentHandoff(t, f.heads, generationReq(1, 11, 13, []server.GenerationRow{{
		Slot: 12, VersionedHashes: []string{"0x" + hex.EncodeToString(vhB[:])},
	}}))
	f.revisions.fail.Store(true)
	if generation, err := f.heads.ReplaceGeneration(ctx, mutableHead, reqB); err == nil {
		t.Fatalf("ReplaceGeneration accepted the injected publication failure: %#v", generation)
	}
	status, err := f.heads.GenerationStatus(ctx, mutableHead)
	if err != nil {
		t.Fatalf("GenerationStatus after failed replacement: %v", err)
	}
	if status.Generation != 1 || status.Root != genA.Root || !status.Exposed || !status.Published {
		t.Fatalf("status after failed generation B = %#v, want selected/published generation A", status)
	}

	f.runImmediateGC()
	f.assertSnapshotStillAuthoritative(old)
	f.assertBlock(blobA, true)
	// Generation B's request is still uncommitted, so its raw leaf must remain
	// staged across the collection cut for an exact retry.
	f.assertBlock(blobB, true)

	genB, err := f.heads.ReplaceGeneration(ctx, mutableHead, reqB)
	if err != nil {
		t.Fatalf("retrying generation B: %v", err)
	}
	if genB.NoOp || genB.Generation != 2 || genB.Root == genA.Root {
		t.Fatalf("healed generation B = %#v, generation A root %s", genB, genA.Root)
	}
	if bytes.Equal(f.heads.Doc(), old.doc) || publicationHeadEntry(t, f.heads, mutableHead).Root != genB.Root {
		t.Fatal("successful generation retry did not install its publication")
	}
	f.runImmediateGC()
	rootB, err := cid.Decode(genB.Root)
	if err != nil {
		t.Fatalf("decoding generation B root: %v", err)
	}
	f.assertBlock(old.root, false)
	f.assertBlock(blobA, false)
	f.assertBlock(rootB, true)
	f.assertBlock(blobB, true)
	f.assertOnlyPin(mutableHead, pinning.PurposeRoot, rootB)
	f.assertNoStagingPins()
	status, err = f.heads.GenerationStatus(ctx, mutableHead)
	if err != nil || status.Generation != 2 || !status.Exposed || !status.Published {
		t.Fatalf("healed generation status = %#v, err=%v", status, err)
	}
}
