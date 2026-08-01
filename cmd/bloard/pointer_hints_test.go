package main

import (
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ipfs/boxo/blockstore"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/multiformats/go-multihash"

	"github.com/blobarchive/bloar/archive"
	"github.com/blobarchive/bloar/p2p"
	"github.com/blobarchive/bloar/p2p/pointerhint"
	"github.com/blobarchive/bloar/server"
)

const pointerTestNet = "pointer-testnet"

type pointerTestSchedule struct {
	mu        sync.Mutex
	snapshots []map[string]pointerhint.Set
	documents [][]cid.Cid
	failNext  error
}

func (s *pointerTestSchedule) ReplaceAllWithDocuments(candidate map[string]pointerhint.Set, documents []cid.Cid) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failNext != nil {
		err := s.failNext
		s.failNext = nil
		return err
	}
	copy := make(map[string]pointerhint.Set, len(candidate))
	for name, set := range candidate {
		copy[name] = set
	}
	s.snapshots = append(s.snapshots, copy)
	s.documents = append(s.documents, append([]cid.Cid(nil), documents...))
	return nil
}

func (s *pointerTestSchedule) lastDocuments(t *testing.T) []cid.Cid {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.documents) == 0 {
		t.Fatal("pointer schedule has no document snapshots")
	}
	return append([]cid.Cid(nil), s.documents[len(s.documents)-1]...)
}

func (s *pointerTestSchedule) failOnce(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failNext = err
}

func (s *pointerTestSchedule) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.snapshots)
}

func (s *pointerTestSchedule) last(t *testing.T) map[string]pointerhint.Set {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.snapshots) == 0 {
		t.Fatal("pointer schedule has no snapshots")
	}
	copy := make(map[string]pointerhint.Set, len(s.snapshots[len(s.snapshots)-1]))
	for name, set := range s.snapshots[len(s.snapshots)-1] {
		copy[name] = set
	}
	return copy
}

type pointerTestDocuments struct {
	mu       sync.Mutex
	staged   map[string]blocks.Block
	current  []cid.Cid
	stageN   int
	replaceN int
}

func newPointerTestDocuments() *pointerTestDocuments {
	return &pointerTestDocuments{staged: make(map[string]blocks.Block)}
}

func (s *pointerTestDocuments) StageCurrentAfterVerification(documents []blocks.Block) error {
	owned := make(map[string]blocks.Block, len(documents))
	for _, document := range documents {
		if document == nil {
			return errors.New("nil document")
		}
		computed, err := p2p.NewDocumentBlock(document.RawData())
		if err != nil {
			return err
		}
		if !computed.Cid().Equals(document.Cid()) {
			return fmt.Errorf("document bytes mismatch %s", document.Cid())
		}
		owned[computed.Cid().KeyString()] = computed
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stageN++
	for key, document := range owned {
		s.staged[key] = document
	}
	return nil
}

func (s *pointerTestDocuments) ReplaceCurrentDocuments(current []cid.Cid) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, document := range current {
		if _, ok := s.staged[document.KeyString()]; !ok {
			return fmt.Errorf("document %s was not staged", document)
		}
	}
	s.current = append([]cid.Cid(nil), current...)
	s.replaceN++
	return nil
}

func (s *pointerTestDocuments) currentSet() map[string]struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make(map[string]struct{}, len(s.current))
	for _, c := range s.current {
		result[c.KeyString()] = struct{}{}
	}
	return result
}

func newPointerTestState(t *testing.T, written, followed []string, signer ed25519.PublicKey) (*pointerState, *pointerTestSchedule, *pointerTestDocuments) {
	t.Helper()
	schedule := &pointerTestSchedule{}
	documents := newPointerTestDocuments()
	state, err := newPointerState(pointerStateConfig{
		Net:               pointerTestNet,
		WrittenHeads:      pointerNameSet(written...),
		FollowedHeads:     pointerNameSet(followed...),
		LocalSigner:       signer,
		Coordinator:       schedule,
		VerifiedDocuments: documents,
	})
	if err != nil {
		t.Fatalf("newPointerState: %v", err)
	}
	t.Cleanup(state.Close)
	return state, schedule, documents
}

func pointerNameSet(names ...string) map[string]struct{} {
	result := make(map[string]struct{}, len(names))
	for _, name := range names {
		result[name] = struct{}{}
	}
	return result
}

func pointerTestCID(t *testing.T, label string) cid.Cid {
	t.Helper()
	c, err := cid.Prefix{Version: 1, Codec: cid.DagCBOR, MhType: multihash.SHA2_256, MhLength: 32}.Sum([]byte(label))
	if err != nil {
		t.Fatalf("test CID %q: %v", label, err)
	}
	return c
}

func pointerEntry(t *testing.T, name, root, manifest string) server.HeadEntry {
	t.Helper()
	synced := uint64(10)
	return server.HeadEntry{
		Name:       name,
		Root:       root,
		OriginSlot: 1,
		SyncedTo:   &synced,
		SegBits:    4,
		FanoutBits: 4,
		DirDepth:   1,
		Manifest:   manifest,
	}
}

func pointerSignedDocument(t *testing.T, key ed25519.PrivateKey, net string, revision *uint64, entries ...server.HeadEntry) (server.Doc, []byte, blocks.Block) {
	t.Helper()
	version := server.LegacyDocVersion
	if revision != nil {
		version = server.DocVersion
	}
	unsigned := server.Unsigned{
		V:         version,
		Net:       net,
		UpdatedAt: "2026-07-22T00:00:00Z",
		Heads:     entries,
		Revision:  revision,
	}
	doc := server.Doc{Unsigned: unsigned}
	if key != nil {
		canonical, err := unsigned.Canonical()
		if err != nil {
			t.Fatalf("canonical document: %v", err)
		}
		doc.Pubkey = hex.EncodeToString(key.Public().(ed25519.PublicKey))
		doc.Signature = hex.EncodeToString(ed25519.Sign(key, canonical))
	}
	if err := doc.ValidateContract(); err != nil {
		t.Fatalf("test document contract: %v", err)
	}
	raw, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("marshal document: %v", err)
	}
	block, err := p2p.NewDocumentBlock(raw)
	if err != nil {
		t.Fatalf("document block: %v", err)
	}
	return doc, raw, block
}

func pointerKey(t *testing.T) (ed25519.PublicKey, ed25519.PrivateKey) {
	t.Helper()
	public, private, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	return public, private
}

func requirePointerSet(t *testing.T, snapshot map[string]pointerhint.Set, name string, root, manifest, document cid.Cid) {
	t.Helper()
	set, ok := snapshot[name]
	if !ok {
		t.Fatalf("pointer snapshot lacks %q: %#v", name, snapshot)
	}
	if !set.Root.Equals(root) || !equalOptionalCID(set.Manifest, manifest) || !equalOptionalCID(set.Document, document) {
		t.Fatalf("pointer %q = root %s manifest %s document %s, want %s %s %s",
			name, set.Root, set.Manifest, set.Document, root, manifest, document)
	}
}

func requireCurrentDocuments(t *testing.T, documents *pointerTestDocuments, want ...cid.Cid) {
	t.Helper()
	got := documents.currentSet()
	if len(got) != len(want) {
		t.Fatalf("current document count = %d, want %d (%v)", len(got), len(want), got)
	}
	for _, c := range want {
		if _, ok := got[c.KeyString()]; !ok {
			t.Errorf("current documents lack %s", c)
		}
	}
}

func requireScheduledDocuments(t *testing.T, schedule *pointerTestSchedule, want ...cid.Cid) {
	t.Helper()
	got := schedule.lastDocuments(t)
	if len(got) != len(want) {
		t.Fatalf("node-local scheduled document count = %d, want %d (%v)", len(got), len(want), got)
	}
	for i, document := range want {
		if !got[i].Equals(document) {
			t.Errorf("node-local scheduled document[%d] = %s, want %s", i, got[i], document)
		}
	}
}

func TestPointerStateComposesWrittenAndFollowedWithoutClobber(t *testing.T) {
	localPublic, localPrivate := pointerKey(t)
	_, upstreamPrivate := pointerKey(t)
	state, schedule, documents := newPointerTestState(t, []string{"written"}, []string{"followed"}, localPublic)

	writtenRoot := pointerTestCID(t, "written-root-1")
	writtenManifest := pointerTestCID(t, "written-manifest-1")
	_, writtenRaw, writtenBlock := pointerSignedDocument(t, localPrivate, pointerTestNet, nil,
		pointerEntry(t, "written", writtenRoot.String(), writtenManifest.String()),
		pointerEntry(t, "followed", pointerTestCID(t, "ignored-local-followed").String(), ""),
		pointerEntry(t, "unconfigured", pointerTestCID(t, "ignored-unconfigured").String(), ""),
	)
	if err := state.admitLocalDocument(writtenRaw); err != nil {
		t.Fatalf("admit local: %v", err)
	}
	first := schedule.last(t)
	if len(first) != 1 {
		t.Fatalf("local snapshot heads = %d, want only configured written head: %#v", len(first), first)
	}
	requirePointerSet(t, first, "written", writtenRoot, writtenManifest, writtenBlock.Cid())
	requireScheduledDocuments(t, schedule, writtenBlock.Cid())

	followedRoot := pointerTestCID(t, "followed-root-1")
	followedManifest := pointerTestCID(t, "followed-manifest-1")
	followedDoc, _, followedBlock := pointerSignedDocument(t, upstreamPrivate, pointerTestNet, nil,
		pointerEntry(t, "followed", followedRoot.String(), followedManifest.String()),
	)
	if err := state.AdmitFollowedDocument(pointerDocumentReader(t, followedDoc), followedBlock, followedDoc); err != nil {
		t.Fatalf("admit followed: %v", err)
	}
	union := schedule.last(t)
	if len(union) != 2 {
		t.Fatalf("union heads = %d, want 2: %#v", len(union), union)
	}
	requirePointerSet(t, union, "written", writtenRoot, writtenManifest, writtenBlock.Cid())
	requirePointerSet(t, union, "followed", followedRoot, followedManifest, followedBlock.Cid())
	requireCurrentDocuments(t, documents, writtenBlock.Cid(), followedBlock.Cid())

	writtenRoot2 := pointerTestCID(t, "written-root-2")
	_, writtenRaw2, writtenBlock2 := pointerSignedDocument(t, localPrivate, pointerTestNet, nil,
		pointerEntry(t, "written", writtenRoot2.String(), ""),
	)
	if err := state.admitLocalDocument(writtenRaw2); err != nil {
		t.Fatalf("update local: %v", err)
	}
	union = schedule.last(t)
	requirePointerSet(t, union, "written", writtenRoot2, cid.Undef, writtenBlock2.Cid())
	requirePointerSet(t, union, "followed", followedRoot, followedManifest, followedBlock.Cid())
	requireCurrentDocuments(t, documents, writtenBlock2.Cid(), followedBlock.Cid())
}

func TestPointerStateKeepsUpstreamAndLocallyRepublishedDocumentsDiscoverable(t *testing.T) {
	localPublic, localPrivate := pointerKey(t)
	_, upstreamPrivate := pointerKey(t)
	state, schedule, documents := newPointerTestState(t, nil, []string{"followed"}, localPublic)

	root := pointerTestCID(t, "republished-followed-root")
	upstreamDoc, _, upstreamBlock := pointerSignedDocument(t, upstreamPrivate, pointerTestNet, nil,
		pointerEntry(t, "followed", root.String(), ""))
	if err := state.AdmitFollowedDocument(pointerDocumentReader(t, upstreamDoc), upstreamBlock, upstreamDoc); err != nil {
		t.Fatalf("admit upstream source document: %v", err)
	}

	// A follower can publish its re-rendered registry under its own IPNS name.
	// That exact local document is distinct from the upstream source document:
	// downstream followers of either name must be able to discover the one they
	// resolved without clobbering the other path.
	_, localRaw, localBlock := pointerSignedDocument(t, localPrivate, pointerTestNet, nil,
		pointerEntry(t, "followed", root.String(), ""))
	if localBlock.Cid().Equals(upstreamBlock.Cid()) {
		t.Fatal("test signer change did not produce a distinct local publication document")
	}
	if err := state.admitLocalDocument(localRaw); err != nil {
		t.Fatalf("admit locally republished document: %v", err)
	}

	snapshot := schedule.last(t)
	if len(snapshot) != 1 {
		t.Fatalf("republishing follower changed real-head schedule cardinality: %#v", snapshot)
	}
	requirePointerSet(t, snapshot, "followed", root, cid.Undef, upstreamBlock.Cid())
	requireScheduledDocuments(t, schedule, localBlock.Cid())
	requireCurrentDocuments(t, documents, upstreamBlock.Cid(), localBlock.Cid())
}

func TestPointerStateKeepsQuietFollowedDocumentProtectedAcrossWriterChurn(t *testing.T) {
	localPublic, localPrivate := pointerKey(t)
	_, upstreamPrivate := pointerKey(t)
	schedule := &pointerTestSchedule{}
	base := blockstore.NewBlockstore(dssync.MutexWrap(datastore.NewMapDatastore()))
	// Capacity one makes ordinary verified history maximally hostile to the
	// quiet followed document: each local publication would evict it without
	// the tracker's explicit current-document set.
	documents, err := pointerhint.NewVerifiedDocumentStore(base, 1)
	if err != nil {
		t.Fatalf("NewVerifiedDocumentStore: %v", err)
	}
	state, err := newPointerState(pointerStateConfig{
		Net:               pointerTestNet,
		WrittenHeads:      pointerNameSet("written"),
		FollowedHeads:     pointerNameSet("followed"),
		LocalSigner:       localPublic,
		Coordinator:       schedule,
		VerifiedDocuments: documents,
	})
	if err != nil {
		t.Fatalf("newPointerState: %v", err)
	}
	t.Cleanup(state.Close)

	followedRoot := pointerTestCID(t, "quiet-followed-root")
	followedDoc, _, followedBlock := pointerSignedDocument(t, upstreamPrivate, pointerTestNet, nil,
		pointerEntry(t, "followed", followedRoot.String(), ""))
	if err := state.AdmitFollowedDocument(pointerDocumentReader(t, followedDoc), followedBlock, followedDoc); err != nil {
		t.Fatalf("admit quiet followed document: %v", err)
	}

	for i := range 24 {
		root := pointerTestCID(t, fmt.Sprintf("churning-written-root-%d", i))
		_, raw, _ := pointerSignedDocument(t, localPrivate, pointerTestNet, nil,
			pointerEntry(t, "written", root.String(), ""))
		if err := state.admitLocalDocument(raw); err != nil {
			t.Fatalf("local churn %d: %v", i, err)
		}
	}
	if ok, err := documents.HasVerified(t.Context(), followedBlock.Cid()); err != nil || !ok {
		t.Fatalf("quiet followed document eligibility after writer churn = %v, %v; want true, nil", ok, err)
	}
	got, err := documents.Get(t.Context(), followedBlock.Cid())
	if err != nil {
		t.Fatalf("get quiet followed document: %v", err)
	}
	if string(got.RawData()) != string(followedBlock.RawData()) {
		t.Fatal("quiet followed document bytes changed")
	}
	final := schedule.last(t)
	requirePointerSet(t, final, "followed", followedRoot, cid.Undef, followedBlock.Cid())
}

func TestPointerStateFollowerOmissionContracts(t *testing.T) {
	_, upstreamPrivate := pointerKey(t)
	state, schedule, documents := newPointerTestState(t, nil, []string{"alpha", "beta"}, nil)

	alpha1 := pointerTestCID(t, "alpha-1")
	beta1 := pointerTestCID(t, "beta-1")
	doc1, _, block1 := pointerSignedDocument(t, upstreamPrivate, pointerTestNet, nil,
		pointerEntry(t, "alpha", alpha1.String(), ""),
		pointerEntry(t, "beta", beta1.String(), ""),
	)
	reader := pointerDocumentReader(t, doc1)
	if err := state.AdmitFollowedDocument(reader, block1, doc1); err != nil {
		t.Fatalf("initial legacy admission: %v", err)
	}

	alpha2 := pointerTestCID(t, "alpha-2")
	legacy, _, legacyBlock := pointerSignedDocument(t, upstreamPrivate, pointerTestNet, nil,
		pointerEntry(t, "alpha", alpha2.String(), ""),
	)
	reader.replace(map[string][]byte{
		"alpha": pointerHeadDoc(t, legacy.Heads[0]),
		"beta":  pointerHeadDoc(t, doc1.Heads[1]),
	})
	if err := state.AdmitFollowedDocument(reader, legacyBlock, legacy); err != nil {
		t.Fatalf("legacy omission: %v", err)
	}
	snapshot := schedule.last(t)
	requirePointerSet(t, snapshot, "alpha", alpha2, cid.Undef, legacyBlock.Cid())
	requirePointerSet(t, snapshot, "beta", beta1, cid.Undef, block1.Cid())
	requireCurrentDocuments(t, documents, legacyBlock.Cid(), block1.Cid())

	revision := uint64(2)
	revisioned, _, revisionedBlock := pointerSignedDocument(t, upstreamPrivate, pointerTestNet, &revision,
		pointerEntry(t, "alpha", alpha2.String(), ""),
	)
	reader.replace(map[string][]byte{"alpha": pointerHeadDoc(t, revisioned.Heads[0])})
	if err := state.AdmitFollowedDocument(reader, revisionedBlock, revisioned); err != nil {
		t.Fatalf("revisioned omission: %v", err)
	}
	snapshot = schedule.last(t)
	if _, ok := snapshot["beta"]; ok {
		t.Fatalf("revisioned omission preserved beta: %#v", snapshot)
	}
	requirePointerSet(t, snapshot, "alpha", alpha2, cid.Undef, revisionedBlock.Cid())
	requireCurrentDocuments(t, documents, revisionedBlock.Cid())

	revision = 3
	uncoveredEntry := pointerEntry(t, "alpha", alpha2.String(), "")
	uncoveredEntry.SyncedTo = nil
	uncovered, _, uncoveredBlock := pointerSignedDocument(t, upstreamPrivate, pointerTestNet, &revision, uncoveredEntry)
	reader.replace(map[string][]byte{})
	if err := state.AdmitFollowedDocument(reader, uncoveredBlock, uncovered); err != nil {
		t.Fatalf("revisioned uncovered withdrawal: %v", err)
	}
	if snapshot = schedule.last(t); len(snapshot) != 0 {
		t.Fatalf("revisioned nil synced_to left pointers: %#v", snapshot)
	}
	requireCurrentDocuments(t, documents)

	revision = 4
	reintroduced, _, reintroducedBlock := pointerSignedDocument(t, upstreamPrivate, pointerTestNet, &revision,
		pointerEntry(t, "alpha", alpha2.String(), ""))
	reader.replace(map[string][]byte{"alpha": pointerHeadDoc(t, reintroduced.Heads[0])})
	if err := state.AdmitFollowedDocument(reader, reintroducedBlock, reintroduced); err != nil {
		t.Fatalf("revisioned reintroduction: %v", err)
	}
	requirePointerSet(t, schedule.last(t), "alpha", alpha2, cid.Undef, reintroducedBlock.Cid())

	revision = 5
	empty, _, emptyBlock := pointerSignedDocument(t, upstreamPrivate, pointerTestNet, &revision,
		pointerEntry(t, "alpha", "", ""),
	)
	reader.replace(map[string][]byte{})
	if err := state.AdmitFollowedDocument(reader, emptyBlock, empty); err != nil {
		t.Fatalf("revisioned empty withdrawal: %v", err)
	}
	if snapshot = schedule.last(t); len(snapshot) != 0 {
		t.Fatalf("revisioned empty root left pointers: %#v", snapshot)
	}
	requireCurrentDocuments(t, documents)
}

func TestPointerStateSourceDocumentsAttachOnlyToCurrentWinningHeads(t *testing.T) {
	_, sourceAPrivate := pointerKey(t)
	_, sourceBPrivate := pointerKey(t)
	state, schedule, documents := newPointerTestState(t, nil, []string{"alpha", "beta"}, nil)

	alphaRoot := pointerTestCID(t, "selected-alpha-root")
	alphaManifest := pointerTestCID(t, "selected-alpha-manifest")
	betaRoot := pointerTestCID(t, "selected-beta-root")
	betaManifest := pointerTestCID(t, "selected-beta-manifest")
	currentAlpha := pointerEntry(t, "alpha", alphaRoot.String(), alphaManifest.String())
	currentBeta := pointerEntry(t, "beta", betaRoot.String(), betaManifest.String())
	reader := &pointerHeadDocuments{docs: map[string][]byte{
		"alpha": pointerHeadDoc(t, currentAlpha),
		"beta":  pointerHeadDoc(t, currentBeta),
	}}

	revisionA := uint64(10)
	docA, _, blockA := pointerSignedDocument(t, sourceAPrivate, pointerTestNet, &revisionA,
		currentAlpha,
		pointerEntry(t, "beta", pointerTestCID(t, "source-a-losing-beta").String(), betaManifest.String()),
	)
	if err := state.AdmitFollowedDocument(reader, blockA, docA, []string{"alpha"}); err != nil {
		t.Fatalf("admit source A: %v", err)
	}
	first := schedule.last(t)
	requirePointerSet(t, first, "alpha", alphaRoot, alphaManifest, blockA.Cid())
	requirePointerSet(t, first, "beta", betaRoot, betaManifest, cid.Undef)
	requireCurrentDocuments(t, documents, blockA.Cid())

	revisionB := uint64(20)
	docB, _, blockB := pointerSignedDocument(t, sourceBPrivate, pointerTestNet, &revisionB,
		currentAlpha, currentBeta)
	if err := state.AdmitFollowedDocument(reader, blockB, docB, []string{"beta"}); err != nil {
		t.Fatalf("admit source B: %v", err)
	}
	second := schedule.last(t)
	requirePointerSet(t, second, "alpha", alphaRoot, alphaManifest, blockA.Cid())
	requirePointerSet(t, second, "beta", betaRoot, betaManifest, blockB.Cid())
	requireCurrentDocuments(t, documents, blockA.Cid(), blockB.Cid())

	// An unallowed line is outside local policy entirely, not merely ineligible
	// to win. Even malformed pointer fields there cannot suppress the authorized
	// beta update or disturb alpha's selected source document.
	revisionB++
	malformedAlpha := currentAlpha
	malformedAlpha.Root = "not-a-cid"
	docB2, _, blockB2 := pointerSignedDocument(t, sourceBPrivate, pointerTestNet, &revisionB,
		malformedAlpha, currentBeta)
	if err := state.AdmitFollowedDocument(reader, blockB2, docB2, []string{"beta"}); err != nil {
		t.Fatalf("admit source B with malformed unallowed alpha: %v", err)
	}
	second = schedule.last(t)
	requirePointerSet(t, second, "alpha", alphaRoot, alphaManifest, blockA.Cid())
	requirePointerSet(t, second, "beta", betaRoot, betaManifest, blockB2.Cid())
	requireCurrentDocuments(t, documents, blockA.Cid(), blockB2.Cid())

	// A later callback is evidence that another source document was admitted,
	// not that it won either head. It must neither erase the two independent
	// winners nor advertise its losing roots or document CID.
	revisionLosing := uint64(30)
	losing, _, losingBlock := pointerSignedDocument(t, sourceAPrivate, pointerTestNet, &revisionLosing,
		pointerEntry(t, "alpha", pointerTestCID(t, "later-losing-alpha").String(), alphaManifest.String()),
		pointerEntry(t, "beta", pointerTestCID(t, "later-losing-beta").String(), betaManifest.String()),
	)
	if err := state.AdmitFollowedDocument(reader, losingBlock, losing, []string{"alpha", "beta"}); err != nil {
		t.Fatalf("admit losing source document: %v", err)
	}
	final := schedule.last(t)
	requirePointerSet(t, final, "alpha", alphaRoot, alphaManifest, blockA.Cid())
	requirePointerSet(t, final, "beta", betaRoot, betaManifest, blockB2.Cid())
	requireCurrentDocuments(t, documents, blockA.Cid(), blockB2.Cid())
}

func TestPointerStateSourceDocumentRequiresCurrentFullHeadEntry(t *testing.T) {
	_, sourceAPrivate := pointerKey(t)
	_, sourceBPrivate := pointerKey(t)
	state, schedule, documents := newPointerTestState(t, nil, []string{"alpha"}, nil)
	root := pointerTestCID(t, "exact-entry-root")
	manifest := pointerTestCID(t, "exact-entry-manifest")
	current := pointerEntry(t, "alpha", root.String(), manifest.String())
	reader := &pointerHeadDocuments{docs: map[string][]byte{"alpha": pointerHeadDoc(t, current)}}

	revisionA := uint64(1)
	docA, _, blockA := pointerSignedDocument(t, sourceAPrivate, pointerTestNet, &revisionA, current)
	if err := state.AdmitFollowedDocument(reader, blockA, docA, []string{"alpha"}); err != nil {
		t.Fatalf("admit current source document: %v", err)
	}
	requirePointerSet(t, schedule.last(t), "alpha", root, manifest, blockA.Cid())

	// Root and manifest alone are not enough. This validly signed line lies about
	// authenticated coverage while reusing both pointer CIDs; it must not replace
	// the exact document which matches the current registry line.
	forged := current
	forgedTo := *current.SyncedTo + 1
	forged.SyncedTo = &forgedTo
	revisionB := uint64(2)
	docB, _, blockB := pointerSignedDocument(t, sourceBPrivate, pointerTestNet, &revisionB, forged)
	if err := state.AdmitFollowedDocument(reader, blockB, docB, []string{"alpha"}); err != nil {
		t.Fatalf("process nonmatching source document: %v", err)
	}
	requirePointerSet(t, schedule.last(t), "alpha", root, manifest, blockA.Cid())
	requireCurrentDocuments(t, documents, blockA.Cid())

	// The same rule applies while refreshing without a new callback: if the
	// serviceable registry line changes at the same root/manifest, the old exact
	// document is no longer proof for it and must be withdrawn.
	reader.replace(map[string][]byte{"alpha": pointerHeadDoc(t, forged)})
	if err := state.RefreshFollowed(reader); err != nil {
		t.Fatalf("refresh changed full entry: %v", err)
	}
	requirePointerSet(t, schedule.last(t), "alpha", root, manifest, cid.Undef)
	requireCurrentDocuments(t, documents)
}

func TestPointerStateAcceptsExplicitFinalizedKindForLocalOmittedKind(t *testing.T) {
	_, sourcePrivate := pointerKey(t)
	state, schedule, _ := newPointerTestState(t, nil, []string{"alpha"}, nil)
	root := pointerTestCID(t, "explicit-finalized-root")
	current := pointerEntry(t, "alpha", root.String(), "")
	reader := &pointerHeadDocuments{docs: map[string][]byte{"alpha": pointerHeadDoc(t, current)}}
	published := current
	published.Kind = server.FinalizedMonotonic
	revision := uint64(1)
	doc, _, block := pointerSignedDocument(t, sourcePrivate, pointerTestNet, &revision, published)
	if err := state.AdmitFollowedDocument(reader, block, doc, []string{"alpha"}); err != nil {
		t.Fatalf("admit explicit finalized kind: %v", err)
	}
	requirePointerSet(t, schedule.last(t), "alpha", root, cid.Undef, block.Cid())
}

type pointerHeadDocuments struct {
	mu          sync.Mutex
	docs        map[string][]byte
	serviceable map[string]bool
	reads       []string
}

func (r *pointerHeadDocuments) HeadDoc(name string) ([]byte, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.reads = append(r.reads, name)
	raw, ok := r.docs[name]
	return append([]byte(nil), raw...), ok
}

func (r *pointerHeadDocuments) Get(name string) (*archive.Head, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.reads = append(r.reads, name)
	if r.serviceable != nil {
		return nil, r.serviceable[name]
	}
	_, ok := r.docs[name]
	return nil, ok
}

func (r *pointerHeadDocuments) replace(docs map[string][]byte) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.docs = docs
	r.serviceable = nil
	r.reads = nil
}

func (r *pointerHeadDocuments) replaceServiceability(serviceable map[string]bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.serviceable = serviceable
	r.reads = nil
}

func (r *pointerHeadDocuments) readSet() map[string]struct{} {
	r.mu.Lock()
	defer r.mu.Unlock()
	result := make(map[string]struct{}, len(r.reads))
	for _, name := range r.reads {
		result[name] = struct{}{}
	}
	return result
}

func pointerHeadDoc(t *testing.T, entry server.HeadEntry) []byte {
	t.Helper()
	raw, err := json.Marshal(entry)
	if err != nil {
		t.Fatalf("marshal head entry: %v", err)
	}
	return raw
}

func pointerDocumentReader(t *testing.T, doc server.Doc) *pointerHeadDocuments {
	t.Helper()
	docs := make(map[string][]byte, len(doc.Heads))
	for _, entry := range doc.Heads {
		if entry.Root == "" || entry.SyncedTo == nil {
			continue
		}
		docs[entry.Name] = pointerHeadDoc(t, entry)
	}
	return &pointerHeadDocuments{docs: docs}
}

func TestPointerStateRestoreFailsClosedAndQuarantineRefreshesAllHeads(t *testing.T) {
	_, upstreamPrivate := pointerKey(t)
	state, schedule, documents := newPointerTestState(t, nil, []string{"finalized", "live"}, nil)
	finalizedRoot := pointerTestCID(t, "restore-finalized")
	liveRoot := pointerTestCID(t, "restore-live")
	revision := uint64(1)
	doc, _, block := pointerSignedDocument(t, upstreamPrivate, pointerTestNet, &revision,
		pointerEntry(t, "finalized", finalizedRoot.String(), ""),
		pointerEntry(t, "live", liveRoot.String(), ""),
	)
	reader := &pointerHeadDocuments{docs: map[string][]byte{
		"finalized": pointerHeadDoc(t, doc.Heads[0]),
		"live":      pointerHeadDoc(t, doc.Heads[1]),
	}}
	if err := state.AdmitFollowedDocument(reader, block, doc); err != nil {
		t.Fatalf("admit followed: %v", err)
	}
	if err := state.RefreshFollowed(reader); err != nil {
		t.Fatalf("refresh serviceable heads: %v", err)
	}
	refreshed := schedule.last(t)
	requirePointerSet(t, refreshed, "finalized", finalizedRoot, cid.Undef, block.Cid())
	requirePointerSet(t, refreshed, "live", liveRoot, cid.Undef, block.Cid())
	requireCurrentDocuments(t, documents, block.Cid())

	// A malformed still-serviceable registry entry must not make a quarantine
	// refresh fail open by leaving the prior complete followed schedule live.
	// The scan error remains visible, but every followed pointer and source
	// document is withdrawn transactionally.
	reader.replace(map[string][]byte{
		"finalized": pointerHeadDoc(t, doc.Heads[0]),
		"live":      []byte("{"),
	})
	reader.replaceServiceability(map[string]bool{"live": true})
	if err := state.RefreshFollowed(reader); err == nil {
		t.Fatal("malformed serviceable refresh returned nil")
	}
	if got := schedule.last(t); len(got) != 0 {
		t.Fatalf("malformed refresh left followed pointers: %#v", got)
	}
	requireCurrentDocuments(t, documents)

	// Re-admission after the local fault is repaired reconstructs the source
	// document state; Follower clears its same-CID success marker when the
	// serviceability callback reports this error.
	reader.replace(map[string][]byte{
		"finalized": pointerHeadDoc(t, doc.Heads[0]),
		"live":      pointerHeadDoc(t, doc.Heads[1]),
	})
	if err := state.AdmitFollowedDocument(reader, block, doc); err != nil {
		t.Fatalf("readmit followed after malformed refresh: %v", err)
	}
	requirePointerSet(t, schedule.last(t), "finalized", finalizedRoot, cid.Undef, block.Cid())
	requirePointerSet(t, schedule.last(t), "live", liveRoot, cid.Undef, block.Cid())
	requireCurrentDocuments(t, documents, block.Cid())

	// The registry omits both the quarantined finalized head and its dependent
	// mutable head. Refresh must ask about every configured name and withdraw
	// both, not merely delete the name supplied by the quarantine event.
	// Model the fail-closed ordering in server.Heads.Quarantine: registry
	// serviceability changes before rendering/signing the replacement document.
	// Even if that rebuild fails and HeadDoc stays stale, the pointers withdraw.
	reader.replaceServiceability(map[string]bool{})
	if err := state.RefreshFollowed(reader); err != nil {
		t.Fatalf("refresh after quarantine: %v", err)
	}
	if got := schedule.last(t); len(got) != 0 {
		t.Fatalf("quarantine refresh left dependent pointers: %#v", got)
	}
	reads := reader.readSet()
	for _, name := range []string{"finalized", "live"} {
		if _, ok := reads[name]; !ok {
			t.Errorf("quarantine refresh did not rescan %q", name)
		}
	}
	requireCurrentDocuments(t, documents)

	// A fresh process can reconstruct durable root/manifest state but cannot
	// reconstruct the exact authenticated upstream document CID.
	restart, restartSchedule, restartDocuments := newPointerTestState(t, nil, []string{"finalized", "live"}, nil)
	reader.replace(map[string][]byte{
		"finalized": pointerHeadDoc(t, doc.Heads[0]),
		"live":      pointerHeadDoc(t, doc.Heads[1]),
	})
	if err := restart.RestoreFollowed(reader); err != nil {
		t.Fatalf("restart restore: %v", err)
	}
	restored := restartSchedule.last(t)
	requirePointerSet(t, restored, "finalized", finalizedRoot, cid.Undef, cid.Undef)
	requirePointerSet(t, restored, "live", liveRoot, cid.Undef, cid.Undef)
	requireCurrentDocuments(t, restartDocuments)
}

func TestPointerStateInvalidInputsFailClosed(t *testing.T) {
	localPublic, localPrivate := pointerKey(t)
	_, wrongPrivate := pointerKey(t)
	root := pointerTestCID(t, "invalid-root")
	validDoc, validRaw, validBlock := pointerSignedDocument(t, localPrivate, pointerTestNet, nil,
		pointerEntry(t, "written", root.String(), ""),
	)

	tests := []struct {
		name string
		run  func(*pointerState) error
	}{
		{
			name: "malformed local document",
			run:  func(state *pointerState) error { return state.admitLocalDocument([]byte("{")) },
		},
		{
			name: "wrong local network",
			run: func(state *pointerState) error {
				_, raw, _ := pointerSignedDocument(t, localPrivate, "other-net", nil,
					pointerEntry(t, "written", root.String(), ""))
				return state.admitLocalDocument(raw)
			},
		},
		{
			name: "wrong local signer",
			run: func(state *pointerState) error {
				_, raw, _ := pointerSignedDocument(t, wrongPrivate, pointerTestNet, nil,
					pointerEntry(t, "written", root.String(), ""))
				return state.admitLocalDocument(raw)
			},
		},
		{
			name: "invalid local signature",
			run: func(state *pointerState) error {
				broken := validDoc
				broken.Signature = hex.EncodeToString(make([]byte, ed25519.SignatureSize))
				raw, err := json.Marshal(broken)
				if err != nil {
					t.Fatal(err)
				}
				return state.admitLocalDocument(raw)
			},
		},
		{
			name: "followed raw CID mismatch",
			run: func(state *pointerState) error {
				other, err := p2p.NewDocumentBlock([]byte("other"))
				if err != nil {
					t.Fatal(err)
				}
				mismatched, err := blocks.NewBlockWithCid(validRaw, other.Cid())
				if err != nil {
					t.Fatal(err)
				}
				return state.AdmitFollowedDocument(pointerDocumentReader(t, validDoc), mismatched, validDoc)
			},
		},
		{
			name: "followed semantic mismatch",
			run: func(state *pointerState) error {
				changed := validDoc
				changed.Net = "changed"
				return state.AdmitFollowedDocument(pointerDocumentReader(t, validDoc), validBlock, changed)
			},
		},
		{
			name: "followed wrong network",
			run: func(state *pointerState) error {
				doc, _, block := pointerSignedDocument(t, localPrivate, "other-net", nil,
					pointerEntry(t, "followed", root.String(), ""))
				return state.AdmitFollowedDocument(pointerDocumentReader(t, doc), block, doc)
			},
		},
		{
			name: "followed invalid signature",
			run: func(state *pointerState) error {
				doc, _, _ := pointerSignedDocument(t, localPrivate, pointerTestNet, nil,
					pointerEntry(t, "followed", root.String(), ""))
				doc.Signature = hex.EncodeToString(make([]byte, ed25519.SignatureSize))
				raw, err := json.Marshal(doc)
				if err != nil {
					t.Fatal(err)
				}
				block, err := p2p.NewDocumentBlock(raw)
				if err != nil {
					t.Fatal(err)
				}
				return state.AdmitFollowedDocument(pointerDocumentReader(t, doc), block, doc)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			state, schedule, documents := newPointerTestState(t, []string{"written"}, []string{"followed"}, localPublic)
			if err := test.run(state); err == nil {
				t.Fatal("invalid transition succeeded")
			}
			if got := schedule.count(); got != 0 {
				t.Fatalf("invalid transition changed schedule %d times", got)
			}
			requireCurrentDocuments(t, documents)
		})
	}
}

func TestPointerStateUnsignedLocalDocumentNeverAdvertisesDocument(t *testing.T) {
	state, schedule, documents := newPointerTestState(t, []string{"written"}, nil, nil)
	root := pointerTestCID(t, "unsigned-root")
	_, raw, _ := pointerSignedDocument(t, nil, pointerTestNet, nil,
		pointerEntry(t, "written", root.String(), ""),
	)
	if err := state.admitLocalDocument(raw); err != nil {
		t.Fatalf("admit unsigned local document: %v", err)
	}
	requirePointerSet(t, schedule.last(t), "written", root, cid.Undef, cid.Undef)
	requireScheduledDocuments(t, schedule)
	requireCurrentDocuments(t, documents)
}

func TestPointerStateScheduleFailureRollsBackCurrentDocuments(t *testing.T) {
	localPublic, localPrivate := pointerKey(t)
	state, schedule, documents := newPointerTestState(t, []string{"written"}, nil, localPublic)
	root1 := pointerTestCID(t, "rollback-root-1")
	_, raw1, block1 := pointerSignedDocument(t, localPrivate, pointerTestNet, nil,
		pointerEntry(t, "written", root1.String(), ""),
	)
	if err := state.admitLocalDocument(raw1); err != nil {
		t.Fatalf("initial local admission: %v", err)
	}
	requireCurrentDocuments(t, documents, block1.Cid())

	schedule.failOnce(errors.New("injected schedule rejection"))
	root2 := pointerTestCID(t, "rollback-root-2")
	_, raw2, _ := pointerSignedDocument(t, localPrivate, pointerTestNet, nil,
		pointerEntry(t, "written", root2.String(), ""),
	)
	if err := state.admitLocalDocument(raw2); err == nil {
		t.Fatal("schedule rejection was ignored")
	}
	requirePointerSet(t, schedule.last(t), "written", root1, cid.Undef, block1.Cid())
	requireScheduledDocuments(t, schedule, block1.Cid())
	requireCurrentDocuments(t, documents, block1.Cid())
}

type blockingPointerSchedule struct {
	base    pointerTestSchedule
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (s *blockingPointerSchedule) ReplaceAllWithDocuments(candidate map[string]pointerhint.Set, documents []cid.Cid) error {
	s.once.Do(func() {
		close(s.entered)
		<-s.release
	})
	return s.base.ReplaceAllWithDocuments(candidate, documents)
}

func TestPointerStateLocalCallbackCopiesCoalescesAndNeverBlocks(t *testing.T) {
	localPublic, localPrivate := pointerKey(t)
	schedule := &blockingPointerSchedule{entered: make(chan struct{}), release: make(chan struct{})}
	documents := newPointerTestDocuments()
	errorsSeen := make(chan error, 1)
	state, err := newPointerState(pointerStateConfig{
		Net:               pointerTestNet,
		WrittenHeads:      pointerNameSet("written"),
		LocalSigner:       localPublic,
		Coordinator:       schedule,
		VerifiedDocuments: documents,
		OnWorkerError: func(err error) {
			select {
			case errorsSeen <- err:
			default:
			}
		},
	})
	if err != nil {
		t.Fatalf("newPointerState: %v", err)
	}
	t.Cleanup(state.Close)

	root1 := pointerTestCID(t, "callback-root-1")
	_, raw1, _ := pointerSignedDocument(t, localPrivate, pointerTestNet, nil,
		pointerEntry(t, "written", root1.String(), ""),
	)
	state.NotifyLocalDocument(raw1)
	// Ownership was copied. Mutating the caller's bytes before Start must not
	// alter the pending publication.
	for i := range raw1 {
		raw1[i] = 'x'
	}
	if err := state.Start(t.Context()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	select {
	case <-schedule.entered:
	case <-time.After(time.Second):
		t.Fatal("worker did not reach blocked schedule")
	}

	root2 := pointerTestCID(t, "callback-root-2")
	_, raw2, _ := pointerSignedDocument(t, localPrivate, pointerTestNet, nil,
		pointerEntry(t, "written", root2.String(), ""),
	)
	root3 := pointerTestCID(t, "callback-root-3")
	_, raw3, block3 := pointerSignedDocument(t, localPrivate, pointerTestNet, nil,
		pointerEntry(t, "written", root3.String(), ""),
	)

	done := make(chan struct{})
	go func() {
		state.NotifyLocalDocument(raw2)
		state.NotifyLocalDocument(raw3)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("OnDoc-facing callback blocked behind provider schedule")
	}
	close(schedule.release)

	deadline := time.Now().Add(2 * time.Second)
	for schedule.base.count() < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := schedule.base.count(); got != 2 {
		t.Fatalf("processed schedule count = %d, want first plus latest coalesced document", got)
	}
	final := schedule.base.last(t)
	requirePointerSet(t, final, "written", root3, cid.Undef, block3.Cid())
	select {
	case err := <-errorsSeen:
		t.Fatalf("worker rejected copied/coalesced document: %v", err)
	default:
	}
}

func TestPointerStateSerializesConcurrentSourceUpdates(t *testing.T) {
	localPublic, localPrivate := pointerKey(t)
	_, upstreamPrivate := pointerKey(t)
	state, schedule, _ := newPointerTestState(t, []string{"written"}, []string{"followed"}, localPublic)
	writtenRoot := pointerTestCID(t, "serialized-written")
	_, writtenRaw, writtenBlock := pointerSignedDocument(t, localPrivate, pointerTestNet, nil,
		pointerEntry(t, "written", writtenRoot.String(), ""))
	followedRoot := pointerTestCID(t, "serialized-followed")
	followedDoc, _, followedBlock := pointerSignedDocument(t, upstreamPrivate, pointerTestNet, nil,
		pointerEntry(t, "followed", followedRoot.String(), ""))
	reader := pointerDocumentReader(t, followedDoc)

	start := make(chan struct{})
	results := make(chan error, 2)
	go func() { <-start; results <- state.admitLocalDocument(writtenRaw) }()
	go func() { <-start; results <- state.AdmitFollowedDocument(reader, followedBlock, followedDoc) }()
	close(start)
	for range 2 {
		if err := <-results; err != nil {
			t.Fatalf("concurrent update: %v", err)
		}
	}
	final := schedule.last(t)
	if len(final) != 2 {
		t.Fatalf("serialized union = %#v, want both sources", final)
	}
	requirePointerSet(t, final, "written", writtenRoot, cid.Undef, writtenBlock.Cid())
	requirePointerSet(t, final, "followed", followedRoot, cid.Undef, followedBlock.Cid())
}

func TestPointerStateConstructorRejectsOverlappingOwnership(t *testing.T) {
	_, err := newPointerState(pointerStateConfig{
		Net:               pointerTestNet,
		WrittenHeads:      pointerNameSet("same"),
		FollowedHeads:     pointerNameSet("same"),
		Coordinator:       &pointerTestSchedule{},
		VerifiedDocuments: newPointerTestDocuments(),
	})
	if err == nil {
		t.Fatal("overlapping written/followed ownership was accepted")
	}
}

func TestPointerStateConstructorRejectsAggregateHeadOverflow(t *testing.T) {
	written := make(map[string]struct{}, pointerhint.MaxCoordinatorHeads)
	for i := range pointerhint.MaxCoordinatorHeads {
		written[fmt.Sprintf("written-%d", i)] = struct{}{}
	}
	followed := pointerNameSet("one-too-many")
	_, err := newPointerState(pointerStateConfig{
		Net:               pointerTestNet,
		WrittenHeads:      written,
		FollowedHeads:     followed,
		Coordinator:       &pointerTestSchedule{},
		VerifiedDocuments: newPointerTestDocuments(),
	})
	if err == nil {
		t.Fatalf("%d configured heads were accepted, hard limit is %d", len(written)+len(followed), pointerhint.MaxCoordinatorHeads)
	}
}

func TestPointerStateCloseBeforeStartRejectsStartAndDropsCallbacks(t *testing.T) {
	state, schedule, _ := newPointerTestState(t, []string{"written"}, nil, nil)
	state.Close()
	state.NotifyLocalDocument([]byte("ignored"))
	if err := state.Start(context.Background()); err == nil {
		t.Fatal("Start after Close succeeded")
	}
	if got := schedule.count(); got != 0 {
		t.Fatalf("closed state changed schedule %d times", got)
	}
}
