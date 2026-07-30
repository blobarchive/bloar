package server_test

// These tests began life as the safety boundary reproducers: a mutation swaps the engine
// to a new root before commit persists it, and the global rebuild re-read every
// head's live engine, so a successful mutation on head B could sign and announce
// head A's volatile (non-durable) root. The fix renders the publication document
// from a per-head durable record (server.entry.durable) and persists the
// irrevocable engine swap under context.WithoutCancel, so the two reproducers are
// FLIPPED here to assert the fix: the cross-head rebuild keeps A's durable root,
// and a canceled request no longer strands the swap. The additional tests cover
// the manifest-persist axis, a failed first follower adoption, and crash/restart
// equality.

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ipfs/go-cid"

	"github.com/blobarchive/bloar/archive"
	"github.com/blobarchive/bloar/catalog"
	"github.com/blobarchive/bloar/ingest"
	"github.com/blobarchive/bloar/server"
	"github.com/blobarchive/bloar/store"
)

var errDurabilityRootPut = errors.New("injected per-head root persist failure")

type failingRoots struct {
	*server.RootStore
	failingHead string
}

func (r *failingRoots) Put(ctx context.Context, name string, root cid.Cid) error {
	if name == r.failingHead {
		return errDurabilityRootPut
	}
	return r.RootStore.Put(ctx, name, root)
}

// TestCrossHeadRebuildAnnouncesUnpersistedRoot is the flipped regression
// reproducer for the injected-persistence-failure case. Head A's root write
// fails after its engine swaps, then an ordinary successful mutation of head B
// rebuilds the shared document. Before the fix that rebuild read A's live engine
// and announced its volatile root; now it renders A from A's durable record, so
// the document still names A's last durable root and the volatile one is never
// signed or served.
func TestCrossHeadRebuildAnnouncesUnpersistedRoot(t *testing.T) {
	ctx := t.Context()
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	defer st.Close()

	cat := catalog.New(st.KV())
	rootStore := server.NewRootStore(st.KV())
	roots := &failingRoots{RootStore: rootStore}
	archiveCfg := archive.Config{Blocks: st.Blocks(), Resolver: cat}
	params := func(name string) archive.Params {
		return archive.Params{Name: name, Net: "auditnet", OriginSlot: 0, SegBits: 2, FanoutBits: 2}
	}

	headA, err := server.OpenHead(ctx, archiveCfg, roots, params("audit-a"))
	if err != nil {
		t.Fatalf("OpenHead(a): %v", err)
	}
	headB, err := server.OpenHead(ctx, archiveCfg, roots, params("audit-b"))
	if err != nil {
		t.Fatalf("OpenHead(b): %v", err)
	}
	durableA0 := headA.Root()
	durableB0 := headB.Root()

	_, signingKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	var onDocs [][]byte
	heads, err := server.NewHeads(server.HeadsConfig{
		Net:        "auditnet",
		Roots:      roots,
		SigningKey: signingKey,
		OnDoc: func(doc []byte) {
			onDocs = append(onDocs, bytes.Clone(doc))
		},
	})
	if err != nil {
		t.Fatalf("NewHeads: %v", err)
	}
	if err := heads.Add(headA); err != nil {
		t.Fatalf("Add(a): %v", err)
	}
	if err := heads.Add(headB); err != nil {
		t.Fatalf("Add(b): %v", err)
	}

	// A candidate A1 is complete, but the root-store write fails. COW keeps the
	// selected engine and immediate publication at durable A0.
	roots.failingHead = "audit-a"
	failed, err := heads.ApplyRefs(ctx, "audit-a", nil, 0, cid.Undef)
	if !errors.Is(err, errDurabilityRootPut) {
		t.Fatalf("ApplyRefs(a) error = %v, want injected persist failure", err)
	}
	if failed.Root.Equals(durableA0) {
		t.Fatal("A did not build a new candidate root before the injected failure")
	}
	if !headA.Root().Equals(durableA0) {
		t.Fatalf("failed candidate replaced selected A engine: got %s, want %s", headA.Root(), durableA0)
	}
	if got := docHead(t, heads.Doc(), "audit-a").Root; got != durableA0.String() {
		t.Fatalf("immediate document root for A = %s, want durable A0 %s", got, durableA0)
	}

	// Once the transient failure clears, an ordinary successful mutation of B
	// rebuilds the shared document. The fix renders A from its durable record, so
	// the rebuild names A0 -- the volatile A1 is never announced across the head
	// boundary.
	roots.failingHead = ""
	appliedB, err := heads.ApplyRefs(ctx, "audit-b", nil, 0, cid.Undef)
	if err != nil {
		t.Fatalf("ApplyRefs(b): %v", err)
	}
	doc := heads.Doc()
	publishedA := docHead(t, doc, "audit-a")
	if publishedA.Root != durableA0.String() {
		t.Fatalf("cross-head rebuild announced A root = %s, want durable A0 %s (never the volatile %s)",
			publishedA.Root, durableA0, failed.Root)
	}
	if publishedA.SyncedTo != nil {
		t.Fatalf("published A synced_to = %v, want null: A0 is an empty head, not the volatile coverage", *publishedA.SyncedTo)
	}
	durableA, ok, err := rootStore.Get(ctx, "audit-a")
	if err != nil || !ok {
		t.Fatalf("reading durable A root: ok=%t err=%v", ok, err)
	}
	if !durableA.Equals(durableA0) {
		t.Fatalf("durable A root moved to %s, want unchanged A0 %s", durableA, durableA0)
	}
	if publishedA.Root != durableA.String() {
		t.Fatalf("published A root %s must equal its durable root %s", publishedA.Root, durableA)
	}

	// B, the head that committed successfully, is published at its own advanced
	// durable root: the fix suppresses only the head whose commit failed.
	if got := docHead(t, doc, "audit-b").Root; got != appliedB.Root.String() {
		t.Fatalf("B published at %s, want its advanced durable root %s", got, appliedB.Root)
	}
	selectedB, ok := heads.Get("audit-b")
	if !ok || !selectedB.Root().Equals(appliedB.Root) || !headB.Root().Equals(durableB0) {
		t.Fatalf("COW selection for B: selected=%v ok=%t original=%s candidate=%s",
			selectedB, ok, headB.Root(), appliedB.Root)
	}

	// The rebuilt bytes cross both publication channels: OnDoc receives them for
	// IPNS, and GET /bloar/v1/heads serves the same signed document over HTTP.
	if len(onDocs) == 0 || !bytes.Equal(onDocs[len(onDocs)-1], doc) {
		t.Fatal("OnDoc did not receive the document naming A's durable root")
	}
	var signed server.Doc
	if err := json.Unmarshal(doc, &signed); err != nil {
		t.Fatalf("decode signed document: %v", err)
	}
	if err := signed.Verify(); err != nil {
		t.Fatalf("document naming A's durable root is not signed-valid: %v", err)
	}

	ing, err := ingest.New(ingest.Config{Blocks: st.Blocks(), Catalog: cat})
	if err != nil {
		t.Fatalf("ingest.New: %v", err)
	}
	httpServer, err := server.New(server.Config{
		Heads:     heads,
		Blocks:    st.Blocks(),
		Ingester:  ing,
		AuthToken: "audit-token",
		Beacon:    server.Beacon{SecondsPerSlot: 12},
	})
	if err != nil {
		t.Fatalf("server.New: %v", err)
	}
	req := httptest.NewRequest(http.MethodGet, "/bloar/v1/heads", nil)
	recorder := httptest.NewRecorder()
	httpServer.ServeHTTP(recorder, req)
	httpDoc := recorder.Body.Bytes()
	if recorder.Code != http.StatusOK || !bytes.Equal(httpDoc, doc) {
		t.Fatalf("GET heads status/body = %d/%s, want 200 and shared document", recorder.Code, httpDoc)
	}

	// A restart loads A0: the published root and the resumable root are the same,
	// exactly as they must be now that the volatile A1 is never announced.
	restartedA, err := server.OpenHead(ctx, archiveCfg, rootStore, params("audit-a"))
	if err != nil {
		t.Fatalf("OpenHead(a) from durable store: %v", err)
	}
	if !restartedA.Root().Equals(durableA0) {
		t.Fatalf("restart root for A = %s, want durable A0 %s", restartedA.Root(), durableA0)
	}
	if _, covered := restartedA.SyncedTo(); covered {
		t.Fatal("restart unexpectedly retained A's non-durable coverage")
	}
}

// TestCanceledMutationCanCreateCrossHeadDurabilityGap is the flipped
// the safety boundary reproducer for the already-canceled-request case. flatfs completes the
// block writes and the engine swap despite the cancellation; before the fix the
// context-aware RootStore then refused the Put, stranding a swapped engine ahead
// of the durable root. commit now runs the Put under context.WithoutCancel, so
// the irrevocable swap is persisted and there is no gap for a later cross-head
// rebuild to expose.
func TestCanceledMutationCanCreateCrossHeadDurabilityGap(t *testing.T) {
	ctx := t.Context()
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	defer st.Close()

	cat := catalog.New(st.KV())
	rootStore := server.NewRootStore(st.KV())
	archiveCfg := archive.Config{Blocks: st.Blocks(), Resolver: cat}
	params := func(name string) archive.Params {
		return archive.Params{Name: name, Net: "auditnet", OriginSlot: 0, SegBits: 2, FanoutBits: 2}
	}

	headA, err := server.OpenHead(ctx, archiveCfg, rootStore, params("audit-canceled-a"))
	if err != nil {
		t.Fatalf("OpenHead(a): %v", err)
	}
	headB, err := server.OpenHead(ctx, archiveCfg, rootStore, params("audit-canceled-b"))
	if err != nil {
		t.Fatalf("OpenHead(b): %v", err)
	}
	durableA0 := headA.Root()
	heads, err := server.NewHeads(server.HeadsConfig{Net: "auditnet", Roots: rootStore})
	if err != nil {
		t.Fatalf("NewHeads: %v", err)
	}
	if err := heads.Add(headA); err != nil {
		t.Fatalf("Add(a): %v", err)
	}
	if err := heads.Add(headB); err != nil {
		t.Fatalf("Add(b): %v", err)
	}

	// The archive's flatfs-backed candidate build completes despite a context
	// that is already canceled. The root is persisted under WithoutCancel before
	// the candidate becomes selected, so no durability gap is exposed.
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	applied, err := heads.ApplyRefs(canceled, "audit-canceled-a", nil, 0, cid.Undef)
	if err != nil {
		t.Fatalf("ApplyRefs(a) with a canceled context = %v, want success: the irrevocable engine swap must persist under WithoutCancel", err)
	}
	selectedA, ok := heads.Get("audit-canceled-a")
	if applied.Root.Equals(durableA0) || !ok || !selectedA.Root().Equals(applied.Root) || !headA.Root().Equals(durableA0) {
		t.Fatalf("canceled COW mutation selection: result=%s selected=%v ok=%t original=%s initial=%s",
			applied.Root, selectedA, ok, headA.Root(), durableA0)
	}
	durableA, ok, err := rootStore.Get(ctx, "audit-canceled-a")
	if err != nil || !ok || !durableA.Equals(applied.Root) {
		t.Fatalf("durable A after a canceled mutation = %s, ok=%t err=%v, want the swapped root %s", durableA, ok, err, applied.Root)
	}
	if got := docHead(t, heads.Doc(), "audit-canceled-a").Root; got != applied.Root.String() {
		t.Fatalf("document A root = %s, want the persisted swapped root %s", got, applied.Root)
	}

	// A later successful mutation of another head reaches the same global rebuild
	// that used to expose the gap. A still names its own durable root, which the
	// fix kept equal to the engine's.
	if _, err := heads.ApplyRefs(ctx, "audit-canceled-b", nil, 0, cid.Undef); err != nil {
		t.Fatalf("ApplyRefs(b): %v", err)
	}
	if got := docHead(t, heads.Doc(), "audit-canceled-a").Root; got != applied.Root.String() {
		t.Fatalf("cross-head rebuild A root = %s, want durable %s", got, applied.Root)
	}
}

// TestFailedFirstFollowerAdoptionOmittedFromDocument covers the safety boundary
// rule for a head with no prior durable record: a followed head whose very first
// adoption fails to persist its root is omitted from the document entirely,
// rather than published at the volatile adopted root. A written head registered
// alongside it proves the document did rebuild -- it just left out the head with
// no durable state.
func TestFailedFirstFollowerAdoptionOmittedFromDocument(t *testing.T) {
	ctx := t.Context()
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	defer st.Close()

	cat := catalog.New(st.KV())
	rootStore := server.NewRootStore(st.KV())
	roots := &failingRoots{RootStore: rootStore}
	archiveCfg := archive.Config{Blocks: st.Blocks(), Resolver: cat}
	params := func(name string) archive.Params {
		return archive.Params{Name: name, Net: "auditnet", OriginSlot: 0, SegBits: 2, FanoutBits: 2}
	}

	written, err := server.OpenHead(ctx, archiveCfg, roots, params("audit-written"))
	if err != nil {
		t.Fatalf("OpenHead(written): %v", err)
	}
	// Built with archive.New rather than OpenHead, so its root block is in the
	// blockstore for adoption but the RootStore has never held it -- a genuine
	// first adoption.
	followed, err := archive.New(ctx, archiveCfg, params("audit-followed"))
	if err != nil {
		t.Fatalf("archive.New(followed): %v", err)
	}

	heads, err := server.NewHeads(server.HeadsConfig{Net: "auditnet", Roots: roots})
	if err != nil {
		t.Fatalf("NewHeads: %v", err)
	}
	if err := heads.Add(written); err != nil {
		t.Fatalf("Add(written): %v", err)
	}

	roots.failingHead = "audit-followed"
	if err := heads.Adopt(ctx, followed, nil, cid.Undef); !errors.Is(err, errDurabilityRootPut) {
		t.Fatalf("Adopt error = %v, want injected persist failure", err)
	}

	doc := heads.Doc()
	var decoded server.Doc
	if err := json.Unmarshal(doc, &decoded); err != nil {
		t.Fatalf("decode document: %v", err)
	}
	for _, h := range decoded.Heads {
		if h.Name == "audit-followed" {
			t.Fatalf("followed head appeared in the document at root %s despite a failed first adoption", h.Root)
		}
	}
	if got := docHead(t, doc, "audit-written").Root; got != written.Root().String() {
		t.Fatalf("written head root = %s, want %s (the document must still carry heads with a durable record)", got, written.Root())
	}
}

// TestUnpersistedHeadDocumentSurvivesRestart proves the durable record is
// exactly what a restart resumes: while head A is unpersisted (engine swapped to
// A1, durable still A0), the document it renders for A is byte-identical to the
// document a fresh registry renders after loading A from the durable store. The
// volatile A1 leaves no trace in either.
func TestUnpersistedHeadDocumentSurvivesRestart(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	st, err := store.Open(dir)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}

	cat := catalog.New(st.KV())
	rootStore := server.NewRootStore(st.KV())
	roots := &failingRoots{RootStore: rootStore}
	archiveCfg := archive.Config{Blocks: st.Blocks(), Resolver: cat}
	params := func(name string) archive.Params {
		return archive.Params{Name: name, Net: "auditnet", OriginSlot: 0, SegBits: 2, FanoutBits: 2}
	}

	headA, err := server.OpenHead(ctx, archiveCfg, roots, params("audit-a"))
	if err != nil {
		t.Fatalf("OpenHead(a): %v", err)
	}
	durableA0 := headA.Root()
	heads, err := server.NewHeads(server.HeadsConfig{Net: "auditnet", Roots: roots})
	if err != nil {
		t.Fatalf("NewHeads: %v", err)
	}
	if err := heads.Add(headA); err != nil {
		t.Fatalf("Add(a): %v", err)
	}

	roots.failingHead = "audit-a"
	failed, err := heads.ApplyRefs(ctx, "audit-a", nil, 0, cid.Undef)
	if !errors.Is(err, errDurabilityRootPut) {
		t.Fatalf("ApplyRefs(a) error = %v, want injected persist failure", err)
	}
	if failed.Root.Equals(durableA0) {
		t.Fatal("A did not swap to a new in-memory root before the injected failure")
	}
	before := docHead(t, heads.Doc(), "audit-a")
	if before.Root != durableA0.String() {
		t.Fatalf("pre-crash document A root = %s, want durable A0 %s", before.Root, durableA0)
	}

	// Restart: close the store, reopen it, and resume A through a fresh registry.
	// OpenHead loads the durable A0 (the failed A1 was never persisted), and Add
	// renders A's durable line from it.
	if err := st.Close(); err != nil {
		t.Fatalf("closing store: %v", err)
	}
	st2, err := store.Open(dir)
	if err != nil {
		t.Fatalf("reopening store: %v", err)
	}
	defer st2.Close()
	cat2 := catalog.New(st2.KV())
	rootStore2 := server.NewRootStore(st2.KV())
	archiveCfg2 := archive.Config{Blocks: st2.Blocks(), Resolver: cat2}
	restartedA, err := server.OpenHead(ctx, archiveCfg2, rootStore2, params("audit-a"))
	if err != nil {
		t.Fatalf("OpenHead(a) after restart: %v", err)
	}
	if !restartedA.Root().Equals(durableA0) {
		t.Fatalf("restart root for A = %s, want durable A0 %s", restartedA.Root(), durableA0)
	}
	heads2, err := server.NewHeads(server.HeadsConfig{Net: "auditnet", Roots: rootStore2})
	if err != nil {
		t.Fatalf("NewHeads (restart): %v", err)
	}
	if err := heads2.Add(restartedA); err != nil {
		t.Fatalf("Add(a) after restart: %v", err)
	}
	after := docHead(t, heads2.Doc(), "audit-a")

	beforeJSON, err := json.Marshal(before)
	if err != nil {
		t.Fatalf("marshal pre-crash entry: %v", err)
	}
	afterJSON, err := json.Marshal(after)
	if err != nil {
		t.Fatalf("marshal post-restart entry: %v", err)
	}
	if !bytes.Equal(beforeJSON, afterJSON) {
		t.Fatalf("A's document line changed across a crash: pre-crash %s, post-restart %s", beforeJSON, afterJSON)
	}
}

// TestManifestTipNotRegressedByRootRebuild covers the manifest-persist
// axis of the safety boundary fix. A manifest-only persist advances the published tip
// without committing a root; a later root-only commit must carry that tip from
// the durable record rather than drop it -- a commit-only snapshot would suppress
// the upgrade, which is the gap the reviewer flagged.
func TestManifestTipNotRegressedByRootRebuild(t *testing.T) {
	s := newStack(t, stackOpts{})

	vhs := s.put(makeBlob(1), makeBlob(2))
	s.refs([]map[string]any{row(9, vhs[0])}, 10)
	root1 := s.headEntry().Root
	if root1 == "" {
		t.Fatal("the first batch published no root")
	}

	tip := s.setManifest([]map[string]any{inboxSourceJSON(0, nil)}, nil)
	if e := s.headEntry(); e.Manifest != tip {
		t.Fatalf("after a manifest-only persist, document manifest = %q, want %q", e.Manifest, tip)
	}

	// A root-only commit rebuilds the document. The tip is carried from the
	// durable record, and the root advances.
	s.refs([]map[string]any{row(11, vhs[1])}, 12)
	e := s.headEntry()
	if e.Root == root1 {
		t.Fatal("the second batch did not advance the root")
	}
	if e.Manifest != tip {
		t.Fatalf("after a root-only rebuild, document manifest = %q, want the durable tip %q", e.Manifest, tip)
	}
}

func docHead(t *testing.T, body []byte, name string) server.HeadEntry {
	t.Helper()
	var doc server.Doc
	if err := json.Unmarshal(body, &doc); err != nil {
		t.Fatalf("decode publication document: %v", err)
	}
	for _, head := range doc.Heads {
		if head.Name == name {
			return head
		}
	}
	t.Fatalf("publication document has no head %q", name)
	return server.HeadEntry{}
}
