package pointerhint

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/ipfs/boxo/blockstore"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/multiformats/go-multihash"
)

func memoryBlocks() blockstore.Blockstore {
	return blockstore.NewBlockstore(dssync.MutexWrap(datastore.NewMapDatastore()))
}

func testBlock(t *testing.T, codec uint64, value string) blocks.Block {
	t.Helper()
	prefix := cid.Prefix{Version: 1, Codec: codec, MhType: multihash.SHA2_256, MhLength: -1}
	c, err := prefix.Sum([]byte(value))
	if err != nil {
		t.Fatalf("hashing test block: %v", err)
	}
	block, err := blocks.NewBlockWithCid([]byte(value), c)
	if err != nil {
		t.Fatalf("building test block: %v", err)
	}
	return block
}

type lyingBlock struct {
	c    cid.Cid
	data []byte
}

func (b lyingBlock) RawData() []byte          { return b.data }
func (b lyingBlock) Cid() cid.Cid             { return b.c }
func (b lyingBlock) String() string           { return b.c.String() }
func (b lyingBlock) Loggable() map[string]any { return map[string]any{"cid": b.c} }

func TestVerifiedDocumentStoreAdmissionIsExplicitAndBounded(t *testing.T) {
	base := memoryBlocks()
	store, err := NewVerifiedDocumentStore(base, 2)
	if err != nil {
		t.Fatalf("NewVerifiedDocumentStore: %v", err)
	}
	doc1 := testBlock(t, cid.Raw, "verified document one")
	doc2 := testBlock(t, cid.Raw, "verified document two")
	doc3 := testBlock(t, cid.Raw, "verified document three")

	// Ordinary blockstore writes carry no trust meaning, even for a raw CID.
	if err := store.Put(t.Context(), doc1); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if ok, err := store.Has(t.Context(), doc1.Cid()); err != nil || !ok {
		t.Fatalf("ordinary presence = %t, %v; want true", ok, err)
	}
	if ok, err := store.HasVerified(t.Context(), doc1.Cid()); err != nil || ok {
		t.Fatalf("verified presence after Put = %t, %v; want false", ok, err)
	}

	for _, doc := range []blocks.Block{doc1, doc2, doc3} {
		if err := store.RetainAfterVerification(doc); err != nil {
			t.Fatalf("RetainAfterVerification(%s): %v", doc.Cid(), err)
		}
	}
	// Capacity two evicts the least recently used verified admission. doc1
	// remains in base, proving HasVerified does not fall through to it.
	if ok, _ := store.HasVerified(t.Context(), doc1.Cid()); ok {
		t.Fatal("the oldest verified document remained eligible above the capacity")
	}
	for _, doc := range []blocks.Block{doc2, doc3} {
		if ok, err := store.HasVerified(t.Context(), doc.Cid()); err != nil || !ok {
			t.Errorf("verified presence for %s = %t, %v; want true", doc.Cid(), ok, err)
		}
		got, err := store.Get(t.Context(), doc.Cid())
		if err != nil || string(got.RawData()) != string(doc.RawData()) {
			t.Errorf("Get(%s) = %v, %v", doc.Cid(), got, err)
		}
	}
	first, err := store.Get(t.Context(), doc3.Cid())
	if err != nil {
		t.Fatalf("Get mutable probe: %v", err)
	}
	first.RawData()[0] ^= 0xff
	second, err := store.Get(t.Context(), doc3.Cid())
	if err != nil {
		t.Fatalf("Get after caller mutation: %v", err)
	}
	if string(second.RawData()) != string(doc3.RawData()) {
		t.Fatal("a caller mutated retained verified document bytes through Get")
	}
}

func TestVerifiedDocumentStorePutManyDoesNotGrantTrust(t *testing.T) {
	store, err := NewVerifiedDocumentStore(memoryBlocks(), 2)
	if err != nil {
		t.Fatalf("NewVerifiedDocumentStore: %v", err)
	}
	docs := []blocks.Block{
		testBlock(t, cid.Raw, "ordinary document one"),
		testBlock(t, cid.Raw, "ordinary document two"),
	}
	if err := store.PutMany(t.Context(), docs); err != nil {
		t.Fatalf("PutMany: %v", err)
	}
	for _, doc := range docs {
		if ok, err := store.Has(t.Context(), doc.Cid()); err != nil || !ok {
			t.Fatalf("ordinary presence for %s = %t, %v; want true", doc.Cid(), ok, err)
		}
		if ok, err := store.HasVerified(t.Context(), doc.Cid()); err != nil || ok {
			t.Fatalf("verified presence for %s after PutMany = %t, %v; want false", doc.Cid(), ok, err)
		}
	}
}

func TestVerifiedDocumentStoreCopiesAdmissionBytes(t *testing.T) {
	store, err := NewVerifiedDocumentStore(memoryBlocks(), 1)
	if err != nil {
		t.Fatalf("NewVerifiedDocumentStore: %v", err)
	}
	doc := testBlock(t, cid.Raw, "owned admission bytes")
	writable := append([]byte(nil), doc.RawData()...)
	input := lyingBlock{c: doc.Cid(), data: writable}
	if err := store.RetainAfterVerification(input); err != nil {
		t.Fatalf("RetainAfterVerification: %v", err)
	}
	writable[0] ^= 0xff
	got, err := store.Get(t.Context(), doc.Cid())
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(got.RawData()) != string(doc.RawData()) {
		t.Fatal("mutating the caller's admitted buffer changed retained verified bytes")
	}
}

func TestVerifiedDocumentStoreReadsRefreshRecency(t *testing.T) {
	store, err := NewVerifiedDocumentStore(memoryBlocks(), 2)
	if err != nil {
		t.Fatalf("NewVerifiedDocumentStore: %v", err)
	}
	doc1 := testBlock(t, cid.Raw, "verified document one")
	doc2 := testBlock(t, cid.Raw, "verified document two")
	doc3 := testBlock(t, cid.Raw, "verified document three")
	for _, doc := range []blocks.Block{doc1, doc2} {
		if err := store.RetainAfterVerification(doc); err != nil {
			t.Fatalf("RetainAfterVerification(%s): %v", doc.Cid(), err)
		}
	}

	// Serving doc1 makes doc2 the least-recently-used admission.
	if _, err := store.Get(t.Context(), doc1.Cid()); err != nil {
		t.Fatalf("Get(%s): %v", doc1.Cid(), err)
	}
	if err := store.RetainAfterVerification(doc3); err != nil {
		t.Fatalf("RetainAfterVerification(%s): %v", doc3.Cid(), err)
	}
	if ok, err := store.HasVerified(t.Context(), doc1.Cid()); err != nil || !ok {
		t.Fatalf("recently read doc1 verified presence = %t, %v; want true", ok, err)
	}
	if ok, err := store.HasVerified(t.Context(), doc2.Cid()); err != nil || ok {
		t.Fatalf("unread doc2 verified presence = %t, %v; want evicted", ok, err)
	}
}

func TestVerifiedDocumentStoreRejectsWrongCodecAndCID(t *testing.T) {
	store, err := NewVerifiedDocumentStore(memoryBlocks(), 1)
	if err != nil {
		t.Fatalf("NewVerifiedDocumentStore: %v", err)
	}
	if err := store.RetainAfterVerification(testBlock(t, cid.DagCBOR, "not raw")); err == nil || !strings.Contains(err.Error(), "not a raw CID") {
		t.Fatalf("dag-cbor admission error = %v, want raw-CID refusal", err)
	}
	honest := testBlock(t, cid.Raw, "honest")
	liar := lyingBlock{c: honest.Cid(), data: []byte("different bytes")}
	if err := store.RetainAfterVerification(liar); err == nil || !strings.Contains(err.Error(), "do not match CID") {
		t.Fatalf("mismatched admission error = %v, want CID refusal", err)
	}
	sha512CID, err := cid.Prefix{
		Version: 1, Codec: cid.Raw, MhType: multihash.SHA2_512, MhLength: -1,
	}.Sum([]byte("wrong hash profile"))
	if err != nil {
		t.Fatalf("building sha2-512 CID: %v", err)
	}
	sha512Block, err := blocks.NewBlockWithCid([]byte("wrong hash profile"), sha512CID)
	if err != nil {
		t.Fatalf("building sha2-512 block: %v", err)
	}
	if err := store.RetainAfterVerification(sha512Block); err == nil || !strings.Contains(err.Error(), "sha2-256") {
		t.Fatalf("sha2-512 admission error = %v, want Bloar CID-profile refusal", err)
	}
}

func TestVerifiedDocumentStoreHonoursCancelledContext(t *testing.T) {
	store, err := NewVerifiedDocumentStore(memoryBlocks(), 1)
	if err != nil {
		t.Fatalf("NewVerifiedDocumentStore: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := store.HasVerified(ctx, testBlock(t, cid.Raw, "doc").Cid()); err == nil {
		t.Fatal("HasVerified ignored cancellation")
	}
}

func TestVerifiedDocumentStoreActiveDocumentSurvivesHistoryChurn(t *testing.T) {
	store, err := NewVerifiedDocumentStore(memoryBlocks(), 1)
	if err != nil {
		t.Fatalf("NewVerifiedDocumentStore: %v", err)
	}
	active := testBlock(t, cid.Raw, "quiet current upstream document")
	if err := store.StageCurrentAfterVerification([]blocks.Block{active}); err != nil {
		t.Fatalf("StageCurrentAfterVerification: %v", err)
	}
	if err := store.ReplaceCurrentDocuments([]cid.Cid{active.Cid()}); err != nil {
		t.Fatalf("ReplaceCurrentDocuments: %v", err)
	}
	for i := 0; i < 32; i++ {
		if err := store.RetainAfterVerification(testBlock(t, cid.Raw, fmt.Sprintf("writer churn %d", i))); err != nil {
			t.Fatalf("RetainAfterVerification(churn %d): %v", i, err)
		}
	}
	if ok, err := store.HasVerified(t.Context(), active.Cid()); err != nil || !ok {
		t.Fatalf("active verified presence after LRU churn = %t, %v; want true", ok, err)
	}
	got, err := store.Get(t.Context(), active.Cid())
	if err != nil || string(got.RawData()) != string(active.RawData()) {
		t.Fatalf("active Get after LRU churn = %v, %v", got, err)
	}
}

func TestVerifiedDocumentStoreTransitionRetainsOldUntilCommit(t *testing.T) {
	store, err := NewVerifiedDocumentStore(memoryBlocks(), 1)
	if err != nil {
		t.Fatalf("NewVerifiedDocumentStore: %v", err)
	}
	old := testBlock(t, cid.Raw, "old current document")
	next := testBlock(t, cid.Raw, "new current document")
	if err := store.StageCurrentAfterVerification([]blocks.Block{old}); err != nil {
		t.Fatal(err)
	}
	if err := store.ReplaceCurrentDocuments([]cid.Cid{old.Cid()}); err != nil {
		t.Fatal(err)
	}
	if err := store.StageCurrentAfterVerification([]blocks.Block{next}); err != nil {
		t.Fatal(err)
	}
	for _, document := range []blocks.Block{old, next} {
		if ok, err := store.HasVerified(t.Context(), document.Cid()); err != nil || !ok {
			t.Fatalf("transition verified presence for %s = %t, %v; want true", document.Cid(), ok, err)
		}
	}
	if err := store.ReplaceCurrentDocuments([]cid.Cid{next.Cid()}); err != nil {
		t.Fatal(err)
	}
	// The one-entry history contains next, so old proves it was removed from
	// protected state rather than remaining eligible accidentally.
	if ok, err := store.HasVerified(t.Context(), old.Cid()); err != nil || ok {
		t.Fatalf("retired document verified presence = %t, %v; want false", ok, err)
	}
	if ok, err := store.HasVerified(t.Context(), next.Cid()); err != nil || !ok {
		t.Fatalf("new document verified presence = %t, %v; want true", ok, err)
	}
}

func TestVerifiedDocumentStoreTransitionRollbackKeepsOldCurrent(t *testing.T) {
	store, err := NewVerifiedDocumentStore(memoryBlocks(), 1)
	if err != nil {
		t.Fatalf("NewVerifiedDocumentStore: %v", err)
	}
	old := testBlock(t, cid.Raw, "rollback old current")
	next := testBlock(t, cid.Raw, "rollback candidate")
	if err := store.StageCurrentAfterVerification([]blocks.Block{old}); err != nil {
		t.Fatal(err)
	}
	if err := store.ReplaceCurrentDocuments([]cid.Cid{old.Cid()}); err != nil {
		t.Fatal(err)
	}
	if err := store.StageCurrentAfterVerification([]blocks.Block{next}); err != nil {
		t.Fatal(err)
	}
	if err := store.ReplaceCurrentDocuments([]cid.Cid{old.Cid()}); err != nil {
		t.Fatalf("rolling active set back: %v", err)
	}
	if ok, err := store.HasVerified(t.Context(), old.Cid()); err != nil || !ok {
		t.Fatalf("rolled-back old document verified presence = %t, %v; want true", ok, err)
	}
}

func TestVerifiedDocumentStoreActiveUpdatesAreTransactional(t *testing.T) {
	store, err := NewVerifiedDocumentStore(memoryBlocks(), 1)
	if err != nil {
		t.Fatalf("NewVerifiedDocumentStore: %v", err)
	}
	old := testBlock(t, cid.Raw, "transaction old current")
	if err := store.StageCurrentAfterVerification([]blocks.Block{old}); err != nil {
		t.Fatal(err)
	}
	if err := store.ReplaceCurrentDocuments([]cid.Cid{old.Cid()}); err != nil {
		t.Fatal(err)
	}

	tooMany := make([]blocks.Block, MaxVerifiedActiveDocuments+1)
	for i := range tooMany {
		tooMany[i] = testBlock(t, cid.Raw, fmt.Sprintf("too many %d", i))
	}
	if err := store.StageCurrentAfterVerification(tooMany); err == nil || !strings.Contains(err.Error(), "exceeds limit") {
		t.Fatalf("over-capacity stage error = %v", err)
	}
	unstaged := testBlock(t, cid.Raw, "unstaged candidate")
	if err := store.ReplaceCurrentDocuments([]cid.Cid{unstaged.Cid()}); err == nil || !strings.Contains(err.Error(), "was not staged") {
		t.Fatalf("unstaged replacement error = %v", err)
	}
	if ok, err := store.HasVerified(t.Context(), old.Cid()); err != nil || !ok {
		t.Fatalf("old active state after rejected updates = %t, %v; want true", ok, err)
	}

	// The bound is on retained documents, not caller slice length. Repeated
	// references to one already-staged CID remain one active document.
	duplicates := make([]cid.Cid, MaxVerifiedActiveDocuments+1)
	for i := range duplicates {
		duplicates[i] = old.Cid()
	}
	if err := store.ReplaceCurrentDocuments(duplicates); err != nil {
		t.Fatalf("duplicate current CID references exceeded the distinct-document bound: %v", err)
	}

	atLimit := make([]blocks.Block, MaxVerifiedActiveDocuments)
	atLimitCIDs := make([]cid.Cid, MaxVerifiedActiveDocuments)
	for i := range atLimit {
		atLimit[i] = testBlock(t, cid.Raw, fmt.Sprintf("at active bound %d", i))
		atLimitCIDs[i] = atLimit[i].Cid()
	}
	if err := store.StageCurrentAfterVerification(atLimit); err != nil {
		t.Fatalf("staging the full heads-plus-local-document bound: %v", err)
	}
	if err := store.ReplaceCurrentDocuments(atLimitCIDs); err != nil {
		t.Fatalf("committing the full heads-plus-local-document bound: %v", err)
	}
}

func TestVerifiedDocumentStoreMalformedStageDoesNotPartiallyAdmit(t *testing.T) {
	store, err := NewVerifiedDocumentStore(memoryBlocks(), 1)
	if err != nil {
		t.Fatalf("NewVerifiedDocumentStore: %v", err)
	}
	old := testBlock(t, cid.Raw, "malformed stage old current")
	valid := testBlock(t, cid.Raw, "valid candidate before malformed")
	if err := store.StageCurrentAfterVerification([]blocks.Block{old}); err != nil {
		t.Fatal(err)
	}
	if err := store.ReplaceCurrentDocuments([]cid.Cid{old.Cid()}); err != nil {
		t.Fatal(err)
	}
	malformed := lyingBlock{c: testBlock(t, cid.Raw, "claimed bytes").Cid(), data: []byte("different bytes")}
	if err := store.StageCurrentAfterVerification([]blocks.Block{valid, malformed}); err == nil || !strings.Contains(err.Error(), "do not match CID") {
		t.Fatalf("malformed multi-document stage error = %v", err)
	}
	if ok, err := store.HasVerified(t.Context(), valid.Cid()); err != nil || ok {
		t.Fatalf("valid prefix of rejected stage became verified = %t, %v; want false", ok, err)
	}
	if ok, err := store.HasVerified(t.Context(), old.Cid()); err != nil || !ok {
		t.Fatalf("old current after rejected malformed stage = %t, %v; want true", ok, err)
	}
}

func TestVerifiedDocumentStoreMixedReplacementDoesNotPartiallyCommit(t *testing.T) {
	store, err := NewVerifiedDocumentStore(memoryBlocks(), 1)
	if err != nil {
		t.Fatalf("NewVerifiedDocumentStore: %v", err)
	}
	old := testBlock(t, cid.Raw, "mixed replace old current")
	next := testBlock(t, cid.Raw, "mixed replace staged candidate")
	unstaged := testBlock(t, cid.Raw, "mixed replace unstaged candidate")
	if err := store.StageCurrentAfterVerification([]blocks.Block{old}); err != nil {
		t.Fatal(err)
	}
	if err := store.ReplaceCurrentDocuments([]cid.Cid{old.Cid()}); err != nil {
		t.Fatal(err)
	}
	if err := store.StageCurrentAfterVerification([]blocks.Block{next}); err != nil {
		t.Fatal(err)
	}
	if err := store.ReplaceCurrentDocuments([]cid.Cid{next.Cid(), unstaged.Cid()}); err == nil || !strings.Contains(err.Error(), "was not staged") {
		t.Fatalf("mixed replacement error = %v", err)
	}
	// Evict old from one-entry history. If the failed call had partially
	// replaced active state with next, old would now disappear completely.
	if err := store.RetainAfterVerification(testBlock(t, cid.Raw, "post-failure history churn")); err != nil {
		t.Fatal(err)
	}
	if ok, err := store.HasVerified(t.Context(), old.Cid()); err != nil || !ok {
		t.Fatalf("old current after rejected mixed replacement = %t, %v; want true", ok, err)
	}
}

func TestVerifiedDocumentStoreActiveConcurrentReadsAndRotation(t *testing.T) {
	store, err := NewVerifiedDocumentStore(memoryBlocks(), 2)
	if err != nil {
		t.Fatalf("NewVerifiedDocumentStore: %v", err)
	}
	docs := []blocks.Block{
		testBlock(t, cid.Raw, "concurrent current one"),
		testBlock(t, cid.Raw, "concurrent current two"),
	}
	if err := store.StageCurrentAfterVerification(docs[:1]); err != nil {
		t.Fatal(err)
	}
	if err := store.ReplaceCurrentDocuments([]cid.Cid{docs[0].Cid()}); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	for reader := 0; reader < 4; reader++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				_, _ = store.HasVerified(t.Context(), docs[i%len(docs)].Cid())
				_, _ = store.Has(t.Context(), docs[i%len(docs)].Cid())
				_, _ = store.GetSize(t.Context(), docs[i%len(docs)].Cid())
			}
		}()
	}
	for i := 0; i < 100; i++ {
		document := docs[i%len(docs)]
		if err := store.StageCurrentAfterVerification([]blocks.Block{document}); err != nil {
			t.Fatal(err)
		}
		if err := store.ReplaceCurrentDocuments([]cid.Cid{document.Cid()}); err != nil {
			t.Fatal(err)
		}
	}
	wg.Wait()
}
