package pointerhint

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"

	bmetrics "github.com/blobarchive/bloar/metrics"
)

func newTestCoordinator(t *testing.T, router ContentProvider, serving *VerifiedDocumentStore, maxHeads int, configure func(*ProviderConfig)) *Coordinator {
	t.Helper()
	cfg := ProviderConfig{
		Router:            router,
		Serving:           serving,
		VerifiedDocuments: serving,
		ReprovideInterval: time.Hour,
		ReprovideJitter:   time.Nanosecond,
		MinWriteInterval:  2 * time.Millisecond,
		RetryMin:          20 * time.Millisecond,
		RetryMax:          20 * time.Millisecond,
		AttemptTimeout:    100 * time.Millisecond,
	}
	if configure != nil {
		configure(&cfg)
	}
	coordinator, err := NewCoordinator(t.Context(), CoordinatorConfig{Provider: cfg, MaxHeads: maxHeads})
	if err != nil {
		t.Fatalf("NewCoordinator: %v", err)
	}
	t.Cleanup(func() {
		if err := coordinator.Close(); err != nil {
			t.Errorf("Coordinator.Close: %v", err)
		}
	})
	return coordinator
}

type selectiveCoordinatorRouter struct {
	mu      sync.Mutex
	calls   []provideCall
	failing map[string]bool
	blocked map[string]<-chan struct{}
	blockAt map[string]int
	count   map[string]int
	entered chan cid.Cid
}

func (r *selectiveCoordinatorRouter) Provide(ctx context.Context, c cid.Cid, announce bool) error {
	r.mu.Lock()
	r.calls = append(r.calls, provideCall{cid: c, started: time.Now()})
	key := c.KeyString()
	failing := r.failing[key]
	if r.count == nil {
		r.count = make(map[string]int)
	}
	r.count[key]++
	var blocked <-chan struct{}
	if r.blockAt[key] == r.count[key] {
		blocked = r.blocked[key]
	}
	r.mu.Unlock()
	if r.entered != nil {
		select {
		case r.entered <- c:
		default:
		}
	}
	if blocked != nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-blocked:
		}
	}
	if !announce {
		return errors.New("announce was false")
	}
	if failing {
		return errors.New("injected CID-specific failure")
	}
	return nil
}

func (r *selectiveCoordinatorRouter) setBlockOnCall(c cid.Cid, call int, release <-chan struct{}) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.blocked == nil {
		r.blocked = make(map[string]<-chan struct{})
		r.blockAt = make(map[string]int)
	}
	r.blocked[c.KeyString()] = release
	r.blockAt[c.KeyString()] = call
}

func (r *selectiveCoordinatorRouter) setFail(c cid.Cid, failing bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.failing == nil {
		r.failing = make(map[string]bool)
	}
	r.failing[c.KeyString()] = failing
}

func (r *selectiveCoordinatorRouter) snapshot() []provideCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]provideCall(nil), r.calls...)
}

func waitCoordinatorCIDCalls(t *testing.T, router *selectiveCoordinatorRouter, c cid.Cid, count int) []provideCall {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		calls := router.snapshot()
		matches := 0
		for _, call := range calls {
			if call.cid.Equals(c) {
				matches++
			}
		}
		if matches >= count {
			return calls
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d calls for %s; got %v", count, c, router.snapshot())
	return nil
}

func TestCoordinatorDeduplicatesSharedPointersAndPreservesWriteCeiling(t *testing.T) {
	serving, err := NewVerifiedDocumentStore(memoryBlocks(), 4)
	if err != nil {
		t.Fatalf("NewVerifiedDocumentStore: %v", err)
	}
	rootA := put(t, serving, cid.DagCBOR, "coordinator root a")
	rootB := put(t, serving, cid.DagCBOR, "coordinator root b")
	manifest := put(t, serving, cid.DagCBOR, "shared coordinator manifest")
	document := testBlock(t, cid.Raw, "shared verified coordinator document")
	if err := serving.RetainAfterVerification(document); err != nil {
		t.Fatalf("RetainAfterVerification: %v", err)
	}

	router := &recordingRouter{}
	coordinator := newTestCoordinator(t, router, serving, 2, func(cfg *ProviderConfig) {
		cfg.MinWriteInterval = 8 * time.Millisecond
	})
	shared := Set{Manifest: manifest, Document: document.Cid()}
	if err := coordinator.UpdateHead("alpha", Set{Root: rootA, Manifest: shared.Manifest, Document: shared.Document}); err != nil {
		t.Fatalf("UpdateHead alpha: %v", err)
	}
	waitCalls(t, router, 3)
	if err := coordinator.UpdateHead("beta", Set{Root: rootB, Manifest: shared.Manifest, Document: shared.Document}); err != nil {
		t.Fatalf("UpdateHead beta: %v", err)
	}
	calls := waitCalls(t, router, 4)

	counts := make(map[string]int)
	for i, call := range calls {
		counts[call.cid.KeyString()]++
		if i > 0 && call.started.Sub(calls[i-1].started) < 7*time.Millisecond {
			t.Fatalf("aggregate Provide starts were %s apart, below the configured process-wide 8ms ceiling", call.started.Sub(calls[i-1].started))
		}
	}
	for _, item := range []cid.Cid{rootA, rootB, manifest, document.Cid()} {
		if counts[item.KeyString()] != 1 {
			t.Errorf("Provide count for %s = %d, want one", item, counts[item.KeyString()])
		}
	}

	// Removing alpha must withdraw its unique root while preserving shared
	// pointers and their existing one-hour cadence through beta.
	if err := coordinator.RemoveHead("alpha"); err != nil {
		t.Fatalf("RemoveHead alpha: %v", err)
	}
	items := providerPointerSnapshot(coordinator.provider)
	assertPointerSchedule(t, items, []Pointer{
		{Kind: Root, CID: rootB},
		{Kind: Manifest, CID: manifest},
		{Kind: Document, CID: document.Cid()},
	})
	time.Sleep(25 * time.Millisecond)
	if got := len(router.snapshot()); got != 4 {
		t.Fatalf("removing one owner restarted a shared pointer; calls = %d, want 4", got)
	}
	if err := coordinator.RemoveHead("beta"); err != nil {
		t.Fatalf("RemoveHead beta: %v", err)
	}
	items = providerPointerSnapshot(coordinator.provider)
	if len(items) != 0 {
		t.Fatalf("schedule after removing every head = %v, want empty", items)
	}
}

func TestCoordinatorReplaceAllInstallsOneAtomicDeduplicatedSchedule(t *testing.T) {
	serving, err := NewVerifiedDocumentStore(memoryBlocks(), 4)
	if err != nil {
		t.Fatalf("NewVerifiedDocumentStore: %v", err)
	}
	oldRoot := put(t, serving, cid.DagCBOR, "replace-all old root")
	rootA := put(t, serving, cid.DagCBOR, "replace-all root a")
	rootB := put(t, serving, cid.DagCBOR, "replace-all root b")
	manifest := put(t, serving, cid.DagCBOR, "replace-all shared manifest")
	document := testBlock(t, cid.Raw, "replace-all shared verified document")
	if err := serving.RetainAfterVerification(document); err != nil {
		t.Fatalf("RetainAfterVerification: %v", err)
	}

	coordinator := newTestCoordinator(t, &recordingRouter{}, serving, 3, nil)
	if err := coordinator.UpdateHead("old", Set{Root: oldRoot}); err != nil {
		t.Fatalf("install old schedule: %v", err)
	}
	_, beforeVersion := coordinator.provider.snapshot()

	input := map[string]Set{
		"alpha": {Root: rootA, Manifest: manifest, Document: document.Cid()},
		"beta":  {Root: rootB, Manifest: manifest, Document: document.Cid()},
		"empty": {},
	}
	if err := coordinator.ReplaceAll(input); err != nil {
		t.Fatalf("ReplaceAll: %v", err)
	}
	items, afterVersion := coordinator.provider.snapshot()
	if afterVersion != beforeVersion+1 {
		t.Fatalf("provider version advanced from %d to %d, want exactly one update", beforeVersion, afterVersion)
	}
	got := make([]Pointer, len(items))
	for i, item := range items {
		got[i] = item.pointer
	}
	assertPointerSchedule(t, got, []Pointer{
		{Kind: Root, CID: rootA},
		{Kind: Root, CID: rootB},
		{Kind: Manifest, CID: manifest},
		{Kind: Document, CID: document.Cid()},
	})
	if _, exists := coordinator.heads["old"]; exists {
		t.Fatalf("replaced snapshot retained withdrawn head: %v", coordinator.heads)
	}
	if !reflect.DeepEqual(coordinator.heads, input) {
		t.Fatalf("installed heads = %#v, want %#v", coordinator.heads, input)
	}

	// Caller ownership ends at the call boundary.
	input["alpha"] = Set{Root: oldRoot}
	delete(input, "beta")
	if got := coordinator.heads["alpha"].Root; !got.Equals(rootA) {
		t.Fatalf("caller mutation changed alpha root to %s, want %s", got, rootA)
	}
	if _, exists := coordinator.heads["beta"]; !exists {
		t.Fatal("caller deletion removed installed beta head")
	}

	if err := coordinator.ReplaceAll(nil); err != nil {
		t.Fatalf("ReplaceAll(nil): %v", err)
	}
	if len(coordinator.heads) != 0 {
		t.Fatalf("nil replacement retained heads: %v", coordinator.heads)
	}
	if got := providerPointerSnapshot(coordinator.provider); len(got) != 0 {
		t.Fatalf("nil replacement retained provider schedule: %v", got)
	}
}

func TestCoordinatorExtraDocumentIsAtomicDeduplicatedAndPreservedByHeadUpdates(t *testing.T) {
	serving, err := NewVerifiedDocumentStore(memoryBlocks(), 4)
	if err != nil {
		t.Fatalf("NewVerifiedDocumentStore: %v", err)
	}
	rootA := put(t, serving, cid.DagCBOR, "extra-document root a")
	rootB := put(t, serving, cid.DagCBOR, "extra-document root b")
	document := testBlock(t, cid.Raw, "extra current publication document")
	if err := serving.RetainAfterVerification(document); err != nil {
		t.Fatalf("RetainAfterVerification: %v", err)
	}

	coordinator := newTestCoordinator(t, &recordingRouter{}, serving, 1, nil)
	inputDocuments := []cid.Cid{document.Cid()}
	if err := coordinator.ReplaceAllWithDocuments(map[string]Set{
		"alpha": {Root: rootA, Document: document.Cid()},
	}, inputDocuments); err != nil {
		t.Fatalf("ReplaceAllWithDocuments: %v", err)
	}
	assertPointerSchedule(t, providerPointerSnapshot(coordinator.provider), []Pointer{
		{Kind: Root, CID: rootA},
		{Kind: Document, CID: document.Cid()},
	})
	if got := len(coordinator.documents); got != 1 {
		t.Fatalf("installed extra document count = %d, want 1", got)
	}

	// Caller ownership ends at the call boundary.
	inputDocuments[0] = cid.Undef
	if !coordinator.documents[0].Equals(document.Cid()) {
		t.Fatalf("caller slice mutation changed installed document to %s", coordinator.documents[0])
	}

	// An extra document is independent of the configured head slots and is
	// preserved by both compatibility mutation APIs.
	if err := coordinator.UpdateHead("alpha", Set{Root: rootB}); err != nil {
		t.Fatalf("UpdateHead: %v", err)
	}
	assertPointerSchedule(t, providerPointerSnapshot(coordinator.provider), []Pointer{
		{Kind: Root, CID: rootB},
		{Kind: Document, CID: document.Cid()},
	})
	if err := coordinator.UpdateHead("beta", Set{Root: rootA}); err == nil {
		t.Fatal("extra document unexpectedly consumed the sole head slot")
	}
	if err := coordinator.RemoveHead("alpha"); err != nil {
		t.Fatalf("RemoveHead: %v", err)
	}
	assertPointerSchedule(t, providerPointerSnapshot(coordinator.provider), []Pointer{
		{Kind: Document, CID: document.Cid()},
	})

	// The compatibility whole-snapshot API deliberately clears extras.
	if err := coordinator.ReplaceAll(map[string]Set{"alpha": {Root: rootA}}); err != nil {
		t.Fatalf("ReplaceAll: %v", err)
	}
	if len(coordinator.documents) != 0 {
		t.Fatalf("ReplaceAll retained extra documents: %v", coordinator.documents)
	}
	assertPointerSchedule(t, providerPointerSnapshot(coordinator.provider), []Pointer{
		{Kind: Root, CID: rootA},
	})
}

func TestCoordinatorExtraDocumentValidationIsTransactional(t *testing.T) {
	serving, err := NewVerifiedDocumentStore(memoryBlocks(), 4)
	if err != nil {
		t.Fatalf("NewVerifiedDocumentStore: %v", err)
	}
	root := put(t, serving, cid.DagCBOR, "extra validation root")
	document := testBlock(t, cid.Raw, "extra validation retained document")
	otherDocument := testBlock(t, cid.Raw, "extra validation second document")
	for _, block := range []blocks.Block{document, otherDocument} {
		if err := serving.RetainAfterVerification(block); err != nil {
			t.Fatalf("RetainAfterVerification(%s): %v", block.Cid(), err)
		}
	}
	wrongCodec := testBlock(t, cid.DagCBOR, "extra validation wrong codec").Cid()
	widePrefix := cid.Prefix{Version: 1, Codec: cid.Raw, MhType: multihash.SHA2_512, MhLength: -1}
	wrongHash, err := widePrefix.Sum([]byte("extra validation wrong hash"))
	if err != nil {
		t.Fatalf("wide raw CID: %v", err)
	}

	coordinator := newTestCoordinator(t, &recordingRouter{}, serving, 1, nil)
	baselineHeads := map[string]Set{"alpha": {Root: root}}
	baselineDocuments := []cid.Cid{document.Cid()}
	if err := coordinator.ReplaceAllWithDocuments(baselineHeads, baselineDocuments); err != nil {
		t.Fatalf("install baseline: %v", err)
	}
	wantSchedule, wantVersion := coordinator.provider.snapshot()

	cases := []struct {
		name      string
		documents []cid.Cid
		want      string
	}{
		{name: "too many even when distinct", documents: []cid.Cid{document.Cid(), otherDocument.Cid()}, want: "exceeds limit 1"},
		{name: "too many even when duplicate", documents: []cid.Cid{document.Cid(), document.Cid()}, want: "exceeds limit 1"},
		{name: "undefined", documents: []cid.Cid{cid.Undef}, want: "document CID must be defined"},
		{name: "wrong codec", documents: []cid.Cid{wrongCodec}, want: "is not a raw CID"},
		{name: "wrong hash", documents: []cid.Cid{wrongHash}, want: "32-byte sha2-256"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := coordinator.ReplaceAllWithDocuments(map[string]Set{"alpha": {Root: root}}, tc.documents)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("ReplaceAllWithDocuments error = %v, want containing %q", err, tc.want)
			}
			if !reflect.DeepEqual(coordinator.heads, baselineHeads) {
				t.Fatalf("rejected documents changed heads to %#v, want %#v", coordinator.heads, baselineHeads)
			}
			if !reflect.DeepEqual(coordinator.documents, baselineDocuments) {
				t.Fatalf("rejected documents changed extras to %#v, want %#v", coordinator.documents, baselineDocuments)
			}
			gotSchedule, gotVersion := coordinator.provider.snapshot()
			if gotVersion != wantVersion {
				t.Fatalf("rejected documents advanced provider version to %d, want %d", gotVersion, wantVersion)
			}
			if !reflect.DeepEqual(gotSchedule, wantSchedule) {
				t.Fatalf("rejected documents changed schedule to %#v, want %#v", gotSchedule, wantSchedule)
			}
		})
	}
}

func TestCoordinatorSnapshotPreflightIsPureAndMatchesReplacementValidation(t *testing.T) {
	serving, err := NewVerifiedDocumentStore(memoryBlocks(), 2)
	if err != nil {
		t.Fatalf("NewVerifiedDocumentStore: %v", err)
	}
	rootA := put(t, serving, cid.DagCBOR, "preflight root a")
	rootB := put(t, serving, cid.DagCBOR, "preflight root b")
	shared := put(t, serving, cid.DagCBOR, "preflight cross-kind cid")
	document := testBlock(t, cid.Raw, "preflight document")
	if err := serving.RetainAfterVerification(document); err != nil {
		t.Fatal(err)
	}
	coordinator := newTestCoordinator(t, &recordingRouter{}, serving, 2, nil)
	baseline := map[string]Set{"alpha": {Root: rootA}}
	if err := coordinator.ReplaceAll(baseline); err != nil {
		t.Fatal(err)
	}
	wantSchedule, wantVersion := coordinator.provider.snapshot()

	candidate := map[string]Set{
		"alpha": {Root: rootB},
		"beta":  {Root: shared},
	}
	if err := coordinator.ValidateAllWithDocuments(candidate, []cid.Cid{document.Cid()}); err != nil {
		t.Fatalf("valid preflight: %v", err)
	}
	if !reflect.DeepEqual(coordinator.heads, baseline) {
		t.Fatalf("preflight changed heads to %#v, want %#v", coordinator.heads, baseline)
	}
	if got, version := coordinator.provider.snapshot(); version != wantVersion || !reflect.DeepEqual(got, wantSchedule) {
		t.Fatalf("preflight changed provider to version=%d schedule=%#v, want version=%d schedule=%#v",
			version, got, wantVersion, wantSchedule)
	}

	conflict := map[string]Set{
		"alpha": {Root: shared},
		"beta":  {Root: rootB, Manifest: shared},
	}
	preflightErr := coordinator.ValidateAllWithDocuments(conflict, []cid.Cid{document.Cid()})
	replaceErr := coordinator.ReplaceAllWithDocuments(conflict, []cid.Cid{document.Cid()})
	if preflightErr == nil || replaceErr == nil ||
		!strings.Contains(preflightErr.Error(), "both root and manifest") ||
		preflightErr.Error() != replaceErr.Error() {
		t.Fatalf("preflight error = %v, replacement error = %v, want identical cross-kind rejection",
			preflightErr, replaceErr)
	}
	if !reflect.DeepEqual(coordinator.heads, baseline) {
		t.Fatalf("rejected preflight/replacement changed heads to %#v, want %#v", coordinator.heads, baseline)
	}
}

func TestCoordinatorReplaceAllRejectsWholeCandidateTransactionally(t *testing.T) {
	serving, err := NewVerifiedDocumentStore(memoryBlocks(), 2)
	if err != nil {
		t.Fatalf("NewVerifiedDocumentStore: %v", err)
	}
	rootA := put(t, serving, cid.DagCBOR, "replace rejection retained root a")
	rootB := put(t, serving, cid.DagCBOR, "replace rejection candidate root b")
	shared := put(t, serving, cid.DagCBOR, "replace rejection ambiguous cid")
	coordinator := newTestCoordinator(t, &recordingRouter{}, serving, 2, nil)
	wantHeads := map[string]Set{"alpha": {Root: rootA}}
	if err := coordinator.ReplaceAll(wantHeads); err != nil {
		t.Fatalf("install baseline: %v", err)
	}
	wantSchedule := providerPointerSnapshot(coordinator.provider)
	_, wantVersion := coordinator.provider.snapshot()

	cases := []struct {
		name  string
		heads map[string]Set
		want  string
	}{
		{
			name:  "too many heads",
			heads: map[string]Set{"alpha": {}, "beta": {}, "gamma": {}},
			want:  "exceeds limit 2",
		},
		{
			name:  "invalid head name",
			heads: map[string]Set{"alpha": {Root: rootA}, "Bad": {Root: rootB}},
			want:  "does not match",
		},
		{
			name:  "invalid individual set",
			heads: map[string]Set{"alpha": {Root: rootA}, "beta": {Manifest: shared}},
			want:  "requires its current root",
		},
		{
			name: "cross head kind conflict",
			heads: map[string]Set{
				"alpha": {Root: shared},
				"beta":  {Root: rootB, Manifest: shared},
			},
			want: "both root and manifest across heads",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := coordinator.ReplaceAll(tc.heads)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("ReplaceAll error = %v, want containing %q", err, tc.want)
			}
			if !reflect.DeepEqual(coordinator.heads, wantHeads) {
				t.Fatalf("rejected candidate changed heads to %#v, want %#v", coordinator.heads, wantHeads)
			}
			gotSchedule, gotVersion := coordinator.provider.snapshot()
			if gotVersion != wantVersion {
				t.Fatalf("rejected candidate advanced provider version to %d, want %d", gotVersion, wantVersion)
			}
			got := make([]Pointer, len(gotSchedule))
			for i, item := range gotSchedule {
				got[i] = item.pointer
			}
			assertPointerSchedule(t, got, wantSchedule)
		})
	}
}

func TestCoordinatorReplaceAllProviderRejectionLeavesBaselineInstalled(t *testing.T) {
	serving, err := NewVerifiedDocumentStore(memoryBlocks(), 1)
	if err != nil {
		t.Fatalf("NewVerifiedDocumentStore: %v", err)
	}
	root := put(t, serving, cid.DagCBOR, "replace provider rejection root")
	document := testBlock(t, cid.Raw, "replace provider rejection document")
	if err := serving.Put(t.Context(), document); err != nil {
		t.Fatalf("Put document: %v", err)
	}
	coordinator, err := NewCoordinator(t.Context(), CoordinatorConfig{
		MaxHeads: 1,
		Provider: ProviderConfig{
			Router:            &recordingRouter{},
			Serving:           serving,
			ReprovideInterval: time.Hour,
			ReprovideJitter:   time.Nanosecond,
			MinWriteInterval:  time.Millisecond,
			RetryMin:          time.Millisecond,
			RetryMax:          time.Millisecond,
			AttemptTimeout:    100 * time.Millisecond,
		},
	})
	if err != nil {
		t.Fatalf("NewCoordinator: %v", err)
	}
	t.Cleanup(func() { _ = coordinator.Close() })
	baseline := map[string]Set{"alpha": {Root: root}}
	if err := coordinator.ReplaceAll(baseline); err != nil {
		t.Fatalf("install baseline: %v", err)
	}
	wantSchedule, wantVersion := coordinator.provider.snapshot()

	err = coordinator.ReplaceAll(map[string]Set{
		"alpha": {Root: root, Document: document.Cid()},
	})
	if err == nil || !strings.Contains(err.Error(), "requires VerifiedDocuments") {
		t.Fatalf("provider rejection error = %v", err)
	}
	if !reflect.DeepEqual(coordinator.heads, baseline) {
		t.Fatalf("provider rejection changed heads to %#v, want %#v", coordinator.heads, baseline)
	}
	gotSchedule, gotVersion := coordinator.provider.snapshot()
	if gotVersion != wantVersion {
		t.Fatalf("provider rejection advanced version to %d, want %d", gotVersion, wantVersion)
	}
	if !reflect.DeepEqual(gotSchedule, wantSchedule) {
		t.Fatalf("provider rejection changed schedule to %#v, want %#v", gotSchedule, wantSchedule)
	}
}

func TestCoordinatorExtraDocumentProviderFailureLeavesBaselineInstalled(t *testing.T) {
	serving, err := NewVerifiedDocumentStore(memoryBlocks(), 2)
	if err != nil {
		t.Fatalf("NewVerifiedDocumentStore: %v", err)
	}
	rootA := put(t, serving, cid.DagCBOR, "extra provider failure root a")
	rootB := put(t, serving, cid.DagCBOR, "extra provider failure root b")
	documentA := testBlock(t, cid.Raw, "extra provider failure document a")
	documentB := testBlock(t, cid.Raw, "extra provider failure document b")
	for _, block := range []blocks.Block{documentA, documentB} {
		if err := serving.RetainAfterVerification(block); err != nil {
			t.Fatalf("RetainAfterVerification(%s): %v", block.Cid(), err)
		}
	}

	coordinator := newTestCoordinator(t, &recordingRouter{}, serving, 1, nil)
	wantHeads := map[string]Set{"alpha": {Root: rootA}}
	wantDocuments := []cid.Cid{documentA.Cid()}
	if err := coordinator.ReplaceAllWithDocuments(wantHeads, wantDocuments); err != nil {
		t.Fatalf("install baseline: %v", err)
	}
	wantSchedule, wantVersion := coordinator.provider.snapshot()
	if err := coordinator.provider.Close(); err != nil {
		t.Fatalf("close provider: %v", err)
	}

	err = coordinator.ReplaceAllWithDocuments(
		map[string]Set{"alpha": {Root: rootB}},
		[]cid.Cid{documentB.Cid()},
	)
	if err == nil || !strings.Contains(err.Error(), "provider is closed") {
		t.Fatalf("provider failure error = %v, want provider is closed", err)
	}
	if !reflect.DeepEqual(coordinator.heads, wantHeads) {
		t.Fatalf("provider failure changed heads to %#v, want %#v", coordinator.heads, wantHeads)
	}
	if !reflect.DeepEqual(coordinator.documents, wantDocuments) {
		t.Fatalf("provider failure changed documents to %#v, want %#v", coordinator.documents, wantDocuments)
	}
	gotSchedule, gotVersion := coordinator.provider.snapshot()
	if gotVersion != wantVersion || !reflect.DeepEqual(gotSchedule, wantSchedule) {
		t.Fatalf("provider failure changed schedule/version to %#v/%d, want %#v/%d", gotSchedule, gotVersion, wantSchedule, wantVersion)
	}
}

func TestCoordinatorReplaceAllSerializesWithConcurrentCompatibilityUpdates(t *testing.T) {
	serving, err := NewVerifiedDocumentStore(memoryBlocks(), 1)
	if err != nil {
		t.Fatalf("NewVerifiedDocumentStore: %v", err)
	}
	roots := []cid.Cid{
		put(t, serving, cid.DagCBOR, "replace concurrent root a"),
		put(t, serving, cid.DagCBOR, "replace concurrent root b"),
		put(t, serving, cid.DagCBOR, "replace concurrent root c"),
	}
	document := testBlock(t, cid.Raw, "replace concurrent extra document")
	if err := serving.RetainAfterVerification(document); err != nil {
		t.Fatalf("RetainAfterVerification: %v", err)
	}
	coordinator := newTestCoordinator(t, &recordingRouter{}, serving, 4, nil)

	start := make(chan struct{})
	errs := make(chan error, 3)
	var wg sync.WaitGroup
	for worker := 0; worker < 3; worker++ {
		worker := worker
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for update := 0; update < 100; update++ {
				var err error
				if worker == 0 {
					err = coordinator.ReplaceAllWithDocuments(map[string]Set{
						"alpha": {Root: roots[update%len(roots)]},
						"beta":  {Root: roots[(update+1)%len(roots)]},
					}, []cid.Cid{document.Cid()})
				} else {
					err = coordinator.UpdateHead(fmt.Sprintf("compat-%d", worker), Set{Root: roots[(worker+update)%len(roots)]})
				}
				if err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("concurrent coordinator mutation: %v", err)
	}

	coordinator.mu.Lock()
	wantItems, err := aggregateCoordinatorState(coordinator.heads, coordinator.documents)
	coordinator.mu.Unlock()
	if err != nil {
		t.Fatalf("final accepted heads are invalid: %v", err)
	}
	assertPointerSchedule(t, providerPointerSnapshot(coordinator.provider), wantItems)
	if len(coordinator.heads) > coordinator.maxHeads {
		t.Fatalf("concurrent mutations installed %d heads above limit %d", len(coordinator.heads), coordinator.maxHeads)
	}
	if len(coordinator.documents) != 1 || !coordinator.documents[0].Equals(document.Cid()) {
		t.Fatalf("concurrent compatibility updates lost extra document: %v", coordinator.documents)
	}
}

func TestCoordinatorRejectsCrossHeadKindConflictTransactionally(t *testing.T) {
	serving, err := NewVerifiedDocumentStore(memoryBlocks(), 1)
	if err != nil {
		t.Fatalf("NewVerifiedDocumentStore: %v", err)
	}
	shared := put(t, serving, cid.DagCBOR, "ambiguous root or manifest")
	rootB := put(t, serving, cid.DagCBOR, "uncommitted beta root")
	router := &recordingRouter{}
	coordinator := newTestCoordinator(t, router, serving, 2, nil)
	if err := coordinator.UpdateHead("alpha", Set{Root: shared}); err != nil {
		t.Fatalf("UpdateHead alpha: %v", err)
	}
	waitCalls(t, router, 1)

	err = coordinator.UpdateHead("beta", Set{Root: rootB, Manifest: shared})
	if err == nil || !strings.Contains(err.Error(), "both root and manifest across heads") {
		t.Fatalf("ambiguous cross-head CID error = %v", err)
	}
	if len(coordinator.heads) != 1 {
		t.Fatalf("conflicting update mutated accepted heads: %v", coordinator.heads)
	}
	items := providerPointerSnapshot(coordinator.provider)
	assertPointerSchedule(t, items, []Pointer{{Kind: Root, CID: shared}})
	time.Sleep(20 * time.Millisecond)
	for _, call := range router.snapshot() {
		if call.cid.Equals(rootB) {
			t.Fatalf("pointer from rejected conflicting head reached DHT: %v", router.snapshot())
		}
	}

	// A later valid update proves the rejected candidate was not partially
	// installed or counted against future updates.
	if err := coordinator.UpdateHead("beta", Set{Root: rootB}); err != nil {
		t.Fatalf("valid beta update after conflict: %v", err)
	}
	waitCalls(t, router, 2)
}

func TestCoordinatorDocumentsRequireLiveVerifiedRetention(t *testing.T) {
	serving, err := NewVerifiedDocumentStore(memoryBlocks(), 2)
	if err != nil {
		t.Fatalf("NewVerifiedDocumentStore: %v", err)
	}
	root := put(t, serving, cid.DagCBOR, "document gate root")
	document := testBlock(t, cid.Raw, "present but not verified for coordinator")
	if err := serving.Put(t.Context(), document); err != nil {
		t.Fatalf("Put document: %v", err)
	}
	router := &recordingRouter{}
	coordinator := newTestCoordinator(t, router, serving, 1, func(cfg *ProviderConfig) {
		cfg.RetryMin = 50 * time.Millisecond
		cfg.RetryMax = 50 * time.Millisecond
	})
	if err := coordinator.UpdateHead("alpha", Set{Root: root, Document: document.Cid()}); err != nil {
		t.Fatalf("UpdateHead: %v", err)
	}
	waitCalls(t, router, 1)
	time.Sleep(15 * time.Millisecond)
	for _, call := range router.snapshot() {
		if call.cid.Equals(document.Cid()) {
			t.Fatal("unverified document reached DHT")
		}
	}
	if err := serving.RetainAfterVerification(document); err != nil {
		t.Fatalf("RetainAfterVerification: %v", err)
	}
	calls := waitCalls(t, router, 2)
	if !calls[1].cid.Equals(document.Cid()) {
		t.Fatalf("first call after verified retention = %s, want document %s", calls[1].cid, document.Cid())
	}

	// A coordinator without a verified-retention authority rejects the whole
	// candidate rather than installing just its root.
	plainRouter := &recordingRouter{}
	plain, err := NewCoordinator(t.Context(), CoordinatorConfig{
		MaxHeads: 1,
		Provider: ProviderConfig{
			Router:            plainRouter,
			Serving:           serving,
			ReprovideInterval: time.Hour,
			ReprovideJitter:   time.Nanosecond,
			MinWriteInterval:  time.Millisecond,
			RetryMin:          time.Millisecond,
			RetryMax:          time.Millisecond,
			AttemptTimeout:    100 * time.Millisecond,
		},
	})
	if err != nil {
		t.Fatalf("NewCoordinator without VerifiedDocuments: %v", err)
	}
	t.Cleanup(func() { _ = plain.Close() })
	if err := plain.UpdateHead("alpha", Set{Root: root, Document: document.Cid()}); err == nil {
		t.Fatal("document schedule without VerifiedDocuments was accepted")
	}
	if len(plain.heads) != 0 {
		t.Fatalf("rejected document schedule mutated coordinator: %v", plain.heads)
	}
	if items, _ := plain.provider.snapshot(); len(items) != 0 {
		t.Fatalf("rejected document schedule partially installed pointers: %v", items)
	}
}

func TestCoordinatorCoalescesCrossHeadUpdateBurst(t *testing.T) {
	serving, err := NewVerifiedDocumentStore(memoryBlocks(), 1)
	if err != nil {
		t.Fatalf("NewVerifiedDocumentStore: %v", err)
	}
	rootA := put(t, serving, cid.DagCBOR, "aggregate in flight")
	rootB := put(t, serving, cid.DagCBOR, "aggregate intermediate")
	rootC := put(t, serving, cid.DagCBOR, "aggregate final")
	release := make(chan struct{})
	router := &recordingRouter{entered: make(chan cid.Cid, 1), release: release}
	coordinator := newTestCoordinator(t, router, serving, 2, nil)
	if err := coordinator.UpdateHead("alpha", Set{Root: rootA}); err != nil {
		t.Fatalf("UpdateHead alpha: %v", err)
	}
	select {
	case got := <-router.entered:
		if !got.Equals(rootA) {
			t.Fatalf("in-flight CID = %s, want %s", got, rootA)
		}
	case <-time.After(time.Second):
		t.Fatal("alpha root did not enter Provide")
	}
	if err := coordinator.UpdateHead("beta", Set{Root: rootB}); err != nil {
		t.Fatalf("UpdateHead beta B: %v", err)
	}
	if err := coordinator.UpdateHead("beta", Set{Root: rootC}); err != nil {
		t.Fatalf("UpdateHead beta C: %v", err)
	}
	close(release)
	calls := waitCalls(t, router, 2)
	if !calls[1].cid.Equals(rootC) {
		t.Fatalf("post-burst Provide = %s, want final root %s", calls[1].cid, rootC)
	}
	for _, call := range calls {
		if call.cid.Equals(rootB) {
			t.Fatalf("coalesced intermediate pointer reached DHT: %v", calls)
		}
	}
}

func TestCoordinatorFreshnessIsOldestSuccessAcrossEveryCurrentCID(t *testing.T) {
	serving, err := NewVerifiedDocumentStore(memoryBlocks(), 1)
	if err != nil {
		t.Fatalf("NewVerifiedDocumentStore: %v", err)
	}
	rootA := put(t, serving, cid.DagCBOR, "older aggregate root success")
	rootB := put(t, serving, cid.DagCBOR, "newer aggregate root success")
	router := &selectiveCoordinatorRouter{}
	router.setFail(rootB, true)
	mx := bmetrics.New()
	coordinator := newTestCoordinator(t, router, serving, 2, func(cfg *ProviderConfig) {
		cfg.Metrics = mx
		cfg.RetryMin = 25 * time.Millisecond
		cfg.RetryMax = 25 * time.Millisecond
	})
	if err := coordinator.UpdateHead("alpha", Set{Root: rootA}); err != nil {
		t.Fatalf("UpdateHead alpha: %v", err)
	}
	waitCoordinatorCIDCalls(t, router, rootA, 1)
	if err := coordinator.UpdateHead("beta", Set{Root: rootB}); err != nil {
		t.Fatalf("UpdateHead beta: %v", err)
	}
	waitCoordinatorCIDCalls(t, router, rootB, 1)
	waitPointerMetric(t, mx, `bloar_pointer_provides_total{kind="root",outcome="error"}`, 1)
	if got := pointerMetricSample(t, mx, `bloar_pointer_provide_last_success_timestamp_seconds{kind="root"}`); got != 0 {
		t.Fatalf("one successful and one failing current root produced freshness %g, want 0", got)
	}
	if got := pointerMetricSample(t, mx, `bloar_pointer_provides_total{kind="root",outcome="ok"}`); got != 1 {
		t.Fatalf("successful per-CID Provide count = %g, want 1", got)
	}
	router.setFail(rootB, false)
	calls := waitCoordinatorCIDCalls(t, router, rootB, 2)
	var secondB time.Time
	for _, call := range calls {
		if call.cid.Equals(rootB) {
			secondB = call.started
		}
	}
	oldest := waitPointerMetric(t, mx, `bloar_pointer_provide_last_success_timestamp_seconds{kind="root"}`, 1)
	if oldest >= float64(secondB.UnixNano())/float64(time.Second) {
		t.Fatalf("aggregate freshness = %g, want older root A success before root B retry at %s", oldest, secondB)
	}
	if got := pointerMetricSample(t, mx, `bloar_pointer_provides_total{kind="root",outcome="ok"}`); got != 2 {
		t.Fatalf("successful per-CID Provide count after recovery = %g, want 2", got)
	}

	// Removing the older member recomputes freshness from retained root B.
	if err := coordinator.RemoveHead("alpha"); err != nil {
		t.Fatalf("RemoveHead alpha: %v", err)
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		retained := pointerMetricSample(t, mx, `bloar_pointer_provide_last_success_timestamp_seconds{kind="root"}`)
		if retained > oldest {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("pure removal did not recompute freshness above removed oldest member %g", oldest)
}

func TestCoordinatorReaddedCIDDoesNotInheritOldInFlightGeneration(t *testing.T) {
	serving, err := NewVerifiedDocumentStore(memoryBlocks(), 1)
	if err != nil {
		t.Fatalf("NewVerifiedDocumentStore: %v", err)
	}
	rootA := put(t, serving, cid.DagCBOR, "in-flight old aggregate root")
	router := &selectiveCoordinatorRouter{entered: make(chan cid.Cid, 8)}
	release := make(chan struct{})
	router.setBlockOnCall(rootA, 2, release)
	mx := bmetrics.New()
	coordinator := newTestCoordinator(t, router, serving, 1, func(cfg *ProviderConfig) {
		cfg.Metrics = mx
		cfg.ReprovideInterval = 100 * time.Millisecond
		cfg.ReprovideJitter = time.Nanosecond
	})
	if err := coordinator.UpdateHead("alpha", Set{Root: rootA}); err != nil {
		t.Fatalf("UpdateHead alpha A: %v", err)
	}
	waitCoordinatorCIDCalls(t, router, rootA, 1)
	waitPointerMetric(t, mx, `bloar_pointer_provide_last_success_timestamp_seconds{kind="root"}`, 1)
	<-router.entered // consume the initial successful call notification
	select {
	case got := <-router.entered:
		if !got.Equals(rootA) {
			t.Fatalf("blocked reprovide = %s, want old root %s", got, rootA)
		}
	case <-time.After(time.Second):
		t.Fatal("old root did not enter blocked reprovide")
	}
	if err := coordinator.UpdateHead("alpha", Set{}); err != nil {
		t.Fatalf("withdraw alpha: %v", err)
	}
	if err := coordinator.UpdateHead("alpha", Set{Root: rootA}); err != nil {
		t.Fatalf("re-add alpha A: %v", err)
	}
	if got := pointerMetricSample(t, mx, `bloar_pointer_provide_last_success_timestamp_seconds{kind="root"}`); got != 0 {
		t.Fatalf("re-added CID inherited prior generation freshness %g, want synchronous reset", got)
	}

	// The old generation completes successfully, while the fresh generation's
	// immediate attempt fails. Neither event may lend the new generation its
	// retired predecessor's timestamp.
	router.setFail(rootA, true)
	close(release)
	waitCoordinatorCIDCalls(t, router, rootA, 3)
	waitPointerMetric(t, mx, `bloar_pointer_provides_total{kind="root",outcome="error"}`, 1)
	if got := pointerMetricSample(t, mx, `bloar_pointer_provide_last_success_timestamp_seconds{kind="root"}`); got != 0 {
		t.Fatalf("stale old generation restamped re-added CID freshness %g", got)
	}
	router.setFail(rootA, false)
	waitCoordinatorCIDCalls(t, router, rootA, 4)
	if got := waitPointerMetric(t, mx, `bloar_pointer_provide_last_success_timestamp_seconds{kind="root"}`, 1); got <= 0 {
		t.Fatalf("fresh generation success left freshness at %g", got)
	}
}

func TestCoordinatorBoundsHeadsAndRejectsInvalidNames(t *testing.T) {
	serving, err := NewVerifiedDocumentStore(memoryBlocks(), 1)
	if err != nil {
		t.Fatalf("NewVerifiedDocumentStore: %v", err)
	}
	root := put(t, serving, cid.DagCBOR, "bounded head root")
	document := testBlock(t, cid.Raw, "bounded extra document")
	if err := serving.RetainAfterVerification(document); err != nil {
		t.Fatalf("RetainAfterVerification: %v", err)
	}
	if got, want := MaxCoordinatorPointers, MaxCoordinatorHeads*3+MaxCoordinatorExtraDocuments; got != want {
		t.Fatalf("MaxCoordinatorPointers = %d, want %d", got, want)
	}
	tooLarge, err := NewCoordinator(t.Context(), CoordinatorConfig{
		MaxHeads: MaxCoordinatorHeads + 1,
		Provider: ProviderConfig{Router: &recordingRouter{}, Serving: serving},
	})
	if err == nil {
		_ = tooLarge.Close()
		t.Fatalf("MaxHeads above hard ceiling %d was accepted", MaxCoordinatorHeads)
	}
	coordinator := newTestCoordinator(t, &recordingRouter{}, serving, 1, nil)
	if err := coordinator.provider.updatePointers(make([]Pointer, MaxCoordinatorPointers+1)); err == nil {
		t.Fatalf("flat provider schedule above hard ceiling %d was accepted", MaxCoordinatorPointers)
	}
	maxSchedule := make([]Pointer, 0, MaxCoordinatorPointers)
	for i := 0; i < MaxCoordinatorHeads; i++ {
		maxSchedule = append(maxSchedule,
			Pointer{Kind: Root, CID: testBlock(t, cid.DagCBOR, fmt.Sprintf("bounded root %d", i)).Cid()},
			Pointer{Kind: Manifest, CID: testBlock(t, cid.DagCBOR, fmt.Sprintf("bounded manifest %d", i)).Cid()},
			Pointer{Kind: Document, CID: testBlock(t, cid.Raw, fmt.Sprintf("bounded head document %d", i)).Cid()},
		)
	}
	maxSchedule = append(maxSchedule, Pointer{Kind: Document, CID: document.Cid()})
	if got := len(maxSchedule); got != MaxCoordinatorPointers {
		t.Fatalf("constructed maximum schedule has %d pointers, want %d", got, MaxCoordinatorPointers)
	}
	if err := coordinator.provider.updatePointers(maxSchedule); err != nil {
		t.Fatalf("flat provider schedule at hard ceiling %d was rejected: %v", MaxCoordinatorPointers, err)
	}
	if err := coordinator.UpdateHead("alpha", Set{}); err != nil {
		t.Fatalf("register empty alpha: %v", err)
	}
	if err := coordinator.UpdateHead("beta", Set{Root: root}); err == nil {
		t.Fatal("head above MaxHeads was accepted")
	}
	if err := coordinator.UpdateHead("alpha", Set{Root: root}); err != nil {
		t.Fatalf("updating an existing head at the limit: %v", err)
	}
	if err := coordinator.RemoveHead("alpha"); err != nil {
		t.Fatalf("RemoveHead alpha: %v", err)
	}
	if err := coordinator.UpdateHead("beta", Set{Root: root}); err != nil {
		t.Fatalf("reusing capacity after removal: %v", err)
	}
	for _, head := range []string{"", "-alpha", "Alpha", "alpha/beta", strings.Repeat("a", MaxCoordinatorHeadNameBytes+1)} {
		if err := coordinator.UpdateHead(head, Set{}); err == nil {
			t.Errorf("invalid head name %q was accepted", head)
		}
	}

	if err := coordinator.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := coordinator.UpdateHead("beta", Set{Root: root}); err == nil {
		t.Fatal("update after Close was accepted")
	}
	if err := coordinator.RemoveHead("beta"); err == nil {
		t.Fatal("removal after Close was accepted")
	}
	if err := coordinator.ReplaceAll(map[string]Set{"beta": {Root: root}}); err == nil {
		t.Fatal("whole-snapshot replacement after Close was accepted")
	}
	if err := coordinator.ReplaceAllWithDocuments(map[string]Set{"beta": {Root: root}}, []cid.Cid{document.Cid()}); err == nil {
		t.Fatal("whole-snapshot replacement with extra document after Close was accepted")
	}
	if err := coordinator.ValidateAllWithDocuments(map[string]Set{"beta": {Root: root}}, []cid.Cid{document.Cid()}); err == nil {
		t.Fatal("whole-snapshot preflight after Close was accepted")
	}
}

func TestCoordinatorConcurrentUpdatesStayWithinFixedHeadBound(t *testing.T) {
	serving, err := NewVerifiedDocumentStore(memoryBlocks(), 1)
	if err != nil {
		t.Fatalf("NewVerifiedDocumentStore: %v", err)
	}
	roots := []cid.Cid{
		put(t, serving, cid.DagCBOR, "concurrent aggregate root a"),
		put(t, serving, cid.DagCBOR, "concurrent aggregate root b"),
	}
	coordinator := newTestCoordinator(t, &recordingRouter{}, serving, 8, nil)

	errs := make(chan error, 8)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		head := fmt.Sprintf("head-%d", i)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for update := 0; update < 100; update++ {
				if err := coordinator.UpdateHead(head, Set{Root: roots[update%len(roots)]}); err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("concurrent UpdateHead: %v", err)
	}
	if len(coordinator.heads) != 8 {
		t.Fatalf("accepted head count = %d, want fixed bound 8", len(coordinator.heads))
	}
	items := providerPointerSnapshot(coordinator.provider)
	assertPointerSchedule(t, items, []Pointer{{Kind: Root, CID: roots[1]}})
}

func assertPointerSchedule(t *testing.T, got, want []Pointer) {
	t.Helper()
	got, err := normalizePointers(got)
	if err != nil {
		t.Fatalf("normalizing actual pointer schedule: %v", err)
	}
	want, err = normalizePointers(want)
	if err != nil {
		t.Fatalf("normalizing expected pointer schedule: %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("pointer schedule length = %d (%v), want %d (%v)", len(got), got, len(want), want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("pointer schedule[%d] = %+v, want %+v", i, got[i], want[i])
		}
	}
}

func providerPointerSnapshot(provider *Provider) []Pointer {
	scheduled, _ := provider.snapshot()
	result := make([]Pointer, len(scheduled))
	for i, item := range scheduled {
		result[i] = item.pointer
	}
	return result
}
