package pointerhint

import (
	"context"
	"errors"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"

	bmetrics "github.com/blobarchive/bloar/metrics"
)

type provideCall struct {
	cid     cid.Cid
	started time.Time
}

type recordingRouter struct {
	mu      sync.Mutex
	calls   []provideCall
	fail    bool
	entered chan cid.Cid
	release <-chan struct{}
}

func (r *recordingRouter) Provide(ctx context.Context, c cid.Cid, announce bool) error {
	r.mu.Lock()
	r.calls = append(r.calls, provideCall{cid: c, started: time.Now()})
	fail := r.fail
	r.mu.Unlock()
	if r.entered != nil {
		select {
		case r.entered <- c:
		default:
		}
	}
	if r.release != nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-r.release:
		}
	}
	if !announce {
		return errors.New("announce was false")
	}
	if fail {
		return errors.New("injected provide failure")
	}
	return nil
}

func (r *recordingRouter) setFail(fail bool) {
	r.mu.Lock()
	r.fail = fail
	r.mu.Unlock()
}

func (r *recordingRouter) FindProvidersAsync(context.Context, cid.Cid, int) <-chan peer.AddrInfo {
	ch := make(chan peer.AddrInfo)
	close(ch)
	return ch
}

func (r *recordingRouter) snapshot() []provideCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]provideCall(nil), r.calls...)
}

func newTestProvider(t *testing.T, router *recordingRouter, serving *VerifiedDocumentStore, configure func(*ProviderConfig)) *Provider {
	t.Helper()
	cfg := ProviderConfig{
		Router:            router,
		Serving:           serving,
		VerifiedDocuments: serving,
		ReprovideInterval: 20 * time.Millisecond,
		MinWriteInterval:  2 * time.Millisecond,
		RetryMin:          3 * time.Millisecond,
		RetryMax:          12 * time.Millisecond,
		AttemptTimeout:    100 * time.Millisecond,
	}
	if configure != nil {
		configure(&cfg)
	}
	provider, err := NewProvider(t.Context(), cfg)
	if err != nil {
		t.Fatalf("NewProvider: %v", err)
	}
	t.Cleanup(func() {
		if err := provider.Close(); err != nil {
			t.Errorf("Provider.Close: %v", err)
		}
	})
	return provider
}

func waitCalls(t *testing.T, router *recordingRouter, count int) []provideCall {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		calls := router.snapshot()
		if len(calls) >= count {
			return calls
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d Provide calls; got %v", count, router.snapshot())
	return nil
}

func put(t *testing.T, store *VerifiedDocumentStore, blockCodec uint64, value string) cid.Cid {
	t.Helper()
	block := testBlock(t, blockCodec, value)
	if err := store.Put(t.Context(), block); err != nil {
		t.Fatalf("Put(%s): %v", block.Cid(), err)
	}
	return block.Cid()
}

func TestProviderAdvertisesOnlyExactCurrentLocalPointersAtBoundedRate(t *testing.T) {
	serving, err := NewVerifiedDocumentStore(memoryBlocks(), 4)
	if err != nil {
		t.Fatalf("NewVerifiedDocumentStore: %v", err)
	}
	rootA := put(t, serving, cid.DagCBOR, "root A")
	manifest := put(t, serving, cid.DagCBOR, "manifest")
	_ = put(t, serving, cid.Raw, "an arbitrary leaf that must never be provided")
	documentBlock := testBlock(t, cid.Raw, "verified publication document")
	if err := serving.RetainAfterVerification(documentBlock); err != nil {
		t.Fatalf("RetainAfterVerification: %v", err)
	}

	router := &recordingRouter{}
	provider := newTestProvider(t, router, serving, func(cfg *ProviderConfig) {
		cfg.ReprovideInterval = time.Hour
	})
	if err := provider.Update(Set{Root: rootA, Manifest: manifest, Document: documentBlock.Cid()}); err != nil {
		t.Fatalf("Update: %v", err)
	}
	calls := waitCalls(t, router, 3)
	want := []cid.Cid{rootA, manifest, documentBlock.Cid()}
	for i, expected := range want {
		if !calls[i].cid.Equals(expected) {
			t.Errorf("Provide[%d] = %s, want %s", i, calls[i].cid, expected)
		}
		if i > 0 && calls[i].started.Sub(calls[i-1].started) < 2*time.Millisecond {
			t.Errorf("Provide starts were %s apart, below configured 2ms ceiling", calls[i].started.Sub(calls[i-1].started))
		}
	}

	rootB := put(t, serving, cid.DagCBOR, "root B")
	if err := provider.Update(Set{Root: rootB, Manifest: manifest, Document: documentBlock.Cid()}); err != nil {
		t.Fatalf("swap Update: %v", err)
	}
	deadline := time.Now().Add(250 * time.Millisecond)
	for time.Now().Before(deadline) {
		calls = router.snapshot()
		newCount := 0
		for _, call := range calls {
			if call.cid.Equals(rootB) {
				newCount++
			}
		}
		if newCount >= 1 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	oldCount, newCount := 0, 0
	for _, call := range router.snapshot() {
		switch {
		case call.cid.Equals(rootA):
			oldCount++
		case call.cid.Equals(rootB):
			newCount++
		}
	}
	if oldCount != 1 {
		t.Errorf("old root was provided %d times after replacement, want its initial call only", oldCount)
	}
	if newCount != 1 {
		t.Errorf("new root was provided %d times, want one immediate replacement provide", newCount)
	}
}

func TestProviderSuccessfullyReprovidesCurrentPointer(t *testing.T) {
	serving, err := NewVerifiedDocumentStore(memoryBlocks(), 1)
	if err != nil {
		t.Fatalf("NewVerifiedDocumentStore: %v", err)
	}
	root := put(t, serving, cid.DagCBOR, "reprovided root")
	router := &recordingRouter{}
	provider := newTestProvider(t, router, serving, func(cfg *ProviderConfig) {
		cfg.ReprovideInterval = 20 * time.Millisecond
		cfg.ReprovideJitter = time.Nanosecond
	})
	if err := provider.Update(Set{Root: root}); err != nil {
		t.Fatalf("Update: %v", err)
	}
	calls := waitCalls(t, router, 2)
	if !calls[0].cid.Equals(root) || !calls[1].cid.Equals(root) {
		t.Fatalf("reprovide CIDs = %s, %s; want %s twice", calls[0].cid, calls[1].cid, root)
	}
}

func TestProviderRequiresLocalPresenceAndExplicitDocumentVerification(t *testing.T) {
	serving, err := NewVerifiedDocumentStore(memoryBlocks(), 2)
	if err != nil {
		t.Fatalf("NewVerifiedDocumentStore: %v", err)
	}
	unverified := testBlock(t, cid.Raw, "present but unverified")
	if err := serving.Put(t.Context(), unverified); err != nil {
		t.Fatalf("Put: %v", err)
	}
	missingRoot := testBlock(t, cid.DagCBOR, "not local").Cid()
	router := &recordingRouter{}
	mx := bmetrics.New()
	provider := newTestProvider(t, router, serving, func(cfg *ProviderConfig) { cfg.Metrics = mx })
	if err := provider.Update(Set{Root: missingRoot, Document: unverified.Cid()}); err != nil {
		t.Fatalf("Update: %v", err)
	}
	time.Sleep(15 * time.Millisecond)
	if calls := router.snapshot(); len(calls) != 0 {
		t.Fatalf("ineligible pointers reached DHT: %v", calls)
	}
	for _, kind := range []string{bmetrics.PointerKindRoot, bmetrics.PointerKindDocument} {
		series := `bloar_pointer_retries_total{kind="` + kind + `",reason="ineligible"}`
		if got := pointerMetricSample(t, mx, series); got <= 0 {
			t.Errorf("%s = %g, want a positive locally-ineligible retry count", series, got)
		}
	}
	if err := serving.RetainAfterVerification(unverified); err != nil {
		t.Fatalf("RetainAfterVerification: %v", err)
	}
	calls := waitCalls(t, router, 1)
	if !calls[0].cid.Equals(unverified.Cid()) {
		t.Fatalf("first eligible Provide = %s, want verified document %s", calls[0].cid, unverified.Cid())
	}
}

func TestProviderCoalescesBurstAndDropsOldSchedule(t *testing.T) {
	serving, err := NewVerifiedDocumentStore(memoryBlocks(), 1)
	if err != nil {
		t.Fatalf("NewVerifiedDocumentStore: %v", err)
	}
	a := put(t, serving, cid.DagCBOR, "a")
	b := put(t, serving, cid.DagCBOR, "b")
	c := put(t, serving, cid.DagCBOR, "c")
	release := make(chan struct{})
	router := &recordingRouter{entered: make(chan cid.Cid, 1), release: release}
	provider := newTestProvider(t, router, serving, func(cfg *ProviderConfig) {
		cfg.ReprovideInterval = time.Hour
	})
	if err := provider.Update(Set{Root: a}); err != nil {
		t.Fatalf("Update A: %v", err)
	}
	select {
	case got := <-router.entered:
		if !got.Equals(a) {
			t.Fatalf("in-flight CID = %s, want A", got)
		}
	case <-time.After(time.Second):
		t.Fatal("A did not enter Provide")
	}
	if err := provider.Update(Set{Root: b}); err != nil {
		t.Fatalf("Update B: %v", err)
	}
	if err := provider.Update(Set{Root: c}); err != nil {
		t.Fatalf("Update C: %v", err)
	}
	close(release)
	calls := waitCalls(t, router, 2)
	if !calls[1].cid.Equals(c) {
		t.Fatalf("post-burst Provide = %s, want latest C %s", calls[1].cid, c)
	}
	for _, call := range calls {
		if call.cid.Equals(b) {
			t.Fatalf("coalesced intermediate B reached DHT: %v", calls)
		}
	}
}

func TestProviderUpdateDuringProvidePreservesRetainedPointerCadence(t *testing.T) {
	serving, err := NewVerifiedDocumentStore(memoryBlocks(), 1)
	if err != nil {
		t.Fatalf("NewVerifiedDocumentStore: %v", err)
	}
	root := put(t, serving, cid.DagCBOR, "retained root")
	manifest := put(t, serving, cid.DagCBOR, "new manifest")
	release := make(chan struct{})
	router := &recordingRouter{entered: make(chan cid.Cid, 1), release: release}
	provider := newTestProvider(t, router, serving, func(cfg *ProviderConfig) {
		cfg.ReprovideInterval = time.Hour
		cfg.MinWriteInterval = time.Millisecond
	})
	if err := provider.Update(Set{Root: root}); err != nil {
		t.Fatalf("Update root: %v", err)
	}
	select {
	case got := <-router.entered:
		if !got.Equals(root) {
			t.Fatalf("in-flight CID = %s, want root %s", got, root)
		}
	case <-time.After(time.Second):
		t.Fatal("root did not enter Provide")
	}
	if err := provider.Update(Set{Root: root, Manifest: manifest}); err != nil {
		t.Fatalf("Update retaining root: %v", err)
	}
	close(release)

	calls := waitCalls(t, router, 2)
	if !calls[1].cid.Equals(manifest) {
		t.Fatalf("post-update Provide = %s, want newly added manifest %s", calls[1].cid, manifest)
	}
	time.Sleep(20 * time.Millisecond)
	rootCalls := 0
	for _, call := range router.snapshot() {
		if call.cid.Equals(root) {
			rootCalls++
		}
	}
	if rootCalls != 1 {
		t.Fatalf("retained root was provided %d times, want no immediate duplicate after update", rootCalls)
	}
}

func TestProviderRetriesWithBoundedExponentialBackoff(t *testing.T) {
	serving, err := NewVerifiedDocumentStore(memoryBlocks(), 1)
	if err != nil {
		t.Fatalf("NewVerifiedDocumentStore: %v", err)
	}
	root := put(t, serving, cid.DagCBOR, "retry root")
	router := &recordingRouter{fail: true}
	provider := newTestProvider(t, router, serving, func(cfg *ProviderConfig) {
		cfg.ReprovideInterval = time.Hour
		cfg.MinWriteInterval = time.Millisecond
		cfg.RetryMin = 5 * time.Millisecond
		cfg.RetryMax = 10 * time.Millisecond
	})
	if err := provider.Update(Set{Root: root}); err != nil {
		t.Fatalf("Update: %v", err)
	}
	calls := waitCalls(t, router, 4)
	deltas := []time.Duration{
		calls[1].started.Sub(calls[0].started),
		calls[2].started.Sub(calls[1].started),
		calls[3].started.Sub(calls[2].started),
	}
	if deltas[0] < 4*time.Millisecond {
		t.Errorf("first retry = %s, want about 5ms", deltas[0])
	}
	for i, delta := range deltas[1:] {
		if delta < 9*time.Millisecond {
			t.Errorf("capped retry %d = %s, want about 10ms", i+2, delta)
		}
	}
}

func TestProviderMetricsResetFreshnessAcrossPointerReplacement(t *testing.T) {
	serving, err := NewVerifiedDocumentStore(memoryBlocks(), 1)
	if err != nil {
		t.Fatalf("NewVerifiedDocumentStore: %v", err)
	}
	rootA := put(t, serving, cid.DagCBOR, "metric root A")
	rootB := put(t, serving, cid.DagCBOR, "metric root B")
	mx := bmetrics.New()
	router := &recordingRouter{}
	provider := newTestProvider(t, router, serving, func(cfg *ProviderConfig) {
		cfg.Metrics = mx
		cfg.ReprovideInterval = time.Hour
		// Leave enough time to inspect the failed replacement before its
		// scheduled retry fires; the retry is then allowed to run and succeed.
		cfg.RetryMin = 500 * time.Millisecond
		cfg.RetryMax = 500 * time.Millisecond
	})
	if err := provider.Update(Set{Root: rootA}); err != nil {
		t.Fatalf("Update A: %v", err)
	}
	waitCalls(t, router, 1)
	if got := waitPointerMetric(t, mx, `bloar_pointer_provides_total{kind="root",outcome="ok"}`, 1); got != 1 {
		t.Fatalf("initial successful provides = %g, want 1", got)
	}
	if got := pointerMetricSample(t, mx, `bloar_pointer_provide_last_success_timestamp_seconds{kind="root"}`); got <= 0 {
		t.Fatalf("initial pointer freshness timestamp = %g, want positive", got)
	}

	router.setFail(true)
	if err := provider.Update(Set{Root: rootB}); err != nil {
		t.Fatalf("Update B: %v", err)
	}
	waitCalls(t, router, 2)
	waitPointerMetric(t, mx, `bloar_pointer_retries_total{kind="root",reason="provide_error"}`, 1)
	if got := pointerMetricSample(t, mx, `bloar_pointer_current{kind="root"}`); got != 1 {
		t.Errorf("replacement current gauge = %g, want 1", got)
	}
	if got := pointerMetricSample(t, mx, `bloar_pointer_provide_last_success_timestamp_seconds{kind="root"}`); got != 0 {
		t.Errorf("replacement inherited old pointer freshness %g, want reset to 0", got)
	}
	if got := pointerMetricSample(t, mx, `bloar_pointer_provides_total{kind="root",outcome="error"}`); got != 1 {
		t.Errorf("failed replacement provides = %g, want 1", got)
	}
	if got := pointerMetricSample(t, mx, `bloar_pointer_retries_total{kind="root",reason="provide_error"}`); got != 1 {
		t.Errorf("replacement provide retries = %g, want 1", got)
	}

	router.setFail(false)
	waitCalls(t, router, 3)
	if got := waitPointerMetric(t, mx, `bloar_pointer_provides_total{kind="root",outcome="ok"}`, 2); got != 2 {
		t.Errorf("successful provides after retry = %g, want 2", got)
	}
	if got := pointerMetricSample(t, mx, `bloar_pointer_provide_last_success_timestamp_seconds{kind="root"}`); got <= 0 {
		t.Errorf("replacement freshness timestamp = %g, want positive after retry", got)
	}
	if err := provider.Close(); err != nil {
		t.Fatalf("Provider.Close: %v", err)
	}
	if got := pointerMetricSample(t, mx, `bloar_pointer_current{kind="root"}`); got != 0 {
		t.Errorf("closed provider current gauge = %g, want 0", got)
	}
}

func TestProviderMinWriteIntervalCapsFastRetries(t *testing.T) {
	serving, err := NewVerifiedDocumentStore(memoryBlocks(), 1)
	if err != nil {
		t.Fatalf("NewVerifiedDocumentStore: %v", err)
	}
	root := put(t, serving, cid.DagCBOR, "write-capped retry root")
	router := &recordingRouter{fail: true}
	provider := newTestProvider(t, router, serving, func(cfg *ProviderConfig) {
		cfg.ReprovideInterval = time.Hour
		cfg.MinWriteInterval = 20 * time.Millisecond
		cfg.RetryMin = time.Millisecond
		cfg.RetryMax = time.Millisecond
	})
	if err := provider.Update(Set{Root: root}); err != nil {
		t.Fatalf("Update: %v", err)
	}
	calls := waitCalls(t, router, 3)
	for i := 1; i < len(calls); i++ {
		if delta := calls[i].started.Sub(calls[i-1].started); delta < 18*time.Millisecond {
			t.Fatalf("retry Provide starts were %s apart, below the configured 20ms ceiling", delta)
		}
	}
}

func TestProviderRejectsRootlessNonEmptySet(t *testing.T) {
	serving, err := NewVerifiedDocumentStore(memoryBlocks(), 1)
	if err != nil {
		t.Fatalf("NewVerifiedDocumentStore: %v", err)
	}
	manifest := put(t, serving, cid.DagCBOR, "manifest without root")
	provider := newTestProvider(t, &recordingRouter{}, serving, nil)
	if err := provider.Update(Set{Manifest: manifest}); err == nil {
		t.Fatal("manifest-only non-empty set was accepted")
	}
}

func TestProviderJitterIsBoundedAndConfigCannotEraseCadence(t *testing.T) {
	for i := 0; i < 1000; i++ {
		got := pointerJittered(time.Hour, 5*time.Minute)
		if got < 55*time.Minute || got > 65*time.Minute {
			t.Fatalf("jittered hour = %s, outside +/-5m", got)
		}
	}
	_, err := providerConfigSettings(ProviderConfig{
		ReprovideInterval: time.Minute,
		ReprovideJitter:   time.Minute,
	})
	if err == nil {
		t.Fatal("jitter equal to the reprovide interval was accepted")
	}
	const maxDuration = time.Duration(1<<63 - 1)
	for i := 0; i < 1000; i++ {
		got := pointerJittered(maxDuration-time.Second, 2*time.Second)
		if got <= 0 {
			t.Fatalf("near-MaxInt64 jitter wrapped to %s", got)
		}
	}
}

func TestProviderRejectsUpdateAfterClose(t *testing.T) {
	serving, err := NewVerifiedDocumentStore(memoryBlocks(), 1)
	if err != nil {
		t.Fatalf("NewVerifiedDocumentStore: %v", err)
	}
	provider := newTestProvider(t, &recordingRouter{}, serving, nil)
	if err := provider.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := provider.Update(Set{}); err == nil {
		t.Fatal("Update after Close succeeded")
	}
}

func TestProviderCloseCancelsInFlightProvide(t *testing.T) {
	serving, err := NewVerifiedDocumentStore(memoryBlocks(), 1)
	if err != nil {
		t.Fatalf("NewVerifiedDocumentStore: %v", err)
	}
	root := put(t, serving, cid.DagCBOR, "in-flight root")
	router := &recordingRouter{
		entered: make(chan cid.Cid, 1),
		release: make(chan struct{}), // never released; Close must cancel.
	}
	mx := bmetrics.New()
	provider, err := NewProvider(t.Context(), ProviderConfig{
		Router:            router,
		Serving:           serving,
		VerifiedDocuments: serving,
		ReprovideInterval: time.Hour,
		MinWriteInterval:  time.Millisecond,
		RetryMin:          time.Minute,
		RetryMax:          time.Minute,
		AttemptTimeout:    time.Hour,
		Metrics:           mx,
	})
	if err != nil {
		t.Fatalf("NewProvider: %v", err)
	}
	if err := provider.Update(Set{Root: root}); err != nil {
		t.Fatalf("Update: %v", err)
	}
	select {
	case <-router.entered:
	case <-time.After(time.Second):
		t.Fatal("Provide did not enter")
	}
	closed := make(chan error, 1)
	go func() { closed <- provider.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not cancel the in-flight Provide")
	}
	if got := pointerMetricSample(t, mx, `bloar_pointer_provides_total{kind="root",outcome="error"}`); got != 0 {
		t.Errorf("shutdown cancellation recorded %g provide errors, want 0", got)
	}
	if got := pointerMetricSample(t, mx, `bloar_pointer_retries_total{kind="root",reason="provide_error"}`); got != 0 {
		t.Errorf("shutdown cancellation scheduled %g retries, want 0", got)
	}
}

func pointerMetricSample(t *testing.T, mx *bmetrics.Metrics, series string) float64 {
	t.Helper()
	recorder := httptest.NewRecorder()
	bmetrics.Handler(mx, nil).ServeHTTP(recorder, httptest.NewRequest("GET", "/metrics", nil))
	for line := range strings.SplitSeq(recorder.Body.String(), "\n") {
		if !strings.HasPrefix(line, series+" ") {
			continue
		}
		value, err := strconv.ParseFloat(strings.TrimSpace(strings.TrimPrefix(line, series+" ")), 64)
		if err != nil {
			t.Fatalf("parsing metric sample %q: %v", line, err)
		}
		return value
	}
	t.Fatalf("metric series %s is absent", series)
	return 0
}

func waitPointerMetric(t *testing.T, mx *bmetrics.Metrics, series string, minimum float64) float64 {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if got := pointerMetricSample(t, mx, series); got >= minimum {
			return got
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s to reach %g", series, minimum)
	return 0
}
