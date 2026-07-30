package unfinalized

import (
	"context"
	"encoding/binary"
	"errors"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/crypto/kzg4844"

	"github.com/blobarchive/bloar/index/archclient"
	"github.com/blobarchive/bloar/index/upstream"
	"github.com/blobarchive/bloar/ingest"
	"github.com/blobarchive/bloar/metrics"
	"github.com/blobarchive/bloar/schema"
	"github.com/blobarchive/bloar/server"
)

type fakeTrackerArchive struct {
	handoff archclient.HeadInfo
	state   server.GenerationStatus
	missing []schema.VersionedHash
	posts   []server.GenerationRequest
	puts    [][][]byte
}

type runTrackerArchive struct {
	mu         sync.Mutex
	handoffs   []archclient.HeadInfo
	headErrors []error
	headRead   int
	state      server.GenerationStatus
	missing    []schema.VersionedHash
	postErrors []error
	postCount  int
	putCount   int
	selected   chan struct{}
	selectOnce sync.Once
}

func (f *runTrackerArchive) Head(context.Context, string) (archclient.HeadInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.headErrors) > 0 {
		err := f.headErrors[0]
		f.headErrors = f.headErrors[1:]
		if err != nil {
			f.headRead++
			return archclient.HeadInfo{}, err
		}
	}
	i := f.headRead
	if i >= len(f.handoffs) {
		i = len(f.handoffs) - 1
	}
	f.headRead++
	return f.handoffs[i], nil
}

func (f *runTrackerArchive) GenerationState(context.Context, string) (server.GenerationStatus, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.state, nil
}

func (f *runTrackerArchive) PostGeneration(
	_ context.Context,
	_ string,
	req server.GenerationRequest,
) (server.GenerationResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.postCount++
	if f.postCount == 1 && len(f.missing) > 0 {
		current := f.state.Generation
		return server.GenerationResponse{}, &archclient.MissingBlobsError{
			ConflictError: &archclient.ConflictError{
				HTTPError:         &archclient.HTTPError{Method: http.MethodPost, Status: http.StatusConflict},
				CurrentGeneration: &current,
			},
			VHs: f.missing,
		}
	}
	if len(f.postErrors) > 0 {
		err := f.postErrors[0]
		f.postErrors = f.postErrors[1:]
		return server.GenerationResponse{}, err
	}
	handoff := f.handoffs[len(f.handoffs)-1]
	if i := f.headRead - 1; i >= 0 && i < len(f.handoffs) {
		handoff = f.handoffs[i]
	}
	f.state = server.GenerationStatus{
		GenerationState: server.GenerationState{
			V:                       2,
			Kind:                    server.UnfinalizedMutable,
			Generation:              req.ExpectedGeneration + 1,
			Root:                    "bafygeneration",
			WindowStart:             req.WindowStart,
			SyncedTo:                req.SyncedTo,
			SourceHeadRoot:          req.SourceHeadRoot,
			SourceHeadSlot:          req.SyncedTo,
			SourceFinalizedSlot:     req.SourceFinalizedSlot,
			SourceFinalizedRoot:     req.SourceFinalizedRoot,
			ObservedHandoffRoot:     req.ObservedHandoffRoot,
			ObservedHandoffSyncedTo: req.ObservedHandoffSyncedTo,
			HandoffHead:             "all",
			HandoffRoot:             handoff.Root,
			HandoffSyncedTo:         *handoff.SyncedTo,
		},
		Exposed:   true,
		Published: true,
	}
	if f.selected != nil {
		f.selectOnce.Do(func() { close(f.selected) })
	}
	return server.GenerationResponse{
		Generation:  req.ExpectedGeneration + 1,
		WindowStart: req.WindowStart,
		SyncedTo:    req.SyncedTo,
		Root:        "bafygeneration",
	}, nil
}

func (f *runTrackerArchive) PutBlobs(_ context.Context, blobs [][]byte) ([]archclient.PutBlob, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.putCount++
	out := make([]archclient.PutBlob, len(blobs))
	for i, blob := range blobs {
		vh, err := ingest.VersionedHash(blob)
		if err != nil {
			return nil, err
		}
		out[i] = archclient.PutBlob{VH: vh, CID: "bafyblob"}
	}
	return out, nil
}

func (f *runTrackerArchive) posts() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.postCount
}

func (f *runTrackerArchive) heads() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.headRead
}

func (f *runTrackerArchive) puts() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.putCount
}

func (f *runTrackerArchive) generation() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.state.Generation
}

type flakyHeaderSource struct {
	*fakeSource
	mu     sync.Mutex
	errors []error
}

func (f *flakyHeaderSource) HeaderByRoot(ctx context.Context, root [32]byte) (upstream.BeaconHeader, error) {
	f.mu.Lock()
	if len(f.errors) > 0 {
		err := f.errors[0]
		f.errors = f.errors[1:]
		f.mu.Unlock()
		return upstream.BeaconHeader{}, err
	}
	f.mu.Unlock()
	return f.fakeSource.HeaderByRoot(ctx, root)
}

func (f *fakeTrackerArchive) Head(context.Context, string) (archclient.HeadInfo, error) {
	return f.handoff, nil
}

func (f *fakeTrackerArchive) GenerationState(context.Context, string) (server.GenerationStatus, error) {
	return f.state, nil
}

func (f *fakeTrackerArchive) PostGeneration(_ context.Context, _ string, req server.GenerationRequest) (server.GenerationResponse, error) {
	f.posts = append(f.posts, req)
	if len(f.posts) == 1 && len(f.missing) > 0 {
		current := f.state.Generation
		return server.GenerationResponse{}, &archclient.MissingBlobsError{
			ConflictError: &archclient.ConflictError{
				HTTPError:         &archclient.HTTPError{Method: http.MethodPost, Status: http.StatusConflict},
				CurrentGeneration: &current,
			},
			VHs: f.missing,
		}
	}
	noop := f.state.Generation > 0 && req.ExpectedGeneration+1 == f.state.Generation
	root := "bafygeneration"
	if noop {
		root = f.state.Root
	}
	return server.GenerationResponse{Generation: req.ExpectedGeneration + 1, WindowStart: req.WindowStart,
		SyncedTo: req.SyncedTo, Root: root, NoOp: noop}, nil
}

func (f *fakeTrackerArchive) PutBlobs(_ context.Context, blobs [][]byte) ([]archclient.PutBlob, error) {
	f.puts = append(f.puts, blobs)
	out := make([]archclient.PutBlob, len(blobs))
	for i, blob := range blobs {
		vh, err := ingest.VersionedHash(blob)
		if err != nil {
			return nil, err
		}
		out[i] = archclient.PutBlob{VH: vh, CID: "bafyblob"}
	}
	return out, nil
}

type fakeBlobClient struct {
	result upstream.Result
	err    error
	reads  int
	asked  [][]schema.VersionedHash
}

func (f *fakeBlobClient) Blobs(_ context.Context, _ uint64, vhs []schema.VersionedHash) (upstream.Result, error) {
	f.reads++
	f.asked = append(f.asked, append([]schema.VersionedHash(nil), vhs...))
	return f.result, f.err
}

func trackerBlob(slot uint64) []byte {
	b := make([]byte, schema.BlobSize)
	binary.BigEndian.PutUint64(b[24:32], slot)
	return b
}

func trackerFixture(t *testing.T) (*fakeSource, []byte, schema.VersionedHash) {
	t.Helper()
	source := fixtureSource()
	blob := trackerBlob(15)
	commitment, err := kzg4844.BlobToCommitment((*kzg4844.Blob)(blob))
	if err != nil {
		t.Fatal(err)
	}
	source.commitments[root(13)] = nil
	source.commitments[root(15)] = [][48]byte{commitment}
	vh := ingest.VersionedHashFromCommitment(commitment)
	return source, blob, vh
}

func TestTrackerRunRecoversExecutionOptimisticInProcess(t *testing.T) {
	source := &flakyHeaderSource{
		fakeSource: fixtureSource(),
		errors: []error{
			&upstream.ExecutionOptimisticError{Path: "/eth/v1/beacon/headers/parent"},
			&upstream.ExecutionOptimisticError{Path: "/eth/v1/beacon/headers/parent"},
			&upstream.ExecutionOptimisticError{Path: "/eth/v1/beacon/headers/parent"},
		},
	}
	handoffSlot := uint64(10)
	archive := &runTrackerArchive{
		handoffs: []archclient.HeadInfo{{
			Name: "all", Root: "bafyall", OriginSlot: 10, SyncedTo: &handoffSlot,
		}},
		state: server.GenerationStatus{GenerationState: server.GenerationState{
			V: 1, Kind: server.UnfinalizedMutable,
		}},
		selected: make(chan struct{}),
	}
	tracker, err := New(Config{
		Headers: source, Archive: archive, Head: "tip", HandoffHead: "all",
		WindowSlots: 8, OverlapSlots: 1, Sources: []BlobSource{{Client: &fakeBlobClient{}}},
		PollInterval: time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	runTrackerUntilSelected(t, tracker, archive.selected)
	if got := archive.posts(); got != 1 {
		t.Fatalf("generation posts = %d, want 1 after in-process source recovery", got)
	}
}

func TestTrackerRunRecoversArchiveUnavailabilityInProcess(t *testing.T) {
	unavailable := &archclient.HTTPError{Method: http.MethodGet, Status: http.StatusServiceUnavailable}
	handoffSlot := uint64(10)

	t.Run("archive unavailable before the first selected generation", func(t *testing.T) {
		archive := &runTrackerArchive{
			handoffs: []archclient.HeadInfo{{
				Name: "all", Root: "bafyall", OriginSlot: 10, SyncedTo: &handoffSlot,
			}},
			headErrors: []error{unavailable},
			state: server.GenerationStatus{GenerationState: server.GenerationState{
				V: 1, Kind: server.UnfinalizedMutable,
			}},
			selected: make(chan struct{}),
		}
		mx := metrics.New()
		tracker, err := New(Config{
			Headers: fixtureSource(), Archive: archive, Head: "tip", HandoffHead: "all",
			WindowSlots: 8, OverlapSlots: 1, Sources: []BlobSource{{Client: &fakeBlobClient{}}},
			PollInterval: time.Millisecond, Metrics: mx,
		})
		if err != nil {
			t.Fatal(err)
		}
		runTrackerUntilSelected(t, tracker, archive.selected)
		if got := archive.heads(); got < 2 {
			t.Fatalf("archive head reads = %d, want unavailable attempt plus recovered selection", got)
		}
		if got := gatheredGauge(t, mx, "bloar_index_archive_available", "tip"); got != 1 {
			t.Fatalf("archive availability after startup recovery = %g, want 1", got)
		}
		if got := gatheredCounter(t, mx, "bloar_unfinalized_retries_total", "tip", metrics.UnfinalizedRetryArchiveUnavailable); got != 1 {
			t.Fatalf("archive-unavailable retries = %g, want 1", got)
		}
	})

	t.Run("selected generation survives a runtime outage", func(t *testing.T) {
		archive := &runTrackerArchive{
			handoffs: []archclient.HeadInfo{{
				Name: "all", Root: "bafyall", OriginSlot: 10, SyncedTo: &handoffSlot,
			}},
			// The first pass selects generation 1. The next pass fails before
			// reading generation state, and the third pass confirms the same
			// durable selection without posting a replacement.
			headErrors: []error{nil, unavailable},
			state: server.GenerationStatus{GenerationState: server.GenerationState{
				V: 1, Kind: server.UnfinalizedMutable,
			}},
			selected: make(chan struct{}),
		}
		mx := metrics.New()
		tracker, err := New(Config{
			Headers: fixtureSource(), Archive: archive, Head: "tip", HandoffHead: "all",
			WindowSlots: 8, OverlapSlots: 1, Sources: []BlobSource{{Client: &fakeBlobClient{}}},
			PollInterval: 50 * time.Millisecond, Metrics: mx,
		})
		if err != nil {
			t.Fatal(err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- tracker.Run(ctx) }()
		select {
		case <-archive.selected:
		case <-time.After(5 * time.Second):
			cancel()
			t.Fatal("tracker did not select its first generation")
		}
		waitForGauge(t, mx, "bloar_index_archive_available", "tip", 0)
		waitForGauge(t, mx, "bloar_index_archive_available", "tip", 1)
		if got := archive.generation(); got != 1 {
			t.Fatalf("selected generation after recovery = %d, want retained generation 1", got)
		}
		if got := archive.posts(); got != 1 {
			t.Fatalf("generation posts after recovery = %d, want the original selection only", got)
		}
		if got := gatheredCounter(t, mx, "bloar_unfinalized_retries_total", "tip", metrics.UnfinalizedRetryArchiveUnavailable); got != 1 {
			t.Fatalf("archive-unavailable retries = %g, want 1", got)
		}
		select {
		case err := <-done:
			t.Fatalf("tracker exited during or after archive recovery: %v", err)
		default:
		}
		cancel()
		if err := <-done; err != nil {
			t.Fatalf("tracker after cancellation = %v, want clean stop", err)
		}
	})
}

func TestTrackerRunLeavesAuthoritativeArchiveFailureTerminal(t *testing.T) {
	handoffSlot := uint64(10)
	unauthorized := &archclient.HTTPError{Method: http.MethodGet, Status: http.StatusUnauthorized}
	archive := &runTrackerArchive{
		handoffs: []archclient.HeadInfo{{
			Name: "all", Root: "bafyall", OriginSlot: 10, SyncedTo: &handoffSlot,
		}},
		headErrors: []error{unauthorized},
		state: server.GenerationStatus{GenerationState: server.GenerationState{
			V: 1, Kind: server.UnfinalizedMutable,
		}},
	}
	tracker, err := New(Config{
		Headers: fixtureSource(), Archive: archive, Head: "tip", HandoffHead: "all",
		WindowSlots: 8, OverlapSlots: 1, Sources: []BlobSource{{Client: &fakeBlobClient{}}},
		PollInterval: time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	err = tracker.Run(context.Background())
	if !errors.As(err, &unauthorized) {
		t.Fatalf("Run error = %T %v, want terminal authoritative 401", err, err)
	}
	if got := archive.heads(); got != 1 {
		t.Fatalf("archive head reads = %d, want no retry after authoritative 401", got)
	}
}

func TestTrackerRunRebuildsAfterHandoffChangesAtGenerationCAS(t *testing.T) {
	source := fixtureSource()
	source.finalized = source.headers[root(12)]
	source.finalized.Finalized = true
	oldSlot, currentSlot := uint64(10), uint64(12)
	generation := uint64(0)
	conflict := &archclient.ConflictError{
		HTTPError:         &archclient.HTTPError{Method: http.MethodPost, Status: http.StatusConflict},
		CurrentGeneration: &generation,
	}
	archive := &runTrackerArchive{
		handoffs: []archclient.HeadInfo{
			{Name: "all", Root: "bafyall-old", OriginSlot: 10, SyncedTo: &oldSlot},
			{Name: "all", Root: "bafyall-current", OriginSlot: 10, SyncedTo: &currentSlot},
		},
		state: server.GenerationStatus{GenerationState: server.GenerationState{
			V: 1, Kind: server.UnfinalizedMutable,
		}},
		postErrors: []error{conflict},
		selected:   make(chan struct{}),
	}
	tracker, err := New(Config{
		Headers: source, Archive: archive, Head: "tip", HandoffHead: "all",
		WindowSlots: 8, OverlapSlots: 1, Sources: []BlobSource{{Client: &fakeBlobClient{}}},
		PollInterval: time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	runTrackerUntilSelected(t, tracker, archive.selected)
	if got := archive.posts(); got != 2 {
		t.Fatalf("generation posts = %d, want stale refusal plus rebuilt selection", got)
	}
}

func TestTrackerClassifiesHandoffChangeAfterSupplyingMissingBlobs(t *testing.T) {
	source, blob, vh := trackerFixture(t)
	source.finalized = source.headers[root(12)]
	source.finalized.Finalized = true
	oldSlot, currentSlot := uint64(10), uint64(12)
	generation := uint64(0)
	conflict := &archclient.ConflictError{
		HTTPError:         &archclient.HTTPError{Method: http.MethodPost, Status: http.StatusConflict},
		CurrentGeneration: &generation,
	}
	archive := &runTrackerArchive{
		handoffs: []archclient.HeadInfo{
			{Name: "all", Root: "bafyall-old", OriginSlot: 10, SyncedTo: &oldSlot},
			{Name: "all", Root: "bafyall-current", OriginSlot: 10, SyncedTo: &currentSlot},
		},
		state: server.GenerationStatus{GenerationState: server.GenerationState{
			V: 1, Kind: server.UnfinalizedMutable,
		}},
		missing:    []schema.VersionedHash{vh},
		postErrors: []error{conflict},
	}
	bytes := &fakeBlobClient{result: upstream.Result{Status: upstream.StatusFound, Blobs: [][]byte{blob}}}
	tracker, err := New(Config{
		Headers: source, Archive: archive, Head: "tip", HandoffHead: "all",
		WindowSlots: 8, OverlapSlots: 1, Sources: []BlobSource{{Client: bytes}},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = tracker.Step(context.Background())
	if !errors.Is(err, ErrHandoffChanged) {
		t.Fatalf("Step error = %T %v, want ErrHandoffChanged", err, err)
	}
	if got := archive.posts(); got != 2 {
		t.Fatalf("generation posts = %d, want missing-blobs request plus stale exact retry", got)
	}
	if got := archive.puts(); got != 1 {
		t.Fatalf("blob puts = %d, want 1", got)
	}
}

func TestTrackerRunLeavesUnclassifiedFailuresTerminal(t *testing.T) {
	source := fixtureSource()
	source.headErr = errors.New("source invariant failed")
	handoffSlot := uint64(10)
	archive := &runTrackerArchive{
		handoffs: []archclient.HeadInfo{{
			Name: "all", Root: "bafyall", OriginSlot: 10, SyncedTo: &handoffSlot,
		}},
		state: server.GenerationStatus{GenerationState: server.GenerationState{
			V: 1, Kind: server.UnfinalizedMutable,
		}},
		selected: make(chan struct{}),
	}
	tracker, err := New(Config{
		Headers: source, Archive: archive, Head: "tip", HandoffHead: "all",
		WindowSlots: 8, OverlapSlots: 1, Sources: []BlobSource{{Client: &fakeBlobClient{}}},
		PollInterval: time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	err = tracker.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "source invariant failed") {
		t.Fatalf("Run error = %v, want terminal source failure", err)
	}
}

func TestTrackerRunLeavesUnchangedGenerationConflictTerminal(t *testing.T) {
	source := fixtureSource()
	handoffSlot := uint64(10)
	generation := uint64(0)
	conflict := &archclient.ConflictError{
		HTTPError:         &archclient.HTTPError{Method: http.MethodPost, Status: http.StatusConflict},
		CurrentGeneration: &generation,
	}
	archive := &runTrackerArchive{
		handoffs: []archclient.HeadInfo{{
			Name: "all", Root: "bafyall", OriginSlot: 10, SyncedTo: &handoffSlot,
		}},
		state: server.GenerationStatus{GenerationState: server.GenerationState{
			V: 1, Kind: server.UnfinalizedMutable,
		}},
		postErrors: []error{conflict},
	}
	tracker, err := New(Config{
		Headers: source, Archive: archive, Head: "tip", HandoffHead: "all",
		WindowSlots: 8, OverlapSlots: 1, Sources: []BlobSource{{Client: &fakeBlobClient{}}},
		PollInterval: time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	err = tracker.Run(context.Background())
	if !errors.As(err, &conflict) {
		t.Fatalf("Run error = %T %v, want generation conflict", err, err)
	}
	if errors.Is(err, ErrHandoffChanged) {
		t.Fatalf("unchanged handoff conflict was classified as retryable: %v", err)
	}
	if got := archive.posts(); got != 1 {
		t.Fatalf("generation posts = %d, want one terminal conflict", got)
	}
}

func TestTrackerRunCancelsTransientWait(t *testing.T) {
	source := &flakyHeaderSource{
		fakeSource: fixtureSource(),
		errors: []error{
			&upstream.ExecutionOptimisticError{Path: "/eth/v1/beacon/headers/parent"},
		},
	}
	handoffSlot := uint64(10)
	archive := &runTrackerArchive{
		handoffs: []archclient.HeadInfo{{
			Name: "all", Root: "bafyall", OriginSlot: 10, SyncedTo: &handoffSlot,
		}},
		state: server.GenerationStatus{GenerationState: server.GenerationState{
			V: 1, Kind: server.UnfinalizedMutable,
		}},
	}
	tracker, err := New(Config{
		Headers: source, Archive: archive, Head: "tip", HandoffHead: "all",
		WindowSlots: 8, OverlapSlots: 1, Sources: []BlobSource{{Client: &fakeBlobClient{}}},
		PollInterval: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- tracker.Run(ctx) }()
	deadline := time.Now().Add(5 * time.Second)
	for archive.heads() == 0 {
		if time.Now().After(deadline) {
			cancel()
			t.Fatal("tracker did not enter its transient retry wait")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run after cancellation = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("tracker did not cancel its transient retry wait")
	}
}

func runTrackerUntilSelected(t *testing.T, tracker *Tracker, selected <-chan struct{}) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- tracker.Run(ctx) }()
	select {
	case <-selected:
		cancel()
	case <-time.After(5 * time.Second):
		cancel()
		t.Fatal("tracker did not recover and select a generation")
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run after cancellation = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("tracker did not stop after cancellation")
	}
}

func waitForGauge(t *testing.T, mx *metrics.Metrics, family, head string, want float64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if got, ok := gatheredMetric(mx, family, map[string]string{"head": head}); ok && got == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s{head=%q} did not become %g", family, head, want)
		}
		time.Sleep(time.Millisecond)
	}
}

func gatheredGauge(t *testing.T, mx *metrics.Metrics, family, head string) float64 {
	t.Helper()
	got, ok := gatheredMetric(mx, family, map[string]string{"head": head})
	if !ok {
		t.Fatalf("metric %s{head=%q} is absent", family, head)
	}
	return got
}

func gatheredCounter(t *testing.T, mx *metrics.Metrics, family, head, reason string) float64 {
	t.Helper()
	got, ok := gatheredMetric(mx, family, map[string]string{"head": head, "reason": reason})
	if !ok {
		t.Fatalf("metric %s{head=%q,reason=%q} is absent", family, head, reason)
	}
	return got
}

func gatheredMetric(mx *metrics.Metrics, family string, labels map[string]string) (float64, bool) {
	families, err := mx.Registry().Gather()
	if err != nil {
		return 0, false
	}
	for _, candidate := range families {
		if candidate.GetName() != family {
			continue
		}
		for _, sample := range candidate.GetMetric() {
			gotLabels := make(map[string]string, len(sample.GetLabel()))
			for _, label := range sample.GetLabel() {
				gotLabels[label.GetName()] = label.GetValue()
			}
			if !reflect.DeepEqual(gotLabels, labels) {
				continue
			}
			if sample.Gauge != nil {
				return sample.GetGauge().GetValue(), true
			}
			if sample.Counter != nil {
				return sample.GetCounter().GetValue(), true
			}
		}
	}
	return 0, false
}

func TestTrackerSuppliesMissingBlobsAndRetriesExactGeneration(t *testing.T) {
	headers, blob, vh := trackerFixture(t)
	handoffSlot := uint64(10)
	archive := &fakeTrackerArchive{
		handoff: archclient.HeadInfo{Name: "all", Root: "bafyall", OriginSlot: 10, SyncedTo: &handoffSlot},
		state:   server.GenerationStatus{GenerationState: server.GenerationState{V: 1, Kind: server.UnfinalizedMutable}},
		missing: []schema.VersionedHash{vh},
	}
	bytes := &fakeBlobClient{result: upstream.Result{Status: upstream.StatusFound, Blobs: [][]byte{blob}}}
	tracker, err := New(Config{Headers: headers, Archive: archive, Head: "tip", HandoffHead: "all",
		WindowSlots: 8, OverlapSlots: 1, Sources: []BlobSource{{Client: bytes}}, MaxPutBlobs: 1})
	if err != nil {
		t.Fatal(err)
	}
	got, err := tracker.Step(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !got.Updated || got.Generation != 1 || got.WindowStart != 10 || got.SyncedTo != 15 {
		t.Fatalf("result = %+v", got)
	}
	if len(archive.posts) != 2 || !reflect.DeepEqual(archive.posts[0], archive.posts[1]) {
		t.Fatalf("posts are not an exact retry: %+v", archive.posts)
	}
	if len(archive.puts) != 1 || len(archive.puts[0]) != 1 || len(archive.puts[0][0]) != schema.BlobSize {
		t.Fatalf("put batches=%d first_batch=%d first_blob_bytes=%d", len(archive.puts), len(archive.puts[0]), len(archive.puts[0][0]))
	}
	if len(bytes.asked) != 1 || bytes.asked[0] != nil {
		t.Fatalf("whole-slot source asked with filter %v", bytes.asked)
	}
	req := archive.posts[0]
	if req.SourceHeadRoot != beaconRoot(root(15)) || req.SourceFinalizedRoot != beaconRoot(root(10)) ||
		req.SourceFinalizedSlot != 10 || len(req.Rows) != 1 || req.Rows[0].Slot != 15 ||
		req.ObservedHandoffRoot != "bafyall" || req.ObservedHandoffSyncedTo != 10 ||
		!reflect.DeepEqual(req.Rows[0].VersionedHashes, []string{archclient.VHHex(vh)}) {
		t.Fatalf("generation request = %+v", req)
	}
}

func TestTrackerSkipsAlreadySelectedSourceGeneration(t *testing.T) {
	headers, _, _ := trackerFixture(t)
	handoffSlot := uint64(10)
	archive := &fakeTrackerArchive{
		handoff: archclient.HeadInfo{Name: "all", Root: "bafyall", OriginSlot: 10, SyncedTo: &handoffSlot},
		state: server.GenerationStatus{GenerationState: server.GenerationState{
			V: 2, Kind: server.UnfinalizedMutable, Generation: 7, WindowStart: 10, SyncedTo: 15,
			SourceHeadRoot: beaconRoot(root(15)), SourceFinalizedSlot: 10, SourceFinalizedRoot: beaconRoot(root(10)),
			ObservedHandoffRoot: "bafyall", ObservedHandoffSyncedTo: 10,
			HandoffHead: "all", HandoffRoot: "bafyall", HandoffSyncedTo: 10,
		}, Exposed: true, Published: true},
	}
	bytes := &fakeBlobClient{}
	tracker, err := New(Config{Headers: headers, Archive: archive, Head: "tip", HandoffHead: "all",
		WindowSlots: 8, OverlapSlots: 1, Sources: []BlobSource{{Client: bytes}}})
	if err != nil {
		t.Fatal(err)
	}
	got, err := tracker.Step(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got.Updated || got.Generation != 7 {
		t.Fatalf("result = %+v", got)
	}
	if len(archive.posts) != 0 || bytes.reads != 0 {
		t.Fatalf("unchanged generation posted=%d source_reads=%d", len(archive.posts), bytes.reads)
	}
}

func TestTrackerCaughtUpFastPathSkipsAncestryRebuild(t *testing.T) {
	tracker, headers, archive := caughtUpTracker(t)

	// A full Build would need both maps. The caught-up path needs only the two
	// source anchors and the already-selected archive witnesses.
	headers.headers = nil
	headers.commitments = nil
	got, err := tracker.Step(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got.Updated || got.Generation != 7 || got.Root != "bafygeneration" {
		t.Fatalf("fast-path result = %+v", got)
	}
	if len(archive.posts) != 0 {
		t.Fatalf("caught-up generation was reposted: %+v", archive.posts)
	}
	if headers.commitmentReads != 0 {
		t.Fatalf("caught-up path rebuilt %d commitment rows", headers.commitmentReads)
	}
}

func TestTrackerCaughtUpFastPathFallsBackOnEveryWitnessChange(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*fakeSource, *fakeTrackerArchive)
	}{
		{"same-slot head reorg", func(source *fakeSource, _ *fakeTrackerArchive) {
			source.head.Root = root(99)
			source.commitments[root(99)] = nil
		}},
		{"finality advance", func(source *fakeSource, _ *fakeTrackerArchive) {
			source.finalized = source.headers[root(12)]
			source.finalized.Finalized = true
		}},
		{"handoff root advance", func(_ *fakeSource, archive *fakeTrackerArchive) {
			archive.handoff.Root = "bafyall-next"
		}},
		{"unpublished selection", func(_ *fakeSource, archive *fakeTrackerArchive) {
			archive.state.Published = false
		}},
		{"unexposed selection", func(_ *fakeSource, archive *fakeTrackerArchive) {
			archive.state.Exposed = false
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tracker, source, archive := caughtUpTracker(t)
			tc.mutate(source, archive)
			if _, err := tracker.Step(context.Background()); err != nil {
				t.Fatal(err)
			}
			if source.commitmentReads == 0 {
				t.Fatal("changed witness skipped the full ancestry build")
			}
			if len(archive.posts) != 1 {
				t.Fatalf("changed witness posts = %d, want 1", len(archive.posts))
			}
		})
	}
}

func TestTrackerCaughtUpFastPathDoesNotMaskSourceError(t *testing.T) {
	tracker, source, archive := caughtUpTracker(t)
	source.headErr = errors.New("head unavailable")
	if _, err := tracker.Step(context.Background()); err == nil || !strings.Contains(err.Error(), "head unavailable") {
		t.Fatalf("Step error = %v, want source error", err)
	}
	if len(archive.posts) != 0 {
		t.Fatalf("source failure posted a generation: %+v", archive.posts)
	}
}

func caughtUpTracker(t *testing.T) (*Tracker, *fakeSource, *fakeTrackerArchive) {
	t.Helper()
	headers, _, _ := trackerFixture(t)
	handoffSlot := uint64(10)
	archive := &fakeTrackerArchive{
		handoff: archclient.HeadInfo{Name: "all", Root: "bafyall", OriginSlot: 10, SyncedTo: &handoffSlot},
		state: server.GenerationStatus{GenerationState: server.GenerationState{
			V: 2, Kind: server.UnfinalizedMutable, Generation: 7, WindowStart: 10, SyncedTo: 15, Root: "bafygeneration",
			SourceHeadRoot: beaconRoot(root(15)), SourceHeadSlot: 15,
			SourceFinalizedSlot: 10, SourceFinalizedRoot: beaconRoot(root(10)),
			ObservedHandoffRoot: "bafyall", ObservedHandoffSyncedTo: 10,
			HandoffHead: "all", HandoffRoot: "bafyall", HandoffSyncedTo: 10,
		}, Exposed: true, Published: true},
	}
	tracker, err := New(Config{Headers: headers, Archive: archive, Head: "tip", HandoffHead: "all",
		WindowSlots: 8, OverlapSlots: 1, Sources: []BlobSource{{Client: &fakeBlobClient{}}}})
	if err != nil {
		t.Fatal(err)
	}
	tracker.previous = &Snapshot{WindowStart: 10, SyncedTo: 15, Head: headers.head, Finalized: headers.finalized}
	return tracker, headers, archive
}

func TestTrackerHealsSelectedButUnpublishedGeneration(t *testing.T) {
	headers, _, _ := trackerFixture(t)
	handoffSlot := uint64(10)
	archive := &fakeTrackerArchive{
		handoff: archclient.HeadInfo{Name: "all", Root: "bafyall", OriginSlot: 10, SyncedTo: &handoffSlot},
		state: server.GenerationStatus{GenerationState: server.GenerationState{
			V: 2, Kind: server.UnfinalizedMutable, Generation: 7, WindowStart: 10, SyncedTo: 15, Root: "bafygeneration",
			SourceHeadRoot: beaconRoot(root(15)), SourceFinalizedSlot: 10, SourceFinalizedRoot: beaconRoot(root(10)),
			ObservedHandoffRoot: "bafyall", ObservedHandoffSyncedTo: 10,
			HandoffHead: "all", HandoffRoot: "bafyall", HandoffSyncedTo: 10,
		}, Exposed: true, Published: false},
	}
	tracker, err := New(Config{Headers: headers, Archive: archive, Head: "tip", HandoffHead: "all",
		WindowSlots: 8, OverlapSlots: 1, Sources: []BlobSource{{Client: &fakeBlobClient{}}}})
	if err != nil {
		t.Fatal(err)
	}
	got, err := tracker.Step(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got.Updated || got.Generation != 7 {
		t.Fatalf("healed result = %+v", got)
	}
	if len(archive.posts) != 1 || archive.posts[0].ExpectedGeneration != 6 {
		t.Fatalf("recovery posts = %+v, want exact generation-6 retry", archive.posts)
	}
}

func TestTrackerUsesFallbackOnlyAfterAnchoringFailure(t *testing.T) {
	_, blob, vh := trackerFixture(t)
	bad := trackerBlob(99)
	primary := &fakeBlobClient{result: upstream.Result{Status: upstream.StatusFound, Blobs: [][]byte{bad}}}
	fallback := &fakeBlobClient{result: upstream.Result{Status: upstream.StatusFound, Blobs: [][]byte{blob}}}
	archive := &fakeTrackerArchive{state: server.GenerationStatus{GenerationState: server.GenerationState{Kind: server.UnfinalizedMutable}}, missing: []schema.VersionedHash{vh}}
	tracker, err := New(Config{Headers: fixtureSource(), Archive: archive, Head: "tip", HandoffHead: "all",
		WindowSlots: 8, Sources: []BlobSource{{Client: primary}, {Client: fallback}}})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := Snapshot{Rows: []archclient.Row{{Slot: 15, VHs: []schema.VersionedHash{vh}}},
		Locations: map[schema.VersionedHash]uint64{vh: 15}}
	if err := tracker.supplyMissing(context.Background(), snapshot, []schema.VersionedHash{vh}); err != nil {
		t.Fatal(err)
	}
	if primary.reads != 1 || fallback.reads != 1 || fallback.asked[0] != nil {
		t.Fatalf("primary reads=%d fallback reads=%d fallback request=%v", primary.reads, fallback.reads, fallback.asked)
	}
}

func TestTrackerRetainsGenerationWhenHandoffExceedsTrustedFinality(t *testing.T) {
	headers, _, _ := trackerFixture(t)
	handoffSlot := uint64(12)
	archive := &fakeTrackerArchive{
		handoff: archclient.HeadInfo{Name: "all", OriginSlot: 10, SyncedTo: &handoffSlot},
		state:   server.GenerationStatus{GenerationState: server.GenerationState{V: 1, Kind: server.UnfinalizedMutable}},
	}
	tracker, err := New(Config{Headers: headers, Archive: archive, Head: "tip", HandoffHead: "all",
		WindowSlots: 8, OverlapSlots: 3, Sources: []BlobSource{{Client: &fakeBlobClient{}}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tracker.Step(context.Background()); !errors.Is(err, ErrHandoffBlocked) {
		t.Fatalf("error = %v, want ErrHandoffBlocked", err)
	}
	if len(archive.posts) != 0 {
		t.Fatalf("posted unsafe generation: %+v", archive.posts)
	}
}

func TestTrackerSelectsWhenRetainedOverlapStartsBelowSourceFinality(t *testing.T) {
	headers := fixtureSource()
	headers.finalized = headers.headers[root(12)]
	headers.finalized.Finalized = true
	handoffSlot := uint64(12)
	archive := &fakeTrackerArchive{
		handoff: archclient.HeadInfo{Name: "all", Root: "bafyall", OriginSlot: 10, SyncedTo: &handoffSlot},
		state:   server.GenerationStatus{GenerationState: server.GenerationState{V: 1, Kind: server.UnfinalizedMutable}},
	}
	tracker, err := New(Config{Headers: headers, Archive: archive, Head: "tip", HandoffHead: "all",
		WindowSlots: 8, OverlapSlots: 3, Sources: []BlobSource{{Client: &fakeBlobClient{}}}})
	if err != nil {
		t.Fatal(err)
	}
	got, err := tracker.Step(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !got.Updated || got.WindowStart != 10 || got.SyncedTo != 15 {
		t.Fatalf("result = %+v", got)
	}
	if len(archive.posts) != 1 || archive.posts[0].SourceFinalizedSlot != 12 {
		t.Fatalf("generation posts = %+v", archive.posts)
	}
}

func TestObservedReorgUsesOnlyOverlappingCanonicalEvidence(t *testing.T) {
	snapshot := func(start uint64, roots ...byte) Snapshot {
		blocks := make([]CanonicalBlock, len(roots))
		for i, r := range roots {
			blocks[i] = CanonicalBlock{Slot: start + uint64(i), Root: root(r)}
		}
		last := blocks[len(blocks)-1]
		return Snapshot{WindowStart: start, SyncedTo: last.Slot,
			Head: upstream.BeaconHeader{Slot: last.Slot, Root: last.Root}, Blocks: blocks}
	}
	tests := []struct {
		name         string
		previous     Snapshot
		next         Snapshot
		wantDepth    uint64
		wantObserved bool
	}{
		{"ordinary extension", snapshot(10, 1, 2, 3), snapshot(10, 1, 2, 3, 4), 0, false},
		{"same tip", snapshot(10, 1, 2, 3), snapshot(10, 1, 2, 3), 0, false},
		{"two slot reorg", snapshot(10, 1, 2, 3), snapshot(10, 1, 8, 9, 10), 2, true},
		{"common ancestor below retained overlap", snapshot(10, 1, 2, 3), snapshot(11, 8, 9, 10), 2, true},
		{"fully advanced window is unknown", snapshot(10, 1, 2, 3), snapshot(13, 3, 4, 5), 0, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			depth, observed := observedReorg(tc.previous, tc.next)
			if depth != tc.wantDepth || observed != tc.wantObserved {
				t.Fatalf("observedReorg = (%d,%t), want (%d,%t)", depth, observed, tc.wantDepth, tc.wantObserved)
			}
		})
	}
}
