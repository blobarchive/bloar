package follow_test

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ipfs/boxo/exchange"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"

	"github.com/blobarchive/bloar/archive"
	"github.com/blobarchive/bloar/follow"
	"github.com/blobarchive/bloar/metrics"
	"github.com/blobarchive/bloar/p2p"
	"github.com/blobarchive/bloar/server"
)

func waitAdmittedRevision(t *testing.T, ctx context.Context, admitted <-chan uint64) uint64 {
	t.Helper()
	select {
	case revision := <-admitted:
		if revision == 0 {
			t.Fatal("admitted document carried no revision")
		}
		return revision
	case <-ctx.Done():
		t.Fatalf("waiting for admitted revision: %v", ctx.Err())
		return 0
	}
}

func waitFetchedRoot(t *testing.T, ctx context.Context, f *follow.Follower, name string, want cid.Cid) {
	t.Helper()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		if got := follow.HeadFetched(f, name); got == want {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("waiting for head %q fetched root %s (last %s): %v",
				name, want, follow.HeadFetched(f, name), ctx.Err())
		case <-ticker.C:
		}
	}
}

func TestRunAfterResumeAdmitsCurrentGenerationWhilePriorSyncIsBlocked(t *testing.T) {
	for _, sourceSet := range []bool{false, true} {
		name := "singular"
		if sourceSet {
			name = "source-set"
		}
		t.Run(name, func(t *testing.T) {
			archiveID := sourceRuntimeArchiveID(t)
			w := newWriterForArchive(t, &archiveID)
			w.ingestSlot(100, 81_001)
			rootA := w.head.Root()

			admitted := make(chan uint64, 4)
			f := newFollower(t, w, func(cfg *follow.Config) {
				if sourceSet {
					sources := []follow.SourceConfig{{
						ID: "writer-a", URL: w.url, PubKey: w.pubkey(), AllowedHeads: []string{testHead},
					}}
					configureSourceRuntime(t, cfg, archiveID, sources)
					cfg.OnAdmittedSourceDocument = func(_ blocks.Block, doc server.Doc, _ []string) error {
						if doc.Revision != nil {
							admitted <- *doc.Revision
						}
						return nil
					}
					return
				}
				id := archiveID
				cfg.ExpectedArchiveID = &id
				cfg.OnAdmittedDocument = func(_ blocks.Block, doc server.Doc) error {
					if doc.Revision != nil {
						admitted <- *doc.Revision
					}
					return nil
				}
			})

			oldSyncEntered := make(chan struct{})
			releaseOldSync := make(chan struct{})
			var releaseOnce sync.Once
			var hookOnce sync.Once
			follow.SetBeforeSyncCommitHook(func() {
				hookOnce.Do(func() {
					close(oldSyncEntered)
					<-releaseOldSync
				})
			})

			ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
			ticks := make(chan time.Time)
			runDone := make(chan error, 1)
			go func() { runDone <- follow.RunAfterResumeTicks(f.f, ctx, ticks) }()
			t.Cleanup(func() {
				releaseOnce.Do(func() { close(releaseOldSync) })
				cancel()
				select {
				case err := <-runDone:
					if err != nil {
						t.Errorf("RunAfterResumeTicks: %v", err)
					}
				case <-time.After(5 * time.Second):
					t.Error("RunAfterResumeTicks did not join its sync worker")
				}
				follow.SetBeforeSyncCommitHook(nil)
			})

			revisionA := waitAdmittedRevision(t, ctx, admitted)
			select {
			case <-oldSyncEntered:
			case <-ctx.Done():
				t.Fatalf("old sync did not reach its completion boundary: %v", ctx.Err())
			}
			if got := follow.HeadAdopted(f.f, testHead); got != rootA {
				t.Fatalf("initial adopted root = %s, want %s", got, rootA)
			}
			if got := follow.HeadFetched(f.f, testHead); got == rootA {
				t.Fatalf("blocked initial sync already stamped root %s fetched", got)
			}

			w.ingestSlot(101, 81_002)
			rootB := w.head.Root()
			select {
			case ticks <- time.Now():
			case <-ctx.Done():
				t.Fatalf("triggering the next admission boundary: %v", ctx.Err())
			}
			revisionB := waitAdmittedRevision(t, ctx, admitted)
			if revisionB <= revisionA {
				t.Fatalf("admitted revision did not advance: %d -> %d", revisionA, revisionB)
			}
			if got := follow.HeadAdopted(f.f, testHead); got != rootB {
				t.Fatalf("new generation was not admitted while old sync remained blocked: got %s, want %s", got, rootB)
			}
			if got := follow.HeadFetched(f.f, testHead); got == rootB {
				t.Fatalf("new root %s was stamped fetched by the blocked old pass", rootB)
			}

			releaseOnce.Do(func() { close(releaseOldSync) })
			waitFetchedRoot(t, ctx, f.f, testHead, rootB)
		})
	}
}

func TestRunAfterResumeServesNewLiveGenerationWhilePriorSyncIsBlocked(t *testing.T) {
	w := newWriter(t)
	docs := newDocServer(t)
	blobsB, hashesB := w.ingestSlot(112, 81_101)
	blobB := blobCID(t, blobsB[0])

	filteredA := buildOverlayHeadWithRows(t, w, overlayFilteredHead, 96, 103, nil)
	mutableA := buildOverlayHeadWithRows(t, w, testHead, 104, 111, nil)
	witnessA := buildOverlayHeadWithRows(t, w, testHandoffHead, 96, 103, nil)
	filteredB := buildOverlayHeadWithRows(t, w, overlayFilteredHead, 96, 111, nil)
	mutableB := buildOverlayHeadWithRows(t, w, testHead, 112, 119,
		[]archive.RefRow{{Slot: 112, VHs: hashesB}})
	witnessB := buildOverlayHeadWithRows(t, w, testHandoffHead, 96, 111, nil)

	docs.set(sign(t, w.key, filteredOverlayDocument(t, w, filteredA, mutableA, witnessA, 1)))
	admitted := make(chan uint64, 4)
	f := newFollower(t, w, func(cfg *follow.Config) {
		configureFilteredOverlayFollower(cfg, docs, nil)
		cfg.Verify = follow.VerifyFull
		cfg.OnAdmittedDocument = func(_ blocks.Block, doc server.Doc) error {
			if doc.Revision != nil {
				admitted <- *doc.Revision
			}
			return nil
		}
	})
	if f.hasLocally(blobB) {
		t.Fatalf("new-generation blob %s was local before its publication", blobB)
	}

	oldSyncEntered := make(chan struct{})
	releaseOldSync := make(chan struct{})
	var releaseOnce sync.Once
	var hookOnce sync.Once
	follow.SetBeforeSyncCommitHook(func() {
		hookOnce.Do(func() {
			close(oldSyncEntered)
			<-releaseOldSync
		})
	})

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	ticks := make(chan time.Time)
	runDone := make(chan error, 1)
	go func() { runDone <- follow.RunAfterResumeTicks(f.f, ctx, ticks) }()
	var epochEnded atomic.Bool
	var endEpoch func()
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(releaseOldSync) })
		cancel()
		select {
		case err := <-runDone:
			if err != nil {
				t.Errorf("RunAfterResumeTicks: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("RunAfterResumeTicks did not join its live-view sync worker")
		}
		if endEpoch != nil && !epochEnded.Swap(true) {
			endEpoch()
		}
		follow.SetBeforeSyncCommitHook(nil)
	})

	if got := waitAdmittedRevision(t, ctx, admitted); got != 1 {
		t.Fatalf("initial admitted revision = %d, want 1", got)
	}
	select {
	case <-oldSyncEntered:
	case <-ctx.Done():
		t.Fatalf("old live generation did not reach sync completion boundary: %v", ctx.Err())
	}

	activeEpoch, err := f.store.Epochs().Begin()
	if err != nil {
		t.Fatalf("beginning admission-overlap collection epoch: %v", err)
	}
	endEpoch = func() { activeEpoch.End() }
	docs.set(sign(t, w.key, filteredOverlayDocument(t, w, filteredB, mutableB, witnessB, 2)))
	select {
	case ticks <- time.Now():
	case <-ctx.Done():
		t.Fatalf("triggering live generation admission: %v", ctx.Err())
	}
	if got := waitAdmittedRevision(t, ctx, admitted); got != 2 {
		t.Fatalf("new admitted revision = %d, want 2", got)
	}
	if got := follow.HeadAdopted(f.f, testHead); got != mutableB.Root() {
		t.Fatalf("mutable B adopted root = %s, want %s", got, mutableB.Root())
	}
	if got := follow.HeadAdopted(f.f, overlayFilteredHead); got != filteredB.Root() {
		t.Fatalf("filtered B adopted root = %s, want %s", got, filteredB.Root())
	}
	if !f.hasLocally(blobB) {
		t.Fatalf("active-epoch admission exposed mutable B without fetching its retained blob %s", blobB)
	}

	handler, err := server.New(server.Config{
		Heads: f.heads, Blocks: f.store.Blocks(), ReadOnly: true,
		LiveHeads: map[string]server.LiveHead{"live": {
			FinalizedHead: overlayFilteredHead, UnfinalizedHead: testHead, RequireVersionedHashes: true,
		}},
		Beacon: server.Beacon{
			GenesisTime: genesisTime, SecondsPerSlot: secondsPerSlot,
			GenesisValidatorsRoot: "0x00", GenesisForkVersion: "0x00000000",
		},
	})
	if err != nil {
		t.Fatalf("constructing live-view server: %v", err)
	}
	srv := httptest.NewServer(handler)
	defer srv.Close()
	url := fmt.Sprintf("%s/live/eth/v1/beacon/blobs/112?versioned_hashes=0x%x",
		srv.URL, hashesB[0][:])
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET newer exact-hash live blob: %v", err)
	}
	defer resp.Body.Close()
	var payload struct {
		Data []string `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		t.Fatalf("decoding newer exact-hash live response: %v", err)
	}
	if resp.StatusCode != http.StatusOK || len(payload.Data) != 1 {
		t.Fatalf("newer exact-hash live response = status:%d blobs:%d, want 200/1",
			resp.StatusCode, len(payload.Data))
	}
	if want := "0x" + hex.EncodeToString(blobsB[0]); payload.Data[0] != want {
		t.Fatalf("newer exact-hash live blob differs: got %d hex chars, want %d",
			len(payload.Data[0]), len(want))
	}

	releaseOnce.Do(func() { close(releaseOldSync) })
	waitFetchedRoot(t, ctx, f.f, overlayFilteredHead, filteredB.Root())
	waitFetchedRoot(t, ctx, f.f, testHead, mutableB.Root())
	if !epochEnded.Swap(true) {
		endEpoch()
	}
}

func logicalHeadDocument(t *testing.T, w *writer, archiveID server.ArchiveID,
	head *archive.Head, revision uint64,
) []byte {
	t.Helper()
	id := archiveID
	rev := revision
	return sign(t, w.key, server.Unsigned{
		V: server.LogicalArchiveDocVersion, Net: testNet, ArchiveID: &id,
		UpdatedAt:  time.Unix(int64(revision), 0).UTC().Format(time.RFC3339),
		Multiaddrs: w.host.AnnounceAddrs(), Heads: []server.HeadEntry{entry(head.Info())}, Revision: &rev,
	})
}

func TestRunAfterResumeCoalescesManyRevisionsDirectlyToLatest(t *testing.T) {
	archiveID := sourceRuntimeArchiveID(t)
	w := newWriterForArchive(t, &archiveID)
	w.ingestSlot(100, 81_201)
	headA := w.head
	w.ingestSlot(101, 81_202)
	headB := w.head
	docs := newDocServer(t)
	docs.set(logicalHeadDocument(t, w, archiveID, headA, 1))

	mx := metrics.New()
	admitted := make(chan uint64, 128)
	f := newFollower(t, w, func(cfg *follow.Config) {
		id := archiveID
		cfg.URL = docs.url
		cfg.ExpectedArchiveID = &id
		cfg.Metrics = mx
		cfg.OnAdmittedDocument = func(_ blocks.Block, doc server.Doc) error {
			if doc.Revision != nil {
				admitted <- *doc.Revision
			}
			return nil
		}
	})

	oldSyncEntered := make(chan struct{})
	releaseOldSync := make(chan struct{})
	var releaseOnce sync.Once
	var firstHook sync.Once
	var syncCommitCalls atomic.Uint64
	follow.SetBeforeSyncCommitHook(func() {
		syncCommitCalls.Add(1)
		firstHook.Do(func() {
			close(oldSyncEntered)
			<-releaseOldSync
		})
	})

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	ticks := make(chan time.Time)
	runDone := make(chan error, 1)
	go func() { runDone <- follow.RunAfterResumeTicks(f.f, ctx, ticks) }()
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(releaseOldSync) })
		cancel()
		select {
		case err := <-runDone:
			if err != nil {
				t.Errorf("RunAfterResumeTicks: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("RunAfterResumeTicks did not join after revision coalescing")
		}
		follow.SetBeforeSyncCommitHook(nil)
	})

	if got := waitAdmittedRevision(t, ctx, admitted); got != 1 {
		t.Fatalf("initial admitted revision = %d, want 1", got)
	}
	select {
	case <-oldSyncEntered:
	case <-ctx.Done():
		t.Fatalf("initial sync did not block: %v", ctx.Err())
	}

	const latestRevision = 101
	for revision := uint64(2); revision <= latestRevision; revision++ {
		head := headA
		if revision == latestRevision {
			head = headB
		}
		docs.set(logicalHeadDocument(t, w, archiveID, head, revision))
		select {
		case ticks <- time.Now():
		case <-ctx.Done():
			t.Fatalf("triggering revision %d: %v", revision, ctx.Err())
		}
		if got := waitAdmittedRevision(t, ctx, admitted); got != revision {
			t.Fatalf("admitted revision = %d, want %d", got, revision)
		}
	}
	if got := follow.HeadAdopted(f.f, testHead); got != headB.Root() {
		t.Fatalf("latest adopted root = %s, want %s", got, headB.Root())
	}
	if got := scrapeSeries(t, mx, `bloar_follow_sync_active`); got != 1 {
		t.Fatalf("active sync workers = %g, want exactly one", got)
	}
	if got := scrapeSeries(t, mx, `bloar_follow_sync_coalesced_total`); got < latestRevision-2 {
		t.Fatalf("coalesced sync wakeups = %g, want at least %d", got, latestRevision-2)
	}

	releaseOnce.Do(func() { close(releaseOldSync) })
	waitFetchedRoot(t, ctx, f.f, testHead, headB.Root())
	if got := syncCommitCalls.Load(); got != 2 {
		t.Fatalf("sync completion boundaries = %d, want old A plus latest B only", got)
	}
}

type signaledFailSessions struct {
	inner   p2p.SessionSource
	target  cid.Cid
	failing atomic.Bool
	once    sync.Once
	failed  chan struct{}
}

func (s *signaledFailSessions) NewSession(ctx context.Context) exchange.Fetcher {
	return &signaledFailFetcher{inner: s.inner.NewSession(ctx), owner: s}
}

type signaledFailFetcher struct {
	inner exchange.Fetcher
	owner *signaledFailSessions
}

func (f *signaledFailFetcher) GetBlock(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	if c == f.owner.target && f.owner.failing.Load() {
		f.owner.once.Do(func() { close(f.owner.failed) })
		return nil, fmt.Errorf("injected async sync failure for %s", c)
	}
	return f.inner.GetBlock(ctx, c)
}

func (f *signaledFailFetcher) GetBlocks(ctx context.Context, cids []cid.Cid) (<-chan blocks.Block, error) {
	return f.inner.GetBlocks(ctx, cids)
}

func TestRunAfterResumeRetriesSyncAfterWorkerFailure(t *testing.T) {
	archiveID := sourceRuntimeArchiveID(t)
	w := newWriterForArchive(t, &archiveID)
	blobs, _ := w.ingestSlot(100, 82_001)
	root := w.head.Root()
	target := blobCID(t, blobs[0])

	admitted := make(chan uint64, 1)
	var sessions *signaledFailSessions
	f := newFollower(t, w, func(cfg *follow.Config) {
		id := archiveID
		cfg.ExpectedArchiveID = &id
		sessions = &signaledFailSessions{
			inner: cfg.Sessions, target: target, failed: make(chan struct{}),
		}
		sessions.failing.Store(true)
		cfg.Sessions = sessions
		cfg.OnAdmittedDocument = func(_ blocks.Block, doc server.Doc) error {
			if doc.Revision != nil {
				admitted <- *doc.Revision
			}
			return nil
		}
	})

	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	ticks := make(chan time.Time)
	runDone := make(chan error, 1)
	go func() { runDone <- follow.RunAfterResumeTicks(f.f, ctx, ticks) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-runDone:
			if err != nil {
				t.Errorf("RunAfterResumeTicks: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("RunAfterResumeTicks did not join after a sync failure")
		}
	})

	_ = waitAdmittedRevision(t, ctx, admitted)
	select {
	case <-sessions.failed:
	case <-ctx.Done():
		t.Fatalf("background sync did not reach the injected failure: %v", ctx.Err())
	}
	if got := follow.HeadAdopted(f.f, testHead); got != root {
		t.Fatalf("sync failure disturbed admitted last-good root: got %s, want %s", got, root)
	}
	if got := follow.HeadFetched(f.f, testHead); got.Defined() {
		t.Fatalf("failed sync stamped fetched root %s", got)
	}

	sessions.failing.Store(false)
	select {
	case ticks <- time.Now():
	case <-ctx.Done():
		t.Fatalf("triggering sync retry admission: %v", ctx.Err())
	}
	waitFetchedRoot(t, ctx, f.f, testHead, root)
}

type cancelBlockSessions struct {
	inner    p2p.SessionSource
	target   cid.Cid
	once     sync.Once
	exitOnce sync.Once
	entered  chan struct{}
	exited   chan struct{}
}

func (s *cancelBlockSessions) NewSession(ctx context.Context) exchange.Fetcher {
	return &cancelBlockFetcher{inner: s.inner.NewSession(ctx), owner: s}
}

type cancelBlockFetcher struct {
	inner exchange.Fetcher
	owner *cancelBlockSessions
}

func (f *cancelBlockFetcher) GetBlock(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	if c != f.owner.target {
		return f.inner.GetBlock(ctx, c)
	}
	f.owner.once.Do(func() { close(f.owner.entered) })
	<-ctx.Done()
	f.owner.exitOnce.Do(func() { close(f.owner.exited) })
	return nil, ctx.Err()
}

func (f *cancelBlockFetcher) GetBlocks(ctx context.Context, cids []cid.Cid) (<-chan blocks.Block, error) {
	return f.inner.GetBlocks(ctx, cids)
}

func TestRunAfterResumeCancellationJoinsSyncWorker(t *testing.T) {
	archiveID := sourceRuntimeArchiveID(t)
	w := newWriterForArchive(t, &archiveID)
	blobs, _ := w.ingestSlot(100, 82_101)
	target := blobCID(t, blobs[0])

	admitted := make(chan uint64, 1)
	var sessions *cancelBlockSessions
	mx := metrics.New()
	f := newFollower(t, w, func(cfg *follow.Config) {
		id := archiveID
		cfg.ExpectedArchiveID = &id
		cfg.Metrics = mx
		sessions = &cancelBlockSessions{
			inner: cfg.Sessions, target: target,
			entered: make(chan struct{}), exited: make(chan struct{}),
		}
		cfg.Sessions = sessions
		cfg.OnAdmittedDocument = func(_ blocks.Block, doc server.Doc) error {
			if doc.Revision != nil {
				admitted <- *doc.Revision
			}
			return nil
		}
	})

	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	ticks := make(chan time.Time)
	runDone := make(chan error, 1)
	go func() { runDone <- follow.RunAfterResumeTicks(f.f, ctx, ticks) }()
	_ = waitAdmittedRevision(t, t.Context(), admitted)
	select {
	case <-sessions.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("sync worker did not enter the cancellable fetch")
	}
	cancel()
	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("RunAfterResumeTicks after cancellation: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("RunAfterResumeTicks returned without joining deadline")
	}
	select {
	case <-sessions.exited:
	default:
		t.Fatal("RunAfterResumeTicks returned before the blocked sync worker exited")
	}
	if got := scrapeSeries(t, mx, `bloar_follow_sync_active`); got != 0 {
		t.Fatalf("sync active after joined cancellation = %g, want 0", got)
	}
}
