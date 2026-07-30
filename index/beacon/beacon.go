// Package beacon implements the beacon indexer of spec 10.1: the process that
// fills the ALL head by walking slots and posting what each one holds to an
// archive. It runs in one of two modes, logged loudly at startup, that differ in
// where they take their truth about what a slot contains.
//
// # Anchored mode (a beacon-shaped upstream)
//
// The trust boundary is inverted from a naive blob-copier's. Existence and
// absence come from a TRUSTED block feed (a beacon node's block API, spec 10.3),
// never from a blob endpoint:
//
//	F = finalized slot (block feed)
//	per slot in [s, min(F, s+B-1)]:
//	    header 404            -> a candidate missed slot; proven by continuity
//	    header 200, 0 commits -> a verifiably blobless slot; no row, coverage advances
//	    header 200, N commits -> the slot MUST carry these N blobs, whose vhs the
//	                             commitments fix; fetch them from the blob sources
//
// Blob sources are ordered, untrusted byte providers. For a slot the block says
// has N blobs, each source is asked in turn for exactly those vhs, and its answer
// is accepted only if it returns N blobs that each commit to the expected vh (the
// KZG anchoring, the one commitment pass per blob spec 10.1 asks for, done here
// rather than after the put). A source that 404s, returns the wrong count, serves
// bytes that do not commit, or fails is skipped for the next; all sources
// exhausted fails the batch. Absence is NEVER recorded from a blob source -- a
// source saying "I do not have it" is a fact about that source, not the slot.
//
// A block feed 404 is the one signal that could still be wrong: a node still
// backfilling historical blocks 404s a header it will later have,
// indistinguishable from a genuine miss on its own. The walk proves which by
// parent-root continuity -- every present slot's parent_root MUST equal the root
// of the most recent present slot before it -- so a 404 is confirmed a real
// missed slot only when the chain skips cleanly over it. A present slot whose
// parent_root does not match is a hidden or absent block, and a fatal error,
// never absence. The anchor is carried across batches and seeded on restart by
// walking headers back from the resume point to the last present slot.
//
// # Mirror mode (deterministic replication from a bloar archive upstream)
//
// The re-derivation of spec 11.5, and it is DETERMINISTIC REPLICATION, not an
// independent honesty check. The same loop is pointed at another bloar archive,
// but with no block feed it has no independent authority on what a slot must
// contain, so it COPIES the source's coverage decisions: the one upstream's 200
// (empty, or with blobs) is recorded as given, its 503 stops the batch and waits,
// and -- after a startup check that its origin_slot is at or below this head's --
// its 404 is a protocol violation rather than absence, because nothing at or above
// the validated origin can legitimately 404. KZG still anchors every INCLUDED
// blob to its versioned hash, so the source cannot forge bytes; but a covered-empty
// 200 over a slot the source silently omitted a real blob from is reproduced, not
// caught. Completeness is therefore INHERITED from the source, not re-derived: a
// re-derived root equal to the source's proves this node faithfully reproduced the
// source's decisions (spec 11.5), never that those decisions were complete against
// the chain. An independent completeness check must run ANCHORED mode against a
// trusted block feed. Finality is the archive's synced_to.
package beacon

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/blobarchive/bloar/index/archclient"
	"github.com/blobarchive/bloar/index/upstream"
	"github.com/blobarchive/bloar/ingest"
	"github.com/blobarchive/bloar/metrics"
	"github.com/blobarchive/bloar/schema"
)

// Source is one whole-slot blob byte source.
type Source struct {
	// Client reaches the source's blobs endpoint.
	Client *upstream.Client
}

// Config is what an Indexer needs.
type Config struct {
	// Sources are the blob byte sources, in order: primary first, then an
	// optional fallback. Anchored mode treats them as untrusted providers and
	// tries each in turn until one is anchored; mirror mode has exactly one, the
	// trusted archive. Sources are asked for the whole slot so canonical block
	// order is preserved. Required, 1..2 entries.
	Sources []Source
	// Blocks is the trusted block feed of anchored mode: the sole authority on
	// what a slot contains (spec 10.1). Nil selects mirror mode, in which the
	// single source's own answers are the authority.
	Blocks *upstream.BlockClient
	// ContinuityCheckpoint is an optional trusted (slot, root) floor for anchored
	// mode's continuity seed walk. Nil is the default: a
	// seed walk that reaches slot 0 without a present header then has no anchor and
	// waits (see seedContinuity). A checkpoint gives the walk a trusted stopping
	// point below the resume slot -- the walk back stops there and anchors to the
	// configured root -- so a young network whose origin is within the bounded walk
	// of zero can be seeded without waiting for the feed to backfill history.
	// Anchored mode only; its slot must be strictly before the first slot the run
	// covers. See ContinuityCheckpoint.
	ContinuityCheckpoint *ContinuityCheckpoint
	// Archive is the bloar archive being written. Required.
	Archive *archclient.Client
	// Head is the head to write, conventionally "all". Required.
	Head string
	// BatchSize is B in spec 10.1's loop: how many slots one refs batch covers.
	// Zero takes the default of 64.
	BatchSize uint64
	// MaxPutBlobs bounds one POST /bloar/v1/blobs, and MUST NOT exceed the
	// durable archive.max_put_blobs expectation (spec 7.2, default 64) or every
	// full put can be a 400. The command boundary validates and cross-checks that
	// expectation before constructing the indexer. Zero takes the same default
	// of 64.
	//
	// A batch of slots routinely carries more blobs than one put allows -- 64
	// slots at the mid-2026 maximum of 21 blobs each is 1344 -- so a batch's
	// bytes go up in as many puts as it takes, and the refs that reference them
	// go up once, afterwards.
	MaxPutBlobs int
	// FetchConcurrency bounds how many of a batch's slots are resolved from the
	// upstream at once. The slots of a batch are independent -- each is its own
	// header/commitments/source resolution -- and reading several at once is the
	// batch's throughput.
	//
	// It never affects what a batch contains: the answers are reassembled in slot
	// order before anything is put or posted, so fetch order cannot reach the DAG
	// (spec 5.1's batch order and spec 11.5's re-derivation are on slot order).
	// Zero takes the default of 6; one reads serially, one slot at a time, which
	// is the reference the concurrent path is held identical to.
	FetchConcurrency int
	// PollInterval is how long Run sleeps when it is caught up. Zero takes 12
	// seconds, one slot. Tests set it to something small.
	PollInterval time.Duration
	// Metrics counts per-source anchored fetches and times block-feed reads (spec
	// 10.1). Optional; nil records nothing.
	Metrics *metrics.Metrics
	// Logger receives progress. Optional.
	Logger *slog.Logger
}

// ContinuityCheckpoint is a trusted (slot, root) pair anchored mode's continuity
// seed walk stops at. It is the operator's escape hatch
// for a network whose origin sits within the bounded seed walk of slot 0, where a
// walk that reaches zero without a present header would otherwise have no anchor
// and wait indefinitely.
//
// When the seed walk reaches Slot:
//
//   - the block feed 404s it     -> the configured Root is the trusted anchor: the
//     feed has not backfilled this far, but the operator asserts this root, and the
//     walk anchors to it rather than waiting.
//   - the feed's header matches   -> the same anchor, now corroborated by the feed.
//   - the feed's header MISMATCHES -> a fatal configuration error: the feed and the
//     operator disagree about history at the checkpoint, and nothing may advance on
//     a disagreement about the very anchor everything chains to.
//
// Slot must be strictly before the first slot the run covers, so the checkpoint
// anchors the walk without ever itself advancing coverage. A later origin_slot is
// the documented alternative to configuring one.
type ContinuityCheckpoint struct {
	Slot uint64
	Root [32]byte
}

// Defaults (spec 10.1, 7.2, 12).
const (
	defaultBatchSize        = 64
	defaultMaxPutBlobs      = 64
	defaultFetchConcurrency = 6
	defaultPollInterval     = 12 * time.Second

	// seedWalkBound caps how far back the continuity seed walks looking for a
	// present slot before giving up. Mainnet gaps are 1-3 slots, so a run this
	// long with no block means the feed is still backfilling history, which is a
	// hard error rather than something to walk through (spec 10.1).
	seedWalkBound = 1024
)

// Indexer is the beacon indexer of spec 10.1.
//
// It holds no coverage state of its own: spec 10 makes both indexers stateless,
// and every pass re-reads synced_to from the archive. A restart therefore
// resumes exactly where the archive is. The one thing it keeps in memory is
// anchored mode's continuity anchor, which is re-seeded from the archive's
// synced_to on restart, so it is derived state, not authority.
type Indexer struct {
	cfg      Config
	log      *slog.Logger
	anchored bool

	// origin is the head's origin_slot, read once: it is immutable for the life
	// of a head (spec 3.1).
	origin uint64
	// gotOrigin guards the one-time startup read (origin, and mirror mode's
	// origin validation), which cannot happen in New because New has no context
	// and the archive may not be up yet.
	gotOrigin bool

	// lastRoot is anchored mode's continuity anchor: the root of the most recent
	// present slot at or below the archive's coverage. haveLastRoot is false when
	// the cached state is anchorless (the genesis wait); seeded records that a seed
	// decision is cached. Updated only at post time, so a discarded prefetch never
	// corrupts it.
	//
	// expectedResume is the archive resume slot the cached anchor is FOR: the first
	// slot the batch it anchors would cover. It is recorded at seed time (the
	// observed start) and re-recorded by an exact successful post (last+1), and the
	// cached state is reusable only while the freshly observed resume slot still
	// equals it. Any mismatch -- forward OR backward -- means the shared archive
	// moved under this indexer (a duplicate writer, spec 11.1's one-writer rule
	// broken, or a rewind), so the cached anchor no longer follows the slot it
	// represents: it is invalidated and the walk reseeds from the new start-1,
	// rather than reused to advance a batch whose leading slots it never validated
	//. Binding it to expectedResume rather than to
	// "has a root" is what closes the held-real-root bypass: even a genuine anchor
	// is trusted only for the exact resume it was computed for.
	lastRoot       [32]byte
	haveLastRoot   bool
	seeded         bool
	expectedResume uint64
}

// New returns an Indexer over cfg.
func New(cfg Config) (*Indexer, error) {
	switch {
	case len(cfg.Sources) == 0:
		return nil, errors.New("beacon: Config.Sources needs at least one source")
	case cfg.Archive == nil:
		return nil, errors.New("beacon: Config.Archive is required")
	case cfg.Head == "":
		return nil, errors.New("beacon: Config.Head is required")
	}
	anchored := cfg.Blocks != nil
	if anchored {
		if len(cfg.Sources) > 2 {
			return nil, fmt.Errorf("beacon: anchored mode takes 1 or 2 sources, got %d", len(cfg.Sources))
		}
	} else {
		if len(cfg.Sources) != 1 {
			return nil, fmt.Errorf("beacon: mirror mode (no block feed) takes exactly one source, got %d", len(cfg.Sources))
		}
		if cfg.ContinuityCheckpoint != nil {
			// The checkpoint anchors the block-feed continuity walk; mirror mode has no
			// block feed and no walk, so a checkpoint here is meaningless and a
			// misconfiguration rather than a no-op. Rejected at
			// the package boundary, independent of the cmd config validation.
			return nil, errors.New("beacon: Config.ContinuityCheckpoint is set but Blocks is nil (mirror mode); the " +
				"checkpoint anchors anchored mode's block-feed continuity walk, and mirror mode runs no such walk")
		}
	}

	if cfg.BatchSize == 0 {
		cfg.BatchSize = defaultBatchSize
	}
	if cfg.MaxPutBlobs == 0 {
		cfg.MaxPutBlobs = defaultMaxPutBlobs
	}
	if cfg.MaxPutBlobs < 0 {
		return nil, fmt.Errorf("beacon: Config.MaxPutBlobs is %d, must be positive", cfg.MaxPutBlobs)
	}
	if cfg.FetchConcurrency == 0 {
		cfg.FetchConcurrency = defaultFetchConcurrency
	}
	if cfg.FetchConcurrency < 0 {
		return nil, fmt.Errorf("beacon: Config.FetchConcurrency is %d, must be positive", cfg.FetchConcurrency)
	}
	if cfg.PollInterval == 0 {
		cfg.PollInterval = defaultPollInterval
	}
	// Strictly positive after defaulting: Run's caught-up wait is
	// time.After(PollInterval), which a non-positive value turns into an immediate
	// loop that hammers the finalized-head read with no delay. A zero is the
	// documented default just applied, so a value at or below zero here is a caller
	// mistake -- caught at construction rather than as a runtime spin.
	if cfg.PollInterval <= 0 {
		return nil, fmt.Errorf("beacon: Config.PollInterval is %s, must be positive", cfg.PollInterval)
	}

	ix := &Indexer{cfg: cfg, log: cfg.Logger, anchored: anchored}
	if ix.log == nil {
		ix.log = slog.New(slog.DiscardHandler)
	}
	return ix, nil
}

// Run drives the loop until ctx is cancelled, which it reports as nil: a
// cancelled indexer stopped because it was asked to.
func (ix *Indexer) Run(ctx context.Context) error {
	ix.log.Info("beacon indexer starting",
		// The mode is the one thing an operator most needs to see: it is the
		// difference between a walk whose truth comes from a trusted block feed
		// and one that trusts a validated bloar archive.
		"mode", ix.modeName(),
		"head", ix.cfg.Head, "batch_size", ix.cfg.BatchSize,
		"sources", len(ix.cfg.Sources),
		"fetch_concurrency", ix.cfg.FetchConcurrency, "poll_interval", ix.cfg.PollInterval)

	// One line per blob source names its fallback position.
	for i := range ix.cfg.Sources {
		label := metrics.SourcePrimary
		if i > 0 {
			label = metrics.SourceFallback
		}
		ix.log.Info("blob source", "position", label, "shape", "whole-slot")
	}

	for {
		var err error
		if ix.cfg.FetchConcurrency <= 1 {
			err = ix.runSerial(ctx)
		} else {
			err = ix.runPipelined(ctx)
		}
		if err == nil || ctx.Err() != nil {
			return nil
		}

		var optimistic *upstream.ExecutionOptimisticError
		if archclient.IsUnavailable(err) {
			// The command boundary owns the slower process-level retry: it
			// reconstructs this stateless indexer and re-reads durable coverage.
			// Do not count that classified dependency outage as a fatal outcome.
			return err
		}
		if !ix.anchored || !errors.As(err, &optimistic) {
			ix.cfg.Metrics.IndexOutcome(ix.cfg.Head, metrics.IndexOutcomeFatal)
			return err
		}

		// The request client has already exhausted its own bounded attempt
		// budget. Preserve the archive's durable coverage and the in-memory
		// continuity anchor (which post updates only after a successful refs
		// commit), then retry the whole planning/fetch loop after one bounded,
		// cancelable poll interval. Re-entering runPipelined also guarantees any
		// failed lookahead is gone and planning starts from fresh archive
		// coverage; no optimistic slot is posted.
		ix.cfg.Metrics.IndexRetry(ix.cfg.Head, metrics.IndexRetryExecutionOptimistic)
		ix.cfg.Metrics.IndexOutcome(ix.cfg.Head, metrics.IndexOutcomeRetry)
		ix.log.Warn("finalized block read remains optimistic; retaining durable head and retrying",
			"head", ix.cfg.Head,
			"reason", metrics.IndexRetryExecutionOptimistic,
			"backoff", ix.cfg.PollInterval,
			"path", optimistic.Path,
			"error", err)

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(ix.cfg.PollInterval):
		}
	}
}

// modeName is the loud startup label of the mode Run is in.
func (ix *Indexer) modeName() string {
	if ix.anchored {
		return "anchored (trusted block feed; blob sources are untrusted bytes)"
	}
	return "mirror (deterministic replication of a source archive; spec 11.5; completeness INHERITED from the source, not verified)"
}

// runSerial is spec 10.1's loop with nothing overlapped: one slot resolved at a
// time, one batch fully recorded before the next is read. FetchConcurrency of
// one or less selects it, and it is the reference behavior the concurrent and
// pipelined paths are held identical to.
func (ix *Indexer) runSerial(ctx context.Context) error {
	for {
		advanced, err := ix.Step(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		if advanced {
			ix.cfg.Metrics.IndexProgress(ix.cfg.Head)
			// More to do, and F may have moved on too: go straight round.
			continue
		}
		ix.cfg.Metrics.IndexOutcome(ix.cfg.Head, metrics.IndexOutcomeCaughtUp)
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(ix.cfg.PollInterval):
		}
	}
}

// runPipelined is runSerial with one batch of lookahead: while a batch is being
// POSTed to the archive, the next batch's slots are fetched from the upstream.
// The POST side still consumes batches strictly in order and never lags the
// fetch side by more than the one batch in flight, so nothing here changes which
// batches exist or what they hold -- only when a batch's fetches happen relative
// to the puts of the batch before it.
//
// It only ever looks one batch ahead, and only when that next batch is fully
// determined and provably identical to what a fresh pass would compute (see
// maybePrefetch). Everything it cannot prove -- the frontier where a batch meets
// finality, an early-stopped batch, any error -- drops back to the planning
// path, which re-reads finality and coverage from scratch. In anchored mode the
// prefetched batch also inherits the batch-before-it's continuity anchor, so its
// walk validates against exactly the anchor a fresh pass would carry.
func (ix *Indexer) runPipelined(ctx context.Context) error {
	if err := ix.loadOrigin(ctx); err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return err
	}

	// The single batch of lookahead, or nil when none is in flight. Holding
	// exactly one is the pipeline's memory bound: one batch of blobs beyond the
	// one being POSTed.
	var ahead *prefetch

	for {
		var fb fetchedBatch
		if ahead != nil {
			// Consume the prefetched batch. It was planned against a finality bound
			// already known to cover it in full, and (anchored) against the anchor
			// its predecessor produced, so it is the batch a fresh pass would make.
			var err error
			fb, err = ahead.await()
			ahead = nil
			if err != nil {
				if ctx.Err() != nil {
					return nil
				}
				return err
			}
		} else {
			p, err := ix.plan(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return nil
				}
				return err
			}
			if p.caughtUp {
				ix.cfg.Metrics.IndexOutcome(ix.cfg.Head, metrics.IndexOutcomeCaughtUp)
				select {
				case <-ctx.Done():
					return nil
				case <-time.After(ix.cfg.PollInterval):
				}
				continue
			}
			if fb, err = ix.fetch(ctx, p); err != nil {
				if ctx.Err() != nil {
					return nil
				}
				return err
			}
		}

		// Start the next batch's fetch while this one POSTs, when its range is
		// provably the batch a fresh pass would compute here.
		ahead = ix.maybePrefetch(ctx, fb)

		advanced, err := ix.post(ctx, fb)
		if err != nil {
			// This batch did not land. Tear the lookahead down cleanly -- cancel
			// its fetch and join its goroutine before returning, so none outlives
			// the run -- and let the restart rebuild from a fresh synced_to.
			if ahead != nil {
				ahead.discard()
			}
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		if !advanced {
			ix.cfg.Metrics.IndexOutcome(ix.cfg.Head, metrics.IndexOutcomeCaughtUp)
			// A batch that recorded nothing: its first slot was not yet covered.
			// There is nothing settled to read past the frontier, so drop any
			// lookahead and wait, then plan afresh.
			if ahead != nil {
				ahead.discard()
				ahead = nil
			}
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(ix.cfg.PollInterval):
			}
		} else {
			ix.cfg.Metrics.IndexProgress(ix.cfg.Head)
		}
	}
}

// prefetch is one batch of lookahead in flight: a background goroutine is
// resolving its slots from the upstream while the batch before it is POSTed.
// runPipelined holds at most one.
type prefetch struct {
	cancel context.CancelFunc
	done   chan struct{}
	fb     fetchedBatch
	err    error
}

// startPrefetch launches the background fetch of plan p under a context derived
// from ctx, so the pipeline can cancel this fetch -- and only this fetch -- if
// the batch ahead of it fails to POST.
func (ix *Indexer) startPrefetch(ctx context.Context, p batchPlan) *prefetch {
	fctx, cancel := context.WithCancel(ctx)
	pf := &prefetch{cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(pf.done)
		pf.fb, pf.err = ix.fetch(fctx, p)
	}()
	return pf
}

// await blocks for the fetch to finish and hands back its result. The batch it
// yields is POSTed under the caller's own context, not this derived one, so the
// cancel here only releases the finished context's resources.
func (pf *prefetch) await() (fetchedBatch, error) {
	<-pf.done
	pf.cancel()
	return pf.fb, pf.err
}

// discard cancels the in-flight fetch and waits for its goroutine to exit: how a
// lookahead is abandoned, without leaking a goroutine, when the batch ahead of
// it fails to POST. A discarded prefetch never touched the Indexer's committed
// anchor, so dropping it cannot corrupt continuity.
func (pf *prefetch) discard() {
	pf.cancel()
	<-pf.done
}

// maybePrefetch starts the fetch of the batch after fb, but only when that next
// batch is provably the one a fresh pass would compute here -- which is what
// keeps the pipeline's batches byte-identical to the serial loop's.
//
// Two things must hold. First, fb committed the whole of its planned range
// (*last == end): a mirror early stop (a not-yet-covered slot) or an anchored
// trailing-404 trim both leave *last < end, meaning the frontier is right here
// and there is nothing settled past it. Second, the entire next batch sits at or
// below the finality fb was planned against. Finality only moves forward, so a
// fresh pass can only raise the bound: a next batch that already fits under the
// bound in hand is the exact full batch a fresh pass would read. Anchored mode
// passes fb's output anchor as the next batch's input anchor -- exactly the anchor
// a fresh pass would carry, since a full batch's last committed slot is its last
// present slot, whose root is that anchor, and post commits it before the next
// plan.
func (ix *Indexer) maybePrefetch(ctx context.Context, fb fetchedBatch) *prefetch {
	if fb.last == nil || *fb.last != fb.end {
		return nil
	}
	start := fb.end + 1
	end := start + ix.cfg.BatchSize - 1
	if end > fb.finalized {
		return nil
	}
	return ix.startPrefetch(ctx, batchPlan{
		start: start, end: end, finalized: fb.finalized,
		inRoot: fb.outRoot, haveIn: fb.haveOut,
	})
}

// Step runs one pass of spec 10.1's loop: at most one batch.
//
// It reports whether it advanced coverage. A false means caught up -- the
// archive is level with the upstream's finality, or the upstream has nothing
// finalized yet -- and the caller sleeps. Tests drive this directly to run an
// indexer to completion without waiting on any clock.
func (ix *Indexer) Step(ctx context.Context) (bool, error) {
	if err := ix.loadOrigin(ctx); err != nil {
		return false, err
	}
	p, err := ix.plan(ctx)
	if err != nil {
		return false, err
	}
	if p.caughtUp {
		return false, nil
	}
	fb, err := ix.fetch(ctx, p)
	if err != nil {
		return false, err
	}
	return ix.post(ctx, fb)
}

// batchPlan is the range one pass will cover, with the finality bound it was
// computed against and (anchored) the continuity anchor its first present slot
// must chain to. The pipeline carries the finality bound forward to prove the
// next batch is one it may fetch ahead (see maybePrefetch), and the anchor so
// the prefetched batch validates as a fresh pass would.
type batchPlan struct {
	start, end uint64
	finalized  uint64
	// caughtUp is set when there is nothing to do this pass: the upstream has no
	// finalized coverage yet, or the archive is already level with it.
	caughtUp bool
	// inRoot is the anchor this batch's first present slot must chain to; haveIn
	// is false when there is no anchor yet (a genesis start). Anchored only.
	inRoot [32]byte
	haveIn bool
}

// plan reads finality and coverage and works out the next batch's range: spec
// 10.1's "F = finalized slot; s = synced_to + 1 (or origin_slot); batch =
// [s, min(F, s+B-1)]". In anchored mode it also seeds the continuity anchor once
// and attaches it to the plan.
func (ix *Indexer) plan(ctx context.Context) (batchPlan, error) {
	finalized, ok, err := ix.finalizedSlot(ctx)
	if err != nil {
		return batchPlan{}, fmt.Errorf("beacon: reading upstream finality: %w", err)
	}
	if !ok {
		// An archive upstream with no coverage yet, or a block feed that is
		// syncing or optimistic. Nothing to copy.
		ix.log.Debug("upstream has no finalized coverage yet")
		return batchPlan{caughtUp: true}, nil
	}
	start, err := ix.resume(ctx)
	if err != nil {
		return batchPlan{}, err
	}
	if start > finalized {
		return batchPlan{caughtUp: true}, nil
	}
	if ix.anchored {
		ready, err := ix.seedContinuity(ctx, start)
		if err != nil {
			return batchPlan{}, err
		}
		if !ready {
			// The continuity anchor cannot be established yet (the walk reached slot 0
			// with no present header and no trusted checkpoint). Absence at the leading
			// edge cannot be proven without an anchor, so wait -- retryable, the same
			// posture as an optimistic feed -- rather than commit a false miss (audit
			// the safety boundary).
			return batchPlan{caughtUp: true}, nil
		}
	}
	end := min(finalized, start+ix.cfg.BatchSize-1)
	return batchPlan{
		start: start, end: end, finalized: finalized,
		inRoot: ix.lastRoot, haveIn: ix.haveLastRoot,
	}, nil
}

// finalizedSlot is F: the block feed's finalized checkpoint in anchored mode,
// the archive's synced_to in mirror mode. Anchored mode never reads finality
// from a blob source (spec 10.3).
func (ix *Indexer) finalizedSlot(ctx context.Context) (uint64, bool, error) {
	if ix.anchored {
		return ix.cfg.Blocks.FinalizedSlot(ctx)
	}
	return ix.cfg.Sources[0].Client.FinalizedSlot(ctx)
}

// loadOrigin reads the head's origin_slot once, and in mirror mode validates the
// upstream archive against it (spec 11.5): the upstream can re-derive this head
// only if its own coverage starts at or before this head's origin, so a higher
// origin_slot is refused. That check is what makes a later unfiltered 404 a
// protocol violation rather than a below-origin absence.
func (ix *Indexer) loadOrigin(ctx context.Context) error {
	if ix.gotOrigin {
		return nil
	}
	info, err := ix.cfg.Archive.Head(ctx, ix.cfg.Head)
	if err != nil {
		return fmt.Errorf("beacon: reading head %q: %w", ix.cfg.Head, err)
	}
	if !ix.anchored {
		upOrigin, err := ix.cfg.Sources[0].Client.OriginSlot(ctx)
		if err != nil {
			return fmt.Errorf("beacon: reading the mirror upstream's origin_slot: %w", err)
		}
		if upOrigin > info.OriginSlot {
			return fmt.Errorf("beacon: refusing to run in mirror mode: the upstream archive's origin_slot %d is above "+
				"this head's origin_slot %d, so it cannot re-derive the whole head (spec 11.5); its coverage must start "+
				"at or before slot %d", upOrigin, info.OriginSlot, info.OriginSlot)
		}
	}
	ix.origin, ix.gotOrigin = info.OriginSlot, true
	return nil
}

// resume returns the first slot this pass must cover: spec 10.1's
// "s = archive synced_to + 1 (or origin_slot)".
func (ix *Indexer) resume(ctx context.Context) (uint64, error) {
	syncedTo, err := ix.cfg.Archive.SyncedTo(ctx, ix.cfg.Head)
	if err != nil {
		return 0, fmt.Errorf("beacon: reading synced_to of head %q: %w", ix.cfg.Head, err)
	}
	if syncedTo == nil {
		return ix.origin, nil
	}
	return *syncedTo + 1, nil
}

// seedContinuity establishes anchored mode's continuity anchor, on the first batch
// of a run (including after a restart), by walking headers backward from start-1 to
// the most recent present slot. That slot's root is the anchor the first present
// slot of the batch must chain to. It reports whether the anchor is ready: a false
// is not an error but a "wait" -- the caller re-plans and retries, the same posture
// as an optimistic feed -- so a run that cannot yet be anchored waits rather than
// advancing coverage over an unproven leading miss.
//
// Every cached seed -- a held real anchor as much as the anchorless genesis wait --
// is bound to the exact resume slot it was computed for (expectedResume) and reused
// only while the freshly observed resume still equals it. When the shared archive
// moves under this indexer (a duplicate writer advancing it, spec 11.1's one-writer
// rule broken) the observed resume no longer matches, so the cached anchor -- which
// followed the slot it left off at, not the one the archive now resumes from -- is
// discarded and the walk reseeds from the new start-1 (see the fast path and the
// expectedResume field). That is what stops a stale anchor, rootless or not, from
// advancing a later batch whose intervening slots it never validated.
//
// The walk is bounded: mainnet gaps are 1-3 slots, so a run of seedWalkBound
// headers with no block means the feed is still backfilling history, which is a
// hard error rather than a hole to walk through.
//
// # The zero boundary
//
// A walk that reaches slot 0 without a present header has NO anchor, and an
// unanchored batch that then accepted its first present slot without a parent
// check would commit the leading 404s as proven absence -- absence proven by
// nothing. So reaching zero unanchored yields no usable seed: it returns not-ready
// and waits, retryable indefinitely, until the feed backfills a present header in
// the walk range or a checkpoint is configured (a later origin_slot is the other
// option). A genuinely PRESENT slot-0 header is the one exception the walk finds
// on its own -- genesis has no parent, so its own root bootstraps the anchor. A
// genesis start (start == 0) is the same exception ahead of time: nothing precedes
// slot 0, so the batch's own slot 0 bootstraps if present (reassembleAnchored only
// bootstraps an unanchored batch on a present first slot).
//
// # The checkpoint
//
// A configured ContinuityCheckpoint gives the walk a trusted floor below the
// resume slot. When the walk reaches the checkpoint slot, the configured root is
// the anchor (whether the feed 404s it or corroborates it); a feed header that
// MISMATCHES the configured root is a fatal error. The checkpoint slot must be
// strictly before the first slot this run covers, so it can never itself advance
// coverage.
func (ix *Indexer) seedContinuity(ctx context.Context, start uint64) (bool, error) {
	if ix.seeded {
		// The cached seed -- a held anchor OR the anchorless genesis wait -- is
		// reusable only while the freshly observed resume slot is exactly the one the
		// anchor was computed for. A match means the predecessor batch really landed
		// (serial or prefetch carry): reuse it. A mismatch in EITHER direction means
		// the shared archive moved under this indexer since the anchor was recorded --
		// a duplicate writer advanced it, or it rewound -- so the anchor no longer
		// follows the slot it represents and would let a later batch skip the
		// intervening slots' validation. Discard it and reseed from the new start-1
		//. This binds even a genuine root to its exact
		// resume, closing the held-real-root bypass.
		if ix.expectedResume == start {
			return true, nil
		}
		// Clear BOTH the cached root and seed readiness, so no stale anchor lingers
		// into the reseed's wait window: if the walk below cannot yet establish an
		// anchor it returns not-ready with haveLastRoot already false, and the next
		// pass reseeds from scratch rather than reusing r-old.
		ix.seeded, ix.haveLastRoot = false, false
	}
	if cp := ix.cfg.ContinuityCheckpoint; cp != nil && cp.Slot >= start {
		return false, fmt.Errorf("beacon: continuity_checkpoint.slot %d is not strictly before the first slot this run "+
			"covers (%d): the checkpoint anchors the walk back from the resume point and must never itself advance "+
			"coverage", cp.Slot, start)
	}
	if start == 0 {
		// Genesis coverage: nothing precedes slot 0 to anchor against, so the batch's
		// own slot 0 bootstraps the anchor if present. A checkpoint here was already
		// rejected above (its slot cannot be strictly below 0). This anchorless state
		// expects resume 0: if a later pass resumes past 0, the fast path above
		// discards it and reseeds.
		ix.seeded, ix.expectedResume = true, start
		return true, nil
	}
	slot := start - 1
	for i := 0; i < seedWalkBound; i++ {
		root, _, present, err := ix.cfg.Blocks.Header(ctx, slot)
		if err != nil {
			return false, fmt.Errorf("beacon: seeding continuity anchor at slot %d: %w", slot, err)
		}
		if cp := ix.cfg.ContinuityCheckpoint; cp != nil && slot == cp.Slot {
			// The trusted floor: the walk stops here and anchors to the configured
			// root. A present header that disagrees with it is a fatal config error --
			// nothing may advance when the feed and the operator disagree about the
			// anchor everything chains to.
			if present && root != cp.Root {
				return false, fmt.Errorf("beacon: continuity_checkpoint mismatch at slot %d: the block feed's header "+
					"root %s does not match the configured continuity_checkpoint.root %s; refusing to advance on a feed "+
					"that disagrees with the trusted checkpoint", cp.Slot, rootHex(root), rootHex(cp.Root))
			}
			ix.lastRoot, ix.haveLastRoot, ix.seeded, ix.expectedResume = cp.Root, true, true, start
			ix.log.Info("seeded continuity anchor from configured checkpoint",
				"slot", cp.Slot, "root", rootHex(cp.Root), "feed_present", present)
			return true, nil
		}
		if present {
			ix.lastRoot, ix.haveLastRoot, ix.seeded, ix.expectedResume = root, true, true, start
			ix.log.Info("seeded continuity anchor", "walked_back_from", start-1, "present_at", slot, "root", rootHex(root))
			return true, nil
		}
		if slot == 0 {
			// Zero reached with no present header and no checkpoint at or below it: no
			// anchor exists, and leading absence cannot be proven without one. Wait,
			// retryable, rather than commit a false miss.
			ix.log.Warn("continuity anchor cannot be seeded yet: the walk reached slot 0 with no present header and "+
				"no continuity_checkpoint; waiting for the feed to backfill a present header (or configure "+
				"upstream.continuity_checkpoint, or raise the head's origin_slot)", "walked_back_from", start-1)
			return false, nil
		}
		slot--
	}
	return false, fmt.Errorf("beacon: could not seed the continuity anchor: no present slot in the %d headers before slot %d; "+
		"the block feed is likely still backfilling historical blocks (mainnet gaps are 1-3 slots)", seedWalkBound, start)
}

// fetchedBatch is a batch whose slots have been resolved but not yet recorded:
// the rows and blob bytes to put, and the last slot the walk actually reached
// (nil if it stopped before its first slot). It carries the range and finality
// bound it was planned against, and (anchored) the continuity anchor it produced,
// so the pipeline can plan and validate the batch after it.
type fetchedBatch struct {
	start, end uint64
	finalized  uint64
	rows       []archclient.Row
	blobs      [][]byte
	last       *uint64
	// outRoot is the continuity anchor after this batch: the last present slot's
	// root, or the input anchor if the batch had no present slot. haveOut is
	// false only when the batch was walked with no anchor and found no present
	// slot. Anchored only.
	outRoot [32]byte
	haveOut bool
}

// fetch resolves a plan's slots. It is the fetch half of a batch -- the half the
// pipeline runs ahead of the POST of the batch before it.
func (ix *Indexer) fetch(ctx context.Context, p batchPlan) (fetchedBatch, error) {
	if ix.anchored {
		rows, blobs, last, outRoot, haveOut, err := ix.walkAnchored(ctx, p.start, p.end, p.inRoot, p.haveIn)
		if err != nil {
			return fetchedBatch{}, err
		}
		return fetchedBatch{
			start: p.start, end: p.end, finalized: p.finalized,
			rows: rows, blobs: blobs, last: last, outRoot: outRoot, haveOut: haveOut,
		}, nil
	}
	rows, blobs, last, err := ix.walkMirror(ctx, p.start, p.end)
	if err != nil {
		return fetchedBatch{}, err
	}
	return fetchedBatch{
		start: p.start, end: p.end, finalized: p.finalized,
		rows: rows, blobs: blobs, last: last,
	}, nil
}

// post records a fetched batch: put its bytes, verify the archive's derived
// versioned hashes against what was fetched, and post the refs. It reports
// whether it advanced coverage. On success in anchored mode it commits the
// batch's continuity anchor -- here, at post time, so a discarded prefetch never
// touches it.
//
// A batch whose walk stopped before its first slot (nil last) advanced nothing:
// the upstream had not covered it yet, and "not yet" is not "nothing", so there
// is no coverage to record and the caller waits.
func (ix *Indexer) post(ctx context.Context, fb fetchedBatch) (bool, error) {
	if fb.last == nil {
		return false, nil
	}

	put, err := ix.put(ctx, fb.blobs)
	if err != nil {
		return false, err
	}
	if ix.anchored {
		// The rows already carry the block-derived vhs; verify is a positional
		// byte-compare against them, no KZG recompute (see verifyAnchored).
		if err := verifyAnchored(fb.rows, put); err != nil {
			return false, err
		}
	} else {
		if err := verify(fb.blobs, put); err != nil {
			return false, err
		}
		if err := assign(fb.rows, put); err != nil {
			return false, err
		}
	}

	// The empty expected_manifest: the beacon indexer writes the ALL head (spec
	// 10.5), an identity filter that never carries a manifest chain, so there is
	// no tip to bind to and the server forbids the field on it.
	res, err := ix.cfg.Archive.PostRefs(ctx, ix.cfg.Head, fb.rows, *fb.last, "")
	if err != nil {
		return false, fmt.Errorf("beacon: posting refs for slots [%d, %d]: %w", fb.start, *fb.last, err)
	}
	if ix.anchored {
		// Bind the carried anchor to the resume it will be validated against next. The
		// batch committed contiguously up to *fb.last (reassembly proved every slot in
		// [start, *fb.last]), so the next batch legitimately resumes at *fb.last+1 and
		// the anchor is exactly its parent. This is a LOCALLY validated value, not the
		// archive's response: a stale or no-op PostRefs reply never redefines the
		// expected resume, so a duplicate writer's advance is caught at the next plan
		// (observed resume != expectedResume -> reseed), not laundered here (audit
		// the safety boundary follow-up).
		ix.lastRoot, ix.haveLastRoot, ix.expectedResume = fb.outRoot, fb.haveOut, *fb.last+1
	}
	ix.log.Info("recorded slots",
		"head", ix.cfg.Head, "from", fb.start, "to", *fb.last,
		"rows", len(fb.rows), "blobs", len(fb.blobs), "root", res.Root, "noop", res.NoOp)
	return true, nil
}

// put posts every blob of a batch, in as many requests as max_put_blobs allows,
// and returns their derived identities in the order they were sent.
func (ix *Indexer) put(ctx context.Context, blobs [][]byte) ([]archclient.PutBlob, error) {
	out := make([]archclient.PutBlob, 0, len(blobs))
	for i := 0; i < len(blobs); i += ix.cfg.MaxPutBlobs {
		chunk := blobs[i:min(i+ix.cfg.MaxPutBlobs, len(blobs))]
		res, err := ix.cfg.Archive.PutBlobs(ctx, chunk)
		if err != nil {
			return nil, fmt.Errorf("beacon: putting blobs [%d, %d): %w", i, i+len(chunk), err)
		}
		out = append(out, res...)
	}
	return out, nil
}

// -------------------------------------------------------------------------
// Anchored mode
// -------------------------------------------------------------------------

// anchoredSlot is one slot's anchored resolution, before the order-dependent
// continuity check: whether it carried a block, its root and its parent's root,
// and the block-anchored blobs a source served (nil for a blobless or missed
// slot).
type anchoredSlot struct {
	present    bool
	root       [32]byte
	parentRoot [32]byte
	vhs        []schema.VersionedHash
	blobs      [][]byte
}

// anchoredResult is one slot's resolution held in slot order for reassembly. The
// explicit done flag makes an un-run slot (a worker the ctx cancelled before it
// launched) impossible to mistake for a resolved one: without it, the zero value
// reads as a missed slot and would silently advance coverage.
type anchoredResult struct {
	res  anchoredSlot
	err  error
	done bool
}

// walkAnchored resolves every slot in [start, end] against the block feed and
// the blob sources, serially or concurrently as FetchConcurrency says, and
// returns the identical result either way: the rows to record (one per slot with
// blobs, vhs already filled from the block), the blob bytes to put (flattened in
// row order), the last slot walked (always end -- anchored mode never stops
// early, finality bounds it), and the continuity anchor after the batch.
func (ix *Indexer) walkAnchored(ctx context.Context, start, end uint64, inRoot [32]byte, haveIn bool) ([]archclient.Row, [][]byte, *uint64, [32]byte, bool, error) {
	if ix.cfg.FetchConcurrency <= 1 || start == end {
		return ix.walkAnchoredSerial(ctx, start, end, inRoot, haveIn)
	}
	return ix.walkAnchoredConcurrent(ctx, start, end, inRoot, haveIn)
}

// walkAnchoredSerial resolves the slots one after another. It is the reference
// the concurrent walk is held identical to.
func (ix *Indexer) walkAnchoredSerial(ctx context.Context, start, end uint64, inRoot [32]byte, haveIn bool) ([]archclient.Row, [][]byte, *uint64, [32]byte, bool, error) {
	results := make([]anchoredResult, int(end-start+1))
	for slot := start; slot <= end; slot++ {
		if err := ctx.Err(); err != nil {
			return nil, nil, nil, [32]byte{}, false, err
		}
		res, err := ix.resolveAnchoredSlot(ctx, slot)
		results[slot-start] = anchoredResult{res: res, err: err, done: true}
		if err != nil {
			// Stop at the first failure; reassembly surfaces it as the first error
			// by slot order, which is the slot the serial walk reached and stopped
			// on. The slots past it are left un-run, and reassembly never reaches
			// them.
			break
		}
	}
	return ix.reassembleAnchored(start, end, results, inRoot, haveIn)
}

// walkAnchoredConcurrent resolves the slots through a bounded fan-out and
// reassembles them in slot order, so its result is byte-for-byte
// walkAnchoredSerial's. Each worker owns one index and wg.Wait sequences the
// writes before the reads; fetch order is structurally unable to reach the DAG.
func (ix *Indexer) walkAnchoredConcurrent(ctx context.Context, start, end uint64, inRoot [32]byte, haveIn bool) ([]archclient.Row, [][]byte, *uint64, [32]byte, bool, error) {
	results := make([]anchoredResult, int(end-start+1))

	sem := make(chan struct{}, ix.cfg.FetchConcurrency)
	var wg sync.WaitGroup
launch:
	for slot := start; slot <= end; slot++ {
		select {
		case <-ctx.Done():
			// Stop launching; the workers already running observe the same
			// cancellation and return promptly.
			break launch
		case sem <- struct{}{}:
		}
		wg.Add(1)
		go func(slot uint64) {
			defer wg.Done()
			defer func() { <-sem }()
			res, err := ix.resolveAnchoredSlot(ctx, slot)
			results[slot-start] = anchoredResult{res: res, err: err, done: true}
		}(slot)
	}
	wg.Wait()
	if err := ctx.Err(); err != nil {
		return nil, nil, nil, [32]byte{}, false, err
	}
	return ix.reassembleAnchored(start, end, results, inRoot, haveIn)
}

// resolveAnchoredSlot resolves one slot: the block feed decides whether it
// carried a block and what blobs it must hold, and the sources supply those
// blobs' bytes. It does no ordering work -- the continuity check is
// reassembly's, because it depends on slot order.
func (ix *Indexer) resolveAnchoredSlot(ctx context.Context, slot uint64) (anchoredSlot, error) {
	root, parentRoot, present, err := ix.cfg.Blocks.Header(ctx, slot)
	if err != nil {
		return anchoredSlot{}, fmt.Errorf("beacon: reading header for slot %d: %w", slot, err)
	}
	if !present {
		return anchoredSlot{present: false}, nil
	}
	commits, err := ix.cfg.Blocks.Commitments(ctx, slot)
	if err != nil {
		return anchoredSlot{}, fmt.Errorf("beacon: reading commitments for slot %d: %w", slot, err)
	}
	if len(commits) == 0 {
		return anchoredSlot{present: true, root: root, parentRoot: parentRoot}, nil
	}
	vhs := make([]schema.VersionedHash, len(commits))
	for i, c := range commits {
		vhs[i] = ingest.VersionedHashFromCommitment(c)
	}
	blobs, err := ix.resolveFromSources(ctx, slot, vhs)
	if err != nil {
		return anchoredSlot{}, err
	}
	return anchoredSlot{present: true, root: root, parentRoot: parentRoot, vhs: vhs, blobs: blobs}, nil
}

// resolveFromSources asks each source in order for the whole slot and returns
// the first anchored answer (spec 10.1). Every source that cannot help -- a 404,
// a wrong count, bytes that do not commit, a 503, or a terminal error -- is
// counted, logged, and skipped for the next; all sources exhausted is a batch
// error naming each one's reason. Absence is never recorded here: a source's
// "no" is about the source, and the block already proved the blobs exist.
func (ix *Indexer) resolveFromSources(ctx context.Context, slot uint64, vhs []schema.VersionedHash) ([][]byte, error) {
	var reasons []string
	for i, src := range ix.cfg.Sources {
		label := metrics.SourcePrimary
		if i > 0 {
			label = metrics.SourceFallback
		}
		blobs, outcome, reason, err := ix.trySource(ctx, src, slot, vhs)
		if err != nil {
			// A cancelled context: the caller stopping us, returned as-is.
			return nil, err
		}
		ix.cfg.Metrics.SourceFetch(label, outcome)
		if outcome == metrics.SourceAnchored {
			return blobs, nil
		}
		// A KZG mismatch is a source serving the wrong blob for a vh it was asked
		// by name -- worth a Warn; the rest are the ordinary "this source does not
		// have it" and are Debug.
		log := ix.log.Debug
		if outcome == metrics.SourceMismatch {
			log = ix.log.Warn
		}
		log("anchored source could not serve the slot; trying the next",
			"slot", slot, "source", label, "outcome", outcome, "reason", reason)
		reasons = append(reasons, fmt.Sprintf("%s: %s", label, reason))
	}
	return nil, fmt.Errorf("beacon: slot %d: no source served the block's %d blobs anchored (%s)",
		slot, len(vhs), strings.Join(reasons, "; "))
}

// trySource asks one source for a slot and classifies the answer against the
// block-derived expectation, without recording metrics (the caller does, once
// per attempt). It returns a non-nil err only for a cancelled context; a source
// that simply cannot help is an outcome, not an error.
//
// Sources are asked unfiltered. The answer arrives in canonical block order,
// which is exactly the order vhs is in, one entry per commitment. Acceptance
// requires exactly len(vhs) blobs, each anchoring positionally to its expected
// vh, so a slot with duplicate commitments returns the duplicate blob N times
// and still anchors positionally.
func (ix *Indexer) trySource(ctx context.Context, src Source, slot uint64, vhs []schema.VersionedHash) (blobs [][]byte, outcome, reason string, err error) {
	res, e := src.Client.Blobs(ctx, slot, nil)
	if e != nil {
		if ctx.Err() != nil {
			return nil, "", "", ctx.Err()
		}
		return nil, metrics.SourceError, e.Error(), nil
	}
	switch res.Status {
	case upstream.StatusFound:
		if len(res.Blobs) != len(vhs) {
			return nil, metrics.SourceAbsent,
				fmt.Sprintf("returned %d blobs, the block has %d", len(res.Blobs), len(vhs)), nil
		}
		for i, b := range res.Blobs {
			// The KZG anchoring: the one commitment pass per blob spec 10.1 asks
			// for, checking the bytes commit to the vh the block named.
			vh, verr := ingest.VersionedHash(b)
			if verr != nil || vh != vhs[i] {
				return nil, metrics.SourceMismatch,
					fmt.Sprintf("blob %d does not commit to its expected versioned hash %s", i, archclient.VHHex(vhs[i])), nil
			}
		}
		return res.Blobs, metrics.SourceAnchored, "", nil
	case upstream.StatusAbsent:
		return nil, metrics.SourceAbsent, "reported the slot absent (404)", nil
	case upstream.StatusNotYetCovered:
		return nil, metrics.SourceError, "reported the slot not yet covered (503)", nil
	default:
		return nil, metrics.SourceError, fmt.Sprintf("returned unknown status %v", res.Status), nil
	}
}

// reassembleAnchored folds the per-slot resolutions into a batch, in slot order,
// enforcing parent-root continuity: every present slot's parent_root must equal
// the root of the most recent present slot before it. A break is fatal and names
// both slots -- the block feed 404'd or is hiding a block it must have, and
// recording the intervening 404s as absence would be the very bug anchored mode
// exists to prevent.
//
// # The committed boundary is the last PRESENT slot, never end
//
// A header-404 slot's absence is proven only by the next present slot chaining
// over it. Interior 404s -- those with a present slot after them in this batch --
// are proven here and committed. A TRAILING run of 404s (past the last present
// slot of the batch) has no proof yet, so it is left uncommitted: those slots are
// re-walked at the head of the next batch, whose first present slot proves them
// against the carried anchor. Committing them now would record absence a later
// batch might prove false, and under spec 5.1's idempotent replay a wrong miss is
// permanent -- exactly the failure anchored mode exists to prevent, and it would
// land before the continuity break that would catch it.
//
// A batch with NO present slot therefore commits nothing (nil last -> the caller
// waits): a feed 404ing an entire range is one still backfilling blocks, and the
// indexer waits for a block rather than recording a missed range. When the final
// finalized slot is itself missed, coverage sits at the last present slot until a
// later present slot finalizes -- that looks like the tip lagging, but it is the
// proof rule holding, not a stall.
func (ix *Indexer) reassembleAnchored(start, end uint64, results []anchoredResult, inRoot [32]byte, haveIn bool) ([]archclient.Row, [][]byte, *uint64, [32]byte, bool, error) {
	anchor, haveAnchor := inRoot, haveIn
	var lastPresent uint64
	var haveLastPresent bool

	var order []pendingRow
	var blobs [][]byte
	for slot := start; slot <= end; slot++ {
		r := results[slot-start]
		if !r.done {
			// Only reachable if the context cancelled a worker before it launched;
			// the concurrent walk checks ctx.Err() before reassembly, so this is a
			// stated invariant rather than a state that reads as a missed slot.
			return nil, nil, nil, [32]byte{}, false,
				fmt.Errorf("beacon: slot %d was not resolved (internal: reassembly reached an un-run slot)", slot)
		}
		if r.err != nil {
			return nil, nil, nil, [32]byte{}, false, r.err
		}
		a := r.res
		if !a.present {
			// A candidate missed slot: recorded as nothing. If a present slot
			// follows it in this batch, that slot's continuity proves the miss and it
			// is committed; if none does (a trailing run), it is left for the next
			// batch to prove.
			continue
		}
		if !haveAnchor && slot != 0 {
			// The bootstrap exception -- accepting an unanchored batch's first present
			// slot without a parent check -- is legitimate ONLY at the literal genesis
			// slot 0, which has no parent to chain to. Any other present slot reached
			// without an anchor is a leading miss whose absence nothing proves (genesis
			// 404s before slot 0's backfill), or a stale-genesis batch that resumed past
			// 0 (a duplicate writer advanced the shared archive) -- either way,
			// bootstrapping here would commit unproven absence, exactly the leading
			// false miss of the safety boundary. Commit nothing and wait (nil last). Note the
			// guard is slot != 0, not slot != start: a batch that starts above 0 must
			// never bootstrap, even on its own first slot. A
			// start > 0 batch is normally anchored here anyway -- seedContinuity produced
			// an anchor or the run waited -- so this is the belt to that suspenders.
			return nil, nil, nil, anchor, haveAnchor, nil
		}
		if haveAnchor && a.parentRoot != anchor {
			return nil, nil, nil, [32]byte{}, false, ix.continuityBreak(slot, a.parentRoot, anchor, lastPresent, haveLastPresent)
		}
		anchor, haveAnchor = a.root, true
		lastPresent, haveLastPresent = slot, true
		if len(a.vhs) > 0 {
			order = append(order, pendingRow{slot: slot, vhs: a.vhs})
			blobs = append(blobs, a.blobs...)
		}
	}
	if !haveLastPresent {
		// No present slot in the whole batch: nothing is proven, so nothing is
		// committed and the caller waits. The anchor is unchanged.
		return nil, nil, nil, anchor, haveAnchor, nil
	}
	// Commit up to the last present slot; the anchor is that slot's root by
	// construction, so the carried continuity state matches the committed boundary.
	last := lastPresent
	return makeAnchoredRows(order), blobs, &last, anchor, haveAnchor, nil
}

// continuityBreak renders the fatal error for a present slot whose parent_root
// does not chain to the previous present slot's root.
func (ix *Indexer) continuityBreak(slot uint64, parentRoot, anchor [32]byte, anchorSlot uint64, haveAnchorSlot bool) error {
	prev := "the last present slot before this batch"
	if haveAnchorSlot {
		prev = fmt.Sprintf("present slot %d", anchorSlot)
	}
	return fmt.Errorf("beacon: continuity broken at slot %d: its parent_root %s does not match the root %s of %s; "+
		"the block feed 404'd or lacks a block it must have (a still-backfilling node, or a hidden block) -- "+
		"refusing to record the slots between as absent", slot, rootHex(parentRoot), rootHex(anchor), prev)
}

// pendingRow is one present slot's row before it becomes an archclient.Row: its
// vhs are already known from the block's commitments (anchored mode derives them
// before any source is asked), unlike mirror mode where they arrive with the put.
type pendingRow struct {
	slot uint64
	vhs  []schema.VersionedHash
}

// makeAnchoredRows renders the anchored tally as refs rows with their
// block-derived vhs already filled, in slot order (spec 5.1).
func makeAnchoredRows(order []pendingRow) []archclient.Row {
	rows := make([]archclient.Row, 0, len(order))
	for _, p := range order {
		rows = append(rows, archclient.Row{Slot: p.slot, VHs: p.vhs})
	}
	return rows
}

// verifyAnchored checks the archive's derived vhs against the block-derived vhs
// the rows already carry: a positional byte-compare, no KZG recompute.
//
// The blobs were already anchored to the block's commitments when a source
// served them (trySource's KZG pass), so the archive -- which re-derives every vh
// from the bytes it received (spec 7.2) -- should answer those same vhs. If it
// does not, the bytes were corrupted between this indexer and the archive: the
// same in-flight-corruption guarantee mirror mode's verify gives, without paying
// for a second commitment, because the block already stated what these blobs are.
func verifyAnchored(rows []archclient.Row, put []archclient.PutBlob) error {
	want := 0
	for i := range rows {
		want += len(rows[i].VHs)
	}
	if want != len(put) {
		return fmt.Errorf("beacon: block-derived %d blobs, archive answered for %d", want, len(put))
	}
	next := 0
	for i := range rows {
		for _, vh := range rows[i].VHs {
			if put[next].VH != vh {
				return fmt.Errorf("beacon: slot %d: a blob was anchored to %s but the archive stored it as %s; "+
					"the blob was corrupted between the source and the archive",
					rows[i].Slot, archclient.VHHex(vh), archclient.VHHex(put[next].VH))
			}
			next++
		}
	}
	return nil
}

// -------------------------------------------------------------------------
// Mirror mode
// -------------------------------------------------------------------------

// walkMirror walks the source archive upstream, spec 11.5's deterministic
// replication: it COPIES the source's per-slot decisions (no block feed of its
// own), so one slot's 200 (empty or with blobs) is recorded as given, a 503 stops
// the batch, and an in-range 404 is a protocol violation (loadOrigin's validation
// guarantees the walk only queries at or above origin, where an archive never 404s
// legitimately). Completeness is inherited from the source, not verified here.
func (ix *Indexer) walkMirror(ctx context.Context, start, end uint64) ([]archclient.Row, [][]byte, *uint64, error) {
	if ix.cfg.FetchConcurrency <= 1 || start == end {
		return ix.walkMirrorSerial(ctx, start, end)
	}
	return ix.walkMirrorConcurrent(ctx, start, end)
}

// walkMirrorSerial is the loop of spec 10.1: one slot after another, never
// fetching past the point the archive says it has not covered yet.
func (ix *Indexer) walkMirrorSerial(ctx context.Context, start, end uint64) ([]archclient.Row, [][]byte, *uint64, error) {
	var (
		order []pending
		blobs [][]byte
	)
	for slot := start; slot <= end; slot++ {
		if err := ctx.Err(); err != nil {
			return nil, nil, nil, err
		}
		res, err := ix.cfg.Sources[0].Client.Blobs(ctx, slot, nil)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("beacon: fetching slot %d: %w", slot, err)
		}
		eff, err := ix.classifyMirror(slot, res, &order, &blobs)
		if err != nil {
			return nil, nil, nil, err
		}
		if eff == slotUncovered {
			return ix.stopEarly(slot, start, order, blobs)
		}
	}
	last := end
	return makeRows(order), blobs, &last, nil
}

// walkMirrorConcurrent fetches the slots through a bounded fan-out and
// reassembles them in slot order, so its result is byte-for-byte
// walkMirrorSerial's: same rows, same blob order, same stopping point.
func (ix *Indexer) walkMirrorConcurrent(ctx context.Context, start, end uint64) ([]archclient.Row, [][]byte, *uint64, error) {
	results := make([]slotResult, int(end-start+1))

	sem := make(chan struct{}, ix.cfg.FetchConcurrency)
	var wg sync.WaitGroup
launch:
	for slot := start; slot <= end; slot++ {
		select {
		case <-ctx.Done():
			break launch
		case sem <- struct{}{}:
		}
		wg.Add(1)
		go func(slot uint64) {
			defer wg.Done()
			defer func() { <-sem }()
			res, err := ix.cfg.Sources[0].Client.Blobs(ctx, slot, nil)
			results[slot-start] = slotResult{res: res, err: err, done: true}
		}(slot)
	}
	wg.Wait()
	if err := ctx.Err(); err != nil {
		return nil, nil, nil, err
	}

	var (
		order []pending
		blobs [][]byte
	)
	for slot := start; slot <= end; slot++ {
		r := results[slot-start]
		if !r.done {
			return nil, nil, nil,
				fmt.Errorf("beacon: slot %d was not fetched (internal: reassembly reached an un-run slot)", slot)
		}
		if r.err != nil {
			return nil, nil, nil, fmt.Errorf("beacon: fetching slot %d: %w", slot, r.err)
		}
		eff, err := ix.classifyMirror(slot, r.res, &order, &blobs)
		if err != nil {
			return nil, nil, nil, err
		}
		if eff == slotUncovered {
			return ix.stopEarly(slot, start, order, blobs)
		}
	}
	last := end
	return makeRows(order), blobs, &last, nil
}

// slotResult is one slot's answer from a concurrent mirror fetch, held in slot
// order. The explicit done flag makes an un-run slot impossible to mistake for a
// found-empty one (StatusFound is the zero value).
type slotResult struct {
	res  upstream.Result
	err  error
	done bool
}

// slotEffect is what one slot's answer does to the running batch.
type slotEffect int

const (
	// slotRecorded is a found (empty or with blobs) slot: accumulated, and the
	// walk carries on.
	slotRecorded slotEffect = iota
	// slotUncovered is a 503: the archive has not got this far, so the walk stops
	// here (see stopEarly).
	slotUncovered
)

// classifyMirror applies one slot's archive answer to the running batch. A found
// slot with blobs appends its row tally and bytes; an empty found slot records
// nothing; a 503 stops the batch; a 404 is a protocol violation (loadOrigin
// validated the upstream's origin, so nothing at or above it can legitimately
// 404), and an unknown status is an error.
func (ix *Indexer) classifyMirror(slot uint64, res upstream.Result, order *[]pending, blobs *[][]byte) (slotEffect, error) {
	switch res.Status {
	case upstream.StatusFound:
		if len(res.Blobs) == 0 {
			// A covered slot with nothing in it: an archive states this as 200
			// {"data": []}. No row, but coverage advances over it.
			return slotRecorded, nil
		}
		*order = append(*order, pending{slot: slot, count: len(res.Blobs)})
		*blobs = append(*blobs, res.Blobs...)
		return slotRecorded, nil

	case upstream.StatusNotYetCovered:
		return slotUncovered, nil

	case upstream.StatusAbsent:
		return 0, fmt.Errorf("beacon: slot %d: the mirror upstream returned 404 for a slot at or above its validated "+
			"origin_slot, which spec 7.1 makes impossible -- an archive 404s only below origin; treating it as absence "+
			"would record a hole, so this is a protocol violation, not a missed slot", slot)

	default:
		return 0, fmt.Errorf("beacon: slot %d: upstream returned unknown status %v", slot, res.Status)
	}
}

// stopEarly renders the batch when the archive reports it has not covered slot
// yet. Nothing about slot is recorded, not even coverage: if it is the batch's
// first slot the batch recorded nothing at all (nil last), otherwise the batch
// ends at the slot before it. "Not yet" is never "nothing" (spec 10.1).
func (ix *Indexer) stopEarly(slot, start uint64, order []pending, blobs [][]byte) ([]archclient.Row, [][]byte, *uint64, error) {
	if slot == start {
		return nil, nil, nil, nil
	}
	ix.log.Debug("upstream has not covered this slot yet; ending batch early", "slot", slot)
	lastSlot := slot - 1
	return makeRows(order), blobs, &lastSlot, nil
}

// pending is one slot's blob count before their versioned hashes are known: the
// bytes are fetched from the upstream, but in mirror mode spec 7.2 has the
// archive derive every vh, so a row's identity arrives only with the put.
type pending struct {
	slot  uint64
	count int
}

// makeRows renders the mirror walk's per-slot tally as refs rows with their vhs
// still to fill in (assign fills them from the put). They come out in slot order,
// which spec 5.1 requires of a batch.
func makeRows(order []pending) []archclient.Row {
	rows := make([]archclient.Row, 0, len(order))
	for _, p := range order {
		rows = append(rows, archclient.Row{Slot: p.slot, VHs: make([]schema.VersionedHash, 0, p.count)})
	}
	return rows
}

// assign fills each mirror-mode row's versioned hashes from the put's answer,
// which is where a row's identity comes from there: the bytes were fetched from
// the upstream, but spec 7.2 has the archive derive every vh and accept no
// metadata.
//
// The blobs went up flattened in row order, so the answer comes back in row
// order too, and this walks the two in step.
func assign(rows []archclient.Row, put []archclient.PutBlob) error {
	want := 0
	for i := range rows {
		want += cap(rows[i].VHs)
	}
	if want != len(put) {
		return fmt.Errorf("beacon: fetched %d blobs, archive answered for %d", want, len(put))
	}

	next := 0
	for i := range rows {
		for range cap(rows[i].VHs) {
			rows[i].VHs = append(rows[i].VHs, put[next].VH)
			next++
		}
	}
	return nil
}

// verify checks the archive's derived vhs against the bytes that were sent, in
// mirror mode (spec 10.1: "verify returned vhs cover what we fetched").
//
// # Why this costs a KZG commitment, and why anchored mode does not
//
// The archive derives every vh itself and accepts no metadata (spec 7.2), so the
// put's answer is the only statement of what these blobs are -- a statement about
// the bytes the archive received, not the bytes the upstream sent. If anything in
// flight mangled a blob, the archive would commit to the mangled bytes, hand back
// their vh, and this indexer would record a row pointing at a blob nobody will
// ever ask for. Recomputing the commitment here is the only thing that catches
// it, so it is done for every blob, and a mismatch is fatal: repeating a corrupt
// pipeline is not a fix.
//
// Anchored mode does not pay this, because the block feed already stated each
// blob's vh before any source served it and the source's bytes were checked
// against that vh (trySource); verifyAnchored then only compares the archive's
// answer to those known vhs.
func verify(blobs [][]byte, put []archclient.PutBlob) error {
	if len(blobs) != len(put) {
		return fmt.Errorf("beacon: sent %d blobs, archive answered for %d", len(blobs), len(put))
	}
	for i, b := range blobs {
		vh, err := ingest.VersionedHash(b)
		if err != nil {
			return fmt.Errorf("beacon: blob %d fetched from the upstream is not a valid KZG blob: %w", i, err)
		}
		if vh != put[i].VH {
			return fmt.Errorf("beacon: blob %d was stored as %s but its bytes commit to %s; "+
				"the blob was corrupted between the upstream and the archive",
				i, archclient.VHHex(put[i].VH), archclient.VHHex(vh))
		}
	}
	return nil
}

// rootHex renders a beacon block root the way its API states it.
func rootHex(r [32]byte) string { return "0x" + hex.EncodeToString(r[:]) }
