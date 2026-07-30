package unfinalized

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/blobarchive/bloar/index/archclient"
	"github.com/blobarchive/bloar/index/upstream"
	"github.com/blobarchive/bloar/ingest"
	"github.com/blobarchive/bloar/metrics"
	"github.com/blobarchive/bloar/schema"
	"github.com/blobarchive/bloar/server"
)

// BlobClient is the beacon-shaped read boundary used for blob bytes. The
// trusted HeaderSource decides which versioned hashes exist; a BlobClient is
// only an untrusted byte provider whose answer is checked against those hashes.
type BlobClient interface {
	Blobs(context.Context, uint64, []schema.VersionedHash) (upstream.Result, error)
}

// BlobSource is one byte provider, in fallback order.
type BlobSource struct {
	Client BlobClient
	// Name is used only in diagnostics. Empty names become source-0, source-1,
	// and so on.
	Name string
}

// Archive is the authenticated archive API the tracker needs. *archclient.Client
// implements it. Keeping the boundary small makes the CAS and retry rules
// deterministic to test without an HTTP server.
type Archive interface {
	Head(context.Context, string) (archclient.HeadInfo, error)
	GenerationState(context.Context, string) (server.GenerationStatus, error)
	PostGeneration(context.Context, string, server.GenerationRequest) (server.GenerationResponse, error)
	PutBlobs(context.Context, [][]byte) ([]archclient.PutBlob, error)
}

// Config defines one bounded optimistic tracker.
type Config struct {
	Headers HeaderSource
	Sources []BlobSource
	Archive Archive

	Head         string
	HandoffHead  string
	WindowSlots  uint64
	OverlapSlots uint64
	MaxPutBlobs  int
	PollInterval time.Duration
	Logger       *slog.Logger
	Metrics      *metrics.Metrics
}

const (
	defaultTrackerMaxPutBlobs = 64
	defaultTrackerPoll        = 12 * time.Second
)

// ErrHandoffChanged marks a generation conflict which the tracker independently
// proved was caused by the finalized handoff changing after the candidate was
// built. The candidate remains refused; Run retains the last-good generation
// and rebuilds from fresh handoff and beacon anchors.
var ErrHandoffChanged = errors.New("unfinalized: finalized handoff changed during generation construction")

// HandoffChangedError preserves both the semantic retry class and the archive's
// original conflict for diagnostics and errors.As callers.
type HandoffChangedError struct {
	ObservedRoot     string
	ObservedSyncedTo uint64
	CurrentRoot      string
	CurrentSyncedTo  *uint64
	Conflict         error
}

func (e *HandoffChangedError) Error() string {
	current := "empty"
	if e.CurrentSyncedTo != nil {
		current = fmt.Sprintf("%s at %d", e.CurrentRoot, *e.CurrentSyncedTo)
	}
	return fmt.Sprintf("%s: observed %s at %d, current %s: %v",
		ErrHandoffChanged, e.ObservedRoot, e.ObservedSyncedTo, current, e.Conflict)
}

func (e *HandoffChangedError) Unwrap() []error {
	return []error{ErrHandoffChanged, e.Conflict}
}

// Tracker repeatedly replaces one mutable archive generation with a complete,
// root-anchored view of the optimistic beacon tip. It does not mutate ALL.
type Tracker struct {
	cfg      Config
	log      *slog.Logger
	mu       sync.Mutex
	previous *Snapshot
}

// StepResult describes one complete pass. Updated is false when the selected
// source root and handoff-derived window already match the durable generation.
type StepResult struct {
	Updated     bool
	Generation  uint64
	WindowStart uint64
	SyncedTo    uint64
	Root        string
}

// New returns a validated Tracker.
func New(cfg Config) (*Tracker, error) {
	switch {
	case cfg.Headers == nil:
		return nil, errors.New("unfinalized: Config.Headers is required")
	case cfg.Archive == nil:
		return nil, errors.New("unfinalized: Config.Archive is required")
	case cfg.Head == "":
		return nil, errors.New("unfinalized: Config.Head is required")
	case cfg.HandoffHead == "":
		return nil, errors.New("unfinalized: Config.HandoffHead is required")
	case cfg.Head == cfg.HandoffHead:
		return nil, errors.New("unfinalized: mutable head cannot authorize its own handoff")
	case cfg.WindowSlots == 0:
		return nil, errors.New("unfinalized: Config.WindowSlots must be positive")
	case cfg.OverlapSlots > cfg.WindowSlots:
		return nil, fmt.Errorf("unfinalized: overlap %d exceeds window bound %d", cfg.OverlapSlots, cfg.WindowSlots)
	case len(cfg.Sources) == 0:
		return nil, errors.New("unfinalized: Config.Sources needs at least one blob source")
	case len(cfg.Sources) > 2:
		return nil, fmt.Errorf("unfinalized: Config.Sources takes at most two sources, got %d", len(cfg.Sources))
	case cfg.MaxPutBlobs < 0:
		return nil, fmt.Errorf("unfinalized: Config.MaxPutBlobs is %d, must be positive", cfg.MaxPutBlobs)
	case cfg.PollInterval < 0:
		return nil, fmt.Errorf("unfinalized: Config.PollInterval is %s, must be positive", cfg.PollInterval)
	}
	for i, source := range cfg.Sources {
		if source.Client == nil {
			return nil, fmt.Errorf("unfinalized: Config.Sources[%d].Client is required", i)
		}
	}
	if cfg.MaxPutBlobs == 0 {
		cfg.MaxPutBlobs = defaultTrackerMaxPutBlobs
	}
	if cfg.PollInterval == 0 {
		cfg.PollInterval = defaultTrackerPoll
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.DiscardHandler)
	}
	return &Tracker{cfg: cfg, log: cfg.Logger}, nil
}

// Run polls until ctx is cancelled. A finalized handoff that is empty, behind,
// or temporarily inconsistent retains the currently selected generation and
// waits. The same is true for an execution-optimistic source and a handoff that
// changes during a generation CAS: both are expected, explicitly typed races,
// and rebuilding them in-process preserves metrics and avoids supervisor churn.
// Every unclassified failure remains terminal so a safety or protocol fault is
// never hidden behind an unbounded retry loop.
func (t *Tracker) Run(ctx context.Context) error {
	t.log.Info("unfinalized indexer starting", "head", t.cfg.Head,
		"handoff_head", t.cfg.HandoffHead, "window_slots", t.cfg.WindowSlots,
		"overlap_slots", t.cfg.OverlapSlots, "sources", len(t.cfg.Sources),
		"poll_interval", t.cfg.PollInterval)
	for {
		result, err := t.Step(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			t.cfg.Metrics.IndexArchiveAvailable(t.cfg.Head, !archclient.IsUnavailable(err))
			switch {
			case errors.Is(err, ErrHandoffBlocked):
				t.log.Warn("retaining the selected provisional generation until finalized handoff advances", "error", err)
			case errors.Is(err, ErrSnapshotChanged):
				t.log.Debug("discarding optimistic snapshot changed during construction", "error", err)
				continue
			default:
				reason, retry := trackerRetryReason(err)
				if !retry {
					return err
				}
				t.log.Warn("keeping the unfinalized tracker alive, retaining any selected provisional generation, and retrying",
					"reason", reason, "error", err)
				t.cfg.Metrics.UnfinalizedRetry(t.cfg.Head, reason)
			}
		} else {
			t.cfg.Metrics.IndexArchiveAvailable(t.cfg.Head, true)
		}
		if err == nil && result.Updated {
			t.log.Info("unfinalized generation selected", "head", t.cfg.Head,
				"generation", result.Generation, "window_start", result.WindowStart,
				"synced_to", result.SyncedTo, "root", result.Root)
			// A newer optimistic slot may already be available. Re-read without an
			// artificial delay, just like the finalized indexers after progress.
			continue
		}

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(t.cfg.PollInterval):
		}
	}
}

// Step constructs and, when changed, selects one complete generation.
func (t *Tracker) Step(ctx context.Context) (StepResult, error) {
	handoff, err := t.cfg.Archive.Head(ctx, t.cfg.HandoffHead)
	if err != nil {
		return StepResult{}, fmt.Errorf("unfinalized: reading handoff head %q: %w", t.cfg.HandoffHead, err)
	}
	if handoff.Kind != "" && handoff.Kind != server.FinalizedMonotonic {
		return StepResult{}, fmt.Errorf("unfinalized: handoff head %q is %q, must be %q",
			t.cfg.HandoffHead, handoff.Kind, server.FinalizedMonotonic)
	}
	if handoff.SyncedTo == nil {
		return StepResult{}, fmt.Errorf("%w: handoff head %q has no finalized coverage", ErrHandoffBlocked, t.cfg.HandoffHead)
	}

	state, err := t.cfg.Archive.GenerationState(ctx, t.cfg.Head)
	if err != nil {
		return StepResult{}, fmt.Errorf("unfinalized: reading mutable generation %q: %w", t.cfg.Head, err)
	}
	if state.Kind != server.UnfinalizedMutable {
		return StepResult{}, fmt.Errorf("unfinalized: archive reports head %q as %q, want %q",
			t.cfg.Head, state.Kind, server.UnfinalizedMutable)
	}

	windowStart := WindowStart(handoff.OriginSlot, *handoff.SyncedTo, t.cfg.OverlapSlots)
	if result, unchanged := t.unchangedSelectedSource(ctx, state, handoff.Root, *handoff.SyncedTo, windowStart); unchanged {
		return result, nil
	}
	snapshot, err := Build(ctx, t.cfg.Headers, windowStart, t.cfg.WindowSlots)
	if err != nil {
		return StepResult{}, err
	}
	if *handoff.SyncedTo > snapshot.Finalized.Slot {
		return StepResult{}, fmt.Errorf("%w: handoff %q covers slot %d but the trusted source finalizes only through %d",
			ErrHandoffBlocked, t.cfg.HandoffHead, *handoff.SyncedTo, snapshot.Finalized.Slot)
	}
	if err := StableHead(ctx, t.cfg.Headers, snapshot.Head.Root); err != nil {
		return StepResult{}, err
	}

	req := generationRequest(state.Generation, snapshot, handoff.Root, *handoff.SyncedTo)
	if selectedGenerationMatches(state.GenerationState, req, t.cfg.HandoffHead, handoff.Root, *handoff.SyncedTo) {
		if state.Exposed && state.Published {
			return t.observeSelected(snapshot, StepResult{Generation: state.Generation, WindowStart: state.WindowStart,
				SyncedTo: state.SyncedTo, Root: state.Root}), nil
		}
		// The selector commit is authoritative, but the response may have been
		// interrupted before exposure or publication. Re-submit the exact prior
		// CAS request so the server heals those owed post-commit steps instead of
		// treating the matching durable record as completion forever.
		req.ExpectedGeneration = state.Generation - 1
	}

	response, err := t.cfg.Archive.PostGeneration(ctx, t.cfg.Head, req)
	if err == nil {
		return t.observeSelected(snapshot, stepResult(response)), nil
	}
	var missing *archclient.MissingBlobsError
	if !errors.As(err, &missing) {
		return StepResult{}, fmt.Errorf("unfinalized: selecting generation: %w",
			t.classifyGenerationConflict(ctx, req, err))
	}
	if missing.CurrentGeneration != nil && *missing.CurrentGeneration != state.Generation {
		return StepResult{}, fmt.Errorf("unfinalized: generation moved from %d to %d while resolving missing blobs: %w",
			state.Generation, *missing.CurrentGeneration, err)
	}
	if err := t.supplyMissing(ctx, snapshot, missing.VHs); err != nil {
		return StepResult{}, err
	}
	// Blob retrieval may be slow. Never select the old candidate if the source
	// changed while its bytes were being fetched; the stored blobs are harmless
	// and the next pass will build the new canonical generation.
	if err := StableHead(ctx, t.cfg.Headers, snapshot.Head.Root); err != nil {
		return StepResult{}, err
	}
	response, err = t.cfg.Archive.PostGeneration(ctx, t.cfg.Head, req)
	if err != nil {
		return StepResult{}, fmt.Errorf("unfinalized: retrying exact generation after supplying blobs: %w",
			t.classifyGenerationConflict(ctx, req, err))
	}
	return t.observeSelected(snapshot, stepResult(response)), nil
}

func trackerRetryReason(err error) (string, bool) {
	var optimistic *upstream.ExecutionOptimisticError
	switch {
	case errors.As(err, &optimistic):
		return metrics.UnfinalizedRetryExecutionOptimistic, true
	case errors.Is(err, ErrHandoffChanged):
		return metrics.UnfinalizedRetryHandoffChanged, true
	case archclient.IsUnavailable(err):
		return metrics.UnfinalizedRetryArchiveUnavailable, true
	default:
		return "", false
	}
}

// classifyGenerationConflict does not infer semantics from a 409 message. It
// independently re-reads the finalized handoff after a generation conflict and
// marks the error retryable only when the exact root/coverage observed by the
// refused request changed. A generation conflict with an unchanged handoff is
// left terminal for operator inspection.
func (t *Tracker) classifyGenerationConflict(
	ctx context.Context,
	req server.GenerationRequest,
	err error,
) error {
	var conflict *archclient.ConflictError
	if !errors.As(err, &conflict) {
		return err
	}
	current, headErr := t.cfg.Archive.Head(ctx, t.cfg.HandoffHead)
	if headErr != nil {
		return err
	}
	if current.Root == req.ObservedHandoffRoot && current.SyncedTo != nil &&
		*current.SyncedTo == req.ObservedHandoffSyncedTo {
		return err
	}
	return &HandoffChangedError{
		ObservedRoot:     req.ObservedHandoffRoot,
		ObservedSyncedTo: req.ObservedHandoffSyncedTo,
		CurrentRoot:      current.Root,
		CurrentSyncedTo:  current.SyncedTo,
		Conflict:         err,
	}
}

// unchangedSelectedSource is the cheap caught-up path. A bounded optimistic
// snapshot can contain roughly two epochs of root-addressed headers, so
// rebuilding it merely to discover that neither source anchor moved makes a
// short live-tip poll interval unnecessarily expensive. The last snapshot was
// already ancestry-checked by Build and StableHead; content addressing means
// equal roots identify the same headers. Re-read finalized before head (the
// same ordering as Build), and skip the walk only when every source and archive
// witness still names that exact selected generation.
//
// Any absence, mismatch, or read error deliberately falls through to Build.
// That preserves Build's validation and diagnostics and prevents this latency
// optimization from becoming a second, weaker admission path.
func (t *Tracker) unchangedSelectedSource(
	ctx context.Context,
	state server.GenerationStatus,
	handoffRoot string,
	handoffSyncedTo uint64,
	windowStart uint64,
) (StepResult, bool) {
	t.mu.Lock()
	previous := t.previous
	t.mu.Unlock()
	if previous == nil || !state.Exposed || !state.Published {
		return StepResult{}, false
	}

	finalized, ok, err := t.cfg.Headers.FinalizedHeader(ctx)
	if err != nil || !ok || !finalized.Finalized {
		return StepResult{}, false
	}
	head, ok, err := t.cfg.Headers.Head(ctx)
	if err != nil || !ok || finalized.Slot > head.Slot {
		return StepResult{}, false
	}

	if previous.WindowStart != windowStart || previous.Head.Root != head.Root || previous.Head.Slot != head.Slot ||
		previous.Finalized.Root != finalized.Root || previous.Finalized.Slot != finalized.Slot ||
		state.Generation == 0 || state.WindowStart != windowStart || state.SyncedTo != head.Slot || state.SourceHeadSlot != head.Slot ||
		state.SourceHeadRoot != beaconRoot(head.Root) || state.SourceFinalizedSlot != finalized.Slot ||
		state.SourceFinalizedRoot != beaconRoot(finalized.Root) ||
		state.ObservedHandoffRoot != handoffRoot || state.ObservedHandoffSyncedTo != handoffSyncedTo ||
		state.HandoffHead != t.cfg.HandoffHead || state.HandoffRoot != handoffRoot ||
		state.HandoffSyncedTo != handoffSyncedTo {
		return StepResult{}, false
	}

	return StepResult{
		Generation:  state.Generation,
		WindowStart: state.WindowStart,
		SyncedTo:    state.SyncedTo,
		Root:        state.Root,
	}, true
}

func (t *Tracker) observeSelected(snapshot Snapshot, result StepResult) StepResult {
	var (
		depth uint64
		reorg bool
	)
	t.mu.Lock()
	if t.previous != nil {
		depth, reorg = observedReorg(*t.previous, snapshot)
	}
	copy := snapshot
	t.previous = &copy
	t.mu.Unlock()

	if reorg {
		t.cfg.Metrics.UnfinalizedReorg(t.cfg.Head, depth)
	}
	t.cfg.Metrics.UnfinalizedSnapshot(t.cfg.Head, snapshot.Head.Slot, snapshot.Finalized.Slot,
		snapshot.WindowStart, result.Generation)
	return result
}

// observedReorg compares only overlapping retained ancestry. If the previous
// tip has fallen entirely below the new window, continuity is unobservable and
// this function deliberately reports nothing rather than inventing a reorg.
// When the windows overlap but their common ancestor is deeper than both, the
// returned depth is a conservative retained-window lower bound.
func observedReorg(previous, next Snapshot) (uint64, bool) {
	if previous.Head.Root == next.Head.Root {
		return 0, false
	}
	if previous.Head.Slot < next.WindowStart {
		return 0, false
	}
	nextRoots := make(map[[32]byte]uint64, len(next.Blocks))
	for _, block := range next.Blocks {
		nextRoots[block.Root] = block.Slot
	}
	if _, extends := nextRoots[previous.Head.Root]; extends {
		return 0, false
	}
	for i := len(previous.Blocks) - 1; i >= 0; i-- {
		block := previous.Blocks[i]
		if _, common := nextRoots[block.Root]; common {
			return previous.Head.Slot - block.Slot, true
		}
	}
	overlapStart := previous.WindowStart
	if next.WindowStart > overlapStart {
		overlapStart = next.WindowStart
	}
	return previous.Head.Slot - overlapStart + 1, true
}

func generationRequest(generation uint64, snapshot Snapshot, handoffRoot string, handoffSyncedTo uint64) server.GenerationRequest {
	rows := make([]server.GenerationRow, 0, len(snapshot.Rows))
	for _, row := range snapshot.Rows {
		vhs := make([]string, 0, len(row.VHs))
		for _, vh := range row.VHs {
			vhs = append(vhs, archclient.VHHex(vh))
		}
		rows = append(rows, server.GenerationRow{Slot: row.Slot, VersionedHashes: vhs})
	}
	return server.GenerationRequest{
		ExpectedGeneration:      generation,
		WindowStart:             snapshot.WindowStart,
		SyncedTo:                snapshot.SyncedTo,
		Rows:                    rows,
		SourceHeadRoot:          beaconRoot(snapshot.Head.Root),
		SourceFinalizedSlot:     snapshot.Finalized.Slot,
		SourceFinalizedRoot:     beaconRoot(snapshot.Finalized.Root),
		ObservedHandoffRoot:     handoffRoot,
		ObservedHandoffSyncedTo: handoffSyncedTo,
	}
}

func selectedGenerationMatches(state server.GenerationState, req server.GenerationRequest, handoffHead, handoffRoot string, handoffSyncedTo uint64) bool {
	return state.Generation > 0 && state.WindowStart == req.WindowStart && state.SyncedTo == req.SyncedTo &&
		state.SourceHeadRoot == req.SourceHeadRoot && state.SourceFinalizedSlot == req.SourceFinalizedSlot &&
		state.SourceFinalizedRoot == req.SourceFinalizedRoot && state.HandoffHead == handoffHead &&
		state.ObservedHandoffRoot == req.ObservedHandoffRoot && state.ObservedHandoffSyncedTo == req.ObservedHandoffSyncedTo &&
		handoffRoot == req.ObservedHandoffRoot && handoffSyncedTo == req.ObservedHandoffSyncedTo
}

func stepResult(response server.GenerationResponse) StepResult {
	return StepResult{Updated: !response.NoOp, Generation: response.Generation,
		WindowStart: response.WindowStart, SyncedTo: response.SyncedTo, Root: response.Root}
}

func beaconRoot(root [32]byte) string { return fmt.Sprintf("0x%x", root) }

// supplyMissing fetches exactly the server-reported missing content. Each source
// is nevertheless checked against the snapshot's complete slot row, so every
// whole-slot response obeys the same positional, KZG-anchored acceptance rule.
func (t *Tracker) supplyMissing(ctx context.Context, snapshot Snapshot, missing []schema.VersionedHash) error {
	if len(missing) == 0 {
		return errors.New("unfinalized: archive returned a missing-blobs conflict with an empty missing_blobs list")
	}
	want := make(map[schema.VersionedHash]bool, len(missing))
	for _, vh := range missing {
		want[vh] = true
	}
	for vh := range want {
		if _, ok := snapshot.Locations[vh]; !ok {
			return fmt.Errorf("unfinalized: archive reports missing blob %s which is absent from the candidate snapshot", archclient.VHHex(vh))
		}
	}

	byVH := make(map[schema.VersionedHash][]byte, len(want))
	for _, row := range snapshot.Rows {
		needed := false
		for _, vh := range row.VHs {
			needed = needed || want[vh]
		}
		if !needed {
			continue
		}
		blobs, err := t.fetchSlot(ctx, row.Slot, row.VHs)
		if err != nil {
			return err
		}
		for i, vh := range row.VHs {
			if want[vh] {
				byVH[vh] = blobs[i]
			}
		}
	}

	// Preserve candidate row/commitment order while deduplicating content. This
	// makes PUT responses straightforward to verify and keeps retries stable.
	orderedVHs := make([]schema.VersionedHash, 0, len(want))
	seen := make(map[schema.VersionedHash]bool, len(want))
	for _, row := range snapshot.Rows {
		for _, vh := range row.VHs {
			if want[vh] && !seen[vh] {
				seen[vh] = true
				orderedVHs = append(orderedVHs, vh)
			}
		}
	}
	if len(orderedVHs) != len(want) {
		return fmt.Errorf("unfinalized: resolved %d of %d missing versioned hashes", len(orderedVHs), len(want))
	}
	for start := 0; start < len(orderedVHs); start += t.cfg.MaxPutBlobs {
		end := min(start+t.cfg.MaxPutBlobs, len(orderedVHs))
		batchVHs := orderedVHs[start:end]
		batch := make([][]byte, len(batchVHs))
		for i, vh := range batchVHs {
			batch[i] = byVH[vh]
		}
		put, err := t.cfg.Archive.PutBlobs(ctx, batch)
		if err != nil {
			return fmt.Errorf("unfinalized: putting %d missing blobs: %w", len(batch), err)
		}
		if len(put) != len(batchVHs) {
			return fmt.Errorf("unfinalized: archive acknowledged %d of %d missing blobs", len(put), len(batchVHs))
		}
		for i := range put {
			if put[i].VH != batchVHs[i] {
				return fmt.Errorf("unfinalized: archive stored blob %d as %s, want %s", i,
					archclient.VHHex(put[i].VH), archclient.VHHex(batchVHs[i]))
			}
		}
	}
	return nil
}

func (t *Tracker) fetchSlot(ctx context.Context, slot uint64, expected []schema.VersionedHash) ([][]byte, error) {
	reasons := make([]string, 0, len(t.cfg.Sources))
	for i, source := range t.cfg.Sources {
		name := source.Name
		if name == "" {
			name = fmt.Sprintf("source-%d", i)
		}
		result, err := source.Client.Blobs(ctx, slot, nil)
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			reasons = append(reasons, name+": "+err.Error())
			continue
		}
		if result.Status != upstream.StatusFound {
			reasons = append(reasons, fmt.Sprintf("%s: status %v", name, result.Status))
			continue
		}
		if len(result.Blobs) != len(expected) {
			reasons = append(reasons, fmt.Sprintf("%s: returned %d blobs, snapshot names %d", name, len(result.Blobs), len(expected)))
			continue
		}
		valid := true
		for j, blob := range result.Blobs {
			vh, err := ingest.VersionedHash(blob)
			if err != nil || vh != expected[j] {
				reasons = append(reasons, fmt.Sprintf("%s: blob %d does not commit to %s", name, j, archclient.VHHex(expected[j])))
				valid = false
				break
			}
		}
		if valid {
			return slices.Clone(result.Blobs), nil
		}
	}
	return nil, fmt.Errorf("unfinalized: slot %d: no source served %d snapshot blobs anchored (%v)",
		slot, len(expected), reasons)
}
