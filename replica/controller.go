package replica

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/pebble/v2"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
)

var (
	// ErrOwnershipDrift means the named Kubo pins and the durable controller
	// ledger no longer agree. The controller fails closed and never guesses
	// which pin an unrelated client intended to own.
	ErrOwnershipDrift = errors.New("replica: Kubo pin ownership drift")
	// ErrGenerationUnprotected means a follower checkpoint is not covered by an
	// exact recursive anchor pin and therefore must not be resumed.
	ErrGenerationUnprotected = errors.New("replica: generation is not recursively pinned")
)

// CleanupError reports safe over-retention after an otherwise successful
// transition or recovery. The authoritative current generation is already
// durable; callers should surface the cleanup debt but must not roll back or
// suppress announcements for the committed generation.
type CleanupError struct {
	Err error
}

func (e *CleanupError) Error() string { return fmt.Sprintf("replica: safe cleanup debt: %v", e.Err) }
func (e *CleanupError) Unwrap() error { return e.Err }

// PinStatus is the exact status of one CID. Indirect is not sufficient: a
// generation is committed only after its anchor is a recursive pin root.
type PinStatus struct {
	Recursive bool
	Name      string
}

// PinProgress is one bounded progress observation from a long initial
// recursive pin. Update does not expose progress in Kubo 0.42.
type PinProgress struct {
	Blocks uint64
	Bytes  uint64
}

// Backend is the narrow Kubo surface the replica controller may mutate. It has
// deliberately no block removal, repo GC, pin enumeration, key, or publish
// method.
type Backend interface {
	PutBlock(context.Context, blocks.Block) error
	PinStatus(context.Context, cid.Cid) (PinStatus, bool, error)
	NamedRecursivePins(context.Context, string) ([]cid.Cid, error)
	PinAddRecursive(context.Context, cid.Cid, string, func(PinProgress)) error
	PinUpdateRecursive(context.Context, cid.Cid, cid.Cid, bool) error
	PinRemoveRecursive(context.Context, cid.Cid) error
}

// Config constructs a Controller.
type Config struct {
	KV        *pebble.DB
	Backend   Backend
	ReplicaID string
	// PinName is the exact Kubo name reserved for generation anchors. Empty
	// derives "bloar-replica/v1/<replica-id>".
	PinName string
	Now     func() time.Time
	// Progress receives bounded initial-pin progress. It must not block.
	Progress func(PinProgress)
	// StateChanged runs after a durable controller-state write. It must not
	// block. Metrics use it to expose Pending during a long recursive pin rather
	// than only after Prepare returns.
	StateChanged func()
}

// Controller coordinates one replica identity. Calls are serialized across
// long pin operations so a newer observation cannot repeatedly cancel and
// starve an initial full-archive pin.
type Controller struct {
	kv        *pebble.DB
	backend   Backend
	replicaID string
	pinName   string
	now       func() time.Time
	progress  func(PinProgress)
	changed   func()
	mu        sync.Mutex
}

// Status is the bounded durable state exposed to operators.
type Status struct {
	Current                 cid.Cid
	Pending                 cid.Cid
	CurrentGeneration       *Generation
	PendingGeneration       *Generation
	CurrentOwnership        Ownership
	PendingOwnership        Ownership
	CurrentAt               time.Time
	PendingAt               time.Time
	Cleanup                 int
	CleanupOldestRetainedAt time.Time
}

// New returns a controller without making an RPC.
func New(cfg Config) (*Controller, error) {
	if cfg.KV == nil {
		return nil, errors.New("replica: Config.KV must not be nil")
	}
	if cfg.Backend == nil {
		return nil, errors.New("replica: Config.Backend must not be nil")
	}
	probe := Generation{ReplicaID: cfg.ReplicaID, UpdatedAt: time.Unix(0, 0), Heads: []Head{{Name: "probe", Root: testValidationCID()}}}
	if _, err := probe.Normalize(); err != nil {
		return nil, err
	}
	pinName := strings.TrimSpace(cfg.PinName)
	if pinName == "" {
		pinName = "bloar-replica/v1/" + cfg.ReplicaID
	}
	if len(pinName) == 0 || len(pinName) > 128 || strings.IndexByte(pinName, 0) >= 0 {
		return nil, errors.New("replica: pin name must be 1-128 bytes and contain no NUL")
	}
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	return &Controller{
		kv: cfg.KV, backend: cfg.Backend, replicaID: cfg.ReplicaID,
		pinName: pinName, now: now, progress: cfg.Progress, changed: cfg.StateChanged,
	}, nil
}

// testValidationCID is only a defined, syntactically valid placeholder used to
// run Generation's replica-ID validation in New. It is never persisted or sent.
func testValidationCID() cid.Cid {
	return cid.MustParse("bafkreigh2akiscaildcw453iz5v2u6xwhmstdlq5jqrc5w5zkqze3mqk3i")
}

// Prepare makes generation recursively durable in Kubo before the follower is
// allowed to commit its trust checkpoint. The prior committed anchor remains
// pinned throughout. Repeating Prepare after a crash is idempotent.
func (c *Controller) Prepare(ctx context.Context, generation Generation) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}

	normalized, block, err := c.candidate(generation)
	if err != nil {
		return err
	}
	if err := c.backend.PutBlock(ctx, block); err != nil {
		return fmt.Errorf("replica: writing generation anchor %s: %w", block.Cid(), err)
	}

	state, err := c.loadState()
	if err != nil {
		return err
	}
	if state.Current != nil && state.Current.Anchor.Equals(block.Cid()) {
		if err := c.requirePinned(ctx, *state.Current); err != nil {
			return err
		}
		// A newer candidate may have been prepared before authority returned to
		// the committed generation. Retire only a pending anchor whose durable
		// ledger says we own it; a borrowed operator pin remains untouched.
		if state.Pending != nil && !state.Pending.Anchor.Equals(block.Cid()) {
			if state.Pending.Ownership == OwnershipOwned {
				state.Cleanup = appendCleanup(state.Cleanup, *state.Pending)
			}
			state.Pending = nil
			if err := saveControllerState(c.kv, state); err != nil {
				return err
			}
			c.notifyStateChanged()
		}
		return cleanupResult(c.cleanupLocked(ctx, &state))
	}

	status, pinned, err := c.backend.PinStatus(ctx, block.Cid())
	if err != nil {
		return fmt.Errorf("replica: checking candidate anchor %s: %w", block.Cid(), err)
	}
	ownership := OwnershipOwned
	known := retainedByAnchor(state, block.Cid())
	if known != nil {
		ownership = known.Ownership
	}
	if !pinned && known != nil && known.Ownership == OwnershipBorrowed {
		// The operator's pin disappeared between Prepare and Commit. Recreating
		// it under our reserved name would silently convert borrowed ownership
		// into controller ownership and makes a concurrent operator re-pin
		// impossible to classify safely. Keep the old committed generation and
		// fail closed until that borrowed pin returns or a newer generation
		// supersedes this pending candidate.
		return fmt.Errorf("%w: borrowed candidate anchor %s disappeared; refusing to recreate it as controller-owned", ErrOwnershipDrift, block.Cid())
	}
	if pinned {
		if !status.Recursive {
			// Upgrading an operator's exact/direct pin to recursive changes their
			// retention policy. The anchor collision is improbable, but ownership
			// must be correct rather than probabilistic.
			return fmt.Errorf("%w: candidate anchor %s is already pinned non-recursively", ErrOwnershipDrift, block.Cid())
		}
		switch {
		case known != nil && ownership == OwnershipOwned && status.Name != c.pinName:
			return fmt.Errorf("%w: known owned candidate anchor %s pin name is %q, want %q", ErrOwnershipDrift, block.Cid(), status.Name, c.pinName)
		case known != nil && ownership == OwnershipBorrowed:
			// The exact recursive pin predated us. Its name may legitimately be
			// changed by its owner; we still never remove or rename it.
		case known == nil && status.Name == c.pinName:
			// Never recover ownership merely from Kubo. The Pebble ledger also
			// holds the replay floors; adopting an orphan pin after losing it would
			// turn retention state into rollback authority.
			return fmt.Errorf("%w: candidate anchor %s uses reserved pin name %q without a durable ownership record", ErrOwnershipDrift, block.Cid(), c.pinName)
		case known == nil:
			ownership = OwnershipBorrowed
		}
	}
	pending := retainedGeneration{
		Generation: normalized,
		Anchor:     block.Cid(),
		Ownership:  ownership,
		At:         c.now().UTC().Truncate(time.Second),
	}
	if state.Pending != nil && state.Pending.Anchor.Equals(pending.Anchor) {
		// A failed or interrupted recursive pin is retried against the same
		// durable intent. Preserve its first-seen time so transition age remains
		// meaningful across retries and process restarts instead of resetting on
		// every source poll.
		pending.At = state.Pending.At
	}
	if state.Pending != nil && !state.Pending.Anchor.Equals(pending.Anchor) && state.Pending.Ownership == OwnershipOwned {
		// Drain prior safe debt before consuming another bounded slot. A failed
		// removal may be retried below, but it cannot be allowed to grow the
		// durable list past its hard bound and wedge every future transition.
		_ = c.cleanupLocked(ctx, &state)
		if len(state.Cleanup) >= maxGenerationHeads+1 {
			return fmt.Errorf("replica: cleanup debt has reached its %d-anchor limit; refusing another pending generation", maxGenerationHeads+1)
		}
		state.Cleanup = appendCleanup(state.Cleanup, *state.Pending)
	}
	// Commit will retire the current owned anchor after the follower checkpoint
	// advances. Reserve that durable cleanup slot now, before Kubo is allowed to
	// pin the candidate. Otherwise a controller already at the cleanup bound
	// could protect a new checkpoint and then be structurally unable to record
	// the old anchor it is allowed to remove.
	if state.Current != nil && state.Current.Ownership == OwnershipOwned &&
		!state.Current.Anchor.Equals(pending.Anchor) &&
		!cleanupContains(state.Cleanup, state.Current.Anchor) &&
		len(state.Cleanup) >= maxGenerationHeads+1 {
		_ = c.cleanupLocked(ctx, &state)
		if len(state.Cleanup) >= maxGenerationHeads+1 {
			return fmt.Errorf("replica: cleanup debt has reached its %d-anchor limit; refusing a transition without room for the current generation", maxGenerationHeads+1)
		}
	}
	state.Cleanup = removeCleanup(state.Cleanup, pending.Anchor)
	state.Pending = &pending
	if err := saveControllerState(c.kv, state); err != nil {
		return err
	}
	c.notifyStateChanged()

	if !pinned || !status.Recursive {
		switch {
		case state.Current != nil && state.Current.Ownership == OwnershipOwned:
			currentStatus, currentPinned, err := c.backend.PinStatus(ctx, state.Current.Anchor)
			if err != nil {
				return fmt.Errorf("replica: checking current anchor %s: %w", state.Current.Anchor, err)
			}
			if !currentPinned || !currentStatus.Recursive || currentStatus.Name != c.pinName {
				return fmt.Errorf("%w: current owned anchor %s is not the exact named recursive pin %q",
					ErrOwnershipDrift, state.Current.Anchor, c.pinName)
			}
			// unpin=false is load-bearing: the old committed generation remains
			// available until the follower checkpoint and controller ledger commit.
			if err := c.backend.PinUpdateRecursive(ctx, state.Current.Anchor, block.Cid(), false); err != nil {
				return c.transitionFailure(ctx, &state, fmt.Errorf("replica: pinning generation %s from %s: %w", block.Cid(), state.Current.Anchor, err))
			}
		default:
			if err := c.backend.PinAddRecursive(ctx, block.Cid(), c.pinName, c.observeProgress); err != nil {
				return c.transitionFailure(ctx, &state, fmt.Errorf("replica: recursively pinning generation %s: %w", block.Cid(), err))
			}
		}
	}

	status, pinned, err = c.backend.PinStatus(ctx, block.Cid())
	if err != nil {
		return c.transitionFailure(ctx, &state, fmt.Errorf("replica: verifying candidate anchor %s: %w", block.Cid(), err))
	}
	if !pinned || !status.Recursive {
		return c.transitionFailure(ctx, &state, fmt.Errorf("%w: candidate anchor %s is not an exact recursive pin", ErrGenerationUnprotected, block.Cid()))
	}
	if ownership == OwnershipOwned && status.Name != c.pinName {
		return c.transitionFailure(ctx, &state, fmt.Errorf("%w: candidate anchor %s pin name is %q, want %q", ErrOwnershipDrift, block.Cid(), status.Name, c.pinName))
	}
	return cleanupResult(c.cleanupLocked(ctx, &state))
}

// Commit records a successfully checkpointed generation, then removes only a
// superseded anchor that the controller ledger says it owns. Cleanup failure is
// safe over-retention and remains durable debt for Recover or the next commit.
func (c *Controller) Commit(ctx context.Context, generation Generation) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	normalized, block, err := c.candidate(generation)
	if err != nil {
		return err
	}
	state, err := c.loadState()
	if err != nil {
		return err
	}
	if state.Pending == nil || !state.Pending.Anchor.Equals(block.Cid()) || !state.Pending.Generation.Equal(normalized) {
		if state.Current != nil && state.Current.Anchor.Equals(block.Cid()) && state.Current.Generation.Equal(normalized) {
			return cleanupResult(c.cleanupLocked(ctx, &state))
		}
		return errors.New("replica: committing a generation that is not the durable pending candidate")
	}
	if err := c.requirePinned(ctx, *state.Pending); err != nil {
		return err
	}

	previous := state.Current
	committed := *state.Pending
	if previous != nil && previous.Ownership == OwnershipOwned &&
		!previous.Anchor.Equals(committed.Anchor) &&
		!cleanupContains(state.Cleanup, previous.Anchor) &&
		len(state.Cleanup) >= maxGenerationHeads+1 {
		// Prepare normally reserves this slot. Keep the defensive check here for
		// state written by an older build or recovered at the exact hard bound.
		_ = c.cleanupLocked(ctx, &state)
		if len(state.Cleanup) >= maxGenerationHeads+1 {
			return fmt.Errorf("replica: cleanup debt has reached its %d-anchor limit; refusing to commit without room for the prior generation", maxGenerationHeads+1)
		}
	}
	committed.At = c.now().UTC().Truncate(time.Second)
	state.Current = &committed
	state.Pending = nil
	if previous != nil && !previous.Anchor.Equals(committed.Anchor) && previous.Ownership == OwnershipOwned {
		state.Cleanup = appendCleanup(state.Cleanup, *previous)
	}
	if err := saveControllerState(c.kv, state); err != nil {
		return err
	}
	c.notifyStateChanged()
	if err := c.cleanupLocked(ctx, &state); err != nil {
		return &CleanupError{Err: err}
	}
	return nil
}

// Protects is the restart backstop: a follower checkpoint may be exposed only
// when an exact committed or crash-pending generation containing that head is
// still recursively pinned.
func (c *Controller) Protects(ctx context.Context, head Head) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	state, err := c.loadState()
	if err != nil {
		return err
	}
	for _, retained := range []*retainedGeneration{state.Current, state.Pending} {
		if retained == nil || !generationContains(retained.Generation, head) {
			continue
		}
		return c.requirePinned(ctx, *retained)
	}
	return fmt.Errorf("%w: no committed or pending anchor contains head %q at root %s", ErrGenerationUnprotected, head.Name, head.Root)
}

// ProtectsAll proves that one retained generation covers the complete set of
// follower checkpoints which will be exposed after a restart. An empty set is
// meaningful: it must be backed by an exact retained zero-head generation, so
// an authenticated all-head withdrawal cannot be reconstructed from the mere
// absence of checkpoints. Protection is a
// property of the pinned DAG roots and manifest tips, not of the anti-replay
// synced-to floors. Resume may safely repair a floor upward after proving the
// root's encoded coverage, without changing the blocks the anchor must retain.
//
// Testing heads independently is still insufficient: a corrupt mixed set could
// otherwise be assembled from Current and Pending even though no single anchor
// protects all of its roots. Current wins when both generations protect the
// same complete root set.
func (c *Controller) ProtectsAll(ctx context.Context, heads []Head) (Generation, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	state, err := c.loadState()
	if err != nil {
		return Generation{}, err
	}
	for _, retained := range []*retainedGeneration{state.Current, state.Pending} {
		if retained == nil || !generationProtectsAll(retained.Generation, heads) {
			continue
		}
		if err := c.requirePinned(ctx, *retained); err != nil {
			return Generation{}, err
		}
		return cloneGeneration(retained.Generation), nil
	}
	return Generation{}, fmt.Errorf("%w: no single committed or pending anchor contains all %d checkpoint heads", ErrGenerationUnprotected, len(heads))
}

// Recover validates named-pin ownership and retries safe cleanup. It never
// adopts an orphan named pin as authority: losing Pebble also loses follower
// replay floors, so guessing from Kubo would create a rollback path.
func (c *Controller) Recover(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	state, err := c.loadState()
	if err != nil {
		return err
	}
	named, err := c.backend.NamedRecursivePins(ctx, c.pinName)
	if err != nil {
		return fmt.Errorf("replica: listing exact named anchors: %w", err)
	}
	allowed := make(map[string]struct{})
	for _, retained := range stateRetained(state) {
		if retained.Ownership == OwnershipOwned {
			allowed[retained.Anchor.KeyString()] = struct{}{}
		}
	}
	for _, anchor := range named {
		if _, ok := allowed[anchor.KeyString()]; !ok {
			return fmt.Errorf("%w: named recursive anchor %s has no durable ownership record", ErrOwnershipDrift, anchor)
		}
	}
	if state.Current != nil {
		if err := c.requirePinned(ctx, *state.Current); err != nil {
			return err
		}
	}
	if state.Pending != nil {
		_, pinned, err := c.backend.PinStatus(ctx, state.Pending.Anchor)
		if err != nil {
			return fmt.Errorf("replica: checking pending anchor %s during recovery: %w", state.Pending.Anchor, err)
		}
		if pinned {
			if err := c.requirePinned(ctx, *state.Pending); err != nil {
				return err
			}
		}
	}
	if err := c.cleanupLocked(ctx, &state); err != nil {
		return &CleanupError{Err: err}
	}
	return nil
}

// Status returns durable anchor state without making an RPC.
func (c *Controller) Status() (Status, error) {
	// State is one Pebble value replaced with a synced Set, so a concurrent
	// read sees either complete version without sharing the long-operation
	// mutex held across a recursive Kubo pin.
	state, err := c.loadState()
	if err != nil {
		return Status{}, err
	}
	status := Status{Cleanup: len(state.Cleanup)}
	if state.Current != nil {
		status.Current = state.Current.Anchor
		generation := cloneGeneration(state.Current.Generation)
		status.CurrentGeneration = &generation
		status.CurrentOwnership = state.Current.Ownership
		status.CurrentAt = state.Current.At
	}
	if state.Pending != nil {
		status.Pending = state.Pending.Anchor
		generation := cloneGeneration(state.Pending.Generation)
		status.PendingGeneration = &generation
		status.PendingOwnership = state.Pending.Ownership
		status.PendingAt = state.Pending.At
	}
	for _, retained := range state.Cleanup {
		if status.CleanupOldestRetainedAt.IsZero() || retained.At.Before(status.CleanupOldestRetainedAt) {
			status.CleanupOldestRetainedAt = retained.At
		}
	}
	return status, nil
}

// AuditCurrent is a read-only liveness proof for the generation the follower
// currently serves. It intentionally ignores incomplete Pending intent and
// cleanup debt. If a concurrent commit swaps Current between the Pebble read
// and Kubo proof, it retries once against the new durable generation so a safe
// old-anchor cleanup does not create a false alarm.
func (c *Controller) AuditCurrent(ctx context.Context) error {
	var prior cid.Cid
	var priorErr error
	for attempt := 0; attempt < 2; attempt++ {
		state, err := c.loadState()
		if err != nil {
			return err
		}
		if state.Current == nil {
			return fmt.Errorf("%w: no committed generation", ErrGenerationUnprotected)
		}
		if attempt > 0 && state.Current.Anchor.Equals(prior) {
			return priorErr
		}
		prior = state.Current.Anchor
		if err := c.requirePinned(ctx, *state.Current); err == nil {
			return nil
		} else {
			priorErr = err
			if attempt == 1 {
				return err
			}
		}
	}
	return fmt.Errorf("%w: current generation changed during audit", ErrGenerationUnprotected)
}

// AuditGeneration is a read-only liveness proof for a generation previously
// returned by ProtectsAll or successfully committed. It accepts either Current
// or crash-Pending because a follower checkpoint can durably advance in the
// small window before the controller promotion completes.
//
// It deliberately does not take the transition mutex. Prepare holds that mutex
// across a potentially multi-hour Kubo recursive pin; readiness must continue
// auditing the generation currently being served during that catch-up. The
// controller state is one atomically replaced Pebble value, and active-version
// arbitration in the runtime prevents a superseded audit from overwriting a
// newer activation result.
func (c *Controller) AuditGeneration(ctx context.Context, generation Generation) error {
	normalized, block, err := c.candidate(generation)
	if err != nil {
		return err
	}
	state, err := c.loadState()
	if err != nil {
		return err
	}
	for _, retained := range []*retainedGeneration{state.Current, state.Pending} {
		if retained == nil || !retained.Anchor.Equals(block.Cid()) || !retained.Generation.Equal(normalized) {
			continue
		}
		return c.requirePinned(ctx, *retained)
	}
	return fmt.Errorf("%w: active generation %s is no longer current or pending", ErrGenerationUnprotected, block.Cid())
}

// ProtectingGeneration returns the durable generation whose exact recursive
// pin protects head. Current wins over pending, matching Protects. The returned
// value is a copy and is safe for callers to retain.
func (s Status) ProtectingGeneration(head Head) (Generation, bool) {
	for _, generation := range []*Generation{s.CurrentGeneration, s.PendingGeneration} {
		if generation != nil && generationContains(*generation, head) {
			copy := *generation
			copy.Heads = slices.Clone(generation.Heads)
			return copy, true
		}
	}
	return Generation{}, false
}

func (c *Controller) candidate(g Generation) (Generation, blocks.Block, error) {
	g.ReplicaID = c.replicaID
	normalized, err := g.Normalize()
	if err != nil {
		return Generation{}, nil, err
	}
	block, err := normalized.Block()
	return normalized, block, err
}

func (c *Controller) loadState() (controllerState, error) {
	state, err := loadControllerState(c.kv)
	if err != nil {
		return controllerState{}, err
	}
	for _, retained := range stateRetained(state) {
		if retained.Generation.ReplicaID != c.replicaID {
			return controllerState{}, fmt.Errorf("replica: controller state belongs to replica %q, configured replica is %q", retained.Generation.ReplicaID, c.replicaID)
		}
	}
	return state, nil
}

func (c *Controller) observeProgress(progress PinProgress) {
	if c.progress != nil {
		c.progress(progress)
	}
}

func (c *Controller) notifyStateChanged() {
	if c.changed != nil {
		c.changed()
	}
}

func (c *Controller) requirePinned(ctx context.Context, retained retainedGeneration) error {
	status, pinned, err := c.backend.PinStatus(ctx, retained.Anchor)
	if err != nil {
		return fmt.Errorf("replica: checking retained anchor %s: %w", retained.Anchor, err)
	}
	if !pinned || !status.Recursive {
		return fmt.Errorf("%w: anchor %s", ErrGenerationUnprotected, retained.Anchor)
	}
	if retained.Ownership == OwnershipOwned && status.Name != c.pinName {
		return fmt.Errorf("%w: owned anchor %s pin name is %q, want %q", ErrOwnershipDrift, retained.Anchor, status.Name, c.pinName)
	}
	return nil
}

func (c *Controller) cleanupLocked(ctx context.Context, state *controllerState) error {
	if len(state.Cleanup) == 0 {
		return nil
	}
	remaining := state.Cleanup[:0]
	var errs []error
	for _, stale := range state.Cleanup {
		status, pinned, err := c.backend.PinStatus(ctx, stale.Anchor)
		if err != nil {
			remaining = append(remaining, stale)
			errs = append(errs, fmt.Errorf("replica: checking cleanup anchor %s: %w", stale.Anchor, err))
			continue
		}
		if !pinned || !status.Recursive || status.Name != c.pinName {
			// Missing is already clean; a renamed pin is no longer demonstrably
			// ours and is deliberately abandoned rather than removed.
			continue
		}
		if err := c.backend.PinRemoveRecursive(ctx, stale.Anchor); err != nil {
			remaining = append(remaining, stale)
			errs = append(errs, fmt.Errorf("replica: removing superseded anchor %s: %w", stale.Anchor, err))
		}
	}
	state.Cleanup = slices.Clone(remaining)
	if err := saveControllerState(c.kv, *state); err != nil {
		errs = append(errs, err)
	} else {
		c.notifyStateChanged()
	}
	return errors.Join(errs...)
}

func cleanupResult(err error) error {
	if err == nil {
		return nil
	}
	return &CleanupError{Err: err}
}

func (c *Controller) transitionFailure(ctx context.Context, state *controllerState, primary error) error {
	if cleanupErr := c.cleanupLocked(ctx, state); cleanupErr != nil {
		// This is not a CleanupError: the pin transition itself failed, so callers
		// must not reinterpret the joined result as a successful commit with only
		// safe over-retention debt.
		return errors.Join(primary, fmt.Errorf("replica: retrying prior cleanup after failed transition: %w", cleanupErr))
	}
	return primary
}

func appendCleanup(cleanup []retainedGeneration, candidate retainedGeneration) []retainedGeneration {
	for _, existing := range cleanup {
		if existing.Anchor.Equals(candidate.Anchor) {
			return cleanup
		}
	}
	return append(cleanup, candidate)
}

func cleanupContains(cleanup []retainedGeneration, anchor cid.Cid) bool {
	return slices.ContainsFunc(cleanup, func(retained retainedGeneration) bool {
		return retained.Anchor.Equals(anchor)
	})
}

func removeCleanup(cleanup []retainedGeneration, anchor cid.Cid) []retainedGeneration {
	result := cleanup[:0]
	for _, retained := range cleanup {
		if !retained.Anchor.Equals(anchor) {
			result = append(result, retained)
		}
	}
	return slices.Clone(result)
}

func generationContains(g Generation, want Head) bool {
	for _, head := range g.Heads {
		if head.Name == want.Name && head.Root.Equals(want.Root) && head.Manifest.Equals(want.Manifest) && head.SyncedTo == want.SyncedTo {
			return true
		}
	}
	return false
}

func generationProtectsAll(g Generation, wants []Head) bool {
	if len(g.Heads) != len(wants) {
		return false
	}
	for _, want := range wants {
		if !slices.ContainsFunc(g.Heads, func(head Head) bool {
			return head.Name == want.Name && head.Root.Equals(want.Root) && head.Manifest.Equals(want.Manifest)
		}) {
			return false
		}
	}
	return true
}

func cloneGeneration(g Generation) Generation {
	copy := g
	copy.Heads = slices.Clone(g.Heads)
	return copy
}

func stateRetained(state controllerState) []retainedGeneration {
	result := slices.Clone(state.Cleanup)
	if state.Current != nil {
		result = append(result, *state.Current)
	}
	if state.Pending != nil {
		result = append(result, *state.Pending)
	}
	return result
}

func retainedByAnchor(state controllerState, anchor cid.Cid) *retainedGeneration {
	for _, retained := range []*retainedGeneration{state.Current, state.Pending} {
		if retained != nil && retained.Anchor.Equals(anchor) {
			return retained
		}
	}
	for i := range state.Cleanup {
		if state.Cleanup[i].Anchor.Equals(anchor) {
			return &state.Cleanup[i]
		}
	}
	return nil
}
