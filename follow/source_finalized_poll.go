package follow

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"

	"github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/go-cid"

	"github.com/blobarchive/bloar/archive"
	"github.com/blobarchive/bloar/server"
)

// finalizedHeadPlanResult is one head-local proof outcome. Results are merged
// in configured head-name order after every worker finishes, so concurrency
// changes neither semantic selection nor diagnostic ordering.
type finalizedHeadPlanResult struct {
	name                 string
	plan                 adoptPlan
	hasPlan              bool
	winner               *resolved
	prior                checkpoint
	conflict             *FinalizedClaimConflictError
	continuityConflict   *ArchiveConflictError
	conflictParticipants []FinalizedClaimCandidate
	admittedSources      []string
	rejectedSources      []string
	incomparable         bool
	comparable           bool
	latch                *ConflictRecord
	err                  error
	fatalErr             error
}

// preflightFinalizedHeads gives every finalized head an independent proof
// deadline. A slow or unavailable proof for one configured head must not consume
// the shared deadline before a later healthy head gets to run. The roster bounds
// worker count, and the indexed result slice preserves deterministic merge order.
func (f *Follower) preflightFinalizedHeads(ctx context.Context, bySource map[string]*resolved) []finalizedHeadPlanResult {
	names := make([]string, 0, len(f.cfg.Heads))
	for _, name := range f.Names() {
		if f.expectedKind(name) == server.FinalizedMonotonic {
			names = append(names, name)
		}
	}
	return runFinalizedHeadWorkers(ctx, names, func(headCtx context.Context, name string) finalizedHeadPlanResult {
		if latch, active := f.conflictLatch(name); active {
			return finalizedHeadPlanResult{name: name, latch: &latch}
		}
		return f.preflightFinalizedHead(headCtx, name, bySource)
	})
}

func runFinalizedHeadWorkers(ctx context.Context, names []string, work func(context.Context, string) finalizedHeadPlanResult) []finalizedHeadPlanResult {
	results := make([]finalizedHeadPlanResult, len(names))
	var group sync.WaitGroup
	for index, name := range names {
		index, name := index, name
		group.Add(1)
		go func() {
			defer group.Done()
			headCtx, cancel := context.WithTimeout(ctx, docTimeout)
			defer cancel()
			results[index] = work(headCtx, name)
			results[index].name = name
		}()
	}
	group.Wait()
	return results
}

func (f *Follower) preflightFinalizedHead(ctx context.Context, name string, bySource map[string]*resolved) finalizedHeadPlanResult {
	result := finalizedHeadPlanResult{name: name}
	type sourceCandidate struct {
		sourceID    string
		candidate   *resolved
		entry       server.HeadEntry
		observation FinalizedClaimCandidate
	}
	candidates := make([]sourceCandidate, 0, len(bySource))
	authorizedSources := 0
	for _, source := range f.sources {
		if !source.allows(name) {
			continue
		}
		authorizedSources++
		candidate := bySource[source.cfg.ID]
		if candidate == nil {
			continue
		}
		entry, published := documentHead(candidate.doc, name)
		if !published || entry.SyncedTo == nil {
			// A source's omission or explicit empty line cannot withdraw a
			// finalized head selected from an independent authority.
			continue
		}
		candidates = append(candidates, sourceCandidate{
			sourceID:    source.cfg.ID,
			candidate:   candidate,
			entry:       entry,
			observation: FinalizedClaimCandidate{SourceID: source.cfg.ID, Document: candidate.doc},
		})
	}
	if len(candidates) == 0 {
		return result
	}

	// Read the durable checkpoint once. Both the floorless typed-manifest gate
	// and the later continuity comparison must be bound to this exact snapshot;
	// separate reads could validate against one floor and adopt against another.
	prior, hasPrior, err := f.state.checkpoint(name)
	if err != nil {
		// A durable-state read failure is not attributable to one publication
		// source. Keep it distinct so the caller fails the poll globally.
		result.fatalErr = err
		return result
	}
	_, _, _, hasManifestFloor, err := f.floors(name, prior, hasPrior)
	if err != nil {
		result.fatalErr = err
		return result
	}

	observations := make([]FinalizedClaimCandidate, 0, len(candidates))
	claims := make(map[string]*finalizedClaim, len(bySource))
	admittedHeads := make(map[string]*archive.Head, len(bySource))
	admissionFailures := make([]FinalizedClaimEvaluationFailure, 0)
	type rootAdmission struct {
		head *archive.Head
		err  error
	}
	// Multiple independent writers commonly publish the exact same root. Share
	// one complete bounded walk per root within this head-local poll, including
	// its failure, rather than multiplying a potentially large read by source
	// count. The cross-poll StructureCache remains bound to the local collection
	// generation and re-establishes block presence after every collection
	// boundary; this map lasts only for this arbitration.
	roots := make(map[string]rootAdmission)
	loadRoot := func(loadCtx context.Context, root cid.Cid) (*archive.Head, error) {
		key := root.KeyString()
		if admitted, ok := roots[key]; ok {
			return admitted.head, admitted.err
		}
		head, err := f.loadWithPointer(loadCtx, name, root)
		roots[key] = rootAdmission{head: head, err: err}
		return head, err
	}

	reject := func(observation FinalizedClaimCandidate, err error) bool {
		admissionFailures = append(admissionFailures, FinalizedClaimEvaluationFailure{
			Left: observation, Right: observation, Err: err,
		})
		result.rejectedSources = append(result.rejectedSources, observation.SourceID)
		if ctx.Err() == nil {
			return false
		}
		result.err = fmt.Errorf("follow: admitting finalized head %q observations: %w", name,
			&FinalizedClaimEvaluationError{Head: name, Failures: admissionFailures})
		return true
	}

	for _, source := range candidates {
		// With no durable manifest floor, generic pairwise ancestry has no
		// trusted stopping point. Validate the complete typed chain before this
		// source can become arbitration evidence or an admitted generation.
		if !hasManifestFloor {
			tip, manifestErr := parseManifestTip(source.entry)
			if manifestErr == nil {
				manifestErr = f.checkManifestAncestryWithPointer(ctx, name, tip, cid.Undef, false)
			}
			if manifestErr != nil {
				manifestErr = fmt.Errorf("follow: source %q first manifest chain for head %q is not admissible: %w",
					source.sourceID, name, manifestErr)
				if reject(source.observation, manifestErr) {
					return result
				}
				continue
			}
		}

		// Manifest admission and root admission are independent boundaries. A
		// well-formed typed chain does not make an attacker-shaped index safe,
		// and a canonical index does not make an arbitrary manifest admissible.
		claim, admitted, err := loadFinalizedClaimWithHead(ctx, f.blocks, name, source.candidate.doc, loadRoot)
		if err != nil {
			err = attributedClaimEvidence(
				fmt.Errorf("follow: bounded admission of finalized claim from %q: %w", source.sourceID, err), true, true)
			if reject(source.observation, err) {
				return result
			}
			continue
		}
		observations = append(observations, source.observation)
		result.admittedSources = append(result.admittedSources, source.sourceID)
		claims[source.sourceID] = claim
		admittedHeads[source.sourceID] = admitted
	}
	if len(observations) == 0 {
		if len(admissionFailures) != 0 {
			result.err = fmt.Errorf("follow: admitting finalized head %q observations: %w", name,
				&FinalizedClaimEvaluationError{Head: name, Failures: admissionFailures})
		}
		return result
	}

	selection, err := selectFinalizedClaim(ctx, f.blocks, name, observations,
		func(_ context.Context, _ blockstore.Blockstore, _ string, candidate FinalizedClaimCandidate) (*finalizedClaim, error) {
			claim := claims[candidate.SourceID]
			if claim == nil {
				return nil, fmt.Errorf("follow: finalized source %q has no bounded admitted claim", candidate.SourceID)
			}
			return claim, nil
		})
	if err != nil {
		var conflict *FinalizedClaimConflictError
		if errors.As(err, &conflict) {
			// Preserve the outer arbitration evidence: it binds each conflict
			// endpoint to its authenticated source. Unwrapping directly to an
			// ArchiveConflictError here would lose that attribution.
			result.conflict = conflict
			byID := make(map[string]FinalizedClaimCandidate)
			for _, pair := range conflict.Conflicts {
				byID[pair.Left.SourceID] = pair.Left
				byID[pair.Right.SourceID] = pair.Right
			}
			for _, sourceID := range slices.Sorted(maps.Keys(byID)) {
				result.conflictParticipants = append(result.conflictParticipants, byID[sourceID])
			}
		}
		var incomparable *FinalizedClaimsIncomparableError
		result.incomparable = errors.As(err, &incomparable)
		result.err = fmt.Errorf("follow: arbitrating finalized head %q: %w", name, err)
		return result
	}
	selection.Unavailable = append(admissionFailures, selection.Unavailable...)
	for _, unavailable := range selection.Unavailable {
		f.log.Warn("publication source proof unavailable for this head; omitting its claim for this poll",
			"head", name, "source", unavailable.Left.SourceID, "peer_source", unavailable.Right.SourceID,
			"err", unavailable.Err)
	}
	closedSelection := len(selection.Unavailable) == 0 && len(observations) == authorizedSources
	winner := bySource[selection.Representative.SourceID]
	entry, ok := documentHead(winner.doc, name)
	if !ok || entry.SyncedTo == nil {
		result.err = fmt.Errorf("follow: finalized arbiter selected source %q without a covered head %q", selection.Representative.SourceID, name)
		return result
	}

	if hasPrior {
		winnerClaim := claims[selection.Representative.SourceID]
		winnerHead := admittedHeads[selection.Representative.SourceID]
		priorClaim, priorHead, hasBaseline, err := finalizedCheckpointClaimWithHead(
			ctx, f.blocks, *f.cfg.ExpectedArchiveID, name, prior,
			func(loadCtx context.Context, root cid.Cid) (*archive.Head, error) {
				if winnerHead != nil && winnerHead.Root() == root {
					return winnerHead, nil
				}
				return f.load(loadCtx, name, root)
			})
		if err != nil {
			result.err = fmt.Errorf("follow: admitting durable last-good for finalized head %q: %w", name, err)
			return result
		}
		var relation ClaimRelation
		if hasBaseline {
			relation, err = classifyLoadedFinalizedClaims(ctx, f.blocks, winnerClaim, priorClaim)
		}
		if err != nil {
			var conflict *ArchiveConflictError
			if errors.As(err, &conflict) {
				// This is a fresh source claim against the durable selected
				// checkpoint, not a simultaneous multi-source arbitration pair.
				result.winner = winner
				result.prior = prior
				result.continuityConflict = conflict
				result.conflictParticipants = append(result.conflictParticipants, selection.Equivalent...)
			}
			result.err = fmt.Errorf("follow: comparing finalized head %q with durable last-good: %w", name, err)
			return result
		}
		if hasBaseline {
			switch relation {
			case LeftClaimDominates:
				// A proven append-only advance is eligible below.
				result.comparable = closedSelection
			case ClaimsEquivalent:
				result.comparable = closedSelection
				// A selected v4 already records this semantic claim. Preserve its
				// provenance when an equivalent peer comes and goes, and restore the
				// exact durable claim if startup left the registry dark.
				if prior.version == checkpointVersionV4 && prior.selected {
					plan, err := f.preflightSourceCheckpointWithHead(ctx, name, prior, priorHead)
					if err != nil {
						result.err = fmt.Errorf("follow: restoring equivalent durable finalized head %q: %w", name, err)
						return result
					}
					if planHasEffect(plan) {
						result.plan, result.hasPlan = plan, true
					}
					return result
				}
			case RightClaimDominates:
				result.comparable = closedSelection
				// A stale source cannot replace the ahead durable claim. A v4
				// checkpoint can still restore a dark post-restart registry.
				if prior.version == checkpointVersionV4 && prior.selected {
					plan, err := f.preflightSourceCheckpointWithHead(ctx, name, prior, priorHead)
					if err != nil {
						result.err = fmt.Errorf("follow: restoring ahead durable finalized head %q: %w", name, err)
						return result
					}
					if planHasEffect(plan) {
						result.plan, result.hasPlan = plan, true
					}
				}
				return result
			case ClaimsIncomparable:
				result.incomparable = true
				result.err = fmt.Errorf("follow: finalized head %q winning observed claim is incomparable with the durable last-good claim", name)
				return result
			default:
				result.err = fmt.Errorf("follow: finalized head %q continuity returned invalid relation %s", name, relation)
				return result
			}
		}
	}
	// A selected source-set claim with no prior content baseline is a closed
	// semantic result too. Later adoption/configuration errors must not leave a
	// stale transient-incomparability gauge set.
	result.comparable = closedSelection

	plan, err := f.preflightEntryWithHead(ctx, entry, winner, admittedHeads[selection.Representative.SourceID])
	if err != nil {
		result.err = fmt.Errorf("follow: adopting arbitrated finalized head %q: %w", name, err)
		return result
	}
	if planHasEffect(plan) {
		result.plan, result.hasPlan, result.winner = plan, true, winner
	}
	return result
}
