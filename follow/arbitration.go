package follow

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/go-cid"

	"github.com/blobarchive/bloar/archive"
	"github.com/blobarchive/bloar/server"
)

// FinalizedClaimCandidate is one locally authenticated source's signed claim.
// SourceID is durable operator policy, not a field asserted by Document. The
// caller must verify Document.Pubkey against that source's pinned key before
// arbitration; SelectFinalizedClaim still verifies the document's signature,
// contract, and referenced archive blocks through ClassifyFinalizedClaims.
type FinalizedClaimCandidate struct {
	SourceID string
	Document server.Doc
}

// FinalizedClaimSelection is the unique semantic maximum proved among all
// observations. Equivalent contains every source at that maximum, sorted by
// source ID. Representative is the first of those equivalent transports; its
// source ID is used only to make fetching and provenance deterministic. It is
// not a semantic tie-break between claims.
type FinalizedClaimSelection struct {
	Head           string
	Representative FinalizedClaimCandidate
	Equivalent     []FinalizedClaimCandidate
	// Unavailable records authenticated observations whose content-addressed
	// proof could not be evaluated in this poll. They are excluded from the
	// semantic maximum rather than delaying independently proven progress, and
	// remain retryable at the same source-local revision and digest.
	Unavailable []FinalizedClaimEvaluationFailure
}

// FinalizedClaimPairEvidence preserves both signed source observations and the
// relationship proved between them. Relation is ClaimsIncomparable for an
// unproven order and ClaimRelationInvalid when Conflict carries cryptographic
// evidence of incompatible histories.
type FinalizedClaimPairEvidence struct {
	Left     FinalizedClaimCandidate
	Right    FinalizedClaimCandidate
	Relation ClaimRelation
	Conflict *ArchiveConflictError
}

// FinalizedClaimConflictError reports cryptographic disagreement between
// authenticated sources. Its signed documents and structured
// ArchiveConflictError values are sufficient for a later durable conflict
// latch to preserve evidence without re-running arbitration.
type FinalizedClaimConflictError struct {
	Head      string
	Conflicts []FinalizedClaimPairEvidence
}

func (e *FinalizedClaimConflictError) Error() string {
	pairs := make([]string, 0, len(e.Conflicts))
	for _, conflict := range e.Conflicts {
		pairs = append(pairs, conflict.Left.SourceID+"/"+conflict.Right.SourceID)
	}
	return fmt.Sprintf("follow: finalized head %q has conflicting authenticated source claims (%s)",
		e.Head, strings.Join(pairs, ", "))
}

// Unwrap exposes every underlying ArchiveConflictError to errors.Is/As while
// retaining the complete source-attributed evidence above.
func (e *FinalizedClaimConflictError) Unwrap() []error {
	errs := make([]error, 0, len(e.Conflicts))
	for _, conflict := range e.Conflicts {
		if conflict.Conflict != nil {
			errs = append(errs, conflict.Conflict)
		}
	}
	return errs
}

// FinalizedClaimsIncomparableError means the observations are individually
// valid but no claim proves it includes every other observation. This is not
// cryptographic conflict evidence: independently progressing writers can be
// temporarily incomparable, so callers should retain last-good state and retry
// rather than route this error to the durable conflict latch.
type FinalizedClaimsIncomparableError struct {
	Head         string
	Incomparable []FinalizedClaimPairEvidence
}

func (e *FinalizedClaimsIncomparableError) Error() string {
	pairs := make([]string, 0, len(e.Incomparable))
	for _, pair := range e.Incomparable {
		pairs = append(pairs, pair.Left.SourceID+"/"+pair.Right.SourceID)
	}
	return fmt.Sprintf("follow: finalized head %q has no claim that dominates every authenticated observation (%s)",
		e.Head, strings.Join(pairs, ", "))
}

// FinalizedClaimEvaluationFailure is an ordinary failure to prove one
// relationship. It deliberately remains distinct from both semantic
// incomparability and cryptographic conflict.
type FinalizedClaimEvaluationFailure struct {
	Left  FinalizedClaimCandidate
	Right FinalizedClaimCandidate
	Err   error
}

// FinalizedClaimEvaluationError reports incomplete or invalid evidence. A
// missing immutable block, canceled context, or malformed source document must
// hold last-good state, but must not become durable conflict evidence.
type FinalizedClaimEvaluationError struct {
	Head     string
	Failures []FinalizedClaimEvaluationFailure
}

func (e *FinalizedClaimEvaluationError) Error() string {
	if len(e.Failures) == 0 {
		return fmt.Sprintf("follow: evaluating finalized head %q claims failed", e.Head)
	}
	failure := e.Failures[0]
	return fmt.Sprintf("follow: evaluating finalized head %q claims from %q and %q: %v",
		e.Head, failure.Left.SourceID, failure.Right.SourceID, failure.Err)
}

// Unwrap exposes every ordinary proof failure. In particular, callers can
// still recognize context cancellation and datastore not-found errors.
func (e *FinalizedClaimEvaluationError) Unwrap() []error {
	errs := make([]error, 0, len(e.Failures))
	for _, failure := range e.Failures {
		if failure.Err != nil {
			errs = append(errs, failure.Err)
		}
	}
	return errs
}

// SelectFinalizedClaim finds the unique semantic claim that is equivalent to
// or dominates every authenticated observation for head.
//
// The comparison is a partial order over archive content, not source
// freshness. Input order, updated_at, signer-local revision, source count, and
// majority are never used to choose a claim. All pairwise proofs are evaluated
// in stable source-ID order so returned diagnostics are reproducible. A set of
// equivalent maximal observations is one semantic result; the stable
// representative merely identifies a transport/provenance source.
//
// Cryptographic conflicts take precedence over ordinary proof failures so a
// transient failure cannot conceal evidence that a durable latch must retain.
// An observation whose own root or one of its side-attributed ordering proofs
// is unavailable is unhealthy for this poll and is omitted; it must not delay a
// unique maximum among the remaining proven observations. If no proven
// observation remains, the evaluation error tells the caller to retain
// last-good state and retry.
func SelectFinalizedClaim(ctx context.Context, blocks blockstore.Blockstore, head string, candidates []FinalizedClaimCandidate) (FinalizedClaimSelection, error) {
	type rootAdmission struct {
		head *archive.Head
		err  error
	}
	roots := make(map[string]rootAdmission)
	structure := archive.NewStructureCache()
	return selectFinalizedClaim(ctx, blocks, head, candidates,
		func(ctx context.Context, blocks blockstore.Blockstore, head string, candidate FinalizedClaimCandidate) (*finalizedClaim, error) {
			claim, _, err := loadFinalizedClaimWithHead(ctx, blocks, head, candidate.Document,
				func(loadCtx context.Context, root cid.Cid) (*archive.Head, error) {
					key := root.KeyString()
					if admitted, ok := roots[key]; ok {
						return admitted.head, admitted.err
					}
					loaded, err := archive.Load(loadCtx, archive.Config{
						Blocks: blocks, StructureCache: structure,
					}, root)
					if err == nil {
						_, err = loaded.Enumerate(loadCtx)
					}
					roots[key] = rootAdmission{head: loaded, err: err}
					return loaded, err
				})
			return claim, err
		})
}

type finalizedClaimLoader func(context.Context, blockstore.Blockstore, string, FinalizedClaimCandidate) (*finalizedClaim, error)

// selectFinalizedClaim is the common deterministic partial-order engine. The
// public helper performs a private bounded-DAG admission; source-set polling
// supplies claims which already crossed the follower's shared-cache admission
// boundary, so those same proofs feed arbitration without a second index walk.
func selectFinalizedClaim(
	ctx context.Context,
	blocks blockstore.Blockstore,
	head string,
	candidates []FinalizedClaimCandidate,
	load finalizedClaimLoader,
) (FinalizedClaimSelection, error) {
	if blocks == nil {
		return FinalizedClaimSelection{}, errors.New("follow: selecting finalized claims with a nil blockstore")
	}
	if load == nil {
		return FinalizedClaimSelection{}, errors.New("follow: selecting finalized claims with a nil claim loader")
	}
	if head == "" {
		return FinalizedClaimSelection{}, errors.New("follow: selecting finalized claims for an empty head name")
	}
	if len(candidates) == 0 {
		return FinalizedClaimSelection{}, fmt.Errorf("follow: selecting finalized head %q with no source observations", head)
	}

	ordered := cloneFinalizedCandidates(candidates)
	for i := range ordered {
		if err := validateSourceID(ordered[i].SourceID); err != nil {
			return FinalizedClaimSelection{}, err
		}
	}
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].SourceID < ordered[j].SourceID })
	for i := 1; i < len(ordered); i++ {
		if ordered[i-1].SourceID == ordered[i].SourceID {
			return FinalizedClaimSelection{}, fmt.Errorf("follow: finalized head %q repeats source observation %q", head, ordered[i].SourceID)
		}
	}

	relations := make([][]ClaimRelation, len(ordered))
	for i := range relations {
		relations[i] = make([]ClaimRelation, len(ordered))
	}
	selfValid := make([]bool, len(ordered))
	eligible := make([]bool, len(ordered))
	loaded := make([]*finalizedClaim, len(ordered))
	conflicts := make([]FinalizedClaimPairEvidence, 0)
	failures := make([]FinalizedClaimEvaluationFailure, 0)

	// Load and validate each candidate exactly once. Pairwise projection may still
	// read archive proof paths, but it must not refetch/redecode the same signed
	// Head block O(N) times merely because the roster has other writers.
	for i := range ordered {
		claim, err := load(ctx, blocks, head, ordered[i])
		if err != nil {
			err = attributedClaimEvidence(
				fmt.Errorf("follow: validating finalized claim from %q: %w", ordered[i].SourceID, err), true, true)
			failure := FinalizedClaimEvaluationFailure{
				Left: ordered[i], Right: ordered[i], Err: err,
			}
			failures = append(failures, failure)
			if ctx.Err() != nil {
				return FinalizedClaimSelection{}, &FinalizedClaimEvaluationError{Head: head, Failures: failures}
			}
			continue
		}
		loaded[i] = claim
		relations[i][i] = ClaimsEquivalent
		selfValid[i] = true
		eligible[i] = true
	}

	for i := range ordered {
		for j := i + 1; j < len(ordered); j++ {
			if !selfValid[i] || !selfValid[j] {
				continue
			}
			relation, err := classifyLoadedFinalizedClaims(ctx, blocks, loaded[i], loaded[j])
			if err != nil {
				var conflict *ArchiveConflictError
				if errors.As(err, &conflict) {
					conflicts = append(conflicts, FinalizedClaimPairEvidence{
						Left: ordered[i], Right: ordered[j], Relation: ClaimRelationInvalid, Conflict: conflict,
					})
					continue
				}
				failures = append(failures, FinalizedClaimEvaluationFailure{
					Left: ordered[i], Right: ordered[j], Err: err,
				})
				if ctx.Err() != nil {
					return FinalizedClaimSelection{}, &FinalizedClaimEvaluationError{Head: head, Failures: failures}
				}
				var unavailable *claimEvidenceError
				if errors.As(err, &unavailable) {
					if unavailable.left {
						eligible[i] = false
					}
					if unavailable.right {
						eligible[j] = false
					}
				}
				continue
			}
			relations[i][j] = relation
			relations[j][i] = reverseClaimRelation(relation)
		}
	}

	if len(conflicts) != 0 {
		return FinalizedClaimSelection{}, &FinalizedClaimConflictError{Head: head, Conflicts: conflicts}
	}

	winnerIndexes := make([]int, 0, len(ordered))
	for i := range ordered {
		if !eligible[i] {
			continue
		}
		dominatesAll := true
		for j := range ordered {
			if !eligible[j] {
				continue
			}
			switch relations[i][j] {
			case ClaimsEquivalent, LeftClaimDominates:
			default:
				dominatesAll = false
			}
		}
		if dominatesAll {
			winnerIndexes = append(winnerIndexes, i)
		}
	}

	if len(winnerIndexes) == 0 {
		// A failed pair that does not involve an eventual winner is irrelevant,
		// but without a winner it may be exactly the missing proof that would
		// establish one. Report it as ordinary incomplete evidence rather than
		// incorrectly declaring the successfully compared subset incomparable.
		eligibleSources := make(map[string]struct{}, len(ordered))
		for i := range ordered {
			if eligible[i] {
				eligibleSources[ordered[i].SourceID] = struct{}{}
			}
		}
		blocking := make([]FinalizedClaimEvaluationFailure, 0, len(failures))
		for _, failure := range failures {
			_, left := eligibleSources[failure.Left.SourceID]
			_, right := eligibleSources[failure.Right.SourceID]
			if left && right {
				blocking = append(blocking, failure)
			}
		}
		if len(eligibleSources) == 0 || len(blocking) != 0 {
			if len(blocking) == 0 {
				blocking = failures
			}
			return FinalizedClaimSelection{}, &FinalizedClaimEvaluationError{Head: head, Failures: blocking}
		}
		incomparable := make([]FinalizedClaimPairEvidence, 0)
		for i := range ordered {
			if !eligible[i] {
				continue
			}
			for j := i + 1; j < len(ordered); j++ {
				if !eligible[j] {
					continue
				}
				if relations[i][j] == ClaimsIncomparable {
					incomparable = append(incomparable, FinalizedClaimPairEvidence{
						Left: ordered[i], Right: ordered[j], Relation: ClaimsIncomparable,
					})
				}
			}
		}
		if len(incomparable) == 0 {
			return FinalizedClaimSelection{}, &FinalizedClaimEvaluationError{
				Head: head,
				Failures: []FinalizedClaimEvaluationFailure{{
					Left: ordered[0], Right: ordered[0],
					Err: errors.New("partial-order classifier produced no global dominator and no incomparable pair"),
				}},
			}
		}
		return FinalizedClaimSelection{}, &FinalizedClaimsIncomparableError{Head: head, Incomparable: incomparable}
	}

	// More than one winner is permitted only because equivalent observations
	// form one semantic class. Sorting above makes the representative stable;
	// it does not let a source ID turn a weaker claim into a winner.
	for i := range winnerIndexes {
		for j := i + 1; j < len(winnerIndexes); j++ {
			left, right := winnerIndexes[i], winnerIndexes[j]
			if relations[left][right] != ClaimsEquivalent {
				return FinalizedClaimSelection{}, &FinalizedClaimEvaluationError{
					Head: head,
					Failures: []FinalizedClaimEvaluationFailure{{
						Left: ordered[left], Right: ordered[right],
						Err: errors.New("multiple global dominators are not semantically equivalent"),
					}},
				}
			}
		}
	}
	winners := make([]FinalizedClaimCandidate, 0, len(winnerIndexes))
	for _, index := range winnerIndexes {
		winners = append(winners, ordered[index])
	}

	return FinalizedClaimSelection{
		Head:           head,
		Representative: winners[0],
		Equivalent:     winners,
		Unavailable:    failures,
	}, nil
}

func reverseClaimRelation(relation ClaimRelation) ClaimRelation {
	switch relation {
	case LeftClaimDominates:
		return RightClaimDominates
	case RightClaimDominates:
		return LeftClaimDominates
	default:
		return relation
	}
}

func cloneFinalizedCandidates(candidates []FinalizedClaimCandidate) []FinalizedClaimCandidate {
	cloned := make([]FinalizedClaimCandidate, len(candidates))
	for i, candidate := range candidates {
		cloned[i] = FinalizedClaimCandidate{SourceID: candidate.SourceID, Document: clonePublicationDocument(candidate.Document)}
	}
	return cloned
}

func clonePublicationDocument(document server.Doc) server.Doc {
	cloned := document
	cloned.Multiaddrs = append([]string(nil), document.Multiaddrs...)
	cloned.Heads = append([]server.HeadEntry(nil), document.Heads...)
	if document.ArchiveID != nil {
		archiveID := *document.ArchiveID
		cloned.ArchiveID = &archiveID
	}
	if document.Revision != nil {
		revision := *document.Revision
		cloned.Revision = &revision
	}
	for i := range cloned.Heads {
		cloneOptionalUint64 := func(value *uint64) *uint64 {
			if value == nil {
				return nil
			}
			cloned := *value
			return &cloned
		}
		cloned.Heads[i].SyncedTo = cloneOptionalUint64(document.Heads[i].SyncedTo)
		cloned.Heads[i].WindowStart = cloneOptionalUint64(document.Heads[i].WindowStart)
		cloned.Heads[i].SourceFinalizedSlot = cloneOptionalUint64(document.Heads[i].SourceFinalizedSlot)
		cloned.Heads[i].HandoffSyncedTo = cloneOptionalUint64(document.Heads[i].HandoffSyncedTo)
	}
	return cloned
}
