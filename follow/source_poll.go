package follow

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/cockroachdb/pebble/v2"

	"github.com/blobarchive/bloar/metrics"
	"github.com/blobarchive/bloar/p2p"
	"github.com/blobarchive/bloar/replica"
	"github.com/blobarchive/bloar/server"
)

func (f *Follower) pollSourceSetAdmission(ctx context.Context) error {
	// A source-set poll is a topology-discovery boundary. Boxo sessions retain
	// peer affinity across requests, which is useful inside a DAG walk but can
	// otherwise strand a follower after its learned writer disappears while an
	// unlearned writer remains connected. New wraps every embedded source-set
	// SessionSource with the lazy refresher; external-retention followers have no
	// embedded session and deliberately skip this hook.
	f.refreshSourceSessions()
	results := f.resolveSources(ctx)
	f.dialSourceResults(ctx, results)
	// Resolution can reveal and authenticate new publication multiaddrs. Rotate
	// again after those dials so claim validation and the fetch pass cannot reuse
	// a session opened concurrently before the newly admitted peers existed.
	f.refreshSourceSessions()
	var errs []error
	f.transition.Lock()
	if err := f.admitSourceResults(ctx, results); err != nil {
		errs = append(errs, err)
	}
	f.transition.Unlock()
	return errors.Join(errs...)
}

func (f *Follower) refreshSourceSessions() {
	if refresher, ok := f.cfg.Sessions.(p2p.SessionRefresher); ok {
		refresher.RefreshSessions()
	}
}

// dialSourceResults treats authenticated multiaddrs as bounded fetch hints, not
// part of the durable transition. All sources dial concurrently under one
// poll-wide budget so an unreachable roster cannot serialize N FetchTimeouts
// while quarantine, Resume, and GC publication wait on transition.
func (f *Follower) dialSourceResults(ctx context.Context, results []sourceResolveResult) {
	dialCtx, cancel := context.WithTimeout(ctx, f.cfg.FetchTimeout)
	defer cancel()
	var group sync.WaitGroup
	for _, result := range results {
		if result.candidate == nil {
			continue
		}
		addresses := append([]string(nil), result.candidate.doc.Multiaddrs...)
		group.Add(1)
		go func() {
			defer group.Done()
			f.dial(dialCtx, addresses)
		}()
	}
	group.Wait()
}

type sourcePlanClosureResult struct {
	plan adoptPlan
	err  error
}

// preflightSourcePlanClosures walks independent retained closures concurrently
// and returns them in the original deterministic plan order. Fresh publication
// gives each head its own proof budget; durable recovery inherits only the
// caller deadline because rebuilding a trusted archive may legitimately take
// longer than one network poll budget.
func (f *Follower) preflightSourcePlanClosures(ctx context.Context, plans []adoptPlan, bounded bool) []sourcePlanClosureResult {
	results := make([]sourcePlanClosureResult, len(plans))
	var group sync.WaitGroup
	for i := range plans {
		i := i
		group.Add(1)
		go func() {
			defer group.Done()
			plan := plans[i]
			planCtx := ctx
			cancel := func() {}
			if bounded {
				planCtx, cancel = context.WithTimeout(ctx, docTimeout)
			}
			defer cancel()
			results[i] = sourcePlanClosureResult{plan: plan}
			results[i].err = f.protectAdoptionClosure(planCtx, &results[i].plan, false)
		}()
	}
	group.Wait()
	return results
}

// admitSourceResults performs one poll-wide transition. It first persists
// authenticated IPNS sequence observations (which are channel facts independent
// of selection), then arbitrates every head and commits all selected checkpoints
// plus source-local publication floors as one serving transaction.
func (f *Follower) admitSourceResults(ctx context.Context, results []sourceResolveResult) error {
	headErrs := f.conflictLatchErrors()
	results, err := f.commitSourceObservations(results)
	if err != nil {
		return errors.Join(errors.Join(headErrs...), err)
	}

	bySource := make(map[string]*resolved, len(results))
	var sourceErrs []error
	for _, result := range results {
		if result.err != nil {
			sourceErrs = append(sourceErrs, result.err)
			var equivocation *authorityEquivocationError
			if errors.As(result.err, &equivocation) {
				f.quarantineSourceMutableEquivocationLocked(result.source, equivocation)
				continue
			}
		}
		if result.candidate == nil {
			continue
		}
		// Resolution checked this floor outside transition. Recheck it here so a
		// concurrent poll cannot commit an older source generation afterward.
		if err := f.sourceFreshnessRefusal(result.source, result.candidate); err != nil {
			sourceErrs = append(sourceErrs, err)
			var equivocation *authorityEquivocationError
			if errors.As(err, &equivocation) {
				f.quarantineSourceMutableEquivocationLocked(result.source, equivocation)
				continue
			}
			continue
		}
		if f.sourcePublishesOwnedMutable(result.source, result.candidate.doc) {
			if err := f.validateOverlayHandoffs(result.candidate.doc, true); err != nil {
				// A mutable boundary is authenticated by this source's document,
				// not by the source set as a whole. Reject the complete source
				// generation, but keep independently authenticated writers
				// eligible for durable recovery and finalized arbitration.
				sourceErrs = append(sourceErrs, fmt.Errorf("follow: source %q mutable boundary: %w", result.source.cfg.ID, err))
				continue
			}
		}
		if err := f.refuseQuarantinedSourceDocument(result.source, result.candidate.doc); err != nil {
			sourceErrs = append(sourceErrs, err)
			continue
		}
		bySource[result.source.cfg.ID] = result.candidate
	}
	allSourcesUnavailable := len(bySource) == 0
	for _, err := range sourceErrs {
		f.log.Warn("publication source unavailable for this poll; retaining its last good claims", "err", err)
	}
	if allSourcesUnavailable {
		f.recordSourceResultMetrics(bySource)
		// Recovery is independent of network availability. It still runs before
		// the transport failure is returned, but there can be no fresh conflict
		// evidence to persist in this branch.
		recoveryErrs, err := f.restoreDarkSourceCheckpoints(ctx)
		if err != nil {
			return errors.Join(errors.Join(sourceErrs...), errors.Join(headErrs...), err)
		}
		headErrs = append(headErrs, recoveryErrs...)
		return errors.Join(errors.Join(sourceErrs...), errors.Join(headErrs...))
	}

	planByName := make(map[string]adoptPlan)

	finalizedResults := f.preflightFinalizedHeads(ctx, bySource)
	f.omitSourcesWithOnlyRejectedFinalizedClaims(bySource, finalizedResults)
	// A source whose only covered claims failed the bounded DAG boundary is not
	// an admitted publication generation. Do not advance its replay floor,
	// invoke its callback, or prepare an empty external-retention generation.
	// Durable recovery still runs, independently of fresh source health.
	if len(bySource) == 0 {
		var finalizedErrs []error
		for _, result := range finalizedResults {
			finalizedErrs = append(finalizedErrs, result.err, result.fatalErr)
		}
		f.recordSourceResultMetrics(bySource)
		recoveryErrs, err := f.restoreDarkSourceCheckpoints(ctx)
		if err != nil {
			return errors.Join(errors.Join(sourceErrs...), errors.Join(headErrs...), errors.Join(finalizedErrs...), err)
		}
		headErrs = append(headErrs, recoveryErrs...)
		return errors.Join(errors.Join(sourceErrs...), errors.Join(headErrs...), errors.Join(finalizedErrs...))
	}
	f.recordSourceResultMetrics(bySource)

	admissions := make([]sourceDocumentAdmission, 0, len(bySource))
	admissionsByID := make(map[string]sourceDocumentAdmission, len(bySource))
	for _, source := range f.sources {
		candidate := bySource[source.cfg.ID]
		if candidate == nil {
			continue
		}
		admission, err := makeSourceDocumentAdmission(source, candidate)
		if err != nil {
			return errors.Join(errors.Join(headErrs...), err)
		}
		admissions = append(admissions, admission)
		admissionsByID[source.cfg.ID] = admission
	}

	// A hard conflict is a durable safety fact, not merely a poll error. Commit
	// every newly proved latch and its exact participant replay floors before
	// recovery can enter a fallible closure walk or external retention prepare.
	if err := f.persistFinalizedConflicts(finalizedResults, admissionsByID); err != nil {
		return errors.Join(errors.Join(headErrs...), err)
	}
	// The safety cut may have added latches. Re-materialize the complete set once
	// so every subsequent early return reports the durable frozen state exactly
	// once, independent of which later subsystem fails.
	headErrs = f.conflictLatchErrors()

	// Recovery remains before fresh serving publication so a conflict,
	// incomparability, or later head-local refusal still leaves the exact durable
	// last-good snapshot live. The conflict safety cut above must precede it.
	recoveryErrs, recoveryErr := f.restoreDarkSourceCheckpoints(ctx)
	headErrs = append(headErrs, recoveryErrs...)
	if recoveryErr != nil {
		return errors.Join(errors.Join(headErrs...), recoveryErr)
	}

	var finalizedFatalErrs []error
	winningDocuments := make(map[string]*resolved)
	for _, result := range finalizedResults {
		if result.fatalErr != nil {
			finalizedFatalErrs = append(finalizedFatalErrs, result.fatalErr)
			continue
		}
		if result.latch != nil {
			f.cfg.Metrics.FollowIncomparableActive(result.name, false)
			continue
		}
		if result.conflict != nil || result.continuityConflict != nil {
			_, latched := f.conflictLatch(result.name)
			if !latched {
				return errors.Join(errors.Join(headErrs...),
					fmt.Errorf("follow: finalized head %q conflict committed without an in-memory latch", result.name))
			}
			continue
		}
		if result.incomparable {
			f.cfg.Metrics.FollowIncomparableActive(result.name, true)
			f.cfg.Metrics.FollowIncomparableObserved(result.name)
		} else if result.comparable {
			f.cfg.Metrics.FollowIncomparableActive(result.name, false)
		}
		if result.err != nil {
			headErrs = append(headErrs, result.err)
			continue
		}
		if !result.hasPlan {
			continue
		}
		planByName[result.name] = result.plan
		if result.winner != nil {
			winningDocuments[result.winner.runtimeSource.cfg.ID] = result.winner
		}
	}
	if len(finalizedFatalErrs) != 0 {
		return errors.Join(errors.Join(headErrs...), errors.Join(finalizedFatalErrs...))
	}

	// Mutable heads remain single-authority. They participate in the same atomic
	// snapshot, but never in cross-writer arbitration. Each head has an
	// independent deadline so a slow authority cannot starve another mutable
	// head merely by sorting first.
	for _, result := range f.preflightMutableHeads(ctx, bySource) {
		if result.fatalErr != nil {
			return errors.Join(errors.Join(headErrs...), result.fatalErr)
		}
		if result.err != nil {
			headErrs = append(headErrs, result.err)
			continue
		}
		if !result.hasPlan {
			continue
		}
		planByName[result.name] = result.plan
		winningDocuments[result.winner.runtimeSource.cfg.ID] = result.winner
	}

	plans := make([]adoptPlan, 0, len(planByName))
	for _, name := range f.Names() {
		if plan, ok := planByName[name]; ok {
			plans = append(plans, plan)
		}
	}

	var boundaryErrs []error
	plans, boundaryErrs = f.omitInvalidProspectiveMutableBoundaries(ctx, plans, winningDocuments)
	headErrs = append(headErrs, boundaryErrs...)

	if f.cfg.Retention == nil && f.hasCollectionGeneration() {
		survivors := make([]adoptPlan, 0, len(plans))
		for _, result := range f.preflightSourcePlanClosures(ctx, plans, true) {
			if result.err != nil {
				headErrs = append(headErrs, fmt.Errorf("follow: preparing head %q closure for source-set publication: %w", result.plan.name, result.err))
				continue
			}
			survivors = append(survivors, result.plan)
		}
		plans = survivors

		// A mutable plan and its selected finalized boundary are one serving
		// dependency group. If closure protection removed a finalized plan, check
		// every surviving mutable plan again against the resulting prospective
		// snapshot and omit any which no longer has a covered boundary. Unrelated
		// finalized plans remain eligible for this poll.
		plans, boundaryErrs = f.omitInvalidProspectiveMutableBoundaries(ctx, plans, winningDocuments)
		headErrs = append(headErrs, boundaryErrs...)
	}

	var retained *replica.Generation
	if f.cfg.Retention != nil {
		generation, err := f.sourceRetentionGeneration(plans, admissions)
		if err != nil {
			return errors.Join(errors.Join(headErrs...), err)
		}
		if err := f.cfg.Retention.Prepare(ctx, generation); err != nil {
			return errors.Join(errors.Join(headErrs...),
				fmt.Errorf("follow: preparing external source-set archive generation: %w", err))
		}
		retained = &generation
	}

	if betweenPhasesHook != nil {
		betweenPhasesHook()
	}
	committedPlans, filterErrs, commitErrs := f.commitSourcePlans(ctx, plans, admissions, winningDocuments)
	if len(commitErrs) != 0 {
		// A conflict latch may already have crossed its earlier durability cut.
		// Preserve that operator-visible safety fact beside the later admission
		// failure instead of making the latch appear to have vanished.
		return errors.Join(errors.Join(headErrs...), errors.Join(commitErrs...))
	}
	plans = committedPlans
	headErrs = append(headErrs, filterErrs...)
	if retained != nil {
		f.gate.Barrier()
		if err := f.cfg.Retention.Commit(ctx, *retained); err != nil {
			return errors.Join(errors.Join(headErrs...),
				fmt.Errorf("follow: committing external source-set archive generation: %w", err))
		}
	}
	forceDocumentCallback := make(map[string]bool)
	for _, plan := range plans {
		if plan.head == nil && !plan.withdraw {
			continue
		}
		document := bySource[plan.cp.sourceID]
		if document == nil || plan.cp.version != checkpointVersionV4 ||
			plan.cp.revision != document.revision || plan.cp.digest != document.digest {
			continue
		}
		forceDocumentCallback[plan.cp.sourceID] = true
	}
	var callbackErrs []error
	for _, source := range f.sources {
		if document := bySource[source.cfg.ID]; document != nil {
			if f.sourceDocumentTouchesLatchedHead(source, document.doc) {
				continue
			}
			if err := f.notifyAdmittedDocument(document, forceDocumentCallback[source.cfg.ID]); err != nil {
				callbackErrs = append(callbackErrs, err)
			}
		}
	}
	headErrs = append(headErrs, callbackErrs...)
	return errors.Join(headErrs...)
}

// recordSourceResultMetrics publishes only documents which survived the whole
// source-local admission boundary: signature/archive binding, replay floors,
// mutable handoff validation, and quarantine. Last claim values deliberately
// remain in place while a source is unavailable; availability and freshness
// distinguish that historical observation from a current one.
func (f *Follower) recordSourceResultMetrics(bySource map[string]*resolved) {
	for _, source := range f.sources {
		candidate := bySource[source.cfg.ID]
		f.cfg.Metrics.FollowSourceAvailable(source.cfg.ID, candidate != nil)
		if candidate == nil {
			continue
		}
		for _, head := range f.Names() {
			if !source.allows(head) {
				continue
			}
			entry, published := documentHead(candidate.doc, head)
			covered := published && entry.SyncedTo != nil
			var syncedTo uint64
			if covered {
				syncedTo = *entry.SyncedTo
			}
			f.cfg.Metrics.FollowSourceHeadClaim(head, source.cfg.ID, syncedTo, covered)
		}
	}
}

// omitSourcesWithOnlyRejectedFinalizedClaims removes a publication candidate
// only when every covered head it is locally authorized to publish is
// finalized and every such claim failed pre-arbitration admission (the
// floorless typed-manifest boundary or the bounded full-DAG boundary). A
// document with another admitted finalized claim remains useful, as does one
// carrying a mutable claim whose independent preflight has not run yet. This
// preserves per-head fault isolation without letting an entirely malformed
// finalized document advance its source-local replay floor.
func (f *Follower) omitSourcesWithOnlyRejectedFinalizedClaims(
	bySource map[string]*resolved,
	results []finalizedHeadPlanResult,
) {
	admitted := make(map[string]int)
	rejected := make(map[string]int)
	for _, result := range results {
		for _, sourceID := range result.admittedSources {
			admitted[sourceID]++
		}
		for _, sourceID := range result.rejectedSources {
			rejected[sourceID]++
		}
	}
	for _, source := range f.sources {
		candidate := bySource[source.cfg.ID]
		if candidate == nil || admitted[source.cfg.ID] != 0 || rejected[source.cfg.ID] == 0 {
			continue
		}
		coveredFinalized := 0
		hasOtherCovered := false
		for _, entry := range candidate.doc.Heads {
			if entry.SyncedTo == nil || !source.allows(entry.Name) {
				continue
			}
			if f.expectedKind(entry.Name) == server.FinalizedMonotonic {
				coveredFinalized++
			} else {
				hasOtherCovered = true
			}
		}
		if !hasOtherCovered && coveredFinalized != 0 && rejected[source.cfg.ID] == coveredFinalized {
			delete(bySource, source.cfg.ID)
		}
	}
}

// preflightDarkSourceCheckpoints returns serving-only plans for durable v4
// checkpoints which are not currently installed in memory. It validates the
// complete attributed checkpoint set before returning any plan. Quarantined
// heads stay dark; a mutable checkpoint whose selected finalized boundary is
// quarantined stays dark with it, while unrelated finalized heads can recover.
func (f *Follower) preflightDarkSourceCheckpoints(ctx context.Context) (map[string]adoptPlan, []error, error) {
	checkpoints := make(map[string]checkpoint, len(f.cfg.Heads))
	for _, name := range f.Names() {
		cp, exists, err := f.state.checkpoint(name)
		if err != nil {
			return nil, nil, err
		}
		if !exists || cp.version != checkpointVersionV4 {
			continue
		}
		if err := f.validateSourceCheckpointProvenance(name, cp); err != nil {
			return nil, nil, err
		}
		if cp.selected {
			checkpoints[name] = cp
		}
	}

	// Quarantine is process-lifetime serviceability state, not a durable claim
	// mutation. Exclude it from recovery without weakening validation of the
	// remaining checkpoints.
	for name := range checkpoints {
		f.mu.Lock()
		head := f.heads[name]
		quarantined := head != nil && head.quarantined
		f.mu.Unlock()
		if quarantined {
			delete(checkpoints, name)
		}
	}
	for name, cp := range checkpoints {
		if cp.kind != server.UnfinalizedMutable {
			continue
		}
		boundary := f.cfg.OverlayFinalizedHeads[name]
		if boundary == "" {
			handoff := f.cfg.ExpectedHandoffs[name]
			if _, selected := f.cfg.Heads[handoff]; selected {
				boundary = handoff
			}
		}
		if boundary != "" {
			if _, available := checkpoints[boundary]; !available {
				delete(checkpoints, name)
			}
		}
	}
	plans := make(map[string]adoptPlan)
	var headErrs []error
	for _, name := range f.Names() {
		cp, restore := checkpoints[name]
		if !restore {
			continue
		}
		plan, err := f.preflightSourceCheckpoint(ctx, name, cp)
		if err != nil {
			headErrs = append(headErrs, fmt.Errorf("follow: preflighting durable head %q for source-set recovery: %w", name, err))
			continue
		}
		if planHasEffect(plan) {
			plans[name] = plan
		}
	}
	ordered := make([]adoptPlan, 0, len(plans))
	for _, name := range f.Names() {
		if plan, ok := plans[name]; ok {
			ordered = append(ordered, plan)
		}
	}
	ordered, boundaryErrs, err := f.omitInvalidResumedMutablePlans(ctx, ordered)
	if err != nil {
		return nil, headErrs, err
	}
	headErrs = append(headErrs, boundaryErrs...)
	plans = make(map[string]adoptPlan, len(ordered))
	for _, plan := range ordered {
		plans[plan.name] = plan
	}
	return plans, headErrs, nil
}

// restoreDarkSourceCheckpoints installs the preflighted durable snapshot in an
// independent serving transaction before fresh arbitration. External retention
// is run even when no serving plan is needed, so a crash-pending Commit is
// retried while every writer is offline.
func (f *Follower) restoreDarkSourceCheckpoints(ctx context.Context) ([]error, error) {
	byName, recoveryErrs, err := f.preflightDarkSourceCheckpoints(ctx)
	if err != nil {
		return recoveryErrs, err
	}
	plans := make([]adoptPlan, 0, len(byName))
	for _, name := range f.Names() {
		if plan, ok := byName[name]; ok {
			plans = append(plans, plan)
		}
	}
	if f.cfg.Retention == nil && len(plans) == 0 {
		return recoveryErrs, nil
	}
	if f.cfg.Retention == nil && f.hasCollectionGeneration() {
		survivors := make([]adoptPlan, 0, len(plans))
		for _, result := range f.preflightSourcePlanClosures(ctx, plans, false) {
			if result.err != nil {
				recoveryErrs = append(recoveryErrs, fmt.Errorf("follow: preparing durable head %q closure for source-set recovery: %w", result.plan.name, result.err))
				continue
			}
			survivors = append(survivors, result.plan)
		}
		plans = survivors
		var boundaryErrs []error
		plans, boundaryErrs, err = f.omitInvalidResumedMutablePlans(ctx, plans)
		if err != nil {
			return recoveryErrs, err
		}
		recoveryErrs = append(recoveryErrs, boundaryErrs...)
	}

	var retained *replica.Generation
	if f.cfg.Retention != nil {
		generation, err := f.sourceRetentionGeneration(plans, nil)
		if err != nil {
			return recoveryErrs, err
		}
		if err := f.cfg.Retention.Prepare(ctx, generation); err != nil {
			return recoveryErrs, fmt.Errorf("follow: preparing external source-set recovery generation: %w", err)
		}
		retained = &generation
	}
	if len(plans) != 0 {
		_, filterErrs, commitErrs := f.commitSourcePlans(ctx, plans, nil, nil)
		recoveryErrs = append(recoveryErrs, filterErrs...)
		if len(commitErrs) != 0 {
			return recoveryErrs, errors.Join(commitErrs...)
		}
	}
	if retained != nil {
		f.gate.Barrier()
		if err := f.cfg.Retention.Commit(ctx, *retained); err != nil {
			return recoveryErrs, fmt.Errorf("follow: committing external source-set recovery generation: %w", err)
		}
	}
	return recoveryErrs, nil
}

func (f *Follower) commitSourceObservations(results []sourceResolveResult) ([]sourceResolveResult, error) {
	observations := make([]sourceChannelObs, 0, len(results))
	for i := range results {
		obs := results[i].obs
		if !obs.hasIPNSSeq {
			continue
		}
		floor, ok, err := f.state.sourceIPNSSeq(obs.ref, obs.ipnsName)
		if err != nil {
			return nil, err
		}
		if ok && obs.ipnsSeq < floor && results[i].candidate != nil && results[i].candidate.source == "ipns" {
			results[i].err = errors.Join(results[i].err, fmt.Errorf("follow: source %q IPNS sequence %d is below the locked floor %d", obs.ref.sourceID, obs.ipnsSeq, floor))
			results[i].candidate = nil
		}
		observations = append(observations, obs)
	}
	if len(observations) == 0 {
		return results, nil
	}
	batch := f.cfg.KV.NewBatch()
	defer batch.Close()
	if err := f.state.stageSourceObservations(batch, observations); err != nil {
		return nil, err
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		return nil, fmt.Errorf("follow: committing source channel observations: %w", err)
	}
	return results, nil
}

func (f *Follower) sourcePublishesOwnedMutable(source *sourceRuntime, document server.Doc) bool {
	for _, entry := range document.Heads {
		if source.allows(entry.Name) && f.expectedKind(entry.Name) == server.UnfinalizedMutable {
			return true
		}
	}
	return false
}

// refuseQuarantinedSourceDocument preserves the document-level quarantine
// boundary independently for each writer. A source publication which still
// advertises any locally followed, source-authorized quarantined head is not
// admitted at all: it contributes neither arbitration claims nor a replay
// floor, while unrelated healthy sources remain usable. Empty lines and
// omissions advertise no serving pointer and may still withdraw mutable state.
// The caller holds transition, the same lock used to set quarantine.
func (f *Follower) refuseQuarantinedSourceDocument(source *sourceRuntime, document server.Doc) error {
	for _, entry := range document.Heads {
		if entry.SyncedTo == nil || !source.allows(entry.Name) {
			continue
		}
		f.mu.Lock()
		head := f.heads[entry.Name]
		quarantined := head != nil && head.quarantined
		f.mu.Unlock()
		if !quarantined {
			continue
		}
		f.cfg.Metrics.FollowRefusal(metrics.RefusalQuarantined)
		return fmt.Errorf("%w: source %q still publishes quarantined head %q", server.ErrQuarantined, source.cfg.ID, entry.Name)
	}
	return nil
}

func (f *Follower) mutableSource(head string) *sourceRuntime {
	var found *sourceRuntime
	for _, source := range f.sources {
		if !source.allows(head) {
			continue
		}
		if found != nil {
			return nil
		}
		found = source
	}
	return found
}

func planHasEffect(plan adoptPlan) bool {
	return plan.writeCheckpoint || plan.head != nil || plan.withdraw
}

func (f *Follower) prospectiveCheckpoint(head string, plans []adoptPlan) (checkpoint, bool, error) {
	for _, plan := range plans {
		if plan.name != head {
			continue
		}
		if plan.withdraw {
			return checkpoint{}, false, nil
		}
		if plan.writeCheckpoint {
			if !plan.cp.selected {
				return checkpoint{}, false, nil
			}
			if plan.head != nil && plan.head.Root() == plan.cp.root {
				return plan.cp, true, nil
			}
			return plan.cp, f.sourceBoundaryCurrentlyServes(head, plan.cp), nil
		}
		if plan.head != nil {
			return plan.cp, plan.head.Root() == plan.cp.root, nil
		}
	}
	cp, ok, err := f.state.checkpoint(head)
	if err != nil || !ok || !cp.selected {
		return checkpoint{}, false, err
	}
	return cp, f.sourceBoundaryCurrentlyServes(head, cp), nil
}

// A durable checkpoint proves provenance and continuity, not availability. A
// mutable head may rely on a prior finalized boundary only while the registry
// actually serves the exact selected root; quarantine and failed dark-start
// recovery therefore cannot be bypassed by stale durable metadata.
func (f *Follower) sourceBoundaryCurrentlyServes(head string, cp checkpoint) bool {
	if f.cfg.Registry == nil || !cp.root.Defined() {
		return false
	}
	current, ok := f.cfg.Registry.Get(head)
	return ok && current.Root() == cp.root
}

func (f *Follower) validateProspectiveMutableBoundary(ctx context.Context, plan adoptPlan, plans []adoptPlan, documents map[string]*resolved) error {
	if plan.head == nil || plan.kind != server.UnfinalizedMutable || plan.withdraw || plan.cp.published == nil {
		return nil
	}
	document := documents[plan.cp.sourceID]
	if document == nil {
		return fmt.Errorf("follow: mutable head %q has no source document for boundary validation", plan.name)
	}
	boundary := f.cfg.OverlayFinalizedHeads[plan.name]
	if boundary == "" {
		handoff := f.cfg.ExpectedHandoffs[plan.name]
		if _, selected := f.cfg.Heads[handoff]; selected {
			boundary = handoff
		}
	}
	if boundary == "" {
		return nil // the signed handoff remains metadata-only on this replica.
	}
	finalized, selected, err := f.prospectiveCheckpoint(boundary, plans)
	if err != nil {
		return err
	}
	if !selected || finalized.version != checkpointVersionV4 {
		return fmt.Errorf("follow: mutable head %q cannot be selected before finalized boundary %q has fresh source provenance", plan.name, boundary)
	}
	relation, err := classifyCheckpointAgainstDocument(ctx, f.blocks, *f.cfg.ExpectedArchiveID, boundary, finalized, document.doc)
	if err != nil {
		return err
	}
	if relation != ClaimsEquivalent && relation != LeftClaimDominates {
		return fmt.Errorf("follow: mutable head %q boundary %q is not covered by the selected finalized snapshot (%s)", plan.name, boundary, relation)
	}
	return nil
}

// omitInvalidProspectiveMutableBoundaries keeps a malformed or unavailable
// mutable/boundary dependency group from aborting independent finalized heads.
// Only the mutable plan is omitted: a valid finalized boundary remains useful
// on its own, while every mutable survivor has been proved coherent with the
// exact prospective finalized snapshot which will be committed beside it.
func (f *Follower) omitInvalidProspectiveMutableBoundaries(ctx context.Context, plans []adoptPlan, documents map[string]*resolved) ([]adoptPlan, []error) {
	results := make([]error, len(plans))
	var group sync.WaitGroup
	for i, plan := range plans {
		if plan.head == nil || plan.kind != server.UnfinalizedMutable || plan.withdraw || plan.cp.published == nil {
			continue
		}
		i, plan := i, plan
		group.Add(1)
		go func() {
			defer group.Done()
			headCtx, cancel := context.WithTimeout(ctx, docTimeout)
			defer cancel()
			results[i] = f.validateProspectiveMutableBoundary(headCtx, plan, plans, documents)
		}()
	}
	group.Wait()

	invalid := make(map[string]struct{})
	var errs []error
	for i, plan := range plans {
		if err := results[i]; err != nil {
			invalid[plan.name] = struct{}{}
			errs = append(errs, fmt.Errorf("follow: validating mutable head %q boundary: %w", plan.name, err))
		}
	}
	if len(invalid) == 0 {
		return plans, nil
	}
	survivors := make([]adoptPlan, 0, len(plans)-len(invalid))
	for _, plan := range plans {
		if _, drop := invalid[plan.name]; !drop {
			survivors = append(survivors, plan)
		}
	}
	return survivors, errs
}

// filterSourcePlansUnderGate is the authoritative embedded-retention proof
// boundary for a source-set transaction. Attribution changes the safe failure
// unit from "the whole document" to one head plus any mutable head which depends
// on it: stale collection tokens, unavailable legacy closures, and vanished
// local anchors omit only the affected plan. The exact survivors are then
// boundary-validated again before any checkpoint, mirror, or pointer is staged.
// Caller holds Gate, so no collection can cut between these proofs and visible
// publication.
func (f *Follower) filterSourcePlansUnderGate(ctx context.Context, plans []adoptPlan, documents map[string]*resolved) ([]adoptPlan, []error, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	generation := f.collectionGeneration()
	survivors := make([]adoptPlan, 0, len(plans))
	var errs []error
	for i := range plans {
		plan := plans[i]
		if plan.head != nil {
			if f.hasCollectionGeneration() {
				if plan.closureGeneration != generation {
					errs = append(errs, fmt.Errorf("follow: head %q closure was proved in collection generation %d, current is %d; omitting this source-set plan",
						plan.name, plan.closureGeneration, generation))
					continue
				}
			} else if err := f.protectAdoptionClosure(ctx, &plan, true); err != nil {
				if ctxErr := ctx.Err(); ctxErr != nil {
					return nil, errs, ctxErr
				}
				errs = append(errs, fmt.Errorf("follow: preparing head %q closure for source-set publication: %w", plan.name, err))
				continue
			}
			if err := f.touchGeneration(ctx, plan.name, plan.head.Root(), plan.tip); err != nil {
				if ctxErr := ctx.Err(); ctxErr != nil {
					return nil, errs, ctxErr
				}
				errs = append(errs, err)
				continue
			}
		}
		survivors = append(survivors, plan)
	}

	var boundaryErrs []error
	if documents == nil {
		var err error
		survivors, boundaryErrs, err = f.omitInvalidResumedMutablePlans(ctx, survivors)
		if err != nil {
			return nil, errs, err
		}
	} else {
		survivors, boundaryErrs = f.omitInvalidProspectiveMutableBoundaries(ctx, survivors, documents)
	}
	errs = append(errs, boundaryErrs...)
	if err := ctx.Err(); err != nil {
		return nil, errs, err
	}
	return survivors, errs, nil
}

// omitInvalidResumedMutablePlans applies the same dependency-group isolation
// to serving-only v4 recovery. The checkpoint map contains survivor plans plus
// exact durable boundaries which are already serviceable; a merely durable but
// dark/quarantined boundary cannot license mutable restoration.
func (f *Follower) omitInvalidResumedMutablePlans(ctx context.Context, plans []adoptPlan) ([]adoptPlan, []error, error) {
	checkpoints := make(map[string]checkpoint, len(f.cfg.Heads))
	planned := make(map[string]struct{}, len(plans))
	for _, plan := range plans {
		planned[plan.name] = struct{}{}
		if !plan.withdraw && plan.cp.selected {
			checkpoints[plan.name] = plan.cp
		}
	}
	for _, name := range f.Names() {
		if _, exists := planned[name]; exists {
			continue
		}
		cp, ok, err := f.state.checkpoint(name)
		if err != nil {
			return nil, nil, err
		}
		if ok && cp.selected && f.sourceBoundaryCurrentlyServes(name, cp) {
			checkpoints[name] = cp
		}
	}

	results := make([]error, len(plans))
	var group sync.WaitGroup
	for i, plan := range plans {
		if !plan.cp.selected || plan.cp.kind != server.UnfinalizedMutable {
			continue
		}
		i, plan := i, plan
		group.Add(1)
		go func() {
			defer group.Done()
			results[i] = f.validateResumedMutableBoundary(ctx, plan.name, plan.cp, checkpoints)
		}()
	}
	group.Wait()

	invalid := make(map[string]struct{})
	var errs []error
	for i, plan := range plans {
		if err := results[i]; err != nil {
			invalid[plan.name] = struct{}{}
			errs = append(errs, fmt.Errorf("follow: validating durable mutable head %q boundary: %w", plan.name, err))
		}
	}
	if len(invalid) == 0 {
		return plans, errs, nil
	}
	survivors := make([]adoptPlan, 0, len(plans)-len(invalid))
	for _, plan := range plans {
		if _, drop := invalid[plan.name]; !drop {
			survivors = append(survivors, plan)
		}
	}
	return survivors, errs, nil
}

func (f *Follower) sourceRetentionGeneration(plans []adoptPlan, _ []sourceDocumentAdmission) (replica.Generation, error) {
	// Render the prospective durable checkpoint map first, including unselected
	// tombstones. UpdatedAt is derived only from this reconstructible post-commit
	// state: a replaced checkpoint's newer wall clock must not leak into the
	// anchor, and source admission clocks are not durably stored with the replay
	// floor. A genuinely empty map therefore has the Unix epoch generation.
	byName := make(map[string]checkpoint, len(f.cfg.Heads))
	for _, name := range f.Names() {
		cp, ok, err := f.state.checkpoint(name)
		if err != nil {
			return replica.Generation{}, err
		}
		if ok {
			byName[name] = cp
		}
	}
	for _, plan := range plans {
		if plan.writeCheckpoint {
			byName[plan.name] = plan.cp
		}
	}
	updatedAt := time.Unix(0, 0).UTC()
	for _, cp := range byName {
		if cp.updatedAt.After(updatedAt) {
			updatedAt = cp.updatedAt
		}
	}
	generation := replica.Generation{UpdatedAt: updatedAt}
	for _, name := range f.Names() {
		if cp, ok := byName[name]; ok && cp.selected && cp.root.Defined() {
			generation.Heads = append(generation.Heads, replica.Head{
				Name: name, Root: cp.root, Manifest: cp.manifestTip, SyncedTo: cp.syncedTo,
			})
		}
	}
	return generation, nil
}
