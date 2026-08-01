package follow

import (
	"context"
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble/v2"
	"github.com/ipfs/go-cid"

	"github.com/blobarchive/bloar/archive"
	"github.com/blobarchive/bloar/metrics"
	"github.com/blobarchive/bloar/pinning"
	"github.com/blobarchive/bloar/replica"
	"github.com/blobarchive/bloar/server"
)

// resumeSourceCheckpoints restores only source-attributed v4 checkpoints. A
// legacy v1-v3 record may remain after an explicit source-set migration, but it
// is a content floor rather than evidence that any currently acknowledged
// source selected the claim. It therefore stays dark until a fresh source poll
// proves continuity and replaces it with v4.
//
// Every v4 record is validated before any is exposed. The records need not all
// come from one publication generation -- independent writers cannot share one
// -- but the selected last-good set is still published through one atomic
// registry transaction, with the same retention/GC boundary as a live poll.
func (f *Follower) resumeSourceCheckpoints(ctx context.Context) error {
	if f.cfg.SourceSet == nil || f.cfg.ExpectedArchiveID == nil {
		return errors.New("follow: source checkpoint resume requires a configured source set and archive ID")
	}
	archiveID := *f.cfg.ExpectedArchiveID
	marker, ok, err := f.state.sourceSetMarker()
	if err != nil {
		return err
	}
	if !ok {
		return errors.New("follow: source checkpoint resume found no durable source-set marker")
	}
	if marker.archiveID != archiveID || marker.revision != f.cfg.SourceSet.Revision || marker.digest != f.cfg.SourceSet.Digest {
		return fmt.Errorf("follow: durable source-set generation does not match the configured generation")
	}

	checkpoints := make(map[string]checkpoint, len(f.cfg.Heads))
	retentionCheckpoints := make(map[string]checkpoint, len(f.cfg.Heads))
	retained := make([]replica.Head, 0, len(f.cfg.Heads))
	hasCheckpoint := false
	var scanErrs []error
	for _, name := range f.Names() {
		cp, exists, err := f.state.checkpoint(name)
		if err != nil {
			// Keep scanning so independently valid siblings can be registered for
			// retention before this nonfatal Resume failure is returned. No member
			// is served until the complete scan and cross-head checks succeed.
			scanErrs = append(scanErrs, err)
			continue
		}
		if !exists {
			continue
		}
		hasCheckpoint = true
		if cp.selected && cp.root.Defined() {
			// External retention is one exact all-head generation. A legacy
			// checkpoint remains hidden as serving authority, but its content is
			// still a member of the durable last-good selection until a fresh v4
			// claim upgrades or withdraws it.
			retained = append(retained, replica.Head{
				Name: name, Root: cp.root, Manifest: cp.manifestTip, SyncedTo: cp.syncedTo,
			})
		}
		if cp.version != checkpointVersionV4 {
			f.log.Warn("legacy followed-head checkpoint has no source attribution; leaving it unserved until a fresh source claim proves continuity",
				"head", name, "checkpoint_version", cp.version)
			if cp.selected {
				retentionCheckpoints[name] = cp
			}
			continue
		}
		if err := f.validateSourceCheckpointProvenance(name, cp); err != nil {
			scanErrs = append(scanErrs, err)
			continue
		}
		checkpoints[name] = cp
		if cp.selected {
			retentionCheckpoints[name] = cp
		}
	}

	if f.cfg.Retention != nil {
		if len(scanErrs) != 0 {
			// An external generation proof is exact. A corrupt/invalid member means
			// we cannot reconstruct the complete set, so do not ask the store to
			// authenticate an incomplete one. Its existing Current/Pending anchor
			// remains untouched while Resume fails closed.
			return errors.Join(scanErrs...)
		}
		// A durable all-withdrawn v4 generation is an exact empty generation and
		// still needs proof. A genuinely fresh store has no generation to prove.
		if hasCheckpoint {
			if err := f.cfg.Retention.ProtectsAll(ctx, retained); err != nil {
				return fmt.Errorf("follow: resuming external source-set archive generation: %w", err)
			}
		}
	} else if len(retentionCheckpoints) != 0 && f.cfg.Reconciler != nil {
		if err := f.protectSourceCheckpoints(ctx, retentionCheckpoints); err != nil {
			scanErrs = append(scanErrs, err)
		}
	}
	if len(scanErrs) != 0 {
		return errors.Join(scanErrs...)
	}

	if len(checkpoints) == 0 {
		return nil
	}
	if err := f.validateResumedMutableBoundaries(ctx, checkpoints); err != nil {
		return err
	}

	plans := make([]adoptPlan, 0, len(checkpoints))
	for _, name := range f.Names() {
		cp, exists := checkpoints[name]
		if !exists {
			continue
		}
		plan, err := f.preflightSourceCheckpoint(ctx, name, cp)
		if err != nil {
			return fmt.Errorf("follow: resuming source-attributed head %q: %w", name, err)
		}
		if plan.head != nil && f.cfg.Retention == nil && f.hasCollectionGeneration() {
			if err := f.protectAdoptionClosure(ctx, &plan, false); err != nil {
				return fmt.Errorf("follow: resuming source-attributed head %q: preparing publication closure: %w", name, err)
			}
		}
		plans = append(plans, plan)
	}

	if errs := f.commitPlansWithStage(ctx, plans, func(*pebble.Batch) error { return nil }); len(errs) != 0 {
		return errors.Join(errs...)
	}
	return nil
}

// protectSourceCheckpoints installs every independently valid selected
// checkpoint as a retention-only reconciler registration before the all-head
// serving checks. Legacy v1-v3 records remain hidden until fresh source
// attribution upgrades them. V4 records are likewise still absent from Registry
// until the complete source snapshot validates, but a later boundary/load/commit
// failure cannot leave their durable last-good roots unregistered for GC.
//
// The closure proof and Gate transaction mirror serving adoption. This matters
// when Resume overlaps an already-active online collection: the old cut cannot
// see the new registration, so every retained block is touched into that epoch
// before the registration becomes visible to future cuts. A plain blockstore
// instead walks while Gate excludes its whole-run collector. Exact compatibility
// mirrors are repaired from the authoritative checkpoint before reconciliation,
// so the reconciler's manifest-tip callback cannot retain a different chain.
func (f *Follower) protectSourceCheckpoints(ctx context.Context, checkpoints map[string]checkpoint) error {
	plans := make([]adoptPlan, 0, len(checkpoints))
	var errs []error
	for _, name := range f.Names() {
		cp, ok := checkpoints[name]
		if !ok {
			continue
		}
		var plan adoptPlan
		var err error
		if cp.version == checkpointVersionV4 {
			plan, err = f.preflightSourceCheckpoint(ctx, name, cp)
		} else {
			plan, err = f.preflightLegacySourceRetentionCheckpoint(ctx, name, cp)
		}
		if err != nil {
			errs = append(errs, fmt.Errorf("follow: protecting source checkpoint %q: %w", name, err))
			continue
		}
		if plan.head == nil {
			continue
		}
		plans = append(plans, plan)
	}
	if len(plans) == 0 {
		return errors.Join(errs...)
	}

	// For generation-aware stores, prove every closure against one collection
	// generation. A cut between two per-head walks invalidates the older token;
	// repeat that head before taking Gate instead of leaving migration protection
	// to a later poll which may have no reachable source.
	if f.hasCollectionGeneration() {
		for {
			protected := make([]adoptPlan, 0, len(plans))
			for i := range plans {
				if plans[i].closureGeneration == f.collectionGeneration() {
					protected = append(protected, plans[i])
					continue
				}
				if err := f.protectAdoptionClosure(ctx, &plans[i], false); err != nil {
					errs = append(errs, fmt.Errorf("follow: protecting source checkpoint %q closure: %w", plans[i].name, err))
					continue
				}
				protected = append(protected, plans[i])
			}
			plans = protected
			if len(plans) == 0 {
				return errors.Join(errs...)
			}

			f.gate.Enter()
			generation := f.collectionGeneration()
			stable := true
			for i := range plans {
				if plans[i].closureGeneration != generation {
					stable = false
					break
				}
			}
			if !stable {
				f.gate.Leave()
				continue
			}
			var commitErr error
			plans, commitErr = f.commitSourceCheckpointRetention(ctx, plans, generation)
			f.gate.Leave()
			if commitErr != nil {
				errs = append(errs, commitErr)
			}
			break
		}
	} else {
		f.gate.Enter()
		protected := make([]adoptPlan, 0, len(plans))
		for i := range plans {
			if err := f.protectAdoptionClosure(ctx, &plans[i], true); err != nil {
				errs = append(errs, fmt.Errorf("follow: protecting source checkpoint %q closure: %w", plans[i].name, err))
				continue
			}
			protected = append(protected, plans[i])
		}
		plans = protected
		var commitErr error
		if len(plans) != 0 {
			plans, commitErr = f.commitSourceCheckpointRetention(ctx, plans, f.collectionGeneration())
		}
		f.gate.Leave()
		if commitErr != nil {
			errs = append(errs, commitErr)
		}
	}

	// Persist the pin ledger before releasing closure-walk staging pins. GC also
	// flushes this same reconciliation under Gate, so racing it here is safe and
	// idempotent; a failure leaves the registration and staging protection in
	// place and makes a future collector fail closed rather than sweep the floor.
	reconciled := make([]adoptPlan, 0, len(plans))
	for _, plan := range plans {
		if _, err := f.cfg.Reconciler.ReconcileHead(ctx, plan.name); err != nil {
			errs = append(errs, fmt.Errorf("follow: reconciling source checkpoint %q: %w", plan.name, err))
			continue
		}
		reconciled = append(reconciled, plan)
	}
	if f.cfg.Staging != nil {
		for _, plan := range reconciled {
			if len(plan.staged) == 0 {
				continue
			}
			if err := f.cfg.Staging.Drop(ctx, plan.staged); err != nil {
				f.log.Error("dropping source checkpoint protection staging pins", "head", plan.name, "err", err)
			}
		}
	}
	return errors.Join(errs...)
}

// commitSourceCheckpointRetention performs the short, gated visibility half. The
// caller has proved every plan in generation and already holds Gate.
func (f *Follower) commitSourceCheckpointRetention(ctx context.Context, plans []adoptPlan, generation uint64) ([]adoptPlan, error) {
	committable := make([]adoptPlan, 0, len(plans))
	var errs []error
	for _, plan := range plans {
		if plan.closureGeneration != generation {
			errs = append(errs, fmt.Errorf("follow: source checkpoint %q closure was proved in collection generation %d, current is %d",
				plan.name, plan.closureGeneration, generation))
			continue
		}
		if err := f.touchGeneration(ctx, plan.name, plan.head.Root(), plan.tip); err != nil {
			errs = append(errs, err)
			continue
		}
		committable = append(committable, plan)
	}
	if len(committable) == 0 {
		return nil, errors.Join(errs...)
	}

	registrations := make([]pinning.Registration, 0, len(committable))
	for _, plan := range committable {
		registrations = append(registrations, pinning.Registration{
			Name: plan.name, Head: plan.head, Policy: f.cfg.Heads[plan.name],
		})
	}
	apply, err := f.cfg.Reconciler.PrepareSetBatch(registrations)
	if err != nil {
		return nil, errors.Join(append(errs, err)...)
	}

	batch := f.cfg.KV.NewBatch()
	defer batch.Close()
	for _, plan := range committable {
		if err := f.cfg.Roots.StagePut(batch, plan.name, plan.head.Root()); err != nil {
			return nil, errors.Join(append(errs, err)...)
		}
		if plan.tip.Defined() {
			if err := f.cfg.Manifests.StagePut(batch, plan.name, plan.tip); err != nil {
				return nil, errors.Join(append(errs, err)...)
			}
		} else if err := f.cfg.Manifests.StageDelete(batch, plan.name); err != nil {
			return nil, errors.Join(append(errs, err)...)
		}
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		return nil, errors.Join(append(errs,
			fmt.Errorf("follow: repairing source-checkpoint compatibility mirrors: %w", err))...)
	}
	apply()
	return committable, errors.Join(errs...)
}

func (f *Follower) preflightLegacySourceRetentionCheckpoint(ctx context.Context, name string, cp checkpoint) (adoptPlan, error) {
	if !cp.selected || !cp.root.Defined() {
		return adoptPlan{}, errors.New("legacy retention checkpoint is not a selected root")
	}
	kind := f.expectedKind(name)
	if cp.kind != kind {
		return adoptPlan{}, fmt.Errorf("checkpoint kind %q differs from configured kind %q", cp.kind, kind)
	}
	if cp.version == checkpointVersionV3 {
		if cp.net != f.cfg.Net {
			return adoptPlan{}, fmt.Errorf("checkpoint network %q differs from configured network %q", cp.net, f.cfg.Net)
		}
		if cp.published == nil {
			return adoptPlan{}, errors.New("selected v3 checkpoint has no authenticated publication entry")
		}
	}

	head, err := f.load(ctx, name, cp.root)
	if err != nil {
		return adoptPlan{}, err
	}
	if cp.version == checkpointVersionV3 {
		if err := matchPublishedRoot(cp.net, *cp.published, head.Info()); err != nil {
			return adoptPlan{}, err
		}
	}
	derived, covered := head.SyncedTo()
	switch kind {
	case server.FinalizedMonotonic:
		if !covered || derived < cp.syncedTo {
			return adoptPlan{}, fmt.Errorf("checkpoint floor %d is above root coverage %d (covered=%t)", cp.syncedTo, derived, covered)
		}
	case server.UnfinalizedMutable:
		if !covered || derived != cp.syncedTo || head.Params().OriginSlot != cp.windowStart {
			return adoptPlan{}, fmt.Errorf("mutable checkpoint window [%d,%d] does not match root origin %d and coverage %d (covered=%t)",
				cp.windowStart, cp.syncedTo, head.Params().OriginSlot, derived, covered)
		}
		if cp.syncedTo < cp.windowStart || cp.syncedTo-cp.windowStart >= f.cfg.MaxMutableWindowSlots[name] {
			return adoptPlan{}, fmt.Errorf("mutable checkpoint window [%d,%d] exceeds its configured %d-slot maximum",
				cp.windowStart, cp.syncedTo, f.cfg.MaxMutableWindowSlots[name])
		}
		if cp.manifestTip.Defined() {
			return adoptPlan{}, fmt.Errorf("mutable checkpoint carries forbidden manifest %s", cp.manifestTip)
		}
	default:
		return adoptPlan{}, fmt.Errorf("checkpoint has unknown head kind %q", kind)
	}
	if err := validateRegistryAdopt(f.cfg.Registry, head, cp.manifestTip, kind); err != nil {
		return adoptPlan{}, err
	}
	return adoptPlan{name: name, head: head, tip: cp.manifestTip, kind: kind, cp: cp}, nil
}

// preflightSourceCheckpoint renders one already-authenticated v4 checkpoint as
// an adoption plan without changing its durable provenance. Resume uses it for
// the initial all-head snapshot; Poll uses the same path when an equivalent or
// stale source observation proves that an ahead durable last-good checkpoint is
// still the correct selection after a failed/omitted Resume. Quarantine remains
// process-lifetime state and is checked before any serving plan is returned.
func (f *Follower) preflightSourceCheckpoint(ctx context.Context, name string, cp checkpoint) (adoptPlan, error) {
	return f.preflightSourceCheckpointWithHead(ctx, name, cp, nil)
}

// preflightSourceCheckpointWithHead carries a bounded continuity proof into a
// dark-checkpoint serving plan without a second complete index walk.
func (f *Follower) preflightSourceCheckpointWithHead(
	ctx context.Context,
	name string,
	cp checkpoint,
	admitted *archive.Head,
) (adoptPlan, error) {
	if err := f.validateSourceCheckpointProvenance(name, cp); err != nil {
		return adoptPlan{}, err
	}
	kind := f.expectedKind(name)
	if cp.kind != kind {
		return adoptPlan{}, fmt.Errorf("checkpoint kind %q differs from configured kind %q", cp.kind, kind)
	}
	if !cp.selected {
		return adoptPlan{name: name, kind: kind, cp: cp, withdraw: true}, nil
	}
	if cp.published == nil {
		return adoptPlan{}, errors.New("selected checkpoint has no exact publication entry")
	}

	f.mu.Lock()
	headState, known := f.heads[name]
	var quarantined bool
	var adopted, adoptedManifest cid.Cid
	if known {
		quarantined = headState.quarantined
		adopted, adoptedManifest = headState.adopted, headState.manifestTip
	}
	f.mu.Unlock()
	if !known {
		return adoptPlan{}, fmt.Errorf("head %q is not followed by this node", name)
	}
	if quarantined {
		f.cfg.Metrics.FollowRefusal(metrics.RefusalQuarantined)
		return adoptPlan{}, fmt.Errorf("%w: head %q", server.ErrQuarantined, name)
	}
	entry := *cloneCheckpointHeadEntry(cp.published)
	if adopted == cp.root && adoptedManifest == cp.manifestTip {
		return adoptPlan{name: name, entry: entry, tip: cp.manifestTip, kind: kind, cp: cp}, nil
	}

	head := admitted
	var err error
	if head == nil {
		head, err = f.load(ctx, name, cp.root)
		if err != nil {
			return adoptPlan{}, fmt.Errorf("loading checkpoint root %s: %w", cp.root, err)
		}
	} else if head.Root() != cp.root {
		return adoptPlan{}, fmt.Errorf("checkpoint head %q pre-admission root %s differs from durable root %s",
			name, head.Root(), cp.root)
	}
	if err := matchPublishedRoot(cp.net, entry, head.Info()); err != nil {
		return adoptPlan{}, err
	}
	if kind == server.UnfinalizedMutable && (head.Params().OriginSlot != cp.windowStart ||
		cp.syncedTo < cp.windowStart || cp.syncedTo-cp.windowStart >= f.cfg.MaxMutableWindowSlots[name]) {
		return adoptPlan{}, fmt.Errorf("mutable checkpoint window [%d,%d] is invalid for loaded root/configuration",
			cp.windowStart, cp.syncedTo)
	}
	if err := validateRegistryAdopt(f.cfg.Registry, head, cp.manifestTip, kind); err != nil {
		return adoptPlan{}, err
	}
	return adoptPlan{name: name, head: head, entry: entry, tip: cp.manifestTip, kind: kind, cp: cp}, nil
}

func (f *Follower) validateSourceCheckpointProvenance(name string, cp checkpoint) error {
	archiveID := *f.cfg.ExpectedArchiveID
	if cp.archiveID != archiveID {
		return fmt.Errorf("follow: head %q checkpoint archive %s differs from configured archive %s", name, cp.archiveID, archiveID)
	}
	if cp.net != f.cfg.Net {
		return fmt.Errorf("follow: head %q checkpoint network %q differs from configured network %q", name, cp.net, f.cfg.Net)
	}
	if cp.published != nil {
		if cp.published.Name != name {
			return fmt.Errorf("follow: head %q checkpoint retains publication line %q", name, cp.published.Name)
		}
		if cp.kind != f.expectedKind(name) {
			return fmt.Errorf("follow: head %q checkpoint kind %q differs from configured kind %q", name, cp.kind, f.expectedKind(name))
		}
	}
	ref := sourceRef{archiveID: archiveID, sourceID: cp.sourceID}
	binding, bound, err := f.state.sourceBinding(ref)
	if err != nil {
		return err
	}
	if !bound || binding.pubkey != cp.authority {
		return fmt.Errorf("follow: head %q checkpoint authority is not the durable binding for source %q", name, cp.sourceID)
	}
	floor, covered, err := f.state.sourcePublicationFloor(ref)
	if err != nil {
		return err
	}
	if !covered || floor.revision < cp.revision || (floor.revision == cp.revision && floor.digest != cp.digest) {
		return fmt.Errorf("follow: head %q checkpoint generation %d is not covered by source %q's durable publication floor", name, cp.revision, cp.sourceID)
	}
	if f.expectedKind(name) == server.UnfinalizedMutable {
		source := f.mutableSource(name)
		if source == nil || source.cfg.ID != cp.sourceID {
			return fmt.Errorf("follow: mutable head %q checkpoint source %q is not its unique configured authority", name, cp.sourceID)
		}
	}
	return nil
}

func (f *Follower) validateResumedMutableBoundaries(ctx context.Context, checkpoints map[string]checkpoint) error {
	for _, name := range f.Names() {
		cp, exists := checkpoints[name]
		if !exists || !cp.selected || cp.kind != server.UnfinalizedMutable {
			continue
		}
		if err := f.validateResumedMutableBoundary(ctx, name, cp, checkpoints); err != nil {
			return err
		}
	}
	return nil
}

func (f *Follower) validateResumedMutableBoundary(ctx context.Context, name string, cp checkpoint, checkpoints map[string]checkpoint) error {
	boundary := f.cfg.OverlayFinalizedHeads[name]
	var witness *server.HeadEntry
	if boundary != "" {
		// A filtered overlay requires an exact same-document witness distinct
		// from the global handoff.
		witness = cp.overlay
	} else {
		handoff := f.cfg.ExpectedHandoffs[name]
		if _, selected := f.cfg.Heads[handoff]; selected {
			boundary = handoff
			witness = cp.handoff
		}
	}
	if boundary == "" {
		return nil // metadata-only global handoff is retained but not served here.
	}
	if witness == nil || witness.Name != boundary {
		return fmt.Errorf("follow: mutable head %q checkpoint lacks the exact same-document boundary witness for %q", name, boundary)
	}
	finalized, selected := checkpoints[boundary]
	if !selected || !finalized.selected || finalized.kind != server.FinalizedMonotonic {
		return fmt.Errorf("follow: mutable head %q cannot resume without selected source-attributed finalized boundary %q", name, boundary)
	}
	relation, err := classifyCheckpointAgainstEntry(ctx, f.blocks, *f.cfg.ExpectedArchiveID, cp.net, boundary, finalized, *witness)
	if err != nil {
		return err
	}
	if relation != ClaimsEquivalent && relation != LeftClaimDominates {
		return fmt.Errorf("follow: mutable head %q boundary %q is not covered by the resumed finalized snapshot (%s)", name, boundary, relation)
	}
	return nil
}
