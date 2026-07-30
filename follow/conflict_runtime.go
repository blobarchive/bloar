package follow

import (
	"fmt"
	"slices"

	"github.com/cockroachdb/pebble/v2"

	"github.com/blobarchive/bloar/server"
)

// ConflictLatchedError is the sanitized poll-level condition for a finalized
// head whose advancement is frozen by durable evidence. It intentionally omits
// roots and manifest CIDs: those remain available through the offline status
// command, while routine poll logs stay bounded to configured head/source
// dimensions and the content-bound evidence identifier.
type ConflictLatchedError struct {
	Head       string
	Sequence   uint64
	Reason     ConflictReason
	EvidenceID [32]byte
	Sources    []string
}

func (e *ConflictLatchedError) Error() string {
	return fmt.Sprintf("follow: finalized head %q advancement is frozen by conflict evidence sha256:%x "+
		"(sequence=%d reason=%s sources=%v)", e.Head, e.EvidenceID, e.Sequence, e.Reason, e.Sources)
}

func (f *Follower) conflictLatchedError(record ConflictRecord) *ConflictLatchedError {
	sources := make([]string, 0, 2)
	for _, source := range []string{record.Left.SourceID, record.Right.SourceID} {
		if source == "" || f.sourceByID[source] == nil || slices.Contains(sources, source) {
			continue
		}
		sources = append(sources, source)
	}
	slices.Sort(sources)
	return &ConflictLatchedError{
		Head: record.Head, Sequence: record.Sequence, Reason: record.Reason,
		EvidenceID: record.EvidenceID, Sources: sources,
	}
}

// loadConflictLatches runs before source-set activation, preserving that
// activation as New's final fallible durable step. Historical latches for heads
// not selected by this runtime stay on disk for future configurations; a latch
// for a configured mutable head is a corrupt/unsafe contract and fails startup.
func (f *Follower) loadConflictLatches() error {
	archiveID := *f.cfg.ExpectedArchiveID
	if err := f.state.validateConflictState(archiveID); err != nil {
		return fmt.Errorf("follow: validating durable conflict state: %w", err)
	}
	marker, markerExists, err := f.state.sourceSetMarker()
	if err != nil {
		return fmt.Errorf("follow: loading conflict-aware source-set marker: %w", err)
	}
	records, err := f.state.conflictLatches(archiveID)
	if err != nil {
		return fmt.Errorf("follow: loading durable conflict latches: %w", err)
	}
	if len(records) != 0 && (!markerExists || marker.archiveID != archiveID || marker.features&sourceSetFeatureConflictLatch == 0) {
		return fmt.Errorf("follow: durable conflict latches are not covered by a conflict-aware source-set marker")
	}
	for _, record := range records {
		if record.SourceSetRevision > marker.revision ||
			record.SourceSetRevision == marker.revision && record.SourceSetDigest != marker.digest {
			return fmt.Errorf("follow: durable conflict latch for head %q is not covered by the source-set marker generation", record.Head)
		}
		if _, configured := f.cfg.Heads[record.Head]; !configured {
			continue
		}
		if f.expectedKind(record.Head) != server.FinalizedMonotonic {
			return fmt.Errorf("follow: durable conflict latch exists for configured non-finalized head %q", record.Head)
		}
		f.conflicts[record.Head] = record
	}
	return nil
}

func (f *Follower) configureSourceSetMetrics() {
	headSources := make(map[string][]string)
	conflictHeadSources := make(map[string][]string)
	for _, head := range f.Names() {
		for _, source := range f.sources {
			if source.allows(head) {
				headSources[head] = append(headSources[head], source.cfg.ID)
				if f.expectedKind(head) == server.FinalizedMonotonic {
					conflictHeadSources[head] = append(conflictHeadSources[head], source.cfg.ID)
				}
			}
		}
	}
	f.cfg.Metrics.ConfigureFollowSourceMetrics(headSources)
	f.cfg.Metrics.ConfigureFollowConflictMetrics(conflictHeadSources)
	for head := range f.conflicts {
		f.cfg.Metrics.FollowConflictActive(head, true)
	}
}

func (f *Follower) conflictLatch(head string) (ConflictRecord, bool) {
	record, ok := f.conflicts[head]
	return record, ok
}

// conflictLatchErrors materializes every configured active latch in stable
// head order. Poll joins these errors across all later failure paths: transport,
// recovery, retention, or callback health must never hide the operator-visible
// fact that advancement is already durably frozen.
func (f *Follower) conflictLatchErrors() []error {
	errList := make([]error, 0, len(f.conflicts))
	for _, head := range f.Names() {
		if record, ok := f.conflictLatch(head); ok {
			errList = append(errList, f.conflictLatchedError(record))
		}
	}
	return errList
}

func (f *Follower) conflictRequestForResult(result finalizedHeadPlanResult) (ConflictLatchRequest, bool, error) {
	archiveID := *f.cfg.ExpectedArchiveID
	set := f.cfg.SourceSet
	if set == nil {
		return ConflictLatchRequest{}, false, fmt.Errorf("follow: finalized conflict evidence requires a configured source set")
	}
	if result.conflict != nil {
		request, err := NewConflictLatchRequestFromError(archiveID, set.Revision, set.Digest, result.conflict)
		return request, true, err
	}
	if result.continuityConflict == nil {
		return ConflictLatchRequest{}, false, nil
	}
	if result.winner == nil || result.winner.runtimeSource == nil {
		return ConflictLatchRequest{}, true, fmt.Errorf("follow: finalized head %q conflict has no attributed fresh winner", result.name)
	}
	conflict := result.continuityConflict
	durable := ConflictCandidateSummary{
		Role: ConflictCandidateDurable, CheckpointVersion: result.prior.version,
		Root: conflict.RightRoot, SyncedTo: conflict.RightSyncedTo,
		Covered: conflict.RightCovered, Manifest: conflict.RightManifest,
	}
	if result.prior.version == checkpointVersionV4 {
		durable.SourceID = result.prior.sourceID
		durable.Revision = result.prior.revision
		durable.Digest = result.prior.digest
	}
	request, err := NewConflictLatchRequest(ConflictLatchInput{
		ArchiveID: archiveID, Head: result.name, Reason: conflict.ReasonCode,
		SourceSetRevision: set.Revision, SourceSetDigest: set.Digest, PairCount: 1,
		Left: ConflictCandidateSummary{
			Role: ConflictCandidateSource, SourceID: result.winner.runtimeSource.cfg.ID,
			Revision: result.winner.revision, Digest: result.winner.digest,
			Root: conflict.LeftRoot, SyncedTo: conflict.LeftSyncedTo,
			Covered: conflict.LeftCovered, Manifest: conflict.LeftManifest,
		},
		Right: durable,
	})
	return request, true, err
}

// persistFinalizedConflicts makes the safety cut before any recovery closure or
// external-retention operation can fail. Every new latch, the irreversible
// marker feature, and the exact current publication floors participating in its
// evidence share one synced Pebble batch. In-memory state and telemetry change
// only after that batch commits.
func (f *Follower) persistFinalizedConflicts(results []finalizedHeadPlanResult, admissions map[string]sourceDocumentAdmission) error {
	type pendingLatch struct {
		request      ConflictLatchRequest
		participants []FinalizedClaimCandidate
	}
	pending := make([]pendingLatch, 0)
	for _, result := range results {
		request, conflict, err := f.conflictRequestForResult(result)
		if err != nil {
			return fmt.Errorf("follow: constructing durable conflict evidence for head %q: %w", result.name, err)
		}
		if conflict {
			pending = append(pending, pendingLatch{request: request, participants: result.conflictParticipants})
		}
	}
	if len(pending) == 0 {
		return nil
	}

	batch := f.cfg.KV.NewBatch()
	defer batch.Close()
	type stagedLatch struct {
		record  ConflictRecord
		created bool
	}
	staged := make([]stagedLatch, 0, len(pending))
	participantIDs := make(map[string]struct{})
	for _, item := range pending {
		record, created, err := f.state.stageConflictLatch(batch, item.request)
		if err != nil {
			return err
		}
		staged = append(staged, stagedLatch{record: record, created: created})
		if !created {
			continue
		}
		recordParticipant := false
		for _, endpoint := range []ConflictCandidateSummary{record.Left, record.Right} {
			if endpoint.Role != ConflictCandidateSource {
				continue
			}
			admission, ok := admissions[endpoint.SourceID]
			if !ok || admission.publication.revision != endpoint.Revision || admission.publication.digest != endpoint.Digest {
				return fmt.Errorf("follow: conflict evidence for head %q lacks its exact retained source %q admission", record.Head, endpoint.SourceID)
			}
			participantIDs[endpoint.SourceID] = struct{}{}
			recordParticipant = true
		}
		if !recordParticipant {
			return fmt.Errorf("follow: conflict evidence for head %q retains no current source endpoint", record.Head)
		}
		for _, participant := range item.participants {
			if participant.Document.Revision == nil {
				return fmt.Errorf("follow: conflict participant %q for head %q has no revision", participant.SourceID, record.Head)
			}
			digest, err := participant.Document.Unsigned.CanonicalDigest()
			if err != nil {
				return fmt.Errorf("follow: hashing conflict participant %q for head %q: %w", participant.SourceID, record.Head, err)
			}
			admission, ok := admissions[participant.SourceID]
			if !ok || admission.publication.revision != *participant.Document.Revision || admission.publication.digest != digest {
				return fmt.Errorf("follow: conflict evidence for head %q lacks the exact current source %q admission", record.Head, participant.SourceID)
			}
			participantIDs[participant.SourceID] = struct{}{}
		}
	}
	participantAdmissions := make([]sourceDocumentAdmission, 0, len(participantIDs))
	for _, source := range f.sources {
		if _, participating := participantIDs[source.cfg.ID]; participating {
			participantAdmissions = append(participantAdmissions, admissions[source.cfg.ID])
		}
	}
	if err := f.state.stageSourceAdmission(batch, *f.cfg.ExpectedArchiveID, nil, participantAdmissions); err != nil {
		return err
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		return fmt.Errorf("follow: committing durable finalized conflict evidence: %w", err)
	}

	for _, item := range staged {
		f.conflicts[item.record.Head] = item.record
		f.cfg.Metrics.FollowConflictActive(item.record.Head, true)
		f.cfg.Metrics.FollowIncomparableActive(item.record.Head, false)
		if !item.created {
			continue
		}
		sources := f.conflictLatchedError(item.record).Sources
		for _, source := range sources {
			f.cfg.Metrics.FollowConflictCreated(item.record.Head, source)
		}
		f.log.Error("durable multi-writer conflict latch created",
			"head", item.record.Head, "reason", item.record.Reason.String(),
			"sequence", item.record.Sequence, "evidence_id", fmt.Sprintf("sha256:%x", item.record.EvidenceID),
			"sources", sources, "pair_count", item.record.PairCount)
	}
	return nil
}

// sourceDocumentTouchesLatchedHead enforces the whole-document callback trust
// boundary. A raw signed document carrying a covered line for a latched head is
// not retained or advertised by the callback, even if another line in that same
// document advanced safely. Admissions and unrelated serving plans remain
// eligible. Omission and explicit empty lines carry no disputed pointer.
func (f *Follower) sourceDocumentTouchesLatchedHead(source *sourceRuntime, document server.Doc) bool {
	for _, entry := range document.Heads {
		if entry.SyncedTo == nil || !source.allows(entry.Name) {
			continue
		}
		if _, latched := f.conflicts[entry.Name]; latched {
			return true
		}
	}
	return false
}
