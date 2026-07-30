package follow

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"time"
	"unicode/utf8"

	"github.com/cockroachdb/pebble/v2"
	"github.com/ipfs/go-cid"

	"github.com/blobarchive/bloar/server"
)

const (
	conflictStateEncodingV1 = byte(1)

	keySourceConflictActive   = "source_conflict:v1:"
	keySourceConflictSequence = "source_conflict_sequence:v1:"
	keySourceConflictHistory  = "source_conflict_history:v1:"

	maxConflictHeadBytes      = 255
	maxConflictCIDBytes       = 256
	maxConflictPairs          = maxSourceSetBindings * (maxSourceSetBindings - 1) / 2
	maxConflictClearHistory   = 16
	maxConflictOperatorBytes  = 63
	maxConflictClearNoteBytes = 512
	conflictEvidenceDomainV1  = "bloar.follow-conflict-evidence/v1\x00"
)

// ErrNoActiveConflictLatch lets offline tooling distinguish an already-clear
// head from malformed state or an evidence-ID mismatch.
var ErrNoActiveConflictLatch = errors.New("follow: no active conflict latch")

func validConflictReason(reason ConflictReason) bool {
	switch reason {
	case ConflictReasonEqualCoverageRootMismatch, ConflictReasonPrefixProjectionMismatch, ConflictReasonManifestBranch:
		return true
	default:
		return false
	}
}

// ConflictCandidateRole distinguishes a currently authenticated publication
// from the already accepted durable checkpoint it is being compared against.
// Keeping the role in evidence avoids inventing a source identity for legacy
// checkpoints while still binding their on-disk schema generation.
type ConflictCandidateRole uint8

const (
	ConflictCandidateSource ConflictCandidateRole = 1 + iota
	ConflictCandidateDurable
)

// ConflictCandidateSummary is the bounded, source-attributed portion of one
// authenticated finalized claim retained as operator evidence. Digest is the
// SHA-256 canonical document digest, not a transport-body hash.
type ConflictCandidateSummary struct {
	Role              ConflictCandidateRole
	CheckpointVersion uint8
	SourceID          string
	Revision          uint64
	Digest            [sha256.Size]byte

	Root     cid.Cid
	SyncedTo uint64
	Covered  bool
	Manifest cid.Cid
}

// ConflictLatchInput is the generic constructor input. It also covers a live
// claim compared with durable v4 checkpoint provenance, where no
// FinalizedClaimConflictError exists.
type ConflictLatchInput struct {
	ArchiveID server.ArchiveID
	Head      string
	Reason    ConflictReason

	SourceSetRevision uint64
	SourceSetDigest   [sha256.Size]byte
	PairCount         uint16

	Left  ConflictCandidateSummary
	Right ConflictCandidateSummary
}

// ConflictLatchRequest is a validated, canonically ordered request. Its fields
// are intentionally private so sequence allocation and evidence-ID generation
// cannot be bypassed by a caller.
type ConflictLatchRequest struct{ input ConflictLatchInput }

// ConflictRecord is one active durable latch. Sequence is a monotonic per-head
// occurrence number which survives clear; EvidenceID is the SHA-256 hash of the
// complete canonical record (including Sequence) under a versioned domain.
type ConflictRecord struct {
	ArchiveID server.ArchiveID
	Head      string
	Sequence  uint64
	Reason    ConflictReason

	SourceSetRevision uint64
	SourceSetDigest   [sha256.Size]byte
	PairCount         uint16

	Left       ConflictCandidateSummary
	Right      ConflictCandidateSummary
	EvidenceID [sha256.Size]byte
}

// ConflictClearRequest is the exact-evidence offline recovery operation.
// Operator and Note are bounded audit text; ClearedAt is normalized to UTC
// seconds in durable state.
type ConflictClearRequest struct {
	ArchiveID  server.ArchiveID
	Head       string
	EvidenceID [sha256.Size]byte
	ClearedAt  time.Time
	Operator   string
	Note       string
}

// ConflictClearRecord is one bounded operator-history entry.
type ConflictClearRecord struct {
	ArchiveID  server.ArchiveID
	Head       string
	Sequence   uint64
	EvidenceID [sha256.Size]byte
	ClearedAt  time.Time
	Operator   string
	Note       string
}

// NewConflictLatchRequest validates and canonically orders a generic evidence
// pair. This is the constructor used for fresh-vs-durable conflicts.
func NewConflictLatchRequest(input ConflictLatchInput) (ConflictLatchRequest, error) {
	if err := validateConflictIdentity(input.ArchiveID, input.Head); err != nil {
		return ConflictLatchRequest{}, err
	}
	if !validConflictReason(input.Reason) {
		return ConflictLatchRequest{}, fmt.Errorf("follow: conflict reason %d is unsupported", input.Reason)
	}
	if input.SourceSetRevision == 0 || input.SourceSetDigest == ([sha256.Size]byte{}) {
		return ConflictLatchRequest{}, errors.New("follow: conflict evidence requires a source-set revision and digest")
	}
	if input.PairCount == 0 || int(input.PairCount) > maxConflictPairs {
		return ConflictLatchRequest{}, fmt.Errorf("follow: conflict pair count %d is outside 1..%d", input.PairCount, maxConflictPairs)
	}
	if err := validateConflictCandidate(input.Left); err != nil {
		return ConflictLatchRequest{}, fmt.Errorf("follow: invalid left conflict candidate: %w", err)
	}
	if err := validateConflictCandidate(input.Right); err != nil {
		return ConflictLatchRequest{}, fmt.Errorf("follow: invalid right conflict candidate: %w", err)
	}
	if conflictCandidatesEqual(input.Left, input.Right) {
		return ConflictLatchRequest{}, errors.New("follow: conflict evidence repeats the same endpoint")
	}
	if input.Left.Role == ConflictCandidateDurable && input.Right.Role == ConflictCandidateDurable {
		return ConflictLatchRequest{}, errors.New("follow: conflict evidence cannot compare two durable checkpoints")
	}
	if input.Left.Role == ConflictCandidateSource && input.Right.Role == ConflictCandidateSource &&
		input.Left.SourceID == input.Right.SourceID {
		return ConflictLatchRequest{}, fmt.Errorf("follow: same-source publication conflict for %q belongs to signer-local equivocation", input.Left.SourceID)
	}
	if conflictCandidateLess(input.Right, input.Left) {
		input.Left, input.Right = input.Right, input.Left
	}
	if err := validateConflictReasonEvidence(input); err != nil {
		return ConflictLatchRequest{}, err
	}
	return ConflictLatchRequest{input: input}, nil
}

// NewConflictLatchRequestFromError deterministically retains one canonical
// pair from a bounded arbitration conflict while preserving the total pair
// count. The selected pair does not depend on source iteration order.
func NewConflictLatchRequestFromError(archiveID server.ArchiveID, sourceSetRevision uint64,
	sourceSetDigest [sha256.Size]byte, conflict *FinalizedClaimConflictError,
) (ConflictLatchRequest, error) {
	if conflict == nil || len(conflict.Conflicts) == 0 {
		return ConflictLatchRequest{}, errors.New("follow: cannot latch empty finalized conflict evidence")
	}
	if len(conflict.Conflicts) > maxConflictPairs {
		return ConflictLatchRequest{}, fmt.Errorf("follow: finalized conflict has %d pairs, maximum is %d", len(conflict.Conflicts), maxConflictPairs)
	}
	type candidate struct {
		request ConflictLatchRequest
		order   []byte
	}
	candidates := make([]candidate, 0, len(conflict.Conflicts))
	for i, pair := range conflict.Conflicts {
		if pair.Conflict == nil {
			return ConflictLatchRequest{}, fmt.Errorf("follow: finalized conflict pair %d has no cryptographic evidence", i)
		}
		if pair.Conflict.ArchiveID != archiveID || pair.Conflict.Head != conflict.Head {
			return ConflictLatchRequest{}, fmt.Errorf("follow: finalized conflict pair %d does not match archive/head", i)
		}
		left, err := conflictSummaryFromCandidate(pair.Left, pair.Conflict.LeftRoot,
			pair.Conflict.LeftSyncedTo, pair.Conflict.LeftCovered, pair.Conflict.LeftManifest)
		if err != nil {
			return ConflictLatchRequest{}, fmt.Errorf("follow: finalized conflict pair %d left candidate: %w", i, err)
		}
		right, err := conflictSummaryFromCandidate(pair.Right, pair.Conflict.RightRoot,
			pair.Conflict.RightSyncedTo, pair.Conflict.RightCovered, pair.Conflict.RightManifest)
		if err != nil {
			return ConflictLatchRequest{}, fmt.Errorf("follow: finalized conflict pair %d right candidate: %w", i, err)
		}
		request, err := NewConflictLatchRequest(ConflictLatchInput{
			ArchiveID: archiveID, Head: conflict.Head, Reason: pair.Conflict.ReasonCode,
			SourceSetRevision: sourceSetRevision, SourceSetDigest: sourceSetDigest,
			PairCount: uint16(len(conflict.Conflicts)), Left: left, Right: right,
		})
		if err != nil {
			return ConflictLatchRequest{}, fmt.Errorf("follow: finalized conflict pair %d: %w", i, err)
		}
		order, err := encodeConflictRequest(request.input, 0)
		if err != nil {
			return ConflictLatchRequest{}, err
		}
		candidates = append(candidates, candidate{request: request, order: order})
	}
	sort.Slice(candidates, func(i, j int) bool { return bytes.Compare(candidates[i].order, candidates[j].order) < 0 })
	return candidates[0].request, nil
}

func conflictSummaryFromCandidate(candidate FinalizedClaimCandidate, root cid.Cid, syncedTo uint64,
	covered bool, manifest cid.Cid,
) (ConflictCandidateSummary, error) {
	if candidate.Document.Revision == nil {
		return ConflictCandidateSummary{}, errors.New("document has no signer-local revision")
	}
	digest, err := candidate.Document.Unsigned.CanonicalDigest()
	if err != nil {
		return ConflictCandidateSummary{}, err
	}
	return ConflictCandidateSummary{
		Role: ConflictCandidateSource, SourceID: candidate.SourceID,
		Revision: *candidate.Document.Revision, Digest: digest,
		Root: root, SyncedTo: syncedTo, Covered: covered, Manifest: manifest,
	}, nil
}

func validateConflictIdentity(archiveID server.ArchiveID, head string) error {
	if archiveID.IsZero() {
		return errors.New("follow: conflict state requires a nonzero archive ID")
	}
	if len(head) == 0 || len(head) > maxConflictHeadBytes || !utf8.ValidString(head) || bytes.IndexByte([]byte(head), 0) >= 0 {
		return fmt.Errorf("follow: conflict head name is invalid or exceeds %d bytes", maxConflictHeadBytes)
	}
	return nil
}

func validateConflictCandidate(candidate ConflictCandidateSummary) error {
	switch candidate.Role {
	case ConflictCandidateSource:
		if candidate.CheckpointVersion != 0 {
			return errors.New("source candidate cannot carry a checkpoint version")
		}
		if err := validateSourceID(candidate.SourceID); err != nil {
			return err
		}
		if candidate.Revision == 0 || candidate.Digest == ([sha256.Size]byte{}) {
			return errors.New("source candidate requires a document revision and digest")
		}
	case ConflictCandidateDurable:
		if candidate.CheckpointVersion < checkpointVersionV1 || candidate.CheckpointVersion > checkpointVersionV4 {
			return fmt.Errorf("durable candidate checkpoint version %d is unsupported", candidate.CheckpointVersion)
		}
		if candidate.CheckpointVersion != checkpointVersionV4 {
			if candidate.Revision != 0 || candidate.Digest != ([sha256.Size]byte{}) {
				return errors.New("unattributed durable candidate cannot carry a source generation")
			}
			if candidate.SourceID != "" {
				return errors.New("only a v4 durable candidate can carry source provenance")
			}
		} else {
			if candidate.SourceID == "" {
				return errors.New("a v4 durable candidate requires source provenance")
			}
			if err := validateSourceID(candidate.SourceID); err != nil {
				return err
			}
			if candidate.Revision == 0 || candidate.Digest == ([sha256.Size]byte{}) {
				return errors.New("attributed durable candidate requires a source revision and digest")
			}
		}
	default:
		return fmt.Errorf("candidate role %d is unsupported", candidate.Role)
	}
	if !candidate.Root.Defined() {
		return errors.New("candidate root is undefined")
	}
	if len(candidate.Root.Bytes()) > maxConflictCIDBytes || candidate.Manifest.Defined() && len(candidate.Manifest.Bytes()) > maxConflictCIDBytes {
		return fmt.Errorf("candidate CID exceeds %d bytes", maxConflictCIDBytes)
	}
	if !candidate.Covered && candidate.SyncedTo != 0 {
		return errors.New("an uncovered candidate cannot have a nonzero synced_to")
	}
	return nil
}

func validateConflictReasonEvidence(input ConflictLatchInput) error {
	switch input.Reason {
	case ConflictReasonEqualCoverageRootMismatch:
		if input.Left.Covered != input.Right.Covered || input.Left.SyncedTo != input.Right.SyncedTo || input.Left.Root == input.Right.Root {
			return errors.New("follow: equal-coverage conflict evidence does not contain equal coverage with different roots")
		}
	case ConflictReasonPrefixProjectionMismatch:
		if input.Left.Covered == input.Right.Covered && input.Left.SyncedTo == input.Right.SyncedTo {
			return errors.New("follow: prefix-projection conflict evidence does not contain different coverage")
		}
	case ConflictReasonManifestBranch:
		if !input.Left.Manifest.Defined() || !input.Right.Manifest.Defined() || input.Left.Manifest == input.Right.Manifest {
			return errors.New("follow: manifest-branch conflict evidence does not contain two different manifest tips")
		}
	}
	return nil
}

func conflictCandidateLess(left, right ConflictCandidateSummary) bool {
	return bytes.Compare(conflictCandidateOrderBytes(left), conflictCandidateOrderBytes(right)) < 0
}

func conflictCandidateOrderBytes(candidate ConflictCandidateSummary) []byte {
	b := make([]byte, 0, 128)
	b = append(b, byte(candidate.Role), candidate.CheckpointVersion)
	b = appendLengthPrefixed8(b, []byte(candidate.SourceID))
	b = binary.BigEndian.AppendUint64(b, candidate.Revision)
	b = append(b, candidate.Digest[:]...)
	b = appendCID16(b, candidate.Root)
	b = binary.BigEndian.AppendUint64(b, candidate.SyncedTo)
	if candidate.Covered {
		b = append(b, 1)
	} else {
		b = append(b, 0)
	}
	return appendCID16(b, candidate.Manifest)
}

func conflictStateKey(prefix string, archiveID server.ArchiveID, head string) []byte {
	b := sourceArchivePrefix(prefix, archiveID)
	b = append(b, head...)
	return b
}

func encodeConflictRequest(input ConflictLatchInput, sequence uint64) ([]byte, error) {
	request, err := NewConflictLatchRequest(input)
	if err != nil {
		return nil, err
	}
	input = request.input
	b := make([]byte, 0, 512)
	b = append(b, conflictStateEncodingV1)
	b = append(b, input.ArchiveID[:]...)
	b = appendLengthPrefixed16(b, []byte(input.Head))
	b = binary.BigEndian.AppendUint64(b, sequence)
	b = append(b, byte(input.Reason))
	b = binary.BigEndian.AppendUint64(b, input.SourceSetRevision)
	b = append(b, input.SourceSetDigest[:]...)
	b = binary.BigEndian.AppendUint16(b, input.PairCount)
	b = appendConflictCandidate(b, input.Left)
	b = appendConflictCandidate(b, input.Right)
	return b, nil
}

func appendConflictCandidate(b []byte, candidate ConflictCandidateSummary) []byte {
	b = append(b, byte(candidate.Role), candidate.CheckpointVersion)
	b = appendLengthPrefixed8(b, []byte(candidate.SourceID))
	b = binary.BigEndian.AppendUint64(b, candidate.Revision)
	b = append(b, candidate.Digest[:]...)
	var flags byte
	if candidate.Covered {
		flags |= 1
	}
	if candidate.Manifest.Defined() {
		flags |= 2
	}
	b = append(b, flags)
	b = binary.BigEndian.AppendUint64(b, candidate.SyncedTo)
	b = appendCID16(b, candidate.Root)
	b = appendCID16(b, candidate.Manifest)
	return b
}

func appendLengthPrefixed8(b, value []byte) []byte {
	b = append(b, byte(len(value)))
	return append(b, value...)
}

func appendLengthPrefixed16(b, value []byte) []byte {
	b = binary.BigEndian.AppendUint16(b, uint16(len(value)))
	return append(b, value...)
}

func appendCID16(b []byte, value cid.Cid) []byte {
	if !value.Defined() {
		return binary.BigEndian.AppendUint16(b, 0)
	}
	return append(appendLengthPrefix16(b, len(value.Bytes())), value.Bytes()...)
}

func appendLengthPrefix16(b []byte, length int) []byte {
	return binary.BigEndian.AppendUint16(b, uint16(length))
}

func evidenceIDForCanonical(canonical []byte) [sha256.Size]byte {
	h := sha256.New()
	_, _ = h.Write([]byte(conflictEvidenceDomainV1))
	_, _ = h.Write(canonical)
	var out [sha256.Size]byte
	copy(out[:], h.Sum(nil))
	return out
}

func recordFromRequest(request ConflictLatchRequest, sequence uint64) (ConflictRecord, error) {
	if sequence == 0 {
		return ConflictRecord{}, errors.New("follow: conflict sequence must be positive")
	}
	canonical, err := encodeConflictRequest(request.input, sequence)
	if err != nil {
		return ConflictRecord{}, err
	}
	in := request.input
	return ConflictRecord{
		ArchiveID: in.ArchiveID, Head: in.Head, Sequence: sequence, Reason: in.Reason,
		SourceSetRevision: in.SourceSetRevision, SourceSetDigest: in.SourceSetDigest, PairCount: in.PairCount,
		Left: in.Left, Right: in.Right, EvidenceID: evidenceIDForCanonical(canonical),
	}, nil
}

func encodeConflictRecord(record ConflictRecord) ([]byte, error) {
	if record.Sequence == 0 {
		return nil, errors.New("follow: conflict record sequence must be positive")
	}
	request, err := NewConflictLatchRequest(ConflictLatchInput{
		ArchiveID: record.ArchiveID, Head: record.Head, Reason: record.Reason,
		SourceSetRevision: record.SourceSetRevision, SourceSetDigest: record.SourceSetDigest,
		PairCount: record.PairCount, Left: record.Left, Right: record.Right,
	})
	if err != nil {
		return nil, err
	}
	canonical, err := encodeConflictRequest(request.input, record.Sequence)
	if err != nil {
		return nil, err
	}
	want := evidenceIDForCanonical(canonical)
	if record.EvidenceID != want {
		return nil, errors.New("follow: conflict evidence ID does not match its canonical record")
	}
	return append(canonical, record.EvidenceID[:]...), nil
}

// StageConflictLatch stages one active latch, its sequence floor, and the
// irreversible source-set feature upgrade in the caller's atomic batch.
func StageConflictLatch(kv *pebble.DB, batch *pebble.Batch, request ConflictLatchRequest) (ConflictRecord, bool, error) {
	if kv == nil {
		return ConflictRecord{}, false, errors.New("follow: cannot stage conflict state with a nil database")
	}
	return (&state{kv: kv}).stageConflictLatch(batch, request)
}

func (s *state) stageConflictLatch(batch *pebble.Batch, request ConflictLatchRequest) (ConflictRecord, bool, error) {
	if batch == nil {
		return ConflictRecord{}, false, errors.New("follow: cannot stage a conflict latch in a nil batch")
	}
	validated, err := NewConflictLatchRequest(request.input)
	if err != nil {
		return ConflictRecord{}, false, err
	}
	request = validated
	marker, ok, err := s.sourceSetMarker()
	if err != nil {
		return ConflictRecord{}, false, err
	}
	if !ok {
		return ConflictRecord{}, false, errors.New("follow: cannot latch a conflict without an active source set")
	}
	input := request.input
	if marker.archiveID != input.ArchiveID || marker.revision != input.SourceSetRevision || marker.digest != input.SourceSetDigest {
		return ConflictRecord{}, false, errors.New("follow: conflict evidence does not match the active source-set generation")
	}
	if active, exists, err := s.conflictLatch(input.ArchiveID, input.Head); err != nil {
		return ConflictRecord{}, false, err
	} else if exists {
		if conflictRecordMatchesRequest(active, input) {
			return active, false, nil
		}
		return ConflictRecord{}, false, fmt.Errorf("follow: head %q already has active conflict evidence %x", input.Head, active.EvidenceID)
	}
	sequence, exists, err := s.conflictSequence(input.ArchiveID, input.Head)
	if err != nil {
		return ConflictRecord{}, false, err
	}
	history, err := s.conflictClearHistory(input.ArchiveID, input.Head)
	if err != nil {
		return ConflictRecord{}, false, err
	}
	if !exists {
		if len(history) != 0 {
			return ConflictRecord{}, false, fmt.Errorf("follow: head %q has conflict clear history without a sequence floor", input.Head)
		}
		sequence = 0
	} else if len(history) == 0 || history[len(history)-1].Sequence != sequence {
		return ConflictRecord{}, false, fmt.Errorf("follow: cleared conflict sequence for head %q is not covered by operator history", input.Head)
	}
	if sequence == ^uint64(0) {
		return ConflictRecord{}, false, fmt.Errorf("follow: conflict sequence for head %q is exhausted", input.Head)
	}
	record, err := recordFromRequest(request, sequence+1)
	if err != nil {
		return ConflictRecord{}, false, err
	}
	encoded, err := encodeConflictRecord(record)
	if err != nil {
		return ConflictRecord{}, false, err
	}
	if err := batch.Set(conflictStateKey(keySourceConflictActive, input.ArchiveID, input.Head), encoded, nil); err != nil {
		return ConflictRecord{}, false, fmt.Errorf("follow: staging active conflict evidence: %w", err)
	}
	if err := batch.Set(conflictStateKey(keySourceConflictSequence, input.ArchiveID, input.Head), encodeConflictSequence(record.Sequence), nil); err != nil {
		return ConflictRecord{}, false, fmt.Errorf("follow: staging conflict sequence: %w", err)
	}
	marker.features |= sourceSetFeatureConflictLatch
	markerBytes, err := encodeSourceSetMarker(marker)
	if err != nil {
		return ConflictRecord{}, false, err
	}
	if err := batch.Set(sourceSetMarkerKey, markerBytes, nil); err != nil {
		return ConflictRecord{}, false, fmt.Errorf("follow: staging conflict-aware source-set marker: %w", err)
	}
	return record, true, nil
}

func conflictRecordMatchesRequest(record ConflictRecord, input ConflictLatchInput) bool {
	return record.ArchiveID == input.ArchiveID && record.Head == input.Head && record.Reason == input.Reason &&
		record.SourceSetRevision == input.SourceSetRevision && record.SourceSetDigest == input.SourceSetDigest &&
		record.PairCount == input.PairCount && conflictCandidatesEqual(record.Left, input.Left) &&
		conflictCandidatesEqual(record.Right, input.Right)
}

func conflictCandidatesEqual(left, right ConflictCandidateSummary) bool {
	return left.Role == right.Role && left.CheckpointVersion == right.CheckpointVersion &&
		left.SourceID == right.SourceID && left.Revision == right.Revision && left.Digest == right.Digest &&
		left.Root == right.Root && left.SyncedTo == right.SyncedTo && left.Covered == right.Covered &&
		left.Manifest == right.Manifest
}

func encodeConflictSequence(sequence uint64) []byte {
	b := []byte{conflictStateEncodingV1}
	return binary.BigEndian.AppendUint64(b, sequence)
}

func decodeConflictSequence(b []byte) (uint64, error) {
	if len(b) != 9 || b[0] != conflictStateEncodingV1 {
		return 0, errors.New("follow: conflict sequence has an unsupported or truncated encoding")
	}
	sequence := binary.BigEndian.Uint64(b[1:])
	if sequence == 0 {
		return 0, errors.New("follow: conflict sequence is zero")
	}
	return sequence, nil
}

func (s *state) conflictSequence(archiveID server.ArchiveID, head string) (uint64, bool, error) {
	if err := validateConflictIdentity(archiveID, head); err != nil {
		return 0, false, err
	}
	v, closer, err := s.kv.Get(conflictStateKey(keySourceConflictSequence, archiveID, head))
	if errors.Is(err, pebble.ErrNotFound) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("follow: reading conflict sequence for head %q: %w", head, err)
	}
	defer closer.Close()
	sequence, err := decodeConflictSequence(v)
	return sequence, err == nil, err
}

// LoadConflictLatch loads one active record. ok=false means no active latch;
// the per-head sequence floor may still exist after a prior exact-ID clear.
func LoadConflictLatch(kv *pebble.DB, archiveID server.ArchiveID, head string) (ConflictRecord, bool, error) {
	if kv == nil {
		return ConflictRecord{}, false, errors.New("follow: cannot load conflict state from a nil database")
	}
	return (&state{kv: kv}).conflictLatch(archiveID, head)
}

func (s *state) conflictLatch(archiveID server.ArchiveID, head string) (ConflictRecord, bool, error) {
	if err := validateConflictIdentity(archiveID, head); err != nil {
		return ConflictRecord{}, false, err
	}
	v, closer, err := s.kv.Get(conflictStateKey(keySourceConflictActive, archiveID, head))
	if errors.Is(err, pebble.ErrNotFound) {
		return ConflictRecord{}, false, nil
	}
	if err != nil {
		return ConflictRecord{}, false, fmt.Errorf("follow: reading active conflict for head %q: %w", head, err)
	}
	defer closer.Close()
	record, err := decodeConflictRecord(v)
	if err != nil {
		return ConflictRecord{}, false, fmt.Errorf("follow: decoding active conflict for head %q: %w", head, err)
	}
	if record.ArchiveID != archiveID || record.Head != head {
		return ConflictRecord{}, false, fmt.Errorf("follow: active conflict key for head %q contains another archive/head", head)
	}
	sequence, ok, err := s.conflictSequence(archiveID, head)
	if err != nil {
		return ConflictRecord{}, false, err
	}
	if !ok || sequence != record.Sequence {
		return ConflictRecord{}, false, fmt.Errorf("follow: active conflict for head %q is not covered by its sequence floor", head)
	}
	if err := s.validateConflictRecordMarker(record); err != nil {
		return ConflictRecord{}, false, err
	}
	return record, true, nil
}

func (s *state) validateConflictRecordMarker(record ConflictRecord) error {
	marker, ok, err := s.sourceSetMarker()
	if err != nil {
		return err
	}
	if !ok || marker.archiveID != record.ArchiveID || marker.features&sourceSetFeatureConflictLatch == 0 {
		return fmt.Errorf("follow: active conflict for head %q is not covered by a conflict-aware source-set marker", record.Head)
	}
	return validateConflictRecordAgainstMarker(record, marker)
}

func validateConflictRecordAgainstMarker(record ConflictRecord, marker sourceSetMarker) error {
	if record.SourceSetRevision > marker.revision ||
		record.SourceSetRevision == marker.revision && record.SourceSetDigest != marker.digest {
		return fmt.Errorf("follow: active conflict for head %q is not covered by the source-set marker generation", record.Head)
	}
	return nil
}

// ListConflictLatches returns every active latch for one logical archive in
// stable head-name order.
func ListConflictLatches(kv *pebble.DB, archiveID server.ArchiveID) ([]ConflictRecord, error) {
	if kv == nil {
		return nil, errors.New("follow: cannot list conflict state from a nil database")
	}
	return (&state{kv: kv}).conflictLatches(archiveID)
}

func (s *state) conflictLatches(archiveID server.ArchiveID) ([]ConflictRecord, error) {
	if archiveID.IsZero() {
		return nil, errors.New("follow: conflict state requires a nonzero archive ID")
	}
	prefix := sourceArchivePrefix(keySourceConflictActive, archiveID)
	it, err := s.kv.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: prefixUpperBound(prefix)})
	if err != nil {
		return nil, fmt.Errorf("follow: opening active conflict scan: %w", err)
	}
	defer it.Close()
	records := make([]ConflictRecord, 0)
	for valid := it.First(); valid; valid = it.Next() {
		head := string(it.Key()[len(prefix):])
		record, err := decodeConflictRecord(it.Value())
		if err != nil {
			return nil, fmt.Errorf("follow: decoding active conflict for head %q: %w", head, err)
		}
		if record.ArchiveID != archiveID || record.Head != head {
			return nil, fmt.Errorf("follow: active conflict key for head %q contains another archive/head", head)
		}
		sequence, ok, err := s.conflictSequence(archiveID, head)
		if err != nil {
			return nil, err
		}
		if !ok || sequence != record.Sequence {
			return nil, fmt.Errorf("follow: active conflict for head %q is not covered by its sequence floor", head)
		}
		if err := s.validateConflictRecordMarker(record); err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	if err := it.Error(); err != nil {
		return nil, fmt.Errorf("follow: scanning active conflicts: %w", err)
	}
	return records, nil
}

type conflictHeadRows struct {
	active         *ConflictRecord
	sequence       *uint64
	history        []ConflictClearRecord
	historyPresent bool
}

// ValidateConflictState verifies the complete durable conflict namespace for
// one logical archive. Startup must call this even when there are no active
// latches: a cleared latch intentionally leaves its sequence and operator
// history behind, and those rows still require the v2 source-set capability
// floor which makes old binaries reject the store.
func ValidateConflictState(kv *pebble.DB, archiveID server.ArchiveID) error {
	if kv == nil {
		return errors.New("follow: cannot validate conflict state in a nil database")
	}
	return (&state{kv: kv}).validateConflictState(archiveID)
}

func (s *state) validateConflictState(archiveID server.ArchiveID) error {
	if archiveID.IsZero() {
		return errors.New("follow: conflict state requires a nonzero archive ID")
	}
	rows := make(map[string]*conflictHeadRows)
	for _, namespace := range []struct {
		prefix string
		kind   string
	}{
		{keySourceConflictActive, "active"},
		{keySourceConflictSequence, "sequence"},
		{keySourceConflictHistory, "history"},
	} {
		if err := s.scanConflictNamespace(archiveID, namespace.prefix, namespace.kind, rows); err != nil {
			return err
		}
	}
	marker, ok, err := s.sourceSetMarker()
	if err != nil {
		return err
	}
	if ok && marker.archiveID != archiveID {
		return fmt.Errorf("follow: source-set marker belongs to archive %s, configured archive is %s", marker.archiveID, archiveID)
	}
	if len(rows) == 0 {
		// The feature bit is irreversible and its first transition is committed
		// atomically with an active record and sequence floor. A clear preserves
		// the sequence plus operator history forever, so an aware marker with no
		// rows cannot be produced by any valid lifecycle: accepting it would turn
		// complete conflict-state loss into a silent global unfreeze.
		if ok && marker.features&sourceSetFeatureConflictLatch != 0 {
			return errors.New("follow: conflict-aware source-set marker has no durable conflict rows")
		}
		return nil
	}
	if !ok || marker.archiveID != archiveID || marker.features&sourceSetFeatureConflictLatch == 0 {
		return errors.New("follow: durable conflict rows are not covered by a conflict-aware source-set marker")
	}

	heads := make([]string, 0, len(rows))
	for head := range rows {
		heads = append(heads, head)
	}
	sort.Strings(heads)
	for _, head := range heads {
		row := rows[head]
		if row.sequence == nil {
			return fmt.Errorf("follow: conflict state for head %q has active/history rows without a sequence floor", head)
		}
		sequence := *row.sequence
		if row.active != nil {
			if row.active.Sequence != sequence {
				return fmt.Errorf("follow: active conflict for head %q has sequence %d, floor is %d", head, row.active.Sequence, sequence)
			}
			if err := validateConflictRecordAgainstMarker(*row.active, marker); err != nil {
				return err
			}
			switch {
			case sequence == 1 && row.historyPresent:
				return fmt.Errorf("follow: first conflict occurrence for head %q unexpectedly has clear history", head)
			case sequence > 1 && !row.historyPresent:
				return fmt.Errorf("follow: active conflict occurrence %d for head %q has no prior clear history", sequence, head)
			case row.historyPresent && row.history[len(row.history)-1].Sequence != sequence-1:
				return fmt.Errorf("follow: active conflict occurrence %d for head %q is not preceded by clear history sequence %d",
					sequence, head, row.history[len(row.history)-1].Sequence)
			}
			continue
		}
		if !row.historyPresent {
			return fmt.Errorf("follow: cleared conflict sequence %d for head %q has no operator history", sequence, head)
		}
		if got := row.history[len(row.history)-1].Sequence; got != sequence {
			return fmt.Errorf("follow: cleared conflict sequence for head %q is %d, operator history ends at %d", head, sequence, got)
		}
	}
	return nil
}

func (s *state) scanConflictNamespace(archiveID server.ArchiveID, prefix, kind string,
	rows map[string]*conflictHeadRows,
) error {
	lower := key(prefix)
	it, err := s.kv.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: prefixUpperBound(lower)})
	if err != nil {
		return fmt.Errorf("follow: opening conflict %s namespace: %w", kind, err)
	}
	defer it.Close()
	for valid := it.First(); valid; valid = it.Next() {
		keyBytes := it.Key()
		suffix := keyBytes[len(lower):]
		if len(suffix) < len(server.ArchiveID{})+2 || suffix[len(server.ArchiveID{})] != sourceStateKeySep {
			return fmt.Errorf("follow: conflict %s namespace contains a malformed key", kind)
		}
		var rowArchive server.ArchiveID
		copy(rowArchive[:], suffix[:len(rowArchive)])
		head := string(suffix[len(rowArchive)+1:])
		// Validate the attacker-controlled key suffix before including it in any
		// diagnostic. In particular, a corrupt row for another archive must not
		// turn an unbounded head suffix into an unbounded startup error/log line.
		if err := validateConflictIdentity(rowArchive, head); err != nil {
			return fmt.Errorf("follow: conflict %s row has invalid identity: %w", kind, err)
		}
		if rowArchive != archiveID {
			return fmt.Errorf("follow: conflict %s row for head %q belongs to archive %s, configured archive is %s",
				kind, head, rowArchive, archiveID)
		}
		row := rows[head]
		if row == nil {
			row = &conflictHeadRows{}
			rows[head] = row
		}
		switch kind {
		case "active":
			record, err := decodeConflictRecord(it.Value())
			if err != nil {
				return fmt.Errorf("follow: decoding active conflict for head %q: %w", head, err)
			}
			if record.ArchiveID != archiveID || record.Head != head {
				return fmt.Errorf("follow: active conflict key for head %q contains another archive/head", head)
			}
			row.active = &record
		case "sequence":
			sequence, err := decodeConflictSequence(it.Value())
			if err != nil {
				return fmt.Errorf("follow: decoding conflict sequence for head %q: %w", head, err)
			}
			row.sequence = &sequence
		case "history":
			history, err := decodeConflictClearHistory(archiveID, head, it.Value())
			if err != nil {
				return fmt.Errorf("follow: decoding conflict clear history for head %q: %w", head, err)
			}
			row.history = history
			row.historyPresent = true
		default:
			return fmt.Errorf("follow: internal unknown conflict namespace %q", kind)
		}
	}
	if err := it.Error(); err != nil {
		return fmt.Errorf("follow: scanning conflict %s namespace: %w", kind, err)
	}
	return nil
}

// ClearConflictLatch performs an offline, synchronous exact-evidence clear. It
// never changes checkpoints, source replay floors, or the per-head sequence.
func ClearConflictLatch(kv *pebble.DB, request ConflictClearRequest) (ConflictClearRecord, error) {
	if kv == nil {
		return ConflictClearRecord{}, errors.New("follow: cannot clear conflict state from a nil database")
	}
	s := &state{kv: kv}
	batch := kv.NewBatch()
	defer batch.Close()
	record, err := s.stageClearConflictLatch(batch, request)
	if err != nil {
		return ConflictClearRecord{}, err
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		return ConflictClearRecord{}, fmt.Errorf("follow: committing conflict clear: %w", err)
	}
	return record, nil
}

func (s *state) stageClearConflictLatch(batch *pebble.Batch, request ConflictClearRequest) (ConflictClearRecord, error) {
	if batch == nil {
		return ConflictClearRecord{}, errors.New("follow: cannot stage a conflict clear in a nil batch")
	}
	request, err := normalizeConflictClearRequest(request)
	if err != nil {
		return ConflictClearRecord{}, err
	}
	active, ok, err := s.conflictLatch(request.ArchiveID, request.Head)
	if err != nil {
		return ConflictClearRecord{}, err
	}
	if !ok {
		return ConflictClearRecord{}, fmt.Errorf("%w for head %q", ErrNoActiveConflictLatch, request.Head)
	}
	if active.EvidenceID != request.EvidenceID {
		return ConflictClearRecord{}, fmt.Errorf("follow: active evidence ID for head %q is %x, not %x", request.Head, active.EvidenceID, request.EvidenceID)
	}
	marker, markerOK, err := s.sourceSetMarker()
	if err != nil {
		return ConflictClearRecord{}, err
	}
	if !markerOK || marker.archiveID != request.ArchiveID || marker.features&sourceSetFeatureConflictLatch == 0 {
		return ConflictClearRecord{}, errors.New("follow: active conflict is not covered by a conflict-aware source-set marker")
	}
	history, err := s.conflictClearHistory(request.ArchiveID, request.Head)
	if err != nil {
		return ConflictClearRecord{}, err
	}
	switch {
	case active.Sequence == 1 && len(history) != 0:
		return ConflictClearRecord{}, fmt.Errorf("follow: first conflict occurrence for head %q unexpectedly has clear history", request.Head)
	case active.Sequence > 1 && len(history) == 0:
		return ConflictClearRecord{}, fmt.Errorf("follow: active conflict occurrence %d for head %q has no prior clear history",
			active.Sequence, request.Head)
	case len(history) != 0 && history[len(history)-1].Sequence != active.Sequence-1:
		return ConflictClearRecord{}, fmt.Errorf("follow: active conflict occurrence %d for head %q is not preceded by clear history sequence %d",
			active.Sequence, request.Head, history[len(history)-1].Sequence)
	}
	record := ConflictClearRecord{
		ArchiveID: request.ArchiveID, Head: request.Head, Sequence: active.Sequence,
		EvidenceID: active.EvidenceID, ClearedAt: request.ClearedAt, Operator: request.Operator, Note: request.Note,
	}
	history = append(history, record)
	if len(history) > maxConflictClearHistory {
		history = append([]ConflictClearRecord(nil), history[len(history)-maxConflictClearHistory:]...)
	}
	encodedHistory, err := encodeConflictClearHistory(request.ArchiveID, request.Head, history)
	if err != nil {
		return ConflictClearRecord{}, err
	}
	if err := batch.Delete(conflictStateKey(keySourceConflictActive, request.ArchiveID, request.Head), nil); err != nil {
		return ConflictClearRecord{}, fmt.Errorf("follow: staging active conflict deletion: %w", err)
	}
	if err := batch.Set(conflictStateKey(keySourceConflictSequence, request.ArchiveID, request.Head), encodeConflictSequence(active.Sequence), nil); err != nil {
		return ConflictClearRecord{}, fmt.Errorf("follow: preserving conflict sequence: %w", err)
	}
	if err := batch.Set(conflictStateKey(keySourceConflictHistory, request.ArchiveID, request.Head), encodedHistory, nil); err != nil {
		return ConflictClearRecord{}, fmt.Errorf("follow: staging conflict clear history: %w", err)
	}
	return record, nil
}

func normalizeConflictClearRequest(request ConflictClearRequest) (ConflictClearRequest, error) {
	if err := validateConflictIdentity(request.ArchiveID, request.Head); err != nil {
		return ConflictClearRequest{}, err
	}
	if request.EvidenceID == ([sha256.Size]byte{}) {
		return ConflictClearRequest{}, errors.New("follow: conflict clear requires an evidence ID")
	}
	if request.ClearedAt.IsZero() || request.ClearedAt.Unix() < 0 {
		return ConflictClearRequest{}, errors.New("follow: conflict clear requires a non-negative timestamp")
	}
	if len(request.Operator) == 0 || len(request.Operator) > maxConflictOperatorBytes ||
		!utf8.ValidString(request.Operator) || bytes.IndexByte([]byte(request.Operator), 0) >= 0 {
		return ConflictClearRequest{}, fmt.Errorf("follow: conflict clear operator is invalid or exceeds %d bytes", maxConflictOperatorBytes)
	}
	if len(request.Note) > maxConflictClearNoteBytes || !utf8.ValidString(request.Note) || bytes.IndexByte([]byte(request.Note), 0) >= 0 {
		return ConflictClearRequest{}, fmt.Errorf("follow: conflict clear note is invalid or exceeds %d bytes", maxConflictClearNoteBytes)
	}
	request.ClearedAt = time.Unix(request.ClearedAt.Unix(), 0).UTC()
	return request, nil
}

// LoadConflictClearHistory returns the retained oldest-to-newest clear audit
// entries for one head. The ring is deliberately bounded.
func LoadConflictClearHistory(kv *pebble.DB, archiveID server.ArchiveID, head string) ([]ConflictClearRecord, error) {
	if kv == nil {
		return nil, errors.New("follow: cannot load conflict history from a nil database")
	}
	return (&state{kv: kv}).conflictClearHistory(archiveID, head)
}

func (s *state) conflictClearHistory(archiveID server.ArchiveID, head string) ([]ConflictClearRecord, error) {
	if err := validateConflictIdentity(archiveID, head); err != nil {
		return nil, err
	}
	v, closer, err := s.kv.Get(conflictStateKey(keySourceConflictHistory, archiveID, head))
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("follow: reading conflict clear history for head %q: %w", head, err)
	}
	defer closer.Close()
	return decodeConflictClearHistory(archiveID, head, v)
}

func encodeConflictClearHistory(archiveID server.ArchiveID, head string, history []ConflictClearRecord) ([]byte, error) {
	if len(history) == 0 || len(history) > maxConflictClearHistory {
		return nil, fmt.Errorf("follow: conflict clear history has %d records, want 1..%d", len(history), maxConflictClearHistory)
	}
	b := []byte{conflictStateEncodingV1, byte(len(history))}
	var prior uint64
	for i, record := range history {
		if record.ArchiveID != archiveID || record.Head != head || record.Sequence == 0 || record.Sequence <= prior ||
			record.EvidenceID == ([sha256.Size]byte{}) {
			return nil, fmt.Errorf("follow: conflict clear history record %d is invalid or out of order", i)
		}
		if i != 0 && record.Sequence != prior+1 {
			return nil, fmt.Errorf("follow: conflict clear history record %d skips from sequence %d to %d", i, prior, record.Sequence)
		}
		normalized, err := normalizeConflictClearRequest(ConflictClearRequest{
			ArchiveID: archiveID, Head: head, EvidenceID: record.EvidenceID, ClearedAt: record.ClearedAt,
			Operator: record.Operator, Note: record.Note,
		})
		if err != nil {
			return nil, err
		}
		prior = record.Sequence
		b = binary.BigEndian.AppendUint64(b, record.Sequence)
		b = append(b, record.EvidenceID[:]...)
		b = binary.BigEndian.AppendUint64(b, uint64(normalized.ClearedAt.Unix()))
		b = appendLengthPrefixed8(b, []byte(normalized.Operator))
		b = appendLengthPrefixed16(b, []byte(normalized.Note))
	}
	return b, nil
}

type conflictDecoder struct {
	b []byte
	i int
}

func (d *conflictDecoder) take(n int) ([]byte, error) {
	if n < 0 || d.i > len(d.b)-n {
		return nil, errors.New("truncated encoding")
	}
	out := d.b[d.i : d.i+n]
	d.i += n
	return out, nil
}

func (d *conflictDecoder) u8() (byte, error) {
	b, err := d.take(1)
	if err != nil {
		return 0, err
	}
	return b[0], nil
}

func (d *conflictDecoder) u16() (uint16, error) {
	b, err := d.take(2)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint16(b), nil
}

func (d *conflictDecoder) u64() (uint64, error) {
	b, err := d.take(8)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(b), nil
}

func (d *conflictDecoder) bytes8(max int) ([]byte, error) {
	n, err := d.u8()
	if err != nil {
		return nil, err
	}
	if int(n) > max {
		return nil, errors.New("length exceeds bound")
	}
	return d.take(int(n))
}

func (d *conflictDecoder) bytes16(max int) ([]byte, error) {
	n, err := d.u16()
	if err != nil {
		return nil, err
	}
	if int(n) > max {
		return nil, errors.New("length exceeds bound")
	}
	return d.take(int(n))
}

func decodeConflictRecord(b []byte) (ConflictRecord, error) {
	d := conflictDecoder{b: b}
	version, err := d.u8()
	if err != nil || version != conflictStateEncodingV1 {
		return ConflictRecord{}, errors.New("follow: conflict record has an unsupported or truncated encoding")
	}
	archiveRaw, err := d.take(len(server.ArchiveID{}))
	if err != nil {
		return ConflictRecord{}, fmt.Errorf("follow: conflict record: %w", err)
	}
	var archiveID server.ArchiveID
	copy(archiveID[:], archiveRaw)
	headRaw, err := d.bytes16(maxConflictHeadBytes)
	if err != nil {
		return ConflictRecord{}, fmt.Errorf("follow: conflict record head: %w", err)
	}
	sequence, err := d.u64()
	if err != nil {
		return ConflictRecord{}, fmt.Errorf("follow: conflict record sequence: %w", err)
	}
	reason, err := d.u8()
	if err != nil {
		return ConflictRecord{}, fmt.Errorf("follow: conflict record reason: %w", err)
	}
	sourceRevision, err := d.u64()
	if err != nil {
		return ConflictRecord{}, fmt.Errorf("follow: conflict record source-set revision: %w", err)
	}
	digestRaw, err := d.take(sha256.Size)
	if err != nil {
		return ConflictRecord{}, fmt.Errorf("follow: conflict record source-set digest: %w", err)
	}
	var sourceDigest [sha256.Size]byte
	copy(sourceDigest[:], digestRaw)
	pairCount, err := d.u16()
	if err != nil {
		return ConflictRecord{}, fmt.Errorf("follow: conflict record pair count: %w", err)
	}
	left, err := decodeConflictCandidate(&d)
	if err != nil {
		return ConflictRecord{}, fmt.Errorf("follow: conflict record left candidate: %w", err)
	}
	right, err := decodeConflictCandidate(&d)
	if err != nil {
		return ConflictRecord{}, fmt.Errorf("follow: conflict record right candidate: %w", err)
	}
	idRaw, err := d.take(sha256.Size)
	if err != nil || d.i != len(d.b) {
		return ConflictRecord{}, errors.New("follow: conflict record has truncated or trailing data")
	}
	var evidenceID [sha256.Size]byte
	copy(evidenceID[:], idRaw)
	record := ConflictRecord{
		ArchiveID: archiveID, Head: string(headRaw), Sequence: sequence, Reason: ConflictReason(reason),
		SourceSetRevision: sourceRevision, SourceSetDigest: sourceDigest, PairCount: pairCount,
		Left: left, Right: right, EvidenceID: evidenceID,
	}
	canonical, err := encodeConflictRecord(record)
	if err != nil {
		return ConflictRecord{}, err
	}
	if !bytes.Equal(canonical, b) {
		return ConflictRecord{}, errors.New("follow: conflict record is not canonically encoded")
	}
	return record, nil
}

func decodeConflictCandidate(d *conflictDecoder) (ConflictCandidateSummary, error) {
	role, err := d.u8()
	if err != nil {
		return ConflictCandidateSummary{}, err
	}
	checkpointVersion, err := d.u8()
	if err != nil {
		return ConflictCandidateSummary{}, err
	}
	sourceRaw, err := d.bytes8(maxSourceIDBytes)
	if err != nil {
		return ConflictCandidateSummary{}, err
	}
	revision, err := d.u64()
	if err != nil {
		return ConflictCandidateSummary{}, err
	}
	digestRaw, err := d.take(sha256.Size)
	if err != nil {
		return ConflictCandidateSummary{}, err
	}
	flags, err := d.u8()
	if err != nil || flags&^byte(3) != 0 {
		return ConflictCandidateSummary{}, errors.New("unsupported candidate flags")
	}
	syncedTo, err := d.u64()
	if err != nil {
		return ConflictCandidateSummary{}, err
	}
	root, err := d.cid16(true)
	if err != nil {
		return ConflictCandidateSummary{}, err
	}
	manifest, err := d.cid16(false)
	if err != nil {
		return ConflictCandidateSummary{}, err
	}
	if manifest.Defined() != (flags&2 != 0) {
		return ConflictCandidateSummary{}, errors.New("manifest flag does not match candidate data")
	}
	var digest [sha256.Size]byte
	copy(digest[:], digestRaw)
	return ConflictCandidateSummary{
		Role: ConflictCandidateRole(role), CheckpointVersion: checkpointVersion,
		SourceID: string(sourceRaw), Revision: revision, Digest: digest,
		Root: root, SyncedTo: syncedTo, Covered: flags&1 != 0, Manifest: manifest,
	}, nil
}

func (d *conflictDecoder) cid16(required bool) (cid.Cid, error) {
	raw, err := d.bytes16(maxConflictCIDBytes)
	if err != nil {
		return cid.Undef, err
	}
	if len(raw) == 0 {
		if required {
			return cid.Undef, errors.New("required CID is empty")
		}
		return cid.Undef, nil
	}
	value, err := cid.Cast(raw)
	if err != nil {
		return cid.Undef, fmt.Errorf("invalid CID: %w", err)
	}
	if !bytes.Equal(value.Bytes(), raw) {
		return cid.Undef, errors.New("CID is not canonically encoded")
	}
	return value, nil
}

func decodeConflictClearHistory(archiveID server.ArchiveID, head string, b []byte) ([]ConflictClearRecord, error) {
	d := conflictDecoder{b: b}
	version, err := d.u8()
	if err != nil || version != conflictStateEncodingV1 {
		return nil, errors.New("follow: conflict clear history has an unsupported or truncated encoding")
	}
	count, err := d.u8()
	if err != nil || count == 0 || int(count) > maxConflictClearHistory {
		return nil, errors.New("follow: conflict clear history count is invalid")
	}
	history := make([]ConflictClearRecord, 0, count)
	for range int(count) {
		sequence, err := d.u64()
		if err != nil {
			return nil, errors.New("follow: conflict clear history is truncated")
		}
		idRaw, err := d.take(sha256.Size)
		if err != nil {
			return nil, errors.New("follow: conflict clear history is truncated")
		}
		seconds, err := d.u64()
		if err != nil || seconds > uint64(^uint64(0)>>1) {
			return nil, errors.New("follow: conflict clear history timestamp is invalid")
		}
		operator, err := d.bytes8(maxConflictOperatorBytes)
		if err != nil {
			return nil, errors.New("follow: conflict clear history operator is invalid")
		}
		note, err := d.bytes16(maxConflictClearNoteBytes)
		if err != nil {
			return nil, errors.New("follow: conflict clear history note is invalid")
		}
		var evidenceID [sha256.Size]byte
		copy(evidenceID[:], idRaw)
		history = append(history, ConflictClearRecord{
			ArchiveID: archiveID, Head: head, Sequence: sequence, EvidenceID: evidenceID,
			ClearedAt: time.Unix(int64(seconds), 0).UTC(), Operator: string(operator), Note: string(note),
		})
	}
	if d.i != len(d.b) {
		return nil, errors.New("follow: conflict clear history has trailing data")
	}
	canonical, err := encodeConflictClearHistory(archiveID, head, history)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(canonical, b) {
		return nil, errors.New("follow: conflict clear history is not canonically encoded")
	}
	return history, nil
}
