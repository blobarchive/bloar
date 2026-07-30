package edge

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sync"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"

	"github.com/blobarchive/bloar/p2p"
	"github.com/blobarchive/bloar/p2p/pointerhint"
	"github.com/blobarchive/bloar/server"
)

// PointerPlan is an immutable, already-validated auxiliary hint update. Sink
// commits it only after the load-bearing provider-before-IPNS transaction has
// definitively succeeded.
type PointerPlan interface {
	Commit() error
}

// PointerPlanner derives exact hints only from the signed publication document
// which Sink has already authenticated against its configured authority,
// network, archive, edge identity, and head catalog. Implementations must not
// accept a caller-supplied CID list through the control protocol.
type PointerPlanner interface {
	PlanAuthenticated(blocks.Block, server.Doc) (PointerPlan, error)
}

type pointerSchedule interface {
	ValidateAllWithDocuments(map[string]pointerhint.Set, []cid.Cid) error
	ReplaceAllWithDocuments(map[string]pointerhint.Set, []cid.Cid) error
}

type verifiedDocumentState interface {
	StageCurrentAfterVerification([]blocks.Block) error
	ReplaceCurrentDocuments([]cid.Cid) error
}

// PointerState owns the edge's one exact-pointer schedule and the verified,
// GC-independent copy of its current signed publication document.
type PointerState struct {
	schedule  pointerSchedule
	documents verifiedDocumentState

	mu      sync.Mutex
	current *pointerSnapshot
}

type pointerSnapshot struct {
	heads    map[string]pointerhint.Set
	document blocks.Block
}

type pointerPlan struct {
	state *PointerState
	next  pointerSnapshot
}

// NewPointerState constructs the edge-local auxiliary hint owner. The
// coordinator performs bounded asynchronous DHT writes; documents is also the
// blockstore served by Bitswap.
func NewPointerState(
	coordinator *pointerhint.Coordinator,
	documents *pointerhint.VerifiedDocumentStore,
) (*PointerState, error) {
	if coordinator == nil {
		return nil, errors.New("edge: pointer coordinator is required")
	}
	if documents == nil {
		return nil, errors.New("edge: verified document store is required")
	}
	return newPointerState(coordinator, documents)
}

func newPointerState(schedule pointerSchedule, documents verifiedDocumentState) (*PointerState, error) {
	if schedule == nil {
		return nil, errors.New("edge: pointer schedule is required")
	}
	if documents == nil {
		return nil, errors.New("edge: verified document state is required")
	}
	return &PointerState{schedule: schedule, documents: documents}, nil
}

// PlanAuthenticated derives and validates one complete replacement without
// changing the active provider schedule. Authentication remains Sink's job;
// this method independently binds claim to document bytes and checks every
// pointer through the coordinator's exact production validator.
func (s *PointerState) PlanAuthenticated(document blocks.Block, claim server.Doc) (PointerPlan, error) {
	next, err := pointerSnapshotFromDocument(document, claim)
	if err != nil {
		return nil, err
	}
	extra := []cid.Cid{next.document.Cid()}
	if err := s.schedule.ValidateAllWithDocuments(next.heads, extra); err != nil {
		return nil, fmt.Errorf("edge: validating exact pointer snapshot: %w", err)
	}
	return &pointerPlan{state: s, next: next}, nil
}

func pointerSnapshotFromDocument(document blocks.Block, claim server.Doc) (pointerSnapshot, error) {
	if document == nil {
		return pointerSnapshot{}, errors.New("edge: pointer publication document is nil")
	}
	exact, err := p2p.NewDocumentBlock(document.RawData())
	if err != nil {
		return pointerSnapshot{}, fmt.Errorf("edge: hashing pointer publication document: %w", err)
	}
	if !exact.Cid().Equals(document.Cid()) {
		return pointerSnapshot{}, fmt.Errorf("edge: pointer publication document bytes do not match CID %s", document.Cid())
	}

	var decoded server.Doc
	decoder := json.NewDecoder(bytes.NewReader(exact.RawData()))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&decoded); err != nil {
		return pointerSnapshot{}, fmt.Errorf("edge: decoding pointer publication document: %w", err)
	}
	if err := requireJSONEOF(decoder); err != nil {
		return pointerSnapshot{}, fmt.Errorf("edge: decoding pointer publication document: %w", err)
	}
	if !reflect.DeepEqual(decoded, claim) {
		return pointerSnapshot{}, errors.New("edge: authenticated pointer claim does not match publication document bytes")
	}

	heads := make(map[string]pointerhint.Set, len(decoded.Heads))
	for _, entry := range decoded.Heads {
		set := pointerhint.Set{}
		// Match the publication contract used by the writer: an uncovered head
		// has no current root, manifest, or document hint.
		if entry.Root != "" && entry.SyncedTo != nil {
			root, err := cid.Decode(entry.Root)
			if err != nil {
				return pointerSnapshot{}, fmt.Errorf("edge: pointer head %q root is not a CID: %w", entry.Name, err)
			}
			set.Root = root
			if entry.Manifest != "" {
				manifest, err := cid.Decode(entry.Manifest)
				if err != nil {
					return pointerSnapshot{}, fmt.Errorf("edge: pointer head %q manifest is not a CID: %w", entry.Name, err)
				}
				set.Manifest = manifest
			}
		}
		heads[entry.Name] = set
	}
	return pointerSnapshot{heads: heads, document: exact}, nil
}

func (p *pointerPlan) Commit() error {
	if p == nil || p.state == nil {
		return errors.New("edge: pointer plan is nil")
	}
	return p.state.replace(p.next)
}

func (s *PointerState) replace(next pointerSnapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	nextHeads := clonePointerHeads(next.heads)
	nextDocuments := []cid.Cid{next.document.Cid()}

	if err := s.documents.StageCurrentAfterVerification([]blocks.Block{next.document}); err != nil {
		return s.withdrawAfterCommitFailure(fmt.Errorf("edge: staging current pointer document: %w", err))
	}
	if err := s.schedule.ReplaceAllWithDocuments(nextHeads, nextDocuments); err != nil {
		return s.withdrawAfterCommitFailure(fmt.Errorf("edge: replacing exact pointer schedule: %w", err))
	}
	if err := s.documents.ReplaceCurrentDocuments(nextDocuments); err != nil {
		return s.withdrawAfterCommitFailure(fmt.Errorf("edge: committing current pointer document: %w", err))
	}
	s.current = &pointerSnapshot{heads: nextHeads, document: next.document}
	return nil
}

// The load-bearing IPNS record already names next when replace is called.
// Restoring the previous local hint snapshot would therefore keep re-providing
// stale roots and a stale document for another provider-record lifetime. Fail
// closed by withdrawing the local schedule instead; remote records cannot be
// revoked and expire naturally.
func (s *PointerState) withdrawAfterCommitFailure(cause error) error {
	s.current = nil
	scheduleErr := s.schedule.ReplaceAllWithDocuments(nil, nil)
	documentsErr := s.documents.ReplaceCurrentDocuments(nil)
	if scheduleErr != nil || documentsErr != nil {
		return fmt.Errorf("%w (withdrawing pointer schedule: %v; clearing current document: %v)",
			cause, scheduleErr, documentsErr)
	}
	return cause
}

func clonePointerHeads(source map[string]pointerhint.Set) map[string]pointerhint.Set {
	if source == nil {
		return nil
	}
	result := make(map[string]pointerhint.Set, len(source))
	for name, set := range source {
		result[name] = set
	}
	return result
}
