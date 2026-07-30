package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sync"

	"github.com/cockroachdb/pebble/v2"
	"github.com/ipfs/go-cid"

	"github.com/blobarchive/bloar/archive"
	"github.com/blobarchive/bloar/schema"
)

// prefixGeneration is the bounded writer-generation keyspace. There is exactly
// one value per head; accepting a new generation replaces it rather than
// appending history.
const prefixGeneration byte = 'g'

const (
	legacyGenerationStateVersion = 1
	generationStateVersion       = 2
)

// HeadPolicy selects the local contract for a configured head. A missing
// policy, or a policy with an omitted Kind, is the legacy finalized-monotonic
// contract.
//
// HandoffHead and MaxWindowSlots are required only for UnfinalizedMutable. The
// handoff head is the durable finalized head whose coverage permits the moving
// window to advance; MaxWindowSlots is an inclusive coverage-width ceiling.
type HeadPolicy struct {
	Kind           HeadKind
	HandoffHead    string
	MaxWindowSlots uint64
}

func (p HeadPolicy) effectiveKind() HeadKind {
	if p.Kind == "" {
		return FinalizedMonotonic
	}
	return p.Kind
}

// GenerationRow is one blob-bearing row in a complete mutable generation.
// Slots without blobs are omitted, exactly as in the refs API.
type GenerationRow struct {
	Slot            uint64   `json:"slot"`
	VersionedHashes []string `json:"versioned_hashes"`
}

// GenerationRequest is the authenticated request accepted by
// POST /bloar/v1/heads/{head}/generation. It is exported so archclient can use
// the server's wire names rather than maintaining a parallel private shape.
type GenerationRequest struct {
	ExpectedGeneration  uint64          `json:"expected_generation"`
	WindowStart         uint64          `json:"window_start"`
	SyncedTo            uint64          `json:"synced_to"`
	Rows                []GenerationRow `json:"rows"`
	SourceHeadRoot      string          `json:"source_head_root"`
	SourceFinalizedSlot uint64          `json:"source_finalized_slot"`
	SourceFinalizedRoot string          `json:"source_finalized_root"`
	// ObservedHandoffRoot and ObservedHandoffSyncedTo identify the exact
	// finalized archive generation the tracker read before constructing this
	// optimistic snapshot. They are part of the request digest: changing either
	// is a new CAS claim, never an exact retry.
	ObservedHandoffRoot     string `json:"observed_handoff_root"`
	ObservedHandoffSyncedTo uint64 `json:"observed_handoff_synced_to"`
}

// GenerationResponse reports the selected complete generation. NoOp is true
// only for an exact retry of the immediately preceding request.
type GenerationResponse struct {
	Generation  uint64 `json:"generation"`
	WindowStart uint64 `json:"window_start"`
	SyncedTo    uint64 `json:"synced_to"`
	Root        string `json:"root"`
	NoOp        bool   `json:"noop"`
}

// GenerationState is the one durable writer record for a mutable head. It is
// deliberately self-contained: after a crash it identifies the selected root,
// the exact normalized request which selected it, both upstream chain anchors,
// and the finalized handoff generation which made its moving origin safe.
//
// Generation zero is a kind baseline only. It is written before a fresh
// mutable name creates its initial empty engine, so a crash can never leave a
// root which a later configuration is free to reinterpret.
type GenerationState struct {
	V                       int      `json:"v"`
	Kind                    HeadKind `json:"kind"`
	Generation              uint64   `json:"generation"`
	RequestDigest           string   `json:"request_digest"`
	Root                    string   `json:"root"`
	WindowStart             uint64   `json:"window_start"`
	SyncedTo                uint64   `json:"synced_to"`
	SourceHeadRoot          string   `json:"source_head_root"`
	SourceHeadSlot          uint64   `json:"source_head_slot"`
	SourceFinalizedSlot     uint64   `json:"source_finalized_slot"`
	SourceFinalizedRoot     string   `json:"source_finalized_root"`
	ObservedHandoffRoot     string   `json:"observed_handoff_root"`
	ObservedHandoffSyncedTo uint64   `json:"observed_handoff_synced_to"`
	HandoffHead             string   `json:"handoff_head"`
	HandoffRoot             string   `json:"handoff_root"`
	HandoffSyncedTo         uint64   `json:"handoff_synced_to"`
}

// KindMismatchError reports an attempt to reinterpret a durable head name
// under another ordering contract.
type KindMismatchError struct {
	Name string
	Want HeadKind
	Got  HeadKind
}

func (e *KindMismatchError) Error() string {
	return fmt.Sprintf("server: head %q is durably %q, cannot open it as %q", e.Name, e.Got, e.Want)
}

// GenerationConflictError is the generation CAS refusal returned by the store
// and API. CurrentGeneration is always included so a stateless indexer can
// reread state and decide whether it sent an exact retry.
type GenerationConflictError struct {
	Head               string
	ExpectedGeneration uint64
	CurrentGeneration  uint64
	Reason             string
}

func (e *GenerationConflictError) Error() string {
	if e.Reason != "" {
		return fmt.Sprintf("server: generation conflict for head %q: expected %d, current %d: %s",
			e.Head, e.ExpectedGeneration, e.CurrentGeneration, e.Reason)
	}
	return fmt.Sprintf("server: generation conflict for head %q: expected %d, current %d",
		e.Head, e.ExpectedGeneration, e.CurrentGeneration)
}

// ErrGenerationOverflow is returned instead of wrapping a head's local
// generation counter. A wrapped counter would make an ancient request current.
var ErrGenerationOverflow = errors.New("server: mutable generation counter exhausted")

// GenerationStates is the persistence seam consumed by Heads. GenerationStore
// is the production implementation; the interface permits deterministic
// commit-failure tests.
type GenerationStates interface {
	Get(ctx context.Context, name string) (GenerationState, bool, error)
	EnsureKind(ctx context.Context, name string, kind HeadKind) (GenerationState, error)
	Commit(ctx context.Context, name string, expected uint64, root cid.Cid, next GenerationState) error
}

// GenerationStore stores a bounded generation record in Pebble. Commit writes
// the selected root mirror and generation record in one synced batch.
type GenerationStore struct {
	kv *pebble.DB
	mu sync.Mutex
}

// NewGenerationStore returns a generation store over kv. kv must be the same
// database used by the node's RootStore: Commit owns the atomic h+g batch.
func NewGenerationStore(kv *pebble.DB) *GenerationStore {
	return &GenerationStore{kv: kv}
}

func generationKey(name string) []byte {
	k := make([]byte, 0, 1+len(name))
	return append(append(k, prefixGeneration), name...)
}

// Get returns the current kind/generation record, or (_, false, nil) for a
// legacy name which has no baseline yet.
func (s *GenerationStore) Get(ctx context.Context, name string) (GenerationState, bool, error) {
	if err := ctx.Err(); err != nil {
		return GenerationState{}, false, err
	}
	if name == "" {
		return GenerationState{}, false, errors.New("server: head name must not be empty")
	}
	v, closer, err := s.kv.Get(generationKey(name))
	if errors.Is(err, pebble.ErrNotFound) {
		return GenerationState{}, false, nil
	}
	if err != nil {
		return GenerationState{}, false, fmt.Errorf("server: reading generation state of head %q: %w", name, err)
	}
	defer closer.Close()
	var st GenerationState
	if err := json.Unmarshal(v, &st); err != nil {
		return GenerationState{}, false, fmt.Errorf("server: decoding generation state of head %q: %w", name, err)
	}
	if st.V == generationStateVersion && st.Generation > 0 {
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(v, &fields); err != nil {
			return GenerationState{}, false, fmt.Errorf("server: decoding generation state fields of head %q: %w", name, err)
		}
		for _, field := range []string{
			"request_digest", "root", "window_start", "synced_to", "source_head_root", "source_head_slot",
			"source_finalized_slot", "source_finalized_root", "observed_handoff_root",
			"observed_handoff_synced_to", "handoff_head", "handoff_root", "handoff_synced_to",
		} {
			raw, ok := fields[field]
			if !ok {
				return GenerationState{}, false, fmt.Errorf("server: head %q generation %d is missing required v2 field %q",
					name, st.Generation, field)
			}
			if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
				return GenerationState{}, false, fmt.Errorf("server: head %q generation %d has null required v2 field %q",
					name, st.Generation, field)
			}
		}
	}
	if err := validateGenerationState(name, st); err != nil {
		return GenerationState{}, false, err
	}
	return st, true, nil
}

// EnsureKind establishes or checks the durable kind baseline. If a root exists
// without a baseline it is legacy finalized-monotonic by definition; therefore
// such a name can be baselined as finalized but cannot be activated as mutable.
func (s *GenerationStore) EnsureKind(ctx context.Context, name string, kind HeadKind) (GenerationState, error) {
	if kind == "" {
		kind = FinalizedMonotonic
	}
	if kind != FinalizedMonotonic && kind != UnfinalizedMutable {
		return GenerationState{}, fmt.Errorf("server: unknown head kind %q", kind)
	}
	if name == "" {
		return GenerationState{}, errors.New("server: head name must not be empty")
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	st, ok, err := s.Get(ctx, name)
	if err != nil {
		return GenerationState{}, err
	}
	if ok {
		if st.Kind != kind {
			return GenerationState{}, &KindMismatchError{Name: name, Want: kind, Got: st.Kind}
		}
		return st, nil
	}
	// A pre-feature root with no kind record is irreversibly legacy finalized.
	if _, closer, err := s.kv.Get(rootKey(name)); err == nil {
		closer.Close()
		if kind != FinalizedMonotonic {
			return GenerationState{}, &KindMismatchError{Name: name, Want: kind, Got: FinalizedMonotonic}
		}
	} else if !errors.Is(err, pebble.ErrNotFound) {
		return GenerationState{}, fmt.Errorf("server: checking legacy root of head %q: %w", name, err)
	}

	st = GenerationState{V: generationStateVersion, Kind: kind}
	b, err := json.Marshal(st)
	if err != nil {
		return GenerationState{}, err
	}
	if err := s.kv.Set(generationKey(name), b, syncWrite); err != nil {
		return GenerationState{}, fmt.Errorf("server: writing kind baseline of head %q: %w", name, err)
	}
	return st, nil
}

// Commit atomically selects next and updates the legacy root mirror. The CAS is
// repeated inside the store lock so two registry instances sharing this store
// cannot both commit the same local generation.
func (s *GenerationStore) Commit(ctx context.Context, name string, expected uint64, root cid.Cid, next GenerationState) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !root.Defined() {
		return fmt.Errorf("server: refusing to commit an undefined generation root for head %q", name)
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	cur, ok, err := s.Get(ctx, name)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("server: head %q has no durable kind baseline", name)
	}
	if cur.Generation != expected {
		return &GenerationConflictError{Head: name, ExpectedGeneration: expected, CurrentGeneration: cur.Generation}
	}
	if expected == math.MaxUint64 {
		return ErrGenerationOverflow
	}
	if next.Generation != expected+1 {
		return fmt.Errorf("server: internal: generation state for %q is %d, want %d", name, next.Generation, expected+1)
	}
	if next.Kind != cur.Kind {
		return &KindMismatchError{Name: name, Want: next.Kind, Got: cur.Kind}
	}
	if next.Root != root.String() {
		return fmt.Errorf("server: internal: generation state root %q differs from selected root %s", next.Root, root)
	}
	if err := validateGenerationState(name, next); err != nil {
		return err
	}
	b, err := json.Marshal(next)
	if err != nil {
		return fmt.Errorf("server: encoding generation state of head %q: %w", name, err)
	}
	batch := s.kv.NewBatch()
	defer batch.Close()
	if err := batch.Set(rootKey(name), root.Bytes(), nil); err != nil {
		return fmt.Errorf("server: staging generation root of head %q: %w", name, err)
	}
	if err := batch.Set(generationKey(name), b, nil); err != nil {
		return fmt.Errorf("server: staging generation state of head %q: %w", name, err)
	}
	if err := batch.Commit(syncWrite); err != nil {
		return fmt.Errorf("server: committing generation %d of head %q: %w", next.Generation, name, err)
	}
	return nil
}

func validateGenerationState(name string, st GenerationState) error {
	if st.V != legacyGenerationStateVersion && st.V != generationStateVersion {
		return fmt.Errorf("server: head %q generation state version is %d, want %d or legacy %d",
			name, st.V, generationStateVersion, legacyGenerationStateVersion)
	}
	if st.Kind != FinalizedMonotonic && st.Kind != UnfinalizedMutable {
		return fmt.Errorf("server: head %q generation state has unknown kind %q", name, st.Kind)
	}
	if st.Generation == 0 {
		return nil
	}
	if st.Kind != UnfinalizedMutable {
		return fmt.Errorf("server: finalized head %q has mutable generation %d", name, st.Generation)
	}
	if len(st.RequestDigest) != sha256.Size*2 {
		return fmt.Errorf("server: head %q generation %d has invalid request digest", name, st.Generation)
	}
	if _, err := hex.DecodeString(st.RequestDigest); err != nil {
		return fmt.Errorf("server: head %q generation %d has invalid request digest: %w", name, st.Generation, err)
	}
	for field, raw := range map[string]string{
		"root": st.Root, "handoff_root": st.HandoffRoot,
	} {
		if _, err := cid.Decode(raw); err != nil {
			return fmt.Errorf("server: head %q generation %d has invalid %s %q: %w", name, st.Generation, field, raw, err)
		}
	}
	if st.V == generationStateVersion {
		if _, err := cid.Decode(st.ObservedHandoffRoot); err != nil {
			return fmt.Errorf("server: head %q generation %d has invalid observed_handoff_root %q: %w",
				name, st.Generation, st.ObservedHandoffRoot, err)
		}
		if st.ObservedHandoffSyncedTo > st.HandoffSyncedTo {
			return fmt.Errorf("server: head %q generation %d observed handoff %d is above commit handoff %d",
				name, st.Generation, st.ObservedHandoffSyncedTo, st.HandoffSyncedTo)
		}
		if st.ObservedHandoffSyncedTo == st.HandoffSyncedTo && st.ObservedHandoffRoot != st.HandoffRoot {
			return fmt.Errorf("server: head %q generation %d changed handoff root without advancing its frontier", name, st.Generation)
		}
	}
	if _, err := parseBeaconRoot(st.SourceHeadRoot); err != nil {
		return fmt.Errorf("server: head %q generation %d has invalid source_head_root: %w", name, st.Generation, err)
	}
	if _, err := parseBeaconRoot(st.SourceFinalizedRoot); err != nil {
		return fmt.Errorf("server: head %q generation %d has invalid source_finalized_root: %w", name, st.Generation, err)
	}
	if st.WindowStart > st.SyncedTo {
		return fmt.Errorf("server: head %q generation %d ends before its window starts", name, st.Generation)
	}
	if st.SourceHeadSlot != st.SyncedTo {
		return fmt.Errorf("server: head %q generation %d source head slot %d differs from synced_to %d",
			name, st.Generation, st.SourceHeadSlot, st.SyncedTo)
	}
	if st.SourceFinalizedSlot > st.SourceHeadSlot {
		return fmt.Errorf("server: head %q generation %d finalized source slot %d is above head slot %d",
			name, st.Generation, st.SourceFinalizedSlot, st.SourceHeadSlot)
	}
	if st.HandoffHead == "" {
		return fmt.Errorf("server: head %q generation %d has no handoff head", name, st.Generation)
	}
	if st.V == generationStateVersion && st.HandoffSyncedTo > st.SourceFinalizedSlot {
		return fmt.Errorf("server: head %q generation %d handoff coverage %d is above source finalized slot %d",
			name, st.Generation, st.HandoffSyncedTo, st.SourceFinalizedSlot)
	}
	if st.WindowStart > st.HandoffSyncedTo && st.WindowStart-st.HandoffSyncedTo > 1 {
		return fmt.Errorf("server: head %q generation %d window starts at %d beyond handoff coverage %d",
			name, st.Generation, st.WindowStart, st.HandoffSyncedTo)
	}
	return nil
}

// normalizedGeneration is the canonical request claim hashed for exact-retry
// detection. ExpectedGeneration is intentionally absent: it is the CAS token,
// not content selected by the generation.
type normalizedGeneration struct {
	WindowStart             uint64          `json:"window_start"`
	SyncedTo                uint64          `json:"synced_to"`
	Rows                    []GenerationRow `json:"rows"`
	SourceHeadRoot          string          `json:"source_head_root"`
	SourceFinalizedSlot     uint64          `json:"source_finalized_slot"`
	SourceFinalizedRoot     string          `json:"source_finalized_root"`
	ObservedHandoffRoot     string          `json:"observed_handoff_root"`
	ObservedHandoffSyncedTo uint64          `json:"observed_handoff_synced_to"`
}

func normalizeGeneration(req GenerationRequest) (normalizedGeneration, []archive.RefRow, [32]byte, error) {
	headRoot, err := parseBeaconRoot(req.SourceHeadRoot)
	if err != nil {
		return normalizedGeneration{}, nil, [32]byte{}, fmt.Errorf("source_head_root: %w", err)
	}
	finalizedRoot, err := parseBeaconRoot(req.SourceFinalizedRoot)
	if err != nil {
		return normalizedGeneration{}, nil, [32]byte{}, fmt.Errorf("source_finalized_root: %w", err)
	}
	handoffRoot, err := cid.Decode(req.ObservedHandoffRoot)
	if err != nil {
		return normalizedGeneration{}, nil, [32]byte{}, fmt.Errorf("observed_handoff_root: %w", err)
	}
	rows := make([]archive.RefRow, 0, len(req.Rows))
	normalizedRows := make([]GenerationRow, 0, len(req.Rows))
	for i, row := range req.Rows {
		vhs := make([]schema.VersionedHash, 0, len(row.VersionedHashes))
		rendered := make([]string, 0, len(row.VersionedHashes))
		for _, raw := range row.VersionedHashes {
			vh, err := parseVH(raw)
			if err != nil {
				return normalizedGeneration{}, nil, [32]byte{}, fmt.Errorf("row %d (slot %d): %w", i, row.Slot, err)
			}
			vhs = append(vhs, vh)
			rendered = append(rendered, vhHex(vh))
		}
		rows = append(rows, archive.RefRow{Slot: row.Slot, VHs: vhs})
		normalizedRows = append(normalizedRows, GenerationRow{Slot: row.Slot, VersionedHashes: rendered})
	}
	n := normalizedGeneration{
		WindowStart: req.WindowStart, SyncedTo: req.SyncedTo, Rows: normalizedRows,
		SourceHeadRoot: renderBeaconRoot(headRoot), SourceFinalizedSlot: req.SourceFinalizedSlot,
		SourceFinalizedRoot: renderBeaconRoot(finalizedRoot), ObservedHandoffRoot: handoffRoot.String(),
		ObservedHandoffSyncedTo: req.ObservedHandoffSyncedTo,
	}
	b, err := json.Marshal(n)
	if err != nil {
		return normalizedGeneration{}, nil, [32]byte{}, err
	}
	return n, rows, sha256.Sum256(b), nil
}

func parseBeaconRoot(raw string) ([32]byte, error) {
	var root [32]byte
	if len(raw) != 2+hex.EncodedLen(len(root)) || len(raw) < 2 || raw[:2] != "0x" {
		return root, fmt.Errorf("%q is not a 0x-prefixed 32-byte beacon block root", raw)
	}
	b, err := hex.DecodeString(raw[2:])
	if err != nil {
		return root, fmt.Errorf("%q is not a hex beacon block root: %w", raw, err)
	}
	copy(root[:], b)
	return root, nil
}

func renderBeaconRoot(root [32]byte) string { return "0x" + hex.EncodeToString(root[:]) }
