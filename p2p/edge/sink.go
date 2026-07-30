package edge

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ipfs/boxo/ipns"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/routing"
	ma "github.com/multiformats/go-multiaddr"

	"github.com/blobarchive/bloar/metrics"
	"github.com/blobarchive/bloar/p2p"
	"github.com/blobarchive/bloar/server"
)

const stateSchema = 1
const maxAllowedHeads = 64

// BlockNotifier wakes existing Bitswap wantlists after the document enters the
// edge's memory-backed document cache.
type BlockNotifier interface {
	NotifyNewBlocks(context.Context, ...blocks.Block) error
}

// SinkConfig pins every authority the public edge is willing to relay. A
// process that can write the Unix socket still cannot turn the edge into an
// arbitrary IPNS signer/provider: it has neither private key, and submitted
// bytes must verify against these public trust pins.
type SinkConfig struct {
	Name               ipns.Name
	DocumentPublicKey  ed25519.PublicKey
	Network            string
	ArchiveID          server.ArchiveID
	EdgePeer           peer.ID
	Documents          *p2p.DocBlockstore
	Provider           p2p.DocumentProvider
	Routing            routing.ValueStore
	Notifier           BlockNotifier
	Pointers           PointerPlanner
	StateFile          string
	MaxDocumentBytes   int
	TransactionTimeout time.Duration
	Metrics            *metrics.Metrics
	Logger             *slog.Logger
	// AllowAdditionalPeers is only for the additive mirror canary, whose one
	// canonical document names both the incumbent and edge peers. Final split
	// mode leaves it false and refuses every advertised peer except EdgePeer.
	AllowAdditionalPeers bool
	// AllowedHeads is an exact publication contract, never a wildcard. Every
	// required head must appear and no unconfigured head may be relayed.
	// Only mutable heads may be optional, to model quarantine withdrawal.
	AllowedHeads map[string]HeadPolicy
}

// HeadPolicy pins the signed contract the edge will relay for one head.
type HeadPolicy struct {
	Kind        server.HeadKind
	HandoffHead string
	Required    bool
}

// Sink verifies, durably stages, provides, and finally routes one signed
// publication. It contains no private signing material.
type Sink struct {
	name                 ipns.Name
	docKey               ed25519.PublicKey
	network              string
	archiveID            server.ArchiveID
	edgePeer             peer.ID
	docs                 *p2p.DocBlockstore
	provider             p2p.DocumentProvider
	routing              routing.ValueStore
	notifier             BlockNotifier
	pointers             PointerPlanner
	stateFile            string
	maxDoc               int
	maxRequest           int64
	transactionTimeout   time.Duration
	metrics              *metrics.Metrics
	log                  *slog.Logger
	allowAdditionalPeers bool
	allowedHeads         map[string]HeadPolicy

	transaction chan struct{}
	stateMu     sync.Mutex
	current     *diskState
	ready       atomic.Bool
}

type diskState struct {
	Schema   int    `json:"schema"`
	Name     string `json:"name"`
	CID      string `json:"cid"`
	Sequence uint64 `json:"sequence"`
	Revision uint64 `json:"revision"`
	Document []byte `json:"document"`
	Record   []byte `json:"record"`
}

type validatedPublication struct {
	state *diskState
	block blocks.Block
	claim server.Doc
}

// NewSink loads but does not announce durable state. Restore performs that
// network work; readiness remains false until Restore or Apply completes.
func NewSink(cfg SinkConfig) (*Sink, error) {
	switch {
	case cfg.Name.String() == "":
		return nil, errors.New("edge: writer IPNS name is required")
	case len(cfg.DocumentPublicKey) != ed25519.PublicKeySize:
		return nil, fmt.Errorf("edge: document public key is %d bytes, want %d", len(cfg.DocumentPublicKey), ed25519.PublicKeySize)
	case cfg.Network == "":
		return nil, errors.New("edge: network is required")
	case cfg.ArchiveID.IsZero():
		return nil, errors.New("edge: archive ID is required")
	case cfg.EdgePeer == "":
		return nil, errors.New("edge: edge peer ID is required")
	case cfg.Name.Peer() == cfg.EdgePeer:
		return nil, errors.New("edge: public edge PeerID must differ from the private IPNS authority PeerID")
	case cfg.Documents == nil:
		return nil, errors.New("edge: document blockstore is required")
	case cfg.Provider == nil:
		return nil, errors.New("edge: document provider is required")
	case cfg.Routing == nil:
		return nil, errors.New("edge: value routing is required")
	case cfg.Notifier == nil:
		return nil, errors.New("edge: block notifier is required")
	case cfg.Pointers == nil:
		return nil, errors.New("edge: pointer planner is required")
	case cfg.StateFile == "" || !filepath.IsAbs(cfg.StateFile):
		return nil, fmt.Errorf("edge: state file %q must be an absolute path", cfg.StateFile)
	case len(cfg.AllowedHeads) == 0 || len(cfg.AllowedHeads) > maxAllowedHeads:
		return nil, fmt.Errorf("edge: allowed heads has %d entries, want 1..%d", len(cfg.AllowedHeads), maxAllowedHeads)
	}
	allowed := make(map[string]HeadPolicy, len(cfg.AllowedHeads))
	for name, policy := range cfg.AllowedHeads {
		if name == "" {
			return nil, errors.New("edge: allowed heads contains an empty name")
		}
		switch policy.Kind {
		case server.FinalizedMonotonic:
			if policy.HandoffHead != "" {
				return nil, fmt.Errorf("edge: finalized head %q must not configure a handoff", name)
			}
			if !policy.Required {
				return nil, fmt.Errorf("edge: finalized head %q must be required; only mutable heads may be optional", name)
			}
		case server.UnfinalizedMutable:
			if policy.HandoffHead == "" {
				return nil, fmt.Errorf("edge: mutable head %q must configure handoff_head", name)
			}
		default:
			return nil, fmt.Errorf("edge: head %q has unsupported kind %q", name, policy.Kind)
		}
		allowed[name] = policy
	}
	for name, policy := range allowed {
		if policy.Kind != server.UnfinalizedMutable {
			continue
		}
		handoff, ok := allowed[policy.HandoffHead]
		if !ok || handoff.Kind != server.FinalizedMonotonic || !handoff.Required {
			return nil, fmt.Errorf("edge: mutable head %q handoff_head %q must name a required finalized head",
				name, policy.HandoffHead)
		}
	}
	if cfg.MaxDocumentBytes == 0 {
		cfg.MaxDocumentBytes = DefaultMaxDocumentBytes
	}
	if cfg.MaxDocumentBytes < 1 {
		return nil, fmt.Errorf("edge: max document bytes is %d, must be positive", cfg.MaxDocumentBytes)
	}
	if cfg.TransactionTimeout == 0 {
		cfg.TransactionTimeout = DefaultTransactionTimeout
	}
	if cfg.TransactionTimeout <= 0 || cfg.TransactionTimeout >= DefaultControlWriteTimeout {
		return nil, fmt.Errorf("edge: transaction timeout %s must be positive and shorter than server write timeout %s",
			cfg.TransactionTimeout, DefaultControlWriteTimeout)
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.DiscardHandler)
	}
	s := &Sink{
		name:                 cfg.Name,
		docKey:               append(ed25519.PublicKey(nil), cfg.DocumentPublicKey...),
		network:              cfg.Network,
		archiveID:            cfg.ArchiveID,
		edgePeer:             cfg.EdgePeer,
		docs:                 cfg.Documents,
		provider:             cfg.Provider,
		routing:              cfg.Routing,
		notifier:             cfg.Notifier,
		pointers:             cfg.Pointers,
		stateFile:            cfg.StateFile,
		maxDoc:               cfg.MaxDocumentBytes,
		maxRequest:           int64(2*cfg.MaxDocumentBytes + 2*ipns.MaxRecordSize + 4096),
		transactionTimeout:   cfg.TransactionTimeout,
		metrics:              cfg.Metrics,
		log:                  cfg.Logger,
		allowAdditionalPeers: cfg.AllowAdditionalPeers,
		allowedHeads:         allowed,
		transaction:          make(chan struct{}, 1),
	}
	state, err := s.loadState()
	if err != nil {
		return nil, err
	}
	s.current = state
	return s, nil
}

// Ready is true only after this process has successfully restored or accepted
// a complete provider-before-record transaction.
func (s *Sink) Ready() bool { return s.ready.Load() }

// HasState reports whether there is a durable current document to restore.
func (s *Sink) HasState() bool {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	return s.current != nil
}

// Restore re-stages and re-announces the durable current document after an edge
// restart. The caller keeps readiness false and retries a transient failure.
func (s *Sink) Restore(ctx context.Context) (bool, error) {
	transactionCtx, cancel := context.WithTimeout(ctx, s.transactionTimeout)
	defer cancel()
	if err := s.acquireTransaction(transactionCtx); err != nil {
		return s.HasState(), &p2p.PublicationStageError{
			Stage: p2p.PublicationStageProvideDocument,
			Err:   fmt.Errorf("edge: waiting to restore publication transaction: %w", err),
		}
	}
	defer s.releaseTransaction()
	current := s.currentState()
	if current == nil {
		return false, nil
	}
	if err := s.applyLocked(transactionCtx, current.Document, current.Record, false); err != nil {
		return true, err
	}
	s.ready.Store(true)
	return true, nil
}

// Apply verifies and publishes one writer submission.
func (s *Sink) Apply(ctx context.Context, document, record []byte) error {
	transactionCtx, cancel := context.WithTimeout(ctx, s.transactionTimeout)
	defer cancel()
	if err := s.acquireTransaction(transactionCtx); err != nil {
		return &p2p.PublicationStageError{
			Stage: p2p.PublicationStageProvideDocument,
			Err:   fmt.Errorf("edge: waiting to begin publication transaction: %w", err),
		}
	}
	defer s.releaseTransaction()
	if err := s.applyLocked(transactionCtx, document, record, true); err != nil {
		return err
	}
	s.ready.Store(true)
	return nil
}

func (s *Sink) applyLocked(ctx context.Context, document, record []byte, persist bool) error {
	publication, err := s.validate(document, record)
	if err != nil {
		return &p2p.PublicationStageError{Stage: p2p.PublicationStageProvideDocument, Err: err}
	}
	state, block := publication.state, publication.block
	prior := s.currentState()
	if prior != nil {
		switch {
		case state.Sequence < prior.Sequence:
			return &p2p.PublicationStageError{
				Stage: p2p.PublicationStagePutRecord,
				Err:   fmt.Errorf("edge: IPNS sequence %d is below durable floor %d", state.Sequence, prior.Sequence),
			}
		case state.Sequence == prior.Sequence && state.CID != prior.CID:
			return &p2p.PublicationStageError{
				Stage: p2p.PublicationStagePutRecord,
				Err:   fmt.Errorf("edge: IPNS sequence %d changes CID from %s to %s", state.Sequence, prior.CID, state.CID),
			}
		case state.Revision < prior.Revision:
			return &p2p.PublicationStageError{
				Stage: p2p.PublicationStagePutRecord,
				Err:   fmt.Errorf("edge: publication revision %d is below durable floor %d", state.Revision, prior.Revision),
			}
		case state.Revision == prior.Revision && state.CID != prior.CID:
			return &p2p.PublicationStageError{
				Stage: p2p.PublicationStagePutRecord,
				Err:   fmt.Errorf("edge: publication revision %d changes CID from %s to %s", state.Revision, prior.CID, state.CID),
			}
		}
	}
	pointerPlan, err := s.pointers.PlanAuthenticated(block, publication.claim)
	if err != nil {
		s.metrics.PointerScheduleUpdate(metrics.OutcomeError)
		return &p2p.PublicationStageError{
			Stage: p2p.PublicationStageProvideDocument,
			Err:   fmt.Errorf("edge: planning exact pointer schedule: %w", err),
		}
	}

	s.docs.PutDoc(block)
	if err := s.notifier.NotifyNewBlocks(ctx, block); err != nil {
		return &p2p.PublicationStageError{
			Stage: p2p.PublicationStageProvideDocument,
			Err:   fmt.Errorf("edge: notifying publication document %s: %w", block.Cid(), err),
		}
	}
	provideStarted := time.Now()
	if err := s.provider.Provide(ctx, block.Cid(), true); err != nil {
		s.observeStage(metrics.IPNSStageProvideDocument, provideStarted, err)
		return &p2p.PublicationStageError{
			Stage: p2p.PublicationStageProvideDocument,
			Err:   fmt.Errorf("edge: providing publication document %s: %w", block.Cid(), err),
		}
	}
	s.observeStage(metrics.IPNSStageProvideDocument, provideStarted, nil)

	if persist && (prior == nil || state.Sequence != prior.Sequence || state.CID != prior.CID ||
		!bytes.Equal(state.Record, prior.Record) || !bytes.Equal(state.Document, prior.Document)) {
		if err := s.persistState(state); err != nil {
			return &p2p.PublicationStageError{Stage: p2p.PublicationStagePutRecord, Err: err}
		}
		s.setCurrentState(state)
	}
	putStarted := time.Now()
	if err := s.routing.PutValue(ctx, string(s.name.RoutingKey()), append([]byte(nil), record...)); err != nil {
		s.observeStage(metrics.IPNSStagePutRecord, putStarted, err)
		return &p2p.PublicationStageError{
			Stage: p2p.PublicationStagePutRecord,
			Err:   fmt.Errorf("edge: putting IPNS record for %s: %w", s.name, err),
		}
	}
	s.observeStage(metrics.IPNSStagePutRecord, putStarted, nil)
	// Pointer hints are auxiliary discovery redundancy. Their complete candidate
	// was validated before any mutation, but it becomes active only after the
	// load-bearing provider-before-IPNS transaction has definitively succeeded.
	// A local scheduling failure must not turn that successful remote commit
	// into a writer-visible error: the remote transaction has already completed.
	if err := pointerPlan.Commit(); err != nil {
		s.metrics.PointerScheduleUpdate(metrics.OutcomeError)
		s.log.Error("publication committed but exact pointer schedule was not replaced",
			"cid", block.Cid(), "revision", state.Revision, "err", err)
	} else {
		s.metrics.PointerScheduleUpdate(metrics.OutcomeOK)
	}
	return nil
}

func (s *Sink) acquireTransaction(ctx context.Context) error {
	select {
	case s.transaction <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Sink) releaseTransaction() { <-s.transaction }

func (s *Sink) currentState() *diskState {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	return s.current
}

func (s *Sink) setCurrentState(state *diskState) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	s.current = state
}

func (s *Sink) observeStage(stage string, started time.Time, err error) {
	outcome := metrics.OutcomeOK
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		outcome = metrics.EdgePublicationOutcomeTimeout
	case errors.Is(err, context.Canceled):
		outcome = metrics.EdgePublicationOutcomeCanceled
	case err != nil:
		outcome = metrics.OutcomeError
	}
	s.metrics.EdgePublicationStage(stage, outcome, time.Since(started))
}

func (s *Sink) validate(document, record []byte) (*validatedPublication, error) {
	if len(document) == 0 || len(document) > s.maxDoc {
		return nil, fmt.Errorf("edge: publication document is %d bytes, want 1..%d", len(document), s.maxDoc)
	}
	if len(record) == 0 || len(record) > ipns.MaxRecordSize {
		return nil, fmt.Errorf("edge: IPNS record is %d bytes, want 1..%d", len(record), ipns.MaxRecordSize)
	}
	block, err := p2p.NewDocumentBlock(document)
	if err != nil {
		return nil, err
	}
	recordCID, sequence, err := p2p.DecodeRecord(record, s.name)
	if err != nil {
		return nil, err
	}
	if sequence == 0 {
		return nil, errors.New("edge: IPNS sequence must be positive")
	}
	if !recordCID.Equals(block.Cid()) {
		return nil, fmt.Errorf("edge: IPNS record names %s, document bytes reproduce %s", recordCID, block.Cid())
	}

	var documentClaim server.Doc
	decoder := json.NewDecoder(bytes.NewReader(document))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&documentClaim); err != nil {
		return nil, fmt.Errorf("edge: decoding publication document: %w", err)
	}
	if err := requireJSONEOF(decoder); err != nil {
		return nil, fmt.Errorf("edge: decoding publication document: %w", err)
	}
	if err := documentClaim.Verify(); err != nil {
		return nil, fmt.Errorf("edge: verifying publication document: %w", err)
	}
	if err := documentClaim.ValidateContract(); err != nil {
		return nil, fmt.Errorf("edge: validating publication contract: %w", err)
	}
	claimedKey, err := hex.DecodeString(documentClaim.Pubkey)
	if err != nil {
		return nil, fmt.Errorf("edge: decoding publication signer: %w", err)
	}
	if len(claimedKey) != len(s.docKey) || subtle.ConstantTimeCompare(claimedKey, s.docKey) != 1 {
		return nil, errors.New("edge: publication document is signed by an unconfigured authority")
	}
	if documentClaim.Net != s.network {
		return nil, fmt.Errorf("edge: publication document is for net %q, want %q", documentClaim.Net, s.network)
	}
	if documentClaim.ArchiveID == nil || *documentClaim.ArchiveID != s.archiveID {
		return nil, fmt.Errorf("edge: publication document archive_id is %v, want %s", documentClaim.ArchiveID, s.archiveID)
	}
	if err := s.validateMultiaddrs(documentClaim.Multiaddrs); err != nil {
		return nil, err
	}
	if err := s.validateHeads(documentClaim.Heads); err != nil {
		return nil, err
	}
	if documentClaim.Revision == nil {
		return nil, errors.New("edge: publication document has no monotonic revision")
	}
	return &validatedPublication{
		state: &diskState{
			Schema: stateSchema, Name: s.name.String(), CID: block.Cid().String(), Sequence: sequence,
			Revision: *documentClaim.Revision,
			Document: append([]byte(nil), document...), Record: append([]byte(nil), record...),
		},
		block: block, claim: documentClaim,
	}, nil
}

func (s *Sink) validateHeads(heads []server.HeadEntry) error {
	if len(heads) > len(s.allowedHeads) {
		return fmt.Errorf("edge: publication contains %d heads, allowed contract contains only %d", len(heads), len(s.allowedHeads))
	}
	seen := make(map[string]struct{}, len(heads))
	for _, head := range heads {
		policy, allowed := s.allowedHeads[head.Name]
		if !allowed {
			return fmt.Errorf("edge: publication contains unconfigured head %q", head.Name)
		}
		if _, duplicate := seen[head.Name]; duplicate {
			return fmt.Errorf("edge: publication duplicates head %q", head.Name)
		}
		seen[head.Name] = struct{}{}
		if got := head.EffectiveKind(); got != policy.Kind {
			return fmt.Errorf("edge: head %q kind is %q, want %q", head.Name, got, policy.Kind)
		}
		if got := head.HandoffHead; got != policy.HandoffHead {
			return fmt.Errorf("edge: head %q handoff is %q, want %q", head.Name, got, policy.HandoffHead)
		}
	}
	for name, policy := range s.allowedHeads {
		if policy.Required {
			if _, present := seen[name]; !present {
				return fmt.Errorf("edge: publication omits required head %q", name)
			}
		}
	}
	return nil
}

func (s *Sink) validateMultiaddrs(raw []string) error {
	if len(raw) == 0 {
		return fmt.Errorf("edge: publication document has no edge multiaddr for peer %s", s.edgePeer)
	}
	foundEdge := false
	for i, text := range raw {
		address, err := ma.NewMultiaddr(text)
		if err != nil {
			return fmt.Errorf("edge: publication multiaddrs[%d] is invalid: %w", i, err)
		}
		info, err := peer.AddrInfoFromP2pAddr(address)
		if err != nil {
			return fmt.Errorf("edge: publication multiaddrs[%d] must end in /p2p/<peerid>: %w", i, err)
		}
		if info.ID == s.edgePeer {
			foundEdge = true
		} else if !s.allowAdditionalPeers {
			return fmt.Errorf("edge: publication multiaddrs[%d] names peer %s, want only edge peer %s",
				i, info.ID, s.edgePeer)
		}
	}
	if !foundEdge {
		return fmt.Errorf("edge: publication multiaddrs do not name this edge peer %s", s.edgePeer)
	}
	return nil
}

func (s *Sink) loadState() (*diskState, error) {
	info, err := os.Lstat(s.stateFile)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("edge: statting state %s: %w", s.stateFile, err)
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("edge: state %s is not a regular file", s.stateFile)
	}
	if info.Size() <= 0 || info.Size() > s.maxRequest {
		return nil, fmt.Errorf("edge: state %s is %d bytes, want 1..%d", s.stateFile, info.Size(), s.maxRequest)
	}
	file, err := os.Open(s.stateFile)
	if err != nil {
		return nil, fmt.Errorf("edge: opening state %s: %w", s.stateFile, err)
	}
	defer file.Close()
	decoder := json.NewDecoder(io.LimitReader(file, s.maxRequest+1))
	decoder.DisallowUnknownFields()
	var state diskState
	if err := decoder.Decode(&state); err != nil {
		return nil, fmt.Errorf("edge: decoding state %s: %w", s.stateFile, err)
	}
	if err := requireJSONEOF(decoder); err != nil {
		return nil, fmt.Errorf("edge: decoding state %s: %w", s.stateFile, err)
	}
	if state.Schema != stateSchema || state.Name != s.name.String() {
		return nil, fmt.Errorf("edge: state identifies schema/name %d/%q, want %d/%q",
			state.Schema, state.Name, stateSchema, s.name)
	}
	publication, err := s.validate(state.Document, state.Record)
	if err != nil {
		return nil, fmt.Errorf("edge: validating durable state %s: %w", s.stateFile, err)
	}
	if _, err := s.pointers.PlanAuthenticated(publication.block, publication.claim); err != nil {
		return nil, fmt.Errorf("edge: validating durable pointer snapshot %s: %w", s.stateFile, err)
	}
	validated := publication.state
	if state.CID != validated.CID || state.Sequence != validated.Sequence || state.Revision != validated.Revision {
		return nil, fmt.Errorf("edge: durable state metadata (%s,%d,%d) disagrees with signed bytes (%s,%d,%d)",
			state.CID, state.Sequence, state.Revision, validated.CID, validated.Sequence, validated.Revision)
	}
	return validated, nil
}

func (s *Sink) persistState(state *diskState) error {
	dir := filepath.Dir(s.stateFile)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("edge: creating state directory %s: %w", dir, err)
	}
	raw, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("edge: encoding durable publication: %w", err)
	}
	if int64(len(raw)) > s.maxRequest {
		return fmt.Errorf("edge: encoded durable publication is %d bytes, limit %d", len(raw), s.maxRequest)
	}
	temp, err := os.CreateTemp(dir, ".bloar-edge-state-*")
	if err != nil {
		return fmt.Errorf("edge: creating state temp in %s: %w", dir, err)
	}
	tempName := temp.Name()
	defer func() {
		_ = temp.Close()
		_ = os.Remove(tempName)
	}()
	if err := temp.Chmod(0o600); err != nil {
		return fmt.Errorf("edge: chmod state temp %s: %w", tempName, err)
	}
	writer := bufio.NewWriter(temp)
	if _, err := writer.Write(raw); err != nil {
		return fmt.Errorf("edge: writing state temp %s: %w", tempName, err)
	}
	if err := writer.Flush(); err != nil {
		return fmt.Errorf("edge: flushing state temp %s: %w", tempName, err)
	}
	if err := temp.Sync(); err != nil {
		return fmt.Errorf("edge: syncing state temp %s: %w", tempName, err)
	}
	if err := temp.Close(); err != nil {
		return fmt.Errorf("edge: closing state temp %s: %w", tempName, err)
	}
	if err := os.Rename(tempName, s.stateFile); err != nil {
		return fmt.Errorf("edge: installing state %s: %w", s.stateFile, err)
	}
	dirHandle, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("edge: opening state directory %s: %w", dir, err)
	}
	if err := dirHandle.Sync(); err != nil {
		_ = dirHandle.Close()
		return fmt.Errorf("edge: syncing state directory %s: %w", dir, err)
	}
	if err := dirHandle.Close(); err != nil {
		return fmt.Errorf("edge: closing state directory %s: %w", dir, err)
	}
	return nil
}

func requireJSONEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("trailing JSON value")
		}
		return err
	}
	return nil
}

// Handler returns the single-endpoint bounded control protocol.
func (s *Sink) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != ProtocolPath {
			http.NotFound(w, r)
			return
		}
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if r.Header.Get("Content-Type") != "application/json" {
			http.Error(w, "content type must be application/json", http.StatusUnsupportedMediaType)
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, s.maxRequest)
		decoder := json.NewDecoder(r.Body)
		decoder.DisallowUnknownFields()
		var wire wirePublication
		if err := decoder.Decode(&wire); err != nil {
			http.Error(w, "invalid publication: "+err.Error(), http.StatusBadRequest)
			return
		}
		if err := requireJSONEOF(decoder); err != nil {
			http.Error(w, "invalid publication: "+err.Error(), http.StatusBadRequest)
			return
		}
		if wire.Schema != protocolSchema || wire.Name != s.name.String() {
			http.Error(w, "publication schema or authority mismatch", http.StatusBadRequest)
			return
		}
		claimedTimeout, err := time.ParseDuration(r.Header.Get(transactionTimeoutHeader))
		if err != nil || claimedTimeout != s.transactionTimeout {
			http.Error(w, fmt.Sprintf("transaction timeout must be exactly %s", s.transactionTimeout), http.StatusConflict)
			return
		}
		if err := s.Apply(r.Context(), wire.Document, wire.Record); err != nil {
			stage := p2p.PublicationStagePutRecord
			var stageErr *p2p.PublicationStageError
			if errors.As(err, &stageErr) {
				stage = stageErr.Stage
			}
			w.Header().Set(stageHeader, string(stage))
			http.Error(w, err.Error(), http.StatusUnprocessableEntity)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
}

// CIDFromState is a small diagnostics seam used by the command and tests.
func (s *Sink) CIDFromState() cid.Cid {
	current := s.currentState()
	if current == nil {
		return cid.Undef
	}
	parsed, err := cid.Parse(current.CID)
	if err != nil {
		return cid.Undef
	}
	return parsed
}
