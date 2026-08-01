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
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/routing"
	ma "github.com/multiformats/go-multiaddr"

	"github.com/blobarchive/bloar/metrics"
	"github.com/blobarchive/bloar/p2p"
	"github.com/blobarchive/bloar/server"
)

const stateSchema = 1
const maxAllowedHeads = 64
const publicationDialPeerTimeout = 15 * time.Second

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
	// RoutingTableSize is an optional bounded diagnostics snapshot. The edge
	// command supplies the Amino DHT routing-table size; tests and alternate
	// routings may omit it. Peer identities never enter metrics.
	RoutingTableSize func() int
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
	admissionTimeout     time.Duration
	transactionTimeout   time.Duration
	metrics              *metrics.Metrics
	log                  *slog.Logger
	routingTableSize     func() int
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

type transactionTrace struct {
	operation string
	stage     string
	outcome   string
	started   time.Time
	wait      time.Duration

	state               *diskState
	durable             *diskState
	durableFloorUpdated bool

	provideObserved bool
	provideDuration time.Duration
	putObserved     bool
	putDuration     time.Duration

	provideBudgetObserved  bool
	provideBudgetRemaining time.Duration
	putBudgetObserved      bool
	putBudgetRemaining     time.Duration

	provideQueryObserved bool
	provideQuery         dhtQueryTrace
	putQueryObserved     bool
	putQuery             dhtQueryTrace
}

// dhtQueryTrace is a bounded aggregate of the events emitted by one routing
// operation. Counts and relative times expose query progress without retaining
// peer identities, addresses, keys, record bytes, or untrusted error strings.
// Value is emitted by PutValue immediately before a final peer RPC; it proves
// fan-out began, not that a remote peer stored the record.
type dhtQueryTrace struct {
	sendingQueries int
	peerResponses  int
	queryErrors    int
	peerDials      int
	closerPeers    int
	maxCloserPeers int
	valueSends     int
	firstValue     time.Duration
	lastEvent      time.Duration
	lookup         dhtLookupTrace
}

type dhtLookupTrace struct {
	requests               int
	responses              int
	terminations           int
	heardTransitions       int
	waitingTransitions     int
	queriedTransitions     int
	unreachableTransitions int
	terminationObserved    bool
	terminationReason      string
	terminationElapsed     time.Duration
	terminationObservedAt  time.Time
	waitingAtTermination   int
	postTerminationWait    time.Duration
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
	admissionTimeout := transactionAdmissionTimeout(cfg.TransactionTimeout)
	if cfg.TransactionTimeout <= 0 || admissionTimeout >= DefaultControlWriteTimeout-cfg.TransactionTimeout {
		return nil, fmt.Errorf("edge: admission timeout %s plus transaction timeout %s must be positive and shorter than server write timeout %s",
			admissionTimeout, cfg.TransactionTimeout, DefaultControlWriteTimeout)
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
		admissionTimeout:     admissionTimeout,
		transactionTimeout:   cfg.TransactionTimeout,
		metrics:              cfg.Metrics,
		log:                  cfg.Logger,
		routingTableSize:     cfg.RoutingTableSize,
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
func (s *Sink) Restore(ctx context.Context) (present bool, retErr error) {
	trace := newTransactionTrace(metrics.EdgePublicationOperationRestore)
	s.sampleRoutingTable()
	finishCtx := ctx
	defer func() { s.finishTransaction(finishCtx, trace, retErr) }()

	transactionCtx, cancel, err := s.beginTransaction(ctx, trace)
	if err != nil {
		trace.durable = s.currentState()
		return s.HasState(), &p2p.PublicationStageError{
			Stage: p2p.PublicationStageProvideDocument,
			Err:   fmt.Errorf("edge: waiting to restore publication transaction: %w", err),
		}
	}
	finishCtx = transactionCtx
	defer cancel()
	defer s.releaseTransaction()
	defer func() { trace.durable = s.currentState() }()

	// Apply can establish readiness while a restore attempt waits for the
	// permit. Recheck under that same permit so the obsolete restore cannot
	// reacquire the DHT path and compete with already-healthy publications.
	if s.Ready() {
		trace.stage = metrics.EdgePublicationStageComplete
		return s.HasState(), nil
	}

	current := s.currentState()
	if current == nil {
		trace.stage = metrics.EdgePublicationStageComplete
		return false, nil
	}
	trace.state = current
	state, err := s.applyLocked(transactionCtx, current.Document, current.Record, false, trace)
	if state != nil {
		trace.state = state
	}
	if err != nil {
		return true, err
	}
	s.ready.Store(true)
	return true, nil
}

// Apply verifies and publishes one writer submission.
func (s *Sink) Apply(ctx context.Context, document, record []byte) (retErr error) {
	trace := newTransactionTrace(metrics.EdgePublicationOperationPublish)
	s.sampleRoutingTable()
	finishCtx := ctx
	defer func() { s.finishTransaction(finishCtx, trace, retErr) }()

	transactionCtx, cancel, err := s.beginTransaction(ctx, trace)
	if err != nil {
		trace.durable = s.currentState()
		return &p2p.PublicationStageError{
			Stage: p2p.PublicationStageProvideDocument,
			Err:   fmt.Errorf("edge: waiting to begin publication transaction: %w", err),
		}
	}
	finishCtx = transactionCtx
	defer cancel()
	defer s.releaseTransaction()
	defer func() { trace.durable = s.currentState() }()

	state, err := s.applyLocked(transactionCtx, document, record, true, trace)
	if state != nil {
		trace.state = state
	}
	if err != nil {
		return err
	}
	s.ready.Store(true)
	return nil
}

func (s *Sink) applyLocked(
	ctx context.Context,
	document, record []byte,
	persist bool,
	trace *transactionTrace,
) (state *diskState, retErr error) {
	// Freeze the terminal outcome at the boundary that produced the error.
	// The transaction context can expire between this return and the outer
	// diagnostic defer; reclassifying there would make transaction telemetry
	// disagree with the stage observation that actually saw the failure.
	defer func() {
		if retErr != nil && trace.outcome == "" {
			trace.outcome = edgePublicationOutcome(ctx, trace.stage, retErr)
		}
	}()

	trace.stage = metrics.EdgePublicationStageValidation
	publication, err := s.validate(document, record)
	if err != nil {
		return nil, &p2p.PublicationStageError{Stage: p2p.PublicationStageProvideDocument, Err: err}
	}
	state, block := publication.state, publication.block
	trace.state = state
	prior := s.currentState()
	if prior != nil {
		switch {
		case state.Sequence < prior.Sequence:
			return state, &p2p.PublicationStageError{
				Stage: p2p.PublicationStagePutRecord,
				Err:   fmt.Errorf("edge: IPNS sequence %d is below durable floor %d", state.Sequence, prior.Sequence),
			}
		case state.Sequence == prior.Sequence && state.CID != prior.CID:
			return state, &p2p.PublicationStageError{
				Stage: p2p.PublicationStagePutRecord,
				Err:   fmt.Errorf("edge: IPNS sequence %d changes CID from %s to %s", state.Sequence, prior.CID, state.CID),
			}
		case state.Revision < prior.Revision:
			return state, &p2p.PublicationStageError{
				Stage: p2p.PublicationStagePutRecord,
				Err:   fmt.Errorf("edge: publication revision %d is below durable floor %d", state.Revision, prior.Revision),
			}
		case state.Revision == prior.Revision && state.CID != prior.CID:
			return state, &p2p.PublicationStageError{
				Stage: p2p.PublicationStagePutRecord,
				Err:   fmt.Errorf("edge: publication revision %d changes CID from %s to %s", state.Revision, prior.CID, state.CID),
			}
		}
	}
	pointerPlan, err := s.pointers.PlanAuthenticated(block, publication.claim)
	if err != nil {
		s.metrics.PointerScheduleUpdate(metrics.OutcomeError)
		return state, &p2p.PublicationStageError{
			Stage: p2p.PublicationStageProvideDocument,
			Err:   fmt.Errorf("edge: planning exact pointer schedule: %w", err),
		}
	}

	trace.stage = metrics.EdgePublicationStageProvideDocument
	s.docs.PutDoc(block)
	if err := s.notifier.NotifyNewBlocks(ctx, block); err != nil {
		return state, &p2p.PublicationStageError{
			Stage: p2p.PublicationStageProvideDocument,
			Err:   fmt.Errorf("edge: notifying publication document %s: %w", block.Cid(), err),
		}
	}
	trace.provideBudgetObserved = true
	trace.provideBudgetRemaining = remainingBudget(ctx)
	provideStarted := time.Now()
	trace.provideQueryObserved = true
	err = s.observeDHTQuery(
		ctx,
		metrics.IPNSStageProvideDocument,
		provideStarted,
		func(queryCtx context.Context) error {
			return s.provider.Provide(queryCtx, block.Cid(), true)
		},
		&trace.provideQuery,
	)
	if err != nil {
		trace.provideObserved = true
		trace.provideDuration, trace.outcome = s.observeStage(
			ctx,
			metrics.IPNSStageProvideDocument,
			provideStarted,
			err,
		)
		return state, &p2p.PublicationStageError{
			Stage: p2p.PublicationStageProvideDocument,
			Err:   fmt.Errorf("edge: providing publication document %s: %w", block.Cid(), err),
		}
	}
	trace.provideObserved = true
	trace.provideDuration, _ = s.observeStage(ctx, metrics.IPNSStageProvideDocument, provideStarted, nil)

	trace.stage = metrics.EdgePublicationStagePersistState
	if persist && (prior == nil || state.Sequence != prior.Sequence || state.CID != prior.CID ||
		!bytes.Equal(state.Record, prior.Record) || !bytes.Equal(state.Document, prior.Document)) {
		if err := s.persistState(state); err != nil {
			return state, &p2p.PublicationStageError{Stage: p2p.PublicationStagePutRecord, Err: err}
		}
		s.setCurrentState(state)
		trace.durableFloorUpdated = true
	}

	trace.stage = metrics.EdgePublicationStagePutRecord
	trace.putBudgetObserved = true
	trace.putBudgetRemaining = remainingBudget(ctx)
	putStarted := time.Now()
	trace.putQueryObserved = true
	err = s.observeDHTQuery(ctx, metrics.IPNSStagePutRecord, putStarted, func(queryCtx context.Context) error {
		return s.routing.PutValue(
			queryCtx,
			string(s.name.RoutingKey()),
			append([]byte(nil), record...),
		)
	}, &trace.putQuery)
	if err != nil {
		trace.putObserved = true
		trace.putDuration, trace.outcome = s.observeStage(
			ctx,
			metrics.IPNSStagePutRecord,
			putStarted,
			err,
		)
		return state, &p2p.PublicationStageError{
			Stage: p2p.PublicationStagePutRecord,
			Err:   fmt.Errorf("edge: putting IPNS record for %s: %w", s.name, err),
		}
	}
	trace.putObserved = true
	trace.putDuration, _ = s.observeStage(ctx, metrics.IPNSStagePutRecord, putStarted, nil)
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
	trace.stage = metrics.EdgePublicationStageComplete
	return state, nil
}

func (s *Sink) acquireTransaction(ctx context.Context) error {
	select {
	case s.transaction <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Sink) beginTransaction(
	ctx context.Context,
	trace *transactionTrace,
) (context.Context, context.CancelFunc, error) {
	admissionCtx, cancelAdmission := context.WithTimeout(ctx, s.admissionTimeout)
	waitStarted := time.Now()
	err := s.acquireTransaction(admissionCtx)
	trace.wait = time.Since(waitStarted)
	if err == nil {
		// A permit and the deadline can become ready in the same scheduler turn.
		// Refuse admission after the bound rather than silently beginning a fresh
		// work budget with a permit acquired by that race.
		if err = admissionCtx.Err(); err != nil {
			s.releaseTransaction()
		}
	}
	if err != nil {
		trace.outcome = edgePublicationOutcome(admissionCtx, trace.stage, err)
		s.metrics.EdgePublicationWait(trace.operation, trace.outcome, trace.wait)
		cancelAdmission()
		return nil, nil, err
	}
	cancelAdmission()
	s.metrics.EdgePublicationWait(trace.operation, metrics.OutcomeOK, trace.wait)
	transactionCtx, cancelTransaction := context.WithTimeout(ctx, s.transactionTimeout)
	return transactionCtx, cancelTransaction, nil
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

func newTransactionTrace(operation string) *transactionTrace {
	return &transactionTrace{
		operation: operation,
		stage:     metrics.EdgePublicationStageAdmission,
		started:   time.Now(),
	}
}

func (s *Sink) finishTransaction(ctx context.Context, trace *transactionTrace, err error) {
	elapsed := time.Since(trace.started)
	outcome := trace.outcome
	if outcome == "" {
		outcome = edgePublicationOutcome(ctx, trace.stage, err)
	}
	s.metrics.EdgePublicationTransaction(trace.operation, trace.stage, outcome, elapsed)

	routingPeers := s.sampleRoutingTable()

	attrs := []any{
		"operation", trace.operation,
		"stage", trace.stage,
		"outcome", outcome,
		"elapsed", elapsed,
		"wait", trace.wait,
		"admission_timeout", s.admissionTimeout,
		"transaction_timeout", s.transactionTimeout,
		"durable_floor_updated", trace.durableFloorUpdated,
	}
	if trace.operation == metrics.EdgePublicationOperationPublish {
		status := http.StatusNoContent
		if err != nil {
			status = http.StatusUnprocessableEntity
		}
		attrs = append(attrs, "http_status", status)
	}
	if trace.provideBudgetObserved {
		attrs = append(attrs, "budget_remaining_before_provide", trace.provideBudgetRemaining)
	}
	if trace.provideObserved {
		attrs = append(attrs, "provide_elapsed", trace.provideDuration)
	}
	if trace.provideQueryObserved {
		attrs = append(attrs,
			"provide_closest_peer_queries", trace.provideQuery.sendingQueries,
			"provide_closest_peer_responses", trace.provideQuery.peerResponses,
			"provide_closest_peer_errors", trace.provideQuery.queryErrors,
			"provide_peer_dials", trace.provideQuery.peerDials,
			"provide_closer_peers_returned", trace.provideQuery.closerPeers,
			"provide_max_closer_peers_per_response", trace.provideQuery.maxCloserPeers,
			"provide_query_last_event_after", trace.provideQuery.lastEvent,
			"provide_lookup_requests", trace.provideQuery.lookup.requests,
			"provide_lookup_responses", trace.provideQuery.lookup.responses,
			"provide_lookup_terminations", trace.provideQuery.lookup.terminations,
			"provide_lookup_heard_transitions", trace.provideQuery.lookup.heardTransitions,
			"provide_lookup_waiting_transitions", trace.provideQuery.lookup.waitingTransitions,
			"provide_lookup_queried_transitions", trace.provideQuery.lookup.queriedTransitions,
			"provide_lookup_unreachable_transitions", trace.provideQuery.lookup.unreachableTransitions,
		)
		if trace.provideQuery.lookup.terminationObserved {
			attrs = append(attrs,
				"provide_lookup_termination_reason", trace.provideQuery.lookup.terminationReason,
				"provide_lookup_termination_after", trace.provideQuery.lookup.terminationElapsed,
				"provide_lookup_waiting_at_termination", trace.provideQuery.lookup.waitingAtTermination,
				"provide_lookup_post_termination_wait", trace.provideQuery.lookup.postTerminationWait,
			)
		}
	}
	if trace.putBudgetObserved {
		attrs = append(attrs, "budget_remaining_before_put", trace.putBudgetRemaining)
	}
	if trace.putObserved {
		attrs = append(attrs, "put_elapsed", trace.putDuration)
	}
	if trace.putQueryObserved {
		attrs = append(attrs,
			"put_closest_peer_queries", trace.putQuery.sendingQueries,
			"put_closest_peer_responses", trace.putQuery.peerResponses,
			"put_closest_peer_errors", trace.putQuery.queryErrors,
			"put_peer_dials", trace.putQuery.peerDials,
			"put_closer_peers_returned", trace.putQuery.closerPeers,
			"put_max_closer_peers_per_response", trace.putQuery.maxCloserPeers,
			"put_value_rpc_attempts", trace.putQuery.valueSends,
			"put_query_last_event_after", trace.putQuery.lastEvent,
			"put_lookup_requests", trace.putQuery.lookup.requests,
			"put_lookup_responses", trace.putQuery.lookup.responses,
			"put_lookup_terminations", trace.putQuery.lookup.terminations,
			"put_lookup_heard_transitions", trace.putQuery.lookup.heardTransitions,
			"put_lookup_waiting_transitions", trace.putQuery.lookup.waitingTransitions,
			"put_lookup_queried_transitions", trace.putQuery.lookup.queriedTransitions,
			"put_lookup_unreachable_transitions", trace.putQuery.lookup.unreachableTransitions,
		)
		if trace.putQuery.valueSends > 0 {
			attrs = append(attrs, "put_value_rpc_phase_after", trace.putQuery.firstValue)
		}
		if trace.putQuery.lookup.terminationObserved {
			attrs = append(attrs,
				"put_lookup_termination_reason", trace.putQuery.lookup.terminationReason,
				"put_lookup_termination_after", trace.putQuery.lookup.terminationElapsed,
				"put_lookup_waiting_at_termination", trace.putQuery.lookup.waitingAtTermination,
				"put_lookup_post_termination_wait", trace.putQuery.lookup.postTerminationWait,
			)
		}
	}
	if trace.state != nil {
		attrs = append(attrs,
			"cid", trace.state.CID,
			"sequence", trace.state.Sequence,
			"revision", trace.state.Revision,
		)
	}
	if durable := trace.durable; durable != nil {
		attrs = append(attrs,
			"durable_cid", durable.CID,
			"durable_sequence", durable.Sequence,
			"durable_revision", durable.Revision,
		)
	}
	if routingPeers >= 0 {
		attrs = append(attrs, "routing_table_peers", routingPeers)
	}
	if err != nil {
		attrs = append(attrs, "err", err)
		s.log.Warn("edge publication transaction failed", attrs...)
		return
	}
	s.log.Debug("edge publication transaction completed", attrs...)
}

func (s *Sink) sampleRoutingTable() int {
	if s.routingTableSize == nil {
		return -1
	}
	peers := s.routingTableSize()
	s.metrics.EdgeDHTRoutingTablePeers(peers, time.Now())
	return peers
}

func (s *Sink) observeDHTQuery(
	ctx context.Context,
	stage string,
	started time.Time,
	run func(context.Context) error,
	trace *dhtQueryTrace,
) error {
	// Every edge publication DHT stage passes through this choke point and
	// intentionally inherits the bounded per-peer dial policy. A future stage
	// with a different liveness contract must opt out explicitly rather than
	// silently recovering the library's much larger default.
	ctx = network.WithDialPeerTimeout(ctx, min(network.GetDialPeerTimeout(ctx), publicationDialPeerTimeout))
	queryCtx, cancel := context.WithCancel(ctx)
	queryCtx, queryEvents := routing.RegisterForQueryEvents(queryCtx)
	queryCtx, lookupEvents := dht.RegisterForLookupEvents(queryCtx)
	queryCollected := make(chan dhtQueryTrace, 1)
	go func() {
		var result dhtQueryTrace
		for event := range queryEvents {
			if event == nil {
				continue
			}
			observedAt := time.Now()
			elapsed := time.Since(started)
			recognized := true
			switch event.Type {
			case routing.SendingQuery:
				result.sendingQueries++
				s.metrics.EdgeDHTQueryEvent(stage, metrics.EdgeDHTQueryEventSendingQuery, observedAt)
			case routing.PeerResponse:
				result.peerResponses++
				result.closerPeers += len(event.Responses)
				result.maxCloserPeers = max(result.maxCloserPeers, len(event.Responses))
				s.metrics.EdgeDHTQueryEvent(stage, metrics.EdgeDHTQueryEventPeerResponse, observedAt)
			case routing.QueryError:
				result.queryErrors++
				s.metrics.EdgeDHTQueryEvent(stage, metrics.EdgeDHTQueryEventQueryError, observedAt)
			case routing.DialingPeer:
				result.peerDials++
				s.metrics.EdgeDHTQueryEvent(stage, metrics.EdgeDHTQueryEventDialingPeer, observedAt)
			case routing.Value:
				if result.valueSends == 0 {
					result.firstValue = elapsed
				}
				result.valueSends++
				s.metrics.EdgeDHTQueryEvent(stage, metrics.EdgeDHTQueryEventValueRPC, observedAt)
			default:
				recognized = false
			}
			if recognized {
				result.lastEvent = elapsed
			}
		}
		queryCollected <- result
	}()
	lookupCollected := make(chan dhtLookupTrace, 1)
	go func() {
		var result dhtLookupTrace
		for event := range lookupEvents {
			if event == nil {
				continue
			}
			if event.Request != nil {
				result.requests++
				addDHTLookupTransitions(&result, event.Request)
			}
			if event.Response != nil {
				result.responses++
				addDHTLookupTransitions(&result, event.Response)
			}
			if event.Terminate == nil {
				continue
			}
			result.terminations++
			reason, ok := edgeDHTLookupTerminationReason(event.Terminate.Reason)
			if !ok {
				continue
			}
			observedAt := time.Now()
			result.terminationObserved = true
			result.terminationReason = reason
			result.terminationElapsed = observedAt.Sub(started)
			result.terminationObservedAt = observedAt
			result.waitingAtTermination = result.waitingTransitions -
				result.queriedTransitions - result.unreachableTransitions
			s.metrics.EdgeDHTLookupTermination(stage, reason, result.waitingAtTermination, observedAt)
		}
		lookupCollected <- result
	}()

	err := run(queryCtx)
	returnedAt := time.Now()
	cancel()
	*trace = <-queryCollected
	trace.lookup = <-lookupCollected
	if trace.lookup.terminationObserved && returnedAt.After(trace.lookup.terminationObservedAt) {
		trace.lookup.postTerminationWait = returnedAt.Sub(trace.lookup.terminationObservedAt)
	}
	return err
}

func addDHTLookupTransitions(result *dhtLookupTrace, update *dht.LookupUpdateEvent) {
	result.heardTransitions += len(update.Heard)
	result.waitingTransitions += len(update.Waiting)
	result.queriedTransitions += len(update.Queried)
	result.unreachableTransitions += len(update.Unreachable)
}

func edgeDHTLookupTerminationReason(reason dht.LookupTerminationReason) (string, bool) {
	switch reason {
	case dht.LookupStopped:
		return metrics.EdgeDHTLookupTerminationStopped, true
	case dht.LookupCancelled:
		return metrics.EdgeDHTLookupTerminationCancelled, true
	case dht.LookupStarvation:
		return metrics.EdgeDHTLookupTerminationStarvation, true
	case dht.LookupCompleted:
		return metrics.EdgeDHTLookupTerminationCompleted, true
	default:
		return "", false
	}
}

func (s *Sink) observeStage(
	ctx context.Context,
	stage string,
	started time.Time,
	err error,
) (time.Duration, string) {
	duration := time.Since(started)
	outcome := edgePublicationOutcome(ctx, stage, err)
	s.metrics.EdgePublicationStage(stage, outcome, duration)
	return duration, outcome
}

func edgePublicationOutcome(ctx context.Context, stage string, err error) string {
	switch {
	case err == nil:
		return metrics.OutcomeOK
	case errors.Is(err, context.DeadlineExceeded):
		return metrics.EdgePublicationOutcomeTimeout
	case errors.Is(err, context.Canceled):
		return metrics.EdgePublicationOutcomeCanceled
	}
	// Some DHT implementations return an opaque terminal error after honoring
	// cancellation. At the DHT and permit-wait boundaries, the transaction
	// context is authoritative about whether its configured budget expired.
	switch stage {
	case metrics.EdgePublicationStageAdmission,
		metrics.EdgePublicationStageProvideDocument,
		metrics.EdgePublicationStagePutRecord:
		switch {
		case errors.Is(ctx.Err(), context.DeadlineExceeded):
			return metrics.EdgePublicationOutcomeTimeout
		case errors.Is(ctx.Err(), context.Canceled):
			return metrics.EdgePublicationOutcomeCanceled
		}
	}
	return metrics.OutcomeError
}

func remainingBudget(ctx context.Context) time.Duration {
	deadline, ok := ctx.Deadline()
	if !ok {
		return 0
	}
	remaining := time.Until(deadline)
	if remaining < 0 {
		return 0
	}
	return remaining
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
