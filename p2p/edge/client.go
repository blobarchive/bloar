// Package edge is the bounded handoff between a private bloard publication
// authority and a public libp2p edge. The writer constructs and signs both the
// publication document and its IPNS record; the edge receives only public,
// already-signed bytes and the ability to stage/provide/route them.
package edge

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"github.com/ipfs/boxo/ipns"
	blocks "github.com/ipfs/go-block-format"

	"github.com/blobarchive/bloar/p2p"
)

const (
	// ProtocolPath is the only mutation the edge control listener accepts.
	ProtocolPath = "/bloar/edge/v1/publication"

	// DefaultMaxDocumentBytes is deliberately far above today's publication
	// documents while keeping a compromised writer from allocating an
	// unbounded request in the public process.
	DefaultMaxDocumentBytes = 1 << 20

	// DefaultTransactionTimeout bounds the edge-owned provider-before-IPNS
	// transaction. It preserves the former two-minute server ceiling as the
	// actual DHT work budget rather than letting a disconnected HTTP request
	// decide whether the transaction lives.
	DefaultTransactionTimeout = 2 * time.Minute

	// DefaultRequestTimeout bounds the writer's complete local handoff. The
	// extra 30 seconds covers the AF_UNIX dial, bounded request body, and
	// structured response around a full edge transaction.
	DefaultRequestTimeout = 150 * time.Second

	// DefaultControlWriteTimeout is the edge HTTP server's outer response
	// ceiling. Keeping it 30 seconds above the writer budget guarantees that
	// the edge transaction or writer budget wins first with a meaningful
	// result instead of net/http cutting the response off at the same instant.
	DefaultControlWriteTimeout = 3 * time.Minute

	maxErrorBytes            = 8 << 10
	protocolSchema           = 2
	stageHeader              = "Bloar-Publication-Stage"
	transactionTimeoutHeader = "Bloar-Edge-Transaction-Timeout"
)

type wirePublication struct {
	Schema   int    `json:"schema"`
	Name     string `json:"name"`
	Document []byte `json:"document"`
	Record   []byte `json:"record"`
}

// ClientConfig configures the writer-initiated AF_UNIX handoff.
type ClientConfig struct {
	Socket             string
	TransactionTimeout time.Duration
	RequestTimeout     time.Duration
	MaxDocumentBytes   int
}

// ClientPolicy implements p2p.PublicationPolicy without holding a public host,
// DHT, blockstore, or any edge credential.
type ClientPolicy struct {
	socket         string
	maxDocument    int
	transaction    time.Duration
	requestTimeout time.Duration
	http           *http.Client
}

var _ p2p.PublicationPolicy = (*ClientPolicy)(nil)

// NewClientPolicy constructs a bounded Unix-socket policy. The absolute path
// requirement keeps a service's working directory from silently selecting a
// different control socket after an operator change.
func NewClientPolicy(cfg ClientConfig) (*ClientPolicy, error) {
	if cfg.Socket == "" || !filepath.IsAbs(cfg.Socket) {
		return nil, fmt.Errorf("edge: control socket %q must be an absolute path", cfg.Socket)
	}
	if len(cfg.Socket) >= 104 {
		return nil, fmt.Errorf("edge: control socket path is %d bytes, must be shorter than 104", len(cfg.Socket))
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
	if cfg.RequestTimeout == 0 {
		cfg.RequestTimeout = DefaultRequestTimeout
	}
	if err := ValidateTimeoutBudget(cfg.TransactionTimeout, cfg.RequestTimeout, DefaultControlWriteTimeout); err != nil {
		return nil, err
	}

	dialer := &net.Dialer{Timeout: min(cfg.RequestTimeout, 10*time.Second)}
	transport := &http.Transport{
		DisableCompression: true,
		DisableKeepAlives:  true,
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return dialer.DialContext(ctx, "unix", cfg.Socket)
		},
	}
	return &ClientPolicy{
		socket:         cfg.Socket,
		maxDocument:    cfg.MaxDocumentBytes,
		transaction:    cfg.TransactionTimeout,
		requestTimeout: cfg.RequestTimeout,
		http: &http.Client{
			Transport: transport,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return errors.New("edge: control redirects are forbidden")
			},
		},
	}, nil
}

// Prepare retains an immutable copy of the document until the Publisher has
// constructed the signed record. No network operation occurs before Commit:
// the edge needs both exact byte strings to verify their binding atomically.
func (p *ClientPolicy) Prepare(_ context.Context, block blocks.Block) (p2p.PublicationCommit, error) {
	if block == nil {
		return nil, errors.New("edge: publication block must not be nil")
	}
	document := append([]byte(nil), block.RawData()...)
	if len(document) == 0 || len(document) > p.maxDocument {
		return nil, fmt.Errorf("edge: publication document is %d bytes, want 1..%d", len(document), p.maxDocument)
	}
	return func(ctx context.Context, name ipns.Name, record []byte) error {
		return p.commit(ctx, name, document, record)
	}, nil
}

func (p *ClientPolicy) commit(ctx context.Context, name ipns.Name, document, record []byte) error {
	if err := ctx.Err(); err != nil {
		return &p2p.PublicationStageError{
			Stage: p2p.PublicationStagePutRecord,
			Err:   fmt.Errorf("edge: publication canceled before handoff: %w", err),
		}
	}
	if len(record) == 0 || len(record) > ipns.MaxRecordSize {
		return &p2p.PublicationStageError{
			Stage: p2p.PublicationStagePutRecord,
			Err:   fmt.Errorf("edge: IPNS record is %d bytes, want 1..%d", len(record), ipns.MaxRecordSize),
		}
	}
	body, err := json.Marshal(wirePublication{
		Schema:   protocolSchema,
		Name:     name.String(),
		Document: document,
		Record:   append([]byte(nil), record...),
	})
	if err != nil {
		return &p2p.PublicationStageError{Stage: p2p.PublicationStagePutRecord, Err: fmt.Errorf("edge: encoding publication: %w", err)}
	}

	// Crossing the control socket is the transaction's commit point. A caller
	// cancellation after this point must not make the writer abandon an edge
	// transaction that can still complete and then count that completion as an
	// error. Retain values but replace cancellation/deadline with this client's
	// explicit outer budget. A real process or AF_UNIX transport failure remains
	// an explicit request error; ordinary caller cancellation cannot manufacture
	// one while the edge is still completing a healthy transaction.
	requestCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), p.requestTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(requestCtx, http.MethodPost, "http://bloar-edge"+ProtocolPath, bytes.NewReader(body))
	if err != nil {
		return &p2p.PublicationStageError{Stage: p2p.PublicationStagePutRecord, Err: fmt.Errorf("edge: building request: %w", err)}
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(transactionTimeoutHeader, p.transaction.String())
	resp, err := p.http.Do(req)
	if err != nil {
		return &p2p.PublicationStageError{Stage: p2p.PublicationStagePutRecord, Err: fmt.Errorf("edge: publishing over %s: %w", p.socket, err)}
	}
	defer resp.Body.Close()
	raw, readErr := io.ReadAll(io.LimitReader(resp.Body, maxErrorBytes+1))
	if readErr != nil {
		return &p2p.PublicationStageError{Stage: p2p.PublicationStagePutRecord, Err: fmt.Errorf("edge: reading response: %w", readErr)}
	}
	if len(raw) > maxErrorBytes {
		return &p2p.PublicationStageError{Stage: p2p.PublicationStagePutRecord, Err: errors.New("edge: response exceeded 8192 bytes")}
	}
	if resp.StatusCode == http.StatusNoContent {
		return nil
	}
	stage := p2p.PublicationStage(resp.Header.Get(stageHeader))
	if stage != p2p.PublicationStageProvideDocument && stage != p2p.PublicationStagePutRecord {
		stage = p2p.PublicationStagePutRecord
	}
	detail := strings.TrimSpace(string(raw))
	if detail == "" {
		detail = http.StatusText(resp.StatusCode)
	}
	return &p2p.PublicationStageError{
		Stage: stage,
		Err:   fmt.Errorf("edge: publication refused with HTTP %d: %s", resp.StatusCode, detail),
	}
}

// ValidateTimeoutBudget enforces the transaction ordering shared by the writer
// and edge. Equality is deliberately refused: simultaneous timers recreate the
// ambiguous "client timed out while the edge may have completed" result this
// protocol exists to avoid.
func ValidateTimeoutBudget(transaction, request, serverWrite time.Duration) error {
	switch {
	case transaction <= 0:
		return fmt.Errorf("edge: transaction timeout is %s, must be positive", transaction)
	case request <= 0:
		return fmt.Errorf("edge: request timeout is %s, must be positive", request)
	case serverWrite <= 0:
		return fmt.Errorf("edge: server write timeout is %s, must be positive", serverWrite)
	case transaction >= request:
		return fmt.Errorf("edge: transaction timeout %s must be shorter than request timeout %s", transaction, request)
	case request >= serverWrite:
		return fmt.Errorf("edge: request timeout %s must be shorter than server write timeout %s", request, serverWrite)
	default:
		return nil
	}
}
