// Package archclient is the client half of the bloar API of spec 7.2: the
// endpoints an indexer writes an archive through, plus the one it reads its own
// progress from.
//
// # Why this is a package and not two methods on each indexer
//
// Both indexers of spec 10 talk to the archive the same way, and the way is not
// trivial: a refs batch can come back 409 with a list of blobs the archive does
// not hold, which is a different failure from a 409 that means the batch
// overlapped coverage, which is different again from the network dropping. An
// indexer that flattened those into "error" would retry the unretryable and
// give up on the retryable. The distinctions live here, once.
//
// # Progress is not state
//
// Spec 10 opens with the whole of an indexer's persistence model: "Both
// indexers are stateless: progress = GET .../synced_to". SyncedTo is therefore
// not a convenience -- it is the resume point, read fresh at the top of every
// loop, and an indexer that cached it would be an indexer that forked from the
// archive after any restart of either.
//
// # Retries
//
// Transport failures and 5xx are retried with exponential backoff; every 4xx is
// not. That split is exactly the archive's own contract: spec 7.2's 400s are
// malformed requests and its 409s are conflicts, and no amount of repeating
// either changes the answer. A 5xx is the archive having a bad day, which is
// what backoff is for.
package archclient

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/blobarchive/bloar/schema"
	"github.com/blobarchive/bloar/server"
)

// Config is what a Client needs to reach an archive.
type Config struct {
	// BaseURL is the archive's root, e.g. https://archive.example.org. Required.
	BaseURL string
	// Token is the bearer token of spec 7.3. Required: every endpoint this
	// client posts to is authenticated.
	Token string
	// HTTPClient issues the requests. Optional; a client with a sane timeout is
	// used when nil.
	HTTPClient *http.Client
	// MaxAttempts bounds the tries of a retryable failure. Zero takes the
	// default of 5.
	MaxAttempts int
	// Backoff is the delay before the second attempt; it doubles each time.
	// Zero takes the default of 250ms. Tests set it to something they can
	// afford to wait for.
	Backoff time.Duration
	// Logger receives retry notices. Optional.
	Logger *slog.Logger
	// ObserveAvailability receives the result of each completed logical archive
	// request after its bounded retry budget. A decoded success response or an
	// authoritative 4xx reports true; exhausted transport failures, malformed
	// success bodies, and 5xx responses report false. Caller cancellation does
	// not report either state. The callback may be invoked concurrently because
	// Client is safe for concurrent use, so it must be concurrency-safe and
	// return promptly.
	ObserveAvailability func(bool)
}

// Defaults.
const (
	defaultMaxAttempts = 5
	defaultBackoff     = 250 * time.Millisecond
	// defaultTimeout covers a whole request. A put of 64 blobs is 8 MiB, and
	// the archive verifies every one of them with a KZG commitment before it
	// answers, so this is generous by design.
	defaultTimeout = 2 * time.Minute
)

// Client speaks the bloar API of spec 7.2. It is safe for concurrent use.
type Client struct {
	base        string
	token       string
	hc          *http.Client
	maxAttempts int
	backoff     time.Duration
	log         *slog.Logger
	observe     func(bool)
}

// New returns a Client over cfg.
func New(cfg Config) (*Client, error) {
	if cfg.BaseURL == "" {
		return nil, errors.New("archclient: Config.BaseURL is required")
	}
	u, err := url.Parse(cfg.BaseURL)
	if err != nil {
		return nil, fmt.Errorf("archclient: Config.BaseURL %q: %w", cfg.BaseURL, err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, fmt.Errorf("archclient: Config.BaseURL %q must be an http or https URL", cfg.BaseURL)
	}
	if cfg.Token == "" {
		// Not a warning: every write this client makes would 401, and finding
		// that out one HTTP round trip into a sync run is worse than finding it
		// out here.
		return nil, errors.New("archclient: Config.Token is required; every endpoint this client posts to is authenticated (spec 7.3)")
	}

	c := &Client{
		base:        strings.TrimSuffix(cfg.BaseURL, "/"),
		token:       cfg.Token,
		hc:          cfg.HTTPClient,
		maxAttempts: cfg.MaxAttempts,
		backoff:     cfg.Backoff,
		log:         cfg.Logger,
		observe:     cfg.ObserveAvailability,
	}
	if c.hc == nil {
		c.hc = &http.Client{Timeout: defaultTimeout}
	}
	if c.maxAttempts == 0 {
		c.maxAttempts = defaultMaxAttempts
	}
	if c.backoff == 0 {
		c.backoff = defaultBackoff
	}
	// Strictly positive after defaulting: the retry backoff is slept
	// out before a second attempt and doubles each time, so a non-positive value
	// retries with no delay. Zero is the documented default just applied; no config
	// key feeds this, but main.go's Config is exactly the caller the constructor
	// boundary guards, so a value at or below zero is rejected here.
	if c.backoff <= 0 {
		return nil, fmt.Errorf("archclient: Config.Backoff is %s, must be positive", c.backoff)
	}
	if c.log == nil {
		c.log = slog.New(slog.DiscardHandler)
	}
	return c, nil
}

// HeadInfo is one head's entry from GET /bloar/v1/heads/{head} (spec 7.2, 8).
// Only what an indexer acts on is decoded: where the head starts, and how far
// it has got.
type HeadInfo struct {
	Name       string  `json:"name"`
	Root       string  `json:"root"`
	OriginSlot uint64  `json:"origin_slot"`
	SyncedTo   *uint64 `json:"synced_to"`
	// Kind is omitted by legacy/finalized publications and explicit on mutable
	// heads. Callers treat omission as finalized-monotonic.
	Kind server.HeadKind `json:"kind,omitempty"`
	// WindowStart is present on mutable publications. OriginSlot on the archive
	// engine equals it, but retaining the signed field makes audits explicit.
	WindowStart *uint64 `json:"window_start,omitempty"`
}

// Head reads GET /bloar/v1/heads/{head}.
//
// An indexer calls this once at startup, for origin_slot: a head that has never
// been written has a null synced_to and no other statement of where it begins,
// and starting a walk at slot 0 instead would be several million pointless
// 404s. Coverage itself comes from SyncedTo, not from here, because this
// document is served with a cache lifetime (spec 7.2) and progress must not be.
func (c *Client) Head(ctx context.Context, head string) (HeadInfo, error) {
	var out HeadInfo
	if err := c.do(ctx, http.MethodGet, "/bloar/v1/heads/"+url.PathEscape(head), nil, "", &out); err != nil {
		return HeadInfo{}, err
	}
	return out, nil
}

// Limits is what the archive advertises about its own operational bounds (spec
// 7.2). Only what an indexer checks itself against is decoded.
type Limits struct {
	// MaxPutBlobs is the most blobs the archive accepts in one POST
	// /bloar/v1/blobs. Zero means the archive did not advertise it -- an archive
	// that predates the field -- and the caller cannot cross-check against it.
	MaxPutBlobs int
}

// Limits reads the archive's advertised max_put_blobs from the publication
// document (GET /bloar/v1/heads, spec 8). An indexer uses it to cross-check its
// durable local archive.max_put_blobs expectation whenever the finalized loop
// is constructed. A differing live value is configuration drift and fails
// closed; temporary unavailability leaves the local bound in force. The whole
// document is fetched because max_put_blobs is an archive-wide field on it, not
// a per-head one; it is a one-time read off a document served with a short cache
// lifetime, so its size does not matter.
func (c *Client) Limits(ctx context.Context) (Limits, error) {
	var out struct {
		MaxPutBlobs int `json:"max_put_blobs"`
	}
	if err := c.do(ctx, http.MethodGet, "/bloar/v1/heads", nil, "", &out); err != nil {
		return Limits{}, err
	}
	return Limits{MaxPutBlobs: out.MaxPutBlobs}, nil
}

// ManifestInfo is a head's published manifest tip (spec 10.5): the decoded
// source schedule the head attests to, and the tip's CID for logging. It is what
// the chain indexer checks its configured schedule against at startup.
type ManifestInfo struct {
	CID     string
	Sources []schema.Source
}

// Manifest reads GET /bloar/v1/heads/{head}/manifest (spec 7.2, 10.5). A head
// with no chain is (nil, nil): the endpoint 404s, and the caller has already
// confirmed the head exists (Head), so the only 404 left is "no chain". Every
// other failure propagates.
//
// The sources are returned in the schema's byte-string form; index/chain converts
// them to its go-ethereum-typed Sources (SourcesFromSchema). The indexer decodes
// the manifest schedule because spec 15's reject-unknown-major rule binds it: an
// indexer validating an upgrade is exactly one of the things that must understand
// the version it reads.
func (c *Client) Manifest(ctx context.Context, head string) (*ManifestInfo, error) {
	var out manifestJSON
	err := c.do(ctx, http.MethodGet, "/bloar/v1/heads/"+url.PathEscape(head)+"/manifest", nil, "", &out)
	if err != nil {
		var httpErr *HTTPError
		if errors.As(err, &httpErr) && httpErr.Status == http.StatusNotFound {
			return nil, nil
		}
		return nil, err
	}
	sources, err := out.toSchema()
	if err != nil {
		return nil, fmt.Errorf("archclient: decoding manifest of head %q: %w", head, err)
	}
	return &ManifestInfo{CID: out.CID, Sources: sources}, nil
}

// manifestJSON mirrors the GET /manifest response shape (server's
// manifestGetResponse). Only what the indexer acts on is decoded.
type manifestJSON struct {
	Manifest struct {
		Sources []struct {
			Type       string   `json:"type"`
			Address    string   `json:"address"`
			Topic      string   `json:"topic"`
			Senders    []string `json:"senders"`
			FromBlock  uint64   `json:"from_block"`
			UntilBlock *uint64  `json:"until_block"`
		} `json:"sources"`
	} `json:"manifest"`
	CID string `json:"cid"`
}

// toSchema parses the JSON sources into schema.Source. Byte widths and the
// cross-type rules are the schema's to enforce -- SourcesFromSchema and the
// manifest validator catch a malformed one -- so this only parses hex and the
// open-ended flag.
func (m manifestJSON) toSchema() ([]schema.Source, error) {
	out := make([]schema.Source, 0, len(m.Manifest.Sources))
	for i, s := range m.Manifest.Sources {
		src := schema.Source{Type: s.Type, FromBlock: s.FromBlock}
		var err error
		if src.Address, err = decodeHex(s.Address); err != nil {
			return nil, fmt.Errorf("source %d address: %w", i, err)
		}
		if s.Topic != "" {
			if src.Topic, err = decodeHex(s.Topic); err != nil {
				return nil, fmt.Errorf("source %d topic: %w", i, err)
			}
		}
		for j, sender := range s.Senders {
			b, err := decodeHex(sender)
			if err != nil {
				return nil, fmt.Errorf("source %d sender %d: %w", i, j, err)
			}
			src.Senders = append(src.Senders, b)
		}
		if s.UntilBlock != nil {
			src.UntilBlock = *s.UntilBlock
		} else {
			src.OpenEnded = true
		}
		out = append(out, src)
	}
	return out, nil
}

// decodeHex parses a 0x-prefixed (optional) hex byte string.
func decodeHex(s string) ([]byte, error) {
	b, err := hex.DecodeString(strings.TrimPrefix(s, "0x"))
	if err != nil {
		return nil, fmt.Errorf("%q is not hex: %w", s, err)
	}
	return b, nil
}

// SyncedTo reads GET /bloar/v1/heads/{head}/synced_to (spec 7.2). A nil result
// is an empty head: the caller starts at origin_slot.
func (c *Client) SyncedTo(ctx context.Context, head string) (*uint64, error) {
	var out struct {
		SyncedTo *uint64 `json:"synced_to"`
	}
	if err := c.do(ctx, http.MethodGet, "/bloar/v1/heads/"+url.PathEscape(head)+"/synced_to", nil, "", &out); err != nil {
		return nil, err
	}
	return out.SyncedTo, nil
}

// PutBlob is one ingested blob's derived identity, as POST /bloar/v1/blobs
// states it.
type PutBlob struct {
	VH  schema.VersionedHash
	CID string
}

// PutBlobs posts the concatenation of blobs to POST /bloar/v1/blobs (spec 7.2)
// and returns their derived identities in body order.
//
// The caller passes bytes and gets versioned hashes back; it does not compute
// them. That is spec 7.2's rule ("No metadata is accepted: the server derives
// everything") and it is also the only reason the returned list is worth
// checking: the archive re-derived every one of these from the bytes it
// actually stored, so comparing them against what the indexer meant to store is
// an end-to-end check on the wire, the framing, and the body order all at once.
// Both indexers do exactly that.
func (c *Client) PutBlobs(ctx context.Context, blobs [][]byte) ([]PutBlob, error) {
	if len(blobs) == 0 {
		return nil, nil
	}
	body := make([]byte, 0, len(blobs)*schema.BlobSize)
	for i, b := range blobs {
		if len(b) != schema.BlobSize {
			return nil, fmt.Errorf("archclient: blob %d is %d bytes, must be exactly %d", i, len(b), schema.BlobSize)
		}
		body = append(body, b...)
	}

	var out struct {
		Blobs []struct {
			VersionedHash string `json:"versioned_hash"`
			CID           string `json:"cid"`
		} `json:"blobs"`
	}
	if err := c.do(ctx, http.MethodPost, "/bloar/v1/blobs", body, "application/octet-stream", &out); err != nil {
		return nil, err
	}
	if len(out.Blobs) != len(blobs) {
		return nil, fmt.Errorf("archclient: put %d blobs, archive answered for %d", len(blobs), len(out.Blobs))
	}

	res := make([]PutBlob, 0, len(out.Blobs))
	for i, b := range out.Blobs {
		vh, err := ParseVH(b.VersionedHash)
		if err != nil {
			return nil, fmt.Errorf("archclient: put blobs answer %d: %w", i, err)
		}
		res = append(res, PutBlob{VH: vh, CID: b.CID})
	}
	return res, nil
}

// Row is one slot's refs, as POST /bloar/v1/heads/{head}/refs takes them.
type Row struct {
	Slot uint64
	VHs  []schema.VersionedHash
}

// RefsResult is a 200 from the refs endpoint.
type RefsResult struct {
	SyncedTo uint64 `json:"synced_to"`
	Root     string `json:"root"`
	NoOp     bool   `json:"noop"`
}

// PostRefs posts a refs batch to POST /bloar/v1/heads/{head}/refs (spec 7.2,
// 5.1).
//
// expectedManifest binds the batch to the manifest tip it was scanned under
// : the empty string for a chainless head (the ALL
// head, and every head predating the manifest chain), the tip CID otherwise. The
// archive commits the batch only if it still holds that tip, so a batch scanned
// under a superseded schedule is refused rather than written across a handoff.
//
// A 409 naming blobs the archive does not hold comes back as a
// *MissingBlobsError; a 409 reporting a stale expected_manifest as a
// *ManifestBindingError; every other 409 as a *ConflictError. The difference
// matters to a caller: missing blobs mean the put half of this indexer's batch
// did not land and the batch can be re-put; a manifest-binding conflict means the
// tip moved and this indexer must stop and resync; a plain conflict means the
// batch disagreed with coverage the archive already has, which is a bug, not a
// retry.
func (c *Client) PostRefs(ctx context.Context, head string, rows []Row, syncedTo uint64, expectedManifest string) (RefsResult, error) {
	type jsonRow struct {
		Slot            uint64   `json:"slot"`
		VersionedHashes []string `json:"versioned_hashes"`
	}
	req := struct {
		Rows             []jsonRow `json:"rows"`
		SyncedTo         uint64    `json:"synced_to"`
		ExpectedManifest string    `json:"expected_manifest,omitempty"`
	}{Rows: make([]jsonRow, 0, len(rows)), SyncedTo: syncedTo, ExpectedManifest: expectedManifest}

	for _, r := range rows {
		vhs := make([]string, 0, len(r.VHs))
		for _, vh := range r.VHs {
			vhs = append(vhs, VHHex(vh))
		}
		req.Rows = append(req.Rows, jsonRow{Slot: r.Slot, VersionedHashes: vhs})
	}

	body, err := json.Marshal(req)
	if err != nil {
		return RefsResult{}, fmt.Errorf("archclient: encoding refs batch: %w", err)
	}

	var out RefsResult
	if err := c.do(ctx, http.MethodPost, "/bloar/v1/heads/"+url.PathEscape(head)+"/refs", body, "application/json", &out); err != nil {
		return RefsResult{}, err
	}
	return out, nil
}

// GenerationState reads the selected mutable generation, its CAS counter, and
// whether the server completed the post-commit exposure/publication steps.
// Generation zero is a configured-but-not-yet-published mutable head.
func (c *Client) GenerationState(ctx context.Context, head string) (server.GenerationStatus, error) {
	var out server.GenerationStatus
	if err := c.do(ctx, http.MethodGet, "/bloar/v1/heads/"+url.PathEscape(head)+"/generation", nil, "", &out); err != nil {
		return server.GenerationStatus{}, err
	}
	return out, nil
}

// PostGeneration atomically replaces a bounded unfinalized head. A 409 exposes
// CurrentGeneration on ConflictError; missing blobs are the existing
// MissingBlobsError subtype and may be put before retrying the exact request.
func (c *Client) PostGeneration(ctx context.Context, head string, req server.GenerationRequest) (server.GenerationResponse, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return server.GenerationResponse{}, fmt.Errorf("archclient: encoding generation: %w", err)
	}
	var out server.GenerationResponse
	if err := c.do(ctx, http.MethodPost, "/bloar/v1/heads/"+url.PathEscape(head)+"/generation", body, "application/json", &out); err != nil {
		return server.GenerationResponse{}, err
	}
	return out, nil
}

// PostManifest advances a head's manifest chain through POST
// /bloar/v1/heads/{head}/manifest (spec 7.2, 10.5) and returns the new tip CID.
//
// It is the publish half of the append-only workflow: the caller has already run
// the chain-aware preflight (the server has no L1 view and cannot, spec 10.5), so
// this only encodes the request and posts it. expectedHeadRoot is the head root
// that preflight validated against -- the generation binding of the safety boundary --
// and prev is the manifest's own prev link (m.Prev), the CID compare-and-swap.
//
// A 409 (a stale prev, or an expected_head_root the head has advanced past) comes
// back as a *ConflictError: the caller re-reads the head and re-runs the
// preflight against it. Every other failure is the shared HTTPError taxonomy.
func (c *Client) PostManifest(ctx context.Context, head string, m *schema.Manifest, expectedHeadRoot string) (string, error) {
	req := struct {
		Manifest         manifestReqJSON `json:"manifest"`
		Confirm          string          `json:"confirm"`
		ExpectedHeadRoot string          `json:"expected_head_root"`
	}{Manifest: manifestToReqJSON(m), Confirm: head, ExpectedHeadRoot: expectedHeadRoot}

	body, err := json.Marshal(req)
	if err != nil {
		return "", fmt.Errorf("archclient: encoding manifest: %w", err)
	}

	var out struct {
		Manifest string `json:"manifest"`
	}
	if err := c.do(ctx, http.MethodPost, "/bloar/v1/heads/"+url.PathEscape(head)+"/manifest", body, "application/json", &out); err != nil {
		return "", err
	}
	return out.Manifest, nil
}

// manifestReqJSON is the manifest POST body shape (server's manifestJSON): the
// mirror of manifestJSON above, in the encode direction. Prev is a pointer so the
// genesis case is an explicit null, matching the spec's "bafy.. | null".
type manifestReqJSON struct {
	V       uint64            `json:"v"`
	Head    string            `json:"head"`
	Sources []manifestSrcJSON `json:"sources"`
	Prev    *string           `json:"prev"`
}

type manifestSrcJSON struct {
	Type       string   `json:"type"`
	Address    string   `json:"address"`
	Topic      string   `json:"topic,omitempty"`
	Senders    []string `json:"senders,omitempty"`
	FromBlock  uint64   `json:"from_block"`
	UntilBlock *uint64  `json:"until_block,omitempty"`
}

// manifestToReqJSON renders a schema.Manifest as the POST body. It omits exactly
// what the canonical encoding treats as absent (spec 10.5): a type-specific key
// that does not apply, and an open-ended source's until_block.
func manifestToReqJSON(m *schema.Manifest) manifestReqJSON {
	out := manifestReqJSON{V: m.V, Head: m.Head}
	for _, s := range m.Sources {
		sj := manifestSrcJSON{Type: s.Type, Address: hexBytes(s.Address), FromBlock: s.FromBlock}
		if len(s.Topic) > 0 {
			sj.Topic = hexBytes(s.Topic)
		}
		for _, sender := range s.Senders {
			sj.Senders = append(sj.Senders, hexBytes(sender))
		}
		if !s.OpenEnded {
			ub := s.UntilBlock
			sj.UntilBlock = &ub
		}
		out.Sources = append(out.Sources, sj)
	}
	if m.Prev.Defined() {
		p := m.Prev.String()
		out.Prev = &p
	}
	return out
}

// hexBytes renders a byte string as the API states an address, topic, or sender.
func hexBytes(b []byte) string { return "0x" + hex.EncodeToString(b) }

// HTTPError is any answer the archive gave that was not a 200. Status is the
// status line; Code and Message are the beacon-shape error body of spec 7, when
// there was one.
type HTTPError struct {
	Method  string
	Path    string
	Status  int
	Code    int
	Message string
}

func (e *HTTPError) Error() string {
	if e.Message != "" {
		return fmt.Sprintf("archclient: %s %s: %d: %s", e.Method, e.Path, e.Status, e.Message)
	}
	return fmt.Sprintf("archclient: %s %s: %d", e.Method, e.Path, e.Status)
}

// ConflictError is a 409 from the refs endpoint: the batch was refused against
// the head's current coverage (spec 5.1).
type ConflictError struct {
	*HTTPError
	// CurrentGeneration is present on mutable-generation conflicts, including
	// generation zero. It is nil on legacy refs/manifest conflicts.
	CurrentGeneration *uint64
}

func (e *ConflictError) Unwrap() error { return e.HTTPError }

// MissingBlobsError is a 409 from the refs endpoint carrying spec 7.2's
// missing_blobs: the batch named blobs the archive does not hold.
//
// It embeds ConflictError, so errors.As finds it as either. That nesting is the
// spec's: every missing-blob failure is a conflict, and only some conflicts are
// missing blobs.
type MissingBlobsError struct {
	*ConflictError
	VHs []schema.VersionedHash
}

func (e *MissingBlobsError) Error() string {
	return fmt.Sprintf("%s (%d blobs missing)", e.ConflictError.Error(), len(e.VHs))
}

func (e *MissingBlobsError) Unwrap() error { return e.ConflictError }

// ManifestBindingError is a 409 from the refs endpoint carrying spec 10.5's
// manifest_tip: the batch's expected_manifest is no longer the head's tip (audit
// the safety boundary). CurrentTip is the tip the archive holds now. It embeds ConflictError,
// so errors.As finds it as either, and a caller that only wants "did the manifest
// move" tests for this one; the head's writer stops and resyncs against CurrentTip.
type ManifestBindingError struct {
	*ConflictError
	CurrentTip string
}

func (e *ManifestBindingError) Error() string {
	return fmt.Sprintf("%s (manifest tip is now %s)", e.ConflictError.Error(), e.CurrentTip)
}

func (e *ManifestBindingError) Unwrap() error { return e.ConflictError }

// errorBody is spec 7's error shape, plus 7.2's and 10.5's additions on a refs
// 409: missing_blobs and the current manifest_tip.
type errorBody struct {
	Code              int      `json:"code"`
	Message           string   `json:"message"`
	MissingBlobs      []string `json:"missing_blobs"`
	ManifestTip       string   `json:"manifest_tip"`
	CurrentGeneration *uint64  `json:"current_generation"`
}

// do issues one request, retrying what is worth retrying, and decodes a 200
// into out.
func (c *Client) do(ctx context.Context, method, path string, body []byte, contentType string, out any) (err error) {
	defer func() {
		c.observeResult(ctx, err)
	}()

	backoff := c.backoff
	var lastErr error

	for attempt := 1; attempt <= c.maxAttempts; attempt++ {
		if attempt > 1 {
			// The wait is the point of the retry, so a cancelled context must
			// come out as the context's error and not as the last HTTP failure:
			// a caller shutting down wants to see that it shut down.
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}
			backoff *= 2
		}

		err := c.attempt(ctx, method, path, body, contentType, out)
		if err == nil {
			return nil
		}
		// A cancelled context is never a transient failure, whatever the
		// transport made of it.
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if !retryable(err) {
			return err
		}
		lastErr = err
		c.log.Warn("archive request failed, retrying",
			"method", method, "path", path, "attempt", attempt, "of", c.maxAttempts, "err", err)
	}
	return fmt.Errorf("archclient: %s %s failed after %d attempts: %w", method, path, c.maxAttempts, lastErr)
}

// observeResult classifies one completed logical request, not its individual
// retry attempts. That keeps a transient failed attempt from declaring the
// archive unavailable when a later attempt in the same bounded request
// succeeds. An authoritative 4xx proves reachability even though its caller
// will still fail closed on the protocol or configuration error.
func (c *Client) observeResult(ctx context.Context, err error) {
	if c.observe == nil {
		return
	}
	if err == nil {
		c.observe(true)
		return
	}
	if ctx.Err() != nil && errors.Is(err, ctx.Err()) {
		return
	}
	if IsUnavailable(err) {
		c.observe(false)
		return
	}
	var httpErr *HTTPError
	if errors.As(err, &httpErr) {
		c.observe(true)
	}
}

// attempt issues exactly one request.
func (c *Client) attempt(ctx context.Context, method, path string, body []byte, contentType string, out any) error {
	// A fresh reader per attempt: a retry re-sends the body, and a reader
	// already drained by the previous attempt would send nothing.
	var rdr io.Reader
	if body != nil {
		rdr = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, rdr)
	if err != nil {
		return fmt.Errorf("archclient: building %s %s: %w", method, path, err)
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	req.Header.Set("Authorization", "Bearer "+c.token)

	resp, err := c.hc.Do(req)
	if err != nil {
		return &transportError{err: fmt.Errorf("archclient: %s %s: %w", method, path, err)}
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return c.statusError(method, path, resp)
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		// A 200 with a body that will not decode is the archive misbehaving,
		// and the next attempt may well get a whole answer: a truncated body is
		// exactly what a connection dropped mid-response looks like.
		return &transportError{err: fmt.Errorf("archclient: %s %s: decoding answer: %w", method, path, err)}
	}
	return nil
}

// statusError renders a non-200 as the most specific error the body supports.
func (c *Client) statusError(method, path string, resp *http.Response) error {
	// Bounded: this is an error path, and an archive answering an error with a
	// gigabyte should not be able to make the indexer buy it.
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))

	httpErr := &HTTPError{Method: method, Path: path, Status: resp.StatusCode}
	var parsed errorBody
	if err := json.Unmarshal(raw, &parsed); err == nil {
		httpErr.Code, httpErr.Message = parsed.Code, parsed.Message
	} else if len(raw) > 0 {
		// Not the shape spec 7 promises, but something was said, and an
		// operator reading a log line wants it. A proxy in front of the archive
		// is the usual author of these.
		httpErr.Message = strings.TrimSpace(string(raw))
	}

	if resp.StatusCode != http.StatusConflict {
		return httpErr
	}
	conflict := &ConflictError{HTTPError: httpErr, CurrentGeneration: parsed.CurrentGeneration}
	// A stale expected_manifest: the head's current tip
	// is in the body, and the caller stops and resyncs against it. Checked before
	// missing_blobs because the two never co-occur -- a refs 409 is one or the
	// other -- and this one is the reason to stop rather than re-put.
	if parsed.ManifestTip != "" {
		return &ManifestBindingError{ConflictError: conflict, CurrentTip: parsed.ManifestTip}
	}
	if len(parsed.MissingBlobs) == 0 {
		return conflict
	}

	vhs := make([]schema.VersionedHash, 0, len(parsed.MissingBlobs))
	for _, s := range parsed.MissingBlobs {
		vh, err := ParseVH(s)
		if err != nil {
			// The list is unreadable, but the conflict is real and its message
			// is intact: degrade to the conflict rather than lose it.
			c.log.Warn("archive named a missing blob that will not parse", "value", s, "err", err)
			return conflict
		}
		vhs = append(vhs, vh)
	}
	return &MissingBlobsError{ConflictError: conflict, VHs: vhs}
}

// transportError marks a request that produced no authoritative application
// response: either the transport returned no HTTP answer, or an HTTP 200 body
// was malformed/truncated and could not be decoded.
type transportError struct{ err error }

func (e *transportError) Error() string { return e.err.Error() }
func (e *transportError) Unwrap() error { return e.err }

// IsUnavailable reports whether err means the archive did not provide an
// authoritative application response: transport failure, malformed/truncated
// success body, or an HTTP 5xx. It remains true through the bounded-retry
// wrapper returned by do.
//
// Finalized indexers use this after the client's per-request attempt budget is
// exhausted to keep the process alive and retry the stateless indexing loop.
// Every 4xx remains false: authentication, malformed requests, unknown heads,
// and conflicts are authoritative answers that must fail closed rather than be
// hidden behind an availability loop.
func IsUnavailable(err error) bool {
	return retryable(err)
}

// retryable reports whether repeating a request could plausibly change the
// answer.
func retryable(err error) bool {
	var transport *transportError
	if errors.As(err, &transport) {
		return true
	}
	var httpErr *HTTPError
	if errors.As(err, &httpErr) {
		// 5xx only. Spec 7.2's 4xx answers -- 400 malformed, 401 unauthorized,
		// 404 unknown head, 409 conflict -- are all statements about the
		// request, and the request does not change between attempts.
		return httpErr.Status >= 500
	}
	return false
}

// ParseVH parses a versioned hash as the API states one. The 0x prefix is
// optional on the way in.
func ParseVH(s string) (schema.VersionedHash, error) {
	h := strings.TrimPrefix(s, "0x")
	if len(h) != 2*schema.VersionedHashSize {
		return schema.VersionedHash{}, fmt.Errorf("versioned hash %q is not %d hex-encoded bytes", s, schema.VersionedHashSize)
	}
	b, err := hex.DecodeString(h)
	if err != nil {
		return schema.VersionedHash{}, fmt.Errorf("versioned hash %q is not hex: %w", s, err)
	}
	return schema.VersionedHash(b), nil
}

// VHHex renders a versioned hash as the API states one.
func VHHex(vh schema.VersionedHash) string { return "0x" + hex.EncodeToString(vh[:]) }
