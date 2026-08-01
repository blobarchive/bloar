// Package upstream is the client half of the beacon-compatible read API of
// spec 7.1: the endpoints the indexers of spec 10 read blobs, blocks, and
// finality out of. Trust does not live here -- it lives in the indexer. This
// package speaks the two wire protocols and classifies their answers; whether an
// answer is a fact about a slot is the caller's decision, made from the shape of
// its whole upstream set.
//
// # One client per upstream
//
// A Client is exactly one endpoint: a beacon node, or a bloar archive. Config.Head
// selects the shape -- unset, this is a beacon node and blobs live at
// /eth/v1/beacon/blobs/{slot}; set, this is a bloar archive (spec 11.5's
// re-derivation) and they live at /{head}/eth/v1/beacon/blobs/{slot}, with the
// head's synced_to standing in for finality:
//
//	                 beacon node                      bloar archive
//	blobs path       /eth/v1/beacon/blobs/{slot}      /{head}/eth/v1/beacon/blobs/{slot}
//	finality         /eth/v1/beacon/headers/finalized /bloar/v1/heads/{head}/synced_to
//	origin           n/a                              /bloar/v1/heads/{head} (origin_slot)
//	empty slot       404                              200 {"data": []}
//	missed slot      404                              200 {"data": []}
//	pruned slot      404  (!)                         n/a
//	not yet there    404 or an old finalized header   503 + Retry-After
//
// There is no fallback and no unanimity rule in this package any more. Both were
// an attempt to make a blob endpoint's absence trustworthy by corroboration; the
// beacon indexer now takes its truth about what a slot contains from a separate
// trusted block feed (BlockClient) and treats blob endpoints as ordered,
// untrusted byte sources -- so an absence from any one of them is never recorded
// as a fact and needs no corroboration. See index/beacon's anchored mode.
//
// # Historical note: a pruned beacon node's 404
//
// A real node serves blobs only within its retention window and 404s a pruned
// slot exactly as it 404s an empty one -- and prysm may instead answer a pruned
// slot 200 with an empty data array, so past retention neither shape is a fact.
// This used to be a live hazard for backfill; it no longer is. Anchored mode
// (index/beacon) makes a beacon node's blob answers advisory: the block feed
// decides existence, and a source that 404s a slot the block proves non-empty is
// simply skipped for the next source. A bloar archive upstream (Head set) has no
// such ambiguity: spec 7.1 makes a covered empty slot a 200 with an empty data
// array, a not-yet-covered slot a 503, and 404 only slots below origin_slot.
package upstream

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/blobarchive/bloar/metrics"
	"github.com/blobarchive/bloar/schema"
)

// Config is what a Client needs to reach an upstream.
type Config struct {
	// BaseURL is the upstream's root. Required.
	BaseURL string
	// Head names the head to read when the upstream is a bloar archive, e.g.
	// "all". Empty means the upstream is a beacon node, whose blobs path carries
	// no head prefix.
	//
	// For the beacon indexer's anchored mode this only shapes the request path: a
	// source's answers are untrusted bytes either way, and a bloar archive named
	// here is just a full-history byte source (its head selects which of its heads
	// to read). For mirror mode (deterministic replication, spec 11.5) it is
	// load-bearing -- it selects the source archive whose coverage decisions the
	// indexer reproduces, and whose synced_to is its finality. Mirror mode inherits
	// that source's completeness rather than re-deriving it; KZG still anchors the
	// blobs it includes.
	Head string
	// HTTPClient issues the requests. Optional.
	HTTPClient *http.Client
	// MaxAttempts bounds the tries of a retryable failure. Zero takes 5.
	MaxAttempts int
	// Backoff is the delay before the second attempt; it doubles each time.
	// Zero takes 250ms.
	Backoff time.Duration
	// Logger receives retry notices. Optional.
	Logger *slog.Logger
	// Metrics times blob fetches and counts the bytes they return (spec 10.1).
	// Optional; nil records nothing.
	Metrics *metrics.Metrics
}

// Defaults.
const (
	defaultMaxAttempts = 5
	defaultBackoff     = 250 * time.Millisecond
	// defaultTimeout covers one request. A slot can carry the mid-2026 maximum
	// of 21 blobs, which is 2.6 MiB of hex on the wire.
	defaultTimeout = 60 * time.Second
)

// Success-body ceilings. Every 200 body is read through an
// io.LimitReader sized to what the endpoint could legitimately return, plus one
// sentinel byte; a body that overruns is a malformed answer, never a fact. The
// timeout above complements this but does not replace it: a custom transport can
// drop the time bound, and gzip expands after decompression, so the memory bound
// has to be its own thing.
//
// The limit wraps resp.Body, which is the reader the code actually consumes.
// Go's default transport requests gzip transparently and hands back the DECODED
// stream (resp.Uncompressed), so the ceiling bounds the expanded bytes -- the
// LimitReader sits above the gzip layer, and a chunked or gzip-inflated body is
// bounded at exactly the same number as a plain one.
const (
	// metaBodyCeiling bounds the small JSON metadata endpoints: the finalized and
	// per-slot headers, an archive's synced_to and head document, and the blinded
	// block whose blob_kzg_commitments anchored mode reads. The blinded block is
	// the largest of them -- a full beacon block minus its execution payload and
	// blobs -- and stays comfortably under 8 MiB even at maximum attestation load;
	// the rest are a few hundred bytes. One ceiling for all of them keeps the read
	// path simple, and 8 MiB is slack over the largest, not a target any of them
	// approaches.
	metaBodyCeiling int64 = 8 << 20
)

// blobsBodyCeiling is the ceiling for a 200 blobs body, sized to what THIS
// request could legitimately return. A filtered request returns
// exactly its requested count; an unfiltered one returns at most a slot's worth,
// the per-slot blob ceiling. The JSON variant is the larger wire form -- each
// blob is "0x"+hex, 2*BlobSize plus quotes and a comma, against octet-stream's
// flat BlobSize -- so sizing to JSON bounds both. The slack covers the
// {"data":[]} envelope and any whitespace a lenient server pads with.
func blobsBodyCeiling(requested int) int64 {
	entries := requested
	if entries == 0 {
		// Unfiltered (mirror mode): bounded by the most blobs any slot can hold.
		entries = schema.MaxBlobsPerSlotCeiling
	}
	return int64(entries)*(2*schema.BlobSize+8) + 1024
}

// Client reads blobs and finality from one upstream. It is safe for concurrent
// use.
type Client struct {
	base        string
	head        string
	hc          *http.Client
	maxAttempts int
	backoff     time.Duration
	log         *slog.Logger
	metrics     *metrics.Metrics
}

// New returns a Client over cfg.
func New(cfg Config) (*Client, error) {
	if cfg.BaseURL == "" {
		return nil, errors.New("upstream: Config.BaseURL is required")
	}
	u, err := url.Parse(cfg.BaseURL)
	if err != nil {
		return nil, fmt.Errorf("upstream: Config.BaseURL %q: %w", cfg.BaseURL, err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, fmt.Errorf("upstream: Config.BaseURL %q must be an http or https URL", cfg.BaseURL)
	}

	c := &Client{
		base:        strings.TrimSuffix(cfg.BaseURL, "/"),
		head:        cfg.Head,
		hc:          cfg.HTTPClient,
		maxAttempts: cfg.MaxAttempts,
		backoff:     cfg.Backoff,
		log:         cfg.Logger,
		metrics:     cfg.Metrics,
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
		return nil, fmt.Errorf("upstream: Config.Backoff is %s, must be positive", c.backoff)
	}
	if c.log == nil {
		c.log = slog.New(slog.DiscardHandler)
	}
	return c, nil
}

// IsArchive reports whether this upstream is another bloar archive rather than
// a beacon node. The beacon indexer logs it, because it is the difference
// between a backfill that is sound and one that is not (spec 10.1).
func (c *Client) IsArchive() bool { return c.head != "" }

// Status is what an upstream said about a slot.
type Status int

const (
	// StatusFound is a 200: Blobs carries what the slot has, which may be
	// nothing (an archive upstream states an empty covered slot as {"data": []}).
	StatusFound Status = iota
	// StatusAbsent is a 404. On an archive upstream this means the slot is
	// below origin_slot, or a filtered request named a blob the slot does not
	// carry -- either way a fact. On a beacon node it means the node has no
	// blobs to give: an empty slot, a missed slot, or a PRUNED one, and the
	// response does not say which.
	StatusAbsent
	// StatusNotYetCovered is a 503: an archive upstream saying the slot is past
	// its synced_to. The caller must stop and wait; it must not record
	// anything, because "not yet" is not "nothing".
	StatusNotYetCovered
)

func (s Status) String() string {
	switch s {
	case StatusFound:
		return "found"
	case StatusAbsent:
		return "absent"
	case StatusNotYetCovered:
		return "not-yet-covered"
	default:
		return "status(" + strconv.Itoa(int(s)) + ")"
	}
}

// Result is one slot's answer.
type Result struct {
	Status Status
	// Blobs is the slot's blob bytes in the order the upstream stated them,
	// which spec 7.1 fixes: block order unfiltered, request order filtered.
	// Only meaningful for StatusFound.
	Blobs [][]byte
}

// FinalizedSlot is the upper bound of what a mirror-mode caller may read: F in
// spec 10.1's loop, in the archive's vocabulary.
//
// For a beacon node this is the finalized checkpoint (spec 10.3: indexers MUST
// only process finalized data). For a bloar archive it is the head's synced_to;
// an archive with no coverage yet returns ok=false, and the caller waits. An
// ok=false is an answer, not a failure -- the caller must not treat it as one.
//
// Anchored mode does not call this: it reads finality from its trusted block
// feed (BlockClient.FinalizedSlot), never from a blob source.
func (c *Client) FinalizedSlot(ctx context.Context) (slot uint64, ok bool, err error) {
	if c.head == "" {
		slot, err = c.beaconFinalizedSlot(ctx)
		return slot, err == nil, err
	}

	path := "/bloar/v1/heads/" + url.PathEscape(c.head) + "/synced_to"
	out, err := getJSON[mirrorSyncedToDTO](ctx, c, path, metaBodyCeiling)
	if err != nil {
		return 0, false, err
	}
	if out.SyncedTo == nil {
		return 0, false, nil
	}
	return *out.SyncedTo, true, nil
}

// mirrorSyncedToDTO decodes an archive upstream's synced_to (spec 7.1). A null
// synced_to is an archive with no coverage yet, which the caller waits on.
type mirrorSyncedToDTO struct {
	SyncedTo *uint64 `json:"synced_to"`
}

// OriginSlot reads this archive upstream's origin_slot from GET
// /bloar/v1/heads/{head} (spec 7.1). It is mirror mode's one validation: a
// bloar archive can only re-derive a head whose history it covers back to at
// least the local head's origin, so the caller requires this to be at or below
// its own origin before trusting the archive's answers (spec 11.5). Below that
// origin the archive 404s, which after this check can only be a protocol
// violation, never absence.
//
// It is meaningful only on an archive upstream (Head set); a beacon node serves
// no such endpoint.
func (c *Client) OriginSlot(ctx context.Context) (uint64, error) {
	// A pointer so an omitted or null origin_slot is refused rather than read as 0
	//. origin_slot is a required input to the mirror origin guard
	// (spec 11.5, loadOrigin): the guard trusts an upstream only if its coverage
	// begins at or below the local origin, and a defaulted 0 would pass that check
	// for free, letting a malformed head document authorize a mirror whose real
	// history was never validated. A required trusted-validation input that is
	// missing is a malformed document, so refusing it at startup is validation
	// hygiene -- an in-range 404 later is already a protocol error in classifyMirror,
	// never absence, so this is not that path. A real head document always states it.
	path := "/bloar/v1/heads/" + url.PathEscape(c.head)
	out, err := getJSON[originSlotDTO](ctx, c, path, metaBodyCeiling)
	if err != nil {
		return 0, err
	}
	if out.OriginSlot == nil {
		return 0, fmt.Errorf("upstream: GET %s: head document has no origin_slot; a mirror upstream must state the "+
			"slot its coverage begins at (spec 7.1) before it can be trusted to re-derive this head", path)
	}
	return *out.OriginSlot, nil
}

// originSlotDTO decodes an archive head document's origin_slot. A pointer so absent
// and null are distinct from an explicit 0.
type originSlotDTO struct {
	OriginSlot *uint64 `json:"origin_slot"`
}

// beaconFinalizedDTO decodes the slot out of a beacon node's finalized header (spec
// 10.1). This mirror/chain-mode reader takes only the slot; the anchored feed's
// BlockClient.FinalizedSlot is the one that also enforces the safety flags.
type beaconFinalizedDTO struct {
	Data struct {
		Header struct {
			Message struct {
				Slot string `json:"slot"`
			} `json:"message"`
		} `json:"header"`
	} `json:"data"`
}

// beaconFinalizedSlot reads GET /eth/v1/beacon/headers/finalized (spec 10.1).
func (c *Client) beaconFinalizedSlot(ctx context.Context) (uint64, error) {
	const path = "/eth/v1/beacon/headers/finalized"
	out, err := getJSON[beaconFinalizedDTO](ctx, c, path, metaBodyCeiling)
	if err != nil {
		return 0, err
	}
	raw := out.Data.Header.Message.Slot
	slot, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("upstream: %s: slot %q is not a number: %w", path, raw, err)
	}
	return slot, nil
}

// Blobs reads a slot's blobs from this one upstream: GET
// .../eth/v1/beacon/blobs/{slot} (spec 7.1). A nil or empty vhs asks for the
// whole slot; a non-empty one asks for exactly those blobs, in that order --
// what the chain indexer of spec 10.2 and anchored mode's filtered request both
// need.
//
// The answer is classified but not judged: a 200 is StatusFound (possibly
// empty), a 404 is StatusAbsent, a 503 is StatusNotYetCovered, and a terminal
// failure is an error. What those mean about the slot is the caller's call --
// anchored mode treats a source's StatusAbsent as "this source cannot help" and
// moves on, mirror mode treats an archive's StatusAbsent as a protocol violation.
//
// It negotiates spec 7.1's raw variant (blobsAccept): a bloar upstream answers
// application/octet-stream and both sides skip the hex round trip, a beacon node
// or a pre-variant bloar ignores the Accept and answers JSON. Either way the
// parsed blobs, their order, and the recorded byte count are the same.
func (c *Client) Blobs(ctx context.Context, slot uint64, vhs []schema.VersionedHash) (Result, error) {
	// Refuse an over-ceiling request before building the URL or dialing (finding
	// the request safety boundary). blobsBodyCeiling scales the success-body limit with
	// len(vhs), so an unchecked caller -- a malformed block feed that fit tens of
	// thousands of commitments inside the metadata cap, or a chain-RPC row set --
	// would otherwise raise the ceiling into the gigabytes. A slot holds at most
	// MaxBlobsPerSlotCeiling blobs, so a request for more is malformed and never
	// leaves the process; the caller's own guards (Commitments below) name the
	// source, this is the transport-level backstop shared by every path.
	if len(vhs) > schema.MaxBlobsPerSlotCeiling {
		return Result{}, fmt.Errorf("upstream: refusing a blobs request for %d versioned hashes; a slot holds at most %d, "+
			"so this is a malformed request and is not sent", len(vhs), schema.MaxBlobsPerSlotCeiling)
	}

	path := c.blobsPath(slot, vhs)
	start := time.Now()

	var res Result
	err := c.do(ctx, path, blobsAccept, blobsBodyCeiling(len(vhs)), func(body []byte, contentType string) error {
		var perr error
		res, perr = parseBlobs(path, vhs, body, contentType)
		return perr
	})
	if err != nil {
		var httpErr *HTTPError
		if errors.As(err, &httpErr) {
			// A 404 and a 503 are answers about the slot, not fetch failures: the
			// round trip happened and returned no blobs, so it is timed like any
			// other. A terminal error below (no answer after retries) is not.
			switch httpErr.Status {
			case http.StatusNotFound:
				c.metrics.UpstreamRead(0, time.Since(start))
				return Result{Status: StatusAbsent}, nil
			case http.StatusServiceUnavailable:
				c.metrics.UpstreamRead(0, time.Since(start))
				return Result{Status: StatusNotYetCovered}, nil
			}
		}
		return Result{}, err
	}

	// The count is raw blob bytes whichever variant answered, which is what the
	// throughput metric has always meant: JSON carries roughly twice this on the
	// wire as hex, octet-stream exactly this.
	n := 0
	for _, b := range res.Blobs {
		n += len(b)
	}
	c.metrics.UpstreamRead(n, time.Since(start))
	return res, nil
}

// blobsAccept negotiates spec 7.1's raw variant first and the JSON default
// second. A bloar upstream that implements the variant answers
// application/octet-stream and saves both sides the hex round trip; a real
// beacon node (and any pre-variant bloar) ignores the first type and answers
// JSON. Sending it to every upstream is safe precisely because JSON is what
// anything that does not implement the variant returns.
const blobsAccept = "application/octet-stream, application/json"

// parseBlobs turns a 200 blobs body into a Result, choosing the wire format by
// Content-Type. Anything that is not application/octet-stream is read as the
// JSON default, which is what a beacon node and a pre-variant bloar return.
func parseBlobs(path string, vhs []schema.VersionedHash, body []byte, contentType string) (Result, error) {
	if isOctetStream(contentType) {
		return parseOctetStreamBlobs(path, vhs, body)
	}
	return parseJSONBlobs(path, vhs, body)
}

// parseJSONBlobs reads the JSON default: {"data": ["0x<hex>", ...]}. Data is a
// pointer so an omitted or null data member is distinguishable from an explicitly
// present empty array: spec 7.1 states a covered slot with no blobs
// as a 200 whose data is [], and only that explicitly-present array is an answer
// about the slot. A missing or null data would decode to a nil slice and, read as
// zero blobs, advance coverage over the slot as covered-empty and freeze any real
// blobs there as absent.
func parseJSONBlobs(path string, vhs []schema.VersionedHash, body []byte) (Result, error) {
	var out struct {
		Data *[]string `json:"data"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		// A torn transfer lands here, and the transportError has it retried
		// exactly as a torn transfer of any body is.
		return Result{}, &transportError{err: fmt.Errorf("upstream: GET %s: decoding answer: %w", path, err)}
	}
	if out.Data == nil {
		// Fail closed as a retryable transport error, exactly as a torn body above:
		// a 200 that omits or nulls data is malformed, never a covered-empty slot.
		return Result{}, &transportError{err: fmt.Errorf("upstream: GET %s: 200 response omits the data array; a "+
			"covered slot with no blobs states it as [], so a missing or null data member is a malformed answer", path)}
	}
	data := *out.Data
	blobs := make([][]byte, 0, len(data))
	for i, s := range data {
		b, err := parseBlob(s)
		if err != nil {
			return Result{}, fmt.Errorf("upstream: %s: blob %d: %w", path, i, err)
		}
		blobs = append(blobs, b)
	}
	return foundBlobs(path, vhs, blobs)
}

// parseOctetStreamBlobs reads spec 7.1's raw variant: the concatenation of
// fixed-size blobs, self-framed by BlobSize.
func parseOctetStreamBlobs(path string, vhs []schema.VersionedHash, body []byte) (Result, error) {
	if len(body)%schema.BlobSize != 0 {
		// Not retryable: a torn transfer surfaces earlier as a read error, so a
		// body that arrived whole and is not a whole number of blobs is a
		// malformed answer, not a transient one.
		return Result{}, fmt.Errorf("upstream: %s: octet-stream body is %d bytes, not a multiple of the %d-byte blob size",
			path, len(body), schema.BlobSize)
	}
	blobs := make([][]byte, 0, len(body)/schema.BlobSize)
	for off := 0; off < len(body); off += schema.BlobSize {
		blobs = append(blobs, bytes.Clone(body[off:off+schema.BlobSize]))
	}
	return foundBlobs(path, vhs, blobs)
}

// foundBlobs applies spec 7.1's exact-count rule for a filtered request -- one
// blob per requested vh, in request order -- and wraps the blobs in a found
// Result. A short answer would silently misalign the caller's vh list against
// the bytes, and the chain indexer of spec 10.2 pairs them positionally.
func foundBlobs(path string, vhs []schema.VersionedHash, blobs [][]byte) (Result, error) {
	// A slot holds at most MaxBlobsPerSlotCeiling blobs, so no answer -- filtered or
	// not -- may carry more. The byte ceiling alone does
	// not bound the COUNT: the JSON-sized 128-entry limit fits roughly twice as many
	// raw octet-stream blobs, and a filtered request's exact-count check below does
	// not run for an unfiltered one. This caps the count before ingest ever sees the
	// slice.
	if len(blobs) > schema.MaxBlobsPerSlotCeiling {
		return Result{}, fmt.Errorf("upstream: %s: response carries %d blobs, more than the %d a slot can hold",
			path, len(blobs), schema.MaxBlobsPerSlotCeiling)
	}
	if len(vhs) > 0 && len(blobs) != len(vhs) {
		return Result{}, fmt.Errorf("upstream: %s: asked for %d blobs, got %d", path, len(vhs), len(blobs))
	}
	return Result{Status: StatusFound, Blobs: blobs}, nil
}

// isOctetStream reports whether a Content-Type is application/octet-stream,
// tolerating any parameters an upstream might append.
func isOctetStream(contentType string) bool {
	mediaType, _, err := mime.ParseMediaType(contentType)
	return err == nil && mediaType == "application/octet-stream"
}

// blobsPath builds the blobs URL for whichever kind of upstream this is.
func (c *Client) blobsPath(slot uint64, vhs []schema.VersionedHash) string {
	var b strings.Builder
	if c.head != "" {
		b.WriteString("/")
		b.WriteString(url.PathEscape(c.head))
	}
	b.WriteString("/eth/v1/beacon/blobs/")
	b.WriteString(strconv.FormatUint(slot, 10))

	if len(vhs) > 0 {
		// Built by hand rather than through url.Values, whose Encode sorts and
		// would lose the order spec 7.1 answers in. Nitro sends them this way
		// too, which is what the server's parser is written against.
		for i, vh := range vhs {
			if i == 0 {
				b.WriteString("?")
			} else {
				b.WriteString("&")
			}
			b.WriteString("versioned_hashes=0x")
			b.WriteString(hex.EncodeToString(vh[:]))
		}
	}
	return b.String()
}

// parseBlob decodes one hex blob from a data array.
func parseBlob(s string) ([]byte, error) {
	b, err := hex.DecodeString(strings.TrimPrefix(s, "0x"))
	if err != nil {
		return nil, fmt.Errorf("not hex: %w", err)
	}
	if len(b) != schema.BlobSize {
		return nil, fmt.Errorf("is %d bytes, want exactly %d", len(b), schema.BlobSize)
	}
	return b, nil
}

// HTTPError is a non-200 answer from an upstream.
type HTTPError struct {
	Path    string
	Status  int
	Message string
}

func (e *HTTPError) Error() string {
	if e.Message != "" {
		return fmt.Sprintf("upstream: GET %s: %d: %s", e.Path, e.Status, e.Message)
	}
	return fmt.Sprintf("upstream: GET %s: %d", e.Path, e.Status)
}

// do issues path with the given Accept header, retrying what is worth retrying,
// and hands each 200's body and Content-Type to handle. handle runs inside the
// retry loop on purpose: a body it rejects as a *transportError (a torn JSON
// answer, say) is retried exactly as a torn transfer is, while a body it rejects
// as anything else is terminal.
//
// 503 is not retried here even though it is a 5xx, and that is the one
// exception worth stating: from an archive upstream it means "not yet covered",
// which is an answer, and Blobs turns it into StatusNotYetCovered so the caller
// can stop the batch and wait. Retrying it inside this function would burn the
// backoff budget waiting for an indexer that runs on its own clock.
func (c *Client) do(ctx context.Context, path, accept string, maxBody int64, handle func(body []byte, contentType string) error) error {
	backoff := c.backoff
	var lastErr error

	for attempt := 1; attempt <= c.maxAttempts; attempt++ {
		if attempt > 1 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}
			backoff *= 2
		}

		body, contentType, err := c.attempt(ctx, path, accept, maxBody)
		if err == nil {
			err = handle(body, contentType)
		}
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if !retryable(err) {
			return err
		}
		lastErr = err
		c.log.Warn("upstream request failed, retrying", "path", path, "attempt", attempt, "of", c.maxAttempts, "err", err)
	}
	return fmt.Errorf("upstream: GET %s failed after %d attempts: %w", path, c.maxAttempts, lastErr)
}

// getJSON issues a GET and decodes a 200 as JSON into a freshly allocated *T. It
// reads the finality endpoints; Blobs uses do directly so it can branch on
// Content-Type. Free functions rather than methods because Go methods cannot take
// type parameters.
func getJSON[T any](ctx context.Context, c *Client, path string, maxBody int64) (*T, error) {
	return getJSONValidated[T](ctx, c, path, maxBody, nil)
}

// getJSONValidated decodes EACH 200 attempt into its OWN freshly allocated *T, then
// runs validate on that attempt's value. The fresh allocation per attempt is the
// point: Go's json.Unmarshal leaves a destination's omitted fields
// untouched, and a presence-aware DTO (optionalBool) does not clear prior state, so
// decoding successive retries into one shared value would let an earlier attempt's
// fields survive into a later one -- or two individually-incomplete responses
// combine into a synthetic answer that passes validation though no single request
// ever returned it. A fresh *T means validate inspects exactly one response, and
// only the succeeding attempt's *T reaches the caller.
//
// validate runs INSIDE the per-attempt handler so a syntactically-valid-but-
// incomplete answer is retried like a torn body rather than accepted as terminal;
// its error is wrapped retryable, so a malformed-first/corrected-second upstream
// recovers within the attempt budget.
func getJSONValidated[T any](ctx context.Context, c *Client, path string, maxBody int64, validate func(*T) error) (*T, error) {
	var result *T
	err := c.do(ctx, path, "application/json", maxBody, func(body []byte, _ string) error {
		out := new(T)
		if err := json.Unmarshal(body, out); err != nil {
			return &transportError{err: fmt.Errorf("upstream: GET %s: decoding answer: %w", path, err)}
		}
		if validate != nil {
			if err := validate(out); err != nil {
				return &transportError{err: err}
			}
		}
		result = out
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// attempt issues exactly one GET with the given Accept header and returns a
// 200's body and Content-Type. A non-200 becomes an *HTTPError.
func (c *Client) attempt(ctx context.Context, path, accept string, maxBody int64) (body []byte, contentType string, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base+path, nil)
	if err != nil {
		return nil, "", fmt.Errorf("upstream: building GET %s: %w", path, err)
	}
	req.Header.Set("Accept", accept)

	resp, err := c.hc.Do(req)
	if err != nil {
		return nil, "", &transportError{err: fmt.Errorf("upstream: GET %s: %w", path, err)}
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		// The cap belongs on error bodies only: a misbehaving upstream must not
		// be able to make an error message unbounded.
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		httpErr := &HTTPError{Path: path, Status: resp.StatusCode}
		var b struct {
			Message string `json:"message"`
		}
		if err := json.Unmarshal(raw, &b); err == nil && b.Message != "" {
			httpErr.Message = b.Message
		} else if len(raw) > 0 {
			httpErr.Message = strings.TrimSpace(string(raw))
		}
		return nil, "", httpErr
	}
	// A declared length over the ceiling is refused before a byte is read (finding
	// the safety boundary). This is a fast-fail only, never the enforcement: a chunked body
	// declares nothing, and a transparently gunzipped one reports ContentLength -1,
	// so the honest path is the LimitReader below. A negative (unknown) length just
	// falls through to it.
	if resp.ContentLength > maxBody {
		return nil, "", &transportError{err: fmt.Errorf("upstream: GET %s: 200 declares %d bytes, over the %d-byte "+
			"ceiling for this endpoint", path, resp.ContentLength, maxBody)}
	}
	// Success bodies are read through a per-endpoint ceiling plus one sentinel byte
	//: a filtered slot is small, an unfiltered octet-stream slot
	// runs to max_blobs*BlobSize, and the JSON variant is twice that, but nothing
	// legitimate exceeds maxBody. The +1 is the overrun probe -- a read that
	// returns it is a body larger than the ceiling, malformed, and refused. The
	// limit wraps resp.Body, the decoded stream the transport already gunzipped, so
	// a gzip-expanded body is bounded at the same maxBody as a plain one. A torn
	// read here is a transportError, so it retries like any torn transfer.
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxBody+1))
	if err != nil {
		return nil, "", &transportError{err: fmt.Errorf("upstream: GET %s: reading answer: %w", path, err)}
	}
	if int64(len(raw)) > maxBody {
		return nil, "", &transportError{err: fmt.Errorf("upstream: GET %s: 200 body exceeds the %d-byte ceiling for "+
			"this endpoint; a well-formed answer never does, so this is a malformed or hostile response", path, maxBody)}
	}
	return raw, resp.Header.Get("Content-Type"), nil
}

// transportError marks a failure that never got a status.
type transportError struct{ err error }

func (e *transportError) Error() string { return e.err.Error() }
func (e *transportError) Unwrap() error { return e.err }

// retryable reports whether repeating a GET could change the answer.
func retryable(err error) bool {
	var transport *transportError
	if errors.As(err, &transport) {
		return true
	}
	var httpErr *HTTPError
	if errors.As(err, &httpErr) {
		switch httpErr.Status {
		case http.StatusServiceUnavailable:
			// 503 is an answer, not a failure: Blobs maps it to StatusNotYetCovered
			// and the caller waits on its own poll interval.
			return false
		case http.StatusTooManyRequests, http.StatusRequestTimeout:
			// 429 and 408 are a provider's canonical back-off signals -- a paid
			// full-history source rate-limiting a backfill, or a slow hop timing
			// out -- and the retry loop's growing backoff is exactly the response
			// they ask for. Terminal-failing them would drop a slot the source
			// would have served a moment later.
			return true
		}
		return httpErr.Status >= 500
	}
	return false
}
