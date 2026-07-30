// Package server implements bloar's HTTP API: the beacon-compatible read API
// of spec 7.1, the bloar API of 7.2, the bearer auth of 7.3, and the HTTPS
// channel of the head publication of spec 8.
//
// # What lives here besides handlers
//
// Heads is the registry the handlers resolve names through, and also the
// serializer every mutation runs under and the publisher of the document those
// mutations produce. Spec 8 requires the document to change atomically with
// root swaps, which makes mutation, root persistence and publication one step
// rather than three; see the type's comment for why that step is here and not
// in the handlers.
//
// RootStore is the other half of that: the head engine hands back a root CID
// per mutation and has nowhere to keep it, so this package persists it and
// resumes from it (OpenHead). Only the writer role needs this; a phase-8
// follower adopts roots from a published document instead.
//
// # Key layout
//
// This package owns four bytes of the node-local KV keyspace that store.KV()
// hands out. catalog's package comment documents its two ('c' catalog, 'p' pin
// ledger) and the rule they all follow: single-byte prefixes, no key of one
// structure a prefix of another's. p2p owns 'i' (the IPNS record sequence) and
// follow owns 'f' (its no-regression floors).
//
//	head root             key: 'h' || name          val: cid.Bytes()
//	manifest tip          key: 'm' || name          val: cid.Bytes()  (spec 10.5)
//	mutable generation    key: 'g' || name          val: GenerationState JSON
//	publication revision  key: 'r' || signer[32]    val: uint64 big-endian
//
// Head names are never scanned as a range, so the name needs no terminator; the
// prefix byte alone keeps these keys clear of the catalog's and the ledger's.
//
// # Cache control
//
// Spec 7.1's caching rules are the load-bearing performance feature of the read
// API: Nitro syncs by fetching one slot at a time in ascending order, serially,
// so per-request latency gates sync speed and a CDN in front of an archive is
// the mitigation. Everything old enough to be beyond the immutable horizon is
// served immutable for a year; everything newer gets a minute, because
// truncation (spec 5.4) is theoretically possible.
package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/ipfs/boxo/blockstore"

	"github.com/blobarchive/bloar/ingest"
	"github.com/blobarchive/bloar/metrics"
	"github.com/blobarchive/bloar/schema"
)

// Beacon is the static network configuration the two beacon endpoints of spec
// 7.1 serve. Nitro's BlobClient.Initialize reads GenesisTime from /genesis and
// SECONDS_PER_SLOT from /config/spec, and computes the slot of every blob it
// wants from them, so these are not decoration: wrong values here are wrong
// slots at the client.
type Beacon struct {
	GenesisTime           uint64
	SecondsPerSlot        uint64
	GenesisValidatorsRoot string
	GenesisForkVersion    string
	// Spec is the /eth/v1/config/spec passthrough (config's beacon.spec_extra).
	// SECONDS_PER_SLOT is added from SecondsPerSlot and must not be in here.
	Spec map[string]string
}

// LiveHead is one local-only virtual beacon view. It is deliberately absent
// from the publication document: FinalizedHead and UnfinalizedHead remain the
// independently authenticated physical heads, while this name is only a
// serving policy which joins them at the finalized coverage frontier.
type LiveHead struct {
	FinalizedHead   string
	UnfinalizedHead string
	// RequireVersionedHashes makes the provisional half of this view an
	// exact-hash overlay. Finalized slots keep the ordinary beacon API (and may
	// therefore be enumerated), while a slot selected from UnfinalizedHead must
	// name at least one versioned_hashes value. This permits a chain-filtered
	// finalized head to share a globally authenticated mutable head without
	// turning the global live tip into an enumeration endpoint.
	RequireVersionedHashes bool
}

// Config is everything a Server serves from.
type Config struct {
	// Heads is the registry of heads this node writes. Required.
	Heads *Heads
	// Blocks is where blob bytes are read from. Required.
	Blocks blockstore.Blockstore
	// ReadOnly mounts only the public GET surface. It is intended for replicas
	// which can serve an authenticated archive but have no authority to mutate
	// it. In read-only mode Ingester and AuthToken must both be absent.
	ReadOnly bool
	// Ingester backs POST /bloar/v1/blobs. Required unless ReadOnly is true.
	Ingester *ingest.Ingester
	// Beacon is the static config of spec 7.1's two other endpoints.
	Beacon Beacon
	// LiveHeads maps local virtual beacon names to one finalized physical head
	// and one bounded mutable physical head. The map is optional. Virtual names
	// are never added to GET /bloar/v1/heads and cannot be mutation targets.
	LiveHeads map[string]LiveHead
	// AuthToken is the bearer token of spec 7.3. Required and never empty unless
	// ReadOnly is true: the endpoints it guards write to the archive.
	AuthToken string
	// MaxPutBlobs bounds a POST /bloar/v1/blobs body (spec 7.2). Zero means the
	// spec default of 64.
	MaxPutBlobs int
	// MaxQueryHashes bounds how many decoded versioned_hashes entries one blobs
	// request may carry across repeated-key or comma-separated array encoding,
	// duplicates included (spec 7.1). A request naming more is 400 before any
	// lookup, which is the count half of the safety boundary's fix. Zero means the
	// default, the protocol ceiling of 128; any set value must be in
	// [1, schema.MaxBlobsPerSlotCeiling], since a request may name no more entries
	// than a slot can hold, and New rejects anything outside that range.
	MaxQueryHashes int
	// MaxResponseBytesInFlight is the process-wide ceiling, in bytes, on the peak
	// live memory all in-flight blob responses may reserve at once (finding
	// the safety boundary). Every blob-carrying response is admitted against it before it
	// reads anything and holds its reservation until it is written. Zero means
	// the default of 1 GiB; any value must admit at least one maximum response
	// (MaxResponseWeight), or New rejects it.
	MaxResponseBytesInFlight int64
	// ImmutableHorizonSlots is how far behind synced_to a slot must be before
	// its answers are cached immutably (spec 7.1). Zero means the spec default
	// of 7200, one day of slots.
	ImmutableHorizonSlots uint64
	// MutationBodyReadTimeout is how long a VALID mutation (an authenticated POST
	// past its head and framing checks) is given to finish uploading its bounded
	// body. It is installed as a read-deadline extension before
	// the first body read, so the daemon's short server-level ReadTimeout can bound
	// slow rejected bodies without cutting off a legitimate multi-megabyte upload.
	// Zero is replaced by a safe nonzero default (New never leaves the refinement
	// off); a negative value is a config error New rejects.
	MutationBodyReadTimeout time.Duration
	// BlobResponseWriteTimeout caps how long the public blobs endpoint may take to
	// write one response. It is installed as a write deadline
	// before the body is written, so a slow reader cannot hold the handler and its
	// admission reservation open indefinitely. The budget is
	// generous -- a maximum multi-megabyte response must complete for even a poor
	// link -- but finite. Zero is replaced by a safe nonzero default (New never
	// leaves the deadline off); a negative value is a config error New rejects.
	BlobResponseWriteTimeout time.Duration
	// Metrics instruments the read path. Optional; nil records nothing, and
	// costs the read path nothing (see instrumentRead).
	Metrics *metrics.Metrics
	// PublicReadLimiter optionally applies weighted global and per-client token
	// buckets to the public GET API. Nil disables rate admission. It never wraps
	// authenticated mutations; the daemon's metrics endpoint is outside Server
	// and therefore outside this limiter as well.
	PublicReadLimiter *PublicReadLimiterConfig
	// Logger receives what the HTTP layer has to say. Optional.
	Logger *slog.Logger
}

// Spec defaults (7.1, 7.2, 12).
const (
	defaultMaxPutBlobs           = 64
	defaultImmutableHorizonSlots = 7200
	// The count cap defaults to the protocol ceiling: a request naming more
	// distinct-or-repeated hashes than any slot could ever hold is not a client
	// bloar serves.
	defaultMaxQueryHashes = schema.MaxBlobsPerSlotCeiling
	// One gibibyte of response memory in flight: about 21 maximum responses at the
	// default cap, or hundreds of the one-to-nine-blob reads Nitro actually makes
	//. Ceiling, not reservation -- it costs nothing idle.
	defaultMaxResponseBytesInFlight = 1 << 30
	// Safe standalone defaults for the per-request deadline refinements of finding
	// the safety boundary. Both are nonzero so a Server constructed directly -- without
	// cmd/bloard's config in front of it -- still bounds a stalled upload and a
	// slow reader. 60s uploads the largest mutation body (a few MiB) at any
	// reasonable link speed; 120s writes the largest blobs response likewise.
	defaultMutationBodyReadTimeout  = 60 * time.Second
	defaultBlobResponseWriteTimeout = 120 * time.Second
)

// retryAfterSeconds is the Retry-After of every 503 (spec 7.1). It is the
// literal 12 the spec names rather than the configured seconds_per_slot: a
// client that retries a not-yet-archived slot is waiting for the indexer, whose
// cadence is its own, and the spec fixes the number.
const retryAfterSeconds = "12"

// Server is the HTTP API. It is an http.Handler; the caller owns the listener.
type Server struct {
	cfg Config
	log *slog.Logger
	mux *http.ServeMux

	// admission is the response-memory budget of the safety boundary, shared by every
	// blob-carrying response.
	admission *admission
	// publicReadLimiter is the optional request-rate budget for public GETs. It is
	// separate from admission: this bounds work offered by clients; admission
	// bounds response memory after a request is accepted.
	publicReadLimiter *publicReadLimiter

	// Both endpoints of spec 7.1 that are pure config are rendered once here:
	// they cannot change while the process runs.
	genesis []byte
	spec    []byte
}

// New returns a Server over cfg.
func New(cfg Config) (*Server, error) {
	switch {
	case cfg.Heads == nil:
		return nil, errors.New("server: Config.Heads must not be nil")
	case cfg.Blocks == nil:
		return nil, errors.New("server: Config.Blocks must not be nil")
	case cfg.ReadOnly && cfg.Ingester != nil:
		return nil, errors.New("server: read-only Config must not carry an Ingester")
	case cfg.ReadOnly && cfg.AuthToken != "":
		return nil, errors.New("server: read-only Config must not carry an AuthToken")
	case !cfg.ReadOnly && cfg.Ingester == nil:
		return nil, errors.New("server: Config.Ingester must not be nil")
	case !cfg.ReadOnly && cfg.AuthToken == "":
		// An empty token is not "no auth": it would make every request with an
		// empty bearer token an admin.
		return nil, errors.New("server: Config.AuthToken must not be empty")
	case cfg.Beacon.SecondsPerSlot == 0:
		return nil, errors.New("server: Config.Beacon.SecondsPerSlot must not be zero")
	}
	if _, dup := cfg.Beacon.Spec["SECONDS_PER_SLOT"]; dup {
		return nil, errors.New("server: Config.Beacon.Spec must not carry SECONDS_PER_SLOT; it is served from Config.Beacon.SecondsPerSlot")
	}
	if err := validateConfiguredLiveHeads(cfg.Heads, cfg.LiveHeads); err != nil {
		return nil, err
	}
	if cfg.LiveHeads != nil {
		live := make(map[string]LiveHead, len(cfg.LiveHeads))
		for name, view := range cfg.LiveHeads {
			live[name] = view
		}
		cfg.LiveHeads = live
	}
	if cfg.MaxPutBlobs == 0 {
		cfg.MaxPutBlobs = defaultMaxPutBlobs
	}
	if cfg.MaxPutBlobs < 0 {
		return nil, fmt.Errorf("server: Config.MaxPutBlobs is %d, must be positive", cfg.MaxPutBlobs)
	}
	if cfg.MaxQueryHashes == 0 {
		cfg.MaxQueryHashes = defaultMaxQueryHashes
	}
	if cfg.MaxQueryHashes < 1 || cfg.MaxQueryHashes > schema.MaxBlobsPerSlotCeiling {
		return nil, fmt.Errorf("server: Config.MaxQueryHashes is %d, must be in [1, %d]: a filtered request may name no "+
			"more entries than a slot can hold, the per-slot blob ceiling; a larger cap would restore the amplification "+
			"surface of the safety boundary", cfg.MaxQueryHashes, schema.MaxBlobsPerSlotCeiling)
	}
	if cfg.MaxResponseBytesInFlight == 0 {
		cfg.MaxResponseBytesInFlight = defaultMaxResponseBytesInFlight
	}
	if cfg.MaxResponseBytesInFlight < 0 {
		return nil, fmt.Errorf("server: Config.MaxResponseBytesInFlight is %d, must be positive", cfg.MaxResponseBytesInFlight)
	}
	// The budget must admit one maximum response, or a request at the cap would
	// wait on a reservation the budget can never grant and block forever (finding
	// the safety boundary).
	if min := MaxResponseWeight(cfg.MaxQueryHashes); cfg.MaxResponseBytesInFlight < min {
		return nil, fmt.Errorf("server: Config.MaxResponseBytesInFlight is %d bytes, but one maximum response can peak at %d "+
			"bytes (%d hashes x worst-case per-entry weight); the budget must admit at least one", cfg.MaxResponseBytesInFlight, min, cfg.MaxQueryHashes)
	}
	if cfg.ImmutableHorizonSlots == 0 {
		cfg.ImmutableHorizonSlots = defaultImmutableHorizonSlots
	}
	// A negative timeout is a config mistake and is rejected; zero (an unset field)
	// is REPLACED with the safe standalone default below -- it does not disable the
	// refinement. New never leaves either bound off.
	if cfg.MutationBodyReadTimeout < 0 {
		return nil, fmt.Errorf("server: Config.MutationBodyReadTimeout is %s, must not be negative", cfg.MutationBodyReadTimeout)
	}
	if cfg.MutationBodyReadTimeout == 0 {
		cfg.MutationBodyReadTimeout = defaultMutationBodyReadTimeout
	}
	if cfg.BlobResponseWriteTimeout < 0 {
		return nil, fmt.Errorf("server: Config.BlobResponseWriteTimeout is %s, must not be negative", cfg.BlobResponseWriteTimeout)
	}
	if cfg.BlobResponseWriteTimeout == 0 {
		cfg.BlobResponseWriteTimeout = defaultBlobResponseWriteTimeout
	}

	var publicReadLimiter *publicReadLimiter
	if cfg.PublicReadLimiter != nil {
		var err error
		publicReadLimiter, err = newPublicReadLimiter(*cfg.PublicReadLimiter, cfg.MaxQueryHashes)
		if err != nil {
			return nil, err
		}
	}

	s := &Server{
		cfg:               cfg,
		log:               cfg.Logger,
		mux:               http.NewServeMux(),
		admission:         newAdmission(cfg.MaxResponseBytesInFlight),
		publicReadLimiter: publicReadLimiter,
	}
	if s.log == nil {
		s.log = slog.New(slog.DiscardHandler)
	}

	var err error
	if s.genesis, err = renderGenesis(cfg.Beacon); err != nil {
		return nil, err
	}
	if s.spec, err = renderSpec(cfg.Beacon); err != nil {
		return nil, err
	}
	s.routes()
	return s, nil
}

// validateConfiguredLiveHeads preserves the ordinary live-view validation and
// relaxes only its handoff-name equality for an exact-hash overlay. The mutable
// entry must still be independently proof-valid against its authenticated
// handoff at read time; the filtered finalized head is a serving frontier, not
// a replacement proof authority.
func validateConfiguredLiveHeads(heads *Heads, views map[string]LiveHead) error {
	ordinary := make(map[string]LiveHead)
	for name, view := range views {
		if !view.RequireVersionedHashes {
			ordinary[name] = view
			continue
		}

		reg := heads.reg.Load()
		switch {
		case name == "":
			return errors.New("server: Config.LiveHeads contains an empty virtual name")
		case strings.Contains(name, "/"):
			return fmt.Errorf("server: virtual live head %q is not one URL path segment", name)
		case view.FinalizedHead == "":
			return fmt.Errorf("server: virtual live head %q has an empty finalized head", name)
		case view.UnfinalizedHead == "":
			return fmt.Errorf("server: virtual live head %q has an empty unfinalized head", name)
		case view.FinalizedHead == view.UnfinalizedHead:
			return fmt.Errorf("server: virtual live head %q maps both roles to physical head %q", name, view.FinalizedHead)
		}
		if _, collision := reg.byName[name]; collision {
			return fmt.Errorf("server: virtual live head %q collides with a physical head", name)
		}
		if e := reg.byName[view.FinalizedHead]; e != nil && e.kind != FinalizedMonotonic {
			return fmt.Errorf("server: virtual live head %q finalized head %q is %q, want %q",
				name, view.FinalizedHead, e.kind, FinalizedMonotonic)
		}
		if e := reg.byName[view.UnfinalizedHead]; e != nil && e.kind != UnfinalizedMutable {
			return fmt.Errorf("server: virtual live head %q unfinalized head %q is %q, want %q",
				name, view.UnfinalizedHead, e.kind, UnfinalizedMutable)
		}
	}
	return validateLiveHeads(heads, ordinary)
}

// routes mounts spec 7. The beacon paths are per-head and the bloar paths are
// not, which is what keeps them from colliding: no request can match both
// (their second segments are distinct literals).
func (s *Server) routes() {
	// 7.1, per head. The blobs endpoint is the only one instrumented: it is the
	// one Nitro syncs through, the only one that touches the store, and the
	// only one whose latency is anything but constant.
	s.mux.HandleFunc("GET /{head}/eth/v1/beacon/blobs/{slot}", s.instrumentRead(s.limitPublicRead(publicReadBlobs, s.handleBlobs)))
	s.mux.HandleFunc("GET /{head}/eth/v1/beacon/genesis", s.limitPublicRead(publicReadMetadata, s.handleGenesis))
	s.mux.HandleFunc("GET /{head}/eth/v1/config/spec", s.limitPublicRead(publicReadMetadata, s.handleSpec))

	// 7.2, public.
	s.mux.HandleFunc("GET /bloar/v1/heads", s.limitPublicRead(publicReadMetadata, s.handleHeads))
	s.mux.HandleFunc("GET /bloar/v1/heads/{head}", s.limitPublicRead(publicReadMetadata, s.handleHead))
	s.mux.HandleFunc("GET /bloar/v1/heads/{head}/synced_to", s.limitPublicRead(publicReadMetadata, s.handleSyncedTo))
	s.mux.HandleFunc("GET /bloar/v1/heads/{head}/manifest", s.limitPublicRead(publicReadMetadata, s.handleGetManifest))
	s.mux.HandleFunc("GET /bloar/v1/heads/{head}/generation", s.limitPublicRead(publicReadMetadata, s.handleGetGeneration))

	if !s.cfg.ReadOnly {
		// 7.2, authenticated (7.3). A read-only server does not mount these
		// routes at all: it has no dummy token and no hidden mutation authority.
		s.mux.HandleFunc("POST /bloar/v1/blobs", s.auth(s.handlePutBlobs))
		s.mux.HandleFunc("POST /bloar/v1/heads/{head}/refs", s.auth(s.handleRefs))
		s.mux.HandleFunc("POST /bloar/v1/heads/{head}/truncate", s.auth(s.handleTruncate))
		s.mux.HandleFunc("POST /bloar/v1/heads/{head}/manifest", s.auth(s.handleManifest))
		s.mux.HandleFunc("POST /bloar/v1/heads/{head}/generation", s.auth(s.handleGeneration))
	}

	// Everything else. Spec 7 says the API answers in JSON, so the mux's
	// plain-text default will not do. The cost is that a wrong method on a real
	// route lands here as a 404 instead of the mux's 405: this pattern matches
	// it, and a match beats a method rejection.
	s.mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		writeError(w, http.StatusNotFound, "no such endpoint: %s %s", r.Method, r.URL.Path)
	})
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.mux.ServeHTTP(w, r) }

// statusRecorder is an http.ResponseWriter that remembers what status was
// written. A handler that writes a body without calling WriteHeader has written
// a 200, which is the zero value's meaning here.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (w *statusRecorder) WriteHeader(status int) {
	if w.status == 0 {
		w.status = status
	}
	w.ResponseWriter.WriteHeader(status)
}

func (w *statusRecorder) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.ResponseWriter.Write(b)
}

// Unwrap exposes the wrapped writer to http.ResponseController, which walks the
// Unwrap chain to reach the underlying connection. Without it,
// wrapping the live writer in this metrics recorder makes SetWriteDeadline (and
// SetReadDeadline) return http.ErrNotSupported, and the blobs handler's write
// deadline is then silently skipped for every request whenever metrics are
// enabled -- which the shipped writer example is. The one method the recorder
// overrides, WriteHeader/Write, still record the status; the controller reaches
// past them to the connection.
func (w *statusRecorder) Unwrap() http.ResponseWriter { return w.ResponseWriter }

// instrumentRead records one read of spec 7.1's blobs endpoint: its head, the
// class of its status, and how long it took.
//
// # The head label is not the path segment
//
// {head} is whatever the client put in the URL, and the read API is public.
// Labelling by it directly would let anyone mint an unbounded number of time
// series by requesting /aaa/, /aab/, ... -- a memory leak in the exporter,
// reachable by GET, from the internet. So an unknown head is labelled with a
// single constant instead, and the registry is what decides which is which. The
// 404 is still counted; it is just counted under a name that cannot grow.
func (s *Server) instrumentRead(next http.HandlerFunc) http.HandlerFunc {
	if s.cfg.Metrics == nil {
		// Nothing to record, so nothing to wrap: no ResponseWriter indirection
		// on the path Nitro syncs through.
		return next
	}
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w}
		next(rec, r)

		head := r.PathValue("head")
		if _, known := s.cfg.Heads.entry(head); !known {
			if _, virtual := s.cfg.LiveHeads[head]; !virtual {
				head = unknownHeadLabel
			}
		}
		status := rec.status
		if status == 0 {
			status = http.StatusOK
		}
		s.cfg.Metrics.BeaconRead(head, status, time.Since(start))
	}
}

// unknownHeadLabel is the `head` label of a read for a head this node does not
// have. See instrumentRead.
const unknownHeadLabel = "_unknown"

// renderGenesis renders GET /{head}/eth/v1/beacon/genesis (spec 7.1). Every
// value is a string: that is the beacon API's convention for the whole "data"
// map, including the numeric ones, and Nitro unmarshals genesis_time into a
// string field.
func renderGenesis(b Beacon) ([]byte, error) {
	body, err := json.Marshal(map[string]any{"data": map[string]string{
		"genesis_time":            strconv.FormatUint(b.GenesisTime, 10),
		"genesis_validators_root": b.GenesisValidatorsRoot,
		"genesis_fork_version":    b.GenesisForkVersion,
	}})
	if err != nil {
		return nil, fmt.Errorf("server: rendering genesis: %w", err)
	}
	return body, nil
}

// renderSpec renders GET /{head}/eth/v1/config/spec (spec 7.1): SECONDS_PER_SLOT,
// which Nitro reads, plus whatever else the config passes through.
func renderSpec(b Beacon) ([]byte, error) {
	data := make(map[string]string, len(b.Spec)+1)
	for k, v := range b.Spec {
		data[k] = v
	}
	data["SECONDS_PER_SLOT"] = strconv.FormatUint(b.SecondsPerSlot, 10)

	body, err := json.Marshal(map[string]any{"data": data})
	if err != nil {
		return nil, fmt.Errorf("server: rendering spec: %w", err)
	}
	return body, nil
}
