package server

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"math"
	"net/http"
	"strconv"
	"strings"

	format "github.com/ipfs/go-ipld-format"

	"github.com/blobarchive/bloar/archive"
	"github.com/blobarchive/bloar/schema"
	"github.com/blobarchive/bloar/store"
)

// octetStream is the media type of spec 7.1's raw response variant.
const octetStream = "application/octet-stream"

// acceptsOctetStream reports whether r asked for the raw-bytes variant of the
// blobs response (spec 7.1).
//
// The negotiation is deliberately minimal, and deliberately not general content
// negotiation: this variant is an opt-in for bloar's own client on the
// archive-to-archive read path, not a feature offered to arbitrary callers. So
// q-values are ignored and `*/*` does not select it -- only an explicit
// application/octet-stream in the Accept list does. Everything else (no header,
// application/json, a wildcard, an unrecognised type) falls through to the JSON
// default byte-for-byte, which is what keeps Nitro from ever seeing this branch.
func acceptsOctetStream(r *http.Request) bool {
	for _, part := range strings.Split(r.Header.Get("Accept"), ",") {
		mediaType := strings.TrimSpace(part)
		// Drop any ";q=..." or other parameters before matching the type.
		if i := strings.IndexByte(mediaType, ';'); i >= 0 {
			mediaType = strings.TrimSpace(mediaType[:i])
		}
		if mediaType == octetStream {
			return true
		}
	}
	return false
}

// blobsResponse is the body of a 200 from spec 7.1's blobs endpoint. Nitro
// reads nothing else: no commitments, no proofs, no slot echo. Blobs are
// self-certifying, and it re-derives the versioned hash of every one of these
// itself.
//
// It is the canonical shape renderBlobsJSON reproduces byte-for-byte; the tests
// pin that equivalence, so this type stays the definition of the wire format
// even though the render path no longer marshals it.
type blobsResponse struct {
	Data []string `json:"data"`
}

// JSON blobs-response layout of spec 7.1, exactly as encoding/json renders
// blobsResponse: {"data":[ "0x"<hex> , ... ]} with no spaces. renderBlobsJSON
// builds precisely these bytes without json.Marshal, so the response size is
// deterministic -- which is what lets the admission weight be an exact bound
// rather than a guess at a growable, pooled marshal buffer's peak (finding
// the safety boundary).
const (
	// jsonEnvelope is the fixed framing of an empty response: {"data":[]}.
	jsonEnvelope = len(`{"data":[]}`)
	// jsonPerEntry is one blob's bytes: "0x"+hex(BlobSize) quoted, plus the comma
	// that precedes every entry but the first. Counting a comma on every entry
	// over-counts the total by one, which blobsJSONSize subtracts back.
	jsonPerEntry = len(`"0x"`) + 2*schema.BlobSize + len(`,`)
)

// blobsJSONSize is the exact byte length renderBlobsJSON produces for n blobs of
// schema.BlobSize each.
func blobsJSONSize(n int) int {
	if n == 0 {
		return jsonEnvelope
	}
	return jsonEnvelope + n*jsonPerEntry - 1
}

// renderBlobsJSON renders the JSON blobs response of spec 7.1 into one buffer
// pre-sized to exactly blobsJSONSize(len(raws)): no json.Marshal, no growable or
// pooled buffer, no intermediate []string. Each entry is "0x"+lowercase-hex, so
// the bytes are identical to json.Marshal(blobsResponse{Data: ...}). Because the
// buffer neither grows nor is retained in any pool, a response's peak memory is
// exactly the raws plus this buffer -- the bound weightPerEntryJSON charges.
func renderBlobsJSON(raws [][]byte) []byte {
	buf := make([]byte, 0, blobsJSONSize(len(raws)))
	buf = append(buf, `{"data":[`...)
	for i, b := range raws {
		if i > 0 {
			buf = append(buf, ',')
		}
		buf = append(buf, `"0x`...)
		buf = hex.AppendEncode(buf, b)
		buf = append(buf, '"')
	}
	buf = append(buf, `]}`...)
	return buf
}

// resolve resolves the head in the request path for a read (spec 7.1), and
// answers the two ways it can fail to be one.
//
// Quarantine (spec 11.4) is 503 rather than 404 on purpose. A 404 from this API
// is a statement about data -- this slot carries no such blob, and it never
// will -- and a quarantined head has made no such statement: it is a head whose
// writer this node has caught serving something that does not verify, and what
// it owes a client is "not from me", not a fact about the chain. There is no
// Retry-After, because nothing here is going to change without an operator.
func (s *Server) resolve(w http.ResponseWriter, r *http.Request) (*entry, bool) {
	name := r.PathValue("head")
	e, ok := s.cfg.Heads.entry(name)
	if !ok {
		writeError(w, http.StatusNotFound, "unknown head %q", name)
		return nil, false
	}
	if e.quarantine != "" {
		w.Header().Set("Cache-Control", "no-store")
		writeError(w, http.StatusServiceUnavailable,
			"head %q is quarantined and is not served by this node: %s", name, e.quarantine)
		return nil, false
	}
	if e.kind == UnfinalizedMutable && (e.durable == nil || !e.proofValid) {
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("Retry-After", retryAfterSeconds)
		writeError(w, http.StatusServiceUnavailable,
			"head %q has no coherent finalized handoff proof; retry", name)
		return nil, false
	}
	if e.durable == nil {
		writeError(w, http.StatusNotFound, "unknown head %q", name)
		return nil, false
	}
	return e, true
}

const (
	finalityHeader      = "X-Bloar-Finality"
	finalityFinalized   = "finalized"
	finalityProvisional = "provisional"
)

// resolveLiveSlot selects one physical head for a virtual read. Both entries
// come from one registry load; that snapshot is the handoff decision for the
// whole request even if either physical head advances while the lookup runs.
func (s *Server) resolveLiveSlot(w http.ResponseWriter, name string, view LiveHead, slot uint64) (*entry, bool) {
	finalized, unfinalized := s.cfg.Heads.liveEntries(view)
	if finalized == nil || finalized.durable == nil || finalized.quarantine != "" ||
		finalized.kind != FinalizedMonotonic || finalized.durable.EffectiveKind() != FinalizedMonotonic ||
		finalized.durable.SyncedTo == nil {
		s.serveLiveUnavailable(w, name, slot)
		return nil, false
	}

	// Finalized presence AND absence are authoritative at and below this
	// frontier. Never fall back to an optimistic generation for a miss here.
	if slot <= *finalized.durable.SyncedTo {
		w.Header().Set(finalityHeader, finalityFinalized)
		return finalized, true
	}

	// Above the finalized frontier, the bounded generation is usable only when
	// its exact durable coverage contains this slot. Missing startup state, a
	// quarantine, or either side of a handoff gap is uncertainty, never absence.
	if unfinalized == nil || unfinalized.durable == nil || unfinalized.quarantine != "" ||
		unfinalized.kind != UnfinalizedMutable || unfinalized.durable.EffectiveKind() != UnfinalizedMutable ||
		!unfinalized.proofValid || unfinalized.proof == nil ||
		unfinalized.durable.WindowStart == nil || unfinalized.durable.SyncedTo == nil ||
		slot < *unfinalized.durable.WindowStart || slot > *unfinalized.durable.SyncedTo {
		s.serveLiveUnavailable(w, name, slot)
		return nil, false
	}
	if view.RequireVersionedHashes {
		// A filtered finalized head and the global mutable head have different
		// authenticated handoff names by design. proofValid above still proves the
		// mutable generation against its own authority; this additional boundary
		// check ensures the filtered view does not claim continuous coverage across
		// a slot neither half can answer for.
		frontier := *finalized.durable.SyncedTo
		if frontier != math.MaxUint64 && *unfinalized.durable.WindowStart > frontier+1 {
			s.serveLiveUnavailable(w, name, slot)
			return nil, false
		}
	} else if unfinalized.proof.HandoffHead != view.FinalizedHead {
		// Ordinary live views retain the stronger same-head handoff relationship.
		s.serveLiveUnavailable(w, name, slot)
		return nil, false
	}
	w.Header().Set(finalityHeader, finalityProvisional)
	// Set this before lookup so every provisional outcome, including an
	// integrity/read failure, is conservatively uncacheable.
	w.Header().Set("Cache-Control", "no-store")
	return unfinalized, true
}

// requireLiveHashes prevents an exact-hash overlay's global provisional tip
// from becoming an enumeration surface. It is intentionally applied only
// after slot resolution: finalized slots preserve the ordinary unfiltered
// beacon API, and callers may request any known provisional hash rather than an
// allowlisted subset.
func (s *Server) requireLiveHashes(w http.ResponseWriter, name string, view LiveHead, selected *entry, rawVHs []string) bool {
	if !view.RequireVersionedHashes || selected.kind != UnfinalizedMutable || len(rawVHs) != 0 {
		return true
	}
	w.Header().Set("Cache-Control", "no-store")
	writeError(w, http.StatusBadRequest,
		"virtual head %q requires versioned_hashes when reading a provisional slot", name)
	return false
}

func (s *Server) serveLiveUnavailable(w http.ResponseWriter, name string, slot uint64) {
	w.Header().Set("Retry-After", retryAfterSeconds)
	w.Header().Set("Cache-Control", "no-store")
	writeError(w, http.StatusServiceUnavailable,
		"virtual head %q has no coherent generation covering slot %d; retry", name, slot)
}

// resolveMetadata maps a virtual metadata request to its finalized physical
// head. Genesis and chain constants never come from the provisional view.
func (s *Server) resolveMetadata(w http.ResponseWriter, r *http.Request) (*entry, bool) {
	name := r.PathValue("head")
	view, virtual := s.cfg.LiveHeads[name]
	if !virtual {
		return s.resolve(w, r)
	}
	finalized, _ := s.cfg.Heads.liveEntries(view)
	if finalized == nil || finalized.durable == nil || finalized.quarantine != "" ||
		finalized.kind != FinalizedMonotonic || finalized.durable.EffectiveKind() != FinalizedMonotonic {
		w.Header().Set("Cache-Control", "no-store")
		writeError(w, http.StatusServiceUnavailable,
			"virtual head %q has no usable finalized metadata source", name)
		return nil, false
	}
	w.Header().Set(finalityHeader, finalityFinalized)
	return finalized, true
}

// handleBlobs serves GET /{head}/eth/v1/beacon/blobs/{slot} (spec 7.1).
func (s *Server) handleBlobs(w http.ResponseWriter, r *http.Request) {
	// The one public endpoint whose body runs to megabytes, so the one that gets a
	// write deadline: a slow reader must not be able to hold this
	// handler and the admission reservation it takes below open
	// indefinitely. Set before any write, so every response path here is bounded.
	if err := setWriteDeadline(w, s.cfg.BlobResponseWriteTimeout); err != nil {
		s.log.Warn("setting blobs response write deadline", "err", err)
	}
	name := r.PathValue("head")
	view, virtual := s.cfg.LiveHeads[name]
	rawVHs := r.URL.Query()["versioned_hashes"]
	if !virtual {
		// Preserve the cheap unknown/quarantined-head rejection before parsing or
		// response-memory admission. This is validation only: the returned entry
		// is deliberately discarded and the snapshot used by the request is
		// resolved again under its reader lease below.
		if _, ok := s.resolve(w, r); !ok {
			return
		}
	}

	rawSlot := r.PathValue("slot")
	slot, err := strconv.ParseUint(rawSlot, 10, 64)
	if err != nil {
		// A real beacon node also takes "head", "finalized", "genesis" and
		// block roots here. An archive has no view of any of them: it indexes
		// slots, and the named ids are all statements about a chain's current
		// opinion of itself. Nitro only ever sends a decimal slot.
		writeError(w, http.StatusBadRequest,
			"invalid slot %q: this endpoint takes a decimal slot number; named block ids are not supported", rawSlot)
		return
	}
	if virtual {
		// As above, reject an unavailable live handoff before reserving response
		// memory, but do not retain or use this entry. The authoritative handoff
		// selection is repeated under the lease after admission.
		if _, ok := s.resolveLiveSlot(w, name, view, slot); !ok {
			return
		}
	}
	// The count cap of the safety boundary, before any parse or lookup: duplicates and
	// comma-separated entries are counted, because the amplification is one blob
	// read per decoded array entry. Spec 7.1 keeps duplicates below the cap fully
	// legal -- order and multiplicity are preserved in the answer -- so this
	// refuses only a request naming more entries than any slot could hold.
	queryVHCount := versionedHashQueryCount(rawVHs)
	if queryVHCount > s.cfg.MaxQueryHashes {
		writeError(w, http.StatusBadRequest,
			"too many versioned_hashes: %d requested, at most %d allowed per request", queryVHCount, s.cfg.MaxQueryHashes)
		return
	}
	vhs, err := parseVersionedHashQuery(rawVHs)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid versioned_hashes: %v", err)
		return
	}

	// Admission before any blob lookup, read, or allocation. A
	// filtered request reads exactly len(vhs) blobs; an unfiltered one reads what
	// the slot holds, unknown until the lookup and bounded by the stored-row
	// ceiling, so it reserves the worst case. The reservation is held until this
	// handler returns -- through the response write -- and released then.
	raw := acceptsOctetStream(r)
	reserveEntries := len(vhs)
	if reserveEntries == 0 {
		reserveEntries = schema.MaxBlobsPerSlotCeiling
	}
	weight, err := s.admission.reserve(r.Context(), reserveEntries, raw)
	if err != nil {
		// The request context ended while this request waited for budget: the
		// client went away, or a deadline fired. Nothing was reserved. 503
		// no-store says "retry" to a client still listening and is invisible to
		// one that is not.
		w.Header().Set("Cache-Control", "no-store")
		writeError(w, http.StatusServiceUnavailable, "server is at its response-memory budget; retry")
		return
	}
	defer s.admission.release(weight)

	// Admission may wait for another response to release memory. Do not hold a
	// GC reader lease across that wait: no root has been selected and no archive
	// block is needed yet. Once admitted, take the shared Gate read side before
	// resolving the physical (or virtual) head and keep it through index lookup,
	// follower fetches, integrity verification, and response rendering. The
	// wrapped writer releases immediately before the first network write; the
	// defer covers cancellation and any future exit which writes nothing.
	w, lease := s.leaseResponseWriter(w)
	defer lease.Release()

	var e *entry
	if virtual {
		var ok bool
		e, ok = s.resolveLiveSlot(w, name, view, slot)
		if !ok {
			return
		}
		if !s.requireLiveHashes(w, name, view, e, rawVHs) {
			return
		}
	} else {
		var ok bool
		e, ok = s.resolve(w, r)
		if !ok {
			return
		}
	}
	head := e.head

	var res archive.Result
	if len(vhs) > 0 {
		res, err = head.LookupVHs(r.Context(), slot, vhs)
	} else {
		res, err = head.Lookup(r.Context(), slot)
	}
	if err != nil {
		if unavailable(err) {
			// A follower walking an index it has not finished fetching (spec
			// 11.3): the block exists, this node has not got it yet, and the
			// walk is the fetch. Retryable, and the pin policy makes it rare --
			// index blocks are fetched eagerly under every policy, so this is
			// the window between adopting a root and finishing its backfill.
			s.log.Info("index block not available", "head", head.Params().Name, "slot", slot, "err", err)
			s.serveNotYet(w, slot)
			return
		}
		if errors.Is(err, store.ErrCorruptBlock) {
			// A dag-cbor index node on the path to this slot failed CID validation
			//: the spine the lookup walked is corrupt. Same 500 and
			// same counter as a corrupt blob leaf -- an operator sizing corruption
			// wants both under one metric -- and the same non-transient cause, so
			// not the 503 a slow follower fetch gets.
			s.log.Error("index block failed CID validation", "head", head.Params().Name, "slot", slot, "err", err)
			s.cfg.Metrics.CorruptRead(head.Params().Name)
			writeError(w, http.StatusInternalServerError, "index for slot %d failed integrity validation", slot)
			return
		}
		s.log.Error("blob lookup failed", "head", head.Params().Name, "slot", slot, "err", err)
		writeError(w, http.StatusInternalServerError, "lookup failed")
		return
	}

	switch res.Status {
	case archive.StatusFound:
		s.serveBlobs(r.Context(), w, e, res, raw)

	case archive.StatusBeforeOrigin:
		s.setCoverageCache(w, e, slot)
		writeError(w, http.StatusNotFound, "slot %d precedes origin_slot %d: head %q is defined never to cover it",
			slot, head.Params().OriginSlot, head.Params().Name)

	case archive.StatusAbsent:
		s.setCoverageCache(w, e, slot)
		// The first missing vh, not all of them: spec 7.1 asks for one, and the
		// engine stops at the first because a caller that asked for N blobs
		// cannot use N-1 anyway.
		missing := "a requested blob"
		if res.MissingVH != nil {
			missing = vhHex(*res.MissingVH)
		}
		writeError(w, http.StatusNotFound, "slot %d does not carry %s", slot, missing)

	case archive.StatusNotYetCovered:
		// The one retryable answer, and the reason the engine distinguishes it
		// from absence at all: this slot is coming, the indexer just has not
		// reached it.
		s.serveNotYet(w, slot)

	default:
		s.log.Error("blob lookup returned an unknown status", "head", head.Params().Name, "slot", slot, "status", res.Status)
		writeError(w, http.StatusInternalServerError, "lookup failed")
	}
}

// serveBlobs answers a covered slot with its blob bytes, read through the
// head's Blobs by the entries the index resolved to. raw selects spec 7.1's
// application/octet-stream variant over the JSON default.
//
// The lookup, the Blobs seam, and every failure mapping are the same for both
// variants: only the final rendering differs, so the blobs are read whole
// before either body is written, which also keeps the error paths able to set
// their own status before anything is on the wire.
func (s *Server) serveBlobs(ctx context.Context, w http.ResponseWriter, e *entry, res archive.Result, raw bool) {
	head := e.head
	name := head.Params().Name
	blobs := e.blobs
	if blobs == nil {
		blobs = blockstoreBlobs{blocks: s.cfg.Blocks}
	}

	raws := make([][]byte, 0, len(res.Entries))
	for _, ref := range res.Entries {
		b, err := blobs.Blob(ctx, ref)
		switch {
		case err == nil:
		case errors.Is(err, store.ErrCorruptBlock):
			// The index resolved and the block is present, but its stored bytes no
			// longer hash to the CID they were requested under. This
			// is local corruption, distinct from both a missing blob and a failed
			// fetch, and mapped to neither of their statuses: not the 404 that would
			// deny the blob exists, and not the 503 that invites a retry, because the
			// same request will fail the same way until an operator repairs the block
			// (bloard fsck). 500, counted, and logged at error level -- a corrupt
			// block a live root references is not transient.
			s.log.Error("blob block failed CID validation",
				"head", name, "slot", res.Slot, "cid", ref.Blob, "vh", vhHex(ref.VH), "err", err)
			s.cfg.Metrics.CorruptRead(name)
			writeError(w, http.StatusInternalServerError, "blob %s failed integrity validation", vhHex(ref.VH))
			return
		case format.IsNotFound(err):
			// The index resolved but the block is gone. On a writer that means
			// GC or an operator took a block a live root still references,
			// which is why it is logged at error level: the response is a
			// retry, but the cause is not transient. A follower never gets here
			// -- its Blobs fetches a block it does not have rather than
			// reporting it missing (spec 11.4) -- so this stays what it was.
			s.log.Error("blob block referenced by the index is not in the blockstore",
				"head", name, "slot", res.Slot, "cid", ref.Blob, "vh", vhHex(ref.VH))
			s.serveNotYet(w, res.Slot)
			return
		case unavailable(err):
			// Spec 11.4's read miss: a follower's on-demand fetch that did not
			// land inside follow.fetch_timeout, or a blob it refuses to serve.
			// The index says this blob exists, so the honest answer is "not
			// from me, not right now" -- 503 and no-store, never the 404 that
			// would tell a client the blob does not exist.
			s.log.Warn("blob is not available to serve",
				"head", name, "slot", res.Slot, "cid", ref.Blob, "vh", vhHex(ref.VH), "err", err)
			s.serveNotYet(w, res.Slot)
			return
		default:
			s.log.Error("reading blob block", "head", name, "slot", res.Slot, "cid", ref.Blob, "err", err)
			writeError(w, http.StatusInternalServerError, "reading blob %s failed", vhHex(ref.VH))
			return
		}
		raws = append(raws, b)
	}

	s.setCoverageCache(w, e, res.Slot)
	if raw {
		// Self-framed by the fixed blob size; a blobless covered slot is an
		// empty body, which bytes.Join yields for an empty slice. This is the
		// path that removes the hex-encode entirely (spec 7.1).
		writeOctetStream(w, http.StatusOK, bytes.Join(raws, nil))
		return
	}
	// An empty covered slot renders {"data":[]}; renderBlobsJSON writes that for
	// an empty raws, so there is no nil-vs-empty slice hazard to guard.
	writeRaw(w, http.StatusOK, renderBlobsJSON(raws))
}

// serveNotYet answers a slot the head has not reached, or one whose blob it
// cannot produce right now (spec 7.1, 11.4).
func (s *Server) serveNotYet(w http.ResponseWriter, slot uint64) {
	w.Header().Set("Retry-After", retryAfterSeconds)
	// Emphatically not cacheable: the answer is "ask again", and a cached
	// "ask again" is a client that never gets the blob.
	w.Header().Set("Cache-Control", "no-store")
	writeError(w, http.StatusServiceUnavailable, "slot %d is not archived yet", slot)
}

// setCoverageCache sets the caching headers of spec 7.1 for a 200 or 404 about
// slot.
//
// The horizon rule is applied to every such answer, including a 404 for a slot
// below origin_slot, which is permanent in a way coverage is not. Two rules
// where one will do is two rules to get wrong, and the immutable window is
// wide: by the time a head has any coverage at all, origin_slot is normally
// far behind the horizon and both rules agree. Where they disagree -- a young
// head -- the uniform rule is the conservative one.
func (s *Server) setCoverageCache(w http.ResponseWriter, e *entry, slot uint64) {
	if e.kind == UnfinalizedMutable {
		// A bounded optimistic generation is replaceable. Both blob presence and
		// authenticated absence may change at the same slot after a reorg, so no
		// data answer from the physical mutable head is cacheable.
		w.Header().Set("Cache-Control", "no-store")
		return
	}
	var syncedTo uint64
	covered := e.durable != nil && e.durable.SyncedTo != nil
	if covered {
		syncedTo = *e.durable.SyncedTo
	}
	if covered && syncedTo >= s.cfg.ImmutableHorizonSlots && slot <= syncedTo-s.cfg.ImmutableHorizonSlots {
		// A year, immutable: this answer is a fact about finalized history that
		// only a truncation could disturb, and truncation this far back is an
		// emergency, not an operation.
		w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
		return
	}
	w.Header().Set("Cache-Control", "public, max-age=60")
}

// handleGenesis serves GET /{head}/eth/v1/beacon/genesis (spec 7.1). The body
// is static; the head in the path is honoured only so that an unknown one 404s
// as it does everywhere else.
func (s *Server) handleGenesis(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.resolveMetadata(w, r); !ok {
		return
	}
	writeRaw(w, http.StatusOK, s.genesis)
}

// handleSpec serves GET /{head}/eth/v1/config/spec (spec 7.1).
func (s *Server) handleSpec(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.resolveMetadata(w, r); !ok {
		return
	}
	writeRaw(w, http.StatusOK, s.spec)
}
