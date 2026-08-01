package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/ipfs/go-cid"

	"github.com/blobarchive/bloar/archive"
	"github.com/blobarchive/bloar/ingest"
	"github.com/blobarchive/bloar/schema"
	"github.com/blobarchive/bloar/store"
)

// maxRefsBody bounds a refs or truncate body. Neither is large: a batch of 512
// slots carrying the mid-2026 maximum of 21 blobs each renders to well under a
// megabyte of hex. This is a sanity bound on an authenticated endpoint, not a
// policy.
const maxRefsBody = 16 << 20

// beginMutationBody installs the read-deadline extension a VALID mutation gets
// before it reads its body. It is called only after auth, the
// head-existence check, and any framing check have already passed, so the
// daemon's short server-level ReadTimeout still bounds every rejected path -- an
// auth-rejected, unknown-head, or over-length request never reaches here and so
// never earns the extension. Past those gates the body is a legitimate upload
// bounded by MaxPutBlobs or maxRefsBody, and this gives it the wall-clock room to
// arrive over a slow-but-honest link.
func (s *Server) beginMutationBody(w http.ResponseWriter) {
	if err := extendReadDeadline(w, s.cfg.MutationBodyReadTimeout); err != nil {
		s.log.Warn("extending mutation body read deadline", "err", err)
	}
}

// beginBoundedMutation is beginMutationBody with the declared-length framing check
// in front of it. A request whose declared
// Content-Length already exceeds the endpoint's byte ceiling is invalid and must
// stay under the short server-level ReadTimeout -- it must NOT earn the long upload
// extension -- so the reject happens BEFORE the extension. MaxBytesReader inside
// decodeJSON stays the real enforcement, for a chunked body or one that lies about
// its length. Returns false, having written the 400, when the declared length is
// already too large; the JSON mutation handlers use it in place of a bare
// beginMutationBody. handlePutBlobs does its own declared-length checks (a whole
// number of blobs) inline and calls beginMutationBody directly afterward.
func (s *Server) beginBoundedMutation(w http.ResponseWriter, r *http.Request, maxBody int64) bool {
	if r.ContentLength > maxBody {
		writeError(w, http.StatusBadRequest,
			"body declares %d bytes; at most %d are accepted for this request", r.ContentLength, maxBody)
		return false
	}
	s.beginMutationBody(w)
	return true
}

// handleHeads serves GET /bloar/v1/heads: the publication document (spec 7.2,
// 8).
func (s *Server) handleHeads(w http.ResponseWriter, r *http.Request) {
	// 12 seconds: one slot. The document changes at most as often as the
	// indexer advances a head, and a follower polling it (spec 11.3) is not
	// harmed by a slot of staleness.
	w.Header().Set("Cache-Control", "public, max-age=12")
	writeRaw(w, http.StatusOK, s.cfg.Heads.Doc())
}

// handleHead serves GET /bloar/v1/heads/{head}: one entry of the same document
// (spec 7.2).
func (s *Server) handleHead(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("head")
	entry, ok := s.cfg.Heads.HeadDoc(name)
	if !ok {
		writeError(w, http.StatusNotFound, "unknown head %q", name)
		return
	}
	w.Header().Set("Cache-Control", "public, max-age=12")
	writeRaw(w, http.StatusOK, entry)
}

// syncedToResponse is the body of GET /bloar/v1/heads/{head}/synced_to. The
// pointer is the point: null is an empty head, and an indexer reads it as "start
// at origin_slot" rather than "start at slot 0" (spec 10).
type syncedToResponse struct {
	SyncedTo *uint64 `json:"synced_to"`
}

// handleSyncedTo serves GET /bloar/v1/heads/{head}/synced_to (spec 7.2). It is
// the whole of an indexer's progress state: both indexers are stateless and
// resume from this (spec 10).
func (s *Server) handleSyncedTo(w http.ResponseWriter, r *http.Request) {
	head, ok := s.cfg.Heads.Get(r.PathValue("head"))
	if !ok {
		writeError(w, http.StatusNotFound, "unknown head %q", r.PathValue("head"))
		return
	}
	var body syncedToResponse
	if slot, covered := head.SyncedTo(); covered {
		body.SyncedTo = &slot
	}
	writeJSON(w, http.StatusOK, body)
}

// GenerationErrorResponse is the stable 409 body archclient consumes from the
// generation endpoint. CurrentGeneration is always present, including zero.
// MissingBlobs is present only when the complete snapshot references blobs not
// yet admitted by POST /bloar/v1/blobs.
type GenerationErrorResponse struct {
	Code              int      `json:"code"`
	Message           string   `json:"message"`
	MissingBlobs      []string `json:"missing_blobs,omitempty"`
	CurrentGeneration *uint64  `json:"current_generation"`
}

// generationRequestJSON preserves required-field presence at the HTTP decode
// boundary while GenerationRequest remains ergonomic for archclient to marshal.
type generationRequestJSON struct {
	ExpectedGeneration      *uint64              `json:"expected_generation"`
	WindowStart             *uint64              `json:"window_start"`
	SyncedTo                *uint64              `json:"synced_to"`
	Rows                    *[]generationRowJSON `json:"rows"`
	SourceHeadRoot          *string              `json:"source_head_root"`
	SourceFinalizedSlot     *uint64              `json:"source_finalized_slot"`
	SourceFinalizedRoot     *string              `json:"source_finalized_root"`
	ObservedHandoffRoot     *string              `json:"observed_handoff_root"`
	ObservedHandoffSyncedTo *uint64              `json:"observed_handoff_synced_to"`
}

type generationRowJSON struct {
	Slot            *uint64   `json:"slot"`
	VersionedHashes *[]string `json:"versioned_hashes"`
}

// handleGetGeneration serves the durable local CAS state. It is no-store: a
// stateless writer reads this precisely to recover from an ambiguous POST.
func (s *Server) handleGetGeneration(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("head")
	state, err := s.cfg.Heads.GenerationStatus(r.Context(), name)
	if err != nil {
		if errors.Is(err, ErrUnknownHead) {
			writeError(w, http.StatusNotFound, "unknown mutable head %q", name)
			return
		}
		s.log.Error("reading mutable generation state", "head", name, "err", err)
		writeError(w, http.StatusInternalServerError, "reading mutable generation state")
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusOK, state)
}

// handleGeneration serves the authenticated complete-snapshot replacement.
func (s *Server) handleGeneration(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("head")
	// Resolve the configured mutable name before extending the upload deadline.
	// Unknown paths retain the daemon's short rejection bound.
	if _, err := s.cfg.Heads.GenerationState(r.Context(), name); err != nil {
		if errors.Is(err, ErrUnknownHead) {
			writeError(w, http.StatusNotFound, "unknown mutable head %q", name)
			return
		}
		s.log.Error("reading mutable generation state", "head", name, "err", err)
		writeError(w, http.StatusInternalServerError, "reading mutable generation state")
		return
	}
	if !s.beginBoundedMutation(w, r, maxRefsBody) {
		return
	}
	var wire generationRequestJSON
	if err := decodeJSON(w, r, &wire); err != nil {
		writeError(w, http.StatusBadRequest, "%v", err)
		return
	}
	switch {
	case wire.ExpectedGeneration == nil:
		writeError(w, http.StatusBadRequest, "expected_generation is required")
		return
	case wire.WindowStart == nil:
		writeError(w, http.StatusBadRequest, "window_start is required")
		return
	case wire.SyncedTo == nil:
		writeError(w, http.StatusBadRequest, "synced_to is required")
		return
	case wire.Rows == nil:
		writeError(w, http.StatusBadRequest, "rows is required; send [] for a covered window with no blobs")
		return
	case wire.SourceHeadRoot == nil:
		writeError(w, http.StatusBadRequest, "source_head_root is required")
		return
	case wire.SourceFinalizedSlot == nil:
		writeError(w, http.StatusBadRequest, "source_finalized_slot is required")
		return
	case wire.SourceFinalizedRoot == nil:
		writeError(w, http.StatusBadRequest, "source_finalized_root is required")
		return
	case wire.ObservedHandoffRoot == nil:
		writeError(w, http.StatusBadRequest, "observed_handoff_root is required")
		return
	case wire.ObservedHandoffSyncedTo == nil:
		writeError(w, http.StatusBadRequest, "observed_handoff_synced_to is required")
		return
	}
	rows := make([]GenerationRow, 0, len(*wire.Rows))
	for i, row := range *wire.Rows {
		if row.Slot == nil {
			writeError(w, http.StatusBadRequest, "row %d: slot is required", i)
			return
		}
		if row.VersionedHashes == nil {
			writeError(w, http.StatusBadRequest, "row %d (slot %d): versioned_hashes is required", i, *row.Slot)
			return
		}
		rows = append(rows, GenerationRow{Slot: *row.Slot, VersionedHashes: *row.VersionedHashes})
	}
	req := GenerationRequest{
		ExpectedGeneration:      *wire.ExpectedGeneration,
		WindowStart:             *wire.WindowStart,
		SyncedTo:                *wire.SyncedTo,
		Rows:                    rows,
		SourceHeadRoot:          *wire.SourceHeadRoot,
		SourceFinalizedSlot:     *wire.SourceFinalizedSlot,
		SourceFinalizedRoot:     *wire.SourceFinalizedRoot,
		ObservedHandoffRoot:     *wire.ObservedHandoffRoot,
		ObservedHandoffSyncedTo: *wire.ObservedHandoffSyncedTo,
	}
	result, err := s.cfg.Heads.ReplaceGeneration(r.Context(), name, req)
	if err != nil {
		s.writeGenerationError(w, name, err)
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) writeGenerationError(w http.ResponseWriter, name string, err error) {
	if errors.Is(err, ErrUnknownHead) {
		writeError(w, http.StatusNotFound, "unknown mutable head %q", name)
		return
	}
	if errors.Is(err, ErrFollowedHead) {
		writeError(w, http.StatusForbidden, "head %q is followed by this node, not written by it", name)
		return
	}
	var invalid *GenerationValidationError
	if errors.As(err, &invalid) {
		writeError(w, http.StatusBadRequest, "%v", invalid)
		return
	}
	current := uint64(0)
	if state, stateErr := s.cfg.Heads.GenerationState(context.Background(), name); stateErr == nil {
		current = state.Generation
	}
	var genConflict *GenerationConflictError
	var archiveConflict *archive.ConflictError
	if errors.As(err, &genConflict) || errors.As(err, &archiveConflict) {
		if genConflict != nil {
			current = genConflict.CurrentGeneration
		}
		body := GenerationErrorResponse{Code: http.StatusConflict, Message: err.Error(), CurrentGeneration: &current}
		var missing *archive.MissingBlobsError
		if errors.As(err, &missing) {
			body.MissingBlobs = make([]string, 0, len(missing.VHs))
			for _, vh := range missing.VHs {
				body.MissingBlobs = append(body.MissingBlobs, vhHex(vh))
			}
		}
		writeJSON(w, http.StatusConflict, body)
		return
	}
	s.log.Error("mutable generation failed", "head", name, "err", err)
	writeError(w, http.StatusInternalServerError, "mutable generation failed")
}

// putBlobsResponse is the body of a 200 from POST /bloar/v1/blobs, in body
// order (spec 7.2).
type putBlobsResponse struct {
	Blobs []putBlobEntry `json:"blobs"`
}

type putBlobEntry struct {
	VersionedHash string `json:"versioned_hash"`
	CID           string `json:"cid"`
}

// handlePutBlobs serves POST /bloar/v1/blobs (spec 7.2, auth).
func (s *Server) handlePutBlobs(w http.ResponseWriter, r *http.Request) {
	maxBytes := int64(s.cfg.MaxPutBlobs) * schema.BlobSize

	// The count limit is checked before the body is read, twice over, because
	// checking it afterwards would mean having already buffered whatever was
	// sent -- and the limit exists to bound exactly that. Content-Length is the
	// cheap check and a lie is free; MaxBytesReader is the one that holds,
	// including for a chunked body that declares no length at all.
	// 400 rather than 413 for an over-long body: spec 7.2 names the status, and
	// it names 400 for both of this endpoint's framing failures.
	if r.ContentLength > maxBytes {
		writeError(w, http.StatusBadRequest,
			"body declares %d bytes; at most %d blobs (%d bytes) per request", r.ContentLength, s.cfg.MaxPutBlobs, maxBytes)
		return
	}
	if r.ContentLength > 0 && r.ContentLength%schema.BlobSize != 0 {
		writeError(w, http.StatusBadRequest,
			"body declares %d bytes, which is not a whole number of %d-byte blobs", r.ContentLength, schema.BlobSize)
		return
	}

	// Past the framing checks this is a valid upload: give its bounded body the
	// read-deadline room to arrive before the first read of it.
	s.beginMutationBody(w)
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBytes))
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeError(w, http.StatusBadRequest,
				"body carries more than the %d blobs (%d bytes) allowed per request", s.cfg.MaxPutBlobs, maxBytes)
			return
		}
		writeError(w, http.StatusBadRequest, "reading body: %v", err)
		return
	}

	put, err := s.cfg.Ingester.PutBlobs(r.Context(), body)
	if err != nil {
		var invalid *ingest.ValidationError
		if errors.As(err, &invalid) {
			// The message names the offending index: a caller re-posting a
			// corrected body needs to know which blob was refused, and nothing
			// else in the response can tell it.
			writeError(w, http.StatusBadRequest, "%v", invalid)
			return
		}
		s.log.Error("put blobs failed", "err", err)
		writeError(w, http.StatusInternalServerError, "storing blobs failed")
		return
	}

	resp := putBlobsResponse{Blobs: make([]putBlobEntry, 0, len(put))}
	for _, p := range put {
		resp.Blobs = append(resp.Blobs, putBlobEntry{VersionedHash: vhHex(p.VH), CID: p.CID.String()})
	}
	writeJSON(w, http.StatusOK, resp)
}

// refsRequest is the body of POST /bloar/v1/heads/{head}/refs (spec 7.2).
type refsRequest struct {
	// A pointer so that an OMITTED rows is refused rather than read as an empty
	// batch. Empty is not the same as absent here: an explicitly
	// present [] is the protocol's legitimate coverage-only batch (advance over
	// blobless slots), while a nil rows is a caller who forgot the field -- or
	// misspelled it, which DisallowUnknownFields turns into its own 400 -- and
	// committing that as covered-empty would freeze the batch's intended blobs as
	// durable false absence. handleRefs rejects nil outright.
	Rows *[]refsRow `json:"rows"`
	// A pointer so that a body which omits it is refused rather than read as
	// "synced_to: 0", which would be a conflict with a confusing message.
	SyncedTo *uint64 `json:"synced_to"`
	// ExpectedManifest binds the batch to the manifest tip it was scanned under
	//. Required for a head with a manifest chain, and
	// forbidden BY PRESENCE for a chainless one; the head-state half of that rule
	// is enforced under the head lock (ApplyRefs), where the tip is known
	// atomically with the commit. The type is presence-aware because the forbidden
	// side is about presence: an absent field, an explicit "", and an explicit null
	// are three distinct states, which a plain string (all collapse to "") and a
	// *string (absent and null collapse to nil) cannot tell apart. handleRefs
	// rejects a present-but-empty value outright, so a chainless head cannot be
	// slipped an empty binding that reads as absence.
	ExpectedManifest optionalString `json:"expected_manifest"`
}

// optionalString distinguishes a JSON string field's three inhabited states --
// absent, present-and-null, present-and-set -- for a field whose contract turns on
// presence. It carries refsRequest.ExpectedManifest, the
// manifest's Prev on both the request (manifestReqJSON, where an omitted key is a
// mistake and an explicit null is genesis -- the safety boundary) and the response
// (manifestJSON), and a manifest source's Topic. encoding/json invokes
// UnmarshalJSON only when the key is present, including for an explicit null, so an
// untouched zero value is exactly "absent".
type optionalString struct {
	Present bool   // the key appeared in the object at all
	Null    bool   // it appeared as a JSON null
	Value   string // the decoded string, when Present && !Null
}

func (o *optionalString) UnmarshalJSON(b []byte) error {
	o.Present = true
	if string(b) == "null" {
		o.Null = true
		return nil
	}
	return json.Unmarshal(b, &o.Value)
}

// MarshalJSON renders the field back to its wire form, so a presence-aware type
// used to DECODE a request body can also appear in a RESPONSE body -- namely
// manifestJSON.Prev, which the GET manifest endpoint returns. It is the value-form
// inverse of UnmarshalJSON: a null when the field is null or was never set (so the
// genesis prev renders as the spec's null), the quoted string otherwise. A value
// receiver so it is called on the non-addressable nested value writeJSON marshals.
func (o optionalString) MarshalJSON() ([]byte, error) {
	if !o.Present || o.Null {
		return []byte("null"), nil
	}
	return json.Marshal(o.Value)
}

// optionalUint64 is optionalString's number sibling: the three inhabited states of
// a JSON number field whose contract turns on presence. It carries manifest
// sources' from_block (required, but an explicit 0 is a valid schedule start, so
// absent must be told apart from zero -- validation rules) and until_block (absent is
// open-ended, an explicit null is rejected -- validation rules). Decode only; these
// fields never appear in a response body (that is manifestSourceJSON's job).
type optionalUint64 struct {
	Present bool
	Null    bool
	Value   uint64
}

func (o *optionalUint64) UnmarshalJSON(b []byte) error {
	o.Present = true
	if string(b) == "null" {
		o.Null = true
		return nil
	}
	return json.Unmarshal(b, &o.Value)
}

// optionalStrings is the same for a JSON array of strings: it records that the key
// appeared -- even as null -- so the manifest decoder can reject a senders array
// PRESENT on a source type that forbids it, a presence a plain nil slice (which
// absent and null both produce) cannot report. Decode only.
type optionalStrings struct {
	Present bool
	Null    bool
	Value   []string
}

func (o *optionalStrings) UnmarshalJSON(b []byte) error {
	o.Present = true
	if string(b) == "null" {
		o.Null = true
		return nil
	}
	return json.Unmarshal(b, &o.Value)
}

// refsRow is one slot's entry in a refs batch. Both fields are pointers so a
// missing or null member is a named 400 at the decode boundary rather than a
// silent zero: an omitted slot must not default to slot 0 (a real slot when
// origin_slot is 0), and an omitted or nulled versioned_hashes must not become an
// empty row that only a later archive rule would reject. A row states which slot
// carries which blobs; neither half is optional.
type refsRow struct {
	Slot            *uint64   `json:"slot"`
	VersionedHashes *[]string `json:"versioned_hashes"`
}

// refsResponse is the body of a 200 from the refs endpoint.
type refsResponse struct {
	SyncedTo uint64 `json:"synced_to"`
	Root     string `json:"root"`
	NoOp     bool   `json:"noop"`
}

// handleRefs serves POST /bloar/v1/heads/{head}/refs (spec 7.2, auth).
func (s *Server) handleRefs(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("head")
	if _, ok := s.cfg.Heads.Get(name); !ok {
		writeError(w, http.StatusNotFound, "unknown head %q", name)
		return
	}

	// The head exists, so this is a valid mutation: reject a declared-oversize body
	// (which stays under the short server bound), then extend the read deadline
	// before decodeJSON reads it. An unknown head above never
	// reaches here and stays under the server-level ReadTimeout.
	if !s.beginBoundedMutation(w, r, maxRefsBody) {
		return
	}
	var req refsRequest
	if err := decodeJSON(w, r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "%v", err)
		return
	}
	if req.SyncedTo == nil {
		writeError(w, http.StatusBadRequest, "synced_to is required")
		return
	}
	// Absent rows is not empty rows. An explicitly present [] is the
	// legitimate coverage-only batch; a nil rows -- omitted or nulled -- is a
	// malformed request, refused here before any coverage advances rather than
	// committed as false absence.
	if req.Rows == nil {
		writeError(w, http.StatusBadRequest,
			"rows is required; send [] to advance coverage over slots that carry no blobs")
		return
	}
	// An absent field is "no expected_manifest" -- cid.Undef -- and ApplyRefs
	// decides forbidden-vs-required from the head's tip under the lock. A field that
	// is PRESENT yet empty ("" or null) is neither absent nor a valid binding: a
	// chainless head forbids the field by presence, and a chained head requires a
	// real CID, so it is a 400 here, before the lock. A present, well-formed value
	// that is merely stale is left to the 409 path inside ApplyRefs.
	var expectedManifest cid.Cid
	switch {
	case !req.ExpectedManifest.Present:
	case req.ExpectedManifest.Null || req.ExpectedManifest.Value == "":
		what := "an empty string"
		if req.ExpectedManifest.Null {
			what = "null"
		}
		writeError(w, http.StatusBadRequest, "expected_manifest is present but %s; a head with a manifest chain "+
			"requires its tip CID and a chainless head forbids the field -- omit it rather than sending an empty value", what)
		return
	default:
		var err error
		if expectedManifest, err = cid.Decode(req.ExpectedManifest.Value); err != nil {
			writeError(w, http.StatusBadRequest, "expected_manifest %q is not a CID: %v", req.ExpectedManifest.Value, err)
			return
		}
	}

	rows := make([]archive.RefRow, 0, len(*req.Rows))
	for i, row := range *req.Rows {
		// A row's slot and hashes are both required and both distinguish absent from
		// zero: a missing slot must not silently mean slot 0, and a
		// missing or nulled versioned_hashes must not silently mean an empty row.
		if row.Slot == nil {
			writeError(w, http.StatusBadRequest, "row %d: slot is required", i)
			return
		}
		if row.VersionedHashes == nil {
			writeError(w, http.StatusBadRequest,
				"row %d (slot %d): versioned_hashes is required; a slot with no blobs has no row", i, *row.Slot)
			return
		}
		vhs, err := parseVHs(*row.VersionedHashes)
		if err != nil {
			writeError(w, http.StatusBadRequest, "row %d (slot %d): %v", i, *row.Slot, err)
			return
		}
		rows = append(rows, archive.RefRow{Slot: *row.Slot, VHs: vhs})
	}

	res, err := s.cfg.Heads.ApplyRefs(r.Context(), name, rows, *req.SyncedTo, expectedManifest)
	if err != nil {
		s.writeApplyError(w, name, err)
		return
	}
	writeJSON(w, http.StatusOK, refsResponse{SyncedTo: res.SyncedTo, Root: res.Root.String(), NoOp: res.NoOp})
}

// truncateRequest is the body of POST /bloar/v1/heads/{head}/truncate.
type truncateRequest struct {
	Slot *uint64 `json:"slot"`
	// Confirm must be the head's own name. Spec 7.2 only recommends it; it is
	// required here, because the operation this guards discards archived
	// coverage and the guard costs a word in a curl line.
	Confirm string `json:"confirm"`
}

// truncateResponse is the body of a 200 from the truncate endpoint.
type truncateResponse struct {
	SyncedTo uint64 `json:"synced_to"`
	Root     string `json:"root"`
}

// handleTruncate serves POST /bloar/v1/heads/{head}/truncate (spec 5.4, 7.2,
// auth).
func (s *Server) handleTruncate(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("head")
	if _, ok := s.cfg.Heads.Get(name); !ok {
		writeError(w, http.StatusNotFound, "unknown head %q", name)
		return
	}

	if !s.beginBoundedMutation(w, r, maxRefsBody) {
		return
	}
	var req truncateRequest
	if err := decodeJSON(w, r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "%v", err)
		return
	}
	if req.Slot == nil {
		writeError(w, http.StatusBadRequest, "slot is required")
		return
	}
	// Before anything else: a mismatched confirmation must not truncate, and
	// the only way to be sure of that is to not have done anything yet.
	if req.Confirm != name {
		writeError(w, http.StatusBadRequest,
			"confirm is %q, must be the head's own name %q; nothing was truncated", req.Confirm, name)
		return
	}

	root, err := s.cfg.Heads.Truncate(r.Context(), name, *req.Slot)
	if err != nil {
		s.writeApplyError(w, name, err)
		return
	}
	s.log.Warn("head truncated", "head", name, "synced_to", *req.Slot, "root", root)
	writeJSON(w, http.StatusOK, truncateResponse{SyncedTo: *req.Slot, Root: root.String()})
}

// manifestResponse is the body of a 200 from the manifest endpoint (spec 7.2).
type manifestResponse struct {
	Manifest string `json:"manifest"`
}

// manifestGetResponse is the body of GET /bloar/v1/heads/{head}/manifest: the
// decoded tip manifest and its CID. It is what an indexer or a reviewer reads the
// head's attested schedule from (spec 10.5, 11.5).
type manifestGetResponse struct {
	Manifest manifestJSON `json:"manifest"`
	CID      string       `json:"cid"`
}

// handleGetManifest serves GET /bloar/v1/heads/{head}/manifest (spec 7.2, 10.5).
// A head with no chain is a 404, distinct from an unknown head's 404: the head is
// here, it simply makes no attested claim about its filter.
func (s *Server) handleGetManifest(w http.ResponseWriter, r *http.Request) {
	// The manifest tip is a mutable selector over immutable blocks, just like a
	// head root. Keep the next GC cut behind tip selection, block read, CID
	// validation, and decode. The writer wrapper drops the lease before JSON is
	// sent, so a slow client cannot delay collection.
	w, lease := s.leaseResponseWriter(w)
	defer lease.Release()

	name := r.PathValue("head")
	if _, ok := s.cfg.Heads.Get(name); !ok {
		writeError(w, http.StatusNotFound, "unknown head %q", name)
		return
	}
	tip, ok := s.cfg.Heads.ManifestTip(name)
	if !ok {
		writeError(w, http.StatusNotFound, "head %q has no manifest chain", name)
		return
	}
	blk, err := s.cfg.Blocks.Get(r.Context(), tip)
	if err != nil {
		if errors.Is(err, store.ErrCorruptBlock) {
			// The manifest block is present but its stored bytes no longer hash to
			// the tip CID. Same 500 and same counter as a corrupt blob
			// or index node: an operator sizing corruption wants every corrupt read
			// under one metric, and this is one.
			s.log.Error("manifest tip block failed CID validation", "head", name, "tip", tip, "err", err)
			s.cfg.Metrics.CorruptRead(name)
			writeError(w, http.StatusInternalServerError, "manifest tip for head %q failed integrity validation", name)
			return
		}
		s.log.Error("reading manifest tip block", "head", name, "tip", tip, "err", err)
		writeError(w, http.StatusInternalServerError, "reading manifest tip")
		return
	}
	m, err := schema.DecodeManifest(blk.RawData())
	if err != nil {
		s.log.Error("decoding manifest tip block", "head", name, "tip", tip, "err", err)
		writeError(w, http.StatusInternalServerError, "decoding manifest tip")
		return
	}
	// The tip advances at most as often as an operator runs an upgrade, which is
	// rarely, and the answer is content-addressed by the cid it carries.
	w.Header().Set("Cache-Control", "public, max-age=12")
	writeJSON(w, http.StatusOK, manifestGetResponse{Manifest: manifestToJSON(m), CID: tip.String()})
}

// handleManifest serves POST /bloar/v1/heads/{head}/manifest (spec 7.2, 10.5,
// auth). It canonicalizes the body to a Manifest block and compare-and-swaps it
// onto the head's tip; it does no semantic schedule validation (that is the
// indexer's, spec 10.5).
func (s *Server) handleManifest(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("head")
	if _, ok := s.cfg.Heads.Get(name); !ok {
		writeError(w, http.StatusNotFound, "unknown head %q", name)
		return
	}

	if !s.beginBoundedMutation(w, r, maxRefsBody) {
		return
	}
	var req manifestRequest
	if err := decodeJSON(w, r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "%v", err)
		return
	}
	if req.Manifest == nil {
		writeError(w, http.StatusBadRequest, "manifest is required")
		return
	}
	// Confirm first, before anything is built: a mismatched confirmation must not
	// advance the chain, and the surest way is to have done nothing yet.
	if req.Confirm != name {
		writeError(w, http.StatusBadRequest,
			"confirm is %q, must be the head's own name %q; nothing was changed", req.Confirm, name)
		return
	}
	if req.Manifest.Head != name {
		writeError(w, http.StatusBadRequest,
			"manifest head is %q but the path names head %q", req.Manifest.Head, name)
		return
	}

	m, prev, err := req.Manifest.toSchema()
	if err != nil {
		writeError(w, http.StatusBadRequest, "%v", err)
		return
	}
	block, manifestCID, err := schema.EncodeManifest(m)
	if err != nil {
		// Every schema.EncodeManifest failure is a malformed manifest: an empty
		// blob-txs allowlist, a cross-type field, a bad byte width, an unknown
		// version (spec 10.5, 15).
		writeError(w, http.StatusBadRequest, "%v", err)
		return
	}
	// The generation binding is not optional: the
	// append-only preflight this POST follows is only meaningful bound to the root
	// it read, so a missing or unparseable expected_head_root is a 400. Checked
	// after the manifest itself so a malformed manifest still reports its own fault.
	if req.ExpectedHeadRoot == "" {
		writeError(w, http.StatusBadRequest, "expected_head_root is required; publish via the indexer's manifest "+
			"preflight (spec 10.5, `bloar-index publish-manifest`), which captures it")
		return
	}
	expectedHeadRoot, err := cid.Decode(req.ExpectedHeadRoot)
	if err != nil {
		writeError(w, http.StatusBadRequest, "expected_head_root %q is not a CID: %v", req.ExpectedHeadRoot, err)
		return
	}

	tip, err := s.cfg.Heads.SetManifest(r.Context(), name, block, manifestCID, prev, expectedHeadRoot)
	if err != nil {
		s.writeManifestError(w, name, err)
		return
	}
	s.log.Info("manifest advanced", "head", name, "tip", tip)
	writeJSON(w, http.StatusOK, manifestResponse{Manifest: tip.String()})
}

// writeManifestError maps a SetManifest failure onto spec 7.2's statuses.
func (s *Server) writeManifestError(w http.ResponseWriter, name string, err error) {
	switch {
	case errors.Is(err, ErrUnknownHead):
		writeError(w, http.StatusNotFound, "unknown head %q", name)
	case errors.Is(err, ErrFollowedHead):
		writeError(w, http.StatusForbidden,
			"head %q is followed by this node, not written by it: its manifest chain comes from the writer's "+
				"publication document (spec 11.3). Send this to the head's writer", name)
	case errors.Is(err, ErrMutableGenerationOnly):
		writeError(w, http.StatusBadRequest, "%v", err)
	case errors.Is(err, ErrManifestConflict):
		// The same 409 a refs or truncate conflict reports: the writer raced
		// another upgrade or is working from a stale tip.
		writeJSON(w, http.StatusConflict, errorBody{Code: http.StatusConflict, Message: err.Error()})
	default:
		var genConflict *ManifestGenerationConflict
		if errors.As(err, &genConflict) {
			// A refs commit advanced the head between the preflight and this POST:
			// a 409, like the prev CAS, and the writer re-runs the preflight.
			writeJSON(w, http.StatusConflict, errorBody{Code: http.StatusConflict, Message: err.Error()})
			return
		}
		s.log.Error("manifest failed", "head", name, "err", err)
		writeError(w, http.StatusInternalServerError, "advancing the manifest chain failed")
	}
}

// writeApplyError maps a mutation failure onto spec 7.2's statuses.
func (s *Server) writeApplyError(w http.ResponseWriter, name string, err error) {
	if errors.Is(err, ErrUnknownHead) {
		writeError(w, http.StatusNotFound, "unknown head %q", name)
		return
	}

	if errors.Is(err, ErrFollowedHead) {
		// 403, and none of the alternatives. The request is well-formed (not
		// 400), the head is right there and read-serving (not 404), no future
		// state of this node accepts it (not 409, which invites a retry), and
		// POST is the only method this path has ever had, so a 405 would owe an
		// Allow header it could not fill in. What is left is exactly what 403
		// means: understood, refused, and not because of who asked -- spec 11.1
		// gives this head one writer and no credential makes this node it.
		writeError(w, http.StatusForbidden,
			"head %q is followed by this node, not written by it: its root comes from the writer's publication "+
				"document (spec 11.3), and a mutation applied here would be replaced by the next poll. Send this to "+
				"the head's writer", name)
		return
	}

	if errors.Is(err, ErrManifestBindingRequired) || errors.Is(err, ErrManifestBindingForbidden) {
		// A malformed refs request: the expected_manifest field's presence does
		// not match whether the head has a manifest chain (spec 10.5). Not a race,
		// so not a 409 -- no future state of the head makes this body correct.
		writeError(w, http.StatusBadRequest, "%v", err)
		return
	}
	if errors.Is(err, ErrMutableGenerationOnly) {
		writeError(w, http.StatusBadRequest, "%v", err)
		return
	}

	var bindConflict *ManifestBindingConflict
	if errors.As(err, &bindConflict) {
		// The manifest chain advanced under a still-running writer (spec 10.5,
		// the safety boundary): a 409 carrying the current tip, so the writer can stop and
		// resync against it rather than keep committing across the handoff.
		writeJSON(w, http.StatusConflict, errorBody{
			Code: http.StatusConflict, Message: bindConflict.Error(), ManifestTip: bindConflict.Current.String(),
		})
		return
	}

	var conflict *archive.ConflictError
	if errors.As(err, &conflict) {
		body := errorBody{Code: http.StatusConflict, Message: conflict.Error()}
		// The two are tested for independently: every missing-blob failure is a
		// conflict, but most conflicts are not missing blobs, and only this one
		// has a list to hand back.
		var missing *archive.MissingBlobsError
		if errors.As(err, &missing) {
			body.MissingBlobs = make([]string, 0, len(missing.VHs))
			for _, vh := range missing.VHs {
				body.MissingBlobs = append(body.MissingBlobs, vhHex(vh))
			}
		}
		writeJSON(w, http.StatusConflict, body)
		return
	}

	s.log.Error("mutation failed", "head", name, "err", err)
	writeError(w, http.StatusInternalServerError, "mutation failed")
}

// decodeJSON reads exactly one bounded JSON value into v under the strict
// contract our own mutation bodies are held to. Two knobs, both
// turning a silent misparse into a named 400 before any mutation runs:
//
//   - DisallowUnknownFields: a key the caller misspelled -- the `rowz` typo of
//     the safety boundary -- is a hard error naming the offending field, not a silently dropped
//     value that leaves a required slice nil and reads as a valid coverage-only
//     batch. It applies at every nesting level, so an unknown key inside a row, a
//     manifest, or a manifest source is caught too.
//   - exactly one value, then EOF: a body carrying a trailing second JSON value --
//     a framing mistake or a smuggled second document -- is rejected, not accepted
//     with only its first value read and the rest ignored.
//
// This is deliberately for bodies WE define, whose one legitimate client (the
// archclient indexer) sends exactly these fields. It is NOT applied to upstream
// responses (index/upstream), which come from real beacon nodes that legitimately
// carry members we do not model; there the tool is presence-awareness of the
// specific coverage-bearing fields, not unknown-field rejection.
func decodeJSON(w http.ResponseWriter, r *http.Request, v any) error {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxRefsBody))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return fmt.Errorf("malformed request body: %w", err)
	}
	// A clean single value leaves the decoder at EOF. Anything else -- a second
	// object, or any trailing non-whitespace -- is a malformed body, not an
	// accepted no-op with its tail dropped.
	if err := dec.Decode(&json.RawMessage{}); err != io.EOF {
		return fmt.Errorf("malformed request body: unexpected data after the JSON value; send exactly one object")
	}
	return nil
}
