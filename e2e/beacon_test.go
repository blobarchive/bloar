package e2e

import (
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/ethereum/go-ethereum/crypto/kzg4844"

	"github.com/blobarchive/bloar/schema"
)

// fakeBeacon is a beacon node's block and blob endpoints, over a synthetic
// chain.
//
// It is the trusted block feed AND the blob source of index/beacon's anchored
// mode. The block half is complete and continuous -- every slot up to finality
// carries a block whose parent_root chains to the slot before it -- because a
// real node never prunes blocks: that is what lets the indexer take existence
// and blob commitments from it and treat the blob half (which a real node does
// prune) as untrusted bytes it anchors against those commitments. A slot with no
// blobs in the fixture is a present but blobless block (0 commitments), not a
// missed one; genuine missed slots and hidden blocks are exercised by the
// index/beacon unit tests, not here.
type fakeBeacon struct {
	http *httptest.Server
	url  string

	// finalized is the slot GET /eth/v1/beacon/headers/finalized reports. It is
	// atomic so a test can advance finality under a running indexer.
	finalized atomic.Uint64

	// slots is the synthetic chain: slot -> blobs, in block order. A slot missing
	// from this map is a present blobless block; its blobs endpoint 404s.
	slots map[uint64][][]byte

	// requests counts blob fetches, so a test can show the loop is not re-reading
	// what it already has.
	requests atomic.Int64
}

// newFakeBeacon starts a beacon node over slots.
func newFakeBeacon(t *testing.T, slots map[uint64][][]byte, finalized uint64) *fakeBeacon {
	t.Helper()
	b := &fakeBeacon{slots: slots}
	b.finalized.Store(finalized)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /eth/v1/beacon/headers/finalized", b.handleFinalized)
	mux.HandleFunc("GET /eth/v1/beacon/headers/{slot}", b.handleHeader)
	mux.HandleFunc("GET /eth/v1/beacon/blinded_blocks/{slot}", b.handleBlindedBlock)
	mux.HandleFunc("GET /eth/v1/beacon/blobs/{slot}", b.handleBlobs)

	b.http = httptest.NewServer(mux)
	t.Cleanup(b.http.Close)
	b.url = b.http.URL
	return b
}

// handleFinalized serves the finalized checkpoint header. Only the slot is
// real; a beacon node states it as a string, and so does this.
func (b *fakeBeacon) handleFinalized(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"execution_optimistic": false,
		"finalized":            true,
		"data": map[string]any{
			"root":      "0x" + strings.Repeat("ab", 32),
			"canonical": true,
			"header": map[string]any{
				"message": map[string]any{
					"slot":           strconv.FormatUint(b.finalized.Load(), 10),
					"proposer_index": "1",
				},
			},
		},
	})
}

// handleHeader serves GET /eth/v1/beacon/headers/{slot}: the block feed's
// per-slot authority. Every slot up to finality is present, with a root and a
// parent_root that chain continuously (parent_root(slot) = root(slot-1)), which
// is what index/beacon's anchored continuity check verifies. A slot past
// finality 404s -- not yet.
func (b *fakeBeacon) handleHeader(w http.ResponseWriter, r *http.Request) {
	slot, err := strconv.ParseUint(r.PathValue("slot"), 10, 64)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"code": 400, "message": "bad slot"})
		return
	}
	if slot > b.finalized.Load() {
		writeJSON(w, http.StatusNotFound, map[string]any{"code": 404, "message": "not finalized"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"execution_optimistic": false,
		"finalized":            true,
		"data": map[string]any{
			"root":      rootHex(deriveRoot(slot)),
			"canonical": true,
			"header": map[string]any{
				"message": map[string]any{
					"slot":        strconv.FormatUint(slot, 10),
					"parent_root": rootHex(deriveRoot(slot - 1)),
				},
			},
		},
	})
}

// handleBlindedBlock serves GET /eth/v1/beacon/blinded_blocks/{slot}: the
// block's blob_kzg_commitments, from which anchored mode derives the slot's
// expected versioned hashes. A blobless slot returns an empty list.
func (b *fakeBeacon) handleBlindedBlock(w http.ResponseWriter, r *http.Request) {
	slot, err := strconv.ParseUint(r.PathValue("slot"), 10, 64)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"code": 400, "message": "bad slot"})
		return
	}
	if slot > b.finalized.Load() {
		writeJSON(w, http.StatusNotFound, map[string]any{"code": 404, "message": "not finalized"})
		return
	}
	commits := make([]string, 0, len(b.slots[slot]))
	for _, blob := range b.slots[slot] {
		commits = append(commits, blobCommitmentHex(blob))
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"execution_optimistic": false,
		"finalized":            true,
		"data": map[string]any{
			"message": map[string]any{
				"body": map[string]any{"blob_kzg_commitments": commits},
			},
		},
	})
}

// deriveRoot is the synthetic chain's deterministic root for a slot. The exact
// value is immaterial; that root(slot) and parent_root(slot+1) agree is what the
// continuity check turns on.
func deriveRoot(slot uint64) [32]byte {
	var r [32]byte
	r[0] = 0xbe
	binary.BigEndian.PutUint64(r[24:], slot)
	return r
}

// rootHex renders a 32-byte root the way the beacon API states it.
func rootHex(r [32]byte) string { return "0x" + hex.EncodeToString(r[:]) }

// blobCommitmentHex is a blob's real KZG commitment, hex-encoded: what a block's
// blob_kzg_commitments carries, and what anchored mode derives its expected vh
// from.
func blobCommitmentHex(blob []byte) string {
	c, err := kzg4844.BlobToCommitment((*kzg4844.Blob)(blob))
	if err != nil {
		panic("e2e: fixture blob is not a valid KZG blob: " + err.Error())
	}
	return "0x" + hex.EncodeToString(c[:])
}

// handleBlobs serves a slot's blobs, honouring the versioned_hashes filter the
// chain indexer sends.
func (b *fakeBeacon) handleBlobs(w http.ResponseWriter, r *http.Request) {
	b.requests.Add(1)

	slot, err := strconv.ParseUint(r.PathValue("slot"), 10, 64)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"code": 400, "message": "bad slot"})
		return
	}
	// A real node will not answer for a slot it has not finalized. Serving one
	// anyway would let a bug in the loop's finality bound pass unnoticed.
	if slot > b.finalized.Load() {
		writeJSON(w, http.StatusNotFound, map[string]any{"code": 404, "message": "not finalized"})
		return
	}

	blobs, ok := b.slots[slot]
	if !ok || len(blobs) == 0 {
		// The 404 that means four different things. See index/beacon's package
		// comment.
		writeJSON(w, http.StatusNotFound, map[string]any{"code": 404, "message": "no blobs at slot"})
		return
	}

	filter := r.URL.Query()["versioned_hashes"]
	if len(filter) == 0 {
		writeJSON(w, http.StatusOK, map[string]any{"data": hexBlobs(blobs)})
		return
	}

	// Filtered: exactly one blob per requested vh, in request order (spec 7.1).
	out := make([][]byte, 0, len(filter))
	for _, want := range filter {
		vh, err := parseVHHex(want)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"code": 400, "message": "bad versioned hash"})
			return
		}
		blob, ok := findBlob(blobs, vh)
		if !ok {
			writeJSON(w, http.StatusNotFound, map[string]any{"code": 404, "message": "slot does not carry " + want})
			return
		}
		out = append(out, blob)
	}
	writeJSON(w, http.StatusOK, map[string]any{"data": hexBlobs(out)})
}

// findBlob returns the blob in blobs whose versioned hash is vh.
func findBlob(blobs [][]byte, vh schema.VersionedHash) ([]byte, bool) {
	for _, blob := range blobs {
		if blobVH(blob) == vh {
			return blob, true
		}
	}
	return nil, false
}

// hexBlobs renders blobs the way the API states them.
func hexBlobs(blobs [][]byte) []string {
	out := make([]string, 0, len(blobs))
	for _, b := range blobs {
		out = append(out, "0x"+hex.EncodeToString(b))
	}
	return out
}

// parseVHHex parses a versioned hash from a query parameter.
func parseVHHex(s string) (schema.VersionedHash, error) {
	raw, err := hex.DecodeString(strings.TrimPrefix(s, "0x"))
	if err != nil {
		return schema.VersionedHash{}, err
	}
	if len(raw) != schema.VersionedHashSize {
		return schema.VersionedHash{}, errShortVH
	}
	return schema.VersionedHash(raw), nil
}

// writeJSON renders v as a response.
func writeJSON(w http.ResponseWriter, code int, v any) {
	body, err := json.Marshal(v)
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_, _ = w.Write(body)
}
