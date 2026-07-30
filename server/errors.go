package server

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/blobarchive/bloar/p2p"
	"github.com/blobarchive/bloar/schema"
)

// ErrBlobUnavailable reports a block this node cannot produce right now, for a
// reason that is not "there is no such block". Spec 7.1 and 11.4 answer it 503
// + Retry-After + no-store: the index says the block exists, so the one thing
// the response must not be is a 404.
//
// It is the seam for the reasons only a follower has and this package cannot
// name: a blob withheld from serving because it failed full verification (spec
// 11.4) is one, and follow wraps it in this. The other reason -- a bitswap
// fetch that did not land -- arrives as *p2p.FetchError from the blockstore
// itself, with no follower code in between to wrap it, which is why unavailable
// tests for both.
var ErrBlobUnavailable = errors.New("server: block unavailable")

// unavailable reports whether err is 503 rather than 404 or 500 (spec 7.1,
// 11.4).
//
// A fetch that failed is emphatically not an ipld not-found: the difference is
// the whole of spec 11.4's status mapping, and p2p.FetchError exists to keep
// the two from ever being confused. A block the index names and this node could
// not get is the archive's failure and is retryable; a block the index does not
// name is an answer.
func unavailable(err error) bool {
	var fetch *p2p.FetchError
	return errors.As(err, &fetch) || errors.Is(err, ErrBlobUnavailable)
}

// errorBody is the beacon-API error shape every endpoint answers failures in
// (spec 7): {"code": <int>, "message": "<str>"}.
//
// MissingBlobs is spec 7.2's one addition to the shape, on the 409 a refs batch
// gets when it names blobs the archive does not hold. ManifestTip is the other:
// the head's current manifest tip, on the 409 a refs batch gets when its
// expected_manifest is stale. Both are omitted
// everywhere else, so a client that ignores them sees exactly the beacon shape.
type errorBody struct {
	Code         int      `json:"code"`
	Message      string   `json:"message"`
	MissingBlobs []string `json:"missing_blobs,omitempty"`
	ManifestTip  string   `json:"manifest_tip,omitempty"`
	// CurrentGeneration is a pointer so generation zero remains present on a
	// generation-endpoint conflict rather than being lost to omitempty.
	CurrentGeneration *uint64 `json:"current_generation,omitempty"`
}

// writeError answers in the beacon error shape. The code goes in the body as
// well as the status line, which is the beacon API's redundancy, not ours.
func writeError(w http.ResponseWriter, code int, format string, args ...any) {
	writeJSON(w, code, errorBody{Code: code, Message: fmt.Sprintf(format, args...)})
}

// writeJSON renders v and writes it. Rendering happens before any header is
// set so that a failure can still answer with a status of its own.
func writeJSON(w http.ResponseWriter, code int, v any) {
	body, err := json.Marshal(v)
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	writeRaw(w, code, body)
}

// writeRaw writes pre-rendered JSON.
func writeRaw(w http.ResponseWriter, code int, body []byte) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_, _ = w.Write(body)
}

// writeOctetStream writes body as spec 7.1's application/octet-stream response
// variant: the raw concatenation of fixed-size blobs, self-framed by their
// size, with no JSON envelope. It is the only response this package writes that
// is not JSON, which is why it does not go through writeRaw.
func writeOctetStream(w http.ResponseWriter, code int, body []byte) {
	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(code)
	_, _ = w.Write(body)
}

// parseVH parses a versioned hash. The 0x prefix is optional on the way in and
// always present on the way out: Nitro sends it, and Postel is cheap here.
func parseVH(s string) (schema.VersionedHash, error) {
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

// parseVHs parses the versioned_hashes query parameters, preserving order:
// spec 7.1 answers a filtered request in request order, so this list is the
// answer's shape.
func parseVHs(raw []string) ([]schema.VersionedHash, error) {
	out := make([]schema.VersionedHash, 0, len(raw))
	for _, s := range raw {
		vh, err := parseVH(s)
		if err != nil {
			return nil, err
		}
		out = append(out, vh)
	}
	return out, nil
}

// versionedHashQueryCount returns the number of hashes encoded by the
// versioned_hashes query parameters. The Beacon API's OpenAPI schema describes
// an array and therefore canonically serializes it as repeated query keys.
// Base's beacon client serializes the same array as one comma-separated value,
// so the read API accepts both forms (and mixtures of them).
func versionedHashQueryCount(raw []string) int {
	n := 0
	for _, encoded := range raw {
		n += strings.Count(encoded, ",") + 1
	}
	return n
}

// parseVersionedHashQuery parses both supported query-array encodings while
// preserving their wire order and multiplicity.
func parseVersionedHashQuery(raw []string) ([]schema.VersionedHash, error) {
	out := make([]schema.VersionedHash, 0, versionedHashQueryCount(raw))
	for _, encoded := range raw {
		for _, s := range strings.Split(encoded, ",") {
			vh, err := parseVH(s)
			if err != nil {
				return nil, err
			}
			out = append(out, vh)
		}
	}
	return out, nil
}

// vhHex renders a versioned hash the way the API always states one.
func vhHex(vh schema.VersionedHash) string { return "0x" + hex.EncodeToString(vh[:]) }
