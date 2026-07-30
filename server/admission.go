package server

import (
	"context"
	"math"

	"golang.org/x/sync/semaphore"

	"github.com/blobarchive/bloar/schema"
)

// Per-entry admission weights, in bytes: what one blob entry costs the budget
// . They charge the PEAK simultaneously-live allocation one
// entry produces, not the bytes that reach the wire, because spec 7.1 buffers
// the whole response before writing any of it (no streaming) -- so the memory a
// request must be admitted against is its high-water mark, not its result.
//
// octet-stream (serveBlobs, raw): the blob is read into raws[i] (BlobSize),
// then bytes.Join copies every blob into one contiguous body that is live at the
// same time as raws (another BlobSize). Peak = 2*BlobSize per entry.
//
// json (serveBlobs, default): renderBlobsJSON builds the whole response into one
// buffer sized to exactly blobsJSONSize -- no json.Marshal, no growable or pooled
// buffer, no intermediate []string -- so the peak is exactly the raws (BlobSize
// per entry) plus that one buffer, and nothing else. For n entries the buffer is
// blobsJSONSize(n) = jsonEnvelope + n*jsonPerEntry - 1 (see beacon.go), so
// peak(n) = n*BlobSize + blobsJSONSize(n). weightPerEntryJSON is peak(1); charging
// it per reserved entry over-reserves the constant envelope by (n-1)*(jsonEnvelope-1)
// >= 0, so n*weightPerEntryJSON >= peak(n) for every n. peak(n) is the response
// rendering the semaphore weighs -- the raws plus the render buffer, not every
// allocation on the handler path -- so what this proves is that those weighted
// renderings never exceed their reservation, the bound the guessed 8*BlobSize
// could not establish.
const (
	weightPerEntryOctet int64 = 2 * schema.BlobSize
	weightPerEntryJSON  int64 = int64(schema.BlobSize + jsonEnvelope + jsonPerEntry - 1)
)

// entryWeight is the per-entry reservation for the chosen encoding. JSON is the
// heavier of the two, which is why config validation sizes the budget against
// it (MaxResponseWeight).
func entryWeight(raw bool) int64 {
	if raw {
		return weightPerEntryOctet
	}
	return weightPerEntryJSON
}

// MaxResponseWeight is the largest reservation a single blobs response can make:
// the heavier (JSON) encoding of the most entries one response can carry. A
// filtered request is capped at maxQueryHashes entries; an unfiltered one at the
// stored-row ceiling. The budget must admit at least this, or the request it is
// meant to bound could never be served -- which config validation rejects.
//
// server.New and the config loader both clamp maxQueryHashes to the stored-row
// ceiling, so entries is at most that ceiling in every configured path. The
// multiplication is guarded anyway: no input may silently overflow int64 into a
// small or negative floor that a tiny budget could then satisfy.
// An input that would overflow clamps to MaxInt64, which no real budget admits.
func MaxResponseWeight(maxQueryHashes int) int64 {
	entries := maxQueryHashes
	if schema.MaxBlobsPerSlotCeiling > entries {
		entries = schema.MaxBlobsPerSlotCeiling
	}
	if int64(entries) > math.MaxInt64/weightPerEntryJSON {
		return math.MaxInt64
	}
	return int64(entries) * weightPerEntryJSON
}

// admission is the process-wide response-memory budget of the safety boundary. Every
// blob-carrying response reserves its worst-case peak against it before it looks
// up, reads, or allocates anything, and holds the reservation until the response
// is written. It is a byte-weighted semaphore: concurrency falls out of the
// weights rather than being a separate count.
type admission struct {
	sem *semaphore.Weighted
}

// newAdmission returns an admission over a budget of limit bytes. limit must be
// at least MaxResponseWeight for the server's cap, or a maximum response would
// block forever; server.New and the config loader both enforce that.
func newAdmission(limit int64) *admission {
	return &admission{sem: semaphore.NewWeighted(limit)}
}

// reserve charges entries of the chosen encoding, blocking until the budget can
// admit them or ctx ends. It reserves nothing and returns ctx.Err() if ctx ends
// first -- a canceled or timed-out waiter leaves the queue holding no budget.
// The returned weight is what release must be handed back, and is zero when
// there was nothing to reserve (a blobless response).
func (a *admission) reserve(ctx context.Context, entries int, raw bool) (int64, error) {
	weight := int64(entries) * entryWeight(raw)
	if weight <= 0 {
		return 0, nil
	}
	if err := a.sem.Acquire(ctx, weight); err != nil {
		return 0, err
	}
	return weight, nil
}

// release returns a reservation to the budget. release(0) is a no-op, so it is
// safe to defer against every reserve outcome.
func (a *admission) release(weight int64) {
	if weight > 0 {
		a.sem.Release(weight)
	}
}
