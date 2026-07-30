package conformance

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto/kzg4844"
	"github.com/ethereum/go-ethereum/ethclient"

	"github.com/offchainlabs/nitro/util/blobs"
	"github.com/offchainlabs/nitro/util/headerreader"
)

// Spec 13.1, the flagship conformance test: Arbitrum Nitro's own beacon client
// -- headerreader.BlobClient, at the version nitro ships -- syncs blobs from an
// in-process bloard. Nothing about nitro is mocked or reimplemented here: the
// blobs are built by nitro's encoder, the KZG proof check is nitro's, and the
// HTTP requests are the ones nitro's own URL construction produces. If this
// file passes, nitro can sync from us.
//
// The one thing that is faked is the execution-layer RPC, because GetBlobs
// derives its slot from a parent-chain header it fetches over ethclient. That
// is a fixture, not a stand-in for anything bloard serves.

// The fixture layout: 6 blobs over 3 slots, plus a 7th that is never ingested
// and so backs the covered-but-absent negative case.
const (
	slotA = 100 // 3 blobs -- wide enough that a reversed filter is meaningful
	slotB = 101 // 1 blob
	slotC = 102 // 2 blobs

	syncedTo = 103 // slots at or above this are uncovered: the 503 path

	fixtureBlobs = 6
	absentBlob   = fixtureBlobs // index of the un-ingested blob
)

// fixtures is the ingested archive plus the fixture material the assertions
// need.
//
// base is the archive the client is pointed at, which is not always the archive
// the fixtures were ingested into: spec 13.8 runs this whole suite against a
// follower that replicated them (see follower_test.go). Everything else here is
// a fact about the blobs and is the same either way.
type fixtures struct {
	stack *stack
	base  string

	// blobs and hashes are all 7 blobs, index-aligned. Only the first 6 are
	// ingested.
	blobs  []kzg4844.Blob
	hashes []common.Hash

	// bySlot maps a fixture slot to the blob indices archived at it, in the
	// order they were declared to the archive.
	bySlot map[uint64][]int
}

// makeFixtures builds real blobs with nitro's own encoder, ingests six of them
// through bloard's HTTP API, and declares the archive synced to syncedTo.
func makeFixtures(t *testing.T) *fixtures { return makeFixturesOn(t, newStack(t)) }

// makeFixturesOn is makeFixtures against a stack the caller built: a writer with
// a libp2p host, for the follower suite.
func makeFixturesOn(t *testing.T, s *stack) *fixtures {
	t.Helper()

	// Deterministic material: byte i of blob n is a function of both, so no
	// two blobs collide and a mixed-up response is visible.
	raw := make([]byte, (fixtureBlobs+1)*blobs.BlobEncodableData)
	for i := range raw {
		raw[i] = byte(i*7 + i/blobs.BlobEncodableData)
	}

	// EncodeBlobs frames the payload, so the request spills past the blob it
	// would fill exactly; take the first seven of whatever it produced.
	encoded, err := blobs.EncodeBlobs(raw)
	if err != nil {
		t.Fatalf("blobs.EncodeBlobs: %v", err)
	}
	if len(encoded) < fixtureBlobs+1 {
		t.Fatalf("EncodeBlobs produced %d blobs, want at least %d", len(encoded), fixtureBlobs+1)
	}
	encoded = encoded[:fixtureBlobs+1]

	_, hashes, err := blobs.ComputeCommitmentsAndHashes(encoded)
	if err != nil {
		t.Fatalf("blobs.ComputeCommitmentsAndHashes: %v", err)
	}

	f := &fixtures{
		stack:  s,
		base:   s.url,
		blobs:  encoded,
		hashes: hashes,
		bySlot: map[uint64][]int{
			slotA: {0, 1, 2},
			slotB: {3},
			slotC: {4, 5},
		},
	}

	// Ingest the six through the real endpoint, and cross-check that bloard's
	// KZG agrees with nitro's about every versioned hash. If these two ever
	// disagree the rest of the test is meaningless.
	body := make([][]byte, 0, fixtureBlobs)
	for i := range fixtureBlobs {
		body = append(body, f.blobs[i][:])
	}
	served := f.stack.put(body...)
	for i, got := range served {
		if want := f.hashes[i].Hex(); !strings.EqualFold(got, want) {
			t.Fatalf("blob %d: bloard computed versioned hash %s, nitro computed %s", i, got, want)
		}
	}

	rows := make([]map[string]any, 0, len(f.bySlot))
	for _, slot := range []uint64{slotA, slotB, slotC} {
		vhs := make([]string, 0, len(f.bySlot[slot]))
		for _, i := range f.bySlot[slot] {
			vhs = append(vhs, f.hashes[i].Hex())
		}
		rows = append(rows, map[string]any{"slot": slot, "versioned_hashes": vhs})
	}
	f.stack.refs(rows, syncedTo)

	return f
}

// beacon is the URL nitro's BlobClient is configured with: the archive, plus
// the head as a path prefix, which is the whole question spec 7.1 poses to
// nitro's URL construction.
func (f *fixtures) beacon() string { return f.base + "/" + testHead }

// at returns the same fixtures pointed at another archive serving the same head.
func (f *fixtures) at(base string) *fixtures {
	out := *f
	out.base = base
	return &out
}

// hashesAt returns the versioned hashes archived at a fixture slot.
func (f *fixtures) hashesAt(slot uint64) []common.Hash {
	out := make([]common.Hash, 0, len(f.bySlot[slot]))
	for _, i := range f.bySlot[slot] {
		out = append(out, f.hashes[i])
	}
	return out
}

// blobsAt returns the blobs archived at a fixture slot.
func (f *fixtures) blobsAt(slot uint64) []kzg4844.Blob {
	out := make([]kzg4844.Blob, 0, len(f.bySlot[slot]))
	for _, i := range f.bySlot[slot] {
		out = append(out, f.blobs[i])
	}
	return out
}

// slotTime is the parent-chain timestamp that nitro maps to slot: GetBlobs
// computes slot = (header.Time - genesisTime) / secondsPerSlot, using the
// genesis and spec values it read in Initialize().
func slotTime(slot uint64) uint64 { return genesisTime + slot*secondsPerSlot }

// newBlobClient builds nitro's client against a beacon URL whose path carries
// the head prefix, and runs the real Initialize(). Proof verification is left
// on: the default config's SkipBlobProofVerification is false and nothing here
// touches it.
func newBlobClient(t *testing.T, beaconURL string, ec *ethclient.Client) *headerreader.BlobClient {
	t.Helper()

	cfg := headerreader.DefaultBlobClientConfig
	cfg.BeaconUrl = beaconURL
	if cfg.Dangerous.SkipBlobProofVerification {
		t.Fatal("nitro's default config skips blob proof verification; this test would prove nothing")
	}

	client, err := headerreader.NewBlobClient(cfg, ec)
	if err != nil {
		t.Fatalf("headerreader.NewBlobClient: %v", err)
	}
	if err := client.Initialize(t.Context()); err != nil {
		t.Fatalf("BlobClient.Initialize against %s: %v", beaconURL, err)
	}
	return client
}

// TestNitroInitialize is the narrow claim that nitro's Initialize() -- which is
// two GETs, /eth/v1/beacon/genesis and /eth/v1/config/spec -- succeeds against
// a beacon URL carrying bloard's /{head} path prefix. Initialize() returning
// nil is itself the assertion that nitro parsed genesis_time and
// SECONDS_PER_SLOT out of the string-valued beacon JSON: it errors on a zero
// SECONDS_PER_SLOT, and a prefix that did not survive nitro's path.Join would
// have 404'd both requests.
func TestNitroInitialize(t *testing.T) { nitroInitialize(t, makeFixtures(t)) }

func nitroInitialize(t *testing.T, f *fixtures) {
	t.Helper()
	newBlobClient(t, f.beacon(), nil)
}

// TestNitroSyncsBlobs is spec 13.1 proper: for every fixture slot, nitro's
// client fetches the slot's blobs through its own code path and hands back
// bytes identical to what was ingested, having verified every KZG proof
// itself.
func TestNitroSyncsBlobs(t *testing.T) { nitroSyncsBlobs(t, makeFixtures(t)) }

func nitroSyncsBlobs(t *testing.T, f *fixtures) {
	t.Helper()
	client := newBlobClient(t, f.beacon(), nil)

	for _, slot := range []uint64{slotA, slotB, slotC} {
		want := f.blobsAt(slot)
		got, err := client.GetBlobsBySlot(t.Context(), slot, f.hashesAt(slot))
		if err != nil {
			t.Fatalf("slot %d: GetBlobsBySlot: %v", slot, err)
		}
		if len(got) != len(want) {
			t.Fatalf("slot %d: got %d blobs, want %d", slot, len(got), len(want))
		}
		for i := range want {
			if got[i] != want[i] {
				t.Errorf("slot %d: blob %d is not the ingested bytes", slot, i)
			}
		}
	}
}

// TestNitroGetBlobsDerivesSlotFromHeader exercises GetBlobs rather than
// GetBlobsBySlot: nitro fetches a parent-chain header over ethclient and
// derives the slot from its timestamp. This is the entry point a real nitro
// node uses, so the timestamp-to-slot arithmetic is part of what 13.1 claims
// works.
func TestNitroGetBlobsDerivesSlotFromHeader(t *testing.T) {
	nitroGetBlobsDerivesSlotFromHeader(t, makeFixtures(t))
}

func nitroGetBlobsDerivesSlotFromHeader(t *testing.T, f *fixtures) {
	t.Helper()

	headers := make(map[common.Hash]*types.Header)
	blockOf := make(map[uint64]common.Hash)
	for _, slot := range []uint64{slotA, slotB, slotC} {
		h := &types.Header{
			Number:     big.NewInt(int64(slot)),
			Time:       slotTime(slot),
			Difficulty: big.NewInt(0),
			GasLimit:   30_000_000,
		}
		headers[h.Hash()] = h
		blockOf[slot] = h.Hash()
	}

	ec := newFakeEL(t, headers)
	client := newBlobClient(t, f.beacon(), ec)

	for _, slot := range []uint64{slotA, slotB, slotC} {
		want := f.blobsAt(slot)
		got, err := client.GetBlobs(t.Context(), blockOf[slot], f.hashesAt(slot))
		if err != nil {
			t.Fatalf("slot %d: GetBlobs: %v", slot, err)
		}
		if len(got) != len(want) {
			t.Fatalf("slot %d: got %d blobs, want %d", slot, len(got), len(want))
		}
		for i := range want {
			if got[i] != want[i] {
				t.Errorf("slot %d: blob %d is not the ingested bytes", slot, i)
			}
		}
	}
}

// TestNitroRequestOrderPreserved pins spec 7.1's ordering rule with the client
// that depends on it. Nitro matches response[i] against versionedHashes[i]
// positionally, so a server answering in archive order rather than request
// order fails nitro outright. Asking for a slot's blobs reversed and getting
// them back reversed is therefore the whole proof: had bloard ignored the
// request order, this would error rather than mis-order.
func TestNitroRequestOrderPreserved(t *testing.T) { nitroRequestOrderPreserved(t, makeFixtures(t)) }

func nitroRequestOrderPreserved(t *testing.T, f *fixtures) {
	t.Helper()
	client := newBlobClient(t, f.beacon(), nil)

	want := f.blobsAt(slotA)
	slices.Reverse(want)
	ask := f.hashesAt(slotA)
	slices.Reverse(ask)

	got, err := client.GetBlobsBySlot(t.Context(), slotA, ask)
	if err != nil {
		t.Fatalf("reversed filter: GetBlobsBySlot: %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("reversed filter: got %d blobs, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("reversed filter: blob %d is not the blob whose hash was asked for at that position", i)
		}
	}
}

// TestNitroVerifiesProofs proves nitro's KZG check is not merely configured but
// load-bearing, by putting a proxy in front of bloard that corrupts one blob.
// Without this, "verification passed" would be indistinguishable from
// "verification never ran": every other case here serves honest bytes.
//
// The proxy flips the last nibble of the first blob, which leaves each field
// element below the BLS modulus -- so the commitment still computes and nitro
// has to notice the mismatch rather than fail to do the math.
func TestNitroVerifiesProofs(t *testing.T) { nitroVerifiesProofs(t, makeFixtures(t)) }

func nitroVerifiesProofs(t *testing.T, f *fixtures) {
	t.Helper()
	proxy := newTamperProxy(t, f.base)
	client := newBlobClient(t, proxy+"/"+testHead, nil)

	got, err := client.GetBlobsBySlot(t.Context(), slotA, f.hashesAt(slotA))
	if err == nil {
		t.Fatal("nitro accepted a corrupted blob: its KZG verification did not run")
	}
	if got != nil {
		t.Errorf("nitro returned %d blobs alongside a verification error", len(got))
	}
	if !strings.Contains(err.Error(), "versioned hash mismatch") {
		t.Errorf("error = %v; want nitro's versioned hash mismatch", err)
	}
}

// TestNitroRejectsAbsentBlob is the covered-but-absent negative case: the slot
// is inside the archive's synced range, but one requested hash is not one the
// archive holds, so bloard 404s. Nitro must surface that as an error promptly
// rather than hang, which the context deadline enforces.
func TestNitroRejectsAbsentBlob(t *testing.T) { nitroRejectsAbsentBlob(t, makeFixtures(t)) }

func nitroRejectsAbsentBlob(t *testing.T, f *fixtures) {
	t.Helper()
	client := newBlobClient(t, f.beacon(), nil)

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	ask := append(f.hashesAt(slotA), f.hashes[absentBlob])
	got, err := client.GetBlobsBySlot(ctx, slotA, ask)
	if err == nil {
		t.Fatal("nitro reported success for a blob the archive does not hold")
	}
	if got != nil {
		t.Errorf("nitro returned %d blobs alongside an error", len(got))
	}
	if ctx.Err() != nil {
		t.Errorf("nitro hung until the deadline rather than surfacing the 404: %v", ctx.Err())
	}
	if !strings.Contains(err.Error(), "404") {
		t.Errorf("error = %v; want nitro to surface bloard's 404", err)
	}
}

// TestNitroRejectsUncoveredSlot is the 503 path of spec 7.1: a slot at or
// beyond synced_to is not yet archived, and bloard says so with a 503 and a
// Retry-After rather than an empty 200. The claim under test is that nitro
// treats that as an error -- an empty success would silently look like a slot
// with no blobs, which is how a node skips data.
func TestNitroRejectsUncoveredSlot(t *testing.T) { nitroRejectsUncoveredSlot(t, makeFixtures(t)) }

func nitroRejectsUncoveredSlot(t *testing.T, f *fixtures) {
	t.Helper()
	client := newBlobClient(t, f.beacon(), nil)

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	got, err := client.GetBlobsBySlot(ctx, syncedTo+1, f.hashesAt(slotA))
	if err == nil {
		t.Fatal("nitro treated an unsynced slot's 503 as success")
	}
	if got != nil {
		t.Errorf("nitro returned %d blobs alongside an error", len(got))
	}
	if ctx.Err() != nil {
		t.Errorf("nitro hung until the deadline rather than surfacing the 503: %v", ctx.Err())
	}
	if !strings.Contains(err.Error(), "503") {
		t.Errorf("error = %v; want nitro to surface bloard's 503", err)
	}
}

// newTamperProxy fronts upstream with a proxy that corrupts the first blob of
// every blobs response and passes everything else through untouched.
func newTamperProxy(t *testing.T, upstream string) string {
	t.Helper()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req, err := http.NewRequestWithContext(r.Context(), r.Method, upstream+r.URL.RequestURI(), r.Body)
		if err != nil {
			t.Errorf("proxy building request: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Errorf("proxy calling upstream: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		defer resp.Body.Close()

		raw, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Errorf("proxy reading upstream body: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		// Everything but a successful blobs response goes through untouched --
		// notably the two Initialize() endpoints, which must still answer.
		var body struct {
			Data []string `json:"data"`
		}
		if !strings.Contains(r.URL.Path, "/eth/v1/beacon/blobs/") ||
			resp.StatusCode != http.StatusOK ||
			json.Unmarshal(raw, &body) != nil || len(body.Data) == 0 {

			w.Header().Set("Content-Type", resp.Header.Get("Content-Type"))
			w.WriteHeader(resp.StatusCode)
			if _, err := w.Write(raw); err != nil {
				t.Errorf("proxy writing passthrough response: %v", err)
			}
			return
		}

		body.Data[0] = flipLastNibble(t, body.Data[0])
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(body); err != nil {
			t.Errorf("proxy writing response: %v", err)
		}
	}))
	t.Cleanup(srv.Close)

	return srv.URL
}

// flipLastNibble corrupts a hex-encoded blob in its lowest bits, which keeps
// every field element below the BLS modulus.
func flipLastNibble(t *testing.T, blob string) string {
	t.Helper()

	raw, err := hex.DecodeString(strings.TrimPrefix(blob, "0x"))
	if err != nil {
		t.Fatalf("proxy decoding blob: %v", err)
	}
	if len(raw) != len(kzg4844.Blob{}) {
		t.Fatalf("proxy got a %d-byte blob, want %d", len(raw), len(kzg4844.Blob{}))
	}
	raw[len(raw)-1] ^= 0x0f
	return "0x" + hex.EncodeToString(raw)
}

// newFakeEL serves the parent-chain headers GetBlobs reads timestamps from.
// This stands in for an execution node, not for anything bloard serves.
func newFakeEL(t *testing.T, headers map[common.Hash]*types.Header) *ethclient.Client {
	t.Helper()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID     json.RawMessage   `json:"id"`
			Method string            `json:"method"`
			Params []json.RawMessage `json:"params"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("fake EL decoding request: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		resp := map[string]any{"jsonrpc": "2.0", "id": req.ID, "result": nil}
		if req.Method == "eth_getBlockByHash" && len(req.Params) > 0 {
			var hash common.Hash
			if err := json.Unmarshal(req.Params[0], &hash); err != nil {
				t.Errorf("fake EL decoding block hash: %v", err)
			} else if h, ok := headers[hash]; ok {
				resp["result"] = h
			}
		} else {
			t.Errorf("fake EL got unexpected method %q; nitro's header fetch should only call eth_getBlockByHash", req.Method)
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(resp); err != nil {
			t.Errorf("fake EL writing response: %v", err)
		}
	}))
	t.Cleanup(srv.Close)

	ec, err := ethclient.Dial(srv.URL)
	if err != nil {
		t.Fatalf("ethclient.Dial: %v", err)
	}
	t.Cleanup(ec.Close)

	return ec
}
