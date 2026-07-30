package server_test

import (
	"encoding/hex"
	"net/http"
	"slices"
	"strings"
	"testing"

	"github.com/blobarchive/bloar/ingest"
)

// TestRefsHappy covers the accepted path of spec 7.2's refs endpoint and the
// response fields an indexer reads back.
func TestRefsHappy(t *testing.T) {
	s := newStack(t, stackOpts{})
	vhs := s.put(makeBlob(1), makeBlob(2))

	resp := s.postJSON("/bloar/v1/heads/"+testHead+"/refs", map[string]any{
		"rows":      []map[string]any{row(9, vhs[0]), row(11, vhs[1])},
		"synced_to": 12,
	}, withAuth)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", resp.StatusCode, readAll(t, resp))
	}

	var out refsBody
	decode(t, resp, &out)
	if out.SyncedTo != 12 {
		t.Errorf("synced_to = %d, want 12", out.SyncedTo)
	}
	if !strings.HasPrefix(out.Root, "bafy") {
		t.Errorf("root = %q, want a dag-cbor CIDv1 (bafy...)", out.Root)
	}
	if out.NoOp {
		t.Error("noop = true for a batch that advanced coverage")
	}
}

// TestRefsNoopReplay covers spec 5.1's idempotent replay through the API. Both
// indexers are stateless and resume from synced_to (spec 10), so a batch
// re-posted after a crash mid-response is the normal case, not an edge one: it
// must be accepted, change nothing, and say so.
func TestRefsNoopReplay(t *testing.T) {
	s := newStack(t, stackOpts{})
	vhs := s.put(makeBlob(1))

	batch := map[string]any{"rows": []map[string]any{row(9, vhs[0])}, "synced_to": 12}
	first := s.postJSON("/bloar/v1/heads/"+testHead+"/refs", batch, withAuth)
	defer first.Body.Close()
	var out refsBody
	decode(t, first, &out)

	replay := s.postJSON("/bloar/v1/heads/"+testHead+"/refs", batch, withAuth)
	defer replay.Body.Close()
	if replay.StatusCode != http.StatusOK {
		t.Fatalf("replay: status = %d, want 200: %s", replay.StatusCode, readAll(t, replay))
	}
	var out2 refsBody
	decode(t, replay, &out2)

	if !out2.NoOp {
		t.Error("noop = false on a replayed batch")
	}
	if out2.Root != out.Root {
		t.Errorf("replay changed the root: %s -> %s", out.Root, out2.Root)
	}
	if out2.SyncedTo != out.SyncedTo {
		t.Errorf("replay changed synced_to: %d -> %d", out.SyncedTo, out2.SyncedTo)
	}
}

// TestRefsMissingBlobs covers spec 7.2's 409 and its missing_blobs list. The
// list is the useful half: a writer whose batch was refused needs to know which
// blobs to post, not merely that something was wrong.
func TestRefsMissingBlobs(t *testing.T) {
	s := newStack(t, stackOpts{})
	known := s.put(makeBlob(1))
	// Ingested nowhere: derive the hashes without posting the blobs.
	unknown := vhsOf(t, makeBlob(50), makeBlob(51))

	resp := s.postJSON("/bloar/v1/heads/"+testHead+"/refs", map[string]any{
		"rows":      []map[string]any{row(9, known[0], unknown[0]), row(10, unknown[1])},
		"synced_to": 12,
	}, withAuth)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("status = %d, want 409: %s", resp.StatusCode, readAll(t, resp))
	}

	body := errorOf(t, resp)
	// Every missing hash, not the first: the writer is fixing a batch and wants
	// the whole list.
	for _, vh := range unknown {
		if !slices.Contains(body.MissingBlobs, vh) {
			t.Errorf("missing_blobs = %v, want it to contain %s", body.MissingBlobs, vh)
		}
	}
	if slices.Contains(body.MissingBlobs, known[0]) {
		t.Errorf("missing_blobs names %s, which the archive holds", known[0])
	}

	// The refusal was total: nothing was applied.
	if got := s.syncedTo(); got != nil {
		t.Errorf("synced_to = %d after a refused batch, want null", *got)
	}
}

// TestRefsConflict covers a 409 that carries no missing_blobs: the field is
// spec 7.2's addition for one failure mode only, and a client keying on it must
// not see it elsewhere.
func TestRefsConflict(t *testing.T) {
	s := newStack(t, stackOpts{})
	vhs := s.put(makeBlob(1), makeBlob(2))
	s.refs([]map[string]any{row(9, vhs[0])}, 12)

	tests := []struct {
		name string
		body map[string]any
	}{
		{"rows out of order", map[string]any{
			"rows": []map[string]any{row(15, vhs[1]), row(14, vhs[0])}, "synced_to": 16,
		}},
		{"row past synced_to", map[string]any{
			"rows": []map[string]any{row(20, vhs[1])}, "synced_to": 16,
		}},
		{"row before origin_slot", map[string]any{
			"rows": []map[string]any{row(2, vhs[1])}, "synced_to": 16,
		}},
		{"partial overlap", map[string]any{
			// Half replay, half extension: spec 5.1 step 3 refuses it rather
			// than guessing which half was meant.
			"rows": []map[string]any{row(9, vhs[0]), row(15, vhs[1])}, "synced_to": 16,
		}},
		{"replay that contradicts", map[string]any{
			"rows": []map[string]any{row(9, vhs[1])}, "synced_to": 12,
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resp := s.postJSON("/bloar/v1/heads/"+testHead+"/refs", tc.body, withAuth)
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusConflict {
				t.Fatalf("status = %d, want 409: %s", resp.StatusCode, readAll(t, resp))
			}
			body := errorOf(t, resp)
			if body.MissingBlobs != nil {
				t.Errorf("missing_blobs = %v on a conflict that is not about missing blobs", body.MissingBlobs)
			}
		})
	}
}

// TestRefsMalformed covers spec 7.2's 400: a body the archive cannot read at
// all, as distinct from one it can read and refuses (409).
func TestRefsMalformed(t *testing.T) {
	s := newStack(t, stackOpts{})

	tests := []struct {
		name string
		body string
	}{
		{"not json", "{"},
		{"empty", ""},
		{"synced_to missing", `{"rows": []}`},
		{"synced_to is a string", `{"rows": [], "synced_to": "12"}`},
		{"synced_to is negative", `{"rows": [], "synced_to": -1}`},
		{"slot is negative", `{"rows": [{"slot": -1, "versioned_hashes": []}], "synced_to": 12}`},
		{"vh is not hex", `{"rows": [{"slot": 9, "versioned_hashes": ["zz"]}], "synced_to": 12}`},
		{"vh is short", `{"rows": [{"slot": 9, "versioned_hashes": ["0xdead"]}], "synced_to": 12}`},
		{"rows is not an array", `{"rows": 3, "synced_to": 12}`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resp := s.do("POST", "/bloar/v1/heads/"+testHead+"/refs", strings.NewReader(tc.body), withAuth)
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400: %s", resp.StatusCode, readAll(t, resp))
			}
			errorOf(t, resp)
		})
	}
}

// TestSyncedTo covers spec 7.2's progress endpoint, which is the whole of an
// indexer's state. null is not a formality: an indexer reads it as "start at
// origin_slot", and a 0 there would send it to slot 0.
func TestSyncedTo(t *testing.T) {
	s := newStack(t, stackOpts{})

	if got := s.syncedTo(); got != nil {
		t.Errorf("synced_to = %d on an empty head, want null", *got)
	}

	vhs := s.put(makeBlob(1))
	s.refs([]map[string]any{row(9, vhs[0])}, 12)

	got := s.syncedTo()
	if got == nil {
		t.Fatal("synced_to = null after a batch")
	}
	if *got != 12 {
		t.Errorf("synced_to = %d, want 12", *got)
	}
}

// TestTruncateConfirm covers the guard on spec 5.4's emergency operation. A
// mismatched confirmation must not truncate, which means it must be checked
// before anything happens rather than after.
func TestTruncateConfirm(t *testing.T) {
	s := newStack(t, stackOpts{})
	vhs := s.put(makeBlob(1), makeBlob(2))
	s.refs([]map[string]any{row(9, vhs[0]), row(11, vhs[1])}, 12)
	before := s.headEntry()

	for _, confirm := range []string{"", "wrong", "ALL", testHead + " "} {
		t.Run("confirm="+confirm, func(t *testing.T) {
			resp := s.postJSON("/bloar/v1/heads/"+testHead+"/truncate",
				map[string]any{"slot": 9, "confirm": confirm}, withAuth)
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400", resp.StatusCode)
			}
			errorOf(t, resp)

			// Nothing happened: same root, same coverage.
			if after := s.headEntry(); after.Root != before.Root {
				t.Errorf("a refused truncate changed the root: %s -> %s", before.Root, after.Root)
			}
			if got := s.syncedTo(); got == nil || *got != 12 {
				t.Errorf("a refused truncate changed synced_to: %v", got)
			}
		})
	}

	t.Run("slot missing", func(t *testing.T) {
		resp := s.postJSON("/bloar/v1/heads/"+testHead+"/truncate", map[string]any{"confirm": testHead}, withAuth)
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400", resp.StatusCode)
		}
	})
}

// TestTruncateHappy covers the accepted path: coverage rolls back, and the
// blobs past the new synced_to stop being served.
func TestTruncateHappy(t *testing.T) {
	s := newStack(t, stackOpts{})
	blobs := [][]byte{makeBlob(1), makeBlob(2)}
	vhs := s.put(blobs...)
	s.refs([]map[string]any{row(9, vhs[0]), row(11, vhs[1])}, 12)

	resp := s.postJSON("/bloar/v1/heads/"+testHead+"/truncate",
		map[string]any{"slot": 10, "confirm": testHead}, withAuth)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", resp.StatusCode, readAll(t, resp))
	}

	var out struct {
		SyncedTo uint64 `json:"synced_to"`
		Root     string `json:"root"`
	}
	decode(t, resp, &out)
	if out.SyncedTo != 10 {
		t.Errorf("synced_to = %d, want 10", out.SyncedTo)
	}
	if !strings.HasPrefix(out.Root, "bafy") {
		t.Errorf("root = %q, want a dag-cbor CIDv1", out.Root)
	}

	// Slot 9 survives; slot 11 is past the new coverage and is now a 503, not
	// a 404: it is no longer archived, and it is coming back.
	if got := s.getBlobs(9, vhs[0]); len(got) != 1 || got[0] != blobHex(blobs[0]) {
		t.Error("slot 9 did not survive a truncate to slot 10")
	}
	after := s.get(blobsURL(11, vhs[1]))
	defer after.Body.Close()
	if after.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("slot 11 after truncate: status = %d, want 503", after.StatusCode)
	}

	// The published document tracks the truncation, not just the engine.
	if entry := s.headEntry(); entry.Root != out.Root {
		t.Errorf("document root = %s, truncate returned %s", entry.Root, out.Root)
	}
}

// TestTruncatePastSyncedTo covers spec 5.4's refusal: a head must not claim
// coverage it never had.
func TestTruncatePastSyncedTo(t *testing.T) {
	s := newStack(t, stackOpts{})
	vhs := s.put(makeBlob(1))
	s.refs([]map[string]any{row(9, vhs[0])}, 12)

	resp := s.postJSON("/bloar/v1/heads/"+testHead+"/truncate",
		map[string]any{"slot": 20, "confirm": testHead}, withAuth)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("status = %d, want 409", resp.StatusCode)
	}
	errorOf(t, resp)
}

// TestRefsUnknownHead covers the 404 on the mutating per-head endpoints.
func TestRefsUnknownHead(t *testing.T) {
	s := newStack(t, stackOpts{})

	for _, path := range []string{"/bloar/v1/heads/nope/refs", "/bloar/v1/heads/nope/truncate"} {
		t.Run(path, func(t *testing.T) {
			resp := s.postJSON(path, map[string]any{"rows": []any{}, "synced_to": 12, "slot": 9, "confirm": "nope"}, withAuth)
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusNotFound {
				t.Fatalf("status = %d, want 404", resp.StatusCode)
			}
			errorOf(t, resp)
		})
	}
}

// refsBody is the 200 body of the refs endpoint.
type refsBody struct {
	SyncedTo uint64 `json:"synced_to"`
	Root     string `json:"root"`
	NoOp     bool   `json:"noop"`
}

// syncedTo reads the head's coverage through the API.
func (s *stack) syncedTo() *uint64 {
	s.t.Helper()
	resp := s.get("/bloar/v1/heads/" + testHead + "/synced_to")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		s.t.Fatalf("GET synced_to: status = %d", resp.StatusCode)
	}
	var out struct {
		SyncedTo *uint64 `json:"synced_to"`
	}
	decode(s.t, resp, &out)
	return out.SyncedTo
}

// vhsOf derives versioned hashes without ingesting the blobs: the archive is
// asked to reference blobs it has never been given.
func vhsOf(t *testing.T, blobs ...[]byte) []string {
	t.Helper()
	out := make([]string, 0, len(blobs))
	for _, b := range blobs {
		vh, err := ingest.VersionedHash(b)
		if err != nil {
			t.Fatalf("deriving a versioned hash: %v", err)
		}
		out = append(out, "0x"+hex.EncodeToString(vh[:]))
	}
	return out
}
