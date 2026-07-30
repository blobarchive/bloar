package server_test

// Regressions for strictness and presence-awareness at the JSON mutation
// boundary. Every mutation body this node defines is decoded
// under one strict contract -- unknown fields rejected at every level, exactly
// one value then EOF -- and every field whose contract turns on presence
// (top-level rows, a row's slot and hashes, a manifest's prev) distinguishes an
// absent key from an explicitly present empty or null value. The invariant these
// assert together: only an explicitly present empty array carries covered-empty
// meaning, and every rejection lands before any coverage advances.

import (
	"fmt"
	"net/http"
	"strings"
	"testing"
)

// dummyVH is a syntactically valid versioned hash for bodies that are rejected
// before any hash is resolved, so it need not name a blob the archive holds.
const dummyVH = "0x00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff"

// postRefsRaw posts a verbatim refs body -- the point is to send bytes a
// map[string]any could not express: a misspelled key, a trailing second value, a
// present null.
func (s *stack) postRefsRaw(body string) *http.Response {
	s.t.Helper()
	return s.do("POST", "/bloar/v1/heads/"+testHead+"/refs", strings.NewReader(body), withAuth)
}

// TestRefsUnknownRowsFieldCommitsFalseAbsence is the safety boundary reproducer,
// now asserting the fix. The historical bug: a refs body misspelling `rows` as
// `rowz` had the typo silently dropped, leaving a nil rows that read as a valid
// coverage-only batch, so the endpoint returned 200, committed a root, and turned
// the intended blob into a durable false absence. With DisallowUnknownFields and
// a required, presence-aware rows, the typo is a 400 naming the offending field,
// nothing is committed, and the correctly spelled retry lands the blob cleanly.
func TestRefsUnknownRowsFieldCommitsFalseAbsence(t *testing.T) {
	s := newStack(t, stackOpts{})
	vh := s.put(makeBlob(303))[0]

	// The caller meant one row but misspelled the top-level key. It is now a 400
	// naming `rowz`, not a silently-accepted no-op.
	typo := s.postRefsRaw(fmt.Sprintf(`{"rowz":[{"slot":8,"versioned_hashes":[%q]}],"synced_to":9}`, vh))
	if typo.StatusCode != http.StatusBadRequest {
		t.Fatalf("refs body with an unknown rowz field: status = %d, want 400, body = %s", typo.StatusCode, readAll(t, typo))
	}
	if msg := errorOf(t, typo).Message; !strings.Contains(msg, "rowz") {
		t.Errorf("400 for the typo does not name the offending field: %q", msg)
	}
	if got := s.syncedTo(); got != nil {
		t.Fatalf("coverage advanced to %d after the rejected typo; the rejection must precede any commit", *got)
	}

	// The shared decoder also requires exactly one value: a trailing second object
	// is a 400, not an accepted no-op with its tail ignored.
	trailing := s.postRefsRaw(`{"rows":[],"synced_to":9} {"unexpected":true}`)
	if trailing.StatusCode != http.StatusBadRequest {
		t.Fatalf("refs body with a trailing JSON value: status = %d, want 400, body = %s", trailing.StatusCode, readAll(t, trailing))
	}
	trailing.Body.Close()
	if got := s.syncedTo(); got != nil {
		t.Fatalf("coverage advanced to %d after the rejected trailing value", *got)
	}

	// Nothing was committed, so the correctly spelled batch lands and the intended
	// blob is served -- the false absence the bug produced never exists.
	correct := s.postRefsRaw(fmt.Sprintf(`{"rows":[{"slot":8,"versioned_hashes":[%q]}],"synced_to":9}`, vh))
	if correct.StatusCode != http.StatusOK {
		t.Fatalf("correctly spelled retry: status = %d, want 200, body = %s", correct.StatusCode, readAll(t, correct))
	}
	correct.Body.Close()
	if got := s.syncedTo(); got == nil || *got != 9 {
		t.Fatalf("coverage after the corrected batch = %v, want 9", got)
	}
	if got := s.getBlobs(8, vh); len(got) != 1 || got[0] != blobHex(makeBlob(303)) {
		t.Fatalf("slot 8 does not serve the intended blob after the corrected batch: %v", got)
	}
}

// TestRefsRowsPresenceRule sweeps the absent-vs-empty distinction across
// the refs schema: the top-level rows, and a row's slot and versioned_hashes.
// Only an explicitly present [] advances covered-empty; an omitted or nulled
// required field, an unknown field at any level, and a trailing value are each a
// 400 that commits nothing. A fresh stack per case makes "nothing was committed"
// checkable as a still-null synced_to.
func TestRefsRowsPresenceRule(t *testing.T) {
	const advance = uint64(9)
	cases := []struct {
		name     string
		body     string
		wantCode int
		wantMsg  string // a substring the 400 must contain; "" for the accepted case
		advances bool   // whether coverage should reach `advance`
	}{
		{"rows present empty advances", `{"rows":[],"synced_to":9}`, http.StatusOK, "", true},
		{"rows omitted", `{"synced_to":9}`, http.StatusBadRequest, "rows is required", false},
		{"rows null", `{"rows":null,"synced_to":9}`, http.StatusBadRequest, "rows is required", false},
		{"row missing slot", fmt.Sprintf(`{"rows":[{"versioned_hashes":[%q]}],"synced_to":9}`, dummyVH),
			http.StatusBadRequest, "slot is required", false},
		{"row null slot", fmt.Sprintf(`{"rows":[{"slot":null,"versioned_hashes":[%q]}],"synced_to":9}`, dummyVH),
			http.StatusBadRequest, "slot is required", false},
		{"row missing versioned_hashes", `{"rows":[{"slot":8}],"synced_to":9}`,
			http.StatusBadRequest, "versioned_hashes is required", false},
		{"row null versioned_hashes", `{"rows":[{"slot":8,"versioned_hashes":null}],"synced_to":9}`,
			http.StatusBadRequest, "versioned_hashes is required", false},
		{"unknown top-level field", `{"rows":[],"synced_to":9,"bogus":1}`,
			http.StatusBadRequest, "bogus", false},
		{"unknown nested row field", fmt.Sprintf(`{"rows":[{"slot":8,"versioned_hashes":[%q],"bogus":1}],"synced_to":9}`, dummyVH),
			http.StatusBadRequest, "bogus", false},
		{"trailing second value", `{"rows":[],"synced_to":9} {"unexpected":true}`,
			http.StatusBadRequest, "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := newStack(t, stackOpts{})
			resp := s.postRefsRaw(tc.body)
			if resp.StatusCode != tc.wantCode {
				t.Fatalf("status = %d, want %d, body = %s", resp.StatusCode, tc.wantCode, readAll(t, resp))
			}
			if tc.wantCode == http.StatusBadRequest {
				if msg := errorOf(t, resp).Message; tc.wantMsg != "" && !strings.Contains(msg, tc.wantMsg) {
					t.Errorf("400 message = %q, want to contain %q", msg, tc.wantMsg)
				}
			} else {
				resp.Body.Close()
			}
			got := s.syncedTo()
			if tc.advances {
				if got == nil || *got != advance {
					t.Fatalf("coverage = %v, want %d for the accepted present-empty batch", got, advance)
				}
			} else if got != nil {
				t.Fatalf("coverage advanced to %d on a rejected body; the rejection must precede any commit", *got)
			}
		})
	}
}

// TestMutationDecoderStrictnessAcrossSchemas confirms the strict decoder
// is shared, not refs-only: truncate and manifest bodies reject an unknown field
// (at the top level and, for manifest, inside a nested source) and a trailing
// second value the same way, each before the mutation they guard.
func TestMutationDecoderStrictnessAcrossSchemas(t *testing.T) {
	truncatePath := "/bloar/v1/heads/" + testHead + "/truncate"
	manifestPath := "/bloar/v1/heads/" + testHead + "/manifest"

	post := func(t *testing.T, path, body string) *http.Response {
		t.Helper()
		s := newStack(t, stackOpts{})
		return s.do("POST", path, strings.NewReader(body), withAuth)
	}

	cases := []struct {
		name    string
		path    string
		body    string
		wantMsg string
	}{
		{"truncate unknown field", truncatePath, `{"slot":9,"confirm":"all","bogus":1}`, "bogus"},
		{"truncate trailing value", truncatePath, `{"slot":9,"confirm":"all"} {"x":1}`, ""},
		{"manifest unknown top field", manifestPath,
			`{"manifest":{"v":1,"head":"all","sources":[],"prev":null},"confirm":"all","expected_head_root":"x","bogus":1}`, "bogus"},
		{"manifest unknown nested source field", manifestPath,
			`{"manifest":{"v":1,"head":"all","sources":[{"type":"inbox-events","address":"0x00","from_block":0,"bogus":1}],"prev":null},"confirm":"all","expected_head_root":"x"}`, "bogus"},
		{"manifest trailing value", manifestPath,
			`{"manifest":{"v":1,"head":"all","sources":[],"prev":null},"confirm":"all","expected_head_root":"x"} {"x":1}`, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp := post(t, tc.path, tc.body)
			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400, body = %s", resp.StatusCode, readAll(t, resp))
			}
			if msg := errorOf(t, resp).Message; tc.wantMsg != "" && !strings.Contains(msg, tc.wantMsg) {
				t.Errorf("400 message = %q, want to contain %q", msg, tc.wantMsg)
			}
		})
	}
}

// TestManifestPrevPresenceRule is the manifest half of the presence sweep:
// the spec's "bafy.. | null" makes an explicit null the genesis manifest, so an
// omitted prev is a mistake (400) and an explicit null bootstraps the chain (200),
// two states a plain *string collapsed into one. A present but empty string is
// neither and is its own 400.
func TestManifestPrevPresenceRule(t *testing.T) {
	// Omitted prev: rejected before the chain is touched.
	t.Run("prev omitted", func(t *testing.T) {
		s := newStack(t, stackOpts{})
		body := map[string]any{
			"manifest":           map[string]any{"v": 1, "head": testHead, "sources": []map[string]any{inboxSourceJSON(0, nil)}},
			"confirm":            testHead,
			"expected_head_root": s.docEntry().Root,
		}
		resp := s.postManifest(body)
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("omitted prev: status = %d, want 400, body = %s", resp.StatusCode, readAll(t, resp))
		}
		if msg := errorOf(t, resp).Message; !strings.Contains(msg, "prev is required") {
			t.Errorf("400 message = %q, want to mention prev is required", msg)
		}
		if got := s.headEntry().Manifest; got != "" {
			t.Errorf("a manifest chain was bootstrapped by a body missing prev: %q", got)
		}
	})

	// Present but empty string: neither a CID nor the genesis null.
	t.Run("prev empty string", func(t *testing.T) {
		s := newStack(t, stackOpts{})
		body := map[string]any{
			"manifest":           map[string]any{"v": 1, "head": testHead, "sources": []map[string]any{inboxSourceJSON(0, nil)}, "prev": ""},
			"confirm":            testHead,
			"expected_head_root": s.docEntry().Root,
		}
		resp := s.postManifest(body)
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("empty-string prev: status = %d, want 400, body = %s", resp.StatusCode, readAll(t, resp))
		}
		if msg := errorOf(t, resp).Message; !strings.Contains(msg, "empty string") {
			t.Errorf("400 message = %q, want to mention the empty string", msg)
		}
	})

	// Explicit null prev: the genesis bootstrap. setManifest sends prev: nil, which
	// marshals to an explicit JSON null.
	t.Run("prev explicit null bootstraps genesis", func(t *testing.T) {
		s := newStack(t, stackOpts{})
		tip := s.setManifest([]map[string]any{inboxSourceJSON(0, nil)}, nil)
		if tip == "" {
			t.Fatal("genesis manifest with an explicit null prev was not accepted")
		}
	})
}

// postGenesisSource posts a genesis manifest carrying exactly one source and
// returns the response, so a table can vary that source's field presence. It binds
// to the head's current root and sends an explicit null prev, the two endpoint
// requires of a genesis (spec 10.5).
func (s *stack) postGenesisSource(src map[string]any) *http.Response {
	s.t.Helper()
	body := map[string]any{
		"manifest":           map[string]any{"v": 1, "head": testHead, "sources": []map[string]any{src}, "prev": nil},
		"confirm":            testHead,
		"expected_head_root": s.docEntry().Root,
	}
	return s.postManifest(body)
}

// TestManifestSourcePresenceRule is the manifest-source half of the presence
// sweep. from_block is required and presence-aware -- an
// explicit 0 is a real schedule start, an absent or null key is a 400.
// until_block is absent for open-ended, a value for bounded, and never an explicit
// null. A type-specific key PRESENT where the type forbids it -- senders on
// inbox-events, topic on blob-txs, null or otherwise -- is a 400 rather than being
// canonicalized into absence. Each rejection lands before the chain is
// touched: a fresh stack per case makes "no chain was created" checkable.
func TestManifestSourcePresenceRule(t *testing.T) {
	inbox := func(extra map[string]any) map[string]any {
		src := map[string]any{"type": "inbox-events", "address": testInboxAddr, "topic": testTopic0, "from_block": 0}
		for k, v := range extra {
			src[k] = v
		}
		return src
	}
	blobTxs := func(extra map[string]any) map[string]any {
		src := map[string]any{"type": "blob-txs", "address": testEOA, "senders": []string{testSender}, "from_block": 0}
		for k, v := range extra {
			src[k] = v
		}
		return src
	}
	// deleteKey returns the source with key removed, for the "omitted" cases a map
	// literal cannot express.
	without := func(src map[string]any, key string) map[string]any {
		delete(src, key)
		return src
	}

	cases := []struct {
		name     string
		source   map[string]any
		wantCode int
		wantMsg  string
	}{
		{"from_block explicit zero accepted", inbox(nil), http.StatusOK, ""},
		{"from_block omitted", without(inbox(nil), "from_block"), http.StatusBadRequest, "from_block is required"},
		{"from_block null", inbox(map[string]any{"from_block": nil}), http.StatusBadRequest, "from_block is required"},

		{"until_block omitted is open-ended", inbox(nil), http.StatusOK, ""},
		{"until_block value accepted", inbox(map[string]any{"until_block": 100}), http.StatusOK, ""},
		{"until_block null", inbox(map[string]any{"until_block": nil}), http.StatusBadRequest, "until_block is present but null"},

		{"inbox with senders present", inbox(map[string]any{"senders": []string{testSender}}),
			http.StatusBadRequest, "senders is a blob-txs field"},
		{"inbox with senders null", inbox(map[string]any{"senders": nil}),
			http.StatusBadRequest, "senders is a blob-txs field"},
		{"blob-txs with topic present", blobTxs(map[string]any{"topic": testTopic0}),
			http.StatusBadRequest, "topic is an inbox-events field"},
		{"blob-txs with topic null", blobTxs(map[string]any{"topic": nil}),
			http.StatusBadRequest, "topic is an inbox-events field"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := newStack(t, stackOpts{})
			resp := s.postGenesisSource(tc.source)
			if resp.StatusCode != tc.wantCode {
				t.Fatalf("status = %d, want %d, body = %s", resp.StatusCode, tc.wantCode, readAll(t, resp))
			}
			if tc.wantCode == http.StatusBadRequest {
				if msg := errorOf(t, resp).Message; tc.wantMsg != "" && !strings.Contains(msg, tc.wantMsg) {
					t.Errorf("400 message = %q, want to contain %q", msg, tc.wantMsg)
				}
				if got := s.headEntry().Manifest; got != "" {
					t.Errorf("a rejected source still created a manifest chain: %q", got)
				}
			} else {
				resp.Body.Close()
				if got := s.headEntry().Manifest; got == "" {
					t.Error("an accepted genesis did not create a manifest chain")
				}
			}
		})
	}
}
