package server_test

import (
	"net/http"
	"testing"
)

// This file is the server half of the safety boundary's remediation (spec 10.5): the
// commit-time bindings a node with no L1 view can and does enforce -- the refs
// expected_manifest compare and the manifest expected_head_root generation
// compare -- exercised over the test stack's head. The append-only semantics
// themselves live in the indexer's preflight (index/chain), which these do not
// duplicate.

// refsBody posts a refs batch with the given fields and returns the response. A
// nil expectedManifest omits the field; an empty rows slice is a coverage-only
// batch, which is enough to exercise the binding without staging blobs.
func (s *stack) refsBody(syncedTo uint64, expectedManifest *string) *http.Response {
	s.t.Helper()
	body := map[string]any{"rows": []any{}, "synced_to": syncedTo}
	if expectedManifest != nil {
		body["expected_manifest"] = *expectedManifest
	}
	return s.postJSON("/bloar/v1/heads/"+testHead+"/refs", body, withAuth)
}

// refsBodyRaw posts a refs batch whose expected_manifest is set to value exactly,
// including a JSON null when value is nil. It expresses the present-but-empty cases
// the *string refsBody cannot: there, nil omits the field, so absence and null are
// indistinguishable.
func (s *stack) refsBodyRaw(syncedTo uint64, value any) *http.Response {
	s.t.Helper()
	body := map[string]any{"rows": []any{}, "synced_to": syncedTo, "expected_manifest": value}
	return s.postJSON("/bloar/v1/heads/"+testHead+"/refs", body, withAuth)
}

// TestRefsExpectedManifestPresenceRule verifies that the refs
// endpoint distinguishes an absent expected_manifest from one that is present but
// empty. On a chainless head the field is forbidden BY PRESENCE, so an explicit ""
// or an explicit null is a 400 -- not silently read as "no binding", which is what
// a plain-string decode did by collapsing absence, "" and null into one value. An
// absent field stays accepted.
func TestRefsExpectedManifestPresenceRule(t *testing.T) {
	s := newStack(t, stackOpts{})

	// A chainless head: absent is accepted, the binding genuinely not being there.
	resp := s.refsBody(uint64(testOrigin)+1, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("absent expected_manifest on a chainless head: status = %d, want 200, body = %s",
			resp.StatusCode, readAll(t, resp))
	}
	resp.Body.Close()

	// Present but empty -- "" or null -- is a 400: never a valid binding, and
	// forbidden by presence on a head with no chain.
	for _, tc := range []struct {
		name  string
		value any
	}{
		{"empty string", ""},
		{"null", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resp := s.refsBodyRaw(uint64(testOrigin)+2, tc.value)
			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("present-but-empty expected_manifest (%s): status = %d, want 400, body = %s",
					tc.name, resp.StatusCode, readAll(t, resp))
			}
			resp.Body.Close()
		})
	}
}

// TestRefsExpectedManifestBinding is the refs half of the safety boundary: a chainless head
// forbids expected_manifest, a head with a chain requires it and requires it to
// equal the tip, and a stale value is a 409 carrying the current tip (spec 10.5).
func TestRefsExpectedManifestBinding(t *testing.T) {
	s := newStack(t, stackOpts{})

	// A real CID that is not the head's manifest tip -- the head's own root -- as
	// the "present but wrong" value in both directions.
	notATip := s.docEntry().Root

	// Chainless: the field is forbidden. The head has no tip to bind to.
	resp := s.refsBody(uint64(testOrigin)+1, &notATip)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected_manifest on a chainless head: status = %d, want 400, body = %s",
			resp.StatusCode, readAll(t, resp))
	}
	resp.Body.Close()

	genesis := s.setManifest([]map[string]any{inboxSourceJSON(0, nil)}, nil)

	// With a chain, the field is required.
	resp = s.refsBody(uint64(testOrigin)+1, nil)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("refs without expected_manifest on a chained head: status = %d, want 400, body = %s",
			resp.StatusCode, readAll(t, resp))
	}
	resp.Body.Close()

	// A stale tip is a 409 that hands back the current tip so the writer can resync.
	resp = s.refsBody(uint64(testOrigin)+1, &notATip)
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("refs with a stale expected_manifest: status = %d, want 409, body = %s",
			resp.StatusCode, readAll(t, resp))
	}
	body := errorOf(t, resp)
	if body.ManifestTip != genesis {
		t.Errorf("409 manifest_tip = %q, want the current tip %q", body.ManifestTip, genesis)
	}

	// The current tip is accepted.
	resp = s.refsBody(uint64(testOrigin)+1, &genesis)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("refs bound to the current tip: status = %d, want 200, body = %s",
			resp.StatusCode, readAll(t, resp))
	}
	resp.Body.Close()
}

// TestManifestExpectedHeadRootBinding is the manifest half of the safety boundary: the POST
// requires expected_head_root, and rejects one the head has advanced past -- the
// generation binding that closes the validate-then-publish race (spec 10.5). A
// refs commit here stands in for one landing between an indexer's preflight and
// its POST.
func TestManifestExpectedHeadRootBinding(t *testing.T) {
	s := newStack(t, stackOpts{})

	genesis := s.setManifest([]map[string]any{inboxSourceJSON(0, nil)}, nil)
	rootAtPreflight := s.docEntry().Root

	// A well-formed advance missing expected_head_root is a 400: the binding is not
	// optional.
	until := uint64(21_000_000)
	advance := map[string]any{
		"manifest": map[string]any{"v": 1, "head": testHead, "prev": genesis, "sources": []map[string]any{
			inboxSourceJSON(0, &until),
			{"type": "blob-txs", "address": testEOA, "senders": []string{testSender}, "from_block": 21_000_001},
		}},
		"confirm": testHead,
	}
	if resp := s.postManifest(advance); resp.StatusCode != http.StatusBadRequest {
		resp.Body.Close()
		t.Fatalf("advance without expected_head_root: status = %d, want 400", resp.StatusCode)
	} else {
		resp.Body.Close()
	}

	// A refs commit advances the head root -- the generation the preflight read is
	// now stale. Bound to the current tip so the refs binding itself passes.
	s.refsBody(uint64(testOrigin)+1, &genesis).Body.Close()
	rootAfterCommit := s.docEntry().Root
	if rootAfterCommit == rootAtPreflight {
		t.Fatal("a refs commit did not change the head root; the generation test would be vacuous")
	}

	// The advance bound to the pre-commit root is refused: the position it was
	// validated against has moved.
	advance["expected_head_root"] = rootAtPreflight
	if resp := s.postManifest(advance); resp.StatusCode != http.StatusConflict {
		resp.Body.Close()
		t.Fatalf("advance bound to a superseded head root: status = %d, want 409", resp.StatusCode)
	} else {
		resp.Body.Close()
	}

	// Bound to the current root -- a re-run preflight -- it lands.
	advance["expected_head_root"] = rootAfterCommit
	tip := s.setManifestBody(advance)
	if tip == genesis {
		t.Fatal("advance produced the genesis tip")
	}
}

// setManifestBody posts a fully-formed manifest body and returns the new tip,
// failing the test if it is not accepted.
func (s *stack) setManifestBody(body map[string]any) string {
	s.t.Helper()
	resp := s.postManifest(body)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		s.t.Fatalf("POST manifest: status = %d, body = %s", resp.StatusCode, readAll(s.t, resp))
	}
	var out struct {
		Manifest string `json:"manifest"`
	}
	decode(s.t, resp, &out)
	return out.Manifest
}
