package server_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"

	"github.com/blobarchive/bloar/server"
)

// A fake inbox contract and event topic for the manifest tests: the mechanics of
// the CAS and the encoding do not depend on the head being a real chain head, so
// these exercise the endpoint over the test stack's "all" head.
const (
	testInboxAddr = "0x1c479675ad559dc151f6ec7ed3fbf8cee79582b6"
	testTopic0    = "0x7394f4a19a13c7b92b5bb71033245305946ef78452f7b4986ac1390b5df4ebd7"
	testEOA       = "0x5050505050505050505050505050505050505050"
	testSender    = "0xa4b0000000000000000000000000000000000000"
)

// inboxSourceJSON builds a valid inbox-events source body.
func inboxSourceJSON(from uint64, until *uint64) map[string]any {
	s := map[string]any{"type": "inbox-events", "address": testInboxAddr, "topic": testTopic0, "from_block": from}
	if until != nil {
		s["until_block"] = *until
	}
	return s
}

// postManifest posts a manifest body and returns the response.
func (s *stack) postManifest(body map[string]any, opts ...reqOpt) *http.Response {
	return s.postJSON("/bloar/v1/heads/"+testHead+"/manifest", body, append([]reqOpt{withAuth}, opts...)...)
}

// setManifest posts a genesis-or-advance manifest and returns the new tip,
// failing the test if it is not accepted. It binds to the head's current root
// , which the manifest POST requires.
func (s *stack) setManifest(sources []map[string]any, prev any) string {
	s.t.Helper()
	body := map[string]any{
		"manifest":           map[string]any{"v": 1, "head": testHead, "sources": sources, "prev": prev},
		"confirm":            testHead,
		"expected_head_root": s.docEntry().Root,
	}
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

// docEntry fetches the head's entry from GET /bloar/v1/heads.
func (s *stack) docEntry() server.HeadEntry {
	s.t.Helper()
	resp := s.get("/bloar/v1/heads")
	defer resp.Body.Close()
	var doc server.Doc
	decode(s.t, resp, &doc)
	for _, e := range doc.Heads {
		if e.Name == testHead {
			return e
		}
	}
	s.t.Fatalf("head %q not in the document", testHead)
	return server.HeadEntry{}
}

// TestCorruptManifestBlockRefusedOnRead is rider B: GET manifest reads the
// tip block through the validating store, so a corrupt manifest block is refused
// with 500 and counted under bloar_store_corrupt_reads_total, the same mapping the
// beacon read paths use.
func TestCorruptManifestBlockRefusedOnRead(t *testing.T) {
	s := newStack(t, stackOpts{instrument: true})
	genesis := s.setManifest([]map[string]any{inboxSourceJSON(0, nil)}, nil)

	// The manifest block is honest here, so the GET is a 200.
	resp := s.get("/bloar/v1/heads/" + testHead + "/manifest")
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		t.Fatalf("GET manifest before corruption: status = %d", resp.StatusCode)
	}
	resp.Body.Close()

	// Corrupt the tip block in place: wrong bytes under its key.
	tip, err := cid.Decode(genesis)
	if err != nil {
		t.Fatalf("decoding tip CID %q: %v", genesis, err)
	}
	ctx := t.Context()
	if err := s.store.Blocks().DeleteBlock(ctx, tip); err != nil {
		t.Fatalf("deleting the manifest block: %v", err)
	}
	bad, err := blocks.NewBlockWithCid([]byte("corrupt manifest bytes under the tip key"), tip)
	if err != nil {
		t.Fatalf("framing corrupt manifest block: %v", err)
	}
	if err := s.store.Blocks().Put(ctx, bad); err != nil {
		t.Fatalf("storing corrupt manifest block: %v", err)
	}

	resp = s.get("/bloar/v1/heads/" + testHead + "/manifest")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("GET corrupt manifest: status = %d, want 500; body = %s", resp.StatusCode, readAll(t, resp))
	}
	if got := corruptReads(t, s.metrics, testHead); got != 1 {
		t.Fatalf("bloar_store_corrupt_reads_total{head=%q} = %v, want 1", testHead, got)
	}
}

// TestManifestCASBootstrapAndAdvance covers the genesis bootstrap, an advance,
// and the two stale-prev conflicts (spec 7.2, 10.5).
func TestManifestCASBootstrapAndAdvance(t *testing.T) {
	s := newStack(t, stackOpts{})

	// A head with no chain omits the manifest field and 404s the manifest GET.
	if e := s.docEntry(); e.Manifest != "" {
		t.Fatalf("fresh head has manifest %q, want none", e.Manifest)
	}
	if resp := s.get("/bloar/v1/heads/" + testHead + "/manifest"); resp.StatusCode != http.StatusNotFound {
		resp.Body.Close()
		t.Fatalf("GET manifest on a chainless head: status = %d, want 404", resp.StatusCode)
	}

	// Genesis: prev null, from no-tip to tip.
	genesis := s.setManifest([]map[string]any{inboxSourceJSON(0, nil)}, nil)
	if e := s.docEntry(); e.Manifest != genesis {
		t.Fatalf("after genesis, doc manifest = %q, want %q", e.Manifest, genesis)
	}

	// GET returns the decoded schedule and the tip CID.
	resp := s.get("/bloar/v1/heads/" + testHead + "/manifest")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET manifest: status = %d, body = %s", resp.StatusCode, readAll(t, resp))
	}
	var got struct {
		Manifest struct {
			Sources []struct {
				Type      string `json:"type"`
				Address   string `json:"address"`
				Topic     string `json:"topic"`
				FromBlock uint64 `json:"from_block"`
			} `json:"sources"`
			Prev *string `json:"prev"`
		} `json:"manifest"`
		CID string `json:"cid"`
	}
	decode(t, resp, &got)
	if got.CID != genesis {
		t.Errorf("GET manifest cid = %q, want %q", got.CID, genesis)
	}
	if got.Manifest.Prev != nil {
		t.Errorf("genesis prev = %v, want null", *got.Manifest.Prev)
	}
	if len(got.Manifest.Sources) != 1 || got.Manifest.Sources[0].Type != "inbox-events" ||
		got.Manifest.Sources[0].Address != testInboxAddr {
		t.Errorf("GET manifest sources = %+v", got.Manifest.Sources)
	}

	// Advance: prev is the genesis tip; close the inbox source and add a blob-txs era.
	until := uint64(21_000_000)
	advanced := s.setManifest([]map[string]any{
		inboxSourceJSON(0, &until),
		{"type": "blob-txs", "address": testEOA, "senders": []string{testSender}, "from_block": 21_000_001},
	}, genesis)
	if advanced == genesis {
		t.Fatal("advance produced the same CID as genesis")
	}
	if e := s.docEntry(); e.Manifest != advanced {
		t.Fatalf("after advance, doc manifest = %q, want %q", e.Manifest, advanced)
	}

	// Stale prev: the genesis tip is no longer current. A valid expected_head_root
	// so the request reaches the prev CAS rather than stopping at the 400 for a
	// missing binding.
	body := map[string]any{
		"manifest":           map[string]any{"v": 1, "head": testHead, "sources": []map[string]any{inboxSourceJSON(0, nil)}, "prev": genesis},
		"confirm":            testHead,
		"expected_head_root": s.docEntry().Root,
	}
	if resp := s.postManifest(body); resp.StatusCode != http.StatusConflict {
		resp.Body.Close()
		t.Fatalf("advance from a stale tip: status = %d, want 409", resp.StatusCode)
	} else {
		resp.Body.Close()
	}

	// A genesis bootstrap when a tip already exists is the same conflict.
	body["manifest"].(map[string]any)["prev"] = nil
	if resp := s.postManifest(body); resp.StatusCode != http.StatusConflict {
		resp.Body.Close()
		t.Fatalf("re-bootstrap over an existing tip: status = %d, want 409", resp.StatusCode)
	} else {
		resp.Body.Close()
	}
}

// TestManifest400Taxonomy walks the malformed-request cases spec 7.2 answers 400.
func TestManifest400Taxonomy(t *testing.T) {
	s := newStack(t, stackOpts{})
	good := []map[string]any{inboxSourceJSON(0, nil)}

	cases := []struct {
		name string
		body map[string]any
	}{
		{"missing manifest", map[string]any{"confirm": testHead}},
		{"missing confirm", map[string]any{"manifest": map[string]any{"v": 1, "head": testHead, "sources": good, "prev": nil}}},
		{"wrong confirm", map[string]any{"manifest": map[string]any{"v": 1, "head": testHead, "sources": good, "prev": nil}, "confirm": "wrong"}},
		{"head mismatch", map[string]any{"manifest": map[string]any{"v": 1, "head": "other", "sources": good, "prev": nil}, "confirm": testHead}},
		{"bad prev cid", map[string]any{"manifest": map[string]any{"v": 1, "head": testHead, "sources": good, "prev": "not-a-cid"}, "confirm": testHead}},
		{
			"blob-txs empty senders",
			map[string]any{"manifest": map[string]any{"v": 1, "head": testHead, "prev": nil,
				"sources": []map[string]any{{"type": "blob-txs", "address": testEOA, "senders": []string{}, "from_block": 0}}}, "confirm": testHead},
		},
		{
			"unknown source type",
			map[string]any{"manifest": map[string]any{"v": 1, "head": testHead, "prev": nil,
				"sources": []map[string]any{{"type": "calldata", "address": testInboxAddr, "from_block": 0}}}, "confirm": testHead},
		},
		{
			"unknown manifest version",
			map[string]any{"manifest": map[string]any{"v": 2, "head": testHead, "sources": good, "prev": nil}, "confirm": testHead},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp := s.postManifest(tc.body)
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400, body = %s", resp.StatusCode, readAll(t, resp))
			}
			errorOf(t, resp) // asserts the beacon error shape
		})
	}

	// A body that is not JSON at all.
	resp := s.postJSON("/bloar/v1/heads/"+testHead+"/manifest", "", withAuth) // string is not an object
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("non-object body: status = %d, want 400", resp.StatusCode)
	}
}

// TestManifestRequiresAuth confirms the endpoint is behind the bearer token.
func TestManifestRequiresAuth(t *testing.T) {
	s := newStack(t, stackOpts{})
	body := map[string]any{
		"manifest": map[string]any{"v": 1, "head": testHead, "sources": []map[string]any{inboxSourceJSON(0, nil)}, "prev": nil},
		"confirm":  testHead,
	}
	resp := s.postJSON("/bloar/v1/heads/"+testHead+"/manifest", body) // no auth
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unauthenticated manifest POST: status = %d, want 401", resp.StatusCode)
	}
}

// TestManifestUnknownHead is the 404 for a head this node does not have.
func TestManifestUnknownHead(t *testing.T) {
	s := newStack(t, stackOpts{})
	if resp := s.get("/bloar/v1/heads/nope/manifest"); resp.StatusCode != http.StatusNotFound {
		resp.Body.Close()
		t.Fatalf("GET manifest of unknown head: status = %d, want 404", resp.StatusCode)
	} else {
		resp.Body.Close()
	}
	body := map[string]any{
		"manifest": map[string]any{"v": 1, "head": "nope", "sources": []map[string]any{inboxSourceJSON(0, nil)}, "prev": nil},
		"confirm":  "nope",
	}
	resp := s.postJSON("/bloar/v1/heads/nope/manifest", body, withAuth)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("POST manifest to unknown head: status = %d, want 404", resp.StatusCode)
	}
}

// TestHeadEntryManifestOmitWhenAbsent is the byte-stability check of spec 8: a
// tip-less head's entry marshals to exactly the bytes it did before this feature
// (no manifest key), and a head with a tip appends the field after dir_depth.
func TestHeadEntryManifestOmitWhenAbsent(t *testing.T) {
	st := uint64(12)
	e := server.HeadEntry{
		Name: "all", Root: "bafyroot", OriginSlot: 8, SyncedTo: &st,
		SegBits: 3, FanoutBits: 2, DirDepth: 1,
	}
	b, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	const before = `{"name":"all","root":"bafyroot","origin_slot":8,"synced_to":12,"seg_bits":3,"fanout_bits":2,"dir_depth":1}`
	if string(b) != before {
		t.Errorf("tip-less head entry changed:\n got %s\nwant %s", b, before)
	}
	if strings.Contains(string(b), "manifest") {
		t.Errorf("tip-less head entry carries a manifest key: %s", b)
	}

	e.Manifest = "bafytip"
	b, err = json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	const withTip = `{"name":"all","root":"bafyroot","origin_slot":8,"synced_to":12,"seg_bits":3,"fanout_bits":2,"dir_depth":1,"manifest":"bafytip"}`
	if string(b) != withTip {
		t.Errorf("head entry with a tip:\n got %s\nwant %s", b, withTip)
	}
}
