package server_test

import (
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/blobarchive/bloar/server"
)

// TestPublicationContractCompatibility pins the wire-level activation rule:
// new fields are appended and omitted, so a legacy finalized claim retains its
// exact bytes. A mutable claim is explicit, revisioned, and bounded.
func TestPublicationContractCompatibility(t *testing.T) {
	synced := uint64(12)
	legacy := server.Unsigned{
		V: server.LegacyDocVersion, Net: "compat", UpdatedAt: "2026-07-22T00:00:00Z",
		Heads: []server.HeadEntry{{
			Name: "all", Root: "bafyroot", OriginSlot: 8, SyncedTo: &synced,
			SegBits: 3, FanoutBits: 2, DirDepth: 0,
		}},
	}
	got, err := legacy.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	want := `{"v":1,"net":"compat","updated_at":"2026-07-22T00:00:00Z","heads":[{"name":"all","root":"bafyroot","origin_slot":8,"synced_to":12,"seg_bits":3,"fanout_bits":2,"dir_depth":0}]}`
	if string(got) != want {
		t.Fatalf("legacy canonical bytes changed:\n got %s\nwant %s", got, want)
	}

	v2Revision := uint64(1)
	v2 := server.Unsigned{
		V: server.DocVersion, Net: "compat", UpdatedAt: "2026-07-22T00:00:01Z",
		Heads: []server.HeadEntry{}, Revision: &v2Revision,
	}
	v2Bytes, err := v2.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	v2Want := `{"v":2,"net":"compat","updated_at":"2026-07-22T00:00:01Z","heads":[],"revision":1}`
	if string(v2Bytes) != v2Want {
		t.Fatalf("v2 canonical bytes changed:\n got %s\nwant %s", v2Bytes, v2Want)
	}

	revision, start, handoffSynced, sourceFinalized := uint64(1), uint64(9), uint64(10), uint64(10)
	handoffRoot := "bafyreiadtfhdbbzr2jcw33xkx4xsvhurwfrjy2inxi2ozogubkxmio376i"
	beaconRoot := "0x" + strings.Repeat("01", 32)
	mutable := server.Doc{Unsigned: server.Unsigned{
		V: server.DocVersion, Net: "compat", UpdatedAt: "2026-07-22T00:00:01Z", Revision: &revision,
		Heads: []server.HeadEntry{{Name: "all", Root: handoffRoot, OriginSlot: 8, SyncedTo: &handoffSynced,
			SegBits: 3, FanoutBits: 2}, {
			Name: "unfinalized", Root: "bafyroot2", OriginSlot: start, SyncedTo: &synced,
			SegBits: 3, FanoutBits: 2, DirDepth: 0, Kind: server.UnfinalizedMutable, WindowStart: &start,
			SourceHeadRoot: beaconRoot, SourceFinalizedSlot: &sourceFinalized, SourceFinalizedRoot: beaconRoot,
			HandoffHead: "all", HandoffRoot: handoffRoot, HandoffSyncedTo: &handoffSynced,
		}},
	}, Pubkey: "present", Signature: "present"}
	if err := mutable.ValidateContract(); err != nil {
		t.Fatalf("valid mutable contract: %v", err)
	}
	first, err := mutable.Unsigned.CanonicalDigest()
	if err != nil {
		t.Fatal(err)
	}
	mutable.Heads[1].Root = "bafyother"
	second, err := mutable.Unsigned.CanonicalDigest()
	if err != nil {
		t.Fatal(err)
	}
	if first == second {
		t.Fatal("different claims at one revision have the same canonical digest")
	}
}

func TestPublicationContractRejectsInvalidMutableClaims(t *testing.T) {
	synced, start, revision, handoffSynced, sourceFinalized := uint64(12), uint64(9), uint64(1), uint64(10), uint64(10)
	handoffRoot := "bafyreiadtfhdbbzr2jcw33xkx4xsvhurwfrjy2inxi2ozogubkxmio376i"
	beaconRoot := "0x" + strings.Repeat("01", 32)
	base := server.Doc{Unsigned: server.Unsigned{
		V: server.DocVersion, Net: "compat", UpdatedAt: "2026-07-22T00:00:01Z", Revision: &revision,
		Heads: []server.HeadEntry{{Name: "all", Root: handoffRoot, OriginSlot: 8, SyncedTo: &handoffSynced,
			SegBits: 3, FanoutBits: 2}, {
			Name: "tip", Root: "bafyroot", OriginSlot: start, SyncedTo: &synced,
			SegBits: 3, FanoutBits: 2, Kind: server.UnfinalizedMutable, WindowStart: &start,
			SourceHeadRoot: beaconRoot, SourceFinalizedSlot: &sourceFinalized, SourceFinalizedRoot: beaconRoot,
			HandoffHead: "all", HandoffRoot: handoffRoot, HandoffSyncedTo: &handoffSynced,
		}},
	}, Pubkey: "present", Signature: "present"}

	tests := []struct {
		name string
		edit func(*server.Doc)
	}{
		{"revision zero", func(d *server.Doc) { z := uint64(0); d.Revision = &z }},
		{"unsigned", func(d *server.Doc) { d.Pubkey, d.Signature = "", "" }},
		{"missing revision", func(d *server.Doc) { d.Revision = nil }},
		{"missing window", func(d *server.Doc) { d.Heads[1].WindowStart = nil }},
		{"window mismatch", func(d *server.Doc) { x := start + 1; d.Heads[1].WindowStart = &x }},
		{"empty snapshot", func(d *server.Doc) { d.Heads[1].SyncedTo = nil }},
		{"backwards window", func(d *server.Doc) { x := synced + 1; d.Heads[1].WindowStart = &x; d.Heads[1].OriginSlot = x }},
		{"manifest", func(d *server.Doc) { d.Heads[1].Manifest = "bafymanifest" }},
		{"unknown kind", func(d *server.Doc) { d.Heads[1].Kind = "future" }},
		{"duplicate name", func(d *server.Doc) { d.Heads = append(d.Heads, d.Heads[1]) }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := base
			d.Heads = append([]server.HeadEntry(nil), base.Heads...)
			tt.edit(&d)
			if err := d.ValidateContract(); err == nil {
				t.Fatal("invalid contract was accepted")
			}
		})
	}
}

// TestHeadsDoc covers the document of spec 8 as served over the HTTPS channel.
func TestHeadsDoc(t *testing.T) {
	s := newStack(t, stackOpts{multiaddrs: []string{"/dns4/archive.example.org/tcp/4001/p2p/12D3KooWTest"}})
	vhs := s.put(makeBlob(1))
	s.refs([]map[string]any{row(9, vhs[0])}, 12)

	resp := s.get("/bloar/v1/heads")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if got := resp.Header.Get("Cache-Control"); got != "public, max-age=12" {
		t.Errorf("Cache-Control = %q, want public, max-age=12", got)
	}

	var doc server.Doc
	decode(t, resp, &doc)

	if doc.V != server.LegacyDocVersion {
		t.Errorf("v = %d, want %d", doc.V, server.LegacyDocVersion)
	}
	if doc.Net != testNet {
		t.Errorf("net = %q, want %q", doc.Net, testNet)
	}
	if doc.UpdatedAt == "" {
		t.Error("updated_at is empty")
	}
	if got := doc.Multiaddrs; len(got) != 1 || !strings.HasPrefix(got[0], "/dns4/") {
		t.Errorf("multiaddrs = %v, want the configured one", got)
	}
	if len(doc.Heads) != 1 {
		t.Fatalf("heads has %d entries, want 1", len(doc.Heads))
	}

	entry := doc.Heads[0]
	if entry.Name != testHead {
		t.Errorf("heads[0].name = %q, want %q", entry.Name, testHead)
	}
	if entry.OriginSlot != testOrigin {
		t.Errorf("heads[0].origin_slot = %d, want %d", entry.OriginSlot, testOrigin)
	}
	if entry.SegBits != testSegBits || entry.FanoutBits != testFanout {
		t.Errorf("heads[0] seg_bits/fanout_bits = %d/%d, want %d/%d",
			entry.SegBits, entry.FanoutBits, testSegBits, testFanout)
	}
	if entry.SyncedTo == nil || *entry.SyncedTo != 12 {
		t.Errorf("heads[0].synced_to = %v, want 12", entry.SyncedTo)
	}
	if !strings.HasPrefix(entry.Root, "bafy") {
		t.Errorf("heads[0].root = %q, want a dag-cbor CIDv1", entry.Root)
	}
	// Coverage stops at slot 12, inside the 8-slot window [8, 15], so nothing
	// has sealed and there is no directory yet.
	if entry.DirDepth != 0 {
		t.Errorf("heads[0].dir_depth = %d, want 0: no window is fully covered at synced_to 12", entry.DirDepth)
	}

	// Unsigned by default: no key was configured.
	if doc.Pubkey != "" || doc.Signature != "" {
		t.Errorf("unsigned stack published pubkey=%q signature=%q", doc.Pubkey, doc.Signature)
	}

	// Covering the window's last slot seals it, which the document must show:
	// dir_depth is read off the engine, not remembered.
	s.refs(nil, 15)
	if got := s.headEntry(); got.DirDepth != 1 {
		t.Errorf("dir_depth = %d after window [8, 15] sealed, want 1", got.DirDepth)
	}
}

// TestHeadsDocEmptyHead covers the two nullable-looking fields: an empty head's
// synced_to is null, and multiaddrs is absent rather than null when nothing is
// configured. Both matter to the signature, since they change the bytes.
func TestHeadsDocEmptyHead(t *testing.T) {
	s := newStack(t, stackOpts{})

	resp := s.get("/bloar/v1/heads")
	defer resp.Body.Close()
	raw := readAll(t, resp)

	var doc server.Doc
	if err := json.Unmarshal([]byte(raw), &doc); err != nil {
		t.Fatalf("decoding document: %v", err)
	}
	if doc.Heads[0].SyncedTo != nil {
		t.Errorf("synced_to = %d on an empty head, want null", *doc.Heads[0].SyncedTo)
	}
	if strings.Contains(raw, "multiaddrs") {
		t.Errorf("document carries a multiaddrs key with none configured: %s", raw)
	}
	if !strings.Contains(raw, `"synced_to":null`) {
		t.Errorf("an empty head's synced_to must be an explicit null: %s", raw)
	}
}

// TestHeadsDocSignature verifies the signature the way a phase-8 follower must:
// decode the served document, re-marshal its unsigned half, and check the
// ed25519 signature over exactly those bytes. Nothing here calls the server's
// own Verify -- a canonicalization that only agrees with itself is not one.
func TestHeadsDocSignature(t *testing.T) {
	s := newStack(t, stackOpts{sign: true})
	vhs := s.put(makeBlob(1))
	s.refs([]map[string]any{row(9, vhs[0])}, 12)

	resp := s.get("/bloar/v1/heads")
	defer resp.Body.Close()
	var doc server.Doc
	decode(t, resp, &doc)

	if doc.Pubkey == "" || doc.Signature == "" {
		t.Fatal("a signing key was configured but the document is unsigned")
	}

	// The published key is the configured one.
	wantPub := s.key.Public().(ed25519.PublicKey)
	if doc.Pubkey != hex.EncodeToString(wantPub) {
		t.Errorf("pubkey = %s, want %s", doc.Pubkey, hex.EncodeToString(wantPub))
	}

	pub, err := hex.DecodeString(doc.Pubkey)
	if err != nil {
		t.Fatalf("pubkey is not hex: %v", err)
	}
	sig, err := hex.DecodeString(doc.Signature)
	if err != nil {
		t.Fatalf("signature is not hex: %v", err)
	}
	// The canonical bytes: encoding/json's marshalling of the unsigned half,
	// reconstructed by a verifier that has only the served document.
	canonical, err := json.Marshal(doc.Unsigned)
	if err != nil {
		t.Fatalf("re-marshalling the unsigned document: %v", err)
	}
	if !ed25519.Verify(ed25519.PublicKey(pub), canonical, sig) {
		t.Fatalf("signature does not verify over the re-marshalled unsigned document:\n%s", canonical)
	}

	t.Run("a tampered document does not verify", func(t *testing.T) {
		// The signature covers the roots. A relay that swaps one for a root of
		// its own is what this is for.
		tampered := doc.Unsigned
		tampered.Heads = append([]server.HeadEntry(nil), doc.Heads...)
		tampered.Heads[0].Root = "bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi"

		canonical, err := json.Marshal(tampered)
		if err != nil {
			t.Fatalf("marshalling: %v", err)
		}
		if ed25519.Verify(ed25519.PublicKey(pub), canonical, sig) {
			t.Error("a document with a swapped root verified against the original signature")
		}
	})

	t.Run("Verify agrees", func(t *testing.T) {
		// The reference implementation must accept what the independent check
		// above accepted, or one of the two is wrong.
		if err := doc.Verify(); err != nil {
			t.Errorf("Doc.Verify: %v", err)
		}
	})
}

// TestHeadsDocTracksMutations is the atomicity requirement of spec 8: the
// served document names the current roots. A document rebuilt anywhere but
// under the mutation's own lock would fail this or, worse, pass it only
// sometimes.
func TestHeadsDocTracksMutations(t *testing.T) {
	s := newStack(t, stackOpts{sign: true})
	vhs := s.put(makeBlob(1), makeBlob(2))

	before := s.headEntry()
	if before.SyncedTo != nil {
		t.Fatalf("synced_to = %d before any batch, want null", *before.SyncedTo)
	}

	resp := s.postJSON("/bloar/v1/heads/"+testHead+"/refs", map[string]any{
		"rows": []map[string]any{row(9, vhs[0])}, "synced_to": 12,
	}, withAuth)
	defer resp.Body.Close()
	var applied refsBody
	decode(t, resp, &applied)

	// The root the mutation returned is the root the document publishes: not an
	// older one, and not one nobody was ever served.
	after := s.headEntry()
	if after.Root != applied.Root {
		t.Errorf("document root = %s, the batch produced %s", after.Root, applied.Root)
	}
	if after.Root == before.Root {
		t.Error("the document root did not change after a batch that changed it")
	}
	if after.SyncedTo == nil || *after.SyncedTo != 12 {
		t.Errorf("document synced_to = %v, want 12", after.SyncedTo)
	}

	// And it is still signed, over the new bytes.
	full := s.doc()
	if err := full.Verify(); err != nil {
		t.Errorf("the republished document does not verify: %v", err)
	}

	// A no-op replay must leave the document alone, including updated_at: it
	// changed nothing, and a document that churns on replays would have
	// followers adopting "new" states forever.
	s.refs([]map[string]any{row(9, vhs[0])}, 12)
	if replayed := s.doc(); replayed.UpdatedAt != full.UpdatedAt || replayed.Heads[0].Root != full.Heads[0].Root {
		t.Errorf("a no-op replay republished the document: %s -> %s", full.UpdatedAt, replayed.UpdatedAt)
	}
}

// TestHeadsDocMaxPutBlobs covers the archive's advertised POST /bloar/v1/blobs
// limit (spec 7.2): it is on the document an indexer reads at startup, it is the
// value the server enforces, and it is outside the signature -- an indexer checks
// it, no follower does.
func TestHeadsDocMaxPutBlobs(t *testing.T) {
	s := newStack(t, stackOpts{sign: true, maxPutBlobs: 7})

	doc := s.doc()
	if doc.MaxPutBlobs != 7 {
		t.Errorf("max_put_blobs = %d, want the configured 7", doc.MaxPutBlobs)
	}
	// Still signed, and the field is not in the canonical bytes a follower
	// reconstructs: the signature covers roots and net, not this writer-facing hint.
	if err := doc.Verify(); err != nil {
		t.Errorf("Verify: %v", err)
	}
	canonical, err := json.Marshal(doc.Unsigned)
	if err != nil {
		t.Fatalf("marshalling the unsigned half: %v", err)
	}
	if strings.Contains(string(canonical), "max_put_blobs") {
		t.Errorf("max_put_blobs leaked into the signed canonical bytes: %s", canonical)
	}

	// Unset takes the spec default the server also floors to, so the document
	// never advertises a limit the archive does not enforce.
	if def := newStack(t, stackOpts{}).doc(); def.MaxPutBlobs != 64 {
		t.Errorf("default max_put_blobs = %d, want the spec default 64", def.MaxPutBlobs)
	}
}

// TestHeadEntryEndpoint covers GET /bloar/v1/heads/{head} (spec 7.2), which
// must be the same entry the document carries.
func TestHeadEntryEndpoint(t *testing.T) {
	s := newStack(t, stackOpts{})
	vhs := s.put(makeBlob(1))
	s.refs([]map[string]any{row(9, vhs[0])}, 12)

	resp := s.get("/bloar/v1/heads/" + testHead)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if got := resp.Header.Get("Cache-Control"); got != "public, max-age=12" {
		t.Errorf("Cache-Control = %q, want public, max-age=12", got)
	}

	var entry server.HeadEntry
	decode(t, resp, &entry)

	// Compared as rendered bytes, which is what a client sees; the struct holds
	// a *uint64, and comparing those would compare addresses.
	got, err := json.Marshal(entry)
	if err != nil {
		t.Fatalf("marshalling the entry: %v", err)
	}
	want, err := json.Marshal(s.doc().Heads[0])
	if err != nil {
		t.Fatalf("marshalling the document entry: %v", err)
	}
	if string(got) != string(want) {
		t.Errorf("the per-head entry disagrees with the document:\n got %s\nwant %s", got, want)
	}
}

// doc fetches the publication document.
func (s *stack) doc() server.Doc {
	s.t.Helper()
	resp := s.get("/bloar/v1/heads")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		s.t.Fatalf("GET /bloar/v1/heads: status = %d", resp.StatusCode)
	}
	var doc server.Doc
	decode(s.t, resp, &doc)
	return doc
}

// headEntry fetches the test head's entry from the document.
func (s *stack) headEntry() server.HeadEntry {
	s.t.Helper()
	doc := s.doc()
	for _, entry := range doc.Heads {
		if entry.Name == testHead {
			return entry
		}
	}
	s.t.Fatalf("head %q is not in the publication document", testHead)
	return server.HeadEntry{}
}
