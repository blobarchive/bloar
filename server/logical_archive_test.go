package server_test

import (
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"

	"github.com/blobarchive/bloar/server"
	"github.com/blobarchive/bloar/store"
)

const testArchiveIDText = "0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20"

func TestArchiveIDCanonicalEncoding(t *testing.T) {
	id, err := server.ParseArchiveID(testArchiveIDText)
	if err != nil {
		t.Fatalf("ParseArchiveID: %v", err)
	}
	if got := id.String(); got != testArchiveIDText {
		t.Fatalf("String = %q, want %q", got, testArchiveIDText)
	}
	raw, err := json.Marshal(id)
	if err != nil {
		t.Fatalf("MarshalJSON: %v", err)
	}
	if got, want := string(raw), `"`+testArchiveIDText+`"`; got != want {
		t.Fatalf("JSON = %s, want %s", got, want)
	}
	var decoded server.ArchiveID
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("UnmarshalJSON: %v", err)
	}
	if decoded != id {
		t.Fatalf("JSON round trip = %s, want %s", decoded, id)
	}

	for _, tc := range []struct {
		name string
		text string
	}{
		{name: "empty", text: ""},
		{name: "short", text: testArchiveIDText[:62]},
		{name: "long", text: testArchiveIDText + "00"},
		{name: "uppercase", text: strings.ToUpper(testArchiveIDText)},
		{name: "non hex", text: strings.Repeat("g", 64)},
		{name: "reserved zero", text: strings.Repeat("0", 64)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := server.ParseArchiveID(tc.text); err == nil {
				t.Fatalf("ParseArchiveID(%q) succeeded", tc.text)
			}
		})
	}

	var zero server.ArchiveID
	if _, err := json.Marshal(zero); err == nil {
		t.Fatal("zero ArchiveID marshalled")
	}
	for _, raw := range []string{`null`, `1`, `"` + strings.Repeat("0", 64) + `"`} {
		if err := json.Unmarshal([]byte(raw), &decoded); err == nil {
			t.Fatalf("UnmarshalJSON(%s) succeeded", raw)
		}
	}
}

func TestLogicalArchivePublicationContract(t *testing.T) {
	id, err := server.ParseArchiveID(testArchiveIDText)
	if err != nil {
		t.Fatal(err)
	}
	revision := uint64(7)
	key := ed25519.NewKeyFromSeed([]byte(strings.Repeat("k", ed25519.SeedSize)))
	base := server.Doc{Unsigned: server.Unsigned{
		V: server.LogicalArchiveDocVersion, Net: "compat", ArchiveID: &id,
		UpdatedAt: "2026-07-22T00:00:02Z", Heads: []server.HeadEntry{}, Revision: &revision,
	}}
	canonical, err := base.Unsigned.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	wantCanonical := `{"v":3,"net":"compat","archive_id":"` + testArchiveIDText +
		`","updated_at":"2026-07-22T00:00:02Z","heads":[],"revision":7}`
	if string(canonical) != wantCanonical {
		t.Fatalf("v3 canonical bytes changed:\n got %s\nwant %s", canonical, wantCanonical)
	}
	base.Pubkey = hex.EncodeToString(key.Public().(ed25519.PublicKey))
	base.Signature = hex.EncodeToString(ed25519.Sign(key, canonical))
	if err := base.ValidateContract(); err != nil {
		t.Fatalf("valid v3 contract: %v", err)
	}
	if err := base.Verify(); err != nil {
		t.Fatalf("valid v3 signature: %v", err)
	}

	t.Run("archive identity is signed", func(t *testing.T) {
		tampered := base
		other := id
		other[0] ^= 0xff
		tampered.ArchiveID = &other
		if err := tampered.Verify(); err == nil {
			t.Fatal("archive_id mutation retained a valid signature")
		}
	})

	tests := []struct {
		name string
		edit func(*server.Doc)
	}{
		{name: "missing identity", edit: func(d *server.Doc) { d.ArchiveID = nil }},
		{name: "zero identity", edit: func(d *server.Doc) { z := server.ArchiveID{}; d.ArchiveID = &z }},
		{name: "missing revision", edit: func(d *server.Doc) { d.Revision = nil }},
		{name: "unsigned", edit: func(d *server.Doc) { d.Pubkey, d.Signature = "", "" }},
		{name: "v1 carrying identity", edit: func(d *server.Doc) { d.V = server.LegacyDocVersion }},
		{name: "v2 carrying identity", edit: func(d *server.Doc) { d.V = server.DocVersion }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			doc := base
			tc.edit(&doc)
			if err := doc.ValidateContract(); err == nil {
				t.Fatal("invalid logical-archive contract was accepted")
			}
		})
	}
}

func TestLogicalArchiveWriterPublishesVersion3(t *testing.T) {
	id, err := server.ParseArchiveID(testArchiveIDText)
	if err != nil {
		t.Fatal(err)
	}
	s := newStack(t, stackOpts{sign: true, archiveID: &id})
	doc := s.doc()
	if doc.V != server.LogicalArchiveDocVersion || doc.ArchiveID == nil || *doc.ArchiveID != id {
		t.Fatalf("publication identity/version = v%d/%v, want v%d/%s",
			doc.V, doc.ArchiveID, server.LogicalArchiveDocVersion, id)
	}
	if doc.Revision == nil || *doc.Revision == 0 {
		t.Fatalf("v3 publication revision = %v", doc.Revision)
	}
	if err := doc.ValidateContract(); err != nil {
		t.Fatalf("published contract: %v", err)
	}
	if err := doc.Verify(); err != nil {
		t.Fatalf("published signature: %v", err)
	}
}

func TestLogicalArchiveHeadsConfigFailsClosed(t *testing.T) {
	id, err := server.ParseArchiveID(testArchiveIDText)
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(t.TempDir(), store.WithPebbleLogger(quietPebble{}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := st.Close(); err != nil {
			t.Errorf("closing store: %v", err)
		}
	})
	roots := server.NewRootStore(st.KV())
	key := ed25519.NewKeyFromSeed([]byte(strings.Repeat("k", ed25519.SeedSize)))

	if _, err := server.NewHeads(server.HeadsConfig{Net: testNet, Roots: roots, ArchiveID: &id}); err == nil {
		t.Fatal("HeadsConfig accepted archive identity without a signing key")
	}
	zero := server.ArchiveID{}
	if _, err := server.NewHeads(server.HeadsConfig{
		Net: testNet, Roots: roots, ArchiveID: &zero, SigningKey: key,
	}); err == nil {
		t.Fatal("HeadsConfig accepted the reserved zero archive identity")
	}
}
