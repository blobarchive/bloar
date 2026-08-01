package follow_test

import (
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/ipfs/go-cid"

	"github.com/blobarchive/bloar/archive"
	"github.com/blobarchive/bloar/server"
)

// This file is the tests' publication-document workshop: a server that serves
// whatever document a test hands it, and the means to hand it one.
//
// The documents are real -- signed with the writer's own key, over the bytes
// server.Unsigned.Canonical produces, which is the recipe spec 8 fixes and the
// verifier's reference implementation. What a test controls is which claim gets
// made: an old root, a rolled-back synced_to, yesterday's timestamp. Those are
// documents a real writer would never publish, which is the point -- the
// no-regression rule exists for the case where something publishes one anyway.
//
// Blocks still move over bitswap from the real writer throughout. Only the
// claim is fabricated.

// docServer is an archive's GET /bloar/v1/heads and nothing else.
type docServer struct {
	t *testing.T

	mu   sync.Mutex
	body []byte
	fail int // if non-zero, the status to answer instead

	url string
}

func newDocServer(t *testing.T) *docServer {
	t.Helper()
	d := &docServer{t: t}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/bloar/v1/heads" {
			http.NotFound(w, r)
			return
		}
		d.mu.Lock()
		body, fail := d.body, d.fail
		d.mu.Unlock()

		if fail != 0 {
			w.WriteHeader(fail)
			return
		}
		if body == nil {
			http.Error(w, "no document", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
	}))
	t.Cleanup(srv.Close)
	d.url = srv.URL
	return d
}

func (d *docServer) set(body []byte) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.body = body
}

func (d *docServer) status(code int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.fail = code
}

// entry renders a head snapshot the way the publication document states one
// (spec 8).
func entry(info archive.Info) server.HeadEntry {
	return server.HeadEntry{
		Name:       info.Name,
		Root:       info.Root.String(),
		OriginSlot: info.OriginSlot,
		SyncedTo:   info.SyncedTo,
		SegBits:    info.SegBits,
		FanoutBits: info.FanoutBits,
		DirDepth:   info.DirDepth,
	}
}

// unsigned is the document the writer would publish right now, dated at.
func (w *writer) unsigned(at time.Time) server.Unsigned {
	return server.Unsigned{
		V:          server.LegacyDocVersion,
		Net:        testNet,
		UpdatedAt:  at.UTC().Format(time.RFC3339),
		Multiaddrs: w.host.AnnounceAddrs(),
		Heads:      []server.HeadEntry{entry(w.head.Info())},
	}
}

// sign renders u as the served document, signed by key: spec 8's canonical
// JSON, with the pubkey and signature appended.
func sign(t *testing.T, key ed25519.PrivateKey, u server.Unsigned) []byte {
	t.Helper()

	canonical, err := u.Canonical()
	if err != nil {
		t.Fatalf("Unsigned.Canonical: %v", err)
	}
	doc := server.Doc{
		Unsigned:  u,
		Pubkey:    hex.EncodeToString(key.Public().(ed25519.PublicKey)),
		Signature: hex.EncodeToString(ed25519.Sign(key, canonical)),
	}
	body, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("marshalling the document: %v", err)
	}

	// The check that the test's own document-making is the one the verifier
	// reads. A test that fabricated an unverifiable document would prove the
	// follower rejects nonsense, which is not what any of these are about.
	var back server.Doc
	if err := json.Unmarshal(body, &back); err != nil {
		t.Fatalf("unmarshalling the document: %v", err)
	}
	if err := back.Verify(); err != nil {
		t.Fatalf("the test's own document does not verify: %v", err)
	}
	return body
}

// publish signs the writer's current state and serves it from d.
func (d *docServer) publish(t *testing.T, w *writer, at time.Time) {
	t.Helper()
	d.set(sign(t, w.key, w.unsigned(at)))
}

// republishAt serves u dated at, whatever it says: the regression tests' one
// tool.
func (d *docServer) republishAt(t *testing.T, w *writer, u server.Unsigned, at time.Time) {
	t.Helper()
	u.UpdatedAt = at.UTC().Format(time.RFC3339)
	d.set(sign(t, w.key, u))
}

// withRoot returns u with the head's root and synced_to replaced: an
// old-but-signed claim.
func withRoot(u server.Unsigned, root cid.Cid, syncedTo uint64) server.Unsigned {
	heads := make([]server.HeadEntry, len(u.Heads))
	copy(heads, u.Heads)
	heads[0].Root = root.String()
	heads[0].SyncedTo = &syncedTo
	u.Heads = heads
	return u
}

// withManifest returns u with the head's manifest tip set (or omitted, for an
// undefined tip): a signed claim about which chain the head attests to.
func withManifest(u server.Unsigned, tip cid.Cid) server.Unsigned {
	heads := make([]server.HeadEntry, len(u.Heads))
	copy(heads, u.Heads)
	if tip.Defined() {
		heads[0].Manifest = tip.String()
	} else {
		heads[0].Manifest = ""
	}
	u.Heads = heads
	return u
}
