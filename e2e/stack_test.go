// Package e2e is spec 13.7: the end-to-end test.
//
// It runs the whole thing in one process -- a real bloard stack over a temp
// store, both indexers of spec 10, a fake beacon node and a fake parent chain
// -- and then does it again, a second bloard re-deriving the first's ALL head
// from the first's read API, to assert spec 11.5's determinism claim: the same
// blobs at the same slots under the same head parameters produce the same root
// CID, whoever built it.
//
// Nothing here is mocked below the HTTP boundary. The archive is the real
// store, catalog, ingest, archive and server packages; the blobs are real KZG
// blobs with real commitments; the indexers are the real loops, driven over
// real HTTP and real JSON-RPC. What is fake is only what is upstream of the
// system: the beacon node and the parent chain, which are the two things spec
// 13.7 says to fake and the two things that cannot be run in a unit test.
//
// There is no Docker and no network: httptest servers, a t.TempDir store, and
// indexer loops whose intervals the tests set to milliseconds.
package e2e

import (
	"crypto/ed25519"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"testing"
	"time"

	"github.com/blobarchive/bloar/archive"
	"github.com/blobarchive/bloar/catalog"
	"github.com/blobarchive/bloar/core"
	"github.com/blobarchive/bloar/index/archclient"
	"github.com/blobarchive/bloar/ingest"
	"github.com/blobarchive/bloar/schema"
	"github.com/blobarchive/bloar/server"
	"github.com/blobarchive/bloar/store"
)

// The synthetic network. Small everywhere: an origin a few slots in rather than
// mainnet's 8626176, and 8-slot windows so that twelve slots of coverage seal
// two segments and grow a directory rather than sitting in the open segment
// where nothing interesting happens.
const (
	testNet         = "testnet"
	testToken       = "e2e-token"
	genesisTime     = 1606824023
	secondsPerSlot  = 12
	testOrigin      = 100
	testSegBits     = 3 // 8 slots per window
	testFanoutBits  = 2
	allHead         = "all"
	arbitrumHead    = "arbitrum-one"
	testFinalSlot   = 111 // the last slot the fake beacon has finalized
	testMaxPutBlobs = 3   // deliberately below a batch's blob count; see the put chunking in both indexers
)

// errShortVH is what the fake beacon says to a malformed versioned hash.
var errShortVH = errors.New("versioned hash is not 32 bytes")

// lanes is how many field elements a blob is made of.
const lanes = schema.BlobSize / 32

// makeBlob builds a valid blob: 4096 field elements, each a 32-byte big-endian
// integer below the BLS12-381 scalar modulus. Leaving each lane's top bytes
// zero keeps every element far below the modulus whatever the low ones say.
func makeBlob(seed uint64) []byte {
	b := make([]byte, schema.BlobSize)
	for i := range lanes {
		binary.BigEndian.PutUint64(b[i*32+24:i*32+32], seed*1_000_003+uint64(i))
	}
	return b
}

// blobVH is the real versioned hash of a blob: a real KZG commitment over real
// bytes. Spec 13.7 asks for a small blob count precisely so this can be real
// rather than stubbed.
func blobVH(blob []byte) schema.VersionedHash {
	vh, err := ingest.VersionedHash(blob)
	if err != nil {
		panic(fmt.Sprintf("e2e: fixture blob is not a valid KZG blob: %v", err))
	}
	return vh
}

// quietPebble drops Pebble's internal logging, which is otherwise the majority
// of a test run's output.
type quietPebble struct{}

func (quietPebble) Infof(string, ...any)  {}
func (quietPebble) Errorf(string, ...any) {}
func (quietPebble) Fatalf(format string, args ...any) {
	panic(fmt.Sprintf(format, args...))
}

// stack is a whole bloard minus main: a store, the heads over it, and the HTTP
// API in front.
type stack struct {
	t     *testing.T
	dir   string
	store *store.Store
	heads *server.Heads
	url   string
	http  *httptest.Server
}

// newStack starts a bloard writing the named heads, all with the same
// parameters -- which is what makes the determinism assertion meaningful: the
// second stack's ALL head is the first's in every respect except how it was
// filled.
func newStack(t *testing.T, headNames ...string) *stack {
	t.Helper()
	ctx := t.Context()

	s := &stack{t: t, dir: t.TempDir()}

	var err error
	if s.store, err = store.Open(s.dir, store.WithPebbleLogger(quietPebble{})); err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() {
		s.http.Close()
		if err := s.store.Close(); err != nil {
			t.Errorf("closing store: %v", err)
		}
	})

	cat := catalog.New(s.store.KV())
	cache, err := core.NewNodeCacheMB(1)
	if err != nil {
		t.Fatalf("core.NewNodeCacheMB: %v", err)
	}
	roots := server.NewRootStore(s.store.KV())

	// Unsigned: spec 8's signature is a follower's concern, and this test has
	// no followers in it. The manifest store is wired so a chain head can carry a
	// published manifest chain (spec 10.5): a chain indexer runs only against one,
	// so the arbitrum head is bootstrapped with a genesis manifest before it syncs.
	if s.heads, err = server.NewHeads(server.HeadsConfig{
		Net: testNet, Roots: roots, SigningKey: ed25519.PrivateKey(nil),
		Manifests: server.NewManifestStore(s.store.KV()), Blocks: s.store.Blocks(),
	}); err != nil {
		t.Fatalf("server.NewHeads: %v", err)
	}

	for _, name := range headNames {
		head, err := server.OpenHead(ctx,
			archive.Config{Blocks: s.store.Blocks(), Resolver: cat, Cache: cache}, roots,
			archive.Params{
				Name: name, Net: testNet, OriginSlot: testOrigin,
				SegBits: testSegBits, FanoutBits: testFanoutBits,
			})
		if err != nil {
			t.Fatalf("server.OpenHead(%s): %v", name, err)
		}
		if err := s.heads.Add(head); err != nil {
			t.Fatalf("Heads.Add(%s): %v", name, err)
		}
	}

	ingester, err := ingest.New(ingest.Config{Blocks: s.store.Blocks(), Catalog: cat})
	if err != nil {
		t.Fatalf("ingest.New: %v", err)
	}
	handler, err := server.New(server.Config{
		Heads:    s.heads,
		Blocks:   s.store.Blocks(),
		Ingester: ingester,
		Beacon: server.Beacon{
			GenesisTime:           genesisTime,
			SecondsPerSlot:        secondsPerSlot,
			GenesisValidatorsRoot: "0x4b363db94e286120d76eb905340fdd4e54bfe9f06bf33ff6cf5ad27f511bfe95",
			GenesisForkVersion:    "0x00000000",
		},
		AuthToken:   testToken,
		MaxPutBlobs: testMaxPutBlobs,
	})
	if err != nil {
		t.Fatalf("server.New: %v", err)
	}

	s.http = httptest.NewServer(handler)
	s.url = s.http.URL
	return s
}

// client returns a bloar API client against this stack, with the retry backoff
// wound down: a test that has to wait a quarter second per attempt to see a
// retry work is a test nobody runs.
func (s *stack) client() *archclient.Client {
	s.t.Helper()
	c, err := archclient.New(archclient.Config{
		BaseURL: s.url,
		Token:   testToken,
		Backoff: time.Millisecond,
	})
	if err != nil {
		s.t.Fatalf("archclient.New: %v", err)
	}
	return c
}

// staleFinalityProxy is a bloar archive whose stated coverage runs ahead of the
// coverage its read API will actually serve.
//
// Everything but GET /bloar/v1/heads/{head}/synced_to is passed through to a
// real archive untouched, so every blob answer -- including the 503 for a slot
// past the real coverage -- is the real one. Only the progress endpoint lies,
// reporting a slot the reads will refuse.
//
// This is not a straw man. It is a lagging replica behind a load balancer (the
// finality read hits a caught-up node, the blob read hits one that is not), and
// it is a truncate (spec 5.4) landing between an indexer's two requests. Both
// put the indexer in exactly this position, and the 503 rule of spec 10.1 is
// the only thing standing between it and a permanently short head.
type staleFinalityProxy struct {
	http *httptest.Server
	url  string
}

// newStaleFinalityProxy fronts target, reporting syncedTo for every head.
func newStaleFinalityProxy(t *testing.T, target string, syncedTo uint64) *staleFinalityProxy {
	t.Helper()
	backend, err := url.Parse(target)
	if err != nil {
		t.Fatalf("parsing the proxy target: %v", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /bloar/v1/heads/{head}/synced_to", func(w http.ResponseWriter, r *http.Request) {
		body, err := json.Marshal(map[string]any{"synced_to": syncedTo})
		if err != nil {
			t.Errorf("rendering the overstated synced_to: %v", err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
	})
	mux.Handle("/", httputil.NewSingleHostReverseProxy(backend))

	p := &staleFinalityProxy{http: httptest.NewServer(mux)}
	t.Cleanup(p.http.Close)
	p.url = p.http.URL
	return p
}

// root returns a head's current root CID: the thing spec 11.5 says two
// independent derivations must agree on.
func (s *stack) root(name string) string {
	s.t.Helper()
	head, ok := s.heads.Get(name)
	if !ok {
		s.t.Fatalf("stack has no head %q", name)
	}
	return head.Root().String()
}

// syncedTo returns a head's coverage.
func (s *stack) syncedTo(name string) (uint64, bool) {
	s.t.Helper()
	head, ok := s.heads.Get(name)
	if !ok {
		s.t.Fatalf("stack has no head %q", name)
	}
	return head.SyncedTo()
}
