package conformance

import (
	"bytes"
	"crypto/ed25519"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/ipfs/go-cid"

	"github.com/blobarchive/bloar/archive"
	"github.com/blobarchive/bloar/catalog"
	"github.com/blobarchive/bloar/core"
	"github.com/blobarchive/bloar/ingest"
	"github.com/blobarchive/bloar/p2p"
	"github.com/blobarchive/bloar/pinning"
	"github.com/blobarchive/bloar/server"
	"github.com/blobarchive/bloar/store"
)

// This file wires the real bloard stack -- store, catalog, ingest, archive,
// server -- over a temp directory, the same construction path cmd/bloard and
// server's own tests use. It is duplicated here rather than imported because
// server/helpers_test.go is in package server_test and so is not importable,
// and because this module deliberately does not depend on bloar's test
// helpers: the point of 13.1 is that a stack built the way an operator builds
// one answers Nitro, so only exported API is used.

// The head the conformance fixtures live under. Nitro's beacon URL carries
// this as a path prefix ("http://host/all"), which is the whole question spec
// 7.1 poses to nitro's URL construction.
const (
	testHead        = "all"
	testMutableHead = "unfinalized"
	testLiveHead    = "live"
	testNet         = "testnet"
	testToken       = "s3cret-token"

	// The fixture window. OriginSlot sits below the fixture slots and the
	// segment/fanout widths are small so the archive's directory actually
	// grows rather than everything landing in one open segment.
	testOrigin  = 96
	testSegBits = 3
	testFanout  = 2

	// The beacon config the server serves and nitro's Initialize() reads.
	// Mainnet's genesis, so fixture slots map to timestamps in the past and
	// nitro's slot-age heuristic takes its ordinary branch.
	genesisTime    = 1606824023
	secondsPerSlot = 12
)

// quietPebble drops Pebble's internal logging. Fatalf still panics: it is
// Pebble saying the store is unusable.
type quietPebble struct{}

func (quietPebble) Infof(string, ...any)  {}
func (quietPebble) Errorf(string, ...any) {}
func (quietPebble) Fatalf(format string, args ...any) {
	panic(fmt.Sprintf(format, args...))
}

// stack is the whole daemon minus main.
type stack struct {
	t     *testing.T
	store *store.Store
	url   string
	http  *httptest.Server

	// The parts a follower needs on the other end (spec 11.2, 11.3): the
	// registry it adopts into, the roots it persists, the host that serves its
	// blocks, and the key its documents are signed with. All zero on a plain
	// writer, which is what every other test here uses.
	heads   *server.Heads
	roots   *server.RootStore
	rec     *pinning.Reconciler
	staging *pinning.Staging
	host    *p2p.Host
	ex      *p2p.Exchange
	key     ed25519.PrivateKey
}

// stackConfig is what newStack's options set.
type stackConfig struct {
	// sign gives the stack a publication signing key (spec 8), and p2p gives it
	// a libp2p host with bitswap (spec 11.2). A follower needs both of its
	// writer; nothing else here needs either.
	sign bool
	p2p  bool
	// live adds a bounded mutable physical head and the local-only /live
	// finalized/provisional view over it. It is opt-in so every pre-existing
	// /all conformance fixture retains precisely its original topology.
	live bool
}

// withP2P makes a stack followable: signed documents, and a host that serves its
// blocks.
func withP2P(c *stackConfig) { c.sign, c.p2p = true, true }

// withLive builds the writer-side topology used by the optimistic-head
// conformance case. Mutable writers publish revisioned signed documents, so
// the signing key is part of the option rather than an incidental test detail.
func withLive(c *stackConfig) { c.sign, c.live = true, true }

// newStack wires the stack the way cmd/bloard does: server.OpenHead over a
// server.RootStore, fronted by httptest.
func newStack(t *testing.T, opts ...func(*stackConfig)) *stack {
	t.Helper()
	ctx := t.Context()

	cfg := &stackConfig{}
	for _, o := range opts {
		o(cfg)
	}

	s := &stack{t: t}
	dir := t.TempDir()

	var err error
	if s.store, err = store.Open(dir, store.WithPebbleLogger(quietPebble{})); err != nil {
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
	s.roots = roots
	archiveCfg := archive.Config{Blocks: s.store.Blocks(), Resolver: cat, Cache: cache}

	ledger := catalog.NewLedger(s.store.KV())
	if s.rec, err = pinning.NewReconciler(pinning.Config{Ledger: ledger}); err != nil {
		t.Fatalf("pinning.NewReconciler: %v", err)
	}
	// The staging pins of spec 9's window (a). Wired here for the same reason
	// the gate is, below: this stack is built out of the exported API the way an
	// operator's is, and both are now things the library does rather than things
	// cmd/bloard remembers to do.
	if s.staging, err = pinning.NewStaging(pinning.StagingConfig{Ledger: ledger, Resolver: cat}); err != nil {
		t.Fatalf("pinning.NewStaging: %v", err)
	}

	var key ed25519.PrivateKey
	if cfg.sign {
		// A follower verifies every document it adopts (spec 8, 11.3), so a
		// writer it can follow signs.
		if _, key, err = ed25519.GenerateKey(nil); err != nil {
			t.Fatalf("generating a signing key: %v", err)
		}
		s.key = key
	}
	if cfg.p2p {
		s.startP2P()
	}

	var replaceFinalized, replaceMutable func(*archive.Head)
	policies := make(map[string]server.HeadPolicy)
	replacements := map[string]func(*archive.Head){testHead: func(head *archive.Head) {
		if replaceFinalized == nil {
			panic("conformance: finalized replacement callback used before binding")
		}
		replaceFinalized(head)
	}}
	if cfg.live {
		policies[testMutableHead] = server.HeadPolicy{
			Kind: server.UnfinalizedMutable, HandoffHead: testHead, MaxWindowSlots: 16,
		}
		// BindReplacement is installed after the initial mutable engine is added
		// to the reconciler. No replacement can occur during construction, and
		// the callback is therefore fully bound before the test receives s.
		replacements[testMutableHead] = func(head *archive.Head) {
			if replaceMutable == nil {
				panic("conformance: mutable replacement callback used before binding")
			}
			replaceMutable(head)
		}
	}

	heads, err := server.NewHeads(server.HeadsConfig{
		Net:               testNet,
		Roots:             roots,
		Policies:          policies,
		GenerationArchive: archiveCfg,
		Replacements:      replacements,
		Multiaddrs:        s.announce(),
		SigningKey:        key,
		OnRoot:            func(name string, _ cid.Cid) { s.rec.Notify(name) },
		// Spec 9's exclusion. This stack has no HTTP middleware and never did:
		// before the gate moved into the mutation engine, this stack ran its
		// mutations ungated. It is configured here rather than assumed
		// because that is what an embedder writes.
		Gate:    s.rec.Gate(),
		Staging: s.staging,
	})
	if err != nil {
		t.Fatalf("server.NewHeads: %v", err)
	}
	s.heads = heads

	head, err := server.OpenHead(ctx,
		archiveCfg,
		roots,
		archive.Params{
			Name:       testHead,
			Net:        testNet,
			OriginSlot: testOrigin,
			SegBits:    testSegBits,
			FanoutBits: testFanout,
		})
	if err != nil {
		t.Fatalf("server.OpenHead: %v", err)
	}
	if err := heads.Add(head); err != nil {
		t.Fatalf("Heads.Add: %v", err)
	}
	if err := s.rec.Add(head, pinning.Full()); err != nil {
		t.Fatalf("Reconciler.Add: %v", err)
	}
	replaceFinalized, err = s.rec.BindReplacement(testHead)
	if err != nil {
		t.Fatalf("Reconciler.BindReplacement(%s): %v", testHead, err)
	}
	if cfg.live {
		mutable, err := server.OpenMutableHead(ctx, archiveCfg, roots, archive.Params{
			Name:       testMutableHead,
			Net:        testNet,
			OriginSlot: testOrigin,
			SegBits:    testSegBits,
			FanoutBits: testFanout,
		})
		if err != nil {
			t.Fatalf("server.OpenMutableHead: %v", err)
		}
		if err := heads.Add(mutable); err != nil {
			t.Fatalf("Heads.Add(%s): %v", testMutableHead, err)
		}
		if err := s.rec.Add(mutable, pinning.Full()); err != nil {
			t.Fatalf("Reconciler.Add(%s): %v", testMutableHead, err)
		}
		replaceMutable, err = s.rec.BindReplacement(testMutableHead)
		if err != nil {
			t.Fatalf("Reconciler.BindReplacement(%s): %v", testMutableHead, err)
		}
	}

	ingester, err := ingest.New(ingest.Config{
		Blocks:  s.store.Blocks(),
		Catalog: cat,
		Gate:    s.rec.Gate(),
		Staging: s.staging,
	})
	if err != nil {
		t.Fatalf("ingest.New: %v", err)
	}

	var liveHeads map[string]server.LiveHead
	if cfg.live {
		liveHeads = map[string]server.LiveHead{
			testLiveHead: {FinalizedHead: testHead, UnfinalizedHead: testMutableHead},
		}
	}
	handler, err := server.New(server.Config{
		Heads:     heads,
		Blocks:    s.store.Blocks(),
		Ingester:  ingester,
		LiveHeads: liveHeads,
		Beacon: server.Beacon{
			GenesisTime:           genesisTime,
			SecondsPerSlot:        secondsPerSlot,
			GenesisValidatorsRoot: "0x4b363db94e286120d76eb905340fdd4e54bfe9f06bf33ff6cf5ad27f511bfe95",
			GenesisForkVersion:    "0x00000000",
		},
		AuthToken: testToken,
	})
	if err != nil {
		t.Fatalf("server.New: %v", err)
	}

	s.http = httptest.NewServer(handler)
	s.url = s.http.URL
	return s
}

// startP2P brings up the libp2p host and bitswap, the way cmd/bloard does.
// Bitswap serves the store's own blockstore: it is the serving half of spec
// 11.2, and handing it a fetching blockstore would make this node answer a
// peer's miss by asking its own peers.
func (s *stack) startP2P() {
	s.t.Helper()

	var err error
	if s.host, err = p2p.NewHost(s.t.Context(), p2p.HostConfig{
		Listen:          []string{"/ip4/127.0.0.1/tcp/0"},
		IdentityKeyFile: filepath.Join(s.t.TempDir(), "p2p.key"),
	}); err != nil {
		s.t.Fatalf("p2p.NewHost: %v", err)
	}
	s.t.Cleanup(func() {
		if err := s.host.Close(); err != nil {
			s.t.Errorf("closing host: %v", err)
		}
	})

	docs, err := p2p.NewDocBlockstore(s.store.Blocks())
	if err != nil {
		s.t.Fatalf("p2p.NewDocBlockstore: %v", err)
	}
	if s.ex, err = p2p.NewExchange(s.t.Context(), p2p.ExchangeConfig{Host: s.host, Blocks: docs}); err != nil {
		s.t.Fatalf("p2p.NewExchange: %v", err)
	}
	s.t.Cleanup(func() {
		if err := s.ex.Close(); err != nil {
			s.t.Errorf("closing exchange: %v", err)
		}
	})
}

// announce is what the publication document claims blocks can be fetched from
// (spec 8).
func (s *stack) announce() []string {
	if s.host == nil {
		return nil
	}
	return s.host.AnnounceAddrs()
}

// pubkey is what a follower configures as follow.pubkey.
func (s *stack) pubkey() ed25519.PublicKey { return s.key.Public().(ed25519.PublicKey) }

// put ingests blobs through the real HTTP endpoint and returns the versioned
// hashes the server computed, in body order.
func (s *stack) put(blobs ...[]byte) []string {
	s.t.Helper()

	var body []byte
	for _, b := range blobs {
		body = append(body, b...)
	}

	resp := s.do("POST", "/bloar/v1/blobs", bytes.NewReader(body))
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		s.t.Fatalf("POST /bloar/v1/blobs: status = %d, body = %s", resp.StatusCode, readAll(s.t, resp))
	}

	var out struct {
		Blobs []struct {
			VersionedHash string `json:"versioned_hash"`
		} `json:"blobs"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		s.t.Fatalf("decoding put response: %v", err)
	}
	if len(out.Blobs) != len(blobs) {
		s.t.Fatalf("POST /bloar/v1/blobs returned %d blobs, want %d", len(out.Blobs), len(blobs))
	}

	vhs := make([]string, 0, len(out.Blobs))
	for _, b := range out.Blobs {
		vhs = append(vhs, b.VersionedHash)
	}
	return vhs
}

// refs posts a refs batch and fails the test if it is not accepted.
func (s *stack) refs(rows []map[string]any, syncedTo uint64) {
	s.t.Helper()

	raw, err := json.Marshal(map[string]any{"rows": rows, "synced_to": syncedTo})
	if err != nil {
		s.t.Fatalf("marshalling refs body: %v", err)
	}
	resp := s.do("POST", "/bloar/v1/heads/"+testHead+"/refs", bytes.NewReader(raw))
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		s.t.Fatalf("POST refs: status = %d, body = %s", resp.StatusCode, readAll(s.t, resp))
	}
}

// do issues an authenticated request against the stack.
func (s *stack) do(method, path string, body io.Reader) *http.Response {
	s.t.Helper()

	req, err := http.NewRequestWithContext(s.t.Context(), method, s.url+path, body)
	if err != nil {
		s.t.Fatalf("building %s %s: %v", method, path, err)
	}
	req.Header.Set("Authorization", "Bearer "+testToken)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		s.t.Fatalf("%s %s: %v", method, path, err)
	}
	return resp
}

// readAll drains a body, for failure messages.
func readAll(t *testing.T, resp *http.Response) string {
	t.Helper()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reading body: %v", err)
	}
	return string(b)
}
