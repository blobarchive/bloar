package follow_test

import (
	"bytes"
	"crypto/ed25519"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/blobarchive/bloar/archive"
	"github.com/blobarchive/bloar/catalog"
	"github.com/blobarchive/bloar/core"
	"github.com/blobarchive/bloar/follow"
	"github.com/blobarchive/bloar/ingest"
	"github.com/blobarchive/bloar/metrics"
	"github.com/blobarchive/bloar/p2p"
	"github.com/blobarchive/bloar/pinning"
	"github.com/blobarchive/bloar/schema"
	"github.com/blobarchive/bloar/server"
	"github.com/blobarchive/bloar/store"
)

// The tests here run a real writer and a real follower in one process, over
// real libp2p hosts on loopback TCP, and move real blobs between them. Nothing
// is mocked: what is being tested is that a follower built out of this
// codebase's parts replicates from a writer built out of the same parts, and a
// fake on either side would test the fake.
//
// The head parameters are tiny (8 slots per window, fanout 4) so that a handful
// of fixture slots actually seals segments and grows a directory -- a window
// policy that retains "some segments and not others" needs there to be some.
const (
	testHead    = "all"
	testNet     = "testnet"
	testToken   = "token"
	testOrigin  = 96
	testSegBits = 3
	testFanout  = 2

	genesisTime    = 1606824023
	secondsPerSlot = 12
)

// lanes is how many field elements a blob is made of.
const lanes = schema.BlobSize / 32

// makeBlob builds a valid, distinct blob. Each 32-byte field element must be
// below the BLS12-381 scalar modulus; leaving each lane's top byte zero puts
// every element far below it whatever the low bytes say (ingest's helpers make
// the same argument at more length).
func makeBlob(seed uint64) []byte {
	b := make([]byte, schema.BlobSize)
	for i := range lanes {
		binary.BigEndian.PutUint64(b[i*32+24:i*32+32], seed+uint64(i))
	}
	return b
}

// quietPebble keeps Pebble's compaction chatter out of the test output.
type quietPebble struct{}

func (quietPebble) Infof(string, ...any)  {}
func (quietPebble) Errorf(string, ...any) {}
func (quietPebble) Fatalf(f string, a ...any) {
	panic(fmt.Sprintf(f, a...))
}

// node is the machinery both roles share: a store, a registry, a reconciler, a
// libp2p host with bitswap, and an HTTP server.
type node struct {
	t         *testing.T
	store     *store.Store
	heads     *server.Heads
	roots     *server.RootStore
	manifests *server.ManifestStore
	rec       *pinning.Reconciler
	cache     *core.NodeCache
	host      *p2p.Host
	docs      *p2p.DocBlockstore
	ex        *p2p.Exchange
	http      *httptest.Server
	url       string
}

// writer is a node that writes testHead, signs its publication document, and
// serves blocks to peers: spec 11.1's writer.
type writer struct {
	*node
	key  ed25519.PrivateKey
	head *archive.Head
	cat  *catalog.Catalog
	ing  *ingest.Ingester
}

// follower is a node that follows testHead from a writer.
type follower struct {
	*node
	f *follow.Follower
	// staging is the fetch pass's staging pins, wired the way the daemon wires
	// them (setupFollow): the same ledger the reconciler drives and GC would
	// mark from. A test reads it to check the pass drops what it took.
	staging *pinning.Staging
}

func newNode(t *testing.T) *node {
	return newNodeWithMetrics(t, nil)
}

// newNodeWithMetrics is newNode with the real p2p collectors enabled. Most
// protocol tests do not need transfer accounting; callers that assert transfer
// behavior opt in explicitly.
func newNodeWithMetrics(t *testing.T, mx *metrics.Metrics) *node {
	t.Helper()
	n := &node{t: t}

	var err error
	if n.store, err = store.Open(t.TempDir(), store.WithPebbleLogger(quietPebble{})); err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() {
		if err := n.store.Close(); err != nil {
			t.Errorf("closing store: %v", err)
		}
	})

	if n.cache, err = core.NewNodeCacheMB(1); err != nil {
		t.Fatalf("core.NewNodeCacheMB: %v", err)
	}
	n.roots = server.NewRootStore(n.store.KV())
	n.manifests = server.NewManifestStore(n.store.KV())

	if n.host, err = p2p.NewHost(t.Context(), p2p.HostConfig{
		Listen:          []string{"/ip4/127.0.0.1/tcp/0"},
		IdentityKeyFile: filepath.Join(t.TempDir(), "p2p.key"),
		Metrics:         mx,
	}); err != nil {
		t.Fatalf("p2p.NewHost: %v", err)
	}
	t.Cleanup(func() {
		if err := n.host.Close(); err != nil {
			t.Errorf("closing host: %v", err)
		}
	})

	// Bitswap serves the local blockstore. Never a fetching one: see
	// p2p.NewExchange and follow.Follower.Blocks.
	if n.docs, err = p2p.NewDocBlockstore(n.store.Blocks()); err != nil {
		t.Fatalf("p2p.NewDocBlockstore: %v", err)
	}
	if n.ex, err = p2p.NewExchange(t.Context(), p2p.ExchangeConfig{
		Host: n.host, Blocks: n.docs, Metrics: mx,
	}); err != nil {
		t.Fatalf("p2p.NewExchange: %v", err)
	}
	t.Cleanup(func() {
		if err := n.ex.Close(); err != nil {
			t.Errorf("closing exchange: %v", err)
		}
	})

	if n.rec, err = pinning.NewReconciler(pinning.Config{
		Ledger:      catalog.NewLedger(n.store.KV()),
		ManifestTip: n.manifests.Get,
	}); err != nil {
		t.Fatalf("pinning.NewReconciler: %v", err)
	}
	return n
}

// serveHTTP mounts the read API over the node's registry.
func (n *node) serveHTTP(ing *ingest.Ingester) {
	n.t.Helper()
	if ing == nil {
		// A follower runs no ingest (spec 11.1), but server.Config requires an
		// Ingester because POST /bloar/v1/blobs is always mounted. One over the
		// follower's own store is the honest stand-in: nothing calls it.
		var err error
		if ing, err = ingest.New(ingest.Config{
			Blocks:  n.store.Blocks(),
			Catalog: catalog.New(n.store.KV()),
		}); err != nil {
			n.t.Fatalf("ingest.New: %v", err)
		}
	}
	handler, err := server.New(server.Config{
		Heads:    n.heads,
		Blocks:   n.store.Blocks(),
		Ingester: ing,
		Beacon: server.Beacon{
			GenesisTime:           genesisTime,
			SecondsPerSlot:        secondsPerSlot,
			GenesisValidatorsRoot: "0x00",
			GenesisForkVersion:    "0x00000000",
		},
		AuthToken: testToken,
	})
	if err != nil {
		n.t.Fatalf("server.New: %v", err)
	}
	n.http = httptest.NewServer(handler)
	n.url = n.http.URL
	n.t.Cleanup(n.http.Close)
}

// newWriter brings up a writer with an open, signed, empty head.
func newWriter(t *testing.T) *writer {
	return newWriterForArchive(t, nil)
}

// newWriterForArchive builds the ordinary real writer stack but, when id is
// present, makes its registry publish the signed, revisioned logical-archive
// document used by independent writers. The stores, signing keys, libp2p
// identities, and revision allocators remain physically independent.
func newWriterForArchive(t *testing.T, id *server.ArchiveID) *writer {
	t.Helper()
	w := &writer{node: newNode(t)}

	_, key, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("generating a signing key: %v", err)
	}
	w.key = key

	w.cat = catalog.New(w.store.KV())
	if w.ing, err = ingest.New(ingest.Config{Blocks: w.store.Blocks(), Catalog: w.cat}); err != nil {
		t.Fatalf("ingest.New: %v", err)
	}
	if w.head, err = server.OpenHead(t.Context(),
		archive.Config{Blocks: w.store.Blocks(), Resolver: w.cat, Cache: w.cache},
		w.roots,
		archive.Params{Name: testHead, Net: testNet, OriginSlot: testOrigin, SegBits: testSegBits, FanoutBits: testFanout},
	); err != nil {
		t.Fatalf("server.OpenHead: %v", err)
	}
	if err := w.rec.Add(w.head, pinning.Full()); err != nil {
		t.Fatalf("Reconciler.Add: %v", err)
	}
	replace, err := w.rec.BindReplacement(testHead)
	if err != nil {
		t.Fatalf("Reconciler.BindReplacement: %v", err)
	}
	w.heads, err = server.NewHeads(server.HeadsConfig{
		Net:        testNet,
		Roots:      w.roots,
		Manifests:  w.manifests,
		Blocks:     w.store.Blocks(),
		Multiaddrs: w.host.AnnounceAddrs(),
		ArchiveID:  id,
		SigningKey: key,
		OnRoot:     func(name string, _ cid.Cid) { w.rec.Notify(name) },
		Replacements: map[string]func(*archive.Head){
			testHead: replace,
		},
	})
	if err != nil {
		t.Fatalf("server.NewHeads: %v", err)
	}
	if err := w.heads.Add(w.head); err != nil {
		t.Fatalf("Heads.Add: %v", err)
	}
	w.serveHTTP(w.ing)
	return w
}

// pubkey is what a follower configures as follow.pubkey.
func (w *writer) pubkey() ed25519.PublicKey { return w.key.Public().(ed25519.PublicKey) }

// ingestSlot puts n blobs and refs them at slot, advancing synced_to to it. It
// returns the blobs, index-aligned with the versioned hashes it also returns.
func (w *writer) ingestSlot(slot uint64, seeds ...uint64) ([][]byte, []schema.VersionedHash) {
	w.t.Helper()

	var body []byte
	blobs := make([][]byte, 0, len(seeds))
	for _, seed := range seeds {
		b := makeBlob(seed)
		blobs = append(blobs, b)
		body = append(body, b...)
	}
	put, err := w.ing.PutBlobs(w.t.Context(), body)
	if err != nil {
		w.t.Fatalf("PutBlobs: %v", err)
	}

	vhs := make([]schema.VersionedHash, 0, len(put))
	for _, p := range put {
		vhs = append(vhs, p.VH)
	}
	w.applyRefs([]archive.RefRow{{Slot: slot, VHs: vhs}}, slot)
	return blobs, vhs
}

// applyRefs applies a batch through the registry, which is what publishes a new
// document.
func (w *writer) applyRefs(rows []archive.RefRow, syncedTo uint64) {
	w.t.Helper()
	// Bind the batch to the head's current tip, which is
	// cid.Undef until a manifest chain is bootstrapped -- exactly what the server
	// requires of a chainless head.
	tip, _ := w.heads.ManifestTip(testHead)
	if _, err := w.heads.ApplyRefs(w.t.Context(), testHead, rows, syncedTo, tip); err != nil {
		w.t.Fatalf("ApplyRefs to %d: %v", syncedTo, err)
	}
	// Writer mutations are copy-on-write: keep the fixture's convenience pointer
	// aligned with the root the registry selected, rather than the retired engine.
	var ok bool
	if w.head, ok = w.heads.Get(testHead); !ok {
		w.t.Fatalf("head %q disappeared after ApplyRefs to %d", testHead, syncedTo)
	}
}

// setManifest advances the head's manifest chain through the registry, the way
// the POST endpoint does, and returns the new tip. from distinguishes one
// manifest from the next; prev is the CAS anchor (cid.Undef for genesis).
func (w *writer) setManifest(prev cid.Cid, from uint64) cid.Cid {
	w.t.Helper()
	m := &schema.Manifest{
		V:    schema.ManifestVersion,
		Head: testHead,
		Sources: []schema.Source{{
			Type:      schema.SourceInboxEvents,
			Address:   bytes.Repeat([]byte{0x1c}, schema.AddressSize),
			Topic:     bytes.Repeat([]byte{0x73}, schema.TopicSize),
			FromBlock: from,
			OpenEnded: true,
		}},
		Prev: prev,
	}
	block, c, err := schema.EncodeManifest(m)
	if err != nil {
		w.t.Fatalf("EncodeManifest: %v", err)
	}
	head, ok := w.heads.Get(testHead)
	if !ok {
		w.t.Fatalf("head %q is not registered", testHead)
	}
	tip, err := w.heads.SetManifest(w.t.Context(), testHead, block, c, prev, head.Root())
	if err != nil {
		w.t.Fatalf("SetManifest(prev=%s): %v", prev, err)
	}
	return tip
}

// newFollower brings up a follower of w, connected to it over libp2p, with no
// HTTP server of its own until serveHTTP is called.
//
// The connection is made by hand here even though the publication document's
// multiaddrs would make it on the first poll (and one test asserts exactly
// that), so that every other test fails about the protocol rather than about
// how long a dial took.
func newFollower(t *testing.T, w *writer, opts ...func(*follow.Config)) *follower {
	return newFollowerWithMetrics(t, w, nil, opts...)
}

func newFollowerWithMetrics(t *testing.T, w *writer, mx *metrics.Metrics, opts ...func(*follow.Config)) *follower {
	t.Helper()
	f := newLoneFollowerWithMetrics(t, w, mx, opts...)
	f.connect(w)
	return f
}

// newLoneFollower is newFollower without the connection: a follower that knows
// a URL and a key and has to find the blocks itself.
func newLoneFollower(t *testing.T, w *writer, opts ...func(*follow.Config)) *follower {
	return newLoneFollowerWithMetrics(t, w, nil, opts...)
}

func newLoneFollowerWithMetrics(t *testing.T, w *writer, mx *metrics.Metrics, opts ...func(*follow.Config)) *follower {
	t.Helper()
	f := &follower{node: newNodeWithMetrics(t, mx)}

	var err error
	f.heads, err = server.NewHeads(server.HeadsConfig{
		Net:        testNet,
		Roots:      f.roots,
		Manifests:  f.manifests,
		Blocks:     f.store.Blocks(),
		Gate:       f.rec.Gate(),
		Multiaddrs: f.host.AnnounceAddrs(),
		OnRoot:     func(name string, _ cid.Cid) { f.rec.Notify(name) },
	})
	if err != nil {
		t.Fatalf("server.NewHeads: %v", err)
	}

	// The staging pins the fetch pass takes, over the follower's own ledger, as
	// setupFollow wires them. The resolver is unused on this side -- the fetch
	// pass stages and drops by CID, never by versioned hash -- but NewStaging
	// requires one, so it gets the follower's catalog.
	f.staging, err = pinning.NewStaging(pinning.StagingConfig{
		Ledger:   catalog.NewLedger(f.store.KV()),
		Resolver: catalog.New(f.store.KV()),
	})
	if err != nil {
		t.Fatalf("pinning.NewStaging: %v", err)
	}

	cfg := follow.Config{
		Net:        testNet,
		URL:        w.url,
		PubKey:     w.pubkey(),
		Heads:      map[string]pinning.Policy{testHead: pinning.Full()},
		Local:      f.store.Blocks(),
		Sessions:   f.ex,
		Host:       f.host,
		Registry:   f.heads,
		Roots:      f.roots,
		Reconciler: f.rec,
		Staging:    f.staging,
		KV:         f.store.KV(),
		Cache:      f.cache,
		Metrics:    mx,
		Logger:     testLogger(t),
	}
	for _, o := range opts {
		o(&cfg)
	}
	if f.f, err = follow.New(cfg); err != nil {
		t.Fatalf("follow.New: %v", err)
	}
	t.Cleanup(func() {
		if err := f.f.Close(); err != nil {
			t.Errorf("closing follower: %v", err)
		}
	})
	return f
}

// connect dials the writer directly, the way p2p.peers would.
func (f *follower) connect(w *writer) {
	f.t.Helper()
	if err := f.host.Libp2p().Connect(f.t.Context(), peerInfo(w)); err != nil {
		f.t.Fatalf("connecting the follower to the writer: %v", err)
	}
}

// ledgerOf is the node's pin ledger: the one the reconciler writes and GC marks
// from (spec 6.2).
func ledgerOf(n *node) *catalog.Ledger { return catalog.NewLedger(n.store.KV()) }

func peerInfo(w *writer) peer.AddrInfo {
	return peer.AddrInfo{ID: w.host.ID(), Addrs: w.host.Libp2p().Addrs()}
}

// poll runs one cycle and fails the test if it errors.
func (f *follower) poll() {
	f.t.Helper()
	if err := f.f.Poll(f.t.Context()); err != nil {
		f.t.Fatalf("Poll: %v", err)
	}
}

// pollErr runs one cycle and returns what it says.
func (f *follower) pollErr() error { return f.f.Poll(f.t.Context()) }

// blobsAt asks the follower's read API for a slot, the way nitro would, and
// returns the status and the decoded blobs.
func (f *follower) blobsAt(slot uint64, vhs ...schema.VersionedHash) (int, []string, http.Header) {
	f.t.Helper()

	url := fmt.Sprintf("%s/%s/eth/v1/beacon/blobs/%d", f.url, testHead, slot)
	for i, vh := range vhs {
		sep := "?"
		if i > 0 {
			sep = "&"
		}
		url += fmt.Sprintf("%sversioned_hashes=0x%x", sep, vh[:])
	}
	req, err := http.NewRequestWithContext(f.t.Context(), http.MethodGet, url, nil)
	if err != nil {
		f.t.Fatalf("building the request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		f.t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()

	var body struct {
		Data    []string `json:"data"`
		Message string   `json:"message"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		f.t.Fatalf("decoding the response: %v", err)
	}
	return resp.StatusCode, body.Data, resp.Header
}

// get issues a plain GET and returns the status.
func (n *node) get(url string) int {
	n.t.Helper()
	req, err := http.NewRequestWithContext(n.t.Context(), http.MethodGet, url, nil)
	if err != nil {
		n.t.Fatalf("building the request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		n.t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	return resp.StatusCode
}

// manifestInDoc returns the manifest tip the node republishes for testHead in
// its own document, or "" when it publishes none.
func (n *node) manifestInDoc() string {
	n.t.Helper()
	b, ok := n.heads.HeadDoc(testHead)
	if !ok {
		n.t.Fatalf("head %q is not in the document", testHead)
	}
	var e server.HeadEntry
	if err := json.Unmarshal(b, &e); err != nil {
		n.t.Fatalf("unmarshalling the head entry: %v", err)
	}
	return e.Manifest
}

// hasLocally reports whether the node holds a block itself, without fetching:
// the question every retention assertion here is really asking.
func (n *node) hasLocally(c cid.Cid) bool {
	n.t.Helper()
	has, err := n.store.Blocks().Has(n.t.Context(), c)
	if err != nil {
		n.t.Fatalf("Has(%s): %v", c, err)
	}
	return has
}

// blobCID is the CID of a blob's bytes.
func blobCID(t *testing.T, blob []byte) cid.Cid {
	t.Helper()
	c, err := schema.BlobCID(blob)
	if err != nil {
		t.Fatalf("schema.BlobCID: %v", err)
	}
	return c
}

// countBlocks counts what a node holds, by codec: index blocks and blobs are
// retained by different rules (spec 9) and a test that lumped them together
// could not see a window policy working.
func (n *node) countBlocks() (index, blobs int) {
	n.t.Helper()
	keys, err := n.store.Blocks().AllKeysChan(n.t.Context())
	if err != nil {
		n.t.Fatalf("AllKeysChan: %v", err)
	}
	for c := range keys {
		// The blockstore is multihash-keyed and reports everything raw (see
		// pinning's markKey), so the codec here says nothing. Size does: an
		// index block is never 128 KiB and a blob is always exactly that.
		size, err := n.store.Blocks().GetSize(n.t.Context(), c)
		if err != nil {
			n.t.Fatalf("GetSize(%s): %v", c, err)
		}
		if size == schema.BlobSize {
			blobs++
			continue
		}
		index++
	}
	return index, blobs
}

// reconcile runs a pin pass over every head, which is what a daemon's
// reconciler loop would do on the notify the follower sends.
func (n *node) reconcile() pinning.Delta {
	n.t.Helper()
	delta, err := n.rec.ReconcileAll(n.t.Context())
	if err != nil {
		n.t.Fatalf("ReconcileAll: %v", err)
	}
	return delta
}

// gc runs a mark-and-sweep over the node's own blockstore. GCConfig.Blocks is
// the local store on a follower and must stay that way: the mark set is what
// the pins reach and the sweep set is what this node holds, and a fetching
// blockstore has no answer to the second.
func (n *node) gc() pinning.GCStats {
	n.t.Helper()
	gc, err := pinning.NewGC(pinning.GCConfig{Blocks: n.store.Blocks(), Reconciler: n.rec})
	if err != nil {
		n.t.Fatalf("pinning.NewGC: %v", err)
	}
	stats, err := gc.Run(n.t.Context())
	if err != nil {
		n.t.Fatalf("GC.Run: %v", err)
	}
	return stats
}

// logs collects a follower's log lines so a test can assert on what an operator
// would see. The quarantine path's whole job is to say something loud enough to
// act on, and a test that only checked for the 503 would not notice it saying
// nothing at all.
type logs struct {
	mu    sync.Mutex
	lines []string
}

// testLogger writes into a *logs and, at the same time, into the test's own
// output, so that a failing test explains itself without a second run.
func testLogger(t *testing.T) *slog.Logger {
	return slog.New(slog.NewTextHandler(testWriter{t}, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

// capturingLogger is testLogger plus a record of every line, for the tests that
// assert on one.
func capturingLogger(t *testing.T, l *logs) *slog.Logger {
	return slog.New(slog.NewTextHandler(io.MultiWriter(testWriter{t}, l), &slog.HandlerOptions{Level: slog.LevelDebug}))
}

func (l *logs) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lines = append(l.lines, string(p))
	return len(p), nil
}

// has reports whether any line mentions sub.
func (l *logs) has(sub string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, s := range l.lines {
		if strings.Contains(s, sub) {
			return true
		}
	}
	return false
}

// testWriter routes a logger into t.Log. It tolerates a line arriving after the
// test has finished, which a background poll loop can do: t.Log on a finished
// test panics, and a log line is never worth a panic.
type testWriter struct{ t *testing.T }

func (w testWriter) Write(p []byte) (int, error) {
	defer func() { _ = recover() }()
	w.t.Logf("%s", bytes.TrimRight(p, "\n"))
	return len(p), nil
}
