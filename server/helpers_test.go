package server_test

import (
	"bytes"
	"crypto/ed25519"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ipfs/go-cid"

	"github.com/blobarchive/bloar/archive"
	"github.com/blobarchive/bloar/catalog"
	"github.com/blobarchive/bloar/core"
	"github.com/blobarchive/bloar/ingest"
	"github.com/blobarchive/bloar/metrics"
	"github.com/blobarchive/bloar/schema"
	"github.com/blobarchive/bloar/server"
	"github.com/blobarchive/bloar/store"
)

// The tests run the real stack -- store, catalog, ingest, archive, server --
// over a temp directory and reach it over a real HTTP connection. Nothing here
// is mocked, because everything that could be mocked is exactly what the server
// phase exists to connect: the caching rules depend on the engine's synced_to,
// the 404 message depends on the engine's MissingVH, and the blob bytes come
// out of the blockstore ingest put them in.

// The test head. Small windows (8 slots) mean a handful of rows seal segments
// and grow the directory, so the wiring is exercised rather than just the open
// segment.
const (
	testHead    = "all"
	testNet     = "testnet"
	testToken   = "s3cret-token"
	testOrigin  = 8
	testSegBits = 3
	testFanout  = 2
)

// lanes is how many field elements a blob is made of.
const lanes = schema.BlobSize / 32

// makeBlob builds a valid blob: 4096 field elements, each a 32-byte big-endian
// integer below the BLS12-381 scalar modulus. Leaving each lane's top bytes
// zero keeps every element far below the modulus whatever the low ones say.
func makeBlob(seed uint64) []byte {
	b := make([]byte, schema.BlobSize)
	for i := range lanes {
		binary.BigEndian.PutUint64(b[i*32+24:i*32+32], seed+uint64(i))
	}
	return b
}

// quietPebble drops Pebble's internal logging, which is otherwise the majority
// of a test run's output. Fatalf still panics: it is Pebble saying the store is
// unusable, and swallowing that would turn a broken store into a confusing test
// failure somewhere else.
type quietPebble struct{}

func (quietPebble) Infof(string, ...any)  {}
func (quietPebble) Errorf(string, ...any) {}
func (quietPebble) Fatalf(format string, args ...any) {
	panic(fmt.Sprintf(format, args...))
}

// stack is the whole daemon minus main: a store, the heads over it, and an HTTP
// server in front.
type stack struct {
	t       *testing.T
	dir     string
	store   *store.Store
	heads   *server.Heads
	url     string
	key     ed25519.PrivateKey
	http    *httptest.Server
	handler *server.Server // the raw handler behind http, for tests that inject a spy ResponseWriter

	// The rest is populated only when stackOpts.instrument is set, for the
	// commit-durability tests. roots is the seam a persist failure is injected
	// through; metrics is a private registry the swap counter is read back from;
	// docs and swaps record what OnDoc and OnRoot were handed, in order.
	roots   *faultyRoots
	metrics *metrics.Metrics
	obsMu   sync.Mutex
	docs    [][]byte
	swaps   []rootSwap
}

// rootSwap is one OnRoot call: the head and the root it was handed.
type rootSwap struct {
	name string
	root cid.Cid
}

// stackOpts are the knobs the tests vary.
type stackOpts struct {
	// dir reuses an existing store directory, for the restart tests.
	dir string
	// segBits overrides the test head's window geometry. Zero keeps the small
	// default used by tests that need to seal windows quickly.
	segBits uint64
	// horizon is the immutable horizon in slots. Zero takes the spec default,
	// which is far too wide to reach in a test.
	horizon uint64
	// maxPutBlobs bounds POST /bloar/v1/blobs. Zero takes the spec default.
	maxPutBlobs int
	// writeTimeout is the blobs response's write budget. Zero
	// takes server.New's safe default, which is too long for a test to reach.
	writeTimeout time.Duration
	// serverMetrics wires a metrics registry into server.Config, which turns on
	// instrumentRead and so wraps the blobs writer in a statusRecorder (finding
	// the safety boundary): the path where the write deadline must still reach the connection
	// through the wrapper. The shipped writer example enables metrics, so the
	// slow-reader regression runs both with and without this.
	serverMetrics bool
	// sign attaches a publication signing key.
	sign bool
	// archiveID activates the signed, revisioned publication-v3 contract. It
	// requires sign, matching the production configuration boundary.
	archiveID *server.ArchiveID
	// multiaddrs is published as the document's multiaddrs.
	multiaddrs []string
	// instrument wires the commit-durability probes onto the stack: a RootStore
	// whose Put can be made to fail, OnDoc/OnRoot recorders, and a metrics
	// registry. Other tests leave it false and build exactly the stack they did
	// before.
	instrument bool
}

// newStack wires the stack the way cmd/bloard does -- server.OpenHead over a
// server.RootStore, so a reopen resumes rather than forks.
func newStack(t *testing.T, opts stackOpts) *stack {
	t.Helper()
	ctx := t.Context()

	s := &stack{t: t, dir: opts.dir}
	if s.dir == "" {
		s.dir = t.TempDir()
	}

	var err error
	if s.store, err = store.Open(s.dir, store.WithPebbleLogger(quietPebble{})); err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() { s.Close() })

	if opts.sign {
		if _, s.key, err = ed25519.GenerateKey(nil); err != nil {
			t.Fatalf("generating a signing key: %v", err)
		}
	}

	cat := catalog.New(s.store.KV())
	cache, err := core.NewNodeCacheMB(1)
	if err != nil {
		t.Fatalf("core.NewNodeCacheMB: %v", err)
	}
	rs := server.NewRootStore(s.store.KV())
	var roots server.Roots = rs
	headsCfg := server.HeadsConfig{
		Net:        testNet,
		Roots:      roots,
		Manifests:  server.NewManifestStore(s.store.KV()),
		Blocks:     s.store.Blocks(),
		Multiaddrs: opts.multiaddrs,
		// The same value server.Config gets below, so the advertised limit matches
		// the enforced one, exactly as cmd/bloard wires them.
		MaxPutBlobs:  opts.maxPutBlobs,
		ArchiveID:    opts.archiveID,
		SigningKey:   s.key,
		Replacements: map[string]func(*archive.Head){testHead: func(*archive.Head) {}},
	}
	if opts.instrument {
		s.roots = &faultyRoots{RootStore: rs}
		s.metrics = metrics.New()
		roots = s.roots
		headsCfg.Roots = s.roots
		headsCfg.Metrics = s.metrics
		headsCfg.OnDoc = func(doc []byte) {
			s.obsMu.Lock()
			defer s.obsMu.Unlock()
			s.docs = append(s.docs, append([]byte(nil), doc...))
		}
		headsCfg.OnRoot = func(name string, root cid.Cid) {
			s.obsMu.Lock()
			defer s.obsMu.Unlock()
			s.swaps = append(s.swaps, rootSwap{name: name, root: root})
		}
	}
	if s.heads, err = server.NewHeads(headsCfg); err != nil {
		t.Fatalf("server.NewHeads: %v", err)
	}

	segBits := opts.segBits
	if segBits == 0 {
		segBits = testSegBits
	}
	head, err := server.OpenHead(ctx, archive.Config{Blocks: s.store.Blocks(), Resolver: cat, Cache: cache}, roots,
		archive.Params{Name: testHead, Net: testNet, OriginSlot: testOrigin, SegBits: segBits, FanoutBits: testFanout})
	if err != nil {
		t.Fatalf("server.OpenHead: %v", err)
	}
	if err := s.heads.Add(head); err != nil {
		t.Fatalf("Heads.Add: %v", err)
	}

	ingester, err := ingest.New(ingest.Config{Blocks: s.store.Blocks(), Catalog: cat})
	if err != nil {
		t.Fatalf("ingest.New: %v", err)
	}
	// A metrics registry on the server turns on instrumentRead's statusRecorder
	// wrapper and lets a test read the counters the handlers bump.
	// An instrumented stack shares the heads' registry (s.metrics, set above), so a
	// test can read back the corrupt-read counter; a serverMetrics stack that is not
	// instrumented gets its own registry purely to turn the wrapper on.
	serverMx := s.metrics
	if opts.serverMetrics && serverMx == nil {
		serverMx = metrics.New()
	}
	handler, err := server.New(server.Config{
		Heads:    s.heads,
		Blocks:   s.store.Blocks(),
		Ingester: ingester,
		Beacon: server.Beacon{
			GenesisTime:           1606824023,
			SecondsPerSlot:        12,
			GenesisValidatorsRoot: "0x4b363db94e286120d76eb905340fdd4e54bfe9f06bf33ff6cf5ad27f511bfe95",
			GenesisForkVersion:    "0x00000000",
			Spec:                  map[string]string{"DEPOSIT_CHAIN_ID": "1"},
		},
		AuthToken:                testToken,
		MaxPutBlobs:              opts.maxPutBlobs,
		ImmutableHorizonSlots:    opts.horizon,
		BlobResponseWriteTimeout: opts.writeTimeout,
		Metrics:                  serverMx,
	})
	if err != nil {
		t.Fatalf("server.New: %v", err)
	}
	s.handler = handler

	s.http = httptest.NewServer(handler)
	s.url = s.http.URL
	return s
}

// Close shuts the stack down. It is idempotent, so a test that restarts may
// close early and let the cleanup run again.
func (s *stack) Close() {
	s.http.Close()
	if err := s.store.Close(); err != nil {
		s.t.Errorf("closing store: %v", err)
	}
}

// docCount is how many documents OnDoc has been handed so far. instrument only.
func (s *stack) docCount() int {
	s.obsMu.Lock()
	defer s.obsMu.Unlock()
	return len(s.docs)
}

// swapCount is how many roots OnRoot has been handed so far. instrument only.
func (s *stack) swapCount() int {
	s.obsMu.Lock()
	defer s.obsMu.Unlock()
	return len(s.swaps)
}

// lastSwap is the most recent OnRoot call. instrument only; the caller has
// established at least one swap.
func (s *stack) lastSwap() rootSwap {
	s.obsMu.Lock()
	defer s.obsMu.Unlock()
	return s.swaps[len(s.swaps)-1]
}

// rootSwapMetric reads the bloar_head_root_swaps_total counter for a head, or 0
// if no swap has been recorded for it. instrument only.
func (s *stack) rootSwapMetric(head string) float64 {
	s.t.Helper()
	families, err := s.metrics.Registry().Gather()
	if err != nil {
		s.t.Fatalf("gathering metrics: %v", err)
	}
	for _, fam := range families {
		if fam.GetName() != "bloar_head_root_swaps_total" {
			continue
		}
		for _, m := range fam.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == "head" && l.GetValue() == head {
					return m.GetCounter().GetValue()
				}
			}
		}
	}
	return 0
}

// durableRoot reads the head's root straight from the RootStore, so a test can
// assert what a restart would resume from rather than what memory serves.
// instrument only.
func (s *stack) durableRoot(head string) string {
	s.t.Helper()
	root, ok, err := s.roots.Get(s.t.Context(), head)
	if err != nil || !ok {
		s.t.Fatalf("reading durable root of %q: ok=%t err=%v", head, ok, err)
	}
	return root.String()
}

// put ingests blobs through the real HTTP endpoint and returns their versioned
// hashes, in body order.
func (s *stack) put(blobs ...[]byte) []string {
	s.t.Helper()
	body := make([]byte, 0, len(blobs)*schema.BlobSize)
	for _, b := range blobs {
		body = append(body, b...)
	}

	resp := s.do("POST", "/bloar/v1/blobs", bytes.NewReader(body), withAuth)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		s.t.Fatalf("POST /bloar/v1/blobs: status = %d, body = %s", resp.StatusCode, readAll(s.t, resp))
	}

	var out struct {
		Blobs []struct {
			VersionedHash string `json:"versioned_hash"`
			CID           string `json:"cid"`
		} `json:"blobs"`
	}
	decode(s.t, resp, &out)
	if len(out.Blobs) != len(blobs) {
		s.t.Fatalf("POST /bloar/v1/blobs returned %d blobs, want %d", len(out.Blobs), len(blobs))
	}
	vhs := make([]string, 0, len(out.Blobs))
	for _, b := range out.Blobs {
		vhs = append(vhs, b.VersionedHash)
	}
	return vhs
}

// refs posts a refs batch and fails the test if it is not accepted. It binds to
// the head's current manifest tip when the head has one (spec 10.5, audit
// the safety boundary), and omits the field otherwise -- exactly what the endpoint requires of
// each.
func (s *stack) refs(rows []map[string]any, syncedTo uint64) {
	s.t.Helper()
	// A coverage-only batch sends an explicit empty array, never null: absent or
	// null rows is a 400, and a real writer always sends [] (the
	// archclient marshals make([]jsonRow, 0, ...)). A nil argument here means
	// "advance covered-empty", so send what a real writer would.
	if rows == nil {
		rows = []map[string]any{}
	}
	body := map[string]any{"rows": rows, "synced_to": syncedTo}
	if tip := s.headEntry().Manifest; tip != "" {
		body["expected_manifest"] = tip
	}
	resp := s.postJSON("/bloar/v1/heads/"+testHead+"/refs", body, withAuth)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		s.t.Fatalf("POST refs: status = %d, body = %s", resp.StatusCode, readAll(s.t, resp))
	}
}

// row builds one refs row.
func row(slot uint64, vhs ...string) map[string]any {
	return map[string]any{"slot": slot, "versioned_hashes": vhs}
}

// reqOpt configures a request.
type reqOpt func(*http.Request)

// withAuth attaches the configured bearer token.
func withAuth(r *http.Request) { r.Header.Set("Authorization", "Bearer "+testToken) }

// do issues a request against the stack.
func (s *stack) do(method, path string, body io.Reader, opts ...reqOpt) *http.Response {
	s.t.Helper()
	req, err := http.NewRequestWithContext(s.t.Context(), method, s.url+path, body)
	if err != nil {
		s.t.Fatalf("building %s %s: %v", method, path, err)
	}
	for _, opt := range opts {
		opt(req)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		s.t.Fatalf("%s %s: %v", method, path, err)
	}
	return resp
}

// get issues a GET.
func (s *stack) get(path string, opts ...reqOpt) *http.Response {
	s.t.Helper()
	return s.do("GET", path, nil, opts...)
}

// postJSON issues a POST with a JSON body.
func (s *stack) postJSON(path string, body any, opts ...reqOpt) *http.Response {
	s.t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		s.t.Fatalf("marshalling body for %s: %v", path, err)
	}
	return s.do("POST", path, bytes.NewReader(raw), opts...)
}

// blobsURL builds a filtered blobs request.
func blobsURL(slot uint64, vhs ...string) string {
	u := "/" + testHead + "/eth/v1/beacon/blobs/" + itoa(slot)
	if len(vhs) > 0 {
		u += "?versioned_hashes=" + strings.Join(vhs, "&versioned_hashes=")
	}
	return u
}

func itoa(v uint64) string {
	var b [20]byte
	return string(strconv.AppendUint(b[:0], v, 10))
}

// getBlobs fetches a slot and asserts a 200, returning the "data" array.
func (s *stack) getBlobs(slot uint64, vhs ...string) []string {
	s.t.Helper()
	resp := s.get(blobsURL(slot, vhs...))
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		s.t.Fatalf("GET blobs slot %d: status = %d, body = %s", slot, resp.StatusCode, readAll(s.t, resp))
	}
	var out struct {
		Data []string `json:"data"`
	}
	decode(s.t, resp, &out)
	return out.Data
}

// decode unmarshals a response body into v.
func decode(t *testing.T, resp *http.Response, v any) {
	t.Helper()
	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}
	if err := json.NewDecoder(resp.Body).Decode(v); err != nil {
		t.Fatalf("decoding response: %v", err)
	}
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

// errorOf decodes a beacon-shape error body and asserts its code matches the
// status line, which spec 7 has stated twice and so must agree with itself.
func errorOf(t *testing.T, resp *http.Response) errBody {
	t.Helper()
	var body errBody
	decode(t, resp, &body)
	if body.Code != resp.StatusCode {
		t.Errorf("error body code = %d, status line = %d; they must agree", body.Code, resp.StatusCode)
	}
	if body.Message == "" {
		t.Error("error body has no message")
	}
	return body
}

// errBody is the beacon error shape of spec 7, plus 7.2's missing_blobs and
// 10.5's manifest_tip.
type errBody struct {
	Code         int      `json:"code"`
	Message      string   `json:"message"`
	MissingBlobs []string `json:"missing_blobs"`
	ManifestTip  string   `json:"manifest_tip"`
}

// blobHex renders a blob the way the API states one, for comparison.
func blobHex(b []byte) string { return "0x" + hex.EncodeToString(b) }

// The Accept header a client sends for spec 7.1's raw variant, and the type the
// server answers it with. octetAccept is exactly what the upstream client sends,
// so a test using it exercises the same comma-list the negotiation splits.
const (
	octetAccept = "application/octet-stream, application/json"
	octetType   = "application/octet-stream"
)

// accept sets a request's Accept header.
func accept(v string) reqOpt { return func(r *http.Request) { r.Header.Set("Accept", v) } }

// getOctet fetches a slot as spec 7.1's raw variant, asserts a 200 and the
// octet-stream content type, and returns the body split into BlobSize blobs.
func (s *stack) getOctet(slot uint64, vhs ...string) [][]byte {
	s.t.Helper()
	resp := s.get(blobsURL(slot, vhs...), accept(octetAccept))
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		s.t.Fatalf("GET octet blobs slot %d: status = %d, body = %s", slot, resp.StatusCode, readAll(s.t, resp))
	}
	if ct := resp.Header.Get("Content-Type"); ct != octetType {
		s.t.Fatalf("Content-Type = %q, want %q", ct, octetType)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		s.t.Fatalf("reading octet body: %v", err)
	}
	if len(body)%schema.BlobSize != 0 {
		s.t.Fatalf("octet body is %d bytes, not a multiple of the %d-byte blob size", len(body), schema.BlobSize)
	}
	out := make([][]byte, 0, len(body)/schema.BlobSize)
	for off := 0; off < len(body); off += schema.BlobSize {
		out = append(out, body[off:off+schema.BlobSize])
	}
	return out
}

// assertRawBlobs compares octet-stream blobs against the bytes they should be.
func assertRawBlobs(t *testing.T, got, want [][]byte) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %d blobs, want %d", len(got), len(want))
	}
	for i := range want {
		if !bytes.Equal(got[i], want[i]) {
			// Not printed: they are 128 KiB each.
			t.Errorf("blob %d is not the one expected at that position", i)
		}
	}
}
