package main

// Full-wiring lifecycle regression for the safety boundary: the composed
// daemon's readiness tracks a followed head end to end. A real bloard follower,
// brought up by serve(), follows a writer whose head binds a versioned hash to the
// wrong blob (verify: full). Component tests prove each hop; this proves the wiring:
// configured startup -> verified adoption -> API serviceable + /readyz 200 +
// follow_head_ready 1 -> a corrupt read quarantines the head -> API 503 + /readyz
// 503 + follow_head_ready 0 -> no later poll resurrects it.

import (
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"

	"github.com/blobarchive/bloar/catalog"
	"github.com/blobarchive/bloar/ingest"
	"github.com/blobarchive/bloar/p2p"
	"github.com/blobarchive/bloar/schema"
	"github.com/blobarchive/bloar/server"
	"github.com/blobarchive/bloar/store"
)

const (
	lcHead    = "all"
	lcNet     = "testnet"
	lcOrigin  = 96
	lcSegBits = 3
	lcFanout  = 2
	lcGenesis = 1606824023
	lcSPS     = 12
	lcSlot    = 100
)

func lcBlob(seed uint64) []byte {
	const lanes = schema.BlobSize / 32
	b := make([]byte, schema.BlobSize)
	for i := 0; i < lanes; i++ {
		for j := 0; j < 8; j++ {
			b[i*32+24+j] = byte((seed + uint64(i)) >> (8 * (7 - j)))
		}
	}
	return b
}

// corruptWriter is a writer host serving a head that binds a versioned hash to the
// wrong blob: honest's vh at served's CID. It serves the DAG over bitswap and a
// signed document over HTTP, exactly the two channels a follower reads.
type corruptWriter struct {
	docURL    string
	pubkeyHex string
	multiaddr string
	vh        schema.VersionedHash
	// docFail, when non-zero, makes the document endpoint answer that status instead
	// of the signed document, to induce ordinary poll failures in the follower.
	docFail *atomic.Int32
}

func buildCorruptWriter(t *testing.T) corruptWriter {
	t.Helper()
	ctx := t.Context()

	st, err := store.Open(t.TempDir(), store.WithPebbleLogger(pebbleLogger{log: newLogger()}))
	if err != nil {
		t.Fatalf("writer store.Open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	host, err := p2p.NewHost(ctx, p2p.HostConfig{
		Listen:          []string{"/ip4/127.0.0.1/tcp/0"},
		IdentityKeyFile: filepath.Join(t.TempDir(), "p2p.key"),
	})
	if err != nil {
		t.Fatalf("writer p2p.NewHost: %v", err)
	}
	t.Cleanup(func() { _ = host.Close() })

	docs, err := p2p.NewDocBlockstore(st.Blocks())
	if err != nil {
		t.Fatalf("writer NewDocBlockstore: %v", err)
	}
	ex, err := p2p.NewExchange(ctx, p2p.ExchangeConfig{Host: host, Blocks: docs})
	if err != nil {
		t.Fatalf("writer NewExchange: %v", err)
	}
	t.Cleanup(func() { _ = ex.Close() })

	_, key, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("writer key: %v", err)
	}

	ing, err := ingest.New(ingest.Config{Blocks: st.Blocks(), Catalog: catalog.New(st.KV())})
	if err != nil {
		t.Fatalf("writer ingest.New: %v", err)
	}
	honest, served := lcBlob(11), lcBlob(22)
	honestVH, err := ingest.VersionedHash(honest)
	if err != nil {
		t.Fatalf("VersionedHash: %v", err)
	}
	servedC, err := schema.BlobCID(served)
	if err != nil {
		t.Fatalf("BlobCID: %v", err)
	}
	if _, err := ing.PutBlobs(ctx, append(append([]byte{}, honest...), served...)); err != nil {
		t.Fatalf("PutBlobs: %v", err)
	}

	// The lie: honest's versioned hash, served's CID. Only recomputing the
	// commitment finds it, which is what verify: full does on the read.
	syncedTo := uint64(lcSlot)
	seg := &schema.Segment{
		Slot0: lcSlot &^ (1<<lcSegBits - 1),
		Rows:  []schema.Row{{Slot: lcSlot, Entries: []schema.RefEntry{{VH: honestVH, Blob: servedC}}}},
	}
	segRaw, segCID, err := schema.EncodeSegment(seg)
	if err != nil {
		t.Fatalf("EncodeSegment: %v", err)
	}
	lcPutRaw(t, st, segRaw, segCID)

	head := &schema.Head{
		Name: lcHead, Net: lcNet, OriginSlot: lcOrigin, SyncedTo: &syncedTo,
		SegBits: lcSegBits, FanoutBits: lcFanout, Open: segCID,
	}
	headRaw, root, err := schema.EncodeHead(head)
	if err != nil {
		t.Fatalf("EncodeHead: %v", err)
	}
	lcPutRaw(t, st, headRaw, root)

	u := server.Unsigned{
		V:          server.DocVersion,
		Net:        lcNet,
		UpdatedAt:  time.Now().UTC().Format(time.RFC3339),
		Multiaddrs: host.AnnounceAddrs(),
		Heads: []server.HeadEntry{{
			Name: lcHead, Root: root.String(), OriginSlot: lcOrigin, SyncedTo: &syncedTo,
			SegBits: lcSegBits, FanoutBits: lcFanout,
		}},
	}
	canonical, err := u.Canonical()
	if err != nil {
		t.Fatalf("Unsigned.Canonical: %v", err)
	}
	body, err := json.Marshal(server.Doc{
		Unsigned:  u,
		Pubkey:    hex.EncodeToString(key.Public().(ed25519.PublicKey)),
		Signature: hex.EncodeToString(ed25519.Sign(key, canonical)),
	})
	if err != nil {
		t.Fatalf("marshalling document: %v", err)
	}
	var docFail atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/bloar/v1/heads" {
			http.NotFound(w, r)
			return
		}
		if code := docFail.Load(); code != 0 {
			w.WriteHeader(int(code))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
	}))
	t.Cleanup(srv.Close)

	addrs := host.AnnounceAddrs()
	if len(addrs) == 0 {
		t.Fatal("writer host announced no addresses")
	}
	return corruptWriter{docURL: srv.URL, pubkeyHex: hex.EncodeToString(key.Public().(ed25519.PublicKey)),
		multiaddr: addrs[0], vh: honestVH, docFail: &docFail}
}

func lcPutRaw(t *testing.T, st *store.Store, raw []byte, c cid.Cid) {
	t.Helper()
	blk, err := blocks.NewBlockWithCid(raw, c)
	if err != nil {
		t.Fatalf("framing block %s: %v", c, err)
	}
	if err := st.Blocks().Put(context.Background(), blk); err != nil {
		t.Fatalf("putting block %s: %v", c, err)
	}
}

func TestFollowerReadinessLifecycle(t *testing.T) {
	w := buildCorruptWriter(t)

	dir := t.TempDir()
	token := filepath.Join(dir, "token")
	if err := os.WriteFile(token, []byte("test-token"), 0o600); err != nil {
		t.Fatalf("writing token: %v", err)
	}
	apiAddr, metricsAddr := freeAddr(t), freeAddr(t)
	cfg := loadString(t, fmt.Sprintf(`
net: testnet
beacon: {genesis_time: %d, seconds_per_slot: %d}
store: {path: %s}
server: {listen: "%s", auth_token_file: %s, metrics_listen: "%s"}
p2p: {listen: ["/ip4/127.0.0.1/tcp/0"], nat_port_map: false, peers: ["%s"], dht: {bootstrap: private}}
follow:
  url: %s
  pubkey: "%s"
  poll_interval: 200ms
  verify: full
  heads:
    all: {pin: {mode: none}}
`, lcGenesis, lcSPS, filepath.Join(dir, "store"), apiAddr, token, metricsAddr, w.multiaddr, w.docURL, w.pubkeyHex))

	// Hold the document at 503 from the start, so the follower cannot adopt and we can
	// prove the pre-adoption red state before releasing it.
	w.docFail.Store(http.StatusServiceUnavailable)

	stop := startServe(t, cfg)
	defer stop(t)

	q := fmt.Sprintf("/all/eth/v1/beacon/blobs/%d?versioned_hashes=0x%x", lcSlot, w.vh[:])

	// 1. At startup with an unreachable writer the configured followed head is unmet:
	// /readyz 503 and the gauge 0. Gate on an observed failed poll so this is asserted
	// only after the follower has actually tried (and failed).
	waitFollowPollsAbove(t, metricsAddr, "error", 0)
	if status, _ := httpGet(t, metricsAddr, "/readyz"); status != http.StatusServiceUnavailable {
		t.Fatalf("/readyz was %d at startup with an unreachable writer, want 503", status)
	}
	if !strings.Contains(scrapeMetrics(t, metricsAddr), `bloar_follow_head_ready{head="all"} 0`) {
		t.Fatalf("follow_head_ready was not 0 before adoption:\n%s", scrapeMetrics(t, metricsAddr))
	}

	// 2. Release the document: the follower resolves, verifies, and adopts. /readyz
	// 200, gauge 1, head serviceable.
	w.docFail.Store(0)
	waitForStatus(t, metricsAddr, "/readyz", http.StatusOK, 30*time.Second)
	if !strings.Contains(scrapeMetrics(t, metricsAddr), `bloar_follow_head_ready{head="all"} 1`) {
		t.Fatalf("follow_head_ready is not 1 after adoption:\n%s", scrapeMetrics(t, metricsAddr))
	}
	if status, body := httpGet(t, apiAddr, "/all/eth/v1/beacon/blobs/98"); status != http.StatusOK {
		t.Fatalf("a covered empty slot answered %d after adoption; the head is not serviceable: %s", status, body)
	}

	// 3. An ordinary FAILED POLL does not withdraw readiness. Set the document to 503,
	// wait for a NEW failed poll to be recorded, then confirm /readyz stays 200 and the
	// gauge 1 -- a served head keeps serving its durable generation while the writer is
	// unreachable.
	mark := followPolls(t, metricsAddr, "error")
	w.docFail.Store(http.StatusServiceUnavailable)
	waitFollowPollsAbove(t, metricsAddr, "error", mark)
	if status, _ := httpGet(t, metricsAddr, "/readyz"); status != http.StatusOK {
		t.Fatalf("/readyz dropped to %d during an ordinary poll failure; an unreachable writer must not withdraw readiness",
			status)
	}
	if !strings.Contains(scrapeMetrics(t, metricsAddr), `bloar_follow_head_ready{head="all"} 1`) {
		t.Fatalf("follow_head_ready fell from 1 during an ordinary poll failure:\n%s", scrapeMetrics(t, metricsAddr))
	}
	w.docFail.Store(0)

	// 4. A read of the corrupt blob under verify: full quarantines the head (spec
	// 11.4): 503, /readyz 503, gauge 0.
	if status, _ := httpGet(t, apiAddr, q); status != http.StatusServiceUnavailable {
		t.Fatalf("the corrupt read returned %d, want 503 (quarantine)", status)
	}
	waitForStatus(t, metricsAddr, "/readyz", http.StatusServiceUnavailable, 5*time.Second)
	if !strings.Contains(scrapeMetrics(t, metricsAddr), `bloar_follow_head_ready{head="all"} 0`) {
		t.Fatalf("follow_head_ready is not 0 after quarantine:\n%s", scrapeMetrics(t, metricsAddr))
	}

	// 5. No later poll resurrects the quarantined head. The document is valid again;
	// wait for a successful poll AFTER quarantine, then confirm it stays out.
	mark = followPolls(t, metricsAddr, "ok")
	waitFollowPollsAbove(t, metricsAddr, "ok", mark)
	if status, _ := httpGet(t, metricsAddr, "/readyz"); status != http.StatusServiceUnavailable {
		t.Fatalf("/readyz recovered to %d after quarantine; a quarantined head must not be resurrected", status)
	}
	if status, _ := httpGet(t, apiAddr, q); status != http.StatusServiceUnavailable {
		t.Fatalf("the quarantined head answered %d after a further successful poll, want 503", status)
	}
	if !strings.Contains(scrapeMetrics(t, metricsAddr), `bloar_follow_head_ready{head="all"} 0`) {
		t.Fatalf("follow_head_ready recovered from 0 after quarantine:\n%s", scrapeMetrics(t, metricsAddr))
	}
}

// followPolls reads the bloar_follow_polls_total counter for the https channel and
// the given outcome, or 0 if the series is not present yet.
func followPolls(t *testing.T, addr, outcome string) float64 {
	t.Helper()
	_, body := httpGet(t, addr, "/metrics")
	prefix := fmt.Sprintf(`bloar_follow_polls_total{channel="https",outcome="%s"} `, outcome)
	for _, line := range strings.Split(body, "\n") {
		if rest, ok := strings.CutPrefix(line, prefix); ok {
			var v float64
			if _, err := fmt.Sscanf(strings.TrimSpace(rest), "%g", &v); err == nil {
				return v
			}
		}
	}
	return 0
}

// waitFollowPollsAbove blocks until the https-channel poll counter for outcome has
// advanced past mark -- an observed watermark, so a caller can gate an assertion on a
// poll actually having happened rather than on a fixed sleep.
func waitFollowPollsAbove(t *testing.T, addr, outcome string, mark float64) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if followPolls(t, addr, outcome) > mark {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("follow_polls_total{channel=\"https\",outcome=%q} did not advance past %g within the deadline", outcome, mark)
}

func httpGet(t *testing.T, addr, path string) (int, string) {
	t.Helper()
	resp, err := http.Get("http://" + addr + path)
	if err != nil {
		t.Fatalf("GET %s%s: %v", addr, path, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(body)
}

func waitForStatus(t *testing.T, addr, path string, want int, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	var last int
	for time.Now().Before(deadline) {
		if status, _ := httpGet(t, addr, path); status == want {
			return
		} else {
			last = status
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("GET %s never reached status %d within %s (last %d)", path, want, within, last)
}

func scrapeMetrics(t *testing.T, addr string) string {
	t.Helper()
	_, body := httpGet(t, addr, "/metrics")
	return body
}
