package upstream_test

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blobarchive/bloar/index/upstream"
	"github.com/blobarchive/bloar/metrics"
	"github.com/blobarchive/bloar/schema"
)

// TestBlobsRecordsTheRead is the upstream instrumentation of spec 10.1: a slot
// fetch is timed and its blob bytes are counted, and a slot the upstream reports
// absent is timed but carries no bytes. The bytes counter is the throughput a
// perf drill had to hand-curl for before the indexer exposed any metrics.
func TestBlobsRecordsTheRead(t *testing.T) {
	// One full blob's worth of hex, which is what parseBlob will accept.
	blob := make([]byte, schema.BlobSize)
	hexBlob := "0x" + hex.EncodeToString(blob)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// The beacon-node blobs path; slot 41 has a blob, everything else is a
		// 404, which for a beacon node is an empty or pruned slot.
		if strings.HasSuffix(r.URL.Path, "/eth/v1/beacon/blobs/41") {
			_ = json.NewEncoder(w).Encode(map[string]any{"data": []string{hexBlob}})
			return
		}
		http.Error(w, `{"message":"not found"}`, http.StatusNotFound)
	}))
	defer srv.Close()

	mx := metrics.New()
	c, err := upstream.New(upstream.Config{BaseURL: srv.URL, Metrics: mx})
	if err != nil {
		t.Fatalf("upstream.New: %v", err)
	}

	res, err := c.Blobs(context.Background(), 41, nil)
	if err != nil {
		t.Fatalf("Blobs(41): %v", err)
	}
	if res.Status != upstream.StatusFound || len(res.Blobs) != 1 {
		t.Fatalf("Blobs(41) = %v with %d blobs, want one found blob", res.Status, len(res.Blobs))
	}

	// An empty slot: still a round trip, so still timed, but no bytes.
	res, err = c.Blobs(context.Background(), 42, nil)
	if err != nil {
		t.Fatalf("Blobs(42): %v", err)
	}
	if res.Status != upstream.StatusAbsent {
		t.Fatalf("Blobs(42) = %v, want absent", res.Status)
	}

	body := scrape(t, mx)
	if got := mustSample(t, body, "bloar_upstream_read_bytes_total"); got != float64(schema.BlobSize) {
		t.Errorf("bloar_upstream_read_bytes_total = %g, want %d (one blob, and nothing from the empty slot)", got, schema.BlobSize)
	}
	if got := mustSample(t, body, "bloar_upstream_read_duration_seconds_count"); got != 2 {
		t.Errorf("bloar_upstream_read_duration_seconds_count = %g, want 2 (both the found and the absent fetch)", got)
	}
}

// TestBlobsWithoutMetricsDoesNotPanic is the nil-safe contract: an indexer with
// metrics_listen unset fetches through a nil *Metrics on every slot.
func TestBlobsWithoutMetricsDoesNotPanic(t *testing.T) {
	blob := make([]byte, schema.BlobSize)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"data": []string{"0x" + hex.EncodeToString(blob)}})
	}))
	defer srv.Close()

	c, err := upstream.New(upstream.Config{BaseURL: srv.URL}) // no Metrics.
	if err != nil {
		t.Fatalf("upstream.New: %v", err)
	}
	if _, err := c.Blobs(context.Background(), 1, nil); err != nil {
		t.Fatalf("Blobs with no metrics: %v", err)
	}
}

// TestBlobsOctetStreamParity is spec 7.1's raw variant on the client side: a
// bloar upstream that answers application/octet-stream yields the same blobs,
// in the same order, as the JSON a beacon node answers. The JSON handler also
// covers the other case the variant must not break -- the client offers the raw
// type, a beacon node ignores it and answers JSON, and the client parses it.
func TestBlobsOctetStreamParity(t *testing.T) {
	blobs := [][]byte{blobN(1), blobN(2), blobN(3)}

	newClient := func(t *testing.T, h http.HandlerFunc) *upstream.Client {
		srv := httptest.NewServer(h)
		t.Cleanup(srv.Close)
		c, err := upstream.New(upstream.Config{BaseURL: srv.URL})
		if err != nil {
			t.Fatalf("upstream.New: %v", err)
		}
		return c
	}

	jsonClient := newClient(t, func(w http.ResponseWriter, r *http.Request) {
		// A beacon node ignores Accept; assert the client offered the raw variant
		// anyway, then answer JSON to prove that path still works.
		if !strings.Contains(r.Header.Get("Accept"), "application/octet-stream") {
			t.Errorf("client did not offer application/octet-stream; Accept = %q", r.Header.Get("Accept"))
		}
		data := make([]string, len(blobs))
		for i, b := range blobs {
			data[i] = "0x" + hex.EncodeToString(b)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"data": data})
	})
	octetClient := newClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		for _, b := range blobs {
			_, _ = w.Write(b)
		}
	})

	jr, err := jsonClient.Blobs(context.Background(), 1, nil)
	if err != nil {
		t.Fatalf("json Blobs: %v", err)
	}
	or, err := octetClient.Blobs(context.Background(), 1, nil)
	if err != nil {
		t.Fatalf("octet Blobs: %v", err)
	}

	if or.Status != upstream.StatusFound {
		t.Fatalf("octet status = %v, want found", or.Status)
	}
	if len(or.Blobs) != len(jr.Blobs) {
		t.Fatalf("octet returned %d blobs, json %d", len(or.Blobs), len(jr.Blobs))
	}
	for i := range or.Blobs {
		if !bytes.Equal(or.Blobs[i], jr.Blobs[i]) {
			t.Errorf("blob %d differs between the octet and json variants", i)
		}
		if !bytes.Equal(or.Blobs[i], blobs[i]) {
			t.Errorf("blob %d is not the source blob", i)
		}
	}
}

// TestBlobsOctetStreamNonMultiple is the guard the raw variant needs that the
// JSON one does not: a body that arrives whole but is not a whole number of
// blobs is a malformed answer, and the error names the path and the length.
func TestBlobsOctetStreamNonMultiple(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = w.Write(make([]byte, schema.BlobSize+7))
	}))
	defer srv.Close()

	c, err := upstream.New(upstream.Config{BaseURL: srv.URL})
	if err != nil {
		t.Fatalf("upstream.New: %v", err)
	}
	_, err = c.Blobs(context.Background(), 1, nil)
	if err == nil {
		t.Fatal("a non-multiple octet-stream body must be an error")
	}
	if !strings.Contains(err.Error(), "multiple") || !strings.Contains(err.Error(), "blobs/1") {
		t.Errorf("error = %v, want it to name the path and the multiple-of-blob-size rule", err)
	}
}

// TestBlobsOctetStreamRecordsRawBytes is the metric on the raw path: the byte
// counter records the raw blob bytes, exactly as the JSON path does -- not the
// ~2x hex that never crossed the wire here in the first place.
func TestBlobsOctetStreamRecordsRawBytes(t *testing.T) {
	blobs := [][]byte{blobN(1), blobN(2)}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		for _, b := range blobs {
			_, _ = w.Write(b)
		}
	}))
	defer srv.Close()

	mx := metrics.New()
	c, err := upstream.New(upstream.Config{BaseURL: srv.URL, Metrics: mx})
	if err != nil {
		t.Fatalf("upstream.New: %v", err)
	}
	res, err := c.Blobs(context.Background(), 1, nil)
	if err != nil {
		t.Fatalf("Blobs: %v", err)
	}
	if res.Status != upstream.StatusFound || len(res.Blobs) != len(blobs) {
		t.Fatalf("Blobs = %v with %d blobs, want two found blobs", res.Status, len(res.Blobs))
	}

	body := scrape(t, mx)
	if got := mustSample(t, body, "bloar_upstream_read_bytes_total"); got != float64(len(blobs)*schema.BlobSize) {
		t.Errorf("bloar_upstream_read_bytes_total = %g, want %d (two raw blobs, not their hex)", got, len(blobs)*schema.BlobSize)
	}
}

// TestBlobsRetriesBackoffCodes is retryable()'s new rule: a 429 (and a 408) are
// a provider's canonical back-off signals, not terminal failures, so the retry
// loop rides them out. A paid full-history source rate-limiting a backfill must
// not drop the slot it would have served a moment later.
func TestBlobsRetriesBackoffCodes(t *testing.T) {
	for _, code := range []int{http.StatusTooManyRequests, http.StatusRequestTimeout} {
		t.Run(http.StatusText(code), func(t *testing.T) {
			var hits int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if atomic.AddInt32(&hits, 1) == 1 {
					http.Error(w, `{"message":"slow down"}`, code)
					return
				}
				blobJSON(w, blobN(3))
			}))
			defer srv.Close()

			c, err := upstream.New(upstream.Config{BaseURL: srv.URL, MaxAttempts: 3, Backoff: time.Millisecond})
			if err != nil {
				t.Fatalf("upstream.New: %v", err)
			}
			res, err := c.Blobs(context.Background(), 5, nil)
			if err != nil {
				t.Fatalf("Blobs after a %d: %v", code, err)
			}
			if res.Status != upstream.StatusFound || len(res.Blobs) != 1 {
				t.Fatalf("Blobs = %v with %d blobs, want the retried blob", res.Status, len(res.Blobs))
			}
			if got := atomic.LoadInt32(&hits); got != 2 {
				t.Errorf("server was hit %d times, want 2 (the %d, then the retry)", got, code)
			}
		})
	}
}

// TestOriginSlot is mirror mode's one validation input: an archive upstream's
// origin_slot, read from GET /bloar/v1/heads/{head}.
func TestOriginSlot(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/bloar/v1/heads/all" {
			t.Errorf("OriginSlot read %s, want /bloar/v1/heads/all", r.URL.Path)
		}
		writeJSON(w, http.StatusOK, map[string]any{"name": "all", "origin_slot": 4700, "synced_to": 5000})
	}))
	defer srv.Close()

	c, err := upstream.New(upstream.Config{BaseURL: srv.URL, Head: "all"})
	if err != nil {
		t.Fatalf("upstream.New: %v", err)
	}
	origin, err := c.OriginSlot(context.Background())
	if err != nil {
		t.Fatalf("OriginSlot: %v", err)
	}
	if origin != 4700 {
		t.Errorf("OriginSlot = %d, want 4700", origin)
	}
}

// TestBlockClientFinalizedSlot is anchored mode's finality bound, and the two
// ways it says "wait" rather than "error": an optimistic head is not a read
// bound (spec 10.3), and a 503 is prysm answering SYNCING.
func TestBlockClientFinalizedSlot(t *testing.T) {
	tests := []struct {
		name     string
		status   int
		body     string
		wantSlot uint64
		wantOK   bool
		wantErr  bool
	}{
		{"finalized", http.StatusOK, `{"execution_optimistic":false,"finalized":true,"data":{"canonical":true,"header":{"message":{"slot":"8626178"}}}}`, 8626178, true, false},
		{"optimistic", http.StatusOK, `{"execution_optimistic":true,"finalized":true,"data":{"canonical":true,"header":{"message":{"slot":"8626178"}}}}`, 0, false, false},
		{"syncing 503", http.StatusServiceUnavailable, `{"message":"beacon node is currently syncing"}`, 0, false, false},
		{"garbage slot", http.StatusOK, `{"execution_optimistic":false,"finalized":true,"data":{"canonical":true,"header":{"message":{"slot":"not-a-number"}}}}`, 0, false, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tt.status)
				fmt.Fprint(w, tt.body)
			}))
			defer srv.Close()

			b, err := upstream.NewBlockClient(upstream.Config{BaseURL: srv.URL, MaxAttempts: 1, Backoff: time.Millisecond})
			if err != nil {
				t.Fatalf("NewBlockClient: %v", err)
			}
			slot, ok, err := b.FinalizedSlot(context.Background())
			if tt.wantErr != (err != nil) {
				t.Fatalf("FinalizedSlot err = %v, wantErr %v", err, tt.wantErr)
			}
			if slot != tt.wantSlot || ok != tt.wantOK {
				t.Errorf("FinalizedSlot = (%d, %v), want (%d, %v)", slot, ok, tt.wantSlot, tt.wantOK)
			}
		})
	}
}

// TestBlockClientHeaderAndCommitments is the per-slot authority: a present slot
// yields its root and parent_root; a 404 is present=false (a candidate missed
// slot); and blinded_blocks yields the block's commitments, empty for a blobless
// slot.
func TestBlockClientHeaderAndCommitments(t *testing.T) {
	root := "0x" + strings.Repeat("11", 32)
	parent := "0x" + strings.Repeat("22", 32)
	commit := "0x" + strings.Repeat("ab", 48)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/eth/v1/beacon/headers/100":
			writeJSON(w, http.StatusOK, map[string]any{
				"execution_optimistic": false,
				"finalized":            true,
				"data": map[string]any{
					"root":      root,
					"canonical": true,
					"header":    map[string]any{"message": map[string]any{"parent_root": parent}},
				},
			})
		case "/eth/v1/beacon/headers/101":
			http.Error(w, `{"message":"not found"}`, http.StatusNotFound)
		case "/eth/v1/beacon/blinded_blocks/100":
			writeJSON(w, http.StatusOK, map[string]any{
				"execution_optimistic": false,
				"finalized":            true,
				"data": map[string]any{"message": map[string]any{"body": map[string]any{
					"blob_kzg_commitments": []string{commit},
				}}},
			})
		case "/eth/v1/beacon/blinded_blocks/101":
			writeJSON(w, http.StatusOK, map[string]any{
				"execution_optimistic": false,
				"finalized":            true,
				"data": map[string]any{"message": map[string]any{"body": map[string]any{
					"blob_kzg_commitments": []string{},
				}}},
			})
		default:
			http.Error(w, `{"message":"unexpected"}`, http.StatusInternalServerError)
		}
	}))
	defer srv.Close()

	b, err := upstream.NewBlockClient(upstream.Config{BaseURL: srv.URL, MaxAttempts: 1, Backoff: time.Millisecond})
	if err != nil {
		t.Fatalf("NewBlockClient: %v", err)
	}

	gotRoot, gotParent, present, err := b.Header(context.Background(), 100)
	if err != nil || !present {
		t.Fatalf("Header(100) = present %v, err %v; want a present slot", present, err)
	}
	if rootHex(gotRoot) != root || rootHex(gotParent) != parent {
		t.Errorf("Header(100) roots = (%s, %s), want (%s, %s)", rootHex(gotRoot), rootHex(gotParent), root, parent)
	}

	_, _, present, err = b.Header(context.Background(), 101)
	if err != nil {
		t.Fatalf("Header(101): %v", err)
	}
	if present {
		t.Error("Header(101) reported present, want a 404 to be present=false")
	}

	commits, err := b.Commitments(context.Background(), 100)
	if err != nil {
		t.Fatalf("Commitments(100): %v", err)
	}
	if len(commits) != 1 || "0x"+hex.EncodeToString(commits[0][:]) != commit {
		t.Errorf("Commitments(100) = %d commitments, want the one block commitment", len(commits))
	}

	commits, err = b.Commitments(context.Background(), 101)
	if err != nil {
		t.Fatalf("Commitments(101): %v", err)
	}
	if len(commits) != 0 {
		t.Errorf("Commitments(101) = %d, want 0 for a blobless slot", len(commits))
	}
}

// blobN builds a distinct BlobSize buffer. The upstream client does not verify
// field elements -- it checks length and splits -- so the bytes need only be
// distinguishable, which makes order and identity meaningful in a test.
func blobN(seed byte) []byte {
	b := make([]byte, schema.BlobSize)
	for i := range b {
		b[i] = seed + byte(i)
	}
	return b
}

// blobJSON writes a 200 {"data":[...]} of the given blobs as hex, the beacon-node
// wire format both real nodes and pre-variant bloar answer.
func blobJSON(w http.ResponseWriter, blobs ...[]byte) {
	data := make([]string, len(blobs))
	for i, b := range blobs {
		data[i] = "0x" + hex.EncodeToString(b)
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"data": data})
}

// rootHex renders a 32-byte root the way the beacon API states it.
func rootHex(r [32]byte) string { return "0x" + hex.EncodeToString(r[:]) }

// writeJSON renders v as a JSON response.
func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

// scrape renders m's registry the way /metrics would.
func scrape(t *testing.T, m *metrics.Metrics) string {
	t.Helper()
	srv := httptest.NewServer(metrics.Handler(m, nil))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/metrics")
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reading /metrics: %v", err)
	}
	return string(body)
}

// mustSample finds one sample by its rendered name-and-labels prefix.
func mustSample(t *testing.T, body, series string) float64 {
	t.Helper()
	for line := range strings.SplitSeq(body, "\n") {
		if v, ok := strings.CutPrefix(line, series+" "); ok {
			var f float64
			if _, err := fmt.Sscanf(v, "%g", &f); err != nil {
				t.Fatalf("parsing %q: %v", line, err)
			}
			return f
		}
	}
	t.Fatalf("series %s is not in the scrape:\n%s", series, body)
	return 0
}
