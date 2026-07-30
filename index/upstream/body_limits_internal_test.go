package upstream

// These regressions verify that blob request/result counts are capped at the
// per-slot ceiling regardless of caller
// input, and the success-body limit is pinned across the decoded-stream matrix
// (gzip expansion, real chunking, declared-over-cap fast-fail, a lying length, the
// exact boundary, and a metadata endpoint).
//
// This file is package-internal so it can reference the unexported ceilings
// directly, and drives some cases through a static RoundTripper (to control the
// declared length and observe reads) and others through a real httptest server (so
// the default transport actually gunzips and dechunks, exercising the reader the
// code consumes).

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blobarchive/bloar/schema"
)

// --- Attacker-scalable blob ceiling -----------------------------------------

// TestBlobsRejectsOverCeilingHashCountWithoutNetwork proves that a request
// for more than the per-slot ceiling of hashes is refused at the start of Blobs,
// before any URL is built or any byte leaves the process -- so the per-request
// blob-body ceiling can never be scaled up by an over-long hash list.
func TestBlobsRejectsOverCeilingHashCountWithoutNetwork(t *testing.T) {
	var calls atomic.Int64
	c, err := New(Config{
		BaseURL: "http://audit.invalid",
		HTTPClient: &http.Client{Transport: rtFunc(func(r *http.Request) (*http.Response, error) {
			calls.Add(1)
			return nil, fmt.Errorf("transport must not be reached")
		})},
		MaxAttempts: 1, Backoff: time.Millisecond,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	vhs := make([]schema.VersionedHash, schema.MaxBlobsPerSlotCeiling+1)
	if _, err := c.Blobs(context.Background(), 1, vhs); err == nil {
		t.Fatalf("Blobs accepted %d hashes, want a refusal at the ceiling of %d", len(vhs), schema.MaxBlobsPerSlotCeiling)
	}
	if n := calls.Load(); n != 0 {
		t.Fatalf("an over-ceiling request made %d transport calls; it must be refused before the network", n)
	}
}

// TestCommitmentsRejectsOverCeilingArray proves anchored mode's commitments
// reader refuses a block that declares more blob commitments than a slot can hold,
// with a precise source error, before those commitments become a hash list that
// would scale the blob-body ceiling downstream.
func TestCommitmentsRejectsOverCeilingArray(t *testing.T) {
	over := schema.MaxBlobsPerSlotCeiling + 1
	commitments := make([]string, over)
	for i := range commitments {
		commitments[i] = "0x" + hex.EncodeToString(make([]byte, 48))
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeBlindedBlock(w, commitments)
	}))
	defer srv.Close()

	bc, err := NewBlockClient(Config{BaseURL: srv.URL, MaxAttempts: 1, Backoff: time.Millisecond})
	if err != nil {
		t.Fatalf("NewBlockClient: %v", err)
	}
	_, err = bc.Commitments(context.Background(), 100)
	if err == nil {
		t.Fatalf("Commitments accepted %d commitments, want a refusal at the ceiling of %d", over, schema.MaxBlobsPerSlotCeiling)
	}
	if !strings.Contains(err.Error(), "more than") {
		t.Errorf("over-ceiling commitments error = %q, want it to name the ceiling", err)
	}
}

// --- Unfiltered result count cap --------------------------------------------

// TestUnfilteredResultCountIsCapped proves that an unfiltered answer
// carrying more blobs than a slot can hold is refused before ingest, even though
// its bytes fit under the JSON-sized ceiling (octet-stream packs ~twice as many
// blobs into the same bytes).
func TestUnfilteredResultCountIsCapped(t *testing.T) {
	over := schema.MaxBlobsPerSlotCeiling + 1
	body := make([]byte, over*schema.BlobSize) // over+ octet-stream blobs, still < the JSON ceiling
	c := staticClient(t, &http.Response{
		StatusCode:    http.StatusOK,
		Header:        http.Header{"Content-Type": []string{"application/octet-stream"}},
		Body:          io.NopCloser(bytes.NewReader(body)),
		ContentLength: int64(len(body)),
	})
	if _, err := c.Blobs(context.Background(), 1, nil); err == nil {
		t.Fatalf("an unfiltered answer of %d blobs was accepted, want a refusal at the ceiling of %d", over, schema.MaxBlobsPerSlotCeiling)
	}
}

// --- Decoded-stream matrix --------------------------------------------------

// TestSuccessBodyDecodedStreamMatrix pins the LimitReader placement across
// the ways a body can arrive: the bound is on the reader the code consumes (the
// decoded, dechunked stream), the declared length is a fast-fail only, and the
// exact boundary is accepted.
func TestSuccessBodyDecodedStreamMatrix(t *testing.T) {
	ceiling := blobsBodyCeiling(1) // a one-hash filtered request's ceiling

	t.Run("gzip-expanded-body-is-bounded", func(t *testing.T) {
		// The compressed body is tiny; decompressed it is far over the ceiling. The
		// default transport gunzips transparently, so the limit -- on resp.Body, the
		// decoded stream -- must catch the expansion.
		var buf bytes.Buffer
		gz := gzip.NewWriter(&buf)
		_, _ = gz.Write(bytes.Repeat([]byte(" "), int(ceiling)+1<<20))
		_ = gz.Close()
		payload := buf.Bytes()
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Encoding", "gzip")
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(payload)
		}))
		defer srv.Close()
		c := realClient(t, srv.URL)
		if _, err := c.Blobs(context.Background(), 1, oneHash()); err == nil {
			t.Fatal("a gzip body that expands past the ceiling was accepted; the limit is below the gunzip layer")
		}
	})

	t.Run("chunked-unknown-length-over-ceiling", func(t *testing.T) {
		// No Content-Length: net/http chunks it, so the declared-length fast-fail
		// cannot apply and only the read limit bounds it.
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			flusher, _ := w.(http.Flusher)
			chunk := bytes.Repeat([]byte(" "), 64<<10)
			for sent := 0; sent <= int(ceiling)+(1<<20); sent += len(chunk) {
				_, _ = w.Write(chunk)
				flusher.Flush()
			}
		}))
		defer srv.Close()
		c := realClient(t, srv.URL)
		if _, err := c.Blobs(context.Background(), 1, oneHash()); err == nil {
			t.Fatal("a chunked body over the ceiling was accepted; the limit did not bound an unknown-length stream")
		}
	})

	t.Run("declared-over-cap-fails-without-reading", func(t *testing.T) {
		// A declared Content-Length over the ceiling is refused before a byte of the
		// body is read: the counting body must show zero reads.
		var read atomic.Int64
		c := staticClient(t, &http.Response{
			StatusCode:    http.StatusOK,
			Header:        http.Header{"Content-Type": []string{"application/json"}},
			Body:          countingBody(&read, int(ceiling)+1<<20),
			ContentLength: ceiling + 1,
		})
		if _, err := c.Blobs(context.Background(), 1, oneHash()); err == nil {
			t.Fatal("a declared-over-ceiling body was accepted")
		}
		if n := read.Load(); n != 0 {
			t.Fatalf("declared-over-ceiling body read %d bytes; the length precheck must fast-fail without reading", n)
		}
	})

	t.Run("lying-declared-length-hits-the-sentinel", func(t *testing.T) {
		// Declares a size under the ceiling but streams far more: the precheck passes
		// (the declaration is a lie), so the LimitReader is the ONLY thing that stops
		// the read. The counting body proves the read is bounded -- it stops at the
		// ceiling plus the one sentinel byte, rather than pulling the whole over-cap
		// stream into memory (which an unbounded read would do before the length check
		// rejected it). This is what makes the LimitReader itself load-bearing.
		var read atomic.Int64
		c := staticClient(t, &http.Response{
			StatusCode:    http.StatusOK,
			Header:        http.Header{"Content-Type": []string{"application/json"}},
			Body:          countingBody(&read, int(ceiling)+1<<20),
			ContentLength: 10, // a lie, well under the ceiling
		})
		if _, err := c.Blobs(context.Background(), 1, oneHash()); err == nil {
			t.Fatal("a body that lied about its length and overran the ceiling was accepted")
		}
		if read.Load() > ceiling+1 {
			t.Fatalf("read %d bytes past the %d-byte ceiling; the limit did not bound the read into memory", read.Load(), ceiling)
		}
	})

	t.Run("exact-boundary-is-accepted", func(t *testing.T) {
		// A well-formed one-blob JSON body padded with trailing whitespace to exactly
		// the ceiling: equal to the limit is not over it, so it must be accepted.
		body := oneBlobJSONPaddedTo(int(ceiling))
		if int64(len(body)) != ceiling {
			t.Fatalf("test body is %d bytes, want exactly the ceiling %d", len(body), ceiling)
		}
		c := staticClient(t, &http.Response{
			StatusCode:    http.StatusOK,
			Header:        http.Header{"Content-Type": []string{"application/json"}},
			Body:          io.NopCloser(bytes.NewReader(body)),
			ContentLength: ceiling,
		})
		res, err := c.Blobs(context.Background(), 1, oneHash())
		if err != nil {
			t.Fatalf("a body exactly at the ceiling was rejected: %v", err)
		}
		if res.Status != StatusFound || len(res.Blobs) != 1 {
			t.Fatalf("exact-boundary body = %v with %d blobs, want one found blob", res.Status, len(res.Blobs))
		}
	})

	t.Run("metadata-endpoint-declared-over-cap", func(t *testing.T) {
		// A metadata endpoint (the beacon finalized header) is bounded by its own,
		// smaller ceiling: a declared length over metaBodyCeiling is refused.
		var read atomic.Int64
		c := staticClient(t, &http.Response{
			StatusCode:    http.StatusOK,
			Header:        http.Header{"Content-Type": []string{"application/json"}},
			Body:          countingBody(&read, int(metaBodyCeiling)+1<<20),
			ContentLength: metaBodyCeiling + 1,
		})
		if _, err := c.beaconFinalizedSlot(context.Background()); err == nil {
			t.Fatal("a metadata body over metaBodyCeiling was accepted")
		}
		if n := read.Load(); n != 0 {
			t.Fatalf("declared-over-ceiling metadata body read %d bytes; want a fast-fail", n)
		}
	})
}

// --- helpers ---------------------------------------------------------------------

// rtFunc adapts a function to http.RoundTripper.
type rtFunc func(*http.Request) (*http.Response, error)

func (f rtFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// staticClient returns a Client whose every request yields resp (a fresh copy of
// its fields per call, so the Body can only be consumed once -- these tests make
// exactly one request each).
func staticClient(t *testing.T, resp *http.Response) *Client {
	t.Helper()
	c, err := New(Config{
		BaseURL: "http://audit.invalid",
		HTTPClient: &http.Client{Transport: rtFunc(func(r *http.Request) (*http.Response, error) {
			resp.Request = r
			return resp, nil
		})},
		MaxAttempts: 1, Backoff: time.Millisecond,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return c
}

// realClient returns a Client over base using the default transport, so gzip and
// chunked transfer are handled the way production handles them.
func realClient(t *testing.T, base string) *Client {
	t.Helper()
	c, err := New(Config{BaseURL: base, MaxAttempts: 1, Backoff: time.Millisecond})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return c
}

func oneHash() []schema.VersionedHash { return []schema.VersionedHash{{0x01}} }

// countingBody is a reader of n space bytes that records how many bytes were read
// through it, so a test can prove a fast-fail path consumed none.
func countingBody(read *atomic.Int64, n int) io.ReadCloser {
	return io.NopCloser(&countingReader{read: read, remaining: n})
}

type countingReader struct {
	read      *atomic.Int64
	remaining int
}

func (r *countingReader) Read(p []byte) (int, error) {
	if r.remaining == 0 {
		return 0, io.EOF
	}
	n := len(p)
	if n > r.remaining {
		n = r.remaining
	}
	for i := range p[:n] {
		p[i] = ' '
	}
	r.remaining -= n
	r.read.Add(int64(n))
	return n, nil
}

// oneBlobJSONPaddedTo renders {"data":["0x<blob>"]} and pads it with trailing
// whitespace (which encoding/json tolerates) to exactly total bytes.
func oneBlobJSONPaddedTo(total int) []byte {
	blob := make([]byte, schema.BlobSize)
	core := `{"data":["0x` + hex.EncodeToString(blob) + `"]}`
	if len(core) > total {
		panic("total smaller than the minimal one-blob body")
	}
	return append([]byte(core), bytes.Repeat([]byte(" "), total-len(core))...)
}

// writeBlindedBlock writes a finalized, non-optimistic blinded-block response
// carrying the given commitments -- the shape BlockClient.Commitments decodes.
func writeBlindedBlock(w http.ResponseWriter, commitments []string) {
	var b strings.Builder
	b.WriteString(`{"execution_optimistic":false,"finalized":true,"data":{"message":{"body":{"blob_kzg_commitments":[`)
	for i, c := range commitments {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteByte('"')
		b.WriteString(c)
		b.WriteByte('"')
	}
	b.WriteString(`]}}}}`)
	w.Header().Set("Content-Type", "application/json")
	_, _ = io.WriteString(w, b.String())
}
