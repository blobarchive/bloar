package upstream_test

// Regression for the safety boundary: a successful upstream body is now read
// through a protocol-derived ceiling, so a source that streams past what the
// endpoint could legitimately return is refused, and the client never accumulates
// the overrun. Before the fix this test's source -- ceiling-plus padding followed
// by an empty data array -- was read whole and accepted; it now fails closed and
// the reader stops at the ceiling.

import (
	"context"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/blobarchive/bloar/index/upstream"
	"github.com/blobarchive/bloar/schema"
)

func TestSuccessfulBlobBodyIsBounded(t *testing.T) {
	// A single-hash filtered request: its ceiling is one blob's JSON form plus
	// slack, a few hundred KiB, so the test proves the bound without allocating
	// tens of MiB. The padding overruns that ceiling several times over.
	vh := schema.VersionedHash{0x01}
	const ceiling = int64(2*schema.BlobSize + 8 + 1024) // must match blobsBodyCeiling(1)
	padding := 4 * ceiling

	spaces := &auditSpaceReader{remaining: padding}
	body := io.NopCloser(io.MultiReader(spaces, auditStringReader(`{"data":[]}`)))
	hc := &http.Client{Transport: auditStaticResponseTransport{response: &http.Response{
		StatusCode:    http.StatusOK,
		Header:        http.Header{"Content-Type": []string{"application/json"}},
		Body:          body,
		ContentLength: -1,
	}}}
	c, err := upstream.New(upstream.Config{
		BaseURL: "http://audit.invalid", HTTPClient: hc,
		MaxAttempts: 1, Backoff: time.Millisecond,
	})
	if err != nil {
		t.Fatalf("upstream.New: %v", err)
	}

	res, err := c.Blobs(context.Background(), 1, []schema.VersionedHash{vh})
	if err == nil {
		t.Fatalf("Blobs accepted an over-ceiling body: status %v, %d blobs", res.Status, len(res.Blobs))
	}
	// The overrun probe is one byte past the ceiling: the client reads at most the
	// ceiling plus that sentinel, never the whole padding stream.
	if spaces.read > ceiling+1 {
		t.Fatalf("client consumed %d padding bytes, want at most the %d-byte ceiling plus a sentinel", spaces.read, ceiling+1)
	}
	t.Logf("the client refused the body after %d bytes (ceiling %d), not the %d bytes offered", spaces.read, ceiling, padding)
}

type auditStaticResponseTransport struct{ response *http.Response }

func (tr auditStaticResponseTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	tr.response.Request = r
	return tr.response, nil
}

type auditSpaceReader struct {
	remaining int64
	read      int64
}

func (r *auditSpaceReader) Read(p []byte) (int, error) {
	if r.remaining == 0 {
		return 0, io.EOF
	}
	n := len(p)
	if int64(n) > r.remaining {
		n = int(r.remaining)
	}
	for i := range p[:n] {
		p[i] = ' '
	}
	r.remaining -= int64(n)
	r.read += int64(n)
	return n, nil
}

type auditStringReader string

func (r auditStringReader) Read(p []byte) (int, error) {
	return copy(p, r), io.EOF
}
