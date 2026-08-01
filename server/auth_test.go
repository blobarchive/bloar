package server_test

import (
	"bytes"
	"net/http"
	"testing"

	"github.com/blobarchive/bloar/schema"
)

// TestAuth covers spec 7.3 across every endpoint that writes. The table is over
// endpoints rather than one endpoint because the failure this guards against is
// a route that forgot the middleware, which a single-endpoint test cannot see.
func TestAuth(t *testing.T) {
	s := newStack(t, stackOpts{})

	endpoints := []struct {
		name string
		call func(opts ...reqOpt) *http.Response
	}{
		{"POST /bloar/v1/blobs", func(opts ...reqOpt) *http.Response {
			return s.do("POST", "/bloar/v1/blobs", bytes.NewReader(makeBlob(1)), opts...)
		}},
		{"POST refs", func(opts ...reqOpt) *http.Response {
			return s.postJSON("/bloar/v1/heads/"+testHead+"/refs", map[string]any{"rows": []any{}, "synced_to": 12}, opts...)
		}},
		{"POST truncate", func(opts ...reqOpt) *http.Response {
			return s.postJSON("/bloar/v1/heads/"+testHead+"/truncate", map[string]any{"slot": 9, "confirm": testHead}, opts...)
		}},
	}

	credentials := []struct {
		name string
		opt  reqOpt
		want int
	}{
		{"no header", func(*http.Request) {}, http.StatusUnauthorized},
		{"wrong token", func(r *http.Request) { r.Header.Set("Authorization", "Bearer wrong") }, http.StatusUnauthorized},
		{"token prefix", func(r *http.Request) { r.Header.Set("Authorization", "Bearer "+testToken[:4]) }, http.StatusUnauthorized},
		{"no scheme", func(r *http.Request) { r.Header.Set("Authorization", testToken) }, http.StatusUnauthorized},
		{"wrong scheme", func(r *http.Request) { r.Header.Set("Authorization", "Basic "+testToken) }, http.StatusUnauthorized},
		{"empty bearer", func(r *http.Request) { r.Header.Set("Authorization", "Bearer ") }, http.StatusUnauthorized},
		// RFC 6750 makes the scheme name case-insensitive; the token is not.
		{"lowercase scheme", func(r *http.Request) { r.Header.Set("Authorization", "bearer "+testToken) }, 0},
		{"good token", withAuth, 0},
	}

	for _, ep := range endpoints {
		for _, cred := range credentials {
			t.Run(ep.name+"/"+cred.name, func(t *testing.T) {
				resp := ep.call(cred.opt)
				defer resp.Body.Close()

				if cred.want == http.StatusUnauthorized {
					if resp.StatusCode != http.StatusUnauthorized {
						t.Fatalf("status = %d, want 401", resp.StatusCode)
					}
					errorOf(t, resp)
					return
				}
				// A good token must get past auth. What the endpoint then makes
				// of the request is that endpoint's own test; all that matters
				// here is that it is not 401.
				if resp.StatusCode == http.StatusUnauthorized {
					t.Fatalf("a valid token was refused: %s", readAll(t, resp))
				}
			})
		}
	}
}

// TestAuthReadsArePublic covers the other half of spec 7.3: the read API takes
// no credentials, which is the entire point of an archive.
func TestAuthReadsArePublic(t *testing.T) {
	s := newStack(t, stackOpts{})
	vhs := s.put(makeBlob(1))
	s.refs([]map[string]any{row(9, vhs[0])}, 12)

	for _, path := range []string{
		blobsURL(9),
		"/" + testHead + "/eth/v1/beacon/genesis",
		"/" + testHead + "/eth/v1/config/spec",
		"/bloar/v1/heads",
		"/bloar/v1/heads/" + testHead,
		"/bloar/v1/heads/" + testHead + "/synced_to",
	} {
		t.Run(path, func(t *testing.T) {
			resp := s.get(path)
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				t.Errorf("status = %d, want 200 without credentials", resp.StatusCode)
			}
		})
	}
}

// TestPutBlobsMaxCount covers the count limit of spec 7.2.
func TestPutBlobsMaxCount(t *testing.T) {
	s := newStack(t, stackOpts{maxPutBlobs: 2})

	t.Run("at the limit", func(t *testing.T) {
		vhs := s.put(makeBlob(1), makeBlob(2))
		if len(vhs) != 2 {
			t.Fatalf("got %d vhs, want 2", len(vhs))
		}
	})

	t.Run("over the limit", func(t *testing.T) {
		body := append(append(makeBlob(1), makeBlob(2)...), makeBlob(3)...)
		resp := s.do("POST", "/bloar/v1/blobs", bytes.NewReader(body), withAuth)
		defer resp.Body.Close()
		// Spec 7.2 names 400 for both of this endpoint's framing failures.
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400", resp.StatusCode)
		}
		errorOf(t, resp)
	})

	t.Run("over the limit, no content-length", func(t *testing.T) {
		// A chunked body declares no length, so the Content-Length check cannot
		// see this one coming: MaxBytesReader is what stops it, which is why
		// there are two checks and not one.
		body := append(append(makeBlob(1), makeBlob(2)...), makeBlob(3)...)
		req, err := http.NewRequestWithContext(t.Context(), "POST", s.url+"/bloar/v1/blobs", chunked(body))
		if err != nil {
			t.Fatalf("building request: %v", err)
		}
		withAuth(req)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("POST: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400", resp.StatusCode)
		}
	})
}

// TestPutBlobsFraming covers the divisibility rule and the KZG rejection of
// spec 7.2.
func TestPutBlobsFraming(t *testing.T) {
	s := newStack(t, stackOpts{})

	t.Run("not a whole number of blobs", func(t *testing.T) {
		resp := s.do("POST", "/bloar/v1/blobs", bytes.NewReader(makeBlob(1)[:100]), withAuth)
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400", resp.StatusCode)
		}
		errorOf(t, resp)
	})

	t.Run("invalid blob names its index", func(t *testing.T) {
		// The second blob's first field element is above the BLS12-381 modulus,
		// so it has no commitment and no versioned hash.
		bad := makeBlob(2)
		for i := range 32 {
			bad[i] = 0xFF
		}
		body := append(append(makeBlob(1), bad...), makeBlob(3)...)

		resp := s.do("POST", "/bloar/v1/blobs", bytes.NewReader(body), withAuth)
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400", resp.StatusCode)
		}
		body404 := errorOf(t, resp)
		// The index is the only thing that tells a caller which blob to fix.
		if !bytes.Contains([]byte(body404.Message), []byte("blob 1")) {
			t.Errorf("message = %q, want it to name the offending index (blob 1)", body404.Message)
		}
	})

	t.Run("empty body", func(t *testing.T) {
		// Zero blobs is divisible by BlobSize, so it is not a framing error:
		// it ingests nothing and says so.
		resp := s.do("POST", "/bloar/v1/blobs", bytes.NewReader(nil), withAuth)
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, want 200: %s", resp.StatusCode, readAll(t, resp))
		}
		var out struct {
			Blobs []any `json:"blobs"`
		}
		decode(t, resp, &out)
		if len(out.Blobs) != 0 {
			t.Errorf("got %d blobs, want 0", len(out.Blobs))
		}
	})
}

// TestPutBlobsResponse covers the response shape of spec 7.2: the derived
// identity of each blob, in body order.
func TestPutBlobsResponse(t *testing.T) {
	s := newStack(t, stackOpts{})

	resp := s.do("POST", "/bloar/v1/blobs", bytes.NewReader(append(makeBlob(1), makeBlob(2)...)), withAuth)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var out struct {
		Blobs []struct {
			VersionedHash string `json:"versioned_hash"`
			CID           string `json:"cid"`
		} `json:"blobs"`
	}
	decode(t, resp, &out)

	if len(out.Blobs) != 2 {
		t.Fatalf("got %d blobs, want 2", len(out.Blobs))
	}
	for i, b := range out.Blobs {
		if len(b.VersionedHash) != 2+2*schema.VersionedHashSize || b.VersionedHash[:2] != "0x" {
			t.Errorf("blob %d: versioned_hash = %q, want 0x-prefixed %d-byte hex", i, b.VersionedHash, schema.VersionedHashSize)
		}
		// Blobs are raw-codec CIDv1, which base32-encode with a bafk prefix.
		if b.CID[:4] != "bafk" {
			t.Errorf("blob %d: cid = %q, want a raw-codec CIDv1 (bafk...)", i, b.CID)
		}
	}
	if out.Blobs[0].VersionedHash == out.Blobs[1].VersionedHash {
		t.Error("two different blobs got the same versioned hash")
	}
}

// chunked wraps b in a reader with no length, so net/http sends it chunked.
func chunked(b []byte) *chunkedReader { return &chunkedReader{r: bytes.NewReader(b)} }

type chunkedReader struct{ r *bytes.Reader }

func (c *chunkedReader) Read(p []byte) (int, error) { return c.r.Read(p) }
