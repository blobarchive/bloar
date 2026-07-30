package server_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
)

// The immutable and mutable caching answers of spec 7.1.
const (
	cacheImmutable = "public, max-age=31536000, immutable"
	cacheFresh     = "public, max-age=60"
)

// TestBlobsFilterOrder is the property Nitro's client enforces on its side and
// errors on: exactly one blob per requested versioned hash, in request order.
// Stored order is not request order, and the difference is not cosmetic --
// Nitro matches the response positionally against the hashes it read out of the
// sequencer inbox message.
func TestBlobsFilterOrder(t *testing.T) {
	s := newStack(t, stackOpts{})
	blobs := [][]byte{makeBlob(1), makeBlob(2), makeBlob(3)}
	vhs := s.put(blobs...)
	s.refs([]map[string]any{row(9, vhs[0], vhs[1], vhs[2])}, 12)

	t.Run("request order", func(t *testing.T) {
		got := s.getBlobs(9, vhs[2], vhs[0])
		want := []string{blobHex(blobs[2]), blobHex(blobs[0])}
		assertBlobs(t, got, want)
	})

	t.Run("reversed", func(t *testing.T) {
		got := s.getBlobs(9, vhs[2], vhs[1], vhs[0])
		want := []string{blobHex(blobs[2]), blobHex(blobs[1]), blobHex(blobs[0])}
		assertBlobs(t, got, want)
	})

	t.Run("duplicate vh yields the blob twice", func(t *testing.T) {
		// Not a deduplicating set: the response is one blob per requested
		// hash, and a client that asked twice is answered twice.
		got := s.getBlobs(9, vhs[1], vhs[1])
		want := []string{blobHex(blobs[1]), blobHex(blobs[1])}
		assertBlobs(t, got, want)
	})

	t.Run("exact count", func(t *testing.T) {
		got := s.getBlobs(9, vhs[0])
		if len(got) != 1 {
			t.Fatalf("asked for 1 blob, got %d", len(got))
		}
	})
}

// TestBlobsFilterArrayEncodings pins compatibility with both the Beacon
// OpenAPI's repeated-key array serialization and Base's comma-separated
// serialization. Mixed encoding is accepted because URL libraries and proxies
// may append another array value without rewriting an existing one.
func TestBlobsFilterArrayEncodings(t *testing.T) {
	s := newStack(t, stackOpts{})
	blobs := [][]byte{makeBlob(1), makeBlob(2), makeBlob(3)}
	vhs := s.put(blobs...)
	s.refs([]map[string]any{row(9, vhs[0], vhs[1], vhs[2])}, 12)

	tests := []struct {
		name  string
		query string
	}{
		{
			name:  "OpenAPI repeated keys",
			query: "versioned_hashes=" + vhs[2] + "&versioned_hashes=" + vhs[0],
		},
		{
			name:  "Base comma-separated array",
			query: "versioned_hashes=" + vhs[2] + "," + vhs[0],
		},
		{
			name:  "mixed encodings",
			query: "versioned_hashes=" + vhs[2] + "," + vhs[0] + "&versioned_hashes=" + vhs[1],
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			resp := s.get(blobsURL(9) + "?" + test.query)
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("status = %d, body = %s", resp.StatusCode, readAll(t, resp))
			}
			var body struct {
				Data []string `json:"data"`
			}
			decode(t, resp, &body)

			want := []string{blobHex(blobs[2]), blobHex(blobs[0])}
			if test.name == "mixed encodings" {
				want = append(want, blobHex(blobs[1]))
			}
			assertBlobs(t, body.Data, want)
		})
	}
}

// TestBlobsNoFilter covers the unfiltered read: every blob at the slot, in
// stored order (spec 7.1).
func TestBlobsNoFilter(t *testing.T) {
	s := newStack(t, stackOpts{})
	blobs := [][]byte{makeBlob(10), makeBlob(20), makeBlob(30)}
	vhs := s.put(blobs...)
	// Stored order is the order the refs named, which is the beacon block's.
	s.refs([]map[string]any{row(9, vhs[1], vhs[2], vhs[0])}, 12)

	got := s.getBlobs(9)
	want := []string{blobHex(blobs[1]), blobHex(blobs[2]), blobHex(blobs[0])}
	assertBlobs(t, got, want)
}

// TestBlobsOctetStream covers spec 7.1's raw variant: a client that sends
// Accept: application/octet-stream gets the same blobs, in the same order, as
// the raw concatenation of BlobSize records instead of the hex JSON. This is
// the path that removes the server-side hex-encode on the bloar-to-bloar read.
func TestBlobsOctetStream(t *testing.T) {
	s := newStack(t, stackOpts{horizon: 2})
	blobs := [][]byte{makeBlob(10), makeBlob(20), makeBlob(30)}
	vhs := s.put(blobs...)
	// Stored order is the order the refs named, not the put order.
	s.refs([]map[string]any{row(9, vhs[1], vhs[2], vhs[0])}, 12)

	t.Run("unfiltered, stored order", func(t *testing.T) {
		got := s.getOctet(9)
		assertRawBlobs(t, got, [][]byte{blobs[1], blobs[2], blobs[0]})
	})

	t.Run("filtered, request order", func(t *testing.T) {
		// The same request-order property the JSON path enforces (spec 7.1),
		// against bytes rather than hex.
		got := s.getOctet(9, vhs[2], vhs[0])
		assertRawBlobs(t, got, [][]byte{blobs[2], blobs[0]})
	})

	t.Run("covered blobless slot is an empty body", func(t *testing.T) {
		// Self-framing means nothing carried is nothing written: a 200 with a
		// zero-length body, not the {"data": []} the JSON path answers.
		got := s.getOctet(10)
		if len(got) != 0 {
			t.Fatalf("blobless slot returned %d blobs, want an empty body", len(got))
		}
	})

	t.Run("cache headers match the JSON path", func(t *testing.T) {
		// The horizon boundary sits at slot 10 (synced_to 12, horizon 2), so a
		// pair on either side proves the octet path runs the same caching rule.
		for _, slot := range []uint64{9, 11} {
			jsonResp := s.get(blobsURL(slot))
			jsonResp.Body.Close()
			octetResp := s.get(blobsURL(slot), accept(octetAccept))
			octetResp.Body.Close()
			if j, o := jsonResp.Header.Get("Cache-Control"), octetResp.Header.Get("Cache-Control"); j != o {
				t.Errorf("slot %d: Cache-Control json %q, octet %q; they must agree", slot, j, o)
			}
		}
	})

	t.Run("negotiation variants", func(t *testing.T) {
		// The variant is opt-in and deliberately not general negotiation: an
		// exact application/octet-stream selects it (with or without a q-value or
		// siblings), and nothing else does.
		for _, ah := range []string{octetType, "application/octet-stream;q=0.9", "text/html, application/octet-stream"} {
			resp := s.get(blobsURL(9), accept(ah))
			ct := resp.Header.Get("Content-Type")
			resp.Body.Close()
			if ct != octetType {
				t.Errorf("Accept %q: Content-Type = %q, want %q", ah, ct, octetType)
			}
		}
	})
}

// TestBlobsJSONDefaultUnaffected is the promise Nitro depends on: the JSON body
// is byte-for-byte identical whatever the Accept header says, unless it names
// the octet-stream variant. A near-miss like "application/octet" must not
// trigger the raw path.
func TestBlobsJSONDefaultUnaffected(t *testing.T) {
	s := newStack(t, stackOpts{})
	blobs := [][]byte{makeBlob(10), makeBlob(20), makeBlob(30)}
	vhs := s.put(blobs...)
	s.refs([]map[string]any{row(9, vhs[1], vhs[2], vhs[0])}, 12)

	// The reference is the body a client that never heard of the variant sends:
	// no Accept header at all. It really is the envelope Nitro reads.
	refResp := s.get(blobsURL(9))
	ref := []byte(readAll(t, refResp))
	refResp.Body.Close()
	var refData struct {
		Data []string `json:"data"`
	}
	if err := json.Unmarshal(ref, &refData); err != nil {
		t.Fatalf("reference body is not the JSON envelope: %v\n%s", err, ref)
	}
	assertBlobs(t, refData.Data, []string{blobHex(blobs[1]), blobHex(blobs[2]), blobHex(blobs[0])})

	for _, ah := range []string{"application/json", "*/*", "text/plain", "application/json, */*", "application/xml", "application/octet", "application/octet-streamx"} {
		t.Run("accept="+ah, func(t *testing.T) {
			resp := s.get(blobsURL(9), accept(ah))
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("status = %d, want 200", resp.StatusCode)
			}
			if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
				t.Errorf("Content-Type = %q, want application/json", ct)
			}
			if got := []byte(readAll(t, resp)); !bytes.Equal(got, ref) {
				t.Errorf("Accept %q changed the JSON body; the default must be byte-for-byte identical", ah)
			}
		})
	}
}

// TestBlobsErrorsIgnoreAccept covers spec 7.1's rule that the error responses
// stay JSON whatever the client asked for: only a 200 is ever raw.
func TestBlobsErrorsIgnoreAccept(t *testing.T) {
	s := newStack(t, stackOpts{})
	vhs := s.put(makeBlob(1), makeBlob(2))
	s.refs([]map[string]any{row(9, vhs[0])}, 12)

	t.Run("404 stays JSON", func(t *testing.T) {
		// A covered slot that does not carry the requested vh. errorOf decodes
		// the beacon error shape and asserts the JSON content type.
		resp := s.get(blobsURL(9, vhs[1]), accept(octetAccept))
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("status = %d, want 404", resp.StatusCode)
		}
		errorOf(t, resp)
	})

	t.Run("503 stays JSON", func(t *testing.T) {
		resp := s.get(blobsURL(13), accept(octetAccept))
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusServiceUnavailable {
			t.Fatalf("status = %d, want 503", resp.StatusCode)
		}
		errorOf(t, resp)
	})
}

// TestBlobsBloblessCovered covers the divergence from a real beacon node that
// spec 7.1 calls out: a covered slot that carries nothing is 200 with an empty
// array, not a 404. The archive knows the slot carried no blobs; that is an
// answer, not a miss.
func TestBlobsBloblessCovered(t *testing.T) {
	s := newStack(t, stackOpts{})
	vhs := s.put(makeBlob(1))
	s.refs([]map[string]any{row(9, vhs[0])}, 12)

	got := s.getBlobs(10)
	if got == nil {
		t.Fatal(`data is null; a blobless covered slot must answer {"data": []}`)
	}
	if len(got) != 0 {
		t.Fatalf("blobless slot returned %d blobs, want 0", len(got))
	}
}

// TestBlobsAbsent404 covers the 404 for a covered slot that does not carry a
// requested blob, whose message must name the first missing hash (spec 7.1).
func TestBlobsAbsent404(t *testing.T) {
	s := newStack(t, stackOpts{})
	vhs := s.put(makeBlob(1), makeBlob(2))
	// Only the first is referenced; the second is ingested but at no slot.
	s.refs([]map[string]any{row(9, vhs[0])}, 12)

	// The present hash comes first, so a server that named the wrong one, or
	// the last one, or "some hash", fails here.
	resp := s.get(blobsURL(9, vhs[0], vhs[1]))
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
	body := errorOf(t, resp)
	if !strings.Contains(body.Message, vhs[1]) {
		t.Errorf("message = %q, want it to name the first missing vh %s", body.Message, vhs[1])
	}
	if resp.Header.Get("Cache-Control") == "" {
		t.Error("a covered 404 must carry a Cache-Control header (spec 7.1)")
	}
}

// TestBlobsBeforeOrigin covers the other 404: a slot the head is defined never
// to cover. Spec 4 is explicit that this outranks coverage -- answering 503 for
// it would have a client retry forever.
func TestBlobsBeforeOrigin(t *testing.T) {
	s := newStack(t, stackOpts{})

	for _, name := range []string{"empty head", "covered head"} {
		t.Run(name, func(t *testing.T) {
			resp := s.get(blobsURL(testOrigin - 1))
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusNotFound {
				t.Fatalf("status = %d, want 404 (a slot below origin_slot is never coming)", resp.StatusCode)
			}
			if got := resp.Header.Get("Retry-After"); got != "" {
				t.Errorf("Retry-After = %q; a slot below origin_slot must not invite a retry", got)
			}
			errorOf(t, resp)
		})
		if name == "empty head" {
			vhs := s.put(makeBlob(1))
			s.refs([]map[string]any{row(9, vhs[0])}, 12)
		}
	}
}

// TestBlobsNotYetCovered covers the one retryable answer (spec 7.1).
func TestBlobsNotYetCovered(t *testing.T) {
	s := newStack(t, stackOpts{})
	vhs := s.put(makeBlob(1))
	s.refs([]map[string]any{row(9, vhs[0])}, 12)

	resp := s.get(blobsURL(13))
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", resp.StatusCode)
	}
	if got := resp.Header.Get("Retry-After"); got != "12" {
		t.Errorf("Retry-After = %q, want 12", got)
	}
	if got := resp.Header.Get("Cache-Control"); got != "no-store" {
		t.Errorf("Cache-Control = %q, want no-store; a cached 503 is a client that never gets the blob", got)
	}
	errorOf(t, resp)
}

// TestBlobsCaching walks the immutable horizon of spec 7.1 from both sides. The
// horizon is what makes a CDN able to absorb a syncing Nitro, so the boundary
// is worth pinning exactly: at synced_to - horizon the answer is immutable, one
// slot newer it is not.
func TestBlobsCaching(t *testing.T) {
	// A horizon of 2 slots with synced_to 12 puts the boundary at slot 10.
	s := newStack(t, stackOpts{horizon: 2})
	blobs := [][]byte{makeBlob(1), makeBlob(2)}
	vhs := s.put(blobs...)
	s.refs([]map[string]any{row(9, vhs[0]), row(11, vhs[1])}, 12)

	tests := []struct {
		name string
		slot uint64
		vh   string
		want string
	}{
		{"at the horizon", 10, "", cacheImmutable},
		{"behind the horizon", 9, vhs[0], cacheImmutable},
		{"inside the horizon", 11, vhs[1], cacheFresh},
		{"at synced_to", 12, "", cacheFresh},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var vhs []string
			if tc.vh != "" {
				vhs = []string{tc.vh}
			}
			resp := s.get(blobsURL(tc.slot, vhs...))
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("status = %d, want 200: %s", resp.StatusCode, readAll(t, resp))
			}
			if got := resp.Header.Get("Cache-Control"); got != tc.want {
				t.Errorf("slot %d: Cache-Control = %q, want %q", tc.slot, got, tc.want)
			}
		})
	}

	t.Run("a covered 404 caches like a 200", func(t *testing.T) {
		resp := s.get(blobsURL(9, vhs[1]))
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("status = %d, want 404", resp.StatusCode)
		}
		if got := resp.Header.Get("Cache-Control"); got != cacheImmutable {
			t.Errorf("Cache-Control = %q, want %q: a definitive absence is as immutable as a presence", got, cacheImmutable)
		}
	})
}

// TestBlobsBadSlot covers spec 7.1's decimal-only rule. A real beacon node
// takes named ids here; an archive indexes slots and has no opinion about which
// one is "head".
func TestBlobsBadSlot(t *testing.T) {
	s := newStack(t, stackOpts{})

	// An empty slot is not in here: /blobs/ is a different URL, not a bad slot,
	// and it 404s as an unrouted path.
	for _, slot := range []string{"head", "finalized", "genesis", "justified", "0x1234", "-1", "12.0", "abc", "1_2", " 9", "+9", "09x"} {
		t.Run("slot="+slot, func(t *testing.T) {
			resp := s.get("/" + testHead + "/eth/v1/beacon/blobs/" + slot)
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("slot %q: status = %d, want 400", slot, resp.StatusCode)
			}
			errorOf(t, resp)
		})
	}
}

// TestBlobsBadVH covers a malformed filter.
func TestBlobsBadVH(t *testing.T) {
	s := newStack(t, stackOpts{})

	for _, vh := range []string{"0xdeadbeef", "not-hex", "0x" + strings.Repeat("z", 64)} {
		t.Run("vh="+vh, func(t *testing.T) {
			resp := s.get(blobsURL(9, vh))
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("vh %q: status = %d, want 400", vh, resp.StatusCode)
			}
			errorOf(t, resp)
		})
	}
}

// TestUnknownHead covers spec 7.1's "Unknown {head} -> 404" across every
// per-head endpoint.
func TestUnknownHead(t *testing.T) {
	s := newStack(t, stackOpts{})

	for _, path := range []string{
		"/nope/eth/v1/beacon/blobs/9",
		"/nope/eth/v1/beacon/genesis",
		"/nope/eth/v1/config/spec",
		"/bloar/v1/heads/nope",
		"/bloar/v1/heads/nope/synced_to",
	} {
		t.Run(path, func(t *testing.T) {
			resp := s.get(path)
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusNotFound {
				t.Fatalf("status = %d, want 404", resp.StatusCode)
			}
			errorOf(t, resp)
		})
	}
}

// TestGenesis covers the payload Nitro's Initialize reads. The field names are
// the contract: Nitro unmarshals data.genesis_time as a string and parses it,
// so a number here or a renamed key breaks it at startup.
func TestGenesis(t *testing.T) {
	s := newStack(t, stackOpts{})

	resp := s.get("/" + testHead + "/eth/v1/beacon/genesis")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var out struct {
		Data map[string]string `json:"data"`
	}
	decode(t, resp, &out)

	want := map[string]string{
		"genesis_time":            "1606824023",
		"genesis_validators_root": "0x4b363db94e286120d76eb905340fdd4e54bfe9f06bf33ff6cf5ad27f511bfe95",
		"genesis_fork_version":    "0x00000000",
	}
	for k, v := range want {
		if out.Data[k] != v {
			t.Errorf("data.%s = %q, want %q", k, out.Data[k], v)
		}
	}
}

// TestSpec covers the other half of Initialize: SECONDS_PER_SLOT, from which
// Nitro derives the slot of every blob it asks for.
func TestSpec(t *testing.T) {
	s := newStack(t, stackOpts{})

	resp := s.get("/" + testHead + "/eth/v1/config/spec")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var out struct {
		Data map[string]string `json:"data"`
	}
	decode(t, resp, &out)

	if out.Data["SECONDS_PER_SLOT"] != "12" {
		t.Errorf("data.SECONDS_PER_SLOT = %q, want \"12\"", out.Data["SECONDS_PER_SLOT"])
	}
	if out.Data["DEPOSIT_CHAIN_ID"] != "1" {
		t.Errorf("data.DEPOSIT_CHAIN_ID = %q, want \"1\" (spec_extra passthrough)", out.Data["DEPOSIT_CHAIN_ID"])
	}
}

// assertBlobs compares a data array against the blobs it should carry.
func assertBlobs(t *testing.T, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %d blobs, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			// Not printed: they are 128 KiB each.
			t.Errorf("blob %d is not the one requested at that position", i)
		}
	}
}
