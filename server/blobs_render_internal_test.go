package server

// Internal tests for the exactly-pre-sized JSON blobs renderer of finding
// the safety boundary. They live in package server because renderBlobsJSON, blobsJSONSize and
// the per-entry weight are unexported, and because the renderer must be proven
// byte-identical to the json.Marshal it replaced, which needs blobsResponse.

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"testing"

	"github.com/blobarchive/bloar/schema"
)

// blobsFor returns n blobs of schema.BlobSize each, with distinct-ish content so
// an accidental aliasing bug in the renderer would show, plus the "0x"+hex
// strings the prior json.Marshal path would have built from them.
func blobsFor(n int) (raws [][]byte, data []string) {
	raws = make([][]byte, n)
	data = make([]string, n)
	for i := range raws {
		b := bytes.Repeat([]byte{byte(i + 1)}, schema.BlobSize)
		raws[i] = b
		data[i] = "0x" + hex.EncodeToString(b)
	}
	return raws, data
}

// TestRenderBlobsJSONMatchesMarshal: the renderer is byte-identical to the
// json.Marshal(blobsResponse{...}) it replaced -- so the read API and
// conformance are unchanged -- and its buffer is exactly blobsJSONSize with no
// growth (len == cap). The counts span one entry, a few small counts (where a
// growable buffer would repeatedly double), and the full ceiling.
func TestRenderBlobsJSONMatchesMarshal(t *testing.T) {
	for _, n := range []int{0, 1, 2, 3, 7, 64, schema.MaxBlobsPerSlotCeiling} {
		raws, data := blobsFor(n)
		want, err := json.Marshal(blobsResponse{Data: data})
		if err != nil {
			t.Fatalf("n=%d: json.Marshal: %v", n, err)
		}
		got := renderBlobsJSON(raws)
		if !bytes.Equal(got, want) {
			t.Errorf("n=%d: renderBlobsJSON output differs from json.Marshal", n)
		}
		if len(got) != blobsJSONSize(n) {
			t.Errorf("n=%d: rendered %d bytes, blobsJSONSize says %d", n, len(got), blobsJSONSize(n))
		}
		if cap(got) != blobsJSONSize(n) {
			t.Errorf("n=%d: buffer cap %d != exact size %d, so it grew", n, cap(got), blobsJSONSize(n))
		}
	}
}

// TestRenderBlobsJSONEmptyIsArrayNotNull holds the one JSON hazard the old path
// guarded by hand: a covered slot carrying nothing must render {"data":[]}, not
// {"data":null}.
func TestRenderBlobsJSONEmptyIsArrayNotNull(t *testing.T) {
	if got := string(renderBlobsJSON(nil)); got != `{"data":[]}` {
		t.Errorf("renderBlobsJSON(nil) = %q, want {\"data\":[]}", got)
	}
}

// TestJSONWeightIsAProvableBound is the safety boundary's core memory claim: for every
// entry count a response can carry, the reserved weight (entries *
// weightPerEntryJSON) dominates the true peak live memory -- the raws plus the
// exact rendered buffer, the only two things simultaneously live during encode.
func TestJSONWeightIsAProvableBound(t *testing.T) {
	for _, n := range []int{1, 2, 3, 64, 127, schema.MaxBlobsPerSlotCeiling} {
		peak := int64(n)*int64(schema.BlobSize) + int64(blobsJSONSize(n))
		budget := int64(n) * weightPerEntryJSON
		if budget < peak {
			t.Errorf("n=%d: reserved budget %d < peak memory %d; the weight is not an upper bound", n, budget, peak)
		}
	}
}
