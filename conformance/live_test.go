package conformance

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto/kzg4844"
	"github.com/offchainlabs/nitro/util/blobs"

	"github.com/blobarchive/bloar/server"
)

// TestNitroLiveHead exercises the operator-facing optimistic topology through
// only exported Bloar APIs and Nitro's actual BlobClient. The physical /all
// head remains the authority through slotA. A complete mutable generation
// overlaps it with different data, then extends through slotC. The virtual
// /live view must therefore make an observable authority decision rather than
// merely find whichever physical archive has the requested blob.
func TestNitroLiveHead(t *testing.T) {
	f := makeLiveFixtures(t)
	client := newBlobClient(t, f.base+"/"+testLiveHead, nil)

	t.Run("finalized bytes use Nitro and remain authoritative", func(t *testing.T) {
		got, err := client.GetBlobsBySlot(t.Context(), slotA, f.hashesAt(slotA))
		if err != nil {
			t.Fatalf("GetBlobsBySlot(%d): %v", slotA, err)
		}
		assertFixtureBlobs(t, got, f.blobsAt(slotA))

		resp := getConformanceBlobs(t, f.base, testLiveHead, slotA, f.hashesAt(slotA)...)
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("GET /live finalized: status = %d, body = %s", resp.StatusCode, readAll(t, resp))
		}
		if got := resp.Header.Get("X-Bloar-Finality"); got != "finalized" {
			t.Errorf("X-Bloar-Finality = %q, want finalized", got)
		}
		if got := resp.Header.Get("Cache-Control"); got == "no-store" {
			t.Errorf("finalized Cache-Control = %q, want finalized caching policy", got)
		}
	})

	t.Run("finalized absence never falls back to overlapping mutable data", func(t *testing.T) {
		// The mutable physical head really does carry hashes[1] at slotA.
		physical := getConformanceBlobs(t, f.base, testMutableHead, slotA, f.hashes[1])
		defer physical.Body.Close()
		if physical.StatusCode != http.StatusOK {
			t.Fatalf("mutable overlap is not present: status = %d, body = %s",
				physical.StatusCode, readAll(t, physical))
		}
		if got := physical.Header.Get("Cache-Control"); got != "no-store" {
			t.Errorf("mutable physical Cache-Control = %q, want no-store", got)
		}

		resp := getConformanceBlobs(t, f.base, testLiveHead, slotA, f.hashes[1])
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("GET /live mutable-only hash below frontier: status = %d, want 404; body = %s",
				resp.StatusCode, readAll(t, resp))
		}
		if got := resp.Header.Get("X-Bloar-Finality"); got != "finalized" {
			t.Errorf("X-Bloar-Finality = %q, want finalized", got)
		}
		if got := resp.Header.Get("Cache-Control"); got == "no-store" {
			t.Errorf("finalized absence Cache-Control = %q, want finalized caching policy", got)
		}

		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()
		got, err := client.GetBlobsBySlot(ctx, slotA, f.hashes[1:2])
		if err == nil {
			t.Fatal("Nitro accepted mutable-only data below the finalized frontier")
		}
		if got != nil {
			t.Errorf("Nitro returned %d blobs with the finalized 404", len(got))
		}
		if ctx.Err() != nil || !strings.Contains(err.Error(), "404") {
			t.Errorf("Nitro finalized absence error = %v, context = %v; want prompt 404", err, ctx.Err())
		}
	})

	t.Run("provisional bytes use Nitro and are never cacheable", func(t *testing.T) {
		for _, slot := range []uint64{slotB, slotC} {
			got, err := client.GetBlobsBySlot(t.Context(), slot, f.hashesAt(slot))
			if err != nil {
				t.Fatalf("GetBlobsBySlot(%d): %v", slot, err)
			}
			assertFixtureBlobs(t, got, f.blobsAt(slot))
		}

		resp := getConformanceBlobs(t, f.base, testLiveHead, slotC, f.hashesAt(slotC)...)
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("GET /live provisional: status = %d, body = %s", resp.StatusCode, readAll(t, resp))
		}
		if got := resp.Header.Get("X-Bloar-Finality"); got != "provisional" {
			t.Errorf("X-Bloar-Finality = %q, want provisional", got)
		}
		if got := resp.Header.Get("Cache-Control"); got != "no-store" {
			t.Errorf("provisional Cache-Control = %q, want no-store", got)
		}
	})

	t.Run("writer refuses an internal handoff gap", func(t *testing.T) {
		// With /all finalized only through slotA, a new generation starting at
		// slotC would leave slotB covered by neither physical head. A correctly
		// configured writer cannot manufacture that serving state through its
		// public API; the lower-level server suite separately injects a hostile
		// adopted state to prove the read path also fails it closed.
		handoff := currentPublishedHead(t, f.stack.heads, testHead)
		req := server.GenerationRequest{
			ExpectedGeneration:      1,
			WindowStart:             slotC,
			SyncedTo:                syncedTo + 1,
			Rows:                    []server.GenerationRow{},
			SourceHeadRoot:          "0x" + fmt.Sprintf("%064x", syncedTo+101),
			SourceFinalizedSlot:     slotA,
			SourceFinalizedRoot:     "0x" + fmt.Sprintf("%064x", slotA+100),
			ObservedHandoffRoot:     handoff.Root,
			ObservedHandoffSyncedTo: *handoff.SyncedTo,
		}
		raw, err := json.Marshal(req)
		if err != nil {
			t.Fatalf("marshal gap generation: %v", err)
		}
		resp := f.stack.do(http.MethodPost, "/bloar/v1/heads/"+testMutableHead+"/generation", bytes.NewReader(raw))
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("gap generation status = %d, want 400; body = %s", resp.StatusCode, readAll(t, resp))
		}
	})

	t.Run("uncovered slots fail closed", func(t *testing.T) {
		gapSlot := uint64(syncedTo + 1)
		resp := getConformanceBlobs(t, f.base, testLiveHead, gapSlot, f.hashesAt(slotB)...)
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusServiceUnavailable {
			t.Fatalf("GET /live uncovered slot: status = %d, want 503; body = %s",
				resp.StatusCode, readAll(t, resp))
		}
		if got := resp.Header.Get("Cache-Control"); got != "no-store" {
			t.Errorf("uncovered Cache-Control = %q, want no-store", got)
		}
		if got := resp.Header.Get("Retry-After"); got == "" {
			t.Error("uncovered response has no Retry-After")
		}

		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()
		got, err := client.GetBlobsBySlot(ctx, gapSlot, f.hashesAt(slotB))
		if err == nil {
			t.Fatal("Nitro treated an uncovered /live slot as success")
		}
		if got != nil {
			t.Errorf("Nitro returned %d blobs with the 503", len(got))
		}
		if ctx.Err() != nil || !strings.Contains(err.Error(), "503") {
			t.Errorf("Nitro uncovered error = %v, context = %v; want prompt 503", err, ctx.Err())
		}
	})

	t.Run("physical finalized head remains unchanged", func(t *testing.T) {
		all := newBlobClient(t, f.base+"/"+testHead, nil)
		got, err := all.GetBlobsBySlot(t.Context(), slotA, f.hashesAt(slotA))
		if err != nil {
			t.Fatalf("/all GetBlobsBySlot(%d): %v", slotA, err)
		}
		assertFixtureBlobs(t, got, f.blobsAt(slotA))

		resp := getConformanceBlobs(t, f.base, testHead, slotA, f.hashesAt(slotA)...)
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("GET /all: status = %d, body = %s", resp.StatusCode, readAll(t, resp))
		}
		if got := resp.Header.Get("X-Bloar-Finality"); got != "" {
			t.Errorf("physical /all gained X-Bloar-Finality = %q", got)
		}
	})
}

// makeLiveFixtures ingests real Nitro blobs over the public blob endpoint,
// advances /all over the public refs endpoint, and selects a complete mutable
// generation over the public generation endpoint. No server test helper or
// package-private state is used.
func makeLiveFixtures(t *testing.T) *fixtures {
	t.Helper()
	s := newStack(t, withLive)

	raw := make([]byte, (fixtureBlobs+1)*blobs.BlobEncodableData)
	for i := range raw {
		raw[i] = byte(i*11 + i/blobs.BlobEncodableData)
	}
	encoded, err := blobs.EncodeBlobs(raw)
	if err != nil {
		t.Fatalf("blobs.EncodeBlobs: %v", err)
	}
	if len(encoded) < fixtureBlobs+1 {
		t.Fatalf("EncodeBlobs produced %d blobs, want at least %d", len(encoded), fixtureBlobs+1)
	}
	encoded = encoded[:fixtureBlobs+1]

	_, hashes, err := blobs.ComputeCommitmentsAndHashes(encoded)
	if err != nil {
		t.Fatalf("blobs.ComputeCommitmentsAndHashes: %v", err)
	}
	f := &fixtures{
		stack:  s,
		base:   s.url,
		blobs:  encoded,
		hashes: hashes,
		bySlot: map[uint64][]int{
			slotA: {0, 2},
			slotB: {3},
			slotC: {4, 5},
		},
	}

	body := make([][]byte, 0, fixtureBlobs)
	for i := range fixtureBlobs {
		body = append(body, f.blobs[i][:])
	}
	served := s.put(body...)
	for i, got := range served {
		if want := f.hashes[i].Hex(); !strings.EqualFold(got, want) {
			t.Fatalf("blob %d: bloard computed versioned hash %s, Nitro computed %s", i, got, want)
		}
	}

	// /all is authoritative through slotA and deliberately does not name blob
	// 1. The mutable generation does name it at the same slot, creating the
	// overlap needed to prove that /live never falls back below the frontier.
	s.refs([]map[string]any{{
		"slot":             slotA,
		"versioned_hashes": []string{f.hashes[0].Hex(), f.hashes[2].Hex()},
	}}, slotA)
	handoff := currentPublishedHead(t, s.heads, testHead)

	req := server.GenerationRequest{
		ExpectedGeneration: 0,
		WindowStart:        slotA,
		SyncedTo:           syncedTo,
		Rows: []server.GenerationRow{
			{Slot: slotA, VersionedHashes: []string{f.hashes[1].Hex()}},
			{Slot: slotB, VersionedHashes: []string{f.hashes[3].Hex()}},
			{Slot: slotC, VersionedHashes: []string{f.hashes[4].Hex(), f.hashes[5].Hex()}},
		},
		SourceHeadRoot:          "0x" + fmt.Sprintf("%064x", syncedTo+100),
		SourceFinalizedSlot:     slotA,
		SourceFinalizedRoot:     "0x" + fmt.Sprintf("%064x", slotA+100),
		ObservedHandoffRoot:     handoff.Root,
		ObservedHandoffSyncedTo: *handoff.SyncedTo,
	}
	rawReq, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("marshal generation request: %v", err)
	}
	resp := s.do(http.MethodPost, "/bloar/v1/heads/"+testMutableHead+"/generation", bytes.NewReader(rawReq))
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST mutable generation: status = %d, body = %s", resp.StatusCode, readAll(t, resp))
	}
	var selected server.GenerationResponse
	if err := json.NewDecoder(resp.Body).Decode(&selected); err != nil {
		t.Fatalf("decode generation response: %v", err)
	}
	if selected.Generation != 1 || selected.WindowStart != slotA || selected.SyncedTo != syncedTo {
		t.Fatalf("selected generation = %+v, want generation=1 window=[%d,%d]", selected, slotA, syncedTo)
	}
	return f
}

func currentPublishedHead(t *testing.T, heads *server.Heads, name string) server.HeadEntry {
	t.Helper()
	var doc server.Doc
	if err := json.Unmarshal(heads.Doc(), &doc); err != nil {
		t.Fatalf("decode current publication: %v", err)
	}
	for _, entry := range doc.Heads {
		if entry.Name == name {
			if entry.SyncedTo == nil {
				t.Fatalf("published handoff head %q is empty", name)
			}
			return entry
		}
	}
	t.Fatalf("published handoff head %q is absent", name)
	return server.HeadEntry{}
}

func getConformanceBlobs(t *testing.T, base, head string, slot uint64, hashes ...common.Hash) *http.Response {
	t.Helper()
	endpoint := fmt.Sprintf("%s/%s/eth/v1/beacon/blobs/%d", base, head, slot)
	query := make(url.Values)
	for _, hash := range hashes {
		query.Add("versioned_hashes", hash.Hex())
	}
	if encoded := query.Encode(); encoded != "" {
		endpoint += "?" + encoded
	}
	resp, err := http.Get(endpoint)
	if err != nil {
		t.Fatalf("GET %s: %v", endpoint, err)
	}
	return resp
}

func assertFixtureBlobs(t *testing.T, got, want []kzg4844.Blob) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %d blobs, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("blob %d is not the ingested bytes", i)
		}
	}
}
