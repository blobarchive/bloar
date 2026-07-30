package server_test

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ipfs/go-cid"

	"github.com/blobarchive/bloar/archive"
	"github.com/blobarchive/bloar/ingest"
	"github.com/blobarchive/bloar/metrics"
	"github.com/blobarchive/bloar/schema"
	"github.com/blobarchive/bloar/server"
)

const liveHead = "live"

type blockingAdoptionRoots struct {
	*server.RootStore
	block   atomic.Bool
	fail    atomic.Bool
	once    sync.Once
	entered chan struct{}
	release chan struct{}
}

func (r *blockingAdoptionRoots) Put(ctx context.Context, name string, root cid.Cid) error {
	if r.block.Load() && name == mutableHead {
		r.once.Do(func() { close(r.entered) })
		<-r.release
	}
	if r.fail.Load() && name == mutableHead {
		return errors.New("test: injected follower root-mirror failure")
	}
	return r.RootStore.Put(ctx, name, root)
}

func liveHTTPServer(t *testing.T, f *generationFixture, horizon uint64) *httptest.Server {
	return liveHTTPServerWithView(t, f, horizon, server.LiveHead{
		FinalizedHead: testHead, UnfinalizedHead: mutableHead,
	})
}

func liveHTTPServerWithView(t *testing.T, f *generationFixture, horizon uint64, view server.LiveHead) *httptest.Server {
	return liveHTTPServerWithViewAndMetrics(t, f, horizon, view, nil)
}

func liveHTTPServerWithViewAndMetrics(t *testing.T, f *generationFixture, horizon uint64, view server.LiveHead, mx *metrics.Metrics) *httptest.Server {
	return liveHTTPServerWithViewsAndMetrics(t, f, horizon, map[string]server.LiveHead{liveHead: view}, mx)
}

func liveHTTPServerWithViewsAndMetrics(t *testing.T, f *generationFixture, horizon uint64, views map[string]server.LiveHead, mx *metrics.Metrics) *httptest.Server {
	t.Helper()
	ingester, err := ingest.New(ingest.Config{Blocks: f.st.Blocks(), Catalog: f.cat})
	if err != nil {
		t.Fatal(err)
	}
	handler, err := server.New(server.Config{
		Heads: f.heads, Blocks: f.st.Blocks(), Ingester: ingester, AuthToken: testToken,
		Beacon: server.Beacon{
			GenesisTime: 1606824023, SecondsPerSlot: 12,
			GenesisValidatorsRoot: "0x" + fmt.Sprintf("%064x", 1),
			GenesisForkVersion:    "0x00000000",
			Spec:                  map[string]string{"DEPOSIT_CHAIN_ID": "1"},
		},
		LiveHeads:             views,
		ImmutableHorizonSlots: horizon,
		Metrics:               mx,
	})
	if err != nil {
		t.Fatal(err)
	}
	return httptest.NewServer(handler)
}

func TestLiveHeadMetricsKeepConfiguredAliasBoundedAndVisible(t *testing.T) {
	f := newGenerationFixture(t, "", nil, nil, nil)
	defer f.close()
	mx := metrics.New()
	httpd := liveHTTPServerWithViewAndMetrics(t, f, 0, server.LiveHead{
		FinalizedHead: testHead, UnfinalizedHead: mutableHead,
	}, mx)
	defer httpd.Close()

	// The configured alias is known even before its first coherent mutable
	// generation. An arbitrary public path remains folded into one bounded label.
	for _, path := range []string{
		"/live/eth/v1/beacon/blobs/11",
		"/attacker-selected-name/eth/v1/beacon/blobs/11",
	} {
		resp, err := http.Get(httpd.URL + path)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		resp.Body.Close()
	}

	metricsHTTP := httptest.NewServer(metrics.Handler(mx, nil))
	defer metricsHTTP.Close()
	resp, err := http.Get(metricsHTTP.URL + "/metrics")
	if err != nil {
		t.Fatalf("GET metrics: %v", err)
	}
	body := readAll(t, resp)
	resp.Body.Close()
	for _, want := range []string{
		`bloar_beacon_reads_total{head="live",status="5xx"} 1`,
		`bloar_beacon_reads_total{head="_unknown",status="4xx"} 1`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics do not contain %q:\n%s", want, body)
		}
	}
	if strings.Contains(body, `head="attacker-selected-name"`) {
		t.Fatalf("untrusted URL segment reached a metric label:\n%s", body)
	}

	// Even an empty caller-owned map is frozen by New. Otherwise a later map
	// mutation could add unbounded labels (and race with concurrent reads).
	callerViews := map[string]server.LiveHead{}
	frozenMetrics := metrics.New()
	frozenHTTP := liveHTTPServerWithViewsAndMetrics(t, f, 0, callerViews, frozenMetrics)
	defer frozenHTTP.Close()
	callerViews["late-caller-mutation"] = server.LiveHead{
		FinalizedHead: testHead, UnfinalizedHead: mutableHead,
	}
	resp, err = http.Get(frozenHTTP.URL + "/late-caller-mutation/eth/v1/beacon/blobs/11")
	if err != nil {
		t.Fatalf("GET late caller mutation: %v", err)
	}
	resp.Body.Close()
	frozenMetricsHTTP := httptest.NewServer(metrics.Handler(frozenMetrics, nil))
	defer frozenMetricsHTTP.Close()
	resp, err = http.Get(frozenMetricsHTTP.URL + "/metrics")
	if err != nil {
		t.Fatalf("GET frozen metrics: %v", err)
	}
	body = readAll(t, resp)
	resp.Body.Close()
	if !strings.Contains(body, `bloar_beacon_reads_total{head="_unknown",status="4xx"} 1`) {
		t.Fatalf("late caller mutation did not stay folded into _unknown:\n%s", body)
	}
	if strings.Contains(body, `head="late-caller-mutation"`) {
		t.Fatalf("late caller mutation reached a metric label:\n%s", body)
	}
}

func adoptLiveFinalized(t *testing.T, f *generationFixture, name string, rows []archive.RefRow, syncedTo uint64) *archive.Head {
	t.Helper()
	head, err := archive.BuildGeneration(t.Context(), f.archive, archive.Params{
		Name: name, Net: testNet, OriginSlot: testOrigin, SegBits: testSegBits, FanoutBits: testFanout,
	}, rows, syncedTo)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.heads.Adopt(t.Context(), head, nil, cid.Undef); err != nil {
		t.Fatal(err)
	}
	return head
}

func testVersionedHash(t *testing.T, text string) schema.VersionedHash {
	t.Helper()
	raw, err := hex.DecodeString(text[2:])
	if err != nil {
		t.Fatal(err)
	}
	var out schema.VersionedHash
	copy(out[:], raw)
	return out
}

func getLive(t *testing.T, base string, slot uint64, hashes ...string) *http.Response {
	t.Helper()
	url := fmt.Sprintf("%s/%s/eth/v1/beacon/blobs/%d", base, liveHead, slot)
	for i, hash := range hashes {
		if i == 0 {
			url += "?"
		} else {
			url += "&"
		}
		url += "versioned_hashes=" + hash
	}
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func responseData(t *testing.T, resp *http.Response) []string {
	t.Helper()
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, body = %s", resp.StatusCode, readAll(t, resp))
	}
	var body struct {
		Data []string `json:"data"`
	}
	decode(t, resp, &body)
	return body.Data
}

func TestLiveHeadFinalizedAuthorityAndProvisionalFallback(t *testing.T) {
	f := newGenerationFixture(t, "", nil, nil, nil)
	defer f.close()

	finalizedVH := f.addBlob(1)
	mutableAtFinalizedVH := f.addBlob(2)
	mutableAboveVH := f.addBlob(3)
	if _, err := f.heads.ApplyRefs(t.Context(), testHead, []archive.RefRow{{
		Slot: 11, VHs: []schema.VersionedHash{testVersionedHash(t, finalizedVH)},
	}}, 11, cid.Undef); err != nil {
		t.Fatal(err)
	}
	req := generationReqAtCurrentHandoff(t, f.heads,
		generationReq(0, 10, 13, []server.GenerationRow{
			{Slot: 11, VersionedHashes: []string{mutableAtFinalizedVH}},
			{Slot: 12, VersionedHashes: []string{mutableAboveVH}},
		}))
	req.SourceFinalizedSlot = 11
	if _, err := f.heads.ReplaceGeneration(t.Context(), mutableHead, req); err != nil {
		t.Fatal(err)
	}

	httpd := liveHTTPServer(t, f, 1)
	defer httpd.Close()

	t.Run("finalized presence wins overlap", func(t *testing.T) {
		resp := getLive(t, httpd.URL, 11)
		if got := resp.Header.Get("X-Bloar-Finality"); got != "finalized" {
			t.Errorf("finality = %q, want finalized", got)
		}
		if got := resp.Header.Get("Cache-Control"); got == "no-store" {
			t.Errorf("finalized response cache = %q", got)
		}
		data := responseData(t, resp)
		if len(data) != 1 || data[0] != "0x010203" {
			t.Fatalf("finalized data = %v, want finalized blob only", data)
		}
	})

	t.Run("old finalized answers retain immutable caching", func(t *testing.T) {
		resp := getLive(t, httpd.URL, 10)
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK || resp.Header.Get("X-Bloar-Finality") != "finalized" ||
			resp.Header.Get("Cache-Control") != "public, max-age=31536000, immutable" {
			t.Fatalf("old finalized response = %d finality=%q cache=%q", resp.StatusCode,
				resp.Header.Get("X-Bloar-Finality"), resp.Header.Get("Cache-Control"))
		}
	})

	t.Run("finalized absence never falls back", func(t *testing.T) {
		resp := getLive(t, httpd.URL, 11, mutableAtFinalizedVH)
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("status = %d, want 404; body=%s", resp.StatusCode, readAll(t, resp))
		}
		if got := resp.Header.Get("X-Bloar-Finality"); got != "finalized" {
			t.Errorf("finality = %q, want finalized", got)
		}
		if got := resp.Header.Get("Cache-Control"); got == "no-store" {
			t.Errorf("finalized absence cache = %q", got)
		}
	})

	t.Run("provisional presence", func(t *testing.T) {
		resp := getLive(t, httpd.URL, 12)
		if got := resp.Header.Get("X-Bloar-Finality"); got != "provisional" {
			t.Errorf("finality = %q, want provisional", got)
		}
		if got := resp.Header.Get("Cache-Control"); got != "no-store" {
			t.Errorf("cache = %q, want no-store", got)
		}
		data := responseData(t, resp)
		if len(data) != 1 || data[0] != "0x030405" {
			t.Fatalf("provisional data = %v", data)
		}
	})

	t.Run("provisional absence", func(t *testing.T) {
		resp := getLive(t, httpd.URL, 12, finalizedVH)
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("status = %d, want 404; body=%s", resp.StatusCode, readAll(t, resp))
		}
		if got := resp.Header.Get("X-Bloar-Finality"); got != "provisional" {
			t.Errorf("finality = %q, want provisional", got)
		}
		if got := resp.Header.Get("Cache-Control"); got != "no-store" {
			t.Errorf("cache = %q, want no-store", got)
		}
	})

	t.Run("outside both coverages is retryable", func(t *testing.T) {
		resp := getLive(t, httpd.URL, 14)
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusServiceUnavailable || resp.Header.Get("Cache-Control") != "no-store" ||
			resp.Header.Get("Retry-After") == "" {
			t.Fatalf("gap response = %d cache=%q retry=%q", resp.StatusCode,
				resp.Header.Get("Cache-Control"), resp.Header.Get("Retry-After"))
		}
	})
}

func TestExactHashLiveHeadPairsFilteredFinalizedWithGlobalMutable(t *testing.T) {
	f := newGenerationFixture(t, "", nil, nil, nil)
	defer f.close()

	const filteredHead = "arb1-finalized"
	filteredVH := f.addBlob(21)
	liveArb1VH := f.addBlob(22)
	foreignLiveVH := f.addBlob(23)
	adoptLiveFinalized(t, f, filteredHead, []archive.RefRow{{
		Slot: 10, VHs: []schema.VersionedHash{testVersionedHash(t, filteredVH)},
	}}, 10)

	req := generationReqAtCurrentHandoff(t, f.heads,
		generationReq(0, 10, 12, []server.GenerationRow{{
			Slot: 11, VersionedHashes: []string{liveArb1VH, foreignLiveVH},
		}}))
	if _, err := f.heads.ReplaceGeneration(t.Context(), mutableHead, req); err != nil {
		t.Fatal(err)
	}

	// The mutable writer is authenticated against testHead, not filteredHead.
	// Constructing this view is the explicit mismatch the exact-hash mode exists
	// to permit; the ordinary mode's validation remains covered below.
	httpd := liveHTTPServerWithView(t, f, 0, server.LiveHead{
		FinalizedHead: filteredHead, UnfinalizedHead: mutableHead, RequireVersionedHashes: true,
	})
	defer httpd.Close()

	t.Run("finalized slots remain enumerable", func(t *testing.T) {
		resp := getLive(t, httpd.URL, 10)
		if resp.Header.Get("X-Bloar-Finality") != "finalized" {
			t.Fatalf("finality = %q, want finalized", resp.Header.Get("X-Bloar-Finality"))
		}
		data := responseData(t, resp)
		if len(data) != 1 || data[0] != "0x151617" {
			t.Fatalf("filtered finalized data = %v", data)
		}
	})

	t.Run("unfiltered provisional enumeration is refused", func(t *testing.T) {
		resp := getLive(t, httpd.URL, 11)
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest || resp.Header.Get("Cache-Control") != "no-store" {
			t.Fatalf("unfiltered provisional = %d cache=%q body=%s", resp.StatusCode,
				resp.Header.Get("Cache-Control"), readAll(t, resp))
		}
		if resp.Header.Get("X-Bloar-Finality") != "provisional" {
			t.Fatalf("finality = %q, want provisional", resp.Header.Get("X-Bloar-Finality"))
		}
	})

	for _, tc := range []struct {
		name string
		hash string
		blob string
	}{
		{name: "filtered chain hash", hash: liveArb1VH, blob: "0x161718"},
		{name: "foreign global hash is not an ACL miss", hash: foreignLiveVH, blob: "0x171819"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resp := getLive(t, httpd.URL, 11, tc.hash)
			if resp.Header.Get("X-Bloar-Finality") != "provisional" || resp.Header.Get("Cache-Control") != "no-store" {
				t.Fatalf("headers finality=%q cache=%q", resp.Header.Get("X-Bloar-Finality"),
					resp.Header.Get("Cache-Control"))
			}
			data := responseData(t, resp)
			if len(data) != 1 || data[0] != tc.blob {
				t.Fatalf("data = %v, want [%s]", data, tc.blob)
			}
		})
	}

	// Exact-hash mode relaxes only the view-to-handoff name equality. Advancing
	// the mutable proof's actual handoff beyond its authenticated source frontier
	// invalidates proofValid and must still fail closed.
	if _, err := f.heads.ApplyRefs(t.Context(), testHead, nil, 11, cid.Undef); err != nil {
		t.Fatal(err)
	}
	resp := getLive(t, httpd.URL, 11, liveArb1VH)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable || resp.Header.Get("Cache-Control") != "no-store" {
		t.Fatalf("invalid-proof response = %d cache=%q body=%s", resp.StatusCode,
			resp.Header.Get("Cache-Control"), readAll(t, resp))
	}
}

func TestExactHashLiveHeadQuarantinedFilteredBoundaryKeepsPhysicalMutable(t *testing.T) {
	f := newGenerationFixture(t, "", nil, nil, nil)
	defer f.close()
	const filteredHead = "arb1-finalized"
	adoptLiveFinalized(t, f, filteredHead, nil, 10)
	req := generationReqAtCurrentHandoff(t, f.heads, generationReq(0, 10, 12, nil))
	if _, err := f.heads.ReplaceGeneration(t.Context(), mutableHead, req); err != nil {
		t.Fatal(err)
	}
	if err := f.heads.Quarantine(filteredHead, "test filtered boundary quarantine"); err != nil {
		t.Fatal(err)
	}
	if physical, ok := f.heads.Get(mutableHead); !ok || physical == nil {
		t.Fatal("global physical mutable head disappeared with filtered-boundary quarantine")
	}
	httpd := liveHTTPServerWithView(t, f, 0, server.LiveHead{
		FinalizedHead: filteredHead, UnfinalizedHead: mutableHead, RequireVersionedHashes: true,
	})
	defer httpd.Close()
	resp := getLive(t, httpd.URL, 11)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable || resp.Header.Get("Cache-Control") != "no-store" {
		t.Fatalf("filtered live response with quarantined boundary = %d cache=%q", resp.StatusCode,
			resp.Header.Get("Cache-Control"))
	}
}

func TestExactHashLiveHeadFilteredHandoffGapFailsClosed(t *testing.T) {
	f := newGenerationFixture(t, "", nil, nil, nil)
	defer f.close()

	const filteredHead = "arb1-finalized"
	adoptLiveFinalized(t, f, filteredHead, nil, 10)
	// The global handoff reaches 11, so a mutable window beginning at 12 is valid
	// against its own proof. It is nevertheless discontinuous from this filtered
	// finalized view, whose authority stops at 10.
	if _, err := f.heads.ApplyRefs(t.Context(), testHead, nil, 11, cid.Undef); err != nil {
		t.Fatal(err)
	}
	liveVH := f.addBlob(24)
	req := generationReq(0, 12, 13, []server.GenerationRow{{
		Slot: 12, VersionedHashes: []string{liveVH},
	}})
	req.SourceFinalizedSlot = 11
	req = generationReqAtCurrentHandoff(t, f.heads, req)
	if _, err := f.heads.ReplaceGeneration(t.Context(), mutableHead, req); err != nil {
		t.Fatal(err)
	}

	httpd := liveHTTPServerWithView(t, f, 0, server.LiveHead{
		FinalizedHead: filteredHead, UnfinalizedHead: mutableHead, RequireVersionedHashes: true,
	})
	defer httpd.Close()
	resp := getLive(t, httpd.URL, 12, liveVH)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable || resp.Header.Get("Cache-Control") != "no-store" ||
		resp.Header.Get("Retry-After") == "" {
		t.Fatalf("handoff gap = %d cache=%q retry=%q body=%s", resp.StatusCode,
			resp.Header.Get("Cache-Control"), resp.Header.Get("Retry-After"), readAll(t, resp))
	}
}

// Admission may wait long enough for finality to advance. The cheap live-view
// probe before the response-memory reservation must never enforce provisional
// exact-hash policy: only the authoritative resolution under the post-admission
// reader lease may decide whether a no-hash request is finalized or provisional.
func TestExactHashLiveHeadRechecksFinalityAfterAdmissionWait(t *testing.T) {
	f := newGenerationFixture(t, "", nil, nil, nil)
	defer f.close()

	finalized11 := f.addBlob(31)
	finalized12 := f.addBlob(32)
	live12 := f.addBlob(33)
	if _, err := f.heads.ApplyRefs(t.Context(), testHead, []archive.RefRow{{
		Slot: 11, VHs: []schema.VersionedHash{testVersionedHash(t, finalized11)},
	}}, 11, cid.Undef); err != nil {
		t.Fatal(err)
	}
	req := generationReq(0, 10, 13, []server.GenerationRow{{
		Slot: 12, VersionedHashes: []string{live12},
	}})
	req.SourceFinalizedSlot = 11
	req = generationReqAtCurrentHandoff(t, f.heads, req)
	if _, err := f.heads.ReplaceGeneration(t.Context(), mutableHead, req); err != nil {
		t.Fatal(err)
	}

	firstGet := make(chan struct{})
	releaseGet := make(chan struct{})
	var once sync.Once
	blocks := &countingBlocks{Blockstore: f.st.Blocks(), fail: map[string]error{}, onGet: func() {
		once.Do(func() {
			close(firstGet)
			<-releaseGet
		})
	}}
	ingester, err := ingest.New(ingest.Config{Blocks: f.st.Blocks(), Catalog: f.cat})
	if err != nil {
		t.Fatal(err)
	}
	handler, err := server.New(server.Config{
		Heads: f.heads, Blocks: blocks, Ingester: ingester, AuthToken: testToken,
		Beacon: server.Beacon{GenesisTime: 1606824023, SecondsPerSlot: 12},
		LiveHeads: map[string]server.LiveHead{liveHead: {
			FinalizedHead: testHead, UnfinalizedHead: mutableHead, RequireVersionedHashes: true,
		}},
		MaxResponseBytesInFlight: server.MaxResponseWeight(schema.MaxBlobsPerSlotCeiling),
	})
	if err != nil {
		t.Fatal(err)
	}

	type result struct {
		code     int
		finality string
		body     string
	}
	fire := func(slot uint64) <-chan result {
		out := make(chan result, 1)
		go func() {
			req := httptest.NewRequest(http.MethodGet,
				fmt.Sprintf("/%s/eth/v1/beacon/blobs/%d", liveHead, slot), nil)
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)
			out <- result{code: rec.Code, finality: rec.Header().Get("X-Bloar-Finality"), body: rec.Body.String()}
		}()
		return out
	}

	// A finalized enumeration holds the complete one-response budget while its
	// first blob is materialized. B probes slot 11 while it is provisional, then
	// must wait at admission rather than returning the old policy's 400.
	aDone := fire(11)
	<-firstGet
	bDone := fire(12)
	time.Sleep(50 * time.Millisecond)
	select {
	case got := <-bDone:
		t.Fatalf("waiting no-hash request completed from stale provisional probe: %+v", got)
	default:
	}

	// Finality advances before B gets the reservation. Its authoritative
	// post-admission resolution must now select the filtered finalized head and
	// allow ordinary enumeration with no versioned_hashes.
	if _, err := f.heads.ApplyRefs(t.Context(), testHead, []archive.RefRow{{
		Slot: 12, VHs: []schema.VersionedHash{testVersionedHash(t, finalized12)},
	}}, 12, cid.Undef); err != nil {
		t.Fatal(err)
	}
	close(releaseGet)
	if got := <-aDone; got.code != http.StatusOK || got.finality != "finalized" {
		t.Fatalf("budget holder = %+v", got)
	}
	if got := <-bDone; got.code != http.StatusOK || got.finality != "finalized" || !strings.Contains(got.body, "0x202122") {
		t.Fatalf("post-wait request = %+v, want finalized slot-11 blob", got)
	}
}

func TestLiveHeadInternalHandoffGapFailsClosed(t *testing.T) {
	f := newGenerationFixture(t, "", nil, nil, nil)
	defer f.close()

	// A correct writer refuses this gap. Build it as an independently followed
	// mutable root so the serving policy itself is still proven fail-closed when
	// an older or malicious publisher presents one.
	tip, err := archive.BuildGeneration(t.Context(), f.archive, archive.Params{
		Name: mutableHead, Net: testNet, OriginSlot: 13, SegBits: testSegBits, FanoutBits: testFanout,
	}, nil, 14)
	if err != nil {
		t.Fatal(err)
	}
	isolated, err := server.NewHeads(server.HeadsConfig{Net: testNet, Roots: f.roots})
	if err != nil {
		t.Fatal(err)
	}
	finalized, ok := f.heads.Get(testHead)
	if !ok {
		t.Fatal("fixture finalized head missing")
	}
	if err := isolated.Add(finalized); err != nil {
		t.Fatal(err)
	}
	invalid := mutablePublicationEntry(t, finalized, tip, 10)
	if err := isolated.AdoptPublished(t.Context(), tip, nil, cid.Undef, invalid); err == nil {
		t.Fatal("gap-bearing mutable publication was adopted")
	}
	f.heads = isolated
	httpd := liveHTTPServer(t, f, 0)
	defer httpd.Close()

	for _, slot := range []uint64{11, 12, 13, 14} {
		resp := getLive(t, httpd.URL, slot)
		resp.Body.Close()
		if resp.StatusCode != http.StatusServiceUnavailable || resp.Header.Get("Cache-Control") != "no-store" {
			t.Errorf("gap slot %d = %d cache=%q", slot, resp.StatusCode, resp.Header.Get("Cache-Control"))
		}
	}
}

func TestLiveHeadStartupAndQuarantineFailClosed(t *testing.T) {
	t.Run("mutable generation zero", func(t *testing.T) {
		f := newGenerationFixture(t, "", nil, nil, nil)
		defer f.close()
		httpd := liveHTTPServer(t, f, 0)
		defer httpd.Close()
		resp := getLive(t, httpd.URL, 11)
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusServiceUnavailable || resp.Header.Get("Cache-Control") != "no-store" {
			t.Fatalf("startup response = %d cache=%q", resp.StatusCode, resp.Header.Get("Cache-Control"))
		}
	})

	t.Run("quarantined mutable affects only provisional range", func(t *testing.T) {
		f := newGenerationFixture(t, "", nil, nil, nil)
		defer f.close()
		if _, err := f.heads.ReplaceGeneration(t.Context(), mutableHead, generationReq(0, 10, 12, nil)); err != nil {
			t.Fatal(err)
		}
		if err := f.heads.Quarantine(mutableHead, "test quarantine"); err != nil {
			t.Fatal(err)
		}
		httpd := liveHTTPServer(t, f, 0)
		defer httpd.Close()

		finalized := getLive(t, httpd.URL, 10)
		finalized.Body.Close()
		if finalized.StatusCode != http.StatusOK || finalized.Header.Get("X-Bloar-Finality") != "finalized" {
			t.Fatalf("finalized response under tip quarantine = %d finality=%q", finalized.StatusCode,
				finalized.Header.Get("X-Bloar-Finality"))
		}
		provisional := getLive(t, httpd.URL, 11)
		defer provisional.Body.Close()
		if provisional.StatusCode != http.StatusServiceUnavailable || provisional.Header.Get("Cache-Control") != "no-store" {
			t.Fatalf("quarantine response = %d cache=%q", provisional.StatusCode,
				provisional.Header.Get("Cache-Control"))
		}
	})

}

func TestLiveHeadMetadataAndPhysicalSurfacesRemainDistinct(t *testing.T) {
	f := newGenerationFixture(t, "", nil, nil, nil)
	defer f.close()
	if _, err := f.heads.ReplaceGeneration(t.Context(), mutableHead, generationReq(0, 10, 12, nil)); err != nil {
		t.Fatal(err)
	}
	httpd := liveHTTPServer(t, f, 0)
	defer httpd.Close()

	for _, suffix := range []string{"eth/v1/beacon/genesis", "eth/v1/config/spec"} {
		physical, err := http.Get(httpd.URL + "/" + testHead + "/" + suffix)
		if err != nil {
			t.Fatal(err)
		}
		physicalBody, _ := io.ReadAll(physical.Body)
		physical.Body.Close()
		virtual, err := http.Get(httpd.URL + "/" + liveHead + "/" + suffix)
		if err != nil {
			t.Fatal(err)
		}
		virtualBody, _ := io.ReadAll(virtual.Body)
		virtual.Body.Close()
		if physical.StatusCode != http.StatusOK || virtual.StatusCode != http.StatusOK || string(physicalBody) != string(virtualBody) {
			t.Fatalf("metadata %s: physical=%d/%s virtual=%d/%s", suffix,
				physical.StatusCode, physicalBody, virtual.StatusCode, virtualBody)
		}
		if physical.Header.Get("X-Bloar-Finality") != "" || virtual.Header.Get("X-Bloar-Finality") != "finalized" {
			t.Fatalf("metadata %s finality: physical=%q virtual=%q", suffix,
				physical.Header.Get("X-Bloar-Finality"), virtual.Header.Get("X-Bloar-Finality"))
		}
	}

	physical := getLive(t, httpd.URL, 11)
	physical.Body.Close()
	if physical.Header.Get("X-Bloar-Finality") != "provisional" {
		t.Fatal("live control request did not select provisional head")
	}
	direct, err := http.Get(fmt.Sprintf("%s/%s/eth/v1/beacon/blobs/11", httpd.URL, mutableHead))
	if err != nil {
		t.Fatal(err)
	}
	direct.Body.Close()
	if direct.Header.Get("X-Bloar-Finality") != "" || direct.Header.Get("Cache-Control") != "no-store" {
		t.Fatalf("physical mutable headers changed: finality=%q cache=%q",
			direct.Header.Get("X-Bloar-Finality"), direct.Header.Get("Cache-Control"))
	}

	docResp, err := http.Get(httpd.URL + "/bloar/v1/heads")
	if err != nil {
		t.Fatal(err)
	}
	defer docResp.Body.Close()
	var doc server.Doc
	if err := json.NewDecoder(docResp.Body).Decode(&doc); err != nil {
		t.Fatal(err)
	}
	for _, head := range doc.Heads {
		if head.Name == liveHead {
			t.Fatal("local live alias leaked into publication document")
		}
	}
	aliasDoc, err := http.Get(httpd.URL + "/bloar/v1/heads/" + liveHead)
	if err != nil {
		t.Fatal(err)
	}
	aliasDoc.Body.Close()
	if aliasDoc.StatusCode != http.StatusNotFound {
		t.Fatalf("virtual publication entry status = %d, want 404", aliasDoc.StatusCode)
	}
}

func TestLiveHeadConcurrentMutableReplacementsStayCoherent(t *testing.T) {
	f := newGenerationFixture(t, "", nil, nil, nil)
	defer f.close()
	if _, err := f.heads.ReplaceGeneration(t.Context(), mutableHead, generationReq(0, 10, 12, nil)); err != nil {
		t.Fatal(err)
	}
	httpd := liveHTTPServer(t, f, 0)
	defer httpd.Close()

	var failed atomic.Bool
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 100 {
				resp, err := http.Get(httpd.URL + "/live/eth/v1/beacon/blobs/11")
				if err != nil {
					failed.Store(true)
					return
				}
				_, _ = io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				if resp.StatusCode != http.StatusOK || resp.Header.Get("X-Bloar-Finality") != "provisional" ||
					resp.Header.Get("Cache-Control") != "no-store" {
					failed.Store(true)
					return
				}
			}
		}()
	}
	for generation := uint64(1); generation <= 100; generation++ {
		req := generationReq(generation, 10, 12, nil)
		req.SourceHeadRoot = "0x" + fmt.Sprintf("%064x", generation+1000)
		if _, err := f.heads.ReplaceGeneration(t.Context(), mutableHead, req); err != nil {
			t.Fatalf("generation %d: %v", generation+1, err)
		}
	}
	wg.Wait()
	if failed.Load() {
		t.Fatal("a concurrent live read observed a torn/unclassified generation")
	}
}

func TestLiveHeadRejectsWriterHandoffMismatch(t *testing.T) {
	f := newGenerationFixture(t, "", nil, nil, nil)
	defer f.close()
	ingester, err := ingest.New(ingest.Config{Blocks: f.st.Blocks(), Catalog: f.cat})
	if err != nil {
		t.Fatal(err)
	}

	_, err = server.New(server.Config{
		Heads: f.heads, Blocks: f.st.Blocks(), Ingester: ingester, AuthToken: testToken,
		Beacon: server.Beacon{SecondsPerSlot: 12},
		LiveHeads: map[string]server.LiveHead{liveHead: {
			FinalizedHead: "different-finalized-head", UnfinalizedHead: mutableHead,
		}},
	})
	if err == nil || !strings.Contains(err.Error(), "handoff head") {
		t.Fatalf("server.New mismatch error = %v, want handoff-head refusal", err)
	}
}

func TestLiveHeadFollowerReadStaysCoherentDuringMutableReAdoption(t *testing.T) {
	f := newGenerationFixture(t, "", nil, nil, nil)
	defer f.close()

	vh := f.addBlob(61)
	params := archive.Params{
		Name: mutableHead, Net: testNet, OriginSlot: 10, SegBits: testSegBits, FanoutBits: testFanout,
	}
	oldTip, err := archive.BuildGeneration(t.Context(), f.archive, params, []archive.RefRow{{
		Slot: 11, VHs: []schema.VersionedHash{testVersionedHash(t, vh)},
	}}, 12)
	if err != nil {
		t.Fatal(err)
	}
	params.OriginSlot = 11
	newTip, err := archive.BuildGeneration(t.Context(), f.archive, params, nil, 14)
	if err != nil {
		t.Fatal(err)
	}

	roots := &blockingAdoptionRoots{
		RootStore: f.roots, entered: make(chan struct{}), release: make(chan struct{}),
	}
	heads, err := server.NewHeads(server.HeadsConfig{Net: testNet, Roots: roots})
	if err != nil {
		t.Fatal(err)
	}
	finalized, ok := f.heads.Get(testHead)
	if !ok {
		t.Fatal("fixture finalized head missing")
	}
	if err := heads.Add(finalized); err != nil {
		t.Fatal(err)
	}
	if err := heads.AdoptPublished(t.Context(), oldTip, nil, cid.Undef,
		mutablePublicationEntry(t, finalized, oldTip, 10)); err != nil {
		t.Fatal(err)
	}

	served := &generationFixture{t: t, st: f.st, cat: f.cat, heads: heads}
	httpd := liveHTTPServer(t, served, 0)
	defer httpd.Close()

	roots.block.Store(true)
	adopted := make(chan error, 1)
	go func() {
		adopted <- heads.AdoptPublished(t.Context(), newTip, nil, cid.Undef,
			mutablePublicationEntry(t, finalized, newTip, 10))
	}()
	released := false
	defer func() {
		if !released {
			close(roots.release)
		}
	}()
	select {
	case <-roots.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("mutable mirror write did not reach the blocking seam")
	}

	// The old generation remains one coherent registry entry while the new root
	// mirror is blocked. A torn {new engine, old window metadata} entry would
	// lose the old generation's blob before the mirror transition commits.
	resp := getLive(t, httpd.URL, 11)
	if resp.StatusCode != http.StatusOK || resp.Header.Get("X-Bloar-Finality") != "provisional" {
		body := readAll(t, resp)
		resp.Body.Close()
		t.Fatalf("read during adoption = %d finality=%q body=%s, want old provisional 200",
			resp.StatusCode, resp.Header.Get("X-Bloar-Finality"), body)
	}
	resp.Body.Close()

	close(roots.release)
	released = true
	if err := <-adopted; err != nil {
		t.Fatalf("AdoptKind(new generation): %v", err)
	}
	after := getLive(t, httpd.URL, 11)
	if after.StatusCode != http.StatusOK || after.Header.Get("X-Bloar-Finality") != "provisional" {
		body := readAll(t, after)
		after.Body.Close()
		t.Fatalf("replacement read = %d finality=%q body=%s, want new provisional 200",
			after.StatusCode, after.Header.Get("X-Bloar-Finality"), body)
	}
	if data := responseData(t, after); len(data) != 0 {
		t.Fatalf("replacement generation retained old-only blob: %v", data)
	}

	// A mirror failure likewise leaves the selected entry wholly unchanged.
	// The old implementation installed the candidate engine before Put and
	// merely inherited the prior durable line.
	roots.block.Store(false)
	roots.fail.Store(true)
	if err := heads.AdoptPublished(t.Context(), oldTip, nil, cid.Undef,
		mutablePublicationEntry(t, finalized, oldTip, 10)); err == nil {
		t.Fatal("re-adoption succeeded despite the injected root-mirror failure")
	}
	selected, ok := heads.Get(mutableHead)
	if !ok || !selected.Root().Equals(newTip.Root()) {
		t.Fatalf("selected mutable root after failed re-adoption = %v (ok=%t), want %s",
			selected, ok, newTip.Root())
	}
}
