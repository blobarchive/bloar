package upstream_test

// Audit follow-up (+ addendum) regressions for coverage-bearing presence at the
// upstream client boundary. The finalized-header, per-slot header, and blinded-block
// reads carry the beacon-API safety booleans -- execution_optimistic, finalized, and
// (headers only) data.canonical -- that authorize a slot as trusted; each must be
// explicitly present with a safe value, checked INSIDE the retry loop so a
// malformed-first/corrected-second node recovers within the attempt budget. Also
// covers mirror-data presence retry and origin_slot presence.

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blobarchive/bloar/index/upstream"
)

// absent marks a field a fixture leaves out entirely, distinct from nil (which puts
// an explicit JSON null). putField applies the three states: omit -> absent, nil ->
// null, a bool -> that value.
type absent struct{}

var omit = absent{}

func putField(m map[string]any, key string, v any) {
	if _, isOmit := v.(absent); isOmit {
		return
	}
	m[key] = v
}

// finalizedBody builds a /headers/finalized response with the three safety flags in
// caller-chosen presence states around a slot.
func finalizedBody(slot string, eo, fin, canon any) map[string]any {
	data := map[string]any{"header": map[string]any{"message": map[string]any{"slot": slot}}}
	putField(data, "canonical", canon)
	body := map[string]any{"data": data}
	putField(body, "execution_optimistic", eo)
	putField(body, "finalized", fin)
	return body
}

// headerBody builds a /headers/{slot} response the same way, around a root and
// parent_root.
func headerBody(root, parent string, eo, fin, canon any) map[string]any {
	data := map[string]any{"root": root, "header": map[string]any{"message": map[string]any{"parent_root": parent}}}
	putField(data, "canonical", canon)
	body := map[string]any{"data": data}
	putField(body, "execution_optimistic", eo)
	putField(body, "finalized", fin)
	return body
}

// blindedMetaBody builds a /blinded_blocks/{slot} response (no canonical member,
// per blinded_block.yaml) around a commitments list.
func blindedMetaBody(commits []string, eo, fin any) map[string]any {
	body := map[string]any{"data": map[string]any{"message": map[string]any{"body": map[string]any{
		"blob_kzg_commitments": commits,
	}}}}
	putField(body, "execution_optimistic", eo)
	putField(body, "finalized", fin)
	return body
}

func newFinalizedServer(t *testing.T, body map[string]any) *upstream.BlockClient {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, body)
	}))
	t.Cleanup(srv.Close)
	b, err := upstream.NewBlockClient(upstream.Config{BaseURL: srv.URL, MaxAttempts: 1, Backoff: time.Millisecond})
	if err != nil {
		t.Fatalf("NewBlockClient: %v", err)
	}
	return b
}

// TestFinalizedBoundMetadataPresence is the addendum's finalized-endpoint
// check: the bound F is authorized ONLY when execution_optimistic:false,
// finalized:true, and data.canonical:true are all explicitly present. Any of the
// three missing or null fails closed; execution_optimistic:true is the one "wait";
// finalized:false or canonical:false on THIS endpoint is the node contradicting
// itself and is a failure, not a wait.
func TestFinalizedBoundMetadataPresence(t *testing.T) {
	cases := []struct {
		name    string
		body    map[string]any
		wantOK  bool
		wantErr bool
	}{
		{"all safe authorizes the bound", finalizedBody("123", false, true, true), true, false},
		{"optimistic waits", finalizedBody("123", true, true, true), false, false},
		{"optimistic but finalized omitted still fails", finalizedBody("123", true, omit, true), false, true},
		{"execution_optimistic omitted", finalizedBody("123", omit, true, true), false, true},
		{"execution_optimistic null", finalizedBody("123", nil, true, true), false, true},
		{"finalized omitted", finalizedBody("123", false, omit, true), false, true},
		{"finalized null", finalizedBody("123", false, nil, true), false, true},
		{"finalized false is a contradiction", finalizedBody("123", false, false, true), false, true},
		{"canonical omitted", finalizedBody("123", false, true, omit), false, true},
		{"canonical null", finalizedBody("123", false, true, nil), false, true},
		{"canonical false is a contradiction", finalizedBody("123", false, true, false), false, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := newFinalizedServer(t, tc.body)
			slot, ok, err := b.FinalizedSlot(context.Background())
			if tc.wantErr != (err != nil) {
				t.Fatalf("FinalizedSlot err = %v, wantErr %v", err, tc.wantErr)
			}
			if ok != tc.wantOK {
				t.Errorf("FinalizedSlot ok = %v, want %v (slot %d)", ok, tc.wantOK, slot)
			}
			if tc.wantOK && slot != 123 {
				t.Errorf("FinalizedSlot slot = %d, want 123", slot)
			}
		})
	}
}

// TestHeaderMetadataPresence is the addendum's per-slot header check: a 200
// header for a slot within F must attest execution_optimistic:false, finalized:true,
// data.canonical:true. Any flag missing, null, or unsafe is the node contradicting
// the bound, so the read fails rather than treating the slot as present.
func TestHeaderMetadataPresence(t *testing.T) {
	root := "0x" + strings.Repeat("11", 32)
	parent := "0x" + strings.Repeat("22", 32)
	cases := []struct {
		name        string
		body        map[string]any
		wantPresent bool
		wantErr     bool
	}{
		{"all safe is present", headerBody(root, parent, false, true, true), true, false},
		{"execution_optimistic omitted", headerBody(root, parent, omit, true, true), false, true},
		{"execution_optimistic null", headerBody(root, parent, nil, true, true), false, true},
		{"execution_optimistic true is a typed transient", headerBody(root, parent, true, true, true), false, true},
		{"finalized omitted", headerBody(root, parent, false, omit, true), false, true},
		{"finalized null", headerBody(root, parent, false, nil, true), false, true},
		{"finalized false is a contradiction", headerBody(root, parent, false, false, true), false, true},
		{"canonical omitted", headerBody(root, parent, false, true, omit), false, true},
		{"canonical null", headerBody(root, parent, false, true, nil), false, true},
		{"canonical false is a contradiction", headerBody(root, parent, false, true, false), false, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				writeJSON(w, http.StatusOK, tc.body)
			}))
			defer srv.Close()
			b, err := upstream.NewBlockClient(upstream.Config{BaseURL: srv.URL, MaxAttempts: 1, Backoff: time.Millisecond})
			if err != nil {
				t.Fatalf("NewBlockClient: %v", err)
			}
			_, _, present, err := b.Header(context.Background(), 100)
			if tc.wantErr != (err != nil) {
				t.Fatalf("Header err = %v, wantErr %v", err, tc.wantErr)
			}
			if present != tc.wantPresent {
				t.Errorf("Header present = %v, want %v", present, tc.wantPresent)
			}
			var optimistic *upstream.ExecutionOptimisticError
			if got := errors.As(err, &optimistic); got != (tc.name == "execution_optimistic true is a typed transient") {
				t.Errorf("ExecutionOptimisticError = %t, want %t (error %T %v)",
					got, tc.name == "execution_optimistic true is a typed transient", err, err)
			}
		})
	}
}

// TestCommitmentsMetadataPresence is the addendum's blinded-block check:
// execution_optimistic:false and finalized:true must be explicitly present (the
// blinded response has no canonical). Missing, null, or unsafe fails the read.
func TestCommitmentsMetadataPresence(t *testing.T) {
	commit := "0x" + strings.Repeat("ab", 48)
	cases := []struct {
		name    string
		body    map[string]any
		wantErr bool
	}{
		{"all safe succeeds", blindedMetaBody([]string{commit}, false, true), false},
		{"execution_optimistic omitted", blindedMetaBody([]string{commit}, omit, true), true},
		{"execution_optimistic null", blindedMetaBody([]string{commit}, nil, true), true},
		{"execution_optimistic true is a typed transient", blindedMetaBody([]string{commit}, true, true), true},
		{"finalized omitted", blindedMetaBody([]string{commit}, false, omit), true},
		{"finalized null", blindedMetaBody([]string{commit}, false, nil), true},
		{"finalized false is a contradiction", blindedMetaBody([]string{commit}, false, false), true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				writeJSON(w, http.StatusOK, tc.body)
			}))
			defer srv.Close()
			b, err := upstream.NewBlockClient(upstream.Config{BaseURL: srv.URL, MaxAttempts: 1, Backoff: time.Millisecond})
			if err != nil {
				t.Fatalf("NewBlockClient: %v", err)
			}
			commits, err := b.Commitments(context.Background(), 7)
			if tc.wantErr != (err != nil) {
				t.Fatalf("Commitments err = %v, wantErr %v", err, tc.wantErr)
			}
			if !tc.wantErr && len(commits) != 1 {
				t.Errorf("Commitments = %d, want 1", len(commits))
			}
			var optimistic *upstream.ExecutionOptimisticError
			if got := errors.As(err, &optimistic); got != (tc.name == "execution_optimistic true is a typed transient") {
				t.Errorf("ExecutionOptimisticError = %t, want %t (error %T %v)",
					got, tc.name == "execution_optimistic true is a typed transient", err, err)
			}
		})
	}
}

// TestFinalizedBoundMetadataRetriesWithinAttemptBudget is the finalized-bound
// retry regression: a first response that omits finalized is a retryable failure, so
// MaxAttempts=2 with a corrected all-safe second response yields two requests and an
// authorized bound.
func TestFinalizedBoundMetadataRetriesWithinAttemptBudget(t *testing.T) {
	var calls atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			writeJSON(w, http.StatusOK, finalizedBody("123", false, omit, true)) // omits finalized
			return
		}
		writeJSON(w, http.StatusOK, finalizedBody("123", false, true, true))
	}))
	defer srv.Close()

	b, err := upstream.NewBlockClient(upstream.Config{BaseURL: srv.URL, MaxAttempts: 2, Backoff: time.Millisecond})
	if err != nil {
		t.Fatalf("NewBlockClient: %v", err)
	}
	slot, ok, err := b.FinalizedSlot(context.Background())
	if err != nil || !ok || slot != 123 {
		t.Fatalf("FinalizedSlot after a malformed-first/corrected-second = (%d, %v, %v), want (123, true, nil)", slot, ok, err)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("server saw %d requests, want 2 (a malformed finalized header must re-enter the attempt loop)", got)
	}
}

// TestCommitmentsPresenceRetriesWithinAttemptBudget verifies that a blinded block
// whose blob_kzg_commitments member is absent (its safety metadata otherwise intact)
// is a malformed answer that must be RETRIED within the attempt budget, not counted
// as one terminal request. MaxAttempts=2, malformed first, corrected second -> two
// requests and success.
func TestCommitmentsPresenceRetriesWithinAttemptBudget(t *testing.T) {
	commit := "0x" + strings.Repeat("ab", 48)
	var calls atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/eth/v1/beacon/blinded_blocks/7" {
			http.Error(w, `{"message":"unexpected path"}`, http.StatusInternalServerError)
			return
		}
		if calls.Add(1) == 1 {
			// Safety metadata present, but the required commitments member omitted.
			writeJSON(w, http.StatusOK, map[string]any{
				"execution_optimistic": false, "finalized": true,
				"data": map[string]any{"message": map[string]any{"body": map[string]any{}}},
			})
			return
		}
		writeJSON(w, http.StatusOK, blindedMetaBody([]string{commit}, false, true))
	}))
	defer srv.Close()

	b, err := upstream.NewBlockClient(upstream.Config{BaseURL: srv.URL, MaxAttempts: 2, Backoff: time.Millisecond})
	if err != nil {
		t.Fatalf("NewBlockClient: %v", err)
	}
	commits, err := b.Commitments(context.Background(), 7)
	if err != nil {
		t.Fatalf("Commitments after a malformed-first/corrected-second: %v", err)
	}
	if len(commits) != 1 {
		t.Fatalf("Commitments = %d, want 1", len(commits))
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("server saw %d requests, want 2 (a malformed answer must re-enter the attempt loop, not fail terminally)", got)
	}
}

// twoBodyBlockFeed serves first to the first request and second to every later one,
// counting requests, and returns a MaxAttempts=2 BlockClient over it. It drives a
// retry across two DISTINCT bodies -- the shape the retry-isolation isolation tests turn on.
func twoBodyBlockFeed(t *testing.T, first, second map[string]any) (*upstream.BlockClient, *atomic.Int64) {
	t.Helper()
	calls := new(atomic.Int64)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			writeJSON(w, http.StatusOK, first)
			return
		}
		writeJSON(w, http.StatusOK, second)
	}))
	t.Cleanup(srv.Close)
	b, err := upstream.NewBlockClient(upstream.Config{BaseURL: srv.URL, MaxAttempts: 2, Backoff: time.Millisecond})
	if err != nil {
		t.Fatalf("NewBlockClient: %v", err)
	}
	return b, calls
}

// TestFinalizedNullFirstCorrectedSecondSucceeds is the retry-isolation case
// reproducer: attempt 1 answers execution_optimistic:null (the rest safe), a
// retryable failure; attempt 2 corrects it to execution_optimistic:false with slot
// 123. Because each attempt decodes into a fresh DTO, attempt 2's false is not
// masked by attempt 1's retained Null, so the call succeeds in two requests.
func TestFinalizedNullFirstCorrectedSecondSucceeds(t *testing.T) {
	b, calls := twoBodyBlockFeed(t,
		finalizedBody("123", nil, true, true),
		finalizedBody("123", false, true, true))
	slot, ok, err := b.FinalizedSlot(context.Background())
	if err != nil || !ok || slot != 123 {
		t.Fatalf("FinalizedSlot = (%d, %v, %v), want (123, true, nil): attempt 2's execution_optimistic:false must not "+
			"inherit attempt 1's null", slot, ok, err)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("server saw %d requests, want 2", got)
	}
}

// TestFinalizedComplementaryIncompleteAttemptsDoNotCombine is the general
// hazard: attempt 1 carries finalized+canonical but omits execution_optimistic;
// attempt 2 carries execution_optimistic+canonical but omits finalized. Neither is
// complete on its own, and a shared destination would let attempt 1's finalized
// survive into attempt 2 and pass validation as a synthetic response no request
// returned. Fresh-per-attempt decoding makes both attempts fail, so the call fails.
func TestFinalizedComplementaryIncompleteAttemptsDoNotCombine(t *testing.T) {
	b, calls := twoBodyBlockFeed(t,
		finalizedBody("123", omit, true, true),
		finalizedBody("123", false, omit, true))
	_, ok, err := b.FinalizedSlot(context.Background())
	if err == nil {
		t.Fatal("FinalizedSlot succeeded by combining two incomplete responses; neither attempt was complete on its own")
	}
	if ok {
		t.Error("FinalizedSlot returned ok on a synthesized response")
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("server saw %d requests, want 2", got)
	}
}

// TestHeaderComplementaryIncompleteAttemptsDoNotCombine is the same isolation
// for the multi-flag per-slot header validator.
func TestHeaderComplementaryIncompleteAttemptsDoNotCombine(t *testing.T) {
	root := "0x" + strings.Repeat("11", 32)
	parent := "0x" + strings.Repeat("22", 32)
	b, calls := twoBodyBlockFeed(t,
		headerBody(root, parent, omit, true, true),
		headerBody(root, parent, false, omit, true))
	_, _, present, err := b.Header(context.Background(), 100)
	if err == nil {
		t.Fatal("Header succeeded by combining two incomplete responses")
	}
	if present {
		t.Error("Header returned present on a synthesized response")
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("server saw %d requests, want 2", got)
	}
}

// TestCommitmentsComplementaryIncompleteAttemptsDoNotCombine is the same
// isolation for the blinded-block validator (execution_optimistic + finalized).
func TestCommitmentsComplementaryIncompleteAttemptsDoNotCombine(t *testing.T) {
	commit := "0x" + strings.Repeat("ab", 48)
	b, calls := twoBodyBlockFeed(t,
		blindedMetaBody([]string{commit}, omit, true),
		blindedMetaBody([]string{commit}, false, omit))
	if _, err := b.Commitments(context.Background(), 7); err == nil {
		t.Fatal("Commitments succeeded by combining two incomplete responses")
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("server saw %d requests, want 2", got)
	}
}

// rawTwoResponseURL serves first to the first request and second to every later
// one, counting requests. It carries raw bodies a map[string]any cannot express:
// a mid-decode type error, or a fully-decoded body with trailing garbage.
func rawTwoResponseURL(t *testing.T, first, second string) (string, *atomic.Int64) {
	t.Helper()
	calls := new(atomic.Int64)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		body := second
		if calls.Add(1) == 1 {
			body = first
		}
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return srv.URL, calls
}

// assertFinalizedRejectsPair drives a MaxAttempts=2 FinalizedSlot over first then
// second and requires the call to FAIL after both requests: no bound may be
// assembled from state that spans two responses.
func assertFinalizedRejectsPair(t *testing.T, first, second string) {
	t.Helper()
	url, calls := rawTwoResponseURL(t, first, second)
	b, err := upstream.NewBlockClient(upstream.Config{BaseURL: url, MaxAttempts: 2, Backoff: time.Millisecond})
	if err != nil {
		t.Fatalf("NewBlockClient: %v", err)
	}
	slot, ok, ferr := b.FinalizedSlot(context.Background())
	if ferr == nil {
		t.Fatalf("FinalizedSlot synthesized a bound (%d, %v) across two attempts; no single response authorized it", slot, ok)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("server saw %d requests, want 2", got)
	}
}

// TestFinalizedDecodeErrorDoesNotSynthesizeBound is the decode-error
// reproducer for the finalized bound: a failed-decode attempt must contribute no
// state to the retry, so a coverage-authorizing bound cannot be assembled from two
// responses.
func TestFinalizedDecodeErrorDoesNotSynthesizeBound(t *testing.T) {
	// The load-bearing case, sensitive to fresh-per-attempt allocation: attempt 1
	// sets the authorizing execution_optimistic:false, then fails decode on data ([]
	// where the DTO has an object); attempt 2 supplies finalized:true, data.canonical
	// :true, and slot 123 but OMITS execution_optimistic. Every flag the validator
	// needs is either in attempt 2 or was set by the failed attempt 1 -- so a SHARED
	// destination synthesizes (123,true,nil), and only fresh-per-attempt allocation
	// (attempt 1 contributes nothing) rejects it.
	t.Run("sensitive to fresh-per-attempt", func(t *testing.T) {
		assertFinalizedRejectsPair(t,
			`{"execution_optimistic":false,"data":[]}`,
			`{"finalized":true,"data":{"canonical":true,"header":{"message":{"slot":"123"}}}}`)
	})

	// Defense in depth: the original two-response pair, where attempt 2 also omits
	// finalized/canonical. The addendum's presence gates reject it independently of
	// isolation, so it fails either way -- kept as a second, independent guard.
	t.Run("defense in depth", func(t *testing.T) {
		assertFinalizedRejectsPair(t,
			`{"execution_optimistic":false,"data":[]}`,
			`{"data":{"header":{"message":{"slot":"123"}}}}`)
	})
}

// TestMalformedFlagErrorNamesItsField pins the optionalBool field-path polish:
// a malformed value for any of the three flags produces a decode error that names
// THAT flag, not a shared hardcoded field. One optionalBool method decodes all three,
// so the field name comes from encoding/json's struct-field context, not the method.
func TestMalformedFlagErrorNamesItsField(t *testing.T) {
	cases := []struct {
		name  string
		body  string
		field string
	}{
		{"finalized", `{"execution_optimistic":false,"finalized":"bad","data":{"canonical":true,"header":{"message":{"slot":"1"}}}}`, "finalized"},
		{"canonical", `{"execution_optimistic":false,"finalized":true,"data":{"canonical":"bad","header":{"message":{"slot":"1"}}}}`, "canonical"},
		{"execution_optimistic", `{"execution_optimistic":"bad","finalized":true,"data":{"canonical":true,"header":{"message":{"slot":"1"}}}}`, "execution_optimistic"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(tc.body))
			}))
			defer srv.Close()
			b, err := upstream.NewBlockClient(upstream.Config{BaseURL: srv.URL, MaxAttempts: 1, Backoff: time.Millisecond})
			if err != nil {
				t.Fatalf("NewBlockClient: %v", err)
			}
			_, _, ferr := b.FinalizedSlot(context.Background())
			if ferr == nil {
				t.Fatal("FinalizedSlot accepted a malformed flag value")
			}
			// The dot prefix keeps the field a distinct path segment, so a match cannot
			// be the DTO's own name (finalizedHeaderDTO contains "finalized").
			if !strings.Contains(ferr.Error(), "."+tc.field) {
				t.Errorf("error %q does not name the malformed field %q", ferr.Error(), tc.field)
			}
		})
	}
}

// TestCommitmentsDecodeErrorDoesNotSynthesizeBlobless is the same for the
// blinded block: attempt 1 populates finalized:true and an explicit-empty
// blob_kzg_commitments path, then hits a type error on execution_optimistic (an
// array) that fails the decode after those fields are set; attempt 2 is {}. A
// shared destination leaked attempt 1's state -- execution_optimistic defaults false
// (the safe value), finalized true, commitments [] -- and returned a blobless slot;
// fresh-per-attempt makes the call fail. (The type error must be mid-decode, not
// trailing garbage: json.Unmarshal validates the whole input first, so a syntax
// error populates nothing.)
func TestCommitmentsDecodeErrorDoesNotSynthesizeBlobless(t *testing.T) {
	url, calls := rawTwoResponseURL(t,
		`{"finalized":true,"data":{"message":{"body":{"blob_kzg_commitments":[]}}},"execution_optimistic":[]}`,
		`{}`)
	b, err := upstream.NewBlockClient(upstream.Config{BaseURL: url, MaxAttempts: 2, Backoff: time.Millisecond})
	if err != nil {
		t.Fatalf("NewBlockClient: %v", err)
	}
	if commits, cerr := b.Commitments(context.Background(), 7); cerr == nil {
		t.Fatalf("Commitments synthesized a blobless slot (len %d) from a failed-decode attempt plus an empty retry", len(commits))
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("server saw %d requests, want 2", got)
	}
}

// TestOriginSlotDecodeErrorDoesNotSynthesizeZero is the same for OriginSlot,
// which uses the no-validation getJSON path (so the fix must cover it too): attempt
// 1 allocates the *uint64 then fails to decode "invalid" into it; attempt 2 is {}. A
// shared destination leaked the allocated pointer and returned (0, nil), silently
// satisfying the mirror origin guard. Fresh-per-attempt makes the call fail.
func TestOriginSlotDecodeErrorDoesNotSynthesizeZero(t *testing.T) {
	url, calls := rawTwoResponseURL(t, `{"origin_slot":"invalid"}`, `{}`)
	c, err := upstream.New(upstream.Config{BaseURL: url, Head: "all", MaxAttempts: 2, Backoff: time.Millisecond})
	if err != nil {
		t.Fatalf("upstream.New: %v", err)
	}
	if origin, oerr := c.OriginSlot(context.Background()); oerr == nil {
		t.Fatalf("OriginSlot synthesized origin %d from a failed-decode attempt plus an empty retry", origin)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("server saw %d requests, want 2", got)
	}
}

// TestMirrorDataPresenceRetriesWithinAttemptBudget covers the mirror check:
// parseJSONBlobs returns a retryable transportError for a 200 that omits data, so it
// too recovers within the attempt budget. MaxAttempts=2, malformed first, corrected
// (covered-empty) second -> two requests and a found empty slot.
func TestMirrorDataPresenceRetriesWithinAttemptBudget(t *testing.T) {
	var calls atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			writeJSON(w, http.StatusOK, map[string]any{}) // omits data
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"data": []string{}}) // covered empty
	}))
	defer srv.Close()

	c, err := upstream.New(upstream.Config{BaseURL: srv.URL, Head: "all", MaxAttempts: 2, Backoff: time.Millisecond})
	if err != nil {
		t.Fatalf("upstream.New: %v", err)
	}
	res, err := c.Blobs(context.Background(), 5, nil)
	if err != nil {
		t.Fatalf("Blobs after a malformed-first/corrected-second: %v", err)
	}
	if res.Status != upstream.StatusFound || len(res.Blobs) != 0 {
		t.Fatalf("Blobs = (%v, %d blobs), want a found empty slot", res.Status, len(res.Blobs))
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("server saw %d requests, want 2 (a malformed answer must re-enter the attempt loop)", got)
	}
}

// TestOriginSlotPresence verifies that a mirror upstream's origin_slot is
// present, not defaulted to zero. An explicit 0 is a real origin and accepted; an
// omitted or null field is an error, so a malformed head document cannot silently
// satisfy the origin guard (which trusts any origin at or below the local one).
func TestOriginSlotPresence(t *testing.T) {
	cases := []struct {
		name    string
		body    map[string]any
		wantErr bool
		want    uint64
	}{
		{"present zero accepted", map[string]any{"name": "all", "origin_slot": 0, "synced_to": 5}, false, 0},
		{"present value accepted", map[string]any{"name": "all", "origin_slot": 4700, "synced_to": 5000}, false, 4700},
		{"omitted", map[string]any{"name": "all", "synced_to": 5}, true, 0},
		{"null", map[string]any{"name": "all", "origin_slot": nil, "synced_to": 5}, true, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				writeJSON(w, http.StatusOK, tc.body)
			}))
			defer srv.Close()

			c, err := upstream.New(upstream.Config{BaseURL: srv.URL, Head: "all", MaxAttempts: 1, Backoff: time.Millisecond})
			if err != nil {
				t.Fatalf("upstream.New: %v", err)
			}
			origin, err := c.OriginSlot(context.Background())
			if tc.wantErr != (err != nil) {
				t.Fatalf("OriginSlot err = %v, wantErr %v", err, tc.wantErr)
			}
			if !tc.wantErr && origin != tc.want {
				t.Errorf("OriginSlot = %d, want %d", origin, tc.want)
			}
		})
	}
}
