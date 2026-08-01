package kubo_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ipfs/go-cid"

	"github.com/blobarchive/bloar/kubo"
)

func requireProtocolError(t *testing.T, err error) *kubo.ProtocolError {
	t.Helper()
	if err == nil {
		t.Fatal("operation succeeded, want protocol error")
	}
	var protocol *kubo.ProtocolError
	if !errors.As(err, &protocol) {
		t.Fatalf("error = %T %v, want ProtocolError", err, err)
	}
	return protocol
}

func requireStreamError(t *testing.T, err error) *kubo.StreamError {
	t.Helper()
	if err == nil {
		t.Fatal("operation succeeded, want stream error")
	}
	var stream *kubo.StreamError
	if !errors.As(err, &stream) {
		t.Fatalf("error = %T %v, want StreamError", err, err)
	}
	return stream
}

func TestRefsAndPinsRPCContract(t *testing.T) {
	refA := testBlock(t, cid.Raw, "local-ref-a").Cid()
	refB := testBlock(t, cid.Raw, "local-ref-b").Cid()
	const prefix = "/proxy"

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method != http.MethodPost {
			t.Errorf("method = %s, want POST", r.Method)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer "+testToken {
			t.Errorf("Authorization = %q", got)
		}
		if strings.Contains(r.RequestURI, testToken) {
			t.Errorf("bearer token leaked in URI %q", r.RequestURI)
		}
		if got := r.Header.Get("Accept"); got != "application/json" {
			t.Errorf("Accept = %q", got)
		}
		query := r.URL.Query()
		if got := query.Get("encoding"); got != "json" {
			t.Errorf("encoding = %q, want json", got)
		}

		switch r.URL.Path {
		case prefix + "/api/v0/refs/local":
			encoder := json.NewEncoder(w)
			_ = encoder.Encode(map[string]string{"Ref": refA.String(), "Err": ""})
			_ = encoder.Encode(map[string]string{"Ref": refB.String(), "Err": ""})
		case prefix + "/api/v0/pin/ls":
			for key, want := range map[string]string{
				"type": "all", "quiet": "false", "stream": "true", "names": "false",
			} {
				if got := query.Get(key); got != want {
					t.Errorf("pin/ls %s = %q, want %q", key, got, want)
				}
			}
			encoder := json.NewEncoder(w)
			_ = encoder.Encode(map[string]string{"Cid": refA.String(), "Type": "direct"})
			_ = encoder.Encode(map[string]string{"Cid": refB.String(), "Type": "recursive"})
		case prefix + "/api/v0/pin/add":
			for key, want := range map[string]string{
				"arg": refA.String(), "recursive": "true", "progress": "false",
				"fast-provide-root": "false", "fast-provide-dag": "false", "fast-provide-wait": "false",
			} {
				if got := query.Get(key); got != want {
					t.Errorf("pin/add %s = %q, want %q", key, got, want)
				}
			}
			writeJSON(t, w, map[string]any{"Pins": []string{refA.String()}})
		case prefix + "/api/v0/pin/rm":
			if got := query.Get("arg"); got != refB.String() {
				t.Errorf("pin/rm arg = %q, want %q", got, refB)
			}
			if got := query.Get("recursive"); got != "false" {
				t.Errorf("pin/rm recursive = %q, want false", got)
			}
			writeJSON(t, w, map[string]any{"Pins": []string{refB.String()}})
		default:
			http.Error(w, "unexpected endpoint", http.StatusNotFound)
		}
	}))
	defer server.Close()

	client := newClient(t, server.URL+prefix, nil)
	limits := kubo.ListLimits{MaxItems: 4, MaxBytes: 4 << 10}
	refs, err := client.RefsLocal(t.Context(), limits)
	if err != nil {
		t.Fatalf("RefsLocal: %v", err)
	}
	if len(refs) != 2 || !refs[0].Equals(refA) || !refs[1].Equals(refB) {
		t.Fatalf("RefsLocal = %v", refs)
	}
	pins, err := client.PinList(t.Context(), kubo.PinTypeAll, limits)
	if err != nil {
		t.Fatalf("PinList: %v", err)
	}
	if len(pins) != 2 || !pins[0].CID.Equals(refA) || pins[0].Type != kubo.PinTypeDirect ||
		!pins[1].CID.Equals(refB) || pins[1].Type != kubo.PinTypeRecursive {
		t.Fatalf("PinList = %+v", pins)
	}
	if err := client.PinAdd(t.Context(), refA, kubo.PinTypeRecursive); err != nil {
		t.Fatalf("PinAdd: %v", err)
	}
	if err := client.PinRemove(t.Context(), refB, kubo.PinTypeDirect); err != nil {
		t.Fatalf("PinRemove: %v", err)
	}
}

func TestRefsAndPinsValidateBeforeNetwork(t *testing.T) {
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		requests.Add(1)
	}))
	defer server.Close()
	client := newClient(t, server.URL, nil)
	target := testBlock(t, cid.Raw, "validation-target").Cid()

	tests := []struct {
		name string
		call func() error
	}{
		{name: "refs item limit", call: func() error {
			_, err := client.RefsLocal(t.Context(), kubo.ListLimits{MaxBytes: 1})
			return err
		}},
		{name: "refs byte limit", call: func() error {
			_, err := client.RefsLocal(t.Context(), kubo.ListLimits{MaxItems: 1})
			return err
		}},
		{name: "refs overflowing byte limit", call: func() error {
			_, err := client.RefsLocal(t.Context(), kubo.ListLimits{MaxItems: 1, MaxBytes: math.MaxInt64})
			return err
		}},
		{name: "refs over client item cap", call: func() error {
			_, err := client.RefsLocal(t.Context(), kubo.ListLimits{MaxItems: kubo.DefaultMaxStreamItems + 1, MaxBytes: 1})
			return err
		}},
		{name: "refs over client byte cap", call: func() error {
			_, err := client.RefsLocal(t.Context(), kubo.ListLimits{MaxItems: 1, MaxBytes: kubo.DefaultMaxStreamBytes + 1})
			return err
		}},
		{name: "invalid list pin type", call: func() error {
			_, err := client.PinList(t.Context(), kubo.PinType("bogus"), kubo.ListLimits{MaxItems: 1, MaxBytes: 1})
			return err
		}},
		{name: "undefined add CID", call: func() error {
			return client.PinAdd(t.Context(), cid.Undef, kubo.PinTypeDirect)
		}},
		{name: "invalid add pin type", call: func() error {
			return client.PinAdd(t.Context(), target, kubo.PinTypeAll)
		}},
		{name: "undefined remove CID", call: func() error {
			return client.PinRemove(t.Context(), cid.Undef, kubo.PinTypeRecursive)
		}},
		{name: "invalid remove pin type", call: func() error {
			return client.PinRemove(t.Context(), target, kubo.PinTypeIndirect)
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := test.call(); err == nil {
				t.Fatal("operation succeeded")
			}
		})
	}
	if got := requests.Load(); got != 0 {
		t.Fatalf("network requests = %d, want 0", got)
	}
}

func TestRefsAndPinsAllowExplicitUnauthenticatedLoopback(t *testing.T) {
	target := testBlock(t, cid.Raw, "unauthenticated-target").Cid()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "" {
			t.Errorf("Authorization = %q, want absent", got)
		}
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/v0/refs/local":
			_ = json.NewEncoder(w).Encode(map[string]string{"Ref": target.String()})
		case "/api/v0/pin/add":
			_ = json.NewEncoder(w).Encode(map[string]any{"Pins": []string{target.String()}})
		default:
			http.Error(w, "unexpected endpoint", http.StatusNotFound)
		}
	}))
	defer server.Close()
	client, err := kubo.New(kubo.Config{
		BaseURL:              server.URL,
		AllowUnauthenticated: true,
	})
	if err != nil {
		t.Fatalf("kubo.New: %v", err)
	}
	refs, err := client.RefsLocal(t.Context(), kubo.ListLimits{MaxItems: 1, MaxBytes: 1024})
	if err != nil || len(refs) != 1 || !refs[0].Equals(target) {
		t.Fatalf("RefsLocal = %v, %v", refs, err)
	}
	if err := client.PinAdd(t.Context(), target, kubo.PinTypeDirect); err != nil {
		t.Fatalf("PinAdd: %v", err)
	}
}

func TestRefsLocalRejectsMalformedDuplicateLateAndBoundedStreams(t *testing.T) {
	target := testBlock(t, cid.Raw, "refs-stream-target").Cid().String()
	tests := []struct {
		name       string
		body       string
		limits     kubo.ListLimits
		wantRedact bool
		wantStream bool
	}{
		{name: "malformed", body: `{"Ref":`, limits: kubo.ListLimits{MaxItems: 4, MaxBytes: 1024}},
		{name: "unknown field", body: `{"Ref":"` + target + `","Other":true}`, limits: kubo.ListLimits{MaxItems: 4, MaxBytes: 1024}},
		{name: "duplicate field", body: `{"Ref":"` + target + `","Ref":"` + target + `"}`, limits: kubo.ListLimits{MaxItems: 4, MaxBytes: 1024}},
		{name: "empty ref", body: `{}`, limits: kubo.ListLimits{MaxItems: 4, MaxBytes: 1024}},
		{name: "invalid CID redacted", body: `{"Ref":"` + testToken + `"}`, limits: kubo.ListLimits{MaxItems: 4, MaxBytes: 1024}, wantRedact: true},
		{name: "duplicate CID", body: `{"Ref":"` + target + `"}` + "\n" + `{"Ref":"` + target + `"}`, limits: kubo.ListLimits{MaxItems: 4, MaxBytes: 1024}},
		{name: "late item error redacted", body: `{"Ref":"` + target + `"}` + "\n" + `{"Err":"late ` + testToken + `"}`, limits: kubo.ListLimits{MaxItems: 4, MaxBytes: 1024}, wantRedact: true, wantStream: true},
		{name: "trailing scalar", body: `{"Ref":"` + target + `"}` + "\ntrue", limits: kubo.ListLimits{MaxItems: 4, MaxBytes: 1024}},
		{name: "item limit", body: `{"Ref":"` + target + `"}` + "\n" + `{"Ref":"` + testBlock(t, cid.Raw, "second-ref").Cid().String() + `"}`, limits: kubo.ListLimits{MaxItems: 1, MaxBytes: 1024}},
		{name: "byte limit", body: `{"Ref":"` + target + `"}` + strings.Repeat(" ", 128), limits: kubo.ListLimits{MaxItems: 4, MaxBytes: 32}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(test.body))
			}))
			defer server.Close()
			refs, err := newClient(t, server.URL, nil).RefsLocal(t.Context(), test.limits)
			if refs != nil {
				t.Fatalf("partial refs returned on error: %v", refs)
			}
			if test.wantStream {
				stream := requireStreamError(t, err)
				if strings.Contains(stream.Error(), testToken) || !strings.Contains(stream.Error(), "[REDACTED]") {
					t.Fatalf("error was not redacted: %v", stream)
				}
				return
			}
			protocol := requireProtocolError(t, err)
			if test.wantRedact && strings.Contains(protocol.Error(), testToken) {
				t.Fatalf("error was not redacted: %v", protocol)
			}
		})
	}
}

func TestPinListRejectsMalformedDuplicateAndInconsistentItems(t *testing.T) {
	target := testBlock(t, cid.Raw, "pin-list-target").Cid().String()
	other := testBlock(t, cid.Raw, "pin-list-other").Cid().String()
	tests := []struct {
		name   string
		body   string
		filter kubo.PinType
		limits kubo.ListLimits
	}{
		{name: "malformed", body: `{"Cid":`, filter: kubo.PinTypeAll},
		{name: "unknown field", body: `{"Cid":"` + target + `","Type":"direct","Other":1}`, filter: kubo.PinTypeAll},
		{name: "duplicate field", body: `{"Cid":"` + target + `","Cid":"` + target + `","Type":"direct"}`, filter: kubo.PinTypeAll},
		{name: "invalid CID", body: `{"Cid":"not-a-cid","Type":"direct"}`, filter: kubo.PinTypeAll},
		{name: "invalid returned type", body: `{"Cid":"` + target + `","Type":"all"}`, filter: kubo.PinTypeAll},
		{name: "mismatched filter", body: `{"Cid":"` + target + `","Type":"recursive"}`, filter: kubo.PinTypeDirect},
		{name: "unsolicited name", body: `{"Cid":"` + target + `","Type":"direct","Name":"surprise"}`, filter: kubo.PinTypeAll},
		{name: "duplicate CID", body: `{"Cid":"` + target + `","Type":"direct"}` + "\n" + `{"Cid":"` + target + `","Type":"recursive"}`, filter: kubo.PinTypeAll},
		{name: "item limit", body: `{"Cid":"` + target + `","Type":"direct"}` + "\n" + `{"Cid":"` + other + `","Type":"recursive"}`, filter: kubo.PinTypeAll, limits: kubo.ListLimits{MaxItems: 1, MaxBytes: 4096}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(test.body))
			}))
			defer server.Close()
			limits := test.limits
			if limits == (kubo.ListLimits{}) {
				limits = kubo.ListLimits{MaxItems: 4, MaxBytes: 4096}
			}
			pins, err := newClient(t, server.URL, nil).PinList(t.Context(), test.filter, limits)
			if pins != nil {
				t.Fatalf("partial pins returned on error: %+v", pins)
			}
			requireProtocolError(t, err)
		})
	}
}

func TestEnumerationsRejectLateTrailerAndTruncatedBody(t *testing.T) {
	target := testBlock(t, cid.Raw, "trailer-target").Cid().String()
	t.Run("late trailer is redacted", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Trailer", "X-Stream-Error")
			_, _ = w.Write([]byte(`{"Ref":"` + target + `"}` + "\n"))
			w.Header().Set("X-Stream-Error", "late "+testToken)
		}))
		defer server.Close()
		refs, err := newClient(t, server.URL, nil).RefsLocal(t.Context(), kubo.ListLimits{MaxItems: 2, MaxBytes: 4096})
		if refs != nil {
			t.Fatalf("partial refs returned: %v", refs)
		}
		stream := requireStreamError(t, err)
		if strings.Contains(stream.Error(), testToken) || !strings.Contains(stream.Error(), "[REDACTED]") {
			t.Fatalf("trailer error was not redacted: %v", stream)
		}
	})

	t.Run("declared body is truncated", func(t *testing.T) {
		body := `{"Ref":"` + target + `"}` + "\n"
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Content-Length", "4096")
			_, _ = w.Write([]byte(body))
		}))
		defer server.Close()
		refs, err := newClient(t, server.URL, nil).RefsLocal(t.Context(), kubo.ListLimits{MaxItems: 2, MaxBytes: 8192})
		if refs != nil {
			t.Fatalf("partial refs returned: %v", refs)
		}
		requireProtocolError(t, err)
	})
}

func TestEnumerationCancellationReturnsNoPartialResult(t *testing.T) {
	target := testBlock(t, cid.Raw, "cancel-target").Cid().String()
	wroteFirst := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"Ref":"` + target + `"}` + "\n"))
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		close(wroteFirst)
		<-r.Context().Done()
	}))
	defer server.Close()
	client := newClient(t, server.URL, func(cfg *kubo.Config) {
		cfg.RequestTimeout = time.Second
	})
	ctx, cancel := context.WithCancel(t.Context())
	type result struct {
		refs []cid.Cid
		err  error
	}
	resultCh := make(chan result, 1)
	go func() {
		refs, err := client.RefsLocal(ctx, kubo.ListLimits{MaxItems: 2, MaxBytes: 4096})
		resultCh <- result{refs: refs, err: err}
	}()
	<-wroteFirst
	cancel()
	resultValue := <-resultCh
	if resultValue.refs != nil {
		t.Fatalf("partial refs returned: %v", resultValue.refs)
	}
	if !errors.Is(resultValue.err, context.Canceled) {
		t.Fatalf("error = %T %v, want context.Canceled", resultValue.err, resultValue.err)
	}
}

func TestPinMutationsRejectMalformedDuplicateAndMismatchedResponses(t *testing.T) {
	target := testBlock(t, cid.Raw, "mutation-target").Cid().String()
	other := testBlock(t, cid.Raw, "mutation-other").Cid().String()
	tests := []struct {
		name string
		body string
	}{
		{name: "malformed", body: `{"Pins":`},
		{name: "unknown field", body: `{"Pins":["` + target + `"],"Other":true}`},
		{name: "duplicate field", body: `{"Pins":["` + target + `"],"Pins":["` + target + `"]}`},
		{name: "missing pin", body: `{"Pins":[]}`},
		{name: "duplicate pin", body: `{"Pins":["` + target + `","` + target + `"]}`},
		{name: "invalid CID", body: `{"Pins":["not-a-cid"]}`},
		{name: "mismatched CID", body: `{"Pins":["` + other + `"]}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(test.body))
			}))
			defer server.Close()
			err := newClient(t, server.URL, nil).PinAdd(t.Context(), cid.MustParse(target), kubo.PinTypeDirect)
			requireProtocolError(t, err)
		})
	}
}

func TestPinRemoveReturnsTypedRedactedNotPinnedError(t *testing.T) {
	target := testBlock(t, cid.Raw, "not-pinned-target").Cid()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"Message": "CID is not pinned: " + testToken,
			"Code":    0,
			"Type":    "error",
		})
	}))
	defer server.Close()

	err := newClient(t, server.URL, nil).PinRemove(t.Context(), target, kubo.PinTypeRecursive)
	if !errors.Is(err, kubo.ErrNotPinned) {
		t.Fatalf("error = %T %v, want ErrNotPinned", err, err)
	}
	var notPinned *kubo.NotPinnedError
	if !errors.As(err, &notPinned) || notPinned.CID != target.String() {
		t.Fatalf("NotPinnedError = %#v", notPinned)
	}
	var status *kubo.StatusError
	if !errors.As(err, &status) || status.Status != http.StatusInternalServerError {
		t.Fatalf("StatusError = %#v", status)
	}
	if strings.Contains(err.Error(), testToken) || !strings.Contains(err.Error(), "[REDACTED]") {
		t.Fatalf("error was not redacted: %v", err)
	}
}

func TestTruncatedStatusCannotEstablishNotPinned(t *testing.T) {
	target := testBlock(t, cid.Raw, "truncated-not-pinned").Cid()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = io.WriteString(w, "CID is not pinned "+strings.Repeat("x", 65<<10))
	}))
	defer server.Close()

	err := newClient(t, server.URL, nil).PinRemove(t.Context(), target, kubo.PinTypeRecursive)
	var status *kubo.StatusError
	if !errors.As(err, &status) || !status.Truncated {
		t.Fatalf("error = %T %v, want truncated StatusError", err, err)
	}
	if errors.Is(err, kubo.ErrNotPinned) {
		t.Fatalf("incomplete status response established pin absence: %v", err)
	}
}
