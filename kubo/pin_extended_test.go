package kubo_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ipfs/go-cid"

	"github.com/blobarchive/bloar/kubo"
)

func TestNamedPinAndUpdateWireContracts(t *testing.T) {
	old := testBlock(t, cid.Raw, "named-pin-old").Cid()
	next := testBlock(t, cid.Raw, "named-pin-next").Cid()
	partial := testBlock(t, cid.Raw, "named-pin-partial").Cid()
	var addCalls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer "+testToken {
			t.Errorf("Authorization = %q", got)
		}
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/v0/pin/add":
			want := url.Values{
				"arg":               {old.String()},
				"encoding":          {"json"},
				"recursive":         {"true"},
				"name":              {"replica"},
				"fast-provide-root": {"false"},
				"fast-provide-dag":  {"false"},
				"fast-provide-wait": {"false"},
			}
			progress := r.URL.Query().Get("progress")
			want.Set("progress", progress)
			if r.URL.Query().Encode() != want.Encode() {
				t.Errorf("pin/add query = %q, want %q", r.URL.RawQuery, want.Encode())
			}
			addCalls.Add(1)
			if progress == "true" {
				writeJSON(t, w, map[string]any{"Progress": 2, "Bytes": 32})
				writeJSON(t, w, map[string]any{"Progress": 3, "Bytes": 48})
			}
			writeJSON(t, w, map[string]any{"Pins": []string{old.String()}})
		case "/api/v0/pin/update":
			want := url.Values{
				"arg":               {old.String(), next.String()},
				"encoding":          {"json"},
				"unpin":             {"false"},
				"fast-provide-root": {"false"},
				"fast-provide-dag":  {"false"},
				"fast-provide-wait": {"false"},
			}
			if r.URL.Query().Encode() != want.Encode() {
				t.Errorf("pin/update query = %q, want %q", r.URL.RawQuery, want.Encode())
			}
			writeJSON(t, w, map[string]any{"Pins": []string{old.String(), next.String()}})
		case "/api/v0/pin/ls":
			query := r.URL.Query()
			if query.Get("arg") != "" {
				want := url.Values{
					"arg":      {old.String()},
					"encoding": {"json"},
					"type":     {"recursive"},
					"quiet":    {"false"},
					"stream":   {"true"},
					"names":    {"true"},
				}
				if query.Encode() != want.Encode() {
					t.Errorf("pin status query = %q, want %q", r.URL.RawQuery, want.Encode())
				}
				writeJSON(t, w, map[string]any{"Cid": old.String(), "Name": "replica", "Type": "recursive"})
				return
			}
			want := url.Values{
				"encoding": {"json"},
				"type":     {"recursive"},
				"quiet":    {"false"},
				"stream":   {"true"},
				"names":    {"true"},
				"name":     {"replica"},
			}
			if query.Encode() != want.Encode() {
				t.Errorf("pin name query = %q, want %q", r.URL.RawQuery, want.Encode())
			}
			writeJSON(t, w, map[string]any{"Cid": old.String(), "Name": "replica", "Type": "recursive"})
			writeJSON(t, w, map[string]any{"Cid": partial.String(), "Name": "replica-old", "Type": "recursive"})
		default:
			http.Error(w, "unexpected endpoint", http.StatusNotFound)
		}
	}))
	defer server.Close()
	client := newClient(t, server.URL, nil)

	if err := client.PinAddNamedRecursive(t.Context(), old, "replica"); err != nil {
		t.Fatalf("PinAddNamedRecursive: %v", err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	var progress []kubo.PinProgress
	snapshot, err := client.PinAddNamedRecursiveProgress(ctx, old, "replica", kubo.ListLimits{MaxItems: 4, MaxBytes: 4096}, func(p kubo.PinProgress) error {
		progress = append(progress, p)
		return nil
	})
	if err != nil {
		t.Fatalf("PinAddNamedRecursiveProgress: %v", err)
	}
	if snapshot != (kubo.PinProgress{Nodes: 3, Bytes: 48}) || len(progress) != 2 || progress[0].Nodes != 2 {
		t.Fatalf("progress = %+v, snapshot = %+v", progress, snapshot)
	}
	if err := client.PinUpdateAddBeforeRemove(ctx, old, next); err != nil {
		t.Fatalf("PinUpdateAddBeforeRemove: %v", err)
	}
	status, err := client.PinStatus(t.Context(), old, kubo.PinTypeRecursive)
	if err != nil || !status.CID.Equals(old) || status.Type != kubo.PinTypeRecursive || status.Name != "replica" {
		t.Fatalf("PinStatus = %+v, %v", status, err)
	}
	pins, err := client.PinListExactName(t.Context(), "replica", kubo.ListLimits{MaxItems: 3, MaxBytes: 4096})
	if err != nil || len(pins) != 1 || !pins[0].CID.Equals(old) || pins[0].Name != "replica" {
		t.Fatalf("PinListExactName = %+v, %v", pins, err)
	}
	if addCalls.Load() != 2 {
		t.Fatalf("pin/add calls = %d, want 2", addCalls.Load())
	}
}

func TestExtendedPinsValidateBeforeNetwork(t *testing.T) {
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { requests.Add(1) }))
	defer server.Close()
	client := newClient(t, server.URL, nil)
	target := testBlock(t, cid.Raw, "extended-pin-validation").Cid()
	other := testBlock(t, cid.Raw, "extended-pin-other").Cid()
	deadline, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	invalidUTF8 := string([]byte{0xff})

	tests := map[string]func() error{
		"named undefined CID": func() error { return client.PinAddNamedRecursive(t.Context(), cid.Undef, "replica") },
		"named empty name":    func() error { return client.PinAddNamedRecursive(t.Context(), target, "") },
		"named oversize name": func() error {
			return client.PinAddNamedRecursive(t.Context(), target, strings.Repeat("n", 256))
		},
		"named invalid UTF8": func() error { return client.PinAddNamedRecursive(t.Context(), target, invalidUTF8) },
		"progress no deadline": func() error {
			_, err := client.PinAddNamedRecursiveProgress(t.Context(), target, "replica", kubo.ListLimits{MaxItems: 2, MaxBytes: 1024}, nil)
			return err
		},
		"progress invalid limits": func() error {
			_, err := client.PinAddNamedRecursiveProgress(deadline, target, "replica", kubo.ListLimits{MaxBytes: 1024}, nil)
			return err
		},
		"update no deadline": func() error { return client.PinUpdateAddBeforeRemove(t.Context(), target, other) },
		"update same CID":    func() error { return client.PinUpdateAddBeforeRemove(deadline, target, target) },
		"update undefined":   func() error { return client.PinUpdateAddBeforeRemove(deadline, cid.Undef, other) },
		"status invalid type": func() error {
			_, err := client.PinStatus(t.Context(), target, kubo.PinTypeAll)
			return err
		},
		"name list empty": func() error {
			_, err := client.PinListExactName(t.Context(), "", kubo.ListLimits{MaxItems: 1, MaxBytes: 1})
			return err
		},
		"name list invalid limits": func() error {
			_, err := client.PinListExactName(t.Context(), "replica", kubo.ListLimits{MaxItems: 1})
			return err
		},
	}
	for name, call := range tests {
		t.Run(name, func(t *testing.T) {
			if err := call(); err == nil {
				t.Fatal("operation succeeded")
			}
		})
	}
	if got := requests.Load(); got != 0 {
		t.Fatalf("network requests = %d, want 0", got)
	}
}

func TestNamedPinProgressStrictStreamAndBounds(t *testing.T) {
	target := testBlock(t, cid.Raw, "pin-progress-target").Cid()
	other := testBlock(t, cid.Raw, "pin-progress-other").Cid()
	tests := []struct {
		name   string
		body   string
		limits kubo.ListLimits
	}{
		{name: "malformed", body: `{"Progress":`},
		{name: "unknown field", body: `{"Progress":1,"Other":true}`},
		{name: "duplicate field", body: `{"Progress":1,"Progress":2}`},
		{name: "negative progress", body: `{"Progress":-1}`},
		{name: "nodes regress", body: `{"Progress":2,"Bytes":10}` + "\n" + `{"Progress":1,"Bytes":10}`},
		{name: "bytes regress", body: `{"Progress":2,"Bytes":10}` + "\n" + `{"Progress":2,"Bytes":9}`},
		{name: "missing final", body: `{"Progress":2,"Bytes":10}`},
		{name: "null final", body: `{"Pins":null}`},
		{name: "mismatched final", body: `{"Pins":["` + other.String() + `"]}`},
		{name: "mixed final", body: `{"Pins":["` + target.String() + `"],"Progress":2}`},
		{name: "after final", body: `{"Pins":["` + target.String() + `"]}` + "\n" + `{"Progress":2}`},
		{name: "item limit", body: `{}` + "\n" + `{"Pins":["` + target.String() + `"]}`, limits: kubo.ListLimits{MaxItems: 1, MaxBytes: 4096}},
		{name: "byte limit", body: `{"Progress":1}` + strings.Repeat(" ", 64), limits: kubo.ListLimits{MaxItems: 4, MaxBytes: 16}},
		{name: "invalid UTF8", body: string([]byte{'{', '"', 'X', 0xff, '"', ':', '1', '}'})},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, test.body)
			}))
			defer server.Close()
			limits := test.limits
			if limits == (kubo.ListLimits{}) {
				limits = kubo.ListLimits{MaxItems: 4, MaxBytes: 4096}
			}
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			_, err := newClient(t, server.URL, nil).PinAddNamedRecursiveProgress(ctx, target, "replica", limits, nil)
			requireProtocolError(t, err)
		})
	}
}

func TestNamedPinProgressDrainsAfterCallbackAndChecksLateTrailer(t *testing.T) {
	target := testBlock(t, cid.Raw, "pin-progress-drain").Cid()
	drained := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Trailer", "X-Stream-Error")
		writeJSON(t, w, map[string]any{"Progress": 1, "Bytes": 2})
		writeJSON(t, w, map[string]any{"Pins": []string{target.String()}})
		w.Header().Set("X-Stream-Error", "late server error")
		close(drained)
	}))
	defer server.Close()
	callbackErr := errors.New("observer stopped")
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	snapshot, err := newClient(t, server.URL, nil).PinAddNamedRecursiveProgress(ctx, target, "replica", kubo.ListLimits{MaxItems: 3, MaxBytes: 4096}, func(kubo.PinProgress) error {
		return callbackErr
	})
	if !errors.Is(err, callbackErr) || snapshot != (kubo.PinProgress{Nodes: 1, Bytes: 2}) {
		t.Fatalf("result = %+v, %v", snapshot, err)
	}
	var late *kubo.StreamError
	if !errors.As(err, &late) {
		t.Fatalf("callback error hid late stream error: %T %v", err, err)
	}
	select {
	case <-drained:
	default:
		t.Fatal("response was not drained after callback error")
	}

	t.Run("late trailer without earlier error", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Trailer", "X-Stream-Error")
			writeJSON(t, w, map[string]any{"Pins": []string{target.String()}})
			w.Header().Set("X-Stream-Error", "late failure")
		}))
		defer server.Close()
		_, err := newClient(t, server.URL, nil).PinAddNamedRecursiveProgress(ctx, target, "replica", kubo.ListLimits{MaxItems: 2, MaxBytes: 4096}, nil)
		var stream *kubo.StreamError
		if !errors.As(err, &stream) {
			t.Fatalf("error = %T %v, want StreamError", err, err)
		}
	})
}

func TestLongPinMutationsUseCallerDeadlineNotRequestTimeout(t *testing.T) {
	old := testBlock(t, cid.Raw, "long-pin-old").Cid()
	next := testBlock(t, cid.Raw, "long-pin-next").Cid()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(25 * time.Millisecond)
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/api/v0/pin/add" {
			writeJSON(t, w, map[string]any{"Pins": []string{old.String()}})
			return
		}
		writeJSON(t, w, map[string]any{"Pins": []string{old.String(), next.String()}})
	}))
	defer server.Close()
	client := newClient(t, server.URL, func(cfg *kubo.Config) { cfg.RequestTimeout = time.Millisecond })
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if _, err := client.PinAddNamedRecursiveProgress(ctx, old, "replica", kubo.ListLimits{MaxItems: 2, MaxBytes: 4096}, nil); err != nil {
		t.Fatalf("progress pin obeyed Config.RequestTimeout: %v", err)
	}
	if err := client.PinUpdateAddBeforeRemove(ctx, old, next); err != nil {
		t.Fatalf("pin update obeyed Config.RequestTimeout: %v", err)
	}
}

func TestLongPinProgressCancellationReturnsLatestSnapshot(t *testing.T) {
	target := testBlock(t, cid.Raw, "long-pin-cancel").Cid()
	wrote := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		writeJSON(t, w, map[string]any{"Progress": 9, "Bytes": 81})
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		close(wrote)
		<-r.Context().Done()
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	client := newClient(t, server.URL, nil)
	observed := make(chan struct{})
	type result struct {
		progress kubo.PinProgress
		err      error
	}
	done := make(chan result, 1)
	go func() {
		progress, err := client.PinAddNamedRecursiveProgress(ctx, target, "replica", kubo.ListLimits{MaxItems: 3, MaxBytes: 4096}, func(kubo.PinProgress) error {
			close(observed)
			return nil
		})
		done <- result{progress: progress, err: err}
	}()
	<-wrote
	<-observed
	cancel()
	got := <-done
	if !errors.Is(got.err, context.Canceled) || got.progress != (kubo.PinProgress{Nodes: 9, Bytes: 81}) {
		t.Fatalf("result = %+v, %v", got.progress, got.err)
	}
}

func TestPinUpdateStrictResponse(t *testing.T) {
	old := testBlock(t, cid.Raw, "pin-update-old").Cid()
	next := testBlock(t, cid.Raw, "pin-update-next").Cid()
	tests := []string{
		`{"Pins":`,
		`{"Pins":["` + old.String() + `"],"Other":true}`,
		`{"Pins":["` + old.String() + `","` + next.String() + `"],"Pins":[]}`,
		`{"Pins":[]}`,
		`{"Pins":["` + next.String() + `","` + old.String() + `"]}`,
		`{"Pins":["not-a-cid","` + next.String() + `"]}`,
		string([]byte{'{', '"', 'P', 0xff, '"', ':', '[', ']', '}'}),
	}
	for i, body := range tests {
		t.Run(string(rune('a'+i)), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, body)
			}))
			defer server.Close()
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			err := newClient(t, server.URL, nil).PinUpdateAddBeforeRemove(ctx, old, next)
			requireProtocolError(t, err)
		})
	}
}

func TestPinUpdateMapsMissingSourceWithoutLosingStatus(t *testing.T) {
	old := testBlock(t, cid.Raw, "pin-update-missing-old").Cid()
	next := testBlock(t, cid.Raw, "pin-update-missing-next").Cid()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"Message": "'from' cid was not recursively pinned already",
			"Code":    0,
			"Type":    "error",
		})
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	err := newClient(t, server.URL, nil).PinUpdateAddBeforeRemove(ctx, old, next)
	if !errors.Is(err, kubo.ErrNotPinned) {
		t.Fatalf("error = %T %v, want ErrNotPinned", err, err)
	}
	var status *kubo.StatusError
	if !errors.As(err, &status) || status.Endpoint != "pin/update" {
		t.Fatalf("StatusError = %#v", status)
	}
}

func TestExactNamePinListValidatesAllPartialMatches(t *testing.T) {
	target := testBlock(t, cid.Raw, "pin-name-target").Cid()
	other := testBlock(t, cid.Raw, "pin-name-other").Cid()
	tests := []struct {
		name string
		body string
	}{
		{name: "malformed", body: `{"Cid":`},
		{name: "unknown", body: `{"Cid":"` + target.String() + `","Name":"replica","Type":"recursive","Other":1}`},
		{name: "duplicate field", body: `{"Cid":"` + target.String() + `","Name":"replica","Name":"replica","Type":"recursive"}`},
		{name: "empty name", body: `{"Cid":"` + target.String() + `","Type":"recursive"}`},
		{name: "oversize name", body: `{"Cid":"` + target.String() + `","Name":"` + strings.Repeat("n", 256) + `","Type":"recursive"}`},
		{name: "wrong type", body: `{"Cid":"` + target.String() + `","Name":"replica","Type":"direct"}`},
		{name: "server filter mismatch", body: `{"Cid":"` + target.String() + `","Name":"unrelated","Type":"recursive"}`},
		{name: "duplicate CID across partials", body: `{"Cid":"` + target.String() + `","Name":"replica","Type":"recursive"}` + "\n" + `{"Cid":"` + target.String() + `","Name":"replica-old","Type":"recursive"}`},
		{name: "invalid UTF8", body: string([]byte(`{"Cid":"`+other.String()+`","Name":"`)) + string([]byte{0xff}) + `","Type":"recursive"}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, test.body)
			}))
			defer server.Close()
			pins, err := newClient(t, server.URL, nil).PinListExactName(t.Context(), "replica", kubo.ListLimits{MaxItems: 3, MaxBytes: 4096})
			if pins != nil {
				t.Fatalf("partial pins returned: %+v", pins)
			}
			requireProtocolError(t, err)
		})
	}
}

func TestPinStatusRequiresExactlyTheRequestedCID(t *testing.T) {
	target := testBlock(t, cid.Raw, "pin-status-target").Cid()
	other := testBlock(t, cid.Raw, "pin-status-other").Cid()
	for name, body := range map[string]string{
		"empty":          "",
		"wrong CID":      `{"Cid":"` + other.String() + `","Name":"replica","Type":"recursive"}`,
		"wrong type":     `{"Cid":"` + target.String() + `","Name":"replica","Type":"direct"}`,
		"two items":      `{"Cid":"` + target.String() + `","Name":"replica","Type":"recursive"}` + "\n" + `{"Cid":"` + target.String() + `","Name":"replica","Type":"recursive"}`,
		"oversized name": `{"Cid":"` + target.String() + `","Name":"` + strings.Repeat("n", 256) + `","Type":"recursive"}`,
	} {
		t.Run(name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, body)
			}))
			defer server.Close()
			_, err := newClient(t, server.URL, nil).PinStatus(t.Context(), target, kubo.PinTypeRecursive)
			requireProtocolError(t, err)
		})
	}
}

func TestPinStatusMapsNotPinnedWithoutLosingStatus(t *testing.T) {
	target := testBlock(t, cid.Raw, "pin-status-not-pinned").Cid()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]any{"Message": "path 'x' is not pinned", "Code": 0, "Type": "error"})
	}))
	defer server.Close()
	_, err := newClient(t, server.URL, nil).PinStatus(t.Context(), target, kubo.PinTypeRecursive)
	if !errors.Is(err, kubo.ErrNotPinned) {
		t.Fatalf("error = %T %v, want ErrNotPinned", err, err)
	}
	var status *kubo.StatusError
	if !errors.As(err, &status) || status.Endpoint != "pin/ls" {
		t.Fatalf("StatusError = %#v", status)
	}
}
