package kubo_test

import (
	"context"
	"errors"
	"fmt"
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

func TestProvideOnceWireAndExactAcknowledgementSet(t *testing.T) {
	first := testBlock(t, cid.Raw, "provide-once-first").Cid()
	second := testBlock(t, cid.Raw, "provide-once-second").Cid()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/api/v0/provide/once" {
			t.Errorf("request = %s %s", r.Method, r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer "+testToken {
			t.Errorf("Authorization = %q", got)
		}
		if got := r.Header.Get("Accept"); got != "application/json" {
			t.Errorf("Accept = %q", got)
		}
		want := url.Values{
			"arg":       {first.String(), second.String()},
			"encoding":  {"json"},
			"recursive": {"false"},
		}
		if r.URL.Query().Encode() != want.Encode() {
			t.Errorf("query = %q, want %q", r.URL.RawQuery, want.Encode())
		}
		w.Header().Set("Content-Type", "application/json")
		// Order is deliberately reversed: the contract is an exact set.
		writeJSON(t, w, map[string]any{"Queued": second.String()})
		writeJSON(t, w, map[string]any{"Queued": first.String()})
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if err := newClient(t, server.URL, nil).ProvideOnce(ctx, []cid.Cid{first, second}, kubo.ListLimits{MaxItems: 2, MaxBytes: 4096}); err != nil {
		t.Fatalf("ProvideOnce: %v", err)
	}
}

func TestProvideOnceAcceptsAbsoluteMaximumBatch(t *testing.T) {
	targets := make([]cid.Cid, kubo.MaximumProvideOnceCIDs)
	for i := range targets {
		targets[i] = testBlock(t, cid.Raw, fmt.Sprintf("provide-maximum-%03d", i)).Cid()
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := len(r.URL.Query()["arg"]); got != kubo.MaximumProvideOnceCIDs {
			t.Errorf("arg count = %d, want %d", got, kubo.MaximumProvideOnceCIDs)
		}
		w.Header().Set("Content-Type", "application/json")
		for _, target := range targets {
			writeJSON(t, w, map[string]any{"Queued": target.String()})
		}
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if err := newClient(t, server.URL, nil).ProvideOnce(ctx, targets, kubo.ListLimits{
		MaxItems: kubo.MaximumProvideOnceCIDs,
		MaxBytes: 64 << 10,
	}); err != nil {
		t.Fatalf("ProvideOnce: %v", err)
	}
}

func TestProvideOnceValidatesRequestAndBoundsBeforeNetwork(t *testing.T) {
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { requests.Add(1) }))
	defer server.Close()
	client := newClient(t, server.URL, nil)
	target := testBlock(t, cid.Raw, "provide-validation").Cid()
	other := testBlock(t, cid.Raw, "provide-validation-other").Cid()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	overBatch := make([]cid.Cid, kubo.MaximumProvideOnceCIDs+1)

	tests := map[string]func() error{
		"empty": func() error {
			return client.ProvideOnce(ctx, nil, kubo.ListLimits{MaxItems: 1, MaxBytes: 1})
		},
		"over batch ceiling": func() error {
			return client.ProvideOnce(ctx, overBatch, kubo.ListLimits{MaxItems: kubo.MaximumProvideOnceCIDs, MaxBytes: 1})
		},
		"no deadline": func() error {
			return client.ProvideOnce(t.Context(), []cid.Cid{target}, kubo.ListLimits{MaxItems: 1, MaxBytes: 1024})
		},
		"undefined CID": func() error {
			return client.ProvideOnce(ctx, []cid.Cid{cid.Undef}, kubo.ListLimits{MaxItems: 1, MaxBytes: 1024})
		},
		"duplicate CID": func() error {
			return client.ProvideOnce(ctx, []cid.Cid{target, target}, kubo.ListLimits{MaxItems: 2, MaxBytes: 1024})
		},
		"missing byte limit": func() error {
			return client.ProvideOnce(ctx, []cid.Cid{target}, kubo.ListLimits{MaxItems: 1})
		},
		"missing item limit": func() error {
			return client.ProvideOnce(ctx, []cid.Cid{target}, kubo.ListLimits{MaxBytes: 1024})
		},
		"item limit below request": func() error {
			return client.ProvideOnce(ctx, []cid.Cid{target, other}, kubo.ListLimits{MaxItems: 1, MaxBytes: 1024})
		},
		"item limit over operation ceiling": func() error {
			return client.ProvideOnce(ctx, []cid.Cid{target}, kubo.ListLimits{MaxItems: kubo.MaximumProvideOnceCIDs + 1, MaxBytes: 1024})
		},
		"byte limit over client ceiling": func() error {
			return client.ProvideOnce(ctx, []cid.Cid{target}, kubo.ListLimits{MaxItems: 1, MaxBytes: kubo.DefaultMaxStreamBytes + 1})
		},
	}
	for name, call := range tests {
		t.Run(name, func(t *testing.T) {
			if err := call(); err == nil {
				t.Fatal("ProvideOnce succeeded")
			}
		})
	}
	if got := requests.Load(); got != 0 {
		t.Fatalf("network requests = %d, want 0", got)
	}
}

func TestProvideOnceRejectsMalformedDuplicateMissingAndBoundedStreams(t *testing.T) {
	target := testBlock(t, cid.Raw, "provide-stream-target").Cid()
	other := testBlock(t, cid.Raw, "provide-stream-other").Cid()
	tests := []struct {
		name    string
		targets []cid.Cid
		body    string
		limits  kubo.ListLimits
	}{
		{name: "malformed", targets: []cid.Cid{target}, body: `{"Queued":`},
		{name: "unknown field", targets: []cid.Cid{target}, body: `{"Queued":"` + target.String() + `","Other":1}`},
		{name: "duplicate field", targets: []cid.Cid{target}, body: `{"Queued":"` + target.String() + `","Queued":"` + target.String() + `"}`},
		{name: "empty CID", targets: []cid.Cid{target}, body: `{}`},
		{name: "invalid CID", targets: []cid.Cid{target}, body: `{"Queued":"not-a-cid"}`},
		{name: "oversized queued CID", targets: []cid.Cid{target}, body: `{"Queued":"` + strings.Repeat("a", 513) + `"}`},
		{name: "unrequested CID", targets: []cid.Cid{target}, body: `{"Queued":"` + other.String() + `"}`},
		{name: "duplicate acknowledgement", targets: []cid.Cid{target, other}, body: `{"Queued":"` + target.String() + `"}` + "\n" + `{"Queued":"` + target.String() + `"}`},
		{name: "missing acknowledgement", targets: []cid.Cid{target, other}, body: `{"Queued":"` + target.String() + `"}`},
		{name: "item limit", targets: []cid.Cid{target}, body: `{"Queued":"` + target.String() + `"}` + "\n" + `{"Queued":"` + target.String() + `"}`, limits: kubo.ListLimits{MaxItems: 1, MaxBytes: 4096}},
		{name: "byte limit", targets: []cid.Cid{target}, body: `{"Queued":"` + target.String() + `"}` + strings.Repeat(" ", 64), limits: kubo.ListLimits{MaxItems: 1, MaxBytes: 32}},
		{name: "invalid UTF8", targets: []cid.Cid{target}, body: string([]byte{'{', '"', 'Q', 0xff, '"', ':', '1', '}'})},
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
				limits = kubo.ListLimits{MaxItems: len(test.targets), MaxBytes: 4096}
			}
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			err := newClient(t, server.URL, nil).ProvideOnce(ctx, test.targets, limits)
			requireProtocolError(t, err)
		})
	}
}

func TestProvideOnceCapsOneJSONItemIndependentlyOfAggregateBudget(t *testing.T) {
	target := testBlock(t, cid.Raw, "provide-item-bound").Cid()
	body := `{"Queued":"` + strings.Repeat("a", 2<<20) + `"}` + "\n"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, body)
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	err := newClient(t, server.URL, nil).ProvideOnce(ctx, []cid.Cid{target}, kubo.ListLimits{
		MaxItems: 1, MaxBytes: int64(len(body) + 1),
	})
	requireProtocolError(t, err)
	if !strings.Contains(err.Error(), "item limit") {
		t.Fatalf("error = %v, want independent item limit", err)
	}
}

func TestProvideOnceChecksLateTrailerAndCallerDeadline(t *testing.T) {
	target := testBlock(t, cid.Raw, "provide-late-target").Cid()
	t.Run("late trailer", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Trailer", "X-Stream-Error")
			writeJSON(t, w, map[string]any{"Queued": target.String()})
			w.Header().Set("X-Stream-Error", "late failure")
		}))
		defer server.Close()
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		err := newClient(t, server.URL, nil).ProvideOnce(ctx, []cid.Cid{target}, kubo.ListLimits{MaxItems: 1, MaxBytes: 4096})
		var stream *kubo.StreamError
		if !errors.As(err, &stream) {
			t.Fatalf("error = %T %v, want StreamError", err, err)
		}
	})

	t.Run("Config timeout bypassed", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			time.Sleep(25 * time.Millisecond)
			w.Header().Set("Content-Type", "application/json")
			writeJSON(t, w, map[string]any{"Queued": target.String()})
		}))
		defer server.Close()
		client := newClient(t, server.URL, func(cfg *kubo.Config) { cfg.RequestTimeout = time.Millisecond })
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		if err := client.ProvideOnce(ctx, []cid.Cid{target}, kubo.ListLimits{MaxItems: 1, MaxBytes: 4096}); err != nil {
			t.Fatalf("ProvideOnce obeyed Config.RequestTimeout: %v", err)
		}
	})

	t.Run("caller deadline expires midstream", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			writeJSON(t, w, map[string]any{"Queued": target.String()})
			if flusher, ok := w.(http.Flusher); ok {
				flusher.Flush()
			}
			<-r.Context().Done()
		}))
		defer server.Close()
		ctx, cancel := context.WithTimeout(t.Context(), 25*time.Millisecond)
		defer cancel()
		err := newClient(t, server.URL, nil).ProvideOnce(ctx, []cid.Cid{target}, kubo.ListLimits{MaxItems: 1, MaxBytes: 4096})
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("error = %T %v, want context deadline exceeded", err, err)
		}
	})
}
