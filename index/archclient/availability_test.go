package archclient_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blobarchive/bloar/index/archclient"
	"github.com/blobarchive/bloar/schema"
)

func availabilityClient(t *testing.T, url string) *archclient.Client {
	t.Helper()
	return availabilityClientWithObserver(t, url, 1, nil, nil)
}

func availabilityClientWithObserver(
	t *testing.T,
	url string,
	maxAttempts int,
	httpClient *http.Client,
	observe func(bool),
) *archclient.Client {
	t.Helper()
	client, err := archclient.New(archclient.Config{
		BaseURL: url, Token: "secret", HTTPClient: httpClient,
		MaxAttempts: maxAttempts, Backoff: time.Millisecond,
		ObserveAvailability: observe,
	})
	if err != nil {
		t.Fatalf("archclient.New: %v", err)
	}
	return client
}

type availabilityRecorder struct {
	mu     sync.Mutex
	values []bool
}

func (r *availabilityRecorder) observe(available bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.values = append(r.values, available)
}

func (r *availabilityRecorder) snapshot() []bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]bool(nil), r.values...)
}

func TestIsUnavailable(t *testing.T) {
	t.Run("transport failure", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
		url := srv.URL
		srv.Close()
		_, err := availabilityClient(t, url).Limits(t.Context())
		if err == nil || !archclient.IsUnavailable(err) {
			t.Fatalf("transport error = %v, want unavailable", err)
		}
	})

	for _, tc := range []struct {
		name        string
		status      int
		body        string
		unavailable bool
	}{
		{name: "server failure", status: http.StatusServiceUnavailable, body: `{"code":503}`, unavailable: true},
		{name: "malformed success", status: http.StatusOK, body: `{`, unavailable: true},
		{name: "bad request", status: http.StatusBadRequest, body: `{"code":400}`, unavailable: false},
		{name: "unauthorized", status: http.StatusUnauthorized, body: `{"code":401}`, unavailable: false},
		{name: "conflict", status: http.StatusConflict, body: `{"code":409}`, unavailable: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer srv.Close()

			_, err := availabilityClient(t, srv.URL).Limits(t.Context())
			if err == nil {
				t.Fatal("fixture unexpectedly succeeded")
			}
			if got := archclient.IsUnavailable(err); got != tc.unavailable {
				t.Fatalf("IsUnavailable(%v) = %v, want %v", err, got, tc.unavailable)
			}
		})
	}
}

func TestAvailabilityObserverRecoversOnTheNextLogicalRequest(t *testing.T) {
	var available atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if !available.Load() {
			http.Error(w, `{"code":503}`, http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte(`{"max_put_blobs":64}`))
	}))
	defer srv.Close()

	var observed availabilityRecorder
	client := availabilityClientWithObserver(t, srv.URL, 1, nil, observed.observe)

	for attempt := 0; attempt < 2; attempt++ {
		if _, err := client.Limits(t.Context()); err == nil || !archclient.IsUnavailable(err) {
			t.Fatalf("unavailable request %d = %v, want unavailable", attempt+1, err)
		}
	}
	if got := observed.snapshot(); !equalAvailability(got, []bool{false, false}) {
		t.Fatalf("outage observations = %v, want [false false]", got)
	}

	available.Store(true)
	if _, err := client.Limits(t.Context()); err != nil {
		t.Fatalf("recovered request: %v", err)
	}
	if got := observed.snapshot(); !equalAvailability(got, []bool{false, false, true}) {
		t.Fatalf("recovery observations = %v, want [false false true]", got)
	}
}

func TestAvailabilityObserverReportsOnlyTheCompletedRetryOutcome(t *testing.T) {
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if attempts.Add(1) == 1 {
			http.Error(w, `{"code":503}`, http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte(`{"max_put_blobs":64}`))
	}))
	defer srv.Close()

	var observed availabilityRecorder
	client := availabilityClientWithObserver(t, srv.URL, 2, nil, observed.observe)
	if _, err := client.Limits(t.Context()); err != nil {
		t.Fatalf("request that recovered inside its retry budget: %v", err)
	}
	if got := observed.snapshot(); !equalAvailability(got, []bool{true}) {
		t.Fatalf("retry observations = %v, want one terminal true", got)
	}
}

func TestAvailabilityObserverCoversBothFinalizedReadPaths(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /bloar/v1/heads", func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, `{"code":503}`, http.StatusServiceUnavailable)
	})
	mux.HandleFunc("GET /bloar/v1/heads/chain/manifest", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"manifest":{"sources":[]},"cid":"bafymanifest"}`))
	})
	mux.HandleFunc("GET /bloar/v1/heads/all", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"name":"all","root":"bafyroot","origin_slot":1,"synced_to":2}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	var observed availabilityRecorder
	client := availabilityClientWithObserver(t, srv.URL, 1, nil, observed.observe)
	if _, err := client.Limits(t.Context()); err == nil || !archclient.IsUnavailable(err) {
		t.Fatalf("outage request = %v, want unavailable", err)
	}
	if _, err := client.Manifest(t.Context(), "chain"); err != nil {
		t.Fatalf("chain manifest after recovery: %v", err)
	}
	if _, err := client.Head(t.Context(), "all"); err != nil {
		t.Fatalf("beacon head after recovery: %v", err)
	}
	if got := observed.snapshot(); !equalAvailability(got, []bool{false, true, true}) {
		t.Fatalf("finalized-path observations = %v, want [false true true]", got)
	}
}

func TestAvailabilityObserverClassifiesTerminalOutcomes(t *testing.T) {
	for _, tc := range []struct {
		name    string
		status  int
		body    string
		want    bool
		wantErr bool
	}{
		{name: "decoded success", status: http.StatusOK, body: `{"max_put_blobs":64}`, want: true},
		{name: "unauthorized", status: http.StatusUnauthorized, body: `{"code":401}`, want: true, wantErr: true},
		{name: "conflict", status: http.StatusConflict, body: `{"code":409}`, want: true, wantErr: true},
		{name: "server failure", status: http.StatusServiceUnavailable, body: `{"code":503}`, want: false, wantErr: true},
		{name: "malformed success", status: http.StatusOK, body: `{`, want: false, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer srv.Close()

			var observed availabilityRecorder
			client := availabilityClientWithObserver(t, srv.URL, 1, nil, observed.observe)
			_, err := client.Limits(t.Context())
			if (err != nil) != tc.wantErr {
				t.Fatalf("request error = %v, wantErr %v", err, tc.wantErr)
			}
			if got := observed.snapshot(); !equalAvailability(got, []bool{tc.want}) {
				t.Fatalf("observations = %v, want [%v]", got, tc.want)
			}
		})
	}

	t.Run("transport failure", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
		url := srv.URL
		srv.Close()

		var observed availabilityRecorder
		client := availabilityClientWithObserver(t, url, 1, nil, observed.observe)
		if _, err := client.Limits(t.Context()); err == nil || !archclient.IsUnavailable(err) {
			t.Fatalf("transport request = %v, want unavailable", err)
		}
		if got := observed.snapshot(); !equalAvailability(got, []bool{false}) {
			t.Fatalf("transport observations = %v, want [false]", got)
		}
	})

	t.Run("client timeout", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
			<-r.Context().Done()
		}))
		defer srv.Close()

		var observed availabilityRecorder
		client := availabilityClientWithObserver(t, srv.URL, 1, &http.Client{Timeout: 20 * time.Millisecond}, observed.observe)
		if _, err := client.Limits(t.Context()); err == nil || !archclient.IsUnavailable(err) {
			t.Fatalf("client-timeout request = %v, want unavailable", err)
		}
		if got := observed.snapshot(); !equalAvailability(got, []bool{false}) {
			t.Fatalf("client-timeout observations = %v, want [false]", got)
		}
	})
}

func TestAvailabilityObserverIgnoresCallerCancellation(t *testing.T) {
	t.Run("before request", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		var observed availabilityRecorder
		client := availabilityClientWithObserver(t, "http://127.0.0.1:1", 1, nil, observed.observe)
		if _, err := client.Limits(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled request = %v, want context.Canceled", err)
		}
		if got := observed.snapshot(); len(got) != 0 {
			t.Fatalf("cancelled request observations = %v, want none", got)
		}
	})

	t.Run("during retry", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusServiceUnavailable)
			cancel()
		}))
		defer srv.Close()

		var observed availabilityRecorder
		client := availabilityClientWithObserver(t, srv.URL, 2, nil, observed.observe)
		if _, err := client.Limits(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled retry = %v, want context.Canceled", err)
		}
		if got := observed.snapshot(); len(got) != 0 {
			t.Fatalf("cancelled retry observations = %v, want none", got)
		}
	})
}

func TestAvailabilityObserverTreatsDecodedSemanticFailureAsReachable(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"blobs":[]}`))
	}))
	defer srv.Close()

	var observed availabilityRecorder
	client := availabilityClientWithObserver(t, srv.URL, 1, nil, observed.observe)
	if _, err := client.PutBlobs(t.Context(), [][]byte{make([]byte, schema.BlobSize)}); err == nil {
		t.Fatal("semantically incomplete success response was accepted")
	}
	if got := observed.snapshot(); !equalAvailability(got, []bool{true}) {
		t.Fatalf("semantic-failure observations = %v, want [true]", got)
	}

	if _, err := client.PutBlobs(t.Context(), nil); err != nil {
		t.Fatalf("empty no-op: %v", err)
	}
	if got := observed.snapshot(); !equalAvailability(got, []bool{true}) {
		t.Fatalf("no-op added an observation: %v", got)
	}
}

func equalAvailability(got, want []bool) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}
