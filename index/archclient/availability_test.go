package archclient_test

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/blobarchive/bloar/index/archclient"
)

func availabilityClient(t *testing.T, url string) *archclient.Client {
	t.Helper()
	client, err := archclient.New(archclient.Config{
		BaseURL: url, Token: "secret", MaxAttempts: 1, Backoff: time.Millisecond,
	})
	if err != nil {
		t.Fatalf("archclient.New: %v", err)
	}
	return client
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
