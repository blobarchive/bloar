package archclient_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/blobarchive/bloar/index/archclient"
	"github.com/blobarchive/bloar/server"
)

func TestGenerationStateAndPost(t *testing.T) {
	var posted server.GenerationRequest
	mux := http.NewServeMux()
	mux.HandleFunc("GET /bloar/v1/heads/tip/generation", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(server.GenerationState{V: 1, Kind: server.UnfinalizedMutable, Generation: 4})
	})
	mux.HandleFunc("POST /bloar/v1/heads/tip/generation", func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer secret" {
			t.Errorf("Authorization = %q", got)
		}
		if err := json.NewDecoder(r.Body).Decode(&posted); err != nil {
			t.Error(err)
		}
		_ = json.NewEncoder(w).Encode(server.GenerationResponse{
			Generation: 5, WindowStart: posted.WindowStart, SyncedTo: posted.SyncedTo, Root: "bafyroot",
		})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	c, err := archclient.New(archclient.Config{BaseURL: srv.URL, Token: "secret", MaxAttempts: 1, Backoff: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	state, err := c.GenerationState(context.Background(), "tip")
	if err != nil || state.Generation != 4 || state.Kind != server.UnfinalizedMutable {
		t.Fatalf("GenerationState = (%+v, %v)", state, err)
	}
	req := server.GenerationRequest{ExpectedGeneration: 4, WindowStart: 10, SyncedTo: 12, Rows: []server.GenerationRow{}}
	res, err := c.PostGeneration(context.Background(), "tip", req)
	if err != nil || res.Generation != 5 || posted.ExpectedGeneration != 4 || posted.Rows == nil {
		t.Fatalf("PostGeneration = (%+v, %v), posted %+v", res, err, posted)
	}
}

func TestGenerationConflictCarriesCASAndMissingBlobs(t *testing.T) {
	const missing = "0x0100000000000000000000000000000000000000000000000000000000000001"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(`{"code":409,"message":"missing","current_generation":7,"missing_blobs":["` + missing + `"]}`))
	}))
	defer srv.Close()
	c, err := archclient.New(archclient.Config{BaseURL: srv.URL, Token: "secret", MaxAttempts: 1, Backoff: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.PostGeneration(context.Background(), "tip", server.GenerationRequest{})
	var missingErr *archclient.MissingBlobsError
	if !errors.As(err, &missingErr) || len(missingErr.VHs) != 1 {
		t.Fatalf("error = %T %v, want MissingBlobsError", err, err)
	}
	var conflict *archclient.ConflictError
	if !errors.As(err, &conflict) || conflict.CurrentGeneration == nil || *conflict.CurrentGeneration != 7 {
		t.Fatalf("conflict = %+v", conflict)
	}
}
