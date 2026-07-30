package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blobarchive/bloar/index/archclient"
)

// archiveStub serves GET /bloar/v1/heads with the given max_put_blobs, omitting
// it when zero the way an archive predating the field does, so the startup
// cross-check can be driven over a real HTTP round trip.
func archiveStub(t *testing.T, maxPutBlobs int) string {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /bloar/v1/heads", func(w http.ResponseWriter, _ *http.Request) {
		body := map[string]any{"v": 1, "net": "testnet", "heads": []any{}}
		if maxPutBlobs != 0 {
			body["max_put_blobs"] = maxPutBlobs
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(body)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv.URL
}

// TestCheckArchiveLimits exercises the archive-limit startup gate: an indexer whose own
// max_put_blobs exceeds what the archive advertises must refuse to start, since
// starting would 400 every full put minutes into the run (spec 7.2, 10.1).
func TestCheckArchiveLimits(t *testing.T) {
	log := slog.New(slog.DiscardHandler)

	cfgFor := func(url string, archivePut, indexPut int) *Config {
		cfg := &Config{}
		cfg.Archive.URL = url
		cfg.Archive.Head = "all"
		cfg.Archive.MaxPutBlobs = archivePut
		cfg.Index.MaxPutBlobs = indexPut
		return cfg
	}
	clientFor := func(t *testing.T, url string) *archclient.Client {
		t.Helper()
		c, err := archclient.New(archclient.Config{
			BaseURL: url, Token: "x", MaxAttempts: 1, Backoff: time.Millisecond,
		})
		if err != nil {
			t.Fatalf("archclient.New: %v", err)
		}
		return c
	}

	t.Run("remote limit differing from the durable local expectation is refused", func(t *testing.T) {
		url := archiveStub(t, 64)
		err := checkArchiveLimits(t.Context(), clientFor(t, url), cfgFor(url, 128, 128), true, nil, log)
		if err == nil {
			t.Fatal("archive limit drift was accepted")
		}
		if !strings.Contains(err.Error(), "128") || !strings.Contains(err.Error(), "64") {
			t.Errorf("error names neither value: %v", err)
		}
	})

	t.Run("equal to the archive's limit starts", func(t *testing.T) {
		url := archiveStub(t, 64)
		if err := checkArchiveLimits(t.Context(), clientFor(t, url), cfgFor(url, 64, 64), true, nil, log); err != nil {
			t.Errorf("a matching max_put_blobs was refused: %v", err)
		}
	})

	t.Run("below the archive's limit starts", func(t *testing.T) {
		url := archiveStub(t, 64)
		if err := checkArchiveLimits(t.Context(), clientFor(t, url), cfgFor(url, 64, 32), true, nil, log); err != nil {
			t.Errorf("a lower max_put_blobs was refused: %v", err)
		}
	})

	t.Run("an archive advertising no limit leaves the durable local guard in force", func(t *testing.T) {
		url := archiveStub(t, 0)
		if err := checkArchiveLimits(t.Context(), clientFor(t, url), cfgFor(url, 128, 128), true, nil, log); err != nil {
			t.Errorf("a config against an archive that advertises no limit was refused: %v", err)
		}
	})

	t.Run("an unavailable archive does not become a hard startup dependency", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "cold", http.StatusServiceUnavailable)
		}))
		url := srv.URL
		srv.Close()

		if err := checkArchiveLimits(t.Context(), clientFor(t, url), cfgFor(url, 64, 64), true, nil, log); err != nil {
			t.Errorf("an unavailable archive blocked startup despite the durable local limit: %v", err)
		}
	})

	t.Run("the unfinalized tracker retains its existing fail-fast scope", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "cold", http.StatusServiceUnavailable)
		}))
		url := srv.URL
		srv.Close()

		err := checkArchiveLimits(t.Context(), clientFor(t, url), cfgFor(url, 64, 64), false, nil, log)
		if err == nil || !archclient.IsUnavailable(err) {
			t.Fatalf("unfinalized limits check = %v, want unavailable failure", err)
		}
	})

	t.Run("an authoritative 4xx still fails closed", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
		}))
		defer srv.Close()

		err := checkArchiveLimits(t.Context(), clientFor(t, srv.URL), cfgFor(srv.URL, 64, 64), true, nil, log)
		if err == nil || !strings.Contains(err.Error(), "401") {
			t.Fatalf("an authoritative 401 did not fail closed: %v", err)
		}
	})
}

func unavailableArchiveError(t *testing.T) error {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "cold", http.StatusServiceUnavailable)
	}))
	defer srv.Close()
	client, err := archclient.New(archclient.Config{
		BaseURL: srv.URL, Token: "x", MaxAttempts: 1, Backoff: time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Limits(t.Context())
	if err == nil || !archclient.IsUnavailable(err) {
		t.Fatalf("fixture error = %v, want archive unavailable", err)
	}
	return err
}

func TestRunFinalizedIndexerRetriesArchiveUnavailabilityInProcess(t *testing.T) {
	log := slog.New(slog.DiscardHandler)
	cfg := &Config{}
	cfg.Archive.URL = "http://archive"
	cfg.Archive.Head = "all"
	cfg.Index.PollInterval = time.Millisecond
	unavailable := unavailableArchiveError(t)

	t.Run("archive unavailable during startup", func(t *testing.T) {
		calls := 0
		err := runFinalizedIndexer(t.Context(), cfg, nil, log, func() error {
			calls++
			if calls == 1 {
				return unavailable
			}
			return nil
		})
		if err != nil || calls != 2 {
			t.Fatalf("run = %v after %d calls, want in-process recovery on call 2", err, calls)
		}
	})

	t.Run("archive disappears after the indexer is already running", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		firstRunning := make(chan struct{})
		releaseOutage := make(chan struct{})
		restartedInProcess := make(chan struct{})
		done := make(chan error, 1)
		calls := 0

		go func() {
			done <- runFinalizedIndexer(ctx, cfg, nil, log, func() error {
				calls++
				if calls == 1 {
					close(firstRunning)
					<-releaseOutage
					return unavailable
				}
				close(restartedInProcess)
				<-ctx.Done()
				return nil
			})
		}()

		<-firstRunning
		close(releaseOutage)
		select {
		case <-restartedInProcess:
		case <-time.After(time.Second):
			t.Fatal("the process-level loop did not restart after a runtime archive outage")
		}
		select {
		case err := <-done:
			t.Fatalf("the finalized indexer process exited during the outage: %v", err)
		default:
		}
		cancel()
		if err := <-done; err != nil {
			t.Fatalf("cancelled retry loop = %v, want clean stop", err)
		}
	})

	t.Run("authoritative safety failure remains fatal", func(t *testing.T) {
		safetyFailure := errors.New("manifest mismatch")
		calls := 0
		err := runFinalizedIndexer(t.Context(), cfg, nil, log, func() error {
			calls++
			return safetyFailure
		})
		if !errors.Is(err, safetyFailure) || calls != 1 {
			t.Fatalf("unclassified failure = %v after %d calls, want one fatal return", err, calls)
		}
	})
}

func TestRunUnfinalizedDoesNotMakeArchiveAvailabilityAStartupDependency(t *testing.T) {
	var limitsRequests atomic.Int32
	headAttempt := make(chan struct{}, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/bloar/v1/heads":
			limitsRequests.Add(1)
		case "/bloar/v1/heads/all":
			select {
			case headAttempt <- struct{}{}:
			default:
			}
		}
		http.Error(w, "cold", http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	overlap := uint64(1)
	cfg := &Config{
		Archive: ArchiveConfig{
			URL: srv.URL, TokenFile: tokenFile(t, "secret\n"), Head: "tip", MaxPutBlobs: 64,
		},
		Upstream: UpstreamConfig{URL: srv.URL, BlockURL: srv.URL},
		Index:    IndexConfig{MaxPutBlobs: 64, PollInterval: time.Millisecond},
		Unfinalized: UnfinalizedConfig{
			HandoffHead: "all", WindowSlots: 8, OverlapSlots: &overlap,
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- runUnfinalized(ctx, cfg, nil, slog.New(slog.DiscardHandler))
	}()

	select {
	case <-headAttempt:
		// Reaching the per-head request proves the limits request exhausted its
		// bounded retries without terminating startup. Stop during the next
		// bounded archive request; cancellation is a clean indexer shutdown.
		cancel()
	case <-time.After(10 * time.Second):
		cancel()
		t.Fatal("unfinalized tracker never started after the archive limits request exhausted retries")
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("cancelled cold-archive tracker = %v, want clean stop", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cold-archive tracker did not stop after cancellation")
	}
	if got := limitsRequests.Load(); got != 5 {
		t.Fatalf("limits attempts = %d, want the client's complete bounded budget of 5", got)
	}
}
