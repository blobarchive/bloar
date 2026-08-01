package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"

	"github.com/blobarchive/bloar/replica"
)

type observableBackend struct {
	pins map[string]replica.PinStatus
}

func TestMetricsListenerFailureIsObservable(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	stop, failures := serveMetricsListener(listener, newReplicaMetrics(nil), slog.New(slog.NewTextHandler(io.Discard, nil)))
	t.Cleanup(stop)
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-failures:
		if err == nil {
			t.Fatal("listener failure channel returned nil")
		}
	case <-time.After(time.Second):
		t.Fatal("listener failure was logged but not reported to the runtime")
	}
}

func (b *observableBackend) PutBlock(context.Context, blocks.Block) error { return nil }

func (b *observableBackend) PinStatus(_ context.Context, target cid.Cid) (replica.PinStatus, bool, error) {
	status, ok := b.pins[target.KeyString()]
	return status, ok, nil
}

func (b *observableBackend) NamedRecursivePins(context.Context, string) ([]cid.Cid, error) {
	return nil, nil
}

func (b *observableBackend) PinAddRecursive(_ context.Context, target cid.Cid, name string, _ func(replica.PinProgress)) error {
	b.pins[target.KeyString()] = replica.PinStatus{Recursive: true, Name: name}
	return nil
}

func (b *observableBackend) PinUpdateRecursive(_ context.Context, old, next cid.Cid, unpin bool) error {
	status, ok := b.pins[old.KeyString()]
	if !ok {
		return errors.New("old pin missing")
	}
	b.pins[next.KeyString()] = status
	if unpin {
		delete(b.pins, old.KeyString())
	}
	return nil
}

func (b *observableBackend) PinRemoveRecursive(_ context.Context, target cid.Cid) error {
	delete(b.pins, target.KeyString())
	return nil
}

func TestReplicaMetricsExposeDurableTransitionAndBoundedHeadState(t *testing.T) {
	db, err := pebble.Open(t.TempDir(), &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	clock := time.Unix(100, 0).UTC()
	mx := newReplicaMetrics([]string{"alpha", "beta"})
	mx.setGateway(true, true)
	mx.now = func() time.Time { return clock }
	controller, err := replica.New(replica.Config{
		KV: db, Backend: &observableBackend{pins: make(map[string]replica.PinStatus)},
		ReplicaID: "metrics-test", Now: func() time.Time { return clock }, StateChanged: mx.refresh,
	})
	if err != nil {
		t.Fatal(err)
	}
	mx.setController(controller)

	current := observableGeneration(10, 11, 12)
	if err := controller.Prepare(t.Context(), current); err != nil {
		t.Fatal(err)
	}
	if err := controller.Commit(t.Context(), current); err != nil {
		t.Fatal(err)
	}
	clock = time.Unix(200, 0).UTC()
	pending := observableGeneration(20, 21, 22)
	if err := controller.Prepare(t.Context(), pending); err != nil {
		t.Fatal(err)
	}

	clock = time.Unix(230, 0).UTC()
	mx.recordTransition("prepare", fmt.Errorf("wrapped: %w", replica.ErrOwnershipDrift))
	server := httptest.NewServer(mx.handler())
	defer server.Close()

	metricsBody := readURL(t, server.URL+"/metrics")
	for _, want := range []string{
		`bloar_replica_generation_current 1`,
		`bloar_replica_generation_pending 1`,
		`bloar_replica_generation_retained_timestamp_seconds{state="current"} 100`,
		`bloar_replica_generation_retained_timestamp_seconds{state="pending"} 200`,
		`bloar_replica_generation_head_present{head="alpha",state="current"} 1`,
		`bloar_replica_generation_synced_to{head="alpha",state="current"} 11`,
		`bloar_replica_generation_synced_to{head="beta",state="pending"} 22`,
		`bloar_replica_transition_in_progress 1`,
		`bloar_replica_transition_started_timestamp_seconds 200`,
		`bloar_replica_transition_age_seconds 30`,
		`bloar_replica_last_commit_timestamp_seconds 100`,
		`bloar_replica_last_transition_failure_timestamp_seconds{class="ownership_drift",operation="prepare"} 230`,
		`bloar_replica_state_readable 1`,
		`bloar_replica_gateway_enabled 1`,
		`bloar_replica_gateway_serving 1`,
	} {
		if !strings.Contains(metricsBody, want) {
			t.Errorf("metrics missing %q\n%s", want, metricsBody)
		}
	}

	response, err := http.Get(server.URL + "/replica/status")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status endpoint = %d", response.StatusCode)
	}
	var status statusResponse
	if err := json.NewDecoder(response.Body).Decode(&status); err != nil {
		t.Fatal(err)
	}
	if status.Current == nil || status.Pending == nil || !status.GatewayEnabled || !status.GatewayServing ||
		!status.TransitionInProgress || status.TransitionAgeSeconds != 30 {
		t.Fatalf("status = %+v", status)
	}
	if !status.Current.RetainedAt.Equal(time.Unix(100, 0)) || !status.Pending.RetainedAt.Equal(time.Unix(200, 0)) {
		t.Fatalf("retained timestamps: current=%v pending=%v", status.Current.RetainedAt, status.Pending.RetainedAt)
	}
	if status.Pending.Ownership != replica.OwnershipOwned || len(status.Pending.Heads) != 2 || status.Pending.Heads[1].SyncedTo != 22 {
		t.Fatalf("pending detail = %+v", status.Pending)
	}
}

func TestTransitionFailureClassIsClosedAndStable(t *testing.T) {
	tests := []struct {
		err  error
		want string
	}{
		{&replica.CleanupError{Err: errors.New("busy")}, "cleanup"},
		{fmt.Errorf("wrapped: %w", replica.ErrOwnershipDrift), "ownership_drift"},
		{fmt.Errorf("wrapped: %w", replica.ErrGenerationUnprotected), "unprotected"},
		{context.DeadlineExceeded, "timeout"},
		{context.Canceled, "canceled"},
		{errors.New("credential redacted"), "other"},
	}
	for _, test := range tests {
		if got := transitionFailureClass(test.err); got != test.want {
			t.Errorf("class(%v) = %q, want %q", test.err, got, test.want)
		}
	}
}

func observableGeneration(updatedAt int64, alpha, beta uint64) replica.Generation {
	return replica.Generation{
		UpdatedAt: time.Unix(updatedAt, 0),
		Heads: []replica.Head{
			{Name: "alpha", Root: blocks.NewBlock([]byte("alpha-root")).Cid(), SyncedTo: alpha},
			{Name: "beta", Root: blocks.NewBlock([]byte("beta-root")).Cid(), Manifest: blocks.NewBlock([]byte("beta-manifest")).Cid(), SyncedTo: beta},
		},
	}
}

func readURL(t *testing.T, url string) string {
	t.Helper()
	response, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("GET %s = %d", url, response.StatusCode)
	}
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}
