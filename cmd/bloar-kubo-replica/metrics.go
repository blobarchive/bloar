package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"slices"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	basemetrics "github.com/blobarchive/bloar/metrics"
	"github.com/blobarchive/bloar/replica"
)

const metricsShutdownGrace = 2 * time.Second

const (
	kuboRuntimeGate       = "kubo_runtime"
	runtimeAuditOperation = "runtime_audit"
)

type replicaMetrics struct {
	base   *basemetrics.Metrics
	health *basemetrics.Health

	pinProgressBlocks     prometheus.Gauge
	pinProgressBytes      prometheus.Gauge
	current               prometheus.Gauge
	pending               prometheus.Gauge
	retainedAt            *prometheus.GaugeVec
	headPresent           *prometheus.GaugeVec
	syncedTo              *prometheus.GaugeVec
	transitionActive      prometheus.Gauge
	transitionStarted     prometheus.Gauge
	transitionAge         prometheus.Gauge
	cleanup               prometheus.Gauge
	cleanupOldestRetained prometheus.Gauge
	cleanupAge            prometheus.Gauge
	lastCommit            prometheus.Gauge
	transitions           *prometheus.CounterVec
	lastFailure           *prometheus.GaugeVec
	announcements         *prometheus.CounterVec
	lastAnnouncement      prometheus.Gauge
	stateReadable         prometheus.Gauge
	gatewayEnabledMetric  prometheus.Gauge
	gatewayServingMetric  prometheus.Gauge

	mu             sync.RWMutex
	controller     *replica.Controller
	gatewayEnabled bool
	gatewayServing bool
	heads          []string
	now            func() time.Time
}

func newReplicaMetrics(heads []string) *replicaMetrics {
	base := basemetrics.New()
	gates := []string{"kubo_replica", kuboRuntimeGate}
	for _, head := range heads {
		gates = append(gates, basemetrics.FollowedHeadGate(head))
		base.FollowHeadReady(head, false)
	}
	m := &replicaMetrics{
		base:   base,
		health: basemetrics.NewHealth(gates...),
		heads:  slices.Clone(heads),
		now:    time.Now,
		pinProgressBlocks: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "bloar", Subsystem: "replica", Name: "pin_progress_blocks",
			Help: "Latest Kubo recursive-pin progress in processed blocks; zero outside an observed initial traversal or before its first update.",
		}),
		pinProgressBytes: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "bloar", Subsystem: "replica", Name: "pin_progress_bytes",
			Help: "Latest Kubo recursive-pin progress in processed bytes.",
		}),
		current: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "bloar", Subsystem: "replica", Name: "generation_current",
			Help: "Whether a durable committed generation exists (0 or 1).",
		}),
		pending: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "bloar", Subsystem: "replica", Name: "generation_pending",
			Help: "Whether a durable generation-transition intent is pending (0 or 1); it may still be recursively pinning.",
		}),
		retainedAt: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "bloar", Subsystem: "replica", Name: "generation_retained_timestamp_seconds",
			Help: "Unix time at which the durable current or pending generation entered that state; zero when absent.",
		}, []string{"state"}),
		headPresent: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "bloar", Subsystem: "replica", Name: "generation_head_present",
			Help: "Whether a configured head is present in the durable current or pending generation (0 or 1).",
		}, []string{"state", "head"}),
		syncedTo: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "bloar", Subsystem: "replica", Name: "generation_synced_to",
			Help: "Durable current or pending generation synced-to floor for a configured head; consult generation_head_present before interpreting zero.",
		}, []string{"state", "head"}),
		transitionActive: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "bloar", Subsystem: "replica", Name: "transition_in_progress",
			Help: "Whether a durable pending generation transition exists (0 or 1).",
		}),
		transitionStarted: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "bloar", Subsystem: "replica", Name: "transition_started_timestamp_seconds",
			Help: "Unix time at which the current durable pending transition began; zero when idle.",
		}),
		transitionAge: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "bloar", Subsystem: "replica", Name: "transition_age_seconds",
			Help: "Age of the current durable pending transition in seconds; zero when idle.",
		}),
		cleanup: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "bloar", Subsystem: "replica", Name: "cleanup_anchors",
			Help: "Superseded controller-owned generation anchors awaiting safe Kubo pin removal.",
		}),
		cleanupOldestRetained: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "bloar", Subsystem: "replica", Name: "cleanup_oldest_retained_timestamp_seconds",
			Help: "Former retention timestamp of the oldest cleanup-debt anchor; zero when there is no debt. This predates, and therefore only bounds, when cleanup debt began.",
		}),
		cleanupAge: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "bloar", Subsystem: "replica", Name: "cleanup_oldest_retained_age_seconds",
			Help: "Age since the oldest cleanup-debt anchor was retained; zero with no debt. This is a conservative upper bound, not the exact cleanup-debt duration.",
		}),
		lastCommit: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "bloar", Subsystem: "replica", Name: "last_commit_timestamp_seconds",
			Help: "Durable current-generation promotion time, preserved across restart; zero before one succeeds.",
		}),
		transitions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "bloar", Subsystem: "replica", Name: "transitions_total",
			Help: "Replica retention transitions by operation and bounded outcome.",
		}, []string{"operation", "outcome"}),
		lastFailure: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "bloar", Subsystem: "replica", Name: "last_transition_failure_timestamp_seconds",
			Help: "Unix time of the last transition failure by bounded operation and class; process-local and zero/absent before a failure.",
		}, []string{"operation", "class"}),
		announcements: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "bloar", Subsystem: "replica", Name: "announcements_total",
			Help: "Bounded Kubo provide-once batches by outcome.",
		}, []string{"outcome"}),
		lastAnnouncement: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "bloar", Subsystem: "replica", Name: "last_announcement_timestamp_seconds",
			Help: "Unix time of the last locally successful bounded Kubo provide-once batch; zero before one succeeds.",
		}),
		stateReadable: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "bloar", Subsystem: "replica", Name: "state_readable",
			Help: "Whether the durable replica controller state was readable on the most recent refresh (0 or 1).",
		}),
		gatewayEnabledMetric: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "bloar", Subsystem: "replica", Name: "gateway_enabled",
			Help: "Whether the optional Kubo-local read-only Bloar gateway is enabled by configuration (0 or 1).",
		}),
		gatewayServingMetric: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "bloar", Subsystem: "replica", Name: "gateway_serving",
			Help: "Whether the optional Kubo-local read-only Bloar gateway listener is currently serving (0 or 1).",
		}),
	}
	for _, state := range []string{"current", "pending"} {
		m.retainedAt.WithLabelValues(state).Set(0)
		for _, head := range m.heads {
			m.headPresent.WithLabelValues(state, head).Set(0)
			m.syncedTo.WithLabelValues(state, head).Set(0)
		}
	}
	base.MustRegister(m.pinProgressBlocks, m.pinProgressBytes, m.current, m.pending,
		m.retainedAt, m.headPresent, m.syncedTo, m.transitionActive, m.transitionStarted, m.transitionAge,
		m.cleanup, m.cleanupOldestRetained, m.cleanupAge, m.lastCommit, m.transitions, m.lastFailure,
		m.announcements, m.lastAnnouncement, m.stateReadable, m.gatewayEnabledMetric, m.gatewayServingMetric)
	return m
}

func (m *replicaMetrics) setController(controller *replica.Controller) {
	m.mu.Lock()
	m.controller = controller
	m.mu.Unlock()
	m.refresh()
}

func (m *replicaMetrics) progress(progress replica.PinProgress) {
	m.pinProgressBlocks.Set(float64(progress.Blocks))
	m.pinProgressBytes.Set(float64(progress.Bytes))
}

func (m *replicaMetrics) setGateway(enabled, serving bool) {
	m.mu.Lock()
	m.gatewayEnabled = enabled
	m.gatewayServing = serving
	m.mu.Unlock()
	m.gatewayEnabledMetric.Set(boolFloat(enabled))
	m.gatewayServingMetric.Set(boolFloat(serving))
}

func (m *replicaMetrics) refresh() {
	m.mu.RLock()
	controller := m.controller
	m.mu.RUnlock()
	if controller == nil {
		return
	}
	status, err := controller.Status()
	if err != nil {
		m.stateReadable.Set(0)
		return
	}
	m.stateReadable.Set(1)
	m.current.Set(boolFloat(status.Current.Defined()))
	if status.Pending.Defined() {
		m.pending.Set(1)
	} else {
		m.pending.Set(0)
	}
	m.setGeneration("current", status.CurrentGeneration, status.CurrentAt)
	m.setGeneration("pending", status.PendingGeneration, status.PendingAt)
	m.lastCommit.Set(unixSeconds(status.CurrentAt))
	m.transitionActive.Set(boolFloat(status.Pending.Defined()))
	m.transitionStarted.Set(unixSeconds(status.PendingAt))
	m.transitionAge.Set(ageSeconds(m.now(), status.PendingAt))
	m.cleanup.Set(float64(status.Cleanup))
	m.cleanupOldestRetained.Set(unixSeconds(status.CleanupOldestRetainedAt))
	m.cleanupAge.Set(ageSeconds(m.now(), status.CleanupOldestRetainedAt))
}

func (m *replicaMetrics) setGeneration(state string, generation *replica.Generation, retainedAt time.Time) {
	m.retainedAt.WithLabelValues(state).Set(unixSeconds(retainedAt))
	values := make(map[string]uint64, len(m.heads))
	if generation != nil {
		for _, head := range generation.Heads {
			values[head.Name] = head.SyncedTo
		}
	}
	for _, head := range m.heads {
		value, present := values[head]
		m.headPresent.WithLabelValues(state, head).Set(boolFloat(present))
		m.syncedTo.WithLabelValues(state, head).Set(float64(value))
	}
}

func (m *replicaMetrics) recordTransition(operation string, err error) {
	if !validTransitionOperation(operation) {
		return
	}
	m.transitions.WithLabelValues(operation, outcome(err)).Inc()
	if err != nil {
		m.lastFailure.WithLabelValues(operation, transitionFailureClass(err)).Set(float64(m.now().Unix()))
	}
}

func validTransitionOperation(operation string) bool {
	switch operation {
	case "prepare", "commit", "protect", "cleanup", "audit", runtimeAuditOperation:
		return true
	default:
		return false
	}
}

func transitionFailureClass(err error) string {
	var cleanup *replica.CleanupError
	switch {
	case errors.As(err, &cleanup):
		return "cleanup"
	case errors.Is(err, replica.ErrOwnershipDrift):
		return "ownership_drift"
	case errors.Is(err, replica.ErrGenerationUnprotected):
		return "unprotected"
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.Is(err, context.Canceled):
		return "canceled"
	default:
		return "other"
	}
}

func boolFloat(value bool) float64 {
	if value {
		return 1
	}
	return 0
}

func unixSeconds(value time.Time) float64 {
	if value.IsZero() {
		return 0
	}
	return float64(value.Unix())
}

func ageSeconds(now, since time.Time) float64 {
	if since.IsZero() || now.Before(since) {
		return 0
	}
	return now.Sub(since).Seconds()
}

func (m *replicaMetrics) ready(head string, ready bool) {
	m.base.FollowHeadReady(head, ready)
	m.health.Set(basemetrics.FollowedHeadGate(head), ready)
}

func (m *replicaMetrics) handler() http.Handler {
	base := basemetrics.Handler(m.base, m.health)
	mux := http.NewServeMux()
	mux.Handle("/", base)
	mux.HandleFunc("GET /replica/status", func(w http.ResponseWriter, _ *http.Request) {
		m.mu.RLock()
		controller := m.controller
		gatewayEnabled := m.gatewayEnabled
		gatewayServing := m.gatewayServing
		m.mu.RUnlock()
		if controller == nil {
			http.Error(w, "controller unavailable", http.StatusServiceUnavailable)
			return
		}
		status, err := controller.Status()
		if err != nil {
			http.Error(w, "controller state unavailable", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(replicaStatusResponse(status, m.now(), gatewayEnabled, gatewayServing))
	})
	return http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		// Transition age is derived from a durable Pending timestamp, but it must
		// advance even when Kubo emits no pin-progress events during a long walk.
		// Refresh immediately before each scrape rather than running a second
		// background ticker solely for derived gauges.
		if request.URL.Path == "/metrics" {
			m.refresh()
		}
		mux.ServeHTTP(w, request)
	})
}

type retainedStatus struct {
	Anchor     string            `json:"anchor"`
	Ownership  replica.Ownership `json:"ownership"`
	RetainedAt time.Time         `json:"retained_at"`
	ReplicaID  string            `json:"replica_id"`
	UpdatedAt  time.Time         `json:"updated_at"`
	Heads      []headStatus      `json:"heads"`
}

type headStatus struct {
	Name     string `json:"name"`
	Root     string `json:"root"`
	Manifest string `json:"manifest,omitempty"`
	SyncedTo uint64 `json:"synced_to"`
}

type statusResponse struct {
	Current                 *retainedStatus `json:"current,omitempty"`
	Pending                 *retainedStatus `json:"pending,omitempty"`
	GatewayEnabled          bool            `json:"gateway_enabled"`
	GatewayServing          bool            `json:"gateway_serving"`
	TransitionInProgress    bool            `json:"transition_in_progress"`
	TransitionStartedAt     *time.Time      `json:"transition_started_at,omitempty"`
	TransitionAgeSeconds    float64         `json:"transition_age_seconds"`
	CleanupAnchors          int             `json:"cleanup_anchors"`
	CleanupOldestRetainedAt *time.Time      `json:"cleanup_oldest_retained_at,omitempty"`
}

func replicaStatusResponse(status replica.Status, now time.Time, gatewayEnabled, gatewayServing bool) statusResponse {
	response := statusResponse{
		Current:              retainedStatusFrom(status.Current, status.CurrentOwnership, status.CurrentAt, status.CurrentGeneration),
		Pending:              retainedStatusFrom(status.Pending, status.PendingOwnership, status.PendingAt, status.PendingGeneration),
		GatewayEnabled:       gatewayEnabled,
		GatewayServing:       gatewayServing,
		TransitionInProgress: status.Pending.Defined(),
		TransitionAgeSeconds: ageSeconds(now, status.PendingAt),
		CleanupAnchors:       status.Cleanup,
	}
	if !status.PendingAt.IsZero() {
		started := status.PendingAt
		response.TransitionStartedAt = &started
	}
	if !status.CleanupOldestRetainedAt.IsZero() {
		oldest := status.CleanupOldestRetainedAt
		response.CleanupOldestRetainedAt = &oldest
	}
	return response
}

func retainedStatusFrom(anchor interface {
	Defined() bool
	String() string
}, ownership replica.Ownership, retainedAt time.Time, generation *replica.Generation) *retainedStatus {
	if !anchor.Defined() || generation == nil {
		return nil
	}
	result := &retainedStatus{
		Anchor: anchor.String(), Ownership: ownership, RetainedAt: retainedAt,
		ReplicaID: generation.ReplicaID, UpdatedAt: generation.UpdatedAt,
		Heads: make([]headStatus, 0, len(generation.Heads)),
	}
	for _, head := range generation.Heads {
		item := headStatus{Name: head.Name, Root: head.Root.String(), SyncedTo: head.SyncedTo}
		if head.Manifest.Defined() {
			item.Manifest = head.Manifest.String()
		}
		result.Heads = append(result.Heads, item)
	}
	return result
}

func serveMetrics(ctx context.Context, listen string, m *replicaMetrics, log *slog.Logger) (func(), <-chan error, error) {
	listener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", listen)
	if err != nil {
		return nil, nil, err
	}
	stop, failures := serveMetricsListener(listener, m, log)
	return stop, failures, nil
}

func serveMetricsListener(listener net.Listener, m *replicaMetrics, log *slog.Logger) (func(), <-chan error) {
	server := &http.Server{Handler: m.handler(), ReadHeaderTimeout: 10 * time.Second}
	failures := make(chan error, 1)
	go func() {
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("replica metrics listener stopped", "err", err)
			failures <- err
		}
	}()
	return func() {
		shutdown, cancel := context.WithTimeout(context.Background(), metricsShutdownGrace)
		defer cancel()
		if err := server.Shutdown(shutdown); err != nil {
			log.Error("replica metrics listener shutdown", "err", err)
		}
	}, failures
}
