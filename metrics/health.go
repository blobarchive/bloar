package metrics

import (
	"encoding/json"
	"net/http"
	"slices"
	"sync"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Readiness gates. A daemon is ready when every one of these fixed gates AND every
// per-followed-head gate (FollowedHeadGate) is met; see Health for what each means.
const (
	// GateStore is the store being open: the flatfs blockstore and the Pebble
	// KV of spec 6. Nothing can be served without it.
	GateStore = "store"
	// GateHeads is every configured WRITER head having been opened and registered
	// (spec 3.1); followed heads have their own per-head gates (FollowedHeadGate).
	// Until it is met the read API 404s written heads that do exist, which is a
	// wrong answer rather than a slow one -- exactly what readiness is for.
	GateHeads = "heads"
	// GateReconcile is the first pin reconciliation having completed (spec 9).
	// A node serving before it has a ledger written by whatever happened last
	// time, and the first GC would mark from it.
	GateReconcile = "reconcile"
	// GateGC is the periodic GC scheduler running. It is
	// raised at the scheduler's start (a start handshake) and cleared only if the
	// scheduler stops with a terminal error -- which RunEvery does only for a non-
	// positive interval, a state the config boundary now rejects before startup. It
	// is the belt-and-braces half of that fix: a GC scheduler that silently stopped
	// while the node stayed ready is exactly what the finding named, so if it ever
	// happens the node leaves readiness rather than accumulating disk with no sweep.
	GateGC = "gc"
)

// followedHeadGatePrefix namespaces the per-followed-head readiness gates of
// the safety boundary so they cannot collide with the fixed gates above or with each
// other, and so /readyz's waiting_on names exactly which followed head is not
// yet registered.
const followedHeadGatePrefix = "followed_head:"

// FollowedHeadGate is the readiness gate for one configured followed head
// . It is unmet until the follower has resumed the head from its
// durable checkpoint or first adopted it from a verified publication document, so
// global readiness stays red -- and the load balancer keeps routing away -- while
// any configured followed head would 404. A head with a corrupt checkpoint never
// registers and so stays red, failing closed rather than serving a wrong answer;
// and a head that quarantines (spec 11.4) goes back to red, so the balancer stops
// routing reads it can only 503. It is the readiness gate that regresses in normal
// operation -- GateGC also regresses, but only on a terminal scheduler failure.
func FollowedHeadGate(head string) string { return followedHeadGatePrefix + head }

// Health is the readiness state behind /readyz.
//
// # Liveness and readiness are different questions
//
// /healthz answers "is this process serving HTTP", and the answer is that it
// replied. It deliberately checks nothing else: a liveness probe that consults
// the store is a liveness probe that restarts the daemon when the disk is slow,
// which turns a degraded node into a crash loop and loses the in-flight work
// besides. The one thing that must not happen to bloard is being killed while
// it holds Pebble's lock and a half-applied batch.
//
// /readyz answers "should this node be in the load balancer", which is a
// question about whether its answers would be right. The gates above are the
// things that make an answer wrong rather than slow. Most are established once by
// the startup sequence and never lost; the exceptions are deliberate -- a followed
// head withdraws its gate on quarantine (spec 11.4), and the GC gate clears if the
// scheduler stops -- so a node whose answers stop being right leaves the balancer.
//
// The zero Health has no gates and is ready. Use NewHealth.
type Health struct {
	mu    sync.Mutex
	gates map[string]bool
}

// NewHealth returns a Health with every named gate unmet.
func NewHealth(gates ...string) *Health {
	h := &Health{gates: make(map[string]bool, len(gates))}
	for _, g := range gates {
		h.gates[g] = false
	}
	return h
}

// Set records whether a gate is met. An unknown gate is added: a caller that
// names one is asserting it matters.
func (h *Health) Set(gate string, ok bool) {
	if h == nil {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.gates[gate] = ok
}

// Ready reports whether every gate is met, and names the ones that are not, in
// sorted order.
func (h *Health) Ready() (bool, []string) {
	if h == nil {
		return true, nil
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	var unmet []string
	for g, ok := range h.gates {
		if !ok {
			unmet = append(unmet, g)
		}
	}
	slices.Sort(unmet)
	return len(unmet) == 0, unmet
}

// Handler is the metrics listener's mux: /metrics, /healthz and /readyz.
//
// It is a separate listener from the API on purpose (spec 12's
// server.metrics_listen). The read API is public and fronted by a CDN; these
// three are neither, and an operator binds them to a private interface. It also
// means the probes answer while the API's listener is still draining a
// shutdown, which is what tells an orchestrator to take the node out before it
// stops.
//
// Either argument may be nil: a nil Metrics serves no /metrics, and a nil
// Health is always ready.
func Handler(m *Metrics, h *Health) http.Handler {
	mux := http.NewServeMux()

	if reg := m.Registry(); reg != nil {
		mux.Handle("GET /metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{
			// A scrape that fails should say so in the response rather than in
			// the daemon's log, which is where the operator is not looking.
			ErrorHandling: promhttp.HTTPErrorOnError,
			Registry:      reg,
		}))
	}

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		writeProbe(w, http.StatusOK, map[string]any{"status": "ok"})
	})

	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		ready, unmet := h.Ready()
		if ready {
			writeProbe(w, http.StatusOK, map[string]any{"status": "ready"})
			return
		}
		// 503, not 500: the node is coming up, and the probe is being told to
		// wait rather than to report a fault.
		writeProbe(w, http.StatusServiceUnavailable, map[string]any{"status": "not ready", "waiting_on": unmet})
	})

	return mux
}

// writeProbe renders a probe response. The body is for a human reading a curl;
// the status code is what the orchestrator reads.
func writeProbe(w http.ResponseWriter, status int, body map[string]any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
