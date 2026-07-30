package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/blobarchive/bloar/metrics"
	"github.com/blobarchive/bloar/store"
)

// metricsShutdownGrace is how long the metrics listener gets to drain. A scrape
// is a few hundred microseconds and a probe is less; this is generous.
const metricsShutdownGrace = 2 * time.Second

// setupMetrics brings up the metrics listener of spec 12's
// server.metrics_listen, or nothing at all when it is unset.
//
// The "nothing at all" case returns a nil *metrics.Metrics, which every
// instrumented seam tolerates (see the metrics package): the daemon then costs
// a nil check per seam and builds no registry. This is why the return is a
// concrete nil rather than a no-op implementation -- a no-op still allocates a
// label lookup per call, and disabled should cost nothing.
//
// The Health returned is nil in that case too, and *metrics.Health is nil-safe
// for the same reason: the startup path calls Set on it either way.
func setupMetrics(ctx context.Context, cfg *Config, st *store.Store, log *slog.Logger) (
	*metrics.Metrics, *metrics.Health, func(*slog.Logger), error) {
	if cfg.Server.MetricsListen == "" {
		return nil, nil, func(*slog.Logger) {}, nil
	}

	mx := metrics.New()
	// Every configured followed head is its own readiness gate, registered here --
	// before any gate is met -- so readiness cannot go green in the window between
	// written-head reconciliation and the follower registering its heads (finding
	// the safety boundary). Its follow_head_ready series is initialised to 0 in the SAME pass
	//, so a scrape between metrics setup and follower setup already
	// shows every configured head at 0 rather than absent. Each stays red until the
	// follower resumes or first adopts it.
	gates := []string{metrics.GateStore, metrics.GateHeads, metrics.GateReconcile, metrics.GateGC}
	for _, name := range sortedFollowedHeads(cfg) {
		gates = append(gates, metrics.FollowedHeadGate(name))
		mx.FollowHeadReady(name, false)
	}
	health := metrics.NewHealth(gates...)
	mx.MustRegister(pebbleSize(st))

	srv := &http.Server{
		Addr:              cfg.Server.MetricsListen,
		Handler:           metrics.Handler(mx, health),
		ReadHeaderTimeout: 10 * time.Second,
	}
	ln, err := (&net.ListenConfig{}).Listen(ctx, "tcp", cfg.Server.MetricsListen)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("bloard: listening on server.metrics_listen %s: %w", cfg.Server.MetricsListen, err)
	}
	go func() {
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			// Not fatal to the daemon. An archive that cannot be scraped is an
			// archive with a monitoring problem; an archive that exits because
			// it cannot be scraped is an outage caused by monitoring.
			log.Error("metrics listener stopped", "err", err)
		}
	}()
	log.Info("metrics serving", "listen", cfg.Server.MetricsListen)

	stop := func(log *slog.Logger) {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), metricsShutdownGrace)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			log.Error("metrics listener shutdown timed out", "err", err)
		}
	}
	return mx, health, stop, nil
}

// pebbleSize publishes the KV store's on-disk size.
//
// Only the KV. The blockstore is flatfs -- one file per block, and its size is a
// walk of a few million directory entries, which is not something to do on a
// scrape (spec 6). An operator watching disk watches the filesystem; what this
// gauge answers is the question the filesystem cannot, which is how much of the
// disk is Pebble rather than blocks.
//
// It is a GaugeFunc rather than a gauge the daemon updates, so the cost lands on
// the scraper's schedule rather than on a timer of its own.
func pebbleSize(st *store.Store) prometheus.Collector {
	return prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "bloar",
		Name:      "store_kv_bytes",
		Help: "On-disk size of the Pebble KV of spec 6 (catalog, pin ledger, roots, follower state). " +
			"The flatfs blockstore is not included: its size is a walk of every block file.",
	}, func() float64 {
		size, err := st.KV().EstimateDiskUsage(nil, []byte{0xff, 0xff, 0xff, 0xff})
		if err != nil {
			return 0
		}
		return float64(size)
	})
}

// kvCensusPrefixes pairs each countable KV prefix with the short human label its
// store_kv_entries series carries. The pin-ledger prefix 'p' is
// omitted on purpose: its rows are already published per head and purpose by
// bloar_pins. The bytes are the on-disk KV prefixes of spec 6 (docs/operations.md
// §2.2), fixed for the life of a store: 'c' catalog, 'h' head roots, 'm' manifest
// tips, 'i' IPNS sequence, 'f' follower state.
var kvCensusPrefixes = []struct {
	b     byte
	label string
}{
	{'c', "catalog"},
	{'h', "roots"},
	{'m', "manifest"},
	{'i', "ipns"},
	{'f', "follower"},
}

// kvCensus returns the GC-cadence hook that refreshes the store_kv_entries gauges
// or nil when metrics are disabled -- in which case the O(n) key
// scans are skipped entirely rather than counted and discarded. It runs inside
// the GC gate, deliberately off the scrape path (see Store.CountPrefix).
func kvCensus(st *store.Store, mx *metrics.Metrics, log *slog.Logger) func() {
	if mx == nil {
		return nil
	}
	return func() {
		for _, p := range kvCensusPrefixes {
			n, err := st.CountPrefix(p.b)
			if err != nil {
				log.Warn("gc: counting kv prefix for store metrics", "prefix", p.label, "err", err)
				continue
			}
			mx.KVEntry(p.label, n)
		}
	}
}

// The pollMetrics RoundTripper that used to live here is gone: follow.Config
// takes a *metrics.Metrics now and counts each channel's answer where it judges
// it (follow.resolve). Wrapping the HTTP client could only ever see the HTTPS
// channel's transport, which meant an IPNS-only follower reported no polls at
// all and a document that arrived 200 and then failed its signature check
// counted as a success.
