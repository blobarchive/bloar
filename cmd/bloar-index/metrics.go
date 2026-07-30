package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/blobarchive/bloar/metrics"
)

// metricsShutdownGrace is how long the metrics listener gets to drain. A scrape
// is a few hundred microseconds and a probe is less; this is generous.
const metricsShutdownGrace = 2 * time.Second

// setupMetrics brings up the indexer's metrics listener of Config.MetricsListen,
// or nothing at all when it is unset.
//
// The "nothing at all" case returns a nil *metrics.Metrics, which every
// instrumented seam tolerates (see the metrics package): the indexer then costs
// a nil check per fetch and builds no registry. This mirrors bloard's
// setupMetrics down to the concrete-nil return, so that a disabled indexer costs
// nothing rather than a no-op's label lookup per call.
//
// There is no Health: an indexer is a stateless client (spec 10) with no serving
// surface and no readiness fact to establish -- nothing about it is "wrong rather
// than slow" for a probe to gate on. Handler takes a nil Health and answers
// /readyz ready whenever the process is up, which for an indexer is the same
// thing /healthz already says. The listener exists for the /metrics scrape and
// for orchestrator uniformity, not because readiness means anything here.
func setupMetrics(ctx context.Context, cfg *Config, log *slog.Logger) (*metrics.Metrics, func(*slog.Logger), error) {
	if cfg.MetricsListen == "" {
		return nil, func(*slog.Logger) {}, nil
	}

	mx := metrics.New()
	srv := &http.Server{
		Addr:              cfg.MetricsListen,
		Handler:           metrics.Handler(mx, nil),
		ReadHeaderTimeout: 10 * time.Second,
	}
	ln, err := (&net.ListenConfig{}).Listen(ctx, "tcp", cfg.MetricsListen)
	if err != nil {
		return nil, nil, fmt.Errorf("bloar-index: listening on metrics_listen %s: %w", cfg.MetricsListen, err)
	}
	go func() {
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			// Not fatal to the indexer. A process that cannot be scraped has a
			// monitoring problem; one that exits because it cannot be scraped is an
			// outage caused by monitoring.
			log.Error("metrics listener stopped", "err", err)
		}
	}()
	log.Info("metrics serving", "listen", cfg.MetricsListen)

	stop := func(log *slog.Logger) {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), metricsShutdownGrace)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			log.Error("metrics listener shutdown timed out", "err", err)
		}
	}
	return mx, stop, nil
}
