package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/ipfs/boxo/blockstore"
	"golang.org/x/net/netutil"

	"github.com/blobarchive/bloar/server"
)

const gatewayShutdownGrace = 15 * time.Second

// serveReadGateway starts the optional public read plane. It receives only the
// Kubo-local blockstore capability, never the network-fetching follower view:
// a public query therefore cannot initiate Bitswap work or make a missing
// retained block look healthy.
func serveReadGateway(ctx context.Context, cfg gatewayConfig, heads *server.Heads, blocks blockstore.Blockstore,
	metrics *replicaMetrics, log *slog.Logger,
) (func(), <-chan error, error) {
	if !cfg.Enabled {
		metrics.setGateway(false, false)
		return func() {}, nil, nil
	}
	metrics.setGateway(true, false)
	spec, err := cfg.specMap()
	if err != nil {
		return nil, nil, err
	}
	limiter, err := cfg.publicReadLimiterConfig(metrics)
	if err != nil {
		return nil, nil, err
	}
	handler, err := server.New(server.Config{
		ReadOnly: true,
		Heads:    heads,
		Blocks:   blocks,
		Beacon: server.Beacon{
			GenesisTime:           cfg.Beacon.GenesisTime,
			SecondsPerSlot:        cfg.Beacon.SecondsPerSlot,
			GenesisValidatorsRoot: cfg.Beacon.GenesisValidatorsRoot,
			GenesisForkVersion:    cfg.Beacon.GenesisForkVersion,
			Spec:                  spec,
		},
		LiveHeads:                cfg.serverLiveHeads(),
		MaxQueryHashes:           cfg.MaxQueryHashes,
		MaxResponseBytesInFlight: cfg.MaxResponseBytesInFlight,
		ImmutableHorizonSlots:    cfg.ImmutableHorizonSlots,
		BlobResponseWriteTimeout: cfg.WriteTimeout.value(),
		PublicReadLimiter:        limiter,
		Metrics:                  metrics.base,
		Logger:                   log.With("component", "gateway"),
	})
	if err != nil {
		return nil, nil, err
	}

	listener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", cfg.Listen)
	if err != nil {
		return nil, nil, fmt.Errorf("listening on gateway.listen %s: %w", cfg.Listen, err)
	}
	if cfg.MaxConns > 0 {
		listener = netutil.LimitListener(listener, cfg.MaxConns)
	}
	httpServer := &http.Server{
		Addr:              cfg.Listen,
		Handler:           handler,
		ReadHeaderTimeout: cfg.ReadHeaderTimeout.value(),
		ReadTimeout:       cfg.ReadTimeout.value(),
		IdleTimeout:       cfg.IdleTimeout.value(),
		MaxHeaderBytes:    cfg.MaxHeaderBytes,
	}
	failures := make(chan error, 1)
	metrics.setGateway(true, true)
	metrics.health.Set("gateway", true)
	go func() {
		log.Info("read-only Kubo gateway serving", "listen", cfg.Listen, "heads", heads.Names(), "max_conns", cfg.MaxConns)
		err := httpServer.Serve(listener)
		metrics.setGateway(true, false)
		metrics.health.Set("gateway", false)
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("read-only Kubo gateway stopped", "err", err)
			failures <- err
			return
		}
		failures <- nil
	}()

	var once sync.Once
	stop := func() {
		once.Do(func() {
			shutdown, cancel := context.WithTimeout(context.Background(), gatewayShutdownGrace)
			defer cancel()
			if err := httpServer.Shutdown(shutdown); err != nil {
				log.Error("read-only Kubo gateway shutdown", "err", err)
			}
		})
	}
	return stop, failures, nil
}
