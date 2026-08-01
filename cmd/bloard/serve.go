package main

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net"
	"net/http"
	"os"
	"os/signal"
	"slices"
	"syscall"
	"time"

	"github.com/ipfs/go-cid"
	"golang.org/x/net/netutil"

	"github.com/blobarchive/bloar/archive"
	"github.com/blobarchive/bloar/catalog"
	"github.com/blobarchive/bloar/core"
	"github.com/blobarchive/bloar/follow"
	"github.com/blobarchive/bloar/ingest"
	"github.com/blobarchive/bloar/metrics"
	"github.com/blobarchive/bloar/pinning"
	"github.com/blobarchive/bloar/server"
	"github.com/blobarchive/bloar/store"
)

// shutdownGrace is how long in-flight requests get to finish after a signal. A
// blobs request is a few blockstore reads and a hex encode; anything still
// running after this is not going to finish.
const shutdownGrace = 15 * time.Second

// serve runs the daemon until ctx is cancelled.
func serve(ctx context.Context, cfg *Config) error {
	log := newLogger()

	token, err := cfg.AuthToken()
	if err != nil {
		return err
	}
	// SIGUSR1 is the deliberately narrow live administrative trigger for a
	// reachability pass. It avoids binding a multi-hour operation to an HTTP
	// request context and leaves authorization to host/process permissions.
	gcSignals := make(chan os.Signal, 1)
	signal.Notify(gcSignals, syscall.SIGUSR1)
	defer signal.Stop(gcSignals)
	signingKey, err := cfg.SigningKey()
	if err != nil {
		return err
	}
	archiveID, err := cfg.ArchiveID()
	if err != nil {
		return err
	}
	specMap, err := cfg.SpecMap()
	if err != nil {
		return err
	}

	st, err := store.Open(cfg.Store.Path, store.WithPebbleLogger(pebbleLogger{log: log.With("component", "pebble")}))
	if err != nil {
		return err
	}
	defer func() {
		if err := st.Close(); err != nil {
			log.Error("closing store", "err", err)
		}
	}()
	log.Info("store opened", "path", st.Path())
	if err := ensureProfileSelection(st.KV(), cfg.profileSelection); err != nil {
		return fmt.Errorf("bloard: profile selection: %w", err)
	}
	if selected := cfg.profileSelection; selected != nil {
		log.Info("follow profile selected",
			"name", selected.Name,
			"schema", selected.Schema,
			"version", selected.Version,
			"digest", selected.Digest,
			"source", selected.Source,
			"provenance_source", selected.Provenance.Source,
			"provenance_revision", selected.Provenance.Revision)
	}

	// The metrics listener, before anything it measures. Two reasons it is first
	// rather than beside the API's listener: everything below takes a *Metrics,
	// and /readyz has to be answerable while the startup below is still running
	// -- a probe that only comes up once the node is ready could only ever say
	// "ready", which is not a readiness probe.
	mx, health, stopMetrics, err := setupMetrics(ctx, cfg, st, log)
	if err != nil {
		return err
	}
	defer stopMetrics(log)
	health.Set(metrics.GateStore, true)

	// The p2p stack, or nothing at all. Everything below it is written so that
	// "nothing at all" is the phase-7 daemon exactly: no host, no bitswap, no
	// publisher, and a document whose multiaddrs are whatever the operator put
	// in p2p.announce.
	//
	// The close order is the construction order reversed, which is what the
	// defers here spell: the publisher is using the DHT, the DHT and bitswap are
	// using the host, and all of them are using the store, so the store's Close
	// (deferred above) runs last and the publisher's first. HTTP is drained
	// before any of them, at the end of this function, because a request in
	// flight on a follower is a request that may still be fetching.
	p2pnet, err := setupP2P(ctx, cfg, st, signingKey, mx, log)
	if err != nil {
		return err
	}
	defer p2pnet.close(log)

	// One catalog, shared: it is the resolver apply_refs validates refs
	// against and the map the ingest pipeline writes, and those are the two
	// halves of the same fact (spec 6.1).
	cat := catalog.New(st.KV())

	ledger := catalog.NewLedger(st.KV())
	// The manifest tip per head (spec 10.5): persisted like head roots, read by
	// the reconciler to pin each head's chain and by the registry to publish it.
	manifests := server.NewManifestStore(st.KV())
	rec, err := pinning.NewReconciler(pinning.Config{
		Ledger: ledger,
		// The tip lookup the reconciler turns into a recursive manifest pin (spec
		// 9). Reading the durable store rather than the registry is what makes the
		// pin survive a restart and follow a followed head's adopted tip.
		ManifestTip: manifests.Get,
		Metrics:     mx,
		Logger:      log.With("component", "pinning"),
	})
	if err != nil {
		return err
	}
	// The staging pins of spec 9's window (a): ingest takes them, the head
	// registry drops them when refs land, GC expires and marks them. One
	// instance, shared by all three -- they are three views of one set of rows.
	staging, err := pinning.NewStaging(pinning.StagingConfig{
		Ledger:   ledger,
		Resolver: cat,
		TTL:      cfg.Ingest.StagingTTL,
	})
	if err != nil {
		return err
	}
	cache, err := core.NewNodeCacheMB(cfg.Store.NodeCacheMB)
	if err != nil {
		return err
	}
	roots := server.NewRootStore(st.KV())

	heads, err := openHeads(ctx, cfg, st, cat, cache, roots, manifests, signingKey, archiveID, rec, staging, mx, p2pnet, log)
	if err != nil {
		return err
	}
	health.Set(metrics.GateHeads, true)
	// After the heads: the publisher has been handed every document the
	// registry rebuilt on the way up, and this is the point at which the newest
	// of them is the one an operator would recognise.
	if err := p2pnet.start(ctx); err != nil {
		return fmt.Errorf("bloard: starting exact-pointer state: %w", err)
	}

	// One pass before serving. A restart may have missed the root swap that a
	// crash interrupted, and reconciliation is the only thing that would notice
	// (spec 9); doing it now also means the first GC has a ledger that was not
	// written by whatever happened last time.
	//
	// Written heads only, because only they are registered yet. A followed head
	// reconciles when it is adopted (spec 11.3, and follow does the notifying),
	// and it must not be reconciled here: a pass enumerates the head, a followed
	// head enumerates over the network, and a startup that failed because the
	// writer was unreachable would be a follower that cannot start without the
	// node it exists to be independent of.
	delta, err := rec.ReconcileAll(ctx)
	if err != nil {
		return fmt.Errorf("bloard: reconciling pins at startup: %w", err)
	}
	log.Info("pins reconciled", "added", delta.Added, "removed", delta.Removed)
	health.Set(metrics.GateReconcile, true)

	// The follower, if this node follows anything (spec 11.3). Closed before the
	// exchange it fetches through: its bitswap sessions are on that exchange,
	// and the defer registered here therefore runs before the one p2pnet
	// registered above.
	follower, err := setupFollow(cfg, st, cache, heads, roots, manifests, rec, staging, mx, health, p2pnet, log)
	if err != nil {
		return err
	}
	if follower != nil {
		defer func() {
			if err := follower.Close(); err != nil {
				log.Error("closing the follower", "err", err)
			}
		}()
	}
	// Before the listener: the heads this node adopted last time are on disk,
	// and there is no reason to 404 them while the first poll runs.
	resumeFollowed(ctx, follower, log)
	if follower != nil && p2pnet.pointers != nil {
		if err := p2pnet.pointers.RestoreFollowed(heads); err != nil {
			return fmt.Errorf("bloard: restoring followed exact pointers: %w", err)
		}
	}

	// GC after the follower, so a node that follows can hand its mark a per-head
	// self-heal path: a pinned block the store is missing under a followed head is
	// fetched and the run repaired, rather than the run failing closed (spec 9's
	// follower self-heal). The scope is per head, not per node: on a mixed node --
	// one that writes some heads and follows others -- a written head keeps
	// today's fail-closed semantics, so a block it lost to local corruption is a
	// hard error an operator alerts on rather than a silent refetch. A pure writer
	// builds no follower and passes a nil resolver, keeping every head
	// fail-closed.
	var gcFetch func(head string) pinning.BlockFetcher
	if follower != nil {
		gcFetch = follower.GCFetch()
	}
	gc, err := pinning.NewGC(pinning.GCConfig{
		Epochs:        st.Epochs(),
		SeparateScrub: true,
		Reconciler:    rec,
		Staging:       staging,
		Fetch:         gcFetch,
		Metrics:       mx,
		// Refresh the store-growth KV-prefix gauges at GC cadence, off the scrape
		// path. Nil when metrics are disabled.
		KVCensus: kvCensus(st, mx, log.With("component", "gc")),
		Logger:   log.With("component", "gc"),
	})
	if err != nil {
		return err
	}

	ingester, err := ingest.New(ingest.Config{
		Blocks:  st.Blocks(),
		Catalog: cat,
		// The gate and the staging pins are the two halves of spec 9's
		// exclusion around a put: the gate keeps a GC from starting inside one,
		// and the pins keep the next GC from sweeping what it accepted before
		// the refs arrive.
		Gate:              rec.Gate(),
		Staging:           staging,
		Metrics:           mx,
		VerifyConcurrency: cfg.Ingest.VerifyConcurrency,
	})
	if err != nil {
		return err
	}
	publicReadLimiter, err := cfg.Server.publicReadLimiterConfig(mx)
	if err != nil {
		return err
	}

	handler, err := server.New(server.Config{
		Heads:     heads,
		Blocks:    st.Blocks(),
		Ingester:  ingester,
		LiveHeads: cfg.serverLiveHeads(),
		Beacon: server.Beacon{
			GenesisTime:           cfg.Beacon.GenesisTime,
			SecondsPerSlot:        cfg.Beacon.SecondsPerSlot,
			GenesisValidatorsRoot: cfg.Beacon.GenesisValidatorsRoot,
			GenesisForkVersion:    cfg.Beacon.GenesisForkVersion,
			Spec:                  specMap,
		},
		AuthToken:                token,
		MaxPutBlobs:              cfg.Server.MaxPutBlobs,
		MaxQueryHashes:           cfg.Server.MaxQueryHashes,
		MaxResponseBytesInFlight: cfg.Server.MaxResponseBytesInFlight,
		ImmutableHorizonSlots:    cfg.Server.ImmutableHorizonSlots,
		// The per-request deadline refinements of the safety boundary, applied by the
		// handlers on top of the server-level bounds set on httpServer below.
		MutationBodyReadTimeout:  cfg.Server.MutationBodyTimeout,
		BlobResponseWriteTimeout: cfg.Server.WriteTimeout,
		PublicReadLimiter:        publicReadLimiter,
		Metrics:                  mx,
		Logger:                   log.With("component", "server"),
	})
	if err != nil {
		return err
	}

	httpServer := newHTTPServer(cfg, handler)

	// Both run until ctx is cancelled and return nil on it, so neither is
	// something the daemon waits on or reports: their failures are per-pass and
	// logged where they happen.
	go func() {
		if err := rec.Run(ctx); err != nil {
			log.Error("pin reconciler stopped", "err", err)
		}
	}()
	go runGCScheduler(ctx, gc, cfg.Store.GCInterval, health, log)
	go runGCTrigger(ctx, gc, gcSignals, log)
	go runScrubScheduler(ctx, gc, cfg.Store.ScrubInterval, log)
	if follower != nil {
		go func() {
			if err := follower.Run(ctx); err != nil {
				log.Error("follower stopped", "err", err)
			}
		}()
	}

	if signingKey != nil {
		pub := signingKey.Public().(ed25519.PublicKey)
		log.Info("publication signing key", "pubkey", fmt.Sprintf("%x", pub))
	}

	// The listener is bounded before it is served: a
	// LimitListener caps concurrently open connections, so a flood cannot spawn an
	// unbounded number of per-connection goroutines and buffers. Built here rather
	// than inside ListenAndServe because that method owns its own listener; Serve
	// takes ours.
	ln, err := listen(cfg)
	if err != nil {
		return err
	}

	errc := make(chan error, 1)
	go func() {
		log.Info("serving", "listen", cfg.Server.Listen, "heads", heads.Names(), "signed", signingKey != nil,
			"gc_interval", cfg.Store.GCInterval, "scrub_interval", cfg.Store.ScrubInterval,
			"p2p", p2pnet.host != nil, "ipns", p2pnet.publisher != nil,
			"max_conns", cfg.Server.MaxConns)
		if err := httpServer.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errc <- err
			return
		}
		errc <- nil
	}()

	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}

	log.Info("shutting down", "grace", shutdownGrace)
	// A fresh context: ctx is already cancelled, and Shutdown takes the
	// deadline for draining, not the one that caused it.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownGrace)
	defer cancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		// Requests were still in flight at the deadline. Say so and close the
		// store anyway: the deferred Close is the one thing that must happen,
		// since a Pebble left open is a lock file the next start trips over.
		log.Error("graceful shutdown timed out", "err", err)
	}
	if err := <-errc; err != nil {
		return err
	}
	return nil
}

// runGCScheduler raises the GC readiness gate as it starts and, if the scheduler
// ever stops with an error, withdraws it. The gate is raised HERE,
// at the runner's entry -- a start handshake -- rather than optimistically in serve
// before the goroutine is launched, so `gc` being met means the scheduler is
// actually running. RunEvery returns an error only for a non-positive interval,
// which the config boundary now rejects before serve is reached -- so the withdrawal
// is an unreachable guard, kept because a GC scheduler that silently stopped while
// the node stayed ready is exactly the failure the finding named. A clean stop on
// ctx cancellation returns nil and leaves readiness alone: that is shutdown, not
// failure.
func runGCScheduler(ctx context.Context, gc *pinning.GC, interval time.Duration, health *metrics.Health, log *slog.Logger) {
	health.Set(metrics.GateGC, true)
	if err := gc.RunEvery(ctx, interval); err != nil {
		health.Set(metrics.GateGC, false)
		log.Error("gc scheduler stopped; readiness withdrawn", "err", err)
	}
}

type gcRunner interface {
	Run(context.Context) (pinning.GCStats, error)
}

// runGCTrigger turns SIGUSR1 into an online pass through the same collector as
// the scheduler. GC's maintenance semaphore serializes it with both a scheduled
// collection and the integrity scrub. The buffered OS-signal channel coalesces
// a burst while a long pass is running instead of creating concurrent sweeps.
func runGCTrigger(ctx context.Context, gc gcRunner, triggers <-chan os.Signal, log *slog.Logger) {
	for {
		select {
		case <-ctx.Done():
			return
		case sig, ok := <-triggers:
			if !ok {
				return
			}
			log.Info("operator-triggered gc requested", "signal", sig.String())
			if _, err := gc.Run(ctx); err != nil && ctx.Err() == nil {
				log.Error("operator-triggered gc failed", "signal", sig.String(), "err", err)
			}
			// signal.Notify sends non-blockingly into the one-slot channel above.
			// Drop the one possible duplicate accumulated while this pass ran; a
			// burst means "please run", not "run once per signal".
			select {
			case _, open := <-triggers:
				if !open {
					return
				}
				log.Info("coalesced duplicate gc trigger")
			default:
			}
		}
	}
}

// runScrubScheduler offsets the first integrity pass by half its interval. With
// the default 168h scrub and 24h GC, the 84h offset keeps every weekly scrub
// halfway between daily GC ticks. GC and scrub are also serialized inside
// pinning.GC as the safety backstop; the offset is an I/O scheduling policy, not
// a correctness device.
func runScrubScheduler(ctx context.Context, gc *pinning.GC, interval time.Duration, log *slog.Logger) {
	delay := interval / 2
	if delay <= 0 {
		delay = interval
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return
	case <-timer.C:
	}
	if _, err := gc.Scrub(ctx); err != nil && ctx.Err() == nil {
		// Scrub already emitted the detailed failure; this identifies the
		// scheduler edge and makes it clear that later passes remain armed.
		log.Error("initial integrity scrub failed; scheduler continuing", "err", err)
	}
	if err := gc.ScrubEvery(ctx, interval); err != nil {
		log.Error("integrity scrub scheduler stopped", "err", err)
	}
}

// newHTTPServer builds the API server with the connection-lifetime bounds of
// the safety boundary. Every bound is nonzero by default (see config's applyDefaults),
// so the listener is safe exposed directly; a reverse proxy in front is defense
// in depth, not a precondition. ReadTimeout is the load-bearing one: it bounds
// header plus body wall-clock for EVERY request, so a slow or stalled body on an
// auth-rejected, unknown-head, or framing-rejected path -- where the handler
// returns without reading the body and net/http drains it to close -- is
// time-bounded at the server rather than able to park a connection. A valid
// mutation extends its own body read past this base (server.MutationBodyReadTimeout),
// and the blobs response caps its own write (server.BlobResponseWriteTimeout), so
// neither a legitimate large upload nor a slow reader needs a loose global bound.
func newHTTPServer(cfg *Config, handler http.Handler) *http.Server {
	return &http.Server{
		Addr: cfg.Server.Listen,
		// No gcExclusion middleware here any more: the gate is held by the
		// things that mutate (server.Heads, ingest.Ingester), not by the thing
		// that receives a POST, so every stack gets the exclusion of spec 9 and
		// not just this one.
		Handler:           handler,
		ReadHeaderTimeout: cfg.Server.ReadHeaderTimeout,
		ReadTimeout:       cfg.Server.ReadTimeout,
		IdleTimeout:       cfg.Server.IdleTimeout,
		MaxHeaderBytes:    cfg.Server.MaxHeaderBytes,
		// No server-level WriteTimeout: a fixed one would cut off a legitimate slow
		// reader of a multi-megabyte blobs response. The blobs handler sets its own
		// per-response write deadline instead (server.BlobResponseWriteTimeout),
		// which is the only body large enough to need one.
	}
}

// listen binds cfg.Server.Listen and wraps it in a LimitListener when a
// connection budget is set. Zero max_conns is unbounded, which
// the applied default never leaves it, so a directly exposed listener always has
// a cap.
func listen(cfg *Config) (net.Listener, error) {
	ln, err := net.Listen("tcp", cfg.Server.Listen)
	if err != nil {
		return nil, fmt.Errorf("bloard: listening on server.listen %s: %w", cfg.Server.Listen, err)
	}
	if cfg.Server.MaxConns > 0 {
		ln = netutil.LimitListener(ln, cfg.Server.MaxConns)
	}
	return ln, nil
}

// openHeads brings up every configured head and registers it with the
// publication registry and the pin reconciler (spec 3.1, 8, 9).
func openHeads(ctx context.Context, cfg *Config, st *store.Store, cat *catalog.Catalog, cache *core.NodeCache,
	roots *server.RootStore, manifests *server.ManifestStore, signingKey ed25519.PrivateKey, archiveID *server.ArchiveID, rec *pinning.Reconciler,
	staging *pinning.Staging, mx *metrics.Metrics, p2pnet *p2pStack, log *slog.Logger) (*server.Heads, error) {
	policies := make(map[string]pinning.Policy, len(cfg.Heads))
	headPolicies := make(map[string]server.HeadPolicy, len(cfg.Heads))
	for _, name := range sortedNames(cfg.Heads) {
		hc := cfg.Heads[name]
		policy, err := headPolicy(cfg, cfg.Heads[name])
		if err != nil {
			return nil, fmt.Errorf("bloard: head %q: %w", name, err)
		}
		policies[name] = policy
		headPolicies[name] = server.HeadPolicy{
			Kind: hc.effectiveKind(), HandoffHead: hc.HandoffHead, MaxWindowSlots: hc.MaxWindowSlots,
		}
	}
	archiveCfg := archive.Config{
		Blocks:   st.Blocks(),
		Resolver: cat,
		Cache:    cache,
	}
	// Open and register every engine with the reconciler before constructing the
	// publication registry. That lets mutable names bind an infallible runtime
	// pointer swap after all fallible name/policy validation has completed; a
	// fallible callback after the generation selector commit would be unsafe for
	// the next GC cut.
	opened := make(map[string]*archive.Head, len(cfg.Heads))
	replacements := make(map[string]func(*archive.Head))
	for _, name := range sortedNames(cfg.Heads) {
		hc := cfg.Heads[name]
		params := archive.Params{
			Name: name, Net: cfg.Net, OriginSlot: *hc.OriginSlot, SegBits: *hc.SegBits, FanoutBits: *hc.FanoutBits,
		}
		policy := policies[name]
		if _, err := follow.ReconcileWriterPromotion(ctx, follow.PromotionConfig{
			KV: st.KV(), Roots: roots, Manifests: manifests, Blocks: st.Blocks(),
			Cache: cache, Params: params, Policy: policy, Logger: log,
		}, name); err != nil {
			return nil, fmt.Errorf("bloard: reconciling head %q from its follower checkpoint before promotion: %w", name, err)
		}
		var (
			head *archive.Head
			err  error
		)
		if hc.effectiveKind() == server.UnfinalizedMutable {
			head, err = server.OpenMutableHead(ctx, archiveCfg, roots, params)
		} else {
			head, err = server.OpenHead(ctx, archiveCfg, roots, params)
		}
		if err != nil {
			return nil, err
		}
		if err := rec.Add(head, policy); err != nil {
			return nil, err
		}
		// Every writer mutation is a complete off-side engine replacement. Bind
		// the reconciler pointer now, while registration validation may still
		// fail; the runtime callback is intentionally infallible.
		replace, err := rec.BindReplacement(name)
		if err != nil {
			return nil, err
		}
		replacements[name] = replace
		opened[name] = head
	}
	heads, err := server.NewHeads(server.HeadsConfig{
		Net:               cfg.Net,
		Roots:             roots,
		Generations:       server.NewGenerationStore(st.KV()),
		Publications:      server.NewPublicationStore(st.KV()),
		Policies:          headPolicies,
		GenerationArchive: archiveCfg,
		// The manifest chain (spec 10.5): the tip store the registry publishes and
		// resumes from, and the blockstore SetManifest stores an accepted Manifest
		// in.
		Manifests: manifests,
		Blocks:    st.Blocks(),
		// Spec 9's exclusion, held across the whole of every mutation. The same
		// gate the reconciler and the GC use: two gates would exclude nothing.
		Gate: rec.Gate(),
		// The other end of the staging pins ingest takes: once a batch's refs
		// are durable, the head's own pins retain its blobs and the staging rows
		// are dropped.
		Staging: staging,
		// Rewinding a window policy can make older Segment closures live again.
		// Tell the registry the configured width so Truncate touches precisely
		// those newly recursive blocks before publishing its root during an
		// online collection epoch. Full and none need no extra closure walk.
		TruncateWindowSlots: func(name string) (uint64, bool) {
			policy, ok := policies[name]
			return policy.WindowSlots(), ok && policy.Mode == pinning.ModeWindow
		},
		Metrics: mx,
		Logger:  log.With("component", "heads"),
		// Where blocks can be fetched from: the running host's addresses, or
		// the operator's claim when there is no host.
		Multiaddrs: p2pnet.multiaddrs(cfg),
		// The archive's own POST /bloar/v1/blobs limit, advertised in the
		// publication document so an indexer can check its config against it at
		// startup. The same value server.Config.MaxPutBlobs enforces below.
		MaxPutBlobs: cfg.Server.MaxPutBlobs,
		ArchiveID:   archiveID,
		SigningKey:  signingKey,
		// Spec 9's push trigger. Notify only marks work, so this does not slow
		// the mutation that called it.
		OnRoot:       func(name string, _ cid.Cid) { rec.Notify(name) },
		Replacements: replacements,
		// Spec 8.1's, and the same deal: nil when nothing publishes to IPNS.
		OnDoc: p2pnet.onDoc(),
	})
	if err != nil {
		return nil, err
	}

	for _, name := range sortedNames(cfg.Heads) {
		hc := cfg.Heads[name]
		head := opened[name]
		if err := heads.Add(head); err != nil {
			return nil, err
		}
		syncedTo, covered := head.SyncedTo()
		log.Info("head open", "head", name, "root", head.Root(), "synced_to", syncedTo, "covered", covered,
			"origin_slot", head.Params().OriginSlot, "pin_mode", hc.Pin.Mode, "kind", hc.effectiveKind())
	}
	return heads, nil
}

// sortedNames returns the config's head names in a stable order, so that a
// startup failure blames the same head every time.
func sortedNames(heads map[string]HeadConfig) []string {
	return slices.Sorted(maps.Keys(heads))
}

// pebbleLogger routes Pebble's internal logging into the daemon's. Pebble logs
// its compactions and flushes whether or not anyone asked, and its default
// logger writes them to the process's standard logger; without this they land
// in bloard's own output as unstructured noise.
type pebbleLogger struct{ log *slog.Logger }

func (l pebbleLogger) Infof(format string, args ...any) {
	l.log.Debug(fmt.Sprintf(format, args...))
}

func (l pebbleLogger) Errorf(format string, args ...any) {
	l.log.Error(fmt.Sprintf(format, args...))
}

func (l pebbleLogger) Fatalf(format string, args ...any) {
	l.log.Error(fmt.Sprintf(format, args...))
	panic(fmt.Sprintf(format, args...))
}
