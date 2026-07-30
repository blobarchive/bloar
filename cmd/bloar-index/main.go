// Command bloar-index is the bloar indexer: the two ingest processes of spec
// 10, which read finalized blobs from an upstream and write them to an
// archive's bloar API.
//
// Usage:
//
//	bloar-index beacon -config <path>    fill the ALL head from a beacon node
//	                                     or another archive (spec 10.1)
//	bloar-index chain  -config <path>    fill a chain head from its L1 posting
//	                                     sources (spec 10.2, 10.4)
//	bloar-index unfinalized -config <path>
//	                                     replace a bounded optimistic-tip head
//	bloar-index publish-manifest -config <path>
//	                                     advance a chain head's manifest chain to
//	                                     the config's schedule, after the L1-aware
//	                                     append-only preflight (spec 10.5)
//
// # Why this is not part of bloard
//
// Spec 11.1 gives every head exactly one writer, and spec 10 makes both
// indexers stateless: their whole progress state is the archive's own
// synced_to, read back over HTTP. So an indexer is a client, not a component --
// it can restart, move host, or run twice by mistake without the archive
// noticing anything worse than duplicated work. Keeping it a separate process
// is what makes that true rather than merely intended.
//
// # A note on nitro
//
// Spec 10.2 says to import nitro packages where practical rather than
// reimplement them. It is not practical: nitro pins a fork of go-ethereum
// through a replace directive, and a replace in this module's graph would
// silently swap the go-ethereum that the archive's KZG, CID and block code is
// built against. That is why conformance/ is its own module. This binary
// therefore uses plain go-ethereum, and takes from nitro only what does not
// need importing: the SequencerBatchDelivered signature (pinned as a constant
// with a test that derives its topic hash) and the slot arithmetic, both of
// which are in index/chain.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ethereum/go-ethereum/rpc"

	"github.com/blobarchive/bloar/index/archclient"
	"github.com/blobarchive/bloar/index/beacon"
	"github.com/blobarchive/bloar/index/chain"
	"github.com/blobarchive/bloar/index/unfinalized"
	"github.com/blobarchive/bloar/index/upstream"
	"github.com/blobarchive/bloar/metrics"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		// The logger may not exist yet (a config that will not parse), so this
		// one failure path is the bare stream.
		fmt.Fprintf(os.Stderr, "bloar-index: %v\n", err)
		os.Exit(1)
	}
}

// run dispatches a subcommand.
//
// There is no default one, unlike bloard's: the two indexers write different
// heads from different sources, and guessing which an operator meant is not a
// convenience worth having.
func run(args []string) error {
	if len(args) == 0 {
		return errors.New("a subcommand is required; try `bloar-index beacon -config <path>`, `bloar-index unfinalized -config <path>`, or `bloar-index chain -config <path>`")
	}
	cmd, args := args[0], args[1:]

	// publish-manifest validates and wires as a chain command: it is the chain
	// head's operator writing that head's manifest chain (spec 10.5).
	loadCmd := cmd
	switch cmd {
	case "beacon", "unfinalized", "chain":
	case "publish-manifest":
		loadCmd = "chain"
	default:
		return fmt.Errorf("unknown subcommand %q; try `bloar-index beacon -config <path>`, "+
			"`bloar-index unfinalized -config <path>`, `bloar-index chain -config <path>`, "+
			"or `bloar-index publish-manifest -config <path>`", cmd)
	}

	fs := flag.NewFlagSet(cmd, flag.ContinueOnError)
	config := fs.String("config", "", "path to the YAML config")
	// -token-file overrides archive.token_file with a plain path. Its purpose is
	// the authenticated admin command an operator runs by hand -- publish-manifest
	// (docs/operations.md §7.5) -- against a host whose installed config carries
	// the systemd-credential form (${CREDENTIALS_DIRECTORY}/token). That form
	// resolves only inside a unit with LoadCredential=; run by hand there is no
	// credential directory, so the operator points this at the token file directly
	// (as root, since the source is 0600 root:root). It is a plain path: Token()
	// does not credential-resolve it.
	tokenFile := fs.String("token-file", "", "override archive.token_file with a plain path (for a hand-run authenticated command)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *config == "" {
		return errors.New("-config is required")
	}
	cfg, err := LoadConfig(*config, loadCmd)
	if err != nil {
		return err
	}
	if *tokenFile != "" {
		cfg.Archive.TokenFile = *tokenFile
	}

	ctx, stop := signalContext()
	defer stop()

	log := newLogger()

	mx, metricsStop, err := setupMetrics(ctx, cfg, log)
	if err != nil {
		return err
	}
	defer metricsStop(log)

	switch cmd {
	case "beacon":
		return runFinalizedIndexer(ctx, cfg, mx, log, func() error {
			return runBeacon(ctx, cfg, mx, log)
		})
	case "unfinalized":
		return runUnfinalized(ctx, cfg, mx, log)
	case "publish-manifest":
		return runPublishManifest(ctx, cfg, mx, log)
	default:
		return runFinalizedIndexer(ctx, cfg, mx, log, func() error {
			return runChain(ctx, cfg, mx, log)
		})
	}
}

// runFinalizedIndexer keeps the stateless finalized indexers alive across an
// unavailable archive. archclient has already exhausted its bounded per-request
// retry budget before an error reaches here; this is the slower process-level
// loop. Re-entering run reconstructs the indexer and re-reads every durable
// resume point from the archive, so no speculative in-memory progress survives.
//
// Only absence of an authoritative application response is retryable. Every
// 4xx, manifest mismatch, conflict, malformed configuration, and upstream
// safety failure still returns immediately. This distinction is load-bearing:
// availability must not turn a fail-closed protocol check into an unbounded
// retry that looks healthy.
func runFinalizedIndexer(
	ctx context.Context,
	cfg *Config,
	mx *metrics.Metrics,
	log *slog.Logger,
	run func() error,
) error {
	for {
		err := run()
		if err == nil || ctx.Err() != nil {
			return nil
		}
		if !archclient.IsUnavailable(err) {
			return err
		}

		mx.IndexArchiveAvailable(cfg.Archive.Head, false)
		mx.IndexRetry(cfg.Archive.Head, metrics.IndexRetryArchiveUnavailable)
		mx.IndexOutcome(cfg.Archive.Head, metrics.IndexOutcomeRetry)
		log.Warn("archive unavailable after bounded request retries; keeping finalized indexer alive",
			"archive", cfg.Archive.URL,
			"head", cfg.Archive.Head,
			"backoff", cfg.Index.PollInterval,
			"error", err)

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(cfg.Index.PollInterval):
		}
	}
}

// runUnfinalized wires the bounded optimistic tracker. The root-addressed block
// feed is the authority; the primary and optional fallback are untrusted byte
// sources, exactly as in anchored finalized indexing.
func runUnfinalized(ctx context.Context, cfg *Config, mx *metrics.Metrics, log *slog.Logger) error {
	arch, err := archiveClient(cfg, log)
	if err != nil {
		return err
	}
	if err := checkArchiveLimits(ctx, arch, cfg, true, mx, log); err != nil {
		return err
	}
	blocks, err := upstream.NewBlockClient(upstream.Config{
		BaseURL: cfg.Upstream.BlockURL,
		Logger:  log,
		Metrics: mx,
	})
	if err != nil {
		return err
	}
	primary, err := upstream.New(upstream.Config{
		BaseURL: cfg.Upstream.URL,
		Logger:  log,
		Metrics: mx,
	})
	if err != nil {
		return err
	}
	sources := []unfinalized.BlobSource{{Client: primary, Name: "primary"}}
	if cfg.Upstream.FallbackURL != "" {
		fallback, err := upstream.New(upstream.Config{
			BaseURL: cfg.Upstream.FallbackURL,
			Head:    cfg.Upstream.FallbackHead,
			Logger:  log,
			Metrics: mx,
		})
		if err != nil {
			return err
		}
		sources = append(sources, unfinalized.BlobSource{Client: fallback, Name: "fallback"})
	}
	tracker, err := unfinalized.New(unfinalized.Config{
		Headers:      blocks,
		Sources:      sources,
		Archive:      arch,
		Head:         cfg.Archive.Head,
		HandoffHead:  cfg.Unfinalized.HandoffHead,
		WindowSlots:  cfg.Unfinalized.WindowSlots,
		OverlapSlots: *cfg.Unfinalized.OverlapSlots,
		MaxPutBlobs:  cfg.Index.MaxPutBlobs,
		PollInterval: cfg.Index.PollInterval,
		Logger:       log,
		Metrics:      mx,
	})
	if err != nil {
		return err
	}
	return tracker.Run(ctx)
}

// runBeacon wires and runs the beacon indexer of spec 10.1.
func runBeacon(ctx context.Context, cfg *Config, mx *metrics.Metrics, log *slog.Logger) error {
	arch, err := archiveClient(cfg, log)
	if err != nil {
		return err
	}
	if err := checkArchiveLimits(ctx, arch, cfg, true, mx, log); err != nil {
		return err
	}

	// The primary blob source: an anchored beacon-shaped upstream (head unset),
	// or the trusted archive of mirror mode (head set).
	primary, err := upstream.New(upstream.Config{
		BaseURL: cfg.Upstream.URL,
		Head:    cfg.Upstream.Head,
		Logger:  log,
		Metrics: mx,
	})
	if err != nil {
		return err
	}
	sources := []beacon.Source{{Client: primary}}

	var blocks *upstream.BlockClient
	if cfg.Upstream.Head == "" {
		// Anchored mode: the trusted block feed (block_url, defaulted to url), plus
		// an optional fallback byte source.
		if blocks, err = upstream.NewBlockClient(upstream.Config{
			BaseURL: cfg.Upstream.BlockURL,
			Logger:  log,
			Metrics: mx,
		}); err != nil {
			return err
		}
		if cfg.Upstream.FallbackURL != "" {
			fb, err := upstream.New(upstream.Config{
				BaseURL: cfg.Upstream.FallbackURL,
				Head:    cfg.Upstream.FallbackHead,
				Logger:  log,
				Metrics: mx,
			})
			if err != nil {
				return err
			}
			sources = append(sources, beacon.Source{Client: fb})
		}
	}

	ix, err := beacon.New(newBeaconConfig(cfg, sources, blocks, arch, mx, log))
	if err != nil {
		return err
	}
	return ix.Run(ctx)
}

// newBeaconConfig assembles the beacon indexer's Config from the loaded file config
// and the wired clients. It is a seam so a test can pin that every config-derived
// field actually reaches beacon.New -- in particular ContinuityCheckpoint, which is
// easy to build clients for and then forget to pass on.
func newBeaconConfig(cfg *Config, sources []beacon.Source, blocks *upstream.BlockClient, arch *archclient.Client, mx *metrics.Metrics, log *slog.Logger) beacon.Config {
	return beacon.Config{
		Sources:              sources,
		Blocks:               blocks,
		ContinuityCheckpoint: cfg.ContinuityCheckpoint(),
		Archive:              arch,
		Head:                 cfg.Archive.Head,
		BatchSize:            cfg.Index.BatchSize,
		MaxPutBlobs:          cfg.Index.MaxPutBlobs,
		FetchConcurrency:     cfg.Index.FetchConcurrency,
		PollInterval:         cfg.Index.PollInterval,
		Metrics:              mx,
		Logger:               log,
	}
}

// runChain wires and runs the chain indexer of spec 10.2.
func runChain(ctx context.Context, cfg *Config, mx *metrics.Metrics, log *slog.Logger) error {
	ix, _, closeRPC, err := newChainIndexer(ctx, cfg, mx, log)
	if err != nil {
		return err
	}
	defer closeRPC()

	// Run performs the startup schedule check itself:
	// the configured schedule must equal the head's published manifest tip, or this
	// indexer would write a head whose data diverges from what its filter attests.
	// A divergence there is an operator's to fix -- publish the new manifest,
	// restart with a matching config -- and Run reports it as a startup error, not
	// something to loop on.
	return ix.Run(ctx)
}

// runPublishManifest advances the chain head's manifest chain to the config's
// schedule. It is the supported way to change a head's
// filter, replacing a raw manifest POST: the indexer runs the L1-aware
// append-only preflight against the current tip, binds the publish to the head
// root it validated at, and re-preflights if a refs commit races it. The operator
// then restarts the chain indexer with this same config, which now equals the new
// tip and so passes CheckSchedule.
func runPublishManifest(ctx context.Context, cfg *Config, mx *metrics.Metrics, log *slog.Logger) error {
	ix, sources, closeRPC, err := newChainIndexer(ctx, cfg, mx, log)
	if err != nil {
		return err
	}
	defer closeRPC()

	tip, err := ix.PublishManifest(ctx, sources)
	if err != nil {
		return err
	}
	log.Info("manifest published", "head", cfg.Archive.Head, "tip", tip)
	fmt.Fprintln(os.Stdout, tip)
	return nil
}

// newChainIndexer builds the chain indexer of spec 10.2 from cfg, shared by the
// chain and publish-manifest subcommands. It returns the indexer, its configured
// schedule, and a closer for the parent-chain RPC dial.
func newChainIndexer(ctx context.Context, cfg *Config, mx *metrics.Metrics, log *slog.Logger) (*chain.Indexer, []chain.Source, func(), error) {
	arch, err := archiveClient(cfg, log)
	if err != nil {
		return nil, nil, nil, err
	}
	if err := checkArchiveLimits(ctx, arch, cfg, true, mx, log); err != nil {
		return nil, nil, nil, err
	}

	// The chain indexer is single-upstream: it knows the exact vhs it wants from
	// its L1 scan and fetches only those, erroring on an absent slot rather than
	// recording anything (spec 10.2), so it needs no block feed and no fallback.
	var up *upstream.Client
	if cfg.Upstream.URL != "" {
		if up, err = upstream.New(upstream.Config{
			BaseURL: cfg.Upstream.URL,
			Head:    cfg.Upstream.Head,
			Logger:  log,
			Metrics: mx,
		}); err != nil {
			return nil, nil, nil, err
		}
	}

	sources, err := cfg.ChainSources()
	if err != nil {
		return nil, nil, nil, err
	}

	// DialContext rather than Dial: a parent chain that is not answering should
	// make a SIGTERM'd unit stop, not hang on a dial with no deadline.
	rawRPC, err := rpc.DialContext(ctx, cfg.Chain.ParentChainRPC)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("bloar-index: dialling chain.parent_chain_rpc: %w", err)
	}
	chainRPC := chain.NewRPCBatchChainClient(rawRPC)

	ix, err := chain.New(chain.Config{
		Chain:                 chainRPC,
		Archive:               arch,
		Upstream:              up,
		Head:                  cfg.Archive.Head,
		AllHead:               cfg.Chain.AllHead,
		Sources:               sources,
		GenesisTime:           cfg.Beacon.GenesisTime,
		SecondsPerSlot:        cfg.Beacon.SecondsPerSlot,
		FetchBlobs:            cfg.Chain.FetchBlobs,
		BlockRange:            cfg.Index.BlockRange,
		BlockFetchConcurrency: cfg.Index.BlockFetchConcurrency,
		RPCBatchSize:          cfg.Index.RPCBatchSize,
		MaxPutBlobs:           cfg.Index.MaxPutBlobs,
		PollInterval:          cfg.Index.PollInterval,
		Logger:                log,
		Metrics:               mx,
	})
	if err != nil {
		rawRPC.Close()
		return nil, nil, nil, err
	}
	return ix, sources, rawRPC.Close, nil
}

// checkArchiveLimits cross-checks the durable local archive.max_put_blobs
// expectation against what the live archive advertises (spec 7.2). validate()
// has already refused index.max_put_blobs above the local expectation, so a
// temporarily unavailable archive does not remove the safety bound or become a
// hard startup dependency.
//
// A reachable archive that advertises a different non-zero value is
// configuration drift and fails closed. An old archive that advertises no value
// cannot be cross-checked, so the durable local expectation remains the guard.
func checkArchiveLimits(
	ctx context.Context,
	arch *archclient.Client,
	cfg *Config,
	tolerateUnavailable bool,
	mx *metrics.Metrics,
	log *slog.Logger,
) error {
	limits, err := arch.Limits(ctx)
	if err != nil {
		if archclient.IsUnavailable(err) {
			if !tolerateUnavailable {
				return fmt.Errorf("reading the archive's limits: %w", err)
			}
			mx.IndexArchiveAvailable(cfg.Archive.Head, false)
			log.Warn("archive unavailable during limits cross-check; using the durable local expectation",
				"archive", cfg.Archive.URL,
				"head", cfg.Archive.Head,
				"archive.max_put_blobs", cfg.Archive.MaxPutBlobs,
				"index.max_put_blobs", cfg.Index.MaxPutBlobs,
				"error", err)
			return nil
		}
		// An authoritative 4xx is not an availability failure. The caller still
		// fails closed on it, while the metric distinguishes it from a cold
		// publication writer.
		mx.IndexArchiveAvailable(cfg.Archive.Head, true)
		return fmt.Errorf("reading the archive's limits: %w", err)
	}
	mx.IndexArchiveAvailable(cfg.Archive.Head, true)
	if limits.MaxPutBlobs == 0 {
		log.Warn("archive advertises no max_put_blobs; using the durable local expectation",
			"archive", cfg.Archive.URL,
			"archive.max_put_blobs", cfg.Archive.MaxPutBlobs,
			"index.max_put_blobs", cfg.Index.MaxPutBlobs)
		return nil
	}
	if limits.MaxPutBlobs != cfg.Archive.MaxPutBlobs {
		return fmt.Errorf("archive.max_put_blobs is %d locally but the archive at %s advertises %d; "+
			"refusing configuration drift before any write (spec 7.2)",
			cfg.Archive.MaxPutBlobs, cfg.Archive.URL, limits.MaxPutBlobs)
	}
	return nil
}

// archiveClient builds the bloar API client both subcommands write through.
func archiveClient(cfg *Config, log *slog.Logger) (*archclient.Client, error) {
	token, err := cfg.Token()
	if err != nil {
		return nil, err
	}
	return archclient.New(archclient.Config{
		BaseURL: cfg.Archive.URL,
		Token:   token,
		Logger:  log,
	})
}

// signalContext returns a context cancelled by SIGINT or SIGTERM, and the stop
// that unregisters the handler. Unregistering re-arms Go's default, so a second
// SIGINT during a slow shutdown kills the process, which is what an operator
// sending it means.
//
// Both indexers report a cancelled context as a clean stop: an indexer holds no
// state to flush, and whatever batch was in flight is either recorded or not.
// Spec 5.1's ingest is idempotent either way, so the next start simply re-reads
// synced_to and carries on.
func signalContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
}

// newLogger returns the indexer's logger.
func newLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
}
