// Command bloar-kubo-replica follows an authenticated Bloar publication into
// an existing operator-owned Kubo node. Kubo remains the sole archive store,
// libp2p identity, Bitswap server, pin database, and garbage collector; this
// process keeps only bounded trust/checkpoint/ownership metadata in Pebble.
package main

import (
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/blobarchive/bloar/core"
	"github.com/blobarchive/bloar/follow"
	"github.com/blobarchive/bloar/kubo"
	"github.com/blobarchive/bloar/pinning"
	"github.com/blobarchive/bloar/replica"
	"github.com/blobarchive/bloar/server"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "bloar-kubo-replica: %v\n", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	flags := flag.NewFlagSet("bloar-kubo-replica", flag.ContinueOnError)
	configPath := flags.String("config", "", "path to replica YAML configuration")
	check := flags.Bool("check", false, "validate configuration and live Kubo compatibility, then exit")
	healthcheck := flags.Bool("healthcheck", false, "check the configured readiness endpoint, then exit")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("unexpected positional arguments")
	}
	if *check && *healthcheck {
		return errors.New("-check and -healthcheck are mutually exclusive")
	}
	cfg, err := loadConfig(*configPath)
	if err != nil {
		return err
	}
	if *healthcheck {
		return runReadinessHealthcheck(cfg.Metrics.Listen)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return serve(ctx, cfg, *check)
}

func serve(ctx context.Context, cfg *config, checkOnly bool) error {
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	selection := buildFollowerHeadSelection(cfg.Replica.Heads)
	client, err := kubo.New(kubo.Config{
		BaseURL:              cfg.Kubo.API,
		BearerTokenFile:      cfg.Kubo.BearerTokenFile,
		AllowUnauthenticated: cfg.Kubo.AllowUnauthenticated,
		AllowInsecureHTTP:    cfg.Kubo.AllowInsecureHTTP,
		RequestTimeout:       cfg.Kubo.RequestTimeout.value(),
		MaxStreamItems:       cfg.Kubo.MaxStreamItems,
		MaxStreamBytes:       cfg.Kubo.MaxStreamBytes,
	})
	if err != nil {
		return err
	}
	preflightCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	info, err := checkKuboCompatibility(preflightCtx, client, cfg.Kubo.ProviderPolicyCheck)
	if err == nil && cfg.Kubo.ProviderPolicyCheck == providerPolicyCheckRuntime {
		err = checkProvidePolicy(preflightCtx, client)
	}
	var selfID peer.ID
	if err == nil {
		identity, idErr := client.ID(preflightCtx)
		if idErr != nil {
			err = idErr
		} else {
			selfID = identity.ID
		}
	}
	cancel()
	if err != nil {
		return fmt.Errorf("kubo replica preflight: %w", err)
	}
	log.Info("Kubo replica preflight passed", "version", info.Version, "peer", selfID,
		"provider_policy_check", cfg.Kubo.ProviderPolicyCheck)
	if checkOnly {
		return nil
	}

	if err := os.MkdirAll(cfg.Replica.StatePath, 0o700); err != nil {
		return fmt.Errorf("creating replica state path: %w", err)
	}
	kv, err := pebble.Open(cfg.Replica.StatePath, &pebble.Options{})
	if err != nil {
		return fmt.Errorf("opening replica metadata: %w", err)
	}
	defer kv.Close()

	// The underlying adapter can satisfy Boxo's broad interface, but Bloar sees
	// only append-only, non-enumerating wrappers. It cannot invoke Kubo block
	// removal, repo enumeration, or GC through these values.
	localRaw, err := kubo.NewLocalBlockstore(client, kubo.BlockstoreConfig{
		Enumeration: kubo.ListLimits{MaxItems: 1, MaxBytes: 1024},
	})
	if err != nil {
		return err
	}
	fetchRaw, err := kubo.NewFetchingBlockstore(localRaw)
	if err != nil {
		return err
	}
	local, err := kubo.NewReplicaBlockstore(localRaw)
	if err != nil {
		return err
	}
	fetch, err := kubo.NewReplicaBlockstore(fetchRaw)
	if err != nil {
		return err
	}

	mx := newReplicaMetrics(selection.names)
	// This gate covers only what the process can observe through its runtime
	// credential. In external provider-policy mode, native-host validation of
	// Provide.Enabled/Strategy remains an explicit operator prerequisite.
	mx.health.Set(kuboRuntimeGate, true)
	backend, err := replica.NewKuboBackend(replica.KuboBackendConfig{
		Client:     client,
		PinTimeout: cfg.Kubo.PinTimeout.value(),
		PinProgressLimits: kubo.ListLimits{
			MaxItems: cfg.Kubo.PinProgressItems,
			MaxBytes: cfg.Kubo.PinProgressBytes,
		},
	})
	if err != nil {
		return err
	}
	controller, err := replica.New(replica.Config{
		KV: kv, Backend: backend, ReplicaID: cfg.Replica.ID, PinName: cfg.Replica.PinName,
		Progress: mx.progress, StateChanged: mx.refresh,
	})
	if err != nil {
		return err
	}
	mx.setController(controller)

	announcer := newAnnouncer(client, cfg.Replica.ID, cfg.Net, cfg.Kubo.AnnounceInterval.value(), mx, log)
	rendezvousCtx, cancelRendezvous := context.WithTimeout(ctx, 2*time.Minute)
	err = announcer.Initialize(rendezvousCtx, selection.names)
	cancelRendezvous()
	if err != nil {
		return fmt.Errorf("initializing Kubo rendezvous blocks: %w", err)
	}
	retention := &observedRetention{controller: controller, announce: announcer, metrics: mx}

	roots := server.NewRootStore(kv)
	manifests := server.NewManifestStore(kv)
	// Kubo owns collection, so there is no embedded reconciler from which to
	// inherit a gate. This one instance joins HTTP reader leases to the
	// follower's post-publication retirement barrier: an old generation is never
	// unpinned while a request which selected it is still materializing blocks.
	readerGate := pinning.NewGate()
	registry, err := server.NewHeads(server.HeadsConfig{
		Net: cfg.Net, Roots: roots, Manifests: manifests, Blocks: local, Gate: readerGate, Metrics: mx.base, Logger: log,
	})
	if err != nil {
		return err
	}
	cache, err := core.NewNodeCacheMB(64)
	if err != nil {
		return err
	}
	publicKey, err := parsePublicKey(cfg.Source.PublicKey)
	if err != nil {
		return err
	}
	follower, err := follow.New(follow.Config{
		Net: cfg.Net, URL: cfg.Source.URL, IPNS: cfg.Source.IPNS, DNSLink: cfg.Source.DNSLink,
		Routing: client.IPNSValueStore(), PubKey: publicKey,
		PollInterval: cfg.Source.PollInterval.value(), FetchTimeout: cfg.Source.FetchTimeout.value(), Verify: follow.VerifyCID,
		Heads: selection.policies, ExpectedKinds: selection.expectedKinds,
		ExpectedHandoffs: selection.expectedHandoffs, MaxMutableWindowSlots: selection.maxMutableWindowSlots,
		OverlayFinalizedHeads: selection.overlayFinalizedHeads,
		Local:                 local, Fetch: fetch, DocumentBlock: client.BlockFetch,
		Registry: registry, Roots: roots, Retention: retention, Gate: readerGate, KV: kv, Cache: cache,
		DialPeer: func(dialCtx context.Context, target peer.AddrInfo) error {
			return dialKuboPeer(dialCtx, client, selfID, target)
		},
		Metrics: mx.base, Ready: mx.ready, Logger: log,
	})
	if err != nil {
		return err
	}
	defer follower.Close()

	// All pure configuration and construction checks are complete before
	// Recover is allowed to remove a stale controller-owned pin.
	if err := controller.Recover(ctx); err != nil {
		var cleanup *replica.CleanupError
		if !errors.As(err, &cleanup) {
			return fmt.Errorf("recovering replica retention state: %w", err)
		}
		log.Warn("replica recovered with safe cleanup debt", "err", cleanup)
	}
	mx.refresh()
	// Recovery proves ledger integrity, but only Resume establishes which exact
	// all-head generation the follower will expose. Keep the retention gate red
	// until ProtectsAll activates that generation (or a first commit succeeds).
	mx.health.Set("kubo_replica", false)

	stopMetrics, metricsFailures, err := serveMetrics(ctx, cfg.Metrics.Listen, mx, log)
	if err != nil {
		return fmt.Errorf("listening on metrics endpoint: %w", err)
	}
	defer stopMetrics()
	// Restore every durable checkpoint before the public listener can answer.
	// This preserves last-good service across a restart even when the writer is
	// down, and prevents a brief empty/404 public view while Run performs
	// Resume. The private metrics listener is already up so a slow or failed
	// restore remains observable through its red readiness gates.
	if err := follower.Resume(ctx); err != nil {
		log.Error("resuming followed heads", "err", err)
	}
	stopGateway, gatewayFailures, err := serveReadGateway(ctx, cfg.Gateway, registry, local, mx, log)
	if err != nil {
		return fmt.Errorf("starting read-only gateway: %w", err)
	}
	defer stopGateway()

	runCtx, cancelRun := context.WithCancel(ctx)
	announceDone := make(chan struct{})
	auditDone := make(chan struct{})
	policyDone := make(chan struct{})
	go func() {
		defer close(announceDone)
		announcer.Run(runCtx)
	}()
	go func() {
		defer close(auditDone)
		runRetentionAudit(runCtx, retention, cfg.Replica.AuditInterval.value(), mx, log)
	}()
	go func() {
		defer close(policyDone)
		runKuboPolicyAudit(runCtx, cfg.Replica.AuditInterval.value(), client, selfID,
			cfg.Kubo.ProviderPolicyCheck, mx, log)
	}()
	log.Info("standalone Kubo archive replica running", "replica", cfg.Replica.ID, "heads", selection.names,
		"metrics", cfg.Metrics.Listen, "gateway", cfg.Gateway.Enabled, "gateway_listen", cfg.Gateway.Listen)
	followerDone := make(chan error, 1)
	go func() { followerDone <- follower.RunAfterResume(runCtx) }()
	followerFinished := false
	select {
	case err = <-followerDone:
		followerFinished = true
	case metricsErr := <-metricsFailures:
		err = fmt.Errorf("metrics endpoint failed after startup: %w", metricsErr)
	case gatewayErr := <-gatewayFailures:
		err = fmt.Errorf("read-only gateway failed after startup: %w", gatewayErr)
	}
	cancelRun()
	if !followerFinished {
		if followerErr := <-followerDone; followerErr != nil && !errors.Is(followerErr, context.Canceled) {
			err = errors.Join(err, followerErr)
		}
	}
	<-announceDone
	<-auditDone
	<-policyDone
	return err
}

// followerHeadSelection is the single normalization boundary between the
// versioned operator format and follow.Config. It deliberately includes only
// selected heads: an authenticated global handoff witness such as "all" stays
// publication metadata and therefore cannot enter a Kubo generation anchor.
type followerHeadSelection struct {
	names                 []string
	policies              map[string]pinning.Policy
	expectedKinds         map[string]server.HeadKind
	expectedHandoffs      map[string]string
	maxMutableWindowSlots map[string]uint64
	overlayFinalizedHeads map[string]string
}

func buildFollowerHeadSelection(heads replicaHeads) followerHeadSelection {
	names := heads.Names()
	selection := followerHeadSelection{
		names:                 names,
		policies:              make(map[string]pinning.Policy, len(names)),
		expectedKinds:         make(map[string]server.HeadKind, len(names)),
		expectedHandoffs:      make(map[string]string),
		maxMutableWindowSlots: make(map[string]uint64),
		overlayFinalizedHeads: make(map[string]string),
	}
	for _, name := range names {
		head, _ := heads.Selection(name)
		selection.policies[name] = pinning.Full()
		selection.expectedKinds[name] = head.Kind
		if head.Kind != server.UnfinalizedMutable {
			continue
		}
		selection.expectedHandoffs[name] = head.HandoffHead
		selection.maxMutableWindowSlots[name] = head.MaxWindowSlots
		if head.OverlayFinalizedHead != "" {
			selection.overlayFinalizedHeads[name] = head.OverlayFinalizedHead
		}
	}
	return selection
}

func dialKuboPeer(ctx context.Context, client *kubo.Client, self peer.ID, target peer.AddrInfo) error {
	if target.ID == self {
		return nil
	}
	addresses, err := peer.AddrInfoToP2pAddrs(&target)
	if err != nil {
		return fmt.Errorf("publication peer %s addresses: %w", target.ID, err)
	}
	if len(addresses) == 0 {
		return fmt.Errorf("publication peer %s has no dial addresses", target.ID)
	}
	var failures []error
	for index, address := range addresses {
		callCtx := ctx
		cancel := func() {}
		if deadline, ok := ctx.Deadline(); ok {
			remaining := len(addresses) - index
			budget := time.Until(deadline) / time.Duration(remaining)
			if budget > 0 {
				callCtx, cancel = context.WithTimeout(ctx, budget)
			}
		}
		_, connectErr := client.SwarmConnect(callCtx, address)
		cancel()
		if connectErr == nil {
			return nil
		}
		failures = append(failures, fmt.Errorf("%s: %w", address, connectErr))
	}
	return fmt.Errorf("connecting Kubo to publication peer %s: %w", target.ID, errors.Join(failures...))
}

type providePolicyClient interface {
	ConfigProvideEnabled(context.Context) (bool, error)
	ConfigProvideStrategy(context.Context) (string, error)
}

func checkProvidePolicy(ctx context.Context, client providePolicyClient) error {
	enabled, err := client.ConfigProvideEnabled(ctx)
	if err != nil {
		return err
	}
	if !enabled {
		return errors.New("Provide.Enabled is false; the archive would replicate but not contribute availability")
	}
	strategy, err := client.ConfigProvideStrategy(ctx)
	if err != nil {
		return err
	}
	if strategy != "roots" {
		return fmt.Errorf("Provide.Strategy is %q, want exactly %q; all/pinned archive walks are unsafe for a shared multi-terabyte replica", strategy, "roots")
	}
	return nil
}

func parsePublicKey(value string) (ed25519.PublicKey, error) {
	if value == "" {
		return nil, nil
	}
	decoded, err := hex.DecodeString(value)
	if err != nil {
		return nil, fmt.Errorf("source.pubkey: %w", err)
	}
	if len(decoded) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("source.pubkey is %d bytes, want %d", len(decoded), ed25519.PublicKeySize)
	}
	return ed25519.PublicKey(decoded), nil
}
