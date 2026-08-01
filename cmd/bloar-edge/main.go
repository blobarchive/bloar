// Command bloar-edge is the public, non-authoritative Bloar P2P process. It
// opens the archive's flatfs blocks read-only, serves them over Bitswap, joins
// the public Amino DHT/rendezvous namespaces, and accepts only already-signed
// publication bundles from a private bloard over an AF_UNIX socket.
package main

import (
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/boxo/ipns"
	"golang.org/x/sys/unix"
	"gopkg.in/yaml.v3"

	"github.com/blobarchive/bloar/metrics"
	"github.com/blobarchive/bloar/p2p"
	publicationedge "github.com/blobarchive/bloar/p2p/edge"
	"github.com/blobarchive/bloar/p2p/pointerhint"
	"github.com/blobarchive/bloar/server"
	"github.com/blobarchive/bloar/store"
)

const (
	defaultMetricsListen = "127.0.0.1:9555"
	restoreMinBackoff    = time.Second
	restoreMaxBackoff    = time.Minute
)

type config struct {
	Net     string        `yaml:"net"`
	Store   storeConfig   `yaml:"store"`
	Control controlConfig `yaml:"control"`
	P2P     p2pConfig     `yaml:"p2p"`
	Metrics metricsConfig `yaml:"metrics"`
}

type storeConfig struct {
	BlocksPath string `yaml:"blocks_path"`
}

type controlConfig struct {
	Socket            string `yaml:"socket"`
	StateFile         string `yaml:"state_file"`
	IPNSName          string `yaml:"ipns_name"`
	DocumentPublicKey string `yaml:"document_public_key"`
	ArchiveID         string `yaml:"archive_id"`
	MaxDocumentBytes  int    `yaml:"max_document_bytes"`
	// TransactionTimeout bounds serialized Provide-then-PutValue work. The
	// writer declares the same value on each request; drift is refused.
	TransactionTimeout   time.Duration               `yaml:"transaction_timeout"`
	AllowAdditionalPeers bool                        `yaml:"allow_additional_peers"`
	AllowedHeads         map[string]headPolicyConfig `yaml:"allowed_heads"`
}

type headPolicyConfig struct {
	Kind        server.HeadKind `yaml:"kind"`
	HandoffHead string          `yaml:"handoff_head"`
	Required    *bool           `yaml:"required"`
}

type p2pConfig struct {
	Listen          []string                    `yaml:"listen"`
	Peers           []string                    `yaml:"peers"`
	Announce        []string                    `yaml:"announce"`
	IdentityKeyFile string                      `yaml:"identity_key_file"`
	NATPortMap      *bool                       `yaml:"nat_port_map"`
	Connection      p2p.ConnectionManagerConfig `yaml:"connection_manager"`
	Resources       p2p.ResourceManagerConfig   `yaml:"resource_manager"`
	Bitswap         bitswapConfig               `yaml:"bitswap"`
	RendezvousHeads []string                    `yaml:"rendezvous_heads"`
}

type bitswapConfig struct {
	MaxQueuedWantsPerPeer      int64 `yaml:"max_queued_wants_per_peer"`
	MaxOutstandingBytesPerPeer int64 `yaml:"max_outstanding_bytes_per_peer"`
	SendWorkers                int64 `yaml:"send_workers"`
	EngineTaskWorkers          int64 `yaml:"engine_task_workers"`
	BlockstoreWorkers          int64 `yaml:"blockstore_workers"`
	MaxCIDBytes                int64 `yaml:"max_cid_bytes"`
}

type metricsConfig struct {
	Listen string `yaml:"listen"`
}

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintf(os.Stderr, "bloar-edge: %v\n", err)
		os.Exit(1)
	}
}

func run(args []string, _ io.Writer) error {
	if len(args) > 0 && args[0] == "healthcheck" {
		return runHealthcheck(args[1:])
	}
	fs := flag.NewFlagSet("bloar-edge", flag.ContinueOnError)
	path := fs.String("config", "", "path to the edge YAML config")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *path == "" {
		return errors.New("-config is required")
	}
	cfg, err := loadConfig(*path)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return serve(ctx, cfg)
}

func runHealthcheck(args []string) error {
	fs := flag.NewFlagSet("bloar-edge healthcheck", flag.ContinueOnError)
	url := fs.String("url", "", "edge readiness URL (overrides -config)")
	configPath := fs.String("config", "", "edge YAML config whose metrics.listen selects the readiness URL")
	timeout := fs.Duration("timeout", 3*time.Second, "complete healthcheck timeout")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *timeout <= 0 {
		return fmt.Errorf("-timeout is %s, must be positive", *timeout)
	}
	target := *url
	if target == "" {
		if *configPath == "" {
			target = "http://127.0.0.1:9555/readyz"
		} else {
			cfg, err := loadConfig(*configPath)
			if err != nil {
				return err
			}
			target, err = readinessURL(cfg.Metrics.Listen)
			if err != nil {
				return err
			}
		}
	}
	client := &http.Client{Timeout: *timeout}
	req, err := http.NewRequest(http.MethodGet, target, nil)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	if resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("readiness returned HTTP %d", resp.StatusCode)
	}
	return nil
}

func readinessURL(listen string) (string, error) {
	host, port, err := net.SplitHostPort(listen)
	if err != nil {
		return "", fmt.Errorf("metrics.listen %q: %w", listen, err)
	}
	switch host {
	case "", "0.0.0.0":
		host = "127.0.0.1"
	case "::":
		host = "::1"
	}
	return "http://" + net.JoinHostPort(host, port) + "/readyz", nil
}

func loadConfig(path string) (*config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("opening config: %w", err)
	}
	var cfg config
	decoder := yaml.NewDecoder(strings.NewReader(string(raw)))
	decoder.KnownFields(true)
	if err := decoder.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("parsing config: %w", err)
	}
	if cfg.Metrics.Listen == "" {
		cfg.Metrics.Listen = defaultMetricsListen
	}
	if cfg.Control.TransactionTimeout == 0 {
		cfg.Control.TransactionTimeout = publicationedge.DefaultTransactionTimeout
	}
	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("config %s: %w", path, err)
	}
	return &cfg, nil
}

func (c *config) validate() error {
	switch {
	case c.Net == "":
		return errors.New("net is required")
	case c.Store.BlocksPath == "" || !filepath.IsAbs(c.Store.BlocksPath):
		return fmt.Errorf("store.blocks_path %q must be an absolute blocks directory", c.Store.BlocksPath)
	case c.Control.Socket == "" || !filepath.IsAbs(c.Control.Socket):
		return fmt.Errorf("control.socket %q must be absolute", c.Control.Socket)
	case c.Control.StateFile == "" || !filepath.IsAbs(c.Control.StateFile):
		return fmt.Errorf("control.state_file %q must be absolute", c.Control.StateFile)
	case c.P2P.IdentityKeyFile == "" || !filepath.IsAbs(c.P2P.IdentityKeyFile):
		return fmt.Errorf("p2p.identity_key_file %q must be absolute", c.P2P.IdentityKeyFile)
	case len(c.P2P.Listen) == 0:
		return errors.New("p2p.listen must explicitly name at least one public listener")
	case len(c.P2P.RendezvousHeads) == 0:
		return errors.New("p2p.rendezvous_heads must not be empty")
	}
	if _, err := ipns.NameFromString(c.Control.IPNSName); err != nil {
		return fmt.Errorf("control.ipns_name: %w", err)
	}
	key, err := hex.DecodeString(c.Control.DocumentPublicKey)
	if err != nil || len(key) != ed25519.PublicKeySize {
		return fmt.Errorf("control.document_public_key must be %d-byte hex", ed25519.PublicKeySize)
	}
	if _, err := server.ParseArchiveID(c.Control.ArchiveID); err != nil {
		return fmt.Errorf("control.archive_id: %w", err)
	}
	if c.Control.MaxDocumentBytes < 0 {
		return fmt.Errorf("control.max_document_bytes is %d, must not be negative", c.Control.MaxDocumentBytes)
	}
	if err := publicationedge.ValidateTimeoutBudget(
		c.Control.TransactionTimeout,
		publicationedge.DefaultRequestTimeout,
		publicationedge.DefaultControlWriteTimeout,
	); err != nil {
		return fmt.Errorf("control timeout budget must be shorter than the writer default request timeout %s: %w",
			publicationedge.DefaultRequestTimeout, err)
	}
	if len(c.Control.AllowedHeads) == 0 {
		return errors.New("control.allowed_heads must explicitly name every relayed head")
	}
	for name, policy := range c.Control.AllowedHeads {
		if name == "" {
			return errors.New("control.allowed_heads contains an empty name")
		}
		switch policy.Kind {
		case server.FinalizedMonotonic:
			if policy.HandoffHead != "" {
				return fmt.Errorf("control.allowed_heads.%s finalized kind must not set handoff_head", name)
			}
			if policy.Required != nil && !*policy.Required {
				return fmt.Errorf("control.allowed_heads.%s finalized kind must be required; only mutable heads may be optional", name)
			}
		case server.UnfinalizedMutable:
			if policy.HandoffHead == "" {
				return fmt.Errorf("control.allowed_heads.%s mutable kind requires handoff_head", name)
			}
		default:
			return fmt.Errorf("control.allowed_heads.%s kind %q is unsupported", name, policy.Kind)
		}
		if policy.Required == nil {
			return fmt.Errorf("control.allowed_heads.%s.required must be explicitly true or false", name)
		}
	}
	for name, policy := range c.Control.AllowedHeads {
		if policy.Kind != server.UnfinalizedMutable {
			continue
		}
		handoff, ok := c.Control.AllowedHeads[policy.HandoffHead]
		if !ok || handoff.Kind != server.FinalizedMonotonic || handoff.Required == nil || !*handoff.Required {
			return fmt.Errorf("control.allowed_heads.%s.handoff_head %q must name a required finalized head",
				name, policy.HandoffHead)
		}
	}
	if _, err := net.ResolveTCPAddr("tcp", c.Metrics.Listen); err != nil {
		return fmt.Errorf("metrics.listen %q: %w", c.Metrics.Listen, err)
	}
	if err := p2p.ValidateExchangeConfig(c.exchangeConfig(nil, nil, nil)); err != nil {
		return err
	}
	nat := true
	if c.P2P.NATPortMap != nil {
		nat = *c.P2P.NATPortMap
	}
	return p2p.ValidateResourceControls(p2p.HostConfig{
		Listen: c.P2P.Listen, Peers: c.P2P.Peers, Announce: c.P2P.Announce,
		IdentityKeyFile: c.P2P.IdentityKeyFile, NATPortMap: nat,
		ConnectionManager: c.P2P.Connection, ResourceManager: c.P2P.Resources,
		Relay: p2p.DefaultRelayConfig(),
	})
}

func (c *config) exchangeConfig(host *p2p.Host, blocks blockstore.Blockstore, mx *metrics.Metrics) p2p.ExchangeConfig {
	return p2p.ExchangeConfig{
		Host: host, Blocks: blocks, Metrics: mx,
		MaxQueuedWantlistEntriesPerPeer: c.P2P.Bitswap.MaxQueuedWantsPerPeer,
		MaxOutstandingBytesPerPeer:      c.P2P.Bitswap.MaxOutstandingBytesPerPeer,
		TaskWorkerCount:                 c.P2P.Bitswap.SendWorkers,
		EngineTaskWorkerCount:           c.P2P.Bitswap.EngineTaskWorkers,
		EngineBlockstoreWorkerCount:     c.P2P.Bitswap.BlockstoreWorkers,
		MaxCIDSize:                      c.P2P.Bitswap.MaxCIDBytes,
	}
}

func serve(ctx context.Context, cfg *config) (retErr error) {
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	mx := metrics.New()
	blocksStore, err := store.OpenReadOnlyBlocks(cfg.Store.BlocksPath)
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, blocksStore.Close()) }()

	nat := true
	if cfg.P2P.NATPortMap != nil {
		nat = *cfg.P2P.NATPortMap
	}
	host, err := p2p.NewHost(ctx, p2p.HostConfig{
		Listen: cfg.P2P.Listen, Peers: cfg.P2P.Peers, Announce: cfg.P2P.Announce,
		IdentityKeyFile: cfg.P2P.IdentityKeyFile, NATPortMap: nat,
		ConnectionManager: cfg.P2P.Connection, ResourceManager: cfg.P2P.Resources,
		Relay: p2p.DefaultRelayConfig(), Metrics: mx, Logger: log.With("component", "p2p"),
	})
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, host.Close()) }()

	docs, err := p2p.NewDocBlockstore(blocksStore)
	if err != nil {
		return err
	}
	verifiedDocs, err := pointerhint.NewVerifiedDocumentStore(docs, 0)
	if err != nil {
		return err
	}
	exchange, err := p2p.NewExchange(ctx, cfg.exchangeConfig(host, verifiedDocs, mx))
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, exchange.Close()) }()

	explicit, err := p2p.ParsePeers(cfg.P2P.Peers)
	if err != nil {
		return err
	}
	router, err := p2p.NewPublicAminoDHT(ctx, host, p2p.MergeBootstrapPeers(explicit, p2p.PublicAminoBootstrapPeers()))
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, router.Close()) }()

	pointerCoordinator, err := pointerhint.NewCoordinator(ctx, pointerhint.CoordinatorConfig{
		Provider: pointerhint.ProviderConfig{
			Router: router, Serving: verifiedDocs, VerifiedDocuments: verifiedDocs, Metrics: mx,
			Logger: log.With("component", "pointer-provider"),
		},
		MaxHeads: len(cfg.Control.AllowedHeads),
	})
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, pointerCoordinator.Close()) }()
	pointerState, err := publicationedge.NewPointerState(pointerCoordinator, verifiedDocs)
	if err != nil {
		return err
	}

	targets := make([]p2p.RendezvousTarget, 0, len(cfg.P2P.RendezvousHeads))
	seenHeads := make(map[string]struct{}, len(cfg.P2P.RendezvousHeads))
	for _, head := range cfg.P2P.RendezvousHeads {
		if head == "" {
			return errors.New("p2p.rendezvous_heads contains an empty head")
		}
		if _, duplicate := seenHeads[head]; duplicate {
			return fmt.Errorf("p2p.rendezvous_heads duplicates %q", head)
		}
		seenHeads[head] = struct{}{}
		targets = append(targets, p2p.RendezvousTarget{Network: cfg.Net, Head: head})
	}
	rendezvous, err := p2p.NewRendezvousService(ctx, p2p.RendezvousConfig{
		Host: host, Router: router, Targets: targets, Logger: log.With("component", "rendezvous"),
	})
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, rendezvous.Close()) }()

	name, _ := ipns.NameFromString(cfg.Control.IPNSName)
	docKey, _ := hex.DecodeString(cfg.Control.DocumentPublicKey)
	archiveID, _ := server.ParseArchiveID(cfg.Control.ArchiveID)
	allowedHeads := make(map[string]publicationedge.HeadPolicy, len(cfg.Control.AllowedHeads))
	for head, policy := range cfg.Control.AllowedHeads {
		allowedHeads[head] = publicationedge.HeadPolicy{
			Kind: policy.Kind, HandoffHead: policy.HandoffHead, Required: *policy.Required,
		}
	}
	sink, err := publicationedge.NewSink(publicationedge.SinkConfig{
		Name: name, DocumentPublicKey: ed25519.PublicKey(docKey), Network: cfg.Net,
		ArchiveID: archiveID, EdgePeer: host.ID(), Documents: docs, Provider: router,
		Routing: router, Notifier: exchange, Pointers: pointerState, StateFile: cfg.Control.StateFile,
		MaxDocumentBytes:     cfg.Control.MaxDocumentBytes,
		TransactionTimeout:   cfg.Control.TransactionTimeout,
		Metrics:              mx,
		Logger:               log.With("component", "publication-sink"),
		RoutingTableSize:     func() int { return router.RoutingTable().Size() },
		AllowAdditionalPeers: cfg.Control.AllowAdditionalPeers,
		AllowedHeads:         allowedHeads,
	})
	if err != nil {
		return err
	}

	controlListener, err := listenUnix(cfg.Control.Socket)
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, controlListener.Close()) }()
	controlServer := &http.Server{
		Handler: sink.Handler(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 2 * time.Minute,
		WriteTimeout: publicationedge.DefaultControlWriteTimeout,
		IdleTimeout:  5 * time.Second, MaxHeaderBytes: 8 << 10,
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		retErr = errors.Join(retErr, controlServer.Shutdown(shutdownCtx))
	}()
	serverErrors := make(chan error, 2)
	go func() {
		if err := controlServer.Serve(controlListener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErrors <- fmt.Errorf("control server: %w", err)
		}
	}()

	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", metrics.Handler(mx, nil))
	metricsMux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !sink.Ready() {
			http.Error(w, "publication not restored", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	metricsServer := &http.Server{
		Addr: cfg.Metrics.Listen, Handler: metricsMux, ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout: 10 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 30 * time.Second,
		MaxHeaderBytes: 8 << 10,
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		retErr = errors.Join(retErr, metricsServer.Shutdown(shutdownCtx))
	}()
	go func() {
		if err := metricsServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErrors <- fmt.Errorf("metrics server: %w", err)
		}
	}()

	if sink.HasState() {
		go restoreLoop(ctx, sink, log)
	}
	log.Info("edge started", "peer_id", host.ID(), "ipns_authority", name,
		"control_socket", cfg.Control.Socket, "transaction_timeout", cfg.Control.TransactionTimeout,
		"metrics", cfg.Metrics.Listen, "dht_profile", "public-wan", "restoring", sink.HasState())

	select {
	case <-ctx.Done():
		return nil
	case err := <-serverErrors:
		return err
	}
}

func restoreLoop(ctx context.Context, sink *publicationedge.Sink, log *slog.Logger) {
	backoff := restoreMinBackoff
	for {
		restored, err := sink.Restore(ctx)
		switch {
		case err == nil && restored:
			log.Info("durable publication restored", "cid", sink.CIDFromState())
			return
		case err == nil:
			return
		case ctx.Err() != nil:
			return
		default:
			log.Warn("restoring durable publication", "err", err, "retry_in", backoff)
		}
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		backoff = min(backoff*2, restoreMaxBackoff)
	}
}

type lockedUnixListener struct {
	net.Listener
	path string
	lock *os.File
	once sync.Once
	err  error
}

func listenUnix(path string) (*lockedUnixListener, error) {
	if path == "" || !filepath.IsAbs(path) || len(path) >= 104 {
		return nil, fmt.Errorf("control socket %q must be absolute and shorter than 104 bytes", path)
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("creating control directory %s: %w", dir, err)
	}
	lock, err := os.OpenFile(path+".lock", os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("opening control lock: %w", err)
	}
	if err := unix.Flock(int(lock.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = lock.Close()
		return nil, fmt.Errorf("locking control socket ownership: %w", err)
	}
	if info, err := os.Lstat(path); err == nil {
		if info.Mode()&os.ModeSocket == 0 {
			_ = lock.Close()
			return nil, fmt.Errorf("refusing to remove non-socket control path %s", path)
		}
		if err := os.Remove(path); err != nil {
			_ = lock.Close()
			return nil, fmt.Errorf("removing stale control socket %s: %w", path, err)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		_ = lock.Close()
		return nil, fmt.Errorf("statting control socket %s: %w", path, err)
	}
	listener, err := net.Listen("unix", path)
	if err != nil {
		_ = lock.Close()
		return nil, fmt.Errorf("listening on control socket %s: %w", path, err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		_ = listener.Close()
		_ = os.Remove(path)
		_ = lock.Close()
		return nil, fmt.Errorf("chmod control socket %s: %w", path, err)
	}
	return &lockedUnixListener{Listener: listener, path: path, lock: lock}, nil
}

func (l *lockedUnixListener) Close() error {
	l.once.Do(func() {
		l.err = l.Listener.Close()
		if errors.Is(l.err, net.ErrClosed) {
			l.err = nil
		}
		if err := os.Remove(l.path); err != nil && !errors.Is(err, os.ErrNotExist) {
			l.err = errors.Join(l.err, err)
		}
		if err := unix.Flock(int(l.lock.Fd()), unix.LOCK_UN); err != nil {
			l.err = errors.Join(l.err, err)
		}
		l.err = errors.Join(l.err, l.lock.Close())
	})
	return l.err
}
