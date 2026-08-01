package main

import (
	"context"
	"crypto/ed25519"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strings"

	"github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/go-cid"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"

	"github.com/blobarchive/bloar/metrics"
	"github.com/blobarchive/bloar/p2p"
	publicationedge "github.com/blobarchive/bloar/p2p/edge"
	"github.com/blobarchive/bloar/p2p/pointerhint"
	"github.com/blobarchive/bloar/store"
)

func (c *Config) validateBitswap() error {
	if !c.P2P.Host() {
		return nil
	}
	if err := p2p.ValidateExchangeConfig(c.P2P.Bitswap.exchangeConfig(nil, nil, nil)); err != nil {
		return err
	}
	return nil
}

func (c *Config) validateP2PResources() error {
	if !c.P2P.Host() {
		return nil
	}
	hostConfig, err := c.P2P.hostConfig(nil, nil)
	if err != nil {
		return err
	}
	if err := p2p.ValidateResourceControls(hostConfig); err != nil {
		return err
	}
	_, err = p2p.RelayOptions(hostConfig.Relay)
	return err
}

// hostConfig is the single YAML-to-library translation point for the libp2p
// host. Tests pin every exposed policy field here so strict YAML acceptance
// cannot drift from the running host's actual resource policy.
func (c P2PConfig) hostConfig(mx *metrics.Metrics, log *slog.Logger) (p2p.HostConfig, error) {
	relay, err := c.Relay.coreConfig()
	if err != nil {
		return p2p.HostConfig{}, err
	}
	return p2p.HostConfig{
		Listen:            c.Listen,
		Peers:             c.Peers,
		Announce:          c.Announce,
		IdentityKeyFile:   c.IdentityKeyFile,
		NATPortMap:        c.NATPortMap,
		ConnectionManager: c.ConnectionManager,
		ResourceManager:   c.ResourceManager,
		Relay:             relay,
		Metrics:           mx,
		Logger:            log,
	}, nil
}

func (c RelayConfig) coreConfig() (p2p.RelayConfig, error) {
	candidates, err := parseRelayCandidates(c.StaticCandidates)
	if err != nil {
		return p2p.RelayConfig{}, err
	}
	serviceEnabled := c.Service.Enabled != nil && *c.Service.Enabled
	holePunching := c.HolePunching != nil && *c.HolePunching
	if len(candidates) == 0 && c.AutoRelay.configured() {
		return p2p.RelayConfig{}, fmt.Errorf("p2p.relay.auto_relay is configured but static_candidates is empty")
	}
	if len(candidates) > 0 && !holePunching {
		return p2p.RelayConfig{}, fmt.Errorf("p2p.relay.static_candidates requires hole_punching: relay circuits are Bloar's DCUtR control plane, not a Bitswap data path")
	}
	resolved := p2p.RelayConfig{
		EnableService:      serviceEnabled,
		EnableHolePunching: holePunching,
		StaticRelays:       candidates,
		Service: p2p.RelayServiceConfig{
			ReservationTTL:        c.Service.ReservationTTL,
			MaxReservations:       c.Service.MaxReservations,
			MaxCircuitsPerPeer:    c.Service.MaxCircuitsPerPeer,
			BufferSizeBytes:       c.Service.BufferSizeBytes,
			MaxReservationsPerIP:  c.Service.MaxReservationsPerIP,
			MaxReservationsPerASN: c.Service.MaxReservationsPerASN,
			CircuitDuration:       c.Service.CircuitDuration,
			CircuitDataBytes:      c.Service.CircuitDataBytes,
		},
		AutoRelay: p2p.AutoRelayConfig{
			DesiredReservations: c.AutoRelay.DesiredReservations,
			MinInterval:         c.AutoRelay.MinInterval,
			BootDelay:           c.AutoRelay.BootDelay,
			Backoff:             c.AutoRelay.Backoff,
			MaxCandidateAge:     c.AutoRelay.MaxCandidateAge,
		},
	}
	// Validate service limits even when the service is explicitly disabled, so
	// a later one-line enable cannot activate a latent unsafe value.
	validation := resolved
	validation.EnableService = true
	if _, err := p2p.RelayOptions(validation); err != nil {
		return p2p.RelayConfig{}, err
	}
	return resolved, nil
}

func (c AutoRelayConfig) configured() bool {
	return c.DesiredReservations != 0 || c.MinInterval != 0 || c.BootDelay != 0 ||
		c.Backoff != 0 || c.MaxCandidateAge != 0
}

func parseRelayCandidates(configured []string) ([]peer.AddrInfo, error) {
	if len(configured) == 0 {
		return nil, nil
	}
	out := make([]peer.AddrInfo, 0, len(configured))
	byPeer := make(map[peer.ID]int, len(configured))
	seenAddress := make(map[peer.ID]map[string]struct{}, len(configured))
	for i, raw := range configured {
		if raw == "" || raw != strings.TrimSpace(raw) {
			return nil, fmt.Errorf("p2p.relay.static_candidates[%d] must be a non-empty peer multiaddr without surrounding whitespace", i)
		}
		address, err := ma.NewMultiaddr(raw)
		if err != nil {
			return nil, fmt.Errorf("p2p.relay.static_candidates[%d] %q is not a multiaddr: %w", i, raw, err)
		}
		candidate, err := peer.AddrInfoFromP2pAddr(address)
		if err != nil {
			return nil, fmt.Errorf("p2p.relay.static_candidates[%d] %q must end in /p2p/<peerid>: %w", i, raw, err)
		}
		index, exists := byPeer[candidate.ID]
		if !exists {
			index = len(out)
			byPeer[candidate.ID] = index
			seenAddress[candidate.ID] = make(map[string]struct{})
			out = append(out, peer.AddrInfo{ID: candidate.ID})
		}
		for _, direct := range candidate.Addrs {
			key := string(direct.Bytes())
			if _, duplicate := seenAddress[candidate.ID][key]; duplicate {
				continue
			}
			seenAddress[candidate.ID][key] = struct{}{}
			out[index].Addrs = append(out[index].Addrs, direct)
		}
	}
	return out, nil
}

// exchangeConfig is the single YAML-to-library translation point. Keeping the
// mapping in a testable helper prevents a newly exposed cap from being accepted
// by strict YAML and then accidentally omitted from the running exchange.
func (c BitswapConfig) exchangeConfig(host *p2p.Host, blocks blockstore.Blockstore, mx *metrics.Metrics) p2p.ExchangeConfig {
	return p2p.ExchangeConfig{
		Host:                            host,
		Blocks:                          blocks,
		Metrics:                         mx,
		DisableServer:                   !c.serves(),
		MaxQueuedWantlistEntriesPerPeer: c.MaxQueuedWantsPerPeer,
		MaxOutstandingBytesPerPeer:      c.MaxOutstandingBytesPerPeer,
		TaskWorkerCount:                 c.SendWorkers,
		EngineTaskWorkerCount:           c.EngineTaskWorkers,
		EngineBlockstoreWorkerCount:     c.BlockstoreWorkers,
		MaxCIDSize:                      c.MaxCIDBytes,
	}
}

func (c BitswapConfig) serves() bool {
	return c.Serve == nil || *c.Serve
}

// p2pStack is this node's libp2p surface, or the absence of one. Every field is
// nil on a config with no p2p block, and every method on it is written to mean
// the right thing in that state: a bloard that does not speak libp2p is a whole
// role of spec 11.1, not a degraded one.
type p2pStack struct {
	host               *p2p.Host
	exchange           *p2p.Exchange
	dht                *dht.IpfsDHT
	rendezvous         *p2p.RendezvousService
	publisher          *p2p.Publisher
	documents          *pointerhint.VerifiedDocumentStore
	pointerFinder      *pointerhint.Finder
	pointerCoordinator *pointerhint.Coordinator
	pointers           *pointerState
}

type p2pSetupDeps struct {
	publicBootstrapPeers func() []peer.AddrInfo
}

// setupP2P builds the stack cfg asks for: nothing, a host with Bitswap, and --
// by default for an embedded host -- an Amino DHT plus rendezvous discovery.
// IPNS publishing/resolution shares that DHT. A failure part-way closes what it
// built.
func setupP2P(ctx context.Context, cfg *Config, st *store.Store, signingKey ed25519.PrivateKey, mx *metrics.Metrics, log *slog.Logger) (*p2pStack, error) {
	return setupP2PWithDeps(ctx, cfg, st, signingKey, mx, log, p2pSetupDeps{
		publicBootstrapPeers: p2p.PublicAminoBootstrapPeers,
	})
}

// setupP2PWithDeps is the hermetic constructor seam. Tests can replace the
// static public-bootstrap source without changing process globals; private mode
// never calls it at all.
func setupP2PWithDeps(ctx context.Context, cfg *Config, st *store.Store, signingKey ed25519.PrivateKey, mx *metrics.Metrics, log *slog.Logger, deps p2pSetupDeps) (*p2pStack, error) {
	n := &p2pStack{}
	if cfg.Publish.Edge != nil && cfg.Publish.Edge.Mode == "required" {
		key, err := p2p.LoadIdentity(cfg.Publish.Edge.IdentityKeyFile)
		if err != nil {
			return nil, err
		}
		authority, err := peer.IDFromPrivateKey(key)
		if err != nil {
			return nil, fmt.Errorf("bloard: deriving private IPNS authority: %w", err)
		}
		edgePeer, err := validateEdgeMultiaddrs(cfg.Publish.Edge.Multiaddrs)
		if err != nil {
			return nil, err
		}
		if authority == edgePeer {
			return nil, fmt.Errorf("bloard: private IPNS authority %s equals public edge peer: use a distinct edge identity", authority)
		}
		policy, err := publicationedge.NewClientPolicy(publicationedge.ClientConfig{
			Socket:             cfg.Publish.Edge.ControlSocket,
			TransactionTimeout: cfg.Publish.Edge.TransactionTimeout,
			RequestTimeout:     cfg.Publish.Edge.RequestTimeout,
			MaxDocumentBytes:   cfg.Publish.Edge.MaxDocumentBytes,
		})
		if err != nil {
			return nil, err
		}
		publisher, err := p2p.NewPublisher(p2p.PublisherConfig{
			Key: key, Policy: policy, KV: st.KV(),
			Republish: cfg.Publish.IPNSRepublish,
			Logger:    log.With("component", "ipns"),
			Metrics:   mx,
		})
		if err != nil {
			return nil, err
		}
		n.publisher = publisher
		log.Info("private IPNS authority configured", "name", publisher.Name(), "edge_peer", edgePeer,
			"edge_socket", cfg.Publish.Edge.ControlSocket, "republish", cfg.Publish.IPNSRepublish)
		return n, nil
	}
	if !cfg.P2P.Host() {
		return n, nil
	}

	hostConfig, err := cfg.P2P.hostConfig(mx, log.With("component", "p2p"))
	if err != nil {
		return nil, err
	}
	host, err := p2p.NewHost(ctx, hostConfig)
	if err != nil {
		return nil, err
	}
	n.host = host
	if cfg.Publish.Edge != nil && cfg.Publish.Edge.Mode == "mirror" {
		edgePeer, err := validateEdgeMultiaddrs(cfg.Publish.Edge.Multiaddrs)
		if err != nil {
			n.close(log)
			return nil, err
		}
		if host.ID() == edgePeer {
			n.close(log)
			return nil, fmt.Errorf("bloard: mirror edge peer %s equals the incumbent IPNS authority: use a distinct edge identity", edgePeer)
		}
	}

	// The document blockstore is built whether or not IPNS is on: with no
	// publisher writing into it, it is the store's blockstore, and building it
	// unconditionally is one branch fewer between here and bitswap.
	localDocs, err := p2p.NewDocBlockstore(st.Blocks())
	if err != nil {
		n.close(log)
		return nil, err
	}
	documents, err := pointerhint.NewVerifiedDocumentStore(localDocs, 0)
	if err != nil {
		n.close(log)
		return nil, err
	}
	n.documents = documents

	// Spec 11.2's server half: this node serves what it has to peers, in either
	// role. The client half is the same object, and is what a phase-8b follower
	// will hand to p2p.FetchingBlockstore.
	exchange, err := p2p.NewExchange(ctx, cfg.P2P.Bitswap.exchangeConfig(host, documents, mx))
	if err != nil {
		n.close(log)
		return nil, err
	}
	n.exchange = exchange

	// The DHT carries IPNS values and synthetic rendezvous provider records. It
	// is deliberately not passed to Bitswap as a generic content router.
	if !cfg.Publish.IPNS && !cfg.followsIPNS() && !cfg.P2P.Rendezvous.enabled() {
		if err := n.setupPointerState(cfg, signingKey, nil, log); err != nil {
			n.close(log)
			return nil, err
		}
		return n, nil
	}

	// Explicit peers always seed the DHT. Public mode augments them with the
	// Amino defaults; private mode does not even read the public set.
	explicit, err := p2p.ParsePeers(cfg.P2P.Peers)
	if err != nil {
		n.close(log)
		return nil, err
	}
	bootstrapMode := cfg.P2P.DHT.bootstrapMode()
	bootstrap := p2p.MergeBootstrapPeers(explicit)
	if bootstrapMode == "public" {
		if deps.publicBootstrapPeers == nil {
			n.close(log)
			return nil, fmt.Errorf("bloard: public DHT bootstrap selected without a bootstrap peer source")
		}
		bootstrap = p2p.MergeBootstrapPeers(explicit, deps.publicBootstrapPeers())
	}
	routing, err := p2p.NewDHT(ctx, host, bootstrap)
	if err != nil {
		n.close(log)
		return nil, err
	}
	n.dht = routing
	finder, err := pointerhint.NewFinder(pointerhint.FinderConfig{
		Router: routing,
		Host:   host.Libp2p(),
	})
	if err != nil {
		n.close(log)
		return nil, err
	}
	n.pointerFinder = finder

	if cfg.P2P.Bitswap.serves() {
		coordinator, err := pointerhint.NewCoordinator(ctx, pointerhint.CoordinatorConfig{
			Provider: pointerhint.ProviderConfig{
				Router:            routing,
				Serving:           documents,
				VerifiedDocuments: documents,
				Metrics:           mx,
				Logger:            log.With("component", "pointer-provider"),
			},
			MaxHeads: len(cfg.Heads) + len(cfg.Follow.heads()),
		})
		if err != nil {
			n.close(log)
			return nil, err
		}
		n.pointerCoordinator = coordinator
	}
	if err := n.setupPointerState(cfg, signingKey, n.pointerCoordinator, log); err != nil {
		n.close(log)
		return nil, err
	}

	if bootstrapMode == "private" && len(bootstrap) == 0 {
		log.Warn("private DHT bootstrap has no p2p.peers: this node reaches only peers that dial it")
	}
	log.Info("amino dht started", "bootstrap_mode", bootstrapMode, "bootstrap_peers", len(bootstrap))

	if cfg.P2P.Rendezvous.enabled() {
		rendezvous, err := p2p.NewRendezvousService(ctx, rendezvousConfig(cfg, host, routing, log))
		if err != nil {
			n.close(log)
			return nil, err
		}
		n.rendezvous = rendezvous
		log.Info("rendezvous discovery started", "targets", len(rendezvousTargets(cfg)),
			"advertising", cfg.P2P.Bitswap.serves())
	}
	if !cfg.Publish.IPNS {
		return n, nil
	}

	policy, err := p2p.NewLocalPublicationPolicy(localDocs, routing, routing)
	if err != nil {
		n.close(log)
		return nil, err
	}
	if cfg.Publish.Edge != nil && cfg.Publish.Edge.Mode == "mirror" {
		edgePolicy, edgeErr := publicationedge.NewClientPolicy(publicationedge.ClientConfig{
			Socket:             cfg.Publish.Edge.ControlSocket,
			TransactionTimeout: cfg.Publish.Edge.TransactionTimeout,
			RequestTimeout:     cfg.Publish.Edge.RequestTimeout,
			MaxDocumentBytes:   cfg.Publish.Edge.MaxDocumentBytes,
		})
		if edgeErr != nil {
			n.close(log)
			return nil, edgeErr
		}
		policy, err = p2p.NewMirrorPublicationPolicy(policy, edgePolicy, func(err error) {
			log.Error("edge publication mirror", "err", err)
		})
		if err != nil {
			n.close(log)
			return nil, err
		}
	}
	publisher, err := p2p.NewPublisher(p2p.PublisherConfig{
		Host: host, Policy: policy, KV: st.KV(),
		Republish: cfg.Publish.IPNSRepublish,
		Logger:    log.With("component", "ipns"),
		Metrics:   mx,
	})
	if err != nil {
		n.close(log)
		return nil, err
	}
	n.publisher = publisher

	log.Info("ipns publishing", "name", publisher.Name(), "republish", cfg.Publish.IPNSRepublish,
		"lifetime", p2p.DefaultLifetime)
	return n, nil
}

func rendezvousConfig(cfg *Config, host *p2p.Host, routing *dht.IpfsDHT, log *slog.Logger) p2p.RendezvousConfig {
	return p2p.RendezvousConfig{
		Host:             host,
		Router:           routing,
		Targets:          rendezvousTargets(cfg),
		DisableProviding: !cfg.P2P.Bitswap.serves(),
		Logger:           log.With("component", "rendezvous"),
	}
}

func rendezvousTargets(cfg *Config) []p2p.RendezvousTarget {
	names := make(map[string]struct{}, len(cfg.Heads)+len(cfg.Follow.heads()))
	for name := range cfg.Heads {
		names[name] = struct{}{}
	}
	for name := range cfg.Follow.heads() {
		names[name] = struct{}{}
	}
	sorted := slices.Sorted(maps.Keys(names))
	targets := make([]p2p.RendezvousTarget, len(sorted))
	for i, name := range sorted {
		targets[i] = p2p.RendezvousTarget{Network: cfg.Net, Head: name}
	}
	return targets
}

// followsIPNS reports whether this node resolves an IPNS name directly or
// through one DNSLink hop (spec 11.3).
func (c *Config) followsIPNS() bool {
	if c.Follow == nil {
		return false
	}
	if c.Follow.IPNS != "" || c.Follow.DNSLink != "" {
		return true
	}
	for _, source := range c.Follow.Sources {
		if source.IPNS != "" || source.DNSLink != "" {
			return true
		}
	}
	return false
}

type noopPointerSchedule struct{}

func (noopPointerSchedule) ReplaceAllWithDocuments(map[string]pointerhint.Set, []cid.Cid) error {
	return nil
}

func (n *p2pStack) setupPointerState(cfg *Config, signingKey ed25519.PrivateKey, coordinator *pointerhint.Coordinator, log *slog.Logger) error {
	if n.documents == nil {
		return nil
	}
	written := make(map[string]struct{}, len(cfg.Heads))
	for name := range cfg.Heads {
		written[name] = struct{}{}
	}
	followed := make(map[string]struct{}, len(cfg.Follow.heads()))
	for name := range cfg.Follow.heads() {
		followed[name] = struct{}{}
	}

	var localSigner ed25519.PublicKey
	if len(signingKey) != 0 {
		pub, ok := signingKey.Public().(ed25519.PublicKey)
		if !ok {
			return fmt.Errorf("bloard: signing key has no ed25519 public key for pointer state")
		}
		localSigner = pub
	}
	var schedule pointerSchedule = noopPointerSchedule{}
	if coordinator != nil {
		schedule = coordinator
	}
	pointers, err := newPointerState(pointerStateConfig{
		Net:               cfg.Net,
		WrittenHeads:      written,
		FollowedHeads:     followed,
		LocalSigner:       localSigner,
		Coordinator:       schedule,
		VerifiedDocuments: n.documents,
		OnWorkerError: func(err error) {
			log.Error("rejecting local exact-pointer update", "err", err)
		},
	})
	if err != nil {
		return err
	}
	n.pointers = pointers
	return nil
}

// multiaddrs is what the publication document claims blocks can be fetched from
// (spec 8). With a host, the host decides (p2p.announce, or what it bound); with
// no host, it is whatever the operator wrote, which is a claim about some other
// node and theirs to make.
func (n *p2pStack) multiaddrs(cfg *Config) []string {
	var addresses []string
	if n.host == nil {
		addresses = append(addresses, cfg.P2P.Announce...)
	} else {
		addresses = append(addresses, n.host.AnnounceAddrs()...)
	}
	if cfg.Publish.Edge != nil {
		addresses = append(addresses, cfg.Publish.Edge.Multiaddrs...)
	}
	if len(addresses) < 2 {
		return addresses
	}
	seen := make(map[string]struct{}, len(addresses))
	out := addresses[:0]
	for _, address := range addresses {
		if _, exists := seen[address]; exists {
			continue
		}
		seen[address] = struct{}{}
		out = append(out, address)
	}
	return out
}

// onDoc is the server.HeadsConfig hook. Both consumers are copy/coalesce-only:
// Heads invokes it under its mutation lock, so neither publication parsing nor
// any DHT work may happen on this call path.
func (n *p2pStack) onDoc() func([]byte) {
	if n.publisher == nil && n.pointers == nil {
		return nil
	}
	return func(doc []byte) {
		if n.publisher != nil {
			n.publisher.Notify(doc)
		}
		if n.pointers != nil {
			n.pointers.NotifyLocalDocument(doc)
		}
	}
}

// start begins publishing. It is called once the heads are open, so that the
// first record names a document with this node's heads in it rather than the
// empty one the registry rebuilds on construction.
func (n *p2pStack) start(ctx context.Context) error {
	if n.pointers != nil {
		if err := n.pointers.Start(ctx); err != nil {
			return err
		}
	}
	if n.publisher != nil {
		n.publisher.Start()
	}
	return nil
}

// close unwinds the stack in dependency order: the publisher is using the DHT,
// and the DHT and bitswap are using the host. Failures are logged rather than
// returned -- this runs from a defer on the way out, and there is nobody left
// to tell.
func (n *p2pStack) close(log *slog.Logger) {
	if n == nil {
		return
	}
	if n.pointers != nil {
		n.pointers.Close()
	}
	if n.pointerCoordinator != nil {
		if err := n.pointerCoordinator.Close(); err != nil {
			log.Error("closing exact-pointer provider", "err", err)
		}
	}
	if n.publisher != nil {
		if err := n.publisher.Close(); err != nil {
			log.Error("closing ipns publisher", "err", err)
		}
	}
	if n.rendezvous != nil {
		if err := n.rendezvous.Close(); err != nil {
			log.Error("closing rendezvous discovery", "err", err)
		}
	}
	if n.exchange != nil {
		if err := n.exchange.Close(); err != nil {
			log.Error("closing bitswap", "err", err)
		}
	}
	if n.dht != nil {
		if err := n.dht.Close(); err != nil {
			log.Error("closing dht", "err", err)
		}
	}
	if n.host != nil {
		if err := n.host.Close(); err != nil {
			log.Error("closing p2p host", "err", err)
		}
	}
}
