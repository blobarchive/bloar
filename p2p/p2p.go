// Package p2p is bloar's libp2p surface: the host both roles run (spec 11.1),
// the bitswap exchange that moves blocks between them (spec 11.2), and the IPNS
// publication channel of spec 8.1.
//
// # What is here, and what a follower takes from it
//
// Host is the libp2p host: a persistent identity, listen addresses, static
// peers it keeps a connection to, and the announce addresses that go into the
// publication document. Exchange is bitswap over that host, in both directions
// at once -- it serves the local blockstore to peers and opens sessions to
// fetch from them, because spec 11.2 gives every node both halves.
//
// FetchingBlockstore is the piece the follower protocol is built out of: a
// blockstore that answers from local blocks and, on a miss, fetches over
// bitswap. Everything a follower does -- pin reconciliation backfilling a DAG,
// a read that misses (spec 11.3, 11.4) -- is that one substitution, and no
// other code has to know whether a block came off the disk or off the network.
// Its comment is the contract; read it before wiring it anywhere.
//
// Publisher is the IPNS half of head publication. It stores each publication
// document as a raw block and names that block in an IPNS record it puts to the
// DHT. Resolve is the other end, for followers.
//
// # The identity key is the IPNS key
//
// Spec 8.1 allows the IPNS record to be signed by the same key as the
// publication document. This package goes one step further and signs with the
// libp2p identity itself, which is what makes the IPNS name and the PeerID the
// same key in two encodings: the name a follower resolves and the peer it then
// fetches blocks from are provably the same node, with nothing to cross-check.
// The identity key file takes the same format as publish.signing_key_file, so
// an operator who wants spec 8.1's one-key deployment can point both at one
// file.
//
// # Key layout
//
// This package owns one byte of the node-local KV keyspace store.KV() hands
// out, under the rule catalog's package comment states: single-byte prefixes,
// no key of one structure a prefix of another's. catalog owns 'c' (blob
// catalog) and 'p' (pin ledger), server owns 'h' (head roots), and this package
// owns 'i':
//
//	ipns sequence  key: 'i' || "seq"
//	               val: 8-byte big-endian uint64
//
// One node publishes one IPNS name, so the space holds exactly one key; it is
// spelled with a suffix rather than as a bare 'i' so that a second thing this
// package one day has to persist has somewhere to go.
package p2p

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/event"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/p2p/host/eventbus"
	ma "github.com/multiformats/go-multiaddr"

	"github.com/blobarchive/bloar/metrics"
)

// Static-peer redial schedule. Spec 11.2 makes peering static, so a peer that
// is down is a peer that will come back, and the only question is how often to
// ask. The backoff caps well under the record lifetimes anything here depends
// on; the check interval is what a live connection costs.
const (
	peerDialTimeout   = 30 * time.Second
	peerMinBackoff    = 1 * time.Second
	peerMaxBackoff    = 5 * time.Minute
	peerCheckInterval = 30 * time.Second
)

// HostConfig is spec 12's p2p block.
type HostConfig struct {
	// Listen is p2p.listen: the multiaddrs to bind. Empty binds nothing, which
	// is a host that dials but cannot be dialled -- a follower behind a NAT.
	Listen []string
	// Peers is p2p.peers: the static peers of spec 11.2, dialled at startup and
	// kept connected.
	Peers []string
	// Announce is p2p.announce: the multiaddrs the publication document claims
	// blocks can be fetched from. Empty derives them from the bound addresses;
	// see AnnounceAddrs.
	Announce []string
	// IdentityKeyFile is the ed25519 key this node's PeerID -- and its IPNS
	// name, when the channel of spec 8.1 is on -- is derived from. Required,
	// and created on first use: an identity that changed per restart would
	// invalidate every multiaddr this node has ever published.
	IdentityKeyFile string
	// NATPortMap asks the host to try UPnP/NAT-PMP mappings for its listeners.
	// The command defaults this on for an enabled p2p block; it remains explicit
	// here so callers and tests can opt out without hidden package defaults.
	NATPortMap bool
	// ConnectionManager governs pruning after a connection burst. Its zero
	// value resolves to Bloar's pinned watermarks and grace period.
	ConnectionManager ConnectionManagerConfig
	// ResourceManager is the independent hard admission boundary for libp2p
	// connections, streams, memory, and file descriptors. Its zero value uses
	// Bloar's pinned home-node policy.
	ResourceManager ResourceManagerConfig
	// Relay is the bounded circuit-v2, static AutoRelay, and DCUtR policy. The
	// package zero value is inert; the daemon explicitly supplies
	// DefaultRelayConfig for every enabled embedded swarm.
	Relay RelayConfig
	// Metrics records the observed AutoNAT state. Optional; nil disables libp2p
	// collectors that honor the metrics option. Libp2p v0.48's AutoNATv2
	// auxiliary event bus is a bounded process-global exception, and upstream
	// collector values registered in more than one registry are shared.
	Metrics *metrics.Metrics
	// Logger receives dial outcomes and the identity's provenance. Optional.
	Logger *slog.Logger
}

// Host is the libp2p host of spec 11.2, plus the static-peer supervision that
// makes "peering is static in v1" true after the first disconnection.
//
// Its lifetime is its own: the goroutines that keep static peers connected run
// until Close, not until the context that built it is cancelled. A daemon
// shutting down wants the host to outlive the cancellation that started the
// shutdown, because the things layered on it (bitswap, the IPNS publisher) are
// still unwinding.
type Host struct {
	h        host.Host
	announce []string
	log      *slog.Logger
	mx       *metrics.Metrics

	cancel context.CancelFunc
	wg     sync.WaitGroup

	closeOnce sync.Once
	closeErr  error

	reachabilitySub event.Subscription
	peerState       *hostPeerState
}

// NewHost builds the host from cfg. ctx bounds construction only; the host runs
// until Close.
func NewHost(ctx context.Context, cfg HostConfig) (*Host, error) {
	log := cfg.Logger
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}

	key, created, err := LoadOrCreateIdentity(cfg.IdentityKeyFile)
	if err != nil {
		return nil, err
	}
	self, err := peer.IDFromPrivateKey(key)
	if err != nil {
		return nil, fmt.Errorf("p2p: deriving peer ID from identity key: %w", err)
	}

	listen, err := parseMultiaddrs("p2p.listen", cfg.Listen)
	if err != nil {
		return nil, err
	}
	peers, err := parsePeers(cfg.Peers)
	if err != nil {
		return nil, err
	}
	identifyAddrs, publishedAddrs, err := configuredAnnounce(cfg.Announce, self)
	if err != nil {
		return nil, err
	}
	relayOpts, err := RelayOptions(cfg.Relay)
	if err != nil {
		return nil, err
	}
	protectedPeers := append(append([]peer.AddrInfo(nil), peers...), cfg.Relay.StaticRelays...)
	resourceOpts, resourcePolicy, resourceOwnership, err := resourceControlOptions(cfg, protectedPeers)
	if err != nil {
		return nil, err
	}

	opts := []libp2p.Option{libp2p.Identity(key)}
	opts = append(opts, reachabilityOptions(cfg.NATPortMap)...)
	if len(listen) == 0 {
		// Not the same as passing no addresses: libp2p reads an unset listen
		// list as "use the defaults" and would bind :4001 behind our back.
		opts = append(opts, libp2p.NoListenAddrs)
	} else {
		opts = append(opts, libp2p.ListenAddrs(listen...))
	}
	if len(identifyAddrs) > 0 {
		// Configured announce addresses are not merely publication-document
		// metadata. Identify must tell connected peers the same reachability
		// claim. A host address never carries its own terminal /p2p component,
		// so configuredAnnounce strips that component before it reaches here.
		factoryAddrs := append([]ma.Multiaddr(nil), identifyAddrs...)
		opts = append(opts, libp2p.AddrsFactory(func([]ma.Multiaddr) []ma.Multiaddr {
			return append([]ma.Multiaddr(nil), factoryAddrs...)
		}))
	}
	opts = append(opts, resourceOpts...)
	opts = append(opts, relayOpts...)

	h, err := constructLibp2pHost(opts, resourceOwnership)
	if err != nil {
		return nil, fmt.Errorf("p2p: building libp2p host: %w", err)
	}

	p := &Host{h: h, log: log, mx: cfg.Metrics}
	p.mx.P2PReachability(metrics.P2PReachabilityUnknown)
	if publishedAddrs != nil {
		p.announce = publishedAddrs
	} else {
		p.announce, err = derivedAnnounceAddrs(h)
	}
	if err != nil {
		if cerr := h.Close(); cerr != nil {
			err = errors.Join(err, cerr)
		}
		return nil, err
	}
	p.reachabilitySub, err = h.EventBus().Subscribe(
		new(event.EvtLocalReachabilityChanged),
		eventbus.Name("bloar reachability logger"),
		eventbus.BufSize(4),
	)
	if err != nil {
		if cerr := h.Close(); cerr != nil {
			err = errors.Join(err, cerr)
		}
		return nil, fmt.Errorf("p2p: observing reachability: %w", err)
	}
	p.peerState = newHostPeerState(h.Network(), cfg.Metrics, peers)

	if created {
		log.Info("p2p identity created", "file", cfg.IdentityKeyFile, "peer_id", h.ID())
	}
	log.Info("p2p host listening", "peer_id", h.ID(), "listen_addrs", h.Network().ListenAddresses(),
		"announce_addrs", p.announce, "nat_port_map", cfg.NATPortMap,
		"conn_low_watermark", resourcePolicy.connections.LowWatermark,
		"conn_high_watermark", resourcePolicy.connections.HighWatermark,
		"conn_grace_period", resourcePolicy.connections.GracePeriod,
		"resource_connections", resourcePolicy.resources.Connections,
		"resource_streams", resourcePolicy.resources.Streams,
		"resource_memory_bytes", resourcePolicy.resources.MemoryBytes,
		"resource_peer_memory_bytes", resourcePolicy.resources.PeerMemoryBytes,
		"resource_peer_file_descriptors", resourcePolicy.resources.PeerFileDescriptors,
		"static_peer_connection_headroom", DefaultResourceStaticPeerConnectionHeadroom,
		"relay_service", cfg.Relay.EnableService,
		"relay_static_candidates", len(cfg.Relay.StaticRelays),
		"dcutr", cfg.Relay.EnableHolePunching)

	bg, cancel := context.WithCancel(context.WithoutCancel(ctx))
	p.cancel = cancel
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		p.logReachability(bg)
	}()
	for _, ai := range peers {
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			p.keepConnected(bg, ai)
		}()
	}
	return p, nil
}

// reachabilityOptions is kept separate from host construction so the feature
// defaults can be asserted without performing UPnP discovery in a test. AutoNAT
// v1 is part of libp2p's basic host. V2 adds per-address probing, which is
// useful for a host carrying both TCP and QUIC listeners. Neither is forced
// public: reachability remains an observed property.
func reachabilityOptions(natPortMap bool) []libp2p.Option {
	opts := []libp2p.Option{libp2p.EnableAutoNATv2()}
	if natPortMap {
		opts = append(opts, libp2p.NATPortMap())
	}
	return opts
}

// ID returns the host's PeerID. It is also the IPNS name this node publishes
// under, in a different encoding; see Publisher.
func (p *Host) ID() peer.ID { return p.h.ID() }

// Libp2p returns the underlying host, for the things built on top of it.
func (p *Host) Libp2p() host.Host { return p.h }

// AnnounceAddrs returns the multiaddrs for the publication document's
// "multiaddrs" (spec 8): p2p.announce if the operator set it -- each addr
// carrying this host's /p2p/<peerid>, appended where the operator omitted it --
// and otherwise the host's bound addresses with that same suffix.
//
// The derived form is a convenience, not a guess at reachability. A wildcard
// listen expands to every address the host bound, loopback and RFC1918
// included, and a node behind a NAT will publish addresses no follower can
// reach. That is why p2p.announce exists and why it wins outright: reachability
// is a claim only the operator can make.
func (p *Host) AnnounceAddrs() []string { return p.announce }

// Close stops the static-peer supervision and closes the host.
func (p *Host) Close() error {
	p.closeOnce.Do(func() {
		p.cancel()
		if p.reachabilitySub != nil {
			if err := p.reachabilitySub.Close(); err != nil {
				p.closeErr = errors.Join(p.closeErr, fmt.Errorf("p2p: closing reachability observer: %w", err))
			}
		}
		p.wg.Wait()
		p.peerState.close()
		if err := p.h.Close(); err != nil {
			p.closeErr = errors.Join(p.closeErr, fmt.Errorf("p2p: closing libp2p host: %w", err))
		}
	})
	return p.closeErr
}

// configuredAnnounce validates and renders a configured announce list for its
// two consumers. Identify gets transport addresses with the terminal
// /p2p/<self> removed; the publication document gets full addresses naming
// this host. A nil publication result means no override was configured.
//
// A configured list is the operator's reachability claim, published as written
// -- with one completion. A follower dials the /p2p/<peerid> an announce
// multiaddr names (follow.dial), so an addr without one carries no identity to
// dial and a follower rejects it as unusable. This host's own peer ID is the
// only identity a configured announce may carry: an addr that omits it is
// completed with it, an addr that already names this host is left as written,
// and an addr naming a different peer is a config error -- announcing another
// node's identity -- that stops startup rather than being published.
func configuredAnnounce(configured []string, self peer.ID) ([]ma.Multiaddr, []string, error) {
	if len(configured) == 0 {
		return nil, nil, nil
	}
	parsed, err := parseMultiaddrs("p2p.announce", configured)
	if err != nil {
		return nil, nil, err
	}
	p2pSelf, err := ma.NewComponent("p2p", self.String())
	if err != nil {
		return nil, nil, fmt.Errorf("p2p: rendering announce addresses: %w", err)
	}
	identify := make([]ma.Multiaddr, 0, len(parsed))
	published := make([]string, 0, len(parsed))
	for _, a := range parsed {
		transport, id := peer.SplitAddr(a)
		if transport == nil || len(transport.Bytes()) == 0 {
			return nil, nil, fmt.Errorf("p2p: p2p.announce %q has no transport address -- a peer ID alone is not dialable", a.String())
		}
		switch id {
		case self:
			identify = append(identify, transport)
			published = append(published, a.String())
		case "":
			identify = append(identify, transport)
			published = append(published, transport.Encapsulate(p2pSelf).String())
		default:
			return nil, nil, fmt.Errorf("p2p: p2p.announce %q names peer %s, not this node (%s) -- announce your own address, not another node's identity", a.String(), id, self)
		}
	}
	return identify, published, nil
}

// derivedAnnounceAddrs resolves the publication list when no operator override
// exists. h.Addrs includes libp2p's observed and mapped addresses; unlike the
// configured form, those addresses already passed through the host's address
// manager and should not be guessed before construction.
func derivedAnnounceAddrs(h host.Host) ([]string, error) {
	bound := h.Addrs()
	if len(bound) == 0 {
		// A bare /p2p/<peerid> is what the renderer produces from no addresses,
		// and it is not an address: nothing can dial it. A host that bound
		// nothing has nowhere to be reached, and the document should say so by
		// omitting the field rather than by carrying a name with no transport.
		return nil, nil
	}
	addrs, err := peer.AddrInfoToP2pAddrs(&peer.AddrInfo{ID: h.ID(), Addrs: bound})
	if err != nil {
		return nil, fmt.Errorf("p2p: rendering announce addresses: %w", err)
	}
	out := make([]string, 0, len(addrs))
	for _, a := range addrs {
		out = append(out, a.String())
	}
	return out, nil
}

// logReachability drains the stateful AutoNAT event subscription for the
// host's lifetime. Its event buffer is deliberately small and bounded; this
// consumer logs only actual transitions, not duplicate observations.
func (p *Host) logReachability(ctx context.Context) {
	last := network.Reachability(-1)
	for {
		select {
		case <-ctx.Done():
			return
		case raw, ok := <-p.reachabilitySub.Out():
			if !ok {
				return
			}
			evt, ok := raw.(event.EvtLocalReachabilityChanged)
			if !ok || evt.Reachability == last {
				continue
			}
			previous := last
			last = evt.Reachability
			p.mx.P2PReachability(reachabilityMetricState(evt.Reachability))
			if previous < 0 {
				p.log.Info("p2p reachability observed", "reachability", evt.Reachability.String())
			} else {
				p.log.Info("p2p reachability changed", "from", previous.String(), "to", evt.Reachability.String())
			}
		}
	}
}

func reachabilityMetricState(state network.Reachability) string {
	switch state {
	case network.ReachabilityPrivate:
		return metrics.P2PReachabilityPrivate
	case network.ReachabilityPublic:
		return metrics.P2PReachabilityPublic
	default:
		return metrics.P2PReachabilityUnknown
	}
}

// parseMultiaddrs parses a config list, blaming the key on a bad entry.
func parseMultiaddrs(key string, in []string) ([]ma.Multiaddr, error) {
	out := make([]ma.Multiaddr, 0, len(in))
	for _, s := range in {
		a, err := ma.NewMultiaddr(s)
		if err != nil {
			return nil, fmt.Errorf("p2p: %s has an unparseable multiaddr %q: %w", key, s, err)
		}
		out = append(out, a)
	}
	return out, nil
}

// parsePeers parses p2p.peers, which are full peer multiaddrs: a static peer
// has to name the peer, since dialling one is dialling an identity at an
// address rather than an address.
func parsePeers(in []string) ([]peer.AddrInfo, error) {
	addrs, err := parseMultiaddrs("p2p.peers", in)
	if err != nil {
		return nil, err
	}
	out := make([]peer.AddrInfo, 0, len(addrs))
	for i, a := range addrs {
		ai, err := peer.AddrInfoFromP2pAddr(a)
		if err != nil {
			return nil, fmt.Errorf("p2p: p2p.peers[%d] %q is not a peer address (it needs a /p2p/<peerid> component): %w",
				i, in[i], err)
		}
		out = append(out, *ai)
	}
	return out, nil
}

// keepConnected dials ai and redials it for as long as ctx lives.
//
// A failed dial backs off; a live connection is rechecked on a fixed interval,
// which is cheap because Connectedness is a lookup rather than a probe. The
// backoff resets on every success, so a peer that flaps is dialled promptly
// each time rather than being punished for its history.
func (p *Host) keepConnected(ctx context.Context, ai peer.AddrInfo) {
	backoff := peerMinBackoff
	for {
		wait := peerCheckInterval
		if p.h.Network().Connectedness(ai.ID) == network.Connected {
			backoff = peerMinBackoff
		} else {
			// Re-added every attempt: the peerstore expires addresses, and
			// these are the only addresses this peer has.
			p.h.Peerstore().AddAddrs(ai.ID, ai.Addrs, peerstore.PermanentAddrTTL)

			dialCtx, cancel := context.WithTimeout(ctx, peerDialTimeout)
			err := p.h.Connect(dialCtx, ai)
			cancel()
			switch {
			case err == nil:
				p.log.Info("static peer connected", "peer", ai.ID)
				backoff = peerMinBackoff
			case ctx.Err() != nil:
				return
			default:
				p.log.Debug("dialling static peer", "peer", ai.ID, "err", err, "retry_in", backoff)
				wait = backoff
				backoff = min(backoff*2, peerMaxBackoff)
			}
		}

		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}
