package p2p

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"time"

	"github.com/ipfs/boxo/bitswap/client/traceability"
	"github.com/ipfs/boxo/blockstore"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/libp2p/go-libp2p"
	crypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
)

const (
	// MaximumProbeCIDs accommodates one current challenge plus the census
	// package's hard maximum of 64 historical samples.
	MaximumProbeCIDs               = 65
	MaximumProbeBytes        int64 = 64 << 20
	MaximumProbeAddrs              = 64
	MaximumProbeAddressBytes       = 64 << 10

	// A dial may hold one connection scope for every accepted address while
	// transport racing is still in flight. Keep the hard budget coherent with
	// MaximumProbeAddrs, with bounded headroom for relay and control traffic.
	probeConnectionHeadroom  = 8
	probeConnectionLimit     = MaximumProbeAddrs + probeConnectionHeadroom
	probeFileDescriptorLimit = probeConnectionLimit + 32
)

const (
	ProbePathUnknown = "unknown"
	ProbePathDirect  = "direct"
	ProbePathRelay   = "relay"
)

// ProbeLimits are finite per-candidate work ceilings. MaxBytes is the aggregate
// verified payload credited as positive evidence; Boxo independently limits
// each inbound Bitswap message to 4 MiB and the ephemeral host has fixed memory
// and stream budgets. One bounded message can therefore arrive before an
// aggregate MaxBytes rejection. Zero values select small defaults; callers may
// tighten but never exceed the exported maxima.
type ProbeLimits struct {
	MaxCIDs  int
	MaxBytes int64
}

// BlockProbe is the content-addressed result for one requested CID. Success
// means bytes with exactly this CID arrived over the isolated target-peer
// connection.
type BlockProbe struct {
	CID      cid.Cid
	Success  bool
	Bytes    int
	Duration time.Duration
	Err      error
}

// PeerProbe is one target-specific Bitswap challenge. Err is a candidate dial
// or lifecycle failure; individual content misses live on Blocks so a current
// server can still be distinguished from one that passes the sampled history.
type PeerProbe struct {
	Peer        peer.ID
	Reachable   bool
	Path        string
	DialLatency time.Duration
	Blocks      []BlockProbe
	Err         error
}

// ProbePeer proves which one peer served a bounded CID set. It creates a fresh
// no-listen libp2p host and starts client-only Bitswap with no content router
// and an empty store. Relay dialing may also connect the relay peer, so network
// isolation alone is not an attribution proof: traced Bitswap provenance must
// name provider for every successful block, otherwise the sample fails closed.
//
// The caller must provide a deadline. Provider addresses and challenge CIDs
// are untrusted and are bounded before any host or goroutine is constructed.
func ProbePeer(ctx context.Context, provider peer.AddrInfo, targets []cid.Cid, limits ProbeLimits) (PeerProbe, error) {
	result := PeerProbe{Peer: provider.ID, Path: ProbePathUnknown}
	if _, ok := ctx.Deadline(); !ok {
		return result, errors.New("p2p: ProbePeer requires a caller deadline")
	}
	maxCIDs := limits.MaxCIDs
	if maxCIDs == 0 {
		maxCIDs = 8
	}
	maxBytes := limits.MaxBytes
	if maxBytes == 0 {
		maxBytes = 16 << 20
	}
	if maxCIDs <= 0 || maxCIDs > MaximumProbeCIDs {
		return result, fmt.Errorf("p2p: probe CID limit must be between 1 and %d", MaximumProbeCIDs)
	}
	if maxBytes <= 0 || maxBytes > MaximumProbeBytes {
		return result, fmt.Errorf("p2p: probe byte limit must be between 1 and %d", MaximumProbeBytes)
	}
	if provider.ID == "" {
		return result, errors.New("p2p: probe provider has an empty PeerID")
	}
	if len(provider.Addrs) == 0 || len(provider.Addrs) > MaximumProbeAddrs {
		return result, fmt.Errorf("p2p: probe provider must have between 1 and %d addresses", MaximumProbeAddrs)
	}
	addressBytes := 0
	for _, address := range provider.Addrs {
		if address == nil {
			return result, errors.New("p2p: probe provider contains a nil address")
		}
		addressBytes += len(address.Bytes())
		if addressBytes > MaximumProbeAddressBytes {
			return result, fmt.Errorf("p2p: probe provider addresses exceed %d bytes", MaximumProbeAddressBytes)
		}
	}
	if len(targets) == 0 || len(targets) > maxCIDs {
		return result, fmt.Errorf("p2p: probe has %d CIDs, limit %d", len(targets), maxCIDs)
	}
	seen := make(map[string]struct{}, len(targets))
	for i, target := range targets {
		if !target.Defined() {
			return result, fmt.Errorf("p2p: probe CID %d is undefined", i)
		}
		key := target.KeyString()
		if _, duplicate := seen[key]; duplicate {
			return result, fmt.Errorf("p2p: probe repeats CID %s", target)
		}
		seen[key] = struct{}{}
	}

	privateKey, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		return result, fmt.Errorf("p2p: generating ephemeral probe identity: %w", err)
	}
	hostCfg := probeHostConfig()
	resourceOpts, _, ownership, err := resourceControlOptions(hostCfg, nil)
	if err != nil {
		return result, fmt.Errorf("p2p: constructing probe resource policy: %w", err)
	}
	opts := []libp2p.Option{libp2p.Identity(privateKey), libp2p.NoListenAddrs, libp2p.EnableRelay()}
	opts = append(opts, resourceOpts...)
	rawHost, err := constructLibp2pHost(opts, ownership)
	if err != nil {
		return result, fmt.Errorf("p2p: constructing ephemeral probe host: %w", err)
	}
	probeHost := &Host{h: rawHost}
	local := blockstore.NewBlockstore(dssync.MutexWrap(datastore.NewMapDatastore()))
	exchange, err := NewExchange(ctx, ExchangeConfig{
		Host: probeHost, Blocks: local, DisableServer: true, TraceBlocks: true,
		MaxQueuedWantlistEntriesPerPeer: int64(maxCIDs),
		MaxOutstandingBytesPerPeer:      min(maxBytes, 4<<20),
		TaskWorkerCount:                 2,
		EngineTaskWorkerCount:           2,
		EngineBlockstoreWorkerCount:     2,
		MaxCIDSize:                      DefaultBitswapMaxCIDSize,
	})
	if err != nil {
		_ = rawHost.Close()
		return result, fmt.Errorf("p2p: starting probe Bitswap client: %w", err)
	}
	defer func() {
		_ = exchange.Close()
		_ = rawHost.Close()
	}()

	dialStarted := time.Now()
	if err := rawHost.Connect(ctx, provider); err != nil {
		result.DialLatency = time.Since(dialStarted)
		result.Err = fmt.Errorf("dialing target peer: %w", err)
		return result, nil
	}
	result.Reachable = true
	result.DialLatency = time.Since(dialStarted)
	result.Path = probeConnectionPath(rawHost.Network().ConnsToPeer(provider.ID))

	var total int64
	for index, target := range targets {
		observation := BlockProbe{CID: target}
		started := time.Now()
		// Give every challenge a share of the remaining peer deadline. A missing
		// first/current block must not consume the entire probe and suppress all
		// historical evidence; early completions naturally donate their time to
		// later samples.
		fetchCtx := ctx
		cancelFetch := func() {}
		if deadline, ok := ctx.Deadline(); ok {
			remaining := len(targets) - index
			budget := time.Until(deadline) / time.Duration(remaining)
			if budget > 0 {
				fetchCtx, cancelFetch = context.WithTimeout(ctx, budget)
			}
		}
		block, fetchErr := exchange.NewSession(fetchCtx).GetBlock(fetchCtx, target)
		cancelFetch()
		observation.Duration = time.Since(started)
		if fetchErr != nil {
			observation.Err = fetchErr
			result.Blocks = append(result.Blocks, observation)
			if ctx.Err() != nil {
				break
			}
			continue
		}
		traced, ok := block.(traceability.Block)
		if !ok {
			observation.Err = errors.New("probe response has no Bitswap peer provenance")
			result.Blocks = append(result.Blocks, observation)
			continue
		}
		if traced.From != provider.ID {
			observation.Err = fmt.Errorf("probe response came from peer %s, not target %s", traced.From, provider.ID)
			result.Blocks = append(result.Blocks, observation)
			continue
		}
		if err := verifyProbeBlock(target, traced.Block); err != nil {
			observation.Err = err
			result.Blocks = append(result.Blocks, observation)
			continue
		}
		observation.Bytes = len(traced.RawData())
		if int64(observation.Bytes) > maxBytes-total {
			observation.Err = fmt.Errorf("probe response exceeds the %d-byte aggregate limit", maxBytes)
			result.Blocks = append(result.Blocks, observation)
			break
		}
		total += int64(observation.Bytes)
		observation.Success = true
		result.Blocks = append(result.Blocks, observation)
	}
	// DCUtR may upgrade a relay connection while challenges run. Report direct
	// if any final connection is direct; that is the actual strongest path this
	// vantage proved during the probe.
	result.Path = probeConnectionPath(rawHost.Network().ConnsToPeer(provider.ID))
	return result, nil
}

func probeHostConfig() HostConfig {
	return HostConfig{
		ConnectionManager: ConnectionManagerConfig{LowWatermark: 1, HighWatermark: 4, GracePeriod: 5 * time.Second},
		ResourceManager: ResourceManagerConfig{
			MemoryBytes: 64 << 20, FileDescriptors: probeFileDescriptorLimit,
			Connections: probeConnectionLimit, InboundConnections: 2, OutboundConnections: probeConnectionLimit,
			Streams: 128, InboundStreams: 64, OutboundStreams: 128,
			PeerConnections: MaximumProbeAddrs, PeerInboundConnections: 2, PeerOutboundConnections: MaximumProbeAddrs,
			PeerStreams: 64, PeerInboundStreams: 32, PeerOutboundStreams: 64,
			PeerMemoryBytes: 32 << 20, PeerFileDescriptors: MaximumProbeAddrs,
		},
	}
}

func verifyProbeBlock(want cid.Cid, block blocks.Block) error {
	if block == nil || !block.Cid().Equals(want) {
		return fmt.Errorf("target peer returned the wrong CID, want %s", want)
	}
	computed, err := want.Prefix().Sum(block.RawData())
	if err != nil {
		return fmt.Errorf("rehashing probe block %s: %w", want, err)
	}
	if !computed.Equals(want) {
		return fmt.Errorf("target peer returned bytes which do not reproduce CID %s", want)
	}
	return nil
}

func probeConnectionPath(connections []network.Conn) string {
	if len(connections) == 0 {
		return ProbePathUnknown
	}
	for _, connection := range connections {
		if !isCircuitAddress(connection.LocalMultiaddr()) && !isCircuitAddress(connection.RemoteMultiaddr()) {
			return ProbePathDirect
		}
	}
	return ProbePathRelay
}

func isCircuitAddress(address ma.Multiaddr) bool {
	if address == nil {
		return false
	}
	for _, protocol := range address.Protocols() {
		if protocol.Code == ma.P_CIRCUIT {
			return true
		}
	}
	return false
}
