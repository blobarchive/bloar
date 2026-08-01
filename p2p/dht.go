package p2p

import (
	"context"
	"errors"
	"fmt"

	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p-kad-dht/amino"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
)

// NewDHT returns the Amino DHT used for IPNS records and synthetic rendezvous
// provider keys. Bitswap itself still receives no content router: the DHT finds
// peers at rendezvous keys, and RendezvousService connects those peers to the
// existing host.
//
// # Mode
//
// ModeAuto: a node that turns out to be publicly reachable serves the DHT, and
// one that does not stays a client and only puts. Both publish, which is the
// only thing spec 8.1 asks for, and spec 11.1 explicitly allows the writer to
// be unreachable -- so the choice cannot be made from config, and is left to
// the reachability the node actually has.
//
// # Bootstrap
//
// bootstrap is the complete peer set the routing table is seeded from. Empty
// means no seeding at all, which keeps this constructor safe for private
// networks and tests. Selecting the public defaults is an explicit caller
// decision made with PublicAminoBootstrapPeers; NewDHT never silently widens a
// supplied private peer set.
func NewDHT(ctx context.Context, h *Host, bootstrap []peer.AddrInfo) (*dht.IpfsDHT, error) {
	return newDHT(ctx, h, bootstrap)
}

// NewPublicAminoDHT returns the Amino DHT profile for a directly reachable
// public edge. It mirrors go-libp2p-kad-dht's WAN defaults as one indivisible
// role contract: only public peers and addresses participate, and routing-table
// IP diversity prevents one network group from dominating a lookup.
//
// Keep this separate from NewDHT. The latter is also used by private DHTs whose
// RFC1918 peers would be intentionally rejected by this profile.
func NewPublicAminoDHT(ctx context.Context, h *Host, bootstrap []peer.AddrInfo) (*dht.IpfsDHT, error) {
	if h == nil {
		return nil, errors.New("p2p: NewPublicAminoDHT needs a host")
	}
	return newDHT(ctx, h, bootstrap,
		dht.QueryFilter(dht.PublicQueryFilter),
		dht.RoutingTableFilter(dht.PublicRoutingTableFilter),
		dht.RoutingTablePeerDiversityFilter(dht.NewRTPeerDiversityFilter(
			h.Libp2p(),
			amino.DefaultMaxPeersPerIPGroupPerCpl,
			amino.DefaultMaxPeersPerIPGroup,
		)),
		dht.AddressFilter(func(addrs []ma.Multiaddr) []ma.Multiaddr {
			return ma.FilterAddrs(addrs, manet.IsPublicAddr)
		}),
	)
}

func newDHT(ctx context.Context, h *Host, bootstrap []peer.AddrInfo, profile ...dht.Option) (*dht.IpfsDHT, error) {
	if h == nil {
		return nil, errors.New("p2p: NewDHT needs a host")
	}
	// WithoutCancel for the reason Host and Exchange have their own lifetimes:
	// this is closed in order at shutdown, not cancelled out from under the
	// publisher that is still using it.
	options := []dht.Option{
		dht.Mode(dht.ModeAuto),
		// The default validator already registers boxo's ipns.Validator for the
		// "ipns" namespace, which is what makes a put verify before it travels
		// and a get verify before it returns. Said here because relying on it
		// silently would make an upstream default load-bearing without a note.
		dht.BootstrapPeers(bootstrap...),
	}
	options = append(options, profile...)
	d, err := dht.New(context.WithoutCancel(ctx), h.Libp2p(), options...)
	if err != nil {
		return nil, fmt.Errorf("p2p: building DHT: %w", err)
	}
	if err := bootstrapDHT(context.WithoutCancel(ctx), d); err != nil {
		if closeErr := d.Close(); closeErr != nil {
			err = errors.Join(err, closeErr)
		}
		return nil, fmt.Errorf("p2p: bootstrapping DHT: %w", err)
	}
	return d, nil
}

type dhtBootstrapper interface {
	Bootstrap(context.Context) error
}

// bootstrapDHT is the narrow seam that makes the constructor's required
// Bootstrap call directly testable without starting a public routing table.
func bootstrapDHT(ctx context.Context, d dhtBootstrapper) error {
	return d.Bootstrap(ctx)
}

// PublicAminoBootstrapPeers returns a defensive copy of go-libp2p-kad-dht's
// current public Amino bootstrap set. Merely reading the set performs no DNS or
// network I/O; callers still decide whether public participation is intended.
func PublicAminoBootstrapPeers() []peer.AddrInfo {
	return MergeBootstrapPeers(dht.GetDefaultBootstrapPeerAddrInfos())
}

// MergeBootstrapPeers combines bootstrap sets by peer identity and address.
// First occurrence fixes peer order; later occurrences add previously unseen
// addresses. Inputs and their address slices are never aliased by the result.
// This lets an operator's explicit peers safely augment public defaults without
// redundant dials, while a private caller can pass only its explicit set.
func MergeBootstrapPeers(groups ...[]peer.AddrInfo) []peer.AddrInfo {
	out := make([]peer.AddrInfo, 0)
	index := make(map[peer.ID]int)
	addresses := make(map[peer.ID]map[string]struct{})
	for _, group := range groups {
		for _, info := range group {
			i, ok := index[info.ID]
			if !ok {
				i = len(out)
				index[info.ID] = i
				addresses[info.ID] = make(map[string]struct{}, len(info.Addrs))
				out = append(out, peer.AddrInfo{ID: info.ID})
			}
			for _, addr := range info.Addrs {
				key := string(addr.Bytes())
				if _, duplicate := addresses[info.ID][key]; duplicate {
					continue
				}
				addresses[info.ID][key] = struct{}{}
				out[i].Addrs = append(out[i].Addrs, addr)
			}
		}
	}
	return out
}

// ParsePeers parses peer multiaddrs, each of which must name a peer. It is
// exported for the daemon, which uses p2p.peers both as static Bitswap peers
// and as explicit Amino bootstrappers.
func ParsePeers(in []string) ([]peer.AddrInfo, error) { return parsePeers(in) }
