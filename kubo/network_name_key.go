package kubo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
)

const (
	maxKeyNameBytes        = 64
	maxMultiaddrTextBytes  = 4 << 10
	maxPublishLifetime     = 30 * 24 * time.Hour
	maxPublishTTL          = 24 * time.Hour
	swarmDirectionInbound  = 1
	swarmDirectionOutbound = 2
)

// SwarmDirection is the validated direction of one Kubo connection.
type SwarmDirection string

const (
	SwarmInbound  SwarmDirection = "inbound"
	SwarmOutbound SwarmDirection = "outbound"
)

// SwarmPeer is one open Kubo connection. Multiple addresses for one Peer ID
// remain distinct connections.
type SwarmPeer struct {
	Peer      peer.ID
	Address   ma.Multiaddr
	Direction SwarmDirection
}

// KeyInfo binds a Kubo keystore name to its IPNS peer identity.
type KeyInfo struct {
	Name string
	ID   peer.ID
}

type swarmPeerWire struct {
	Addr      string
	Peer      string
	Latency   string
	Muxer     string
	Direction *int
	Streams   []struct{ Protocol string }
	Identify  struct {
		Addresses    []string
		AgentVersion string
		ID           string
		Protocols    []string
		PublicKey    string
	}
}

type keyInfoWire struct {
	ID   string `json:"Id"`
	Name string
}

// NamePublishOptions is the deliberately narrow safe subset of name/publish.
// Publishing is online, V1-compatible, and never asks Kubo to resolve/fetch the
// already content-addressed target. Sequence nil lets Kubo allocate one.
type NamePublishOptions struct {
	Key      string
	Lifetime time.Duration
	TTL      time.Duration
	Sequence *uint64
}

// NamePublication is Kubo's validated acknowledgement of an IPNS publish.
type NamePublication struct {
	Name  peer.ID
	Value cid.Cid
}

// SwarmPeers returns the bounded set of open connections without streams,
// latency, muxer, or identify metadata.
func (c *Client) SwarmPeers(ctx context.Context) ([]SwarmPeer, error) {
	const endpoint = "swarm/peers"
	maxBytes, maxItems := c.networkCollectionLimits()
	query := jsonQuery()
	query.Set("verbose", "false")
	query.Set("streams", "false")
	query.Set("latency", "false")
	query.Set("direction", "true")
	query.Set("identify", "false")
	raw, err := c.post(ctx, endpoint, query, nil, "", "application/json", maxBytes)
	if err != nil {
		return nil, err
	}
	var envelope struct {
		Peers json.RawMessage
	}
	if err := decodeStrictJSON(raw, &envelope); err != nil {
		return nil, c.protocol(endpoint, "decoding JSON: %v", err)
	}
	if envelope.Peers == nil {
		return nil, c.protocol(endpoint, "response is missing Peers")
	}
	if strings.TrimSpace(string(envelope.Peers)) == "null" {
		return []SwarmPeer{}, nil
	}
	result := make([]SwarmPeer, 0)
	err = decodeJSONArray(envelope.Peers, maxItems, func(itemNumber int, raw json.RawMessage) error {
		i := itemNumber - 1
		var item swarmPeerWire
		if err := decodeStrictJSON(raw, &item); err != nil {
			return c.protocol(endpoint, "decoding peer %d: %v", i, err)
		}
		if item.Peer == "" || len(item.Peer) > maxCIDTextBytes {
			return c.protocol(endpoint, "peer %d has an empty or oversized Peer ID", i)
		}
		id, err := peer.Decode(item.Peer)
		if err != nil {
			return c.protocol(endpoint, "peer %d has an invalid Peer ID: %v", i, err)
		}
		if item.Addr == "" || len(item.Addr) > maxMultiaddrTextBytes {
			return c.protocol(endpoint, "peer %d has an empty or oversized address", i)
		}
		address, err := ma.NewMultiaddr(item.Addr)
		if err != nil {
			return c.protocol(endpoint, "peer %d has an invalid address: %v", i, err)
		}
		if _, last := ma.SplitLast(address); last != nil && last.Protocol().Code == ma.P_P2P {
			return c.protocol(endpoint, "peer %d address unexpectedly includes a peer ID", i)
		}
		if item.Direction == nil {
			return c.protocol(endpoint, "peer %d is missing Direction", i)
		}
		var direction SwarmDirection
		switch *item.Direction {
		case swarmDirectionInbound:
			direction = SwarmInbound
		case swarmDirectionOutbound:
			direction = SwarmOutbound
		default:
			return c.protocol(endpoint, "peer %d has invalid Direction %d", i, *item.Direction)
		}
		if item.Latency != "" || item.Muxer != "" || len(item.Streams) != 0 ||
			len(item.Identify.Addresses) != 0 || item.Identify.AgentVersion != "" || item.Identify.ID != "" ||
			len(item.Identify.Protocols) != 0 || item.Identify.PublicKey != "" {
			return c.protocol(endpoint, "peer %d includes metadata that was explicitly disabled", i)
		}
		result = append(result, SwarmPeer{Peer: id, Address: address, Direction: direction})
		return nil
	})
	if err != nil {
		var protocol *ProtocolError
		if errors.As(err, &protocol) {
			return nil, err
		}
		return nil, c.protocol(endpoint, "decoding Peers: %v", err)
	}
	return result, nil
}

// SwarmConnect asks Kubo to connect to exactly one fully qualified peer
// multiaddress and verifies the acknowledgement names that peer.
func (c *Client) SwarmConnect(ctx context.Context, address ma.Multiaddr) (peer.ID, error) {
	const endpoint = "swarm/connect"
	if address == nil {
		return "", errors.New("kubo: swarm/connect requires a peer multiaddress")
	}
	addressText := address.String()
	if addressText == "" || len(addressText) > maxMultiaddrTextBytes {
		return "", fmt.Errorf("kubo: swarm/connect address exceeds the %d-byte limit", maxMultiaddrTextBytes)
	}
	info, err := peer.AddrInfoFromP2pAddr(address)
	if err != nil || info.ID == "" {
		return "", errors.New("kubo: swarm/connect address must end in /p2p/<peer-id>")
	}
	query := jsonQuery()
	query.Set("arg", addressText)
	raw, err := c.post(ctx, endpoint, query, nil, "", "application/json", maxMetadataBytes)
	if err != nil {
		return "", err
	}
	var wire struct{ Strings []string }
	if err := decodeStrictJSON(raw, &wire); err != nil {
		return "", c.protocol(endpoint, "decoding JSON: %v", err)
	}
	want := "connect " + info.ID.String() + " success"
	if len(wire.Strings) != 1 || wire.Strings[0] != want {
		return "", c.protocol(endpoint, "response is not the exact success acknowledgement for peer %s", info.ID)
	}
	return info.ID, nil
}

// NameResolve recursively resolves one IPNS peer name to exactly one root CID.
// Paths with a suffix are rejected so callers cannot accidentally discard it.
func (c *Client) NameResolve(ctx context.Context, name peer.ID) (cid.Cid, error) {
	const endpoint = "name/resolve"
	if name == "" || len(name) > maxCIDTextBytes || name.Validate() != nil {
		return cid.Undef, errors.New("kubo: name/resolve requires an IPNS peer name")
	}
	query := jsonQuery()
	query.Set("arg", name.String())
	query.Set("recursive", "true")
	query.Set("nocache", "false")
	query.Set("dht-record-count", "16")
	query.Set("dht-timeout", "1m0s")
	query.Set("stream", "false")
	raw, err := c.post(ctx, endpoint, query, nil, "", "application/json", maxMetadataBytes)
	if err != nil {
		return cid.Undef, err
	}
	var wire struct{ Path string }
	if err := decodeStrictJSON(raw, &wire); err != nil {
		return cid.Undef, c.protocol(endpoint, "decoding JSON: %v", err)
	}
	resolved, err := exactIPFSPath(wire.Path)
	if err != nil {
		return cid.Undef, c.protocol(endpoint, "%v", err)
	}
	return resolved, nil
}

// NamePublish publishes exactly one CID using an existing named Kubo key.
func (c *Client) NamePublish(ctx context.Context, target cid.Cid, options NamePublishOptions) (NamePublication, error) {
	const endpoint = "name/publish"
	targetText, err := boundedCIDArgument(endpoint, target)
	if err != nil {
		return NamePublication{}, err
	}
	if !validKeyName(options.Key, true) {
		return NamePublication{}, errors.New("kubo: name/publish has an invalid key name")
	}
	if options.Lifetime <= 0 || options.Lifetime > maxPublishLifetime {
		return NamePublication{}, fmt.Errorf("kubo: name/publish Lifetime must be between 1ns and %s", maxPublishLifetime)
	}
	if options.TTL <= 0 || options.TTL > maxPublishTTL || options.TTL > options.Lifetime {
		return NamePublication{}, fmt.Errorf("kubo: name/publish TTL must be positive, at most %s, and no longer than Lifetime", maxPublishTTL)
	}
	path := "/ipfs/" + targetText
	query := jsonQuery()
	query.Set("arg", path)
	query.Set("key", options.Key)
	query.Set("resolve", "false")
	query.Set("lifetime", options.Lifetime.String())
	query.Set("ttl", options.TTL.String())
	query.Set("quieter", "false")
	query.Set("v1compat", "true")
	query.Set("allow-offline", "false")
	query.Set("allow-delegated", "false")
	query.Set("ipns-base", "b58mh")
	if options.Sequence != nil {
		query.Set("sequence", strconv.FormatUint(*options.Sequence, 10))
	}
	raw, err := c.post(ctx, endpoint, query, nil, "", "application/json", maxMetadataBytes)
	if err != nil {
		return NamePublication{}, err
	}
	var wire struct {
		Name  string
		Value string
	}
	if err := decodeStrictJSON(raw, &wire); err != nil {
		return NamePublication{}, c.protocol(endpoint, "decoding JSON: %v", err)
	}
	if wire.Name == "" || len(wire.Name) > maxCIDTextBytes {
		return NamePublication{}, c.protocol(endpoint, "response has an empty or oversized IPNS Name")
	}
	name, err := peer.Decode(wire.Name)
	if err != nil {
		return NamePublication{}, c.protocol(endpoint, "response has an invalid IPNS Name: %v", err)
	}
	value, err := exactIPFSPath(wire.Value)
	if err != nil {
		return NamePublication{}, c.protocol(endpoint, "%v", err)
	}
	if !value.Equals(target) {
		return NamePublication{}, c.protocol(endpoint, "response Value names CID %s, want %s", value, target)
	}
	return NamePublication{Name: name, Value: value}, nil
}

// KeyList returns the bounded named IPNS keystore inventory, including IDs.
func (c *Client) KeyList(ctx context.Context) ([]KeyInfo, error) {
	const endpoint = "key/ls"
	maxBytes, maxItems := c.networkCollectionLimits()
	query := jsonQuery()
	query.Set("l", "true")
	query.Set("ipns-base", "b58mh")
	raw, err := c.post(ctx, endpoint, query, nil, "", "application/json", maxBytes)
	if err != nil {
		return nil, err
	}
	var wire struct {
		Keys json.RawMessage
	}
	if err := decodeStrictJSON(raw, &wire); err != nil {
		return nil, c.protocol(endpoint, "decoding JSON: %v", err)
	}
	if wire.Keys == nil {
		return nil, c.protocol(endpoint, "response is missing Keys")
	}
	if strings.TrimSpace(string(wire.Keys)) == "null" {
		return []KeyInfo{}, nil
	}
	result := make([]KeyInfo, 0)
	seenNames := make(map[string]struct{})
	err = decodeJSONArray(wire.Keys, maxItems, func(itemNumber int, raw json.RawMessage) error {
		i := itemNumber - 1
		var item keyInfoWire
		if err := decodeStrictJSON(raw, &item); err != nil {
			return c.protocol(endpoint, "decoding key %d: %v", i, err)
		}
		if !validKeyName(item.Name, true) {
			return c.protocol(endpoint, "key %d has an invalid name", i)
		}
		if _, duplicate := seenNames[item.Name]; duplicate {
			return c.protocol(endpoint, "response repeats key name %q", item.Name)
		}
		if item.ID == "" || len(item.ID) > maxCIDTextBytes {
			return c.protocol(endpoint, "key %d has an empty or oversized ID", i)
		}
		id, err := peer.Decode(item.ID)
		if err != nil {
			return c.protocol(endpoint, "key %d has an invalid ID: %v", i, err)
		}
		seenNames[item.Name] = struct{}{}
		result = append(result, KeyInfo{Name: item.Name, ID: id})
		return nil
	})
	if err != nil {
		var protocol *ProtocolError
		if errors.As(err, &protocol) {
			return nil, err
		}
		return nil, c.protocol(endpoint, "decoding Keys: %v", err)
	}
	return result, nil
}

func (c *Client) networkCollectionLimits() (int64, int) {
	maxBytes := c.maxStreamBytes
	if maxBytes > DefaultMaxStreamBytes {
		maxBytes = DefaultMaxStreamBytes
	}
	maxItems := c.maxStreamItems
	if maxItems > DefaultMaxStreamItems {
		maxItems = DefaultMaxStreamItems
	}
	return maxBytes, maxItems
}

// KeyGenerate creates one Ed25519 IPNS key. RSA and caller-selected key sizes
// are intentionally absent from this online API.
func (c *Client) KeyGenerate(ctx context.Context, name string) (KeyInfo, error) {
	const endpoint = "key/gen"
	if !validKeyName(name, false) {
		return KeyInfo{}, errors.New("kubo: key/gen requires a safe non-self key name")
	}
	query := jsonQuery()
	query.Set("arg", name)
	query.Set("type", "ed25519")
	query.Set("ipns-base", "b58mh")
	raw, err := c.post(ctx, endpoint, query, nil, "", "application/json", maxMetadataBytes)
	if err != nil {
		return KeyInfo{}, err
	}
	var wire struct {
		ID   string `json:"Id"`
		Name string
	}
	if err := decodeStrictJSON(raw, &wire); err != nil {
		return KeyInfo{}, c.protocol(endpoint, "decoding JSON: %v", err)
	}
	if wire.Name != name {
		return KeyInfo{}, c.protocol(endpoint, "response names key %q, want %q", wire.Name, name)
	}
	if wire.ID == "" || len(wire.ID) > maxCIDTextBytes {
		return KeyInfo{}, c.protocol(endpoint, "response has an empty or oversized ID")
	}
	id, err := peer.Decode(wire.ID)
	if err != nil {
		return KeyInfo{}, c.protocol(endpoint, "response has an invalid ID: %v", err)
	}
	return KeyInfo{Name: wire.Name, ID: id}, nil
}

func exactIPFSPath(value string) (cid.Cid, error) {
	const prefix = "/ipfs/"
	if !strings.HasPrefix(value, prefix) {
		return cid.Undef, errors.New("response path is not an /ipfs path")
	}
	remainder := strings.TrimPrefix(value, prefix)
	if remainder == "" || len(remainder) > maxCIDTextBytes || strings.Contains(remainder, "/") {
		return cid.Undef, errors.New("response path is not exactly one root CID")
	}
	parsed, err := cid.Parse(remainder)
	if err != nil || !parsed.Defined() {
		return cid.Undef, errors.New("response path has an invalid CID")
	}
	return parsed, nil
}

func validKeyName(name string, allowSelf bool) bool {
	if name == "self" {
		return allowSelf
	}
	if name == "" || len(name) > maxKeyNameBytes {
		return false
	}
	for i := 0; i < len(name); i++ {
		ch := name[i]
		if !(ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch >= '0' && ch <= '9' || strings.ContainsRune("._-", rune(ch))) {
			return false
		}
	}
	return true
}
