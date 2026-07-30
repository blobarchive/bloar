package kubo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
	libp2prouting "github.com/libp2p/go-libp2p/core/routing"
	ma "github.com/multiformats/go-multiaddr"
)

const (
	// MaximumFindProviders is the largest provider census one RPC may request.
	MaximumFindProviders = 64
	// MaximumFindProviderEvents bounds DHT progress events as well as provider
	// results. Kubo emits both through the same QueryEvent stream.
	MaximumFindProviderEvents = 4096
	// MaximumFindProviderAddresses bounds the unique addresses retained for one
	// provider and the addresses accepted in any one QueryEvent AddrInfo.
	MaximumFindProviderAddresses = 64
	// MaximumFindProviderAddressBytes bounds all multiaddr text decoded from one
	// response, including duplicates and non-provider progress events.
	MaximumFindProviderAddressBytes int64 = 64 << 10
	// MaximumFindProviderStreamBytes is a second, operation-specific ceiling
	// below the client's general stream ceiling.
	MaximumFindProviderStreamBytes int64 = 1 << 20

	maxQueryEventResponses         = 64
	maxFindProviderPeerIDTextBytes = 256
	maxFindProviderMultiaddrBytes  = 2048
)

// FindProvidersLimits is the complete caller-selected resource budget for one
// provider-discovery RPC. Every field is required; provider discovery never
// silently becomes unbounded.
type FindProvidersLimits struct {
	// NumProviders is sent to Kubo as num-providers and bounds the unique result
	// set accepted from the response.
	NumProviders int
	// MaxEvents and MaxBytes bound Kubo's complete QueryEvent stream.
	MaxEvents int
	MaxBytes  int64
	// MaxAddressesPerProvider bounds each decoded AddrInfo and the merged unique
	// addresses retained when Kubo reports the same provider more than once.
	MaxAddressesPerProvider int
	// MaxAddressBytes bounds the total multiaddr text processed across the
	// complete stream, including duplicate and progress-event addresses.
	MaxAddressBytes int64
}

// FindProviders performs one bounded routing/findprovs query and returns a
// first-seen-ordered, PeerID-deduplicated provider set. Duplicate provider
// events merge unique multiaddrs without exceeding the caller's address bound.
//
// Kubo's network lookup is potentially slow, so ctx must carry an explicit
// caller deadline. Unlike ordinary metadata RPCs, Config.RequestTimeout is not
// applied on top of that deadline.
func (c *Client) FindProviders(ctx context.Context, target cid.Cid, limits FindProvidersLimits) ([]peer.AddrInfo, error) {
	const endpoint = "routing/findprovs"
	targetText, err := boundedCIDArgument(endpoint, target)
	if err != nil {
		return nil, err
	}
	if err := requireFindProvidersDeadline(ctx, endpoint); err != nil {
		return nil, err
	}
	if err := c.validateFindProvidersLimits(endpoint, limits); err != nil {
		return nil, err
	}

	query := jsonQuery()
	query.Set("arg", targetText)
	query.Set("num-providers", fmt.Sprintf("%d", limits.NumProviders))

	providers := make([]peer.AddrInfo, 0, limits.NumProviders)
	providerIndexes := make(map[peer.ID]int, limits.NumProviders)
	providerAddresses := make(map[peer.ID]map[string]struct{}, limits.NumProviders)
	var addressBytes int64
	validationFailed := false
	err = c.postJSONStreamContext(ctx, endpoint, query, limits.MaxBytes, limits.MaxEvents, func(item int, raw json.RawMessage) error {
		if validationFailed {
			return nil
		}
		event, eventType, err := c.decodeFindProvidersEvent(endpoint, item, raw, limits, &addressBytes)
		if err != nil {
			validationFailed = true
			return err
		}
		switch eventType {
		case libp2prouting.Provider:
			provider := event[0]
			index, duplicate := providerIndexes[provider.ID]
			if !duplicate {
				if len(providers) >= limits.NumProviders {
					validationFailed = true
					return c.protocol(endpoint, "stream item %d exceeds the requested %d unique providers", item, limits.NumProviders)
				}
				index = len(providers)
				providerIndexes[provider.ID] = index
				providerAddresses[provider.ID] = make(map[string]struct{}, len(provider.Addrs))
				providers = append(providers, peer.AddrInfo{ID: provider.ID})
			}
			seenAddresses := providerAddresses[provider.ID]
			for _, address := range provider.Addrs {
				key := string(address.Bytes())
				if _, duplicate := seenAddresses[key]; duplicate {
					continue
				}
				if len(providers[index].Addrs) >= limits.MaxAddressesPerProvider {
					validationFailed = true
					return c.protocol(endpoint, "provider %s exceeds the %d-address merged limit", provider.ID, limits.MaxAddressesPerProvider)
				}
				seenAddresses[key] = struct{}{}
				providers[index].Addrs = append(providers[index].Addrs, address)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return providers, nil
}

func (c *Client) validateFindProvidersLimits(endpoint string, limits FindProvidersLimits) error {
	if limits.NumProviders <= 0 || limits.NumProviders > MaximumFindProviders {
		return fmt.Errorf("kubo: %s requires NumProviders between 1 and %d", endpoint, MaximumFindProviders)
	}
	if limits.MaxEvents <= 0 || limits.MaxEvents > MaximumFindProviderEvents || limits.MaxEvents > c.maxStreamItems {
		maximum := MaximumFindProviderEvents
		if c.maxStreamItems < maximum {
			maximum = c.maxStreamItems
		}
		return fmt.Errorf("kubo: %s requires MaxEvents between 1 and %d", endpoint, maximum)
	}
	if limits.MaxBytes <= 0 || limits.MaxBytes > MaximumFindProviderStreamBytes || limits.MaxBytes > c.maxStreamBytes {
		maximum := MaximumFindProviderStreamBytes
		if c.maxStreamBytes < maximum {
			maximum = c.maxStreamBytes
		}
		return fmt.Errorf("kubo: %s requires MaxBytes between 1 and %d", endpoint, maximum)
	}
	if limits.MaxAddressesPerProvider <= 0 || limits.MaxAddressesPerProvider > MaximumFindProviderAddresses {
		return fmt.Errorf("kubo: %s requires MaxAddressesPerProvider between 1 and %d", endpoint, MaximumFindProviderAddresses)
	}
	if limits.MaxAddressBytes <= 0 || limits.MaxAddressBytes > MaximumFindProviderAddressBytes {
		return fmt.Errorf("kubo: %s requires MaxAddressBytes between 1 and %d", endpoint, MaximumFindProviderAddressBytes)
	}
	return nil
}

func requireFindProvidersDeadline(ctx context.Context, endpoint string) error {
	if err := ctx.Err(); err != nil {
		return context.Cause(ctx)
	}
	if _, ok := ctx.Deadline(); !ok {
		return fmt.Errorf("kubo: %s streaming network lookup requires a caller-supplied context deadline", endpoint)
	}
	return nil
}

// decodeFindProvidersEvent validates the complete fixed QueryEvent schema and
// every nested AddrInfo. It returns Responses only for a provider event; other
// valid progress events are deliberately not retained.
func (c *Client) decodeFindProvidersEvent(
	endpoint string,
	item int,
	raw json.RawMessage,
	limits FindProvidersLimits,
	addressBytes *int64,
) ([]peer.AddrInfo, libp2prouting.QueryEventType, error) {
	if !utf8.Valid(raw) {
		return nil, 0, c.protocol(endpoint, "stream item %d is not valid UTF-8", item)
	}
	var wire struct {
		ID        *string
		Type      *int
		Responses json.RawMessage
		Extra     *string
	}
	if err := decodeStrictJSON(raw, &wire); err != nil {
		return nil, 0, c.protocolDetail(endpoint, fmt.Sprintf("decoding stream item %d", item), err)
	}
	if wire.ID == nil || wire.Type == nil || wire.Responses == nil || wire.Extra == nil {
		return nil, 0, c.protocol(endpoint, "stream item %d is missing ID, Type, Responses, or Extra", item)
	}
	if len(*wire.ID) > maxFindProviderPeerIDTextBytes {
		return nil, 0, c.protocol(endpoint, "stream item %d ID exceeds the %d-byte limit", item, maxFindProviderPeerIDTextBytes)
	}
	if *wire.ID != "" {
		id, err := peer.Decode(*wire.ID)
		if err != nil || id.String() != *wire.ID {
			return nil, 0, c.protocol(endpoint, "stream item %d has an invalid or noncanonical ID", item)
		}
	}
	if len(*wire.Extra) > maxDiagnosticBytes {
		return nil, 0, c.protocol(endpoint, "stream item %d Extra exceeds the %d-byte limit", item, maxDiagnosticBytes)
	}

	eventType := libp2prouting.QueryEventType(*wire.Type)
	if eventType < libp2prouting.SendingQuery || eventType > libp2prouting.DialingPeer {
		return nil, 0, c.protocol(endpoint, "stream item %d has unsupported QueryEvent Type %d", item, *wire.Type)
	}
	responses, err := c.decodeQueryEventResponses(endpoint, item, wire.Responses, limits, addressBytes)
	if err != nil {
		return nil, 0, err
	}

	switch eventType {
	case libp2prouting.Provider:
		if *wire.ID != "" || *wire.Extra != "" {
			return nil, 0, c.protocol(endpoint, "stream item %d provider event has unexpected ID or Extra", item)
		}
		if len(responses) != 1 {
			return nil, 0, c.protocol(endpoint, "stream item %d provider event has %d Responses, want exactly 1", item, len(responses))
		}
		return responses, eventType, nil
	case libp2prouting.QueryError:
		if *wire.Extra == "" {
			return nil, 0, c.protocol(endpoint, "stream item %d query error has an empty Extra", item)
		}
		if len(responses) != 0 {
			return nil, 0, c.protocol(endpoint, "stream item %d query error has unexpected Responses", item)
		}
		// A routing failure is an incomplete provider observation, not successful
		// discovery of an empty swarm. Fail the operation even if earlier progress
		// happened; callers must not publish a complete census from a partial DHT
		// walk. Extra was bounded above before it reaches this error.
		return nil, eventType, fmt.Errorf("kubo: %s query failed: %s", endpoint, *wire.Extra)
	case libp2prouting.Value:
		return nil, 0, c.protocol(endpoint, "stream item %d contains a value event in a provider query", item)
	case libp2prouting.PeerResponse:
		if *wire.ID == "" || *wire.Extra != "" {
			return nil, 0, c.protocol(endpoint, "stream item %d peer response has an empty ID or non-empty Extra", item)
		}
	default:
		if *wire.ID == "" || *wire.Extra != "" || len(responses) != 0 {
			return nil, 0, c.protocol(endpoint, "stream item %d has invalid fields for QueryEvent Type %d", item, *wire.Type)
		}
	}
	return nil, eventType, nil
}

func (c *Client) decodeQueryEventResponses(
	endpoint string,
	item int,
	raw json.RawMessage,
	limits FindProvidersLimits,
	addressBytes *int64,
) ([]peer.AddrInfo, error) {
	if strings.TrimSpace(string(raw)) == "null" {
		return nil, nil
	}
	responses := make([]peer.AddrInfo, 0, 1)
	err := decodeJSONArray(raw, maxQueryEventResponses, func(response int, rawInfo json.RawMessage) error {
		info, err := c.decodeQueryEventAddrInfo(endpoint, item, response, rawInfo, limits, addressBytes)
		if err != nil {
			return err
		}
		responses = append(responses, info)
		return nil
	})
	if err != nil {
		var protocol *ProtocolError
		if errors.As(err, &protocol) {
			return nil, err
		}
		return nil, c.protocolDetail(endpoint, fmt.Sprintf("decoding stream item %d Responses", item), err)
	}
	return responses, nil
}

func (c *Client) decodeQueryEventAddrInfo(
	endpoint string,
	item int,
	response int,
	raw json.RawMessage,
	limits FindProvidersLimits,
	addressBytes *int64,
) (peer.AddrInfo, error) {
	var wire struct {
		ID    *string
		Addrs json.RawMessage
	}
	if err := decodeStrictJSON(raw, &wire); err != nil {
		return peer.AddrInfo{}, c.protocolDetail(endpoint, fmt.Sprintf("decoding stream item %d response %d", item, response), err)
	}
	if wire.ID == nil || wire.Addrs == nil || *wire.ID == "" {
		return peer.AddrInfo{}, c.protocol(endpoint, "stream item %d response %d is missing ID or Addrs", item, response)
	}
	if len(*wire.ID) > maxFindProviderPeerIDTextBytes {
		return peer.AddrInfo{}, c.protocol(endpoint, "stream item %d response %d ID exceeds the %d-byte limit", item, response, maxFindProviderPeerIDTextBytes)
	}
	id, err := peer.Decode(*wire.ID)
	if err != nil || id.String() != *wire.ID {
		return peer.AddrInfo{}, c.protocol(endpoint, "stream item %d response %d has an invalid or noncanonical ID", item, response)
	}
	info := peer.AddrInfo{ID: id}
	if strings.TrimSpace(string(wire.Addrs)) == "null" {
		return info, nil
	}
	seen := make(map[string]struct{})
	err = decodeJSONArray(wire.Addrs, limits.MaxAddressesPerProvider, func(address int, rawAddress json.RawMessage) error {
		var text string
		if err := decodeStrictJSON(rawAddress, &text); err != nil {
			return c.protocolDetail(endpoint, fmt.Sprintf("decoding stream item %d response %d address %d", item, response, address), err)
		}
		if text == "" || len(text) > maxFindProviderMultiaddrBytes {
			return c.protocol(endpoint, "stream item %d response %d address %d has a size outside 1..%d bytes", item, response, address, maxFindProviderMultiaddrBytes)
		}
		*addressBytes += int64(len(text))
		if *addressBytes > limits.MaxAddressBytes {
			return c.protocol(endpoint, "response multiaddr text exceeds the %d-byte limit", limits.MaxAddressBytes)
		}
		addressValue, err := ma.NewMultiaddr(text)
		if err != nil || addressValue.String() != text {
			return c.protocol(endpoint, "stream item %d response %d address %d is invalid or noncanonical", item, response, address)
		}
		key := string(addressValue.Bytes())
		if _, duplicate := seen[key]; duplicate {
			return nil
		}
		seen[key] = struct{}{}
		info.Addrs = append(info.Addrs, addressValue)
		return nil
	})
	if err != nil {
		var protocol *ProtocolError
		if errors.As(err, &protocol) {
			return peer.AddrInfo{}, err
		}
		return peer.AddrInfo{}, c.protocolDetail(endpoint, fmt.Sprintf("decoding stream item %d response %d Addrs", item, response), err)
	}
	return info, nil
}
