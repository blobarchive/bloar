package kubo

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/ipfs/boxo/ipns"
	"github.com/libp2p/go-libp2p/core/peer"
	libp2prouting "github.com/libp2p/go-libp2p/core/routing"
)

const maxIPNSRoutingResponseBytes int64 = 16 << 10

// IPNSRecord retrieves, decodes, and authenticates Kubo's best raw IPNS record
// for name. The returned bytes remain in the wire format expected by
// routing.ValueStore and Boxo's IPNS resolver.
func (c *Client) IPNSRecord(ctx context.Context, name peer.ID) ([]byte, error) {
	const endpoint = "routing/get"
	if name == "" || len(name) > maxCIDTextBytes {
		return nil, errors.New("kubo: routing/get requires an IPNS peer name")
	}
	validatedName, err := peer.IDFromBytes([]byte(name))
	if err != nil {
		return nil, errors.New("kubo: routing/get requires an IPNS peer name")
	}
	query := jsonQuery()
	query.Set("arg", ipns.NamespacePrefix+ipns.NameFromPeer(validatedName).String())
	raw, err := c.post(ctx, endpoint, query, nil, "", "application/json", maxIPNSRoutingResponseBytes)
	if err != nil {
		return nil, asRoutingNotFound(err)
	}
	if !utf8.Valid(raw) {
		return nil, c.protocol(endpoint, "response is not valid UTF-8")
	}
	var wire struct {
		ID        *string
		Type      *int
		Responses json.RawMessage
		Extra     *string
	}
	if err := decodeStrictJSON(raw, &wire); err != nil {
		return nil, c.protocolDetail(endpoint, "decoding JSON", err)
	}
	if wire.ID == nil || wire.Type == nil || wire.Responses == nil || wire.Extra == nil {
		return nil, c.protocol(endpoint, "response is missing ID, Type, Responses, or Extra")
	}
	if *wire.ID != "" {
		return nil, c.protocol(endpoint, "value response has unexpected peer ID %q", *wire.ID)
	}
	if *wire.Type != int(libp2prouting.Value) {
		return nil, c.protocol(endpoint, "response Type is %d, want routing value type %d", *wire.Type, libp2prouting.Value)
	}
	if strings.TrimSpace(string(wire.Responses)) != "null" {
		return nil, c.protocol(endpoint, "value response has unexpected peer Responses")
	}
	encoded := *wire.Extra
	if encoded == "" {
		return nil, c.protocol(endpoint, "response has an empty Extra record")
	}
	if strings.ContainsAny(encoded, " \t\r\n") {
		return nil, c.protocol(endpoint, "Extra record contains base64 whitespace")
	}
	maxEncoded := base64.StdEncoding.EncodedLen(ipns.MaxRecordSize)
	if len(encoded) > maxEncoded {
		return nil, c.protocol(endpoint, "Extra record exceeds the %d-byte decoded limit", ipns.MaxRecordSize)
	}
	recordBytes, err := base64.StdEncoding.Strict().DecodeString(encoded)
	if err != nil {
		return nil, c.protocolDetail(endpoint, "decoding Extra base64", err)
	}
	if len(recordBytes) == 0 || len(recordBytes) > ipns.MaxRecordSize {
		return nil, c.protocol(endpoint, "decoded IPNS record size %d is outside the 1..%d-byte limit", len(recordBytes), ipns.MaxRecordSize)
	}
	record, err := ipns.UnmarshalRecord(recordBytes)
	if err != nil {
		return nil, c.protocolDetail(endpoint, "decoding IPNS record", err)
	}
	if err := ipns.ValidateWithName(record, ipns.NameFromPeer(validatedName)); err != nil {
		return nil, c.protocolDetail(endpoint, "validating IPNS record", err)
	}
	return append([]byte(nil), recordBytes...), nil
}

// IPNSValueStore returns a read-only routing.ValueStore backed by routing/get.
// It accepts only binary /ipns/<peer> routing keys. Opaque routing hints such
// as DHT quorum are accepted but Kubo chooses the single best record; Expired
// and Offline are refused because this narrow RPC cannot honor them.
func (c *Client) IPNSValueStore() libp2prouting.ValueStore {
	return &ipnsValueStore{client: c}
}

type ipnsValueStore struct {
	client *Client
}

var _ libp2prouting.ValueStore = (*ipnsValueStore)(nil)

func (*ipnsValueStore) PutValue(context.Context, string, []byte, ...libp2prouting.Option) error {
	return libp2prouting.ErrNotSupported
}

func (s *ipnsValueStore) GetValue(ctx context.Context, key string, options ...libp2prouting.Option) ([]byte, error) {
	if err := validateRoutingReadOptions(options); err != nil {
		return nil, err
	}
	name, err := routingIPNSName(key)
	if err != nil {
		return nil, err
	}
	return s.client.IPNSRecord(ctx, name.Peer())
}

func (s *ipnsValueStore) SearchValue(ctx context.Context, key string, options ...libp2prouting.Option) (<-chan []byte, error) {
	value, err := s.GetValue(ctx, key, options...)
	if errors.Is(err, libp2prouting.ErrNotFound) {
		result := make(chan []byte)
		close(result)
		return result, nil
	}
	if err != nil {
		return nil, err
	}
	result := make(chan []byte, 1)
	result <- value
	close(result)
	return result, nil
}

func routingIPNSName(key string) (ipns.Name, error) {
	if !strings.HasPrefix(key, ipns.NamespacePrefix) {
		return ipns.Name{}, libp2prouting.ErrNotSupported
	}
	name, err := ipns.NameFromRoutingKey([]byte(key))
	if err != nil {
		return ipns.Name{}, fmt.Errorf("kubo: invalid IPNS routing key: %w", err)
	}
	return name, nil
}

func validateRoutingReadOptions(options []libp2prouting.Option) error {
	for _, option := range options {
		if option == nil {
			return errors.New("kubo: nil routing option")
		}
	}
	var applied libp2prouting.Options
	if err := applied.Apply(options...); err != nil {
		return fmt.Errorf("kubo: applying routing options: %w", err)
	}
	if applied.Expired || applied.Offline {
		return libp2prouting.ErrNotSupported
	}
	// Other contains implementation hints (notably dht.Quorum from Boxo's
	// resolver). routing/get already asks Kubo for its best value, so these are
	// safe to accept and deliberately do not broaden the RPC.
	return nil
}

type routingNotFoundError struct {
	status *StatusError
}

func (e *routingNotFoundError) Error() string {
	return e.status.Error()
}

func (e *routingNotFoundError) Unwrap() []error {
	return []error{libp2prouting.ErrNotFound, e.status}
}

func asRoutingNotFound(err error) error {
	var status *StatusError
	if errors.As(err, &status) && !status.Truncated && status.routingAbsent {
		return &routingNotFoundError{status: status}
	}
	return err
}

func messageIsRoutingNotFound(message string) bool {
	return strings.TrimSpace(message) == libp2prouting.ErrNotFound.Error()
}
