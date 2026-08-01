package p2p

import (
	"strings"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"

	"github.com/blobarchive/bloar/schema"
)

func TestRelayDefaultsAreBloarOwnedAndControlPlaneOnly(t *testing.T) {
	cfg := DefaultRelayConfig()
	resolved, err := resolveRelayConfig(cfg)
	if err != nil {
		t.Fatalf("resolving defaults: %v", err)
	}
	if !resolved.serviceEnabled || !resolved.holePunchingEnabled {
		t.Fatalf("enabled defaults did not select service + hole punching: %+v", resolved)
	}
	if len(resolved.staticRelays) != 0 {
		t.Fatalf("enabled defaults selected %d AutoRelay candidates; want none", len(resolved.staticRelays))
	}
	want := struct {
		ttl, circuitDuration time.Duration
		reservations         int
		circuits             int
		buffer               int
		perPeer              int
		perIP                int
		perASN               int
		data                 int64
	}{time.Hour, 2 * time.Minute, 32, 4, 2 << 10, 1, 8, 16, 128 << 10}
	got := resolved.service
	//lint:ignore SA1019 The upstream relay resource still carries this compatibility field; pin its resolved value while we configure it.
	gotPerPeer := got.MaxReservationsPerPeer
	if got.ReservationTTL != want.ttl || got.MaxReservations != want.reservations ||
		got.MaxCircuits != want.circuits || got.BufferSize != want.buffer ||
		gotPerPeer != want.perPeer || got.MaxReservationsPerIP != want.perIP ||
		got.MaxReservationsPerASN != want.perASN || got.Limit == nil ||
		got.Limit.Duration != want.circuitDuration || got.Limit.Data != want.data {
		t.Fatalf("resolved relay defaults = %+v, limit=%+v; want %+v", got, got.Limit, want)
	}

	// The raw EIP-4844 blob consumes the entire circuit allowance. Bitswap's
	// message and CID framing necessarily makes an encoded transfer larger, so
	// a default relay circuit cannot be mistaken for a blob data path.
	if DefaultRelayCircuitDataBytes != int64(schema.BlobSize) {
		t.Fatalf("relay bytes = %d, blob bytes = %d; review the control-plane-only invariant",
			DefaultRelayCircuitDataBytes, schema.BlobSize)
	}
}

func TestRelayOptionPlanHasExplicitDisabledAndNoCandidateModes(t *testing.T) {
	opts, err := RelayOptions(RelayConfig{})
	if err != nil {
		t.Fatalf("zero config: %v", err)
	}
	if len(opts) != 0 {
		t.Fatalf("zero config produced %d libp2p options; want inert", len(opts))
	}

	// AutoRelay tuning without candidates is also inert rather than installing
	// an AutoRelay instance with no peer source (which libp2p v0.48 panics on).
	opts, err = RelayOptions(RelayConfig{AutoRelay: AutoRelayConfig{Backoff: -time.Second}})
	if err != nil {
		t.Fatalf("inactive AutoRelay tuning was validated despite no candidates: %v", err)
	}
	if len(opts) != 0 {
		t.Fatalf("no-candidate config produced %d options; want inert", len(opts))
	}

	opts, err = RelayOptions(DefaultRelayConfig())
	if err != nil {
		t.Fatalf("enabled defaults: %v", err)
	}
	var applied libp2p.Config
	if err := applied.Apply(opts...); err != nil {
		t.Fatalf("applying options: %v", err)
	}
	if !applied.Relay || !applied.RelayCustom {
		t.Error("relay transport was not pinned on")
	}
	if !applied.EnableRelayService {
		t.Error("bounded relay service is disabled")
	}
	if !applied.EnableHolePunching {
		t.Error("DCUtR is disabled")
	}
	if applied.EnableAutoRelay {
		t.Error("AutoRelay was enabled without static candidates")
	}
}

func TestRelayStaticCandidatesEnableOnlyBoundedAutoRelay(t *testing.T) {
	candidate := relayTestCandidate(t, "/ip4/127.0.0.1/tcp/4001")
	cfg := RelayConfig{StaticRelays: []peer.AddrInfo{candidate}}
	resolved, err := resolveRelayConfig(cfg)
	if err != nil {
		t.Fatalf("resolving one candidate: %v", err)
	}
	if resolved.desiredReservations != 1 {
		t.Fatalf("desired reservations = %d, want capped to one candidate", resolved.desiredReservations)
	}
	if resolved.minInterval != DefaultAutoRelayMinInterval ||
		resolved.bootDelay != DefaultAutoRelayBootDelay ||
		resolved.backoff != DefaultAutoRelayBackoff ||
		resolved.maxCandidateAge != DefaultAutoRelayMaxCandidateAge {
		t.Fatalf("AutoRelay defaults drifted: %+v", resolved)
	}

	opts, err := RelayOptions(cfg)
	if err != nil {
		t.Fatalf("building options: %v", err)
	}
	var applied libp2p.Config
	if err := applied.Apply(opts...); err != nil {
		t.Fatalf("applying options: %v", err)
	}
	if !applied.EnableAutoRelay {
		t.Fatal("static candidate did not enable AutoRelay")
	}
	if applied.EnableRelayService || applied.EnableHolePunching {
		t.Error("static candidates silently enabled service or DCUtR")
	}
	if !applied.Relay || !applied.RelayCustom {
		t.Error("AutoRelay did not explicitly enable the circuit transport")
	}
}

func TestRelayConfigRejectsUnsafeLimits(t *testing.T) {
	tests := []struct {
		name string
		cfg  RelayConfig
		want string
	}{
		{
			name: "negative reservations",
			cfg:  RelayConfig{EnableService: true, Service: RelayServiceConfig{MaxReservations: -1}},
			want: "MaxReservations",
		},
		{
			name: "subsecond circuit",
			cfg:  RelayConfig{EnableService: true, Service: RelayServiceConfig{CircuitDuration: time.Millisecond}},
			want: "whole number of seconds",
		},
		{
			name: "fractional circuit",
			cfg:  RelayConfig{EnableService: true, Service: RelayServiceConfig{CircuitDuration: 1500 * time.Millisecond}},
			want: "whole number of seconds",
		},
		{
			name: "per IP exceeds total",
			cfg:  RelayConfig{EnableService: true, Service: RelayServiceConfig{MaxReservations: 2, MaxReservationsPerIP: 3}},
			want: "MaxReservationsPerIP",
		},
		{
			name: "per ASN exceeds total",
			cfg:  RelayConfig{EnableService: true, Service: RelayServiceConfig{MaxReservations: 2, MaxReservationsPerIP: 1, MaxReservationsPerASN: 3}},
			want: "MaxReservationsPerASN",
		},
		{
			name: "too many desired relays",
			cfg: RelayConfig{
				StaticRelays: []peer.AddrInfo{relayTestCandidate(t, "/ip4/127.0.0.1/tcp/4001")},
				AutoRelay:    AutoRelayConfig{DesiredReservations: 2},
			},
			want: "exceeds the 1 static relay candidates",
		},
		{
			name: "candidate expires before source refresh",
			cfg: RelayConfig{
				StaticRelays: []peer.AddrInfo{relayTestCandidate(t, "/ip4/127.0.0.1/tcp/4001")},
				AutoRelay: AutoRelayConfig{
					MinInterval:     time.Minute,
					MaxCandidateAge: 30 * time.Second,
				},
			},
			want: "below MinInterval",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := RelayOptions(tt.cfg)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error = %v, want %q", err, tt.want)
			}
		})
	}
}

func TestRelayStaticCandidateValidationAndCopy(t *testing.T) {
	valid := relayTestCandidate(t, "/ip4/127.0.0.1/tcp/4001")
	other := relayTestCandidate(t, "/ip4/127.0.0.1/tcp/4002")
	tests := []struct {
		name string
		in   []peer.AddrInfo
		want string
	}{
		{name: "empty ID", in: []peer.AddrInfo{{Addrs: valid.Addrs}}, want: "invalid peer ID"},
		{name: "malformed ID", in: []peer.AddrInfo{{ID: peer.ID("not-a-multihash"), Addrs: valid.Addrs}}, want: "invalid peer ID"},
		{name: "no address", in: []peer.AddrInfo{{ID: valid.ID}}, want: "no direct addresses"},
		{name: "nil address", in: []peer.AddrInfo{{ID: valid.ID, Addrs: []ma.Multiaddr{nil}}}, want: "is nil"},
		{name: "circuit candidate", in: []peer.AddrInfo{{ID: valid.ID, Addrs: []ma.Multiaddr{ma.StringCast("/p2p-circuit")}}}, want: "direct transport address"},
		{name: "address carries peer ID", in: []peer.AddrInfo{{ID: valid.ID, Addrs: []ma.Multiaddr{ma.StringCast("/ip4/127.0.0.1/tcp/4001/p2p/" + valid.ID.String())}}}, want: "without /p2p"},
		{name: "duplicate peer", in: []peer.AddrInfo{valid, {ID: valid.ID, Addrs: other.Addrs}}, want: "duplicates peer"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := normalizeStaticRelays(tt.in)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error = %v, want %q", err, tt.want)
			}
		})
	}

	duplicateAddress := valid.Addrs[0]
	in := []peer.AddrInfo{{ID: valid.ID, Addrs: []ma.Multiaddr{duplicateAddress, duplicateAddress}}}
	got, err := normalizeStaticRelays(in)
	if err != nil {
		t.Fatalf("normalizing valid candidate: %v", err)
	}
	if len(got) != 1 || len(got[0].Addrs) != 1 {
		t.Fatalf("normalized candidates = %+v, want duplicate address removed", got)
	}
	in[0].ID = other.ID
	in[0].Addrs[0] = other.Addrs[0]
	if got[0].ID != valid.ID || got[0].Addrs[0].String() != duplicateAddress.String() {
		t.Fatalf("resolved candidate followed caller mutation: %+v", got[0])
	}
}

func relayTestCandidate(t *testing.T, address string) peer.AddrInfo {
	t.Helper()
	private, _, err := crypto.GenerateEd25519Key(nil)
	if err != nil {
		t.Fatalf("generating relay identity: %v", err)
	}
	id, err := peer.IDFromPrivateKey(private)
	if err != nil {
		t.Fatalf("deriving relay PeerID: %v", err)
	}
	return peer.AddrInfo{ID: id, Addrs: []ma.Multiaddr{ma.StringCast(address)}}
}
