package main

import (
	"strings"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/blobarchive/bloar/p2p"
)

func TestRelayDefaultsOnlyApplyToEmbeddedHost(t *testing.T) {
	hostless := loadString(t, limitsConfigBase)
	if hostless.P2P.Relay.configured() {
		t.Fatalf("hostless config acquired relay defaults: %+v", hostless.P2P.Relay)
	}

	hosted := loadString(t, limitsConfigBase+"p2p: {}\n")
	r := hosted.P2P.Relay
	if r.Service.Enabled == nil || !*r.Service.Enabled || r.HolePunching == nil || !*r.HolePunching {
		t.Fatalf("embedded relay/DCUtR defaults = %+v", r)
	}
	if len(r.StaticCandidates) != 0 || r.AutoRelay.configured() {
		t.Fatalf("AutoRelay activated without static candidates: %+v", r)
	}
	if r.Service.ReservationTTL != p2p.DefaultRelayReservationTTL ||
		r.Service.MaxReservations != p2p.DefaultRelayMaxReservations ||
		r.Service.MaxCircuitsPerPeer != p2p.DefaultRelayMaxCircuitsPerPeer ||
		r.Service.BufferSizeBytes != p2p.DefaultRelayBufferSizeBytes ||
		r.Service.MaxReservationsPerIP != p2p.DefaultRelayMaxReservationsPerIP ||
		r.Service.MaxReservationsPerASN != p2p.DefaultRelayMaxReservationsPerASN ||
		r.Service.CircuitDuration != p2p.DefaultRelayCircuitDuration ||
		r.Service.CircuitDataBytes != p2p.DefaultRelayCircuitDataBytes {
		t.Fatalf("relay service limits drifted: %+v", r.Service)
	}

	hostConfig, err := hosted.P2P.hostConfig(nil, nil)
	if err != nil {
		t.Fatalf("hostConfig: %v", err)
	}
	opts, err := p2p.RelayOptions(hostConfig.Relay)
	if err != nil {
		t.Fatalf("RelayOptions: %v", err)
	}
	var applied libp2p.Config
	if err := applied.Apply(opts...); err != nil {
		t.Fatalf("applying relay defaults: %v", err)
	}
	if !applied.EnableRelayService || !applied.EnableHolePunching || applied.EnableAutoRelay {
		t.Fatalf("applied relay defaults: service=%v dcutr=%v autorelay=%v",
			applied.EnableRelayService, applied.EnableHolePunching, applied.EnableAutoRelay)
	}
}

func TestRelayExplicitOptOutIsInert(t *testing.T) {
	cfg := loadString(t, limitsConfigBase+`p2p:
  relay:
    service: {enabled: false}
    hole_punching: false
`)
	hostConfig, err := cfg.P2P.hostConfig(nil, nil)
	if err != nil {
		t.Fatalf("hostConfig: %v", err)
	}
	if hostConfig.Relay.EnableService || hostConfig.Relay.EnableHolePunching || len(hostConfig.Relay.StaticRelays) != 0 {
		t.Fatalf("relay opt-out mapping = %+v", hostConfig.Relay)
	}
	opts, err := p2p.RelayOptions(hostConfig.Relay)
	if err != nil {
		t.Fatalf("RelayOptions: %v", err)
	}
	if len(opts) != 0 {
		t.Fatalf("relay opt-out produced %d libp2p options", len(opts))
	}
}

func TestRelayYAMLMapsLimitsAndGroupsCandidateAddresses(t *testing.T) {
	first := relayConfigTestPeer(t)
	second := relayConfigTestPeer(t)
	oneTCP := "/dns4/relay.example.invalid/tcp/4001/p2p/" + first.String()
	oneQUIC := "/ip4/192.0.2.10/udp/4001/quic-v1/p2p/" + first.String()
	twoTCP := "/ip4/192.0.2.11/tcp/4001/p2p/" + second.String()
	cfg := loadString(t, limitsConfigBase+`p2p:
  relay:
    service:
      reservation_ttl: 2h
      max_reservations: 20
      max_circuits_per_peer: 3
      buffer_size_bytes: 4096
      max_reservations_per_ip: 5
      max_reservations_per_asn: 10
      circuit_duration: 90s
      circuit_data_bytes: 65536
    static_candidates:
      - "`+oneTCP+`"
      - "`+oneQUIC+`"
      - "`+oneTCP+`"
      - "`+twoTCP+`"
    auto_relay:
      min_interval: 45s
      boot_delay: 10s
      backoff: 10m
      max_candidate_age: 1h
`)

	if cfg.P2P.Relay.AutoRelay.DesiredReservations != 2 {
		t.Fatalf("default desired reservations = %d, want two unique candidates", cfg.P2P.Relay.AutoRelay.DesiredReservations)
	}
	hostConfig, err := cfg.P2P.hostConfig(nil, nil)
	if err != nil {
		t.Fatalf("hostConfig: %v", err)
	}
	r := hostConfig.Relay
	if len(r.StaticRelays) != 2 || r.StaticRelays[0].ID != first || len(r.StaticRelays[0].Addrs) != 2 ||
		r.StaticRelays[1].ID != second || len(r.StaticRelays[1].Addrs) != 1 {
		t.Fatalf("grouped static relays = %+v", r.StaticRelays)
	}
	if r.Service.ReservationTTL != 2*time.Hour || r.Service.MaxReservations != 20 ||
		r.Service.MaxCircuitsPerPeer != 3 || r.Service.BufferSizeBytes != 4096 ||
		r.Service.MaxReservationsPerIP != 5 || r.Service.MaxReservationsPerASN != 10 ||
		r.Service.CircuitDuration != 90*time.Second || r.Service.CircuitDataBytes != 65536 {
		t.Fatalf("service mapping = %+v", r.Service)
	}
	if r.AutoRelay.DesiredReservations != 2 || r.AutoRelay.MinInterval != 45*time.Second ||
		r.AutoRelay.BootDelay != 10*time.Second || r.AutoRelay.Backoff != 10*time.Minute ||
		r.AutoRelay.MaxCandidateAge != time.Hour {
		t.Fatalf("AutoRelay mapping = %+v", r.AutoRelay)
	}
}

func TestRelayYAMLFailsClosed(t *testing.T) {
	id := relayConfigTestPeer(t)
	other := relayConfigTestPeer(t)
	direct := "/ip4/192.0.2.10/tcp/4001/p2p/" + id.String()
	circuit := "/ip4/192.0.2.10/tcp/4001/p2p/" + id.String() + "/p2p-circuit/p2p/" + other.String()
	tests := []struct {
		name  string
		block string
		want  string
	}{
		{name: "unknown field", block: "    data_plane_fallback: true\n", want: "field data_plane_fallback not found"},
		{name: "negative disabled service cap", block: "    service: {enabled: false, max_reservations: -1}\n    hole_punching: false\n", want: "MaxReservations"},
		{name: "AutoRelay tuning without candidates", block: "    auto_relay: {min_interval: 1m}\n", want: "static_candidates is empty"},
		{name: "candidate without transport", block: "    static_candidates: [\"/p2p/" + id.String() + "\"]\n", want: "has no direct addresses"},
		{name: "candidate with relay circuit", block: "    static_candidates: [\"" + circuit + "\"]\n", want: "direct transport address"},
		{name: "candidate surrounding whitespace", block: "    static_candidates: [\" " + direct + "\"]\n", want: "without surrounding whitespace"},
		{name: "candidates with DCUtR disabled", block: "    hole_punching: false\n    static_candidates: [\"" + direct + "\"]\n", want: "requires hole_punching"},
		{name: "too many desired reservations", block: "    static_candidates: [\"" + direct + "\"]\n    auto_relay: {desired_reservations: 2}\n", want: "exceeds the 1 static relay candidates"},
		{name: "candidate expires before refresh", block: "    static_candidates: [\"" + direct + "\"]\n    auto_relay: {min_interval: 2m, max_candidate_age: 1m}\n", want: "below MinInterval"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			yaml := limitsConfigBase + "p2p:\n  relay:\n" + test.block
			_, err := LoadConfig(writeFile(t, "config.yaml", yaml))
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("LoadConfig error = %v, want mention %q", err, test.want)
			}
		})
	}
}

func TestRelayCandidatesCountAgainstProtectedConnectionBudget(t *testing.T) {
	id := relayConfigTestPeer(t)
	direct := "/ip4/192.0.2.10/tcp/4001/p2p/" + id.String()
	_, err := LoadConfig(writeFile(t, "config.yaml", limitsConfigBase+`p2p:
  relay:
    static_candidates: ["`+direct+`"]
  connection_manager: {low_watermark: 1, high_watermark: 2}
`))
	if err == nil || !strings.Contains(err.Error(), "protected configured peers") {
		t.Fatalf("LoadConfig error = %v, want protected relay candidate budget refusal", err)
	}
}

func relayConfigTestPeer(t *testing.T) peer.ID {
	t.Helper()
	private, _, err := crypto.GenerateEd25519Key(nil)
	if err != nil {
		t.Fatalf("generating relay identity: %v", err)
	}
	id, err := peer.IDFromPrivateKey(private)
	if err != nil {
		t.Fatalf("deriving relay PeerID: %v", err)
	}
	return id
}
