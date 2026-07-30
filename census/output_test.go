package census

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestPrometheusIsAggregateAndUnlabelled(t *testing.T) {
	report := Report{
		ObservedAt:         time.Unix(1234, 0).UTC(),
		DurationMS:         250,
		RendezvousCID:      "rendezvous-secret",
		CurrentCID:         "current-secret",
		HistoricalRequired: 3,
		LowerBounds:        LowerBounds{Observed: 4, Reachable: 3, Current: 2, SampledArchive: 1},
		Complete:           true,
		Peers:              []PeerReport{{PeerID: "peer-secret", Addresses: []string{"/ip4/192.0.2.1/tcp/4001"}}},
	}
	exposition := report.Prometheus()
	for _, secret := range []string{"rendezvous-secret", "current-secret", "peer-secret", "192.0.2.1", "{"} {
		if strings.Contains(exposition, secret) {
			t.Fatalf("Prometheus output exposed %q:\n%s", secret, exposition)
		}
	}
	for _, metric := range []string{
		"bloar_swarm_census_observed_lower_bound 4",
		"bloar_swarm_census_reachable_lower_bound 3",
		"bloar_swarm_census_current_lower_bound 2",
		"bloar_swarm_census_sampled_archive_lower_bound 1",
		"bloar_swarm_census_complete 1",
		"bloar_swarm_census_observed_timestamp_seconds 1234",
	} {
		if !strings.Contains(exposition, metric) {
			t.Fatalf("Prometheus output lacks %q:\n%s", metric, exposition)
		}
	}
}

func TestWriteJSONOmitsPeersUnlessReportIncludesThem(t *testing.T) {
	report := Report{Version: 1, LowerBounds: LowerBounds{Observed: 2}}
	var encoded bytes.Buffer
	if err := WriteJSON(&encoded, report, false); err != nil {
		t.Fatal(err)
	}
	var value map[string]any
	if err := json.Unmarshal(encoded.Bytes(), &value); err != nil {
		t.Fatal(err)
	}
	if _, exists := value["peers"]; exists {
		t.Fatalf("aggregate JSON contains raw peer field: %s", encoded.String())
	}

	report.Peers = []PeerReport{{PeerID: "peer-one", State: PeerSampledArchive}}
	encoded.Reset()
	if err := WriteJSON(&encoded, report, true); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(encoded.String(), "peer-one") {
		t.Fatalf("opt-in raw JSON lacks peer: %s", encoded.String())
	}
}
