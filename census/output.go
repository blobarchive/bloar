package census

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

// WriteJSON emits a report. Per-peer data appears only when the Inspector that
// produced report was configured with IncludePeers.
func WriteJSON(writer io.Writer, report Report, pretty bool) error {
	encoder := json.NewEncoder(writer)
	if pretty {
		encoder.SetIndent("", "  ")
	}
	return encoder.Encode(report)
}

// Prometheus returns aggregate exposition with no labels. In particular it
// never emits peer IDs, CIDs, or multiaddrs, which keeps ordinary scraping both
// bounded and private. Raw peer evidence is available only through opt-in JSON.
func (report Report) Prometheus() string {
	var output strings.Builder
	gauge := func(name, help string, value any) {
		fmt.Fprintf(&output, "# HELP %s %s\n", name, help)
		fmt.Fprintf(&output, "# TYPE %s gauge\n", name)
		fmt.Fprintf(&output, "%s %v\n", name, value)
	}
	gauge("bloar_swarm_census_observed_lower_bound", "Providers observed from this local vantage point.", report.LowerBounds.Observed)
	gauge("bloar_swarm_census_reachable_lower_bound", "Observed providers reached by a peer-targeted probe.", report.LowerBounds.Reachable)
	gauge("bloar_swarm_census_current_lower_bound", "Reachable providers that proved the current challenge.", report.LowerBounds.Current)
	gauge("bloar_swarm_census_sampled_archive_lower_bound", "Current providers that proved every configured historical sample; not a full-archive proof.", report.LowerBounds.SampledArchive)
	gauge("bloar_swarm_census_historical_challenges", "Historical challenge count in this observation.", report.HistoricalRequired)
	gauge("bloar_swarm_census_probe_attempts", "Peer-targeted probes attempted in this observation.", report.ProbeAttempts)
	gauge("bloar_swarm_census_probe_completed", "Peer-targeted probes completed in this observation.", report.ProbeCompleted)
	gauge("bloar_swarm_census_errors", "Discovery and probe errors in this observation.", report.ErrorCount)
	gauge("bloar_swarm_census_address_bytes_accepted", "Provider multiaddr wire bytes admitted under the configured bound.", report.AddressBytesAccepted)
	gauge("bloar_swarm_census_complete", "Whether the configured bounded observation completed.", boolFloat(report.Complete))
	gauge("bloar_swarm_census_discovery_complete", "Whether bounded provider discovery completed.", boolFloat(report.DiscoveryComplete))
	gauge("bloar_swarm_census_probe_complete", "Whether every admitted provider received a completed probe.", boolFloat(report.ProbeComplete))
	gauge("bloar_swarm_census_truncated", "Whether provider or address input reached a configured limit.", boolFloat(report.Truncated))
	gauge("bloar_swarm_census_timed_out", "Whether discovery or overall work exhausted a time bound.", boolFloat(report.TimedOut))
	gauge("bloar_swarm_census_canceled", "Whether the caller canceled this observation.", boolFloat(report.Canceled))
	gauge("bloar_swarm_census_discovery_failed", "Whether provider discovery failed before producing a bounded sample.", boolFloat(report.DiscoveryFailed))
	gauge("bloar_swarm_census_observed_timestamp_seconds", "Unix timestamp for the start of this local observation.", report.ObservedAt.Unix())
	gauge("bloar_swarm_census_duration_seconds", "Wall-clock duration of this observation.", float64(report.DurationMS)/1000)
	return output.String()
}

func boolFloat(value bool) int {
	if value {
		return 1
	}
	return 0
}
