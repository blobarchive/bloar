package metrics_test

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/blobarchive/bloar/metrics"
)

// scrape renders m's registry the way /metrics would.
func scrape(t *testing.T, m *metrics.Metrics) string {
	t.Helper()
	srv := httptest.NewServer(metrics.Handler(m, nil))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/metrics")
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /metrics = %d, want 200", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reading /metrics: %v", err)
	}
	return string(body)
}

// sampleValue finds one sample in a scrape by its rendered name-and-labels
// prefix, e.g. `bloar_head_synced_to{head="all"}`.
func sampleValue(t *testing.T, body, series string) (float64, bool) {
	t.Helper()
	for line := range strings.SplitSeq(body, "\n") {
		if !strings.HasPrefix(line, series+" ") {
			continue
		}
		var v float64
		if _, err := fmt.Sscanf(strings.TrimPrefix(line, series+" "), "%g", &v); err != nil {
			t.Fatalf("parsing %q: %v", line, err)
		}
		return v, true
	}
	return 0, false
}

// mustSample is sampleValue for a series the test requires to exist.
func mustSample(t *testing.T, body, series string) float64 {
	t.Helper()
	v, ok := sampleValue(t, body, series)
	if !ok {
		t.Fatalf("series %s is not in the scrape:\n%s", series, body)
	}
	return v
}

// TestScrapeMovesWhenThingsHappen is the smoke test: every counter and gauge the
// daemon publishes exists, and each one moves when the thing it counts happens.
//
// It drives the *Metrics directly rather than a whole daemon: what is being
// checked is that the instrument set is wired to its own registry and that the
// names and labels come out as documented. That the seams call these methods is
// what the seams' own tests say (server, ingest, pinning).
func TestScrapeMovesWhenThingsHappen(t *testing.T) {
	m := metrics.New()

	// Nothing has happened, but the scrape must already be a valid exposition:
	// a registry that only sprouts series once traffic arrives is one an
	// operator cannot build a dashboard against.
	fresh := scrape(t, m)
	if !strings.Contains(fresh, "bloar_ingest_blobs_total") {
		t.Fatalf("a fresh registry does not publish bloar_ingest_blobs_total:\n%s", fresh)
	}
	for _, direction := range []string{metrics.P2PDirectionInbound, metrics.P2PDirectionOutbound} {
		for _, transport := range []string{metrics.P2PTransportTCP, metrics.P2PTransportQUIC, metrics.P2PTransportRelay, metrics.P2PTransportOther} {
			series := fmt.Sprintf(`bloar_p2p_live_peers{direction=%q,transport=%q}`, direction, transport)
			if got := mustSample(t, fresh, series); got != 0 {
				t.Errorf("fresh %s = %g, want 0", series, got)
			}
		}
	}
	for _, class := range []string{metrics.BitswapPeerStatic, metrics.BitswapPeerRendezvous, metrics.BitswapPeerRelay, metrics.BitswapPeerOther} {
		series := fmt.Sprintf(`bloar_bitswap_scheduled_bytes_total{peer_class=%q}`, class)
		if got := mustSample(t, fresh, series); got != 0 {
			t.Errorf("fresh %s = %g, want 0", series, got)
		}
	}
	for _, operation := range []string{metrics.RendezvousOperationProvide, metrics.RendezvousOperationDiscover} {
		series := fmt.Sprintf(`bloar_rendezvous_active{operation=%q}`, operation)
		if got := mustSample(t, fresh, series); got != 0 {
			t.Errorf("fresh %s = %g, want 0", series, got)
		}
	}
	for _, kind := range []string{metrics.PointerKindRoot, metrics.PointerKindManifest, metrics.PointerKindDocument} {
		series := fmt.Sprintf(`bloar_pointer_current{kind=%q}`, kind)
		if got := mustSample(t, fresh, series); got != 0 {
			t.Errorf("fresh %s = %g, want 0", series, got)
		}
	}

	m.HeadInfo("all", 8626176, true, 3)
	m.RootSwap("all")
	m.RootSwap("all")
	m.Adoption("arbitrum-one")
	m.Quarantined("arbitrum-one", true)
	m.BeaconRead("all", http.StatusOK, 5*time.Millisecond)
	m.BeaconRead("all", http.StatusNotFound, time.Millisecond)
	m.BeaconRead("all", http.StatusServiceUnavailable, time.Millisecond)
	m.PublicReadAdmission(metrics.PublicReadAdmitted, 3)
	m.PublicReadAdmission(metrics.PublicReadRejectedClient, 129)
	m.Ingested(3, 3*131072)
	m.IngestReject(metrics.RejectKZG)
	m.KZGVerify(2 * time.Millisecond)
	m.StorePut(3 * time.Millisecond)
	m.UpstreamRead(131072, 20*time.Millisecond)
	m.UpstreamRead(0, 5*time.Millisecond) // an empty or not-yet-covered slot: timed, no bytes.
	m.Pins("all", "root", 1)
	m.Pins("all", "window", 12)
	m.Reconciled("all", 4, 2, 10*time.Millisecond)
	m.ReconcileError("arbitrum-one")
	m.GCActive(true)
	m.GCPhase(metrics.GCPhaseMark)
	m.GCPhaseDuration(metrics.GCPhasePrepare, 20*time.Millisecond)
	m.GCRun(true, 900, 100, 5, 30*time.Second)
	m.GCObserved(1000, 12, 3)
	m.GCPhase("")
	m.GCActive(false)
	m.StagingPins(7)
	m.StagingExpired(2)
	m.ScrubActive(true)
	m.ScrubRun(true, 1000, 131072, 45*time.Second)
	m.ScrubActive(false)
	m.BitswapFetch(true, 4096)
	m.BitswapFetch(false, 0)
	m.P2PReachability(metrics.P2PReachabilityPrivate)
	m.P2PLivePeers(metrics.P2PDirectionInbound, metrics.P2PTransportTCP, 2)
	m.BitswapScheduled(metrics.BitswapPeerStatic, 8192)
	observedAt := time.Unix(1_700_000_000, 0)
	m.RendezvousActive(metrics.RendezvousOperationProvide, true)
	m.RendezvousActive(metrics.RendezvousOperationDiscover, true)
	m.RendezvousProvide(metrics.OutcomeError, observedAt.Add(-time.Minute))
	m.RendezvousProvide(metrics.OutcomeOK, observedAt)
	m.RendezvousDiscovery(metrics.RendezvousDiscoveryAvailable, 7)
	m.PointerCurrent(metrics.PointerKindRoot, true, true)
	m.PointerProvide(metrics.PointerKindRoot, metrics.OutcomeError, observedAt.Add(-time.Minute))
	m.PointerRetry(metrics.PointerKindRoot, metrics.PointerRetryProvideError)
	m.PointerProvide(metrics.PointerKindRoot, metrics.OutcomeOK, observedAt)
	m.PointerScheduleUpdate(metrics.OutcomeOK)
	m.PointerScheduleUpdate(metrics.OutcomeError)
	m.IPNSPublicationStage(metrics.IPNSStageProvideDocument, metrics.OutcomeOK, observedAt.Add(-time.Second))
	m.IPNSPublicationStage(metrics.IPNSStagePutRecord, metrics.OutcomeOK, observedAt)
	m.EdgePublicationStage(metrics.IPNSStageProvideDocument, metrics.OutcomeOK, 2*time.Second)
	m.EdgePublicationStage(metrics.IPNSStagePutRecord, metrics.EdgePublicationOutcomeTimeout, 90*time.Second)
	m.EdgePublicationWait(metrics.EdgePublicationOperationPublish, metrics.OutcomeOK, 250*time.Millisecond)
	m.EdgePublicationTransaction(
		metrics.EdgePublicationOperationPublish,
		metrics.EdgePublicationStagePutRecord,
		metrics.EdgePublicationOutcomeTimeout,
		90*time.Second,
	)
	m.EdgeDHTRoutingTablePeers(17, observedAt)
	m.EdgeDHTQueryEvent(
		metrics.IPNSStageProvideDocument,
		metrics.EdgeDHTQueryEventSendingQuery,
		observedAt.Add(-time.Second),
	)
	m.EdgeDHTQueryEvent(metrics.IPNSStagePutRecord, metrics.EdgeDHTQueryEventPeerResponse, observedAt)
	m.FollowPoll(metrics.ChannelHTTPS, metrics.OutcomeOK)
	m.FollowPoll(metrics.ChannelIPNS, metrics.OutcomeError)
	m.FollowRefusal(metrics.RefusalSyncedToFloor)
	m.FollowRefusal(metrics.RefusalQuarantined)
	m.FollowRefusal(metrics.RefusalHandoffBlocked)
	m.FollowSyncedToFloorLag("all", 20)
	m.IndexSegment("all", metrics.IndexSegmentOpen, metrics.IndexSegmentWarningBytes, 341, 2416)
	m.IndexSegment("all", metrics.IndexSegmentSealed, 950534, 3873, 12150)
	m.IndexSegment("all", metrics.IndexSegmentSealed, 622702, 1337, 8070)
	m.IndexApply("all", 1048577)
	m.IndexNodeLimitRefusal("all", metrics.IndexNodeSegment)

	body := scrape(t, m)
	for _, want := range []struct {
		series string
		value  float64
	}{
		{`bloar_head_synced_to{head="all"}`, 8626176},
		{`bloar_head_dir_depth{head="all"}`, 3},
		{`bloar_head_covered{head="all"}`, 1},
		{`bloar_head_root_swaps_total{head="all"}`, 2},
		{`bloar_head_adoptions_total{head="arbitrum-one"}`, 1},
		{`bloar_head_quarantined{head="arbitrum-one"}`, 1},
		// The status class, not the code: 404 and 200 are different series,
		// 404 and 410 would not be.
		{`bloar_beacon_reads_total{head="all",status="2xx"}`, 1},
		{`bloar_beacon_reads_total{head="all",status="4xx"}`, 1},
		{`bloar_beacon_reads_total{head="all",status="5xx"}`, 1},
		{`bloar_beacon_read_duration_seconds_count{head="all"}`, 3},
		{`bloar_public_read_admissions_total{outcome="admitted"}`, 1},
		{`bloar_public_read_admission_cost_total{outcome="admitted"}`, 3},
		{`bloar_public_read_admissions_total{outcome="rejected_client"}`, 1},
		{`bloar_public_read_admission_cost_total{outcome="rejected_client"}`, 129},
		{`bloar_ingest_blobs_total`, 3},
		{`bloar_ingest_bytes_total`, 3 * 131072},
		{`bloar_ingest_rejects_total{reason="kzg"}`, 1},
		{`bloar_ingest_kzg_verify_duration_seconds_count`, 1},
		{`bloar_store_put_duration_seconds_count`, 1},
		// Both reads are timed; only the one with blobs adds bytes.
		{`bloar_upstream_read_duration_seconds_count`, 2},
		{`bloar_upstream_read_bytes_total`, 131072},
		{`bloar_pins{head="all",purpose="root"}`, 1},
		{`bloar_pins{head="all",purpose="window"}`, 12},
		{`bloar_pins_added_total{head="all"}`, 4},
		{`bloar_pins_removed_total{head="all"}`, 2},
		{`bloar_pin_reconcile_duration_seconds_count{head="all"}`, 1},
		{`bloar_pin_reconcile_errors_total{head="arbitrum-one"}`, 1},
		{`bloar_gc_runs_total{outcome="ok"}`, 1},
		{`bloar_gc_active`, 0},
		{`bloar_gc_phase_active{phase="mark"}`, 0},
		{`bloar_gc_phase_duration_seconds_count{phase="prepare"}`, 1},
		{`bloar_gc_marked_blocks`, 900},
		{`bloar_gc_scanned_blocks`, 1000},
		{`bloar_gc_protected_blocks`, 12},
		{`bloar_gc_protected_skips_total`, 3},
		{`bloar_gc_swept_blocks_total`, 100},
		{`bloar_gc_refetched_blocks_total`, 5},
		{`bloar_gc_duration_seconds_count`, 1},
		{`bloar_staging_pins`, 7},
		{`bloar_staging_expired_total`, 2},
		{`bloar_scrub_runs_total{outcome="ok"}`, 1},
		{`bloar_scrub_active`, 0},
		{`bloar_scrub_scanned_blocks`, 1000},
		{`bloar_scrub_validated_bytes`, 131072},
		{`bloar_scrub_duration_seconds_count`, 1},
		{`bloar_p2p_reachability{state="unknown"}`, 0},
		{`bloar_p2p_reachability{state="private"}`, 1},
		{`bloar_p2p_reachability{state="public"}`, 0},
		{`bloar_p2p_live_peers{direction="inbound",transport="tcp"}`, 2},
		{`bloar_bitswap_fetches_total{outcome="ok"}`, 1},
		{`bloar_bitswap_fetches_total{outcome="error"}`, 1},
		{`bloar_bitswap_fetched_bytes_total`, 4096},
		{`bloar_bitswap_scheduled_bytes_total{peer_class="static"}`, 8192},
		{`bloar_rendezvous_active{operation="provide"}`, 1},
		{`bloar_rendezvous_active{operation="discover"}`, 1},
		{`bloar_rendezvous_provides_total{outcome="ok"}`, 1},
		{`bloar_rendezvous_provides_total{outcome="error"}`, 1},
		{`bloar_rendezvous_provide_last_success_timestamp_seconds`, float64(observedAt.Unix())},
		{`bloar_rendezvous_discovery_rounds_total{outcome="available"}`, 1},
		{`bloar_rendezvous_observed_provider_samples`, 7},
		{`bloar_pointer_current{kind="root"}`, 1},
		{`bloar_pointer_provides_total{kind="root",outcome="ok"}`, 1},
		{`bloar_pointer_provides_total{kind="root",outcome="error"}`, 1},
		{`bloar_pointer_retries_total{kind="root",reason="provide_error"}`, 1},
		{`bloar_pointer_provide_last_success_timestamp_seconds{kind="root"}`, float64(observedAt.Unix())},
		{`bloar_pointer_schedule_updates_total{outcome="ok"}`, 1},
		{`bloar_pointer_schedule_updates_total{outcome="error"}`, 1},
		{`bloar_ipns_publication_stage_total{outcome="ok",stage="provide_document"}`, 1},
		{`bloar_ipns_publication_stage_total{outcome="ok",stage="put_record"}`, 1},
		{`bloar_ipns_publication_last_success_timestamp_seconds`, float64(observedAt.Unix())},
		{`bloar_edge_publication_stage_duration_seconds_count{outcome="ok",stage="provide_document"}`, 1},
		{`bloar_edge_publication_stage_duration_seconds_count{outcome="timeout",stage="put_record"}`, 1},
		{`bloar_edge_publication_transactions_total{operation="publish",outcome="timeout",stage="put_record"}`, 1},
		{`bloar_edge_publication_transaction_duration_seconds_count{operation="publish",outcome="timeout"}`, 1},
		{`bloar_edge_publication_wait_duration_seconds_count{operation="publish",outcome="ok"}`, 1},
		{`bloar_edge_dht_routing_table_peers`, 17},
		{`bloar_edge_dht_routing_table_sample_timestamp_seconds`, float64(observedAt.Unix())},
		{`bloar_edge_dht_query_events_total{event="sending_query",stage="provide_document"}`, 1},
		{`bloar_edge_dht_query_events_total{event="peer_response",stage="put_record"}`, 1},
		{`bloar_edge_dht_query_events_total{event="value_rpc",stage="put_record"}`, 0},
		{`bloar_edge_dht_query_event_last_timestamp_seconds{event="sending_query",stage="provide_document"}`, float64(observedAt.Add(-time.Second).Unix())},
		{`bloar_edge_dht_query_event_last_timestamp_seconds{event="peer_response",stage="put_record"}`, float64(observedAt.Unix())},
		{`bloar_edge_dht_query_event_last_timestamp_seconds{event="value_rpc",stage="put_record"}`, 0},
		{`bloar_follow_polls_total{channel="https",outcome="ok"}`, 1},
		{`bloar_follow_polls_total{channel="ipns",outcome="error"}`, 1},
		{`bloar_follow_refusals_total{reason="synced_to_floor"}`, 1},
		{`bloar_follow_refusals_total{reason="quarantined"}`, 1},
		{`bloar_follow_refusals_total{reason="handoff_blocked"}`, 1},
		{`bloar_follow_synced_to_floor_lag{head="all"}`, 20},
		{`bloar_index_segment_encoded_bytes{head="all",state="open"}`, metrics.IndexSegmentWarningBytes},
		{`bloar_index_segment_rows{head="all",state="open"}`, 341},
		{`bloar_index_segment_refs{head="all",state="open"}`, 2416},
		{`bloar_index_segment_encoded_bytes{head="all",state="sealed"}`, 622702},
		{`bloar_index_segment_rows{head="all",state="sealed"}`, 1337},
		{`bloar_index_segment_refs{head="all",state="sealed"}`, 8070},
		{`bloar_index_segment_sealed_encoded_bytes_count{head="all"}`, 2},
		{`bloar_index_segment_sealed_encoded_bytes_sum{head="all"}`, 1573236},
		{`bloar_index_segment_sealed_max_encoded_bytes{head="all"}`, 950534},
		{`bloar_index_apply_encoded_bytes_total{head="all"}`, 1048577},
		{`bloar_index_node_limit_refusals_total{head="all",node="segment"}`, 1},
	} {
		if got := mustSample(t, body, want.series); got != want.value {
			t.Errorf("%s = %g, want %g", want.series, got, want.value)
		}
	}
	if !strings.Contains(body, "# HELP bloar_bitswap_scheduled_bytes_total Raw block payload bytes scheduled") ||
		!strings.Contains(body, "attempted payload, not delivery confirmation") {
		t.Errorf("Bitswap scheduled-byte HELP does not state its pre-send semantics:\n%s", body)
	}

	// The last-success gauge is a timestamp, not a fixed count, so it is checked
	// for being stamped rather than for a value: a successful run set it, and an
	// alert watches it stop moving.
	if v, ok := sampleValue(t, body, `bloar_gc_last_success_timestamp_seconds`); !ok || v <= 0 {
		t.Errorf("bloar_gc_last_success_timestamp_seconds = %g (present %t), want a positive timestamp after a successful run", v, ok)
	}
	if v, ok := sampleValue(t, body, `bloar_scrub_last_success_timestamp_seconds`); !ok || v <= 0 {
		t.Errorf("bloar_scrub_last_success_timestamp_seconds = %g (present %t), want a positive timestamp after a successful scrub", v, ok)
	}
}

// TestHeadNotCoveredPublishesNoSyncedTo checks the one gauge that is absent
// rather than zero.
//
// An empty head has no synced_to (spec 3.1 makes the field nullable), and 0 is a
// slot. Publishing 0 would make a lag alert on an empty head fire as though it
// were 8.6 million slots behind, so the covered gauge is what says whether
// synced_to means anything.
func TestHeadNotCoveredPublishesNoSyncedTo(t *testing.T) {
	m := metrics.New()
	m.HeadInfo("all", 0, false, 0)

	body := scrape(t, m)
	if v, ok := sampleValue(t, body, `bloar_head_synced_to{head="all"}`); ok {
		t.Errorf("an empty head published synced_to = %g; it has none, and 0 is a slot", v)
	}
	if got := mustSample(t, body, `bloar_head_covered{head="all"}`); got != 0 {
		t.Errorf(`bloar_head_covered{head="all"} = %g, want 0`, got)
	}
}

// TestNilMetricsIsTheDisabledState is the contract every seam relies on: metrics
// are off by default (server.metrics_listen is empty), which means every
// instrumented call site is invoked on a nil *Metrics on every ordinary daemon.
func TestNilMetricsIsTheDisabledState(t *testing.T) {
	var m *metrics.Metrics // exactly what setupMetrics returns when disabled.

	if m.Registry() != nil {
		t.Error("a nil Metrics has a registry; there would be nothing to serve it from")
	}
	// Every method, because a single one that panics is a nil-dereference in the
	// ingest or read path of every default-configured node.
	m.HeadInfo("all", 1, true, 1)
	m.RootSwap("all")
	m.Adoption("all")
	m.Quarantined("all", true)
	m.BeaconRead("all", 200, time.Millisecond)
	m.PublicReadAdmission(metrics.PublicReadAdmitted, 1)
	m.P2PReachability(metrics.P2PReachabilityUnknown)
	m.P2PLivePeers(metrics.P2PDirectionInbound, metrics.P2PTransportTCP, 1)
	m.Ingested(1, 131072)
	m.IngestReject(metrics.RejectKZG)
	m.KZGVerify(time.Millisecond)
	m.StorePut(time.Millisecond)
	m.UpstreamRead(131072, time.Millisecond)
	m.Pins("all", "root", 1)
	m.Reconciled("all", 1, 0, time.Millisecond)
	m.ReconcileError("all")
	m.GCRun(true, 1, 0, 0, time.Second)
	m.GCActive(true)
	m.GCPhase(metrics.GCPhaseMark)
	m.GCPhaseDuration(metrics.GCPhaseMark, time.Second)
	m.GCObserved(1, 1, 1)
	m.StagingPins(1)
	m.StagingExpired(1)
	m.ScrubActive(true)
	m.ScrubRun(true, 1, 1, time.Second)
	m.BitswapFetch(true, 1)
	m.BitswapScheduled(metrics.BitswapPeerStatic, 1)
	m.RendezvousActive(metrics.RendezvousOperationProvide, true)
	m.RendezvousProvide(metrics.OutcomeOK, time.Now())
	m.RendezvousDiscovery(metrics.RendezvousDiscoveryAvailable, 1)
	m.PointerCurrent(metrics.PointerKindRoot, true, true)
	m.PointerProvide(metrics.PointerKindRoot, metrics.OutcomeOK, time.Now())
	m.PointerRetry(metrics.PointerKindRoot, metrics.PointerRetryProvideError)
	m.IPNSPublicationStage(metrics.IPNSStageProvideDocument, metrics.OutcomeOK, time.Now())
	m.IPNSPublicationStage(metrics.IPNSStagePutRecord, metrics.OutcomeOK, time.Now())
	m.EdgePublicationTransaction(
		metrics.EdgePublicationOperationPublish,
		metrics.EdgePublicationStageComplete,
		metrics.OutcomeOK,
		time.Second,
	)
	m.EdgePublicationWait(metrics.EdgePublicationOperationPublish, metrics.OutcomeOK, time.Millisecond)
	m.EdgeDHTRoutingTablePeers(1, time.Now())
	m.EdgeDHTQueryEvent(metrics.IPNSStagePutRecord, metrics.EdgeDHTQueryEventSendingQuery, time.Now())
	m.EdgeDHTLookupTermination(metrics.IPNSStagePutRecord, metrics.EdgeDHTLookupTerminationCompleted, 1, time.Now())
	m.FollowPoll(metrics.ChannelHTTPS, metrics.OutcomeOK)
	m.FollowRefusal(metrics.RefusalSyncedToFloor)
	m.FollowSyncedToFloorLag("all", 1)
	m.IndexSegment("all", metrics.IndexSegmentOpen, 1, 1, 1)
	m.IndexApply("all", 1)
	m.IndexNodeLimitRefusal("all", metrics.IndexNodeSegment)
	m.ConfigureFollowSourceMetrics(map[string][]string{"all": {"writer-a", "writer-b"}})
	m.FollowSourceAvailable("writer-a", true)
	m.FollowSourceHeadClaim("all", "writer-a", 1, true)
	m.FollowSourceSelected("all", "writer-a")
	m.ConfigureFollowConflictMetrics(map[string][]string{"all": {"writer-a", "writer-b"}})
	m.FollowConflictActive("all", true)
	m.FollowConflictCreated("all", "writer-a")
	m.FollowIncomparableActive("all", true)
	m.FollowIncomparableObserved("all")
	m.MustRegister(nil)
}

func TestIndexMetricLabelsStayClosed(t *testing.T) {
	m := metrics.New()
	m.IndexSegment("all", "bafy-unbounded-state", 100, 2, 3)
	m.IndexNodeLimitRefusal("all", "bafy-unbounded-node")
	m.IndexSegment("all", metrics.IndexSegmentOpen, 100, 2, 3)
	m.IndexSegment("all", metrics.IndexSegmentOpen, -1, -1, -1)
	m.IndexSegment("all", metrics.IndexSegmentSealed, 90, 1, 2)
	m.IndexNodeLimitRefusal("all", metrics.IndexNodeSegment)

	body := scrape(t, m)
	if strings.Contains(body, "bafy-unbounded") {
		t.Fatalf("an unbounded Segment state or node kind reached metric labels:\n%s", body)
	}
	if got := mustSample(t, body, `bloar_index_segment_encoded_bytes{head="all",state="open"}`); got != 100 {
		t.Fatalf("negative Segment measurement changed the gauge: %g", got)
	}
	assertExactMetricLabels(t, m, "bloar_index_segment_encoded_bytes", "head", "state")
	assertExactMetricLabels(t, m, "bloar_index_segment_rows", "head", "state")
	assertExactMetricLabels(t, m, "bloar_index_segment_refs", "head", "state")
	assertExactMetricLabels(t, m, "bloar_index_segment_sealed_encoded_bytes", "head")
	assertExactMetricLabels(t, m, "bloar_index_segment_sealed_max_encoded_bytes", "head")
	assertExactMetricLabels(t, m, "bloar_index_node_limit_refusals_total", "head", "node")
}

func TestP2PMetricLabelsStayClosed(t *testing.T) {
	m := metrics.New()
	m.P2PLivePeers("peer-id-as-direction", metrics.P2PTransportTCP, 1)
	m.P2PLivePeers(metrics.P2PDirectionInbound, "/ip4/198.51.100.1/tcp/4001", 1)
	m.P2PLivePeers(metrics.P2PDirectionInbound, metrics.P2PTransportTCP, -1)
	m.BitswapScheduled("12D3KooWunboundedPeerID", 100)
	m.BitswapScheduled(metrics.BitswapPeerOther, -1)
	m.RendezvousActive("untrusted-operation", true)
	m.RendezvousProvide("untrusted-outcome", time.Now())
	m.RendezvousDiscovery("untrusted-outcome", 4)
	m.RendezvousDiscovery(metrics.RendezvousDiscoveryEmpty, -1)
	m.PointerCurrent("bafy-untrusted-kind", true, true)
	m.PointerProvide(metrics.PointerKindRoot, "bafy-untrusted-outcome", time.Now())
	m.PointerSchedule("bafy-untrusted-kind", true, time.Now())
	m.PointerProvideOutcome(metrics.PointerKindRoot, "bafy-untrusted-outcome")
	m.PointerRetry(metrics.PointerKindRoot, "12D3Koo-untrusted-reason")
	m.PointerScheduleUpdate("bafy-untrusted-outcome")
	m.IPNSPublicationStage("bafy-untrusted-stage", metrics.OutcomeOK, time.Now())
	m.IPNSPublicationStage(metrics.IPNSStagePutRecord, "bafy-untrusted-outcome", time.Now())
	m.EdgePublicationStage("bafy-untrusted-stage", metrics.OutcomeOK, time.Second)
	m.EdgePublicationStage(metrics.IPNSStagePutRecord, "bafy-untrusted-outcome", time.Second)
	m.EdgePublicationStage(metrics.IPNSStagePutRecord, metrics.OutcomeOK, -time.Second)
	m.EdgePublicationTransaction(
		"bafy-untrusted-operation",
		metrics.EdgePublicationStageComplete,
		metrics.OutcomeOK,
		time.Second,
	)
	m.EdgePublicationTransaction(
		metrics.EdgePublicationOperationPublish,
		"bafy-untrusted-stage",
		metrics.OutcomeOK,
		time.Second,
	)
	m.EdgePublicationTransaction(
		metrics.EdgePublicationOperationPublish,
		metrics.EdgePublicationStageComplete,
		"bafy-untrusted-outcome",
		time.Second,
	)
	m.EdgePublicationWait("bafy-untrusted-operation", metrics.OutcomeOK, time.Second)
	m.EdgePublicationWait(metrics.EdgePublicationOperationPublish, "bafy-untrusted-outcome", time.Second)
	m.EdgeDHTRoutingTablePeers(-1, time.Now())
	m.EdgeDHTQueryEvent("bafy-untrusted-stage", metrics.EdgeDHTQueryEventSendingQuery, time.Now())
	m.EdgeDHTQueryEvent(metrics.IPNSStagePutRecord, "12D3Koo-untrusted-event", time.Now())
	m.EdgeDHTLookupTermination(
		"bafy-untrusted-stage",
		metrics.EdgeDHTLookupTerminationCompleted,
		1,
		time.Now(),
	)
	m.EdgeDHTLookupTermination(
		metrics.IPNSStagePutRecord,
		"12D3Koo-untrusted-reason",
		1,
		time.Now(),
	)
	m.EdgeDHTLookupTermination(
		metrics.IPNSStagePutRecord,
		metrics.EdgeDHTLookupTerminationCompleted,
		-1,
		time.Now(),
	)

	body := scrape(t, m)
	for _, unbounded := range []string{
		"peer-id-as-direction",
		"198.51.100.1",
		"12D3KooWunboundedPeerID",
		"untrusted-operation",
		"untrusted-outcome",
		"bafy-untrusted-kind",
		"12D3Koo-untrusted-reason",
		"12D3Koo-untrusted-event",
		"bafy-untrusted-stage",
	} {
		if strings.Contains(body, unbounded) {
			t.Fatalf("unbounded value %q reached a metric label:\n%s", unbounded, body)
		}
	}
	if got := mustSample(t, body, `bloar_p2p_live_peers{direction="inbound",transport="tcp"}`); got != 0 {
		t.Fatalf("negative live-peer count changed the gauge: %g", got)
	}
	if got := mustSample(t, body, `bloar_bitswap_scheduled_bytes_total{peer_class="other"}`); got != 0 {
		t.Fatalf("negative scheduled bytes changed the counter: %g", got)
	}
	if got := mustSample(t, body, `bloar_edge_dht_lookup_terminations_total{reason="completed",stage="put_record"}`); got != 0 {
		t.Fatalf("negative waiting snapshot changed the termination counter: %g", got)
	}

	// Assert the collector schema, not just the absence of the example IDs
	// above. A PeerID label with an empty value would still be a cardinality bug.
	assertExactMetricLabels(t, m, "bloar_p2p_live_peers", "direction", "transport")
	assertExactMetricLabels(t, m, "bloar_bitswap_scheduled_bytes_total", "peer_class")
	assertExactMetricLabels(t, m, "bloar_rendezvous_active", "operation")
	assertExactMetricLabels(t, m, "bloar_rendezvous_provides_total", "outcome")
	assertExactMetricLabels(t, m, "bloar_rendezvous_discovery_rounds_total", "outcome")
	assertExactMetricLabels(t, m, "bloar_pointer_current", "kind")
	assertExactMetricLabels(t, m, "bloar_pointer_provides_total", "kind", "outcome")
	assertExactMetricLabels(t, m, "bloar_pointer_retries_total", "kind", "reason")
	assertExactMetricLabels(t, m, "bloar_pointer_provide_last_success_timestamp_seconds", "kind")
	assertExactMetricLabels(t, m, "bloar_pointer_schedule_updates_total", "outcome")
	assertExactMetricLabels(t, m, "bloar_edge_publication_transactions_total", "operation", "outcome", "stage")
	assertExactMetricLabels(t, m, "bloar_edge_publication_transaction_duration_seconds", "operation", "outcome")
	assertExactMetricLabels(t, m, "bloar_edge_publication_wait_duration_seconds", "operation", "outcome")
	assertExactMetricLabels(t, m, "bloar_edge_dht_query_events_total", "event", "stage")
	assertExactMetricLabels(t, m, "bloar_edge_dht_query_event_last_timestamp_seconds", "event", "stage")
	assertExactMetricLabels(t, m, "bloar_edge_dht_lookup_terminations_total", "reason", "stage")
	assertExactMetricLabels(t, m, "bloar_edge_dht_lookup_termination_last_timestamp_seconds", "reason", "stage")
	assertExactMetricLabels(t, m, "bloar_edge_dht_lookup_waiting_at_last_termination", "reason", "stage")
}

func TestEdgeDHTLookupTerminationMetricsMaterializeClosedReasons(t *testing.T) {
	m := metrics.New()
	reasons := []string{
		metrics.EdgeDHTLookupTerminationStopped,
		metrics.EdgeDHTLookupTerminationCancelled,
		metrics.EdgeDHTLookupTerminationStarvation,
		metrics.EdgeDHTLookupTerminationCompleted,
	}
	stages := []string{metrics.IPNSStageProvideDocument, metrics.IPNSStagePutRecord}

	fresh := scrape(t, m)
	for _, stage := range stages {
		for _, reason := range reasons {
			labels := fmt.Sprintf(`{reason=%q,stage=%q}`, reason, stage)
			for _, name := range []string{
				"bloar_edge_dht_lookup_terminations_total",
				"bloar_edge_dht_lookup_termination_last_timestamp_seconds",
				"bloar_edge_dht_lookup_waiting_at_last_termination",
			} {
				if got := mustSample(t, fresh, name+labels); got != 0 {
					t.Errorf("fresh %s%s = %g, want 0", name, labels, got)
				}
			}
		}
	}

	observedAt := time.Unix(1_700_000_500, 0)
	for i, reason := range reasons {
		m.EdgeDHTLookupTermination(metrics.IPNSStagePutRecord, reason, i+1, observedAt.Add(time.Duration(i)*time.Second))
	}
	body := scrape(t, m)
	for i, reason := range reasons {
		labels := fmt.Sprintf(`{reason=%q,stage=%q}`, reason, metrics.IPNSStagePutRecord)
		if got := mustSample(t, body, "bloar_edge_dht_lookup_terminations_total"+labels); got != 1 {
			t.Errorf("termination count %s = %g, want 1", labels, got)
		}
		if got := mustSample(t, body, "bloar_edge_dht_lookup_termination_last_timestamp_seconds"+labels); got != float64(observedAt.Add(time.Duration(i)*time.Second).Unix()) {
			t.Errorf("termination timestamp %s = %g", labels, got)
		}
		if got := mustSample(t, body, "bloar_edge_dht_lookup_waiting_at_last_termination"+labels); got != float64(i+1) {
			t.Errorf("waiting snapshot %s = %g, want %d", labels, got, i+1)
		}
	}
}

func TestNewInitializesClosedFollowRefusalSeries(t *testing.T) {
	m := metrics.New()
	body := scrape(t, m)
	for _, reason := range []string{
		metrics.RefusalSyncedToFloor,
		metrics.RefusalQuarantined,
		metrics.RefusalManifestAncestry,
		metrics.RefusalCoverageMismatch,
		metrics.RefusalUpdatedAtFloor,
		metrics.RefusalIPNSSeqFloor,
		metrics.RefusalHandoffBlocked,
	} {
		series := `bloar_follow_refusals_total{reason="` + reason + `"}`
		if got := mustSample(t, body, series); got != 0 {
			t.Errorf("%s = %g, want 0", series, got)
		}
	}
}

func TestFollowerAdmissionAndSyncMetricsKeepSuccessSemanticsDistinct(t *testing.T) {
	m := metrics.New()
	admissionAt := time.Unix(1_700_000_100, 0)
	syncAt := time.Unix(1_700_000_200, 0)

	m.FollowAdmission(metrics.OutcomeError, 3*time.Second, admissionAt.Add(-time.Minute))
	if got := mustSample(t, scrape(t, m), `bloar_follow_admission_last_success_timestamp_seconds`); got != 0 {
		t.Fatalf("failed admission advanced last success to %g", got)
	}
	m.FollowAdmission(metrics.OutcomeOK, 250*time.Millisecond, admissionAt)

	for _, outcome := range []string{
		metrics.FollowSyncNoop,
		metrics.FollowSyncSuperseded,
		metrics.OutcomeError,
	} {
		m.FollowSync("live", outcome, time.Second, syncAt.Add(-time.Minute))
	}
	if got, present := sampleValue(t, scrape(t, m), `bloar_follow_sync_last_success_timestamp_seconds{head="live"}`); present {
		t.Fatalf("non-completing sync materialized last success at %g", got)
	}
	m.FollowSync("live", metrics.FollowSyncCompleted, 2*time.Second, syncAt)
	m.FollowSyncActive(true)
	m.FollowSyncCoalesced()
	m.FollowSyncCoalesced()

	body := scrape(t, m)
	for sample, want := range map[string]float64{
		`bloar_follow_admission_duration_seconds_count{outcome="error"}`:             1,
		`bloar_follow_admission_duration_seconds_count{outcome="ok"}`:                1,
		`bloar_follow_admission_last_success_timestamp_seconds`:                      float64(admissionAt.Unix()),
		`bloar_follow_sync_duration_seconds_count{head="live",outcome="noop"}`:       1,
		`bloar_follow_sync_duration_seconds_count{head="live",outcome="superseded"}`: 1,
		`bloar_follow_sync_duration_seconds_count{head="live",outcome="error"}`:      1,
		`bloar_follow_sync_duration_seconds_count{head="live",outcome="completed"}`:  1,
		`bloar_follow_sync_last_success_timestamp_seconds{head="live"}`:              float64(syncAt.Unix()),
		`bloar_follow_sync_active`:          1,
		`bloar_follow_sync_coalesced_total`: 2,
	} {
		if got := mustSample(t, body, sample); got != want {
			t.Errorf("%s = %g, want %g", sample, got, want)
		}
	}

	m.FollowSyncActive(false)
	m.FollowAdmission("unbounded", time.Second, admissionAt.Add(time.Hour))
	m.FollowSync("live", "unbounded", time.Second, syncAt.Add(time.Hour))
	body = scrape(t, m)
	if got := mustSample(t, body, `bloar_follow_sync_active`); got != 0 {
		t.Errorf("inactive sync gauge = %g, want 0", got)
	}
	if strings.Contains(body, `outcome="unbounded"`) {
		t.Fatalf("an unbounded follower outcome reached metric labels:\n%s", body)
	}
	assertExactMetricLabels(t, m, "bloar_follow_admission_duration_seconds", "outcome")
	assertExactMetricLabels(t, m, "bloar_follow_sync_duration_seconds", "head", "outcome")
	assertExactMetricLabels(t, m, "bloar_follow_sync_last_success_timestamp_seconds", "head")
}

func TestFollowSourceMetricsUseOnlyConfiguredCells(t *testing.T) {
	m := metrics.New()
	if body := scrape(t, m); strings.Contains(body, "bloar_follow_source_") {
		t.Fatalf("unconfigured source metrics materialized label cells:\n%s", body)
	}

	m.ConfigureFollowSourceMetrics(map[string][]string{
		"all":      {"writer-a", "writer-b", "writer-a"},
		"filtered": {"writer-b"},
		"":         {"ignored-empty-head"},
	})
	fresh := scrape(t, m)
	for _, series := range []string{
		`bloar_follow_source_available{source="writer-a"}`,
		`bloar_follow_source_available{source="writer-b"}`,
		`bloar_follow_source_last_success_timestamp_seconds{source="writer-a"}`,
		`bloar_follow_source_head_covered{head="all",source="writer-a"}`,
		`bloar_follow_source_head_covered{head="all",source="writer-b"}`,
		`bloar_follow_source_head_covered{head="filtered",source="writer-b"}`,
		`bloar_follow_source_selected{head="all",source="writer-a"}`,
		`bloar_follow_source_selected{head="all",source="writer-b"}`,
	} {
		if got := mustSample(t, fresh, series); got != 0 {
			t.Errorf("fresh %s = %g, want 0", series, got)
		}
	}
	if value, ok := sampleValue(t, fresh, `bloar_follow_source_head_synced_to{head="all",source="writer-a"}`); ok {
		t.Fatalf("unobserved synced_to materialized at %g", value)
	}

	m.FollowSourceAvailable("writer-a", true)
	m.FollowSourceHeadClaim("all", "writer-a", 123, true)
	m.FollowSourceHeadClaim("filtered", "writer-b", 999, false)
	m.FollowSourceSelected("all", "writer-a")
	m.FollowSourceAvailable("writer-a", false)

	// Publication-controlled and merely cross-product values never become
	// labels or mutate a configured series.
	m.FollowSourceAvailable("https://untrusted.example/writer", true)
	m.FollowSourceHeadClaim("all", "unconfigured", 999, true)
	m.FollowSourceHeadClaim("unconfigured-head", "writer-a", 999, true)
	m.FollowSourceSelected("all", "12D3Koo-untrusted-peer")
	m.FollowSourceSelected("unconfigured-head", "")

	body := scrape(t, m)
	for series, want := range map[string]float64{
		`bloar_follow_source_available{source="writer-a"}`:                    0,
		`bloar_follow_source_head_covered{head="all",source="writer-a"}`:      1,
		`bloar_follow_source_head_synced_to{head="all",source="writer-a"}`:    123,
		`bloar_follow_source_head_covered{head="filtered",source="writer-b"}`: 0,
		`bloar_follow_source_selected{head="all",source="writer-a"}`:          1,
		`bloar_follow_source_selected{head="all",source="writer-b"}`:          0,
	} {
		if got := mustSample(t, body, series); got != want {
			t.Errorf("%s = %g, want %g", series, got, want)
		}
	}
	if got := mustSample(t, body, `bloar_follow_source_last_success_timestamp_seconds{source="writer-a"}`); got <= 0 {
		t.Errorf("writer-a last success = %g, want a positive timestamp retained across outage", got)
	}
	if value, ok := sampleValue(t, body, `bloar_follow_source_head_synced_to{head="filtered",source="writer-b"}`); ok {
		t.Errorf("uncovered filtered claim retained synced_to at %g", value)
	}
	for _, unbounded := range []string{"untrusted.example", "unconfigured-head", "12D3Koo-untrusted-peer"} {
		if strings.Contains(body, unbounded) {
			t.Fatalf("unconfigured value %q reached a source metric label:\n%s", unbounded, body)
		}
	}
	assertExactMetricLabels(t, m, "bloar_follow_source_available", "source")
	assertExactMetricLabels(t, m, "bloar_follow_source_last_success_timestamp_seconds", "source")
	assertExactMetricLabels(t, m, "bloar_follow_source_head_covered", "head", "source")
	assertExactMetricLabels(t, m, "bloar_follow_source_head_synced_to", "head", "source")
	assertExactMetricLabels(t, m, "bloar_follow_source_selected", "head", "source")

	m.FollowSourceSelected("all", "")
	body = scrape(t, m)
	for _, source := range []string{"writer-a", "writer-b"} {
		series := `bloar_follow_source_selected{head="all",source="` + source + `"}`
		if got := mustSample(t, body, series); got != 0 {
			t.Errorf("cleared selection %s = %g, want 0", series, got)
		}
	}
}

func TestFollowSourceMetricReconfigurationRemovesRetiredCells(t *testing.T) {
	m := metrics.New()
	m.ConfigureFollowSourceMetrics(map[string][]string{
		"all":      {"writer-a", "writer-b"},
		"filtered": {"writer-b"},
	})
	m.FollowSourceAvailable("writer-a", true)
	m.FollowSourceAvailable("writer-b", true)
	m.FollowSourceHeadClaim("all", "writer-a", 100, true)
	m.FollowSourceHeadClaim("filtered", "writer-b", 90, true)
	m.FollowSourceSelected("filtered", "writer-b")

	// Exact replacement removes retired cells and their source-only series,
	// preserves unchanged observations, and gives new cells zero baselines.
	m.ConfigureFollowSourceMetrics(map[string][]string{
		"filtered": {"writer-b", "writer-c"},
	})
	body := scrape(t, m)
	for _, retired := range []string{
		`bloar_follow_source_available{source="writer-a"}`,
		`bloar_follow_source_last_success_timestamp_seconds{source="writer-a"}`,
		`bloar_follow_source_head_covered{head="all",source="writer-a"}`,
		`bloar_follow_source_head_synced_to{head="all",source="writer-a"}`,
		`bloar_follow_source_head_covered{head="all",source="writer-b"}`,
		`bloar_follow_source_selected{head="all",source="writer-b"}`,
	} {
		if value, ok := sampleValue(t, body, retired); ok {
			t.Errorf("retired cell %s remained at %g", retired, value)
		}
	}
	if got := mustSample(t, body, `bloar_follow_source_available{source="writer-b"}`); got != 1 {
		t.Errorf("unchanged writer-b availability reset to %g", got)
	}
	if got := mustSample(t, body, `bloar_follow_source_head_synced_to{head="filtered",source="writer-b"}`); got != 90 {
		t.Errorf("unchanged writer-b claim reset to %g", got)
	}
	if got := mustSample(t, body, `bloar_follow_source_selected{head="filtered",source="writer-b"}`); got != 1 {
		t.Errorf("unchanged writer-b selection reset to %g", got)
	}
	for _, fresh := range []string{
		`bloar_follow_source_available{source="writer-c"}`,
		`bloar_follow_source_last_success_timestamp_seconds{source="writer-c"}`,
		`bloar_follow_source_head_covered{head="filtered",source="writer-c"}`,
		`bloar_follow_source_selected{head="filtered",source="writer-c"}`,
	} {
		if got := mustSample(t, body, fresh); got != 0 {
			t.Errorf("new cell %s = %g, want zero", fresh, got)
		}
	}
	if value, ok := sampleValue(t, body, `bloar_follow_source_head_synced_to{head="filtered",source="writer-c"}`); ok {
		t.Errorf("new writer-c synced_to materialized at %g", value)
	}
}

func TestFollowConflictMetricsUseOnlyConfiguredCells(t *testing.T) {
	m := metrics.New()
	if body := scrape(t, m); strings.Contains(body, "bloar_follow_conflict_") || strings.Contains(body, "bloar_follow_incomparable_") {
		t.Fatalf("unconfigured conflict metrics materialized label cells:\n%s", body)
	}

	m.ConfigureFollowConflictMetrics(map[string][]string{
		"all":      {"writer-a", "writer-b", "writer-a"}, // duplicate is one cell
		"filtered": {"writer-b"},
		"":         {"ignored-empty-head"},
	})
	fresh := scrape(t, m)
	for _, series := range []string{
		`bloar_follow_conflict_active{head="all"}`,
		`bloar_follow_conflict_active{head="filtered"}`,
		`bloar_follow_conflicts_total{head="all",source="writer-a"}`,
		`bloar_follow_conflicts_total{head="all",source="writer-b"}`,
		`bloar_follow_conflicts_total{head="filtered",source="writer-b"}`,
		`bloar_follow_incomparable_active{head="all"}`,
		`bloar_follow_incomparable_active{head="filtered"}`,
		`bloar_follow_incomparable_total{head="all"}`,
		`bloar_follow_incomparable_total{head="filtered"}`,
	} {
		if got := mustSample(t, fresh, series); got != 0 {
			t.Errorf("fresh %s = %g, want 0", series, got)
		}
	}
	for _, absent := range []string{
		`bloar_follow_conflicts_total{head="filtered",source="writer-a"}`,
		`bloar_follow_conflicts_total{head="all",source="unconfigured"}`,
	} {
		if value, ok := sampleValue(t, fresh, absent); ok {
			t.Errorf("unconfigured cell %s materialized at %g", absent, value)
		}
	}

	m.FollowConflictActive("all", true)
	m.FollowConflictCreated("all", "writer-a")
	m.FollowConflictCreated("all", "writer-a")
	m.FollowConflictCreated("all", "writer-b")
	m.FollowIncomparableActive("all", true)
	m.FollowIncomparableObserved("all")
	m.FollowIncomparableObserved("all")

	// None of these untrusted or merely cross-product values is a configured
	// cell, so none may reach a label or mutate a configured series.
	m.FollowConflictActive("bafy-untrusted-cid", true)
	m.FollowConflictCreated("all", "https://untrusted.example/writer")
	m.FollowConflictCreated("filtered", "writer-a")
	m.FollowIncomparableActive("12D3Koo-untrusted-peer", true)
	m.FollowIncomparableObserved("revision-18446744073709551615")

	body := scrape(t, m)
	for series, want := range map[string]float64{
		`bloar_follow_conflict_active{head="all"}`:                        1,
		`bloar_follow_conflict_active{head="filtered"}`:                   0,
		`bloar_follow_conflicts_total{head="all",source="writer-a"}`:      2,
		`bloar_follow_conflicts_total{head="all",source="writer-b"}`:      1,
		`bloar_follow_conflicts_total{head="filtered",source="writer-b"}`: 0,
		`bloar_follow_incomparable_active{head="all"}`:                    1,
		`bloar_follow_incomparable_total{head="all"}`:                     2,
	} {
		if got := mustSample(t, body, series); got != want {
			t.Errorf("%s = %g, want %g", series, got, want)
		}
	}
	for _, unbounded := range []string{
		"bafy-untrusted-cid",
		"untrusted.example",
		"12D3Koo-untrusted-peer",
		"18446744073709551615",
	} {
		if strings.Contains(body, unbounded) {
			t.Fatalf("unconfigured value %q reached a conflict metric label:\n%s", unbounded, body)
		}
	}
	assertExactMetricLabels(t, m, "bloar_follow_conflict_active", "head")
	assertExactMetricLabels(t, m, "bloar_follow_conflicts_total", "head", "source")
	assertExactMetricLabels(t, m, "bloar_follow_incomparable_active", "head")
	assertExactMetricLabels(t, m, "bloar_follow_incomparable_total", "head")
}

func TestFollowConflictMetricReconfigurationRemovesRetiredCells(t *testing.T) {
	m := metrics.New()
	m.ConfigureFollowConflictMetrics(map[string][]string{
		"all":      {"writer-a", "writer-b"},
		"filtered": {"writer-b"},
	})
	m.FollowConflictActive("all", true)
	m.FollowConflictCreated("all", "writer-a")
	m.FollowConflictCreated("filtered", "writer-b")
	m.FollowIncomparableActive("all", true)
	m.FollowIncomparableObserved("all")

	// Reconfiguration is exact, not additive: retired cells disappear rather
	// than lingering at their prior values, unchanged cells keep their values,
	// and newly authorized cells receive an explicit zero baseline.
	m.ConfigureFollowConflictMetrics(map[string][]string{
		"filtered": {"writer-b", "writer-c"},
	})
	body := scrape(t, m)
	for _, retired := range []string{
		`bloar_follow_conflict_active{head="all"}`,
		`bloar_follow_conflicts_total{head="all",source="writer-a"}`,
		`bloar_follow_conflicts_total{head="all",source="writer-b"}`,
		`bloar_follow_incomparable_active{head="all"}`,
		`bloar_follow_incomparable_total{head="all"}`,
	} {
		if value, ok := sampleValue(t, body, retired); ok {
			t.Errorf("retired cell %s remained at %g", retired, value)
		}
	}
	if got := mustSample(t, body, `bloar_follow_conflicts_total{head="filtered",source="writer-b"}`); got != 1 {
		t.Errorf("unchanged source cell reset to %g, want retained count 1", got)
	}
	if got := mustSample(t, body, `bloar_follow_conflicts_total{head="filtered",source="writer-c"}`); got != 0 {
		t.Errorf("new source cell = %g, want zero baseline", got)
	}
	if got := mustSample(t, body, `bloar_follow_conflict_active{head="filtered"}`); got != 0 {
		t.Errorf("unchanged filtered active gauge = %g, want 0", got)
	}

	// Repeating the exact configuration is idempotent and must not reset a
	// counter that has moved since the prior call.
	m.FollowConflictCreated("filtered", "writer-c")
	m.ConfigureFollowConflictMetrics(map[string][]string{"filtered": {"writer-b", "writer-c"}})
	body = scrape(t, m)
	if got := mustSample(t, body, `bloar_follow_conflicts_total{head="filtered",source="writer-c"}`); got != 1 {
		t.Errorf("idempotent reconfiguration reset writer-c count to %g, want 1", got)
	}
}

func TestSwarmDiscoveryMetricTransitionsAreDeterministic(t *testing.T) {
	m := metrics.New()
	failedAt := time.Unix(1_700_000_000, 0)
	succeededAt := failedAt.Add(45 * time.Second)

	m.RendezvousProvide(metrics.OutcomeError, failedAt)
	first := scrape(t, m)
	if got := mustSample(t, first, `bloar_rendezvous_provide_last_success_timestamp_seconds`); got != 0 {
		t.Fatalf("failed rendezvous provide stamped success time %g", got)
	}
	m.RendezvousDiscovery(metrics.RendezvousDiscoveryEmpty, 16)
	m.RendezvousDiscovery(metrics.RendezvousDiscoveryTimeout, 3)
	m.RendezvousProvide(metrics.OutcomeOK, succeededAt)

	m.PointerCurrent(metrics.PointerKindDocument, true, true)
	m.PointerProvide(metrics.PointerKindDocument, metrics.OutcomeError, failedAt)
	m.PointerRetry(metrics.PointerKindDocument, metrics.PointerRetryProvideError)
	failed := scrape(t, m)
	if got := mustSample(t, failed, `bloar_pointer_provide_last_success_timestamp_seconds{kind="document"}`); got != 0 {
		t.Fatalf("failed pointer provide stamped success time %g", got)
	}
	m.PointerProvide(metrics.PointerKindDocument, metrics.OutcomeOK, succeededAt)
	succeeded := scrape(t, m)
	if got := mustSample(t, succeeded, `bloar_rendezvous_provide_last_success_timestamp_seconds`); got != float64(succeededAt.Unix()) {
		t.Errorf("rendezvous last success = %g, want %d", got, succeededAt.Unix())
	}
	if got := mustSample(t, succeeded, `bloar_pointer_provide_last_success_timestamp_seconds{kind="document"}`); got != float64(succeededAt.Unix()) {
		t.Errorf("pointer last success = %g, want %d", got, succeededAt.Unix())
	}

	// A changed current CID must never inherit the prior CID's freshness, and
	// withdrawal suppresses freshness alerts by lowering pointer_current too.
	m.PointerCurrent(metrics.PointerKindDocument, true, true)
	changed := scrape(t, m)
	if got := mustSample(t, changed, `bloar_pointer_provide_last_success_timestamp_seconds{kind="document"}`); got != 0 {
		t.Errorf("changed pointer inherited freshness %g, want 0", got)
	}
	m.PointerCurrent(metrics.PointerKindDocument, false, false)
	withdrawn := scrape(t, m)
	if got := mustSample(t, withdrawn, `bloar_pointer_current{kind="document"}`); got != 0 {
		t.Errorf("withdrawn pointer current = %g, want 0", got)
	}
}

func TestPointerScheduleSeparatesAggregateFreshnessFromAttemptCounts(t *testing.T) {
	m := metrics.New()
	oldest := time.Unix(1_700_000_000, 123_000_000)

	m.PointerSchedule(metrics.PointerKindRoot, true, time.Time{})
	m.PointerProvideOutcome(metrics.PointerKindRoot, metrics.OutcomeOK)
	partial := scrape(t, m)
	if got := mustSample(t, partial, `bloar_pointer_current{kind="root"}`); got != 1 {
		t.Fatalf("partially successful aggregate current = %g, want 1", got)
	}
	if got := mustSample(t, partial, `bloar_pointer_provides_total{kind="root",outcome="ok"}`); got != 1 {
		t.Fatalf("partial aggregate successful calls = %g, want 1", got)
	}
	if got := mustSample(t, partial, `bloar_pointer_provide_last_success_timestamp_seconds{kind="root"}`); got != 0 {
		t.Fatalf("partial aggregate freshness = %g, want 0 until every CID succeeds", got)
	}

	m.PointerSchedule(metrics.PointerKindRoot, true, oldest)
	m.PointerProvideOutcome(metrics.PointerKindRoot, metrics.OutcomeError)
	complete := scrape(t, m)
	if got := mustSample(t, complete, `bloar_pointer_provides_total{kind="root",outcome="error"}`); got != 1 {
		t.Fatalf("aggregate failed calls = %g, want 1", got)
	}
	wantOldest := float64(oldest.Unix()) + 0.123
	if got := mustSample(t, complete, `bloar_pointer_provide_last_success_timestamp_seconds{kind="root"}`); got != wantOldest {
		t.Fatalf("aggregate oldest freshness = %g, want %g", got, wantOldest)
	}

	m.PointerSchedule(metrics.PointerKindRoot, false, oldest)
	withdrawn := scrape(t, m)
	if got := mustSample(t, withdrawn, `bloar_pointer_current{kind="root"}`); got != 0 {
		t.Fatalf("withdrawn aggregate current = %g, want 0", got)
	}
	if got := mustSample(t, withdrawn, `bloar_pointer_provide_last_success_timestamp_seconds{kind="root"}`); got != 0 {
		t.Fatalf("withdrawn aggregate freshness = %g, want 0", got)
	}
}

func assertExactMetricLabels(t *testing.T, m *metrics.Metrics, familyName string, want ...string) {
	t.Helper()
	families, err := m.Registry().Gather()
	if err != nil {
		t.Fatalf("gathering metrics: %v", err)
	}
	wanted := make(map[string]struct{}, len(want))
	for _, name := range want {
		wanted[name] = struct{}{}
	}
	for _, family := range families {
		if family.GetName() != familyName {
			continue
		}
		for _, sample := range family.Metric {
			if len(sample.Label) != len(wanted) {
				t.Fatalf("%s labels = %v, want exactly %v", familyName, sample.Label, want)
			}
			for _, label := range sample.Label {
				if _, ok := wanted[label.GetName()]; !ok {
					t.Fatalf("%s exposes unexpected label %q; want exactly %v", familyName, label.GetName(), want)
				}
			}
		}
		return
	}
	t.Fatalf("metric family %q is absent", familyName)
}

// TestPublicReadAdmissionLabelsStayClosed is the cardinality boundary: the
// observer accepts a string to avoid a server<->metrics import cycle, but only
// the four compile-time outcomes may ever become label values.
func TestPublicReadAdmissionLabelsStayClosed(t *testing.T) {
	m := metrics.New()
	m.PublicReadAdmission("/all/eth/v1/beacon/blobs/1", 129)
	m.PublicReadAdmission("198.51.100.7", 1)
	m.PublicReadAdmission(metrics.PublicReadRejectedGlobal, -1)

	body := scrape(t, m)
	if strings.Contains(body, `/all/eth/v1/beacon/blobs/1`) || strings.Contains(body, `198.51.100.7`) {
		t.Fatalf("an unbounded route or client value reached the metric labels:\n%s", body)
	}
	if got := mustSample(t, body, `bloar_public_read_admissions_total{outcome="rejected_global"}`); got != 0 {
		t.Fatalf("negative cost recorded an admission event: got %g", got)
	}
}

func TestUnfinalizedSnapshotAndReorgMetrics(t *testing.T) {
	m := metrics.New()
	m.UnfinalizedSnapshot("tip", 105, 100, 99, 7)
	m.UnfinalizedRetry("tip", metrics.UnfinalizedRetryExecutionOptimistic)
	m.UnfinalizedRetry("tip", metrics.UnfinalizedRetryHandoffChanged)
	m.UnfinalizedRetry("tip", metrics.UnfinalizedRetryHandoffChanged)
	m.UnfinalizedRetry("tip", metrics.UnfinalizedRetryArchiveUnavailable)
	m.UnfinalizedRetry("tip", "raw archive error must not become a label")
	m.UnfinalizedReorg("tip", 3)
	// Invalid bounds must not overwrite the last coherent observation.
	m.UnfinalizedSnapshot("tip", 99, 100, 101, 8)

	body := scrape(t, m)
	for sample, want := range map[string]float64{
		`bloar_unfinalized_source_head_slot{head="tip"}`:                            105,
		`bloar_unfinalized_source_finalized_slot{head="tip"}`:                       100,
		`bloar_unfinalized_window_start_slot{head="tip"}`:                           99,
		`bloar_unfinalized_window_slots{head="tip"}`:                                7,
		`bloar_unfinalized_generation{head="tip"}`:                                  7,
		`bloar_unfinalized_retries_total{head="tip",reason="execution_optimistic"}`: 1,
		`bloar_unfinalized_retries_total{head="tip",reason="handoff_changed"}`:      2,
		`bloar_unfinalized_retries_total{head="tip",reason="archive_unavailable"}`:  1,
		`bloar_unfinalized_reorgs_total{head="tip"}`:                                1,
	} {
		if got := mustSample(t, body, sample); got != want {
			t.Errorf("%s = %g, want %g", sample, got, want)
		}
	}
	if got := mustSample(t, body, `bloar_unfinalized_reorg_depth_slots_sum{head="tip"}`); got != 3 {
		t.Errorf("reorg depth sum = %g, want 3", got)
	}
	if got := mustSample(t, body, `bloar_unfinalized_last_success_timestamp_seconds{head="tip"}`); got <= 0 {
		t.Errorf("last success timestamp = %g, want positive", got)
	}
	if strings.Contains(body, `reason="raw archive error must not become a label"`) {
		t.Fatalf("an unbounded unfinalized retry reason reached the metric labels:\n%s", body)
	}
}

func TestIndexerRetryOutcomeAndProgressMetrics(t *testing.T) {
	m := metrics.New()
	m.IndexRetry("all", metrics.IndexRetryExecutionOptimistic)
	m.IndexRetry("all", metrics.IndexRetryArchiveUnavailable)
	m.IndexRetry("all", "raw upstream error must not become a label")
	m.IndexOutcome("all", metrics.IndexOutcomeRetry)
	m.IndexOutcome("all", metrics.IndexOutcomeCaughtUp)
	m.IndexOutcome("all", "unbounded")
	m.IndexProgress("all")
	m.IndexArchiveAvailable("all", false)
	m.IndexBlockFetch("arb", metrics.IndexBlockFetchBatch, true, 16, 250*time.Millisecond)
	m.IndexBlockFetch("arb", metrics.IndexBlockFetchFallback, false, 4, time.Second)
	m.IndexBlockFetch("arb", "rpc-url-must-not-be-a-label", true, 100, time.Second)
	m.IndexBlockFetchConfig("arb", 4, 16)
	m.IndexBlockFetchInFlight("arb", 1)
	m.IndexBlockFetchInFlight("arb", 1)
	m.IndexBlockFetchInFlight("arb", -1)
	m.IndexBlockFetchReorderDepth("arb", 3)
	m.IndexBlockFetchReorderDepth("arb", 1)

	body := scrape(t, m)
	for sample, want := range map[string]float64{
		`bloar_index_retries_total{head="all",reason="execution_optimistic"}`:                  1,
		`bloar_index_retries_total{head="all",reason="archive_unavailable"}`:                   1,
		`bloar_index_outcomes_total{head="all",outcome="retry"}`:                               1,
		`bloar_index_outcomes_total{head="all",outcome="caught_up"}`:                           1,
		`bloar_index_outcomes_total{head="all",outcome="progress"}`:                            1,
		`bloar_index_archive_available{head="all"}`:                                            0,
		`bloar_index_l1_block_fetch_batches_total{head="arb",mode="batch",outcome="ok"}`:       1,
		`bloar_index_l1_block_fetch_batches_total{head="arb",mode="fallback",outcome="error"}`: 1,
		`bloar_index_l1_block_fetch_blocks_total{head="arb",mode="batch"}`:                     16,
		`bloar_index_l1_block_fetch_blocks_total{head="arb",mode="fallback"}`:                  4,
		`bloar_index_l1_block_fetch_duration_seconds_count{head="arb",mode="batch"}`:           1,
		`bloar_index_l1_block_fetch_duration_seconds_count{head="arb",mode="fallback"}`:        1,
		`bloar_index_l1_block_fetch_workers{head="arb"}`:                                       4,
		`bloar_index_l1_block_fetch_batch_size{head="arb"}`:                                    16,
		`bloar_index_l1_block_fetch_in_flight{head="arb"}`:                                     1,
		`bloar_index_l1_block_fetch_reorder_depth{head="arb"}`:                                 1,
	} {
		if got := mustSample(t, body, sample); got != want {
			t.Errorf("%s = %g, want %g", sample, got, want)
		}
	}
	if got := mustSample(t, body, `bloar_index_last_progress_timestamp_seconds{head="all"}`); got <= 0 {
		t.Errorf("last progress timestamp = %g, want positive", got)
	}
	m.IndexArchiveAvailable("all", true)
	if got := mustSample(t, scrape(t, m), `bloar_index_archive_available{head="all"}`); got != 1 {
		t.Errorf("archive available after recovery = %g, want 1", got)
	}
	if strings.Contains(body, `reason="raw upstream error must not become a label"`) ||
		strings.Contains(body, `outcome="unbounded"`) ||
		strings.Contains(body, `mode="rpc-url-must-not-be-a-label"`) {
		t.Fatalf("an unbounded reason or outcome reached metric labels:\n%s", body)
	}
	assertExactMetricLabels(t, m, "bloar_index_retries_total", "head", "reason")
	assertExactMetricLabels(t, m, "bloar_index_outcomes_total", "head", "outcome")
	assertExactMetricLabels(t, m, "bloar_index_last_progress_timestamp_seconds", "head")
	assertExactMetricLabels(t, m, "bloar_index_archive_available", "head")
	assertExactMetricLabels(t, m, "bloar_index_l1_block_fetch_batches_total", "head", "mode", "outcome")
	assertExactMetricLabels(t, m, "bloar_index_l1_block_fetch_blocks_total", "head", "mode")
	assertExactMetricLabels(t, m, "bloar_index_l1_block_fetch_duration_seconds", "head", "mode")
	assertExactMetricLabels(t, m, "bloar_index_l1_block_fetch_workers", "head")
	assertExactMetricLabels(t, m, "bloar_index_l1_block_fetch_batch_size", "head")
	assertExactMetricLabels(t, m, "bloar_index_l1_block_fetch_in_flight", "head")
	assertExactMetricLabels(t, m, "bloar_index_l1_block_fetch_reorder_depth", "head")
}

func TestUnfinalizedSnapshotRejectsFullUint64Window(t *testing.T) {
	m := metrics.New()
	m.UnfinalizedSnapshot("tip", ^uint64(0), 0, 0, 1)

	if body := scrape(t, m); strings.Contains(body, `bloar_unfinalized_window_slots{head="tip"}`) {
		t.Fatalf("full uint64 window must not be exported:\n%s", body)
	}
}

// TestDisabledHandlerServesProbesButNoMetrics is what the metrics listener looks
// like if a caller mounts it without a registry. It is not a configuration
// bloard produces -- no listener exists when metrics are off -- but Handler
// documents both arguments as optional, and probes without metrics is the
// combination that has to keep working.
func TestDisabledHandlerServesProbesButNoMetrics(t *testing.T) {
	srv := httptest.NewServer(metrics.Handler(nil, nil))
	defer srv.Close()

	if got := statusOf(t, srv.URL+"/metrics"); got != http.StatusNotFound {
		t.Errorf("GET /metrics with no registry = %d, want 404", got)
	}
	if got := statusOf(t, srv.URL+"/healthz"); got != http.StatusOK {
		t.Errorf("GET /healthz = %d, want 200", got)
	}
	// A nil Health is ready: it has no gates, so there is nothing unmet.
	if got := statusOf(t, srv.URL+"/readyz"); got != http.StatusOK {
		t.Errorf("GET /readyz with no Health = %d, want 200", got)
	}
}

// TestReadyzGates is the readiness semantics of spec 7.4: not ready until every
// gate is met, and each gate names itself while it is not.
func TestReadyzGates(t *testing.T) {
	health := metrics.NewHealth(metrics.GateStore, metrics.GateHeads, metrics.GateReconcile)
	srv := httptest.NewServer(metrics.Handler(metrics.New(), health))
	defer srv.Close()

	// Liveness is not readiness. The process is serving from the first moment,
	// and says so, while readiness is still false -- which is the whole reason
	// they are two endpoints.
	if got := statusOf(t, srv.URL+"/healthz"); got != http.StatusOK {
		t.Errorf("GET /healthz before startup finished = %d, want 200: liveness is 'this process is serving'", got)
	}
	if got := statusOf(t, srv.URL+"/readyz"); got != http.StatusServiceUnavailable {
		t.Errorf("GET /readyz before any gate is met = %d, want 503", got)
	}
	if body := bodyOf(t, srv.URL+"/readyz"); !strings.Contains(body, metrics.GateStore) ||
		!strings.Contains(body, metrics.GateHeads) || !strings.Contains(body, metrics.GateReconcile) {
		t.Errorf("GET /readyz does not name the unmet gates: %s", body)
	}

	// The startup sequence, one gate at a time. Not ready until the last one.
	for _, gate := range []string{metrics.GateStore, metrics.GateHeads} {
		health.Set(gate, true)
		if got := statusOf(t, srv.URL+"/readyz"); got != http.StatusServiceUnavailable {
			t.Errorf("GET /readyz with %s met but reconcile outstanding = %d, want 503", gate, got)
		}
	}
	health.Set(metrics.GateReconcile, true)
	if got := statusOf(t, srv.URL+"/readyz"); got != http.StatusOK {
		t.Errorf("GET /readyz with every gate met = %d, want 200", got)
	}

	// And it goes back: readiness is a state, not a latch.
	health.Set(metrics.GateReconcile, false)
	if got := statusOf(t, srv.URL+"/readyz"); got != http.StatusServiceUnavailable {
		t.Errorf("GET /readyz after a gate went unmet = %d, want 503", got)
	}
}

// TestHealthReadyNamesUnmetGatesSorted checks the API under the endpoint.
func TestHealthReadyNamesUnmetGatesSorted(t *testing.T) {
	health := metrics.NewHealth(metrics.GateStore, metrics.GateHeads, metrics.GateReconcile)

	ready, unmet := health.Ready()
	if ready {
		t.Error("a fresh Health is ready; every gate is unmet")
	}
	// Sorted, so that a probe body and a log line do not shuffle between calls.
	if want := []string{metrics.GateHeads, metrics.GateReconcile, metrics.GateStore}; !equal(unmet, want) {
		t.Errorf("unmet gates = %v, want %v (sorted)", unmet, want)
	}

	var nilHealth *metrics.Health
	if ready, unmet := nilHealth.Ready(); !ready || unmet != nil {
		t.Errorf("a nil Health reports (%t, %v), want (true, []): nil is 'no gates to fail'", ready, unmet)
	}
}

func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func statusOf(t *testing.T, url string) int {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	return resp.StatusCode
}

func bodyOf(t *testing.T, url string) string {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reading %s: %v", url, err)
	}
	return string(body)
}
