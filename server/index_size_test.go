package server_test

import (
	"errors"
	"testing"

	"github.com/ipfs/go-cid"

	"github.com/blobarchive/bloar/archive"
	"github.com/blobarchive/bloar/ingest"
	"github.com/blobarchive/bloar/metrics"
	"github.com/blobarchive/bloar/schema"
)

// TestOversizedIndexSegmentPublishesNothing drives the writer through both
// operator thresholds and then across the reader admission boundary. The
// failed mutation may have encoded unreachable staging blocks, but it must not
// publish a Head that names the oversized Segment or disturb the prior durable
// generation.
func TestOversizedIndexSegmentPublishesNothing(t *testing.T) {
	s := newStack(t, stackOpts{instrument: true, segBits: 9})
	blob := makeBlob(217)
	vhText := s.put(blob)[0]
	vh, err := ingest.VersionedHash(blob)
	if err != nil {
		t.Fatalf("ingest.VersionedHash: %v", err)
	}
	blobCID, err := schema.BlobCID(blob)
	if err != nil {
		t.Fatalf("schema.BlobCID: %v", err)
	}

	rows, encodedSizes := denseRowsThrough(t, vh, blobCID, archive.MaxIndexNodeBytes)
	warningRows := firstSizeAbove(t, encodedSizes, metrics.IndexSegmentWarningBytes)
	criticalRows := firstSizeAbove(t, encodedSizes, metrics.IndexSegmentCriticalBytes)
	refusedRows := firstSizeAbove(t, encodedSizes, archive.MaxIndexNodeBytes)
	if !(warningRows < criticalRows && criticalRows < refusedRows) {
		t.Fatalf("threshold row counts are warning=%d critical=%d refusal=%d, want strictly ascending", warningRows, criticalRows, refusedRows)
	}
	t.Logf("synthetic exact-byte crossings: warning=%d bytes/%d rows, critical=%d/%d, refusal=%d/%d",
		encodedSizes[warningRows-1], warningRows,
		encodedSizes[criticalRows-1], criticalRows,
		encodedSizes[refusedRows-1], refusedRows)

	warning, err := s.heads.ApplyRefs(t.Context(), testHead, rows[:warningRows], rows[warningRows-1].Slot, cid.Undef)
	if err != nil {
		t.Fatalf("warning-size ApplyRefs: %v", err)
	}
	warningSize := encodedSizes[warningRows-1]
	if warningSize <= metrics.IndexSegmentWarningBytes || warningSize >= metrics.IndexSegmentCriticalBytes {
		t.Fatalf("warning sample = %d bytes, want (%d, %d)", warningSize, metrics.IndexSegmentWarningBytes, metrics.IndexSegmentCriticalBytes)
	}
	if got := s.metricValue("bloar_index_segment_encoded_bytes", map[string]string{"head": testHead, "state": metrics.IndexSegmentOpen}); got != float64(warningSize) {
		t.Fatalf("open Segment gauge after warning apply = %g, want %d", got, warningSize)
	}
	assertOpenSample(t, warning.Index, warningSize, warningRows)

	critical, err := s.heads.ApplyRefs(t.Context(), testHead, rows[warningRows:criticalRows], rows[criticalRows-1].Slot, cid.Undef)
	if err != nil {
		t.Fatalf("critical-size ApplyRefs: %v", err)
	}
	criticalSize := encodedSizes[criticalRows-1]
	if criticalSize <= metrics.IndexSegmentCriticalBytes || criticalSize > archive.MaxIndexNodeBytes {
		t.Fatalf("critical sample = %d bytes, want (%d, %d]", criticalSize, metrics.IndexSegmentCriticalBytes, archive.MaxIndexNodeBytes)
	}
	if !critical.Root.Defined() || critical.Root.Equals(warning.Root) {
		t.Fatalf("critical apply root = %s after warning root %s, want a new defined root", critical.Root, warning.Root)
	}
	if got := s.metricValue("bloar_index_segment_encoded_bytes", map[string]string{"head": testHead, "state": metrics.IndexSegmentOpen}); got != float64(criticalSize) {
		t.Fatalf("open Segment gauge after critical apply = %g, want %d", got, criticalSize)
	}
	assertOpenSample(t, critical.Index, criticalSize, criticalRows)
	acceptedApplyBytes := s.metricValue("bloar_index_apply_encoded_bytes_total", map[string]string{"head": testHead})
	if want := float64(warning.Index.EncodedBytes + critical.Index.EncodedBytes); acceptedApplyBytes != want {
		t.Fatalf("successful apply-byte counter = %g, want exact reported sum %g", acceptedApplyBytes, want)
	}

	beforeRoot := critical.Root.String()
	beforeDurable := s.durableRoot(testHead)
	beforeDocs, beforeSwaps := s.docCount(), s.swapCount()
	_, err = s.heads.ApplyRefs(t.Context(), testHead, rows[criticalRows:refusedRows], rows[refusedRows-1].Slot, cid.Undef)
	var oversized *archive.IndexNodeTooLargeError
	if !errors.As(err, &oversized) {
		t.Fatalf("oversized ApplyRefs error = %v, want *archive.IndexNodeTooLargeError", err)
	}
	if oversized.Kind != archive.IndexNodeSegment || oversized.State != archive.SegmentOpen {
		t.Errorf("oversized node = kind %q state %q, want segment/open", oversized.Kind, oversized.State)
	}
	if oversized.EncodedBytes != encodedSizes[refusedRows-1] || oversized.EncodedBytes <= oversized.LimitBytes {
		t.Errorf("oversized bytes = %d limit = %d, want exact encoded %d above limit", oversized.EncodedBytes, oversized.LimitBytes, encodedSizes[refusedRows-1])
	}
	if oversized.Rows != refusedRows || oversized.Refs != refusedRows*schema.MaxBlobsPerSlotCeiling {
		t.Errorf("oversized density = %d rows/%d refs, want %d/%d", oversized.Rows, oversized.Refs, refusedRows, refusedRows*schema.MaxBlobsPerSlotCeiling)
	}

	served, ok := s.heads.Get(testHead)
	if !ok {
		t.Fatal("prior generation stopped serving after oversized apply")
	}
	if got := served.Root().String(); got != beforeRoot {
		t.Errorf("served root after refusal = %s, want prior root %s", got, beforeRoot)
	}
	if got := s.durableRoot(testHead); got != beforeDurable {
		t.Errorf("durable root after refusal = %s, want prior root %s", got, beforeDurable)
	}
	if got := s.docCount(); got != beforeDocs {
		t.Errorf("publication documents after refusal = %d, want unchanged %d", got, beforeDocs)
	}
	if got := s.swapCount(); got != beforeSwaps {
		t.Errorf("root notifications after refusal = %d, want unchanged %d", got, beforeSwaps)
	}
	lookup, err := served.Lookup(t.Context(), testOrigin)
	if err != nil || lookup.Status != archive.StatusFound || len(lookup.Entries) != schema.MaxBlobsPerSlotCeiling {
		t.Fatalf("prior generation lookup after refusal = status %v entries %d err %v", lookup.Status, len(lookup.Entries), err)
	}
	if got := s.getBlobs(testOrigin, vhText); len(got) != 1 {
		t.Fatalf("prior generation HTTP read after refusal returned %d blobs, want 1", len(got))
	}

	if got := s.metricValue("bloar_index_segment_encoded_bytes", map[string]string{"head": testHead, "state": metrics.IndexSegmentOpen}); got != float64(criticalSize) {
		t.Errorf("accepted-state gauge moved on refusal: got %g, want %d", got, criticalSize)
	}
	if got := s.metricValue("bloar_index_apply_encoded_bytes_total", map[string]string{"head": testHead}); got != acceptedApplyBytes {
		t.Errorf("accepted apply-byte counter moved on refusal: got %g, want %g", got, acceptedApplyBytes)
	}
	if got := s.metricValue("bloar_index_node_limit_refusals_total", map[string]string{"head": testHead, "node": metrics.IndexNodeSegment}); got != 1 {
		t.Errorf("Segment refusal counter = %g, want 1", got)
	}
}

func TestSegmentMetricsRestoredFromDurableRoot(t *testing.T) {
	dir := t.TempDir()
	first := newStack(t, stackOpts{dir: dir, instrument: true, segBits: 9})
	vh := first.put(makeBlob(218))[0]
	first.refs([]map[string]any{row(testOrigin, vh)}, (1<<9)-1)
	wantOpen := first.metricValue("bloar_index_segment_encoded_bytes", map[string]string{"head": testHead, "state": metrics.IndexSegmentOpen})
	wantSealed := first.metricValue("bloar_index_segment_encoded_bytes", map[string]string{"head": testHead, "state": metrics.IndexSegmentSealed})
	if wantOpen <= 0 || wantSealed <= 0 {
		t.Fatal("first writer did not publish a positive open Segment size")
	}
	first.Close()

	restarted := newStack(t, stackOpts{dir: dir, instrument: true, segBits: 9})
	if got := restarted.metricValue("bloar_index_segment_encoded_bytes", map[string]string{"head": testHead, "state": metrics.IndexSegmentOpen}); got != wantOpen {
		t.Fatalf("open Segment gauge after restart = %g, want durable exact size %g", got, wantOpen)
	}
	if got := restarted.metricValue("bloar_index_segment_encoded_bytes", map[string]string{"head": testHead, "state": metrics.IndexSegmentSealed}); got != wantSealed {
		t.Fatalf("sealed Segment gauge after restart = %g, want durable exact size %g", got, wantSealed)
	}
	if got := restarted.metricValue("bloar_index_segment_rows", map[string]string{"head": testHead, "state": metrics.IndexSegmentOpen}); got != 0 {
		t.Fatalf("open Segment rows after restart = %g, want 0", got)
	}
	if got := restarted.metricValue("bloar_index_segment_rows", map[string]string{"head": testHead, "state": metrics.IndexSegmentSealed}); got != 1 {
		t.Fatalf("sealed Segment rows after restart = %g, want 1", got)
	}
	if got := restarted.metricValue("bloar_index_segment_refs", map[string]string{"head": testHead, "state": metrics.IndexSegmentSealed}); got != 1 {
		t.Fatalf("sealed Segment refs after restart = %g, want 1", got)
	}
	for _, name := range []string{
		"bloar_index_segment_sealed_encoded_bytes",
		"bloar_index_segment_sealed_max_encoded_bytes",
	} {
		if restarted.metricPresent(name, map[string]string{"head": testHead}) {
			t.Errorf("restart replayed historical seal into %s", name)
		}
	}
}

func assertOpenSample(t *testing.T, stats archive.IndexApplyStats, encodedBytes, rows int) {
	t.Helper()
	if len(stats.Segments) != 1 {
		t.Fatalf("apply reported %d Segment samples, want one open sample", len(stats.Segments))
	}
	sample := stats.Segments[0]
	if sample.State != archive.SegmentOpen || sample.EncodedBytes != encodedBytes ||
		sample.Rows != rows || sample.Refs != rows*schema.MaxBlobsPerSlotCeiling {
		t.Fatalf("open sample = %#v, want bytes=%d rows=%d refs=%d", sample, encodedBytes, rows, rows*schema.MaxBlobsPerSlotCeiling)
	}
}

// denseRowsThrough constructs one sparse row per slot with the maximum bounded
// refs per row. It uses the production canonical encoder to locate thresholds;
// the test does not carry a parallel byte-size formula.
func denseRowsThrough(t *testing.T, vh schema.VersionedHash, blob cid.Cid, target int) ([]archive.RefRow, []int) {
	t.Helper()
	refs := make([]schema.RefEntry, schema.MaxBlobsPerSlotCeiling)
	vhs := make([]schema.VersionedHash, schema.MaxBlobsPerSlotCeiling)
	for i := range refs {
		refs[i] = schema.RefEntry{VH: vh, Blob: blob}
		vhs[i] = vh
	}

	segment := schema.Segment{Slot0: 0}
	var rows []archive.RefRow
	var sizes []int
	for len(sizes) < 1<<9-testOrigin {
		slot := uint64(testOrigin + len(sizes))
		segment.Rows = append(segment.Rows, schema.Row{Slot: slot, Entries: refs})
		data, _, err := schema.EncodeSegment(&segment)
		if err != nil {
			t.Fatalf("encoding synthetic Segment with %d rows: %v", len(segment.Rows), err)
		}
		rows = append(rows, archive.RefRow{Slot: slot, VHs: vhs})
		sizes = append(sizes, len(data))
		if len(data) > target {
			return rows, sizes
		}
	}
	t.Fatalf("synthetic Segment did not exceed %d bytes within its window", target)
	return nil, nil
}

func firstSizeAbove(t *testing.T, sizes []int, target int) int {
	t.Helper()
	for i, size := range sizes {
		if size > target {
			return i + 1
		}
	}
	t.Fatalf("no encoded size exceeded %d", target)
	return 0
}

func (s *stack) metricValue(name string, labels map[string]string) float64 {
	s.t.Helper()
	families, err := s.metrics.Registry().Gather()
	if err != nil {
		s.t.Fatalf("gathering metrics: %v", err)
	}
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, sample := range family.GetMetric() {
			matched := len(sample.GetLabel()) == len(labels)
			for _, label := range sample.GetLabel() {
				if labels[label.GetName()] != label.GetValue() {
					matched = false
				}
			}
			if !matched {
				continue
			}
			switch family.GetType().String() {
			case "GAUGE":
				return sample.GetGauge().GetValue()
			case "COUNTER":
				return sample.GetCounter().GetValue()
			default:
				s.t.Fatalf("metric %s has unsupported type %s", name, family.GetType())
			}
		}
	}
	s.t.Fatalf("metric %s with labels %v is absent", name, labels)
	return 0
}

func (s *stack) metricPresent(name string, labels map[string]string) bool {
	s.t.Helper()
	families, err := s.metrics.Registry().Gather()
	if err != nil {
		s.t.Fatalf("gathering metrics: %v", err)
	}
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, sample := range family.GetMetric() {
			matched := len(sample.GetLabel()) == len(labels)
			for _, label := range sample.GetLabel() {
				if labels[label.GetName()] != label.GetValue() {
					matched = false
				}
			}
			if matched {
				return true
			}
		}
	}
	return false
}
