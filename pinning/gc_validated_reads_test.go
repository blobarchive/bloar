package pinning_test

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/ipfs/go-cid"

	"github.com/blobarchive/bloar/archive"
	"github.com/blobarchive/bloar/pinning"
)

// sumBlockBytes is the total encoded size of the given blocks, read straight from the
// fixture store -- the ground truth the mark's byte counters must match.
func sumBlockBytes(t *testing.T, f *fixture, cids ...cid.Cid) int64 {
	t.Helper()
	var total int64
	for _, c := range cids {
		blk, err := f.bs.Get(f.ctx, c)
		if err != nil {
			t.Fatalf("Get(%s): %v", c, err)
		}
		total += int64(len(blk.RawData()))
	}
	return total
}

// indexBlockCIDs is a head's dag-cbor index blocks: root, dir pages, sealed segments,
// open segment.
func indexBlockCIDs(h *archive.Head, f *fixture) []cid.Cid {
	e := f.enumerate(h)
	cids := append([]cid.Cid{e.Root}, e.DirPages...)
	for _, s := range e.Sealed {
		cids = append(cids, s.CID)
	}
	if e.Open.Defined() {
		cids = append(cids, e.Open)
	}
	return cids
}

// TestGCValidatedReadsSplitByClass pins the split capacity counters. CID
// validation hashes the whole block, and a dag-cbor node's
// size is not fixed (a sealed Segment can be hundreds of KiB, spec 12.1), so the mark's
// cost is set by BYTES, not a per-block count: it reports validated bytes per class
// (the cost signal) alongside the read counts per class (the read-amplification
// signal). This drives a MIXED closure -- a recursive Full head whose expansion reads
// its dag-cbor index nodes and its raw blob leaves, plus staging rows (direct raw pins)
// -- and asserts both the counts and the BYTE totals per class against the fixture's
// real block sizes. Each class is independent: a leaf-only mutation moves the raw count
// and raw bytes and leaves the node figures untouched.
func TestGCValidatedReadsSplitByClass(t *testing.T) {
	f := newFixture(t, withStaging(0))
	h := f.head("full", pinning.Full())
	f.apply(h, 11, f.row(8, 1), f.row(9, 2))
	f.apply(h, 20, f.row(12, 3)) // 3 distinct blobs (ids 1,2,3) reached recursively
	f.stage(4, 5)                // 2 distinct staging leaves (raw, direct)
	f.reconcileAll()

	// The head's index blocks are the dag-cbor reads: root + dir pages + sealed
	// segments + the open segment. The recursive walk reads each once.
	e := f.enumerate(h)
	nodes := 1 + len(e.DirPages) + len(e.Sealed)
	if e.Open.Defined() {
		nodes++
	}
	const blobs, staged = 3, 2

	stats := f.runGC()
	if stats.ValidatedNodeReads != nodes {
		t.Fatalf("ValidatedNodeReads = %d, want %d (the head's index blocks)", stats.ValidatedNodeReads, nodes)
	}
	if stats.ValidatedRawReads != blobs+staged {
		t.Fatalf("ValidatedRawReads = %d, want %d (%d blobs + %d staging leaves)",
			stats.ValidatedRawReads, blobs+staged, blobs, staged)
	}
	if stats.ValidatedReads != stats.ValidatedRawReads+stats.ValidatedNodeReads {
		t.Fatalf("ValidatedReads = %d, want raw+node = %d", stats.ValidatedReads, stats.ValidatedRawReads+stats.ValidatedNodeReads)
	}

	// The BYTES are the cost signal: the total sha2-256'd, size-dependent for dag-cbor
	// (a node is not a fixed size). They must equal the fixture's real block sizes,
	// class by class -- node bytes = the index blocks, raw bytes = the blobs + staging
	// leaves the mark read.
	wantNodeBytes := sumBlockBytes(t, f, indexBlockCIDs(h, f)...)
	wantRawBytes := sumBlockBytes(t, f, f.blobCID(1), f.blobCID(2), f.blobCID(3), f.blobCID(4), f.blobCID(5))
	if stats.ValidatedNodeBytes != wantNodeBytes {
		t.Fatalf("ValidatedNodeBytes = %d, want %d (the index blocks' encoded sizes)", stats.ValidatedNodeBytes, wantNodeBytes)
	}
	if stats.ValidatedRawBytes != wantRawBytes {
		t.Fatalf("ValidatedRawBytes = %d, want %d (the blobs' + staging leaves' sizes)", stats.ValidatedRawBytes, wantRawBytes)
	}
	if stats.ValidatedBytes != wantRawBytes+wantNodeBytes {
		t.Fatalf("ValidatedBytes = %d, want raw+node = %d", stats.ValidatedBytes, wantRawBytes+wantNodeBytes)
	}

	// Independence: stage one more leaf. Both the raw count and the raw bytes rise by
	// that leaf; the node count and node bytes are untouched. A classifier that lumped
	// leaves into the node tally (or vice versa) would move the wrong counter.
	f.stage(6)
	f.reconcileAll()
	after := f.runGC()
	if after.ValidatedNodeReads != nodes || after.ValidatedNodeBytes != wantNodeBytes {
		t.Fatalf("after a leaf-only mutation node reads/bytes = %d/%d, want unchanged %d/%d",
			after.ValidatedNodeReads, after.ValidatedNodeBytes, nodes, wantNodeBytes)
	}
	if after.ValidatedRawReads != blobs+staged+1 {
		t.Fatalf("after staging one leaf ValidatedRawReads = %d, want %d", after.ValidatedRawReads, blobs+staged+1)
	}
	if want := wantRawBytes + sumBlockBytes(t, f, f.blobCID(6)); after.ValidatedRawBytes != want {
		t.Fatalf("after staging one leaf ValidatedRawBytes = %d, want %d", after.ValidatedRawBytes, want)
	}
}

// TestGCValidatedReadsResetPerRun pins the per-run reset (the safety boundary round
// 5): the counts are one run's work, not a lifetime accumulator. Two runs with
// nothing changed between them must report identical counts; dropping the reset in
// run() doubles the second.
func TestGCValidatedReadsResetPerRun(t *testing.T) {
	f := newFixture(t, withStaging(0))
	h := f.head("full", pinning.Full())
	f.apply(h, 11, f.row(8, 1))
	f.stage(2)
	f.reconcileAll()

	first := f.runGC()
	if first.ValidatedReads == 0 || first.ValidatedBytes == 0 {
		t.Fatal("first run validated nothing; the fixture is meant to have a closure")
	}
	second := f.runGC()
	if second.ValidatedReads != first.ValidatedReads ||
		second.ValidatedRawReads != first.ValidatedRawReads ||
		second.ValidatedNodeReads != first.ValidatedNodeReads {
		t.Fatalf("second run counts (total %d raw %d node %d) != first (total %d raw %d node %d); counts must reset per run",
			second.ValidatedReads, second.ValidatedRawReads, second.ValidatedNodeReads,
			first.ValidatedReads, first.ValidatedRawReads, first.ValidatedNodeReads)
	}
	if second.ValidatedBytes != first.ValidatedBytes ||
		second.ValidatedRawBytes != first.ValidatedRawBytes ||
		second.ValidatedNodeBytes != first.ValidatedNodeBytes {
		t.Fatalf("second run bytes (total %d raw %d node %d) != first (total %d raw %d node %d); byte totals must reset per run",
			second.ValidatedBytes, second.ValidatedRawBytes, second.ValidatedNodeBytes,
			first.ValidatedBytes, first.ValidatedRawBytes, first.ValidatedNodeBytes)
	}
}

// captureHandler records every slog record for assertion.
type captureHandler struct {
	mu      *sync.Mutex
	records *[]slog.Record
}

func (h captureHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h captureHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	*h.records = append(*h.records, r.Clone())
	return nil
}
func (h captureHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h captureHandler) WithGroup(string) slog.Handler      { return h }

// TestGCLogLineHasValidatedReadFields pins the operational surface (audit
// the safety boundary follow-up): the scheduled `gc` log line carries the total and both split
// read counts, so an operator sizes GC from the log without instrumenting the code.
// Removing any of the three fields (or the split) must fail here.
func TestGCLogLineHasValidatedReadFields(t *testing.T) {
	f := newFixture(t, withStaging(0))
	h := f.head("full", pinning.Full())
	f.apply(h, 11, f.row(8, 1))
	f.stage(2)
	f.reconcileAll()

	var mu sync.Mutex
	var records []slog.Record
	gc, err := pinning.NewGC(pinning.GCConfig{
		Blocks: f.bs, Reconciler: f.rec, Staging: f.staging,
		Logger: slog.New(captureHandler{mu: &mu, records: &records}),
	})
	if err != nil {
		t.Fatalf("NewGC: %v", err)
	}

	// The values one run reports; the scheduled run below sees the same closure.
	want, err := gc.Run(f.ctx)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	ctx, cancel := context.WithCancel(f.ctx)
	done := make(chan error, 1)
	go func() { done <- gc.RunEvery(ctx, time.Millisecond) }()

	var rec slog.Record
	deadline := time.Now().Add(5 * time.Second)
	for {
		mu.Lock()
		for _, r := range records {
			if r.Level == slog.LevelInfo && r.Message == "gc" {
				rec = r
			}
		}
		found := rec.Message == "gc"
		mu.Unlock()
		if found || time.Now().After(deadline) {
			break
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	<-done
	if rec.Message != "gc" {
		t.Fatal("no scheduled gc log line was captured")
	}

	fields := map[string]int64{}
	rec.Attrs(func(a slog.Attr) bool {
		if a.Value.Kind() == slog.KindInt64 {
			fields[a.Key] = a.Value.Int64()
		}
		return true
	})
	for _, tc := range []struct {
		key  string
		want int64
	}{
		{"validated_reads", int64(want.ValidatedReads)},
		{"validated_raw_reads", int64(want.ValidatedRawReads)},
		{"validated_node_reads", int64(want.ValidatedNodeReads)},
		{"validated_bytes", want.ValidatedBytes},
		{"validated_raw_bytes", want.ValidatedRawBytes},
		{"validated_node_bytes", want.ValidatedNodeBytes},
	} {
		got, ok := fields[tc.key]
		if !ok {
			t.Fatalf("the gc log line is missing field %q", tc.key)
		}
		if got != tc.want {
			t.Fatalf("gc log field %q = %d, want %d", tc.key, got, tc.want)
		}
	}
	// A byte field must actually carry bytes, not be mislabeled zero.
	if want.ValidatedBytes == 0 {
		t.Fatal("fixture produced zero validated bytes; the byte assertion is vacuous")
	}
}
