package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/ipfs/boxo/exchange"
	"github.com/ipfs/go-cid"

	"github.com/blobarchive/bloar/catalog"
	"github.com/blobarchive/bloar/follow"
	"github.com/blobarchive/bloar/pinning"
	"github.com/blobarchive/bloar/server"
	"github.com/blobarchive/bloar/store"
)

func conflictCommandConfig(t *testing.T, storePath string) string {
	t.Helper()
	rendered := renderSourceSetConfig(t, testSources(), testSourceHeads(), 7, "", "")
	rendered = strings.Replace(rendered, "store: {path: /x}", fmt.Sprintf("store: {path: %q}", storePath), 1)
	return writeFile(t, "conflicts.yaml", rendered)
}

func TestConflictsStatusEmptyHumanAndJSON(t *testing.T) {
	config := conflictCommandConfig(t, t.TempDir())

	var human bytes.Buffer
	if err := run([]string{"conflicts", "status", "-config", config}, &human); err != nil {
		t.Fatalf("conflicts status: %v", err)
	}
	wantHuman := "archive: " + testFollowArchiveID + "\nactive conflict latches: 0\n"
	if human.String() != wantHuman {
		t.Fatalf("human status = %q, want %q", human.String(), wantHuman)
	}

	var raw bytes.Buffer
	if err := run([]string{"conflicts", "status", "-config", config, "-json"}, &raw); err != nil {
		t.Fatalf("conflicts status -json: %v", err)
	}
	var got conflictStatusReport
	if err := json.Unmarshal(raw.Bytes(), &got); err != nil {
		t.Fatalf("decoding JSON status: %v\n%s", err, raw.String())
	}
	if got.Schema != "bloar.conflicts.status/v1" || got.ArchiveID != testFollowArchiveID || got.Conflicts == nil || len(got.Conflicts) != 0 {
		t.Fatalf("JSON status = %+v", got)
	}
}

func TestConflictsRequiresSourceSetAndExclusiveStore(t *testing.T) {
	singular := &Config{Store: StoreConfig{Path: t.TempDir()}}
	if err := conflictStatus(singular, "", false, &bytes.Buffer{}); err == nil || !strings.Contains(err.Error(), "follow.sources") {
		t.Fatalf("singular status error = %v, want source-set refusal", err)
	}

	storePath := t.TempDir()
	config := conflictCommandConfig(t, storePath)
	held := openStore(t, storePath)
	defer held.Close()
	var out bytes.Buffer
	err := run([]string{"conflicts", "status", "-config", config}, &out)
	if err == nil || !strings.Contains(err.Error(), "locked") || !strings.Contains(err.Error(), "stop the daemon") {
		t.Fatalf("locked status error = %v", err)
	}
}

func TestConflictEvidenceIDParsing(t *testing.T) {
	text := "sha256:" + strings.Repeat("ab", sha256.Size)
	got, err := parseConflictEvidenceID(text)
	if err != nil {
		t.Fatal(err)
	}
	if formatDigest(got) != text {
		t.Fatalf("round trip = %q, want %q", formatDigest(got), text)
	}
	for _, invalid := range []string{
		"", strings.Repeat("ab", sha256.Size), "sha256:" + strings.Repeat("a", 63),
		"sha256:" + strings.Repeat("AB", sha256.Size), "sha256:" + strings.Repeat("zz", sha256.Size),
	} {
		if _, err := parseConflictEvidenceID(invalid); err == nil {
			t.Errorf("parseConflictEvidenceID(%q) succeeded", invalid)
		}
	}
}

func TestConflictStatusReportPreservesEvidenceRoles(t *testing.T) {
	archiveID, err := server.ParseArchiveID(testFollowArchiveID)
	if err != nil {
		t.Fatal(err)
	}
	rootA := cidUnder(t, cid.Raw, []byte("root-a"))
	rootB := cidUnder(t, cid.Raw, []byte("root-b"))
	doc := sha256.Sum256([]byte("document"))
	roster := sha256.Sum256([]byte("roster"))
	evidence := sha256.Sum256([]byte("evidence"))
	report := newConflictStatusReport(archiveID, []follow.ConflictRecord{{
		ArchiveID: archiveID, Head: "all", Sequence: 3,
		Reason:            follow.ConflictReasonEqualCoverageRootMismatch,
		SourceSetRevision: 7, SourceSetDigest: roster, PairCount: 2, EvidenceID: evidence,
		Left: follow.ConflictCandidateSummary{
			Role: follow.ConflictCandidateSource, SourceID: "writer-a", Revision: 9, Digest: doc,
			Root: rootA, Covered: true, SyncedTo: 100,
		},
		Right: follow.ConflictCandidateSummary{
			Role: follow.ConflictCandidateDurable, CheckpointVersion: 3,
			Root: rootB, Covered: true, SyncedTo: 100,
		},
	}})
	if len(report.Conflicts) != 1 {
		t.Fatalf("report = %+v", report)
	}
	got := report.Conflicts[0]
	if got.Left.Role != "source" || got.Left.SourceID != "writer-a" || got.Left.DocumentDigest != formatDigest(doc) {
		t.Fatalf("source evidence = %+v", got.Left)
	}
	if got.Right.Role != "durable_checkpoint" || got.Right.CheckpointVersion != 3 || got.Right.DocumentDigest != "" {
		t.Fatalf("durable evidence = %+v", got.Right)
	}
	human := report.human()
	for _, field := range []string{
		"evidence: " + formatDigest(evidence), "reason: equal_coverage_root_mismatch",
		"role=source checkpoint_version=0 source=writer-a",
		"role=durable_checkpoint checkpoint_version=3 source=none", "conflicting pairs: 2",
	} {
		if !strings.Contains(human, field) {
			t.Errorf("human report omitted %q:\n%s", field, human)
		}
	}
}

func TestConflictsClearRequiresExactActiveEvidence(t *testing.T) {
	storePath := t.TempDir()
	configPath := conflictCommandConfig(t, storePath)
	cfg, err := LoadConfig(configPath)
	if err != nil {
		t.Fatal(err)
	}
	record := seedConflictLatch(t, cfg)
	var status bytes.Buffer
	if err := run([]string{"conflicts", "status", "-config", configPath, "-head", record.Head, "-json"}, &status); err != nil {
		t.Fatalf("filtered active status: %v", err)
	}
	var active conflictStatusReport
	if err := json.Unmarshal(status.Bytes(), &active); err != nil {
		t.Fatal(err)
	}
	if len(active.Conflicts) != 1 || active.Conflicts[0].EvidenceID != formatDigest(record.EvidenceID) ||
		active.Conflicts[0].Head != record.Head {
		t.Fatalf("filtered active status = %+v", active)
	}

	wrong := "sha256:" + strings.Repeat("00", sha256.Size)
	err = run([]string{"conflicts", "clear", "-config", configPath, "-head", record.Head, "-evidence", wrong}, &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "does not match") || !strings.Contains(err.Error(), formatDigest(record.EvidenceID)) {
		t.Fatalf("wrong-evidence clear = %v", err)
	}

	var out bytes.Buffer
	if err := run([]string{"conflicts", "clear", "-config", configPath, "-head", record.Head,
		"-evidence", formatDigest(record.EvidenceID)}, &out); err != nil {
		t.Fatalf("exact-evidence clear: %v", err)
	}
	if !strings.Contains(out.String(), "cleared conflict latch") || !strings.Contains(out.String(), "sequence 1") {
		t.Fatalf("clear output = %q", out.String())
	}

	// The same command is not idempotent by accident: an already-cleared latch is
	// a clean, explicit error rather than an apparent second authorization.
	err = run([]string{"conflicts", "clear", "-config", configPath, "-head", record.Head,
		"-evidence", formatDigest(record.EvidenceID)}, &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "no active conflict latch") {
		t.Fatalf("second clear = %v", err)
	}

	st := openStore(t, storePath)
	defer st.Close()
	history, err := follow.LoadConflictClearHistory(st.KV(), record.ArchiveID, record.Head)
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 1 || history[0].EvidenceID != record.EvidenceID || history[0].Operator != fmt.Sprintf("uid:%d", os.Geteuid()) {
		t.Fatalf("clear history = %+v", history)
	}
}

func TestConflictCommandsRejectCorruptClearedCapabilityFloor(t *testing.T) {
	storePath := t.TempDir()
	configPath := conflictCommandConfig(t, storePath)
	cfg, err := LoadConfig(configPath)
	if err != nil {
		t.Fatal(err)
	}
	record := seedConflictLatch(t, cfg)
	st := openStore(t, storePath)
	if _, err := follow.ClearConflictLatch(st.KV(), follow.ConflictClearRequest{
		ArchiveID: record.ArchiveID, Head: record.Head, EvidenceID: record.EvidenceID,
		ClearedAt: time.Unix(1, 0), Operator: "uid:1000",
	}); err != nil {
		t.Fatal(err)
	}

	// The source-set marker key is part of follow's documented durable layout.
	// Downgrading its v2 feature-bearing encoding to v1 simulates an old binary
	// erasing the capability floor after the active latch has been cleared.
	const sourceSetMarkerKey = "fsource_set:v1"
	encoded, closer, err := st.KV().Get([]byte(sourceSetMarkerKey))
	if err != nil {
		t.Fatal(err)
	}
	marker := bytes.Clone(encoded)
	if err := closer.Close(); err != nil {
		t.Fatal(err)
	}
	const sourceSetMarkerV1Bytes = 1 + 32 + 8 + 32
	if len(marker) <= sourceSetMarkerV1Bytes {
		t.Fatalf("source-set marker has %d bytes, want feature-bearing v2 encoding", len(marker))
	}
	marker = marker[:sourceSetMarkerV1Bytes]
	marker[0] = 1
	if err := st.KV().Set([]byte(sourceSetMarkerKey), marker, pebble.Sync); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	for _, args := range [][]string{
		{"conflicts", "status", "-config", configPath},
		{"conflicts", "clear", "-config", configPath, "-head", record.Head,
			"-evidence", formatDigest(record.EvidenceID)},
	} {
		err := run(args, &bytes.Buffer{})
		if err == nil || !strings.Contains(err.Error(), "not covered by a conflict-aware source-set marker") {
			t.Errorf("run(%q) = %v, want corrupt capability-floor refusal", args, err)
		}
	}
}

func TestConflictCommandsRejectConflictAwareMarkerWithoutAuditRows(t *testing.T) {
	storePath := t.TempDir()
	configPath := conflictCommandConfig(t, storePath)
	cfg, err := LoadConfig(configPath)
	if err != nil {
		t.Fatal(err)
	}
	record := seedConflictLatch(t, cfg)
	st := openStore(t, storePath)
	if _, err := follow.ClearConflictLatch(st.KV(), follow.ConflictClearRequest{
		ArchiveID: record.ArchiveID, Head: record.Head, EvidenceID: record.EvidenceID,
		ClearedAt: time.Unix(1, 0), Operator: "uid:1000",
	}); err != nil {
		t.Fatal(err)
	}
	for _, prefix := range []string{"source_conflict:v1:", "source_conflict_sequence:v1:", "source_conflict_history:v1:"} {
		key := append([]byte("f"+prefix), record.ArchiveID[:]...)
		key = append(key, ':')
		key = append(key, record.Head...)
		if err := st.KV().Delete(key, pebble.Sync); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	for _, args := range [][]string{
		{"conflicts", "status", "-config", configPath},
		{"conflicts", "clear", "-config", configPath, "-head", record.Head,
			"-evidence", formatDigest(record.EvidenceID)},
	} {
		err := run(args, &bytes.Buffer{})
		if err == nil || !strings.Contains(err.Error(), "conflict-aware source-set marker has no durable conflict rows") {
			t.Errorf("run(%q) = %v, want lost conflict-state refusal", args, err)
		}
	}
}

type conflictTestSessions struct{}

func (conflictTestSessions) NewSession(context.Context) exchange.Fetcher { return nil }

// seedConflictLatch uses the public construction and persistence boundaries so
// the command test exercises the same source-set marker and evidence encoding as
// the daemon, without copying private KV keys into the CLI package.
func seedConflictLatch(t *testing.T, cfg *Config) follow.ConflictRecord {
	t.Helper()
	st, err := store.Open(cfg.Store.Path, store.WithPebbleLogger(quietPebble{}))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	archiveID, err := cfg.Follow.ExpectedArchiveID()
	if err != nil || archiveID == nil {
		t.Fatalf("archive ID = %v, err = %v", archiveID, err)
	}
	sourceSet, err := cfg.Follow.runtimeSourceSet(cfg.Net)
	if err != nil {
		t.Fatal(err)
	}
	ledger := catalog.NewLedger(st.KV())
	manifests := server.NewManifestStore(st.KV())
	reconciler, err := pinning.NewReconciler(pinning.Config{Ledger: ledger, ManifestTip: manifests.Get})
	if err != nil {
		t.Fatal(err)
	}
	roots := server.NewRootStore(st.KV())
	registry, err := server.NewHeads(server.HeadsConfig{
		Net: cfg.Net, Roots: roots, Manifests: manifests, Blocks: st.Blocks(), Gate: reconciler.Gate(),
	})
	if err != nil {
		t.Fatal(err)
	}
	follower, err := follow.New(follow.Config{
		Net: cfg.Net, ExpectedArchiveID: archiveID, SourceSet: sourceSet,
		Heads:            map[string]pinning.Policy{"all": pinning.None(), "tip": pinning.Full()},
		ExpectedKinds:    map[string]server.HeadKind{"all": server.FinalizedMonotonic, "tip": server.UnfinalizedMutable},
		ExpectedHandoffs: map[string]string{"tip": "all"}, MaxMutableWindowSlots: map[string]uint64{"tip": 64},
		Local: st.Blocks(), Sessions: conflictTestSessions{}, Registry: registry, Roots: roots,
		Manifests: manifests, Reconciler: reconciler, KV: st.KV(),
	})
	if err != nil {
		t.Fatalf("activating source set: %v", err)
	}
	defer follower.Close()

	digestA := sha256.Sum256([]byte("writer-a document"))
	digestB := sha256.Sum256([]byte("writer-b document"))
	rootA := cidUnder(t, cid.Raw, []byte("writer-a root"))
	rootB := cidUnder(t, cid.Raw, []byte("writer-b root"))
	request, err := follow.NewConflictLatchRequest(follow.ConflictLatchInput{
		ArchiveID: *archiveID, Head: "all", Reason: follow.ConflictReasonEqualCoverageRootMismatch,
		SourceSetRevision: sourceSet.Revision, SourceSetDigest: sourceSet.Digest, PairCount: 1,
		Left: follow.ConflictCandidateSummary{
			Role: follow.ConflictCandidateSource, SourceID: "writer-a", Revision: 11, Digest: digestA,
			Root: rootA, Covered: true, SyncedTo: 123,
		},
		Right: follow.ConflictCandidateSummary{
			Role: follow.ConflictCandidateSource, SourceID: "writer-b", Revision: 12, Digest: digestB,
			Root: rootB, Covered: true, SyncedTo: 123,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	batch := st.KV().NewBatch()
	defer batch.Close()
	record, created, err := follow.StageConflictLatch(st.KV(), batch, request)
	if err != nil {
		t.Fatal(err)
	}
	if !created {
		t.Fatal("fresh conflict latch was not created")
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		t.Fatal(err)
	}
	return record
}

func TestConflictsCommandGrammar(t *testing.T) {
	for _, tc := range []struct {
		args []string
		want string
	}{
		{args: []string{"conflicts"}, want: "expected `status` or `clear`"},
		{args: []string{"conflicts", "unknown"}, want: "unknown action"},
		{args: []string{"conflicts", "clear"}, want: "-head is required"},
		{args: []string{"conflicts", "clear", "-head", "all"}, want: "-evidence is required"},
		{args: []string{"conflicts", "status", "extra"}, want: "no positional arguments"},
	} {
		err := run(tc.args, &bytes.Buffer{})
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("run(%q) = %v, want error containing %q", tc.args, err, tc.want)
		}
	}

	if _, err := parseConflictEvidenceID("sha256:" + strings.Repeat("0", 64)); err != nil {
		t.Fatalf("valid evidence syntax: %v", err)
	}
}
