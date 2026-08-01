package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/ipfs/go-cid"

	"github.com/blobarchive/bloar/follow"
	"github.com/blobarchive/bloar/server"
	"github.com/blobarchive/bloar/store"
)

// runConflicts dispatches the offline multi-writer conflict-latch tools.
func runConflicts(args []string, out io.Writer) error {
	if len(args) == 0 {
		return errors.New("conflicts: expected `status` or `clear`")
	}
	action, args := args[0], args[1:]
	switch action {
	case "status":
		fs := flag.NewFlagSet("conflicts status", flag.ContinueOnError)
		config := fs.String("config", "", "path to the YAML config (spec 12)")
		head := fs.String("head", "", "restrict the report to one physical finalized head")
		asJSON := fs.Bool("json", false, "emit a deterministic JSON report")
		if err := fs.Parse(args); err != nil {
			return err
		}
		if fs.NArg() != 0 {
			return errors.New("conflicts status: no positional arguments are accepted")
		}
		cfg, err := loadFrom(*config)
		if err != nil {
			return err
		}
		return conflictStatus(cfg, *head, *asJSON, out)

	case "clear":
		fs := flag.NewFlagSet("conflicts clear", flag.ContinueOnError)
		config := fs.String("config", "", "path to the YAML config (spec 12)")
		head := fs.String("head", "", "physical finalized head whose latch was investigated (required)")
		evidence := fs.String("evidence", "", "exact sha256 evidence ID reported by status (required)")
		if err := fs.Parse(args); err != nil {
			return err
		}
		if fs.NArg() != 0 {
			return errors.New("conflicts clear: no positional arguments are accepted")
		}
		if *head == "" {
			return errors.New("conflicts clear: -head is required")
		}
		if *evidence == "" {
			return errors.New("conflicts clear: -evidence is required")
		}
		cfg, err := loadFrom(*config)
		if err != nil {
			return err
		}
		return conflictClear(cfg, *head, *evidence, out)

	default:
		return fmt.Errorf("conflicts: unknown action %q; expected `status` or `clear`", action)
	}
}

func conflictStatus(cfg *Config, head string, asJSON bool, out io.Writer) error {
	archiveID, err := conflictArchiveID(cfg)
	if err != nil {
		return err
	}
	st, err := openConflictStore(cfg, "conflicts status")
	if err != nil {
		return err
	}
	defer closeConflictStore(st)
	if err := follow.ValidateConflictState(st.KV(), archiveID); err != nil {
		return fmt.Errorf("bloard: conflicts status: validating durable conflict state: %w", err)
	}

	var records []follow.ConflictRecord
	if head == "" {
		records, err = follow.ListConflictLatches(st.KV(), archiveID)
	} else {
		var record follow.ConflictRecord
		var ok bool
		record, ok, err = follow.LoadConflictLatch(st.KV(), archiveID, head)
		if ok {
			records = []follow.ConflictRecord{record}
		}
	}
	if err != nil {
		return fmt.Errorf("bloard: conflicts status: %w", err)
	}

	report := newConflictStatusReport(archiveID, records)
	if asJSON {
		enc := json.NewEncoder(out)
		enc.SetIndent("", "  ")
		if err := enc.Encode(report); err != nil {
			return fmt.Errorf("bloard: conflicts status: writing JSON report: %w", err)
		}
		return nil
	}
	if _, err := io.WriteString(out, report.human()); err != nil {
		return fmt.Errorf("bloard: conflicts status: writing report: %w", err)
	}
	return nil
}

func conflictClear(cfg *Config, head, evidence string, out io.Writer) error {
	archiveID, err := conflictArchiveID(cfg)
	if err != nil {
		return err
	}
	evidenceID, err := parseConflictEvidenceID(evidence)
	if err != nil {
		return fmt.Errorf("bloard: conflicts clear: %w", err)
	}
	st, err := openConflictStore(cfg, "conflicts clear")
	if err != nil {
		return err
	}
	defer closeConflictStore(st)
	if err := follow.ValidateConflictState(st.KV(), archiveID); err != nil {
		return fmt.Errorf("bloard: conflicts clear: validating durable conflict state: %w", err)
	}

	// Give the operator a precise no-latch or stale-ID diagnostic before the
	// atomic clear. The store is exclusively held, so this cannot race a poll or
	// another offline command.
	active, ok, err := follow.LoadConflictLatch(st.KV(), archiveID, head)
	if err != nil {
		return fmt.Errorf("bloard: conflicts clear: %w", err)
	}
	if !ok {
		return fmt.Errorf("bloard: conflicts clear: head %q has no active conflict latch", head)
	}
	if active.EvidenceID != evidenceID {
		return fmt.Errorf("bloard: conflicts clear: evidence ID does not match the active latch for head %q (current %s)",
			head, formatDigest(active.EvidenceID))
	}

	cleared, err := follow.ClearConflictLatch(st.KV(), follow.ConflictClearRequest{
		ArchiveID:  archiveID,
		Head:       head,
		EvidenceID: evidenceID,
		ClearedAt:  time.Now().UTC(),
		// Numeric effective UID is supplied by the kernel, bounded, and remains
		// meaningful even when the account has no passwd entry in a container.
		Operator: fmt.Sprintf("uid:%d", os.Geteuid()),
	})
	if err != nil {
		return fmt.Errorf("bloard: conflicts clear: %w", err)
	}
	if _, err := fmt.Fprintf(out, "cleared conflict latch for head %q at sequence %d (evidence %s)\n",
		cleared.Head, cleared.Sequence, formatDigest(cleared.EvidenceID)); err != nil {
		// The durable clear already committed. Make that explicit: retrying with
		// the same evidence correctly reports that there is no active latch.
		return fmt.Errorf("bloard: conflicts clear committed, but writing confirmation failed: %w", err)
	}
	return nil
}

func conflictArchiveID(cfg *Config) (server.ArchiveID, error) {
	if cfg == nil || cfg.Follow == nil || !cfg.Follow.sourceSetMode() {
		return server.ArchiveID{}, errors.New("bloard: conflicts requires a follower configured with follow.sources and follow.source_set")
	}
	archiveID, err := cfg.Follow.ExpectedArchiveID()
	if err != nil {
		return server.ArchiveID{}, fmt.Errorf("bloard: conflicts: %w", err)
	}
	if archiveID == nil {
		return server.ArchiveID{}, errors.New("bloard: conflicts requires follow.archive_id")
	}
	return *archiveID, nil
}

func openConflictStore(cfg *Config, operation string) (*store.Store, error) {
	locked, err := store.Locked(cfg.Store.Path)
	if err != nil {
		return nil, err
	}
	if locked {
		return nil, fmt.Errorf("bloard: the store at %s is locked; a daemon (or another tool) is holding it. %s needs "+
			"exclusive access -- stop the daemon and retry", cfg.Store.Path, operation)
	}
	log := newLogger()
	st, err := store.Open(cfg.Store.Path, store.WithPebbleLogger(pebbleLogger{log: log.With("component", "pebble")}))
	if err != nil {
		return nil, err
	}
	return st, nil
}

func closeConflictStore(st *store.Store) {
	if err := st.Close(); err != nil {
		newLogger().Error("closing store after conflict operation", "err", err)
	}
}

func parseConflictEvidenceID(text string) ([sha256.Size]byte, error) {
	const prefix = "sha256:"
	var out [sha256.Size]byte
	if !strings.HasPrefix(text, prefix) || len(text) != len(prefix)+sha256.Size*2 {
		return out, errors.New("-evidence must have the form sha256:<64 lowercase hexadecimal characters>")
	}
	raw := text[len(prefix):]
	if raw != strings.ToLower(raw) {
		return out, errors.New("-evidence must use lowercase hexadecimal")
	}
	decoded, err := hex.DecodeString(raw)
	if err != nil {
		return out, fmt.Errorf("-evidence is not hexadecimal: %w", err)
	}
	copy(out[:], decoded)
	return out, nil
}

func formatDigest(value [sha256.Size]byte) string {
	return "sha256:" + hex.EncodeToString(value[:])
}

func formatCID(value cid.Cid) string {
	if !value.Defined() {
		return ""
	}
	return value.String()
}

type conflictCandidateReport struct {
	Role              string `json:"role"`
	CheckpointVersion uint8  `json:"checkpoint_version"`
	SourceID          string `json:"source_id"`
	Revision          uint64 `json:"revision"`
	DocumentDigest    string `json:"document_digest"`
	Root              string `json:"root"`
	Covered           bool   `json:"covered"`
	SyncedTo          uint64 `json:"synced_to"`
	Manifest          string `json:"manifest"`
}

type conflictRecordReport struct {
	Head              string                  `json:"head"`
	Sequence          uint64                  `json:"sequence"`
	EvidenceID        string                  `json:"evidence_id"`
	Reason            string                  `json:"reason"`
	SourceSetRevision uint64                  `json:"source_set_revision"`
	SourceSetDigest   string                  `json:"source_set_digest"`
	PairCount         uint16                  `json:"pair_count"`
	Left              conflictCandidateReport `json:"left"`
	Right             conflictCandidateReport `json:"right"`
}

type conflictStatusReport struct {
	Schema    string                 `json:"schema"`
	ArchiveID string                 `json:"archive_id"`
	Conflicts []conflictRecordReport `json:"conflicts"`
}

func newConflictStatusReport(archiveID server.ArchiveID, records []follow.ConflictRecord) conflictStatusReport {
	conflicts := make([]conflictRecordReport, 0, len(records))
	for _, record := range records {
		conflicts = append(conflicts, conflictRecordReport{
			Head: record.Head, Sequence: record.Sequence, EvidenceID: formatDigest(record.EvidenceID),
			Reason: record.Reason.String(), SourceSetRevision: record.SourceSetRevision,
			SourceSetDigest: formatDigest(record.SourceSetDigest), PairCount: record.PairCount,
			Left: candidateReport(record.Left), Right: candidateReport(record.Right),
		})
	}
	return conflictStatusReport{
		Schema: "bloar.conflicts.status/v1", ArchiveID: archiveID.String(), Conflicts: conflicts,
	}
}

func candidateReport(candidate follow.ConflictCandidateSummary) conflictCandidateReport {
	return conflictCandidateReport{
		Role: formatConflictCandidateRole(candidate.Role), CheckpointVersion: candidate.CheckpointVersion,
		SourceID: candidate.SourceID, Revision: candidate.Revision, DocumentDigest: formatOptionalDigest(candidate.Digest),
		Root: formatCID(candidate.Root), Covered: candidate.Covered, SyncedTo: candidate.SyncedTo,
		Manifest: formatCID(candidate.Manifest),
	}
}

func formatOptionalDigest(value [sha256.Size]byte) string {
	if value == ([sha256.Size]byte{}) {
		return ""
	}
	return formatDigest(value)
}

func formatConflictCandidateRole(role follow.ConflictCandidateRole) string {
	switch role {
	case follow.ConflictCandidateSource:
		return "source"
	case follow.ConflictCandidateDurable:
		return "durable_checkpoint"
	default:
		return fmt.Sprintf("unknown_%d", role)
	}
}

func (r conflictStatusReport) human() string {
	var out strings.Builder
	fmt.Fprintf(&out, "archive: %s\n", r.ArchiveID)
	fmt.Fprintf(&out, "active conflict latches: %d\n", len(r.Conflicts))
	for _, record := range r.Conflicts {
		fmt.Fprintf(&out, "\nhead: %s\n", record.Head)
		fmt.Fprintf(&out, "  sequence: %d\n", record.Sequence)
		fmt.Fprintf(&out, "  evidence: %s\n", record.EvidenceID)
		fmt.Fprintf(&out, "  reason: %s\n", record.Reason)
		fmt.Fprintf(&out, "  source set: revision=%d digest=%s\n", record.SourceSetRevision, record.SourceSetDigest)
		fmt.Fprintf(&out, "  conflicting pairs: %d\n", record.PairCount)
		writeCandidateHuman(&out, "left", record.Left)
		writeCandidateHuman(&out, "right", record.Right)
	}
	return out.String()
}

func writeCandidateHuman(out *strings.Builder, side string, candidate conflictCandidateReport) {
	coverage := "none"
	if candidate.Covered {
		coverage = fmt.Sprintf("synced_to=%d", candidate.SyncedTo)
	}
	manifest := candidate.Manifest
	if manifest == "" {
		manifest = "none"
	}
	source := candidate.SourceID
	if source == "" {
		source = "none"
	}
	document := candidate.DocumentDigest
	if document == "" {
		document = "none"
	}
	fmt.Fprintf(out, "  %s: role=%s checkpoint_version=%d source=%s revision=%d document=%s\n",
		side, candidate.Role, candidate.CheckpointVersion, source, candidate.Revision, document)
	fmt.Fprintf(out, "    root=%s coverage=%s manifest=%s\n", candidate.Root, coverage, manifest)
}
