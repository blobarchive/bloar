package chain

// This file is the chain indexer's side of the manifest chain (spec 10.5): the
// conversion between a Source and its DAG-CBOR sibling schema.Source, and the
// append-only check that decides whether a proposed schedule may legally replace
// the one a head's published manifest already attests.

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ipfs/go-cid"

	"github.com/blobarchive/bloar/index/archclient"
	"github.com/blobarchive/bloar/schema"
)

// SourcesFromSchema converts a manifest's decoded sources (schema.Source, byte
// strings) into the go-ethereum-typed Sources the indexer scans with. schema
// owes nothing to this package -- the DAG layer is deliberately free of the
// L1-facing types -- so the mapping lives here, where both types are in scope.
func SourcesFromSchema(sources []schema.Source) ([]Source, error) {
	out := make([]Source, 0, len(sources))
	for i, s := range sources {
		c, err := sourceFromSchema(s)
		if err != nil {
			return nil, fmt.Errorf("chain: manifest source %d: %w", i, err)
		}
		out = append(out, c)
	}
	return out, nil
}

// SourcesToSchema is the inverse, for a caller building a manifest from a
// configured schedule (an audit tool, a test). The indexer itself only reads
// manifests, so it only needs the forward direction.
func SourcesToSchema(sources []Source) []schema.Source {
	out := make([]schema.Source, 0, len(sources))
	for _, s := range sources {
		out = append(out, sourceToSchema(s))
	}
	return out
}

func sourceFromSchema(s schema.Source) (Source, error) {
	if len(s.Address) != common.AddressLength {
		return Source{}, fmt.Errorf("address is %d bytes, want %d", len(s.Address), common.AddressLength)
	}
	out := Source{
		Type:       SourceType(s.Type),
		Address:    common.BytesToAddress(s.Address),
		FromBlock:  s.FromBlock,
		UntilBlock: s.UntilBlock,
		OpenEnded:  s.OpenEnded,
	}
	switch s.Type {
	case schema.SourceInboxEvents:
		if len(s.Topic) != common.HashLength {
			return Source{}, fmt.Errorf("inbox-events topic is %d bytes, want %d", len(s.Topic), common.HashLength)
		}
		out.Topic = common.BytesToHash(s.Topic)
	case schema.SourceBlobTxs:
		for j, snd := range s.Senders {
			if len(snd) != common.AddressLength {
				return Source{}, fmt.Errorf("sender %d is %d bytes, want %d", j, len(snd), common.AddressLength)
			}
			out.Senders = append(out.Senders, common.BytesToAddress(snd))
		}
	default:
		return Source{}, fmt.Errorf("unknown type %q", s.Type)
	}
	return out, nil
}

func sourceToSchema(s Source) schema.Source {
	out := schema.Source{
		Type:       string(s.Type),
		Address:    append([]byte(nil), s.Address.Bytes()...),
		FromBlock:  s.FromBlock,
		UntilBlock: s.UntilBlock,
		OpenEnded:  s.OpenEnded,
	}
	switch s.Type {
	case SourceInboxEvents:
		out.Topic = append([]byte(nil), s.Topic.Bytes()...)
	case SourceBlobTxs:
		for _, snd := range s.Senders {
			out.Senders = append(out.Senders, append([]byte(nil), snd.Bytes()...))
		}
	}
	return out
}

// recoveryOrder is the mechanically-enforced sequence for correcting a rule the
// head's position has already passed (spec 5.4, 10.5). It is quoted in the error
// so an operator who hits an append-only refusal reads the fix, not just the
// rule: manifest-first is what ValidateUpgrade rejects, and truncate-first is what
// makes it pass.
const recoveryOrder = "to change a rule the head's position has already covered, move the position back first: " +
	"truncate the head to before the affected range, publish the corrected manifest, then resync (spec 10.5)"

// ValidateUpgrade reports whether proposed is a legal append-only successor of
// current at position (spec 10.4's immutability rule, 10.5's formal statement of
// it). position is the head's synced_to mapped to an L1 block: the last block
// whose data the head has frozen. Every rule applying to blocks at or behind
// position must be unchanged; only rules strictly ahead of it may differ.
//
// # What "at or behind position" means, and why order matters there
//
// A source has covered ground when it has activated at or before the position
// (from_block <= position). Its covered range is [from_block, min(until_block,
// position)], with an open-ended source covering through the position. Two things
// are frozen about that range: which transactions it selects, and where the
// source sits in the list -- because the head's rows are the sources' union
// deduplicated in list order (spec 10.4), so reordering two covered sources moves
// a shared blob's position and changes the bytes the head already served.
//
// So the covered sources of current and proposed must match one-for-one, in
// order, each with identical covered ground. New sources and reordering are legal
// only wholly ahead of the position, where nothing has been decided yet -- which
// is exactly close-and-add (spec 10.4): cap an open source at a block ahead of the
// position, append the replacement starting after it.
func ValidateUpgrade(current, proposed []Source, position uint64) error {
	cCov := coveredSources(current, position)
	pCov := coveredSources(proposed, position)
	if len(cCov) != len(pCov) {
		return fmt.Errorf("chain: the proposed schedule changes the sources covering L1 block %d or earlier "+
			"(%d covered sources, was %d): a source that has covered ground is immutable (spec 10.4). %s",
			position, len(pCov), len(cCov), recoveryOrder)
	}
	for i := range cCov {
		if err := sameCoveredGround(cCov[i], pCov[i], position); err != nil {
			return fmt.Errorf("chain: the proposed schedule rewrites source %d, which has covered L1 block %d or "+
				"earlier: %w. %s", i, position, err, recoveryOrder)
		}
	}
	return nil
}

// coveredSources filters to the sources that have activated at or before
// position, preserving list order: those are the ones whose ground is frozen.
func coveredSources(sources []Source, position uint64) []Source {
	var out []Source
	for _, s := range sources {
		if s.FromBlock <= position {
			out = append(out, s)
		}
	}
	return out
}

// sameCoveredGround reports whether c and p select the identical set of
// transactions over [from, position] as the same list entry. Everything but the
// upper bound must be equal outright; the upper bound is compared only up to the
// position, so an open-ended source may be capped -- but only at a block ahead of
// the position, which is the one change to a covered source that leaves its
// covered ground untouched (spec 10.4's close-and-add).
func sameCoveredGround(c, p Source, position uint64) error {
	if c.Type != p.Type {
		return fmt.Errorf("type changed from %q to %q", c.Type, p.Type)
	}
	if c.Address != p.Address {
		return fmt.Errorf("address changed from %s to %s", c.Address, p.Address)
	}
	if c.FromBlock != p.FromBlock {
		return fmt.Errorf("from_block changed from %d to %d", c.FromBlock, p.FromBlock)
	}
	switch c.Type {
	case SourceInboxEvents:
		if c.Topic != p.Topic {
			return fmt.Errorf("topic changed from %s to %s", c.Topic, p.Topic)
		}
	case SourceBlobTxs:
		if !sendersEqual(c.Senders, p.Senders) {
			return fmt.Errorf("sender allowlist changed")
		}
	}
	if ce, pe := coveredEnd(c, position), coveredEnd(p, position); ce != pe {
		return fmt.Errorf("the covered range shrank: it now ends at L1 block %d, was %d (an until_block may move only "+
			"while still ahead of the position)", pe, ce)
	}
	return nil
}

// coveredEnd is a source's last covered block relative to position:
// min(until_block, position), with an open-ended source covering through it.
func coveredEnd(s Source, position uint64) uint64 {
	if s.OpenEnded {
		return position
	}
	return min(s.UntilBlock, position)
}

// sendersEqual compares two allowlists as ordered lists: order is part of a
// source's identity (it affects the manifest's CID), so a reordered allowlist is
// a changed source, not the same one.
func sendersEqual(a, b []common.Address) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !bytes.Equal(a[i].Bytes(), b[i].Bytes()) {
			return false
		}
	}
	return true
}

// CheckSchedule verifies the indexer's configured source schedule against the
// head's published manifest chain (spec 10.5) and refuses to run on any
// disagreement. It is run once at startup, before Run, and on success records the
// tip so every refs POST is bound to it.
//
// The check is EXACT equality against the published tip, not append-only
// succession: a chain indexer must run precisely the schedule its tip attests, so
// that the tip it binds each batch to is the schedule those batches were actually
// scanned under. A config that is a legal FUTURE successor of the tip is still
// refused here -- it is rejected until its own manifest is published (via
// PublishManifest, which is where append-only succession is enforced) and the
// config then equals the new tip. This is the startup half of closing the
// point-in-time gap: the runtime schedule can no longer diverge from the tip and
// cross the divergence boundary unchecked.
//
// A head with NO published chain is refused, not run: a chain head's selection is
// only verifiable bound to a manifest (spec 10.5), so without one there is no tip
// to bind refs to and nothing a third party could check. The operator bootstraps
// a genesis manifest (PublishManifest with a null prev) first.
func (ix *Indexer) CheckSchedule(ctx context.Context) error {
	// Close the mutation boundary at the start of every attempt and reopen it only
	// on success (below): a failed check -- a tip that moved to a schedule this
	// config no longer equals, an unreachable archive -- must never leave an earlier
	// verified=true and its tip active, which would keep Step and Run committing
	// against a schedule this attempt could not confirm. This is the
	// single home of that invariant; reconcileManifestTip relies on it.
	ix.verified, ix.manifestTip = false, ""

	info, err := ix.cfg.Archive.Manifest(ctx, ix.cfg.Head)
	if err != nil {
		return fmt.Errorf("chain: reading the published manifest of head %q: %w", ix.cfg.Head, err)
	}
	if info == nil {
		return fmt.Errorf("chain: head %q has no published manifest chain, but a chain indexer's selection is only "+
			"verifiable bound to one. Bootstrap a genesis manifest describing this schedule "+
			"before running the indexer", ix.cfg.Head)
	}
	published, err := SourcesFromSchema(info.Sources)
	if err != nil {
		return fmt.Errorf("chain: the published manifest of head %q does not convert: %w", ix.cfg.Head, err)
	}
	if !schedulesEqual(published, ix.cfg.Sources) {
		return fmt.Errorf("chain: the configured schedule of head %q does not equal its published manifest tip %s. "+
			"A chain indexer must run exactly the tip's schedule (spec 10.5); to change it, publish the new manifest "+
			"first (the append-only preflight, PublishManifest), then restart with a config matching the new tip. %s",
			ix.cfg.Head, info.CID, recoveryOrder)
	}
	// Bind the schedule to this tip and reopen the mutation boundary: manifestTip is
	// what every refs POST carries, and verified is what Step and Run gate on (spec
	// 10.5, the safety boundary). Both are set only here, and cleared at the top of every
	// attempt, so the boundary is open only while an equal-schedule tip holds.
	ix.manifestTip, ix.verified = info.CID, true
	ix.log.Info("configured schedule equals the published manifest tip", "head", ix.cfg.Head, "tip", info.CID)
	return nil
}

// reconcileManifestTip rereads the head's published manifest tip once per poll and
// revalidates the configured schedule against it on any change (spec 10.5, audit
// the safety boundary). Run calls it at the top of every cycle.
//
// The commit-time binding alone (checkManifestBinding on the server) only catches
// a superseded schedule when the indexer next writes refs. A caught-up process
// with no new finalized work never writes, so a manifest upgrade could leave it
// running an old schedule indefinitely, undetected. This closes that: the reread
// is a cheap GET of the tip, and only a change pays for the full re-check.
//
// On a change CheckSchedule re-runs the exact-equality check against the new tip
// and either adopts it -- setting verified and advancing manifestTip to the new
// CID, so subsequent refs bind to it -- or refuses, in which case CheckSchedule has
// already cleared the verified state (it clears at the start of every attempt), so
// nothing downstream can commit; the refusal returns for Run to exit loudly on
// (fail closed).
func (ix *Indexer) reconcileManifestTip(ctx context.Context) error {
	info, err := ix.cfg.Archive.Manifest(ctx, ix.cfg.Head)
	if err != nil {
		return fmt.Errorf("chain: rereading the published manifest tip of head %q: %w", ix.cfg.Head, err)
	}
	if info != nil && info.CID == ix.manifestTip {
		return nil // unchanged; the common per-poll path pays only this one GET.
	}
	// The tip changed under this running process (or the chain vanished, info nil).
	// CheckSchedule closes the boundary before it revalidates, so a failed recheck
	// exits with verified clear and only a matching new tip reopens it.
	if err := ix.CheckSchedule(ctx); err != nil {
		return fmt.Errorf("chain: head %q's published manifest tip changed under this running indexer and the "+
			"configured schedule no longer equals it (spec 10.5): %w", ix.cfg.Head, err)
	}
	ix.log.Info("adopted a changed manifest tip whose schedule still matches the config",
		"head", ix.cfg.Head, "tip", ix.manifestTip)
	return nil
}

// maxPublishAttempts bounds PublishManifest's re-preflight loop. Each retry is
// prompted by a refs commit advancing the head between the preflight and the POST
// , which is a per-poll event, so a
// small bound converges unless refs are landing pathologically fast.
const maxPublishAttempts = 10

// PublishManifest is the append-only preflight and publish of a manifest upgrade
// : the chain-aware, authenticated workflow that is the
// supported way to advance a head's filter, in place of a raw manifest POST. Only
// the indexer sees L1, so only it can run the predecessor-semantic check the
// server cannot -- this validates the new schedule against the DECODED current
// tip at the head's L1 position, then POSTs it bound to the head root that
// position was read from.
//
// newSources is the whole new schedule (spec 10.5: sources is the full list, not
// a delta). A head with no tip yet is the genesis bootstrap: there is no
// predecessor to validate, and the schedule publishes with a null prev.
//
// The generation binding closes the gap between validating and publishing: a refs
// commit landing there advances the head root, the POST's expected_head_root no
// longer matches, the server answers 409, and this re-preflights against the
// advanced head -- no quiescing of refs, bounded by maxPublishAttempts. It returns
// the new tip CID.
func (ix *Indexer) PublishManifest(ctx context.Context, newSources []Source) (string, error) {
	if err := ValidateSources(newSources); err != nil {
		return "", err
	}
	for attempt := 1; ; attempt++ {
		tip, err := ix.preflightManifest(ctx, newSources)
		if err == nil {
			return tip, nil
		}
		var conflict *archclient.ConflictError
		if !errors.As(err, &conflict) || attempt >= maxPublishAttempts {
			return "", err
		}
		ix.log.Info("manifest publish raced a head advance; re-running the append-only preflight",
			"head", ix.cfg.Head, "attempt", attempt, "err", err)
	}
}

// preflightManifest runs one attempt of the preflight and POST. The head's root
// and synced_to are read from the one GET /heads/{head} document, so the L1
// position (from synced_to) and the generation the POST binds to (the root) are
// the same head snapshot; any refs commit after this read is caught by the
// server's generation compare and retried by PublishManifest.
func (ix *Indexer) preflightManifest(ctx context.Context, newSources []Source) (string, error) {
	head, err := ix.cfg.Archive.Head(ctx, ix.cfg.Head)
	if err != nil {
		return "", fmt.Errorf("chain: reading head %q: %w", ix.cfg.Head, err)
	}
	info, err := ix.cfg.Archive.Manifest(ctx, ix.cfg.Head)
	if err != nil {
		return "", fmt.Errorf("chain: reading the published manifest of head %q: %w", ix.cfg.Head, err)
	}

	prev := cid.Undef
	if info != nil {
		published, err := SourcesFromSchema(info.Sources)
		if err != nil {
			return "", fmt.Errorf("chain: the published manifest of head %q does not convert: %w", ix.cfg.Head, err)
		}
		// The append-only check the server cannot make (spec 10.5): the decoded
		// predecessor against the proposed schedule, at the head's true L1 position.
		// An empty head has covered nothing, so there is no position to place and
		// nothing is frozen -- any schedule is a legal successor.
		if head.SyncedTo != nil {
			position, err := ix.positionOfSlot(ctx, *head.SyncedTo)
			if err != nil {
				return "", err
			}
			if err := ValidateUpgrade(published, newSources, position); err != nil {
				return "", fmt.Errorf("chain: the proposed schedule for head %q is not a legal successor of its "+
					"published tip %s at L1 position %d: %w", ix.cfg.Head, info.CID, position, err)
			}
		}
		if prev, err = cid.Decode(info.CID); err != nil {
			return "", fmt.Errorf("chain: the published manifest tip %q of head %q is not a CID: %w",
				info.CID, ix.cfg.Head, err)
		}
	}

	manifest := &schema.Manifest{
		V:       schema.ManifestVersion,
		Head:    ix.cfg.Head,
		Sources: SourcesToSchema(newSources),
		Prev:    prev,
	}
	return ix.cfg.Archive.PostManifest(ctx, ix.cfg.Head, manifest, head.Root)
}

// positionOfSlot maps the head's synced_to slot to the L1 block whose data it
// froze: the last finalized block whose slot is at or below synced_to. It reuses
// the slot-inverse search resume already relies on.
//
// It refuses, rather than guessing, when this L1 node's finalized view lags the
// head's coverage -- when synced_to maps past the node's own finalized tag. The
// honest position is then a block this node cannot see, and clamping it down to
// the finalized tip would under-count what the head has covered and quietly
// weaken the append-only check above (spec 10.5) exactly when the node is behind.
// The refusal is retryable: a node that catches up places the position exactly.
func (ix *Indexer) positionOfSlot(ctx context.Context, syncedTo uint64) (uint64, error) {
	latest, err := ix.cfg.Chain.HeaderByNumber(ctx, ix.finalized)
	if err != nil {
		return 0, fmt.Errorf("chain: reading the finalized L1 header for the manifest position: %w", err)
	}
	if latest == nil || !latest.Number.IsUint64() {
		return 0, errors.New("chain: no finalized L1 block to map the head's position onto")
	}
	latestNum := latest.Number.Uint64()
	firstBeyond, err := ix.blockAtOrAfterSlot(ctx, syncedTo+1, latestNum)
	if err != nil {
		return 0, err
	}
	if firstBeyond == 0 {
		return 0, nil
	}
	if firstBeyond > latestNum {
		// The inverse search found no finalized block past synced_to and clamped to
		// latest+1. That is the head's coverage meeting or overshooting this node's
		// finalized tag, and the two are not the same case. Post-merge L1 slots are
		// strictly increasing in block number, so slot(latest) vs synced_to
		// separates them cleanly: if the finalized block sits exactly at synced_to,
		// latest IS the position and the fall-through returns it; if its slot is
		// BELOW synced_to, the head has frozen data past what this node can see, the
		// true position is a block beyond the finalized tip, and clamping to latest
		// would under-count the covered sources. Refuse rather than guess.
		latestSlot, err := ix.slotOf(latest)
		if err != nil {
			return 0, err
		}
		if latestSlot < syncedTo {
			return 0, fmt.Errorf("chain: this L1 node's finalized view is behind head %q's coverage: its finalized "+
				"block %d is at slot %d, but the head's synced_to is slot %d, so the head has frozen data past this "+
				"node's finalized tag and its L1 position cannot be placed. Retry when this node catches up",
				ix.cfg.Head, latestNum, latestSlot, syncedTo)
		}
	}
	return firstBeyond - 1, nil
}

// schedulesEqual reports whether two schedules are identical entry for entry. It
// is the empty-head check: with nothing covered, only exact equality attests that
// the indexer will build the head the published genesis manifest describes.
func schedulesEqual(a, b []Source) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Type != b[i].Type || a[i].Address != b[i].Address || a[i].FromBlock != b[i].FromBlock ||
			a[i].OpenEnded != b[i].OpenEnded {
			return false
		}
		if !a[i].OpenEnded && a[i].UntilBlock != b[i].UntilBlock {
			return false
		}
		switch a[i].Type {
		case SourceInboxEvents:
			if a[i].Topic != b[i].Topic {
				return false
			}
		case SourceBlobTxs:
			if !sendersEqual(a[i].Senders, b[i].Senders) {
				return false
			}
		}
	}
	return true
}
