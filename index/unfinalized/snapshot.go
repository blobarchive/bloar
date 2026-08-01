// Package unfinalized builds complete, bounded archive generations from the
// canonical optimistic beacon chain. It never mutates the finalized ALL head.
package unfinalized

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/blobarchive/bloar/index/archclient"
	"github.com/blobarchive/bloar/index/upstream"
	"github.com/blobarchive/bloar/ingest"
	"github.com/blobarchive/bloar/schema"
)

// HeaderSource is the root-addressed beacon authority needed to take one
// coherent provisional snapshot. *upstream.BlockClient implements it.
type HeaderSource interface {
	Head(context.Context) (upstream.BeaconHeader, bool, error)
	FinalizedHeader(context.Context) (upstream.BeaconHeader, bool, error)
	HeaderByRoot(context.Context, [32]byte) (upstream.BeaconHeader, error)
	CommitmentsByRoot(context.Context, [32]byte) ([][48]byte, error)
}

// CanonicalBlock is one present block in a snapshot. Missing slot numbers
// between adjacent records are authenticated skipped slots; a record with no
// VHs is an authenticated blobless block.
type CanonicalBlock struct {
	Slot       uint64
	Root       [32]byte
	ParentRoot [32]byte
	VHs        []schema.VersionedHash
}

// Snapshot is the complete mutable-head claim for [WindowStart, SyncedTo].
// Rows contains only blob-carrying slots, while Blocks preserves the bounded
// canonical ancestry used to build it and Locations maps missing VHs back to a
// slot from which their bytes can be fetched.
type Snapshot struct {
	WindowStart uint64
	SyncedTo    uint64
	Head        upstream.BeaconHeader
	Finalized   upstream.BeaconHeader
	Blocks      []CanonicalBlock
	Rows        []archclient.Row
	Locations   map[schema.VersionedHash]uint64
}

// ErrHandoffBlocked means the finalized archive has not advanced far enough to
// keep the requested provisional window bounded. The safe action is to retain
// the currently selected generation, not to publish a gap.
var ErrHandoffBlocked = errors.New("unfinalized: finalized handoff is blocked")

// ErrSnapshotChanged means the optimistic source selected another canonical
// head after a candidate was built. It is normal during slot production and
// reorgs: discard the candidate and take another snapshot.
var ErrSnapshotChanged = errors.New("unfinalized: optimistic snapshot changed")

// WindowStart computes the oldest provisional slot retained behind the selected
// finalized ALL frontier. overlap is a count of slots, so overlap=1 retains the
// frontier itself and overlap=0 starts at frontier+1. Arithmetic is saturating.
func WindowStart(networkOrigin, handoffSyncedTo, overlap uint64) uint64 {
	next := handoffSyncedTo
	if next != ^uint64(0) {
		next++
	}
	var low uint64
	if next > overlap {
		low = next - overlap
	}
	if low < networkOrigin {
		return networkOrigin
	}
	return low
}

// Build reads and validates one complete bounded canonical snapshot. Callers
// must re-read Head after Build and require the same root immediately before
// publishing; that final check closes a head change during the walk.
func Build(ctx context.Context, source HeaderSource, windowStart, maxWindowSlots uint64) (Snapshot, error) {
	if source == nil {
		return Snapshot{}, errors.New("unfinalized: nil header source")
	}
	if maxWindowSlots == 0 {
		return Snapshot{}, errors.New("unfinalized: max window slots must be positive")
	}
	// Read the monotonic anchor before the moving tip. Reading these in the other
	// order permits a finality transition between requests to pair an older head
	// with a newer finalized checkpoint that it did not descend from. The source
	// APIs are individually coherent but do not provide a multi-request snapshot.
	finalized, ok, err := source.FinalizedHeader(ctx)
	if err != nil {
		return Snapshot{}, fmt.Errorf("unfinalized: reading finalized header: %w", err)
	}
	if !ok {
		return Snapshot{}, errors.New("unfinalized: finalized header is not available yet")
	}
	if !finalized.Finalized {
		return Snapshot{}, errors.New("unfinalized: finalized endpoint returned a non-finalized header")
	}
	head, ok, err := source.Head(ctx)
	if err != nil {
		return Snapshot{}, fmt.Errorf("unfinalized: reading canonical head: %w", err)
	}
	if !ok {
		return Snapshot{}, errors.New("unfinalized: canonical head is not available yet")
	}
	if finalized.Slot > head.Slot {
		return Snapshot{}, fmt.Errorf("unfinalized: finalized slot %d is above canonical head slot %d", finalized.Slot, head.Slot)
	}
	if windowStart > head.Slot {
		return Snapshot{}, fmt.Errorf("unfinalized: window start %d is above canonical head slot %d", windowStart, head.Slot)
	}
	width := head.Slot - windowStart + 1
	if width > maxWindowSlots {
		return Snapshot{}, fmt.Errorf("%w: canonical window [%d,%d] is %d slots, maximum %d; retain the previous generation until ALL advances",
			ErrHandoffBlocked, windowStart, head.Slot, width, maxWindowSlots)
	}
	reverse := make([]CanonicalBlock, 0, width)
	seen := make(map[[32]byte]struct{}, width)
	cur := head
	foundFinalized := false
	// The finalized checkpoint may sit immediately below the retained window
	// (overlap=0). Continue the root walk far enough to prove it without adding
	// below-window blocks to the snapshot. Two windows is a deliberately bounded
	// proof budget and comfortably covers Ethereum's ordinary ~2-epoch finality
	// distance; exceeding it is a stale/malformed source, not permission to scan
	// without limit.
	maxProofHeaders := maxWindowSlots * 2
	if maxProofHeaders < maxWindowSlots { // overflow
		maxProofHeaders = ^uint64(0)
	}
	var proofHeaders uint64
	for {
		if err := ctx.Err(); err != nil {
			return Snapshot{}, err
		}
		if _, duplicate := seen[cur.Root]; duplicate {
			return Snapshot{}, fmt.Errorf("unfinalized: canonical ancestry loops at root 0x%x", cur.Root)
		}
		seen[cur.Root] = struct{}{}
		if cur.Slot == finalized.Slot {
			if cur.Root != finalized.Root {
				return Snapshot{}, fmt.Errorf("unfinalized: canonical ancestry has root 0x%x at finalized slot %d, want finalized root 0x%x",
					cur.Root, cur.Slot, finalized.Root)
			}
			foundFinalized = true
		}
		if cur.Slot >= windowStart {
			commitments, err := source.CommitmentsByRoot(ctx, cur.Root)
			if err != nil {
				return Snapshot{}, fmt.Errorf("unfinalized: reading commitments for root 0x%x at slot %d: %w", cur.Root, cur.Slot, err)
			}
			block := CanonicalBlock{Slot: cur.Slot, Root: cur.Root, ParentRoot: cur.ParentRoot}
			block.VHs = make([]schema.VersionedHash, len(commitments))
			for i, commitment := range commitments {
				block.VHs[i] = ingest.VersionedHashFromCommitment(commitment)
			}
			reverse = append(reverse, block)
		}
		if foundFinalized && cur.Slot <= windowStart {
			break
		}
		// Once the anchor has been proven, continuing below it is intentional:
		// the retained overlap may start behind the source's current finality.
		// Before it has been proven, crossing the slot is an incoherent snapshot.
		if !foundFinalized && cur.Slot < finalized.Slot {
			return Snapshot{}, fmt.Errorf("unfinalized: canonical ancestry passed below finalized slot %d without root 0x%x",
				finalized.Slot, finalized.Root)
		}
		proofHeaders++
		if proofHeaders > maxProofHeaders {
			return Snapshot{}, fmt.Errorf("unfinalized: canonical ancestry did not reach finalized root within %d headers", maxProofHeaders)
		}

		parent, err := source.HeaderByRoot(ctx, cur.ParentRoot)
		if err != nil {
			return Snapshot{}, fmt.Errorf("unfinalized: reading parent 0x%x of slot %d: %w", cur.ParentRoot, cur.Slot, err)
		}
		if parent.Root != cur.ParentRoot {
			return Snapshot{}, fmt.Errorf("unfinalized: parent lookup for 0x%x returned 0x%x", cur.ParentRoot, parent.Root)
		}
		if parent.Slot >= cur.Slot {
			return Snapshot{}, fmt.Errorf("unfinalized: parent slot %d is not below child slot %d", parent.Slot, cur.Slot)
		}
		cur = parent
	}
	if !foundFinalized {
		return Snapshot{}, fmt.Errorf("unfinalized: canonical ancestry did not contain finalized root 0x%x at slot %d",
			finalized.Root, finalized.Slot)
	}

	slices.Reverse(reverse)
	rows := make([]archclient.Row, 0, len(reverse))
	locations := make(map[schema.VersionedHash]uint64)
	for _, block := range reverse {
		if len(block.VHs) == 0 {
			continue
		}
		vhs := slices.Clone(block.VHs)
		rows = append(rows, archclient.Row{Slot: block.Slot, VHs: vhs})
		for _, vh := range vhs {
			// Prefer the newest occurrence when a content-identical blob appears
			// more than once; a pruning byte source is most likely to retain it.
			locations[vh] = block.Slot
		}
	}
	return Snapshot{
		WindowStart: windowStart,
		SyncedTo:    head.Slot,
		Head:        head,
		Finalized:   finalized,
		Blocks:      reverse,
		Rows:        rows,
		Locations:   locations,
	}, nil
}

// StableHead confirms that the source still selects the root Build walked.
// It is intentionally a separate read immediately before publication.
func StableHead(ctx context.Context, source HeaderSource, want [32]byte) error {
	head, ok, err := source.Head(ctx)
	if err != nil {
		return fmt.Errorf("unfinalized: re-reading canonical head: %w", err)
	}
	if !ok {
		return fmt.Errorf("%w: canonical head became unavailable before publication", ErrSnapshotChanged)
	}
	if head.Root != want {
		return fmt.Errorf("%w: built 0x%x, now 0x%x", ErrSnapshotChanged, want, head.Root)
	}
	return nil
}
