package chain

import (
	"context"
	"math/big"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/common"

	"github.com/blobarchive/bloar/schema"
)

var (
	addrInbox  = common.HexToAddress("0x1c479675ad559DC151F6Ec7ed3FbF8ceE79582B6")
	addrEOA    = common.HexToAddress("0x5050505050505050505050505050505050505050")
	addrPoster = common.HexToAddress("0xa4b0000000000000000000000000000000000000")
	topicSBD   = common.HexToHash("0x7394f4a19a13c7b92b5bb71033245305946ef78452f7b4986ac1390b5df4ebd7")
)

func inbox(from uint64, until uint64, open bool) Source {
	return Source{Type: SourceInboxEvents, Address: addrInbox, Topic: topicSBD, FromBlock: from, UntilBlock: until, OpenEnded: open}
}

func blobtxs(from uint64, open bool, senders ...common.Address) Source {
	return Source{Type: SourceBlobTxs, Address: addrEOA, Senders: senders, FromBlock: from, OpenEnded: open}
}

// TestValidateUpgrade is spec 10.5's append-only check: the past a head has
// covered is frozen, the future is open, and close-and-add is the only shape of
// change that only ever adds ranges ahead of the position.
func TestValidateUpgrade(t *testing.T) {
	position := uint64(1000)

	cases := []struct {
		name     string
		current  []Source
		proposed []Source
		wantErr  string // "" means the upgrade is legal
	}{
		{
			name:     "unchanged",
			current:  []Source{inbox(0, 0, true)},
			proposed: []Source{inbox(0, 0, true)},
		},
		{
			name:     "close-and-add ahead of position",
			current:  []Source{inbox(0, 0, true)},
			proposed: []Source{inbox(0, 2000, false), blobtxs(2001, true, addrPoster)},
		},
		{
			name:     "cap an open source exactly at the position boundary",
			current:  []Source{inbox(0, 0, true)},
			proposed: []Source{inbox(0, 1000, false)},
			// coveredEnd is min(until, position); until==position leaves the covered
			// range identical, so this is legal.
		},
		{
			name:     "until moved behind position",
			current:  []Source{inbox(0, 0, true)},
			proposed: []Source{inbox(0, 999, false)},
			wantErr:  "covered range shrank",
		},
		{
			name:     "new source activating behind position",
			current:  []Source{inbox(0, 0, true)},
			proposed: []Source{inbox(0, 0, true), blobtxs(500, true, addrPoster)},
			wantErr:  "covering L1 block 1000 or earlier",
		},
		{
			name:     "covered source removed",
			current:  []Source{inbox(0, 2000, false), blobtxs(0, true, addrPoster)},
			proposed: []Source{inbox(0, 2000, false)},
			wantErr:  "covering L1 block 1000 or earlier",
		},
		{
			name:     "order change behind position",
			current:  []Source{inbox(0, 0, true), blobtxs(0, true, addrPoster)},
			proposed: []Source{blobtxs(0, true, addrPoster), inbox(0, 0, true)},
			wantErr:  "rewrites source 0",
		},
		{
			name:    "order change wholly ahead of position",
			current: []Source{inbox(0, 900, false), blobtxs(2000, false, addrPoster), inbox(3000, 0, true)},
			// The two ahead-of-position sources swap; the covered one (source 0) is
			// unchanged, so this is legal.
			proposed: []Source{inbox(0, 900, false), inbox(3000, 0, true), blobtxs(2000, false, addrPoster)},
		},
		{
			name:     "allowlist changed on a covered source",
			current:  []Source{blobtxs(0, true, addrPoster)},
			proposed: []Source{blobtxs(0, true, addrPoster, addrInbox)},
			wantErr:  "sender allowlist changed",
		},
		{
			name:     "allowlist reordered on a covered source",
			current:  []Source{blobtxs(0, true, addrPoster, addrInbox)},
			proposed: []Source{blobtxs(0, true, addrInbox, addrPoster)},
			wantErr:  "sender allowlist changed",
		},
		{
			name:     "from_block changed on a covered source",
			current:  []Source{inbox(0, 0, true)},
			proposed: []Source{inbox(1, 0, true)},
			// Moving from_block to 1 keeps it covered (1 <= 1000) but changes the
			// range; note it also stays in coveredSources, so this is a rewrite.
			wantErr: "from_block changed",
		},
		{
			name: "nothing covered, any schedule is legal",
			// Every source activates ahead of the position, so nothing is frozen and
			// even a wholly different schedule passes. This is what makes the check
			// vacuous for a head that has covered nothing behind the position.
			current:  []Source{inbox(2000, 0, true)},
			proposed: []Source{blobtxs(3000, true, addrPoster)},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateUpgrade(tc.current, tc.proposed, position)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("want legal upgrade, got %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("want error containing %q, got %v", tc.wantErr, err)
			}
			// Every refusal names the recovery order, so an operator reading the log
			// sees the fix.
			if err != nil && !strings.Contains(err.Error(), "truncate the head") {
				t.Errorf("refusal does not name the recovery order: %v", err)
			}
		})
	}
}

// TestSourceSchemaRoundTrip checks the conversion both directions preserves every
// field, including the type-specific ones and the open-ended flag.
func TestSourceSchemaRoundTrip(t *testing.T) {
	sources := []Source{
		inbox(0, 21_000_000, false),
		blobtxs(21_000_001, true, addrPoster, addrInbox),
	}
	schemaSources := SourcesToSchema(sources)

	// The schema side must itself be a valid manifest source list.
	m := &schema.Manifest{V: schema.ManifestVersion, Head: "arbitrum-one", Sources: schemaSources}
	if err := m.Validate(); err != nil {
		t.Fatalf("converted sources do not form a valid manifest: %v", err)
	}

	back, err := SourcesFromSchema(schemaSources)
	if err != nil {
		t.Fatalf("SourcesFromSchema: %v", err)
	}
	if len(back) != len(sources) {
		t.Fatalf("round trip changed source count: %d != %d", len(back), len(sources))
	}
	for i := range sources {
		if err := sameCoveredGround(sources[i], back[i], 1<<62); err != nil {
			t.Errorf("source %d changed across the round trip: %v", i, err)
		}
		if sources[i].OpenEnded != back[i].OpenEnded {
			t.Errorf("source %d OpenEnded changed: %t != %t", i, back[i].OpenEnded, sources[i].OpenEnded)
		}
	}
}

// buildLinearChain builds a fake parent chain of blocks 0..last where block n
// lands in slot n (timestamp genesis + n*sps), so slot number and block number
// coincide over the built range and the slot<->block inverse is the identity.
// That lets the position tests reason about the finalized tag, the synced_to
// slot, and the resulting L1 position in one unit.
func buildLinearChain(t *testing.T, last uint64) *fakeChain {
	b := newChainBuilder(t)
	for n := uint64(0); n <= last; n++ {
		b.addBlock(n)
	}
	return b.chain()
}

// positionIndexer wires an Indexer to fc with its finalized tag pinned to a
// concrete block, so positionOfSlot reads that block as the node's finalized tip
// instead of the live "finalized" tag the fakeChain cannot resolve.
func positionIndexer(fc *fakeChain, finalized uint64) *Indexer {
	ix := newTestIndexer(fc, []Source{inboxOpen(testInbox, 0)})
	ix.finalized = new(big.Int).SetUint64(finalized)
	return ix
}

// TestPositionOfSlotRefusesUnderNodeLag is the node-lag regression: positionOfSlot must not
// clamp the head's L1 position down to a lagging node's finalized tip. Here the
// node's finalized tip is block 10 (slot 10) but the head's synced_to is slot 20
// -- the head has frozen ten slots of data past what this node can see. The
// inverse search finds no block past slot 20 in [0, 10] and clamps to 11; the OLD
// code returned 11-1 = 10, silently pinning the position to the finalized tip and
// under-counting covered ground. The fix refuses. The manifest preflight
// (PublishManifest, manifest.go) propagates this error straight out of its
// `return err`, so the append-only check fails and the operator retries -- it is
// never a silently-skipped check.
func TestPositionOfSlotRefusesUnderNodeLag(t *testing.T) {
	fc := buildLinearChain(t, 10)
	ix := positionIndexer(fc, 10)

	pos, err := ix.positionOfSlot(context.Background(), 20)
	if err == nil {
		t.Fatalf("positionOfSlot placed position %d while the node's finalized view lagged the head's coverage", pos)
	}
	for _, want := range []string{"behind", "finalized block 10", "synced_to is slot 20", "Retry"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("lag error missing %q: %v", want, err)
		}
	}
}

// TestPositionLagWouldWeakenAppendOnlyGuard grounds the regression in the guard
// it protects. A source activates at L1 block 15; with synced_to at slot 20 the
// head's TRUE position is block 20, so that source has covered ground [15, 20] and
// an in-place rewrite of it (here a changed topic) must be refused.
//
// Under node lag the OLD positionOfSlot clamped the position down to the finalized
// tip, block 10. At position 10 the source (from_block 15) is not yet covered, so
// ValidateUpgrade sees nothing frozen and ACCEPTS the rewrite -- the append-only
// weakening this bug is about. Asserting both positions pins that contrast; the
// fix removes the clamp by refusing positionOfSlot outright (tested above), so
// CheckSchedule never reaches ValidateUpgrade with a clamped position under lag.
func TestPositionLagWouldWeakenAppendOnlyGuard(t *testing.T) {
	current := []Source{inbox(15, 0, true)}
	rewritten := []Source{inbox(15, 0, true)}
	rewritten[0].Topic = common.HexToHash("0xdead")

	if err := ValidateUpgrade(current, rewritten, 20); err == nil {
		t.Fatal("ValidateUpgrade accepted a rewrite of a source covered at the true position 20")
	}
	if err := ValidateUpgrade(current, rewritten, 10); err != nil {
		t.Fatalf("at the clamped position 10 the rewrite should look legal (source not yet covered), got %v", err)
	}
}

// TestPositionOfSlotPlacesWhenNodeCaughtUp is the other side of the fix: a node
// that is level with or ahead of the head's coverage places the position exactly,
// including the exact-boundary case the refusal must not off-by-one into.
func TestPositionOfSlotPlacesWhenNodeCaughtUp(t *testing.T) {
	fc := buildLinearChain(t, 25)

	// The lagging node from the regression has caught up: its finalized tip is now
	// block 25 (slot 25), past the head's synced_to slot 20. The position is placed
	// exactly -- the last block at or below slot 20 is block 20 -- with no refusal.
	ix := positionIndexer(fc, 25)
	pos, err := ix.positionOfSlot(context.Background(), 20)
	if err != nil {
		t.Fatalf("positionOfSlot with a caught-up node: %v", err)
	}
	if pos != 20 {
		t.Fatalf("caught-up position = %d, want 20", pos)
	}

	// The boundary the fix must not off-by-one: synced_to maps EXACTLY to the
	// finalized tip's slot. The inverse search still clamps to latest+1 here, but
	// the finalized block sits AT synced_to, not below it, so this is caught-up,
	// not lag -- the position is the finalized tip, placed without refusal.
	ixBoundary := positionIndexer(fc, 20)
	pos, err = ixBoundary.positionOfSlot(context.Background(), 20)
	if err != nil {
		t.Fatalf("positionOfSlot at the exact finalized boundary refused: %v", err)
	}
	if pos != 20 {
		t.Fatalf("boundary position = %d, want 20", pos)
	}
}
