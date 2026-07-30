package pinning_test

import (
	"testing"

	"github.com/ipfs/go-cid"

	"github.com/blobarchive/bloar/pinning"
)

// TestManifestChainSurvivesGC is spec 9's guarantee for the manifest chain: a
// head's tip is pinned recursively under every retention mode, and because each
// Manifest links its predecessor the one pin protects the whole chain to genesis.
// The test advances the tip and checks that the old chain stays protected through
// the new tip's prev link, and that GC never sweeps any of it.
func TestManifestChainSurvivesGC(t *testing.T) {
	for _, tc := range []struct {
		name   string
		policy pinning.Policy
		// blobs the policy retains at synced_to 11 (slots 9 and 10 both fall in a
		// 4-slot window, so window keeps them; none keeps no blobs).
		blobs []uint64
	}{
		{"full", pinning.Full(), []uint64{1, 2}},
		{"none", pinning.None(), nil},
		{"window", pinning.Window(slotsDur(4), testSecondsPerSlot), []uint64{1, 2}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			h := f.head("arbitrum-one", tc.policy)
			// A little data, so the head has a real index the manifest sits beside.
			f.apply(h, 11, f.row(9, 1), f.row(10, 2))

			// Genesis, then an upgrade chained to it.
			genesis := f.putManifest("arbitrum-one", cid.Undef, 0)
			upgrade := f.putManifest("arbitrum-one", genesis, 21_000_001)
			f.setManifestTip("arbitrum-one", upgrade)

			// Reconcile takes the recursive tip pin, then GC keeps the whole chain.
			f.reconcileAll()
			pins := f.pins("arbitrum-one")
			if !hasPinAt(pins, pinning.PurposeManifest, upgrade, true) {
				t.Fatalf("no recursive manifest pin on the tip %s", upgrade)
			}
			if hasPinAt(pins, pinning.PurposeManifest, genesis, true) {
				t.Errorf("genesis is pinned directly; only the tip should carry a pin, the chain rides its prev link")
			}

			f.runGC()
			f.expect().index(h).blobs(tc.blobs...).manifests(genesis, upgrade).check()

			// Advance the tip: a new Manifest chained to the old one. The pin swaps
			// to it -- old tip pin removed, new tip pin added -- and the old chain
			// stays protected because the new tip links it.
			advanced := f.putManifest("arbitrum-one", upgrade, 22_000_000)
			f.setManifestTip("arbitrum-one", advanced)
			f.reconcileAll()

			pins = f.pins("arbitrum-one")
			if !hasPinAt(pins, pinning.PurposeManifest, advanced, true) {
				t.Fatalf("no recursive manifest pin on the advanced tip %s", advanced)
			}
			if hasPinAt(pins, pinning.PurposeManifest, upgrade, true) {
				t.Errorf("the previous tip's pin was not dropped after the advance")
			}

			f.runGC()
			// The whole chain, all three manifests, survives: the advanced tip's pin
			// reaches upgrade, whose prev reaches genesis.
			f.expect().index(h).blobs(tc.blobs...).manifests(genesis, upgrade, advanced).check()
		})
	}
}

// TestManifestPinIsNotReconciledAway guards the seam between the reconciler and
// the manifest pin: a head that gains, then keeps, a tip reconciles to a steady
// state -- the pin is added once and never churned, and a second pass with an
// unchanged tip is a no-op for it.
func TestManifestPinIsNotReconciledAway(t *testing.T) {
	f := newFixture(t)
	h := f.head("arbitrum-one", pinning.Full())
	f.apply(h, 11, f.row(9, 1))

	tip := f.putManifest("arbitrum-one", cid.Undef, 0)
	f.setManifestTip("arbitrum-one", tip)

	first := f.reconcileAll()
	if first.Added == 0 {
		t.Fatal("first reconcile added nothing; the manifest pin should have landed")
	}
	// A second pass with nothing changed touches nothing: the manifest pin is not
	// re-added and, crucially, not removed.
	second := f.reconcileAll()
	if second.Added != 0 || second.Removed != 0 {
		t.Fatalf("second reconcile was not a no-op: %+v", second)
	}
	if !hasPinAt(f.pins("arbitrum-one"), pinning.PurposeManifest, tip, true) {
		t.Fatal("the manifest pin was reconciled away")
	}
}
