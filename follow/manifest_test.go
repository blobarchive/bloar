package follow_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"

	"github.com/blobarchive/bloar/catalog"
	"github.com/blobarchive/bloar/follow"
	"github.com/blobarchive/bloar/metrics"
	"github.com/blobarchive/bloar/p2p/pointerhint"
	"github.com/blobarchive/bloar/pinning"
	"github.com/blobarchive/bloar/schema"
)

// hasManifestPin reports whether the head's ledger holds a recursive manifest pin
// on tip.
func hasManifestPin(t *testing.T, led *catalog.Ledger, head string, tip cid.Cid) bool {
	t.Helper()
	entries, err := led.ListAll(t.Context(), head)
	if err != nil {
		t.Fatalf("ListAll(%q): %v", head, err)
	}
	for _, e := range entries {
		if e.Purpose == pinning.PurposeManifest && e.CID == tip && e.Recursive {
			return true
		}
	}
	return false
}

// adoptedTip is the manifest tip the follower has accepted and persisted, which
// is what it republishes and what the reconciler pins.
func (f *follower) adoptedTip() (cid.Cid, bool) {
	f.t.Helper()
	tip, ok, err := f.manifests.Get(f.t.Context(), testHead)
	if err != nil {
		f.t.Fatalf("ManifestStore.Get: %v", err)
	}
	return tip, ok
}

// TestManifestReplicatedAndPinned is the happy path over real bitswap (spec 9,
// 10.5, 11.3): a writer publishes a manifest chain, and its follower fetches the
// whole chain, pins the tip recursively, and republishes it -- and a proper
// extension is adopted while a first tip sets the floor.
func TestManifestReplicatedAndPinned(t *testing.T) {
	w := newWriter(t)
	_, vhs := w.ingestSlot(testOrigin, 1)

	f := newFollower(t, w)
	f.serveHTTP(nil)

	// The writer bootstraps a genesis manifest, and the follower adopts it: a
	// first tip, which sets the floor with no walk.
	genesis := w.setManifest(cid.Undef, 0)
	f.poll()
	if tip, ok := f.adoptedTip(); !ok || tip != genesis {
		t.Fatalf("after genesis poll, adopted tip = %s (ok=%t), want %s", tip, ok, genesis)
	}
	if !f.hasLocally(genesis) {
		t.Errorf("follower did not fetch the genesis manifest block")
	}

	// The writer advances the chain; the follower adopts the extension (its prev
	// walks back through the genesis it holds) and fetches the new tip.
	upgrade := w.setManifest(genesis, 21_000_001)
	f.poll()
	if tip, _ := f.adoptedTip(); tip != upgrade {
		t.Fatalf("after upgrade poll, adopted tip = %s, want %s", tip, upgrade)
	}
	if !f.hasLocally(upgrade) || !f.hasLocally(genesis) {
		t.Errorf("follower is missing part of the manifest chain: upgrade=%t genesis=%t",
			f.hasLocally(upgrade), f.hasLocally(genesis))
	}

	// The reconciler turns the adopted tip into a recursive manifest pin, and GC
	// keeps the whole chain through it.
	f.reconcile()
	led := ledgerOf(f.node)
	if !hasManifestPin(t, led, testHead, upgrade) {
		t.Fatalf("follower did not pin the manifest tip %s recursively", upgrade)
	}
	f.gc()
	if !f.hasLocally(upgrade) || !f.hasLocally(genesis) {
		t.Errorf("GC swept the manifest chain: upgrade=%t genesis=%t", f.hasLocally(upgrade), f.hasLocally(genesis))
	}

	// The follower republishes the tip in its own document, so a follower-of-a-
	// follower would see the same attestation.
	if got := f.manifestInDoc(); got != upgrade.String() {
		t.Errorf("follower republished manifest = %q, want %q", got, upgrade)
	}

	// And it still serves the blobs the head names, unaffected by the manifest
	// chain riding alongside.
	if status, _, _ := f.blobsAt(testOrigin, vhs[0]); status != 200 {
		t.Errorf("blob read after manifest sync: status = %d, want 200", status)
	}
}

// TestFirstManifestAdmissionAcceptsAnExistingValidChain covers a follower which
// joins after more than one schedule version already exists. First-admission
// validation must walk the complete typed chain to genesis, but it must not
// require operators to replay old publications one at a time.
func TestFirstManifestAdmissionAcceptsAnExistingValidChain(t *testing.T) {
	w := newWriter(t)
	w.ingestSlot(testOrigin, 1)
	genesis := w.setManifest(cid.Undef, 0)
	tip := w.setManifest(genesis, 21_000_001)

	f := newFollower(t, w)
	f.poll()

	if adopted, ok := f.adoptedTip(); !ok || adopted != tip {
		t.Fatalf("adopted tip = %s (ok=%t), want %s", adopted, ok, tip)
	}
	for label, c := range map[string]cid.Cid{"genesis": genesis, "tip": tip} {
		if !f.hasLocally(c) {
			t.Errorf("follower is missing validated first-chain %s %s", label, c)
		}
	}
}

// storeSwappedManifest stores a manifest block in the writer's blockstore without
// advancing the chain through the CAS: a block a malicious or forked writer would
// serve as a replacement genesis, one that descends from nothing the follower
// holds. It is served over bitswap like any other block.
func storeSwappedManifest(w *writer, from uint64) cid.Cid {
	w.t.Helper()
	m := &schema.Manifest{
		V:    schema.ManifestVersion,
		Head: testHead,
		Sources: []schema.Source{{
			Type:      schema.SourceInboxEvents,
			Address:   bytes.Repeat([]byte{0x99}, schema.AddressSize),
			Topic:     bytes.Repeat([]byte{0x88}, schema.TopicSize),
			FromBlock: from,
			OpenEnded: true,
		}},
	}
	block, c, err := schema.EncodeManifest(m)
	if err != nil {
		w.t.Fatalf("EncodeManifest: %v", err)
	}
	blk, err := blocks.NewBlockWithCid(block, c)
	if err != nil {
		w.t.Fatalf("framing swapped manifest: %v", err)
	}
	if err := w.store.Blocks().Put(w.t.Context(), blk); err != nil {
		w.t.Fatalf("storing swapped manifest: %v", err)
	}
	return c
}

// TestFirstManifestAdmissionRefusesBeforeEveryRetentionModeMutatesState pins
// the security boundary on the real Poll path. The same signed first tip must be
// schema-refused before a full, window, or none policy can checkpoint it,
// install compatibility mirrors, expose it, or create its recursive manifest
// pin. The fetched invalid block is an unreferenced cache entry; no attacker
// controlled closure becomes retention state.
func TestFirstManifestAdmissionRefusesBeforeEveryRetentionModeMutatesState(t *testing.T) {
	window := pinning.Window(time.Duration(windowSlots*secondsPerSlot)*time.Second, secondsPerSlot)
	policies := []struct {
		name   string
		policy pinning.Policy
	}{
		{name: "full", policy: pinning.Full()},
		{name: "window", policy: window},
		{name: "none", policy: pinning.None()},
	}

	for _, tc := range policies {
		t.Run(tc.name, func(t *testing.T) {
			w := newWriter(t)
			w.ingestSlot(testOrigin, 1)

			// This is a valid Manifest for a different head. A floorless follower
			// must reject its CID before the retention walk can treat it as an
			// arbitrary generic DAG.
			wrongHead := &schema.Manifest{
				V:    schema.ManifestVersion,
				Head: "other",
				Sources: []schema.Source{{
					Type:      schema.SourceInboxEvents,
					Address:   bytes.Repeat([]byte{0x44}, schema.AddressSize),
					Topic:     bytes.Repeat([]byte{0x55}, schema.TopicSize),
					OpenEnded: true,
				}},
			}
			raw, tip, err := schema.EncodeManifest(wrongHead)
			if err != nil {
				t.Fatalf("EncodeManifest: %v", err)
			}
			blk, err := blocks.NewBlockWithCid(raw, tip)
			if err != nil {
				t.Fatalf("framing wrong-head manifest: %v", err)
			}
			if err := w.store.Blocks().Put(t.Context(), blk); err != nil {
				t.Fatalf("storing wrong-head manifest: %v", err)
			}

			d := newDocServer(t)
			f := newFollower(t, w, func(c *follow.Config) {
				c.URL = d.url
				c.Heads = map[string]pinning.Policy{testHead: tc.policy}
			})
			d.set(sign(t, w.key, withManifest(w.unsigned(time.Now()), tip)))

			err = f.pollErr()
			if err == nil || !bytes.Contains([]byte(err.Error()), []byte(`names head "other", want "all"`)) {
				t.Fatalf("Poll error = %v, want wrong-head first-manifest refusal", err)
			}
			if _, _, _, _, ok, err := follow.ReadCheckpoint(f.store.KV(), testHead); err != nil || ok {
				t.Fatalf("checkpoint after refusal: ok=%t err=%v, want absent", ok, err)
			}
			if _, ok, err := f.roots.Get(t.Context(), testHead); err != nil || ok {
				t.Fatalf("root mirror after refusal: ok=%t err=%v, want absent", ok, err)
			}
			if _, ok, err := f.manifests.Get(t.Context(), testHead); err != nil || ok {
				t.Fatalf("manifest mirror after refusal: ok=%t err=%v, want absent", ok, err)
			}
			if _, ok := f.heads.Get(testHead); ok {
				t.Fatal("invalid first manifest exposed a serving head")
			}
			pins, err := ledgerOf(f.node).ListAll(t.Context(), testHead)
			if err != nil {
				t.Fatalf("ListAll: %v", err)
			}
			if len(pins) != 0 {
				t.Fatalf("invalid first manifest created pins: %+v", pins)
			}
		})
	}
}

// TestManifestAncestryRefusesSwap is spec 11.3's manifest-ancestry floor: once a
// follower has accepted a tip, a newly published tip that does not descend from
// it is refused, the same way a synced_to regression is. The document is signed
// by the writer's own key -- a valid claim a real writer would never make -- and
// the block it names is fetchable, so the refusal is the ancestry walk's, not a
// missing block.
func TestManifestAncestryRefusesSwap(t *testing.T) {
	w := newWriter(t)
	w.ingestSlot(testOrigin, 1)
	genesis := w.setManifest(cid.Undef, 0)

	// The follower resolves from a document workshop we control, but fetches blocks
	// over bitswap from the real writer.
	d := newDocServer(t)
	mx := metrics.New()
	findCalls := 0
	f := newLoneFollower(t, w, func(c *follow.Config) {
		c.URL = d.url
		c.Metrics = mx
		c.FindPointer = func(context.Context, pointerhint.Pointer) error {
			findCalls++
			return nil
		}
	})
	f.connect(w)

	// A legitimate document carrying the genesis tip: the follower adopts it and
	// the floor becomes genesis.
	now := time.Now()
	d.set(sign(t, w.key, withManifest(w.unsigned(now), genesis)))
	f.poll()
	if tip, ok := f.adoptedTip(); !ok || tip != genesis {
		t.Fatalf("adopted tip = %s (ok=%t), want the genesis %s", tip, ok, genesis)
	}

	// A swapped chain: an unrelated genesis, published fresher so nothing but the
	// ancestry floor stands between it and adoption.
	swapped := storeSwappedManifest(w, 500)
	d.set(sign(t, w.key, withManifest(w.unsigned(now.Add(time.Minute)), swapped)))

	err := f.pollErr()
	if err == nil {
		t.Fatal("poll accepted a swapped manifest chain")
	}
	if !bytes.Contains([]byte(err.Error()), []byte("does not descend")) {
		t.Fatalf("refusal is not the ancestry floor's: %v", err)
	}
	if findCalls != 0 {
		t.Fatalf("semantic manifest rewrite triggered %d exact-provider lookups, want 0", findCalls)
	}
	if got := refusalCount(t, mx, "manifest_ancestry"); got != 1 {
		t.Fatalf("manifest ancestry refusal count = %g, want one semantic decision", got)
	}
	// The accepted tip is unchanged: the follower goes on attesting the chain it
	// already holds.
	if tip, _ := f.adoptedTip(); tip != genesis {
		t.Errorf("swapped chain moved the adopted tip to %s, want the genesis %s", tip, genesis)
	}
}
