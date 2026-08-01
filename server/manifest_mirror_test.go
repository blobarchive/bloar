package server_test

// The atomic-checkpoint transition requires an exact manifest mirror:
// the manifest compatibility mirror ('m<head>') must be EXACT against the generation
// being adopted. Heads.Adopt used to write it only for a defined tip, so a generation
// with no chain left an older tip in place -- read by the pin reconciler and
// republishable by a later promotion. Adopt now CLEARS the mirror for an undefined
// tip, via ManifestStore.Delete.

import (
	"testing"

	"github.com/ipfs/go-cid"

	"github.com/blobarchive/bloar/archive"
	"github.com/blobarchive/bloar/catalog"
	"github.com/blobarchive/bloar/server"
	"github.com/blobarchive/bloar/store"
)

func TestAdoptClearsManifestMirrorOnUndefinedTip(t *testing.T) {
	ctx := t.Context()
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	defer st.Close()

	cat := catalog.New(st.KV())
	roots := server.NewRootStore(st.KV())
	manifests := server.NewManifestStore(st.KV())
	archiveCfg := archive.Config{Blocks: st.Blocks(), Resolver: cat}
	params := archive.Params{Name: "audit-followed", Net: "auditnet", OriginSlot: 0, SegBits: 2, FanoutBits: 2}

	// Built with archive.New so its root block is durable for adoption without the
	// RootStore having held it: a followed head.
	followed, err := archive.New(ctx, archiveCfg, params)
	if err != nil {
		t.Fatalf("archive.New: %v", err)
	}

	heads, err := server.NewHeads(server.HeadsConfig{Net: "auditnet", Roots: roots, Manifests: manifests, Blocks: st.Blocks()})
	if err != nil {
		t.Fatalf("NewHeads: %v", err)
	}

	// Adopt with a defined manifest tip: the mirror records it. Any defined CID stands
	// in for a tip here -- the mirror stores the CID, it does not load the block.
	tip := followed.Root()
	if err := heads.Adopt(ctx, followed, nil, tip); err != nil {
		t.Fatalf("Adopt(defined tip): %v", err)
	}
	if got, has, err := manifests.Get(ctx, "audit-followed"); err != nil || !has || got != tip {
		t.Fatalf("manifest mirror after adopting a defined tip = %s (has=%t, err=%v), want %s", got, has, err, tip)
	}

	// Re-adopt the same head with an UNDEFINED tip -- a generation with no chain. The
	// mirror must be made exact: the older tip is CLEARED, not left for the pin
	// reconciler or a later promotion to republish.
	if err := heads.Adopt(ctx, followed, nil, cid.Undef); err != nil {
		t.Fatalf("Adopt(undefined tip): %v", err)
	}
	if got, has, err := manifests.Get(ctx, "audit-followed"); err != nil || has {
		t.Errorf("manifest mirror after adopting an undefined tip = %s (has=%t, err=%v), want it cleared", got, has, err)
	}
}
