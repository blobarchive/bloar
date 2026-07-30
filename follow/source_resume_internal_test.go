package follow

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/ipfs/boxo/blockstore"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"

	"github.com/blobarchive/bloar/archive"
	"github.com/blobarchive/bloar/catalog"
	"github.com/blobarchive/bloar/pinning"
	"github.com/blobarchive/bloar/replica"
	"github.com/blobarchive/bloar/schema"
	"github.com/blobarchive/bloar/server"
)

type sourceResumeResolver struct{}

func (sourceResumeResolver) ResolveBlob(context.Context, schema.VersionedHash) (cid.Cid, bool, error) {
	return cid.Undef, false, nil
}

type sourceResumeRetention struct {
	protected []replica.Head
}

func (*sourceResumeRetention) Prepare(context.Context, replica.Generation) error { return nil }
func (*sourceResumeRetention) Commit(context.Context, replica.Generation) error  { return nil }
func (r *sourceResumeRetention) ProtectsAll(_ context.Context, heads []replica.Head) error {
	r.protected = append([]replica.Head(nil), heads...)
	return nil
}

func sourceResumeTestBlocks() blockstore.Blockstore {
	return blockstore.NewBlockstore(dssync.MutexWrap(datastore.NewMapDatastore()))
}

func sourceResumeTestEntry(t *testing.T, name string, syncedTo uint64) server.HeadEntry {
	t.Helper()
	return server.HeadEntry{
		Name: name, Root: epochTestCID(t, byte(syncedTo)).String(), OriginSlot: 0, SyncedTo: &syncedTo,
		SegBits: 3, FanoutBits: 2, DirDepth: 1,
	}
}

func sourceResumeTestFollower(st *state, blocks blockstore.Blockstore, archiveID server.ArchiveID,
	set *SourceSetConfig, heads map[string]pinning.Policy, kinds map[string]server.HeadKind,
	sources ...*sourceRuntime,
) *Follower {
	return &Follower{
		cfg: Config{
			Net: checkpointV3TestNet, ExpectedArchiveID: &archiveID, SourceSet: set,
			Heads: heads, ExpectedKinds: kinds,
		},
		log:        slog.New(slog.NewTextHandler(io.Discard, nil)),
		state:      st,
		blocks:     blocks,
		sources:    sources,
		sourceByID: make(map[string]*sourceRuntime),
		heads:      make(map[string]*headState),
	}
}

func sourceResumeRuntime(archiveID server.ArchiveID, sourceID string, pubkey [32]byte, heads ...string) *sourceRuntime {
	allowed := make(map[string]struct{}, len(heads))
	for _, head := range heads {
		allowed[head] = struct{}{}
	}
	return &sourceRuntime{
		cfg: SourceConfig{ID: sourceID, PubKey: append([]byte(nil), pubkey[:]...), AllowedHeads: append([]string(nil), heads...)},
		ref: sourceRef{archiveID: archiveID, sourceID: sourceID}, allowed: allowed,
	}
}

func sourceResumeCommitFloor(t *testing.T, st *state, ref sourceRef, floor sourcePublicationFloor) {
	t.Helper()
	batch := st.kv.NewBatch()
	defer batch.Close()
	if err := st.stageSourcePublicationFloor(batch, ref, floor); err != nil {
		t.Fatalf("staging source publication floor: %v", err)
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		t.Fatalf("committing source publication floor: %v", err)
	}
}

func sourceResumeWireEmbedded(t *testing.T, f *Follower, st *state, bs blockstore.Blockstore) (*server.Heads, *pinning.Reconciler, *server.RootStore, *server.ManifestStore) {
	t.Helper()
	gate := pinning.NewGate()
	roots := server.NewRootStore(st.kv)
	manifests := server.NewManifestStore(st.kv)
	reconciler, err := pinning.NewReconciler(pinning.Config{
		Ledger: catalog.NewLedger(st.kv), Gate: gate, ManifestTip: manifests.Get,
	})
	if err != nil {
		t.Fatal(err)
	}
	registry, err := server.NewHeads(server.HeadsConfig{
		Net: checkpointV3TestNet, Roots: roots, Manifests: manifests, Blocks: bs, Gate: gate,
	})
	if err != nil {
		t.Fatal(err)
	}
	f.cfg.Local = bs
	f.cfg.Registry = registry
	f.cfg.Roots = roots
	f.cfg.Manifests = manifests
	f.cfg.Reconciler = reconciler
	f.cfg.KV = st.kv
	f.gate = gate
	for name, policy := range f.cfg.Heads {
		f.heads[name] = &headState{policy: policy}
	}
	return registry, reconciler, roots, manifests
}

func sourceResumeManifestBlock(t *testing.T, name string, prev cid.Cid, from uint64) blocks.Block {
	t.Helper()
	raw, c, err := schema.EncodeManifest(&schema.Manifest{
		V: schema.ManifestVersion, Head: name, Prev: prev,
		Sources: []schema.Source{{
			Type: schema.SourceInboxEvents, Address: bytes.Repeat([]byte{0x31}, schema.AddressSize),
			Topic: bytes.Repeat([]byte{0x42}, schema.TopicSize), FromBlock: from, OpenEnded: true,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	block, err := blocks.NewBlockWithCid(raw, c)
	if err != nil {
		t.Fatal(err)
	}
	return block
}

func sourceResumeManifestChain(t *testing.T, bs blockstore.Blockstore, name string) (cid.Cid, cid.Cid) {
	t.Helper()
	genesis := sourceResumeManifestBlock(t, name, cid.Undef, 1)
	tip := sourceResumeManifestBlock(t, name, genesis.Cid(), 2)
	if err := bs.PutMany(t.Context(), []blocks.Block{genesis, tip}); err != nil {
		t.Fatal(err)
	}
	return genesis.Cid(), tip.Cid()
}

func TestResumeSourceCheckpointsLeavesLegacyStateDarkUntilV4Provenance(t *testing.T) {
	st := openSourceStateTestDB(t)
	archiveID := sourceStateTestArchive(80)
	digest := sourceStateTestValue(81)
	key := sourceStateTestValue(82)
	binding := sourceBinding{sourceID: "writer-a", pubkey: key}
	activation := sourceStateTestActivation(archiveID, 1, digest, binding)
	if err := st.activateSourceSet(activation); err != nil {
		t.Fatal(err)
	}

	v1Entry := sourceResumeTestEntry(t, "v1-head", 10)
	v1Root, err := cid.Decode(v1Entry.Root)
	if err != nil {
		t.Fatal(err)
	}
	v1 := checkpoint{
		root: v1Root, syncedTo: 10,
		updatedAt: time.Unix(10, 0).UTC(), kind: server.FinalizedMonotonic,
	}
	v2Entry := sourceResumeTestEntry(t, "v2-head", 11)
	v2Root, err := cid.Decode(v2Entry.Root)
	if err != nil {
		t.Fatal(err)
	}
	v2Floor := checkpointV3TestFloor(2)
	v2 := checkpoint{
		root: v2Root, syncedTo: 11, updatedAt: time.Unix(11, 0).UTC(), kind: server.FinalizedMonotonic,
		authority: v2Floor.authority, revision: v2Floor.revision, digest: v2Floor.digest,
	}
	v3Entry := sourceResumeTestEntry(t, "v3-head", 12)
	v3, err := makeCheckpointV3(checkpointV3TestNet, true, &v3Entry, nil, time.Unix(12, 0).UTC(), checkpointV3TestFloor(3))
	if err != nil {
		t.Fatal(err)
	}
	legacy := map[string]checkpoint{"v1-head": v1, "v2-head": v2, "v3-head": v3}
	for name, cp := range legacy {
		if err := st.putCheckpoint(name, cp); err != nil {
			t.Fatal(err)
		}
	}
	heads := map[string]pinning.Policy{
		"v1-head": pinning.Full(), "v2-head": pinning.Full(), "v3-head": pinning.Full(),
	}
	f := sourceResumeTestFollower(st, sourceResumeTestBlocks(), archiveID,
		&SourceSetConfig{Revision: activation.marker.revision, Digest: activation.marker.digest},
		heads, nil)
	for name, policy := range heads {
		f.heads[name] = &headState{policy: policy}
	}
	if err := f.resumeSourceCheckpoints(t.Context()); err != nil {
		t.Fatalf("legacy source-mode resume: %v", err)
	}
	for name := range heads {
		if f.heads[name].adopted.Defined() {
			t.Fatalf("legacy checkpoint %q became served as %s without source attribution", name, f.heads[name].adopted)
		}
	}

	// A fresh source poll would write v4 and its source floor atomically. Prove
	// that this exact provenance crosses the gate which the retained v1 record did
	// not; loading/exposing the root belongs to the integration resume path.
	floor := sourcePublicationFloor{revision: 1, digest: sourceStateTestValue(83)}
	sourceResumeCommitFloor(t, st, sourceRef{archiveID: archiveID, sourceID: binding.sourceID}, floor)
	entry := sourceResumeTestEntry(t, "v1-head", 10)
	cp, err := makeCheckpointV4(checkpointV3TestNet, archiveID, binding.sourceID, true, &entry, nil, nil,
		time.Unix(11, 0).UTC(), authorityFloor{authority: key, revision: floor.revision, digest: floor.digest})
	if err != nil {
		t.Fatal(err)
	}
	if err := f.validateSourceCheckpointProvenance("v1-head", cp); err != nil {
		t.Fatalf("fresh v4 provenance remained dark: %v", err)
	}
}

func TestResumeSourceCheckpointsExternallyProtectsHiddenLegacyGeneration(t *testing.T) {
	st := openSourceStateTestDB(t)
	archiveID := sourceStateTestArchive(84)
	key := sourceStateTestValue(86)
	binding := sourceBinding{sourceID: "writer-a", pubkey: key}
	activation := sourceStateTestActivation(archiveID, 1, sourceStateTestValue(85), binding)
	if err := st.activateSourceSet(activation); err != nil {
		t.Fatal(err)
	}
	floor := sourcePublicationFloor{revision: 1, digest: sourceStateTestValue(87)}
	sourceResumeCommitFloor(t, st, sourceRef{archiveID: archiveID, sourceID: binding.sourceID}, floor)

	legacyEntry := sourceResumeTestEntry(t, "legacy", 10)
	legacyRoot, err := cid.Decode(legacyEntry.Root)
	if err != nil {
		t.Fatal(err)
	}
	legacyTip := epochTestCID(t, 88)
	legacyCP := checkpoint{
		root: legacyRoot, syncedTo: 10, manifestTip: legacyTip, updatedAt: time.Unix(10, 0).UTC(),
		kind: server.FinalizedMonotonic,
	}
	if err := st.putCheckpoint(legacyEntry.Name, legacyCP); err != nil {
		t.Fatal(err)
	}
	bs, v4Entry, _ := sourceResumeArchiveEntries(t)
	v4CP, err := makeCheckpointV4(checkpointV3TestNet, archiveID, binding.sourceID, true, &v4Entry, nil, nil,
		time.Unix(11, 0).UTC(), authorityFloor{authority: key, revision: floor.revision, digest: floor.digest})
	if err != nil {
		t.Fatal(err)
	}
	if err := st.putCheckpoint(v4Entry.Name, v4CP); err != nil {
		t.Fatal(err)
	}

	retention := &sourceResumeRetention{}
	heads := map[string]pinning.Policy{legacyEntry.Name: pinning.Full(), v4Entry.Name: pinning.Full()}
	f := sourceResumeTestFollower(st, bs, archiveID,
		&SourceSetConfig{Revision: activation.marker.revision, Digest: activation.marker.digest},
		heads, nil)
	gate := pinning.NewGate()
	roots := server.NewRootStore(st.kv)
	manifests := server.NewManifestStore(st.kv)
	registry, err := server.NewHeads(server.HeadsConfig{
		Net: checkpointV3TestNet, Roots: roots, Manifests: manifests, Blocks: bs, Gate: gate,
	})
	if err != nil {
		t.Fatal(err)
	}
	f.cfg.Retention = retention
	f.cfg.Local = bs
	f.cfg.Registry = registry
	f.cfg.Roots = roots
	f.cfg.Manifests = manifests
	f.cfg.KV = st.kv
	f.cfg.Gate = gate
	f.gate = gate
	for name, policy := range heads {
		f.heads[name] = &headState{policy: policy}
	}

	if err := f.resumeSourceCheckpoints(t.Context()); err != nil {
		t.Fatalf("mixed source-mode resume: %v", err)
	}
	if len(retention.protected) != 2 {
		t.Fatalf("protected generation = %+v, want hidden legacy plus v4", retention.protected)
	}
	protected := make(map[string]replica.Head, len(retention.protected))
	for _, head := range retention.protected {
		protected[head.Name] = head
	}
	if got := protected[legacyEntry.Name]; got.Root != legacyRoot || got.Manifest != legacyTip || got.SyncedTo != 10 {
		t.Fatalf("protected legacy head = %+v, want root=%s manifest=%s synced_to=10", got, legacyRoot, legacyTip)
	}
	if got := protected[v4Entry.Name]; got.Root != v4CP.root || got.Manifest != v4CP.manifestTip || got.SyncedTo != v4CP.syncedTo {
		t.Fatalf("protected v4 head = %+v, want checkpoint %+v", got, v4CP)
	}
	if _, serving := registry.Get(legacyEntry.Name); serving || f.heads[legacyEntry.Name].adopted.Defined() {
		t.Fatal("hidden legacy checkpoint became served")
	}
	if got, serving := registry.Get(v4Entry.Name); !serving || got.Root() != v4CP.root {
		t.Fatalf("v4 registry root = %v serving=%t, want %s", got, serving, v4CP.root)
	}
}

func TestResumeSourceCheckpointsPinsHiddenLegacyRootAndManifestThroughGC(t *testing.T) {
	st := openSourceStateTestDB(t)
	archiveID := sourceStateTestArchive(87)
	binding := sourceBinding{sourceID: "writer-a", pubkey: sourceStateTestValue(88)}
	activation := sourceStateTestActivation(archiveID, 1, sourceStateTestValue(89), binding)
	if err := st.activateSourceSet(activation); err != nil {
		t.Fatal(err)
	}

	bs, entry, _ := sourceResumeArchiveEntries(t)
	root, err := cid.Decode(entry.Root)
	if err != nil {
		t.Fatal(err)
	}
	genesis, manifest := sourceResumeManifestChain(t, bs, entry.Name)
	cp := checkpoint{
		root: root, syncedTo: *entry.SyncedTo, manifestTip: manifest,
		updatedAt: time.Unix(10, 0).UTC(), kind: server.FinalizedMonotonic,
	}
	if err := st.putCheckpoint(entry.Name, cp); err != nil {
		t.Fatal(err)
	}
	junk := blocks.NewBlock([]byte("unreachable migration test block"))
	if err := bs.Put(t.Context(), junk); err != nil {
		t.Fatal(err)
	}

	f := sourceResumeTestFollower(st, bs, archiveID,
		&SourceSetConfig{Revision: activation.marker.revision, Digest: activation.marker.digest},
		map[string]pinning.Policy{entry.Name: pinning.Full()}, nil)
	registry, reconciler, roots, manifests := sourceResumeWireEmbedded(t, f, st, bs)
	if err := f.resumeSourceCheckpoints(t.Context()); err != nil {
		t.Fatalf("legacy source-mode resume: %v", err)
	}
	if _, serving := registry.Get(entry.Name); serving {
		t.Fatal("legacy checkpoint became served without source attribution")
	}
	if names := reconciler.Names(); len(names) != 1 || names[0] != entry.Name {
		t.Fatalf("retention-only reconciler names = %v, want [%s]", names, entry.Name)
	}
	if got, ok, err := roots.Get(t.Context(), entry.Name); err != nil || !ok || got != root {
		t.Fatalf("repaired root mirror = %s ok=%t err=%v, want %s", got, ok, err, root)
	}
	if got, ok, err := manifests.Get(t.Context(), entry.Name); err != nil || !ok || got != manifest {
		t.Fatalf("repaired manifest mirror = %s ok=%t err=%v, want %s", got, ok, err, manifest)
	}

	gc, err := pinning.NewGC(pinning.GCConfig{Blocks: bs, Reconciler: reconciler})
	if err != nil {
		t.Fatal(err)
	}
	stats, err := gc.Run(t.Context())
	if err != nil {
		t.Fatalf("GC after hidden legacy registration: %v", err)
	}
	if stats.Swept == 0 {
		t.Fatal("GC swept no unreachable fixture block")
	}
	for label, c := range map[string]cid.Cid{"root": root, "manifest tip": manifest, "manifest genesis": genesis} {
		has, err := bs.Has(t.Context(), c)
		if err != nil || !has {
			t.Fatalf("GC lost hidden legacy %s %s: has=%t err=%v", label, c, has, err)
		}
	}
	if has, err := bs.Has(t.Context(), junk.Cid()); err != nil || has {
		t.Fatalf("GC retained unreachable block %s: has=%t err=%v", junk.Cid(), has, err)
	}
}

func TestResumeSourceCheckpointsProtectsValidV4SiblingBeforeReturningFailure(t *testing.T) {
	st := openSourceStateTestDB(t)
	archiveID := sourceStateTestArchive(95)
	key := sourceStateTestValue(96)
	binding := sourceBinding{sourceID: "writer-a", pubkey: key}
	activation := sourceStateTestActivation(archiveID, 1, sourceStateTestValue(97), binding)
	if err := st.activateSourceSet(activation); err != nil {
		t.Fatal(err)
	}
	floor := sourcePublicationFloor{revision: 1, digest: sourceStateTestValue(98)}
	sourceResumeCommitFloor(t, st, sourceRef{archiveID: archiveID, sourceID: binding.sourceID}, floor)

	bs, validEntry, _ := sourceResumeArchiveEntries(t)
	genesis, manifest := sourceResumeManifestChain(t, bs, validEntry.Name)
	validEntry.Manifest = manifest.String()
	missingHead, err := archive.New(t.Context(), archive.Config{Blocks: bs, Resolver: sourceResumeResolver{}}, archive.Params{
		Name: "missing", Net: checkpointV3TestNet, OriginSlot: 0, SegBits: 3, FanoutBits: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := missingHead.ApplyRefs(t.Context(), nil, 10); err != nil {
		t.Fatal(err)
	}
	missingManifestGenesis := sourceResumeManifestBlock(t, "missing", cid.Undef, 1)
	missingManifestTip := sourceResumeManifestBlock(t, "missing", missingManifestGenesis.Cid(), 2)
	// Store the tip but deliberately omit its predecessor. Loading and exact
	// publication validation therefore succeed; only the retained-closure walk
	// fails, after which the valid sibling must still be registered.
	if err := bs.Put(t.Context(), missingManifestTip); err != nil {
		t.Fatal(err)
	}
	missingEntry := finalizedPublicationEntry(missingHead, missingManifestTip.Cid())
	makeV4 := func(entry server.HeadEntry) checkpoint {
		cp, err := makeCheckpointV4(checkpointV3TestNet, archiveID, binding.sourceID, true, &entry, nil, nil,
			time.Unix(10, 0).UTC(), authorityFloor{authority: key, revision: floor.revision, digest: floor.digest})
		if err != nil {
			t.Fatal(err)
		}
		return cp
	}
	validCP, missingCP := makeV4(validEntry), makeV4(missingEntry)
	if err := st.putCheckpoint(validEntry.Name, validCP); err != nil {
		t.Fatal(err)
	}
	if err := st.putCheckpoint(missingEntry.Name, missingCP); err != nil {
		t.Fatal(err)
	}

	f := sourceResumeTestFollower(st, bs, archiveID,
		&SourceSetConfig{Revision: activation.marker.revision, Digest: activation.marker.digest},
		map[string]pinning.Policy{validEntry.Name: pinning.Full(), missingEntry.Name: pinning.Full()}, nil)
	registry, reconciler, _, _ := sourceResumeWireEmbedded(t, f, st, bs)
	err = f.resumeSourceCheckpoints(t.Context())
	if err == nil || !strings.Contains(err.Error(), "missing") {
		t.Fatalf("Resume error = %v, want missing sibling failure", err)
	}
	for _, name := range []string{validEntry.Name, missingEntry.Name} {
		if _, serving := registry.Get(name); serving {
			t.Fatalf("head %q became served despite all-snapshot Resume failure", name)
		}
	}
	if names := reconciler.Names(); len(names) != 1 || names[0] != validEntry.Name {
		t.Fatalf("retention-only reconciler names = %v, want only valid sibling %q", names, validEntry.Name)
	}

	gc, err := pinning.NewGC(pinning.GCConfig{Blocks: bs, Reconciler: reconciler})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := gc.Run(t.Context()); err != nil {
		t.Fatalf("GC after partial v4 Resume failure: %v", err)
	}
	for label, c := range map[string]cid.Cid{"root": validCP.root, "manifest tip": manifest, "manifest genesis": genesis} {
		has, err := bs.Has(t.Context(), c)
		if err != nil || !has {
			t.Fatalf("GC lost valid hidden v4 sibling %s %s: has=%t err=%v", label, c, has, err)
		}
	}
}

func TestValidateSourceCheckpointProvenanceRejectsTrustAndFloorMismatch(t *testing.T) {
	tests := []struct {
		name string
		edit func(*checkpoint, *sourcePublicationFloor)
		want string
	}{
		{name: "archive", edit: func(cp *checkpoint, _ *sourcePublicationFloor) { cp.archiveID = sourceStateTestArchive(91) }, want: "differs from configured archive"},
		{name: "unknown source", edit: func(cp *checkpoint, _ *sourcePublicationFloor) { cp.sourceID = "writer-b" }, want: "not the durable binding"},
		{name: "source binding", edit: func(cp *checkpoint, _ *sourcePublicationFloor) { cp.authority[0] ^= 0xff }, want: "not the durable binding"},
		{name: "missing floor", edit: func(_ *checkpoint, floor *sourcePublicationFloor) { floor.revision = 0 }, want: "not covered"},
		{name: "floor behind", edit: func(cp *checkpoint, floor *sourcePublicationFloor) { cp.revision = floor.revision + 1 }, want: "not covered"},
		{name: "floor digest", edit: func(cp *checkpoint, _ *sourcePublicationFloor) { cp.digest[0] ^= 0xff }, want: "not covered"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st := openSourceStateTestDB(t)
			archiveID := sourceStateTestArchive(90)
			key := sourceStateTestValue(92)
			binding := sourceBinding{sourceID: "writer-a", pubkey: key}
			if err := st.activateSourceSet(sourceStateTestActivation(archiveID, 1, sourceStateTestValue(93), binding)); err != nil {
				t.Fatal(err)
			}
			floor := sourcePublicationFloor{revision: 7, digest: sourceStateTestValue(94)}
			entry := sourceResumeTestEntry(t, "all", 10)
			cp, err := makeCheckpointV4(checkpointV3TestNet, archiveID, binding.sourceID, true, &entry, nil, nil,
				time.Unix(10, 0).UTC(), authorityFloor{authority: key, revision: floor.revision, digest: floor.digest})
			if err != nil {
				t.Fatal(err)
			}
			tt.edit(&cp, &floor)
			if floor.revision != 0 {
				sourceResumeCommitFloor(t, st, sourceRef{archiveID: archiveID, sourceID: binding.sourceID}, floor)
			}
			f := sourceResumeTestFollower(st, sourceResumeTestBlocks(), archiveID, &SourceSetConfig{},
				map[string]pinning.Policy{"all": pinning.Full()}, nil)
			if err := f.validateSourceCheckpointProvenance("all", cp); err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error = %v, want substring %q", err, tt.want)
			}
		})
	}
}

func TestValidateSourceCheckpointProvenanceRetainsFinalizedButNotMutableRetiredSource(t *testing.T) {
	st := openSourceStateTestDB(t)
	archiveID := sourceStateTestArchive(100)
	retiredKey := sourceStateTestValue(101)
	activeKey := sourceStateTestValue(102)
	retired := sourceBinding{sourceID: "retired", pubkey: retiredKey}
	active := sourceBinding{sourceID: "active", pubkey: activeKey}
	if err := st.activateSourceSet(sourceStateTestActivation(archiveID, 1, sourceStateTestValue(103), retired, active)); err != nil {
		t.Fatal(err)
	}
	current := sourceStateTestActivation(archiveID, 2, sourceStateTestValue(104), active)
	if err := st.activateSourceSet(current); err != nil {
		t.Fatal(err)
	}
	floor := sourcePublicationFloor{revision: 3, digest: sourceStateTestValue(105)}
	sourceResumeCommitFloor(t, st, sourceRef{archiveID: archiveID, sourceID: retired.sourceID}, floor)
	activeRuntime := sourceResumeRuntime(archiveID, active.sourceID, active.pubkey, "all", "live")
	f := sourceResumeTestFollower(st, sourceResumeTestBlocks(), archiveID,
		&SourceSetConfig{Revision: current.marker.revision, Digest: current.marker.digest},
		map[string]pinning.Policy{"all": pinning.Full(), "live": pinning.Full()},
		map[string]server.HeadKind{"live": server.UnfinalizedMutable}, activeRuntime)

	finalized := sourceResumeTestEntry(t, "all", 10)
	finalizedCP, err := makeCheckpointV4(checkpointV3TestNet, archiveID, retired.sourceID, true, &finalized, nil, nil,
		time.Unix(10, 0).UTC(), authorityFloor{authority: retiredKey, revision: floor.revision, digest: floor.digest})
	if err != nil {
		t.Fatal(err)
	}
	if err := f.validateSourceCheckpointProvenance("all", finalizedCP); err != nil {
		t.Fatalf("retained finalized last-good from retired source was refused: %v", err)
	}

	handoff, mutable := checkpointV3TestEntries(t)
	mutable.Name = "live"
	mutableCP, err := makeCheckpointV4(checkpointV3TestNet, archiveID, retired.sourceID, true, &mutable, &handoff, nil,
		time.Unix(10, 0).UTC(), authorityFloor{authority: retiredKey, revision: floor.revision, digest: floor.digest})
	if err != nil {
		t.Fatal(err)
	}
	if err := f.validateSourceCheckpointProvenance("live", mutableCP); err == nil || !strings.Contains(err.Error(), "not its unique configured authority") {
		t.Fatalf("retired mutable source error = %v", err)
	}
}

func sourceResumeArchiveEntries(t *testing.T) (blockstore.Blockstore, server.HeadEntry, server.HeadEntry) {
	t.Helper()
	blocks := sourceResumeTestBlocks()
	head, err := archive.New(t.Context(), archive.Config{Blocks: blocks, Resolver: sourceResumeResolver{}}, archive.Params{
		Name: "arb1-filtered", Net: checkpointV3TestNet, OriginSlot: 0, SegBits: 3, FanoutBits: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := head.ApplyRefs(t.Context(), nil, 10); err != nil {
		t.Fatal(err)
	}
	lower := finalizedPublicationEntry(head, cid.Undef)
	if _, err := head.ApplyRefs(t.Context(), nil, 18); err != nil {
		t.Fatal(err)
	}
	higher := finalizedPublicationEntry(head, cid.Undef)
	return blocks, lower, higher
}

func sourceResumeMutableCheckpoint(t *testing.T, archiveID server.ArchiveID, overlay *server.HeadEntry) checkpoint {
	t.Helper()
	handoff, mutable := checkpointV3TestEntries(t)
	mutable.Name = "live"
	cp, err := makeCheckpointV4(checkpointV3TestNet, archiveID, "live-writer", true, &mutable, &handoff, overlay,
		time.Unix(10, 0).UTC(), checkpointV3TestFloor(20))
	if err != nil {
		t.Fatal(err)
	}
	return cp
}

func sourceResumeFinalizedCheckpoint(t *testing.T, archiveID server.ArchiveID, entry server.HeadEntry) checkpoint {
	t.Helper()
	cp, err := makeCheckpointV4(checkpointV3TestNet, archiveID, "finalized-writer", true, &entry, nil, nil,
		time.Unix(10, 0).UTC(), checkpointV3TestFloor(21))
	if err != nil {
		t.Fatal(err)
	}
	return cp
}

func TestValidateResumedMutableBoundariesRequiresOverlayAndDominatingFinalizedClaim(t *testing.T) {
	blocks, lower, higher := sourceResumeArchiveEntries(t)
	archiveID := sourceStateTestArchive(110)
	f := sourceResumeTestFollower(&state{}, blocks, archiveID, &SourceSetConfig{},
		map[string]pinning.Policy{"arb1-filtered": pinning.Full(), "live": pinning.Full()},
		map[string]server.HeadKind{"live": server.UnfinalizedMutable})
	f.cfg.ExpectedHandoffs = map[string]string{"live": "arb1"}
	f.cfg.OverlayFinalizedHeads = map[string]string{"live": "arb1-filtered"}

	missingOverlay := sourceResumeMutableCheckpoint(t, archiveID, nil)
	if err := f.validateResumedMutableBoundaries(t.Context(), map[string]checkpoint{
		"live": missingOverlay, "arb1-filtered": sourceResumeFinalizedCheckpoint(t, archiveID, lower),
	}); err == nil || !strings.Contains(err.Error(), "lacks the exact") {
		t.Fatalf("missing overlay error = %v", err)
	}

	withLowerOverlay := sourceResumeMutableCheckpoint(t, archiveID, &lower)
	if err := f.validateResumedMutableBoundaries(t.Context(), map[string]checkpoint{"live": withLowerOverlay}); err == nil ||
		!strings.Contains(err.Error(), "without selected source-attributed finalized boundary") {
		t.Fatalf("missing finalized boundary error = %v", err)
	}
	withdrawnFinalized := sourceResumeFinalizedCheckpoint(t, archiveID, lower)
	withdrawnFinalized.selected = false
	if err := f.validateResumedMutableBoundaries(t.Context(), map[string]checkpoint{
		"live": withLowerOverlay, "arb1-filtered": withdrawnFinalized,
	}); err == nil || !strings.Contains(err.Error(), "without selected source-attributed finalized boundary") {
		t.Fatalf("withdrawn finalized boundary error = %v", err)
	}

	for _, tt := range []struct {
		name      string
		overlay   server.HeadEntry
		finalized server.HeadEntry
		wantErr   bool
	}{
		{name: "equivalent", overlay: lower, finalized: lower},
		{name: "selected dominates witness", overlay: lower, finalized: higher},
		{name: "selected lags witness", overlay: higher, finalized: lower, wantErr: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			err := f.validateResumedMutableBoundaries(context.Background(), map[string]checkpoint{
				"live":          sourceResumeMutableCheckpoint(t, archiveID, &tt.overlay),
				"arb1-filtered": sourceResumeFinalizedCheckpoint(t, archiveID, tt.finalized),
			})
			if tt.wantErr {
				if err == nil || !strings.Contains(err.Error(), "is not covered") {
					t.Fatalf("error = %v, want non-dominating refusal", err)
				}
			} else if err != nil {
				t.Fatalf("valid resumed boundary: %v", err)
			}
		})
	}
}

func TestSourceRetentionGenerationUsesProspectiveDurableCheckpointClock(t *testing.T) {
	st := openSourceStateTestDB(t)
	archiveID := sourceStateTestArchive(120)
	alpha := sourceResumeTestEntry(t, "alpha", 10)
	beta := sourceResumeTestEntry(t, "beta", 11)
	makeCP := func(entry server.HeadEntry, selected bool, at time.Time, floor authorityFloor) checkpoint {
		t.Helper()
		cp, err := makeCheckpointV4(checkpointV3TestNet, archiveID, "writer-a", selected, &entry, nil, nil, at, floor)
		if err != nil {
			t.Fatal(err)
		}
		return cp
	}
	alphaCP := makeCP(alpha, true, time.Unix(1, 0).UTC(), checkpointV3TestFloor(31))
	oldBetaCP := makeCP(beta, true, time.Unix(9, 0).UTC(), checkpointV3TestFloor(32))
	if err := st.putCheckpoint(alpha.Name, alphaCP); err != nil {
		t.Fatal(err)
	}
	if err := st.putCheckpoint(beta.Name, oldBetaCP); err != nil {
		t.Fatal(err)
	}

	// A later source revision may legitimately carry an older wall clock. The
	// prospective tombstone replaces beta@t9 with beta@t2; retaining t9 in the
	// external generation would be impossible to reconstruct after restart.
	tombstone := makeCP(beta, false, time.Unix(2, 0).UTC(), checkpointV3TestFloor(33))
	f := sourceResumeTestFollower(st, sourceResumeTestBlocks(), archiveID, &SourceSetConfig{},
		map[string]pinning.Policy{alpha.Name: pinning.Full(), beta.Name: pinning.Full()}, nil)
	plan := adoptPlan{name: beta.Name, kind: server.FinalizedMonotonic, cp: tombstone, writeCheckpoint: true, withdraw: true}
	generation, err := f.sourceRetentionGeneration([]adoptPlan{plan}, []sourceDocumentAdmission{{updatedAt: time.Unix(99, 0).UTC()}})
	if err != nil {
		t.Fatal(err)
	}
	if want := time.Unix(2, 0).UTC(); !generation.UpdatedAt.Equal(want) {
		t.Fatalf("prospective UpdatedAt = %s, want reconstructible %s", generation.UpdatedAt, want)
	}
	if len(generation.Heads) != 1 || generation.Heads[0].Name != alpha.Name || generation.Heads[0].Root != alphaCP.root {
		t.Fatalf("prospective heads = %+v, want only alpha", generation.Heads)
	}

	batch := st.kv.NewBatch()
	defer batch.Close()
	if err := st.stageCheckpoint(batch, beta.Name, tombstone); err != nil {
		t.Fatal(err)
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		t.Fatal(err)
	}
	recovered, err := f.sourceRetentionGeneration(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !recovered.UpdatedAt.Equal(generation.UpdatedAt) || len(recovered.Heads) != 1 || recovered.Heads[0] != generation.Heads[0] {
		t.Fatalf("recovered generation = %+v, want prospective %+v", recovered, generation)
	}
}
