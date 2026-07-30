package follow_test

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"strings"
	"testing"

	"github.com/ipfs/boxo/blockstore"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	format "github.com/ipfs/go-ipld-format"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/fluent/qp"
	"github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/multiformats/go-multihash"

	"github.com/blobarchive/bloar/archive"
	"github.com/blobarchive/bloar/follow"
	"github.com/blobarchive/bloar/schema"
	"github.com/blobarchive/bloar/server"
)

type equivalenceResolver struct {
	blocks blockstore.Blockstore
	byVH   map[schema.VersionedHash]cid.Cid
}

func (r *equivalenceResolver) ResolveBlob(_ context.Context, vh schema.VersionedHash) (cid.Cid, bool, error) {
	c, ok := r.byVH[vh]
	return c, ok, nil
}

func (r *equivalenceResolver) add(t *testing.T, id uint64) schema.VersionedHash {
	t.Helper()
	var vh schema.VersionedHash
	vh[0] = 1
	binary.BigEndian.PutUint64(vh[24:], id)
	raw := append([]byte("equivalence-blob:"), vh[:]...)
	mh, err := multihash.Sum(raw, multihash.SHA2_256, -1)
	if err != nil {
		t.Fatal(err)
	}
	c := cid.NewCidV1(cid.Raw, mh)
	blk, err := blocks.NewBlockWithCid(raw, c)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.blocks.Put(t.Context(), blk); err != nil {
		t.Fatal(err)
	}
	r.byVH[vh] = c
	return vh
}

type equivalenceRow struct {
	slot uint64
	ids  []uint64
}

type equivalenceStep struct {
	syncedTo uint64
	rows     []equivalenceRow
}

type equivalenceWriter struct {
	blocks blockstore.Blockstore
	head   *archive.Head
}

type equivalenceWriteSpy struct {
	blockstore.Blockstore
	puts int
}

// equivalenceContextIgnoringBlockstore keeps ordinary root reads successful
// under a canceled context so the cancellation test reaches the explicit gate
// in the manifest ancestry walk.
type equivalenceContextIgnoringBlockstore struct{ blockstore.Blockstore }

func (s equivalenceContextIgnoringBlockstore) Get(_ context.Context, c cid.Cid) (blocks.Block, error) {
	return s.Blockstore.Get(context.Background(), c)
}

func (s equivalenceContextIgnoringBlockstore) Has(_ context.Context, c cid.Cid) (bool, error) {
	return s.Blockstore.Has(context.Background(), c)
}

func (s equivalenceContextIgnoringBlockstore) GetSize(_ context.Context, c cid.Cid) (int, error) {
	return s.Blockstore.GetSize(context.Background(), c)
}

func (s *equivalenceWriteSpy) Put(ctx context.Context, block blocks.Block) error {
	s.puts++
	return s.Blockstore.Put(ctx, block)
}

func (s *equivalenceWriteSpy) PutMany(ctx context.Context, batch []blocks.Block) error {
	s.puts += len(batch)
	return s.Blockstore.PutMany(ctx, batch)
}

func newEquivalenceBlockstore() blockstore.Blockstore {
	return blockstore.NewBlockstore(dssync.MutexWrap(datastore.NewMapDatastore()))
}

func buildEquivalenceWriter(t *testing.T, params archive.Params, steps ...equivalenceStep) *equivalenceWriter {
	t.Helper()
	bs := newEquivalenceBlockstore()
	resolver := &equivalenceResolver{blocks: bs, byVH: make(map[schema.VersionedHash]cid.Cid)}
	h, err := archive.New(t.Context(), archive.Config{Blocks: bs, Resolver: resolver}, params)
	if err != nil {
		t.Fatal(err)
	}
	for _, step := range steps {
		rows := make([]archive.RefRow, 0, len(step.rows))
		for _, row := range step.rows {
			vhs := make([]schema.VersionedHash, 0, len(row.ids))
			for _, id := range row.ids {
				vhs = append(vhs, resolver.add(t, id))
			}
			rows = append(rows, archive.RefRow{Slot: row.slot, VHs: vhs})
		}
		if _, err := h.ApplyRefs(t.Context(), rows, step.syncedTo); err != nil {
			t.Fatalf("applying refs through %d: %v", step.syncedTo, err)
		}
	}
	return &equivalenceWriter{blocks: bs, head: h}
}

func copyEquivalenceStores(t *testing.T, sources ...blockstore.Blockstore) blockstore.Blockstore {
	t.Helper()
	dst := newEquivalenceBlockstore()
	for _, source := range sources {
		keys, err := source.AllKeysChan(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		for c := range keys {
			blk, err := source.Get(t.Context(), c)
			if err != nil {
				t.Fatal(err)
			}
			if err := dst.Put(t.Context(), blk); err != nil {
				t.Fatal(err)
			}
		}
	}
	return dst
}

func equivalenceKeyCount(t *testing.T, bs blockstore.Blockstore) int {
	t.Helper()
	keys, err := bs.AllKeysChan(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for range keys {
		count++
	}
	return count
}

func equivalenceArchiveID(seed byte) server.ArchiveID {
	var id server.ArchiveID
	for i := range id {
		id[i] = seed + byte(i)
	}
	return id
}

func signEquivalenceClaim(t *testing.T, key ed25519.PrivateKey, id server.ArchiveID, info archive.Info, manifest cid.Cid, revision uint64, updatedAt string) server.Doc {
	t.Helper()
	archiveID := id
	entry := server.HeadEntry{
		Name:       info.Name,
		Root:       info.Root.String(),
		OriginSlot: info.OriginSlot,
		SyncedTo:   info.SyncedTo,
		SegBits:    info.SegBits,
		FanoutBits: info.FanoutBits,
		DirDepth:   info.DirDepth,
	}
	if manifest.Defined() {
		entry.Manifest = manifest.String()
	}
	doc := server.Doc{Unsigned: server.Unsigned{
		V:         server.LogicalArchiveDocVersion,
		Net:       info.Net,
		ArchiveID: &archiveID,
		UpdatedAt: updatedAt,
		Heads:     []server.HeadEntry{entry},
		Revision:  &revision,
	}}
	canonical, err := doc.Unsigned.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	doc.Pubkey = hex.EncodeToString(key.Public().(ed25519.PublicKey))
	doc.Signature = hex.EncodeToString(ed25519.Sign(key, canonical))
	if err := doc.Verify(); err != nil {
		t.Fatalf("fixture signature: %v", err)
	}
	if err := doc.ValidateContract(); err != nil {
		t.Fatalf("fixture contract: %v", err)
	}
	return doc
}

func equivalenceKey(t *testing.T) ed25519.PrivateKey {
	t.Helper()
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return key
}

func putEquivalenceManifest(t *testing.T, bs blockstore.Blockstore, prev cid.Cid, schedule uint64) cid.Cid {
	t.Helper()
	address := make([]byte, schema.AddressSize)
	topic := make([]byte, schema.TopicSize)
	binary.BigEndian.PutUint64(address[len(address)-8:], schedule+1)
	binary.BigEndian.PutUint64(topic[len(topic)-8:], schedule+1)
	raw, c, err := schema.EncodeManifest(&schema.Manifest{
		V:    schema.ManifestVersion,
		Head: "all",
		Sources: []schema.Source{{
			Type:      schema.SourceInboxEvents,
			Address:   address,
			Topic:     topic,
			FromBlock: schedule,
			OpenEnded: true,
		}},
		Prev: prev,
	})
	if err != nil {
		t.Fatal(err)
	}
	blk, err := blocks.NewBlockWithCid(raw, c)
	if err != nil {
		t.Fatal(err)
	}
	if err := bs.Put(t.Context(), blk); err != nil {
		t.Fatal(err)
	}
	return c
}

func putEquivalenceBytes(t *testing.T, bs blockstore.Blockstore, codec uint64, raw []byte) cid.Cid {
	t.Helper()
	mh, err := multihash.Sum(raw, multihash.SHA2_256, -1)
	if err != nil {
		t.Fatal(err)
	}
	c := cid.NewCidV1(codec, mh)
	blk, err := blocks.NewBlockWithCid(raw, c)
	if err != nil {
		t.Fatal(err)
	}
	if err := bs.Put(t.Context(), blk); err != nil {
		t.Fatal(err)
	}
	return c
}

func putVersionAgnosticManifestLink(t *testing.T, bs blockstore.Blockstore, ancestor cid.Cid) cid.Cid {
	t.Helper()
	n, err := qp.BuildMap(basicnode.Prototype.Map, 2, func(ma datamodel.MapAssembler) {
		qp.MapEntry(ma, "future-version", qp.Int(999))
		qp.MapEntry(ma, "ancestor", qp.Link(cidlink.Link{Cid: ancestor}))
	})
	if err != nil {
		t.Fatal(err)
	}
	var encoded bytes.Buffer
	if err := dagcbor.Encode(n, &encoded); err != nil {
		t.Fatal(err)
	}
	return putEquivalenceBytes(t, bs, cid.DagCBOR, encoded.Bytes())
}

func requireArchiveConflict(t *testing.T, err error) *follow.ArchiveConflictError {
	t.Helper()
	if err == nil {
		t.Fatal("comparison succeeded, want archive conflict")
	}
	var conflict *follow.ArchiveConflictError
	if !errors.As(err, &conflict) {
		t.Fatalf("error type = %T (%v), want *follow.ArchiveConflictError", err, err)
	}
	return conflict
}

func swappedClaimRelation(relation follow.ClaimRelation) follow.ClaimRelation {
	switch relation {
	case follow.LeftClaimDominates:
		return follow.RightClaimDominates
	case follow.RightClaimDominates:
		return follow.LeftClaimDominates
	default:
		return relation
	}
}

func requireSymmetricClaimRelation(t *testing.T, bs blockstore.Blockstore, head string, left, right server.Doc, want follow.ClaimRelation) {
	t.Helper()
	got, err := follow.ClassifyFinalizedClaims(t.Context(), bs, head, left, right)
	if err != nil || got != want {
		t.Fatalf("left/right relation = %s, %v; want %s", got, err, want)
	}
	got, err = follow.ClassifyFinalizedClaims(t.Context(), bs, head, right, left)
	if swapped := swappedClaimRelation(want); err != nil || got != swapped {
		t.Fatalf("right/left relation = %s, %v; want %s", got, err, swapped)
	}
}

func requireSymmetricClaimRelationWithoutWrites(t *testing.T, bs blockstore.Blockstore, head string, left, right server.Doc, want follow.ClaimRelation) {
	t.Helper()
	before := equivalenceKeyCount(t, bs)
	spy := &equivalenceWriteSpy{Blockstore: bs}
	requireSymmetricClaimRelation(t, spy, head, left, right, want)
	if spy.puts != 0 {
		t.Fatalf("claim classification wrote %d block(s) through the live-store boundary", spy.puts)
	}
	if after := equivalenceKeyCount(t, bs); after != before {
		t.Fatalf("claim classification changed live-store key count from %d to %d", before, after)
	}
}

func requireSymmetricArchiveConflict(t *testing.T, bs blockstore.Blockstore, head string, left, right server.Doc) {
	t.Helper()
	forwardRelation, forwardErr := follow.ClassifyFinalizedClaims(t.Context(), bs, head, left, right)
	if forwardRelation != follow.ClaimRelationInvalid {
		t.Fatalf("left/right conflict relation = %s, want invalid", forwardRelation)
	}
	forward := requireArchiveConflict(t, forwardErr)
	reverseRelation, reverseErr := follow.ClassifyFinalizedClaims(t.Context(), bs, head, right, left)
	if reverseRelation != follow.ClaimRelationInvalid {
		t.Fatalf("right/left conflict relation = %s, want invalid", reverseRelation)
	}
	reverse := requireArchiveConflict(t, reverseErr)
	if forward.ArchiveID != reverse.ArchiveID || forward.Head != reverse.Head ||
		forward.ReasonCode != reverse.ReasonCode || forward.Reason != reverse.Reason ||
		forward.LeftRoot != reverse.RightRoot || forward.RightRoot != reverse.LeftRoot ||
		forward.LeftSyncedTo != reverse.RightSyncedTo || forward.RightSyncedTo != reverse.LeftSyncedTo ||
		forward.LeftCovered != reverse.RightCovered || forward.RightCovered != reverse.LeftCovered ||
		forward.LeftManifest != reverse.RightManifest || forward.RightManifest != reverse.LeftManifest {
		t.Fatalf("swapped conflict evidence is not symmetric:\nforward=%+v\nreverse=%+v", forward, reverse)
	}
}

func requireSymmetricOrdinaryError(t *testing.T, ctx context.Context, bs blockstore.Blockstore, head string, left, right server.Doc, matches func(error) bool) {
	t.Helper()
	for _, pair := range [][2]server.Doc{{left, right}, {right, left}} {
		relation, err := follow.ClassifyFinalizedClaims(ctx, bs, head, pair[0], pair[1])
		if relation != follow.ClaimRelationInvalid || err == nil {
			t.Fatalf("refused comparison = %s, %v; want invalid relation and ordinary error", relation, err)
		}
		var conflict *follow.ArchiveConflictError
		if errors.As(err, &conflict) {
			t.Fatalf("ordinary refusal was reported as archive conflict: %v", err)
		}
		if matches != nil && !matches(err) {
			t.Fatalf("ordinary error %v does not preserve the expected cause", err)
		}
	}
}

func requireSymmetricTransient(t *testing.T, ctx context.Context, bs blockstore.Blockstore, head string, left, right server.Doc, matches func(error) bool) {
	t.Helper()
	requireSymmetricOrdinaryError(t, ctx, bs, head, left, right, matches)
}

func requireSymmetricTransientWithoutWrites(t *testing.T, ctx context.Context, bs blockstore.Blockstore, head string, left, right server.Doc, matches func(error) bool) {
	t.Helper()
	before := equivalenceKeyCount(t, bs)
	spy := &equivalenceWriteSpy{Blockstore: bs}
	requireSymmetricTransient(t, ctx, spy, head, left, right, matches)
	if spy.puts != 0 {
		t.Fatalf("failed proof wrote %d block(s) through the live-store boundary", spy.puts)
	}
	if after := equivalenceKeyCount(t, bs); after != before {
		t.Fatalf("failed proof changed live-store key count from %d to %d", before, after)
	}
}

func TestFinalizedClaimCompositionMatrix(t *testing.T) {
	params := archive.Params{Name: "all", Net: "equivalence-net", OriginSlot: 96, SegBits: 3, FanoutBits: 2}
	lower := buildEquivalenceWriter(t, params,
		equivalenceStep{syncedTo: 103, rows: []equivalenceRow{{slot: 98, ids: []uint64{1}}}})
	higher := buildEquivalenceWriter(t, params,
		equivalenceStep{syncedTo: 103, rows: []equivalenceRow{{slot: 98, ids: []uint64{1}}}},
		equivalenceStep{syncedTo: 119, rows: []equivalenceRow{{slot: 107, ids: []uint64{2}}, {slot: 116, ids: []uint64{3}}}})
	id := equivalenceArchiveID(11)
	leftKey, rightKey := equivalenceKey(t), equivalenceKey(t)
	genesisLower := putEquivalenceManifest(t, lower.blocks, cid.Undef, 10)
	genesisHigher := putEquivalenceManifest(t, higher.blocks, cid.Undef, 10)
	if genesisLower != genesisHigher {
		t.Fatalf("independent common genesis differs: %s != %s", genesisLower, genesisHigher)
	}
	lowerBranch := putEquivalenceManifest(t, lower.blocks, genesisLower, 20)
	higherBranch := putEquivalenceManifest(t, higher.blocks, genesisHigher, 30)
	union := copyEquivalenceStores(t, lower.blocks, higher.blocks)
	doc := func(key ed25519.PrivateKey, writer *equivalenceWriter, manifest cid.Cid) server.Doc {
		return signEquivalenceClaim(t, key, id, writer.head.Info(), manifest, 1, "2026-07-22T00:00:00Z")
	}

	t.Run("root advance with both manifests absent", func(t *testing.T) {
		requireSymmetricClaimRelationWithoutWrites(t, union, "all",
			doc(leftKey, higher, cid.Undef), doc(rightKey, lower, cid.Undef), follow.LeftClaimDominates)
	})
	t.Run("root advance with equal manifest", func(t *testing.T) {
		requireSymmetricClaimRelationWithoutWrites(t, union, "all",
			doc(leftKey, higher, genesisHigher), doc(rightKey, lower, genesisLower), follow.LeftClaimDominates)
	})
	t.Run("root advance with one manifest present", func(t *testing.T) {
		requireSymmetricClaimRelationWithoutWrites(t, union, "all",
			doc(leftKey, higher, genesisHigher), doc(rightKey, lower, cid.Undef), follow.ClaimsIncomparable)
	})
	t.Run("root advance with divergent manifests", func(t *testing.T) {
		requireSymmetricArchiveConflict(t, union, "all",
			doc(leftKey, higher, higherBranch), doc(rightKey, lower, lowerBranch))
	})
	t.Run("equal empty generations", func(t *testing.T) {
		emptyLeft := buildEquivalenceWriter(t, params)
		emptyRight := buildEquivalenceWriter(t, params)
		if emptyLeft.head.Root() != emptyRight.head.Root() {
			t.Fatalf("independent empty roots differ: %s != %s", emptyLeft.head.Root(), emptyRight.head.Root())
		}
		emptyUnion := copyEquivalenceStores(t, emptyLeft.blocks, emptyRight.blocks)
		requireSymmetricClaimRelationWithoutWrites(t, emptyUnion, "all",
			doc(leftKey, emptyLeft, cid.Undef), doc(rightKey, emptyRight, cid.Undef), follow.ClaimsEquivalent)
	})
}

func TestFinalizedClaimProjectionBoundariesAreSymmetricAndWriteIsolated(t *testing.T) {
	id := equivalenceArchiveID(21)
	leftKey, rightKey := equivalenceKey(t), equivalenceKey(t)

	exercise := func(t *testing.T, params archive.Params, lowerSteps, higherSteps []equivalenceStep, checkDepthShrink bool) {
		t.Helper()
		lower := buildEquivalenceWriter(t, params, lowerSteps...)
		higher := buildEquivalenceWriter(t, params, higherSteps...)
		if checkDepthShrink && higher.head.Info().DirDepth <= lower.head.Info().DirDepth {
			t.Fatalf("fixture does not shrink directory depth: higher=%d lower=%d",
				higher.head.Info().DirDepth, lower.head.Info().DirDepth)
		}
		union := copyEquivalenceStores(t, lower.blocks, higher.blocks)
		lowerDoc := signEquivalenceClaim(t, leftKey, id, lower.head.Info(), cid.Undef, 50, "2099-01-01T00:00:00Z")
		higherDoc := signEquivalenceClaim(t, rightKey, id, higher.head.Info(), cid.Undef, 1, "1990-01-01T00:00:00Z")
		requireSymmetricClaimRelationWithoutWrites(t, union, params.Name, higherDoc, lowerDoc, follow.LeftClaimDominates)
	}

	t.Run("project to empty", func(t *testing.T) {
		params := archive.Params{Name: "all", Net: "projection-net", OriginSlot: 96, SegBits: 3, FanoutBits: 2}
		exercise(t, params, nil, []equivalenceStep{{
			syncedTo: 101, rows: []equivalenceRow{{slot: 98, ids: []uint64{1}}},
		}}, false)
	})
	t.Run("project within open segment", func(t *testing.T) {
		params := archive.Params{Name: "all", Net: "projection-net", OriginSlot: 96, SegBits: 3, FanoutBits: 2}
		prefix := equivalenceStep{syncedTo: 100, rows: []equivalenceRow{{slot: 98, ids: []uint64{1}}}}
		exercise(t, params, []equivalenceStep{prefix}, []equivalenceStep{prefix, {
			syncedTo: 102, rows: []equivalenceRow{{slot: 102, ids: []uint64{2}}},
		}}, false)
	})
	t.Run("non-aligned origin", func(t *testing.T) {
		params := archive.Params{Name: "all", Net: "projection-net", OriginSlot: 99, SegBits: 3, FanoutBits: 2}
		prefix := equivalenceStep{syncedTo: 102, rows: []equivalenceRow{{slot: 100, ids: []uint64{1}}}}
		exercise(t, params, []equivalenceStep{prefix}, []equivalenceStep{prefix, {
			syncedTo: 111, rows: []equivalenceRow{{slot: 107, ids: []uint64{2}}},
		}}, false)
	})
	t.Run("directory shrink", func(t *testing.T) {
		params := archive.Params{Name: "all", Net: "projection-net", OriginSlot: 3, SegBits: 1, FanoutBits: 1}
		prefix := equivalenceStep{syncedTo: 6, rows: []equivalenceRow{{slot: 3, ids: []uint64{1}}, {slot: 5, ids: []uint64{2}}}}
		higher := []equivalenceStep{prefix,
			{syncedTo: 14, rows: []equivalenceRow{{slot: 9, ids: []uint64{3}}, {slot: 13, ids: []uint64{4}}}},
			{syncedTo: 30, rows: []equivalenceRow{{slot: 17, ids: []uint64{5}}, {slot: 25, ids: []uint64{6}}}},
			{syncedTo: 46, rows: []equivalenceRow{{slot: 33, ids: []uint64{7}}, {slot: 41, ids: []uint64{8}}}},
		}
		exercise(t, params, []equivalenceStep{prefix}, higher, true)
	})
}

func TestFinalizedClaimProofGapsStayTransient(t *testing.T) {
	params := archive.Params{Name: "all", Net: "proof-gap-net", OriginSlot: 96, SegBits: 3, FanoutBits: 2}
	prefix := equivalenceStep{syncedTo: 103, rows: []equivalenceRow{{slot: 98, ids: []uint64{1}}}}
	lower := buildEquivalenceWriter(t, params, prefix)
	higher := buildEquivalenceWriter(t, params, prefix,
		equivalenceStep{syncedTo: 119, rows: []equivalenceRow{{slot: 107, ids: []uint64{2}}, {slot: 116, ids: []uint64{3}}}})
	id := equivalenceArchiveID(31)
	leftKey, rightKey := equivalenceKey(t), equivalenceKey(t)
	doc := func(key ed25519.PrivateKey, writer *equivalenceWriter, manifest cid.Cid) server.Doc {
		return signEquivalenceClaim(t, key, id, writer.head.Info(), manifest, 1, "2026-07-22T00:00:00Z")
	}

	t.Run("missing projection descendant", func(t *testing.T) {
		union := copyEquivalenceStores(t, lower.blocks, higher.blocks)
		enumerated, err := higher.head.Enumerate(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if !enumerated.Open.Defined() {
			t.Fatal("higher fixture has no open segment")
		}
		if err := union.DeleteBlock(t.Context(), enumerated.Open); err != nil {
			t.Fatal(err)
		}
		requireSymmetricTransientWithoutWrites(t, t.Context(), union, "all",
			doc(leftKey, higher, cid.Undef), doc(rightKey, lower, cid.Undef),
			func(err error) bool { return errors.Is(err, format.ErrNotFound{}) })
	})

	t.Run("missing intermediate manifest", func(t *testing.T) {
		chain := newEquivalenceBlockstore()
		genesis := putEquivalenceManifest(t, chain, cid.Undef, 10)
		middle := putEquivalenceManifest(t, chain, genesis, 20)
		tip := putEquivalenceManifest(t, chain, middle, 30)
		union := copyEquivalenceStores(t, lower.blocks, chain)
		if err := union.DeleteBlock(t.Context(), middle); err != nil {
			t.Fatal(err)
		}
		requireSymmetricTransientWithoutWrites(t, t.Context(), union, "all",
			doc(leftKey, lower, tip), doc(rightKey, lower, genesis),
			func(err error) bool { return errors.Is(err, format.ErrNotFound{}) })
	})

	t.Run("descendant link proves an unavailable ancestor", func(t *testing.T) {
		chain := newEquivalenceBlockstore()
		genesis := putEquivalenceManifest(t, chain, cid.Undef, 10)
		middle := putEquivalenceManifest(t, chain, genesis, 20)
		tip := putEquivalenceManifest(t, chain, middle, 30)
		union := copyEquivalenceStores(t, lower.blocks, chain)
		if err := union.DeleteBlock(t.Context(), middle); err != nil {
			t.Fatal(err)
		}
		requireSymmetricClaimRelation(t, union, "all",
			doc(leftKey, lower, tip), doc(rightKey, lower, middle), follow.LeftClaimDominates)
	})

	t.Run("generic future manifest link remains version agnostic", func(t *testing.T) {
		chain := newEquivalenceBlockstore()
		ancestor := putEquivalenceManifest(t, chain, cid.Undef, 10)
		futureTip := putVersionAgnosticManifestLink(t, chain, ancestor)
		union := copyEquivalenceStores(t, lower.blocks, chain)
		if err := union.DeleteBlock(t.Context(), ancestor); err != nil {
			t.Fatal(err)
		}
		requireSymmetricClaimRelation(t, union, "all",
			doc(leftKey, lower, futureTip), doc(rightKey, lower, ancestor), follow.LeftClaimDominates)
	})

	t.Run("equal unavailable manifest CID is cryptographic equality", func(t *testing.T) {
		unavailableStore := newEquivalenceBlockstore()
		unavailable := putEquivalenceManifest(t, unavailableStore, cid.Undef, 99)
		// Only the signed CID is shared. Equality needs no block fetch because a
		// CID already commits to the exact same history tip on both sides.
		union := copyEquivalenceStores(t, lower.blocks)
		requireSymmetricClaimRelation(t, union, "all",
			doc(leftKey, lower, unavailable), doc(rightKey, lower, unavailable), follow.ClaimsEquivalent)
	})

	t.Run("canceled ancestry walk", func(t *testing.T) {
		chain := newEquivalenceBlockstore()
		genesis := putEquivalenceManifest(t, chain, cid.Undef, 10)
		tip := putEquivalenceManifest(t, chain, genesis, 20)
		union := equivalenceContextIgnoringBlockstore{Blockstore: copyEquivalenceStores(t, lower.blocks, chain)}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		requireSymmetricTransient(t, ctx, union, "all",
			doc(leftKey, lower, tip), doc(rightKey, lower, genesis),
			func(err error) bool { return errors.Is(err, context.Canceled) })
	})

	t.Run("wrong-codec manifest is invalid source evidence", func(t *testing.T) {
		chain := newEquivalenceBlockstore()
		genesis := putEquivalenceManifest(t, chain, cid.Undef, 10)
		wrongCodec := putEquivalenceBytes(t, chain, cid.Raw, []byte("not a manifest"))
		union := copyEquivalenceStores(t, lower.blocks, chain)
		requireSymmetricOrdinaryError(t, t.Context(), union, "all",
			doc(leftKey, lower, wrongCodec), doc(rightKey, lower, genesis),
			func(err error) bool { return strings.Contains(err.Error(), "not a dag-cbor block") })
	})

	t.Run("malformed dag-cbor manifest is invalid source evidence", func(t *testing.T) {
		chain := newEquivalenceBlockstore()
		genesis := putEquivalenceManifest(t, chain, cid.Undef, 10)
		malformed := putEquivalenceBytes(t, chain, cid.DagCBOR, []byte{0xff})
		union := copyEquivalenceStores(t, lower.blocks, chain)
		requireSymmetricOrdinaryError(t, t.Context(), union, "all",
			doc(leftKey, lower, malformed), doc(rightKey, lower, genesis),
			func(err error) bool { return strings.Contains(err.Error(), "decoding block") })
	})
}

func TestFinalizedClaimImmutableIdentityTuple(t *testing.T) {
	baseParams := archive.Params{Name: "all", Net: "identity-net", OriginSlot: 96, SegBits: 3, FanoutBits: 2}
	base := buildEquivalenceWriter(t, baseParams, equivalenceStep{syncedTo: 103})
	id := equivalenceArchiveID(41)
	leftKey, rightKey := equivalenceKey(t), equivalenceKey(t)
	baseDoc := signEquivalenceClaim(t, leftKey, id, base.head.Info(), cid.Undef, 1, "2026-07-22T00:00:00Z")

	t.Run("archive ID", func(t *testing.T) {
		otherDoc := signEquivalenceClaim(t, rightKey, equivalenceArchiveID(42), base.head.Info(), cid.Undef, 1, "2026-07-22T00:00:00Z")
		requireSymmetricClaimRelation(t, copyEquivalenceStores(t, base.blocks), "all", baseDoc, otherDoc, follow.ClaimsIncomparable)
	})

	variants := []struct {
		name   string
		params archive.Params
	}{
		{"network", archive.Params{Name: "all", Net: "other-net", OriginSlot: 96, SegBits: 3, FanoutBits: 2}},
		{"origin slot", archive.Params{Name: "all", Net: "identity-net", OriginSlot: 97, SegBits: 3, FanoutBits: 2}},
		{"segment bits", archive.Params{Name: "all", Net: "identity-net", OriginSlot: 96, SegBits: 2, FanoutBits: 2}},
		{"fanout bits", archive.Params{Name: "all", Net: "identity-net", OriginSlot: 96, SegBits: 3, FanoutBits: 3}},
	}
	for _, variant := range variants {
		t.Run(variant.name, func(t *testing.T) {
			other := buildEquivalenceWriter(t, variant.params, equivalenceStep{syncedTo: 103})
			union := copyEquivalenceStores(t, base.blocks, other.blocks)
			otherDoc := signEquivalenceClaim(t, rightKey, id, other.head.Info(), cid.Undef, 1, "2026-07-22T00:00:00Z")
			requireSymmetricClaimRelation(t, union, "all", baseDoc, otherDoc, follow.ClaimsIncomparable)
		})
	}

	t.Run("explicit and omitted finalized kind are equivalent", func(t *testing.T) {
		explicit := signEquivalenceClaim(t, rightKey, id, base.head.Info(), cid.Undef, 1, "2026-07-22T00:00:00Z")
		explicit.Heads[0].Kind = server.FinalizedMonotonic
		canonical, err := explicit.Unsigned.Canonical()
		if err != nil {
			t.Fatal(err)
		}
		explicit.Signature = hex.EncodeToString(ed25519.Sign(rightKey, canonical))
		requireSymmetricClaimRelation(t, copyEquivalenceStores(t, base.blocks), "all", baseDoc, explicit, follow.ClaimsEquivalent)
	})

	t.Run("different head name is not silently compared", func(t *testing.T) {
		otherParams := baseParams
		otherParams.Name = "other"
		other := buildEquivalenceWriter(t, otherParams, equivalenceStep{syncedTo: 103})
		otherDoc := signEquivalenceClaim(t, rightKey, id, other.head.Info(), cid.Undef, 1, "2026-07-22T00:00:00Z")
		union := copyEquivalenceStores(t, base.blocks, other.blocks)
		requireSymmetricOrdinaryError(t, t.Context(), union, "all", baseDoc, otherDoc,
			func(err error) bool { return strings.Contains(err.Error(), "does not publish head") })
	})
}

func TestFinalizedClaimEquivalenceAndDominance(t *testing.T) {
	params := archive.Params{Name: "all", Net: "equivalence-net", OriginSlot: 96, SegBits: 3, FanoutBits: 2}
	prefixRows := []equivalenceRow{{slot: 98, ids: []uint64{1}}, {slot: 107, ids: []uint64{2}}}

	// These writers use independent stores and different batching, but the same
	// ordered input. Their content-addressed roots must converge exactly.
	left := buildEquivalenceWriter(t, params, equivalenceStep{syncedTo: 111, rows: prefixRows})
	right := buildEquivalenceWriter(t, params,
		equivalenceStep{syncedTo: 103, rows: prefixRows[:1]},
		equivalenceStep{syncedTo: 111, rows: prefixRows[1:]})
	if left.head.Root() != right.head.Root() {
		t.Fatalf("independent same-input roots differ: %s != %s", left.head.Root(), right.head.Root())
	}

	leftGenesis := putEquivalenceManifest(t, left.blocks, cid.Undef, 10)
	rightGenesis := putEquivalenceManifest(t, right.blocks, cid.Undef, 10)
	if leftGenesis != rightGenesis {
		t.Fatalf("independently encoded manifests differ: %s != %s", leftGenesis, rightGenesis)
	}

	id := equivalenceArchiveID(1)
	leftDoc := signEquivalenceClaim(t, equivalenceKey(t), id, left.head.Info(), leftGenesis, 1, "2099-01-01T00:00:00Z")
	rightDoc := signEquivalenceClaim(t, equivalenceKey(t), id, right.head.Info(), rightGenesis, 900, "2000-01-01T00:00:00Z")
	union := copyEquivalenceStores(t, left.blocks, right.blocks)
	relation, err := follow.ClassifyFinalizedClaims(t.Context(), union, "all", leftDoc, rightDoc)
	if err != nil || relation != follow.ClaimsEquivalent {
		t.Fatalf("same-input independent claims = %s, %v; want equivalent", relation, err)
	}
	rightTip := putEquivalenceManifest(t, right.blocks, rightGenesis, 20)
	rightManifestAdvance := signEquivalenceClaim(t, equivalenceKey(t), id, right.head.Info(), rightTip, 1, "1980-01-01T00:00:00Z")
	union = copyEquivalenceStores(t, left.blocks, right.blocks)
	relation, err = follow.ClassifyFinalizedClaims(t.Context(), union, "all", leftDoc, rightManifestAdvance)
	if err != nil || relation != follow.RightClaimDominates {
		t.Fatalf("same root with descendant manifest on right = %s, %v; want right-dominates", relation, err)
	}

	// Extend a third independent writer from the same prefix. Its older wall
	// clock and lower signer-local revision cannot stop root/manifest proofs from
	// making it dominant.
	higher := buildEquivalenceWriter(t, params,
		equivalenceStep{syncedTo: 103, rows: prefixRows[:1]},
		equivalenceStep{syncedTo: 111, rows: prefixRows[1:]},
		equivalenceStep{syncedTo: 119, rows: []equivalenceRow{{slot: 116, ids: []uint64{3}}}})
	higherGenesis := putEquivalenceManifest(t, higher.blocks, cid.Undef, 10)
	higherTip := putEquivalenceManifest(t, higher.blocks, higherGenesis, 20)
	higherDoc := signEquivalenceClaim(t, equivalenceKey(t), id, higher.head.Info(), higherTip, 1, "1990-01-01T00:00:00Z")
	union = copyEquivalenceStores(t, left.blocks, higher.blocks)
	keysBefore := equivalenceKeyCount(t, union)
	spy := &equivalenceWriteSpy{Blockstore: union}
	relation, err = follow.ClassifyFinalizedClaims(t.Context(), spy, "all", higherDoc, leftDoc)
	if err != nil || relation != follow.LeftClaimDominates {
		t.Fatalf("higher claim on left = %s, %v; want left-dominates", relation, err)
	}
	if spy.puts != 0 {
		t.Fatalf("isolated root projection wrote %d block(s) through the live-store boundary", spy.puts)
	}
	if keysAfter := equivalenceKeyCount(t, union); keysAfter != keysBefore {
		t.Fatalf("isolated root projection changed live-store key count from %d to %d", keysBefore, keysAfter)
	}
	relation, err = follow.ClassifyFinalizedClaims(t.Context(), union, "all", leftDoc, higherDoc)
	if err != nil || relation != follow.RightClaimDominates {
		t.Fatalf("higher claim on right = %s, %v; want right-dominates", relation, err)
	}
}

func TestFinalizedClaimConflicts(t *testing.T) {
	params := archive.Params{Name: "all", Net: "equivalence-net", OriginSlot: 96, SegBits: 3, FanoutBits: 2}
	id := equivalenceArchiveID(1)
	keyA, keyB := equivalenceKey(t), equivalenceKey(t)

	t.Run("equal coverage different roots", func(t *testing.T) {
		a := buildEquivalenceWriter(t, params, equivalenceStep{syncedTo: 111, rows: []equivalenceRow{{slot: 98, ids: []uint64{1}}}})
		b := buildEquivalenceWriter(t, params, equivalenceStep{syncedTo: 111, rows: []equivalenceRow{{slot: 98, ids: []uint64{99}}}})
		union := copyEquivalenceStores(t, a.blocks, b.blocks)
		leftDoc := signEquivalenceClaim(t, keyA, id, a.head.Info(), cid.Undef, 1, "2026-01-01T00:00:00Z")
		rightDoc := signEquivalenceClaim(t, keyB, id, b.head.Info(), cid.Undef, 1, "2026-01-01T00:00:00Z")
		requireSymmetricArchiveConflict(t, union, "all", leftDoc, rightDoc)
		_, err := follow.ClassifyFinalizedClaims(t.Context(), union, "all", leftDoc, rightDoc)
		conflict := requireArchiveConflict(t, err)
		if conflict.LeftRoot == conflict.RightRoot || conflict.Head != "all" || conflict.ArchiveID != id ||
			conflict.ReasonCode != follow.ConflictReasonEqualCoverageRootMismatch {
			t.Fatalf("conflict evidence = %+v", conflict)
		}
	})

	t.Run("higher root rewrites the lower prefix", func(t *testing.T) {
		lower := buildEquivalenceWriter(t, params, equivalenceStep{syncedTo: 111, rows: []equivalenceRow{{slot: 98, ids: []uint64{1}}}})
		higher := buildEquivalenceWriter(t, params,
			equivalenceStep{syncedTo: 111, rows: []equivalenceRow{{slot: 98, ids: []uint64{99}}}},
			equivalenceStep{syncedTo: 119, rows: []equivalenceRow{{slot: 116, ids: []uint64{3}}}})
		union := copyEquivalenceStores(t, lower.blocks, higher.blocks)
		leftDoc := signEquivalenceClaim(t, keyA, id, lower.head.Info(), cid.Undef, 1, "2026-01-01T00:00:00Z")
		rightDoc := signEquivalenceClaim(t, keyB, id, higher.head.Info(), cid.Undef, 1, "2026-01-01T00:00:00Z")
		requireSymmetricArchiveConflict(t, union, "all", leftDoc, rightDoc)
		_, err := follow.ClassifyFinalizedClaims(t.Context(), union, "all", leftDoc, rightDoc)
		if conflict := requireArchiveConflict(t, err); conflict.ReasonCode != follow.ConflictReasonPrefixProjectionMismatch {
			t.Fatalf("projection conflict reason = %s", conflict.ReasonCode)
		}
	})

	t.Run("divergent manifest histories", func(t *testing.T) {
		a := buildEquivalenceWriter(t, params, equivalenceStep{syncedTo: 111})
		b := buildEquivalenceWriter(t, params, equivalenceStep{syncedTo: 111})
		genesisA := putEquivalenceManifest(t, a.blocks, cid.Undef, 10)
		genesisB := putEquivalenceManifest(t, b.blocks, cid.Undef, 10)
		if genesisA != genesisB {
			t.Fatalf("divergent branches do not share genesis: %s != %s", genesisA, genesisB)
		}
		tipA := putEquivalenceManifest(t, a.blocks, genesisA, 20)
		tipB := putEquivalenceManifest(t, b.blocks, genesisB, 30)
		union := copyEquivalenceStores(t, a.blocks, b.blocks)
		leftDoc := signEquivalenceClaim(t, keyA, id, a.head.Info(), tipA, 1, "2026-01-01T00:00:00Z")
		rightDoc := signEquivalenceClaim(t, keyB, id, b.head.Info(), tipB, 1, "2026-01-01T00:00:00Z")
		requireSymmetricArchiveConflict(t, union, "all", leftDoc, rightDoc)
		_, err := follow.ClassifyFinalizedClaims(t.Context(), union, "all", leftDoc, rightDoc)
		if conflict := requireArchiveConflict(t, err); conflict.ReasonCode != follow.ConflictReasonManifestBranch {
			t.Fatalf("manifest conflict reason = %s", conflict.ReasonCode)
		}
	})
}

func TestFinalizedClaimIncomparabilityAndTransientFailures(t *testing.T) {
	params := archive.Params{Name: "all", Net: "equivalence-net", OriginSlot: 96, SegBits: 3, FanoutBits: 2}
	id := equivalenceArchiveID(1)
	keyA, keyB := equivalenceKey(t), equivalenceKey(t)
	lower := buildEquivalenceWriter(t, params, equivalenceStep{syncedTo: 111})
	higher := buildEquivalenceWriter(t, params,
		equivalenceStep{syncedTo: 111},
		equivalenceStep{syncedTo: 119, rows: []equivalenceRow{{slot: 116, ids: []uint64{3}}}})
	lowerGenesis := putEquivalenceManifest(t, lower.blocks, cid.Undef, 10)
	lowerDescendant := putEquivalenceManifest(t, lower.blocks, lowerGenesis, 20)
	higherGenesis := putEquivalenceManifest(t, higher.blocks, cid.Undef, 10)
	union := copyEquivalenceStores(t, lower.blocks, higher.blocks)

	t.Run("root and manifest orders point opposite ways", func(t *testing.T) {
		relation, err := follow.ClassifyFinalizedClaims(t.Context(), union, "all",
			signEquivalenceClaim(t, keyA, id, higher.head.Info(), higherGenesis, 1, "2026-01-01T00:00:00Z"),
			signEquivalenceClaim(t, keyB, id, lower.head.Info(), lowerDescendant, 1, "2026-01-01T00:00:00Z"))
		if err != nil || relation != follow.ClaimsIncomparable {
			t.Fatalf("opposed root/manifest order = %s, %v; want incomparable", relation, err)
		}
	})

	t.Run("manifest introduction requires an explicit migration", func(t *testing.T) {
		relation, err := follow.ClassifyFinalizedClaims(t.Context(), union, "all",
			signEquivalenceClaim(t, keyA, id, lower.head.Info(), cid.Undef, 1, "2026-01-01T00:00:00Z"),
			signEquivalenceClaim(t, keyB, id, lower.head.Info(), lowerGenesis, 1, "2026-01-01T00:00:00Z"))
		if err != nil || relation != follow.ClaimsIncomparable {
			t.Fatalf("absent/present manifests = %s, %v; want incomparable", relation, err)
		}
	})

	t.Run("different archive identities are unrelated", func(t *testing.T) {
		relation, err := follow.ClassifyFinalizedClaims(t.Context(), union, "all",
			signEquivalenceClaim(t, keyA, id, lower.head.Info(), cid.Undef, 1, "2026-01-01T00:00:00Z"),
			signEquivalenceClaim(t, keyB, equivalenceArchiveID(2), lower.head.Info(), cid.Undef, 1, "2026-01-01T00:00:00Z"))
		if err != nil || relation != follow.ClaimsIncomparable {
			t.Fatalf("different archive IDs = %s, %v; want incomparable", relation, err)
		}
	})

	t.Run("missing ancestry block is transient", func(t *testing.T) {
		successorStore := newEquivalenceBlockstore()
		successor := putEquivalenceManifest(t, successorStore, lowerGenesis, 40)
		// Deliberately do not copy successorStore into the classifier store.
		_, err := follow.ClassifyFinalizedClaims(t.Context(), union, "all",
			signEquivalenceClaim(t, keyA, id, lower.head.Info(), lowerGenesis, 1, "2026-01-01T00:00:00Z"),
			signEquivalenceClaim(t, keyB, id, lower.head.Info(), successor, 1, "2026-01-01T00:00:00Z"))
		if err == nil {
			t.Fatal("comparison succeeded without the manifest block")
		}
		var conflict *follow.ArchiveConflictError
		if errors.As(err, &conflict) {
			t.Fatalf("missing block was reported as hard conflict: %v", err)
		}
	})

	t.Run("missing root block is transient", func(t *testing.T) {
		empty := newEquivalenceBlockstore()
		_, err := follow.ClassifyFinalizedClaims(t.Context(), empty, "all",
			signEquivalenceClaim(t, keyA, id, lower.head.Info(), cid.Undef, 1, "2026-01-01T00:00:00Z"),
			signEquivalenceClaim(t, keyB, id, lower.head.Info(), cid.Undef, 1, "2026-01-01T00:00:00Z"))
		if err == nil {
			t.Fatal("comparison succeeded without root block")
		}
		var conflict *follow.ArchiveConflictError
		if errors.As(err, &conflict) {
			t.Fatalf("missing root was reported as hard conflict: %v", err)
		}
	})

	t.Run("signed entry must reproduce its root", func(t *testing.T) {
		lying := signEquivalenceClaim(t, keyA, id, lower.head.Info(), cid.Undef, 1, "2026-01-01T00:00:00Z")
		lying.Heads[0].DirDepth++
		canonical, err := lying.Unsigned.Canonical()
		if err != nil {
			t.Fatal(err)
		}
		lying.Signature = hex.EncodeToString(ed25519.Sign(keyA, canonical))
		_, err = follow.ClassifyFinalizedClaims(t.Context(), union, "all", lying,
			signEquivalenceClaim(t, keyB, id, lower.head.Info(), cid.Undef, 1, "2026-01-01T00:00:00Z"))
		if err == nil {
			t.Fatal("signed entry whose dir_depth contradicted its root was accepted")
		}
		var conflict *follow.ArchiveConflictError
		if errors.As(err, &conflict) {
			t.Fatalf("malformed signed claim was reported as cross-writer conflict: %v", err)
		}
	})
}

func TestFinalizedClaimRefusesMutableAuthorities(t *testing.T) {
	params := archive.Params{Name: "all", Net: "equivalence-net", OriginSlot: 96, SegBits: 3, FanoutBits: 2}
	w := buildEquivalenceWriter(t, params, equivalenceStep{syncedTo: 111})
	id := equivalenceArchiveID(1)
	key := equivalenceKey(t)
	finalized := signEquivalenceClaim(t, key, id, w.head.Info(), cid.Undef, 1, "2026-01-01T00:00:00Z")

	start, sourceFinalized := uint64(104), uint64(111)
	handoffSynced := *w.head.Info().SyncedTo
	beaconRoot := "0x" + hex.EncodeToString(make([]byte, 32))
	mutable := finalized
	mutable.Heads = append(mutable.Heads, server.HeadEntry{
		Name: "live", Root: w.head.Root().String(), OriginSlot: start, SyncedTo: &sourceFinalized,
		SegBits: 3, FanoutBits: 2, Kind: server.UnfinalizedMutable, WindowStart: &start,
		SourceHeadRoot: beaconRoot, SourceFinalizedSlot: &sourceFinalized, SourceFinalizedRoot: beaconRoot,
		HandoffHead: "all", HandoffRoot: w.head.Root().String(), HandoffSyncedTo: &handoffSynced,
	})
	canonical, err := mutable.Unsigned.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	mutable.Signature = hex.EncodeToString(ed25519.Sign(key, canonical))
	if err := mutable.ValidateContract(); err != nil {
		t.Fatalf("mutable fixture contract: %v", err)
	}
	union := copyEquivalenceStores(t, w.blocks)
	_, err = follow.ClassifyFinalizedClaims(t.Context(), union, "live", mutable, mutable)
	if err == nil {
		t.Fatal("multi-writer mutable claim was accepted")
	}
	var conflict *follow.ArchiveConflictError
	if errors.As(err, &conflict) {
		t.Fatalf("mutable refusal was reported as archive conflict: %v", err)
	}
}
