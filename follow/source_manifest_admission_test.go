package follow_test

import (
	"bytes"
	"crypto/ed25519"
	"errors"
	"fmt"
	"testing"
	"time"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/fluent/qp"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"

	"github.com/blobarchive/bloar/follow"
	"github.com/blobarchive/bloar/schema"
	"github.com/blobarchive/bloar/server"
)

func storeSourceManifestRaw(t *testing.T, w *writer, raw []byte) cid.Cid {
	t.Helper()
	c, err := schema.NodeCID(raw)
	if err != nil {
		t.Fatalf("NodeCID: %v", err)
	}
	block, err := blocks.NewBlockWithCid(raw, c)
	if err != nil {
		t.Fatalf("NewBlockWithCid: %v", err)
	}
	if err := w.store.Blocks().Put(t.Context(), block); err != nil {
		t.Fatalf("storing source manifest block %s: %v", c, err)
	}
	return c
}

func sourceManifestRaw(t *testing.T, head string, prev cid.Cid) []byte {
	t.Helper()
	raw, _, err := schema.EncodeManifest(&schema.Manifest{
		V:    schema.ManifestVersion,
		Head: head,
		Sources: []schema.Source{{
			Type:      schema.SourceInboxEvents,
			Address:   bytes.Repeat([]byte{0x61}, schema.AddressSize),
			Topic:     bytes.Repeat([]byte{0x62}, schema.TopicSize),
			OpenEnded: true,
		}},
		Prev: prev,
	})
	if err != nil {
		t.Fatalf("EncodeManifest: %v", err)
	}
	return raw
}

func sourceArbitraryDAGRaw(t *testing.T, links ...cid.Cid) []byte {
	t.Helper()
	n, err := qp.BuildMap(basicnode.Prototype.Map, int64(1+len(links)), func(ma datamodel.MapAssembler) {
		qp.MapEntry(ma, "not-a-manifest", qp.String("attacker-controlled"))
		for i, link := range links {
			qp.MapEntry(ma, fmt.Sprintf("link-%04d", i), qp.Link(cidlink.Link{Cid: link}))
		}
	})
	if err != nil {
		t.Fatalf("building arbitrary DAG block: %v", err)
	}
	var raw bytes.Buffer
	if err := dagcbor.Encode(n, &raw); err != nil {
		t.Fatalf("encoding arbitrary DAG block: %v", err)
	}
	return raw.Bytes()
}

func newManifestSourceSetFollower(t *testing.T, w *writer, tips ...cid.Cid) (*follower, server.ArchiveID) {
	t.Helper()
	archiveID := sourceRuntimeArchiveID(t)
	sources := make([]follow.SourceConfig, 0, len(tips))
	for i, tip := range tips {
		id := fmt.Sprintf("writer-%c", 'a'+i)
		key := sourceRuntimeKey(t)
		docs := newDocServer(t)
		claim := entry(w.head.Info())
		if tip.Defined() {
			claim.Manifest = tip.String()
		}
		docs.set(sourceRuntimeDocument(t, w, key, archiveID, uint64(i+1), time.Unix(int64(i+1), 0), claim))
		sources = append(sources, follow.SourceConfig{
			ID:           id,
			URL:          docs.url,
			PubKey:       key.Public().(ed25519.PublicKey),
			AllowedHeads: []string{testHead},
		})
	}
	f := newFollower(t, w, func(cfg *follow.Config) {
		configureSourceRuntime(t, cfg, archiveID, sources)
	})
	return f, archiveID
}

func requireNoManifestConflictLatch(t *testing.T, f *follower, archiveID server.ArchiveID) {
	t.Helper()
	if _, ok, err := follow.LoadConflictLatch(f.store.KV(), archiveID, testHead); err != nil || ok {
		t.Fatalf("invalid first manifest created a conflict latch: ok=%t err=%v", ok, err)
	}
}

func TestSourceSetFirstManifestRefusesInvalidCandidateBeforeArbitration(t *testing.T) {
	tests := []struct {
		name  string
		build func(*testing.T, *writer, cid.Cid) cid.Cid
	}{
		{
			name: "malformed",
			build: func(t *testing.T, w *writer, _ cid.Cid) cid.Cid {
				return storeSourceManifestRaw(t, w, []byte{0xff})
			},
		},
		{
			name: "wrong head",
			build: func(t *testing.T, w *writer, _ cid.Cid) cid.Cid {
				return storeSourceManifestRaw(t, w, sourceManifestRaw(t, "other", cid.Undef))
			},
		},
		{
			name: "branching arbitrary DAG",
			build: func(t *testing.T, w *writer, valid cid.Cid) cid.Cid {
				other, err := schema.NodeCID([]byte("unavailable-child"))
				if err != nil {
					t.Fatalf("NodeCID: %v", err)
				}
				return storeSourceManifestRaw(t, w, sourceArbitraryDAGRaw(t, valid, other))
			},
		},
		{
			name: "oversized",
			build: func(t *testing.T, w *writer, _ cid.Cid) cid.Cid {
				return storeSourceManifestRaw(t, w, bytes.Repeat([]byte{0}, (1<<20)+1))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			w := newWriter(t)
			w.ingestSlot(testOrigin, 1)
			valid := w.setManifest(cid.Undef, 1)
			invalid := tc.build(t, w, valid)
			f, archiveID := newManifestSourceSetFollower(t, w, invalid, valid)

			if err := f.pollErr(); err != nil {
				t.Fatalf("healthy peer did not survive invalid first-manifest candidate: %v", err)
			}
			if got := follow.HeadAdopted(f.f, testHead); got != w.head.Root() {
				t.Fatalf("adopted root = %s, want healthy root %s", got, w.head.Root())
			}
			if got, ok := f.adoptedTip(); !ok || got != valid {
				t.Fatalf("adopted manifest = %s (present=%t), want healthy tip %s (invalid %s)", got, ok, valid, invalid)
			}
			requireNoManifestConflictLatch(t, f, archiveID)
			if _, ok, err := follow.ReadSourcePublicationFloor(f.store.KV(), archiveID, "writer-a"); err != nil || ok {
				t.Fatalf("manifest-invalid source advanced its replay floor: ok=%t err=%v", ok, err)
			}
			if revision, ok, err := follow.ReadSourcePublicationFloor(f.store.KV(), archiveID, "writer-b"); err != nil || !ok || revision != 2 {
				t.Fatalf("healthy source replay floor = %d ok=%t err=%v, want revision 2", revision, ok, err)
			}
		})
	}
}

func TestSourceSetInvalidFirstManifestsNeverBecomeConflictEvidenceOrState(t *testing.T) {
	w := newWriter(t)
	w.ingestSlot(testOrigin, 1)
	left := storeSourceManifestRaw(t, w, sourceManifestRaw(t, "other-a", cid.Undef))
	right := storeSourceManifestRaw(t, w, sourceManifestRaw(t, "other-b", cid.Undef))
	f, archiveID := newManifestSourceSetFollower(t, w, left, right)

	err := f.pollErr()
	var evaluation *follow.FinalizedClaimEvaluationError
	if err == nil || !errors.As(err, &evaluation) {
		t.Fatalf("Poll error = %T (%v), want ordinary first-manifest evaluation failure", err, err)
	}
	requireNoManifestConflictLatch(t, f, archiveID)
	if _, _, _, _, ok, err := follow.ReadCheckpoint(f.store.KV(), testHead); err != nil || ok {
		t.Fatalf("checkpoint after two invalid candidates: ok=%t err=%v, want absent", ok, err)
	}
	if _, ok, err := f.roots.Get(t.Context(), testHead); err != nil || ok {
		t.Fatalf("root mirror after two invalid candidates: ok=%t err=%v, want absent", ok, err)
	}
	if _, ok, err := f.manifests.Get(t.Context(), testHead); err != nil || ok {
		t.Fatalf("manifest mirror after two invalid candidates: ok=%t err=%v, want absent", ok, err)
	}
	if _, ok := f.heads.Get(testHead); ok {
		t.Fatal("two invalid first manifests exposed a serving head")
	}
	pins, err := ledgerOf(f.node).ListAll(t.Context(), testHead)
	if err != nil {
		t.Fatalf("ListAll: %v", err)
	}
	if len(pins) != 0 {
		t.Fatalf("two invalid first manifests created pins: %+v", pins)
	}
	for _, sourceID := range []string{"writer-a", "writer-b"} {
		if _, ok, err := follow.ReadSourcePublicationFloor(f.store.KV(), archiveID, sourceID); err != nil || ok {
			t.Fatalf("manifest-invalid source %q advanced its replay floor: ok=%t err=%v", sourceID, ok, err)
		}
	}
}

func TestSourceSetManifestAndDAGAdmissionFailuresCombineBeforeArbitration(t *testing.T) {
	w := newWriter(t)
	w.ingestSlot(testOrigin, 1)
	validManifest := w.setManifest(cid.Undef, 1)
	invalidManifest := storeSourceManifestRaw(t, w, sourceManifestRaw(t, "other", cid.Undef))
	malformedRoot, malformedSyncedTo := repeatedSubtreeHeadSeed(t, w, 71)

	archiveID := sourceRuntimeArchiveID(t)
	manifestKey, dagKey := sourceRuntimeKey(t), sourceRuntimeKey(t)
	manifestDocs, dagDocs := newDocServer(t), newDocServer(t)

	manifestEntry := entry(w.head.Info())
	manifestEntry.Manifest = invalidManifest.String()
	manifestDocs.set(sourceRuntimeDocument(t, w, manifestKey, archiveID, 1, time.Unix(1, 0), manifestEntry))

	dagEntry := followAdmissionEntry(malformedRoot, malformedSyncedTo)
	dagEntry.Manifest = validManifest.String()
	dagDocs.set(sourceRuntimeDocument(t, w, dagKey, archiveID, 1, time.Unix(1, 0), dagEntry))

	sources := []follow.SourceConfig{
		{
			ID: "writer-manifest", URL: manifestDocs.url, PubKey: manifestKey.Public().(ed25519.PublicKey),
			AllowedHeads: []string{testHead},
		},
		{
			ID: "writer-dag", URL: dagDocs.url, PubKey: dagKey.Public().(ed25519.PublicKey),
			AllowedHeads: []string{testHead},
		},
	}
	f := newFollower(t, w, func(cfg *follow.Config) {
		configureSourceRuntime(t, cfg, archiveID, sources)
	})

	err := f.pollErr()
	var evaluation *follow.FinalizedClaimEvaluationError
	if err == nil || !errors.As(err, &evaluation) {
		t.Fatalf("Poll error = %T (%v), want combined pre-arbitration evaluation failure", err, err)
	}
	if len(evaluation.Failures) != 2 {
		t.Fatalf("combined admission failures = %d, want 2: %+v", len(evaluation.Failures), evaluation.Failures)
	}
	var sawManifest, sawDAG bool
	for _, failure := range evaluation.Failures {
		message := failure.Err.Error()
		sawManifest = sawManifest || bytes.Contains([]byte(message), []byte("first manifest chain"))
		sawDAG = sawDAG || bytes.Contains([]byte(message), []byte("shared at multiple positions"))
	}
	if !sawManifest || !sawDAG {
		t.Fatalf("combined admission failures = %+v, want manifest and DAG failures", evaluation.Failures)
	}
	requireNoManifestConflictLatch(t, f, archiveID)
	if _, _, _, _, ok, err := follow.ReadCheckpoint(f.store.KV(), testHead); err != nil || ok {
		t.Fatalf("checkpoint after combined admission failures: ok=%t err=%v, want absent", ok, err)
	}
	if _, ok, err := f.roots.Get(t.Context(), testHead); err != nil || ok {
		t.Fatalf("root mirror after combined admission failures: ok=%t err=%v, want absent", ok, err)
	}
	if _, ok, err := f.manifests.Get(t.Context(), testHead); err != nil || ok {
		t.Fatalf("manifest mirror after combined admission failures: ok=%t err=%v, want absent", ok, err)
	}
	if _, ok := f.heads.Get(testHead); ok {
		t.Fatal("combined admission failures exposed a serving head")
	}
	pins, err := ledgerOf(f.node).ListAll(t.Context(), testHead)
	if err != nil {
		t.Fatalf("ListAll: %v", err)
	}
	if len(pins) != 0 {
		t.Fatalf("combined admission failures created pins: %+v", pins)
	}
	for _, sourceID := range []string{"writer-manifest", "writer-dag"} {
		if _, ok, err := follow.ReadSourcePublicationFloor(f.store.KV(), archiveID, sourceID); err != nil || ok {
			t.Fatalf("rejected source %q advanced its replay floor: ok=%t err=%v", sourceID, ok, err)
		}
	}
}

func TestSourceSetTypedBoundaryPrecedesGeneric4096HopArbitrationWalk(t *testing.T) {
	w := newWriter(t)
	w.ingestSlot(testOrigin, 1)
	valid := w.setManifest(cid.Undef, 1)

	// This is exactly maxManifestWalk generic one-link DAG blocks. Before the
	// source-set boundary, arbitration would follow all 4096 links trying to
	// compare this attacker-controlled graph with the healthy peer's Manifest.
	// Typed first-admission validation must reject the tip itself instead.
	const genericHops = 4096
	tip := storeSourceManifestRaw(t, w, sourceArbitraryDAGRaw(t))
	childOfTip := cid.Undef
	for i := 1; i < genericHops; i++ {
		childOfTip = tip
		tip = storeSourceManifestRaw(t, w, sourceArbitraryDAGRaw(t, tip))
	}

	f, archiveID := newManifestSourceSetFollower(t, w, tip, valid)
	if err := f.pollErr(); err != nil {
		t.Fatalf("healthy peer did not survive generic-DAG candidate: %v", err)
	}
	if got, ok := f.adoptedTip(); !ok || got != valid {
		t.Fatalf("adopted manifest = %s (present=%t), want healthy tip %s", got, ok, valid)
	}
	if f.hasLocally(childOfTip) {
		t.Fatalf("arbitration fetched child %s below invalid tip %s; generic walk ran before typed admission", childOfTip, tip)
	}
	requireNoManifestConflictLatch(t, f, archiveID)
}
