package follow_test

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/binary"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/ipfs/boxo/blockstore"
	blocks "github.com/ipfs/go-block-format"

	"github.com/blobarchive/bloar/archive"
	"github.com/blobarchive/bloar/follow"
	"github.com/blobarchive/bloar/metrics"
	"github.com/blobarchive/bloar/pinning"
	"github.com/blobarchive/bloar/server"
)

const sourceRuntimeArchiveIDText = "0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20"

// sourceRuntimePlainBlockstore deliberately erases the optional collection
// generation interface while retaining the ordinary Boxo blockstore surface.
type sourceRuntimePlainBlockstore struct{ blockstore.Blockstore }

func sourceRuntimeArchiveID(t *testing.T) server.ArchiveID {
	t.Helper()
	id, err := server.ParseArchiveID(sourceRuntimeArchiveIDText)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func sourceRuntimeKey(t *testing.T) ed25519.PrivateKey {
	t.Helper()
	_, key, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	return key
}

func sourceRuntimeDocument(t *testing.T, w *writer, key ed25519.PrivateKey, archiveID server.ArchiveID,
	revision uint64, at time.Time, entries ...server.HeadEntry) []byte {
	t.Helper()
	id := archiveID
	rev := revision
	return sign(t, key, server.Unsigned{
		V: server.LogicalArchiveDocVersion, Net: testNet, ArchiveID: &id,
		UpdatedAt: at.UTC().Format(time.RFC3339), Multiaddrs: w.host.AnnounceAddrs(),
		Heads: entries, Revision: &rev,
	})
}

func configureSourceRuntime(t *testing.T, cfg *follow.Config, archiveID server.ArchiveID, sources []follow.SourceConfig) {
	t.Helper()
	digest, err := follow.SourceSetDigest(cfg.Net, archiveID, sources)
	if err != nil {
		t.Fatalf("SourceSetDigest: %v", err)
	}
	id := archiveID
	cfg.URL, cfg.IPNS, cfg.DNSLink, cfg.PubKey = "", "", "", nil
	cfg.ExpectedArchiveID = &id
	cfg.SourceSet = &follow.SourceSetConfig{Revision: 1, Digest: digest, Sources: sources}
}

type sourceRuntimeV4 struct {
	selected  bool
	sourceID  string
	revision  uint64
	archiveID server.ArchiveID
	authority [ed25519.PublicKeySize]byte
}

// readSourceRuntimeV4 deliberately checks the durable wire record rather than
// an in-memory follower field. The fixed prefix is checkpoint v4's public
// durability contract: version/flags, net/source lengths, timestamp/revision,
// archive identity, authority, digest, and three entry lengths. A restart below
// independently proves the same record remains consumable by production code.
func readSourceRuntimeV4(t *testing.T, kv *pebble.DB, head string) sourceRuntimeV4 {
	t.Helper()
	key := append([]byte{'f'}, []byte("checkpoint:"+head)...)
	raw, closer, err := kv.Get(key)
	if err != nil {
		t.Fatalf("reading checkpoint %q: %v", head, err)
	}
	defer closer.Close()
	const fixed = 132
	if len(raw) < fixed {
		t.Fatalf("checkpoint %q is %d bytes, want v4 fixed prefix", head, len(raw))
	}
	if raw[0] != 4 {
		t.Fatalf("checkpoint %q version = %d, want source-attributed v4", head, raw[0])
	}
	sourceLen := int(raw[4])
	if sourceLen == 0 || fixed+sourceLen > len(raw) {
		t.Fatalf("checkpoint %q source length = %d for %d bytes", head, sourceLen, len(raw))
	}
	got := sourceRuntimeV4{
		selected: raw[1]&1 != 0,
		sourceID: string(raw[fixed : fixed+sourceLen]),
		revision: binary.BigEndian.Uint64(raw[16:24]),
	}
	copy(got.archiveID[:], raw[24:56])
	copy(got.authority[:], raw[56:88])
	return got
}

func TestSourceRuntimeEquivalentWritersSelectStableV4AndResume(t *testing.T) {
	w := newWriter(t)
	w.ingestSlot(100, 1)
	archiveID := sourceRuntimeArchiveID(t)
	writerAKey, writerZKey := sourceRuntimeKey(t), sourceRuntimeKey(t)
	writerADocs, writerZDocs := newDocServer(t), newDocServer(t)
	claim := entry(w.head.Info())
	// Cross-writer clocks and revisions are intentionally inverted. Equivalent
	// content has no semantic winner; source ID supplies only a stable transport
	// and provenance representative.
	writerADocs.set(sourceRuntimeDocument(t, w, writerAKey, archiveID, 50, time.Unix(50, 0), claim))
	writerZDocs.set(sourceRuntimeDocument(t, w, writerZKey, archiveID, 1, time.Unix(1, 0), claim))
	sources := []follow.SourceConfig{
		{ID: "writer-z", URL: writerZDocs.url, PubKey: writerZKey.Public().(ed25519.PublicKey), AllowedHeads: []string{testHead}},
		{ID: "writer-a", URL: writerADocs.url, PubKey: writerAKey.Public().(ed25519.PublicKey), AllowedHeads: []string{testHead}},
	}
	f := newFollower(t, w, func(cfg *follow.Config) {
		configureSourceRuntime(t, cfg, archiveID, sources)
	})
	f.poll()

	if got := follow.HeadAdopted(f.f, testHead); got != w.head.Root() {
		t.Fatalf("adopted root = %s, want %s", got, w.head.Root())
	}
	cp := readSourceRuntimeV4(t, f.store.KV(), testHead)
	if !cp.selected || cp.sourceID != "writer-a" || cp.revision != 50 || cp.archiveID != archiveID ||
		!bytes.Equal(cp.authority[:], writerAKey.Public().(ed25519.PublicKey)) {
		t.Fatalf("source-attributed checkpoint = %+v, want selected writer-a revision 50", cp)
	}

	restarted := f.restart(t, w, func(cfg *follow.Config) {
		configureSourceRuntime(t, cfg, archiveID, sources)
	})
	if err := restarted.Resume(t.Context()); err != nil {
		t.Fatalf("Resume source-attributed v4: %v", err)
	}
	if got := follow.HeadAdopted(restarted, testHead); got != w.head.Root() {
		t.Fatalf("resumed root = %s, want %s", got, w.head.Root())
	}
}

func TestSourceRuntimeMigrationKeepsHiddenLegacyCheckpointThroughGC(t *testing.T) {
	w := newWriter(t)
	blobs, _ := w.ingestSlot(100, 1)
	f := newFollower(t, w)
	f.poll()
	f.reconcile()

	root, _, _, _, ok, err := follow.ReadCheckpoint(f.store.KV(), testHead)
	if err != nil || !ok {
		t.Fatalf("reading legacy checkpoint: ok=%t err=%v", ok, err)
	}
	blob := blobCID(t, blobs[0])
	for _, c := range []struct {
		name string
		has  bool
	}{
		{name: "root", has: f.hasLocally(root)},
		{name: "blob", has: f.hasLocally(blob)},
	} {
		if !c.has {
			t.Fatalf("legacy fixture is missing local %s before migration", c.name)
		}
	}

	archiveID := sourceRuntimeArchiveID(t)
	sources := []follow.SourceConfig{{
		ID: "writer-a", URL: w.url, PubKey: w.pubkey(), AllowedHeads: []string{testHead},
	}}
	restarted := f.restart(t, w, func(cfg *follow.Config) {
		configureSourceRuntime(t, cfg, archiveID, sources)
		cfg.SourceSet.MigrateLegacySource = "writer-a"
	})
	if err := restarted.Resume(t.Context()); err != nil {
		t.Fatalf("Resume after explicit source-set migration: %v", err)
	}
	if _, serving := f.heads.Get(testHead); serving {
		t.Fatal("legacy checkpoint became served without v4 source provenance")
	}

	stats := f.gc()
	if stats.Scanned == 0 {
		t.Fatal("migration regression did not run a real GC sweep")
	}
	if !f.hasLocally(root) {
		t.Fatalf("GC swept hidden legacy checkpoint root %s", root)
	}
	if !f.hasLocally(blob) {
		t.Fatalf("GC swept blob %s reachable from hidden full-retention checkpoint", blob)
	}
}

func TestSourceRuntimeRedundantChannelsResolveConcurrently(t *testing.T) {
	w := newIPNSWriter(t)
	w.ingestSlot(100, 1)
	archiveID := sourceRuntimeArchiveID(t)
	document := sourceRuntimeDocument(t, w.writer, w.key, archiveID, 1, time.Unix(1, 0), entry(w.head.Info()))
	w.publish(t, document)

	hung := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, request *http.Request) {
		<-request.Context().Done()
	}))
	t.Cleanup(hung.Close)
	sources := []follow.SourceConfig{{
		ID: "writer-a", URL: hung.URL, IPNS: w.name(), PubKey: w.pubkey(), AllowedHeads: []string{testHead},
	}}
	f := newFollower(t, w.writer, func(cfg *follow.Config) {
		configureSourceRuntime(t, cfg, archiveID, sources)
		cfg.Routing = w.routing
		cfg.FetchTimeout = 150 * time.Millisecond
	})
	started := time.Now()
	if err := f.pollErr(); err != nil {
		t.Fatalf("healthy IPNS beside hung HTTPS: %v", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("redundant source resolution took %s, want one bounded concurrent channel wait", elapsed)
	}
	if got := follow.HeadAdopted(f.f, testHead); got != w.head.Root() {
		t.Fatalf("IPNS-selected root = %s, want %s", got, w.head.Root())
	}
}

func TestSourceRuntimePollBeforeResumeRestoresEquivalentV4Checkpoint(t *testing.T) {
	w := newWriter(t)
	w.ingestSlot(100, 1)
	archiveID := sourceRuntimeArchiveID(t)
	key := sourceRuntimeKey(t)
	docs := newDocServer(t)
	docs.set(sourceRuntimeDocument(t, w, key, archiveID, 1, time.Unix(1, 0), entry(w.head.Info())))
	sources := []follow.SourceConfig{{
		ID: "writer-a", URL: docs.url, PubKey: key.Public().(ed25519.PublicKey), AllowedHeads: []string{testHead},
	}}
	f := newFollower(t, w, func(cfg *follow.Config) {
		configureSourceRuntime(t, cfg, archiveID, sources)
	})
	f.poll()

	restarted := f.restart(t, w, func(cfg *follow.Config) {
		configureSourceRuntime(t, cfg, archiveID, sources)
	})
	// Deliberately skip Resume, matching Run's recovery path after an all-head
	// Resume failure. An equivalent healthy poll must not mistake the durable
	// checkpoint for an already-visible registry entry.
	if err := restarted.Poll(t.Context()); err != nil {
		t.Fatalf("Poll before Resume: %v", err)
	}
	if got := follow.HeadAdopted(restarted, testHead); got != w.head.Root() {
		t.Fatalf("restored root = %s, want durable checkpoint %s", got, w.head.Root())
	}
}

func TestSourceRuntimePollBeforeResumeRetainsAheadV4Checkpoint(t *testing.T) {
	w := newWriter(t)
	w.ingestSlot(100, 1)
	archiveID := sourceRuntimeArchiveID(t)
	key := sourceRuntimeKey(t)
	docs := newDocServer(t)
	root100 := w.head.Root()
	entry100 := entry(w.head.Info())
	docs.set(sourceRuntimeDocument(t, w, key, archiveID, 1, time.Unix(1, 0), entry100))
	sources := []follow.SourceConfig{{
		ID: "writer-a", URL: docs.url, PubKey: key.Public().(ed25519.PublicKey), AllowedHeads: []string{testHead},
	}}
	f := newFollower(t, w, func(cfg *follow.Config) {
		configureSourceRuntime(t, cfg, archiveID, sources)
	})
	f.poll()

	w.ingestSlot(101, 2)
	root101 := w.head.Root()
	docs.set(sourceRuntimeDocument(t, w, key, archiveID, 2, time.Unix(2, 0), entry(w.head.Info())))
	if err := f.pollErr(); err != nil {
		t.Fatalf("adopting ahead checkpoint: %v", err)
	}

	restarted := f.restart(t, w, func(cfg *follow.Config) {
		configureSourceRuntime(t, cfg, archiveID, sources)
	})
	// A newer source-local revision may temporarily carry compatible stale
	// content. It raises only that source's replay floor; the ahead, attributed
	// durable claim remains the selected serving state.
	docs.set(sourceRuntimeDocument(t, w, key, archiveID, 3, time.Unix(3, 0), entry100))
	if err := restarted.Poll(t.Context()); err != nil {
		t.Fatalf("stale healthy poll before Resume: %v", err)
	}
	if got := follow.HeadAdopted(restarted, testHead); got != root101 {
		t.Fatalf("restored root = %s, want ahead durable checkpoint %s (stale source root %s)", got, root101, root100)
	}
}

func TestSourceRuntimeOfflinePollRestoresCompleteFinalizedAndMutableV4Snapshot(t *testing.T) {
	w := newWriter(t)
	archiveID := sourceRuntimeArchiveID(t)
	key := sourceRuntimeKey(t)
	docs := newDocServer(t)
	mutable := buildMutableGeneration(t, w, 96, 103)
	unsigned := revisionedUnsigned(w, mutable, 1, time.Unix(1, 0), server.UnfinalizedMutable)
	unsigned.V = server.LogicalArchiveDocVersion
	id := archiveID
	unsigned.ArchiveID = &id
	docs.set(sign(t, key, unsigned))
	sources := []follow.SourceConfig{{
		ID: "writer-a", URL: docs.url, PubKey: key.Public().(ed25519.PublicKey),
		AllowedHeads: []string{testHandoffHead, testHead},
	}}
	f := newFollower(t, w, func(cfg *follow.Config) {
		configureMutableFollower(cfg, 16)
		configureSourceRuntime(t, cfg, archiveID, sources)
	})
	f.poll()
	finalized, ok := f.heads.Get(testHandoffHead)
	if !ok {
		t.Fatal("initial finalized boundary is not serving")
	}
	finalizedRoot := finalized.Root()

	docs.status(http.StatusServiceUnavailable)
	restarted := f.restart(t, w, func(cfg *follow.Config) {
		configureMutableFollower(cfg, 16)
		configureSourceRuntime(t, cfg, archiveID, sources)
	})
	// Deliberately skip Resume and remove every live source. Poll must rebuild
	// the complete source-attributed serving snapshot from durable state before
	// reporting the transport outage; neither checkpoint nor source floor is
	// rewritten by these serving-only plans.
	if err := restarted.Poll(t.Context()); err == nil {
		t.Fatal("offline Poll returned nil; want the source outage reported after recovery")
	}
	if got := follow.HeadAdopted(restarted, testHandoffHead); got != finalizedRoot {
		t.Fatalf("offline finalized root = %s, want %s", got, finalizedRoot)
	}
	if got := follow.HeadAdopted(restarted, testHead); got != mutable.Root() {
		t.Fatalf("offline mutable root = %s, want %s", got, mutable.Root())
	}
}

func TestSourceRuntimeConflictAfterDarkStartupStillServesDurableLastGood(t *testing.T) {
	w := newWriter(t)
	w.ingestSlot(100, 1)
	durableRoot := w.head.Root()
	conflicting := buildDocumentHead(t, w, testHead, testOrigin, 100, testSegBits, testFanout)
	if conflicting.Root() == durableRoot {
		t.Fatal("conflict fixture reproduced the durable root")
	}
	archiveID := sourceRuntimeArchiveID(t)
	keyA, keyB := sourceRuntimeKey(t), sourceRuntimeKey(t)
	docsA, docsB := newDocServer(t), newDocServer(t)
	docsA.set(sourceRuntimeDocument(t, w, keyA, archiveID, 1, time.Unix(1, 0), entry(w.head.Info())))
	docsB.status(http.StatusServiceUnavailable)
	sources := []follow.SourceConfig{
		{ID: "writer-a", URL: docsA.url, PubKey: keyA.Public().(ed25519.PublicKey), AllowedHeads: []string{testHead}},
		{ID: "writer-b", URL: docsB.url, PubKey: keyB.Public().(ed25519.PublicKey), AllowedHeads: []string{testHead}},
	}
	f := newFollower(t, w, func(cfg *follow.Config) {
		configureSourceRuntime(t, cfg, archiveID, sources)
	})
	f.poll()

	restarted := f.restart(t, w, func(cfg *follow.Config) {
		configureSourceRuntime(t, cfg, archiveID, sources)
	})
	docsB.set(sourceRuntimeDocument(t, w, keyB, archiveID, 1, time.Unix(1, 0), entry(conflicting.Info())))
	docsB.status(0)
	err := restarted.Poll(t.Context())
	var conflict *follow.ConflictLatchedError
	if !errors.As(err, &conflict) {
		t.Fatalf("conflicting dark-start poll error = %T (%v), want durable conflict latch", err, err)
	}
	if got := follow.HeadAdopted(restarted, testHead); got != durableRoot {
		t.Fatalf("root after conflicting poll = %s, want restored durable last-good %s", got, durableRoot)
	}
	if served, ok := f.heads.Get(testHead); !ok || served.Root() != durableRoot {
		t.Fatalf("registry after conflicting poll = %v ok=%t, want durable last-good", served, ok)
	}
}

func TestSourceRuntimeQuarantinedNoOpRefusesWholeSourceDocument(t *testing.T) {
	w := newWriter(t)
	w.ingestSlot(100, 1)
	archiveID := sourceRuntimeArchiveID(t)
	key := sourceRuntimeKey(t)
	docs := newDocServer(t)
	claim := entry(w.head.Info())
	docs.set(sourceRuntimeDocument(t, w, key, archiveID, 1, time.Unix(1, 0), claim))
	sources := []follow.SourceConfig{{
		ID: "writer-a", URL: docs.url, PubKey: key.Public().(ed25519.PublicKey), AllowedHeads: []string{testHead},
	}}
	f := newFollower(t, w, func(cfg *follow.Config) {
		configureSourceRuntime(t, cfg, archiveID, sources)
	})
	f.poll()
	if err := follow.QuarantineHead(f.f, testHead, "test verification failure"); err == nil {
		t.Fatal("QuarantineHead returned nil")
	}

	docs.set(sourceRuntimeDocument(t, w, key, archiveID, 2, time.Unix(2, 0), claim))
	err := f.pollErr()
	if err == nil || !strings.Contains(err.Error(), "quarantined") {
		t.Fatalf("quarantined equivalent publication error = %v", err)
	}
	_, serving := f.heads.Get(testHead)
	if !follow.HeadQuarantined(f.f, testHead) || serving {
		t.Fatal("quarantined source publication restored service")
	}
}

func TestSourceRuntimeUnavailableHighRevisionWriterDoesNotBlockHealthyAdvance(t *testing.T) {
	w := newWriter(t)
	w.ingestSlot(100, 2)
	archiveID := sourceRuntimeArchiveID(t)
	writerAKey, writerZKey := sourceRuntimeKey(t), sourceRuntimeKey(t)
	writerADocs, writerZDocs := newDocServer(t), newDocServer(t)
	initial := entry(w.head.Info())
	writerADocs.set(sourceRuntimeDocument(t, w, writerAKey, archiveID, 500, time.Unix(5_000, 0), initial))
	writerZDocs.set(sourceRuntimeDocument(t, w, writerZKey, archiveID, 1, time.Unix(1, 0), initial))
	sources := []follow.SourceConfig{
		{ID: "writer-a", URL: writerADocs.url, PubKey: writerAKey.Public().(ed25519.PublicKey), AllowedHeads: []string{testHead}},
		{ID: "writer-z", URL: writerZDocs.url, PubKey: writerZKey.Public().(ed25519.PublicKey), AllowedHeads: []string{testHead}},
	}
	f := newFollower(t, w, func(cfg *follow.Config) {
		configureSourceRuntime(t, cfg, archiveID, sources)
	})
	f.poll()

	// The previously selected writer disappears. The other writer advances on
	// its own revision 2 despite writer-a's durable revision 500 floor and much
	// newer diagnostic clock: neither ordering fact crosses source boundaries.
	w.ingestSlot(101, 3)
	advanced := entry(w.head.Info())
	writerADocs.status(http.StatusServiceUnavailable)
	writerZDocs.set(sourceRuntimeDocument(t, w, writerZKey, archiveID, 2, time.Unix(2, 0), advanced))
	if err := f.pollErr(); err != nil {
		t.Fatalf("healthy source advance with unavailable peer: %v", err)
	}
	if got := follow.HeadAdopted(f.f, testHead); got != w.head.Root() {
		t.Fatalf("healthy source adopted root = %s, want %s", got, w.head.Root())
	}
	if got := followerSyncedTo(t, f); got != 101 {
		t.Fatalf("healthy source synced_to = %d, want 101", got)
	}
	cp := readSourceRuntimeV4(t, f.store.KV(), testHead)
	if cp.sourceID != "writer-z" || cp.revision != 2 || !bytes.Equal(cp.authority[:], writerZKey.Public().(ed25519.PublicKey)) {
		t.Fatalf("advanced checkpoint = %+v, want writer-z revision 2", cp)
	}
}

func TestSourceRuntimeFinalizedEquivocationDoesNotBlockIndependentWriter(t *testing.T) {
	w := newWriter(t)
	w.ingestSlot(100, 1)
	archiveID := sourceRuntimeArchiveID(t)
	keyA, keyB := sourceRuntimeKey(t), sourceRuntimeKey(t)
	docsA, docsB := newDocServer(t), newDocServer(t)
	initial := entry(w.head.Info())
	docsA.set(sourceRuntimeDocument(t, w, keyA, archiveID, 1, time.Unix(1, 0), initial))
	docsB.set(sourceRuntimeDocument(t, w, keyB, archiveID, 1, time.Unix(1, 0), initial))
	sources := []follow.SourceConfig{
		{ID: "writer-a", URL: docsA.url, PubKey: keyA.Public().(ed25519.PublicKey), AllowedHeads: []string{testHead}},
		{ID: "writer-b", URL: docsB.url, PubKey: keyB.Public().(ed25519.PublicKey), AllowedHeads: []string{testHead}},
	}
	f := newFollower(t, w, func(cfg *follow.Config) {
		configureSourceRuntime(t, cfg, archiveID, sources)
	})
	f.poll()

	// writer-a equivocates at its already admitted revision. That source is
	// omitted, but finalized state is not authority-local and writer-b's proven
	// append-only advance remains independently admissible.
	docsA.set(sourceRuntimeDocument(t, w, keyA, archiveID, 1, time.Unix(2, 0), initial))
	w.ingestSlot(101, 2)
	docsB.set(sourceRuntimeDocument(t, w, keyB, archiveID, 2, time.Unix(2, 0), entry(w.head.Info())))
	if err := f.pollErr(); err != nil {
		t.Fatalf("healthy writer advance beside finalized equivocation: %v", err)
	}
	if got := follow.HeadAdopted(f.f, testHead); got != w.head.Root() {
		t.Fatalf("root after unrelated source equivocation = %s, want healthy advance %s", got, w.head.Root())
	}
	if got := followerSyncedTo(t, f); got != 101 {
		t.Fatalf("synced_to after unrelated source equivocation = %d, want 101", got)
	}
}

func TestSourceRuntimeUnavailableHeadProofDoesNotBlockIndependentHead(t *testing.T) {
	w := newWriter(t)
	archiveID := sourceRuntimeArchiveID(t)
	keyA, keyB := sourceRuntimeKey(t), sourceRuntimeKey(t)
	docsA, docsB := newDocServer(t), newDocServer(t)
	missingTo := uint64(100)
	missingAlpha := server.HeadEntry{
		Name: "alpha", Root: rawCID(t, "missing-alpha-head").String(), OriginSlot: testOrigin,
		SyncedTo: &missingTo, SegBits: testSegBits, FanoutBits: testFanout, DirDepth: 1,
	}
	beta := buildDocumentHead(t, w, "beta", testOrigin, 100, testSegBits, testFanout)
	docsA.set(sourceRuntimeDocument(t, w, keyA, archiveID, 1, time.Unix(1, 0), missingAlpha))
	docsB.set(sourceRuntimeDocument(t, w, keyB, archiveID, 1, time.Unix(1, 0), entry(beta.Info())))
	sources := []follow.SourceConfig{
		{ID: "writer-a", URL: docsA.url, PubKey: keyA.Public().(ed25519.PublicKey), AllowedHeads: []string{"alpha"}},
		{ID: "writer-b", URL: docsB.url, PubKey: keyB.Public().(ed25519.PublicKey), AllowedHeads: []string{"beta"}},
	}
	f := newFollower(t, w, func(cfg *follow.Config) {
		cfg.Heads = map[string]pinning.Policy{"alpha": pinning.Full(), "beta": pinning.Full()}
		configureSourceRuntime(t, cfg, archiveID, sources)
	})
	err := f.pollErr()
	var evaluation *follow.FinalizedClaimEvaluationError
	if !errors.As(err, &evaluation) {
		t.Fatalf("poll error = %T (%v), want alpha proof evaluation failure", err, err)
	}
	if _, ok := f.heads.Get("alpha"); ok {
		t.Fatal("unproven alpha became serviceable")
	}
	if got, ok := f.heads.Get("beta"); !ok || got.Root() != beta.Root() {
		t.Fatalf("independent beta = %v ok=%t, want %s", got, ok, beta.Root())
	}
}

func TestSourceRuntimeInvalidMutableBoundaryDoesNotBlockIndependentSourceOrRecovery(t *testing.T) {
	w := newWriter(t)
	archiveID := sourceRuntimeArchiveID(t)
	keyA, keyB := sourceRuntimeKey(t), sourceRuntimeKey(t)
	docsA, docsB := newDocServer(t), newDocServer(t)

	// The global handoff covers immediately before the mutable window, so the
	// publication contract is valid. This filtered replica's selected boundary
	// deliberately lags it by one extra slot and must reject source A locally.
	filtered := buildDocumentHead(t, w, overlayFilteredHead, 96, 103, testSegBits, testFanout)
	mutable := buildDocumentHead(t, w, testHead, 105, 111, testSegBits, testFanout)
	witness := buildDocumentHead(t, w, testHandoffHead, 96, 104, testSegBits, testFanout)
	badMutable := filteredOverlayDocument(t, w, filtered, mutable, witness, 1)
	idA := archiveID
	badMutable.ArchiveID = &idA
	docsA.set(sign(t, keyA, badMutable))

	beta := buildDocumentHead(t, w, "beta", 96, 100, testSegBits, testFanout)
	docsB.set(sourceRuntimeDocument(t, w, keyB, archiveID, 1, time.Unix(1, 0), entry(beta.Info())))
	sources := []follow.SourceConfig{
		{ID: "writer-a", URL: docsA.url, PubKey: keyA.Public().(ed25519.PublicKey), AllowedHeads: []string{overlayFilteredHead, testHead}},
		{ID: "writer-b", URL: docsB.url, PubKey: keyB.Public().(ed25519.PublicKey), AllowedHeads: []string{"beta"}},
	}
	configure := func(cfg *follow.Config) {
		cfg.Heads = map[string]pinning.Policy{
			overlayFilteredHead: pinning.Full(), testHead: pinning.Full(), "beta": pinning.Full(),
		}
		cfg.ExpectedKinds = map[string]server.HeadKind{testHead: server.UnfinalizedMutable}
		cfg.ExpectedHandoffs = map[string]string{testHead: testHandoffHead}
		cfg.OverlayFinalizedHeads = map[string]string{testHead: overlayFilteredHead}
		cfg.MaxMutableWindowSlots = map[string]uint64{testHead: 32}
		configureSourceRuntime(t, cfg, archiveID, sources)
	}
	f := newFollower(t, w, configure)
	if err := f.pollErr(); err != nil {
		t.Fatalf("healthy beta beside invalid mutable source: %v", err)
	}
	if got, ok := f.heads.Get("beta"); !ok || got.Root() != beta.Root() {
		t.Fatalf("independent beta = %v ok=%t, want %s", got, ok, beta.Root())
	}
	if _, ok := f.heads.Get(testHead); ok {
		t.Fatal("invalid mutable source became serviceable")
	}

	// On a fresh registry, make the independent source unavailable. Poll must
	// still restore beta from its durable checkpoint even while source A repeats
	// the same locally invalid mutable generation.
	docsB.status(http.StatusServiceUnavailable)
	restarted := f.restart(t, w, configure)
	if err := restarted.Poll(t.Context()); err == nil {
		t.Fatal("offline independent source Poll returned nil")
	}
	if got := follow.HeadAdopted(restarted, "beta"); got != beta.Root() {
		t.Fatalf("durable beta after bad-source/offline recovery = %s, want %s", got, beta.Root())
	}
}

func TestSourceRuntimeProspectiveMutableBoundaryFailureDoesNotBlockFinalizedPlans(t *testing.T) {
	w := newWriter(t)
	archiveID := sourceRuntimeArchiveID(t)
	mutableKey, finalizedKey := sourceRuntimeKey(t), sourceRuntimeKey(t)
	mutableDocs, finalizedDocs := newDocServer(t), newDocServer(t)

	// The mutable authority authenticates a locally coherent boundary at 104,
	// but is not authorized to select that finalized head on this replica. The
	// finalized authority's selected snapshot covers only 103. The mutable plan
	// must therefore be omitted after arbitration without rolling back either
	// independent finalized plan.
	mutableBoundary := buildDocumentHead(t, w, overlayFilteredHead, 96, 104, testSegBits, testFanout)
	mutable := buildDocumentHead(t, w, testHead, 105, 111, testSegBits, testFanout)
	witness := buildDocumentHead(t, w, testHandoffHead, 96, 104, testSegBits, testFanout)
	mutableUnsigned := filteredOverlayDocument(t, w, mutableBoundary, mutable, witness, 1)
	mutableUnsigned.V = server.LogicalArchiveDocVersion
	mutableArchiveID := archiveID
	mutableUnsigned.ArchiveID = &mutableArchiveID
	mutableDocs.set(sign(t, mutableKey, mutableUnsigned))

	selectedBoundary := buildDocumentHead(t, w, overlayFilteredHead, 96, 103, testSegBits, testFanout)
	beta := buildDocumentHead(t, w, "beta", 96, 100, testSegBits, testFanout)
	finalizedDocs.set(sourceRuntimeDocument(t, w, finalizedKey, archiveID, 1, time.Unix(1, 0),
		entry(selectedBoundary.Info()), entry(beta.Info())))
	sources := []follow.SourceConfig{
		{ID: "mutable-writer", URL: mutableDocs.url, PubKey: mutableKey.Public().(ed25519.PublicKey), AllowedHeads: []string{testHead}},
		{ID: "finalized-writer", URL: finalizedDocs.url, PubKey: finalizedKey.Public().(ed25519.PublicKey), AllowedHeads: []string{overlayFilteredHead, "beta"}},
	}
	f := newFollower(t, w, func(cfg *follow.Config) {
		cfg.Heads = map[string]pinning.Policy{
			overlayFilteredHead: pinning.Full(), testHead: pinning.Full(), "beta": pinning.Full(),
		}
		cfg.ExpectedKinds = map[string]server.HeadKind{testHead: server.UnfinalizedMutable}
		cfg.ExpectedHandoffs = map[string]string{testHead: testHandoffHead}
		cfg.OverlayFinalizedHeads = map[string]string{testHead: overlayFilteredHead}
		cfg.MaxMutableWindowSlots = map[string]uint64{testHead: 32}
		configureSourceRuntime(t, cfg, archiveID, sources)
	})
	err := f.pollErr()
	if err == nil || !strings.Contains(err.Error(), "not covered by the selected finalized snapshot") {
		t.Fatalf("prospective boundary Poll error = %v", err)
	}
	if got, ok := f.heads.Get(overlayFilteredHead); !ok || got.Root() != selectedBoundary.Root() {
		t.Fatalf("selected finalized boundary = %v ok=%t, want %s", got, ok, selectedBoundary.Root())
	}
	if got, ok := f.heads.Get("beta"); !ok || got.Root() != beta.Root() {
		t.Fatalf("independent beta = %v ok=%t, want %s", got, ok, beta.Root())
	}
	if _, ok := f.heads.Get(testHead); ok {
		t.Fatal("mutable head with uncovered prospective boundary became serviceable")
	}
}

func TestSourceRuntimeClosureFailureDoesNotBlockIndependentHead(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		name := "generation-aware"
		if legacy {
			name = "legacy-gated"
		}
		t.Run(name, func(t *testing.T) {
			w := newWriter(t)
			archiveID := sourceRuntimeArchiveID(t)
			alphaKey, betaKey := sourceRuntimeKey(t), sourceRuntimeKey(t)
			alphaDocs, betaDocs := newDocServer(t), newDocServer(t)
			blobs, hashes := w.ingestSlot(100, 9101)
			alphaBlob := blobCID(t, blobs[0])
			alpha := buildOverlayHeadWithRows(t, w, "alpha", 96, 100,
				[]archive.RefRow{{Slot: 100, VHs: hashes}})
			beta := buildDocumentHead(t, w, "beta", 96, 100, testSegBits, testFanout)
			alphaDocs.set(sourceRuntimeDocument(t, w, alphaKey, archiveID, 1, time.Unix(1, 0), entry(alpha.Info())))
			betaDocs.set(sourceRuntimeDocument(t, w, betaKey, archiveID, 1, time.Unix(1, 0), entry(beta.Info())))
			if err := w.store.Blocks().DeleteBlock(t.Context(), alphaBlob); err != nil {
				t.Fatalf("hiding alpha closure blob: %v", err)
			}
			sources := []follow.SourceConfig{
				{ID: "alpha-writer", URL: alphaDocs.url, PubKey: alphaKey.Public().(ed25519.PublicKey), AllowedHeads: []string{"alpha"}},
				{ID: "beta-writer", URL: betaDocs.url, PubKey: betaKey.Public().(ed25519.PublicKey), AllowedHeads: []string{"beta"}},
			}
			f := newFollower(t, w, func(cfg *follow.Config) {
				cfg.Heads = map[string]pinning.Policy{"alpha": pinning.Full(), "beta": pinning.Full()}
				cfg.FetchTimeout = 100 * time.Millisecond
				configureSourceRuntime(t, cfg, archiveID, sources)
				if legacy {
					cfg.Local = sourceRuntimePlainBlockstore{Blockstore: cfg.Local}
				}
			})
			if !legacy {
				epoch, err := f.store.Epochs().Begin()
				if err != nil {
					t.Fatal(err)
				}
				defer epoch.End()
			}
			err := f.pollErr()
			if err == nil || !strings.Contains(err.Error(), "closure") {
				t.Fatalf("Poll error = %v, want alpha closure failure", err)
			}
			if _, ok := f.heads.Get("alpha"); ok {
				t.Fatal("head with unavailable retained closure became serviceable")
			}
			if got, ok := f.heads.Get("beta"); !ok || got.Root() != beta.Root() {
				t.Fatalf("independent beta = %v ok=%t, want %s", got, ok, beta.Root())
			}
		})
	}
}

func TestSourceRuntimeDarkBoundaryClosureFailureRestoresIndependentSibling(t *testing.T) {
	w := newWriter(t)
	archiveID := sourceRuntimeArchiveID(t)
	mutableKey, finalizedKey := sourceRuntimeKey(t), sourceRuntimeKey(t)
	mutableDocs, finalizedDocs := newDocServer(t), newDocServer(t)
	blobs, hashes := w.ingestSlot(104, 9201)
	boundaryBlob := blobCID(t, blobs[0])
	boundary := buildOverlayHeadWithRows(t, w, overlayFilteredHead, 96, 104,
		[]archive.RefRow{{Slot: 104, VHs: hashes}})
	mutable := buildDocumentHead(t, w, testHead, 105, 111, testSegBits, testFanout)
	witness := buildDocumentHead(t, w, testHandoffHead, 96, 104, testSegBits, testFanout)
	beta := buildDocumentHead(t, w, "beta", 96, 100, testSegBits, testFanout)
	mutableUnsigned := filteredOverlayDocument(t, w, boundary, mutable, witness, 1)
	mutableUnsigned.V = server.LogicalArchiveDocVersion
	mutableArchiveID := archiveID
	mutableUnsigned.ArchiveID = &mutableArchiveID
	mutableDocs.set(sign(t, mutableKey, mutableUnsigned))
	finalizedDocs.set(sourceRuntimeDocument(t, w, finalizedKey, archiveID, 1, time.Unix(1, 0),
		entry(boundary.Info()), entry(beta.Info())))
	sources := []follow.SourceConfig{
		{ID: "mutable-writer", URL: mutableDocs.url, PubKey: mutableKey.Public().(ed25519.PublicKey), AllowedHeads: []string{testHead}},
		{ID: "finalized-writer", URL: finalizedDocs.url, PubKey: finalizedKey.Public().(ed25519.PublicKey), AllowedHeads: []string{overlayFilteredHead, "beta"}},
	}
	configure := func(cfg *follow.Config) {
		cfg.Heads = map[string]pinning.Policy{
			overlayFilteredHead: pinning.Full(), testHead: pinning.Full(), "beta": pinning.Full(),
		}
		cfg.ExpectedKinds = map[string]server.HeadKind{testHead: server.UnfinalizedMutable}
		cfg.ExpectedHandoffs = map[string]string{testHead: testHandoffHead}
		cfg.OverlayFinalizedHeads = map[string]string{testHead: overlayFilteredHead}
		cfg.MaxMutableWindowSlots = map[string]uint64{testHead: 32}
		cfg.FetchTimeout = 100 * time.Millisecond
		configureSourceRuntime(t, cfg, archiveID, sources)
		cfg.Local = sourceRuntimePlainBlockstore{Blockstore: cfg.Local}
	}
	f := newFollower(t, w, configure)
	f.poll()
	if !f.hasLocally(boundaryBlob) {
		t.Fatalf("initial retained boundary closure lacks blob %s", boundaryBlob)
	}
	if err := f.store.Blocks().DeleteBlock(t.Context(), boundaryBlob); err != nil {
		t.Fatalf("hiding follower boundary blob: %v", err)
	}
	if err := w.store.Blocks().DeleteBlock(t.Context(), boundaryBlob); err != nil {
		t.Fatalf("hiding writer boundary blob: %v", err)
	}

	restarted := f.restart(t, w, configure)
	err := restarted.Poll(t.Context())
	if err == nil || !strings.Contains(err.Error(), "closure") {
		t.Fatalf("dark recovery Poll error = %v, want boundary closure failure", err)
	}
	if got := follow.HeadAdopted(restarted, "beta"); got != beta.Root() {
		t.Fatalf("independent durable beta = %s, want %s", got, beta.Root())
	}
	if got := follow.HeadAdopted(restarted, overlayFilteredHead); got.Defined() {
		t.Fatalf("boundary with missing closure became serviceable at %s", got)
	}
	if got := follow.HeadAdopted(restarted, testHead); got.Defined() {
		t.Fatalf("mutable dependent of missing boundary became serviceable at %s", got)
	}
}

func TestSourceRuntimeQuarantinedBoundaryDoesNotAbortIndependentPlanOrPhysicalMutable(t *testing.T) {
	w := newWriter(t)
	archiveID := sourceRuntimeArchiveID(t)
	mutableKey, finalizedKey := sourceRuntimeKey(t), sourceRuntimeKey(t)
	mutableDocs, finalizedDocs := newDocServer(t), newDocServer(t)
	boundary := buildDocumentHead(t, w, overlayFilteredHead, 96, 104, testSegBits, testFanout)
	mutable := buildDocumentHead(t, w, testHead, 105, 111, testSegBits, testFanout)
	witness := buildDocumentHead(t, w, testHandoffHead, 96, 104, testSegBits, testFanout)
	mutableUnsigned := filteredOverlayDocument(t, w, boundary, mutable, witness, 1)
	mutableUnsigned.V = server.LogicalArchiveDocVersion
	mutableArchiveID := archiveID
	mutableUnsigned.ArchiveID = &mutableArchiveID
	mutableDocs.set(sign(t, mutableKey, mutableUnsigned))
	finalizedDocs.set(sourceRuntimeDocument(t, w, finalizedKey, archiveID, 1, time.Unix(1, 0), entry(boundary.Info())))
	sources := []follow.SourceConfig{
		{ID: "mutable-writer", URL: mutableDocs.url, PubKey: mutableKey.Public().(ed25519.PublicKey), AllowedHeads: []string{testHead, "beta"}},
		{ID: "finalized-writer", URL: finalizedDocs.url, PubKey: finalizedKey.Public().(ed25519.PublicKey), AllowedHeads: []string{overlayFilteredHead}},
	}
	configure := func(cfg *follow.Config) {
		cfg.Heads = map[string]pinning.Policy{
			overlayFilteredHead: pinning.Full(), testHead: pinning.Full(), "beta": pinning.Full(),
		}
		cfg.ExpectedKinds = map[string]server.HeadKind{testHead: server.UnfinalizedMutable}
		cfg.ExpectedHandoffs = map[string]string{testHead: testHandoffHead}
		cfg.OverlayFinalizedHeads = map[string]string{testHead: overlayFilteredHead}
		cfg.MaxMutableWindowSlots = map[string]uint64{testHead: 32}
		configureSourceRuntime(t, cfg, archiveID, sources)
	}
	f := newFollower(t, w, configure)
	f.poll()
	if err := follow.QuarantineHead(f.f, overlayFilteredHead, "test boundary quarantine"); err == nil {
		t.Fatal("QuarantineHead returned nil")
	}
	if got, ok := f.heads.Get(testHead); !ok || got.Root() != mutable.Root() {
		t.Fatalf("physical mutable after boundary quarantine = %v ok=%t, want %s", got, ok, mutable.Root())
	}

	// The same mutable generation at a newer source revision now accompanies an
	// independent beta. Its durable finalized checkpoint still exists, but the
	// registry correctly reports that boundary unavailable. Drop only the new
	// mutable serving plan; beta must commit and the prior independently valid
	// physical mutable generation remains available.
	beta := buildDocumentHead(t, w, "beta", 96, 100, testSegBits, testFanout)
	revision := uint64(2)
	mutableUnsigned.Revision = &revision
	mutableUnsigned.UpdatedAt = time.Unix(2, 0).UTC().Format(time.RFC3339)
	mutableUnsigned.Heads = append(mutableUnsigned.Heads, entry(beta.Info()))
	mutableDocs.set(sign(t, mutableKey, mutableUnsigned))
	finalizedDocs.status(http.StatusServiceUnavailable)
	err := f.pollErr()
	if err == nil || !strings.Contains(err.Error(), "cannot be selected before finalized boundary") {
		t.Fatalf("quarantined boundary Poll error = %v", err)
	}
	if got, ok := f.heads.Get("beta"); !ok || got.Root() != beta.Root() {
		t.Fatalf("independent beta = %v ok=%t, want %s", got, ok, beta.Root())
	}
	if got, ok := f.heads.Get(testHead); !ok || got.Root() != mutable.Root() {
		t.Fatalf("physical mutable after refused retry = %v ok=%t, want prior %s", got, ok, mutable.Root())
	}
}

func TestSourceRuntimeSameDocumentCallbackRepeatsAfterPartialHeadRepair(t *testing.T) {
	w := newWriter(t)
	archiveID := sourceRuntimeArchiveID(t)
	key := sourceRuntimeKey(t)
	docs := newDocServer(t)
	alpha := buildDocumentHead(t, w, "alpha", 96, 100, testSegBits, testFanout)
	beta := buildDocumentHead(t, w, "beta", 96, 100, testSegBits, testFanout)
	alphaBlock, err := w.store.Blocks().Get(t.Context(), alpha.Root())
	if err != nil {
		t.Fatalf("reading alpha root: %v", err)
	}
	if err := w.store.Blocks().DeleteBlock(t.Context(), alpha.Root()); err != nil {
		t.Fatalf("temporarily hiding alpha root: %v", err)
	}
	docs.set(sourceRuntimeDocument(t, w, key, archiveID, 1, time.Unix(1, 0), entry(alpha.Info()), entry(beta.Info())))
	sources := []follow.SourceConfig{{
		ID: "writer-a", URL: docs.url, PubKey: key.Public().(ed25519.PublicKey), AllowedHeads: []string{"alpha", "beta"},
	}}
	callbackCalls := 0
	mx := metrics.New()
	f := newFollower(t, w, func(cfg *follow.Config) {
		cfg.Heads = map[string]pinning.Policy{"alpha": pinning.Full(), "beta": pinning.Full()}
		configureSourceRuntime(t, cfg, archiveID, sources)
		cfg.Metrics = mx
		cfg.OnAdmittedSourceDocument = func(blocks.Block, server.Doc, []string) error {
			callbackCalls++
			return nil
		}
	})
	if err := f.pollErr(); err == nil {
		t.Fatal("first partial Poll returned nil despite unavailable alpha")
	}
	if _, ok := f.heads.Get("alpha"); ok {
		t.Fatal("unavailable alpha became serviceable")
	}
	if got, ok := f.heads.Get("beta"); !ok || got.Root() != beta.Root() {
		t.Fatalf("healthy beta = %v ok=%t, want %s", got, ok, beta.Root())
	}
	if got := scrapeSeries(t, mx, `bloar_follow_source_selected{head="alpha",source="writer-a"}`); got != 0 {
		t.Fatalf("closure-filtered alpha selection = %g, want 0", got)
	}
	if got := scrapeSeries(t, mx, `bloar_follow_source_selected{head="beta",source="writer-a"}`); got != 1 {
		t.Fatalf("committed beta selection = %g, want 1", got)
	}
	if callbackCalls != 1 {
		t.Fatalf("callback calls after partial admission = %d, want 1", callbackCalls)
	}

	if err := w.store.Blocks().Put(t.Context(), alphaBlock); err != nil {
		t.Fatalf("restoring alpha root: %v", err)
	}
	if err := f.pollErr(); err != nil {
		t.Fatalf("repairing alpha with exact same document: %v", err)
	}
	if got, ok := f.heads.Get("alpha"); !ok || got.Root() != alpha.Root() {
		t.Fatalf("repaired alpha = %v ok=%t, want %s", got, ok, alpha.Root())
	}
	if got := scrapeSeries(t, mx, `bloar_follow_source_selected{head="alpha",source="writer-a"}`); got != 1 {
		t.Fatalf("repaired committed alpha selection = %g, want 1", got)
	}
	if callbackCalls != 2 {
		t.Fatalf("callback calls after same-CID repair = %d, want 2", callbackCalls)
	}
	if err := f.pollErr(); err != nil {
		t.Fatalf("exact no-op retry: %v", err)
	}
	if callbackCalls != 2 {
		t.Fatalf("callback calls after true no-op = %d, want 2", callbackCalls)
	}
}

func TestSourceRuntimeCallbackFailureDoesNotStarveIndependentSource(t *testing.T) {
	w := newWriter(t)
	archiveID := sourceRuntimeArchiveID(t)
	alphaKey, betaKey := sourceRuntimeKey(t), sourceRuntimeKey(t)
	alphaDocs, betaDocs := newDocServer(t), newDocServer(t)
	alpha := buildDocumentHead(t, w, "alpha", 96, 100, testSegBits, testFanout)
	beta := buildDocumentHead(t, w, "beta", 96, 100, testSegBits, testFanout)
	alphaDocs.set(sourceRuntimeDocument(t, w, alphaKey, archiveID, 1, time.Unix(1, 0), entry(alpha.Info())))
	betaDocs.set(sourceRuntimeDocument(t, w, betaKey, archiveID, 1, time.Unix(1, 0), entry(beta.Info())))
	sources := []follow.SourceConfig{
		{ID: "alpha-writer", URL: alphaDocs.url, PubKey: alphaKey.Public().(ed25519.PublicKey), AllowedHeads: []string{"alpha"}},
		{ID: "beta-writer", URL: betaDocs.url, PubKey: betaKey.Public().(ed25519.PublicKey), AllowedHeads: []string{"beta"}},
	}
	alphaCalls, betaCalls := 0, 0
	f := newFollower(t, w, func(cfg *follow.Config) {
		cfg.Heads = map[string]pinning.Policy{"alpha": pinning.Full(), "beta": pinning.Full()}
		configureSourceRuntime(t, cfg, archiveID, sources)
		cfg.OnAdmittedSourceDocument = func(_ blocks.Block, document server.Doc, _ []string) error {
			if len(document.Heads) == 1 && document.Heads[0].Name == "alpha" {
				alphaCalls++
				if alphaCalls == 1 {
					return errors.New("test alpha callback outage")
				}
				return nil
			}
			betaCalls++
			return nil
		}
	})
	err := f.pollErr()
	if err == nil || !strings.Contains(err.Error(), "test alpha callback outage") {
		t.Fatalf("first callback Poll error = %v", err)
	}
	if alphaCalls != 1 || betaCalls != 1 {
		t.Fatalf("callback calls after alpha failure = alpha:%d beta:%d, want 1/1", alphaCalls, betaCalls)
	}
	if got, ok := f.heads.Get("beta"); !ok || got.Root() != beta.Root() {
		t.Fatalf("beta serving after peer callback failure = %v ok=%t", got, ok)
	}
	if err := f.pollErr(); err != nil {
		t.Fatalf("retrying failed source callback: %v", err)
	}
	if alphaCalls != 2 || betaCalls != 1 {
		t.Fatalf("callback calls after retry = alpha:%d beta:%d, want 2/1", alphaCalls, betaCalls)
	}
}

func TestSourceRuntimeCommitCancellationPersistsNoCheckpointOrSourceFloor(t *testing.T) {
	w := newWriter(t)
	archiveID := sourceRuntimeArchiveID(t)
	key := sourceRuntimeKey(t)
	docs := newDocServer(t)
	alpha := buildDocumentHead(t, w, "alpha", 96, 100, testSegBits, testFanout)
	docs.set(sourceRuntimeDocument(t, w, key, archiveID, 1, time.Unix(1, 0), entry(alpha.Info())))
	sources := []follow.SourceConfig{{
		ID: "alpha-writer", URL: docs.url, PubKey: key.Public().(ed25519.PublicKey), AllowedHeads: []string{"alpha"},
	}}
	f := newFollower(t, w, func(cfg *follow.Config) {
		cfg.Heads = map[string]pinning.Policy{"alpha": pinning.Full()}
		configureSourceRuntime(t, cfg, archiveID, sources)
	})
	ctx, cancel := context.WithCancel(t.Context())
	follow.SetBetweenPhasesHook(cancel)
	t.Cleanup(func() { follow.SetBetweenPhasesHook(nil) })
	err := f.f.Poll(ctx)
	follow.SetBetweenPhasesHook(nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Poll error = %v, want context.Canceled", err)
	}
	if _, _, _, _, ok, err := follow.ReadCheckpoint(f.store.KV(), "alpha"); err != nil || ok {
		t.Fatalf("checkpoint after canceled commit: ok=%t err=%v", ok, err)
	}
	if revision, ok, err := follow.ReadSourcePublicationFloor(f.store.KV(), archiveID, "alpha-writer"); err != nil || ok {
		t.Fatalf("source floor after canceled commit = %d ok=%t err=%v", revision, ok, err)
	}
}

func TestSourceRuntimeIgnoresMutableClaimFromNonAuthority(t *testing.T) {
	w := newWriter(t)
	archiveID := sourceRuntimeArchiveID(t)
	writerAKey, writerBKey := sourceRuntimeKey(t), sourceRuntimeKey(t)
	writerADocs, writerBDocs := newDocServer(t), newDocServer(t)
	owned := buildMutableGeneration(t, w, 96, 103)
	unowned := buildMutableGeneration(t, w, 104, 111)
	ownedDoc := revisionedUnsigned(w, owned, 1, time.Unix(1, 0), server.UnfinalizedMutable)
	unownedDoc := revisionedUnsigned(w, unowned, 1, time.Unix(1, 0), server.UnfinalizedMutable)
	ownedDoc.V, unownedDoc.V = server.LogicalArchiveDocVersion, server.LogicalArchiveDocVersion
	ownedID, unownedID := archiveID, archiveID
	ownedDoc.ArchiveID, unownedDoc.ArchiveID = &ownedID, &unownedID
	writerADocs.set(sign(t, writerAKey, ownedDoc))
	writerBDocs.set(sign(t, writerBKey, unownedDoc))
	sources := []follow.SourceConfig{
		{ID: "writer-a", URL: writerADocs.url, PubKey: writerAKey.Public().(ed25519.PublicKey), AllowedHeads: []string{testHandoffHead, testHead}},
		// writer-b may independently corroborate finalized history, but its signed
		// document's mutable line is outside local authority policy.
		{ID: "writer-b", URL: writerBDocs.url, PubKey: writerBKey.Public().(ed25519.PublicKey), AllowedHeads: []string{testHandoffHead}},
	}
	f := newFollower(t, w, func(cfg *follow.Config) {
		configureMutableFollower(cfg, 16)
		configureSourceRuntime(t, cfg, archiveID, sources)
	})
	f.poll()
	if got := follow.HeadAdopted(f.f, testHead); got != owned.Root() {
		t.Fatalf("mutable root = %s, want configured authority root %s", got, owned.Root())
	}
	cp := readSourceRuntimeV4(t, f.store.KV(), testHead)
	if cp.sourceID != "writer-a" {
		t.Fatalf("mutable checkpoint source = %q, want writer-a", cp.sourceID)
	}

	// With writer-a unavailable, writer-b cannot use a newer locally signed
	// mutable generation to replace the last-good single-authority snapshot.
	writerADocs.status(http.StatusServiceUnavailable)
	revision := uint64(2)
	unownedDoc.Revision = &revision
	unownedDoc.UpdatedAt = time.Unix(2, 0).UTC().Format(time.RFC3339)
	writerBDocs.set(sign(t, writerBKey, unownedDoc))
	if err := f.pollErr(); err != nil {
		t.Fatalf("poll with only non-authoritative mutable claimant: %v", err)
	}
	if got := follow.HeadAdopted(f.f, testHead); got != owned.Root() {
		t.Fatalf("non-authority changed mutable root to %s, want last-good %s", got, owned.Root())
	}
	after := readSourceRuntimeV4(t, f.store.KV(), testHead)
	if after.sourceID != "writer-a" || after.revision != cp.revision {
		t.Fatalf("non-authority changed mutable provenance from %+v to %+v", cp, after)
	}
}
