package follow_test

import (
	"context"
	"crypto/ed25519"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"

	"github.com/blobarchive/bloar/archive"
	"github.com/blobarchive/bloar/follow"
	"github.com/blobarchive/bloar/metrics"
	"github.com/blobarchive/bloar/pinning"
	"github.com/blobarchive/bloar/server"
)

func logicalArchiveDocument(t *testing.T, key ed25519.PrivateKey, archiveID server.ArchiveID, unsigned server.Unsigned) []byte {
	t.Helper()
	unsigned.V = server.LogicalArchiveDocVersion
	id := archiveID
	unsigned.ArchiveID = &id
	return sign(t, key, unsigned)
}

func requireRuntimeHeadRoot(t *testing.T, f *follower, name string, want string) {
	t.Helper()
	head, ok := f.heads.Get(name)
	if !ok || head.Root().String() != want {
		t.Fatalf("head %q = %v ok=%t, want root %s", name, head, ok, want)
	}
}

func requireRuntimeConflictLatch(t *testing.T, f *follower, archiveID server.ArchiveID, head string) follow.ConflictRecord {
	t.Helper()
	record, ok, err := follow.LoadConflictLatch(f.store.KV(), archiveID, head)
	if err != nil {
		t.Fatalf("loading conflict latch for %q: %v", head, err)
	}
	if !ok {
		t.Fatalf("head %q has no durable conflict latch", head)
	}
	return record
}

func TestConflictLatchPersistsFreezesAndAllowsUnrelatedProgress(t *testing.T) {
	w := newWriter(t)
	_, vhs := w.ingestSlot(100, 31_001, 31_002)
	archiveID := sourceRuntimeArchiveID(t)
	keyA, keyB, keyC := sourceRuntimeKey(t), sourceRuntimeKey(t), sourceRuntimeKey(t)
	docsA, docsB, docsC := newDocServer(t), newDocServer(t), newDocServer(t)

	alphaGood := buildOverlayHeadWithRows(t, w, "alpha", 96, 100, []archive.RefRow{{Slot: 100, VHs: vhs[:1]}})
	alphaBad := buildOverlayHeadWithRows(t, w, "alpha", 96, 100, []archive.RefRow{{Slot: 100, VHs: vhs[1:]}})
	beta100 := buildDocumentHead(t, w, "beta", 96, 100, testSegBits, testFanout)
	beta101 := buildDocumentHead(t, w, "beta", 96, 101, testSegBits, testFanout)
	beta102 := buildDocumentHead(t, w, "beta", 96, 102, testSegBits, testFanout)

	docsA.set(sourceRuntimeDocument(t, w, keyA, archiveID, 1, time.Unix(1, 0),
		entry(alphaGood.Info()), entry(beta100.Info())))
	docsB.status(http.StatusServiceUnavailable)
	docsC.status(http.StatusServiceUnavailable)
	sources := []follow.SourceConfig{
		{ID: "writer-a", URL: docsA.url, PubKey: keyA.Public().(ed25519.PublicKey), AllowedHeads: []string{"alpha", "beta"}},
		{ID: "writer-b", URL: docsB.url, PubKey: keyB.Public().(ed25519.PublicKey), AllowedHeads: []string{"alpha"}},
		{ID: "writer-c", URL: docsC.url, PubKey: keyC.Public().(ed25519.PublicKey), AllowedHeads: []string{"alpha"}},
	}
	callbackCalls := 0
	configure := func(cfg *follow.Config) {
		cfg.Heads = map[string]pinning.Policy{"alpha": pinning.Full(), "beta": pinning.Full()}
		configureSourceRuntime(t, cfg, archiveID, sources)
		cfg.OnAdmittedSourceDocument = func(blocks.Block, server.Doc, []string) error {
			callbackCalls++
			return nil
		}
	}
	f := newFollower(t, w, configure)
	f.poll()
	if callbackCalls != 1 {
		t.Fatalf("initial callback calls = %d, want 1", callbackCalls)
	}

	// A and B now make incompatible claims at the same finalized coverage while
	// A independently advances beta. The latch/floors must be durable before the
	// unrelated beta plan commits, and neither raw conflicting document may reach
	// the whole-document callback.
	docsA.set(sourceRuntimeDocument(t, w, keyA, archiveID, 2, time.Unix(2, 0),
		entry(alphaGood.Info()), entry(beta101.Info())))
	docsB.set(sourceRuntimeDocument(t, w, keyB, archiveID, 1, time.Unix(1, 0), entry(alphaBad.Info())))
	docsB.status(0)
	docsC.set(sourceRuntimeDocument(t, w, keyC, archiveID, 1, time.Unix(1, 0), entry(alphaBad.Info())))
	docsC.status(0)
	// Cancel at the ordinary admission barrier. The conflict latch and every
	// participant floor must already be durable even though the independent beta
	// plan cannot reach its normal commit. This distinguishes the early safety
	// cut from the later all-admissions batch.
	ctx, cancel := context.WithCancel(t.Context())
	follow.SetBetweenPhasesHook(cancel)
	t.Cleanup(func() { follow.SetBetweenPhasesHook(nil) })
	err := f.f.Poll(ctx)
	follow.SetBetweenPhasesHook(nil)
	var latched *follow.ConflictLatchedError
	if !errors.As(err, &latched) {
		t.Fatalf("conflicting poll error = %T (%v), want durable ConflictLatchedError", err, err)
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("conflicting poll error = %v, want later admission cancellation", err)
	}
	record := requireRuntimeConflictLatch(t, f, archiveID, "alpha")
	if record.Sequence != 1 || record.PairCount != 2 {
		t.Fatalf("initial conflict record = sequence %d pair_count %d, want 1/2", record.Sequence, record.PairCount)
	}
	for source, revision := range map[string]uint64{"writer-a": 2, "writer-b": 1, "writer-c": 1} {
		got, ok, err := follow.ReadSourcePublicationFloor(f.store.KV(), archiveID, source)
		if err != nil || !ok || got != revision {
			t.Fatalf("conflict participant %q floor = %d ok=%t err=%v, want %d", source, got, ok, err, revision)
		}
	}
	requireRuntimeHeadRoot(t, f, "alpha", alphaGood.Root().String())
	requireRuntimeHeadRoot(t, f, "beta", beta100.Root().String())
	if callbackCalls != 1 {
		t.Fatalf("callback calls after conflict = %d, want conflicting whole documents suppressed", callbackCalls)
	}

	// Transport failure on every writer must not hide the already-durable latch.
	// The caller needs both the source outage and the operator-action condition.
	docsA.status(http.StatusServiceUnavailable)
	docsB.status(http.StatusServiceUnavailable)
	docsC.status(http.StatusServiceUnavailable)
	err = f.pollErr()
	latched = nil
	if !errors.As(err, &latched) || !strings.Contains(err.Error(), "status 503") {
		t.Fatalf("all-sources-unavailable error = %T (%v), want latch and transport failures", err, err)
	}
	docsA.status(0)
	docsB.status(0)
	docsC.status(0)

	// With the injected failure gone, the active latch remains local to alpha and
	// beta can reach the ordinary admission commit.
	err = f.pollErr()
	latched = nil
	if !errors.As(err, &latched) {
		t.Fatalf("retry beside active latch error = %T (%v), want ConflictLatchedError", err, err)
	}
	requireRuntimeHeadRoot(t, f, "alpha", alphaGood.Root().String())
	requireRuntimeHeadRoot(t, f, "beta", beta101.Root().String())

	// Restart with a fresh registry, prove the exact durable last-good snapshot is
	// restored, then make both writers converge. Convergence does not clear an
	// operator latch; alpha remains frozen while beta and replay floors advance.
	next := f.restart(t, w, configure)
	if err := next.Resume(t.Context()); err != nil {
		t.Fatalf("resuming active conflict latch: %v", err)
	}
	requireRuntimeHeadRoot(t, f, "alpha", alphaGood.Root().String())
	requireRuntimeHeadRoot(t, f, "beta", beta101.Root().String())
	docsA.set(sourceRuntimeDocument(t, w, keyA, archiveID, 3, time.Unix(3, 0),
		entry(alphaGood.Info()), entry(beta102.Info())))
	docsB.set(sourceRuntimeDocument(t, w, keyB, archiveID, 2, time.Unix(2, 0), entry(alphaGood.Info())))
	docsC.set(sourceRuntimeDocument(t, w, keyC, archiveID, 2, time.Unix(2, 0), entry(alphaGood.Info())))
	err = next.Poll(t.Context())
	latched = nil
	if !errors.As(err, &latched) {
		t.Fatalf("post-restart converged poll error = %T (%v), want active ConflictLatchedError", err, err)
	}
	after := requireRuntimeConflictLatch(t, f, archiveID, "alpha")
	if after.Sequence != record.Sequence || after.EvidenceID != record.EvidenceID {
		t.Fatalf("convergence replaced conflict latch: before seq/id=%d/%x after=%d/%x",
			record.Sequence, record.EvidenceID, after.Sequence, after.EvidenceID)
	}
	requireRuntimeHeadRoot(t, f, "alpha", alphaGood.Root().String())
	requireRuntimeHeadRoot(t, f, "beta", beta102.Root().String())
	if callbackCalls != 1 {
		t.Fatalf("callback calls after restart/convergence = %d, want latched documents still suppressed", callbackCalls)
	}
}

func TestConflictLatchFreshVersusDurableCanAttributeSameSource(t *testing.T) {
	w := newWriter(t)
	_, vhs := w.ingestSlot(100, 32_001, 32_002)
	archiveID := sourceRuntimeArchiveID(t)
	key := sourceRuntimeKey(t)
	docs := newDocServer(t)
	good := buildOverlayHeadWithRows(t, w, "alpha", 96, 100, []archive.RefRow{{Slot: 100, VHs: vhs[:1]}})
	bad := buildOverlayHeadWithRows(t, w, "alpha", 96, 100, []archive.RefRow{{Slot: 100, VHs: vhs[1:]}})
	docs.set(sourceRuntimeDocument(t, w, key, archiveID, 1, time.Unix(1, 0), entry(good.Info())))
	sources := []follow.SourceConfig{{
		ID: "writer-a", URL: docs.url, PubKey: key.Public().(ed25519.PublicKey), AllowedHeads: []string{"alpha"},
	}}
	callbackCalls := 0
	f := newFollower(t, w, func(cfg *follow.Config) {
		cfg.Heads = map[string]pinning.Policy{"alpha": pinning.Full()}
		configureSourceRuntime(t, cfg, archiveID, sources)
		cfg.OnAdmittedSourceDocument = func(blocks.Block, server.Doc, []string) error {
			callbackCalls++
			return nil
		}
	})
	f.poll()

	// The signer-local generation legitimately advances, but its finalized claim
	// forks from that same source's durable v4 checkpoint. Source and durable are
	// distinct evidence roles even though both carry source_id=writer-a.
	docs.set(sourceRuntimeDocument(t, w, key, archiveID, 2, time.Unix(2, 0), entry(bad.Info())))
	err := f.pollErr()
	var latched *follow.ConflictLatchedError
	if !errors.As(err, &latched) {
		t.Fatalf("fresh-vs-durable poll error = %T (%v), want ConflictLatchedError", err, err)
	}
	record := requireRuntimeConflictLatch(t, f, archiveID, "alpha")
	if record.Left.Role != follow.ConflictCandidateSource || record.Right.Role != follow.ConflictCandidateDurable {
		t.Fatalf("same-source evidence roles = %d/%d, want source/durable", record.Left.Role, record.Right.Role)
	}
	if record.Left.SourceID != "writer-a" || record.Right.SourceID != "writer-a" || record.Right.CheckpointVersion != 4 {
		t.Fatalf("same-source evidence attribution = left:%+v right:%+v", record.Left, record.Right)
	}
	requireRuntimeHeadRoot(t, f, "alpha", good.Root().String())
	if callbackCalls != 1 {
		t.Fatalf("callback calls after same-source conflict = %d, want conflicting generation suppressed", callbackCalls)
	}
}

func TestConflictLatchAllowsMutableOnlyWithinFrozenBoundary(t *testing.T) {
	w := newWriter(t)
	_, vhs := w.ingestSlot(103, 33_001)
	archiveID := sourceRuntimeArchiveID(t)
	keyA, keyB := sourceRuntimeKey(t), sourceRuntimeKey(t)
	docsA, docsB := newDocServer(t), newDocServer(t)

	boundaryGood := buildOverlayHeadWithRows(t, w, overlayFilteredHead, 96, 103, nil)
	boundaryBad := buildOverlayHeadWithRows(t, w, overlayFilteredHead, 96, 103,
		[]archive.RefRow{{Slot: 103, VHs: vhs}})
	boundaryAhead := buildOverlayHeadWithRows(t, w, overlayFilteredHead, 96, 104, nil)
	mutableA := buildOverlayHeadWithRows(t, w, testHead, 104, 111, nil)
	mutableB := buildOverlayHeadWithRows(t, w, testHead, 104, 112, nil)
	mutableC := buildOverlayHeadWithRows(t, w, testHead, 104, 113, nil)
	witness := buildOverlayHeadWithRows(t, w, testHandoffHead, 96, 103, nil)

	unsignedA := filteredOverlayDocument(t, w, boundaryGood, mutableA, witness, 1)
	docsA.set(logicalArchiveDocument(t, keyA, archiveID, unsignedA))
	docsB.status(http.StatusServiceUnavailable)
	sources := []follow.SourceConfig{
		{ID: "writer-a", URL: docsA.url, PubKey: keyA.Public().(ed25519.PublicKey), AllowedHeads: []string{overlayFilteredHead, testHead}},
		{ID: "writer-b", URL: docsB.url, PubKey: keyB.Public().(ed25519.PublicKey), AllowedHeads: []string{overlayFilteredHead}},
	}
	callbackCalls := 0
	f := newFollower(t, w, func(cfg *follow.Config) {
		cfg.Heads = map[string]pinning.Policy{overlayFilteredHead: pinning.Full(), testHead: pinning.Full()}
		cfg.ExpectedKinds = map[string]server.HeadKind{testHead: server.UnfinalizedMutable}
		cfg.ExpectedHandoffs = map[string]string{testHead: testHandoffHead}
		cfg.OverlayFinalizedHeads = map[string]string{testHead: overlayFilteredHead}
		cfg.MaxMutableWindowSlots = map[string]uint64{testHead: 32}
		configureSourceRuntime(t, cfg, archiveID, sources)
		cfg.OnAdmittedSourceDocument = func(blocks.Block, server.Doc, []string) error {
			callbackCalls++
			return nil
		}
	})
	f.poll()

	// Latch the finalized overlay, but carry an exact copy of its frozen boundary
	// beside a newer mutable generation. The mutable line is safe and useful even
	// though its containing raw document is not eligible for the callback.
	unsignedB := filteredOverlayDocument(t, w, boundaryGood, mutableB, witness, 2)
	docsA.set(logicalArchiveDocument(t, keyA, archiveID, unsignedB))
	docsB.set(sourceRuntimeDocument(t, w, keyB, archiveID, 1, time.Unix(1, 0), entry(boundaryBad.Info())))
	docsB.status(0)
	err := f.pollErr()
	var latched *follow.ConflictLatchedError
	if !errors.As(err, &latched) {
		t.Fatalf("boundary conflict poll error = %T (%v), want ConflictLatchedError", err, err)
	}
	requireRuntimeHeadRoot(t, f, overlayFilteredHead, boundaryGood.Root().String())
	requireRuntimeHeadRoot(t, f, testHead, mutableB.Root().String())
	if callbackCalls != 1 {
		t.Fatalf("callback calls after boundary latch = %d, want whole documents suppressed", callbackCalls)
	}

	// A same-document boundary ahead of the exact frozen checkpoint cannot license
	// another mutable replacement. The active latch remains head-local: it rejects
	// only this dependent plan rather than quarantining the source or whole poll.
	unsignedC := filteredOverlayDocument(t, w, boundaryAhead, mutableC, witness, 3)
	docsA.set(logicalArchiveDocument(t, keyA, archiveID, unsignedC))
	err = f.pollErr()
	if err == nil || !strings.Contains(err.Error(), "not covered by the selected finalized snapshot") {
		t.Fatalf("mutable-above-frozen-boundary poll error = %v", err)
	}
	requireRuntimeHeadRoot(t, f, overlayFilteredHead, boundaryGood.Root().String())
	requireRuntimeHeadRoot(t, f, testHead, mutableB.Root().String())
	if callbackCalls != 1 {
		t.Fatalf("callback calls after rejected mutable boundary = %d, want latched documents suppressed", callbackCalls)
	}
}

func TestTransientIncomparableDoesNotLatchAndClearsOnlyAfterClosedConvergence(t *testing.T) {
	w := newWriter(t)
	w.ingestSlot(100, 34_001)
	archiveID := sourceRuntimeArchiveID(t)
	keyA, keyB := sourceRuntimeKey(t), sourceRuntimeKey(t)
	docsA, docsB := newDocServer(t), newDocServer(t)
	plain := entry(w.head.Info())
	docsA.set(sourceRuntimeDocument(t, w, keyA, archiveID, 1, time.Unix(1, 0), plain))
	docsB.status(http.StatusServiceUnavailable)
	sources := []follow.SourceConfig{
		{ID: "writer-a", URL: docsA.url, PubKey: keyA.Public().(ed25519.PublicKey), AllowedHeads: []string{testHead}},
		{ID: "writer-b", URL: docsB.url, PubKey: keyB.Public().(ed25519.PublicKey), AllowedHeads: []string{testHead}},
	}
	mx := metrics.New()
	f := newFollower(t, w, func(cfg *follow.Config) {
		configureSourceRuntime(t, cfg, archiveID, sources)
		cfg.Metrics = mx
	})
	f.poll()

	// A present manifest on only one otherwise-equal claim is intentionally
	// incomparable, not cryptographic conflict. It holds last-good and sets only
	// transient telemetry.
	tip := w.setManifest(cid.Undef, 34_002)
	withManifest := plain
	withManifest.Manifest = tip.String()
	docsA.set(sourceRuntimeDocument(t, w, keyA, archiveID, 2, time.Unix(2, 0), plain))
	docsB.set(sourceRuntimeDocument(t, w, keyB, archiveID, 1, time.Unix(1, 0), withManifest))
	docsB.status(0)
	err := f.pollErr()
	var incomparable *follow.FinalizedClaimsIncomparableError
	if !errors.As(err, &incomparable) {
		t.Fatalf("incomparable poll error = %T (%v), want FinalizedClaimsIncomparableError", err, err)
	}
	if _, ok, err := follow.LoadConflictLatch(f.store.KV(), archiveID, testHead); err != nil || ok {
		t.Fatalf("incomparable result created durable latch: ok=%t err=%v", ok, err)
	}
	if got := scrapeSeries(t, mx, `bloar_follow_incomparable_active{head="all"}`); got != 1 {
		t.Fatalf("incomparable active = %g, want 1", got)
	}
	if got := scrapeSeries(t, mx, `bloar_follow_incomparable_total{head="all"}`); got != 1 {
		t.Fatalf("incomparable total = %g, want 1", got)
	}

	// A lone comparable writer does not prove the earlier cross-writer relation
	// converged. Transport absence must leave the sticky gauge set.
	docsA.set(sourceRuntimeDocument(t, w, keyA, archiveID, 3, time.Unix(3, 0), plain))
	docsB.status(http.StatusServiceUnavailable)
	if err := f.pollErr(); err != nil {
		t.Fatalf("single healthy writer after incomparable result: %v", err)
	}
	if got := scrapeSeries(t, mx, `bloar_follow_incomparable_active{head="all"}`); got != 1 {
		t.Fatalf("incomparable active after missing peer = %g, want sticky 1", got)
	}

	// Only a complete, successfully compared source set clears the transient
	// condition. The counter remains the count of incomparable observations.
	docsA.set(sourceRuntimeDocument(t, w, keyA, archiveID, 4, time.Unix(4, 0), plain))
	docsB.set(sourceRuntimeDocument(t, w, keyB, archiveID, 2, time.Unix(2, 0), plain))
	docsB.status(0)
	if err := f.pollErr(); err != nil {
		t.Fatalf("closed source-set convergence: %v", err)
	}
	if got := scrapeSeries(t, mx, `bloar_follow_incomparable_active{head="all"}`); got != 0 {
		t.Fatalf("incomparable active after convergence = %g, want 0", got)
	}
	if got := scrapeSeries(t, mx, `bloar_follow_incomparable_total{head="all"}`); got != 1 {
		t.Fatalf("incomparable total after convergence = %g, want 1", got)
	}
	if _, ok, err := follow.LoadConflictLatch(f.store.KV(), archiveID, testHead); err != nil || ok {
		t.Fatalf("converged incomparable path created durable latch: ok=%t err=%v", ok, err)
	}
}
