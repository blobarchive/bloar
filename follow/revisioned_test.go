package follow_test

import (
	"crypto/ed25519"
	"encoding/json"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blobarchive/bloar/archive"
	"github.com/blobarchive/bloar/follow"
	"github.com/blobarchive/bloar/pinning"
	"github.com/blobarchive/bloar/server"
)

const testHandoffHead = "0-finalized-handoff"

func buildMutableGeneration(t *testing.T, w *writer, start, end uint64) *archive.Head {
	t.Helper()
	h, err := archive.BuildGeneration(t.Context(), archive.Config{
		Blocks: w.store.Blocks(), Resolver: w.cat, Cache: w.cache,
	}, archive.Params{
		Name: testHead, Net: testNet, OriginSlot: start, SegBits: testSegBits, FanoutBits: testFanout,
	}, nil, end)
	if err != nil {
		t.Fatalf("BuildGeneration([%d,%d]): %v", start, end, err)
	}
	return h
}

func revisionedUnsigned(w *writer, h *archive.Head, revision uint64, at time.Time, kind server.HeadKind) server.Unsigned {
	info := h.Info()
	e := entry(info)
	e.Kind = kind
	if kind == server.UnfinalizedMutable {
		start := info.OriginSlot
		e.WindowStart = &start

		// A mutable claim is licensed by an exact finalized witness in the same
		// authenticated document. Keep one deterministic F=103 witness across the
		// A/B/A fixtures: it touches A's end and B's start, so changing the mutable
		// generation exercises revision ordering without also regressing finalized
		// lineage.
		handoff, err := archive.BuildGeneration(w.t.Context(), archive.Config{
			Blocks: w.store.Blocks(), Resolver: w.cat, Cache: w.cache,
		}, archive.Params{
			Name: testHandoffHead, Net: testNet, OriginSlot: testOrigin,
			SegBits: testSegBits, FanoutBits: testFanout,
		}, nil, 103)
		if err != nil {
			w.t.Fatalf("BuildGeneration(%s): %v", testHandoffHead, err)
		}
		witness := entry(handoff.Info())
		frontier := *witness.SyncedTo
		sourceFinalized := frontier
		handoffSynced := frontier
		e.SourceHeadRoot = "0x" + strings.Repeat("11", 32)
		e.SourceFinalizedSlot = &sourceFinalized
		e.SourceFinalizedRoot = "0x" + strings.Repeat("22", 32)
		e.HandoffHead = testHandoffHead
		e.HandoffRoot = witness.Root
		e.HandoffSyncedTo = &handoffSynced

		return server.Unsigned{
			V: server.DocVersion, Net: testNet, UpdatedAt: at.UTC().Format(time.RFC3339),
			Multiaddrs: w.host.AnnounceAddrs(), Heads: []server.HeadEntry{e, witness}, Revision: &revision,
		}
	}
	return server.Unsigned{
		V: server.DocVersion, Net: testNet, UpdatedAt: at.UTC().Format(time.RFC3339),
		Multiaddrs: w.host.AnnounceAddrs(), Heads: []server.HeadEntry{e}, Revision: &revision,
	}
}

func configureMutableFollower(c *follow.Config, maxWindow uint64) {
	// Follow the finalized witness as well as the mutable head. Documents must
	// carry both, and the registry deliberately keeps mutable service hidden until
	// the exact witness is locally adopted and can bind its in-process lineage.
	c.Heads = map[string]pinning.Policy{
		testHandoffHead: pinning.Full(),
		testHead:        pinning.Full(),
	}
	c.ExpectedKinds = map[string]server.HeadKind{testHead: server.UnfinalizedMutable}
	c.ExpectedHandoffs = map[string]string{testHead: testHandoffHead}
	c.MaxMutableWindowSlots = map[string]uint64{testHead: maxWindow}
}

func mutableFollower(t *testing.T, w *writer, docs *docServer, maxWindow uint64) *follower {
	t.Helper()
	return newFollower(t, w, func(c *follow.Config) {
		c.URL = docs.url
		configureMutableFollower(c, maxWindow)
	})
}

func TestMutableFollowerRequiresPinnedDocumentAuthority(t *testing.T) {
	_, err := follow.New(follow.Config{
		Net:     testNet,
		DNSLink: "swarm.example",
		Routing: newMemRouting(),
		Heads: map[string]pinning.Policy{
			testHead: pinning.Full(),
		},
		ExpectedKinds: map[string]server.HeadKind{
			testHead: server.UnfinalizedMutable,
		},
		ExpectedHandoffs:      map[string]string{testHead: testHandoffHead},
		MaxMutableWindowSlots: map[string]uint64{testHead: 16},
	})
	if err == nil || !strings.Contains(err.Error(), "require a pinned") {
		t.Fatalf("follow.New with delegated mutable signer error = %v, want pinned-authority refusal", err)
	}
}

func TestMutableResumeRequiresConfiguredCheckpointAuthority(t *testing.T) {
	w := newWriter(t)
	docs := newDocServer(t)
	h := buildMutableGeneration(t, w, 96, 103)
	f := mutableFollower(t, w, docs, 16)
	docs.set(sign(t, w.key, revisionedUnsigned(w, h, 1, time.Unix(1, 0), server.UnfinalizedMutable)))
	f.poll()

	rotated, _, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	restarted := f.restart(t, w, func(c *follow.Config) {
		c.URL = docs.url
		c.PubKey = rotated
		configureMutableFollower(c, 16)
	})
	if err := restarted.Resume(t.Context()); err == nil || !strings.Contains(err.Error(), "differs from configured authority") {
		t.Fatalf("Resume after authority rotation error = %v, want stale-authority refusal", err)
	}
	if _, ok := f.heads.Get(testHead); ok {
		t.Fatal("Resume exposed mutable checkpoint signed by the prior configured authority")
	}
}

func TestMutableResumeWithoutProofAwareCheckpointFailsClosed(t *testing.T) {
	w := newWriter(t)
	docs := newDocServer(t)
	h := buildMutableGeneration(t, w, 96, 103)
	f := mutableFollower(t, w, docs, 16)
	docs.set(sign(t, w.key, revisionedUnsigned(w, h, 1, time.Unix(1, 0), server.UnfinalizedMutable)))
	f.poll()
	for _, name := range []string{testHandoffHead, testHead} {
		if err := follow.DowngradeCheckpointToProoflessV2(f.store.KV(), name); err != nil {
			t.Fatalf("downgrading %s checkpoint to v2: %v", name, err)
		}
	}

	restarted := f.restart(t, w, func(c *follow.Config) {
		c.URL = docs.url
		configureMutableFollower(c, 16)
	})
	err := restarted.Resume(t.Context())
	if err == nil || !strings.Contains(err.Error(), "lacks proof-aware publication metadata") {
		t.Fatalf("Resume from proofless checkpoint error = %v, want proof-aware fail-closed refusal", err)
	}
	if _, ok := f.heads.Get(testHead); ok {
		t.Fatal("Resume exposed mutable checkpoint without its authenticated handoff proof")
	}
}

func TestRevisionOrdersMutableDocumentsInsteadOfWriterClock(t *testing.T) {
	w := newWriter(t)
	docs := newDocServer(t)
	a := buildMutableGeneration(t, w, 96, 103)
	b := buildMutableGeneration(t, w, 104, 111)
	f := mutableFollower(t, w, docs, 16)

	docs.set(sign(t, w.key, revisionedUnsigned(w, a, 1, time.Unix(4_000_000_000, 0), server.UnfinalizedMutable)))
	f.poll()
	// An exact same-revision, same-digest repeat is idempotent, not replay or
	// equivocation.
	f.poll()

	// Revision 2 has an older diagnostic timestamp. It still wins.
	docs.set(sign(t, w.key, revisionedUnsigned(w, b, 2, time.Unix(2_000_000_000, 0), server.UnfinalizedMutable)))
	f.poll()
	if got := follow.HeadAdopted(f.f, testHead); got != b.Root() {
		t.Fatalf("after revision 2, adopted %s, want %s", got, b.Root())
	}

	// A lower revision with a newer clock is replay and cannot roll the root back.
	docs.set(sign(t, w.key, revisionedUnsigned(w, a, 1, time.Unix(5_000_000_000, 0), server.UnfinalizedMutable)))
	if err := f.pollErr(); err == nil || !strings.Contains(err.Error(), "publication revision 1") {
		t.Fatalf("lower-revision poll error = %v, want revision replay refusal", err)
	}
	if got := follow.HeadAdopted(f.f, testHead); got != b.Root() {
		t.Fatalf("replay changed adopted root to %s, want %s", got, b.Root())
	}

	// Reusing revision 2 for a different canonical claim is equivocation. The
	// mutable head is withdrawn rather than selecting either claim.
	docs.set(sign(t, w.key, revisionedUnsigned(w, a, 2, time.Unix(6_000_000_000, 0), server.UnfinalizedMutable)))
	if err := f.pollErr(); err == nil || !strings.Contains(err.Error(), "equivocated") {
		t.Fatalf("equivocation poll error = %v, want digest conflict", err)
	}
	if !follow.HeadQuarantined(f.f, testHead) {
		t.Fatal("same-revision digest conflict did not quarantine the mutable head")
	}
}

func TestMutableABAReturnIsANewFetchGeneration(t *testing.T) {
	w := newWriter(t)
	docs := newDocServer(t)
	a := buildMutableGeneration(t, w, 96, 103)
	b := buildMutableGeneration(t, w, 104, 111)
	f := mutableFollower(t, w, docs, 16)

	docs.set(sign(t, w.key, revisionedUnsigned(w, a, 1, time.Unix(1, 0), server.UnfinalizedMutable)))
	f.poll()
	docs.set(sign(t, w.key, revisionedUnsigned(w, b, 2, time.Unix(2, 0), server.UnfinalizedMutable)))
	f.poll()

	var observed atomic.Bool
	follow.SetBeforeSyncCommitHook(func() {
		observed.Store(true)
		if got := follow.HeadFetched(f.f, testHead); got.Defined() {
			t.Errorf("A -> B -> A retained stale fetch marker %s before the returning A walk", got)
		}
	})
	t.Cleanup(func() { follow.SetBeforeSyncCommitHook(nil) })
	docs.set(sign(t, w.key, revisionedUnsigned(w, a, 3, time.Unix(3, 0), server.UnfinalizedMutable)))
	f.poll()
	follow.SetBeforeSyncCommitHook(nil)
	if !observed.Load() {
		t.Fatal("returning A generation did not run a fetch pass")
	}
	if got := follow.HeadFetched(f.f, testHead); got != a.Root() {
		t.Fatalf("returning A fetch marker = %s, want %s", got, a.Root())
	}
	kind, _, revision, _, start, ok, err := follow.ReadRevisionedCheckpoint(f.store.KV(), testHead)
	if err != nil || !ok || kind != server.UnfinalizedMutable || revision != 3 || start != 96 {
		t.Fatalf("returning A checkpoint = kind=%q revision=%d start=%d ok=%t err=%v", kind, revision, start, ok, err)
	}
}

func TestMutableContractRejectedBeforeAdmission(t *testing.T) {
	t.Run("same-document handoff witness missing", func(t *testing.T) {
		w := newWriter(t)
		docs := newDocServer(t)
		h := buildMutableGeneration(t, w, 96, 103)
		f := mutableFollower(t, w, docs, 16)
		u := revisionedUnsigned(w, h, 1, time.Unix(1, 0), server.UnfinalizedMutable)
		u.Heads = u.Heads[:1]
		docs.set(sign(t, w.key, u))
		if err := f.pollErr(); err == nil || !strings.Contains(err.Error(), "handoff proof does not match") {
			t.Fatalf("missing handoff witness error = %v, want publication-contract refusal", err)
		}
		if _, _, ok, err := follow.ReadAuthorityFloor(f.store.KV(), w.pubkey()); err != nil || ok {
			t.Fatalf("witness-refused document advanced authority floor: ok=%t err=%v", ok, err)
		}
	})

	t.Run("signed kind omission", func(t *testing.T) {
		w := newWriter(t)
		docs := newDocServer(t)
		h := buildMutableGeneration(t, w, 96, 103)
		f := mutableFollower(t, w, docs, 16)
		docs.set(sign(t, w.key, revisionedUnsigned(w, h, 1, time.Unix(1, 0), server.FinalizedMonotonic)))
		if err := f.pollErr(); err == nil || !strings.Contains(err.Error(), "expects \"unfinalized-mutable\"") {
			t.Fatalf("kind omission error = %v", err)
		}
		if _, _, ok, err := follow.ReadAuthorityFloor(f.store.KV(), w.pubkey()); err != nil || ok {
			t.Fatalf("kind-refused document advanced authority floor: ok=%t err=%v", ok, err)
		}
	})

	t.Run("relay strips signed kind", func(t *testing.T) {
		w := newWriter(t)
		docs := newDocServer(t)
		h := buildMutableGeneration(t, w, 96, 103)
		f := mutableFollower(t, w, docs, 16)
		body := sign(t, w.key, revisionedUnsigned(w, h, 1, time.Unix(1, 0), server.UnfinalizedMutable))
		var raw map[string]any
		if err := json.Unmarshal(body, &raw); err != nil {
			t.Fatal(err)
		}
		heads := raw["heads"].([]any)
		delete(heads[0].(map[string]any), "kind")
		body, _ = json.Marshal(raw)
		docs.set(body)
		if err := f.pollErr(); err == nil || !strings.Contains(err.Error(), "does not verify") {
			t.Fatalf("stripped-kind error = %v", err)
		}
	})

	t.Run("window above local bound", func(t *testing.T) {
		w := newWriter(t)
		docs := newDocServer(t)
		h := buildMutableGeneration(t, w, 96, 104)
		f := mutableFollower(t, w, docs, 8)
		docs.set(sign(t, w.key, revisionedUnsigned(w, h, 1, time.Unix(1, 0), server.UnfinalizedMutable)))
		if err := f.pollErr(); err == nil || !strings.Contains(err.Error(), "8-slot maximum") {
			t.Fatalf("oversize-window error = %v", err)
		}
	})

	t.Run("root origin differs from signed window", func(t *testing.T) {
		w := newWriter(t)
		docs := newDocServer(t)
		h := buildMutableGeneration(t, w, 96, 103)
		f := mutableFollower(t, w, docs, 16)
		u := revisionedUnsigned(w, h, 1, time.Unix(1, 0), server.UnfinalizedMutable)
		wrong := uint64(97)
		u.Heads[0].OriginSlot, u.Heads[0].WindowStart = wrong, &wrong
		docs.set(sign(t, w.key, u))
		if err := f.pollErr(); err == nil || !strings.Contains(err.Error(), "root") || !strings.Contains(err.Error(), "origin_slot 96") {
			t.Fatalf("root/window mismatch error = %v", err)
		}
		if _, _, ok, err := follow.ReadAuthorityFloor(f.store.KV(), w.pubkey()); err != nil || ok {
			t.Fatalf("root-refused document advanced authority floor: ok=%t err=%v", ok, err)
		}
	})
}

func TestRevisionedAuthorityRefusesLegacyDowngrade(t *testing.T) {
	w := newWriter(t)
	docs := newDocServer(t)
	f := newFollower(t, w, func(c *follow.Config) { c.URL = docs.url })

	// Explicit finalized kind is a revisioned extension. The empty head changes
	// no checkpoint, which also proves the authority floor is committed for a
	// fully admitted document independently of head movement.
	docs.set(sign(t, w.key, revisionedUnsigned(w, w.head, 7, time.Unix(10, 0), server.FinalizedMonotonic)))
	f.poll()
	if revision, _, ok, err := follow.ReadAuthorityFloor(f.store.KV(), w.pubkey()); err != nil || !ok || revision != 7 {
		t.Fatalf("revisioned authority floor = %d ok=%t err=%v, want 7", revision, ok, err)
	}
	if _, ok, err := follow.ReadUpdatedAt(f.store.KV()); err != nil || ok {
		t.Fatalf("revisioned diagnostic clock contaminated legacy floor: ok=%t err=%v", ok, err)
	}

	docs.set(sign(t, w.key, w.unsigned(time.Unix(1_000, 0))))
	if err := f.pollErr(); err == nil || !strings.Contains(err.Error(), "revisioned-to-legacy downgrade") {
		t.Fatalf("legacy downgrade error = %v", err)
	}
}

func TestRevisionAndCheckpointBecomeDurableBeforeExposure(t *testing.T) {
	w := newWriter(t)
	docs := newDocServer(t)
	a := buildMutableGeneration(t, w, 96, 103)
	b := buildMutableGeneration(t, w, 104, 111)
	f := mutableFollower(t, w, docs, 16)
	docs.set(sign(t, w.key, revisionedUnsigned(w, a, 1, time.Unix(1, 0), server.UnfinalizedMutable)))
	f.poll()

	var observed atomic.Bool
	follow.SetBeforeExposeHook(func() {
		observed.Store(true)
		revision, _, floorOK, floorErr := follow.ReadAuthorityFloor(f.store.KV(), w.pubkey())
		_, _, checkpointRevision, _, _, cpOK, cpErr := follow.ReadRevisionedCheckpoint(f.store.KV(), testHead)
		if floorErr != nil || cpErr != nil || !floorOK || !cpOK || revision != 2 || checkpointRevision != 2 {
			t.Errorf("exposure boundary saw floor=(%d,%t,%v) checkpoint=(%d,%t,%v), want atomic revision 2",
				revision, floorOK, floorErr, checkpointRevision, cpOK, cpErr)
		}
	})
	t.Cleanup(func() { follow.SetBeforeExposeHook(nil) })
	docs.set(sign(t, w.key, revisionedUnsigned(w, b, 2, time.Unix(2, 0), server.UnfinalizedMutable)))
	f.poll()
	follow.SetBeforeExposeHook(nil)
	if !observed.Load() {
		t.Fatal("exposure boundary hook did not run")
	}

	// A higher revision that fails complete preflight must advance neither half.
	u := revisionedUnsigned(w, a, 3, time.Unix(3, 0), server.UnfinalizedMutable)
	wrong := uint64(97)
	u.Heads[0].OriginSlot, u.Heads[0].WindowStart = wrong, &wrong
	docs.set(sign(t, w.key, u))
	if err := f.pollErr(); err == nil {
		t.Fatal("invalid revision 3 was admitted")
	}
	revision, _, _, _ := follow.ReadAuthorityFloor(f.store.KV(), w.pubkey())
	_, _, checkpointRevision, _, _, _, _ := follow.ReadRevisionedCheckpoint(f.store.KV(), testHead)
	if revision != 2 || checkpointRevision != 2 {
		t.Fatalf("failed preflight left floor/checkpoint at %d/%d, want 2/2", revision, checkpointRevision)
	}
}

func TestHigherIPNSSequenceDoesNotOverridePublicationRevision(t *testing.T) {
	w := newIPNSWriter(t)
	a := buildMutableGeneration(t, w.writer, 96, 103)
	b := buildMutableGeneration(t, w.writer, 104, 111)
	f := ipnsFollower(t, w, func(c *follow.Config) {
		configureMutableFollower(c, 16)
	})

	w.publish(t, sign(t, w.key, revisionedUnsigned(w.writer, a, 2, time.Unix(2, 0), server.UnfinalizedMutable)))
	f.poll()
	_, higherSeq := w.publish(t, sign(t, w.key, revisionedUnsigned(w.writer, b, 1, time.Unix(3, 0), server.UnfinalizedMutable)))
	if err := f.pollErr(); err == nil || !strings.Contains(err.Error(), "publication revision 1") {
		t.Fatalf("higher-sequence/lower-revision error = %v", err)
	}
	if seq, ok, err := follow.ReadIPNSSeqFor(f.store.KV(), w.name()); err != nil || !ok || seq != higherSeq {
		t.Fatalf("IPNS floor = %d ok=%t err=%v, want independently raised to %d", seq, ok, err, higherSeq)
	}
	if got := follow.HeadAdopted(f.f, testHead); got != a.Root() {
		t.Fatalf("higher transport sequence changed publication root to %s, want %s", got, a.Root())
	}
}

func TestDualChannelEqualRevisionConflictIsDetectedBeforeSelection(t *testing.T) {
	w := newIPNSWriter(t)
	docs := newDocServer(t)
	a := buildMutableGeneration(t, w.writer, 96, 103)
	b := buildMutableGeneration(t, w.writer, 104, 111)
	f := newFollower(t, w.writer, func(c *follow.Config) {
		c.URL, c.IPNS, c.Routing = docs.url, w.name(), w.routing
		configureMutableFollower(c, 16)
	})
	docs.set(sign(t, w.key, revisionedUnsigned(w.writer, a, 5, time.Unix(100, 0), server.UnfinalizedMutable)))
	_, seq := w.publish(t, sign(t, w.key, revisionedUnsigned(w.writer, b, 5, time.Unix(200, 0), server.UnfinalizedMutable)))

	if err := f.pollErr(); err == nil || !strings.Contains(err.Error(), "equivocated") {
		t.Fatalf("dual-channel conflict error = %v", err)
	}
	if !follow.HeadQuarantined(f.f, testHead) {
		t.Fatal("dual-channel equal-revision conflict did not quarantine mutable head")
	}
	if _, _, ok, err := follow.ReadAuthorityFloor(f.store.KV(), w.pubkey()); err != nil || ok {
		t.Fatalf("conflicted candidates advanced authority floor: ok=%t err=%v", ok, err)
	}
	if got, ok, err := follow.ReadIPNSSeqFor(f.store.KV(), w.name()); err != nil || !ok || got != seq {
		t.Fatalf("conflict discarded independent IPNS observation: got=%d ok=%t err=%v want=%d", got, ok, err, seq)
	}
}
