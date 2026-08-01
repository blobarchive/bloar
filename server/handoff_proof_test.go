package server_test

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"sync"
	"testing"

	"github.com/ipfs/go-cid"

	"github.com/blobarchive/bloar/archive"
	"github.com/blobarchive/bloar/catalog"
	"github.com/blobarchive/bloar/schema"
	"github.com/blobarchive/bloar/server"
	"github.com/blobarchive/bloar/store"
)

type pausingResolver struct {
	archive.BlobResolver
	once    sync.Once
	entered chan struct{}
	release chan struct{}
}

type skewedGenerationStates struct{ server.GenerationStates }

func (s skewedGenerationStates) Get(ctx context.Context, name string) (server.GenerationState, bool, error) {
	state, ok, err := s.GenerationStates.Get(ctx, name)
	if ok && err == nil && name == mutableHead && state.Generation > 0 {
		state.WindowStart++
	}
	return state, ok, err
}

func (s skewedGenerationStates) EnsureKind(ctx context.Context, name string, kind server.HeadKind) (server.GenerationState, error) {
	state, err := s.GenerationStates.EnsureKind(ctx, name, kind)
	if err == nil && name == mutableHead && state.Generation > 0 {
		state.WindowStart++
	}
	return state, err
}

func (r *pausingResolver) ResolveBlob(ctx context.Context, vh schema.VersionedHash) (cid.Cid, bool, error) {
	r.once.Do(func() { close(r.entered) })
	select {
	case <-ctx.Done():
		return cid.Undef, false, ctx.Err()
	case <-r.release:
		return r.BlobResolver.ResolveBlob(ctx, vh)
	}
}

func publicationEntry(t *testing.T, heads *server.Heads, name string) server.HeadEntry {
	t.Helper()
	doc := decodeDoc(t, heads)
	for _, entry := range doc.Heads {
		if entry.Name == name {
			return entry
		}
	}
	t.Fatalf("publication has no head %q", name)
	return server.HeadEntry{}
}

func TestMutableProofTracksAppendLineageAndSourceWatermark(t *testing.T) {
	f := newGenerationFixture(t, "", nil, nil, nil)
	defer f.close()

	req := generationReq(0, 10, 13, nil)
	req.SourceFinalizedSlot = 12
	if _, err := f.heads.ReplaceGeneration(t.Context(), mutableHead, req); err != nil {
		t.Fatal(err)
	}
	state, err := f.heads.GenerationState(t.Context(), mutableHead)
	if err != nil {
		t.Fatal(err)
	}
	anchorRoot, anchorSlot := state.HandoffRoot, state.HandoffSyncedTo

	for _, frontier := range []uint64{11, 12} {
		if _, err := f.heads.ApplyRefs(t.Context(), testHead, nil, frontier, cid.Undef); err != nil {
			t.Fatalf("advance finalized handoff to %d: %v", frontier, err)
		}
		if _, ok := f.heads.Get(mutableHead); !ok {
			t.Fatalf("same-lineage advance to %d suppressed mutable generation", frontier)
		}
		finalized := publicationEntry(t, f.heads, testHead)
		mutable := publicationEntry(t, f.heads, mutableHead)
		if finalized.SyncedTo == nil || *finalized.SyncedTo != frontier || mutable.HandoffSyncedTo == nil ||
			*mutable.HandoffSyncedTo != frontier || mutable.HandoffRoot != finalized.Root {
			t.Fatalf("frontier %d publication pair is torn: finalized=%#v mutable=%#v", frontier, finalized, mutable)
		}
		persisted, err := f.heads.GenerationState(t.Context(), mutableHead)
		if err != nil {
			t.Fatal(err)
		}
		if persisted.HandoffRoot != anchorRoot || persisted.HandoffSyncedTo != anchorSlot {
			t.Fatalf("signed proof refresh rewrote durable commit anchor: got %s/%d want %s/%d",
				persisted.HandoffRoot, persisted.HandoffSyncedTo, anchorRoot, anchorSlot)
		}
	}

	if _, err := f.heads.ApplyRefs(t.Context(), testHead, nil, 13, cid.Undef); err != nil {
		t.Fatal(err)
	}
	if _, ok := f.heads.Get(mutableHead); ok {
		t.Fatal("handoff advance beyond source finalized watermark left mutable generation served")
	}
	if _, ok := f.heads.HeadDoc(mutableHead); ok {
		t.Fatal("invalid mutable generation remained in publication")
	}
	if _, err := f.heads.ReplaceGeneration(t.Context(), mutableHead, req); err == nil {
		t.Fatal("exact retry resurrected a generation invalidated above its source watermark")
	} else {
		var conflict *server.GenerationConflictError
		if !errors.As(err, &conflict) {
			t.Fatalf("invalidated retry error = %T %v, want generation conflict", err, err)
		}
	}

	httpd := generationHTTPServer(t, f)
	defer httpd.Close()
	resp, err := http.Get(httpd.URL + "/" + mutableHead + "/eth/v1/beacon/blobs/12")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable || resp.Header.Get("Cache-Control") != "no-store" ||
		resp.Header.Get("Retry-After") == "" {
		t.Fatalf("invalid physical mutable read = %d cache=%q retry=%q", resp.StatusCode,
			resp.Header.Get("Cache-Control"), resp.Header.Get("Retry-After"))
	}
}

func TestFinalizedTruncateInvalidatesMutableProofWithoutABARearm(t *testing.T) {
	f := newGenerationFixture(t, "", nil, nil, nil)
	defer f.close()
	req := generationReq(0, 10, 12, nil)
	req.SourceFinalizedSlot = 12
	if _, err := f.heads.ReplaceGeneration(t.Context(), mutableHead, req); err != nil {
		t.Fatal(err)
	}
	anchor := publicationEntry(t, f.heads, testHead)
	before := decodeDoc(t, f.heads)

	root, err := f.heads.Truncate(t.Context(), testHead, 10)
	if err != nil {
		t.Fatal(err)
	}
	afterNoOp := decodeDoc(t, f.heads)
	if root.String() != anchor.Root || afterNoOp.Revision == nil || before.Revision == nil ||
		*afterNoOp.Revision != *before.Revision {
		t.Fatalf("no-op truncate changed root/revision: root=%s anchor=%s before=%v after=%v",
			root, anchor.Root, before.Revision, afterNoOp.Revision)
	}
	if _, ok := f.heads.Get(mutableHead); !ok {
		t.Fatal("root-equal truncate rotated lineage and suppressed mutable generation")
	}

	if _, err := f.heads.Truncate(t.Context(), testHead, 9); err != nil {
		t.Fatal(err)
	}
	if _, ok := f.heads.Get(mutableHead); ok {
		t.Fatal("real finalized rewind left dependent mutable generation served")
	}
	if _, err := f.heads.ApplyRefs(t.Context(), testHead, nil, 10, cid.Undef); err != nil {
		t.Fatal(err)
	}
	restored := publicationEntry(t, f.heads, testHead)
	if restored.Root != anchor.Root || restored.SyncedTo == nil || *restored.SyncedTo != 10 {
		t.Fatalf("test did not recreate finalized root ABA: restored=%#v anchor=%#v", restored, anchor)
	}
	if _, ok := f.heads.Get(mutableHead); ok {
		t.Fatal("finalized root ABA reactivated a mutable proof from the retired lineage")
	}
	if _, err := f.heads.ReplaceGeneration(t.Context(), mutableHead, req); err == nil {
		t.Fatal("exact retry reactivated mutable proof after finalized root ABA")
	}
}

func TestStaleObservedHandoffIsZeroEffect(t *testing.T) {
	f := newGenerationFixture(t, "", nil, nil, nil)
	defer f.close()
	req := generationReq(0, 10, 12, nil)
	if _, err := f.heads.ApplyRefs(t.Context(), testHead, nil, 11, cid.Undef); err != nil {
		t.Fatal(err)
	}
	beforeDoc := bytes.Clone(f.heads.Doc())
	beforeRoot, ok, err := f.roots.Get(t.Context(), mutableHead)
	if err != nil || !ok {
		t.Fatalf("mutable baseline root: ok=%t err=%v", ok, err)
	}
	beforeState, err := f.heads.GenerationState(t.Context(), mutableHead)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.heads.ReplaceGeneration(t.Context(), mutableHead, req); err == nil {
		t.Fatal("stale observed finalized generation was accepted")
	}
	afterRoot, ok, err := f.roots.Get(t.Context(), mutableHead)
	if err != nil || !ok || !afterRoot.Equals(beforeRoot) {
		t.Fatalf("stale request changed selector root: before=%s after=%s ok=%t err=%v", beforeRoot, afterRoot, ok, err)
	}
	afterState, err := f.heads.GenerationState(t.Context(), mutableHead)
	if err != nil {
		t.Fatal(err)
	}
	if afterState != beforeState || !bytes.Equal(f.heads.Doc(), beforeDoc) || f.replaceCalls.Load() != 0 {
		t.Fatalf("stale request changed state/publication/callback: before=%#v after=%#v callbacks=%d",
			beforeState, afterState, f.replaceCalls.Load())
	}
}

func TestGenerationCommitRevalidatesHandoffAfterSlowBuild(t *testing.T) {
	type result struct {
		response server.GenerationResponse
		err      error
	}
	for _, tc := range []struct {
		name            string
		sourceFinalized uint64
		mutate          func(*testing.T, *server.Heads)
		wantSuccess     bool
	}{
		{"same-lineage-advance-within-watermark", 11, func(t *testing.T, heads *server.Heads) {
			if _, err := heads.ApplyRefs(t.Context(), testHead, nil, 11, cid.Undef); err != nil {
				t.Fatal(err)
			}
		}, true},
		{"same-lineage-advance-beyond-watermark", 10, func(t *testing.T, heads *server.Heads) {
			if _, err := heads.ApplyRefs(t.Context(), testHead, nil, 11, cid.Undef); err != nil {
				t.Fatal(err)
			}
		}, false},
		{"truncate-root-aba", 11, func(t *testing.T, heads *server.Heads) {
			before := publicationEntry(t, heads, testHead)
			if _, err := heads.Truncate(t.Context(), testHead, 9); err != nil {
				t.Fatal(err)
			}
			if _, err := heads.ApplyRefs(t.Context(), testHead, nil, 10, cid.Undef); err != nil {
				t.Fatal(err)
			}
			after := publicationEntry(t, heads, testHead)
			if after.Root != before.Root {
				t.Fatalf("test did not create finalized root ABA: before=%s after=%s", before.Root, after.Root)
			}
		}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newGenerationFixture(t, "", nil, nil, nil)
			defer f.close()
			vh := f.addBlob(88)
			pause := &pausingResolver{BlobResolver: f.cat, entered: make(chan struct{}), release: make(chan struct{})}
			f.archive.Resolver = pause
			f.heads = rebuildGenerationRegistry(t, f, f.roots.GenerationStore(), f.roots.PublicationStore())
			req := generationReq(0, 10, 12, []server.GenerationRow{{Slot: 11, VersionedHashes: []string{vh}}})
			req.SourceFinalizedSlot = tc.sourceFinalized
			done := make(chan result, 1)
			go func() {
				response, err := f.heads.ReplaceGeneration(t.Context(), mutableHead, req)
				done <- result{response: response, err: err}
			}()
			select {
			case <-pause.entered:
			case <-t.Context().Done():
				t.Fatal("generation build did not reach resolver pause")
			}
			tc.mutate(t, f.heads)
			close(pause.release)
			got := <-done
			if tc.wantSuccess {
				if got.err != nil || got.response.Generation != 1 {
					t.Fatalf("generation after allowed handoff advance = %#v, %v", got.response, got.err)
				}
				state, err := f.heads.GenerationState(t.Context(), mutableHead)
				if err != nil {
					t.Fatal(err)
				}
				if state.ObservedHandoffSyncedTo != 10 || state.HandoffSyncedTo != 11 || state.ObservedHandoffRoot == state.HandoffRoot {
					t.Fatalf("state did not retain observed and commit-time handoffs separately: %#v", state)
				}
				return
			}
			var conflict *server.GenerationConflictError
			if !errors.As(got.err, &conflict) {
				t.Fatalf("unsafe post-build handoff returned %T %v, want generation conflict", got.err, got.err)
			}
			state, err := f.heads.GenerationState(t.Context(), mutableHead)
			if err != nil || state.Generation != 0 {
				t.Fatalf("refused post-build handoff changed generation state: %#v err=%v", state, err)
			}
			if _, ok := f.heads.Get(mutableHead); ok {
				t.Fatal("refused post-build handoff exposed mutable generation")
			}
		})
	}
}

func openGenerationRegistry(t *testing.T, dir string, key ed25519.PrivateKey, mutableFirst bool) (*store.Store, *server.Heads) {
	t.Helper()
	st, err := store.Open(dir, store.WithPebbleLogger(quietPebble{}))
	if err != nil {
		t.Fatal(err)
	}
	roots := server.NewRootStore(st.KV())
	archiveCfg := archive.Config{Blocks: st.Blocks(), Resolver: catalog.New(st.KV())}
	heads, err := server.NewHeads(server.HeadsConfig{
		Net: testNet, Roots: roots, Generations: roots.GenerationStore(), Publications: roots.PublicationStore(),
		Policies: map[string]server.HeadPolicy{mutableHead: {
			Kind: server.UnfinalizedMutable, HandoffHead: testHead, MaxWindowSlots: 8,
		}},
		GenerationArchive: archiveCfg, SigningKey: key,
		Replacements: map[string]func(*archive.Head){testHead: func(*archive.Head) {}, mutableHead: func(*archive.Head) {}},
	})
	if err != nil {
		st.Close()
		t.Fatal(err)
	}
	finalized, err := server.OpenHead(t.Context(), archiveCfg, roots,
		archive.Params{Name: testHead, Net: testNet, OriginSlot: testOrigin, SegBits: testSegBits, FanoutBits: testFanout})
	if err != nil {
		st.Close()
		t.Fatal(err)
	}
	mutable, err := server.OpenMutableHead(t.Context(), archiveCfg, roots,
		archive.Params{Name: mutableHead, Net: testNet, OriginSlot: testOrigin, SegBits: testSegBits, FanoutBits: testFanout})
	if err != nil {
		st.Close()
		t.Fatal(err)
	}
	add := func(head *archive.Head) {
		if err := heads.Add(head); err != nil {
			st.Close()
			t.Fatal(err)
		}
	}
	if mutableFirst {
		add(mutable)
		add(finalized)
	} else {
		add(finalized)
		add(mutable)
	}
	return st, heads
}

func TestMutableRestartBindingIsExactAndOrderIndependent(t *testing.T) {
	for _, mutableFirst := range []bool{false, true} {
		label := "finalized-first"
		if mutableFirst {
			label = "mutable-first"
		}
		t.Run("exact/"+label, func(t *testing.T) {
			dir := t.TempDir()
			f := newGenerationFixture(t, dir, nil, nil, nil)
			key := append(ed25519.PrivateKey(nil), f.key...)
			if _, err := f.heads.ReplaceGeneration(t.Context(), mutableHead, generationReq(0, 10, 12, nil)); err != nil {
				t.Fatal(err)
			}
			f.close()
			st, heads := openGenerationRegistry(t, dir, key, mutableFirst)
			defer st.Close()
			if _, ok := heads.Get(mutableHead); !ok {
				t.Fatal("exact restart anchor did not bind mutable proof")
			}
		})

		t.Run("mismatch-and-aba/"+label, func(t *testing.T) {
			dir := t.TempDir()
			f := newGenerationFixture(t, dir, nil, nil, nil)
			key := append(ed25519.PrivateKey(nil), f.key...)
			req := generationReq(0, 10, 12, nil)
			req.SourceFinalizedSlot = 11
			if _, err := f.heads.ReplaceGeneration(t.Context(), mutableHead, req); err != nil {
				t.Fatal(err)
			}
			state, err := f.heads.GenerationState(t.Context(), mutableHead)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := f.heads.ApplyRefs(t.Context(), testHead, nil, 11, cid.Undef); err != nil {
				t.Fatal(err)
			}
			f.close()

			st, heads := openGenerationRegistry(t, dir, key, mutableFirst)
			defer st.Close()
			if _, ok := heads.Get(mutableHead); ok {
				t.Fatal("restart rebound mutable proof to a non-exact handoff generation")
			}
			if _, err := heads.Truncate(t.Context(), testHead, 10); err != nil {
				t.Fatal(err)
			}
			restored := publicationEntry(t, heads, testHead)
			if restored.Root != state.HandoffRoot || restored.SyncedTo == nil || *restored.SyncedTo != state.HandoffSyncedTo {
				t.Fatalf("test did not recreate exact restart anchor: restored=%#v state=%#v", restored, state)
			}
			if _, ok := heads.Get(mutableHead); ok {
				t.Fatal("restart mismatch followed by root ABA rearmed mutable proof")
			}
		})
	}
}

func TestFollowedFinalizedReplacementRotatesLineage(t *testing.T) {
	f := newGenerationFixture(t, "", nil, nil, nil)
	defer f.close()
	finalizedA, ok := f.heads.Get(testHead)
	if !ok {
		t.Fatal("fixture finalized head missing")
	}
	mutable, err := archive.BuildGeneration(t.Context(), f.archive, archive.Params{
		Name: mutableHead, Net: testNet, OriginSlot: 10, SegBits: testSegBits, FanoutBits: testFanout,
	}, nil, 12)
	if err != nil {
		t.Fatal(err)
	}

	roots := server.NewRootStore(f.st.KV())
	followed, err := server.NewHeads(server.HeadsConfig{Net: testNet, Roots: roots})
	if err != nil {
		t.Fatal(err)
	}
	if err := followed.AdoptKind(t.Context(), finalizedA, nil, cid.Undef, server.FinalizedMonotonic); err != nil {
		t.Fatal(err)
	}
	if err := followed.AdoptPublished(t.Context(), mutable, nil, cid.Undef,
		mutablePublicationEntry(t, finalizedA, mutable, 10)); err != nil {
		t.Fatal(err)
	}
	if _, ok := followed.Get(mutableHead); !ok {
		t.Fatal("coherent followed mutable pair was not served")
	}

	vh := f.addBlob(77)
	finalizedB, err := archive.BuildGeneration(t.Context(), f.archive, archive.Params{
		Name: testHead, Net: testNet, OriginSlot: testOrigin, SegBits: testSegBits, FanoutBits: testFanout,
	}, []archive.RefRow{{Slot: 10, VHs: []schema.VersionedHash{testVersionedHash(t, vh)}}}, 10)
	if err != nil {
		t.Fatal(err)
	}
	if finalizedB.Root().Equals(finalizedA.Root()) {
		t.Fatal("test follower replacement did not change finalized root")
	}
	if err := followed.AdoptKind(t.Context(), finalizedB, nil, cid.Undef, server.FinalizedMonotonic); err != nil {
		t.Fatal(err)
	}
	if _, ok := followed.Get(mutableHead); ok {
		t.Fatal("distinct followed finalized root inherited prior lineage and left mutable proof valid")
	}
}

func TestAdoptPublishedOwnsProofPointerValues(t *testing.T) {
	f := newGenerationFixture(t, "", nil, nil, nil)
	defer f.close()
	finalized, ok := f.heads.Get(testHead)
	if !ok {
		t.Fatal("fixture finalized head missing")
	}
	mutable, err := archive.BuildGeneration(t.Context(), f.archive, archive.Params{
		Name: mutableHead, Net: testNet, OriginSlot: 10, SegBits: testSegBits, FanoutBits: testFanout,
	}, nil, 12)
	if err != nil {
		t.Fatal(err)
	}
	heads, err := server.NewHeads(server.HeadsConfig{Net: testNet, Roots: server.NewRootStore(f.st.KV())})
	if err != nil {
		t.Fatal(err)
	}
	if err := heads.AdoptKind(t.Context(), finalized, nil, cid.Undef, server.FinalizedMonotonic); err != nil {
		t.Fatal(err)
	}
	entry := mutablePublicationEntry(t, finalized, mutable, 10)
	if err := heads.AdoptPublished(t.Context(), mutable, nil, cid.Undef, entry); err != nil {
		t.Fatal(err)
	}
	*entry.SyncedTo = 99
	*entry.WindowStart = 99
	*entry.SourceFinalizedSlot = 99
	*entry.HandoffSyncedTo = 99
	// Force a fresh registry coherence pass. An exact finalized replay preserves
	// lineage, so only an illicit alias to the caller's proof pointers could
	// suppress or corrupt the mutable entry.
	if err := heads.AdoptKind(t.Context(), finalized, nil, cid.Undef, server.FinalizedMonotonic); err != nil {
		t.Fatal(err)
	}
	selected, ok := heads.Get(mutableHead)
	if !ok || selected.Params().OriginSlot != 10 {
		t.Fatalf("caller mutation of adopted proof changed registry: selected=%v ok=%t", selected, ok)
	}
}

func TestPublicationProofArithmeticSaturatesAtMaxUint64(t *testing.T) {
	max := uint64(math.MaxUint64)
	revision := uint64(1)
	handoffRoot := "bafyreiadtfhdbbzr2jcw33xkx4xsvhurwfrjy2inxi2ozogubkxmio376i"
	doc := server.Doc{Unsigned: server.Unsigned{
		V: server.DocVersion, Net: testNet, UpdatedAt: "2026-07-22T00:00:00Z", Revision: &revision,
		Heads: []server.HeadEntry{
			{Name: testHead, Root: handoffRoot, OriginSlot: 0, SyncedTo: &max, SegBits: 3, FanoutBits: 2},
			{Name: mutableHead, Root: handoffRoot, OriginSlot: max, SyncedTo: &max, SegBits: 3, FanoutBits: 2,
				Kind: server.UnfinalizedMutable, WindowStart: &max,
				SourceHeadRoot: "0x" + string(bytes.Repeat([]byte{'1'}, 64)), SourceFinalizedSlot: &max,
				SourceFinalizedRoot: "0x" + string(bytes.Repeat([]byte{'2'}, 64)),
				HandoffHead:         testHead, HandoffRoot: handoffRoot, HandoffSyncedTo: &max},
		},
	}, Pubkey: "present", Signature: "present"}
	if err := doc.ValidateContract(); err != nil {
		t.Fatalf("max-slot coherent proof was rejected through overflowing F+1 arithmetic: %v", err)
	}
}

func TestGenerationStateV2RequiresNumericAnchorPresence(t *testing.T) {
	st, err := store.Open(t.TempDir(), store.WithPebbleLogger(quietPebble{}))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	for _, tc := range []struct {
		name string
		raw  string
	}{
		{"stripped-v2", `{"v":2,"kind":"unfinalized-mutable","generation":1}`},
		{"null-v2", `{"v":2,"kind":"unfinalized-mutable","generation":1,"request_digest":"","root":"","window_start":0,"synced_to":0,"source_head_root":"","source_head_slot":0,"source_finalized_slot":0,"source_finalized_root":"","observed_handoff_root":"","observed_handoff_synced_to":null,"handoff_head":"","handoff_root":"","handoff_synced_to":0}`},
	} {
		key := append([]byte{'g'}, tc.name...)
		if err := st.KV().Set(key, []byte(tc.raw), nil); err != nil {
			t.Fatal(err)
		}
		if _, _, err := server.NewGenerationStore(st.KV()).Get(t.Context(), tc.name); err == nil {
			t.Fatalf("v2 generation state %q with stripped/null numeric anchor was accepted as slot zero", tc.name)
		}
	}
}

func TestAdoptPublishedMalformedProofHasNoEffects(t *testing.T) {
	f := newGenerationFixture(t, "", nil, nil, nil)
	defer f.close()
	mutable, err := archive.BuildGeneration(t.Context(), f.archive, archive.Params{
		Name: "direct-mutable", Net: testNet, OriginSlot: 10, SegBits: testSegBits, FanoutBits: testFanout,
	}, nil, 12)
	if err != nil {
		t.Fatal(err)
	}
	roots := server.NewRootStore(f.st.KV())
	heads, err := server.NewHeads(server.HeadsConfig{Net: testNet, Roots: roots})
	if err != nil {
		t.Fatal(err)
	}
	info := mutable.Info()
	start, synced := info.OriginSlot, *info.SyncedTo
	malformed := server.HeadEntry{Name: info.Name, Root: info.Root.String(), OriginSlot: info.OriginSlot,
		SyncedTo: &synced, SegBits: info.SegBits, FanoutBits: info.FanoutBits, DirDepth: info.DirDepth,
		Kind: server.UnfinalizedMutable, WindowStart: &start}
	before := bytes.Clone(heads.Doc())
	if err := heads.AdoptPublished(t.Context(), mutable, nil, cid.Undef, malformed); err == nil {
		t.Fatal("direct malformed mutable proof was adopted")
	}
	if _, ok, err := roots.Get(t.Context(), info.Name); err != nil || ok {
		t.Fatalf("malformed adoption wrote root mirror: ok=%t err=%v", ok, err)
	}
	if !bytes.Equal(before, heads.Doc()) || len(heads.Names()) != 0 {
		t.Fatalf("malformed adoption changed registry/publication: names=%v", heads.Names())
	}
}

func TestAddPreRendersBeforeRegistryVisibility(t *testing.T) {
	f := newGenerationFixture(t, "", nil, nil, nil)
	defer f.close()
	if _, err := f.heads.ReplaceGeneration(t.Context(), mutableHead, generationReq(0, 10, 12, nil)); err != nil {
		t.Fatal(err)
	}
	revisions := &failOnceRevisions{PublicationRevisions: f.roots.PublicationStore()}
	heads, err := server.NewHeads(server.HeadsConfig{
		Net: testNet, Roots: f.roots, Generations: f.roots.GenerationStore(), Publications: revisions,
		Policies: map[string]server.HeadPolicy{mutableHead: {
			Kind: server.UnfinalizedMutable, HandoffHead: testHead, MaxWindowSlots: 8,
		}},
		GenerationArchive: f.archive, SigningKey: f.key,
		Replacements: map[string]func(*archive.Head){testHead: func(*archive.Head) {}, mutableHead: func(*archive.Head) {}},
	})
	if err != nil {
		t.Fatal(err)
	}
	finalized, ok := f.heads.Get(testHead)
	if !ok {
		t.Fatal("fixture finalized head missing")
	}
	revisions.fail.Store(true)
	if err := heads.Add(finalized); err == nil {
		t.Fatal("injected publication allocation failure was accepted")
	}
	if len(heads.Names()) != 0 {
		t.Fatalf("failed Add changed registry visibility: %v", heads.Names())
	}
	if err := heads.Add(finalized); err != nil {
		t.Fatalf("failed Add was not exactly retryable: %v", err)
	}
	if names := heads.Names(); len(names) != 1 || names[0] != testHead {
		t.Fatalf("successful Add names = %v", names)
	}
}

func TestAddRejectsGenerationStateThatDoesNotDescribeRoot(t *testing.T) {
	f := newGenerationFixture(t, "", nil, nil, nil)
	defer f.close()
	if _, err := f.heads.ReplaceGeneration(t.Context(), mutableHead, generationReq(0, 10, 12, nil)); err != nil {
		t.Fatal(err)
	}
	states := skewedGenerationStates{GenerationStates: f.roots.GenerationStore()}
	heads, err := server.NewHeads(server.HeadsConfig{
		Net: testNet, Roots: f.roots, Generations: states, Publications: f.roots.PublicationStore(),
		Policies: map[string]server.HeadPolicy{mutableHead: {
			Kind: server.UnfinalizedMutable, HandoffHead: testHead, MaxWindowSlots: 8,
		}},
		GenerationArchive: f.archive, SigningKey: f.key,
		Replacements: map[string]func(*archive.Head){testHead: func(*archive.Head) {}, mutableHead: func(*archive.Head) {}},
	})
	if err != nil {
		t.Fatal(err)
	}
	finalized, _ := f.heads.Get(testHead)
	mutable, _ := f.heads.Get(mutableHead)
	if err := heads.Add(finalized); err != nil {
		t.Fatal(err)
	}
	if err := heads.Add(mutable); err == nil {
		t.Fatal("mutable Add accepted state window which did not describe the loaded root")
	}
	if _, ok := heads.Get(mutableHead); ok {
		t.Fatal("state/root mismatch became served")
	}
}

func TestQuarantineRetriesFailedPublicationWithdrawal(t *testing.T) {
	f := newGenerationFixture(t, "", nil, nil, nil)
	defer f.close()
	if _, err := f.heads.ReplaceGeneration(t.Context(), mutableHead, generationReq(0, 10, 12, nil)); err != nil {
		t.Fatal(err)
	}
	revisions := &failOnceRevisions{PublicationRevisions: f.roots.PublicationStore()}
	heads := rebuildGenerationRegistry(t, f, f.roots.GenerationStore(), revisions)
	if _, ok := heads.Get(testHead); !ok {
		t.Fatal("rebuilt finalized head missing")
	}
	revisions.fail.Store(true)
	if err := heads.Quarantine(testHead, "test invalid finalized authority"); err == nil {
		t.Fatal("injected quarantine publication failure was not reported")
	}
	if _, ok := heads.Get(testHead); ok {
		t.Fatal("publication failure kept quarantined head locally served")
	}
	if _, ok := heads.Get(mutableHead); ok {
		t.Fatal("publication failure kept dependent mutable head locally served")
	}
	if _, ok := heads.HeadDoc(testHead); !ok {
		t.Fatal("test did not retain the stale prior publication after injected failure")
	}
	if err := heads.Quarantine(testHead, "ignored on retry"); err != nil {
		t.Fatalf("repeated quarantine did not retry publication withdrawal: %v", err)
	}
	if _, ok := heads.HeadDoc(testHead); ok {
		t.Fatal("quarantine retry left finalized head in publication")
	}
	if _, ok := heads.HeadDoc(mutableHead); ok {
		t.Fatal("quarantine retry left dependent mutable head in publication")
	}
}

func TestHeadsConfigClonesReplacementCallbacks(t *testing.T) {
	f := newGenerationFixture(t, "", nil, nil, nil)
	defer f.close()
	if _, err := f.heads.ReplaceGeneration(t.Context(), mutableHead, generationReq(0, 10, 12, nil)); err != nil {
		t.Fatal(err)
	}
	var calls int
	replacements := map[string]func(*archive.Head){
		testHead:    func(*archive.Head) {},
		mutableHead: func(*archive.Head) { calls++ },
	}
	heads, err := server.NewHeads(server.HeadsConfig{
		Net: testNet, Roots: f.roots, Generations: f.roots.GenerationStore(), Publications: f.roots.PublicationStore(),
		Policies: map[string]server.HeadPolicy{mutableHead: {
			Kind: server.UnfinalizedMutable, HandoffHead: testHead, MaxWindowSlots: 8,
		}},
		GenerationArchive: f.archive, SigningKey: f.key, Replacements: replacements,
	})
	if err != nil {
		t.Fatal(err)
	}
	finalized, _ := f.heads.Get(testHead)
	mutable, _ := f.heads.Get(mutableHead)
	if err := heads.Add(finalized); err != nil {
		t.Fatal(err)
	}
	if err := heads.Add(mutable); err != nil {
		t.Fatal(err)
	}
	replacements[mutableHead] = nil
	req := generationReq(1, 10, 12, nil)
	req.SourceHeadRoot = "0x" + string(bytes.Repeat([]byte{'3'}, 64))
	if _, err := heads.ReplaceGeneration(t.Context(), mutableHead, req); err != nil {
		t.Fatalf("caller mutation of Replacements changed runtime callback map: %v", err)
	}
	if calls != 1 {
		t.Fatalf("cloned replacement callback calls = %d, want 1", calls)
	}
}

func TestMutablePublicationJSONRetainsZeroProofFields(t *testing.T) {
	zero := uint64(0)
	entry := server.HeadEntry{SourceFinalizedSlot: &zero, HandoffSyncedTo: &zero}
	raw, err := json.Marshal(entry)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(raw, []byte(`"source_finalized_slot":0`)) || !bytes.Contains(raw, []byte(`"handoff_synced_to":0`)) {
		t.Fatalf("zero-valued proof fields were omitted: %s", raw)
	}
}
