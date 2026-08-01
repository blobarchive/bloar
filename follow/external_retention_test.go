package follow_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ipfs/boxo/blockstore"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"

	"github.com/blobarchive/bloar/archive"
	"github.com/blobarchive/bloar/follow"
	"github.com/blobarchive/bloar/p2p"
	"github.com/blobarchive/bloar/pinning"
	"github.com/blobarchive/bloar/replica"
	"github.com/blobarchive/bloar/server"
)

type recordingRetention struct {
	roots      *server.RootStore
	current    *replica.Generation
	pending    *replica.Generation
	events     []string
	protectErr error
}

func (r *recordingRetention) Prepare(ctx context.Context, generation replica.Generation) error {
	for _, head := range generation.Heads {
		if durable, ok, err := r.roots.Get(ctx, head.Name); err != nil {
			return err
		} else if ok && durable.Equals(head.Root) {
			return fmt.Errorf("checkpoint/root mirror advanced before external Prepare for %s", head.Root)
		}
	}
	copy := generation
	copy.Heads = append([]replica.Head(nil), generation.Heads...)
	r.pending = &copy
	r.events = append(r.events, "prepare:"+generation.Heads[0].Root.String())
	return nil
}

func (r *recordingRetention) Commit(ctx context.Context, generation replica.Generation) error {
	if r.pending == nil || !r.pending.UpdatedAt.Equal(generation.UpdatedAt) || !slices.Equal(r.pending.Heads, generation.Heads) {
		return errors.New("committed generation was not pending")
	}
	for _, head := range generation.Heads {
		durable, ok, err := r.roots.Get(ctx, head.Name)
		if err != nil {
			return err
		}
		if !ok || !durable.Equals(head.Root) {
			return fmt.Errorf("external Commit ran before checkpoint/root mirror for %s", head.Root)
		}
	}
	copy := *r.pending
	r.current, r.pending = &copy, nil
	r.events = append(r.events, "commit:"+generation.Heads[0].Root.String())
	return nil
}

func (r *recordingRetention) ProtectsAll(_ context.Context, heads []replica.Head) error {
	r.events = append(r.events, fmt.Sprintf("protect:%d", len(heads)))
	if r.protectErr != nil {
		return r.protectErr
	}
	for _, generation := range []*replica.Generation{r.current, r.pending} {
		if generation == nil {
			continue
		}
		if len(generation.Heads) != len(heads) {
			continue
		}
		matched := true
		for _, head := range heads {
			if !slices.ContainsFunc(generation.Heads, func(retained replica.Head) bool {
				return retained.Name == head.Name && retained.Root.Equals(head.Root) && retained.Manifest.Equals(head.Manifest)
			}) {
				matched = false
				break
			}
		}
		if matched {
			return nil
		}
	}
	return replica.ErrGenerationUnprotected
}

func TestExternalRetentionPinsBeforeCheckpointAndProtectsResume(t *testing.T) {
	w := newWriter(t)
	w.ingestSlot(testOrigin, 1)
	retention := &recordingRetention{}
	f := newFollower(t, w, externalRetentionOption(t, retention))
	retention.roots = f.roots

	f.poll()
	if len(retention.events) != 2 || retention.events[0][:8] != "prepare:" || retention.events[1][:7] != "commit:" {
		t.Fatalf("retention events = %#v", retention.events)
	}
	first := retention.current.Heads[0]
	if first.Name != testHead || first.SyncedTo != testOrigin {
		t.Fatalf("retained head = %+v", first)
	}

	w.ingestSlot(testOrigin+1, 2)
	f.poll()
	if got := retention.current.Heads[0].SyncedTo; got != testOrigin+1 {
		t.Fatalf("retained synced_to = %d", got)
	}

	restarted := f.restart(t, w, externalRetentionOption(t, retention))
	if err := restarted.Resume(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, ok := f.heads.Get(testHead); !ok {
		t.Fatal("externally protected checkpoint was not resumed")
	}
	if event := retention.events[len(retention.events)-1]; event != "protect:1" {
		t.Fatalf("last event = %q, want protect", event)
	}
}

func TestExternalRetentionResumeFailsClosedWithoutPinProof(t *testing.T) {
	w := newWriter(t)
	w.ingestSlot(testOrigin, 1)
	retention := &recordingRetention{}
	f := newFollower(t, w, externalRetentionOption(t, retention))
	retention.roots = f.roots
	f.poll()

	retention.protectErr = replica.ErrGenerationUnprotected
	restarted := f.restart(t, w, externalRetentionOption(t, retention))
	if err := restarted.Resume(t.Context()); !errors.Is(err, replica.ErrGenerationUnprotected) {
		t.Fatalf("Resume error = %v", err)
	}
	if _, ok := f.heads.Get(testHead); ok {
		t.Fatal("unprotected checkpoint was exposed")
	}
}

func TestExternalRetentionResumeRepairsFloorWithoutChangingProtectionRoot(t *testing.T) {
	w := newWriter(t)
	w.ingestSlot(testOrigin, 1)
	retention := &recordingRetention{}
	f := newFollower(t, w, externalRetentionOption(t, retention))
	retention.roots = f.roots
	f.poll()

	root, syncedTo, manifest, updatedAt, ok, err := follow.ReadCheckpoint(f.store.KV(), testHead)
	if err != nil || !ok {
		t.Fatalf("reading checkpoint: ok=%t err=%v", ok, err)
	}
	if syncedTo == 0 {
		t.Fatal("test needs a positive coverage floor")
	}
	// Simulate the safe crash/corruption direction: the root still encodes the
	// full coverage retained by Kubo, but its anti-replay floor is lower. Resume
	// must prove the unchanged root set, repair the floor upward, and remain
	// restartable without manufacturing another external pin generation.
	if err := follow.WriteCheckpoint(f.store.KV(), testHead, root, syncedTo-1, manifest, updatedAt); err != nil {
		t.Fatal(err)
	}
	restarted := f.restart(t, w, externalRetentionOption(t, retention))
	if err := restarted.Resume(t.Context()); err != nil {
		t.Fatalf("resume with upward floor repair: %v", err)
	}
	_, repaired, _, _, ok, err := follow.ReadCheckpoint(f.store.KV(), testHead)
	if err != nil || !ok || repaired != syncedTo {
		t.Fatalf("repaired checkpoint: synced_to=%d ok=%t err=%v, want %d", repaired, ok, err, syncedTo)
	}

	// The repaired floor is metadata; the original retained root and manifest
	// still prove the complete block closure on another restart.
	if err := restarted.Resume(t.Context()); err != nil {
		t.Fatalf("second resume after floor repair: %v", err)
	}
}

func TestExternalRetentionKeepsFilteredLivePairAtomicAndGlobalWitnessMetadataOnly(t *testing.T) {
	const (
		filteredName = "arbitrum-one"
		mutableName  = "unfinalized"
		witnessName  = "all"
	)
	w := newWriter(t)
	docs := newDocServer(t)
	retention := &recordingRetention{}
	configure := func(c *follow.Config) {
		c.URL = docs.url
		c.Heads = map[string]pinning.Policy{
			filteredName: pinning.Full(),
			mutableName:  pinning.Full(),
		}
		c.ExpectedKinds = map[string]server.HeadKind{mutableName: server.UnfinalizedMutable}
		c.ExpectedHandoffs = map[string]string{mutableName: witnessName}
		c.OverlayFinalizedHeads = map[string]string{mutableName: filteredName}
		c.MaxMutableWindowSlots = map[string]uint64{mutableName: 32}
		externalRetentionOption(t, retention)(c)
	}
	f := newFollower(t, w, configure)
	retention.roots = f.roots

	filteredA := buildDocumentHead(t, w, filteredName, 96, 103, testSegBits, testFanout)
	mutableA := buildDocumentHead(t, w, mutableName, 104, 111, testSegBits, testFanout)
	witnessA := buildDocumentHead(t, w, witnessName, 96, 103, testSegBits, testFanout)
	docs.set(sign(t, w.key, filteredOverlayDocument(t, w, filteredA, mutableA, witnessA, 1)))
	f.poll()

	requireExternalPair := func(generation *replica.Generation, filtered, mutable *archive.Head) {
		t.Helper()
		if generation == nil || len(generation.Heads) != 2 {
			t.Fatalf("retained generation = %#v, want exactly two selected heads", generation)
		}
		want := map[string]cid.Cid{filteredName: filtered.Root(), mutableName: mutable.Root()}
		for _, head := range generation.Heads {
			root, selected := want[head.Name]
			if !selected || !head.Root.Equals(root) {
				t.Fatalf("retained head = %+v, want selected pair %#v", head, want)
			}
			delete(want, head.Name)
		}
		if len(want) != 0 {
			t.Fatalf("retained generation omitted selected heads %#v", want)
		}
	}
	requireExternalPair(retention.current, filteredA, mutableA)
	requireNoOverlayWitnessState(t, f.f, f, witnessA)

	// A globally coherent document whose filtered frontier leaves a local gap
	// must be refused before external Prepare. In particular it cannot create a
	// one-sided pending Kubo generation or fetch any of the candidate roots.
	filteredGap := buildDocumentHead(t, w, filteredName, 96, 104, testSegBits, testFanout)
	mutableGap := buildDocumentHead(t, w, mutableName, 112, 119, testSegBits, testFanout)
	witnessGap := buildDocumentHead(t, w, witnessName, 96, 111, testSegBits, testFanout)
	docs.set(sign(t, w.key, filteredOverlayDocument(t, w, filteredGap, mutableGap, witnessGap, 2)))
	eventsBefore := slices.Clone(retention.events)
	if err := f.pollErr(); err == nil || !strings.Contains(err.Error(), "window_start 112") {
		t.Fatalf("filtered handoff-gap Poll error = %v", err)
	}
	if !slices.Equal(eventsBefore, retention.events) || retention.pending != nil {
		t.Fatalf("refused document mutated external retention: before=%v after=%v pending=%#v",
			eventsBefore, retention.events, retention.pending)
	}
	requireExternalPair(retention.current, filteredA, mutableA)
	for _, candidate := range []cid.Cid{filteredGap.Root(), mutableGap.Root(), witnessGap.Root()} {
		if f.hasLocally(candidate) {
			t.Fatalf("refused gap document fetched candidate root %s", candidate)
		}
	}

	// One valid publication rotates both selected roots through one Prepare and
	// one Commit. The global witness changes too, but remains outside retention.
	filteredB := buildDocumentHead(t, w, filteredName, 96, 111, testSegBits, testFanout)
	mutableB := buildDocumentHead(t, w, mutableName, 112, 119, testSegBits, testFanout)
	witnessB := buildDocumentHead(t, w, witnessName, 96, 111, testSegBits, testFanout)
	docs.set(sign(t, w.key, filteredOverlayDocument(t, w, filteredB, mutableB, witnessB, 3)))
	eventsBefore = slices.Clone(retention.events)
	f.poll()
	if got := retention.events[len(eventsBefore):]; len(got) != 2 || !strings.HasPrefix(got[0], "prepare:") || !strings.HasPrefix(got[1], "commit:") {
		t.Fatalf("pair rotation events = %v, want one prepare then one commit", got)
	}
	requireExternalPair(retention.current, filteredB, mutableB)
	requireNoOverlayWitnessState(t, f.f, f, witnessB)

	// Resume reconstructs the signed proof and metadata-only witness, rechecks
	// the overlay boundary, and requires one external anchor for the whole pair.
	restarted := f.restart(t, w, configure)
	if err := restarted.Resume(t.Context()); err != nil {
		t.Fatalf("Resume(filtered external pair): %v", err)
	}
	if event := retention.events[len(retention.events)-1]; event != "protect:2" {
		t.Fatalf("last retention event = %q, want protect:2", event)
	}
	requireSelectedRoot(t, f.heads, filteredName, filteredB.Root())
	requireSelectedRoot(t, f.heads, mutableName, mutableB.Root())
	requireCheckpointRevision(t, f, filteredName, 3)
	requireCheckpointRevision(t, f, mutableName, 3)
	requireNoOverlayWitnessState(t, restarted, f, witnessB)

	// A selected-set config change cannot compose old v3 checkpoints with a new
	// name. The whole newly configured set stays unexposed until a fresh signed
	// publication prepares one exact replacement generation; the old external
	// pair remains current and protected in the meantime.
	const addedName = "optimism"
	expandedReady := newReadyRecorder()
	expandedConfigure := func(c *follow.Config) {
		configure(c)
		c.Heads[addedName] = pinning.Full()
		c.ExpectedKinds[addedName] = server.FinalizedMonotonic
		c.Ready = expandedReady.hook()
	}
	eventsBefore = slices.Clone(retention.events)
	expanded := f.restart(t, w, expandedConfigure)
	if err := expanded.Resume(t.Context()); err == nil || !strings.Contains(err.Error(), "2 of 3 configured head records") {
		t.Fatalf("Resume after selected-set expansion = %v, want exact-set refusal", err)
	}
	if !slices.Equal(eventsBefore, retention.events) {
		t.Fatalf("selected-set refusal changed retention: before=%v after=%v", eventsBefore, retention.events)
	}
	requireExternalPair(retention.current, filteredB, mutableB)
	for _, name := range []string{filteredName, mutableName, addedName} {
		if _, exposed := f.heads.Get(name); exposed || expandedReady.isReady(name) {
			t.Fatalf("head %q exposed=%t ready=%t after exact-set resume refusal", name, exposed, expandedReady.isReady(name))
		}
	}
}

func TestExternalRetentionConfigurationIsNarrow(t *testing.T) {
	w := newWriter(t)
	f := newFollower(t, w)
	base := follow.Config{
		Net: testNet, URL: w.url, PubKey: w.pubkey(),
		Heads: map[string]pinning.Policy{testHead: pinning.Full()},
		Local: f.store.Blocks(), Registry: f.heads, Roots: f.roots, KV: f.store.KV(),
	}
	retention := &recordingRetention{roots: f.roots}
	if _, err := follow.New(base); err == nil {
		t.Fatal("missing embedded sessions accepted")
	}
	base.Retention = retention
	if _, err := follow.New(base); err == nil {
		t.Fatal("external retention without Fetch accepted")
	}
	base.Fetch = f.store.Blocks()
	if _, err := follow.New(base); err == nil || !strings.Contains(err.Error(), "Config.Gate") {
		t.Fatalf("external retention without a shared reader gate error = %v", err)
	}
	base.Gate = pinning.NewGate()
	if _, err := follow.New(base); err == nil || !strings.Contains(err.Error(), "do not share one gate") {
		t.Fatalf("external retention with a gate different from Registry error = %v", err)
	}
	base.Gate = f.rec.Gate()
	base.Heads[testHead] = pinning.None()
	if _, err := follow.New(base); err == nil {
		t.Fatal("non-full external archive policy accepted")
	}
}

func externalRetentionOption(t *testing.T, retention follow.Retention) func(*follow.Config) {
	t.Helper()
	return func(cfg *follow.Config) {
		cfg.Fetch = p2p.FetchingBlockstore(t.Context(), cfg.Local, cfg.Sessions)
		cfg.Sessions = nil
		cfg.Retention = retention
		cfg.Gate = cfg.Reconciler.Gate()
		cfg.Reconciler = nil
		cfg.Staging = nil
	}
}

var _ follow.Retention = (*recordingRetention)(nil)

// pausedIndexRead blocks the first archive-index Get after Arm. The follower's
// HTTP handler reads blobs through Server.Blocks, while its immutable Head
// engine reads index nodes through this wrapper, so the paused CID is a retired
// generation node rather than a blob shared with the successor.
type pausedIndexRead struct {
	blockstore.Blockstore

	mu      sync.Mutex
	armed   bool
	paused  cid.Cid
	entered chan struct{}
	release chan struct{}
}

func newPausedIndexRead(inner blockstore.Blockstore) *pausedIndexRead {
	return &pausedIndexRead{Blockstore: inner, entered: make(chan struct{}), release: make(chan struct{})}
}

func (p *pausedIndexRead) Arm() {
	p.mu.Lock()
	p.armed = true
	p.mu.Unlock()
}

func (p *pausedIndexRead) Get(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	p.mu.Lock()
	if !p.armed {
		p.mu.Unlock()
		return p.Blockstore.Get(ctx, c)
	}
	p.armed = false
	p.paused = c
	close(p.entered)
	release := p.release
	p.mu.Unlock()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-release:
		return p.Blockstore.Get(ctx, c)
	}
}

func (p *pausedIndexRead) PausedCID() cid.Cid {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.paused
}

// gcRetention simulates the destructive half of Kubo's old-anchor retirement:
// its second Commit removes the exact index block a pre-swap request is paused
// on. Without the shared exclusive barrier, Commit enters and the read resumes
// into a collected block. With it, the response fully materializes first.
type gcRetention struct {
	blocks blockstore.Blockstore
	paused *pausedIndexRead

	mu      sync.Mutex
	current *replica.Generation
	pending *replica.Generation
	cleanup chan struct{}
}

func (r *gcRetention) Prepare(_ context.Context, generation replica.Generation) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	copy := generation
	copy.Heads = slices.Clone(generation.Heads)
	r.pending = &copy
	return nil
}

func (r *gcRetention) Commit(ctx context.Context, generation replica.Generation) error {
	r.mu.Lock()
	if r.pending == nil || !r.pending.UpdatedAt.Equal(generation.UpdatedAt) || !slices.Equal(r.pending.Heads, generation.Heads) {
		r.mu.Unlock()
		return errors.New("gc retention: commit did not match pending generation")
	}
	old := r.current
	copy := *r.pending
	copy.Heads = slices.Clone(r.pending.Heads)
	r.current, r.pending = &copy, nil
	r.mu.Unlock()
	if old == nil {
		return nil
	}
	close(r.cleanup)
	target := r.paused.PausedCID()
	if !target.Defined() {
		return errors.New("gc retention: no old-generation read was paused")
	}
	return r.blocks.DeleteBlock(ctx, target)
}

func (*gcRetention) ProtectsAll(context.Context, []replica.Head) error { return nil }

func TestExternalRetentionDrainsOldReadersBeforeKuboCleanup(t *testing.T) {
	w := newWriter(t)
	w.ingestSlot(testOrigin, 1)

	n := newNode(t)
	gate := pinning.NewGate()
	paused := newPausedIndexRead(n.store.Blocks())
	var err error
	n.heads, err = server.NewHeads(server.HeadsConfig{
		Net: testNet, Roots: n.roots, Manifests: n.manifests, Blocks: paused, Gate: gate,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := n.host.Libp2p().Connect(t.Context(), peerInfo(w)); err != nil {
		t.Fatal(err)
	}
	fetch := p2p.FetchingBlockstore(t.Context(), paused, n.ex)
	retention := &gcRetention{blocks: n.store.Blocks(), paused: paused, cleanup: make(chan struct{})}
	follower, err := follow.New(follow.Config{
		Net: testNet, URL: w.url, PubKey: w.pubkey(), Verify: follow.VerifyCID,
		Heads: map[string]pinning.Policy{testHead: pinning.Full()},
		Local: paused, Fetch: fetch, Registry: n.heads, Roots: n.roots,
		Retention: retention, Gate: gate, KV: n.store.KV(), Logger: testLogger(t),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = follower.Close() })
	if err := follower.Poll(t.Context()); err != nil {
		t.Fatalf("first Poll: %v", err)
	}

	old, ok := n.heads.Get(testHead)
	if !ok {
		t.Fatal("first generation was not exposed")
	}
	oldRoot := old.Root()
	n.serveHTTP(nil)
	paused.Arm()

	type response struct {
		status int
		body   []byte
		err    error
	}
	responseDone := make(chan response, 1)
	go func() {
		resp, err := http.Get(fmt.Sprintf("%s/%s/eth/v1/beacon/blobs/%d", n.url, testHead, testOrigin))
		if err != nil {
			responseDone <- response{err: err}
			return
		}
		defer resp.Body.Close()
		body, readErr := io.ReadAll(resp.Body)
		responseDone <- response{status: resp.StatusCode, body: body, err: readErr}
	}()
	select {
	case <-paused.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("old-generation request did not pause on an index read")
	}
	if paused.PausedCID().Equals(oldRoot) {
		t.Fatal("test paused on the eagerly loaded Head block, not a lazily materialized old-generation index node")
	}

	w.ingestSlot(testOrigin+1, 2)
	want, _ := w.heads.Get(testHead)
	pollDone := make(chan error, 1)
	go func() { pollDone <- follower.Poll(t.Context()) }()

	deadline := time.After(5 * time.Second)
	for {
		current, exists := n.heads.Get(testHead)
		if exists && current.Root().Equals(want.Root()) {
			// pausedIndexRead can only intercept archive index reads. Enumerate
			// successor B's complete index closure and prove the paused A node is
			// not one of B's root, directory pages, or segment nodes. Deleting it
			// below therefore models a legitimate old-anchor GC, not corruption of
			// a block the successor still needs.
			enum, err := current.Enumerate(t.Context())
			if err != nil {
				t.Fatalf("enumerating successor generation: %v", err)
			}
			target := paused.PausedCID()
			reachable := enum.Root.Equals(target) || enum.Open.Equals(target)
			for _, page := range enum.DirPages {
				reachable = reachable || page.Equals(target)
			}
			for _, segment := range enum.Sealed {
				reachable = reachable || segment.CID.Equals(target)
			}
			if reachable {
				t.Fatalf("paused A index block %s remains reachable from successor B %s", target, enum.Root)
			}
			break
		}
		select {
		case err := <-pollDone:
			t.Fatalf("second Poll returned before exposing its generation: %v", err)
		case <-deadline:
			t.Fatal("second generation did not become visible")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	select {
	case <-retention.cleanup:
		t.Fatal("external cleanup crossed the post-publication barrier while an old reader was materializing")
	case err := <-pollDone:
		t.Fatalf("second Poll returned before the old reader drained: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	close(paused.release)
	var got response
	select {
	case got = <-responseDone:
	case <-time.After(5 * time.Second):
		t.Fatal("old-generation response did not finish")
	}
	if got.err != nil || got.status != http.StatusOK {
		t.Fatalf("old-generation response: status=%d err=%v body=%q", got.status, got.err, got.body)
	}
	var payload struct {
		Data []string `json:"data"`
	}
	if err := json.Unmarshal(got.body, &payload); err != nil || len(payload.Data) != 1 {
		t.Fatalf("old-generation response was not fully materialized: blobs=%d err=%v", len(payload.Data), err)
	}
	select {
	case err := <-pollDone:
		if err != nil {
			t.Fatalf("second Poll: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("second Poll did not complete after the old reader drained")
	}
	select {
	case <-retention.cleanup:
	default:
		t.Fatal("external cleanup did not run after the reader drained")
	}
	if has, err := n.store.Blocks().Has(t.Context(), paused.PausedCID()); err != nil {
		t.Fatal(err)
	} else if has {
		t.Fatal("simulated Kubo GC did not remove the retired index block")
	}

	// The deletion above is intentionally real in the fixture's blockstore.
	// Serve B afterward to prove the removed A-only node was genuinely outside
	// the successor closure and that retirement did not merely preserve A at the
	// cost of breaking the newly visible generation.
	resp, err := http.Get(fmt.Sprintf("%s/%s/eth/v1/beacon/blobs/%d", n.url, testHead, testOrigin+1))
	if err != nil {
		t.Fatalf("requesting successor generation: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reading successor generation: %v", err)
	}
	var successorPayload struct {
		Data []string `json:"data"`
	}
	if err := json.Unmarshal(body, &successorPayload); err != nil || resp.StatusCode != http.StatusOK || len(successorPayload.Data) != 1 {
		t.Fatalf("successor B after A cleanup: status=%d blobs=%d err=%v body=%q",
			resp.StatusCode, len(successorPayload.Data), err, body)
	}
}

var _ follow.Retention = (*gcRetention)(nil)
