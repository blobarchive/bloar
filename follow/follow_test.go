package follow_test

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/blobarchive/bloar/follow"
	"github.com/blobarchive/bloar/metrics"
	"github.com/blobarchive/bloar/p2p/pointerhint"
	"github.com/blobarchive/bloar/pinning"
	"github.com/blobarchive/bloar/server"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
)

// TestAdoptAndServe is the protocol end to end: a writer archives blobs, a
// follower polls its document, fetches the DAG over bitswap, and answers the
// same read API with the same bytes -- having been told nothing but a URL and a
// public key.
func TestAdoptAndServe(t *testing.T) {
	w := newWriter(t)
	blobs, vhs := w.ingestSlot(100, 1, 2)

	f := newFollower(t, w)
	f.serveHTTP(nil)
	f.poll()

	status, data, _ := f.blobsAt(100)
	if status != http.StatusOK {
		t.Fatalf("GET slot 100 from the follower: status = %d, want 200", status)
	}
	if len(data) != 2 {
		t.Fatalf("got %d blobs, want 2", len(data))
	}
	for i, want := range blobs {
		if data[i] != "0x"+hex.EncodeToString(want) {
			t.Errorf("blob %d is not the bytes the writer ingested", i)
		}
	}

	// The filtered form nitro actually uses, in request order.
	status, data, _ = f.blobsAt(100, vhs[1], vhs[0])
	if status != http.StatusOK {
		t.Fatalf("filtered GET: status = %d, want 200", status)
	}
	if data[0] != "0x"+hex.EncodeToString(blobs[1]) || data[1] != "0x"+hex.EncodeToString(blobs[0]) {
		t.Error("the filtered response is not in request order")
	}

	// A followed head is published like a written one, so a follower of this
	// follower would work (spec 11.3's chain of adoptions).
	if names := f.heads.Names(); len(names) != 1 || names[0] != testHead {
		t.Errorf("the follower's registry has %v, want [%s]", names, testHead)
	}
	head, ok := f.heads.Get(testHead)
	if !ok {
		t.Fatal("the follower did not register the head")
	}
	if syncedTo, covered := head.SyncedTo(); !covered || syncedTo != 100 {
		t.Errorf("the follower's synced_to = %d (covered %t), want 100", syncedTo, covered)
	}
	if head.Root() != w.head.Root() {
		t.Errorf("the follower adopted root %s, the writer published %s", head.Root(), w.head.Root())
	}
}

func TestRootFetchMissFindsExactPointerAndRetries(t *testing.T) {
	w := newWriter(t)
	w.ingestSlot(testOrigin, 1)

	var f *follower
	findCalls := 0
	f = newLoneFollower(t, w, func(cfg *follow.Config) {
		// Suppress the authenticated multiaddr convenience path so the first
		// ordinary root fetch proves the exact-pointer fallback connects the
		// provider rather than generic Bitswap routing doing so.
		cfg.Host = nil
		cfg.FetchTimeout = 50 * time.Millisecond
		cfg.FindPointer = func(ctx context.Context, pointer pointerhint.Pointer) error {
			findCalls++
			if pointer.Kind != pointerhint.Root || !pointer.CID.Equals(w.head.Root()) {
				return fmt.Errorf("finder received %s %s, want root %s", pointer.Kind, pointer.CID, w.head.Root())
			}
			return f.host.Libp2p().Connect(ctx, peerInfo(w))
		}
	})
	if err := f.pollErr(); err != nil {
		t.Fatalf("Poll after exact root discovery: %v", err)
	}
	if findCalls != 1 {
		t.Fatalf("exact root finder calls = %d, want 1", findCalls)
	}
	if got, ok := f.heads.Get(testHead); !ok || !got.Root().Equals(w.head.Root()) {
		t.Fatalf("adopted root after discovery = %v, present=%t; want %s", got, ok, w.head.Root())
	}
}

func TestSyncFetchMissFindsCurrentRootNotMissingDescendant(t *testing.T) {
	w := newWriter(t)
	w.ingestSlot(testOrigin, 1)

	var f *follower
	findCalls := 0
	f = newLoneFollower(t, w, func(cfg *follow.Config) {
		// The authenticated Head block is copied below, so initial adoption has
		// no root miss. Suppress document-address dialing to leave the follower
		// cold until sync reaches a missing descendant.
		cfg.Host = nil
		cfg.FetchTimeout = 50 * time.Millisecond
		cfg.FindPointer = func(ctx context.Context, pointer pointerhint.Pointer) error {
			findCalls++
			if pointer.Kind != pointerhint.Root || !pointer.CID.Equals(w.head.Root()) {
				return fmt.Errorf("finder received %s %s, want current root %s", pointer.Kind, pointer.CID, w.head.Root())
			}
			return f.host.Libp2p().Connect(ctx, peerInfo(w))
		}
	})

	rootBlock, err := w.store.Blocks().Get(t.Context(), w.head.Root())
	if err != nil {
		t.Fatalf("reading writer Head block: %v", err)
	}
	if err := f.store.Blocks().Put(t.Context(), rootBlock); err != nil {
		t.Fatalf("preseeding follower Head block: %v", err)
	}
	enum, err := w.head.Enumerate(t.Context())
	if err != nil {
		t.Fatalf("enumerating writer head: %v", err)
	}
	if !enum.Open.Defined() {
		t.Fatal("fixture has no descendant open Segment")
	}
	if has, err := f.store.Blocks().Has(t.Context(), enum.Open); err != nil || has {
		t.Fatalf("follower has descendant before Poll = %t, %v; want false", has, err)
	}

	if err := f.pollErr(); err != nil {
		t.Fatalf("Poll after exact current-root discovery during sync: %v", err)
	}
	if findCalls != 1 {
		t.Fatalf("exact root finder calls = %d, want 1", findCalls)
	}
	if has, err := f.store.Blocks().Has(t.Context(), enum.Open); err != nil || !has {
		t.Fatalf("follower has descendant after retry = %t, %v; want true", has, err)
	}
}

func TestSyncManifestFetchMissFindsTipNotMissingAncestor(t *testing.T) {
	w := newWriter(t)
	w.ingestSlot(testOrigin, 1)
	firstTip := w.setManifest(cid.Undef, 1)
	currentTip := w.setManifest(firstTip, 2)

	var f *follower
	findCalls := 0
	f = newLoneFollower(t, w, func(cfg *follow.Config) {
		cfg.Host = nil
		cfg.FetchTimeout = 50 * time.Millisecond
		cfg.FindPointer = func(ctx context.Context, pointer pointerhint.Pointer) error {
			findCalls++
			if pointer.Kind != pointerhint.Manifest || !pointer.CID.Equals(currentTip) {
				return fmt.Errorf("finder received %s %s, want current manifest %s", pointer.Kind, pointer.CID, currentTip)
			}
			return f.host.Libp2p().Connect(ctx, peerInfo(w))
		}
	})

	// Stage a crash/restart shape: the complete archive and new tip are local,
	// while the durable checkpoint still names the previous tip. Resume first
	// proves that generation. Removing only the old tip afterward leaves the
	// next signed publication's ancestry preflight satisfiable (the new tip
	// links to the known floor) but makes the post-adoption manifest walk miss.
	keys, err := w.store.Blocks().AllKeysChan(t.Context())
	if err != nil {
		t.Fatalf("enumerating writer blocks: %v", err)
	}
	for c := range keys {
		block, err := w.store.Blocks().Get(t.Context(), c)
		if err != nil {
			t.Fatalf("reading writer block %s: %v", c, err)
		}
		if err := f.store.Blocks().Put(t.Context(), block); err != nil {
			t.Fatalf("copying writer block %s: %v", c, err)
		}
	}
	syncedTo, covered := w.head.SyncedTo()
	if !covered {
		t.Fatal("writer fixture is not covered")
	}
	if err := follow.WriteCheckpoint(f.store.KV(), testHead, w.head.Root(), syncedTo, firstTip, time.Now().Add(-time.Hour)); err != nil {
		t.Fatalf("writing prior manifest checkpoint: %v", err)
	}
	if err := f.f.Resume(t.Context()); err != nil {
		t.Fatalf("resuming prior manifest generation: %v", err)
	}
	if err := f.store.Blocks().DeleteBlock(t.Context(), firstTip); err != nil {
		t.Fatalf("removing old manifest ancestor: %v", err)
	}

	if err := f.pollErr(); err != nil {
		t.Fatalf("Poll after exact manifest-tip discovery during sync: %v", err)
	}
	if findCalls != 1 {
		t.Fatalf("exact manifest finder calls = %d, want 1", findCalls)
	}
	if has, err := f.store.Blocks().Has(t.Context(), firstTip); err != nil || !has {
		t.Fatalf("follower has missing manifest ancestor after retry = %t, %v; want true", has, err)
	}
}

func TestQuarantineReportsServiceabilityChange(t *testing.T) {
	w := newWriter(t)
	w.ingestSlot(testOrigin, 1)
	changes := 0
	f := newFollower(t, w, func(cfg *follow.Config) {
		cfg.OnServiceabilityChanged = func() error {
			changes++
			return nil
		}
	})
	f.poll()
	if err := follow.QuarantineHead(f.f, testHead, "test pointer withdrawal"); err == nil {
		t.Fatal("QuarantineHead returned nil")
	}
	if changes != 1 {
		t.Fatalf("serviceability change callbacks = %d, want 1", changes)
	}
}

// TestAdoptTracksTheWriter: every poll adopts what the writer has published
// since the last one, and the follower's answers move with it.
func TestAdoptTracksTheWriter(t *testing.T) {
	w := newWriter(t)
	w.ingestSlot(100, 1)

	f := newFollower(t, w)
	f.serveHTTP(nil)
	f.poll()

	if status, _, _ := f.blobsAt(120); status != http.StatusServiceUnavailable {
		t.Fatalf("GET an unarchived slot: status = %d, want 503", status)
	}

	blobs, _ := w.ingestSlot(120, 7)
	f.poll()

	status, data, _ := f.blobsAt(120)
	if status != http.StatusOK {
		t.Fatalf("GET slot 120 after the second poll: status = %d, want 200", status)
	}
	if data[0] != "0x"+hex.EncodeToString(blobs[0]) {
		t.Error("slot 120 is not the bytes the writer ingested")
	}
}

// TestAdoptDialsThePublishedMultiaddrs: the follower is given a URL and nothing
// else -- no peers, no connection -- and finds the blocks anyway, because spec
// 11.2 has it dial the multiaddrs the document advertises.
func TestAdoptDialsThePublishedMultiaddrs(t *testing.T) {
	w := newWriter(t)
	blobs, _ := w.ingestSlot(100, 1)

	f := newLoneFollower(t, w)
	f.serveHTTP(nil)

	// Nothing connects these two but the multiaddrs in the document the poll
	// itself resolves.
	f.poll()

	status, data, _ := f.blobsAt(100)
	if status != http.StatusOK {
		t.Fatalf("GET slot 100: status = %d, want 200", status)
	}
	if data[0] != "0x"+hex.EncodeToString(blobs[0]) {
		t.Error("slot 100 is not the bytes the writer ingested")
	}
}

func TestAdoptPassesAuthenticatedMultiaddrsToExternalDialer(t *testing.T) {
	w := newWriter(t)
	w.ingestSlot(100, 1)
	called := make(chan peer.AddrInfo, 1)
	f := newLoneFollower(t, w, func(cfg *follow.Config) {
		cfg.Host = nil
		cfg.FetchTimeout = 10 * time.Millisecond
		cfg.DialPeer = func(_ context.Context, target peer.AddrInfo) error {
			called <- target
			return errors.New("simulated external dial failure")
		}
	})
	// The dial hint is non-authoritative and its failure is nonfatal; this poll
	// may still fail later because the deliberately disconnected test follower
	// cannot fetch the root in ten milliseconds.
	_ = f.f.Poll(t.Context())
	select {
	case target := <-called:
		if target.ID != w.host.ID() {
			t.Fatalf("external dial target = %s, want writer %s", target.ID, w.host.ID())
		}
	default:
		t.Fatal("authenticated publication multiaddr was not passed to external dialer")
	}
}

// TestNoRegressionSyncedTo is spec 11.3's first rule, on its own: a document
// that is fresh, correctly signed, and says a head has less coverage than this
// node has already adopted is refused.
//
// The document is dated in the future so that only the synced_to rule can
// reject it. That is the point of testing the rules independently: a
// no-regression check that only worked because another check fired first would
// pass a test that asserted "it was refused" and fail the moment an attacker
// dated their replay correctly.
func TestNoRegressionSyncedTo(t *testing.T) {
	w := newWriter(t)
	docs := newDocServer(t)
	mx := metrics.New()
	f := newFollower(t, w, func(c *follow.Config) { c.URL = docs.url; c.Metrics = mx })
	f.serveHTTP(nil)

	w.ingestSlot(100, 1)
	old := w.unsigned(time.Now())
	oldRoot, oldSyncedTo := w.head.Root(), uint64(100)

	w.ingestSlot(120, 2)
	docs.publish(t, w, time.Now())
	f.poll()

	if syncedTo := followerSyncedTo(t, f); syncedTo != 120 {
		t.Fatalf("the follower adopted synced_to %d, want 120", syncedTo)
	}

	// The rollback: the earlier root, republished later than the document that
	// superseded it.
	docs.republishAt(t, w, withRoot(old, oldRoot, oldSyncedTo), time.Now().Add(time.Hour))
	err := f.pollErr()
	if err == nil {
		t.Fatal("the follower adopted a document that rolled its head back")
	}
	if !strings.Contains(err.Error(), "below the adopted floor") {
		t.Errorf("err = %v, want it to name the floor", err)
	}
	if syncedTo := followerSyncedTo(t, f); syncedTo != 120 {
		t.Errorf("the follower's synced_to = %d after the rollback, want it to stay at 120", syncedTo)
	}
	if status, _, _ := f.blobsAt(120); status != http.StatusOK {
		t.Errorf("GET slot 120 after the rollback: status = %d, want the follower to still serve it", status)
	}

	// The rollback-document regression is about a poll that resolved a valid document --
	// it decoded, verified, and was newer -- so follow_polls_total counts it "ok",
	// exactly as it did the first poll. The regression is a per-head refusal that
	// happens after that judgement, and only follow_refusals_total sees it.
	if got := refusalCount(t, mx, metrics.RefusalSyncedToFloor); got != 1 {
		t.Errorf("bloar_follow_refusals_total{reason=%q} = %g, want 1", metrics.RefusalSyncedToFloor, got)
	}
	if got := pollCount(t, mx, metrics.ChannelHTTPS, metrics.OutcomeOK); got != 2 {
		t.Errorf("bloar_follow_polls_total{outcome=ok} = %g, want 2: the refused poll still resolved a valid document", got)
	}

	// The refusal opens a divergence window (spec 11.3), and the floor-lag gauge
	// is what makes it visible while it lasts: the follower is serving up to slot
	// 120 as covered, the writer now claims only 100, and the lag is the 20 slots
	// between where the two answer differently.
	if got := floorLagGauge(t, mx, testHead); got != 20 {
		t.Errorf("bloar_follow_synced_to_floor_lag{head=%q} = %g, want 20 (floor 120 - published 100)", testHead, got)
	}

	// The window self-heals when the writer's coverage passes the floor again. The
	// writer re-syncs past where it truncated and republishes; the follower accepts
	// this time, which closes the window and resets the gauge to 0.
	w.ingestSlot(140, 3)
	docs.publish(t, w, time.Now().Add(2*time.Hour))
	f.poll()
	if syncedTo := followerSyncedTo(t, f); syncedTo != 140 {
		t.Fatalf("the follower's synced_to = %d after the writer re-synced, want 140", syncedTo)
	}
	if got := floorLagGauge(t, mx, testHead); got != 0 {
		t.Errorf("bloar_follow_synced_to_floor_lag{head=%q} = %g after an accepted publication, want 0", testHead, got)
	}
}

// TestNoRegressionUpdatedAt is spec 11.3's second rule, on its own: a document
// older than one already accepted is refused whatever it says about the heads.
//
// Its head claims are fresh -- a later root, more coverage -- so nothing but the
// timestamp can reject it. This is the withheld-update attack of spec 8.1 in its
// pure form: an attacker replaying yesterday's document does not get to have it
// adopted because it happens to name a root this node has not seen.
func TestNoRegressionUpdatedAt(t *testing.T) {
	w := newWriter(t)
	docs := newDocServer(t)
	f := newFollower(t, w, func(c *follow.Config) { c.URL = docs.url })

	w.ingestSlot(100, 1)
	docs.publish(t, w, time.Now())
	f.poll()

	w.ingestSlot(120, 2)
	docs.republishAt(t, w, w.unsigned(time.Now()), time.Now().Add(-time.Hour))
	err := f.pollErr()
	if err == nil {
		t.Fatal("the follower adopted a document dated before the one it had already accepted")
	}
	if !strings.Contains(err.Error(), "before the accepted floor") {
		t.Errorf("err = %v, want it to name the floor", err)
	}
	if syncedTo := followerSyncedTo(t, f); syncedTo != 100 {
		t.Errorf("the follower's synced_to = %d, want it to have ignored the stale document and stayed at 100", syncedTo)
	}
}

// TestLogicalArchiveV3IsFollowed proves the version gate and the ordinary
// single-source follower both understand publication v3. Multi-source
// arbitration is layered above this path; adding a signed archive identity must
// not make a current one-authority deployment unreadable.
func TestLogicalArchiveV3IsFollowed(t *testing.T) {
	w := newWriter(t)
	docs := newDocServer(t)
	id, err := server.ParseArchiveID("0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20")
	if err != nil {
		t.Fatal(err)
	}
	f := newFollower(t, w, func(c *follow.Config) {
		c.URL = docs.url
		c.ExpectedArchiveID = &id
	})

	w.ingestSlot(100, 1)
	u := w.unsigned(time.Now())
	revision := uint64(1)
	u.V = server.LogicalArchiveDocVersion
	u.ArchiveID = &id
	u.Revision = &revision
	docs.set(sign(t, w.key, u))
	f.poll()

	if syncedTo := followerSyncedTo(t, f); syncedTo != 100 {
		t.Fatalf("v3 follower synced_to = %d, want 100", syncedTo)
	}
}

func TestExpectedArchiveIDRefusesOtherOrLegacyArchives(t *testing.T) {
	w := newWriter(t)
	docs := newDocServer(t)
	expected, err := server.ParseArchiveID("0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20")
	if err != nil {
		t.Fatal(err)
	}
	f := newFollower(t, w, func(c *follow.Config) {
		c.URL = docs.url
		c.ExpectedArchiveID = &expected
	})

	w.ingestSlot(100, 1)
	revision := uint64(1)
	other := expected
	other[0] ^= 0xff
	u := w.unsigned(time.Now())
	u.V, u.ArchiveID, u.Revision = server.LogicalArchiveDocVersion, &other, &revision
	docs.set(sign(t, w.key, u))
	if err := f.pollErr(); err == nil || !strings.Contains(err.Error(), "is for logical archive") {
		t.Fatalf("wrong archive poll error = %v, want logical archive refusal", err)
	}
	if _, ok := f.heads.Get(testHead); ok {
		t.Fatal("wrong logical archive was adopted")
	}

	legacy := w.unsigned(time.Now().Add(time.Second))
	legacy.V = server.DocVersion
	legacy.Revision = &revision
	docs.set(sign(t, w.key, legacy))
	if err := f.pollErr(); err == nil || !strings.Contains(err.Error(), "has no signed logical archive identity") {
		t.Fatalf("legacy archive poll error = %v, want missing archive identity refusal", err)
	}
	if _, ok := f.heads.Get(testHead); ok {
		t.Fatal("legacy document was adopted by an archive-pinned follower")
	}
}

// TestNoRegressionSurvivesRestart: the floors are on disk, so a follower that
// is restarted and immediately handed a rolled-back document refuses it -- which
// is the moment the attack is worth trying, and the moment an in-memory floor
// would be gone.
func TestNoRegressionSurvivesRestart(t *testing.T) {
	w := newWriter(t)
	docs := newDocServer(t)
	f := newFollower(t, w, func(c *follow.Config) { c.URL = docs.url })

	w.ingestSlot(100, 1)
	old := w.unsigned(time.Now())
	oldRoot := w.head.Root()

	w.ingestSlot(120, 2)
	docs.publish(t, w, time.Now())
	f.poll()

	// A restart: a new registry and a new follower over the same store, which is
	// the same state a process that came back up would find.
	restarted := f.restart(t, w, func(c *follow.Config) { c.URL = docs.url })
	if err := restarted.Resume(t.Context()); err != nil {
		t.Fatalf("Resume: %v", err)
	}

	docs.republishAt(t, w, withRoot(old, oldRoot, 100), time.Now().Add(time.Hour))
	if err := restarted.Poll(t.Context()); err == nil {
		t.Fatal("a restarted follower adopted a rolled-back document")
	} else if !strings.Contains(err.Error(), "below the adopted floor") {
		t.Errorf("err = %v, want it to name the floor", err)
	}
}

// TestResumeServesWithoutTheWriter: a follower that restarts serves the heads it
// had adopted before it can reach anything. The root and the blocks are on disk;
// waiting for a poll to serve them would be an outage for no reason.
func TestResumeServesWithoutTheWriter(t *testing.T) {
	w := newWriter(t)
	blobs, _ := w.ingestSlot(100, 1)

	f := newFollower(t, w)
	f.poll()

	// The writer's document channel goes away entirely.
	w.http.Close()

	restarted := f.restart(t, w)
	f.serveHTTP(nil)
	if err := restarted.Resume(t.Context()); err != nil {
		t.Fatalf("Resume: %v", err)
	}

	status, data, _ := f.blobsAt(100)
	if status != http.StatusOK {
		t.Fatalf("GET slot 100 after a restart with the writer down: status = %d, want 200", status)
	}
	if data[0] != "0x"+hex.EncodeToString(blobs[0]) {
		t.Error("slot 100 is not the bytes the writer ingested")
	}
}

// TestUnsignedDocumentRefused: this build follows a key, not a URL. An unsigned
// document is an unauthenticated claim about what to serve, and there is no
// configuration in which one is adopted.
func TestUnsignedDocumentRefused(t *testing.T) {
	w := newWriter(t)
	docs := newDocServer(t)
	f := newFollower(t, w, func(c *follow.Config) { c.URL = docs.url })

	w.ingestSlot(100, 1)
	u := w.unsigned(time.Now())
	body, err := u.Canonical()
	if err != nil {
		t.Fatalf("Unsigned.Canonical: %v", err)
	}
	docs.set(body) // the unsigned document: no pubkey, no signature.

	if err := f.pollErr(); err == nil {
		t.Fatal("the follower adopted an unsigned document")
	}
	if _, ok := f.heads.Get(testHead); ok {
		t.Error("the follower registered a head from an unsigned document")
	}
}

// TestWrongKeyRefused: a document signed by a key that is not the one this node
// follows verifies perfectly against the key it carries. That is exactly why the
// key it carries is checked against the configured one first.
func TestWrongKeyRefused(t *testing.T) {
	w := newWriter(t)
	docs := newDocServer(t)
	f := newFollower(t, w, func(c *follow.Config) { c.URL = docs.url })

	w.ingestSlot(100, 1)

	// A perfectly good archive, signed by somebody else.
	other := newWriter(t)
	docs.set(sign(t, other.key, w.unsigned(time.Now())))

	err := f.pollErr()
	if err == nil {
		t.Fatal("the follower adopted a document signed by a key it does not follow")
	}
	if !strings.Contains(err.Error(), "does not follow") {
		t.Errorf("err = %v, want it to say the key is not the followed one", err)
	}
	if _, ok := f.heads.Get(testHead); ok {
		t.Error("the follower registered a head from a document signed by the wrong key")
	}
}

// TestPollSurvivesAnUnreachableWriter: the document channel failing is not the
// follower failing. It reports the failure and goes on serving what it adopted.
func TestPollSurvivesAnUnreachableWriter(t *testing.T) {
	w := newWriter(t)
	docs := newDocServer(t)
	f := newFollower(t, w, func(c *follow.Config) { c.URL = docs.url })
	f.serveHTTP(nil)

	w.ingestSlot(100, 1)
	docs.publish(t, w, time.Now())
	f.poll()

	docs.status(http.StatusInternalServerError)
	if err := f.pollErr(); err == nil {
		t.Fatal("a poll against a broken document server reported success")
	}
	if status, _, _ := f.blobsAt(100); status != http.StatusOK {
		t.Errorf("GET slot 100 while the writer is broken: status = %d, want the follower to still serve it", status)
	}
}

// TestMutationRefusedOnAFollowedHead is spec 11.1 at the HTTP layer: exactly one
// writer per head, and this node is not it. 403 -- not 404 (the head is right
// there, and read-serving), not 409 (no state of this node accepts it), not 400
// (the request is fine).
func TestMutationRefusedOnAFollowedHead(t *testing.T) {
	w := newWriter(t)
	w.ingestSlot(100, 1)

	f := newFollower(t, w)
	f.serveHTTP(nil)
	f.poll()

	for _, tc := range []struct{ name, path, body string }{
		{"refs", "/refs", `{"rows": [], "synced_to": 130}`},
		{"truncate", "/truncate", `{"slot": 99, "confirm": "` + testHead + `"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req, err := http.NewRequestWithContext(t.Context(), http.MethodPost,
				f.url+"/bloar/v1/heads/"+testHead+tc.path, strings.NewReader(tc.body))
			if err != nil {
				t.Fatalf("building the request: %v", err)
			}
			req.Header.Set("Authorization", "Bearer "+testToken)
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("POST %s: %v", tc.path, err)
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusForbidden {
				t.Errorf("POST %s on a followed head: status = %d, want 403", tc.path, resp.StatusCode)
			}
		})
	}

	// And the head is untouched: still the writer's root, still serving.
	head, _ := f.heads.Get(testHead)
	if head.Root() != w.head.Root() {
		t.Error("a refused mutation moved the followed head")
	}
}

// followerSyncedTo reads the coverage the follower is serving.
func followerSyncedTo(t *testing.T, f *follower) uint64 {
	t.Helper()
	head, ok := f.heads.Get(testHead)
	if !ok {
		t.Fatal("the follower has no head registered")
	}
	syncedTo, covered := head.SyncedTo()
	if !covered {
		t.Fatal("the follower's head is empty")
	}
	return syncedTo
}

// restart builds a second follower over the same store, registry-fresh: what a
// process that came back up would have. The old one's session is closed, since
// two followers fetching through one exchange is not a thing a daemon does.
func (f *follower) restart(t *testing.T, w *writer, opts ...func(*follow.Config)) *follow.Follower {
	t.Helper()
	if err := f.f.Close(); err != nil {
		t.Fatalf("closing the first follower: %v", err)
	}

	gate := pinning.NewGate()
	heads, err := server.NewHeads(server.HeadsConfig{
		Net: testNet, Roots: f.roots, Manifests: f.manifests,
		Blocks: f.store.Blocks(), Gate: gate,
	})
	if err != nil {
		t.Fatalf("server.NewHeads: %v", err)
	}
	f.heads = heads

	rec, err := pinning.NewReconciler(pinning.Config{
		Ledger: ledgerOf(f.node), ManifestTip: f.manifests.Get, Gate: gate,
	})
	if err != nil {
		t.Fatalf("pinning.NewReconciler: %v", err)
	}
	f.rec = rec

	cfg := follow.Config{
		Net:        testNet,
		URL:        w.url,
		PubKey:     w.pubkey(),
		Heads:      map[string]pinning.Policy{testHead: pinning.Full()},
		Local:      f.store.Blocks(),
		Sessions:   f.ex,
		Host:       f.host,
		Registry:   f.heads,
		Roots:      f.roots,
		Reconciler: f.rec,
		Staging:    f.staging,
		KV:         f.store.KV(),
		Cache:      f.cache,
		Logger:     testLogger(t),
	}
	for _, o := range opts {
		o(&cfg)
	}
	next, err := follow.New(cfg)
	if err != nil {
		t.Fatalf("follow.New: %v", err)
	}
	f.f = next
	t.Cleanup(func() {
		if err := next.Close(); err != nil {
			t.Errorf("closing the restarted follower: %v", err)
		}
	})
	return next
}
