package follow_test

import (
	"context"
	"crypto/ed25519"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/ipfs/boxo/namesys"

	"github.com/blobarchive/bloar/follow"
	"github.com/blobarchive/bloar/server"
)

func dnslinkLookup(target *string, failure *error) namesys.LookupTXTFunc {
	return func(ctx context.Context, query string) ([]string, error) {
		if _, ok := ctx.Deadline(); !ok {
			return nil, errors.New("DNSLink lookup has no deadline")
		}
		if query != "_dnslink.swarm.example." {
			return nil, errors.New("unexpected DNSLink query: " + query)
		}
		if *failure != nil {
			return nil, *failure
		}
		return []string{"dnslink=/ipns/" + *target}, nil
	}
}

func newDNSLinkFollower(t *testing.T, w *ipnsWriter, target *string, failure *error, pinned ed25519.PublicKey) *follower {
	t.Helper()
	return newFollower(t, w.writer, func(c *follow.Config) {
		c.URL = ""
		c.IPNS = ""
		c.DNSLink = "swarm.example"
		c.LookupTXT = dnslinkLookup(target, failure)
		c.Routing = w.routing
		c.PubKey = pinned
	})
}

func TestDNSLinkBootstrapsSignerAndSurvivesResolverFailure(t *testing.T) {
	w := newIPNSWriter(t)
	target := w.name()
	var dnsFailure error
	f := newDNSLinkFollower(t, w, &target, &dnsFailure, nil)
	f.serveHTTP(nil)

	w.ingestSlot(100, 1)
	w.publish(t, w.heads.Doc())
	f.poll()
	name, signer, ok, err := follow.ReadDelegation(f.store.KV())
	if err != nil || !ok {
		t.Fatalf("reading committed delegation: name=%q ok=%t err=%v", name, ok, err)
	}
	if name != w.name() || !signer.Equal(w.pubkey()) {
		t.Fatalf("delegation = (%s, %x), want (%s, %x)", name, signer, w.name(), w.pubkey())
	}

	// DNS is now unavailable. The last *admitted* name and signer are durable
	// fallback authority, so a newer record on that name remains usable.
	dnsFailure = errors.New("temporary resolver failure")
	w.ingestSlot(120, 2)
	w.publish(t, w.heads.Doc())
	if err := f.pollErr(); err != nil {
		t.Fatalf("poll with DNS fallback: %v", err)
	}
	if got := followerSyncedTo(t, f); got != 120 {
		t.Fatalf("fallback adopted synced_to %d, want 120", got)
	}
	if status, _, _ := f.blobsAt(120); status != http.StatusOK {
		t.Fatalf("GET slot 120 after DNS fallback = %d, want 200", status)
	}
}

func TestDNSLinkDelegationAndCheckpointCommitBeforeExposure(t *testing.T) {
	w := newIPNSWriter(t)
	target := w.name()
	var dnsFailure error
	f := newDNSLinkFollower(t, w, &target, &dnsFailure, nil)
	f.serveHTTP(nil)
	w.ingestSlot(100, 1)
	w.publish(t, w.heads.Doc())

	observed := false
	follow.SetBeforeExposeHook(func() {
		root, syncedTo, _, _, ok, err := follow.ReadCheckpoint(f.store.KV(), testHead)
		if err != nil || !ok || root != w.head.Root() || syncedTo != 100 {
			t.Fatalf("checkpoint at exposure boundary = (%s, %d, ok=%t, err=%v)", root, syncedTo, ok, err)
		}
		name, signer, ok, err := follow.ReadDelegation(f.store.KV())
		if err != nil || !ok || name != w.name() || !signer.Equal(w.pubkey()) {
			t.Fatalf("delegation at exposure boundary = (%q, %x, ok=%t, err=%v)", name, signer, ok, err)
		}
		observed = true
	})
	defer follow.SetBeforeExposeHook(nil)
	f.poll()
	if !observed {
		t.Fatal("exposure hook did not observe the atomic authority/checkpoint commit")
	}
}

func TestDNSLinkRotatesNameAndDocumentSigner(t *testing.T) {
	first := newIPNSWriter(t)
	second := newIPNSWriter(t)
	target := first.name()
	var dnsFailure error
	f := newDNSLinkFollower(t, first, &target, &dnsFailure, nil)
	f.connect(second.writer)
	f.serveHTTP(nil)

	first.ingestSlot(100, 1)
	first.publish(t, first.heads.Doc())
	f.poll()

	second.ingestSlot(120, 2)
	second.publish(t, second.heads.Doc())
	// The follower uses one DHT abstraction; copy the second writer's authentic
	// record into that routing view, as a converged public DHT would.
	first.routing.put(string(second.pub.Name().RoutingKey()), second.record(t))
	target = second.name()
	if err := f.pollErr(); err != nil {
		t.Fatalf("poll after DNSLink rotation: %v", err)
	}
	if got := followerSyncedTo(t, f); got != 120 {
		t.Fatalf("rotated signer adopted synced_to %d, want 120", got)
	}
	name, signer, ok, err := follow.ReadDelegation(f.store.KV())
	if err != nil || !ok || name != second.name() || !signer.Equal(second.pubkey()) {
		t.Fatalf("rotated delegation = (%q, %x, ok=%t, err=%v), want (%q, %x)",
			name, signer, ok, err, second.name(), second.pubkey())
	}
	for _, writer := range []*ipnsWriter{first, second} {
		if _, ok, err := follow.ReadIPNSSeqFor(f.store.KV(), writer.name()); err != nil || !ok {
			t.Fatalf("per-name sequence floor for %s: ok=%t err=%v", writer.name(), ok, err)
		}
	}
}

func TestDNSLinkRevisionedSignerHandoffBeatsOldHTTPSFutureClock(t *testing.T) {
	first := newIPNSWriter(t)
	second := newIPNSWriter(t)
	docs := newDocServer(t)
	target := first.name()
	var dnsFailure error

	first.ingestSlot(100, 1)
	oldAt := time.Unix(4_000_000_000, 0)
	oldBody := sign(t, first.key, revisionedUnsigned(first.writer, first.head, 99, oldAt, server.FinalizedMonotonic))
	first.publish(t, oldBody)
	docs.set(oldBody)

	f := newFollower(t, first.writer, func(c *follow.Config) {
		c.URL = docs.url
		c.IPNS = ""
		c.DNSLink = "swarm.example"
		c.LookupTXT = dnslinkLookup(&target, &dnsFailure)
		c.Routing = first.routing
		c.PubKey = nil
	})
	f.connect(second.writer)
	f.poll()

	second.ingestSlot(120, 2)
	newAt := time.Unix(2_000_000_000, 0)
	newBody := sign(t, second.key, revisionedUnsigned(second.writer, second.head, 1, newAt, server.FinalizedMonotonic))
	second.publish(t, newBody)
	first.routing.put(string(second.pub.Name().RoutingKey()), second.record(t))
	target = second.name()

	// HTTPS still carries the retired signer's revision 99 and far-future clock.
	// DNSLink is the authenticated authority handoff, so its signer-local revision
	// 1 wins without comparing either incomparable revision or diagnostic clock.
	if err := f.pollErr(); err != nil {
		t.Fatalf("poll after revisioned DNSLink signer handoff: %v", err)
	}
	if got := followerSyncedTo(t, f); got != 120 {
		t.Fatalf("rotated signer adopted synced_to %d, want 120", got)
	}
	name, signer, ok, err := follow.ReadDelegation(f.store.KV())
	if err != nil || !ok || name != second.name() || !signer.Equal(second.pubkey()) {
		t.Fatalf("rotated delegation = (%q, %x, ok=%t, err=%v), want (%q, %x)",
			name, signer, ok, err, second.name(), second.pubkey())
	}
	if revision, _, ok, err := follow.ReadAuthorityFloor(f.store.KV(), second.pubkey()); err != nil || !ok || revision != 1 {
		t.Fatalf("new signer authority floor = %d ok=%t err=%v, want revision 1", revision, ok, err)
	}
}

func TestDNSLinkExplicitSignerPinRefusesRotation(t *testing.T) {
	first := newIPNSWriter(t)
	second := newIPNSWriter(t)
	target := first.name()
	var dnsFailure error
	f := newDNSLinkFollower(t, first, &target, &dnsFailure, first.pubkey())
	f.connect(second.writer)
	f.serveHTTP(nil)

	first.ingestSlot(100, 1)
	first.publish(t, first.heads.Doc())
	f.poll()

	second.ingestSlot(120, 2)
	second.publish(t, second.heads.Doc())
	first.routing.put(string(second.pub.Name().RoutingKey()), second.record(t))
	target = second.name()
	err := f.pollErr()
	if err == nil || !strings.Contains(err.Error(), "key it does not follow") {
		t.Fatalf("pinned signer rotation error = %v", err)
	}
	if got := followerSyncedTo(t, f); got != 100 {
		t.Fatalf("pinned follower advanced to %d, want 100", got)
	}
}

func TestHTTPSCannotBootstrapUnpinnedDNSLinkSigner(t *testing.T) {
	w := newIPNSWriter(t)
	docs := newDocServer(t)
	docs.publish(t, w.writer, time.Now())
	target := w.name()
	dnsFailure := errors.New("resolver unavailable")

	f := newFollower(t, w.writer, func(c *follow.Config) {
		c.URL = docs.url
		c.IPNS = ""
		c.DNSLink = "swarm.example"
		c.LookupTXT = dnslinkLookup(&target, &dnsFailure)
		c.Routing = w.routing
		c.PubKey = nil
	})
	f.serveHTTP(nil)
	if err := f.pollErr(); err == nil || !strings.Contains(err.Error(), "cannot bootstrap") {
		t.Fatalf("HTTPS bootstrap error = %v", err)
	}
	if _, _, ok, err := follow.ReadDelegation(f.store.KV()); err != nil || ok {
		t.Fatalf("unauthenticated HTTPS created delegation: ok=%t err=%v", ok, err)
	}
}

func TestFreshnessRefusedDNSLinkDocumentDoesNotRotateSigner(t *testing.T) {
	first := newIPNSWriter(t)
	second := newIPNSWriter(t)
	target := first.name()
	var dnsFailure error
	f := newDNSLinkFollower(t, first, &target, &dnsFailure, nil)
	f.connect(second.writer)
	f.serveHTTP(nil)

	first.ingestSlot(100, 1)
	first.publish(t, sign(t, first.key, first.unsigned(time.Now())))
	f.poll()

	second.ingestSlot(120, 2)
	second.publish(t, sign(t, second.key, second.unsigned(time.Now().Add(-time.Hour))))
	first.routing.put(string(second.pub.Name().RoutingKey()), second.record(t))
	target = second.name()
	if err := f.pollErr(); err == nil || !strings.Contains(err.Error(), "before the accepted floor") {
		t.Fatalf("stale rotated document error = %v", err)
	}
	name, signer, ok, err := follow.ReadDelegation(f.store.KV())
	if err != nil || !ok || name != first.name() || !signer.Equal(first.pubkey()) {
		t.Fatalf("refused document rotated delegation to (%q, %x, ok=%t, err=%v)", name, signer, ok, err)
	}
	if _, ok, err := follow.ReadIPNSSeqFor(f.store.KV(), second.name()); err != nil || !ok {
		t.Fatalf("authenticated stale record did not raise its independent sequence floor: ok=%t err=%v", ok, err)
	}
}
