package follow_test

// follow-up regression for the safety boundary: quarantine withdraws readiness. A head is
// adopted and reports ready, then a read finds a false versioned-hash binding and
// quarantines it (spec 11.4) -- it now answers 503 to every read, so its readiness
// must withdraw and the load balancer stop routing to it. reportReady's earlier
// "only ever true" contract was wrong: a head CAN regress, on quarantine.

import (
	"net/http"
	"testing"

	"github.com/blobarchive/bloar/follow"
	"github.com/blobarchive/bloar/pinning"
)

func TestQuarantineWithdrawsReadiness(t *testing.T) {
	w := newWriter(t)
	docs := newDocServer(t)

	rr := newReadyRecorder()
	f := newFollower(t, w, func(c *follow.Config) {
		c.URL = docs.url
		c.Verify = follow.VerifyFull
		// none retains the index and no blobs (spec 9), so the head adopts and serves
		// on the poll, and nothing about its blobs is verified until a read asks.
		c.Heads = map[string]pinning.Policy{testHead: pinning.None()}
		c.Ready = rr.hook()
	})
	f.serveHTTP(nil)

	c := plantCorruptHead(t, w, 100)
	publishCorrupt(t, w, docs, c)
	f.poll() // adopts the index; no blob touched, so the head is served and ready

	if _, ok := f.heads.Get(testHead); !ok {
		t.Fatal("the head was not adopted; this test is about a read quarantining an already-served head")
	}
	if !rr.isReady(testHead) {
		t.Fatal("the head did not report ready after adoption")
	}

	// A read fetches the corrupt blob, finds the false vh binding, and quarantines
	// the head (spec 11.4). It now 503s every read, so readiness must withdraw.
	if status, _, _ := f.blobsAt(100, c.vh); status != http.StatusServiceUnavailable {
		t.Fatalf("GET a corrupt blob under verify: full: status = %d, want 503", status)
	}
	if _, ok := f.heads.Quarantined(testHead); !ok {
		t.Fatal("the read did not quarantine the head")
	}
	if rr.isReady(testHead) {
		t.Fatal("a quarantined head stayed ready; readiness must withdraw on quarantine")
	}
}

// TestQuarantineNonResurrection is the safety boundary follow-up's
// Resume/Poll half on the real Config.Ready hook: once a head is quarantined,
// neither a further Poll nor a Resume (both attempted) resurrects it -- readiness
// stays withdrawn and the API stays 503 for this process's lifetime.
func TestQuarantineNonResurrection(t *testing.T) {
	w := newWriter(t)
	docs := newDocServer(t)

	rr := newReadyRecorder()
	f := newFollower(t, w, func(c *follow.Config) {
		c.URL = docs.url
		c.Verify = follow.VerifyFull
		c.Heads = map[string]pinning.Policy{testHead: pinning.None()}
		c.Ready = rr.hook()
	})
	f.serveHTTP(nil)

	c := plantCorruptHead(t, w, 100)
	publishCorrupt(t, w, docs, c)
	f.poll() // adopt: ready
	if !rr.isReady(testHead) {
		t.Fatal("the head did not report ready after adoption")
	}

	// Quarantine on the corrupt read.
	if status, _, _ := f.blobsAt(100, c.vh); status != http.StatusServiceUnavailable {
		t.Fatalf("the corrupt read returned %d, want 503", status)
	}
	if rr.isReady(testHead) {
		t.Fatal("the head stayed ready after quarantine")
	}

	assertStillQuarantined := func(after string) {
		t.Helper()
		if rr.isReady(testHead) {
			t.Fatalf("%s resurrected a quarantined head's readiness", after)
		}
		if status, _, _ := f.blobsAt(100, c.vh); status != http.StatusServiceUnavailable {
			t.Fatalf("%s resurrected a quarantined head's API: status = %d, want 503", after, status)
		}
		if _, ok := f.heads.Quarantined(testHead); !ok {
			t.Fatalf("the head is no longer quarantined after %s", after)
		}
	}

	// A further Poll must not resurrect it.
	_ = f.pollErr()
	assertStillQuarantined("a further poll")

	// A Resume must not resurrect it either.
	_ = f.f.Resume(t.Context())
	assertStillQuarantined("a resume")
}
