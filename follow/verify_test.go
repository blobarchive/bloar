package follow_test

import (
	"encoding/hex"
	"net/http"
	"strings"
	"testing"
	"time"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"

	"github.com/blobarchive/bloar/follow"
	"github.com/blobarchive/bloar/ingest"
	"github.com/blobarchive/bloar/pinning"
	"github.com/blobarchive/bloar/schema"
	"github.com/blobarchive/bloar/server"
)

// The corruption these tests are about is the one thing content addressing
// cannot catch, and therefore the only thing follow.verify: full adds.
//
// Every block here is genuine. The blobs are valid KZG blobs, each stored under
// the CID of its own bytes; the Segment and the Head encode and hash exactly as
// the writer's own engine would produce them; the document is signed with the
// writer's real key over spec 8's canonical bytes. Bitswap will hand every one
// of them over and verify every multihash on the way.
//
// The lie is one field. A RefEntry says "versioned hash V is at blob C", and it
// is built here with V taken from one blob and C from another. Nothing in the
// DAG contradicts it: C's bytes hash to C, and V is a perfectly good versioned
// hash -- of a different blob. Only recomputing the commitment finds it, which
// is the whole of what full verification is for.

// corrupt is a head whose index binds a versioned hash to the wrong blob.
type corrupt struct {
	root cid.Cid
	slot uint64

	// vh is what the index claims is at the slot; honest is the blob that vh
	// actually belongs to, and served is the blob the index points at.
	vh      schema.VersionedHash
	honest  []byte
	served  []byte
	servedC cid.Cid
}

// plantCorruptHead builds the DAG of a malicious writer directly, block by
// block, and puts it in w's store, where bitswap will serve it.
//
// Directly rather than through the engine because the engine cannot be made to
// do this: apply_refs resolves each versioned hash through the blob catalog
// (spec 5.1), so the binding it writes is the one ingest derived from the bytes.
// A wrong binding is not a state the writer's code can reach, which is the
// point of the test -- the follower must not assume the writer ran that code.
func plantCorruptHead(t *testing.T, w *writer, slot uint64) *corrupt {
	t.Helper()

	honest, served := makeBlob(11), makeBlob(22)
	honestVH, err := ingest.VersionedHash(honest)
	if err != nil {
		t.Fatalf("VersionedHash: %v", err)
	}
	servedC := blobCID(t, served)

	// Both blobs are ingested normally: the corruption is in the index, not in
	// the blocks, and the served blob has to actually be there to be served.
	if _, err := w.ing.PutBlobs(t.Context(), append(append([]byte{}, honest...), served...)); err != nil {
		t.Fatalf("PutBlobs: %v", err)
	}

	// The lie: honest's versioned hash, served's CID.
	seg := &schema.Segment{
		Slot0: slot &^ (1<<testSegBits - 1),
		Rows:  []schema.Row{{Slot: slot, Entries: []schema.RefEntry{{VH: honestVH, Blob: servedC}}}},
	}
	segRaw, segCID, err := schema.EncodeSegment(seg)
	if err != nil {
		t.Fatalf("EncodeSegment: %v", err)
	}
	putRaw(t, w, segRaw, segCID)

	syncedTo := slot
	head := &schema.Head{
		Name:       testHead,
		Net:        testNet,
		OriginSlot: testOrigin,
		SyncedTo:   &syncedTo,
		SegBits:    testSegBits,
		FanoutBits: testFanout,
		Open:       segCID,
	}
	headRaw, root, err := schema.EncodeHead(head)
	if err != nil {
		t.Fatalf("EncodeHead: %v", err)
	}
	putRaw(t, w, headRaw, root)

	return &corrupt{root: root, slot: slot, vh: honestVH, honest: honest, served: served, servedC: servedC}
}

// putRaw writes a block into a node's store under a CID its bytes hash to.
func putRaw(t *testing.T, w *writer, raw []byte, c cid.Cid) {
	t.Helper()
	blk, err := blocks.NewBlockWithCid(raw, c)
	if err != nil {
		t.Fatalf("framing block %s: %v", c, err)
	}
	if err := w.store.Blocks().Put(t.Context(), blk); err != nil {
		t.Fatalf("putting block %s: %v", c, err)
	}
}

// publishCorrupt serves a signed document naming the planted root. The
// signature is real: a malicious writer signs its lies, which is exactly why a
// signature is not integrity.
func publishCorrupt(t *testing.T, w *writer, docs *docServer, c *corrupt) {
	t.Helper()
	syncedTo := c.slot
	docs.set(sign(t, w.key, server.Unsigned{
		V:          server.DocVersion,
		Net:        testNet,
		UpdatedAt:  time.Now().UTC().Format(time.RFC3339),
		Multiaddrs: w.host.AnnounceAddrs(),
		Heads: []server.HeadEntry{{
			Name:       testHead,
			Root:       c.root.String(),
			OriginSlot: testOrigin,
			SyncedTo:   &syncedTo,
			SegBits:    testSegBits,
			FanoutBits: testFanout,
		}},
	}))
}

// TestVerifyFullQuarantinesACorruptHead is spec 11.4's teeth: under full
// verification, a blob whose commitment does not match the versioned hash the
// index bound it to takes the whole head out of service. Not the block -- the
// head. A writer that will assert one wrong binding has forfeited the thing its
// signature was for.
func TestVerifyFullQuarantinesACorruptHead(t *testing.T) {
	w := newWriter(t)
	docs := newDocServer(t)

	var lines logs
	f := newFollower(t, w, func(c *follow.Config) {
		c.URL = docs.url
		c.Verify = follow.VerifyFull
		c.Logger = capturingLogger(t, &lines)
	})
	f.serveHTTP(nil)

	c := plantCorruptHead(t, w, 100)
	publishCorrupt(t, w, docs, c)

	// Keep an online collection active across adoption. Its publication-
	// protection walk fetches the retained closure without interpreting the
	// Segment's KZG bindings. The ordinary full-verification pass must still
	// verify every entry even though that earlier pass made the corrupt blob
	// local; treating "already local" as "already verified" would adopt it.
	epoch, err := f.store.Epochs().Begin()
	if err != nil {
		t.Fatalf("begin collection epoch: %v", err)
	}
	t.Cleanup(func() { epoch.End() })

	// The fetch pass walks the segment, fetches the blob it names, recomputes
	// the commitment, and finds the binding false. Spec 11.4 has that quarantine
	// the head, so the poll reports it.
	err = f.pollErr()
	if err == nil {
		t.Fatal("a follower with verify: full adopted and synced a head whose index binds a vh to the wrong blob")
	}
	if !strings.Contains(err.Error(), "quarantined") {
		t.Errorf("err = %v, want it to say the head is quarantined", err)
	}

	// Stopped serving it entirely (spec 11.4). 503 rather than 404: a 404 from
	// this API is a statement about blobs, and this is a statement about the
	// node.
	status, _, header := f.blobsAt(100, c.vh)
	if status != http.StatusServiceUnavailable {
		t.Errorf("GET a quarantined head: status = %d, want 503", status)
	}
	if got := header.Get("Cache-Control"); got != "no-store" {
		t.Errorf("Cache-Control = %q, want no-store", got)
	}
	// Every endpoint of the head, not just the one that found it.
	for _, path := range []string{"/eth/v1/beacon/genesis", "/eth/v1/config/spec"} {
		if status := f.get(f.url + "/" + testHead + path); status != http.StatusServiceUnavailable {
			t.Errorf("GET %s on a quarantined head: status = %d, want 503", path, status)
		}
	}
	// And it is out of the document, so a follower of this follower does not
	// take it on trust from us.
	if names := f.heads.Names(); len(names) != 0 {
		t.Errorf("the follower still publishes %v, want a quarantined head to be out of the document", names)
	}

	// Loud enough to act on: the head, the mismatch, both hashes, and the key
	// the operator has to go and look at.
	for _, want := range []string{"QUARANTINED", testHead, "0x" + hex.EncodeToString(c.vh[:]), c.servedC.String()} {
		if !lines.has(want) {
			t.Errorf("the quarantine log does not mention %q; an operator has to be able to act on this", want)
		}
	}

	// A fresh, correctly-signed document does not clear it: the signature is
	// what is in question.
	publishCorrupt(t, w, docs, c)
	if err := f.pollErr(); err == nil || !strings.Contains(err.Error(), "quarantine") {
		t.Errorf("polling after a quarantine: err = %v, want the head to stay quarantined", err)
	}
	if status, _, _ := f.blobsAt(100, c.vh); status != http.StatusServiceUnavailable {
		t.Errorf("GET after a re-publication: status = %d, want the quarantine to hold", status)
	}
}

// TestVerifyFullQuarantinesOnTheReadPath: a blob outside the pin window is
// never walked by the fetch pass, so the read that fetches it is where its
// binding is checked. This is the case that makes verification a property of the
// fetch path rather than of the sync loop -- a window follower would otherwise
// serve exactly the blobs nothing ever verified.
func TestVerifyFullQuarantinesOnTheReadPath(t *testing.T) {
	w := newWriter(t)
	docs := newDocServer(t)

	f := newFollower(t, w, func(c *follow.Config) {
		c.URL = docs.url
		c.Verify = follow.VerifyFull
		// none retains the index and no blobs (spec 9), so nothing about this
		// head's blobs is fetched until somebody asks for one.
		c.Heads = map[string]pinning.Policy{testHead: pinning.None()}
	})
	f.serveHTTP(nil)

	c := plantCorruptHead(t, w, 100)
	publishCorrupt(t, w, docs, c)
	f.poll() // adopts and syncs the index: no blob is touched, so no complaint.

	if _, ok := f.heads.Get(testHead); !ok {
		t.Fatal("the head was not adopted; this test is about the read path finding the corruption")
	}
	if status, _, _ := f.blobsAt(100, c.vh); status != http.StatusServiceUnavailable {
		t.Errorf("GET a corrupt blob under verify: full: status = %d, want 503 and no blob", status)
	}
	if _, ok := f.heads.Quarantined(testHead); !ok {
		t.Error("a read that found a false vh binding did not quarantine the head")
	}
}

// TestVerifyCIDServesTheCorruption documents exactly what full verification
// buys, by showing what the default does not.
//
// The same DAG, the same document, a follower that differs in one config value:
// it serves the corrupt blob with a 200. That is not a bug. Spec 11.4's cid mode
// promises multihash verification, and it delivers it -- these bytes are the
// bytes the writer put at that CID. What it does not promise is that the
// writer's index is honest about which blob is which, and nitro's own KZG check
// is what catches this in the end (spec 11.4's last paragraph, and 13.1's
// TestNitroVerifiesProofs).
func TestVerifyCIDServesTheCorruption(t *testing.T) {
	w := newWriter(t)
	docs := newDocServer(t)

	f := newFollower(t, w, func(c *follow.Config) {
		c.URL = docs.url
		c.Verify = follow.VerifyCID // the default
	})
	f.serveHTTP(nil)

	c := plantCorruptHead(t, w, 100)
	publishCorrupt(t, w, docs, c)
	f.poll()

	status, data, _ := f.blobsAt(100, c.vh)
	if status != http.StatusOK {
		t.Fatalf("GET under verify: cid: status = %d, want 200 -- cid mode does not check vh bindings", status)
	}
	if data[0] != "0x"+hex.EncodeToString(c.served) {
		t.Fatal("verify: cid did not serve the blob the index pointed at")
	}
	if data[0] == "0x"+hex.EncodeToString(c.honest) {
		t.Fatal("the fixture is not corrupt: the index pointed at the right blob after all")
	}
	if _, ok := f.heads.Quarantined(testHead); ok {
		t.Error("verify: cid quarantined a head; only full checks vh bindings")
	}
}

// TestVerifyFullAcceptsAnHonestHead: the check is not a tripwire that fires on
// anything unusual. A real archive, replicated under full verification, syncs and
// serves exactly as it does under cid -- at the cost of a commitment per blob.
func TestVerifyFullAcceptsAnHonestHead(t *testing.T) {
	w := newWriter(t)
	blobs, vhs := w.ingestSlot(100, 1, 2)

	f := newFollower(t, w, func(c *follow.Config) { c.Verify = follow.VerifyFull })
	f.serveHTTP(nil)
	f.poll()

	status, data, _ := f.blobsAt(100, vhs[0], vhs[1])
	if status != http.StatusOK {
		t.Fatalf("GET an honest head under verify: full: status = %d, want 200", status)
	}
	for i, want := range blobs {
		if data[i] != "0x"+hex.EncodeToString(want) {
			t.Errorf("blob %d is not the bytes the writer ingested", i)
		}
	}
	if _, ok := f.heads.Quarantined(testHead); ok {
		t.Error("verify: full quarantined an honest head")
	}
}
