package p2p_test

import (
	"context"
	"errors"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/boxo/ipns"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/routing"

	bmetrics "github.com/blobarchive/bloar/metrics"
	"github.com/blobarchive/bloar/p2p"
)

// TestPublishRecordRoundTrip is spec 8.1's record: what the publisher put is
// what a follower resolves, and it verifies under IPNS validation against the
// name alone -- which is the whole authenticity claim.
func TestPublishRecordRoundTrip(t *testing.T) {
	h := newTestHost(t)
	vs := newMemRouting()
	pub := newTestPublisher(t, h, memBlocks(), vs, memKV(t))

	doc := []byte(`{"v":1,"net":"mainnet","updated_at":"2026-07-16T00:00:00Z","heads":[]}`)
	docCid, seq, err := pub.Publish(t.Context(), doc)
	if err != nil {
		t.Fatalf("publishing: %v", err)
	}

	gotCid, gotSeq, err := p2p.Resolve(t.Context(), vs, pub.Name())
	if err != nil {
		t.Fatalf("resolving: %v", err)
	}
	if gotCid != docCid {
		t.Errorf("resolved %s, want %s", gotCid, docCid)
	}
	if gotSeq != seq {
		t.Errorf("resolved sequence %d, want %d", gotSeq, seq)
	}
}

// TestPublishProvidesDocumentBeforeRecord pins the cold-start ordering. An IPNS
// value is useful to a fresh follower only when the exact document CID it names
// already has a provider record; publishing the name first creates a valid but
// unfetchable bootstrap path.
func TestPublishProvidesDocumentBeforeRecord(t *testing.T) {
	h := newTestHost(t)
	route := newOrderedPublisherRouting()
	pub := newTestPublisher(t, h, memBlocks(), route, memKV(t))

	document := []byte(`{"v":1,"net":"mainnet","updated_at":"2026-07-28T00:00:00Z","heads":[]}`)
	documentCID, _, err := pub.Publish(t.Context(), document)
	if err != nil {
		t.Fatalf("publishing: %v", err)
	}
	events, provided := route.snapshot()
	if got, want := strings.Join(events, ","), "provide,put"; got != want {
		t.Fatalf("publication order = %q, want %q", got, want)
	}
	if !provided.Equals(documentCID) {
		t.Fatalf("provided CID = %s, want published document %s", provided, documentCID)
	}
}

// TestPublishRefusesUnprovidedDocument makes the ordering fail closed. The
// previous IPNS value stays authoritative when the new document cannot be
// advertised; retrying later is safe, while naming an undiscoverable CID is
// not.
func TestPublishRefusesUnprovidedDocument(t *testing.T) {
	h := newTestHost(t)
	route := newOrderedPublisherRouting()
	mx := bmetrics.New()
	pub := newTestPublisher(t, h, memBlocks(), route, memKV(t), func(c *p2p.PublisherConfig) {
		c.Metrics = mx
	})

	firstCID, firstSequence, err := pub.Publish(t.Context(), []byte("available document"))
	if err != nil {
		t.Fatalf("publishing initial document: %v", err)
	}

	route.provideErr = errors.New("provider unavailable")
	if _, _, err := pub.Publish(t.Context(), []byte("unavailable document")); err == nil ||
		!strings.Contains(err.Error(), "before IPNS update") {
		t.Fatalf("Publish error = %v, want pre-IPNS provider failure", err)
	}
	if gotCID, gotSequence, err := p2p.Resolve(t.Context(), route, pub.Name()); err != nil {
		t.Fatalf("Resolve after failed provide: %v", err)
	} else if !gotCID.Equals(firstCID) || gotSequence != firstSequence {
		t.Fatalf("record after failed provide = (%s, %d), want prior (%s, %d)",
			gotCID, gotSequence, firstCID, firstSequence)
	}
	if got := ipnsMetricSample(t, mx, `bloar_ipns_publication_stage_total{outcome="error",stage="provide_document"}`); got != 1 {
		t.Fatalf("provide failure metric = %g, want 1", got)
	}
	if got := ipnsMetricSample(t, mx, `bloar_ipns_publication_stage_total{outcome="error",stage="put_record"}`); got != 0 {
		t.Fatalf("record error metric after pre-record refusal = %g, want 0", got)
	}
	firstSuccess := ipnsMetricSample(t, mx, `bloar_ipns_publication_last_success_timestamp_seconds`)
	if firstSuccess == 0 {
		t.Fatal("initial successful publication did not stamp last-success time")
	}

	route.provideErr = nil
	secondCID, secondSequence, err := pub.Publish(t.Context(), []byte("unavailable document"))
	if err != nil {
		t.Fatalf("retrying document after provider recovery: %v", err)
	}
	if secondCID.Equals(firstCID) {
		t.Fatalf("retry CID = %s, want a changed document", secondCID)
	}
	if secondSequence != firstSequence+1 {
		t.Fatalf("retry sequence = %d, want %d; failed provide must not consume a sequence",
			secondSequence, firstSequence+1)
	}
	if got := ipnsMetricSample(t, mx, `bloar_ipns_publication_stage_total{outcome="ok",stage="provide_document"}`); got != 2 {
		t.Fatalf("successful provide metric = %g, want 2", got)
	}
	if got := ipnsMetricSample(t, mx, `bloar_ipns_publication_stage_total{outcome="ok",stage="put_record"}`); got != 2 {
		t.Fatalf("successful record metric = %g, want 2", got)
	}
	if got := ipnsMetricSample(t, mx, `bloar_ipns_publication_last_success_timestamp_seconds`); got < firstSuccess {
		t.Fatalf("last-success timestamp regressed from %g to %g", firstSuccess, got)
	}
	events, _ := route.snapshot()
	if got, want := strings.Join(events, ","), "provide,put,provide,provide,put"; got != want {
		t.Fatalf("publication events = %q, want %q", got, want)
	}
}

func TestPublishMetricsDistinguishRecordFailure(t *testing.T) {
	h := newTestHost(t)
	route := &flakyRouting{memRouting: newMemRouting()}
	route.failNext(1)
	mx := bmetrics.New()
	pub := newTestPublisher(t, h, memBlocks(), route, memKV(t), func(c *p2p.PublisherConfig) {
		c.Metrics = mx
	})

	document := []byte("document whose first record put fails")
	if _, _, err := pub.Publish(t.Context(), document); err == nil {
		t.Fatal("Publish with a failing record store succeeded")
	}
	if got := ipnsMetricSample(t, mx, `bloar_ipns_publication_stage_total{outcome="ok",stage="provide_document"}`); got != 1 {
		t.Fatalf("successful provide metric = %g, want 1", got)
	}
	if got := ipnsMetricSample(t, mx, `bloar_ipns_publication_stage_total{outcome="error",stage="put_record"}`); got != 1 {
		t.Fatalf("record error metric = %g, want 1", got)
	}
	if got := ipnsMetricSample(t, mx, `bloar_ipns_publication_last_success_timestamp_seconds`); got != 0 {
		t.Fatalf("failed record put stamped last success %g", got)
	}

	if _, _, err := pub.Publish(t.Context(), document); err != nil {
		t.Fatalf("Publish after record-store recovery: %v", err)
	}
	if got := ipnsMetricSample(t, mx, `bloar_ipns_publication_stage_total{outcome="ok",stage="put_record"}`); got != 1 {
		t.Fatalf("successful record metric = %g, want 1", got)
	}
	if got := ipnsMetricSample(t, mx, `bloar_ipns_publication_last_success_timestamp_seconds`); got == 0 {
		t.Fatal("successful transaction did not stamp last success")
	}
}

func ipnsMetricSample(t *testing.T, mx *bmetrics.Metrics, series string) float64 {
	t.Helper()
	recorder := httptest.NewRecorder()
	bmetrics.Handler(mx, nil).ServeHTTP(recorder, httptest.NewRequest("GET", "/metrics", nil))
	for line := range strings.SplitSeq(recorder.Body.String(), "\n") {
		if !strings.HasPrefix(line, series+" ") {
			continue
		}
		value, err := strconv.ParseFloat(strings.TrimSpace(strings.TrimPrefix(line, series+" ")), 64)
		if err != nil {
			t.Fatalf("parsing metric sample %q: %v", line, err)
		}
		return value
	}
	t.Fatalf("metric series %s is absent", series)
	return 0
}

// TestPublishNameIsThePeerID pins the decision in the package comment: the IPNS
// name and the PeerID are one key. A follower that resolves the name and then
// dials the peer in the document's multiaddrs is talking to the signer, and
// nothing has to check that.
func TestPublishNameIsThePeerID(t *testing.T) {
	h := newTestHost(t)
	pub := newTestPublisher(t, h, memBlocks(), newMemRouting(), memKV(t))

	if pub.Name() != ipns.NameFromPeer(h.ID()) {
		t.Errorf("name = %s, want the PeerID's name %s", pub.Name(), ipns.NameFromPeer(h.ID()))
	}
	if !strings.HasPrefix(pub.Name().String(), "k51") {
		t.Errorf("name = %s, want the k51.. base36 libp2p-key form", pub.Name())
	}
}

// TestPublishDocBlockMatchesServedBytes is the byte-identity spec 8.1 asks for:
// the block the record names holds exactly the document the HTTPS channel
// serves. If these ever diverge, the two channels are publishing two documents
// and only one of them is signed by what it claims.
func TestPublishDocBlockMatchesServedBytes(t *testing.T) {
	h := newTestHost(t)
	base := memBlocks()
	docs := newTestDocs(t, base)
	pub := newTestPublisher(t, h, nil, newMemRouting(), memKV(t), withDocs(docs))

	doc := []byte(`{"v":1,"net":"mainnet","updated_at":"2026-07-16T00:00:00Z","heads":[{"name":"all"}]}`)
	docCid, _, err := pub.Publish(t.Context(), doc)
	if err != nil {
		t.Fatalf("publishing: %v", err)
	}

	blk, err := docs.Get(t.Context(), docCid)
	if err != nil {
		t.Fatalf("getting the published document block: %v", err)
	}
	if string(blk.RawData()) != string(doc) {
		t.Errorf("document block holds %q, want the served bytes %q", blk.RawData(), doc)
	}
	// And the CID is the CID of those bytes, not of anything re-rendered.
	if want := rawBlock(t, doc).Cid(); docCid != want {
		t.Errorf("document CID = %s, want the raw block CID of the served bytes %s", docCid, want)
	}
}

// TestPublishSequenceAcrossRestart is the KV persistence claim. A publisher
// that restarted and reused a number would be publishing records every follower
// is required to reject (spec 11.3), which is an archive that has silently
// stopped publishing -- so the number has to survive the process.
func TestPublishSequenceAcrossRestart(t *testing.T) {
	h := newTestHost(t)
	kvPath := filepath.Join(t.TempDir(), "kv")
	vs := newMemRouting()

	kv, closeKV := openKV(t, kvPath)
	pub := newTestPublisher(t, h, memBlocks(), vs, kv)
	_, first, err := pub.Publish(t.Context(), []byte("doc one"))
	if err != nil {
		t.Fatalf("publishing before restart: %v", err)
	}
	closeKV()

	// The restart: same store, a publisher that knows nothing but what is on
	// disk.
	kv2, _ := openKV(t, kvPath)
	pub2 := newTestPublisher(t, h, memBlocks(), vs, kv2)
	_, second, err := pub2.Publish(t.Context(), []byte("doc two"))
	if err != nil {
		t.Fatalf("publishing after restart: %v", err)
	}

	if second <= first {
		t.Errorf("sequence after restart = %d, want it above the %d published before", second, first)
	}
	if _, resolved, err := p2p.Resolve(t.Context(), vs, pub2.Name()); err != nil {
		t.Fatalf("resolving after restart: %v", err)
	} else if resolved != second {
		t.Errorf("resolved sequence %d, want %d", resolved, second)
	}
}

// TestPublishUnchangedDocKeepsSequence: a republish is the same claim said
// again before it expires, so it keeps its number and takes a fresh validity.
// Bumping here would be a regression signal for no reason -- followers compare
// sequences to decide whether anything happened.
func TestPublishUnchangedDocKeepsSequence(t *testing.T) {
	h := newTestHost(t)
	pub := newTestPublisher(t, h, memBlocks(), newMemRouting(), memKV(t))

	doc := []byte("a document that is not changing")
	_, first, err := pub.Publish(t.Context(), doc)
	if err != nil {
		t.Fatalf("publishing: %v", err)
	}
	_, second, err := pub.Publish(t.Context(), doc)
	if err != nil {
		t.Fatalf("republishing: %v", err)
	}
	if first != second {
		t.Errorf("republishing an unchanged document moved the sequence %d -> %d", first, second)
	}

	_, third, err := pub.Publish(t.Context(), []byte("a different document"))
	if err != nil {
		t.Fatalf("publishing a changed document: %v", err)
	}
	if third <= second {
		t.Errorf("a changed document published at sequence %d, want it above %d", third, second)
	}
}

// TestResolveRejectsForgedRecord: the name is the key, so a record signed by
// anyone else does not verify under it. This is what a follower relies on when
// it takes a document off IPNS instead of HTTPS.
func TestResolveRejectsForgedRecord(t *testing.T) {
	victim, attacker := newTestHost(t), newTestHost(t)
	vs := newMemRouting()

	// The attacker publishes, then files its record under the victim's name.
	attackerPub := newTestPublisher(t, attacker, memBlocks(), vs, memKV(t))
	if _, _, err := attackerPub.Publish(t.Context(), []byte("a document the victim never wrote")); err != nil {
		t.Fatalf("publishing as the attacker: %v", err)
	}
	raw, err := vs.GetValue(t.Context(), string(attackerPub.Name().RoutingKey()))
	if err != nil {
		t.Fatalf("reading the attacker's record: %v", err)
	}
	victimName := ipns.NameFromPeer(victim.ID())
	vs.put(string(victimName.RoutingKey()), raw)

	if _, _, err := p2p.Resolve(t.Context(), vs, victimName); err == nil {
		t.Fatal("a record signed by another key verified under the victim's name")
	}
	if _, _, err := p2p.DecodeRecord(raw, victimName); err == nil {
		t.Fatal("DecodeRecord accepted a record signed by another key")
	}
	// Sanity: the same bytes are fine under the name they were signed for, so
	// the rejection above is about the name and not about the record.
	if _, _, err := p2p.DecodeRecord(raw, attackerPub.Name()); err != nil {
		t.Fatalf("the attacker's own record does not verify under its own name: %v", err)
	}
}

// TestDocBlockstoreIsInvisibleToGC is the reason DocBlockstore exists: GC (spec
// 9) sweeps what AllKeysChan enumerates minus what the pins mark, and a
// publication document is under no pin. If it showed up here it would be swept,
// and the live IPNS record would name a block this node had deleted.
func TestDocBlockstoreIsInvisibleToGC(t *testing.T) {
	base := memBlocks()
	own := rawBlock(t, []byte("a block this node stores"))
	putBlock(t, base, own)

	docs := newTestDocs(t, base)
	doc := rawBlock(t, []byte("a publication document"))
	docs.PutDoc(doc)

	// Reads see it...
	if has, err := docs.Has(t.Context(), doc.Cid()); err != nil || !has {
		t.Fatalf("Has(document) = %v, %v; want true, nil", has, err)
	}
	if _, err := docs.Get(t.Context(), doc.Cid()); err != nil {
		t.Fatalf("Get(document): %v", err)
	}

	// ...and the enumeration GC sweeps from does not.
	keys, err := docs.AllKeysChan(t.Context())
	if err != nil {
		t.Fatalf("AllKeysChan: %v", err)
	}
	var got []cid.Cid
	for k := range keys {
		got = append(got, k)
	}
	if len(got) != 1 {
		t.Fatalf("AllKeysChan yielded %d keys, want 1 (only the node's own block)", len(got))
	}
	if string(got[0].Hash()) != string(own.Cid().Hash()) {
		t.Errorf("AllKeysChan yielded %s, want the node's own block %s", got[0], own.Cid())
	}
}

// TestDocBlockstorePassesThrough: with nothing published into it, it is the
// blockstore underneath. That is what lets bloard build one unconditionally and
// branch only on whether there is a publisher.
func TestDocBlockstorePassesThrough(t *testing.T) {
	base := memBlocks()
	docs := newTestDocs(t, base)

	blk := rawBlock(t, []byte("through"))
	if err := docs.Put(t.Context(), blk); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if has, err := base.Has(t.Context(), blk.Cid()); err != nil || !has {
		t.Fatalf("Put did not reach the base blockstore (has=%v, err=%v)", has, err)
	}
	if _, err := docs.Get(t.Context(), blk.Cid()); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if size, err := docs.GetSize(t.Context(), blk.Cid()); err != nil || size != len(blk.RawData()) {
		t.Fatalf("GetSize = %d, %v; want %d, nil", size, err, len(blk.RawData()))
	}

	absent := rawBlock(t, []byte("absent"))
	if _, err := docs.Get(t.Context(), absent.Cid()); err == nil {
		t.Error("Get of an absent block succeeded")
	}
}

// TestPublisherNotifyPublishes covers the hook server.Heads calls: Notify only
// marks, and the loop is what does the work.
func TestPublisherNotifyPublishes(t *testing.T) {
	h := newTestHost(t)
	vs := newMemRouting()
	pub := newTestPublisher(t, h, memBlocks(), vs, memKV(t))

	doc := []byte("a document handed over by the OnDoc hook")
	pub.Notify(doc)
	pub.Start()
	defer func() {
		if err := pub.Close(); err != nil {
			t.Errorf("closing publisher: %v", err)
		}
	}()

	want := rawBlock(t, doc).Cid()
	waitFor(t, "the notified document to be published", func() bool {
		got, _, err := p2p.Resolve(context.Background(), vs, pub.Name())
		return err == nil && got == want
	})
}

// TestPublisherRetriesAfterFailure is the startup case, which is the common
// case: a daemon publishes the moment its heads are open, and at that point its
// DHT has usually not found a peer yet, so the first attempt fails. If a
// failure waited for the republish interval, the name would go unpublished for
// hours every time a node restarted -- which is precisely when a follower most
// wants to hear from it.
func TestPublisherRetriesAfterFailure(t *testing.T) {
	h := newTestHost(t)
	vs := &flakyRouting{memRouting: newMemRouting()}
	vs.failNext(1)

	// A short republish interval, which is also the ceiling on the retry wait:
	// the retry this is testing is a timer, and the test should not sit through
	// the real 15s floor to watch it fire.
	pub := newTestPublisher(t, h, memBlocks(), vs, memKV(t), withRepublish(50*time.Millisecond))
	pub.Start()
	pub.Notify([]byte("a document published by a node that has just started"))
	defer func() {
		if err := pub.Close(); err != nil {
			t.Errorf("closing publisher: %v", err)
		}
	}()

	waitFor(t, "the failed publication to be retried", func() bool {
		_, _, err := p2p.Resolve(context.Background(), vs, pub.Name())
		return err == nil
	})
	if got := vs.attempts(); got < 2 {
		t.Errorf("the publisher made %d attempts, want a retry after the first failed", got)
	}
}

// flakyRouting fails the first n PutValues, like a DHT with no peers yet.
type flakyRouting struct {
	*memRouting
	mu    sync.Mutex
	fail  int
	tries int
}

func (r *flakyRouting) failNext(n int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.fail = n
}

func (r *flakyRouting) attempts() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.tries
}

func (r *flakyRouting) PutValue(ctx context.Context, k string, v []byte, opts ...routing.Option) error {
	r.mu.Lock()
	r.tries++
	failing := r.fail > 0
	if failing {
		r.fail--
	}
	r.mu.Unlock()
	if failing {
		return errors.New("failed to find any peer in table")
	}
	return r.memRouting.PutValue(ctx, k, v, opts...)
}

// newTestPublisher builds a Publisher over the given routing and KV. blocks is
// the base blockstore for the documents; pass nil with withDocs to supply the
// DocBlockstore directly.
func newTestPublisher(t *testing.T, h *p2p.Host, base blockstore.Blockstore, vs interface {
	routing.ValueStore
	p2p.DocumentProvider
}, kv *pebble.DB,
	opts ...func(*p2p.PublisherConfig)) *p2p.Publisher {
	t.Helper()
	cfg := p2p.PublisherConfig{Host: h, Routing: vs, Provider: vs, KV: kv}
	if base != nil {
		cfg.Docs = newTestDocs(t, base)
	}
	for _, o := range opts {
		o(&cfg)
	}
	pub, err := p2p.NewPublisher(cfg)
	if err != nil {
		t.Fatalf("building publisher: %v", err)
	}
	return pub
}

func withDocs(d *p2p.DocBlockstore) func(*p2p.PublisherConfig) {
	return func(c *p2p.PublisherConfig) { c.Docs = d }
}

func withRepublish(d time.Duration) func(*p2p.PublisherConfig) {
	return func(c *p2p.PublisherConfig) { c.Republish = d }
}

// memRouting is a routing.ValueStore that is a map: it makes the record tests
// about the record rather than about a DHT converging. The DHT itself is tested
// against a real one in dht_test.go.
type memRouting struct {
	mu sync.Mutex
	m  map[string][]byte
}

type orderedPublisherRouting struct {
	*memRouting
	mu         sync.Mutex
	events     []string
	provided   cid.Cid
	provideErr error
}

func newOrderedPublisherRouting() *orderedPublisherRouting {
	return &orderedPublisherRouting{memRouting: newMemRouting()}
}

func (r *orderedPublisherRouting) Provide(_ context.Context, c cid.Cid, recursive bool) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, "provide")
	if !recursive {
		return errors.New("publication document provide was not recursive")
	}
	r.provided = c
	return r.provideErr
}

func (r *orderedPublisherRouting) PutValue(ctx context.Context, key string, value []byte, opts ...routing.Option) error {
	r.mu.Lock()
	r.events = append(r.events, "put")
	r.mu.Unlock()
	return r.memRouting.PutValue(ctx, key, value, opts...)
}

func (r *orderedPublisherRouting) snapshot() ([]string, cid.Cid) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.events...), r.provided
}

func newMemRouting() *memRouting { return &memRouting{m: map[string][]byte{}} }

func (r *memRouting) put(k string, v []byte) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.m[k] = v
}

func (r *memRouting) PutValue(_ context.Context, k string, v []byte, _ ...routing.Option) error {
	r.put(k, v)
	return nil
}

func (r *memRouting) Provide(context.Context, cid.Cid, bool) error { return nil }

func (r *memRouting) GetValue(_ context.Context, k string, _ ...routing.Option) ([]byte, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	v, ok := r.m[k]
	if !ok {
		return nil, routing.ErrNotFound
	}
	return v, nil
}

func (r *memRouting) SearchValue(_ context.Context, k string, _ ...routing.Option) (<-chan []byte, error) {
	return nil, errors.New("memRouting: SearchValue is not implemented")
}
