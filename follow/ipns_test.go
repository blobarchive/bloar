package follow_test

import (
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"errors"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ipfs/boxo/ipns"
	"github.com/ipfs/boxo/path"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/routing"
	mh "github.com/multiformats/go-multihash"

	"github.com/blobarchive/bloar/follow"
	"github.com/blobarchive/bloar/p2p"
)

// The IPNS channel of spec 8.1, from the follower's end. The publisher is the
// real p2p.Publisher, signing with the writer's real libp2p identity and storing
// the document as the raw block a follower then fetches over bitswap. What is a
// map here is the DHT: these tests are about which document a follower adopts,
// and a real DHT converging would only make them slow. p2p's own tests cover the
// DHT.

// ipnsWriter is a writer with the IPNS channel switched on.
type ipnsWriter struct {
	*writer
	pub     *p2p.Publisher
	routing *memRouting
}

func newIPNSWriter(t *testing.T) *ipnsWriter {
	t.Helper()
	w := &ipnsWriter{writer: newWriter(t), routing: newMemRouting()}

	pub, err := p2p.NewPublisher(p2p.PublisherConfig{
		Host:     w.host,
		Docs:     w.docs,
		Routing:  w.routing,
		Provider: w.routing,
		KV:       w.store.KV(),
		Logger:   testLogger(t),
	})
	if err != nil {
		t.Fatalf("p2p.NewPublisher: %v", err)
	}
	w.pub = pub
	return w
}

// name is what a follower configures as follow.ipns.
func (w *ipnsWriter) name() string { return w.pub.Name().String() }

// publish stores the document as a block and puts a record naming it, which is
// what a writer's publisher does on every root swap.
func (w *ipnsWriter) publish(t *testing.T, body []byte) (cid.Cid, uint64) {
	t.Helper()
	c, seq, err := w.pub.Publish(t.Context(), body)
	if err != nil {
		t.Fatalf("Publisher.Publish: %v", err)
	}
	return c, seq
}

// record is the raw IPNS record currently in the routing table: what a replay
// attack keeps a copy of.
func (w *ipnsWriter) record(t *testing.T) []byte {
	t.Helper()
	raw, err := w.routing.GetValue(t.Context(), string(w.pub.Name().RoutingKey()))
	if err != nil {
		t.Fatalf("reading the record: %v", err)
	}
	return raw
}

// replay puts an old record back, which is the whole attack: it is signed, it is
// inside its lifetime, and it is stale.
func (w *ipnsWriter) replay(raw []byte) {
	w.routing.put(string(w.pub.Name().RoutingKey()), raw)
}

// forge signs a record naming c with sequence seq using the writer's own libp2p
// peer key, and puts it in the routing table. This models the attacker behind
// the authenticated-IPNS floor hardening:
// a record is signed by the transport key, not the document-signing key (spec
// 11.5), so anyone holding the peer key can mint a valid record for any
// sequence and any target -- including a garbage or unauthentic document.
func (w *ipnsWriter) forge(t *testing.T, c cid.Cid, seq uint64) {
	t.Helper()
	priv := w.host.Libp2p().Peerstore().PrivKey(w.host.ID())
	if priv == nil {
		t.Fatal("the writer's host has no private key in its peerstore")
	}
	rec, err := ipns.NewRecord(priv, path.FromCid(c), seq, time.Now().Add(time.Hour), ipns.DefaultRecordTTL)
	if err != nil {
		t.Fatalf("ipns.NewRecord: %v", err)
	}
	raw, err := ipns.MarshalRecord(rec)
	if err != nil {
		t.Fatalf("ipns.MarshalRecord: %v", err)
	}
	w.routing.put(string(w.pub.Name().RoutingKey()), raw)
}

// rawCID is the CID of a raw block with these bytes, computed but never stored:
// a well-formed name for a document no one holds.
func rawCID(t *testing.T, data string) cid.Cid {
	t.Helper()
	h, err := mh.Sum([]byte(data), mh.SHA2_256, -1)
	if err != nil {
		t.Fatalf("mh.Sum: %v", err)
	}
	return cid.NewCidV1(cid.Raw, h)
}

// ipnsFollower follows over the IPNS channel alone.
func ipnsFollower(t *testing.T, w *ipnsWriter, opts ...func(*follow.Config)) *follower {
	t.Helper()
	all := append([]func(*follow.Config){func(c *follow.Config) {
		c.URL = "" // the IPNS channel and nothing else.
		c.IPNS = w.name()
		c.Routing = w.routing
	}}, opts...)
	return newFollower(t, w.writer, all...)
}

// TestIPNSChannel: a follower given a name and a key -- no URL at all -- resolves
// the record, fetches the document block it names over bitswap, and serves the
// head. Spec 8.1's channel, end to end.
func TestIPNSChannel(t *testing.T) {
	w := newIPNSWriter(t)
	blobs, _ := w.ingestSlot(100, 1)

	f := ipnsFollower(t, w)
	f.serveHTTP(nil)
	w.publish(t, w.heads.Doc())
	f.poll()

	status, data, _ := f.blobsAt(100)
	if status != http.StatusOK {
		t.Fatalf("GET slot 100 from an IPNS-only follower: status = %d, want 200", status)
	}
	if data[0] != "0x"+hex.EncodeToString(blobs[0]) {
		t.Error("slot 100 is not the bytes the writer ingested")
	}
}

// TestNoRegressionIPNSSequence is spec 11.3's third rule, on its own: a record
// whose sequence is below one already accepted is refused before the document it
// names is even fetched.
//
// This is spec 8.1's withheld-update attack in the form the sequence exists to
// stop: the record is authentic, correctly signed by the archive's own key, and
// still valid. It is simply an old one, put back.
func TestNoRegressionIPNSSequence(t *testing.T) {
	w := newIPNSWriter(t)
	f := ipnsFollower(t, w)
	f.serveHTTP(nil)

	w.ingestSlot(100, 1)
	w.publish(t, w.heads.Doc())
	old := w.record(t)
	f.poll()

	w.ingestSlot(120, 2)
	_, seq := w.publish(t, w.heads.Doc())
	if seq < 2 {
		t.Fatalf("the second publication has sequence %d; the test needs it to have advanced", seq)
	}
	f.poll()
	if got := followerSyncedTo(t, f); got != 120 {
		t.Fatalf("the follower adopted synced_to %d, want 120", got)
	}

	// The replay.
	w.replay(old)
	err := f.pollErr()
	if err == nil {
		t.Fatal("the follower accepted a replayed IPNS record")
	}
	if !strings.Contains(err.Error(), "below the accepted floor") {
		t.Errorf("err = %v, want it to name the sequence floor", err)
	}
	if got := followerSyncedTo(t, f); got != 120 {
		t.Errorf("the follower's synced_to = %d after a replay, want it to stay at 120", got)
	}
	if status, _, _ := f.blobsAt(120); status != http.StatusOK {
		t.Errorf("GET slot 120 after a replay: status = %d, want the follower to still serve it", status)
	}
}

// TestIPNSFloorRisesOnlyAfterAuth pins the authenticated-IPNS floor: the
// sequence floor must not rise
// on the strength of a record's transport signature alone. A record is signed
// by the libp2p peer key, which is not the document-signing key (spec 11.5), so
// anyone holding the peer key can mint a valid record naming any block with any
// sequence. If a follower raised its floor from such a record before checking
// the document it named, one high-numbered record naming garbage would pin the
// floor above every legitimate record forever -- a permanent DoS from a
// transport-key compromise that is meant to have no effect past its own window.
func TestIPNSFloorRisesOnlyAfterAuth(t *testing.T) {
	w := newIPNSWriter(t)
	f := ipnsFollower(t, w)
	f.serveHTTP(nil)

	w.ingestSlot(100, 1)

	// The attack: a document signed by a key the follower does not follow,
	// stored as a real block, then named by a record with an enormous sequence.
	// The record verifies against the peer key; the document does not verify
	// against the followed key.
	_, wrongKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("generating an impostor key: %v", err)
	}
	badCID, _ := w.publish(t, sign(t, wrongKey, w.unsigned(time.Now())))
	const hugeSeq uint64 = 1 << 40
	w.forge(t, badCID, hugeSeq)

	if err := f.pollErr(); err == nil {
		t.Fatal("the follower adopted a document signed by a key it does not follow")
	}

	// The floor must be untouched. A legitimate record with a small sequence,
	// naming the archive's own document, is still accepted -- if the impostor's
	// 2^40 had been persisted as the floor, this record (sequence 2) would be
	// refused forever as a replay of something below it.
	w.publish(t, w.heads.Doc())
	if err := f.pollErr(); err != nil {
		t.Fatalf("the follower refused a legitimate record after an impostor named a bad document: %v", err)
	}
	if got := followerSyncedTo(t, f); got != 100 {
		t.Fatalf("the follower adopted synced_to %d, want 100", got)
	}
	if status, _, _ := f.blobsAt(100); status != http.StatusOK {
		t.Errorf("GET slot 100: status = %d, want the follower to serve what the legitimate record named", status)
	}
}

// TestIPNSReplayRefusedBeforeFetch is the other half of the authenticated-IPNS
// floor hardening: the
// sequence *comparison* stays where it was, before the document is fetched. A
// record whose sequence is below the floor is refused on the number alone, with
// no bitswap round trip for the block it names -- so a below-floor record naming
// a block no one holds still fails on the sequence, not on a fetch.
func TestIPNSReplayRefusedBeforeFetch(t *testing.T) {
	w := newIPNSWriter(t)
	f := ipnsFollower(t, w)
	f.serveHTTP(nil)

	// A floor above 1.
	w.ingestSlot(100, 1)
	w.publish(t, w.heads.Doc())
	f.poll()
	w.ingestSlot(120, 2)
	_, seq := w.publish(t, w.heads.Doc())
	if seq < 2 {
		t.Fatalf("the second publication has sequence %d; the test needs it above 1", seq)
	}
	f.poll()

	// A record below the floor naming a block that was never published. Were the
	// document fetched before the sequence was compared, this would fail with a
	// fetch error; refused on the sequence, it never reaches the fetch.
	w.forge(t, rawCID(t, "a forged document no one holds"), 1)
	err := f.pollErr()
	if err == nil {
		t.Fatal("the follower accepted a replayed IPNS record")
	}
	if !strings.Contains(err.Error(), "below the accepted floor") {
		t.Errorf("err = %v, want the sequence floor, which is what proves the block was never fetched", err)
	}
}

// TestIPNSEqualSequenceAccepted guards the comparison against tightening from <
// to <=. A writer republishes an unchanged document under the same sequence on
// every republish interval (p2p.Publisher keeps the number when the value has
// not changed), so a follower that refused an equal sequence would reject every
// republish and, on a channel with no fresher document, resolve nothing at all.
func TestIPNSEqualSequenceAccepted(t *testing.T) {
	w := newIPNSWriter(t)
	f := ipnsFollower(t, w)
	f.serveHTTP(nil)

	w.ingestSlot(100, 1)
	_, seq := w.publish(t, w.heads.Doc())
	f.poll()

	// The republish: the same document, hence the same sequence.
	_, again := w.publish(t, w.heads.Doc())
	if again != seq {
		t.Fatalf("republishing an unchanged document changed the sequence from %d to %d", seq, again)
	}
	if err := f.pollErr(); err != nil {
		t.Fatalf("the follower refused a republished record at the same sequence %d: %v", seq, err)
	}
	if got := followerSyncedTo(t, f); got != 100 {
		t.Errorf("the follower's synced_to = %d after a same-sequence republish, want it to stay at 100", got)
	}
}

// TestChannelMergeStaleHTTPS: both channels configured, HTTPS serving an old
// document, IPNS naming a fresh one. Spec 8.1: the freshest document that
// verifies wins.
func TestChannelMergeStaleHTTPS(t *testing.T) {
	w := newIPNSWriter(t)
	docs := newDocServer(t)
	f := newFollower(t, w.writer, func(c *follow.Config) {
		c.URL = docs.url
		c.IPNS = w.name()
		c.Routing = w.routing
	})
	f.serveHTTP(nil)

	// The stale HTTPS document: a real one, from before the second batch.
	w.ingestSlot(100, 1)
	stale := sign(t, w.key, w.unsigned(time.Now().Add(-time.Hour)))
	docs.set(stale)

	// The fresh IPNS document.
	w.ingestSlot(120, 2)
	w.publish(t, sign(t, w.key, w.unsigned(time.Now())))

	f.poll()
	if got := followerSyncedTo(t, f); got != 120 {
		t.Errorf("the follower adopted synced_to %d from a stale HTTPS document, want 120 from the fresher IPNS one", got)
	}
	if status, _, _ := f.blobsAt(120); status != http.StatusOK {
		t.Errorf("GET slot 120: status = %d, want the follower to serve what the fresher channel named", status)
	}
}

// TestChannelMergeStaleIPNS: the same the other way round, which is the case
// spec 8.1 warns about -- IPNS provides authenticity, not freshness, and a
// record inside its lifetime can name yesterday's document indefinitely.
func TestChannelMergeStaleIPNS(t *testing.T) {
	w := newIPNSWriter(t)
	docs := newDocServer(t)
	f := newFollower(t, w.writer, func(c *follow.Config) {
		c.URL = docs.url
		c.IPNS = w.name()
		c.Routing = w.routing
	})
	f.serveHTTP(nil)

	// The stale IPNS document, published and then left behind.
	w.ingestSlot(100, 1)
	w.publish(t, sign(t, w.key, w.unsigned(time.Now().Add(-time.Hour))))

	// The fresh HTTPS document.
	w.ingestSlot(120, 2)
	docs.publish(t, w.writer, time.Now())

	f.poll()
	if got := followerSyncedTo(t, f); got != 120 {
		t.Errorf("the follower adopted synced_to %d from a stale IPNS record, want 120 from the fresher HTTPS one", got)
	}
	if status, _, _ := f.blobsAt(120); status != http.StatusOK {
		t.Errorf("GET slot 120: status = %d, want the follower to serve what the fresher channel named", status)
	}
}

// TestChannelMergeSurvivesOneChannel: with both configured, one channel failing
// entirely is not the follower failing. That redundancy is the reason to
// configure both.
func TestChannelMergeSurvivesOneChannel(t *testing.T) {
	w := newIPNSWriter(t)
	docs := newDocServer(t)
	f := newFollower(t, w.writer, func(c *follow.Config) {
		c.URL = docs.url
		c.IPNS = w.name()
		c.Routing = w.routing
	})
	f.serveHTTP(nil)

	w.ingestSlot(100, 1)
	docs.status(http.StatusBadGateway) // the writer's web server is down.
	w.publish(t, w.heads.Doc())

	if err := f.pollErr(); err != nil {
		t.Fatalf("a poll with one channel down: %v", err)
	}
	if got := followerSyncedTo(t, f); got != 100 {
		t.Errorf("the follower adopted synced_to %d, want 100 over the channel that was up", got)
	}

	// And the other way: IPNS resolving to nothing, HTTPS serving.
	w.ingestSlot(120, 2)
	docs.status(0)
	docs.publish(t, w.writer, time.Now())
	w.routing.put(string(w.pub.Name().RoutingKey()), []byte("not a record"))

	if err := f.pollErr(); err != nil {
		t.Fatalf("a poll with the other channel down: %v", err)
	}
	if got := followerSyncedTo(t, f); got != 120 {
		t.Errorf("the follower adopted synced_to %d, want 120 over the channel that was up", got)
	}
}

// TestBothChannelsDownIsAnError: a follower that cannot resolve anything says
// so. It goes on serving what it has -- the heads are adopted, the blocks are
// local -- but a node whose every channel is failing is not a healthy node, and
// nothing else is going to notice.
func TestBothChannelsDownIsAnError(t *testing.T) {
	w := newIPNSWriter(t)
	docs := newDocServer(t)
	f := newFollower(t, w.writer, func(c *follow.Config) {
		c.URL = docs.url
		c.IPNS = w.name()
		c.Routing = w.routing
	})
	f.serveHTTP(nil)

	w.ingestSlot(100, 1)
	w.publish(t, w.heads.Doc())
	f.poll()

	docs.status(http.StatusBadGateway)
	w.routing.put(string(w.pub.Name().RoutingKey()), []byte("not a record"))
	if err := f.pollErr(); err == nil {
		t.Error("a poll with every channel down reported success")
	}
	if status, _, _ := f.blobsAt(100); status != http.StatusOK {
		t.Errorf("GET slot 100 with every channel down: status = %d, want the follower to still serve it", status)
	}
}

// A cold public-DHT resolution may consume most of the publication-name
// budget. The named block must still get a fresh follow.fetch_timeout rather
// than inheriting what remains of that lookup deadline.
func TestIPNSDocumentFetchGetsIndependentTimeout(t *testing.T) {
	w := newIPNSWriter(t)
	w.ingestSlot(100, 1)
	w.publish(t, w.heads.Doc())

	const fetchTimeout = 3 * time.Minute
	f := ipnsFollower(t, w, func(c *follow.Config) {
		c.FetchTimeout = fetchTimeout
		c.DocumentBlock = func(ctx context.Context, id cid.Cid) (blocks.Block, error) {
			deadline, ok := ctx.Deadline()
			if !ok {
				t.Fatal("document fetch context has no deadline")
			}
			if remaining := time.Until(deadline); remaining < fetchTimeout-time.Second {
				t.Fatalf("document fetch inherited publication-resolution deadline: remaining %s, want about %s", remaining, fetchTimeout)
			}
			return w.docs.Get(ctx, id)
		}
	})
	f.poll()
}

// memRouting is a routing.ValueStore that is a map. See the file comment.
type memRouting struct {
	mu sync.Mutex
	m  map[string][]byte
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

var _ routing.ValueStore = (*memRouting)(nil)
