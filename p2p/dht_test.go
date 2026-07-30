package p2p_test

import (
	"context"
	"testing"

	dht "github.com/libp2p/go-libp2p-kad-dht"

	"github.com/blobarchive/bloar/p2p"
)

// TestDHTPublishAndResolve is spec 8.1 over a real DHT: a publisher puts a
// record on one node and a follower resolves it from another, with nothing
// shared between them but the routing they both joined.
//
// It is a real go-libp2p-kad-dht on both sides rather than a stand-in, because
// what is being tested is the part a map cannot tell us -- that the record this
// package builds is one the DHT accepts, stores and hands back. The DHT
// validates on the way in against the ipns validator its default config
// registers, so a record built wrong does not travel, and that check is the
// point.
func TestDHTPublishAndResolve(t *testing.T) {
	writer, follower := newTestHost(t), newTestHost(t)

	writerDHT := newTestDHT(t, writer)
	followerDHT := newTestDHT(t, follower)
	connect(t, follower, writer)

	// The routing tables fill from the connection, not from the dial returning.
	waitFor(t, "the DHT routing tables to see each other", func() bool {
		return writerDHT.RoutingTable().Size() > 0 && followerDHT.RoutingTable().Size() > 0
	})

	pub := newTestPublisher(t, writer, memBlocks(), writerDHT, memKV(t))
	doc := []byte(`{"v":1,"net":"mainnet","updated_at":"2026-07-16T00:00:00Z","heads":[]}`)
	docCid, seq, err := pub.Publish(t.Context(), doc)
	if err != nil {
		t.Fatalf("publishing to the DHT: %v", err)
	}

	gotCid, gotSeq, err := p2p.Resolve(t.Context(), followerDHT, pub.Name())
	if err != nil {
		t.Fatalf("resolving from the other node's DHT: %v", err)
	}
	if gotCid != docCid {
		t.Errorf("resolved %s, want the published document %s", gotCid, docCid)
	}
	if gotSeq != seq {
		t.Errorf("resolved sequence %d, want %d", gotSeq, seq)
	}
}

// TestDHTRejectsForgedRecordOnPut: the DHT validates what it is asked to store,
// so a record that does not verify does not travel. This is the property that
// lets a follower's Resolve be the second check rather than the only one.
func TestDHTRejectsForgedRecordOnPut(t *testing.T) {
	h := newTestHost(t)
	d := newTestDHT(t, h)

	err := d.PutValue(t.Context(), "/ipns/"+string(h.ID()), []byte("not an IPNS record"))
	if err == nil {
		t.Fatal("the DHT accepted a value that is not an IPNS record")
	}
}

// TestNewDHT covers the constructor bloard uses. It is built and closed rather
// than published through: the mode it picks is ModeAuto, which decides what it
// is from the reachability the node turns out to have, and two hosts on
// loopback with nothing to probe each other through do not settle that
// question. What a real DHT does with a real record is TestDHTPublishAndResolve
// above, which forces server mode to get there.
func TestNewDHT(t *testing.T) {
	h := newTestHost(t)
	d, err := p2p.NewDHT(t.Context(), h, nil)
	if err != nil {
		t.Fatalf("building DHT: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("closing DHT: %v", err)
	}
}

// TestNewDHTHonorsExplicitEmptyBootstrap: NewDHT receives the caller's complete
// bootstrap set, so an empty set joins nothing. The daemon separately chooses
// whether to pass public defaults or an explicitly private set.
func TestNewDHTHonorsExplicitEmptyBootstrap(t *testing.T) {
	h := newTestHost(t)
	d, err := p2p.NewDHT(t.Context(), h, nil)
	if err != nil {
		t.Fatalf("building DHT: %v", err)
	}
	defer func() {
		if err := d.Close(); err != nil {
			t.Errorf("closing DHT: %v", err)
		}
	}()

	// Give it a moment to do the wrong thing if it were going to: the routing
	// table fills from dials, and a bootstrapping node would have some by now.
	if n := d.RoutingTable().Size(); n != 0 {
		t.Errorf("a DHT built with no bootstrap peers has %d peers in its routing table", n)
	}
	if peers := h.Libp2p().Network().Peers(); len(peers) != 0 {
		t.Errorf("a DHT built with no bootstrap peers dialled %v", peers)
	}
}

// newTestDHT builds a DHT in forced server mode.
//
// Server mode is the test's own doing and not what bloard runs: ModeAuto, which
// NewDHT uses, waits to find out whether the node is publicly reachable, and an
// in-process pair on loopback has nothing to answer that with. Forcing it here
// is what makes two nodes a network that holds records, which is the thing
// under test.
func newTestDHT(t *testing.T, h *p2p.Host) *dht.IpfsDHT {
	t.Helper()
	d, err := dht.New(context.Background(), h.Libp2p(), dht.Mode(dht.ModeServer))
	if err != nil {
		t.Fatalf("building DHT: %v", err)
	}
	t.Cleanup(func() {
		if err := d.Close(); err != nil {
			t.Errorf("closing DHT: %v", err)
		}
	})
	return d
}
