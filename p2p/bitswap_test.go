package p2p_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ipfs/boxo/exchange"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"

	"github.com/blobarchive/bloar/p2p"
)

// TestFetchingBlockstoreFetches is the follower primitive end to end: two real
// hosts, the block on one of them, and a Get on the other that has to cross the
// network to answer.
func TestFetchingBlockstoreFetches(t *testing.T) {
	server, client := newTestHost(t), newTestHost(t)
	connect(t, client, server)

	serverBlocks, clientBlocks := memBlocks(), memBlocks()
	want := rawBlock(t, []byte("a block only the server has"))
	putBlock(t, serverBlocks, want)

	newTestExchange(t, server, serverBlocks)
	clientExchange := newTestExchange(t, client, clientBlocks)

	bs := p2p.FetchingBlockstore(t.Context(), clientBlocks, clientExchange)

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	got, err := bs.Get(ctx, want.Cid())
	if err != nil {
		t.Fatalf("fetching block: %v", err)
	}
	if got.Cid() != want.Cid() {
		t.Errorf("fetched %s, want %s", got.Cid(), want.Cid())
	}
	if string(got.RawData()) != string(want.RawData()) {
		t.Errorf("fetched %q, want %q", got.RawData(), want.RawData())
	}
}

// TestFetchingBlockstoreFetchesFromLaterPeer is the same fetch with the
// connection made after both exchanges exist, which is the other order the two
// can happen in.
//
// Both orders are load-bearing and neither is hypothetical: bitswap hears about
// peers from a libp2p notifiee it registers when it starts, so a peer connected
// before that -- a static peer the host dialled on the way up, anything inbound
// since the listener opened -- is one it would never hear about at all.
// TestFetchingBlockstoreFetches covers that order by connecting first; this one
// covers the notifiee actually working.
func TestFetchingBlockstoreFetchesFromLaterPeer(t *testing.T) {
	server, client := newTestHost(t), newTestHost(t)

	serverBlocks, clientBlocks := memBlocks(), memBlocks()
	want := rawBlock(t, []byte("connected only after bitswap started"))
	putBlock(t, serverBlocks, want)

	newTestExchange(t, server, serverBlocks)
	clientExchange := newTestExchange(t, client, clientBlocks)

	connect(t, client, server)

	bs := p2p.FetchingBlockstore(t.Context(), clientBlocks, clientExchange)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	got, err := bs.Get(ctx, want.Cid())
	if err != nil {
		t.Fatalf("fetching block: %v", err)
	}
	if got.Cid() != want.Cid() {
		t.Errorf("fetched %s, want %s", got.Cid(), want.Cid())
	}
}

// TestRefreshSessionsDiscoversSurvivingPeer is the multi-writer failover
// regression. A long-lived Boxo session learns only the writer which answers
// its first want. If that writer disappears, Boxo remembers that some peer was
// discovered even though the session now has none, and does not broadcast the
// next want to another globally connected writer. RefreshSessions must rotate
// that session without replaying network callbacks or interrupting a request.
func TestRefreshSessionsDiscoversSurvivingPeer(t *testing.T) {
	first, survivor, client := newTestHost(t), newTestHost(t), newTestHost(t)
	firstBlocks, survivorBlocks, clientBlocks := memBlocks(), memBlocks(), memBlocks()
	firstOnly := rawBlock(t, []byte("learn the first writer"))
	survivorOnly := rawBlock(t, []byte("fetch after the first writer disappears"))
	putBlock(t, firstBlocks, firstOnly)
	putBlock(t, survivorBlocks, survivorOnly)

	newTestExchange(t, first, firstBlocks)
	newTestExchange(t, survivor, survivorBlocks)
	clientExchange := newTestExchange(t, client, clientBlocks)
	connect(t, client, first)

	refreshing := p2p.NewRefreshingSessionSource(clientExchange)
	bs := p2p.FetchingBlockstore(t.Context(), clientBlocks, refreshing)
	firstCtx, cancelFirst := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancelFirst()
	if _, err := bs.Get(firstCtx, firstOnly.Cid()); err != nil {
		t.Fatalf("priming session from first writer: %v", err)
	}

	// The survivor connects only after the session has learned the first writer,
	// so it is globally connected but absent from that session's peer set.
	connect(t, client, survivor)
	if err := client.Libp2p().Network().ClosePeer(first.ID()); err != nil {
		t.Fatalf("disconnecting first writer: %v", err)
	}

	// Repetition is intentional: refresh is an epoch change, not a synthetic
	// PeerConnected callback, and must remain safe when two discovery boundaries
	// occur before the next block request.
	refreshing.RefreshSessions()
	refreshing.RefreshSessions()
	fetchCtx, cancelFetch := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancelFetch()
	got, err := bs.Get(fetchCtx, survivorOnly.Cid())
	if err != nil {
		t.Fatalf("fetching from surviving writer after refresh: %v", err)
	}
	if got.Cid() != survivorOnly.Cid() {
		t.Fatalf("surviving writer returned %s, want %s", got.Cid(), survivorOnly.Cid())
	}
}

// TestExchangeServerOptOutKeepsClient proves the opt-out changes only one
// direction. The opted-out node refuses a block it already has, then fetches a
// different block from the ordinary node over the same connection.
func TestExchangeServerOptOutKeepsClient(t *testing.T) {
	optedOut, serving := newTestHost(t), newTestHost(t)
	connect(t, optedOut, serving)

	optedOutBlocks, servingBlocks := memBlocks(), memBlocks()
	private := rawBlock(t, []byte("present but deliberately not served"))
	public := rawBlock(t, []byte("the opted-out node may still fetch"))
	putBlock(t, optedOutBlocks, private)
	putBlock(t, servingBlocks, public)

	optedOutExchange, err := p2p.NewExchange(t.Context(), p2p.ExchangeConfig{
		Host:          optedOut,
		Blocks:        optedOutBlocks,
		DisableServer: true,
	})
	if err != nil {
		t.Fatalf("building opted-out exchange: %v", err)
	}
	t.Cleanup(func() {
		if err := optedOutExchange.Close(); err != nil {
			t.Errorf("closing opted-out exchange: %v", err)
		}
	})
	servingExchange := newTestExchange(t, serving, servingBlocks)

	servingFetcher := p2p.FetchingBlockstore(t.Context(), servingBlocks, servingExchange)
	refuseCtx, cancelRefuse := context.WithTimeout(t.Context(), 500*time.Millisecond)
	defer cancelRefuse()
	if _, err := servingFetcher.Get(refuseCtx, private.Cid()); err == nil {
		t.Fatal("server-disabled exchange served a local block")
	} else if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("server-disabled fetch error = %v, want deadline exceeded", err)
	}

	optedOutFetcher := p2p.FetchingBlockstore(t.Context(), optedOutBlocks, optedOutExchange)
	fetchCtx, cancelFetch := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancelFetch()
	got, err := optedOutFetcher.Get(fetchCtx, public.Cid())
	if err != nil {
		t.Fatalf("opted-out exchange client fetch: %v", err)
	}
	if got.Cid() != public.Cid() {
		t.Errorf("client fetched %s, want %s", got.Cid(), public.Cid())
	}
}

// TestFetchingBlockstoreCaches is the other half of the same contract: a
// fetched block is written through, so the second read is local. It is asserted
// by asking the local store directly -- the block being there is the claim, and
// a second Get would pass whether or not it was.
func TestFetchingBlockstoreCaches(t *testing.T) {
	server, client := newTestHost(t), newTestHost(t)
	connect(t, client, server)

	serverBlocks, clientBlocks := memBlocks(), memBlocks()
	want := rawBlock(t, []byte("cache me"))
	putBlock(t, serverBlocks, want)

	newTestExchange(t, server, serverBlocks)
	clientExchange := newTestExchange(t, client, clientBlocks)
	bs := p2p.FetchingBlockstore(t.Context(), clientBlocks, clientExchange)

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	if _, err := bs.Get(ctx, want.Cid()); err != nil {
		t.Fatalf("fetching block: %v", err)
	}

	has, err := clientBlocks.Has(context.Background(), want.Cid())
	if err != nil {
		t.Fatalf("checking local store: %v", err)
	}
	if !has {
		t.Fatal("fetched block was not written to the local store")
	}

	// And now the network cannot be what answers: with the session source
	// replaced by one that fails the test if it is asked, a hit proves the read
	// was local.
	local := p2p.FetchingBlockstore(t.Context(), clientBlocks, refusingSource{t})
	got, err := local.Get(context.Background(), want.Cid())
	if err != nil {
		t.Fatalf("second Get went to the network or failed: %v", err)
	}
	if got.Cid() != want.Cid() {
		t.Errorf("second Get returned %s, want %s", got.Cid(), want.Cid())
	}
}

// TestFetchingBlockstoreDeadline covers the read-miss path of spec 11.4: the
// index named a block, no peer has it, and follow.fetch_timeout is what ends
// the wait. The error has to be both distinguishable from not-found (11.4 makes
// that a 503 rather than a 404) and recognisable as a timeout.
func TestFetchingBlockstoreDeadline(t *testing.T) {
	server, client := newTestHost(t), newTestHost(t)
	connect(t, client, server)

	newTestExchange(t, server, memBlocks()) // holds nothing
	clientBlocks := memBlocks()
	clientExchange := newTestExchange(t, client, clientBlocks)
	bs := p2p.FetchingBlockstore(t.Context(), clientBlocks, clientExchange)

	absent := rawBlock(t, []byte("nobody has this"))

	ctx, cancel := context.WithTimeout(t.Context(), 250*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err := bs.Get(ctx, absent.Cid())
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("Get of an absent block succeeded")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Get error = %v, want it to unwrap to context.DeadlineExceeded", err)
	}
	var fetchErr *p2p.FetchError
	if !errors.As(err, &fetchErr) {
		t.Errorf("Get error = %v, want a *p2p.FetchError", err)
	} else if fetchErr.Cid != absent.Cid() {
		t.Errorf("FetchError.Cid = %s, want %s", fetchErr.Cid, absent.Cid())
	}
	if elapsed > 10*time.Second {
		t.Errorf("Get took %s, want it bounded by the 250ms deadline", elapsed)
	}
}

// TestFetchingBlockstoreHasFetches: Has crosses the network, because it answers
// "this node has this block" and the only way to make that true is to get it.
func TestFetchingBlockstoreHasFetches(t *testing.T) {
	server, client := newTestHost(t), newTestHost(t)
	connect(t, client, server)

	serverBlocks, clientBlocks := memBlocks(), memBlocks()
	want := rawBlock(t, []byte("has me"))
	putBlock(t, serverBlocks, want)

	newTestExchange(t, server, serverBlocks)
	clientExchange := newTestExchange(t, client, clientBlocks)
	bs := p2p.FetchingBlockstore(t.Context(), clientBlocks, clientExchange)

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	has, err := bs.Has(ctx, want.Cid())
	if err != nil {
		t.Fatalf("Has: %v", err)
	}
	if !has {
		t.Fatal("Has returned false for a block a peer holds")
	}
	// Has says "have", so it must have.
	local, err := clientBlocks.Has(context.Background(), want.Cid())
	if err != nil {
		t.Fatalf("checking local store: %v", err)
	}
	if !local {
		t.Error("Has returned true without the block being local")
	}
}

// TestFetchingBlockstoreAllKeysChanIsLocal is the one that stops a GC from
// enumerating the network. It is asserted against a source that fails the test
// if a session is ever opened: the point is not that the answer is short, it is
// that nothing was asked.
func TestFetchingBlockstoreAllKeysChanIsLocal(t *testing.T) {
	localBlocks := memBlocks()
	mine := rawBlock(t, []byte("mine"))
	putBlock(t, localBlocks, mine)

	// A source whose sessions hold a block this node does not: if AllKeysChan
	// reached the network, this is what would show up in it.
	theirs := rawBlock(t, []byte("theirs"))
	bs := p2p.FetchingBlockstore(t.Context(), localBlocks, refusingSource{t})

	keys, err := bs.AllKeysChan(t.Context())
	if err != nil {
		t.Fatalf("AllKeysChan: %v", err)
	}
	var got []cid.Cid
	for k := range keys {
		got = append(got, k)
	}
	if len(got) != 1 {
		t.Fatalf("AllKeysChan yielded %d keys, want 1 (the one local block)", len(got))
	}
	// The blockstore is keyed by multihash, so compare there: a raw-codec CID
	// out of AllKeysChan is the same block as the one that went in.
	if string(got[0].Hash()) != string(mine.Cid().Hash()) {
		t.Errorf("AllKeysChan yielded %s, want %s", got[0], mine.Cid())
	}
	if string(got[0].Hash()) == string(theirs.Cid().Hash()) {
		t.Error("AllKeysChan yielded a block only a peer has")
	}
}

// TestFetchingBlockstoreWritesAreLocal: Put and DeleteBlock do not consult a
// session, and land in the store underneath.
func TestFetchingBlockstoreWritesAreLocal(t *testing.T) {
	localBlocks := memBlocks()
	bs := p2p.FetchingBlockstore(t.Context(), localBlocks, refusingSource{t})

	blk := rawBlock(t, []byte("written locally"))
	if err := bs.Put(context.Background(), blk); err != nil {
		t.Fatalf("Put: %v", err)
	}
	has, err := localBlocks.Has(context.Background(), blk.Cid())
	if err != nil {
		t.Fatalf("checking local store: %v", err)
	}
	if !has {
		t.Fatal("Put did not reach the local store")
	}

	if err := bs.DeleteBlock(context.Background(), blk.Cid()); err != nil {
		t.Fatalf("DeleteBlock: %v", err)
	}
	if has, err := localBlocks.Has(context.Background(), blk.Cid()); err != nil || has {
		t.Fatalf("DeleteBlock did not reach the local store (has=%v, err=%v)", has, err)
	}
}

// refusingSource fails the test if anything opens a session on it. It is how
// the local-only claims here are asserted: not by checking the answer, but by
// checking that the network was never reached for it.
type refusingSource struct{ t *testing.T }

func (s refusingSource) NewSession(context.Context) exchange.Fetcher {
	return refusingFetcher(s)
}

type refusingFetcher struct{ t *testing.T }

func (f refusingFetcher) GetBlock(_ context.Context, c cid.Cid) (blocks.Block, error) {
	f.t.Helper()
	f.t.Errorf("a local-only operation fetched %s over the network", c)
	return nil, errors.New("refusingFetcher")
}

func (f refusingFetcher) GetBlocks(_ context.Context, ks []cid.Cid) (<-chan blocks.Block, error) {
	f.t.Helper()
	f.t.Errorf("a local-only operation fetched %d blocks over the network", len(ks))
	return nil, errors.New("refusingFetcher")
}
