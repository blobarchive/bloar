package pointerhint

import (
	"context"
	"crypto/rand"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multihash"
)

type staticFinderRouter struct {
	mu        sync.Mutex
	queries   []cid.Cid
	requested []int
	providers []peer.AddrInfo
}

type blockingFinderHost struct {
	self peer.ID

	mu          sync.Mutex
	connected   map[peer.ID]bool
	inFlight    int
	maxInFlight int
	release     <-chan struct{}
}

func (h *blockingFinderHost) ID() peer.ID { return h.self }

func (h *blockingFinderHost) Connectedness(id peer.ID) network.Connectedness {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.connected[id] {
		return network.Connected
	}
	return network.NotConnected
}

func (h *blockingFinderHost) Connect(ctx context.Context, info peer.AddrInfo) error {
	h.mu.Lock()
	h.inFlight++
	if h.inFlight > h.maxInFlight {
		h.maxInFlight = h.inFlight
	}
	h.mu.Unlock()
	if h.release != nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-h.release:
		}
	}
	h.mu.Lock()
	h.inFlight--
	if h.connected == nil {
		h.connected = make(map[peer.ID]bool)
	}
	h.connected[info.ID] = true
	h.mu.Unlock()
	return nil
}

func (h *blockingFinderHost) concurrency() (int, int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.inFlight, h.maxInFlight
}

type hangingFinderRouter struct{}

func (hangingFinderRouter) FindProvidersAsync(context.Context, cid.Cid, int) <-chan peer.AddrInfo {
	return make(chan peer.AddrInfo)
}

func (r *staticFinderRouter) FindProvidersAsync(ctx context.Context, c cid.Cid, count int) <-chan peer.AddrInfo {
	r.mu.Lock()
	r.queries = append(r.queries, c)
	r.requested = append(r.requested, count)
	providers := append([]peer.AddrInfo(nil), r.providers...)
	r.mu.Unlock()
	ch := make(chan peer.AddrInfo)
	go func() {
		defer close(ch)
		for _, provider := range providers {
			select {
			case <-ctx.Done():
				return
			case ch <- provider:
			}
		}
	}()
	return ch
}

func randomPeerID(t *testing.T) peer.ID {
	t.Helper()
	_, public, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateEd25519Key: %v", err)
	}
	id, err := peer.IDFromPublicKey(public)
	if err != nil {
		t.Fatalf("IDFromPublicKey: %v", err)
	}
	return id
}

func TestFinderBoundsProviderResultsAndAddressBytes(t *testing.T) {
	h, err := libp2p.New(libp2p.NoListenAddrs)
	if err != nil {
		t.Fatalf("libp2p.New: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })
	short := ma.StringCast("/ip4/127.0.0.1/tcp/1")
	large := ma.StringCast("/dns4/this-name-is-deliberately-too-large-for-the-address-budget.example/tcp/1")
	router := &staticFinderRouter{providers: []peer.AddrInfo{
		{ID: randomPeerID(t), Addrs: []ma.Multiaddr{large}},
		{ID: randomPeerID(t), Addrs: []ma.Multiaddr{short}},
		{ID: randomPeerID(t), Addrs: []ma.Multiaddr{short}},
	}}
	finder, err := NewFinder(FinderConfig{
		Router:          router,
		Host:            h,
		MaxResults:      2,
		MaxAddressBytes: len(short.Bytes()),
		DialConcurrency: 1,
		DialTimeout:     5 * time.Millisecond,
		FindTimeout:     time.Second,
	})
	if err != nil {
		t.Fatalf("NewFinder: %v", err)
	}
	pointer := Pointer{Kind: Manifest, CID: testBlock(t, cid.DagCBOR, "known manifest").Cid()}
	result, err := finder.FindAndDial(t.Context(), pointer)
	if err != nil {
		t.Fatalf("FindAndDial: %v", err)
	}
	if result.Results != 2 {
		t.Errorf("results consumed = %d, want hard cap 2", result.Results)
	}
	if result.Dialed != 1 {
		t.Errorf("dials = %d, want only the one address that fit the budget", result.Dialed)
	}
	router.mu.Lock()
	defer router.mu.Unlock()
	if len(router.requested) != 1 || router.requested[0] != 2 {
		t.Errorf("FindProvidersAsync counts = %v, want [2]", router.requested)
	}
	if len(router.queries) != 1 || !router.queries[0].Equals(pointer.CID) {
		t.Errorf("FindProvidersAsync queries = %v, want exact pointer CID %s", router.queries, pointer.CID)
	}
}

func TestFinderAcceptsLaterUsableAddressForSamePeer(t *testing.T) {
	host := &blockingFinderHost{self: randomPeerID(t)}
	id := randomPeerID(t)
	usable := ma.StringCast("/ip4/127.0.0.1/tcp/4001")
	oversized := ma.StringCast("/dns4/this-address-is-over-the-first-budget.example/tcp/4001")
	router := &staticFinderRouter{providers: []peer.AddrInfo{
		{ID: id, Addrs: []ma.Multiaddr{oversized}},
		{ID: id, Addrs: []ma.Multiaddr{usable}},
	}}
	finder := &Finder{router: router, host: host, cfg: finderSettings{
		maxResults: 2, maxAddressBytes: len(usable.Bytes()), dialConcurrency: 1,
		dialTimeout: time.Second, findTimeout: time.Second,
	}}
	result, err := finder.FindAndDial(t.Context(), Pointer{Kind: Root, CID: testBlock(t, cid.DagCBOR, "root").Cid()})
	if err != nil {
		t.Fatalf("FindAndDial: %v", err)
	}
	if result.Dialed != 1 || result.Connected != 1 {
		t.Fatalf("same-peer address upgrade result = %+v, want later usable lead dialed", result)
	}
}

func TestFinderRejectsUntypedAndNonRawDocumentQueries(t *testing.T) {
	h, err := libp2p.New(libp2p.NoListenAddrs)
	if err != nil {
		t.Fatalf("libp2p.New: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })
	router := &staticFinderRouter{}
	finder, err := NewFinder(FinderConfig{Router: router, Host: h})
	if err != nil {
		t.Fatalf("NewFinder: %v", err)
	}
	dag := testBlock(t, cid.DagCBOR, "dag").Cid()
	if _, err := finder.FindAndDial(t.Context(), Pointer{CID: dag}); err == nil {
		t.Fatal("an untyped arbitrary-CID query was accepted")
	}
	if _, err := finder.FindAndDial(t.Context(), Pointer{Kind: Document, CID: dag}); err == nil {
		t.Fatal("a non-raw publication-document query was accepted")
	}
	raw := testBlock(t, cid.Raw, "raw").Cid()
	if _, err := finder.FindAndDial(t.Context(), Pointer{Kind: Root, CID: raw}); err == nil {
		t.Fatal("a raw CID posing as a root was accepted")
	}
	sha512, err := cid.Prefix{
		Version: 1, Codec: cid.Raw, MhType: multihash.SHA2_512, MhLength: -1,
	}.Sum([]byte("non-Bloar hash profile"))
	if err != nil {
		t.Fatalf("building sha2-512 CID: %v", err)
	}
	if _, err := finder.FindAndDial(t.Context(), Pointer{Kind: Document, CID: sha512}); err == nil {
		t.Fatal("a document outside Bloar's CIDv1/sha2-256 profile was accepted")
	}
	router.mu.Lock()
	defer router.mu.Unlock()
	if len(router.queries) != 0 {
		t.Fatalf("rejected pointers reached the provider router: %v", router.queries)
	}
}

func TestFinderBoundsDialConcurrency(t *testing.T) {
	release := make(chan struct{})
	host := &blockingFinderHost{self: randomPeerID(t), release: release}
	providers := make([]peer.AddrInfo, 6)
	for i := range providers {
		providers[i] = peer.AddrInfo{ID: randomPeerID(t)}
	}
	router := &staticFinderRouter{providers: providers}
	finder := &Finder{
		router: router,
		host:   host,
		cfg: finderSettings{
			maxResults:      len(providers),
			maxAddressBytes: 1,
			dialConcurrency: 2,
			dialTimeout:     time.Second,
			findTimeout:     time.Second,
		},
	}
	done := make(chan FindResult, 1)
	errs := make(chan error, 1)
	go func() {
		result, err := finder.FindAndDial(t.Context(), Pointer{Kind: Root, CID: testBlock(t, cid.DagCBOR, "root").Cid()})
		done <- result
		errs <- err
	}()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		inFlight, maximum := host.concurrency()
		if inFlight == 2 && maximum == 2 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if inFlight, maximum := host.concurrency(); inFlight != 2 || maximum != 2 {
		t.Fatalf("before release: in_flight=%d max=%d, want 2/2", inFlight, maximum)
	}
	close(release)
	var result FindResult
	select {
	case result = <-done:
	case <-time.After(time.Second):
		t.Fatal("FindAndDial did not finish after releasing bounded dials")
	}
	select {
	case err := <-errs:
		if err != nil {
			t.Fatalf("FindAndDial: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("FindAndDial did not report its result")
	}
	if result.Dialed != len(providers) || result.Connected != len(providers) {
		t.Errorf("result = %+v, want all %d providers dialed and connected", result, len(providers))
	}
	if _, maximum := host.concurrency(); maximum != 2 {
		t.Errorf("maximum dial concurrency = %d, want 2", maximum)
	}
}

func TestFinderBoundsHungProviderQuery(t *testing.T) {
	host := &blockingFinderHost{self: randomPeerID(t)}
	finder := &Finder{
		router: hangingFinderRouter{},
		host:   host,
		cfg: finderSettings{
			maxResults:      1,
			maxAddressBytes: 1,
			dialConcurrency: 1,
			dialTimeout:     time.Second,
			findTimeout:     10 * time.Millisecond,
		},
	}
	started := time.Now()
	_, err := finder.FindAndDial(t.Context(), Pointer{Kind: Root, CID: testBlock(t, cid.DagCBOR, "root").Cid()})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("hung query error = %v, want deadline exceeded", err)
	}
	if elapsed := time.Since(started); elapsed > 250*time.Millisecond {
		t.Errorf("hung query returned after %s, want bounded near 10ms", elapsed)
	}
}

func TestFinderReportsOverallDeadlineAfterResultCapWithHungDial(t *testing.T) {
	never := make(chan struct{})
	host := &blockingFinderHost{self: randomPeerID(t), release: never}
	router := &staticFinderRouter{providers: []peer.AddrInfo{{ID: randomPeerID(t)}}}
	finder := &Finder{
		router: router,
		host:   host,
		cfg: finderSettings{
			maxResults:      1,
			maxAddressBytes: 1,
			dialConcurrency: 1,
			dialTimeout:     time.Second,
			findTimeout:     10 * time.Millisecond,
		},
	}
	started := time.Now()
	result, err := finder.FindAndDial(t.Context(), Pointer{Kind: Root, CID: testBlock(t, cid.DagCBOR, "root").Cid()})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("result-cap hung dial error = %v, want deadline exceeded; result=%+v", err, result)
	}
	if result.Dialed != 1 || result.DialFailed != 1 {
		t.Fatalf("result-cap hung dial stats = %+v, want one attempted/failed dial", result)
	}
	if elapsed := time.Since(started); elapsed > 250*time.Millisecond {
		t.Fatalf("result-cap hung dial returned after %s, want bounded near 10ms", elapsed)
	}
}

func TestFinderPerDialTimeoutPrecedesOverallTimeout(t *testing.T) {
	never := make(chan struct{})
	host := &blockingFinderHost{self: randomPeerID(t), release: never}
	router := &staticFinderRouter{providers: []peer.AddrInfo{{ID: randomPeerID(t)}}}
	finder := &Finder{router: router, host: host, cfg: finderSettings{
		maxResults: 1, maxAddressBytes: 1, dialConcurrency: 1,
		dialTimeout: 10 * time.Millisecond, findTimeout: time.Second,
	}}
	started := time.Now()
	result, err := finder.FindAndDial(t.Context(), Pointer{Kind: Root, CID: testBlock(t, cid.DagCBOR, "root").Cid()})
	if err != nil {
		t.Fatalf("FindAndDial overall error = %v, want completed round with a failed dial", err)
	}
	if result.Dialed != 1 || result.DialFailed != 1 {
		t.Fatalf("per-dial timeout result = %+v, want one attempted/failed dial", result)
	}
	if elapsed := time.Since(started); elapsed > 250*time.Millisecond {
		t.Fatalf("per-dial timeout returned after %s, want bounded near 10ms", elapsed)
	}
}

func TestFinderPreservesCallerCancellationBeforeNilProviderChannel(t *testing.T) {
	host := &blockingFinderHost{self: randomPeerID(t)}
	router := &nilFinderRouter{}
	finder := &Finder{
		router: router,
		host:   host,
		cfg: finderSettings{
			maxResults:      1,
			maxAddressBytes: 1,
			dialConcurrency: 1,
			dialTimeout:     time.Second,
			findTimeout:     time.Second,
		},
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := finder.FindAndDial(ctx, Pointer{Kind: Root, CID: testBlock(t, cid.DagCBOR, "root").Cid()})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("pre-cancelled nil-channel lookup error = %v, want context canceled", err)
	}
	router.mu.Lock()
	defer router.mu.Unlock()
	if router.calls != 0 {
		t.Fatalf("pre-cancelled lookup called provider router %d times, want zero", router.calls)
	}
}

type nilFinderRouter struct {
	mu    sync.Mutex
	calls int
}

func (r *nilFinderRouter) FindProvidersAsync(context.Context, cid.Cid, int) <-chan peer.AddrInfo {
	r.mu.Lock()
	r.calls++
	r.mu.Unlock()
	return nil
}
