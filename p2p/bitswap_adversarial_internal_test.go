package p2p

import (
	"context"
	"encoding/binary"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ipfs/boxo/blockstore"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multihash"
)

// TestBitswapFloodIsBoundedAndDoesNotStarveAnotherPeer is deliberately above
// the unit-test seam: three real libp2p hosts speak Bitswap over loopback TCP.
// One peer asks for far more blocks than its accepted wantlist while another
// asks for one. The server's first flood read is held open so every limit can
// be observed under pressure rather than sampled after the work has drained.
func TestBitswapFloodIsBoundedAndDoesNotStarveAnotherPeer(t *testing.T) {
	const (
		wantCap        = 8
		floodWantCount = 64
		blockBytes     = 64 << 10
		engineWorkers  = 2
	)

	serverHost := newAdmissionTestHost(t)
	floodHost := newAdmissionTestHost(t)
	honestHost := newAdmissionTestHost(t)
	connectAdmissionTestHosts(t, floodHost, serverHost)
	connectAdmissionTestHosts(t, honestHost, serverHost)

	serverBase := newAdmissionTestBlockstore()
	floodCIDs := make([]cid.Cid, 0, floodWantCount)
	for i := 0; i < floodWantCount; i++ {
		blk := admissionTestBlock(t, blockBytes, uint64(i))
		putAdmissionTestBlock(t, serverBase, blk)
		floodCIDs = append(floodCIDs, blk.Cid())
	}
	// A small index-like block is answered directly to the initial WANT_HAVE;
	// the flood uses blob-like blocks that take the ordinary HAVE -> WANT_BLOCK
	// path. Both are real shapes in the Bloar archive.
	honestBlock := admissionTestBlock(t, 512, floodWantCount+1)
	putAdmissionTestBlock(t, serverBase, honestBlock)

	observedStore := newFloodGateBlockstore(serverBase, floodCIDs)
	serverExchange := newAdmissionTestExchange(t, ExchangeConfig{
		Host:                            serverHost,
		Blocks:                          observedStore,
		MaxQueuedWantlistEntriesPerPeer: wantCap,
		MaxOutstandingBytesPerPeer:      blockBytes,
		TaskWorkerCount:                 engineWorkers,
		EngineTaskWorkerCount:           engineWorkers,
		EngineBlockstoreWorkerCount:     engineWorkers,
		MaxCIDSize:                      MinimumBitswapMaxCIDSize,
	})
	floodExchange := newAdmissionTestExchange(t, ExchangeConfig{
		Host:          floodHost,
		Blocks:        newAdmissionTestBlockstore(),
		DisableServer: true,
	})
	honestExchange := newAdmissionTestExchange(t, ExchangeConfig{
		Host:          honestHost,
		Blocks:        newAdmissionTestBlockstore(),
		DisableServer: true,
	})
	// This cleanup is registered after the exchanges, so it runs before their
	// Close methods and cannot leave an engine worker held in Blockstore.Get.
	t.Cleanup(observedStore.release)

	monitorCtx, stopMonitor := context.WithCancel(t.Context())
	var maxWantlist atomic.Int64
	var wantlistOverCap atomic.Bool
	monitorDone := make(chan struct{})
	go func() {
		defer close(monitorDone)
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		for {
			n := int64(len(serverExchange.bs.WantlistForPeer(floodHost.ID())))
			for previous := maxWantlist.Load(); n > previous && !maxWantlist.CompareAndSwap(previous, n); {
				previous = maxWantlist.Load()
			}
			if n > wantCap {
				wantlistOverCap.Store(true)
			}
			select {
			case <-monitorCtx.Done():
				return
			case <-ticker.C:
			}
		}
	}()

	floodCtx, cancelFlood := context.WithCancel(t.Context())
	floodOut, err := floodExchange.NewSession(floodCtx).GetBlocks(floodCtx, floodCIDs)
	if err != nil {
		cancelFlood()
		t.Fatalf("starting flood wantlist: %v", err)
	}
	floodDone := make(chan struct{})
	go func() {
		defer close(floodDone)
		for range floodOut {
		}
	}()

	waitAdmissionCondition(t, 10*time.Second, "flood wantlist to reach its cap", func() bool {
		return len(serverExchange.bs.WantlistForPeer(floodHost.ID())) == wantCap
	})
	select {
	case <-observedStore.floodStarted:
	case <-time.After(10 * time.Second):
		cancelFlood()
		t.Fatal("flood peer never advanced from HAVE to a block read")
	}

	// MaxOutstandingBytesPerPeer is a soft scheduler watermark, not a hard
	// byte counter. Boxo does not expose per-peer activeWork through a stable
	// API. This fixture makes its observable consequence exact: every flood
	// block has size == watermark, every resulting envelope contains one CID,
	// and its Get is held open. A second flood Get beginning now would mean the
	// peer received more work after reaching the watermark.
	if stats := observedStore.snapshot(); stats.floodStarts != 1 || stats.activeFlood != 1 {
		cancelFlood()
		t.Fatalf("flood work before honest request = %+v, want one held block task", stats)
	}

	honestCtx, cancelHonest := context.WithTimeout(t.Context(), 10*time.Second)
	got, err := honestExchange.NewSession(honestCtx).GetBlock(honestCtx, honestBlock.Cid())
	cancelHonest()
	if err != nil {
		cancelFlood()
		t.Fatalf("honest peer starved behind held flood work: %v (blockstore=%+v flood_wants=%d honest_wants=%d)",
			err, observedStore.snapshot(),
			len(serverExchange.bs.WantlistForPeer(floodHost.ID())),
			len(serverExchange.bs.WantlistForPeer(honestHost.ID())))
	}
	if got.Cid() != honestBlock.Cid() {
		cancelFlood()
		t.Fatalf("honest peer got %s, want %s", got.Cid(), honestBlock.Cid())
	}

	stats := observedStore.snapshot()
	if stats.floodStarts != 1 || stats.maxFlood != 1 || stats.activeFlood != 1 {
		cancelFlood()
		t.Errorf("flood peer crossed one-block outstanding watermark while held: %+v", stats)
	}
	// With one-CID envelopes, each Blockstore.Get is synchronous in one engine
	// task worker. The held flood read and successful honest read therefore
	// make Get concurrency an observable worker-bound assertion for this test,
	// without reaching into Boxo's private scheduler structures.
	if stats.maxActive > engineWorkers {
		t.Errorf("active block work reached %d, configured engine workers = %d", stats.maxActive, engineWorkers)
	}

	stopMonitor()
	<-monitorDone
	if wantlistOverCap.Load() {
		t.Errorf("flood wantlist exceeded configured cap %d", wantCap)
	}
	if max := maxWantlist.Load(); max != wantCap {
		t.Errorf("maximum observed flood wantlist = %d, want cap %d", max, wantCap)
	}

	// Capture every assertion while the flood is still held. Cancellation then
	// stops retries; releasing the read lets the server and client shut down.
	cancelFlood()
	observedStore.release()
	select {
	case <-floodDone:
	case <-time.After(10 * time.Second):
		t.Fatal("flood request did not stop after cancellation")
	}
}

type floodGateStats struct {
	active      int
	maxActive   int
	activeFlood int
	maxFlood    int
	floodStarts int
}

// floodGateBlockstore turns a Blockstore.Get into a deterministic observation
// point. Only flood CIDs are gated; the honest peer can still prove liveness.
type floodGateBlockstore struct {
	blockstore.Blockstore

	flood map[string]struct{}
	gate  chan struct{}

	releaseOnce  sync.Once
	startedOnce  sync.Once
	floodStarted chan struct{}

	mu    sync.Mutex
	stats floodGateStats
}

func newFloodGateBlockstore(inner blockstore.Blockstore, flood []cid.Cid) *floodGateBlockstore {
	set := make(map[string]struct{}, len(flood))
	for _, c := range flood {
		set[c.KeyString()] = struct{}{}
	}
	return &floodGateBlockstore{
		Blockstore:   inner,
		flood:        set,
		gate:         make(chan struct{}),
		floodStarted: make(chan struct{}),
	}
}

func (b *floodGateBlockstore) Get(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	_, isFlood := b.flood[c.KeyString()]
	b.mu.Lock()
	b.stats.active++
	if b.stats.active > b.stats.maxActive {
		b.stats.maxActive = b.stats.active
	}
	if isFlood {
		b.stats.activeFlood++
		b.stats.floodStarts++
		if b.stats.activeFlood > b.stats.maxFlood {
			b.stats.maxFlood = b.stats.activeFlood
		}
	}
	b.mu.Unlock()

	if isFlood {
		b.startedOnce.Do(func() { close(b.floodStarted) })
		select {
		case <-b.gate:
		case <-ctx.Done():
			b.finishGet(true)
			return nil, ctx.Err()
		}
	}

	blk, err := b.Blockstore.Get(ctx, c)
	b.finishGet(isFlood)
	return blk, err
}

func (b *floodGateBlockstore) finishGet(isFlood bool) {
	b.mu.Lock()
	b.stats.active--
	if isFlood {
		b.stats.activeFlood--
	}
	b.mu.Unlock()
}

func (b *floodGateBlockstore) snapshot() floodGateStats {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.stats
}

func (b *floodGateBlockstore) release() {
	b.releaseOnce.Do(func() { close(b.gate) })
}

func newAdmissionTestHost(t *testing.T) *Host {
	t.Helper()
	h, err := NewHost(t.Context(), HostConfig{
		Listen:          []string{"/ip4/127.0.0.1/tcp/0"},
		IdentityKeyFile: filepath.Join(t.TempDir(), "p2p.key"),
	})
	if err != nil {
		t.Fatalf("building admission-test host: %v", err)
	}
	t.Cleanup(func() {
		if err := h.Close(); err != nil {
			t.Errorf("closing admission-test host: %v", err)
		}
	})
	return h
}

func connectAdmissionTestHosts(t *testing.T, from, to *Host) {
	t.Helper()
	if err := from.Libp2p().Connect(t.Context(), peer.AddrInfo{ID: to.ID(), Addrs: to.Libp2p().Addrs()}); err != nil {
		t.Fatalf("connecting %s to %s: %v", from.ID(), to.ID(), err)
	}
}

func newAdmissionTestBlockstore() blockstore.Blockstore {
	return blockstore.NewBlockstore(dssync.MutexWrap(datastore.NewMapDatastore()))
}

func admissionTestBlock(t *testing.T, size int, sequence uint64) blocks.Block {
	t.Helper()
	data := make([]byte, size)
	binary.LittleEndian.PutUint64(data, sequence)
	h, err := multihash.Sum(data, multihash.SHA2_256, -1)
	if err != nil {
		t.Fatalf("hashing admission-test block: %v", err)
	}
	blk, err := blocks.NewBlockWithCid(data, cid.NewCidV1(cid.Raw, h))
	if err != nil {
		t.Fatalf("building admission-test block: %v", err)
	}
	return blk
}

func putAdmissionTestBlock(t *testing.T, store blockstore.Blockstore, blk blocks.Block) {
	t.Helper()
	if err := store.Put(t.Context(), blk); err != nil {
		t.Fatalf("putting admission-test block: %v", err)
	}
}

func newAdmissionTestExchange(t *testing.T, cfg ExchangeConfig) *Exchange {
	t.Helper()
	exchange, err := NewExchange(t.Context(), cfg)
	if err != nil {
		t.Fatalf("building admission-test exchange: %v", err)
	}
	t.Cleanup(func() {
		if err := exchange.Close(); err != nil {
			t.Errorf("closing admission-test exchange: %v", err)
		}
	})
	return exchange
}

func waitAdmissionCondition(t *testing.T, timeout time.Duration, description string, ready func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if ready() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", description)
}
