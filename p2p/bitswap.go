package p2p

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/ipfs/boxo/bitswap"
	bitswapclient "github.com/ipfs/boxo/bitswap/client"
	bsnet "github.com/ipfs/boxo/bitswap/network/bsnet"
	"github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/boxo/exchange"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/blobarchive/bloar/metrics"
)

// ExchangeConfig is what an Exchange needs.
type ExchangeConfig struct {
	// Host is the libp2p host to speak bitswap over. Required.
	Host *Host
	// Blocks is the blockstore served to peers, and the one a fetch writes
	// through to. Required. On a writer this is the store's blockstore and the
	// serving half is all that runs; on a follower it is the same blockstore,
	// with FetchingBlockstore layered over it for the reading half.
	Blocks blockstore.Blockstore
	// Metrics counts what the fetching half fetches and the raw block payload
	// bytes the serving half schedules in outbound envelopes (spec 11.2).
	// Scheduled bytes are traced before send and are not delivery-confirmed.
	// Optional; nil records nothing and wraps nothing.
	Metrics *metrics.Metrics
	// DisableServer is the explicit serve opt-out. The zero value deliberately
	// serves: both writers and followers are useful Bitswap peers by default.
	// Disabling the server does not disable the client or its fetch cache.
	DisableServer bool
	// TraceBlocks wraps fetched blocks with the PeerID that actually supplied
	// their bytes. It is intended for target-attribution probes; ordinary archive
	// operation leaves it disabled to avoid changing returned block types.
	TraceBlocks bool

	// The remaining fields bound work accepted from and performed for peers.
	// Zero selects Bloar's pinned default; a negative value is invalid. They are
	// int64 so config adapters can validate before converting to Boxo's
	// platform-sized int/uint options. These are queue/concurrency/working-set
	// bounds, not bandwidth-rate limits.
	//
	// The fields deliberately have no numeric ordering relationship: a queue
	// may safely be smaller than a worker pool, the three worker pools bound
	// independent pipeline stages, and CID bytes are not response bytes.
	// MaxOutstandingBytesPerPeer is Boxo's soft per-peer scheduling watermark;
	// the task that crosses it is allowed to finish, so it is not a hard memory
	// ceiling.
	MaxQueuedWantlistEntriesPerPeer int64
	MaxOutstandingBytesPerPeer      int64
	TaskWorkerCount                 int64
	EngineTaskWorkerCount           int64
	EngineBlockstoreWorkerCount     int64
	MaxCIDSize                      int64
}

// Bloar owns these defaults rather than inheriting Boxo's. They intentionally
// match Boxo v0.41's bounded server defaults, so an upstream default change
// cannot silently widen (or unexpectedly constrict) a deployed node's resource
// posture. Every value is passed to bitswap.New explicitly.
const (
	DefaultBitswapMaxQueuedWantlistEntriesPerPeer int64 = 1024
	DefaultBitswapMaxOutstandingBytesPerPeer      int64 = 1 << 20
	DefaultBitswapTaskWorkerCount                 int64 = 8
	DefaultBitswapEngineTaskWorkerCount           int64 = 8
	DefaultBitswapEngineBlockstoreWorkerCount     int64 = 128
	// CIDv1 has four varints plus a digest. Boxo v0.41 permits its 128-byte
	// verifier digest ceiling plus four worst-case 10-byte varints.
	DefaultBitswapMaxCIDSize int64 = 128 + 4*10
	// MinimumBitswapMaxCIDSize is the encoded length of every CID Bloar
	// publishes: CIDv1 + raw or dag-cbor + sha2-256 multihash. A lower Boxo
	// limit silently ignores valid requests for Bloar blocks, leaving a node
	// apparently connected but unable to serve the archive.
	MinimumBitswapMaxCIDSize int64 = 4 + 32
)

type exchangeSettings struct {
	maxQueuedWantlistEntriesPerPeer uint
	maxOutstandingBytesPerPeer      int
	taskWorkerCount                 int
	engineTaskWorkerCount           int
	engineBlockstoreWorkerCount     int
	maxCIDSize                      uint
}

// ValidateExchangeConfig validates Bloar's Bitswap work limits without
// requiring a live host or blockstore. Daemon config adapters use it before
// opening any network listeners; NewExchange still performs the same check.
func ValidateExchangeConfig(cfg ExchangeConfig) error {
	_, err := cfg.settings()
	return err
}

func (cfg ExchangeConfig) settings() (exchangeSettings, error) {
	// Boxo accepts this option as uint but converts it to int while parsing a
	// wantlist. Bound it to int here as well, otherwise a valid uint that is too
	// large for int would wrap and make the admission check reject every want.
	queuedValue, err := bitswapValue("MaxQueuedWantlistEntriesPerPeer", cfg.MaxQueuedWantlistEntriesPerPeer,
		DefaultBitswapMaxQueuedWantlistEntriesPerPeer, uint64(^uint(0)>>1), "int")
	if err != nil {
		return exchangeSettings{}, err
	}
	queued := uint(queuedValue)
	outstanding, err := bitswapInt("MaxOutstandingBytesPerPeer", cfg.MaxOutstandingBytesPerPeer,
		DefaultBitswapMaxOutstandingBytesPerPeer)
	if err != nil {
		return exchangeSettings{}, err
	}
	taskWorkers, err := bitswapInt("TaskWorkerCount", cfg.TaskWorkerCount,
		DefaultBitswapTaskWorkerCount)
	if err != nil {
		return exchangeSettings{}, err
	}
	engineTaskWorkers, err := bitswapInt("EngineTaskWorkerCount", cfg.EngineTaskWorkerCount,
		DefaultBitswapEngineTaskWorkerCount)
	if err != nil {
		return exchangeSettings{}, err
	}
	blockstoreWorkers, err := bitswapInt("EngineBlockstoreWorkerCount", cfg.EngineBlockstoreWorkerCount,
		DefaultBitswapEngineBlockstoreWorkerCount)
	if err != nil {
		return exchangeSettings{}, err
	}
	maxCIDSize, err := bitswapUint("MaxCIDSize", cfg.MaxCIDSize, DefaultBitswapMaxCIDSize)
	if err != nil {
		return exchangeSettings{}, err
	}
	if maxCIDSize < uint(MinimumBitswapMaxCIDSize) {
		return exchangeSettings{}, fmt.Errorf(
			"p2p: ExchangeConfig.MaxCIDSize must be at least %d bytes to admit Bloar CIDv1 sha2-256 identifiers",
			MinimumBitswapMaxCIDSize,
		)
	}
	return exchangeSettings{
		maxQueuedWantlistEntriesPerPeer: queued,
		maxOutstandingBytesPerPeer:      outstanding,
		taskWorkerCount:                 taskWorkers,
		engineTaskWorkerCount:           engineTaskWorkers,
		engineBlockstoreWorkerCount:     blockstoreWorkers,
		maxCIDSize:                      maxCIDSize,
	}, nil
}

func bitswapInt(name string, value, fallback int64) (int, error) {
	v, err := bitswapValue(name, value, fallback, uint64(^uint(0)>>1), "int")
	return int(v), err
}

func bitswapUint(name string, value, fallback int64) (uint, error) {
	v, err := bitswapValue(name, value, fallback, uint64(^uint(0)), "uint")
	return uint(v), err
}

// bitswapValue is the single checked conversion path for Boxo options. max is
// injectable so the overflow branch can be executed on a 64-bit test host as
// well as by a real 32-bit build; production callers pass their platform's
// int or uint ceiling.
func bitswapValue(name string, value, fallback int64, max uint64, target string) (uint64, error) {
	if value == 0 {
		value = fallback
	}
	if value < 0 {
		return 0, fmt.Errorf("p2p: ExchangeConfig.%s must be greater than zero", name)
	}
	if uint64(value) > max {
		return 0, fmt.Errorf("p2p: ExchangeConfig.%s overflows %s on this platform", name, target)
	}
	return uint64(value), nil
}

// Exchange is bitswap over a Host: by default, the server of spec 11.2 (any node
// serves the blocks it has) and the client half that FetchingBlockstore fetches
// through. Both roles get both halves unless the operator explicitly opts out
// of serving: a follower serves what it has replicated, and a writer is free to
// fetch.
//
// # No content routing
//
// The client is built with no provider finder, so a session asks the peers it
// is connected to and nobody else. That is spec 11.2's "peering is static in
// v1: no DHT is required for block exchange"; the DHT this package builds
// elsewhere carries IPNS records and is not consulted here. What a node can
// fetch is therefore exactly what its static peers and the publication
// document's multiaddrs add up to, which is a property worth having: block
// availability is a thing an operator configures rather than a thing that
// happens.
type Exchange struct {
	bs *bitswap.Bitswap
	mx *metrics.Metrics
}

// NewExchange starts bitswap on cfg.Host. ctx bounds construction only; the
// exchange runs until Close.
func NewExchange(ctx context.Context, cfg ExchangeConfig) (*Exchange, error) {
	if cfg.Host == nil {
		return nil, errors.New("p2p: ExchangeConfig.Host must not be nil")
	}
	if cfg.Blocks == nil {
		return nil, errors.New("p2p: ExchangeConfig.Blocks must not be nil")
	}
	settings, err := cfg.settings()
	if err != nil {
		return nil, err
	}
	net := bsnet.NewFromIpfsHost(cfg.Host.Libp2p())
	options := []bitswap.Option{
		bitswap.MaxQueuedWantlistEntriesPerPeer(settings.maxQueuedWantlistEntriesPerPeer),
		bitswap.MaxOutstandingBytesPerPeer(settings.maxOutstandingBytesPerPeer),
		bitswap.TaskWorkerCount(settings.taskWorkerCount),
		bitswap.EngineTaskWorkerCount(settings.engineTaskWorkerCount),
		bitswap.EngineBlockstoreWorkerCount(settings.engineBlockstoreWorkerCount),
		bitswap.MaxCidSize(settings.maxCIDSize),
		bitswap.WithServerEnabled(!cfg.DisableServer),
	}
	if cfg.TraceBlocks {
		options = append(options, bitswap.WithClientOption(bitswapclient.WithTraceBlock(true)))
	}
	if cfg.Metrics != nil {
		options = append(options, bitswap.WithTracer(&bitswapMetricsTracer{
			mx:       cfg.Metrics,
			classify: bitswapPeerClassifier(cfg.Host),
		}))
	}
	// WithoutCancel for the same reason Host keeps its own: this runs until
	// Close, and Close is sequenced after the things that depend on it.
	bs := bitswap.New(context.WithoutCancel(ctx), net, nil, cfg.Blocks, options...)

	// Peers the host already has have to be handed over by hand.
	//
	// bitswap learns about peers from a libp2p notifiee it registers as it
	// starts, and a notifiee only ever hears about connections opened after it
	// exists. Every connection older than this call -- a static peer the host
	// dialled while this was still being built, anything inbound since the
	// listener came up -- would otherwise be a peer bitswap never asks for
	// blocks and never answers, permanently, because nothing redials a peer
	// that is already connected. That is a node that looks healthy, has peers,
	// and fetches nothing.
	//
	// A peer connecting during the two lines above is seen twice, which leaves
	// bitswap holding it one refcount past its disconnection: a peer it tries
	// to talk to for nothing. That is the right side of the trade -- the other
	// order (enumerate, then start) drops such a peer instead of double-counting
	// it, and a peer bitswap has forgotten is worse than one it over-remembers.
	for _, p := range cfg.Host.Libp2p().Network().Peers() {
		bs.PeerConnected(p)
	}
	return &Exchange{bs: bs, mx: cfg.Metrics}, nil
}

// bitswapPeerClassifier keeps instrumentation optional even for a Host whose
// peer tracker is absent (for example a narrowly constructed test host). A
// missing tracker degrades to the bounded "other" class, never a nil receiver
// dereference or a peer-derived label.
func bitswapPeerClassifier(h *Host) func(peer.ID) string {
	if h == nil || h.peerState == nil {
		return func(peer.ID) string { return metrics.BitswapPeerOther }
	}
	return h.peerState.bitswapPeerClass
}

// NewSession opens a bitswap session that lives until ctx is done. It satisfies
// SessionSource.
//
// The session is wrapped for metrics when there are any. Wrapping here rather
// than in FetchingBlockstore is what keeps the count honest: every fetch this
// node makes goes through a session, including the follower's fetch pass and
// the document fetcher, and none of them go through the same blockstore.
func (e *Exchange) NewSession(ctx context.Context) exchange.Fetcher {
	return countedFetcher(e.bs.NewSession(ctx), e.mx)
}

type fetcherGeneration struct {
	epoch   uint64
	fetcher exchange.Fetcher
	cancel  context.CancelFunc
	refs    int
	retired bool
}

// refreshingFetcher is one logical session whose transport implementation
// rotates lazily when its source advances the epoch. A generation is
// reference-counted so refreshing never cancels a concurrent Fetcher call or
// an open GetBlocks channel. The parent context remains the lifetime bound for
// every generation.
type refreshingFetcher struct {
	ctx        context.Context
	epoch      func() uint64
	newSession func(context.Context) exchange.Fetcher

	mu     sync.Mutex
	active *fetcherGeneration
}

func (f *refreshingFetcher) acquire() (exchange.Fetcher, func()) {
	f.mu.Lock()
	epoch := f.epoch()
	if f.active == nil || f.active.epoch != epoch {
		if f.active != nil {
			f.active.retired = true
			if f.active.refs == 0 {
				f.active.cancel()
			}
		}
		ctx, cancel := context.WithCancel(f.ctx)
		f.active = &fetcherGeneration{
			epoch: epoch, fetcher: f.newSession(ctx), cancel: cancel,
		}
	}
	generation := f.active
	generation.refs++
	f.mu.Unlock()

	return generation.fetcher, func() {
		f.mu.Lock()
		generation.refs--
		if generation.retired && generation.refs == 0 {
			generation.cancel()
		}
		f.mu.Unlock()
	}
}

func (f *refreshingFetcher) GetBlock(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	inner, release := f.acquire()
	defer release()
	return inner.GetBlock(ctx, c)
}

func (f *refreshingFetcher) GetBlocks(ctx context.Context, cids []cid.Cid) (<-chan blocks.Block, error) {
	inner, release := f.acquire()
	in, err := inner.GetBlocks(ctx, cids)
	if err != nil {
		release()
		return nil, err
	}
	out := make(chan blocks.Block)
	go func() {
		defer close(out)
		defer release()
		for {
			select {
			case <-ctx.Done():
				return
			case block, ok := <-in:
				if !ok {
					return
				}
				select {
				case out <- block:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out, nil
}

// NotifyNewBlocks tells the serving half about blocks that arrived some other
// way, so it can answer peers waiting on them.
func (e *Exchange) NotifyNewBlocks(ctx context.Context, blks ...blocks.Block) error {
	return e.bs.NotifyNewBlocks(ctx, blks...)
}

// Close stops bitswap. The host outlives it; see the shutdown order in bloard.
func (e *Exchange) Close() error {
	if err := e.bs.Close(); err != nil {
		return fmt.Errorf("p2p: closing bitswap: %w", err)
	}
	return nil
}

// counted is an exchange.Fetcher that counts what it fetches (spec 11.2).
type counted struct {
	inner exchange.Fetcher
	mx    *metrics.Metrics
}

// countedFetcher wraps f if there is anything to count, and returns it
// untouched otherwise: a disabled build gets no indirection on the fetch path.
func countedFetcher(f exchange.Fetcher, mx *metrics.Metrics) exchange.Fetcher {
	if mx == nil {
		return f
	}
	return counted{inner: f, mx: mx}
}

func (c counted) GetBlock(ctx context.Context, k cid.Cid) (blocks.Block, error) {
	blk, err := c.inner.GetBlock(ctx, k)
	if err != nil {
		c.mx.BitswapFetch(false, 0)
		return nil, err
	}
	c.mx.BitswapFetch(true, len(blk.RawData()))
	return blk, nil
}

// GetBlocks counts each block as it arrives rather than counting the call.
//
// The channel is the contract: a bitswap session returns blocks as peers supply
// them and simply closes when ctx ends, so there is no error to observe and no
// moment at which the caller is told how many it did not get. Counting arrivals
// is therefore the only thing that can be counted honestly here -- a failure
// shows up as blocks the caller asked for and this counter never saw, which is
// what the follower's own retry loop reacts to as well.
func (c counted) GetBlocks(ctx context.Context, ks []cid.Cid) (<-chan blocks.Block, error) {
	in, err := c.inner.GetBlocks(ctx, ks)
	if err != nil {
		c.mx.BitswapFetch(false, 0)
		return nil, err
	}
	out := make(chan blocks.Block)
	go func() {
		defer close(out)
		for blk := range in {
			c.mx.BitswapFetch(true, len(blk.RawData()))
			select {
			case out <- blk:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out, nil
}

// SessionSource opens bitswap sessions. *Exchange is the implementation; the
// interface is here so that FetchingBlockstore can be tested against a fetcher
// that is a map, and so that nothing about the follower's blockstore depends on
// bitswap in particular.
type SessionSource interface {
	NewSession(ctx context.Context) exchange.Fetcher
}

// SessionRefresher is implemented by a SessionSource whose existing logical
// sessions can move to a fresh transport session at a discovery boundary. The
// refresh is lazy and must not cancel calls already in flight.
type SessionRefresher interface {
	SessionSource
	RefreshSessions()
}

type refreshingSessionSource struct {
	source SessionSource
	epoch  atomic.Uint64
}

// NewRefreshingSessionSource wraps a session source with an explicit discovery
// epoch. A refresh makes every logical session use a fresh transport session on
// its next request. Calls already in flight finish on their original session;
// the retired session is canceled when its final caller releases it.
//
// A Boxo session deliberately remembers peers which answered earlier wants.
// In a static topology that makes a DAG walk efficient. In a multi-writer
// follower, however, the only peer learned by a session can disappear while a
// different, globally connected writer remains unknown to that session. Boxo
// records that a peer was discovered historically, sees no current session
// peers, and suppresses the broadcast which would discover the survivor.
// Advancing this wrapper at publication-source discovery boundaries makes the
// next request rediscover the current peer set while requests between
// boundaries retain session affinity.
//
// Refresh changes no libp2p or Bitswap connection state. In particular it does
// not replay connection callbacks from a racy network snapshot.
func NewRefreshingSessionSource(source SessionSource) SessionRefresher {
	s := &refreshingSessionSource{source: source}
	s.epoch.Store(1)
	return s
}

func (s *refreshingSessionSource) NewSession(ctx context.Context) exchange.Fetcher {
	return &refreshingFetcher{ctx: ctx, epoch: s.epoch.Load, newSession: s.source.NewSession}
}

func (s *refreshingSessionSource) RefreshSessions() { s.epoch.Add(1) }

// FetchError reports a block that no peer supplied. It is deliberately not
// ipld.ErrNotFound: spec 11.4 makes the difference an HTTP status, where a
// block that resolves in the index but does not arrive is 503 (the archive
// failed to get it) and a block the index does not name is 404 (there is no
// such blob). A fetch that ran out of time unwraps to the context's error, so
// errors.Is(err, context.DeadlineExceeded) answers "did we simply not wait long
// enough".
type FetchError struct {
	Cid cid.Cid
	Err error
}

func (e *FetchError) Error() string {
	return fmt.Sprintf("p2p: fetching block %s over bitswap: %v", e.Cid, e.Err)
}

func (e *FetchError) Unwrap() error { return e.Err }

// FetchingBlockstore returns a blockstore that answers from local and fetches
// what local does not have over bitswap, writing through to local as it goes.
//
// This is the follower of spec 11.3 and 11.4, in one substitution: every read
// path in this codebase -- the head engine walking a DAG, a beacon lookup
// reaching a blob, pin reconciliation marking a root -- goes through a
// blockstore and asks it for blocks by CID. Give those paths this instead of
// the local store and they replicate over IPFS, unmodified and unaware. There
// is no other follower sync mechanism, and there does not need to be.
//
// # Which methods reach the network
//
//   - Get, Has, GetSize: local first. A local miss fetches, bounded by the
//     context passed to that call, and writes the block to local before
//     answering, so a second call is a disk read. Has and GetSize fetch because
//     there is no honest way for them not to: they answer "this node has this
//     block", and a peer saying it has one does not make that true here.
//   - Put, PutMany, DeleteBlock: local only. A write has nowhere else to go.
//   - AllKeysChan: local only, and that is a contract rather than an omission.
//     It means "the blocks this node holds". GC (spec 9) sweeps what
//     AllKeysChan enumerates minus what the pins mark, so a version of it that
//     reached the network would enumerate blocks this node does not have and
//     then try to delete them from a disk they were never on -- and would block
//     on the network to do it. There is no such set as "every block on IPFS",
//     and no caller here wants one.
//
// # Fetched blocks are cached, not pinned
//
// A fetch writes through to local and stops. Nothing pins what it wrote, so GC
// sweeps it unless some policy's pins reach it, which is spec 11.4's
// "fetched-on-demand blocks are cached but unpinned" exactly. Retention is not
// this type's job and must not become it: what a follower keeps is decided by
// pin reconciliation running over this blockstore, where a recursive pin walks
// a DAG, misses locally on every block, and thereby fetches and caches the
// blocks the policy says to hold. Backfill and retention are the same act.
//
// # The two contexts
//
// ctx is the bitswap session's lifetime -- the span over which this node
// fetches at all, which for a daemon is the daemon. The context passed to each
// Get/Has/GetSize is the deadline on that one fetch, which is where
// follow.fetch_timeout (spec 11.4) belongs. They are not the same knob and must
// not be wired to the same value: one session is what makes a DAG walk cheap
// (it learns which peers answer and stops asking the ones that do not), and a
// session that died with the first read would relearn that on every block.
func FetchingBlockstore(ctx context.Context, local blockstore.Blockstore, src SessionSource) blockstore.Blockstore {
	return &fetchingBlockstore{local: local, src: src, ctx: ctx}
}

type fetchingBlockstore struct {
	local blockstore.Blockstore
	src   SessionSource

	// ctx is the session's lifetime; see FetchingBlockstore.
	ctx  context.Context
	once sync.Once
	sess exchange.Fetcher
}

var _ blockstore.Blockstore = (*fetchingBlockstore)(nil)

// session opens the shared session on first use. Lazily, so that a node which
// never misses never opens one.
func (f *fetchingBlockstore) session() exchange.Fetcher {
	f.once.Do(func() { f.sess = f.src.NewSession(f.ctx) })
	return f.sess
}

// fetch gets c from a peer and caches it locally.
//
// A failed local write is reported rather than swallowed: the block is in hand
// and could be returned, but a blockstore that answers Get and does not
// remember is one where every later Has still says no, and the caller would
// re-fetch the same block forever without ever being told why.
func (f *fetchingBlockstore) fetch(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	b, err := f.session().GetBlock(ctx, c)
	if err != nil {
		return nil, &FetchError{Cid: c, Err: err}
	}
	if err := f.local.Put(ctx, b); err != nil {
		return nil, fmt.Errorf("p2p: caching fetched block %s: %w", c, err)
	}
	return b, nil
}

func (f *fetchingBlockstore) Get(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	b, err := f.local.Get(ctx, c)
	if err == nil || !ipld.IsNotFound(err) {
		return b, err
	}
	return f.fetch(ctx, c)
}

func (f *fetchingBlockstore) Has(ctx context.Context, c cid.Cid) (bool, error) {
	has, err := f.local.Has(ctx, c)
	if err != nil || has {
		return has, err
	}
	if _, err := f.fetch(ctx, c); err != nil {
		return false, err
	}
	return true, nil
}

func (f *fetchingBlockstore) GetSize(ctx context.Context, c cid.Cid) (int, error) {
	size, err := f.local.GetSize(ctx, c)
	if err == nil || !ipld.IsNotFound(err) {
		return size, err
	}
	b, err := f.fetch(ctx, c)
	if err != nil {
		return 0, err
	}
	return len(b.RawData()), nil
}

func (f *fetchingBlockstore) Put(ctx context.Context, b blocks.Block) error {
	return f.local.Put(ctx, b)
}

func (f *fetchingBlockstore) PutMany(ctx context.Context, bs []blocks.Block) error {
	return f.local.PutMany(ctx, bs)
}

func (f *fetchingBlockstore) DeleteBlock(ctx context.Context, c cid.Cid) error {
	return f.local.DeleteBlock(ctx, c)
}

// AllKeysChan is local-only. See FetchingBlockstore.
func (f *fetchingBlockstore) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	return f.local.AllKeysChan(ctx)
}
