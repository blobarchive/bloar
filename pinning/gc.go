package pinning

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/ipfs/boxo/blockstore"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	"github.com/ipld/go-ipld-prime/datamodel"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"

	"github.com/blobarchive/bloar/catalog"
	"github.com/blobarchive/bloar/metrics"
	"github.com/blobarchive/bloar/store"
)

// BlockFetcher makes a block the mark reached but the local store does not have
// present. It is the follower's fetching blockstore (p2p.FetchingBlockstore, as
// follow wires it): a Get fetches over bitswap and writes the block through to
// the local store the sweep enumerates. GC only ever reads through it, so the
// seam is one method rather than a whole blockstore.
type BlockFetcher interface {
	Get(ctx context.Context, c cid.Cid) (blocks.Block, error)
}

// GCConfig is what a GC needs.
type GCConfig struct {
	// Blocks is the legacy offline blockstore to sweep. Set this only when no
	// Epochs coordinator is available; the whole run then holds Gate.
	Blocks blockstore.Blockstore
	// Epochs enables online collection. The daemon passes its store coordinator;
	// GC then holds Gate only long enough to flush reconciliation, expire staging
	// rows, snapshot pins, and start an epoch. Mark and sweep run alongside
	// application traffic. Blocks remains the compatibility path for offline
	// tools and focused tests; exactly one of Blocks or Epochs is required.
	Epochs *store.BlockstoreEpochs
	// SeparateScrub makes the reachability mark validate dag-cbor index nodes
	// but use a presence check for raw leaves. A separately scheduled Scrub then
	// CID-validates every stored block without holding the writer gate. This
	// removes multi-terabyte raw hashing from the retention-critical GC pass
	// without weakening the store-integrity audit.
	SeparateScrub bool
	// Reconciler names the heads whose pins are the mark set, provides the
	// ledger those pins live in, and owns the gate GC excludes everything with.
	// Required.
	Reconciler *Reconciler
	// Staging is the staging pins of spec 9's window (a). Optional; nil is a
	// node whose ingest does not stage, and whose runs therefore have neither
	// staging pins to mark nor expired ones to drop.
	Staging *Staging
	// Fetch resolves the self-heal fetcher for a head's missing pinned block, or
	// nil to fail closed for that head (spec 9's follower self-heal, scoped per
	// head). A followed head resolves to the follower's fetching blockstore -- a
	// block its pin names and the store does not have is a dangling pin the fetch
	// pass window can leave, and a fetch-capable node repairs it instead of
	// wedging. A written head resolves to nil, where a missing pinned block is
	// real divergence, not a block to go and get. Optional; nil is a pure writer,
	// every head fail-closed. The per-head scope is what keeps a node that both
	// writes and follows fail-closed on the heads it writes -- a global fetcher
	// would heal a written head's real corruption and mask it as a refetch
	// (follow.Follower.GCFetch builds the resolver from what the node follows).
	Fetch func(head string) BlockFetcher
	// Metrics instruments each run. Optional; nil records nothing.
	Metrics *metrics.Metrics
	// KVCensus, if set, is invoked after each SUCCESSFUL run to refresh the
	// store-growth KV-prefix gauges (the store-growth observability work). It runs after a successful GC,
	// outside the short snapshot gate, and is the only place those O(n) key counts happen -- never on a
	// scrape. Optional; nil publishes none.
	KVCensus func()
	// Logger receives what a run has to say. Optional.
	Logger *slog.Logger
}

// GC is the mark-and-sweep collector of spec 9.
//
// # Why this is hand-rolled
//
// Spec 9 says "boxo offline GC". There is no such thing to call: boxo v0.41 has
// no GC package (the implementation the phrase refers to lives in kubo, over a
// full IPFS node), and its pinner interface -- the input such a GC would want --
// is a pin database with its own datastore-backed indexes. Adopting it would
// mean keeping the pin state twice, in the ledger of spec 6.2 and in a
// dspinner, and a GC that swept from the second while reconciliation wrote the
// first would collect live blocks the moment the two disagreed. The ledger is
// the pin state; a collector that marks from it is a queue and a decoder, and
// that is what this is.
type GC struct {
	blocks        blockstore.Blockstore
	epochs        *store.BlockstoreEpochs
	separateScrub bool
	rec           *Reconciler
	staging       *Staging
	fetchFor      func(head string) BlockFetcher
	mx            *metrics.Metrics
	kvCensus      func()
	log           *slog.Logger
	// maintenance serializes reachability GC and integrity scrub. A channel is
	// used instead of sync.Mutex so a cancelled scheduled run can stop waiting.
	maintenance chan struct{}

	// refetched counts the blocks the current run's mark had to fetch to repair
	// a dangling pin. It is run-scoped: reset in run and read out before sweep.
	// Maintenance serialization prevents GC/scrub overlap; the online mark and
	// sweep do not hold Gate exclusively, so that gate is not the synchronization.
	refetched int

	// validatedRawReads/validatedNodeReads count the mark's SUCCESSFUL validating Gets
	// split by codec, and validatedRawBytes/validatedNodeBytes sum the bytes those
	// reads hashed. The counts are the read-amplification signal; the BYTES are the
	// cost signal, because CID validation hashes the whole block and a dag-cbor node is
	// NOT a fixed size -- a sealed Segment can be hundreds of KiB (spec 12.1), far more
	// than a leaf, so a per-node count cannot be turned into a time. Neither is the
	// marked set: a block direct-pinned under several heads is one marked entry but one
	// read (and its bytes) PER head, before the shared set dedups, and staging adds its
	// own rows. Run-scoped like refetched. Only successful reads are counted (see
	// readForMark): a run whose mark fails returns no stats at all.
	validatedRawReads  int
	validatedNodeReads int
	validatedRawBytes  int64
	validatedNodeBytes int64
}

// GCStats reports one run.
type GCStats struct {
	// Pins is the ledger rows the mark started from, across every head.
	Pins int
	// Marked is the distinct blocks reachable from them.
	Marked int
	// Scanned is the blocks the sweep looked at, Swept the ones it deleted.
	Scanned int
	Swept   int
	// Protected is the number of distinct multihashes touched by successful
	// application operations during the online epoch. ProtectedSkips is the
	// subset encountered as otherwise-deletable sweep candidates. Both are zero
	// on the legacy offline path.
	Protected      int
	ProtectedSkips int
	// Staged is the staging rows that survived the expiry pass and were marked;
	// Expired is the ones that did not (spec 9's window (a)).
	Staged  int
	Expired int
	// Refetched is the missing pinned blocks the mark fetched to repair rather
	// than fail on (spec 9's follower self-heal). Only a followed head's misses
	// are counted here: a written head fails closed on a missing pinned block, so
	// its real divergence is never quietly refetched. A nonzero rate is dangling
	// pins being created under a followed head, which an operator should look
	// into.
	Refetched int
	// ValidatedRawReads and ValidatedNodeReads are the mark's successful validating
	// Gets this run split by codec, ValidatedReads their sum; ValidatedRawBytes and
	// ValidatedNodeBytes are the bytes those reads hashed, ValidatedBytes their sum
	//. The COUNTS are the read-amplification signal (how many
	// times the mark re-reads shared direct pins); the BYTES are the cost signal, and
	// the capacity bound is linear in them: CID validation hashes the whole block and a
	// dag-cbor node is not a fixed size -- a sealed Segment can be hundreds of KiB (spec
	// 12.1), so a per-node count cannot be turned into a time. Size mark cost as
	// ValidatedBytes / the host's sha2-256 throughput, split raw vs node where the
	// per-read framing overhead matters. None of these is Marked: a block direct-pinned
	// under N heads is one Marked entry but N reads (and N x its bytes), each head's row
	// read before the shared set dedups, and staging rows add their own. These are
	// successful-run statistics -- a run whose mark fails returns GCStats{}.
	ValidatedReads     int
	ValidatedRawReads  int
	ValidatedNodeReads int
	ValidatedBytes     int64
	ValidatedRawBytes  int64
	ValidatedNodeBytes int64
	// Duration is wall clock, including the reconciliation flush.
	Duration time.Duration

	// stagingObserved distinguishes a real zero-row snapshot from a prepare
	// failure before pins() completed. It is internal so the public statistics
	// remain the stable, nonnegative values operators expect.
	stagingObserved bool
}

// ScrubStats reports one complete store-integrity pass. A scrub validates the
// CID of every object it observes, deletes nothing, and returns at the first
// corrupt or unreadable block.
type ScrubStats struct {
	Scanned  int
	Bytes    int64
	Duration time.Duration
}

// NewGC returns a GC over cfg.
func NewGC(cfg GCConfig) (*GC, error) {
	if cfg.Blocks == nil && cfg.Epochs == nil {
		return nil, errors.New("pinning: exactly one of GCConfig.Blocks or GCConfig.Epochs must be set")
	}
	if cfg.Blocks != nil && cfg.Epochs != nil {
		return nil, errors.New("pinning: GCConfig.Blocks and GCConfig.Epochs are mutually exclusive")
	}
	if cfg.SeparateScrub && cfg.Epochs == nil {
		return nil, errors.New("pinning: GCConfig.SeparateScrub requires GCConfig.Epochs")
	}
	if cfg.Epochs != nil && !cfg.Epochs.CompleteEnumeration() {
		return nil, errors.New("pinning: GCConfig.Epochs lacks complete error-preserving block enumeration")
	}
	if cfg.Reconciler == nil {
		return nil, errors.New("pinning: GCConfig.Reconciler must not be nil")
	}
	blocks := cfg.Blocks
	if cfg.Epochs != nil {
		blocks = cfg.Epochs.CollectorBlocks()
	}
	g := &GC{
		blocks: blocks, epochs: cfg.Epochs, separateScrub: cfg.SeparateScrub,
		rec: cfg.Reconciler, staging: cfg.Staging, fetchFor: cfg.Fetch,
		mx: cfg.Metrics, kvCensus: cfg.KVCensus, log: cfg.Logger,
		maintenance: make(chan struct{}, 1),
	}
	if g.log == nil {
		g.log = slog.New(slog.DiscardHandler)
	}
	return g, nil
}

// RunEvery runs GC on a schedule until ctx is cancelled, on which it returns
// nil. Interval is spec 12's store.gc_interval. A failed run is logged and the
// schedule continues: GC is maintenance, and a daemon that exits because a
// sweep failed has turned a disk-space problem into an outage.
func (g *GC) RunEvery(ctx context.Context, interval time.Duration) error {
	if interval <= 0 {
		return fmt.Errorf("pinning: gc interval is %s, must be positive", interval)
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
		_, err := g.Run(ctx)
		switch {
		case ctx.Err() != nil:
			return nil
		case err != nil:
			// Run logs the phase and error with any partial statistics. The
			// scheduler deliberately continues after a failed maintenance pass.
		}
	}
}

// Run performs one mark-and-sweep. With Epochs configured (the daemon path), it
// takes a short consistent pin cut and then collects online; with only Blocks,
// it preserves the legacy whole-run exclusion for offline callers and tests.
// The daemon's SIGUSR1 operator trigger calls this same method.
//
// It briefly excludes publication and ingest (spec 9, Gate) and then flushes
// reconciliation before snapshotting pins and activating a storage epoch. The
// gate is released before mark and sweep. The flush is not belt-and-braces: the
// mark set is the ledger, the ledger is only current if reconciliation has run
// since the last root swap, and reconciliation is asynchronous. Without it, a
// GC that started right after an apply would mark from the previous root's pins
// and sweep the new root's spine -- blocks a published root points at. Owning
// the flush is also how "GC must not run concurrently with reconciliation"
// stops being a race to avoid and becomes an order to follow.
//
// # The ingest window
//
// A blob is written by POST /bloar/v1/blobs and becomes reachable only when the
// refs that name it are applied (spec 7.2's second call). The gate keeps a run
// from starting mid-request, but the gap between the two requests outlives both
// of them, and a GC landing in it used to sweep the blob -- spec 9's known
// window (a).
//
// The staging pins close it. Ingest pins every blob it accepts under the
// reserved staging head before it answers (see Staging), so a blob is in the
// mark set from the moment the put returns until the refs that name it land.
// This is what makes the ordering below matter:
//
//  1. Expire first. A staging row past its TTL is an abandoned put, and its
//     blobs should be swept by this run rather than the next one. Doing it
//     before the mark rather than after is what makes "expired" and "swept" the
//     same run, so an operator watching the disk sees the space come back when
//     the TTL says it should.
//  2. Mark from every row that is left, staging included. A staging pin is a
//     direct pin on a leaf (blobs have no links, spec 2), so it costs one mark
//     set entry and no traversal.
//
// The expiry pass and epoch cut run inside the gate, so they cannot race an
// ingest extending a row's expiry: the put either finishes before T0 and its
// staging row is in the snapshot, or starts after T0 and its block write is
// protected by the active epoch.
func (g *GC) Run(ctx context.Context) (GCStats, error) {
	start := time.Now()
	if err := g.acquireMaintenance(ctx); err != nil {
		return GCStats{}, err
	}
	defer g.releaseMaintenance()
	g.mx.GCActive(true)
	g.mx.GCPhase("")
	defer func() {
		g.mx.GCPhase("")
		g.mx.GCActive(false)
	}()
	g.log.Info("gc started", "online", g.epochs != nil)

	var stats GCStats
	var err error
	if g.epochs == nil {
		stats, err = g.runOffline(ctx)
	} else {
		stats, err = g.runOnline(ctx)
	}
	stats.Duration = time.Since(start)
	g.mx.GCRun(err == nil, stats.Marked, stats.Swept, stats.Refetched, stats.Duration)
	// A prepare/mark failure never started a sweep and must not erase the last
	// useful sweep progress. Once enumeration started, the gauges intentionally
	// describe the current/most recent attempt, including a partial failed one.
	if err == nil || stats.Scanned > 0 {
		g.mx.GCObserved(stats.Scanned, stats.Protected, stats.ProtectedSkips)
	}
	if stats.stagingObserved {
		g.mx.StagingPins(stats.Staged)
	}
	g.mx.StagingExpired(stats.Expired)
	// Store-growth gauges (the store-growth observability work), published only on a completed run. The
	// scanned-minus-swept is the last online sweep's observed remaining count;
	// concurrent additions may not enter its non-snapshot enumeration, so it is
	// intentionally a trend rather than an exact live census. The KV census reads
	// current prefix counts. A failed run left partial or zero stats, so the last
	// good values are kept rather than overwritten.
	if err == nil {
		g.mx.StoreBlocks(stats.Scanned - stats.Swept)
		if g.kvCensus != nil {
			endPhase := g.beginPhase(metrics.GCPhaseCensus)
			g.kvCensus()
			endPhase(nil)
		}
		g.log.Info("gc", "event", "completed", "swept", stats.Swept, "marked", stats.Marked,
			"scanned", stats.Scanned, "protected", stats.Protected,
			"protected_skips", stats.ProtectedSkips, "pins", stats.Pins,
			"staged", stats.Staged, "expired", stats.Expired, "refetched", stats.Refetched,
			"validated_reads", stats.ValidatedReads, "validated_raw_reads", stats.ValidatedRawReads,
			"validated_node_reads", stats.ValidatedNodeReads, "validated_bytes", stats.ValidatedBytes,
			"validated_raw_bytes", stats.ValidatedRawBytes, "validated_node_bytes", stats.ValidatedNodeBytes,
			"took", stats.Duration)
	} else {
		g.log.Error("gc", "event", "failed", "err", err, "scanned", stats.Scanned, "swept", stats.Swept,
			"protected", stats.Protected, "protected_skips", stats.ProtectedSkips, "took", stats.Duration)
	}
	return stats, err
}

// runOffline preserves the original whole-run exclusion for callers that pass
// only Blocks. Production uses runOnline; this path keeps offline tools and
// focused tests fail-safe rather than silently making an uncoordinated sweep.
func (g *GC) runOffline(ctx context.Context) (GCStats, error) {
	release := g.rec.gate.exclude()
	defer release()
	endPrepare := g.beginPhase(metrics.GCPhasePrepare)
	groups, npins, staged, expired, err := g.prepare(ctx)
	endPrepare(err)
	if err != nil {
		return GCStats{Expired: expired}, err
	}
	endMark := g.beginPhase(metrics.GCPhaseMark)
	marked, err := g.mark(ctx, groups)
	endMark(err)
	if err != nil {
		return GCStats{Pins: npins, Staged: staged, Expired: expired, stagingObserved: true}, err
	}
	endSweep := g.beginPhase(metrics.GCPhaseSweep)
	stats, err := g.sweep(ctx, marked)
	endSweep(err)
	g.finishStats(&stats, npins, staged, expired, marked)
	return stats, err
}

// runOnline holds Gate only across the T0 consistency cut: reconciliation,
// staging expiry, the pin snapshot, and epoch activation. Mark builds M from
// that T0 snapshot while traffic builds T through the application blockstore;
// sweep retains M union T. Candidate deletion rechecks T under the same per-key
// shard lock used by application operations. runOffline is the legacy fallback
// which holds Gate across prepare, mark, and sweep and therefore needs no T.
func (g *GC) runOnline(ctx context.Context) (GCStats, error) {
	endPrepare := g.beginPhase(metrics.GCPhasePrepare)
	release := g.rec.gate.exclude()
	groups, npins, staged, expired, err := g.prepare(ctx)
	var epoch *store.BlockstoreEpoch
	if err == nil {
		epoch, err = g.epochs.Begin()
		if err != nil {
			err = fmt.Errorf("pinning: gc: beginning protection epoch: %w", err)
		}
	}
	release()
	endPrepare(err)
	if err != nil {
		return GCStats{Expired: expired}, err
	}

	endMark := g.beginPhase(metrics.GCPhaseMark)
	marked, err := g.mark(ctx, groups)
	endMark(err)
	if err != nil {
		return GCStats{Pins: npins, Staged: staged, Expired: expired, Protected: epoch.End(), stagingObserved: true}, err
	}

	endSweep := g.beginPhase(metrics.GCPhaseSweep)
	stats, err := g.sweepOnline(ctx, epoch, marked)
	stats.Protected = epoch.End()
	endSweep(err)
	g.finishStats(&stats, npins, staged, expired, marked)
	return stats, err
}

// prepare takes the stable pin snapshot from which a run's mark begins. The
// caller holds Gate exclusively.
func (g *GC) prepare(ctx context.Context) (groups []headMark, npins, staged, expired int, err error) {
	g.refetched, g.validatedRawReads, g.validatedNodeReads = 0, 0, 0
	g.validatedRawBytes, g.validatedNodeBytes = 0, 0
	if _, err := g.rec.reconcileAll(ctx); err != nil {
		return nil, 0, 0, 0, fmt.Errorf("pinning: gc: flushing pin reconciliation: %w", err)
	}
	expired, err = g.expireStaging(ctx)
	if err != nil {
		return nil, 0, 0, 0, err
	}
	groups, npins, staged, err = g.pins(ctx)
	if err != nil {
		return nil, 0, 0, expired, err
	}
	return groups, npins, staged, expired, nil
}

func (g *GC) finishStats(stats *GCStats, npins, staged, expired int, marked map[string]struct{}) {
	stats.Pins, stats.Marked = npins, len(marked)
	stats.Staged, stats.Expired = staged, expired
	stats.stagingObserved = true
	stats.Refetched = g.refetched
	stats.ValidatedRawReads, stats.ValidatedNodeReads = g.validatedRawReads, g.validatedNodeReads
	stats.ValidatedReads = g.validatedRawReads + g.validatedNodeReads
	stats.ValidatedRawBytes, stats.ValidatedNodeBytes = g.validatedRawBytes, g.validatedNodeBytes
	stats.ValidatedBytes = g.validatedRawBytes + g.validatedNodeBytes
}

func (g *GC) acquireMaintenance(ctx context.Context) error {
	select {
	case g.maintenance <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (g *GC) releaseMaintenance() { <-g.maintenance }

// beginPhase selects a bounded metrics label, emits lifecycle logs, and
// returns the corresponding completion hook.
func (g *GC) beginPhase(phase string) func(error) {
	start := time.Now()
	g.mx.GCPhase(phase)
	g.log.Info("gc phase started", "phase", phase)
	return func(err error) {
		d := time.Since(start)
		g.mx.GCPhaseDuration(phase, d)
		if err != nil {
			g.log.Error("gc phase failed", "phase", phase, "err", err, "took", d)
			return
		}
		g.log.Info("gc phase completed", "phase", phase, "took", d)
	}
}

// expireStaging drops the staging rows whose TTL has passed. The caller holds
// the gate.
func (g *GC) expireStaging(ctx context.Context) (int, error) {
	if g.staging == nil {
		return 0, nil
	}
	n, err := g.staging.DropExpired(ctx)
	if err != nil {
		return 0, fmt.Errorf("pinning: gc: expiring staging pins: %w", err)
	}
	if n > 0 {
		// Loud on purpose. Every one of these is a put an indexer was told had
		// succeeded and then never referenced, which means an indexer crashed
		// between two requests, or is broken.
		g.log.Warn("gc: dropped expired staging pins; these blobs were ingested and never referenced",
			"pins", n, "ttl", g.staging.TTL())
	}
	return n, nil
}

// headMark is one head's ledger rows paired with the fetcher a miss under it may
// heal through: the follower's blockstore for a followed head, nil for a written
// one (spec 9's per-head self-heal scope). The mark walks these groups in the
// order pins returns them -- written heads first -- so a block shared with a
// followed head is verified under the written head's fail-closed rule before any
// followed-head walk could fetch it. See mark.
type headMark struct {
	head  string
	fetch BlockFetcher
	pins  []catalog.PinEntry
}

// pins groups every registered head's ledger rows with the fetcher that head's
// misses may heal through, and appends the staging rows that survived the expiry
// pass. It returns the groups (written heads first, then followed), the total
// pin count, and, for the stats, how many were staging.
//
// The written-before-followed order is load-bearing, not cosmetic: it is what
// makes the shared-block reasoning in mark hold. Within each of the two the
// heads keep g.rec.Names()'s sorted order, so a failure blames the same head
// every run.
//
// Rows of a head that is no longer registered are not in it, and so are not
// marked: a head removed from the config stops retaining its blocks, and its
// rows linger like catalog entries do (spec 6.1) until a rebuild clears them.
// The alternative -- retaining forever for a head nobody serves -- is the worse
// of the two, and neither is silent: the rows are still there to be read.
//
// The staging rows are the exception to that rule, and have to be: they are
// filed under a name that is not a head and never appears in Names(), so
// reading them is a separate step rather than a loop iteration. Forgetting it
// would sweep every blob that has been put but not yet referenced. They carry a
// nil fetch: a staging pin is a direct pin on a blob, so it is read-checked like
// any direct pin (a missing staged block fails the run closed -- ingest stored
// and pinned it, so its absence is real divergence, not a block to go and get),
// and are appended last so they do not disturb the written-before-followed order
// above.
func (g *GC) pins(ctx context.Context) (groups []headMark, total int, staged int, err error) {
	var written, followed []headMark
	for _, name := range g.rec.Names() {
		entries, err := g.rec.ledger.ListAll(ctx, name)
		if err != nil {
			return nil, 0, 0, fmt.Errorf("pinning: gc: reading pins of head %q: %w", name, err)
		}
		total += len(entries)
		grp := headMark{head: name, pins: entries}
		if g.fetchFor != nil {
			grp.fetch = g.fetchFor(name)
		}
		if grp.fetch == nil {
			written = append(written, grp)
		} else {
			followed = append(followed, grp)
		}
	}
	groups = append(written, followed...)
	if g.staging != nil {
		entries, err := g.staging.List(ctx)
		if err != nil {
			return nil, 0, 0, fmt.Errorf("pinning: gc: reading staging pins: %w", err)
		}
		groups = append(groups, headMark{head: StagingHead, pins: entries})
		total += len(entries)
		staged = len(entries)
	}
	return groups, total, staged, nil
}

// markKey is a block's identity for marking: its multihash, not its CID.
//
// The blockstore is keyed by multihash, so AllKeysChan reports every block
// under a raw-codec CID whatever it actually holds (boxo blockstore.go). A mark
// set keyed by CID would therefore match nothing on the sweep side for the
// dag-cbor half of the DAG, and quietly delete the whole index.
func markKey(c cid.Cid) string { return string(c.Hash()) }

// mark walks the DAG from the pins and returns every block reachable under
// them. A direct pin marks and reads exactly one block; a recursive pin marks
// everything its links reach.
//
// Every index block the mark keeps is CID-validated before it is marked. When
// SeparateScrub is disabled, raw leaves are validated here too (the legacy
// behavior); when enabled, raw leaves are presence-checked and the scheduled
// full-store scrub owns their byte validation. A missing retained block still
// fails the whole run
// rather than being skipped -- with one exception: a block that is genuinely
// ABSENT (not corrupt) under a FOLLOWED head is refetched, making it durable, and
// the walk carries on (see readForMark). A present-but-corrupt block fails the run
// under every head and is repaired out of band (bloard fsck), never healed here.
// Under a written head the disagreement between the ledger and the blockstore is
// real, and the one thing a collector must not do with a DAG it cannot fully read
// is delete the parts of it that are left.
//
// # Per-head walk order, and the shared block
//
// The walk is one accumulator (marked, expanded) drained per head, in the order
// pins returns the groups: every written head first, then every followed one. A
// block reachable from two heads is visited once -- whichever head's walk
// expands it first wins, and the other finds it in expanded and skips it -- so
// which head's fetch rule applies to a shared block is decided by that order.
//
// Written-first is what keeps the divergence signal honest. Take a block shared
// (by multihash) between a written head W and a followed head F, missing
// locally:
//
//   - If it is reachable from W, W's walk reaches it first, under a nil fetch,
//     and fails the run closed naming W. That is correct: W's own store has lost
//     a block W pins, which is real divergence an operator must see, whatever
//     other head happens to share it.
//   - If it is reachable only from F, W never touches it; F's walk heals it and
//     the run continues. The byte content is identical by CID, so a heal through
//     F restores exactly the block W would have wanted too.
//
// The one thing the order forbids is F healing a block before W verifies it: if
// F ran first it would write the block through, mark it expanded, and W's later
// walk would skip it -- laundering W's miss into a silent refetch. Draining every
// written head before any followed one makes that unreachable, because a miss on
// any written-head path aborts the whole run before a followed head is walked.
func (g *GC) mark(ctx context.Context, groups []headMark) (map[string]struct{}, error) {
	marked := make(map[string]struct{})
	expanded := make(map[string]struct{})
	for _, grp := range groups {
		if err := g.markHead(ctx, grp, marked, expanded); err != nil {
			return nil, err
		}
	}
	return marked, nil
}

// markHead drains one head's frontier into the shared marked and expanded sets,
// healing a miss through grp.fetch or failing closed when it is nil. The caller
// walks written heads before followed ones; see mark.
func (g *GC) markHead(ctx context.Context, grp headMark, marked, expanded map[string]struct{}) error {
	var frontier []cid.Cid
	for _, p := range grp.pins {
		if p.Recursive {
			// A recursive pin's root is read and expanded by the frontier loop below
			// -- the root block, then everything its links reach -- so it is not read
			// here as well, only queued.
			frontier = append(frontier, p.CID)
			continue
		}
		// A direct pin retains exactly its one block and none of its descendants
		// (ModeNone semantics: an index block kept without its blob children, spec
		// 9). It is still READ here, through the same fail-closed/self-heal path a
		// walked block takes -- a written head fails closed on an unreadable direct
		// target, a followed head refetches one that is genuinely absent and counts
		// it (a corrupt one fails the run under either head; see readForMark) -- so a
		// direct pin whose block is gone is no longer silently marked present (audit
		// the safety boundary). Its links are never enqueued: that is the whole of the
		// direct/recursive distinction.
		if err := g.retainForMark(ctx, p.CID, grp.head, grp.fetch); err != nil {
			return err
		}
		marked[markKey(p.CID)] = struct{}{}
	}

	visited := 0
	nextProgress := time.Now().Add(time.Minute)
	for len(frontier) > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		c := frontier[len(frontier)-1]
		frontier = frontier[:len(frontier)-1]
		visited++
		// Raw blocks are leaves, so the multihash-keyed marked set is also their
		// visited set. This avoids a second map entry for each of the archive's
		// tens of millions of blobs. DAG-CBOR keeps the CID-keyed expanded set
		// because codec controls how identical bytes are interpreted.
		if c.Prefix().Codec == cid.Raw {
			key := markKey(c)
			if _, done := marked[key]; done {
				continue
			}
			if err := g.retainForMark(ctx, c, grp.head, grp.fetch); err != nil {
				return err
			}
			marked[key] = struct{}{}
			if visited%8192 == 0 && time.Now().After(nextProgress) {
				g.log.Info("gc progress", "phase", metrics.GCPhaseMark, "head", grp.head,
					"marked", len(marked), "frontier", len(frontier))
				nextProgress = time.Now().Add(time.Minute)
			}
			continue
		}
		// Keyed by CID, not multihash: what a block's links are depends on how
		// its bytes are read, which is what the codec says.
		if _, done := expanded[c.KeyString()]; done {
			continue
		}
		expanded[c.KeyString()] = struct{}{}
		marked[markKey(c)] = struct{}{}

		kids, err := g.links(ctx, c, grp.head, grp.fetch)
		if err != nil {
			return err
		}
		frontier = append(frontier, kids...)
		if visited%8192 == 0 && time.Now().After(nextProgress) {
			g.log.Info("gc progress", "phase", metrics.GCPhaseMark, "head", grp.head,
				"marked", len(marked), "frontier", len(frontier))
			nextProgress = time.Now().Add(time.Minute)
		}
	}
	return nil
}

// links returns the CIDs a block links to.
//
// The links are read out of the data model, not out of a schema: any link
// anywhere in a dag-cbor block is a link, whether or not this build knows what
// the block means. A collector that switched on node types would sweep the
// children of anything it failed to recognise -- a block from a future schema
// version, a shape a bug produced -- and switching is unnecessary besides, since
// spec 2 requires every reference in the DAG to be a real IPLD link precisely
// so that a generic traversal finds all of them.
func (g *GC) links(ctx context.Context, c cid.Cid, head string, fetch BlockFetcher) ([]cid.Cid, error) {
	switch codec := c.Prefix().Codec; codec {
	case cid.Raw:
		// A raw blob is a leaf: it has no links (spec 2). But it is still READ here,
		// through the same fail-closed/self-heal path an index node takes, so a raw
		// leaf a recursive pin's walk reaches that is gone is caught -- a written head
		// fails closed, a followed head refetches one that is genuinely absent (a
		// corrupt one fails the run under either head; see readForMark) -- rather than
		// marked terminal unread. Only its readability matters; a leaf
		// has no links, so the block itself is discarded.
		if err := g.retainForMark(ctx, c, head, fetch); err != nil {
			return nil, err
		}
		return nil, nil
	case cid.DagCBOR:
	default:
		return nil, fmt.Errorf("pinning: gc: block %s has codec 0x%x; bloar's DAG is raw blobs and dag-cbor index nodes (spec 2)",
			c, codec)
	}

	blk, err := g.readForMark(ctx, c, head, fetch)
	if err != nil {
		return nil, err
	}
	nb := basicnode.Prototype.Any.NewBuilder()
	if err := dagcbor.Decode(nb, bytes.NewReader(blk.RawData())); err != nil {
		return nil, fmt.Errorf("pinning: gc: decoding block %s: %w", c, err)
	}
	var out []cid.Cid
	if err := appendLinks(nb.Build(), &out); err != nil {
		return nil, fmt.Errorf("pinning: gc: reading links of block %s: %w", c, err)
	}
	return out, nil
}

// retainForMark proves that a marked target exists. With a separate integrity
// scrub, raw leaves need only a presence check during reachability GC: they
// cannot contain links, and the scrub owns full-byte CID validation. DAG-CBOR
// nodes still need a validating Get because their links define the closure.
// The role-scoped follower heal remains identical on an absent raw block.
func (g *GC) retainForMark(ctx context.Context, c cid.Cid, head string, fetch BlockFetcher) error {
	if !g.separateScrub || c.Prefix().Codec != cid.Raw {
		_, err := g.readForMark(ctx, c, head, fetch)
		return err
	}
	present, err := g.blocks.Has(ctx, c)
	if err != nil {
		return fmt.Errorf("pinning: gc: checking pinned block %s under head %q: %w", c, head, err)
	}
	if present {
		return nil
	}
	if fetch == nil {
		return fmt.Errorf("pinning: gc: pinned block %s under head %q is absent locally", c, head)
	}
	if _, err := fetch.Get(ctx, c); err != nil {
		return fmt.Errorf("pinning: gc: pinned block %s under head %q is absent locally and could not be refetched: %w",
			c, head, err)
	}
	g.refetched++
	return nil
}

// readForMark reads a block the mark reached under head, healing a dangling pin
// through fetch (spec 9's follower self-heal) when head is followed, or failing
// closed when fetch is nil.
//
// A follower's fetch pass makes blocks durable before the ledger pins that
// retain them land (spec 11.3), and a GC in that window can sweep one an
// adoption is about to pin -- leaving a recursive pin whose descendant is gone.
// Under a followed head (fetch set) the mark fetches the block back, the same
// fetch the pass would have made, and the next run finds it present. Under a
// written head (fetch nil) a missing pinned block is real divergence, so the
// read stays fail-closed and the error names the head, so an operator sees which
// head's store lost the block. fetch is scoped per head, not per node: a node
// that both writes and follows heals its followed heads and fails closed on the
// heads it writes.
//
// Every marked block routes through this validating read when SeparateScrub is
// false. In the production split, every dag-cbor node still comes here because
// its links define reachability, while raw leaves route through retainForMark's
// Has check and get their full-byte validation from Scrub. Missing blobs retain
// the same written-head fail-closed and followed-head self-heal behavior in
// either mode. A direct pin's descendants are never enqueued -- ModeNone keeps
// the one block, not its children.
//
// The Get is the store's validating read, so the heal covers ONE
// class only: a block that is genuinely absent (ipld.NotFound). A block that is
// present but CORRUPT (its bytes no longer hash to its CID) fails the read with
// ErrCorruptBlock, which is not NotFound, so the fetch path below is not taken --
// corruption fails the run for a followed head exactly as for a written one. That
// is deliberate: the fetcher would overwrite the local corrupt block from a peer
// and mask it, so corruption is repaired out of band by an explicit delete and
// refetch (bloard fsck, see docs/operations.md), never silently during the mark.
// There is no size-only shortcut and no separate corruption registry; the mark
// reads the bytes it retains.
func (g *GC) readForMark(ctx context.Context, c cid.Cid, head string, fetch BlockFetcher) (blocks.Block, error) {
	blk, err := g.blocks.Get(ctx, c)
	if err == nil {
		// A successful validating Get: one sha2-256 over the WHOLE block. Tally the
		// read and its byte length by codec -- the bytes are the cost signal, since a
		// dag-cbor node's size is not fixed (a sealed Segment can be hundreds of KiB).
		// Only successful reads are counted -- a corrupt or absent read below aborts the
		// mark, and a failed run returns GCStats{}, so nothing from it is ever surfaced;
		// these are successful-run statistics. A healed block is not counted here
		// either: its local Get missed (no local hash), and the refetch is verified over
		// the exchange, not by this validating read.
		n := int64(len(blk.RawData()))
		if c.Prefix().Codec == cid.Raw {
			g.validatedRawReads++
			g.validatedRawBytes += n
		} else {
			g.validatedNodeReads++
			g.validatedNodeBytes += n
		}
		return blk, nil
	}
	if fetch == nil || !ipld.IsNotFound(err) {
		return nil, fmt.Errorf("pinning: gc: reading pinned block %s under head %q: %w", c, head, err)
	}
	healed, ferr := fetch.Get(ctx, c)
	if ferr != nil {
		return nil, fmt.Errorf("pinning: gc: pinned block %s under head %q is absent locally and could not be refetched: %w",
			c, head, ferr)
	}
	g.refetched++
	return healed, nil
}

// appendLinks collects every link in the subtree rooted at n.
func appendLinks(n datamodel.Node, out *[]cid.Cid) error {
	switch n.Kind() {
	case datamodel.Kind_Link:
		l, err := n.AsLink()
		if err != nil {
			return err
		}
		cl, ok := l.(cidlink.Link)
		if !ok {
			return fmt.Errorf("link %s is not a CID link", l)
		}
		*out = append(*out, cl.Cid)
	case datamodel.Kind_Map:
		for it := n.MapIterator(); !it.Done(); {
			_, v, err := it.Next()
			if err != nil {
				return err
			}
			if err := appendLinks(v, out); err != nil {
				return err
			}
		}
	case datamodel.Kind_List:
		for it := n.ListIterator(); !it.Done(); {
			_, v, err := it.Next()
			if err != nil {
				return err
			}
			if err := appendLinks(v, out); err != nil {
				return err
			}
		}
	}
	return nil
}

// sweep deletes every block the mark did not reach.
//
// Deleting during the scan is safe and is what the blockstore is built for: the
// keys arrive on a channel from a datastore query, and a block deleted after it
// has been reported is a block nobody was going to read again.
func (g *GC) sweep(ctx context.Context, marked map[string]struct{}) (GCStats, error) {
	var stats GCStats
	keys, err := g.blocks.AllKeysChan(ctx)
	if err != nil {
		return stats, fmt.Errorf("pinning: gc: listing blocks: %w", err)
	}
	for c := range keys {
		if err := ctx.Err(); err != nil {
			return stats, err
		}
		stats.Scanned++
		if _, live := marked[markKey(c)]; live {
			continue
		}
		if err := g.blocks.DeleteBlock(ctx, c); err != nil {
			return stats, fmt.Errorf("pinning: gc: deleting block %s: %w", c, err)
		}
		stats.Swept++
	}
	// AllKeysChan reports a cancelled scan by closing the channel, so the loop
	// above ends the same way whether it finished or was interrupted.
	return stats, ctx.Err()
}

// sweepOnline consumes the store's error-preserving enumeration and delegates
// every deletion decision to the active epoch. Enumeration is intentionally
// not a snapshot: a concurrent addition may be seen or missed, but either its
// successful application operation protects the multihash or it lands after
// the candidate deletion and therefore wins the ordering. Only keys absent
// from both the T0 mark set and the epoch's protected set are deleted.
func (g *GC) sweepOnline(ctx context.Context, epoch *store.BlockstoreEpoch, marked map[string]struct{}) (GCStats, error) {
	var stats GCStats
	scanCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	keys, errs, err := epoch.AllKeys(scanCtx)
	if err != nil {
		return stats, fmt.Errorf("pinning: gc: listing blocks: %w", err)
	}
	nextProgress := time.Now().Add(time.Minute)
	for keys != nil || errs != nil {
		select {
		case <-ctx.Done():
			return stats, ctx.Err()
		case scanErr, ok := <-errs:
			if !ok {
				errs = nil
				continue
			}
			if scanErr != nil {
				return stats, fmt.Errorf("pinning: gc: listing blocks: %w", scanErr)
			}
		case c, ok := <-keys:
			if !ok {
				keys = nil
				continue
			}
			stats.Scanned++
			if _, live := marked[markKey(c)]; live {
				continue
			}
			deleted, protected, err := epoch.DeleteCandidate(ctx, c)
			if err != nil {
				return stats, fmt.Errorf("pinning: gc: deleting block %s: %w", c, err)
			}
			if deleted {
				stats.Swept++
			}
			if protected {
				stats.ProtectedSkips++
			}
			if stats.Scanned%8192 == 0 && time.Now().After(nextProgress) {
				protectedNow := epoch.Protected()
				g.mx.GCProgress(stats.Scanned, protectedNow)
				g.log.Info("gc progress", "phase", metrics.GCPhaseSweep, "scanned", stats.Scanned,
					"swept", stats.Swept, "protected", protectedNow, "protected_skips", stats.ProtectedSkips)
				nextProgress = time.Now().Add(time.Minute)
			}
		}
	}
	if err := ctx.Err(); err != nil {
		return stats, err
	}
	return stats, nil
}

// Scrub validates every object observed in the blockstore against its
// multihash. It is online and read-only: it neither holds Gate nor participates
// in a protection epoch, and it never deletes or refetches. Reachability GC
// separately catches missing retained blocks; scrub catches corruption in any
// stored object, including currently unreachable garbage.
func (g *GC) Scrub(ctx context.Context) (ScrubStats, error) {
	start := time.Now()
	if g.epochs == nil {
		return ScrubStats{}, errors.New("pinning: integrity scrub requires GCConfig.Epochs")
	}
	if err := g.acquireMaintenance(ctx); err != nil {
		return ScrubStats{}, err
	}
	defer g.releaseMaintenance()
	g.mx.ScrubActive(true)
	defer g.mx.ScrubActive(false)
	g.log.Info("integrity scrub started")

	var stats ScrubStats
	scanCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	keys, errs, err := g.epochs.AllKeys(scanCtx)
	if err == nil {
		nextProgress := time.Now().Add(time.Minute)
		for keys != nil || errs != nil {
			select {
			case <-ctx.Done():
				err = ctx.Err()
				keys, errs = nil, nil
			case scanErr, ok := <-errs:
				if !ok {
					errs = nil
					continue
				}
				if scanErr != nil {
					err = fmt.Errorf("pinning: scrub: listing blocks: %w", scanErr)
					keys, errs = nil, nil
				}
			case c, ok := <-keys:
				if !ok {
					keys = nil
					continue
				}
				blk, readErr := g.blocks.Get(ctx, c)
				if readErr != nil {
					err = fmt.Errorf("pinning: scrub: validating block %s: %w", c, readErr)
					keys, errs = nil, nil
					continue
				}
				stats.Scanned++
				stats.Bytes += int64(len(blk.RawData()))
				if stats.Scanned%8192 == 0 && time.Now().After(nextProgress) {
					g.mx.ScrubProgress(stats.Scanned, stats.Bytes)
					g.log.Info("integrity scrub progress", "scanned", stats.Scanned, "validated_bytes", stats.Bytes)
					nextProgress = time.Now().Add(time.Minute)
				}
			}
		}
	} else {
		err = fmt.Errorf("pinning: scrub: listing blocks: %w", err)
	}
	if err == nil {
		err = ctx.Err()
	}
	stats.Duration = time.Since(start)
	g.mx.ScrubRun(err == nil, stats.Scanned, stats.Bytes, stats.Duration)
	if err != nil {
		g.log.Error("integrity scrub failed", "err", err, "scanned", stats.Scanned,
			"validated_bytes", stats.Bytes, "took", stats.Duration)
		return stats, err
	}
	g.log.Info("integrity scrub completed", "scanned", stats.Scanned,
		"validated_bytes", stats.Bytes, "took", stats.Duration)
	return stats, nil
}

// ScrubEvery runs full integrity verification on a schedule until ctx is
// cancelled. A failed pass is logged by Scrub and the schedule continues.
func (g *GC) ScrubEvery(ctx context.Context, interval time.Duration) error {
	if g.epochs == nil {
		return errors.New("pinning: integrity scrub scheduler requires GCConfig.Epochs")
	}
	if interval <= 0 {
		return fmt.Errorf("pinning: scrub interval is %s, must be positive", interval)
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
		if _, err := g.Scrub(ctx); ctx.Err() != nil {
			return nil
		} else if err != nil {
			continue
		}
	}
}
