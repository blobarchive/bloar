// Package chain implements the chain indexer of spec 10.2: the process that
// fills a per-chain head with exactly the blobs that chain's L1 posting sources
// named, and nothing else. (v1 shipped it as package arbitrum, back when a chain
// head could only mean an Arbitrum SequencerInbox.)
//
// The loop is spec 10.2's:
//
//	loop:
//	  L = latest finalized L1 block
//	  b = L1 block for archive synced_to (via timestamp -> slot inverse) + 1
//	  scan the sources active over [b, L] (spec 10.4):
//	      for each type-3 (blob) tx a source selects:
//	          vhs  = tx.BlobHashes()            # in-tx order
//	          slot = (block.timestamp - genesis_time) / SECONDS_PER_SLOT
//	          merge into row for slot (encounter order, dedup)
//	  if fetch_blobs: fetch exactly those vhs from upstream, POST /bloar/v1/blobs
//	  else:           require chain synced_to target <= ALL head synced_to; wait
//	  POST refs {rows, synced_to = slot(latest scanned finalized L1 block)}
//
// # The filter is a schedule, not a rule
//
// A head's filter is an ordered list of Sources (spec 10.4), and the loop above
// changes only in which transactions the scan step selects. The shape is fixed;
// the schedule is how a single head is spelled across a history in which the
// chain changed how it posted -- a SequencerInbox era and a later plain-EOA era
// are two sources over disjoint block ranges, and "arbitrum-one" stays one head.
// The union of what the sources select, deduplicated per row in encounter order,
// is the head's rows: see scan.
//
// Two source types ship, and they cost differently to scan. An inbox-events
// source lets eth_getLogs do the selection server-side and reads no block
// bodies; a blob-txs source has no log to key on and must read every block in
// its range. scan keeps them apart so the common single-inbox head never pays
// the block-body cost -- the asymmetry is spelled out on scanInboxEvents and
// scanBlobTxs.
//
// # What this head is, and what it is not
//
// A chain head is an index, not a copy. It says "this chain posted these blobs
// at these slots", and the blobs it points at are the same blocks the ALL head
// points at -- one blob, one CID, one file on disk, however many heads reference
// it. That is what makes fetch_blobs a choice at all: with an ALL head on the
// same archive there is nothing to fetch, only refs to record, and the two modes
// exist because not every deployment writes both.
//
// # Two modes, one ordering problem
//
// A refs batch is refused with a 409 naming any blob the archive does not hold
// (spec 5.1 step 4). So the blobs must be there first, and the two modes differ
// only in who put them there:
//
//   - fetch_blobs: true -- this indexer fetches exactly the vhs it saw from the
//     upstream and posts them itself. Self-sufficient; the archive needs no ALL
//     head.
//   - fetch_blobs: false -- the ALL head's beacon indexer is putting the same
//     blobs, and this one waits for it. Spec 10.2's rule: "require chain
//     synced_to target <= ALL head synced_to; wait". The ALL head's coverage is
//     the statement that the blobs of every slot up to it are in the archive,
//     so a chain batch whose target slot is at or below it will find them.
//
// The wait is real waiting, not clamping the batch down to what the ALL head
// has: see waitForAll.
package chain

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"sort"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rpc"

	"github.com/blobarchive/bloar/index/archclient"
	"github.com/blobarchive/bloar/index/upstream"
	"github.com/blobarchive/bloar/metrics"
	"github.com/blobarchive/bloar/schema"
)

// ChainClient is the parent-chain RPC this indexer needs. *ethclient.Client
// satisfies it.
//
// It is an interface for the usual reason -- these five calls are the whole
// dependency, and naming them is cheaper than a mock of everything an
// ethclient does -- and also because it documents the RPC surface an operator
// has to expose: a node with getLogs, getTransactionByHash, getBlockByHash and
// getBlockByNumber (both the header, for the finalized tag and the slot inverse,
// and full transactions, for blob-txs sources) is enough. BlockByNumber is only
// called when a blob-txs source (spec 10.4) is active over a range; a head with
// only inbox-events sources never reads a block body.
type ChainClient interface {
	HeaderByNumber(ctx context.Context, number *big.Int) (*types.Header, error)
	HeaderByHash(ctx context.Context, hash common.Hash) (*types.Header, error)
	BlockByNumber(ctx context.Context, number *big.Int) (*types.Block, error)
	FilterLogs(ctx context.Context, q ethereum.FilterQuery) ([]types.Log, error)
	TransactionByHash(ctx context.Context, hash common.Hash) (tx *types.Transaction, isPending bool, err error)
}

// BlockBatchClient is the optional accelerated full-block surface used by
// blob-txs scans. The result MUST have the same length and order as numbers;
// the scanner checks that contract before reducing any rows. A ChainClient
// without this extension still works: each worker falls back to serial
// BlockByNumber calls inside its bounded chunk.
type BlockBatchClient interface {
	BlocksByNumber(ctx context.Context, numbers []uint64) ([]*types.Block, error)
}

// Config is what an Indexer needs.
type Config struct {
	// Chain is the parent-chain RPC. Required.
	Chain ChainClient
	// Archive is the bloar archive being written. Required.
	Archive *archclient.Client
	// Upstream is where blobs are fetched from when FetchBlobs is set. Required
	// then, ignored otherwise.
	Upstream *upstream.Client
	// Head is the chain head to write, e.g. "arbitrum-one". Required.
	Head string
	// AllHead is the head whose coverage gates this one when FetchBlobs is
	// false, conventionally "all". Required then, ignored otherwise.
	AllHead string
	// Sources is the head's ordered filter schedule (spec 10.4). Required and
	// non-empty; New validates it with ValidateSources. The rows the indexer
	// posts are the deduplicated union of what these sources select, in the
	// list order given here.
	Sources []Source
	// GenesisTime and SecondsPerSlot are the beacon clock this indexer turns
	// L1 block timestamps into slots with (spec 10.2). Both required, and both
	// MUST match the archive's own beacon config: they decide which slot every
	// row lands on, and a disagreement puts the refs where nobody looks for
	// them. Nothing can check this at runtime -- the archive serves its beacon
	// config, but a wrong value there is wrong everywhere consistently -- so it
	// is an operator's job to configure one clock in two files.
	GenesisTime    uint64
	SecondsPerSlot uint64
	// FetchBlobs selects the mode described in the package comment.
	FetchBlobs bool
	// BlockRange is how many L1 blocks one scan covers. Zero takes 1000, which
	// is comfortably inside what public RPC providers allow eth_getLogs to
	// span.
	BlockRange uint64
	// BlockFetchConcurrency bounds the blob-txs full-block worker pool. Zero
	// takes 4. It does not affect inbox-events sources, which read no block
	// bodies.
	BlockFetchConcurrency int
	// RPCBatchSize bounds how many consecutive eth_getBlockByNumber calls an
	// accelerated worker sends in one JSON-RPC batch. Zero takes 16. A
	// ChainClient without BlockBatchClient still uses chunks of this size, but
	// executes the calls serially inside each worker.
	RPCBatchSize int
	// MaxPutBlobs bounds one POST /bloar/v1/blobs (spec 7.2, default 64). Zero
	// takes 64. Ignored unless FetchBlobs.
	MaxPutBlobs int
	// PollInterval is how long Run sleeps when caught up, and how often
	// waitForAll re-reads the ALL head. Zero takes 12 seconds.
	PollInterval time.Duration
	// Logger receives progress. Optional.
	Logger *slog.Logger
	// Metrics receives bounded block-fetch work observations. Optional.
	Metrics *metrics.Metrics
}

// Defaults.
const (
	defaultBlockRange            = 1000
	defaultBlockFetchConcurrency = 4
	defaultRPCBatchSize          = 16
	defaultMaxPutBlobs           = 64
	defaultPollInterval          = 12 * time.Second
	maxBlockFetchConcurrency     = 32
	maxRPCBatchSize              = 128
)

// SourceType is one of the two L1 posting mechanisms a chain-head source can
// select (spec 10.4).
type SourceType string

const (
	// SourceInboxEvents selects the type-3 batch transaction behind each of a
	// contract's logs for an event topic. The shipped default -- a SequencerInbox
	// and its SequencerBatchDelivered topic -- is the v1 filter, now named.
	SourceInboxEvents SourceType = "inbox-events"
	// SourceBlobTxs selects type-3 transactions to a recipient from an allowlisted
	// sender: the Base-style arrangement of posting blobs to a plain EOA.
	SourceBlobTxs SourceType = "blob-txs"
)

// Source is one entry in a head's ordered filter schedule (spec 10.4).
//
// A head's rows are the deduplicated union of what its sources select. The
// schedule exists so one head can span a history in which the chain changed how
// it posted: a SequencerInbox era and a later EOA era of the same chain are two
// sources over disjoint (or, harmlessly, overlapping) block ranges, and the head
// keeps its name and its meaning across the change.
type Source struct {
	Type SourceType
	// Address is the SequencerInbox contract (inbox-events) or the recipient EOA
	// (blob-txs).
	Address common.Address
	// Topic is the event topic0 an inbox-events source matches; ignored for
	// blob-txs. For the shipped default it is SequencerBatchDeliveredTopic.
	Topic common.Hash
	// Senders is the REQUIRED, non-empty sender allowlist of a blob-txs source;
	// ignored for inbox-events. It is not optional and not a convenience: anyone
	// can send a blob transaction to any address, so an empty allowlist would let
	// any third party's blobs be recorded as this chain's history (spec 10.4).
	// ValidateSources refuses one.
	Senders []common.Address
	// FromBlock and UntilBlock bound the source's L1 range, both inclusive.
	// OpenEnded is UntilBlock absent: the source runs to the scan's end.
	FromBlock  uint64
	UntilBlock uint64
	OpenEnded  bool
}

// activeRange intersects the source's block range with [from, to], reporting
// whether any block is covered.
func (s Source) activeRange(from, to uint64) (lo, hi uint64, ok bool) {
	lo, hi = max(s.FromBlock, from), to
	if !s.OpenEnded {
		hi = min(s.UntilBlock, to)
	}
	if lo > hi {
		return 0, 0, false
	}
	return lo, hi, true
}

// senderAllowed reports whether addr is in a blob-txs source's allowlist. The
// allowlist is a handful of sequencer addresses, and this is asked only of a
// transaction already sent to the source's recipient, so a linear scan is both
// cheap and clearer than a set rebuilt per scan.
func (s Source) senderAllowed(addr common.Address) bool {
	for _, a := range s.Senders {
		if a == addr {
			return true
		}
	}
	return false
}

// emptyAllowlistReason is spec 10.4's reason a blob-txs source MUST carry a
// non-empty allowlist, quoted where the refusal happens so an operator who hits
// it reads the argument, not just the rule.
const emptyAllowlistReason = "anyone can send a blob transaction to any address, so a blob-txs source with no " +
	"sender allowlist is a write handle to the head that any third party holds, and a head is a claim about what a " +
	"specific sequencer posted, which an unrestricted EOA source cannot make (spec 10.4)"

// ValidateSources checks a source schedule against the structural invariants of
// spec 10.4, independently of how it was parsed. New calls it, and so does the
// config loader (cmd/bloar-index), so a bad schedule is refused at config load
// with the same rule the engine would enforce.
//
// The invariants are per-source -- a non-zero address, a topic or a non-empty
// sender allowlist as the type demands, until_block not before from_block -- and
// one that spans the whole schedule: its sources must leave no uncovered block
// range between them (requireContiguous). That last one guards the single
// boundary union-with-dedup does not make safe; the rest is field hygiene.
//
// What it does NOT check is immutability (spec 10.4, 10.5): whether a proposed
// schedule changes only ground AHEAD of the head's position is a statement about
// where synced_to sits on L1, which needs the chain RPC and the manifest chain
// to evaluate. That check is the manifest validation of spec 10.5 and lands in
// manifest validation; this function enforces the purely local floor beneath it.
func ValidateSources(sources []Source) error {
	if len(sources) == 0 {
		return errors.New("chain: a head needs at least one source (spec 10.4)")
	}
	for i, s := range sources {
		if s.Address == (common.Address{}) {
			return fmt.Errorf("chain: source %d has a zero address", i)
		}
		switch s.Type {
		case SourceInboxEvents:
			if s.Topic == (common.Hash{}) {
				return fmt.Errorf("chain: source %d is inbox-events with a zero topic", i)
			}
		case SourceBlobTxs:
			if len(s.Senders) == 0 {
				return fmt.Errorf("chain: source %d is blob-txs with an empty sender allowlist: %s", i, emptyAllowlistReason)
			}
		default:
			return fmt.Errorf("chain: source %d has unknown type %q; want %q or %q",
				i, s.Type, SourceInboxEvents, SourceBlobTxs)
		}
		if !s.OpenEnded && s.UntilBlock < s.FromBlock {
			return fmt.Errorf("chain: source %d has until_block %d before from_block %d", i, s.UntilBlock, s.FromBlock)
		}
	}
	return requireContiguous(sources)
}

// requireContiguous refuses a schedule that leaves an uncovered block range
// BETWEEN two of its sources. It is the guard for the one boundary union-with-
// dedup does not make safe.
//
// Overlapping sources hand off cleanly: a blob two sources both select is one
// ref, in the first encounter's position (scan). A GAP is the opposite -- blocks
// no source selects -- and scanRange still advances synced_to across them while
// recording nothing, so every batch a chain posted in the hole is frozen as a
// permanent, false 404: replay locks it in, followers inherit it, and nothing
// errors. That is the source-contiguity hardening. Since overlap is safe and only a gap is not, the one
// off-by-one at a close-and-add boundary (source A until 1000, source B from
// 1002) is the whole exposure, and it is refused here rather than served wrong.
//
// A gap BEFORE the earliest source is legitimate -- the schedule simply starts
// there -- so coverage is measured from the lowest from_block onward, never from
// zero. An open-ended source covers to infinity, so once the sorted walk reaches
// one with no gap before it, no later source can leave a hole; that is why holes
// can exist only among the bounded ranges preceding the earliest open end.
func requireContiguous(sources []Source) error {
	// The caller's order is semantically meaningful -- spec 10.5 compares
	// schedules in list order, and the rowBuilder's encounter order is
	// source-list order -- so sort a permutation of indices and never the slice
	// itself. The indices also carry each source's list position into the error.
	order := make([]int, len(sources))
	for i := range order {
		order[i] = i
	}
	sort.SliceStable(order, func(a, b int) bool {
		return sources[order[a]].FromBlock < sources[order[b]].FromBlock
	})

	// coveredMax is the highest block covered contiguously from the earliest
	// from_block reached so far; left is the source that reaches it, named as the
	// left side of a hole if the next source starts past coveredMax+1.
	left := order[0]
	if sources[left].OpenEnded {
		return nil
	}
	coveredMax := sources[left].UntilBlock
	for _, i := range order[1:] {
		s := sources[i]
		// A hole exists iff s starts more than one block past the covered max.
		// Phrased as a subtraction (never coveredMax+1) so an until_block at the
		// top of the uint64 range cannot overflow the comparison into a false gap.
		if s.FromBlock > coveredMax && s.FromBlock-coveredMax > 1 {
			return fmt.Errorf("chain: sources %d and %d leave L1 blocks %d..%d uncovered, a gap between two covered "+
				"ranges (spec 10.4): the scan advances synced_to across those blocks while selecting no batch, so every "+
				"blob this chain posted in them is recorded as a permanent, false 404 that replay and followers then "+
				"inherit. Overlap adjacent sources to hand off between them; a gap is refused",
				left, i, coveredMax+1, s.FromBlock-1)
		}
		if s.OpenEnded {
			return nil
		}
		if s.UntilBlock > coveredMax {
			coveredMax, left = s.UntilBlock, i
		}
	}
	return nil
}

// Indexer is the chain indexer of spec 10.2.
type Indexer struct {
	cfg Config
	log *slog.Logger

	origin    uint64
	gotOrigin bool

	// verified records that CheckSchedule has confirmed the configured schedule
	// exactly equals the head's published manifest tip.
	// It is the fail-closed gate on the exported mutation boundary: Step refuses
	// until it is set, and Run sets it by running CheckSchedule itself, so an
	// embedding caller cannot build an unattested chain by driving Run or Step
	// without the schedule check. The per-poll tip reread clears it before
	// revalidating a changed tip, so a tip the configured schedule no longer
	// matches can never be committed against.
	verified bool

	// manifestTip is the CID of the manifest tip verified above, sent as
	// expected_manifest on every refs POST so the archive binds each committed
	// batch to it. It is meaningful only while verified
	// is set -- Step's guard is on verified, not on this string -- and the per-poll
	// reread advances it when the head adopts a new tip carrying the same schedule.
	manifestTip string

	// hint is the last (slot, L1 block) pair this indexer posted, used to skip
	// the search in resume. See resume for why it is only a hint.
	hint      progress
	haveHint  bool
	finalized *big.Int
}

// progress pairs a head's synced_to with the L1 block that produced it.
type progress struct {
	slot  uint64
	block uint64
}

// New returns an Indexer over cfg.
func New(cfg Config) (*Indexer, error) {
	switch {
	case cfg.Chain == nil:
		return nil, errors.New("chain: Config.Chain is required")
	case cfg.Archive == nil:
		return nil, errors.New("chain: Config.Archive is required")
	case cfg.Head == "":
		return nil, errors.New("chain: Config.Head is required")
	case cfg.GenesisTime == 0:
		// A zero would not fail; it would put every row on a slot several
		// million too high, consistently, and look fine doing it.
		return nil, errors.New("chain: Config.GenesisTime is required")
	case cfg.SecondsPerSlot == 0:
		return nil, errors.New("chain: Config.SecondsPerSlot is required")
	case cfg.FetchBlobs && cfg.Upstream == nil:
		return nil, errors.New("chain: Config.Upstream is required when FetchBlobs is set")
	case !cfg.FetchBlobs && cfg.AllHead == "":
		return nil, errors.New("chain: Config.AllHead is required when FetchBlobs is not set: " +
			"without it there is nothing to wait for, and every refs batch would race the blobs it references")
	}
	if err := ValidateSources(cfg.Sources); err != nil {
		return nil, err
	}
	// Take an indexer-owned deep copy of the schedule. cfg is a shallow copy of the
	// caller's Config, so cfg.Sources still aliases the caller's slice and each
	// blob-txs source's Senders slice; without this, a caller that retains and later
	// mutates those slices could turn a schedule verified as A into B under a running
	// indexer, scanning B while binding refs to A's tip. Verification
	// must bind a snapshot nothing the caller holds can change.
	cfg.Sources = cloneSources(cfg.Sources)
	if cfg.BlockRange == 0 {
		cfg.BlockRange = defaultBlockRange
	}
	if cfg.BlockFetchConcurrency == 0 {
		cfg.BlockFetchConcurrency = defaultBlockFetchConcurrency
	}
	if cfg.BlockFetchConcurrency < 0 || cfg.BlockFetchConcurrency > maxBlockFetchConcurrency {
		return nil, fmt.Errorf("chain: Config.BlockFetchConcurrency is %d, must be in [1,%d]",
			cfg.BlockFetchConcurrency, maxBlockFetchConcurrency)
	}
	if cfg.RPCBatchSize == 0 {
		cfg.RPCBatchSize = defaultRPCBatchSize
	}
	if cfg.RPCBatchSize < 0 || cfg.RPCBatchSize > maxRPCBatchSize {
		return nil, fmt.Errorf("chain: Config.RPCBatchSize is %d, must be in [1,%d]",
			cfg.RPCBatchSize, maxRPCBatchSize)
	}
	if cfg.MaxPutBlobs == 0 {
		cfg.MaxPutBlobs = defaultMaxPutBlobs
	}
	if cfg.MaxPutBlobs < 0 {
		return nil, fmt.Errorf("chain: Config.MaxPutBlobs is %d, must be positive", cfg.MaxPutBlobs)
	}
	if cfg.PollInterval == 0 {
		cfg.PollInterval = defaultPollInterval
	}
	// Strictly positive after defaulting: Run's caught-up wait and
	// waitForAll both sleep with time.After(PollInterval), which a non-positive
	// value collapses into an immediate loop that re-reads the archive with no
	// delay. A zero is the default just applied, so a value at or below zero here is
	// a caller mistake -- caught at construction rather than as a runtime spin.
	if cfg.PollInterval <= 0 {
		return nil, fmt.Errorf("chain: Config.PollInterval is %s, must be positive", cfg.PollInterval)
	}

	ix := &Indexer{
		cfg: cfg,
		log: cfg.Logger,
		// rpc.FinalizedBlockNumber is what ethclient renders as the "finalized"
		// block tag. Spec 10.3: indexers MUST only process finalized data, and
		// this is the whole of how that is enforced on the L1 side -- every
		// header and every log range this indexer asks for is bounded by it.
		finalized: big.NewInt(rpc.FinalizedBlockNumber.Int64()),
	}
	if ix.log == nil {
		ix.log = slog.New(slog.DiscardHandler)
	}
	return ix, nil
}

// cloneSources returns a deep copy of a schedule that shares no mutable state with
// the argument, so an indexer holding it is insulated from a caller mutating the
// slices it passed to New. Source's Address (common.Address) and
// Topic (common.Hash) are fixed-size byte arrays -- value types copied whole with
// the struct -- so the only reference-typed state to duplicate is the outer slice
// and each source's Senders slice; its elements are common.Address values, copied
// by the append.
func cloneSources(sources []Source) []Source {
	out := make([]Source, len(sources))
	for i, s := range sources {
		if s.Senders != nil {
			s.Senders = append([]common.Address(nil), s.Senders...)
		}
		out[i] = s
	}
	return out
}

// errScheduleUnverified is Step's fail-closed refusal before the configured
// schedule has been checked against the head's published manifest tip (spec 10.5,
// the safety boundary). It is the exported mutation boundary's guard: Run clears it by
// running CheckSchedule itself, and a direct Step -- an embedding caller reaching
// past Run -- hits this rather than scanning and committing an unattested chain.
var errScheduleUnverified = errors.New("chain: the configured schedule has not been verified against the head's " +
	"published manifest tip; call CheckSchedule (Run does this itself) before Step")

// Run drives the loop until ctx is cancelled, which it reports as nil.
//
// It performs the startup schedule check itself, so the exported entry point
// cannot build an unattested chain: a caller that reaches Run without a prior
// CheckSchedule still fails closed on a chainless head or a schedule that does not
// equal the published tip. CheckSchedule is skipped when
// the state it establishes already exists, which is what lets it be run more than
// once -- at startup here, and again by the per-poll reread when a tip changes.
func (ix *Indexer) Run(ctx context.Context) error {
	if !ix.verified {
		if err := ix.CheckSchedule(ctx); err != nil {
			return err
		}
	}
	ix.log.Info("chain indexer starting",
		"head", ix.cfg.Head, "sources", len(ix.cfg.Sources), "fetch_blobs", ix.cfg.FetchBlobs,
		"block_range", ix.cfg.BlockRange, "poll_interval", ix.cfg.PollInterval)

	for {
		// Every poll cycle rereads the published manifest tip before scanning: a
		// caught-up process must not stay alive on a superseded schedule when there
		// is no new finalized work to trip the commit-time binding (spec 10.5, audit
		// the safety boundary). A changed tip is revalidated and either adopted or exited on.
		if err := ix.reconcileManifestTip(ctx); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		advanced, err := ix.Step(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		if advanced {
			continue
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(ix.cfg.PollInterval):
		}
	}
}

// Step runs one pass: at most one scan of at most BlockRange L1 blocks.
//
// It reports whether it advanced coverage. Tests drive this directly, so the
// verified-schedule guard lives here rather than only in Run: Step is the exported
// mutation boundary, and it must fail closed until CheckSchedule has bound the
// configured schedule to the head's published tip.
// Without this, an embedding caller could scan and commit refs -- omitting
// expected_manifest, which a chainless head accepts -- and build a chain no
// published manifest ever attested.
func (ix *Indexer) Step(ctx context.Context) (bool, error) {
	if !ix.verified {
		return false, errScheduleUnverified
	}
	if err := ix.loadOrigin(ctx); err != nil {
		return false, err
	}

	latest, err := ix.cfg.Chain.HeaderByNumber(ctx, ix.finalized)
	if err != nil {
		return false, fmt.Errorf("chain: reading the finalized L1 header: %w", err)
	}
	if latest == nil {
		// A chain with nothing finalized yet: a fresh devnet, mostly.
		return false, nil
	}
	if !latest.Number.IsUint64() {
		return false, fmt.Errorf("chain: finalized L1 block number %s does not fit in a uint64", latest.Number)
	}
	last := latest.Number.Uint64()

	from, err := ix.resume(ctx, last)
	if err != nil {
		return false, err
	}
	if from > last {
		return false, nil
	}

	to := min(last, from+ix.cfg.BlockRange-1)
	return ix.scanRange(ctx, from, to)
}

// loadOrigin reads the head's origin_slot once.
func (ix *Indexer) loadOrigin(ctx context.Context) error {
	if ix.gotOrigin {
		return nil
	}
	info, err := ix.cfg.Archive.Head(ctx, ix.cfg.Head)
	if err != nil {
		return fmt.Errorf("chain: reading head %q: %w", ix.cfg.Head, err)
	}
	ix.origin, ix.gotOrigin = info.OriginSlot, true
	return nil
}

// resume returns the first L1 block this pass must scan: spec 10.2's "b = L1
// block for archive synced_to (via timestamp -> slot inverse) + 1".
//
// # Inverting slot -> L1 block
//
// The head records slots; the scan reads L1 blocks; the only bridge between
// them is timestamp arithmetic in one direction (slot = (ts - genesis) / sps).
// Inverting it means finding the first L1 block whose slot is past the head's
// coverage -- that is, the first block with
//
//	timestamp >= genesis_time + (synced_to + 1) * seconds_per_slot
//
// and since post-merge block timestamps are strictly increasing, that predicate
// is monotonic in the block number and a binary search over [0, latest] finds
// it in ~25 header reads. That is the honest inverse, and it is what this does
// when it has nothing better.
//
// The something better is the hint: having just posted synced_to = T from a
// scan that ended at L1 block E, this indexer knows the pair (T, E) exactly,
// with no search and no arithmetic. If the archive still says synced_to == T,
// the next scan starts at E+1. If it says anything else -- a restart, an
// operator's truncate, a second indexer on the same head -- the hint is stale
// and the search runs. The archive remains the only source of progress (spec
// 10); the hint is a cache in front of a derivation, and it is checked against
// the archive every single pass, never trusted over it.
func (ix *Indexer) resume(ctx context.Context, latest uint64) (uint64, error) {
	syncedTo, err := ix.cfg.Archive.SyncedTo(ctx, ix.cfg.Head)
	if err != nil {
		return 0, fmt.Errorf("chain: reading synced_to of head %q: %w", ix.cfg.Head, err)
	}
	if syncedTo != nil && ix.haveHint && ix.hint.slot == *syncedTo {
		return ix.hint.block + 1, nil
	}

	// The first slot not yet covered. An empty head starts at its origin.
	want := ix.origin
	if syncedTo != nil {
		want = *syncedTo + 1
	}
	block, err := ix.blockAtOrAfterSlot(ctx, want, latest)
	if err != nil {
		return 0, err
	}
	ix.log.Debug("resolved resume point by search", "slot", want, "l1_block", block)
	return block, nil
}

// blockAtOrAfterSlot finds the lowest L1 block number in [0, latest] whose
// timestamp lands in slot or later, or latest+1 if none does.
func (ix *Indexer) blockAtOrAfterSlot(ctx context.Context, slot, latest uint64) (uint64, error) {
	target := ix.cfg.GenesisTime + slot*ix.cfg.SecondsPerSlot

	var searchErr error
	// sort.Search wants an int; L1 block numbers are nowhere near overflowing
	// one on any 64-bit build, and a 32-bit build would have failed at the
	// uint64 header count long before this.
	found := sort.Search(int(latest)+1, func(i int) bool {
		if searchErr != nil {
			return true // unwind: the answer is discarded.
		}
		h, err := ix.cfg.Chain.HeaderByNumber(ctx, new(big.Int).SetUint64(uint64(i)))
		if err != nil {
			searchErr = fmt.Errorf("chain: reading L1 header %d during the slot search: %w", i, err)
			return true
		}
		return h.Time >= target
	})
	if searchErr != nil {
		return 0, searchErr
	}
	return uint64(found), nil
}

// scanRange scans [from, to] and records what it finds.
func (ix *Indexer) scanRange(ctx context.Context, from, to uint64) (bool, error) {
	rows, err := ix.scan(ctx, from, to)
	if err != nil {
		return false, err
	}

	// The batch's coverage: spec 10.2's "synced_to = slot(latest scanned
	// finalized L1 block)". Everything up to this slot has now been decided
	// about -- a scanned range with no blob batches in it advances coverage
	// exactly as one full of them does, which is what makes the head's 404s
	// mean "this chain posted nothing here" rather than "not yet".
	target, err := ix.slotOfBlock(ctx, to)
	if err != nil {
		return false, err
	}
	if target < ix.origin {
		// The scan has not reached the head's origin yet. Nothing to record and
		// nothing to advance: coverage starts at origin_slot.
		ix.log.Debug("scanned L1 range precedes the head's origin slot", "to", to, "slot", target, "origin", ix.origin)
		return false, nil
	}

	if ix.cfg.FetchBlobs {
		if err := ix.fetchAndPut(ctx, rows); err != nil {
			return false, err
		}
	} else if err := ix.waitForAll(ctx, target); err != nil {
		return false, err
	}

	res, err := ix.cfg.Archive.PostRefs(ctx, ix.cfg.Head, rows, target, ix.manifestTip)
	if err != nil {
		var bind *archclient.ManifestBindingError
		if errors.As(err, &bind) {
			// The manifest tip advanced under this still-running indexer (spec 10.5,
			// the safety boundary). The schedule it scanned under is superseded, so it must
			// not keep committing across the handoff: stop, and let a restart run
			// CheckSchedule against the new tip (or refuse if the config is stale).
			return false, fmt.Errorf("chain: head %q's manifest tip advanced from %s to %s under this running indexer; "+
				"stop and restart with a schedule matching the new tip (spec 10.5): %w",
				ix.cfg.Head, ix.manifestTip, bind.CurrentTip, err)
		}
		return false, fmt.Errorf("chain: posting refs for L1 blocks [%d, %d] (slots up to %d): %w", from, to, target, err)
	}

	ix.hint, ix.haveHint = progress{slot: target, block: to}, true
	ix.log.Info("recorded chain batches",
		"head", ix.cfg.Head, "l1_from", from, "l1_to", to, "synced_to", target,
		"rows", len(rows), "root", res.Root, "noop", res.NoOp)
	return true, nil
}

// scan reads every source active over [from, to] and renders their union as
// refs rows (spec 10.4).
//
// The sources are visited in list order, and each adds its matches to the one
// rowBuilder in the order it encounters them: a slot's row accumulates source
// 0's blobs, then source 1's, and the builder's per-row dedup drops any hash a
// later source repeats. That is spec 10.4's "union, deduplicated per row in
// encounter order" made concrete -- encounter order is source-list order, then
// within a source the scan's own order, which for both source types bottoms out
// at ascending L1 block and then index-within-block. inbox-events normalizes the
// RPC result by (block number, transaction index, log index), and blob-txs reads
// a block's transactions in body order, so two runs over the same finalized
// blocks build byte-identical rows (spec 11.5).
//
// Overlapping ranges are harmless by the same dedup: a transaction two sources
// both select contributes one ref, in the position the first source gave it, so
// a migration need not compute an exact hand-off block.
func (ix *Indexer) scan(ctx context.Context, from, to uint64) ([]archclient.Row, error) {
	b := newRowBuilder()
	for i := range ix.cfg.Sources {
		src := ix.cfg.Sources[i]
		lo, hi, ok := src.activeRange(from, to)
		if !ok {
			continue
		}
		var err error
		switch src.Type {
		case SourceInboxEvents:
			err = ix.scanInboxEvents(ctx, src, lo, hi, b)
		case SourceBlobTxs:
			err = ix.scanBlobTxs(ctx, src, lo, hi, b)
		default:
			// ValidateSources refuses an unknown type at construction, so this is
			// unreachable; it is here so a third source type cannot be added to
			// the type switches elsewhere without this one failing to compile.
			err = fmt.Errorf("chain: source %d has unknown type %q", i, src.Type)
		}
		if err != nil {
			return nil, err
		}
	}
	return b.rows(), nil
}

// scanInboxEvents records the type-3 batch transaction behind every log a
// contract emitted for a topic over [from, to] (spec 10.4's inbox-events
// source, and the v1 filter).
//
// This is the cheap path, and the common one. eth_getLogs does the selection
// server-side, so the whole range costs one getLogs; only the blocks that
// matched are then read, one getTransactionByHash and one getBlockByHash each.
// Cost scales with the matches found, not the range scanned -- the asymmetry
// scanBlobTxs cannot share, and the reason scan keeps the two apart.
func (ix *Indexer) scanInboxEvents(ctx context.Context, src Source, from, to uint64, b *rowBuilder) error {
	// The completeness of this one getLogs is trusted, not checked: nothing here
	// distinguishes a full answer from one a provider silently capped, and a
	// dropped log is a batch recorded as a permanent 404 (spec 10.4). That trust
	// rides on the operator's node choice -- parent_chain_rpc MUST return complete
	// eth_getLogs results or error, never truncate silently (spec 10.2, 10.4).
	logs, err := ix.cfg.Chain.FilterLogs(ctx, ethereum.FilterQuery{
		FromBlock: new(big.Int).SetUint64(from),
		ToBlock:   new(big.Int).SetUint64(to),
		Addresses: []common.Address{src.Address},
		Topics:    [][]common.Hash{{src.Topic}},
	})
	if err != nil {
		return fmt.Errorf("chain: reading %s logs in L1 blocks [%d, %d]: %w", src.Address, from, to, err)
	}
	logs, err = canonicalInboxLogs(logs, src, from, to)
	if err != nil {
		return err
	}

	for _, l := range logs {
		if l.Removed {
			// A log from a reorged-out block. Only finalized blocks are ever
			// scanned (spec 10.3), so this should not arrive; if it does, the
			// node is describing a chain this indexer must not record.
			return fmt.Errorf("chain: node returned a removed log at L1 block %d, tx %s, "+
				"inside the finalized range [%d, %d]", l.BlockNumber, l.TxHash, from, to)
		}

		tx, pending, err := ix.cfg.Chain.TransactionByHash(ctx, l.TxHash)
		if err != nil {
			return fmt.Errorf("chain: reading batch tx %s (L1 block %d): %w", l.TxHash, l.BlockNumber, err)
		}
		if pending {
			return fmt.Errorf("chain: node calls batch tx %s pending, but it emitted a log in finalized L1 block %d",
				l.TxHash, l.BlockNumber)
		}

		if tx.Type() != types.BlobTxType {
			// A calldata or AnyTrust or delayed-only batch: spec 10.2's
			// "non-blob batches produce no rows; coverage still advances".
			//
			// The transaction type is the test, rather than the event's
			// dataLocation argument (see inbox.go), because it is the one that
			// cannot lie about the thing being recorded here. A row is a claim
			// that these versioned hashes were posted in this transaction, and
			// only a type-3 transaction has versioned hashes at all: the
			// protocol put them there. dataLocation is a number the contract
			// wrote, and reading it would mean trusting the contract's
			// description of a transaction over the transaction.
			ix.log.Debug("skipping non-blob batch", "tx", l.TxHash, "type", tx.Type(), "l1_block", l.BlockNumber)
			continue
		}
		hashes := tx.BlobHashes()
		if len(hashes) == 0 {
			return fmt.Errorf("chain: batch tx %s is a blob transaction carrying no blob hashes", l.TxHash)
		}

		slot, err := ix.slotOfHash(ctx, l.BlockHash)
		if err != nil {
			return err
		}
		if err := ix.recordMatch(b, slot, hashes, l.TxHash); err != nil {
			return err
		}
	}
	return nil
}

// canonicalInboxLogs makes the encounter order of one inbox-events source a
// function of chain position rather than of an RPC server's response order.
// FilterLogs owns the returned slice, so this always copies before sorting.
// That also prevents one scan from reordering a shared RPC cache underneath a
// concurrent caller.
//
// The structural checks are part of the same trust boundary. A finalized log
// must be inside the requested range, one block number must name one block hash,
// one in-block log index must name one position, and the transaction/log
// metadata must describe an order a block could actually have. Refusing an
// incoherent answer is safer than deterministically recording the wrong rows.
func canonicalInboxLogs(logs []types.Log, src Source, from, to uint64) ([]types.Log, error) {
	ordered := append([]types.Log(nil), logs...)
	sort.SliceStable(ordered, func(i, j int) bool {
		if ordered[i].BlockNumber != ordered[j].BlockNumber {
			return ordered[i].BlockNumber < ordered[j].BlockNumber
		}
		if ordered[i].TxIndex != ordered[j].TxIndex {
			return ordered[i].TxIndex < ordered[j].TxIndex
		}
		return ordered[i].Index < ordered[j].Index
	})

	type logPosition struct {
		block uint64
		index uint
	}
	type txPosition struct {
		block uint64
		index uint
	}

	blockHashes := make(map[uint64]common.Hash)
	blockNumbers := make(map[common.Hash]uint64)
	logPositions := make(map[logPosition]struct{})
	txHashes := make(map[txPosition]common.Hash)
	txPositions := make(map[common.Hash]txPosition)

	var previous types.Log
	havePrevious := false
	for _, l := range ordered {
		if l.BlockNumber < from || l.BlockNumber > to {
			return nil, fmt.Errorf("chain: node returned a log from L1 block %d outside requested range [%d, %d]",
				l.BlockNumber, from, to)
		}
		if l.Address != src.Address {
			return nil, fmt.Errorf("chain: node returned a log from address %s for inbox-events source %s",
				l.Address, src.Address)
		}
		if len(l.Topics) == 0 || l.Topics[0] != src.Topic {
			return nil, fmt.Errorf("chain: node returned a log with the wrong topic0 for inbox-events source %s", src.Address)
		}
		if l.BlockHash == (common.Hash{}) {
			return nil, fmt.Errorf("chain: node returned a log at L1 block %d with a zero block hash", l.BlockNumber)
		}
		if l.TxHash == (common.Hash{}) {
			return nil, fmt.Errorf("chain: node returned a log at L1 block %d, tx index %d with a zero transaction hash",
				l.BlockNumber, l.TxIndex)
		}

		if hash, ok := blockHashes[l.BlockNumber]; ok && hash != l.BlockHash {
			return nil, fmt.Errorf("chain: node returned inconsistent block hashes %s and %s for L1 block %d",
				hash, l.BlockHash, l.BlockNumber)
		}
		blockHashes[l.BlockNumber] = l.BlockHash
		if number, ok := blockNumbers[l.BlockHash]; ok && number != l.BlockNumber {
			return nil, fmt.Errorf("chain: node returned block hash %s for both L1 blocks %d and %d",
				l.BlockHash, number, l.BlockNumber)
		}
		blockNumbers[l.BlockHash] = l.BlockNumber

		lp := logPosition{block: l.BlockNumber, index: l.Index}
		if _, ok := logPositions[lp]; ok {
			return nil, fmt.Errorf("chain: node returned duplicate log position at L1 block %d, log index %d",
				l.BlockNumber, l.Index)
		}
		logPositions[lp] = struct{}{}

		tp := txPosition{block: l.BlockNumber, index: l.TxIndex}
		if hash, ok := txHashes[tp]; ok && hash != l.TxHash {
			return nil, fmt.Errorf("chain: node returned transaction index %d in L1 block %d with two hashes, %s and %s",
				l.TxIndex, l.BlockNumber, hash, l.TxHash)
		}
		txHashes[tp] = l.TxHash
		if pos, ok := txPositions[l.TxHash]; ok && pos != tp {
			return nil, fmt.Errorf("chain: node returned transaction %s at both L1 block %d index %d and L1 block %d index %d",
				l.TxHash, pos.block, pos.index, l.BlockNumber, l.TxIndex)
		}
		txPositions[l.TxHash] = tp

		// Log indexes are assigned across a block in transaction execution order.
		// Once sorted by transaction index, they therefore have to increase too.
		if havePrevious && previous.BlockNumber == l.BlockNumber && l.Index <= previous.Index {
			return nil, fmt.Errorf("chain: node returned impossible log ordering in L1 block %d: transaction index %d log index %d follows transaction index %d log index %d",
				l.BlockNumber, l.TxIndex, l.Index, previous.TxIndex, previous.Index)
		}
		previous, havePrevious = l, true
	}

	return ordered, nil
}

// scanBlobTxs records every type-3 transaction sent to a recipient by an
// allowlisted sender over [from, to] (spec 10.4's blob-txs source).
//
// # Why this reads whole blocks where inbox-events does not
//
// A blob-txs source selects on a transaction's recipient and sender, neither of
// which any log carries -- there is no event to hand eth_getLogs. The only way
// to see every type-3 transaction to an address is to read the block bodies and
// look, so this path costs one getBlockByNumber (full transactions) per block in
// the range, scaling with the range rather than with the matches. That is the
// cost asymmetry scan is built around: a range with no blob-txs source active
// reads no block bodies at all, so the common single-inbox head never pays it.
func (ix *Indexer) scanBlobTxs(ctx context.Context, src Source, from, to uint64, b *rowBuilder) error {
	if from > to {
		return nil
	}
	// scan tests intentionally construct an Indexer without New. Keep that
	// narrow seam useful while making production defaults live in New: an
	// unset value here means one worker and one call per chunk, not an empty
	// worker pool or a divide-by-zero loop.
	workers := ix.cfg.BlockFetchConcurrency
	if workers <= 0 {
		workers = 1
	}
	batchSize := ix.cfg.RPCBatchSize
	if batchSize <= 0 {
		batchSize = 1
	}
	ix.cfg.Metrics.IndexBlockFetchConfig(ix.cfg.Head, workers, batchSize)
	ix.cfg.Metrics.IndexBlockFetchReorderDepth(ix.cfg.Head, 0)
	defer ix.cfg.Metrics.IndexBlockFetchReorderDepth(ix.cfg.Head, 0)

	type fetchJob struct {
		sequence int
		numbers  []uint64
	}
	type fetchResult struct {
		fetchJob
		blocks []*types.Block
		err    error
	}

	workCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	jobs := make(chan fetchJob, workers)
	results := make(chan fetchResult, workers)
	permits := make(chan struct{}, workers)
	var workerWG sync.WaitGroup
	workerWG.Add(workers)
	for range workers {
		go func() {
			defer workerWG.Done()
			for job := range jobs {
				blocks, err := ix.fetchBlockChunk(workCtx, job.numbers)
				select {
				case results <- fetchResult{fetchJob: job, blocks: blocks, err: err}:
				case <-workCtx.Done():
					return
				}
				if err != nil {
					return
				}
			}
		}()
	}

	// Feed work from a goroutine so workers may complete and publish results
	// while the bounded jobs channel is full. A synchronous producer here can
	// deadlock: all workers block on a full results channel while the caller is
	// still blocked trying to enqueue the next job.
	producerDone := make(chan struct{})
	go func() {
		defer close(producerDone)
		defer close(jobs)
		sequence := 0
		for first := from; ; sequence++ {
			last := to
			// Compare the zero-based distance, not a count of to-first+1:
			// the latter overflows for the full uint64 range.
			if to-first >= uint64(batchSize) {
				last = first + uint64(batchSize) - 1
			}
			numbers := make([]uint64, last-first+1)
			for i := range numbers {
				numbers[i] = first + uint64(i)
			}
			select {
			case permits <- struct{}{}:
			case <-workCtx.Done():
				return
			}
			select {
			case jobs <- fetchJob{sequence: sequence, numbers: numbers}:
			case <-workCtx.Done():
				return
			}
			if last == to {
				return
			}
			first = last + 1
		}
	}()
	go func() {
		workerWG.Wait()
		close(results)
	}()

	// Fetching may finish out of order, but reduction may not: source encounter
	// order is part of the root. Hold at most the bounded in-flight window and
	// reduce chunks strictly by sequence, then transactions in block-body order.
	next := 0
	pending := make(map[int]fetchResult, workers)
	var firstErr error
	for result := range results {
		if firstErr != nil {
			continue // Drain until every worker has observed cancellation.
		}
		if result.err != nil {
			firstErr = fmt.Errorf("chain: reading L1 blocks for blob-txs source %s: %w", src.Address, result.err)
			cancel()
			continue
		}
		pending[result.sequence] = result
		ix.cfg.Metrics.IndexBlockFetchReorderDepth(ix.cfg.Head, len(pending))
		for {
			ready, ok := pending[next]
			if !ok {
				break
			}
			delete(pending, next)
			ix.cfg.Metrics.IndexBlockFetchReorderDepth(ix.cfg.Head, len(pending))
			for i, block := range ready.blocks {
				if err := ix.scanBlobTxBlock(src, ready.numbers[i], block, b); err != nil {
					firstErr = err
					cancel()
					break
				}
			}
			if firstErr != nil {
				break
			}
			next++
			<-permits
		}
	}
	<-producerDone
	if firstErr == nil && ctx.Err() != nil {
		return ctx.Err()
	}
	return firstErr
}

func (ix *Indexer) fetchBlockChunk(ctx context.Context, numbers []uint64) (blocks []*types.Block, err error) {
	started := time.Now()
	mode := metrics.IndexBlockFetchFallback
	attempted := 0
	defer func() {
		ix.cfg.Metrics.IndexBlockFetch(ix.cfg.Head, mode, err == nil, attempted, time.Since(started))
	}()
	ix.cfg.Metrics.IndexBlockFetchInFlight(ix.cfg.Head, 1)
	defer ix.cfg.Metrics.IndexBlockFetchInFlight(ix.cfg.Head, -1)

	if batch, ok := ix.cfg.Chain.(BlockBatchClient); ok {
		mode = metrics.IndexBlockFetchBatch
		attempted = len(numbers)
		blocks, err = batch.BlocksByNumber(ctx, numbers)
	} else {
		blocks = make([]*types.Block, len(numbers))
		for i, number := range numbers {
			attempted++
			blocks[i], err = ix.cfg.Chain.BlockByNumber(ctx, new(big.Int).SetUint64(number))
			if err != nil {
				err = fmt.Errorf("chain: reading L1 block %d: %w", number, err)
				break
			}
		}
	}
	if err != nil {
		return nil, err
	}
	if len(blocks) != len(numbers) {
		return nil, fmt.Errorf("chain: block batch returned %d blocks for %d requested numbers", len(blocks), len(numbers))
	}
	for i, block := range blocks {
		if block == nil {
			return nil, fmt.Errorf("chain: block batch returned nil for L1 block %d", numbers[i])
		}
		if got := block.NumberU64(); got != numbers[i] {
			return nil, fmt.Errorf("chain: block batch returned L1 block %d at position for block %d", got, numbers[i])
		}
	}
	return blocks, nil
}

func (ix *Indexer) scanBlobTxBlock(src Source, number uint64, block *types.Block, b *rowBuilder) error {
	slot, err := ix.slotOf(block.Header())
	if err != nil {
		return err
	}
	for _, tx := range block.Transactions() {
		if tx.Type() != types.BlobTxType {
			continue
		}
		if dst := tx.To(); dst == nil || *dst != src.Address {
			continue
		}
		// The sender is recovered from the signature, never taken from the
		// node's rendered "from": a blob-txs source is a claim that a specific
		// sequencer posted these blobs (spec 10.4), and the only unforgeable
		// answer to who sent a transaction is the ECDSA recovery its own
		// signature admits. LatestSignerForChainID, keyed by the transaction's
		// own chain id, is the scheme every type-3 transaction is signed under.
		sender, err := types.Sender(types.LatestSignerForChainID(tx.ChainId()), tx)
		if err != nil {
			return fmt.Errorf("chain: recovering the sender of tx %s (L1 block %d): %w", tx.Hash(), number, err)
		}
		if !src.senderAllowed(sender) {
			continue
		}
		hashes := tx.BlobHashes()
		if len(hashes) == 0 {
			return fmt.Errorf("chain: tx %s is a blob transaction carrying no blob hashes", tx.Hash())
		}
		if err := ix.recordMatch(b, slot, hashes, tx.Hash()); err != nil {
			return err
		}
	}
	return nil
}

// recordMatch merges one matched transaction's blob hashes into slot's row,
// after the one check every match must pass whatever source found it: a row
// below the head's origin_slot names a slot the head is defined never to cover
// (spec 4), and would 409 the whole batch. resume never starts a scan that low,
// so reaching here is a bug worth a legible error rather than a wire fault.
func (ix *Indexer) recordMatch(b *rowBuilder, slot uint64, hashes []common.Hash, txHash common.Hash) error {
	if slot < ix.origin {
		return fmt.Errorf("chain: tx %s lands on slot %d, below head %q's origin_slot %d",
			txHash, slot, ix.cfg.Head, ix.origin)
	}
	b.add(slot, hashes)
	return nil
}

// slotOfHash returns the beacon slot of the L1 block with this hash.
func (ix *Indexer) slotOfHash(ctx context.Context, hash common.Hash) (uint64, error) {
	h, err := ix.cfg.Chain.HeaderByHash(ctx, hash)
	if err != nil {
		return 0, fmt.Errorf("chain: reading L1 header %s: %w", hash, err)
	}
	return ix.slotOf(h)
}

// slotOfBlock returns the beacon slot of the L1 block with this number.
func (ix *Indexer) slotOfBlock(ctx context.Context, number uint64) (uint64, error) {
	h, err := ix.cfg.Chain.HeaderByNumber(ctx, new(big.Int).SetUint64(number))
	if err != nil {
		return 0, fmt.Errorf("chain: reading L1 header %d: %w", number, err)
	}
	return ix.slotOf(h)
}

// slotOf is spec 10.2's "slot = (block.timestamp - genesis_time) /
// SECONDS_PER_SLOT", which is also exactly what nitro's BlobClient computes
// before it asks for a block's blobs (util/headerreader/blob_client.go). The
// two must agree: this decides where a blob is filed, and that decides where
// nitro looks for it.
func (ix *Indexer) slotOf(h *types.Header) (uint64, error) {
	if h.Time < ix.cfg.GenesisTime {
		return 0, fmt.Errorf("chain: L1 block %s has timestamp %d, before the beacon genesis time %d; "+
			"the configured genesis_time does not belong to this chain", h.Number, h.Time, ix.cfg.GenesisTime)
	}
	return (h.Time - ix.cfg.GenesisTime) / ix.cfg.SecondsPerSlot, nil
}

// fetchAndPut is the fetch_blobs: true half of spec 10.2: fetch exactly the vhs
// the scan saw, from the upstream, and post them to the archive.
func (ix *Indexer) fetchAndPut(ctx context.Context, rows []archclient.Row) error {
	for _, row := range rows {
		res, err := ix.cfg.Upstream.Blobs(ctx, row.Slot, row.VHs)
		if err != nil {
			return fmt.Errorf("chain: fetching %d blobs for slot %d: %w", len(row.VHs), row.Slot, err)
		}
		switch res.Status {
		case upstream.StatusFound:
			// Spec 7.1 answers a filtered request with one blob per requested
			// vh in request order, and package upstream has already checked the
			// count, so these pair up positionally with row.VHs.
		case upstream.StatusAbsent:
			// The matched transaction says these blobs were posted at this slot;
			// the upstream says it has not got them. On a beacon-node upstream
			// that is retention (the blobs are gone), on an archive upstream it
			// is a hole. Either way this indexer cannot record the row, and
			// must not skip it: skipping would advance coverage over a slot
			// whose blobs it knows exist, turning a retryable 503 into a
			// permanent 404.
			return fmt.Errorf("chain: upstream does not have the blobs this chain posted at slot %d "+
				"(%d versioned hashes, first %s); if this is a beacon node, the slot is likely past its retention "+
				"and the upstream must be an archive", row.Slot, len(row.VHs), archclient.VHHex(row.VHs[0]))
		case upstream.StatusNotYetCovered:
			return fmt.Errorf("chain: upstream has not covered slot %d yet", row.Slot)
		default:
			return fmt.Errorf("chain: slot %d: upstream returned unknown status %v", row.Slot, res.Status)
		}

		put, err := ix.put(ctx, res.Blobs)
		if err != nil {
			return err
		}
		// The check spec 10.1 asks of the beacon indexer, free here: this
		// indexer knew what it wanted before it asked, so the archive's derived
		// vhs can be compared against the matched transaction's own list without
		// recomputing a commitment. A mismatch means the bytes that reached the
		// archive are not the blobs this chain posted.
		if len(put) != len(row.VHs) {
			return fmt.Errorf("chain: slot %d: put %d blobs, archive answered for %d", row.Slot, len(row.VHs), len(put))
		}
		for i, p := range put {
			if p.VH != row.VHs[i] {
				return fmt.Errorf("chain: slot %d: blob %d was posted by this chain as %s but the archive stored it as %s; "+
					"the blob was corrupted between the upstream and the archive",
					row.Slot, i, archclient.VHHex(row.VHs[i]), archclient.VHHex(p.VH))
			}
		}
	}
	return nil
}

// put posts blobs in as many requests as max_put_blobs allows.
func (ix *Indexer) put(ctx context.Context, blobs [][]byte) ([]archclient.PutBlob, error) {
	out := make([]archclient.PutBlob, 0, len(blobs))
	for i := 0; i < len(blobs); i += ix.cfg.MaxPutBlobs {
		chunk := blobs[i:min(i+ix.cfg.MaxPutBlobs, len(blobs))]
		res, err := ix.cfg.Archive.PutBlobs(ctx, chunk)
		if err != nil {
			return nil, fmt.Errorf("chain: putting blobs [%d, %d): %w", i, i+len(chunk), err)
		}
		out = append(out, res...)
	}
	return out, nil
}

// waitForAll is the fetch_blobs: false half of spec 10.2: "require chain
// synced_to target <= ALL head synced_to; wait".
//
// # Why this waits rather than trimming the batch
//
// The alternative is to clamp: post refs only up to the ALL head's synced_to
// and re-scan the rest next pass. It would make progress where this blocks, and
// it is rejected anyway, for two reasons.
//
// The first is that the block is short by construction. Both indexers are
// pinned to the same finality (spec 10.3) -- one to the beacon finalized
// checkpoint, one to the L1 finalized block, which is the same checkpoint seen
// from the execution layer. The ALL head is therefore never far behind this
// target, and the wait is the beacon indexer's next pass, not an outage. If it
// *is* an outage, blocking is the correct behaviour and the log line says so:
// this head cannot record refs to blobs that are not in the archive, and
// pretending otherwise means a 409 on every attempt.
//
// The second is that clamping quietly changes what a head means. Coverage is a
// claim that a slot has been decided about; advancing it to wherever another
// head happens to have reached is advancing it for a reason that has nothing to
// do with this chain. The batch boundary would then depend on the beacon
// indexer's timing, which is to say the head's shape would depend on a race --
// and spec 11.5 requires two independent runs over the same data to produce the
// same root. Waiting keeps the boundary a function of the L1 range alone.
func (ix *Indexer) waitForAll(ctx context.Context, target uint64) error {
	for first := true; ; first = false {
		syncedTo, err := ix.cfg.Archive.SyncedTo(ctx, ix.cfg.AllHead)
		if err != nil {
			return fmt.Errorf("chain: reading synced_to of the all head %q: %w", ix.cfg.AllHead, err)
		}
		if syncedTo != nil && *syncedTo >= target {
			return nil
		}
		if first {
			var allSyncedTo any = "none"
			if syncedTo != nil {
				allSyncedTo = *syncedTo
			}
			ix.log.Info("waiting for the all head to cover this batch",
				"head", ix.cfg.Head, "all_head", ix.cfg.AllHead, "target", target, "all_synced_to", allSyncedTo)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(ix.cfg.PollInterval):
		}
	}
}

// rowBuilder merges the blob hashes of every matched transaction into one row
// per slot, in encounter order, deduplicating within a row (spec 10.2, 10.4).
//
// Encounter order is the order add is called in, which scan drives: source-list
// order first (source 0's matches before source 1's), then within a source
// ascending by L1 block and by index within a block, and finally a transaction's
// own BlobHashes in the order it posted them. So a row is the order this chain
// posted its blobs in, which is the order they should be read back in.
//
// Dedup is within a row only. The same blob posted twice in one slot -- by one
// transaction, or by two sources that both select it -- is one ref, held in the
// position of its first encounter; the same blob referenced from two different
// slots is two rows, both correct, both pointing at the one block on disk.
type rowBuilder struct {
	order []uint64
	byOrd map[uint64]*slotRow
}

type slotRow struct {
	vhs  []schema.VersionedHash
	seen map[schema.VersionedHash]bool
}

func newRowBuilder() *rowBuilder {
	return &rowBuilder{byOrd: make(map[uint64]*slotRow)}
}

// add merges one transaction's blob hashes into slot's row.
func (b *rowBuilder) add(slot uint64, hashes []common.Hash) {
	row, ok := b.byOrd[slot]
	if !ok {
		row = &slotRow{seen: make(map[schema.VersionedHash]bool, len(hashes))}
		b.byOrd[slot] = row
		b.order = append(b.order, slot)
	}
	for _, h := range hashes {
		vh := schema.VersionedHash(h)
		if row.seen[vh] {
			continue
		}
		row.seen[vh] = true
		row.vhs = append(row.vhs, vh)
	}
}

// rows renders the merged rows, sorted ascending by slot as spec 5.1 requires.
//
// The sort is not decoration even though the scan reads blocks in ascending
// order: two L1 blocks can land in the same slot, and while that keeps slots
// non-decreasing rather than shuffled, the map iteration this reads from has no
// order at all.
func (b *rowBuilder) rows() []archclient.Row {
	slots := make([]uint64, len(b.order))
	copy(slots, b.order)
	sort.Slice(slots, func(i, j int) bool { return slots[i] < slots[j] })

	out := make([]archclient.Row, 0, len(slots))
	for _, s := range slots {
		out = append(out, archclient.Row{Slot: s, VHs: b.byOrd[s].vhs})
	}
	return out
}
