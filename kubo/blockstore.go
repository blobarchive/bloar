package kubo

import (
	"context"
	"errors"
	"fmt"

	"github.com/ipfs/boxo/blockstore"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
)

const (
	// DefaultMaxPutManyBlocks bounds one adapter-level PutMany call. Each block
	// remains independently bounded by Client.maxBlockBytes.
	DefaultMaxPutManyBlocks = 64
)

var (
	// ErrLocalBlockAbsent identifies an authoritative miss from Kubo's local
	// blockstore. It is deliberately different from ErrBlockFetchFailed: GC and
	// offline archive logic may act on a local miss, but must not reinterpret a
	// failed network request as proof of local absence.
	ErrLocalBlockAbsent = errors.New("kubo: block is absent locally")

	// ErrBlockFetchFailed identifies failure of the network-capable follower
	// path, including failure to make a fetched block durable locally.
	ErrBlockFetchFailed = errors.New("kubo: block fetch failed")
)

// LocalBlockAbsentError identifies the exact CID Kubo could not read with its
// offline exchange. It also unwraps to ipld.ErrNotFound so LocalBlockstore
// satisfies the blockstore convention used by Bloar's archive and GC logic.
type LocalBlockAbsentError struct {
	CID   cid.Cid
	Cause error
}

func (e *LocalBlockAbsentError) Error() string {
	if e == nil {
		return "<nil>"
	}
	return fmt.Sprintf("kubo: local block %s is absent", e.CID)
}

// Unwrap preserves the stable Kubo class, the conventional IPFS blockstore
// class, and the client's bounded/redacted RPC error.
func (e *LocalBlockAbsentError) Unwrap() []error {
	if e == nil {
		return nil
	}
	errs := []error{ErrLocalBlockAbsent, ipld.ErrNotFound{Cid: e.CID}}
	if e.Cause != nil {
		errs = append(errs, e.Cause)
	}
	return errs
}

// BlockFetchError distinguishes the network-capable follower path from a
// local-only miss. Operation is one of "fetch" or "cache" and is set only by
// this package.
type BlockFetchError struct {
	CID       cid.Cid
	Operation string
	Cause     error
}

func (e *BlockFetchError) Error() string {
	if e == nil {
		return "<nil>"
	}
	return fmt.Sprintf("kubo: %s block %s failed: %v", e.Operation, e.CID, e.Cause)
}

// Unwrap exposes both the stable fetch class and the bounded/redacted cause.
func (e *BlockFetchError) Unwrap() []error {
	if e == nil || e.Cause == nil {
		return []error{ErrBlockFetchFailed}
	}
	return []error{ErrBlockFetchFailed, e.Cause}
}

// BlockstoreConfig supplies the finite budgets needed to adapt Kubo's RPC to
// boxo's Blockstore interface.
type BlockstoreConfig struct {
	// Enumeration bounds one refs/local snapshot used by AllKeysChan. Both
	// fields are mandatory; see ListLimits.
	Enumeration ListLimits

	// MaxPutManyBlocks may tighten the fixed default, but cannot raise it. Zero
	// selects DefaultMaxPutManyBlocks.
	MaxPutManyBlocks int
}

// LocalBlockstore is a validating, strictly local view of a managed Kubo repo.
// Its read methods pass offline=true to the Kubo command API, which replaces
// Kubo's network exchange with an offline exchange for that atomic request.
type LocalBlockstore struct {
	client           *Client
	enumeration      ListLimits
	maxPutManyBlocks int
}

var _ blockstore.Blockstore = (*LocalBlockstore)(nil)

// NewLocalBlockstore validates the adapter's finite collection budgets. It
// does not make an RPC; managed-backend construction is responsible for the
// compatibility and ownership checks before exposing this view.
func NewLocalBlockstore(client *Client, cfg BlockstoreConfig) (*LocalBlockstore, error) {
	if client == nil {
		return nil, errors.New("kubo: local blockstore requires a client")
	}
	if err := client.validateListLimits("refs/local", cfg.Enumeration); err != nil {
		return nil, err
	}
	maxPutManyBlocks := cfg.MaxPutManyBlocks
	if maxPutManyBlocks == 0 {
		maxPutManyBlocks = DefaultMaxPutManyBlocks
	}
	if maxPutManyBlocks < 1 || maxPutManyBlocks > DefaultMaxPutManyBlocks {
		return nil, fmt.Errorf("kubo: blockstore MaxPutManyBlocks must be between 1 and %d", DefaultMaxPutManyBlocks)
	}
	return &LocalBlockstore{
		client:           client,
		enumeration:      cfg.Enumeration,
		maxPutManyBlocks: maxPutManyBlocks,
	}, nil
}

// BlockGetLocal reads and verifies one block without allowing Kubo to consult
// its exchange. The offline flag is a global Kubo command option; in Kubo
// 0.42.x it constructs an offline CoreAPI for this request before block/get.
func (c *Client) BlockGetLocal(ctx context.Context, expected cid.Cid) (blocks.Block, error) {
	return c.blockGetMode(ctx, expected, true)
}

// BlockFetch reads and verifies one block through Kubo's online block service.
// Callers that require durable follower semantics must store the result before
// returning it; FetchingBlockstore does so explicitly rather than depending on
// Kubo's configurable write-through behavior.
func (c *Client) BlockFetch(ctx context.Context, expected cid.Cid) (blocks.Block, error) {
	return c.blockGetMode(ctx, expected, false)
}

func (c *Client) blockGetMode(ctx context.Context, expected cid.Cid, offline bool) (blocks.Block, error) {
	const endpoint = "block/get"
	expectedText, err := boundedCIDArgument(endpoint, expected)
	if err != nil {
		return nil, err
	}
	query := jsonQuery()
	query.Set("arg", expectedText)
	if offline {
		query.Set("offline", "true")
	} else {
		query.Set("offline", "false")
	}
	raw, err := c.post(ctx, endpoint, query, nil, "", "application/vnd.ipld.raw", c.maxBlockBytes)
	if err != nil {
		return nil, c.asNotFound(endpoint, expected, err)
	}
	if err := verifyCID(expected, raw); err != nil {
		return nil, c.protocol(endpoint, "%v", err)
	}
	block, err := blocks.NewBlockWithCid(raw, expected)
	if err != nil {
		return nil, c.protocol(endpoint, "constructing verified block: %v", err)
	}
	return block, nil
}

func (s *LocalBlockstore) DeleteBlock(ctx context.Context, target cid.Cid) error {
	return s.client.BlockRemove(ctx, target)
}

func (s *LocalBlockstore) Has(ctx context.Context, target cid.Cid) (bool, error) {
	_, err := s.Get(ctx, target)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, ErrLocalBlockAbsent) {
		return false, nil
	}
	return false, err
}

// Get verifies returned bytes against target. A Kubo not-found response is
// promoted to the local-absence class and never triggers a network request.
func (s *LocalBlockstore) Get(ctx context.Context, target cid.Cid) (blocks.Block, error) {
	block, err := s.client.BlockGetLocal(ctx, target)
	if errors.Is(err, ErrNotFound) {
		return nil, &LocalBlockAbsentError{CID: target, Cause: err}
	}
	return block, err
}

func (s *LocalBlockstore) GetSize(ctx context.Context, target cid.Cid) (int, error) {
	block, err := s.Get(ctx, target)
	if err != nil {
		return 0, err
	}
	return len(block.RawData()), nil
}

func (s *LocalBlockstore) Put(ctx context.Context, block blocks.Block) error {
	_, err := s.client.BlockPut(ctx, block)
	return err
}

// PutMany preflights the complete bounded batch before issuing the first RPC.
// A later transport failure may still leave a prefix written, matching the
// Blockstore interface's ordinary partial-batch behavior.
func (s *LocalBlockstore) PutMany(ctx context.Context, input []blocks.Block) error {
	if len(input) > s.maxPutManyBlocks {
		return fmt.Errorf("kubo: blockstore PutMany has %d blocks, over the %d-block limit", len(input), s.maxPutManyBlocks)
	}
	for i, block := range input {
		if err := s.validatePutBlock(block); err != nil {
			return fmt.Errorf("kubo: blockstore PutMany block %d: %w", i, err)
		}
	}
	for _, block := range input {
		if err := s.Put(ctx, block); err != nil {
			return err
		}
	}
	return nil
}

func (s *LocalBlockstore) validatePutBlock(block blocks.Block) error {
	if block == nil || !block.Cid().Defined() {
		return errors.New("requires a block with a defined CID")
	}
	data := block.RawData()
	if int64(len(data)) > s.client.maxBlockBytes {
		return fmt.Errorf("block is %d bytes, over the %d-byte limit", len(data), s.client.maxBlockBytes)
	}
	if _, err := bloarCodecName(block.Cid()); err != nil {
		return err
	}
	if err := verifyCID(block.Cid(), data); err != nil {
		return err
	}
	return nil
}

// AllKeysChan returns one bounded snapshot of Kubo's local block keys. Kubo's
// refs/local command uses raw-codec CIDs because its underlying blockstore is
// keyed by multihash; that is the same contract as boxo Blockstore.
func (s *LocalBlockstore) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	refs, err := s.client.RefsLocal(ctx, s.enumeration)
	if err != nil {
		return nil, err
	}
	return bufferedCIDSnapshot(ctx, refs)
}

func bufferedCIDSnapshot(ctx context.Context, refs []cid.Cid) (<-chan cid.Cid, error) {
	// The snapshot is already bounded and resident, so buffer it completely.
	// A caller that abandons the returned channel cannot strand a producer
	// goroutine; this adapter therefore adds no lifecycle obligation beyond the
	// context that bounded the RPC itself. Cancellation discards the channel
	// instead of returning a partial snapshot as if it were complete.
	out := make(chan cid.Cid, len(refs))
	for _, ref := range refs {
		if cause := context.Cause(ctx); cause != nil {
			return nil, cause
		}
		out <- ref
	}
	close(out)
	return out, nil
}

// FetchingBlockstore layers network-capable follower reads over the strict
// local view. Mutations and enumeration always remain local.
type FetchingBlockstore struct {
	local *LocalBlockstore
}

var _ blockstore.Blockstore = (*FetchingBlockstore)(nil)

func NewFetchingBlockstore(local *LocalBlockstore) (*FetchingBlockstore, error) {
	if local == nil {
		return nil, errors.New("kubo: fetching blockstore requires a local blockstore")
	}
	return &FetchingBlockstore{local: local}, nil
}

func (s *FetchingBlockstore) DeleteBlock(ctx context.Context, target cid.Cid) error {
	return s.local.DeleteBlock(ctx, target)
}

func (s *FetchingBlockstore) Has(ctx context.Context, target cid.Cid) (bool, error) {
	_, err := s.Get(ctx, target)
	if err != nil {
		return false, err
	}
	return true, nil
}

func (s *FetchingBlockstore) Get(ctx context.Context, target cid.Cid) (blocks.Block, error) {
	block, err := s.local.Get(ctx, target)
	if err == nil || !errors.Is(err, ErrLocalBlockAbsent) {
		return block, err
	}

	block, err = s.local.client.BlockFetch(ctx, target)
	if err != nil {
		return nil, &BlockFetchError{CID: target, Operation: "fetch", Cause: err}
	}
	// Store explicitly even though Kubo commonly caches online block/get. That
	// behavior is configurable; follower durability is not.
	if err := s.local.Put(ctx, block); err != nil {
		return nil, &BlockFetchError{CID: target, Operation: "cache", Cause: err}
	}
	return block, nil
}

func (s *FetchingBlockstore) GetSize(ctx context.Context, target cid.Cid) (int, error) {
	block, err := s.Get(ctx, target)
	if err != nil {
		return 0, err
	}
	return len(block.RawData()), nil
}

func (s *FetchingBlockstore) Put(ctx context.Context, block blocks.Block) error {
	return s.local.Put(ctx, block)
}

func (s *FetchingBlockstore) PutMany(ctx context.Context, input []blocks.Block) error {
	return s.local.PutMany(ctx, input)
}

func (s *FetchingBlockstore) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	return s.local.AllKeysChan(ctx)
}
