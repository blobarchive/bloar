package chain

import (
	"context"
	"errors"
	"math/big"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/core/types"
)

type recordingBatchChain struct {
	*fakeChain
	active      atomic.Int32
	maxActive   atomic.Int32
	mu          sync.Mutex
	calls       [][]uint64
	completions []uint64
	firstDelay  time.Duration
	normalDelay time.Duration
}

func (c *recordingBatchChain) BlocksByNumber(ctx context.Context, numbers []uint64) ([]*types.Block, error) {
	active := c.active.Add(1)
	defer c.active.Add(-1)
	for {
		max := c.maxActive.Load()
		if active <= max || c.maxActive.CompareAndSwap(max, active) {
			break
		}
	}
	c.mu.Lock()
	c.calls = append(c.calls, append([]uint64(nil), numbers...))
	c.mu.Unlock()

	delay := c.normalDelay
	if numbers[0] == 0 {
		delay = c.firstDelay
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	blocks := make([]*types.Block, len(numbers))
	for i, number := range numbers {
		if number >= uint64(len(c.blocks)) {
			return nil, errors.New("out of range")
		}
		blocks[i] = c.blocks[number]
	}
	c.mu.Lock()
	c.completions = append(c.completions, numbers[0])
	c.mu.Unlock()
	return blocks, nil
}

func TestScanBlobTxsBatchesInParallelButReducesInCanonicalOrder(t *testing.T) {
	builder := newChainBuilder(t)
	for n := byte(1); n <= 12; n++ {
		builder.addBlock(400, txEntry{tx: blobTx(t, keyA, testInbox, uint64(n), hashes(n))})
	}
	chain := &recordingBatchChain{
		fakeChain:   builder.chain(),
		firstDelay:  50 * time.Millisecond,
		normalDelay: 2 * time.Millisecond,
	}
	ix := newTestIndexer(chain, []Source{blobTxs(testInbox, 0, senderA)})
	ix.cfg.BlockFetchConcurrency = 3
	ix.cfg.RPCBatchSize = 2

	rows, err := ix.scan(context.Background(), 0, 11)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if got := chain.maxActive.Load(); got < 2 || got > 3 {
		t.Fatalf("max concurrent batches = %d, want in [2,3]", got)
	}
	chain.mu.Lock()
	calls := append([][]uint64(nil), chain.calls...)
	completions := append([]uint64(nil), chain.completions...)
	chain.mu.Unlock()
	if len(calls) != 6 {
		t.Fatalf("batch calls = %d, want 6: %v", len(calls), calls)
	}
	seen := make(map[uint64][]uint64, len(calls))
	for _, call := range calls {
		seen[call[0]] = call
	}
	for first := uint64(0); first < 12; first += 2 {
		want := []uint64{first, first + 1}
		if got := seen[first]; len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
			t.Fatalf("chunk from %d = %v, want %v", first, got, want)
		}
	}
	if len(completions) < 2 || completions[0] == 0 {
		t.Fatalf("completion order = %v, want a later chunk before delayed chunk 0", completions)
	}
	assertRow(t, rowsBySlot(rows), 400, vhs(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12))
}

type recordingFallbackChain struct {
	*fakeChain
	active    atomic.Int32
	maxActive atomic.Int32
	calls     atomic.Int32
}

func (c *recordingFallbackChain) BlockByNumber(ctx context.Context, number *big.Int) (*types.Block, error) {
	active := c.active.Add(1)
	defer c.active.Add(-1)
	c.calls.Add(1)
	for {
		max := c.maxActive.Load()
		if active <= max || c.maxActive.CompareAndSwap(max, active) {
			break
		}
	}
	timer := time.NewTimer(3 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return c.fakeChain.BlockByNumber(ctx, number)
}

func TestScanBlobTxsFallsBackToBoundedBlockByNumberWorkers(t *testing.T) {
	builder := newChainBuilder(t)
	for n := byte(1); n <= 10; n++ {
		builder.addBlock(500, txEntry{tx: blobTx(t, keyA, testInbox, uint64(n), hashes(n))})
	}
	chain := &recordingFallbackChain{fakeChain: builder.chain()}
	ix := newTestIndexer(chain, []Source{blobTxs(testInbox, 0, senderA)})
	ix.cfg.BlockFetchConcurrency = 4
	ix.cfg.RPCBatchSize = 3

	rows, err := ix.scan(context.Background(), 0, 9)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if got := chain.calls.Load(); got != 10 {
		t.Fatalf("BlockByNumber calls = %d, want 10", got)
	}
	if got := chain.maxActive.Load(); got < 2 || got > 4 {
		t.Fatalf("max concurrent BlockByNumber calls = %d, want in [2,4]", got)
	}
	assertRow(t, rowsBySlot(rows), 500, vhs(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
}

func TestScanBlobTxsModesAreByteEquivalentToSerial(t *testing.T) {
	builder := newChainBuilder(t)
	for n := byte(1); n <= 9; n++ {
		slot := uint64(800) + uint64(n)/3
		builder.addBlock(slot,
			txEntry{tx: blobTx(t, keyA, testInbox, uint64(n), hashes(n))},
			txEntry{tx: blobTx(t, keyB, testInbox, uint64(n)+100, hashes(n+100))},
		)
	}
	source := blobTxs(testInbox, 0, senderA)

	serial := newTestIndexer(builder.chain(), []Source{source})
	serial.cfg.BlockFetchConcurrency = 1
	serial.cfg.RPCBatchSize = 1
	want, err := serial.scan(context.Background(), 0, 8)
	if err != nil {
		t.Fatalf("serial scan: %v", err)
	}

	tests := []struct {
		name      string
		chain     ChainClient
		workers   int
		batchSize int
	}{
		{
			name:      "parallel-only fallback",
			chain:     &recordingFallbackChain{fakeChain: builder.chain()},
			workers:   4,
			batchSize: 1,
		},
		{
			name: "batch-only",
			chain: &recordingBatchChain{
				fakeChain:  builder.chain(),
				firstDelay: 10 * time.Millisecond,
			},
			workers:   1,
			batchSize: 3,
		},
		{
			name: "parallel plus batch with adversarial completion order",
			chain: &recordingBatchChain{
				fakeChain:   builder.chain(),
				firstDelay:  20 * time.Millisecond,
				normalDelay: time.Millisecond,
			},
			workers:   4,
			batchSize: 3,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ix := newTestIndexer(tt.chain, []Source{source})
			ix.cfg.BlockFetchConcurrency = tt.workers
			ix.cfg.RPCBatchSize = tt.batchSize
			got, err := ix.scan(context.Background(), 0, 8)
			if err != nil {
				t.Fatalf("scan: %v", err)
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("rows differ from serial:\n got  %#v\n want %#v", got, want)
			}
		})
	}
}

type functionBatchChain struct {
	*fakeChain
	fn func(context.Context, []uint64) ([]*types.Block, error)
}

func (c *functionBatchChain) BlocksByNumber(ctx context.Context, numbers []uint64) ([]*types.Block, error) {
	return c.fn(ctx, numbers)
}

func TestScanBlobTxsCancelsAndJoinsWorkersOnFetchError(t *testing.T) {
	builder := newChainBuilder(t)
	for n := byte(1); n <= 8; n++ {
		builder.addBlock(600, txEntry{tx: blobTx(t, keyA, testInbox, uint64(n), hashes(n))})
	}
	var started atomic.Int32
	var active atomic.Int32
	chain := &functionBatchChain{fakeChain: builder.chain()}
	chain.fn = func(ctx context.Context, numbers []uint64) ([]*types.Block, error) {
		started.Add(1)
		active.Add(1)
		defer active.Add(-1)
		if numbers[0] == 0 {
			deadline := time.NewTimer(time.Second)
			defer deadline.Stop()
			for started.Load() < 2 {
				select {
				case <-time.After(time.Millisecond):
				case <-deadline.C:
					return nil, errors.New("workers did not overlap")
				}
			}
			return nil, errors.New("synthetic batch failure")
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}
	ix := newTestIndexer(chain, []Source{blobTxs(testInbox, 0, senderA)})
	ix.cfg.BlockFetchConcurrency = 3
	ix.cfg.RPCBatchSize = 2

	done := make(chan error, 1)
	go func() {
		_, err := ix.scan(context.Background(), 0, 7)
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "synthetic batch failure") {
			t.Fatalf("scan error = %v, want synthetic batch failure", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("scan did not return after a worker failed")
	}
	if got := active.Load(); got != 0 {
		t.Fatalf("active workers after scan returned = %d, want 0", got)
	}
}

func TestFetchBlockChunkRejectsBrokenBatchContracts(t *testing.T) {
	builder := newChainBuilder(t)
	builder.addBlock(700)
	builder.addBlock(701)
	tests := []struct {
		name string
		fn   func([]uint64) []*types.Block
		want string
	}{
		{"wrong length", func([]uint64) []*types.Block { return []*types.Block{builder.blocks[0]} }, "1 blocks for 2"},
		{"nil block", func([]uint64) []*types.Block { return []*types.Block{builder.blocks[0], nil} }, "returned nil"},
		{"duplicate block", func([]uint64) []*types.Block { return []*types.Block{builder.blocks[0], builder.blocks[0]} }, "at position"},
		{"wrong number", func([]uint64) []*types.Block { return []*types.Block{builder.blocks[1], builder.blocks[0]} }, "at position"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			chain := &functionBatchChain{fakeChain: builder.chain()}
			chain.fn = func(_ context.Context, numbers []uint64) ([]*types.Block, error) {
				return tt.fn(numbers), nil
			}
			ix := newTestIndexer(chain, nil)
			_, err := ix.fetchBlockChunk(context.Background(), []uint64{0, 1})
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("fetchBlockChunk error = %v, want %q", err, tt.want)
			}
		})
	}
}
