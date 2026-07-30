package chain

import (
	"context"
	"crypto/ecdsa"
	"fmt"
	"log/slog"
	"math/big"
	"reflect"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/holiman/uint256"

	"github.com/blobarchive/bloar/index/archclient"
	"github.com/blobarchive/bloar/schema"
)

// The scan tests drive Indexer.scan directly against an in-memory ChainClient.
// scan is the whole of what the source-schedule scanner changed -- the source schedule, the two
// per-type scanners, and the encounter-order union -- and it needs neither an
// archive nor the resume/Step machinery to exercise, so these bypass both and
// assert on the rows scan returns.

const (
	testGenesis = 1_600_000_000
	testSPS     = 12
)

// testChainID signs every synthetic transaction. The sender is recovered from
// the signature (that is the point of the allowlist), so the two keys below must
// produce two distinct, real senders.
var testChainID = big.NewInt(4242)

var (
	keyA, _ = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	keyB, _ = crypto.HexToECDSA("0000000000000000000000000000000000000000000000000000000000000042")

	senderA = crypto.PubkeyToAddress(keyA.PublicKey)

	testInbox = common.HexToAddress("0x00000000000000000000000000000000000000A1")
	otherAddr = common.HexToAddress("0x00000000000000000000000000000000000000B2")
	testTopic = SequencerBatchDeliveredTopic
)

func TestScanEncounterOrderDedup(t *testing.T) {
	b := newChainBuilder(t)
	// One inbox batch in slot 100 carrying [1, 2].
	tx := blobTx(t, keyA, testInbox, 0, hashes(1, 2))
	b.addBlock(100, txEntry{tx: tx, logAddr: testInbox, logTopic: testTopic})

	// Two inbox-events sources over the same inbox and topic, ranges that both
	// cover block 0. The one log is selected twice; the row must hold [1, 2]
	// once, in the position the first source gave it -- never [1, 2, 1, 2].
	ix := newTestIndexer(b.chain(), []Source{
		inboxRange(testInbox, 0, 0),
		inboxOpen(testInbox, 0),
	})
	rows, err := ix.scan(context.Background(), 0, 0)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(rows))
	}
	assertRow(t, rowsBySlot(rows), 100, vhs(1, 2))
}

func TestScanCrossSourceMergeOrder(t *testing.T) {
	b := newChainBuilder(t)
	// One block, slot 100, with two blob transactions: an inbox batch [1, 2] and
	// an EOA post [2, 3] to a different address, sharing hash 2.
	inboxTx := blobTx(t, keyA, testInbox, 0, hashes(1, 2))
	eoaTx := blobTx(t, keyA, otherAddr, 1, hashes(2, 3))
	b.addBlock(100,
		txEntry{tx: inboxTx, logAddr: testInbox, logTopic: testTopic},
		txEntry{tx: eoaTx},
	)

	// inbox-events first, blob-txs second. Encounter order is source-list order,
	// so the row is [1, 2] from source 0, then 3 appended by source 1 -- 2 is
	// already present and stays in its first-encounter position. Result [1, 2, 3].
	ix := newTestIndexer(b.chain(), []Source{
		inboxOpen(testInbox, 0),
		blobTxs(otherAddr, 0, senderA),
	})
	rows, err := ix.scan(context.Background(), 0, 0)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(rows))
	}
	assertRow(t, rowsBySlot(rows), 100, vhs(1, 2, 3))
}

func TestScanInboxEventsCanonicalRPCOrder(t *testing.T) {
	b := newChainBuilder(t)
	// Keep every transaction in one beacon slot so its encounter order is
	// visible inside one row. The first transaction also carries two hashes: the
	// normalization may order transactions, but must not reorder BlobHashes.
	b.addBlock(100,
		txEntry{tx: blobTx(t, keyA, testInbox, 0, hashes(1, 2)), logAddr: testInbox, logTopic: testTopic},
		txEntry{tx: blobTx(t, keyA, testInbox, 1, hashes(3)), logAddr: testInbox, logTopic: testTopic},
	)
	b.addBlock(100,
		txEntry{tx: blobTx(t, keyA, testInbox, 2, hashes(4, 5)), logAddr: testInbox, logTopic: testTopic},
	)
	fc := b.chain()
	canonical := append([]types.Log(nil), fc.logs...)

	cases := []struct {
		name string
		logs []types.Log
	}{
		{name: "ordered", logs: append([]types.Log(nil), canonical...)},
		{name: "reverse", logs: []types.Log{canonical[2], canonical[1], canonical[0]}},
		{name: "shuffled", logs: []types.Log{canonical[1], canonical[2], canonical[0]}},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			before := append([]types.Log(nil), tt.logs...)
			chain := &filterLogsResultChain{fakeChain: fc, result: tt.logs}
			ix := newTestIndexer(chain, []Source{inboxOpen(testInbox, 0)})
			rows, err := ix.scan(context.Background(), 0, 1)
			if err != nil {
				t.Fatalf("scan: %v", err)
			}
			if !reflect.DeepEqual(chain.result, before) {
				t.Fatalf("scan mutated the RPC-owned logs:\n got  %+v\n want %+v", chain.result, before)
			}
			if len(rows) != 1 {
				t.Fatalf("rows = %d, want 1", len(rows))
			}
			assertRow(t, rowsBySlot(rows), 100, vhs(1, 2, 3, 4, 5))
		})
	}
}

func TestScanInboxEventsRejectsMalformedLogOrderingMetadata(t *testing.T) {
	b := newChainBuilder(t)
	b.addBlock(100,
		txEntry{tx: blobTx(t, keyA, testInbox, 0, hashes(1)), logAddr: testInbox, logTopic: testTopic},
		txEntry{tx: blobTx(t, keyA, testInbox, 1, hashes(2)), logAddr: testInbox, logTopic: testTopic},
	)
	fc := b.chain()

	tests := []struct {
		name   string
		mutate func([]types.Log) []types.Log
		want   string
	}{
		{
			name: "outside requested range",
			mutate: func(logs []types.Log) []types.Log {
				logs[0].BlockNumber = 1
				return logs
			},
			want: "outside requested range [0, 0]",
		},
		{
			name: "one block number with inconsistent hashes",
			mutate: func(logs []types.Log) []types.Log {
				logs[1].BlockHash = common.HexToHash("0xdead")
				return logs
			},
			want: "inconsistent block hashes",
		},
		{
			name: "duplicate log position",
			mutate: func(logs []types.Log) []types.Log {
				return append(logs, logs[0])
			},
			want: "duplicate log position",
		},
		{
			name: "one transaction position with inconsistent hashes",
			mutate: func(logs []types.Log) []types.Log {
				logs[1].TxIndex = logs[0].TxIndex
				return logs
			},
			want: "with two hashes",
		},
		{
			name: "log index runs backwards across transactions",
			mutate: func(logs []types.Log) []types.Log {
				logs[0].Index = 2
				logs[1].Index = 1
				return logs
			},
			want: "impossible log ordering",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logs := tt.mutate(append([]types.Log(nil), fc.logs...))
			chain := &filterLogsResultChain{fakeChain: fc, result: logs}
			ix := newTestIndexer(chain, []Source{inboxOpen(testInbox, 0)})
			_, err := ix.scan(context.Background(), 0, 0)
			if err == nil {
				t.Fatal("scan accepted malformed FilterLogs metadata")
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error = %q, want it to contain %q", err, tt.want)
			}
		})
	}
}

func TestScanScheduleBoundaries(t *testing.T) {
	b := newChainBuilder(t)
	// Blocks 0..5, each an inbox batch in slot 200+n carrying vh(n+1).
	for n := uint64(0); n <= 5; n++ {
		tx := blobTx(t, keyA, testInbox, n, hashes(byte(n+1)))
		b.addBlock(200+n, txEntry{tx: tx, logAddr: testInbox, logTopic: testTopic})
	}
	fc := b.chain()

	t.Run("closed range contributes only within [from, until]", func(t *testing.T) {
		ix := newTestIndexer(fc, []Source{inboxRange(testInbox, 2, 4)})
		rows, err := ix.scan(context.Background(), 0, 5)
		if err != nil {
			t.Fatalf("scan: %v", err)
		}
		// Blocks 2, 3, 4 -> slots 202, 203, 204; blocks 0,1,5 carry matching txs
		// but sit outside the source's range and contribute nothing.
		assertSlots(t, rowsBySlot(rows), 202, 203, 204)
	})

	t.Run("open-ended runs to the scan end", func(t *testing.T) {
		ix := newTestIndexer(fc, []Source{inboxOpen(testInbox, 3)})
		rows, err := ix.scan(context.Background(), 0, 5)
		if err != nil {
			t.Fatalf("scan: %v", err)
		}
		assertSlots(t, rowsBySlot(rows), 203, 204, 205)
	})

	t.Run("blocks no source covers contribute nothing", func(t *testing.T) {
		ix := newTestIndexer(fc, []Source{inboxRange(testInbox, 0, 1)})
		rows, err := ix.scan(context.Background(), 0, 5)
		if err != nil {
			t.Fatalf("scan: %v", err)
		}
		assertSlots(t, rowsBySlot(rows), 200, 201)
	})
}

func TestScanBlobTxsFiltering(t *testing.T) {
	b := newChainBuilder(t)
	// Every case the blob-txs selector distinguishes, one per slot:
	//   300  type-3 to the target from the allowlisted sender      -> matched
	//   301  type-3 to the target from a NON-allowlisted sender     -> not matched
	//   302  type-3 to a DIFFERENT address from the allowed sender  -> not matched
	//   303  a non-type-3 tx to the target from the allowed sender  -> not matched
	b.addBlock(300, txEntry{tx: blobTx(t, keyA, testInbox, 0, hashes(1))})
	b.addBlock(301, txEntry{tx: blobTx(t, keyB, testInbox, 0, hashes(2))})
	b.addBlock(302, txEntry{tx: blobTx(t, keyA, otherAddr, 1, hashes(3))})
	b.addBlock(303, txEntry{tx: callTx(t, keyA, testInbox, 2)})

	ix := newTestIndexer(b.chain(), []Source{blobTxs(testInbox, 0, senderA)})
	rows, err := ix.scan(context.Background(), 0, 3)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	got := rowsBySlot(rows)
	assertSlots(t, got, 300)
	assertRow(t, got, 300, vhs(1))
}

func TestValidateSources(t *testing.T) {
	tests := []struct {
		name    string
		sources []Source
		want    string // substring of the error, or "" for accept
	}{
		{"empty schedule", nil, "at least one source"},
		{"inbox-events ok", []Source{inboxOpen(testInbox, 0)}, ""},
		{"blob-txs ok", []Source{blobTxs(testInbox, 0, senderA)}, ""},
		{"zero address", []Source{{Type: SourceInboxEvents, Topic: testTopic, OpenEnded: true}}, "zero address"},
		{"zero topic", []Source{{Type: SourceInboxEvents, Address: testInbox, OpenEnded: true}}, "zero topic"},
		{"unknown type", []Source{{Type: "nope", Address: testInbox, OpenEnded: true}}, "unknown type"},
		{
			"empty allowlist",
			[]Source{{Type: SourceBlobTxs, Address: testInbox, OpenEnded: true}},
			"empty sender allowlist",
		},
		{
			"from after until",
			[]Source{{Type: SourceInboxEvents, Address: testInbox, Topic: testTopic, FromBlock: 10, UntilBlock: 5}},
			"before from_block",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateSources(tt.sources)
			if tt.want == "" {
				if err != nil {
					t.Fatalf("ValidateSources: %v, want accept", err)
				}
				return
			}
			if err == nil {
				t.Fatal("ValidateSources accepted an invalid schedule")
			}
			if got := err.Error(); !strings.Contains(got, tt.want) {
				t.Errorf("error = %q, want it to contain %q", got, tt.want)
			}
		})
	}
}

// TestValidateSourcesContiguity is the source-contiguity hardening: a schedule must leave no uncovered
// block range between its sources. Overlap is the supported hand-off; a gap is
// refused, because the scan advances synced_to across an uncovered range while
// recording nothing and freezes the batches there as permanent, false 404s.
func TestValidateSourcesContiguity(t *testing.T) {
	tests := []struct {
		name    string
		sources []Source
		want    string // substring of the error, or "" for accept
	}{
		{
			// THE regression: an off-by-one close-and-add boundary. A until 1000,
			// B from 1002, so block 1001 is covered by no source.
			"off-by-one gap",
			[]Source{inboxRange(testInbox, 100, 1000), inboxOpen(testInbox, 1002)},
			"1001..1001",
		},
		{"contiguous adjacent", []Source{inboxRange(testInbox, 0, 1000), inboxOpen(testInbox, 1001)}, ""},
		{"overlapping ranges", []Source{inboxRange(testInbox, 0, 1000), inboxOpen(testInbox, 500)}, ""},
		{"identical from_blocks", []Source{inboxRange(testInbox, 0, 1000), inboxOpen(testInbox, 0)}, ""},
		{"single open-ended", []Source{inboxOpen(testInbox, 100)}, ""},
		{"single bounded", []Source{inboxRange(testInbox, 100, 200)}, ""},
		{
			// A gap before the earliest source is fine: the schedule starts there.
			"gap before the first source",
			[]Source{inboxRange(testInbox, 500, 600), inboxOpen(testInbox, 601)},
			"",
		},
		{
			// A bounded source wholly after an open-ended one is already covered by
			// the union; the open end reaches it, so there is no hole to refuse.
			"bounded source shadowed after an open end",
			[]Source{inboxOpen(testInbox, 100), inboxRange(testInbox, 5000, 6000)},
			"",
		},
		{
			// A bounded source overlapping under an open-ended one that starts later.
			"open-ended source starting inside a bounded one",
			[]Source{inboxRange(testInbox, 100, 1000), inboxOpen(testInbox, 500)},
			"",
		},
		{
			// Two holes; the first by block order (201..299) is the one named, not
			// the later 401..499.
			"multiple gaps name the first",
			[]Source{inboxRange(testInbox, 100, 200), inboxRange(testInbox, 300, 400), inboxRange(testInbox, 500, 600)},
			"sources 0 and 1 leave L1 blocks 201..299",
		},
		{
			// The gap is named against the source that reaches the covered max (0,
			// which reaches 1000), not the shorter source between them (1, to 500).
			"gap named against the furthest-reaching source",
			[]Source{inboxRange(testInbox, 100, 1000), inboxRange(testInbox, 200, 500), inboxOpen(testInbox, 1002)},
			"sources 0 and 2 leave L1 blocks 1001..1001",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateSources(tt.sources)
			if tt.want == "" {
				if err != nil {
					t.Fatalf("ValidateSources: %v, want accept", err)
				}
				return
			}
			if err == nil {
				t.Fatal("ValidateSources accepted a schedule with an internal gap")
			}
			if got := err.Error(); !strings.Contains(got, tt.want) {
				t.Errorf("error = %q, want it to contain %q", got, tt.want)
			}
			// The multi-gap case must name only the first hole.
			if tt.name == "multiple gaps name the first" && strings.Contains(err.Error(), "401..499") {
				t.Errorf("error names a later hole instead of the first: %v", err)
			}
		})
	}
}

// TestNewRefusesGapIntroducingUpgrade is spec 10.5's upgrade path: the proposed
// schedule an operator restarts the indexer with is validated at New (the
// indexer is where semantic validation runs; the archive server compares CIDs
// and nothing else). A close-and-add that caps the open inbox at 1000 and
// appends its replacement one block too late leaves block 1001 uncovered, and
// New's ValidateSources floor refuses it before the indexer ever runs.
func TestNewRefusesGapIntroducingUpgrade(t *testing.T) {
	arch, err := archclient.New(archclient.Config{BaseURL: "http://archive.invalid", Token: "t"})
	if err != nil {
		t.Fatalf("archclient.New: %v", err)
	}
	_, err = New(Config{
		Chain:          &fakeChain{},
		Archive:        arch,
		AllHead:        "all",
		Head:           "arbitrum-one",
		GenesisTime:    testGenesis,
		SecondsPerSlot: testSPS,
		Sources:        []Source{inboxRange(testInbox, 0, 1000), inboxOpen(testInbox, 1002)},
	})
	if err == nil {
		t.Fatal("New accepted a gap-introducing close-and-add upgrade")
	}
	if !strings.Contains(err.Error(), "1001..1001") {
		t.Errorf("error does not name the uncovered block: %v", err)
	}
}

// newTestIndexer builds an Indexer wired to fc with the given schedule, ready to
// scan. It skips New (which wants an archive and a full Config) because scan
// touches none of that; origin is left at 0, so no slot is below it.
func newTestIndexer(fc ChainClient, sources []Source) *Indexer {
	return &Indexer{
		cfg: Config{
			Chain:          fc,
			GenesisTime:    testGenesis,
			SecondsPerSlot: testSPS,
			Head:           "test",
			Sources:        sources,
		},
		log: slog.New(slog.DiscardHandler),
	}
}

func inboxOpen(addr common.Address, from uint64) Source {
	return Source{Type: SourceInboxEvents, Address: addr, Topic: testTopic, FromBlock: from, OpenEnded: true}
}

func inboxRange(addr common.Address, from, until uint64) Source {
	return Source{Type: SourceInboxEvents, Address: addr, Topic: testTopic, FromBlock: from, UntilBlock: until}
}

func blobTxs(addr common.Address, from uint64, senders ...common.Address) Source {
	return Source{Type: SourceBlobTxs, Address: addr, Senders: senders, FromBlock: from, OpenEnded: true}
}

// fakeChain is an in-memory ChainClient over a handful of built blocks.
type fakeChain struct {
	blocks []*types.Block
	byHash map[common.Hash]*types.Header
	txs    map[common.Hash]*types.Transaction
	logs   []types.Log
}

// filterLogsResultChain returns result exactly as stored. Unlike fakeChain's
// normal filtered copy, this models an RPC/cache-owned slice so tests can prove
// the scanner does not reorder its caller's memory.
type filterLogsResultChain struct {
	*fakeChain
	result []types.Log
}

func (c *filterLogsResultChain) FilterLogs(context.Context, ethereum.FilterQuery) ([]types.Log, error) {
	return c.result, nil
}

func (c *fakeChain) HeaderByNumber(_ context.Context, number *big.Int) (*types.Header, error) {
	i := number.Uint64()
	if i >= uint64(len(c.blocks)) {
		return nil, fmt.Errorf("fakeChain: no block %d", i)
	}
	return c.blocks[i].Header(), nil
}

func (c *fakeChain) HeaderByHash(_ context.Context, hash common.Hash) (*types.Header, error) {
	h, ok := c.byHash[hash]
	if !ok {
		return nil, fmt.Errorf("fakeChain: no header %s", hash)
	}
	return h, nil
}

func (c *fakeChain) BlockByNumber(_ context.Context, number *big.Int) (*types.Block, error) {
	i := number.Uint64()
	if i >= uint64(len(c.blocks)) {
		return nil, fmt.Errorf("fakeChain: no block %d", i)
	}
	return c.blocks[i], nil
}

func (c *fakeChain) TransactionByHash(_ context.Context, hash common.Hash) (*types.Transaction, bool, error) {
	tx, ok := c.txs[hash]
	if !ok {
		return nil, false, fmt.Errorf("fakeChain: no tx %s", hash)
	}
	return tx, false, nil
}

// FilterLogs returns the stored logs in [FromBlock, ToBlock] matching the query's
// address set and topic0, in ascending (block, index) order -- which is the
// order they were appended, so scan sees them the way a real node returns them.
func (c *fakeChain) FilterLogs(_ context.Context, q ethereum.FilterQuery) ([]types.Log, error) {
	from, to := q.FromBlock.Uint64(), q.ToBlock.Uint64()
	var out []types.Log
	for _, l := range c.logs {
		if l.BlockNumber < from || l.BlockNumber > to {
			continue
		}
		if !addrIn(q.Addresses, l.Address) || !topic0In(q.Topics, l.Topics) {
			continue
		}
		out = append(out, l)
	}
	return out, nil
}

func addrIn(want []common.Address, got common.Address) bool {
	if len(want) == 0 {
		return true
	}
	for _, a := range want {
		if a == got {
			return true
		}
	}
	return false
}

func topic0In(want [][]common.Hash, got []common.Hash) bool {
	if len(want) == 0 || len(want[0]) == 0 {
		return true
	}
	if len(got) == 0 {
		return false
	}
	for _, w := range want[0] {
		if w == got[0] {
			return true
		}
	}
	return false
}

// chainBuilder assembles a synthetic parent chain block by block.
type chainBuilder struct {
	t      *testing.T
	blocks []*types.Block
	byHash map[common.Hash]*types.Header
	txs    map[common.Hash]*types.Transaction
	logs   []types.Log
}

func newChainBuilder(t *testing.T) *chainBuilder {
	return &chainBuilder{
		t:      t,
		byHash: make(map[common.Hash]*types.Header),
		txs:    make(map[common.Hash]*types.Transaction),
	}
}

// txEntry is one transaction to place in a block, and -- when logAddr is set --
// the inbox-events log to emit for it.
type txEntry struct {
	tx       *types.Transaction
	logAddr  common.Address
	logTopic common.Hash
}

// addBlock appends a block in the given slot carrying entries, registering its
// header, transactions, and any logs. The timestamp is genesis + slot*sps, so
// the block's slot is exactly the number passed here.
func (b *chainBuilder) addBlock(slot uint64, entries ...txEntry) {
	number := uint64(len(b.blocks))
	var parent common.Hash
	if number > 0 {
		parent = b.blocks[number-1].Hash()
	}
	h := &types.Header{
		ParentHash:  parent,
		UncleHash:   types.EmptyUncleHash,
		Root:        types.EmptyRootHash,
		TxHash:      types.EmptyTxsHash,
		ReceiptHash: types.EmptyReceiptsHash,
		Difficulty:  big.NewInt(0),
		Number:      new(big.Int).SetUint64(number),
		GasLimit:    30_000_000,
		Time:        testGenesis + slot*testSPS,
		BaseFee:     big.NewInt(7),
	}
	txs := make([]*types.Transaction, 0, len(entries))
	for _, e := range entries {
		txs = append(txs, e.tx)
	}
	blk := types.NewBlockWithHeader(h).WithBody(types.Body{Transactions: txs})
	bh := blk.Hash()

	b.blocks = append(b.blocks, blk)
	b.byHash[bh] = blk.Header()
	for i, e := range entries {
		b.txs[e.tx.Hash()] = e.tx
		if e.logAddr != (common.Address{}) {
			b.logs = append(b.logs, types.Log{
				Address:     e.logAddr,
				Topics:      []common.Hash{e.logTopic},
				BlockNumber: number,
				TxHash:      e.tx.Hash(),
				TxIndex:     uint(i),
				BlockHash:   bh,
				Index:       uint(len(b.logs)),
			})
		}
	}
}

func (b *chainBuilder) chain() *fakeChain {
	return &fakeChain{blocks: b.blocks, byHash: b.byHash, txs: b.txs, logs: b.logs}
}

// blobTx is a signed type-3 transaction to addr carrying vhs.
func blobTx(t *testing.T, key *ecdsa.PrivateKey, addr common.Address, nonce uint64, vhs []common.Hash) *types.Transaction {
	t.Helper()
	tx := types.NewTx(&types.BlobTx{
		ChainID:    uint256.MustFromBig(testChainID),
		Nonce:      nonce,
		GasTipCap:  uint256.NewInt(1),
		GasFeeCap:  uint256.NewInt(100),
		Gas:        1_000_000,
		To:         addr,
		BlobFeeCap: uint256.NewInt(100),
		BlobHashes: vhs,
	})
	return sign(t, key, tx)
}

// callTx is a signed type-2 (non-blob) transaction to addr.
func callTx(t *testing.T, key *ecdsa.PrivateKey, addr common.Address, nonce uint64) *types.Transaction {
	t.Helper()
	to := addr
	tx := types.NewTx(&types.DynamicFeeTx{
		ChainID:   testChainID,
		Nonce:     nonce,
		GasTipCap: big.NewInt(1),
		GasFeeCap: big.NewInt(100),
		Gas:       21_000,
		To:        &to,
		Data:      []byte("not a blob batch"),
	})
	return sign(t, key, tx)
}

func sign(t *testing.T, key *ecdsa.PrivateKey, tx *types.Transaction) *types.Transaction {
	t.Helper()
	signed, err := types.SignTx(tx, types.NewCancunSigner(testChainID), key)
	if err != nil {
		t.Fatalf("signing: %v", err)
	}
	return signed
}

// hashes renders blob hash byte tags as the common.Hash a transaction carries.
func hashes(ns ...byte) []common.Hash {
	out := make([]common.Hash, len(ns))
	for i, n := range ns {
		out[i] = vh(n)
	}
	return out
}

// vh is a distinct blob versioned hash for tag n.
func vh(n byte) common.Hash {
	var h common.Hash
	h[0] = 0x01
	h[31] = n
	return h
}

func vhs(ns ...byte) []schema.VersionedHash {
	out := make([]schema.VersionedHash, len(ns))
	for i, n := range ns {
		out[i] = schema.VersionedHash(vh(n))
	}
	return out
}

func rowsBySlot(rows []archclient.Row) map[uint64][]schema.VersionedHash {
	m := make(map[uint64][]schema.VersionedHash, len(rows))
	for _, r := range rows {
		m[r.Slot] = r.VHs
	}
	return m
}

func assertRow(t *testing.T, got map[uint64][]schema.VersionedHash, slot uint64, want []schema.VersionedHash) {
	t.Helper()
	g, ok := got[slot]
	if !ok {
		t.Fatalf("no row for slot %d", slot)
	}
	if len(g) != len(want) {
		t.Fatalf("slot %d: %d vhs %v, want %d %v", slot, len(g), g, len(want), want)
	}
	for i := range want {
		if g[i] != want[i] {
			t.Errorf("slot %d, vh %d = %x, want %x", slot, i, g[i], want[i])
		}
	}
}

func assertSlots(t *testing.T, got map[uint64][]schema.VersionedHash, want ...uint64) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("rows cover %d slots %v, want %d %v", len(got), keysOf(got), len(want), want)
	}
	for _, s := range want {
		if _, ok := got[s]; !ok {
			t.Errorf("no row for slot %d; have %v", s, keysOf(got))
		}
	}
}

func keysOf(m map[uint64][]schema.VersionedHash) []uint64 {
	out := make([]uint64, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
