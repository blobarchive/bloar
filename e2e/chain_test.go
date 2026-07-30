package e2e

import (
	"crypto/ecdsa"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/holiman/uint256"

	chainidx "github.com/blobarchive/bloar/index/chain"
)

// fakeChain is a parent chain's JSON-RPC, over a synthetic chain of blocks.
//
// It serves the methods index/chain's ChainClient names, and it serves them
// through go-ethereum's own types: every header and every transaction here
// is a real types.Header and a real signed types.Transaction, marshalled with
// the marshallers ethclient unmarshals with. That is deliberate. A hand-rolled
// JSON block would be a test of this file's idea of the wire format, and the
// one thing this test must not do is agree with itself about a format nitro and
// geth define.
type fakeChain struct {
	http *httptest.Server
	url  string

	blocks    []*fakeBlock
	byHash    map[common.Hash]*fakeBlock
	txByHash  map[common.Hash]*fakeTx
	finalized uint64
}

// fakeBlock is one L1 block: enough of a header to hash, plus what it carried.
type fakeBlock struct {
	header *types.Header
	txs    []*fakeTx
	logs   []*types.Log
}

// fakeTx is one transaction and the block it landed in.
type fakeTx struct {
	tx     *types.Transaction
	from   common.Address
	block  *fakeBlock
	index  uint
	sender common.Address
}

// chainID is the synthetic parent chain's id.
var chainID = big.NewInt(1337)

// chainKey signs every transaction on it. A fixed key, because ethclient
// refuses a transaction with no signature (it checks R) and the sender is never
// asserted on.
var chainKey, _ = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")

// newFakeChain starts a parent chain whose finalized head is the last block.
func newFakeChain(t *testing.T, blocks []*fakeBlock) *fakeChain {
	t.Helper()
	c := &fakeChain{
		blocks:   blocks,
		byHash:   make(map[common.Hash]*fakeBlock, len(blocks)),
		txByHash: make(map[common.Hash]*fakeTx),
	}
	if len(blocks) == 0 {
		t.Fatal("newFakeChain: a chain needs at least one block")
	}
	c.finalized = uint64(len(blocks) - 1)

	for _, b := range blocks {
		c.byHash[b.header.Hash()] = b
		for _, tx := range b.txs {
			c.txByHash[tx.tx.Hash()] = tx
		}
	}

	c.http = httptest.NewServer(http.HandlerFunc(c.serve))
	t.Cleanup(c.http.Close)
	c.url = c.http.URL
	return c
}

// rpcRequest is a JSON-RPC 2.0 call.
type rpcRequest struct {
	Version string            `json:"jsonrpc"`
	ID      json.RawMessage   `json:"id"`
	Method  string            `json:"method"`
	Params  []json.RawMessage `json:"params"`
}

// rpcResponse is its answer.
type rpcResponse struct {
	Version string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// serve dispatches one call.
func (c *fakeChain) serve(w http.ResponseWriter, r *http.Request) {
	var req rpcRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, rpcResponse{Version: "2.0", Error: &rpcError{Code: -32700, Message: err.Error()}})
		return
	}

	result, err := c.dispatch(req)
	resp := rpcResponse{Version: "2.0", ID: req.ID}
	if err != nil {
		resp.Error = &rpcError{Code: -32000, Message: err.Error()}
		writeJSON(w, http.StatusOK, resp)
		return
	}
	raw, err := json.Marshal(result)
	if err != nil {
		resp.Error = &rpcError{Code: -32603, Message: err.Error()}
		writeJSON(w, http.StatusOK, resp)
		return
	}
	resp.Result = raw
	writeJSON(w, http.StatusOK, resp)
}

// dispatch runs one method.
func (c *fakeChain) dispatch(req rpcRequest) (any, error) {
	switch req.Method {
	case "eth_getBlockByNumber":
		return c.getBlockByNumber(req.Params)
	case "eth_getBlockByHash":
		return c.getBlockByHash(req.Params)
	case "eth_getTransactionByHash":
		return c.getTransactionByHash(req.Params)
	case "eth_getLogs":
		return c.getLogs(req.Params)
	case "eth_chainId":
		return hexutil.Big(*chainID), nil
	default:
		return nil, fmt.Errorf("the method %s is not served by this chain", req.Method)
	}
}

// getBlockByNumber serves eth_getBlockByNumber. HeaderByNumber asks with
// fullTx=false and decodes a types.Header, so that case renders the header
// alone; BlockByNumber asks with fullTx=true and decodes a whole block, which a
// blob-txs source (spec 10.4) needs because it reads block bodies rather than
// keying on a log. fullBlock renders that.
func (c *fakeChain) getBlockByNumber(params []json.RawMessage) (any, error) {
	if len(params) < 1 {
		return nil, fmt.Errorf("eth_getBlockByNumber wants a block number")
	}
	var tag string
	if err := json.Unmarshal(params[0], &tag); err != nil {
		return nil, err
	}

	var number uint64
	switch tag {
	case "finalized":
		// The tag spec 10.3 requires the indexer to use, and the whole reason
		// this fake serves tags at all: an indexer that asked for "latest"
		// would sync unfinalized blocks and this would be the test that
		// noticed.
		number = c.finalized
	case "latest":
		number = uint64(len(c.blocks) - 1)
	case "earliest":
		number = 0
	default:
		n, err := hexutil.DecodeUint64(tag)
		if err != nil {
			return nil, fmt.Errorf("bad block number %q: %w", tag, err)
		}
		number = n
	}
	if number >= uint64(len(c.blocks)) {
		// A number past the chain: null, which ethclient reads as NotFound.
		return nil, nil
	}

	full := false
	if len(params) >= 2 {
		_ = json.Unmarshal(params[1], &full)
	}
	if !full {
		return c.blocks[number].header, nil
	}
	return c.fullBlock(c.blocks[number])
}

// fullBlock renders a block the way ethclient's BlockByNumber decodes it: the
// header's own fields, plus the full transactions and an empty uncle list. Only
// a blob-txs source's scan reaches this path (scanBlobTxs reads whole blocks),
// and it never hashes the block -- it reads the header's timestamp for the slot
// and the transactions for their senders and recipients -- so a served header
// whose transactionsRoot no longer matches the empty one the block was built
// with is invisible to it. What is not invisible is ethclient's own consistency
// check: a block with transactions must not carry the empty transactions root,
// so txsRoot fills a non-empty one in.
func (c *fakeChain) fullBlock(blk *fakeBlock) (any, error) {
	raw, err := json.Marshal(blk.header)
	if err != nil {
		return nil, err
	}
	var obj map[string]any
	if err := json.Unmarshal(raw, &obj); err != nil {
		return nil, err
	}
	obj["transactionsRoot"] = txsRoot(blk.txs)

	txs := make([]any, 0, len(blk.txs))
	for _, ft := range blk.txs {
		txRaw, err := ft.tx.MarshalJSON()
		if err != nil {
			return nil, err
		}
		var tx map[string]any
		if err := json.Unmarshal(txRaw, &tx); err != nil {
			return nil, err
		}
		// The rendered block-context fields a node merges over a transaction, the
		// same ones getTransactionByHash sets. rpcTransaction tolerates them
		// missing, but a real node always states them.
		tx["blockHash"] = blk.header.Hash()
		tx["blockNumber"] = hexutil.EncodeBig(blk.header.Number)
		tx["transactionIndex"] = hexutil.Uint64(ft.index)
		tx["from"] = ft.from
		txs = append(txs, tx)
	}
	obj["transactions"] = txs
	obj["uncles"] = []any{}
	return obj, nil
}

// txsRoot is a non-empty transactions root for a block that carries any, and the
// empty one otherwise. ethclient rejects a block whose transactionsRoot is the
// empty hash while its transaction list is not (and the reverse); the exact
// value is never checked against the transactions, so a hash of their hashes is
// enough to keep the two consistent.
func txsRoot(txs []*fakeTx) common.Hash {
	if len(txs) == 0 {
		return types.EmptyTxsHash
	}
	var buf []byte
	for _, ft := range txs {
		h := ft.tx.Hash()
		buf = append(buf, h[:]...)
	}
	return crypto.Keccak256Hash(buf)
}

// getBlockByHash serves eth_getBlockByHash.
func (c *fakeChain) getBlockByHash(params []json.RawMessage) (any, error) {
	if len(params) < 1 {
		return nil, fmt.Errorf("eth_getBlockByHash wants a hash")
	}
	var hash common.Hash
	if err := json.Unmarshal(params[0], &hash); err != nil {
		return nil, err
	}
	b, ok := c.byHash[hash]
	if !ok {
		return nil, nil
	}
	return b.header, nil
}

// getTransactionByHash serves eth_getTransactionByHash, in the shape
// ethclient's rpcTransaction decodes: the transaction's own fields, plus the
// block it landed in and its sender.
func (c *fakeChain) getTransactionByHash(params []json.RawMessage) (any, error) {
	if len(params) < 1 {
		return nil, fmt.Errorf("eth_getTransactionByHash wants a hash")
	}
	var hash common.Hash
	if err := json.Unmarshal(params[0], &hash); err != nil {
		return nil, err
	}
	tx, ok := c.txByHash[hash]
	if !ok {
		return nil, nil
	}

	// The transaction marshals itself; the extra fields are merged in over the
	// top, which is how a real node renders them.
	raw, err := tx.tx.MarshalJSON()
	if err != nil {
		return nil, err
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	out["blockHash"] = tx.block.header.Hash()
	out["blockNumber"] = hexutil.EncodeBig(tx.block.header.Number)
	out["transactionIndex"] = hexutil.Uint64(tx.index)
	out["from"] = tx.from
	return out, nil
}

// logFilter is eth_getLogs' argument, as ethclient's toFilterArg renders it.
type logFilter struct {
	FromBlock string           `json:"fromBlock"`
	ToBlock   string           `json:"toBlock"`
	Address   []common.Address `json:"address"`
	Topics    [][]common.Hash  `json:"topics"`
	BlockHash *common.Hash     `json:"blockHash"`
}

// getLogs serves eth_getLogs over the block range, filtering by address and
// topic0 -- which is all index/chain asks for, and all this needs to be
// honest about.
func (c *fakeChain) getLogs(params []json.RawMessage) (any, error) {
	if len(params) < 1 {
		return nil, fmt.Errorf("eth_getLogs wants a filter")
	}
	var f logFilter
	if err := json.Unmarshal(params[0], &f); err != nil {
		return nil, err
	}

	from, err := c.blockTag(f.FromBlock, 0)
	if err != nil {
		return nil, err
	}
	to, err := c.blockTag(f.ToBlock, uint64(len(c.blocks)-1))
	if err != nil {
		return nil, err
	}
	// A real node refuses a range past its head rather than quietly clipping
	// it. The indexer bounds every scan by the finalized block, so asking past
	// it is a bug worth failing on.
	if to > c.finalized {
		return nil, fmt.Errorf("toBlock %d is past the finalized block %d", to, c.finalized)
	}

	out := []*types.Log{}
	for n := from; n <= to && n < uint64(len(c.blocks)); n++ {
		for _, l := range c.blocks[n].logs {
			if !matchAddress(f.Address, l.Address) || !matchTopics(f.Topics, l.Topics) {
				continue
			}
			out = append(out, l)
		}
	}
	return out, nil
}

// blockTag resolves one end of a log filter's range.
func (c *fakeChain) blockTag(tag string, def uint64) (uint64, error) {
	switch tag {
	case "":
		return def, nil
	case "finalized":
		return c.finalized, nil
	case "latest", "pending":
		return uint64(len(c.blocks) - 1), nil
	case "earliest":
		return 0, nil
	default:
		return hexutil.DecodeUint64(tag)
	}
}

// matchAddress reports whether a log's address passes the filter.
func matchAddress(want []common.Address, got common.Address) bool {
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

// matchTopics reports whether a log's topics pass the filter, positionally: an
// empty slot in the filter matches anything.
func matchTopics(want [][]common.Hash, got []common.Hash) bool {
	if len(want) > len(got) {
		return false
	}
	for i, alts := range want {
		if len(alts) == 0 {
			continue
		}
		ok := false
		for _, a := range alts {
			if a == got[i] {
				ok = true
				break
			}
		}
		if !ok {
			return false
		}
	}
	return true
}

// chainBuilder assembles a synthetic parent chain block by block.
type chainBuilder struct {
	t      *testing.T
	inbox  common.Address
	blocks []*fakeBlock
	nonce  uint64
}

func newChainBuilder(t *testing.T, inbox common.Address) *chainBuilder {
	return &chainBuilder{t: t, inbox: inbox}
}

// addBlock appends a block with the given timestamp, and returns it so
// transactions can be added to it.
func (b *chainBuilder) addBlock(timestamp uint64) *fakeBlock {
	number := uint64(len(b.blocks))
	var parent common.Hash
	if number > 0 {
		parent = b.blocks[number-1].header.Hash()
	}
	// Real header fields, so that Hash() is a real hash and the JSON carries
	// everything types.Header's unmarshaller demands.
	blk := &fakeBlock{header: &types.Header{
		ParentHash:  parent,
		UncleHash:   types.EmptyUncleHash,
		Root:        types.EmptyRootHash,
		TxHash:      types.EmptyTxsHash,
		ReceiptHash: types.EmptyReceiptsHash,
		Difficulty:  big.NewInt(0),
		Number:      new(big.Int).SetUint64(number),
		GasLimit:    30_000_000,
		Time:        timestamp,
		BaseFee:     big.NewInt(7),
	}}
	b.blocks = append(b.blocks, blk)
	return blk
}

// addBlobBatch adds a type-3 SequencerInbox transaction carrying vhs, and the
// SequencerBatchDelivered log that goes with it.
func (b *chainBuilder) addBlobBatch(blk *fakeBlock, vhs []common.Hash) *fakeTx {
	b.t.Helper()
	tx := types.NewTx(&types.BlobTx{
		ChainID:    uint256.MustFromBig(chainID),
		Nonce:      b.nextNonce(),
		GasTipCap:  uint256.NewInt(1),
		GasFeeCap:  uint256.NewInt(100),
		Gas:        1_000_000,
		To:         b.inbox,
		Value:      uint256.NewInt(0),
		Data:       []byte{0x8f, 0x11, 0x1f, 0x3c},
		BlobFeeCap: uint256.NewInt(100),
		BlobHashes: vhs,
	})
	return b.addTx(blk, tx)
}

// addBlobTxFrom adds a type-3 blob transaction from key to recipient, and emits
// NO log. It is a blob-txs source's posting mechanism (spec 10.4, the Base-style
// plain-EOA arrangement): the source selects on the transaction's recipient and
// its recovered sender, both read from the block body, so there is no
// SequencerBatchDelivered event to key on and none is written. The block it lands
// in therefore has to be served with its full body, which is what fullBlock is
// for.
func (b *chainBuilder) addBlobTxFrom(blk *fakeBlock, key *ecdsa.PrivateKey, recipient common.Address, vhs []common.Hash) *fakeTx {
	b.t.Helper()
	unsigned := types.NewTx(&types.BlobTx{
		ChainID:    uint256.MustFromBig(chainID),
		Nonce:      b.nextNonce(),
		GasTipCap:  uint256.NewInt(1),
		GasFeeCap:  uint256.NewInt(100),
		Gas:        1_000_000,
		To:         recipient,
		Value:      uint256.NewInt(0),
		BlobFeeCap: uint256.NewInt(100),
		BlobHashes: vhs,
	})
	signer := types.NewCancunSigner(chainID)
	tx, err := types.SignTx(unsigned, signer, key)
	if err != nil {
		b.t.Fatalf("signing an EOA blob transaction: %v", err)
	}
	// The sender the scan recovers from the signature (scanBlobTxs does the same),
	// never the node's rendered "from": a blob-txs source is a claim a specific
	// sequencer posted, and only the ECDSA recovery is unforgeable (spec 10.4).
	from, err := types.Sender(signer, tx)
	if err != nil {
		b.t.Fatalf("recovering the EOA sender: %v", err)
	}
	ft := &fakeTx{tx: tx, from: from, block: blk, index: uint(len(blk.txs)), sender: from}
	blk.txs = append(blk.txs, ft)
	return ft
}

// addCalldataBatch adds a type-2 SequencerInbox transaction -- a calldata batch
// -- and its log. Spec 10.2: a non-blob batch produces no rows, but coverage
// still advances over it.
func (b *chainBuilder) addCalldataBatch(blk *fakeBlock, data []byte) *fakeTx {
	b.t.Helper()
	to := b.inbox
	tx := types.NewTx(&types.DynamicFeeTx{
		ChainID:   chainID,
		Nonce:     b.nextNonce(),
		GasTipCap: big.NewInt(1),
		GasFeeCap: big.NewInt(100),
		Gas:       1_000_000,
		To:        &to,
		Value:     big.NewInt(0),
		Data:      data,
	})
	return b.addTx(blk, tx)
}

// addTx signs a transaction into a block and emits its inbox log.
func (b *chainBuilder) addTx(blk *fakeBlock, unsigned *types.Transaction) *fakeTx {
	b.t.Helper()
	signer := types.NewCancunSigner(chainID)
	tx, err := types.SignTx(unsigned, signer, chainKey)
	if err != nil {
		b.t.Fatalf("signing a batch transaction: %v", err)
	}
	from, err := types.Sender(signer, tx)
	if err != nil {
		b.t.Fatalf("recovering the sender: %v", err)
	}

	ft := &fakeTx{tx: tx, from: from, block: blk, index: uint(len(blk.txs)), sender: from}
	blk.txs = append(blk.txs, ft)

	seq := big.NewInt(int64(len(blk.logs)))
	blk.logs = append(blk.logs, &types.Log{
		Address: b.inbox,
		// The three indexed arguments: batchSequenceNumber, beforeAcc,
		// afterAcc. Only topic0 is read, but a log with the wrong number of
		// topics is not the log the contract emits.
		Topics: []common.Hash{
			chainidx.SequencerBatchDeliveredTopic,
			common.BigToHash(seq),
			{},
			{},
		},
		// The non-indexed body. Not decoded by the indexer -- it reads the
		// transaction instead -- but it is the right length: seven 32-byte
		// words for delayedAcc, afterDelayedMessagesRead, the four TimeBounds
		// fields, and dataLocation.
		Data:        make([]byte, 7*32),
		BlockNumber: blk.header.Number.Uint64(),
		TxHash:      tx.Hash(),
		TxIndex:     ft.index,
		BlockHash:   blk.header.Hash(),
		Index:       uint(len(blk.logs)),
	})
	return ft
}

// nextNonce hands out sequential nonces.
func (b *chainBuilder) nextNonce() uint64 {
	n := b.nonce
	b.nonce++
	return n
}
