package chain

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"
)

// RPCBatchChainClient is an ethclient.Client with the optional BlockBatchClient
// acceleration used by blob-txs scans. All ordinary ChainClient calls retain
// ethclient's behaviour through the embedded client; only consecutive full
// eth_getBlockByNumber reads take the batch path.
type RPCBatchChainClient struct {
	*ethclient.Client
	rpc *rpc.Client
}

// NewRPCBatchChainClient wraps client without taking separate ownership of it.
// Closing the embedded ethclient or the original rpc client closes the one
// underlying connection.
func NewRPCBatchChainClient(client *rpc.Client) *RPCBatchChainClient {
	return &RPCBatchChainClient{
		Client: ethclient.NewClient(client),
		rpc:    client,
	}
}

type batchBlockBody struct {
	Hash         *common.Hash    `json:"hash"`
	Transactions json.RawMessage `json:"transactions"`
}

// BlocksByNumber reads full consecutive blocks in one JSON-RPC batch and
// returns them in request order. BatchCallContext keeps request/result
// association by element even if the server answers out of order.
func (c *RPCBatchChainClient) BlocksByNumber(ctx context.Context, numbers []uint64) ([]*types.Block, error) {
	raw := make([]json.RawMessage, len(numbers))
	elems := make([]rpc.BatchElem, len(numbers))
	for i, number := range numbers {
		elems[i] = rpc.BatchElem{
			Method: "eth_getBlockByNumber",
			Args:   []any{hexutil.EncodeUint64(number), true},
			Result: &raw[i],
		}
	}
	if err := c.rpc.BatchCallContext(ctx, elems); err != nil {
		return nil, fmt.Errorf("chain: batch eth_getBlockByNumber: %w", err)
	}

	blocks := make([]*types.Block, len(numbers))
	for i, number := range numbers {
		if elems[i].Error != nil {
			return nil, fmt.Errorf("chain: eth_getBlockByNumber %d in batch: %w", number, elems[i].Error)
		}
		if len(raw[i]) == 0 || bytes.Equal(bytes.TrimSpace(raw[i]), []byte("null")) {
			return nil, fmt.Errorf("chain: eth_getBlockByNumber %d in batch: %w", number, ethereum.NotFound)
		}

		var header *types.Header
		if err := json.Unmarshal(raw[i], &header); err != nil {
			return nil, fmt.Errorf("chain: decoding L1 block %d header: %w", number, err)
		}
		if header == nil {
			return nil, fmt.Errorf("chain: decoding L1 block %d header: %w", number, ethereum.NotFound)
		}
		if header.Number == nil || !header.Number.IsUint64() || header.Number.Uint64() != number {
			return nil, fmt.Errorf("chain: requested L1 block %d but batch response header names %v", number, header.Number)
		}

		var body batchBlockBody
		if err := json.Unmarshal(raw[i], &body); err != nil {
			return nil, fmt.Errorf("chain: decoding L1 block %d body: %w", number, err)
		}
		if body.Hash == nil {
			return nil, fmt.Errorf("chain: L1 block %d batch response has no block hash", number)
		}
		if want := header.Hash(); *body.Hash != want {
			return nil, fmt.Errorf("chain: L1 block %d response hash %s does not match header hash %s",
				number, *body.Hash, want)
		}
		if len(body.Transactions) == 0 || bytes.Equal(bytes.TrimSpace(body.Transactions), []byte("null")) {
			return nil, fmt.Errorf("chain: L1 block %d batch response has no transactions field", number)
		}
		var txs []*types.Transaction
		if err := json.Unmarshal(body.Transactions, &txs); err != nil {
			return nil, fmt.Errorf("chain: decoding L1 block %d transactions: %w", number, err)
		}
		for j, tx := range txs {
			if tx == nil {
				return nil, fmt.Errorf("chain: L1 block %d transaction %d is null", number, j)
			}
		}
		if header.TxHash == types.EmptyTxsHash && len(txs) > 0 {
			return nil, fmt.Errorf("chain: L1 block %d header says empty transactions but response contains %d",
				number, len(txs))
		}
		if header.TxHash != types.EmptyTxsHash && len(txs) == 0 {
			return nil, fmt.Errorf("chain: L1 block %d header says transactions exist but response is empty", number)
		}
		blocks[i] = types.NewBlockWithHeader(header).WithBody(types.Body{Transactions: txs})
	}
	return blocks, nil
}
