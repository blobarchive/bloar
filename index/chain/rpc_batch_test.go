package chain

import (
	"context"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rpc"
)

type batchRPCRequest struct {
	JSONRPC string            `json:"jsonrpc"`
	ID      json.RawMessage   `json:"id"`
	Method  string            `json:"method"`
	Params  []json.RawMessage `json:"params"`
}

func rpcBlockPayload(t *testing.T, number uint64, tx *types.Transaction) json.RawMessage {
	t.Helper()
	header := &types.Header{
		ParentHash:  common.HexToHash("0x1234"),
		UncleHash:   types.EmptyUncleHash,
		Root:        types.EmptyRootHash,
		TxHash:      common.HexToHash("0x1"),
		ReceiptHash: types.EmptyReceiptsHash,
		Difficulty:  big.NewInt(0),
		Number:      new(big.Int).SetUint64(number),
		GasLimit:    30_000_000,
		Time:        testGenesis + number*testSPS,
		BaseFee:     big.NewInt(7),
	}
	headerJSON, err := json.Marshal(header)
	if err != nil {
		t.Fatalf("marshal header: %v", err)
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(headerJSON, &object); err != nil {
		t.Fatalf("decode rendered header: %v", err)
	}
	object["hash"], _ = json.Marshal(header.Hash())
	object["transactions"], err = json.Marshal([]*types.Transaction{tx})
	if err != nil {
		t.Fatalf("marshal transactions: %v", err)
	}
	raw, err := json.Marshal(object)
	if err != nil {
		t.Fatalf("marshal block response: %v", err)
	}
	return raw
}

func TestRPCBatchChainClientUsesOneBatchAndPreservesRequestOrder(t *testing.T) {
	tx20 := blobTx(t, keyA, testInbox, 20, hashes(20))
	tx21 := blobTx(t, keyA, testInbox, 21, hashes(21))
	payloads := map[string]json.RawMessage{
		"0x14": rpcBlockPayload(t, 20, tx20),
		"0x15": rpcBlockPayload(t, 21, tx21),
	}
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		var requests []batchRPCRequest
		if err := json.NewDecoder(r.Body).Decode(&requests); err != nil {
			t.Errorf("decode batch: %v", err)
			http.Error(w, "bad batch", http.StatusBadRequest)
			return
		}
		if len(requests) != 2 {
			t.Errorf("request elements = %d, want 2", len(requests))
		}
		responses := make([]map[string]any, 0, len(requests))
		// Deliberately reverse the wire response. JSON-RPC ids, not response
		// arrival order, must associate each result with its request element.
		for i := len(requests) - 1; i >= 0; i-- {
			req := requests[i]
			if req.Method != "eth_getBlockByNumber" {
				t.Errorf("method = %q", req.Method)
			}
			var number string
			var full bool
			if len(req.Params) != 2 {
				t.Errorf("params = %d, want 2", len(req.Params))
			} else {
				_ = json.Unmarshal(req.Params[0], &number)
				_ = json.Unmarshal(req.Params[1], &full)
			}
			if !full {
				t.Errorf("full transaction flag = false for %s", number)
			}
			responses = append(responses, map[string]any{
				"jsonrpc": "2.0",
				"id":      req.ID,
				"result":  payloads[number],
			})
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(responses); err != nil {
			t.Errorf("encode responses: %v", err)
		}
	}))
	defer server.Close()

	rawClient, err := rpc.DialContext(context.Background(), server.URL)
	if err != nil {
		t.Fatalf("rpc.DialContext: %v", err)
	}
	defer rawClient.Close()
	client := NewRPCBatchChainClient(rawClient)
	blocks, err := client.BlocksByNumber(context.Background(), []uint64{20, 21})
	if err != nil {
		t.Fatalf("BlocksByNumber: %v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("HTTP calls = %d, want one JSON-RPC batch", calls.Load())
	}
	if len(blocks) != 2 || blocks[0].NumberU64() != 20 || blocks[1].NumberU64() != 21 {
		t.Fatalf("block numbers = %v, want [20 21]", []uint64{blocks[0].NumberU64(), blocks[1].NumberU64()})
	}
	if got := blocks[0].Transactions()[0].Hash(); got != tx20.Hash() {
		t.Errorf("block 20 tx = %s, want %s", got, tx20.Hash())
	}
	if got := blocks[1].Transactions()[0].Hash(); got != tx21.Hash() {
		t.Errorf("block 21 tx = %s, want %s", got, tx21.Hash())
	}
}

func TestRPCBatchChainClientSurfacesPerElementError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var requests []batchRPCRequest
		if err := json.NewDecoder(r.Body).Decode(&requests); err != nil {
			t.Errorf("decode batch: %v", err)
			http.Error(w, "bad batch", http.StatusBadRequest)
			return
		}
		responses := make([]map[string]any, len(requests))
		for i, req := range requests {
			responses[i] = map[string]any{
				"jsonrpc": "2.0",
				"id":      req.ID,
				"error": map[string]any{
					"code":    -32000,
					"message": "synthetic provider refusal",
				},
			}
		}
		_ = json.NewEncoder(w).Encode(responses)
	}))
	defer server.Close()
	rawClient, err := rpc.DialContext(context.Background(), server.URL)
	if err != nil {
		t.Fatalf("rpc.DialContext: %v", err)
	}
	defer rawClient.Close()

	_, err = NewRPCBatchChainClient(rawClient).BlocksByNumber(context.Background(), []uint64{20})
	if err == nil || !strings.Contains(err.Error(), "synthetic provider refusal") {
		t.Fatalf("BlocksByNumber error = %v, want provider refusal", err)
	}
}

func TestRPCBatchChainClientRejectsMalformedSuccessfulBlock(t *testing.T) {
	tx := blobTx(t, keyA, testInbox, 20, hashes(20))
	payload := rpcBlockPayload(t, 21, tx) // wrong block number for request 20.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var requests []batchRPCRequest
		if err := json.NewDecoder(r.Body).Decode(&requests); err != nil {
			t.Errorf("decode batch: %v", err)
			http.Error(w, "bad batch", http.StatusBadRequest)
			return
		}
		if len(requests) != 1 {
			t.Errorf("request elements = %d, want 1", len(requests))
			http.Error(w, "bad batch size", http.StatusBadRequest)
			return
		}
		_ = json.NewEncoder(w).Encode([]map[string]any{{
			"jsonrpc": "2.0",
			"id":      requests[0].ID,
			"result":  payload,
		}})
	}))
	defer server.Close()
	rawClient, err := rpc.DialContext(context.Background(), server.URL)
	if err != nil {
		t.Fatalf("rpc.DialContext: %v", err)
	}
	defer rawClient.Close()

	_, err = NewRPCBatchChainClient(rawClient).BlocksByNumber(context.Background(), []uint64{20})
	if err == nil || !strings.Contains(err.Error(), "requested L1 block 20") {
		t.Fatalf("BlocksByNumber error = %v, want wrong-number rejection", err)
	}
}
