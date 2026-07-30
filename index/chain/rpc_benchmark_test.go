package chain

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/rpc"
)

// TestBlobTxsRPCBenchmark is an opt-in full scanner benchmark against a local
// trusted execution RPC. Normal suites skip it. It exercises the production
// JSON-RPC batch adapter, transaction decoding, signature recovery, filtering,
// bounded reorder pipeline, and row rendering without mutating an archive.
//
// Required environment:
//
//	BLOAR_BENCH_RPC, BLOAR_BENCH_FROM, BLOAR_BENCH_COUNT,
//	BLOAR_BENCH_ADDRESS, BLOAR_BENCH_SENDER
//
// Optional:
//
//	BLOAR_BENCH_WORKERS (default 4), BLOAR_BENCH_BATCH_SIZE (default 16)
//
// The one-worker 100k control exceeds go test's default ten-minute process
// timeout on the reference host. Invoke this opt-in test with -timeout 4h; the
// benchmark also keeps its own four-hour context as the in-process bound.
func TestBlobTxsRPCBenchmark(t *testing.T) {
	rpcURL := os.Getenv("BLOAR_BENCH_RPC")
	if rpcURL == "" {
		t.Skip("BLOAR_BENCH_RPC is unset")
	}
	from := benchUint64(t, "BLOAR_BENCH_FROM", 0)
	count := benchUint64(t, "BLOAR_BENCH_COUNT", 0)
	if count == 0 {
		t.Fatal("BLOAR_BENCH_COUNT must be positive")
	}
	if count-1 > ^uint64(0)-from {
		t.Fatal("benchmark block range overflows uint64")
	}
	workers := int(benchUint64(t, "BLOAR_BENCH_WORKERS", defaultBlockFetchConcurrency))
	batchSize := int(benchUint64(t, "BLOAR_BENCH_BATCH_SIZE", defaultRPCBatchSize))
	if workers < 1 || workers > maxBlockFetchConcurrency {
		t.Fatalf("BLOAR_BENCH_WORKERS=%d, want [1,%d]", workers, maxBlockFetchConcurrency)
	}
	if batchSize < 1 || batchSize > maxRPCBatchSize {
		t.Fatalf("BLOAR_BENCH_BATCH_SIZE=%d, want [1,%d]", batchSize, maxRPCBatchSize)
	}
	address := benchAddress(t, "BLOAR_BENCH_ADDRESS")
	sender := benchAddress(t, "BLOAR_BENCH_SENDER")

	// One worker over a transaction-dense 100k-block interval is deliberately
	// part of the matrix and can take more than two hours on the local host.
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Hour)
	defer cancel()
	rawRPC, err := rpc.DialContext(ctx, rpcURL)
	if err != nil {
		t.Fatalf("dial benchmark RPC: %v", err)
	}
	defer rawRPC.Close()

	ix := &Indexer{
		cfg: Config{
			Chain:                 NewRPCBatchChainClient(rawRPC),
			Head:                  "benchmark",
			Sources:               []Source{blobTxsSource(address, from, sender)},
			GenesisTime:           1_606_824_023,
			SecondsPerSlot:        12,
			BlockFetchConcurrency: workers,
			RPCBatchSize:          batchSize,
		},
		log: slog.New(slog.DiscardHandler),
	}
	started := time.Now()
	rows, err := ix.scan(ctx, from, from+count-1)
	if err != nil {
		t.Fatalf("scan benchmark interval: %v", err)
	}
	elapsed := time.Since(started)
	rendered, err := json.Marshal(rows)
	if err != nil {
		t.Fatalf("marshal benchmark rows: %v", err)
	}
	digest := sha256.Sum256(rendered)
	t.Logf("BLOAR_BENCH_RESULT from=%d count=%d workers=%d batch_size=%d rows=%d row_bytes=%d row_sha256=%s elapsed=%s blocks_per_second=%.3f",
		from, count, workers, batchSize, len(rows), len(rendered), hex.EncodeToString(digest[:]), elapsed,
		float64(count)/elapsed.Seconds())
}

func benchUint64(t *testing.T, key string, defaultValue uint64) uint64 {
	t.Helper()
	raw := os.Getenv(key)
	if raw == "" {
		return defaultValue
	}
	value, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		t.Fatalf("%s=%q is not uint64: %v", key, raw, err)
	}
	return value
}

func benchAddress(t *testing.T, key string) common.Address {
	t.Helper()
	raw := os.Getenv(key)
	if !common.IsHexAddress(raw) {
		t.Fatalf("%s=%q is not a 20-byte hex address", key, raw)
	}
	return common.HexToAddress(raw)
}

func blobTxsSource(address common.Address, from uint64, sender common.Address) Source {
	return Source{
		Type:      SourceBlobTxs,
		Address:   address,
		Senders:   []common.Address{sender},
		FromBlock: from,
		OpenEnded: true,
	}
}
