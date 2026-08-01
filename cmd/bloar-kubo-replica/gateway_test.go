package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"

	"github.com/blobarchive/bloar/archive"
	"github.com/blobarchive/bloar/ingest"
	"github.com/blobarchive/bloar/kubo"
	"github.com/blobarchive/bloar/schema"
	"github.com/blobarchive/bloar/server"
)

type memoryGatewayRoots struct {
	mu    sync.RWMutex
	roots map[string]cid.Cid
}

func newMemoryGatewayRoots() *memoryGatewayRoots {
	return &memoryGatewayRoots{roots: make(map[string]cid.Cid)}
}

func (r *memoryGatewayRoots) Get(_ context.Context, name string) (cid.Cid, bool, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	root, ok := r.roots[name]
	return root, ok, nil
}

func (r *memoryGatewayRoots) Put(_ context.Context, name string, root cid.Cid) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.roots[name] = root
	return nil
}

func TestReadGatewayLifecycleAndMutationAbsence(t *testing.T) {
	listen := reserveGatewayAddress(t)
	cfg := gatewayConfig{
		Enabled: true,
		Listen:  listen,
		Beacon:  gatewayBeaconConfig{GenesisTime: 1606824023},
	}
	if err := cfg.defaultsAndValidate(replicaHeads{}); err != nil {
		t.Fatal(err)
	}
	heads, err := server.NewHeads(server.HeadsConfig{Net: "testnet", Roots: newMemoryGatewayRoots()})
	if err != nil {
		t.Fatal(err)
	}
	blocks := blockstore.NewBlockstore(dssync.MutexWrap(ds.NewMapDatastore()))
	mx := newReplicaMetrics(nil)
	mx.health.Set("kubo_replica", true)
	mx.health.Set(kuboRuntimeGate, true)
	stop, failures, err := serveReadGateway(t.Context(), cfg, heads, blocks, mx,
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(stop)

	response, err := http.Get("http://" + listen + "/bloar/v1/heads")
	if err != nil {
		t.Fatal(err)
	}
	body, readErr := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if readErr != nil {
		t.Fatal(readErr)
	}
	if response.StatusCode != http.StatusOK || !strings.Contains(string(body), `"heads"`) {
		t.Fatalf("GET heads = %d %s", response.StatusCode, body)
	}

	mutation, err := http.Post("http://"+listen+"/bloar/v1/blobs", "application/json", strings.NewReader(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	_ = mutation.Body.Close()
	if mutation.StatusCode != http.StatusNotFound {
		t.Fatalf("POST mutation status = %d, want 404", mutation.StatusCode)
	}

	ready, waiting := mx.health.Ready()
	if !ready || len(waiting) != 0 {
		t.Fatalf("gateway readiness = %t waiting %v", ready, waiting)
	}
	stop()
	select {
	case err := <-failures:
		if err != nil {
			t.Fatalf("gateway shutdown = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("gateway did not report its clean shutdown")
	}
}

func TestReadGatewayBindFailureIsFatal(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	cfg := gatewayConfig{
		Enabled: true,
		Listen:  listener.Addr().String(),
		Beacon:  gatewayBeaconConfig{GenesisTime: 1606824023},
	}
	if err := cfg.defaultsAndValidate(replicaHeads{}); err != nil {
		t.Fatal(err)
	}
	heads, err := server.NewHeads(server.HeadsConfig{Net: "testnet", Roots: newMemoryGatewayRoots()})
	if err != nil {
		t.Fatal(err)
	}
	blocks := blockstore.NewBlockstore(dssync.MutexWrap(ds.NewMapDatastore()))
	if _, _, err := serveReadGateway(t.Context(), cfg, heads, blocks, newReplicaMetrics(nil),
		slog.New(slog.NewTextHandler(io.Discard, nil))); err == nil {
		t.Fatal("occupied gateway address was accepted")
	}
}

type gatewayBlobResolver struct {
	vh   schema.VersionedHash
	blob cid.Cid
}

func (r gatewayBlobResolver) ResolveBlob(_ context.Context, vh schema.VersionedHash) (cid.Cid, bool, error) {
	if vh != r.vh {
		return cid.Undef, false, nil
	}
	return r.blob, true, nil
}

// TestReadGatewayUsesOnlyKuboLocalBlocks builds a real archive DAG and then
// serves it through Kubo's HTTP RPC adapter. The fake RPC refuses any
// network-capable block/get, so success proves the public path uses
// BlockGetLocal (offline=true). The two failure cases also prove that a public
// miss or corrupt local block is surfaced rather than healed through Bitswap.
func TestReadGatewayUsesOnlyKuboLocalBlocks(t *testing.T) {
	const (
		headName = "all"
		slot     = uint64(64)
	)
	ctx := t.Context()
	source := blockstore.NewBlockstore(dssync.MutexWrap(ds.NewMapDatastore()))
	blob := make([]byte, schema.BlobSize)
	for lane := range schema.BlobSize / 32 {
		blob[lane*32+31] = byte(lane)
	}
	vh, err := ingest.VersionedHash(blob)
	if err != nil {
		t.Fatal(err)
	}
	blobCID, err := schema.BlobCID(blob)
	if err != nil {
		t.Fatal(err)
	}
	blobBlock, err := blocks.NewBlockWithCid(blob, blobCID)
	if err != nil {
		t.Fatal(err)
	}
	if err := source.Put(ctx, blobBlock); err != nil {
		t.Fatal(err)
	}
	written, err := archive.New(ctx, archive.Config{
		Blocks: source, Resolver: gatewayBlobResolver{vh: vh, blob: blobCID},
	}, archive.Params{Name: headName, Net: "testnet", OriginSlot: slot, SegBits: 3, FanoutBits: 2})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := written.ApplyRefs(ctx, []archive.RefRow{{Slot: slot, VHs: []schema.VersionedHash{vh}}}, slot); err != nil {
		t.Fatal(err)
	}

	served := make(map[string][]byte)
	keys, err := source.AllKeysChan(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for key := range keys {
		block, err := source.Get(ctx, key)
		if err != nil {
			t.Fatal(err)
		}
		served[string(key.Hash())] = bytes.Clone(block.RawData())
	}

	var servedMu sync.RWMutex
	var localReads atomic.Int64
	var networkReads atomic.Int64
	kuboRPC := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/api/v0/block/get" {
			http.Error(w, "unexpected Kubo RPC", http.StatusNotFound)
			return
		}
		switch r.URL.Query().Get("offline") {
		case "true":
			localReads.Add(1)
		case "false":
			networkReads.Add(1)
			http.Error(w, "network reads are forbidden", http.StatusTeapot)
			return
		default:
			http.Error(w, "offline mode was not explicit", http.StatusBadRequest)
			return
		}
		target, err := cid.Decode(r.URL.Query().Get("arg"))
		if err != nil {
			http.Error(w, "invalid CID", http.StatusBadRequest)
			return
		}
		servedMu.RLock()
		raw, ok := served[string(target.Hash())]
		servedMu.RUnlock()
		if !ok {
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"Message": "block was not found locally (offline)",
				"Code":    0,
				"Type":    "error",
			})
			return
		}
		w.Header().Set("Content-Type", "application/vnd.ipld.raw")
		_, _ = w.Write(raw)
	}))
	defer kuboRPC.Close()

	client, err := kubo.New(kubo.Config{
		BaseURL:              kuboRPC.URL,
		AllowUnauthenticated: true,
		RequestTimeout:       time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	localRaw, err := kubo.NewLocalBlockstore(client, kubo.BlockstoreConfig{
		Enumeration: kubo.ListLimits{MaxItems: 1, MaxBytes: 1024},
	})
	if err != nil {
		t.Fatal(err)
	}
	local, err := kubo.NewReplicaBlockstore(localRaw)
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := archive.Load(ctx, archive.Config{Blocks: local}, written.Root())
	if err != nil {
		t.Fatal(err)
	}
	roots := newMemoryGatewayRoots()
	if err := roots.Put(ctx, headName, written.Root()); err != nil {
		t.Fatal(err)
	}
	heads, err := server.NewHeads(server.HeadsConfig{
		Net: "testnet", Roots: roots, Blocks: local,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := heads.Add(loaded); err != nil {
		t.Fatal(err)
	}

	listen := reserveGatewayAddress(t)
	cfg := gatewayConfig{
		Enabled: true,
		Listen:  listen,
		Beacon:  gatewayBeaconConfig{GenesisTime: 1606824023},
	}
	if err := cfg.defaultsAndValidate(replicaHeads{}); err != nil {
		t.Fatal(err)
	}
	stop, _, err := serveReadGateway(ctx, cfg, heads, local, newReplicaMetrics([]string{headName}),
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(stop)

	url := "http://" + listen + "/" + headName + "/eth/v1/beacon/blobs/" +
		"64?versioned_hashes=0x" + hex.EncodeToString(vh[:])
	response, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	var body struct {
		Data []string `json:"data"`
	}
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusOK || len(body.Data) != 1 ||
		body.Data[0] != "0x"+hex.EncodeToString(blob) {
		t.Fatalf("gateway blob response = status %d data entries %d", response.StatusCode, len(body.Data))
	}
	if localReads.Load() == 0 || networkReads.Load() != 0 {
		t.Fatalf("Kubo reads after success: local=%d network=%d", localReads.Load(), networkReads.Load())
	}

	const parallelReads = 16
	failures := make(chan error, parallelReads)
	var group sync.WaitGroup
	for range parallelReads {
		group.Add(1)
		go func() {
			defer group.Done()
			response, err := http.Get(url)
			if err != nil {
				failures <- err
				return
			}
			defer response.Body.Close()
			var body struct {
				Data []string `json:"data"`
			}
			if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
				failures <- err
				return
			}
			if response.StatusCode != http.StatusOK || len(body.Data) != 1 ||
				body.Data[0] != "0x"+hex.EncodeToString(blob) {
				failures <- fmt.Errorf("status %d entries %d", response.StatusCode, len(body.Data))
			}
		}()
	}
	group.Wait()
	close(failures)
	for err := range failures {
		t.Errorf("parallel Kubo-local gateway read: %v", err)
	}
	if networkReads.Load() != 0 {
		t.Fatalf("parallel local reads triggered %d network-capable reads", networkReads.Load())
	}

	servedMu.Lock()
	delete(served, string(blobCID.Hash()))
	servedMu.Unlock()
	response, err = http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("missing Kubo-local blob status = %d, want 503", response.StatusCode)
	}
	if networkReads.Load() != 0 {
		t.Fatalf("missing local block triggered %d network-capable reads", networkReads.Load())
	}

	servedMu.Lock()
	served[string(blobCID.Hash())] = []byte("corrupt bytes under the requested CID")
	servedMu.Unlock()
	response, err = http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusInternalServerError {
		t.Fatalf("corrupt Kubo-local blob status = %d, want 500", response.StatusCode)
	}
	if networkReads.Load() != 0 {
		t.Fatalf("corrupt local block triggered %d network-capable reads", networkReads.Load())
	}
}

func reserveGatewayAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return address
}
