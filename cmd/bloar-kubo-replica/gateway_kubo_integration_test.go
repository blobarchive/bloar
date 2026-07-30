package main

import (
	"encoding/hex"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/ipfs/boxo/blockstore"
	blocks "github.com/ipfs/go-block-format"
	ds "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"

	"github.com/blobarchive/bloar/archive"
	"github.com/blobarchive/bloar/ingest"
	"github.com/blobarchive/bloar/kubo"
	"github.com/blobarchive/bloar/schema"
	"github.com/blobarchive/bloar/server"
)

// TestDisposableKubo042ReadGateway proves the complete local serving path
// against a test-owned stock Kubo 0.42 daemon. It is gated with the existing
// destructive Kubo integration switch because it creates a repository and
// invokes repo/gc, but never accepts an external Kubo URL.
func TestDisposableKubo042ReadGateway(t *testing.T) {
	switch gate := os.Getenv(disposableKuboIntegrationEnv); gate {
	case "":
		t.Skip("set " + disposableKuboIntegrationEnv + "=1 to run the disposable Kubo read-gateway test")
	case "1":
	default:
		t.Fatalf("%s must be exactly 1 when set", disposableKuboIntegrationEnv)
	}
	binaryName := os.Getenv("BLOAR_KUBO_BINARY")
	if binaryName == "" {
		binaryName = "ipfs"
	}
	binary, err := exec.LookPath(binaryName)
	if err != nil {
		t.Fatalf("locating Kubo ipfs binary %q: %v", binaryName, err)
	}
	requireStableKubo042CLI(t, binary)

	daemon := newDisposableKuboDaemon(t, binary)
	t.Cleanup(func() {
		if err := daemon.stop(); err != nil {
			t.Errorf("stopping disposable Kubo: %v", err)
		}
	})
	daemon.start(t)

	const (
		headName = "all"
		slot     = uint64(128)
		pinName  = "bloar-replica-gateway-integration"
	)
	ctx := t.Context()
	source := blockstore.NewBlockstore(dssync.MutexWrap(ds.NewMapDatastore()))
	blob := make([]byte, schema.BlobSize)
	for lane := range schema.BlobSize / 32 {
		blob[lane*32+31] = byte(lane + 1)
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

	admin := newDisposableKuboClient(t, daemon.baseURL())
	keys, err := source.AllKeysChan(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for key := range keys {
		block, err := source.Get(ctx, key)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := admin.BlockPut(ctx, block); err != nil {
			t.Fatalf("put archive block %s into disposable Kubo: %v", block.Cid(), err)
		}
	}
	if err := admin.PinAddNamedRecursive(ctx, written.Root(), pinName); err != nil {
		t.Fatalf("pin archive root: %v", err)
	}
	sentinel := disposableRawBlock(t, "unrelated and intentionally unpinned")
	if _, err := admin.BlockPut(ctx, sentinel); err != nil {
		t.Fatal(err)
	}
	removed := runDisposableRepoGC(t, ctx, admin)
	if !containsDisposableCID(removed.Removed, sentinel.Cid()) {
		t.Fatalf("repo/gc did not remove sentinel %s: %v", sentinel.Cid(), removed.Removed)
	}

	query := func(base string) {
		t.Helper()
		url := base + "/" + headName + "/eth/v1/beacon/blobs/128?versioned_hashes=0x" +
			hex.EncodeToString(vh[:])
		response, err := http.Get(url)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		var body struct {
			Data []string `json:"data"`
		}
		if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if response.StatusCode != http.StatusOK || len(body.Data) != 1 ||
			body.Data[0] != "0x"+hex.EncodeToString(blob) {
			t.Fatalf("Kubo gateway response = status %d entries %d", response.StatusCode, len(body.Data))
		}
	}

	start := func(client *kubo.Client) (string, func()) {
		t.Helper()
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
			Enabled: true, Listen: listen,
			Beacon: gatewayBeaconConfig{GenesisTime: 1606824023},
		}
		if err := cfg.defaultsAndValidate(replicaHeads{}); err != nil {
			t.Fatal(err)
		}
		stop, failures, err := serveReadGateway(ctx, cfg, heads, local, newReplicaMetrics([]string{headName}),
			slog.New(slog.NewTextHandler(io.Discard, nil)))
		if err != nil {
			t.Fatal(err)
		}
		return "http://" + listen, func() {
			stop()
			select {
			case err := <-failures:
				if err != nil {
					t.Fatalf("gateway shutdown: %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("gateway did not stop")
			}
		}
	}

	replicaClient := newDisposableKuboClient(t, daemon.baseURL())
	base, stopGateway := start(replicaClient)
	query(base)
	stopGateway()

	if err := daemon.stop(); err != nil {
		t.Fatalf("stopping Kubo for restart: %v", err)
	}
	daemon.start(t)
	replicaClient = newDisposableKuboClient(t, daemon.baseURL())
	base, stopGateway = start(replicaClient)
	defer stopGateway()
	query(base)
	assertDisposableRecursivePin(t, ctx, replicaClient, written.Root(), pinName)
}
