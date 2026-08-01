package main

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
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

// TestDisposableKubo042FullDaemonReadGateway exercises the same validated
// configuration and serve path as the packaged daemon. Everything it owns is
// disposable: one stock Kubo 0.42 repository, one signed publication source,
// one replica state directory, and loopback listeners reserved by the test.
//
// It deliberately covers the operational gap below the component tests:
//
//   - first adoption through the follower and retention controller;
//   - read-only Bloar/beacon service with Kubo as the sole archive store;
//   - bounded concurrent load with measured Kubo RPC amplification and heap use;
//   - source-offline GC and restart from the durable checkpoint;
//   - configured public admission rejection and replenishment; and
//   - listener rollback without deleting Kubo, MFS, or unrelated pins.
func TestDisposableKubo042FullDaemonReadGateway(t *testing.T) {
	requireDisposableKuboIntegration(t)
	binary := requireDisposableKuboBinary(t)

	daemon := newDisposableKuboDaemon(t, binary)
	t.Cleanup(func() {
		if err := daemon.stop(); err != nil {
			t.Errorf("stopping disposable Kubo: %v", err)
		}
	})
	daemon.start(t)

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
	defer cancel()
	admin := newDisposableKuboClient(t, daemon.baseURL())
	assertDisposableSwarmIsolated(t, ctx, admin)

	// Count the exact Kubo RPCs made by the production replica without changing
	// their responses. The admin harness continues to use Kubo directly, so its
	// setup and GC calls cannot contaminate the serving-path measurement.
	upstream, err := url.Parse(daemon.baseURL())
	if err != nil {
		t.Fatal(err)
	}
	var localBlockGets atomic.Int64
	var networkBlockGets atomic.Int64
	reverse := httputil.NewSingleHostReverseProxy(upstream)
	kuboProxy := httptestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v0/block/get" {
			switch r.URL.Query().Get("offline") {
			case "true":
				localBlockGets.Add(1)
			case "false":
				networkBlockGets.Add(1)
			}
		}
		reverse.ServeHTTP(w, r)
	}))

	const (
		headName = "all"
		slot     = uint64(128)
	)
	sourceBlocks := blockstore.NewBlockstore(dssync.MutexWrap(ds.NewMapDatastore()))
	blob := deterministicGatewayBlob(t)
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
	if err := sourceBlocks.Put(ctx, blobBlock); err != nil {
		t.Fatal(err)
	}
	written, err := archive.New(ctx, archive.Config{
		Blocks: sourceBlocks, Resolver: gatewayBlobResolver{vh: vh, blob: blobCID},
	}, archive.Params{Name: headName, Net: "testnet", OriginSlot: slot, SegBits: 3, FanoutBits: 2})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := written.ApplyRefs(ctx, []archive.RefRow{{Slot: slot, VHs: []schema.VersionedHash{vh}}}, slot); err != nil {
		t.Fatal(err)
	}
	putAllDisposableBlocks(t, ctx, admin, sourceBlocks)

	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	publicationBody := signedDisposablePublication(t, private, written.Info())
	publication := httptestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/bloar/v1/heads" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(publicationBody)
	}))

	// One independent recursive pin and one MFS file represent operator-owned
	// Kubo state. The replica must neither rename nor delete either across GC and
	// restart.
	unrelated := disposableRawBlock(t, "operator-owned unrelated recursive pin")
	putDisposableBlocks(t, ctx, admin, unrelated)
	const unrelatedPinName = "operator-unrelated"
	if err := admin.PinAddNamedRecursive(ctx, unrelated.Cid(), unrelatedPinName); err != nil {
		t.Fatalf("pinning unrelated block: %v", err)
	}
	const mfsPath = "/operator/sentinel.txt"
	const mfsBody = "operator MFS state must survive replica tests\n"
	runDisposableKuboInput(t, daemon, mfsBody, "files", "write", "--create", "--parents", mfsPath)

	ports, releasePorts := reserveDisposableLoopbackPorts(t, 2)
	releasePorts()
	statePath := filepath.Join(t.TempDir(), "replica-state")
	configPath := writeFullDaemonConfig(t, fullDaemonConfig{
		StatePath:    statePath,
		SourceURL:    publication.URL,
		PublicKey:    hex.EncodeToString(public),
		KuboAPI:      kuboProxy.URL,
		Gateway:      fmt.Sprintf("127.0.0.1:%d", ports[0]),
		Metrics:      fmt.Sprintf("127.0.0.1:%d", ports[1]),
		LowAdmission: false,
	})
	cfg, err := loadConfig(configPath)
	if err != nil {
		t.Fatalf("loading full-daemon config: %v", err)
	}

	gatewayBase := "http://" + cfg.Gateway.Listen
	metricsBase := "http://" + cfg.Metrics.Listen
	first := startDisposableReplica(t, cfg)
	defer first.cancel()
	waitForExactGatewayBlob(t, gatewayBase, headName, slot, vh, blob, 40*time.Second)
	waitForMetric(t, metricsBase, "bloar_replica_gateway_enabled 1", 10*time.Second)
	waitForMetric(t, metricsBase, "bloar_replica_gateway_serving 1", 10*time.Second)
	assertGatewayMutationAbsent(t, gatewayBase)

	localBefore := localBlockGets.Load()
	networkBefore := networkBlockGets.Load()
	load := runGatewayLoad(t, gatewayBase, headName, slot, vh, 128, 16)
	localDelta := localBlockGets.Load() - localBefore
	networkDelta := networkBlockGets.Load() - networkBefore
	if networkDelta != 0 {
		t.Fatalf("load used %d network-capable Kubo block/get calls, want 0", networkDelta)
	}
	if localDelta == 0 || localDelta > int64(load.Requests*16) {
		t.Fatalf("load Kubo-local RPCs = %d for %d requests, want in [1,%d]",
			localDelta, load.Requests, load.Requests*16)
	}
	if load.PeakHeapDelta > 256<<20 {
		t.Fatalf("load heap delta = %d bytes, want <= 256 MiB", load.PeakHeapDelta)
	}
	if load.P99 > 5*time.Second {
		t.Fatalf("load p99 = %s, want <= 5s on disposable local Kubo", load.P99)
	}
	t.Logf("bounded load: requests=%d concurrency=%d p50=%s p95=%s p99=%s max=%s "+
		"local_block_gets=%d rpc_per_request=%.2f heap_delta=%d",
		load.Requests, load.Concurrency, load.P50, load.P95, load.P99, load.Max,
		localDelta, float64(localDelta)/float64(load.Requests), load.PeakHeapDelta)

	// The publication source is now genuinely unreachable. Reads must continue,
	// a real Kubo GC must preserve the adopted generation and operator state, and
	// the daemon must then stop without leaving either listener behind.
	publication.Close()
	runDisposableRepoGC(t, ctx, admin)
	waitForExactGatewayBlob(t, gatewayBase, headName, slot, vh, blob, 5*time.Second)
	assertDisposableRecursivePin(t, ctx, admin, unrelated.Cid(), unrelatedPinName)
	if got := runDisposableKuboOutput(t, daemon, "files", "read", mfsPath); got != mfsBody {
		t.Fatalf("MFS sentinel = %q, want %q", got, mfsBody)
	}
	stopDisposableReplica(t, first)
	waitForLoopbackClosed(t, cfg.Gateway.Listen)
	waitForLoopbackClosed(t, cfg.Metrics.Listen)

	// Restart from the same validated state while the source remains down. A low
	// configured admission budget makes the first exact read consume the burst,
	// the immediate repeat reject, and a replenished repeat succeed.
	restartConfigPath := writeFullDaemonConfig(t, fullDaemonConfig{
		StatePath:    statePath,
		SourceURL:    publication.URL,
		PublicKey:    hex.EncodeToString(public),
		KuboAPI:      kuboProxy.URL,
		Gateway:      cfg.Gateway.Listen,
		Metrics:      cfg.Metrics.Listen,
		LowAdmission: true,
	})
	restartCfg, err := loadConfig(restartConfigPath)
	if err != nil {
		t.Fatalf("loading restart config: %v", err)
	}
	second := startDisposableReplica(t, restartCfg)
	defer second.cancel()
	waitForExactGatewayBlob(t, gatewayBase, headName, slot, vh, blob, 20*time.Second)
	assertGatewayStatus(t, gatewayBlobURL(gatewayBase, headName, slot, vh), http.StatusTooManyRequests)
	time.Sleep(2100 * time.Millisecond)
	assertExactGatewayBlob(t, gatewayBase, headName, slot, vh, blob)
	assertDisposableRecursivePin(t, ctx, admin, unrelated.Cid(), unrelatedPinName)
	if got := runDisposableKuboOutput(t, daemon, "files", "read", mfsPath); got != mfsBody {
		t.Fatalf("MFS sentinel after restart = %q, want %q", got, mfsBody)
	}
	assertDisposableSwarmIsolated(t, ctx, admin)
	if _, err := os.Stat(filepath.Join(statePath, "blocks")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("replica state unexpectedly contains a FlatFS blocks directory: %v", err)
	}
	stopDisposableReplica(t, second)
	waitForLoopbackClosed(t, restartCfg.Gateway.Listen)
	waitForLoopbackClosed(t, restartCfg.Metrics.Listen)
}

type fullDaemonConfig struct {
	StatePath    string
	SourceURL    string
	PublicKey    string
	KuboAPI      string
	Gateway      string
	Metrics      string
	LowAdmission bool
}

func writeFullDaemonConfig(t *testing.T, input fullDaemonConfig) string {
	t.Helper()
	admission := ""
	if input.LowAdmission {
		admission = `
  max_query_hashes: 1
  public_read_admission:
    global_rate: 1
    global_burst: 2
    client_rate: 1
    client_burst: 2
    client_buckets: 16
    client_bucket_ttl: 1m`
	}
	body := fmt.Sprintf(`version: 1
net: testnet
replica:
  id: local-gateway-proof
  state_path: %s
  pin_name: bloar-replica/v1/local-gateway-proof
  heads: [all]
  audit_interval: 10s
source:
  url: %s
  pubkey: %s
  poll_interval: 10s
  fetch_timeout: 2s
kubo:
  api: %s
  allow_unauthenticated: true
  request_timeout: 5s
  pin_timeout: 1m
  announce_interval: 1m
gateway:
  enabled: true
  listen: %s
  beacon:
    genesis_time: 1606824023%s
metrics:
  listen: %s
`, input.StatePath, input.SourceURL, input.PublicKey, input.KuboAPI, input.Gateway, admission, input.Metrics)
	path := filepath.Join(t.TempDir(), "replica.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

type disposableReplicaRun struct {
	cancel context.CancelFunc
	done   chan error
}

func startDisposableReplica(t *testing.T, cfg *config) disposableReplicaRun {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- serve(ctx, cfg, false) }()
	return disposableReplicaRun{cancel: cancel, done: done}
}

func stopDisposableReplica(t *testing.T, run disposableReplicaRun) {
	t.Helper()
	run.cancel()
	select {
	case err := <-run.done:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("stopping replica daemon: %v", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("replica daemon did not stop")
	}
}

func requireDisposableKuboIntegration(t *testing.T) {
	t.Helper()
	switch gate := os.Getenv(disposableKuboIntegrationEnv); gate {
	case "":
		t.Skip("set " + disposableKuboIntegrationEnv + "=1 to run the disposable full-daemon gateway test")
	case "1":
	default:
		t.Fatalf("%s must be exactly 1 when set", disposableKuboIntegrationEnv)
	}
}

func requireDisposableKuboBinary(t *testing.T) string {
	t.Helper()
	name := os.Getenv("BLOAR_KUBO_BINARY")
	if name == "" {
		name = "ipfs"
	}
	binary, err := exec.LookPath(name)
	if err != nil {
		t.Fatalf("locating Kubo ipfs binary %q: %v", name, err)
	}
	requireStableKubo042CLI(t, binary)
	return binary
}

func httptestServer(t *testing.T, handler http.Handler) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	return server
}

func deterministicGatewayBlob(t *testing.T) []byte {
	t.Helper()
	blob := make([]byte, schema.BlobSize)
	for lane := range schema.BlobSize / 32 {
		blob[lane*32+31] = byte(lane + 1)
	}
	return blob
}

func signedDisposablePublication(t *testing.T, key ed25519.PrivateKey, info archive.Info) []byte {
	t.Helper()
	entry := server.HeadEntry{
		Name:       info.Name,
		Root:       info.Root.String(),
		OriginSlot: info.OriginSlot,
		SyncedTo:   info.SyncedTo,
		SegBits:    info.SegBits,
		FanoutBits: info.FanoutBits,
		DirDepth:   info.DirDepth,
	}
	doc := server.Doc{Unsigned: server.Unsigned{
		V: server.LegacyDocVersion, Net: info.Net,
		UpdatedAt: time.Unix(1, 0).UTC().Format(time.RFC3339),
		Heads:     []server.HeadEntry{entry},
	}}
	canonical, err := doc.Unsigned.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	doc.Pubkey = hex.EncodeToString(key.Public().(ed25519.PublicKey))
	doc.Signature = hex.EncodeToString(ed25519.Sign(key, canonical))
	if err := doc.Verify(); err != nil {
		t.Fatalf("verifying publication fixture: %v", err)
	}
	body, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func putAllDisposableBlocks(t *testing.T, ctx context.Context, target *kubo.Client, source blockstore.Blockstore) {
	t.Helper()
	keys, err := source.AllKeysChan(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for key := range keys {
		block, err := source.Get(ctx, key)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := target.BlockPut(ctx, block); err != nil {
			t.Fatalf("putting archive block %s: %v", block.Cid(), err)
		}
	}
}

func assertDisposableSwarmIsolated(t *testing.T, ctx context.Context, client *kubo.Client) {
	t.Helper()
	peers, err := client.SwarmPeers(ctx)
	if err != nil {
		// The shared disposable daemon is intentionally started with --offline
		// after Bootstrap=[] and MDNS=false are written. Kubo 0.42 refuses the
		// swarm/peers RPC in that mode; that refusal is stronger isolation
		// evidence than an empty online snapshot because the daemon cannot dial
		// or accept any libp2p peer for the whole run.
		if strings.Contains(err.Error(), "must be run in online mode") {
			return
		}
		t.Fatalf("listing disposable Kubo peers: %v", err)
	}
	if len(peers) != 0 {
		t.Fatalf("disposable Kubo discovered peers despite empty bootstrap and disabled mDNS: %+v", peers)
	}
}

func runDisposableKuboInput(t *testing.T, daemon *disposableKuboDaemon, input string, args ...string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, daemon.binary, args...)
	cmd.Env = daemon.isolatedEnv()
	cmd.Stdin = strings.NewReader(input)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("%s %s failed: %v\n%s", daemon.binary, strings.Join(args, " "), err, boundedDisposableOutput(output))
	}
}

func runDisposableKuboOutput(t *testing.T, daemon *disposableKuboDaemon, args ...string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, daemon.binary, args...)
	cmd.Env = daemon.isolatedEnv()
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("%s %s failed: %v\n%s", daemon.binary, strings.Join(args, " "), err, boundedDisposableOutput(output))
	}
	return string(output)
}

func gatewayBlobURL(base, head string, slot uint64, vh schema.VersionedHash) string {
	return fmt.Sprintf("%s/%s/eth/v1/beacon/blobs/%d?versioned_hashes=0x%s",
		base, head, slot, hex.EncodeToString(vh[:]))
}

func waitForExactGatewayBlob(t *testing.T, base, head string, slot uint64, vh schema.VersionedHash, blob []byte, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last error
	for time.Now().Before(deadline) {
		last = checkExactGatewayBlob(base, head, slot, vh, blob)
		if last == nil {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("gateway did not serve exact blob within %s: %v", timeout, last)
}

func assertExactGatewayBlob(t *testing.T, base, head string, slot uint64, vh schema.VersionedHash, blob []byte) {
	t.Helper()
	if err := checkExactGatewayBlob(base, head, slot, vh, blob); err != nil {
		t.Fatal(err)
	}
}

func checkExactGatewayBlob(base, head string, slot uint64, vh schema.VersionedHash, blob []byte) error {
	response, err := http.Get(gatewayBlobURL(base, head, slot, vh))
	if err != nil {
		return err
	}
	defer response.Body.Close()
	var body struct {
		Data []string `json:"data"`
	}
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		return err
	}
	want := "0x" + hex.EncodeToString(blob)
	if response.StatusCode != http.StatusOK || len(body.Data) != 1 || body.Data[0] != want {
		return fmt.Errorf("gateway response = status %d entries %d exact %t",
			response.StatusCode, len(body.Data), len(body.Data) == 1 && body.Data[0] == want)
	}
	return nil
}

func assertGatewayStatus(t *testing.T, target string, want int) {
	t.Helper()
	response, err := http.Get(target)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, response.Body)
	if response.StatusCode != want {
		t.Fatalf("GET %s status = %d, want %d", target, response.StatusCode, want)
	}
}

func assertGatewayMutationAbsent(t *testing.T, base string) {
	t.Helper()
	response, err := http.Post(base+"/bloar/v1/blobs", "application/json", strings.NewReader(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusNotFound {
		t.Fatalf("POST mutation status = %d, want 404", response.StatusCode)
	}
}

func waitForMetric(t *testing.T, base, needle string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last string
	for time.Now().Before(deadline) {
		response, err := http.Get(base + "/metrics")
		if err == nil {
			body, readErr := io.ReadAll(response.Body)
			_ = response.Body.Close()
			if readErr == nil {
				last = string(body)
				if strings.Contains(last, needle) {
					return
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("metric %q did not appear within %s; final body contains %d bytes", needle, timeout, len(last))
}

func waitForLoopbackClosed(t *testing.T, address string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", address, 100*time.Millisecond)
		if err != nil {
			return
		}
		_ = conn.Close()
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("listener %s remained open after daemon shutdown", address)
}

type gatewayLoadResult struct {
	Requests      int
	Concurrency   int
	P50           time.Duration
	P95           time.Duration
	P99           time.Duration
	Max           time.Duration
	PeakHeapDelta uint64
}

func runGatewayLoad(t *testing.T, base, head string, slot uint64, vh schema.VersionedHash, requests, concurrency int) gatewayLoadResult {
	t.Helper()
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	var peak atomic.Uint64
	peak.Store(before.Alloc)
	sampleDone := make(chan struct{})
	go func() {
		ticker := time.NewTicker(2 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				var current runtime.MemStats
				runtime.ReadMemStats(&current)
				for previous := peak.Load(); current.Alloc > previous && !peak.CompareAndSwap(previous, current.Alloc); previous = peak.Load() {
				}
			case <-sampleDone:
				return
			}
		}
	}()

	target := gatewayBlobURL(base, head, slot, vh)
	client := &http.Client{Timeout: 10 * time.Second}
	jobs := make(chan int)
	latencies := make([]time.Duration, requests)
	failures := make(chan error, requests)
	var wg sync.WaitGroup
	for range concurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for index := range jobs {
				started := time.Now()
				response, err := client.Get(target)
				if err == nil {
					_, err = io.Copy(io.Discard, response.Body)
					closeErr := response.Body.Close()
					if err == nil {
						err = closeErr
					}
					if err == nil && response.StatusCode != http.StatusOK {
						err = fmt.Errorf("status %d", response.StatusCode)
					}
				}
				latencies[index] = time.Since(started)
				if err != nil {
					failures <- fmt.Errorf("request %d: %w", index, err)
				}
			}
		}()
	}
	for index := range requests {
		jobs <- index
	}
	close(jobs)
	wg.Wait()
	close(sampleDone)
	close(failures)
	for err := range failures {
		t.Fatal(err)
	}
	slices.Sort(latencies)
	percentile := func(percent int) time.Duration {
		index := (len(latencies)*percent + 99) / 100
		if index < 1 {
			index = 1
		}
		return latencies[index-1]
	}
	peakAlloc := peak.Load()
	if peakAlloc < before.Alloc {
		peakAlloc = before.Alloc
	}
	return gatewayLoadResult{
		Requests: requests, Concurrency: concurrency,
		P50: percentile(50), P95: percentile(95), P99: percentile(99), Max: latencies[len(latencies)-1],
		PeakHeapDelta: peakAlloc - before.Alloc,
	}
}
