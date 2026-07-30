package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"

	"github.com/blobarchive/bloar/kubo"
	"github.com/blobarchive/bloar/replica"
)

const disposableKuboIntegrationEnv = "BLOAR_KUBO_DISPOSABLE_INTEGRATION"

// Run against a disposable, test-owned stock Kubo 0.42 daemon:
//
//	BLOAR_KUBO_DISPOSABLE_INTEGRATION=1 go test ./cmd/bloar-kubo-replica -run TestDisposableKubo042GCAndRestartRetention -v
//
// BLOAR_KUBO_BINARY may name a non-default ipfs binary. This test deliberately
// has no external-URL option: it creates the repository itself and is therefore
// the only code allowed to invoke repo/gc on the daemon under test.
func TestDisposableKubo042GCAndRestartRetention(t *testing.T) {
	switch gate := os.Getenv(disposableKuboIntegrationEnv); gate {
	case "":
		t.Skip("set " + disposableKuboIntegrationEnv + "=1 to run the disposable Kubo GC/restart test")
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

	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()

	// Only replicaClient is passed to the production retention backend. The
	// independently constructed adminClient belongs to this test harness and is
	// the sole caller of block setup, repo/gc, and physical-state assertions.
	replicaClient := newDisposableKuboClient(t, daemon.baseURL())
	info, err := replicaClient.CheckReplicaCompatibility(ctx)
	if err != nil {
		t.Fatalf("replica compatibility: %v", err)
	}
	if !strings.HasPrefix(strings.TrimPrefix(info.Version, "v"), "0.42.") {
		t.Fatalf("Kubo version = %q, want stable 0.42.x", info.Version)
	}
	if err := checkProvidePolicy(ctx, replicaClient); err != nil {
		t.Fatalf("replica provide policy: %v", err)
	}
	adminClient := newDisposableKuboClient(t, daemon.baseURL())

	const replicaID = "disposable-kubo-gc"
	const pinName = "bloar-replica/v1/" + replicaID
	metadataDir := filepath.Join(t.TempDir(), "controller")
	db, controller := openDisposableController(t, metadataDir, replicaClient, replicaID)
	defer func() {
		if db != nil {
			if err := db.Close(); err != nil {
				t.Errorf("closing controller database: %v", err)
			}
		}
	}()

	filteredA := disposableRawBlock(t, "filtered generation A")
	mutableA := disposableRawBlock(t, "mutable generation A")
	filteredB := disposableRawBlock(t, "filtered generation B")
	mutableB := disposableRawBlock(t, "mutable generation B")
	sentinel := disposableRawBlock(t, "deliberately unpinned GC sentinel")
	pairA := replica.Generation{
		UpdatedAt: time.Unix(1, 0).UTC(),
		Heads: []replica.Head{
			{Name: "arbitrum-one", Root: filteredA.Cid(), SyncedTo: 100},
			{Name: "unfinalized", Root: mutableA.Cid(), SyncedTo: 108},
		},
	}
	pairB := replica.Generation{
		UpdatedAt: time.Unix(2, 0).UTC(),
		Heads: []replica.Head{
			{Name: "arbitrum-one", Root: filteredB.Cid(), SyncedTo: 109},
			{Name: "unfinalized", Root: mutableB.Cid(), SyncedTo: 117},
		},
	}
	normalizedA, anchorA := disposableGeneration(t, replicaID, pairA)
	normalizedB, anchorB := disposableGeneration(t, replicaID, pairB)

	putDisposableBlocks(t, ctx, adminClient, filteredA, mutableA, sentinel)
	if err := controller.Prepare(ctx, pairA); err != nil {
		t.Fatalf("prepare pair A: %v", err)
	}
	if err := controller.Commit(ctx, pairA); err != nil {
		t.Fatalf("commit pair A: %v", err)
	}
	assertDisposableRecursivePin(t, ctx, adminClient, anchorA.Cid(), pinName)
	assertDisposableBlocksPresent(t, ctx, adminClient, anchorA, filteredA, mutableA)

	// Current-only boundary: a real repo GC must retain A through its anchor and
	// collect an unrelated block, proving this pass did actual destructive work.
	removed := runDisposableRepoGC(t, ctx, adminClient)
	if !containsDisposableCID(removed.Removed, sentinel.Cid()) {
		t.Fatalf("current-only GC did not report removing sentinel %s: %v", sentinel.Cid(), removed.Removed)
	}
	assertDisposableBlockAbsent(t, ctx, adminClient, sentinel.Cid())
	assertDisposableBlocksPresent(t, ctx, adminClient, anchorA, filteredA, mutableA)

	// Add B only after the first GC: until Prepare establishes B's recursive
	// anchor, these leaves are intentionally unprotected and collectible.
	putDisposableBlocks(t, ctx, adminClient, filteredB, mutableB)
	if err := controller.Prepare(ctx, pairB); err != nil {
		t.Fatalf("prepare pair B: %v", err)
	}
	status, err := controller.Status()
	if err != nil {
		t.Fatalf("status with pair B pending: %v", err)
	}
	if status.CurrentGeneration == nil || !status.CurrentGeneration.Equal(normalizedA) ||
		status.PendingGeneration == nil || !status.PendingGeneration.Equal(normalizedB) {
		t.Fatalf("prepare state does not retain complete A/B generations: %+v", status)
	}
	assertDisposableRecursivePin(t, ctx, adminClient, anchorA.Cid(), pinName)
	assertDisposableRecursivePin(t, ctx, adminClient, anchorB.Cid(), pinName)

	// Prepare boundary: Kubo's pin/update is add-before-remove with unpin=false,
	// so a concurrent collector serialized at this boundary must see both pins.
	runDisposableRepoGC(t, ctx, adminClient)
	assertDisposableBlocksPresent(t, ctx, adminClient,
		anchorA, filteredA, mutableA, anchorB, filteredB, mutableB)

	if err := controller.Commit(ctx, pairB); err != nil {
		t.Fatalf("commit pair B: %v", err)
	}
	assertDisposableNotPinned(t, ctx, adminClient, anchorA.Cid())
	assertDisposableRecursivePin(t, ctx, adminClient, anchorB.Cid(), pinName)

	// Committed boundary: retiring A makes only A collectible; B remains the
	// complete filtered/mutable pair protected by one exact recursive anchor.
	runDisposableRepoGC(t, ctx, adminClient)
	assertDisposableBlockAbsent(t, ctx, adminClient, anchorA.Cid())
	assertDisposableBlockAbsent(t, ctx, adminClient, filteredA.Cid())
	assertDisposableBlockAbsent(t, ctx, adminClient, mutableA.Cid())
	assertDisposableBlocksPresent(t, ctx, adminClient, anchorB, filteredB, mutableB)

	if err := db.Close(); err != nil {
		t.Fatalf("closing controller before restart: %v", err)
	}
	db = nil
	if err := daemon.stop(); err != nil {
		t.Fatalf("stopping Kubo before restart: %v", err)
	}
	daemon.start(t)

	// Recreate both clients and the controller to ensure no in-memory state from
	// either side contributes to recovery.
	replicaClient = newDisposableKuboClient(t, daemon.baseURL())
	if _, err := replicaClient.CheckReplicaCompatibility(ctx); err != nil {
		t.Fatalf("replica compatibility after restart: %v", err)
	}
	if err := checkProvidePolicy(ctx, replicaClient); err != nil {
		t.Fatalf("replica provide policy after restart: %v", err)
	}
	adminClient = newDisposableKuboClient(t, daemon.baseURL())
	db, controller = openDisposableController(t, metadataDir, replicaClient, replicaID)
	if err := controller.Recover(ctx); err != nil {
		t.Fatalf("recover pair B after Kubo/controller restart: %v", err)
	}
	protected, err := controller.ProtectsAll(ctx, pairB.Heads)
	if err != nil {
		t.Fatalf("prove pair B protection after restart: %v", err)
	}
	if !protected.Equal(normalizedB) {
		t.Fatalf("protected generation = %+v, want %+v", protected, normalizedB)
	}
	status, err = controller.Status()
	if err != nil {
		t.Fatalf("status after recovery: %v", err)
	}
	if status.CurrentGeneration == nil || !status.CurrentGeneration.Equal(normalizedB) ||
		status.PendingGeneration != nil || status.Cleanup != 0 {
		t.Fatalf("recovered controller state = %+v, want only pair B current", status)
	}
	assertDisposableRecursivePin(t, ctx, adminClient, anchorB.Cid(), pinName)
	runDisposableRepoGC(t, ctx, adminClient)
	assertDisposableBlocksPresent(t, ctx, adminClient, anchorB, filteredB, mutableB)
}

type disposableKuboDaemon struct {
	binary  string
	repo    string
	apiPort int
	cmd     *exec.Cmd
	done    chan error
	logFile *os.File
	attempt int
}

func newDisposableKuboDaemon(t *testing.T, binary string) *disposableKuboDaemon {
	t.Helper()
	ports, release := reserveDisposableLoopbackPorts(t, 3)
	defer release()

	d := &disposableKuboDaemon{
		binary:  binary,
		repo:    filepath.Join(t.TempDir(), "kubo"),
		apiPort: ports[0],
	}
	d.run(t, "init", "--profile=test")
	d.run(t, "config", "Addresses.API", fmt.Sprintf("/ip4/127.0.0.1/tcp/%d", ports[0]))
	d.run(t, "config", "Addresses.Gateway", fmt.Sprintf("/ip4/127.0.0.1/tcp/%d", ports[1]))
	d.run(t, "config", "--json", "Addresses.Swarm", fmt.Sprintf("[%q]", fmt.Sprintf("/ip4/127.0.0.1/tcp/%d", ports[2])))
	d.run(t, "config", "--json", "Bootstrap", "[]")
	d.run(t, "config", "--json", "Discovery.MDNS.Enabled", "false")
	d.run(t, "config", "--json", "Provide.Enabled", "true")
	d.run(t, "config", "Provide.Strategy", "roots")
	return d
}

func (d *disposableKuboDaemon) baseURL() string {
	return "http://127.0.0.1:" + strconv.Itoa(d.apiPort)
}

func (d *disposableKuboDaemon) isolatedEnv() []string {
	env := make([]string, 0, len(os.Environ())+1)
	for _, entry := range os.Environ() {
		if !strings.HasPrefix(entry, "IPFS_PATH=") {
			env = append(env, entry)
		}
	}
	return append(env, "IPFS_PATH="+d.repo)
}

func (d *disposableKuboDaemon) run(t *testing.T, args ...string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, d.binary, args...)
	cmd.Env = d.isolatedEnv()
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("%s %s failed: %v\n%s", d.binary, strings.Join(args, " "), err, boundedDisposableOutput(output))
	}
}

func (d *disposableKuboDaemon) start(t *testing.T) {
	t.Helper()
	if d.cmd != nil {
		t.Fatal("disposable Kubo daemon is already running")
	}
	d.attempt++
	logPath := filepath.Join(filepath.Dir(d.repo), fmt.Sprintf("kubo-daemon-%d.log", d.attempt))
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		t.Fatalf("opening Kubo daemon log: %v", err)
	}
	cmd := exec.Command(d.binary, "daemon", "--offline")
	cmd.Env = d.isolatedEnv()
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err := cmd.Start(); err != nil {
		_ = logFile.Close()
		t.Fatalf("starting disposable Kubo: %v", err)
	}
	d.cmd = cmd
	d.done = make(chan error, 1)
	d.logFile = logFile
	done := d.done
	go func() { done <- cmd.Wait() }()

	deadline := time.Now().Add(30 * time.Second)
	for {
		select {
		case err := <-d.done:
			d.cmd = nil
			d.done = nil
			_ = d.logFile.Close()
			d.logFile = nil
			t.Fatalf("disposable Kubo exited before readiness: %v\n%s", err, d.logs(t, logPath))
		default:
		}
		probe, err := kubo.New(kubo.Config{
			BaseURL: d.baseURL(), AllowUnauthenticated: true,
			RequestTimeout: time.Second,
		})
		if err != nil {
			t.Fatalf("constructing disposable Kubo readiness client: %v", err)
		}
		probeCtx, cancel := context.WithTimeout(t.Context(), time.Second)
		_, err = probe.Version(probeCtx)
		cancel()
		if err == nil {
			return
		}
		if time.Now().After(deadline) {
			_ = d.stop()
			t.Fatalf("disposable Kubo did not become ready: %v\n%s", err, d.logs(t, logPath))
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (d *disposableKuboDaemon) stop() error {
	if d.cmd == nil {
		return nil
	}
	cmd, done, logFile := d.cmd, d.done, d.logFile
	d.cmd, d.done, d.logFile = nil, nil, nil

	signalErr := cmd.Process.Signal(os.Interrupt)
	if signalErr != nil {
		_ = cmd.Process.Kill()
	}
	var waitErr error
	select {
	case waitErr = <-done:
	case <-time.After(15 * time.Second):
		killErr := cmd.Process.Kill()
		waitErr = <-done
		if killErr != nil && !errors.Is(killErr, os.ErrProcessDone) {
			return fmt.Errorf("interrupt timed out and kill failed: %w", killErr)
		}
	}
	if err := logFile.Close(); err != nil {
		return fmt.Errorf("closing daemon log: %w", err)
	}
	// A signal-derived ExitError is expected on platforms where Kubo does not
	// trap SIGINT into a zero exit status. Reaping the process is the proof that
	// the repository lock and API port are available for the restart.
	if signalErr != nil && waitErr != nil {
		return errors.Join(signalErr, waitErr)
	}
	return nil
}

func (d *disposableKuboDaemon) logs(t *testing.T, path string) string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		return "unable to read daemon log: " + err.Error()
	}
	return boundedDisposableOutput(raw)
}

func requireStableKubo042CLI(t *testing.T, binary string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, binary, "version", "--number")
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("reading Kubo CLI version: %v\n%s", err, boundedDisposableOutput(output))
	}
	version := strings.TrimSpace(string(output))
	core, _, _ := strings.Cut(strings.TrimPrefix(version, "v"), "+")
	if !strings.HasPrefix(core, "0.42.") || strings.Contains(core, "-") || strings.ContainsAny(version, " \t\r\n") {
		t.Fatalf("Kubo CLI version = %q, want stable 0.42.x", version)
	}
}

func reserveDisposableLoopbackPorts(t *testing.T, count int) ([]int, func()) {
	t.Helper()
	listeners := make([]net.Listener, 0, count)
	ports := make([]int, 0, count)
	for len(listeners) < count {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			for _, held := range listeners {
				_ = held.Close()
			}
			t.Fatalf("reserving loopback port: %v", err)
		}
		listeners = append(listeners, listener)
		ports = append(ports, listener.Addr().(*net.TCPAddr).Port)
	}
	return ports, func() {
		for _, listener := range listeners {
			_ = listener.Close()
		}
	}
}

func newDisposableKuboClient(t *testing.T, baseURL string) *kubo.Client {
	t.Helper()
	client, err := kubo.New(kubo.Config{
		BaseURL: baseURL, AllowUnauthenticated: true,
		RequestTimeout: 30 * time.Second,
		MaxStreamBytes: 64 << 20, MaxStreamItems: 1_000_000,
	})
	if err != nil {
		t.Fatalf("constructing disposable Kubo client: %v", err)
	}
	return client
}

func openDisposableController(t *testing.T, path string, client *kubo.Client, replicaID string) (*pebble.DB, *replica.Controller) {
	t.Helper()
	db, err := pebble.Open(path, &pebble.Options{})
	if err != nil {
		t.Fatalf("opening controller database: %v", err)
	}
	backend, err := replica.NewKuboBackend(replica.KuboBackendConfig{
		Client: client, PinTimeout: 30 * time.Second,
		NamedPinLimits:    kubo.ListLimits{MaxItems: 16, MaxBytes: 64 << 10},
		PinProgressLimits: kubo.ListLimits{MaxItems: 64, MaxBytes: 64 << 10},
	})
	if err != nil {
		_ = db.Close()
		t.Fatalf("constructing Kubo retention backend: %v", err)
	}
	controller, err := replica.New(replica.Config{KV: db, Backend: backend, ReplicaID: replicaID})
	if err != nil {
		_ = db.Close()
		t.Fatalf("constructing retention controller: %v", err)
	}
	return db, controller
}

func disposableRawBlock(t *testing.T, value string) blocks.Block {
	t.Helper()
	data := []byte(value)
	target, err := (cid.Prefix{Version: 1, Codec: cid.Raw, MhType: multihash.SHA2_256, MhLength: 32}).Sum(data)
	if err != nil {
		t.Fatalf("hashing disposable block: %v", err)
	}
	block, err := blocks.NewBlockWithCid(data, target)
	if err != nil {
		t.Fatalf("constructing disposable block: %v", err)
	}
	return block
}

func disposableGeneration(t *testing.T, replicaID string, generation replica.Generation) (replica.Generation, blocks.Block) {
	t.Helper()
	generation.ReplicaID = replicaID
	normalized, err := generation.Normalize()
	if err != nil {
		t.Fatalf("normalizing disposable generation: %v", err)
	}
	anchor, err := normalized.Block()
	if err != nil {
		t.Fatalf("building disposable generation anchor: %v", err)
	}
	return normalized, anchor
}

func putDisposableBlocks(t *testing.T, ctx context.Context, client *kubo.Client, input ...blocks.Block) {
	t.Helper()
	for _, block := range input {
		if _, err := client.BlockPut(ctx, block); err != nil {
			t.Fatalf("put block %s: %v", block.Cid(), err)
		}
	}
}

func runDisposableRepoGC(t *testing.T, ctx context.Context, admin *kubo.Client) kubo.RepoGCResult {
	t.Helper()
	result, err := admin.RepoGC(ctx)
	if err != nil {
		t.Fatalf("running disposable repo GC: %v", err)
	}
	return result
}

func assertDisposableBlocksPresent(t *testing.T, ctx context.Context, client *kubo.Client, input ...blocks.Block) {
	t.Helper()
	for _, block := range input {
		got, err := client.BlockGetLocal(ctx, block.Cid())
		if err != nil {
			t.Fatalf("block %s is not locally present: %v", block.Cid(), err)
		}
		if !got.Cid().Equals(block.Cid()) {
			t.Fatalf("local block CID = %s, want %s", got.Cid(), block.Cid())
		}
	}
}

func assertDisposableBlockAbsent(t *testing.T, ctx context.Context, client *kubo.Client, target cid.Cid) {
	t.Helper()
	if _, err := client.BlockGetLocal(ctx, target); !errors.Is(err, kubo.ErrNotFound) {
		if err == nil {
			t.Fatalf("block %s remains locally present after GC", target)
		}
		t.Fatalf("checking collected block %s: %v", target, err)
	}
}

func assertDisposableRecursivePin(t *testing.T, ctx context.Context, client *kubo.Client, target cid.Cid, name string) {
	t.Helper()
	info, err := client.PinStatus(ctx, target, kubo.PinTypeRecursive)
	if err != nil {
		t.Fatalf("recursive pin %s: %v", target, err)
	}
	if !info.CID.Equals(target) || info.Name != name {
		t.Fatalf("recursive pin = %+v, want CID %s name %q", info, target, name)
	}
}

func assertDisposableNotPinned(t *testing.T, ctx context.Context, client *kubo.Client, target cid.Cid) {
	t.Helper()
	if _, err := client.PinStatus(ctx, target, kubo.PinTypeRecursive); !errors.Is(err, kubo.ErrNotPinned) {
		if err == nil {
			t.Fatalf("anchor %s remains recursively pinned", target)
		}
		t.Fatalf("checking retired pin %s: %v", target, err)
	}
}

func containsDisposableCID(input []cid.Cid, target cid.Cid) bool {
	for _, candidate := range input {
		if candidate.Equals(target) {
			return true
		}
	}
	return false
}

func boundedDisposableOutput(raw []byte) string {
	const limit = 32 << 10
	if len(raw) <= limit {
		return string(raw)
	}
	return "..." + string(raw[len(raw)-limit:])
}
