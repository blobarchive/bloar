package main

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"
	"time"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	mh "github.com/multiformats/go-multihash"

	"github.com/blobarchive/bloar/kubo"
	"github.com/blobarchive/bloar/p2p"
	"github.com/blobarchive/bloar/replica"
)

type recordingProvider struct {
	calls  chan []cid.Cid
	stored map[string]blocks.Block
	pinned map[string]struct{}
}

type policyClientStub struct {
	version  kubo.VersionInfo
	identity kubo.IDInfo
	enabled  bool
	strategy string
	err      error
}

func (p *policyClientStub) CheckReplicaCompatibility(context.Context) (kubo.VersionInfo, error) {
	return p.version, p.err
}

func (p *policyClientStub) CheckReplicaRuntimeCompatibility(context.Context) (kubo.VersionInfo, error) {
	return p.version, p.err
}

func (p *policyClientStub) ConfigProvideEnabled(context.Context) (bool, error) {
	return p.enabled, p.err
}

func (p *policyClientStub) ConfigProvideStrategy(context.Context) (string, error) {
	return p.strategy, p.err
}

func (p *policyClientStub) ID(context.Context) (kubo.IDInfo, error) {
	return p.identity, p.err
}

func TestKuboPolicyAuditRejectsLiveDrift(t *testing.T) {
	expected := testCommandPeerID(t, "expected-kubo")
	base := policyClientStub{
		version:  kubo.VersionInfo{Version: "0.42.0"},
		identity: kubo.IDInfo{ID: expected},
		enabled:  true, strategy: "roots",
	}
	if err := auditKuboPolicy(t.Context(), &base, expected, providerPolicyCheckRuntime); err != nil {
		t.Fatalf("safe policy rejected: %v", err)
	}

	unsafeStrategy := base
	unsafeStrategy.strategy = "all"
	if err := auditKuboPolicy(t.Context(), &unsafeStrategy, expected, providerPolicyCheckRuntime); err == nil {
		t.Fatal("unsafe provider strategy accepted")
	}
	if err := auditKuboPolicy(t.Context(), &unsafeStrategy, expected, providerPolicyCheckExternal); err != nil {
		t.Fatalf("external provider-policy mode attempted a config read: %v", err)
	}

	changedIdentity := base
	changedIdentity.identity.ID = testCommandPeerID(t, "replacement-kubo")
	if err := auditKuboPolicy(t.Context(), &changedIdentity, expected, providerPolicyCheckRuntime); err == nil {
		t.Fatal("changed Kubo PeerID accepted")
	}
}

func TestKuboPolicyAuditWithdrawsReadiness(t *testing.T) {
	expected := testCommandPeerID(t, "expected-kubo")
	client := &policyClientStub{
		version: kubo.VersionInfo{Version: "0.42.0"}, identity: kubo.IDInfo{ID: expected},
		enabled: true, strategy: "pinned",
	}
	metrics := newReplicaMetrics(nil)
	metrics.health.Set("kubo_replica", true)
	metrics.health.Set(kuboRuntimeGate, true)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		runKuboPolicyAudit(ctx, time.Millisecond, client, expected, providerPolicyCheckRuntime,
			metrics, slog.New(slog.NewTextHandler(io.Discard, nil)))
		close(done)
	}()
	deadline := time.Now().Add(time.Second)
	for {
		_, waiting := metrics.health.Ready()
		if slices.Contains(waiting, kuboRuntimeGate) {
			break
		}
		if time.Now().After(deadline) {
			cancel()
			<-done
			t.Fatal("policy drift did not withdraw readiness")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	<-done
}

func (p *recordingProvider) ProvideOnce(_ context.Context, targets []cid.Cid, limits kubo.ListLimits) error {
	if limits.MaxItems != len(targets) || limits.MaxBytes < int64(len(targets)) {
		panic("unbounded or undersized announce limits")
	}
	p.calls <- slices.Clone(targets)
	return nil
}

func (p *recordingProvider) BlockPut(_ context.Context, block blocks.Block) (kubo.BlockStat, error) {
	if p.stored == nil {
		p.stored = make(map[string]blocks.Block)
	}
	p.stored[block.Cid().KeyString()] = block
	return kubo.BlockStat{CID: block.Cid(), Size: int64(len(block.RawData()))}, nil
}

func (p *recordingProvider) PinAdd(_ context.Context, target cid.Cid, pinType kubo.PinType) error {
	if pinType != kubo.PinTypeDirect {
		panic("rendezvous block was not directly pinned")
	}
	if _, ok := p.stored[target.KeyString()]; !ok {
		panic("rendezvous block pinned before it was stored")
	}
	if p.pinned == nil {
		p.pinned = make(map[string]struct{})
	}
	p.pinned[target.KeyString()] = struct{}{}
	return nil
}

func TestAnnouncerMaterializesRendezvousBlocksBeforeTheyCanBeProvided(t *testing.T) {
	provider := &recordingProvider{calls: make(chan []cid.Cid, 1)}
	announcer := newAnnouncer(provider, "replica-1", "testnet", time.Hour, newReplicaMetrics([]string{"a", "b"}), slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err := announcer.Initialize(t.Context(), []string{"a", "b"}); err != nil {
		t.Fatal(err)
	}
	for _, head := range []string{"a", "b"} {
		key, err := p2p.RendezvousCID("testnet", head)
		if err != nil {
			t.Fatal(err)
		}
		block, stored := provider.stored[key.KeyString()]
		_, pinned := provider.pinned[key.KeyString()]
		if !stored || !pinned || !block.Cid().Equals(key) {
			t.Fatalf("rendezvous %q: stored=%t pinned=%t block=%v", head, stored, pinned, block)
		}
	}
}

func TestDialKuboPeerUsesFullyQualifiedPublicationAddress(t *testing.T) {
	targetID := testCommandPeerID(t, "publication-peer")
	address := ma.StringCast("/ip4/203.0.113.9/tcp/4001")
	wantAddress := address.String() + "/p2p/" + targetID.String()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/api/v0/swarm/connect" || request.URL.Query().Get("arg") != wantAddress {
			t.Errorf("request = %s?%s", request.URL.Path, request.URL.RawQuery)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"Strings": []string{"connect " + targetID.String() + " success"}})
	}))
	defer server.Close()
	client, err := kubo.New(kubo.Config{BaseURL: server.URL, AllowUnauthenticated: true, RequestTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if err := dialKuboPeer(t.Context(), client, testCommandPeerID(t, "self"), peer.AddrInfo{ID: targetID, Addrs: []ma.Multiaddr{address}}); err != nil {
		t.Fatal(err)
	}
}

func TestAnnouncerProvidesOnlyBoundedGenerationEntryPoints(t *testing.T) {
	provider := &recordingProvider{calls: make(chan []cid.Cid, 1)}
	mx := newReplicaMetrics([]string{"a", "b"})
	announcer := newAnnouncer(provider, "replica-1", "testnet", time.Hour, mx, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err := announcer.Initialize(t.Context(), []string{"a", "b"}); err != nil {
		t.Fatal(err)
	}
	generation := replica.Generation{
		UpdatedAt: time.Unix(10, 0),
		Heads: []replica.Head{
			{Name: "a", Root: testCommandCID("a-root"), Manifest: testCommandCID("a-manifest"), SyncedTo: 1},
			{Name: "b", Root: testCommandCID("b-root"), SyncedTo: 2},
		},
	}
	if err := announcer.Update(generation); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() { announcer.Run(ctx); close(done) }()

	var got []cid.Cid
	select {
	case got = <-provider.calls:
	case <-time.After(2 * time.Second):
		t.Fatal("announcement did not run")
	}
	cancel()
	<-done
	if len(got) != 6 { // anchor + two rendezvous keys + two roots + one manifest
		t.Fatalf("announced %d CIDs, want 6: %v", len(got), got)
	}
	generation.ReplicaID = "replica-1"
	anchor, err := generation.Block()
	if err != nil {
		t.Fatal(err)
	}
	rendezvousA, _ := p2p.RendezvousCID("testnet", "a")
	rendezvousB, _ := p2p.RendezvousCID("testnet", "b")
	for _, want := range []cid.Cid{anchor.Cid(), rendezvousA, rendezvousB, generation.Heads[0].Root, generation.Heads[0].Manifest, generation.Heads[1].Root} {
		if !containsCID(got, want) {
			t.Errorf("missing announcement %s", want)
		}
	}
}

func TestAnnouncerResumeObservationIsBoundedAndReplacesHead(t *testing.T) {
	provider := &recordingProvider{calls: make(chan []cid.Cid, 1)}
	announcer := newAnnouncer(provider, "replica-1", "testnet", time.Hour, newReplicaMetrics([]string{"a"}), slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err := announcer.Initialize(t.Context(), []string{"a"}); err != nil {
		t.Fatal(err)
	}
	if err := announcer.Update(replica.Generation{ReplicaID: "replica-1", UpdatedAt: time.Unix(1, 0), Heads: []replica.Head{{Name: "a", Root: testCommandCID("old")}}}); err != nil {
		t.Fatal(err)
	}
	if err := announcer.Update(replica.Generation{ReplicaID: "replica-1", UpdatedAt: time.Unix(2, 0), Heads: []replica.Head{{Name: "a", Root: testCommandCID("new")}}}); err != nil {
		t.Fatal(err)
	}
	latest := replica.Generation{ReplicaID: "replica-1", UpdatedAt: time.Unix(2, 0), Heads: []replica.Head{{Name: "a", Root: testCommandCID("new")}}}
	anchor, _ := latest.Block()
	rendezvous, _ := p2p.RendezvousCID("testnet", "a")
	got, _ := announcer.snapshot()
	if len(got) != 3 || !containsCID(got, latest.Heads[0].Root) || !containsCID(got, anchor.Cid()) || !containsCID(got, rendezvous) || containsCID(got, testCommandCID("old")) {
		t.Fatalf("resume targets = %v", got)
	}
}

func containsCID(values []cid.Cid, want cid.Cid) bool {
	return slices.ContainsFunc(values, func(value cid.Cid) bool { return value.Equals(want) })
}

func testCommandCID(value string) cid.Cid {
	return blocks.NewBlock([]byte(value)).Cid()
}

func testCommandPeerID(t *testing.T, value string) peer.ID {
	t.Helper()
	digest, err := mh.Sum([]byte(value), mh.SHA2_256, -1)
	if err != nil {
		t.Fatal(err)
	}
	id, err := peer.IDFromBytes(digest)
	if err != nil {
		t.Fatal(err)
	}
	return id
}
