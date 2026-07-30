package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"strings"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
	mh "github.com/multiformats/go-multihash"

	"github.com/blobarchive/bloar/census"
)

type testFactory struct {
	finder census.Finder
	prober census.Prober
	opened bool
}

func (*testFactory) RegisterFlags(*flag.FlagSet) {}

func (factory *testFactory) Open(context.Context, census.Limits) (transport, error) {
	factory.opened = true
	return transport{Finder: factory.finder, Prober: factory.prober}, nil
}

type testFinder struct{ provider peer.AddrInfo }

func (finder testFinder) FindProviders(_ context.Context, _ cid.Cid, _ int) (<-chan peer.AddrInfo, error) {
	providers := make(chan peer.AddrInfo, 1)
	providers <- finder.provider
	close(providers)
	return providers, nil
}

type testProber struct{}

func (testProber) Probe(_ context.Context, _ peer.AddrInfo, challenges census.ChallengeSet) (census.ProbeResult, error) {
	historical := make([]bool, len(challenges.Historical))
	for index := range historical {
		historical[index] = true
	}
	return census.ProbeResult{Reachable: true, Current: true, Historical: historical, Path: census.PathDirect}, nil
}

func TestRunEmitsAggregateJSONByDefault(t *testing.T) {
	rendezvous := commandTestCID(t, "rendezvous")
	current := commandTestCID(t, "current")
	historical := commandTestCID(t, "old")
	providerID := commandTestPeerID(t, "provider")
	factory := &testFactory{finder: testFinder{provider: peer.AddrInfo{ID: providerID}}, prober: testProber{}}
	var stdout, stderr bytes.Buffer
	err := run(context.Background(), []string{
		"-rendezvous", rendezvous.String(),
		"-current", current.String(),
		"-historical", historical.String(),
	}, &stdout, &stderr, factory)
	if err != nil {
		t.Fatalf("run: %v (stderr=%s)", err, stderr.String())
	}
	if !factory.opened {
		t.Fatal("transport was not opened")
	}
	var report map[string]any
	if err := json.Unmarshal(stdout.Bytes(), &report); err != nil {
		t.Fatalf("decode output: %v\n%s", err, stdout.String())
	}
	if _, exists := report["peers"]; exists {
		t.Fatalf("default JSON exposed peer details: %s", stdout.String())
	}
	lower := report["lower_bounds"].(map[string]any)
	if lower["sampled_archive"] != float64(1) {
		t.Fatalf("sampled-archive lower bound = %v, want 1", lower["sampled_archive"])
	}
}

func TestRunRawPeersIsExplicitAndJSONOnly(t *testing.T) {
	rendezvous := commandTestCID(t, "rendezvous")
	current := commandTestCID(t, "current")
	historical := commandTestCID(t, "old")
	providerID := commandTestPeerID(t, "provider")
	factory := &testFactory{finder: testFinder{provider: peer.AddrInfo{ID: providerID}}, prober: testProber{}}
	base := []string{"-rendezvous", rendezvous.String(), "-current", current.String(), "-historical", historical.String()}
	var stdout, stderr bytes.Buffer
	if err := run(context.Background(), append(base, "-raw-peers"), &stdout, &stderr, factory); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(stdout.String(), providerID.String()) {
		t.Fatalf("raw JSON lacks peer ID: %s", stdout.String())
	}

	stdout.Reset()
	stderr.Reset()
	factory.opened = false
	if err := run(context.Background(), append(base, "-raw-peers", "-format", "prometheus"), &stdout, &stderr, factory); err == nil {
		t.Fatal("raw peer details were accepted for Prometheus output")
	}
	if factory.opened {
		t.Fatal("transport opened before invalid privacy/output combination was rejected")
	}
}

func TestRunDerivesRendezvousFromNetworkAndHead(t *testing.T) {
	current := commandTestCID(t, "current")
	historical := commandTestCID(t, "old")
	factory := &testFactory{finder: testFinder{provider: peer.AddrInfo{ID: commandTestPeerID(t, "provider")}}, prober: testProber{}}
	var stdout, stderr bytes.Buffer
	if err := run(context.Background(), []string{
		"-net", "example", "-head", "all",
		"-current", current.String(), "-historical", historical.String(),
	}, &stdout, &stderr, factory); err != nil {
		t.Fatalf("derived rendezvous run: %v", err)
	}
	if err := run(context.Background(), []string{
		"-rendezvous", commandTestCID(t, "rendezvous").String(), "-net", "example", "-head", "all",
		"-current", current.String(), "-historical", historical.String(),
	}, &stdout, &stderr, factory); err == nil {
		t.Fatal("explicit and derived rendezvous inputs were accepted together")
	}
}

func commandTestCID(t *testing.T, value string) cid.Cid {
	t.Helper()
	digest, err := mh.Sum([]byte(value), mh.SHA2_256, -1)
	if err != nil {
		t.Fatal(err)
	}
	return cid.NewCidV1(cid.Raw, digest)
}

func commandTestPeerID(t *testing.T, value string) peer.ID {
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
