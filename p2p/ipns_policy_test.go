package p2p_test

import (
	"context"
	"crypto/rand"
	"errors"
	"path/filepath"
	"slices"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/ipfs/boxo/ipns"
	blocks "github.com/ipfs/go-block-format"
	"github.com/libp2p/go-libp2p/core/crypto"

	"github.com/blobarchive/bloar/metrics"
	"github.com/blobarchive/bloar/p2p"
)

func TestMirrorPublicationPolicyKeepsPrimaryAuthoritative(t *testing.T) {
	var events []string
	mirrorFailure := errors.New("canary edge unavailable")
	primary := &recordingPublicationPolicy{name: "primary", events: &events}
	mirror := &recordingPublicationPolicy{name: "mirror", events: &events, commitErr: mirrorFailure}
	var observed []error
	policy, err := p2p.NewMirrorPublicationPolicy(primary, mirror, func(err error) {
		observed = append(observed, err)
	})
	if err != nil {
		t.Fatal(err)
	}
	block, err := p2p.NewDocumentBlock([]byte("same signed publication"))
	if err != nil {
		t.Fatal(err)
	}
	commit, err := policy.Prepare(t.Context(), block)
	if err != nil {
		t.Fatal(err)
	}
	if err := commit(t.Context(), ipns.Name{}, []byte("record")); err != nil {
		t.Fatalf("mirror failure interrupted authoritative publication: %v", err)
	}
	if want := []string{"primary.prepare", "mirror.prepare", "primary.commit", "mirror.commit"}; !slices.Equal(events, want) {
		t.Fatalf("events = %v, want %v", events, want)
	}
	if len(observed) != 1 || !errors.Is(observed[0], mirrorFailure) {
		t.Fatalf("mirror errors = %v, want [%v]", observed, mirrorFailure)
	}

	events = nil
	primaryFailure := errors.New("incumbent publication failed")
	primary.commitErr = primaryFailure
	mirror.commitErr = nil
	commit, err = policy.Prepare(t.Context(), block)
	if err != nil {
		t.Fatal(err)
	}
	if err := commit(t.Context(), ipns.Name{}, []byte("record")); !errors.Is(err, primaryFailure) {
		t.Fatalf("primary failure = %v, want %v", err, primaryFailure)
	}
	if want := []string{"primary.prepare", "mirror.prepare", "primary.commit"}; !slices.Equal(events, want) {
		t.Fatalf("events after primary failure = %v, want %v", events, want)
	}
}

func TestPublisherMetricsClassifyPostPrepareSequenceFailureAsRecordFailure(t *testing.T) {
	key, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	kvPath := filepath.Join(t.TempDir(), "kv")
	seed, err := pebble.Open(kvPath, &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := seed.Close(); err != nil {
		t.Fatal(err)
	}
	kv, err := pebble.Open(kvPath, &pebble.Options{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer kv.Close()
	policy := &recordingPublicationPolicy{name: "edge", events: new([]string)}
	mx := metrics.New()
	publisher, err := p2p.NewPublisher(p2p.PublisherConfig{Key: key, Policy: policy, KV: kv, Metrics: mx})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := publisher.Publish(t.Context(), []byte("document")); err == nil {
		t.Fatal("Publish succeeded against a read-only sequence store")
	}
	assertMetric(t, mx, "bloar_ipns_publication_stage_total",
		map[string]string{"stage": metrics.IPNSStageProvideDocument, "outcome": metrics.OutcomeOK}, 1)
	assertMetric(t, mx, "bloar_ipns_publication_stage_total",
		map[string]string{"stage": metrics.IPNSStageProvideDocument, "outcome": metrics.OutcomeError}, 0)
	assertMetric(t, mx, "bloar_ipns_publication_stage_total",
		map[string]string{"stage": metrics.IPNSStagePutRecord, "outcome": metrics.OutcomeError}, 1)
}

func TestPublisherRejectsHostWhosePeerstoreLostPrivateKey(t *testing.T) {
	host := newTestHost(t)
	host.Libp2p().Peerstore().RemovePeer(host.ID())
	kv, err := pebble.Open(filepath.Join(t.TempDir(), "kv"), &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer kv.Close()
	if _, err := p2p.NewPublisher(p2p.PublisherConfig{
		Host: host, Policy: &recordingPublicationPolicy{name: "unused", events: new([]string)}, KV: kv,
	}); err == nil {
		t.Fatal("NewPublisher accepted a Host with no private key")
	}
}

type recordingPublicationPolicy struct {
	name       string
	events     *[]string
	prepareErr error
	commitErr  error
}

func (p *recordingPublicationPolicy) Prepare(_ context.Context, block blocks.Block) (p2p.PublicationCommit, error) {
	*p.events = append(*p.events, p.name+".prepare")
	if block == nil {
		return nil, errors.New("test policy received nil block")
	}
	if p.prepareErr != nil {
		return nil, p.prepareErr
	}
	return func(context.Context, ipns.Name, []byte) error {
		*p.events = append(*p.events, p.name+".commit")
		return p.commitErr
	}, nil
}

func assertMetric(t *testing.T, mx *metrics.Metrics, family string, labels map[string]string, want float64) {
	t.Helper()
	families, err := mx.Registry().Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, candidate := range families {
		if candidate.GetName() != family {
			continue
		}
		for _, sample := range candidate.Metric {
			matched := len(sample.Label) == len(labels)
			for _, label := range sample.Label {
				if labels[label.GetName()] != label.GetValue() {
					matched = false
					break
				}
			}
			if matched {
				if got := sample.GetCounter().GetValue(); got != want {
					t.Fatalf("%s%v = %g, want %g", family, labels, got, want)
				}
				return
			}
		}
	}
	t.Fatalf("%s%v was not exported", family, labels)
}
