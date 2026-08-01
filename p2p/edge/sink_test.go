package edge_test

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/boxo/ipns"
	"github.com/ipfs/boxo/path"
	"github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/routing"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multihash"

	"github.com/blobarchive/bloar/metrics"
	"github.com/blobarchive/bloar/p2p"
	"github.com/blobarchive/bloar/p2p/edge"
	"github.com/blobarchive/bloar/p2p/pointerhint"
	"github.com/blobarchive/bloar/server"
)

var testArchiveID = mustArchiveID("1111111111111111111111111111111111111111111111111111111111111111")

func TestDefaultTimeoutBudgetIsStrictlyOrdered(t *testing.T) {
	if err := edge.ValidateTimeoutBudget(
		edge.DefaultTransactionTimeout,
		edge.DefaultRequestTimeout,
		edge.DefaultControlWriteTimeout,
	); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name                string
		transaction, client time.Duration
		server              time.Duration
		want                string
	}{
		{
			name: "transaction equals client", transaction: time.Minute, client: time.Minute,
			server: 2 * time.Minute, want: "must be shorter than request",
		},
		{
			name: "admission plus transaction equals client", transaction: time.Minute, client: 80 * time.Second,
			server: 2 * time.Minute, want: "admission timeout 20s plus transaction timeout 1m0s must be shorter",
		},
		{
			name: "client equals server", transaction: time.Minute, client: 2 * time.Minute,
			server: 2 * time.Minute, want: "must be shorter than server",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := edge.ValidateTimeoutBudget(tc.transaction, tc.client, tc.server)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("ValidateTimeoutBudget error = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestSinkOrdersProvideBeforeRecordAndPersistsContract(t *testing.T) {
	fixture := newFixture(t)
	sink := fixture.newSink(t, fixture.route, fixture.docs, fixture.stateFile)
	document, record := fixture.publication(t, 1, 1, "all")
	if err := sink.Apply(t.Context(), document, record); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if got := fixture.route.snapshot(); !slices.Equal(got, []string{"provide", "put"}) {
		t.Fatalf("routing order = %v, want [provide put]", got)
	}
	if !sink.Ready() {
		t.Fatal("sink did not become ready after the complete transaction")
	}
	info, err := os.Stat(fixture.stateFile)
	if err != nil {
		t.Fatalf("state: %v", err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("state mode = %#o, want 0600", info.Mode().Perm())
	}
	block, _ := p2p.NewDocumentBlock(document)
	if has, err := fixture.docs.Has(t.Context(), block.Cid()); err != nil || !has {
		t.Fatalf("staged document Has = %v, %v", has, err)
	}
}

func TestSinkPlansBeforeMutationAndCommitsPointersOnlyAfterRecord(t *testing.T) {
	fixture := newFixture(t)
	pointers := &recordingPointerPlanner{route: fixture.route}
	sink := fixture.newSinkWithPointers(
		t, fixture.route, fixture.route, fixture.docs, fixture.stateFile,
		edge.DefaultTransactionTimeout, nil, pointers,
	)
	document, record := fixture.publication(t, 2, 2, "all")
	if err := sink.Apply(t.Context(), document, record); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if got, want := fixture.route.snapshot(), []string{"plan", "provide", "put", "pointers"}; !slices.Equal(got, want) {
		t.Fatalf("publication order = %v, want %v", got, want)
	}

	// Anti-replay refusal occurs before pointer preflight and before every
	// primary or auxiliary mutation.
	oldDocument, oldRecord := fixture.publication(t, 1, 1, "all")
	if err := sink.Apply(t.Context(), oldDocument, oldRecord); err == nil ||
		!strings.Contains(err.Error(), "below durable floor") {
		t.Fatalf("old Apply error = %v", err)
	}
	if got, want := fixture.route.snapshot(), []string{"plan", "provide", "put", "pointers"}; !slices.Equal(got, want) {
		t.Fatalf("anti-replay refusal changed route = %v, want %v", got, want)
	}
}

func TestSinkPointerPreflightAndPostCommitFailuresKeepPrimaryTruthful(t *testing.T) {
	t.Run("preflight fails before primary mutation", func(t *testing.T) {
		fixture := newFixture(t)
		mx := metrics.New()
		pointers := &recordingPointerPlanner{
			route: fixture.route, planErr: errors.New("injected pointer preflight failure"),
		}
		sink := fixture.newSinkWithPointers(
			t, fixture.route, fixture.route, fixture.docs, fixture.stateFile,
			edge.DefaultTransactionTimeout, mx, pointers,
		)
		document, record := fixture.publication(t, 1, 1, "all")
		if err := sink.Apply(t.Context(), document, record); err == nil ||
			!strings.Contains(err.Error(), "injected pointer preflight failure") {
			t.Fatalf("Apply error = %v", err)
		}
		if got, want := fixture.route.snapshot(), []string{"plan"}; !slices.Equal(got, want) {
			t.Fatalf("preflight failure route = %v, want %v", got, want)
		}
		if _, err := os.Stat(fixture.stateFile); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("preflight failure created durable state: %v", err)
		}
		body := scrapeMetrics(t, mx)
		if want := `bloar_pointer_schedule_updates_total{outcome="error"} 1`; !strings.Contains(body, want) {
			t.Fatalf("metrics missing %q:\n%s", want, body)
		}
	})

	t.Run("put failure never commits pointers", func(t *testing.T) {
		fixture := newFixture(t)
		fixture.route.putErr = errors.New("injected record failure")
		pointers := &recordingPointerPlanner{route: fixture.route}
		sink := fixture.newSinkWithPointers(
			t, fixture.route, fixture.route, fixture.docs, fixture.stateFile,
			edge.DefaultTransactionTimeout, nil, pointers,
		)
		document, record := fixture.publication(t, 1, 1, "all")
		if err := sink.Apply(t.Context(), document, record); err == nil ||
			!strings.Contains(err.Error(), "injected record failure") {
			t.Fatalf("Apply error = %v", err)
		}
		if got, want := fixture.route.snapshot(), []string{"plan", "provide", "put"}; !slices.Equal(got, want) {
			t.Fatalf("put failure route = %v, want %v", got, want)
		}
		if pointers.commits != 0 {
			t.Fatalf("put failure committed %d pointer plans", pointers.commits)
		}
	})

	t.Run("postcommit failure remains primary success", func(t *testing.T) {
		fixture := newFixture(t)
		mx := metrics.New()
		pointers := &recordingPointerPlanner{
			route: fixture.route, commitErr: errors.New("injected pointer commit failure"),
		}
		sink := fixture.newSinkWithPointers(
			t, fixture.route, fixture.route, fixture.docs, fixture.stateFile,
			edge.DefaultTransactionTimeout, mx, pointers,
		)
		document, record := fixture.publication(t, 1, 1, "all")
		if err := sink.Apply(t.Context(), document, record); err != nil {
			t.Fatalf("Apply turned completed primary publication into error: %v", err)
		}
		if !sink.Ready() {
			t.Fatal("completed primary publication did not make sink ready")
		}
		if got, want := fixture.route.snapshot(), []string{"plan", "provide", "put", "pointers"}; !slices.Equal(got, want) {
			t.Fatalf("postcommit failure route = %v, want %v", got, want)
		}
		body := scrapeMetrics(t, mx)
		if want := `bloar_pointer_schedule_updates_total{outcome="error"} 1`; !strings.Contains(body, want) {
			t.Fatalf("metrics missing %q:\n%s", want, body)
		}
		if want := `bloar_edge_publication_stage_duration_seconds_count{outcome="ok",stage="put_record"} 1`; !strings.Contains(body, want) {
			t.Fatalf("metrics missing %q:\n%s", want, body)
		}
	})
}

func TestSinkRealPointerPreflightRejectsWrongCIDProfileBeforeDHT(t *testing.T) {
	fixture := newFixture(t)
	verified, err := pointerhint.NewVerifiedDocumentStore(fixture.docs, 0)
	if err != nil {
		t.Fatal(err)
	}
	coordinator, err := pointerhint.NewCoordinator(t.Context(), pointerhint.CoordinatorConfig{
		Provider: pointerhint.ProviderConfig{
			Router: fixture.route, Serving: verified, VerifiedDocuments: verified,
			ReprovideInterval: time.Hour, ReprovideJitter: time.Nanosecond,
		},
		MaxHeads: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := coordinator.Close(); err != nil {
			t.Error(err)
		}
	})
	pointers, err := edge.NewPointerState(coordinator, verified)
	if err != nil {
		t.Fatal(err)
	}
	sink := fixture.newSinkWithPointers(
		t, fixture.route, fixture.route, fixture.docs, fixture.stateFile,
		edge.DefaultTransactionTimeout, nil, pointers,
	)

	claim := fixture.document(1, "all")
	rawRoot, err := p2p.NewDocumentBlock([]byte("raw root is not dag-cbor"))
	if err != nil {
		t.Fatal(err)
	}
	claim.Heads[0].Root = rawRoot.Cid().String()
	document, record := fixture.encode(t, claim, 1)
	if err := sink.Apply(t.Context(), document, record); err == nil ||
		!strings.Contains(err.Error(), "is not a dag-cbor CID") {
		t.Fatalf("Apply error = %v, want strict pointer CID profile refusal", err)
	}
	if got := fixture.route.snapshot(); len(got) != 0 {
		t.Fatalf("wrong pointer CID profile reached DHT: %v", got)
	}
	if _, err := os.Stat(fixture.stateFile); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("wrong pointer CID profile created durable state: %v", err)
	}
}

func TestSinkRestoreReinstallsPointerScheduleAfterPrimaryRecord(t *testing.T) {
	fixture := newFixture(t)
	document, record := fixture.publication(t, 1, 1, "all")
	first := fixture.newSink(t, fixture.route, fixture.docs, fixture.stateFile)
	if err := first.Apply(t.Context(), document, record); err != nil {
		t.Fatal(err)
	}

	restoredRoute := newRoute()
	pointers := &recordingPointerPlanner{route: restoredRoute}
	second := fixture.newSinkWithPointers(
		t, restoredRoute, restoredRoute, memDocs(t), fixture.stateFile,
		edge.DefaultTransactionTimeout, nil, pointers,
	)
	// NewSink preflights durable bytes without mutating the network; isolate
	// Restore's ordering below.
	restoredRoute.reset()
	pointers.reset()
	if present, err := second.Restore(t.Context()); err != nil || !present {
		t.Fatalf("Restore = %v, %v, want true,nil", present, err)
	}
	if got, want := restoredRoute.snapshot(), []string{"plan", "provide", "put", "pointers"}; !slices.Equal(got, want) {
		t.Fatalf("restore order = %v, want %v", got, want)
	}
	if !second.Ready() || pointers.plans != 1 || pointers.commits != 1 {
		t.Fatalf("restored state ready=%v plans=%d commits=%d, want true/1/1",
			second.Ready(), pointers.plans, pointers.commits)
	}
}

func TestSinkCrashAfterDurableStageRestoresWithoutWriterRepublish(t *testing.T) {
	fixture := newFixture(t)
	fixture.route.putErr = errors.New("DHT put interrupted")
	first := fixture.newSink(t, fixture.route, fixture.docs, fixture.stateFile)
	document, record := fixture.publication(t, 7, 3, "all")
	if err := first.Apply(t.Context(), document, record); err == nil ||
		!strings.Contains(err.Error(), "putting IPNS record") {
		t.Fatalf("Apply error = %v, want post-persist DHT failure", err)
	}
	if first.Ready() {
		t.Fatal("failed transaction became ready")
	}
	if _, err := os.Stat(fixture.stateFile); err != nil {
		t.Fatalf("post-crash durable state: %v", err)
	}

	// A new edge process with an empty in-memory document cache, while the
	// private writer remains unchanged and sends nothing.
	restoredDocs := memDocs(t)
	recoveredRoute := newRoute()
	second := fixture.newSink(t, recoveredRoute, restoredDocs, fixture.stateFile)
	if second.Ready() {
		t.Fatal("restart was ready before durable restoration")
	}
	if present, err := second.Restore(t.Context()); err != nil || !present {
		t.Fatalf("Restore = %v, %v, want true,nil", present, err)
	}
	if !second.Ready() {
		t.Fatal("restart did not become ready after restoration")
	}
	if got := recoveredRoute.snapshot(); !slices.Equal(got, []string{"provide", "put"}) {
		t.Fatalf("restore order = %v, want [provide put]", got)
	}
	block, _ := p2p.NewDocumentBlock(document)
	if has, err := restoredDocs.Has(t.Context(), block.Cid()); err != nil || !has {
		t.Fatalf("restored document Has = %v, %v", has, err)
	}

	// The exact same signed bytes remain safely replayable after a transient
	// failure, but a lower transport sequence cannot cross the durable floor.
	if err := second.Apply(t.Context(), document, record); err != nil {
		t.Fatalf("idempotent replay: %v", err)
	}
	oldDocument, oldRecord := fixture.publication(t, 6, 2, "all")
	if err := second.Apply(t.Context(), oldDocument, oldRecord); err == nil ||
		!strings.Contains(err.Error(), "below durable floor") {
		t.Fatalf("lower replay error = %v", err)
	}
}

func TestSinkFailsClosedOnAuthorityCatalogAndRevision(t *testing.T) {
	tests := []struct {
		name string
		edit func(*fixture, *server.Doc) ed25519.PrivateKey
		want string
	}{
		{
			name: "wrong document signer",
			edit: func(_ *fixture, _ *server.Doc) ed25519.PrivateKey {
				_, other, _ := ed25519.GenerateKey(rand.Reader)
				return other
			},
			want: "unconfigured authority",
		},
		{
			name: "unconfigured head",
			edit: func(_ *fixture, doc *server.Doc) ed25519.PrivateKey {
				doc.Heads[0].Name = "other"
				return nil
			},
			want: "unconfigured head",
		},
		{
			name: "wrong effective kind",
			edit: func(_ *fixture, doc *server.Doc) ed25519.PrivateKey {
				doc.Heads[0].Kind = server.UnfinalizedMutable
				return nil
			},
			want: "validating publication contract",
		},
		{
			name: "edge address absent",
			edit: func(f *fixture, doc *server.Doc) ed25519.PrivateKey {
				doc.Multiaddrs = []string{"/ip4/203.0.113.2/tcp/4005/p2p/" + f.otherPeer.String()}
				return nil
			},
			want: "want only edge peer",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fixture := newFixture(t)
			sink := fixture.newSink(t, fixture.route, fixture.docs, fixture.stateFile)
			doc := fixture.document(1, "all")
			signingKey := tc.edit(fixture, &doc)
			if signingKey == nil {
				signingKey = fixture.documentKey
			}
			signDocument(t, &doc, signingKey)
			document, err := json.Marshal(doc)
			if err != nil {
				t.Fatal(err)
			}
			record := fixture.record(t, document, 1)
			if err := sink.Apply(t.Context(), document, record); err == nil ||
				!strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Apply error = %v, want %q", err, tc.want)
			}
		})
	}

	fixture := newFixture(t)
	sink := fixture.newSink(t, fixture.route, fixture.docs, fixture.stateFile)
	document2, record2 := fixture.publication(t, 2, 2, "all")
	if err := sink.Apply(t.Context(), document2, record2); err != nil {
		t.Fatal(err)
	}
	document1, record3 := fixture.publication(t, 3, 1, "all")
	if err := sink.Apply(t.Context(), document1, record3); err == nil ||
		!strings.Contains(err.Error(), "revision 1 is below durable floor 2") {
		t.Fatalf("revision regression error = %v", err)
	}
	sameRevision := fixture.document(2, "all")
	sameRevision.UpdatedAt = "2000-01-01T00:00:00Z"
	documentChanged, record4 := fixture.encode(t, sameRevision, 4)
	if err := sink.Apply(t.Context(), documentChanged, record4); err == nil ||
		!strings.Contains(err.Error(), "revision 2 changes CID") {
		t.Fatalf("same-revision rewrite error = %v", err)
	}
}

func TestSinkAllowsOptionalMutableWithdrawalButNotRequiredFinalizedOmission(t *testing.T) {
	fixture := newFixture(t)
	sink, err := edge.NewSink(edge.SinkConfig{
		Name: fixture.name, DocumentPublicKey: fixture.documentKey.Public().(ed25519.PublicKey),
		Network: "mainnet", ArchiveID: testArchiveID, EdgePeer: fixture.edgePeer,
		Documents: fixture.docs, Provider: fixture.route, Routing: fixture.route, Notifier: noopNotifier{},
		Pointers:  noopPointerPlanner{},
		StateFile: fixture.stateFile,
		AllowedHeads: map[string]edge.HeadPolicy{
			"all":         {Kind: server.FinalizedMonotonic, Required: true},
			"unfinalized": {Kind: server.UnfinalizedMutable, HandoffHead: "all", Required: false},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	withMutable := fixture.document(1, "all")
	withMutable.Heads = append(withMutable.Heads, mutableEntry(t, withMutable.Heads[0]))
	document1, record1 := fixture.encode(t, withMutable, 1)
	if err := sink.Apply(t.Context(), document1, record1); err != nil {
		t.Fatalf("initial mutable publication: %v", err)
	}

	// Quarantine legitimately withdraws the optional mutable entry while the
	// required finalized authority remains published.
	withoutMutable := fixture.document(2, "all")
	document2, record2 := fixture.encode(t, withoutMutable, 2)
	if err := sink.Apply(t.Context(), document2, record2); err != nil {
		t.Fatalf("optional mutable withdrawal: %v", err)
	}

	missingFinalized := fixture.document(3, "all")
	missingFinalized.Heads = nil
	document3, record3 := fixture.encode(t, missingFinalized, 3)
	if err := sink.Apply(t.Context(), document3, record3); err == nil ||
		!strings.Contains(err.Error(), `omits required head "all"`) {
		t.Fatalf("required finalized omission error = %v", err)
	}
}

func TestUnixClientPublisherHasNoHostAndEdgeAcceptsSignedRecord(t *testing.T) {
	fixture := newFixture(t)
	sink := fixture.newSink(t, fixture.route, fixture.docs, fixture.stateFile)
	socket := serveUnixSink(t, sink)
	policy, err := edge.NewClientPolicy(edge.ClientConfig{Socket: socket})
	if err != nil {
		t.Fatal(err)
	}
	kv, err := pebble.Open(filepath.Join(t.TempDir(), "kv"), &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer kv.Close()
	publisher, err := p2p.NewPublisher(p2p.PublisherConfig{
		Key: fixture.ipnsKey, Policy: policy, KV: kv, Republish: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	doc := fixture.document(1, "all")
	signDocument(t, &doc, fixture.documentKey)
	document, _ := json.Marshal(doc)
	if _, seq, err := publisher.Publish(t.Context(), document); err != nil || seq != 1 {
		t.Fatalf("hostless Publish = seq %d, %v", seq, err)
	}
	if got := fixture.route.snapshot(); !slices.Equal(got, []string{"provide", "put"}) {
		t.Fatalf("edge route = %v", got)
	}
}

func TestUnixClientCallerCancellationAfterHandoffWaitsForLateCompletion(t *testing.T) {
	fixture := newFixture(t)
	provider := newGatedProvider()
	route := newRoute()
	mx := metrics.New()
	transactionTimeout := 500 * time.Millisecond
	sink := fixture.newSinkWith(
		t, provider, route, fixture.docs, fixture.stateFile, transactionTimeout, mx,
	)
	socket := serveUnixSink(t, sink)
	policy, err := edge.NewClientPolicy(edge.ClientConfig{
		Socket:             socket,
		TransactionTimeout: transactionTimeout,
		RequestTimeout:     2 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	kv, err := pebble.Open(filepath.Join(t.TempDir(), "kv"), &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer kv.Close()
	publisher, err := p2p.NewPublisher(p2p.PublisherConfig{
		Key: fixture.ipnsKey, Policy: policy, KV: kv, Republish: time.Hour, Metrics: mx,
	})
	if err != nil {
		t.Fatal(err)
	}
	doc := fixture.document(1, "all")
	signDocument(t, &doc, fixture.documentKey)
	document, _ := json.Marshal(doc)
	ctx, cancel := context.WithCancel(t.Context())
	result := make(chan error, 1)
	go func() {
		_, _, err := publisher.Publish(ctx, document)
		result <- err
	}()
	<-provider.started

	// Once the signed request crosses the private socket it is a bounded commit,
	// not an abortable read. Canceling the caller must not make the writer book
	// an error while the edge is still capable of completing it.
	cancel()
	select {
	case err := <-result:
		t.Fatalf("Publish returned on caller cancellation before edge outcome: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	close(provider.release)
	if err := <-result; err != nil {
		t.Fatalf("Publish after late edge completion: %v", err)
	}
	if got := route.snapshot(); !slices.Equal(got, []string{"put"}) {
		t.Fatalf("value route = %v, want [put]", got)
	}
	body := scrapeMetrics(t, mx)
	for _, want := range []string{
		`bloar_ipns_publication_stage_total{outcome="ok",stage="provide_document"} 1`,
		`bloar_ipns_publication_stage_total{outcome="ok",stage="put_record"} 1`,
		`bloar_edge_publication_stage_duration_seconds_count{outcome="ok",stage="provide_document"} 1`,
		`bloar_edge_publication_stage_duration_seconds_count{outcome="ok",stage="put_record"} 1`,
		`bloar_edge_publication_transactions_total{operation="publish",outcome="ok",stage="complete"} 1`,
		`bloar_edge_publication_transaction_duration_seconds_count{operation="publish",outcome="ok"} 1`,
		`bloar_edge_publication_wait_duration_seconds_count{operation="publish",outcome="ok"} 1`,
		`bloar_edge_publication_transactions_total{operation="publish",outcome="canceled",stage="admission"} 0`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics missing %q:\n%s", want, body)
		}
	}
}

func TestUnixClientReceivesStructuredStageBeforeOuterDeadline(t *testing.T) {
	fixture := newFixture(t)
	provider := &deadlineProvider{started: make(chan struct{})}
	route := newRoute()
	mx := metrics.New()
	transactionTimeout := 40 * time.Millisecond
	sink := fixture.newSinkWith(
		t, provider, route, fixture.docs, fixture.stateFile, transactionTimeout, mx,
	)
	socket := serveUnixSink(t, sink)
	policy, err := edge.NewClientPolicy(edge.ClientConfig{
		Socket:             socket,
		TransactionTimeout: transactionTimeout,
		RequestTimeout:     200 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	document, record := fixture.publication(t, 1, 1, "all")
	block, err := p2p.NewDocumentBlock(document)
	if err != nil {
		t.Fatal(err)
	}
	commit, err := policy.Prepare(t.Context(), block)
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	err = commit(t.Context(), fixture.name, record)
	if err == nil {
		t.Fatal("Commit succeeded despite edge transaction deadline")
	}
	var stageErr *p2p.PublicationStageError
	if !errors.As(err, &stageErr) || stageErr.Stage != p2p.PublicationStageProvideDocument {
		t.Fatalf("Commit error = %v, want provide_document stage", err)
	}
	if !strings.Contains(err.Error(), "HTTP 422") || !strings.Contains(err.Error(), "context deadline exceeded") {
		t.Fatalf("Commit error = %v, want structured edge deadline", err)
	}
	if elapsed := time.Since(started); elapsed >= 150*time.Millisecond {
		t.Fatalf("structured edge deadline arrived after %s, too close to 200ms client budget", elapsed)
	}
	if _, err := os.Stat(fixture.stateFile); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("provide timeout created durable state: %v", err)
	}
	if got := route.snapshot(); len(got) != 0 {
		t.Fatalf("record route ran after provide timeout: %v", got)
	}
	body := scrapeMetrics(t, mx)
	if want := `bloar_edge_publication_stage_duration_seconds_count{outcome="timeout",stage="provide_document"} 1`; !strings.Contains(body, want) {
		t.Fatalf("metrics missing %q:\n%s", want, body)
	}
}

func TestUnixClientAndEdgeRefuseTransactionBudgetDriftBeforeMutation(t *testing.T) {
	fixture := newFixture(t)
	mx := metrics.New()
	sink := fixture.newSinkWith(
		t, fixture.route, fixture.route, fixture.docs, fixture.stateFile, 500*time.Millisecond, mx,
	)
	socket := serveUnixSink(t, sink)
	policy, err := edge.NewClientPolicy(edge.ClientConfig{
		Socket:             socket,
		TransactionTimeout: 400 * time.Millisecond,
		RequestTimeout:     time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	document, record := fixture.publication(t, 1, 1, "all")
	block, err := p2p.NewDocumentBlock(document)
	if err != nil {
		t.Fatal(err)
	}
	commit, err := policy.Prepare(t.Context(), block)
	if err != nil {
		t.Fatal(err)
	}
	if err := commit(t.Context(), fixture.name, record); err == nil ||
		!strings.Contains(err.Error(), "HTTP 409") ||
		!strings.Contains(err.Error(), "transaction timeout must be exactly 500ms") {
		t.Fatalf("Commit error = %v, want pre-mutation timeout mismatch", err)
	}
	if got := fixture.route.snapshot(); len(got) != 0 {
		t.Fatalf("budget mismatch reached DHT: %v", got)
	}
	if _, err := os.Stat(fixture.stateFile); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("budget mismatch created durable state: %v", err)
	}
	body := scrapeMetrics(t, mx)
	for _, stage := range []string{
		metrics.EdgePublicationStageAdmission,
		metrics.EdgePublicationStageValidation,
		metrics.EdgePublicationStageProvideDocument,
		metrics.EdgePublicationStagePersistState,
		metrics.EdgePublicationStagePutRecord,
		metrics.EdgePublicationStageComplete,
	} {
		series := `bloar_edge_publication_transactions_total{operation="publish",outcome="error",stage="` + stage + `"} 0`
		if !strings.Contains(body, series) {
			t.Fatalf("pre-transaction rejection changed edge transaction telemetry; missing %q:\n%s", series, body)
		}
	}
}

func TestSinkPutDeadlinePersistsFloorAndColdRestoreRetriesIdempotently(t *testing.T) {
	fixture := newFixture(t)
	route := newDeadlinePutRoute()
	transactionTimeout := 40 * time.Millisecond
	mx := metrics.New()
	var logBuffer bytes.Buffer
	sink := fixture.newSinkWithDiagnostics(
		t,
		route,
		route,
		fixture.docs,
		fixture.stateFile,
		transactionTimeout,
		mx,
		noopPointerPlanner{},
		slog.New(slog.NewTextHandler(&logBuffer, &slog.HandlerOptions{Level: slog.LevelDebug})),
		func() int { return 17 },
	)
	document, record := fixture.publication(t, 7, 3, "all")
	err := sink.Apply(t.Context(), document, record)
	var stageErr *p2p.PublicationStageError
	if !errors.As(err, &stageErr) || stageErr.Stage != p2p.PublicationStagePutRecord ||
		!errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Apply error = %v, want put_record deadline", err)
	}
	if sink.Ready() {
		t.Fatal("deadline transaction became ready")
	}
	if _, err := os.Stat(fixture.stateFile); err != nil {
		t.Fatalf("post-deadline durable state: %v", err)
	}
	block, err := p2p.NewDocumentBlock(document)
	if err != nil {
		t.Fatal(err)
	}
	logged := logBuffer.String()
	for _, want := range []string{
		`msg="edge publication transaction failed"`,
		"operation=publish",
		"stage=put_record",
		"outcome=timeout",
		"http_status=422",
		"transaction_timeout=40ms",
		"durable_floor_updated=true",
		"budget_remaining_before_provide=",
		"provide_elapsed=",
		"budget_remaining_before_put=",
		"put_elapsed=",
		"cid=" + block.Cid().String(),
		"sequence=7",
		"revision=3",
		"durable_sequence=7",
		"durable_revision=3",
		"routing_table_peers=17",
	} {
		if !strings.Contains(logged, want) {
			t.Fatalf("diagnostic log missing %q:\n%s", want, logged)
		}
	}
	for _, secret := range []string{string(document), hex.EncodeToString(record)} {
		if strings.Contains(logged, secret) {
			t.Fatalf("diagnostic log contains submitted publication bytes %q", secret)
		}
	}
	body := scrapeMetrics(t, mx)
	for _, want := range []string{
		`bloar_edge_publication_transactions_total{operation="publish",outcome="timeout",stage="put_record"} 1`,
		`bloar_edge_publication_transaction_duration_seconds_count{operation="publish",outcome="timeout"} 1`,
		`bloar_edge_publication_wait_duration_seconds_count{operation="publish",outcome="ok"} 1`,
		`bloar_edge_publication_stage_duration_seconds_count{outcome="ok",stage="provide_document"} 1`,
		`bloar_edge_publication_stage_duration_seconds_count{outcome="timeout",stage="put_record"} 1`,
		`bloar_edge_dht_routing_table_peers 17`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics missing %q:\n%s", want, body)
		}
	}
	if !strings.Contains(body, "bloar_edge_dht_routing_table_sample_timestamp_seconds ") ||
		strings.Contains(body, "bloar_edge_dht_routing_table_sample_timestamp_seconds 0\n") {
		t.Fatalf("routing-table sample timestamp is absent or zero:\n%s", body)
	}

	// A cold process has only the pre-PutValue durable floor. Restore repeats
	// Provide then PutValue, and the exact original request remains idempotent.
	recoveredRoute := newRoute()
	restarted := fixture.newSinkWith(
		t, recoveredRoute, recoveredRoute, memDocs(t), fixture.stateFile,
		transactionTimeout, nil,
	)
	if present, err := restarted.Restore(t.Context()); err != nil || !present {
		t.Fatalf("Restore = %v, %v, want true,nil", present, err)
	}
	if err := restarted.Apply(t.Context(), document, record); err != nil {
		t.Fatalf("idempotent post-restore retry: %v", err)
	}
	if got := recoveredRoute.snapshot(); !slices.Equal(got, []string{"provide", "put", "provide", "put"}) {
		t.Fatalf("recovery route = %v, want two ordered idempotent transactions", got)
	}
}

func TestSinkPublicationStagesUseMinPreservingDialTimeoutWithoutChangingTransactionDeadline(t *testing.T) {
	fixture := newFixture(t)
	route := &contextInspectRoute{eventRoute: newRoute()}
	const transactionTimeout = 500 * time.Millisecond
	sink := fixture.newSinkWith(
		t, route, route, fixture.docs, fixture.stateFile, transactionTimeout, nil,
	)
	if got := network.GetDialPeerTimeout(t.Context()); got != network.DialPeerTimeout {
		t.Fatalf("unrelated dial timeout before Apply = %s, want default %s", got, network.DialPeerTimeout)
	}
	document, record := fixture.publication(t, 1, 1, "all")
	if err := sink.Apply(t.Context(), document, record); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if got := route.provideDialTimeouts[0]; got != 15*time.Second {
		t.Fatalf("Provide dial timeout = %s, want 15s", got)
	}
	if got := route.putDialTimeouts[0]; got != 15*time.Second {
		t.Fatalf("PutValue dial timeout = %s, want 15s", got)
	}
	if route.provideDeadlines[0].IsZero() || !route.provideDeadlines[0].Equal(route.putDeadlines[0]) {
		t.Fatalf("work deadline changed across publication stages: provide=%s put=%s",
			route.provideDeadlines[0], route.putDeadlines[0])
	}
	if got := route.provideBudgetRemaining[0]; got < 400*time.Millisecond || got > transactionTimeout {
		t.Fatalf("Provide work budget at entry = %s, want approximately fresh %s", got, transactionTimeout)
	}

	shortDialCtx := network.WithDialPeerTimeout(t.Context(), 5*time.Second)
	document2, record2 := fixture.publication(t, 2, 2, "all")
	if err := sink.Apply(shortDialCtx, document2, record2); err != nil {
		t.Fatalf("Apply with inherited shorter dial timeout: %v", err)
	}
	if got := route.provideDialTimeouts[1]; got != 5*time.Second {
		t.Fatalf("Provide inherited dial timeout = %s, want 5s", got)
	}
	if got := route.putDialTimeouts[1]; got != 5*time.Second {
		t.Fatalf("PutValue inherited dial timeout = %s, want 5s", got)
	}
	if route.provideDeadlines[1].IsZero() || !route.provideDeadlines[1].Equal(route.putDeadlines[1]) {
		t.Fatalf("work deadline changed with inherited dial timeout: provide=%s put=%s",
			route.provideDeadlines[1], route.putDeadlines[1])
	}
	if got := network.GetDialPeerTimeout(shortDialCtx); got != 5*time.Second {
		t.Fatalf("caller dial timeout after Apply = %s, want unchanged 5s", got)
	}
	if got := network.GetDialPeerTimeout(t.Context()); got != network.DialPeerTimeout {
		t.Fatalf("unrelated dial timeout after Apply = %s, want default %s", got, network.DialPeerTimeout)
	}
}

func TestSinkRestoreTimeoutEmitsOperationSpecificDiagnostics(t *testing.T) {
	fixture := newFixture(t)
	document, record := fixture.publication(t, 9, 4, "all")
	initial := fixture.newSink(t, fixture.route, fixture.docs, fixture.stateFile)
	if err := initial.Apply(t.Context(), document, record); err != nil {
		t.Fatalf("initial Apply: %v", err)
	}

	route := newOpaqueDeadlinePutRoute()
	mx := metrics.New()
	var logBuffer bytes.Buffer
	restarted := fixture.newSinkWithDiagnostics(
		t,
		route,
		route,
		memDocs(t),
		fixture.stateFile,
		40*time.Millisecond,
		mx,
		noopPointerPlanner{},
		slog.New(slog.NewTextHandler(&logBuffer, &slog.HandlerOptions{Level: slog.LevelDebug})),
		func() int { return 23 },
	)
	present, err := restarted.Restore(t.Context())
	if !present {
		t.Fatal("Restore reported no durable publication")
	}
	var stageErr *p2p.PublicationStageError
	if !errors.As(err, &stageErr) || stageErr.Stage != p2p.PublicationStagePutRecord {
		t.Fatalf("Restore error = %v, want put_record stage", err)
	}
	if restarted.Ready() {
		t.Fatal("timed-out restore became ready")
	}

	logged := logBuffer.String()
	for _, want := range []string{
		`msg="edge publication transaction failed"`,
		"operation=restore",
		"stage=put_record",
		"outcome=timeout",
		"transaction_timeout=40ms",
		"durable_floor_updated=false",
		"sequence=9",
		"revision=4",
		"durable_sequence=9",
		"routing_table_peers=23",
		"put_closest_peer_queries=0",
		"put_closest_peer_responses=0",
		"put_closest_peer_errors=0",
		"put_value_rpc_attempts=0",
	} {
		if !strings.Contains(logged, want) {
			t.Fatalf("restore diagnostic log missing %q:\n%s", want, logged)
		}
	}
	if strings.Contains(logged, "http_status=") {
		t.Fatalf("restore diagnostic log incorrectly contains an HTTP status:\n%s", logged)
	}
	body := scrapeMetrics(t, mx)
	for _, want := range []string{
		`bloar_edge_publication_transactions_total{operation="restore",outcome="timeout",stage="put_record"} 1`,
		`bloar_edge_publication_transaction_duration_seconds_count{operation="restore",outcome="timeout"} 1`,
		`bloar_edge_publication_wait_duration_seconds_count{operation="restore",outcome="ok"} 1`,
		`bloar_edge_publication_stage_duration_seconds_count{outcome="timeout",stage="put_record"} 1`,
		`bloar_edge_dht_routing_table_peers 23`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("restore metrics missing %q:\n%s", want, body)
		}
	}
}

func TestSinkPutValueQueryDiagnosticsSeparateLookupFromFanout(t *testing.T) {
	fixture := newFixture(t)
	route := newQueryEventDeadlinePutRoute()
	mx := metrics.New()
	var logBuffer bytes.Buffer
	sink := fixture.newSinkWithDiagnostics(
		t,
		route,
		route,
		fixture.docs,
		fixture.stateFile,
		time.Second,
		mx,
		noopPointerPlanner{},
		slog.New(slog.NewTextHandler(&logBuffer, &slog.HandlerOptions{Level: slog.LevelDebug})),
		func() int { return 29 },
	)
	document, record := fixture.publication(t, 1, 1, "all")
	err := sink.Apply(t.Context(), document, record)
	var stageErr *p2p.PublicationStageError
	if !errors.As(err, &stageErr) || stageErr.Stage != p2p.PublicationStagePutRecord {
		t.Fatalf("Apply error = %v, want put_record stage", err)
	}

	logged := logBuffer.String()
	for _, want := range []string{
		"provide_closest_peer_queries=7",
		"provide_closest_peer_responses=6",
		"provide_closest_peer_errors=2",
		"provide_peer_dials=1",
		"provide_closer_peers_returned=18",
		"provide_max_closer_peers_per_response=3",
		"provide_query_last_event_after=",
		"provide_lookup_requests=25",
		"provide_lookup_responses=1",
		"provide_lookup_terminations=4",
		"provide_lookup_heard_transitions=4",
		"provide_lookup_waiting_transitions=2",
		"provide_lookup_queried_transitions=1",
		"provide_lookup_unreachable_transitions=0",
		"provide_lookup_termination_reason=completed",
		"provide_lookup_termination_after=",
		"provide_lookup_waiting_at_termination=1",
		"provide_lookup_post_termination_wait=",
		"put_closest_peer_queries=24",
		"put_closest_peer_responses=18",
		"put_closest_peer_errors=3",
		"put_peer_dials=4",
		"put_closer_peers_returned=36",
		"put_max_closer_peers_per_response=2",
		"put_value_rpc_attempts=20",
		"put_value_rpc_phase_after=",
		"put_query_last_event_after=",
		"put_lookup_requests=25",
		"put_lookup_responses=1",
		"put_lookup_terminations=4",
		"put_lookup_heard_transitions=5",
		"put_lookup_waiting_transitions=3",
		"put_lookup_queried_transitions=1",
		"put_lookup_unreachable_transitions=1",
		"put_lookup_termination_reason=completed",
		"put_lookup_termination_after=",
		"put_lookup_waiting_at_termination=1",
		"put_lookup_post_termination_wait=",
	} {
		if !strings.Contains(logged, want) {
			t.Fatalf("query-event diagnostic log missing %q:\n%s", want, logged)
		}
	}
	if !route.contextLiveAfterTermination {
		t.Fatal("lookup termination canceled the routing observer context before PutValue returned")
	}
	if strings.Contains(logged, "put_lookup_post_termination_wait=0s") {
		t.Fatalf("delayed routing return recorded zero post-termination wait:\n%s", logged)
	}
	for _, forbidden := range []string{
		"sensitive-query-peer",
		"sensitive-query-error",
		"sensitive-query-address.example",
		"sensitive-lookup-node",
		"sensitive-lookup-key",
		"sensitive-lookup-cause",
		"sensitive-lookup-source",
		"sensitive-lookup-peer",
	} {
		if strings.Contains(logged, forbidden) {
			t.Fatalf("routing event detail %q reached the diagnostic log:\n%s", forbidden, logged)
		}
	}

	body := scrapeMetrics(t, mx)
	for _, want := range []string{
		`bloar_edge_dht_query_events_total{event="sending_query",stage="provide_document"} 7`,
		`bloar_edge_dht_query_events_total{event="peer_response",stage="provide_document"} 6`,
		`bloar_edge_dht_query_events_total{event="query_error",stage="provide_document"} 2`,
		`bloar_edge_dht_query_events_total{event="dialing_peer",stage="provide_document"} 1`,
		`bloar_edge_dht_query_events_total{event="value_rpc",stage="provide_document"} 0`,
		`bloar_edge_dht_query_events_total{event="sending_query",stage="put_record"} 24`,
		`bloar_edge_dht_query_events_total{event="peer_response",stage="put_record"} 18`,
		`bloar_edge_dht_query_events_total{event="query_error",stage="put_record"} 3`,
		`bloar_edge_dht_query_events_total{event="dialing_peer",stage="put_record"} 4`,
		`bloar_edge_dht_query_events_total{event="value_rpc",stage="put_record"} 20`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("query-event metrics missing %q:\n%s", want, body)
		}
	}
	for _, stage := range []string{metrics.IPNSStageProvideDocument, metrics.IPNSStagePutRecord} {
		for _, reason := range []string{
			metrics.EdgeDHTLookupTerminationStopped,
			metrics.EdgeDHTLookupTerminationCancelled,
			metrics.EdgeDHTLookupTerminationStarvation,
			metrics.EdgeDHTLookupTerminationCompleted,
		} {
			for _, want := range []string{
				fmt.Sprintf(`bloar_edge_dht_lookup_terminations_total{reason=%q,stage=%q} 1`, reason, stage),
				fmt.Sprintf(`bloar_edge_dht_lookup_waiting_at_last_termination{reason=%q,stage=%q} 1`, reason, stage),
			} {
				if !strings.Contains(body, want) {
					t.Fatalf("lookup-termination metrics missing %q:\n%s", want, body)
				}
			}
			timestamp := fmt.Sprintf(`bloar_edge_dht_lookup_termination_last_timestamp_seconds{reason=%q,stage=%q}`, reason, stage)
			if !strings.Contains(body, timestamp+" ") || strings.Contains(body, timestamp+" 0\n") {
				t.Fatalf("lookup-termination timestamp was not stamped for %s/%s:\n%s", stage, reason, body)
			}
		}
	}
	for _, forbidden := range []string{
		"sensitive-query-peer",
		"sensitive-query-error",
		"sensitive-query-address.example",
		"sensitive-lookup-node",
		"sensitive-lookup-key",
		"sensitive-lookup-cause",
		"sensitive-lookup-source",
		"sensitive-lookup-peer",
	} {
		if strings.Contains(body, forbidden) {
			t.Fatalf("routing event detail %q reached the metrics scrape:\n%s", forbidden, body)
		}
	}
}

func TestSinkDiagnosticsExposePublishWaitingBehindRestore(t *testing.T) {
	fixture := newFixture(t)
	document1, record1 := fixture.publication(t, 1, 1, "all")
	initial := fixture.newSink(t, fixture.route, fixture.docs, fixture.stateFile)
	if err := initial.Apply(t.Context(), document1, record1); err != nil {
		t.Fatalf("initial Apply: %v", err)
	}

	provider := newGatedProvider()
	route := newRoute()
	mx := metrics.New()
	var logBuffer bytes.Buffer
	sink := fixture.newSinkWithDiagnostics(
		t,
		provider,
		route,
		memDocs(t),
		fixture.stateFile,
		500*time.Millisecond,
		mx,
		noopPointerPlanner{},
		slog.New(slog.NewTextHandler(&logBuffer, &slog.HandlerOptions{Level: slog.LevelDebug})),
		func() int { return 31 },
	)
	restoreResult := make(chan error, 1)
	go func() {
		_, err := sink.Restore(t.Context())
		restoreResult <- err
	}()
	<-provider.started

	document2, record2 := fixture.publication(t, 2, 2, "all")
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	err := sink.Apply(canceled, document2, record2)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("queued Apply error = %v, want canceled", err)
	}

	close(provider.release)
	if err := <-restoreResult; err != nil {
		t.Fatalf("Restore after releasing provider: %v", err)
	}
	if got := route.snapshot(); !slices.Equal(got, []string{"put"}) {
		t.Fatalf("route events = %v, want only restore put", got)
	}

	logged := logBuffer.String()
	for _, want := range []string{
		`msg="edge publication transaction failed"`,
		"operation=publish",
		"stage=admission",
		"outcome=canceled",
		"http_status=422",
		"durable_sequence=1",
		"routing_table_peers=31",
	} {
		if !strings.Contains(logged, want) {
			t.Fatalf("contention diagnostic log missing %q:\n%s", want, logged)
		}
	}
	if strings.Contains(logged, "sequence=2") {
		t.Fatalf("canceled pre-admission publication identifiers reached the log:\n%s", logged)
	}
	body := scrapeMetrics(t, mx)
	for _, want := range []string{
		`bloar_edge_publication_transactions_total{operation="publish",outcome="canceled",stage="admission"} 1`,
		`bloar_edge_publication_wait_duration_seconds_count{operation="publish",outcome="canceled"} 1`,
		`bloar_edge_publication_transactions_total{operation="restore",outcome="ok",stage="complete"} 1`,
		`bloar_edge_publication_wait_duration_seconds_count{operation="restore",outcome="ok"} 1`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("contention metrics missing %q:\n%s", want, body)
		}
	}
}

func TestSinkQueuedApplyReceivesFreshTransactionBudgetAfterAdmission(t *testing.T) {
	fixture := newFixture(t)
	document1, record1 := fixture.publication(t, 1, 1, "all")
	initial := fixture.newSink(t, fixture.route, fixture.docs, fixture.stateFile)
	if err := initial.Apply(t.Context(), document1, record1); err != nil {
		t.Fatalf("initial Apply: %v", err)
	}

	provider := newSequencedBudgetProvider()
	route := newRoute()
	const transactionTimeout = 300 * time.Millisecond
	sink := fixture.newSinkWith(
		t, provider, route, memDocs(t), fixture.stateFile, transactionTimeout, nil,
	)
	firstResult := make(chan error, 1)
	go func() {
		_, err := sink.Restore(t.Context())
		firstResult <- err
	}()
	<-provider.firstStarted

	document2, record2 := fixture.publication(t, 2, 2, "all")
	secondResult := make(chan error, 1)
	go func() { secondResult <- sink.Apply(t.Context(), document2, record2) }()

	// Consume more than half of the configured work budget while the second
	// publication is queued. The second provider call must still receive a new
	// full budget after it acquires the permit.
	time.Sleep(180 * time.Millisecond)
	close(provider.releaseFirst)
	if err := <-firstResult; err != nil {
		t.Fatalf("Restore: %v", err)
	}
	var remaining time.Duration
	select {
	case remaining = <-provider.secondBudget:
	case err := <-secondResult:
		t.Fatalf("queued Apply returned before entering DHT work: %v", err)
	case <-time.After(time.Second):
		t.Fatal("queued Apply did not enter DHT work")
	}
	if remaining < 250*time.Millisecond {
		t.Fatalf("queued Apply began with %s remaining, want a fresh %s work budget", remaining, transactionTimeout)
	}
	if err := <-secondResult; err != nil {
		t.Fatalf("queued Apply: %v", err)
	}
	if got := route.snapshot(); !slices.Equal(got, []string{"put", "put"}) {
		t.Fatalf("route events = %v, want two completed puts", got)
	}
}

func TestSinkQueuedRestoreStopsWhenApplyEstablishesReadiness(t *testing.T) {
	fixture := newFixture(t)
	document1, record1 := fixture.publication(t, 1, 1, "all")
	initial := fixture.newSink(t, fixture.route, fixture.docs, fixture.stateFile)
	if err := initial.Apply(t.Context(), document1, record1); err != nil {
		t.Fatalf("initial Apply: %v", err)
	}

	provider := newCountingGatedProvider()
	route := newRoute()
	sink := fixture.newSinkWith(
		t, provider, route, memDocs(t), fixture.stateFile, 500*time.Millisecond, nil,
	)
	document2, record2 := fixture.publication(t, 2, 2, "all")
	applyResult := make(chan error, 1)
	go func() { applyResult <- sink.Apply(t.Context(), document2, record2) }()
	<-provider.started

	restoreResult := make(chan error, 1)
	go func() {
		present, err := sink.Restore(t.Context())
		if err == nil && !present {
			err = errors.New("queued Restore reported no durable state")
		}
		restoreResult <- err
	}()
	select {
	case err := <-restoreResult:
		t.Fatalf("Restore returned before the active Apply released the permit: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	close(provider.release)
	if err := <-applyResult; err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if err := <-restoreResult; err != nil {
		t.Fatalf("queued Restore: %v", err)
	}
	if got := provider.callCount(); got != 1 {
		t.Fatalf("provider calls = %d, want only the readiness-establishing Apply", got)
	}
	if got := route.snapshot(); !slices.Equal(got, []string{"put"}) {
		t.Fatalf("route events = %v, want no redundant restore DHT transaction", got)
	}
}

func TestSinkAdmissionTimeoutDoesNotEnterDHT(t *testing.T) {
	fixture := newFixture(t)
	provider := newStubbornProvider()
	route := newRoute()
	const transactionTimeout = 40 * time.Millisecond
	mx := metrics.New()
	sink := fixture.newSinkWith(
		t, provider, route, fixture.docs, fixture.stateFile, transactionTimeout, mx,
	)
	document1, record1 := fixture.publication(t, 1, 1, "all")
	firstResult := make(chan error, 1)
	go func() { firstResult <- sink.Apply(t.Context(), document1, record1) }()
	<-provider.started

	document2, record2 := fixture.publication(t, 2, 2, "all")
	started := time.Now()
	err := sink.Apply(t.Context(), document2, record2)
	elapsed := time.Since(started)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("queued Apply error = %v, want admission deadline", err)
	}
	if elapsed < 20*time.Millisecond || elapsed > 200*time.Millisecond {
		t.Fatalf("admission deadline elapsed = %s, want bounded near %s", elapsed, transactionTimeout)
	}
	if got := route.snapshot(); len(got) != 0 {
		t.Fatalf("timed-out admission entered record routing: %v", got)
	}

	close(provider.release)
	if err := <-firstResult; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("held Apply error = %v, want its work deadline", err)
	}
	body := scrapeMetrics(t, mx)
	for _, want := range []string{
		`bloar_edge_publication_transactions_total{operation="publish",outcome="timeout",stage="admission"} 1`,
		`bloar_edge_publication_wait_duration_seconds_count{operation="publish",outcome="timeout"} 1`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("admission metrics missing %q:\n%s", want, body)
		}
	}
}

func TestSinkSustainedChangingHeadPublicationsRetainLatestFloor(t *testing.T) {
	fixture := newFixture(t)
	sink := fixture.newSink(t, fixture.route, fixture.docs, fixture.stateFile)
	const publications = 32
	var latestDocument, latestRecord []byte
	for revision := uint64(1); revision <= publications; revision++ {
		latestDocument, latestRecord = fixture.publication(t, revision, revision, "all")
		if err := sink.Apply(t.Context(), latestDocument, latestRecord); err != nil {
			t.Fatalf("Apply revision %d: %v", revision, err)
		}
	}
	if got := len(fixture.route.snapshot()); got != publications*2 {
		t.Fatalf("route event count = %d, want %d", got, publications*2)
	}

	// Reopening from disk proves the rapid sequence ended with one coherent
	// latest floor rather than an earlier durable intermediate.
	recoveredRoute := newRoute()
	restarted := fixture.newSink(t, recoveredRoute, memDocs(t), fixture.stateFile)
	if present, err := restarted.Restore(t.Context()); err != nil || !present {
		t.Fatalf("Restore latest publication = %v, %v", present, err)
	}
	if err := restarted.Apply(t.Context(), latestDocument, latestRecord); err != nil {
		t.Fatalf("replay latest publication: %v", err)
	}
	oldDocument, oldRecord := fixture.publication(t, publications-1, publications-1, "all")
	if err := restarted.Apply(t.Context(), oldDocument, oldRecord); err == nil ||
		!strings.Contains(err.Error(), "below durable floor") {
		t.Fatalf("old publication error = %v, want durable-floor refusal", err)
	}
}

type fixture struct {
	documentKey ed25519.PrivateKey
	ipnsKey     crypto.PrivKey
	name        ipns.Name
	edgePeer    peer.ID
	otherPeer   peer.ID
	route       *eventRoute
	docs        *p2p.DocBlockstore
	stateFile   string
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	_, documentKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	ipnsKey, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	authority, _ := peer.IDFromPrivateKey(ipnsKey)
	edgePeer := randomPeer(t)
	return &fixture{
		documentKey: documentKey, ipnsKey: ipnsKey, name: ipns.NameFromPeer(authority),
		edgePeer: edgePeer, otherPeer: randomPeer(t), route: newRoute(), docs: memDocs(t),
		stateFile: filepath.Join(t.TempDir(), "publication.json"),
	}
}

func (f *fixture) newSink(t *testing.T, route *eventRoute, docs *p2p.DocBlockstore, state string) *edge.Sink {
	return f.newSinkWith(t, route, route, docs, state, edge.DefaultTransactionTimeout, nil)
}

func (f *fixture) newSinkWith(
	t *testing.T,
	provider p2p.DocumentProvider,
	valueRouting routing.ValueStore,
	docs *p2p.DocBlockstore,
	state string,
	transactionTimeout time.Duration,
	mx *metrics.Metrics,
) *edge.Sink {
	return f.newSinkWithPointers(t, provider, valueRouting, docs, state, transactionTimeout, mx, noopPointerPlanner{})
}

func (f *fixture) newSinkWithPointers(
	t *testing.T,
	provider p2p.DocumentProvider,
	valueRouting routing.ValueStore,
	docs *p2p.DocBlockstore,
	state string,
	transactionTimeout time.Duration,
	mx *metrics.Metrics,
	pointers edge.PointerPlanner,
) *edge.Sink {
	return f.newSinkWithDiagnostics(
		t,
		provider,
		valueRouting,
		docs,
		state,
		transactionTimeout,
		mx,
		pointers,
		nil,
		nil,
	)
}

func (f *fixture) newSinkWithDiagnostics(
	t *testing.T,
	provider p2p.DocumentProvider,
	valueRouting routing.ValueStore,
	docs *p2p.DocBlockstore,
	state string,
	transactionTimeout time.Duration,
	mx *metrics.Metrics,
	pointers edge.PointerPlanner,
	logger *slog.Logger,
	routingTableSize func() int,
) *edge.Sink {
	t.Helper()
	sink, err := edge.NewSink(edge.SinkConfig{
		Name: f.name, DocumentPublicKey: f.documentKey.Public().(ed25519.PublicKey),
		Network: "mainnet", ArchiveID: testArchiveID, EdgePeer: f.edgePeer,
		Documents: docs, Provider: provider, Routing: valueRouting, Notifier: noopNotifier{},
		Pointers:           pointers,
		StateFile:          state,
		TransactionTimeout: transactionTimeout,
		Metrics:            mx,
		Logger:             logger,
		RoutingTableSize:   routingTableSize,
		AllowedHeads: map[string]edge.HeadPolicy{
			"all": {Kind: server.FinalizedMonotonic, Required: true},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return sink
}

func (f *fixture) document(revision uint64, head string) server.Doc {
	synced := uint64(12)
	root := dagCBORCID([]byte("root"))
	doc := server.Doc{Unsigned: server.Unsigned{
		V: server.LogicalArchiveDocVersion, Net: "mainnet", ArchiveID: &testArchiveID,
		UpdatedAt:  time.Now().UTC().Format(time.RFC3339Nano),
		Multiaddrs: []string{"/ip4/203.0.113.1/tcp/4005/p2p/" + f.edgePeer.String()},
		Heads: []server.HeadEntry{{
			Name: head, Root: root.String(), OriginSlot: 1, SyncedTo: &synced,
			SegBits: 9, FanoutBits: 8, DirDepth: 1,
		}},
		Revision: &revision,
	}}
	return doc
}

func (f *fixture) publication(t *testing.T, sequence, revision uint64, head string) ([]byte, []byte) {
	t.Helper()
	doc := f.document(revision, head)
	return f.encode(t, doc, sequence)
}

func (f *fixture) encode(t *testing.T, doc server.Doc, sequence uint64) ([]byte, []byte) {
	t.Helper()
	signDocument(t, &doc, f.documentKey)
	document, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	return document, f.record(t, document, sequence)
}

func mutableEntry(t *testing.T, finalizedHead server.HeadEntry) server.HeadEntry {
	t.Helper()
	root := dagCBORCID([]byte("mutable-root"))
	window, synced := uint64(10), uint64(13)
	finalized := *finalizedHead.SyncedTo
	handoffSynced := *finalizedHead.SyncedTo
	beaconRoot := "0x" + strings.Repeat("1", 64)
	return server.HeadEntry{
		Name: "unfinalized", Root: root.String(), OriginSlot: window, SyncedTo: &synced,
		SegBits: 5, FanoutBits: 8, DirDepth: 1, Kind: server.UnfinalizedMutable, WindowStart: &window,
		SourceHeadRoot: beaconRoot, SourceFinalizedSlot: &finalized, SourceFinalizedRoot: beaconRoot,
		HandoffHead: finalizedHead.Name, HandoffRoot: finalizedHead.Root, HandoffSyncedTo: &handoffSynced,
	}
}

func (f *fixture) record(t *testing.T, document []byte, sequence uint64) []byte {
	t.Helper()
	block, err := p2p.NewDocumentBlock(document)
	if err != nil {
		t.Fatal(err)
	}
	record, err := ipns.NewRecord(f.ipnsKey, path.FromCid(block.Cid()), sequence,
		time.Now().Add(24*time.Hour), ipns.DefaultRecordTTL)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := ipns.MarshalRecord(record)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func signDocument(t *testing.T, doc *server.Doc, key ed25519.PrivateKey) {
	t.Helper()
	canonical, err := doc.Unsigned.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	doc.Pubkey = hex.EncodeToString(key.Public().(ed25519.PublicKey))
	doc.Signature = hex.EncodeToString(ed25519.Sign(key, canonical))
}

func randomPeer(t *testing.T) peer.ID {
	t.Helper()
	key, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	id, err := peer.IDFromPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

type noopNotifier struct{}

func (noopNotifier) NotifyNewBlocks(context.Context, ...blocks.Block) error { return nil }

type noopPointerPlanner struct{}

func (noopPointerPlanner) PlanAuthenticated(blocks.Block, server.Doc) (edge.PointerPlan, error) {
	return noopPointerPlan{}, nil
}

type noopPointerPlan struct{}

func (noopPointerPlan) Commit() error { return nil }

type recordingPointerPlanner struct {
	mu        sync.Mutex
	route     *eventRoute
	planErr   error
	commitErr error
	plans     int
	commits   int
}

func (p *recordingPointerPlanner) PlanAuthenticated(blocks.Block, server.Doc) (edge.PointerPlan, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.plans++
	p.route.record("plan")
	if p.planErr != nil {
		return nil, p.planErr
	}
	return recordingPointerPlan{owner: p}, nil
}

func (p *recordingPointerPlanner) reset() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.plans = 0
	p.commits = 0
}

type recordingPointerPlan struct {
	owner *recordingPointerPlanner
}

func (p recordingPointerPlan) Commit() error {
	p.owner.mu.Lock()
	defer p.owner.mu.Unlock()
	p.owner.commits++
	p.owner.route.record("pointers")
	return p.owner.commitErr
}

type gatedProvider struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

type sequencedBudgetProvider struct {
	mu           sync.Mutex
	calls        int
	firstStarted chan struct{}
	releaseFirst chan struct{}
	secondBudget chan time.Duration
}

func newSequencedBudgetProvider() *sequencedBudgetProvider {
	return &sequencedBudgetProvider{
		firstStarted: make(chan struct{}),
		releaseFirst: make(chan struct{}),
		secondBudget: make(chan time.Duration, 1),
	}
}

func (p *sequencedBudgetProvider) Provide(ctx context.Context, _ cid.Cid, _ bool) error {
	p.mu.Lock()
	p.calls++
	call := p.calls
	p.mu.Unlock()
	if call == 1 {
		close(p.firstStarted)
		select {
		case <-p.releaseFirst:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	deadline, ok := ctx.Deadline()
	if !ok {
		return errors.New("second provider call has no work deadline")
	}
	p.secondBudget <- time.Until(deadline)
	return nil
}

type stubbornProvider struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

type countingGatedProvider struct {
	mu      sync.Mutex
	calls   int
	started chan struct{}
	release chan struct{}
}

func newCountingGatedProvider() *countingGatedProvider {
	return &countingGatedProvider{started: make(chan struct{}), release: make(chan struct{})}
}

func (p *countingGatedProvider) Provide(ctx context.Context, _ cid.Cid, _ bool) error {
	p.mu.Lock()
	p.calls++
	call := p.calls
	p.mu.Unlock()
	if call == 1 {
		close(p.started)
	}
	select {
	case <-p.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *countingGatedProvider) callCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls
}

func newStubbornProvider() *stubbornProvider {
	return &stubbornProvider{started: make(chan struct{}), release: make(chan struct{})}
}

func (p *stubbornProvider) Provide(ctx context.Context, _ cid.Cid, _ bool) error {
	p.once.Do(func() { close(p.started) })
	<-p.release
	return ctx.Err()
}

func newGatedProvider() *gatedProvider {
	return &gatedProvider{started: make(chan struct{}), release: make(chan struct{})}
}

func (p *gatedProvider) Provide(ctx context.Context, _ cid.Cid, _ bool) error {
	p.once.Do(func() { close(p.started) })
	select {
	case <-p.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type deadlineProvider struct {
	started chan struct{}
	once    sync.Once
}

func (p *deadlineProvider) Provide(ctx context.Context, _ cid.Cid, _ bool) error {
	p.once.Do(func() { close(p.started) })
	<-ctx.Done()
	return ctx.Err()
}

type eventRoute struct {
	mu         sync.Mutex
	values     map[string][]byte
	events     []string
	provideErr error
	putErr     error
}

type contextInspectRoute struct {
	*eventRoute
	provideDialTimeouts    []time.Duration
	putDialTimeouts        []time.Duration
	provideDeadlines       []time.Time
	putDeadlines           []time.Time
	provideBudgetRemaining []time.Duration
}

func (r *contextInspectRoute) Provide(ctx context.Context, _ cid.Cid, _ bool) error {
	deadline, _ := ctx.Deadline()
	r.provideDialTimeouts = append(r.provideDialTimeouts, network.GetDialPeerTimeout(ctx))
	r.provideDeadlines = append(r.provideDeadlines, deadline)
	r.provideBudgetRemaining = append(r.provideBudgetRemaining, time.Until(deadline))
	r.record("provide")
	return nil
}

func (r *contextInspectRoute) PutValue(ctx context.Context, key string, value []byte, _ ...routing.Option) error {
	deadline, _ := ctx.Deadline()
	r.putDialTimeouts = append(r.putDialTimeouts, network.GetDialPeerTimeout(ctx))
	r.putDeadlines = append(r.putDeadlines, deadline)
	r.record("put")
	r.mu.Lock()
	r.values[key] = append([]byte(nil), value...)
	r.mu.Unlock()
	return nil
}

func newRoute() *eventRoute { return &eventRoute{values: make(map[string][]byte)} }

func (r *eventRoute) Provide(context.Context, cid.Cid, bool) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, "provide")
	return r.provideErr
}

func (r *eventRoute) PutValue(_ context.Context, key string, value []byte, _ ...routing.Option) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, "put")
	if r.putErr != nil {
		return r.putErr
	}
	r.values[key] = append([]byte(nil), value...)
	return nil
}

func (r *eventRoute) GetValue(_ context.Context, key string, _ ...routing.Option) ([]byte, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	value, ok := r.values[key]
	if !ok {
		return nil, routing.ErrNotFound
	}
	return append([]byte(nil), value...), nil
}

func (r *eventRoute) SearchValue(context.Context, string, ...routing.Option) (<-chan []byte, error) {
	return nil, errors.New("not implemented")
}

func (r *eventRoute) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.events...)
}

func (r *eventRoute) record(event string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, event)
}

func (r *eventRoute) reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = nil
}

type deadlinePutRoute struct {
	*eventRoute
}

func newDeadlinePutRoute() *deadlinePutRoute {
	return &deadlinePutRoute{eventRoute: newRoute()}
}

func (r *deadlinePutRoute) PutValue(ctx context.Context, _ string, _ []byte, _ ...routing.Option) error {
	r.mu.Lock()
	r.events = append(r.events, "put")
	r.mu.Unlock()
	<-ctx.Done()
	return ctx.Err()
}

type opaqueDeadlinePutRoute struct {
	*eventRoute
}

func newOpaqueDeadlinePutRoute() *opaqueDeadlinePutRoute {
	return &opaqueDeadlinePutRoute{eventRoute: newRoute()}
}

type queryEventDeadlinePutRoute struct {
	*eventRoute
	contextLiveAfterTermination bool
}

func newQueryEventDeadlinePutRoute() *queryEventDeadlinePutRoute {
	return &queryEventDeadlinePutRoute{eventRoute: newRoute()}
}

func (r *queryEventDeadlinePutRoute) Provide(
	ctx context.Context,
	_ cid.Cid,
	_ bool,
) error {
	r.mu.Lock()
	r.events = append(r.events, "provide")
	r.mu.Unlock()

	sensitive := peer.ID("sensitive-query-peer")
	sensitiveAddr := ma.StringCast("/dns4/sensitive-query-address.example/tcp/4001")
	for i := 0; i < 7; i++ {
		routing.PublishQueryEvent(ctx, &routing.QueryEvent{
			Type: routing.SendingQuery,
			ID:   sensitive,
		})
		if i < 6 {
			routing.PublishQueryEvent(ctx, &routing.QueryEvent{
				Type: routing.PeerResponse,
				ID:   sensitive,
				Responses: []*peer.AddrInfo{
					{ID: sensitive, Addrs: []ma.Multiaddr{sensitiveAddr}},
					{ID: sensitive},
					{ID: sensitive},
				},
			})
		}
	}
	for i := 0; i < 2; i++ {
		routing.PublishQueryEvent(ctx, &routing.QueryEvent{
			Type:  routing.QueryError,
			ID:    sensitive,
			Extra: "sensitive-query-error",
		})
	}
	routing.PublishQueryEvent(ctx, &routing.QueryEvent{
		Type: routing.DialingPeer,
		ID:   sensitive,
	})
	for range 24 {
		publishSensitiveLookupUpdate(ctx, true, 0, 0, 0, 0)
	}
	publishSensitiveLookupUpdate(ctx, true, 0, 2, 0, 0)
	publishSensitiveLookupUpdate(ctx, false, 4, 0, 1, 0)
	publishSensitiveLookupTerminations(ctx)
	return nil
}

func (r *queryEventDeadlinePutRoute) PutValue(
	ctx context.Context,
	_ string,
	_ []byte,
	_ ...routing.Option,
) error {
	r.mu.Lock()
	r.events = append(r.events, "put")
	r.mu.Unlock()

	sensitive := peer.ID("sensitive-query-peer")
	sensitiveAddr := ma.StringCast("/dns4/sensitive-query-address.example/tcp/4001")
	for i := 0; i < 24; i++ {
		routing.PublishQueryEvent(ctx, &routing.QueryEvent{
			Type: routing.SendingQuery,
			ID:   sensitive,
		})
		if i < 18 {
			routing.PublishQueryEvent(ctx, &routing.QueryEvent{
				Type: routing.PeerResponse,
				ID:   sensitive,
				Responses: []*peer.AddrInfo{
					{ID: sensitive, Addrs: []ma.Multiaddr{sensitiveAddr}},
					{ID: sensitive},
				},
			})
		}
	}
	for i := 0; i < 3; i++ {
		routing.PublishQueryEvent(ctx, &routing.QueryEvent{
			Type:  routing.QueryError,
			ID:    sensitive,
			Extra: "sensitive-query-error",
		})
	}
	for i := 0; i < 4; i++ {
		routing.PublishQueryEvent(ctx, &routing.QueryEvent{
			Type: routing.DialingPeer,
			ID:   sensitive,
		})
	}
	for i := 0; i < 20; i++ {
		routing.PublishQueryEvent(ctx, &routing.QueryEvent{
			Type: routing.Value,
			ID:   sensitive,
		})
	}
	for range 24 {
		publishSensitiveLookupUpdate(ctx, true, 0, 0, 0, 0)
	}
	publishSensitiveLookupUpdate(ctx, true, 0, 3, 0, 0)
	publishSensitiveLookupUpdate(ctx, false, 5, 0, 1, 1)
	publishSensitiveLookupTerminations(ctx)
	r.contextLiveAfterTermination = ctx.Err() == nil
	time.Sleep(25 * time.Millisecond)
	return context.DeadlineExceeded
}

func publishSensitiveLookupUpdate(
	ctx context.Context,
	request bool,
	heard, waiting, queried, unreachable int,
) {
	update := &dht.LookupUpdateEvent{
		Cause:       &dht.PeerKadID{Peer: peer.ID("sensitive-lookup-cause")},
		Source:      &dht.PeerKadID{Peer: peer.ID("sensitive-lookup-source")},
		Heard:       sensitiveLookupPeers(heard),
		Waiting:     sensitiveLookupPeers(waiting),
		Queried:     sensitiveLookupPeers(queried),
		Unreachable: sensitiveLookupPeers(unreachable),
	}
	event := &dht.LookupEvent{
		Node: &dht.PeerKadID{Peer: peer.ID("sensitive-lookup-node")},
		Key:  &dht.KeyKadID{Key: "sensitive-lookup-key"},
	}
	if request {
		event.Request = update
	} else {
		event.Response = update
	}
	dht.PublishLookupEvent(ctx, event)
}

func publishSensitiveLookupTerminations(ctx context.Context) {
	for _, reason := range []dht.LookupTerminationReason{
		dht.LookupStopped,
		dht.LookupCancelled,
		dht.LookupStarvation,
		dht.LookupCompleted,
	} {
		dht.PublishLookupEvent(ctx, &dht.LookupEvent{
			Node:      &dht.PeerKadID{Peer: peer.ID("sensitive-lookup-node")},
			Key:       &dht.KeyKadID{Key: "sensitive-lookup-key"},
			Terminate: dht.NewLookupTerminateEvent(reason),
		})
	}
}

func sensitiveLookupPeers(n int) []*dht.PeerKadID {
	peers := make([]*dht.PeerKadID, n)
	for i := range peers {
		peers[i] = &dht.PeerKadID{Peer: peer.ID("sensitive-lookup-peer")}
	}
	return peers
}

func (r *opaqueDeadlinePutRoute) PutValue(
	ctx context.Context,
	_ string,
	_ []byte,
	_ ...routing.Option,
) error {
	r.mu.Lock()
	r.events = append(r.events, "put")
	r.mu.Unlock()
	<-ctx.Done()
	return errors.New("routing operation stopped")
}

func serveUnixSink(t *testing.T, sink *edge.Sink) string {
	t.Helper()
	socket := filepath.Join(t.TempDir(), "edge.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: sink.Handler()}
	go func() {
		_ = server.Serve(listener)
	}()
	t.Cleanup(func() {
		_ = server.Close()
		_ = listener.Close()
	})
	return socket
}

func scrapeMetrics(t *testing.T, mx *metrics.Metrics) string {
	t.Helper()
	recorder := httptest.NewRecorder()
	metrics.Handler(mx, nil).ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("metrics returned HTTP %d", recorder.Code)
	}
	return recorder.Body.String()
}

func memDocs(t *testing.T) *p2p.DocBlockstore {
	t.Helper()
	base := blockstore.NewBlockstore(dssync.MutexWrap(datastore.NewMapDatastore()))
	docs, err := p2p.NewDocBlockstore(base)
	if err != nil {
		t.Fatal(err)
	}
	return docs
}

func mustArchiveID(raw string) server.ArchiveID {
	id, err := server.ParseArchiveID(raw)
	if err != nil {
		panic(err)
	}
	return id
}

func dagCBORCID(raw []byte) cid.Cid {
	hash, err := multihash.Sum(raw, multihash.SHA2_256, 32)
	if err != nil {
		panic(err)
	}
	return cid.NewCidV1(cid.DagCBOR, hash)
}
