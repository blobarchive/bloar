package p2p

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble/v2"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/boxo/ipns"
	"github.com/ipfs/boxo/path"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/routing"
	mh "github.com/multiformats/go-multihash"

	"github.com/blobarchive/bloar/metrics"
)

// prefixIPNS is this package's byte of the KV keyspace. See the package comment
// for the layout and catalog's for the rest of it.
const prefixIPNS byte = 'i'

// ipnsSeqKey is 'i' || "seq".
var ipnsSeqKey = append([]byte{prefixIPNS}, "seq"...)

// DefaultLifetime is how long a published record stays valid (spec 8.1). It is
// boxo's default for the same reason boxo picked it: it matches the Amino DHT's
// expiration window, so a record that outlived it would be gone from the
// network before it was invalid, and one that fell short would be invalid while
// the network still served it.
const DefaultLifetime = ipns.DefaultRecordLifetime

// DefaultRepublish is spec 12's publish.ipns_republish default.
const DefaultRepublish = 4 * time.Hour

// How long a failed publication waits before trying again.
//
// A publish fails for reasons that pass: at startup the DHT has usually not
// found anyone yet ("failed to find any peer in table"), which is the single
// most likely outcome of the first attempt a daemon ever makes. Waiting a whole
// republish interval to try that again would leave the name unpublished for
// hours because the node was briefly young -- so a failure retries on its own
// schedule, and only a success goes back to the interval. The ceiling stays
// well under any sane republish interval, and the floor is high enough that a
// node with no peers at all is not a log-spinner.
const (
	publishMinBackoff = 15 * time.Second
	publishMaxBackoff = 10 * time.Minute
)

// docBlockCacheSize is how many publication documents DocBlockstore keeps. The
// live one is the only one that must be there; the rest are for the followers
// still holding the record that named them, and a document is about a kilobyte,
// so the bound is generous by being irrelevant.
const docBlockCacheSize = 64

// PublisherConfig is what a Publisher needs.
type PublisherConfig struct {
	// Host supplies the key that signs records and, through its PeerID, the
	// name they are published under. Host and Key are mutually exclusive; one
	// is required. Host is the monolithic deployment's compatibility path.
	Host *Host
	// Key lets a private writer sign IPNS records without constructing a
	// libp2p host. The public edge receives only the signed record, never this
	// private key.
	Key crypto.PrivKey
	// Policy owns the deployment-specific side effects around signing. When it
	// is nil, Docs, Provider, and Routing construct the monolithic policy.
	Policy PublicationPolicy
	// Docs is where the document blocks go in monolithic mode.
	Docs *DocBlockstore
	// Routing is where DHT records are put in monolithic mode. It is an interface
	// rather than *dht.IpfsDHT because nothing here needs a DHT in particular,
	// and because a test needs to be able to be a map.
	Routing routing.ValueStore
	// Provider advertises the exact publication-document CID in monolithic mode.
	Provider DocumentProvider
	// KV is store.KV(): the record sequence is persisted there. Required.
	KV *pebble.DB
	// Republish is spec 12's publish.ipns_republish. Zero is DefaultRepublish.
	Republish time.Duration
	// Lifetime is the published record's validity. Zero is DefaultLifetime.
	Lifetime time.Duration
	// Logger receives publication outcomes. Optional.
	Logger *slog.Logger
	// Metrics records the two closed publication stages and the last complete
	// provider-before-IPNS transaction. Optional.
	Metrics *metrics.Metrics
}

// Publisher is the IPNS channel of spec 8.1: it stores each publication
// document as a raw block and puts an IPNS record naming that block to the DHT.
//
// # How it is driven
//
// Notify is the hook server.Heads calls after every document rebuild, and it
// only marks work: the document is stashed and a goroutine does the block
// write, the signing and the DHT put. That is deliberate and not merely for
// speed. Heads calls the hook under the lock that makes root swaps and document
// rebuilds one step (see its comment), and a DHT put is seconds of network; a
// publisher that did its work in the hook would make every POST refs wait for
// the Amino DHT.
//
// The consequence is coalescing of pending VALUES: when the loop starts an attempt it
// publishes the newest document pending at that moment and overwrites (drops) any
// superseded value pending before it. It is NOT cancellation of an in-flight put by a
// later NOTIFICATION, though: a Notify that arrives while Publish is already blocked in
// PutValue cannot stop that put, so an intermediate document can complete before the
// loop picks up the newer pending one. (Close cancels the context and waits, but cannot
// retract a write already delivered.) A buffered wakeup can also cause a redundant
// republish of the SAME document. Spec 8.1 asks for a NOTIFICATION on every root swap,
// not a publication per swap, so coalescing is permitted -- which is why an offline
// reconstruction that posts intermediate documents (a manifest replay) must run with
// IPNS disabled rather than rely on coalescing to hide them.
//
// # Sequence numbers
//
// The sequence is persisted before the record that carries it is published, so
// a crash loses a number rather than reusing one. Followers reject a regressed
// sequence (spec 11.3), which makes reuse the one unrecoverable mistake here
// and a gap merely untidy. The first publication after a restart always takes a
// new number, because what the last process published is not knowable from the
// number alone.
type Publisher struct {
	key       crypto.PrivKey
	name      ipns.Name
	policy    PublicationPolicy
	mx        *metrics.Metrics
	kv        *pebble.DB
	republish time.Duration
	lifetime  time.Duration
	log       *slog.Logger

	// pending is the newest document Notify has seen, and signal wakes the loop
	// for it. Buffered by one: the loop republishes whatever is current when it
	// runs, so a second signal it has not read yet is a signal it does not need.
	pending atomic.Pointer[[]byte]
	signal  chan struct{}

	// mu guards the publication state: one publication at a time, and seq only
	// ever goes up.
	mu   sync.Mutex
	seq  uint64
	last cid.Cid

	cancel context.CancelFunc
	done   chan struct{}

	startOnce sync.Once
	closeOnce sync.Once
}

// DocumentProvider is the narrow DHT capability the IPNS publisher needs to
// make a newly named publication document discoverable before advertising it.
// The bool is the announce flag from libp2p's ContentRouting contract.
type DocumentProvider interface {
	Provide(context.Context, cid.Cid, bool) error
}

// PublicationStage is the bounded failure vocabulary shared by an in-process
// policy and the private-writer-to-public-edge transport.
type PublicationStage string

const (
	PublicationStageProvideDocument PublicationStage = "provide_document"
	PublicationStagePutRecord       PublicationStage = "put_record"
)

// PublicationStageError preserves which half of the provider-before-record
// transaction failed across a process boundary.
type PublicationStageError struct {
	Stage PublicationStage
	Err   error
}

func (e *PublicationStageError) Error() string {
	if e == nil {
		return "p2p: publication stage failed"
	}
	return fmt.Sprintf("p2p: publication stage %s: %v", e.Stage, e.Err)
}

func (e *PublicationStageError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// PublicationCommit commits an already-signed IPNS record for the prepared
// document. A remote implementation carries both bytes to the edge here; a
// monolithic implementation only puts the record because Prepare already
// staged and provided the document locally.
type PublicationCommit func(context.Context, ipns.Name, []byte) error

// PublicationPolicy is the typed in-process security seam between private
// document/record construction and the deployment-specific publication side.
//
// Prepare MUST copy or otherwise retain b until the returned commit completes.
// A policy MUST make the document available and provide its CID before putting
// the record. Publisher serializes record sequence allocation, but callers
// should still treat a policy as concurrency-safe.
type PublicationPolicy interface {
	Prepare(context.Context, blocks.Block) (PublicationCommit, error)
}

type localPublicationPolicy struct {
	docs     *DocBlockstore
	provider DocumentProvider
	routing  routing.ValueStore
}

type mirrorPublicationPolicy struct {
	primary PublicationPolicy
	mirror  PublicationPolicy
	onError func(error)
}

// NewLocalPublicationPolicy returns the monolithic provider-before-record
// implementation. It keeps the old single-process mode available while the
// public edge is an explicit alternative policy.
func NewLocalPublicationPolicy(docs *DocBlockstore, provider DocumentProvider, routing routing.ValueStore) (PublicationPolicy, error) {
	switch {
	case docs == nil:
		return nil, errors.New("p2p: local publication docs must not be nil")
	case provider == nil:
		return nil, errors.New("p2p: local publication provider must not be nil")
	case routing == nil:
		return nil, errors.New("p2p: local publication routing must not be nil")
	}
	return &localPublicationPolicy{docs: docs, provider: provider, routing: routing}, nil
}

// NewMirrorPublicationPolicy keeps primary authoritative and copies the exact
// same already-signed publication to mirror on a best-effort basis. It exists
// for an additive edge canary: failure of a not-yet-authoritative edge cannot
// interrupt the established monolithic IPNS channel. The edge's own readiness
// and evidence are what accept or reject the canary.
func NewMirrorPublicationPolicy(primary, mirror PublicationPolicy, onError func(error)) (PublicationPolicy, error) {
	if primary == nil || mirror == nil {
		return nil, errors.New("p2p: mirror publication requires primary and mirror policies")
	}
	if onError == nil {
		onError = func(error) {}
	}
	return &mirrorPublicationPolicy{primary: primary, mirror: mirror, onError: onError}, nil
}

func (p *localPublicationPolicy) Prepare(ctx context.Context, b blocks.Block) (PublicationCommit, error) {
	p.docs.PutDoc(b)
	if err := p.provider.Provide(ctx, b.Cid(), true); err != nil {
		return nil, &PublicationStageError{
			Stage: PublicationStageProvideDocument,
			Err:   fmt.Errorf("providing publication document %s before IPNS update: %w", b.Cid(), err),
		}
	}
	return func(ctx context.Context, name ipns.Name, raw []byte) error {
		if err := p.routing.PutValue(ctx, string(name.RoutingKey()), raw); err != nil {
			return &PublicationStageError{
				Stage: PublicationStagePutRecord,
				Err:   fmt.Errorf("putting IPNS record for %s: %w", name, err),
			}
		}
		return nil
	}, nil
}

func (p *mirrorPublicationPolicy) Prepare(ctx context.Context, b blocks.Block) (PublicationCommit, error) {
	primaryCommit, err := p.primary.Prepare(ctx, b)
	if err != nil {
		return nil, err
	}
	mirrorCommit, mirrorErr := p.mirror.Prepare(ctx, b)
	if mirrorErr != nil {
		p.onError(mirrorErr)
	}
	return func(ctx context.Context, name ipns.Name, raw []byte) error {
		if err := primaryCommit(ctx, name, raw); err != nil {
			return err
		}
		if mirrorErr == nil {
			if err := mirrorCommit(ctx, name, raw); err != nil {
				p.onError(err)
			}
		}
		return nil
	}, nil
}

// NewPublisher returns a Publisher. It reads the persisted sequence, so a
// restart resumes above whatever the last process published.
func NewPublisher(cfg PublisherConfig) (*Publisher, error) {
	if cfg.Host != nil && cfg.Key != nil {
		return nil, errors.New("p2p: PublisherConfig.Host and Key are mutually exclusive")
	}
	key := cfg.Key
	if cfg.Host != nil {
		key = cfg.Host.Libp2p().Peerstore().PrivKey(cfg.Host.ID())
	}
	if key == nil {
		return nil, errors.New("p2p: PublisherConfig requires Host or Key")
	}
	self, err := peer.IDFromPrivateKey(key)
	if err != nil {
		return nil, fmt.Errorf("p2p: deriving publisher peer ID: %w", err)
	}
	policy := cfg.Policy
	if policy == nil {
		policy, err = NewLocalPublicationPolicy(cfg.Docs, cfg.Provider, cfg.Routing)
		if err != nil {
			return nil, err
		}
	}
	if cfg.KV == nil {
		return nil, errors.New("p2p: PublisherConfig.KV must not be nil")
	}

	p := &Publisher{
		key:       key,
		name:      ipns.NameFromPeer(self),
		policy:    policy,
		mx:        cfg.Metrics,
		kv:        cfg.KV,
		republish: cfg.Republish,
		lifetime:  cfg.Lifetime,
		log:       cfg.Logger,
		signal:    make(chan struct{}, 1),
		done:      make(chan struct{}),
	}
	if p.republish == 0 {
		p.republish = DefaultRepublish
	}
	if p.lifetime == 0 {
		p.lifetime = DefaultLifetime
	}
	// Strictly positive after defaulting: run waits out the
	// republish interval on a time.NewTimer, which a non-positive value fires
	// immediately and continuously, republishing the record in a tight loop; a
	// non-positive lifetime signs records that are already expired. Zero is the
	// documented default just applied, so a value at or below zero is the caller's.
	// The daemon's config boundary rejects the republish interval before New is
	// reached; this is the library guard for any other caller.
	if p.republish <= 0 {
		return nil, fmt.Errorf("p2p: PublisherConfig.Republish is %s, must be positive", p.republish)
	}
	if p.lifetime <= 0 {
		return nil, fmt.Errorf("p2p: PublisherConfig.Lifetime is %s, must be positive", p.lifetime)
	}
	if p.log == nil {
		p.log = slog.New(slog.DiscardHandler)
	}

	seq, err := loadSeq(cfg.KV)
	if err != nil {
		return nil, err
	}
	p.seq = seq
	return p, nil
}

// Name returns the IPNS name this node publishes under: the PeerID, in the
// base36 libp2p-key form (k51...) an operator pastes after /ipns/.
func (p *Publisher) Name() ipns.Name { return p.name }

// Notify hands the publisher a rebuilt publication document. It is
// server.HeadsConfig.OnDoc: it must not block, and does not.
func (p *Publisher) Notify(doc []byte) {
	p.pending.Store(&doc)
	select {
	case p.signal <- struct{}{}:
	default:
	}
}

// Start runs the publication loop until Close.
func (p *Publisher) Start() {
	p.startOnce.Do(func() {
		ctx, cancel := context.WithCancel(context.Background())
		p.cancel = cancel
		go p.run(ctx)
	})
}

// Close stops the loop and waits for an in-flight publication to unwind. It is
// sequenced before the DHT and the host are closed, because it is using both.
func (p *Publisher) Close() error {
	p.closeOnce.Do(func() {
		if p.cancel == nil {
			close(p.done)
			return
		}
		p.cancel()
		<-p.done
	})
	return nil
}

// run publishes the pending document, then whenever one arrives, the republish
// interval comes round, or a failed attempt is due another try.
//
// The wait is a timer rather than a ticker because every one of those three is
// a reason to publish and they are not on a common schedule: what "republish
// every 4h" means is four hours since the last time this said anything, not a
// slot in a grid that a mutation-driven publish has already filled.
func (p *Publisher) run(ctx context.Context) {
	defer close(p.done)

	backoff := publishMinBackoff
	for {
		wait := p.republish
		if doc := p.pending.Load(); doc != nil {
			c, seq, err := p.Publish(ctx, *doc)
			switch {
			case err == nil:
				p.log.Info("ipns published", "name", p.name, "doc", c, "sequence", seq)
				backoff = publishMinBackoff
			case ctx.Err() != nil:
				return
			default:
				// Logged, and tried again shortly. The record already on the
				// network stays valid for its lifetime and the HTTPS channel is
				// untouched, so this is a freshness problem rather than an
				// outage -- but it is one that fixes itself only if something
				// retries. Spec 8.1 has followers reading both channels for
				// exactly this reason.
				wait = min(backoff, p.republish)
				p.log.Error("ipns publish", "name", p.name, "err", err, "retry_in", wait)
				backoff = min(backoff*2, publishMaxBackoff)
			}
		}

		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-p.signal:
			timer.Stop()
		case <-timer.C:
		}
	}
}

// Publish stores doc as a raw block, advertises that exact CID, and only then
// puts an IPNS record naming it. It returns the block's CID and the record's
// sequence.
//
// It is what the loop calls and what a test calls; nothing else should, because
// a caller racing the loop would only be racing itself for the sequence.
func (p *Publisher) Publish(ctx context.Context, doc []byte) (cid.Cid, uint64, error) {
	blk, err := NewDocumentBlock(doc)
	if err != nil {
		return cid.Undef, 0, err
	}
	commit, err := p.policy.Prepare(ctx, blk)
	if err != nil {
		p.mx.IPNSPublicationStage(metrics.IPNSStageProvideDocument, metrics.OutcomeError, time.Now())
		return cid.Undef, 0, err
	}
	// Preserve the monolith's stage semantics across every later failure:
	// once Prepare succeeds the document stage is successful and any sequence,
	// signing, marshalling, or record-commit failure belongs to put_record.
	// A remote policy can refine a commit error back to provide_document when
	// the public edge reports that its actual Provide failed.
	provideOK, recordOK := true, false
	defer func() {
		completedAt := time.Now()
		provideOutcome := metrics.OutcomeError
		if provideOK {
			provideOutcome = metrics.OutcomeOK
		}
		p.mx.IPNSPublicationStage(metrics.IPNSStageProvideDocument, provideOutcome, completedAt)
		if provideOK {
			recordOutcome := metrics.OutcomeError
			if recordOK {
				recordOutcome = metrics.OutcomeOK
			}
			p.mx.IPNSPublicationStage(metrics.IPNSStagePutRecord, recordOutcome, completedAt)
		}
	}()

	p.mu.Lock()
	defer p.mu.Unlock()

	seq := p.seq
	if blk.Cid() != p.last {
		// A new value needs a new number. An unchanged one keeps its number and
		// gets a fresh validity instead, which is what a republish is: the same
		// claim, said again before it expires.
		seq, err = p.nextSeq()
		if err != nil {
			return cid.Undef, 0, err
		}
	}

	rec, err := ipns.NewRecord(p.key, path.FromCid(blk.Cid()), seq, time.Now().Add(p.lifetime), ipns.DefaultRecordTTL)
	if err != nil {
		return cid.Undef, 0, fmt.Errorf("p2p: building IPNS record: %w", err)
	}
	raw, err := ipns.MarshalRecord(rec)
	if err != nil {
		return cid.Undef, 0, fmt.Errorf("p2p: marshalling IPNS record: %w", err)
	}
	if err := commit(ctx, p.name, raw); err != nil {
		var stageErr *PublicationStageError
		if errors.As(err, &stageErr) && stageErr.Stage == PublicationStageProvideDocument {
			provideOK = false
		}
		return cid.Undef, 0, err
	}

	p.last = blk.Cid()
	recordOK = true
	return blk.Cid(), seq, nil
}

// nextSeq bumps the sequence and persists it before it is used. The caller
// holds mu.
func (p *Publisher) nextSeq() (uint64, error) {
	next := p.seq + 1
	if err := storeSeq(p.kv, next); err != nil {
		return 0, err
	}
	p.seq = next
	return next, nil
}

// NewDocumentBlock renders the exact publication-document bytes as the raw
// block spec 8.1 stores: raw codec, sha2-256, which is what every other leaf in
// this archive is (spec 2). It is shared by the publisher and follower so the
// bytes accepted through HTTPS and IPNS acquire precisely the same canonical
// content identity before they can be retained or advertised.
func NewDocumentBlock(doc []byte) (blocks.Block, error) {
	raw := append([]byte(nil), doc...)
	h, err := mh.Sum(raw, mh.SHA2_256, -1)
	if err != nil {
		return nil, fmt.Errorf("p2p: hashing publication document: %w", err)
	}
	blk, err := blocks.NewBlockWithCid(raw, cid.NewCidV1(cid.Raw, h))
	if err != nil {
		return nil, fmt.Errorf("p2p: building publication document block: %w", err)
	}
	return blk, nil
}

// Resolve reads name's IPNS record from vs and returns the CID it names and its
// sequence. It is the follower's half of spec 8.1.
func Resolve(ctx context.Context, vs routing.ValueStore, name ipns.Name) (cid.Cid, uint64, error) {
	raw, err := vs.GetValue(ctx, string(name.RoutingKey()))
	if err != nil {
		return cid.Undef, 0, fmt.Errorf("p2p: resolving IPNS name %s: %w", name, err)
	}
	return DecodeRecord(raw, name)
}

// DecodeRecord verifies a marshalled IPNS record against name and returns the
// CID it names and its sequence.
//
// The verification is not redundant with the DHT's. A DHT validates records it
// stores and serves, which stops it from carrying junk; it is not a statement
// to the caller, and Resolve may be handed a value store that is a cache, a
// gateway, or a file. This is the check that says the record was signed by the
// key the name is, has not expired, and says what it appears to say -- so it is
// done here, on the bytes, every time.
func DecodeRecord(raw []byte, name ipns.Name) (cid.Cid, uint64, error) {
	rec, err := ipns.UnmarshalRecord(raw)
	if err != nil {
		return cid.Undef, 0, fmt.Errorf("p2p: undecodable IPNS record for %s: %w", name, err)
	}
	if err := ipns.ValidateWithName(rec, name); err != nil {
		return cid.Undef, 0, fmt.Errorf("p2p: IPNS record for %s does not verify: %w", name, err)
	}
	value, err := rec.Value()
	if err != nil {
		return cid.Undef, 0, fmt.Errorf("p2p: IPNS record for %s has no value: %w", name, err)
	}
	// /ipfs/<cid> and nothing else: spec 8.1 publishes a block, and a name that
	// resolved to another name, or to a path into something, is not a
	// publication document this archive wrote.
	immutable, err := path.NewImmutablePath(value)
	if err != nil {
		return cid.Undef, 0, fmt.Errorf("p2p: IPNS record for %s names %q, want /ipfs/<cid>: %w", name, value, err)
	}
	if len(immutable.Segments()) != 2 {
		return cid.Undef, 0, fmt.Errorf("p2p: IPNS record for %s names %q, want /ipfs/<cid> with no path", name, value)
	}
	seq, err := rec.Sequence()
	if err != nil {
		return cid.Undef, 0, fmt.Errorf("p2p: IPNS record for %s has no sequence: %w", name, err)
	}
	return immutable.RootCid(), seq, nil
}

// loadSeq reads the persisted record sequence. A KV that has never published
// reads 0, and the first publication is 1.
func loadSeq(kv *pebble.DB) (uint64, error) {
	v, closer, err := kv.Get(ipnsSeqKey)
	if errors.Is(err, pebble.ErrNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("p2p: reading IPNS sequence: %w", err)
	}
	defer closer.Close()
	if len(v) != 8 {
		return 0, fmt.Errorf("p2p: IPNS sequence is %d bytes, want 8", len(v))
	}
	return binary.BigEndian.Uint64(v), nil
}

// storeSeq persists the record sequence. Synchronously: a sequence that reached
// only the page cache would be reused after a crash, and a reused sequence is a
// record every follower is required to reject (spec 11.3) -- an archive that
// silently stopped publishing.
func storeSeq(kv *pebble.DB, seq uint64) error {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], seq)
	if err := kv.Set(ipnsSeqKey, b[:], pebble.Sync); err != nil {
		return fmt.Errorf("p2p: writing IPNS sequence: %w", err)
	}
	return nil
}

// DocBlockstore is the blockstore bitswap serves: the node's own blocks, plus
// the publication documents of spec 8.1 held in memory in front of them.
//
// # Why the documents are not just stored
//
// Spec 8.1 has the writer store the document as a raw block, and the obvious
// place is the node's blockstore. Nothing would keep it there. GC (spec 9)
// sweeps every block the pin ledger does not mark; the ledger holds what
// reconciliation computes from each head's pin policy; and a publication
// document belongs to no head and no policy. The first sweep after a quiet day
// would collect the exact block the live IPNS record names, and the archive
// would go on publishing a name that resolved to nothing.
//
// Pinning it out of harm's way is not available either: the ledger is
// reconciliation's output, not an input, and a row written into it by hand is a
// row the next pass removes.
//
// So the documents live here: in memory, ahead of the node's blocks for reads,
// bounded to the last few, and invisible to GC -- because GC enumerates the
// node's blockstore, and this is not it. Being in memory costs nothing that
// matters. A restart rebuilds the document (server.Heads does, at startup) and
// publishes a record for it immediately, and the document it replaces described
// a state the new one describes better. A follower still holding a record for
// an evicted document re-resolves and gets the current one, which is the same
// path it already takes for any stale record; spec 8.1's dual-channel
// resolution and spec 11.3's no-regression rule are what make that safe, and
// they are load-bearing whether or not this cache exists.
//
// Reads see documents first, then the node's blocks. Everything else -- writes,
// deletes, AllKeysChan -- goes to the node's blocks and only there, so this is
// invisible to every caller except the one asking for a document by its CID.
type DocBlockstore struct {
	base blockstore.Blockstore
	docs *lru.Cache[string, blocks.Block]
}

var _ blockstore.Blockstore = (*DocBlockstore)(nil)

// NewDocBlockstore layers document storage over base, which is the node's own
// blockstore. It is safe to build one even when the IPNS channel is off: with
// nothing publishing into it, it is base.
func NewDocBlockstore(base blockstore.Blockstore) (*DocBlockstore, error) {
	if base == nil {
		return nil, errors.New("p2p: DocBlockstore base must not be nil")
	}
	docs, err := lru.New[string, blocks.Block](docBlockCacheSize)
	if err != nil {
		return nil, fmt.Errorf("p2p: building document block cache: %w", err)
	}
	return &DocBlockstore{base: base, docs: docs}, nil
}

// PutDoc adds a publication document block. It is idempotent by content
// addressing: republishing an unchanged document re-adds the same block under
// the same key.
func (d *DocBlockstore) PutDoc(b blocks.Block) { d.docs.Add(b.Cid().KeyString(), b) }

func (d *DocBlockstore) Get(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	if b, ok := d.docs.Get(c.KeyString()); ok {
		return b, nil
	}
	return d.base.Get(ctx, c)
}

func (d *DocBlockstore) Has(ctx context.Context, c cid.Cid) (bool, error) {
	if d.docs.Contains(c.KeyString()) {
		return true, nil
	}
	return d.base.Has(ctx, c)
}

func (d *DocBlockstore) GetSize(ctx context.Context, c cid.Cid) (int, error) {
	if b, ok := d.docs.Get(c.KeyString()); ok {
		return len(b.RawData()), nil
	}
	return d.base.GetSize(ctx, c)
}

func (d *DocBlockstore) Put(ctx context.Context, b blocks.Block) error { return d.base.Put(ctx, b) }

func (d *DocBlockstore) PutMany(ctx context.Context, bs []blocks.Block) error {
	return d.base.PutMany(ctx, bs)
}

// DeleteBlock deletes from the node's blocks. A document block is not deletable
// and does not need to be: the cache evicts.
func (d *DocBlockstore) DeleteBlock(ctx context.Context, c cid.Cid) error {
	return d.base.DeleteBlock(ctx, c)
}

// AllKeysChan enumerates the node's blocks and not the documents, which is what
// keeps GC from sweeping a store it does not own. See the type comment.
func (d *DocBlockstore) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	return d.base.AllKeysChan(ctx)
}
