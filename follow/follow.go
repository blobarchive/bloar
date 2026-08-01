// Package follow implements the follower protocol of spec 11.3 and the read
// semantics of 11.4: the loop that tracks another archive's published heads,
// replicates them over bitswap, and serves them from the same read API their
// writer serves.
//
// # What a follower is
//
// A follower runs no indexers, no ingest, and no mutation code (spec 11.1). It
// polls a publication document, checks the signature, adopts the roots it names,
// and lets pin reconciliation fetch the DAG behind them. That is the whole
// protocol, and almost all of it is code that already existed for other reasons:
//
//   - p2p.FetchingBlockstore turns "read a block" into "read a block, fetching it
//     if we have not got it". Every read path in this codebase goes through a
//     blockstore, so substituting that one is what makes a head engine replicate.
//   - pinning.Desired says which blocks this node's policy retains for a head.
//     Walking the recursive ones over that blockstore fetches exactly them --
//     spec 11.3's "pin reconciliation over a bitswap-backed blockstore IS the
//     sync mechanism", made literal: retention and backfill are one act.
//   - server.Heads.Adopt registers a root the way a mutation registers one, so a
//     followed head serves, publishes and reconciles like a written one.
//
// What is left, and what is actually in this package, is the trust boundary: who
// signed the document, whether it moved forwards, and whether the bytes that
// arrive are the bytes the index promised.
//
// # No regression
//
// A signature says who wrote a document, not when. An attacker who can withhold
// updates -- or a stale IPNS record still inside its lifetime -- can serve an
// old, perfectly valid document forever, and a follower that took it at face
// value would roll its heads backwards and un-serve slots it already served.
//
// So every floor this package accepts is persisted and never goes down. Legacy
// finalized publications retain the original per-head synced_to and document
// updated_at floors. A revisioned publication is instead ordered by the verified
// document signing key, revision and canonical claim digest; its clock is
// diagnostic and a mutable head may replace its complete bounded coverage. Both
// modes independently retain the per-IPNS-name record sequence and, for finalized
// heads, the accepted manifest tip (spec 10.5's append-only guarantee). They are
// checked independently because they fail independently -- a rolled-back head
// inside a fresh document, a replayed document inside a fresh record, an
// equivocated revision, a swapped filter history inside a fresh tip -- and they
// are on disk because the attack is most attractive exactly when a follower
// restarts. Spec 8.1's dual-channel resolution is the other half: two channels a
// stale answer has to be stale on at once.
//
// A head's root and its per-head floors are one fact, though, not three, and are
// committed as one: an atomic checkpoint (state.checkpoint) written in a single
// synced batch before the head is exposed, and the only thing Resume reads. That
// atomicity is what closes the safety boundary and the safety boundary -- a crash can no longer leave
// a newer root durable beside a stale floor, nor pair a root from one generation
// with a manifest tip from another.
//
// # Verification
//
// Content addressing already makes forgery structurally impossible: a block that
// hashes to the CID the index asked for is the block the writer put there, and
// bitswap checks that on every block it accepts. That is follow.verify: cid, and
// it is inherent rather than something this package does.
//
// What content addressing cannot check is the one binding in the DAG that is not
// a hash of what it points at: a RefEntry says "versioned hash V lives at blob
// C", and nothing about C's bytes proves KZG(C) is V. A writer that lied there
// would serve a well-formed, correctly-addressed blob that is not the blob the
// chain committed to. follow.verify: full recomputes the commitment and checks
// it, and a mismatch quarantines the head (spec 11.4): not the block, the head,
// because a writer that got this wrong has forfeited the only thing its
// signature was vouching for.
//
// # Key layout
//
// This package owns one byte of the node-local KV keyspace store.KV() hands out,
// under the rule catalog's package comment states: single-byte prefixes, no key
// of one structure a prefix of another's. catalog owns 'c' (blob catalog) and
// 'p' (pin ledger), server owns 'h' (head roots), p2p owns 'i' (the IPNS record
// sequence), and this package owns 'f':
//
//	checkpoint       key: 'f' || "checkpoint:" || <head name>
//	                 val: the authoritative per-head generation (state.checkpoint)
//	updated_at floor key: 'f' || "updated_at"
//	                 val: 8-byte big-endian unix seconds
//	ipns seq floors  key: 'f' || "ipns_floors:v1"
//	                 val: bounded versioned set of (IPNS name, uint64 sequence)
//	delegation       key: 'f' || "delegation:v1"
//	                 val: last admitted DNSLink-selected IPNS name + document signer
//	authority floor  key: 'f' || "authority:v1:" || <32-byte signer key>
//	                 val: revisioned-mode marker + revision + canonical digest
//	synced_to floor  key: 'f' || "synced_to:" || <head name> (legacy, read-only)
//	manifest floor   key: 'f' || "manifest:" || <head name> (legacy, read-only)
//	source-set marker key: 'f' || "source_set:v1"
//	                 val: logical archive, monotonic roster generation, and
//	                 irreversible store feature bits
//	conflict latch   key: 'f' || "source_conflict:v1:" || <archive ID> || ':' || <head>
//	                 val: one bounded, content-bound active evidence record
//	conflict seq     key: 'f' || "source_conflict_sequence:v1:" || <archive ID> || ':' || <head>
//	                 val: monotonic occurrence floor retained across clears
//	conflict history key: 'f' || "source_conflict_history:v1:" || <archive ID> || ':' || <head>
//	                 val: bounded exact-evidence operator clear history
//	verified segment key: 'f' || "verified_segment:v1:" || <Segment CID bytes>
//	                 val: derived proof that every RefEntry binding succeeded;
//	                 sealed Segments only, versioned and safe to discard
//
// The adopted root lives in the checkpoint: it and the head's floors are the same
// durable fact, and Resume reads it and nothing else. The head root server.RootStore
// keeps under 'h' (and the manifest tip server.ManifestStore keeps under 'm') is a
// write-through compatibility mirror for the read/serve path and the pin reconciler,
// re-derived from the checkpoint on every adoption -- never a resume source and
// never an authority. A follower's root is node-local state in exactly the sense a
// writer's is (spec 6), and a restart resumes from the checkpoint without waiting
// for the network.
package follow

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net"
	"net/http"
	"slices"
	"sync"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/boxo/exchange"
	"github.com/ipfs/boxo/ipns"
	"github.com/ipfs/boxo/namesys"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/routing"

	"github.com/blobarchive/bloar/archive"
	"github.com/blobarchive/bloar/core"
	"github.com/blobarchive/bloar/metrics"
	"github.com/blobarchive/bloar/p2p"
	"github.com/blobarchive/bloar/p2p/pointerhint"
	"github.com/blobarchive/bloar/pinning"
	"github.com/blobarchive/bloar/replica"
	"github.com/blobarchive/bloar/server"
)

// Spec 12's follow defaults.
const (
	// DefaultPollInterval is follow.poll_interval.
	DefaultPollInterval = 60 * time.Second
	// DefaultFetchTimeout is follow.fetch_timeout: how long one block fetch is
	// given before it is a 503 (spec 11.4).
	DefaultFetchTimeout = 5 * time.Second
)

// docTimeout bounds one background publication-name resolution operation
// across HTTPS, DNSLink, and IPNS. It is not follow.fetch_timeout: that one
// bounds an individual block fetch, including a read path a client is waiting
// on. In the IPNS path this deadline ends after the name resolves; fetching the
// named document gets a fresh follow.fetch_timeout budget.
//
// A cold public Amino-DHT lookup can legitimately exceed thirty seconds while
// the new node fills its routing table. Keeping the old thirty-second outer
// deadline made every attempt cancel at the same point and start over, so an
// IPNS-only follower could stay empty even while the record and its document
// provider were healthy. Two minutes remains bounded while covering cold DHT
// convergence observed in the production preflight.
const docTimeout = 2 * time.Minute

// maxDocBytes bounds a publication document. It is kilobytes (spec 8) and this
// is four orders of magnitude of headroom; the point is only that a follower
// polls a machine it does not run, over a link that can hand it anything.
const maxDocBytes = 1 << 20

// Verify is follow.verify (spec 11.4).
type Verify int

const (
	// VerifyCID is spec 11.4's default: multihash verification on every block,
	// which is inherent to IPFS rather than a thing this package does.
	VerifyCID Verify = iota
	// VerifyFull additionally recomputes the KZG commitment of every blob and
	// checks the versioned hash the index bound to it.
	VerifyFull
)

func (v Verify) String() string {
	switch v {
	case VerifyCID:
		return "cid"
	case VerifyFull:
		return "full"
	default:
		return fmt.Sprintf("Verify(%d)", int(v))
	}
}

// ParseVerify parses the config spelling of a verify mode (spec 12).
func ParseVerify(s string) (Verify, error) {
	switch s {
	case "", "cid":
		return VerifyCID, nil
	case "full":
		return VerifyFull, nil
	default:
		return 0, fmt.Errorf("follow: verify %q must be one of cid, full", s)
	}
}

// Config is spec 12's follow block, plus the node's machinery to run it over.
type Config struct {
	// Net is the network every followed head must be on, and this node's own.
	// A document for another network is refused rather than adopted: net is a
	// head parameter (spec 3.1) and the registry would refuse it anyway, but
	// the refusal belongs where it can name the document that carried it.
	Net string
	// ExpectedArchiveID pins the signed logical archive namespace. When set,
	// every accepted publication must be version 3 and carry this exact ID.
	// The ID separates archives but authorizes no signer; PubKey (or a
	// source-specific key in multi-source mode) remains the trust decision.
	ExpectedArchiveID *server.ArchiveID
	// SourceSet is the bounded set of independently keyed publication
	// authorities. When non-nil it replaces the singular URL/IPNS/DNSLink/PubKey
	// authority fields; ExpectedArchiveID remains the shared logical namespace.
	SourceSet *SourceSetConfig

	// URL is follow.url: the archive whose GET /bloar/v1/heads to poll. Either
	// this or IPNS is required; both together is spec 8.1's dual-channel
	// resolution, where the freshest document that verifies wins.
	URL string
	// IPNS is follow.ipns: the name to resolve (k51...). Requires Routing.
	IPNS string
	// DNSLink is follow.dnslink: a DNS name whose one-hop DNSLink record selects
	// the IPNS name. It is mutually exclusive with IPNS. With no PubKey it also
	// delegates the document signer; an explicit PubKey pins that signer while
	// still allowing the DNS owner to rotate the transport name.
	DNSLink string
	// Routing resolves IPNS names. Required iff IPNS or DNSLink is set.
	Routing routing.ValueStore
	// LookupTXT resolves DNSLink. Optional; net.DefaultResolver is used when
	// DNSLink is configured and this is nil. It exists as an injection seam for
	// deterministic tests and deployments with a controlled resolver.
	LookupTXT namesys.LookupTXTFunc
	// PubKey is follow.pubkey: the ed25519 key every document must verify
	// against. Required unless unpinned DNSLink delegation is configured.
	//
	// A direct URL/IPNS follower always pins this key. DNSLink may instead
	// authenticate an IPNS name whose signed record names the exact document CID;
	// that chain is allowed to delegate the self-signing key carried by the
	// document. An unsigned document is still an
	// unauthenticated claim about what to serve, arriving over a channel (a
	// poll, a DHT record) with no other authentication in it -- and the whole
	// point of the signature is that a follower need not trust the transport.
	PubKey ed25519.PublicKey

	// PollInterval is follow.poll_interval. Zero is DefaultPollInterval.
	PollInterval time.Duration
	// FetchTimeout is follow.fetch_timeout. Zero is DefaultFetchTimeout.
	FetchTimeout time.Duration
	// Verify is follow.verify.
	Verify Verify
	// Heads is follow.heads: the heads to follow, and the pin policy that
	// decides what this node retains of each (spec 9). A head the document
	// carries and this map does not is not followed; a head this map carries
	// and the document does not is logged, once per poll.
	Heads map[string]pinning.Policy
	// ExpectedKinds pins each followed head's authenticated ordering contract.
	// Omission (of the map or one name) means finalized-monotonic, preserving
	// every pre-mutable configuration. A document cannot opt a follower into a
	// mutable head by changing or stripping the signed kind field.
	ExpectedKinds map[string]server.HeadKind
	// MaxMutableWindowSlots bounds the complete snapshot a mutable head may ask
	// this follower to fetch and retain. It is required and strictly positive for
	// every expected unfinalized-mutable head, and ignored nowhere: a limit on an
	// unknown or finalized name is rejected at construction as a config typo.
	MaxMutableWindowSlots map[string]uint64
	// ExpectedHandoffs pins each mutable head to the finalized witness name its
	// signed proof must carry. The witness may be a selected finalized head, or it
	// may be authenticated metadata only: filtered replicas retain their own
	// finalized frontier while preserving the writer's exact global handoff proof.
	ExpectedHandoffs map[string]string
	// OverlayFinalizedHeads maps a selected mutable head to the selected filtered
	// finalized head which closes that replica's serving boundary. It is used only
	// when ExpectedHandoffs names a different, metadata-only global witness. A
	// document may advance the mutable window only when this retained finalized
	// frontier reaches window_start-1, so adoption and GC cannot open a slot gap.
	OverlayFinalizedHeads map[string]string

	// Local is the node's own blockstore: what this node holds. Required. It is
	// what fetched blocks are cached in and what GC sweeps -- and it is
	// deliberately not the blockstore the follower reads through; see New.
	Local blockstore.Blockstore
	// Sessions opens embedded-Bloar bitswap sessions. It is required for the
	// ordinary follower and mutually exclusive with Fetch.
	Sessions p2p.SessionSource
	// Fetch is a caller-owned network-capable blockstore used by standalone
	// archive replicas. It is required with Retention, mutually exclusive with
	// Sessions, and must write fetched blocks durably into Local before returning.
	// Kubo's FetchingBlockstore is the intended implementation.
	Fetch blockstore.Blockstore
	// DocumentBlock fetches a raw publication block without making it part of
	// the retained archive. It is required for IPNS/DNSLink when Fetch is used;
	// Kubo Client.BlockFetch is the intended implementation.
	DocumentBlock func(context.Context, cid.Cid) (blocks.Block, error)
	// FindPointer is the deliberately narrow DHT escape hatch for an exact
	// authenticated publication pointer. The follower calls it only after an
	// ordinary document/root/manifest fetch has failed, then retries the same
	// content-addressed read. It is never given descendant or arbitrary block
	// CIDs and is never installed as generic Bitswap routing. Optional.
	FindPointer func(context.Context, pointerhint.Pointer) error
	// Host dials the multiaddrs the publication document advertises (spec
	// 11.2). Optional: without one, a follower's peers are whatever p2p.peers
	// already connected, which is a complete configuration -- the document's
	// multiaddrs are a convenience the writer offers, not the only way to know
	// where the blocks are.
	Host *p2p.Host
	// DialPeer is the external-store equivalent of Host.Connect. Standalone
	// Kubo replicas wire it to the bounded swarm/connect RPC so authenticated
	// publication hints reach the process which actually runs Bitswap. It is
	// mutually exclusive with Host. Failures are logged and remain nonfatal.
	DialPeer func(context.Context, peer.AddrInfo) error

	// Registry is where adopted heads are registered. Required.
	Registry *server.Heads
	// Roots is where adopted roots are read back from at startup. Required, and
	// the same RootStore Registry commits through.
	Roots *server.RootStore
	// Manifests is the write-through compatibility mirror for followed manifest
	// tips. If nil, New constructs one over KV; explicit injection is useful for
	// callers and fault tests which share the registry's store.
	Manifests *server.ManifestStore
	// Reconciler is told about every adopted head, and its policies are what
	// the fetch pass fetches. Required. Its gate (Reconciler.Gate()) is the one
	// publication and each fetched-block-plus-staging transition use, so the
	// online collector's T0 cut cannot split either operation. Mark and sweep do
	// not hold that gate; the active application blockstore protects their keys.
	Reconciler *pinning.Reconciler
	// Gate is the external-retention publication/read boundary. A standalone
	// archive replica must pass the exact *pinning.Gate also configured on
	// Registry: AdoptBatch publishes under its read side, HTTP readers lease the
	// same side, and the follower takes an exclusive barrier after publication
	// before Retention.Commit may unpin the retired Kubo generation. Required
	// with Retention. Embedded followers derive the gate from Reconciler.
	Gate *pinning.Gate
	// Retention selects the standalone archive-replica path. Before a follower
	// checkpoint can commit, it recursively protects one canonical all-head
	// generation in the external store; after the checkpoint commits, it retires
	// only superseded anchors it durably owns. The external store, not Bloar's
	// pin ledger or GC, owns archive retention in this mode.
	Retention Retention
	// Staging pins every block the fetch pass fetches until the head's own pins
	// retain it, so a GC between the fetch and the reconcile that pins it cannot
	// sweep it (spec 11.3; the follower half of spec 9's window (a)). Optional,
	// and a follower with a GC wants it: without it the fetch pass runs
	// unprotected, exactly as it did before this window was closed. The same
	// *pinning.Staging ingest and GC share.
	Staging *pinning.Staging
	// KV is store.KV(), where the no-regression floors live. Required.
	KV *pebble.DB
	// Cache is the decoded-node cache. Optional.
	Cache *core.NodeCache
	// HTTP is the client the URL channel polls with. Optional.
	HTTP *http.Client
	// Metrics counts each channel's answer to each poll (spec 11.3). Optional;
	// nil records nothing.
	Metrics *metrics.Metrics
	// Ready reports a followed head's readiness changing. It is called with true the
	// first time a head is registered -- resumed from its durable checkpoint (Resume)
	// or first adopted from a verified document (Poll) -- and with false when the head
	// is quarantined and stops being served (spec 11.4). It is the daemon's readiness
	// hook: until every configured followed head has been raised,
	// global readiness stays red and the load balancer routes away, so a head with a
	// missing first-adoption root or a corrupt checkpoint fails closed rather than
	// 404ing behind a green probe; and a head that quarantines withdraws its readiness
	// so the balancer stops routing reads it can only 503. Those two are the only
	// transitions -- an ordinary poll failure leaves a served head serving its durable
	// root and does not regress readiness. Optional; nil reports nothing.
	Ready func(head string, ready bool)
	// OnAdmittedDocument receives the exact raw/sha2-256 source-document block
	// only after its winning whole-document generation is durable and, in
	// external-replica mode, the retention transaction has committed. A caller
	// can therefore retain and advertise this block without turning transport
	// receipt or signature verification alone into authority. Failure is surfaced
	// from Poll after adoption remains durable; the same current/no-op document is
	// retried on a later poll. A successful CID is called once per source per
	// Follower lifetime (singular mode has one implicit source), while a restarted
	// follower calls it again to rebuild ephemeral verified-document serving state.
	// It is singular-mode only: a multi-source consumer must use
	// OnAdmittedSourceDocument so local AllowedHeads policy crosses the callback
	// boundary with the authenticated document. Optional.
	OnAdmittedDocument func(blocks.Block, server.Doc) error
	// OnAdmittedSourceDocument is the multi-source form of
	// OnAdmittedDocument. allowedHeads is the exact operator-authorized subset for
	// the source which supplied block; a consumer must not derive pointers or
	// service claims from any other line the valid signed document carries.
	// Optional.
	OnAdmittedSourceDocument func(blocks.Block, server.Doc, []string) error
	// OnServiceabilityChanged reports a fail-closed registry transition such as
	// quarantine. It runs after Registry has applied the transition and may scan
	// every configured followed head to withdraw exact pointer hints, including
	// dependent mutable heads invalidated by a finalized handoff quarantine.
	// Optional; errors are logged because quarantine itself cannot be undone.
	OnServiceabilityChanged func() error
	// Logger receives what a poll has to say. Optional.
	Logger *slog.Logger
}

// Retention is the narrow transaction boundary between follower authority and
// an external archive store. Implementations must preserve the old committed
// generation until Commit follows the follower's durable checkpoint. Commit
// may retire that old generation: Follower calls it only after the new serving
// snapshot is visible and an exclusive Config.Gate barrier has drained every
// reader which could have selected the retired snapshot.
type Retention interface {
	Prepare(context.Context, replica.Generation) error
	Commit(context.Context, replica.Generation) error
	ProtectsAll(context.Context, []replica.Head) error
}

// headState is one followed head's progress, in memory. The floors that must
// survive a restart are in the KV; this is what the loop would otherwise
// recompute every poll.
type headState struct {
	policy pinning.Policy
	// adopted is the root currently registered, and fetched is the root whose
	// fetch pass completed. They differ while a backfill is in flight or after
	// one failed, which is what makes the pass retry without re-adopting.
	adopted cid.Cid
	fetched cid.Cid
	// manifestTip is the head's accepted manifest tip (spec 10.5), cid.Undef for a
	// head with no chain, and manifestFetched is the tip whose chain the fetch pass
	// has replicated. They differ the same way adopted and fetched do, so the pass
	// re-walks a new chain without re-adopting.
	manifestTip     cid.Cid
	manifestFetched cid.Cid
	// syncCompletions advances only when a retained-closure pass actually
	// CAS-stamps at least one snapshotted root/tip complete. It is process-local
	// telemetry state, not an authority or durability floor.
	syncCompletions uint64
	// quarantined stops this head being adopted again (spec 11.4). The registry
	// refuses too; this is here so the loop does not have to ask it to find out.
	quarantined bool
}

// Follower is the loop of spec 11.3.
//
// # Its lifetime is its own
//
// The bitswap session every singular-source fetch runs in lives from New to
// Close, not for the span of one poll. A source-set follower advances an
// explicit discovery epoch at each poll boundary instead: calls already in
// flight finish on their old session, while the next miss uses a fresh session
// which can discover a surviving writer. In both modes a session is shared
// across sequential reads between discovery boundaries. This lifetime is why
// Close exists and why a daemon sequences it before the exchange underneath.
type Follower struct {
	cfg        Config
	log        *slog.Logger
	client     *http.Client
	sources    []*sourceRuntime
	sourceByID map[string]*sourceRuntime
	name       ipns.Name
	hasIPNS    bool
	dnsLink    string
	lookup     namesys.LookupTXTFunc
	state      *state

	// blocks is the follower's read path: local, then bitswap, bounded per call
	// by follow.fetch_timeout. Every followed head's engine reads through it,
	// and so does the fetch pass.
	blocks blockstore.Blockstore
	// fetch is the same thing without the per-call bound, for the callers that
	// bring their own deadline.
	fetch blockstore.Blockstore
	// structure retains only compact, content-addressed Segment shape proofs.
	// Sharing it across loaded generations makes complete signed-DAG admission
	// incremental without making the cache authoritative.
	structure *archive.StructureCache
	// gate linearizes publication and each fetched-block-plus-staging transition
	// with the online GC's T0 cut (spec 9). It does not exclude the concurrent
	// mark/sweep; application blockstore operations supply epoch protection. This
	// is the same gate the reconciler and GC share.
	gate *pinning.Gate

	sessCtx    context.Context
	sessCancel context.CancelFunc

	// docSess fetches publication document blocks named by IPNS records. It is
	// separate from blocks on purpose: those blocks are not part of any head,
	// nothing pins them, and writing them into the local store would leave GC to
	// sweep a block per publication that never had to be there.
	docOnce sync.Once
	docSess exchange.Fetcher

	// transition serializes checkpoint transitions -- the adopt-and-checkpoint work
	// of Poll (admit) and Resume -- so only one commits at a time. Poll and Resume
	// are exported and a caller may run them concurrently; the single-writer contract
	// the spec 11.3 floors rely on is enforced here in code, not just documented.
	// Without it a later-but-older transition could lower a floor a newer one raised,
	// or a Resume could expose a stale checkpoint after a Poll committed a fresh one
	//. It is the outer lock: held for the whole of a
	// transition, with mu (the in-memory head maps) taken briefly inside it, never
	// the reverse. The ordinary post-publication sync commits no checkpoint and
	// does not take it. The exceptional protection closure for a Poll or Resume
	// which overlaps an active GC does run inside the transition, so that rare
	// pre-publication fetch can delay other transitions without blocking GC.
	transition sync.Mutex
	// syncPermit serializes retained-closure work across the daemon's coalescing
	// background worker and exported direct Poll calls. It is a channel rather
	// than a mutex so a caller waiting behind a long pass can abandon the wait
	// when its context is cancelled.
	syncPermit chan struct{}
	// admittedDocuments retains the exact source-document CID whose callback
	// completed for each source in this process. The empty key is singular-source
	// mode. It is deliberately ephemeral: verified-document serving is fail-closed
	// after restart until a successfully admitted current/no-op poll reconstructs
	// it. transition protects the map and every callback invocation.
	admittedDocuments map[string]cid.Cid
	// conflicts is the configured subset of durable cross-writer conflict
	// latches loaded before the source-set activation commit. transition protects
	// it alongside source admission. A latch freezes advancement of only its
	// finalized head; it neither withdraws the last-good registry entry nor reuses
	// the data-integrity quarantine path.
	conflicts map[string]ConflictRecord

	mu    sync.Mutex
	heads map[string]*headState
	// walked memoises index blocks whose subtree is fully fetched. The value is
	// the blockstore collection generation in which the proof completed. A new
	// GC increments the generation before it can delete, invalidating every old
	// presence proof even after that GC has ended. See sync.go.
	walked map[string]uint64
	// verifiedSegments is the hot cache of the durable semantic proof used by
	// verify: full. A successful entry means every RefEntry was checked,
	// including blobs which were already local. Segment and blob CIDs make that
	// fact immutable across presence-generation changes and refetches; state
	// persists versioned markers for sealed Segments across restarts; the one
	// mutable-position/open Segment is only cached in memory to keep abandoned
	// intermediate open CIDs from growing KV forever. Protection-only walks
	// never populate either layer. See sync.go.
	// The bool records whether the hot proof is also durable. false is the
	// bounded open-Segment cache; true is a sealed proof already represented by
	// its versioned Pebble marker. Keeping the classification here avoids a KV
	// lookup for every historical sealed Segment on every head update.
	verifiedSegments map[string]bool
	// verifiedOpen keeps at most one transient proof key per followed head. When
	// that head's open Segment changes, the old non-durable hot entry is evicted;
	// if it became sealed first, promotion changes the entry to durable and it is
	// retained like every other sealed proof.
	verifiedOpen map[string]string
}

// New returns a Follower over cfg.
func New(cfg Config) (*Follower, error) {
	// Validate and detach source authority configuration before any durable state
	// is inspected. Activating that roster is a later construction step, after
	// every other dependency and policy check has succeeded.
	sourceSet, err := validateAndCloneSourceSet(cfg)
	if err != nil {
		return nil, err
	}
	cfg.SourceSet = sourceSet
	sourceMode := sourceSet != nil

	// Once a store has acknowledged source-set mode, starting a singular runtime
	// would let legacy checkpoints and global floors bypass its retained source
	// policy. Fail before Resume or any poller can observe the store.
	if !sourceMode && cfg.KV != nil {
		if _, sourceSet, err := (&state{kv: cfg.KV}).sourceSetMarker(); err != nil {
			return nil, fmt.Errorf("follow: checking durable source-set mode: %w", err)
		} else if sourceSet {
			return nil, errors.New("follow: durable source-set state exists; refusing to start the singular-source runtime")
		}
	}
	mutableExpected := false
	for name := range cfg.Heads {
		if cfg.ExpectedKinds[name] == server.UnfinalizedMutable {
			mutableExpected = true
			break
		}
	}
	switch {
	case cfg.Net == "":
		return nil, errors.New("follow: Config.Net must not be empty")
	case cfg.ExpectedArchiveID != nil && cfg.ExpectedArchiveID.IsZero():
		return nil, errors.New("follow: Config.ExpectedArchiveID must not be zero")
	case !sourceMode && cfg.URL == "" && cfg.IPNS == "" && cfg.DNSLink == "":
		return nil, errors.New("follow: none of Config.URL, Config.IPNS or Config.DNSLink is set; a follower needs a channel to " +
			"resolve the publication document on (spec 8, 8.1)")
	case !sourceMode && cfg.IPNS != "" && cfg.DNSLink != "":
		return nil, errors.New("follow: Config.IPNS and Config.DNSLink are mutually exclusive; configure one name authority")
	case !sourceMode && (cfg.IPNS != "" || cfg.DNSLink != "") && cfg.Routing == nil:
		return nil, errors.New("follow: Config.IPNS or Config.DNSLink is set but Config.Routing is nil; resolving a name needs a value store")
	case !sourceMode && len(cfg.PubKey) != ed25519.PublicKeySize && !(cfg.DNSLink != "" && len(cfg.PubKey) == 0):
		return nil, fmt.Errorf("follow: Config.PubKey is %d bytes, want %d: a follower verifies every document it "+
			"adopts (spec 8); only DNSLink may delegate it", len(cfg.PubKey), ed25519.PublicKeySize)
	case !sourceMode && mutableExpected && len(cfg.PubKey) != ed25519.PublicKeySize:
		return nil, fmt.Errorf("follow: unfinalized-mutable heads require a pinned %d-byte Config.PubKey: "+
			"mutable revision order is authority-local, so DNSLink must not rotate the document signer", ed25519.PublicKeySize)
	case sourceMode && cfg.OnAdmittedDocument != nil:
		return nil, errors.New("follow: Config.OnAdmittedDocument lacks source AllowedHeads; use OnAdmittedSourceDocument in source-set mode")
	case !sourceMode && cfg.OnAdmittedSourceDocument != nil:
		return nil, errors.New("follow: Config.OnAdmittedSourceDocument is only valid in source-set mode")
	case len(cfg.Heads) == 0:
		return nil, errors.New("follow: Config.Heads is empty; a follower with no heads to follow does nothing")
	case cfg.Local == nil:
		return nil, errors.New("follow: Config.Local must not be nil")
	case cfg.Retention == nil && cfg.Sessions == nil:
		return nil, errors.New("follow: Config.Sessions must not be nil for an embedded follower")
	case cfg.Retention == nil && cfg.Fetch != nil:
		return nil, errors.New("follow: Config.Fetch requires Config.Retention")
	case cfg.Retention != nil && cfg.Fetch == nil:
		return nil, errors.New("follow: Config.Fetch must not be nil with Config.Retention")
	case cfg.Retention != nil && cfg.Sessions != nil:
		return nil, errors.New("follow: Config.Sessions and Config.Retention are mutually exclusive")
	case cfg.Retention != nil && cfg.Gate == nil:
		return nil, errors.New("follow: Config.Gate must not be nil with external Retention")
	case cfg.Retention != nil && cfg.Reconciler != nil && cfg.Reconciler.Gate() != cfg.Gate:
		return nil, errors.New("follow: external Retention, Reconciler, and Registry must share one Config.Gate")
	case cfg.Retention == nil && cfg.Gate != nil && cfg.Reconciler != nil && cfg.Reconciler.Gate() != cfg.Gate:
		return nil, errors.New("follow: Config.Gate differs from Reconciler.Gate")
	case cfg.Host != nil && cfg.DialPeer != nil:
		return nil, errors.New("follow: Config.Host and Config.DialPeer are mutually exclusive")
	case !sourceMode && cfg.Retention != nil && (cfg.IPNS != "" || cfg.DNSLink != "") && cfg.DocumentBlock == nil:
		return nil, errors.New("follow: Config.DocumentBlock is required for IPNS or DNSLink with external retention")
	case cfg.Registry == nil:
		return nil, errors.New("follow: Config.Registry must not be nil")
	case cfg.Roots == nil:
		return nil, errors.New("follow: Config.Roots must not be nil")
	case cfg.Retention == nil && cfg.Reconciler == nil:
		return nil, errors.New("follow: Config.Reconciler must not be nil for an embedded follower")
	case cfg.Retention != nil && cfg.Staging != nil:
		return nil, errors.New("follow: Config.Staging must be nil with external retention")
	case cfg.Retention != nil && cfg.Verify != VerifyCID:
		return nil, errors.New("follow: external archive retention currently requires verify: cid")
	case cfg.KV == nil:
		return nil, errors.New("follow: Config.KV must not be nil")
	}
	for name, p := range cfg.Heads {
		if err := p.Validate(); err != nil {
			return nil, fmt.Errorf("follow: head %q: %w", name, err)
		}
		kind := cfg.ExpectedKinds[name]
		if kind == "" {
			kind = server.FinalizedMonotonic
		}
		switch kind {
		case server.FinalizedMonotonic:
			if _, configured := cfg.MaxMutableWindowSlots[name]; configured {
				return nil, fmt.Errorf("follow: head %q is finalized-monotonic but configures MaxMutableWindowSlots", name)
			}
			if handoff := cfg.ExpectedHandoffs[name]; handoff != "" {
				return nil, fmt.Errorf("follow: finalized-monotonic head %q configures mutable handoff %q", name, handoff)
			}
		case server.UnfinalizedMutable:
			if p.Mode != pinning.ModeFull {
				return nil, fmt.Errorf("follow: unfinalized-mutable head %q must use pin mode full", name)
			}
			if cfg.MaxMutableWindowSlots[name] == 0 {
				return nil, fmt.Errorf("follow: unfinalized-mutable head %q requires a positive MaxMutableWindowSlots", name)
			}
			if cfg.ExpectedHandoffs[name] == "" {
				return nil, fmt.Errorf("follow: unfinalized-mutable head %q requires an ExpectedHandoffs entry", name)
			}
			if cfg.ExpectedHandoffs[name] == name {
				return nil, fmt.Errorf("follow: unfinalized-mutable head %q cannot hand off to itself", name)
			}
			handoff := cfg.ExpectedHandoffs[name]
			if _, selected := cfg.Heads[handoff]; selected {
				if handoffKind := cfg.ExpectedKinds[handoff]; handoffKind != "" && handoffKind != server.FinalizedMonotonic {
					return nil, fmt.Errorf("follow: unfinalized-mutable head %q handoff %q is not finalized-monotonic", name, handoff)
				}
			}
			if overlay := cfg.OverlayFinalizedHeads[name]; overlay != "" {
				if overlay == handoff {
					return nil, fmt.Errorf("follow: unfinalized-mutable head %q overlay finalized head %q equals its authenticated handoff; omit the redundant overlay", name, overlay)
				}
				if _, selected := cfg.Heads[overlay]; !selected {
					return nil, fmt.Errorf("follow: unfinalized-mutable head %q overlay finalized head %q must be selected", name, overlay)
				}
				if overlayKind := cfg.ExpectedKinds[overlay]; overlayKind != "" && overlayKind != server.FinalizedMonotonic {
					return nil, fmt.Errorf("follow: unfinalized-mutable head %q overlay head %q is not finalized-monotonic", name, overlay)
				}
			}
		default:
			return nil, fmt.Errorf("follow: head %q has unknown expected kind %q", name, kind)
		}
		if cfg.Retention != nil && p.Mode != pinning.ModeFull {
			return nil, fmt.Errorf("follow: external archive replica head %q must use pin mode full", name)
		}
	}
	for name := range cfg.ExpectedKinds {
		if _, ok := cfg.Heads[name]; !ok {
			return nil, fmt.Errorf("follow: ExpectedKinds configures unknown head %q", name)
		}
	}
	for name := range cfg.MaxMutableWindowSlots {
		if _, ok := cfg.Heads[name]; !ok {
			return nil, fmt.Errorf("follow: MaxMutableWindowSlots configures unknown head %q", name)
		}
	}
	for name := range cfg.ExpectedHandoffs {
		if _, ok := cfg.Heads[name]; !ok {
			return nil, fmt.Errorf("follow: ExpectedHandoffs configures unknown head %q", name)
		}
	}
	for name := range cfg.OverlayFinalizedHeads {
		if _, ok := cfg.Heads[name]; !ok {
			return nil, fmt.Errorf("follow: OverlayFinalizedHeads configures unknown head %q", name)
		}
		if cfg.ExpectedKinds[name] != server.UnfinalizedMutable {
			return nil, fmt.Errorf("follow: OverlayFinalizedHeads configures non-mutable head %q", name)
		}
	}
	if cfg.Manifests == nil {
		cfg.Manifests = server.NewManifestStore(cfg.KV)
	}
	if err := cfg.Registry.ValidateFollowerStores(cfg.KV, cfg.Roots, cfg.Manifests); err != nil {
		return nil, fmt.Errorf("follow: atomic follower stores: %w", err)
	}
	if cfg.Retention != nil {
		if err := cfg.Registry.ValidateFollowerGate(cfg.Gate); err != nil {
			return nil, fmt.Errorf("follow: external retention reader barrier: %w", err)
		}
	}
	if cfg.ExpectedArchiveID != nil {
		archiveID := *cfg.ExpectedArchiveID
		cfg.ExpectedArchiveID = &archiveID
	}
	if sourceMode && cfg.Retention == nil {
		cfg.Sessions = p2p.NewRefreshingSessionSource(cfg.Sessions)
	}

	f := &Follower{
		cfg: cfg, log: cfg.Logger, client: cfg.HTTP, state: &state{kv: cfg.KV},
		sourceByID: make(map[string]*sourceRuntime), admittedDocuments: make(map[string]cid.Cid),
		conflicts: make(map[string]ConflictRecord),
		structure: archive.NewStructureCache(), syncPermit: make(chan struct{}, 1),
	}
	f.syncPermit <- struct{}{}
	if cfg.Retention != nil {
		f.gate = cfg.Gate
	} else if cfg.Reconciler != nil {
		f.gate = cfg.Reconciler.Gate()
	} else {
		f.gate = pinning.NewGate()
	}
	if f.log == nil {
		f.log = slog.New(slog.DiscardHandler)
	}
	if f.client == nil {
		f.client = http.DefaultClient
	}
	if f.cfg.PollInterval == 0 {
		f.cfg.PollInterval = DefaultPollInterval
	}
	if f.cfg.FetchTimeout == 0 {
		f.cfg.FetchTimeout = DefaultFetchTimeout
	}
	// Strictly positive after defaulting: a non-positive poll
	// interval reaches Run's time.NewTicker and panics the process, and a
	// non-positive fetch timeout makes bounded's per-block context pre-expired. The
	// daemon's config boundary rejects both before New is reached; this is the
	// library guard for any other caller, turning a late panic into a start error.
	if f.cfg.PollInterval <= 0 {
		return nil, fmt.Errorf("follow: Config.PollInterval is %s, must be positive", f.cfg.PollInterval)
	}
	if f.cfg.FetchTimeout <= 0 {
		return nil, fmt.Errorf("follow: Config.FetchTimeout is %s, must be positive", f.cfg.FetchTimeout)
	}
	if f.cfg.IPNS != "" {
		name, err := ipns.NameFromString(f.cfg.IPNS)
		if err != nil {
			return nil, fmt.Errorf("follow: follow.ipns %q is not an IPNS name: %w", f.cfg.IPNS, err)
		}
		f.name, f.hasIPNS = name, true
	}
	if f.cfg.DNSLink != "" {
		f.dnsLink, f.hasIPNS = f.cfg.DNSLink, true
		f.lookup = f.cfg.LookupTXT
		if f.lookup == nil {
			f.lookup = net.DefaultResolver.LookupTXT
		}
	}
	if sourceMode {
		f.sources, err = buildSourceRuntimes(*f.cfg.ExpectedArchiveID, sourceSet, f.cfg.LookupTXT)
		if err != nil {
			return nil, err
		}
		for _, source := range f.sources {
			f.sourceByID[source.cfg.ID] = source
		}
	}

	f.heads = make(map[string]*headState, len(cfg.Heads))
	for name, p := range cfg.Heads {
		f.heads[name] = &headState{policy: p}
	}
	f.walked = map[string]uint64{}
	f.verifiedSegments = map[string]bool{}
	f.verifiedOpen = map[string]string{}
	if sourceMode {
		if err := f.loadConflictLatches(); err != nil {
			return nil, err
		}
	}

	// WithoutCancel of nothing: the session outlives every context that asks it
	// for a block, and Close is what ends it. See the type comment.
	f.sessCtx, f.sessCancel = context.WithCancel(context.Background())
	if cfg.Retention != nil {
		f.fetch = cfg.Fetch
	} else {
		f.fetch = p2p.FetchingBlockstore(f.sessCtx, cfg.Local, cfg.Sessions)
	}
	f.blocks = bounded{inner: f.fetch, timeout: f.cfg.FetchTimeout}
	if sourceMode {
		activation, err := sourceSetActivationForConfig(*f.cfg.ExpectedArchiveID, sourceSet)
		if err != nil {
			f.sessCancel()
			return nil, err
		}
		// This is the final fallible construction step. Every dependency, policy,
		// transport name, duration, store, and resolver has already been checked;
		// once the irreversible roster marker commits, New can only return the
		// fully runnable source-set follower.
		if err := f.activateSourceSet(activation); err != nil {
			f.sessCancel()
			return nil, err
		}
		f.configureSourceSetMetrics()
	}
	return f, nil
}

// Close ends the bitswap session every fetch runs in. A daemon calls it before
// closing the exchange those sessions are on.
func (f *Follower) Close() error {
	f.sessCancel()
	return nil
}

// GCFetch is the per-head self-heal resolver a GC scopes its fetch with
// (pinning.GCConfig.Fetch). For a head this node follows it returns the
// follower's fetching blockstore -- the bounded read path, so a heal a GC makes
// during its online mark is capped at follow.fetch_timeout rather than able to
// hold the protection epoch and prevent sweep progress on a block no peer has.
// The mark does not hold the publication gate while fetching. The fetching view
// writes through to the local application store GC sweeps, so a healed block is
// protected, marked, and present for the rest of the run and every run after.
// For any other head it returns nil, so a node that also writes
// keeps its written heads fail-closed on a missing pinned block (spec 9): a
// followed head's miss is a dangling pin to repair, a written head's is real
// divergence to alert on. A pure writer builds no follower and passes a nil
// resolver, and every head fails closed.
func (f *Follower) GCFetch() func(head string) pinning.BlockFetcher {
	followed := make(map[string]bool, len(f.cfg.Heads))
	for name := range f.cfg.Heads {
		followed[name] = true
	}
	return func(head string) pinning.BlockFetcher {
		if followed[head] {
			return f.blocks
		}
		return nil
	}
}

// Names returns the followed head names, sorted. It is the order every loop
// here works in, so that a log or a failure names them the same way twice.
func (f *Follower) Names() []string { return slices.Sorted(maps.Keys(f.cfg.Heads)) }

func (f *Follower) expectedKind(name string) server.HeadKind {
	kind := f.cfg.ExpectedKinds[name]
	if kind == "" {
		return server.FinalizedMonotonic
	}
	return kind
}

// validateExpectedKinds runs immediately after document authentication and
// contract validation, before dial hints or any head/manifest DAG fetch. It is
// therefore impossible for a signed kind change (or a signed omission which
// would mean finalized) to make the follower fetch under the wrong policy.
func (f *Follower) validateExpectedKinds(doc server.Doc) error {
	for _, entry := range doc.Heads {
		if _, followed := f.cfg.Heads[entry.Name]; !followed {
			continue
		}
		want, got := f.expectedKind(entry.Name), entry.EffectiveKind()
		if got != want {
			return fmt.Errorf("head %q declares kind %q, this follower expects %q", entry.Name, got, want)
		}
		if got != server.UnfinalizedMutable {
			continue
		}
		if wantHandoff := f.cfg.ExpectedHandoffs[entry.Name]; entry.HandoffHead != wantHandoff {
			return fmt.Errorf("unfinalized-mutable head %q names handoff %q, this follower expects %q",
				entry.Name, entry.HandoffHead, wantHandoff)
		}
		// ValidateContract established both pointers and start <= end.
		start, end := *entry.WindowStart, *entry.SyncedTo
		limit := f.cfg.MaxMutableWindowSlots[entry.Name]
		if limit == 0 || end-start >= limit {
			return fmt.Errorf("unfinalized-mutable head %q covers [%d,%d], above this follower's %d-slot maximum",
				entry.Name, start, end, limit)
		}
	}
	return nil
}

// validateOverlayHandoffs proves the local filtered-finalized/live boundary from
// one authenticated publication document. The signed mutable proof still names
// its global handoff witness; OverlayFinalizedHeads is a separate retention
// contract saying which selected finalized head this replica actually keeps.
// Both lines must therefore occur in the same document, and the retained
// finalized frontier must reach the slot immediately before the mutable window.
// This runs before any head DAG fetch or durable write, and again on v3 resume.
func (f *Follower) validateOverlayHandoffs(doc server.Doc, report bool) error {
	if len(f.cfg.OverlayFinalizedHeads) == 0 {
		return nil
	}
	byName := make(map[string]server.HeadEntry, len(doc.Heads))
	for _, entry := range doc.Heads {
		byName[entry.Name] = entry
	}
	refuse := func(mutable, finalized string, windowStart *uint64, finalizedTo *uint64, reason string) error {
		if report {
			f.cfg.Metrics.FollowRefusal(metrics.RefusalHandoffBlocked)
			attrs := []any{"mutable_head", mutable, "finalized_head", finalized, "reason", reason}
			if windowStart != nil {
				attrs = append(attrs, "window_start", *windowStart)
			}
			if finalizedTo != nil {
				attrs = append(attrs, "finalized_synced_to", *finalizedTo)
			}
			f.log.Warn("refused authenticated live overlay: retained finalized coverage does not meet the mutable handoff boundary", attrs...)
		}
		return fmt.Errorf("follow: mutable head %q cannot overlay retained finalized head %q: %s", mutable, finalized, reason)
	}

	for mutable, finalized := range f.cfg.OverlayFinalizedHeads {
		mutableEntry, selected := byName[mutable]
		if !selected || mutableEntry.SyncedTo == nil {
			// A revisioned omission/withdrawal of the mutable head is safe: there is
			// no provisional serving range to join. Explicit mutable emptiness is
			// rejected by the publication contract before this helper runs.
			continue
		}
		finalizedEntry, ok := byName[finalized]
		if !ok || finalizedEntry.SyncedTo == nil {
			return refuse(mutable, finalized, mutableEntry.WindowStart, nil,
				"the mutable head is selected but the retained finalized head is absent or uncovered in the same document")
		}
		if mutableEntry.WindowStart == nil {
			return refuse(mutable, finalized, nil, finalizedEntry.SyncedTo,
				"the mutable head has no authenticated window_start")
		}
		// Written without finalized+1 so MaxUint64 remains well-defined.
		if *mutableEntry.WindowStart > *finalizedEntry.SyncedTo &&
			*mutableEntry.WindowStart-*finalizedEntry.SyncedTo > 1 {
			return refuse(mutable, finalized, mutableEntry.WindowStart, finalizedEntry.SyncedTo,
				fmt.Sprintf("window_start %d is beyond finalized synced_to %d plus one", *mutableEntry.WindowStart, *finalizedEntry.SyncedTo))
		}
	}
	return nil
}

// Run polls until ctx is cancelled, on which it returns nil: a cancelled daemon
// is shutting down, not failing.
//
// It resumes first, then polls immediately rather than after a tick: a follower
// that started because someone restarted it should not wait a minute to find out
// what it missed.
//
// A failed poll is logged and left to the next tick. There is nothing else to do
// with it -- the writer is unreachable, or answering something that does not
// verify, and both are conditions that end when they end. The heads this node
// already adopted go on being served throughout, which is the point of a
// follower.
func (f *Follower) Run(ctx context.Context) error {
	if err := f.Resume(ctx); err != nil {
		f.log.Error("resuming followed heads", "err", err)
	}
	return f.RunAfterResume(ctx)
}

// Resume registers every followed head this node has already adopted from its
// durable checkpoint. In the normal startup order no collection epoch is active,
// so locally loading the root and validating the generation token is sufficient.
// If an exported/concurrent Resume overlaps an active epoch, it uses the same
// retained-closure protection walk as adoption and may fetch missing blocks
// before exposing the checkpoint.
//
// A restart that waited for a poll to serve would be a restart that stops
// serving: the blocks are on disk, the root is on disk, and the writer being
// unreachable at that moment has no bearing on either. It is called by Run, and
// a daemon may call it first to be sure the heads are up before it listens.
func (f *Follower) Resume(ctx context.Context) error {
	// The whole of Resume is a checkpoint transition -- it reads each head's
	// checkpoint, may repair its floor, and exposes it -- so it holds the transition
	// lock for its duration, serialized against a concurrent Poll (see the field).
	f.transition.Lock()
	defer f.transition.Unlock()
	if f.cfg.SourceSet != nil {
		return f.resumeSourceCheckpoints(ctx)
	}

	checkpoints := make(map[string]checkpoint, len(f.cfg.Heads))
	anyV3 := false
	for _, name := range f.Names() {
		cp, ok, err := f.state.checkpoint(name)
		if err != nil {
			// A v3 generation is one document. Do not partially expose the other
			// members when one record is corrupt or undecodable.
			return err
		}
		if !ok {
			continue
		}
		checkpoints[name] = cp
		anyV3 = anyV3 || cp.version == checkpointVersionV3
	}
	if anyV3 {
		return f.resumeDocumentCheckpoint(ctx, checkpoints)
	}
	return f.resumeLegacyCheckpoints(ctx)
}

func (f *Follower) resumeLegacyCheckpoints(ctx context.Context) error {
	var errs []error
	if f.cfg.Retention != nil {
		// External retention is one all-head transaction. Prove that one exact
		// current or crash-pending anchor covers the complete durable checkpoint
		// set before exposing any member of it. Per-head proofs could otherwise
		// accept a mixed Current/Pending set which no single recursive pin owns.
		var retained []replica.Head
		for _, name := range f.Names() {
			cp, ok, err := f.state.checkpoint(name)
			if err != nil {
				errs = append(errs, err)
				continue
			}
			if ok {
				retained = append(retained, replica.Head{
					Name: name, Root: cp.root, Manifest: cp.manifestTip, SyncedTo: cp.syncedTo,
				})
			}
		}
		if len(errs) > 0 {
			return errors.Join(errs...)
		}
		if len(retained) > 0 {
			if err := f.cfg.Retention.ProtectsAll(ctx, retained); err != nil {
				return fmt.Errorf("follow: resuming external archive generation: %w", err)
			}
		}
	}
	for _, name := range f.Names() {
		cp, ok, err := f.state.checkpoint(name)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if !ok {
			// No checkpoint. A root a pre-checkpoint follower persisted through the
			// server RootStore is NOT resumed: pairing it with independently stored
			// floors is exactly the split-generation state the safety boundary exploited. The head
			// stays unexposed until the next fresh verified publication commits its
			// first checkpoint (approved design: no migration synthesis). Zero
			// followers are deployed, so the wait costs nothing.
			if root, has, gerr := f.cfg.Roots.Get(ctx, name); gerr != nil {
				errs = append(errs, gerr)
			} else if has {
				f.log.Warn("followed head has a legacy root but no atomic checkpoint; not resuming it. It will be "+
					"exposed when the next verified publication commits its first checkpoint",
					"head", name, "legacy_root", root)
			}
			continue
		}
		expectedKind := f.expectedKind(name)
		if cp.kind != expectedKind {
			errs = append(errs, fmt.Errorf("follow: head %q checkpoint kind %q differs from configured expected kind %q; refusing to resume",
				name, cp.kind, expectedKind))
			continue
		}
		if cp.version == checkpointVersionV2 && expectedKind == server.UnfinalizedMutable {
			errs = append(errs, fmt.Errorf("follow: mutable head %q v2 checkpoint lacks proof-aware publication metadata and its exact signed handoff witness; refusing proofless resume until a fresh authenticated document writes v3", name))
			continue
		}
		if expectedKind == server.UnfinalizedMutable {
			var configuredAuthority [ed25519.PublicKeySize]byte
			copy(configuredAuthority[:], f.cfg.PubKey)
			if cp.authority != configuredAuthority {
				errs = append(errs, fmt.Errorf("follow: mutable head %q checkpoint authority %x differs from configured authority %x; refusing to resume stale provisional state",
					name, cp.authority[:8], configuredAuthority[:8]))
				continue
			}
		}
		if cp.revision != 0 {
			floor, ok, err := f.state.authorityFloor(cp.authority)
			if err != nil {
				errs = append(errs, err)
				continue
			}
			if !ok || floor.revision < cp.revision || (floor.revision == cp.revision && floor.digest != cp.digest) {
				errs = append(errs, fmt.Errorf("follow: head %q checkpoint publication revision %d is not covered by its durable authority floor; refusing an inconsistent resume",
					name, cp.revision))
				continue
			}
		} else if expectedKind == server.UnfinalizedMutable {
			errs = append(errs, fmt.Errorf("follow: head %q has a legacy checkpoint but is configured unfinalized-mutable; refusing to reinterpret finalized state", name))
			continue
		}
		head, err := f.load(ctx, name, cp.root)
		if err != nil {
			errs = append(errs, fmt.Errorf("follow: resuming head %q at %s: %w", name, cp.root, err))
			continue
		}

		// Coverage consistency (spec 11.3), the resume direction: the coverage the
		// durable root actually encodes, against the floor the checkpoint records.
		derived, covered := head.SyncedTo()
		repairCheckpoint := false
		switch {
		case cp.kind == server.UnfinalizedMutable && (!covered || derived != cp.syncedTo || head.Params().OriginSlot != cp.windowStart):
			f.cfg.Metrics.FollowRefusal(metrics.RefusalCoverageMismatch)
			errs = append(errs, fmt.Errorf("follow: mutable head %q checkpoint claims window [%d,%d], but root %s has origin %d and coverage %d (covered=%t); refusing to resume",
				name, cp.windowStart, cp.syncedTo, cp.root, head.Params().OriginSlot, derived, covered))
			continue
		case cp.kind == server.UnfinalizedMutable && (cp.syncedTo < cp.windowStart ||
			cp.syncedTo-cp.windowStart >= f.cfg.MaxMutableWindowSlots[name]):
			errs = append(errs, fmt.Errorf("follow: mutable head %q checkpoint window [%d,%d] exceeds its configured %d-slot maximum",
				name, cp.windowStart, cp.syncedTo, f.cfg.MaxMutableWindowSlots[name]))
			continue
		case cp.kind == server.UnfinalizedMutable && cp.manifestTip.Defined():
			errs = append(errs, fmt.Errorf("follow: mutable head %q checkpoint carries forbidden manifest %s", name, cp.manifestTip))
			continue
		case cp.kind == server.FinalizedMonotonic && (!covered || derived < cp.syncedTo):
			// The floor claims more coverage than the root encodes: an inconsistent
			// local state. Fail closed -- refuse to serve rather than serve a root that
			// sits below its own anti-regression floor. Never repaired down.
			f.cfg.Metrics.FollowRefusal(metrics.RefusalCoverageMismatch)
			f.log.Error("followed head NOT resumed: its checkpoint floor is above the coverage its root encodes, an "+
				"inconsistent local state; failing closed rather than serving a root below its floor",
				"head", name, "checkpoint_synced_to", cp.syncedTo, "root_coverage", derived, "covered", covered)
			errs = append(errs, fmt.Errorf("follow: head %q checkpoint floor %d is above its root's coverage %d "+
				"(covered=%t); refusing to resume an inconsistent generation", name, cp.syncedTo, derived, covered))
			continue
		case cp.kind == server.FinalizedMonotonic && derived > cp.syncedTo:
			// The root encodes more coverage than the floor records: safe to serve, but
			// the floor is repaired UP to it before we do, so a later document cannot
			// regress into the gap.
			f.log.Warn("repairing a followed head's checkpoint floor up to the coverage its root encodes",
				"head", name, "checkpoint_synced_to", cp.syncedTo, "root_coverage", derived)
			cp.syncedTo = derived
			repairCheckpoint = true
		}

		// A durable checkpoint may not have reached Registry/Reconciler before the
		// previous process crashed. Reuse admission's closure protocol so Resume
		// cannot publish that generation in the middle of a collector whose T0 pin
		// snapshot did not contain it. The normal startup path sees no active epoch
		// and records only a generation token; an exported/concurrent Resume during
		// collection walks and protects the exact retained closure.
		plan := adoptPlan{name: name, head: head, tip: cp.manifestTip, kind: cp.kind}
		if f.cfg.Retention == nil && f.hasCollectionGeneration() {
			if err := f.protectAdoptionClosure(ctx, &plan, false); err != nil {
				errs = append(errs, fmt.Errorf("follow: resuming head %q at %s: preparing publication closure: %w",
					name, cp.root, err))
				continue
			}
		}

		// Loading, coverage validation, and any active-epoch closure walk above
		// may fetch, so they stay outside Gate. The publication itself is one
		// gated commit: legacy stores prove their closure here; epoch stores check
		// the token; then anchors, checkpoint repair, exposure and reconciliation
		// become visible before the next collection cut.
		f.gate.Enter()
		if f.cfg.Retention == nil && !f.hasCollectionGeneration() {
			err = f.protectAdoptionClosure(ctx, &plan, true)
		}
		if err == nil && f.cfg.Retention == nil && plan.closureGeneration != f.collectionGeneration() {
			err = fmt.Errorf("collection generation changed from %d to %d before checkpoint resume; refusing publication and retrying",
				plan.closureGeneration, f.collectionGeneration())
		}
		if err == nil {
			err = f.touchGeneration(ctx, name, head.Root(), cp.manifestTip)
		}
		if err == nil && repairCheckpoint {
			err = f.state.putCheckpoint(name, cp)
			if err != nil {
				err = fmt.Errorf("follow: repairing head %q checkpoint floor: %w", name, err)
			}
		}
		if err == nil {
			err = f.expose(ctx, name, head, cp.manifestTip, cp.kind, nil)
		}
		f.gate.Leave()
		if err != nil {
			errs = append(errs, fmt.Errorf("follow: resuming head %q at %s: %w", name, cp.root, err))
			continue
		}
		if f.cfg.Staging != nil && len(plan.staged) > 0 {
			if err := f.cfg.Staging.Drop(ctx, plan.staged); err != nil {
				f.log.Error("dropping resume publication staging pins", "head", name, "err", err)
			}
		}
		f.log.Info("followed head resumed", "head", name, "root", cp.root, "synced_to", cp.syncedTo,
			"manifest", cidOrNone(cp.manifestTip))
	}
	return errors.Join(errs...)
}

// resumeDocumentCheckpoint restores one v3 authenticated document generation.
// Every configured name must have one record from the same authority/revision/
// digest; a missing, legacy, corrupt, or mismatched member suppresses the entire
// generation. Only after that proof is complete do we load/protect selected DAGs
// and expose all selections/withdrawals through the same AdoptBatch used by Poll.
func (f *Follower) resumeDocumentCheckpoint(ctx context.Context, checkpoints map[string]checkpoint) error {
	names := f.Names()
	if len(checkpoints) != len(names) {
		return fmt.Errorf("follow: v3 checkpoint generation has %d of %d configured head records; refusing partial resume",
			len(checkpoints), len(names))
	}

	var publication authorityFloor
	var updatedAt time.Time
	var network string
	authenticated := make(map[string]*server.HeadEntry)
	addAuthenticated := func(entry *server.HeadEntry) error {
		if entry == nil {
			return nil
		}
		if old := authenticated[entry.Name]; old != nil && !headEntriesEqual(old, entry) {
			return fmt.Errorf("follow: v3 checkpoint generation composes conflicting authenticated lines for head %q", entry.Name)
		}
		authenticated[entry.Name] = entry
		return nil
	}

	for i, name := range names {
		cp := checkpoints[name]
		if cp.version != checkpointVersionV3 {
			return fmt.Errorf("follow: v3 checkpoint generation mixes head %q version %d; refusing partial resume", name, cp.version)
		}
		if i == 0 {
			publication = authorityFloor{authority: cp.authority, revision: cp.revision, digest: cp.digest}
			updatedAt = cp.updatedAt
			network = cp.net
		} else if cp.authority != publication.authority || cp.revision != publication.revision || cp.digest != publication.digest || !cp.updatedAt.Equal(updatedAt) {
			return fmt.Errorf("follow: v3 checkpoint for head %q belongs to a different authenticated document generation", name)
		}
		if cp.net != network {
			return fmt.Errorf("follow: v3 checkpoint for head %q belongs to network %q, group network is %q", name, cp.net, network)
		}
		if cp.published != nil && cp.published.Name != name {
			return fmt.Errorf("follow: v3 checkpoint for head %q retains line %q", name, cp.published.Name)
		}
		if cp.published != nil && cp.kind != f.expectedKind(name) {
			return fmt.Errorf("follow: head %q checkpoint kind %q differs from configured expected kind %q", name, cp.kind, f.expectedKind(name))
		}
		if !cp.selected {
			continue // retained lines on tombstones are baselines, not this document's selection set.
		}
		if cp.published == nil {
			return fmt.Errorf("follow: selected v3 checkpoint for head %q has no authenticated line", name)
		}
		if err := addAuthenticated(cp.published); err != nil {
			return err
		}
		if cp.kind == server.UnfinalizedMutable {
			if cp.handoff == nil || cp.published.HandoffHead != f.cfg.ExpectedHandoffs[name] {
				return fmt.Errorf("follow: mutable checkpoint %q does not carry configured handoff %q", name, f.cfg.ExpectedHandoffs[name])
			}
			if err := addAuthenticated(cp.handoff); err != nil {
				return err
			}
			if handoffCP, configured := checkpoints[cp.handoff.Name]; configured &&
				(!handoffCP.selected || !headEntriesEqual(handoffCP.published, cp.handoff)) {
				return fmt.Errorf("follow: mutable checkpoint %q handoff witness does not equal selected configured head %q", name, cp.handoff.Name)
			}
		}
	}

	// The authority floor and configured/delegated signer must authorize exactly
	// the same generation. A greater floor with older checkpoints is a torn local
	// state: every accepted v3 document rewrites every configured record in one
	// Pebble batch, so there is no valid reason to resume it partially.
	floor, ok, err := f.state.authorityFloor(publication.authority)
	if err != nil {
		return err
	}
	if !ok || floor.revision != publication.revision || floor.digest != publication.digest {
		return fmt.Errorf("follow: v3 checkpoint revision %d is not the exact durable authority floor", publication.revision)
	}
	var expectedAuthority [ed25519.PublicKeySize]byte
	if len(f.cfg.PubKey) == ed25519.PublicKeySize {
		copy(expectedAuthority[:], f.cfg.PubKey)
	} else {
		delegated, ok, err := f.state.delegation()
		if err != nil {
			return err
		}
		if !ok || len(delegated.pubkey) != ed25519.PublicKeySize {
			return errors.New("follow: v3 checkpoint has no durable configured or DNSLink-delegated authority")
		}
		copy(expectedAuthority[:], delegated.pubkey)
	}
	if publication.authority != expectedAuthority {
		return fmt.Errorf("follow: v3 checkpoint authority %x differs from configured authority %x",
			publication.authority[:8], expectedAuthority[:8])
	}
	if network != f.cfg.Net {
		return fmt.Errorf("follow: v3 checkpoint network %q differs from configured network %q", network, f.cfg.Net)
	}

	// Revalidate the selected signed subset as a document contract. Metadata-only
	// handoff witnesses are included even when this replica does not select them.
	entries := make([]server.HeadEntry, 0, len(authenticated))
	for _, name := range slices.Sorted(maps.Keys(authenticated)) {
		entries = append(entries, *authenticated[name])
	}
	revision := publication.revision
	proofDoc := server.Doc{Unsigned: server.Unsigned{V: server.DocVersion, Net: network, Heads: entries, Revision: &revision}, Pubkey: "checkpoint-v3", Signature: "checkpoint-v3"}
	if err := proofDoc.ValidateContract(); err != nil {
		return fmt.Errorf("follow: v3 checkpoint generation has an invalid authenticated contract: %w", err)
	}
	if err := f.validateOverlayHandoffs(proofDoc, true); err != nil {
		return fmt.Errorf("follow: v3 checkpoint generation has an invalid retained live overlay: %w", err)
	}

	if f.cfg.Retention != nil {
		retained := make([]replica.Head, 0, len(names))
		for _, name := range names {
			cp := checkpoints[name]
			if cp.selected {
				retained = append(retained, replica.Head{Name: name, Root: cp.root, Manifest: cp.manifestTip, SyncedTo: cp.syncedTo})
			}
		}
		if err := f.cfg.Retention.ProtectsAll(ctx, retained); err != nil {
			return fmt.Errorf("follow: resuming external archive generation: %w", err)
		}
	}

	plans := make([]adoptPlan, 0, len(names))
	for _, name := range names {
		cp := checkpoints[name]
		if !cp.selected {
			plans = append(plans, adoptPlan{name: name, kind: f.expectedKind(name), cp: cp, withdraw: true})
			continue
		}
		head, err := f.load(ctx, name, cp.root)
		if err != nil {
			return fmt.Errorf("follow: resuming head %q at %s: %w", name, cp.root, err)
		}
		derived, covered := head.SyncedTo()
		if !covered || derived != cp.syncedTo {
			return fmt.Errorf("follow: v3 checkpoint head %q claims coverage %d but root %s covers %d (covered=%t)",
				name, cp.syncedTo, cp.root, derived, covered)
		}
		if cp.kind == server.UnfinalizedMutable && (head.Params().OriginSlot != cp.windowStart ||
			cp.syncedTo < cp.windowStart || cp.syncedTo-cp.windowStart >= f.cfg.MaxMutableWindowSlots[name]) {
			return fmt.Errorf("follow: v3 mutable checkpoint %q window [%d,%d] is invalid for loaded root/configuration",
				name, cp.windowStart, cp.syncedTo)
		}
		if err := validateRegistryAdopt(f.cfg.Registry, head, cp.manifestTip, cp.kind); err != nil {
			return fmt.Errorf("follow: resuming head %q: %w", name, err)
		}
		plan := adoptPlan{name: name, head: head, entry: *cp.published, tip: cp.manifestTip, kind: cp.kind, cp: cp}
		if f.cfg.Retention == nil && f.hasCollectionGeneration() {
			if err := f.protectAdoptionClosure(ctx, &plan, false); err != nil {
				return fmt.Errorf("follow: resuming head %q: preparing publication closure: %w", name, err)
			}
		}
		plans = append(plans, plan)
	}

	document := &resolved{doc: proofDoc, updatedAt: updatedAt, revisioned: true, authority: publication.authority,
		revision: publication.revision, digest: publication.digest}
	if errs := f.commitPlans(ctx, plans, document); len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

// pollSingularAdmission resolves, verifies, and atomically adopts one
// singular-source publication. Retained-closure sync is deliberately absent:
// Poll adds it synchronously for direct callers, while RunAfterResume wakes the
// bounded background worker so a long closure walk cannot stall the next
// authenticated publication.
func (f *Follower) pollSingularAdmission(ctx context.Context) error {
	var errs []error
	doc, obs, err := f.resolve(ctx)
	var resolvedEquivocation *authorityEquivocationError
	if err != nil {
		errs = append(errs, err)
		errors.As(err, &resolvedEquivocation)
	}
	if doc != nil || obs.hasIPNSSeq || resolvedEquivocation != nil {
		// The transition lock spans only admit, not the resolve above (pure network,
		// touches no state) or the fetch pass below (commits no checkpoint): it is the
		// admit-and-checkpoint work that must not interleave with a concurrent Resume or
		// another Poll. The replay
		// floors resolve checked are re-validated inside admit, under this lock, because
		// a concurrent poll may have moved them since. admit runs even with
		// no document candidate, to raise the replay floor from an authenticated channel
		// observation.
		if afterResolveHook != nil {
			afterResolveHook()
		}
		f.transition.Lock()
		if resolvedEquivocation != nil {
			f.quarantineMutableEquivocationLocked(resolvedEquivocation)
		}
		err := f.admit(ctx, doc, obs)
		var admittedEquivocation *authorityEquivocationError
		if errors.As(err, &admittedEquivocation) {
			f.quarantineMutableEquivocationLocked(admittedEquivocation)
		}
		f.transition.Unlock()
		if err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// afterResolveHook, when set, is called in Poll after resolve returns and before
// the transition lock is taken. It exists only for the concurrent-poll regression
// tests (see export_test.go): a document that resolved against the old floors can
// be held here while a newer poll commits, so the stale poll then enters the locked
// admission and the re-validation of the transition invariant can be observed deterministically,
// without sleeps. Nil in production.
var afterResolveHook func()

// betweenPhasesHook, when set, is called inside admit after phase 1 (preflight) and
// before phase 2 (commit), while the transition lock is held. It exists only for the
// quarantine-race regression tests: a test can pause
// a poll here, with its plans decided, and race a quarantine against the commit to
// prove the two are serialized. Nil in production.
var betweenPhasesHook func()

// beforeExposeHook, when set, is called inside commitEntry after the checkpoint is
// durable and before expose runs, while the transition lock is held. It lets the
// regression test hold a poll at exactly the checkpoint-written-but-not-yet-exposed
// point and race a quarantine against the exposure, proving the two cannot tear a
// generation (checkpoint written but mirror not). Nil in production.
var beforeExposeHook func()

// beforeAdmissionCommitHook injects a failure immediately before the one Pebble
// durability barrier. It is nil in production and exists for crash-boundary
// tests which prove checkpoints, mirrors, reconciliation and serving all remain
// on the old generation when that commit fails.
var beforeAdmissionCommitHook func() error

// admit performs a poll's locked admission: it raises the replay floor from the
// authenticated channel observation, then, if a document candidate survived
// resolution, admits it as a WHOLE and records its freshness.
//
// # The channel observation is committed independent of any document
//
// The IPNS replay floor is raised from obs -- an authenticated record's sequence --
// as the first thing under the lock, whether or not a document candidate exists. A
// record that authenticated against the configured key but named a stale document
// still raised the floor; discarding its sequence with the refused document would
// leave a replay window a later intermediate record slips through. doc may be nil (an
// obs-only poll); everything below is skipped then.
//
// # Document-level admission
//
// A validly-signed document can still be self-contradictory: one head's root and
// claimed coverage disagree, another's manifest tip does not descend from the one
// held, a head has regressed its synced_to. The document is admitted or refused as a
// WHOLE, never head by head. An earlier design checkpointed each head as it passed,
// so a document with head A valid and head B inconsistent committed A's generation
// and raised the global freshness floor before B could refuse -- pinning freshness
// for a document this node serves nothing coherent from, and suppressing a later
// admissible repair for B. So admission is two phases: phase 1 preflights EVERY
// followed entry with ZERO writes, and only if all of them pass does phase 2 commit
// the complete adoption plan in one synced batch and expose one prospective serving
// snapshot. One inconsistent entry refuses the document, leaving every checkpoint,
// compatibility mirror, and ordering floor untouched.
//
// # Replay floors re-validated under the lock
//
// resolve checked the document's updated_at and the IPNS record's sequence against
// the floors for liveness, but outside the transition lock a concurrent poll may
// have raised either since. This runs under the lock (Poll holds it across the
// call), so the floors are re-read and enforced here, atomically with the writes
// they gate: an older document admitted now would overwrite a newer per-head
// checkpoint or lower a floor a newer poll raised.
//
// The document ordering floor is part of that same batch even when no selected head
// changed. A refused document or failed durability barrier therefore raises no floor,
// while an authenticated no-op document still closes its replay window atomically.
func (f *Follower) admit(ctx context.Context, doc *resolved, obs channelObs) error {
	// The IPNS replay floor, raised as one guarded RMW under the lock, from the
	// authenticated channel observation and INDEPENDENT of any document.
	// The floor is a monotonic max fact about the channel: it rises for any record that
	// authenticated this poll, even one whose document was freshness-refused, lost the
	// freshness contest, or is absent. A record a newer poll has already lifted the
	// floor past is a replay -- it neither lowers the stored floor nor, when the winning
	// document is that record, is admitted.
	if obs.hasIPNSSeq {
		floor, ok, err := f.state.ipnsSeq(obs.ipnsName, f.dnsLink == "")
		if err != nil {
			return err
		}
		switch {
		case ok && obs.ipnsSeq < floor:
			if doc != nil && doc.source == metrics.ChannelIPNS {
				f.cfg.Metrics.FollowRefusal(metrics.RefusalIPNSSeqFloor)
				return fmt.Errorf("follow: IPNS record for %s has sequence %d, below the accepted floor %d; refusing a "+
					"replayed record whose document a newer poll superseded",
					obs.ipnsName, obs.ipnsSeq, floor)
			}
			// The winning document is HTTPS, or there is no document; the stale IPNS
			// record simply does not raise the floor.
		default:
			if err := f.state.setIPNSSeq(obs.ipnsName, obs.ipnsSeq, f.dnsLink == ""); err != nil {
				return err
			}
		}
	}

	if doc == nil {
		return nil // an obs-only poll: the channel observation was committed, nothing to adopt.
	}

	// Re-validate the ordering floor under the transition lock. Resolution's
	// check was only an early liveness filter; another poll may have committed a
	// higher revision (or legacy timestamp) while this one was on the network.
	if err := f.freshnessRefusal(doc); err != nil {
		if !doc.revisioned {
			f.cfg.Metrics.FollowRefusal(metrics.RefusalUpdatedAtFloor)
		}
		return err
	}
	if err := f.validateOverlayHandoffs(doc.doc, true); err != nil {
		return err
	}

	f.dial(ctx, doc.doc.Multiaddrs)

	byName := make(map[string]server.HeadEntry, len(doc.doc.Heads))
	for _, e := range doc.doc.Heads {
		byName[e.Name] = e
	}

	// Phase 1: preflight every followed entry, no writes. Any refusal refuses the
	// whole document -- no checkpoint committed, no floor raised.
	var (
		plans []adoptPlan
		errs  []error
	)
	for _, name := range f.Names() {
		entry, ok := byName[name]
		if !ok {
			if !doc.revisioned {
				// Legacy documents predate authenticated selection sets. Preserve
				// their historical behavior: omission means "no update", not an
				// instruction to unserve a previously selected head.
				f.log.Warn("followed head is not in the publication document",
					"head", name, "source", doc.source, "published", len(byName))
				continue
			}
			plan, err := f.preflightWithdrawal(ctx, name, nil, doc)
			if err != nil {
				errs = append(errs, fmt.Errorf("follow: withdrawing omitted head %q: %w", name, err))
			} else {
				plans = append(plans, plan)
			}
			continue
		}
		if doc.revisioned && entry.SyncedTo == nil {
			plan, err := f.preflightWithdrawal(ctx, name, &entry, doc)
			if err != nil {
				errs = append(errs, fmt.Errorf("follow: admitting empty head %q: %w", name, err))
			} else {
				plans = append(plans, plan)
			}
			continue
		}
		plan, err := f.preflightEntry(ctx, entry, doc)
		if err != nil {
			errs = append(errs, fmt.Errorf("follow: adopting head %q: %w", name, err))
		} else {
			plans = append(plans, plan)
		}
	}
	if len(errs) > 0 {
		// A refused document: leave every checkpoint and the global floor untouched
		//. The IPNS sequence floor above may still have risen -- it is a
		// fact about the channel, independent of whether this document was admitted.
		return errors.Join(errs...)
	}

	// Establish the complete retained closure before publication. This may
	// fetch, so it remains phase-1 work outside Gate; each plan carries a
	// monotonic collection-generation proof into the atomic commit below.
	if f.cfg.Retention == nil && f.hasCollectionGeneration() {
		for i := range plans {
			if err := f.protectAdoptionClosure(ctx, &plans[i], false); err != nil {
				errs = append(errs, fmt.Errorf("follow: preparing head %q closure for publication: %w", plans[i].name, err))
			}
		}
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}

	// The external archive store is the durability barrier in replica mode.
	// Build the generation which will remain served after this admission (fresh
	// document entries plus prior checkpoints for selected heads the document
	// leaves absent or empty), and finish its recursive pin before the first
	// checkpoint write. Prepare persists intent before mutating Kubo and retains
	// the old committed generation throughout.
	var retained *replica.Generation
	if f.cfg.Retention != nil {
		generation, err := f.retentionGeneration(doc.updatedAt, byName, doc.revisioned)
		if err != nil {
			return err
		}
		if err := f.cfg.Retention.Prepare(ctx, generation); err != nil {
			return fmt.Errorf("follow: preparing external archive generation: %w", err)
		}
		retained = &generation
	}

	if betweenPhasesHook != nil {
		betweenPhasesHook()
	}

	// Phase 2: every entry passed -- validate every closure proof and commit all
	// checkpoints/exposures behind one Gate acquisition.
	errs = append(errs, f.commitPlans(ctx, plans, doc)...)
	if len(errs) == 0 && retained != nil {
		// The follower checkpoint is already durable here. A failure leaves the
		// candidate as a safe pending pin which Resume accepts and the next poll
		// retries; it never exposes an unprotected checkpoint. AdoptBatch has also
		// made the new serving snapshot visible. Drain the shared registry-reader
		// gate now: every request which could have selected the retired snapshot
		// must finish materializing it before Commit is allowed to unpin its Kubo
		// anchor. The barrier reopens before the remote RPC, so new readers of the
		// already-visible generation remain concurrent with slow Kubo cleanup.
		f.gate.Barrier()
		if err := f.cfg.Retention.Commit(ctx, *retained); err != nil {
			errs = append(errs, fmt.Errorf("follow: committing external archive generation: %w", err))
		}
	}
	if len(errs) == 0 {
		if err := f.notifyAdmittedDocument(doc, false); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// notifyAdmittedDocument is the post-durability handoff to the p2p serving
// layer. admit is always called with transition held, so callback invocation
// and the per-process success marker are serialized with every competing Poll
// and Resume without another lock.
func (f *Follower) notifyAdmittedDocument(doc *resolved, servingChanged bool) error {
	if f.cfg.OnAdmittedDocument == nil && f.cfg.OnAdmittedSourceDocument == nil {
		return nil
	}
	if doc == nil || doc.block == nil || !doc.block.Cid().Defined() {
		return errors.New("follow: admitted publication document has no exact source block")
	}
	key := ""
	if doc.runtimeSource != nil {
		key = doc.runtimeSource.cfg.ID
	}
	if current := f.admittedDocuments[key]; !servingChanged && current.Defined() && current.Equals(doc.block.Cid()) {
		return nil
	}
	if servingChanged {
		// A source document can be admitted while only a subset of its heads is
		// serviceable. If an exact-CID retry later makes another head visible, its
		// post-durability callback must run again so that head acquires the exact
		// document pointer. Clear the old success marker before invoking the
		// callback: on failure, an otherwise no-op later poll must retry it too.
		delete(f.admittedDocuments, key)
	}
	var err error
	if doc.runtimeSource != nil {
		if f.cfg.OnAdmittedSourceDocument == nil {
			return nil
		}
		allowed := append([]string(nil), doc.runtimeSource.cfg.AllowedHeads...)
		err = f.cfg.OnAdmittedSourceDocument(doc.block, doc.doc, allowed)
	} else if f.cfg.OnAdmittedDocument != nil {
		err = f.cfg.OnAdmittedDocument(doc.block, doc.doc)
	}
	if err != nil {
		return fmt.Errorf("follow: retaining admitted publication document %s: %w", doc.block.Cid(), err)
	}
	if f.admittedDocuments == nil {
		f.admittedDocuments = make(map[string]cid.Cid)
	}
	f.admittedDocuments[key] = doc.block.Cid()
	return nil
}

func (f *Follower) retentionGeneration(updatedAt time.Time, entries map[string]server.HeadEntry, revisioned bool) (replica.Generation, error) {
	generation := replica.Generation{UpdatedAt: updatedAt}
	for _, name := range f.Names() {
		if entry, ok := entries[name]; ok && entry.SyncedTo != nil {
			root, err := cid.Decode(entry.Root)
			if err != nil {
				return replica.Generation{}, fmt.Errorf("follow: external generation head %q root: %w", name, err)
			}
			tip, err := parseManifestTip(entry)
			if err != nil {
				return replica.Generation{}, err
			}
			generation.Heads = append(generation.Heads, replica.Head{
				Name: name, Root: root, Manifest: tip, SyncedTo: *entry.SyncedTo,
			})
			continue
		}
		// A revisioned document authenticates the complete selection set. Missing
		// or explicitly empty configured heads are withdrawals and therefore do
		// not belong to the candidate generation. Legacy documents predate that
		// contract: their omission remains "no update" and keeps the prior anchor.
		if revisioned {
			continue
		}
		checkpoint, ok, err := f.state.checkpoint(name)
		if err != nil {
			return replica.Generation{}, err
		}
		if ok && checkpoint.selected && checkpoint.root.Defined() {
			generation.Heads = append(generation.Heads, replica.Head{
				Name: name, Root: checkpoint.root, Manifest: checkpoint.manifestTip, SyncedTo: checkpoint.syncedTo,
			})
		}
	}
	return generation, nil
}

// adoptPlan is what phase 1 (preflightEntry) decided for one followed head, carried
// into phase 2 (commitEntry). head is nil for an entry there is nothing to commit --
// an empty head, or one already served at this exact generation; otherwise head is
// the loaded, validated engine to expose, cp is the generation to checkpoint, and
// writeCheckpoint says whether a checkpoint must be written (false when one already
// records this exact generation -- a repeated legacy claim or an idempotent
// same-revision/same-digest publication).
type adoptPlan struct {
	name            string
	head            *archive.Head
	entry           server.HeadEntry
	tip             cid.Cid
	kind            server.HeadKind
	cp              checkpoint
	writeCheckpoint bool
	// withdraw is an authenticated revisioned-document omission (or a first-use
	// explicit empty finalized line). It removes the served registration and
	// compatibility mirrors, installs a desired-empty reconciler tombstone, and
	// retains a v3 checkpoint tombstone so the signer-local revision and any
	// finalized anti-regression baseline survive restart.
	withdraw bool
	// closureGeneration proves that protectAdoptionClosure completed without a
	// GC cut. staged pins cover blocks fetched by that pass until commit makes
	// the generation visible to reconciliation.
	closureGeneration uint64
	staged            []cid.Cid
}

// preflightWithdrawal turns a revisioned document's complete selection set into
// one durable tombstone. A nil entry is an authenticated omission. A non-nil
// entry is an explicitly empty finalized head and is accepted only before this
// follower has ever selected coverage for that name: publishing null coverage
// after a covered finalized generation is a regression, not an alternate way to
// withdraw it. Omission remains the deliberate withdrawal spelling.
func (f *Follower) preflightWithdrawal(ctx context.Context, name string, entry *server.HeadEntry, document *resolved) (adoptPlan, error) {
	if !document.revisioned {
		return adoptPlan{}, errors.New("legacy documents do not authenticate withdrawals")
	}
	if entry != nil {
		if entry.Name != name {
			return adoptPlan{}, fmt.Errorf("empty publication entry names %q, want %q", entry.Name, name)
		}
		if entry.EffectiveKind() != server.FinalizedMonotonic || entry.SyncedTo != nil {
			return adoptPlan{}, fmt.Errorf("head %q is not an explicitly empty finalized entry", name)
		}
		if _, err := cid.Decode(entry.Root); err != nil {
			return adoptPlan{}, fmt.Errorf("empty head %q root %q is not a CID: %w", name, entry.Root, err)
		}
	}

	prior, hasPrior, err := f.state.checkpoint(name)
	if err != nil {
		return adoptPlan{}, err
	}
	if entry != nil && hasPrior && ((prior.version != checkpointVersionV3 && prior.version != checkpointVersionV4) || prior.published != nil) {
		return adoptPlan{}, fmt.Errorf("finalized head %q previously covered slot %d; an explicit null synced_to would regress it (omit the head to withdraw it)",
			name, prior.syncedTo)
	}

	var published, handoff, overlay *server.HeadEntry
	if hasPrior {
		switch {
		case prior.version == checkpointVersionV3 || prior.version == checkpointVersionV4:
			published, handoff, overlay = prior.published, prior.handoff, prior.overlay
		case prior.kind == server.FinalizedMonotonic:
			// v1/v2 did not retain the exact signed line. Reconstruct the exact
			// finalized serving facts from the authenticated checkpoint root so
			// its monotonic floor survives conversion to a v3 tombstone.
			head, loadErr := f.load(ctx, name, prior.root)
			if loadErr != nil {
				return adoptPlan{}, fmt.Errorf("loading prior finalized generation for withdrawal: %w", loadErr)
			}
			derived, covered := head.SyncedTo()
			if !covered || derived < prior.syncedTo {
				return adoptPlan{}, fmt.Errorf("prior finalized checkpoint for %q claims %d but root %s covers only %d (covered=%t)",
					name, prior.syncedTo, prior.root, derived, covered)
			}
			line := finalizedPublicationEntry(head, prior.manifestTip)
			published = &line
		case prior.kind == server.UnfinalizedMutable:
			// v2 mutable checkpoints intentionally contain no source/handoff
			// proof, so they cannot be converted into a valid exact v3 retained
			// line. Dropping the line would also drop the immutable seg/fanout
			// baseline across omit -> reappear. Fail closed until one fresh
			// selected document upgrades it to v3; no deployed v2 mutable state
			// is silently reinterpreted.
			return adoptPlan{}, fmt.Errorf("mutable head %q v2 checkpoint lacks proof-aware publication metadata; admit one fresh selected generation before withdrawing it", name)
		default:
			return adoptPlan{}, fmt.Errorf("checkpoint for %q has unknown kind %q", name, prior.kind)
		}
	}

	if hasPrior && (prior.version == checkpointVersionV3 || prior.version == checkpointVersionV4) && prior.net != document.doc.Net {
		return adoptPlan{}, fmt.Errorf("head %q checkpoint network %q differs from authenticated document network %q", name, prior.net, document.doc.Net)
	}
	if hasPrior && document.runtimeSource != nil && f.expectedKind(name) == server.UnfinalizedMutable {
		switch prior.version {
		case checkpointVersionV4:
			if prior.sourceID != document.runtimeSource.cfg.ID {
				return adoptPlan{}, fmt.Errorf("mutable head %q is durably bound to source %q, refusing withdrawal from source %q",
					name, prior.sourceID, document.runtimeSource.cfg.ID)
			}
		case checkpointVersionV2, checkpointVersionV3:
			if prior.authority != document.authority {
				return adoptPlan{}, fmt.Errorf("mutable head %q legacy checkpoint authority differs from source %q", name, document.runtimeSource.cfg.ID)
			}
		}
	}
	var cp checkpoint
	if document.runtimeSource != nil {
		cp, err = makeCheckpointV4(document.doc.Net, document.runtimeSource.ref.archiveID, document.runtimeSource.cfg.ID,
			false, published, handoff, overlay, document.updatedAt, *document.publicationFloor())
	} else {
		cp, err = makeCheckpointV3(document.doc.Net, false, published, handoff, document.updatedAt, *document.publicationFloor())
	}
	if err != nil {
		return adoptPlan{}, err
	}
	return adoptPlan{name: name, kind: f.expectedKind(name), cp: cp, writeCheckpoint: true, withdraw: true}, nil
}

func finalizedPublicationEntry(head *archive.Head, manifestTip cid.Cid) server.HeadEntry {
	info := head.Info()
	entry := server.HeadEntry{
		Name: info.Name, Root: info.Root.String(), OriginSlot: info.OriginSlot, SyncedTo: info.SyncedTo,
		SegBits: info.SegBits, FanoutBits: info.FanoutBits, DirDepth: info.DirDepth,
	}
	if manifestTip.Defined() {
		entry.Manifest = manifestTip.String()
	}
	return entry
}

// preflightEntry applies the entry's authenticated ordering contract with ZERO
// writes, returning the phase-2 plan or a refusal. It is
// the read half of what was one adoptEntry: quarantine, the floors, the
// manifest-ancestry walk, the already-serving short-circuit, the head load and the
// coverage and bounded structural checks -- everything that decides admissibility,
// and nothing that mutates durable selection state. The Head, directory pages, and
// Segment index blocks are fetched and validated here; blob bodies remain lazy.
// Those content-addressed block fetches may populate the local blockstore, but no
// checkpoint, retention intent, pin ledger or floor is changed for a rejected DAG.
func (f *Follower) preflightEntry(ctx context.Context, e server.HeadEntry, document *resolved) (adoptPlan, error) {
	return f.preflightEntryWithHead(ctx, e, document, nil)
}

// preflightEntryWithHead accepts a Head which has already crossed the complete
// bounded-DAG admission boundary. Source-set arbitration uses it to carry the
// exact proof selected from an authenticated observation into adoption instead
// of re-walking the same potentially large index. Singular publication passes
// nil and retains the ordinary load path.
func (f *Follower) preflightEntryWithHead(
	ctx context.Context,
	e server.HeadEntry,
	document *resolved,
	admitted *archive.Head,
) (adoptPlan, error) {
	kind := e.EffectiveKind()
	// Snapshot the head's in-memory generation under f.mu: quarantined is written by
	// the fetch pass's quarantine, which runs outside the transition lock, and adopted
	// and manifestTip by expose; reading them unlocked would race those (the same
	// discipline sync uses).
	f.mu.Lock()
	hs, known := f.heads[e.Name]
	var (
		quarantined       bool
		adopted, adoptTip cid.Cid
	)
	if known {
		quarantined = hs.quarantined
		adopted, adoptTip = hs.adopted, hs.manifestTip
	}
	f.mu.Unlock()
	if !known {
		return adoptPlan{}, fmt.Errorf("head %q is not followed by this node", e.Name)
	}
	if quarantined {
		// A quarantine is one-way for the life of the process (spec 11.4): a writer
		// still publishing this head has it refused every poll, and the whole document
		// with it. Caught here, before any write, so the refused generation
		// never reaches a checkpoint.
		f.cfg.Metrics.FollowRefusal(metrics.RefusalQuarantined)
		return adoptPlan{}, fmt.Errorf("%w: head %q", server.ErrQuarantined, e.Name)
	}

	root, err := cid.Decode(e.Root)
	if err != nil {
		return adoptPlan{}, fmt.Errorf("root %q is not a CID: %w", e.Root, err)
	}
	if e.SyncedTo == nil {
		// An empty head is not adoptable and not a refusal: there is nothing to serve
		// and nothing to fetch, so it contributes no plan and does not refuse the
		// document.
		f.log.Info("followed head is published empty", "head", e.Name, "root", root)
		return adoptPlan{name: e.Name, entry: e, kind: kind}, nil
	}
	tip, err := parseManifestTip(e)
	if err != nil {
		return adoptPlan{}, err
	}

	// The floors: the checkpoint's once one exists, else the retained legacy facts
	// a pre-checkpoint follower may have left. The checkpoint supersedes them, so
	// the two are never combined -- and its synced_to is >= any legacy floor by
	// construction, since the first checkpoint had to clear the legacy floor to be
	// written at all.
	cp, hasCP, err := f.state.checkpoint(e.Name)
	if err != nil {
		return adoptPlan{}, err
	}
	if hasCP && ((cp.version != checkpointVersionV3 && cp.version != checkpointVersionV4) || cp.published != nil) && cp.kind != kind {
		return adoptPlan{}, fmt.Errorf("checkpoint kind %q differs from the authenticated document kind %q", cp.kind, kind)
	}
	if hasCP && (cp.version == checkpointVersionV3 || cp.version == checkpointVersionV4) {
		if cp.net != document.doc.Net {
			return adoptPlan{}, fmt.Errorf("head %q checkpoint network %q differs from authenticated document network %q", e.Name, cp.net, document.doc.Net)
		}
	}
	if hasCP && (cp.version == checkpointVersionV3 || cp.version == checkpointVersionV4) && cp.published != nil {
		if err := validateCheckpointBaseline(*cp.published, e); err != nil {
			return adoptPlan{}, err
		}
	}
	floor, hasFloor, mfloor, hasMFloor, err := f.floors(e.Name, cp, hasCP)
	if err != nil {
		return adoptPlan{}, err
	}

	if kind == server.FinalizedMonotonic && hasFloor && *e.SyncedTo < floor {
		// Spec 11.3's first rule. This is the withheld-update attack, or a writer
		// that was rebuilt from behind, and the two are indistinguishable from here:
		// either way this node has already served slots this document says are not
		// archived, and adopting it would take them back.
		//
		// Counted here rather than by the poll: the document resolved cleanly, so
		// FollowPoll already recorded an "ok" for it, and this per-head defence is
		// the thing that would otherwise be invisible.
		//
		// The floor lag is the width of the divergence window this refusal opens:
		// for the slots between the writer's lower synced_to and this floor, the
		// follower goes on serving its last good root as covered while the writer
		// answers differently, until the writer's coverage climbs back past the
		// floor (spec 11.3's bounded, self-healing tradeoff). The gauge and the log
		// are what make that window visible; without them an operator can only find
		// it by comparing writer and follower by hand.
		lag := floor - *e.SyncedTo
		f.cfg.Metrics.FollowRefusal(metrics.RefusalSyncedToFloor)
		f.cfg.Metrics.FollowSyncedToFloorLag(e.Name, lag)
		f.log.Warn("refused a followed head on the synced_to floor: the writer has retracted below what this node "+
			"already served, so it keeps serving its last good root for the slots between and diverges from the writer "+
			"there until the writer's coverage passes the floor again (spec 11.3)",
			"head", e.Name, "floor", floor, "published_synced_to", *e.SyncedTo, "lag", lag)
		return adoptPlan{}, fmt.Errorf("published synced_to %d is below the adopted floor %d; refusing to regress head "+
			"%q (spec 11.3)", *e.SyncedTo, floor, e.Name)
	}
	// The floor is satisfied -- either this head has no floor yet or the writer's
	// coverage is at or above it -- so any divergence window this head had open is
	// closed. Reset the lag gauge here, about the floor alone, rather than after
	// the adoption below.
	if kind == server.FinalizedMonotonic {
		f.cfg.Metrics.FollowSyncedToFloorLag(e.Name, 0)
	}

	// The manifest-ancestry floor (spec 11.3), before the adoption it gates: a
	// published tip that does not descend from the one this node holds is a
	// rewritten filter history, refused exactly as a synced_to regression is. The
	// walk needs the new chain, which it fetches as it goes; doing it here rather
	// than in the fetch pass is what makes the refusal actually block the adoption,
	// so the head goes on serving its last good state.
	if kind == server.FinalizedMonotonic {
		if err := f.checkManifestAncestryWithPointer(ctx, e.Name, tip, mfloor, hasMFloor); err != nil {
			return adoptPlan{}, err
		}
	}
	// A defined manifest tip is one of the generation anchors commit publishes.
	// The ancestry walk already fetches a changed tip when a floor exists, but a
	// first or unchanged tip takes a cheaper path above. Make it local in every
	// case while network work is still allowed; commit will re-touch it through
	// cfg.Local under the GC gate and abort if it disappeared in between.
	if tip.Defined() {
		if err := f.getManifestTipWithPointer(ctx, tip); err != nil {
			return adoptPlan{}, fmt.Errorf("fetching manifest tip %s for head %q before publication: %w", tip, e.Name, err)
		}
	}

	// Already serving this exact generation -- the checkpoint records the same root,
	// coverage floor and manifest tip -- so there is nothing to load, checkpoint or
	// expose. The claimed synced_to must equal the checkpoint's: a document that reuses
	// the served root and tip but claims a HIGHER synced_to is a root/floor
	// contradiction, and short-circuiting here would return a no-op plan and, since
	// nothing refused, let admit raise the freshness floor for it (the safety boundary
	// follow-up). It falls through to the coverage check below instead, which refuses
	// it. The checkpoint's synced_to is the coverage the served root actually encodes
	// -- preflight verified derived==claimed before it was written -- so requiring the
	// claim to equal it is the derived==claimed check without re-loading.
	serving := adopted == root && adoptTip == tip
	if serving && hasCP && f.checkpointMatchesDocument(cp, e, document, root, tip) {
		return adoptPlan{name: e.Name, entry: e, kind: kind}, nil
	}

	// Load and structurally admit the head: this makes the Head, canonical
	// directory, and Segment index blocks durable while leaving blob bodies lazy
	// (spec 11.3). The bounded walk completes before the coverage check below and
	// before any durable selection or retention mutation.
	head := admitted
	if head == nil {
		head, err = f.loadWithPointer(ctx, e.Name, root)
		if err != nil {
			return adoptPlan{}, err
		}
	} else if head.Root() != root {
		return adoptPlan{}, fmt.Errorf("head %q pre-admission root %s differs from published root %s",
			e.Name, head.Root(), root)
	}

	// Coverage consistency (spec 11.3), the adoption direction: the coverage the
	// loaded root encodes must equal the synced_to the signed entry claims. A
	// mismatch is a document whose root and floor disagree -- refused, so the head
	// keeps serving its last good generation rather than checkpointing an
	// inconsistent one.
	derived, covered := head.SyncedTo()
	if !covered || derived != *e.SyncedTo {
		f.cfg.Metrics.FollowRefusal(metrics.RefusalCoverageMismatch)
		f.log.Warn("refused a followed head: the coverage its root encodes does not match the published synced_to",
			"head", e.Name, "published_synced_to", *e.SyncedTo, "root_coverage", derived, "covered", covered)
		return adoptPlan{}, fmt.Errorf("head %q root %s covers %d (covered=%t) but the document claims synced_to %d; "+
			"refusing a root whose coverage contradicts its floor (spec 11.3)", e.Name, root, derived, covered, *e.SyncedTo)
	}
	if kind == server.UnfinalizedMutable {
		if hasCP && document.runtimeSource != nil {
			switch cp.version {
			case checkpointVersionV4:
				if cp.sourceID != document.runtimeSource.cfg.ID {
					return adoptPlan{}, fmt.Errorf("mutable head %q is durably bound to source %q, refusing source %q", e.Name, cp.sourceID, document.runtimeSource.cfg.ID)
				}
			case checkpointVersionV2, checkpointVersionV3:
				if cp.authority != document.authority {
					return adoptPlan{}, fmt.Errorf("mutable head %q legacy checkpoint authority differs from source %q", e.Name, document.runtimeSource.cfg.ID)
				}
			}
		}
		if e.WindowStart == nil {
			return adoptPlan{}, fmt.Errorf("mutable head %q has no window_start", e.Name)
		}
		if got := head.Params().OriginSlot; got != *e.WindowStart {
			f.cfg.Metrics.FollowRefusal(metrics.RefusalCoverageMismatch)
			return adoptPlan{}, fmt.Errorf("mutable head %q root %s has origin_slot %d but the document claims window_start %d",
				e.Name, root, got, *e.WindowStart)
		}
		limit := f.cfg.MaxMutableWindowSlots[e.Name]
		if limit == 0 || *e.SyncedTo-*e.WindowStart >= limit {
			return adoptPlan{}, fmt.Errorf("mutable head %q root covers more than this follower's %d-slot maximum", e.Name, limit)
		}
	}

	// Every deterministic refusal Registry.Adopt would make, run now with zero effect
	//. commitEntry writes the checkpoint and raises the floor BEFORE it
	// calls Adopt, so a predictable Adopt refusal -- an immutable-params change against
	// the already-followed generation, a name this node writes, a defined tip with no
	// manifest store -- would otherwise leave a durable checkpoint a restart into an
	// empty registry could resume, losing the immutability baseline. The registry runs
	// the identical checks in Adopt, so the preflight and the commit cannot drift.
	if err := validateRegistryAdopt(f.cfg.Registry, head, tip, kind); err != nil {
		return adoptPlan{}, err
	}

	// A checkpoint is owed unless one already records this exact authenticated
	// generation. expose still runs to re-register a generation adopted durably but
	// not exposed before a crash.
	writeCP := !hasCP || !f.checkpointMatchesDocument(cp, e, document, root, tip)
	newCP := checkpoint{
		root: root, syncedTo: *e.SyncedTo, manifestTip: tip, updatedAt: document.updatedAt, kind: kind,
	}
	if document.revisioned {
		var handoff, overlay *server.HeadEntry
		if kind == server.UnfinalizedMutable {
			witness, ok := documentHead(document.doc, e.HandoffHead)
			if !ok {
				// ValidateContract has already established this. Keep the check at
				// the checkpoint boundary so a future caller cannot durably omit the
				// exact same-document witness required for restart.
				return adoptPlan{}, fmt.Errorf("mutable head %q has no finalized handoff witness %q", e.Name, e.HandoffHead)
			}
			handoff = &witness
			if overlayName := f.cfg.OverlayFinalizedHeads[e.Name]; overlayName != "" {
				overlayEntry, ok := documentHead(document.doc, overlayName)
				if !ok {
					return adoptPlan{}, fmt.Errorf("mutable head %q has no filtered-finalized overlay witness %q", e.Name, overlayName)
				}
				overlay = &overlayEntry
			}
		}
		var err error
		if document.runtimeSource != nil {
			newCP, err = makeCheckpointV4(document.doc.Net, document.runtimeSource.ref.archiveID, document.runtimeSource.cfg.ID,
				true, &e, handoff, overlay, document.updatedAt, *document.publicationFloor())
		} else {
			newCP, err = makeCheckpointV3(document.doc.Net, true, &e, handoff, document.updatedAt, *document.publicationFloor())
		}
		if err != nil {
			return adoptPlan{}, err
		}
	}
	return adoptPlan{
		name:            e.Name,
		head:            head,
		entry:           e,
		tip:             tip,
		kind:            kind,
		cp:              newCP,
		writeCheckpoint: writeCP,
	}, nil
}

func (f *Follower) checkpointMatchesDocument(cp checkpoint, e server.HeadEntry, document *resolved, root, tip cid.Cid) bool {
	if cp.root != root || cp.syncedTo != *e.SyncedTo || cp.manifestTip != tip || cp.kind != e.EffectiveKind() {
		return false
	}
	if !document.revisioned {
		return cp.revision == 0
	}
	if document.runtimeSource != nil {
		if cp.version != checkpointVersionV4 || cp.archiveID != document.runtimeSource.ref.archiveID ||
			cp.sourceID != document.runtimeSource.cfg.ID {
			return false
		}
	} else if cp.version != checkpointVersionV3 {
		return false
	}
	if !cp.selected || cp.net != document.doc.Net || !headEntriesEqual(cp.published, &e) {
		return false
	}
	if cp.revision != document.revision || cp.authority != document.authority || cp.digest != document.digest {
		return false
	}
	if e.EffectiveKind() == server.UnfinalizedMutable {
		witness, ok := documentHead(document.doc, e.HandoffHead)
		if !ok || !headEntriesEqual(cp.handoff, &witness) {
			return false
		}
		if document.runtimeSource == nil {
			return cp.overlay == nil
		}
		overlayName := f.cfg.OverlayFinalizedHeads[e.Name]
		if overlayName == "" {
			return cp.overlay == nil
		}
		overlay, ok := documentHead(document.doc, overlayName)
		return ok && headEntriesEqual(cp.overlay, &overlay)
	}
	return cp.handoff == nil && cp.overlay == nil
}

func documentHead(doc server.Doc, name string) (server.HeadEntry, bool) {
	for _, entry := range doc.Heads {
		if entry.Name == name {
			return entry, true
		}
	}
	return server.HeadEntry{}, false
}

func optionalSlotsEqual(left, right *uint64) bool {
	return left == nil && right == nil || left != nil && right != nil && *left == *right
}

func headEntriesEqual(left, right *server.HeadEntry) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return left.Name == right.Name && left.Root == right.Root && left.OriginSlot == right.OriginSlot &&
		optionalSlotsEqual(left.SyncedTo, right.SyncedTo) && left.SegBits == right.SegBits &&
		left.FanoutBits == right.FanoutBits && left.DirDepth == right.DirDepth && left.Manifest == right.Manifest &&
		left.Kind == right.Kind && optionalSlotsEqual(left.WindowStart, right.WindowStart) &&
		left.SourceHeadRoot == right.SourceHeadRoot && optionalSlotsEqual(left.SourceFinalizedSlot, right.SourceFinalizedSlot) &&
		left.SourceFinalizedRoot == right.SourceFinalizedRoot && left.HandoffHead == right.HandoffHead &&
		left.HandoffRoot == right.HandoffRoot && optionalSlotsEqual(left.HandoffSyncedTo, right.HandoffSyncedTo)
}

// validateCheckpointBaseline preserves immutable archive parameters across an
// authenticated withdrawal. Once AdoptBatch removes the server registry entry,
// the registry no longer has an old engine against which to compare a later
// reappearance; the retained v3 line is that durable baseline. Finalized heads
// keep their fixed origin, while a mutable generation is allowed to rotate its
// bounded origin/window but not its archive geometry.
func validateCheckpointBaseline(prior, next server.HeadEntry) error {
	if prior.Name != next.Name || prior.EffectiveKind() != next.EffectiveKind() {
		return fmt.Errorf("head %q reappears under a different authenticated identity/kind", next.Name)
	}
	if prior.SegBits != next.SegBits || prior.FanoutBits != next.FanoutBits {
		return fmt.Errorf("head %q changes immutable seg_bits/fanout_bits across withdrawal (%d/%d -> %d/%d)",
			next.Name, prior.SegBits, prior.FanoutBits, next.SegBits, next.FanoutBits)
	}
	if next.EffectiveKind() == server.FinalizedMonotonic && prior.OriginSlot != next.OriginSlot {
		return fmt.Errorf("finalized head %q changes immutable origin_slot across withdrawal (%d -> %d)",
			next.Name, prior.OriginSlot, next.OriginSlot)
	}
	return nil
}

// commitPlans is phase 2 for the whole document: the writes preflightEntry deferred.
// It renders the complete prospective registry, commits every checkpoint/tombstone,
// compatibility mirror, and ordering floor in one synced batch, then exposes that
// one snapshot. The order is load-bearing: durability precedes visibility, so restart
// can resume the exact generation and no mirror can name a generation the checkpoint
// does not. A no-op plan still commits the document ordering floor.
func (f *Follower) commitPlans(ctx context.Context, plans []adoptPlan, document *resolved) []error {
	return f.commitPlansWithStage(ctx, plans, func(batch *pebble.Batch) error {
		return f.state.stageAdmission(batch, plans, document.updatedAt, document.delegation, document.publicationFloor())
	})
}

func (f *Follower) commitSourcePlans(ctx context.Context, plans []adoptPlan, admissions []sourceDocumentAdmission, documents map[string]*resolved) ([]adoptPlan, []error, []error) {
	archiveID := *f.cfg.ExpectedArchiveID
	if f.cfg.Retention != nil {
		// External retention prepared one exact complete generation before this
		// transaction. Its API has no per-head attribution with which to revise
		// that generation safely, so preserve fail-closed all-or-nothing commit
		// semantics for this mode.
		errs := f.commitPlansWithStage(ctx, plans, func(batch *pebble.Batch) error {
			return f.state.stageSourceAdmission(batch, archiveID, plans, admissions)
		})
		return plans, nil, errs
	}
	return f.commitPlansWithStageFiltered(ctx, plans,
		func(batch *pebble.Batch, survivors []adoptPlan) error {
			return f.state.stageSourceAdmission(batch, archiveID, survivors, admissions)
		},
		func(ctx context.Context, candidates []adoptPlan) ([]adoptPlan, []error, error) {
			return f.filterSourcePlansUnderGate(ctx, candidates, documents)
		})
}

// recordSourcePlanSelections mirrors source checkpoints only after the serving
// transaction succeeds. Transport winners and plans rejected by the online-GC
// filter never reach this method. Resume plans need not rewrite their already
// durable checkpoints, so both changing and idempotent v4 plans participate. A
// selected checkpoint names its durable source; a tombstone clears the gauge.
func (f *Follower) recordSourcePlanSelections(plans []adoptPlan) {
	if f.cfg.SourceSet == nil {
		return
	}
	for _, plan := range plans {
		if plan.cp.version == checkpointVersionV4 && plan.cp.selected {
			f.cfg.Metrics.FollowSourceSelected(plan.name, plan.cp.sourceID)
			continue
		}
		f.cfg.Metrics.FollowSourceSelected(plan.name, "")
	}
}

// commitPlansWithStage is the common serving transaction for singular and
// source-set admission. stager joins the mode's ordering/provenance facts to
// the exact checkpoint and compatibility-mirror batch before visibility.
func (f *Follower) commitPlansWithStage(ctx context.Context, plans []adoptPlan, stager func(*pebble.Batch) error) []error {
	_, _, errs := f.commitPlansWithStageFiltered(ctx, plans,
		func(batch *pebble.Batch, _ []adoptPlan) error { return stager(batch) }, nil)
	return errs
}

// commitPlansWithStageFiltered is the common transaction engine plus an
// optional source-set-only filter which runs while Gate excludes collection.
// The filter's exact survivor slice is used for checkpoint staging, mirrors,
// reconciliation, registry visibility, in-memory state, logs, and the returned
// callback basis; there is no second copy of the pre-filter plans which could
// accidentally become durable or visible.
func (f *Follower) commitPlansWithStageFiltered(
	ctx context.Context,
	plans []adoptPlan,
	stager func(*pebble.Batch, []adoptPlan) error,
	filter func(context.Context, []adoptPlan) ([]adoptPlan, []error, error),
) ([]adoptPlan, []error, []error) {
	var committed []adoptPlan
	survivors := plans
	var filterErrs []error
	errs := func() []error {
		f.gate.Enter()
		defer f.gate.Leave()

		if filter != nil {
			var err error
			survivors, filterErrs, err = filter(ctx, survivors)
			if err != nil {
				return []error{err}
			}
		} else {
			// An ordinary Boxo blockstore cannot expose collection generations. Its
			// compatible collector holds Gate for the whole run, so retain the same
			// safety model by proving the closure under this commit's Gate. The walk
			// knows Gate is already held and does not recursively enter it when staging
			// a fetched block.
			if f.cfg.Retention == nil && !f.hasCollectionGeneration() {
				for i := range survivors {
					if err := f.protectAdoptionClosure(ctx, &survivors[i], true); err != nil {
						return []error{fmt.Errorf("follow: preparing head %q closure for publication: %w", survivors[i].name, err)}
					}
				}
			}

			generation := f.collectionGeneration()
			// Validate every proof and anchor before the first durable write. This is
			// what makes a singular document's online-GC refusal whole rather than
			// per-head. Source-set mode supplies filter above for attributable pruning.
			for _, p := range survivors {
				if p.head == nil {
					continue
				}
				if f.cfg.Retention == nil && p.closureGeneration != generation {
					return []error{fmt.Errorf("follow: head %q closure was proved in collection generation %d, current is %d; refusing publication and retrying the document",
						p.name, p.closureGeneration, generation)}
				}
				if err := f.touchGeneration(ctx, p.name, p.head.Root(), p.tip); err != nil {
					return []error{err}
				}
			}
		}

		// Prepare the reconciler's complete pointer delta before the durability
		// barrier. Its returned closure is deliberately infallible and is applied
		// inside AdoptBatch's BeforeVisible hook while Gate still excludes GC.
		var registrations []pinning.Registration
		for _, p := range survivors {
			switch {
			case p.head != nil:
				registrations = append(registrations, pinning.Registration{Name: p.name, Head: p.head, Policy: f.cfg.Heads[p.name]})
			case p.withdraw:
				registrations = append(registrations, pinning.Registration{Name: p.name, Policy: f.cfg.Heads[p.name]})
			}
		}
		applyReconciler := func() {}
		if f.cfg.Reconciler != nil {
			var err error
			applyReconciler, err = f.cfg.Reconciler.PrepareSetBatch(registrations)
			if err != nil {
				return []error{err}
			}
		}

		// One Pebble batch is the durable selection fact. It contains all v3
		// checkpoints/tombstones, the signer/delegation floor, and exact root and
		// manifest compatibility mirrors. AdoptBatch calls Commit only after it
		// has validated and rendered the complete prospective serving snapshot.
		batch := f.cfg.KV.NewBatch()
		defer batch.Close()
		if err := stager(batch, survivors); err != nil {
			return []error{err}
		}

		var upserts []server.Adoption
		var withdrawals []string
		for _, p := range survivors {
			switch {
			case p.head != nil:
				if err := f.cfg.Roots.StagePut(batch, p.name, p.head.Root()); err != nil {
					return []error{err}
				}
				if p.tip.Defined() {
					if err := f.cfg.Manifests.StagePut(batch, p.name, p.tip); err != nil {
						return []error{err}
					}
				} else if err := f.cfg.Manifests.StageDelete(batch, p.name); err != nil {
					return []error{err}
				}
				adoption := server.Adoption{Head: p.head, Blobs: &blobs{f: f, head: p.name}, ManifestTip: p.tip}
				if p.cp.revision != 0 {
					published := p.entry
					adoption.Published = &published
				}
				if p.cp.revision != 0 && p.kind == server.UnfinalizedMutable {
					published := p.entry
					// A filtered replica authenticates the writer's global
					// finalized line without selecting, pinning, checkpointing, or
					// serving it. Preserve that exact same-document witness only in
					// the mutable registry entry so its live proof remains valid.
					if _, selected := f.cfg.Heads[published.HandoffHead]; !selected {
						if p.cp.handoff == nil {
							return []error{fmt.Errorf("follow: mutable head %q has no metadata-only handoff witness", p.name)}
						}
						witness := *cloneCheckpointHeadEntry(p.cp.handoff)
						adoption.HandoffWitness = &witness
					}
				}
				upserts = append(upserts, adoption)
			case p.withdraw:
				if err := f.cfg.Roots.StageDelete(batch, p.name); err != nil {
					return []error{err}
				}
				if err := f.cfg.Manifests.StageDelete(batch, p.name); err != nil {
					return []error{err}
				}
				withdrawals = append(withdrawals, p.name)
			}
		}

		persist := func() error {
			if beforeAdmissionCommitHook != nil {
				if err := beforeAdmissionCommitHook(); err != nil {
					return err
				}
			}
			if err := batch.Commit(pebble.Sync); err != nil {
				return fmt.Errorf("follow: committing authenticated document generation: %w", err)
			}
			return nil
		}
		if len(upserts) == 0 && len(withdrawals) == 0 {
			// A legacy no-op or an exact revisioned retry still admits its ordering
			// and delegation facts, but has no serving pointer to swap.
			if err := persist(); err != nil {
				return []error{err}
			}
			return nil
		}

		if err := f.cfg.Registry.AdoptBatch(ctx, upserts, withdrawals, server.AdoptionHooks{
			Persist: persist,
			BeforeVisible: func() {
				if beforeExposeHook != nil {
					beforeExposeHook()
				}
				applyReconciler()
				f.installHeadPlans(survivors)
			},
		}); err != nil {
			return []error{err}
		}
		for _, p := range survivors {
			if p.head != nil {
				committed = append(committed, p)
			}
			if p.withdraw {
				f.reportReady(p.name, false)
				f.log.Info("followed head withdrawn by authenticated publication", "head", p.name,
					"revision", p.cp.revision)
			} else if p.head != nil {
				f.reportReady(p.name, true)
				syncedTo, _ := p.head.SyncedTo()
				f.log.Info("followed head adopted", "head", p.name, "root", p.head.Root(), "synced_to", syncedTo,
					"manifest", cidOrNone(p.tip), "pin_mode", f.cfg.Heads[p.name].Mode)
			}
		}
		return nil
	}()

	// Once exposed, Reconciler.Set/Notify is part of expose and a future GC's
	// gated flush will pin the closure. Staging rows can therefore drop outside
	// Gate; if a GC gets there first they merely remain as redundant live roots.
	if f.cfg.Staging != nil {
		for _, p := range committed {
			if len(p.staged) == 0 {
				continue
			}
			if err := f.cfg.Staging.Drop(ctx, p.staged); err != nil {
				f.log.Error("dropping adoption-closure staging pins", "head", p.name, "err", err)
			}
		}
	}
	if len(errs) == 0 {
		f.recordSourcePlanSelections(survivors)
	}
	return survivors, filterErrs, errs
}

// installHeadPlans is AdoptBatch's infallible in-memory half. It runs after the
// complete checkpoint/mirror batch is durable and after the reconciler batch is
// installed, but before the server registry becomes visible. Readers therefore
// cannot reach a new root while the follower still describes the old generation.
func (f *Follower) installHeadPlans(plans []adoptPlan) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, p := range plans {
		hs := f.heads[p.name]
		if hs == nil {
			continue // construction validated all plan names; defensive only.
		}
		if p.withdraw {
			hs.adopted = cid.Undef
			hs.fetched = cid.Undef
			hs.manifestTip = cid.Undef
			hs.manifestFetched = cid.Undef
			continue
		}
		if p.head == nil {
			continue
		}
		root := p.head.Root()
		if f.cfg.Retention != nil {
			hs.fetched = root
			hs.manifestFetched = p.tip
		} else {
			if hs.adopted != root {
				hs.fetched = cid.Undef
			}
			if hs.manifestTip != p.tip {
				hs.manifestFetched = cid.Undef
			}
		}
		hs.adopted = root
		hs.manifestTip = p.tip
	}
}

// touchGeneration verifies that the locally-published anchors of a followed
// generation still exist immediately before its checkpoint and exposure. Local
// is the validating application blockstore view, so these Gets both reject
// corrupt anchor bytes and protect the CIDs in an active epoch. Network reads do
// not belong here: all fetching is preflight work, outside the gate, and a
// missing local anchor must abort without a checkpoint naming it.
func (f *Follower) touchGeneration(ctx context.Context, name string, root, manifestTip cid.Cid) error {
	_, err := f.cfg.Local.Get(ctx, root)
	if err != nil {
		return fmt.Errorf("follow: local root %s for head %q disappeared or failed validation before publication: %w", root, name, err)
	}
	if !manifestTip.Defined() {
		return nil
	}
	_, err = f.cfg.Local.Get(ctx, manifestTip)
	if err != nil {
		return fmt.Errorf("follow: local manifest tip %s for head %q disappeared or failed validation before publication: %w", manifestTip, name, err)
	}
	return nil
}

// floors returns the synced_to and manifest-tip floors a document entry is checked
// against: the checkpoint's once one exists, else the retained legacy facts.
func (f *Follower) floors(name string, cp checkpoint, hasCP bool) (syncedTo uint64, hasSyncedTo bool,
	manifestTip cid.Cid, hasManifestTip bool, err error) {
	if hasCP {
		if (cp.version == checkpointVersionV3 || cp.version == checkpointVersionV4) && cp.published == nil {
			return 0, false, cid.Undef, false, nil
		}
		return cp.syncedTo, true, cp.manifestTip, cp.manifestTip.Defined(), nil
	}
	if syncedTo, hasSyncedTo, err = f.state.legacySyncedTo(name); err != nil {
		return 0, false, cid.Undef, false, err
	}
	if manifestTip, hasManifestTip, err = f.state.legacyManifestFloor(name); err != nil {
		return 0, false, cid.Undef, false, err
	}
	return syncedTo, hasSyncedTo, manifestTip, hasManifestTip, nil
}

func (f *Follower) checkManifestAncestryWithPointer(ctx context.Context, head string, tip, floor cid.Cid, hasFloor bool) error {
	err := f.checkManifestAncestry(ctx, head, tip, floor, hasFloor)
	if err == nil || !tip.Defined() || !isFetchMiss(err) || f.cfg.FindPointer == nil || ctx.Err() != nil {
		return err
	}
	if findErr := f.cfg.FindPointer(ctx, pointerhint.Pointer{Kind: pointerhint.Manifest, CID: tip}); findErr != nil {
		return errors.Join(err, fmt.Errorf("finding exact manifest pointer %s: %w", tip, findErr))
	}
	return f.checkManifestAncestry(ctx, head, tip, floor, hasFloor)
}

func (f *Follower) getManifestTipWithPointer(ctx context.Context, tip cid.Cid) error {
	_, err := f.blocks.Get(ctx, tip)
	if err == nil || !isFetchMiss(err) || f.cfg.FindPointer == nil || ctx.Err() != nil {
		return err
	}
	if findErr := f.cfg.FindPointer(ctx, pointerhint.Pointer{Kind: pointerhint.Manifest, CID: tip}); findErr != nil {
		return errors.Join(err, fmt.Errorf("finding exact manifest pointer %s: %w", tip, findErr))
	}
	_, err = f.blocks.Get(ctx, tip)
	return err
}

func (f *Follower) loadWithPointer(ctx context.Context, name string, root cid.Cid) (*archive.Head, error) {
	head, err := f.load(ctx, name, root)
	if err == nil || !isFetchMiss(err) || f.cfg.FindPointer == nil || ctx.Err() != nil {
		return head, err
	}
	if findErr := f.cfg.FindPointer(ctx, pointerhint.Pointer{Kind: pointerhint.Root, CID: root}); findErr != nil {
		return nil, errors.Join(err, fmt.Errorf("finding exact root pointer %s: %w", root, findErr))
	}
	return f.load(ctx, name, root)
}

// syncWithPointer gives an already-adopted but incompletely replicated head the
// same exact-provider escape hatch as its initial entry-block fetch. This
// matters after a restart or partial pass: the current Head or Manifest tip can
// be local while a descendant needed by the retained closure is missing, so the
// initial pointer read has no miss on which to discover a provider.
//
// The missing descendant is deliberately never queried. sync tags the failed
// phase with the authenticated root or manifest tip it snapshotted; after one
// genuine fetching-blockstore miss, this finds only that exact entry pointer
// and retries the ordinary pass once. A concurrent adoption may make the
// snapshot historical before lookup, but it remains an authenticated local
// generation, while sync's own generation checks bind the retry to the state
// current when it starts.
func (f *Follower) syncWithPointer(ctx context.Context, name string) error {
	err := f.sync(ctx, name)
	var phase *syncPointerError
	if err == nil || !errors.As(err, &phase) || !isFetchMiss(err) || f.cfg.FindPointer == nil || ctx.Err() != nil {
		return err
	}

	if findErr := f.cfg.FindPointer(ctx, phase.pointer); findErr != nil {
		return errors.Join(err, fmt.Errorf("finding exact %s pointer %s while syncing head %q: %w",
			phase.pointer.Kind, phase.pointer.CID, name, findErr))
	}
	return f.sync(ctx, name)
}

func isFetchMiss(err error) bool {
	var fetchErr *p2p.FetchError
	return errors.As(err, &fetchErr)
}

// load loads and validates the head at root over the fetching blockstore. It is
// the read half of an adoption: the Head, complete canonical directory, and every
// Segment index block are fetched and structurally validated before any checkpoint,
// retention intent, pin or floor can change. Blob bodies below Segments stay lazy
// (spec 11.3).
func (f *Follower) load(ctx context.Context, name string, root cid.Cid) (*archive.Head, error) {
	head, err := archive.Load(ctx, archive.Config{
		// The fetching blockstore, so that loading a head this node has never seen
		// is a fetch of its Head block, and every read below it likewise. No
		// Resolver: that is apply_refs's vh -> CID map, and a followed head never
		// mutates (spec 11.1).
		Blocks:         f.blocks,
		Cache:          f.cfg.Cache,
		StructureCache: f.structure,
		// f.blocks is a bounded fetching facade and intentionally does not claim
		// optional local-store capabilities. Bind the shared structure cache to
		// the underlying application blockstore's collection generation so a
		// proof survives ordinary publications but never a GC boundary.
		CollectionGeneration: collectionGenerationSource(f.cfg.Local),
	}, root)
	if err != nil {
		return nil, fmt.Errorf("loading root %s: %w", root, err)
	}
	if got := head.Params().Net; got != f.cfg.Net {
		return nil, fmt.Errorf("head %q at %s is on net %q, this node is on net %q", name, root, got, f.cfg.Net)
	}
	if got := head.Params().Name; got != name {
		// The document said one name and the Head block says another. Either the
		// document is lying about what it published or the writer has crossed two
		// heads; both are for a human.
		return nil, fmt.Errorf("root %s is head %q, published under the name %q", root, got, name)
	}
	if _, err := head.Enumerate(ctx); err != nil {
		return nil, fmt.Errorf("root %s failed bounded structure admission: %w", root, err)
	}
	return head, nil
}

func collectionGenerationSource(blocks blockstore.Blockstore) archive.CollectionGenerationSource {
	source, _ := blocks.(archive.CollectionGenerationSource)
	return source
}

// expose registers a loaded head as the served, reconciled generation: the
// registry (which persists the compatibility mirrors and swaps what the read API
// serves), then the reconciler, then a nudge to reconcile it.
//
// The reconciler is told after the registry and notified after that, in that
// order, for the reason spec 9's reconciliation order exists: what is registered
// must be reconcilable, and a notification for a head the reconciler does not know
// yet is a notification it drops.
//
// It is called only once the head's checkpoint is durable (adoptEntry) or has been
// read back from one (Resume), so the RootStore and ManifestStore mirrors it writes
// through Registry.Adopt never name a generation the authoritative checkpoint does
// not.
func (f *Follower) expose(ctx context.Context, name string, head *archive.Head, manifestTip cid.Cid, kind server.HeadKind,
	published *server.HeadEntry) error {
	// Snapshot every headState field this call reads under f.mu: quarantined
	// is written by quarantine, which now flips under transition -- expose runs under
	// transition too, so it will not see a mid-flip -- but reading it (and adopted and
	// manifestTip) after releasing f.mu would still race a quarantine or a concurrent
	// pass, so take them all in one hold. hs.policy is set once at New and never
	// mutated, so reading it unlocked below is safe.
	root := head.Root()
	f.mu.Lock()
	hs, ok := f.heads[name]
	var (
		quarantined       bool
		adopted, adoptTip cid.Cid
	)
	if ok {
		quarantined = hs.quarantined
		adopted, adoptTip = hs.adopted, hs.manifestTip
	}
	f.mu.Unlock()
	if !ok {
		return fmt.Errorf("follow: head %q is not followed by this node", name)
	}
	if quarantined {
		// A quarantine is one-way for the life of the process (spec 11.4), so a
		// writer still publishing this head has it refused every poll. That steady
		// refusal is the signal worth counting, distinct from the head_quarantined
		// gauge, which only says the state is set.
		f.cfg.Metrics.FollowRefusal(metrics.RefusalQuarantined)
		return fmt.Errorf("%w: head %q", server.ErrQuarantined, name)
	}
	if adopted == root && adoptTip == manifestTip {
		// Already serving this exact root and tip; the fetch pass is where the work
		// is. Still a served head, so its readiness holds.
		f.reportReady(name, true)
		return nil
	}

	if err := registryAdopt(ctx, f.cfg.Registry, head, &blobs{f: f, head: name}, manifestTip, kind, published); err != nil {
		return err
	}
	if f.cfg.Reconciler != nil {
		if err := f.cfg.Reconciler.Set(head, hs.policy); err != nil {
			return err
		}
		f.cfg.Reconciler.Notify(name)
	}

	f.mu.Lock()
	if f.cfg.Retention != nil {
		// The completed recursive Kubo generation pin is the full-archive sync
		// barrier. Do not immediately re-walk the multi-terabyte DAG through the
		// embedded follower's pin reconciler; Kubo has already fetched and made
		// every descendant durable before the checkpoint was published.
		hs.fetched = root
		hs.manifestFetched = manifestTip
	} else if hs.adopted != root {
		// A completion marker belongs to one adoption, not merely to a CID that
		// may recur later. Clearing on every root transition closes A -> B -> GC
		// -> A: the old A marker cannot suppress the rewalk that repairs any
		// A-only descendants collected while B was current.
		hs.fetched = cid.Undef
	}
	if f.cfg.Retention == nil && hs.manifestTip != manifestTip {
		hs.manifestFetched = cid.Undef
	}
	hs.adopted = root
	hs.manifestTip = manifestTip
	f.mu.Unlock()

	// Registered and served: raise this head's readiness. It is
	// the last step, after the registry, reconciler and in-memory state all agree
	// the head is live, so global readiness only clears once the head truly serves.
	f.reportReady(name, true)

	syncedTo, _ := head.SyncedTo()
	f.log.Info("followed head adopted", "head", name, "root", root, "synced_to", syncedTo,
		"manifest", cidOrNone(manifestTip), "pin_mode", hs.policy.Mode)
	return nil
}

// kindAdopter is the server registry extension introduced with revisioned
// mutable generations. Keeping the dependency behind this interface lets the
// follower retain source compatibility while the two package commits are
// integrated; finalized heads use the long-standing methods on either build.
type kindAdopter interface {
	ValidateAdoptKind(*archive.Head, cid.Cid, server.HeadKind) error
	AdoptKind(context.Context, *archive.Head, server.Blobs, cid.Cid, server.HeadKind) error
}

type publishedAdopter interface {
	AdoptPublished(context.Context, *archive.Head, server.Blobs, cid.Cid, server.HeadEntry) error
}

func validateRegistryAdopt(registry *server.Heads, head *archive.Head, tip cid.Cid, kind server.HeadKind) error {
	if adopter, ok := any(registry).(kindAdopter); ok {
		return adopter.ValidateAdoptKind(head, tip, kind)
	}
	if kind != server.FinalizedMonotonic {
		return errors.New("follow: this server registry build cannot adopt unfinalized-mutable heads")
	}
	return registry.ValidateAdopt(head, tip)
}

func registryAdopt(ctx context.Context, registry *server.Heads, head *archive.Head, blobs server.Blobs, tip cid.Cid,
	kind server.HeadKind, published *server.HeadEntry) error {
	if kind == server.UnfinalizedMutable {
		if published == nil {
			return errors.New("follow: mutable checkpoint lacks proof-aware publication metadata; wait for a fresh authenticated document")
		}
		if adopter, ok := any(registry).(publishedAdopter); ok {
			return adopter.AdoptPublished(ctx, head, blobs, tip, *published)
		}
		return errors.New("follow: this server registry build cannot adopt proof-aware mutable heads")
	}
	if adopter, ok := any(registry).(kindAdopter); ok {
		return adopter.AdoptKind(ctx, head, blobs, tip, kind)
	}
	if kind != server.FinalizedMonotonic {
		return errors.New("follow: this server registry build cannot adopt unfinalized-mutable heads")
	}
	return registry.Adopt(ctx, head, blobs, tip)
}

// reportReady tells the daemon a followed head's readiness changed (finding
// the safety boundary), via Config.Ready. It is called with true once a head is registered --
// on the resume or adoption that exposes it -- and with false when the head is
// quarantined, which takes it out of service entirely (spec 11.4). Those are the
// only two transitions: an ordinary poll failure leaves a served head serving its
// durable root, so it does not withdraw readiness, but a quarantine does, because
// the head then answers 503 and the load balancer must route away from it.
func (f *Follower) reportReady(name string, ready bool) {
	if f.cfg.Ready != nil {
		f.cfg.Ready(name, ready)
	}
}

// dial connects to the multiaddrs the document advertises (spec 11.2).
//
// Every poll, and failures are logged at debug and otherwise ignored. This is
// not the peering mechanism -- p2p.peers is, and it supervises its connections
// -- it is the writer telling followers where it is, which is a hint that may be
// stale, wrong for this network position, or one of several addresses of which
// one works. A poll that dialled nothing still fetches from whatever the node is
// already connected to.
func (f *Follower) dial(ctx context.Context, addrs []string) {
	if (f.cfg.Host == nil && f.cfg.DialPeer == nil) || len(addrs) == 0 {
		return
	}
	const (
		maxPublicationDialPeers        = 64
		maxPublicationDialAddressBytes = 64 << 10
	)
	if len(addrs) > maxPublicationDialPeers {
		f.log.Warn("publication document advertises too many peer multiaddrs", "count", len(addrs), "limit", maxPublicationDialPeers)
		return
	}
	addressBytes := 0
	for _, address := range addrs {
		addressBytes += len(address)
		if addressBytes > maxPublicationDialAddressBytes {
			f.log.Warn("publication document peer multiaddrs exceed the byte limit", "bytes", addressBytes, "limit", maxPublicationDialAddressBytes)
			return
		}
	}
	peers, err := p2p.ParsePeers(addrs)
	if err != nil {
		f.log.Warn("publication document advertises an unusable multiaddr", "err", err)
		return
	}
	// Publication addresses are a bounded hint, never a reason to hold the
	// authenticated adoption path for one FetchTimeout per advertised peer.
	// Share one total budget and divide its remainder across peer attempts.
	batchCtx, cancelBatch := context.WithTimeout(ctx, f.cfg.FetchTimeout)
	defer cancelBatch()
	for index, ai := range peers {
		if f.cfg.Host != nil && ai.ID == f.cfg.Host.ID() {
			continue // ourselves: a follower of a document we published.
		}
		dialCtx := batchCtx
		cancel := func() {}
		if deadline, ok := batchCtx.Deadline(); ok {
			remainingPeers := len(peers) - index
			budget := time.Until(deadline) / time.Duration(remainingPeers)
			if budget > 0 {
				dialCtx, cancel = context.WithTimeout(batchCtx, budget)
			}
		}
		if f.cfg.Host != nil {
			err = f.cfg.Host.Libp2p().Connect(dialCtx, ai)
		} else {
			err = f.cfg.DialPeer(dialCtx, ai)
		}
		cancel()
		if err != nil {
			f.log.Debug("dialling a peer from the publication document", "peer", ai.ID, "err", err)
		}
	}
}

// quarantine stops serving name and says why, everywhere at once: here, so the
// loop stops adopting it; the registry, so its reads answer 503 and the
// document stops carrying it; and the log, at error, because this is the one
// thing in the protocol that means somebody is lying.
//
// It returns the error the caller should hand back: what a read of a
// quarantined head owes a client is 503, never the blob and never a 404 (spec
// 11.4).
func (f *Follower) quarantine(name, reason string, args ...any) error {
	// The quarantine write is serialized into the transition linearization.
	// Both the in-memory hs.quarantined flip that preflight reads and the registry flip
	// that expose's Adopt reads are taken under transition, so a followed head cannot
	// flip quarantined between a poll's preflight and its commit: the poll either sees
	// the quarantine and refuses the whole document with no effect, or it commits fully
	// and the quarantine applies after -- never a checkpoint written but not exposed,
	// nor a freshness floor raised for a head this pass has decided to stop serving.
	// The fetch and read paths that call this hold neither transition nor f.mu, so this
	// is a quick flip that does not stall a backfill; lock order transition -> f.mu and
	// transition -> the registry lock is preserved (the fetch walk itself never holds
	// transition, only this write does).
	f.transition.Lock()
	defer f.transition.Unlock()
	return f.quarantineLocked(name, reason, args...)
}

// quarantineLocked is the implementation after quarantine has acquired
// transition. Keeping the state change in one helper makes the non-reentrant
// lock boundary explicit while preserving one readiness, registry, and logging
// sequence for every verification failure.
func (f *Follower) quarantineLocked(name, reason string, args ...any) error {
	detail := fmt.Sprintf(reason, args...)
	f.mu.Lock()
	if hs, ok := f.heads[name]; ok {
		hs.quarantined = true
	}
	f.mu.Unlock()
	if err := f.cfg.Registry.Quarantine(name, detail); err != nil {
		f.log.Error("quarantining a head in the registry", "head", name, "err", err)
	}
	f.notifyServiceabilityChanged()
	// Withdraw readiness: the head now answers 503 to every
	// read (spec 11.4), so a node still green on it would keep the load balancer
	// routing requests it can only fail. This is the one way a followed head's
	// readiness regresses; an ordinary poll failure leaves the durable root serving.
	f.reportReady(name, false)

	f.log.Error("QUARANTINED a followed head: it served data that does not verify. This node has stopped serving the "+
		"head entirely and will not adopt it again while it runs (spec 11.4). The writer's signature vouches for the "+
		"completeness of a head, and this is evidence against the writer, not against the block: check the pubkey you "+
		"are following, and whether the archive at that key is the one you think it is",
		"head", name, "reason", detail, "url", f.cfg.URL, "ipns", f.cfg.IPNS,
		"pubkey", fmt.Sprintf("%x", f.cfg.PubKey), "verify", f.cfg.Verify)
	return fmt.Errorf("%w: head %q is quarantined: %s", server.ErrBlobUnavailable, name, detail)
}

// quarantineMutableEquivocationLocked applies the fail-closed consequence of a
// same-authority, same-revision digest conflict. Finalized heads retain their
// ordinary monotonic floor; every head explicitly configured as mutable is
// withdrawn because the authority has made its generation order ambiguous.
// The caller holds transition, matching quarantineLocked's linearization.
func (f *Follower) quarantineMutableEquivocationLocked(e *authorityEquivocationError) {
	f.quarantineMutableEquivocationForSourceLocked(nil, e)
}

// quarantineSourceMutableEquivocationLocked limits a source-local generation
// conflict to the mutable heads that source is authorized to publish. Finalized
// heads use content partial-order arbitration, while an unrelated mutable
// source must not lose service because another signer equivocated.
func (f *Follower) quarantineSourceMutableEquivocationLocked(source *sourceRuntime, e *authorityEquivocationError) {
	f.quarantineMutableEquivocationForSourceLocked(source, e)
}

func (f *Follower) quarantineMutableEquivocationForSourceLocked(source *sourceRuntime, e *authorityEquivocationError) {
	for _, name := range f.Names() {
		if f.expectedKind(name) != server.UnfinalizedMutable {
			continue
		}
		if source != nil && !source.allows(name) {
			continue
		}
		f.mu.Lock()
		hs := f.heads[name]
		already := hs != nil && hs.quarantined
		if hs != nil {
			hs.quarantined = true
		}
		f.mu.Unlock()
		if already {
			continue
		}
		if _, served := f.cfg.Registry.Get(name); served {
			if err := f.cfg.Registry.Quarantine(name, e.Error()); err != nil {
				f.log.Error("quarantining an equivocated mutable head in the registry", "head", name, "err", err)
			}
		}
		f.reportReady(name, false)
		f.log.Error("QUARANTINED a mutable followed head: its signing authority assigned two claims to one publication revision",
			"head", name, "authority", fmt.Sprintf("%x", e.authority[:8]), "revision", e.revision,
			"first_digest", fmt.Sprintf("%x", e.first[:8]), "second_digest", fmt.Sprintf("%x", e.second[:8]))
	}
	f.notifyServiceabilityChanged()
}

func (f *Follower) notifyServiceabilityChanged() {
	if f.cfg.OnServiceabilityChanged == nil {
		return
	}
	if err := f.cfg.OnServiceabilityChanged(); err != nil {
		// The callback may have withdrawn the ephemeral exact-pointer snapshot
		// before reporting a malformed registry view or a schedule failure. Make
		// the same authenticated source document retry its post-durability handoff
		// on a later admissible poll instead of suppressing it by CID forever. Both
		// quarantine paths call this with transition held, which is the same
		// serialization notifyAdmittedDocument uses for this marker.
		clear(f.admittedDocuments)
		f.log.Error("refreshing followed-head serviceability after quarantine", "err", err)
	}
}
