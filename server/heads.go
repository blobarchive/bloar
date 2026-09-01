package server

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"math"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/go-cid"

	"github.com/blobarchive/bloar/archive"
	"github.com/blobarchive/bloar/metrics"
	"github.com/blobarchive/bloar/schema"
)

// ErrUnknownHead reports a head this node neither writes nor follows. The HTTP
// layer maps it to 404 (spec 7.1, 7.2).
var ErrUnknownHead = errors.New("server: unknown head")

// ErrFollowedHead reports a mutation aimed at a head this node follows rather
// than writes. Spec 11.1 gives every head exactly one writer, and this node is
// not it: the root comes from the publication document of whoever is (spec
// 11.3), and a local mutation would be overwritten by the next poll even if the
// engine would run it.
//
// The HTTP layer maps it to 403 (see writeApplyError): the request is
// well-formed and authenticated, and refusing it is not a statement about the
// caller's credentials -- no token makes this node the writer.
var ErrFollowedHead = errors.New("server: head is followed, not written, by this node")

// ErrManifestBindingRequired and ErrManifestBindingForbidden report a refs
// request whose expected_manifest does not match the head's manifest-chain state
// . A head with a manifest chain requires the field so
// every committed batch is bound to the tip it was scanned under; a chainless
// head forbids it, because there is no tip to bind to and a present value would
// mean the writer thinks this head is something it is not. Both are malformed
// requests the HTTP layer maps to 400 (see writeApplyError).
var (
	ErrManifestBindingRequired  = errors.New("server: expected_manifest is required for a head with a manifest chain")
	ErrManifestBindingForbidden = errors.New("server: expected_manifest is forbidden for a head with no manifest chain")
	// ErrMutableGenerationOnly rejects legacy incremental mutations against a
	// complete-snapshot mutable head. Mixing the two update contracts would let
	// refs/truncate silently bypass the durable generation CAS and source anchors.
	ErrMutableGenerationOnly = errors.New("server: unfinalized-mutable head accepts only complete generation replacement")
)

// ManifestBindingConflict reports a refs request whose expected_manifest names a
// manifest tip that is no longer the head's current one (spec 10.5, audit
// the safety boundary). It is the commit-time binding that closes the gap point-in-time
// schedule validation left open: the manifest chain advanced under a still-running
// writer, and a batch scanned under the old tip must not be committed across the
// handoff. Current is the tip the head holds now, echoed to the writer so it can
// stop and resync against it; the HTTP layer maps this to 409 and carries Current
// as manifest_tip.
type ManifestBindingConflict struct {
	Head     string
	Expected cid.Cid
	Current  cid.Cid
}

func (e *ManifestBindingConflict) Error() string {
	return fmt.Sprintf("server: refs for head %q carry expected_manifest %s but the head's manifest tip is now %s: "+
		"the manifest chain advanced under this writer (spec 10.5). Stop and resync with a schedule matching the "+
		"current tip", e.Head, cidOrNull(e.Expected), cidOrNull(e.Current))
}

// Blobs reads the blob bytes an index entry resolves to. It is one head's blob
// read path, and the seam a follower is inserted at.
//
// The writer's implementation is the node's blockstore (blockstoreBlobs, the
// default when a head registers none): the CID is in hand, the block is local,
// and reading it is the whole operation. A follower's fetches what is not local
// over bitswap, bounded by follow.fetch_timeout, and -- under follow.verify:
// full -- checks the blob against the entry that named it before handing it
// back (spec 11.4). The entry rather than the CID is the argument for exactly
// that reason: the vh -> CID binding is what full verification checks, and a
// reader handed only the CID could not check it.
type Blobs interface {
	// Blob returns the bytes of e.Blob. A block that is not there is
	// ipld.ErrNotFound; a fetch that failed is anything unavailable() matches,
	// which the HTTP layer answers 503 (spec 7.1, 11.4).
	Blob(ctx context.Context, e schema.RefEntry) ([]byte, error)
}

// blockstoreBlobs is the writer's Blobs: a blockstore read, and nothing else.
type blockstoreBlobs struct{ blocks blockstore.Blockstore }

func (b blockstoreBlobs) Blob(ctx context.Context, e schema.RefEntry) ([]byte, error) {
	blk, err := b.blocks.Get(ctx, e.Blob)
	if err != nil {
		return nil, err
	}
	return blk.RawData(), nil
}

// Gate is the GC-cut linearization of spec 9. A mutation holds it from its first
// block write through root publication so the collector's pin snapshot and
// epoch activation cannot split that transition. A block-materializing HTTP
// read holds the same read side from root/tip selection until its response is
// fully assembled, so replacing and unpinning that immutable snapshot cannot
// let the next cut collect a descendant which the request has not reached yet.
// The lease is released before the response body is written. The online sweep
// itself runs concurrently and relies on application-blockstore protection; a
// legacy collector may conservatively exclude the whole run.
// *pinning.Gate is the implementation; this is an interface so that the
// dependency points the way it already does (pinning reads heads, heads does
// not read pinning) and so that a test can supply its own.
//
// # Why the exclusion lives here
//
// A mutation writes its blocks bottom-up and swaps the root last (spec 5). If it
// completes before the cut, reconciliation includes the published root in M;
// if it starts after the cut, its protected block operations enter T. The gate
// is held across the whole of ApplyRefs and Truncate so the cut cannot land
// between an unprotected early write and the root swap.
//
// It used to be an HTTP middleware in cmd/bloard, which meant every stack that
// was not cmd/bloard -- the conformance suite's, an embedded daemon, a
// follower's own registry -- ran its mutations ungated and its GC unexcluded.
// The exclusion is a property of mutating the archive, not of receiving a POST,
// so it belongs on the type that mutates.
type Gate interface {
	// Enter registers an application transition or bounded reader lease which a
	// GC cut must not split. Every Enter is paired with a Leave. It is not
	// reentrant: code already in a mutation must not acquire a reader lease.
	Enter()
	Leave()
}

// noGate is what an unconfigured registry mutates under.
type noGate struct{}

func (noGate) Enter() {}
func (noGate) Leave() {}

// Staging is the staging-pin half of spec 9's window (a), as this package needs
// it: the thing that knows a batch's blobs are now reachable from the head and
// no longer need a pin of their own. pinning.Staging is the implementation.
//
// ingest pins every blob it accepts under a reserved ledger head so that a GC
// between the blobs POST and the refs POST cannot sweep it. This is the other
// end: once the refs naming those blobs are durable, the head's own pins retain
// them and the staging rows are dead weight. Dropping them here rather than in
// the reconciler is deliberate -- the reserved head has no policy and no engine
// to enumerate, and the reconciler must never see it.
type Staging interface {
	// DropRefs drops the staging pins of every blob the rows name. It is called
	// only after the batch's root is durable, and its error is reported but not
	// fatal: an undropped row expires on its own.
	DropRefs(ctx context.Context, rows []archive.RefRow) error
}

// Roots persists the current root of each head this node writes, as this
// package consumes it: OpenHead resumes a head from it, and commit makes a
// mutation's new root durable through it before anything announces that root.
// *RootStore is the implementation; the interface is the seam that lets a test
// stand in a store whose Put fails, which is the failure commit's
// durability-before-announcement rule turns on.
type Roots interface {
	// Get returns the persisted root of the named head, or (undef, false, nil)
	// for one that has never been created.
	Get(ctx context.Context, name string) (cid.Cid, bool, error)
	// Put records root as the named head's current root. commit calls it once
	// the engine has published the root in memory and before the document that
	// would announce it is rebuilt.
	Put(ctx context.Context, name string, root cid.Cid) error
}

// ParamsMismatchError reports a head whose on-disk root was built with
// parameters the config no longer asks for. Spec 3.1 makes origin_slot,
// seg_bits and fanout_bits immutable for the life of a head, so this is fatal
// rather than something to reconcile.
type ParamsMismatchError struct {
	Name string
	Want archive.Params // what the config asks for
	Got  archive.Params // what the stored root was built with
}

func (e *ParamsMismatchError) Error() string {
	return fmt.Sprintf("server: head %q on disk was built with net=%q origin_slot=%d seg_bits=%d fanout_bits=%d, "+
		"but the config asks for net=%q origin_slot=%d seg_bits=%d fanout_bits=%d; these parameters are immutable for "+
		"the life of a head (spec 3.1). Either restore the config, or build a new head under a different name and let a "+
		"rebuild reuse the blob blocks -- do not repoint this one.",
		e.Name, e.Got.Net, e.Got.OriginSlot, e.Got.SegBits, e.Got.FanoutBits,
		e.Want.Net, e.Want.OriginSlot, e.Want.SegBits, e.Want.FanoutBits)
}

// OpenHead resumes the named head from its persisted root, or creates it if it
// has none. It is the only way a head should be brought up: archive.New on a
// head that already exists would silently fork it back to empty.
//
// A resumed head whose stored parameters differ from params is a
// *ParamsMismatchError.
func OpenHead(ctx context.Context, cfg archive.Config, roots Roots, params archive.Params) (*archive.Head, error) {
	return OpenHeadKind(ctx, cfg, roots, params, FinalizedMonotonic)
}

// OpenMutableHead opens a mutable writer engine. A fresh name gets a durable
// mutable baseline before its initial empty root is created; a restarted name
// resumes from its selected GenerationState and may therefore have a different
// origin_slot from the bootstrap params. Name, net, seg_bits and fanout_bits
// remain immutable.
func OpenMutableHead(ctx context.Context, cfg archive.Config, roots Roots, params archive.Params) (*archive.Head, error) {
	return OpenHeadKind(ctx, cfg, roots, params, UnfinalizedMutable)
}

// OpenHeadKind is OpenHead with an explicit durable ordering contract. It is
// the supported startup path for both contracts. RootStore exposes its sibling
// GenerationStore through the small provider interface below; custom Roots
// implementations may embed RootStore (as the fault-injection tests do) to
// retain that provider.
func OpenHeadKind(ctx context.Context, cfg archive.Config, roots Roots, params archive.Params, kind HeadKind) (*archive.Head, error) {
	if kind == "" {
		kind = FinalizedMonotonic
	}
	states := generationStatesFromRoots(roots)
	if states == nil {
		if kind == UnfinalizedMutable {
			return nil, errors.New("server: mutable head requires a GenerationStore sharing the RootStore")
		}
		return openLegacyHead(ctx, cfg, roots, params)
	}
	baseline, err := states.EnsureKind(ctx, params.Name, kind)
	if err != nil {
		return nil, err
	}

	root, ok, err := roots.Get(ctx, params.Name)
	if err != nil {
		return nil, err
	}
	if !ok {
		if baseline.Generation != 0 {
			return nil, fmt.Errorf("server: mutable head %q generation %d has no root mirror", params.Name, baseline.Generation)
		}
		h, err := archive.New(ctx, cfg, params)
		if err != nil {
			return nil, fmt.Errorf("server: creating head %q: %w", params.Name, err)
		}
		// An empty head's root is persisted now rather than at the first
		// mutation: the block is written, and a restart before any refs arrive
		// should resume the head, not manufacture a second identical one.
		if err := roots.Put(ctx, params.Name, h.Root()); err != nil {
			return nil, err
		}
		return h, nil
	}

	if baseline.Generation > 0 {
		stateRoot, err := cid.Decode(baseline.Root)
		if err != nil {
			return nil, err
		}
		if !stateRoot.Equals(root) {
			return nil, fmt.Errorf("server: mutable head %q root mirror %s differs from generation %d root %s",
				params.Name, root, baseline.Generation, stateRoot)
		}
	}

	h, err := archive.Load(ctx, cfg, root)
	if err != nil {
		return nil, fmt.Errorf("server: loading head %q from root %s: %w", params.Name, root, err)
	}
	got := h.Params()
	if kind == UnfinalizedMutable && baseline.Generation > 0 {
		if got.Name != params.Name || got.Net != params.Net || got.SegBits != params.SegBits || got.FanoutBits != params.FanoutBits {
			return nil, &ParamsMismatchError{Name: params.Name, Want: params, Got: got}
		}
		info := h.Info()
		if got.OriginSlot != baseline.WindowStart || info.SyncedTo == nil || *info.SyncedTo != baseline.SyncedTo {
			return nil, fmt.Errorf("server: mutable head %q generation %d state covers [%d,%d] but root %s covers origin=%d synced_to=%v",
				params.Name, baseline.Generation, baseline.WindowStart, baseline.SyncedTo, root, got.OriginSlot, info.SyncedTo)
		}
	} else if got != params {
		return nil, &ParamsMismatchError{Name: params.Name, Want: params, Got: got}
	}
	return h, nil
}

// openLegacyHead preserves OpenHead's pre-generation-store behavior for an
// injected Roots implementation which does not expose a Pebble baseline store.
func openLegacyHead(ctx context.Context, cfg archive.Config, roots Roots, params archive.Params) (*archive.Head, error) {
	root, ok, err := roots.Get(ctx, params.Name)
	if err != nil {
		return nil, err
	}
	if !ok {
		h, err := archive.New(ctx, cfg, params)
		if err != nil {
			return nil, fmt.Errorf("server: creating head %q: %w", params.Name, err)
		}
		if err := roots.Put(ctx, params.Name, h.Root()); err != nil {
			return nil, err
		}
		return h, nil
	}
	h, err := archive.Load(ctx, cfg, root)
	if err != nil {
		return nil, fmt.Errorf("server: loading head %q from root %s: %w", params.Name, root, err)
	}
	if got := h.Params(); got != params {
		return nil, &ParamsMismatchError{Name: params.Name, Want: params, Got: got}
	}
	return h, nil
}

type generationStoreProvider interface {
	GenerationStore() *GenerationStore
}

func generationStatesFromRoots(roots Roots) GenerationStates {
	if p, ok := roots.(generationStoreProvider); ok {
		return p.GenerationStore()
	}
	return nil
}

type publicationStoreProvider interface {
	PublicationStore() *PublicationStore
}

func publicationRevisionsFromRoots(roots Roots) PublicationRevisions {
	if p, ok := roots.(publicationStoreProvider); ok {
		return p.PublicationStore()
	}
	return nil
}

// HeadsConfig is what a Heads registry needs beyond the heads themselves.
type HeadsConfig struct {
	// Net is the network every head must belong to, and the document's "net".
	Net string
	// Roots persists each head's current root. Required.
	Roots Roots
	// Generations is the bounded mutable-generation store. Nil derives the
	// store paired with a RootStore. A mutable writer policy requires it.
	Generations GenerationStates
	// Publications durably allocates signer-local publication revisions. Nil
	// derives the allocator paired with a RootStore. It is used only after a
	// durable mutable entry activates.
	Publications PublicationRevisions
	// Policies configures written mutable heads. Missing names retain the legacy
	// finalized-monotonic contract.
	Policies map[string]HeadPolicy
	// GenerationArchive is the archive machinery used to build a complete
	// mutable generation off-side. It may share blocks, resolver and cache with
	// the ordinary writer engines; BuildGeneration never mutates them.
	GenerationArchive archive.Config
	// Manifests persists each head's manifest chain tip (spec 10.5). Optional;
	// nil is a node with no manifest chains, on which SetManifest is unavailable
	// and every head's publication entry omits the manifest field. It is read at
	// Add and Adopt to resume a head's tip, and written by SetManifest and Adopt.
	Manifests *ManifestStore
	// Blocks is where SetManifest stores a Manifest block it accepts (spec 7.2,
	// 10.5). Required iff Manifests is set: a tip with no block is a dangling CID.
	Blocks blockstore.Blockstore
	// Gate keeps the online GC's T0 consistency cut out of every whole mutation
	// (spec 9); mark and sweep run concurrently afterward. Optional only for a
	// stack with no GC: a node which runs GC and leaves this nil can take its pin
	// snapshot after an early block write but before the root publication that
	// would account for it. pinning.Reconciler.Gate() is the one to pass -- the
	// same Gate the reconciler and GC share, since two gates coordinate nothing.
	//
	// Lock order: the gate is taken before Heads.mu and released after it,
	// never the reverse. A mutation that took mu first could hold the serializer
	// while it waited for the collector's cut (or a legacy whole-run sweep), and
	// every read of the document behind it. See Staging for the other half of the
	// ordering rule.
	Gate Gate
	// Staging drops the staging pins of a batch whose refs have landed (spec 9,
	// window (a)). Optional; nil is a registry that leaves them to expire.
	Staging Staging
	// TruncateWindowSlots reports a writable head's trailing-window retention
	// width in slots. Truncate uses it to protect sealed Segment closures which
	// become newly recursive when an emergency rewind moves the window
	// backwards during an online GC epoch. Return ok=false for full/none modes.
	// A daemon using window retention with online GC must configure this hook.
	TruncateWindowSlots func(head string) (slots uint64, ok bool)
	// Metrics instruments root swaps, adoptions, quarantine and per-head
	// synced_to/dir_depth. Optional; nil records nothing.
	Metrics *metrics.Metrics
	// Logger receives what a mutation has to say for itself. Optional.
	Logger *slog.Logger
	// Multiaddrs is published as the document's "multiaddrs": where to fetch
	// blocks from (spec 8). Empty omits the field, which is what a node with no
	// libp2p host has to say.
	Multiaddrs []string
	// MaxPutBlobs is published as the document's "max_put_blobs" (spec 7.2): the
	// archive's own POST /bloar/v1/blobs count limit, so an indexer can check its
	// configured max_put_blobs against it at startup (see Doc.MaxPutBlobs). It
	// MUST be the same value passed to server.Config.MaxPutBlobs -- both come from
	// server.max_put_blobs -- or the archive would advertise a limit it does not
	// enforce. Zero takes the spec default of 64, matching server.New.
	MaxPutBlobs int
	// ArchiveID enables publication version 3 and is signed into every document.
	// Independent writers of one logical archive configure the same value while
	// retaining distinct SigningKey, IPNS, URL, and state identities. It is an
	// identity, not an authorization policy. Optional; nil retains v1/v2 output.
	ArchiveID *ArchiveID
	// SigningKey signs the publication document. Optional; nil publishes
	// unsigned.
	SigningKey ed25519.PrivateKey
	// OnRoot is called after every root swap, with the head's name and its new
	// root. Optional; nil is no notification.
	//
	// It exists for pin reconciliation, which spec 9 requires after every root
	// swap. It is called with the mutation lock held and must not block or
	// re-enter this type: the contract is that it marks work to be done
	// elsewhere, not that it does any.
	OnRoot func(head string, root cid.Cid)
	// Replacements retarget consumers which retain an engine pointer (notably
	// the pin reconciler) after any local COW root transition is durable and
	// before it becomes visible. Every locally writable head which can mutate
	// requires its own startup-bound, infallible callback. A fallible callback
	// here would create an unsafe state: durability could name the candidate
	// while GC still reconciled the retired engine. A process crash in the
	// instruction gap is safe because restart opens the durable root before GC.
	Replacements map[string]func(head *archive.Head)
	// OnDoc is called after every rebuild of the publication document, with the
	// document exactly as GET /bloar/v1/heads serves it. Optional; nil is no
	// notification. The bytes are not the callee's to modify.
	//
	// It exists for the IPNS channel of spec 8.1, which stores the document as
	// a raw block and names that block in a record. The bytes are handed over
	// rather than fetched afterwards for the reason 8.1 stores the document at
	// all: both channels have to carry the identical document, and a reader
	// that went and got Doc() would be reading whatever the next mutation had
	// published by then.
	//
	// Same contract as OnRoot, and for a sharper reason: this runs under the
	// lock that makes root swaps and rebuilds one step, and publishing a name
	// is seconds of network. A hook that did its work here would put the DHT on
	// the critical path of every POST refs.
	OnDoc func(doc []byte)
}

// entry is one head in the registry: the engine, where its blobs are read from,
// and how this node relates to it. Entries are immutable once published; a
// change swaps a new one in.
type entry struct {
	head *archive.Head
	// blobs is the head's blob read path. Nil is the node's blockstore, which
	// is what a written head always wants; see Blobs.
	blobs Blobs
	// manifestTip is the head's manifest chain tip (spec 10.5), or cid.Undef for
	// a head with no chain. It is carried on the entry so that rebuild renders the
	// publication document's manifest field without a KV read; it is written to
	// the durable ManifestStore alongside, which is what a restart and the pin
	// reconciler read.
	manifestTip cid.Cid
	// durable is the head's last durably-published document line: the exact
	// HeadEntry rebuild renders, captured at the moments the head's publication
	// state last became durable, never from the live engine. It is what closes
	// the cross-head window of the safety boundary: a mutation swaps the engine to a new
	// root before commit persists it, so e.head.Info() can be a root the RootStore
	// does not yet hold; rendering every head from this record instead of its live
	// engine means a rebuild triggered by another head cannot announce that
	// non-durable root.
	//
	// It is (re)captured at exactly the moments durable publication state changes,
	// and nowhere else: Add (a new head, root loaded from the RootStore and so
	// durable by definition), a successful root commit (after Roots.Put), a
	// successful manifest persist (SetManifest, which advances the tip without a
	// root commit), and follower adoption after its root persists. Its Manifest
	// field is advanced by the manifest-persist point independently of the root so
	// that a manifest upgrade is neither suppressed nor later regressed by a root
	// rebuild.
	//
	// Nil is a head with no durable publication state yet: a followed head whose
	// first adoption failed to persist its root. It has no prior durable root to
	// fall back to, so rebuild omits it entirely rather than announce the volatile
	// one.
	durable *HeadEntry
	// followed marks a head this node replicates rather than writes (spec
	// 11.3). It is what the mutation endpoints refuse.
	followed bool
	// quarantine is the reason a followed head is no longer served, or "" for
	// a head in good standing. Spec 11.4: a blob that fails full verification
	// is evidence of a malicious writer, and the response is to stop serving
	// the head rather than to skip the blob. A quarantined entry stays in the
	// registry precisely so that its reads can say so; it is out of names, and
	// so out of the publication document and out of Get.
	quarantine string
	// republish says this entry is eligible for this node's publication
	// document. An unsigned follower may serve a mutable root but cannot make the
	// signed, revisioned authority claim required to republish it.
	republish bool
	// kind is the authenticated ordering contract of this local entry.
	kind HeadKind
	// generation is nonzero only for a selected mutable writer generation. It
	// distinguishes an exact retry already exposed from a post-commit crash that
	// still needs the bound replacement callback and the registry swap.
	generation uint64
	// lineage is the in-process append-only identity of a finalized entry. It is
	// preserved across ApplyRefs, rotated by Truncate, and necessarily new after
	// restart. A mutable generation captures the pointer at selection, which is
	// stronger than comparing roots: an ABA root cannot revive proof invalidated
	// by a rewrite.
	lineage *finalizedLineage
	// proof is the durable source/handoff claim of a mutable writer generation.
	// proofLineage is the finalized lineage observed when that claim was checked.
	// They are nil for finalized and legacy/proofless followed entries.
	proof        *GenerationState
	proofLineage *finalizedLineage
	// handoffWitness is the exact finalized line authenticated beside a followed
	// mutable line when this replica does not select that finalized head. It is
	// metadata only: never a registry entry, serving name, pin target, root mirror,
	// or publication line. metadataHandoff distinguishes that deliberate partial
	// selection from an ordinary mutable proof which has lost its physical
	// finalized lineage and must fail closed.
	handoffWitness  *HeadEntry
	metadataHandoff bool
	// proofValid is recomputed whenever the immutable registry is replaced. It
	// gates physical mutable reads, virtual provisional reads, and publication.
	proofValid bool
	// bindRestartProof permits exactly one startup anchor binding when Add order
	// registers the mutable engine before its handoff. Once the handoff exists it
	// is consumed, successful or not; later root ABA cannot re-arm it.
	bindRestartProof bool
}

// finalizedLineage is deliberately non-zero-sized: distinct allocations must
// have distinct addresses because pointer identity is the rewrite barrier.
type finalizedLineage struct{ _ byte }

// registry is the immutable set of heads, swapped wholesale when one changes so
// that the read path never locks.
type registry struct {
	byName map[string]*entry
	// names is every servable head, sorted: the registry minus whatever is
	// quarantined.
	names []string
}

// name returns e's head name. Every entry has a head; a followed head is not
// registered until its first adoption.
func (e *entry) name() string { return e.head.Params().Name }

// withRegistry returns a copy of reg with e registered under its name.
func (reg *registry) with(e *entry) *registry {
	next := &registry{byName: maps.Clone(reg.byName)}
	next.byName[e.name()] = e
	return cohereRegistry(next)
}

// cohereRegistry derives every mutable entry's availability from one registry
// snapshot. It also refreshes the mutable publication line to the exact current
// finalized handoff generation, so a signed document never combines entries
// from different points in time even though the durable generation state keeps
// the older tracker-observed anchor used to establish lineage.
func cohereRegistry(reg *registry) *registry {
	next := &registry{byName: maps.Clone(reg.byName)}
	for name, current := range next.byName {
		if current.kind != UnfinalizedMutable {
			continue
		}
		e := *current
		if e.bindRestartProof && e.proof != nil {
			if handoff := next.byName[e.proof.HandoffHead]; handoff != nil && handoff.durable != nil {
				e.bindRestartProof = false
				if handoff.durable.Root == e.proof.HandoffRoot && handoff.durable.SyncedTo != nil &&
					*handoff.durable.SyncedTo == e.proof.HandoffSyncedTo {
					e.proofLineage = handoff.lineage
				}
			}
		}
		// A metadata-only replica which later selects the exact witnessed
		// finalized generation may bind into the ordinary in-process lineage
		// model. Exact root/frontier equality is required for the first binding;
		// after that, the usual lineage identity governs advances and rewrites.
		if e.metadataHandoff && e.proofLineage == nil && e.proof != nil {
			if handoff := next.byName[e.proof.HandoffHead]; physicalHandoffMatchesWitness(handoff, e.handoffWitness) {
				e.proofLineage = handoff.lineage
			}
		}
		e.proofValid = mutableProofValid(next, &e)
		if e.proofValid && e.durable != nil {
			line := *e.durable
			if handoff := next.byName[e.proof.HandoffHead]; handoff != nil {
				line.HandoffRoot = handoff.durable.Root
				synced := *handoff.durable.SyncedTo
				line.HandoffSyncedTo = &synced
			} else {
				// metadataHandoffValid established the exact immutable witness.
				line.HandoffRoot = e.handoffWitness.Root
				synced := *e.handoffWitness.SyncedTo
				line.HandoffSyncedTo = &synced
			}
			e.durable = &line
		}
		next.byName[name] = &e
	}
	next.names = servable(next.byName)
	return next
}

func mutableProofValid(reg *registry, e *entry) bool {
	if e == nil || e.kind != UnfinalizedMutable || e.durable == nil || e.proof == nil ||
		e.proof.V != generationStateVersion || !mutableEntryMatchesProof(e) {
		return false
	}
	handoff, physical := reg.byName[e.proof.HandoffHead]
	if !physical {
		return metadataHandoffValid(e)
	}
	if handoff == nil || handoff.quarantine != "" || handoff.durable == nil ||
		handoff.kind != FinalizedMonotonic || handoff.durable.EffectiveKind() != FinalizedMonotonic ||
		handoff.durable.SyncedTo == nil || handoff.lineage == nil || e.proofLineage == nil || handoff.lineage != e.proofLineage {
		return false
	}
	frontier := *handoff.durable.SyncedTo
	if frontier < e.proof.HandoffSyncedTo || frontier > e.proof.SourceFinalizedSlot {
		return false
	}
	return frontier == math.MaxUint64 || e.proof.WindowStart <= frontier+1
}

func physicalHandoffMatchesWitness(handoff *entry, witness *HeadEntry) bool {
	return handoff != nil && handoff.quarantine == "" && handoff.durable != nil && handoff.lineage != nil &&
		handoff.kind == FinalizedMonotonic && handoff.durable.EffectiveKind() == FinalizedMonotonic &&
		witness != nil && witness.EffectiveKind() == FinalizedMonotonic && witness.SyncedTo != nil &&
		handoff.durable.Root == witness.Root && handoff.durable.SyncedTo != nil &&
		*handoff.durable.SyncedTo == *witness.SyncedTo
}

func metadataHandoffValid(e *entry) bool {
	if e == nil || !e.metadataHandoff || e.handoffWitness == nil || e.proof == nil || e.durable == nil {
		return false
	}
	witness := e.handoffWitness
	if witness.EffectiveKind() != FinalizedMonotonic || witness.SyncedTo == nil ||
		witness.WindowStart != nil || witness.SourceHeadRoot != "" || witness.SourceFinalizedSlot != nil ||
		witness.SourceFinalizedRoot != "" || witness.HandoffHead != "" || witness.HandoffRoot != "" ||
		witness.HandoffSyncedTo != nil {
		return false
	}
	return witness.Name == e.proof.HandoffHead && witness.Root == e.proof.HandoffRoot &&
		*witness.SyncedTo == e.proof.HandoffSyncedTo && e.durable.HandoffHead == witness.Name &&
		e.durable.HandoffRoot == witness.Root && e.durable.HandoffSyncedTo != nil &&
		*e.durable.HandoffSyncedTo == *witness.SyncedTo
}

// mutableEntryMatchesProof ties the served/archive-derived line to the durable
// proof object. HandoffRoot/HandoffSyncedTo are deliberately excluded: cohereRegistry
// refreshes those two signed fields to the current same-lineage finalized entry,
// while the proof retains the older commit anchor for restart binding.
func mutableEntryMatchesProof(e *entry) bool {
	if e == nil || e.head == nil || e.durable == nil || e.proof == nil {
		return false
	}
	line, proof := e.durable, e.proof
	info := e.head.Info()
	return line.EffectiveKind() == UnfinalizedMutable && line.Kind == UnfinalizedMutable &&
		line.Name == info.Name && line.Root == info.Root.String() && line.Root == proof.Root &&
		line.OriginSlot == info.OriginSlot && line.OriginSlot == proof.WindowStart &&
		equalOptionalSlot(line.SyncedTo, info.SyncedTo) && line.SyncedTo != nil && *line.SyncedTo == proof.SyncedTo &&
		line.SegBits == info.SegBits && line.FanoutBits == info.FanoutBits && line.DirDepth == info.DirDepth &&
		line.Manifest == "" && line.WindowStart != nil && *line.WindowStart == proof.WindowStart &&
		line.SourceHeadRoot == proof.SourceHeadRoot && line.SourceFinalizedSlot != nil &&
		*line.SourceFinalizedSlot == proof.SourceFinalizedSlot && line.SourceFinalizedRoot == proof.SourceFinalizedRoot &&
		line.HandoffHead == proof.HandoffHead && line.HandoffRoot != "" && line.HandoffSyncedTo != nil
}

// servable is the sorted names of every entry that is not quarantined.
func servable(byName map[string]*entry) []string {
	names := make([]string, 0, len(byName))
	for name, e := range byName {
		if e.quarantine == "" && e.durable != nil && (e.kind != UnfinalizedMutable || e.proofValid) {
			names = append(names, name)
		}
	}
	slices.Sort(names)
	return names
}

// published is one rendered publication document plus its per-head slices,
// built together so that GET /bloar/v1/heads/{head} cannot disagree with
// GET /bloar/v1/heads.
type published struct {
	doc         []byte
	heads       map[string][]byte
	generations map[string]uint64
}

// Heads is the set of heads this node serves: the registry the HTTP layer
// resolves names through, the serializer every mutation goes through, and the
// publisher of the document those mutations produce.
//
// # Written and followed heads
//
// A head is here because this node writes it (Add) or because this node follows
// it (Adopt), and spec 11.1 lets one node do both at once. The difference is
// narrow by construction: a followed head is the same engine over a root that
// arrived instead of one that was computed, so every read path treats the two
// identically and only two things distinguish them. Mutation is refused on a
// followed head, because its writer is elsewhere. And its blobs are read
// through the Blobs its adopter supplied, because they may not be local yet.
//
// Everything else -- the document, the root persistence, the reconciliation
// trigger -- is deliberately the same code, since a follower republishing what
// it has adopted is a publication document like any other, and a follower of a
// follower is thereby a thing that works.
//
// # Why mutation lives here and not in the handlers
//
// Spec 8 requires the document to be updated atomically with root swaps: a
// reader must never see a root that was never current. Three things therefore
// have to happen as one step -- the head engine's swap, the root's durability,
// and the document's rebuild -- and they can only be one step if a single
// object owns all three. That object is this one, and mu is the sequence: a
// mutation holds it from before the engine call until after the rebuild, so
// documents are published in mutation order and each one names roots that were
// current when it was built.
//
// Reads take no lock at all. The registry and the document are each read
// through an atomic pointer, so a GET during an ApplyRefs answers from the
// document the previous mutation published: stale by at most one mutation,
// never incoherent. Staleness is fine and inherent (the document is a
// point-in-time claim served over a network); a root that was never current is
// not.
type Heads struct {
	cfg HeadsConfig

	// mu serializes mutations and the document rebuilds they cause. It is never
	// held by a read.
	mu  sync.Mutex
	reg atomic.Pointer[registry]
	pub atomic.Pointer[published]

	// revisioned is the one-way publication-mode latch. It is restored from any
	// configured durable mutable generation at startup and set before the first
	// such generation is published. It never clears during the process, even if
	// the mutable entry is quarantined.
	revisioned bool
}

// ValidateFollowerStores proves that a follower's caller-owned Pebble batch is
// the durability mechanism for this registry's compatibility stores too.
// AdoptBatch intentionally trusts its Persist hook and does not perform a
// second Roots.Put/ManifestStore.Put afterward; accepting different databases
// would make the registry visible beside stale restart mirrors. Custom Roots
// implementations cannot expose this identity and are therefore refused for
// document-atomic follower adoption.
func (h *Heads) ValidateFollowerStores(kv *pebble.DB, roots *RootStore, manifests *ManifestStore) error {
	if kv == nil || roots == nil || roots.kv != kv {
		return errors.New("server: follower checkpoint and root mirror must share one Pebble database")
	}
	registryRoots, ok := h.cfg.Roots.(*RootStore)
	if !ok || registryRoots.kv != kv {
		return errors.New("server: follower registry Roots must be a RootStore on the checkpoint Pebble database")
	}
	if manifests == nil || manifests.kv != kv {
		return errors.New("server: follower checkpoint and manifest mirror must share one Pebble database")
	}
	if h.cfg.Manifests != nil && h.cfg.Manifests.kv != kv {
		return errors.New("server: follower registry ManifestStore must share the checkpoint Pebble database")
	}
	return nil
}

// NewHeads returns an empty registry. Heads are added with Add.
func NewHeads(cfg HeadsConfig) (*Heads, error) {
	if cfg.Net == "" {
		return nil, errors.New("server: HeadsConfig.Net must not be empty")
	}
	if cfg.Roots == nil {
		return nil, errors.New("server: HeadsConfig.Roots must not be nil")
	}
	if cfg.SigningKey != nil && len(cfg.SigningKey) != ed25519.PrivateKeySize {
		return nil, fmt.Errorf("server: signing key is %d bytes, want %d", len(cfg.SigningKey), ed25519.PrivateKeySize)
	}
	if cfg.Generations == nil {
		cfg.Generations = generationStatesFromRoots(cfg.Roots)
	}
	if cfg.Publications == nil {
		cfg.Publications = publicationRevisionsFromRoots(cfg.Roots)
	}
	if cfg.ArchiveID != nil {
		if cfg.ArchiveID.IsZero() {
			return nil, errors.New("server: HeadsConfig.ArchiveID must not be all zeroes")
		}
		if cfg.SigningKey == nil {
			return nil, errors.New("server: HeadsConfig.ArchiveID requires HeadsConfig.SigningKey")
		}
		if cfg.Publications == nil {
			return nil, errors.New("server: HeadsConfig.ArchiveID requires HeadsConfig.Publications")
		}
		archiveID := *cfg.ArchiveID
		cfg.ArchiveID = &archiveID
	}
	cfg.Policies = maps.Clone(cfg.Policies)
	cfg.Replacements = maps.Clone(cfg.Replacements)
	for name, policy := range cfg.Policies {
		if name == "" {
			return nil, errors.New("server: HeadsConfig.Policies contains an empty head name")
		}
		switch policy.effectiveKind() {
		case FinalizedMonotonic:
			if policy.HandoffHead != "" || policy.MaxWindowSlots != 0 {
				return nil, fmt.Errorf("server: finalized head policy %q carries mutable handoff/window fields", name)
			}
		case UnfinalizedMutable:
			switch {
			case cfg.SigningKey == nil:
				return nil, fmt.Errorf("server: mutable head %q requires HeadsConfig.SigningKey", name)
			case cfg.Generations == nil:
				return nil, fmt.Errorf("server: mutable head %q requires HeadsConfig.Generations", name)
			case cfg.Publications == nil:
				return nil, fmt.Errorf("server: mutable head %q requires HeadsConfig.Publications", name)
			case cfg.GenerationArchive.Blocks == nil:
				return nil, fmt.Errorf("server: mutable head %q requires HeadsConfig.GenerationArchive.Blocks", name)
			case cfg.GenerationArchive.Resolver == nil:
				return nil, fmt.Errorf("server: mutable head %q requires HeadsConfig.GenerationArchive.Resolver", name)
			case cfg.Replacements[name] == nil:
				return nil, fmt.Errorf("server: mutable head %q requires a startup-bound HeadsConfig.Replacements callback", name)
			case policy.HandoffHead == "":
				return nil, fmt.Errorf("server: mutable head %q requires a handoff head", name)
			case policy.HandoffHead == name:
				return nil, fmt.Errorf("server: mutable head %q cannot hand off to itself", name)
			case policy.MaxWindowSlots == 0:
				return nil, fmt.Errorf("server: mutable head %q requires a positive max window", name)
			}
		default:
			return nil, fmt.Errorf("server: head policy %q has unknown kind %q", name, policy.Kind)
		}
	}
	if cfg.Gate == nil {
		cfg.Gate = noGate{}
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.DiscardHandler)
	}
	// The same flooring server.New does, so the document advertises the limit the
	// Server actually enforces even when the caller left the field zero.
	if cfg.MaxPutBlobs == 0 {
		cfg.MaxPutBlobs = defaultMaxPutBlobs
	}
	h := &Heads{cfg: cfg}
	// Revisioned mode is a permanent signer-local downgrade floor. It can have
	// been activated by a mutable follower this node republished, or by a writer
	// policy removed since the prior run, so current configured generation state
	// alone is not sufficient to restore it.
	if cfg.SigningKey != nil && cfg.Publications != nil {
		signer := cfg.SigningKey.Public().(ed25519.PublicKey)
		_, active, err := cfg.Publications.Current(context.Background(), signer)
		if err != nil {
			return nil, err
		}
		h.revisioned = active
	}
	// A restart must not emit even a transient revisionless document before it
	// re-adds a previously selected mutable head: followers which saw that head
	// already hold the signer downgrade floor. Restore the latch from the bounded
	// durable state before the initial empty rebuild and its OnDoc callback.
	if cfg.Generations != nil {
		for name, policy := range cfg.Policies {
			if policy.effectiveKind() != UnfinalizedMutable {
				continue
			}
			st, ok, err := cfg.Generations.Get(context.Background(), name)
			if err != nil {
				return nil, err
			}
			if ok && st.Generation > 0 {
				h.revisioned = true
				break
			}
		}
	}
	h.reg.Store(&registry{byName: map[string]*entry{}})
	if err := h.rebuild(); err != nil {
		return nil, err
	}
	return h, nil
}

// Add registers a head this node writes, under the name in its own parameters.
//
// The head's root must already be durable. Add reads the RootStore under the
// mutation lock and requires the persisted root for the name to equal
// head.Root(); a head whose root the store does not hold, or holds differently,
// is refused without being registered, rebuilt, or signed. rebuild renders and
// signs this head from head.Root() as its first durable document line (see
// entry.durable), so registering a head whose root the store never persisted
// would announce a volatile root outside the OpenHead -> Add path -- the
// cross-head half of the safety boundary, reached without any rebuild. OpenHead, the
// supported way to bring a head up, persists the root first (a created head's
// now, a resumed head's by having loaded from it), so the daemon's
// OpenHead -> Add path always satisfies this. Add verifies but does not write
// the root: silently persisting here would surprise other callers and mask the
// very mistake the check exists to catch.
func (h *Heads) Add(head *archive.Head) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if err := h.checkNet(head); err != nil {
		return err
	}
	name := head.Params().Name
	policy := h.policy(name)
	kind := policy.effectiveKind()
	if _, dup := h.reg.Load().byName[name]; dup {
		return fmt.Errorf("server: head %q is already registered", name)
	}
	// The precondition: the head's live root is what rebuild will publish and
	// sign below, so the RootStore must already hold exactly it. Verify, do not
	// persist (see the doc comment).
	root := head.Root()
	persisted, ok, err := h.cfg.Roots.Get(context.Background(), name)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("server: head %q cannot be registered: its root %s is not persisted in the RootStore; "+
			"open the head through OpenHead so its root is durable before Add", name, root)
	}
	if !persisted.Equals(root) {
		return fmt.Errorf("server: head %q cannot be registered: its live root %s does not match the persisted root %s; "+
			"Add publishes only a durable root, so persist this root before registering the head", name, root, persisted)
	}
	var generation uint64
	var generationState *GenerationState
	if h.cfg.Generations != nil {
		state, err := h.cfg.Generations.EnsureKind(context.Background(), name, kind)
		if err != nil {
			return err
		}
		generation = state.Generation
		if generation > 0 {
			if kind == UnfinalizedMutable {
				if state.HandoffHead != policy.HandoffHead {
					return fmt.Errorf("server: mutable head %q generation %d was selected against handoff %q, current policy names %q",
						name, generation, state.HandoffHead, policy.HandoffHead)
				}
				if state.SyncedTo < state.WindowStart || state.SyncedTo-state.WindowStart >= policy.MaxWindowSlots {
					return fmt.Errorf("server: mutable head %q generation %d covers [%d,%d], above current max_window_slots %d",
						name, generation, state.WindowStart, state.SyncedTo, policy.MaxWindowSlots)
				}
			}
			stateRoot, err := cid.Decode(state.Root)
			if err != nil {
				return err
			}
			if !stateRoot.Equals(root) {
				return fmt.Errorf("server: mutable head %q generation %d selects root %s, live root is %s", name, generation, stateRoot, root)
			}
			if kind == UnfinalizedMutable {
				info := head.Info()
				if info.OriginSlot != state.WindowStart || info.SyncedTo == nil || *info.SyncedTo != state.SyncedTo {
					return fmt.Errorf("server: mutable head %q generation %d state window [%d,%d] does not describe loaded root origin/coverage",
						name, generation, state.WindowStart, state.SyncedTo)
				}
			}
			stateCopy := state
			generationState = &stateCopy
		}
	} else if kind == UnfinalizedMutable {
		return fmt.Errorf("server: mutable head %q requires a generation store", name)
	}
	// The head's manifest tip as persisted, so a restart resumes the head's
	// published manifest field without waiting for a mutation (spec 10.5). A head
	// with no chain has none, and the field stays omitted.
	tip, _, err := h.manifestTip(context.Background(), name)
	if err != nil {
		return err
	}
	// The head's position as opened, so that a node that has not applied a
	// batch since it started still reports where it is rather than nothing. Its
	// root is durable -- the precondition above checked that the RootStore holds
	// exactly it -- so it is also this head's first durable document line (see
	// entry.durable), which rebuild renders from here on.
	info := head.Info()
	var durable *HeadEntry
	entryToAdd := &entry{head: head, manifestTip: tip, kind: kind, generation: generation, republish: true}
	if kind == FinalizedMonotonic {
		pub := headEntryKind(info, tip, kind)
		durable = &pub
		entryToAdd.lineage = &finalizedLineage{}
	} else if generationState != nil && generationState.V == generationStateVersion {
		pub := headEntryGeneration(info, *generationState)
		durable = &pub
		entryToAdd.proof = generationState
		entryToAdd.bindRestartProof = true
	}
	// A selected legacy-v1 mutable generation is deliberately opened only as a
	// recovery source. It remains absent from serving and publication until the
	// tracker advances the CAS to a proof-aware v2 generation.
	entryToAdd.durable = durable
	prospective := h.reg.Load().with(entryToAdd)
	doc, err := h.buildPublication(prospective)
	if err != nil {
		return err
	}
	h.reg.Store(prospective)
	h.installPublication(doc)
	syncedTo, covered := uint64(0), info.SyncedTo != nil
	if covered {
		syncedTo = *info.SyncedTo
	}
	h.cfg.Metrics.HeadInfo(name, syncedTo, covered, info.DirDepth)
	h.cfg.Metrics.HeadStructure(name, sealedSegmentWindows(covered, syncedTo, info.OriginSlot, info.SegBits))
	h.restoreIndexSegmentMetrics(context.Background(), name, head)
	h.cfg.Metrics.Quarantined(name, false)
	return nil
}

func (h *Heads) policy(name string) HeadPolicy {
	if p, ok := h.cfg.Policies[name]; ok {
		return p
	}
	return HeadPolicy{Kind: FinalizedMonotonic}
}

// ValidateAdopt runs the deterministic, semantic refusals Adopt would apply, without
// mutating anything, so a follower can run them in its zero-effect adoption preflight
// and never durably checkpoint a generation Adopt is certain to reject (the safety boundary
// follow-up, the transition invariant). Adopt calls the same checks (validateAdoptLocked) as its
// first step, so the two paths cannot drift. It covers the predictable refusals:
// wrong net, a name this node writes rather than follows, a quarantined head, an
// immutable-params change against the already-followed generation, and a defined
// manifest tip on a node with no manifest store. It does NOT anticipate an I/O
// failure or a state change that races the follow-up commit; those the commit's own
// Adopt still surfaces.
func (h *Heads) ValidateAdopt(head *archive.Head, manifestTip cid.Cid) error {
	return h.ValidateAdoptKind(head, manifestTip, FinalizedMonotonic)
}

// ValidateAdoptKind is ValidateAdopt with the authenticated contract carried by
// the publication entry. Mutable replacements may move origin_slot; every
// other archive parameter remains immutable.
func (h *Heads) ValidateAdoptKind(head *archive.Head, manifestTip cid.Cid, kind HeadKind) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.validateAdoptLocked(head, manifestTip, kind)
}

// validateAdoptLocked is ValidateAdopt's body and Adopt's first step. The caller
// holds h.mu.
func (h *Heads) validateAdoptLocked(head *archive.Head, manifestTip cid.Cid, kind HeadKind) error {
	if kind == "" {
		kind = FinalizedMonotonic
	}
	if kind != FinalizedMonotonic && kind != UnfinalizedMutable {
		return fmt.Errorf("server: refusing to adopt unknown head kind %q", kind)
	}
	if err := h.checkNet(head); err != nil {
		return err
	}
	name := head.Params().Name
	if configured, ok := h.cfg.Policies[name]; ok && configured.effectiveKind() != kind {
		return &KindMismatchError{Name: name, Want: configured.effectiveKind(), Got: kind}
	}
	if kind == UnfinalizedMutable {
		if manifestTip.Defined() {
			return fmt.Errorf("server: refusing mutable head %q with finalized manifest tip %s", name, manifestTip)
		}
	}
	reg := h.reg.Load()
	if old, ok := reg.byName[name]; ok {
		if !old.followed {
			return fmt.Errorf("server: head %q is written by this node; it cannot also be followed", name)
		}
		if old.quarantine != "" {
			return fmt.Errorf("%w: %s", ErrQuarantined, old.quarantine)
		}
		if old.kind != kind {
			return &KindMismatchError{Name: name, Want: old.kind, Got: kind}
		}
		got, want := head.Params(), old.head.Params()
		paramsMatch := got == want
		if kind == UnfinalizedMutable {
			paramsMatch = got.Name == want.Name && got.Net == want.Net && got.SegBits == want.SegBits && got.FanoutBits == want.FanoutBits
		}
		if !paramsMatch {
			// Spec 3.1 makes these immutable for the life of a head, so this is
			// not a head that changed: it is a different head under a name this
			// node already had, which is either a writer that rebuilt one
			// without renaming it or a document that is not the one it claims.
			return fmt.Errorf("server: refusing to adopt %q at %s: it was built with net=%q origin_slot=%d "+
				"seg_bits=%d fanout_bits=%d, but the head this node already follows under that name has net=%q "+
				"origin_slot=%d seg_bits=%d fanout_bits=%d, and spec 3.1 makes those immutable for the life of a head",
				name, head.Root(), got.Net, got.OriginSlot, got.SegBits, got.FanoutBits,
				want.Net, want.OriginSlot, want.SegBits, want.FanoutBits)
		}
	}
	// A defined manifest tip needs a manifest store to record it (spec 10.5); Adopt's
	// putManifestTip would fail, so refuse it here in the preflight too.
	if manifestTip.Defined() && h.cfg.Manifests == nil {
		return fmt.Errorf("server: refusing to adopt %q with manifest tip %s: this node has no manifest store configured",
			name, manifestTip)
	}
	return nil
}

// Adopt registers head as a head this node follows (spec 11.3), reading its
// blobs through blobs, and makes it the head served under its name from here
// on. It is the follower's equivalent of a root swap: the same three steps
// ApplyRefs finishes with -- persist the root, republish the document, notify
// the pin reconciler -- around a root that arrived rather than one that was
// computed.
//
// It registers or replaces: every poll that adopts a fresher root calls it
// again with a fresh engine over that root, because a *archive.Head is one
// root's reader and a new root is a new one. Replacing is a pointer swap the
// read path never locks for, so a request in flight finishes against the root
// it started on -- which is a root the writer published, and so an answer that
// was true when it was given.
//
// The root is persisted for the same reason a writer's is: a restart resumes
// serving what was adopted without waiting for the network, and the
// no-regression floor (spec 11.3) is what keeps that from being a way back to
// an old root.
//
// Adopting over a head this node writes is refused. So is adopting a
// quarantined one: spec 11.4's quarantine is not a state a fresher document
// clears, because the document is signed by the writer the quarantine is
// evidence against.
//
// manifestTip is the head's manifest chain tip as this document carried it (spec
// 10.5), cid.Undef for a head with no chain. The caller has already applied the
// manifest-ancestry floor (spec 11.3) before adopting; this only records the
// accepted tip, so the republished document carries it onward and the pin
// reconciler retains its chain. It is persisted before the entry is swapped, the
// same publish-last ordering the root uses.
func (h *Heads) Adopt(ctx context.Context, head *archive.Head, blobs Blobs, manifestTip cid.Cid) error {
	return h.AdoptKind(ctx, head, blobs, manifestTip, FinalizedMonotonic)
}

// Adoption is one followed-head upsert in an authenticated document
// transition. Published carries the exact signed line from that document. It is
// required for an unfinalized-mutable head and may be nil for a legacy finalized
// adoption, in which case AdoptBatch derives the finalized line from Head.
// HandoffWitness optionally carries the exact finalized line authenticated in
// the same document as a mutable Published line. It is needed only when that
// finalized head is metadata-only for this replica; AdoptBatch never turns it
// into a physical registry entry or persists a compatibility mirror for it.
//
// Head, Published, HandoffWitness, and every pointer field reachable from the
// publication entries are copied into an immutable registry generation before
// Persist runs. Blobs is retained as the read-through source for the adopted
// head.
type Adoption struct {
	Head           *archive.Head
	Blobs          Blobs
	ManifestTip    cid.Cid
	Published      *HeadEntry
	HandoffWitness *HeadEntry
}

// AdoptionHooks join external durable selection and pointer-holding consumers
// to one registry transition. Persist is the only fallible step after the
// prospective registry and publication have been validated; it must durably
// record the complete document generation (including compatibility root and
// manifest mirrors) or return an error. BeforeVisible is then called exactly
// once and must be infallible. A follower uses it to apply a prevalidated
// reconciler batch before the new roots become reachable through Heads.
//
// BeforeVisible may be nil when there is no pointer-holding consumer. Persist is
// required even when the caller has no durable work (pass an explicit no-op):
// making that boundary visible prevents an accidentally crash-unsafe caller.
// The legacy single-head Adopt methods provide their write-through persistence
// on the caller's behalf.
type AdoptionHooks struct {
	Persist       func() error
	BeforeVisible func()
}

// AdoptKind is Adopt with the authenticated head contract made explicit.
func (h *Heads) AdoptKind(ctx context.Context, head *archive.Head, blobs Blobs, manifestTip cid.Cid, kind HeadKind) error {
	if kind == "" {
		kind = FinalizedMonotonic
	}
	if kind == UnfinalizedMutable {
		return errors.New("server: mutable follower adoption requires its exact proof-aware publication entry")
	}
	if kind != FinalizedMonotonic {
		return fmt.Errorf("server: refusing to adopt unknown head kind %q", kind)
	}
	return h.adoptOne(ctx, Adoption{Head: head, Blobs: blobs, ManifestTip: manifestTip})
}

// AdoptPublished adopts the exact authenticated publication entry. Mutable
// entries require this path so their source/handoff proof becomes part of the
// immutable serving registry; reconstructing a line from the archive root
// would silently discard the proof which licenses provisional service.
func (h *Heads) AdoptPublished(ctx context.Context, head *archive.Head, blobs Blobs, manifestTip cid.Cid, published HeadEntry) error {
	published = cloneHeadEntry(published)
	return h.adoptOne(ctx, Adoption{Head: head, Blobs: blobs, ManifestTip: manifestTip, Published: &published})
}

func cloneHeadEntry(e HeadEntry) HeadEntry {
	clone := func(value *uint64) *uint64 {
		if value == nil {
			return nil
		}
		v := *value
		return &v
	}
	e.SyncedTo = clone(e.SyncedTo)
	e.WindowStart = clone(e.WindowStart)
	e.SourceFinalizedSlot = clone(e.SourceFinalizedSlot)
	e.HandoffSyncedTo = clone(e.HandoffSyncedTo)
	return e
}

// validatePublishedHead rejects a malformed or mismatched authenticated line
// before AdoptPublished writes either the manifest mirror or compatibility
// root. Network callers normally arrive through Doc.ValidateContract, but this
// exported API is itself a trust boundary and must remain zero-effect on bad
// direct input.
func validatePublishedHead(head *archive.Head, manifestTip cid.Cid, published HeadEntry) error {
	if head == nil {
		return errors.New("server: cannot adopt a nil head")
	}
	info := head.Info()
	wantManifest := ""
	if manifestTip.Defined() {
		wantManifest = manifestTip.String()
	}
	if published.Name != info.Name || published.Root != info.Root.String() || published.OriginSlot != info.OriginSlot ||
		!equalOptionalSlot(published.SyncedTo, info.SyncedTo) || published.SegBits != info.SegBits ||
		published.FanoutBits != info.FanoutBits || published.DirDepth != info.DirDepth || published.Manifest != wantManifest {
		return fmt.Errorf("server: publication entry does not exactly describe adopted head %q at %s", info.Name, info.Root)
	}
	switch published.EffectiveKind() {
	case FinalizedMonotonic:
		if published.WindowStart != nil || published.SourceHeadRoot != "" || published.SourceFinalizedSlot != nil ||
			published.SourceFinalizedRoot != "" || published.HandoffHead != "" || published.HandoffRoot != "" ||
			published.HandoffSyncedTo != nil {
			return fmt.Errorf("server: finalized publication entry for %q carries mutable proof fields", info.Name)
		}
	case UnfinalizedMutable:
		if published.Kind != UnfinalizedMutable || published.WindowStart == nil || published.SyncedTo == nil ||
			published.SourceHeadRoot == "" || published.SourceFinalizedSlot == nil || published.SourceFinalizedRoot == "" ||
			published.HandoffHead == "" || published.HandoffRoot == "" || published.HandoffSyncedTo == nil {
			return fmt.Errorf("server: mutable publication entry for %q has incomplete proof fields", info.Name)
		}
		if published.HandoffHead == info.Name {
			return fmt.Errorf("server: mutable publication entry for %q names itself as finalized handoff", info.Name)
		}
		if *published.WindowStart != info.OriginSlot || *published.SyncedTo < *published.WindowStart || published.Manifest != "" {
			return fmt.Errorf("server: mutable publication entry for %q has an incoherent window", info.Name)
		}
		if _, err := parseBeaconRoot(published.SourceHeadRoot); err != nil {
			return fmt.Errorf("server: mutable publication entry for %q has invalid source_head_root: %w", info.Name, err)
		}
		if _, err := parseBeaconRoot(published.SourceFinalizedRoot); err != nil {
			return fmt.Errorf("server: mutable publication entry for %q has invalid source_finalized_root: %w", info.Name, err)
		}
		if _, err := cid.Decode(published.HandoffRoot); err != nil {
			return fmt.Errorf("server: mutable publication entry for %q has invalid handoff_root: %w", info.Name, err)
		}
		if *published.SourceFinalizedSlot > *published.SyncedTo || *published.HandoffSyncedTo > *published.SourceFinalizedSlot ||
			*published.HandoffSyncedTo != math.MaxUint64 && *published.WindowStart > *published.HandoffSyncedTo+1 {
			return fmt.Errorf("server: mutable publication entry for %q has an incoherent handoff proof", info.Name)
		}
	default:
		return fmt.Errorf("server: publication entry for %q has unknown kind %q", info.Name, published.Kind)
	}
	return nil
}

func validateHandoffWitness(published, witness HeadEntry) error {
	if published.EffectiveKind() != UnfinalizedMutable {
		return fmt.Errorf("server: finalized publication entry %q cannot carry a handoff witness", published.Name)
	}
	// ValidateContract is the authoritative same-document relationship check.
	// The placeholder signature fields are non-empty only to exercise the
	// revisioned schema; authentication was performed by the Adoption caller.
	revision := uint64(1)
	doc := Doc{
		Unsigned:  Unsigned{V: DocVersion, Net: "adoption", Heads: []HeadEntry{published, witness}, Revision: &revision},
		Pubkey:    "authenticated-adoption",
		Signature: "authenticated-adoption",
	}
	if err := doc.ValidateContract(); err != nil {
		return fmt.Errorf("server: authenticated handoff witness for mutable head %q is not its exact same-document finalized line: %w",
			published.Name, err)
	}
	return nil
}

func equalOptionalSlot(a, b *uint64) bool {
	return a == nil && b == nil || a != nil && b != nil && *a == *b
}

// adoptOne preserves the original single-head API's write-through mirrors while
// routing its visibility change through the document-atomic batch machinery.
func (h *Heads) adoptOne(ctx context.Context, adoption Adoption) error {
	return h.AdoptBatch(ctx, []Adoption{adoption}, nil, AdoptionHooks{Persist: func() error {
		name := adoption.Head.Params().Name
		if adoption.ManifestTip.Defined() {
			if err := h.putManifestTip(ctx, name, adoption.ManifestTip); err != nil {
				return err
			}
		} else if err := h.clearManifestTip(ctx, name); err != nil {
			return err
		}
		return h.cfg.Roots.Put(context.WithoutCancel(ctx), name, adoption.Head.Root())
	}})
}

// AdoptBatch installs one complete authenticated followed-head document
// generation. Upserts and withdrawals are validated against one old registry,
// composed into one prospective registry, cohered once, and rendered once before
// any caller-owned durable state changes. Persist is then allowed to fail with
// zero serving/publication effect. After it succeeds, BeforeVisible and the two
// atomic pointer stores are deliberately infallible.
//
// A withdrawal removes a followed entry from serving/publication. Withdrawing an
// unknown name is an idempotent no-op, which lets a first accepted document
// persist an omission tombstone. A quarantined entry remains as a nonservable
// in-memory tombstone so omission cannot clear the process-lifetime quarantine.
func (h *Heads) AdoptBatch(ctx context.Context, upserts []Adoption, withdrawals []string, hooks AdoptionHooks) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if len(upserts) == 0 && len(withdrawals) == 0 {
		return errors.New("server: empty adoption batch")
	}
	if hooks.Persist == nil {
		return errors.New("server: adoption batch requires a Persist hook")
	}

	type preparedAdoption struct {
		adoption Adoption
		name     string
		kind     HeadKind
		pub      HeadEntry
		witness  *HeadEntry
	}
	prepared := make([]preparedAdoption, 0, len(upserts))
	changed := make(map[string]string, len(upserts)+len(withdrawals))
	for i, adoption := range upserts {
		if adoption.Head == nil {
			return fmt.Errorf("server: adoption %d has a nil head", i)
		}
		name := adoption.Head.Params().Name
		if prior, duplicate := changed[name]; duplicate {
			return fmt.Errorf("server: head %q appears in both/duplicate adoption changes (%s and upsert)", name, prior)
		}
		changed[name] = "upsert"

		kind := FinalizedMonotonic
		pub := headEntryKind(adoption.Head.Info(), adoption.ManifestTip, kind)
		if adoption.Published != nil {
			pub = cloneHeadEntry(*adoption.Published)
			kind = pub.EffectiveKind()
			if err := validatePublishedHead(adoption.Head, adoption.ManifestTip, pub); err != nil {
				return fmt.Errorf("server: adoption %d: %w", i, err)
			}
		}
		if kind == UnfinalizedMutable && adoption.Published == nil {
			return fmt.Errorf("server: adoption %d: mutable follower adoption has no authenticated proof entry", i)
		}
		var witness *HeadEntry
		if adoption.HandoffWitness != nil {
			copy := cloneHeadEntry(*adoption.HandoffWitness)
			if err := validateHandoffWitness(pub, copy); err != nil {
				return fmt.Errorf("server: adoption %d: %w", i, err)
			}
			witness = &copy
		}
		if err := h.validateAdoptLocked(adoption.Head, adoption.ManifestTip, kind); err != nil {
			return fmt.Errorf("server: adoption %d: %w", i, err)
		}
		prepared = append(prepared, preparedAdoption{adoption: adoption, name: name, kind: kind, pub: pub, witness: witness})
	}
	for i, name := range withdrawals {
		if name == "" {
			return fmt.Errorf("server: withdrawal %d has an empty head name", i)
		}
		if prior, duplicate := changed[name]; duplicate {
			return fmt.Errorf("server: head %q appears in both/duplicate adoption changes (%s and withdrawal)", name, prior)
		}
		changed[name] = "withdrawal"
		if old := h.reg.Load().byName[name]; old != nil && !old.followed {
			return fmt.Errorf("server: head %q is written by this node; it cannot be withdrawn by a follower document", name)
		}
	}

	// Compose without calling registry.with: coherence must run exactly once over
	// the complete document generation, after every finalized and mutable line is
	// present. Start with withdrawals, retaining only quarantine tombstones.
	byName := maps.Clone(h.reg.Load().byName)
	for _, name := range withdrawals {
		old := byName[name]
		if old == nil {
			continue
		}
		if old.quarantine == "" {
			delete(byName, name)
			continue
		}
		tombstone := *old
		tombstone.durable = nil
		tombstone.manifestTip = cid.Undef
		tombstone.proofValid = false
		byName[name] = &tombstone
	}

	// Finalized entries establish the lineage namespace mutable proofs bind to.
	// Exact same-root replay keeps an existing token; every distinct replacement
	// gets a new one, even at equal coverage.
	for _, p := range prepared {
		if p.kind != FinalizedMonotonic {
			continue
		}
		lineage := &finalizedLineage{}
		if old := h.reg.Load().byName[p.name]; old != nil && old.kind == FinalizedMonotonic && old.lineage != nil &&
			old.head.Root().Equals(p.adoption.Head.Root()) {
			lineage = old.lineage
		}
		pub := p.pub
		byName[p.name] = &entry{head: p.adoption.Head, blobs: p.adoption.Blobs, manifestTip: p.adoption.ManifestTip,
			followed: true, durable: &pub, kind: FinalizedMonotonic, republish: true, lineage: lineage}
	}

	for _, p := range prepared {
		if p.kind != UnfinalizedMutable {
			continue
		}
		pub := p.pub
		proof := &GenerationState{
			V: generationStateVersion, Kind: UnfinalizedMutable, Root: pub.Root,
			WindowStart: *pub.WindowStart, SyncedTo: *pub.SyncedTo,
			SourceHeadRoot: pub.SourceHeadRoot, SourceHeadSlot: *pub.SyncedTo,
			SourceFinalizedSlot: *pub.SourceFinalizedSlot, SourceFinalizedRoot: pub.SourceFinalizedRoot,
			ObservedHandoffRoot: pub.HandoffRoot, ObservedHandoffSyncedTo: *pub.HandoffSyncedTo,
			HandoffHead: pub.HandoffHead, HandoffRoot: pub.HandoffRoot, HandoffSyncedTo: *pub.HandoffSyncedTo,
		}
		var proofLineage *finalizedLineage
		handoff := byName[proof.HandoffHead]
		if handoff != nil && handoff.durable != nil &&
			handoff.kind == FinalizedMonotonic && handoff.durable.SyncedTo != nil &&
			handoff.durable.Root == proof.HandoffRoot && *handoff.durable.SyncedTo == proof.HandoffSyncedTo {
			proofLineage = handoff.lineage
		}
		metadataHandoff := p.witness != nil && handoff == nil
		republish := h.cfg.SigningKey != nil && h.cfg.Publications != nil && !metadataHandoff
		byName[p.name] = &entry{head: p.adoption.Head, blobs: p.adoption.Blobs, manifestTip: cid.Undef,
			followed: true, durable: &pub, kind: UnfinalizedMutable, republish: republish,
			proof: proof, proofLineage: proofLineage, handoffWitness: p.witness, metadataHandoff: metadataHandoff}
	}

	prospective := cohereRegistry(&registry{byName: byName})
	for _, p := range prepared {
		if p.kind != UnfinalizedMutable {
			continue
		}
		selected := prospective.byName[p.name]
		if selected == nil || !selected.proofValid {
			return fmt.Errorf("server: mutable adoption %q does not form a coherent finalized handoff pair in this batch", p.name)
		}
	}
	doc, err := h.buildPublication(prospective)
	if err != nil {
		return err
	}
	if err := hooks.Persist(); err != nil {
		return err
	}
	if hooks.BeforeVisible != nil {
		hooks.BeforeVisible()
	}
	h.reg.Store(prospective)
	h.installPublication(doc)

	for _, p := range prepared {
		if p.kind == UnfinalizedMutable && prospective.byName[p.name].republish {
			h.revisioned = true
		}
		h.cfg.Metrics.Adoption(p.name)
		h.cfg.Metrics.Quarantined(p.name, false)
		h.notifyRootSwap(p.name, p.adoption.Head)
	}
	return nil
}

// ErrQuarantined reports a head this node has stopped serving (spec 11.4).
var ErrQuarantined = errors.New("server: head is quarantined")

// Quarantine stops serving the named head and says why (spec 11.4). The head
// leaves the publication document and Get immediately; its beacon reads answer
// 503 rather than 404, because a 404 there is a statement about blobs and this
// is a statement about the node.
//
// It is one-way for the life of the process. The thing quarantine responds to
// is a writer whose signature vouched for data that does not verify, and the
// only thing that could clear it is a decision an operator makes -- by
// restarting this node against a different key, or the same one having found
// out what happened.
func (h *Heads) Quarantine(name, reason string) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	reg := h.reg.Load()
	old, ok := reg.byName[name]
	if !ok {
		return fmt.Errorf("%w: %q", ErrUnknownHead, name)
	}
	if old.quarantine != "" {
		// Local serving already failed closed on the first attempt. Retry the
		// publication withdrawal because an earlier allocator/signing failure may
		// have left the last externally distributed document stale.
		return h.rebuild()
	}
	if reason == "" {
		return errors.New("server: refusing to quarantine a head without a reason")
	}
	h.reg.Store(reg.with(&entry{head: old.head, blobs: old.blobs, manifestTip: old.manifestTip,
		durable: old.durable, followed: old.followed, quarantine: reason, kind: old.kind, generation: old.generation,
		republish: old.republish, lineage: old.lineage, proof: old.proof, proofLineage: old.proofLineage,
		handoffWitness: old.handoffWitness, metadataHandoff: old.metadataHandoff,
		proofValid: old.proofValid, bindRestartProof: old.bindRestartProof}))
	h.cfg.Metrics.Quarantined(name, true)
	return h.rebuild()
}

// Quarantined returns the reason the named head is quarantined, if it is.
func (h *Heads) Quarantined(name string) (string, bool) {
	e, ok := h.reg.Load().byName[name]
	if !ok || e.quarantine == "" {
		return "", false
	}
	return e.quarantine, true
}

// checkNet rejects a head from another network. The caller holds mu.
func (h *Heads) checkNet(head *archive.Head) error {
	if params := head.Params(); params.Net != h.cfg.Net {
		return fmt.Errorf("server: head %q is on net %q, this node publishes net %q", params.Name, params.Net, h.cfg.Net)
	}
	return nil
}

// Get returns the named head. A quarantined head is not one: it is registered
// only so that the read path can answer for it.
func (h *Heads) Get(name string) (*archive.Head, bool) {
	e, ok := h.entry(name)
	if !ok || e.quarantine != "" || e.durable == nil || e.kind == UnfinalizedMutable && !e.proofValid {
		return nil, false
	}
	return e.head, true
}

// entry returns the named registry entry, quarantined or not. It is the read
// path's resolution step; Get is the same thing for callers that only want a
// head they can serve from.
func (h *Heads) entry(name string) (*entry, bool) {
	e, ok := h.reg.Load().byName[name]
	return e, ok
}

// liveEntries returns both physical entries from one immutable registry
// generation. A caller must not resolve the names independently: a finalized
// root swap and a mutable replacement may occur between two loads, producing a
// pairing which never existed and an incorrect handoff decision.
func (h *Heads) liveEntries(view LiveHead) (finalized, unfinalized *entry) {
	reg := h.reg.Load()
	return reg.byName[view.FinalizedHead], reg.byName[view.UnfinalizedHead]
}

// validateLiveHeads checks the low-level server configuration against physical
// entries which are already registered. Missing entries are allowed here so a
// first-start follower can expose a retryable virtual view before its initial
// adoption; cmd/bloard validates that both names are declared in its static
// writer/follower configuration.
func validateLiveHeads(heads *Heads, views map[string]LiveHead) error {
	if len(views) == 0 {
		return nil
	}
	reg := heads.reg.Load()
	for name, view := range views {
		switch {
		case name == "":
			return errors.New("server: Config.LiveHeads contains an empty virtual name")
		case strings.Contains(name, "/"):
			return fmt.Errorf("server: virtual live head %q is not one URL path segment", name)
		case view.FinalizedHead == "":
			return fmt.Errorf("server: virtual live head %q has an empty finalized head", name)
		case view.UnfinalizedHead == "":
			return fmt.Errorf("server: virtual live head %q has an empty unfinalized head", name)
		case view.FinalizedHead == view.UnfinalizedHead:
			return fmt.Errorf("server: virtual live head %q maps both roles to physical head %q", name, view.FinalizedHead)
		}
		if _, collision := reg.byName[name]; collision {
			return fmt.Errorf("server: virtual live head %q collides with a physical head", name)
		}
		if e := reg.byName[view.FinalizedHead]; e != nil && e.kind != FinalizedMonotonic {
			return fmt.Errorf("server: virtual live head %q finalized head %q is %q, want %q",
				name, view.FinalizedHead, e.kind, FinalizedMonotonic)
		}
		if e := reg.byName[view.UnfinalizedHead]; e != nil && e.kind != UnfinalizedMutable {
			return fmt.Errorf("server: virtual live head %q unfinalized head %q is %q, want %q",
				name, view.UnfinalizedHead, e.kind, UnfinalizedMutable)
		}
		if policy, written := heads.cfg.Policies[view.UnfinalizedHead]; written &&
			policy.effectiveKind() == UnfinalizedMutable && policy.HandoffHead != view.FinalizedHead {
			return fmt.Errorf("server: virtual live head %q pairs mutable head %q with finalized head %q, but its handoff head is %q",
				name, view.UnfinalizedHead, view.FinalizedHead, policy.HandoffHead)
		}
	}
	return nil
}

// Names returns the registered head names, sorted.
func (h *Heads) Names() []string { return h.reg.Load().names }

// ManifestTip returns the named head's manifest chain tip, or (undef, false) for
// a head with no chain or one this node does not have. It reads the in-memory
// entry, which is what rebuild renders from; the durable copy is in ManifestStore
// and the pin reconciler reads that one.
func (h *Heads) ManifestTip(name string) (cid.Cid, bool) {
	e, ok := h.reg.Load().byName[name]
	if !ok || !e.manifestTip.Defined() {
		return cid.Undef, false
	}
	return e.manifestTip, true
}

// manifestTip reads a head's persisted manifest tip. A node with no ManifestStore
// configured has no chains, so every head's tip is undefined.
func (h *Heads) manifestTip(ctx context.Context, name string) (cid.Cid, bool, error) {
	if h.cfg.Manifests == nil {
		return cid.Undef, false, nil
	}
	return h.cfg.Manifests.Get(ctx, name)
}

// putManifestTip records a head's new manifest tip durably. The caller holds mu.
func (h *Heads) putManifestTip(ctx context.Context, name string, tip cid.Cid) error {
	if h.cfg.Manifests == nil {
		return fmt.Errorf("server: head %q has a manifest tip but this node has no ManifestStore configured", name)
	}
	return h.cfg.Manifests.Put(ctx, name, tip)
}

// clearManifestTip removes a head's persisted manifest tip, for a generation with
// no chain, so the compatibility mirror stays EXACT. A
// node with no ManifestStore has no chains and nothing to clear. The caller holds
// mu.
func (h *Heads) clearManifestTip(ctx context.Context, name string) error {
	if h.cfg.Manifests == nil {
		return nil
	}
	return h.cfg.Manifests.Delete(ctx, name)
}

// Doc returns the current publication document (spec 8) as served JSON.
func (h *Heads) Doc() []byte { return h.pub.Load().doc }

// HeadDoc returns the named head's entry in the current document.
func (h *Heads) HeadDoc(name string) ([]byte, bool) {
	b, ok := h.pub.Load().heads[name]
	return b, ok
}

// GenerationState returns the durable kind/generation record for a configured
// mutable writer. Generation zero is the initialized-but-unbuilt state.
func (h *Heads) GenerationState(ctx context.Context, name string) (GenerationState, error) {
	policy, ok := h.cfg.Policies[name]
	if !ok || policy.effectiveKind() != UnfinalizedMutable {
		return GenerationState{}, fmt.Errorf("%w: %q", ErrUnknownHead, name)
	}
	if h.cfg.Generations == nil {
		return GenerationState{}, fmt.Errorf("server: mutable head %q has no generation store", name)
	}
	st, ok, err := h.cfg.Generations.Get(ctx, name)
	if err != nil {
		return GenerationState{}, err
	}
	if !ok {
		return GenerationState{}, fmt.Errorf("server: mutable head %q has no durable kind baseline; open it through OpenMutableHead", name)
	}
	if st.Kind != UnfinalizedMutable {
		return GenerationState{}, &KindMismatchError{Name: name, Want: UnfinalizedMutable, Got: st.Kind}
	}
	return st, nil
}

// GenerationStatus couples the durable mutable CAS record to the two
// post-commit visibility steps a stateless writer may need to heal after an
// ambiguous request. Exposed means the lock-free serving registry selects this
// local generation; Published means the current signed document was rebuilt
// from this exact local generation, not merely from an identical root.
type GenerationStatus struct {
	GenerationState
	Exposed   bool `json:"exposed"`
	Published bool `json:"published"`
}

// GenerationStatus returns durable state plus its in-process exposure and
// publication status.
func (h *Heads) GenerationStatus(ctx context.Context, name string) (GenerationStatus, error) {
	st, err := h.GenerationState(ctx, name)
	if err != nil {
		return GenerationStatus{}, err
	}
	status := GenerationStatus{GenerationState: st}
	if st.Generation == 0 {
		return status, nil
	}
	e, ok := h.reg.Load().byName[name]
	root, rootErr := cid.Parse(st.Root)
	status.Exposed = rootErr == nil && ok && e.quarantine == "" && e.durable != nil &&
		e.generation == st.Generation && e.head.Root().Equals(root) && e.proofValid
	status.Published = h.generationPublished(name, st)
	return status, nil
}

// ReplaceGeneration builds and selects one complete bounded mutable snapshot.
// The candidate engine is built independently of the currently served engine.
// Selection writes its root mirror and complete GenerationState in one synced
// Pebble batch, then retargets pointer-holding consumers before exposing it.
//
// Exact retry is deliberately more than a cheap response: if the first request
// durably committed and then failed or crashed before the replacement callback, registry swap,
// or publication, the retry reconstructs and finishes those owed steps without
// allocating another local generation.
func (h *Heads) ReplaceGeneration(ctx context.Context, name string, req GenerationRequest) (GenerationResponse, error) {
	policy, configured := h.cfg.Policies[name]
	if !configured || policy.effectiveKind() != UnfinalizedMutable {
		return GenerationResponse{}, fmt.Errorf("%w: %q", ErrUnknownHead, name)
	}
	normalized, rows, digest, err := normalizeGeneration(req)
	if err != nil {
		return GenerationResponse{}, &GenerationValidationError{Reason: err.Error()}
	}
	digestHex := hex.EncodeToString(digest[:])

	// A complete generation writes a fresh DAG. Keep the GC cut out from before
	// those first writes until the durable selector and exposure are settled.
	h.cfg.Gate.Enter()
	defer h.cfg.Gate.Leave()

	// Read the authoritative durable CAS state, not the registry: a previous
	// attempt may have committed it and crashed before exposure.
	current, err := h.GenerationState(ctx, name)
	if err != nil {
		return GenerationResponse{}, err
	}
	if req.ExpectedGeneration != current.Generation {
		if req.ExpectedGeneration != math.MaxUint64 && req.ExpectedGeneration+1 == current.Generation && current.RequestDigest == digestHex {
			return h.finishGenerationRetry(ctx, name, current, rows)
		}
		return GenerationResponse{}, &GenerationConflictError{
			Head: name, ExpectedGeneration: req.ExpectedGeneration, CurrentGeneration: current.Generation,
			Reason: "expected_generation is stale or its request differs from the selected generation",
		}
	}
	if current.Generation == math.MaxUint64 {
		return GenerationResponse{}, ErrGenerationOverflow
	}
	if req.WindowStart > req.SyncedTo {
		return GenerationResponse{}, &GenerationValidationError{Reason: fmt.Sprintf("window_start %d is above synced_to %d", req.WindowStart, req.SyncedTo)}
	}
	// Width is (synced-start+1). Express the upper-bound check as a difference
	// so synced_to==MaxUint64 cannot wrap.
	if req.SyncedTo-req.WindowStart >= policy.MaxWindowSlots {
		width := fmt.Sprintf("%d", req.SyncedTo-req.WindowStart+1)
		if req.SyncedTo-req.WindowStart == math.MaxUint64 {
			width = "2^64"
		}
		return GenerationResponse{}, &GenerationValidationError{Reason: fmt.Sprintf(
			"generation covers %s slots, above max_window_slots %d", width, policy.MaxWindowSlots)}
	}
	if req.SourceFinalizedSlot > req.SyncedTo {
		return GenerationResponse{}, &GenerationValidationError{Reason: fmt.Sprintf(
			"source_finalized_slot %d is above source head slot/synced_to %d", req.SourceFinalizedSlot, req.SyncedTo)}
	}
	if req.ObservedHandoffSyncedTo > req.SourceFinalizedSlot {
		return GenerationResponse{}, &GenerationValidationError{Reason: fmt.Sprintf(
			"observed_handoff_synced_to %d is above source_finalized_slot %d", req.ObservedHandoffSyncedTo, req.SourceFinalizedSlot)}
	}
	if req.ObservedHandoffSyncedTo != math.MaxUint64 && req.WindowStart > req.ObservedHandoffSyncedTo+1 {
		return GenerationResponse{}, &GenerationValidationError{Reason: fmt.Sprintf(
			"window_start %d would leave a gap above observed handoff synced_to %d", req.WindowStart, req.ObservedHandoffSyncedTo)}
	}
	if uint64(len(rows)) > policy.MaxWindowSlots {
		return GenerationResponse{}, &GenerationValidationError{Reason: fmt.Sprintf(
			"generation carries %d blob rows, above max_window_slots %d", len(rows), policy.MaxWindowSlots)}
	}

	// Bind the tracker-observed finalized anchor exactly before the potentially
	// slow build. Ordinary finalized appends may happen while building, but only
	// this captured in-process lineage can license them at commit.
	h.mu.Lock()
	e, err := h.mutableWritableLocked(name)
	if err != nil {
		h.mu.Unlock()
		return GenerationResponse{}, err
	}
	lineage, err := h.bindObservedHandoffLocked(name, policy, req, current.Generation)
	if err != nil {
		h.mu.Unlock()
		return GenerationResponse{}, err
	}
	base := e.head.Params()
	h.mu.Unlock()
	params := base
	params.OriginSlot = req.WindowStart
	candidate, err := archive.BuildGeneration(ctx, h.cfg.GenerationArchive, params, rows, req.SyncedTo)
	if err != nil {
		h.recordIndexNodeLimitRefusal(name, err)
		return GenerationResponse{}, err
	}

	// Recheck the CAS and lineage after the build. ALL may have advanced while
	// blobs were resolved only if it is still the same append-only lineage, has
	// not outrun the candidate's trusted source-finalized watermark, and leaves
	// no handoff gap.
	h.mu.Lock()
	defer h.mu.Unlock()
	current, err = h.generationStateLocked(ctx, name)
	if err != nil {
		return GenerationResponse{}, err
	}
	if current.Generation != req.ExpectedGeneration {
		if req.ExpectedGeneration != math.MaxUint64 && req.ExpectedGeneration+1 == current.Generation && current.RequestDigest == digestHex {
			return h.finishGenerationRetryLocked(ctx, name, current, rows)
		}
		return GenerationResponse{}, &GenerationConflictError{Head: name, ExpectedGeneration: req.ExpectedGeneration,
			CurrentGeneration: current.Generation, Reason: "another generation was selected during the build"}
	}
	if _, err := h.mutableWritableLocked(name); err != nil {
		return GenerationResponse{}, err
	}
	handoff, handoffSynced, err := h.validateAdvancedHandoffLocked(name, policy, req, current.Generation, lineage)
	if err != nil {
		return GenerationResponse{}, err
	}
	next := GenerationState{
		V: generationStateVersion, Kind: UnfinalizedMutable, Generation: current.Generation + 1,
		RequestDigest: digestHex, Root: candidate.Root().String(), WindowStart: normalized.WindowStart,
		SyncedTo: normalized.SyncedTo, SourceHeadRoot: normalized.SourceHeadRoot,
		SourceHeadSlot: normalized.SyncedTo, SourceFinalizedSlot: normalized.SourceFinalizedSlot,
		SourceFinalizedRoot: normalized.SourceFinalizedRoot,
		ObservedHandoffRoot: normalized.ObservedHandoffRoot, ObservedHandoffSyncedTo: normalized.ObservedHandoffSyncedTo,
		HandoffHead: policy.HandoffHead,
		HandoffRoot: handoff.durable.Root, HandoffSyncedTo: handoffSynced,
	}
	prospective, doc, err := h.prospectiveGenerationLocked(name, candidate, next, lineage)
	if err != nil {
		return GenerationResponse{}, err
	}
	// The engine build is complete. From here cancellation cannot roll it back,
	// so the selector commit must get the same chance to finish as an ordinary
	// root commit after an in-memory engine swap.
	if err := h.cfg.Generations.Commit(context.WithoutCancel(ctx), name, current.Generation, candidate.Root(), next); err != nil {
		return GenerationResponse{}, err
	}
	h.cfg.Replacements[name](candidate)
	h.reg.Store(prospective)
	h.installPublication(doc)
	h.notifyRootSwap(name, candidate)
	h.recordIndexApply(name, candidate.LastApplyStats())
	h.dropStaging(context.WithoutCancel(ctx), name, rows)
	return generationResponse(next, false), nil
}

// GenerationValidationError is a body/config-independent malformed generation
// request. The HTTP layer maps it to 400 rather than inviting a state retry.
type GenerationValidationError struct{ Reason string }

func (e *GenerationValidationError) Error() string {
	return "server: invalid mutable generation: " + e.Reason
}

func (h *Heads) handoffConflict(name string, generation uint64, reason string) error {
	return &GenerationConflictError{Head: name, ExpectedGeneration: generation, CurrentGeneration: generation, Reason: reason}
}

func (h *Heads) durableHandoffLocked(name string, policy HeadPolicy, generation uint64) (*entry, uint64, error) {
	handoff, ok := h.reg.Load().byName[policy.HandoffHead]
	if !ok || handoff.quarantine != "" || handoff.durable == nil {
		return nil, 0, h.handoffConflict(name, generation,
			fmt.Sprintf("handoff head %q has no durable served generation", policy.HandoffHead))
	}
	if handoff.kind != FinalizedMonotonic || handoff.durable.EffectiveKind() != FinalizedMonotonic || handoff.lineage == nil {
		return nil, 0, h.handoffConflict(name, generation,
			fmt.Sprintf("handoff head %q is not a usable finalized-monotonic lineage", policy.HandoffHead))
	}
	if handoff.durable.SyncedTo == nil {
		return nil, 0, h.handoffConflict(name, generation, fmt.Sprintf("handoff head %q is empty", policy.HandoffHead))
	}
	return handoff, *handoff.durable.SyncedTo, nil
}

func (h *Heads) bindObservedHandoffLocked(name string, policy HeadPolicy, req GenerationRequest, generation uint64) (*finalizedLineage, error) {
	handoff, frontier, err := h.durableHandoffLocked(name, policy, generation)
	if err != nil {
		return nil, err
	}
	if handoff.durable.Root != req.ObservedHandoffRoot || frontier != req.ObservedHandoffSyncedTo {
		return nil, h.handoffConflict(name, generation, fmt.Sprintf(
			"observed handoff %s at %d is stale; current %q is %s at %d",
			req.ObservedHandoffRoot, req.ObservedHandoffSyncedTo, policy.HandoffHead, handoff.durable.Root, frontier))
	}
	if frontier > req.SourceFinalizedSlot {
		return nil, h.handoffConflict(name, generation, fmt.Sprintf(
			"handoff frontier %d is above source_finalized_slot %d", frontier, req.SourceFinalizedSlot))
	}
	return handoff.lineage, nil
}

func (h *Heads) validateAdvancedHandoffLocked(name string, policy HeadPolicy, req GenerationRequest, generation uint64,
	lineage *finalizedLineage) (*entry, uint64, error) {
	handoff, frontier, err := h.durableHandoffLocked(name, policy, generation)
	if err != nil {
		return nil, 0, err
	}
	switch {
	case handoff.lineage != lineage:
		return nil, 0, h.handoffConflict(name, generation, fmt.Sprintf("handoff head %q was rewritten while the generation was built", policy.HandoffHead))
	case frontier < req.ObservedHandoffSyncedTo:
		return nil, 0, h.handoffConflict(name, generation, fmt.Sprintf("handoff head %q regressed from observed %d to %d", policy.HandoffHead,
			req.ObservedHandoffSyncedTo, frontier))
	case frontier > req.SourceFinalizedSlot:
		return nil, 0, h.handoffConflict(name, generation, fmt.Sprintf("handoff frontier %d advanced above source_finalized_slot %d",
			frontier, req.SourceFinalizedSlot))
	case frontier != math.MaxUint64 && req.WindowStart > frontier+1:
		return nil, 0, h.handoffConflict(name, generation, fmt.Sprintf("window_start %d would leave a gap above handoff %q synced_to %d",
			req.WindowStart, policy.HandoffHead, frontier))
	}
	return handoff, frontier, nil
}

func (h *Heads) prospectiveGenerationLocked(name string, candidate *archive.Head, st GenerationState,
	lineage *finalizedLineage) (*registry, *published, error) {
	e, err := h.mutableWritableLocked(name)
	if err != nil {
		return nil, nil, err
	}
	if h.cfg.Replacements[name] == nil {
		return nil, nil, fmt.Errorf("server: mutable head %q has no replacement callback", name)
	}
	state := st
	pub := headEntryGeneration(candidate.Info(), state)
	ne := *e
	ne.head = candidate
	ne.manifestTip = cid.Undef
	ne.durable = &pub
	ne.generation = state.Generation
	ne.proof = &state
	ne.proofLineage = lineage
	ne.proofValid = false
	ne.bindRestartProof = false
	prospective := h.reg.Load().with(&ne)
	selected := prospective.byName[name]
	if selected == nil || !selected.proofValid {
		return nil, nil, h.handoffConflict(name, st.Generation-1, "candidate does not form a coherent finalized handoff pair")
	}
	doc, err := h.buildPublication(prospective)
	if err != nil {
		return nil, nil, err
	}
	return prospective, doc, nil
}

func (h *Heads) generationStateLocked(ctx context.Context, name string) (GenerationState, error) {
	st, ok, err := h.cfg.Generations.Get(ctx, name)
	if err != nil {
		return GenerationState{}, err
	}
	if !ok {
		return GenerationState{}, fmt.Errorf("server: mutable head %q has no durable kind baseline", name)
	}
	if st.Kind != UnfinalizedMutable {
		return GenerationState{}, &KindMismatchError{Name: name, Want: UnfinalizedMutable, Got: st.Kind}
	}
	return st, nil
}

func (h *Heads) mutableWritableLocked(name string) (*entry, error) {
	e, err := h.writable(name)
	if err != nil {
		return nil, err
	}
	if e.kind != UnfinalizedMutable {
		return nil, fmt.Errorf("server: head %q is %q; complete generations require %q", name, e.kind, UnfinalizedMutable)
	}
	return e, nil
}

func (h *Heads) finishGenerationRetry(ctx context.Context, name string, st GenerationState, rows []archive.RefRow) (GenerationResponse, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.finishGenerationRetryLocked(ctx, name, st, rows)
}

func (h *Heads) finishGenerationRetryLocked(ctx context.Context, name string, st GenerationState, rows []archive.RefRow) (GenerationResponse, error) {
	e, err := h.mutableWritableLocked(name)
	if err != nil {
		return GenerationResponse{}, err
	}
	root, err := cid.Decode(st.Root)
	if err != nil {
		return GenerationResponse{}, err
	}
	// If the exact generation is already coherently exposed, keep the serving
	// pointer. Otherwise rebuild the complete bounded DAG from the authenticated
	// retry rows and verify its deterministic root. A crash (or an unsupported
	// second Heads over the same selector store) may have committed the state
	// before reconciliation retained its index DAG; staging still protects the
	// referenced blobs, and rebuilding restores every index block before the
	// prospective publication and reconciler retarget.
	candidate := e.head
	if e.generation != st.Generation || !e.head.Root().Equals(root) || !e.proofValid {
		params := e.head.Params()
		params.OriginSlot = st.WindowStart
		candidate, err = archive.BuildGeneration(ctx, h.cfg.GenerationArchive, params, rows, st.SyncedTo)
		if err != nil {
			h.recordIndexNodeLimitRefusal(name, err)
			return GenerationResponse{}, fmt.Errorf("server: rebuilding committed generation %d of head %q: %w", st.Generation, name, err)
		}
		if !candidate.Root().Equals(root) {
			return GenerationResponse{}, fmt.Errorf("server: exact retry rebuilt generation %d of head %q at %s, durable selector names %s",
				st.Generation, name, candidate.Root(), root)
		}
	}
	selected := h.reg.Load().byName[name]
	coherentSelection := selected != nil && selected.generation == st.Generation && selected.proofValid &&
		selected.head.Root().Equals(root)
	if !coherentSelection || !h.generationPublished(name, st) {
		if err := h.exposeGenerationLocked(name, candidate, st); err != nil {
			return GenerationResponse{}, err
		}
	}
	// Always retry the idempotent staging release: a crash may have landed after
	// publication but before the original request reached this final step.
	h.dropStaging(context.WithoutCancel(ctx), name, rows)
	return generationResponse(st, true), nil
}

func (h *Heads) generationPublished(name string, st GenerationState) bool {
	publication := h.pub.Load()
	b, ok := publication.heads[name]
	if !ok {
		return false
	}
	selected := h.reg.Load().byName[name]
	if selected == nil || selected.durable == nil || selected.generation != st.Generation ||
		selected.head == nil || selected.head.Root().String() != st.Root {
		return false
	}
	want, err := json.Marshal(*selected.durable)
	return err == nil && publication.generations[name] == st.Generation && bytes.Equal(b, want)
}

// exposeGenerationLocked performs the post-durability half of generation
// selection. The bound replacement callback runs before the lock-free registry can expose the
// candidate. Publication precedes notifications and staging release.
func (h *Heads) exposeGenerationLocked(name string, candidate *archive.Head, st GenerationState) error {
	e, err := h.mutableWritableLocked(name)
	if err != nil {
		return err
	}
	var lineage *finalizedLineage
	if e.generation == st.Generation && e.head.Root().Equals(candidate.Root()) && e.proof != nil && e.proofLineage != nil {
		lineage = e.proofLineage
	} else {
		// After a crash no in-process ancestry token survives. Rebind only when
		// the finalized head is still exactly the durable observed anchor; an
		// ordinary advance requires the tracker to select a fresh generation.
		policy := h.policy(name)
		handoff, frontier, bindErr := h.durableHandoffLocked(name, policy, st.Generation)
		if bindErr != nil {
			return bindErr
		}
		if handoff.durable.Root != st.HandoffRoot || frontier != st.HandoffSyncedTo {
			return h.handoffConflict(name, st.Generation, "committed generation cannot be rebound after restart because its exact handoff anchor changed; select a fresh generation")
		}
		lineage = handoff.lineage
	}
	prospective, doc, err := h.prospectiveGenerationLocked(name, candidate, st, lineage)
	if err != nil {
		return err
	}
	h.cfg.Replacements[name](candidate)
	h.reg.Store(prospective)
	h.installPublication(doc)
	h.notifyRootSwap(name, candidate)
	return nil
}

func generationResponse(st GenerationState, noop bool) GenerationResponse {
	return GenerationResponse{Generation: st.Generation, WindowStart: st.WindowStart, SyncedTo: st.SyncedTo, Root: st.Root, NoOp: noop}
}

// ApplyRefs applies a batch to the named head, persists the root it produces,
// and republishes the document (spec 5.1, 7.2, 8).
//
// expectedManifest is the manifest tip the writer scanned this batch under (spec
// 10.5, the safety boundary), compared to the head's registered tip under h.mu before
// the engine is touched: the commit-time binding that stops a batch scanned under
// a superseded schedule from landing across a tip handoff. cid.Undef means the
// request carried none, which is required of exactly the chainless heads (the ALL
// head, and every head predating the manifest chain) and refused of every head
// that has a tip. The server does the CID equality it can; the full
// predecessor-semantic check is the indexer's authenticated preflight (spec 10.5,
// and PublishManifest), because only the indexer sees L1.
//
// The whole of it runs inside the gate, so the collector's consistency cut
// cannot land between the engine's first block write and the root swap. Either
// this root is reconciled into the cut's M, or its post-cut block operations are
// protected in T (spec 9; see Gate).
func (h *Heads) ApplyRefs(ctx context.Context, name string, rows []archive.RefRow, syncedTo uint64, expectedManifest cid.Cid) (archive.ApplyResult, error) {
	h.cfg.Gate.Enter()
	defer h.cfg.Gate.Leave()

	h.mu.Lock()
	defer h.mu.Unlock()

	e, err := h.writable(name)
	if err != nil {
		return archive.ApplyResult{}, err
	}
	if e.kind == UnfinalizedMutable {
		return archive.ApplyResult{}, ErrMutableGenerationOnly
	}
	if err := checkManifestBinding(name, e.manifestTip, expectedManifest); err != nil {
		return archive.ApplyResult{}, err
	}
	replace, err := h.replacement(name)
	if err != nil {
		return archive.ApplyResult{}, err
	}
	base, err := durableEntryRoot(e)
	if err != nil {
		return archive.ApplyResult{}, err
	}
	candidate, err := e.head.CloneAt(ctx, base)
	if err != nil {
		return archive.ApplyResult{}, fmt.Errorf("server: cloning durable head %q at %s: %w", name, base, err)
	}
	res, err := candidate.ApplyRefs(ctx, rows, syncedTo)
	if err != nil {
		h.recordIndexNodeLimitRefusal(name, err)
		return archive.ApplyResult{}, err
	}
	if res.NoOp {
		// The staging pins are dropped anyway: a replay is a batch whose refs are
		// already in the head (spec 5.1), so its blobs are retained by the head's
		// own pins and the rows are as dead as they would be after a first
		// application.
		h.dropStaging(ctx, name, rows)
		return res, nil
	}
	pub := headEntryKind(candidate.Info(), e.manifestTip, e.kind)
	ne := *e
	ne.head = candidate
	ne.durable = &pub
	prospective := h.reg.Load().with(&ne)
	doc, err := h.buildPublication(prospective)
	if err != nil {
		return res, err
	}
	if err := h.commitCandidate(ctx, name, candidate, prospective, doc, replace); err != nil {
		return res, err
	}
	h.recordIndexApply(name, res.Index)
	// Only once the root that names these blobs is durable. Dropping earlier
	// would leave a crash-restarted node holding blobs that no head references
	// and no staging row protects, which is exactly the window the rows exist
	// to close.
	h.dropStaging(ctx, name, rows)
	return res, nil
}

// checkManifestBinding compares a refs request's expected_manifest against the
// head's registered tip. It is the whole of the
// server's manifest enforcement on the commit path: an exact CID compare, which
// is all a node with no L1 view can do. A head with a tip requires the field and
// requires it to equal the tip; a chainless head forbids it. The caller holds mu,
// so the tip read here is the same generation the engine is about to mutate.
func checkManifestBinding(name string, tip, expected cid.Cid) error {
	switch {
	case tip.Defined() && !expected.Defined():
		return fmt.Errorf("%w: head %q tip is %s", ErrManifestBindingRequired, name, tip)
	case !tip.Defined() && expected.Defined():
		return fmt.Errorf("%w: head %q, expected_manifest %s", ErrManifestBindingForbidden, name, expected)
	case tip.Defined() && expected != tip:
		return &ManifestBindingConflict{Head: name, Expected: expected, Current: tip}
	}
	return nil
}

// sealedSegmentWindows is the count of sealed segment windows in a head's
// directory, derived arithmetically from its coverage the way archive's openOrd
// and dirBase are (spec 4): the open window is always ord(synced_to+1), so every
// window below it is sealed. It counts windows -- including any that sealed empty
// to a null directory entry -- so it is the directory's extent rather than a
// census of non-empty Segment blocks, but it needs no DAG read. An empty head has
// none. It backs bloar_index_segments (the store-growth observability work).
func sealedSegmentWindows(covered bool, syncedTo, originSlot, segBits uint64) uint64 {
	if !covered {
		return 0
	}
	open := (syncedTo + 1) >> segBits
	base := originSlot >> segBits
	if open <= base {
		return 0
	}
	return open - base
}

// dropStaging releases the staging pins of a batch that has landed. The caller
// holds mu.
//
// A failure is logged, not returned. The batch is applied and its root is
// published; the rows are a retention detail that expires on its own (spec 9's
// staging TTL), and failing the caller's request over one would turn a
// successful mutation into an indexer error and a pointless replay.
func (h *Heads) dropStaging(ctx context.Context, name string, rows []archive.RefRow) {
	if h.cfg.Staging == nil || len(rows) == 0 {
		return
	}
	if err := h.cfg.Staging.DropRefs(ctx, rows); err != nil {
		h.cfg.Logger.Error("dropping staging pins", "head", name, "err", err)
	}
}

// Truncate truncates the named head to slot and returns the new root (spec 5.4,
// 7.2). The gate prevents the collector's T0 cut from splitting its spine
// rewrite and root publication. For a window-policy head the configured policy
// width selects TruncateRetainingWindow: rewinding can make older sealed
// Segments recursive, so their blob closures are validating-read through the
// application view before the new root is published. Full already retained
// those closures; none deliberately does not.
func (h *Heads) Truncate(ctx context.Context, name string, slot uint64) (cid.Cid, error) {
	h.cfg.Gate.Enter()
	defer h.cfg.Gate.Leave()

	h.mu.Lock()
	defer h.mu.Unlock()

	e, err := h.writable(name)
	if err != nil {
		return cid.Undef, err
	}
	if e.kind == UnfinalizedMutable {
		return cid.Undef, ErrMutableGenerationOnly
	}
	replace, err := h.replacement(name)
	if err != nil {
		return cid.Undef, err
	}
	base, err := durableEntryRoot(e)
	if err != nil {
		return cid.Undef, err
	}
	candidate, err := e.head.CloneAt(ctx, base)
	if err != nil {
		return cid.Undef, fmt.Errorf("server: cloning durable head %q at %s: %w", name, base, err)
	}
	var root cid.Cid
	if h.cfg.TruncateWindowSlots != nil {
		if slots, window := h.cfg.TruncateWindowSlots(name); window {
			root, err = candidate.TruncateRetainingWindow(ctx, slot, slots)
		} else {
			root, err = candidate.Truncate(ctx, slot)
		}
	} else {
		root, err = candidate.Truncate(ctx, slot)
	}
	if err != nil {
		h.recordIndexNodeLimitRefusal(name, err)
		return cid.Undef, err
	}
	if root.Equals(base) {
		// A structural no-op must not rotate the lineage token: dependent mutable
		// proofs are licensed by this exact append lineage. It also must not burn a
		// publication revision or retarget reconciliation to an equivalent clone.
		return root, nil
	}
	pub := headEntryKind(candidate.Info(), e.manifestTip, e.kind)
	ne := *e
	ne.head = candidate
	ne.durable = &pub
	ne.lineage = &finalizedLineage{}
	prospective := h.reg.Load().with(&ne)
	doc, err := h.buildPublication(prospective)
	if err != nil {
		return cid.Undef, err
	}
	if err := h.commitCandidate(ctx, name, candidate, prospective, doc, replace); err != nil {
		return cid.Undef, err
	}
	h.recordOpenSegment(ctx, name, candidate)
	return root, nil
}

// writable resolves a head a mutation is allowed to touch. The caller holds mu.
//
// The two refusals are different facts and the HTTP layer answers them
// differently: a head that is not here at all is 404, and one that is here
// because this node follows it is 403 (spec 11.1 -- one writer per head, and it
// is somebody else).
func (h *Heads) writable(name string) (*entry, error) {
	e, ok := h.reg.Load().byName[name]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrUnknownHead, name)
	}
	if e.followed {
		return nil, fmt.Errorf("%w: %q", ErrFollowedHead, name)
	}
	return e, nil
}

func (h *Heads) replacement(name string) (func(*archive.Head), error) {
	replace := h.cfg.Replacements[name]
	if replace == nil {
		// A registry with neither GC gate nor reconciliation notification has no
		// external engine pointer to retarget. Keep the embedding API lightweight
		// for that intentionally non-collecting case; any GC/reconciled stack must
		// make the pointer handoff explicit.
		if _, ungated := h.cfg.Gate.(noGate); ungated && h.cfg.OnRoot == nil {
			return func(*archive.Head) {}, nil
		}
		return nil, fmt.Errorf("server: writable head %q has no infallible replacement callback; local root transitions require one so reconciliation cannot retain the retired engine", name)
	}
	return replace, nil
}

func durableEntryRoot(e *entry) (cid.Cid, error) {
	if e == nil || e.durable == nil {
		return cid.Undef, errors.New("server: writable head has no durable publication root")
	}
	root, err := cid.Decode(e.durable.Root)
	if err != nil {
		return cid.Undef, fmt.Errorf("server: decoding durable root %q of head %q: %w", e.durable.Root, e.name(), err)
	}
	return root, nil
}

// commitCandidate is the no-failure-after-retarget half of a local COW root
// transition. Every fallible archive operation and publication render has
// already completed. A failed root Put leaves the old engine, reconciler,
// registry, and document untouched. After it succeeds the replacement callback
// is startup-validated and infallible, and atomic pointer stores cannot fail.
func (h *Heads) commitCandidate(ctx context.Context, name string, candidate *archive.Head, reg *registry, doc *published,
	replace func(*archive.Head)) error {
	root := candidate.Root()
	if err := h.cfg.Roots.Put(context.WithoutCancel(ctx), name, root); err != nil {
		return err
	}
	replace(candidate)
	h.reg.Store(reg)
	h.installPublication(doc)
	h.notifyRootSwap(name, candidate)
	return nil
}

func (h *Heads) notifyRootSwap(name string, head *archive.Head) {
	if h.cfg.OnRoot != nil {
		h.cfg.OnRoot(name, head.Root())
	}
	h.cfg.Metrics.RootSwap(name)
	info := head.Info()
	syncedTo, covered := uint64(0), info.SyncedTo != nil
	if covered {
		syncedTo = *info.SyncedTo
	}
	h.cfg.Metrics.HeadInfo(name, syncedTo, covered, info.DirDepth)
	h.cfg.Metrics.HeadStructure(name, sealedSegmentWindows(covered, syncedTo, info.OriginSlot, info.SegBits))
}

func (h *Heads) recordIndexApply(name string, stats archive.IndexApplyStats) {
	h.cfg.Metrics.IndexApply(name, stats.EncodedBytes)
	for _, sample := range stats.Segments {
		h.cfg.Metrics.IndexSegment(name, string(sample.State), sample.EncodedBytes, sample.Rows, sample.Refs)
	}
}

func (h *Heads) recordOpenSegment(ctx context.Context, name string, head *archive.Head) {
	if h.cfg.Metrics == nil {
		return
	}
	sample, covered, err := head.OpenSegmentSample(ctx)
	if err != nil {
		h.cfg.Logger.Warn("reading current open Segment for metrics", "head", name, "err", err)
		return
	}
	if covered {
		h.cfg.Metrics.IndexSegment(name, string(sample.State), sample.EncodedBytes, sample.Rows, sample.Refs)
	}
}

func (h *Heads) restoreIndexSegmentMetrics(ctx context.Context, name string, head *archive.Head) {
	if h.cfg.Metrics == nil {
		return
	}
	open, covered, err := head.OpenSegmentSample(ctx)
	if err != nil {
		h.cfg.Logger.Warn("restoring current open Segment metrics", "head", name, "err", err)
		return
	}
	if covered {
		h.cfg.Metrics.IndexSegmentSnapshot(name, string(open.State), open.EncodedBytes, open.Rows, open.Refs)
	}
	sealed, found, err := head.LatestSealedSegmentSample(ctx)
	if err != nil {
		h.cfg.Logger.Warn("restoring latest sealed Segment metrics", "head", name, "err", err)
		return
	}
	if found {
		h.cfg.Metrics.IndexSegmentSnapshot(name, string(sealed.State), sealed.EncodedBytes, sealed.Rows, sealed.Refs)
	}
}

func (h *Heads) recordIndexNodeLimitRefusal(name string, err error) {
	var oversized *archive.IndexNodeTooLargeError
	if errors.As(err, &oversized) {
		h.cfg.Metrics.IndexNodeLimitRefusal(name, string(oversized.Kind))
	}
}

// rebuild renders and publishes the document from each head's durable record
// (entry.durable), never from its live engine. The caller holds mu.
func (h *Heads) rebuild() error {
	reg := h.reg.Load()
	next, err := h.buildPublication(reg)
	if err != nil {
		return err
	}
	h.installPublication(next)
	return nil
}

// buildPublication performs every fallible step needed to publish reg without
// making reg or its document visible. Local root transitions call it before
// persisting or retargeting anything, so a signing/revision/render failure
// leaves the old engine, reconciler target, registry, and document as one
// coherent generation. A revision allocated for a candidate whose later root
// persist fails is an intentional harmless hole.
func (h *Heads) buildPublication(reg *registry) (*published, error) {
	doc := Unsigned{
		V:   LegacyDocVersion,
		Net: h.cfg.Net,
		// Second precision, UTC: the field is a freshness claim followers
		// compare for regression (spec 11.3), not a timing measurement.
		UpdatedAt:  time.Now().UTC().Format(time.RFC3339),
		Multiaddrs: h.cfg.Multiaddrs,
		// Never nil: "heads": [] is a node that writes nothing, and a missing
		// key would be a different statement.
		Heads: make([]HeadEntry, 0, len(reg.names)),
	}
	if h.cfg.ArchiveID != nil {
		archiveID := *h.cfg.ArchiveID
		doc.ArchiveID = &archiveID
	}
	// Sorted by name, because the signature covers the rendered bytes and a
	// map's iteration order would make two documents over identical state
	// disagree.
	for _, name := range reg.names {
		e := reg.byName[name]
		// Rendered exclusively from the head's durable record, never from its live
		// engine: e.head.Info() can be a root the RootStore does not yet hold (audit
		// the safety boundary). A head with no durable record yet -- a followed head whose first
		// adoption has not persisted its root -- is omitted, having no prior durable
		// root to announce in its place.
		if e.durable == nil {
			continue
		}
		if !e.republish {
			continue
		}
		published := cloneHeadEntry(*e.durable)
		// The registry retains an exact revisioned finalized line internally for
		// pointer admission. Its local republished document may be revisionless;
		// explicit finalized kind is the one field whose omission is the canonical
		// legacy spelling, so normalize it at this rendering boundary only.
		if published.EffectiveKind() == FinalizedMonotonic {
			published.Kind = ""
		}
		doc.Heads = append(doc.Heads, published)
	}
	// Revisioning activates with the first durable mutable line and never falls
	// back merely because that line is quarantined. The durable entry still
	// exists and a revisionless document from this signer would be a downgrade at
	// every follower which has seen it.
	revisioned := h.revisioned || h.cfg.ArchiveID != nil
	for _, e := range reg.byName {
		if e.durable != nil && e.republish && e.kind == UnfinalizedMutable {
			revisioned = true
			break
		}
	}
	if revisioned {
		if h.cfg.SigningKey == nil || h.cfg.Publications == nil {
			return nil, errors.New("server: a durable mutable head requires a signing key and PublicationStore")
		}
		pub, ok := h.cfg.SigningKey.Public().(ed25519.PublicKey)
		if !ok {
			return nil, errors.New("server: signing key has no ed25519 public key")
		}
		revision, err := h.cfg.Publications.Next(context.Background(), pub)
		if err != nil {
			return nil, err
		}
		if h.cfg.ArchiveID != nil {
			doc.V = LogicalArchiveDocVersion
		} else {
			doc.V = DocVersion
		}
		doc.Revision = &revision
		// Next durably activated the signer-local downgrade floor even if the
		// caller later fails to persist its candidate root.
		h.revisioned = true
	}

	signed, err := doc.sign(h.cfg.SigningKey)
	if err != nil {
		return nil, err
	}
	if err := signed.ValidateContract(); err != nil {
		return nil, fmt.Errorf("server: refusing to publish an invalid document: %w", err)
	}
	// Attached after signing: it is not part of the signed document (see
	// Doc.MaxPutBlobs), so it goes on once the signature is already fixed.
	signed.MaxPutBlobs = h.cfg.MaxPutBlobs
	body, err := json.Marshal(signed)
	if err != nil {
		return nil, fmt.Errorf("server: rendering publication document: %w", err)
	}

	next := &published{doc: body, heads: make(map[string][]byte, len(doc.Heads)), generations: make(map[string]uint64, len(doc.Heads))}
	for _, entry := range doc.Heads {
		b, err := json.Marshal(entry)
		if err != nil {
			return nil, fmt.Errorf("server: rendering head %q: %w", entry.Name, err)
		}
		next.heads[entry.Name] = b
		next.generations[entry.Name] = reg.byName[entry.Name].generation
	}
	return next, nil
}

// installPublication is the infallible visibility half of buildPublication.
// The caller has already installed the matching registry when this accompanies
// a root transition.
func (h *Heads) installPublication(next *published) {
	h.pub.Store(next)
	if h.cfg.OnDoc != nil {
		// After the store, so that a hook which reads back through this type
		// sees the document it was handed rather than the previous one.
		h.cfg.OnDoc(next.doc)
	}
}
