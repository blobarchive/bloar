package follow

import (
	"bytes"
	"context"
	"fmt"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	"github.com/ipld/go-ipld-prime/datamodel"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"

	"github.com/blobarchive/bloar/p2p/pointerhint"
	"github.com/blobarchive/bloar/pinning"
	"github.com/blobarchive/bloar/schema"
)

// beforeSyncCommitHook is test-only synchronization for a collection cut at
// the exact walk-complete/completion-stamp boundary. Nil in production.
var beforeSyncCommitHook func()

// syncPointerError identifies the authenticated entry pointer whose provider
// can satisfy a failed sync phase. Its wrapped error remains authoritative:
// callers must still require a genuine fetching-blockstore miss before using
// the hint. In particular, semantic traversal and verification failures never
// become provider lookups merely because they occurred inside a root walk.
type syncPointerError struct {
	pointer pointerhint.Pointer
	err     error
}

func (e *syncPointerError) Error() string { return e.err.Error() }
func (e *syncPointerError) Unwrap() error { return e.err }

func syncPointerFailure(kind pointerhint.Kind, pointer cid.Cid, err error) error {
	return &syncPointerError{pointer: pointerhint.Pointer{Kind: kind, CID: pointer}, err: err}
}

// sync is the fetch pass: it makes local every block this node's pin policy
// retains for the head (spec 11.3).
//
// # Why this is the sync mechanism
//
// Spec 11.3 says pin reconciliation over a bitswap-backed blockstore is the
// sync, and the reason it can say that is that both halves ask the same
// question. pinning.Desired computes what the policy retains from the head's
// own structure; GC marks exactly those pins and their reachable blocks and
// sweeps the rest. So "what this node should hold" already has an answer, and
// fetching is not a second policy -- it is that answer, read through a
// blockstore whose misses fetch.
//
// Hence the shape below: compute the desired pins, then make each one local.
// A direct pin is one block. A recursive pin is its whole reachable subtree,
// walked the way GC's mark walks it, so that the set this fetches and the set
// GC keeps are the same set by construction rather than by two implementations
// agreeing. A window policy fetches its window's blobs and no others because
// that is what its pins say, and the index arrives whole under every policy
// because every policy pins all of it (spec 9) -- which is what lets a follower
// answer 404-vs-503 exactly like the writer while holding almost no blobs.
//
// # The fetch-then-pin window
//
// Every block this makes durable is unreferenced until the reconcile that pins
// the adopted root lands, and that reconcile is asynchronous (the adoption
// notifies it). A GC in that gap marks from the pre-adoption ledger, finds the
// fresh blocks unreferenced, and sweeps them -- and the adoption's pins then
// name a block that is gone. It is the follower's cousin of spec 9's window (a).
//
// A staging pin on each block closes it, exactly as it does for ingest: the
// block is pinned under the reserved staging head the moment it is durable
// (fetchStaged), so the first GC that can run already has it in its mark set.
// The pins are dropped once the pass finishes -- the root is durable and
// registered, and a GC reconciles every head before it marks, so the head's own
// pins retain the blocks from here -- and expire on their TTL if the pass dies
// first.
//
// # Cost
//
// A pass runs on every poll and a root moves on every batch the writer applies,
// so walking the whole DAG each time is not available: under a full policy the
// one recursive pin is the root, and the root's subtree is the archive.
//
// On the production epoch store, walked makes presence checking incremental.
// Entries are stamped with the monotonic collection generation and trusted only
// in that generation; the first walk after each GC cut therefore revisits shared
// subtrees so its application-store reads protect them. Structural sharing then
// makes later passes cost only the changed spine and open segment. Missing blocks
// may fetch, but local hits are ordinary store checks. The memo is process-local,
// so restart also pays one complete presence walk.
//
// A plain blockstore has no token which advances after its legacy collector.
// It therefore disables the shared presence memo and rewalks the complete
// retained closure under Gate; otherwise A -> B -> GC -> A could trust a stale
// process-lifetime proof. That compatibility cost does not apply to the
// generation-aware production store.
//
// verify: full has a separate semantic memo, verifiedSegments. Its first
// ordinary sync checks every RefEntry in each Segment, including a blob already
// present locally. Segment and blob CIDs make that successful proof immutable
// across collection generations and refetches. Sealed-Segment proofs are stored
// under versioned CID keys in the follower KV and survive restart; the mutable
// open Segment is memory-only to avoid leaking one key per intermediate CID.
// Protection-only adoption/Resume walks do not perform KZG semantics and never
// confer or populate either proof layer.
func (f *Follower) sync(ctx context.Context, name string) error {
	// Snapshot every headState field this pass reads while holding f.mu: expose
	// mutates them under the same lock (adopted, manifestTip) and this pass runs
	// unlocked for the length of a DAG walk, so reading them unlocked would race a
	// concurrent transition. The snapshot is also what the completion
	// commit below is checked against.
	f.mu.Lock()
	hs, ok := f.heads[name]
	var (
		quarantined                         bool
		root, fetched, tip, manifestFetched cid.Cid
	)
	if ok {
		quarantined = hs.quarantined
		root, fetched = hs.adopted, hs.fetched
		tip, manifestFetched = hs.manifestTip, hs.manifestFetched
	}
	f.mu.Unlock()
	if !ok || quarantined || !root.Defined() {
		return nil
	}
	// Two things can be stale independently: the root, moved by a refs batch, and
	// the manifest tip, moved by an operator's upgrade (which does not move the
	// root, spec 10.5). Either alone is a pass; neither is a no-op.
	rootStale := root != fetched
	tipStale := tip.Defined() && tip != manifestFetched
	if !rootStale && !tipStale {
		return nil
	}
	// A plain Blockstore has no monotonic deletion generation. Its compatible
	// GC uses whole-run Gate exclusion, so hold that same Gate across the entire
	// walk and completion stamp. The production epoch store takes the online
	// path below and needs only the short completion barrier.
	legacyGate := !f.hasCollectionGeneration()
	if legacyGate {
		f.gate.Enter()
		defer f.gate.Leave()
	}

	head, ok := f.cfg.Registry.Get(name)
	if !ok {
		return nil // quarantined, or dropped out from under us.
	}
	// Bind the walk to the snapshot. This pass will enumerate and walk the
	// head the Registry holds, but the staleness flags and the completion CAS below are
	// about the root/tip snapshotted from headState. expose swaps the Registry entry
	// BEFORE it updates headState, so between the two the Registry can hold a newer
	// generation than the snapshot -- and an equal-floor A->B->A adoption can even leave
	// headState.adopted back at the snapshot while the Registry moved through B. Walking
	// B and then stamping the snapshot A fetched would mark A synced though only B was
	// walked. So skip the pass unless the Registry head is exactly the snapshotted root;
	// the next poll retries once the two are consistent.
	if head.Root() != root {
		return nil
	}

	// Bind memo use to the monotonic collection generation at the start of this
	// walk. Begin increments it before any sweep can delete, and it remains at
	// that value after End, so a completed GC invalidates stale presence proofs
	// even while collection is idle.
	w := &walk{
		f: f, head: name, segs: map[string]bool{}, durableSegs: map[string]bool{},
		generation: f.collectionGeneration(), gateHeld: legacyGate,
	}
	if rootStale {
		// Enumerate is itself part of the fetch: it walks the directory over the
		// fetching blockstore, so a follower that has never seen this head reads
		// its Head block, its dir pages and its open segment off the network here,
		// and pinning.Desired then names blocks this node is holding.
		enum, err := head.Enumerate(ctx)
		if err != nil {
			return syncPointerFailure(pointerhint.Root, root, fmt.Errorf("enumerating %s: %w", root, err))
		}
		desired, err := pinning.Desired(hs.policy, enum)
		if err != nil {
			return err
		}
		// The head's Segment blocks, which are the only blocks that name blobs. The
		// walk needs to know which they are: a blob's versioned hash is in the
		// RefEntry that points at it and nowhere else, and full verification is
		// exactly the check that those two agree.
		for _, s := range enum.Sealed {
			w.segs[s.CID.KeyString()] = true
			w.durableSegs[s.CID.KeyString()] = true
			if f.cfg.Verify == VerifyFull {
				// A Segment can have been verified while open and then be skipped
				// below by the generation-scoped presence memo after it seals. Give
				// the semantic cache its new durable classification independently
				// of whether traversal needs to expand it again.
				if _, err := f.segmentVerified(name, s.CID, true); err != nil {
					return fmt.Errorf("loading the full-verification proof for sealed Segment %s: %w", s.CID, err)
				}
			}
		}
		if enum.Open.Defined() {
			w.segs[enum.Open.KeyString()] = true
		}
		for _, pin := range desired {
			if err := ctx.Err(); err != nil {
				return err
			}
			if !pin.Recursive {
				if err := w.ensure(ctx, pin.CID); err != nil {
					return syncPointerFailure(pointerhint.Root, root,
						fmt.Errorf("fetching the %s pin %s: %w", pin.Purpose, pin.CID, err))
				}
				continue
			}
			if err := w.recurse(ctx, pin.CID); err != nil {
				return syncPointerFailure(pointerhint.Root, root,
					fmt.Errorf("fetching the %s pin %s: %w", pin.Purpose, pin.CID, err))
			}
		}
	}
	if tipStale {
		// The manifest chain (spec 9, 10.5): a generic recursive walk from the tip,
		// which fetches and stages every Manifest back to genesis the same way a
		// recursive pin's subtree is. It decodes no manifest (spec 15) -- the walk
		// follows prev links out of the data model, exactly as the pin reconciler's
		// mark will, so the set fetched here is the set the recursive manifest pin
		// retains. Structural sharing makes an upgrade cheap: a new tip shares the
		// whole chain below it, and the walk stops at the first Manifest already
		// held.
		if err := w.recurse(ctx, tip); err != nil {
			return syncPointerFailure(pointerhint.Manifest, tip,
				fmt.Errorf("fetching the manifest chain %s: %w", tip, err))
		}
	}

	// Commit fetch completion CAS-style: stamp fetched/manifestFetched
	// only if the generation this pass fetched is still the adopted one. A newer
	// transition during the pass -- expose swapping adopted or manifestTip to a later
	// generation -- makes this pass stale, and stamping its older root or tip would
	// mark a generation synced that the head has already moved past, so the fetch pass
	// for the newer generation would never run. When it is stale the stamp is skipped
	// and the next pass re-fetches against the current generation.
	// Gate makes the generation check and completion stamp atomic with the next
	// GC cut. A walk that crossed a cut has deliberately not produced a valid
	// post-cut presence proof: leave fetched stale and keep its staging pins so a
	// later poll retries instead of suppressing the missing work.
	if beforeSyncCommitHook != nil {
		beforeSyncCommitHook()
	}
	if !legacyGate {
		f.gate.Enter()
	}
	if got := f.collectionGeneration(); got != w.generation {
		if !legacyGate {
			f.gate.Leave()
		}
		return fmt.Errorf("blockstore collection generation changed from %d to %d during fetch; retrying the generation",
			w.generation, got)
	}
	f.mu.Lock()
	completed := false
	if rootStale && hs.adopted == root {
		hs.fetched = root
		completed = true
	}
	if tipStale && hs.manifestTip == tip {
		hs.manifestFetched = tip
		completed = true
	}
	if completed {
		hs.syncCompletions++
	}
	f.mu.Unlock()
	if !legacyGate {
		f.gate.Leave()
	}

	// The pass's staging pins have done their job: every block it fetched is
	// durable and reachable from the adopted root, which is registered with the
	// reconciler, and a GC reconciles every head before it marks -- so the head's
	// own pins retain them from here. Drop the rows, as server.Heads drops
	// ingest's once a batch's refs land. A failure is logged, not returned: an
	// undropped row expires on its TTL and retains nothing the head does not.
	if f.cfg.Staging != nil && len(w.staged) > 0 {
		if err := f.cfg.Staging.Drop(ctx, w.staged); err != nil {
			f.log.Error("dropping fetch-pass staging pins", "head", name, "err", err)
		}
	}

	if w.fetched > 0 {
		f.log.Info("followed head synced", "head", name, "root", root, "blocks_fetched", w.fetched,
			"blobs_verified", w.verified)
	}
	return nil
}

// protectAdoptionClosure fetches and application-touches exactly the blocks
// the plan's retention policy will pin, plus its manifest chain, before an
// adoption or Resume exposes that generation. The returned generation is a
// proof token: commit holds Gate and requires the blockstore's monotonic
// collection generation still to equal it. A GC cut at any point invalidates
// the proof and the publication transition is retried without exposure.
//
// This is a generic CID/link walk even under verify: full. Semantic/KZG
// verification remains the ordinary post-adoption sync's responsibility, as it
// was before online GC; this pass establishes durability and M-union-T safety.
func (f *Follower) protectAdoptionClosure(ctx context.Context, p *adoptPlan, gateHeld bool) error {
	if p.head == nil {
		return nil
	}
	f.mu.Lock()
	hs := f.heads[p.name]
	policy := hs.policy
	f.mu.Unlock()

	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		generation := f.collectionGeneration()
		if active, observable := f.activeCollectionEpoch(); f.hasCollectionGeneration() && observable && active == 0 {
			// No collector can delete between this observation and commit unless
			// Begin first increments generation. Commit holds Gate and rechecks the
			// token, so the ordinary idle path needs no full closure walk.
			p.closureGeneration = generation
			return nil
		}
		w := &walk{
			f: f, head: p.name, segs: map[string]bool{}, generation: generation,
			protectionOnly: true, noMemoCommit: true, gateHeld: gateHeld,
		}
		enum, err := p.head.Enumerate(ctx)
		if err != nil {
			return fmt.Errorf("protecting adoption root %s for head %q: %w", p.head.Root(), p.name, err)
		}
		desired, err := pinning.Desired(policy, enum)
		if err != nil {
			return err
		}
		for _, pin := range desired {
			if err := ctx.Err(); err != nil {
				return err
			}
			if pin.Recursive {
				err = w.recurse(ctx, pin.CID)
			} else {
				err = w.ensure(ctx, pin.CID)
			}
			if err != nil {
				return fmt.Errorf("protecting the %s pin %s for head %q: %w", pin.Purpose, pin.CID, p.name, err)
			}
		}
		if p.tip.Defined() {
			if err := w.recurse(ctx, p.tip); err != nil {
				return fmt.Errorf("protecting manifest chain %s for head %q: %w", p.tip, p.name, err)
			}
		}
		p.staged = append(p.staged, w.staged...)
		if f.collectionGeneration() == generation {
			p.closureGeneration = generation
			return nil
		}
		// A collection began while the walk was in flight. Old reads may have
		// preceded its cut; repeat under the new generation. Any fetched blocks
		// stay staged until a successful commit (or TTL on ultimate refusal).
		f.log.Info("adoption closure crossed a collection cut; retrying before publication",
			"head", p.name, "root", p.head.Root(), "from_generation", generation,
			"to_generation", f.collectionGeneration())
	}
}

// walk is one fetch pass over one head.
type walk struct {
	f    *Follower
	head string
	// generation is the blockstore collection generation captured when this
	// walk began. If it changes underneath us, commit discards the memo proof;
	// the fetch itself remains useful, but its cross-cut presence proof does not.
	generation uint64
	// protectionOnly walks links to establish local presence/epoch protection
	// before publication. It deliberately does not perform full-policy semantic
	// verification or publish its result into the verified fetch memo; the
	// ordinary sync after adoption or Resume still owns that work.
	protectionOnly bool
	noMemoCommit   bool
	gateHeld       bool
	// segs is the head's Segment CIDs; see sync.
	segs        map[string]bool
	durableSegs map[string]bool // sealed Segments whose proof is persisted

	// pending is this recurse's marks, held out of the shared memo until the
	// recurse drains. A mark means "expanded", not yet "subtree complete": the
	// second is only true once the stack that held the node's descendants has
	// emptied, and commit is what promotes pending to f.walked when it has (see
	// recurse). A recurse that dies mid-walk drops it, so f.walked never records a
	// subtree whose descendants were not all fetched -- which, left uncorrected,
	// is a follower that reports a head synced while missing sealed segments and
	// blobs it will not re-fetch until the process restarts.
	//
	// Walk-local, so it needs no lock: one walk runs on one goroutine, and two
	// concurrent walks each carry their own and commit under f.mu.
	pending map[string]bool

	fetched  int
	verified int
	// staged is every block this pass took a staging pin on, to drop when it
	// finishes (see sync). Only blocks this pass actually fetched are here: one
	// already local is retained by the head's pins or an earlier pass's rows.
	staged []cid.Cid
}

// ensure makes c local, fetching and staging it if it is not. It is the
// direct-pin path: all that is wanted is that the block is held, so the block it
// fetches is discarded rather than read back.
func (w *walk) ensure(ctx context.Context, c cid.Cid) error {
	had, err := w.f.cfg.Local.Has(ctx, c)
	if err != nil {
		return err
	}
	if had {
		return nil
	}
	_, err = w.fetchStaged(ctx, c)
	return err
}

// block returns c's block, fetching and staging it if this node has not got it
// (see fetchStaged) and reading it back from local if it has. It is the path for
// the callers that need the bytes -- expanding an index node's links, decoding a
// Segment -- as opposed to ensure, which only needs the block held.
func (w *walk) block(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	had, err := w.f.cfg.Local.Has(ctx, c)
	if err != nil {
		return nil, err
	}
	if had {
		return w.f.cfg.Local.Get(ctx, c)
	}
	return w.fetchStaged(ctx, c)
}

// fetchStaged fetches c over bitswap and takes a staging pin on it, and returns
// the block. The caller has established c is not local.
//
// The pin keeps a later GC cut from treating a block fetched before the head's
// own pins land as garbage (see sync's fetch-then-pin window). Block write and
// staging pin share one Gate transition: if they finish before T0 the staging
// row is in M; if T0 wins, the write goes through the active application view
// and enters T before publication. The write precedes the pin so a crash never
// leaves a pin naming an absent block. A node with no staging configured still
// has epoch protection for a post-cut write, but has no durable bridge across a
// later cut before reconciliation; that is the old unsafe compatibility mode.
func (w *walk) fetchStaged(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	if w.f.cfg.Staging == nil {
		blk, err := w.f.blocks.Get(ctx, c)
		if err != nil {
			return nil, err
		}
		w.fetched++
		return blk, nil
	}
	if !w.gateHeld {
		w.f.gate.Enter()
		defer w.f.gate.Leave()
	}
	blk, err := w.f.blocks.Get(ctx, c)
	if err != nil {
		return nil, err
	}
	if err := w.f.cfg.Staging.Pin(ctx, []cid.Cid{c}); err != nil {
		return nil, fmt.Errorf("staging fetched block %s: %w", c, err)
	}
	w.fetched++
	w.staged = append(w.staged, c)
	return blk, nil
}

// recurse fetches c and everything reachable from it.
//
// Depth-first over an explicit stack rather than the call stack: a directory is
// shallow (spec 3.3) but a segment's blob list is not, and a walk of a
// production head has no business being bounded by a goroutine's stack.
//
// The marks it makes are buffered and only committed to the shared memo if the
// stack drains: a mark is written when a node is expanded, which is before its
// descendants are walked, so until the loop below empties the stack it is not yet
// known that they all were. Commit at the end and drop on any error, and the memo
// means what done says it does -- the subtree is known complete -- rather than
// "the children were enumerated once". Prior passes that were left half-marked by
// a mid-walk failure are what silently un-synced a follower.
func (w *walk) recurse(ctx context.Context, root cid.Cid) error {
	w.pending = map[string]bool{}
	stack := []cid.Cid{root}
	for len(stack) > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		c := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		if w.done(c) {
			continue
		}
		kids, err := w.expand(ctx, c)
		if err != nil {
			return err
		}
		w.mark(c)
		stack = append(stack, kids...)
	}
	// The stack drained: every node mark recorded was popped again as its
	// descendants were, so each of their subtrees is fully fetched. Only now is it
	// sound for a later walk to stop at them.
	w.commit()
	return nil
}

// done reports whether c's subtree is known complete. Only index blocks are
// memoised: a blob is a leaf, and "is it local" is a lookup on the store that
// holds it rather than a thing worth remembering separately for every blob in
// an archive.
//
// It consults both the committed memo (f.walked, subtrees an earlier recurse
// proved complete) and this recurse's pending marks (nodes already expanded in
// the current walk), so a node reached twice -- a structurally-shared page, a
// segment a later pass meets again -- is still expanded once.
func (w *walk) done(c cid.Cid) bool {
	if c.Prefix().Codec != cid.DagCBOR {
		return false
	}
	if w.pending[c.KeyString()] {
		return true
	}
	if !w.f.hasCollectionGeneration() {
		// A plain blockstore has no token that changes after its legacy,
		// whole-Gate collector completes. A process-lifetime memo could therefore
		// resurrect an A subtree after A -> B -> GC -> A. The compatibility path
		// deliberately pays for a complete walk under Gate instead.
		return false
	}
	w.f.mu.Lock()
	defer w.f.mu.Unlock()
	stamp, ok := w.f.walked[c.KeyString()]
	return ok && stamp == w.generation
}

// mark records that c has been expanded in this recurse. It goes to the
// walk-local pending set rather than straight to f.walked: c's subtree is not
// known complete until the recurse drains, and commit is what moves the marks
// across once it has (see recurse).
func (w *walk) mark(c cid.Cid) {
	if c.Prefix().Codec != cid.DagCBOR {
		return
	}
	w.pending[c.KeyString()] = true
}

// commit promotes this recurse's pending marks into the shared memo. It is
// called only when recurse drained without error, which is what the marks now
// stand for: every one of them was popped again with its descendants, so its
// subtree is fully fetched (see recurse).
func (w *walk) commit() {
	if w.noMemoCommit || !w.f.hasCollectionGeneration() {
		return
	}
	if w.f.collectionGeneration() != w.generation {
		// The walk crossed a collection cut. Some reads may precede the cut and
		// therefore prove neither post-cut presence nor epoch protection. Keep the
		// fetched bytes, but do not memoize the mixed-generation walk.
		return
	}
	w.f.mu.Lock()
	defer w.f.mu.Unlock()
	for k := range w.pending {
		w.f.walked[k] = w.generation
	}
}

// collectionGeneration asks the application blockstore for the monotonic GC
// generation. Ordinary Boxo blockstores have no online collector and therefore
// remain at generation zero forever.
func (f *Follower) collectionGeneration() uint64 {
	if generations, ok := f.cfg.Local.(interface{ CollectionGeneration() uint64 }); ok {
		return generations.CollectionGeneration()
	}
	return 0
}

func (f *Follower) hasCollectionGeneration() bool {
	_, ok := f.cfg.Local.(interface{ CollectionGeneration() uint64 })
	return ok
}

func (f *Follower) activeCollectionEpoch() (uint64, bool) {
	epochs, ok := f.cfg.Local.(interface{ ActiveEpoch() uint64 })
	if !ok {
		return 0, false
	}
	return epochs.ActiveEpoch(), true
}

// expand fetches c and returns the children still to walk.
//
// A Segment is expanded from its schema rather than from its links, and every
// other block from its links. The difference is only visible under full
// verification, and it is the whole of it: a generic traversal sees a Segment's
// blob references as a list of CIDs, which is enough to fetch them and not
// enough to check them, because what a RefEntry asserts is that a versioned hash
// and a CID name the same blob. The two traversals visit the same blocks -- a
// Segment's only links are its entries' blobs (spec 3.2) -- so the fetch set is
// the same set GC will mark either way.
func (w *walk) expand(ctx context.Context, c cid.Cid) ([]cid.Cid, error) {
	switch c.Prefix().Codec {
	case cid.Raw:
		// A blob reached without a Segment above it. Under cid verification it
		// is a block like any other; under full it is a blob nothing binds a
		// versioned hash to, which is a promise this node cannot keep. Spec 3
		// puts blob links in Segments and nowhere else, so this is a head shaped
		// like nothing the schema describes.
		if w.f.cfg.Verify == VerifyFull && !w.protectionOnly {
			reason := "block %s is a raw block reachable from something that is not a Segment; under verify: full every blob must be bound to a versioned hash by the index (spec 3.2, 11.4)"
			return nil, w.f.quarantine(w.head, reason, c)
		}
		return nil, w.ensure(ctx, c)
	case cid.DagCBOR:
	default:
		return nil, fmt.Errorf("block %s has codec 0x%x; bloar's DAG is raw blobs and dag-cbor index nodes (spec 2)",
			c, c.Prefix().Codec)
	}

	blk, err := w.block(ctx, c)
	if err != nil {
		return nil, err
	}

	if w.segs[c.KeyString()] {
		return nil, w.segment(ctx, blk.RawData(), c)
	}
	return links(blk.RawData(), c)
}

// segment makes every blob one Segment names local and, under verify: full,
// checks every binding before this Segment is considered semantically verified
// (spec 11.4).
//
// The semantic memo is keyed by Segment CID, independently of the
// generation-scoped presence memo. A Segment CID commits to all RefEntries and
// each blob CID commits to its bytes, so a completed verification remains true
// after GC, a refetch, and restart. Every RefEntry is checked on the first proof
// even when its blob was already local. The proof is stored under a versioned
// Segment-CID key in the follower's checksummed KV; changing the verification
// rule changes that key version and forces re-verification. The in-memory map is
// only its hot cache. A protection-only walk neither performs these checks nor
// marks the Segment semantically verified.
func (w *walk) segment(ctx context.Context, raw []byte, c cid.Cid) error {
	seg, err := schema.DecodeSegment(raw)
	if err != nil {
		return fmt.Errorf("decoding segment %s: %w", c, err)
	}
	verified := false
	durable := w.durableSegs[c.KeyString()]
	if w.f.cfg.Verify == VerifyFull && !w.protectionOnly {
		verified, err = w.f.segmentVerified(w.head, c, durable)
		if err != nil {
			return err
		}
	}
	for _, row := range seg.Rows {
		for _, e := range row.Entries {
			if w.protectionOnly || w.f.cfg.Verify != VerifyFull || verified {
				if err := w.ensure(ctx, e.Blob); err != nil {
					return fmt.Errorf("ensuring blob %s of slot %d: %w", e.Blob, row.Slot, err)
				}
				continue
			}
			// Full verification must cover a blob even when it was already local.
			// In particular, an online-GC publication-protection walk may have
			// fetched it immediately before this semantic pass. block uses the
			// validating application view for local hits and stages network misses.
			blk, err := w.block(ctx, e.Blob)
			if err != nil {
				return fmt.Errorf("reading blob %s of slot %d for verification: %w", e.Blob, row.Slot, err)
			}
			if err := w.f.verifyBlob(w.head, e, blk.RawData()); err != nil {
				return err
			}
			w.verified++
		}
	}
	if w.f.cfg.Verify == VerifyFull && !w.protectionOnly && !verified {
		// Publish the semantic proof only after every entry succeeds. A partial
		// pass must never let a later walk skip the unverified suffix.
		if err := w.f.markSegmentVerified(w.head, c, durable); err != nil {
			return err
		}
	}
	return nil
}

func (f *Follower) segmentVerified(head string, c cid.Cid, durable bool) (bool, error) {
	key := c.KeyString()
	f.mu.Lock()
	f.classifyVerifiedSegmentLocked(head, key, durable)
	durableHot, ok := f.verifiedSegments[key]
	f.mu.Unlock()
	if ok {
		// The same immutable Segment CID can first appear as one head's open
		// Segment and later become sealed (or be shared with another head's
		// sealed Segment). Its in-memory proof is already semantically complete;
		// promote that proof when the durable classification becomes known so the
		// next restart does not needlessly verify it again.
		if durable && !durableHot {
			if err := f.state.markSegmentVerified(c); err != nil {
				return false, err
			}
			f.mu.Lock()
			f.verifiedSegments[key] = true
			f.mu.Unlock()
		}
		return true, nil
	}
	if !durable {
		return false, nil
	}
	ok, err := f.state.segmentVerified(c)
	if err != nil || !ok {
		return ok, err
	}
	f.mu.Lock()
	f.verifiedSegments[key] = true
	f.mu.Unlock()
	return true, nil
}

func (f *Follower) markSegmentVerified(head string, c cid.Cid, durable bool) error {
	if durable {
		if err := f.state.markSegmentVerified(c); err != nil {
			return err
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.verifiedSegments == nil {
		f.verifiedSegments = make(map[string]bool)
	}
	f.classifyVerifiedSegmentLocked(head, c.KeyString(), durable)
	// Never downgrade a sealed/durable proof when another head happens to see
	// the same Segment CID in its open position.
	f.verifiedSegments[c.KeyString()] = f.verifiedSegments[c.KeyString()] || durable
	return nil
}

// classifyVerifiedSegmentLocked bounds memory-only proofs to one open Segment
// per head. Durable entries are never evicted: their hot copy avoids a Pebble
// lookup on every later walk, and their on-disk marker remains authoritative.
// f.mu is held by the caller.
func (f *Follower) classifyVerifiedSegmentLocked(head, key string, durable bool) {
	if f.verifiedSegments == nil {
		f.verifiedSegments = make(map[string]bool)
	}
	if f.verifiedOpen == nil {
		f.verifiedOpen = make(map[string]string)
	}
	if durable {
		if f.verifiedOpen[head] == key {
			delete(f.verifiedOpen, head)
		}
		return
	}
	if prior := f.verifiedOpen[head]; prior != "" && prior != key {
		if priorDurable, present := f.verifiedSegments[prior]; present && !priorDurable {
			delete(f.verifiedSegments, prior)
		}
	}
	f.verifiedOpen[head] = key
}

// links returns the CIDs a dag-cbor block links to.
//
// Read out of the data model rather than out of a schema, which is what GC does
// and for the same reason (see pinning's links): every reference in the DAG is a
// real IPLD link precisely so that a generic traversal finds all of them, and a
// walk that switched on node types would silently stop at a block it did not
// recognise -- and here, "stop" means "do not fetch", which is a follower that
// serves 503 for a slot it should have had.
func links(raw []byte, c cid.Cid) ([]cid.Cid, error) {
	nb := basicnode.Prototype.Any.NewBuilder()
	if err := dagcbor.Decode(nb, bytes.NewReader(raw)); err != nil {
		return nil, fmt.Errorf("decoding block %s: %w", c, err)
	}
	var out []cid.Cid
	if err := appendLinks(nb.Build(), &out); err != nil {
		return nil, fmt.Errorf("reading the links of block %s: %w", c, err)
	}
	return out, nil
}

// appendLinks collects every link in the subtree rooted at n.
func appendLinks(n datamodel.Node, out *[]cid.Cid) error {
	switch n.Kind() {
	case datamodel.Kind_Link:
		l, err := n.AsLink()
		if err != nil {
			return err
		}
		cl, ok := l.(cidlink.Link)
		if !ok {
			return fmt.Errorf("link %s is not a CID link", l)
		}
		*out = append(*out, cl.Cid)
	case datamodel.Kind_Map:
		for it := n.MapIterator(); !it.Done(); {
			_, v, err := it.Next()
			if err != nil {
				return err
			}
			if err := appendLinks(v, out); err != nil {
				return err
			}
		}
	case datamodel.Kind_List:
		for it := n.ListIterator(); !it.Done(); {
			_, v, err := it.Next()
			if err != nil {
				return err
			}
			if err := appendLinks(v, out); err != nil {
				return err
			}
		}
	}
	return nil
}
