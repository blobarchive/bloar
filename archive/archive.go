// Package archive implements the head engine: the read path of spec 4 and the
// mutation algorithms of spec 5 (apply_refs, seal, dir_append, truncate) over
// one head.
//
// # Concurrency
//
// A Head serves reads concurrently and mutates under a single writer. Every
// read starts by loading the current state atomically and then walks only
// immutable, content-addressed blocks, so a reader either sees the state before
// a mutation or the state after it, never a half-applied one. Mutations
// serialize on an internal lock, but the caller is still responsible for
// ordering them: spec 5 is single-writer per head, and two concurrent
// ApplyRefs calls are a correctness bug in the caller even though they cannot
// corrupt the DAG here.
package archive

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/go-cid"

	"github.com/blobarchive/bloar/core"
	"github.com/blobarchive/bloar/schema"
)

// Config is the machinery a Head is built over.
type Config struct {
	// Blocks holds every block: index nodes and blobs. Required.
	Blocks blockstore.Blockstore
	// Resolver maps vh -> blob CID for ApplyRefs. Required for mutation; a
	// read-only Head may leave it nil.
	Resolver BlobResolver
	// Cache is the decoded-node cache (spec 6.3). Optional; nil means every
	// read decodes.
	Cache *core.NodeCache
	// StructureCache retains compact Segment-position proofs across successive
	// roots. Optional; a Head creates a private cache when omitted. Followers
	// should share one across all loaded generations so strict structure
	// admission remains incremental.
	StructureCache *StructureCache
	// CollectionGeneration is the optional monotonic deletion generation of
	// Blocks' underlying local store. A new generation must be allocated before
	// a collector can delete blocks, and remain current after that collector
	// ends. StructureCache uses it to reuse a Segment's presence proof only
	// while no collection boundary has occurred. When omitted, Config.Blocks is
	// checked for the same interface; if neither exposes it, cached shape proofs
	// save decoding but every admission conservatively re-reads the block.
	CollectionGeneration CollectionGenerationSource
}

// CollectionGenerationSource is the optional invalidation token used by
// StructureCache. Store.BlockstoreEpochs' application blockstore implements
// this contract.
type CollectionGenerationSource interface {
	CollectionGeneration() uint64
}

// Params are the immutable parameters of a head (spec 3.1). Changing any of
// them means building a new head.
type Params struct {
	Name       string
	Net        string
	OriginSlot uint64
	SegBits    uint64
	FanoutBits uint64
}

// Info is a snapshot of everything the publication document reports about a
// head (spec 8).
type Info struct {
	Params
	Root     cid.Cid
	SyncedTo *uint64
	DirDepth uint64
}

// state is one immutable version of a head. It is published by atomic swap and
// never mutated after that, so a reader that has loaded it may use it for as
// long as it likes.
type state struct {
	params Params

	// syncedTo is meaningful only when covered is true; covered false is the
	// spec's "synced_to: null", an empty head.
	syncedTo uint64
	covered  bool

	dirDepth uint64
	dir      cid.Cid // undef iff dirDepth == 0
	open     cid.Cid // undef iff !covered
	root     cid.Cid // CID of the Head block this state encodes to
}

// Head is the engine for one head: the read path and the mutation algorithms.
type Head struct {
	cfg Config

	segs *core.NodeStore[schema.Segment]
	dirs *core.NodeStore[schema.DirNode]
	head *core.NodeStore[schema.Head]
	// structure is the compact validation cache used by Enumerate. It carries no
	// mutable or accepted state; see StructureCache.
	structure *StructureCache

	// mu serializes mutators so that a misuse cannot interleave two applies
	// into one root. It is not the ordering guarantee: see the package comment.
	mu sync.Mutex
	// cur is the published state. Readers load it; mutators store it last.
	cur atomic.Pointer[state]
}

func newHead(cfg Config) (*Head, error) {
	if cfg.Blocks == nil {
		return nil, errors.New("archive: Config.Blocks must not be nil")
	}
	if cfg.CollectionGeneration == nil {
		cfg.CollectionGeneration, _ = cfg.Blocks.(CollectionGenerationSource)
	}
	if cfg.StructureCache == nil {
		cfg.StructureCache = NewStructureCache()
	}
	return &Head{
		cfg:       cfg,
		segs:      core.NewNodeStore(cfg.Blocks, core.Codec[schema.Segment]{Encode: schema.EncodeSegment, Decode: schema.DecodeSegment}, cfg.Cache),
		dirs:      core.NewNodeStore(cfg.Blocks, core.Codec[schema.DirNode]{Encode: schema.EncodeDirNode, Decode: schema.DecodeDirNode}, cfg.Cache),
		head:      core.NewNodeStore(cfg.Blocks, core.Codec[schema.Head]{Encode: schema.EncodeHead, Decode: schema.DecodeHead}, cfg.Cache),
		structure: cfg.StructureCache,
	}, nil
}

// New creates an empty head with the given parameters, writes its Head block,
// and returns the engine. An empty head has no directory and no open segment;
// the first ApplyRefs creates both.
func New(ctx context.Context, cfg Config, params Params) (*Head, error) {
	h, err := newHead(cfg)
	if err != nil {
		return nil, err
	}
	if params.SegBits > 63 {
		return nil, fmt.Errorf("archive: seg_bits %d is out of range", params.SegBits)
	}
	if params.FanoutBits == 0 || params.FanoutBits > 32 {
		return nil, fmt.Errorf("archive: fanout_bits %d is out of range, must be in [1, 32]", params.FanoutBits)
	}
	st := &state{params: params}
	if err := h.publish(ctx, st); err != nil {
		return nil, err
	}
	return h, nil
}

// Load opens the head rooted at root. The Head block is read now; everything
// below it is read lazily.
func Load(ctx context.Context, cfg Config, root cid.Cid) (*Head, error) {
	h, err := newHead(cfg)
	if err != nil {
		return nil, err
	}
	st, err := h.loadState(ctx, root)
	if err != nil {
		return nil, err
	}
	h.cur.Store(st)
	return h, nil
}

// CloneAt opens an independent engine over root using the same blockstore,
// resolver, and decoded-node cache as h. Mutating the returned Head never swaps
// h's in-memory state. Registry owners use this to build a complete candidate
// off-side and publish it only after durability and every prospective external
// view have been prepared.
func (h *Head) CloneAt(ctx context.Context, root cid.Cid) (*Head, error) {
	return Load(ctx, h.cfg, root)
}

// loadState reads and decodes the Head block at root.
func (h *Head) loadState(ctx context.Context, root cid.Cid) (*state, error) {
	if !root.Defined() {
		return nil, errors.New("archive: Load with an undefined root CID")
	}
	if err := validateIndexCID(root, "head root"); err != nil {
		return nil, err
	}
	blk, err := h.cfg.Blocks.Get(ctx, root)
	if err != nil {
		return nil, fmt.Errorf("archive: reading head %s: %w", root, err)
	}
	data := blk.RawData()
	if len(data) > MaxEnumerationNodeBytes {
		return nil, fmt.Errorf("archive: Head %s is %d encoded bytes, above the %d-byte per-node admission budget",
			root, len(data), MaxEnumerationNodeBytes)
	}
	if err := verifyBlockCID(root, data); err != nil {
		return nil, fmt.Errorf("archive: reading head %s: %w", root, err)
	}
	obj, err := schema.DecodeHead(data)
	if err != nil {
		return nil, fmt.Errorf("archive: decoding head %s: %w", root, err)
	}
	st := &state{
		params: Params{
			Name:       obj.Name,
			Net:        obj.Net,
			OriginSlot: obj.OriginSlot,
			SegBits:    obj.SegBits,
			FanoutBits: obj.FanoutBits,
		},
		dirDepth: obj.DirDepth,
		dir:      obj.Dir,
		open:     obj.Open,
		root:     root,
	}
	if obj.SyncedTo != nil {
		st.syncedTo, st.covered = *obj.SyncedTo, true
	}
	if st.covered != st.open.Defined() {
		return nil, fmt.Errorf("archive: head %s has synced_to set=%t but open set=%t; a covered head always has an open segment",
			root, st.covered, st.open.Defined())
	}
	if st.params.FanoutBits == 0 || st.params.FanoutBits > 32 || st.params.SegBits > 63 {
		return nil, fmt.Errorf("archive: head %s has unusable parameters seg_bits=%d fanout_bits=%d",
			root, st.params.SegBits, st.params.FanoutBits)
	}
	if _, _, err := validateDirectoryGeometry(st); err != nil {
		return nil, fmt.Errorf("archive: head %s has invalid directory geometry: %w", root, err)
	}
	if st.dir.Defined() {
		if err := validateIndexCID(st.dir, "directory root"); err != nil {
			return nil, fmt.Errorf("archive: head %s: %w", root, err)
		}
	}
	if st.open.Defined() {
		if err := validateIndexCID(st.open, "open segment"); err != nil {
			return nil, fmt.Errorf("archive: head %s: %w", root, err)
		}
	}
	return st, nil
}

// headObject renders st as the schema object it encodes to.
func (st *state) headObject() *schema.Head {
	obj := &schema.Head{
		Name:       st.params.Name,
		Net:        st.params.Net,
		OriginSlot: st.params.OriginSlot,
		SegBits:    st.params.SegBits,
		FanoutBits: st.params.FanoutBits,
		DirDepth:   st.dirDepth,
		Dir:        st.dir,
		Open:       st.open,
	}
	if st.covered {
		// A fresh pointer per call: st is shared with readers and must stay
		// immutable, and schema.Head takes the pointer by reference.
		synced := st.syncedTo
		obj.SyncedTo = &synced
	}
	return obj
}

// publish writes st's Head block and swaps it in as the current state. It is
// the last step of every mutation: every block the new state references is
// already durable by the time the swap happens, so a crash before it leaves the
// old root intact and the new blocks orphaned (spec 5).
func (h *Head) publish(ctx context.Context, st *state) error {
	root, err := h.head.NewNode(st.headObject()).Commit(ctx)
	if err != nil {
		return fmt.Errorf("archive: writing head: %w", err)
	}
	st.root = root
	h.cur.Store(st)
	return nil
}

// dirBase returns the directory index origin: the ordinal of the head's first
// window (spec 4).
func (st *state) dirBase() uint64 { return ord(st.params.OriginSlot, st.params.SegBits) }

// openOrd returns the ordinal of the open segment's window. st must be
// covered: an empty head has no open segment.
//
// This doubles as the head's sealed count, which is openOrd - dirBase. The
// count has to be derived rather than read off the directory, because pages
// trim their trailing nulls (spec 3.3) and so their length does not report how
// many entries were appended. What licenses the derivation is the invariant
// that the open segment is always the window ord(synced_to + 1): a window is
// unsealed exactly when synced_to has not passed its end, so every window below
// the open one is sealed and every window at or above it is not.
func (h *Head) openOrd(ctx context.Context, st *state) (uint64, error) {
	seg, err := h.segs.GetNode(ctx, st.open)
	if err != nil {
		return 0, fmt.Errorf("archive: reading open segment %s: %w", st.open, err)
	}
	return ord(seg.Slot0, st.params.SegBits), nil
}

// Root returns the CID of the current Head block.
func (h *Head) Root() cid.Cid { return h.cur.Load().root }

// SyncedTo returns the last covered slot. covered is false for an empty head,
// the spec's "synced_to: null".
func (h *Head) SyncedTo() (slot uint64, covered bool) {
	st := h.cur.Load()
	return st.syncedTo, st.covered
}

// Params returns the head's immutable parameters.
func (h *Head) Params() Params { return h.cur.Load().params }

// Info returns everything the publication document reports about the head
// (spec 8), read from a single atomic snapshot.
func (h *Head) Info() Info {
	st := h.cur.Load()
	info := Info{Params: st.params, Root: st.root, DirDepth: st.dirDepth}
	if st.covered {
		synced := st.syncedTo
		info.SyncedTo = &synced
	}
	return info
}
