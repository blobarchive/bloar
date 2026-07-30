package pinning_test

import (
	"context"
	"encoding/binary"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/ipfs/boxo/blockstore"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/multiformats/go-multihash"

	"github.com/blobarchive/bloar/archive"
	"github.com/blobarchive/bloar/catalog"
	"github.com/blobarchive/bloar/pinning"
	"github.com/blobarchive/bloar/schema"
)

// The test heads: 4 slots per window, fanout 4, origin 8 (so dir_base is 2).
// Tiny on purpose -- a handful of windows exercises sealing, directory growth
// and a sliding window, and every slot number in a test can be read at sight.
const (
	testSegBits        = 2
	testFanoutBits     = 2
	testOrigin         = 8
	testSecondsPerSlot = 12
)

// slotsDur renders n slots as the duration a config would carry.
func slotsDur(n uint64) time.Duration {
	return time.Duration(n*testSecondsPerSlot) * time.Second
}

// fakeCatalog stands in for the blob catalog of spec 6.1.
//
// Its "blobs" are a few bytes rather than 128 KiB of KZG-verified data: GC
// marks and sweeps blocks by CID and never looks inside a raw one, so a blob
// here has to be a distinct raw block and nothing more. The ingest pipeline is
// what makes a real one, and it is tested where it lives.
type fakeCatalog struct {
	mu   sync.Mutex
	byVH map[schema.VersionedHash]cid.Cid
}

func (c *fakeCatalog) ResolveBlob(_ context.Context, vh schema.VersionedHash) (cid.Cid, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	blob, ok := c.byVH[vh]
	return blob, ok, nil
}

// mkVH returns the distinct versioned hash numbered n.
func mkVH(n uint64) schema.VersionedHash {
	var vh schema.VersionedHash
	vh[0] = 0x01
	binary.BigEndian.PutUint64(vh[24:], n)
	return vh
}

func blobBytes(id uint64) []byte { return fmt.Appendf(nil, "blob:%d", id) }

// fixture is one blockstore and one KV, shared by every head in a test: two
// heads that reference the same blob reference the same block, which is what
// makes cross-head sharing testable at all (data-structures 6).
type fixture struct {
	t      *testing.T
	ctx    context.Context
	bs     blockstore.Blockstore
	config fixtureConfig
	led    *catalog.Ledger
	cat    *fakeCatalog
	rec    *pinning.Reconciler
	gc     *pinning.GC
	// staging is nil unless the test asked for it (withStaging), which is what
	// makes "a node whose ingest does not stage" a case the other tests cover
	// for free.
	staging *pinning.Staging

	// manifestTips is the per-head manifest tip the reconciler reads (spec 10.5),
	// standing in for server.ManifestStore. A head with no entry has no chain.
	manifestTips map[string]cid.Cid

	// clock is what the staging TTL is measured against, so that an expiry is a
	// call to advance rather than a sleep.
	clockMu sync.Mutex
	clock   time.Time

	// labels names every block the fixture knows about, keyed as the mark set
	// keys it: by multihash, because that is what the blockstore is keyed by.
	labels map[string]string
}

// fixtureConfig is what newFixture's options set.
type fixtureConfig struct {
	staging    bool
	stagingTTL time.Duration
	// stableGeneration models an application blockstore between collection
	// boundaries. Missing-block tests use it so reconciliation may reuse the
	// pre-loss structure proof and exercise GC's per-head mark/self-heal policy,
	// as the production generation-aware store does.
	stableGeneration bool
}

type fixtureOpt func(*fixtureConfig)

// withStaging gives the fixture the staging pins of spec 9's window (a), with
// the given TTL (zero is pinning.DefaultStagingTTL).
func withStaging(ttl time.Duration) fixtureOpt {
	return func(c *fixtureConfig) { c.staging, c.stagingTTL = true, ttl }
}

func withStableCollectionGeneration() fixtureOpt {
	return func(c *fixtureConfig) { c.stableGeneration = true }
}

type staticCollectionGeneration struct{}

func (staticCollectionGeneration) CollectionGeneration() uint64 { return 0 }

// epoch is the fixture clock's zero. A fixed instant rather than time.Now, so
// that a test's expiries are the same numbers on every run.
var epoch = time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)

func newFixture(t *testing.T, opts ...fixtureOpt) *fixture {
	t.Helper()
	var fc fixtureConfig
	for _, o := range opts {
		o(&fc)
	}

	bs := blockstore.NewBlockstore(dssync.MutexWrap(datastore.NewMapDatastore()))
	kv, err := pebble.Open(filepath.Join(t.TempDir(), "kv"), &pebble.Options{})
	if err != nil {
		t.Fatalf("opening kv: %v", err)
	}
	t.Cleanup(func() {
		if err := kv.Close(); err != nil {
			t.Errorf("closing kv: %v", err)
		}
	})

	f := &fixture{
		t:            t,
		ctx:          context.Background(),
		bs:           bs,
		config:       fc,
		led:          catalog.NewLedger(kv),
		cat:          &fakeCatalog{byVH: map[schema.VersionedHash]cid.Cid{}},
		clock:        epoch,
		labels:       map[string]string{},
		manifestTips: map[string]cid.Cid{},
	}
	f.rec, err = pinning.NewReconciler(pinning.Config{Ledger: f.led, ManifestTip: f.manifestTip})
	if err != nil {
		t.Fatalf("NewReconciler: %v", err)
	}
	if fc.staging {
		f.staging, err = pinning.NewStaging(pinning.StagingConfig{
			Ledger:   f.led,
			Resolver: f.cat,
			TTL:      fc.stagingTTL,
			Now:      f.now,
		})
		if err != nil {
			t.Fatalf("NewStaging: %v", err)
		}
	}
	f.gc, err = pinning.NewGC(pinning.GCConfig{Blocks: bs, Reconciler: f.rec, Staging: f.staging})
	if err != nil {
		t.Fatalf("NewGC: %v", err)
	}
	return f
}

// now is the fixture clock, as pinning.StagingConfig.Now.
func (f *fixture) now() time.Time {
	f.clockMu.Lock()
	defer f.clockMu.Unlock()
	return f.clock
}

// advance moves the fixture clock forward.
func (f *fixture) advance(d time.Duration) {
	f.clockMu.Lock()
	defer f.clockMu.Unlock()
	f.clock = f.clock.Add(d)
}

// stage pins the named blobs as ingest would, having stored their blocks first.
func (f *fixture) stage(ids ...uint64) {
	f.t.Helper()
	cids := make([]cid.Cid, 0, len(ids))
	for _, id := range ids {
		f.putBlob(id)
		cids = append(cids, f.blobCID(id))
	}
	if err := f.staging.Pin(f.ctx, cids); err != nil {
		f.t.Fatalf("Staging.Pin(%v): %v", ids, err)
	}
}

// stagingPins returns every staging row.
func (f *fixture) stagingPins() []catalog.PinEntry {
	f.t.Helper()
	entries, err := f.staging.List(f.ctx)
	if err != nil {
		f.t.Fatalf("Staging.List: %v", err)
	}
	return entries
}

// manifestTip is the reconciler's tip lookup (pinning.Config.ManifestTip).
func (f *fixture) manifestTip(_ context.Context, head string) (cid.Cid, bool, error) {
	c, ok := f.manifestTips[head]
	return c, ok, nil
}

// putManifest stores a real dag-cbor Manifest block chained to prev and returns
// its CID. The block is decodable, so GC walks its prev link the way it walks any
// index block -- which is the point: the chain is protected by following links,
// not by GC knowing what a manifest is.
func (f *fixture) putManifest(head string, prev cid.Cid, from uint64) cid.Cid {
	f.t.Helper()
	m := &schema.Manifest{
		V:    schema.ManifestVersion,
		Head: head,
		Sources: []schema.Source{{
			Type:      schema.SourceInboxEvents,
			Address:   make([]byte, schema.AddressSize),
			Topic:     make([]byte, schema.TopicSize),
			FromBlock: from,
			OpenEnded: true,
		}},
		Prev: prev,
	}
	data, c, err := schema.EncodeManifest(m)
	if err != nil {
		f.t.Fatalf("EncodeManifest: %v", err)
	}
	blk, err := blocks.NewBlockWithCid(data, c)
	if err != nil {
		f.t.Fatalf("framing manifest: %v", err)
	}
	if err := f.bs.Put(f.ctx, blk); err != nil {
		f.t.Fatalf("storing manifest: %v", err)
	}
	f.label(c, fmt.Sprintf("manifest %s (from_block %d)", head, from))
	return c
}

// setManifestTip points a head at a manifest tip, as an accepted upgrade would.
func (f *fixture) setManifestTip(head string, tip cid.Cid) { f.manifestTips[head] = tip }

// head creates a head under a policy and registers it with the reconciler.
func (f *fixture) head(name string, p pinning.Policy) *archive.Head {
	f.t.Helper()
	cfg := archive.Config{Blocks: f.bs, Resolver: f.cat}
	if f.config.stableGeneration {
		cfg.CollectionGeneration = staticCollectionGeneration{}
	}
	h, err := archive.New(f.ctx, cfg, archive.Params{
		Name:       name,
		Net:        "testnet",
		OriginSlot: testOrigin,
		SegBits:    testSegBits,
		FanoutBits: testFanoutBits,
	})
	if err != nil {
		f.t.Fatalf("archive.New(%q): %v", name, err)
	}
	if err := f.rec.Add(h, p); err != nil {
		f.t.Fatalf("Reconciler.Add(%q): %v", name, err)
	}
	return h
}

// row builds a RefRow at slot over the blobs named by ids, storing each blob
// block and registering it in the catalog, as POST /bloar/v1/blobs would.
func (f *fixture) row(slot uint64, ids ...uint64) archive.RefRow {
	f.t.Helper()
	vhs := make([]schema.VersionedHash, 0, len(ids))
	for _, id := range ids {
		vhs = append(vhs, f.putBlob(id))
	}
	return archive.RefRow{Slot: slot, VHs: vhs}
}

// putBlob stores blob id's block. Storing the same id twice is storing the same
// bytes: one block, one CID, however many heads reference it.
func (f *fixture) putBlob(id uint64) schema.VersionedHash {
	f.t.Helper()
	vh := mkVH(id)
	c := f.blobCID(id)
	blk, err := blocks.NewBlockWithCid(blobBytes(id), c)
	if err != nil {
		f.t.Fatalf("framing blob %d: %v", id, err)
	}
	if err := f.bs.Put(f.ctx, blk); err != nil {
		f.t.Fatalf("storing blob %d: %v", id, err)
	}
	f.cat.mu.Lock()
	f.cat.byVH[vh] = c
	f.cat.mu.Unlock()
	f.label(c, fmt.Sprintf("blob %d", id))
	return vh
}

func (f *fixture) blobCID(id uint64) cid.Cid {
	f.t.Helper()
	mh, err := multihash.Sum(blobBytes(id), multihash.SHA2_256, -1)
	if err != nil {
		f.t.Fatalf("hashing blob %d: %v", id, err)
	}
	return cid.NewCidV1(cid.Raw, mh)
}

// putIndexNode stores a real, decodable dag-cbor index node (a Segment) and
// returns its CID. It stands in for a block a follower's fetch pass has made
// durable: a follower stages index nodes, not only blobs, so a test of the fetch
// window needs a staged block that is not a leaf. The blob it references need
// not exist -- a staging pin is direct, so the mark keeps the node without ever
// reading its links.
func (f *fixture) putIndexNode(slot uint64) cid.Cid {
	f.t.Helper()
	seg := &schema.Segment{Slot0: slot, Rows: []schema.Row{
		{Slot: slot, Entries: []schema.RefEntry{{VH: mkVH(slot), Blob: f.blobCID(slot)}}},
	}}
	data, c, err := schema.EncodeSegment(seg)
	if err != nil {
		f.t.Fatalf("EncodeSegment: %v", err)
	}
	blk, err := blocks.NewBlockWithCid(data, c)
	if err != nil {
		f.t.Fatalf("framing index node: %v", err)
	}
	if err := f.bs.Put(f.ctx, blk); err != nil {
		f.t.Fatalf("storing index node: %v", err)
	}
	f.label(c, fmt.Sprintf("index node (segment slot0=%d)", slot))
	return c
}

func (f *fixture) apply(h *archive.Head, syncedTo uint64, rows ...archive.RefRow) {
	f.t.Helper()
	if _, err := h.ApplyRefs(f.ctx, rows, syncedTo); err != nil {
		f.t.Fatalf("ApplyRefs(%q, syncedTo=%d): %v", h.Params().Name, syncedTo, err)
	}
}

func (f *fixture) enumerate(h *archive.Head) *archive.Enumeration {
	f.t.Helper()
	e, err := h.Enumerate(f.ctx)
	if err != nil {
		f.t.Fatalf("Enumerate(%q): %v", h.Params().Name, err)
	}
	return e
}

func (f *fixture) reconcileAll() pinning.Delta {
	f.t.Helper()
	delta, err := f.rec.ReconcileAll(f.ctx)
	if err != nil {
		f.t.Fatalf("ReconcileAll: %v", err)
	}
	return delta
}

func (f *fixture) runGC() pinning.GCStats {
	f.t.Helper()
	stats, err := f.gc.Run(f.ctx)
	if err != nil {
		f.t.Fatalf("gc.Run: %v", err)
	}
	return stats
}

func (f *fixture) pins(head string) []catalog.PinEntry {
	f.t.Helper()
	entries, err := f.led.ListAll(f.ctx, head)
	if err != nil {
		f.t.Fatalf("ListAll(%q): %v", head, err)
	}
	return entries
}

// hasPinAt reports whether the ledger holds exactly this row.
func hasPinAt(entries []catalog.PinEntry, purpose string, c cid.Cid, recursive bool) bool {
	for _, e := range entries {
		if e.Purpose == purpose && e.CID == c && e.Recursive == recursive {
			return true
		}
	}
	return false
}

// markKey is a block's identity in the blockstore: its multihash. A CID would
// not do -- the blockstore is multihash-keyed, so it reports every block under
// a raw-codec CID whatever codec it was written with.
func markKey(c cid.Cid) string { return string(c.Hash()) }

func (f *fixture) label(c cid.Cid, name string) { f.labels[markKey(c)] = name }

func (f *fixture) named(key string) string {
	if n, ok := f.labels[key]; ok {
		return n
	}
	return "unlabelled block (an orphan of an earlier root?)"
}

// blockSet is every block currently in the blockstore, keyed as the mark set
// keys them.
func (f *fixture) blockSet() map[string]bool {
	f.t.Helper()
	keys, err := f.bs.AllKeysChan(f.ctx)
	if err != nil {
		f.t.Fatalf("AllKeysChan: %v", err)
	}
	out := map[string]bool{}
	for c := range keys {
		out[markKey(c)] = true
	}
	return out
}

// survivors is the set of blocks a test expects to be left, built by naming
// them. Every expectation in this package is exact: a GC test that only checks
// what survived would pass while sweeping nothing.
type survivors struct {
	f    *fixture
	want map[string]string // mark key -> label
}

func (f *fixture) expect() *survivors { return &survivors{f: f, want: map[string]string{}} }

// blobs adds blob blocks by id.
func (s *survivors) blobs(ids ...uint64) *survivors {
	for _, id := range ids {
		c := s.f.blobCID(id)
		s.want[markKey(c)] = fmt.Sprintf("blob %d", id)
	}
	return s
}

// index adds every index block of a head's current root: the root, every
// DirNode page, every sealed Segment, the open Segment.
func (s *survivors) index(h *archive.Head) *survivors {
	e := s.f.enumerate(h)
	name := h.Params().Name
	s.want[markKey(e.Root)] = name + " root"
	for i, c := range e.DirPages {
		s.want[markKey(c)] = fmt.Sprintf("%s dir page %d", name, i)
	}
	for _, seg := range e.Sealed {
		s.want[markKey(seg.CID)] = fmt.Sprintf("%s sealed segment ord %d (slots %d..%d)", name, seg.Ord, seg.FirstSlot, seg.LastSlot)
	}
	if e.Open.Defined() {
		s.want[markKey(e.Open)] = fmt.Sprintf("%s open segment (ord %d)", name, e.OpenOrd)
	}
	return s
}

// cid adds one block by CID, for the cases a test names directly.
func (s *survivors) cid(c cid.Cid, label string) *survivors {
	s.want[markKey(c)] = label
	return s
}

// manifests adds a manifest chain by CID: every Manifest a recursive tip pin
// keeps alive back to genesis.
func (s *survivors) manifests(cids ...cid.Cid) *survivors {
	for i, c := range cids {
		s.want[markKey(c)] = fmt.Sprintf("manifest %d", i)
	}
	return s
}

// check asserts the blockstore holds exactly these blocks.
func (s *survivors) check() {
	s.f.t.Helper()
	got := s.f.blockSet()
	for k, label := range s.want {
		if !got[k] {
			s.f.t.Errorf("swept %s; it must survive", label)
		}
	}
	for k := range got {
		if _, ok := s.want[k]; !ok {
			s.f.t.Errorf("kept %s; it must be collected", s.f.named(k))
		}
	}
}
