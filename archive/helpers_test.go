package archive_test

import (
	"context"
	"encoding/binary"
	"errors"
	"sync"
	"testing"

	"github.com/ipfs/boxo/blockstore"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/multiformats/go-multihash"

	"github.com/blobarchive/bloar/archive"
	"github.com/blobarchive/bloar/schema"
)

// The test head: 8 slots per window, fanout 4. Tiny on purpose -- capacity(1)
// is 4 segments and capacity(2) is 16, so a few hundred slots exercise three
// directory levels and every growth boundary.
const (
	testSegBits    = 3
	testFanoutBits = 2
	testOrigin     = 40 // window-aligned: dir_base = 5
)

func testParams() archive.Params {
	return archive.Params{
		Name:       "test",
		Net:        "testnet",
		OriginSlot: testOrigin,
		SegBits:    testSegBits,
		FanoutBits: testFanoutBits,
	}
}

func newBlockstore() blockstore.Blockstore {
	return blockstore.NewBlockstore(dssync.MutexWrap(datastore.NewMapDatastore()))
}

// fakeCatalog stands in for the blob catalog of spec 6.1, which phase 4 owns.
type fakeCatalog struct {
	bs blockstore.Blockstore

	mu   sync.Mutex
	byVH map[schema.VersionedHash]cid.Cid
	err  error // when set, every resolve fails with it
}

func newFakeCatalog(bs blockstore.Blockstore) *fakeCatalog {
	return &fakeCatalog{bs: bs, byVH: make(map[schema.VersionedHash]cid.Cid)}
}

func (c *fakeCatalog) ResolveBlob(_ context.Context, vh schema.VersionedHash) (cid.Cid, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.err != nil {
		return cid.Undef, false, c.err
	}
	blob, ok := c.byVH[vh]
	return blob, ok, nil
}

// add registers vh and stores its block, as POST /bloar/v1/blobs would.
//
// The block stands in for a blob rather than being one: archive only ever asks
// the blockstore whether the block is present. Blob bytes, their KZG
// commitment, and the vh derivation belong to the ingest package; putting 128
// KiB behind every vh here would buy this package nothing but minutes.
func (c *fakeCatalog) add(t *testing.T, vh schema.VersionedHash) cid.Cid {
	t.Helper()
	blob := blobCID(t, vh)
	blk, err := blocks.NewBlockWithCid(blobBytes(vh), blob)
	if err != nil {
		t.Fatalf("framing blob block: %v", err)
	}
	if err := c.bs.Put(context.Background(), blk); err != nil {
		t.Fatalf("storing blob block: %v", err)
	}
	c.mu.Lock()
	c.byVH[vh] = blob
	c.mu.Unlock()
	return blob
}

// addCatalogOnly registers vh but stores no block: a catalog entry that
// outlived its block, which spec 6.1 says GC leaves behind and 5.1 step 4 must
// catch.
func (c *fakeCatalog) addCatalogOnly(t *testing.T, vh schema.VersionedHash) cid.Cid {
	t.Helper()
	blob := blobCID(t, vh)
	c.mu.Lock()
	c.byVH[vh] = blob
	c.mu.Unlock()
	return blob
}

func blobBytes(vh schema.VersionedHash) []byte {
	return append([]byte("blob:"), vh[:]...)
}

func blobCID(t *testing.T, vh schema.VersionedHash) cid.Cid {
	t.Helper()
	mh, err := multihash.Sum(blobBytes(vh), multihash.SHA2_256, -1)
	if err != nil {
		t.Fatalf("hashing blob: %v", err)
	}
	return cid.NewCidV1(cid.Raw, mh)
}

// mkVH returns the distinct versioned hash numbered n.
func mkVH(n uint64) schema.VersionedHash {
	var vh schema.VersionedHash
	vh[0] = 0x01
	binary.BigEndian.PutUint64(vh[24:], n)
	return vh
}

// harness is one head over an in-memory blockstore and catalog.
type harness struct {
	t   *testing.T
	ctx context.Context
	bs  blockstore.Blockstore
	cat *fakeCatalog
	h   *archive.Head
}

func newHarness(t *testing.T, params archive.Params) *harness {
	t.Helper()
	bs := newBlockstore()
	return newHarnessOver(t, params, bs, newFakeCatalog(bs))
}

// newHarnessOver builds a head over an existing blockstore and catalog, so two
// heads can share blocks the way the real archive does.
func newHarnessOver(t *testing.T, params archive.Params, bs blockstore.Blockstore, cat *fakeCatalog) *harness {
	t.Helper()
	ctx := context.Background()
	h, err := archive.New(ctx, archive.Config{Blocks: bs, Resolver: cat}, params)
	if err != nil {
		t.Fatalf("archive.New: %v", err)
	}
	return &harness{t: t, ctx: ctx, bs: bs, cat: cat, h: h}
}

// reload opens the head at root over the same blocks, as a restart would.
func (hs *harness) reload(t *testing.T, root cid.Cid) *harness {
	t.Helper()
	h, err := archive.Load(hs.ctx, archive.Config{Blocks: hs.bs, Resolver: hs.cat}, root)
	if err != nil {
		t.Fatalf("archive.Load(%s): %v", root, err)
	}
	return &harness{t: t, ctx: hs.ctx, bs: hs.bs, cat: hs.cat, h: h}
}

// row builds a RefRow at slot over the blobs named by ids, registering each one
// so that ApplyRefs can resolve it.
func (hs *harness) row(slot uint64, ids ...uint64) archive.RefRow {
	hs.t.Helper()
	vhs := make([]schema.VersionedHash, 0, len(ids))
	for _, id := range ids {
		vh := mkVH(id)
		hs.cat.add(hs.t, vh)
		vhs = append(vhs, vh)
	}
	return archive.RefRow{Slot: slot, VHs: vhs}
}

func (hs *harness) apply(rows []archive.RefRow, syncedTo uint64) archive.ApplyResult {
	hs.t.Helper()
	res, err := hs.h.ApplyRefs(hs.ctx, rows, syncedTo)
	if err != nil {
		hs.t.Fatalf("ApplyRefs(syncedTo=%d): %v", syncedTo, err)
	}
	return res
}

// applyErr expects ApplyRefs to fail and returns the error.
func (hs *harness) applyErr(rows []archive.RefRow, syncedTo uint64) error {
	hs.t.Helper()
	res, err := hs.h.ApplyRefs(hs.ctx, rows, syncedTo)
	if err == nil {
		hs.t.Fatalf("ApplyRefs(syncedTo=%d): want error, got %+v", syncedTo, res)
	}
	return err
}

func (hs *harness) lookup(slot uint64) archive.Result {
	hs.t.Helper()
	res, err := hs.h.Lookup(hs.ctx, slot)
	if err != nil {
		hs.t.Fatalf("Lookup(%d): %v", slot, err)
	}
	return res
}

func (hs *harness) lookupVHs(slot uint64, ids ...uint64) archive.Result {
	hs.t.Helper()
	vhs := make([]schema.VersionedHash, 0, len(ids))
	for _, id := range ids {
		vhs = append(vhs, mkVH(id))
	}
	res, err := hs.h.LookupVHs(hs.ctx, slot, vhs)
	if err != nil {
		hs.t.Fatalf("LookupVHs(%d, %v): %v", slot, ids, err)
	}
	return res
}

// wantStatus asserts a lookup's status.
func wantStatus(t *testing.T, res archive.Result, want archive.Status, what string) {
	t.Helper()
	if res.Status != want {
		t.Errorf("%s: status %v, want %v", what, res.Status, want)
	}
}

// wantBlobs asserts a found result carries exactly the blobs named by ids, in
// order.
func wantBlobs(t *testing.T, res archive.Result, what string, ids ...uint64) {
	t.Helper()
	if res.Status != archive.StatusFound {
		t.Errorf("%s: status %v, want %v", what, res.Status, archive.StatusFound)
		return
	}
	if len(res.Entries) != len(ids) {
		t.Errorf("%s: %d entries, want %d", what, len(res.Entries), len(ids))
		return
	}
	for i, id := range ids {
		vh := mkVH(id)
		if res.Entries[i].VH != vh {
			t.Errorf("%s: entry %d vh 0x%x, want 0x%x", what, i, res.Entries[i].VH[:], vh[:])
		}
		if want := blobCID(t, vh); res.Entries[i].Blob != want {
			t.Errorf("%s: entry %d blob %s, want %s", what, i, res.Entries[i].Blob, want)
		}
	}
}

// wantConflict asserts err is a *archive.ConflictError, as the server needs to
// map it to 409.
func wantConflict(t *testing.T, err error, what string) *archive.ConflictError {
	t.Helper()
	var ce *archive.ConflictError
	if !errors.As(err, &ce) {
		t.Fatalf("%s: error %v is not a *archive.ConflictError", what, err)
	}
	return ce
}

// canonicalDepth returns the directory depth a fresh build reaches after n
// appends: depth grows only when an append lands exactly on the current
// capacity (spec 5.3).
func canonicalDepth(n, fanoutBits uint64) uint64 {
	if n == 0 {
		return 0
	}
	depth, capacity := uint64(1), uint64(1)<<fanoutBits
	for n > capacity {
		depth++
		capacity <<= fanoutBits
	}
	return depth
}

// countingStore counts Get calls per CID so a test can assert which blocks a
// read actually touched.
type countingStore struct {
	blockstore.Blockstore

	mu   sync.Mutex
	gets map[cid.Cid]int
}

func newCountingStore() *countingStore {
	return &countingStore{
		Blockstore: newBlockstore(),
		gets:       make(map[cid.Cid]int),
	}
}

func (s *countingStore) Get(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	s.mu.Lock()
	s.gets[c]++
	s.mu.Unlock()
	return s.Blockstore.Get(ctx, c)
}

func (s *countingStore) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.gets = make(map[cid.Cid]int)
}

func (s *countingStore) totalGets() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, v := range s.gets {
		n += v
	}
	return n
}

// blockCIDs returns every block in the store.
func blockCIDs(t *testing.T, bs blockstore.Blockstore) map[cid.Cid]bool {
	t.Helper()
	ch, err := bs.AllKeysChan(context.Background())
	if err != nil {
		t.Fatalf("AllKeysChan: %v", err)
	}
	out := make(map[cid.Cid]bool)
	for k := range ch {
		out[k] = true
	}
	return out
}
