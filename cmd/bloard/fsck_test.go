package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/ipfs/boxo/blockstore"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/multiformats/go-multihash"

	"github.com/blobarchive/bloar/catalog"
	"github.com/blobarchive/bloar/store"
)

// fsckFixture is a store holding a two-block DAG -- a dag-cbor index node
// recursively pinned under head "h", linking one raw blob leaf -- with the blob
// stored corrupt (wrong bytes under its key) or honest, as the test asks.
type fsckFixture struct {
	dir   string
	index cid.Cid
	blob  cid.Cid
	// honest is the bytes the blob's CID commits to, so a test can put them back.
	honest []byte
}

func cidUnder(t *testing.T, codec uint64, data []byte) cid.Cid {
	t.Helper()
	c, err := cid.Prefix{Version: 1, Codec: codec, MhType: multihash.SHA2_256, MhLength: -1}.Sum(data)
	if err != nil {
		t.Fatalf("hashing: %v", err)
	}
	return c
}

// dagLinkNode builds a dag-cbor block that is a list of links to children.
func dagLinkNode(t *testing.T, children ...cid.Cid) blocks.Block {
	t.Helper()
	nb := basicnode.Prototype.List.NewBuilder()
	la, err := nb.BeginList(int64(len(children)))
	if err != nil {
		t.Fatalf("begin list: %v", err)
	}
	for _, c := range children {
		if err := la.AssembleValue().AssignLink(cidlink.Link{Cid: c}); err != nil {
			t.Fatalf("assign link: %v", err)
		}
	}
	if err := la.Finish(); err != nil {
		t.Fatalf("finish list: %v", err)
	}
	var buf bytes.Buffer
	if err := dagcbor.Encode(nb.Build(), &buf); err != nil {
		t.Fatalf("encode dag-cbor: %v", err)
	}
	data := buf.Bytes()
	blk, err := blocks.NewBlockWithCid(data, cidUnder(t, cid.DagCBOR, data))
	if err != nil {
		t.Fatalf("framing node: %v", err)
	}
	return blk
}

// newFsckFixture writes the DAG and pins the index node recursively under head
// "h". If corruptBlob, the blob leaf is stored with bytes that do not hash to its
// CID; otherwise it is honest.
func newFsckFixture(t *testing.T, corruptBlob bool) *fsckFixture {
	t.Helper()
	ctx := context.Background()
	dir := t.TempDir()
	st, err := store.Open(dir, store.WithPebbleLogger(quietPebble{}))
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	defer func() {
		if err := st.Close(); err != nil {
			t.Fatalf("store.Close: %v", err)
		}
	}()

	honest := []byte("the honest bytes of a raw blob leaf")
	blobCID := cidUnder(t, cid.Raw, honest)

	blobBytes := honest
	if corruptBlob {
		blobBytes = []byte("corrupted bytes, wrong hash, same key")
	}
	blobBlk, err := blocks.NewBlockWithCid(blobBytes, blobCID)
	if err != nil {
		t.Fatalf("framing blob: %v", err)
	}
	if err := st.Blocks().Put(ctx, blobBlk); err != nil {
		t.Fatalf("storing blob: %v", err)
	}

	index := dagLinkNode(t, blobCID)
	if err := st.Blocks().Put(ctx, index); err != nil {
		t.Fatalf("storing index node: %v", err)
	}

	ledger := catalog.NewLedger(st.KV())
	if err := ledger.Add(ctx, "h", "root", index.Cid(), true); err != nil {
		t.Fatalf("pinning index node: %v", err)
	}

	return &fsckFixture{dir: dir, index: index.Cid(), blob: blobCID, honest: honest}
}

func (f *fsckFixture) config() *Config {
	// fsckHeads reads only the keys of Heads, so a zero-value HeadConfig suffices
	// to name head "h" the pins live under.
	return &Config{Store: StoreConfig{Path: f.dir}, Heads: map[string]HeadConfig{"h": {}}}
}

// has reopens the store and reports whether a block is present, for asserting
// what --repair deleted.
func (f *fsckFixture) has(t *testing.T, c cid.Cid) bool {
	t.Helper()
	st, err := store.Open(f.dir, store.WithPebbleLogger(quietPebble{}))
	if err != nil {
		t.Fatalf("reopening store: %v", err)
	}
	defer func() { _ = st.Close() }()
	// Has bypasses validation (it reads no bytes), so it answers "is the key
	// there" even for a corrupt block -- which is what a delete assertion wants.
	has, err := blockstore.Blockstore(st.Blocks()).Has(context.Background(), c)
	if err != nil {
		t.Fatalf("Has(%s): %v", c, err)
	}
	return has
}

func TestFsckReportsCorruptWithoutDeleting(t *testing.T) {
	f := newFsckFixture(t, true)
	var out bytes.Buffer

	err := fsck(context.Background(), f.config(), false, "", &out)
	if !errors.Is(err, errCorruptBlocksFound) {
		t.Fatalf("fsck report-only: got %v, want errCorruptBlocksFound", err)
	}
	if !strings.Contains(out.String(), f.blob.String()) {
		t.Errorf("report did not list the corrupt blob %s:\n%s", f.blob, out.String())
	}
	if strings.Contains(out.String(), "deleted") {
		t.Errorf("report-only mentioned a deletion:\n%s", out.String())
	}
	// The block is still there: report-only never deletes.
	if !f.has(t, f.blob) {
		t.Error("report-only deleted the corrupt block")
	}
}

func TestFsckRepairRefusesWhileLocked(t *testing.T) {
	f := newFsckFixture(t, true)

	// Hold the store lock, standing in for a running daemon.
	held, err := store.Open(f.dir, store.WithPebbleLogger(quietPebble{}))
	if err != nil {
		t.Fatalf("holding the store lock: %v", err)
	}
	defer func() { _ = held.Close() }()

	var out bytes.Buffer
	err = fsck(context.Background(), f.config(), true, "", &out)
	if err == nil || errors.Is(err, errCorruptBlocksFound) {
		t.Fatalf("fsck --repair while locked: got %v, want a lock refusal", err)
	}
	if !strings.Contains(err.Error(), "locked") {
		t.Errorf("refusal did not mention the lock: %v", err)
	}
	// Nothing was deleted: it refused before opening.
	if err := held.Close(); err != nil {
		t.Fatalf("closing held store: %v", err)
	}
	if !f.has(t, f.blob) {
		t.Error("fsck --repair deleted a block despite refusing on the lock")
	}
}

func TestFsckRepairDeletesExactlyCorruptBlocks(t *testing.T) {
	f := newFsckFixture(t, true)
	var out bytes.Buffer

	err := fsck(context.Background(), f.config(), true, "", &out)
	if !errors.Is(err, errCorruptBlocksFound) {
		t.Fatalf("fsck --repair: got %v, want errCorruptBlocksFound", err)
	}
	if !f.has(t, f.index) {
		t.Error("--repair deleted the honest index node")
	}
	if f.has(t, f.blob) {
		t.Error("--repair did not delete the corrupt blob")
	}
	// The discovery inventory lists the block corrupt, then the delete line records
	// its removal -- the inventory precedes the mutation.
	s := out.String()
	if !strings.Contains(s, "corrupt\t"+f.blob.String()) {
		t.Errorf("report did not list the corrupt block in its inventory:\n%s", s)
	}
	if !strings.Contains(s, "deleted\t"+f.blob.String()) {
		t.Errorf("report did not record the deletion:\n%s", s)
	}
	if strings.Index(s, "corrupt\t"+f.blob.String()) > strings.Index(s, "deleted\t"+f.blob.String()) {
		t.Errorf("the deletion was printed before the inventory:\n%s", s)
	}
}

func TestFsckCleanStorePasses(t *testing.T) {
	f := newFsckFixture(t, false)
	var out bytes.Buffer

	if err := fsck(context.Background(), f.config(), false, "", &out); err != nil {
		t.Fatalf("fsck on a clean store: %v", err)
	}
	if !strings.Contains(out.String(), "corrupt: 0") {
		t.Errorf("clean report did not say zero corrupt:\n%s", out.String())
	}
}

// chainFixture is a three-block chain A -> B -> L (two dag-cbor nodes over one raw
// leaf) with the leaf stored corrupt. It pins nothing; each test adds the pins
// whose ordering it exercises. The store stays open so pins and the walk share it.
type chainFixture struct {
	st      *store.Store
	a, b, l cid.Cid
}

func newChainFixture(t *testing.T) *chainFixture {
	t.Helper()
	ctx := context.Background()
	st := openStore(t, t.TempDir())
	t.Cleanup(func() { _ = st.Close() })

	honest := []byte("chain leaf honest bytes")
	leaf := cidUnder(t, cid.Raw, honest)
	putCorrupt(t, st, leaf, []byte("corrupt chain leaf, wrong hash"))

	b := dagLinkNode(t, leaf)
	if err := st.Blocks().Put(ctx, b); err != nil {
		t.Fatalf("storing B: %v", err)
	}
	a := dagLinkNode(t, b.Cid())
	if err := st.Blocks().Put(ctx, a); err != nil {
		t.Fatalf("storing A: %v", err)
	}
	return &chainFixture{st: st, a: a.Cid(), b: b.Cid(), l: leaf}
}

func (f *chainFixture) pin(t *testing.T, head, purpose string, c cid.Cid, recursive bool) {
	t.Helper()
	if err := catalog.NewLedger(f.st.KV()).Add(context.Background(), head, purpose, c, recursive); err != nil {
		t.Fatalf("pinning %s under %q/%q: %v", c, head, purpose, err)
	}
}

func (f *chainFixture) walk(t *testing.T, heads ...string) fsckReport {
	t.Helper()
	var out bytes.Buffer
	report, err := runFsck(context.Background(), f.st, heads, false, &out)
	if err != nil {
		t.Fatalf("runFsck: %v", err)
	}
	return report
}

// TestFsckExpandsDirectPinnedNodeUnderLaterRecursivePin is the follow-up blocker: a
// node validated under a DIRECT pin must still be EXPANDED when a RECURSIVE pin
// reaches it, or its subtree (here a corrupt leaf) goes unchecked. B is direct-
// pinned and A -> B -> corrupt-leaf is recursive-pinned; the leaf must be reported.
func TestFsckExpandsDirectPinnedNodeUnderLaterRecursivePin(t *testing.T) {
	f := newChainFixture(t)
	f.pin(t, "h", "index", f.b, false) // direct: validated, not expanded
	f.pin(t, "h", "root", f.a, true)   // recursive: reaches B, must expand it

	report := f.walk(t, "h")
	if !containsCID(report.corrupt, f.l) {
		t.Fatalf("corrupt leaf under a direct-then-recursive pin was not reported; corrupt=%v", report.corrupt)
	}
}

// TestFsckExpandsAcrossHeadsDirectThenRecursive is the cross-head variant: B is
// direct-pinned in one head and reachable via a recursive pin in another, walked
// in that order. The shared expanded/validated state must not let the direct visit
// suppress the recursive expansion.
func TestFsckExpandsAcrossHeadsDirectThenRecursive(t *testing.T) {
	f := newChainFixture(t)
	f.pin(t, "h1", "index", f.b, false) // direct in h1
	f.pin(t, "h2", "root", f.a, true)   // recursive in h2

	report := f.walk(t, "h1", "h2") // h1 (direct) walked before h2 (recursive)
	if !containsCID(report.corrupt, f.l) {
		t.Fatalf("corrupt leaf shared across heads was not reported; corrupt=%v", report.corrupt)
	}
}

// deleteFailBS fails DeleteBlock for one CID and delegates everything else, so a
// test can drive --repair through a partial deletion failure.
type deleteFailBS struct {
	blockstore.Blockstore
	failCID cid.Cid
}

func (d deleteFailBS) DeleteBlock(ctx context.Context, c cid.Cid) error {
	if c.Equals(d.failCID) {
		return errors.New("injected delete failure")
	}
	return d.Blockstore.DeleteBlock(ctx, c)
}

// TestFsckRepairContinuesPastDeleteFailure is the follow-up blocker: --repair must
// print the full inventory before any mutation, then attempt every deletion,
// continuing past a failure and naming it in the exit status -- never half-apply
// and return with no report.
func TestFsckRepairContinuesPastDeleteFailure(t *testing.T) {
	ctx := context.Background()
	st := openStore(t, t.TempDir())
	defer st.Close()

	c1 := cidUnder(t, cid.Raw, []byte("first corrupt block bytes"))
	c2 := cidUnder(t, cid.Raw, []byte("second corrupt block bytes"))
	putCorrupt(t, st, c1, []byte("corrupt one"))
	putCorrupt(t, st, c2, []byte("corrupt two"))
	ledger := catalog.NewLedger(st.KV())
	if err := ledger.Add(ctx, "h", "root", c1, false); err != nil {
		t.Fatalf("pinning c1: %v", err)
	}
	if err := ledger.Add(ctx, "h", "root", c2, false); err != nil {
		t.Fatalf("pinning c2: %v", err)
	}

	failing := deleteFailBS{Blockstore: st.Blocks(), failCID: c2}
	var out bytes.Buffer
	report, err := fsckCore(ctx, failing, ledger, []string{"h"}, true, &out)
	if err != nil {
		t.Fatalf("fsckCore returned a fatal error, want the failure carried in the report: %v", err)
	}

	s := out.String()
	// Both inventories survive to the output. First, the complete pre-mutation
	// finding list: both corrupt blocks, before any delete line.
	if !strings.Contains(s, "corrupt\t"+c1.String()) || !strings.Contains(s, "corrupt\t"+c2.String()) {
		t.Errorf("discovery inventory did not list both corrupt blocks:\n%s", s)
	}
	firstMutation := len(s)
	for _, marker := range []string{"deleted\t", "delete-failed\t"} {
		if i := strings.Index(s, marker); i >= 0 && i < firstMutation {
			firstMutation = i
		}
	}
	if strings.Index(s, "corrupt\t"+c1.String()) > firstMutation || strings.Index(s, "corrupt\t"+c2.String()) > firstMutation {
		t.Errorf("a mutation line was printed before the discovery inventory:\n%s", s)
	}
	// Second, the per-CID repair outcomes: exactly one deleted line and one
	// delete-failed line, naming the right blocks -- a successful deletion is not
	// erased by the failure.
	if !strings.Contains(s, "deleted\t"+c1.String()) {
		t.Errorf("output did not record c1's successful deletion:\n%s", s)
	}
	if !strings.Contains(s, "delete-failed\t"+c2.String()) {
		t.Errorf("output did not record c2's delete failure:\n%s", s)
	}
	// The report struct carries both outcomes too.
	if !containsCID(report.repaired, c1) {
		t.Errorf("c1 was not recorded deleted: %v", report.repaired)
	}
	if len(report.deleteFailed) != 1 || !report.deleteFailed[0].cid.Equals(c2) {
		t.Errorf("c2's delete failure was not recorded: %v", report.deleteFailed)
	}
	// Exit status is nonzero and names the block that could not be deleted.
	exit := report.exitError()
	if exit == nil || !strings.Contains(exit.Error(), c2.String()) {
		t.Errorf("exit status did not name the failed block: %v", exit)
	}
	// And the store reflects it: c1 gone, c2 still present.
	if has, _ := blockstore.Blockstore(st.Blocks()).Has(ctx, c1); has {
		t.Error("c1 was not actually deleted")
	}
	if has, _ := blockstore.Blockstore(st.Blocks()).Has(ctx, c2); !has {
		t.Error("c2 was deleted despite the injected failure")
	}
}

// TestFsckMissingPinnedBlockExitsNonzero is rider A: a dangling pin (a block the
// ledger names but the store does not hold) produces a nonzero exit, typed
// separately from corruption.
func TestFsckMissingPinnedBlockExitsNonzero(t *testing.T) {
	ctx := context.Background()
	st := openStore(t, t.TempDir())
	defer st.Close()

	missing := cidUnder(t, cid.Raw, []byte("never stored, only pinned"))
	if err := catalog.NewLedger(st.KV()).Add(ctx, "h", "root", missing, false); err != nil {
		t.Fatalf("pinning the missing block: %v", err)
	}

	var out bytes.Buffer
	report, err := runFsck(ctx, st, []string{"h"}, false, &out)
	if err != nil {
		t.Fatalf("runFsck: %v", err)
	}
	if len(report.corrupt) != 0 {
		t.Errorf("a missing block was miscounted as corrupt: %v", report.corrupt)
	}
	if !containsCID(report.missing, missing) {
		t.Errorf("the missing block was not reported: %v", report.missing)
	}
	exit := report.exitError()
	if !errors.Is(exit, errMissingBlocksFound) {
		t.Errorf("a missing block did not set the missing exit status: %v", exit)
	}
	if errors.Is(exit, errCorruptBlocksFound) {
		t.Error("a missing-only run flagged corruption")
	}
	if !strings.Contains(out.String(), "missing\t"+missing.String()) {
		t.Errorf("report did not list the missing block:\n%s", out.String())
	}
}

// alwaysFailWriter fails every write, for the pre-mutation discovery boundary.
type alwaysFailWriter struct{}

func (alwaysFailWriter) Write([]byte) (int, error) { return 0, errors.New("injected write failure") }

// failOnContainsWriter fails a write whose bytes contain marker and delegates the
// rest, so a test can let the discovery inventory through and fail only the
// mutation-phase outcome lines.
type failOnContainsWriter struct {
	w      io.Writer
	marker string
}

func (f *failOnContainsWriter) Write(p []byte) (int, error) {
	if strings.Contains(string(p), f.marker) {
		return 0, errors.New("injected write failure")
	}
	return f.w.Write(p)
}

// TestFsckRepairDiscoveryWriteFailureIsFatalBeforeMutation is the follow-up blocker's
// pre-mutation half: if the discovery inventory cannot be written, fsck --repair
// must return the error and delete NOTHING -- an operator must never be left having
// deleted blocks whose finding they could not see.
func TestFsckRepairDiscoveryWriteFailureIsFatalBeforeMutation(t *testing.T) {
	ctx := context.Background()
	st := openStore(t, t.TempDir())
	defer st.Close()

	c1 := cidUnder(t, cid.Raw, []byte("first corrupt block bytes"))
	c2 := cidUnder(t, cid.Raw, []byte("second corrupt block bytes"))
	putCorrupt(t, st, c1, []byte("corrupt one"))
	putCorrupt(t, st, c2, []byte("corrupt two"))
	ledger := catalog.NewLedger(st.KV())
	if err := ledger.Add(ctx, "h", "root", c1, false); err != nil {
		t.Fatalf("pinning c1: %v", err)
	}
	if err := ledger.Add(ctx, "h", "root", c2, false); err != nil {
		t.Fatalf("pinning c2: %v", err)
	}

	report, err := fsckCore(ctx, st.Blocks(), ledger, []string{"h"}, true, alwaysFailWriter{})
	if err == nil {
		t.Fatal("a discovery write failure did not return an error")
	}
	// No mutation was even attempted.
	if len(report.repaired) != 0 || len(report.deleteFailed) != 0 {
		t.Errorf("the store was mutated despite a discovery write failure: repaired=%v failed=%v",
			report.repaired, report.deleteFailed)
	}
	// Both corrupt blocks are still present.
	for _, c := range []cid.Cid{c1, c2} {
		if has, herr := blockstore.Blockstore(st.Blocks()).Has(ctx, c); herr != nil || !has {
			t.Errorf("block %s was deleted despite the fatal discovery write failure (has=%t err=%v)", c, has, herr)
		}
	}
}

// TestFsckRepairOutcomeWriteFailureStillRepairsAndReportsAll is the post-mutation
// half: once deletion has begun, an output failure cannot roll it back. Every
// deletion is attempted, every per-CID outcome is retained in the report, and the
// joined exit status carries the output error, the corruption status, and the
// delete failure together -- none masking another.
func TestFsckRepairOutcomeWriteFailureStillRepairsAndReportsAll(t *testing.T) {
	ctx := context.Background()
	st := openStore(t, t.TempDir())
	defer st.Close()

	c1 := cidUnder(t, cid.Raw, []byte("first corrupt block bytes"))
	c2 := cidUnder(t, cid.Raw, []byte("second corrupt block bytes"))
	putCorrupt(t, st, c1, []byte("corrupt one"))
	putCorrupt(t, st, c2, []byte("corrupt two"))
	ledger := catalog.NewLedger(st.KV())
	if err := ledger.Add(ctx, "h", "root", c1, false); err != nil {
		t.Fatalf("pinning c1: %v", err)
	}
	if err := ledger.Add(ctx, "h", "root", c2, false); err != nil {
		t.Fatalf("pinning c2: %v", err)
	}

	var buf bytes.Buffer
	// Discovery lines carry no "delete"; the mutation-phase outcome lines all do.
	out := &failOnContainsWriter{w: &buf, marker: "delete"}
	// c2's deletion also fails, so the joined status must carry three faults.
	failing := deleteFailBS{Blockstore: st.Blocks(), failCID: c2}

	report, outErr := fsckCore(ctx, failing, ledger, []string{"h"}, true, out)
	if outErr == nil {
		t.Fatal("outcome write failures did not produce an output error")
	}
	// Both deletions were attempted; both outcomes retained despite the write errors.
	if !containsCID(report.repaired, c1) {
		t.Errorf("c1's successful deletion was not retained: %v", report.repaired)
	}
	if len(report.deleteFailed) != 1 || !report.deleteFailed[0].cid.Equals(c2) {
		t.Errorf("c2's delete failure was not retained: %v", report.deleteFailed)
	}
	// The discovery inventory still made it out (its writes lacked the marker).
	if !strings.Contains(buf.String(), "corrupt\t"+c1.String()) || !strings.Contains(buf.String(), "corrupt\t"+c2.String()) {
		t.Errorf("discovery inventory was lost:\n%s", buf.String())
	}
	// The store reflects the repair: c1 gone, c2 present.
	if has, _ := blockstore.Blockstore(st.Blocks()).Has(ctx, c1); has {
		t.Error("c1 was not actually deleted")
	}
	if has, _ := blockstore.Blockstore(st.Blocks()).Has(ctx, c2); !has {
		t.Error("c2 was deleted despite the injected failure")
	}
	// The joined exit status (what the CLI returns) carries all three faults.
	joined := errors.Join(outErr, report.exitError())
	if !errors.Is(joined, errCorruptBlocksFound) {
		t.Errorf("joined status lost the corruption signal: %v", joined)
	}
	if !strings.Contains(joined.Error(), c2.String()) {
		t.Errorf("joined status did not name the un-deletable block: %v", joined)
	}
	if !strings.Contains(joined.Error(), "writing the repair report") {
		t.Errorf("joined status did not carry the output failure: %v", joined)
	}
}

// flushRecordingWriter delegates Write and fails Flush after a set number of
// successful flushes, for the output-flush boundary: a buffered writer accepts
// every Write and only fails at Flush.
type flushRecordingWriter struct {
	w              io.Writer
	failFlushAfter int // successful flushes allowed before failing
	flushes        int
}

func (f *flushRecordingWriter) Write(p []byte) (int, error) { return f.w.Write(p) }

func (f *flushRecordingWriter) Flush() error {
	f.flushes++
	if f.flushes > f.failFlushAfter {
		return errors.New("injected flush failure")
	}
	return nil
}

// TestFsckRepairDiscoveryFlushFailureIsFatalBeforeMutation is the follow-up blocker's
// pre-mutation half for buffered output: every Write succeeds but the flush fails,
// so the inventory is stuck in the buffer -- fsck --repair must return the error and
// delete NOTHING.
func TestFsckRepairDiscoveryFlushFailureIsFatalBeforeMutation(t *testing.T) {
	ctx := context.Background()
	st := openStore(t, t.TempDir())
	defer st.Close()

	c1 := cidUnder(t, cid.Raw, []byte("first corrupt block bytes"))
	c2 := cidUnder(t, cid.Raw, []byte("second corrupt block bytes"))
	putCorrupt(t, st, c1, []byte("corrupt one"))
	putCorrupt(t, st, c2, []byte("corrupt two"))
	led := catalog.NewLedger(st.KV())
	if err := led.Add(ctx, "h", "root", c1, false); err != nil {
		t.Fatalf("pinning c1: %v", err)
	}
	if err := led.Add(ctx, "h", "root", c2, false); err != nil {
		t.Fatalf("pinning c2: %v", err)
	}

	var buf bytes.Buffer
	w := &flushRecordingWriter{w: &buf, failFlushAfter: 0} // Write ok, first Flush fails
	report, err := fsckCore(ctx, st.Blocks(), led, []string{"h"}, true, w)
	if err == nil {
		t.Fatal("a discovery flush failure did not return an error")
	}
	if len(report.repaired) != 0 || len(report.deleteFailed) != 0 {
		t.Errorf("the store was mutated despite the discovery flush failure: repaired=%v failed=%v",
			report.repaired, report.deleteFailed)
	}
	for _, c := range []cid.Cid{c1, c2} {
		if has, herr := blockstore.Blockstore(st.Blocks()).Has(ctx, c); herr != nil || !has {
			t.Errorf("block %s was deleted despite the fatal discovery flush failure (has=%t err=%v)", c, has, herr)
		}
	}
	// The bytes were written even though the flush failed -- Write is not the boundary.
	if !strings.Contains(buf.String(), "corrupt\t"+c1.String()) {
		t.Errorf("discovery was not even written before the flush:\n%s", buf.String())
	}
}

// TestFsckRepairOutcomeFlushFailureStillRepairsAndErrors is the post-mutation half:
// the discovery flush succeeds, but the flush after the repair outcomes fails. Every
// repair still happens and the flush failure is joined into the status, never
// rolling a deletion back.
func TestFsckRepairOutcomeFlushFailureStillRepairsAndErrors(t *testing.T) {
	ctx := context.Background()
	st := openStore(t, t.TempDir())
	defer st.Close()

	c1 := cidUnder(t, cid.Raw, []byte("first corrupt block bytes"))
	c2 := cidUnder(t, cid.Raw, []byte("second corrupt block bytes"))
	putCorrupt(t, st, c1, []byte("corrupt one"))
	putCorrupt(t, st, c2, []byte("corrupt two"))
	led := catalog.NewLedger(st.KV())
	if err := led.Add(ctx, "h", "root", c1, false); err != nil {
		t.Fatalf("pinning c1: %v", err)
	}
	if err := led.Add(ctx, "h", "root", c2, false); err != nil {
		t.Fatalf("pinning c2: %v", err)
	}

	var buf bytes.Buffer
	w := &flushRecordingWriter{w: &buf, failFlushAfter: 1} // discovery flush ok, post-mutation flush fails
	report, outErr := fsckCore(ctx, st.Blocks(), led, []string{"h"}, true, w)
	if outErr == nil {
		t.Fatal("the post-mutation flush failure did not return an error")
	}
	if len(report.repaired) != 2 {
		t.Errorf("not all corrupt blocks were repaired past the flush failure: %v", report.repaired)
	}
	for _, c := range []cid.Cid{c1, c2} {
		if has, _ := blockstore.Blockstore(st.Blocks()).Has(ctx, c); has {
			t.Errorf("block %s was not deleted despite continuing past the flush failure", c)
		}
	}
	joined := errors.Join(outErr, report.exitError())
	if !errors.Is(joined, errCorruptBlocksFound) {
		t.Errorf("joined status lost the corruption signal: %v", joined)
	}
	if !strings.Contains(joined.Error(), "repair report") {
		t.Errorf("joined status dropped the flush failure: %v", joined)
	}
}

// TestFsckRepairErrorJoinsOutputCorruptAndMissing pins the outer join through the
// REAL fsck() entry (config + store it opens itself, as the CLI does): a
// post-mutation output failure alongside corrupt and missing findings must all
// survive together in the one returned error, never one masking another. It is the
// integration counterpart to the fsckCore-level outcome test, and the regression
// that fails if fsck() returns the output error early instead of joining it.
func TestFsckRepairErrorJoinsOutputCorruptAndMissing(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	corrupt := cidUnder(t, cid.Raw, []byte("corrupt block honest bytes"))
	missing := cidUnder(t, cid.Raw, []byte("never stored, only pinned"))
	func() {
		st := openStore(t, dir)
		defer st.Close()
		putCorrupt(t, st, corrupt, []byte("corrupt bytes on disk"))
		led := catalog.NewLedger(st.KV())
		if err := led.Add(ctx, "h", "root", corrupt, false); err != nil {
			t.Fatalf("pinning the corrupt block: %v", err)
		}
		if err := led.Add(ctx, "h", "root", missing, false); err != nil {
			t.Fatalf("pinning the missing block: %v", err)
		}
	}()

	cfg := &Config{Store: StoreConfig{Path: dir}, Heads: map[string]HeadConfig{"h": {}}}
	var buf bytes.Buffer
	// Post-mutation output failure: discovery lines carry no "delete", the outcome
	// lines all do, so the corrupt block is deleted and its outcome write fails.
	out := &failOnContainsWriter{w: &buf, marker: "delete"}

	err := fsck(ctx, cfg, true, "", out)
	if err == nil {
		t.Fatal("fsck returned nil despite an output failure and corrupt+missing findings")
	}
	if !errors.Is(err, errCorruptBlocksFound) {
		t.Errorf("the returned error masked the corruption status: %v", err)
	}
	if !errors.Is(err, errMissingBlocksFound) {
		t.Errorf("the returned error masked the missing status: %v", err)
	}
	if !strings.Contains(err.Error(), "repair report") {
		t.Errorf("the returned error dropped the output failure: %v", err)
	}
	if !strings.Contains(buf.String(), "corrupt\t"+corrupt.String()) || !strings.Contains(buf.String(), "missing\t"+missing.String()) {
		t.Errorf("discovery inventory was not written:\n%s", buf.String())
	}
}

// TestFsckExitJoinsAllSignals pins the production join helper fsck() uses: all four
// things an operator must act on -- the output error, the corruption status, the
// missing-pin status, and every delete failure WITH its CID -- must survive together
// in the single returned error. The earlier suite stayed green when report.deleteFailed
// was cleared just before the join because no test carried a delete failure through
// the production join; this one does.
func TestFsckExitJoinsAllSignals(t *testing.T) {
	c1 := cidUnder(t, cid.Raw, []byte("a corrupt block that was deleted"))
	c2 := cidUnder(t, cid.Raw, []byte("a corrupt block whose delete failed"))
	m1 := cidUnder(t, cid.Raw, []byte("a missing pinned block"))
	// Retain the delete cause and the output error as sentinels, so the join can be
	// checked by errors.Is identity, not only by the text it renders: the join must
	// preserve the concrete errors an operator's own errors.Is would match, not merely
	// splice their messages together.
	deleteCause := errors.New("permission denied")
	outErr := errors.New("bloard: fsck writing the repair report (repairs were still applied): injected write failure")
	report := fsckReport{
		corrupt:      []cid.Cid{c1, c2},
		missing:      []cid.Cid{m1},
		repaired:     []cid.Cid{c1},
		deleteFailed: []deleteFailure{{cid: c2, err: deleteCause}},
	}

	joined := fsckExit(report, outErr)
	if joined == nil {
		t.Fatal("fsckExit returned nil despite an output error and findings")
	}
	if !errors.Is(joined, errCorruptBlocksFound) {
		t.Errorf("join dropped the corruption status: %v", joined)
	}
	if !errors.Is(joined, errMissingBlocksFound) {
		t.Errorf("join dropped the missing status: %v", joined)
	}
	// The output error and the delete cause survive by identity, not just by text.
	if !errors.Is(joined, outErr) {
		t.Errorf("join dropped the output error's identity: %v", joined)
	}
	if !errors.Is(joined, deleteCause) {
		t.Errorf("join dropped the delete failure's cause identity: %v", joined)
	}
	if !strings.Contains(joined.Error(), "injected write failure") {
		t.Errorf("join dropped the output error: %v", joined)
	}
	// The delete failure survives with its CID and the wrapper text too -- the exact
	// signal the injected mutation (clear report.deleteFailed before the join) erases.
	if !strings.Contains(joined.Error(), c2.String()) || !strings.Contains(joined.Error(), "could not delete") {
		t.Errorf("join dropped the delete failure or its CID: %v", joined)
	}
}

// putCorrupt stores bad bytes under c's key (NewBlockWithCid does not verify), the
// on-disk corruption of the safety boundary.
func putCorrupt(t *testing.T, st *store.Store, c cid.Cid, bad []byte) {
	t.Helper()
	blk, err := blocks.NewBlockWithCid(bad, c)
	if err != nil {
		t.Fatalf("framing corrupt block: %v", err)
	}
	if err := st.Blocks().Put(context.Background(), blk); err != nil {
		t.Fatalf("storing corrupt block: %v", err)
	}
}

func containsCID(list []cid.Cid, c cid.Cid) bool {
	for _, x := range list {
		if x.Equals(c) {
			return true
		}
	}
	return false
}
