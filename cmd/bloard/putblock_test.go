package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
)

func TestPutBlockRejectsWrongBytes(t *testing.T) {
	ctx := context.Background()
	st := openStore(t, t.TempDir())
	defer st.Close()

	honest := []byte("the bytes this CID commits to")
	c := cidUnder(t, cid.Raw, honest)

	var out bytes.Buffer
	err := runPutBlock(ctx, st, c, []byte("not those bytes"), &out)
	if err == nil {
		t.Fatal("put-block wrote a block whose bytes do not reproduce the stated CID")
	}
	if !strings.Contains(err.Error(), "hash to") {
		t.Errorf("error did not explain the hash mismatch: %v", err)
	}
	// Nothing was written: the validation is before the store is touched.
	if has, err := st.Blocks().Has(ctx, c); err != nil || has {
		t.Errorf("a block was stored despite the mismatch: has=%t err=%v", has, err)
	}
}

func TestPutBlockStoresRightBytes(t *testing.T) {
	ctx := context.Background()
	st := openStore(t, t.TempDir())
	defer st.Close()

	honest := []byte("exactly the bytes the CID commits to")
	c := cidUnder(t, cid.Raw, honest)

	var out bytes.Buffer
	if err := runPutBlock(ctx, st, c, honest, &out); err != nil {
		t.Fatalf("put-block on matching bytes: %v", err)
	}
	if !strings.Contains(out.String(), "wrote "+c.String()) {
		t.Errorf("put-block did not report the write:\n%s", out.String())
	}
	// And it reads back through the validating store, which is the proof the bytes
	// stored are the bytes the CID commits to.
	got, err := st.Blocks().Get(ctx, c)
	if err != nil {
		t.Fatalf("reading back the block: %v", err)
	}
	if !bytes.Equal(got.RawData(), honest) {
		t.Errorf("read back %q, want %q", got.RawData(), honest)
	}
}

func TestPutBlockRefusesCorruptExisting(t *testing.T) {
	ctx := context.Background()
	st := openStore(t, t.TempDir())
	defer st.Close()

	honest := []byte("what the CID commits to")
	c := cidUnder(t, cid.Raw, honest)
	// A corrupt block already occupies the key; flatfs would skip an ordinary Put,
	// so put-block must refuse and point at fsck --repair rather than silently
	// no-op over the corruption.
	corruptBlk, err := blocks.NewBlockWithCid([]byte("corrupt bytes under the key"), c)
	if err != nil {
		t.Fatalf("framing corrupt block: %v", err)
	}
	if err := st.Blocks().Put(ctx, corruptBlk); err != nil {
		t.Fatalf("storing corrupt block: %v", err)
	}

	var out bytes.Buffer
	err = runPutBlock(ctx, st, c, honest, &out)
	if err == nil {
		t.Fatal("put-block no-oped over an existing corrupt block")
	}
	if !strings.Contains(err.Error(), "fsck --repair") {
		t.Errorf("error did not point at the delete-first recovery: %v", err)
	}
}

// TestPutBlockRoundTripAfterRepair is the full raw-blob recovery: a corrupt block
// is quarantined by fsck --repair (deleted, turned into a miss) and then refilled
// by put-block with the correct bytes, after which it reads again.
func TestPutBlockRoundTripAfterRepair(t *testing.T) {
	ctx := context.Background()
	f := newFsckFixture(t, true)

	// Delete the corrupt blob (offline, exclusive).
	var out bytes.Buffer
	if err := fsck(ctx, f.config(), true, "", &out); err == nil {
		t.Fatal("fsck --repair reported no corruption to delete")
	}
	if f.has(t, f.blob) {
		t.Fatal("fsck --repair did not delete the corrupt blob")
	}

	// Refill it with the correct bytes.
	st := openStore(t, f.dir)
	defer st.Close()
	if err := runPutBlock(ctx, st, f.blob, f.honest, &out); err != nil {
		t.Fatalf("put-block refilling the deleted blob: %v", err)
	}

	// It reads again, and validates: the store serves the honest bytes.
	got, err := st.Blocks().Get(ctx, f.blob)
	if err != nil {
		t.Fatalf("reading the refilled blob: %v", err)
	}
	if !bytes.Equal(got.RawData(), f.honest) {
		t.Error("the refilled blob did not read back as the honest bytes")
	}
	// And a fresh fsck is clean.
	var out2 bytes.Buffer
	if _, err := runFsck(ctx, st, []string{"h"}, false, &out2); err != nil {
		t.Fatalf("fsck after repair+refill: %v", err)
	}
	if !strings.Contains(out2.String(), "corrupt: 0") {
		t.Errorf("store is not clean after repair+refill:\n%s", out2.String())
	}
}

// TestPutBlockRefusesWhileLocked is the writer half of the exclusive-ownership
// guard: like fsck --repair, put-block must not write into a store a daemon holds.
func TestPutBlockRefusesWhileLocked(t *testing.T) {
	dir := t.TempDir()
	honest := []byte("bytes for a locked store")
	c := cidUnder(t, cid.Raw, honest)

	held := openStore(t, dir)
	defer held.Close()

	tmp := filepath.Join(t.TempDir(), "blob")
	if err := os.WriteFile(tmp, honest, 0o644); err != nil {
		t.Fatalf("writing input file: %v", err)
	}
	var out bytes.Buffer
	err := putBlock(context.Background(), &Config{Store: StoreConfig{Path: dir}}, c.String(), tmp, &out)
	if err == nil || !strings.Contains(err.Error(), "locked") {
		t.Fatalf("put-block while locked: got %v, want a lock refusal", err)
	}
}
