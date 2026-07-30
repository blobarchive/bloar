package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"

	"github.com/blobarchive/bloar/store"
)

// putBlock writes one block whose bytes come from a file and whose identity is an
// explicit CID. It is the raw-blob repair path: the writer's half
// of recovering a corrupt block, after `bloard fsck --repair` has deleted it. The
// CID is an input, not derived, and the file's bytes MUST reproduce exactly that
// CID under its own prefix and codec -- put-block fails before writing anything
// otherwise, so it can only ever add a block that is what it claims to be.
//
// It is offline: it takes the store's lock, so the daemon must be stopped. flatfs
// skips a Put whose key already exists, so a block cannot be replaced in place --
// the corrupt one must be deleted first (fsck --repair), which is what turns the
// key into the miss this Put fills.
func putBlock(ctx context.Context, cfg *Config, cidStr, file string, out io.Writer) error {
	log := newLogger()

	c, err := cid.Decode(cidStr)
	if err != nil {
		return fmt.Errorf("bloard: --cid %q is not a valid CID: %w", cidStr, err)
	}
	data, err := os.ReadFile(file)
	if err != nil {
		return fmt.Errorf("bloard: reading %s: %w", file, err)
	}

	locked, err := store.Locked(cfg.Store.Path)
	if err != nil {
		return err
	}
	if locked {
		return fmt.Errorf("bloard: the store at %s is locked; a daemon (or another tool) is holding it. put-block needs "+
			"exclusive access -- stop the daemon and retry", cfg.Store.Path)
	}

	st, err := store.Open(cfg.Store.Path, store.WithPebbleLogger(pebbleLogger{log: log.With("component", "pebble")}))
	if err != nil {
		return err
	}
	defer func() {
		if err := st.Close(); err != nil {
			log.Error("closing store", "err", err)
		}
	}()

	return runPutBlock(ctx, st, c, data, out)
}

// runPutBlock is the testable core of putBlock: it validates data against c and,
// if they match and no block is already stored under c, writes it. It takes an
// already-open store so a test needs no config file.
func runPutBlock(ctx context.Context, st *store.Store, c cid.Cid, data []byte, out io.Writer) error {
	// Validate first, before touching the store: the file's bytes must reproduce
	// exactly the stated CID. Re-summing under c's own prefix takes its version,
	// codec, hash function and length, so a mismatch is a genuine content
	// disagreement and not a codec quibble. Fail here and nothing is written.
	got, err := c.Prefix().Sum(data)
	if err != nil {
		return fmt.Errorf("bloard: hashing the input for %s: %w", c, err)
	}
	if !got.Equals(c) {
		return fmt.Errorf("bloard: refusing to write: the file's %d bytes hash to %s, not the stated CID %s", len(data), got, c)
	}

	// The bytes are correct for the CID. Refuse to no-op over an existing key: a
	// present-and-valid block is nothing to do, and a present-but-corrupt one
	// would be silently skipped by flatfs, leaving the corruption in place -- so
	// read through the validating store and tell the operator to delete it first.
	existing, err := st.Blocks().Get(ctx, c)
	switch {
	case err == nil:
		_ = existing
		fmt.Fprintf(out, "%s already present and valid; nothing to do\n", c)
		return nil
	case errors.Is(err, store.ErrCorruptBlock):
		return fmt.Errorf("bloard: a block already exists under %s but its stored bytes are corrupt; delete it first with "+
			"`bloard fsck --repair`, then re-run put-block", c)
	case ipld.IsNotFound(err):
		// Absent: write it.
	default:
		return fmt.Errorf("bloard: checking the store for %s: %w", c, err)
	}

	blk, err := blocks.NewBlockWithCid(data, c)
	if err != nil {
		return fmt.Errorf("bloard: framing block %s: %w", c, err)
	}
	if err := st.Blocks().Put(ctx, blk); err != nil {
		return fmt.Errorf("bloard: writing block %s: %w", c, err)
	}
	fmt.Fprintf(out, "wrote %s (%d bytes)\n", c, len(data))
	return nil
}
