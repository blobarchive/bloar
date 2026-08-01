package main

import (
	"context"
	"fmt"
	"io"

	"github.com/blobarchive/bloar/catalog"
	"github.com/blobarchive/bloar/ingest"
	"github.com/blobarchive/bloar/store"
)

// rebuild regenerates the blob catalog from the blockstore (spec 6, 6.1).
//
// This is the subcommand spec 6 requires to exist, and it rebuilds ONLY the
// catalog (the `c` prefix): the map from a versioned hash to its blob block,
// which is a pure derived cache of the blocks on disk. It is offline by
// construction -- it takes the store's lock, so the daemon must be stopped --
// and slow, because it reads every block in the store and pays a KZG commitment
// for each blob-sized one.
//
// It does not restore the rest of the KV. Head roots and manifest tips (server),
// the writer IPNS sequence (p2p), and a follower's checkpoint and anti-replay
// floors (follow) are current-selection, monotonic, or anti-replay facts that
// are not re-derivable from an unordered blockstore -- a root is a CID, and
// nothing in a store full of Head blocks says which one is current (see
// server.RootStore). The pin ledger is not rebuilt here either: reconciliation
// owns it (and its staging leases are time-bearing, spec 9).
func rebuild(ctx context.Context, cfg *Config, clear bool, out io.Writer) error {
	log := newLogger()

	st, err := store.Open(cfg.Store.Path, store.WithPebbleLogger(pebbleLogger{log: log.With("component", "pebble")}))
	if err != nil {
		return err
	}
	defer func() {
		if err := st.Close(); err != nil {
			log.Error("closing store", "err", err)
		}
	}()

	cat := catalog.New(st.KV())
	if clear {
		// Rebuild only upserts, so it can repair a wrong entry but never
		// retract one for a blob the store no longer holds. Clearing first is
		// what makes the result say nothing but what the blockstore has.
		log.Info("clearing the blob catalog")
		if err := cat.Clear(ctx); err != nil {
			return err
		}
	}

	log.Info("rebuilding the blob catalog", "store", st.Path())
	stats, err := ingest.Rebuild(ctx, ingest.Config{Blocks: st.Blocks(), Catalog: cat})
	// Stats are printed even on failure: a walk that died halfway still says
	// how far it got, and an operator who has waited hours for it should not
	// have to guess.
	printStats(out, stats)
	if err != nil {
		return err
	}
	return nil
}

// printStats reports a walk. Upserted == 0 over a non-zero Blobs count is an
// intact catalog, which is the answer an operator running this on a hunch wants
// to see.
func printStats(out io.Writer, stats ingest.RebuildStats) {
	fmt.Fprintf(out, "scanned:  %d blocks\n", stats.Scanned)
	fmt.Fprintf(out, "blobs:    %d\n", stats.Blobs)
	fmt.Fprintf(out, "skipped:  %d (not blobs)\n", stats.Skipped)
	fmt.Fprintf(out, "upserted: %d catalog entries\n", stats.Upserted)
}
