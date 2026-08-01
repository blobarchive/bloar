package ingest

import (
	"context"
	"fmt"

	format "github.com/ipfs/go-ipld-format"

	"github.com/blobarchive/bloar/catalog"
	"github.com/blobarchive/bloar/schema"
)

// rebuildBatch is how many catalog entries accumulate before a synced write.
// The walk is dominated by reading 128 KiB blocks and committing to them, so
// this only needs to be big enough that fsyncs are not the story.
const rebuildBatch = 512

// RebuildStats reports what a Rebuild walk saw.
//
// Blobs counts blocks that are real blobs; Upserted counts the catalog entries
// that were actually written, which is fewer whenever the catalog already held
// the right answer. Rebuilding an intact catalog is therefore Upserted == 0,
// which is the useful thing for an operator to see.
type RebuildStats struct {
	// Scanned is every block visited.
	Scanned int64
	// Blobs is blocks that are BlobSize bytes and carry a valid KZG commitment.
	Blobs int64
	// Upserted is catalog entries written.
	Upserted int64
	// Skipped is blocks that are not blobs: Scanned - Blobs.
	Skipped int64
}

// Rebuild regenerates the blob catalog (spec 6.1) from the blockstore: it walks
// every block, and for each one that is exactly BlobSize bytes it attempts a KZG
// commitment, deriving vh and upserting vh -> CID when that succeeds. Blocks
// that fail -- index nodes, and any other 128 KiB of bytes that are not
// canonical field elements -- are skipped, because the commitment is the only
// thing that can tell a blob from bytes that merely look like one.
//
// This is the slow path, by construction. A full flatfs walk reads every block
// in the store from its own file and pays a KZG commitment (milliseconds) for
// each blob-sized one. On a real archive that is hours. It is meant to be run
// offline, by an operator who has decided the catalog is wrong, and it does not
// pretend to be resumable or concurrency-safe against a live writer.
//
// Rebuild regenerates the catalog only. The pin ledger (spec 6.2) is not
// touched and is not rebuildable this way: it records intent, not content, and
// is reconciled against the desired pin set by the machinery of spec 9.
//
// Rebuild only ever upserts, so it repairs a catalog that is missing entries or
// has wrong ones, but cannot retract an entry for a blob the store no longer
// has. Callers that want the catalog to say nothing but what the blockstore
// holds should Catalog.Clear first.
func Rebuild(ctx context.Context, cfg Config) (RebuildStats, error) {
	var stats RebuildStats
	if err := cfg.check(); err != nil {
		return stats, err
	}

	keys, err := cfg.Blocks.AllKeysChan(ctx)
	if err != nil {
		return stats, fmt.Errorf("ingest: walking the blockstore: %w", err)
	}

	pending := make([]catalog.Entry, 0, rebuildBatch)
	flush := func() error {
		if err := cfg.Catalog.PutBatch(ctx, pending); err != nil {
			return err
		}
		stats.Upserted += int64(len(pending))
		pending = pending[:0]
		return nil
	}

	for c := range keys {
		if err := ctx.Err(); err != nil {
			return stats, err
		}
		stats.Scanned++

		blk, err := cfg.Blocks.Get(ctx, c)
		if format.IsNotFound(err) {
			// Raced a deletion, or the walk saw a key whose block is gone.
			// Nothing to catalog, and nothing worth failing the walk over.
			stats.Skipped++
			continue
		}
		if err != nil {
			return stats, fmt.Errorf("ingest: reading block %s: %w", c, err)
		}

		data := blk.RawData()
		if len(data) != schema.BlobSize {
			stats.Skipped++
			continue
		}
		vh, err := VersionedHash(data)
		if err != nil {
			stats.Skipped++
			continue
		}
		stats.Blobs++

		// The CID is recomputed rather than taken from the walk: AllKeysChan
		// reports keys as raw-codec CIDs of the multihash, which happens to be
		// right for a blob, but the derivation in schema is what defines a blob
		// block's identity and this walk sees non-blobs too.
		blob, err := schema.BlobCID(data)
		if err != nil {
			return stats, fmt.Errorf("ingest: computing CID for block %s: %w", c, err)
		}

		if have, ok, err := cfg.Catalog.ResolveBlob(ctx, vh); err != nil {
			return stats, err
		} else if ok && have.Equals(blob) {
			continue
		}
		pending = append(pending, catalog.Entry{VH: vh, Blob: blob})
		if len(pending) == rebuildBatch {
			if err := flush(); err != nil {
				return stats, err
			}
		}
	}
	if err := flush(); err != nil {
		return stats, err
	}
	return stats, nil
}
