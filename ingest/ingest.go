// Package ingest implements the blob intake pipeline of spec 7.2's
// POST /bloar/v1/blobs, minus the HTTP: verify blobs, derive their identity,
// store their blocks, and upsert the blob catalog.
//
// The server phase owns everything this package deliberately does not do:
// framing, authentication, the max_put_blobs count limit (spec 7.2, default
// 64), and mapping the errors here onto status codes. PutBlobs takes a whole
// body of any length and will happily ingest a thousand blobs; bounding N is
// the caller's job, and must happen before the body is read into memory to be
// worth anything.
package ingest

import (
	"context"
	"crypto/sha256"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/crypto/kzg4844"
	"github.com/ipfs/boxo/blockstore"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"

	"github.com/blobarchive/bloar/catalog"
	"github.com/blobarchive/bloar/metrics"
	"github.com/blobarchive/bloar/schema"
)

// Gate is the GC exclusion of spec 9, as ingest needs it. *pinning.Gate is the
// implementation; this is an interface so that ingest does not import pinning
// (pinning is the layer above: it reads what this writes).
//
// See server.Gate for the general argument. The ingest-specific one is that a
// blob's block and its staging pin must both exist before PutBlobs returns, and
// a GC admitted between those two writes would sweep the block a moment before
// the pin that was about to retain it landed -- leaving a ledger row naming a
// block that is gone, and an indexer holding a 200 for a blob that no longer
// exists.
type Gate interface {
	Enter()
	Leave()
}

// noGate is what an unconfigured Ingester puts under.
type noGate struct{}

func (noGate) Enter() {}
func (noGate) Leave() {}

// Staging pins freshly ingested blobs so that a GC between the blobs POST and
// the refs POST cannot sweep them (spec 9, window (a)). pinning.Staging is the
// implementation.
//
// This is half of closing that window; server.Staging is the other. A blob is
// unreachable from any head until the refs naming it are applied, and the two
// are separate requests (spec 7.2), so the gate alone cannot help: it keeps a
// GC from starting *during* a put, but the gap it must survive outlives the
// request. A pin taken here does survive it, and is dropped when the refs land.
type Staging interface {
	// Pin records a direct staging pin per CID, with an expiry, and returns
	// only once they are durable.
	Pin(ctx context.Context, cids []cid.Cid) error
}

// Config is the node-local state an Ingester writes to.
type Config struct {
	// Blocks is the blockstore blob blocks land in. Required.
	Blocks blockstore.Blockstore
	// Catalog is the blob catalog of spec 6.1. Required.
	Catalog *catalog.Catalog
	// Gate excludes GC for the whole of a put (spec 9). Optional; nil is a
	// stack with no GC in it.
	Gate Gate
	// Staging pins what a put accepts until its refs land (spec 9, window (a)).
	// Optional, and a node with a GC wants it: without it, a GC that runs
	// between a blobs POST and the refs POST that names them sweeps the blobs,
	// and the indexer has to notice the 409 and re-put.
	Staging Staging
	// Metrics instruments the pipeline. Optional; nil records nothing.
	Metrics *metrics.Metrics
	// VerifyConcurrency bounds how many of a batch's blobs pass 1 verifies at
	// once. Zero is a default derived from GOMAXPROCS (see verifyConcurrency);
	// 1 is serial. The default is deliberately small: each blob's commitment
	// already fans its own MSM across every core, so a few concurrent verifies
	// saturate the machine and more only adds scheduler contention.
	VerifyConcurrency int
}

func (c Config) check() error {
	if c.Blocks == nil {
		return fmt.Errorf("ingest: Config.Blocks must not be nil")
	}
	if c.Catalog == nil {
		return fmt.Errorf("ingest: Config.Catalog must not be nil")
	}
	return nil
}

// Ingester is the blob intake pipeline. It is safe for concurrent use: every
// write it makes is idempotent and content-addressed, so two callers putting
// the same blob race only to write identical bytes.
type Ingester struct {
	cfg Config
}

// New returns an Ingester over cfg.
func New(cfg Config) (*Ingester, error) {
	if err := cfg.check(); err != nil {
		return nil, err
	}
	if cfg.Gate == nil {
		cfg.Gate = noGate{}
	}
	return &Ingester{cfg: cfg}, nil
}

// PutResult is one ingested blob's derived identity, which is all spec 7.2
// returns: the server accepts no metadata and derives everything.
type PutResult struct {
	VH  schema.VersionedHash
	CID cid.Cid
}

// ValidationError reports a body the ingest pipeline refuses. Spec 7.2 maps
// every one of these to HTTP 400.
//
// Index is the offending blob's position in the body, or -1 when the complaint
// is about the body as a whole and no single blob is to blame.
type ValidationError struct {
	Index  int
	Reason string
	Err    error
}

func (e *ValidationError) Error() string {
	var where string
	if e.Index >= 0 {
		where = fmt.Sprintf(" at blob %d", e.Index)
	}
	if e.Err != nil {
		return fmt.Sprintf("ingest: %s%s: %v", e.Reason, where, e.Err)
	}
	return fmt.Sprintf("ingest: %s%s", e.Reason, where)
}

func (e *ValidationError) Unwrap() error { return e.Err }

// PutBlobs ingests the concatenation of N blobs, per spec 7.2. It returns one
// PutResult per blob, in body order.
//
// The whole batch is verified before anything is written, so a body carrying
// even one non-canonical blob is rejected having stored nothing: a *ValidationError
// naming the offending index. Verification is where a put fails in practice,
// and a caller retrying a corrected body should not have to reason about which
// prefix of the previous attempt landed.
//
// Per blob, the block is made durable before its catalog entry. The orders
// fail differently and only one of them is safe. A block with no catalog entry
// is invisible until a rebuild finds it, and costs disk. A catalog entry with
// no block is a dangling reference that apply_refs must catch, which is why
// apply_refs double-checks blockstore presence (spec 5.1 step 4) -- but that
// check exists for GC's sake, and this path has no business manufacturing more
// work for it.
//
// # Staging pins and the ingest window
//
// The whole call runs inside the gate, so no GC starts in the middle of it. The
// window that matters is the one after it returns: the blobs are durable and
// still unreachable, because the refs that name them are a second request (spec
// 7.2), and a GC in that gap used to sweep them (spec 9's known window (a)).
//
// The staging pins close it. Every accepted blob gets a direct ledger pin under
// the reserved staging head before this returns, so the first GC that can
// possibly run after the call already has the blobs in its mark set -- the only
// dangerous GC being one that starts after this returns and before the refs
// land, since the gate excludes any that would start during either request. The
// pins are dropped when the refs land (server.Staging) and expire on their own
// if they never do (ingest.staging_ttl), so an abandoned put costs a day of disk
// rather than forever.
//
// The pins are taken after the blocks are written, and the order is not
// interchangeable. A pin is a claim that a block is retained, and the ledger is
// the pin state rather than a record of one kept elsewhere (see the pinning
// package): a row written first would be that claim made about a block that does
// not exist yet, and a crash in between would leave it made about a block that
// never will. GC happens to tolerate such a row today -- a direct pin is marked
// without reading the block, and the sweep never meets a block that is not there
// -- but that is an accident of the mark's shape, not a property to build on,
// and `bloar_pins{head="_staging"}` would be counting blobs this node does not
// have.
//
// Writing the block first means the crash leaves the opposite: a durable blob
// with no pin, which is unreachable, gets swept, and produces a 409 at the refs
// POST that the indexer answers by re-putting. That is exactly the pre-staging
// behaviour of spec 9's window (a) -- so the failure mode of this mechanism is
// the thing it replaced, which is the right way round for it to fail.
func (i *Ingester) PutBlobs(ctx context.Context, body []byte) ([]PutResult, error) {
	i.cfg.Gate.Enter()
	defer i.cfg.Gate.Leave()

	if len(body)%schema.BlobSize != 0 {
		i.cfg.Metrics.IngestReject(metrics.RejectFraming)
		return nil, &ValidationError{
			Index:  -1,
			Reason: fmt.Sprintf("body of %d bytes is not a whole number of %d-byte blobs", len(body), schema.BlobSize),
		}
	}
	n := len(body) / schema.BlobSize

	// Pass 1: verify and derive. Nothing is written here. The blobs are
	// independent -- each derives its identity from its own bytes and touches no
	// shared state -- so they verify concurrently, bounded by verifyConcurrency,
	// with each worker owning the out slot at its index.
	out := make([]PutResult, n)
	if verr := i.verify(body, n, out); verr != nil {
		i.cfg.Metrics.IngestReject(metrics.RejectKZG)
		return nil, verr
	}

	// Pass 2: store. Both writes are idempotent, so a retry after a partial
	// failure re-does the prefix that already landed rather than conflicting
	// with it.
	for k := range n {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		blk, err := blocks.NewBlockWithCid(blobAt(body, k), out[k].CID)
		if err != nil {
			i.cfg.Metrics.IngestReject(metrics.RejectStore)
			return nil, fmt.Errorf("ingest: framing blob %d as block %s: %w", k, out[k].CID, err)
		}
		start := time.Now()
		if err := i.cfg.Blocks.Put(ctx, blk); err != nil {
			i.cfg.Metrics.IngestReject(metrics.RejectStore)
			return nil, fmt.Errorf("ingest: storing blob %d as block %s: %w", k, out[k].CID, err)
		}
		i.cfg.Metrics.StorePut(time.Since(start))
		if err := i.cfg.Catalog.Put(ctx, out[k].VH, out[k].CID); err != nil {
			i.cfg.Metrics.IngestReject(metrics.RejectStore)
			return nil, fmt.Errorf("ingest: cataloguing blob %d: %w", k, err)
		}
	}

	// Pass 3: stage. One batched call rather than a pin per blob in the loop
	// above: the rows are independent of each other and of the blocks, and the
	// only ordering that matters is that every block is durable first.
	if i.cfg.Staging != nil && n > 0 {
		cids := make([]cid.Cid, n)
		for k := range n {
			cids[k] = out[k].CID
		}
		if err := i.cfg.Staging.Pin(ctx, cids); err != nil {
			// Fatal to the request, and deliberately so. The blobs are stored;
			// what failed is the promise that they will still be there when the
			// refs arrive. Answering 200 would hand the indexer a receipt for a
			// put that a GC may erase, and the indexer would find out at the
			// refs POST -- which is exactly the failure the staging pins exist
			// to remove. A 500 makes it re-put now, into a store where the
			// blocks already are, which is cheap and idempotent.
			i.cfg.Metrics.IngestReject(metrics.RejectStore)
			return nil, fmt.Errorf("ingest: staging %d blobs: %w", n, err)
		}
	}

	i.cfg.Metrics.Ingested(n, len(body))
	return out, nil
}

// maxVerifyWorkers caps the default cross-blob verify fan-out. A single
// BlobToCommitment already parallelizes its MSM across the machine (measured
// bursting to 2200-2700% CPU on one call), so concurrent calls contend for the
// same cores rather than adding independent throughput. A small cap is enough to
// hide the serial gaps between those fan-outs -- the sha256, the CID hash, the
// scheduling -- while leaving each call room to spread out; a larger one just
// oversubscribes and the scheduler pays for it. Raise ingest.verify_concurrency
// past this only with a measurement showing idle cores under ingest.
const maxVerifyWorkers = 4

// verify runs pass 1 across the body's n blobs, writing each blob's derived
// identity into its slot of out. It returns the *ValidationError of the
// lowest-indexed blob that fails, or nil.
//
// The lowest index and not the first to fail in wall time: spec 7.2 rejects a
// body by the position of its offending blob, and a parallel verify must report
// the index a serial one would -- byte for byte the same 400 -- whatever order
// the failures happen to land in. Every blob is verified rather than cancelling
// on the first failure: a rejected batch ends the request, so there is no
// throughput to protect by stopping early, and running to completion makes the
// lowest-index guarantee a plain scan of the per-index error slots rather than a
// race to cancel before a lower index can start.
func (i *Ingester) verify(body []byte, n int, out []PutResult) *ValidationError {
	w := i.verifyConcurrency(n)
	if w <= 1 {
		// The single-worker path, kept as the loop it is so the common case pays
		// nothing for the fan-out machinery.
		for k := range n {
			if verr := i.verifyBlob(body, k, out); verr != nil {
				return verr
			}
		}
		return nil
	}

	errs := make([]*ValidationError, n)
	var next atomic.Int64
	var wg sync.WaitGroup
	wg.Add(w)
	for range w {
		go func() {
			defer wg.Done()
			for {
				k := int(next.Add(1)) - 1
				if k >= n {
					return
				}
				errs[k] = i.verifyBlob(body, k, out)
			}
		}()
	}
	wg.Wait()

	for k := range n {
		if errs[k] != nil {
			return errs[k]
		}
	}
	return nil
}

// verifyConcurrency is how many of a batch's n blobs pass 1 verifies at once.
// A configured value wins; zero derives min(GOMAXPROCS, maxVerifyWorkers). The
// clamp to n keeps a small batch from starting idle workers.
func (i *Ingester) verifyConcurrency(n int) int {
	w := i.cfg.VerifyConcurrency
	if w <= 0 {
		w = min(runtime.GOMAXPROCS(0), maxVerifyWorkers)
	}
	return min(w, n)
}

// verifyBlob derives blob k's identity into out[k], or returns the
// ValidationError naming k. It is pass 1 for one blob and the unit the workers
// parallelize over; the per-call KZG timing is recorded here so the histogram
// stays per-call whether the calls run serially or concurrently.
func (i *Ingester) verifyBlob(body []byte, k int, out []PutResult) *ValidationError {
	raw := blobAt(body, k)
	start := time.Now()
	vh, err := VersionedHash(raw)
	i.cfg.Metrics.KZGVerify(time.Since(start))
	if err != nil {
		return &ValidationError{Index: k, Reason: "blob is not a valid KZG blob", Err: err}
	}
	c, err := schema.BlobCID(raw)
	if err != nil {
		return &ValidationError{Index: k, Reason: "computing blob CID", Err: err}
	}
	out[k] = PutResult{VH: vh, CID: c}
	return nil
}

// blobAt returns blob k of a body PutBlobs has already length-checked. It is a
// window into body, not a copy: the blob is 128 KiB and both the KZG commitment
// and the CID only read it.
func blobAt(body []byte, k int) []byte {
	return body[k*schema.BlobSize : (k+1)*schema.BlobSize]
}

// VersionedHash computes 0x01 || sha256(kzg_commitment(blob))[1:] (spec 1). It
// is the identity of a blob as the execution layer sees it, and the only thing
// tying the DAG's refs to blob bytes: everything else in bloar is content
// addressing, which cannot tell a blob from any other 128 KiB of bytes.
//
// A blob whose bytes are not canonical BLS12-381 field elements has no
// commitment and so no versioned hash; that is the error returned.
func VersionedHash(blob []byte) (schema.VersionedHash, error) {
	if len(blob) != schema.BlobSize {
		return schema.VersionedHash{}, fmt.Errorf("ingest: blob must be exactly %d bytes, got %d", schema.BlobSize, len(blob))
	}
	// A pointer into the caller's bytes: kzg4844.Blob is exactly BlobSize, and
	// BlobToCommitment does not retain or mutate it.
	commitment, err := kzg4844.BlobToCommitment((*kzg4844.Blob)(blob))
	if err != nil {
		return schema.VersionedHash{}, fmt.Errorf("ingest: computing KZG commitment: %w", err)
	}
	return schema.VersionedHash(kzg4844.CalcBlobHashV1(sha256.New(), &commitment)), nil
}

// VersionedHashFromCommitment computes 0x01 || sha256(commitment)[1:] (spec 1)
// from a KZG commitment already in hand, without the blob bytes.
//
// It is the block-derived path of spec 10.1's anchored mode: a beacon block's
// blob_kzg_commitments state a slot's blobs, and the vh a chain sees is derived
// from the commitment, not the blob. There are no bytes to recompute from here
// -- the whole point is that the block feed knows what a slot must contain before
// any blob source is asked -- so this takes the commitment directly. The blob
// bytes a source then serves are checked against this vh with VersionedHash.
func VersionedHashFromCommitment(commitment [48]byte) schema.VersionedHash {
	c := kzg4844.Commitment(commitment)
	return schema.VersionedHash(kzg4844.CalcBlobHashV1(sha256.New(), &c))
}
