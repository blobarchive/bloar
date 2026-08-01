package server_test

// Benchmarks for always-on read validation (every local block Get re-hashed
// against its CID), OFF (a plain flatfs blockstore) vs ON (store.Validating
// over it).
//
// # One store, one flatfs instance, one shared fixture
//
// Every benchmark here shares a single on-disk store built once (buildBenchFixture,
// behind sync.Once): ONE live flatfs instance and one real on-disk Pebble,
// constructed by hand the way store.Open constructs them -- because store.Open only
// hands out the validating wrapper, and OFF must be the *same* flatfs instance
// unwrapped, not a second concurrent handle (flatfs does not coordinate sync
// state, op-maps, or disk-usage accounting across handles). OFF is the plain
// blockstore over that instance, ON is store.Validating over the SAME instance, so
// every paired comparison reads the identical bytes off the identical inodes. The
// fixture ingests 64 blobs and commits a covered 64-window head (a depth-3
// directory over 64 sealed Segments); RawGet reads one of its blobs, IndexNodeGet
// reads one of its Segment nodes, the endpoint reads serve one of its slots, GC
// marks its pinned closure, and ApplyRefs replays its 64 blobs into fresh heads.
// The production store.Open path is untouched.
//
// The OFF-vs-ON delta on identical hardware and store is the decision-relevant
// quantity; absolute latency is hardware/filesystem specific. Set
// BLOAR_BENCH_STORE_DIR to a real target-host disk (the test temp directory is
// tmpfs in some environments), run several counts, and retain the raw benchmark
// output with that deployment's evidence rather than in this source tree.

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/go-cid"
	flatfs "github.com/ipfs/go-ds-flatfs"

	"github.com/blobarchive/bloar/archive"
	"github.com/blobarchive/bloar/catalog"
	"github.com/blobarchive/bloar/core"
	"github.com/blobarchive/bloar/ingest"
	"github.com/blobarchive/bloar/pinning"
	"github.com/blobarchive/bloar/schema"
	"github.com/blobarchive/bloar/server"
	"github.com/blobarchive/bloar/store"
)

const (
	benchOrigin  = 8
	benchWindows = 64 // 64 sealed 8-slot windows, one blob each: a depth-3 directory.
	benchBatch   = 64 // ApplyRefs batch size.
)

// benchFixture is the single on-disk store shared by every benchmark: one live
// flatfs instance (plain = OFF, validating = ON, over the SAME instance) plus a
// real on-disk Pebble, plus a covered multi-segment head, built once.
type benchFixture struct {
	dir        string
	plain      blockstore.Blockstore
	validating blockstore.Blockstore
	kv         *pebble.DB
	cat        *catalog.Catalog

	root     cid.Cid                // the covered head's persisted root
	readSlot uint64                 // a covered slot to read
	blobCID  cid.Cid                // the blob at readSlot
	indexCID cid.Cid                // a sealed Segment node the GC mark reads
	vhs      []schema.VersionedHash // the 64 blob vhs, for ApplyRefs
}

var (
	benchOnce sync.Once
	benchFix  *benchFixture
	benchErr  error
)

// sharedFixture returns the one store every benchmark uses, building it on first
// call. It is deliberately not torn down (its flatfs/Pebble handles and temp dir
// leak for the life of the bench process); the reproduction recipe creates the base
// dir with mktemp and removes it after the process exits.
func sharedFixture(b *testing.B) *benchFixture {
	benchOnce.Do(buildBenchFixture)
	if benchErr != nil {
		b.Fatalf("building the shared bench fixture: %v", benchErr)
	}
	return benchFix
}

func benchFixParams() archive.Params {
	return archive.Params{Name: testHead, Net: testNet, OriginSlot: benchOrigin, SegBits: testSegBits, FanoutBits: testFanout}
}

func buildBenchFixture() {
	benchFix, benchErr = func() (*benchFixture, error) {
		ctx := context.Background()
		base := os.Getenv("BLOAR_BENCH_STORE_DIR")
		if base == "" {
			base = os.TempDir()
		}
		dir, err := os.MkdirTemp(base, "bloar-bench")
		if err != nil {
			return nil, err
		}

		// store.Open's construction, minus the validating wrapper, so OFF and ON are
		// the same flatfs instance wrapped two ways.
		shard, err := flatfs.ParseShardFunc(store.ShardFunc)
		if err != nil {
			return nil, err
		}
		ds, err := flatfs.CreateOrOpen(filepath.Join(dir, "blocks"), shard, true)
		if err != nil {
			return nil, err
		}
		plain := blockstore.NewBlockstore(ds, blockstore.NoPrefix())
		validating := store.Validating(plain)
		kv, err := pebble.Open(filepath.Join(dir, "kv"), &pebble.Options{Logger: quietPebble{}})
		if err != nil {
			return nil, err
		}
		cat := catalog.New(kv)

		ing, err := ingest.New(ingest.Config{Blocks: validating, Catalog: cat})
		if err != nil {
			return nil, err
		}
		body := make([]byte, 0, benchWindows*schema.BlobSize)
		for i := range benchWindows {
			body = append(body, makeBlob(uint64(i)+1)...)
		}
		put, err := ing.PutBlobs(ctx, body)
		if err != nil || len(put) != benchWindows {
			return nil, err
		}
		vhs := make([]schema.VersionedHash, len(put))
		for i := range put {
			vhs[i] = put[i].VH
		}

		head, err := archive.New(ctx, archive.Config{Blocks: validating, Resolver: cat}, benchFixParams())
		if err != nil {
			return nil, err
		}
		rows := make([]archive.RefRow, benchWindows)
		for i := range benchWindows {
			rows[i] = archive.RefRow{Slot: uint64(benchOrigin + i*8), VHs: []schema.VersionedHash{put[i].VH}}
		}
		if _, err := head.ApplyRefs(ctx, rows, uint64(benchOrigin+benchWindows*8-1)); err != nil {
			return nil, err
		}
		if err := server.NewRootStore(kv).Put(ctx, testHead, head.Root()); err != nil {
			return nil, err
		}
		enum, err := head.Enumerate(ctx)
		if err != nil {
			return nil, err
		}
		if len(enum.Sealed) == 0 {
			return nil, errors.New("fixture head sealed no segments")
		}

		return &benchFixture{
			dir: dir, plain: plain, validating: validating, kv: kv, cat: cat,
			root: head.Root(), readSlot: benchOrigin, blobCID: put[0].CID,
			indexCID: enum.Sealed[0].CID, vhs: vhs,
		}, nil
	}()
}

// blocks returns the blockstore under test: validating (ON) or plain (OFF), both
// over the one flatfs instance.
func (f *benchFixture) blocks(on bool) blockstore.Blockstore {
	if on {
		return f.validating
	}
	return f.plain
}

var benchValidation = []struct {
	name string
	on   bool
}{
	{"off", false},
	{"on", true},
}

// BenchmarkOnDiskRawBlobGet is the fundamental number: one blockstore Get of a
// ~131 KiB blob off the shared flatfs instance, plain vs validating. The delta is
// the sha2-256 the fix adds to every raw read, and there is no raw-block cache to
// amortise it.
func BenchmarkOnDiskRawBlobGet(b *testing.B) {
	f := sharedFixture(b)
	ctx := context.Background()
	for _, v := range benchValidation {
		b.Run(v.name, func(b *testing.B) {
			bs := f.blocks(v.on)
			b.SetBytes(schema.BlobSize)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if _, err := bs.Get(ctx, f.blobCID); err != nil {
					b.Fatalf("Get: %v", err)
				}
			}
		})
	}
}

// BenchmarkOnDiskIndexNodeGet measures the validation of one small dag-cbor Segment
// node, plain vs validating. Paired with BenchmarkOnDiskRawBlobGet it calibrates the
// BYTE-based validation-capacity model: each read delta is a byte-linear sha2-256
// plus a fixed per-read overhead, so the two block sizes (96 bytes here,
// 131,072 in RawBlobGet) solve for both constants -- throughput T and per-read
// overhead O. This 96-byte block has negligible hash cost, so its delta is almost pure O.
// The mark's cost is validated_bytes / T + validated_reads x O, NOT a per-node count
// times a per-node cost -- a dag-cbor node's size is not fixed (a full window's Segment
// is hundreds of KiB, spec 12.1), so a count is the read-amplification signal only.
func BenchmarkOnDiskIndexNodeGet(b *testing.B) {
	f := sharedFixture(b)
	ctx := context.Background()
	for _, v := range benchValidation {
		b.Run(v.name, func(b *testing.B) {
			bs := f.blocks(v.on)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if _, err := bs.Get(ctx, f.indexCID); err != nil {
					b.Fatalf("Get: %v", err)
				}
			}
		})
	}
}

// BenchmarkOnDiskBlobReadJSON and BenchmarkOnDiskBlobReadOctet are the full public
// read path over the shared store, plain vs validating, with the decoded-node cache
// warm and node-cache-cold. "node-cache-cold" purges core.NodeCache before each
// read so the index node is re-read and re-decoded off flatfs; the OS page cache is
// warm either way, so this is NOT a cold-disk figure. The blob's
// hash is paid on every read regardless; node-cache-cold isolates the index
// traversal the cache serves.
func BenchmarkOnDiskBlobReadJSON(b *testing.B) {
	benchOnDiskRead(b, "application/json")
}

func BenchmarkOnDiskBlobReadOctet(b *testing.B) {
	benchOnDiskRead(b, "application/octet-stream")
}

func benchOnDiskRead(b *testing.B, accept string) {
	f := sharedFixture(b)
	for _, v := range benchValidation {
		for _, cold := range []bool{false, true} {
			name := v.name + "/nodecache-warm"
			if cold {
				name = v.name + "/nodecache-cold"
			}
			b.Run(name, func(b *testing.B) {
				bs := f.blocks(v.on)
				cache, err := core.NewNodeCacheMB(64)
				if err != nil {
					b.Fatalf("NewNodeCacheMB: %v", err)
				}
				handler := benchReadServer(b, f, bs, cache)
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					if cold {
						b.StopTimer()
						cache.Purge()
						b.StartTimer()
					}
					req := httptest.NewRequest(http.MethodGet, "/"+testHead+"/eth/v1/beacon/blobs/"+itoa(f.readSlot), nil)
					req.Header.Set("Accept", accept)
					rec := httptest.NewRecorder()
					handler.ServeHTTP(rec, req)
					if rec.Code != http.StatusOK {
						b.Fatalf("GET status = %d", rec.Code)
					}
				}
			})
		}
	}
}

// benchReadServer loads the shared head over bs (reading the persisted root's spine
// from the store) and builds a server that reads through bs. Only the read wrapper
// varies; the stored bytes are the fixture's.
func benchReadServer(b *testing.B, f *benchFixture, bs blockstore.Blockstore, cache *core.NodeCache) *server.Server {
	b.Helper()
	ctx := context.Background()
	head, err := archive.Load(ctx, archive.Config{Blocks: bs, Resolver: f.cat, Cache: cache}, f.root)
	if err != nil {
		b.Fatalf("archive.Load: %v", err)
	}
	heads, err := server.NewHeads(server.HeadsConfig{Net: testNet, Roots: server.NewRootStore(f.kv)})
	if err != nil {
		b.Fatalf("NewHeads: %v", err)
	}
	if err := heads.Add(head); err != nil {
		b.Fatalf("Heads.Add: %v", err)
	}
	ing, err := ingest.New(ingest.Config{Blocks: bs, Catalog: f.cat})
	if err != nil {
		b.Fatalf("ingest.New: %v", err)
	}
	handler, err := server.New(server.Config{
		Heads: heads, Blocks: bs, Ingester: ing,
		AuthToken: "bench", Beacon: server.Beacon{SecondsPerSlot: 12},
	})
	if err != nil {
		b.Fatalf("server.New: %v", err)
	}
	return handler
}

// BenchmarkOnDiskApplyRefs64 measures the mutation-time cost of no longer trusting
// Has: a fresh head applies one 64-blob batch, reading every blob (the fixture's,
// all pinned so GC never sweeps them) through the store to validate before
// committing. A fresh head per iteration (excluded from the timer) keeps every
// measured call the non-replay path that resolves rows.
func BenchmarkOnDiskApplyRefs64(b *testing.B) {
	f := sharedFixture(b)
	ctx := context.Background()
	rows := make([]archive.RefRow, benchBatch)
	for i := range benchBatch {
		rows[i] = archive.RefRow{Slot: uint64(benchOrigin + i), VHs: []schema.VersionedHash{f.vhs[i]}}
	}
	syncedTo := uint64(benchOrigin + benchBatch - 1)
	for _, v := range benchValidation {
		b.Run(v.name, func(b *testing.B) {
			bs := f.blocks(v.on)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				b.StopTimer()
				head, err := archive.New(ctx, archive.Config{Blocks: bs, Resolver: f.cat}, benchFixParams())
				if err != nil {
					b.Fatalf("archive.New: %v", err)
				}
				b.StartTimer()
				if _, err := head.ApplyRefs(ctx, rows, syncedTo); err != nil {
					b.Fatalf("ApplyRefs: %v", err)
				}
			}
		})
	}
}

// BenchmarkOnDiskGCMark measures one whole GC run (spec 9) over the shared covered
// head, plain vs validating. Its OFF-vs-ON figure is OBSERVATIONAL, not a controlled
// paired comparison: -count runs all OFF iterations then all ON, so the difference
// cannot be separated from order/thermal drift. The rigorous size comes instead from
// the isolated per-block costs (BenchmarkOnDiskIndexNodeGet and BenchmarkOnDiskRawBlobGet)
// times what the mark reads. A warmup run reconciles the pins and asserts the mark
// traverses a representative closure (more blocks marked than there are blobs), so it
// can never degrade to an empty or short-circuited mark.
//
// This is an integrated validation harness: the mark reads and validates every
// raw leaf and direct pin as well as index nodes, so a whole GC.Run over this
// fixture's closure pays the leaf reads too. It exercises the deliberately
// combined validation path and is observational only; production runs GC and
// full-byte scrub separately, so do not use this benchmark to size that split.
func BenchmarkOnDiskGCMark(b *testing.B) {
	f := sharedFixture(b)
	ctx := context.Background()
	for _, v := range benchValidation {
		b.Run(v.name, func(b *testing.B) {
			bs := f.blocks(v.on)
			head, err := archive.Load(ctx, archive.Config{Blocks: bs, Resolver: f.cat}, f.root)
			if err != nil {
				b.Fatalf("archive.Load: %v", err)
			}
			led := catalog.NewLedger(f.kv)
			rec, err := pinning.NewReconciler(pinning.Config{Ledger: led})
			if err != nil {
				b.Fatalf("NewReconciler: %v", err)
			}
			if err := rec.Add(head, pinning.Full()); err != nil {
				b.Fatalf("Reconciler.Add: %v", err)
			}
			gc, err := pinning.NewGC(pinning.GCConfig{Blocks: bs, Reconciler: rec})
			if err != nil {
				b.Fatalf("NewGC: %v", err)
			}
			stats, err := gc.Run(ctx)
			if err != nil {
				b.Fatalf("GC.Run (warmup): %v", err)
			}
			if stats.Marked <= benchWindows {
				b.Fatalf("GC mark reached only %d blocks over %d windows; closure is not representative",
					stats.Marked, benchWindows)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if _, err := gc.Run(ctx); err != nil {
					b.Fatalf("GC.Run: %v", err)
				}
			}
		})
	}
}
