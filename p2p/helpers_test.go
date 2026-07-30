package p2p_test

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/ipfs/boxo/blockstore"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multihash"

	"github.com/blobarchive/bloar/p2p"
)

// The tests here run real libp2p hosts over loopback TCP rather than a mock
// network. What is being tested is that this package's wiring produces a node
// that actually exchanges blocks and actually resolves a name -- a mock network
// would test the mock. TCP on 127.0.0.1 with an ephemeral port is fast enough
// that it does not show up next to the rest of the suite.
func newTestHost(t *testing.T, opts ...func(*p2p.HostConfig)) *p2p.Host {
	t.Helper()
	cfg := p2p.HostConfig{
		Listen:          []string{"/ip4/127.0.0.1/tcp/0"},
		IdentityKeyFile: filepath.Join(t.TempDir(), "p2p.key"),
	}
	for _, o := range opts {
		o(&cfg)
	}
	h, err := p2p.NewHost(t.Context(), cfg)
	if err != nil {
		t.Fatalf("building host: %v", err)
	}
	t.Cleanup(func() {
		if err := h.Close(); err != nil {
			t.Errorf("closing host: %v", err)
		}
	})
	return h
}

// connect wires a to b directly, which is what p2p.peers does in a daemon. The
// tests do it by hand so that a failure is about bitswap or IPNS rather than
// about how long a dial took.
func connect(t *testing.T, a, b *p2p.Host) {
	t.Helper()
	if err := a.Libp2p().Connect(t.Context(), hostInfo(b)); err != nil {
		t.Fatalf("connecting %s to %s: %v", a.ID(), b.ID(), err)
	}
}

func hostInfo(h *p2p.Host) peer.AddrInfo {
	return peer.AddrInfo{ID: h.ID(), Addrs: h.Libp2p().Addrs()}
}

func memBlocks() blockstore.Blockstore {
	return blockstore.NewBlockstore(dssync.MutexWrap(datastore.NewMapDatastore()))
}

func memKV(t *testing.T) *pebble.DB {
	t.Helper()
	kv, _ := openKV(t, filepath.Join(t.TempDir(), "kv"))
	return kv
}

// openKV opens a Pebble at an explicit path, so that a test can close one and
// reopen the same bytes -- which is how the restart tests here are spelled. The
// returned close is what such a test calls; it is also the cleanup, and it is
// once-guarded because Pebble panics on a second Close rather than reporting
// one.
func openKV(t *testing.T, path string) (*pebble.DB, func()) {
	t.Helper()
	kv, err := pebble.Open(path, &pebble.Options{Logger: quietPebble{}})
	if err != nil {
		t.Fatalf("opening kv: %v", err)
	}
	var once sync.Once
	closeKV := func() {
		once.Do(func() {
			if err := kv.Close(); err != nil {
				t.Errorf("closing kv: %v", err)
			}
		})
	}
	t.Cleanup(closeKV)
	return kv, closeKV
}

// quietPebble keeps Pebble's compaction chatter out of the test output.
type quietPebble struct{}

func (quietPebble) Infof(string, ...any)  {}
func (quietPebble) Errorf(string, ...any) {}
func (quietPebble) Fatalf(f string, a ...any) {
	panic(fmt.Sprintf(f, a...))
}

// rawBlock renders b as the raw/sha2-256 block everything in this archive
// stores leaves as (spec 2).
func rawBlock(t *testing.T, b []byte) blocks.Block {
	t.Helper()
	h, err := multihash.Sum(b, multihash.SHA2_256, -1)
	if err != nil {
		t.Fatalf("hashing block: %v", err)
	}
	blk, err := blocks.NewBlockWithCid(b, cid.NewCidV1(cid.Raw, h))
	if err != nil {
		t.Fatalf("building block: %v", err)
	}
	return blk
}

func putBlock(t *testing.T, bs blockstore.Blockstore, b blocks.Block) {
	t.Helper()
	if err := bs.Put(context.Background(), b); err != nil {
		t.Fatalf("putting block: %v", err)
	}
}

func newTestExchange(t *testing.T, h *p2p.Host, bs blockstore.Blockstore) *p2p.Exchange {
	t.Helper()
	e, err := p2p.NewExchange(t.Context(), p2p.ExchangeConfig{Host: h, Blocks: bs})
	if err != nil {
		t.Fatalf("building exchange: %v", err)
	}
	t.Cleanup(func() {
		if err := e.Close(); err != nil {
			t.Errorf("closing exchange: %v", err)
		}
	})
	return e
}

func newTestDocs(t *testing.T, base blockstore.Blockstore) *p2p.DocBlockstore {
	t.Helper()
	d, err := p2p.NewDocBlockstore(base)
	if err != nil {
		t.Fatalf("building doc blockstore: %v", err)
	}
	return d
}
