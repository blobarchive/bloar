package store

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/ipfs/boxo/blockstore"
	dshelp "github.com/ipfs/boxo/datastore/dshelp"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	datastore "github.com/ipfs/go-datastore"
	flatfs "github.com/ipfs/go-ds-flatfs"
	ipld "github.com/ipfs/go-ipld-format"
)

// ErrReadOnly is the error class returned by every mutation attempted through
// a ReadOnlyBlockstore. Callers should match it with errors.Is; a
// *ReadOnlyError identifies the rejected operation.
var ErrReadOnly = errors.New("store: blockstore is read-only")

// ErrReadOnlyBlocksClosed is returned when a read is attempted after the
// ReadOnlyBlockstore has been closed.
var ErrReadOnlyBlocksClosed = errors.New("store: read-only blockstore is closed")

// ReadOnlyError reports a mutation rejected by a ReadOnlyBlockstore.
type ReadOnlyError struct {
	Operation string
}

func (e *ReadOnlyError) Error() string {
	return fmt.Sprintf("store: cannot %s: blockstore is read-only", e.Operation)
}

// Is makes every ReadOnlyError part of the ErrReadOnly class.
func (e *ReadOnlyError) Is(target error) bool { return target == ErrReadOnly }

// ReadOnlyBlockstore reads an existing bloar flatfs block directory without
// opening Pebble or the go-ds-flatfs Datastore.
//
// go-ds-flatfs.Open is intentionally not used here: opening that Datastore
// removes and recreates .temp, calculates disk usage, writes
// diskUsage.cache, and starts a checkpoint goroutine. This reader derives the
// same paths directly from the verified SHARDING function and performs only
// os.Open/os.Stat operations. It is therefore safe to use against a
// kernel-mounted read-only view while another process owns the writable store.
//
// Get re-hashes every returned block through the same Validating wrapper used
// by Store.Blocks. The mutation methods fail before touching the filesystem;
// the filesystem mount should still be read-only as the independent
// containment boundary.
type ReadOnlyBlockstore struct {
	raw       *readOnlyFlatFSBlockstore
	validated blockstore.Blockstore
}

// OpenReadOnlyBlocks opens an existing <store.path>/blocks directory.
//
// blocksPath must name the blocks directory itself, not the combined store
// root. The directory and its SHARDING file must already exist, and the shard
// function must exactly match ShardFunc. The call creates no files and takes no
// lock on <store.path>/kv.
func OpenReadOnlyBlocks(blocksPath string) (*ReadOnlyBlockstore, error) {
	if blocksPath == "" {
		return nil, errors.New("store: read-only blocks path must not be empty")
	}
	absolute, err := filepath.Abs(blocksPath)
	if err != nil {
		return nil, fmt.Errorf("store: resolving read-only blocks path %s: %w", blocksPath, err)
	}
	info, err := os.Stat(absolute)
	if err != nil {
		return nil, fmt.Errorf("store: opening read-only blocks at %s: %w", absolute, err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("store: read-only blocks path %s is not a directory", absolute)
	}

	shard, err := flatfs.ReadShardFunc(absolute)
	if err != nil {
		return nil, fmt.Errorf("store: reading shard function at %s: %w", absolute, err)
	}
	if shard.String() != ShardFunc {
		return nil, &ShardMismatchError{Path: absolute, Want: ShardFunc, Got: shard.String()}
	}

	raw := &readOnlyFlatFSBlockstore{
		path:  absolute,
		shard: shard.Func(),
		done:  make(chan struct{}),
	}
	return &ReadOnlyBlockstore{
		raw:       raw,
		validated: Validating(raw),
	}, nil
}

func (r *ReadOnlyBlockstore) DeleteBlock(context.Context, cid.Cid) error {
	return &ReadOnlyError{Operation: "delete block"}
}

func (r *ReadOnlyBlockstore) Has(ctx context.Context, c cid.Cid) (bool, error) {
	return r.raw.Has(ctx, c)
}

func (r *ReadOnlyBlockstore) Get(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	return r.validated.Get(ctx, c)
}

func (r *ReadOnlyBlockstore) GetSize(ctx context.Context, c cid.Cid) (int, error) {
	return r.raw.GetSize(ctx, c)
}

func (r *ReadOnlyBlockstore) Put(context.Context, blocks.Block) error {
	return &ReadOnlyError{Operation: "put block"}
}

func (r *ReadOnlyBlockstore) PutMany(context.Context, []blocks.Block) error {
	return &ReadOnlyError{Operation: "put blocks"}
}

func (r *ReadOnlyBlockstore) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	return r.raw.AllKeysChan(ctx)
}

// View implements blockstore.Viewer. The callback receives a slice owned by
// this call and must follow the Viewer contract by neither retaining nor
// mutating it.
func (r *ReadOnlyBlockstore) View(ctx context.Context, c cid.Cid, callback func([]byte) error) error {
	block, err := r.Get(ctx, c)
	if err != nil {
		return err
	}
	return callback(block.RawData())
}

// Close is idempotent. It prevents new reads and asks in-flight enumeration to
// stop; there is no datastore, Pebble database, background goroutine, or
// persistent file descriptor to close.
func (r *ReadOnlyBlockstore) Close() error {
	r.raw.closeOnce.Do(func() { close(r.raw.done) })
	return nil
}

var (
	_ blockstore.Blockstore = (*ReadOnlyBlockstore)(nil)
	_ blockstore.Viewer     = (*ReadOnlyBlockstore)(nil)
	_ io.Closer             = (*ReadOnlyBlockstore)(nil)
)

// readOnlyFlatFSBlockstore implements the unvalidated filesystem reads that
// Validating wraps. It is kept private so no caller can accidentally bypass
// the CID re-hash performed by ReadOnlyBlockstore.Get.
type readOnlyFlatFSBlockstore struct {
	path  string
	shard flatfs.ShardFunc

	done      chan struct{}
	closeOnce sync.Once
}

func (r *readOnlyFlatFSBlockstore) checkReadable(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	select {
	case <-r.done:
		return ErrReadOnlyBlocksClosed
	default:
		return nil
	}
}

func (r *readOnlyFlatFSBlockstore) filePath(c cid.Cid) (string, bool) {
	if !c.Defined() {
		return "", false
	}
	key := dshelp.MultihashToDsKey(c.Hash()).String()
	if len(key) < 2 || key[0] != '/' || strings.ContainsRune(key[1:], '/') {
		return "", false
	}
	name := key[1:]
	return filepath.Join(r.path, r.shard(name), name+".data"), true
}

func (r *readOnlyFlatFSBlockstore) DeleteBlock(context.Context, cid.Cid) error {
	return &ReadOnlyError{Operation: "delete block"}
}

func (r *readOnlyFlatFSBlockstore) Has(ctx context.Context, c cid.Cid) (bool, error) {
	if err := r.checkReadable(ctx); err != nil {
		return false, err
	}
	path, ok := r.filePath(c)
	if !ok {
		return false, nil
	}
	info, err := os.Stat(path)
	switch {
	case err == nil:
		return info.Mode().IsRegular(), nil
	case errors.Is(err, os.ErrNotExist):
		return false, nil
	default:
		return false, err
	}
}

func (r *readOnlyFlatFSBlockstore) Get(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	if err := r.checkReadable(ctx); err != nil {
		return nil, err
	}
	path, ok := r.filePath(c)
	if !ok {
		return nil, ipld.ErrNotFound{Cid: c}
	}
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, ipld.ErrNotFound{Cid: c}
	}
	if err != nil {
		return nil, err
	}
	if err := r.checkReadable(ctx); err != nil {
		return nil, err
	}
	block, err := blocks.NewBlockWithCid(data, c)
	if err != nil {
		return nil, fmt.Errorf("store: framing read-only block %s: %w", c, err)
	}
	return block, nil
}

func (r *readOnlyFlatFSBlockstore) GetSize(ctx context.Context, c cid.Cid) (int, error) {
	if err := r.checkReadable(ctx); err != nil {
		return -1, err
	}
	path, ok := r.filePath(c)
	if !ok {
		return -1, ipld.ErrNotFound{Cid: c}
	}
	info, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return -1, ipld.ErrNotFound{Cid: c}
	}
	if err != nil {
		return -1, err
	}
	if !info.Mode().IsRegular() {
		return -1, ipld.ErrNotFound{Cid: c}
	}
	return int(info.Size()), nil
}

func (r *readOnlyFlatFSBlockstore) Put(context.Context, blocks.Block) error {
	return &ReadOnlyError{Operation: "put block"}
}

func (r *readOnlyFlatFSBlockstore) PutMany(context.Context, []blocks.Block) error {
	return &ReadOnlyError{Operation: "put blocks"}
}

func (r *readOnlyFlatFSBlockstore) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	if err := r.checkReadable(ctx); err != nil {
		return nil, err
	}
	root, err := os.Open(r.path)
	if err != nil {
		return nil, err
	}

	output := make(chan cid.Cid, 128)
	go func() {
		defer root.Close()
		defer close(output)

		for {
			dirs, err := root.ReadDir(256)
			if err != nil && !errors.Is(err, io.EOF) {
				return
			}
			for _, dir := range dirs {
				if !dir.IsDir() || strings.HasPrefix(dir.Name(), ".") {
					continue
				}
				if !r.enumerateShard(ctx, filepath.Join(r.path, dir.Name()), output) {
					return
				}
			}
			if errors.Is(err, io.EOF) {
				return
			}
		}
	}()
	return output, nil
}

func (r *readOnlyFlatFSBlockstore) enumerateShard(ctx context.Context, path string, output chan<- cid.Cid) bool {
	dir, err := os.Open(path)
	if err != nil {
		return false
	}
	defer dir.Close()

	for {
		entries, err := dir.ReadDir(256)
		if err != nil && !errors.Is(err, io.EOF) {
			return false
		}
		for _, entry := range entries {
			if !entry.Type().IsRegular() || !strings.HasSuffix(entry.Name(), ".data") {
				continue
			}
			name := strings.TrimSuffix(entry.Name(), ".data")
			hash, decodeErr := dshelp.DsKeyToMultihash(datastore.NewKey(name))
			if decodeErr != nil {
				continue
			}
			c := cid.NewCidV1(cid.Raw, hash)
			select {
			case <-ctx.Done():
				return false
			case <-r.done:
				return false
			case output <- c:
			}
		}
		if errors.Is(err, io.EOF) {
			return true
		}
	}
}
