package store_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	flatfs "github.com/ipfs/go-ds-flatfs"
	ipld "github.com/ipfs/go-ipld-format"

	"github.com/blobarchive/bloar/store"
)

func openReadOnly(t *testing.T, path string) *store.ReadOnlyBlockstore {
	t.Helper()
	reader, err := store.OpenReadOnlyBlocks(path)
	if err != nil {
		t.Fatalf("OpenReadOnlyBlocks(%s): %v", path, err)
	}
	t.Cleanup(func() {
		if err := reader.Close(); err != nil {
			t.Errorf("Close read-only blockstore: %v", err)
		}
	})
	return reader
}

func TestReadOnlyBlocksParityWithOpenStore(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	writer := mustOpen(t, root)

	raw := blocks.NewBlock([]byte("raw block visible through both stores"))
	dagData := []byte("dag-cbor bytes need not decode for blockstore parity")
	dagCID := cidOver(t, cid.DagCBOR, dagData)
	dagBlock, err := blocks.NewBlockWithCid(dagData, dagCID)
	if err != nil {
		t.Fatalf("creating dag-cbor block: %v", err)
	}
	for _, block := range []blocks.Block{raw, dagBlock} {
		if err := writer.Blocks().Put(ctx, block); err != nil {
			t.Fatalf("writer Put(%s): %v", block.Cid(), err)
		}
	}

	// This succeeds while writer still owns Pebble's exclusive KV lock,
	// proving the reader does not open the combined store.
	reader := openReadOnly(t, filepath.Join(root, "blocks"))
	for _, block := range []blocks.Block{raw, dagBlock} {
		has, err := reader.Has(ctx, block.Cid())
		if err != nil {
			t.Fatalf("reader Has(%s): %v", block.Cid(), err)
		}
		if !has {
			t.Errorf("reader Has(%s) = false, want true", block.Cid())
		}
		size, err := reader.GetSize(ctx, block.Cid())
		if err != nil {
			t.Fatalf("reader GetSize(%s): %v", block.Cid(), err)
		}
		if size != len(block.RawData()) {
			t.Errorf("reader GetSize(%s) = %d, want %d", block.Cid(), size, len(block.RawData()))
		}
		got, err := reader.Get(ctx, block.Cid())
		if err != nil {
			t.Fatalf("reader Get(%s): %v", block.Cid(), err)
		}
		if !bytes.Equal(got.RawData(), block.RawData()) {
			t.Errorf("reader Get(%s) bytes differ", block.Cid())
		}
	}

	missing := blocks.NewBlock([]byte("not stored")).Cid()
	if has, err := reader.Has(ctx, missing); err != nil || has {
		t.Fatalf("reader Has(missing) = (%t, %v), want (false, nil)", has, err)
	}
	if size, err := reader.GetSize(ctx, missing); !ipld.IsNotFound(err) || size != -1 {
		t.Fatalf("reader GetSize(missing) = (%d, %v), want (-1, not-found)", size, err)
	}
	if _, err := reader.Get(ctx, missing); !ipld.IsNotFound(err) {
		t.Fatalf("reader Get(missing) = %v, want not-found", err)
	}

	var viewed []byte
	if err := reader.View(ctx, raw.Cid(), func(data []byte) error {
		viewed = append(viewed, data...)
		return nil
	}); err != nil {
		t.Fatalf("reader View: %v", err)
	}
	if !bytes.Equal(viewed, raw.RawData()) {
		t.Errorf("reader View bytes differ")
	}

	keys, err := reader.AllKeysChan(ctx)
	if err != nil {
		t.Fatalf("reader AllKeysChan: %v", err)
	}
	var hashes []string
	for c := range keys {
		hashes = append(hashes, c.Hash().B58String())
	}
	for _, block := range []blocks.Block{raw, dagBlock} {
		if !slices.Contains(hashes, block.Cid().Hash().B58String()) {
			t.Errorf("AllKeysChan omitted %s", block.Cid())
		}
	}
}

func TestReadOnlyBlocksRejectMutations(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	writer := mustOpen(t, root)
	existing := blocks.NewBlock([]byte("must survive rejected deletion"))
	if err := writer.Blocks().Put(ctx, existing); err != nil {
		t.Fatalf("writer Put: %v", err)
	}
	reader := openReadOnly(t, filepath.Join(root, "blocks"))

	candidate := blocks.NewBlock([]byte("must not be written"))
	tests := []struct {
		name string
		op   func() error
		want string
	}{
		{name: "put", op: func() error { return reader.Put(ctx, candidate) }, want: "put block"},
		{name: "put many", op: func() error {
			return reader.PutMany(ctx, []blocks.Block{candidate})
		}, want: "put blocks"},
		{name: "delete", op: func() error { return reader.DeleteBlock(ctx, existing.Cid()) }, want: "delete block"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.op()
			if !errors.Is(err, store.ErrReadOnly) {
				t.Fatalf("error = %v, want ErrReadOnly", err)
			}
			var readOnly *store.ReadOnlyError
			if !errors.As(err, &readOnly) {
				t.Fatalf("error = %v (%T), want *store.ReadOnlyError", err, err)
			}
			if readOnly.Operation != tt.want {
				t.Errorf("operation = %q, want %q", readOnly.Operation, tt.want)
			}
		})
	}

	if has, err := writer.Blocks().Has(ctx, candidate.Cid()); err != nil || has {
		t.Fatalf("writer Has(rejected put) = (%t, %v), want (false, nil)", has, err)
	}
	if has, err := writer.Blocks().Has(ctx, existing.Cid()); err != nil || !has {
		t.Fatalf("writer Has(rejected delete) = (%t, %v), want (true, nil)", has, err)
	}
}

func TestReadOnlyBlocksSeesConcurrentWriterCommits(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	writer := mustOpen(t, root)
	reader := openReadOnly(t, filepath.Join(root, "blocks"))

	const count = 64
	written := make(chan blocks.Block, count)
	writeErr := make(chan error, 1)
	go func() {
		defer close(written)
		for i := 0; i < count; i++ {
			block := blocks.NewBlock([]byte(fmt.Sprintf("concurrently committed block %03d", i)))
			if err := writer.Blocks().Put(ctx, block); err != nil {
				writeErr <- err
				return
			}
			written <- block
		}
	}()

	for block := range written {
		deadline := time.Now().Add(2 * time.Second)
		for {
			has, err := reader.Has(ctx, block.Cid())
			if err != nil {
				t.Fatalf("reader Has(%s): %v", block.Cid(), err)
			}
			if has {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("reader did not observe committed block %s", block.Cid())
			}
			time.Sleep(time.Millisecond)
		}
		got, err := reader.Get(ctx, block.Cid())
		if err != nil {
			t.Fatalf("reader Get(%s): %v", block.Cid(), err)
		}
		if !bytes.Equal(got.RawData(), block.RawData()) {
			t.Fatalf("reader Get(%s) returned different bytes", block.Cid())
		}
	}
	select {
	case err := <-writeErr:
		t.Fatalf("concurrent writer: %v", err)
	default:
	}
}

func TestReadOnlyBlocksValidatesContent(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	writer := mustOpen(t, root)

	honest := []byte("the CID commits to these bytes")
	c := cidOver(t, cid.Raw, honest)
	corrupt(t, writer.Blocks(), c, []byte("different bytes stored under that key"))

	reader := openReadOnly(t, filepath.Join(root, "blocks"))
	if _, err := reader.Get(ctx, c); !errors.Is(err, store.ErrCorruptBlock) {
		t.Fatalf("Get corrupt block = %v, want ErrCorruptBlock", err)
	}
}

func TestOpenReadOnlyBlocksDoesNotCreateMetadata(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX mode-bit test")
	}
	ctx := context.Background()
	root := t.TempDir()
	writer, err := store.Open(root)
	if err != nil {
		t.Fatalf("Open writer: %v", err)
	}
	block := blocks.NewBlock([]byte("read from a mode-bit read-only tree"))
	if err := writer.Blocks().Put(ctx, block); err != nil {
		t.Fatalf("writer Put: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("writer Close: %v", err)
	}

	blocksPath := filepath.Join(root, "blocks")
	if err := os.RemoveAll(filepath.Join(blocksPath, ".temp")); err != nil {
		t.Fatalf("remove writer .temp: %v", err)
	}
	if err := os.Remove(filepath.Join(blocksPath, flatfs.DiskUsageFile)); err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("remove writer disk-usage cache: %v", err)
	}
	restoreTreeModes(t, blocksPath)
	makeTreeReadOnly(t, blocksPath)

	reader := openReadOnly(t, blocksPath)
	if _, err := reader.Get(ctx, block.Cid()); err != nil {
		t.Fatalf("Get on read-only tree: %v", err)
	}
	if _, err := reader.AllKeysChan(ctx); err != nil {
		t.Fatalf("AllKeysChan on read-only tree: %v", err)
	}
	if err := reader.Put(ctx, blocks.NewBlock([]byte("rejected"))); !errors.Is(err, store.ErrReadOnly) {
		t.Fatalf("Put on read-only tree = %v, want ErrReadOnly", err)
	}
	for _, forbidden := range []string{".temp", flatfs.DiskUsageFile} {
		if _, err := os.Stat(filepath.Join(blocksPath, forbidden)); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("%s exists after read-only open: %v", forbidden, err)
		}
	}
}

func TestOpenReadOnlyBlocksValidation(t *testing.T) {
	tests := []struct {
		name string
		path func(t *testing.T) string
		want func(error) bool
	}{
		{
			name: "empty path",
			path: func(*testing.T) string { return "" },
			want: func(err error) bool { return err != nil },
		},
		{
			name: "missing directory",
			path: func(t *testing.T) string { return filepath.Join(t.TempDir(), "missing") },
			want: func(err error) bool { return errors.Is(err, os.ErrNotExist) },
		},
		{
			name: "file",
			path: func(t *testing.T) string {
				path := filepath.Join(t.TempDir(), "file")
				if err := os.WriteFile(path, []byte("x"), 0o644); err != nil {
					t.Fatal(err)
				}
				return path
			},
			want: func(err error) bool { return err != nil },
		},
		{
			name: "missing SHARDING",
			path: func(t *testing.T) string { return t.TempDir() },
			want: func(err error) bool { return errors.Is(err, flatfs.ErrShardingFileMissing) },
		},
		{
			name: "wrong shard",
			path: func(t *testing.T) string {
				path := t.TempDir()
				if err := os.WriteFile(
					filepath.Join(path, "SHARDING"),
					[]byte("/repo/flatfs/shard/v1/next-to-last/2\n"),
					0o644,
				); err != nil {
					t.Fatal(err)
				}
				return path
			},
			want: func(err error) bool {
				var mismatch *store.ShardMismatchError
				return errors.As(err, &mismatch)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader, err := store.OpenReadOnlyBlocks(tt.path(t))
			if reader != nil {
				_ = reader.Close()
			}
			if !tt.want(err) {
				t.Fatalf("OpenReadOnlyBlocks error = %v", err)
			}
		})
	}
}

func TestReadOnlyBlocksClose(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	writer := mustOpen(t, root)
	block := blocks.NewBlock([]byte("closed reader"))
	if err := writer.Blocks().Put(ctx, block); err != nil {
		t.Fatalf("writer Put: %v", err)
	}
	reader, err := store.OpenReadOnlyBlocks(filepath.Join(root, "blocks"))
	if err != nil {
		t.Fatalf("OpenReadOnlyBlocks: %v", err)
	}
	if err := reader.Close(); err != nil {
		t.Fatalf("Close #1: %v", err)
	}
	if err := reader.Close(); err != nil {
		t.Fatalf("Close #2: %v", err)
	}
	if _, err := reader.Get(ctx, block.Cid()); !errors.Is(err, store.ErrReadOnlyBlocksClosed) {
		t.Fatalf("Get after Close = %v, want ErrReadOnlyBlocksClosed", err)
	}
	if _, err := reader.AllKeysChan(ctx); !errors.Is(err, store.ErrReadOnlyBlocksClosed) {
		t.Fatalf("AllKeysChan after Close = %v, want ErrReadOnlyBlocksClosed", err)
	}
	// Mutations keep their stronger and more useful classification.
	if err := reader.DeleteBlock(ctx, block.Cid()); !errors.Is(err, store.ErrReadOnly) {
		t.Fatalf("DeleteBlock after Close = %v, want ErrReadOnly", err)
	}
}

// TestReadOnlyBlocksKernelReadOnlyMount is an opt-in integration test. It
// re-executes this test binary in a locked-down container with blocks mounted
// read-only, which exercises the actual kernel EROFS boundary rather than only
// POSIX mode bits. Set BLOAR_TEST_CONTAINER_IMAGE to a local Linux image and
// run with CGO_ENABLED=0.
func TestReadOnlyBlocksKernelReadOnlyMount(t *testing.T) {
	if os.Getenv("BLOAR_RO_MOUNT_CHILD") == "1" {
		testReadOnlyBlocksKernelMountChild(t)
		return
	}
	image := os.Getenv("BLOAR_TEST_CONTAINER_IMAGE")
	if image == "" {
		t.Skip("set BLOAR_TEST_CONTAINER_IMAGE to run the kernel read-only mount test")
	}
	if runtime.GOOS != "linux" {
		t.Skip("container mount test requires Linux")
	}

	ctx := context.Background()
	root := t.TempDir()
	writer, err := store.Open(root)
	if err != nil {
		t.Fatalf("Open writer: %v", err)
	}
	block := blocks.NewBlock([]byte("read through a kernel read-only bind mount"))
	if err := writer.Blocks().Put(ctx, block); err != nil {
		t.Fatalf("writer Put: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("writer Close: %v", err)
	}
	blocksPath := filepath.Join(root, "blocks")
	if err := os.RemoveAll(filepath.Join(blocksPath, ".temp")); err != nil {
		t.Fatalf("remove writer .temp: %v", err)
	}
	if err := os.Remove(filepath.Join(blocksPath, flatfs.DiskUsageFile)); err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("remove writer disk-usage cache: %v", err)
	}
	makeTreeWorldReadable(t, root)

	executable, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}
	args := []string{
		"run", "--rm",
		"--network=none",
		"--read-only",
		"--cap-drop=ALL",
		"--security-opt=no-new-privileges",
		"--user=65532:65532",
		"-e", "BLOAR_RO_MOUNT_CHILD=1",
		"-e", "BLOAR_RO_MOUNT_CID=" + block.Cid().String(),
		"-v", executable + ":/readonly-test:ro",
		"-v", blocksPath + ":/blocks:ro",
		"--entrypoint=/readonly-test",
		image,
		"-test.run=^TestReadOnlyBlocksKernelReadOnlyMount$",
		"-test.v",
	}
	command := exec.Command("docker", args...)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("kernel read-only container: %v\n%s", err, output)
	}
	t.Logf("kernel read-only child:\n%s", output)
}

func testReadOnlyBlocksKernelMountChild(t *testing.T) {
	ctx := context.Background()
	c, err := cid.Parse(os.Getenv("BLOAR_RO_MOUNT_CID"))
	if err != nil {
		t.Fatalf("parse BLOAR_RO_MOUNT_CID: %v", err)
	}
	reader := openReadOnly(t, "/blocks")
	if _, err := reader.Get(ctx, c); err != nil {
		t.Fatalf("Get from kernel read-only mount: %v", err)
	}
	probe := "/blocks/readonly-open-must-not-write"
	err = os.WriteFile(probe, []byte("forbidden"), 0o600)
	if err == nil {
		_ = os.Remove(probe)
		t.Fatal("kernel read-only mount accepted a direct filesystem write")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "read-only file system") {
		t.Fatalf("direct filesystem write = %v, want read-only filesystem error", err)
	}
	if err := reader.Put(ctx, blocks.NewBlock([]byte("forbidden"))); !errors.Is(err, store.ErrReadOnly) {
		t.Fatalf("Put = %v, want ErrReadOnly", err)
	}
	for _, forbidden := range []string{".temp", flatfs.DiskUsageFile} {
		if _, err := os.Stat(filepath.Join("/blocks", forbidden)); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("%s exists after open: %v", forbidden, err)
		}
	}
}

func makeTreeReadOnly(t *testing.T, root string) {
	t.Helper()
	if err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return os.Chmod(path, 0o555)
		}
		return os.Chmod(path, 0o444)
	}); err != nil {
		t.Fatalf("make tree read-only: %v", err)
	}
}

func restoreTreeModes(t *testing.T, root string) {
	t.Helper()
	t.Cleanup(func() {
		_ = filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
			if err != nil {
				return nil
			}
			if entry.IsDir() {
				_ = os.Chmod(path, 0o755)
			} else {
				_ = os.Chmod(path, 0o644)
			}
			return nil
		})
	})
}

func makeTreeWorldReadable(t *testing.T, root string) {
	t.Helper()
	if err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return os.Chmod(path, 0o755)
		}
		return os.Chmod(path, 0o644)
	}); err != nil {
		t.Fatalf("make tree world-readable: %v", err)
	}
}
