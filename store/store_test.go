package store_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	blocks "github.com/ipfs/go-block-format"

	"github.com/blobarchive/bloar/store"
)

func mustOpen(t *testing.T, path string) *store.Store {
	t.Helper()
	s, err := store.Open(path)
	if err != nil {
		t.Fatalf("Open(%s): %v", path, err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestOpenCreatesLayout(t *testing.T) {
	path := t.TempDir()
	s := mustOpen(t, path)

	if s.Path() != path {
		t.Errorf("Path: got %s want %s", s.Path(), path)
	}
	if s.Blocks() == nil {
		t.Error("Blocks: got nil")
	}
	if s.KV() == nil {
		t.Error("KV: got nil")
	}

	tests := []struct {
		name string
		path string
		dir  bool
	}{
		{name: "blocks dir", path: filepath.Join(path, "blocks"), dir: true},
		{name: "kv dir", path: filepath.Join(path, "kv"), dir: true},
		{name: "SHARDING file", path: filepath.Join(path, "blocks", "SHARDING")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fi, err := os.Stat(tt.path)
			if err != nil {
				t.Fatalf("stat: %v", err)
			}
			if fi.IsDir() != tt.dir {
				t.Errorf("IsDir: got %t want %t", fi.IsDir(), tt.dir)
			}
		})
	}

	sharding, err := os.ReadFile(filepath.Join(path, "blocks", "SHARDING"))
	if err != nil {
		t.Fatalf("reading SHARDING: %v", err)
	}
	if got := string(sharding); got != store.ShardFunc+"\n" {
		t.Errorf("SHARDING: got %q want %q", got, store.ShardFunc+"\n")
	}
}

// TestReopenRoundtrip: blocks written before a close are readable after
// reopening the same path.
func TestReopenRoundtrip(t *testing.T) {
	ctx := context.Background()
	path := t.TempDir()

	blk := blocks.NewBlock([]byte("a block that outlives the process"))
	s1, err := store.Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := s1.Blocks().Put(ctx, blk); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := s1.KV().Set([]byte("k"), []byte("v"), nil); err != nil {
		t.Fatalf("kv Set: %v", err)
	}
	if err := s1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	s2 := mustOpen(t, path)
	got, err := s2.Blocks().Get(ctx, blk.Cid())
	if err != nil {
		t.Fatalf("Get after reopen: %v", err)
	}
	if string(got.RawData()) != string(blk.RawData()) {
		t.Errorf("block bytes: got %q want %q", got.RawData(), blk.RawData())
	}
	val, closer, err := s2.KV().Get([]byte("k"))
	if err != nil {
		t.Fatalf("kv Get after reopen: %v", err)
	}
	if string(val) != "v" {
		t.Errorf("kv value: got %q want %q", val, "v")
	}
	if err := closer.Close(); err != nil {
		t.Fatalf("kv closer: %v", err)
	}
}

// TestShardFuncVerification: an existing store created with a different shard
// function is refused, not silently reinterpreted.
func TestShardFuncVerification(t *testing.T) {
	tests := []struct {
		name     string
		sharding string
		wantErr  bool
		wantGot  string
	}{
		{name: "matching", sharding: store.ShardFunc + "\n"},
		{
			name:     "different suffix length",
			sharding: "/repo/flatfs/shard/v1/next-to-last/2\n",
			wantErr:  true,
			wantGot:  "/repo/flatfs/shard/v1/next-to-last/2",
		},
		{
			name:     "different shard function",
			sharding: "/repo/flatfs/shard/v1/prefix/3\n",
			wantErr:  true,
			wantGot:  "/repo/flatfs/shard/v1/prefix/3",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := t.TempDir()
			blocksPath := filepath.Join(path, "blocks")
			if err := os.MkdirAll(blocksPath, 0o755); err != nil {
				t.Fatalf("mkdir: %v", err)
			}
			if err := os.WriteFile(filepath.Join(blocksPath, "SHARDING"), []byte(tt.sharding), 0o644); err != nil {
				t.Fatalf("writing SHARDING: %v", err)
			}

			s, err := store.Open(path)
			if s != nil {
				t.Cleanup(func() { _ = s.Close() })
			}
			if !tt.wantErr {
				if err != nil {
					t.Fatalf("Open: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatal("Open: want a shard mismatch error, got nil")
			}
			var mismatch *store.ShardMismatchError
			if !errors.As(err, &mismatch) {
				t.Fatalf("Open error: got %v (%T), want *store.ShardMismatchError", err, err)
			}
			if mismatch.Got != tt.wantGot {
				t.Errorf("Got: got %q want %q", mismatch.Got, tt.wantGot)
			}
			if mismatch.Want != store.ShardFunc {
				t.Errorf("Want: got %q want %q", mismatch.Want, store.ShardFunc)
			}
		})
	}
}

// TestOpenExistingKeepsShardFunc: reopening a store bloar created never
// rewrites or re-derives its shard function.
func TestOpenExistingKeepsShardFunc(t *testing.T) {
	path := t.TempDir()
	s1, err := store.Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := s1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	before, err := os.ReadFile(filepath.Join(path, "blocks", "SHARDING"))
	if err != nil {
		t.Fatalf("reading SHARDING: %v", err)
	}

	s2 := mustOpen(t, path)
	_ = s2
	after, err := os.ReadFile(filepath.Join(path, "blocks", "SHARDING"))
	if err != nil {
		t.Fatalf("reading SHARDING after reopen: %v", err)
	}
	if string(before) != string(after) {
		t.Errorf("SHARDING changed on reopen: %q -> %q", before, after)
	}
}

func TestCloseIsIdempotent(t *testing.T) {
	s, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for i := 1; i <= 3; i++ {
		if err := s.Close(); err != nil {
			t.Errorf("Close #%d: %v", i, err)
		}
	}
}

func TestOpenErrors(t *testing.T) {
	file := filepath.Join(t.TempDir(), "not-a-dir")
	if err := os.WriteFile(file, []byte("x"), 0o644); err != nil {
		t.Fatalf("writing file: %v", err)
	}

	tests := []struct {
		name string
		path string
	}{
		{name: "empty path", path: ""},
		{name: "path is a file", path: file},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, err := store.Open(tt.path)
			if err == nil {
				_ = s.Close()
				t.Fatalf("Open(%q): want error, got nil", tt.path)
			}
		})
	}
}
