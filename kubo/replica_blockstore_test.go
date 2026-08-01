package kubo_test

import (
	"context"
	"errors"
	"testing"

	blockstore "github.com/ipfs/boxo/blockstore"
	blocks "github.com/ipfs/go-block-format"
	ds "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/sync"

	"github.com/blobarchive/bloar/kubo"
)

func TestReplicaBlockstoreIsAppendOnlyAndNonEnumerating(t *testing.T) {
	inner := blockstore.NewBlockstore(sync.MutexWrap(ds.NewMapDatastore()))
	view, err := kubo.NewReplicaBlockstore(inner)
	if err != nil {
		t.Fatal(err)
	}
	block := blocks.NewBlock([]byte("replica"))
	if err := view.Put(t.Context(), block); err != nil {
		t.Fatal(err)
	}
	got, err := view.Get(t.Context(), block.Cid())
	if err != nil || !got.Cid().Equals(block.Cid()) {
		t.Fatalf("Get = %v, %v", got, err)
	}
	if err := view.DeleteBlock(t.Context(), block.Cid()); !errors.Is(err, kubo.ErrReplicaDeleteForbidden) {
		t.Fatalf("DeleteBlock error = %v", err)
	}
	if _, err := view.AllKeysChan(t.Context()); !errors.Is(err, kubo.ErrReplicaEnumerationForbidden) {
		t.Fatalf("AllKeysChan error = %v", err)
	}
	if has, err := inner.Has(context.Background(), block.Cid()); err != nil || !has {
		t.Fatalf("forbidden delete reached shared store: has=%t err=%v", has, err)
	}
}

func TestReplicaBlockstoreRejectsNil(t *testing.T) {
	if _, err := kubo.NewReplicaBlockstore(nil); err == nil {
		t.Fatal("nil inner accepted")
	}
}
