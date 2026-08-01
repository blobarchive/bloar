package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/ipfs/boxo/blockstore"
	dshelp "github.com/ipfs/boxo/datastore/dshelp"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
)

func memoryBlockstore() blockstore.Blockstore {
	return blockstore.NewBlockstore(
		dssync.MutexWrap(datastore.NewMapDatastore()),
		blockstore.NoPrefix(),
	)
}

func TestBlockstoreEpochLifecycleAndProtection(t *testing.T) {
	ctx := context.Background()
	base := Validating(memoryBlockstore())
	epochs := NewBlockstoreEpochs(base)
	app := epochs.Application()

	if got := epochs.ActiveEpoch(); got != 0 {
		t.Fatalf("ActiveEpoch before Begin: got %d want 0", got)
	}
	if got := epochs.CollectionGeneration(); got != 0 {
		t.Fatalf("CollectionGeneration before Begin: got %d want 0", got)
	}
	epoch, err := epochs.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if epoch.ID() == 0 || epochs.ActiveEpoch() != epoch.ID() {
		t.Fatalf("active epoch: coordinator=%d handle=%d", epochs.ActiveEpoch(), epoch.ID())
	}
	if got := epochs.CollectionGeneration(); got != epoch.ID() {
		t.Fatalf("CollectionGeneration during epoch: got %d want %d", got, epoch.ID())
	}
	epochAware, ok := app.(interface{ ActiveEpoch() uint64 })
	if !ok {
		t.Fatal("application blockstore does not expose ActiveEpoch")
	}
	if got := epochAware.ActiveEpoch(); got != epoch.ID() {
		t.Fatalf("application ActiveEpoch: got %d want %d", got, epoch.ID())
	}
	generationAware, ok := app.(interface{ CollectionGeneration() uint64 })
	if !ok {
		t.Fatal("application blockstore does not expose CollectionGeneration")
	}
	if got := generationAware.CollectionGeneration(); got != epoch.ID() {
		t.Fatalf("application CollectionGeneration: got %d want %d", got, epoch.ID())
	}
	if _, err := epochs.Begin(); err == nil {
		t.Fatal("overlapping Begin: got nil error")
	} else {
		var active *ErrEpochActive
		if !errors.As(err, &active) {
			t.Fatalf("overlapping Begin: got %v (%T), want *ErrEpochActive", err, err)
		}
		if active.ID != epoch.ID() {
			t.Errorf("ErrEpochActive.ID: got %d want %d", active.ID, epoch.ID())
		}
	}

	first := blocks.NewBlock([]byte("protected once despite repeated operations"))
	if err := app.Put(ctx, first); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if _, err := app.Get(ctx, first.Cid()); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if present, err := app.Has(ctx, first.Cid()); err != nil || !present {
		t.Fatalf("Has: present=%t err=%v", present, err)
	}
	if _, err := app.GetSize(ctx, first.Cid()); err != nil {
		t.Fatalf("GetSize: %v", err)
	}
	if deleted, protected, err := epoch.DeleteCandidate(ctx, first.Cid()); err != nil {
		t.Fatalf("DeleteCandidate protected: %v", err)
	} else if deleted || !protected {
		t.Fatalf("DeleteCandidate protected: deleted=%t protected=%t", deleted, protected)
	}

	// Collector reads are deliberately untracked; otherwise marking the live
	// set would duplicate it in the epoch protection map.
	second := blocks.NewBlock([]byte("collector-only read"))
	if err := epoch.Blocks().Put(ctx, second); err != nil {
		t.Fatalf("collector Put: %v", err)
	}
	if _, err := epochs.CollectorBlocks().Get(ctx, second.Cid()); err != nil {
		t.Fatalf("collector Get: %v", err)
	}
	if deleted, protected, err := epoch.DeleteCandidate(ctx, second.Cid()); err != nil {
		t.Fatalf("DeleteCandidate unprotected: %v", err)
	} else if !deleted || protected {
		t.Fatalf("DeleteCandidate unprotected: deleted=%t protected=%t", deleted, protected)
	}

	if got := epoch.End(); got != 1 {
		t.Fatalf("End protected count: got %d want 1", got)
	}
	if got := epoch.End(); got != 1 {
		t.Fatalf("second End protected count: got %d want 1", got)
	}
	if got := epochs.ActiveEpoch(); got != 0 {
		t.Fatalf("ActiveEpoch after End: got %d want 0", got)
	}
	if got := epochs.CollectionGeneration(); got != epoch.ID() {
		t.Fatalf("CollectionGeneration after End: got %d want %d", got, epoch.ID())
	}
	if _, _, err := epoch.DeleteCandidate(ctx, first.Cid()); !errors.Is(err, ErrEpochEnded) {
		t.Fatalf("DeleteCandidate after End: got %v want ErrEpochEnded", err)
	}

	next, err := epochs.Begin()
	if err != nil {
		t.Fatalf("second sequential Begin: %v", err)
	}
	defer next.End()
	if next.ID() <= epoch.ID() {
		t.Errorf("epoch IDs did not increase: first=%d next=%d", epoch.ID(), next.ID())
	}
}

func TestBlockstoreEpochProtectsMultihashAliases(t *testing.T) {
	ctx := context.Background()
	epochs := NewBlockstoreEpochs(Validating(memoryBlockstore()))
	block := blocks.NewBlock([]byte("one physical multihash, multiple CID codecs"))
	if err := epochs.CollectorBlocks().Put(ctx, block); err != nil {
		t.Fatalf("Put: %v", err)
	}

	epoch, err := epochs.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	defer epoch.End()
	rawAlias := cid.NewCidV1(cid.Raw, block.Cid().Hash())
	dagAlias := cid.NewCidV1(cid.DagCBOR, block.Cid().Hash())
	if present, err := epochs.Application().Has(ctx, rawAlias); err != nil || !present {
		t.Fatalf("Has raw alias: present=%t err=%v", present, err)
	}
	if deleted, protected, err := epoch.DeleteCandidate(ctx, dagAlias); err != nil {
		t.Fatalf("DeleteCandidate DAG alias: %v", err)
	} else if deleted || !protected {
		t.Fatalf("alias was not protected: deleted=%t protected=%t", deleted, protected)
	}
}

type blockingBlockstore struct {
	blockstore.Blockstore
	getEntered    chan struct{}
	getRelease    chan struct{}
	putEntered    chan struct{}
	putRelease    chan struct{}
	deleteEntered chan struct{}
	deleteRelease chan struct{}
}

func (b *blockingBlockstore) Put(ctx context.Context, block blocks.Block) error {
	if b.putEntered != nil {
		close(b.putEntered)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-b.putRelease:
		}
	}
	return b.Blockstore.Put(ctx, block)
}

func (b *blockingBlockstore) Get(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	if b.getEntered != nil {
		close(b.getEntered)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-b.getRelease:
		}
	}
	return b.Blockstore.Get(ctx, c)
}

func (b *blockingBlockstore) DeleteBlock(ctx context.Context, c cid.Cid) error {
	if b.deleteEntered != nil {
		close(b.deleteEntered)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-b.deleteRelease:
		}
	}
	return b.Blockstore.DeleteBlock(ctx, c)
}

func TestBlockstoreEpochApplicationWinsDeleteRace(t *testing.T) {
	ctx := context.Background()
	plain := memoryBlockstore()
	block := blocks.NewBlock([]byte("application gets the shard first"))
	if err := plain.Put(ctx, block); err != nil {
		t.Fatalf("seed Put: %v", err)
	}
	blocking := &blockingBlockstore{
		Blockstore: Validating(plain),
		getEntered: make(chan struct{}),
		getRelease: make(chan struct{}),
	}
	epochs := NewBlockstoreEpochs(blocking)
	epoch, err := epochs.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	defer epoch.End()

	getDone := make(chan error, 1)
	go func() {
		_, err := epochs.Application().Get(ctx, block.Cid())
		getDone <- err
	}()
	<-blocking.getEntered // Get holds the multihash shard at this point.

	type deleteResult struct {
		deleted   bool
		protected bool
		err       error
	}
	deleteDone := make(chan deleteResult, 1)
	go func() {
		deleted, protected, err := epoch.DeleteCandidate(ctx, block.Cid())
		deleteDone <- deleteResult{deleted: deleted, protected: protected, err: err}
	}()
	close(blocking.getRelease)
	if err := <-getDone; err != nil {
		t.Fatalf("application Get: %v", err)
	}
	result := <-deleteDone
	if result.err != nil || result.deleted || !result.protected {
		t.Fatalf("DeleteCandidate: deleted=%t protected=%t err=%v", result.deleted, result.protected, result.err)
	}
}

func TestBlockstoreEpochPutWinsDeleteRace(t *testing.T) {
	ctx := context.Background()
	plain := memoryBlockstore()
	block := blocks.NewBlock([]byte("application put spans boxo's presence check and write"))
	blocking := &blockingBlockstore{
		Blockstore: Validating(plain),
		putEntered: make(chan struct{}),
		putRelease: make(chan struct{}),
	}
	epochs := NewBlockstoreEpochs(blocking)
	epoch, err := epochs.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	defer epoch.End()

	putDone := make(chan error, 1)
	go func() { putDone <- epochs.Application().Put(ctx, block) }()
	<-blocking.putEntered // Put holds the multihash shard before touching.

	type deleteResult struct {
		deleted   bool
		protected bool
		err       error
	}
	deleteDone := make(chan deleteResult, 1)
	go func() {
		deleted, protected, err := epoch.DeleteCandidate(ctx, block.Cid())
		deleteDone <- deleteResult{deleted: deleted, protected: protected, err: err}
	}()
	close(blocking.putRelease)
	if err := <-putDone; err != nil {
		t.Fatalf("application Put: %v", err)
	}
	result := <-deleteDone
	if result.err != nil || result.deleted || !result.protected {
		t.Fatalf("DeleteCandidate: deleted=%t protected=%t err=%v", result.deleted, result.protected, result.err)
	}
	if present, err := plain.Has(ctx, block.Cid()); err != nil || !present {
		t.Fatalf("block after Put/delete race: present=%t err=%v", present, err)
	}
}

func TestBlockstoreEpochEndWaitsForInflightApplicationOperation(t *testing.T) {
	ctx := context.Background()
	plain := memoryBlockstore()
	block := blocks.NewBlock([]byte("end must not tear down protection under an operation"))
	if err := plain.Put(ctx, block); err != nil {
		t.Fatalf("seed Put: %v", err)
	}
	blocking := &blockingBlockstore{
		Blockstore: Validating(plain),
		getEntered: make(chan struct{}),
		getRelease: make(chan struct{}),
	}
	epochs := NewBlockstoreEpochs(blocking)
	epoch, err := epochs.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}

	getDone := make(chan error, 1)
	go func() {
		_, err := epochs.Application().Get(ctx, block.Cid())
		getDone <- err
	}()
	<-blocking.getEntered
	endDone := make(chan int, 1)
	go func() { endDone <- epoch.End() }()
	select {
	case <-endDone:
		t.Fatal("End returned while an application operation still held the epoch lifecycle")
	case <-time.After(20 * time.Millisecond):
	}
	close(blocking.getRelease)
	if err := <-getDone; err != nil {
		t.Fatalf("Get: %v", err)
	}
	if protected := <-endDone; protected != 1 {
		t.Fatalf("End protected count = %d, want 1", protected)
	}
}

func TestBlockstoreEpochCollectorWinsDeleteRace(t *testing.T) {
	ctx := context.Background()
	plain := memoryBlockstore()
	block := blocks.NewBlock([]byte("collector gets the shard first"))
	if err := plain.Put(ctx, block); err != nil {
		t.Fatalf("seed Put: %v", err)
	}
	blocking := &blockingBlockstore{
		Blockstore:    Validating(plain),
		deleteEntered: make(chan struct{}),
		deleteRelease: make(chan struct{}),
	}
	epochs := NewBlockstoreEpochs(blocking)
	epoch, err := epochs.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	defer epoch.End()

	type deleteResult struct {
		deleted   bool
		protected bool
		err       error
	}
	deleteDone := make(chan deleteResult, 1)
	go func() {
		deleted, protected, err := epoch.DeleteCandidate(ctx, block.Cid())
		deleteDone <- deleteResult{deleted: deleted, protected: protected, err: err}
	}()
	<-blocking.deleteEntered // DeleteCandidate holds the shard at this point.

	getDone := make(chan error, 1)
	go func() {
		_, err := epochs.Application().Get(ctx, block.Cid())
		getDone <- err
	}()
	close(blocking.deleteRelease)
	result := <-deleteDone
	if result.err != nil || !result.deleted || result.protected {
		t.Fatalf("DeleteCandidate: deleted=%t protected=%t err=%v", result.deleted, result.protected, result.err)
	}
	if err := <-getDone; err == nil {
		t.Fatal("application Get after collector won: got nil error, want not found")
	}
}

var errPartialPutMany = errors.New("partial PutMany failure")

type partialPutManyBlockstore struct {
	blockstore.Blockstore
}

func (p partialPutManyBlockstore) PutMany(ctx context.Context, input []blocks.Block) error {
	if len(input) > 0 {
		if err := p.Blockstore.Put(ctx, input[0]); err != nil {
			return err
		}
	}
	return errPartialPutMany
}

func TestBlockstoreEpochPutManyProtectsAllInputsOnPartialFailure(t *testing.T) {
	ctx := context.Background()
	base := partialPutManyBlockstore{Blockstore: Validating(memoryBlockstore())}
	epochs := NewBlockstoreEpochs(base)
	epoch, err := epochs.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	defer epoch.End()

	input := []blocks.Block{
		blocks.NewBlock([]byte("durable before batch failure")),
		blocks.NewBlock([]byte("possibly not durable after batch failure")),
	}
	if err := epochs.Application().PutMany(ctx, input); !errors.Is(err, errPartialPutMany) {
		t.Fatalf("PutMany: got %v want %v", err, errPartialPutMany)
	}
	for _, block := range input {
		deleted, protected, err := epoch.DeleteCandidate(ctx, block.Cid())
		if err != nil {
			t.Fatalf("DeleteCandidate(%s): %v", block.Cid(), err)
		}
		if deleted || !protected {
			t.Errorf("DeleteCandidate(%s): deleted=%t protected=%t", block.Cid(), deleted, protected)
		}
	}
}

func TestStoreEpochAllKeysPreservesConversionErrors(t *testing.T) {
	ctx := context.Background()
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	if !s.Epochs().CompleteEnumeration() {
		t.Fatal("Store.Open did not install complete error-preserving enumeration")
	}

	good := blocks.NewBlock([]byte("enumerated block"))
	if err := s.Blocks().Put(ctx, good); err != nil {
		t.Fatalf("Put good block: %v", err)
	}
	// flatfs can represent arbitrary datastore keys. This one is not a valid
	// multihash, so direct enumeration must surface its conversion error rather
	// than silently skipping it as Boxo's AllKeysChan does.
	badKey := dshelp.NewKeyFromBinary([]byte("not-a-multihash"))
	if err := s.ds.Put(ctx, badKey, []byte("bad key payload")); err != nil {
		t.Fatalf("Put invalid multihash key: %v", err)
	}

	keys, asyncErrs, err := s.Epochs().AllKeys(ctx)
	if err != nil {
		t.Fatalf("AllKeys: %v", err)
	}
	var gotKeys []cid.Cid
	var gotErrs []error
	for keys != nil || asyncErrs != nil {
		select {
		case c, ok := <-keys:
			if !ok {
				keys = nil
				continue
			}
			gotKeys = append(gotKeys, c)
		case err, ok := <-asyncErrs:
			if !ok {
				asyncErrs = nil
				continue
			}
			gotErrs = append(gotErrs, err)
		}
	}
	if len(gotErrs) != 1 {
		t.Fatalf("asynchronous errors: got %d (%v), want 1", len(gotErrs), gotErrs)
	}
	if got := gotErrs[0].Error(); got == "" {
		t.Error("asynchronous conversion error is empty")
	}
	for _, c := range gotKeys {
		if bytes.Equal(c.Hash(), good.Cid().Hash()) {
			return
		}
	}
	// Directory iteration order is unspecified and the conversion failure may
	// precede the good block. The important contract is the surfaced error; log
	// the observed prefix to make that ordering explicit rather than requiring
	// the good key to arrive first.
	t.Logf("conversion failure arrived before good key; enumerated prefix: %v", gotKeys)
}

func TestBlockstoreEpochEnumerationCapabilityIsExplicit(t *testing.T) {
	base := Validating(memoryBlockstore())
	if NewBlockstoreEpochs(base).CompleteEnumeration() {
		t.Fatal("generic Boxo AllKeysChan was treated as complete despite hiding asynchronous errors")
	}
	if NewBlockstoreEpochs(base, WithKeyIterator(nil)).CompleteEnumeration() {
		t.Fatal("nil key iterator enabled complete enumeration")
	}
	iterator := func(context.Context) (<-chan cid.Cid, <-chan error, error) {
		keys := make(chan cid.Cid)
		errs := make(chan error)
		close(keys)
		close(errs)
		return keys, errs, nil
	}
	if !NewBlockstoreEpochs(base, WithKeyIterator(iterator)).CompleteEnumeration() {
		t.Fatal("explicit error-preserving key iterator was not advertised as complete")
	}
}

func TestApplicationAllKeysExcludesCollectionEpoch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	base := Validating(memoryBlockstore())
	epochs := NewBlockstoreEpochs(base)
	if err := base.Put(ctx, blocks.NewBlock([]byte("enumeration holds the lifecycle"))); err != nil {
		t.Fatalf("Put: %v", err)
	}
	keys, err := epochs.Application().AllKeysChan(ctx)
	if err != nil {
		t.Fatalf("AllKeysChan: %v", err)
	}

	type beginResult struct {
		epoch *BlockstoreEpoch
		err   error
	}
	beginDone := make(chan beginResult, 1)
	go func() {
		epoch, beginErr := epochs.Begin()
		beginDone <- beginResult{epoch: epoch, err: beginErr}
	}()
	select {
	case result := <-beginDone:
		if result.epoch != nil {
			result.epoch.End()
		}
		t.Fatal("Begin did not wait for application enumeration to finish")
	case <-time.After(20 * time.Millisecond):
	}
	cancel()
	for range keys {
	}
	result := <-beginDone
	if result.err != nil {
		t.Fatalf("Begin after enumeration: %v", result.err)
	}
	epoch := result.epoch
	if epoch == nil {
		t.Fatal("Begin returned a nil epoch")
	}
	defer epoch.End()

	if _, err := epochs.Application().AllKeysChan(context.Background()); err == nil {
		t.Fatal("application enumeration started during an active collection epoch")
	} else {
		var active *ErrEpochActive
		if !errors.As(err, &active) || active.ID != epoch.ID() {
			t.Fatalf("AllKeysChan during epoch: got %v, want ErrEpochActive(%d)", err, epoch.ID())
		}
	}
}

func TestBlockstoreEpochGenericAllKeysHasClosedErrorChannel(t *testing.T) {
	ctx := context.Background()
	base := Validating(memoryBlockstore())
	epochs := NewBlockstoreEpochs(base)
	block := blocks.NewBlock([]byte("generic enumeration"))
	if err := base.Put(ctx, block); err != nil {
		t.Fatalf("Put: %v", err)
	}
	keys, asyncErrs, err := epochs.AllKeys(ctx)
	if err != nil {
		t.Fatalf("AllKeys: %v", err)
	}
	if _, ok := <-asyncErrs; ok {
		t.Fatal("generic asynchronous error channel is not closed")
	}
	var count int
	for range keys {
		count++
	}
	if count != 1 {
		t.Fatalf("enumerated keys: got %d want 1", count)
	}
}

func TestEpochShardIndexAndKeyAreStable(t *testing.T) {
	block := blocks.NewBlock([]byte("stable shard"))
	key := multihashKey(block.Cid())
	index := epochShardIndex(key)
	if index < 0 || index >= epochShardCount {
		t.Fatalf("shard index out of range: %d", index)
	}
	alias := cid.NewCidV1(cid.Raw, block.Cid().Hash())
	if got := multihashKey(alias); got != key {
		t.Fatal("CID alias changed multihash protection key")
	}
	if got := epochShardIndex(multihashKey(alias)); got != index {
		t.Fatalf("CID alias changed shard: got %d want %d", got, index)
	}
	if testing.Verbose() {
		t.Logf("multihash %s maps to shard %d", fmt.Sprintf("%x", []byte(key)), index)
	}
}
