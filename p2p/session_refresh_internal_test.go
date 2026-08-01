package p2p

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ipfs/boxo/exchange"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
)

type refreshTestFactory struct {
	mu       sync.Mutex
	sessions []*refreshTestSession
	created  chan *refreshTestSession
}

func (f *refreshTestFactory) newSession(ctx context.Context) exchange.Fetcher {
	f.mu.Lock()
	defer f.mu.Unlock()
	s := &refreshTestSession{
		ctx:     ctx,
		started: make(chan struct{}),
		release: make(chan struct{}),
		block:   blocks.NewBlock([]byte(fmt.Sprintf("session-%d", len(f.sessions)))),
	}
	if len(f.sessions) > 0 {
		close(s.release)
	}
	f.sessions = append(f.sessions, s)
	f.created <- s
	return s
}

func (f *refreshTestFactory) snapshot() []*refreshTestSession {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*refreshTestSession(nil), f.sessions...)
}

type refreshTestSession struct {
	ctx       context.Context
	started   chan struct{}
	startOnce sync.Once
	release   chan struct{}
	block     blocks.Block
}

func (s *refreshTestSession) GetBlock(ctx context.Context, _ cid.Cid) (blocks.Block, error) {
	s.startOnce.Do(func() { close(s.started) })
	select {
	case <-s.release:
		return s.block, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-s.ctx.Done():
		return nil, s.ctx.Err()
	}
}

func (s *refreshTestSession) GetBlocks(ctx context.Context, cids []cid.Cid) (<-chan blocks.Block, error) {
	out := make(chan blocks.Block)
	go func() {
		defer close(out)
		for _, c := range cids {
			block, err := s.GetBlock(ctx, c)
			if err != nil {
				return
			}
			select {
			case out <- block:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out, nil
}

// TestRefreshingFetcherRetiresAfterInflight proves the synchronization
// contract separately from Bitswap: refresh is lazy, a new caller moves to the
// new epoch immediately, and the old session is canceled only after its final
// in-flight call returns.
func TestRefreshingFetcherRetiresAfterInflight(t *testing.T) {
	var epoch atomic.Uint64
	epoch.Store(1)
	factory := &refreshTestFactory{created: make(chan *refreshTestSession, 4)}
	parent, cancel := context.WithCancel(t.Context())
	defer cancel()
	fetcher := &refreshingFetcher{
		ctx: parent, epoch: epoch.Load, newSession: factory.newSession,
	}
	want := blocks.NewBlock([]byte("wanted")).Cid()

	firstResult := make(chan error, 1)
	go func() {
		_, err := fetcher.GetBlock(t.Context(), want)
		firstResult <- err
	}()
	var first *refreshTestSession
	select {
	case <-time.After(time.Second):
		t.Fatal("first session was not created")
	case first = <-factory.created:
	}
	select {
	case <-first.started:
	case <-time.After(time.Second):
		t.Fatal("first session fetch did not start")
	}

	epoch.Add(1)
	if _, err := fetcher.GetBlock(t.Context(), want); err != nil {
		t.Fatalf("new-epoch fetch: %v", err)
	}
	if got := len(factory.snapshot()); got != 2 {
		t.Fatalf("sessions after refresh = %d, want 2", got)
	}
	select {
	case <-first.ctx.Done():
		t.Fatal("refresh canceled the in-flight old session")
	default:
	}

	close(first.release)
	if err := <-firstResult; err != nil {
		t.Fatalf("old in-flight fetch: %v", err)
	}
	select {
	case <-first.ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("retired session was not canceled after its final release")
	}

	// Several refresh boundaries before another request create one generation,
	// not one unused session per boundary.
	epoch.Add(3)
	if got := len(factory.snapshot()); got != 2 {
		t.Fatalf("refresh eagerly created sessions: got %d, want 2", got)
	}
	if _, err := fetcher.GetBlock(t.Context(), want); err != nil {
		t.Fatalf("fetch after repeated refresh: %v", err)
	}
	if got := len(factory.snapshot()); got != 3 {
		t.Fatalf("sessions after repeated lazy refresh = %d, want 3", got)
	}
}

// TestRefreshingFetcherRetainsOpenGetBlocks covers the channel-shaped Fetcher
// contract: the generation lease lasts until the returned channel closes, not
// merely until the inner GetBlocks method returns.
func TestRefreshingFetcherRetainsOpenGetBlocks(t *testing.T) {
	var epoch atomic.Uint64
	epoch.Store(1)
	factory := &refreshTestFactory{created: make(chan *refreshTestSession, 4)}
	parent, cancel := context.WithCancel(t.Context())
	defer cancel()
	fetcher := &refreshingFetcher{
		ctx: parent, epoch: epoch.Load, newSession: factory.newSession,
	}
	want := blocks.NewBlock([]byte("wanted through a channel")).Cid()

	out, err := fetcher.GetBlocks(t.Context(), []cid.Cid{want})
	if err != nil {
		t.Fatalf("opening first GetBlocks: %v", err)
	}
	first := <-factory.created
	select {
	case <-first.started:
	case <-time.After(time.Second):
		t.Fatal("first GetBlocks request did not start")
	}

	epoch.Add(1)
	if _, err := fetcher.GetBlock(t.Context(), want); err != nil {
		t.Fatalf("new-epoch fetch: %v", err)
	}
	select {
	case <-first.ctx.Done():
		t.Fatal("refresh canceled an open GetBlocks generation")
	default:
	}

	close(first.release)
	var received int
	for range out {
		received++
	}
	if received != 1 {
		t.Fatalf("GetBlocks delivered %d blocks, want 1", received)
	}
	select {
	case <-first.ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("retired GetBlocks generation was not canceled after channel close")
	}
}
