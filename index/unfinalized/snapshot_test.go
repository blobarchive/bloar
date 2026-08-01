package unfinalized

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/blobarchive/bloar/index/upstream"
)

type fakeSource struct {
	head            upstream.BeaconHeader
	finalized       upstream.BeaconHeader
	headOK          bool
	finalizedOK     bool
	headReads       int
	commitmentReads int
	headErr         error
	changeHead      *upstream.BeaconHeader
	headers         map[[32]byte]upstream.BeaconHeader
	commitments     map[[32]byte][][48]byte
}

type orderedSource struct {
	*fakeSource
	calls []string
}

func (s *orderedSource) Head(ctx context.Context) (upstream.BeaconHeader, bool, error) {
	s.calls = append(s.calls, "head")
	return s.fakeSource.Head(ctx)
}

func (s *orderedSource) FinalizedHeader(ctx context.Context) (upstream.BeaconHeader, bool, error) {
	s.calls = append(s.calls, "finalized")
	return s.fakeSource.FinalizedHeader(ctx)
}

func (f *fakeSource) Head(context.Context) (upstream.BeaconHeader, bool, error) {
	f.headReads++
	if f.headErr != nil {
		return upstream.BeaconHeader{}, false, f.headErr
	}
	if f.changeHead != nil && f.headReads > 1 {
		return *f.changeHead, f.headOK, nil
	}
	return f.head, f.headOK, nil
}

func (f *fakeSource) FinalizedHeader(context.Context) (upstream.BeaconHeader, bool, error) {
	return f.finalized, f.finalizedOK, nil
}

func (f *fakeSource) HeaderByRoot(_ context.Context, root [32]byte) (upstream.BeaconHeader, error) {
	h, ok := f.headers[root]
	if !ok {
		return upstream.BeaconHeader{}, fmt.Errorf("missing root %x", root)
	}
	return h, nil
}

func (f *fakeSource) CommitmentsByRoot(_ context.Context, root [32]byte) ([][48]byte, error) {
	f.commitmentReads++
	return slices.Clone(f.commitments[root]), nil
}

func root(n byte) [32]byte { return [32]byte{31: n} }

func fixtureSource() *fakeSource {
	h10 := upstream.BeaconHeader{Slot: 10, Root: root(10), ParentRoot: root(9), Finalized: true}
	h12 := upstream.BeaconHeader{Slot: 12, Root: root(12), ParentRoot: h10.Root}
	h13 := upstream.BeaconHeader{Slot: 13, Root: root(13), ParentRoot: h12.Root}
	h15 := upstream.BeaconHeader{Slot: 15, Root: root(15), ParentRoot: h13.Root}
	return &fakeSource{
		head: h15, finalized: h10, headOK: true, finalizedOK: true,
		headers: map[[32]byte]upstream.BeaconHeader{
			h10.Root: h10, h12.Root: h12, h13.Root: h13, h15.Root: h15,
		},
		commitments: map[[32]byte][][48]byte{
			h13.Root: {{47: 13}},
			h15.Root: {{47: 15}, {47: 16}},
		},
	}
}

func TestBuildCompleteBoundedSnapshot(t *testing.T) {
	source := fixtureSource()
	snap, err := Build(context.Background(), source, 10, 8)
	if err != nil {
		t.Fatal(err)
	}
	if snap.WindowStart != 10 || snap.SyncedTo != 15 || snap.Head.Root != root(15) || snap.Finalized.Root != root(10) {
		t.Fatalf("snapshot bounds/anchors = %+v", snap)
	}
	wantSlots := []uint64{10, 12, 13, 15}
	gotSlots := make([]uint64, len(snap.Blocks))
	for i, b := range snap.Blocks {
		gotSlots[i] = b.Slot
	}
	if !slices.Equal(gotSlots, wantSlots) {
		t.Fatalf("block slots = %v, want %v", gotSlots, wantSlots)
	}
	if len(snap.Rows) != 2 || snap.Rows[0].Slot != 13 || len(snap.Rows[0].VHs) != 1 ||
		snap.Rows[1].Slot != 15 || len(snap.Rows[1].VHs) != 2 {
		t.Fatalf("rows = %+v", snap.Rows)
	}
	for vh, slot := range snap.Locations {
		if slot != 13 && slot != 15 {
			t.Fatalf("vh %x location = %d", vh, slot)
		}
	}
	if err := StableHead(context.Background(), source, snap.Head.Root); err != nil {
		t.Fatalf("stable head: %v", err)
	}
}

func TestBuildReadsFinalizedAnchorBeforeMovingHead(t *testing.T) {
	source := &orderedSource{fakeSource: fixtureSource()}
	if _, err := Build(context.Background(), source, 10, 8); err != nil {
		t.Fatal(err)
	}
	want := []string{"finalized", "head"}
	if len(source.calls) < len(want) || !slices.Equal(source.calls[:len(want)], want) {
		t.Fatalf("initial source reads = %v, want %v", source.calls, want)
	}
}

func TestBuildRejectsUnsafeAncestryAndBounds(t *testing.T) {
	t.Run("handoff blocked", func(t *testing.T) {
		_, err := Build(context.Background(), fixtureSource(), 1, 8)
		if !errors.Is(err, ErrHandoffBlocked) {
			t.Fatalf("error = %v, want ErrHandoffBlocked", err)
		}
	})

	t.Run("finalized root mismatch", func(t *testing.T) {
		s := fixtureSource()
		s.finalized.Root = root(99)
		if _, err := Build(context.Background(), s, 10, 8); err == nil || errors.Is(err, ErrSnapshotChanged) {
			t.Fatalf("error = %v, want fatal finalized-root mismatch", err)
		}
	})

	t.Run("finalized slot skipped by candidate ancestry", func(t *testing.T) {
		s := fixtureSource()
		s.finalized = upstream.BeaconHeader{Slot: 11, Root: root(99), Finalized: true}
		if _, err := Build(context.Background(), s, 10, 8); err == nil || errors.Is(err, ErrSnapshotChanged) {
			t.Fatalf("error = %v, want fatal missing finalized anchor", err)
		}
	})

	t.Run("nondecreasing parent", func(t *testing.T) {
		s := fixtureSource()
		bad := s.headers[root(13)]
		bad.Slot = 15
		s.headers[root(13)] = bad
		if _, err := Build(context.Background(), s, 10, 8); err == nil {
			t.Fatal("nondecreasing parent accepted")
		}
	})

	t.Run("loop", func(t *testing.T) {
		s := fixtureSource()
		h := s.headers[root(13)]
		h.ParentRoot = root(15)
		s.headers[root(13)] = h
		if _, err := Build(context.Background(), s, 10, 8); err == nil {
			t.Fatal("ancestry loop accepted")
		}
	})

	t.Run("finalized immediately below window", func(t *testing.T) {
		s := fixtureSource()
		snap, err := Build(context.Background(), s, 11, 8)
		if err != nil {
			t.Fatalf("snapshot should prove the below-window anchor: %v", err)
		}
		if len(snap.Blocks) == 0 || snap.Blocks[0].Slot != 12 {
			t.Fatalf("below-window finalized block leaked into snapshot: %+v", snap.Blocks)
		}
	})

	t.Run("retained overlap extends below finalized anchor", func(t *testing.T) {
		s := fixtureSource()
		s.finalized = s.headers[root(12)]
		s.finalized.Finalized = true
		snap, err := Build(context.Background(), s, 10, 8)
		if err != nil {
			t.Fatalf("snapshot should retain overlap below finality: %v", err)
		}
		if snap.Finalized.Slot != 12 || len(snap.Blocks) == 0 || snap.Blocks[0].Slot != 10 {
			t.Fatalf("snapshot anchor/overlap = finalized %d blocks %+v", snap.Finalized.Slot, snap.Blocks)
		}
	})
}

func TestStableHeadDetectsConcurrentChange(t *testing.T) {
	s := fixtureSource()
	changed := s.head
	changed.Root = root(42)
	s.changeHead = &changed
	snap, err := Build(context.Background(), s, 10, 8)
	if err != nil {
		t.Fatal(err)
	}
	if err := StableHead(context.Background(), s, snap.Head.Root); err == nil {
		t.Fatal("head change during snapshot was accepted")
	}
}

func TestWindowStartIsUnderflowSafe(t *testing.T) {
	tests := []struct {
		origin, handoff, overlap, want uint64
	}{
		{8, 12, 4, 9},
		{8, 1, 8, 8},
		{0, 0, 0, 1},
		{0, ^uint64(0), 4, ^uint64(0) - 4},
	}
	for _, tt := range tests {
		if got := WindowStart(tt.origin, tt.handoff, tt.overlap); got != tt.want {
			t.Errorf("WindowStart(%d,%d,%d) = %d, want %d", tt.origin, tt.handoff, tt.overlap, got, tt.want)
		}
	}
}
