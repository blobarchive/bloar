package kubo

import (
	"context"
	"errors"
	"testing"

	"github.com/ipfs/go-cid"
)

func TestBufferedCIDSnapshotFailsClosedOnCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	result, err := bufferedCIDSnapshot(ctx, []cid.Cid{cid.Undef, cid.Undef})
	if result != nil {
		t.Fatal("canceled snapshot returned a partial channel")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
}
