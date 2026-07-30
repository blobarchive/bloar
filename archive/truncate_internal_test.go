package archive

import (
	"context"
	"strings"
	"testing"

	"github.com/ipfs/boxo/blockstore"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"

	"github.com/blobarchive/bloar/schema"
)

func TestTruncateRejectsNonRawSurvivingBlobLink(t *testing.T) {
	bs := blockstore.NewBlockstore(dssync.MutexWrap(datastore.NewMapDatastore()))
	h := &Head{cfg: Config{Blocks: bs}}
	raw := blocks.NewBlock([]byte("malformed segment child"))
	nonRaw := cid.NewCidV1(cid.DagCBOR, raw.Cid().Hash())
	rows := []schema.Row{{Slot: 42, Entries: []schema.RefEntry{{Blob: nonRaw}}}}

	err := h.touchTruncatedBlobs(context.Background(), rows)
	if err == nil {
		t.Fatal("touchTruncatedBlobs accepted a defined non-raw RefEntry.Blob")
	}
	if msg := err.Error(); !strings.Contains(msg, nonRaw.String()) || !strings.Contains(msg, "want raw") {
		t.Fatalf("error = %q, want CID and raw-codec refusal", msg)
	}
}
