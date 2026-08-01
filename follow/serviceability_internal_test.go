package follow

import (
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
)

func TestServiceabilityCallbackFailureRetriesCurrentDocumentAdmission(t *testing.T) {
	document, err := cid.Prefix{
		Version:  1,
		Codec:    cid.Raw,
		MhType:   multihash.SHA2_256,
		MhLength: 32,
	}.Sum([]byte("current admitted publication"))
	if err != nil {
		t.Fatalf("document CID: %v", err)
	}
	f := &Follower{
		cfg: Config{OnServiceabilityChanged: func() error {
			return errors.New("injected pointer refresh failure")
		}},
		log:               slog.New(slog.NewTextHandler(io.Discard, nil)),
		admittedDocuments: map[string]cid.Cid{"": document, "writer-b": document},
	}

	f.notifyServiceabilityChanged()
	if len(f.admittedDocuments) != 0 {
		t.Fatalf("admitted document markers = %v, want empty for same-CID retry", f.admittedDocuments)
	}
}
