package server_test

import (
	"testing"

	"github.com/blobarchive/bloar/archive"
	"github.com/blobarchive/bloar/catalog"
	"github.com/blobarchive/bloar/core"
	"github.com/blobarchive/bloar/server"
	"github.com/blobarchive/bloar/store"
)

// TestHeadsOnDoc covers the hook the IPNS channel of spec 8.1 hangs off.
//
// The property that matters is not that the hook fires but what it is handed:
// spec 8.1 has the writer store the document as a block "so that both channels
// carry byte-identical documents", which is only true if the bytes given here
// are the bytes GET /bloar/v1/heads serves. A hook that was handed a
// re-rendered document, or that went and read Doc() a moment later, would
// publish a block whose contents the HTTPS channel disagrees with -- and the
// signature covers the bytes, so the disagreement would be unresolvable.
func TestHeadsOnDoc(t *testing.T) {
	ctx := t.Context()
	st, err := store.Open(t.TempDir(), store.WithPebbleLogger(quietPebble{}))
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() {
		if err := st.Close(); err != nil {
			t.Errorf("closing store: %v", err)
		}
	})

	var docs [][]byte
	roots := server.NewRootStore(st.KV())
	heads, err := server.NewHeads(server.HeadsConfig{
		Net:   testNet,
		Roots: roots,
		OnDoc: func(doc []byte) { docs = append(docs, doc) },
	})
	if err != nil {
		t.Fatalf("server.NewHeads: %v", err)
	}

	// The registry rebuilds on construction, so the hook has already fired: a
	// node with no heads still publishes a document, and IPNS still names it.
	if len(docs) != 1 {
		t.Fatalf("OnDoc fired %d times during NewHeads, want 1", len(docs))
	}
	if string(docs[0]) != string(heads.Doc()) {
		t.Errorf("OnDoc was handed %q, want the served document %q", docs[0], heads.Doc())
	}

	cache, err := core.NewNodeCacheMB(1)
	if err != nil {
		t.Fatalf("core.NewNodeCacheMB: %v", err)
	}
	head, err := server.OpenHead(ctx, archive.Config{
		Blocks:   st.Blocks(),
		Resolver: catalog.New(st.KV()),
		Cache:    cache,
	}, roots, archive.Params{
		Name: testHead, Net: testNet, OriginSlot: testOrigin, SegBits: testSegBits, FanoutBits: testFanout,
	})
	if err != nil {
		t.Fatalf("server.OpenHead: %v", err)
	}
	if err := heads.Add(head); err != nil {
		t.Fatalf("Heads.Add: %v", err)
	}

	if len(docs) != 2 {
		t.Fatalf("OnDoc fired %d times, want 2 (construction and the Add)", len(docs))
	}
	if string(docs[1]) != string(heads.Doc()) {
		t.Errorf("OnDoc was handed %q, want the served document %q", docs[1], heads.Doc())
	}
	if string(docs[0]) == string(docs[1]) {
		t.Error("adding a head did not change the document the hook was handed")
	}
}

// TestHeadsNilOnDoc: the hook is optional, which is what a node with no IPNS
// channel needs it to be.
func TestHeadsNilOnDoc(t *testing.T) {
	st, err := store.Open(t.TempDir(), store.WithPebbleLogger(quietPebble{}))
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() {
		if err := st.Close(); err != nil {
			t.Errorf("closing store: %v", err)
		}
	})

	heads, err := server.NewHeads(server.HeadsConfig{Net: testNet, Roots: server.NewRootStore(st.KV())})
	if err != nil {
		t.Fatalf("server.NewHeads: %v", err)
	}
	if len(heads.Doc()) == 0 {
		t.Error("a registry with no OnDoc published no document")
	}
}
