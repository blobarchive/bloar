package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/blobarchive/bloar/p2p"
	"github.com/blobarchive/bloar/server"
)

func TestDaemonBitswapServesOnlyAdmittedUpstreamDocumentFromVerifiedLayer(t *testing.T) {
	upstreamPublic, upstreamPrivate := pointerKey(t)
	dir := t.TempDir()
	cfg := loadString(t, fmt.Sprintf(`
net: %s
beacon: {genesis_time: 1}
store: {path: %s}
server: {auth_token_file: /test/token}
follow:
  url: https://upstream.invalid/bloar/v1/heads
  pubkey: %s
  heads:
    followed: {pin: {mode: full}}
p2p:
  listen: ["/ip4/127.0.0.1/tcp/0"]
  nat_port_map: false
  dht: {bootstrap: private}
  rendezvous: {enabled: false}
`, pointerTestNet, filepath.Join(dir, "store"), hex.EncodeToString(upstreamPublic)))
	st := openStore(t, cfg.Store.Path)
	defer st.Close()

	n, err := setupP2PWithDeps(t.Context(), cfg, st, nil, nil, newLogger(), p2pSetupDeps{
		publicBootstrapPeers: func() []peer.AddrInfo {
			t.Fatal("private static test consulted public bootstrap peers")
			return nil
		},
	})
	if err != nil {
		t.Fatalf("setupP2PWithDeps: %v", err)
	}
	defer n.close(newLogger())
	if n.exchange == nil || n.documents == nil || n.pointers == nil {
		t.Fatalf("pointer-serving stack incomplete: exchange=%t documents=%t pointers=%t",
			n.exchange != nil, n.documents != nil, n.pointers != nil)
	}

	clientHost, err := p2p.NewHost(t.Context(), p2p.HostConfig{
		Listen:          []string{"/ip4/127.0.0.1/tcp/0"},
		IdentityKeyFile: filepath.Join(t.TempDir(), "client.key"),
	})
	if err != nil {
		t.Fatalf("client NewHost: %v", err)
	}
	defer clientHost.Close()
	clientBlocks := blockstore.NewBlockstore(dssync.MutexWrap(datastore.NewMapDatastore()))
	clientExchange, err := p2p.NewExchange(t.Context(), p2p.ExchangeConfig{Host: clientHost, Blocks: clientBlocks})
	if err != nil {
		t.Fatalf("client NewExchange: %v", err)
	}
	defer clientExchange.Close()
	if err := clientHost.Libp2p().Connect(t.Context(), peer.AddrInfo{
		ID: n.host.ID(), Addrs: n.host.Libp2p().Addrs(),
	}); err != nil {
		t.Fatalf("connect client to daemon: %v", err)
	}
	fetching := p2p.FetchingBlockstore(t.Context(), clientBlocks, clientExchange)

	root := pointerTestCID(t, "served-followed-root")
	doc, raw, document := pointerSignedDocument(t, upstreamPrivate, pointerTestNet, nil,
		pointerEntry(t, "followed", root.String(), ""))
	reader := pointerDocumentReader(t, doc)
	if err := n.pointers.AdmitFollowedDocument(reader, document, doc); err != nil {
		t.Fatalf("AdmitFollowedDocument: %v", err)
	}
	if has, err := st.Blocks().Has(t.Context(), document.Cid()); err != nil || has {
		t.Fatalf("archive blockstore has admitted document = %t, %v; want false (verified layer owns it)", has, err)
	}
	if has, err := n.documents.HasVerified(t.Context(), document.Cid()); err != nil || !has {
		t.Fatalf("verified document eligibility = %t, %v; want true, nil", has, err)
	}
	fetchCtx, cancelFetch := context.WithTimeout(t.Context(), 5*time.Second)
	got, err := fetching.Get(fetchCtx, document.Cid())
	cancelFetch()
	if err != nil {
		t.Fatalf("fetch admitted publication document through daemon Bitswap: %v", err)
	}
	if !got.Cid().Equals(document.Cid()) || string(got.RawData()) != string(raw) {
		t.Fatalf("served publication document = %s %q, want %s exact admitted bytes",
			got.Cid(), got.RawData(), document.Cid())
	}

	// A syntactically valid document with an invalid signature reaches the
	// daemon boundary but is rejected before the verified serving layer changes.
	rejected := doc
	rejected.UpdatedAt = "2026-07-22T00:01:00Z"
	rejected.Signature = strings.Repeat("00", 64)
	rejectedRaw, err := json.Marshal(rejected)
	if err != nil {
		t.Fatalf("marshal rejected document: %v", err)
	}
	rejectedBlock, err := p2p.NewDocumentBlock(rejectedRaw)
	if err != nil {
		t.Fatalf("hash rejected document: %v", err)
	}
	if err := n.pointers.AdmitFollowedDocument(reader, rejectedBlock, rejected); err == nil {
		t.Fatal("invalidly signed document was admitted")
	}
	assertDocumentNotServed(t, n, fetching, rejectedBlock.Cid())

	// Bytes that are not even a publication document likewise never cross the
	// trust boundary or enter the real exchange's serving view.
	malformedBlock, err := p2p.NewDocumentBlock([]byte("{not-json"))
	if err != nil {
		t.Fatalf("hash malformed document: %v", err)
	}
	if err := n.pointers.AdmitFollowedDocument(reader, malformedBlock, server.Doc{}); err == nil {
		t.Fatal("malformed document was admitted")
	}
	assertDocumentNotServed(t, n, fetching, malformedBlock.Cid())
}

func assertDocumentNotServed(t *testing.T, n *p2pStack, fetching blockstore.Blockstore, document cid.Cid) {
	t.Helper()
	if has, err := n.documents.HasVerified(t.Context(), document); err != nil || has {
		t.Fatalf("rejected document %s verified eligibility = %t, %v; want false, nil", document, has, err)
	}
	if has, err := n.documents.Has(t.Context(), document); err != nil || has {
		t.Fatalf("rejected document %s is in daemon serving view = %t, %v; want false, nil", document, has, err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 400*time.Millisecond)
	defer cancel()
	if _, err := fetching.Get(ctx, document); err == nil {
		t.Fatalf("rejected document %s was served over Bitswap", document)
	}
}
