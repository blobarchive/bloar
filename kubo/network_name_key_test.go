package kubo_test

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	ma "github.com/multiformats/go-multiaddr"

	"github.com/blobarchive/bloar/kubo"
)

func mustMultiaddr(t *testing.T, value string) ma.Multiaddr {
	t.Helper()
	address, err := ma.NewMultiaddr(value)
	if err != nil {
		t.Fatalf("NewMultiaddr(%q): %v", value, err)
	}
	return address
}

func TestNetworkNameAndKeyRPCContract(t *testing.T) {
	remoteID := testPeerID(t)
	nameID := testPeerID(t)
	generatedID := testPeerID(t)
	target := testBlock(t, cid.Raw, "published target").Cid()
	peerAddress := mustMultiaddr(t, "/ip4/203.0.113.7/tcp/4001/p2p/"+remoteID.String())
	sequence := uint64(7)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.Header.Get("Authorization") != "Bearer "+testToken {
			t.Errorf("request = %s, auth %q", r.Method, r.Header.Get("Authorization"))
		}
		query := r.URL.Query()
		switch r.URL.Path {
		case "/api/v0/swarm/peers":
			for key, want := range map[string]string{
				"verbose": "false", "streams": "false", "latency": "false", "direction": "true", "identify": "false",
			} {
				if query.Get(key) != want {
					t.Errorf("swarm/peers %s = %q, want %q", key, query.Get(key), want)
				}
			}
			writeJSON(t, w, map[string]any{"Peers": []map[string]any{{
				"Addr": "/ip4/203.0.113.7/tcp/4001", "Peer": remoteID.String(), "Direction": 2,
				"Identify": map[string]any{"Addresses": nil, "AgentVersion": "", "ID": "", "Protocols": nil, "PublicKey": ""},
			}}})
		case "/api/v0/swarm/connect":
			if query.Get("arg") != peerAddress.String() {
				t.Errorf("swarm/connect arg = %q", query.Get("arg"))
			}
			writeJSON(t, w, map[string]any{"Strings": []string{"connect " + remoteID.String() + " success"}})
		case "/api/v0/name/resolve":
			if query.Get("arg") != nameID.String() || query.Get("recursive") != "true" || query.Get("stream") != "false" {
				t.Errorf("name/resolve query = %v", query)
			}
			writeJSON(t, w, map[string]string{"Path": "/ipfs/" + target.String()})
		case "/api/v0/name/publish":
			for key, want := range map[string]string{
				"arg": "/ipfs/" + target.String(), "key": "publisher", "resolve": "false", "lifetime": "48h0m0s",
				"ttl": "5m0s", "sequence": "7", "v1compat": "true", "allow-offline": "false", "allow-delegated": "false",
			} {
				if query.Get(key) != want {
					t.Errorf("name/publish %s = %q, want %q", key, query.Get(key), want)
				}
			}
			writeJSON(t, w, map[string]string{"Name": nameID.String(), "Value": "/ipfs/" + target.String()})
		case "/api/v0/key/ls":
			if query.Get("l") != "true" || query.Get("ipns-base") != "b58mh" {
				t.Errorf("key/ls query = %v", query)
			}
			writeJSON(t, w, map[string]any{"Keys": []map[string]string{
				{"Name": "self", "Id": nameID.String()}, {"Name": "publisher", "Id": generatedID.String()},
			}})
		case "/api/v0/key/gen":
			if query.Get("arg") != "new-key" || query.Get("type") != "ed25519" || query.Has("size") {
				t.Errorf("key/gen query = %v", query)
			}
			writeJSON(t, w, map[string]string{"Name": "new-key", "Id": generatedID.String()})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client := newClient(t, server.URL, nil)

	peers, err := client.SwarmPeers(t.Context())
	if err != nil || len(peers) != 1 || peers[0].Peer != remoteID || peers[0].Direction != kubo.SwarmOutbound {
		t.Fatalf("SwarmPeers = %+v, %v", peers, err)
	}
	connected, err := client.SwarmConnect(t.Context(), peerAddress)
	if err != nil || connected != remoteID {
		t.Fatalf("SwarmConnect = %s, %v", connected, err)
	}
	resolved, err := client.NameResolve(t.Context(), nameID)
	if err != nil || !resolved.Equals(target) {
		t.Fatalf("NameResolve = %s, %v", resolved, err)
	}
	published, err := client.NamePublish(t.Context(), target, kubo.NamePublishOptions{
		Key: "publisher", Lifetime: 48 * time.Hour, TTL: 5 * time.Minute, Sequence: &sequence,
	})
	if err != nil || published.Name != nameID || !published.Value.Equals(target) {
		t.Fatalf("NamePublish = %+v, %v", published, err)
	}
	keys, err := client.KeyList(t.Context())
	if err != nil || len(keys) != 2 || keys[0].Name != "self" || keys[1].ID != generatedID {
		t.Fatalf("KeyList = %+v, %v", keys, err)
	}
	generated, err := client.KeyGenerate(t.Context(), "new-key")
	if err != nil || generated.Name != "new-key" || generated.ID != generatedID {
		t.Fatalf("KeyGenerate = %+v, %v", generated, err)
	}
}

func TestSwarmPeersRejectsMalformedEntries(t *testing.T) {
	remoteID := testPeerID(t)
	base := func() map[string]any {
		return map[string]any{"Addr": "/ip4/203.0.113.8/tcp/4001", "Peer": remoteID.String(), "Direction": 1}
	}
	tests := []struct {
		name  string
		peers []map[string]any
	}{
		{name: "bad peer", peers: []map[string]any{{"Addr": "/ip4/203.0.113.8/tcp/4001", "Peer": "bad", "Direction": 1}}},
		{name: "bad address", peers: []map[string]any{{"Addr": "bad", "Peer": remoteID.String(), "Direction": 1}}},
		{name: "bad direction", peers: []map[string]any{{"Addr": "/ip4/203.0.113.8/tcp/4001", "Peer": remoteID.String(), "Direction": 9}}},
		{name: "disabled metadata", peers: []map[string]any{{"Addr": "/ip4/203.0.113.8/tcp/4001", "Peer": remoteID.String(), "Direction": 1, "Latency": "1ms"}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				writeJSON(t, w, map[string]any{"Peers": test.peers})
			}))
			defer server.Close()
			_, err := newClient(t, server.URL, nil).SwarmPeers(t.Context())
			var protocol *kubo.ProtocolError
			if !errors.As(err, &protocol) {
				t.Fatalf("error = %T %v, want ProtocolError", err, err)
			}
		})
	}

	// Kubo reports live libp2p connections, not a unique peer/address set.
	// Simultaneous connections can legitimately share both values.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(t, w, map[string]any{"Peers": []map[string]any{base(), base()}})
	}))
	defer server.Close()
	peers, err := newClient(t, server.URL, nil).SwarmPeers(t.Context())
	if err != nil || len(peers) != 2 {
		t.Fatalf("duplicate live connections = %+v, %v; want two entries", peers, err)
	}
}

func TestNameAndKeyResponsesMustMatchRequest(t *testing.T) {
	target := testBlock(t, cid.Raw, "name target").Cid()
	other := testBlock(t, cid.Raw, "other target").Cid()
	nameID := testPeerID(t)
	tests := []struct {
		name     string
		path     string
		response any
		call     func(*kubo.Client) error
	}{
		{
			name: "resolve suffix", path: "/api/v0/name/resolve", response: map[string]string{"Path": "/ipfs/" + target.String() + "/suffix"},
			call: func(c *kubo.Client) error { _, err := c.NameResolve(t.Context(), nameID); return err },
		},
		{
			name: "publish CID mismatch", path: "/api/v0/name/publish", response: map[string]string{"Name": nameID.String(), "Value": "/ipfs/" + other.String()},
			call: func(c *kubo.Client) error {
				_, err := c.NamePublish(t.Context(), target, kubo.NamePublishOptions{Key: "self", Lifetime: time.Hour, TTL: time.Minute})
				return err
			},
		},
		{
			name: "generated name mismatch", path: "/api/v0/key/gen", response: map[string]string{"Name": "other", "Id": nameID.String()},
			call: func(c *kubo.Client) error { _, err := c.KeyGenerate(t.Context(), "wanted"); return err },
		},
		{
			name: "duplicate key name", path: "/api/v0/key/ls", response: map[string]any{"Keys": []map[string]string{
				{"Name": "same", "Id": nameID.String()}, {"Name": "same", "Id": nameID.String()},
			}},
			call: func(c *kubo.Client) error { _, err := c.KeyList(t.Context()); return err },
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != test.path {
					t.Errorf("path = %s, want %s", r.URL.Path, test.path)
				}
				writeJSON(t, w, test.response)
			}))
			defer server.Close()
			err := test.call(newClient(t, server.URL, nil))
			var protocol *kubo.ProtocolError
			if !errors.As(err, &protocol) {
				t.Fatalf("error = %T %v, want ProtocolError", err, err)
			}
		})
	}
}

func TestNetworkNameKeyInputsFailBeforeNetwork(t *testing.T) {
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { requests.Add(1) }))
	defer server.Close()
	client := newClient(t, server.URL, nil)
	target := testBlock(t, cid.Raw, "input target").Cid()

	if _, err := client.SwarmConnect(t.Context(), mustMultiaddr(t, "/ip4/127.0.0.1/tcp/4001")); err == nil {
		t.Fatal("SwarmConnect accepted an address without peer ID")
	}
	if _, err := client.NamePublish(t.Context(), target, kubo.NamePublishOptions{Key: "bad key", Lifetime: time.Hour, TTL: time.Minute}); err == nil {
		t.Fatal("NamePublish accepted an unsafe key name")
	}
	if _, err := client.NamePublish(t.Context(), target, kubo.NamePublishOptions{Key: "self", Lifetime: time.Minute, TTL: time.Hour}); err == nil {
		t.Fatal("NamePublish accepted TTL over Lifetime")
	}
	if _, err := client.KeyGenerate(t.Context(), "self"); err == nil {
		t.Fatal("KeyGenerate accepted self")
	}
	if _, err := client.KeyGenerate(t.Context(), "bad/name"); err == nil {
		t.Fatal("KeyGenerate accepted an unsafe name")
	}
	if got := requests.Load(); got != 0 {
		t.Fatalf("invalid calls issued %d requests", got)
	}
}

func TestNetworkCollectionsRespectConfiguredBounds(t *testing.T) {
	remoteID := testPeerID(t)
	for name, path := range map[string]string{"peers": "/api/v0/swarm/peers", "keys": "/api/v0/key/ls"} {
		t.Run(name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != path {
					t.Errorf("path = %s, want %s", r.URL.Path, path)
				}
				if name == "peers" {
					writeJSON(t, w, map[string]any{"Peers": []map[string]any{{"Addr": "/ip4/203.0.113.1/tcp/1", "Peer": remoteID.String(), "Direction": 1}}})
				} else {
					writeJSON(t, w, map[string]any{"Keys": []map[string]string{{"Name": "self", "Id": remoteID.String()}}})
				}
			}))
			defer server.Close()
			client := newClient(t, server.URL, func(c *kubo.Config) { c.MaxStreamBytes = 16 })
			var err error
			if name == "peers" {
				_, err = client.SwarmPeers(t.Context())
			} else {
				_, err = client.KeyList(t.Context())
			}
			var protocol *kubo.ProtocolError
			if !errors.As(err, &protocol) || !strings.Contains(err.Error(), "limit") {
				t.Fatalf("error = %T %v, want bounded ProtocolError", err, err)
			}
		})
	}
}

func TestNetworkCollectionsEnforceItemsWhileDecoding(t *testing.T) {
	remoteID := testPeerID(t)
	for name, test := range map[string]struct {
		response any
		call     func(*kubo.Client) error
	}{
		"peers": {
			response: map[string]any{"Peers": []map[string]any{
				{"Addr": "/ip4/203.0.113.1/tcp/1", "Peer": remoteID.String(), "Direction": 1},
				{"Addr": "/ip4/203.0.113.2/tcp/2", "Peer": remoteID.String(), "Direction": 2},
			}},
			call: func(c *kubo.Client) error { _, err := c.SwarmPeers(t.Context()); return err },
		},
		"keys": {
			response: map[string]any{"Keys": []map[string]string{
				{"Name": "first", "Id": remoteID.String()},
				{"Name": "second", "Id": remoteID.String()},
			}},
			call: func(c *kubo.Client) error { _, err := c.KeyList(t.Context()); return err },
		},
	} {
		t.Run(name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				writeJSON(t, w, test.response)
			}))
			defer server.Close()
			client := newClient(t, server.URL, func(c *kubo.Config) { c.MaxStreamItems = 1 })
			err := test.call(client)
			var protocol *kubo.ProtocolError
			if !errors.As(err, &protocol) || !strings.Contains(err.Error(), "more than 1 items") {
				t.Fatalf("error = %T %v, want one-item ProtocolError", err, err)
			}
		})
	}
}

func TestSwarmConnectRejectsWrongAcknowledgement(t *testing.T) {
	id := testPeerID(t)
	address := mustMultiaddr(t, fmt.Sprintf("/ip4/203.0.113.9/tcp/4001/p2p/%s", id))
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(t, w, map[string]any{"Strings": []string{"connect someone-else success"}})
	}))
	defer server.Close()
	_, err := newClient(t, server.URL, nil).SwarmConnect(t.Context(), address)
	var protocol *kubo.ProtocolError
	if !errors.As(err, &protocol) {
		t.Fatalf("error = %T %v, want ProtocolError", err, err)
	}
}
