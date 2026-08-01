package kubo_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multihash"

	"github.com/blobarchive/bloar/kubo"
)

const testToken = "test-secret-token_123"

func newClient(t *testing.T, base string, configure func(*kubo.Config)) *kubo.Client {
	t.Helper()
	cfg := kubo.Config{BaseURL: base, BearerToken: testToken}
	if configure != nil {
		configure(&cfg)
	}
	client, err := kubo.New(cfg)
	if err != nil {
		t.Fatalf("kubo.New: %v", err)
	}
	return client
}

func testBlock(t *testing.T, codec uint64, value string) blocks.Block {
	t.Helper()
	prefix := cid.Prefix{Version: 1, Codec: codec, MhType: multihash.SHA2_256, MhLength: 32}
	c, err := prefix.Sum([]byte(value))
	if err != nil {
		t.Fatalf("CID: %v", err)
	}
	block, err := blocks.NewBlockWithCid([]byte(value), c)
	if err != nil {
		t.Fatalf("block: %v", err)
	}
	return block
}

func testPeerID(t *testing.T) peer.ID {
	t.Helper()
	_, public, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateEd25519Key: %v", err)
	}
	id, err := peer.IDFromPublicKey(public)
	if err != nil {
		t.Fatalf("IDFromPublicKey: %v", err)
	}
	return id
}

func writeJSON(t *testing.T, w http.ResponseWriter, value any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(value); err != nil {
		t.Errorf("encoding response: %v", err)
	}
}

func TestCoreRPCContract(t *testing.T) {
	target := testBlock(t, cid.DagCBOR, "the exact dag-cbor block bytes")
	localID := testPeerID(t)
	const prefix = "/kubo-proxy"

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method = %s, want POST", r.Method)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer "+testToken {
			t.Errorf("Authorization = %q", got)
		}
		if strings.Contains(r.RequestURI, testToken) {
			t.Errorf("bearer token leaked into request URI %q", r.RequestURI)
		}
		if got := r.URL.Query().Get("encoding"); got != "json" {
			t.Errorf("encoding = %q, want json", got)
		}

		switch r.URL.Path {
		case prefix + "/api/v0/version":
			if got := r.Header.Get("Accept"); got != "application/json" {
				t.Errorf("version Accept = %q", got)
			}
			writeJSON(t, w, map[string]any{
				"Version": "0.42.0", "Commit": "abc", "Repo": "19", "System": "amd64/linux", "Golang": "go1.26",
			})
		case prefix + "/api/v0/id":
			writeJSON(t, w, map[string]any{
				"ID": localID.String(), "PublicKey": "opaque-key", "Addresses": []string{"/ip4/127.0.0.1/tcp/4001"},
				"AgentVersion": "kubo/0.42.0", "Protocols": []string{"/ipfs/id/1.0.0"},
			})
		case prefix + "/api/v0/block/put":
			query := r.URL.Query()
			for key, want := range map[string]string{
				"cid-codec": "dag-cbor", "mhtype": "sha2-256", "mhlen": "32", "pin": "false", "allow-big-block": "false",
			} {
				if got := query.Get(key); got != want {
					t.Errorf("block/put %s = %q, want %q", key, got, want)
				}
			}
			if err := r.ParseMultipartForm(3 << 20); err != nil {
				t.Errorf("ParseMultipartForm: %v", err)
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			file, header, err := r.FormFile("file")
			if err != nil {
				t.Errorf("FormFile: %v", err)
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			defer file.Close()
			if header.Filename != "block" {
				t.Errorf("filename = %q, want block", header.Filename)
			}
			got, err := io.ReadAll(io.LimitReader(file, kubo.DefaultMaxBlockBytes+1))
			if err != nil {
				t.Errorf("reading upload: %v", err)
			}
			if !bytes.Equal(got, target.RawData()) {
				t.Errorf("uploaded bytes = %q", got)
			}
			writeJSON(t, w, map[string]any{"Key": target.Cid().String(), "Size": len(got)})
		case prefix + "/api/v0/block/get":
			if got := r.Header.Get("Accept"); got != "application/vnd.ipld.raw" {
				t.Errorf("block/get Accept = %q", got)
			}
			if got := r.URL.Query().Get("arg"); got != target.Cid().String() {
				t.Errorf("block/get arg = %q", got)
			}
			w.Header().Set("Content-Type", "application/vnd.ipld.raw")
			_, _ = w.Write(target.RawData())
		case prefix + "/api/v0/block/stat":
			writeJSON(t, w, map[string]any{"Key": target.Cid().String(), "Size": len(target.RawData())})
		case prefix + "/api/v0/block/rm":
			if got := r.URL.Query().Get("force"); got != "false" {
				t.Errorf("block/rm force = %q", got)
			}
			writeJSON(t, w, map[string]any{"Hash": target.Cid().String(), "Error": ""})
		default:
			http.Error(w, "unexpected endpoint", http.StatusNotFound)
		}
	}))
	defer server.Close()

	client := newClient(t, server.URL+prefix+"/", nil)
	version, err := client.Version(t.Context())
	if err != nil {
		t.Fatalf("Version: %v", err)
	}
	if version.Version != "0.42.0" || version.Repo != "19" || version.Golang != "go1.26" {
		t.Errorf("Version = %+v", version)
	}
	identity, err := client.ID(t.Context())
	if err != nil {
		t.Fatalf("ID: %v", err)
	}
	if identity.ID != localID || len(identity.Addresses) != 1 || identity.AgentVersion != "kubo/0.42.0" {
		t.Errorf("ID = %+v", identity)
	}
	put, err := client.BlockPut(t.Context(), target)
	if err != nil {
		t.Fatalf("BlockPut: %v", err)
	}
	if !put.CID.Equals(target.Cid()) || put.Size != int64(len(target.RawData())) {
		t.Errorf("BlockPut = %+v", put)
	}
	got, err := client.BlockGet(t.Context(), target.Cid())
	if err != nil {
		t.Fatalf("BlockGet: %v", err)
	}
	if !bytes.Equal(got.RawData(), target.RawData()) {
		t.Errorf("BlockGet bytes = %q", got.RawData())
	}
	stat, err := client.BlockStat(t.Context(), target.Cid())
	if err != nil {
		t.Fatalf("BlockStat: %v", err)
	}
	if !stat.CID.Equals(target.Cid()) || stat.Size != int64(len(target.RawData())) {
		t.Errorf("BlockStat = %+v", stat)
	}
	if err := client.BlockRemove(t.Context(), target.Cid()); err != nil {
		t.Fatalf("BlockRemove: %v", err)
	}
}

func TestBaseURLValidationDoesNotEchoCredentials(t *testing.T) {
	tests := []struct {
		name string
		url  string
		cfg  func(*kubo.Config)
	}{
		{name: "empty"},
		{name: "relative", url: "localhost:5001"},
		{name: "wrong scheme", url: "ftp://localhost:5001"},
		{name: "missing host", url: "https:///api"},
		{name: "userinfo", url: "https://visible-user:password-must-not-leak@example.test"},
		{name: "query", url: "https://example.test?api_auth=password-must-not-leak"},
		{name: "fragment", url: "https://example.test/#fragment"},
		{name: "encoded path", url: "https://example.test/reverse%20proxy"},
		{name: "unclean path", url: "https://example.test/a/../b"},
		{name: "empty port", url: "https://example.test:"},
		{name: "zero port", url: "https://example.test:0"},
		{name: "out of range port", url: "https://example.test:65536"},
		{name: "public plaintext", url: "http://example.test"},
		{name: "malformed secret", url: "http://password-must-not-leak%zz"},
		{name: "negative timeout", url: "https://example.test", cfg: func(c *kubo.Config) { c.RequestTimeout = -1 }},
		{name: "negative block limit", url: "https://example.test", cfg: func(c *kubo.Config) { c.MaxBlockBytes = -1 }},
		{name: "oversized block limit", url: "https://example.test", cfg: func(c *kubo.Config) { c.MaxBlockBytes = kubo.DefaultMaxBlockBytes + 1 }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := kubo.Config{BaseURL: test.url, BearerToken: testToken}
			if test.cfg != nil {
				test.cfg(&cfg)
			}
			_, err := kubo.New(cfg)
			if err == nil {
				t.Fatal("New succeeded")
			}
			if strings.Contains(err.Error(), "password-must-not-leak") || strings.Contains(err.Error(), testToken) {
				t.Fatalf("configuration error leaked a credential: %v", err)
			}
		})
	}

	if _, err := kubo.New(kubo.Config{
		BaseURL:           "http://example.test",
		BearerToken:       testToken,
		AllowInsecureHTTP: true,
	}); err != nil {
		t.Fatalf("explicitly allowed plaintext: %v", err)
	}
}

func TestBearerTokenFileAndHeaderOnlyAuthentication(t *testing.T) {
	credential := filepath.Join(t.TempDir(), "kubo.token")
	if err := os.WriteFile(credential, []byte("file-token_456\n"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer file-token_456" {
			t.Errorf("Authorization = %q", got)
		}
		if strings.Contains(r.RequestURI, "file-token_456") {
			t.Errorf("token leaked into URL: %s", r.RequestURI)
		}
		writeJSON(t, w, map[string]string{"Version": "0.42.0"})
	}))
	defer server.Close()

	client, err := kubo.New(kubo.Config{BaseURL: server.URL, BearerTokenFile: credential})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, err := client.Version(t.Context()); err != nil {
		t.Fatalf("Version: %v", err)
	}

	for name, cfg := range map[string]kubo.Config{
		"missing":      {BaseURL: server.URL},
		"both":         {BaseURL: server.URL, BearerToken: testToken, BearerTokenFile: credential},
		"invalid":      {BaseURL: server.URL, BearerToken: "not a bearer token"},
		"padding only": {BaseURL: server.URL, BearerToken: "===="},
		"empty file": func() kubo.Config {
			path := filepath.Join(t.TempDir(), "empty")
			if err := os.WriteFile(path, nil, 0o600); err != nil {
				t.Fatalf("WriteFile: %v", err)
			}
			return kubo.Config{BaseURL: server.URL, BearerTokenFile: path}
		}(),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := kubo.New(cfg); err == nil {
				t.Fatal("New succeeded")
			}
		})
	}
}

func TestStatusNotFoundAndBearerRedaction(t *testing.T) {
	target := testBlock(t, cid.Raw, "missing target")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		writeJSON(t, w, map[string]any{
			"Message": "block was not found locally; echoed Authorization: " + r.Header.Get("Authorization"),
			"Code":    0,
			"Type":    "error-" + testToken,
		})
	}))
	defer server.Close()
	client := newClient(t, server.URL, nil)

	_, err := client.BlockGet(t.Context(), target.Cid())
	if !errors.Is(err, kubo.ErrNotFound) {
		t.Fatalf("BlockGet error = %v, want ErrNotFound", err)
	}
	var notFound *kubo.NotFoundError
	if !errors.As(err, &notFound) || notFound.CID != target.Cid().String() {
		t.Fatalf("NotFoundError = %#v", notFound)
	}
	var status *kubo.StatusError
	if !errors.As(err, &status) || status.Status != http.StatusInternalServerError || status.Type != "error-[REDACTED]" {
		t.Fatalf("StatusError = %#v", status)
	}
	if strings.Contains(err.Error(), testToken) || strings.Contains(status.Message, testToken) || strings.Contains(status.Type, testToken) {
		t.Fatalf("error leaked bearer token: %v", err)
	}
}

func TestNotFoundClassificationPrecedesBearerRedaction(t *testing.T) {
	target := testBlock(t, cid.Raw, "missing target")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		writeJSON(t, w, map[string]string{"Message": "blockstore: block not found"})
	}))
	defer server.Close()
	client := newClient(t, server.URL, func(c *kubo.Config) { c.BearerToken = "not" })

	_, err := client.BlockGet(t.Context(), target.Cid())
	if !errors.Is(err, kubo.ErrNotFound) {
		t.Fatalf("BlockGet error = %v, want ErrNotFound", err)
	}
	if strings.Contains(err.Error(), "blockstore: block not found") {
		t.Fatalf("error did not redact bearer token: %v", err)
	}
}

func TestNonNotFoundStatusStaysTypedAndBounded(t *testing.T) {
	target := testBlock(t, cid.Raw, "status target")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = io.WriteString(w, strings.Repeat("x", 64<<10+100))
	}))
	defer server.Close()
	client := newClient(t, server.URL, nil)

	_, err := client.BlockStat(t.Context(), target.Cid())
	var status *kubo.StatusError
	if !errors.As(err, &status) {
		t.Fatalf("error = %T %v, want StatusError", err, err)
	}
	if status.Status != http.StatusForbidden || !status.Truncated || len(status.Message) > 64<<10 {
		t.Fatalf("StatusError = %+v, message len %d", status, len(status.Message))
	}
	if errors.Is(err, kubo.ErrNotFound) {
		t.Fatalf("403 classified as not found: %v", err)
	}
}

func TestTruncatedStatusCannotEstablishBlockAbsence(t *testing.T) {
	target := testBlock(t, cid.Raw, "status-prefix-is-not-proof")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = io.WriteString(w, "blockstore: block not found "+strings.Repeat("x", 65<<10))
	}))
	defer server.Close()

	_, err := newClient(t, server.URL, nil).BlockGetLocal(t.Context(), target.Cid())
	var status *kubo.StatusError
	if !errors.As(err, &status) || !status.Truncated {
		t.Fatalf("error = %T %v, want truncated StatusError", err, err)
	}
	if errors.Is(err, kubo.ErrNotFound) {
		t.Fatalf("incomplete status response established block absence: %v", err)
	}
}

func TestMalformedTruncatedAndOversizedSuccessBodies(t *testing.T) {
	tests := []struct {
		name    string
		handler http.HandlerFunc
	}{
		{
			name: "malformed JSON",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, `{"Version":`)
			},
		},
		{
			name: "truncated declared body",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("Content-Length", "100")
				_, _ = io.WriteString(w, `{"Version":"0.42.0"}`)
			},
		},
		{
			name: "oversized metadata",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, strings.Repeat("x", 64<<10+1))
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(test.handler)
			defer server.Close()
			client := newClient(t, server.URL, nil)
			_, err := client.Version(t.Context())
			var protocol *kubo.ProtocolError
			if !errors.As(err, &protocol) {
				t.Fatalf("error = %T %v, want ProtocolError", err, err)
			}
		})
	}
}

func TestSuccessResponseContentTypeIsPinned(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = io.WriteString(w, `{"Version":"0.42.0"}`)
	}))
	defer server.Close()

	_, err := newClient(t, server.URL, nil).Version(t.Context())
	var protocol *kubo.ProtocolError
	if !errors.As(err, &protocol) || !strings.Contains(protocol.Problem, "Content-Type") {
		t.Fatalf("error = %T %v, want Content-Type ProtocolError", err, err)
	}
}

func TestBlockGetRejectsOversizeAndCIDMismatch(t *testing.T) {
	wanted := testBlock(t, cid.Raw, "wanted")
	other := testBlock(t, cid.Raw, "other bytes")
	for name, body := range map[string][]byte{
		"oversized":    []byte("12345"),
		"CID mismatch": other.RawData(),
	} {
		t.Run(name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/vnd.ipld.raw")
				_, _ = w.Write(body)
			}))
			defer server.Close()
			client := newClient(t, server.URL, func(c *kubo.Config) {
				if name == "oversized" {
					c.MaxBlockBytes = 4
				}
			})
			_, err := client.BlockGet(t.Context(), wanted.Cid())
			var protocol *kubo.ProtocolError
			if !errors.As(err, &protocol) {
				t.Fatalf("error = %T %v, want ProtocolError", err, err)
			}
		})
	}
}

func TestBlockResponsesMustNameExactCIDAndSize(t *testing.T) {
	wanted := testBlock(t, cid.Raw, "wanted block")
	other := testBlock(t, cid.Raw, "other block")
	tests := []struct {
		name     string
		endpoint string
		response map[string]any
		call     func(*kubo.Client) error
	}{
		{
			name: "stat CID mismatch", endpoint: "/api/v0/block/stat",
			response: map[string]any{"Key": other.Cid().String(), "Size": len(other.RawData())},
			call:     func(c *kubo.Client) error { _, err := c.BlockStat(t.Context(), wanted.Cid()); return err },
		},
		{
			name: "stat negative size", endpoint: "/api/v0/block/stat",
			response: map[string]any{"Key": wanted.Cid().String(), "Size": -1},
			call:     func(c *kubo.Client) error { _, err := c.BlockStat(t.Context(), wanted.Cid()); return err },
		},
		{
			name: "stat missing size", endpoint: "/api/v0/block/stat",
			response: map[string]any{"Key": wanted.Cid().String()},
			call:     func(c *kubo.Client) error { _, err := c.BlockStat(t.Context(), wanted.Cid()); return err },
		},
		{
			name: "put CID mismatch", endpoint: "/api/v0/block/put",
			response: map[string]any{"Key": other.Cid().String(), "Size": len(wanted.RawData())},
			call:     func(c *kubo.Client) error { _, err := c.BlockPut(t.Context(), wanted); return err },
		},
		{
			name: "put size mismatch", endpoint: "/api/v0/block/put",
			response: map[string]any{"Key": wanted.Cid().String(), "Size": len(wanted.RawData()) + 1},
			call:     func(c *kubo.Client) error { _, err := c.BlockPut(t.Context(), wanted); return err },
		},
		{
			name: "remove CID mismatch", endpoint: "/api/v0/block/rm",
			response: map[string]any{"Hash": other.Cid().String()},
			call:     func(c *kubo.Client) error { return c.BlockRemove(t.Context(), wanted.Cid()) },
		},
		{
			name: "remove error CID mismatch", endpoint: "/api/v0/block/rm",
			response: map[string]any{"Hash": other.Cid().String(), "Error": "blockstore: block not found"},
			call:     func(c *kubo.Client) error { return c.BlockRemove(t.Context(), wanted.Cid()) },
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != test.endpoint {
					t.Errorf("path = %s, want %s", r.URL.Path, test.endpoint)
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

func TestOversizedCIDArgumentsFailBeforeNetwork(t *testing.T) {
	digest := bytes.Repeat([]byte("x"), 768)
	hash, err := multihash.Encode(digest, multihash.IDENTITY)
	if err != nil {
		t.Fatalf("identity multihash: %v", err)
	}
	oversized := cid.NewCidV1(cid.Raw, hash)
	if len(oversized.String()) <= 512 {
		t.Fatalf("test CID text is only %d bytes", len(oversized.String()))
	}
	oversizedBlock, err := blocks.NewBlockWithCid(digest, oversized)
	if err != nil {
		t.Fatalf("NewBlockWithCid: %v", err)
	}

	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		requests.Add(1)
	}))
	defer server.Close()
	client := newClient(t, server.URL, nil)

	calls := map[string]func() error{
		"get":       func() error { _, err := client.BlockGet(t.Context(), oversized); return err },
		"local get": func() error { _, err := client.BlockGetLocal(t.Context(), oversized); return err },
		"fetch":     func() error { _, err := client.BlockFetch(t.Context(), oversized); return err },
		"stat":      func() error { _, err := client.BlockStat(t.Context(), oversized); return err },
		"remove":    func() error { return client.BlockRemove(t.Context(), oversized) },
		"pin add":   func() error { return client.PinAdd(t.Context(), oversized, kubo.PinTypeDirect) },
		"pin remove": func() error {
			return client.PinRemove(t.Context(), oversized, kubo.PinTypeDirect)
		},
		"name publish": func() error {
			_, err := client.NamePublish(t.Context(), oversized, kubo.NamePublishOptions{
				Key: "self", Lifetime: time.Hour, TTL: time.Minute,
			})
			return err
		},
		"put": func() error { _, err := client.BlockPut(t.Context(), oversizedBlock); return err },
	}
	for name, call := range calls {
		t.Run(name, func(t *testing.T) {
			err := call()
			if err == nil {
				t.Fatal("operation succeeded")
			}
			if strings.Contains(err.Error(), oversized.String()) || len(err.Error()) > 256 {
				t.Fatalf("unbounded CID diagnostic (%d bytes): %v", len(err.Error()), err)
			}
		})
	}
	if got := requests.Load(); got != 0 {
		t.Fatalf("oversized CID calls issued %d requests", got)
	}
}

type lyingBlock struct {
	cid  cid.Cid
	data []byte
}

func (b lyingBlock) Cid() cid.Cid             { return b.cid }
func (b lyingBlock) RawData() []byte          { return b.data }
func (b lyingBlock) String() string           { return b.cid.String() }
func (b lyingBlock) Loggable() map[string]any { return map[string]any{"cid": b.cid} }

func TestBlockPutValidatesInputBeforeNetwork(t *testing.T) {
	honest := testBlock(t, cid.Raw, "honest")
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		requests.Add(1)
	}))
	defer server.Close()

	client := newClient(t, server.URL, func(c *kubo.Config) { c.MaxBlockBytes = 8 })
	if _, err := client.BlockPut(t.Context(), lyingBlock{cid: honest.Cid(), data: []byte("liar")}); err == nil {
		t.Fatal("mismatched block was accepted")
	}
	if _, err := client.BlockPut(t.Context(), testBlock(t, cid.Raw, "more than eight bytes")); err == nil {
		t.Fatal("oversized block was accepted")
	}
	if got := requests.Load(); got != 0 {
		t.Fatalf("invalid inputs issued %d network requests", got)
	}
}

func TestIDRejectsMalformedIdentityAndAddress(t *testing.T) {
	validID := testPeerID(t)
	for name, response := range map[string]map[string]any{
		"peer ID": {"ID": "not-a-peer-id"},
		"address": {"ID": validID.String(), "Addresses": []string{"not-a-multiaddr"}},
	} {
		t.Run(name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				writeJSON(t, w, response)
			}))
			defer server.Close()
			_, err := newClient(t, server.URL, nil).ID(t.Context())
			var protocol *kubo.ProtocolError
			if !errors.As(err, &protocol) {
				t.Fatalf("error = %T %v, want ProtocolError", err, err)
			}
		})
	}
}

func TestBlockRemoveCommandNotFoundBody(t *testing.T) {
	target := testBlock(t, cid.Raw, "remove missing")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(t, w, map[string]string{"Hash": target.Cid().String(), "Error": "blockstore: block not found"})
	}))
	defer server.Close()
	err := newClient(t, server.URL, nil).BlockRemove(t.Context(), target.Cid())
	if !errors.Is(err, kubo.ErrNotFound) {
		t.Fatalf("BlockRemove error = %v, want ErrNotFound", err)
	}
}

func TestGenericHTTPNotFoundIsNotBlockAbsence(t *testing.T) {
	target := testBlock(t, cid.Raw, "command endpoint outage").Cid()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "command not found", http.StatusNotFound)
	}))
	defer server.Close()

	for name, call := range map[string]func(*kubo.Client) error{
		"get":    func(c *kubo.Client) error { _, err := c.BlockGet(t.Context(), target); return err },
		"stat":   func(c *kubo.Client) error { _, err := c.BlockStat(t.Context(), target); return err },
		"remove": func(c *kubo.Client) error { return c.BlockRemove(t.Context(), target) },
	} {
		t.Run(name, func(t *testing.T) {
			err := call(newClient(t, server.URL, nil))
			var status *kubo.StatusError
			if !errors.As(err, &status) || status.Status != http.StatusNotFound {
				t.Fatalf("error = %T %v, want HTTP 404 StatusError", err, err)
			}
			if errors.Is(err, kubo.ErrNotFound) {
				t.Fatalf("generic command 404 classified as block absence: %v", err)
			}
		})
	}
}

func TestTimeoutAndCallerCancellation(t *testing.T) {
	t.Run("request timeout", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
			time.Sleep(250 * time.Millisecond)
		}))
		defer server.Close()
		client := newClient(t, server.URL, func(c *kubo.Config) { c.RequestTimeout = 20 * time.Millisecond })
		started := time.Now()
		_, err := client.Version(t.Context())
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("error = %v, want deadline exceeded", err)
		}
		if elapsed := time.Since(started); elapsed > 200*time.Millisecond {
			t.Fatalf("request returned after %s", elapsed)
		}
	})

	t.Run("HTTP client timeout", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
			time.Sleep(100 * time.Millisecond)
		}))
		defer server.Close()
		httpClient := &http.Client{Timeout: 10 * time.Millisecond}
		client := newClient(t, server.URL, func(c *kubo.Config) { c.HTTPClient = httpClient })
		_, err := client.Version(t.Context())
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("error = %v, want deadline exceeded", err)
		}
	})

	t.Run("caller cause", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
		defer server.Close()
		client := newClient(t, server.URL, nil)
		cause := errors.New("caller stopped lookup")
		ctx, cancel := context.WithCancelCause(t.Context())
		cancel(cause)
		_, err := client.Version(ctx)
		if !errors.Is(err, cause) {
			t.Fatalf("error = %v, want caller cause", err)
		}
	})
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

func TestTransportErrorsRedactBearer(t *testing.T) {
	hc := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		return nil, fmt.Errorf("injected transport echoed %s", req.Header.Get("Authorization"))
	})}
	client := newClient(t, "https://kubo.example.test", func(c *kubo.Config) { c.HTTPClient = hc })
	_, err := client.Version(t.Context())
	var transport *kubo.TransportError
	if !errors.As(err, &transport) {
		t.Fatalf("error = %T %v, want TransportError", err, err)
	}
	if strings.Contains(err.Error(), testToken) {
		t.Fatalf("transport error leaked bearer: %v", err)
	}
}

func TestRedirectIsNotFollowedWithBearer(t *testing.T) {
	var redirected atomic.Int64
	destination := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		redirected.Add(1)
	}))
	defer destination.Close()
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, destination.URL, http.StatusTemporaryRedirect)
	}))
	defer source.Close()

	_, err := newClient(t, source.URL, nil).Version(t.Context())
	var status *kubo.StatusError
	if !errors.As(err, &status) || status.Status != http.StatusTemporaryRedirect {
		t.Fatalf("error = %T %v, want 307 StatusError", err, err)
	}
	if got := redirected.Load(); got != 0 {
		t.Fatalf("redirect target received %d authenticated requests", got)
	}
}
