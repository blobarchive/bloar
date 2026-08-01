package kubo_test

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ipfs/boxo/ipns"
	boxopath "github.com/ipfs/boxo/path"
	"github.com/ipfs/go-cid"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	libp2pcrypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/routing"

	"github.com/blobarchive/bloar/kubo"
)

func TestIPNSRecordAndReadOnlyValueStoreContract(t *testing.T) {
	name, record := testIPNSRecord(t, "ipns-routing-record")
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if r.Method != http.MethodPost || r.URL.Path != "/api/v0/routing/get" {
			t.Errorf("request = %s %s", r.Method, r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer "+testToken {
			t.Errorf("Authorization = %q", got)
		}
		if got := r.Header.Get("Accept"); got != "application/json" {
			t.Errorf("Accept = %q", got)
		}
		query := r.URL.Query()
		if got, want := query.Get("arg"), ipns.NamespacePrefix+ipns.NameFromPeer(name).String(); got != want {
			t.Errorf("arg = %q, want %q", got, want)
		}
		if got := query.Get("encoding"); got != "json" || len(query) != 2 {
			t.Errorf("query = %v", query)
		}
		writeRoutingValue(t, w, record)
	}))
	defer server.Close()
	client := newClient(t, server.URL, nil)

	raw, err := client.IPNSRecord(t.Context(), name)
	if err != nil || string(raw) != string(record) {
		t.Fatalf("IPNSRecord = %x, %v", raw, err)
	}
	raw[0] ^= 0xff

	store := client.IPNSValueStore()
	key := string(ipns.NameFromPeer(name).RoutingKey())
	got, err := store.GetValue(t.Context(), key, dht.Quorum(16))
	if err != nil || string(got) != string(record) {
		t.Fatalf("GetValue = %x, %v", got, err)
	}
	values, err := store.SearchValue(t.Context(), key, dht.Quorum(16))
	if err != nil {
		t.Fatalf("SearchValue: %v", err)
	}
	first, ok := <-values
	if !ok || string(first) != string(record) {
		t.Fatalf("SearchValue first = %x, %v", first, ok)
	}
	if extra, ok := <-values; ok {
		t.Fatalf("SearchValue returned extra value %x", extra)
	}
	if err := store.PutValue(t.Context(), key, record); !errors.Is(err, routing.ErrNotSupported) {
		t.Fatalf("PutValue error = %v, want ErrNotSupported", err)
	}
	if got := requests.Load(); got != 3 {
		t.Fatalf("routing/get requests = %d, want 3", got)
	}
}

func TestIPNSValueStoreRejectsUnsupportedKeysAndOptionsBeforeNetwork(t *testing.T) {
	name, record := testIPNSRecord(t, "ipns-options-record")
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { requests.Add(1) }))
	defer server.Close()
	store := newClient(t, server.URL, nil).IPNSValueStore()
	key := string(ipns.NameFromPeer(name).RoutingKey())
	optionErr := errors.New("bad option")
	badOption := routing.Option(func(*routing.Options) error { return optionErr })

	tests := map[string]func() error{
		"text IPNS key": func() error {
			_, err := store.GetValue(t.Context(), ipns.NamespacePrefix+ipns.NameFromPeer(name).String())
			return err
		},
		"malformed binary key": func() error {
			_, err := store.GetValue(t.Context(), "/ipns/bad")
			return err
		},
		"other namespace": func() error {
			_, err := store.GetValue(t.Context(), "/pk/"+string(name))
			return err
		},
		"expired": func() error {
			_, err := store.GetValue(t.Context(), key, routing.Expired)
			return err
		},
		"offline": func() error {
			_, err := store.GetValue(t.Context(), key, routing.Offline)
			return err
		},
		"nil option": func() error {
			_, err := store.GetValue(t.Context(), key, nil)
			return err
		},
		"failing option": func() error {
			_, err := store.GetValue(t.Context(), key, badOption)
			return err
		},
		"search unsupported": func() error {
			_, err := store.SearchValue(t.Context(), "/pk/"+string(name))
			return err
		},
	}
	for testName, call := range tests {
		t.Run(testName, func(t *testing.T) {
			if err := call(); err == nil {
				t.Fatal("operation succeeded")
			}
		})
	}
	if err := store.PutValue(t.Context(), key, record, badOption); !errors.Is(err, routing.ErrNotSupported) {
		t.Fatalf("PutValue error = %v", err)
	}
	if got := requests.Load(); got != 0 {
		t.Fatalf("network requests = %d, want 0", got)
	}
}

func TestIPNSRecordRejectsInvalidPeerIDBeforeNetwork(t *testing.T) {
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { requests.Add(1) }))
	defer server.Close()
	client := newClient(t, server.URL, nil)
	for _, name := range []peer.ID{"", "x", peer.ID(strings.Repeat("x", 513))} {
		if record, err := client.IPNSRecord(t.Context(), name); err == nil || record != nil {
			t.Fatalf("IPNSRecord(%q) = %x, %v", name, record, err)
		}
	}
	if got := requests.Load(); got != 0 {
		t.Fatalf("network requests = %d, want 0", got)
	}
}

func TestIPNSRecordRejectsMalformedAndInconsistentResponses(t *testing.T) {
	name, record := testIPNSRecord(t, "ipns-malformed-record")
	otherName, otherRecord := testIPNSRecord(t, "ipns-other-record")
	_ = otherName
	encoded := base64.StdEncoding.EncodeToString(record)
	base := `{"ID":"","Type":5,"Responses":null,"Extra":"` + encoded + `"}`
	tests := []struct {
		name string
		body string
	}{
		{name: "malformed", body: `{"Extra":`},
		{name: "unknown field", body: strings.TrimSuffix(base, "}") + `,"Other":true}`},
		{name: "duplicate field", body: strings.TrimSuffix(base, "}") + `,"Extra":"` + encoded + `"}`},
		{name: "missing ID", body: `{"Type":5,"Responses":null,"Extra":"` + encoded + `"}`},
		{name: "missing Type", body: `{"ID":"","Responses":null,"Extra":"` + encoded + `"}`},
		{name: "missing Responses", body: `{"ID":"","Type":5,"Extra":"` + encoded + `"}`},
		{name: "missing Extra", body: `{"ID":"","Type":5,"Responses":null}`},
		{name: "nonempty ID", body: `{"ID":"peer","Type":5,"Responses":null,"Extra":"` + encoded + `"}`},
		{name: "wrong Type", body: `{"ID":"","Type":4,"Responses":null,"Extra":"` + encoded + `"}`},
		{name: "unexpected Responses", body: `{"ID":"","Type":5,"Responses":[],"Extra":"` + encoded + `"}`},
		{name: "empty record", body: `{"ID":"","Type":5,"Responses":null,"Extra":""}`},
		{name: "base64 whitespace", body: `{"ID":"","Type":5,"Responses":null,"Extra":"Y Q=="}`},
		{name: "invalid base64", body: `{"ID":"","Type":5,"Responses":null,"Extra":"***"}`},
		{name: "noncanonical base64", body: `{"ID":"","Type":5,"Responses":null,"Extra":"Zh=="}`},
		{name: "invalid record", body: `{"ID":"","Type":5,"Responses":null,"Extra":"` + base64.StdEncoding.EncodeToString([]byte("not an IPNS record")) + `"}`},
		{name: "wrong signed name", body: `{"ID":"","Type":5,"Responses":null,"Extra":"` + base64.StdEncoding.EncodeToString(otherRecord) + `"}`},
		{name: "encoded oversize", body: `{"ID":"","Type":5,"Responses":null,"Extra":"` + strings.Repeat("A", base64.StdEncoding.EncodedLen(ipns.MaxRecordSize)+4) + `"}`},
		{name: "invalid UTF8", body: string([]byte{'{', '"', 'X', 0xff, '"', ':', '1', '}'})},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, test.body)
			}))
			defer server.Close()
			raw, err := newClient(t, server.URL, nil).IPNSRecord(t.Context(), name)
			if raw != nil {
				t.Fatalf("partial record returned: %x", raw)
			}
			requireProtocolError(t, err)
		})
	}
}

func TestIPNSRecordResponseByteBounds(t *testing.T) {
	name, _ := testIPNSRecord(t, "ipns-byte-bounds")
	for _, test := range []struct {
		name   string
		header bool
	}{
		{name: "declared", header: true},
		{name: "streamed"},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if test.header {
					w.Header().Set("Content-Length", "20000")
				}
				_, _ = io.WriteString(w, strings.Repeat("x", 20_000))
			}))
			defer server.Close()
			_, err := newClient(t, server.URL, nil).IPNSRecord(t.Context(), name)
			requireProtocolError(t, err)
		})
	}
}

func TestIPNSRoutingNotFoundMapping(t *testing.T) {
	name, _ := testIPNSRecord(t, "ipns-not-found")
	for _, test := range []struct {
		name       string
		message    string
		token      string
		wantAbsent bool
	}{
		{name: "exact", message: routing.ErrNotFound.Error(), wantAbsent: true},
		{name: "classified before redaction", message: routing.ErrNotFound.Error(), token: "not", wantAbsent: true},
		{name: "not exact", message: "wrapped: " + routing.ErrNotFound.Error()},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusInternalServerError)
				_ = json.NewEncoder(w).Encode(map[string]any{"Message": test.message, "Code": 0, "Type": "error"})
			}))
			defer server.Close()
			client := newClient(t, server.URL, func(cfg *kubo.Config) {
				if test.token != "" {
					cfg.BearerToken = test.token
				}
			})
			_, err := client.IPNSRecord(t.Context(), name)
			if errors.Is(err, routing.ErrNotFound) != test.wantAbsent {
				t.Fatalf("error = %T %v, ErrNotFound=%v", err, err, errors.Is(err, routing.ErrNotFound))
			}
			var status *kubo.StatusError
			if !errors.As(err, &status) {
				t.Fatalf("StatusError lost: %T %v", err, err)
			}
			if test.wantAbsent {
				values, searchErr := client.IPNSValueStore().SearchValue(t.Context(), string(ipns.NameFromPeer(name).RoutingKey()))
				if searchErr != nil {
					t.Fatalf("SearchValue: %v", searchErr)
				}
				if value, ok := <-values; ok {
					t.Fatalf("SearchValue returned %x for absent record", value)
				}
			}
		})
	}
}

func TestIPNSRecordContextCancellation(t *testing.T) {
	name, _ := testIPNSRecord(t, "ipns-context")
	started := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		<-r.Context().Done()
	}))
	defer server.Close()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		_, err := newClient(t, server.URL, nil).IPNSRecord(ctx, name)
		done <- err
	}()
	<-started
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %T %v, want context.Canceled", err, err)
	}
}

func testIPNSRecord(t *testing.T, seed string) (peer.ID, []byte) {
	t.Helper()
	privateKey, publicKey, err := libp2pcrypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateEd25519Key: %v", err)
	}
	name, err := peer.IDFromPublicKey(publicKey)
	if err != nil {
		t.Fatalf("IDFromPublicKey: %v", err)
	}
	target := testBlock(t, cid.Raw, seed).Cid()
	record, err := ipns.NewRecord(privateKey, boxopath.FromCid(target), 7, time.Now().Add(time.Hour), time.Minute)
	if err != nil {
		t.Fatalf("NewRecord: %v", err)
	}
	raw, err := ipns.MarshalRecord(record)
	if err != nil {
		t.Fatalf("MarshalRecord: %v", err)
	}
	return name, raw
}

func writeRoutingValue(t *testing.T, w http.ResponseWriter, record []byte) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	writeJSON(t, w, map[string]any{
		"ID":        "",
		"Type":      int(routing.Value),
		"Responses": nil,
		"Extra":     base64.StdEncoding.EncodeToString(record),
	})
}
