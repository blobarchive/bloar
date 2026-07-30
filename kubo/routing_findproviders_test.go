package kubo_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/routing"
	ma "github.com/multiformats/go-multiaddr"

	"github.com/blobarchive/bloar/kubo"
)

func censusLimits(providers int) kubo.FindProvidersLimits {
	return kubo.FindProvidersLimits{
		NumProviders:            providers,
		MaxEvents:               64,
		MaxBytes:                64 << 10,
		MaxAddressesPerProvider: 8,
		MaxAddressBytes:         8 << 10,
	}
}

func TestFindProvidersWireSchemaAndDeduplication(t *testing.T) {
	target := testBlock(t, cid.Raw, "provider census target").Cid()
	first := testPeerID(t)
	second := testPeerID(t)
	firstAddress := mustMultiaddr(t, "/ip4/192.0.2.10/tcp/4001")
	secondAddress := mustMultiaddr(t, "/ip6/2001:db8::10/tcp/4001")

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/api/v0/routing/findprovs" {
			t.Errorf("request = %s %s", r.Method, r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer "+testToken {
			t.Errorf("Authorization = %q", got)
		}
		if got := r.Header.Get("Accept"); got != "application/json" {
			t.Errorf("Accept = %q", got)
		}
		want := url.Values{
			"arg":           {target.String()},
			"encoding":      {"json"},
			"num-providers": {"2"},
		}
		if r.URL.Query().Encode() != want.Encode() {
			t.Errorf("query = %q, want %q", r.URL.RawQuery, want.Encode())
		}
		w.Header().Set("Content-Type", "application/json")
		writeJSON(t, w, map[string]any{
			"ID": first.String(), "Type": int(routing.SendingQuery), "Responses": nil, "Extra": "",
		})
		writeJSON(t, w, map[string]any{
			"ID": second.String(), "Type": int(routing.PeerResponse),
			"Responses": []peer.AddrInfo{{ID: first, Addrs: []ma.Multiaddr{firstAddress}}}, "Extra": "",
		})
		// Kubo or composed routers may report one provider more than once. The
		// public contract returns one PeerID and merges unique addresses.
		writeJSON(t, w, map[string]any{
			"ID": "", "Type": int(routing.Provider),
			"Responses": []peer.AddrInfo{{ID: first, Addrs: []ma.Multiaddr{firstAddress, firstAddress}}}, "Extra": "",
		})
		writeJSON(t, w, map[string]any{
			"ID": "", "Type": int(routing.Provider),
			"Responses": []peer.AddrInfo{{ID: first, Addrs: []ma.Multiaddr{secondAddress}}}, "Extra": "",
		})
		writeJSON(t, w, map[string]any{
			"ID": "", "Type": int(routing.Provider),
			"Responses": []peer.AddrInfo{{ID: second}}, "Extra": "",
		})
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	providers, err := newClient(t, server.URL, nil).FindProviders(ctx, target, censusLimits(2))
	if err != nil {
		t.Fatalf("FindProviders: %v", err)
	}
	if len(providers) != 2 || providers[0].ID != first || providers[1].ID != second {
		t.Fatalf("providers = %+v", providers)
	}
	if len(providers[0].Addrs) != 2 || !providers[0].Addrs[0].Equal(firstAddress) || !providers[0].Addrs[1].Equal(secondAddress) {
		t.Fatalf("first provider addresses = %v", providers[0].Addrs)
	}
	if len(providers[1].Addrs) != 0 {
		t.Fatalf("second provider addresses = %v, want none", providers[1].Addrs)
	}
}

func TestFindProvidersAcceptsEmptyResult(t *testing.T) {
	target := testBlock(t, cid.Raw, "empty provider census").Cid()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	providers, err := newClient(t, server.URL, nil).FindProviders(ctx, target, censusLimits(4))
	if err != nil || len(providers) != 0 {
		t.Fatalf("FindProviders = %+v, %v", providers, err)
	}
}

func TestFindProvidersRejectsMalformedAndInconsistentEvents(t *testing.T) {
	target := testBlock(t, cid.Raw, "malformed provider census").Cid()
	provider := testPeerID(t).String()
	address := "/ip4/192.0.2.20/tcp/4001"
	info := `{"ID":"` + provider + `","Addrs":["` + address + `"]}`
	valid := `{"ID":"","Type":4,"Responses":[` + info + `],"Extra":""}`
	tests := map[string]string{
		"malformed":                   `{"ID":`,
		"unknown event field":         strings.TrimSuffix(valid, "}") + `,"Other":true}`,
		"duplicate event field":       strings.TrimSuffix(valid, "}") + `,"Extra":""}`,
		"missing event field":         `{"ID":"","Type":4,"Responses":[` + info + `]}`,
		"unsupported event type":      `{"ID":"","Type":99,"Responses":null,"Extra":""}`,
		"value event":                 `{"ID":"","Type":5,"Responses":null,"Extra":"value"}`,
		"provider with top ID":        `{"ID":"` + provider + `","Type":4,"Responses":[` + info + `],"Extra":""}`,
		"provider with Extra":         `{"ID":"","Type":4,"Responses":[` + info + `],"Extra":"extra"}`,
		"provider with null response": `{"ID":"","Type":4,"Responses":null,"Extra":""}`,
		"provider with two responses": `{"ID":"","Type":4,"Responses":[` + info + `,` + info + `],"Extra":""}`,
		"unknown AddrInfo field":      `{"ID":"","Type":4,"Responses":[{"ID":"` + provider + `","Addrs":[],"Other":1}],"Extra":""}`,
		"duplicate AddrInfo field":    `{"ID":"","Type":4,"Responses":[{"ID":"` + provider + `","ID":"` + provider + `","Addrs":[]}],"Extra":""}`,
		"missing AddrInfo Addrs":      `{"ID":"","Type":4,"Responses":[{"ID":"` + provider + `"}],"Extra":""}`,
		"invalid provider ID":         `{"ID":"","Type":4,"Responses":[{"ID":"not-a-peer","Addrs":[]}],"Extra":""}`,
		"invalid multiaddr":           `{"ID":"","Type":4,"Responses":[{"ID":"` + provider + `","Addrs":["not-an-address"]}],"Extra":""}`,
		"non-string multiaddr":        `{"ID":"","Type":4,"Responses":[{"ID":"` + provider + `","Addrs":[7]}],"Extra":""}`,
		"query error without message": `{"ID":"","Type":3,"Responses":null,"Extra":""}`,
		"peer response without ID":    `{"ID":"","Type":1,"Responses":[],"Extra":""}`,
		"progress event responses":    `{"ID":"` + provider + `","Type":0,"Responses":[` + info + `],"Extra":""}`,
		"invalid UTF-8":               string([]byte{'{', '"', 'I', 0xff, '"', ':', '1', '}'}),
	}
	for name, body := range tests {
		t.Run(name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, body)
			}))
			defer server.Close()
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			providers, err := newClient(t, server.URL, nil).FindProviders(ctx, target, censusLimits(2))
			if providers != nil {
				t.Fatalf("partial providers returned: %+v", providers)
			}
			requireProtocolError(t, err)
		})
	}
}

func TestFindProvidersRejectsRoutingErrorsAndLateTrailers(t *testing.T) {
	target := testBlock(t, cid.Raw, "provider query errors").Cid()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		writeJSON(t, w, map[string]any{"ID": "", "Type": int(routing.QueryError), "Responses": nil, "Extra": "one DHT peer failed"})
	}))
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	providers, err := newClient(t, server.URL, nil).FindProviders(ctx, target, censusLimits(2))
	cancel()
	server.Close()
	if err == nil || providers != nil || !strings.Contains(err.Error(), "one DHT peer failed") {
		t.Fatalf("QueryError result = %+v, %v", providers, err)
	}

	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Trailer", "X-Stream-Error")
		_, _ = io.WriteString(w, "\n")
		w.Header().Set("X-Stream-Error", "late routing failure")
	}))
	defer server.Close()
	ctx, cancel = context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	_, err = newClient(t, server.URL, nil).FindProviders(ctx, target, censusLimits(2))
	var stream *kubo.StreamError
	if !errors.As(err, &stream) {
		t.Fatalf("late trailer error = %T %v, want StreamError", err, err)
	}
}

func TestFindProvidersValidatesArgumentsBeforeNetwork(t *testing.T) {
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { requests.Add(1) }))
	defer server.Close()
	client := newClient(t, server.URL, nil)
	target := testBlock(t, cid.Raw, "provider census validation").Cid()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()

	valid := censusLimits(2)
	tests := map[string]func() error{
		"undefined CID": func() error {
			_, err := client.FindProviders(ctx, cid.Undef, valid)
			return err
		},
		"no caller deadline": func() error {
			_, err := client.FindProviders(t.Context(), target, valid)
			return err
		},
		"zero providers": func() error {
			limits := valid
			limits.NumProviders = 0
			_, err := client.FindProviders(ctx, target, limits)
			return err
		},
		"too many providers": func() error {
			limits := valid
			limits.NumProviders = kubo.MaximumFindProviders + 1
			_, err := client.FindProviders(ctx, target, limits)
			return err
		},
		"zero events": func() error {
			limits := valid
			limits.MaxEvents = 0
			_, err := client.FindProviders(ctx, target, limits)
			return err
		},
		"too many events": func() error {
			limits := valid
			limits.MaxEvents = kubo.MaximumFindProviderEvents + 1
			_, err := client.FindProviders(ctx, target, limits)
			return err
		},
		"zero stream bytes": func() error {
			limits := valid
			limits.MaxBytes = 0
			_, err := client.FindProviders(ctx, target, limits)
			return err
		},
		"too many stream bytes": func() error {
			limits := valid
			limits.MaxBytes = kubo.MaximumFindProviderStreamBytes + 1
			_, err := client.FindProviders(ctx, target, limits)
			return err
		},
		"zero addresses": func() error {
			limits := valid
			limits.MaxAddressesPerProvider = 0
			_, err := client.FindProviders(ctx, target, limits)
			return err
		},
		"too many addresses": func() error {
			limits := valid
			limits.MaxAddressesPerProvider = kubo.MaximumFindProviderAddresses + 1
			_, err := client.FindProviders(ctx, target, limits)
			return err
		},
		"zero address bytes": func() error {
			limits := valid
			limits.MaxAddressBytes = 0
			_, err := client.FindProviders(ctx, target, limits)
			return err
		},
		"too many address bytes": func() error {
			limits := valid
			limits.MaxAddressBytes = kubo.MaximumFindProviderAddressBytes + 1
			_, err := client.FindProviders(ctx, target, limits)
			return err
		},
	}
	for name, call := range tests {
		t.Run(name, func(t *testing.T) {
			if err := call(); err == nil {
				t.Fatal("FindProviders succeeded")
			}
		})
	}
	if got := requests.Load(); got != 0 {
		t.Fatalf("network requests = %d, want 0", got)
	}
}

func TestFindProvidersEnforcesProviderEventAndAddressBounds(t *testing.T) {
	target := testBlock(t, cid.Raw, "provider census response bounds").Cid()
	first := testPeerID(t)
	second := testPeerID(t)
	address := "/ip4/192.0.2.30/tcp/4001"
	otherAddress := "/ip4/192.0.2.31/tcp/4001"
	providerEvent := func(id peer.ID, addresses ...string) string {
		quoted := make([]string, len(addresses))
		for i, value := range addresses {
			quoted[i] = fmt.Sprintf("%q", value)
		}
		return fmt.Sprintf(`{"ID":"","Type":4,"Responses":[{"ID":%q,"Addrs":[%s]}],"Extra":""}`,
			id.String(), strings.Join(quoted, ","))
	}
	tests := []struct {
		name   string
		body   string
		limits kubo.FindProvidersLimits
	}{
		{
			name: "unique provider limit",
			body: providerEvent(first) + "\n" + providerEvent(second),
			limits: func() kubo.FindProvidersLimits {
				limits := censusLimits(1)
				return limits
			}(),
		},
		{
			name: "event limit",
			body: providerEvent(first) + "\n" + providerEvent(first),
			limits: func() kubo.FindProvidersLimits {
				limits := censusLimits(1)
				limits.MaxEvents = 1
				return limits
			}(),
		},
		{
			name: "addresses per provider",
			body: providerEvent(first, address, otherAddress),
			limits: func() kubo.FindProvidersLimits {
				limits := censusLimits(1)
				limits.MaxAddressesPerProvider = 1
				return limits
			}(),
		},
		{
			name: "address bytes",
			body: providerEvent(first, address),
			limits: func() kubo.FindProvidersLimits {
				limits := censusLimits(1)
				limits.MaxAddressBytes = int64(len(address) - 1)
				return limits
			}(),
		},
		{
			name: "stream bytes",
			body: providerEvent(first),
			limits: func() kubo.FindProvidersLimits {
				limits := censusLimits(1)
				limits.MaxBytes = 8
				return limits
			}(),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, test.body)
			}))
			defer server.Close()
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			providers, err := newClient(t, server.URL, nil).FindProviders(ctx, target, test.limits)
			if providers != nil {
				t.Fatalf("partial providers returned: %+v", providers)
			}
			requireProtocolError(t, err)
		})
	}
}

func TestFindProvidersUsesCallerDeadline(t *testing.T) {
	target := testBlock(t, cid.Raw, "provider census caller deadline").Cid()
	provider := testPeerID(t)
	validEvent := fmt.Sprintf(`{"ID":"","Type":4,"Responses":[{"ID":%q,"Addrs":null}],"Extra":""}`, provider.String())

	t.Run("Config timeout bypassed", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			time.Sleep(25 * time.Millisecond)
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, validEvent)
		}))
		defer server.Close()
		client := newClient(t, server.URL, func(cfg *kubo.Config) { cfg.RequestTimeout = time.Millisecond })
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		if _, err := client.FindProviders(ctx, target, censusLimits(1)); err != nil {
			t.Fatalf("FindProviders obeyed Config.RequestTimeout: %v", err)
		}
	})

	t.Run("caller deadline expires midstream", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, validEvent)
			if flusher, ok := w.(http.Flusher); ok {
				flusher.Flush()
			}
			<-r.Context().Done()
		}))
		defer server.Close()
		ctx, cancel := context.WithTimeout(t.Context(), 25*time.Millisecond)
		defer cancel()
		_, err := newClient(t, server.URL, nil).FindProviders(ctx, target, censusLimits(1))
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("error = %T %v, want context deadline exceeded", err, err)
		}
	})
}

func TestCensusCompatibilityUsesMinimalReadOnlyProfile(t *testing.T) {
	commands := []string{
		"ipfs routing findprovs",
		"ipfs routing findprovs --num-providers / ipfs routing findprovs -n",
		"ipfs version",
	}
	server := compatibilityServer(t, []byte(strings.Join(commands, "\n")+"\n"))
	defer server.Close()
	client := newClient(t, server.URL, nil)
	info, err := client.CheckCensusCompatibility(t.Context())
	if err != nil || info.Version != "0.42.0" {
		t.Fatalf("CheckCensusCompatibility = %+v, %v", info, err)
	}
	if _, err := client.CheckReplicaCompatibility(t.Context()); err == nil {
		t.Fatal("replica compatibility accepted census-only command tree")
	}
}

func TestCensusCompatibilityRejectsMissingCapability(t *testing.T) {
	commands := []byte("ipfs routing findprovs\nipfs version\n")
	server := compatibilityServer(t, commands)
	defer server.Close()
	_, err := newClient(t, server.URL, nil).CheckCensusCompatibility(t.Context())
	var capability *kubo.CapabilityError
	if !errors.As(err, &capability) || capability.Missing != "ipfs routing findprovs --num-providers" {
		t.Fatalf("error = %T %v, want missing num-providers CapabilityError", err, err)
	}
}
