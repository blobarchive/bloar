package kubo_test

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/blobarchive/bloar/kubo"
)

func TestProvideConfigReadContract(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if r.Method != http.MethodPost {
			t.Errorf("method = %s, want POST", r.Method)
		}
		if r.URL.Path != "/api/v0/config" {
			t.Errorf("path = %q, want /api/v0/config", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer "+testToken {
			t.Errorf("Authorization = %q", got)
		}
		if got := r.Header.Get("Accept"); got != "application/json" {
			t.Errorf("Accept = %q, want application/json", got)
		}
		if got := r.Header.Get("Content-Type"); got != "" {
			t.Errorf("Content-Type = %q, want empty", got)
		}
		if r.ContentLength != 0 {
			t.Errorf("ContentLength = %d, want 0", r.ContentLength)
		}

		key := r.URL.Query().Get("arg")
		wantQuery := url.Values{
			"arg":         []string{key},
			"encoding":    []string{"json"},
			"expand-auto": []string{"false"},
		}
		if got := r.URL.Query(); !reflect.DeepEqual(got, wantQuery) {
			t.Errorf("query = %v, want %v", got, wantQuery)
		}
		switch key {
		case "Provide.Enabled":
			writeJSON(t, w, map[string]any{"Key": key, "Value": false})
		case "Provide.Strategy":
			writeJSON(t, w, map[string]any{"Key": key, "Value": "pinned+mfs+entities"})
		default:
			t.Errorf("unsafe or unexpected config key requested: %q", key)
			http.Error(w, "unexpected key", http.StatusBadRequest)
		}
	}))
	defer server.Close()

	client := newClient(t, server.URL, nil)
	enabled, err := client.ConfigProvideEnabled(t.Context())
	if err != nil || enabled {
		t.Fatalf("ConfigProvideEnabled = %t, %v; want false, nil", enabled, err)
	}
	strategy, err := client.ConfigProvideStrategy(t.Context())
	if err != nil || strategy != "pinned+mfs+entities" {
		t.Fatalf("ConfigProvideStrategy = %q, %v", strategy, err)
	}
	if got := requests.Load(); got != 2 {
		t.Fatalf("requests = %d, want 2", got)
	}
}

func TestProvideConfigReadsRequireExactSchemas(t *testing.T) {
	tests := []struct {
		name string
		body []byte
		call func(*kubo.Client) error
	}{
		{
			name: "enabled missing key", body: []byte(`{"Value":true}`),
			call: func(c *kubo.Client) error { _, err := c.ConfigProvideEnabled(t.Context()); return err },
		},
		{
			name: "enabled missing value", body: []byte(`{"Key":"Provide.Enabled"}`),
			call: func(c *kubo.Client) error { _, err := c.ConfigProvideEnabled(t.Context()); return err },
		},
		{
			name: "enabled null value", body: []byte(`{"Key":"Provide.Enabled","Value":null}`),
			call: func(c *kubo.Client) error { _, err := c.ConfigProvideEnabled(t.Context()); return err },
		},
		{
			name: "enabled wrong value type", body: []byte(`{"Key":"Provide.Enabled","Value":"true"}`),
			call: func(c *kubo.Client) error { _, err := c.ConfigProvideEnabled(t.Context()); return err },
		},
		{
			name: "enabled wrong echoed key", body: []byte(`{"Key":"Identity.PrivKey","Value":true}`),
			call: func(c *kubo.Client) error { _, err := c.ConfigProvideEnabled(t.Context()); return err },
		},
		{
			name: "enabled unknown field", body: []byte(`{"Key":"Provide.Enabled","Value":true,"Extra":false}`),
			call: func(c *kubo.Client) error { _, err := c.ConfigProvideEnabled(t.Context()); return err },
		},
		{
			name: "enabled duplicate field", body: []byte(`{"Key":"Provide.Enabled","Value":true,"Value":false}`),
			call: func(c *kubo.Client) error { _, err := c.ConfigProvideEnabled(t.Context()); return err },
		},
		{
			name: "enabled noncanonical field", body: []byte(`{"key":"Provide.Enabled","Value":true}`),
			call: func(c *kubo.Client) error { _, err := c.ConfigProvideEnabled(t.Context()); return err },
		},
		{
			name: "strategy missing key", body: []byte(`{"Value":"roots"}`),
			call: func(c *kubo.Client) error { _, err := c.ConfigProvideStrategy(t.Context()); return err },
		},
		{
			name: "strategy missing value", body: []byte(`{"Key":"Provide.Strategy"}`),
			call: func(c *kubo.Client) error { _, err := c.ConfigProvideStrategy(t.Context()); return err },
		},
		{
			name: "strategy null value", body: []byte(`{"Key":"Provide.Strategy","Value":null}`),
			call: func(c *kubo.Client) error { _, err := c.ConfigProvideStrategy(t.Context()); return err },
		},
		{
			name: "strategy wrong value type", body: []byte(`{"Key":"Provide.Strategy","Value":true}`),
			call: func(c *kubo.Client) error { _, err := c.ConfigProvideStrategy(t.Context()); return err },
		},
		{
			name: "strategy wrong echoed key", body: []byte(`{"Key":"Provide.Enabled","Value":"roots"}`),
			call: func(c *kubo.Client) error { _, err := c.ConfigProvideStrategy(t.Context()); return err },
		},
		{
			name: "strategy unknown field", body: []byte(`{"Key":"Provide.Strategy","Value":"roots","Secret":"x"}`),
			call: func(c *kubo.Client) error { _, err := c.ConfigProvideStrategy(t.Context()); return err },
		},
		{
			name: "strategy duplicate field", body: []byte(`{"Key":"Provide.Strategy","Value":"roots","Value":"all"}`),
			call: func(c *kubo.Client) error { _, err := c.ConfigProvideStrategy(t.Context()); return err },
		},
		{
			name: "multiple JSON values", body: []byte(`{"Key":"Provide.Strategy","Value":"roots"}{}`),
			call: func(c *kubo.Client) error { _, err := c.ConfigProvideStrategy(t.Context()); return err },
		},
		{
			name: "malformed JSON", body: []byte(`{"Key":"Provide.Strategy","Value":`),
			call: func(c *kubo.Client) error { _, err := c.ConfigProvideStrategy(t.Context()); return err },
		},
		{
			name: "invalid UTF-8", body: []byte("{\"Key\":\"Provide.Strategy\",\"Value\":\"\xff\"}"),
			call: func(c *kubo.Client) error { _, err := c.ConfigProvideStrategy(t.Context()); return err },
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write(test.body)
			}))
			defer server.Close()

			err := test.call(newClient(t, server.URL, nil))
			var protocol *kubo.ProtocolError
			if !errors.As(err, &protocol) || protocol.Endpoint != "config" {
				t.Fatalf("error = %T %v, want config ProtocolError", err, err)
			}
		})
	}
}

func TestProvideConfigStrategyPreservesLiteralValue(t *testing.T) {
	for _, value := range []string{"", " all ", strings.Repeat("x", 512)} {
		t.Run(strconv.Quote(value), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				writeJSON(t, w, map[string]any{"Key": "Provide.Strategy", "Value": value})
			}))
			defer server.Close()

			got, err := newClient(t, server.URL, nil).ConfigProvideStrategy(t.Context())
			if err != nil || got != value {
				t.Fatalf("ConfigProvideStrategy = %q, %v; want exact %q", got, err, value)
			}
		})
	}
}

func TestProvideConfigEnabledAcceptsBothBooleanValues(t *testing.T) {
	for _, value := range []bool{false, true} {
		t.Run(strconv.FormatBool(value), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				writeJSON(t, w, map[string]any{"Key": "Provide.Enabled", "Value": value})
			}))
			defer server.Close()

			got, err := newClient(t, server.URL, nil).ConfigProvideEnabled(t.Context())
			if err != nil || got != value {
				t.Fatalf("ConfigProvideEnabled = %t, %v; want %t", got, err, value)
			}
		})
	}
}

func TestProvideConfigReadsBoundSuccessBodies(t *testing.T) {
	tests := []struct {
		name    string
		handler http.HandlerFunc
	}{
		{
			name: "declared oversized response",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("Content-Length", strconv.Itoa((64<<10)+1))
				_, _ = io.WriteString(w, `{}`)
			},
		},
		{
			name: "streamed oversized response",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.(http.Flusher).Flush()
				_, _ = io.WriteString(w, strings.Repeat("x", (64<<10)+1))
			},
		},
		{
			name: "oversized strategy value",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				writeJSON(t, w, map[string]any{"Key": "Provide.Strategy", "Value": strings.Repeat("x", 513)})
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(test.handler)
			defer server.Close()
			_, err := newClient(t, server.URL, nil).ConfigProvideStrategy(t.Context())
			var protocol *kubo.ProtocolError
			if !errors.As(err, &protocol) || protocol.Endpoint != "config" {
				t.Fatalf("error = %T %v, want config ProtocolError", err, err)
			}
		})
	}
}

func TestProvideConfigReadsPreserveTypedStatusErrorsAndRedact(t *testing.T) {
	for name, call := range map[string]func(*kubo.Client) error{
		"enabled":  func(c *kubo.Client) error { _, err := c.ConfigProvideEnabled(t.Context()); return err },
		"strategy": func(c *kubo.Client) error { _, err := c.ConfigProvideStrategy(t.Context()); return err },
	} {
		t.Run(name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusForbidden)
				_, _ = io.WriteString(w, `{"Message":"denied `+testToken+`","Code":7,"Type":"auth-`+testToken+`"}`)
			}))
			defer server.Close()

			err := call(newClient(t, server.URL, nil))
			var status *kubo.StatusError
			if !errors.As(err, &status) || status.Endpoint != "config" || status.Status != http.StatusForbidden || status.Code != 7 {
				t.Fatalf("error = %T %v, want config 403 StatusError", err, err)
			}
			if strings.Contains(err.Error(), testToken) || strings.Contains(status.Message, testToken) || strings.Contains(status.Type, testToken) {
				t.Fatalf("status error leaked bearer token: %#v", status)
			}
		})
	}
}
