package kubo_test

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ipfs/go-cid"

	"github.com/blobarchive/bloar/kubo"
)

func beginJSONStream(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Trailer", "X-Stream-Error")
}

func TestRepositoryRPCContract(t *testing.T) {
	first := testBlock(t, cid.Raw, "first collected block").Cid()
	second := testBlock(t, cid.Raw, "second collected block").Cid()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method = %s, want POST", r.Method)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer "+testToken {
			t.Errorf("Authorization = %q", got)
		}
		switch r.URL.Path {
		case "/api/v0/repo/stat":
			if r.URL.Query().Get("size-only") != "false" || r.URL.Query().Get("human") != "false" {
				t.Errorf("repo/stat query = %v", r.URL.Query())
			}
			writeJSON(t, w, map[string]any{
				"NumObjects": uint64(17), "RepoSize": uint64(2048), "StorageMax": uint64(4096),
				"RepoPath": "/var/lib/ipfs", "Version": "fs-repo@19",
			})
		case "/api/v0/repo/gc":
			query := r.URL.Query()
			if query.Get("stream-errors") != "true" || query.Get("quiet") != "false" || query.Get("silent") != "false" {
				t.Errorf("repo/gc query = %v", query)
			}
			beginJSONStream(w)
			writeJSON(t, w, map[string]any{"Key": map[string]string{"/": first.String()}})
			writeJSON(t, w, map[string]any{"Key": map[string]string{"/": second.String()}})
		case "/api/v0/repo/verify":
			query := r.URL.Query()
			if query.Get("drop") != "false" || query.Get("heal") != "false" || query.Get("heal-timeout") != "30s" {
				t.Errorf("repo/verify query = %v", query)
			}
			beginJSONStream(w)
			writeJSON(t, w, map[string]any{"Msg": "", "Progress": 1})
			writeJSON(t, w, map[string]any{"Msg": "", "Progress": 2})
			writeJSON(t, w, map[string]any{"Msg": "verify complete, all blocks validated.", "Progress": 0})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client := newClient(t, server.URL, nil)

	stat, err := client.RepoStat(t.Context())
	if err != nil {
		t.Fatalf("RepoStat: %v", err)
	}
	if stat.NumObjects != 17 || stat.RepoSize != 2048 || stat.StorageMax != 4096 || stat.Version != "fs-repo@19" {
		t.Errorf("RepoStat = %+v", stat)
	}
	gc, err := client.RepoGC(t.Context())
	if err != nil {
		t.Fatalf("RepoGC: %v", err)
	}
	if len(gc.Removed) != 2 || !gc.Removed[0].Equals(first) || !gc.Removed[1].Equals(second) {
		t.Errorf("RepoGC = %+v", gc)
	}
	verified, err := client.RepoVerify(t.Context())
	if err != nil {
		t.Fatalf("RepoVerify: %v", err)
	}
	if verified.BlocksProcessed != 2 || len(verified.Messages) != 1 {
		t.Errorf("RepoVerify = %+v", verified)
	}
}

func TestRepoGCDrainsLateItemsAndTrailer(t *testing.T) {
	removed := testBlock(t, cid.Raw, "removed after error item").Cid()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		beginJSONStream(w)
		writeJSON(t, w, map[string]any{"Key": nil, "Error": "early failure echoed " + testToken})
		writeJSON(t, w, map[string]any{"Key": map[string]string{"/": removed.String()}})
		w.Header().Set("X-Stream-Error", "late failure echoed "+testToken)
	}))
	defer server.Close()

	result, err := newClient(t, server.URL, nil).RepoGC(t.Context())
	var stream *kubo.StreamError
	if !errors.As(err, &stream) || stream.Item != 1 {
		t.Fatalf("error = %T %v, want item StreamError", err, err)
	}
	if strings.Contains(err.Error(), testToken) {
		t.Fatalf("stream error leaked bearer: %v", err)
	}
	if len(result.Removed) != 1 || !result.Removed[0].Equals(removed) {
		t.Fatalf("late valid item was not drained: %+v", result)
	}
}

func TestRepoGCRejectsMalformedDuplicateAndBoundedStreams(t *testing.T) {
	target := testBlock(t, cid.Raw, "gc target").Cid()
	tests := []struct {
		name      string
		configure func(*kubo.Config)
		handler   http.HandlerFunc
	}{
		{
			name: "malformed late JSON",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				beginJSONStream(w)
				writeJSON(t, w, map[string]any{"Key": map[string]string{"/": target.String()}})
				_, _ = io.WriteString(w, `{"Key":`)
			},
		},
		{
			name: "duplicate object field",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				beginJSONStream(w)
				_, _ = fmt.Fprintf(w, `{"Key":{"/":%q},"Key":{"/":%q}}`, target.String(), target.String())
			},
		},
		{
			name: "excessive JSON nesting",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				beginJSONStream(w)
				const depth = 70
				_, _ = io.WriteString(w, `{"Key":`+strings.Repeat("[", depth)+`null`+strings.Repeat("]", depth)+`}`)
			},
		},
		{
			name: "duplicate CID",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				beginJSONStream(w)
				writeJSON(t, w, map[string]any{"Key": map[string]string{"/": target.String()}})
				writeJSON(t, w, map[string]any{"Key": map[string]string{"/": target.String()}})
			},
		},
		{
			name:      "item limit",
			configure: func(c *kubo.Config) { c.MaxStreamItems = 1 },
			handler: func(w http.ResponseWriter, _ *http.Request) {
				beginJSONStream(w)
				writeJSON(t, w, map[string]any{"Key": map[string]string{"/": target.String()}})
				writeJSON(t, w, map[string]any{"Error": "second item"})
			},
		},
		{
			name:      "byte limit",
			configure: func(c *kubo.Config) { c.MaxStreamBytes = 32 },
			handler: func(w http.ResponseWriter, _ *http.Request) {
				beginJSONStream(w)
				writeJSON(t, w, map[string]string{"Error": strings.Repeat("x", 64)})
			},
		},
		{
			name: "wrong content type",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "text/plain")
				_, _ = io.WriteString(w, `{}`)
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(test.handler)
			defer server.Close()
			_, err := newClient(t, server.URL, test.configure).RepoGC(t.Context())
			var protocol *kubo.ProtocolError
			if !errors.As(err, &protocol) {
				t.Fatalf("error = %T %v, want ProtocolError", err, err)
			}
		})
	}
}

func TestRepoVerifyRequiresMonotonicProgressAndCompletion(t *testing.T) {
	tests := []struct {
		name    string
		items   []map[string]any
		trailer string
		want    any
	}{
		{
			name:  "duplicate progress",
			items: []map[string]any{{"Msg": "", "Progress": 1}, {"Msg": "", "Progress": 1}, {"Msg": "verify complete, all blocks validated.", "Progress": 0}},
			want:  (*kubo.ProtocolError)(nil),
		},
		{
			name:  "missing completion",
			items: []map[string]any{{"Msg": "", "Progress": 1}},
			want:  (*kubo.ProtocolError)(nil),
		},
		{
			name:    "late trailer",
			items:   []map[string]any{{"Msg": "verify complete, all blocks validated.", "Progress": 0}},
			trailer: "late verify failure",
			want:    (*kubo.StreamError)(nil),
		},
		{
			name:  "item after completion",
			items: []map[string]any{{"Msg": "verify complete, all blocks validated.", "Progress": 0}, {"Msg": "", "Progress": 1}},
			want:  (*kubo.ProtocolError)(nil),
		},
		{
			name: "corruption cannot be masked by success",
			items: []map[string]any{
				{"Msg": "block bafy was corrupt", "Progress": 0},
				{"Msg": "verify complete, all blocks validated.", "Progress": 0},
			},
			want: (*kubo.ProtocolError)(nil),
		},
		{
			name:  "corrupt completion is not success",
			items: []map[string]any{{"Msg": "verify complete, 1 blocks corrupt", "Progress": 0}},
			want:  (*kubo.ProtocolError)(nil),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				beginJSONStream(w)
				for _, item := range test.items {
					writeJSON(t, w, item)
				}
				if test.trailer != "" {
					w.Header().Set("X-Stream-Error", test.trailer)
				}
			}))
			defer server.Close()
			_, err := newClient(t, server.URL, nil).RepoVerify(t.Context())
			switch test.want.(type) {
			case *kubo.ProtocolError:
				var target *kubo.ProtocolError
				if !errors.As(err, &target) {
					t.Fatalf("error = %T %v, want ProtocolError", err, err)
				}
			case *kubo.StreamError:
				var target *kubo.StreamError
				if !errors.As(err, &target) {
					t.Fatalf("error = %T %v, want StreamError", err, err)
				}
			}
		})
	}
}

func TestRepoVerifyParsesCompletionBeforeBearerRedaction(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		beginJSONStream(w)
		writeJSON(t, w, map[string]any{"Msg": "verify complete, all blocks validated.", "Progress": 0})
	}))
	defer server.Close()

	client := newClient(t, server.URL, func(c *kubo.Config) { c.BearerToken = "verify" })
	result, err := client.RepoVerify(t.Context())
	if err != nil {
		t.Fatalf("RepoVerify: %v", err)
	}
	if len(result.Messages) != 1 || strings.Contains(result.Messages[0], "verify") {
		t.Fatalf("redacted completion messages = %q", result.Messages)
	}
}

func TestRepoStatStrictSchema(t *testing.T) {
	for name, response := range map[string]string{
		"missing field":   `{"NumObjects":1,"RepoSize":2,"StorageMax":3,"RepoPath":"/repo"}`,
		"unknown field":   `{"NumObjects":1,"RepoSize":2,"StorageMax":3,"RepoPath":"/repo","Version":"fs-repo@19","Extra":1}`,
		"duplicate field": `{"NumObjects":1,"NumObjects":2,"RepoSize":2,"StorageMax":3,"RepoPath":"/repo","Version":"fs-repo@19"}`,
		"negative number": `{"NumObjects":-1,"RepoSize":2,"StorageMax":3,"RepoPath":"/repo","Version":"fs-repo@19"}`,
	} {
		t.Run(name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, response)
			}))
			defer server.Close()
			_, err := newClient(t, server.URL, nil).RepoStat(t.Context())
			var protocol *kubo.ProtocolError
			if !errors.As(err, &protocol) {
				t.Fatalf("error = %T %v, want ProtocolError", err, err)
			}
		})
	}
}
