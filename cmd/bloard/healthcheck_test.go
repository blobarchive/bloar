package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHealthcheckFlagRequiresReadyStatus(t *testing.T) {
	status := http.StatusOK
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/readyz" {
			t.Errorf("request = %s %s, want GET /readyz", r.Method, r.URL.Path)
		}
		w.WriteHeader(status)
	}))
	defer server.Close()
	listen := strings.TrimPrefix(server.URL, "http://")
	body := strings.Replace(specExample, "server:\n", "server:\n  metrics_listen: "+listen+"\n", 1)
	path := writeFile(t, "healthcheck.yaml", body)

	if err := run([]string{"-config", path, "-healthcheck"}, io.Discard); err != nil {
		t.Fatalf("ready healthcheck: %v", err)
	}
	status = http.StatusServiceUnavailable
	if err := run([]string{"-healthcheck", "-config", path}, io.Discard); err == nil {
		t.Fatal("healthcheck accepted a non-ready daemon")
	}
}

func TestReadinessURLFollowsMetricsListener(t *testing.T) {
	for listen, want := range map[string]string{
		"0.0.0.0:9550":   "http://127.0.0.1:9550/readyz",
		":9550":          "http://127.0.0.1:9550/readyz",
		"[::]:9550":      "http://[::1]:9550/readyz",
		"127.0.0.2:9550": "http://127.0.0.2:9550/readyz",
	} {
		got, err := readinessURL(listen)
		if err != nil || got != want {
			t.Fatalf("readinessURL(%q) = %q, %v; want %q", listen, got, err, want)
		}
	}
	if _, err := readinessURL(""); err == nil {
		t.Fatal("empty metrics listener accepted")
	}
}
