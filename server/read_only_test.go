package server_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/blobarchive/bloar/catalog"
	"github.com/blobarchive/bloar/ingest"
	"github.com/blobarchive/bloar/server"
)

func TestReadOnlyServerMountsOnlyPublicReads(t *testing.T) {
	stack := newStack(t, stackOpts{})
	handler, err := server.New(server.Config{
		ReadOnly: true,
		Heads:    stack.heads,
		Blocks:   stack.store.Blocks(),
		Beacon: server.Beacon{
			GenesisTime:           1606824023,
			SecondsPerSlot:        12,
			GenesisValidatorsRoot: "0x4b363db94e286120d76eb905340fdd4e54bfe9f06bf33ff6cf5ad27f511bfe95",
			GenesisForkVersion:    "0x00000000",
		},
	})
	if err != nil {
		t.Fatalf("server.New(read-only): %v", err)
	}

	read := httptest.NewRequest(http.MethodGet, "/all/eth/v1/beacon/genesis", nil)
	read.SetPathValue("head", "all")
	readResult := httptest.NewRecorder()
	handler.ServeHTTP(readResult, read)
	if readResult.Code != http.StatusOK {
		t.Fatalf("GET genesis status = %d, body %s", readResult.Code, readResult.Body.String())
	}

	for _, path := range []string{
		"/bloar/v1/blobs",
		"/bloar/v1/heads/all/refs",
		"/bloar/v1/heads/all/truncate",
		"/bloar/v1/heads/all/manifest",
		"/bloar/v1/heads/all/generation",
	} {
		t.Run(path, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{}`))
			result := httptest.NewRecorder()
			handler.ServeHTTP(result, request)
			if result.Code != http.StatusNotFound {
				t.Fatalf("POST %s status = %d, want 404; body %s", path, result.Code, result.Body.String())
			}
		})
	}
}

func TestReadOnlyServerRejectsMutationCapabilities(t *testing.T) {
	stack := newStack(t, stackOpts{})
	base := server.Config{
		ReadOnly: true,
		Heads:    stack.heads,
		Blocks:   stack.store.Blocks(),
		Beacon:   server.Beacon{SecondsPerSlot: 12},
	}

	withToken := base
	withToken.AuthToken = "must-not-be-accepted"
	if _, err := server.New(withToken); err == nil || !strings.Contains(err.Error(), "must not carry an AuthToken") {
		t.Fatalf("read-only token error = %v", err)
	}

	ingester, err := ingest.New(ingest.Config{
		Blocks: stack.store.Blocks(), Catalog: catalog.New(stack.store.KV()),
	})
	if err != nil {
		t.Fatal(err)
	}
	withIngester := base
	withIngester.Ingester = ingester
	if _, err := server.New(withIngester); err == nil || !strings.Contains(err.Error(), "must not carry an Ingester") {
		t.Fatalf("read-only ingester error = %v", err)
	}

	// The ordinary server contract remains fail-closed: omitting read-only mode
	// does not make a nil ingester or empty token acceptable.
	writer := base
	writer.ReadOnly = false
	if _, err := server.New(writer); err == nil || !strings.Contains(err.Error(), "Ingester must not be nil") {
		t.Fatalf("writer without ingester error = %v", err)
	}
}
