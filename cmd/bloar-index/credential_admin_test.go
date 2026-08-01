package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// credentialChainConfig is a valid chain/publish-manifest config in the installed
// systemd form -- archive.token_file is the ${CREDENTIALS_DIRECTORY}/token
// credential reference (deploy/examples/arbitrum-one.yaml) -- with archiveURL
// pointed wherever the test needs.
func credentialChainConfig(archiveURL string) string {
	return `
beacon:
  genesis_time: 1606824023
archive:
  url: ` + archiveURL + `
  token_file: "${CREDENTIALS_DIRECTORY}/token"
  head: arbitrum-one
chain:
  parent_chain_rpc: http://127.0.0.1:1
  sources:
    - type: inbox-events
      address: "0x1c479675ad559DC151F6Ec7ed3FbF8ceE79582B6"
      from_block: 0
`
}

// TestPublishManifestNakedCredentialConfigFails reproduces the credential-handoff failure:
// `bloar-index publish-manifest -config <installed credential-form config>` run
// by hand (no CREDENTIALS_DIRECTORY) fails on the token before doing any work.
// This is why the runbook (§7.5) documents the -token-file override that the next
// test exercises.
func TestPublishManifestNakedCredentialConfigFails(t *testing.T) {
	t.Setenv("CREDENTIALS_DIRECTORY", "")
	cfgPath := writeConfig(t, credentialChainConfig("http://127.0.0.1:1"))

	err := run([]string{"publish-manifest", "-config", cfgPath})
	if err == nil {
		t.Fatal("publish-manifest with a credential-form config and no CREDENTIALS_DIRECTORY unexpectedly succeeded")
	}
	if !strings.Contains(err.Error(), "CREDENTIALS_DIRECTORY") {
		t.Fatalf("error does not name the missing variable: %v", err)
	}
	if !strings.Contains(err.Error(), "archive.token_file") {
		t.Fatalf("error does not name the key: %v", err)
	}
}

// TestPublishManifestTokenFileOverrideLoadsOffline is the runbook regression for
// the documented hand-run command that BOTH §7.5 (manifest upgrade) and §7.6
// (missed-source recovery, step 4) repeat: `bloar-index publish-manifest -config
// <instance file> -token-file /etc/bloar/token`. The same credential-form config
// plus `-token-file <plain path>` loads the token and gets past auth with no
// CREDENTIALS_DIRECTORY. The archive answers the limits probe with a non-retryable
// 400, so the command stops there -- immediately after the token load -- rather
// than retrying an unreachable host; the assertion is that the failure is the
// archive, never the credential. Removing the -token-file handling makes this fail
// on the token, the exact failure the fix removes.
func TestPublishManifestTokenFileOverrideLoadsOffline(t *testing.T) {
	t.Setenv("CREDENTIALS_DIRECTORY", "")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer srv.Close()

	cfgPath := writeConfig(t, credentialChainConfig(srv.URL))
	plainToken := tokenFile(t, "s3cret\n")

	err := run([]string{"publish-manifest", "-config", cfgPath, "-token-file", plainToken})
	if err == nil {
		t.Fatal("publish-manifest against a 400 archive unexpectedly succeeded")
	}
	if strings.Contains(err.Error(), "CREDENTIALS_DIRECTORY") || strings.Contains(err.Error(), "token_file") {
		t.Fatalf("publish-manifest failed on the token, not past it: %v", err)
	}
	if !strings.Contains(err.Error(), "limits") {
		t.Fatalf("expected the archive-limits failure that follows the token load, got: %v", err)
	}
}

// TestPublishManifestSendsCredentialToken drives publish-manifest all the way to
// its AUTHENTICATED MUTATION -- POST /bloar/v1/heads/{head}/manifest -- against a
// fake archive that serves the read steps, and asserts the Authorization header
// AT THAT POST is exactly the loaded token. A fresh head has no manifest, so the
// preflight skips the L1 position check and goes straight to the POST (see
// index/chain preflightManifest), which is what lets an in-process test reach a
// real mutation.
//
// This is the load-bearing auth check, independent of the privileged verifier:
// replacing archiveClient's token with a constant makes the POST carry a
// different Bearer (fails the value assertion), and discarding it (empty) makes
// archclient.New refuse before any request (the POST is never reached).
func TestPublishManifestSendsCredentialToken(t *testing.T) {
	t.Setenv("CREDENTIALS_DIRECTORY", "")
	const head = "arbitrum-one"
	var mu sync.Mutex
	var postAuth string
	var posted bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/bloar/v1/heads":
			_, _ = w.Write([]byte(`{"max_put_blobs":64}`))
		case r.Method == http.MethodGet && r.URL.Path == "/bloar/v1/heads/"+head:
			_, _ = w.Write([]byte(`{"name":"` + head + `","root":"bafyreiabc","origin_slot":8626176,"synced_to":null}`))
		case r.Method == http.MethodGet && r.URL.Path == "/bloar/v1/heads/"+head+"/manifest":
			w.WriteHeader(http.StatusNotFound) // no manifest yet -> a genesis publish
		case r.Method == http.MethodPost && r.URL.Path == "/bloar/v1/heads/"+head+"/manifest":
			mu.Lock()
			postAuth = r.Header.Get("Authorization")
			posted = true
			mu.Unlock()
			_, _ = w.Write([]byte(`{"manifest":"bafyreitip"}`))
		default:
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer srv.Close()

	cfgPath := writeConfig(t, credentialChainConfig(srv.URL))
	plainToken := tokenFile(t, "s3cret\n")

	if err := run([]string{"publish-manifest", "-config", cfgPath, "-token-file", plainToken}); err != nil {
		t.Fatalf("publish-manifest against the fake archive failed before completing the mutation: %v", err)
	}
	mu.Lock()
	auth, ok := postAuth, posted
	mu.Unlock()
	if !ok {
		t.Fatal("the authenticated manifest POST was never reached")
	}
	if auth != "Bearer s3cret" {
		t.Fatalf("the manifest POST carried Authorization %q, want %q; the loaded token did not reach the mutation", auth, "Bearer s3cret")
	}
}
