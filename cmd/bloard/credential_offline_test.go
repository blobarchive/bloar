package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// credentialFormConfig is a full, valid bloard config in the installed systemd
// form -- server.auth_token_file is the ${CREDENTIALS_DIRECTORY}/token credential
// reference (deploy/examples/writer.yaml) -- with store.path pointed at a scratch
// directory so the offline commands can actually run.
func credentialFormConfig(storePath string) string {
	return `
net: mainnet
beacon:
  genesis_time: 1606824023
  seconds_per_slot: 12
store:
  path: ` + storePath + `
server:
  listen: "127.0.0.1:0"
  auth_token_file: "${CREDENTIALS_DIRECTORY}/token"
heads:
  all: {}
`
}

// TestRebuildLoadsCredentialConfigOffline is the runbook regression for §7.3:
// `bloard rebuild -config <installed credential-form config>` must work with NO
// CREDENTIALS_DIRECTORY in the environment. rebuild never consumes auth, and once
// credential resolution moved out of LoadConfig (to AuthToken) the config loads
// and the walk runs. Moving resolution back into LoadConfig makes this fail at
// config load -- the exact failure the reviewer reproduced with writer.yaml.
func TestRebuildLoadsCredentialConfigOffline(t *testing.T) {
	t.Setenv("CREDENTIALS_DIRECTORY", "")
	storePath := t.TempDir()
	seedBlobs(t, storePath, 2)
	cfgPath := writeFile(t, "bloard.yaml", credentialFormConfig(storePath))

	var out bytes.Buffer
	if err := run([]string{"rebuild", "-config", cfgPath}, &out); err != nil {
		t.Fatalf("rebuild of a credential-form config with no CREDENTIALS_DIRECTORY failed: %v", err)
	}
	if !strings.Contains(out.String(), "blobs:    2") {
		t.Errorf("rebuild did not walk the store:\n%s", out.String())
	}
}

// TestServeFailsClosedWithoutCredentialDir proves the fail-closed half of the
// deferred resolution: serve() refuses a credential-form config with no
// CREDENTIALS_DIRECTORY at the token read, which is its FIRST step -- before it
// opens the store or binds any listener. A fail-closed serve() therefore never
// creates the store, which is what this asserts. Replacing AuthToken()'s result
// with a constant (ignoring the credential error) makes serve() proceed past the
// token read and open the store, failing this test -- so the check is load-bearing.
func TestServeFailsClosedWithoutCredentialDir(t *testing.T) {
	t.Setenv("CREDENTIALS_DIRECTORY", "")
	storePath := filepath.Join(t.TempDir(), "store-should-not-exist")

	cfg := loadString(t, `
net: mainnet
beacon:
  genesis_time: 1606824023
  seconds_per_slot: 12
store:
  path: `+storePath+`
server:
  listen: "127.0.0.1:0"
  auth_token_file: "${CREDENTIALS_DIRECTORY}/token"
heads:
  all: {}
`)

	// Bound serve() so a mutation that lets it proceed to run cannot hang the test.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	err := serve(ctx, cfg)

	if err == nil || !strings.Contains(err.Error(), "CREDENTIALS_DIRECTORY") {
		t.Fatalf("serve() did not fail closed on the unset credential directory: %v", err)
	}
	// store.Open, the metrics listener, and the API listener are all AFTER the
	// token read in serve(); a store that was never created proves serve() failed
	// before every one of them.
	if _, statErr := os.Stat(storePath); !os.IsNotExist(statErr) {
		t.Errorf("serve() created the store %s; it did not fail before opening it (or binding a listener)", storePath)
	}
}
