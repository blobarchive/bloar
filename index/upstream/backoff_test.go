package upstream_test

// focused regression for the upstream client's half of the safety boundary: a
// non-positive retry backoff is rejected at construction, rather than accepted
// and left to retry with no delay. No config key feeds this; main.go's Config is
// the caller the constructor boundary guards.

import (
	"strings"
	"testing"
	"time"

	"github.com/blobarchive/bloar/index/upstream"
)

func TestUpstreamNegativeBackoffRejected(t *testing.T) {
	_, err := upstream.New(upstream.Config{BaseURL: "https://beacon.invalid", Backoff: -time.Second})
	if err == nil {
		t.Fatal("upstream.New accepted a negative Backoff; it must be rejected")
	}
	if !strings.Contains(err.Error(), "Config.Backoff is -1s, must be positive") {
		t.Fatalf("error = %q, want it to name the non-positive backoff", err)
	}
	// NewBlockClient shares the same Config and constructor, so it is guarded too.
	if _, err := upstream.NewBlockClient(upstream.Config{BaseURL: "https://beacon.invalid", Backoff: -time.Second}); err == nil {
		t.Fatal("upstream.NewBlockClient accepted a negative Backoff; it must be rejected")
	}
}
