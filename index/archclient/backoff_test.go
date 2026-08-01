package archclient_test

// focused regression for the archive client's half of the safety boundary: a
// non-positive retry backoff is rejected at construction, rather than accepted
// and left to retry with no delay. No config key feeds this; main.go's Config is
// the caller the constructor boundary guards.

import (
	"strings"
	"testing"
	"time"

	"github.com/blobarchive/bloar/index/archclient"
)

func TestArchclientNegativeBackoffRejected(t *testing.T) {
	_, err := archclient.New(archclient.Config{BaseURL: "https://archive.invalid", Token: "t", Backoff: -time.Second})
	if err == nil {
		t.Fatal("archclient.New accepted a negative Backoff; it must be rejected")
	}
	if !strings.Contains(err.Error(), "Config.Backoff is -1s, must be positive") {
		t.Fatalf("error = %q, want it to name the non-positive backoff", err)
	}
}
