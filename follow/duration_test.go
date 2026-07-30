package follow_test

import (
	"crypto/ed25519"
	"strings"
	"testing"
	"time"

	"github.com/blobarchive/bloar/catalog"
	"github.com/blobarchive/bloar/follow"
	"github.com/blobarchive/bloar/pinning"
	"github.com/blobarchive/bloar/server"
	"github.com/blobarchive/bloar/store"
)

// TestNonPositiveDurationsRejectedByNew is the fixed counterpart of the
// panic the audit recorded: New rejects a non-positive poll interval or fetch
// timeout at construction, so Run never reaches time.NewTicker
// with a negative value and the follower's bounded reads never build a pre-expired
// context. The rejection is an ordinary startup error, not a panic.
func TestNonPositiveDurationsRejectedByNew(t *testing.T) {
	st, err := store.Open(t.TempDir(), store.WithPebbleLogger(quietPebble{}))
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() {
		if err := st.Close(); err != nil {
			t.Errorf("store.Close: %v", err)
		}
	})

	roots := server.NewRootStore(st.KV())
	registry, err := server.NewHeads(server.HeadsConfig{Net: testNet, Roots: roots})
	if err != nil {
		t.Fatalf("server.NewHeads: %v", err)
	}
	rec, err := pinning.NewReconciler(pinning.Config{Ledger: catalog.NewLedger(st.KV())})
	if err != nil {
		t.Fatalf("pinning.NewReconciler: %v", err)
	}
	pub, _, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("ed25519.GenerateKey: %v", err)
	}

	base := func() follow.Config {
		return follow.Config{
			Net:        testNet,
			URL:        "https://writer.invalid",
			PubKey:     pub,
			Heads:      map[string]pinning.Policy{testHead: pinning.Full()},
			Local:      st.Blocks(),
			Sessions:   auditNoFetchSessions{},
			Registry:   registry,
			Roots:      roots,
			Reconciler: rec,
			KV:         st.KV(),
		}
	}

	for _, tc := range []struct {
		name string
		set  func(*follow.Config)
		want string
	}{
		{"poll interval", func(c *follow.Config) { c.PollInterval = -time.Second }, "Config.PollInterval is -1s, must be positive"},
		{"fetch timeout", func(c *follow.Config) { c.FetchTimeout = -time.Second }, "Config.FetchTimeout is -1s, must be positive"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := base()
			tc.set(&cfg)
			f, err := follow.New(cfg)
			if err == nil {
				_ = f.Close()
				t.Fatalf("New accepted a negative %s; it must be rejected", tc.name)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %q, want it to contain %q", err, tc.want)
			}
		})
	}
}
