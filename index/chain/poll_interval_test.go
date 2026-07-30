package chain

// focused proof that the chain indexer now rejects a non-positive poll
// interval at construction, rather than accepting it and turning
// its caught-up wait and waitForAll into immediate repeated archive reads.

import (
	"strings"
	"testing"
	"time"
)

func TestChainNegativePollIntervalRejectedByNew(t *testing.T) {
	b := newChainBuilder(t)
	b.addBlock(0)
	_, client := newAuditManifestArchive(t, []Source{inboxOpen(testInbox, 0)})
	_, err := New(Config{
		Chain:          b.chain(),
		Archive:        client,
		Head:           "audit",
		AllHead:        "all",
		Sources:        []Source{inboxOpen(testInbox, 0)},
		GenesisTime:    testGenesis,
		SecondsPerSlot: testSPS,
		BlockRange:     1,
		PollInterval:   -time.Second,
	})
	if err == nil {
		t.Fatal("chain.New accepted a negative poll_interval; it must be rejected")
	}
	if !strings.Contains(err.Error(), "Config.PollInterval is -1s, must be positive") {
		t.Fatalf("error = %q, want it to name the non-positive poll interval", err)
	}
}
