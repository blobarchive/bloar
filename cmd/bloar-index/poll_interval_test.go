package main

// focused regression for the indexer config boundary's half of finding
// the safety boundary: a non-positive index.poll_interval is rejected at LoadConfig, rather
// than accepted and left to turn each indexer's caught-up wait into an immediate
// upstream-hammering loop.

import (
	"strings"
	"testing"
)

func TestIndexNegativePollIntervalRejected(t *testing.T) {
	body := beaconConfig(t) + `
index:
  poll_interval: -1s
`
	_, err := LoadConfig(writeConfig(t, body), "beacon")
	if err == nil {
		t.Fatal("a negative index.poll_interval was accepted; it must be rejected")
	}
	if !strings.Contains(err.Error(), "index.poll_interval is -1s, must be positive") {
		t.Fatalf("error = %q, want it to name the non-positive poll interval", err)
	}
}
