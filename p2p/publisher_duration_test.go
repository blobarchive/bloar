package p2p_test

// focused regression for the IPNS publisher's half of the safety boundary: a
// non-positive republish interval or record lifetime is rejected at construction,
// rather than accepted and left to fire run's timer immediately and continuously,
// or to sign an already-expired record.

import (
	"strings"
	"testing"
	"time"

	"github.com/blobarchive/bloar/p2p"
)

func TestPublisherNonPositiveDurationsRejected(t *testing.T) {
	h := newTestHost(t)
	docs := newTestDocs(t, memBlocks())
	kv := memKV(t)
	vs := newMemRouting()

	for _, tc := range []struct {
		name string
		cfg  p2p.PublisherConfig
		want string
	}{
		{
			name: "republish",
			cfg:  p2p.PublisherConfig{Host: h, Docs: docs, Routing: vs, Provider: vs, KV: kv, Republish: -time.Second},
			want: "PublisherConfig.Republish is -1s, must be positive",
		},
		{
			name: "lifetime",
			cfg:  p2p.PublisherConfig{Host: h, Docs: docs, Routing: vs, Provider: vs, KV: kv, Lifetime: -time.Second},
			want: "PublisherConfig.Lifetime is -1s, must be positive",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := p2p.NewPublisher(tc.cfg); err == nil {
				t.Fatalf("NewPublisher accepted a negative %s; it must be rejected", tc.name)
			} else if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %q, want it to contain %q", err, tc.want)
			}
		})
	}
}
