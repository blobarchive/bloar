package p2p_test

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"

	"github.com/ipfs/boxo/namesys"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/blobarchive/bloar/p2p"
)

func TestResolveDNSLinkNameOneHop(t *testing.T) {
	_, public, err := crypto.GenerateEd25519Key(nil)
	if err != nil {
		t.Fatal(err)
	}
	id, err := peer.IDFromPublicKey(public)
	if err != nil {
		t.Fatal(err)
	}
	want := id.String()

	lookup := func(_ context.Context, query string) ([]string, error) {
		if query != "_dnslink.swarm.example." {
			t.Fatalf("TXT query = %q", query)
		}
		return []string{"unrelated=value", "dnslink=/ipns/" + want}, nil
	}
	got, err := p2p.ResolveDNSLinkName(t.Context(), "swarm.example", lookup)
	if err != nil {
		t.Fatal(err)
	}
	if got.Peer() != id {
		t.Fatalf("resolved peer = %s, want %s", got.Peer(), id)
	}
}

func TestResolveDNSLinkNameRejectsNonSignerTargets(t *testing.T) {
	_, public, err := crypto.GenerateEd25519Key(nil)
	if err != nil {
		t.Fatal(err)
	}
	id, err := peer.IDFromPublicKey(public)
	if err != nil {
		t.Fatal(err)
	}
	block, err := cid.Parse("bafkreihsko35wfj4kby6snqgnawudq576erfci2rkqp3fo4sndc3ryjape")
	if err != nil {
		t.Fatal(err)
	}

	tests := map[string]string{
		"immutable content": "/ipfs/" + block.String(),
		"path suffix":       "/ipns/" + id.String() + "/latest",
		"recursive DNS":     "/ipns/next.example",
	}
	for name, target := range tests {
		t.Run(name, func(t *testing.T) {
			lookup := func(context.Context, string) ([]string, error) {
				return []string{"dnslink=" + target}, nil
			}
			if _, err := p2p.ResolveDNSLinkName(t.Context(), "swarm.example", lookup); err == nil {
				t.Fatalf("accepted DNSLink target %q", target)
			}
		})
	}
}

func TestResolveDNSLinkNameRejectsAmbiguousAndMissingRecords(t *testing.T) {
	_, public, err := crypto.GenerateEd25519Key(nil)
	if err != nil {
		t.Fatal(err)
	}
	id, err := peer.IDFromPublicKey(public)
	if err != nil {
		t.Fatal(err)
	}
	target := "dnslink=/ipns/" + id.String()

	tests := map[string][]string{
		"ambiguous": {target, target},
		"missing":   {"not-dnslink=/ipns/" + id.String()},
	}
	for name, records := range tests {
		t.Run(name, func(t *testing.T) {
			lookup := func(context.Context, string) ([]string, error) { return records, nil }
			if _, err := p2p.ResolveDNSLinkName(t.Context(), "swarm.example", lookup); err == nil {
				t.Fatalf("accepted records %q", records)
			}
		})
	}
}

func TestResolveDNSLinkNamePropagatesLookupAndContextErrors(t *testing.T) {
	sentinel := errors.New("resolver unavailable")
	lookup := func(context.Context, string) ([]string, error) { return nil, sentinel }
	if _, err := p2p.ResolveDNSLinkName(t.Context(), "swarm.example", lookup); !errors.Is(err, sentinel) {
		t.Fatalf("error = %v, want resolver sentinel", err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	blocking := func(ctx context.Context, _ string) ([]string, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if _, err := p2p.ResolveDNSLinkName(ctx, "swarm.example", blocking); !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context canceled", err)
	}
}

func TestResolveDNSLinkNameValidatesInputs(t *testing.T) {
	if _, err := p2p.ResolveDNSLinkName(t.Context(), "swarm.example", nil); err == nil {
		t.Fatal("nil TXT lookup accepted")
	}

	lookup := func(context.Context, string) ([]string, error) {
		return nil, &net.DNSError{IsNotFound: true}
	}
	if _, err := p2p.ResolveDNSLinkName(t.Context(), "bad/domain", lookup); err == nil || !strings.Contains(err.Error(), "invalid DNSLink domain") {
		t.Fatalf("invalid domain error = %v", err)
	}

	if _, err := p2p.ResolveDNSLinkName(t.Context(), "swarm.example", lookup); !errors.Is(err, namesys.ErrMissingDNSLinkRecord) {
		t.Fatalf("missing record error = %v", err)
	}
}

func TestValidateDNSLinkDomain(t *testing.T) {
	for _, domain := range []string{"swarm.example", "swarm.example."} {
		if err := p2p.ValidateDNSLinkDomain(domain); err != nil {
			t.Errorf("ValidateDNSLinkDomain(%q): %v", domain, err)
		}
	}
	for _, domain := range []string{"", " swarm.example", "swarm.example/path", "https://swarm.example", strings.Repeat("a", 64) + ".example"} {
		if err := p2p.ValidateDNSLinkDomain(domain); err == nil {
			t.Errorf("ValidateDNSLinkDomain(%q) accepted an invalid domain", domain)
		}
	}
}
