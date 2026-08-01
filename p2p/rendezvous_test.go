package p2p_test

import (
	"strings"
	"testing"

	"github.com/ipfs/go-cid"

	"github.com/blobarchive/bloar/p2p"
)

func TestRendezvousCIDGoldenVectors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		net  string
		head string
		want string
	}{
		{net: "mainnet", head: "arbitrum-one", want: "bafkreihsko35wfj4kby6snqgnawudq576erfci2rkqp3fo4sndc3ryjape"},
		{net: "a/b", head: "c", want: "bafkreidybc6ssbzkml6yza7qmw2eq3reak24jwbjhvmhrbpnzmbdbzcegq"},
		{net: "a", head: "b/c", want: "bafkreie2gasmneiuxosz4azs6yc3yaorvsee5lxlmzn3zwdz547faxejcu"},
		{net: "网络", head: "头", want: "bafkreidstajb7xeg7vljhsodj324vtud7k6xuzlpauzdqoepwlreifrccy"},
	}

	for _, tt := range tests {
		t.Run(tt.net+"/"+tt.head, func(t *testing.T) {
			got, err := p2p.RendezvousCID(tt.net, tt.head)
			if err != nil {
				t.Fatal(err)
			}
			if got.String() != tt.want {
				t.Fatalf("RendezvousCID(%q, %q) = %s, want %s", tt.net, tt.head, got, tt.want)
			}
			if got.Version() != 1 || got.Type() != cid.Raw {
				t.Fatalf("RendezvousCID(%q, %q) prefix = v%d codec=%d, want CIDv1 raw", tt.net, tt.head, got.Version(), got.Type())
			}
		})
	}
}

func TestRendezvousCIDIsUnambiguous(t *testing.T) {
	t.Parallel()

	a, err := p2p.RendezvousCID("a/b", "c")
	if err != nil {
		t.Fatal(err)
	}
	b, err := p2p.RendezvousCID("a", "b/c")
	if err != nil {
		t.Fatal(err)
	}
	if a.Equals(b) {
		t.Fatalf("length-delimited namespace collided: %s", a)
	}
}

func TestRendezvousBlockMaterializesTheStableProviderKey(t *testing.T) {
	t.Parallel()

	key, err := p2p.RendezvousCID("mainnet", "arbitrum-one")
	if err != nil {
		t.Fatal(err)
	}
	block, err := p2p.RendezvousBlock("mainnet", "arbitrum-one")
	if err != nil {
		t.Fatal(err)
	}
	if !block.Cid().Equals(key) {
		t.Fatalf("rendezvous block CID = %s, want %s", block.Cid(), key)
	}
	rehashed, err := key.Prefix().Sum(block.RawData())
	if err != nil {
		t.Fatal(err)
	}
	if !rehashed.Equals(key) {
		t.Fatalf("rendezvous bytes hash to %s, want %s", rehashed, key)
	}
}

func TestRendezvousCIDRejectsInvalidComponents(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		net  string
		head string
	}{
		{name: "empty network", head: "head"},
		{name: "empty head", net: "net"},
		{name: "invalid network utf8", net: string([]byte{0xff}), head: "head"},
		{name: "invalid head utf8", net: "net", head: string([]byte{0xff})},
		{name: "network too long", net: strings.Repeat("n", 4097), head: "head"},
		{name: "head too long", net: "net", head: strings.Repeat("h", 4097)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got, err := p2p.RendezvousCID(tt.net, tt.head); err == nil || got.Defined() {
				t.Fatalf("RendezvousCID(%q, %q) = (%s, %v), want undefined CID and error", tt.net, tt.head, got, err)
			}
		})
	}
}
