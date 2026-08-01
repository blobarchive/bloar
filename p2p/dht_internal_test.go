package p2p

import (
	"context"
	"errors"
	"testing"

	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
)

func TestMergeBootstrapPeersUnionsPeersAndAddressesWithoutAliasing(t *testing.T) {
	t.Parallel()
	p1, p2, p3 := rendezvousPeerID(t), rendezvousPeerID(t), rendezvousPeerID(t)
	a := ma.StringCast("/ip4/192.0.2.1/tcp/4001")
	b := ma.StringCast("/ip4/192.0.2.2/tcp/4001")
	c := ma.StringCast("/ip4/192.0.2.1/udp/4001/quic-v1")
	d := ma.StringCast("/ip4/192.0.2.3/tcp/4001")
	explicit := []peer.AddrInfo{
		{ID: p1, Addrs: []ma.Multiaddr{a}},
		{ID: p2, Addrs: []ma.Multiaddr{b}},
	}
	public := []peer.AddrInfo{
		{ID: p1, Addrs: []ma.Multiaddr{a, c}},
		{ID: p3, Addrs: []ma.Multiaddr{d}},
	}

	got := MergeBootstrapPeers(explicit, public)
	if len(got) != 3 {
		t.Fatalf("merged peers = %d, want 3", len(got))
	}
	if got[0].ID != p1 || got[1].ID != p2 || got[2].ID != p3 {
		t.Fatalf("merged order = %v, want first-occurrence order [%s %s %s]", []peer.ID{got[0].ID, got[1].ID, got[2].ID}, p1, p2, p3)
	}
	if len(got[0].Addrs) != 2 || !got[0].Addrs[0].Equal(a) || !got[0].Addrs[1].Equal(c) {
		t.Fatalf("merged addresses for duplicate peer = %v, want [%s %s]", got[0].Addrs, a, c)
	}

	got[0].Addrs = append(got[0].Addrs, d)
	if len(explicit[0].Addrs) != 1 || len(public[0].Addrs) != 2 {
		t.Fatal("merged result aliased an input address slice")
	}
}

func TestPublicAminoBootstrapPeersAreNonemptyAndDeduplicated(t *testing.T) {
	t.Parallel()
	got := PublicAminoBootstrapPeers()
	if len(got) == 0 {
		t.Fatal("public Amino bootstrap set is empty")
	}
	seenPeers := make(map[peer.ID]bool, len(got))
	for _, info := range got {
		if info.ID == "" || len(info.Addrs) == 0 {
			t.Fatalf("invalid public bootstrap entry: %+v", info)
		}
		if seenPeers[info.ID] {
			t.Fatalf("public bootstrap peer %s was not deduplicated", info.ID)
		}
		seenPeers[info.ID] = true
	}
}

type recordingDHTBootstrapper struct {
	called int
	err    error
}

func (d *recordingDHTBootstrapper) Bootstrap(context.Context) error {
	d.called++
	return d.err
}

func TestBootstrapDHTInvokesBootstrapAndPropagatesError(t *testing.T) {
	t.Parallel()
	want := errors.New("bootstrap failed")
	fake := &recordingDHTBootstrapper{err: want}
	if err := bootstrapDHT(t.Context(), fake); !errors.Is(err, want) {
		t.Fatalf("bootstrapDHT error = %v, want %v", err, want)
	}
	if fake.called != 1 {
		t.Fatalf("Bootstrap calls = %d, want 1", fake.called)
	}
}
