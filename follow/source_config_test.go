package follow

import (
	"context"
	"crypto/ed25519"
	"strings"
	"testing"

	"github.com/libp2p/go-libp2p/core/routing"

	"github.com/blobarchive/bloar/pinning"
	"github.com/blobarchive/bloar/server"
)

func sourceConfigArchiveID(t *testing.T) server.ArchiveID {
	t.Helper()
	id, err := server.ParseArchiveID("0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20")
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func TestValidateAndCloneSourceSetCanonicalizesAndDetaches(t *testing.T) {
	id := sourceConfigArchiveID(t)
	sources := []SourceConfig{
		{
			ID: "writer-b", URL: "https://B.EXAMPLE.ORG:00443/",
			PubKey: ed25519.PublicKey(strings.Repeat("b", ed25519.PublicKeySize)), AllowedHeads: []string{"all"},
		},
		{
			ID: "writer-a", URL: "https://A.EXAMPLE.ORG/",
			PubKey: ed25519.PublicKey(strings.Repeat("a", ed25519.PublicKeySize)), AllowedHeads: []string{"tip", "all"},
		},
	}
	digest, err := SourceSetDigest("mainnet", id, sources)
	if err != nil {
		t.Fatal(err)
	}
	cfg := Config{
		Net: "mainnet", ExpectedArchiveID: &id,
		SourceSet: &SourceSetConfig{
			Revision: 7, Digest: digest, Sources: sources, MigrateLegacySource: "writer-a",
		},
		Heads: map[string]pinning.Policy{"all": pinning.Full(), "tip": pinning.Full()},
		ExpectedKinds: map[string]server.HeadKind{
			"all": server.FinalizedMonotonic, "tip": server.UnfinalizedMutable,
		},
	}

	got, err := validateAndCloneSourceSet(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if got.Revision != 7 || got.Digest != digest || got.MigrateLegacySource != "writer-a" || got.MigrateLegacyIPNS != "" {
		t.Fatalf("validated source set metadata = %+v", got)
	}
	if len(got.Sources) != 2 || got.Sources[0].ID != "writer-a" || got.Sources[1].ID != "writer-b" {
		t.Fatalf("source order = %+v", got.Sources)
	}
	if got.Sources[0].URL != "https://a.example.org" || got.Sources[1].URL != "https://b.example.org" {
		t.Fatalf("normalized URLs = %q, %q", got.Sources[0].URL, got.Sources[1].URL)
	}
	if strings.Join(got.Sources[0].AllowedHeads, ",") != "all,tip" {
		t.Fatalf("normalized allowed heads = %v", got.Sources[0].AllowedHeads)
	}

	// Neither direction aliases caller-owned authority data.
	sources[1].PubKey[0] = 'x'
	sources[1].AllowedHeads[0] = "changed"
	if got.Sources[0].PubKey[0] != 'a' || strings.Join(got.Sources[0].AllowedHeads, ",") != "all,tip" {
		t.Fatal("validated source set aliases its input")
	}
	got.Sources[0].PubKey[1] = 'y'
	got.Sources[0].AllowedHeads[0] = "changed-again"
	if cfg.SourceSet.Sources[1].PubKey[1] != 'a' || cfg.SourceSet.Sources[1].AllowedHeads[0] != "changed" {
		t.Fatal("input source set aliases the validated copy")
	}
}

func TestValidateAndCloneSourceSetRejectsUnacknowledgedRuntimeRoster(t *testing.T) {
	id := sourceConfigArchiveID(t)
	key := ed25519.PublicKey(strings.Repeat("a", ed25519.PublicKeySize))
	cfg := Config{
		Net: "mainnet", ExpectedArchiveID: &id,
		SourceSet: &SourceSetConfig{
			Revision: 1,
			Sources:  []SourceConfig{{ID: "writer-a", URL: "https://writer.example", PubKey: key, AllowedHeads: []string{"all"}}},
		},
		Heads: map[string]pinning.Policy{"all": pinning.Full()},
	}
	if _, err := validateAndCloneSourceSet(cfg); err == nil || !strings.Contains(err.Error(), "does not acknowledge") {
		t.Fatalf("unacknowledged source set error = %v", err)
	}
}

func TestValidateAndCloneSourceSetDerivesDirectMigrationName(t *testing.T) {
	id := sourceConfigArchiveID(t)
	const name = "k51qzi5uqu5dmc9hz7x2fd156p883lc3w1i36tu4i4r0yd7ohnd4a12j9zeun8"
	sources := []SourceConfig{{
		ID: "writer-a", IPNS: name,
		PubKey: ed25519.PublicKey(strings.Repeat("a", ed25519.PublicKeySize)), AllowedHeads: []string{"all"},
	}}
	digest, err := SourceSetDigest("mainnet", id, sources)
	if err != nil {
		t.Fatal(err)
	}
	cfg := Config{
		Net: "mainnet", ExpectedArchiveID: &id,
		SourceSet: &SourceSetConfig{
			Revision: 1, Digest: digest, Sources: sources,
			MigrateLegacySource: "writer-a", MigrateLegacyIPNS: name,
		},
		Heads: map[string]pinning.Policy{"all": pinning.Full()},
		// A non-nil routing seam is enough for pure construction validation.
		Routing: sourceConfigValueStore{},
	}
	got, err := validateAndCloneSourceSet(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if got.MigrateLegacyIPNS != name {
		t.Fatalf("migration IPNS = %q", got.MigrateLegacyIPNS)
	}
}

// sourceConfigValueStore is never called: source-set construction validation
// is deliberately read-only and performs no resolution.
type sourceConfigValueStore struct{}

func (sourceConfigValueStore) PutValue(context.Context, string, []byte, ...routing.Option) error {
	panic("unexpected network call")
}

func (sourceConfigValueStore) GetValue(context.Context, string, ...routing.Option) ([]byte, error) {
	panic("unexpected network call")
}

func (sourceConfigValueStore) SearchValue(context.Context, string, ...routing.Option) (<-chan []byte, error) {
	panic("unexpected network call")
}
