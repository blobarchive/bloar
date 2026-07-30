package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble/v2"

	"github.com/blobarchive/bloar/server"
)

const validProfileBundle = `
schema: bloar.follow-profile-bundle/v1
profiles:
  - schema: bloar.follow-profile/v1
    name: fixture-mainnet
    version: 1
    aliases: [fixture]
    provenance:
      source: hermetic test fixture
      revision: fixture-v1
    network:
      name: mainnet
      beacon:
        genesis_time: 1606824023
        seconds_per_slot: 12
        genesis_validators_root: "0x4b363db94e286120d76eb905340fdd4e54bfe9f06bf33ff6cf5ad27f511bfe95"
        genesis_fork_version: "0x00000000"
        spec_extra:
          CONFIG_NAME: fixture
    trust:
      mode: dnslink-delegated
      dnslink: writer.fixture.invalid
      url: https://writer.fixture.invalid/heads
    heads:
      fixture-head:
        pin: {mode: window, duration: 720h}
`

func TestEmbeddedProductionFollowProfiles(t *testing.T) {
	profiles, err := decodeProfileBundle([]byte(embeddedProfileBundle), "built-in")
	if err != nil {
		t.Fatalf("decodeProfileBundle: %v", err)
	}
	const signer = "6698f6c8767529ffb725ce5201a86602106cc87ed7c9129a649428ca0ea6d7b5"
	want := map[string]struct {
		dnslink          string
		heads            []string
		digest           string
		liveFinalized    string
		expectedHandoff  string
		requireExactHash bool
	}{
		"ethereum-mainnet-all-a": {
			dnslink: "ethereum-mainnet-all-a.blobarchive.net",
			heads:   []string{"all"},
			digest:  "sha256:9c175ccedc95e9a0e910a128eadd4c5bd767ce980e1bbe495d65b76ad83d978e",
		},
		"ethereum-mainnet-arb1-a": {
			dnslink: "ethereum-mainnet-arb1-a.blobarchive.net",
			heads:   []string{"arbitrum-one"},
			digest:  "sha256:5834dba26a6c6159c8393dd494f926393f5db9d4e3aa44f0d9c596556a285fa5",
		},
		"ethereum-mainnet-robinhood-a": {
			dnslink: "ethereum-mainnet-robinhood-a.blobarchive.net",
			heads:   []string{"robinhood"},
			digest:  "sha256:ff45e33a71c9b900a02a3380c6c3cbf80750f96500eb7dedc42971d2c165260e",
		},
		"ethereum-mainnet-base-a": {
			dnslink: "ethereum-mainnet-base-a.blobarchive.net",
			heads:   []string{"base"},
			digest:  "sha256:c0d4cdf272542a027ed054f6c12edd445d4ce26d69aa0640043712410ef51958",
		},
		"ethereum-mainnet-arb1-live-a": {
			dnslink:          "ethereum-mainnet-arb1-live-a.blobarchive.net",
			heads:            []string{"arbitrum-one", "unfinalized"},
			digest:           "sha256:4b16e6ed186544e9ddd4136cebe9b93d42a36ca4280fd9fcf205610476be4d43",
			liveFinalized:    "arbitrum-one",
			expectedHandoff:  "all",
			requireExactHash: true,
		},
		"ethereum-mainnet-robinhood-live-a": {
			dnslink:          "ethereum-mainnet-robinhood-live-a.blobarchive.net",
			heads:            []string{"robinhood", "unfinalized"},
			digest:           "sha256:dfc42695a695567b9023b845c31ff8da072104a4ecc3dce8e4db9b02a7249d43",
			liveFinalized:    "robinhood",
			expectedHandoff:  "all",
			requireExactHash: true,
		},
		"ethereum-mainnet-base-live-a": {
			dnslink:          "ethereum-mainnet-base-live-a.blobarchive.net",
			heads:            []string{"base", "unfinalized"},
			digest:           "sha256:28bcc485b823fe9144f55980960da43fbad704de1e64d50fdf001cccedca081f",
			liveFinalized:    "base",
			expectedHandoff:  "all",
			requireExactHash: true,
		},
		"ethereum-mainnet-all-live-a": {
			dnslink:         "ethereum-mainnet-all-live-a.blobarchive.net",
			heads:           []string{"all", "unfinalized"},
			digest:          "sha256:3c2e5bbcf887f674c0d28cb4fcffcffa72156bd8a3437a3baa85dead3239a59f",
			liveFinalized:   "all",
			expectedHandoff: "all",
		},
	}
	if len(profiles) != len(want) {
		t.Fatalf("built-in profile count = %d, want %d", len(profiles), len(want))
	}
	for _, profile := range profiles {
		expected, ok := want[profile.Name]
		if !ok {
			t.Errorf("unexpected built-in follow profile %q", profile.Name)
			continue
		}
		if profile.Version != 1 || !reflect.DeepEqual(profile.Aliases, []string{profile.Name}) {
			t.Errorf("%s version/aliases = %d/%v", profile.Name, profile.Version, profile.Aliases)
		}
		if profile.Digest != expected.digest {
			t.Errorf("%s digest = %q, want %q", profile.Name, profile.Digest, expected.digest)
		}
		if profile.Trust.Mode != "dnslink+signer-pin" || profile.Trust.DNSLink != expected.dnslink || profile.Trust.PubKey != signer || profile.Trust.URL != "" {
			t.Errorf("%s trust = %+v", profile.Name, profile.Trust)
		}
		if profile.Verify != "full" {
			t.Errorf("%s verify = %q, want full", profile.Name, profile.Verify)
		}
		if len(profile.Heads) != len(expected.heads) {
			t.Errorf("%s heads = %v", profile.Name, profile.Heads)
			continue
		}
		for _, name := range expected.heads {
			head, ok := profile.Heads[name]
			if !ok || head.Pin.Mode != "full" || head.Pin.Duration != 0 {
				t.Errorf("%s head %q = %+v, present=%v", profile.Name, name, head, ok)
			}
		}
		if expected.liveFinalized != "" {
			mutable := profile.Heads["unfinalized"]
			view, ok := profile.LiveHeads["live"]
			if mutable.Kind != server.UnfinalizedMutable || mutable.HandoffHead != expected.expectedHandoff || mutable.MaxWindowSlots != 128 {
				t.Errorf("%s mutable head = %+v", profile.Name, mutable)
			}
			if len(profile.LiveHeads) != 1 || !ok || view.FinalizedHead != expected.liveFinalized || view.UnfinalizedHead != "unfinalized" ||
				view.RequireVersionedHashes != expected.requireExactHash {
				t.Errorf("%s live heads = %+v", profile.Name, profile.LiveHeads)
			}
		} else if len(profile.LiveHeads) != 0 {
			t.Errorf("%s unexpectedly configures live heads: %+v", profile.Name, profile.LiveHeads)
		}
		delete(want, profile.Name)
	}
	if len(want) != 0 {
		t.Errorf("missing built-in follow profiles: %v", want)
	}
}

func TestEmbeddedProductionFollowProfilesExpandWithoutLocalBundle(t *testing.T) {
	for _, selector := range []string{
		"ethereum-mainnet-all-a",
		"ethereum-mainnet-arb1-a",
		"ethereum-mainnet-robinhood-a",
		"ethereum-mainnet-base-a",
		"ethereum-mainnet-arb1-live-a",
		"ethereum-mainnet-robinhood-live-a",
		"ethereum-mainnet-base-live-a",
		"ethereum-mainnet-all-live-a",
	} {
		t.Run(selector, func(t *testing.T) {
			path := writeProfileTestFile(t, t.TempDir(), "config.yaml", `
follow: `+selector+`
store: {path: /x}
server: {auth_token_file: /t}
p2p: {}
`)
			cfg, err := LoadConfig(path)
			if err != nil {
				t.Fatalf("LoadConfig: %v", err)
			}
			if cfg.profileSelection == nil || cfg.profileSelection.Name != selector || cfg.profileSelection.Source != "built-in" {
				t.Fatalf("selection = %+v", cfg.profileSelection)
			}
			if cfg.Follow.DNSLink != selector+".blobarchive.net" || cfg.Follow.PubKey == "" || cfg.Follow.URL != "" || cfg.Follow.Verify != "full" {
				t.Errorf("expanded follow = %+v", cfg.Follow)
			}
			if strings.Contains(selector, "-live-") {
				liveWant := map[string]struct {
					finalized string
					handoff   string
					exactHash bool
				}{
					"ethereum-mainnet-all-live-a":       {finalized: "all", handoff: "all"},
					"ethereum-mainnet-arb1-live-a":      {finalized: "arbitrum-one", handoff: "all", exactHash: true},
					"ethereum-mainnet-robinhood-live-a": {finalized: "robinhood", handoff: "all", exactHash: true},
					"ethereum-mainnet-base-live-a":      {finalized: "base", handoff: "all", exactHash: true},
				}[selector]
				mutable := cfg.Follow.Heads["unfinalized"]
				view, ok := cfg.LiveHeads["live"]
				if len(cfg.Follow.Heads) != 2 || mutable.Kind != server.UnfinalizedMutable || mutable.HandoffHead != liveWant.handoff || mutable.MaxWindowSlots != 128 {
					t.Errorf("expanded mutable follow = %+v", mutable)
				}
				if len(cfg.LiveHeads) != 1 || !ok || view.FinalizedHead != liveWant.finalized || view.UnfinalizedHead != "unfinalized" ||
					view.RequireVersionedHashes != liveWant.exactHash {
					t.Errorf("expanded live heads = %+v", cfg.LiveHeads)
				}
				overlays := cfg.followedLiveOverlays()
				if liveWant.exactHash {
					if len(overlays) != 1 || overlays["unfinalized"] != liveWant.finalized {
						t.Errorf("expanded overlay contract = %+v", overlays)
					}
				} else if len(overlays) != 0 {
					t.Errorf("ordinary live view unexpectedly configures overlay: %+v", overlays)
				}
			} else if len(cfg.LiveHeads) != 0 {
				t.Errorf("finalized selector unexpectedly configures live heads: %+v", cfg.LiveHeads)
			}
		})
	}
}

func TestFollowProfileLiveHeadValidationFailsClosed(t *testing.T) {
	bundle := strings.Replace(validProfileBundle, `    heads:
      fixture-head:
        pin: {mode: window, duration: 720h}
`, `    heads:
      fixture-head:
        pin: {mode: full}
      mutable:
        kind: unfinalized-mutable
        handoff_head: fixture-head
        max_window_slots: 128
        pin: {mode: full}
    live_heads:
      live:
        finalized_head: fixture-head
        unfinalized_head: mutable
`, 1)
	if _, err := decodeProfileBundle([]byte(bundle), "valid-live"); err != nil {
		t.Fatalf("valid live follow profile: %v", err)
	}
	tests := []struct {
		name string
		edit func(string) string
		want string
	}{
		{
			name: "missing finalized head",
			edit: func(s string) string {
				return strings.Replace(s, "finalized_head: fixture-head", "finalized_head: missing", 1)
			},
			want: "is not followed",
		},
		{
			name: "non mutable unfinalized head",
			edit: func(s string) string {
				return strings.Replace(s, "kind: unfinalized-mutable", "kind: finalized-monotonic", 1)
			},
			want: "finalized but carries mutable",
		},
		{
			name: "unbounded mutable head",
			edit: func(s string) string {
				return strings.Replace(s, "max_window_slots: 128", "max_window_slots: 0", 1)
			},
			want: "max_window_slots",
		},
		{
			name: "missing mutable head",
			edit: func(s string) string {
				return strings.Replace(s, "unfinalized_head: mutable", "unfinalized_head: missing", 1)
			},
			want: "is not a followed mutable head",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := decodeProfileBundle([]byte(tc.edit(bundle)), tc.name); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want %q", err, tc.want)
			}
		})
	}

	path := writeProfileTestFile(t, t.TempDir(), "collision.yaml", `
follow: ethereum-mainnet-all-live-a
live_heads:
  live: {finalized_head: all, unfinalized_head: unfinalized}
store: {path: /x}
server: {auth_token_file: /t}
p2p: {}
`)
	if _, err := LoadConfig(path); err == nil || !strings.Contains(err.Error(), `profile-derived field "live_heads"`) {
		t.Fatalf("explicit live_heads collision err = %v", err)
	}
}

func TestFollowProfileExpansionEqualsCanonicalConfig(t *testing.T) {
	dir := t.TempDir()
	writeProfileTestFile(t, dir, "profiles.yaml", validProfileBundle)
	profilePath := writeProfileTestFile(t, dir, "profile-config.yaml", `
profile:
  file: profiles.yaml
follow: fixture
store: {path: /var/lib/bloar}
server: {auth_token_file: /etc/bloar/token}
p2p: {}
`)
	canonicalPath := writeProfileTestFile(t, dir, "canonical-config.yaml", `
net: mainnet
beacon:
  genesis_time: 1606824023
  seconds_per_slot: 12
  genesis_validators_root: "0x4b363db94e286120d76eb905340fdd4e54bfe9f06bf33ff6cf5ad27f511bfe95"
  genesis_fork_version: "0x00000000"
  spec_extra: {CONFIG_NAME: fixture}
store: {path: /var/lib/bloar}
server: {auth_token_file: /etc/bloar/token}
p2p: {}
follow:
  url: https://writer.fixture.invalid/heads
  dnslink: writer.fixture.invalid
  heads:
    fixture-head:
      pin: {mode: window, duration: 720h}
`)

	profileCfg, err := LoadConfig(profilePath)
	if err != nil {
		t.Fatalf("LoadConfig(profile): %v", err)
	}
	canonicalCfg, err := LoadConfig(canonicalPath)
	if err != nil {
		t.Fatalf("LoadConfig(canonical): %v", err)
	}
	if profileCfg.profileSelection == nil {
		t.Fatal("scalar follow did not retain profile selection metadata")
	}
	if got := profileCfg.profileSelection; got.Name != "fixture-mainnet" || got.Version != 1 || got.Schema != followProfileSchema || !strings.HasPrefix(got.Digest, "sha256:") || got.Source != "local:"+filepath.Join(dir, "profiles.yaml") {
		t.Errorf("selection = %+v", got)
	}
	profileCfg.profileSelection = nil
	if !reflect.DeepEqual(profileCfg, canonicalCfg) {
		t.Errorf("expanded config differs from canonical\nexpanded:  %#v\ncanonical: %#v", profileCfg, canonicalCfg)
	}
}

func TestFollowProfileSignerPinAndDigestAcknowledgement(t *testing.T) {
	dir := t.TempDir()
	pubkey := strings.Repeat("ab", 32)
	bundle := strings.Replace(validProfileBundle, "mode: dnslink-delegated", "mode: dnslink+signer-pin\n      pubkey: \""+pubkey+"\"", 1)
	profiles, err := decodeProfileBundle([]byte(bundle), "fixture")
	if err != nil {
		t.Fatalf("decodeProfileBundle: %v", err)
	}
	digest, err := profiles[0].contentDigest()
	if err != nil {
		t.Fatal(err)
	}
	writeProfileTestFile(t, dir, "profiles.yaml", bundle)
	path := writeProfileTestFile(t, dir, "config.yaml", `
profile:
  file: profiles.yaml
  acknowledge_digest: `+digest+`
follow: fixture-mainnet@v1
store: {path: /x}
server: {auth_token_file: /t}
p2p: {}
`)
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.Follow.PubKey != pubkey || cfg.profileSelection.acknowledgeDigest != digest {
		t.Errorf("signer pin/ack = %q/%q", cfg.Follow.PubKey, cfg.profileSelection.acknowledgeDigest)
	}

	wrong := strings.Replace(string(mustReadProfileTestFile(t, path)), digest, "sha256:"+strings.Repeat("0", 64), 1)
	wrongPath := writeProfileTestFile(t, dir, "wrong-ack.yaml", wrong)
	if _, err := LoadConfig(wrongPath); err == nil || !strings.Contains(err.Error(), "selected profile digest") {
		t.Fatalf("wrong acknowledgement err = %v", err)
	}
}

func TestFollowProfileExpansionRunsBeforeStrictDecode(t *testing.T) {
	dir := t.TempDir()
	writeProfileTestFile(t, dir, "profiles.yaml", validProfileBundle)
	path := writeProfileTestFile(t, dir, "config.yaml", `
profile: {file: profiles.yaml}
follow: fixture
store: {path: /x}
server: {auth_token_file: /t}
p2p: {}
sever: {listen: ":1"}
`)
	_, err := LoadConfig(path)
	if err == nil || !strings.Contains(err.Error(), "field sever not found") {
		t.Fatalf("strict decode err = %v", err)
	}
}

func TestFollowProfileCollisionAndNarrowOverrides(t *testing.T) {
	dir := t.TempDir()
	writeProfileTestFile(t, dir, "profiles.yaml", validProfileBundle)
	conflict := writeProfileTestFile(t, dir, "conflict.yaml", `
profile: {file: profiles.yaml}
follow: fixture
net: mainnet
store: {path: /x}
server: {auth_token_file: /t}
p2p: {}
`)
	if _, err := LoadConfig(conflict); err == nil || !strings.Contains(err.Error(), `profile-derived field "net"`) {
		t.Fatalf("root collision err = %v", err)
	}

	override := writeProfileTestFile(t, dir, "override.yaml", `
profile:
  file: profiles.yaml
  overrides:
    follow:
      heads:
        fixture-head:
          pin:
            duration: 168h
follow: fixture
store: {path: /x}
server: {auth_token_file: /t}
p2p: {}
`)
	cfg, err := LoadConfig(override)
	if err != nil {
		t.Fatalf("LoadConfig(override): %v", err)
	}
	if got := cfg.Follow.Heads["fixture-head"].Pin.Duration.String(); got != "168h0m0s" {
		t.Errorf("overridden duration = %s", got)
	}

	addField := writeProfileTestFile(t, dir, "add.yaml", `
profile:
  file: profiles.yaml
  overrides:
    follow:
      verify: full
follow: fixture
store: {path: /x}
server: {auth_token_file: /t}
p2p: {}
`)
	if _, err := LoadConfig(addField); err == nil || !strings.Contains(err.Error(), "is not supplied by the selected profile") {
		t.Fatalf("additive override err = %v", err)
	}
}

func TestProfileBundleFailsClosed(t *testing.T) {
	tests := []struct {
		name string
		edit func(string) string
		want string
	}{
		{"bundle schema", func(s string) string {
			return strings.Replace(s, profileBundleSchema, "bloar.follow-profile-bundle/v2", 1)
		}, "want"},
		{"profile schema", func(s string) string { return strings.Replace(s, followProfileSchema, "bloar.follow-profile/v2", 1) }, "has schema"},
		{"version", func(s string) string { return strings.Replace(s, "version: 1", "version: 0", 1) }, "version must be positive"},
		{"provenance", func(s string) string { return strings.Replace(s, "source: hermetic test fixture", "source: ''", 1) }, "provenance.source is required"},
		{"network", func(s string) string { return strings.Replace(s, "name: mainnet", "name: Mainnet", 1) }, "network.name"},
		{"beacon root", func(s string) string {
			return strings.Replace(s, "0x4b363db94e286120d76eb905340fdd4e54bfe9f06bf33ff6cf5ad27f511bfe95", "0x12", 1)
		}, "genesis_validators_root"},
		{"trust mode", func(s string) string { return strings.Replace(s, "dnslink-delegated", "direct", 1) }, "must be dnslink-delegated"},
		{"delegated signer pin", func(s string) string {
			return strings.Replace(s, "dnslink: writer.fixture.invalid", "dnslink: writer.fixture.invalid\n      pubkey: \""+strings.Repeat("ab", 32)+"\"", 1)
		}, "must not set pubkey"},
		{"bad url", func(s string) string {
			return strings.Replace(s, "https://writer.fixture.invalid/heads", "http://writer.fixture.invalid/heads", 1)
		}, "absolute HTTPS"},
		{"url query secret", func(s string) string {
			return strings.Replace(s, "https://writer.fixture.invalid/heads", "https://writer.fixture.invalid/heads?token=secret", 1)
		}, "without userinfo, query, or fragment"},
		{"no heads", func(s string) string { i := strings.Index(s, "    heads:\n"); return s[:i] + "    heads: {}\n" }, "at least one head"},
		{"bad retention", func(s string) string { return strings.Replace(s, "duration: 720h", "duration: 0s", 1) }, "positive duration"},
		{"unknown field", func(s string) string { return strings.Replace(s, "version: 1", "version: 1\n    surprise: true", 1) }, "field surprise not found"},
		{"declared digest", func(s string) string {
			return strings.Replace(s, "version: 1", "version: 1\n    digest: sha256:"+strings.Repeat("0", 64), 1)
		}, "computed"},
		{"yaml merge", func(s string) string { return strings.Replace(s, "    version: 1", "    <<: {version: 1}", 1) }, "merge keys"},
		{"yaml anchor", func(s string) string { return strings.Replace(s, "    provenance:", "    provenance: &provenance", 1) }, "anchors are not allowed"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			writeProfileTestFile(t, dir, "profiles.yaml", tc.edit(validProfileBundle))
			path := writeProfileTestFile(t, dir, "config.yaml", profileTestConfig("fixture"))
			_, err := LoadConfig(path)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestProfileAliasCollisionAndConfigSyntaxFailClosed(t *testing.T) {
	dir := t.TempDir()
	profiles, err := decodeProfileBundle([]byte(validProfileBundle), "fixture")
	if err != nil {
		t.Fatal(err)
	}
	second := profiles[0]
	second.Name = "second"
	second.Aliases = []string{"fixture"}
	catalog := make(map[string]catalogProfile)
	if err := addProfiles(catalog, profiles, "first"); err != nil {
		t.Fatal(err)
	}
	if err := addProfiles(catalog, []followProfile{second}, "second"); err == nil || !strings.Contains(err.Error(), "collides") {
		t.Fatalf("alias collision err = %v", err)
	}
	second.Aliases = []string{"second"}
	second.Version = 2
	if err := addProfiles(catalog, []followProfile{second}, "second-v2"); err != nil {
		t.Fatalf("a second immutable version should coexist under its versioned selector: %v", err)
	}
	if _, ok := catalog["second@v2"]; !ok {
		t.Fatal("versioned canonical selector was not registered")
	}

	writeProfileTestFile(t, dir, "profiles.yaml", validProfileBundle)
	dupRoot := writeProfileTestFile(t, dir, "duplicate.yaml", `
profile: {file: profiles.yaml}
follow: fixture
follow: fixture-mainnet
store: {path: /x}
server: {auth_token_file: /t}
p2p: {}
`)
	if _, err := LoadConfig(dupRoot); err == nil || !strings.Contains(err.Error(), `duplicate mapping key "follow"`) {
		t.Fatalf("duplicate config key err = %v", err)
	}

	unknownControl := writeProfileTestFile(t, dir, "unknown-control.yaml", `
profile: {file: profiles.yaml, registry: https://example.invalid}
follow: fixture
store: {path: /x}
server: {auth_token_file: /t}
p2p: {}
`)
	if _, err := LoadConfig(unknownControl); err == nil || !strings.Contains(err.Error(), "field registry not found") {
		t.Fatalf("profile control err = %v", err)
	}
}

func TestBuiltInProfilesAreSelectableAndCannotBeOverridden(t *testing.T) {
	dir := t.TempDir()
	catalog, err := loadProfileCatalogWithBuiltins(filepath.Join(dir, "config.yaml"), "", []byte(validProfileBundle))
	if err != nil {
		t.Fatalf("loading built-in fixture: %v", err)
	}
	if got, ok := catalog["fixture-mainnet@v1"]; !ok || got.source != "built-in" || got.profile.Name != "fixture-mainnet" {
		t.Fatalf("versioned built-in selection = %+v, %v", got, ok)
	}
	writeProfileTestFile(t, dir, "profiles.yaml", validProfileBundle)
	if _, err := loadProfileCatalogWithBuiltins(filepath.Join(dir, "config.yaml"), "profiles.yaml", []byte(validProfileBundle)); err == nil || !strings.Contains(err.Error(), "collides") {
		t.Fatalf("local override err = %v", err)
	}
}

func TestPersistedProfileDigestChangeNeedsAcknowledgement(t *testing.T) {
	db, err := pebble.Open(filepath.Join(t.TempDir(), "kv"), &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	first := &ProfileSelection{Name: "fixture", Schema: followProfileSchema, Version: 1, Digest: "sha256:" + strings.Repeat("1", 64)}
	if err := ensureProfileSelection(db, first); err != nil {
		t.Fatalf("first selection: %v", err)
	}
	changed := &ProfileSelection{Name: first.Name, Schema: first.Schema, Version: first.Version, Digest: "sha256:" + strings.Repeat("2", 64)}
	if err := ensureProfileSelection(db, changed); err == nil || !strings.Contains(err.Error(), "without explicit acknowledgement") {
		t.Fatalf("changed digest err = %v", err)
	}
	changed.acknowledgeDigest = changed.Digest
	if err := ensureProfileSelection(db, changed); err != nil {
		t.Fatalf("acknowledged selection: %v", err)
	}
	versioned := &ProfileSelection{Name: first.Name, Schema: first.Schema, Version: 2, Digest: "sha256:" + strings.Repeat("3", 64)}
	if err := ensureProfileSelection(db, versioned); err != nil {
		t.Fatalf("new version should be explicit without acknowledgement: %v", err)
	}
	raw, closer, err := db.Get(profileSelectionKey)
	if err != nil {
		t.Fatal(err)
	}
	defer closer.Close()
	var stored storedProfileSelection
	if err := json.Unmarshal(raw, &stored); err != nil {
		t.Fatal(err)
	}
	if stored.Version != 2 || stored.Digest != versioned.Digest {
		t.Errorf("stored = %+v", stored)
	}
}

func TestPersistedProfileMetadataCorruptionFailsClosed(t *testing.T) {
	db, err := pebble.Open(filepath.Join(t.TempDir(), "kv"), &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Set(profileSelectionKey, []byte(`{"name":"fixture","schema":"bloar.follow-profile/v1","version":0,"digest":"broken"}`), pebble.Sync); err != nil {
		t.Fatal(err)
	}
	selected := &ProfileSelection{Name: "fixture", Schema: followProfileSchema, Version: 1, Digest: "sha256:" + strings.Repeat("1", 64)}
	if err := ensureProfileSelection(db, selected); err == nil || !strings.Contains(err.Error(), "persisted profile selection is invalid") {
		t.Fatalf("corrupt state err = %v", err)
	}
}

func TestConfigInspectNeverReadsOrPrintsSecrets(t *testing.T) {
	dir := t.TempDir()
	writeProfileTestFile(t, dir, "profiles.yaml", validProfileBundle)
	tokenSecret := "TOP-SECRET-TOKEN-CONTENT"
	keySecret := "TOP-SECRET-PRIVATE-KEY-CONTENT"
	tokenPath := writeProfileTestFile(t, dir, "token", tokenSecret)
	keyPath := writeProfileTestFile(t, dir, "signing.key", keySecret)
	path := writeProfileTestFile(t, dir, "config.yaml", `
profile: {file: profiles.yaml}
follow: fixture
store: {path: /x}
server: {auth_token_file: `+tokenPath+`}
publish: {signing_key_file: `+keyPath+`}
p2p: {}
`)
	var out bytes.Buffer
	if err := inspectConfig(path, &out); err != nil {
		t.Fatalf("inspectConfig: %v", err)
	}
	text := out.String()
	for _, secret := range []string{tokenSecret, keySecret} {
		if strings.Contains(text, secret) {
			t.Fatalf("inspect output leaked %q:\n%s", secret, text)
		}
	}
	for _, want := range []string{"fixture-mainnet", "sha256:", "hermetic test fixture", "config:", tokenPath, keyPath} {
		if !strings.Contains(text, want) {
			t.Errorf("inspect output missing %q:\n%s", want, text)
		}
	}
}

func TestProfileDigestIsDeterministic(t *testing.T) {
	profiles, err := decodeProfileBundle([]byte(validProfileBundle), "fixture")
	if err != nil {
		t.Fatal(err)
	}
	first := profiles[0]
	second := first
	second.Heads = make(map[string]profileHead)
	second.Heads["z-head"] = profileHead{Pin: PinConfig{Mode: "none"}}
	second.Heads["fixture-head"] = first.Heads["fixture-head"]
	first.Heads["z-head"] = profileHead{Pin: PinConfig{Mode: "none"}}
	digestA, err := first.contentDigest()
	if err != nil {
		t.Fatal(err)
	}
	digestB, err := second.contentDigest()
	if err != nil {
		t.Fatal(err)
	}
	if digestA != digestB {
		t.Fatalf("map insertion order changed digest: %s != %s", digestA, digestB)
	}
}

func profileTestConfig(selector string) string {
	return `
profile: {file: profiles.yaml}
follow: ` + selector + `
store: {path: /x}
server: {auth_token_file: /t}
p2p: {}
`
}

func writeProfileTestFile(t *testing.T, dir, name, content string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("writing %s: %v", path, err)
	}
	return path
}

func mustReadProfileTestFile(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}
