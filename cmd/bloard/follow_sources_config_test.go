package main

import (
	"encoding/hex"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/blobarchive/bloar/server"
)

const (
	testFollowArchiveID = "0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20"
	testFollowPubkeyB   = "1112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f30"
)

func testSourceHeads() map[string]FollowHeadConfig {
	return map[string]FollowHeadConfig{
		"all": {Pin: PinConfig{Mode: "none"}},
		"tip": {
			Kind: server.UnfinalizedMutable, HandoffHead: "all", MaxWindowSlots: 64,
			Pin: PinConfig{Mode: "full"},
		},
	}
}

func testSources() map[string]FollowSourceConfig {
	return map[string]FollowSourceConfig{
		"writer-a": {URL: "https://a.example.org/", PubKey: followPubkey, Heads: []string{"tip", "all"}},
		"writer-b": {URL: "https://b.example.org", PubKey: testFollowPubkeyB, Heads: []string{"all"}},
	}
}

func renderSourceSetConfig(t *testing.T, sources map[string]FollowSourceConfig, heads map[string]FollowHeadConfig, revision uint64, ack, migrate string) string {
	t.Helper()
	followConfig := &FollowConfig{ArchiveID: testFollowArchiveID, Sources: sources, Heads: heads}
	if ack == "" {
		var err error
		ack, err = followConfig.SourceSetDigest("mainnet")
		if err != nil {
			t.Fatalf("source-set digest: %v", err)
		}
	}

	var out strings.Builder
	out.WriteString("net: mainnet\n")
	out.WriteString("beacon: {genesis_time: 1606824023}\n")
	out.WriteString("store: {path: /x}\n")
	out.WriteString("server: {auth_token_file: /t}\n")
	out.WriteString("p2p: {}\n")
	out.WriteString("follow:\n")
	fmt.Fprintf(&out, "  archive_id: %q\n", testFollowArchiveID)
	out.WriteString("  sources:\n")
	ids := make([]string, 0, len(sources))
	for id := range sources {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	for _, id := range ids {
		source := sources[id]
		fmt.Fprintf(&out, "    %s:\n", id)
		if source.URL != "" {
			fmt.Fprintf(&out, "      url: %q\n", source.URL)
		}
		if source.IPNS != "" {
			fmt.Fprintf(&out, "      ipns: %q\n", source.IPNS)
		}
		if source.DNSLink != "" {
			fmt.Fprintf(&out, "      dnslink: %q\n", source.DNSLink)
		}
		if source.PubKey != "" {
			fmt.Fprintf(&out, "      pubkey: %q\n", source.PubKey)
		}
		out.WriteString("      heads: [")
		for i, head := range source.Heads {
			if i > 0 {
				out.WriteString(", ")
			}
			fmt.Fprintf(&out, "%q", head)
		}
		out.WriteString("]\n")
	}
	out.WriteString("  source_set:\n")
	fmt.Fprintf(&out, "    revision: %d\n", revision)
	fmt.Fprintf(&out, "    acknowledge_digest: %q\n", ack)
	if migrate != "" {
		fmt.Fprintf(&out, "  migrate_legacy_source: %q\n", migrate)
	}
	out.WriteString("  heads:\n")
	headNames := make([]string, 0, len(heads))
	for name := range heads {
		headNames = append(headNames, name)
	}
	slices.Sort(headNames)
	for _, name := range headNames {
		head := heads[name]
		fmt.Fprintf(&out, "    %s:\n", name)
		if head.Kind != "" {
			fmt.Fprintf(&out, "      kind: %s\n", head.Kind)
		}
		if head.HandoffHead != "" {
			fmt.Fprintf(&out, "      handoff_head: %s\n", head.HandoffHead)
		}
		if head.MaxWindowSlots != 0 {
			fmt.Fprintf(&out, "      max_window_slots: %d\n", head.MaxWindowSlots)
		}
		fmt.Fprintf(&out, "      pin: {mode: %s", head.Pin.Mode)
		if head.Pin.Duration != 0 {
			fmt.Fprintf(&out, ", duration: %s", head.Pin.Duration)
		}
		out.WriteString("}\n")
	}
	return out.String()
}

func TestFollowSourceSetLoads(t *testing.T) {
	cfg := loadString(t, renderSourceSetConfig(t, testSources(), testSourceHeads(), 7, "", "writer-a"))
	if len(cfg.Follow.Sources) != 2 || cfg.Follow.SourceSet.Revision != 7 {
		t.Fatalf("source set = sources:%d revision:%d", len(cfg.Follow.Sources), cfg.Follow.SourceSet.Revision)
	}
	if cfg.Follow.MigrateLegacySource != "writer-a" {
		t.Fatalf("migrate_legacy_source = %q", cfg.Follow.MigrateLegacySource)
	}
	id, err := cfg.Follow.ExpectedArchiveID()
	if err != nil || id == nil || id.String() != testFollowArchiveID {
		t.Fatalf("expected archive ID = %v, err = %v", id, err)
	}

	dnsSources := testSources()
	dns := dnsSources["writer-a"]
	dns.URL, dns.DNSLink = "", "Swarm.Example."
	dnsSources["writer-a"] = dns
	loadString(t, renderSourceSetConfig(t, dnsSources, testSourceHeads(), 8, "", ""))
}

func TestFollowSourceSetRuntimeAdapterPreservesAcknowledgedNormalization(t *testing.T) {
	const directIPNS = "k51qzi5uqu5dmc9hz7x2fd156p883lc3w1i36tu4i4r0yd7ohnd4a12j9zeun8"
	sources := testSources()
	writerA := sources["writer-a"]
	writerA.URL = "https://A.EXAMPLE.ORG:00443/"
	writerA.IPNS = directIPNS
	sources["writer-a"] = writerA
	cfg := loadString(t, renderSourceSetConfig(t, sources, testSourceHeads(), 7, "", "writer-a"))

	runtime, err := cfg.Follow.runtimeSourceSet(cfg.Net)
	if err != nil {
		t.Fatal(err)
	}
	if runtime == nil || runtime.Revision != 7 || runtime.MigrateLegacySource != "writer-a" || runtime.MigrateLegacyIPNS != directIPNS {
		t.Fatalf("runtime source-set metadata = %+v", runtime)
	}
	wantDigest, err := cfg.Follow.SourceSetDigest(cfg.Net)
	if err != nil {
		t.Fatal(err)
	}
	if "sha256:"+hex.EncodeToString(runtime.Digest[:]) != wantDigest {
		t.Fatalf("runtime digest = %x, want %s", runtime.Digest, wantDigest)
	}
	if len(runtime.Sources) != 2 || runtime.Sources[0].ID != "writer-a" || runtime.Sources[1].ID != "writer-b" {
		t.Fatalf("runtime source order = %+v", runtime.Sources)
	}
	if got := runtime.Sources[0]; got.URL != "https://a.example.org" || got.IPNS != directIPNS ||
		hex.EncodeToString(got.PubKey) != followPubkey || strings.Join(got.AllowedHeads, ",") != "all,tip" {
		t.Fatalf("runtime writer-a = %+v, key=%x", got, got.PubKey)
	}

	// The adapter gives the runtime ownership of detached key/head slices.
	runtime.Sources[0].PubKey[0] ^= 0xff
	runtime.Sources[0].AllowedHeads[0] = "changed"
	if cfg.Follow.Sources["writer-a"].PubKey != followPubkey || strings.Join(cfg.Follow.Sources["writer-a"].Heads, ",") != "tip,all" {
		t.Fatal("runtime source authority aliases decoded YAML")
	}
}

func TestFollowSingularRemainsCompatibleAndMayPinArchiveID(t *testing.T) {
	cfg := loadString(t, `
net: mainnet
beacon: {genesis_time: 1606824023}
store: {path: /x}
server: {auth_token_file: /t}
p2p: {}
follow:
  url: https://archive.example.org
  dnslink: swarm.example
  archive_id: "`+testFollowArchiveID+`"
  heads: {all: {pin: {mode: none}}}
`)
	if key, err := cfg.Follow.Key(); err != nil || key != nil {
		t.Fatalf("legacy delegated DNSLink key = %x, err = %v", key, err)
	}
	if id, err := cfg.Follow.ExpectedArchiveID(); err != nil || id == nil || id.String() != testFollowArchiveID {
		t.Fatalf("legacy archive ID = %v, err = %v", id, err)
	}
}

func TestFollowSourceSetDigestIsCanonicalAndScoped(t *testing.T) {
	a := &FollowConfig{
		ArchiveID:           testFollowArchiveID,
		Sources:             testSources(),
		Heads:               testSourceHeads(),
		SourceSet:           &FollowSourceSetConfig{Revision: 1, AcknowledgeDigest: "ignored"},
		PollInterval:        time.Second,
		FetchTimeout:        2 * time.Second,
		Verify:              "cid",
		MigrateLegacySource: "writer-a",
	}
	b := &FollowConfig{
		ArchiveID: testFollowArchiveID,
		Sources: map[string]FollowSourceConfig{
			"writer-b": {URL: "https://B.EXAMPLE.ORG/", PubKey: strings.ToUpper(testFollowPubkeyB), Heads: []string{"all"}},
			"writer-a": {URL: "https://A.EXAMPLE.ORG", PubKey: strings.ToUpper(followPubkey), Heads: []string{"all", "tip"}},
		},
		Heads: map[string]FollowHeadConfig{
			"all": {Pin: PinConfig{Mode: "full"}},
			"tip": {Kind: server.UnfinalizedMutable, HandoffHead: "all", MaxWindowSlots: 64, Pin: PinConfig{Mode: "full"}},
		},
		SourceSet:           &FollowSourceSetConfig{Revision: 99},
		PollInterval:        10 * time.Minute,
		FetchTimeout:        time.Minute,
		Verify:              "full",
		MigrateLegacySource: "writer-b",
	}

	gotA, err := a.SourceSetDigest("mainnet")
	if err != nil {
		t.Fatal(err)
	}
	gotB, err := b.SourceSetDigest("mainnet")
	if err != nil {
		t.Fatal(err)
	}
	if gotA != gotB {
		t.Fatalf("equivalent normalized rosters differ: %s != %s", gotA, gotB)
	}
	const known = "sha256:2ca0b5453d556c1c16a9bdbc6764f1f2ee399ccde68b7ce38f386e276d870443"
	if gotA != known {
		t.Fatalf("source-set digest = %s, update known vector %s", gotA, known)
	}
	if other, err := a.SourceSetDigest("holesky"); err != nil || other == gotA {
		t.Fatalf("network did not domain-separate digest: other=%s err=%v", other, err)
	}
	otherArchive := *a
	otherArchive.ArchiveID = strings.Repeat("22", 32)
	if other, err := otherArchive.SourceSetDigest("mainnet"); err != nil || other == gotA {
		t.Fatalf("archive ID did not domain-separate digest: other=%s err=%v", other, err)
	}
	changed := *a
	changed.Sources = testSources()
	s := changed.Sources["writer-b"]
	s.Heads = []string{"tip", "all"}
	changed.Sources["writer-b"] = s
	if other, err := changed.SourceSetDigest("mainnet"); err != nil || other == gotA {
		t.Fatalf("head assignment did not change digest: other=%s err=%v", other, err)
	}
}

func TestFollowSourceSetAcknowledgementReportsExpectedDigest(t *testing.T) {
	sources, heads := testSources(), testSourceHeads()
	expected, err := (&FollowConfig{ArchiveID: testFollowArchiveID, Sources: sources, Heads: heads}).SourceSetDigest("mainnet")
	if err != nil {
		t.Fatal(err)
	}
	body := renderSourceSetConfig(t, sources, heads, 1, "sha256:"+strings.Repeat("0", 64), "")
	_, err = LoadConfig(writeFile(t, "config.yaml", body))
	if err == nil || !strings.Contains(err.Error(), expected) {
		t.Fatalf("ack mismatch err = %v, want expected digest %s", err, expected)
	}
}

func TestFollowSourceSetBounds(t *testing.T) {
	makeSources := func(count int) map[string]FollowSourceConfig {
		sources := make(map[string]FollowSourceConfig, count)
		for i := 0; i < count; i++ {
			sources[fmt.Sprintf("writer-%02d", i)] = FollowSourceConfig{
				URL:    fmt.Sprintf("https://writer-%02d.example.org", i),
				PubKey: fmt.Sprintf("%064x", i+1), Heads: []string{"all"},
			}
		}
		return sources
	}
	heads := map[string]FollowHeadConfig{"all": {Pin: PinConfig{Mode: "none"}}}
	for _, count := range []int{1, 32} {
		t.Run(fmt.Sprintf("accept-%d", count), func(t *testing.T) {
			loadString(t, renderSourceSetConfig(t, makeSources(count), heads, 1, "", ""))
		})
	}
	assertConfigError(t, renderSourceSetConfig(t, makeSources(33), heads, 1, "", ""), "must be in [1,32]")
}

func TestFollowSourceIDBounds(t *testing.T) {
	if err := validateFollowSourceID(strings.Repeat("a", 63)); err != nil {
		t.Fatalf("63-byte source ID: %v", err)
	}
	for _, id := range []string{"", "-writer", "writer-", "Writer", "writer_name", strings.Repeat("a", 64)} {
		if err := validateFollowSourceID(id); err == nil {
			t.Errorf("source ID %q accepted", id)
		}
	}
}

func TestFollowSourceSetModeAndSourceErrors(t *testing.T) {
	const prefix = `
net: mainnet
beacon: {genesis_time: 1606824023}
store: {path: /x}
server: {auth_token_file: /t}
p2p: {}
`
	for _, tc := range []struct {
		name string
		body string
		want string
	}{
		{"sources only", prefix + `follow: {sources: {}, heads: {all: {pin: {mode: none}}}}`, "requires follow.source_set"},
		{"source set only", prefix + `follow: {source_set: {revision: 1}, heads: {all: {pin: {mode: none}}}}`, "requires follow.sources"},
		{"null sources", prefix + `follow: {sources: null, source_set: {revision: 1}, heads: {all: {pin: {mode: none}}}}`, "sources must be a mapping"},
		{"null source set", prefix + `follow: {sources: {}, source_set: null, heads: {all: {pin: {mode: none}}}}`, "source_set must be a mapping"},
		{"legacy migration control", prefix + `follow: {url: https://a.example, pubkey: "` + followPubkey + `", migrate_legacy_source: old, heads: {all: {pin: {mode: none}}}}`, "valid only with follow.sources"},
	} {
		t.Run(tc.name, func(t *testing.T) { assertConfigError(t, tc.body, tc.want) })
	}

	valid := renderSourceSetConfig(t, testSources(), testSourceHeads(), 1, "", "")
	cases := []struct {
		name string
		body string
		want string
	}{
		{"mixed singular", strings.Replace(valid, "  archive_id:", "  url: https://legacy.example.org\n  archive_id:", 1), "forbids the singular"},
		{"missing archive ID", strings.Replace(valid, "  archive_id: \""+testFollowArchiveID+"\"\n", "", 1), "archive_id is required"},
		{"malformed archive ID", strings.Replace(valid, testFollowArchiveID, "not-an-archive-id", 1), "follow.archive_id"},
		{"zero revision", strings.Replace(valid, "    revision: 1", "    revision: 0", 1), "revision must be positive"},
		{"unknown migration", strings.Replace(valid, "  heads:\n", "  migrate_legacy_source: missing\n  heads:\n", 1), "does not name a configured source"},
		{"invalid source ID", strings.Replace(valid, "    writer-a:", "    Writer_A:", 1), "follow source ID"},
		{"duplicate URL", strings.Replace(valid, "https://b.example.org", "https://A.EXAMPLE.ORG/", 1), "duplicates source"},
		{"duplicate URL with zero-padded default port", strings.Replace(valid, "https://b.example.org", "https://A.EXAMPLE.ORG:00443/", 1), "duplicates source"},
		{"duplicate signer", strings.Replace(valid, testFollowPubkeyB, strings.ToUpper(followPubkey), 1), "duplicates the signer"},
		{"all-zero signer", strings.Replace(valid, "      pubkey: \""+followPubkey+"\"", "      pubkey: \""+strings.Repeat("0", len(followPubkey))+"\"", 1), "must not be the all-zero"},
		{"missing mutable coverage", strings.Replace(valid, "heads: [\"tip\", \"all\"]", "heads: [\"all\"]", 1), "not allowed by any"},
		{"two mutable authorities", strings.Replace(valid, "heads: [\"all\"]", "heads: [\"all\", \"tip\"]", 1), "exactly one source"},
		{"duplicate allowed head", strings.Replace(valid, "heads: [\"all\"]", "heads: [\"all\", \"all\"]", 1), "contains duplicate"},
		{"unknown allowed head", strings.Replace(valid, "heads: [\"all\"]", "heads: [\"missing\"]", 1), "not in follow.heads"},
		{"source without channel", strings.Replace(valid, "      url: \"https://a.example.org/\"\n", "", 1), "needs at least one channel"},
		{"DNSLink without pinned key", strings.Replace(strings.Replace(valid, "      url: \"https://a.example.org/\"", "      dnslink: swarm.example", 1), "      pubkey: \""+followPubkey+"\"\n", "", 1), "must be a pinned"},
		{"IPNS and DNSLink", strings.Replace(valid, "      url: \"https://a.example.org/\"", "      ipns: k51qzi5uqu5dmc9hz7x2fd156p883lc3w1i36tu4i4r0yd7ohnd4a12j9zeun8\n      dnslink: swarm.example", 1), "mutually exclusive"},
		{"invalid IPNS", strings.Replace(valid, "      url: \"https://a.example.org/\"", "      ipns: not-an-ipns-name", 1), "not an IPNS name"},
		{"duplicate name", strings.Replace(strings.Replace(valid, "      url: \"https://a.example.org/\"", "      dnslink: Swarm.Example.", 1), "      url: \"https://b.example.org\"", "      dnslink: swarm.example", 1), "duplicates the name"},
		{"invalid URL", strings.Replace(valid, "https://a.example.org/", "https://user@example.org/?query=1", 1), "absolute HTTP(S) URL"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) { assertConfigError(t, tc.body, tc.want) })
	}
}

func TestNormalizeFollowSourceURLCanonicalizesNumericPort(t *testing.T) {
	for _, tc := range []struct {
		raw, want string
	}{
		{"https://EXAMPLE.ORG:00443/", "https://example.org"},
		{"https://EXAMPLE.ORG:08080/", "https://example.org:8080"},
	} {
		got, err := normalizeFollowSourceURL(tc.raw)
		if err != nil || got != tc.want {
			t.Errorf("normalizeFollowSourceURL(%q) = %q, %v; want %q", tc.raw, got, err, tc.want)
		}
	}
}
