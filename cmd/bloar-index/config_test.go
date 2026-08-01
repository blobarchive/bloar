package main

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

// writeConfig writes a config file and returns its path.
func writeConfig(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "index.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("writing the config: %v", err)
	}
	return path
}

// tokenFile writes a token file and returns its path.
func tokenFile(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("writing the token file: %v", err)
	}
	return path
}

// beaconConfig is a minimal valid config for the beacon subcommand.
func beaconConfig(t *testing.T) string {
	return `
archive:
  url: http://archive.example.org:8550
  token_file: ` + tokenFile(t, "s3cret\n") + `
  head: all
upstream:
  url: http://beacon.example.org
`
}

func unfinalizedConfig(t *testing.T) string {
	return `
archive:
  url: http://archive.example.org:8550
  token_file: ` + tokenFile(t, "s3cret\n") + `
  head: unfinalized
upstream:
  url: http://beacon.example.org
`
}

// chainConfig is a minimal valid config for the chain subcommand: the shipped
// default filter, one inbox-events source over the SequencerInbox with its topic
// left to default.
func chainConfig(t *testing.T) string {
	return `
beacon:
  genesis_time: 1606824023
archive:
  url: http://archive.example.org:8550
  token_file: ` + tokenFile(t, "s3cret\n") + `
  head: arbitrum-one
chain:
  parent_chain_rpc: http://l1.example.org:8545
  sources:
    - type: inbox-events
      address: "0x1c479675ad559DC151F6Ec7ed3FbF8ceE79582B6"
      from_block: 0
`
}

func TestLoadConfigDefaults(t *testing.T) {
	cfg, err := LoadConfig(writeConfig(t, beaconConfig(t)), "beacon")
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}

	// Every default the spec names, applied.
	if cfg.Beacon.SecondsPerSlot != 12 {
		t.Errorf("seconds_per_slot = %d, want the network default 12", cfg.Beacon.SecondsPerSlot)
	}
	if cfg.Index.BatchSize != defaultBatchSize {
		t.Errorf("batch_size = %d, want %d", cfg.Index.BatchSize, defaultBatchSize)
	}
	if cfg.Index.FetchConcurrency != defaultFetchConcurrency {
		t.Errorf("fetch_concurrency = %d, want %d", cfg.Index.FetchConcurrency, defaultFetchConcurrency)
	}
	if cfg.Index.BlockFetchConcurrency != defaultBlockFetchConcurrency {
		t.Errorf("block_fetch_concurrency = %d, want %d", cfg.Index.BlockFetchConcurrency, defaultBlockFetchConcurrency)
	}
	if cfg.Index.RPCBatchSize != defaultRPCBatchSize {
		t.Errorf("rpc_batch_size = %d, want %d", cfg.Index.RPCBatchSize, defaultRPCBatchSize)
	}
	if cfg.Index.MaxPutBlobs != defaultMaxPutBlobs {
		t.Errorf("max_put_blobs = %d, want %d", cfg.Index.MaxPutBlobs, defaultMaxPutBlobs)
	}
	if cfg.Archive.MaxPutBlobs != defaultArchiveMaxPutBlobs {
		t.Errorf("archive.max_put_blobs = %d, want %d", cfg.Archive.MaxPutBlobs, defaultArchiveMaxPutBlobs)
	}
	if cfg.Index.PollInterval != defaultPollInterval {
		t.Errorf("poll_interval = %v, want %v", cfg.Index.PollInterval, defaultPollInterval)
	}
	if cfg.Chain.AllHead != defaultAllHead {
		t.Errorf("all_head = %q, want %q", cfg.Chain.AllHead, defaultAllHead)
	}

	// An unset upstream.head is anchored mode, whose block feed defaults to the
	// blob source: one beacon node that is both.
	if cfg.Upstream.Head != "" {
		t.Errorf("upstream.head = %q, want empty (anchored mode)", cfg.Upstream.Head)
	}
	if cfg.Upstream.BlockURL != cfg.Upstream.URL {
		t.Errorf("upstream.block_url = %q, want it defaulted to upstream.url %q", cfg.Upstream.BlockURL, cfg.Upstream.URL)
	}

	// metrics_listen is off by default: an unset key builds no registry and
	// serves nothing, the same as bloard's.
	if cfg.MetricsListen != "" {
		t.Errorf("metrics_listen = %q, want empty (metrics off by default)", cfg.MetricsListen)
	}
}

// TestLoadConfigChainMinimal is the other subcommand's floor: what an operator
// must actually write to run a chain indexer, and what they get for free.
func TestLoadConfigChainMinimal(t *testing.T) {
	cfg, err := LoadConfig(writeConfig(t, chainConfig(t)), "chain")
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}

	if cfg.Index.BlockRange != defaultBlockRange {
		t.Errorf("block_range = %d, want %d", cfg.Index.BlockRange, defaultBlockRange)
	}
	// fetch_blobs defaults to false, which is the mode that trails the ALL
	// head, which is why all_head has to default to something.
	if cfg.Chain.FetchBlobs {
		t.Error("fetch_blobs defaulted to true; it must default to the trailing mode")
	}
	if cfg.Chain.AllHead != defaultAllHead {
		t.Errorf("all_head = %q, want %q", cfg.Chain.AllHead, defaultAllHead)
	}
	// The trailing mode needs no upstream at all: the ALL head's indexer is
	// putting the blobs.
	if cfg.Upstream.URL != "" {
		t.Errorf("upstream.url = %q, want empty", cfg.Upstream.URL)
	}

	// The one thing an operator gets for free in the default filter: the
	// inbox-events topic, defaulted to the pinned SequencerBatchDelivered hash
	// so the common case need not restate it.
	sources, err := cfg.ChainSources()
	if err != nil {
		t.Fatalf("ChainSources: %v", err)
	}
	if len(sources) != 1 {
		t.Fatalf("sources = %d, want the single default inbox-events source", len(sources))
	}
	if string(sources[0].Type) != "inbox-events" {
		t.Errorf("source type = %q, want inbox-events", sources[0].Type)
	}
	if sources[0].Topic.Hex() != "0x7394f4a19a13c7b92b5bb71033245305946ef78452f7b4986ac1390b5df4ebd7" {
		t.Errorf("topic = %s, want the defaulted SequencerBatchDelivered hash", sources[0].Topic.Hex())
	}
	if !sources[0].OpenEnded {
		t.Error("a source with no until_block must be open-ended")
	}
}

func TestLoadConfigUnfinalized(t *testing.T) {
	cfg, err := LoadConfig(writeConfig(t, unfinalizedConfig(t)), "unfinalized")
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.Unfinalized.HandoffHead != "all" || cfg.Unfinalized.WindowSlots != 96 ||
		cfg.Unfinalized.OverlapSlots == nil || *cfg.Unfinalized.OverlapSlots != 8 {
		t.Fatalf("unfinalized defaults = %+v", cfg.Unfinalized)
	}
	if cfg.Upstream.BlockURL != cfg.Upstream.URL {
		t.Fatalf("block_url = %q, want %q", cfg.Upstream.BlockURL, cfg.Upstream.URL)
	}

	t.Run("explicit zero overlap", func(t *testing.T) {
		body := unfinalizedConfig(t) + "\nunfinalized:\n  window_slots: 64\n  overlap_slots: 0\n"
		got, err := LoadConfig(writeConfig(t, body), "unfinalized")
		if err != nil {
			t.Fatal(err)
		}
		if got.Unfinalized.OverlapSlots == nil || *got.Unfinalized.OverlapSlots != 0 {
			t.Fatalf("overlap = %v, want explicit zero", got.Unfinalized.OverlapSlots)
		}
	})

	t.Run("self handoff", func(t *testing.T) {
		body := unfinalizedConfig(t) + "\nunfinalized:\n  handoff_head: unfinalized\n"
		if _, err := LoadConfig(writeConfig(t, body), "unfinalized"); err == nil {
			t.Fatal("self handoff accepted")
		}
	})

	t.Run("archive mirror forbidden", func(t *testing.T) {
		body := unfinalizedConfig(t) + "\nupstream:\n  url: http://archive-source\n  head: all\n"
		if _, err := LoadConfig(writeConfig(t, body), "unfinalized"); err == nil {
			t.Fatal("archive mirror accepted as live beacon authority")
		}
	})

	t.Run("overlap exceeds window", func(t *testing.T) {
		body := unfinalizedConfig(t) + "\nunfinalized:\n  window_slots: 8\n  overlap_slots: 9\n"
		if _, err := LoadConfig(writeConfig(t, body), "unfinalized"); err == nil {
			t.Fatal("overlap wider than window accepted")
		}
	})
}

func TestLoadConfigParsesEverything(t *testing.T) {
	body := `
metrics_listen: "127.0.0.1:9551"
beacon:
  genesis_time: 1606824023
  seconds_per_slot: 6
archive:
  url: http://archive.example.org:8550
  token_file: ` + tokenFile(t, "  s3cret  \n\n") + `
  head: arbitrum-one
  max_put_blobs: 48
upstream:
  url: http://other-archive.example.org
  head: all
index:
  batch_size: 128
  block_range: 500
  block_fetch_concurrency: 7
  rpc_batch_size: 24
  fetch_concurrency: 8
  max_put_blobs: 32
  poll_interval: 30s
chain:
  parent_chain_rpc: http://l1.example.org:8545
  fetch_blobs: true
  all_head: everything
  sources:
    - type: inbox-events
      address: "0x1c479675ad559DC151F6Ec7ed3FbF8ceE79582B6"
      topic: "0x7394f4a19a13c7b92b5bb71033245305946ef78452f7b4986ac1390b5df4ebd7"
      from_block: 0
      until_block: 21000000
    - type: blob-txs
      address: "0x5050000000000000000000000000000000000050"
      senders: ["0xA4B0000000000000000000000000000000000A4b"]
      from_block: 21000001
`
	cfg, err := LoadConfig(writeConfig(t, body), "chain")
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}

	if cfg.Beacon.SecondsPerSlot != 6 {
		t.Errorf("seconds_per_slot = %d, want 6", cfg.Beacon.SecondsPerSlot)
	}
	if cfg.MetricsListen != "127.0.0.1:9551" {
		t.Errorf("metrics_listen = %q, want the configured address", cfg.MetricsListen)
	}
	if cfg.Index.BatchSize != 128 || cfg.Index.BlockRange != 500 || cfg.Index.MaxPutBlobs != 32 {
		t.Errorf("index knobs = %+v, want batch 128, range 500, put 32", cfg.Index)
	}
	if cfg.Archive.MaxPutBlobs != 48 {
		t.Errorf("archive.max_put_blobs = %d, want 48", cfg.Archive.MaxPutBlobs)
	}
	if cfg.Index.FetchConcurrency != 8 {
		t.Errorf("fetch_concurrency = %d, want 8", cfg.Index.FetchConcurrency)
	}
	if cfg.Index.BlockFetchConcurrency != 7 {
		t.Errorf("block_fetch_concurrency = %d, want 7", cfg.Index.BlockFetchConcurrency)
	}
	if cfg.Index.RPCBatchSize != 24 {
		t.Errorf("rpc_batch_size = %d, want 24", cfg.Index.RPCBatchSize)
	}
	if cfg.Index.PollInterval != 30*time.Second {
		t.Errorf("poll_interval = %v, want 30s", cfg.Index.PollInterval)
	}
	if !cfg.Chain.FetchBlobs {
		t.Error("fetch_blobs = false, want true")
	}
	if cfg.Upstream.Head != "all" {
		t.Errorf("upstream.head = %q, want all", cfg.Upstream.Head)
	}

	// The token is the file's content without the whitespace an operator's
	// editor added; a trailing newline is not part of a secret.
	token, err := cfg.Token()
	if err != nil {
		t.Fatalf("Token: %v", err)
	}
	if token != "s3cret" {
		t.Errorf("token = %q, want %q", token, "s3cret")
	}

	// The whole schedule, parsed: an explicit-topic inbox-events source closed at
	// a block, then an open-ended blob-txs source with a one-address allowlist.
	// The two together exercise every field a SourceConfig carries, and the hex
	// checksums the parser applies.
	sources, err := cfg.ChainSources()
	if err != nil {
		t.Fatalf("ChainSources: %v", err)
	}
	if len(sources) != 2 {
		t.Fatalf("sources = %d, want 2", len(sources))
	}
	if string(sources[0].Type) != "inbox-events" ||
		sources[0].Address.Hex() != "0x1c479675ad559DC151F6Ec7ed3FbF8ceE79582B6" ||
		sources[0].FromBlock != 0 || sources[0].OpenEnded || sources[0].UntilBlock != 21_000_000 {
		t.Errorf("source 0 = %+v, want the closed inbox-events source", sources[0])
	}
	if string(sources[1].Type) != "blob-txs" ||
		sources[1].Address.Hex() != "0x5050000000000000000000000000000000000050" ||
		!sources[1].OpenEnded || sources[1].FromBlock != 21_000_001 || len(sources[1].Senders) != 1 ||
		strings.ToLower(sources[1].Senders[0].Hex()) != "0xa4b0000000000000000000000000000000000a4b" {
		t.Errorf("source 1 = %+v, want the open-ended blob-txs source", sources[1])
	}
}

// TestLoadConfigAnchoredFallback keeps the provider-neutral second-source
// contract explicit. A fallback may be another beacon endpoint or a Bloar
// archive; its bytes remain untrusted and are accepted only after anchoring to
// the block feed's commitments.
func TestLoadConfigAnchoredFallback(t *testing.T) {
	body := "archive:\n  url: http://a\n  token_file: " + tokenFile(t, "s3cret\n") + "\n  head: all\n" +
		"upstream:\n  url: http://beacon\n  fallback_url: http://archive\n  fallback_head: all\n"
	cfg, err := LoadConfig(writeConfig(t, body), "beacon")
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.Upstream.FallbackURL != "http://archive" || cfg.Upstream.FallbackHead != "all" {
		t.Fatalf("fallback = (%q, %q), want (http://archive, all)",
			cfg.Upstream.FallbackURL, cfg.Upstream.FallbackHead)
	}
}

// TestLoadConfigContinuityCheckpoint parses a valid upstream.continuity_checkpoint
// and turns it into the beacon indexer's (slot, root) form.
func TestLoadConfigContinuityCheckpoint(t *testing.T) {
	root := "0x" + strings.Repeat("cd", 32)
	body := "archive:\n  url: http://a\n  token_file: " + tokenFile(t, "s3cret\n") + "\n  head: all\n" +
		"upstream:\n  url: http://beacon\n  continuity_checkpoint:\n    slot: 1234\n    root: \"" + root + "\"\n"
	cfg, err := LoadConfig(writeConfig(t, body), "beacon")
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	cp := cfg.ContinuityCheckpoint()
	if cp == nil {
		t.Fatal("ContinuityCheckpoint() = nil, want the configured checkpoint")
	}
	if cp.Slot != 1234 {
		t.Errorf("checkpoint slot = %d, want 1234", cp.Slot)
	}
	var want [32]byte
	for i := range want {
		want[i] = 0xcd
	}
	if cp.Root != want {
		t.Errorf("checkpoint root = %x, want %x", cp.Root, want)
	}

	// No checkpoint configured -> nil, so a plain config never carries a stray anchor.
	plain, err := LoadConfig(writeConfig(t, beaconConfig(t)), "beacon")
	if err != nil {
		t.Fatalf("LoadConfig (no checkpoint): %v", err)
	}
	if plain.ContinuityCheckpoint() != nil {
		t.Error("ContinuityCheckpoint() is non-nil for a config that set none")
	}
}

// TestBeaconConfigWiresContinuityCheckpoint pins the wiring in runBeacon: the parsed
// checkpoint must actually reach beacon.New. It drives the config through the same
// seam runBeacon uses (newBeaconConfig), so deleting `ContinuityCheckpoint:
// cfg.ContinuityCheckpoint()` from that assembly -- easy to do, since the clients
// build fine without it -- fails this test rather than silently shipping an
// unanchored indexer.
func TestBeaconConfigWiresContinuityCheckpoint(t *testing.T) {
	root := "0x" + strings.Repeat("ab", 32)
	body := "archive:\n  url: http://a\n  token_file: " + tokenFile(t, "s3cret\n") + "\n  head: all\n" +
		"upstream:\n  url: http://beacon\n  continuity_checkpoint:\n    slot: 4242\n    root: \"" + root + "\"\n"
	cfg, err := LoadConfig(writeConfig(t, body), "beacon")
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}

	// The client args are irrelevant to the field under test; the seam only assembles
	// the struct.
	bc := newBeaconConfig(cfg, nil, nil, nil, nil, nil)
	if bc.ContinuityCheckpoint == nil {
		t.Fatal("newBeaconConfig dropped the continuity checkpoint; runBeacon would build an unanchored indexer")
	}
	if bc.ContinuityCheckpoint.Slot != 4242 {
		t.Errorf("wired checkpoint slot = %d, want 4242", bc.ContinuityCheckpoint.Slot)
	}

	// Control: a config with no checkpoint assembles a nil field, so the seam never
	// invents one.
	plain, err := LoadConfig(writeConfig(t, beaconConfig(t)), "beacon")
	if err != nil {
		t.Fatalf("LoadConfig (no checkpoint): %v", err)
	}
	if newBeaconConfig(plain, nil, nil, nil, nil, nil).ContinuityCheckpoint != nil {
		t.Error("newBeaconConfig invented a checkpoint for a config that set none")
	}
}

// TestLoadConfigIsStrict is the reason decoding sets KnownFields: a key this
// struct does not know is an operator's typo, and the alternative to failing is
// running with a default they thought they had overridden.
func TestLoadConfigIsStrict(t *testing.T) {
	body := beaconConfig(t) + `
index:
  batch_sise: 128
`
	_, err := LoadConfig(writeConfig(t, body), "beacon")
	if err == nil {
		t.Fatal("a config with an unknown key was accepted")
	}
	if !strings.Contains(err.Error(), "batch_sise") {
		t.Errorf("the error does not name the offending key: %v", err)
	}
}

func TestLoadConfigRejects(t *testing.T) {
	tests := []struct {
		name string
		cmd  string
		body string
		want string
	}{
		{
			name: "no archive url",
			cmd:  "beacon",
			body: "archive:\n  token_file: /dev/null\n  head: all\nupstream:\n  url: http://b\n",
			want: "archive.url is required",
		},
		{
			name: "no token file",
			cmd:  "beacon",
			body: "archive:\n  url: http://a\n  head: all\nupstream:\n  url: http://b\n",
			want: "archive.token_file is required",
		},
		{
			name: "no head",
			cmd:  "beacon",
			body: "archive:\n  url: http://a\n  token_file: /dev/null\nupstream:\n  url: http://b\n",
			want: "archive.head is required",
		},
		{
			name: "beacon with no upstream",
			cmd:  "beacon",
			body: "archive:\n  url: http://a\n  token_file: /dev/null\n  head: all\n",
			want: "upstream.url is required",
		},
		{
			name: "chain with no parent chain",
			cmd:  "chain",
			body: "beacon:\n  genesis_time: 1\narchive:\n  url: http://a\n  token_file: /dev/null\n  head: arb\n",
			want: "chain.parent_chain_rpc is required",
		},
		{
			// The explicit sources list is required: v1's implicit single-inbox
			// filter is gone, and an empty schedule is refused rather than
			// silently defaulted (spec 10.4).
			name: "chain with no sources",
			cmd:  "chain",
			body: "beacon:\n  genesis_time: 1\narchive:\n  url: http://a\n  token_file: /dev/null\n  head: arb\n" +
				"chain:\n  parent_chain_rpc: http://l1\n",
			want: "at least one source",
		},
		{
			// A zero genesis_time would not fail at runtime. It would file
			// every row several million slots too high, consistently.
			name: "chain with no genesis time",
			cmd:  "chain",
			body: "archive:\n  url: http://a\n  token_file: /dev/null\n  head: arb\n" +
				"chain:\n  parent_chain_rpc: http://l1\n  sources:\n    - type: inbox-events\n" +
				"      address: \"0x1c479675ad559DC151F6Ec7ed3FbF8ceE79582B6\"\n      from_block: 0\n",
			want: "beacon.genesis_time is required",
		},
		{
			// common.HexToAddress would take this silently and produce a real
			// address that emits no logs, and the indexer would advance
			// coverage over the whole chain recording nothing.
			name: "chain with a bad source address",
			cmd:  "chain",
			body: "beacon:\n  genesis_time: 1\narchive:\n  url: http://a\n  token_file: /dev/null\n  head: arb\n" +
				"chain:\n  parent_chain_rpc: http://l1\n  sources:\n    - type: inbox-events\n" +
				"      address: \"not-an-address\"\n      from_block: 0\n",
			want: "is not a 20-byte hex address",
		},
		{
			// The reason a blob-txs source MUST carry an allowlist: without it,
			// any third party's blobs would be recorded as this chain's history
			// (spec 10.4).
			name: "chain blob-txs with an empty allowlist",
			cmd:  "chain",
			body: "beacon:\n  genesis_time: 1\narchive:\n  url: http://a\n  token_file: /dev/null\n  head: arb\n" +
				"chain:\n  parent_chain_rpc: http://l1\n  sources:\n    - type: blob-txs\n" +
				"      address: \"0x5050000000000000000000000000000000000050\"\n      from_block: 0\n",
			want: "any third party",
		},
		{
			name: "chain with an unknown source type",
			cmd:  "chain",
			body: "beacon:\n  genesis_time: 1\narchive:\n  url: http://a\n  token_file: /dev/null\n  head: arb\n" +
				"chain:\n  parent_chain_rpc: http://l1\n  sources:\n    - type: not-a-type\n" +
				"      address: \"0x1c479675ad559DC151F6Ec7ed3FbF8ceE79582B6\"\n      from_block: 0\n",
			want: "is unknown",
		},
		{
			name: "chain source with from after until",
			cmd:  "chain",
			body: "beacon:\n  genesis_time: 1\narchive:\n  url: http://a\n  token_file: /dev/null\n  head: arb\n" +
				"chain:\n  parent_chain_rpc: http://l1\n  sources:\n    - type: inbox-events\n" +
				"      address: \"0x1c479675ad559DC151F6Ec7ed3FbF8ceE79582B6\"\n      from_block: 10\n      until_block: 5\n",
			want: "before from_block",
		},
		{
			name: "chain fetching blobs with no upstream",
			cmd:  "chain",
			body: "beacon:\n  genesis_time: 1\narchive:\n  url: http://a\n  token_file: /dev/null\n  head: arb\n" +
				"chain:\n  parent_chain_rpc: http://l1\n  fetch_blobs: true\n  sources:\n    - type: inbox-events\n" +
				"      address: \"0x1c479675ad559DC151F6Ec7ed3FbF8ceE79582B6\"\n      from_block: 0\n",
			want: "there is nowhere to fetch the blobs from",
		},
		{
			// Waiting for its own coverage to precede itself: a deadlock the
			// config can see coming.
			name: "chain trailing itself",
			cmd:  "chain",
			body: "beacon:\n  genesis_time: 1\narchive:\n  url: http://a\n  token_file: /dev/null\n  head: all\n" +
				"chain:\n  parent_chain_rpc: http://l1\n  all_head: all\n  sources:\n    - type: inbox-events\n" +
				"      address: \"0x1c479675ad559DC151F6Ec7ed3FbF8ceE79582B6\"\n      from_block: 0\n",
			want: "would wait for its own coverage",
		},
		{
			// A fallback head names which archive to read, so it is meaningless --
			// and a likely misconfiguration -- without a fallback URL to read from.
			name: "fallback head without fallback url",
			cmd:  "beacon",
			body: "archive:\n  url: http://a\n  token_file: /dev/null\n  head: all\nupstream:\n  url: http://b\n" +
				"  fallback_head: all\n",
			want: "upstream.fallback_head is set but upstream.fallback_url is not",
		},
		{
			// block_url is anchored mode's block feed; a mirror upstream (head set)
			// reads no block feed, so carrying one is a misconfiguration.
			name: "mirror with a block feed",
			cmd:  "beacon",
			body: "archive:\n  url: http://a\n  token_file: /dev/null\n  head: all\nupstream:\n  url: http://b\n" +
				"  head: all\n  block_url: http://c\n",
			want: "upstream.block_url is set with upstream.head",
		},
		{
			// A fallback is an anchored-mode blob source; a mirror upstream trusts
			// its one archive and takes none.
			name: "mirror with a fallback",
			cmd:  "beacon",
			body: "archive:\n  url: http://a\n  token_file: /dev/null\n  head: all\nupstream:\n  url: http://b\n" +
				"  head: all\n  fallback_url: http://c\n",
			want: "upstream.fallback_url/fallback_head is set with upstream.head",
		},
		{
			// block_url is a beacon-indexer setting; the chain indexer takes a single
			// upstream, so carrying it is a misconfiguration rather than a no-op.
			name: "chain with a block feed",
			cmd:  "chain",
			body: "beacon:\n  genesis_time: 1\narchive:\n  url: http://a\n  token_file: /dev/null\n  head: arb\n" +
				"upstream:\n  url: http://b\n  block_url: http://c\n" +
				"chain:\n  parent_chain_rpc: http://l1\n  sources:\n    - type: inbox-events\n" +
				"      address: \"0x1c479675ad559DC151F6Ec7ed3FbF8ceE79582B6\"\n      from_block: 0\n",
			want: "the chain indexer takes a single upstream",
		},
		{
			// Same for a fallback: the chain indexer fetches exactly the vhs it saw
			// and has no second-source semantics.
			name: "chain with a fallback",
			cmd:  "chain",
			body: "beacon:\n  genesis_time: 1\narchive:\n  url: http://a\n  token_file: /dev/null\n  head: arb\n" +
				"upstream:\n  url: http://b\n  fallback_url: http://c\n" +
				"chain:\n  parent_chain_rpc: http://l1\n  sources:\n    - type: inbox-events\n" +
				"      address: \"0x1c479675ad559DC151F6Ec7ed3FbF8ceE79582B6\"\n      from_block: 0\n",
			want: "the chain indexer takes a single upstream",
		},
		{
			name: "negative max_put_blobs",
			cmd:  "beacon",
			body: "archive:\n  url: http://a\n  token_file: /dev/null\n  head: all\nupstream:\n  url: http://b\n" +
				"index:\n  max_put_blobs: -1\n",
			want: "must be positive",
		},
		{
			name: "negative archive max_put_blobs",
			cmd:  "beacon",
			body: "archive:\n  url: http://a\n  token_file: /dev/null\n  head: all\n  max_put_blobs: -1\n" +
				"upstream:\n  url: http://b\n",
			want: "archive.max_put_blobs is -1, must be positive",
		},
		{
			name: "index max_put_blobs exceeds durable archive expectation",
			cmd:  "beacon",
			body: "archive:\n  url: http://a\n  token_file: /dev/null\n  head: all\n  max_put_blobs: 32\n" +
				"upstream:\n  url: http://b\nindex:\n  max_put_blobs: 64\n",
			want: "index.max_put_blobs is 64 but archive.max_put_blobs is 32",
		},
		{
			// A too-large max_put_blobs 400s every full put mid-run rather than at
			// startup (spec 7.2). The sanity ceiling catches the absurd case before
			// any network round trip; the archive's own limit is the real bound.
			name: "max_put_blobs over the sanity limit",
			cmd:  "beacon",
			body: "archive:\n  url: http://a\n  token_file: /dev/null\n  head: all\nupstream:\n  url: http://b\n" +
				"index:\n  max_put_blobs: 100000\n",
			want: "over the sanity limit",
		},
		{
			// An absurd batch_size pushes the refs POST body over the archive's
			// 16 MiB cap and is refused 400 mid-run (spec 7.2, 10.1).
			name: "batch_size over the limit",
			cmd:  "beacon",
			body: "archive:\n  url: http://a\n  token_file: /dev/null\n  head: all\nupstream:\n  url: http://b\n" +
				"index:\n  batch_size: 100000\n",
			want: "over the limit",
		},
		{
			name: "negative fetch_concurrency",
			cmd:  "beacon",
			body: "archive:\n  url: http://a\n  token_file: /dev/null\n  head: all\nupstream:\n  url: http://b\n" +
				"index:\n  fetch_concurrency: -1\n",
			want: "must be positive",
		},
		{
			name: "negative block_fetch_concurrency",
			cmd:  "beacon",
			body: "archive:\n  url: http://a\n  token_file: /dev/null\n  head: all\nupstream:\n  url: http://b\n" +
				"index:\n  block_fetch_concurrency: -1\n",
			want: "block_fetch_concurrency is -1",
		},
		{
			name: "block_fetch_concurrency over limit",
			cmd:  "beacon",
			body: "archive:\n  url: http://a\n  token_file: /dev/null\n  head: all\nupstream:\n  url: http://b\n" +
				"index:\n  block_fetch_concurrency: 33\n",
			want: "must be in [1,32]",
		},
		{
			name: "negative rpc_batch_size",
			cmd:  "beacon",
			body: "archive:\n  url: http://a\n  token_file: /dev/null\n  head: all\nupstream:\n  url: http://b\n" +
				"index:\n  rpc_batch_size: -1\n",
			want: "rpc_batch_size is -1",
		},
		{
			name: "rpc_batch_size over limit",
			cmd:  "beacon",
			body: "archive:\n  url: http://a\n  token_file: /dev/null\n  head: all\nupstream:\n  url: http://b\n" +
				"index:\n  rpc_batch_size: 129\n",
			want: "must be in [1,128]",
		},
		{
			// A continuity_checkpoint.root that is not a 32-byte hex hash would parse
			// leniently into a real-but-wrong anchor; reject it at load.
			name: "checkpoint with a bad root",
			cmd:  "beacon",
			body: "archive:\n  url: http://a\n  token_file: /dev/null\n  head: all\nupstream:\n  url: http://b\n" +
				"  continuity_checkpoint:\n    slot: 100\n    root: not-a-root\n",
			want: "not a 0x-prefixed 32-byte hex block root",
		},
		{
			// A bare 64-hex root (no 0x) is 32 valid hex bytes, which common.IsHexHash
			// alone accepts; the docs say 0x-prefixed, so the prefix is required exactly
			//.
			name: "checkpoint root without 0x prefix",
			cmd:  "beacon",
			body: "archive:\n  url: http://a\n  token_file: /dev/null\n  head: all\nupstream:\n  url: http://b\n" +
				"  continuity_checkpoint:\n    slot: 100\n    root: \"" + strings.Repeat("ab", 32) + "\"\n",
			want: "not a 0x-prefixed 32-byte hex block root",
		},
		{
			// The checkpoint anchors anchored mode's block-feed continuity walk; a
			// mirror upstream (head set) runs no such walk.
			name: "mirror with a checkpoint",
			cmd:  "beacon",
			body: "archive:\n  url: http://a\n  token_file: /dev/null\n  head: all\nupstream:\n  url: http://b\n" +
				"  head: all\n  continuity_checkpoint:\n    slot: 100\n    root: \"0x" + strings.Repeat("ab", 32) + "\"\n",
			want: "upstream.continuity_checkpoint is set with upstream.head",
		},
		{
			// continuity_checkpoint is a beacon-indexer setting; the chain indexer
			// takes a single upstream and runs no continuity walk.
			name: "chain with a checkpoint",
			cmd:  "chain",
			body: "beacon:\n  genesis_time: 1\narchive:\n  url: http://a\n  token_file: /dev/null\n  head: arb\n" +
				"upstream:\n  url: http://b\n  continuity_checkpoint:\n    slot: 100\n    root: \"0x" + strings.Repeat("ab", 32) + "\"\n" +
				"chain:\n  parent_chain_rpc: http://l1\n  sources:\n    - type: inbox-events\n" +
				"      address: \"0x1c479675ad559DC151F6Ec7ed3FbF8ceE79582B6\"\n      from_block: 0\n",
			want: "the chain indexer takes a single upstream",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := LoadConfig(writeConfig(t, tt.body), tt.cmd)
			if err == nil {
				t.Fatal("the config was accepted")
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("error = %v\nwant it to contain %q", err, tt.want)
			}
		})
	}
}

// TestLoadConfigBounds checks the sanity ceilings at their boundary: a config
// exactly at each limit is accepted and its values survive intact, and one slot
// over is rejected on the offending key. The boundary is the case worth pinning
// -- an off-by-one in the comparison is what would reject a valid config or admit
// an invalid one.
func TestLoadConfigBounds(t *testing.T) {
	body := func(putBlobs, batch int) string {
		return "archive:\n  url: http://a\n  token_file: " + tokenFile(t, "s3cret\n") + "\n  head: all\n" +
			fmt.Sprintf("  max_put_blobs: %d\n", putBlobs) +
			"upstream:\n  url: http://b\n" +
			fmt.Sprintf("index:\n  max_put_blobs: %d\n  batch_size: %d\n", putBlobs, batch)
	}

	cfg, err := LoadConfig(writeConfig(t, body(maxPutBlobsLimit, maxBatchSizeLimit)), "beacon")
	if err != nil {
		t.Fatalf("a config at the ceilings was rejected: %v", err)
	}
	if cfg.Index.MaxPutBlobs != maxPutBlobsLimit || cfg.Index.BatchSize != maxBatchSizeLimit {
		t.Errorf("index = %+v, want max_put_blobs %d and batch_size %d", cfg.Index, maxPutBlobsLimit, maxBatchSizeLimit)
	}
	if cfg.Archive.MaxPutBlobs != maxPutBlobsLimit {
		t.Errorf("archive.max_put_blobs = %d, want %d", cfg.Archive.MaxPutBlobs, maxPutBlobsLimit)
	}

	if _, err := LoadConfig(writeConfig(t, body(maxPutBlobsLimit+1, defaultBatchSize)), "beacon"); err == nil ||
		!strings.Contains(err.Error(), "max_put_blobs") {
		t.Errorf("max_put_blobs %d was not rejected on its own key: %v", maxPutBlobsLimit+1, err)
	}
	if _, err := LoadConfig(writeConfig(t, body(defaultMaxPutBlobs, maxBatchSizeLimit+1)), "beacon"); err == nil ||
		!strings.Contains(err.Error(), "batch_size") {
		t.Errorf("batch_size %d was not rejected on its own key: %v", maxBatchSizeLimit+1, err)
	}
}

// TestTokenRejectsEmpty guards the one file whose emptiness is not obviously an
// error: an empty token would make every write a 401, one round trip into a
// sync run rather than at startup.
func TestTokenRejectsEmpty(t *testing.T) {
	cfg := &Config{}
	cfg.Archive.TokenFile = tokenFile(t, "   \n")
	if _, err := cfg.Token(); err == nil {
		t.Fatal("an empty token file was accepted")
	}
}

// TestResolveTokenFile covers the ${CREDENTIALS_DIRECTORY} handling that lets one
// config serve both the systemd credential handoff (deploy/systemd, audit
// the safety boundary) and a plain file path (manual, container, docker-compose).
func TestResolveTokenFile(t *testing.T) {
	// A plain path is returned untouched, whether or not a credential directory
	// happens to be in the environment.
	t.Run("plain path unchanged", func(t *testing.T) {
		t.Setenv("CREDENTIALS_DIRECTORY", "/run/credentials/bloar-index@beacon-all.service")
		got, err := resolveTokenFile("/etc/bloar/token")
		if err != nil {
			t.Fatalf("resolveTokenFile: %v", err)
		}
		if got != "/etc/bloar/token" {
			t.Fatalf("plain path changed: got %q", got)
		}
	})

	// The credential prefix expands to the delivered copy under the directory
	// systemd set for the unit.
	t.Run("credential prefix expands", func(t *testing.T) {
		t.Setenv("CREDENTIALS_DIRECTORY", "/run/credentials/bloar-index@beacon-all.service")
		got, err := resolveTokenFile("${CREDENTIALS_DIRECTORY}/token")
		if err != nil {
			t.Fatalf("resolveTokenFile: %v", err)
		}
		want := "/run/credentials/bloar-index@beacon-all.service/token"
		if got != want {
			t.Fatalf("resolved path: got %q, want %q", got, want)
		}
	})

	// The prefix with an unset directory is a hard error, never a fallthrough to a
	// literal /token: this is a unit missing LoadCredential=, or the config run
	// outside systemd, and the error must name the variable.
	t.Run("credential prefix without directory errors", func(t *testing.T) {
		t.Setenv("CREDENTIALS_DIRECTORY", "")
		got, err := resolveTokenFile("${CREDENTIALS_DIRECTORY}/token")
		if err == nil {
			t.Fatalf("a credential-style token_file resolved to %q with no CREDENTIALS_DIRECTORY", got)
		}
		if !strings.Contains(err.Error(), "CREDENTIALS_DIRECTORY") {
			t.Fatalf("error does not name the variable: %v", err)
		}
	})

	// Only the exact ${CREDENTIALS_DIRECTORY} prefix is a credential reference:
	// this is not general interpolation, so any other $-looking value is a plain
	// path and is not expanded.
	t.Run("other variables are not interpolated", func(t *testing.T) {
		t.Setenv("CREDENTIALS_DIRECTORY", "/run/credentials/x")
		t.Setenv("HOME", "/home/somebody")
		got, err := resolveTokenFile("${HOME}/token")
		if err != nil {
			t.Fatalf("resolveTokenFile: %v", err)
		}
		if got != "${HOME}/token" {
			t.Fatalf("a non-credential variable was expanded: got %q", got)
		}
	})
}

// TestTokenResolvesCredentialTokenFile proves the systemd form works at the read:
// LoadConfig accepts a ${CREDENTIALS_DIRECTORY}/token config with no variable in
// the environment, and Token(), given the credential directory, resolves to the
// delivered copy and reads it.
func TestTokenResolvesCredentialTokenFile(t *testing.T) {
	path := writeConfig(t, `
archive:
  url: http://archive.example.org:8550
  token_file: "${CREDENTIALS_DIRECTORY}/token"
  head: all
upstream:
  url: http://beacon.example.org
`)
	cfg, err := LoadConfig(path, "beacon")
	if err != nil {
		t.Fatalf("LoadConfig of a credential-form config: %v", err)
	}
	// Nothing is resolved at load; the credential form is left intact.
	if want := "${CREDENTIALS_DIRECTORY}/token"; cfg.Archive.TokenFile != want {
		t.Fatalf("token_file changed at load: got %q, want %q", cfg.Archive.TokenFile, want)
	}

	credDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(credDir, "token"), []byte("s3cret\n"), 0o400); err != nil {
		t.Fatalf("writing the delivered token: %v", err)
	}
	t.Setenv("CREDENTIALS_DIRECTORY", credDir)
	got, err := cfg.Token()
	if err != nil {
		t.Fatalf("Token: %v", err)
	}
	if got != "s3cret" {
		t.Fatalf("token: got %q, want %q", got, "s3cret")
	}
}

// TestTokenRejectsCredentialTokenFileWithoutDir proves resolution moved to the
// read without losing the fail-closed guarantee: LoadConfig of the credential
// form succeeds with no CREDENTIALS_DIRECTORY, but Token() -- which every
// subcommand reaches through archiveClient at startup -- fails with an error
// naming both the variable and the key, never a literal /token.
func TestTokenRejectsCredentialTokenFileWithoutDir(t *testing.T) {
	t.Setenv("CREDENTIALS_DIRECTORY", "")
	path := writeConfig(t, `
archive:
  url: http://archive.example.org:8550
  token_file: "${CREDENTIALS_DIRECTORY}/token"
  head: all
upstream:
  url: http://beacon.example.org
`)
	cfg, err := LoadConfig(path, "beacon")
	if err != nil {
		t.Fatalf("LoadConfig must not need the credential directory: %v", err)
	}
	if _, err := cfg.Token(); err == nil {
		t.Fatal("Token read a credential-style token_file with no CREDENTIALS_DIRECTORY")
	} else if !strings.Contains(err.Error(), "CREDENTIALS_DIRECTORY") {
		t.Fatalf("error does not name the variable: %v", err)
	} else if !strings.Contains(err.Error(), "archive.token_file") {
		t.Fatalf("error does not name the key: %v", err)
	}
}

// TestExampleConfigsParse checks the shipped indexer examples against the real
// loader. See the same test in cmd/bloard for why.
func TestExampleConfigsParse(t *testing.T) {
	// The shipped examples are the systemd form (archive.token_file is
	// ${CREDENTIALS_DIRECTORY}/token), and they must load with NO credential
	// directory in the environment: resolution is deferred to Token() so an
	// installed config is not tied to being run under systemd at config-load time.
	// The credential form is resolved and exercised in
	// TestTokenResolvesCredentialTokenFile.
	dir, err := filepath.Abs(filepath.Join("..", "..", "deploy", "examples"))
	if err != nil {
		t.Fatalf("resolving the examples directory: %v", err)
	}
	for _, tc := range []struct{ file, cmd string }{
		{"beacon-all.yaml", "beacon"},
		{"backfill-all.yaml", "beacon"},
		{"mirror.yaml", "beacon"},
		{"arbitrum-one.yaml", "chain"},
		{"base-mainnet.yaml", "chain"},
	} {
		t.Run(tc.file, func(t *testing.T) {
			cfg, err := LoadConfig(filepath.Join(dir, tc.file), tc.cmd)
			if err != nil {
				t.Fatalf("deploy/examples/%s does not load for `bloar-index %s`: %v", tc.file, tc.cmd, err)
			}
			// These are the systemd-installed indexer configs, so their token MUST
			// be the exact credential form (§3.1). A plain path here would reproduce
			// the safety boundary DynamicUser crash loop the credential handoff fixes.
			if want := "${CREDENTIALS_DIRECTORY}/token"; cfg.Archive.TokenFile != want {
				t.Errorf("deploy/examples/%s: archive.token_file = %q, want the credential form %q",
					tc.file, cfg.Archive.TokenFile, want)
			}
		})
	}
}

func TestBaseMainnetExamplePinsAuthoritativeSchedule(t *testing.T) {
	dir, err := filepath.Abs(filepath.Join("..", "..", "deploy", "examples"))
	if err != nil {
		t.Fatalf("resolving the examples directory: %v", err)
	}
	cfg, err := LoadConfig(filepath.Join(dir, "base-mainnet.yaml"), "chain")
	if err != nil {
		t.Fatalf("loading Base example: %v", err)
	}
	if cfg.Archive.Head != "base" || cfg.Chain.FetchBlobs || cfg.Chain.AllHead != "all" {
		t.Fatalf("Base archive/reuse topology = head %q fetch_blobs %t all_head %q",
			cfg.Archive.Head, cfg.Chain.FetchBlobs, cfg.Chain.AllHead)
	}
	if len(cfg.Chain.Sources) != 1 {
		t.Fatalf("Base source count = %d, want 1", len(cfg.Chain.Sources))
	}
	source := cfg.Chain.Sources[0]
	if source.Type != "blob-txs" ||
		source.Address != "0xff00000000000000000000000000000000008453" ||
		!reflect.DeepEqual(source.Senders, []string{"0x5050f69a9786f081509234f1a7f4684b5e5b76c9"}) ||
		source.FromBlock != 19426587 || source.UntilBlock != nil {
		t.Errorf("Base source schedule = %+v", source)
	}
	if cfg.Index.BlockFetchConcurrency != 4 || cfg.Index.RPCBatchSize != 16 {
		t.Errorf("Base bounded fetch defaults = workers %d batch %d, want 4/16",
			cfg.Index.BlockFetchConcurrency, cfg.Index.RPCBatchSize)
	}
}
