package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"gopkg.in/yaml.v3"

	"github.com/blobarchive/bloar/index/beacon"
	"github.com/blobarchive/bloar/index/chain"
	"github.com/blobarchive/bloar/schema"
)

// Config is an indexer's YAML configuration.
//
// Spec 12 describes it in one line -- "Indexer configs (separate processes)
// carry their upstreams, the archive URL, token, head name, and fetch_blobs" --
// and this is that line expanded. The block names follow the daemon's config
// where the meaning is the same: `beacon:` is the same clock (genesis_time,
// seconds_per_slot) under the same name, because an indexer that disagreed with
// its archive about the clock would file every row on the wrong slot.
//
// Decoding is strict, like the daemon's: an unknown key is an error rather than
// a typo that leaves a default in place.
//
// Not every block applies to every subcommand -- the beacon indexer has no use
// for a parent chain, and the chain indexer needs a clock the beacon one does
// not -- so validation takes the subcommand. A config carrying a block its
// subcommand ignores is accepted: one file per deployment that runs both
// indexers is a reasonable thing to want.
type Config struct {
	// MetricsListen is where this indexer serves /metrics, /healthz and /readyz.
	// Empty (the default) serves none of them and builds no registry, the same
	// semantics as bloard's server.metrics_listen (spec 12). It is a top-level
	// key rather than a server block because an indexer runs no HTTP server of
	// its own -- it is a client (spec 10), and this listener is the one exception.
	// Bind it privately, e.g. "127.0.0.1:9551".
	MetricsListen string `yaml:"metrics_listen"`

	Beacon   BeaconConfig   `yaml:"beacon"`
	Archive  ArchiveConfig  `yaml:"archive"`
	Upstream UpstreamConfig `yaml:"upstream"`
	Index    IndexConfig    `yaml:"index"`
	Chain    ChainConfig    `yaml:"chain"`
	// Unfinalized configures the explicit optimistic-tip subcommand. Keeping it
	// separate prevents a typo from weakening the finalized beacon indexer.
	Unfinalized UnfinalizedConfig `yaml:"unfinalized"`
}

// BeaconConfig is the beacon clock: spec 10.2 turns L1 block timestamps into
// slots with it. Only the chain subcommand reads it; the beacon indexer takes
// slot numbers from the upstream and never does the arithmetic.
type BeaconConfig struct {
	GenesisTime    uint64 `yaml:"genesis_time"`
	SecondsPerSlot uint64 `yaml:"seconds_per_slot"`
}

// ArchiveConfig is the bloar archive this indexer writes (spec 7.2).
type ArchiveConfig struct {
	URL string `yaml:"url"`
	// TokenFile holds the bearer token of spec 7.3, on one line.
	TokenFile string `yaml:"token_file"`
	// Head is the head to write: "all" for the beacon indexer, a chain's name
	// for the chain one.
	Head string `yaml:"head"`
	// MaxPutBlobs is the durable local expectation of the archive's
	// server.max_put_blobs. It lets an indexer validate its own batch bound even
	// while the archive is temporarily unreachable, instead of making a live
	// publication writer a hard startup dependency. When the archive is reachable,
	// checkArchiveLimits requires its advertised value to match this one exactly;
	// configuration drift therefore still fails closed.
	MaxPutBlobs int `yaml:"max_put_blobs"`
}

// UpstreamConfig is where blobs are read from (spec 7.1, 10.1), and it selects
// the beacon indexer's mode.
type UpstreamConfig struct {
	URL string `yaml:"url"`
	// Head selects the mode. Unset is ANCHORED mode: URL is a beacon-shaped blob
	// source whose bytes are untrusted, and BlockURL below is the trusted block
	// feed that decides what each slot contains. Set is MIRROR mode (deterministic
	// replication, spec 11.5): URL is a bloar archive whose coverage decisions this
	// node copies, and Head names which of its heads to reproduce. Mirror mode has
	// no block feed, so it INHERITS the source's completeness rather than proving
	// it -- KZG still anchors the included blobs, but a covered-empty answer over an
	// omitted blob is reproduced, not caught. Use anchored mode against a trusted
	// block feed for an independent completeness check.
	//
	// The modes are not detected; this is the switch. The old hazard -- a beacon
	// node 404ing a pruned slot exactly as an empty one -- no longer records a
	// hole, because anchored mode takes existence from the block feed and treats a
	// blob source's 404 as "this source cannot help", never as absence.
	Head string `yaml:"head"`
	// BlockURL is anchored mode's trusted block feed (a beacon node's block API):
	// the sole authority on which slots carried blocks, whether they were blobless,
	// and what blobs a slot must hold. Anchored mode only (Head unset); it
	// defaults to URL, for the common case where the beacon node is both the block
	// feed and the blob source. Setting it with Head is a configuration error.
	BlockURL string `yaml:"block_url"`
	// FallbackURL is anchored mode's optional second blob source, tried when the
	// primary cannot serve a slot's blocks-attested blobs. Both sources are
	// untrusted byte providers, tried in order; a source is accepted only when its
	// bytes commit to the block-derived versioned hashes, so a fallback needs no
	// corroboration and absence is never recorded from either. The canonical use
	// is a full-history provider behind a primary that has pruned old slots.
	// Forbidden in mirror mode (a mirror trusts its one archive).
	FallbackURL string `yaml:"fallback_url"`
	// FallbackHead names the head to read when the fallback is itself a bloar
	// archive serving as an untrusted byte source; its head only shapes the
	// request path. Setting it without FallbackURL is a configuration error.
	FallbackHead string `yaml:"fallback_head"`
	// ContinuityCheckpoint is anchored mode's trusted (slot, root) floor for the
	// continuity seed walk. The seed walks headers back
	// from the resume point to find the anchor its first present slot must chain to;
	// on a young network whose origin sits within the bounded walk of slot 0, that
	// walk can reach zero without a present header and then has no anchor -- it
	// cannot prove leading absence, so it waits indefinitely. A checkpoint gives it a
	// trusted stopping point: the walk stops at the checkpoint slot and anchors to
	// the configured root (whether the feed 404s that slot or corroborates it; a feed
	// header that disagrees is a fatal error). Its slot MUST be strictly before the
	// first slot the run covers, so it never itself advances coverage. Anchored mode
	// only; raising the head's origin_slot is the alternative.
	ContinuityCheckpoint *CheckpointConfig `yaml:"continuity_checkpoint"`
}

// CheckpointConfig is upstream.continuity_checkpoint.
type CheckpointConfig struct {
	Slot uint64 `yaml:"slot"`
	// Root is the trusted block root at Slot, a 0x-prefixed 32-byte hex hash.
	Root string `yaml:"root"`
}

// IndexConfig is the loop's knobs.
type IndexConfig struct {
	// BatchSize is B in spec 10.1's loop: how many slots one refs batch covers.
	// Beacon subcommand only. Bounded (maxBatchSizeLimit) so a batch's refs POST
	// cannot overrun the archive's 16 MiB body cap.
	BatchSize uint64 `yaml:"batch_size"`
	// BlockRange is how many L1 blocks one scan covers. Chain subcommand only.
	BlockRange uint64 `yaml:"block_range"`
	// BlockFetchConcurrency bounds how many blob-txs full-block chunks the
	// chain indexer reads concurrently. Chain subcommand only.
	BlockFetchConcurrency int `yaml:"block_fetch_concurrency"`
	// RPCBatchSize bounds consecutive eth_getBlockByNumber calls in one
	// JSON-RPC batch per worker. Chain subcommand only.
	RPCBatchSize int `yaml:"rpc_batch_size"`
	// FetchConcurrency bounds how many of a batch's slots the beacon indexer
	// reads from the upstream at once (spec 10.1). Beacon subcommand only. One is
	// the original serial fetch; the default is 6.
	FetchConcurrency int `yaml:"fetch_concurrency"`
	// MaxPutBlobs bounds one POST /bloar/v1/blobs. It must not exceed the
	// durable archive.max_put_blobs expectation (spec 7.2, default 64).
	// checkArchiveLimits cross-checks that expectation against a reachable
	// archive; a local sanity ceiling (maxPutBlobsLimit) catches an absurd value
	// before either check.
	MaxPutBlobs int `yaml:"max_put_blobs"`
	// PollInterval is how long the loop sleeps when it is caught up.
	PollInterval time.Duration `yaml:"poll_interval"`
}

// UnfinalizedConfig bounds the complete provisional generation and its overlap
// with the finalized ALL head.
type UnfinalizedConfig struct {
	// HandoffHead is the finalized-monotonic head whose selected coverage permits
	// old provisional slots to retire. Defaults to all.
	HandoffHead string `yaml:"handoff_head"`
	// WindowSlots is a hard upper bound on [window_start, optimistic head]. If ALL
	// lags far enough to exceed it, the current generation is retained.
	WindowSlots uint64 `yaml:"window_slots"`
	// OverlapSlots is a pointer so an explicit zero (abut exactly at ALL+1) is
	// distinct from omission, which defaults to eight slots.
	OverlapSlots *uint64 `yaml:"overlap_slots"`
}

// ChainConfig is the chain indexer's own configuration (spec 10.2, 10.4).
type ChainConfig struct {
	// ParentChainRPC MUST be a trusted full node, or a provider known to return
	// complete eth_getLogs results or error -- never truncate silently: an
	// inbox-events source trusts it to return every matching log and cannot detect
	// a capped answer (spec 10.2, 10.4).
	ParentChainRPC string `yaml:"parent_chain_rpc"`
	// Sources is the head's ordered filter schedule (spec 10.4, 12). Required and
	// non-empty; there is no implicit single-inbox default, for the reason on
	// ChainSources.
	Sources []SourceConfig `yaml:"sources"`
	// FetchBlobs selects between fetching this chain's blobs from the upstream
	// and waiting for the ALL head's indexer to have put them (spec 10.2).
	FetchBlobs bool `yaml:"fetch_blobs"`
	// AllHead is the head whose coverage gates this one when FetchBlobs is
	// false. Defaults to "all".
	AllHead string `yaml:"all_head"`
}

// SourceConfig is one entry of chain.sources (spec 10.4, 12) as it appears in
// YAML. ChainSources turns it into a chain.Source, parsing its hex fields and
// defaulting an inbox-events topic.
type SourceConfig struct {
	Type    string `yaml:"type"`
	Address string `yaml:"address"`
	// Topic is inbox-events only; omitted, it defaults to the pinned
	// SequencerBatchDelivered topic (spec 10.2, 10.4).
	Topic string `yaml:"topic"`
	// Senders is blob-txs only: the REQUIRED, non-empty allowlist (spec 10.4).
	Senders []string `yaml:"senders"`
	// FromBlock is inclusive. UntilBlock is inclusive too, and a nil pointer is
	// an absent key -- an open-ended source, distinct from an explicit 0.
	FromBlock  uint64  `yaml:"from_block"`
	UntilBlock *uint64 `yaml:"until_block"`
}

// Defaults (spec 7.2, 10, 12).
const (
	defaultBatchSize             = 64
	defaultBlockRange            = 1000
	defaultBlockFetchConcurrency = 4
	defaultRPCBatchSize          = 16
	defaultFetchConcurrency      = 6
	defaultArchiveMaxPutBlobs    = 64
	defaultMaxPutBlobs           = 64
	defaultPollInterval          = 12 * time.Second
	defaultAllHead               = "all"
	defaultMutableWindow         = 96
	defaultMutableOverlap        = 8
)

// Local sanity ceilings validate() rejects above. They are not the operational
// archive limit: archive.max_put_blobs is the durable local expectation,
// cross-checked against a reachable archive by checkArchiveLimits, and
// batch_size never reaches an archive that would tell it no. These only catch
// an absurd value before any network round trip.
const (
	maxBlockFetchConcurrency = 32
	maxRPCBatchSize          = 128
	// maxPutBlobsLimit caps index.max_put_blobs. A put of this many blobs is
	// 1024 * 128 KiB = 128 MiB in one POST /bloar/v1/blobs body, far past any real
	// archive's configuration (spec 7.2 default 64 = 8 MiB), so a value over it is
	// a typo. The archive's actual limit, whatever it is, is enforced separately at
	// startup.
	maxPutBlobsLimit = 1024
	// maxBatchSizeLimit caps index.batch_size (B in spec 10.1): how many slots one
	// POST .../refs body carries. The archive rejects a refs body over 16 MiB
	// (server maxRefsBody). A worst-case row at the mid-2026 maximum of 21
	// blobs/slot is 21 versioned hashes -- "0x"+64 hex, quoted and comma-separated,
	// ~69 bytes each = ~1449 bytes -- plus a ~52-byte {"slot":...,
	// "versioned_hashes":[...]} wrapper, so ~1.5 KiB per row. At this cap the body
	// is 2048 * 1.5 KiB ~= 3 MiB, a ~5x margin under 16 MiB that still holds (~7
	// MiB) if blob counts grow to 48/slot.
	maxBatchSizeLimit = 2048
)

// LoadConfig reads and validates the config at path for a subcommand.
func LoadConfig(path, cmd string) (*Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("bloar-index: opening config: %w", err)
	}
	defer f.Close()

	var cfg Config
	dec := yaml.NewDecoder(f)
	dec.KnownFields(true)
	if err := dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("bloar-index: parsing config %s: %w", path, err)
	}
	// Whether the operator wrote block_url, captured before applyDefaults fills it
	// in from url, so validation can tell an explicit block_url from the defaulted
	// one (a chain config carrying one is a misconfiguration, not a no-op).
	explicitBlockURL := cfg.Upstream.BlockURL != ""
	cfg.applyDefaults()
	if err := cfg.validate(cmd, explicitBlockURL); err != nil {
		return nil, fmt.Errorf("bloar-index: config %s: %w", path, err)
	}
	// A credential-style token_file is NOT resolved here. Resolution happens at
	// Token() (the read), which keeps config loading free of the environment and
	// lets the -token-file override (see main.go) point an authenticated admin
	// command run by hand -- publish-manifest (docs/operations.md §7.5) -- at a
	// plain path, without the installed credential-form config failing to load.
	return &cfg, nil
}

// credentialsDirRef is the one reference resolveTokenFile expands: systemd's
// $CREDENTIALS_DIRECTORY, the per-unit directory a `LoadCredential=` line drops
// the archive token into (deploy/systemd/bloar-index@.service,
// docs/operations.md §3.1). It is matched only as a literal leading prefix; this
// is deliberately not general environment interpolation.
const credentialsDirRef = "${CREDENTIALS_DIRECTORY}"

// resolveTokenFile turns a configured token_file into the path to read from.
//
// A value that does not begin with credentialsDirRef is an ordinary filesystem
// path -- manual invocation, a container, or docker-compose -- and is returned
// unchanged. A value that does begin with it is a systemd credential reference:
// the ${CREDENTIALS_DIRECTORY} prefix is replaced with what systemd sets in the
// environment for a unit carrying `LoadCredential=`, and nothing else in the
// string is touched.
//
// An unset CREDENTIALS_DIRECTORY under a credential-style path is an error, never
// a silent fallthrough: expanding the unset variable to "" would turn
// ${CREDENTIALS_DIRECTORY}/token into a literal /token and read the wrong file
// (or none), which is exactly the misconfiguration the credential handoff exists
// to prevent -- a unit missing its LoadCredential= line, or a credential-style
// config run outside systemd.
func resolveTokenFile(tokenFile string) (string, error) {
	if !strings.HasPrefix(tokenFile, credentialsDirRef) {
		return tokenFile, nil
	}
	dir := os.Getenv("CREDENTIALS_DIRECTORY")
	if dir == "" {
		return "", fmt.Errorf("%q begins with ${CREDENTIALS_DIRECTORY}, but that variable is unset: a "+
			"credential-style token_file resolves only under a systemd unit with a LoadCredential=token:... line "+
			"(deploy/systemd); for manual invocation, a container, or docker-compose, configure a plain file path",
			tokenFile)
	}
	return filepath.Join(dir, strings.TrimPrefix(tokenFile, credentialsDirRef)), nil
}

// applyDefaults fills in everything with a default.
func (c *Config) applyDefaults() {
	if c.Beacon.SecondsPerSlot == 0 {
		c.Beacon.SecondsPerSlot = schema.SecondsPerSlot
	}
	if c.Index.BatchSize == 0 {
		c.Index.BatchSize = defaultBatchSize
	}
	if c.Index.BlockRange == 0 {
		c.Index.BlockRange = defaultBlockRange
	}
	if c.Index.BlockFetchConcurrency == 0 {
		c.Index.BlockFetchConcurrency = defaultBlockFetchConcurrency
	}
	if c.Index.RPCBatchSize == 0 {
		c.Index.RPCBatchSize = defaultRPCBatchSize
	}
	if c.Index.FetchConcurrency == 0 {
		c.Index.FetchConcurrency = defaultFetchConcurrency
	}
	if c.Archive.MaxPutBlobs == 0 {
		c.Archive.MaxPutBlobs = defaultArchiveMaxPutBlobs
	}
	if c.Index.MaxPutBlobs == 0 {
		c.Index.MaxPutBlobs = defaultMaxPutBlobs
	}
	if c.Index.PollInterval == 0 {
		c.Index.PollInterval = defaultPollInterval
	}
	if c.Chain.AllHead == "" {
		c.Chain.AllHead = defaultAllHead
	}
	if c.Unfinalized.HandoffHead == "" {
		c.Unfinalized.HandoffHead = defaultAllHead
	}
	if c.Unfinalized.WindowSlots == 0 {
		c.Unfinalized.WindowSlots = defaultMutableWindow
	}
	if c.Unfinalized.OverlapSlots == nil {
		overlap := uint64(defaultMutableOverlap)
		c.Unfinalized.OverlapSlots = &overlap
	}
	// Anchored mode's block feed defaults to the blob source: the common case is
	// one beacon node that is both. Only when Head is unset -- a mirror upstream
	// reads no block feed.
	if c.Upstream.Head == "" && c.Upstream.BlockURL == "" {
		c.Upstream.BlockURL = c.Upstream.URL
	}
}

// validate rejects a config that would fail later, or worse, not fail.
// explicitBlockURL is whether the operator wrote upstream.block_url before it was
// defaulted (see LoadConfig).
func (c *Config) validate(cmd string, explicitBlockURL bool) error {
	if c.Archive.URL == "" {
		return errors.New("archive.url is required")
	}
	if c.Archive.TokenFile == "" {
		return errors.New("archive.token_file is required: every endpoint an indexer writes is authenticated (spec 7.3)")
	}
	if c.Archive.Head == "" {
		return errors.New("archive.head is required")
	}
	if c.Archive.MaxPutBlobs < 0 {
		return fmt.Errorf("archive.max_put_blobs is %d, must be positive", c.Archive.MaxPutBlobs)
	}
	if c.Archive.MaxPutBlobs > maxPutBlobsLimit {
		return fmt.Errorf("archive.max_put_blobs is %d, over the sanity limit of %d; it is the durable local "+
			"expectation of the archive's server.max_put_blobs and is cross-checked when the archive is reachable",
			c.Archive.MaxPutBlobs, maxPutBlobsLimit)
	}
	if c.Index.MaxPutBlobs < 0 {
		return fmt.Errorf("index.max_put_blobs is %d, must be positive", c.Index.MaxPutBlobs)
	}
	if c.Index.MaxPutBlobs > maxPutBlobsLimit {
		return fmt.Errorf("index.max_put_blobs is %d, over the sanity limit of %d; archive.max_put_blobs "+
			"is the durable operational bound and is cross-checked against a reachable archive (spec 7.2)",
			c.Index.MaxPutBlobs, maxPutBlobsLimit)
	}
	if c.Index.MaxPutBlobs > c.Archive.MaxPutBlobs {
		return fmt.Errorf("index.max_put_blobs is %d but archive.max_put_blobs is %d; every full put could exceed "+
			"the archive limit, so the local configuration is refused without requiring the archive to be online",
			c.Index.MaxPutBlobs, c.Archive.MaxPutBlobs)
	}
	if c.Index.BatchSize > maxBatchSizeLimit {
		return fmt.Errorf("index.batch_size is %d, over the limit of %d; a larger batch's refs POST would exceed "+
			"the archive's 16 MiB body cap and be rejected 400 mid-run (spec 7.2, 10.1)",
			c.Index.BatchSize, maxBatchSizeLimit)
	}
	if c.Index.FetchConcurrency < 0 {
		return fmt.Errorf("index.fetch_concurrency is %d, must be positive", c.Index.FetchConcurrency)
	}
	if c.Index.BlockFetchConcurrency < 1 || c.Index.BlockFetchConcurrency > maxBlockFetchConcurrency {
		return fmt.Errorf("index.block_fetch_concurrency is %d, must be in [1,%d]",
			c.Index.BlockFetchConcurrency, maxBlockFetchConcurrency)
	}
	if c.Index.RPCBatchSize < 1 || c.Index.RPCBatchSize > maxRPCBatchSize {
		return fmt.Errorf("index.rpc_batch_size is %d, must be in [1,%d]",
			c.Index.RPCBatchSize, maxRPCBatchSize)
	}
	// Strictly positive after defaulting: both indexers sleep
	// time.After(poll_interval) when caught up, which a non-positive value turns
	// into an immediate loop hammering their upstream. Zero is the documented
	// default applyDefaults just applied, so a value at or below zero is the
	// operator's; beacon.New and chain.New guard it again as belt and braces.
	if c.Index.PollInterval <= 0 {
		return fmt.Errorf("index.poll_interval is %s, must be positive", c.Index.PollInterval)
	}
	if c.Upstream.FallbackHead != "" && c.Upstream.FallbackURL == "" {
		return errors.New("upstream.fallback_head is set but upstream.fallback_url is not: a head names which archive " +
			"to read, and there is no fallback archive to read it from")
	}
	if cp := c.Upstream.ContinuityCheckpoint; cp != nil {
		// The 0x prefix is required, not just tolerated: common.IsHexHash strips it if
		// present and would accept bare 64-hex, but the docs and CheckpointConfig say
		// 0x-prefixed, so a value without it is rejected.
		has0x := strings.HasPrefix(cp.Root, "0x") || strings.HasPrefix(cp.Root, "0X")
		if !has0x || !common.IsHexHash(cp.Root) {
			return fmt.Errorf("upstream.continuity_checkpoint.root %q is not a 0x-prefixed 32-byte hex block root", cp.Root)
		}
	}

	switch cmd {
	case "beacon":
		if c.Upstream.URL == "" {
			return errors.New("upstream.url is required: the beacon indexer has nothing to read without it")
		}
		if c.Upstream.Head != "" {
			// Mirror mode: the one archive is trusted, so there is no block feed to
			// anchor against and no fallback to corroborate. Both are anchored-mode
			// keys, and carrying them here is a misconfiguration, not a no-op.
			if explicitBlockURL {
				return errors.New("upstream.block_url is set with upstream.head: block_url is anchored mode's trusted " +
					"block feed, and a mirror upstream (head set) reads no block feed -- it trusts its archive")
			}
			if c.Upstream.FallbackURL != "" || c.Upstream.FallbackHead != "" {
				return errors.New("upstream.fallback_url/fallback_head is set with upstream.head: a fallback is an " +
					"anchored-mode blob source, and a mirror upstream (head set) trusts its one archive and takes none")
			}
			if c.Upstream.ContinuityCheckpoint != nil {
				return errors.New("upstream.continuity_checkpoint is set with upstream.head: the checkpoint anchors " +
					"anchored mode's block-feed continuity walk, and a mirror upstream (head set) trusts its one archive " +
					"and runs no such walk")
			}
		}
	case "unfinalized":
		if c.Upstream.URL == "" {
			return errors.New("upstream.url is required: the unfinalized indexer needs a beacon blob source")
		}
		if c.Upstream.BlockURL == "" {
			return errors.New("upstream.block_url is required: the unfinalized indexer needs a trusted root-addressed block feed")
		}
		if c.Upstream.Head != "" {
			return errors.New("upstream.head is forbidden for the unfinalized indexer: an archive mirror cannot attest the live canonical beacon head")
		}
		if c.Upstream.ContinuityCheckpoint != nil {
			return errors.New("upstream.continuity_checkpoint is finalized backfill state and is not used by the root-addressed unfinalized walk")
		}
		if c.Unfinalized.HandoffHead == c.Archive.Head {
			return fmt.Errorf("unfinalized.handoff_head and archive.head are both %q: a mutable head cannot authorize its own retirement", c.Archive.Head)
		}
		if c.Unfinalized.WindowSlots == 0 || c.Unfinalized.WindowSlots > 4096 {
			return fmt.Errorf("unfinalized.window_slots is %d, must be in [1,4096]", c.Unfinalized.WindowSlots)
		}
		if c.Unfinalized.OverlapSlots == nil {
			return errors.New("internal: unfinalized.overlap_slots default was not applied")
		}
		if *c.Unfinalized.OverlapSlots > c.Unfinalized.WindowSlots {
			return fmt.Errorf("unfinalized.overlap_slots is %d, greater than window_slots %d",
				*c.Unfinalized.OverlapSlots, c.Unfinalized.WindowSlots)
		}
	case "chain":
		if explicitBlockURL || c.Upstream.FallbackURL != "" || c.Upstream.FallbackHead != "" ||
			c.Upstream.ContinuityCheckpoint != nil {
			return errors.New("upstream.block_url/fallback_url/fallback_head/continuity_checkpoint " +
				"are beacon-indexer settings: the chain indexer takes a single upstream (it fetches exactly the versioned " +
				"hashes its L1 scan saw, spec 10.2)")
		}
		if c.Chain.ParentChainRPC == "" {
			return errors.New("chain.parent_chain_rpc is required")
		}
		if _, err := c.ChainSources(); err != nil {
			return err
		}
		if c.Beacon.GenesisTime == 0 {
			// This is spec 10.2's slot arithmetic. A zero would not fail: it
			// would file every row several million slots too high, and do it
			// consistently enough to look deliberate.
			return errors.New("beacon.genesis_time is required: it is what turns L1 block timestamps into slots (spec 10.2)")
		}
		if c.Chain.FetchBlobs && c.Upstream.URL == "" {
			return errors.New("chain.fetch_blobs is set but upstream.url is not: there is nowhere to fetch the blobs from")
		}
		if !c.Chain.FetchBlobs && c.Chain.AllHead == c.Archive.Head {
			return fmt.Errorf("chain.all_head and archive.head are both %q: with fetch_blobs off this head would "+
				"wait for its own coverage to precede itself, which it never will", c.Archive.Head)
		}
	default:
		return fmt.Errorf("unknown subcommand %q", cmd)
	}
	return nil
}

// ChainSources parses chain.sources into the engine's ordered schedule (spec
// 10.4), or returns the first problem it finds. It is called at config load, so
// a bad schedule fails there rather than one RPC round trip into a sync run.
//
// There is no backward-compatible single-inbox shorthand. v1 carried one
// sequencer_inbox field and made it the entire filter; this loader requires the
// explicit sources list, because the config's whole contract is strict and
// explicit (see Config) -- an implicit source is exactly the silent default
// KnownFields exists to refuse, and it would carry no from_block, leaving the
// head's origin to stand in for a real posting boundary. The old shape does not
// quietly keep working either: KnownFields rejects a stray `sequencer_inbox` key
// or `arbitrum:` block by name, and deploy/examples carries the sources form.
//
// The per-source hex fields are parsed here, where a lenient common.HexToAddress
// or common.HexToHash would otherwise turn a typo into a real-but-wrong address
// or topic that silently matches nothing; the structural invariants (non-empty
// list, blob-txs allowlist, from <= until) are chain.ValidateSources', so the
// engine enforces the same rules whoever built the Source.
func (c *Config) ChainSources() ([]chain.Source, error) {
	out := make([]chain.Source, 0, len(c.Chain.Sources))
	for i, s := range c.Chain.Sources {
		src := chain.Source{Type: chain.SourceType(s.Type), FromBlock: s.FromBlock}
		if s.UntilBlock == nil {
			src.OpenEnded = true
		} else {
			src.UntilBlock = *s.UntilBlock
		}

		if !common.IsHexAddress(s.Address) {
			return nil, fmt.Errorf("chain.sources[%d].address %q is not a 20-byte hex address", i, s.Address)
		}
		src.Address = common.HexToAddress(s.Address)

		switch chain.SourceType(s.Type) {
		case chain.SourceInboxEvents:
			if len(s.Senders) != 0 {
				return nil, fmt.Errorf("chain.sources[%d] is inbox-events but carries senders, which only blob-txs uses", i)
			}
			if s.Topic == "" {
				src.Topic = chain.SequencerBatchDeliveredTopic
			} else {
				if !common.IsHexHash(s.Topic) {
					return nil, fmt.Errorf("chain.sources[%d].topic %q is not a 32-byte hex hash", i, s.Topic)
				}
				src.Topic = common.HexToHash(s.Topic)
			}
		case chain.SourceBlobTxs:
			if s.Topic != "" {
				return nil, fmt.Errorf("chain.sources[%d] is blob-txs but carries a topic, which only inbox-events uses", i)
			}
			senders := make([]common.Address, 0, len(s.Senders))
			for j, a := range s.Senders {
				if !common.IsHexAddress(a) {
					return nil, fmt.Errorf("chain.sources[%d].senders[%d] %q is not a 20-byte hex address", i, j, a)
				}
				senders = append(senders, common.HexToAddress(a))
			}
			src.Senders = senders
		default:
			return nil, fmt.Errorf("chain.sources[%d].type %q is unknown; want %q or %q",
				i, s.Type, chain.SourceInboxEvents, chain.SourceBlobTxs)
		}
		out = append(out, src)
	}
	if err := chain.ValidateSources(out); err != nil {
		return nil, err
	}
	return out, nil
}

// ContinuityCheckpoint parses upstream.continuity_checkpoint into the beacon
// indexer's form, or nil if none is configured. validate has already checked the
// root is a 0x-prefixed 32-byte hex hash, so the parse cannot fail here.
func (c *Config) ContinuityCheckpoint() *beacon.ContinuityCheckpoint {
	cp := c.Upstream.ContinuityCheckpoint
	if cp == nil {
		return nil
	}
	return &beacon.ContinuityCheckpoint{Slot: cp.Slot, Root: [32]byte(common.HexToHash(cp.Root))}
}

// Token reads the bearer token of spec 7.3.
//
// The systemd-credential form of archive.token_file is resolved here, at the
// read, not at config load: that keeps config loading independent of the
// environment and lets `-token-file` (main.go) substitute a plain path for a
// hand-run authenticated command without the installed credential-form config
// failing to load. Every subcommand reaches this through archiveClient at
// startup, so a missing credential directory still fails a service closed.
func (c *Config) Token() (string, error) {
	path, err := resolveTokenFile(c.Archive.TokenFile)
	if err != nil {
		return "", fmt.Errorf("bloar-index: archive.token_file %w", err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("bloar-index: reading archive.token_file: %w", err)
	}
	token := strings.TrimSpace(string(raw))
	if token == "" {
		return "", fmt.Errorf("bloar-index: archive.token_file %s is empty", path)
	}
	return token, nil
}
