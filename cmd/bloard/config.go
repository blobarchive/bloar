package main

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/ipfs/boxo/ipns"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	"gopkg.in/yaml.v3"

	"github.com/blobarchive/bloar/follow"
	"github.com/blobarchive/bloar/p2p"
	publicationedge "github.com/blobarchive/bloar/p2p/edge"
	"github.com/blobarchive/bloar/pinning"
	"github.com/blobarchive/bloar/schema"
	"github.com/blobarchive/bloar/server"
)

// Config is the YAML configuration of spec 12.
//
// Decoding is strict: a key this struct does not know is an error, because the
// alternative is a typo that silently leaves a default in place. Every key spec
// 12 lists is therefore present here, including the ones no phase-5 code reads.
type Config struct {
	Net     string        `yaml:"net"`
	Beacon  BeaconConfig  `yaml:"beacon"`
	Store   StoreConfig   `yaml:"store"`
	Server  ServerConfig  `yaml:"server"`
	Ingest  IngestConfig  `yaml:"ingest"`
	Publish PublishConfig `yaml:"publish"`
	P2P     P2PConfig     `yaml:"p2p"`
	// Heads is the heads this node writes (spec 11.1). A node that follows
	// heads may have none.
	Heads map[string]HeadConfig `yaml:"heads"`
	// Follow is the heads this node follows (spec 11.3). Absent is a node that
	// follows nothing, which is every writer.
	Follow *FollowConfig `yaml:"follow"`
	// LiveHeads are local-only virtual beacon views. Each joins one finalized
	// physical head to one bounded mutable physical head without adding another
	// entry to the signed publication document.
	LiveHeads map[string]LiveHeadConfig `yaml:"live_heads"`

	// profileSelection is populated only when follow was scalar shorthand. It is
	// intentionally outside the canonical YAML schema: expansion removes the
	// profile control block before the existing KnownFields decoder runs.
	profileSelection *ProfileSelection
}

// LiveHeadConfig maps one virtual beacon path segment to its two independently
// published physical inputs.
type LiveHeadConfig struct {
	FinalizedHead   string `yaml:"finalized_head"`
	UnfinalizedHead string `yaml:"unfinalized_head"`
	// RequireVersionedHashes turns the provisional half into an exact-hash
	// overlay. It is mandatory when FinalizedHead differs from the mutable
	// head's authenticated handoff, because the global tip is not a claim that
	// every provisional blob belongs to the filtered chain.
	RequireVersionedHashes bool `yaml:"require_versioned_hashes"`
}

// FollowConfig is spec 12's follow block: the archive this node replicates
// heads from, and which of them.
//
// A node may write some heads and follow others (spec 11.1's "role is per-head
// via config"), which is why this sits beside heads rather than replacing it.
// What it may not do is both to the same head: one writer per head, and a node
// that wrote and followed one would be racing itself.
type FollowConfig struct {
	// URL is the archive to poll for the publication document (spec 8). IPNS is
	// a directly pinned name for the same document; DNSLink is a stable DNS name
	// that selects an IPNS name one hop away. IPNS and DNSLink are mutually
	// exclusive, while URL may accompany either as a redundant data channel.
	URL     string `yaml:"url"`
	IPNS    string `yaml:"ipns"`
	DNSLink string `yaml:"dnslink"`
	// PubKey is the hex ed25519 public key every document must verify against.
	// It is required for URL/direct-IPNS trust. DNSLink may omit it and delegate
	// the signer through DNS -> authenticated IPNS -> exact document CID ->
	// document self-signature; setting it pins the signer and disables rotation.
	PubKey string `yaml:"pubkey"`
	// ArchiveID optionally pins a singular legacy source to one version-3
	// logical archive. It is required in source-set mode, where independently
	// keyed writers are comparable only inside that stable namespace.
	ArchiveID string `yaml:"archive_id"`
	// Sources is the bounded, explicitly acknowledged set of independent
	// publication authorities. It is mutually exclusive with the singular
	// URL/IPNS/DNSLink/PubKey fields above. SourceSet supplies the operator's
	// monotonic roster revision and acknowledgement of the normalized contents.
	Sources   map[string]FollowSourceConfig `yaml:"sources"`
	SourceSet *FollowSourceSetConfig        `yaml:"source_set"`
	// MigrateLegacySource assigns pre-source-set follower state to one source
	// during the explicit migration. It is a state-transition control and is
	// deliberately excluded from the source-set digest.
	MigrateLegacySource string `yaml:"migrate_legacy_source"`
	sourcesSet          bool
	sourceSetSet        bool
	// PollInterval is how often the document is resolved (spec 11.3).
	PollInterval time.Duration `yaml:"poll_interval"`
	// FetchTimeout bounds one block fetch (spec 11.4).
	FetchTimeout time.Duration `yaml:"fetch_timeout"`
	// Verify is cid (default) or full (spec 11.4).
	Verify string `yaml:"verify"`
	// Heads is the heads to follow, each with the pin policy that decides what
	// this node retains of it (spec 9). Only pin: origin_slot, seg_bits and
	// fanout_bits are the head's own and arrive in its Head block, and a
	// follower that had an opinion about them could only ever be wrong.
	Heads map[string]FollowHeadConfig `yaml:"heads"`
}

// FollowSourceSetConfig is the operator-controlled roster generation. Runtime
// persistence compares Revision and AcknowledgeDigest; config validation makes
// every accepted generation self-describing and explicitly reviewed.
type FollowSourceSetConfig struct {
	Revision          uint64 `yaml:"revision"`
	AcknowledgeDigest string `yaml:"acknowledge_digest"`
}

// FollowSourceConfig is one independently keyed publication authority. Heads
// is explicit because different finalized archives may have different writer
// coverage; every selected mutable head remains single-authority.
type FollowSourceConfig struct {
	URL     string   `yaml:"url"`
	IPNS    string   `yaml:"ipns"`
	DNSLink string   `yaml:"dnslink"`
	PubKey  string   `yaml:"pubkey"`
	Heads   []string `yaml:"heads"`
}

// FollowHeadConfig is one followed head.
type FollowHeadConfig struct {
	// Kind pins the authenticated ordering contract expected from the writer.
	// Omission preserves the finalized-monotonic contract.
	Kind server.HeadKind `yaml:"kind"`
	// HandoffHead is the finalized head whose exact same-document entry must
	// witness an unfinalized-mutable generation. It is required only for mutable
	// heads; the witness need not itself be retained by a filtered replica.
	HandoffHead string `yaml:"handoff_head"`
	// MaxWindowSlots is required only for unfinalized-mutable and bounds the
	// complete generation this follower will fetch and retain.
	MaxWindowSlots uint64    `yaml:"max_window_slots"`
	Pin            PinConfig `yaml:"pin"`
}

// Key parses follow.pubkey.
func (f *FollowConfig) Key() (ed25519.PublicKey, error) {
	text := strings.TrimSpace(f.PubKey)
	if text == "" && f.DNSLink != "" {
		return nil, nil
	}
	raw, err := hex.DecodeString(text)
	if err != nil {
		return nil, fmt.Errorf("follow.pubkey is not hex: %w", err)
	}
	if len(raw) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("follow.pubkey decodes to %d bytes, want an ed25519 public key (%d)",
			len(raw), ed25519.PublicKeySize)
	}
	return ed25519.PublicKey(raw), nil
}

// ExpectedArchiveID parses follow.archive_id. A nil result preserves the
// legacy unpinned mode; source-set validation requires a non-nil result.
func (f *FollowConfig) ExpectedArchiveID() (*server.ArchiveID, error) {
	if f == nil || f.ArchiveID == "" {
		return nil, nil
	}
	id, err := server.ParseArchiveID(f.ArchiveID)
	if err != nil {
		return nil, fmt.Errorf("follow.archive_id: %w", err)
	}
	return &id, nil
}

func (f *FollowConfig) sourceSetMode() bool {
	return f != nil && (f.sourcesSet || f.Sources != nil)
}

type normalizedFollowSource struct {
	ID      string   `json:"id"`
	URL     string   `json:"url"`
	IPNS    string   `json:"ipns"`
	DNSLink string   `json:"dnslink"`
	PubKey  string   `json:"pubkey"`
	Heads   []string `json:"heads"`
}

type followSourceSetDigestInput struct {
	Schema    string                   `json:"schema"`
	Net       string                   `json:"net"`
	ArchiveID string                   `json:"archive_id"`
	Sources   []normalizedFollowSource `json:"sources"`
}

// SourceSetDigest returns the deterministic acknowledgement for this roster.
// It intentionally excludes the roster revision, migration control, polling,
// fetching, verification, and retention policy. The digest changes only when
// the logical archive namespace or normalized source authority/coverage does.
func (f *FollowConfig) SourceSetDigest(netName string) (string, error) {
	if f == nil {
		return "", errors.New("follow source set is absent")
	}
	archiveID, err := f.ExpectedArchiveID()
	if err != nil {
		return "", err
	}
	if archiveID == nil {
		return "", errors.New("follow.archive_id is required in source-set mode")
	}
	sources, err := f.normalizedSources()
	if err != nil {
		return "", err
	}
	raw, err := json.Marshal(followSourceSetDigestInput{
		Schema:    followSourceSetSchema,
		Net:       netName,
		ArchiveID: archiveID.String(),
		Sources:   sources,
	})
	if err != nil {
		return "", fmt.Errorf("encoding normalized follow source set: %w", err)
	}
	sum := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

// runtimeSourceSet maps the strictly validated YAML authority roster to the
// follow package's detached runtime form. The acknowledged digest is recomputed
// independently by both boundaries so a future normalization change cannot
// silently authorize a different roster in the daemon and the library.
func (f *FollowConfig) runtimeSourceSet(netName string) (*follow.SourceSetConfig, error) {
	if f == nil || !f.sourceSetMode() {
		return nil, nil
	}
	if f.SourceSet == nil {
		return nil, errors.New("follow.source_set must be a mapping, not null")
	}
	archiveID, err := f.ExpectedArchiveID()
	if err != nil {
		return nil, err
	}
	if archiveID == nil {
		return nil, errors.New("follow.archive_id is required in source-set mode")
	}
	normalized, err := f.normalizedSources()
	if err != nil {
		return nil, err
	}
	sources := make([]follow.SourceConfig, len(normalized))
	migrationIPNS := ""
	for i, source := range normalized {
		key, err := hex.DecodeString(source.PubKey)
		if err != nil || len(key) != ed25519.PublicKeySize {
			return nil, fmt.Errorf("follow.sources.%s.pubkey is not a normalized ed25519 public key", source.ID)
		}
		sources[i] = follow.SourceConfig{
			ID: source.ID, URL: source.URL, IPNS: source.IPNS, DNSLink: source.DNSLink,
			PubKey: ed25519.PublicKey(key), AllowedHeads: slices.Clone(source.Heads),
		}
		if source.ID == f.MigrateLegacySource {
			migrationIPNS = source.IPNS
		}
	}

	wantText, err := f.SourceSetDigest(netName)
	if err != nil {
		return nil, err
	}
	if f.SourceSet.AcknowledgeDigest != wantText {
		return nil, fmt.Errorf("follow.source_set.acknowledge_digest is %q, expected %q", f.SourceSet.AcknowledgeDigest, wantText)
	}
	wantBytes, err := hex.DecodeString(strings.TrimPrefix(wantText, "sha256:"))
	if err != nil || len(wantBytes) != sha256.Size {
		return nil, errors.New("internal follow source-set digest is not sha256")
	}
	var digest [sha256.Size]byte
	copy(digest[:], wantBytes)
	libraryDigest, err := follow.SourceSetDigest(netName, *archiveID, sources)
	if err != nil {
		return nil, fmt.Errorf("normalizing follow source set for runtime: %w", err)
	}
	if !bytes.Equal(digest[:], libraryDigest[:]) {
		return nil, fmt.Errorf("internal follow source-set normalization disagreement: config=%x runtime=%x", digest, libraryDigest)
	}
	return &follow.SourceSetConfig{
		Revision: f.SourceSet.Revision, Digest: digest, Sources: sources,
		MigrateLegacySource: f.MigrateLegacySource, MigrateLegacyIPNS: migrationIPNS,
	}, nil
}

func (f *FollowConfig) normalizedSources() ([]normalizedFollowSource, error) {
	ids := make([]string, 0, len(f.Sources))
	for id := range f.Sources {
		ids = append(ids, id)
	}
	slices.Sort(ids)

	seenURLs := make(map[string]string, len(ids))
	seenNames := make(map[string]string, len(ids))
	seenSigners := make(map[string]string, len(ids))
	normalized := make([]normalizedFollowSource, 0, len(ids))
	for _, id := range ids {
		if err := validateFollowSourceID(id); err != nil {
			return nil, err
		}
		source := f.Sources[id]
		if source.URL == "" && source.IPNS == "" && source.DNSLink == "" {
			return nil, fmt.Errorf("follow.sources.%s needs at least one channel: url, ipns, or dnslink", id)
		}
		if source.IPNS != "" && source.DNSLink != "" {
			return nil, fmt.Errorf("follow.sources.%s.ipns and dnslink are mutually exclusive name authorities", id)
		}

		n := normalizedFollowSource{ID: id}
		if source.URL != "" {
			value, err := normalizeFollowSourceURL(source.URL)
			if err != nil {
				return nil, fmt.Errorf("follow.sources.%s.url: %w", id, err)
			}
			if prior, exists := seenURLs[value]; exists {
				return nil, fmt.Errorf("follow.sources.%s.url duplicates source %s after normalization", id, prior)
			}
			seenURLs[value] = id
			n.URL = value
		}
		if source.IPNS != "" {
			name, err := ipns.NameFromString(source.IPNS)
			if err != nil {
				return nil, fmt.Errorf("follow.sources.%s.ipns %q is not an IPNS name: %w", id, source.IPNS, err)
			}
			n.IPNS = name.String()
			if prior, exists := seenNames[n.IPNS]; exists {
				return nil, fmt.Errorf("follow.sources.%s.ipns duplicates the name used by source %s", id, prior)
			}
			seenNames[n.IPNS] = id
		}
		if source.DNSLink != "" {
			if err := p2p.ValidateDNSLinkDomain(source.DNSLink); err != nil {
				return nil, fmt.Errorf("follow.sources.%s.dnslink: %w", id, err)
			}
			n.DNSLink = strings.TrimSuffix(strings.ToLower(source.DNSLink), ".")
			if prior, exists := seenNames[n.DNSLink]; exists {
				return nil, fmt.Errorf("follow.sources.%s.dnslink duplicates the name used by source %s", id, prior)
			}
			seenNames[n.DNSLink] = id
		}

		keyText := strings.TrimSpace(source.PubKey)
		key, err := hex.DecodeString(keyText)
		if err != nil || len(key) != ed25519.PublicKeySize {
			return nil, fmt.Errorf("follow.sources.%s.pubkey must be a pinned %d-byte hex ed25519 public key", id, ed25519.PublicKeySize)
		}
		var zeroKey [ed25519.PublicKeySize]byte
		if bytes.Equal(key, zeroKey[:]) {
			return nil, fmt.Errorf("follow.sources.%s.pubkey must not be the all-zero ed25519 public key", id)
		}
		n.PubKey = hex.EncodeToString(key)
		if prior, exists := seenSigners[n.PubKey]; exists {
			return nil, fmt.Errorf("follow.sources.%s.pubkey duplicates the signer used by source %s", id, prior)
		}
		seenSigners[n.PubKey] = id

		if len(source.Heads) == 0 {
			return nil, fmt.Errorf("follow.sources.%s.heads must name at least one followed head", id)
		}
		headSet := make(map[string]struct{}, len(source.Heads))
		for _, head := range source.Heads {
			if _, exists := headSet[head]; exists {
				return nil, fmt.Errorf("follow.sources.%s.heads contains duplicate %q", id, head)
			}
			if _, exists := f.Heads[head]; !exists {
				return nil, fmt.Errorf("follow.sources.%s.heads names %q, which is not in follow.heads", id, head)
			}
			headSet[head] = struct{}{}
			n.Heads = append(n.Heads, head)
		}
		slices.Sort(n.Heads)
		normalized = append(normalized, n)
	}
	return normalized, nil
}

func validateFollowSourceID(id string) error {
	if len(id) < 1 || len(id) > 63 {
		return fmt.Errorf("follow source ID %q must be 1-63 lowercase ASCII letters, digits, or interior hyphens", id)
	}
	for i := 0; i < len(id); i++ {
		c := id[i]
		if (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || (c == '-' && i > 0 && i < len(id)-1) {
			continue
		}
		return fmt.Errorf("follow source ID %q must be 1-63 lowercase ASCII letters, digits, or interior hyphens", id)
	}
	return nil
}

func normalizeFollowSourceURL(raw string) (string, error) {
	if raw == "" || strings.TrimSpace(raw) != raw {
		return "", errors.New("must be a non-empty absolute HTTP(S) URL without surrounding whitespace")
	}
	u, err := url.Parse(raw)
	if err != nil || u == nil {
		return "", errors.New("must be an absolute HTTP(S) URL without userinfo, query, or fragment")
	}
	scheme := strings.ToLower(u.Scheme)
	if u.Opaque != "" || u.Host == "" || (scheme != "http" && scheme != "https") ||
		u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" {
		return "", errors.New("must be an absolute HTTP(S) URL without userinfo, query, or fragment")
	}
	u.Scheme = scheme
	host, port := strings.TrimSuffix(strings.ToLower(u.Hostname()), "."), u.Port()
	if host == "" {
		return "", errors.New("host must not be empty")
	}
	if port != "" {
		value, err := strconv.Atoi(port)
		if err != nil || value < 1 || value > 65535 {
			return "", errors.New("port must be a decimal number in [1,65535]")
		}
		port = strconv.Itoa(value)
		if (scheme == "http" && value == 80) || (scheme == "https" && value == 443) {
			port = ""
		}
	}
	if port != "" {
		u.Host = net.JoinHostPort(host, port)
	} else if strings.Contains(host, ":") {
		u.Host = "[" + host + "]"
	} else {
		u.Host = host
	}
	return strings.TrimSuffix(u.String(), "/"), nil
}

// BeaconConfig is the static network description the beacon endpoints of spec
// 7.1 serve, and the clock Nitro derives slot numbers with.
type BeaconConfig struct {
	GenesisTime    uint64 `yaml:"genesis_time"`
	SecondsPerSlot uint64 `yaml:"seconds_per_slot"`
	// GenesisValidatorsRoot and GenesisForkVersion are served verbatim by
	// /eth/v1/beacon/genesis.
	GenesisValidatorsRoot string `yaml:"genesis_validators_root"`
	// GenesisForkVersion is not in spec 12's config listing, but spec 7.1's
	// genesis payload has the field and there is nowhere else it could come
	// from. It defaults to the all-zero version, which is what a client that
	// does not read it sees either way (Nitro reads only genesis_time).
	GenesisForkVersion string `yaml:"genesis_fork_version"`
	// SpecExtra is passed through to /eth/v1/config/spec. The beacon API makes
	// every value in that map a string; YAML scalars are accepted and rendered
	// as written, so `DEPOSIT_CHAIN_ID: 1` serves "1".
	SpecExtra map[string]any `yaml:"spec_extra"`
}

// StoreConfig is spec 6's on-disk state.
type StoreConfig struct {
	Path string `yaml:"path"`
	// GCInterval is the GC schedule of spec 9: how often serve runs a
	// mark-and-sweep.
	GCInterval time.Duration `yaml:"gc_interval"`
	// ScrubInterval schedules the read-only, full-byte CID integrity pass.
	// Reachability GC remains fast by presence-checking raw leaves; scrub owns
	// validation of every stored raw byte without blocking writers.
	ScrubInterval time.Duration `yaml:"scrub_interval"`
	NodeCacheMB   int           `yaml:"node_cache_mb"`
}

// ServerConfig is spec 7's HTTP surface.
type ServerConfig struct {
	Listen        string `yaml:"listen"`
	AuthTokenFile string `yaml:"auth_token_file"`
	MaxPutBlobs   int    `yaml:"max_put_blobs"`
	// MaxQueryHashes bounds how many versioned_hashes one blobs request may
	// carry, duplicates included (spec 7.1). A request naming more is 400 before
	// any blob lookup: the count half of the safety boundary's fix. Zero is the
	// default, the protocol ceiling of 128; any set value must be in
	// [1, schema.MaxBlobsPerSlotCeiling] -- it may be lowered but not raised above
	// the per-slot ceiling.
	MaxQueryHashes int `yaml:"max_query_hashes"`
	// MaxResponseBytesInFlight is the process-wide ceiling, in bytes, on the peak
	// live memory all in-flight blob responses reserve at once.
	// Every blob-carrying response is admitted against it before it reads
	// anything. Zero is the default of 1 GiB; any value must admit at least one
	// maximum response, or startup fails.
	MaxResponseBytesInFlight int64 `yaml:"max_response_bytes_in_flight"`
	// ImmutableHorizonSlots is how far behind synced_to a slot must be for its
	// answers to be cached immutably (spec 7.1).
	ImmutableHorizonSlots uint64 `yaml:"immutable_horizon_slots"`
	// Connection-lifetime and concurrency bounds. The shipped
	// defaults are safe for a listener exposed directly; a reverse proxy or CDN in
	// front is defense in depth, not a precondition. All are configurable so a test
	// can drive them with short values.
	//
	// ReadHeaderTimeout bounds reading request headers. ReadTimeout is the finite
	// wall-clock bound on reading a WHOLE request, header plus body, and is what
	// closes a slow or stalled body on every path -- including the auth-rejected,
	// unknown-head, and framing-rejected ones, where a handler returns without
	// reading the body and net/http's drain-and-close becomes time-bounded at the
	// server. A valid mutation extends this per-request (MutationBodyTimeout) so a
	// legitimate large upload is not cut off. WriteTimeout is the public blobs
	// response's write budget: a slow reader of a multi-megabyte body cannot hold a
	// handler open past it. IdleTimeout bounds a kept-alive but silent connection.
	ReadHeaderTimeout time.Duration `yaml:"read_header_timeout"`
	ReadTimeout       time.Duration `yaml:"read_timeout"`
	WriteTimeout      time.Duration `yaml:"write_timeout"`
	IdleTimeout       time.Duration `yaml:"idle_timeout"`
	// MutationBodyTimeout is the read-deadline extension a valid authenticated
	// mutation gets before its body is read, sized to upload the byte ceiling its
	// endpoint allows. It must exceed ReadTimeout to have any effect.
	MutationBodyTimeout time.Duration `yaml:"mutation_body_timeout"`
	// MaxHeaderBytes caps request headers, tightened well below net/http's 1 MiB
	// default: bloar's requests are a bearer token and a short path, so a large
	// header block is only ever an attempt to make one cost more than it should.
	MaxHeaderBytes int `yaml:"max_header_bytes"`
	// MaxConns bounds concurrently open connections via a LimitListener (finding
	// the safety boundary). Zero means unlimited, which the applied default never leaves it.
	MaxConns int `yaml:"max_conns"`
	// MetricsListen is where /metrics, /healthz and /readyz are served. Empty
	// (the default) serves none of them and builds no registry.
	//
	// It is a second listener rather than three more routes on the first
	// because the first is public: spec 7.1's read API is meant to sit behind a
	// CDN, and neither a scrape endpoint nor a probe belongs on it. An operator
	// binds this to a private interface ("127.0.0.1:9550").
	MetricsListen string `yaml:"metrics_listen"`
	// PublicReadAdmission is the weighted, default-on budget for the public GET
	// API. It is separate from the response-memory and connection budgets above:
	// those bound work already in flight, while this bounds how quickly new work
	// is admitted globally and per client.
	PublicReadAdmission PublicReadAdmissionConfig `yaml:"public_read_admission"`
}

// PublicReadAdmissionConfig is the YAML adapter for server.PublicReadLimiterConfig.
// Rates are weighted request-cost units per second; bursts are weighted units.
// A blobs request costs one plus its number of versioned_hashes, or one plus
// server.max_query_hashes when unfiltered. Metadata reads cost one.
type PublicReadAdmissionConfig struct {
	// Enabled is a pointer so an omitted key can default on without overwriting
	// an explicit `enabled: false` opt-out.
	Enabled *bool `yaml:"enabled"`

	GlobalRate  float64 `yaml:"global_rate"`
	GlobalBurst int     `yaml:"global_burst"`
	ClientRate  float64 `yaml:"client_rate"`
	ClientBurst int     `yaml:"client_burst"`

	ClientBuckets   int           `yaml:"client_buckets"`
	ClientBucketTTL time.Duration `yaml:"client_bucket_ttl"`

	// TrustedProxyHeader is an IP-list header such as X-Forwarded-For. It is
	// honored only when RemoteAddr belongs to a canonical CIDR in
	// TrustedProxyCIDRs; configuring exactly one side is an error.
	TrustedProxyHeader string   `yaml:"trusted_proxy_header"`
	TrustedProxyCIDRs  []string `yaml:"trusted_proxy_cidrs"`
}

// IngestConfig is the blob intake pipeline's knobs (spec 7.2, 9).
type IngestConfig struct {
	// StagingTTL is how long a staging pin survives an ingest whose refs never
	// arrive (spec 9's window (a)). Zero is pinning.DefaultStagingTTL.
	//
	// It bounds the cost of an abandoned put. Every blob a POST /bloar/v1/blobs
	// accepts is pinned until the refs naming it are applied; if they never are
	// -- an indexer that crashed between the two requests -- the pin, and the
	// blob's disk, last this long. Shorter reclaims sooner and gives a slow
	// indexer less room to finish; longer is disk. See docs/operations.md.
	StagingTTL time.Duration `yaml:"staging_ttl"`
	// VerifyConcurrency bounds how many of a batch's blobs the KZG pass verifies
	// at once (spec 7.2). Zero is a default derived from the core count; 1 is
	// serial. Each blob's commitment already spreads its own arithmetic across
	// every core, so the default is small on purpose: a few concurrent verifies
	// fill the machine and more just adds contention. Raise it only with a
	// measurement showing idle cores under ingest.
	VerifyConcurrency int `yaml:"verify_concurrency"`
}

// PublishConfig is spec 8.
type PublishConfig struct {
	// ArchiveID is the shared logical archive identity published by independent
	// writer keys. It is 32 nonzero random bytes as 64 lowercase hex characters.
	// Optional; setting it activates signed, revisioned publication version 3.
	ArchiveID string `yaml:"archive_id"`
	// SigningKeyFile holds a hex ed25519 key, either a 32-byte seed or a
	// 64-byte private key, on one line. Optional: an unset key publishes an
	// unsigned document.
	SigningKeyFile string `yaml:"signing_key_file"`
	// IPNS turns on the channel of spec 8.1. Without Edge it uses the embedded
	// p2p host. With Edge, this private process signs and hands the record to the
	// public edge without constructing a DHT host of its own.
	IPNS bool `yaml:"ipns"`
	// IPNSRepublish is how often a record is re-signed and put again, against
	// its 48h lifetime.
	IPNSRepublish time.Duration `yaml:"ipns_republish"`
	// Edge moves DHT/Bitswap publication into a distinct public process. The
	// private writer retains the IPNS key and sends only the exact document and
	// already-signed record over an AF_UNIX control socket.
	Edge *PublishEdgeConfig `yaml:"edge"`
}

// PublishEdgeConfig is the private writer's half of the two-process launch
// topology. Mode "required" is the final split. Mode "mirror" is an additive
// canary: the existing monolith remains authoritative while the same signed
// bytes are copied to the edge best-effort.
type PublishEdgeConfig struct {
	Mode            string   `yaml:"mode"`
	ControlSocket   string   `yaml:"control_socket"`
	IdentityKeyFile string   `yaml:"identity_key_file"`
	Multiaddrs      []string `yaml:"multiaddrs"`
	// TransactionTimeout must exactly match the public edge's control timeout.
	// It is sent with every request so config drift fails before mutation.
	TransactionTimeout time.Duration `yaml:"transaction_timeout"`
	// RequestTimeout is the writer's outer AF_UNIX budget. It must be strictly
	// greater than bounded permit admission plus TransactionTimeout, and lower
	// than the edge server ceiling.
	RequestTimeout   time.Duration `yaml:"request_timeout"`
	MaxDocumentBytes int           `yaml:"max_document_bytes"`
}

// P2PConfig is spec 11.2's libp2p host.
//
// A config with no p2p block runs no host at all, which is a whole role of spec
// 11.1: a writer that only serves HTTPS. Presence of the block explicitly opts
// this process into the embedded swarm. Within a present block, an omitted
// listen gets the TCP+QUIC defaults while an explicit listen: [] is dial-only.
type P2PConfig struct {
	Listen []string `yaml:"listen"`
	Peers  []string `yaml:"peers"`
	// Announce becomes the publication document's multiaddrs. Unset, a running
	// host derives them from what it bound; see p2p.Host.AnnounceAddrs.
	Announce []string `yaml:"announce"`
	// IdentityKeyFile is the ed25519 key the PeerID -- and the IPNS name -- is
	// derived from, in publish.signing_key_file's format so that spec 8.1's
	// "MAY be the same key" can be exactly that. Created on first run.
	// Defaults to p2p.key under store.path: the identity has to be stable
	// across restarts or every published multiaddr goes stale, and the store is
	// already the node's identity in every other sense.
	IdentityKeyFile string `yaml:"identity_key_file"`
	// NATPortMap enables UPnP/NAT-PMP listener mappings. It defaults on for an
	// enabled p2p block and can be explicitly disabled for locked-down networks.
	NATPortMap bool `yaml:"nat_port_map"`
	// Bitswap owns the serving posture and every Boxo work bound Bloar relies
	// on. Defaults are applied only when the p2p block enables a host.
	Bitswap BitswapConfig `yaml:"bitswap"`
	// ConnectionManager controls opportunistic pruning. Its low/high marks are
	// not hard connection limits; ResourceManager is the admission boundary.
	ConnectionManager p2p.ConnectionManagerConfig `yaml:"connection_manager"`
	// ResourceManager pins hard system and per-peer libp2p budgets. Zero fields
	// select Bloar's documented defaults; negative or inconsistent values fail
	// during config loading.
	ResourceManager p2p.ResourceManagerConfig `yaml:"resource_manager"`
	// DHT selects how the Amino routing table is seeded. Public is the embedded
	// swarm default; private uses only p2p.peers and is the hermetic mode for a
	// private deployment or test network.
	DHT DHTConfig `yaml:"dht"`
	// Rendezvous is default-on for every embedded host. It discovers peers for
	// the union of written and followed heads; enabled: false retains explicit
	// static peering and any IPNS DHT use without joining rendezvous namespaces.
	Rendezvous RendezvousConfig `yaml:"rendezvous"`
	// Relay is the bounded circuit-v2/DCUtR control plane. The relay service and
	// hole punching default on for an embedded host; AutoRelay remains off until
	// static_candidates names at least one explicit relay.
	Relay RelayConfig `yaml:"relay"`

	// These are syntax-presence bits populated by LoadConfig. Slices alone
	// cannot distinguish an omitted listen key from an explicit empty list, and
	// the zero value cannot distinguish an absent p2p block from p2p: {}.
	present       bool
	listenSet     bool
	natPortMapSet bool
}

// BitswapConfig exposes Bloar's pinned Boxo limits. Values are deliberately
// int64 so YAML cannot silently wrap a platform-sized int/uint before p2p's
// validation sees it. Zero selects the pinned default; negatives are invalid.
type BitswapConfig struct {
	// Serve is a pointer so omitted defaults on while explicit false remains an
	// opt-out. Client fetching stays enabled either way.
	Serve *bool `yaml:"serve"`

	MaxQueuedWantsPerPeer      int64 `yaml:"max_queued_wants_per_peer"`
	MaxOutstandingBytesPerPeer int64 `yaml:"max_outstanding_bytes_per_peer"`
	SendWorkers                int64 `yaml:"send_workers"`
	EngineTaskWorkers          int64 `yaml:"engine_task_workers"`
	BlockstoreWorkers          int64 `yaml:"blockstore_workers"`
	MaxCIDBytes                int64 `yaml:"max_cid_bytes"`
}

type DHTConfig struct {
	// Bootstrap is public (default) or private. Public augments p2p.peers with
	// the go-libp2p Amino bootstrap set; private never reads that set.
	Bootstrap string `yaml:"bootstrap"`
}

func (d DHTConfig) bootstrapMode() string {
	if d.Bootstrap == "" {
		return defaultDHTBootstrap
	}
	return d.Bootstrap
}

type RendezvousConfig struct {
	// Enabled is a pointer so an omitted key defaults on while explicit false
	// is retained as the opt-out.
	Enabled *bool `yaml:"enabled"`
}

// RelayConfig is daemon-facing relay policy. StaticCandidates are full peer
// multiaddrs (direct transport plus terminal /p2p/<peerid>); multiple entries
// for one PeerID are grouped into one AutoRelay candidate.
type RelayConfig struct {
	Service          RelayServiceConfig `yaml:"service"`
	HolePunching     *bool              `yaml:"hole_punching"`
	StaticCandidates []string           `yaml:"static_candidates"`
	AutoRelay        AutoRelayConfig    `yaml:"auto_relay"`
}

type RelayServiceConfig struct {
	Enabled               *bool         `yaml:"enabled"`
	ReservationTTL        time.Duration `yaml:"reservation_ttl"`
	MaxReservations       int           `yaml:"max_reservations"`
	MaxCircuitsPerPeer    int           `yaml:"max_circuits_per_peer"`
	BufferSizeBytes       int           `yaml:"buffer_size_bytes"`
	MaxReservationsPerIP  int           `yaml:"max_reservations_per_ip"`
	MaxReservationsPerASN int           `yaml:"max_reservations_per_asn"`
	CircuitDuration       time.Duration `yaml:"circuit_duration"`
	CircuitDataBytes      int64         `yaml:"circuit_data_bytes"`
}

type AutoRelayConfig struct {
	DesiredReservations int           `yaml:"desired_reservations"`
	MinInterval         time.Duration `yaml:"min_interval"`
	BootDelay           time.Duration `yaml:"boot_delay"`
	Backoff             time.Duration `yaml:"backoff"`
	MaxCandidateAge     time.Duration `yaml:"max_candidate_age"`
}

// Host reports whether this config asks for a libp2p host. YAML block presence
// is authoritative, while the field checks preserve the programmatic config
// path used by tests and embedders.
func (p P2PConfig) Host() bool {
	return p.present || len(p.Listen) > 0 || len(p.Peers) > 0 || p.Bitswap.configured() ||
		p.ConnectionManager != (p2p.ConnectionManagerConfig{}) ||
		p.ResourceManager != (p2p.ResourceManagerConfig{}) ||
		p.DHT.Bootstrap != "" || p.Rendezvous.Enabled != nil || p.Relay.configured()
}

func (b BitswapConfig) configured() bool {
	return b.Serve != nil || b.MaxQueuedWantsPerPeer != 0 || b.MaxOutstandingBytesPerPeer != 0 ||
		b.SendWorkers != 0 || b.EngineTaskWorkers != 0 || b.BlockstoreWorkers != 0 || b.MaxCIDBytes != 0
}

func (r RendezvousConfig) enabled() bool {
	return r.Enabled == nil || *r.Enabled
}

func (r RelayConfig) configured() bool {
	return r.Service.Enabled != nil || r.HolePunching != nil || len(r.StaticCandidates) > 0 ||
		r.Service.ReservationTTL != 0 || r.Service.MaxReservations != 0 ||
		r.Service.MaxCircuitsPerPeer != 0 || r.Service.BufferSizeBytes != 0 ||
		r.Service.MaxReservationsPerIP != 0 || r.Service.MaxReservationsPerASN != 0 ||
		r.Service.CircuitDuration != 0 || r.Service.CircuitDataBytes != 0 ||
		r.AutoRelay.DesiredReservations != 0 || r.AutoRelay.MinInterval != 0 ||
		r.AutoRelay.BootDelay != 0 || r.AutoRelay.Backoff != 0 || r.AutoRelay.MaxCandidateAge != 0
}

// HeadConfig is one head this node writes. OriginSlot, SegBits and FanoutBits
// are immutable for the life of the head (spec 3.1): changing one in the config
// does not migrate anything, it is caught at startup and refused.
type HeadConfig struct {
	// Kind selects the head's local ordering contract. Omission is the legacy
	// finalized-monotonic writer. A mutable writer additionally names the
	// finalized head which authorizes its moving window and its hard size bound.
	Kind           server.HeadKind `yaml:"kind"`
	HandoffHead    string          `yaml:"handoff_head"`
	MaxWindowSlots uint64          `yaml:"max_window_slots"`
	// OriginSlot is a pointer so that an absent one is distinguishable from
	// slot 0, which is a legitimate value on a network whose blobs start at
	// genesis.
	OriginSlot *uint64 `yaml:"origin_slot"`
	SegBits    *uint64 `yaml:"seg_bits"`
	FanoutBits *uint64 `yaml:"fanout_bits"`
	// Pin is the retention policy of spec 9. It is validated here so that a bad
	// policy is a startup error rather than a surprise later, and rendered into
	// a pinning.Policy by headPolicy.
	Pin PinConfig `yaml:"pin"`
}

// PinConfig is a head's pin policy (spec 9).
type PinConfig struct {
	Mode     string        `yaml:"mode"`
	Duration time.Duration `yaml:"duration"`
}

func validateEdgeMultiaddrs(configured []string) (peer.ID, error) {
	if len(configured) == 0 {
		return "", errors.New("publish.edge.multiaddrs must name at least one public edge address")
	}
	var edgePeer peer.ID
	seen := make(map[string]struct{}, len(configured))
	for i, raw := range configured {
		if raw == "" || raw != strings.TrimSpace(raw) {
			return "", fmt.Errorf("publish.edge.multiaddrs[%d] must be non-empty without surrounding whitespace", i)
		}
		address, err := ma.NewMultiaddr(raw)
		if err != nil {
			return "", fmt.Errorf("publish.edge.multiaddrs[%d] %q is invalid: %w", i, raw, err)
		}
		info, err := peer.AddrInfoFromP2pAddr(address)
		if err != nil {
			return "", fmt.Errorf("publish.edge.multiaddrs[%d] %q must end in /p2p/<edge-peer-id>: %w", i, raw, err)
		}
		if edgePeer == "" {
			edgePeer = info.ID
		} else if info.ID != edgePeer {
			return "", fmt.Errorf("publish.edge.multiaddrs name multiple edge peers %s and %s", edgePeer, info.ID)
		}
		if _, duplicate := seen[raw]; duplicate {
			return "", fmt.Errorf("publish.edge.multiaddrs[%d] duplicates %q", i, raw)
		}
		seen[raw] = struct{}{}
	}
	return edgePeer, nil
}

// Defaults. The two spec 12 names as an operator would expect them, plus the
// values the rest of the spec fixes.
const (
	followSourceSetSchema           = "bloar.follow-source-set/v1"
	maxFollowSources                = 32
	defaultListen                   = ":8550"
	defaultMaxPutBlobs              = 64
	defaultMaxQueryHashes           = schema.MaxBlobsPerSlotCeiling
	defaultMaxResponseBytesInFlight = 1 << 30
	defaultImmutableHorizonSlots    = 7200
	// Connection-lifetime and concurrency defaults, all safe for a
	// directly exposed listener. ReadTimeout bounds a whole request's read while
	// staying above the 10s header bound; a valid mutation extends its own body
	// read to mutationBodyTimeout. WriteTimeout is generous enough for the largest
	// blobs response over a poor link but finite. MaxHeaderBytes is 64 KiB, far
	// under net/http's 1 MiB. MaxConns is a per-process connection budget.
	defaultReadHeaderTimeout   = 10 * time.Second
	defaultReadTimeout         = 15 * time.Second
	defaultWriteTimeout        = 120 * time.Second
	defaultIdleTimeout         = 60 * time.Second
	defaultMutationBodyTimeout = 60 * time.Second
	defaultMaxHeaderBytes      = 64 << 10
	defaultMaxConns            = 1024
	// Public-read admission defaults are provisional until production rollout
	// measurements replace them. At the maximum cost of 129 units they admit
	// about eight sustained unfiltered reads/s from one client and 32/s process-
	// wide, with room for 31 and 127 respectively in an initial burst. Filtered
	// serial sync is substantially cheaper (one unit plus each requested hash).
	defaultPublicReadGlobalRate    = 4096
	defaultPublicReadGlobalBurst   = 16384
	defaultPublicReadClientRate    = 1024
	defaultPublicReadClientBurst   = 4096
	defaultPublicReadClientBuckets = 4096
	defaultPublicReadClientTTL     = 15 * time.Minute
	defaultNodeCacheMB             = 256
	defaultGCInterval              = 24 * time.Hour
	defaultScrubInterval           = 7 * 24 * time.Hour
	defaultIPNSRepublish           = 4 * time.Hour
	defaultIdentityKeyFile         = "p2p.key"
	defaultP2PPort                 = "4001"
	defaultDHTBootstrap            = "public"
	defaultSegBits                 = 9
	defaultFanoutBits              = 8
	defaultGenesisForkVersion      = "0x00000000"
	defaultVerify                  = "cid"
	maxMutableWindowSlots          = 4096
)

var defaultP2PListen = []string{
	"/ip4/0.0.0.0/tcp/" + defaultP2PPort,
	"/ip4/0.0.0.0/udp/" + defaultP2PPort + "/quic-v1",
}

// pinModes are the policies of spec 9.
var pinModes = map[string]bool{"full": true, "window": true, "none": true}

// LoadConfig reads and validates the config at path.
func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("bloard: opening config: %w", err)
	}
	expanded, selection, err := expandProfileConfig(path, data)
	if err != nil {
		return nil, fmt.Errorf("bloard: expanding config %s: %w", path, err)
	}

	var cfg Config
	dec := yaml.NewDecoder(bytes.NewReader(expanded))
	dec.KnownFields(true)
	if err := dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("bloard: parsing config %s: %w", path, err)
	}
	if err := markP2PPresence(expanded, &cfg.P2P); err != nil {
		return nil, fmt.Errorf("bloard: parsing config %s: %w", path, err)
	}
	if err := markFollowSourcePresence(expanded, cfg.Follow); err != nil {
		return nil, fmt.Errorf("bloard: parsing config %s: %w", path, err)
	}
	cfg.profileSelection = selection
	if err := cfg.applyDefaults(); err != nil {
		return nil, err
	}
	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("bloard: config %s: %w", path, err)
	}
	// A credential-style auth_token_file is NOT resolved here. Resolution happens
	// at AuthToken() (the read), so a token-free offline subcommand -- `bloard
	// rebuild` (docs/operations.md §7.3) never consumes auth -- loads the installed
	// credential-form config even with no CREDENTIALS_DIRECTORY in the environment.
	// serve() still fails closed: it reads the token before it binds anything.
	return &cfg, nil
}

// markP2PPresence records the two distinctions Go's decoded zero values lose:
// no p2p block versus p2p: {}, and an omitted listen versus listen: []. The
// strict decode above remains authoritative for fields and types; this walk
// only observes the already-parsed YAML shape.
func markP2PPresence(data []byte, p *P2PConfig) error {
	var doc yaml.Node
	if err := yaml.Unmarshal(data, &doc); err != nil {
		return err
	}
	if len(doc.Content) == 0 || doc.Content[0].Kind != yaml.MappingNode {
		return nil
	}
	root := doc.Content[0]
	for i := 0; i+1 < len(root.Content); i += 2 {
		if root.Content[i].Value != "p2p" {
			continue
		}
		block := root.Content[i+1]
		if block.Kind != yaml.MappingNode {
			return errors.New("p2p must be a mapping; use p2p: {} to enable defaults or omit the block to disable the embedded swarm")
		}
		p.present = true
		for j := 0; j+1 < len(block.Content); j += 2 {
			switch block.Content[j].Value {
			case "listen":
				if block.Content[j+1].Kind != yaml.SequenceNode {
					return errors.New("p2p.listen must be a list; use [] for a dial-only host")
				}
				p.listenSet = true
			case "nat_port_map":
				p.natPortMapSet = true
			}
		}
		return nil
	}
	return nil
}

// markFollowSourcePresence preserves explicit empty/null source-set controls,
// whose YAML presence is security-significant but whose decoded Go values can
// otherwise be indistinguishable from omission.
func markFollowSourcePresence(data []byte, f *FollowConfig) error {
	if f == nil {
		return nil
	}
	var doc yaml.Node
	if err := yaml.Unmarshal(data, &doc); err != nil {
		return err
	}
	if len(doc.Content) == 0 || doc.Content[0].Kind != yaml.MappingNode {
		return nil
	}
	root := doc.Content[0]
	for i := 0; i+1 < len(root.Content); i += 2 {
		if root.Content[i].Value != "follow" {
			continue
		}
		block := root.Content[i+1]
		if block.Kind != yaml.MappingNode {
			return nil
		}
		for j := 0; j+1 < len(block.Content); j += 2 {
			switch block.Content[j].Value {
			case "sources":
				f.sourcesSet = true
			case "source_set":
				f.sourceSetSet = true
			}
		}
		return nil
	}
	return nil
}

// credentialsDirRef is the one reference resolveTokenFile expands: systemd's
// $CREDENTIALS_DIRECTORY, the per-unit directory a `LoadCredential=` line drops
// the archive token into (deploy/systemd/bloard.service, docs/operations.md
// §3.1). It is matched only as a literal leading prefix; this is deliberately
// not general environment interpolation.
const credentialsDirRef = "${CREDENTIALS_DIRECTORY}"

// resolveTokenFile turns a configured auth_token_file into the path to read from.
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
			"credential-style auth_token_file resolves only under a systemd unit with a LoadCredential=token:... line "+
			"(deploy/systemd); for manual invocation, a container, or docker-compose, configure a plain file path",
			tokenFile)
	}
	return filepath.Join(dir, strings.TrimPrefix(tokenFile, credentialsDirRef)), nil
}

// applyDefaults fills in everything the spec gives a default for.
func (c *Config) applyDefaults() error {
	if c.Beacon.SecondsPerSlot == 0 {
		c.Beacon.SecondsPerSlot = schema.SecondsPerSlot
	}
	if c.Beacon.GenesisForkVersion == "" {
		c.Beacon.GenesisForkVersion = defaultGenesisForkVersion
	}
	if c.Beacon.GenesisValidatorsRoot == "" {
		c.Beacon.GenesisValidatorsRoot = "0x" + strings.Repeat("0", 64)
	}
	if c.Store.NodeCacheMB == 0 {
		c.Store.NodeCacheMB = defaultNodeCacheMB
	}
	if c.Store.GCInterval == 0 {
		c.Store.GCInterval = defaultGCInterval
	}
	if c.Store.ScrubInterval == 0 {
		c.Store.ScrubInterval = defaultScrubInterval
	}
	if c.Server.Listen == "" {
		c.Server.Listen = defaultListen
	}
	if c.Server.MaxPutBlobs == 0 {
		c.Server.MaxPutBlobs = defaultMaxPutBlobs
	}
	if c.Server.MaxQueryHashes == 0 {
		c.Server.MaxQueryHashes = defaultMaxQueryHashes
	}
	if c.Server.MaxResponseBytesInFlight == 0 {
		c.Server.MaxResponseBytesInFlight = defaultMaxResponseBytesInFlight
	}
	if c.Server.ImmutableHorizonSlots == 0 {
		c.Server.ImmutableHorizonSlots = defaultImmutableHorizonSlots
	}
	if c.Server.ReadHeaderTimeout == 0 {
		c.Server.ReadHeaderTimeout = defaultReadHeaderTimeout
	}
	if c.Server.ReadTimeout == 0 {
		c.Server.ReadTimeout = defaultReadTimeout
	}
	if c.Server.WriteTimeout == 0 {
		c.Server.WriteTimeout = defaultWriteTimeout
	}
	if c.Server.IdleTimeout == 0 {
		c.Server.IdleTimeout = defaultIdleTimeout
	}
	if c.Server.MutationBodyTimeout == 0 {
		c.Server.MutationBodyTimeout = defaultMutationBodyTimeout
	}
	if c.Server.MaxHeaderBytes == 0 {
		c.Server.MaxHeaderBytes = defaultMaxHeaderBytes
	}
	if c.Server.MaxConns == 0 {
		c.Server.MaxConns = defaultMaxConns
	}
	if c.Server.PublicReadAdmission.Enabled == nil {
		enabled := true
		c.Server.PublicReadAdmission.Enabled = &enabled
	}
	if c.Server.PublicReadAdmission.GlobalRate == 0 {
		c.Server.PublicReadAdmission.GlobalRate = defaultPublicReadGlobalRate
	}
	if c.Server.PublicReadAdmission.GlobalBurst == 0 {
		c.Server.PublicReadAdmission.GlobalBurst = defaultPublicReadGlobalBurst
	}
	if c.Server.PublicReadAdmission.ClientRate == 0 {
		c.Server.PublicReadAdmission.ClientRate = defaultPublicReadClientRate
	}
	if c.Server.PublicReadAdmission.ClientBurst == 0 {
		c.Server.PublicReadAdmission.ClientBurst = defaultPublicReadClientBurst
	}
	if c.Server.PublicReadAdmission.ClientBuckets == 0 {
		c.Server.PublicReadAdmission.ClientBuckets = defaultPublicReadClientBuckets
	}
	if c.Server.PublicReadAdmission.ClientBucketTTL == 0 {
		c.Server.PublicReadAdmission.ClientBucketTTL = defaultPublicReadClientTTL
	}
	if c.Ingest.StagingTTL == 0 {
		c.Ingest.StagingTTL = pinning.DefaultStagingTTL
	}
	if c.Publish.IPNSRepublish == 0 {
		c.Publish.IPNSRepublish = defaultIPNSRepublish
	}
	if c.Publish.Edge != nil {
		if c.Publish.Edge.Mode == "" {
			c.Publish.Edge.Mode = "required"
		}
		if c.Publish.Edge.IdentityKeyFile == "" && c.Store.Path != "" {
			c.Publish.Edge.IdentityKeyFile = filepath.Join(c.Store.Path, defaultIdentityKeyFile)
		}
		if c.Publish.Edge.TransactionTimeout == 0 {
			c.Publish.Edge.TransactionTimeout = publicationedge.DefaultTransactionTimeout
		}
		if c.Publish.Edge.RequestTimeout == 0 {
			c.Publish.Edge.RequestTimeout = publicationedge.DefaultRequestTimeout
		}
		if c.Publish.Edge.MaxDocumentBytes == 0 {
			c.Publish.Edge.MaxDocumentBytes = publicationedge.DefaultMaxDocumentBytes
		}
	}
	if c.P2P.Host() && !c.P2P.listenSet && len(c.P2P.Listen) == 0 {
		c.P2P.Listen = append([]string(nil), defaultP2PListen...)
	}
	if c.P2P.Host() && !c.P2P.natPortMapSet {
		c.P2P.NATPortMap = true
	}
	if c.P2P.Host() && c.P2P.IdentityKeyFile == "" && c.Store.Path != "" {
		c.P2P.IdentityKeyFile = filepath.Join(c.Store.Path, defaultIdentityKeyFile)
	}
	if c.P2P.Host() {
		if c.P2P.DHT.Bootstrap == "" {
			c.P2P.DHT.Bootstrap = defaultDHTBootstrap
		}
		if c.P2P.Rendezvous.Enabled == nil {
			enabled := true
			c.P2P.Rendezvous.Enabled = &enabled
		}
		if c.P2P.Bitswap.Serve == nil {
			serve := true
			c.P2P.Bitswap.Serve = &serve
		}
		if c.P2P.Bitswap.MaxQueuedWantsPerPeer == 0 {
			c.P2P.Bitswap.MaxQueuedWantsPerPeer = p2p.DefaultBitswapMaxQueuedWantlistEntriesPerPeer
		}
		if c.P2P.Bitswap.MaxOutstandingBytesPerPeer == 0 {
			c.P2P.Bitswap.MaxOutstandingBytesPerPeer = p2p.DefaultBitswapMaxOutstandingBytesPerPeer
		}
		if c.P2P.Bitswap.SendWorkers == 0 {
			c.P2P.Bitswap.SendWorkers = p2p.DefaultBitswapTaskWorkerCount
		}
		if c.P2P.Bitswap.EngineTaskWorkers == 0 {
			c.P2P.Bitswap.EngineTaskWorkers = p2p.DefaultBitswapEngineTaskWorkerCount
		}
		if c.P2P.Bitswap.BlockstoreWorkers == 0 {
			c.P2P.Bitswap.BlockstoreWorkers = p2p.DefaultBitswapEngineBlockstoreWorkerCount
		}
		if c.P2P.Bitswap.MaxCIDBytes == 0 {
			c.P2P.Bitswap.MaxCIDBytes = p2p.DefaultBitswapMaxCIDSize
		}
		if c.P2P.Relay.Service.Enabled == nil {
			enabled := true
			c.P2P.Relay.Service.Enabled = &enabled
		}
		if c.P2P.Relay.HolePunching == nil {
			enabled := true
			c.P2P.Relay.HolePunching = &enabled
		}
		if c.P2P.Relay.Service.ReservationTTL == 0 {
			c.P2P.Relay.Service.ReservationTTL = p2p.DefaultRelayReservationTTL
		}
		if c.P2P.Relay.Service.MaxReservations == 0 {
			c.P2P.Relay.Service.MaxReservations = p2p.DefaultRelayMaxReservations
		}
		if c.P2P.Relay.Service.MaxCircuitsPerPeer == 0 {
			c.P2P.Relay.Service.MaxCircuitsPerPeer = p2p.DefaultRelayMaxCircuitsPerPeer
		}
		if c.P2P.Relay.Service.BufferSizeBytes == 0 {
			c.P2P.Relay.Service.BufferSizeBytes = p2p.DefaultRelayBufferSizeBytes
		}
		if c.P2P.Relay.Service.MaxReservationsPerIP == 0 {
			c.P2P.Relay.Service.MaxReservationsPerIP = p2p.DefaultRelayMaxReservationsPerIP
		}
		if c.P2P.Relay.Service.MaxReservationsPerASN == 0 {
			c.P2P.Relay.Service.MaxReservationsPerASN = p2p.DefaultRelayMaxReservationsPerASN
		}
		if c.P2P.Relay.Service.CircuitDuration == 0 {
			c.P2P.Relay.Service.CircuitDuration = p2p.DefaultRelayCircuitDuration
		}
		if c.P2P.Relay.Service.CircuitDataBytes == 0 {
			c.P2P.Relay.Service.CircuitDataBytes = p2p.DefaultRelayCircuitDataBytes
		}
		if len(c.P2P.Relay.StaticCandidates) > 0 {
			candidates, err := parseRelayCandidates(c.P2P.Relay.StaticCandidates)
			if err != nil {
				return err
			}
			if c.P2P.Relay.AutoRelay.DesiredReservations == 0 {
				c.P2P.Relay.AutoRelay.DesiredReservations = p2p.DefaultAutoRelayDesiredReservations
				if c.P2P.Relay.AutoRelay.DesiredReservations > len(candidates) {
					c.P2P.Relay.AutoRelay.DesiredReservations = len(candidates)
				}
			}
			if c.P2P.Relay.AutoRelay.MinInterval == 0 {
				c.P2P.Relay.AutoRelay.MinInterval = p2p.DefaultAutoRelayMinInterval
			}
			if c.P2P.Relay.AutoRelay.BootDelay == 0 {
				c.P2P.Relay.AutoRelay.BootDelay = p2p.DefaultAutoRelayBootDelay
			}
			if c.P2P.Relay.AutoRelay.Backoff == 0 {
				c.P2P.Relay.AutoRelay.Backoff = p2p.DefaultAutoRelayBackoff
			}
			if c.P2P.Relay.AutoRelay.MaxCandidateAge == 0 {
				c.P2P.Relay.AutoRelay.MaxCandidateAge = p2p.DefaultAutoRelayMaxCandidateAge
			}
		}
	}

	if f := c.Follow; f != nil {
		if f.PollInterval == 0 {
			f.PollInterval = follow.DefaultPollInterval
		}
		if f.FetchTimeout == 0 {
			f.FetchTimeout = follow.DefaultFetchTimeout
		}
		if f.Verify == "" {
			f.Verify = defaultVerify
		}
		for name, h := range f.Heads {
			if h.Pin.Mode == "" {
				// Not the writer's default. A follower that meant to hold a
				// window and typed the key wrong would silently retain the
				// entire archive, which is the one mistake here with a bill
				// attached; and spec 11.3's whole point is that a follower's
				// policy is its own choice, so it should have to make it.
				return fmt.Errorf("follow.heads.%s.pin.mode is required: a follower's pin policy is what decides "+
					"what it fetches and keeps (spec 11.3), and there is no safe default to guess", name)
			}
			f.Heads[name] = h
		}
	}

	for name, h := range c.Heads {
		if h.OriginSlot == nil && c.Net == "mainnet" {
			// Spec 1: origin_slot defaults to the network's first blob slot.
			// Mainnet is the only network whose first blob slot this build
			// knows; anywhere else the operator has to say.
			origin := uint64(schema.DencunMainnetSlot)
			h.OriginSlot = &origin
		}
		if h.SegBits == nil {
			segBits := uint64(defaultSegBits)
			h.SegBits = &segBits
		}
		if h.FanoutBits == nil {
			fanoutBits := uint64(defaultFanoutBits)
			h.FanoutBits = &fanoutBits
		}
		if h.Pin.Mode == "" {
			h.Pin.Mode = "full"
		}
		c.Heads[name] = h
	}
	return nil
}

// requirePositive rejects a scheduling interval or timeout that is not strictly
// positive after defaulting. Every duration it guards has a
// documented zero sentinel applyDefaults has already replaced, so a value at or
// below zero reaching it is one the operator set to something its consumer -- a
// ticker, a timer, a context deadline -- turns into a panic, a spin loop, or an
// already-expired deadline.
func requirePositive(name string, d time.Duration) error {
	if d <= 0 {
		return fmt.Errorf("%s is %s, must be positive", name, d)
	}
	return nil
}

// validate rejects a config that would fail later, or worse, not fail.
func (c *Config) validate() error {
	if c.Net == "" {
		return errors.New("net is required")
	}
	if c.Beacon.GenesisTime == 0 {
		// Nitro turns L1 block timestamps into slots with this. A zero would
		// not fail; it would serve wrong slots forever.
		return errors.New("beacon.genesis_time is required")
	}
	if _, dup := c.Beacon.SpecExtra["SECONDS_PER_SLOT"]; dup {
		return errors.New("beacon.spec_extra must not carry SECONDS_PER_SLOT: it is served from beacon.seconds_per_slot, " +
			"which is also what the archive's own slot arithmetic uses; two sources for it is one too many")
	}
	if _, err := c.SpecMap(); err != nil {
		return err
	}
	if c.Store.Path == "" {
		return errors.New("store.path is required")
	}
	if c.Server.AuthTokenFile == "" {
		// Spec 7.2's ingest and admin endpoints are always mounted, so there is
		// always something to guard.
		return errors.New("server.auth_token_file is required")
	}
	if c.Server.MaxPutBlobs < 0 {
		return fmt.Errorf("server.max_put_blobs is %d, must be positive", c.Server.MaxPutBlobs)
	}
	if c.Server.MaxQueryHashes < 1 || c.Server.MaxQueryHashes > schema.MaxBlobsPerSlotCeiling {
		return fmt.Errorf("server.max_query_hashes is %d, must be in [1, %d] (the per-slot blob ceiling): a filtered "+
			"request may name no more entries than a slot can hold, and a larger cap would restore the amplification "+
			"surface of the safety boundary", c.Server.MaxQueryHashes, schema.MaxBlobsPerSlotCeiling)
	}
	if c.Server.MaxResponseBytesInFlight < 0 {
		return fmt.Errorf("server.max_response_bytes_in_flight is %d, must be positive", c.Server.MaxResponseBytesInFlight)
	}
	// The budget must admit one maximum response, or a request at the cap would
	// wait forever on a reservation the budget can never grant.
	// server.New enforces the same; checking it here turns a fatal runtime state
	// into a startup error the operator can read.
	if minBudget := server.MaxResponseWeight(c.Server.MaxQueryHashes); c.Server.MaxResponseBytesInFlight < minBudget {
		return fmt.Errorf("server.max_response_bytes_in_flight is %d bytes, but one maximum response can peak at %d bytes "+
			"(%d hashes x worst-case per-entry weight); the budget must admit at least one",
			c.Server.MaxResponseBytesInFlight, minBudget, c.Server.MaxQueryHashes)
	}
	// The connection-lifetime bounds must be non-negative durations, and a valid
	// mutation's body extension has to exceed the base read bound or it could never
	// grant more time than the request already had. applyDefaults
	// has already replaced any zero with its default, so a value here is one the
	// operator set.
	for _, d := range []struct {
		name string
		val  time.Duration
	}{
		{"server.read_header_timeout", c.Server.ReadHeaderTimeout},
		{"server.read_timeout", c.Server.ReadTimeout},
		{"server.write_timeout", c.Server.WriteTimeout},
		{"server.idle_timeout", c.Server.IdleTimeout},
		{"server.mutation_body_timeout", c.Server.MutationBodyTimeout},
	} {
		if d.val < 0 {
			return fmt.Errorf("%s is %s, must not be negative", d.name, d.val)
		}
	}
	if c.Server.MutationBodyTimeout <= c.Server.ReadTimeout {
		return fmt.Errorf("server.mutation_body_timeout (%s) must exceed server.read_timeout (%s): it extends a valid "+
			"upload's body read deadline past the base bound, so a value at or below it grants no extra time",
			c.Server.MutationBodyTimeout, c.Server.ReadTimeout)
	}
	if c.Server.MaxHeaderBytes < 0 {
		return fmt.Errorf("server.max_header_bytes is %d, must not be negative", c.Server.MaxHeaderBytes)
	}
	if c.Server.MaxConns < 0 {
		return fmt.Errorf("server.max_conns is %d, must not be negative", c.Server.MaxConns)
	}
	if err := c.validatePublicReadAdmission(); err != nil {
		return err
	}
	if c.Ingest.StagingTTL < 0 {
		return fmt.Errorf("ingest.staging_ttl is %s, must not be negative", c.Ingest.StagingTTL)
	}
	if c.Ingest.VerifyConcurrency < 0 {
		return fmt.Errorf("ingest.verify_concurrency is %d, must not be negative", c.Ingest.VerifyConcurrency)
	}
	if c.Store.NodeCacheMB < 0 {
		return fmt.Errorf("store.node_cache_mb is %d, must not be negative", c.Store.NodeCacheMB)
	}
	// Every scheduling interval must be strictly positive after defaulting (finding
	// the safety boundary). Zero is the documented sentinel applyDefaults has already replaced,
	// so a non-positive value here is one the operator set to a value its consumer
	// -- a ticker, a timer -- would panic on or spin on. Rejecting it here turns
	// each of those late runtime failures into a startup error; the GC and IPNS
	// library constructors keep their own guard behind this as belt and braces.
	// The server connection-lifetime bounds above are already strictly positive
	// after defaulting via their own the safety boundary checks; these are the intervals that
	// check had no reason to cover.
	if err := requirePositive("store.gc_interval", c.Store.GCInterval); err != nil {
		return err
	}
	if err := requirePositive("store.scrub_interval", c.Store.ScrubInterval); err != nil {
		return err
	}
	if err := requirePositive("publish.ipns_republish", c.Publish.IPNSRepublish); err != nil {
		return err
	}
	if err := c.validateBitswap(); err != nil {
		return err
	}
	if err := c.validateP2PResources(); err != nil {
		return err
	}
	if c.P2P.Host() && c.P2P.DHT.Bootstrap != "public" && c.P2P.DHT.Bootstrap != "private" {
		return fmt.Errorf("p2p.dht.bootstrap is %q, must be public or private", c.P2P.DHT.Bootstrap)
	}
	if c.Publish.Edge != nil {
		if !c.Publish.IPNS {
			return errors.New("publish.edge requires publish.ipns: the edge carries already-signed IPNS records")
		}
		if c.Publish.Edge.Mode != "required" && c.Publish.Edge.Mode != "mirror" {
			return fmt.Errorf("publish.edge.mode is %q, must be required or mirror", c.Publish.Edge.Mode)
		}
		if c.Publish.Edge.Mode == "required" && c.P2P.Host() {
			return errors.New("publish.edge.mode required forbids an embedded p2p host: the private writer must not join the public DHT")
		}
		if c.Publish.Edge.Mode == "mirror" && !c.P2P.Host() {
			return errors.New("publish.edge.mode mirror requires the existing embedded p2p host it mirrors")
		}
		if c.Publish.Edge.IdentityKeyFile == "" {
			return errors.New("publish.edge.identity_key_file is required")
		}
		if c.Publish.Edge.Mode == "mirror" && c.Publish.Edge.IdentityKeyFile != c.P2P.IdentityKeyFile {
			return fmt.Errorf("publish.edge.identity_key_file %q must equal p2p.identity_key_file %q in mirror mode: "+
				"the incumbent IPNS authority remains authoritative", c.Publish.Edge.IdentityKeyFile, c.P2P.IdentityKeyFile)
		}
		if _, err := publicationedge.NewClientPolicy(publicationedge.ClientConfig{
			Socket:             c.Publish.Edge.ControlSocket,
			TransactionTimeout: c.Publish.Edge.TransactionTimeout,
			RequestTimeout:     c.Publish.Edge.RequestTimeout,
			MaxDocumentBytes:   c.Publish.Edge.MaxDocumentBytes,
		}); err != nil {
			return fmt.Errorf("publish.edge: %w", err)
		}
		if _, err := validateEdgeMultiaddrs(c.Publish.Edge.Multiaddrs); err != nil {
			return err
		}
	}
	if c.Publish.IPNS && !c.P2P.Host() && c.Publish.Edge == nil {
		return errors.New("publish.ipns is set but there is no p2p block to publish from: the record is signed by the " +
			"libp2p identity and put to a DHT the host joins, and neither exists without one. Give p2p.listen an address " +
			"(or p2p.peers a peer, for a node that only dials out), or unset publish.ipns rather than run an archive that " +
			"believes it is publishing to IPNS and is not")
	}
	if c.Publish.ArchiveID != "" {
		if _, err := server.ParseArchiveID(c.Publish.ArchiveID); err != nil {
			return fmt.Errorf("publish.archive_id: %w", err)
		}
		if c.Publish.SigningKeyFile == "" {
			return errors.New("publish.archive_id requires publish.signing_key_file: a logical archive identity must be signed")
		}
		if len(c.Heads) == 0 {
			return errors.New("publish.archive_id is set but this node writes no heads")
		}
	}
	// The follow block first: a config that carries one and got it wrong should
	// be told what is wrong with it, not that it has no heads -- which, if the
	// block was meant to supply them, is a symptom rather than the fault.
	if err := c.validateFollow(); err != nil {
		return err
	}
	if len(c.Heads) == 0 && len(c.Follow.heads()) == 0 {
		// Either role, or both (spec 11.1). Neither is a daemon that opens a
		// store, serves an API with nothing behind it, and does nothing else.
		return errors.New("this node must write at least one head or follow at least one: set heads (spec 11.1) " +
			"or follow.heads (spec 11.3)")
	}
	for name, h := range c.Heads {
		if err := h.validate(name); err != nil {
			return err
		}
		if h.effectiveKind() != server.UnfinalizedMutable {
			continue
		}
		if c.Publish.SigningKeyFile == "" {
			return fmt.Errorf("heads.%s is unfinalized-mutable but publish.signing_key_file is empty: mutable publication revisions require one stable signing authority", name)
		}
		handoff, ok := c.Heads[h.HandoffHead]
		if !ok {
			return fmt.Errorf("heads.%s.handoff_head %q is not a locally written head: the first mutable writer requires its finalized handoff in the same atomic registry", name, h.HandoffHead)
		}
		if handoff.effectiveKind() != server.FinalizedMonotonic {
			return fmt.Errorf("heads.%s.handoff_head %q is %q, must be %q", name, h.HandoffHead,
				handoff.effectiveKind(), server.FinalizedMonotonic)
		}
	}
	return c.validateLiveHeads()
}

func (c *Config) validateLiveHeads() error {
	followedOverlays := make(map[string]string)
	for name, view := range c.LiveHeads {
		switch {
		case name == "":
			return errors.New("live_heads contains an empty virtual name")
		case strings.Contains(name, "/"):
			return fmt.Errorf("live_heads.%s is not one URL path segment", name)
		case view.FinalizedHead == "":
			return fmt.Errorf("live_heads.%s.finalized_head is required", name)
		case view.UnfinalizedHead == "":
			return fmt.Errorf("live_heads.%s.unfinalized_head is required", name)
		case view.FinalizedHead == view.UnfinalizedHead:
			return fmt.Errorf("live_heads.%s maps both roles to physical head %q", name, view.FinalizedHead)
		}
		if _, collision := c.declaredHeadKind(name); collision {
			return fmt.Errorf("live_heads.%s collides with a physical head", name)
		}
		finalized, ok := c.declaredHeadKind(view.FinalizedHead)
		if !ok {
			return fmt.Errorf("live_heads.%s.finalized_head %q is not a declared written or followed head", name, view.FinalizedHead)
		}
		if finalized != server.FinalizedMonotonic {
			return fmt.Errorf("live_heads.%s.finalized_head %q is %q, must be %q",
				name, view.FinalizedHead, finalized, server.FinalizedMonotonic)
		}
		unfinalized, ok := c.declaredHeadKind(view.UnfinalizedHead)
		if !ok {
			return fmt.Errorf("live_heads.%s.unfinalized_head %q is not a declared written or followed head", name, view.UnfinalizedHead)
		}
		if unfinalized != server.UnfinalizedMutable {
			return fmt.Errorf("live_heads.%s.unfinalized_head %q is %q, must be %q",
				name, view.UnfinalizedHead, unfinalized, server.UnfinalizedMutable)
		}
		// A mismatched finalized name is deliberately not a replacement proof
		// authority. It is a filtered serving frontier over the globally proved
		// mutable tip, and therefore must be exact-hash-only. setupFollow also binds
		// this pair into whole-document gap validation before adoption.
		var handoff string
		if mutable, written := c.Heads[view.UnfinalizedHead]; written {
			handoff = mutable.HandoffHead
		} else if c.Follow != nil {
			handoff = c.Follow.Heads[view.UnfinalizedHead].HandoffHead
		}
		if handoff != "" && handoff != view.FinalizedHead && !view.RequireVersionedHashes {
			return fmt.Errorf("live_heads.%s pairs mutable head %q (authenticated handoff %q) with filtered finalized head %q; require_versioned_hashes must be true",
				name, view.UnfinalizedHead, handoff, view.FinalizedHead)
		}
		if c.Follow != nil {
			_, mutableFollowed := c.Follow.Heads[view.UnfinalizedHead]
			_, finalizedFollowed := c.Follow.Heads[view.FinalizedHead]
			if mutableFollowed && finalizedFollowed && handoff != view.FinalizedHead {
				if prior := followedOverlays[view.UnfinalizedHead]; prior != "" && prior != view.FinalizedHead {
					return fmt.Errorf("followed mutable head %q is mapped to multiple filtered finalized heads (%q and %q); configure one retained overlay frontier",
						view.UnfinalizedHead, prior, view.FinalizedHead)
				}
				followedOverlays[view.UnfinalizedHead] = view.FinalizedHead
			}
		}
	}
	return nil
}

func (c *Config) declaredHeadKind(name string) (server.HeadKind, bool) {
	if h, ok := c.Heads[name]; ok {
		return h.effectiveKind(), true
	}
	if c.Follow != nil {
		if h, ok := c.Follow.Heads[name]; ok {
			return h.effectiveKind(), true
		}
	}
	return "", false
}

func (c *Config) serverLiveHeads() map[string]server.LiveHead {
	if len(c.LiveHeads) == 0 {
		return nil
	}
	views := make(map[string]server.LiveHead, len(c.LiveHeads))
	for name, view := range c.LiveHeads {
		views[name] = server.LiveHead{
			FinalizedHead: view.FinalizedHead, UnfinalizedHead: view.UnfinalizedHead,
			RequireVersionedHashes: view.RequireVersionedHashes,
		}
	}
	return views
}

// followedLiveOverlays is the subset of live views whose two physical sources
// are followed from the same authenticated publication and whose retained
// finalized frontier differs from the mutable proof's handoff. validateLiveHeads
// has already proved that each such view is exact-hash-only and that one mutable
// name maps to one retained frontier; setupFollow passes this contract to the
// follower's atomic document admission. A locally written finalized source is
// not part of the remote document and stays fail-closed at the server's runtime
// slot boundary instead.
func (c *Config) followedLiveOverlays() map[string]string {
	if c.Follow == nil {
		return nil
	}
	overlays := make(map[string]string)
	for _, view := range c.LiveHeads {
		mutable, followed := c.Follow.Heads[view.UnfinalizedHead]
		_, finalizedFollowed := c.Follow.Heads[view.FinalizedHead]
		if followed && mutable.effectiveKind() == server.UnfinalizedMutable &&
			finalizedFollowed && mutable.HandoffHead != view.FinalizedHead {
			overlays[view.UnfinalizedHead] = view.FinalizedHead
		}
	}
	if len(overlays) == 0 {
		return nil
	}
	return overlays
}

// heads is the followed head map of a config block that may be absent.
func (f *FollowConfig) heads() map[string]FollowHeadConfig {
	if f == nil {
		return nil
	}
	return f.Heads
}

// validateFollow rejects a follow block that could not run.
func (c *Config) validateFollow() error {
	f := c.Follow
	if f == nil {
		return nil
	}
	if len(f.Heads) == 0 {
		return errors.New("follow is set but follow.heads is empty: there is nothing to follow. Remove the block, " +
			"or name the heads to replicate")
	}
	sourceMode, err := c.validateFollowAuthority(f)
	if err != nil {
		return err
	}
	if _, err := follow.ParseVerify(f.Verify); err != nil {
		return fmt.Errorf("follow.verify is %q, must be one of cid, full", f.Verify)
	}
	// Strictly positive after defaulting: applyDefaults has
	// replaced a zero poll interval or fetch timeout with follow's default, so a
	// non-positive value here is the operator's. A negative poll interval panics
	// the follower's time.NewTicker; a negative fetch timeout makes every block
	// read a pre-expired context. follow.New guards both again as belt and braces.
	if err := requirePositive("follow.poll_interval", f.PollInterval); err != nil {
		return err
	}
	if err := requirePositive("follow.fetch_timeout", f.FetchTimeout); err != nil {
		return err
	}
	if !c.P2P.Host() {
		// Not a warning. Adopting a root is reading its Head block, and a
		// follower with no libp2p host has no way to read a block it does not
		// have: the entire protocol is bitswap (spec 11.2), and the publication
		// document only says which CIDs to go and get.
		return errors.New("follow is set but there is no p2p block: a follower replicates over bitswap (spec 11.2) " +
			"and cannot fetch a single block without a host. Give p2p.listen an address, or p2p.peers the writer's")
	}
	for name, h := range f.Heads {
		if _, dup := c.Heads[name]; dup {
			// Spec 11.1: exactly one writer per head. A node that wrote and
			// followed the same head would apply refs to a root the next poll
			// replaced, and publish whichever won the race.
			return fmt.Errorf("head %q is in both heads and follow.heads: a head has exactly one writer (spec 11.1), "+
				"so this node either writes it or follows it", name)
		}
		if !pinModes[h.Pin.Mode] {
			return fmt.Errorf("follow.heads.%s.pin.mode is %q, must be one of full, window, none", name, h.Pin.Mode)
		}
		if h.Pin.Mode == "window" && h.Pin.Duration == 0 {
			return fmt.Errorf("follow.heads.%s.pin.mode is window but no duration is set", name)
		}
		kind := h.effectiveKind()
		switch kind {
		case server.FinalizedMonotonic:
			if h.HandoffHead != "" || h.MaxWindowSlots != 0 {
				return fmt.Errorf("follow.heads.%s is finalized-monotonic but carries mutable handoff/window fields", name)
			}
		case server.UnfinalizedMutable:
			if !sourceMode && f.PubKey == "" {
				return fmt.Errorf("follow.heads.%s is unfinalized-mutable but follow.pubkey is empty: mutable revision order requires one pinned signing authority", name)
			}
			if h.Pin.Mode != "full" {
				return fmt.Errorf("follow.heads.%s is unfinalized-mutable and must use pin.mode full", name)
			}
			if h.HandoffHead == "" {
				return fmt.Errorf("follow.heads.%s is unfinalized-mutable but handoff_head is empty", name)
			}
			if h.HandoffHead == name {
				return fmt.Errorf("follow.heads.%s cannot name itself as handoff_head", name)
			}
			if h.MaxWindowSlots == 0 || h.MaxWindowSlots > maxMutableWindowSlots {
				return fmt.Errorf("follow.heads.%s.max_window_slots is %d, must be in [1,%d]", name, h.MaxWindowSlots, maxMutableWindowSlots)
			}
		default:
			return fmt.Errorf("follow.heads.%s.kind is %q, must be %q or %q", name, h.Kind,
				server.FinalizedMonotonic, server.UnfinalizedMutable)
		}
	}
	return nil
}

func (c *Config) validateFollowAuthority(f *FollowConfig) (bool, error) {
	sourcesPresent := f.sourceSetMode()
	sourceSetPresent := f.sourceSetSet || f.SourceSet != nil
	if sourcesPresent != sourceSetPresent {
		if sourcesPresent {
			return false, errors.New("follow.sources requires follow.source_set with a positive revision and exact acknowledgement digest")
		}
		return false, errors.New("follow.source_set requires follow.sources")
	}
	if !sourcesPresent {
		if f.MigrateLegacySource != "" {
			return false, errors.New("follow.migrate_legacy_source is valid only with follow.sources and follow.source_set")
		}
		if _, err := f.ExpectedArchiveID(); err != nil {
			return false, err
		}
		if f.URL == "" && f.IPNS == "" && f.DNSLink == "" {
			return false, errors.New("follow needs a channel to resolve the publication document on: set follow.url, " +
				"follow.ipns, follow.dnslink, or URL plus one name channel (spec 8, 8.1)")
		}
		if f.IPNS != "" && f.DNSLink != "" {
			return false, errors.New("follow.ipns and follow.dnslink are mutually exclusive name authorities; configure one")
		}
		if f.DNSLink != "" {
			if err := p2p.ValidateDNSLinkDomain(f.DNSLink); err != nil {
				return false, fmt.Errorf("follow.dnslink: %w", err)
			}
		}
		if (f.IPNS != "" || f.DNSLink != "") && !c.P2P.Host() {
			return false, errors.New("follow.ipns or follow.dnslink is set but there is no p2p block to resolve its IPNS name from: an IPNS name is " +
				"resolved on a DHT a host joins, and there is neither without one. Give p2p.listen an address (or " +
				"p2p.peers a peer, for a node that only dials out), or follow over follow.url alone")
		}
		if f.PubKey == "" && f.DNSLink == "" {
			return false, errors.New("follow.pubkey is required: a follower verifies the signature on every document it " +
				"adopts; only follow.dnslink may delegate that signer (spec 8, 11.3)")
		}
		if _, err := f.Key(); err != nil {
			return false, err
		}
		return false, nil
	}

	if f.Sources == nil {
		return false, errors.New("follow.sources must be a mapping, not null")
	}
	if f.SourceSet == nil {
		return false, errors.New("follow.source_set must be a mapping, not null")
	}
	if f.URL != "" || f.IPNS != "" || f.DNSLink != "" || f.PubKey != "" {
		return false, errors.New("follow.sources mode forbids the singular follow.url, follow.ipns, follow.dnslink, and follow.pubkey fields")
	}
	if f.ArchiveID == "" {
		return false, errors.New("follow.archive_id is required in source-set mode")
	}
	if _, err := f.ExpectedArchiveID(); err != nil {
		return false, err
	}
	if len(f.Sources) < 1 || len(f.Sources) > maxFollowSources {
		return false, fmt.Errorf("follow.sources has %d entries, must be in [1,%d]", len(f.Sources), maxFollowSources)
	}
	if f.SourceSet.Revision == 0 {
		return false, errors.New("follow.source_set.revision must be positive")
	}
	if f.MigrateLegacySource != "" {
		if _, exists := f.Sources[f.MigrateLegacySource]; !exists {
			return false, fmt.Errorf("follow.migrate_legacy_source %q does not name a configured source", f.MigrateLegacySource)
		}
	}

	normalized, err := f.normalizedSources()
	if err != nil {
		return false, err
	}
	coverage := make(map[string]int, len(f.Heads))
	hasNameChannel := false
	for _, source := range normalized {
		if source.IPNS != "" || source.DNSLink != "" {
			hasNameChannel = true
		}
		for _, head := range source.Heads {
			coverage[head]++
		}
	}
	for name, head := range f.Heads {
		if coverage[name] == 0 {
			return false, fmt.Errorf("follow.heads.%s is not allowed by any follow source", name)
		}
		if head.effectiveKind() == server.UnfinalizedMutable && coverage[name] != 1 {
			return false, fmt.Errorf("follow.heads.%s is unfinalized-mutable and must be allowed by exactly one source, got %d", name, coverage[name])
		}
	}
	if hasNameChannel && !c.P2P.Host() {
		return false, errors.New("a follow source uses ipns or dnslink but there is no p2p block to resolve its name")
	}
	expected, err := f.SourceSetDigest(c.Net)
	if err != nil {
		return false, err
	}
	if f.SourceSet.AcknowledgeDigest != expected {
		return false, fmt.Errorf("follow.source_set.acknowledge_digest is %q, expected %q; set it to %s after reviewing the normalized source roster",
			f.SourceSet.AcknowledgeDigest, expected, expected)
	}
	return true, nil
}

func (h HeadConfig) validate(name string) error {
	switch kind := h.effectiveKind(); kind {
	case server.FinalizedMonotonic:
		if h.HandoffHead != "" || h.MaxWindowSlots != 0 {
			return fmt.Errorf("heads.%s is finalized-monotonic but carries mutable handoff/window fields", name)
		}
	case server.UnfinalizedMutable:
		if h.HandoffHead == "" {
			return fmt.Errorf("heads.%s.handoff_head is required for unfinalized-mutable", name)
		}
		if h.HandoffHead == name {
			return fmt.Errorf("heads.%s.handoff_head cannot name itself", name)
		}
		if h.MaxWindowSlots == 0 || h.MaxWindowSlots > maxMutableWindowSlots {
			return fmt.Errorf("heads.%s.max_window_slots is %d, must be in [1,%d]", name, h.MaxWindowSlots, maxMutableWindowSlots)
		}
		if h.Pin.Mode != "full" {
			return fmt.Errorf("heads.%s is unfinalized-mutable and must use pin.mode full", name)
		}
	default:
		return fmt.Errorf("heads.%s.kind is %q, must be %q or %q", name, h.Kind,
			server.FinalizedMonotonic, server.UnfinalizedMutable)
	}
	if h.OriginSlot == nil {
		return fmt.Errorf("heads.%s.origin_slot is required (it defaults only on mainnet, to the Dencun slot %d)",
			name, schema.DencunMainnetSlot)
	}
	if *h.SegBits > 63 {
		return fmt.Errorf("heads.%s.seg_bits is %d, must be at most 63", name, *h.SegBits)
	}
	if *h.FanoutBits == 0 || *h.FanoutBits > 32 {
		return fmt.Errorf("heads.%s.fanout_bits is %d, must be in [1, 32]", name, *h.FanoutBits)
	}
	if !pinModes[h.Pin.Mode] {
		return fmt.Errorf("heads.%s.pin.mode is %q, must be one of full, window, none", name, h.Pin.Mode)
	}
	if h.Pin.Mode == "window" && h.Pin.Duration == 0 {
		return fmt.Errorf("heads.%s.pin.mode is window but no duration is set", name)
	}
	return nil
}

func (h HeadConfig) effectiveKind() server.HeadKind {
	if h.Kind == "" {
		return server.FinalizedMonotonic
	}
	return h.Kind
}

func (h FollowHeadConfig) effectiveKind() server.HeadKind {
	if h.Kind == "" {
		return server.FinalizedMonotonic
	}
	return h.Kind
}

// SpecMap renders beacon.spec_extra as the string map /eth/v1/config/spec
// serves. Every value in a beacon config map is a string, so scalars are
// rendered as written and anything structured is refused.
func (c *Config) SpecMap() (map[string]string, error) {
	out := make(map[string]string, len(c.Beacon.SpecExtra))
	for k, v := range c.Beacon.SpecExtra {
		s, err := specValue(v)
		if err != nil {
			return nil, fmt.Errorf("beacon.spec_extra.%s: %w", k, err)
		}
		out[k] = s
	}
	return out, nil
}

// specValue renders one spec_extra scalar.
func specValue(v any) (string, error) {
	switch v := v.(type) {
	case string:
		return v, nil
	case bool:
		return strconv.FormatBool(v), nil
	case int:
		return strconv.Itoa(v), nil
	case int64:
		return strconv.FormatInt(v, 10), nil
	case uint64:
		return strconv.FormatUint(v, 10), nil
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64), nil
	case nil:
		return "", errors.New("value is null; the beacon spec map has no nulls in it")
	default:
		return "", fmt.Errorf("value is %T; only scalars can be served, the beacon spec map is a map of strings", v)
	}
}

// AuthToken reads the bearer token of spec 7.3. Surrounding whitespace is
// trimmed: the file is one an operator wrote, and a trailing newline is not
// part of the secret.
//
// The systemd-credential form of server.auth_token_file is resolved here, at the
// read, not at config load: this is the only place bloard consumes the token, so
// deferring resolution to it is what lets a token-free offline subcommand load
// the same installed config with no CREDENTIALS_DIRECTORY set (see LoadConfig).
// serve() calls this before it binds anything, so a missing credential directory
// still fails startup closed.
func (c *Config) AuthToken() (string, error) {
	path, err := resolveTokenFile(c.Server.AuthTokenFile)
	if err != nil {
		return "", fmt.Errorf("bloard: server.auth_token_file %w", err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("bloard: reading server.auth_token_file: %w", err)
	}
	token := strings.TrimSpace(string(raw))
	if token == "" {
		return "", fmt.Errorf("bloard: server.auth_token_file %s is empty", path)
	}
	return token, nil
}

// SigningKey reads the publication signing key of spec 8, or nil if none is
// configured.
//
// The file is hex on one line: either a 32-byte seed or a full 64-byte ed25519
// private key. Hex because the public key and the signature it produces are hex
// in the document itself, and a key file an operator can eyeball against a
// published pubkey is worth more than a format with a header.
func (c *Config) SigningKey() (ed25519.PrivateKey, error) {
	if c.Publish.SigningKeyFile == "" {
		return nil, nil
	}
	raw, err := os.ReadFile(c.Publish.SigningKeyFile)
	if err != nil {
		return nil, fmt.Errorf("bloard: reading publish.signing_key_file: %w", err)
	}
	key, err := hex.DecodeString(strings.TrimSpace(string(raw)))
	if err != nil {
		return nil, fmt.Errorf("bloard: publish.signing_key_file %s is not hex: %w", c.Publish.SigningKeyFile, err)
	}
	switch len(key) {
	case ed25519.SeedSize:
		return ed25519.NewKeyFromSeed(key), nil
	case ed25519.PrivateKeySize:
		priv := ed25519.PrivateKey(key)
		// A 64-byte ed25519 private key is seed || public-key, and the type checks
		// nothing about the public half: re-derive it from the seed and require a
		// constant-time match, so an inconsistent expanded key is
		// refused here rather than producing a publication document whose signatures
		// do not verify against the public key it advertises. The 32-byte seed form
		// has no second half to disagree and is the preferred input; see the field doc.
		derived := ed25519.NewKeyFromSeed(priv.Seed())
		if subtle.ConstantTimeCompare(derived[ed25519.SeedSize:], priv[ed25519.SeedSize:]) != 1 {
			return nil, fmt.Errorf("bloard: publish.signing_key_file %s is a 64-byte expanded ed25519 key whose public "+
				"half does not derive from its seed; supply the 32-byte seed form, or a consistent expanded key",
				c.Publish.SigningKeyFile)
		}
		return priv, nil
	default:
		return nil, fmt.Errorf("bloard: publish.signing_key_file %s decodes to %d bytes, want an ed25519 seed (%d) or private key (%d)",
			c.Publish.SigningKeyFile, len(key), ed25519.SeedSize, ed25519.PrivateKeySize)
	}
}

// ArchiveID parses publish.archive_id, or returns nil when publication v3 is
// disabled. LoadConfig has already validated the same spelling; this method is
// also the programmatic-config boundary used by tests and embedders.
func (c *Config) ArchiveID() (*server.ArchiveID, error) {
	if c.Publish.ArchiveID == "" {
		return nil, nil
	}
	id, err := server.ParseArchiveID(c.Publish.ArchiveID)
	if err != nil {
		return nil, fmt.Errorf("bloard: publish.archive_id: %w", err)
	}
	return &id, nil
}
