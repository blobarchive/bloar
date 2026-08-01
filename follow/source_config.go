package follow

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"slices"
	"strconv"
	"strings"

	"github.com/ipfs/boxo/ipns"

	"github.com/blobarchive/bloar/p2p"
	"github.com/blobarchive/bloar/server"
)

const (
	sourceSetDigestSchema = "bloar.follow-source-set/v1"
	maxSourceSetSources   = 32
)

// SourceConfig is one independently authenticated publication source. PubKey
// authorizes this source only; AllowedHeads bounds which signed claims it may
// contribute to arbitration.
type SourceConfig struct {
	ID           string
	URL          string
	IPNS         string
	DNSLink      string
	PubKey       ed25519.PublicKey
	AllowedHeads []string
}

// SourceSetConfig is an explicitly acknowledged generation of publication
// authorities. Digest binds the normalized roster, network, and the
// Config.ExpectedArchiveID namespace; Revision supplies monotonic operator
// ordering for changes to that roster.
type SourceSetConfig struct {
	Revision uint64
	Digest   [sha256.Size]byte
	Sources  []SourceConfig

	// MigrateLegacySource explicitly assigns pre-source-set state to one source.
	// MigrateLegacyIPNS is the selected source's normalized direct IPNS name, or
	// empty when it has none. Activation requires it only when the store actually
	// contains the legacy unnamed IPNS sequence floor.
	MigrateLegacySource string
	MigrateLegacyIPNS   string
}

type sourceSetDigestSource struct {
	ID      string   `json:"id"`
	URL     string   `json:"url"`
	IPNS    string   `json:"ipns"`
	DNSLink string   `json:"dnslink"`
	PubKey  string   `json:"pubkey"`
	Heads   []string `json:"heads"`
}

type sourceSetDigestInput struct {
	Schema    string                  `json:"schema"`
	Net       string                  `json:"net"`
	ArchiveID string                  `json:"archive_id"`
	Sources   []sourceSetDigestSource `json:"sources"`
}

// SourceSetDigest computes the acknowledgement digest over the canonical
// source roster. It is exported so non-daemon callers can construct the same
// library configuration without reimplementing the wire-stable digest.
func SourceSetDigest(netName string, archiveID server.ArchiveID, sources []SourceConfig) ([sha256.Size]byte, error) {
	var zero [sha256.Size]byte
	if netName == "" {
		return zero, errors.New("follow: source-set digest network must not be empty")
	}
	if archiveID.IsZero() {
		return zero, errors.New("follow: source-set digest archive ID must not be zero")
	}
	normalized, err := normalizeSourceConfigs(sources, nil)
	if err != nil {
		return zero, err
	}
	digestSources := make([]sourceSetDigestSource, len(normalized))
	for i, source := range normalized {
		digestSources[i] = sourceSetDigestSource{
			ID: source.ID, URL: source.URL, IPNS: source.IPNS, DNSLink: source.DNSLink,
			PubKey: hex.EncodeToString(source.PubKey), Heads: slices.Clone(source.AllowedHeads),
		}
	}
	raw, err := json.Marshal(sourceSetDigestInput{
		Schema: sourceSetDigestSchema, Net: netName, ArchiveID: archiveID.String(), Sources: digestSources,
	})
	if err != nil {
		return zero, fmt.Errorf("follow: encoding normalized source set: %w", err)
	}
	return sha256.Sum256(raw), nil
}

// validateAndCloneSourceSet performs the read-only library trust-boundary
// checks. It returns a fully detached, canonical copy; durable activation is a
// separate transition and must happen only after all of New's other checks.
func validateAndCloneSourceSet(cfg Config) (*SourceSetConfig, error) {
	if cfg.SourceSet == nil {
		return nil, nil
	}
	if cfg.ExpectedArchiveID == nil {
		return nil, errors.New("follow: Config.ExpectedArchiveID is required with Config.SourceSet")
	}
	if cfg.ExpectedArchiveID.IsZero() {
		return nil, errors.New("follow: Config.ExpectedArchiveID must not be zero")
	}
	if cfg.URL != "" || cfg.IPNS != "" || cfg.DNSLink != "" || len(cfg.PubKey) != 0 {
		return nil, errors.New("follow: Config.SourceSet is mutually exclusive with Config.URL, Config.IPNS, Config.DNSLink, and Config.PubKey")
	}
	if cfg.SourceSet.Revision == 0 {
		return nil, errors.New("follow: Config.SourceSet.Revision must be positive")
	}
	if len(cfg.SourceSet.Sources) < 1 || len(cfg.SourceSet.Sources) > maxSourceSetSources {
		return nil, fmt.Errorf("follow: Config.SourceSet.Sources has %d entries, must be in [1,%d]", len(cfg.SourceSet.Sources), maxSourceSetSources)
	}

	normalized, err := normalizeSourceConfigs(cfg.SourceSet.Sources, func(head string) bool {
		_, exists := cfg.Heads[head]
		return exists
	})
	if err != nil {
		return nil, err
	}
	coverage := make(map[string]int, len(cfg.Heads))
	hasNameChannel := false
	byID := make(map[string]SourceConfig, len(normalized))
	for _, source := range normalized {
		byID[source.ID] = source
		hasNameChannel = hasNameChannel || source.IPNS != "" || source.DNSLink != ""
		for _, head := range source.AllowedHeads {
			coverage[head]++
		}
	}
	for head := range cfg.Heads {
		if coverage[head] == 0 {
			return nil, fmt.Errorf("follow: head %q is not allowed by any Config.SourceSet source", head)
		}
		if cfg.ExpectedKinds[head] == server.UnfinalizedMutable && coverage[head] != 1 {
			return nil, fmt.Errorf("follow: unfinalized-mutable head %q must be allowed by exactly one Config.SourceSet source, got %d", head, coverage[head])
		}
	}
	if hasNameChannel && cfg.Routing == nil {
		return nil, errors.New("follow: a Config.SourceSet source uses IPNS or DNSLink but Config.Routing is nil")
	}
	if hasNameChannel && cfg.Retention != nil && cfg.DocumentBlock == nil {
		return nil, errors.New("follow: Config.DocumentBlock is required for source-set IPNS or DNSLink with external retention")
	}

	migrationIPNS := ""
	if cfg.SourceSet.MigrateLegacySource != "" {
		source, ok := byID[cfg.SourceSet.MigrateLegacySource]
		if !ok {
			return nil, fmt.Errorf("follow: Config.SourceSet.MigrateLegacySource %q does not name a configured source", cfg.SourceSet.MigrateLegacySource)
		}
		migrationIPNS = source.IPNS
		if cfg.SourceSet.MigrateLegacyIPNS != migrationIPNS {
			return nil, fmt.Errorf("follow: Config.SourceSet.MigrateLegacyIPNS does not match source %q's direct IPNS name", source.ID)
		}
	} else if cfg.SourceSet.MigrateLegacyIPNS != "" {
		return nil, errors.New("follow: Config.SourceSet.MigrateLegacyIPNS requires Config.SourceSet.MigrateLegacySource")
	}

	digest, err := SourceSetDigest(cfg.Net, *cfg.ExpectedArchiveID, normalized)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(cfg.SourceSet.Digest[:], digest[:]) {
		return nil, fmt.Errorf("follow: Config.SourceSet.Digest does not acknowledge the normalized source roster: got %x, want %x", cfg.SourceSet.Digest, digest)
	}
	return &SourceSetConfig{
		Revision: cfg.SourceSet.Revision, Digest: digest, Sources: normalized,
		MigrateLegacySource: cfg.SourceSet.MigrateLegacySource, MigrateLegacyIPNS: migrationIPNS,
	}, nil
}

func normalizeSourceConfigs(sources []SourceConfig, configuredHead func(string) bool) ([]SourceConfig, error) {
	if len(sources) < 1 || len(sources) > maxSourceSetSources {
		return nil, fmt.Errorf("follow: source set has %d sources, must be in [1,%d]", len(sources), maxSourceSetSources)
	}
	result := make([]SourceConfig, len(sources))
	copy(result, sources)
	slices.SortFunc(result, func(a, b SourceConfig) int { return strings.Compare(a.ID, b.ID) })

	seenIDs := make(map[string]struct{}, len(result))
	seenURLs := make(map[string]string, len(result))
	seenNames := make(map[string]string, len(result))
	seenSigners := make(map[string]string, len(result))
	for i := range result {
		source := &result[i]
		if err := validateSourceID(source.ID); err != nil {
			return nil, err
		}
		if _, exists := seenIDs[source.ID]; exists {
			return nil, fmt.Errorf("follow: duplicate source ID %q", source.ID)
		}
		seenIDs[source.ID] = struct{}{}
		if source.URL == "" && source.IPNS == "" && source.DNSLink == "" {
			return nil, fmt.Errorf("follow: source %q needs at least one channel: URL, IPNS, or DNSLink", source.ID)
		}
		if source.IPNS != "" && source.DNSLink != "" {
			return nil, fmt.Errorf("follow: source %q IPNS and DNSLink are mutually exclusive", source.ID)
		}
		if source.URL != "" {
			normalized, err := normalizeSourceURL(source.URL)
			if err != nil {
				return nil, fmt.Errorf("follow: source %q URL: %w", source.ID, err)
			}
			if prior, exists := seenURLs[normalized]; exists {
				return nil, fmt.Errorf("follow: source %q URL duplicates source %q after normalization", source.ID, prior)
			}
			seenURLs[normalized] = source.ID
			source.URL = normalized
		}
		if source.IPNS != "" {
			name, err := ipns.NameFromString(source.IPNS)
			if err != nil {
				return nil, fmt.Errorf("follow: source %q IPNS %q is invalid: %w", source.ID, source.IPNS, err)
			}
			source.IPNS = name.String()
			if prior, exists := seenNames[source.IPNS]; exists {
				return nil, fmt.Errorf("follow: source %q IPNS duplicates the name used by source %q", source.ID, prior)
			}
			seenNames[source.IPNS] = source.ID
		}
		if source.DNSLink != "" {
			if err := p2p.ValidateDNSLinkDomain(source.DNSLink); err != nil {
				return nil, fmt.Errorf("follow: source %q DNSLink: %w", source.ID, err)
			}
			source.DNSLink = strings.TrimSuffix(strings.ToLower(source.DNSLink), ".")
			if prior, exists := seenNames[source.DNSLink]; exists {
				return nil, fmt.Errorf("follow: source %q DNSLink duplicates the name used by source %q", source.ID, prior)
			}
			seenNames[source.DNSLink] = source.ID
		}
		if len(source.PubKey) != ed25519.PublicKeySize {
			return nil, fmt.Errorf("follow: source %q PubKey is %d bytes, want %d", source.ID, len(source.PubKey), ed25519.PublicKeySize)
		}
		if bytes.Equal(source.PubKey, make([]byte, ed25519.PublicKeySize)) {
			return nil, fmt.Errorf("follow: source %q PubKey must not be all-zero", source.ID)
		}
		source.PubKey = slices.Clone(source.PubKey)
		key := string(source.PubKey)
		if prior, exists := seenSigners[key]; exists {
			return nil, fmt.Errorf("follow: source %q PubKey duplicates source %q", source.ID, prior)
		}
		seenSigners[key] = source.ID

		if len(source.AllowedHeads) == 0 {
			return nil, fmt.Errorf("follow: source %q AllowedHeads must not be empty", source.ID)
		}
		source.AllowedHeads = slices.Clone(source.AllowedHeads)
		slices.Sort(source.AllowedHeads)
		for j, head := range source.AllowedHeads {
			if j > 0 && head == source.AllowedHeads[j-1] {
				return nil, fmt.Errorf("follow: source %q AllowedHeads contains duplicate %q", source.ID, head)
			}
			if configuredHead != nil {
				if exists := configuredHead(head); !exists {
					return nil, fmt.Errorf("follow: source %q allows unknown head %q", source.ID, head)
				}
			}
		}
	}
	return result, nil
}

func normalizeSourceURL(raw string) (string, error) {
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
