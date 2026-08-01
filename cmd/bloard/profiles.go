package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/cockroachdb/pebble/v2"
	"gopkg.in/yaml.v3"

	"github.com/blobarchive/bloar/follow"
	"github.com/blobarchive/bloar/p2p"
	"github.com/blobarchive/bloar/server"
)

const (
	profileBundleSchema = "bloar.follow-profile-bundle/v1"
	followProfileSchema = "bloar.follow-profile/v1"
	maxProfileFileBytes = 1 << 20
)

// embeddedProfileBundle is deliberately immutable source text. These built-in
// follow profiles are reviewed production policy, not a remotely updated
// registry: changing any record changes its content digest and is subject to
// the persisted profile upgrade checks below. Names are opaque lookup keys;
// callers must never infer network, head, finality, or writer identity by
// splitting a name.
const embeddedProfileBundle = `
schema: bloar.follow-profile-bundle/v1
profiles:
  - schema: bloar.follow-profile/v1
    name: ethereum-mainnet-all-a
    version: 1
    digest: sha256:9c175ccedc95e9a0e910a128eadd4c5bd767ce980e1bbe495d65b76ad83d978e
    aliases: [ethereum-mainnet-all-a]
    provenance:
      source: blobarchive.net production authority
      revision: initial-production-catalog
      note: Ethereum mainnet finalized ALL head, writer a
    network:
      name: mainnet
      beacon:
        genesis_time: 1606824023
        seconds_per_slot: 12
        genesis_validators_root: "0x4b363db94e286120d76eb905340fdd4e54bfe9f06bf33ff6cf5ad27f511bfe95"
        genesis_fork_version: "0x00000000"
        spec_extra:
          DEPOSIT_CHAIN_ID: "1"
          DEPOSIT_NETWORK_ID: "1"
    trust:
      mode: dnslink+signer-pin
      dnslink: ethereum-mainnet-all-a.blobarchive.net
      pubkey: "6698f6c8767529ffb725ce5201a86602106cc87ed7c9129a649428ca0ea6d7b5"
    verify: full
    heads:
      all:
        pin: {mode: full}

  - schema: bloar.follow-profile/v1
    name: ethereum-mainnet-arb1-a
    version: 1
    digest: sha256:5834dba26a6c6159c8393dd494f926393f5db9d4e3aa44f0d9c596556a285fa5
    aliases: [ethereum-mainnet-arb1-a]
    provenance:
      source: blobarchive.net production authority
      revision: initial-production-catalog
      note: Ethereum mainnet finalized Arbitrum One head, writer a
    network:
      name: mainnet
      beacon:
        genesis_time: 1606824023
        seconds_per_slot: 12
        genesis_validators_root: "0x4b363db94e286120d76eb905340fdd4e54bfe9f06bf33ff6cf5ad27f511bfe95"
        genesis_fork_version: "0x00000000"
        spec_extra:
          DEPOSIT_CHAIN_ID: "1"
          DEPOSIT_NETWORK_ID: "1"
    trust:
      mode: dnslink+signer-pin
      dnslink: ethereum-mainnet-arb1-a.blobarchive.net
      pubkey: "6698f6c8767529ffb725ce5201a86602106cc87ed7c9129a649428ca0ea6d7b5"
    verify: full
    heads:
      arbitrum-one:
        pin: {mode: full}

  - schema: bloar.follow-profile/v1
    name: ethereum-mainnet-robinhood-a
    version: 1
    digest: sha256:ff45e33a71c9b900a02a3380c6c3cbf80750f96500eb7dedc42971d2c165260e
    aliases: [ethereum-mainnet-robinhood-a]
    provenance:
      source: blobarchive.net production authority
      revision: initial-production-catalog
      note: Ethereum mainnet finalized Robinhood head, writer a
    network:
      name: mainnet
      beacon:
        genesis_time: 1606824023
        seconds_per_slot: 12
        genesis_validators_root: "0x4b363db94e286120d76eb905340fdd4e54bfe9f06bf33ff6cf5ad27f511bfe95"
        genesis_fork_version: "0x00000000"
        spec_extra:
          DEPOSIT_CHAIN_ID: "1"
          DEPOSIT_NETWORK_ID: "1"
    trust:
      mode: dnslink+signer-pin
      dnslink: ethereum-mainnet-robinhood-a.blobarchive.net
      pubkey: "6698f6c8767529ffb725ce5201a86602106cc87ed7c9129a649428ca0ea6d7b5"
    verify: full
    heads:
      robinhood:
        pin: {mode: full}

  - schema: bloar.follow-profile/v1
    name: ethereum-mainnet-base-a
    version: 1
    digest: sha256:c0d4cdf272542a027ed054f6c12edd445d4ce26d69aa0640043712410ef51958
    aliases: [ethereum-mainnet-base-a]
    provenance:
      source: blobarchive.net production authority
      revision: initial-production-catalog
      note: Ethereum mainnet finalized Base head, writer a
    network:
      name: mainnet
      beacon:
        genesis_time: 1606824023
        seconds_per_slot: 12
        genesis_validators_root: "0x4b363db94e286120d76eb905340fdd4e54bfe9f06bf33ff6cf5ad27f511bfe95"
        genesis_fork_version: "0x00000000"
        spec_extra:
          DEPOSIT_CHAIN_ID: "1"
          DEPOSIT_NETWORK_ID: "1"
    trust:
      mode: dnslink+signer-pin
      dnslink: ethereum-mainnet-base-a.blobarchive.net
      pubkey: "6698f6c8767529ffb725ce5201a86602106cc87ed7c9129a649428ca0ea6d7b5"
    verify: full
    heads:
      base:
        pin: {mode: full}

  - schema: bloar.follow-profile/v1
    name: ethereum-mainnet-arb1-live-a
    version: 1
    digest: sha256:4b16e6ed186544e9ddd4136cebe9b93d42a36ca4280fd9fcf205610476be4d43
    aliases: [ethereum-mainnet-arb1-live-a]
    provenance:
      source: blobarchive.net production authority
      revision: initial-production-catalog
      note: Ethereum mainnet finalized Arbitrum One plus bounded live view, writer a
    network:
      name: mainnet
      beacon:
        genesis_time: 1606824023
        seconds_per_slot: 12
        genesis_validators_root: "0x4b363db94e286120d76eb905340fdd4e54bfe9f06bf33ff6cf5ad27f511bfe95"
        genesis_fork_version: "0x00000000"
        spec_extra:
          DEPOSIT_CHAIN_ID: "1"
          DEPOSIT_NETWORK_ID: "1"
    trust:
      mode: dnslink+signer-pin
      dnslink: ethereum-mainnet-arb1-live-a.blobarchive.net
      pubkey: "6698f6c8767529ffb725ce5201a86602106cc87ed7c9129a649428ca0ea6d7b5"
    verify: full
    heads:
      arbitrum-one:
        pin: {mode: full}
      unfinalized:
        kind: unfinalized-mutable
        handoff_head: all
        max_window_slots: 128
        pin: {mode: full}
    live_heads:
      live:
        finalized_head: arbitrum-one
        unfinalized_head: unfinalized
        require_versioned_hashes: true

  - schema: bloar.follow-profile/v1
    name: ethereum-mainnet-robinhood-live-a
    version: 1
    digest: sha256:dfc42695a695567b9023b845c31ff8da072104a4ecc3dce8e4db9b02a7249d43
    aliases: [ethereum-mainnet-robinhood-live-a]
    provenance:
      source: blobarchive.net production authority
      revision: initial-production-catalog
      note: Ethereum mainnet finalized Robinhood plus bounded live view, writer a
    network:
      name: mainnet
      beacon:
        genesis_time: 1606824023
        seconds_per_slot: 12
        genesis_validators_root: "0x4b363db94e286120d76eb905340fdd4e54bfe9f06bf33ff6cf5ad27f511bfe95"
        genesis_fork_version: "0x00000000"
        spec_extra:
          DEPOSIT_CHAIN_ID: "1"
          DEPOSIT_NETWORK_ID: "1"
    trust:
      mode: dnslink+signer-pin
      dnslink: ethereum-mainnet-robinhood-live-a.blobarchive.net
      pubkey: "6698f6c8767529ffb725ce5201a86602106cc87ed7c9129a649428ca0ea6d7b5"
    verify: full
    heads:
      robinhood:
        pin: {mode: full}
      unfinalized:
        kind: unfinalized-mutable
        handoff_head: all
        max_window_slots: 128
        pin: {mode: full}
    live_heads:
      live:
        finalized_head: robinhood
        unfinalized_head: unfinalized
        require_versioned_hashes: true

  - schema: bloar.follow-profile/v1
    name: ethereum-mainnet-base-live-a
    version: 1
    digest: sha256:28bcc485b823fe9144f55980960da43fbad704de1e64d50fdf001cccedca081f
    aliases: [ethereum-mainnet-base-live-a]
    provenance:
      source: blobarchive.net production authority
      revision: initial-production-catalog
      note: Ethereum mainnet finalized Base plus bounded live view, writer a
    network:
      name: mainnet
      beacon:
        genesis_time: 1606824023
        seconds_per_slot: 12
        genesis_validators_root: "0x4b363db94e286120d76eb905340fdd4e54bfe9f06bf33ff6cf5ad27f511bfe95"
        genesis_fork_version: "0x00000000"
        spec_extra:
          DEPOSIT_CHAIN_ID: "1"
          DEPOSIT_NETWORK_ID: "1"
    trust:
      mode: dnslink+signer-pin
      dnslink: ethereum-mainnet-base-live-a.blobarchive.net
      pubkey: "6698f6c8767529ffb725ce5201a86602106cc87ed7c9129a649428ca0ea6d7b5"
    verify: full
    heads:
      base:
        pin: {mode: full}
      unfinalized:
        kind: unfinalized-mutable
        handoff_head: all
        max_window_slots: 128
        pin: {mode: full}
    live_heads:
      live:
        finalized_head: base
        unfinalized_head: unfinalized
        require_versioned_hashes: true

  - schema: bloar.follow-profile/v1
    name: ethereum-mainnet-all-live-a
    version: 1
    digest: sha256:3c2e5bbcf887f674c0d28cb4fcffcffa72156bd8a3437a3baa85dead3239a59f
    aliases: [ethereum-mainnet-all-live-a]
    provenance:
      source: blobarchive.net production authority
      revision: initial-production-catalog
      note: Ethereum mainnet finalized ALL plus bounded live view, writer a
    network:
      name: mainnet
      beacon:
        genesis_time: 1606824023
        seconds_per_slot: 12
        genesis_validators_root: "0x4b363db94e286120d76eb905340fdd4e54bfe9f06bf33ff6cf5ad27f511bfe95"
        genesis_fork_version: "0x00000000"
        spec_extra:
          DEPOSIT_CHAIN_ID: "1"
          DEPOSIT_NETWORK_ID: "1"
    trust:
      mode: dnslink+signer-pin
      dnslink: ethereum-mainnet-all-live-a.blobarchive.net
      pubkey: "6698f6c8767529ffb725ce5201a86602106cc87ed7c9129a649428ca0ea6d7b5"
    verify: full
    heads:
      all:
        pin: {mode: full}
      unfinalized:
        kind: unfinalized-mutable
        handoff_head: all
        max_window_slots: 128
        pin: {mode: full}
    live_heads:
      live:
        finalized_head: all
        unfinalized_head: unfinalized
`

var profileNamePattern = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9-]{0,62}[a-z0-9])?$`)

// ProfileSelection is the immutable profile identity attached to an expanded
// config. Digest covers the complete profile record, including aliases,
// provenance, trust, network data, heads, and retention. Acknowledgement is an
// input control and is intentionally excluded from inspect/log output.
type ProfileSelection struct {
	Name       string            `yaml:"name" json:"name"`
	Schema     string            `yaml:"schema" json:"schema"`
	Version    uint64            `yaml:"version" json:"version"`
	Digest     string            `yaml:"digest" json:"digest"`
	Source     string            `yaml:"source" json:"source"`
	Provenance profileProvenance `yaml:"provenance" json:"provenance"`

	acknowledgeDigest string
}

type profileControl struct {
	File              string    `yaml:"file"`
	AcknowledgeDigest string    `yaml:"acknowledge_digest"`
	Overrides         yaml.Node `yaml:"overrides"`
}

type profileBundle struct {
	Schema   string          `yaml:"schema" json:"schema"`
	Profiles []followProfile `yaml:"profiles" json:"profiles"`
}

type followProfile struct {
	Schema     string                     `yaml:"schema" json:"schema"`
	Name       string                     `yaml:"name" json:"name"`
	Version    uint64                     `yaml:"version" json:"version"`
	Digest     string                     `yaml:"digest,omitempty" json:"-"`
	Aliases    []string                   `yaml:"aliases,omitempty" json:"aliases,omitempty"`
	Provenance profileProvenance          `yaml:"provenance" json:"provenance"`
	Network    profileNetwork             `yaml:"network" json:"network"`
	Trust      profileTrust               `yaml:"trust" json:"trust"`
	Verify     string                     `yaml:"verify,omitempty" json:"verify,omitempty"`
	Heads      map[string]profileHead     `yaml:"heads" json:"heads"`
	LiveHeads  map[string]profileLiveHead `yaml:"live_heads,omitempty" json:"live_heads,omitempty"`
}

type profileProvenance struct {
	Source   string `yaml:"source" json:"source"`
	Revision string `yaml:"revision,omitempty" json:"revision,omitempty"`
	Note     string `yaml:"note,omitempty" json:"note,omitempty"`
}

type profileNetwork struct {
	Name   string        `yaml:"name" json:"name"`
	Beacon profileBeacon `yaml:"beacon" json:"beacon"`
}

type profileBeacon struct {
	GenesisTime           uint64            `yaml:"genesis_time" json:"genesis_time"`
	SecondsPerSlot        uint64            `yaml:"seconds_per_slot" json:"seconds_per_slot"`
	GenesisValidatorsRoot string            `yaml:"genesis_validators_root" json:"genesis_validators_root"`
	GenesisForkVersion    string            `yaml:"genesis_fork_version" json:"genesis_fork_version"`
	SpecExtra             map[string]string `yaml:"spec_extra,omitempty" json:"spec_extra,omitempty"`
}

type profileTrust struct {
	Mode    string `yaml:"mode" json:"mode"`
	DNSLink string `yaml:"dnslink" json:"dnslink"`
	URL     string `yaml:"url,omitempty" json:"url,omitempty"`
	PubKey  string `yaml:"pubkey,omitempty" json:"pubkey,omitempty"`
}

type profileHead struct {
	Kind           server.HeadKind `yaml:"kind,omitempty" json:"kind,omitempty"`
	HandoffHead    string          `yaml:"handoff_head,omitempty" json:"handoff_head,omitempty"`
	MaxWindowSlots uint64          `yaml:"max_window_slots,omitempty" json:"max_window_slots,omitempty"`
	Pin            PinConfig       `yaml:"pin" json:"pin"`
}

type profileLiveHead struct {
	FinalizedHead          string `yaml:"finalized_head" json:"finalized_head"`
	UnfinalizedHead        string `yaml:"unfinalized_head" json:"unfinalized_head"`
	RequireVersionedHashes bool   `yaml:"require_versioned_hashes,omitempty" json:"require_versioned_hashes,omitempty"`
}

type expandedProfile struct {
	Net       string                    `yaml:"net"`
	Beacon    profileBeacon             `yaml:"beacon"`
	Follow    expandedProfileFollow     `yaml:"follow"`
	LiveHeads map[string]LiveHeadConfig `yaml:"live_heads,omitempty"`
}

type expandedProfileFollow struct {
	URL     string                      `yaml:"url,omitempty"`
	DNSLink string                      `yaml:"dnslink"`
	PubKey  string                      `yaml:"pubkey,omitempty"`
	Verify  string                      `yaml:"verify,omitempty"`
	Heads   map[string]FollowHeadConfig `yaml:"heads"`
}

type catalogProfile struct {
	profile followProfile
	digest  string
	source  string
}

// expandProfileConfig performs the only non-canonical YAML transformation in
// bloard. A mapping-valued follow block is returned byte-for-byte so existing
// configs retain their exact decoder behaviour. A string-valued follow block
// is resolved and expanded at the syntax-tree layer before KnownFields runs.
func expandProfileConfig(configPath string, data []byte) ([]byte, *ProfileSelection, error) {
	doc, root, err := decodeYAMLDocument(data, configPath)
	if err != nil {
		return nil, nil, err
	}
	followKey, followNode := mappingEntry(root, "follow")
	_, controlNode := mappingEntry(root, "profile")

	if followNode == nil || followNode.Kind != yaml.ScalarNode {
		if controlNode != nil {
			return nil, nil, errors.New("profile is set but follow is not a scalar profile name")
		}
		return data, nil, nil
	}
	if followNode.Tag != "!!str" || strings.TrimSpace(followNode.Value) == "" || followNode.Value != strings.TrimSpace(followNode.Value) {
		return nil, nil, errors.New("follow profile selector must be a non-empty string without surrounding whitespace")
	}
	if err := validateProfileSyntax(doc); err != nil {
		return nil, nil, fmt.Errorf("profile config syntax: %w", err)
	}

	control, err := decodeProfileControl(controlNode)
	if err != nil {
		return nil, nil, err
	}
	catalog, err := loadProfileCatalog(configPath, control.File)
	if err != nil {
		return nil, nil, err
	}
	selected, ok := catalog[followNode.Value]
	if !ok {
		return nil, nil, fmt.Errorf("unknown follow profile %q", followNode.Value)
	}
	if control.AcknowledgeDigest != "" {
		if err := validateDigest(control.AcknowledgeDigest); err != nil {
			return nil, nil, fmt.Errorf("profile.acknowledge_digest: %w", err)
		}
		if control.AcknowledgeDigest != selected.digest {
			return nil, nil, fmt.Errorf("profile.acknowledge_digest is %q, selected profile digest is %q", control.AcknowledgeDigest, selected.digest)
		}
	}

	expansionNode, err := profileExpansionNode(selected.profile)
	if err != nil {
		return nil, nil, err
	}
	if control.Overrides.Kind != 0 {
		if control.Overrides.Kind != yaml.MappingNode {
			return nil, nil, errors.New("profile.overrides must be a mapping of exact profile-derived fields")
		}
		if err := applyProfileOverrides(expansionNode, &control.Overrides, nil); err != nil {
			return nil, nil, fmt.Errorf("profile.overrides: %w", err)
		}
	}

	for _, key := range []string{"net", "beacon", "live_heads"} {
		keyNode, valueNode := mappingEntry(expansionNode, key)
		if valueNode == nil {
			continue
		}
		if _, existing := mappingEntry(root, key); existing != nil {
			return nil, nil, fmt.Errorf("profile-derived field %q is also explicit at the config root; move an intentional exact override under profile.overrides.%s", key, key)
		}
		root.Content = append(root.Content, cloneYAMLNode(keyNode), cloneYAMLNode(valueNode))
	}
	_, expandedFollow := mappingEntry(expansionNode, "follow")
	*followNode = *cloneYAMLNode(expandedFollow)
	_ = followKey
	removeMappingEntry(root, "profile")

	var out bytes.Buffer
	enc := yaml.NewEncoder(&out)
	enc.SetIndent(2)
	if err := enc.Encode(doc.Content[0]); err != nil {
		return nil, nil, fmt.Errorf("encoding expanded profile config: %w", err)
	}
	if err := enc.Close(); err != nil {
		return nil, nil, fmt.Errorf("encoding expanded profile config: %w", err)
	}
	selection := &ProfileSelection{
		Name:              selected.profile.Name,
		Schema:            selected.profile.Schema,
		Version:           selected.profile.Version,
		Digest:            selected.digest,
		Source:            selected.source,
		Provenance:        selected.profile.Provenance,
		acknowledgeDigest: control.AcknowledgeDigest,
	}
	return out.Bytes(), selection, nil
}

func decodeYAMLDocument(data []byte, source string) (*yaml.Node, *yaml.Node, error) {
	dec := yaml.NewDecoder(bytes.NewReader(data))
	var doc yaml.Node
	if err := dec.Decode(&doc); err != nil {
		return nil, nil, fmt.Errorf("parsing YAML %s: %w", source, err)
	}
	if len(doc.Content) == 0 || doc.Content[0].Kind != yaml.MappingNode {
		return nil, nil, fmt.Errorf("parsing YAML %s: document root must be a mapping", source)
	}
	var extra yaml.Node
	if err := dec.Decode(&extra); err != io.EOF {
		if err == nil {
			return nil, nil, fmt.Errorf("parsing YAML %s: multiple documents are not allowed", source)
		}
		return nil, nil, fmt.Errorf("parsing YAML %s: %w", source, err)
	}
	return &doc, doc.Content[0], nil
}

func decodeProfileControl(node *yaml.Node) (profileControl, error) {
	if node == nil {
		return profileControl{}, nil
	}
	if node.Kind != yaml.MappingNode {
		return profileControl{}, errors.New("profile must be a mapping")
	}
	var encoded bytes.Buffer
	if err := yaml.NewEncoder(&encoded).Encode(node); err != nil {
		return profileControl{}, err
	}
	var control profileControl
	dec := yaml.NewDecoder(&encoded)
	dec.KnownFields(true)
	if err := dec.Decode(&control); err != nil {
		return profileControl{}, fmt.Errorf("profile: %w", err)
	}
	if control.File != strings.TrimSpace(control.File) {
		return profileControl{}, errors.New("profile.file must not have surrounding whitespace")
	}
	return control, nil
}

func loadProfileCatalog(configPath, extension string) (map[string]catalogProfile, error) {
	return loadProfileCatalogWithBuiltins(configPath, extension, []byte(embeddedProfileBundle))
}

func loadProfileCatalogWithBuiltins(configPath, extension string, builtinData []byte) (map[string]catalogProfile, error) {
	builtins, err := decodeProfileBundle(builtinData, "built-in profiles")
	if err != nil {
		return nil, fmt.Errorf("invalid built-in profile bundle: %w", err)
	}
	catalog := make(map[string]catalogProfile)
	if err := addProfiles(catalog, builtins, "built-in"); err != nil {
		return nil, err
	}
	if extension == "" {
		return catalog, nil
	}
	path := extension
	if !filepath.IsAbs(path) {
		path = filepath.Join(filepath.Dir(configPath), path)
	}
	data, err := readBoundedProfileFile(path)
	if err != nil {
		return nil, err
	}
	profiles, err := decodeProfileBundle(data, path)
	if err != nil {
		return nil, err
	}
	if err := addProfiles(catalog, profiles, "local:"+filepath.Clean(path)); err != nil {
		return nil, err
	}
	return catalog, nil
}

func readBoundedProfileFile(path string) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("opening local profile extension %s: %w", path, err)
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, maxProfileFileBytes+1))
	if err != nil {
		return nil, fmt.Errorf("reading local profile extension %s: %w", path, err)
	}
	if len(data) > maxProfileFileBytes {
		return nil, fmt.Errorf("local profile extension %s exceeds the %d-byte maximum", path, maxProfileFileBytes)
	}
	return data, nil
}

func decodeProfileBundle(data []byte, source string) ([]followProfile, error) {
	doc, _, err := decodeYAMLDocument(data, source)
	if err != nil {
		return nil, err
	}
	if err := validateProfileSyntax(doc); err != nil {
		return nil, fmt.Errorf("profile bundle %s: %w", source, err)
	}
	var bundle profileBundle
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(&bundle); err != nil {
		return nil, fmt.Errorf("profile bundle %s: %w", source, err)
	}
	if bundle.Schema != profileBundleSchema {
		return nil, fmt.Errorf("profile bundle %s has schema %q, want %q", source, bundle.Schema, profileBundleSchema)
	}
	for i := range bundle.Profiles {
		if err := bundle.Profiles[i].validate(); err != nil {
			return nil, fmt.Errorf("profile bundle %s profile %d: %w", source, i, err)
		}
	}
	return bundle.Profiles, nil
}

func addProfiles(catalog map[string]catalogProfile, profiles []followProfile, source string) error {
	for _, profile := range profiles {
		digest, err := profile.contentDigest()
		if err != nil {
			return err
		}
		if profile.Digest != "" && profile.Digest != digest {
			return fmt.Errorf("profile %q declares digest %q, computed %q", profile.Name, profile.Digest, digest)
		}
		entry := catalogProfile{profile: profile, digest: digest, source: source}
		// The canonical token always carries the version, so v1 remains
		// selectable after v2 is added. A human-friendly unversioned shorthand is
		// an explicit alias and, like every alias, may belong to only one version.
		canonical := fmt.Sprintf("%s@v%d", profile.Name, profile.Version)
		for _, token := range append([]string{canonical}, profile.Aliases...) {
			if prior, exists := catalog[token]; exists {
				return fmt.Errorf("profile name or alias %q collides between %s profile %q and %s profile %q", token, prior.source, prior.profile.Name, source, profile.Name)
			}
			catalog[token] = entry
		}
	}
	return nil
}

func (p followProfile) validate() error {
	if p.Schema != followProfileSchema {
		return fmt.Errorf("profile %q has schema %q, want %q", p.Name, p.Schema, followProfileSchema)
	}
	if err := validateProfileName("name", p.Name); err != nil {
		return err
	}
	if p.Version == 0 {
		return fmt.Errorf("profile %q version must be positive", p.Name)
	}
	for _, alias := range p.Aliases {
		if err := validateProfileName("alias", alias); err != nil {
			return fmt.Errorf("profile %q: %w", p.Name, err)
		}
	}
	if strings.TrimSpace(p.Provenance.Source) == "" {
		return fmt.Errorf("profile %q provenance.source is required", p.Name)
	}
	if err := validateProfileName("network.name", p.Network.Name); err != nil {
		return fmt.Errorf("profile %q: %w", p.Name, err)
	}
	if err := p.Network.Beacon.validate(); err != nil {
		return fmt.Errorf("profile %q network.beacon: %w", p.Name, err)
	}
	if err := p.Trust.validate(); err != nil {
		return fmt.Errorf("profile %q trust: %w", p.Name, err)
	}
	if p.Verify != "" {
		if _, err := follow.ParseVerify(p.Verify); err != nil {
			return fmt.Errorf("profile %q verify: %w", p.Name, err)
		}
	}
	if len(p.Heads) == 0 {
		return fmt.Errorf("profile %q must follow at least one head", p.Name)
	}
	for name, head := range p.Heads {
		if err := validateProfileName("head name", name); err != nil {
			return fmt.Errorf("profile %q: %w", p.Name, err)
		}
		if err := validateProfilePin(head.Pin); err != nil {
			return fmt.Errorf("profile %q head %q: %w", p.Name, name, err)
		}
		switch head.Kind {
		case "", server.FinalizedMonotonic:
			if head.HandoffHead != "" || head.MaxWindowSlots != 0 {
				return fmt.Errorf("profile %q head %q is finalized but carries mutable handoff/window fields", p.Name, name)
			}
		case server.UnfinalizedMutable:
			if head.HandoffHead == "" || head.HandoffHead == name {
				return fmt.Errorf("profile %q head %q requires a distinct handoff_head", p.Name, name)
			}
			if head.MaxWindowSlots == 0 || head.MaxWindowSlots > maxMutableWindowSlots {
				return fmt.Errorf("profile %q head %q max_window_slots must be in [1,%d]", p.Name, name, maxMutableWindowSlots)
			}
			if head.Pin.Mode != "full" {
				return fmt.Errorf("profile %q head %q is mutable and requires pin mode full", p.Name, name)
			}
		default:
			return fmt.Errorf("profile %q head %q kind is %q, must be %q or %q", p.Name, name, head.Kind,
				server.FinalizedMonotonic, server.UnfinalizedMutable)
		}
	}
	for name, view := range p.LiveHeads {
		if err := validateProfileName("live head name", name); err != nil {
			return fmt.Errorf("profile %q: %w", p.Name, err)
		}
		if view.FinalizedHead == "" || view.UnfinalizedHead == "" || view.FinalizedHead == view.UnfinalizedHead {
			return fmt.Errorf("profile %q live head %q requires distinct finalized_head and unfinalized_head", p.Name, name)
		}
		if _, ok := p.Heads[view.FinalizedHead]; !ok {
			return fmt.Errorf("profile %q live head %q finalized_head %q is not followed", p.Name, name, view.FinalizedHead)
		}
		mutable, ok := p.Heads[view.UnfinalizedHead]
		if !ok || mutable.Kind != server.UnfinalizedMutable {
			return fmt.Errorf("profile %q live head %q unfinalized_head %q is not a followed mutable head", p.Name, name, view.UnfinalizedHead)
		}
	}
	if p.Digest != "" {
		if err := validateDigest(p.Digest); err != nil {
			return fmt.Errorf("profile %q digest: %w", p.Name, err)
		}
	}
	return nil
}

func validateProfileName(field, value string) error {
	if !profileNamePattern.MatchString(value) {
		return fmt.Errorf("%s %q must be 1-64 lower-case letters, digits, or interior hyphens", field, value)
	}
	return nil
}

func (b profileBeacon) validate() error {
	if b.GenesisTime == 0 {
		return errors.New("genesis_time must be positive")
	}
	if b.SecondsPerSlot == 0 {
		return errors.New("seconds_per_slot must be positive")
	}
	if err := validateFixedHex(b.GenesisValidatorsRoot, 32); err != nil {
		return fmt.Errorf("genesis_validators_root: %w", err)
	}
	if err := validateFixedHex(b.GenesisForkVersion, 4); err != nil {
		return fmt.Errorf("genesis_fork_version: %w", err)
	}
	if _, present := b.SpecExtra["SECONDS_PER_SLOT"]; present {
		return errors.New("spec_extra must not redefine SECONDS_PER_SLOT")
	}
	return nil
}

func validateFixedHex(value string, size int) error {
	if !strings.HasPrefix(value, "0x") {
		return fmt.Errorf("must be 0x-prefixed %d-byte hex", size)
	}
	raw, err := hex.DecodeString(strings.TrimPrefix(value, "0x"))
	if err != nil || len(raw) != size {
		return fmt.Errorf("must be 0x-prefixed %d-byte hex", size)
	}
	return nil
}

func (t profileTrust) validate() error {
	if err := p2p.ValidateDNSLinkDomain(t.DNSLink); err != nil {
		return fmt.Errorf("dnslink: %w", err)
	}
	if t.URL != "" {
		u, err := url.Parse(t.URL)
		if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" {
			return fmt.Errorf("url %q must be an absolute HTTPS URL without userinfo, query, or fragment", t.URL)
		}
	}
	switch t.Mode {
	case "dnslink-delegated":
		if t.PubKey != "" {
			return errors.New("dnslink-delegated must not set pubkey; the authenticated DNSLink/IPNS chain delegates signer rotation")
		}
	case "dnslink+signer-pin":
		if err := validatePublicKey(t.PubKey); err != nil {
			return fmt.Errorf("dnslink+signer-pin pubkey: %w", err)
		}
	default:
		return fmt.Errorf("mode %q must be dnslink-delegated or dnslink+signer-pin", t.Mode)
	}
	return nil
}

func validatePublicKey(value string) error {
	raw, err := hex.DecodeString(value)
	if err != nil || len(raw) != 32 {
		return errors.New("must be a 32-byte hex ed25519 public key")
	}
	return nil
}

func validateProfilePin(pin PinConfig) error {
	switch pin.Mode {
	case "window":
		if pin.Duration <= 0 {
			return errors.New("window pin requires a positive duration")
		}
	case "full", "none":
		if pin.Duration != 0 {
			return fmt.Errorf("%s pin must not set duration", pin.Mode)
		}
	default:
		return fmt.Errorf("pin mode %q must be full, window, or none", pin.Mode)
	}
	return nil
}

func (p followProfile) contentDigest() (string, error) {
	p.Digest = ""
	raw, err := json.Marshal(p)
	if err != nil {
		return "", fmt.Errorf("encoding profile %q for digest: %w", p.Name, err)
	}
	sum := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

func validateDigest(value string) error {
	if !strings.HasPrefix(value, "sha256:") {
		return errors.New("must use sha256:<64 lower-case hex> form")
	}
	hexPart := strings.TrimPrefix(value, "sha256:")
	if len(hexPart) != sha256.Size*2 || strings.ToLower(hexPart) != hexPart {
		return errors.New("must use sha256:<64 lower-case hex> form")
	}
	if _, err := hex.DecodeString(hexPart); err != nil {
		return errors.New("must use sha256:<64 lower-case hex> form")
	}
	return nil
}

func profileExpansionNode(profile followProfile) (*yaml.Node, error) {
	heads := make(map[string]FollowHeadConfig, len(profile.Heads))
	for name, head := range profile.Heads {
		heads[name] = FollowHeadConfig(head)
	}
	liveHeads := make(map[string]LiveHeadConfig, len(profile.LiveHeads))
	for name, view := range profile.LiveHeads {
		liveHeads[name] = LiveHeadConfig(view)
	}
	expansion := expandedProfile{
		Net:       profile.Network.Name,
		Beacon:    profile.Network.Beacon,
		LiveHeads: liveHeads,
		Follow: expandedProfileFollow{
			URL:     profile.Trust.URL,
			DNSLink: profile.Trust.DNSLink,
			PubKey:  profile.Trust.PubKey,
			Verify:  profile.Verify,
			Heads:   heads,
		},
	}
	raw, err := yaml.Marshal(expansion)
	if err != nil {
		return nil, fmt.Errorf("encoding profile %q expansion: %w", profile.Name, err)
	}
	_, root, err := decodeYAMLDocument(raw, "expanded profile "+profile.Name)
	return root, err
}

// applyProfileOverrides permits only exact leaves already supplied by the
// profile. It cannot add a field or replace a mapping/sequence wholesale, so an
// override remains a visible, narrow exception rather than a second profile.
func applyProfileOverrides(dst, override *yaml.Node, path []string) error {
	if dst.Kind != yaml.MappingNode || override.Kind != yaml.MappingNode {
		return fmt.Errorf("%s must be a mapping", strings.Join(path, "."))
	}
	for i := 0; i < len(override.Content); i += 2 {
		key := override.Content[i].Value
		value := override.Content[i+1]
		_, existing := mappingEntry(dst, key)
		fieldPath := append(append([]string(nil), path...), key)
		if existing == nil {
			return fmt.Errorf("%s is not supplied by the selected profile", strings.Join(fieldPath, "."))
		}
		if existing.Kind == yaml.MappingNode {
			if value.Kind != yaml.MappingNode {
				return fmt.Errorf("%s is a mapping; override exact child fields instead", strings.Join(fieldPath, "."))
			}
			if err := applyProfileOverrides(existing, value, fieldPath); err != nil {
				return err
			}
			continue
		}
		if existing.Kind == yaml.SequenceNode || value.Kind == yaml.MappingNode || value.Kind == yaml.SequenceNode {
			return fmt.Errorf("%s is not an overridable scalar leaf", strings.Join(fieldPath, "."))
		}
		*existing = *cloneYAMLNode(value)
	}
	return nil
}

func validateProfileSyntax(node *yaml.Node) error {
	if node == nil {
		return nil
	}
	if node.Anchor != "" {
		return errors.New("YAML anchors are not allowed in profile-bearing config or profile bundles")
	}
	if node.Kind == yaml.AliasNode {
		return errors.New("YAML aliases are not allowed in profile-bearing config or profile bundles")
	}
	if node.Kind == yaml.MappingNode {
		seen := make(map[string]struct{}, len(node.Content)/2)
		for i := 0; i < len(node.Content); i += 2 {
			key := node.Content[i]
			if key.Value == "<<" {
				return errors.New("YAML merge keys are not allowed in profile-bearing config or profile bundles")
			}
			if key.Kind != yaml.ScalarNode || key.Tag != "!!str" {
				return errors.New("mapping keys must be strings")
			}
			if _, duplicate := seen[key.Value]; duplicate {
				return fmt.Errorf("duplicate mapping key %q", key.Value)
			}
			seen[key.Value] = struct{}{}
		}
	}
	for _, child := range node.Content {
		if err := validateProfileSyntax(child); err != nil {
			return err
		}
	}
	return nil
}

func mappingEntry(mapping *yaml.Node, key string) (*yaml.Node, *yaml.Node) {
	if mapping == nil || mapping.Kind != yaml.MappingNode {
		return nil, nil
	}
	for i := 0; i+1 < len(mapping.Content); i += 2 {
		if mapping.Content[i].Value == key {
			return mapping.Content[i], mapping.Content[i+1]
		}
	}
	return nil, nil
}

func removeMappingEntry(mapping *yaml.Node, key string) {
	for i := 0; i+1 < len(mapping.Content); i += 2 {
		if mapping.Content[i].Value == key {
			mapping.Content = append(mapping.Content[:i], mapping.Content[i+2:]...)
			return
		}
	}
}

func cloneYAMLNode(node *yaml.Node) *yaml.Node {
	if node == nil {
		return nil
	}
	clone := *node
	clone.Content = make([]*yaml.Node, len(node.Content))
	for i, child := range node.Content {
		clone.Content[i] = cloneYAMLNode(child)
	}
	if node.Alias != nil {
		clone.Alias = cloneYAMLNode(node.Alias)
	}
	return &clone
}

var profileSelectionKey = []byte("Pprofile-selection:v1")

type storedProfileSelection struct {
	Name    string `json:"name"`
	Schema  string `json:"schema"`
	Version uint64 `json:"version"`
	Digest  string `json:"digest"`
}

// ensureProfileSelection persists the profile identity beside the follower's
// anti-rollback state. A same-name, same-version content change is refused
// unless the config acknowledges the exact new digest. Selecting a differently
// named/versioned profile is itself explicit in config and advances the record.
func ensureProfileSelection(kv *pebble.DB, selected *ProfileSelection) error {
	if selected == nil {
		return nil
	}
	current := storedProfileSelection{Name: selected.Name, Schema: selected.Schema, Version: selected.Version, Digest: selected.Digest}
	raw, closer, err := kv.Get(profileSelectionKey)
	if errors.Is(err, pebble.ErrNotFound) {
		return persistProfileSelection(kv, current)
	}
	if err != nil {
		return fmt.Errorf("reading persisted profile selection: %w", err)
	}
	storedRaw := append([]byte(nil), raw...)
	if err := closer.Close(); err != nil {
		return fmt.Errorf("closing persisted profile selection read: %w", err)
	}
	var prior storedProfileSelection
	if err := json.Unmarshal(storedRaw, &prior); err != nil {
		return fmt.Errorf("persisted profile selection is invalid: %w", err)
	}
	if err := prior.validate(); err != nil {
		return fmt.Errorf("persisted profile selection is invalid: %w", err)
	}
	if prior.Name == current.Name && prior.Schema == current.Schema && prior.Version == current.Version && prior.Digest != current.Digest {
		if selected.acknowledgeDigest != current.Digest {
			return fmt.Errorf("profile %s schema %s version %d changed digest from %s to %s without explicit acknowledgement; set profile.acknowledge_digest to %s after reviewing the expanded config", current.Name, current.Schema, current.Version, prior.Digest, current.Digest, current.Digest)
		}
	}
	if prior == current {
		return nil
	}
	return persistProfileSelection(kv, current)
}

func (s storedProfileSelection) validate() error {
	if err := validateProfileName("name", s.Name); err != nil {
		return err
	}
	if s.Schema != followProfileSchema {
		return fmt.Errorf("schema is %q, want %q", s.Schema, followProfileSchema)
	}
	if s.Version == 0 {
		return errors.New("version must be positive")
	}
	if err := validateDigest(s.Digest); err != nil {
		return fmt.Errorf("digest: %w", err)
	}
	return nil
}

func persistProfileSelection(kv *pebble.DB, selected storedProfileSelection) error {
	raw, err := json.Marshal(selected)
	if err != nil {
		return fmt.Errorf("encoding profile selection: %w", err)
	}
	if err := kv.Set(profileSelectionKey, raw, pebble.Sync); err != nil {
		return fmt.Errorf("persisting profile selection: %w", err)
	}
	return nil
}

// inspectConfig writes effective, validated config without reading any token or
// private-key file. File references remain visible because they are operational
// config, but secret contents can never enter this path.
func inspectConfig(path string, out io.Writer) error {
	cfg, err := LoadConfig(path)
	if err != nil {
		return err
	}
	inspection := struct {
		Profile *ProfileSelection `yaml:"profile,omitempty"`
		Config  *Config           `yaml:"config"`
	}{Profile: cfg.profileSelection, Config: cfg}
	enc := yaml.NewEncoder(out)
	enc.SetIndent(2)
	if err := enc.Encode(inspection); err != nil {
		return fmt.Errorf("encoding inspected config: %w", err)
	}
	return enc.Close()
}
