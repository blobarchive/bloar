package kubo

import (
	"bufio"
	"context"
	"strconv"
	"strings"
	"unicode/utf8"
)

const (
	// SupportedKuboLine is the only daemon release line covered by this client.
	SupportedKuboLine = "0.42.x"
	maxCommandsBytes  = 1 << 20
	maxCommandLines   = 4096
	maxCommandLine    = 4096
)

// requiredCapabilities is deliberately explicit. A version string alone is
// not enough: reverse proxies and downstream builds can expose only part of
// Kubo's administrative API. The commands endpoint is inspected before any
// managed-backend mutation or network call.
var requiredCapabilities = []string{
	"ipfs block get",
	"ipfs block put",
	"ipfs block put --allow-big-block",
	"ipfs block put --cid-codec",
	"ipfs block put --mhlen",
	"ipfs block put --mhtype",
	"ipfs block put --pin",
	"ipfs block rm",
	"ipfs block rm --force",
	"ipfs block rm --quiet",
	"ipfs block stat",
	"ipfs config",
	"ipfs config --expand-auto",
	"ipfs id",
	"ipfs key gen",
	"ipfs key gen --ipns-base",
	"ipfs key gen --type",
	"ipfs key ls",
	"ipfs key ls -l",
	"ipfs key ls --ipns-base",
	"ipfs name publish",
	"ipfs name publish --allow-delegated",
	"ipfs name publish --allow-offline",
	"ipfs name publish --ipns-base",
	"ipfs name publish --key",
	"ipfs name publish --lifetime",
	"ipfs name publish --quieter",
	"ipfs name publish --resolve",
	"ipfs name publish --sequence",
	"ipfs name publish --ttl",
	"ipfs name publish --v1compat",
	"ipfs name resolve",
	"ipfs name resolve --dht-record-count",
	"ipfs name resolve --dht-timeout",
	"ipfs name resolve --nocache",
	"ipfs name resolve --recursive",
	"ipfs name resolve --stream",
	"ipfs pin add",
	"ipfs pin add --fast-provide-dag",
	"ipfs pin add --fast-provide-root",
	"ipfs pin add --fast-provide-wait",
	"ipfs pin add --name",
	"ipfs pin add --progress",
	"ipfs pin add --recursive",
	"ipfs pin ls",
	"ipfs pin ls --name",
	"ipfs pin ls --names",
	"ipfs pin ls --quiet",
	"ipfs pin ls --stream",
	"ipfs pin ls --type",
	"ipfs pin rm",
	"ipfs pin rm --recursive",
	"ipfs pin update",
	"ipfs pin update --fast-provide-dag",
	"ipfs pin update --fast-provide-root",
	"ipfs pin update --fast-provide-wait",
	"ipfs pin update --unpin",
	"ipfs provide once",
	"ipfs provide once --recursive",
	"ipfs refs local",
	"ipfs repo gc",
	"ipfs repo gc --quiet",
	"ipfs repo gc --silent",
	"ipfs repo gc --stream-errors",
	"ipfs repo stat",
	"ipfs repo stat --human",
	"ipfs repo stat --size-only",
	"ipfs repo verify",
	"ipfs repo verify --drop",
	"ipfs repo verify --heal",
	"ipfs repo verify --heal-timeout",
	"ipfs routing get",
	"ipfs swarm connect",
	"ipfs swarm peers",
	"ipfs swarm peers --direction",
	"ipfs swarm peers --identify",
	"ipfs swarm peers --latency",
	"ipfs swarm peers --streams",
	"ipfs swarm peers --verbose",
	"ipfs version",
}

// replicaRequiredCapabilities is the least-privilege RPC surface used by a
// standalone replica. In particular it excludes block removal, repository
// inventory/GC/verify, key generation, name publication, and generic config
// access beyond the exact reads enforced by ConfigProvideEnabled and
// ConfigProvideStrategy.
var replicaRequiredCapabilities = []string{
	"ipfs block get",
	"ipfs block put",
	"ipfs block put --allow-big-block",
	"ipfs block put --cid-codec",
	"ipfs block put --mhlen",
	"ipfs block put --mhtype",
	"ipfs block put --pin",
	"ipfs config",
	"ipfs config --expand-auto",
	"ipfs id",
	"ipfs pin add",
	"ipfs pin add --fast-provide-dag",
	"ipfs pin add --fast-provide-root",
	"ipfs pin add --fast-provide-wait",
	"ipfs pin add --name",
	"ipfs pin add --progress",
	"ipfs pin add --recursive",
	"ipfs pin ls",
	"ipfs pin ls --name",
	"ipfs pin ls --names",
	"ipfs pin ls --quiet",
	"ipfs pin ls --stream",
	"ipfs pin ls --type",
	"ipfs pin rm",
	"ipfs pin rm --recursive",
	"ipfs pin update",
	"ipfs pin update --fast-provide-dag",
	"ipfs pin update --fast-provide-root",
	"ipfs pin update --fast-provide-wait",
	"ipfs pin update --unpin",
	"ipfs provide once",
	"ipfs provide once --recursive",
	"ipfs routing get",
	"ipfs swarm connect",
	"ipfs version",
}

// replicaRuntimeCapabilities is the authenticated surface needed by the
// distributed quickstart after an operator has checked Provide.Enabled and
// Provide.Strategy from the native host. Kubo's API.Authorizations matches only
// URL paths, so granting /api/v0/config for a read would also grant arbitrary
// config writes. Excluding the command entirely is the only least-privilege
// contract Kubo's built-in authorization layer can enforce.
var replicaRuntimeCapabilities = []string{
	"ipfs block get",
	"ipfs block put",
	"ipfs block put --allow-big-block",
	"ipfs block put --cid-codec",
	"ipfs block put --mhlen",
	"ipfs block put --mhtype",
	"ipfs block put --pin",
	"ipfs id",
	"ipfs pin add",
	"ipfs pin add --fast-provide-dag",
	"ipfs pin add --fast-provide-root",
	"ipfs pin add --fast-provide-wait",
	"ipfs pin add --name",
	"ipfs pin add --progress",
	"ipfs pin add --recursive",
	"ipfs pin ls",
	"ipfs pin ls --name",
	"ipfs pin ls --names",
	"ipfs pin ls --quiet",
	"ipfs pin ls --stream",
	"ipfs pin ls --type",
	"ipfs pin rm",
	"ipfs pin rm --recursive",
	"ipfs pin update",
	"ipfs pin update --fast-provide-dag",
	"ipfs pin update --fast-provide-root",
	"ipfs pin update --fast-provide-wait",
	"ipfs pin update --unpin",
	"ipfs provide once",
	"ipfs provide once --recursive",
	"ipfs routing get",
	"ipfs swarm connect",
	"ipfs version",
}

// censusRequiredCapabilities is the read-only network surface used by the
// decentralized swarm census. It deliberately excludes every block, pin,
// repository, key, and publication mutation.
var censusRequiredCapabilities = []string{
	"ipfs routing findprovs",
	"ipfs routing findprovs --num-providers",
	"ipfs version",
}

// CheckCompatibility reads the daemon's version and advertised command tree,
// then fails closed unless it is a stable 0.42.x release exposing every RPC
// and flag this client pins. An optional leading "v" and SemVer build metadata
// are safe because neither changes precedence; prerelease suffixes are
// deliberately rejected. Managed-backend construction must call this once
// before any mutation or network RPC. Version remains available separately for
// diagnostics against unsupported daemons.
func (c *Client) CheckCompatibility(ctx context.Context) (VersionInfo, error) {
	return c.checkCompatibility(ctx, requiredCapabilities)
}

// CheckReplicaCompatibility applies the Kubo version gate to only the narrow
// standalone-replica RPC profile. A reverse proxy can therefore omit block
// removal, repo GC/verify, refs inventory, key generation, and name publishing
// without weakening the broader managed-node CheckCompatibility contract.
func (c *Client) CheckReplicaCompatibility(ctx context.Context) (VersionInfo, error) {
	return c.checkCompatibility(ctx, replicaRequiredCapabilities)
}

// CheckReplicaRuntimeCompatibility checks the standalone replica surface that
// deliberately excludes Kubo's config RPC. It is for deployments which perform
// the provider-policy read on the native host and give the container a narrower
// runtime bearer token.
func (c *Client) CheckReplicaRuntimeCompatibility(ctx context.Context) (VersionInfo, error) {
	return c.checkCompatibility(ctx, replicaRuntimeCapabilities)
}

// CheckCensusCompatibility applies the Kubo version gate to the minimal,
// read-only provider-discovery profile used by FindProviders. This lets an
// operator expose only routing/findprovs and version through a reverse proxy.
func (c *Client) CheckCensusCompatibility(ctx context.Context) (VersionInfo, error) {
	return c.checkCompatibility(ctx, censusRequiredCapabilities)
}

func (c *Client) checkCompatibility(ctx context.Context, required []string) (VersionInfo, error) {
	info, err := c.Version(ctx)
	if err != nil {
		return VersionInfo{}, err
	}
	if !supportedKuboVersion(info.Version) {
		return info, &CompatibilityError{
			Version:   boundedText(c.redact(info.Version), 256),
			Supported: SupportedKuboLine,
		}
	}
	if err := c.checkCapabilities(ctx, required); err != nil {
		return info, err
	}
	return info, nil
}

func (c *Client) checkCapabilities(ctx context.Context, required []string) error {
	const endpoint = "commands"
	query := jsonQuery()
	query.Set("encoding", "text")
	query.Set("flags", "true")
	raw, err := c.post(ctx, endpoint, query, nil, "", "text/plain", maxCommandsBytes)
	if err != nil {
		return err
	}
	if !utf8.Valid(raw) {
		return c.protocol(endpoint, "response is not valid UTF-8")
	}

	advertised := make(map[string]struct{}, len(required)*2)
	scanner := bufio.NewScanner(strings.NewReader(string(raw)))
	scanner.Buffer(make([]byte, 1024), maxCommandLine)
	lines := 0
	for scanner.Scan() {
		lines++
		if lines > maxCommandLines {
			return c.protocol(endpoint, "response contains more than %d lines", maxCommandLines)
		}
		line := scanner.Text()
		if line == "" || strings.TrimSpace(line) != line {
			return c.protocol(endpoint, "response line %d is empty or has surrounding whitespace", lines)
		}
		for i, alternative := range strings.Split(line, " / ") {
			if !validCapabilityLine(alternative) {
				// Kubo 0.42 renders a pair of boolean shorthand aliases as a
				// trailing "--". The canonical long-form command remains valid
				// and is what compatibility profiles require. Ignore malformed
				// aliases, but never an invalid canonical command.
				if i == 0 {
					return c.protocol(endpoint, "response line %d has invalid command syntax", lines)
				}
				continue
			}
			if _, duplicate := advertised[alternative]; duplicate {
				return c.protocol(endpoint, "response repeats capability %q", alternative)
			}
			advertised[alternative] = struct{}{}
		}
	}
	if err := scanner.Err(); err != nil {
		return c.protocol(endpoint, "reading response: %v", err)
	}
	if lines == 0 {
		return c.protocol(endpoint, "response is empty")
	}
	for _, capability := range required {
		if _, ok := advertised[capability]; !ok {
			return &CapabilityError{Missing: capability}
		}
	}
	return nil
}

func validCapabilityLine(line string) bool {
	if line == "" || strings.TrimSpace(line) != line || strings.ContainsAny(line, "\t\r\n") {
		return false
	}
	parts := strings.Split(line, " ")
	if len(parts) < 1 || parts[0] != "ipfs" {
		return false
	}
	for i, part := range parts {
		if part == "" {
			return false
		}
		flag := i > 0 && strings.HasPrefix(part, "-")
		if flag {
			if len(part) < 2 || len(part) > 128 {
				return false
			}
			part = strings.TrimLeft(part, "-")
		}
		if part == "" || len(part) > 128 {
			return false
		}
		for j := 0; j < len(part); j++ {
			ch := part[j]
			if !(ch >= 'a' && ch <= 'z' || flag && ch >= 'A' && ch <= 'Z' || ch >= '0' && ch <= '9' || ch == '-') {
				return false
			}
		}
	}
	return true
}

func supportedKuboVersion(version string) bool {
	if version == "" || strings.TrimSpace(version) != version {
		return false
	}
	version = strings.TrimPrefix(version, "v")
	core, build, hasBuild := strings.Cut(version, "+")
	if strings.Contains(core, "-") || strings.Contains(build, "+") {
		return false
	}
	parts := strings.Split(core, ".")
	if len(parts) != 3 || parts[0] != "0" || parts[1] != "42" || !canonicalUint(parts[2]) {
		return false
	}
	if hasBuild && !validBuildMetadata(build) {
		return false
	}
	return true
}

func canonicalUint(value string) bool {
	if value == "" || len(value) > 1 && value[0] == '0' {
		return false
	}
	_, err := strconv.ParseUint(value, 10, 64)
	return err == nil
}

func validBuildMetadata(build string) bool {
	if build == "" {
		return false
	}
	for _, identifier := range strings.Split(build, ".") {
		if identifier == "" {
			return false
		}
		for i := 0; i < len(identifier); i++ {
			ch := identifier[i]
			if !(ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch >= '0' && ch <= '9' || ch == '-') {
				return false
			}
		}
	}
	return true
}

func boundedText(value string, limit int) string {
	value = strings.ToValidUTF8(value, "�")
	if len(value) <= limit {
		return value
	}
	if limit <= 3 {
		return strings.Repeat(".", limit)
	}
	cut := limit - 3
	for cut > 0 && !utf8.ValidString(value[:cut]) {
		cut--
	}
	return value[:cut] + "..."
}
