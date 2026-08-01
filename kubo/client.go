package kubo

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net"
	"net/http"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multihash"
)

const (
	// DefaultRequestTimeout bounds one complete RPC, including response reads.
	DefaultRequestTimeout = 30 * time.Second
	// DefaultMaxBlockBytes matches Kubo's standard Bitswap-compatible block
	// ceiling. This client never enables Kubo's allow-big-block escape hatch.
	DefaultMaxBlockBytes int64 = 2 << 20
	// DefaultMaxStreamBytes bounds one collection or object stream. Callers may
	// override it for an explicitly sized large repository, but cannot disable
	// it or exceed MaximumStreamBytes.
	DefaultMaxStreamBytes int64 = 16 << 20
	// DefaultMaxStreamItems bounds decoded collection entries in one RPC.
	DefaultMaxStreamItems = 100_000
	// MaximumStreamBytes is the absolute configurable ceiling. It accommodates
	// a large managed Kubo repository while keeping every request finite; every
	// NDJSON value also has a separate fixed 1 MiB ceiling so this aggregate
	// allowance can never become one proportional allocation. The much smaller
	// default remains appropriate for ordinary metadata calls.
	MaximumStreamBytes int64 = 8 << 30
	// MaximumStreamItems is the absolute configurable item ceiling for large
	// repo streams such as refs/local and repo/verify.
	MaximumStreamItems = 100_000_000

	maxMetadataBytes       int64 = 64 << 10
	maxErrorBytes          int64 = 64 << 10
	maxErrorTypeBytes            = 256
	maxDiagnosticBytes           = 8 << 10
	maxCIDTextBytes              = 512
	maxBearerTokenBytes          = 8 << 10
	maxCredentialFileBytes       = 16 << 10
)

// Config constructs a bounded Kubo RPC client.
type Config struct {
	// BaseURL is the Kubo RPC authority, optionally followed by a reverse-proxy
	// path prefix. It must not contain userinfo, query parameters, or a fragment.
	BaseURL string

	// Exactly one bearer source is required unless AllowUnauthenticated is set.
	// BearerTokenFile is read once by New; surrounding whitespace (normally one
	// trailing newline) is removed.
	BearerToken     string
	BearerTokenFile string
	// AllowUnauthenticated explicitly selects a credential-free RPC. It is
	// accepted only for loopback hosts (over HTTP or HTTPS) and is mutually
	// exclusive with both bearer sources.
	AllowUnauthenticated bool

	// HTTPClient is copied before use. Redirects are always disabled so its
	// bearer credential cannot be forwarded to another URL.
	HTTPClient *http.Client
	// RequestTimeout applies even when HTTPClient has no Timeout. Zero selects
	// DefaultRequestTimeout; negative values are invalid.
	RequestTimeout time.Duration
	// MaxBlockBytes may tighten, but never exceed, Kubo's standard 2 MiB limit.
	// Zero selects DefaultMaxBlockBytes.
	MaxBlockBytes int64
	// MaxStreamBytes and MaxStreamItems set finite collection ceilings. Zero
	// selects the corresponding default; values up to MaximumStreamBytes and
	// MaximumStreamItems support explicitly sized large managed repositories.
	MaxStreamBytes int64
	MaxStreamItems int

	// Plain HTTP is accepted automatically only for loopback hosts. Set this
	// explicitly for a trusted non-loopback network; HTTPS is preferred because
	// the Kubo RPC API grants administrative access.
	AllowInsecureHTTP bool
}

// Client is safe for concurrent use.
type Client struct {
	base           url.URL
	token          string
	http           http.Client
	timeout        time.Duration
	maxBlockBytes  int64
	maxStreamBytes int64
	maxStreamItems int
}

// VersionInfo is the bounded response from /api/v0/version.
type VersionInfo struct {
	Version string
	Commit  string
	Repo    string
	System  string
	Golang  string
}

// IDInfo is the validated local identity from /api/v0/id.
type IDInfo struct {
	ID           peer.ID
	PublicKey    string
	Addresses    []ma.Multiaddr
	AgentVersion string
	Protocols    []string
}

// BlockStat is Kubo's identity and byte count for one block.
type BlockStat struct {
	CID  cid.Cid
	Size int64
}

// New validates cfg, loads its credential, and returns an idle client.
func New(cfg Config) (*Client, error) {
	base, err := parseBaseURL(cfg.BaseURL, cfg.AllowInsecureHTTP || cfg.AllowUnauthenticated)
	if err != nil {
		return nil, err
	}
	if cfg.AllowUnauthenticated && !loopbackHost(base.Hostname()) {
		return nil, errors.New("kubo: unauthenticated RPC is restricted to loopback hosts")
	}
	token, err := loadBearerToken(cfg.BearerToken, cfg.BearerTokenFile, cfg.AllowUnauthenticated)
	if err != nil {
		return nil, err
	}

	timeout := cfg.RequestTimeout
	if timeout == 0 {
		timeout = DefaultRequestTimeout
	}
	if timeout < 0 {
		return nil, errors.New("kubo: Config.RequestTimeout must not be negative")
	}
	maxBlockBytes := cfg.MaxBlockBytes
	if maxBlockBytes == 0 {
		maxBlockBytes = DefaultMaxBlockBytes
	}
	if maxBlockBytes < 0 || maxBlockBytes > DefaultMaxBlockBytes {
		return nil, fmt.Errorf("kubo: Config.MaxBlockBytes must be between 1 and %d", DefaultMaxBlockBytes)
	}
	maxStreamBytes := cfg.MaxStreamBytes
	if maxStreamBytes == 0 {
		maxStreamBytes = DefaultMaxStreamBytes
	}
	if maxStreamBytes < 0 || maxStreamBytes > MaximumStreamBytes {
		return nil, fmt.Errorf("kubo: Config.MaxStreamBytes must be between 1 and %d", MaximumStreamBytes)
	}
	maxStreamItems := cfg.MaxStreamItems
	if maxStreamItems == 0 {
		maxStreamItems = DefaultMaxStreamItems
	}
	if maxStreamItems < 0 || maxStreamItems > MaximumStreamItems {
		return nil, fmt.Errorf("kubo: Config.MaxStreamItems must be between 1 and %d", MaximumStreamItems)
	}

	var hc http.Client
	if cfg.HTTPClient != nil {
		hc = *cfg.HTTPClient
	}
	// Authentication redirects are never implicit. Returning ErrUseLastResponse
	// gives the caller a typed StatusError for the original 3xx response.
	hc.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}

	return &Client{
		base:           base,
		token:          token,
		http:           hc,
		timeout:        timeout,
		maxBlockBytes:  maxBlockBytes,
		maxStreamBytes: maxStreamBytes,
		maxStreamItems: maxStreamItems,
	}, nil
}

func parseBaseURL(raw string, allowInsecure bool) (url.URL, error) {
	if raw == "" {
		return url.URL{}, errors.New("kubo: Config.BaseURL is required")
	}
	u, err := url.Parse(raw)
	if err != nil {
		// Do not quote raw: userinfo or a query may contain a credential.
		return url.URL{}, errors.New("kubo: Config.BaseURL is not a valid URL")
	}
	u.Scheme = strings.ToLower(u.Scheme)
	if u.Scheme != "http" && u.Scheme != "https" {
		return url.URL{}, errors.New("kubo: Config.BaseURL must use http or https")
	}
	if u.Opaque != "" || u.Host == "" || u.Hostname() == "" {
		return url.URL{}, errors.New("kubo: Config.BaseURL must be an absolute URL with a host")
	}
	if strings.HasSuffix(u.Host, ":") {
		return url.URL{}, errors.New("kubo: Config.BaseURL must not contain an empty port")
	}
	if port := u.Port(); port != "" {
		n, err := strconv.ParseUint(port, 10, 16)
		if err != nil || n == 0 {
			return url.URL{}, errors.New("kubo: Config.BaseURL has an invalid port")
		}
	}
	if u.User != nil {
		return url.URL{}, errors.New("kubo: Config.BaseURL must not contain userinfo; use bearer authentication")
	}
	if u.RawQuery != "" || u.ForceQuery {
		return url.URL{}, errors.New("kubo: Config.BaseURL must not contain a query")
	}
	if u.Fragment != "" {
		return url.URL{}, errors.New("kubo: Config.BaseURL must not contain a fragment")
	}
	if u.RawPath != "" || u.EscapedPath() != u.Path {
		return url.URL{}, errors.New("kubo: Config.BaseURL must not contain an encoded path")
	}
	prefix := strings.TrimSuffix(u.Path, "/")
	if prefix != "" && (path.Clean(prefix) != prefix || !strings.HasPrefix(prefix, "/")) {
		return url.URL{}, errors.New("kubo: Config.BaseURL has a non-canonical path prefix")
	}
	u.Path = prefix
	if u.Scheme == "http" && !allowInsecure && !loopbackHost(u.Hostname()) {
		return url.URL{}, errors.New("kubo: plain HTTP bearer authentication is restricted to loopback; use HTTPS or AllowInsecureHTTP")
	}
	return *u, nil
}

func loopbackHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func loadBearerToken(value, file string, unauthenticated bool) (string, error) {
	if unauthenticated {
		if value != "" || file != "" {
			return "", errors.New("kubo: AllowUnauthenticated is mutually exclusive with bearer authentication")
		}
		return "", nil
	}
	if value != "" && file != "" {
		return "", errors.New("kubo: configure exactly one of BearerToken and BearerTokenFile")
	}
	if value == "" && file == "" {
		return "", errors.New("kubo: bearer authentication is required")
	}
	token := value
	if file != "" {
		f, err := os.Open(file)
		if err != nil {
			return "", fmt.Errorf("kubo: opening BearerTokenFile: %w", err)
		}
		defer f.Close()
		info, err := f.Stat()
		if err != nil {
			return "", fmt.Errorf("kubo: inspecting BearerTokenFile: %w", err)
		}
		if !info.Mode().IsRegular() {
			return "", errors.New("kubo: BearerTokenFile must be a regular file")
		}
		raw, err := io.ReadAll(io.LimitReader(f, maxCredentialFileBytes+1))
		if err != nil {
			return "", fmt.Errorf("kubo: reading BearerTokenFile: %w", err)
		}
		if len(raw) > maxCredentialFileBytes {
			return "", errors.New("kubo: BearerTokenFile is too large")
		}
		token = strings.TrimSpace(string(raw))
	}
	if err := validateBearerToken(token); err != nil {
		return "", err
	}
	return token, nil
}

func validateBearerToken(token string) error {
	if token == "" {
		return errors.New("kubo: bearer token must not be empty")
	}
	if len(token) > maxBearerTokenBytes {
		return errors.New("kubo: bearer token is too large")
	}
	seenData := false
	seenPadding := false
	for i := 0; i < len(token); i++ {
		ch := token[i]
		if ch == '=' {
			seenPadding = true
			continue
		}
		allowed := ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch >= '0' && ch <= '9' ||
			strings.ContainsRune("-._~+/", rune(ch))
		if !allowed || seenPadding {
			return errors.New("kubo: bearer token is not a valid RFC 6750 bearer credential")
		}
		seenData = true
	}
	if !seenData {
		return errors.New("kubo: bearer token is not a valid RFC 6750 bearer credential")
	}
	return nil
}

// Version returns the daemon's version metadata.
func (c *Client) Version(ctx context.Context) (VersionInfo, error) {
	const endpoint = "version"
	raw, err := c.post(ctx, endpoint, jsonQuery(), nil, "", "application/json", maxMetadataBytes)
	if err != nil {
		return VersionInfo{}, err
	}
	var wire struct {
		Version string
		Commit  string
		Repo    string
		System  string
		Golang  string
	}
	if err := decodeStrictJSON(raw, &wire); err != nil {
		return VersionInfo{}, c.protocol(endpoint, "decoding JSON: %v", err)
	}
	if wire.Version == "" {
		return VersionInfo{}, c.protocol(endpoint, "response has an empty Version")
	}
	return VersionInfo(wire), nil
}

// ID returns and validates the local Kubo peer identity. It intentionally does
// not expose Kubo's optional remote-peer lookup argument.
func (c *Client) ID(ctx context.Context) (IDInfo, error) {
	const endpoint = "id"
	raw, err := c.post(ctx, endpoint, jsonQuery(), nil, "", "application/json", maxMetadataBytes)
	if err != nil {
		return IDInfo{}, err
	}
	var wire struct {
		ID           string
		PublicKey    string
		Addresses    []string
		AgentVersion string
		Protocols    []string
	}
	if err := decodeStrictJSON(raw, &wire); err != nil {
		return IDInfo{}, c.protocol(endpoint, "decoding JSON: %v", err)
	}
	id, err := peer.Decode(wire.ID)
	if err != nil {
		return IDInfo{}, c.protocol(endpoint, "invalid peer ID: %v", err)
	}
	addresses := make([]ma.Multiaddr, len(wire.Addresses))
	for i, rawAddress := range wire.Addresses {
		address, err := ma.NewMultiaddr(rawAddress)
		if err != nil {
			return IDInfo{}, c.protocol(endpoint, "invalid address %d: %v", i, err)
		}
		addresses[i] = address
	}
	return IDInfo{
		ID:           id,
		PublicKey:    wire.PublicKey,
		Addresses:    addresses,
		AgentVersion: wire.AgentVersion,
		Protocols:    append([]string(nil), wire.Protocols...),
	}, nil
}

// BlockGet returns a block only after recomputing and matching the requested
// CID. A 200 response is never treated as trusted merely because it came from
// the authenticated daemon.
func (c *Client) BlockGet(ctx context.Context, expected cid.Cid) (blocks.Block, error) {
	const endpoint = "block/get"
	expectedText, err := boundedCIDArgument(endpoint, expected)
	if err != nil {
		return nil, err
	}
	query := jsonQuery()
	query.Set("arg", expectedText)
	raw, err := c.post(ctx, endpoint, query, nil, "", "application/vnd.ipld.raw", c.maxBlockBytes)
	if err != nil {
		return nil, c.asNotFound(endpoint, expected, err)
	}
	if err := verifyCID(expected, raw); err != nil {
		return nil, c.protocol(endpoint, "%v", err)
	}
	block, err := blocks.NewBlockWithCid(raw, expected)
	if err != nil {
		return nil, c.protocol(endpoint, "constructing verified block: %v", err)
	}
	return block, nil
}

// BlockPut stores one already-content-addressed Bloar block. Only Bloar's
// CIDv1 raw/dag-cbor sha2-256 profile is accepted, and Kubo's returned CID and
// size must exactly match the input.
func (c *Client) BlockPut(ctx context.Context, block blocks.Block) (BlockStat, error) {
	const endpoint = "block/put"
	if block == nil || !block.Cid().Defined() {
		return BlockStat{}, errors.New("kubo: block/put requires a block with a defined CID")
	}
	data := append([]byte(nil), block.RawData()...)
	if int64(len(data)) > c.maxBlockBytes {
		return BlockStat{}, fmt.Errorf("kubo: block/put block is %d bytes, over the %d-byte limit", len(data), c.maxBlockBytes)
	}
	codec, err := bloarCodecName(block.Cid())
	if err != nil {
		return BlockStat{}, err
	}
	if err := verifyCID(block.Cid(), data); err != nil {
		return BlockStat{}, fmt.Errorf("kubo: block/put: %w", err)
	}

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	part, err := writer.CreateFormFile("file", "block")
	if err != nil {
		return BlockStat{}, fmt.Errorf("kubo: block/put: building multipart body: %w", err)
	}
	if _, err := part.Write(data); err != nil {
		return BlockStat{}, fmt.Errorf("kubo: block/put: building multipart body: %w", err)
	}
	if err := writer.Close(); err != nil {
		return BlockStat{}, fmt.Errorf("kubo: block/put: closing multipart body: %w", err)
	}

	query := jsonQuery()
	query.Set("cid-codec", codec)
	query.Set("mhtype", "sha2-256")
	query.Set("mhlen", "32")
	query.Set("pin", "false")
	query.Set("allow-big-block", "false")
	raw, err := c.post(ctx, endpoint, query, body.Bytes(), writer.FormDataContentType(), "application/json", maxMetadataBytes)
	if err != nil {
		return BlockStat{}, err
	}
	return c.decodeBlockStat(endpoint, block.Cid(), int64(len(data)), raw)
}

// BlockStat returns Kubo's byte count and verifies that its answer names the
// exact requested CID. Kubo 0.42 may fetch this command's block online; callers
// that require authoritative local presence must use BlockGetLocal instead.
func (c *Client) BlockStat(ctx context.Context, expected cid.Cid) (BlockStat, error) {
	const endpoint = "block/stat"
	expectedText, err := boundedCIDArgument(endpoint, expected)
	if err != nil {
		return BlockStat{}, err
	}
	query := jsonQuery()
	query.Set("arg", expectedText)
	raw, err := c.post(ctx, endpoint, query, nil, "", "application/json", maxMetadataBytes)
	if err != nil {
		return BlockStat{}, c.asNotFound(endpoint, expected, err)
	}
	return c.decodeBlockStat(endpoint, expected, -1, raw)
}

// BlockRemove removes exactly one block. Missing blocks return ErrNotFound.
func (c *Client) BlockRemove(ctx context.Context, expected cid.Cid) error {
	const endpoint = "block/rm"
	expectedText, err := boundedCIDArgument(endpoint, expected)
	if err != nil {
		return err
	}
	query := jsonQuery()
	query.Set("arg", expectedText)
	query.Set("force", "false")
	query.Set("quiet", "false")
	raw, err := c.post(ctx, endpoint, query, nil, "", "application/json", maxMetadataBytes)
	if err != nil {
		return c.asNotFound(endpoint, expected, err)
	}
	var wire struct {
		Hash  string
		Error string
	}
	if err := decodeStrictJSON(raw, &wire); err != nil {
		return c.protocol(endpoint, "decoding JSON: %v", err)
	}
	if wire.Hash == "" {
		return c.protocol(endpoint, "response is missing Hash")
	}
	if len(wire.Hash) > maxCIDTextBytes {
		return c.protocol(endpoint, "Hash CID exceeds the %d-byte limit", maxCIDTextBytes)
	}
	removed, err := cid.Parse(wire.Hash)
	if err != nil {
		return c.protocol(endpoint, "invalid Hash CID: %v", err)
	}
	if !removed.Equals(expected) {
		return c.protocol(endpoint, "response names CID %s, want %s", removed, expected)
	}
	if wire.Error != "" {
		status := &StatusError{
			Endpoint: endpoint, Status: http.StatusOK,
			Message: boundedText(c.redact(wire.Error), int(maxErrorBytes)),
		}
		if messageIsBlockNotFound(wire.Error, expectedText) {
			return &NotFoundError{Endpoint: endpoint, CID: expected.String(), Status: status}
		}
		return c.protocol(endpoint, "Kubo reported a removal error: %s", wire.Error)
	}
	return nil
}

func (c *Client) decodeBlockStat(endpoint string, expected cid.Cid, expectedSize int64, raw []byte) (BlockStat, error) {
	var wire struct {
		Key  *string
		Size *int64
	}
	if err := decodeStrictJSON(raw, &wire); err != nil {
		return BlockStat{}, c.protocol(endpoint, "decoding JSON: %v", err)
	}
	if wire.Key == nil || wire.Size == nil {
		return BlockStat{}, c.protocol(endpoint, "response is missing Key or Size")
	}
	got, err := cid.Parse(*wire.Key)
	if err != nil {
		return BlockStat{}, c.protocol(endpoint, "invalid Key CID: %v", err)
	}
	if !got.Equals(expected) {
		return BlockStat{}, c.protocol(endpoint, "response names CID %s, want %s", got, expected)
	}
	if *wire.Size < 0 || *wire.Size > c.maxBlockBytes {
		return BlockStat{}, c.protocol(endpoint, "response size %d is outside the 0..%d-byte limit", *wire.Size, c.maxBlockBytes)
	}
	if expectedSize >= 0 && *wire.Size != expectedSize {
		return BlockStat{}, c.protocol(endpoint, "response size %d, want %d", *wire.Size, expectedSize)
	}
	return BlockStat{CID: got, Size: *wire.Size}, nil
}

func bloarCodecName(c cid.Cid) (string, error) {
	prefix := c.Prefix()
	if prefix.Version != 1 || prefix.MhType != multihash.SHA2_256 || prefix.MhLength != 32 {
		return "", errors.New("kubo: block/put accepts only CIDv1 sha2-256-32 blocks")
	}
	switch prefix.Codec {
	case cid.Raw:
		return "raw", nil
	case cid.DagCBOR:
		return "dag-cbor", nil
	default:
		return "", fmt.Errorf("kubo: block/put does not support CID codec 0x%x", prefix.Codec)
	}
}

func verifyCID(expected cid.Cid, data []byte) error {
	computed, err := expected.Prefix().Sum(data)
	if err != nil {
		return fmt.Errorf("hashing bytes for CID %s: %w", expected, err)
	}
	if !computed.Equals(expected) {
		return fmt.Errorf("block bytes do not match CID %s", expected)
	}
	return nil
}

func boundedCIDArgument(endpoint string, target cid.Cid) (string, error) {
	if !target.Defined() {
		return "", fmt.Errorf("kubo: %s requires a defined CID", endpoint)
	}
	text := target.String()
	if len(text) > maxCIDTextBytes {
		return "", fmt.Errorf("kubo: %s CID exceeds the %d-byte limit", endpoint, maxCIDTextBytes)
	}
	return text, nil
}

func jsonQuery() url.Values {
	return url.Values{"encoding": []string{"json"}}
}

func (c *Client) post(ctx context.Context, endpoint string, query url.Values, body []byte, contentType, accept string, maxBody int64) ([]byte, error) {
	requestCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	return c.postContext(requestCtx, endpoint, query, body, contentType, accept, maxBody)
}

// postContext performs one bounded RPC using exactly the lifetime carried by
// ctx. Ordinary calls go through post, which applies Config.RequestTimeout.
// The few explicitly long-running RPCs require a caller deadline and use this
// helper directly instead.
func (c *Client) postContext(ctx context.Context, endpoint string, query url.Values, body []byte, contentType, accept string, maxBody int64) ([]byte, error) {
	u := c.base
	u.Path = strings.TrimSuffix(u.Path, "/") + "/api/v0/" + endpoint
	u.RawQuery = query.Encode()
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), reader)
	if err != nil {
		return nil, c.protocol(endpoint, "building request: %v", err)
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	req.Header.Set("Accept", accept)
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return nil, context.Cause(ctx)
		}
		// http.Client.Timeout expires independently of the request context. Preserve the
		// standard deadline classification, but not the original *url.Error: a
		// custom transport may have included the bearer value in its text.
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, context.DeadlineExceeded
		}
		if errors.Is(err, context.Canceled) {
			return nil, context.Canceled
		}
		return nil, &TransportError{Endpoint: endpoint, Problem: boundedText(c.redact(err.Error()), maxDiagnosticBytes)}
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, c.readStatusError(endpoint, resp)
	}
	mediaType, _, err := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	if err != nil || mediaType != accept {
		return nil, c.protocol(endpoint, "response Content-Type is %q, want %s", resp.Header.Get("Content-Type"), accept)
	}
	return c.readSuccess(endpoint, resp, maxBody)
}

func (c *Client) readSuccess(endpoint string, resp *http.Response, maxBody int64) ([]byte, error) {
	if resp.ContentLength > maxBody {
		return nil, c.protocol(endpoint, "response declares %d bytes, over the %d-byte limit", resp.ContentLength, maxBody)
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxBody+1))
	if err != nil {
		if resp.Request != nil && resp.Request.Context().Err() != nil {
			return nil, context.Cause(resp.Request.Context())
		}
		return nil, c.protocol(endpoint, "reading response body: %v", err)
	}
	if int64(len(raw)) > maxBody {
		return nil, c.protocol(endpoint, "response body exceeds the %d-byte limit", maxBody)
	}
	if resp.ContentLength >= 0 && int64(len(raw)) != resp.ContentLength {
		return nil, c.protocol(endpoint, "truncated response: read %d of %d declared bytes", len(raw), resp.ContentLength)
	}
	return raw, nil
}

func (c *Client) readStatusError(endpoint string, resp *http.Response) error {
	raw, readErr := io.ReadAll(io.LimitReader(resp.Body, maxErrorBytes+1))
	if readErr != nil && resp.Request != nil && resp.Request.Context().Err() != nil {
		return context.Cause(resp.Request.Context())
	}
	truncated := int64(len(raw)) > maxErrorBytes
	if truncated {
		raw = raw[:maxErrorBytes]
	}
	status := &StatusError{Endpoint: endpoint, Status: resp.StatusCode, Truncated: truncated || readErr != nil}
	var wire struct {
		Message string
		Code    int
		Type    string
	}
	if err := json.Unmarshal(raw, &wire); err == nil {
		status.blockNotFound = messageIsBlockNotFound(wire.Message, "")
		status.notPinned = messageIsNotPinned(wire.Message)
		status.routingAbsent = messageIsRoutingNotFound(wire.Message)
		message := c.redact(strings.TrimSpace(wire.Message))
		status.Truncated = status.Truncated || len(message) > int(maxErrorBytes)
		status.Message = boundedText(message, int(maxErrorBytes))
		status.Code = wire.Code
		typeName := c.redact(strings.TrimSpace(wire.Type))
		status.Truncated = status.Truncated || len(typeName) > maxErrorTypeBytes
		status.Type = boundedText(typeName, maxErrorTypeBytes)
	} else {
		status.blockNotFound = messageIsBlockNotFound(string(raw), "")
		status.notPinned = messageIsNotPinned(string(raw))
		status.routingAbsent = messageIsRoutingNotFound(string(raw))
		message := c.redact(strings.TrimSpace(string(raw)))
		status.Truncated = status.Truncated || len(message) > int(maxErrorBytes)
		status.Message = boundedText(message, int(maxErrorBytes))
	}
	if status.Message == "" {
		status.Message = http.StatusText(resp.StatusCode)
	}
	return status
}

func (c *Client) asNotFound(endpoint string, expected cid.Cid, err error) error {
	var status *StatusError
	if errors.As(err, &status) && !status.Truncated && status.Status != http.StatusNotFound &&
		(status.blockNotFound || messageIsBlockNotFound(status.Message, expected.String())) {
		return &NotFoundError{Endpoint: endpoint, CID: expected.String(), Status: status}
	}
	return err
}

func messageIsBlockNotFound(message, expected string) bool {
	message = strings.ToLower(message)
	expected = strings.ToLower(expected)
	if strings.Contains(message, "command not found") {
		return false
	}
	return strings.Contains(message, "block was not found locally") ||
		strings.Contains(message, "blockstore: block not found") ||
		strings.Contains(message, "no such block") ||
		expected != "" && strings.Contains(message, "could not find "+expected)
}

func (c *Client) protocol(endpoint, format string, args ...any) error {
	problem := c.redact(fmt.Sprintf(format, args...))
	return &ProtocolError{Endpoint: endpoint, Problem: boundedText(problem, maxDiagnosticBytes)}
}

func (c *Client) redact(message string) string {
	if c == nil || c.token == "" {
		return message
	}
	return strings.ReplaceAll(message, c.token, "[REDACTED]")
}
