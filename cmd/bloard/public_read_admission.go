package main

import (
	"fmt"
	"net/netip"
	"strings"

	"golang.org/x/time/rate"

	"github.com/blobarchive/bloar/metrics"
	"github.com/blobarchive/bloar/server"
)

// publicReadLimiterConfig translates the strict YAML surface into the server
// limiter. It validates even an explicitly disabled block so a latent bad
// configuration cannot become active merely by changing enabled to true.
func (c ServerConfig) publicReadLimiterConfig(mx *metrics.Metrics) (*server.PublicReadLimiterConfig, error) {
	cfg := c.PublicReadAdmission
	prefixes, err := parseTrustedProxyCIDRs(cfg.TrustedProxyCIDRs)
	if err != nil {
		return nil, err
	}

	limiter := &server.PublicReadLimiterConfig{
		GlobalRate:       rate.Limit(cfg.GlobalRate),
		GlobalBurst:      cfg.GlobalBurst,
		PerClientRate:    rate.Limit(cfg.ClientRate),
		PerClientBurst:   cfg.ClientBurst,
		MaxClientBuckets: cfg.ClientBuckets,
		ClientBucketTTL:  cfg.ClientBucketTTL,
		ForwardedHeader:  cfg.TrustedProxyHeader,
		TrustedProxies:   prefixes,
	}
	if mx != nil {
		limiter.Observe = func(outcome server.PublicReadAdmissionOutcome, cost int) {
			mx.PublicReadAdmission(outcome.String(), cost)
		}
	}
	if err := server.ValidatePublicReadLimiterConfig(*limiter, c.MaxQueryHashes); err != nil {
		return nil, fmt.Errorf("server.public_read_admission: %w", err)
	}
	if cfg.Enabled != nil && !*cfg.Enabled {
		return nil, nil
	}
	return limiter, nil
}

func (c *Config) validatePublicReadAdmission() error {
	_, err := c.Server.publicReadLimiterConfig(nil)
	return err
}

// parseTrustedProxyCIDRs is intentionally stricter than netip.ParsePrefix:
// configuration must be a whitespace-free canonical network prefix. Rejecting
// host bits prevents `192.0.2.9/24` from silently granting trust to a broader
// network than the text appears to name. Native IPv4 and IPv6 are accepted;
// zones and IPv4-mapped IPv6 are not useful in an HTTP proxy allowlist.
func parseTrustedProxyCIDRs(raw []string) ([]netip.Prefix, error) {
	prefixes := make([]netip.Prefix, 0, len(raw))
	seen := make(map[netip.Prefix]struct{}, len(raw))
	for i, text := range raw {
		if text == "" || strings.TrimSpace(text) != text {
			return nil, fmt.Errorf("server.public_read_admission.trusted_proxy_cidrs[%d] must be a non-empty CIDR without surrounding whitespace", i)
		}
		prefix, err := netip.ParsePrefix(text)
		if err != nil {
			return nil, fmt.Errorf("server.public_read_admission.trusted_proxy_cidrs[%d] %q is not a CIDR: %w", i, text, err)
		}
		if prefix.Addr().Zone() != "" || prefix.Addr().Is4In6() {
			return nil, fmt.Errorf("server.public_read_admission.trusted_proxy_cidrs[%d] %q must be native IPv4 or IPv6 without a zone", i, text)
		}
		if prefix != prefix.Masked() {
			return nil, fmt.Errorf("server.public_read_admission.trusted_proxy_cidrs[%d] %q has host bits set; use canonical network %q", i, text, prefix.Masked())
		}
		if _, duplicate := seen[prefix]; duplicate {
			return nil, fmt.Errorf("server.public_read_admission.trusted_proxy_cidrs[%d] %q is duplicated", i, text)
		}
		seen[prefix] = struct{}{}
		prefixes = append(prefixes, prefix)
	}
	return prefixes, nil
}
