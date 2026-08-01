package p2p

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/ipfs/boxo/ipns"
	"github.com/ipfs/boxo/namesys"
	"github.com/ipfs/boxo/path"
	"github.com/miekg/dns"
)

// ValidateDNSLinkDomain validates the exact domain spelling accepted by the
// one-hop resolver without performing a lookup. Configuration adapters use it
// to fail before opening the store or any listener. A trailing root dot is
// valid; surrounding whitespace and path/scheme spellings are not.
func ValidateDNSLinkDomain(domain string) error {
	if domain == "" || strings.TrimSpace(domain) != domain {
		return fmt.Errorf("p2p: DNSLink domain %q must be a non-empty DNS name without surrounding whitespace", domain)
	}
	for _, r := range domain {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' || r == '.' {
			continue
		}
		return fmt.Errorf("p2p: invalid DNSLink domain %q: not a valid DNS name (schemes and paths are not allowed)", domain)
	}
	if _, ok := dns.IsDomainName(domain); !ok {
		return fmt.Errorf("p2p: invalid DNSLink domain %q: not a valid DNS name", domain)
	}
	return nil
}

// ResolveDNSLinkName resolves exactly one DNSLink hop from domain to an IPNS
// name. It deliberately does not resolve the resulting IPNS record: callers
// need the authenticated name and its sequence number to enforce replay floors
// before they admit the document it names.
//
// A DNSLink target must be exactly /ipns/<peer-id>. Targets under /ipfs,
// DNS-to-DNS delegation, and path suffixes are rejected. Keeping this boundary
// narrow makes DNS an explicit signer-delegation mechanism rather than an
// unauthenticated content channel.
func ResolveDNSLinkName(ctx context.Context, domain string, lookup namesys.LookupTXTFunc) (ipns.Name, error) {
	if lookup == nil {
		return ipns.Name{}, errors.New("p2p: DNSLink TXT lookup must not be nil")
	}
	if err := ValidateDNSLinkDomain(domain); err != nil {
		return ipns.Name{}, err
	}
	if err := ctx.Err(); err != nil {
		return ipns.Name{}, fmt.Errorf("p2p: resolving DNSLink domain %q: %w", domain, err)
	}

	request, err := path.NewPath("/ipns/" + domain)
	if err != nil {
		return ipns.Name{}, fmt.Errorf("p2p: invalid DNSLink domain %q: %w", domain, err)
	}
	if segments := request.Segments(); len(segments) != 2 || segments[1] != domain {
		return ipns.Name{}, fmt.Errorf("p2p: invalid DNSLink domain %q: want one DNS name with no path", domain)
	}

	result, err := namesys.NewDNSResolver(lookup).Resolve(ctx, request, namesys.ResolveWithDepth(1))
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ipns.Name{}, fmt.Errorf("p2p: resolving DNSLink domain %q: %w", domain, ctxErr)
	}
	// A valid one-hop answer remains mutable, so Boxo reports the intentional
	// depth boundary while returning the resolved path. No other resolution
	// error is safe to reinterpret as an answer.
	if !errors.Is(err, namesys.ErrResolveRecursion) {
		if err == nil {
			err = errors.New("DNSLink answer is immutable")
		}
		return ipns.Name{}, fmt.Errorf("p2p: resolving DNSLink domain %q: %w", domain, err)
	}
	if result.Path == nil {
		return ipns.Name{}, fmt.Errorf("p2p: resolving DNSLink domain %q returned no path", domain)
	}

	segments := result.Path.Segments()
	if result.Path.Namespace() != path.IPNSNamespace || len(segments) != 2 {
		return ipns.Name{}, fmt.Errorf("p2p: DNSLink domain %q names %q, want /ipns/<peer-id> with no path", domain, result.Path)
	}

	name, err := ipns.NameFromString(segments[1])
	if err != nil {
		return ipns.Name{}, fmt.Errorf("p2p: DNSLink domain %q delegates to invalid IPNS name %q: %w", domain, segments[1], err)
	}
	return name, nil
}
