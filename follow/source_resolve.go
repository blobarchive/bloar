package follow

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"

	"github.com/ipfs/boxo/ipns"
	"github.com/ipfs/boxo/namesys"

	"github.com/blobarchive/bloar/metrics"
	"github.com/blobarchive/bloar/p2p"
	"github.com/blobarchive/bloar/server"
)

// sourceRuntime is the parsed, detached runtime form of one configured source.
// Its sourceRef is the only key used for durable replay state; transport names
// and URLs may change in a later acknowledged roster without resetting it.
type sourceRuntime struct {
	cfg     SourceConfig
	ref     sourceRef
	allowed map[string]struct{}

	name    ipns.Name
	hasIPNS bool
	lookup  namesys.LookupTXTFunc
}

func buildSourceRuntimes(archiveID server.ArchiveID, set *SourceSetConfig, lookup namesys.LookupTXTFunc) ([]*sourceRuntime, error) {
	if set == nil {
		return nil, nil
	}
	if archiveID.IsZero() {
		return nil, errors.New("follow: building source runtimes with an empty archive ID")
	}
	if lookup == nil {
		lookup = net.DefaultResolver.LookupTXT
	}
	runtimes := make([]*sourceRuntime, 0, len(set.Sources))
	for _, configured := range set.Sources {
		source := &sourceRuntime{
			cfg:     configured,
			ref:     sourceRef{archiveID: archiveID, sourceID: configured.ID},
			allowed: make(map[string]struct{}, len(configured.AllowedHeads)),
			lookup:  lookup,
		}
		for _, head := range configured.AllowedHeads {
			source.allowed[head] = struct{}{}
		}
		if configured.IPNS != "" {
			name, err := ipns.NameFromString(configured.IPNS)
			if err != nil {
				return nil, fmt.Errorf("follow: source %q IPNS name: %w", configured.ID, err)
			}
			source.name, source.hasIPNS = name, true
		} else if configured.DNSLink != "" {
			source.hasIPNS = true
		}
		runtimes = append(runtimes, source)
	}
	return runtimes, nil
}

func (s *sourceRuntime) allows(head string) bool {
	_, ok := s.allowed[head]
	return ok
}

type sourceChannelObs struct {
	ref        sourceRef
	ipnsName   ipns.Name
	ipnsSeq    uint64
	hasIPNSSeq bool
}

type sourceResolveResult struct {
	source    *sourceRuntime
	candidate *resolved
	obs       sourceChannelObs
	err       error
}

// resolveSources asks every authorized source concurrently. Each source gets
// one independent document deadline shared by its redundant channels, so a
// dead source costs at most one bounded wait and never serially delays the
// healthy sources behind it.
func (f *Follower) resolveSources(ctx context.Context) []sourceResolveResult {
	results := make(chan sourceResolveResult, len(f.sources))
	for _, source := range f.sources {
		source := source
		go func() {
			sourceCtx, cancel := context.WithTimeout(ctx, docTimeout)
			defer cancel()
			candidate, obs, err := f.resolveSource(sourceCtx, source)
			results <- sourceResolveResult{source: source, candidate: candidate, obs: obs, err: err}
		}()
	}
	out := make([]sourceResolveResult, len(f.sources))
	byID := make(map[string]sourceResolveResult, len(f.sources))
	for range f.sources {
		result := <-results
		byID[result.source.cfg.ID] = result
	}
	// Config normalization sorts sources by ID. Restore that stable order after
	// concurrent completion so diagnostics and later batch construction do not
	// depend on scheduler timing.
	for i, source := range f.sources {
		out[i] = byID[source.cfg.ID]
	}
	return out
}

func (f *Follower) resolveSource(ctx context.Context, source *sourceRuntime) (*resolved, sourceChannelObs, error) {
	type channelResult struct {
		channel   string
		candidate *resolved
		obs       sourceChannelObs
		err       error
	}
	var (
		candidates []*resolved
		obs        sourceChannelObs
		errs       []error
	)
	consider := func(channel string, candidate *resolved, err error) {
		switch {
		case err != nil:
			f.cfg.Metrics.FollowPoll(channel, metrics.OutcomeError)
			errs = append(errs, fmt.Errorf("source %q %s: %w", source.cfg.ID, channel, err))
		case candidate != nil:
			f.cfg.Metrics.FollowPoll(channel, metrics.OutcomeOK)
			candidates = append(candidates, candidate)
		}
	}

	// Redundant channels share one source deadline but do not queue behind one
	// another. A stalled HTTPS server must not consume the entire budget before a
	// healthy IPNS record is even attempted (and vice versa).
	count := 0
	results := make(chan channelResult, 2)
	if source.cfg.URL != "" {
		count++
		go func() {
			channelCtx, cancel := context.WithTimeout(ctx, f.cfg.FetchTimeout)
			defer cancel()
			candidate, err := f.resolveSourceHTTPS(channelCtx, source)
			results <- channelResult{channel: metrics.ChannelHTTPS, candidate: candidate, err: err}
		}()
	}
	if source.hasIPNS {
		count++
		go func() {
			channelCtx, cancel := context.WithTimeout(ctx, f.cfg.FetchTimeout)
			defer cancel()
			name, remember, err := f.resolveSourceIPNSAuthority(channelCtx, source)
			var candidate *resolved
			var channelObs sourceChannelObs
			if err == nil {
				candidate, channelObs, err = f.resolveSourceIPNS(channelCtx, source, name, remember)
			}
			results <- channelResult{channel: metrics.ChannelIPNS, candidate: candidate, obs: channelObs, err: err}
		}()
	}
	byChannel := make(map[string]channelResult, count)
	for range count {
		result := <-results
		byChannel[result.channel] = result
	}
	// Stable processing preserves deterministic error ordering and tie behavior
	// independently of which transport won the scheduler race.
	for _, channel := range []string{metrics.ChannelHTTPS, metrics.ChannelIPNS} {
		result, configured := byChannel[channel]
		if !configured {
			continue
		}
		if result.obs.hasIPNSSeq {
			obs = result.obs
		}
		consider(channel, result.candidate, result.err)
	}

	for _, err := range errs {
		var equivocation *authorityEquivocationError
		if errors.As(err, &equivocation) {
			return nil, obs, errors.Join(errs...)
		}
	}
	var best *resolved
	for _, candidate := range candidates {
		if best == nil {
			best = candidate
			continue
		}
		var err error
		best, err = fresherCandidate(best, candidate)
		if err != nil {
			return nil, obs, errors.Join(append(errs, err)...)
		}
	}
	if best == nil {
		return nil, obs, errors.Join(errs...)
	}
	for _, err := range errs {
		f.log.Warn("a publication source channel failed", "source_id", source.cfg.ID, "err", err)
	}
	f.log.Debug("publication source resolved", "source_id", source.cfg.ID, "channel", best.source,
		"revision", best.revision, "heads", len(best.doc.Heads))
	return best, obs, nil
}

func (f *Follower) resolveSourceHTTPS(ctx context.Context, source *sourceRuntime) (*resolved, error) {
	url := source.cfg.URL + "/bloar/v1/heads"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("building the request for %s: %w", url, err)
	}
	resp, err := f.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("polling %s: %w", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("polling %s: status %d", url, resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxDocBytes+1))
	if err != nil {
		return nil, fmt.Errorf("reading the document from %s: %w", url, err)
	}
	if len(body) > maxDocBytes {
		return nil, fmt.Errorf("the document at %s is larger than %d bytes", url, maxDocBytes)
	}
	block, err := p2p.NewDocumentBlock(body)
	if err != nil {
		return nil, fmt.Errorf("identifying the document from %s: %w", url, err)
	}
	doc, signer, err := f.verifySignature(body, metrics.ChannelHTTPS, source.cfg.PubKey)
	if err != nil {
		return nil, err
	}
	candidate, err := f.parseSourceCandidate(source, doc, metrics.ChannelHTTPS, signer)
	if err != nil {
		return nil, err
	}
	candidate.block = block
	if err := f.sourceFreshnessRefusal(source, candidate); err != nil {
		return nil, err
	}
	return candidate, nil
}

func (f *Follower) resolveSourceIPNSAuthority(ctx context.Context, source *sourceRuntime) (ipns.Name, bool, error) {
	if source.cfg.DNSLink == "" {
		return source.name, false, nil
	}
	name, err := p2p.ResolveDNSLinkName(ctx, source.cfg.DNSLink, source.lookup)
	if err == nil {
		return name, true, nil
	}
	remembered, ok, stateErr := f.state.sourceDelegation(source.ref)
	if stateErr != nil {
		return ipns.Name{}, false, stateErr
	}
	if !ok {
		return ipns.Name{}, false, fmt.Errorf("resolving DNSLink %q: %w; no admitted source delegation is available for fallback", source.cfg.DNSLink, err)
	}
	f.log.Warn("source DNSLink resolution failed; using the last admitted delegation", "source_id", source.cfg.ID,
		"dnslink", source.cfg.DNSLink, "ipns", remembered.name, "err", err)
	return remembered.name, true, nil
}

func (f *Follower) resolveSourceIPNS(ctx context.Context, source *sourceRuntime, name ipns.Name, remember bool) (*resolved, sourceChannelObs, error) {
	cid, seq, err := p2p.Resolve(ctx, f.cfg.Routing, name)
	if err != nil {
		return nil, sourceChannelObs{}, err
	}
	floor, ok, err := f.state.sourceIPNSSeq(source.ref, name)
	if err != nil {
		return nil, sourceChannelObs{}, err
	}
	if ok && seq < floor {
		return nil, sourceChannelObs{}, fmt.Errorf("IPNS record for %s has sequence %d, below source %q floor %d", name, seq, source.cfg.ID, floor)
	}
	block, err := f.docBlock(ctx, cid)
	if err != nil {
		return nil, sourceChannelObs{}, err
	}
	doc, signer, err := f.verifySignature(block.RawData(), metrics.ChannelIPNS, source.cfg.PubKey)
	if err != nil {
		return nil, sourceChannelObs{}, err
	}
	obs := sourceChannelObs{ref: source.ref, ipnsName: name, ipnsSeq: seq, hasIPNSSeq: true}
	candidate, err := f.parseSourceCandidate(source, doc, metrics.ChannelIPNS, signer)
	if err != nil {
		return nil, obs, err
	}
	candidate.block = block
	if remember {
		candidate.delegation = &delegation{name: name, pubkey: append([]byte(nil), signer...)}
	}
	if err := f.sourceFreshnessRefusal(source, candidate); err != nil {
		return nil, obs, err
	}
	return candidate, obs, nil
}

func (f *Follower) parseSourceCandidate(source *sourceRuntime, doc server.Doc, channel string, signer ed25519.PublicKey) (*resolved, error) {
	if err := doc.ValidateContract(); err != nil {
		return nil, fmt.Errorf("the %s document violates the publication contract: %w", channel, err)
	}
	if doc.V != server.LogicalArchiveDocVersion || doc.ArchiveID == nil || doc.Revision == nil {
		return nil, fmt.Errorf("source %q must publish version %d logical-archive documents", source.cfg.ID, server.LogicalArchiveDocVersion)
	}
	for _, entry := range doc.Heads {
		if !source.allows(entry.Name) {
			continue
		}
		want, got := f.expectedKind(entry.Name), entry.EffectiveKind()
		if got != want {
			return nil, fmt.Errorf("source %q head %q declares kind %q, this follower expects %q", source.cfg.ID, entry.Name, got, want)
		}
		if got != server.UnfinalizedMutable {
			continue
		}
		if entry.HandoffHead != f.cfg.ExpectedHandoffs[entry.Name] {
			return nil, fmt.Errorf("source %q mutable head %q names handoff %q, want %q", source.cfg.ID,
				entry.Name, entry.HandoffHead, f.cfg.ExpectedHandoffs[entry.Name])
		}
		limit := f.cfg.MaxMutableWindowSlots[entry.Name]
		if entry.WindowStart == nil || entry.SyncedTo == nil || limit == 0 || *entry.SyncedTo-*entry.WindowStart >= limit {
			return nil, fmt.Errorf("source %q mutable head %q exceeds its configured %d-slot window", source.cfg.ID, entry.Name, limit)
		}
	}
	updatedAt, err := time.Parse(time.RFC3339, doc.UpdatedAt)
	if err != nil {
		return nil, fmt.Errorf("the %s document has an unparseable updated_at %q: %w", channel, doc.UpdatedAt, err)
	}
	if len(signer) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("source %q document has no verified signing authority", source.cfg.ID)
	}
	digest, err := doc.Unsigned.CanonicalDigest()
	if err != nil {
		return nil, fmt.Errorf("canonicalizing source %q publication claim: %w", source.cfg.ID, err)
	}
	candidate := &resolved{
		doc: doc, runtimeSource: source, source: channel, updatedAt: updatedAt, revisioned: true,
		revision: *doc.Revision, digest: digest,
	}
	copy(candidate.authority[:], signer)
	return candidate, nil
}

func (f *Follower) sourceFreshnessRefusal(source *sourceRuntime, candidate *resolved) error {
	if candidate == nil || !candidate.revisioned {
		return fmt.Errorf("source %q candidate has no publication revision", source.cfg.ID)
	}
	floor, ok, err := f.state.sourcePublicationFloor(source.ref)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	switch {
	case candidate.revision < floor.revision:
		return fmt.Errorf("source %q publication revision %d is below its accepted floor %d", source.cfg.ID, candidate.revision, floor.revision)
	case candidate.revision == floor.revision && candidate.digest != floor.digest:
		return &authorityEquivocationError{
			authority: candidate.authority, revision: candidate.revision, first: floor.digest, second: candidate.digest,
		}
	default:
		return nil
	}
}
