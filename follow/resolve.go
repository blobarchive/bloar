package follow

import (
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/ipfs/boxo/ipns"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"

	"github.com/blobarchive/bloar/metrics"
	"github.com/blobarchive/bloar/p2p"
	"github.com/blobarchive/bloar/p2p/pointerhint"
	"github.com/blobarchive/bloar/server"
)

// resolved is one publication document that authenticated against either the
// configured signer or a DNSLink->IPNS->CID delegation: it decoded, it is this
// version and network, and its signature verifies.
// It is a document CANDIDATE -- whether it is fresh enough to adopt is a separate
// admissibility outcome.
type resolved struct {
	doc server.Doc
	// runtimeSource is non-nil only in acknowledged source-set mode. It is local
	// authorization metadata, never data asserted by the signed document.
	runtimeSource *sourceRuntime
	// block is the exact raw/sha2-256 publication block whose bytes produced
	// doc. HTTPS has no transport CID, so the follower derives it from the
	// response bytes; IPNS additionally proves it equals the CID the signed
	// record named. It remains attached through whole-document admission so
	// only the winning, durably admitted source document can be retained.
	block     blocks.Block
	source    string // "https" or "ipns", for the log
	updatedAt time.Time
	// authority is the verified document signing key. Revision and digest are
	// ordered only inside this authority; neither the transport URL nor the IPNS
	// key is a publication clock.
	authority  [32]byte
	revision   uint64
	digest     [32]byte
	revisioned bool
	// delegation is committed only if this candidate wins and the whole
	// document is admitted. It is nil for direct-IPNS/HTTPS trust.
	delegation *delegation
}

func (r *resolved) publicationFloor() *authorityFloor {
	if r == nil || !r.revisioned {
		return nil
	}
	return &authorityFloor{authority: r.authority, revision: r.revision, digest: r.digest}
}

// authorityEquivocationError is cryptographic evidence that one document
// signing key assigned two canonical claims to the same revision. Poll treats
// it differently from an ordinary stale/malformed candidate: every configured
// mutable head is quarantined because no deterministic generation remains.
type authorityEquivocationError struct {
	authority [32]byte
	revision  uint64
	first     [32]byte
	second    [32]byte
}

func (e *authorityEquivocationError) Error() string {
	return fmt.Sprintf("follow: publication authority %x equivocated at revision %d: canonical digests %x and %x differ",
		e.authority[:8], e.revision, e.first[:8], e.second[:8])
}

// channelObs is the authenticated channel-level facts a poll observed, independent
// of whether any document candidate survives to be adopted (the follow-up hardening,
// the transition invariant). Today it is the IPNS record sequence, captured strictly AFTER the named
// document authenticated against the configured key (the authenticated-IPNS floor hardening: a record naming an
// unauthenticated document must never raise the replay floor). The locked admission
// raises the monotonic-max replay floor from this observation even when the document
// it named was freshness-refused, or lost the freshness contest, or no document was
// adopted at all -- the floor is a fact about the channel, not about which document
// won (see state.setIPNSSeq).
type channelObs struct {
	ipnsName   ipns.Name
	ipnsSeq    uint64
	hasIPNSSeq bool
}

// resolve reads the publication document from every configured channel and
// returns the freshest one worth adopting, or nil if there is none.
//
// # Freshest valid wins
//
// Spec 8.1: "when both are configured, take the freshest document that passes
// signature verification, subject to the no-regression rule". The two channels
// fail in opposite directions and that is the whole reason to have both. HTTPS
// is as fresh as the writer's last mutation and as available as the writer's
// web server: it goes away when the writer does. IPNS is as available as the
// DHT and as fresh as the last republish: it survives the writer, and can hand
// back a record from hours ago that is still valid and still signed.
//
// So both are asked and checked independently. Two legacy answers retain the
// original updated_at ordering, including across legacy DNS signer rotation. Two
// revisioned answers from one authority use only revision and canonical digest;
// their updated_at is diagnostic. If either answer is revisioned, different
// authorities have no comparable clock or counter. The one ordered transition is
// an authenticated DNSLink handoff: its IPNS document supersedes the HTTPS
// document authenticated by the previously remembered signer. Every other such
// cross-authority pair is refused as incomparable. An ordinary error on one
// channel is not an error -- it is the other channel's turn -- but equivocation
// is never masked, and an error on all channels is surfaced because a follower
// that cannot resolve anything should say so rather than look healthy.
func (f *Follower) resolve(ctx context.Context) (*resolved, channelObs, error) {
	var (
		candidates []*resolved
		obs        channelObs
		errs       []error
	)
	// consider is also where a poll is counted, one sample per configured
	// channel. It is here rather than at the HTTP client because this is the
	// point at which what a channel gave us has been judged: a document that
	// arrived with a 200 and then failed its signature check is an error here
	// and a success to any transport-level counter, and the IPNS channel has no
	// transport to instrument at all.
	consider := func(channel string, r *resolved, err error) {
		switch {
		case err != nil:
			f.cfg.Metrics.FollowPoll(channel, metrics.OutcomeError)
			errs = append(errs, err)
		case r == nil:
			// Not reachable today -- every path through both resolvers returns a
			// document or an error -- and counted as neither rather than as a
			// success if it ever becomes reachable.
		default:
			f.cfg.Metrics.FollowPoll(channel, metrics.OutcomeOK)
			candidates = append(candidates, r)
		}
	}
	if f.cfg.URL != "" {
		r, err := f.resolveHTTPS(ctx)
		consider(metrics.ChannelHTTPS, r, err)
	}
	if f.hasIPNS {
		// The IPNS channel returns its authenticated observation (the record sequence)
		// separately from its document candidate, so a candidate that is freshness-
		// refused here does not discard the observation. obs is kept
		// whenever a record authenticated, whether or not it yields an adoptable
		// document; the locked admission raises the replay floor from it regardless.
		name, signer, remember, nameErr := f.resolveIPNSAuthority(ctx)
		var r *resolved
		var o channelObs
		var err error
		if nameErr != nil {
			err = nameErr
		} else {
			r, o, err = f.resolveIPNS(ctx, name, signer, remember)
		}
		if o.hasIPNSSeq {
			obs = o
		}
		consider(metrics.ChannelIPNS, r, err)
	}

	// Equivocation against an already-admitted floor is not a lossy-channel
	// failure which the other channel may mask. Preserve it for Poll's quarantine
	// path even if another candidate resolved successfully.
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
		// No adoptable document this poll. That is not a failure -- a writer with
		// nothing to say republishes the same document forever -- and it does not
		// discard obs: an authenticated IPNS record still raises the replay floor under
		// the lock even when its document was old or refused.
		return nil, obs, errors.Join(errs...)
	}
	// Errors from the channel that lost are logged rather than returned: the
	// poll succeeded, and a follower that reported an unreachable IPNS while
	// adopting a fresh document over HTTPS would be crying wolf on its own
	// redundancy.
	for _, err := range errs {
		f.log.Warn("a publication channel failed", "err", err)
	}
	attrs := []any{"source", best.source, "updated_at", best.updatedAt, "heads", len(best.doc.Heads)}
	if best.revisioned {
		attrs = append(attrs, "revision", best.revision, "authority", fmt.Sprintf("%x", best.authority[:8]))
	}
	f.log.Debug("publication document resolved", attrs...)
	return best, obs, nil
}

// fresherCandidate selects two authenticated channel answers. Two revisionless
// legacy documents retain updated_at ordering exactly, including across signer
// rotation. Revisioned answers from one authority use only revision (with
// equal-revision digest conflict detection). If either candidate is revisioned,
// an authenticated DNSLink-selected IPNS candidate is the only explicit
// cross-authority order and wins over the remembered old HTTPS signer. No clock
// comparison can order any other such pair, so it fails closed.
func fresherCandidate(a, b *resolved) (*resolved, error) {
	if a.authority != b.authority && (a.revisioned || b.revisioned) {
		switch {
		case a.delegation != nil && b.delegation == nil:
			return a, nil
		case b.delegation != nil && a.delegation == nil:
			return b, nil
		default:
			return nil, fmt.Errorf("follow: publication candidates from signing authorities %x and %x are incomparable: "+
				"signer-local revisions and updated_at cannot order different authorities without an authenticated DNSLink handoff",
				a.authority[:8], b.authority[:8])
		}
	}
	switch {
	case a.revisioned && b.revisioned:
		switch {
		case b.revision > a.revision:
			return b, nil
		case b.revision < a.revision:
			return a, nil
		case b.digest != a.digest:
			return nil, &authorityEquivocationError{
				authority: a.authority, revision: a.revision, first: a.digest, second: b.digest,
			}
		default:
			return a, nil
		}
	case a.revisioned:
		return a, nil
	case b.revisioned:
		return b, nil
	}
	if b.updatedAt.After(a.updatedAt) {
		return b, nil
	}
	return a, nil
}

// resolveIPNSAuthority returns the name and signer policy for this poll. A DNS
// failure may use only a previously admitted, crash-consistent delegation; HTTPS
// can never create or rotate that state.
func (f *Follower) resolveIPNSAuthority(ctx context.Context) (ipns.Name, ed25519.PublicKey, bool, error) {
	if f.dnsLink == "" {
		return f.name, f.cfg.PubKey, false, nil
	}
	dnsCtx, cancel := context.WithTimeout(ctx, docTimeout)
	defer cancel()
	name, err := p2p.ResolveDNSLinkName(dnsCtx, f.dnsLink, f.lookup)
	if err == nil {
		return name, f.cfg.PubKey, true, nil
	}
	remembered, ok, stateErr := f.state.delegation()
	if stateErr != nil {
		return ipns.Name{}, nil, false, stateErr
	}
	if !ok {
		return ipns.Name{}, nil, false, fmt.Errorf("follow: resolving DNSLink %q: %w; no admitted delegation is available for fallback", f.dnsLink, err)
	}
	signer := ed25519.PublicKey(remembered.pubkey)
	if len(f.cfg.PubKey) == ed25519.PublicKeySize {
		signer = f.cfg.PubKey
	}
	f.log.Warn("DNSLink resolution failed; using the last admitted delegation", "dnslink", f.dnsLink,
		"ipns", remembered.name, "err", err)
	return remembered.name, signer, true, nil
}

func (f *Follower) httpsSigner() (ed25519.PublicKey, error) {
	if len(f.cfg.PubKey) == ed25519.PublicKeySize {
		return f.cfg.PubKey, nil
	}
	remembered, ok, err := f.state.delegation()
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, errors.New("follow: HTTPS cannot bootstrap an unpinned DNSLink signer; no document has yet been admitted through DNSLink and IPNS")
	}
	return ed25519.PublicKey(remembered.pubkey), nil
}

// resolveHTTPS polls GET {url}/bloar/v1/heads (spec 8).
func (f *Follower) resolveHTTPS(ctx context.Context) (*resolved, error) {
	ctx, cancel := context.WithTimeout(ctx, docTimeout)
	defer cancel()

	url := strings.TrimSuffix(f.cfg.URL, "/") + "/bloar/v1/heads"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("follow: building the request for %s: %w", url, err)
	}
	resp, err := f.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("follow: polling %s: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("follow: polling %s: status %d", url, resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxDocBytes+1))
	if err != nil {
		return nil, fmt.Errorf("follow: reading the document from %s: %w", url, err)
	}
	if len(body) > maxDocBytes {
		return nil, fmt.Errorf("follow: the document at %s is larger than %d bytes", url, maxDocBytes)
	}
	blk, err := p2p.NewDocumentBlock(body)
	if err != nil {
		return nil, fmt.Errorf("follow: identifying the document from %s: %w", url, err)
	}
	signer, err := f.httpsSigner()
	if err != nil {
		return nil, err
	}
	doc, authenticatedSigner, err := f.verifySignature(body, "https", signer)
	if err != nil {
		return nil, err
	}
	r, err := f.parseCandidate(doc, "https", authenticatedSigner)
	if err != nil {
		return nil, err
	}
	r.block = blk
	if err := f.freshnessRefusal(r); err != nil {
		return nil, err
	}
	return r, nil
}

// resolveIPNS resolves the name and fetches the document block it points at (spec
// 8.1). It returns two separable outcomes: the document candidate (nil
// when the named document is old or malformed -- an unparseable updated_at), and the
// authenticated channel observation (the record sequence), which is captured strictly
// at "signature verified" and survives even when the candidate does not.
func (f *Follower) resolveIPNS(ctx context.Context, name ipns.Name, signer ed25519.PublicKey, remember bool) (*resolved, channelObs, error) {
	resolveCtx, cancel := context.WithTimeout(ctx, docTimeout)
	c, seq, err := p2p.Resolve(resolveCtx, f.cfg.Routing, name)
	cancel()
	if err != nil {
		return nil, channelObs{}, fmt.Errorf("follow: %w", err)
	}

	// A best-effort early reject, for liveness only: a record whose sequence is
	// already below the floor is a replay, and there is no reason to fetch the block
	// it names -- and it is not yet authenticated, so it yields no observation. This is
	// NOT the authoritative admission decision -- the floor read here is outside the
	// transition lock and a concurrent poll may move it -- so the binding
	// read/compare/raise is made under the lock in admit, as one guarded RMW (audit
	// the follow-up hardening, the transition invariant). Here it only avoids a pointless fetch.
	floor, ok, err := f.state.ipnsSeq(name, f.dnsLink == "")
	if err != nil {
		return nil, channelObs{}, err
	}
	if ok && seq < floor {
		return nil, channelObs{}, fmt.Errorf("follow: IPNS record for %s has sequence %d, below the accepted floor %d; "+
			"refusing a replayed record (spec 11.3)", name, seq, floor)
	}

	blk, err := f.docBlock(ctx, c)
	if err != nil {
		return nil, channelObs{}, err
	}
	// Verify the named document's signature against the configured key. The sequence
	// becomes a valid channel observation at EXACTLY this point and no earlier: raising
	// the floor on the record's transport signature alone would let anyone holding the
	// libp2p peer key, which is not the document-signing key (spec 11.5), mint a record
	// with an enormous sequence naming garbage and pin the floor above every real record
	// forever -- a permanent DoS from a key whose compromise is meant to end with its
	// window (the authenticated-IPNS floor hardening). And no later either: capturing it here, before the candidate is
	// parsed, is what keeps a document that is correctly signed but malformed (an
	// unparseable updated_at) from discarding a sequence the signature already earned
	//.
	doc, authenticatedSigner, err := f.verifySignature(blk.RawData(), "ipns", signer)
	if err != nil {
		return nil, channelObs{}, err // not from the configured key: the sequence is not an observation.
	}
	obs := channelObs{ipnsName: name, ipnsSeq: seq, hasIPNSSeq: true}

	// From here on any refusal is candidate-only and returns obs alongside the error, so
	// the locked admission still raises the replay floor from the authenticated record.
	// parseCandidate is a malformation check (a bad updated_at); freshnessRefusal is the
	// separable replay check (a document older than the floor). Both leave obs intact.
	r, err := f.parseCandidate(doc, "ipns", authenticatedSigner)
	if err != nil {
		return nil, obs, err
	}
	r.block = blk
	// Mark a DNSLink-selected candidate before freshness and dual-channel
	// selection. The delegation is still only committed after this candidate wins
	// and its complete document passes admission; at this point it records why two
	// otherwise incomparable signer authorities have an explicit order.
	if remember {
		r.delegation = &delegation{name: name, pubkey: append([]byte(nil), authenticatedSigner...)}
	}
	if err := f.freshnessRefusal(r); err != nil {
		return nil, obs, err
	}
	return r, obs, nil
}

// docBlock fetches a publication document block over bitswap.
//
// Through its own session, which never writes what it gets to the local
// blockstore. A document block belongs to no head and no pin policy, so a
// document cached locally is a block GC sweeps -- once per publication, forever,
// for a block that was only ever a kilobyte in flight. p2p.DocBlockstore is the
// writer's side of the same argument.
func (f *Follower) docBlock(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	if c.Prefix().Codec != cid.Raw {
		// Spec 8.1 stores the canonical JSON as a raw block. Anything else is a
		// name that resolves to something this archive did not publish.
		return nil, fmt.Errorf("follow: IPNS record names %s, which is not a raw block", c)
	}
	blk, err := f.fetchDocumentBlock(ctx, c)
	if err != nil && f.cfg.FindPointer != nil && ctx.Err() == nil {
		findErr := f.cfg.FindPointer(ctx, pointerhint.Pointer{Kind: pointerhint.Document, CID: c})
		if findErr != nil {
			return nil, &p2p.FetchError{Cid: c, Err: errors.Join(err,
				fmt.Errorf("finding exact publication-document pointer: %w", findErr))}
		}
		// Use a fresh per-fetch timeout after discovery. Reusing the first
		// attempt's context would make a timeout-triggered lookup succeed only to
		// have the retry fail immediately on the already-expired deadline.
		blk, err = f.fetchDocumentBlock(ctx, c)
	}
	if err != nil {
		return nil, &p2p.FetchError{Cid: c, Err: err}
	}
	if blk == nil || !blk.Cid().Equals(c) {
		return nil, &p2p.FetchError{Cid: c, Err: errors.New("document fetch returned the wrong CID")}
	}
	return verifiedDocumentBlock(c, blk.RawData())
}

func (f *Follower) fetchDocumentBlock(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	fetchCtx, cancel := context.WithTimeout(ctx, f.cfg.FetchTimeout)
	defer cancel()
	if f.cfg.DocumentBlock != nil {
		return f.cfg.DocumentBlock(fetchCtx, c)
	}

	f.docOnce.Do(func() { f.docSess = f.cfg.Sessions.NewSession(f.sessCtx) })
	return f.docSess.GetBlock(fetchCtx, c)
}

// verifiedDocumentBlock independently hashes fetched bytes instead of trusting
// a Block implementation's reported CID. Bitswap normally supplies this
// guarantee, but Config.DocumentBlock is an injection boundary and an exact
// source-document CID is the capability later provider hints advertise.
func verifiedDocumentBlock(want cid.Cid, raw []byte) (blocks.Block, error) {
	blk, err := p2p.NewDocumentBlock(raw)
	if err != nil {
		return nil, &p2p.FetchError{Cid: want, Err: err}
	}
	if !blk.Cid().Equals(want) {
		return nil, &p2p.FetchError{Cid: want, Err: fmt.Errorf("document bytes hash to %s, not the record CID", blk.Cid())}
	}
	return blk, nil
}

// verifySignature decodes and authenticates a document's bytes up to and INCLUDING
// the signature check (spec 8): is this a document, is it this version and network,
// and is it self-signed by a valid ed25519 key? When expected is nonempty it must
// also be that pinned/previously delegated key. It stops exactly at "signature verified" --
// the point at which an IPNS record's sequence becomes a trustworthy channel
// observation. It deliberately does no more: parsing the
// candidate (updated_at) is parseCandidate's job, because a malformation found AFTER
// the signature verifies must not discard an observation the signature has already
// earned. Nothing is trusted before this returns -- including the multiaddrs, which
// is why dialling happens later: an unverified document telling a follower who to
// connect to is an unverified document choosing its peers.
func (f *Follower) verifySignature(body []byte, source string, expected ed25519.PublicKey) (server.Doc, ed25519.PublicKey, error) {
	var doc server.Doc
	if err := json.Unmarshal(body, &doc); err != nil {
		return server.Doc{}, nil, fmt.Errorf("follow: the %s document does not decode: %w", source, err)
	}
	if !server.SupportedDocVersion(doc.V) {
		// Spec 15: readers MUST reject unknown major versions. A future document is
		// not a supported document with ignorable extra fields: its signed contract
		// is one this build cannot reproduce.
		return server.Doc{}, nil, fmt.Errorf("follow: the %s document is version %d, this build follows versions %d, %d, or legacy %d",
			source, doc.V, server.LogicalArchiveDocVersion, server.DocVersion, server.LegacyDocVersion)
	}
	if doc.Net != f.cfg.Net {
		return server.Doc{}, nil, fmt.Errorf("follow: the %s document is for net %q, this node is on net %q", source, doc.Net, f.cfg.Net)
	}

	// The key first, then the signature. Verify checks the document against the
	// key the document carries, which alone is only self-consistency. A nonempty
	// expected key pins it here. With expected empty, the caller has already
	// authenticated the exact bytes through DNSLink -> IPNS signature -> CID and
	// this signature establishes the delegated document key it may commit.
	pub, err := hex.DecodeString(doc.Pubkey)
	if err != nil {
		return server.Doc{}, nil, fmt.Errorf("follow: the %s document has an undecodable pubkey: %w", source, err)
	}
	if len(pub) != ed25519.PublicKeySize {
		return server.Doc{}, nil, fmt.Errorf("follow: the %s document pubkey is %d bytes, want %d", source, len(pub), ed25519.PublicKeySize)
	}
	authenticated := ed25519.PublicKey(pub)
	if len(expected) != 0 && !authenticated.Equal(expected) {
		return server.Doc{}, nil, fmt.Errorf("follow: the %s document is signed by %s, this node follows %x; refusing to adopt a "+
			"document from a key it does not follow", source, shortKey(doc.Pubkey), expected)
	}
	if err := doc.Verify(); err != nil {
		return server.Doc{}, nil, fmt.Errorf("follow: the %s document does not verify: %w", source, err)
	}
	if expectedID := f.cfg.ExpectedArchiveID; expectedID != nil {
		if doc.V != server.LogicalArchiveDocVersion || doc.ArchiveID == nil {
			return server.Doc{}, nil, fmt.Errorf("follow: the %s document is version %d and has no signed logical archive identity; this node follows archive %s",
				source, doc.V, expectedID)
		}
		if *doc.ArchiveID != *expectedID {
			return server.Doc{}, nil, fmt.Errorf("follow: the %s document is for logical archive %s, this node follows archive %s",
				source, doc.ArchiveID, expectedID)
		}
	}
	return doc, authenticated, nil
}

// parseCandidate turns a signature-verified document into an adoptable candidate by
// parsing its updated_at (spec 8). It runs only after verifySignature, so a failure
// here is candidate malformation the signature already vouched for the origin of: the
// IPNS caller keeps the channel observation it captured at "signature verified" and
// only the candidate is discarded.
func (f *Follower) parseCandidate(doc server.Doc, source string, signer ed25519.PublicKey) (*resolved, error) {
	if err := doc.ValidateContract(); err != nil {
		return nil, fmt.Errorf("follow: the %s document violates the publication contract: %w", source, err)
	}
	if err := f.validateExpectedKinds(doc); err != nil {
		return nil, fmt.Errorf("follow: the %s document: %w", source, err)
	}
	updatedAt, err := time.Parse(time.RFC3339, doc.UpdatedAt)
	if err != nil {
		return nil, fmt.Errorf("follow: the %s document has an unparseable updated_at %q: %w", source, doc.UpdatedAt, err)
	}
	if len(signer) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("follow: the %s document has no verified signing authority", source)
	}
	r := &resolved{doc: doc, source: source, updatedAt: updatedAt}
	copy(r.authority[:], signer)
	if doc.Revision != nil {
		r.revisioned = true
		r.revision = *doc.Revision
		r.digest, err = doc.Unsigned.CanonicalDigest()
		if err != nil {
			return nil, fmt.Errorf("follow: canonicalizing the %s publication claim: %w", source, err)
		}
	}
	return r, nil
}

// freshnessRefusal applies the document-ordering floor: authority-local revision
// and canonical digest for a revisioned candidate, otherwise the exact legacy
// updated_at rule. It is a best-effort resolution-time check; admit repeats it
// under the transition lock. Like parseCandidate it runs only after signature
// verification, and an IPNS caller retains the independently authenticated record
// sequence even when this candidate is replay, downgrade, or equivocation.
func (f *Follower) freshnessRefusal(candidate *resolved) error {
	floor, revisioned, err := f.state.authorityFloor(candidate.authority)
	if err != nil {
		return err
	}
	if candidate.revisioned {
		if !revisioned {
			return nil
		}
		switch {
		case candidate.revision < floor.revision:
			return fmt.Errorf("follow: the %s document has publication revision %d, below authority %x's accepted floor %d; refusing a replayed document",
				candidate.source, candidate.revision, candidate.authority[:8], floor.revision)
		case candidate.revision == floor.revision && candidate.digest != floor.digest:
			return &authorityEquivocationError{
				authority: candidate.authority, revision: candidate.revision, first: floor.digest, second: candidate.digest,
			}
		default:
			return nil
		}
	}
	if revisioned {
		return fmt.Errorf("follow: the %s document from authority %x omits publication revision after revision %d was admitted; refusing a revisioned-to-legacy downgrade",
			candidate.source, candidate.authority[:8], floor.revision)
	}
	legacyFloor, ok, err := f.state.updatedAt()
	if err != nil {
		return err
	}
	if ok && candidate.updatedAt.Before(legacyFloor) {
		return fmt.Errorf("follow: the %s document is dated %s, before the accepted floor %s; refusing a "+
			"replayed document (spec 11.3)", candidate.source, candidate.updatedAt.UTC().Format(time.RFC3339), legacyFloor.Format(time.RFC3339))
	}
	return nil
}

// shortKey renders an unwanted key for a log line, without printing 64
// characters of hex nobody is going to read to the end of.
func shortKey(hexKey string) string {
	if len(hexKey) <= 16 {
		return hexKey
	}
	return hexKey[:16] + "..."
}
