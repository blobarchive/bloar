package main

import (
	"context"
	"crypto/ed25519"
	"fmt"
	"log/slog"
	"maps"
	"slices"

	blocks "github.com/ipfs/go-block-format"

	"github.com/blobarchive/bloar/core"
	"github.com/blobarchive/bloar/follow"
	"github.com/blobarchive/bloar/metrics"
	"github.com/blobarchive/bloar/p2p/pointerhint"
	"github.com/blobarchive/bloar/pinning"
	"github.com/blobarchive/bloar/server"
	"github.com/blobarchive/bloar/store"
)

// sortedFollowedHeads is the configured followed-head names in a stable order.
// It is what the readiness gates of the safety boundary are keyed by, so a probe body
// names them the same way every time.
func sortedFollowedHeads(cfg *Config) []string {
	return slices.Sorted(maps.Keys(cfg.Follow.heads()))
}

// setupFollow builds the follower of spec 11.3, or nothing at all when the
// config has no follow block. Nothing at all is every writer, and every method
// on a nil *follow.Follower this file calls is written to mean that.
//
// # What it is wired to, and what it is deliberately not
//
// The exchange is bitswap over the store's own blockstore: that is the serving
// half of spec 11.2, and it must stay the local store. The follower's fetching
// blockstore reads through it as a client. Handing that fetching blockstore to
// the exchange instead would make this node answer a peer's request for a block
// it does not have by asking its peers for it -- a fetch loop with this node in
// the middle, and every follower of this one dragging it into their misses.
//
// GC likewise sweeps the local blockstore (see serve): its mark set is what the
// pins reach and its sweep set is what this node holds, and "what this node
// holds" over a fetching blockstore would be a question with no answer.
func setupFollow(cfg *Config, st *store.Store, cache *core.NodeCache, heads *server.Heads, roots *server.RootStore,
	manifests *server.ManifestStore, rec *pinning.Reconciler, staging *pinning.Staging, mx *metrics.Metrics,
	health *metrics.Health, p2pnet *p2pStack,
	log *slog.Logger) (*follow.Follower, error) {
	if cfg.Follow == nil {
		return nil, nil
	}
	sourceSet, err := cfg.Follow.runtimeSourceSet(cfg.Net)
	if err != nil {
		return nil, fmt.Errorf("bloard: %w", err)
	}
	if p2pnet.exchange == nil {
		// The config validation says this cannot happen; the check is here
		// because the failure it prevents is a follower that silently fetches
		// nothing.
		return nil, fmt.Errorf("bloard: follow is configured but this node has no bitswap exchange")
	}

	var key ed25519.PublicKey
	if sourceSet == nil {
		key, err = cfg.Follow.Key()
		if err != nil {
			return nil, fmt.Errorf("bloard: %w", err)
		}
	}
	expectedArchiveID, err := cfg.Follow.ExpectedArchiveID()
	if err != nil {
		return nil, fmt.Errorf("bloard: %w", err)
	}
	verify, err := follow.ParseVerify(cfg.Follow.Verify)
	if err != nil {
		return nil, err
	}
	var findPointer func(context.Context, pointerhint.Pointer) error
	if p2pnet.pointerFinder != nil {
		findPointer = func(ctx context.Context, pointer pointerhint.Pointer) error {
			_, err := p2pnet.pointerFinder.FindAndDial(ctx, pointer)
			return err
		}
	}
	var onAdmittedDocument func(blocks.Block, server.Doc) error
	var onAdmittedSourceDocument func(blocks.Block, server.Doc, []string) error
	var onServiceabilityChanged func() error
	if p2pnet.pointers != nil {
		if sourceSet == nil {
			onAdmittedDocument = func(block blocks.Block, doc server.Doc) error {
				return p2pnet.pointers.AdmitFollowedDocument(heads, block, doc)
			}
		} else {
			onAdmittedSourceDocument = func(block blocks.Block, doc server.Doc, allowed []string) error {
				return p2pnet.pointers.AdmitFollowedDocument(heads, block, doc, allowed)
			}
		}
		onServiceabilityChanged = func() error { return p2pnet.pointers.RefreshFollowed(heads) }
	}

	policies := make(map[string]pinning.Policy, len(cfg.Follow.Heads))
	expectedKinds := make(map[string]server.HeadKind, len(cfg.Follow.Heads))
	expectedHandoffs := make(map[string]string)
	maxMutableWindowSlots := make(map[string]uint64)
	for name, hc := range cfg.Follow.Heads {
		policy, err := headPolicy(cfg, HeadConfig{Pin: hc.Pin})
		if err != nil {
			return nil, fmt.Errorf("bloard: follow.heads.%s: %w", name, err)
		}
		policies[name] = policy
		expectedKinds[name] = hc.effectiveKind()
		if hc.effectiveKind() == server.UnfinalizedMutable {
			expectedHandoffs[name] = hc.HandoffHead
			maxMutableWindowSlots[name] = hc.MaxWindowSlots
		}
	}
	f, err := follow.New(follow.Config{
		Net:                   cfg.Net,
		ExpectedArchiveID:     expectedArchiveID,
		SourceSet:             sourceSet,
		URL:                   cfg.Follow.URL,
		IPNS:                  cfg.Follow.IPNS,
		DNSLink:               cfg.Follow.DNSLink,
		Routing:               p2pnet.dht,
		PubKey:                key,
		PollInterval:          cfg.Follow.PollInterval,
		FetchTimeout:          cfg.Follow.FetchTimeout,
		Verify:                verify,
		Heads:                 policies,
		ExpectedKinds:         expectedKinds,
		ExpectedHandoffs:      expectedHandoffs,
		OverlayFinalizedHeads: cfg.followedLiveOverlays(),
		MaxMutableWindowSlots: maxMutableWindowSlots,
		Local:                 st.Blocks(),
		Sessions:              p2pnet.exchange,
		Host:                  p2pnet.host,
		FindPointer:           findPointer,
		Registry:              heads,
		Roots:                 roots,
		Manifests:             manifests,
		Reconciler:            rec,
		// The staging pins that keep a GC from sweeping a block the fetch pass
		// made durable before the reconcile that pins it lands (spec 11.3). The
		// same *pinning.Staging ingest takes and GC expires.
		Staging: staging,
		KV:      st.KV(),
		Cache:   cache,
		Metrics: mx,
		// Readiness: the follower calls this the first time it
		// registers each followed head, and it raises both the per-head readiness
		// gate /readyz aggregates and the follow_head_ready metric. Every configured
		// followed head is initialised red below, so the node stays out of the load
		// balancer until each one has served.
		Ready: func(head string, ready bool) {
			health.Set(metrics.FollowedHeadGate(head), ready)
			mx.FollowHeadReady(head, ready)
		},
		OnAdmittedDocument:       onAdmittedDocument,
		OnAdmittedSourceDocument: onAdmittedSourceDocument,
		OnServiceabilityChanged:  onServiceabilityChanged,
		Logger:                   log.With("component", "follow"),
	})
	if err != nil {
		return nil, err
	}
	// The follow_head_ready series are initialised to 0 in setupMetrics (follow-up item
	// 7), so nothing to do here but wire the readiness hook above.
	log.Info("following", "url", cfg.Follow.URL, "ipns", cfg.Follow.IPNS, "dnslink", cfg.Follow.DNSLink,
		"sources", len(cfg.Follow.Sources), "heads", f.Names(), "poll_interval", cfg.Follow.PollInterval,
		"fetch_timeout", cfg.Follow.FetchTimeout, "verify", verify)
	return f, nil
}

// resumeFollowed brings up every followed head this node has already adopted,
// before the listener opens.
//
// Synchronously, and failures are logged rather than fatal. A follower's heads
// are on disk (their roots in the RootStore, their blocks in the blockstore), so
// resuming needs nothing from the network and there is no reason to serve 404s
// for them while the first poll runs. But a head that fails to resume is not a
// reason to refuse to start either: the rest of this node's heads are fine, and
// the poll loop is going to try again in a minute anyway.
func resumeFollowed(ctx context.Context, f *follow.Follower, log *slog.Logger) {
	if f == nil {
		return
	}
	if err := f.Resume(ctx); err != nil {
		log.Error("resuming followed heads: they will be adopted again on the first poll", "err", err)
	}
}
