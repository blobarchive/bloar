package follow

import (
	"context"
	"errors"
	"fmt"

	"github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/go-cid"

	"github.com/blobarchive/bloar/archive"
	"github.com/blobarchive/bloar/server"
)

// classifyCheckpointAgainstDocument is the same proof with the durable claim
// on the left. It is used to show that the selected finalized snapshot covers
// the same-document boundary carried by a mutable source.
func classifyCheckpointAgainstDocument(ctx context.Context, blocks blockstore.Blockstore, archiveID server.ArchiveID,
	head string, checkpoint checkpoint, document server.Doc,
) (ClaimRelation, error) {
	left, ok, err := finalizedCheckpointClaim(ctx, blocks, archiveID, head, checkpoint)
	if err != nil {
		return ClaimRelationInvalid, err
	}
	if !ok {
		return ClaimRelationInvalid, fmt.Errorf("follow: finalized head %q has no durable selected claim", head)
	}
	right, err := loadFinalizedClaim(ctx, blocks, head, document)
	if err != nil {
		return ClaimRelationInvalid, err
	}
	return classifyLoadedFinalizedClaims(ctx, blocks, left, right)
}

// classifyCheckpointAgainstEntry proves a durable finalized checkpoint covers
// one exact finalized line retained from a mutable source document. Unlike
// classifyCheckpointAgainstDocument it needs no reconstructed signature: the
// line was authenticated before it was checkpointed, while this comparison
// revalidates its content-addressed root and append-only relationship after a
// restart.
func classifyCheckpointAgainstEntry(ctx context.Context, blocks blockstore.Blockstore, archiveID server.ArchiveID,
	net, head string, checkpoint checkpoint, entry server.HeadEntry,
) (ClaimRelation, error) {
	left, ok, err := finalizedCheckpointClaim(ctx, blocks, archiveID, head, checkpoint)
	if err != nil {
		return ClaimRelationInvalid, err
	}
	if !ok {
		return ClaimRelationInvalid, fmt.Errorf("follow: finalized head %q has no durable selected claim", head)
	}
	right, err := finalizedEntryClaim(ctx, blocks, archiveID, net, head, entry)
	if err != nil {
		return ClaimRelationInvalid, err
	}
	return classifyLoadedFinalizedClaims(ctx, blocks, left, right)
}

func finalizedEntryClaim(ctx context.Context, blocks blockstore.Blockstore, archiveID server.ArchiveID,
	net, head string, entry server.HeadEntry,
) (*finalizedClaim, error) {
	if archiveID.IsZero() {
		return nil, errors.New("follow: comparing a finalized entry with an empty logical archive ID")
	}
	if net == "" {
		return nil, fmt.Errorf("follow: finalized head %q witness has no network", head)
	}
	if entry.Name != head {
		return nil, fmt.Errorf("follow: finalized head %q witness names %q", head, entry.Name)
	}
	if entry.EffectiveKind() != server.FinalizedMonotonic {
		return nil, fmt.Errorf("follow: finalized head %q witness is %s", head, entry.EffectiveKind())
	}
	if entry.SyncedTo == nil {
		return nil, fmt.Errorf("follow: finalized head %q witness has no coverage", head)
	}
	root, err := cid.Decode(entry.Root)
	if err != nil {
		return nil, fmt.Errorf("follow: finalized head %q witness root: %w", head, err)
	}
	loaded, err := archive.Load(ctx, archive.Config{Blocks: blocks}, root)
	if err != nil {
		return nil, fmt.Errorf("follow: loading finalized head %q witness root %s: %w", head, root, err)
	}
	if err := matchPublishedRoot(net, entry, loaded.Info()); err != nil {
		return nil, err
	}
	manifest, err := parseManifestTip(entry)
	if err != nil {
		return nil, err
	}
	return &finalizedClaim{archiveID: archiveID, net: net, entry: entry, root: root, manifest: manifest}, nil
}

func finalizedCheckpointClaim(ctx context.Context, blocks blockstore.Blockstore, archiveID server.ArchiveID,
	head string, cp checkpoint,
) (*finalizedClaim, bool, error) {
	claim, _, ok, err := finalizedCheckpointClaimWithHead(ctx, blocks, archiveID, head, cp,
		func(ctx context.Context, root cid.Cid) (*archive.Head, error) {
			return archive.Load(ctx, archive.Config{Blocks: blocks}, root)
		})
	return claim, ok, err
}

// finalizedCheckpointClaimWithHead is the checkpoint analogue of
// loadFinalizedClaimWithHead. A source-set continuity check supplies the
// bounded follower loader so both the fresh observation and the durable
// baseline are structurally admitted before either can create conflict
// evidence.
func finalizedCheckpointClaimWithHead(
	ctx context.Context,
	blocks blockstore.Blockstore,
	archiveID server.ArchiveID,
	head string,
	cp checkpoint,
	load func(context.Context, cid.Cid) (*archive.Head, error),
) (*finalizedClaim, *archive.Head, bool, error) {
	if load == nil {
		return nil, nil, false, errors.New("follow: finalized checkpoint loader is nil")
	}
	if archiveID.IsZero() {
		return nil, nil, false, errors.New("follow: comparing a checkpoint with an empty logical archive ID")
	}
	if cp.version == checkpointVersionV4 && cp.archiveID != archiveID {
		return nil, nil, false, fmt.Errorf("follow: checkpoint archive %s differs from configured archive %s", cp.archiveID, archiveID)
	}
	if cp.published == nil && (!cp.selected || !cp.root.Defined()) {
		return nil, nil, false, nil
	}

	var entry server.HeadEntry
	network := cp.net
	var loaded *archive.Head
	if cp.published != nil {
		entry = *cloneCheckpointHeadEntry(cp.published)
		if entry.Name != head {
			return nil, nil, false, fmt.Errorf("follow: checkpoint for head %q retains line %q", head, entry.Name)
		}
	} else {
		var err error
		loaded, err = load(ctx, cp.root)
		if err != nil {
			return nil, nil, false, fmt.Errorf("follow: loading legacy checkpoint root %s for head %q: %w", cp.root, head, err)
		}
		entry = finalizedPublicationEntry(loaded, cp.manifestTip)
		if network == "" {
			network = loaded.Info().Net
		}
	}
	if entry.EffectiveKind() != server.FinalizedMonotonic {
		return nil, nil, false, fmt.Errorf("follow: checkpoint head %q is %s, want finalized-monotonic", head, entry.EffectiveKind())
	}
	if network == "" {
		return nil, nil, false, fmt.Errorf("follow: checkpoint head %q has no network", head)
	}
	root, err := cid.Decode(entry.Root)
	if err != nil {
		return nil, nil, false, fmt.Errorf("follow: checkpoint head %q root: %w", head, err)
	}
	if loaded == nil {
		loaded, err = load(ctx, root)
		if err != nil {
			return nil, nil, false, fmt.Errorf("follow: loading checkpoint head %q root %s: %w", head, root, err)
		}
	} else if loaded.Root() != root {
		return nil, nil, false, fmt.Errorf("follow: legacy checkpoint head %q derived root %s but retained root is %s",
			head, loaded.Root(), root)
	}
	if err := matchPublishedRoot(network, entry, loaded.Info()); err != nil {
		return nil, nil, false, err
	}
	manifest, err := parseManifestTip(entry)
	if err != nil {
		return nil, nil, false, err
	}
	return &finalizedClaim{
		archiveID: archiveID,
		net:       network,
		entry:     entry,
		root:      root,
		manifest:  manifest,
	}, loaded, true, nil
}

func classifyLoadedFinalizedClaims(ctx context.Context, blocks blockstore.Blockstore, left, right *finalizedClaim) (ClaimRelation, error) {
	if !sameClaimIdentity(left, right) {
		return ClaimsIncomparable, nil
	}
	rootRelation, err := compareClaimRoots(ctx, blocks, left, right)
	if err != nil {
		return ClaimRelationInvalid, err
	}
	manifestRelation, err := compareClaimManifests(ctx, blocks, left, right)
	if err != nil {
		return ClaimRelationInvalid, err
	}
	switch {
	case rootRelation == ClaimsEquivalent:
		return manifestRelation, nil
	case manifestRelation == ClaimsEquivalent:
		return rootRelation, nil
	case rootRelation == manifestRelation:
		return rootRelation, nil
	default:
		return ClaimsIncomparable, nil
	}
}
