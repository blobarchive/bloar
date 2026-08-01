package follow

import (
	"context"
	"crypto/ed25519"
	"fmt"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/ipfs/boxo/ipns"
	"github.com/ipfs/go-cid"

	"github.com/blobarchive/bloar/archive"
	"github.com/blobarchive/bloar/server"
)

// SyncHead runs one fetch pass over a head, the pass Poll runs after adoption
// . It lets a test drive the pass in isolation, gated against a
// concurrent transition, without a document.
func SyncHead(f *Follower, ctx context.Context, name string) error { return f.sync(ctx, name) }

// HeadFetched returns the root a head's last completed fetch pass stamped, for the
// stale-stamp assertion of the transition invariant. It reads under f.mu, so it is safe to call
// while a pass or a transition runs.
func HeadFetched(f *Follower, name string) cid.Cid {
	f.mu.Lock()
	defer f.mu.Unlock()
	if hs, ok := f.heads[name]; ok {
		return hs.fetched
	}
	return cid.Undef
}

// ReadIPNSSeqFor reads one name's replay floor for DNSLink rotation tests.
func ReadIPNSSeqFor(kv *pebble.DB, rawName string) (uint64, bool, error) {
	name, err := ipns.NameFromString(rawName)
	if err != nil {
		return 0, false, err
	}
	return (&state{kv: kv}).ipnsSeq(name, false)
}

// ReadDelegation returns the last DNSLink-selected IPNS name and document
// signer. It is test-only evidence for crash-consistent rotation behavior.
func ReadDelegation(kv *pebble.DB) (string, ed25519.PublicKey, bool, error) {
	d, ok, err := (&state{kv: kv}).delegation()
	if err != nil || !ok {
		return "", nil, ok, err
	}
	return d.name.String(), ed25519.PublicKey(d.pubkey), true, nil
}

// HeadAdopted returns the root a head currently serves, for the regression test to
// swap it under the lock while a fetch pass is gated mid-walk.
func HeadAdopted(f *Follower, name string) cid.Cid {
	f.mu.Lock()
	defer f.mu.Unlock()
	if hs, ok := f.heads[name]; ok {
		return hs.adopted
	}
	return cid.Undef
}

// SetHeadAdopted overwrites a head's adopted root under f.mu, standing in for the
// transition (expose) that would move it, so the regression test can make a running
// fetch pass stale deterministically without driving a whole adoption.
func SetHeadAdopted(f *Follower, name string, root cid.Cid) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if hs, ok := f.heads[name]; ok {
		hs.adopted = root
	}
}

// SetHeadFetched overwrites the fetch-completion root for a deterministic sync
// retry test.
func SetHeadFetched(f *Follower, name string, root cid.Cid) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if hs, ok := f.heads[name]; ok {
		hs.fetched = root
	}
}

// SetBeforeSyncCommitHook installs a test hook after a walk completes and
// before its generation check/completion stamp.
func SetBeforeSyncCommitHook(fn func()) { beforeSyncCommitHook = fn }

// RunAfterResumeTicks runs the daemon loop against test-controlled poll
// boundaries. Production constructs the ordinary time.Ticker in
// RunAfterResume; exposing only this inner seam keeps concurrency tests
// deterministic without adding a clock or hook to Config.
func RunAfterResumeTicks(f *Follower, ctx context.Context, ticks <-chan time.Time) error {
	return f.runAfterResume(ctx, ticks)
}

// WriteCheckpoint commits a follower checkpoint the way an adoption would, so an
// external test can stage a crash state -- a committed generation, with or without
// its compatibility mirrors -- without driving the poll that makes one. It is the
// atomic unit the safety boundary/the safety boundary turn on, exposed for tests only.
func WriteCheckpoint(kv *pebble.DB, head string, root cid.Cid, syncedTo uint64, manifestTip cid.Cid, updatedAt time.Time) error {
	return (&state{kv: kv}).putCheckpoint(head, checkpoint{
		root:        root,
		syncedTo:    syncedTo,
		manifestTip: manifestTip,
		updatedAt:   updatedAt,
	})
}

// ReadCheckpoint reads a head's committed checkpoint back for assertions. ok is
// false for a head that has never checkpointed.
func ReadCheckpoint(kv *pebble.DB, head string) (root cid.Cid, syncedTo uint64, manifestTip cid.Cid, updatedAt time.Time, ok bool, err error) {
	cp, ok, err := (&state{kv: kv}).checkpoint(head)
	if err != nil || !ok {
		return cid.Undef, 0, cid.Undef, time.Time{}, ok, err
	}
	return cp.root, cp.syncedTo, cp.manifestTip, cp.updatedAt, true, nil
}

// ReadRevisionedCheckpoint exposes the v2-only generation fields for the
// revision-ordering integration tests. revision==0 identifies a legacy v1
// checkpoint.
func ReadRevisionedCheckpoint(kv *pebble.DB, head string) (kind server.HeadKind, authority [32]byte,
	revision uint64, digest [32]byte, windowStart uint64, ok bool, err error) {
	cp, ok, err := (&state{kv: kv}).checkpoint(head)
	if err != nil || !ok {
		return "", [32]byte{}, 0, [32]byte{}, 0, ok, err
	}
	return cp.kind, cp.authority, cp.revision, cp.digest, cp.windowStart, true, nil
}

// DowngradeCheckpointToProoflessV2 rewrites one selected v3 record using the
// pre-v3 layout. It is a test-only migration seam for proving mutable resume
// fails closed when the exact signed handoff witness is unavailable.
func DowngradeCheckpointToProoflessV2(kv *pebble.DB, head string) error {
	s := &state{kv: kv}
	cp, ok, err := s.checkpoint(head)
	if err != nil {
		return err
	}
	if !ok || !cp.selected || !cp.root.Defined() {
		return fmt.Errorf("follow: cannot downgrade absent/unselected checkpoint %q", head)
	}
	cp.version = checkpointVersionV2
	cp.selected = true
	cp.published = nil
	cp.handoff = nil
	b := kv.NewBatch()
	defer b.Close()
	if err := s.stageCheckpoint(b, head, cp); err != nil {
		return err
	}
	return b.Commit(pebble.Sync)
}

// ReadAuthorityFloor returns one signer key's admitted publication order.
func ReadAuthorityFloor(kv *pebble.DB, authority ed25519.PublicKey) (revision uint64, digest [32]byte, ok bool, err error) {
	var key [32]byte
	if len(authority) != len(key) {
		return 0, [32]byte{}, false, nil
	}
	copy(key[:], authority)
	floor, ok, err := (&state{kv: kv}).authorityFloor(key)
	if err != nil || !ok {
		return 0, [32]byte{}, ok, err
	}
	return floor.revision, floor.digest, true, nil
}

// ReadSourcePublicationFloor returns one source-local replay floor for source
// set cancellation/atomicity assertions.
func ReadSourcePublicationFloor(kv *pebble.DB, archiveID server.ArchiveID, sourceID string) (revision uint64, ok bool, err error) {
	floor, ok, err := (&state{kv: kv}).sourcePublicationFloor(sourceRef{archiveID: archiveID, sourceID: sourceID})
	if err != nil || !ok {
		return 0, ok, err
	}
	return floor.revision, true, nil
}

// ReadUpdatedAt reads the global freshness floor back for assertions -- the fact
// the safety boundary's follow-up guards against a coverage-mismatched document raising.
// ok is false when no floor has been written yet.
func ReadUpdatedAt(kv *pebble.DB) (updatedAt time.Time, ok bool, err error) {
	return (&state{kv: kv}).updatedAt()
}

// ReadIPNSSeq reads the IPNS replay floor back for assertions. ok is
// false when no sequence has been accepted yet.
func ReadIPNSSeq(kv *pebble.DB) (seq uint64, ok bool, err error) {
	s := &state{kv: kv}
	floors, err := s.ipnsFloors()
	if err != nil {
		return 0, false, err
	}
	for _, floor := range floors {
		if !ok || floor.seq > seq {
			seq, ok = floor.seq, true
		}
	}
	if ok {
		return seq, true, nil
	}
	return s.get(keyIPNSSeq)
}

// SetPromotionBeforeCommit installs a hook fired immediately before the writer
// promotion handoff batch commits. A hook returning an error simulates a
// crash after the batch is staged but before it is durable, so a test can prove the
// handoff left the mirrors and checkpoint unchanged and a rerun reconciles. Pass nil
// to clear it.
func SetPromotionBeforeCommit(fn func() error) { promotionBeforeCommit = fn }

// SetAfterResolveHook installs a hook fired in Poll after resolve returns and before
// the transition lock is taken. A test uses it to hold a poll that
// resolved against the old floors while a newer poll commits, then release it into
// the locked admission, reconstructing a concurrent Poll/Poll race deterministically.
// Pass nil to clear it.
func SetAfterResolveHook(fn func()) { afterResolveHook = fn }

// SetBetweenPhasesHook installs a hook fired inside admit between phase 1 (preflight)
// and phase 2 (commit), with the transition lock held. A test uses it to
// pause a poll with its plans decided and race a quarantine against the commit. Pass
// nil to clear it.
func SetBetweenPhasesHook(fn func()) { betweenPhasesHook = fn }

// SetBeforeExposeHook installs a hook fired inside commitEntry after the checkpoint is
// durable and before expose, with the transition lock held. A test uses
// it to hold a poll at the checkpoint-written-but-not-exposed point and race a
// quarantine against the exposure. Pass nil to clear it.
func SetBeforeExposeHook(fn func()) { beforeExposeHook = fn }

// SetBeforeAdmissionCommitHook injects a failure immediately before the atomic
// checkpoint/mirror commit. Pass nil to clear it.
func SetBeforeAdmissionCommitHook(fn func() error) { beforeAdmissionCommitHook = fn }

// QuarantineHead drives the follower's quarantine path, the way the fetch
// or read path does on a verification failure, so a test can race a quarantine
// against a poll deterministically.
func QuarantineHead(f *Follower, name, reason string) error { return f.quarantine(name, "%s", reason) }

// ExposeHead loads root from the local store and exposes it as name, the registry-
// and-headState step a poll's commit runs. A test uses it to put a
// generation in the Registry and headState directly, so it can reconstruct the
// Registry-vs-snapshot skew a fetch pass must not stamp through.
func ExposeHead(f *Follower, ctx context.Context, name string, root cid.Cid) error {
	head, err := archive.Load(ctx, archive.Config{Blocks: f.cfg.Local}, root)
	if err != nil {
		return err
	}
	f.gate.Enter()
	defer f.gate.Leave()
	if err := f.touchGeneration(ctx, name, root, cid.Undef); err != nil {
		return err
	}
	return f.expose(ctx, name, head, cid.Undef, f.expectedKind(name), nil)
}

// HeadQuarantined reports whether a head's in-memory quarantine flag is set, read
// under f.mu.
func HeadQuarantined(f *Follower, name string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if hs, ok := f.heads[name]; ok {
		return hs.quarantined
	}
	return false
}
