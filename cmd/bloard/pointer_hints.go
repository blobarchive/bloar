package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"sync"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"

	"github.com/blobarchive/bloar/archive"
	"github.com/blobarchive/bloar/p2p"
	"github.com/blobarchive/bloar/p2p/pointerhint"
	"github.com/blobarchive/bloar/server"
)

// pointerSchedule is the one process-wide exact-pointer provider schedule.
// Keeping this seam narrower than *pointerhint.Coordinator makes the daemon's
// ownership and atomic-composition rules directly testable.
type pointerSchedule interface {
	ReplaceAllWithDocuments(map[string]pointerhint.Set, []cid.Cid) error
}

// verifiedDocumentState is the trust-gated serving cache. The concrete
// implementation is *pointerhint.VerifiedDocumentStore; this interface exists
// only so pointer state does not gain access to unrelated blockstore methods.
// Staging preserves the old protected set until the matching schedule swap has
// succeeded, and ReplaceCurrentDocuments commits (or rolls back) that swap.
type verifiedDocumentState interface {
	StageCurrentAfterVerification([]blocks.Block) error
	ReplaceCurrentDocuments([]cid.Cid) error
}

type headDocumentReader interface {
	HeadDoc(string) ([]byte, bool)
	Get(string) (*archive.Head, bool)
}

type pointerStateConfig struct {
	Net string

	WrittenHeads  map[string]struct{}
	FollowedHeads map[string]struct{}

	// LocalSigner is the writer signing key's public half. A nil key is a
	// deliberate unsigned writer: its trusted in-process callback may update
	// root/manifest pointers, but its document is never retained or advertised.
	LocalSigner ed25519.PublicKey

	Coordinator       pointerSchedule
	VerifiedDocuments verifiedDocumentState

	// OnWorkerError observes a rejected asynchronous local document. It runs on
	// the worker, never on server.Heads' mutation path.
	OnWorkerError func(error)
}

// pointerState owns the daemon's two pointer sources. Written heads and
// followed heads are never allowed to overwrite one another: each transition
// constructs their complete union and gives it to Coordinator.ReplaceAll once.
// The state maps change only if that atomic replacement succeeds.
type pointerState struct {
	net           string
	writtenNames  map[string]struct{}
	followedNames map[string]struct{}
	localSigner   ed25519.PublicKey
	coordinator   pointerSchedule
	verifiedDocs  verifiedDocumentState
	onWorkerError func(error)

	mu       sync.Mutex
	written  map[string]pointerHeadState
	followed map[string]pointerHeadState
	// localPublication is the exact signed document this daemon renders and may
	// publish under its own IPNS name. A follower also retains the distinct
	// upstream source document in followed; both are current trust paths and
	// must remain independently discoverable.
	localPublication blocks.Block

	// server.Heads invokes OnDoc while holding its mutation lock. That callback
	// may therefore do only bounded in-memory work: copy the latest bytes,
	// replace any older pending value, and make a non-blocking wake attempt.
	pendingMu sync.Mutex
	pending   []byte
	wake      chan struct{}
	closed    bool

	lifecycleMu sync.Mutex
	started     bool
	cancel      context.CancelFunc
	done        chan struct{}
}

// pointerHeadState keeps the exact authenticated source block beside the set
// which names it. This is intentionally not derivable from a history LRU: a
// quiet current follower document must stay protected while a busy local writer
// rotates many newer documents.
type pointerHeadState struct {
	set      pointerhint.Set
	document blocks.Block
	entry    server.HeadEntry
}

func newPointerState(cfg pointerStateConfig) (*pointerState, error) {
	if cfg.Net == "" {
		return nil, errors.New("bloard: pointer state network must not be empty")
	}
	if cfg.Coordinator == nil {
		return nil, errors.New("bloard: pointer state coordinator must not be nil")
	}
	if cfg.VerifiedDocuments == nil {
		return nil, errors.New("bloard: pointer state verified document store must not be nil")
	}
	if len(cfg.LocalSigner) != 0 && len(cfg.LocalSigner) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("bloard: local pointer-document signer is %d bytes, want %d", len(cfg.LocalSigner), ed25519.PublicKeySize)
	}

	written := cloneNameSet(cfg.WrittenHeads)
	followed := cloneNameSet(cfg.FollowedHeads)
	if len(written)+len(followed) > pointerhint.MaxCoordinatorHeads {
		return nil, fmt.Errorf("bloard: pointer state has %d configured heads, exceeds limit %d",
			len(written)+len(followed), pointerhint.MaxCoordinatorHeads)
	}
	for name := range written {
		if _, duplicate := followed[name]; duplicate {
			return nil, fmt.Errorf("bloard: head %q cannot be both written and followed pointer state", name)
		}
	}

	return &pointerState{
		net:           cfg.Net,
		writtenNames:  written,
		followedNames: followed,
		localSigner:   append(ed25519.PublicKey(nil), cfg.LocalSigner...),
		coordinator:   cfg.Coordinator,
		verifiedDocs:  cfg.VerifiedDocuments,
		onWorkerError: cfg.OnWorkerError,
		written:       make(map[string]pointerHeadState),
		followed:      make(map[string]pointerHeadState),
		wake:          make(chan struct{}, 1),
	}, nil
}

func cloneNameSet(source map[string]struct{}) map[string]struct{} {
	result := make(map[string]struct{}, len(source))
	for name := range source {
		result[name] = struct{}{}
	}
	return result
}

// NotifyLocalDocument is suitable for server.HeadsConfig.OnDoc. It copies the
// bytes because ownership remains with Heads, coalesces them to the newest
// publication, and never waits for the pointer worker or a network operation.
func (s *pointerState) NotifyLocalDocument(raw []byte) {
	if s == nil {
		return
	}
	owned := append([]byte(nil), raw...)
	s.pendingMu.Lock()
	if s.closed {
		s.pendingMu.Unlock()
		return
	}
	s.pending = owned
	s.pendingMu.Unlock()
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// Start launches the local-document worker. Notifications made before Start
// remain coalesced and are processed once the worker begins.
func (s *pointerState) Start(parent context.Context) error {
	if s == nil {
		return nil
	}
	if parent == nil {
		return errors.New("bloard: pointer state start context must not be nil")
	}

	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	if s.started {
		return nil
	}
	s.pendingMu.Lock()
	if s.closed {
		s.pendingMu.Unlock()
		return errors.New("bloard: pointer state is closed")
	}
	ctx, cancel := context.WithCancel(parent)
	s.cancel = cancel
	s.done = make(chan struct{})
	s.started = true
	hasPending := len(s.pending) != 0
	s.pendingMu.Unlock()
	if hasPending {
		select {
		case s.wake <- struct{}{}:
		default:
		}
	}
	go s.run(ctx)
	return nil
}

func (s *pointerState) run(ctx context.Context) {
	defer close(s.done)
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.wake:
		}

		raw := s.takePending()
		if raw == nil {
			continue
		}
		if err := s.admitLocalDocument(raw); err != nil && s.onWorkerError != nil {
			s.onWorkerError(err)
		}
	}
}

func (s *pointerState) takePending() []byte {
	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()
	raw := s.pending
	s.pending = nil
	return raw
}

// Close stops the worker and makes future OnDoc notifications harmless. It is
// safe to call before Start and is idempotent.
func (s *pointerState) Close() {
	if s == nil {
		return
	}
	s.lifecycleMu.Lock()
	s.pendingMu.Lock()
	if s.closed {
		s.pendingMu.Unlock()
		s.lifecycleMu.Unlock()
		return
	}
	s.closed = true
	s.pending = nil
	s.pendingMu.Unlock()
	cancel, done := s.cancel, s.done
	s.lifecycleMu.Unlock()
	if cancel != nil {
		cancel()
	}
	if done != nil {
		<-done
	}
}

// admitLocalDocument authenticates the exact publication produced by this
// process and replaces only the written half of pointer state. It is run by the
// worker, not by Heads.OnDoc.
func (s *pointerState) admitLocalDocument(raw []byte) error {
	doc, err := decodePointerDocument(raw)
	if err != nil {
		return fmt.Errorf("bloard: local pointer document: %w", err)
	}
	if doc.Net != s.net {
		return fmt.Errorf("bloard: local pointer document is for net %q, want %q", doc.Net, s.net)
	}
	if err := doc.ValidateContract(); err != nil {
		return fmt.Errorf("bloard: local pointer document contract: %w", err)
	}

	document := cid.Undef
	var block blocks.Block
	if len(s.localSigner) != 0 {
		if err := verifyExpectedDocumentSigner(doc, s.localSigner); err != nil {
			return fmt.Errorf("bloard: local pointer document signer: %w", err)
		}
		block, err = p2p.NewDocumentBlock(raw)
		if err != nil {
			return err
		}
		document = block.Cid()
	}

	next, err := statesFromDocument(doc, s.writtenNames, document, block)
	if err != nil {
		return fmt.Errorf("bloard: local pointer document: %w", err)
	}
	return s.replaceWritten(next, block)
}

// AdmitFollowedDocument is the daemon boundary behind
// follow.Config.OnAdmittedDocument. The follower has already authenticated and
// durably admitted this exact source document; this method independently pins
// the raw bytes to the supplied semantic document and rechecks self-signature,
// contract and network.
//
// A source document is not a complete global snapshot when multiple sources
// independently win different heads. The registry is the authoritative merged
// serviceability view: rebuild every followed pointer from it, preserve exact
// source documents for unchanged roots, and attach this document only to heads
// whose current root and manifest it proves exactly. Multi-source callers pass
// one allowedHeads slice so a valid but locally unauthorized line cannot acquire
// a document pointer; omission retains singular mode's all-followed behavior.
func (s *pointerState) AdmitFollowedDocument(heads headDocumentReader, block blocks.Block, doc server.Doc, allowedHeads ...[]string) error {
	if heads == nil {
		return errors.New("bloard: followed pointer admission requires a head document reader")
	}
	if block == nil {
		return errors.New("bloard: followed pointer document block is nil")
	}
	exact, err := p2p.NewDocumentBlock(block.RawData())
	if err != nil {
		return fmt.Errorf("bloard: hashing followed pointer document: %w", err)
	}
	if !exact.Cid().Equals(block.Cid()) {
		return fmt.Errorf("bloard: followed pointer document bytes do not match CID %s", block.Cid())
	}
	decoded, err := decodePointerDocument(exact.RawData())
	if err != nil {
		return fmt.Errorf("bloard: followed pointer document: %w", err)
	}
	if !reflect.DeepEqual(decoded, doc) {
		return errors.New("bloard: followed pointer document block does not encode the authenticated document")
	}
	if doc.Net != s.net {
		return fmt.Errorf("bloard: followed pointer document is for net %q, want %q", doc.Net, s.net)
	}
	if err := doc.Verify(); err != nil {
		return fmt.Errorf("bloard: followed pointer document signature: %w", err)
	}
	if err := doc.ValidateContract(); err != nil {
		return fmt.Errorf("bloard: followed pointer document contract: %w", err)
	}

	var allowed map[string]struct{}
	admissible := s.followedNames
	if len(allowedHeads) > 1 {
		return errors.New("bloard: followed pointer admission received more than one allowed-head set")
	}
	if len(allowedHeads) == 1 {
		allowed = make(map[string]struct{}, len(allowedHeads[0]))
		admissible = make(map[string]struct{}, len(allowedHeads[0]))
		for _, name := range allowedHeads[0] {
			allowed[name] = struct{}{}
			if _, followed := s.followedNames[name]; followed {
				admissible[name] = struct{}{}
			}
		}
	}
	updates, err := statesFromDocument(doc, admissible, exact.Cid(), exact)
	if err != nil {
		return fmt.Errorf("bloard: followed pointer document: %w", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	next, err := s.currentFollowedLocked(heads, true)
	if err != nil {
		return s.withdrawFollowedAfterScanErrorLocked(err)
	}
	for name, admitted := range updates {
		if allowed != nil {
			if _, authorized := allowed[name]; !authorized {
				continue
			}
		}
		current, selected := next[name]
		if !selected || !pointerHeadEntriesEqual(current.entry, admitted.entry) {
			continue
		}
		current.set.Document = exact.Cid()
		current.document = exact
		next[name] = current
	}
	if err := s.replaceAllLocked(s.written, next, s.localPublication); err != nil {
		return err
	}
	s.followed = next
	return nil
}

// RestoreFollowed reconstructs the durable followed root/manifest view after
// Follower.Resume. HeadDoc is per-head state and cannot prove which exact
// upstream publication document selected it, so document pointers deliberately
// remain absent until the next authenticated follower admission.
func (s *pointerState) RestoreFollowed(heads headDocumentReader) error {
	return s.rebuildFollowed(heads, false)
}

// RefreshFollowed rescans every configured followed head from the registry's
// current serviceability view. Quarantining one finalized handoff can also make
// dependent mutable heads unserviceable, so a quarantine hook must call this
// full refresh rather than deleting only the name it was handed.
func (s *pointerState) RefreshFollowed(heads headDocumentReader) error {
	return s.rebuildFollowed(heads, true)
}

func (s *pointerState) rebuildFollowed(heads headDocumentReader, preserveCurrentDocuments bool) error {
	if heads == nil {
		return errors.New("bloard: followed pointer restore requires a head document reader")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	next, err := s.currentFollowedLocked(heads, preserveCurrentDocuments)
	if err != nil {
		return s.withdrawFollowedAfterScanErrorLocked(err)
	}

	if err := s.replaceAllLocked(s.written, next, s.localPublication); err != nil {
		return err
	}
	s.followed = next
	return nil
}

// currentFollowedLocked reconstructs the complete current followed snapshot
// from the registry's serviceability view. If requested, an authenticated
// source document remains attached only while its exact root and manifest are
// still current. s.mu must be held.
func (s *pointerState) currentFollowedLocked(heads headDocumentReader, preserveCurrentDocuments bool) (map[string]pointerHeadState, error) {
	next := make(map[string]pointerHeadState, len(s.followedNames))
	for name := range s.followedNames {
		// Read serviceability from the registry snapshot, not only from the
		// rendered publication document. Quarantine swaps the registry first;
		// if signing/rebuilding the document then fails, HeadDoc may still be the
		// stale pre-quarantine bytes and must not keep an exact pointer live.
		if _, serviceable := heads.Get(name); !serviceable {
			continue
		}
		raw, ok := heads.HeadDoc(name)
		if !ok {
			continue
		}
		var entry server.HeadEntry
		if err := json.Unmarshal(raw, &entry); err != nil {
			return nil, fmt.Errorf("bloard: restoring followed pointer head %q: %w", name, err)
		}
		if entry.Name != name {
			return nil, fmt.Errorf("bloard: restoring followed pointer head %q got entry for %q", name, entry.Name)
		}
		set, err := pointerSetFromEntry(entry, cid.Undef)
		if err != nil {
			return nil, fmt.Errorf("bloard: restoring followed pointer head %q: %w", name, err)
		}
		if set.Root.Defined() {
			// A refresh after quarantine preserves an exact source document only
			// when the still-serviceable root and manifest are unchanged. On a
			// fresh restart there is no in-memory old state, so this naturally
			// reconstructs root/manifest while failing closed on Document.
			old, existed := s.followed[name]
			state := pointerHeadState{set: set, entry: clonePointerHeadEntry(entry)}
			if preserveCurrentDocuments && existed && pointerHeadEntriesEqual(old.entry, entry) &&
				old.set.Root.Equals(set.Root) && equalOptionalCID(old.set.Manifest, set.Manifest) {
				set.Document = old.set.Document
				state.set = set
				state.document = old.document
			}
			next[name] = state
		}
	}
	return next, nil
}

// withdrawFollowedAfterScanErrorLocked prevents one malformed serviceable
// registry entry from leaving a quarantined or dependent head's previous exact
// pointers installed. A refresh is a complete snapshot replacement: if any
// entry cannot be represented, every followed pointer is withdrawn while the
// independently owned written/local state remains available. The scan error is
// still returned so the follower can make the same source document retry its
// post-durability admission after the local registry issue is repaired.
// s.mu must be held.
func (s *pointerState) withdrawFollowedAfterScanErrorLocked(scanErr error) error {
	empty := make(map[string]pointerHeadState)
	if err := s.replaceAllLocked(s.written, empty, s.localPublication); err != nil {
		return errors.Join(scanErr, fmt.Errorf("bloard: withdrawing followed pointers after registry scan failure: %w", err))
	}
	s.followed = empty
	return scanErr
}

func (s *pointerState) replaceWritten(next map[string]pointerHeadState, localPublication blocks.Block) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.replaceAllLocked(next, s.followed, localPublication); err != nil {
		return err
	}
	s.written = next
	s.localPublication = localPublication
	return nil
}

func (s *pointerState) replaceAllLocked(written, followed map[string]pointerHeadState, localPublication blocks.Block) error {
	all, err := composePointerSets(written, followed)
	if err != nil {
		return err
	}
	oldAll, err := composePointerSets(s.written, s.followed)
	if err != nil {
		return err
	}
	oldExtra, err := localPublicationPointers(s.localPublication)
	if err != nil {
		return err
	}
	newExtra, err := localPublicationPointers(localPublication)
	if err != nil {
		return err
	}
	_, oldCIDs, err := activePointerDocuments(s.written, s.followed, s.localPublication)
	if err != nil {
		return err
	}
	newDocuments, newCIDs, err := activePointerDocuments(written, followed, localPublication)
	if err != nil {
		return err
	}
	if err := s.verifiedDocs.StageCurrentAfterVerification(newDocuments); err != nil {
		return fmt.Errorf("bloard: staging current pointer documents: %w", err)
	}
	if err := s.coordinator.ReplaceAllWithDocuments(all, newExtra); err != nil {
		rollbackErr := s.verifiedDocs.ReplaceCurrentDocuments(oldCIDs)
		if rollbackErr != nil {
			return fmt.Errorf("bloard: replacing exact pointer schedule: %w (restoring prior current documents: %v)", err, rollbackErr)
		}
		return fmt.Errorf("bloard: replacing exact pointer schedule: %w", err)
	}
	if err := s.verifiedDocs.ReplaceCurrentDocuments(newCIDs); err != nil {
		// This should be unreachable because Stage validated every new CID. Make
		// the failure transactional even in that case: restore the old provider
		// schedule first, then its exact active-document set.
		scheduleErr := s.coordinator.ReplaceAllWithDocuments(oldAll, oldExtra)
		documentsErr := s.verifiedDocs.ReplaceCurrentDocuments(oldCIDs)
		if scheduleErr != nil || documentsErr != nil {
			return fmt.Errorf("bloard: committing current pointer documents: %w (restoring prior schedule: %v; documents: %v)", err, scheduleErr, documentsErr)
		}
		return fmt.Errorf("bloard: committing current pointer documents: %w", err)
	}
	return nil
}

func composePointerSets(written, followed map[string]pointerHeadState) (map[string]pointerhint.Set, error) {
	all := make(map[string]pointerhint.Set, len(written)+len(followed))
	for name, state := range written {
		all[name] = state.set
	}
	for name, state := range followed {
		if _, collision := all[name]; collision {
			return nil, fmt.Errorf("bloard: pointer state ownership collision for head %q", name)
		}
		all[name] = state.set
	}
	return all, nil
}

func activePointerDocuments(written, followed map[string]pointerHeadState, localPublication blocks.Block) ([]blocks.Block, []cid.Cid, error) {
	byCID := make(map[string]blocks.Block, len(written)+len(followed)+1)
	for _, source := range []map[string]pointerHeadState{written, followed} {
		for name, state := range source {
			if !state.set.Document.Defined() {
				if state.document != nil {
					return nil, nil, fmt.Errorf("bloard: pointer head %q retains a document block without a document pointer", name)
				}
				continue
			}
			if state.document == nil {
				return nil, nil, fmt.Errorf("bloard: pointer head %q names document %s without retaining its exact block", name, state.set.Document)
			}
			if !state.document.Cid().Equals(state.set.Document) {
				return nil, nil, fmt.Errorf("bloard: pointer head %q document block %s differs from pointer %s", name, state.document.Cid(), state.set.Document)
			}
			byCID[state.set.Document.KeyString()] = state.document
		}
	}
	if localPublication != nil {
		exact, err := p2p.NewDocumentBlock(localPublication.RawData())
		if err != nil {
			return nil, nil, fmt.Errorf("bloard: hashing local publication document: %w", err)
		}
		if !exact.Cid().Equals(localPublication.Cid()) {
			return nil, nil, fmt.Errorf("bloard: local publication document bytes do not match CID %s", localPublication.Cid())
		}
		byCID[exact.Cid().KeyString()] = exact
	}
	keys := make([]string, 0, len(byCID))
	for key := range byCID {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	documents := make([]blocks.Block, 0, len(keys))
	cids := make([]cid.Cid, 0, len(keys))
	for _, key := range keys {
		document := byCID[key]
		documents = append(documents, document)
		cids = append(cids, document.Cid())
	}
	return documents, cids, nil
}

func localPublicationPointers(document blocks.Block) ([]cid.Cid, error) {
	if document == nil {
		return nil, nil
	}
	exact, err := p2p.NewDocumentBlock(document.RawData())
	if err != nil {
		return nil, fmt.Errorf("bloard: hashing local publication document pointer: %w", err)
	}
	if !exact.Cid().Equals(document.Cid()) {
		return nil, fmt.Errorf("bloard: local publication document bytes do not match CID %s", document.Cid())
	}
	return []cid.Cid{exact.Cid()}, nil
}

func decodePointerDocument(raw []byte) (server.Doc, error) {
	var doc server.Doc
	if err := json.Unmarshal(raw, &doc); err != nil {
		return server.Doc{}, fmt.Errorf("does not decode: %w", err)
	}
	return doc, nil
}

func verifyExpectedDocumentSigner(doc server.Doc, expected ed25519.PublicKey) error {
	if err := doc.Verify(); err != nil {
		return err
	}
	encoded, err := hex.DecodeString(doc.Pubkey)
	if err != nil {
		return fmt.Errorf("decoding embedded public key: %w", err)
	}
	if !bytes.Equal(encoded, expected) {
		return fmt.Errorf("embedded public key %x does not match configured local signer %x", encoded, expected)
	}
	return nil
}

// statesFromDocument selects only configured ownership names. A hybrid node's
// locally rendered document includes followed entries too; the written path
// must not claim those, just as the follower path must not claim written ones.
func statesFromDocument(doc server.Doc, configured map[string]struct{}, document cid.Cid, block blocks.Block) (map[string]pointerHeadState, error) {
	states := make(map[string]pointerHeadState, len(configured))
	for _, entry := range doc.Heads {
		if _, selected := configured[entry.Name]; !selected {
			continue
		}
		set, err := pointerSetFromEntry(entry, document)
		if err != nil {
			return nil, fmt.Errorf("head %q: %w", entry.Name, err)
		}
		if set.Root.Defined() {
			state := pointerHeadState{set: set, entry: clonePointerHeadEntry(entry)}
			if set.Document.Defined() {
				state.document = block
			}
			states[entry.Name] = state
		}
	}
	return states, nil
}

func clonePointerHeadEntry(entry server.HeadEntry) server.HeadEntry {
	cloneSlot := func(value *uint64) *uint64 {
		if value == nil {
			return nil
		}
		copy := *value
		return &copy
	}
	entry.SyncedTo = cloneSlot(entry.SyncedTo)
	entry.WindowStart = cloneSlot(entry.WindowStart)
	entry.SourceFinalizedSlot = cloneSlot(entry.SourceFinalizedSlot)
	entry.HandoffSyncedTo = cloneSlot(entry.HandoffSyncedTo)
	return entry
}

// pointerHeadEntriesEqual requires every authenticated field to match the
// registry's current serviceable line. The only normalization is the wire-level
// spelling of finalized-monotonic: a revisioned upstream may state it explicitly,
// while this follower's revisionless local document must omit the kind field.
func pointerHeadEntriesEqual(left, right server.HeadEntry) bool {
	if left.EffectiveKind() == server.FinalizedMonotonic && right.EffectiveKind() == server.FinalizedMonotonic {
		left.Kind, right.Kind = "", ""
	}
	return reflect.DeepEqual(left, right)
}

func pointerSetFromEntry(entry server.HeadEntry, document cid.Cid) (pointerhint.Set, error) {
	// An uncovered entry is not a current pointer even if it still carries a
	// historical root. Revisioned follower documents use this, as well as
	// omission, to withdraw an exact current generation.
	if entry.Root == "" || entry.SyncedTo == nil {
		return pointerhint.Set{}, nil
	}
	root, err := cid.Decode(entry.Root)
	if err != nil {
		return pointerhint.Set{}, fmt.Errorf("root is not a CID: %w", err)
	}
	manifest := cid.Undef
	if entry.Manifest != "" {
		manifest, err = cid.Decode(entry.Manifest)
		if err != nil {
			return pointerhint.Set{}, fmt.Errorf("manifest is not a CID: %w", err)
		}
	}
	return pointerhint.Set{Root: root, Manifest: manifest, Document: document}, nil
}

func equalOptionalCID(a, b cid.Cid) bool {
	if !a.Defined() || !b.Defined() {
		return !a.Defined() && !b.Defined()
	}
	return a.Equals(b)
}
