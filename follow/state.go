package follow

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/ipfs/boxo/ipns"
	"github.com/ipfs/go-cid"

	"github.com/blobarchive/bloar/server"
)

// prefixFollow is this package's byte of the KV keyspace. See the package
// comment for the layout and catalog's for the rest of it.
const prefixFollow byte = 'f'

// The keys under it. Spelled out rather than numbered, and readable in a hex
// dump because a key an operator can read is worth more than the bytes it costs.
//
// checkpoint is the authoritative per-head record (spec 11.3, the safety boundary,
// the safety boundary): one atomic generation of {root, synced_to, manifest tip, authorizing
// updated_at}, plus kind/authority/revision/digest/window for a v2 revisioned
// generation, written in a single synced batch and the only thing Resume reads.
// synced_to and manifest are the pre-checkpoint per-head floors, retained as
// anti-regression facts a first checkpoint must clear but never written by this
// build. updated_at is the legacy global freshness floor. A revisioned document
// uses the signer-keyed authority floor instead; it is raised in the same batch
// as every checkpoint the document authorized.
var (
	keyUpdatedAt  = key("updated_at")
	keyIPNSSeq    = key("ipns_seq") // legacy single-name floor, read-only
	keyIPNSFloors = key("ipns_floors:v1")
	keyDelegation = key("delegation:v1")
	keyAuthority  = "authority:v1:" // 'f' || "authority:v1:" || 32-byte document signing key
	keyCheckpoint = "checkpoint:"   // 'f' || "checkpoint:" || name
	keySyncedTo   = "synced_to:"    // 'f' || "synced_to:" || name (legacy floor)
	keyManifest   = "manifest:"     // 'f' || "manifest:" || name (legacy floor)
	// verified_segment:v1 is a durable semantic proof, keyed by Segment CID.
	// Bump the key version if the KZG/binding verification rule changes; old
	// proofs then become unreachable and are conservatively recomputed.
	keyVerifiedSegment = "verified_segment:v1:"
)

const maxIPNSFloorNames = 32

func key(s string) []byte { return append([]byte{prefixFollow}, s...) }

// state is the follower's on-disk no-regression memory (spec 11.3).
//
// # Why it is persisted
//
// The floors are what makes a signature enough. A publication document proves
// who wrote it and nothing about when, so a follower's only defence against a
// valid-but-old document -- withheld updates, a stale IPNS record inside its
// lifetime, a writer rolled back from a backup -- is to remember how far it has
// already come. In memory that defence lasts until the process does, and the
// moment a follower is most likely to be handed an old document is the moment it
// restarts: it has just asked every channel it has, from scratch, with no
// opinion about what the answer should look like.
//
// # Why the checkpoint is atomic
//
// The root a follower serves and the floors that keep it from regressing are one
// fact, not four. An older design wrote the root (through the server RootStore)
// and each floor as separate synchronous operations; a crash between them left a
// newer root durable with an absent or stale floor, and the follower would then
// accept a correctly-signed rollback or compose a root from one generation with
// a manifest tip from another. checkpoint is the fix: root,
// synced_to, manifest tip and the authorizing document time are written together
// in one synced batch, before the head is exposed, and Resume reads only that
// record. The server RootStore and ManifestStore entries for a followed head are
// write-through compatibility mirrors for the read/serve path and pin reconciler
// -- never a resume source, never an authority.
type state struct{ kv *pebble.DB }

func verifiedSegmentKey(c cid.Cid) []byte {
	return append(key(keyVerifiedSegment), c.Bytes()...)
}

// segmentVerified reports whether a prior successful verify:full pass checked
// every RefEntry binding in this immutable Segment. Pebble protects this local
// marker with the same checksummed WAL/SST machinery as follower checkpoints;
// an absent marker is never an error and simply causes re-verification.
func (s *state) segmentVerified(c cid.Cid) (bool, error) {
	v, closer, err := s.kv.Get(verifiedSegmentKey(c))
	if errors.Is(err, pebble.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("follow: reading the full-verification proof for Segment %s: %w", c, err)
	}
	defer closer.Close()
	if len(v) != 1 || v[0] != 1 {
		return false, fmt.Errorf("follow: full-verification proof for Segment %s has an invalid value", c)
	}
	return true, nil
}

// markSegmentVerified persists a proof only after every entry in c succeeded.
// NoSync is safe: losing a recent marker on power loss causes extra work after
// restart, never misplaced trust. Pebble still writes the record through its
// checksummed WAL and reports storage errors to the sync pass.
func (s *state) markSegmentVerified(c cid.Cid) error {
	if err := s.kv.Set(verifiedSegmentKey(c), []byte{1}, pebble.NoSync); err != nil {
		return fmt.Errorf("follow: recording the full-verification proof for Segment %s: %w", c, err)
	}
	return nil
}

// checkpoint is one followed head's authoritative generation: the root it serves,
// the coverage floor that root satisfies, the manifest tip it attests (undefined
// for a head with no chain), and the publication updated_at that authorized them.
// Version 2 additionally binds the authenticated head kind and signer-local
// publication generation. Version 3 also binds the signed document network so
// even a first-use withdrawal tombstone has a durable cross-network baseline.
//
// Version 3 makes selection explicit and retains the exact authenticated
// publication line. selected=false is an authenticated withdrawal, not an absent
// checkpoint: publication authority/revision/digest still advance, while
// published retains the last selected line (when one exists) as the immutable
// parameter and anti-regression baseline. A mutable line also retains the exact
// finalized handoff line that accompanied it in the same signed document. Resume
// can therefore prove the pair it is about to expose instead of reconstructing a
// proof from whichever finalized generation happens to be current.
//
// Version 4 adds the signed logical archive identity and the locally authorized
// source identity. These are deliberately stored on every head, rather than as a
// process-wide generation, because one atomic selected snapshot may contain
// compatible claims chosen from different independent writers.
type checkpoint struct {
	root        cid.Cid
	syncedTo    uint64
	manifestTip cid.Cid
	updatedAt   time.Time

	// The fields below are present in v2/v3 checkpoints. revision==0 is the
	// exact legacy v1 encoding and finalized-monotonic contract. A revisioned
	// finalized head uses v2 too: the publication authority and claim digest are
	// part of the generation which authorized its root even though its ordinary
	// synced_to floor remains monotonic.
	kind        server.HeadKind
	authority   [32]byte
	revision    uint64
	digest      [32]byte
	windowStart uint64

	// version is populated by decode. Zero preserves the construction contract of
	// existing callers: revision zero encodes v1 and a non-zero revision encodes
	// v2. New document-level adoption constructs v3 explicitly.
	version   byte
	selected  bool
	net       string
	archiveID server.ArchiveID
	sourceID  string
	published *server.HeadEntry
	handoff   *server.HeadEntry
	overlay   *server.HeadEntry
}

// checkpointVersion is the schema version of an encoded checkpoint record. A
// reader that meets a version it does not know refuses the record rather than
// guessing at its layout (spec 15's reject-unknown rule, applied to node-local
// state).
const (
	checkpointVersionV1 byte = 1
	checkpointVersionV2 byte = 2
	checkpointVersionV3 byte = 3
	checkpointVersionV4 byte = 4
)

const maxCheckpointHeadEntryBytes = 1 << 20

// encodeCheckpoint renders cp: version, flags, the two fixed-width numbers, then
// the root (length-prefixed) and the manifest tip (the remainder, present iff the
// flag says so). updated_at is stored as int64 seconds in two's complement so a
// pre-1970 forgery round-trips rather than wrapping.
func encodeCheckpoint(cp checkpoint) ([]byte, error) {
	version := cp.version
	if version == 0 {
		if cp.revision != 0 {
			version = checkpointVersionV2
		} else {
			version = checkpointVersionV1
		}
	}
	if err := validateCheckpointForEncoding(cp, version); err != nil {
		return nil, err
	}
	switch version {
	case checkpointVersionV1:
		return encodeCheckpointV1(cp), nil
	case checkpointVersionV2:
		return encodeCheckpointV2(cp), nil
	case checkpointVersionV3:
		return encodeCheckpointV3(cp)
	case checkpointVersionV4:
		return encodeCheckpointV4(cp)
	default:
		return nil, fmt.Errorf("follow: refusing to encode checkpoint version %d", version)
	}
}

func encodeCheckpointV1(cp checkpoint) []byte {
	root := cp.root.Bytes()
	var flags byte
	var tip []byte
	if cp.manifestTip.Defined() {
		flags |= 1
		tip = cp.manifestTip.Bytes()
	}
	b := make([]byte, 0, 2+8+8+2+len(root)+len(tip))
	b = append(b, checkpointVersionV1, flags)
	b = binary.BigEndian.AppendUint64(b, cp.syncedTo)
	b = binary.BigEndian.AppendUint64(b, uint64(cp.updatedAt.Unix()))
	b = binary.BigEndian.AppendUint16(b, uint16(len(root)))
	b = append(b, root...)
	b = append(b, tip...)
	return b
}

// encodeCheckpointV2 appends the authority-local publication generation to the
// legacy root/floor tuple. The layout is fixed-width until the existing
// length-prefixed root and optional manifest tip:
//
//	version, flags, kind, reserved,
//	synced_to, updated_at, publication revision, window_start,
//	authority pubkey[32], canonical digest[32], root length, root, manifest
//
// A distinct version keeps every v1 byte stable and makes an older binary fail
// closed instead of interpreting a mutable checkpoint as finalized state.
func encodeCheckpointV2(cp checkpoint) []byte {
	root := cp.root.Bytes()
	var flags byte
	var tip []byte
	if cp.manifestTip.Defined() {
		flags |= 1
		tip = cp.manifestTip.Bytes()
	}
	var kind byte
	switch cp.kind {
	case "", server.FinalizedMonotonic:
		kind = 0
	case server.UnfinalizedMutable:
		kind = 1
	default:
		// Callers validate kinds before checkpointing. Retaining an impossible
		// marker here makes a programming error decode as corrupt state rather
		// than silently assigning it the finalized contract.
		kind = 0xff
	}
	b := make([]byte, 0, 4+8*4+32+32+2+len(root)+len(tip))
	b = append(b, checkpointVersionV2, flags, kind, 0)
	b = binary.BigEndian.AppendUint64(b, cp.syncedTo)
	b = binary.BigEndian.AppendUint64(b, uint64(cp.updatedAt.Unix()))
	b = binary.BigEndian.AppendUint64(b, cp.revision)
	b = binary.BigEndian.AppendUint64(b, cp.windowStart)
	b = append(b, cp.authority[:]...)
	b = append(b, cp.digest[:]...)
	b = binary.BigEndian.AppendUint16(b, uint16(len(root)))
	b = append(b, root...)
	b = append(b, tip...)
	return b
}

// encodeCheckpointV3 stores document identity first, followed by canonical JSON
// encodings of the exact HeadEntry values. HeadEntry is the signed publication
// schema itself; using its canonical encoding preserves pointer presence (most
// importantly an explicitly observed slot zero) and automatically fails closed
// if a future binary adds signed fields without bumping this local schema.
//
//	version, flags, network length, updated_at, revision,
//	authority[32], digest[32], published length, handoff length,
//	network bytes, published JSON, handoff JSON
//
// flags bit 0 is selected, bit 1 says a retained/published line follows, and bit
// 2 says its same-document finalized handoff witness follows.
func encodeCheckpointV3(cp checkpoint) ([]byte, error) {
	entry, err := encodeCheckpointHeadEntry(cp.published)
	if err != nil {
		return nil, fmt.Errorf("follow: encoding checkpoint v3 publication entry: %w", err)
	}
	witness, err := encodeCheckpointHeadEntry(cp.handoff)
	if err != nil {
		return nil, fmt.Errorf("follow: encoding checkpoint v3 handoff entry: %w", err)
	}
	var flags byte
	if cp.selected {
		flags |= 1
	}
	if cp.published != nil {
		flags |= 2
	}
	if cp.handoff != nil {
		flags |= 4
	}
	b := make([]byte, 0, 92+len(cp.net)+len(entry)+len(witness))
	b = append(b, checkpointVersionV3, flags)
	b = binary.BigEndian.AppendUint16(b, uint16(len(cp.net)))
	b = binary.BigEndian.AppendUint64(b, uint64(cp.updatedAt.Unix()))
	b = binary.BigEndian.AppendUint64(b, cp.revision)
	b = append(b, cp.authority[:]...)
	b = append(b, cp.digest[:]...)
	b = binary.BigEndian.AppendUint32(b, uint32(len(entry)))
	b = binary.BigEndian.AppendUint32(b, uint32(len(witness)))
	b = append(b, cp.net...)
	b = append(b, entry...)
	b = append(b, witness...)
	return b, nil
}

// encodeCheckpointV4 extends the proof-complete v3 record with the identities
// needed to attribute a selected claim after restart:
//
//	version, flags, network length, source ID length, reserved[3],
//	updated_at, publication revision, archive ID[32], authority[32], digest[32],
//	published length, handoff length, overlay length,
//	source ID bytes, network bytes, published JSON, handoff JSON, overlay JSON
//
// The fixed header is 132 bytes. Source ID is the canonical operator-assigned
// name bound to authority out of band; archive ID is the value authenticated by
// the signed version-3 publication. Keeping the exact HeadEntry pair and signed
// document digest preserves the same proof boundary as checkpoint v3. Overlay
// is a separately authenticated finalized claim selected from the global source
// snapshot to prove a replica's configured filtered-finalized/live boundary.
func encodeCheckpointV4(cp checkpoint) ([]byte, error) {
	entry, err := encodeCheckpointHeadEntry(cp.published)
	if err != nil {
		return nil, fmt.Errorf("follow: encoding checkpoint v4 publication entry: %w", err)
	}
	witness, err := encodeCheckpointHeadEntry(cp.handoff)
	if err != nil {
		return nil, fmt.Errorf("follow: encoding checkpoint v4 handoff entry: %w", err)
	}
	overlay, err := encodeCheckpointHeadEntry(cp.overlay)
	if err != nil {
		return nil, fmt.Errorf("follow: encoding checkpoint v4 overlay entry: %w", err)
	}
	var flags byte
	if cp.selected {
		flags |= 1
	}
	if cp.published != nil {
		flags |= 2
	}
	if cp.handoff != nil {
		flags |= 4
	}
	if cp.overlay != nil {
		flags |= 8
	}
	b := make([]byte, 0, 132+len(cp.sourceID)+len(cp.net)+len(entry)+len(witness)+len(overlay))
	b = append(b, checkpointVersionV4, flags)
	b = binary.BigEndian.AppendUint16(b, uint16(len(cp.net)))
	b = append(b, byte(len(cp.sourceID)), 0, 0, 0)
	b = binary.BigEndian.AppendUint64(b, uint64(cp.updatedAt.Unix()))
	b = binary.BigEndian.AppendUint64(b, cp.revision)
	b = append(b, cp.archiveID[:]...)
	b = append(b, cp.authority[:]...)
	b = append(b, cp.digest[:]...)
	b = binary.BigEndian.AppendUint32(b, uint32(len(entry)))
	b = binary.BigEndian.AppendUint32(b, uint32(len(witness)))
	b = binary.BigEndian.AppendUint32(b, uint32(len(overlay)))
	b = append(b, cp.sourceID...)
	b = append(b, cp.net...)
	b = append(b, entry...)
	b = append(b, witness...)
	b = append(b, overlay...)
	return b, nil
}

func encodeCheckpointHeadEntry(entry *server.HeadEntry) ([]byte, error) {
	if entry == nil {
		return nil, nil
	}
	b, err := json.Marshal(entry)
	if err != nil {
		return nil, err
	}
	if len(b) > maxCheckpointHeadEntryBytes {
		return nil, fmt.Errorf("HeadEntry is %d bytes, maximum is %d", len(b), maxCheckpointHeadEntryBytes)
	}
	return b, nil
}

// makeCheckpointV3 is the construction boundary for document-level checkpoint
// generations. published is the exact currently selected line, or the retained
// last-selected line when selected is false. A head omitted before it has ever
// been selected has both published and handoff nil. publication identifies the
// signed document which selected or withdrew the head.
func makeCheckpointV3(net string, selected bool, published, handoff *server.HeadEntry, updatedAt time.Time, publication authorityFloor) (checkpoint, error) {
	cp := checkpoint{
		updatedAt: updatedAt,
		kind:      server.FinalizedMonotonic,
		authority: publication.authority,
		revision:  publication.revision,
		digest:    publication.digest,
		version:   checkpointVersionV3,
		selected:  selected,
		net:       net,
		published: cloneCheckpointHeadEntry(published),
		handoff:   cloneCheckpointHeadEntry(handoff),
	}
	return normalizeCheckpointV3(cp)
}

// makeCheckpointV4 is the source-set construction boundary. archiveID and
// sourceID are mandatory provenance: archiveID is signed into every logical
// archive publication and sourceID names the locally pinned signer which
// produced publication. A withdrawal retains the last exact publication,
// handoff, and filtered-finalized overlay entries in the same way as v3 retains
// its signed pair, while the new document generation and source remain explicit.
func makeCheckpointV4(net string, archiveID server.ArchiveID, sourceID string, selected bool, published, handoff, overlay *server.HeadEntry, updatedAt time.Time, publication authorityFloor) (checkpoint, error) {
	cp := checkpoint{
		updatedAt: updatedAt,
		kind:      server.FinalizedMonotonic,
		authority: publication.authority,
		revision:  publication.revision,
		digest:    publication.digest,
		version:   checkpointVersionV4,
		selected:  selected,
		net:       net,
		archiveID: archiveID,
		sourceID:  sourceID,
		published: cloneCheckpointHeadEntry(published),
		handoff:   cloneCheckpointHeadEntry(handoff),
		overlay:   cloneCheckpointHeadEntry(overlay),
	}
	return normalizeCheckpointV4(cp)
}

func cloneCheckpointHeadEntry(entry *server.HeadEntry) *server.HeadEntry {
	if entry == nil {
		return nil
	}
	cloneSlot := func(slot *uint64) *uint64 {
		if slot == nil {
			return nil
		}
		value := *slot
		return &value
	}
	copy := *entry
	copy.SyncedTo = cloneSlot(entry.SyncedTo)
	copy.WindowStart = cloneSlot(entry.WindowStart)
	copy.SourceFinalizedSlot = cloneSlot(entry.SourceFinalizedSlot)
	copy.HandoffSyncedTo = cloneSlot(entry.HandoffSyncedTo)
	return &copy
}

// normalizeCheckpointV3 validates the exact retained line and projects its
// serving/floor fields into the legacy checkpoint members used throughout the
// follower. The entry remains authoritative; the projection is deliberately not
// encoded twice, avoiding a second representation that could disagree after a
// crash or schema migration.
func normalizeCheckpointV3(cp checkpoint) (checkpoint, error) {
	if !cp.archiveID.IsZero() || cp.sourceID != "" || cp.overlay != nil {
		return checkpoint{}, errors.New("follow: checkpoint v3 carries source-set provenance")
	}
	return normalizeProofCheckpoint(cp, checkpointVersionV3)
}

func normalizeCheckpointV4(cp checkpoint) (checkpoint, error) {
	if cp.archiveID.IsZero() {
		return checkpoint{}, errors.New("follow: checkpoint v4 has an empty archive ID")
	}
	if err := validateSourceID(cp.sourceID); err != nil {
		return checkpoint{}, fmt.Errorf("follow: checkpoint v4 has an invalid source ID: %w", err)
	}
	normalized, err := normalizeProofCheckpoint(cp, checkpointVersionV4)
	if err != nil {
		return checkpoint{}, err
	}
	if normalized.overlay == nil {
		return normalized, nil
	}
	if normalized.published == nil || normalized.published.EffectiveKind() != server.UnfinalizedMutable {
		return checkpoint{}, errors.New("follow: checkpoint v4 finalized or empty publication carries a filtered-finalized overlay witness")
	}
	if normalized.overlay.EffectiveKind() != server.FinalizedMonotonic {
		return checkpoint{}, fmt.Errorf("follow: checkpoint v4 overlay head %q is not finalized-monotonic", normalized.overlay.Name)
	}
	if normalized.overlay.SyncedTo == nil {
		return checkpoint{}, fmt.Errorf("follow: checkpoint v4 overlay head %q is uncovered", normalized.overlay.Name)
	}
	revision := normalized.revision
	archiveID := normalized.archiveID
	overlayDoc := server.Doc{
		Unsigned: server.Unsigned{
			V:         server.LogicalArchiveDocVersion,
			Net:       normalized.net,
			ArchiveID: &archiveID,
			Heads:     []server.HeadEntry{*cloneCheckpointHeadEntry(normalized.overlay)},
			Revision:  &revision,
		},
		Pubkey:    "checkpoint v4 overlay",
		Signature: "checkpoint v4 overlay",
	}
	if err := overlayDoc.ValidateContract(); err != nil {
		return checkpoint{}, fmt.Errorf("follow: checkpoint v4 overlay publication contract is invalid: %w", err)
	}
	if _, err := cid.Decode(normalized.overlay.Root); err != nil {
		return checkpoint{}, fmt.Errorf("follow: checkpoint v4 overlay head %q has an undecodable root: %w", normalized.overlay.Name, err)
	}
	if normalized.overlay.Manifest != "" {
		if _, err := cid.Decode(normalized.overlay.Manifest); err != nil {
			return checkpoint{}, fmt.Errorf("follow: checkpoint v4 overlay head %q has an undecodable manifest tip: %w", normalized.overlay.Name, err)
		}
	}
	// ValidateContract established a non-nil mutable window start. The retained
	// finalized overlay must reach the slot immediately before that window; it may
	// extend farther, and subtraction keeps MaxUint64 well-defined.
	if *normalized.published.WindowStart > *normalized.overlay.SyncedTo &&
		*normalized.published.WindowStart-*normalized.overlay.SyncedTo > 1 {
		return checkpoint{}, fmt.Errorf("follow: checkpoint v4 overlay head %q ends at %d before mutable head %q window starts at %d",
			normalized.overlay.Name, *normalized.overlay.SyncedTo, normalized.published.Name, *normalized.published.WindowStart)
	}
	normalized.overlay = cloneCheckpointHeadEntry(normalized.overlay)
	return normalized, nil
}

// normalizeProofCheckpoint validates the exact retained line and projects it
// into the serving fields shared by v3 and v4. Version 4 validates the logical
// archive publication contract; version 3 preserves the earlier proof-aware
// contract byte for byte.
func normalizeProofCheckpoint(cp checkpoint, version byte) (checkpoint, error) {
	label := fmt.Sprintf("checkpoint v%d", version)
	if cp.net == "" || len(cp.net) > int(^uint16(0)) {
		return checkpoint{}, fmt.Errorf("follow: %s has an empty or oversized network", label)
	}
	if cp.revision == 0 {
		return checkpoint{}, fmt.Errorf("follow: %s has publication revision 0", label)
	}
	if cp.authority == ([32]byte{}) {
		return checkpoint{}, fmt.Errorf("follow: %s has an empty publication authority", label)
	}
	if cp.selected && cp.published == nil {
		return checkpoint{}, fmt.Errorf("follow: selected %s has no authenticated publication entry", label)
	}
	if cp.published == nil {
		if cp.handoff != nil {
			return checkpoint{}, fmt.Errorf("follow: %s has a handoff witness without a retained publication entry", label)
		}
		cp.version = version
		cp.root = cid.Undef
		cp.syncedTo = 0
		cp.manifestTip = cid.Undef
		cp.kind = ""
		cp.windowStart = 0
		return cp, nil
	}
	if cp.published.SyncedTo == nil {
		return checkpoint{}, fmt.Errorf("follow: %s retained head %q is uncovered", label, cp.published.Name)
	}

	heads := []server.HeadEntry{*cloneCheckpointHeadEntry(cp.published)}
	switch cp.published.EffectiveKind() {
	case server.FinalizedMonotonic:
		if cp.handoff != nil {
			return checkpoint{}, fmt.Errorf("follow: %s finalized head %q carries a handoff witness", label, cp.published.Name)
		}
	case server.UnfinalizedMutable:
		if cp.handoff == nil {
			return checkpoint{}, fmt.Errorf("follow: %s mutable head %q has no same-document handoff witness", label, cp.published.Name)
		}
		heads = append(heads, *cloneCheckpointHeadEntry(cp.handoff))
	default:
		return checkpoint{}, fmt.Errorf("follow: %s head %q has unknown kind %q", label, cp.published.Name, cp.published.Kind)
	}
	revision := cp.revision
	docVersion := server.DocVersion
	var archiveID *server.ArchiveID
	if version == checkpointVersionV4 {
		docVersion = server.LogicalArchiveDocVersion
		id := cp.archiveID
		archiveID = &id
	}
	doc := server.Doc{
		Unsigned:  server.Unsigned{V: docVersion, Net: cp.net, ArchiveID: archiveID, Heads: heads, Revision: &revision},
		Pubkey:    label,
		Signature: label,
	}
	if err := doc.ValidateContract(); err != nil {
		return checkpoint{}, fmt.Errorf("follow: %s retained publication contract is invalid: %w", label, err)
	}
	root, err := cid.Decode(cp.published.Root)
	if err != nil {
		return checkpoint{}, fmt.Errorf("follow: %s head %q has an undecodable root: %w", label, cp.published.Name, err)
	}
	manifestTip := cid.Undef
	if cp.published.Manifest != "" {
		manifestTip, err = cid.Decode(cp.published.Manifest)
		if err != nil {
			return checkpoint{}, fmt.Errorf("follow: %s head %q has an undecodable manifest tip: %w", label, cp.published.Name, err)
		}
	}
	cp.version = version
	cp.published = cloneCheckpointHeadEntry(cp.published)
	cp.handoff = cloneCheckpointHeadEntry(cp.handoff)
	cp.root = root
	cp.syncedTo = *cp.published.SyncedTo
	cp.manifestTip = manifestTip
	cp.kind = cp.published.EffectiveKind()
	cp.windowStart = 0
	if cp.published.WindowStart != nil {
		cp.windowStart = *cp.published.WindowStart
	}
	return cp, nil
}

func validateCheckpointForEncoding(cp checkpoint, version byte) error {
	switch version {
	case checkpointVersionV1:
		if !cp.root.Defined() {
			return errors.New("follow: checkpoint v1 has an undefined root")
		}
		if cp.revision != 0 {
			return errors.New("follow: checkpoint v1 has a publication revision")
		}
		if cp.kind != "" && cp.kind != server.FinalizedMonotonic {
			return fmt.Errorf("follow: checkpoint v1 has unsupported head kind %q", cp.kind)
		}
		if cp.published != nil || cp.handoff != nil || cp.overlay != nil || !cp.archiveID.IsZero() || cp.sourceID != "" {
			return errors.New("follow: checkpoint v1 carries v3 publication proof fields")
		}
	case checkpointVersionV2:
		if !cp.root.Defined() {
			return errors.New("follow: checkpoint v2 has an undefined root")
		}
		if cp.revision == 0 {
			return errors.New("follow: checkpoint v2 has publication revision 0")
		}
		if cp.authority == ([32]byte{}) {
			return errors.New("follow: checkpoint v2 has an empty publication authority")
		}
		if cp.kind != "" && cp.kind != server.FinalizedMonotonic && cp.kind != server.UnfinalizedMutable {
			return fmt.Errorf("follow: checkpoint v2 has unknown head kind %q", cp.kind)
		}
		if cp.kind != server.UnfinalizedMutable && cp.windowStart != 0 {
			return errors.New("follow: checkpoint v2 finalized head has a mutable window start")
		}
		if cp.kind == server.UnfinalizedMutable && cp.manifestTip.Defined() {
			return errors.New("follow: checkpoint v2 mutable head carries a manifest tip")
		}
		if cp.published != nil || cp.handoff != nil || cp.overlay != nil || !cp.archiveID.IsZero() || cp.sourceID != "" {
			return errors.New("follow: checkpoint v2 carries v3 publication proof fields")
		}
	case checkpointVersionV3:
		if _, err := normalizeCheckpointV3(cp); err != nil {
			return err
		}
	case checkpointVersionV4:
		if _, err := normalizeCheckpointV4(cp); err != nil {
			return err
		}
	default:
		return fmt.Errorf("follow: unknown checkpoint version %d", version)
	}
	return nil
}

// decodeCheckpoint parses a record encodeCheckpoint wrote.
func decodeCheckpoint(b []byte) (checkpoint, error) {
	if len(b) < 1 {
		return checkpoint{}, fmt.Errorf("checkpoint record is %d bytes, too short", len(b))
	}
	switch b[0] {
	case checkpointVersionV1:
		return decodeCheckpointV1(b)
	case checkpointVersionV2:
		return decodeCheckpointV2(b)
	case checkpointVersionV3:
		return decodeCheckpointV3(b)
	case checkpointVersionV4:
		return decodeCheckpointV4(b)
	default:
		return checkpoint{}, fmt.Errorf("checkpoint record is version %d, this build reads versions %d, %d, %d, and %d",
			b[0], checkpointVersionV1, checkpointVersionV2, checkpointVersionV3, checkpointVersionV4)
	}
}

func decodeCheckpointV1(b []byte) (checkpoint, error) {
	if len(b) < 20 {
		return checkpoint{}, fmt.Errorf("checkpoint record is %d bytes, too short", len(b))
	}
	flags := b[1]
	cp := checkpoint{
		syncedTo:  binary.BigEndian.Uint64(b[2:10]),
		updatedAt: time.Unix(int64(binary.BigEndian.Uint64(b[10:18])), 0).UTC(),
		kind:      server.FinalizedMonotonic,
		version:   checkpointVersionV1,
		selected:  true,
	}
	rootLen := int(binary.BigEndian.Uint16(b[18:20]))
	if len(b) < 20+rootLen {
		return checkpoint{}, fmt.Errorf("checkpoint record claims a %d-byte root but holds %d bytes", rootLen, len(b)-20)
	}
	root, err := cid.Cast(b[20 : 20+rootLen])
	if err != nil {
		return checkpoint{}, fmt.Errorf("checkpoint record has an undecodable root: %w", err)
	}
	cp.root = root
	if flags&1 != 0 {
		if len(b) == 20+rootLen {
			return checkpoint{}, fmt.Errorf("checkpoint record says it carries a manifest tip but it is absent")
		}
		tip, err := cid.Cast(b[20+rootLen:])
		if err != nil {
			return checkpoint{}, fmt.Errorf("checkpoint record has an undecodable manifest tip: %w", err)
		}
		cp.manifestTip = tip
	}
	return cp, nil
}

func decodeCheckpointV2(b []byte) (checkpoint, error) {
	const fixed = 4 + 8*4 + 32 + 32 + 2
	if len(b) < fixed {
		return checkpoint{}, fmt.Errorf("checkpoint v2 record is %d bytes, too short", len(b))
	}
	if b[3] != 0 {
		return checkpoint{}, fmt.Errorf("checkpoint v2 record has non-zero reserved flags")
	}
	cp := checkpoint{
		syncedTo:    binary.BigEndian.Uint64(b[4:12]),
		updatedAt:   time.Unix(int64(binary.BigEndian.Uint64(b[12:20])), 0).UTC(),
		revision:    binary.BigEndian.Uint64(b[20:28]),
		windowStart: binary.BigEndian.Uint64(b[28:36]),
		version:     checkpointVersionV2,
		selected:    true,
	}
	switch b[2] {
	case 0:
		cp.kind = server.FinalizedMonotonic
	case 1:
		cp.kind = server.UnfinalizedMutable
	default:
		return checkpoint{}, fmt.Errorf("checkpoint v2 record has unknown head kind %d", b[2])
	}
	if cp.revision == 0 {
		return checkpoint{}, fmt.Errorf("checkpoint v2 record has publication revision 0")
	}
	copy(cp.authority[:], b[36:68])
	copy(cp.digest[:], b[68:100])
	if cp.authority == ([32]byte{}) {
		return checkpoint{}, fmt.Errorf("checkpoint v2 record has an empty publication authority")
	}
	rootLen := int(binary.BigEndian.Uint16(b[100:102]))
	if rootLen == 0 || len(b) < fixed+rootLen {
		return checkpoint{}, fmt.Errorf("checkpoint v2 record claims a %d-byte root but holds %d bytes", rootLen, len(b)-fixed)
	}
	root, err := cid.Cast(b[fixed : fixed+rootLen])
	if err != nil {
		return checkpoint{}, fmt.Errorf("checkpoint v2 record has an undecodable root: %w", err)
	}
	cp.root = root
	if b[1]&^byte(1) != 0 {
		return checkpoint{}, fmt.Errorf("checkpoint v2 record has unknown flags %#x", b[1])
	}
	if b[1]&1 != 0 {
		if len(b) == fixed+rootLen {
			return checkpoint{}, fmt.Errorf("checkpoint v2 record says it carries a manifest tip but it is absent")
		}
		tip, err := cid.Cast(b[fixed+rootLen:])
		if err != nil {
			return checkpoint{}, fmt.Errorf("checkpoint v2 record has an undecodable manifest tip: %w", err)
		}
		cp.manifestTip = tip
	} else if len(b) != fixed+rootLen {
		return checkpoint{}, fmt.Errorf("checkpoint v2 record has %d trailing bytes without a manifest flag", len(b)-(fixed+rootLen))
	}
	if cp.kind == server.UnfinalizedMutable && cp.manifestTip.Defined() {
		return checkpoint{}, fmt.Errorf("checkpoint v2 mutable head carries a manifest tip")
	}
	return cp, nil
}

func decodeCheckpointV3(b []byte) (checkpoint, error) {
	const fixed = 92
	if len(b) < fixed {
		return checkpoint{}, fmt.Errorf("checkpoint v3 record is %d bytes, too short", len(b))
	}
	if b[1]&^byte(7) != 0 {
		return checkpoint{}, fmt.Errorf("checkpoint v3 record has unknown flags %#x", b[1])
	}
	netLen := int(binary.BigEndian.Uint16(b[2:4]))
	entryLen := int(binary.BigEndian.Uint32(b[84:88]))
	witnessLen := int(binary.BigEndian.Uint32(b[88:92]))
	if entryLen > maxCheckpointHeadEntryBytes || witnessLen > maxCheckpointHeadEntryBytes {
		return checkpoint{}, fmt.Errorf("checkpoint v3 record carries an oversized HeadEntry (%d/%d bytes)", entryLen, witnessLen)
	}
	if netLen == 0 || netLen > len(b)-fixed || entryLen > len(b)-fixed-netLen ||
		witnessLen > len(b)-fixed-netLen-entryLen || len(b) != fixed+netLen+entryLen+witnessLen {
		return checkpoint{}, fmt.Errorf("checkpoint v3 record lengths net=%d entry=%d handoff=%d do not match its %d-byte payload", netLen, entryLen, witnessLen, len(b)-fixed)
	}
	hasEntry, hasWitness := b[1]&2 != 0, b[1]&4 != 0
	if hasEntry != (entryLen != 0) || hasWitness != (witnessLen != 0) {
		return checkpoint{}, errors.New("checkpoint v3 record flags disagree with its HeadEntry lengths")
	}
	entryStart := fixed + netLen
	entry, err := decodeCheckpointHeadEntry(b[entryStart : entryStart+entryLen])
	if err != nil {
		return checkpoint{}, fmt.Errorf("checkpoint v3 record has an invalid publication entry: %w", err)
	}
	witness, err := decodeCheckpointHeadEntry(b[entryStart+entryLen:])
	if err != nil {
		return checkpoint{}, fmt.Errorf("checkpoint v3 record has an invalid handoff entry: %w", err)
	}
	cp := checkpoint{
		updatedAt: time.Unix(int64(binary.BigEndian.Uint64(b[4:12])), 0).UTC(),
		revision:  binary.BigEndian.Uint64(b[12:20]),
		version:   checkpointVersionV3,
		selected:  b[1]&1 != 0,
		net:       string(b[fixed:entryStart]),
		published: entry,
		handoff:   witness,
	}
	copy(cp.authority[:], b[20:52])
	copy(cp.digest[:], b[52:84])
	return normalizeCheckpointV3(cp)
}

func decodeCheckpointV4(b []byte) (checkpoint, error) {
	const fixed = 132
	if len(b) < fixed {
		return checkpoint{}, fmt.Errorf("checkpoint v4 record is %d bytes, too short", len(b))
	}
	if b[1]&^byte(15) != 0 {
		return checkpoint{}, fmt.Errorf("checkpoint v4 record has unknown flags %#x", b[1])
	}
	if b[5] != 0 || b[6] != 0 || b[7] != 0 {
		return checkpoint{}, errors.New("checkpoint v4 record has non-zero reserved bytes")
	}
	netLen := int(binary.BigEndian.Uint16(b[2:4]))
	sourceLen := int(b[4])
	entryLen := int(binary.BigEndian.Uint32(b[120:124]))
	witnessLen := int(binary.BigEndian.Uint32(b[124:128]))
	overlayLen := int(binary.BigEndian.Uint32(b[128:132]))
	if entryLen > maxCheckpointHeadEntryBytes || witnessLen > maxCheckpointHeadEntryBytes || overlayLen > maxCheckpointHeadEntryBytes {
		return checkpoint{}, fmt.Errorf("checkpoint v4 record carries an oversized HeadEntry (%d/%d/%d bytes)", entryLen, witnessLen, overlayLen)
	}
	payloadLen := len(b) - fixed
	if sourceLen == 0 || sourceLen > payloadLen || netLen == 0 || netLen > payloadLen-sourceLen ||
		entryLen > payloadLen-sourceLen-netLen || witnessLen > payloadLen-sourceLen-netLen-entryLen ||
		overlayLen > payloadLen-sourceLen-netLen-entryLen-witnessLen ||
		payloadLen != sourceLen+netLen+entryLen+witnessLen+overlayLen {
		return checkpoint{}, fmt.Errorf("checkpoint v4 record lengths source=%d net=%d entry=%d handoff=%d overlay=%d do not match its %d-byte payload",
			sourceLen, netLen, entryLen, witnessLen, overlayLen, payloadLen)
	}
	hasEntry, hasWitness, hasOverlay := b[1]&2 != 0, b[1]&4 != 0, b[1]&8 != 0
	if hasEntry != (entryLen != 0) || hasWitness != (witnessLen != 0) || hasOverlay != (overlayLen != 0) {
		return checkpoint{}, errors.New("checkpoint v4 record flags disagree with its HeadEntry lengths")
	}
	sourceStart := fixed
	netStart := sourceStart + sourceLen
	entryStart := netStart + netLen
	entry, err := decodeCheckpointHeadEntry(b[entryStart : entryStart+entryLen])
	if err != nil {
		return checkpoint{}, fmt.Errorf("checkpoint v4 record has an invalid publication entry: %w", err)
	}
	witnessEnd := entryStart + entryLen + witnessLen
	witness, err := decodeCheckpointHeadEntry(b[entryStart+entryLen : witnessEnd])
	if err != nil {
		return checkpoint{}, fmt.Errorf("checkpoint v4 record has an invalid handoff entry: %w", err)
	}
	overlay, err := decodeCheckpointHeadEntry(b[witnessEnd:])
	if err != nil {
		return checkpoint{}, fmt.Errorf("checkpoint v4 record has an invalid overlay entry: %w", err)
	}
	cp := checkpoint{
		updatedAt: time.Unix(int64(binary.BigEndian.Uint64(b[8:16])), 0).UTC(),
		revision:  binary.BigEndian.Uint64(b[16:24]),
		version:   checkpointVersionV4,
		selected:  b[1]&1 != 0,
		sourceID:  string(b[sourceStart:netStart]),
		net:       string(b[netStart:entryStart]),
		published: entry,
		handoff:   witness,
		overlay:   overlay,
	}
	copy(cp.archiveID[:], b[24:56])
	copy(cp.authority[:], b[56:88])
	copy(cp.digest[:], b[88:120])
	return normalizeCheckpointV4(cp)
}

func decodeCheckpointHeadEntry(b []byte) (*server.HeadEntry, error) {
	if len(b) == 0 {
		return nil, nil
	}
	var entry server.HeadEntry
	decoder := json.NewDecoder(bytes.NewReader(b))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&entry); err != nil {
		return nil, err
	}
	// Re-marshalling is both a trailing-token check and a canonicality check. It
	// rejects duplicate fields, alternate numeric forms, and unknown data rather
	// than accepting two byte representations for one durable proof.
	canonical, err := json.Marshal(&entry)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(canonical, b) {
		return nil, errors.New("HeadEntry is not in its canonical JSON encoding")
	}
	return &entry, nil
}

type authorityFloor struct {
	authority [32]byte
	revision  uint64
	digest    [32]byte
}

func authorityFloorKey(authority [32]byte) []byte {
	return append(key(keyAuthority), authority[:]...)
}

func encodeAuthorityFloor(floor authorityFloor) ([]byte, error) {
	if floor.authority == ([32]byte{}) || floor.revision == 0 {
		return nil, errors.New("follow: refusing to encode an invalid publication authority floor")
	}
	b := []byte{1}
	b = binary.BigEndian.AppendUint64(b, floor.revision)
	b = append(b, floor.digest[:]...)
	return b, nil
}

// stageAuthorityFloor raises one signer-local document floor in a caller-owned
// batch. The follower transition lock must serialize the read/modify/write, just
// as it does for the legacy updated_at floor.
func (s *state) stageAuthorityFloor(b *pebble.Batch, floor authorityFloor) error {
	if b == nil {
		return errors.New("follow: cannot stage a publication authority floor in a nil batch")
	}
	current, ok, err := s.authorityFloor(floor.authority)
	if err != nil {
		return err
	}
	if ok {
		switch {
		case floor.revision < current.revision:
			return fmt.Errorf("follow: refusing to lower publication authority floor from %d to %d", current.revision, floor.revision)
		case floor.revision == current.revision && floor.digest != current.digest:
			return fmt.Errorf("follow: refusing conflicting digests at publication revision %d", floor.revision)
		}
	}
	encoded, err := encodeAuthorityFloor(floor)
	if err != nil {
		return err
	}
	if err := b.Set(authorityFloorKey(floor.authority), encoded, nil); err != nil {
		return fmt.Errorf("follow: staging publication authority floor: %w", err)
	}
	return nil
}

// authorityFloor returns the admitted revision/digest for one verified document
// signing key. The presence of this record is also the durable mode bit: once it
// exists, a revisionless claim from the same authority is a downgrade forever.
func (s *state) authorityFloor(authority [32]byte) (authorityFloor, bool, error) {
	v, closer, err := s.kv.Get(authorityFloorKey(authority))
	if errors.Is(err, pebble.ErrNotFound) {
		return authorityFloor{}, false, nil
	}
	if err != nil {
		return authorityFloor{}, false, fmt.Errorf("follow: reading publication authority floor: %w", err)
	}
	defer closer.Close()
	if len(v) != 1+8+32 || v[0] != 1 {
		return authorityFloor{}, false, fmt.Errorf("follow: publication authority floor has an unsupported or truncated encoding")
	}
	floor := authorityFloor{authority: authority, revision: binary.BigEndian.Uint64(v[1:9])}
	copy(floor.digest[:], v[9:])
	if floor.revision == 0 {
		return authorityFloor{}, false, fmt.Errorf("follow: publication authority floor has revision 0")
	}
	return floor, true, nil
}

// checkpoint returns the head's authoritative generation, or ok=false for a head
// that has never checkpointed.
func (s *state) checkpoint(head string) (checkpoint, bool, error) {
	v, closer, err := s.kv.Get(append(key(keyCheckpoint), head...))
	if errors.Is(err, pebble.ErrNotFound) {
		return checkpoint{}, false, nil
	}
	if err != nil {
		return checkpoint{}, false, fmt.Errorf("follow: reading the checkpoint of head %q: %w", head, err)
	}
	defer closer.Close()
	cp, err := decodeCheckpoint(v)
	if err != nil {
		return checkpoint{}, false, fmt.Errorf("follow: head %q has an undecodable checkpoint: %w", head, err)
	}
	if (cp.version == checkpointVersionV3 || cp.version == checkpointVersionV4) && cp.published != nil && cp.published.Name != head {
		return checkpoint{}, false, fmt.Errorf("follow: head %q has a checkpoint retaining publication entry %q", head, cp.published.Name)
	}
	return cp, true, nil
}

// stageCheckpoint stages one authoritative checkpoint in a caller-owned batch.
// It deliberately does not commit: document-level adoption combines every
// selected/tombstone record, ordering floor, compatibility mirror, and any
// other transition state into one synchronous Pebble commit.
func (s *state) stageCheckpoint(b *pebble.Batch, head string, cp checkpoint) error {
	if b == nil {
		return errors.New("follow: cannot stage a checkpoint in a nil batch")
	}
	if head == "" {
		return errors.New("follow: refusing to checkpoint an empty head name")
	}
	if cp.updatedAt.Unix() < 0 {
		return fmt.Errorf("follow: refusing to checkpoint head %q with a document dated %s", head, cp.updatedAt)
	}
	if (cp.version == checkpointVersionV3 || cp.version == checkpointVersionV4) && cp.published != nil && cp.published.Name != head {
		return fmt.Errorf("follow: refusing to checkpoint head %q with publication entry %q", head, cp.published.Name)
	}
	encoded, err := encodeCheckpoint(cp)
	if err != nil {
		return fmt.Errorf("follow: encoding the checkpoint of head %q: %w", head, err)
	}
	if err := b.Set(append(key(keyCheckpoint), head...), encoded, nil); err != nil {
		return fmt.Errorf("follow: staging the checkpoint of head %q: %w", head, err)
	}
	return nil
}

// putCheckpoint commits cp as the head's authoritative generation in one synced
// batch, and in the same batch raises the global freshness floor to cp's
// authorizing time. The two writes are atomic so a crash after the commit cannot
// leave the checkpoint durable while an older document is still admissible (audit
// the safety boundary's freshness half): reading the global floor back is always at least the
// authorizing time of every committed checkpoint.
//
// The floor is read (for the max) before the batch, not inside it, so the
// read-modify-write is only correct under serialization: the Follower's transition
// lock provides it, held across every adopt-and-checkpoint transition (Poll and
// Resume), so no two legacy putCheckpoint calls -- and no document-level
// stageAdmission -- interleave their floor read and write. Without that a
// later-but-older transition
// could lower a floor a newer one raised.
func (s *state) putCheckpoint(head string, cp checkpoint) error {
	if !cp.root.Defined() {
		return fmt.Errorf("follow: refusing to checkpoint head %q with an undefined root", head)
	}
	if cp.updatedAt.Unix() < 0 {
		return fmt.Errorf("follow: refusing to checkpoint head %q with a document dated %s", head, cp.updatedAt)
	}

	b := s.kv.NewBatch()
	defer b.Close()
	if err := s.stageCheckpoint(b, head, cp); err != nil {
		return err
	}
	// updated_at remains the exact legacy ordering floor. A v2 checkpoint's
	// timestamp is diagnostic; its signer-local revision was atomically admitted
	// with the original checkpoint and must not contaminate the legacy clock.
	if cp.revision == 0 {
		cur, ok, err := s.updatedAt()
		if err != nil {
			return err
		}
		if !ok || cp.updatedAt.After(cur) {
			var u [8]byte
			binary.BigEndian.PutUint64(u[:], uint64(cp.updatedAt.Unix()))
			if err := b.Set(keyUpdatedAt, u[:], nil); err != nil {
				return fmt.Errorf("follow: staging the freshness floor for head %q: %w", head, err)
			}
		}
	}
	if err := b.Commit(pebble.Sync); err != nil {
		return fmt.Errorf("follow: committing the checkpoint of head %q: %w", head, err)
	}
	return nil
}

// stageRetireCheckpoint stages the deletion of head's checkpoint into b. It is
// the retirement half of the follower->writer promotion handoff (the safety boundary
// follow-up): the promotion materializes the compatibility mirrors from the
// checkpoint and deletes the checkpoint in the SAME synced batch, so the handoff
// is one atomic step. Retire-by-delete makes it one-way and rerun-safe by
// construction -- a crash before the batch commits changes nothing and the rerun
// reconciles again; a crash after leaves no checkpoint, so every later startup is
// a no-op and the promoted writer's own advancing root and manifest stand. It
// stages only; the caller commits b synchronously.
func (s *state) stageRetireCheckpoint(b *pebble.Batch, head string) error {
	if err := b.Delete(append(key(keyCheckpoint), head...), nil); err != nil {
		return fmt.Errorf("follow: staging the retirement of head %q's checkpoint: %w", head, err)
	}
	return nil
}

// legacySyncedTo returns the head's pre-checkpoint synced_to floor, if a follower
// built before the atomic checkpoint left one. It is consulted only when no
// checkpoint exists, as a retained anti-regression fact the first checkpoint must
// clear; this build never writes it. A head that never adopted a covered root has
// none, which is not zero: zero is a floor a document could regress to.
func (s *state) legacySyncedTo(head string) (uint64, bool, error) {
	v, ok, err := s.get(append(key(keySyncedTo), head...))
	if err != nil {
		return 0, false, fmt.Errorf("follow: reading the legacy synced_to floor of head %q: %w", head, err)
	}
	return v, ok, nil
}

// legacyManifestFloor returns the head's pre-checkpoint manifest tip floor (spec
// 11.3), if a pre-checkpoint follower left one. Like legacySyncedTo it is a
// retained fact, consulted only when no checkpoint exists and never written here.
// A head that never accepted a tip has none, which is not cid.Undef: undef is a
// floor no chain could descend from.
func (s *state) legacyManifestFloor(head string) (cid.Cid, bool, error) {
	v, closer, err := s.kv.Get(append(key(keyManifest), head...))
	if errors.Is(err, pebble.ErrNotFound) {
		return cid.Undef, false, nil
	}
	if err != nil {
		return cid.Undef, false, fmt.Errorf("follow: reading the legacy manifest floor of head %q: %w", head, err)
	}
	defer closer.Close()
	tip, err := cid.Cast(v)
	if err != nil {
		return cid.Undef, false, fmt.Errorf("follow: head %q has an undecodable legacy manifest floor: %w", head, err)
	}
	return tip, true, nil
}

// updatedAt returns the freshest document this node has accepted, by the clock
// of whoever wrote it. It is the global freshness floor: legacy single-head
// checkpoints raise it in putCheckpoint, while revisioned documents stage it in
// the same atomic batch as their complete adoption plan (stageAdmission), including
// documents whose plan contains no changed head.
func (s *state) updatedAt() (time.Time, bool, error) {
	v, ok, err := s.get(keyUpdatedAt)
	if err != nil {
		return time.Time{}, false, fmt.Errorf("follow: reading the updated_at floor: %w", err)
	}
	if !ok {
		return time.Time{}, false, nil
	}
	return time.Unix(int64(v), 0).UTC(), true, nil
}

type ipnsFloor struct {
	name ipns.Name
	seq  uint64
}

// ipnsFloors is an MRU-ordered, bounded set. DNSLink may delegate to a new IPNS
// name during rotation, so one global sequence is incorrect: sequences from
// independent keys have no ordering relationship. The bound prevents a
// repeatedly changed DNS record from growing local state forever. Evicting an
// old name does not reopen content rollback because the global document and
// per-head floors remain independently monotonic.
func (s *state) ipnsFloors() ([]ipnsFloor, error) {
	v, closer, err := s.kv.Get(keyIPNSFloors)
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("follow: reading IPNS sequence floors: %w", err)
	}
	defer closer.Close()
	if len(v) < 2 || v[0] != 1 {
		return nil, fmt.Errorf("follow: IPNS sequence floors have an unsupported or truncated encoding")
	}
	count := int(v[1])
	if count > maxIPNSFloorNames {
		return nil, fmt.Errorf("follow: IPNS sequence floors contain %d names, maximum is %d", count, maxIPNSFloorNames)
	}
	rest := v[2:]
	out := make([]ipnsFloor, 0, count)
	for i := 0; i < count; i++ {
		if len(rest) < 2 {
			return nil, fmt.Errorf("follow: IPNS sequence floor %d is truncated before its name", i)
		}
		n := int(binary.BigEndian.Uint16(rest[:2]))
		rest = rest[2:]
		if n == 0 || len(rest) < n+8 {
			return nil, fmt.Errorf("follow: IPNS sequence floor %d has an invalid name length %d", i, n)
		}
		name, err := ipns.NameFromString(string(rest[:n]))
		if err != nil {
			return nil, fmt.Errorf("follow: IPNS sequence floor %d has an invalid name: %w", i, err)
		}
		out = append(out, ipnsFloor{name: name, seq: binary.BigEndian.Uint64(rest[n : n+8])})
		rest = rest[n+8:]
	}
	if len(rest) != 0 {
		return nil, fmt.Errorf("follow: IPNS sequence floors have %d trailing bytes", len(rest))
	}
	return out, nil
}

func encodeIPNSFloors(floors []ipnsFloor) ([]byte, error) {
	if len(floors) > maxIPNSFloorNames {
		return nil, fmt.Errorf("follow: refusing to encode %d IPNS floors, maximum is %d", len(floors), maxIPNSFloorNames)
	}
	b := []byte{1, byte(len(floors))}
	for _, floor := range floors {
		name := floor.name.String()
		if len(name) == 0 || len(name) > int(^uint16(0)) {
			return nil, fmt.Errorf("follow: refusing to encode an invalid IPNS floor name")
		}
		b = binary.BigEndian.AppendUint16(b, uint16(len(name)))
		b = append(b, name...)
		b = binary.BigEndian.AppendUint64(b, floor.seq)
	}
	return b, nil
}

// ipnsSeq returns the floor for one IPNS name. allowLegacy imports the old
// single-name key for a statically configured direct-IPNS follower only; a
// DNSLink-selected replacement name must never inherit another key's sequence.
func (s *state) ipnsSeq(name ipns.Name, allowLegacy bool) (uint64, bool, error) {
	floors, err := s.ipnsFloors()
	if err != nil {
		return 0, false, err
	}
	for _, floor := range floors {
		if floor.name == name {
			return floor.seq, true, nil
		}
	}
	if !allowLegacy {
		return 0, false, nil
	}
	v, ok, err := s.get(keyIPNSSeq)
	if err != nil {
		return 0, false, fmt.Errorf("follow: reading the legacy IPNS sequence floor: %w", err)
	}
	return v, ok, nil
}

// setIPNSSeq raises one name's record floor and moves it to the MRU front. The
// currently committed DNS delegation is protected from bounded-set eviction;
// all content floors remain in force even for an older name that is evicted.
func (s *state) setIPNSSeq(name ipns.Name, seq uint64, allowLegacy bool) error {
	floors, err := s.ipnsFloors()
	if err != nil {
		return err
	}
	var cur uint64
	var ok, migrateLegacy bool
	for _, floor := range floors {
		if floor.name == name {
			cur, ok = floor.seq, true
			break
		}
	}
	if !ok && allowLegacy {
		cur, ok, err = s.get(keyIPNSSeq)
		if err != nil {
			return fmt.Errorf("follow: reading the legacy IPNS sequence floor: %w", err)
		}
		migrateLegacy = ok
	}
	if ok && seq < cur {
		return nil
	}
	updated := ipnsFloor{name: name, seq: max(seq, cur)}
	out := []ipnsFloor{updated}
	for _, floor := range floors {
		if floor.name != name {
			out = append(out, floor)
		}
	}
	if len(out) > maxIPNSFloorNames {
		protected, hasProtected, err := s.delegation()
		if err != nil {
			return err
		}
		drop := len(out) - 1
		if hasProtected && out[drop].name == protected.name {
			drop--
		}
		out = append(out[:drop], out[drop+1:]...)
	}
	encoded, err := encodeIPNSFloors(out)
	if err != nil {
		return err
	}
	b := s.kv.NewBatch()
	defer b.Close()
	if err := b.Set(keyIPNSFloors, encoded, nil); err != nil {
		return fmt.Errorf("follow: staging the IPNS sequence floor for %s: %w", name, err)
	}
	// The legacy key had no name. Once a direct-IPNS follower has assigned it
	// to its configured name, delete it in the same synced batch; otherwise a
	// later intentional name change would incorrectly inherit an unrelated
	// sequence forever.
	if migrateLegacy {
		if err := b.Delete(keyIPNSSeq, nil); err != nil {
			return fmt.Errorf("follow: staging legacy IPNS floor retirement: %w", err)
		}
	}
	if err := b.Commit(pebble.Sync); err != nil {
		return fmt.Errorf("follow: writing the IPNS sequence floor for %s: %w", name, err)
	}
	return nil
}

type delegation struct {
	name   ipns.Name
	pubkey []byte
}

func (s *state) delegation() (delegation, bool, error) {
	v, closer, err := s.kv.Get(keyDelegation)
	if errors.Is(err, pebble.ErrNotFound) {
		return delegation{}, false, nil
	}
	if err != nil {
		return delegation{}, false, fmt.Errorf("follow: reading DNSLink delegation: %w", err)
	}
	defer closer.Close()
	if len(v) < 3 || v[0] != 1 {
		return delegation{}, false, fmt.Errorf("follow: DNSLink delegation has an unsupported or truncated encoding")
	}
	n := int(binary.BigEndian.Uint16(v[1:3]))
	if n == 0 || len(v) != 3+n+32 {
		return delegation{}, false, fmt.Errorf("follow: DNSLink delegation has an invalid name or signer length")
	}
	name, err := ipns.NameFromString(string(v[3 : 3+n]))
	if err != nil {
		return delegation{}, false, fmt.Errorf("follow: DNSLink delegation has an invalid IPNS name: %w", err)
	}
	return delegation{name: name, pubkey: append([]byte(nil), v[3+n:]...)}, true, nil
}

func encodeDelegation(d delegation) ([]byte, error) {
	name := d.name.String()
	if len(name) == 0 || len(name) > int(^uint16(0)) || len(d.pubkey) != 32 {
		return nil, errors.New("follow: refusing to encode an invalid DNSLink delegation")
	}
	b := []byte{1}
	b = binary.BigEndian.AppendUint16(b, uint16(len(name)))
	b = append(b, name...)
	b = append(b, d.pubkey...)
	return b, nil
}

// commitAuthority is the legacy no-head-change admission used by the DNSLink
// state tests: it atomically raises updated_at and commits the exact
// IPNS-name/signer delegation used after DNS failure.
func (s *state) commitAuthority(t time.Time, d *delegation) error {
	return s.commitAdmission(nil, t, d, nil)
}

// commitAdmission makes a whole document's authoritative state one Pebble
// transaction: every changed head checkpoint, either the legacy updated_at floor
// or the revisioned signer/digest floor, and the DNSLink name/signer delegation.
// Compatibility mirrors and in-memory head exposure happen after this durable
// commit and are repaired from checkpoints on resume, so a crash cannot pair a
// newly admitted generation with an old ordering mode or signer (or vice versa).
func (s *state) commitAdmission(plans []adoptPlan, t time.Time, d *delegation, publication *authorityFloor) error {
	b := s.kv.NewBatch()
	defer b.Close()
	if err := s.stageAdmission(b, plans, t, d, publication); err != nil {
		return err
	}
	if err := b.Commit(pebble.Sync); err != nil {
		return fmt.Errorf("follow: committing document authority: %w", err)
	}
	return nil
}

// stageAdmission stages the ordering floor, delegation, and every selected or
// withdrawn checkpoint into a caller-owned batch. The follower uses it to join
// those authority records to the compatibility root/manifest mirrors in the
// same synchronous Pebble commit which AdoptBatch makes its durability barrier.
func (s *state) stageAdmission(b *pebble.Batch, plans []adoptPlan, t time.Time, d *delegation, publication *authorityFloor) error {
	if b == nil {
		return errors.New("follow: cannot stage document admission in a nil batch")
	}
	if t.Unix() < 0 {
		return fmt.Errorf("follow: refusing a document dated %s", t)
	}
	for _, plan := range plans {
		if !plan.writeCheckpoint {
			continue
		}
		if !plan.cp.root.Defined() && !((plan.cp.version == checkpointVersionV3 || plan.cp.version == checkpointVersionV4) && !plan.cp.selected) {
			return fmt.Errorf("follow: refusing to checkpoint head %q with an undefined root", plan.name)
		}
		if plan.cp.updatedAt.Unix() < 0 {
			return fmt.Errorf("follow: refusing to checkpoint head %q with a document dated %s", plan.name, plan.cp.updatedAt)
		}
		if publication == nil && plan.cp.revision != 0 {
			return fmt.Errorf("follow: refusing revisioned checkpoint for head %q without a publication authority floor", plan.name)
		}
		if publication != nil && (plan.cp.revision != publication.revision || plan.cp.authority != publication.authority ||
			plan.cp.digest != publication.digest) {
			return fmt.Errorf("follow: refusing checkpoint for head %q whose publication generation differs from the document authority floor", plan.name)
		}
		if plan.cp.revision != 0 && plan.cp.authority == ([32]byte{}) {
			return fmt.Errorf("follow: refusing revisioned checkpoint for head %q with an empty authority", plan.name)
		}
		if err := s.stageCheckpoint(b, plan.name, plan.cp); err != nil {
			return err
		}
	}
	if publication == nil {
		cur, ok, err := s.updatedAt()
		if err != nil {
			return err
		}
		if !ok || t.After(cur) {
			var encoded [8]byte
			binary.BigEndian.PutUint64(encoded[:], uint64(t.Unix()))
			if err := b.Set(keyUpdatedAt, encoded[:], nil); err != nil {
				return fmt.Errorf("follow: staging the updated_at floor: %w", err)
			}
		}
	} else {
		if err := s.stageAuthorityFloor(b, *publication); err != nil {
			return err
		}
	}
	if d != nil {
		encoded, err := encodeDelegation(*d)
		if err != nil {
			return err
		}
		if err := b.Set(keyDelegation, encoded, nil); err != nil {
			return fmt.Errorf("follow: staging DNSLink delegation: %w", err)
		}
	}
	return nil
}

func (s *state) get(k []byte) (uint64, bool, error) {
	v, closer, err := s.kv.Get(k)
	if errors.Is(err, pebble.ErrNotFound) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	defer closer.Close()
	if len(v) != 8 {
		return 0, false, fmt.Errorf("value is %d bytes, want 8", len(v))
	}
	return binary.BigEndian.Uint64(v), true, nil
}

// put writes a floor synchronously. A floor that reached only the page cache is
// a floor a crash removes, and the crash is exactly when it is needed: see the
// type comment.
func (s *state) put(k []byte, v uint64) error {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], v)
	return s.kv.Set(k, b[:], pebble.Sync)
}
