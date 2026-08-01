package server

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/ipfs/go-cid"

	"github.com/blobarchive/bloar/archive"
)

// DocVersion is the proof-aware publication document schema. Version 1 remains
// readable for finalized-only authorities. LogicalArchiveDocVersion adds the
// signed logical archive identity used to compare independently keyed writers.
const (
	LegacyDocVersion         = 1
	DocVersion               = 2
	LogicalArchiveDocVersion = 3
)

// SupportedDocVersion reports whether this build understands v's signed
// publication contract. Readers reject every other major version.
func SupportedDocVersion(v int) bool {
	return v == LegacyDocVersion || v == DocVersion || v == LogicalArchiveDocVersion
}

// ArchiveID is the stable, non-secret identity of one logical archive. It is
// shared by independent writers and survives signer, IPNS name, URL, and source
// membership changes. It domain-separates claims; it does not authorize the
// key which signed one. Followers authorize keys through their local trust
// configuration.
//
// The wire form is exactly 64 lowercase hex characters. A random 32-byte value
// makes accidental collision negligible while keeping identity independent of
// an evolving head set or key membership policy.
type ArchiveID [32]byte

// ParseArchiveID parses the canonical wire/config spelling of an ArchiveID.
func ParseArchiveID(s string) (ArchiveID, error) {
	var id ArchiveID
	if len(s) != hex.EncodedLen(len(id)) {
		return id, fmt.Errorf("server: archive_id has %d characters, want %d lowercase hex characters", len(s), hex.EncodedLen(len(id)))
	}
	if s != strings.ToLower(s) {
		return id, errors.New("server: archive_id must use lowercase hex")
	}
	raw, err := hex.DecodeString(s)
	if err != nil {
		return id, fmt.Errorf("server: archive_id is not hex: %w", err)
	}
	copy(id[:], raw)
	if id.IsZero() {
		return ArchiveID{}, errors.New("server: archive_id must not be all zeroes")
	}
	return id, nil
}

// String returns the canonical wire/config spelling of id.
func (id ArchiveID) String() string { return hex.EncodeToString(id[:]) }

// IsZero reports whether id is the reserved zero value.
func (id ArchiveID) IsZero() bool { return id == ArchiveID{} }

// MarshalJSON renders ArchiveID as its fixed-width lowercase hex string.
func (id ArchiveID) MarshalJSON() ([]byte, error) {
	if id.IsZero() {
		return nil, errors.New("server: archive_id must not be all zeroes")
	}
	return json.Marshal(id.String())
}

// UnmarshalJSON accepts only ArchiveID's canonical fixed-width lowercase hex
// string. JSON null is handled by the pointer field on Unsigned and is rejected
// by the version-3 contract as a missing identity.
func (id *ArchiveID) UnmarshalJSON(b []byte) error {
	if id == nil {
		return errors.New("server: cannot decode archive_id into a nil receiver")
	}
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return fmt.Errorf("server: archive_id must be a string: %w", err)
	}
	parsed, err := ParseArchiveID(s)
	if err != nil {
		return err
	}
	*id = parsed
	return nil
}

// HeadKind selects the authenticated ordering contract of one published head.
// An omitted kind is FinalizedMonotonic, preserving the exact JSON and signature
// bytes of every publication produced before mutable heads existed.
type HeadKind string

const (
	// FinalizedMonotonic is the legacy contract: origin_slot is fixed and
	// synced_to may only advance at a follower.
	FinalizedMonotonic HeadKind = "finalized-monotonic"
	// UnfinalizedMutable is a complete, bounded snapshot of the optimistic beacon
	// chain. A higher signed document revision replaces the whole snapshot, so
	// origin/coverage and rows may legitimately move backwards or disappear.
	UnfinalizedMutable HeadKind = "unfinalized-mutable"
)

// Doc is the publication document of spec 8, exactly as GET /bloar/v1/heads
// serves it. Pubkey and Signature are present iff the writer signs.
type Doc struct {
	Unsigned
	// Pubkey is the hex ed25519 public key the signature verifies against. It
	// says who signed, not whether to trust them: which key to trust is
	// out-of-band (spec 11.5).
	Pubkey string `json:"pubkey,omitempty"`
	// Signature is the hex ed25519 signature over Unsigned.Canonical().
	Signature string `json:"signature,omitempty"`
	// MaxPutBlobs is the archive's own POST /bloar/v1/blobs count limit (spec
	// 7.2), echoed here so an indexer can refuse to start when its configured
	// max_put_blobs exceeds it, rather than 400 every full put mid-run (spec
	// 10.1). It is the same value the Server enforces on that endpoint; both come
	// from server.max_put_blobs.
	//
	// It sits outside Unsigned deliberately: the signature exists so an
	// untrusting follower can trust the roots it adopts (spec 11.3), and no
	// follower acts on this field -- only an indexer does, and an indexer already
	// holds the archive's write token and trusts it. Omitted when zero, which is
	// also how a reader tells an archive that predates the field from one that
	// advertises a real limit -- there is no archive advertising zero, since the
	// server floors it at the spec default of 64.
	MaxPutBlobs int `json:"max_put_blobs,omitempty"`
}

// Unsigned is the part of the document a signature covers: everything except
// the signature and the key that made it.
type Unsigned struct {
	V   int    `json:"v"`
	Net string `json:"net"`
	// ArchiveID is present exactly in version 3. It is signed and stable across
	// independent writer identities; it is not a credential or a key roster.
	ArchiveID  *ArchiveID  `json:"archive_id,omitempty"`
	UpdatedAt  string      `json:"updated_at"`
	Multiaddrs []string    `json:"multiaddrs,omitempty"`
	Heads      []HeadEntry `json:"heads"`
	// Revision is a signer-local total order over publication documents. It is
	// appended and omitted so legacy finalized documents remain byte-identical.
	// Once present it, rather than UpdatedAt, orders documents from this signing
	// authority. Mutable heads require it.
	Revision *uint64 `json:"revision,omitempty"`
}

// HeadEntry is one head's line in the document, and the body of
// GET /bloar/v1/heads/{head}.
//
// Manifest is the CID of the tip of the head's manifest chain (spec 8, 10.5),
// appended after DirDepth and OMITTED entirely for a head with no chain -- never
// an explicit null. omitempty on a string is exactly that: the empty string
// disappears from the marshalled bytes, so a head that predates the manifest
// chain (the ALL head, the drill and dogfood heads) marshals to the bytes it did
// before, and its existing signature still verifies. That backward compatibility
// is the whole reason the field is omit-when-absent rather than null (spec 8).
type HeadEntry struct {
	Name       string  `json:"name"`
	Root       string  `json:"root"`
	OriginSlot uint64  `json:"origin_slot"`
	SyncedTo   *uint64 `json:"synced_to"`
	SegBits    uint64  `json:"seg_bits"`
	FanoutBits uint64  `json:"fanout_bits"`
	DirDepth   uint64  `json:"dir_depth"`
	Manifest   string  `json:"manifest,omitempty"`
	// Kind is omitted for the legacy finalized-monotonic contract. An explicit
	// value is signed and therefore cannot be stripped or changed by a relay.
	Kind HeadKind `json:"kind,omitempty"`
	// WindowStart is required for an unfinalized-mutable head and must equal its
	// root's origin_slot. The redundancy is intentional: it makes the bounded,
	// replaceable coverage contract explicit instead of silently changing the
	// long-standing meaning of origin_slot for finalized heads.
	WindowStart *uint64 `json:"window_start,omitempty"`
	// The remaining fields are the signed ancestry/handoff proof for an
	// unfinalized-mutable entry. Numeric fields are pointers so validation can
	// distinguish an explicitly observed slot zero from a stripped field.
	SourceHeadRoot      string  `json:"source_head_root,omitempty"`
	SourceFinalizedSlot *uint64 `json:"source_finalized_slot,omitempty"`
	SourceFinalizedRoot string  `json:"source_finalized_root,omitempty"`
	HandoffHead         string  `json:"handoff_head,omitempty"`
	HandoffRoot         string  `json:"handoff_root,omitempty"`
	HandoffSyncedTo     *uint64 `json:"handoff_synced_to,omitempty"`
}

// EffectiveKind returns the contract an entry carries. Omission is the legacy
// finalized contract, not an unknown kind.
func (e HeadEntry) EffectiveKind() HeadKind {
	if e.Kind == "" {
		return FinalizedMonotonic
	}
	return e.Kind
}

// ValidateContract checks the schema relationships that give head kind and
// document revision their meaning. It deliberately does not verify CIDs or the
// signature; callers do those at their existing trust boundaries.
func (d Doc) ValidateContract() error {
	if !SupportedDocVersion(d.V) {
		return fmt.Errorf("server: publication document version is %d, want %d, %d, or legacy %d",
			d.V, LogicalArchiveDocVersion, DocVersion, LegacyDocVersion)
	}
	switch d.V {
	case LogicalArchiveDocVersion:
		if d.ArchiveID == nil || d.ArchiveID.IsZero() {
			return errors.New("server: version 3 publication document requires a nonzero archive_id")
		}
		if d.Revision == nil {
			return errors.New("server: version 3 publication document requires a revision")
		}
		if d.Pubkey == "" || d.Signature == "" {
			return errors.New("server: version 3 publication document must be signed")
		}
	default:
		if d.ArchiveID != nil {
			return fmt.Errorf("server: version %d publication document carries version 3 archive_id", d.V)
		}
	}
	if d.Revision != nil {
		if *d.Revision == 0 {
			return errors.New("server: revisioned publication document has revision 0; revisions start at 1")
		}
		if d.Pubkey == "" || d.Signature == "" {
			return errors.New("server: revisioned publication document must be signed")
		}
	}

	seen := make(map[string]struct{}, len(d.Heads))
	for i, e := range d.Heads {
		if e.Name == "" {
			return fmt.Errorf("server: publication head %d has an empty name", i)
		}
		if _, duplicate := seen[e.Name]; duplicate {
			return fmt.Errorf("server: publication document contains duplicate head name %q", e.Name)
		}
		seen[e.Name] = struct{}{}

		switch e.EffectiveKind() {
		case FinalizedMonotonic:
			if e.WindowStart != nil {
				return fmt.Errorf("server: finalized-monotonic head %q carries window_start", e.Name)
			}
			// An explicit kind is a revisioned extension. Revisionless documents
			// retain the exact legacy schema, including omission of this field.
			if e.Kind != "" && d.Revision == nil {
				return fmt.Errorf("server: head %q declares kind without a document revision", e.Name)
			}
			if e.SourceHeadRoot != "" || e.SourceFinalizedSlot != nil || e.SourceFinalizedRoot != "" ||
				e.HandoffHead != "" || e.HandoffRoot != "" || e.HandoffSyncedTo != nil {
				return fmt.Errorf("server: finalized-monotonic head %q carries mutable proof fields", e.Name)
			}
		case UnfinalizedMutable:
			if d.V != DocVersion && d.V != LogicalArchiveDocVersion {
				return fmt.Errorf("server: version %d document cannot carry unfinalized-mutable head %q; version %d or %d proof fields are required",
					d.V, e.Name, DocVersion, LogicalArchiveDocVersion)
			}
			if d.Revision == nil {
				return fmt.Errorf("server: unfinalized-mutable head %q requires a document revision", e.Name)
			}
			if e.WindowStart == nil {
				return fmt.Errorf("server: unfinalized-mutable head %q requires window_start", e.Name)
			}
			if *e.WindowStart != e.OriginSlot {
				return fmt.Errorf("server: unfinalized-mutable head %q window_start %d differs from root origin_slot %d",
					e.Name, *e.WindowStart, e.OriginSlot)
			}
			if e.SyncedTo == nil {
				return fmt.Errorf("server: unfinalized-mutable head %q must publish a covered snapshot", e.Name)
			}
			if *e.SyncedTo < *e.WindowStart {
				return fmt.Errorf("server: unfinalized-mutable head %q ends at %d before window_start %d",
					e.Name, *e.SyncedTo, *e.WindowStart)
			}
			if e.Manifest != "" {
				return fmt.Errorf("server: unfinalized-mutable head %q cannot carry a finalized filter manifest", e.Name)
			}
			if e.SourceHeadRoot == "" || e.SourceFinalizedSlot == nil || e.SourceFinalizedRoot == "" ||
				e.HandoffHead == "" || e.HandoffRoot == "" || e.HandoffSyncedTo == nil {
				return fmt.Errorf("server: unfinalized-mutable head %q requires complete source and handoff proof fields", e.Name)
			}
			if _, err := parseBeaconRoot(e.SourceHeadRoot); err != nil {
				return fmt.Errorf("server: unfinalized-mutable head %q has invalid source_head_root: %w", e.Name, err)
			}
			if _, err := parseBeaconRoot(e.SourceFinalizedRoot); err != nil {
				return fmt.Errorf("server: unfinalized-mutable head %q has invalid source_finalized_root: %w", e.Name, err)
			}
			if _, err := cid.Decode(e.HandoffRoot); err != nil {
				return fmt.Errorf("server: unfinalized-mutable head %q has invalid handoff_root: %w", e.Name, err)
			}
			if *e.SourceFinalizedSlot > *e.SyncedTo {
				return fmt.Errorf("server: unfinalized-mutable head %q source finalized slot %d is above synced_to %d",
					e.Name, *e.SourceFinalizedSlot, *e.SyncedTo)
			}
			if *e.HandoffSyncedTo > *e.SourceFinalizedSlot {
				return fmt.Errorf("server: unfinalized-mutable head %q handoff synced_to %d is above source finalized slot %d",
					e.Name, *e.HandoffSyncedTo, *e.SourceFinalizedSlot)
			}
			if *e.HandoffSyncedTo != ^uint64(0) && *e.WindowStart > *e.HandoffSyncedTo+1 {
				return fmt.Errorf("server: unfinalized-mutable head %q window starts at %d beyond handoff synced_to %d",
					e.Name, *e.WindowStart, *e.HandoffSyncedTo)
			}
		default:
			return fmt.Errorf("server: head %q declares unknown kind %q", e.Name, e.Kind)
		}
	}
	// A mutable proof is meaningful only if the same authenticated document
	// carries the exact finalized generation it names. This also prevents a
	// relay from composing individually signed-looking entries from different
	// publication generations.
	byName := make(map[string]HeadEntry, len(d.Heads))
	for _, e := range d.Heads {
		byName[e.Name] = e
	}
	for _, e := range d.Heads {
		if e.EffectiveKind() != UnfinalizedMutable {
			continue
		}
		handoff, ok := byName[e.HandoffHead]
		if !ok || handoff.EffectiveKind() != FinalizedMonotonic || handoff.SyncedTo == nil ||
			handoff.Root != e.HandoffRoot || *handoff.SyncedTo != *e.HandoffSyncedTo {
			return fmt.Errorf("server: unfinalized-mutable head %q handoff proof does not match finalized head %q in the same document",
				e.Name, e.HandoffHead)
		}
	}
	return nil
}

// headEntry renders an archive snapshot as a document line. manifestTip is the
// head's manifest chain tip, cid.Undef for a head with no chain (which omits the
// field).
func headEntry(info archive.Info, manifestTip cid.Cid) HeadEntry {
	e := HeadEntry{
		Name:       info.Name,
		Root:       info.Root.String(),
		OriginSlot: info.OriginSlot,
		SyncedTo:   info.SyncedTo,
		SegBits:    info.SegBits,
		FanoutBits: info.FanoutBits,
		DirDepth:   info.DirDepth,
	}
	if manifestTip.Defined() {
		e.Manifest = manifestTip.String()
	}
	return e
}

// headEntryKind renders a durable registry line under its authenticated local
// contract. FinalizedMonotonic deliberately delegates to the legacy renderer so
// kind and window_start remain omitted byte-for-byte. Mutable generations are
// complete covered snapshots, so their origin is also the signed window start.
func headEntryKind(info archive.Info, manifestTip cid.Cid, kind HeadKind) HeadEntry {
	e := headEntry(info, manifestTip)
	if kind == UnfinalizedMutable {
		start := info.OriginSlot
		e.Kind = UnfinalizedMutable
		e.WindowStart = &start
	}
	return e
}

// headEntryGeneration renders a proof-aware mutable generation. The state is
// already validated before it can reach a registry entry; keeping the copying
// here makes the exact signed claim explicit and avoids deriving proof fields
// from whichever finalized entry happens to be current during a later rebuild.
func headEntryGeneration(info archive.Info, st GenerationState) HeadEntry {
	e := headEntryKind(info, cid.Undef, UnfinalizedMutable)
	sourceFinalized := st.SourceFinalizedSlot
	handoffSynced := st.HandoffSyncedTo
	e.SourceHeadRoot = st.SourceHeadRoot
	e.SourceFinalizedSlot = &sourceFinalized
	e.SourceFinalizedRoot = st.SourceFinalizedRoot
	e.HandoffHead = st.HandoffHead
	e.HandoffRoot = st.HandoffRoot
	e.HandoffSyncedTo = &handoffSynced
	return e
}

// Canonical returns the exact bytes a signature covers.
//
// # The canonicalization rule
//
// Canonical JSON here is defined as, and only as, encoding/json's marshalling
// of the Unsigned struct: no indentation, fields in struct-declaration order
// (v, net, archive_id when v3, updated_at, multiaddrs, heads, revision), Go's default HTML escaping on,
// multiaddrs omitted when empty, heads ordered by name, and synced_to rendered
// as null for an empty head. Nothing sorts keys or normalizes numbers at
// runtime, because the struct definition already fixes both.
//
// A verifier reproduces these bytes by unmarshalling the served document into
// a Doc and re-marshalling its embedded Unsigned -- which is what Verify does,
// and what a phase-8 follower must do. That round-trip is stable for every
// field: an omitted multiaddrs decodes to nil and re-encodes to omitted, an
// omitted head manifest decodes to the empty string and re-encodes to omitted,
// and no other field is optional. What a verifier MUST NOT do is re-serialize
// from its own struct or a generic map: field order and escaping would be its
// own, not this one's.
func (u Unsigned) Canonical() ([]byte, error) {
	b, err := json.Marshal(u)
	if err != nil {
		return nil, fmt.Errorf("server: rendering publication document: %w", err)
	}
	return b, nil
}

// CanonicalDigest is the stable identity used to distinguish an idempotent
// repeat from same-revision equivocation. It hashes the canonical signed claim,
// not raw transport JSON and not merely a head root.
func (u Unsigned) CanonicalDigest() ([32]byte, error) {
	b, err := u.Canonical()
	if err != nil {
		return [32]byte{}, err
	}
	return sha256.Sum256(b), nil
}

// sign returns d with the pubkey and signature of key attached.
func (u Unsigned) sign(key ed25519.PrivateKey) (Doc, error) {
	d := Doc{Unsigned: u}
	if key == nil {
		return d, nil
	}
	canonical, err := u.Canonical()
	if err != nil {
		return Doc{}, err
	}
	pub, ok := key.Public().(ed25519.PublicKey)
	if !ok {
		return Doc{}, errors.New("server: signing key has no ed25519 public key")
	}
	d.Pubkey = hex.EncodeToString(pub)
	d.Signature = hex.EncodeToString(ed25519.Sign(key, canonical))
	return d, nil
}

// Verify checks d's signature against the key d carries. It is the reference
// implementation of the rule Canonical documents; a follower still has to
// decide whether d.Pubkey is a key it trusts, which this cannot tell it.
//
// An unsigned document is an error here rather than a pass: a caller reaching
// for Verify wants a signature, and "there wasn't one" is not a verification.
func (d Doc) Verify() error {
	if d.Pubkey == "" || d.Signature == "" {
		return errors.New("server: publication document is not signed")
	}
	pub, err := hex.DecodeString(d.Pubkey)
	if err != nil {
		return fmt.Errorf("server: publication document has an undecodable pubkey: %w", err)
	}
	if len(pub) != ed25519.PublicKeySize {
		return fmt.Errorf("server: publication document pubkey is %d bytes, want %d", len(pub), ed25519.PublicKeySize)
	}
	sig, err := hex.DecodeString(d.Signature)
	if err != nil {
		return fmt.Errorf("server: publication document has an undecodable signature: %w", err)
	}
	canonical, err := d.Unsigned.Canonical()
	if err != nil {
		return err
	}
	if !ed25519.Verify(pub, canonical, sig) {
		return errors.New("server: publication document signature does not verify")
	}
	return nil
}
