package follow

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"

	"github.com/cockroachdb/pebble/v2"
	"github.com/ipfs/boxo/ipns"

	"github.com/blobarchive/bloar/server"
)

// Source-set state is deliberately separate from the legacy single-source
// records in state.go. Activating a source set copies unambiguous legacy facts;
// it never moves or deletes them. That makes the transition auditable and
// copy-only. The initial v1 marker does not by itself make an actually old
// binary fail closed: New checks it before starting today's singular runtime,
// while rollout and checkpoint provenance prevent that downgrade. A store
// feature which old code must not ignore upgrades this same marker to a strict
// v2 encoding, as the conflict latch does below.
const (
	keySourceNamespace    = "source_"
	keySourceSet          = "source_set:v1"
	keySourceBinding      = "source_binding:v1:"
	keySourceSigner       = "source_signer:v1:"
	keySourcePublication  = "source_publication:v1:"
	keySourceIPNSFloors   = "source_ipns_floors:v1:"
	keySourceDelegation   = "source_delegation:v1:"
	maxSourceIDBytes      = 63
	maxSourceSetBindings  = 32
	sourceStateKeySep     = byte(':')
	sourceStateEncodingV1 = byte(1)
	sourceStateEncodingV2 = byte(2)

	// sourceSetFeatureConflictLatch makes durable cross-writer conflict state
	// part of the source-set contract. A marker carrying this bit uses the v2
	// encoding, so binaries which only understand v1 refuse to start rather than
	// silently ignoring an active or previously cleared latch.
	sourceSetFeatureConflictLatch uint64 = 1 << 0
	sourceSetKnownFeatures               = sourceSetFeatureConflictLatch
)

var sourceSetMarkerKey = key(keySourceSet)

// sourceRef is the durable identity of one publication source. sourceID is an
// operator-chosen stable name, not a transport name and not a signing key.
// Including archiveID in every source-local key prevents accidental state reuse
// if two logical archives choose the same source name.
type sourceRef struct {
	archiveID server.ArchiveID
	sourceID  string
}

type sourceBinding struct {
	sourceID string
	pubkey   [32]byte
}

// sourceSetMarker is the durable acknowledgement floor for the configured
// source roster. digest is computed by the configuration layer over its
// canonical source policy; this layer enforces its monotonic revision and
// preserves store capability bits which a roster update must never erase.
type sourceSetMarker struct {
	archiveID server.ArchiveID
	revision  uint64
	digest    [32]byte
	features  uint64
}

// sourceSetActivation describes one already validated and canonically hashed
// roster. legacyMigration is only consulted when no marker exists. It may stay
// present on later idempotent startups, which avoids making migration config a
// one-shot setting whose removal has to race the first successful restart.
type sourceSetActivation struct {
	marker          sourceSetMarker
	bindings        []sourceBinding
	legacyMigration *sourceLegacyMigration
}

// sourceLegacyMigration explicitly assigns the old process-wide source state
// to one stable source ID. directIPNSName is required only when the pre-name
// legacy fipns_seq key exists, since that key contains no name to migrate.
type sourceLegacyMigration struct {
	sourceID       string
	directIPNSName *ipns.Name
}

type sourcePublicationFloor struct {
	revision uint64
	digest   [32]byte
}

func validateSourceID(sourceID string) error {
	if len(sourceID) == 0 || len(sourceID) > maxSourceIDBytes {
		return fmt.Errorf("follow: source ID %q is %d bytes, want 1..%d", sourceID, len(sourceID), maxSourceIDBytes)
	}
	for i := range len(sourceID) {
		c := sourceID[i]
		alphaNumeric := c >= 'a' && c <= 'z' || c >= '0' && c <= '9'
		if !alphaNumeric && !(c == '-' && i != 0 && i != len(sourceID)-1) {
			return fmt.Errorf("follow: source ID %q must contain only lowercase ASCII letters, digits, and interior hyphens", sourceID)
		}
	}
	return nil
}

func validateSourceRef(ref sourceRef) error {
	if ref.archiveID.IsZero() {
		return errors.New("follow: source state requires a nonzero archive ID")
	}
	return validateSourceID(ref.sourceID)
}

func sourceScopedKey(prefix string, ref sourceRef) []byte {
	b := sourceArchivePrefix(prefix, ref.archiveID)
	b = append(b, ref.sourceID...)
	return b
}

func sourceArchivePrefix(prefix string, archiveID server.ArchiveID) []byte {
	b := make([]byte, 0, 1+len(prefix)+len(archiveID)+1)
	b = append(b, prefixFollow)
	b = append(b, prefix...)
	b = append(b, archiveID[:]...)
	b = append(b, sourceStateKeySep)
	return b
}

// sourceSignerKey is the reverse half of the one-to-one source identity. A
// signer which returns after retirement must use its original source ID and
// replay floors; binding the same key under a fresh ID would reset those floors.
func sourceSignerKey(archiveID server.ArchiveID, pubkey [32]byte) []byte {
	b := make([]byte, 0, 1+len(keySourceSigner)+len(archiveID)+1+len(pubkey))
	b = append(b, prefixFollow)
	b = append(b, keySourceSigner...)
	b = append(b, archiveID[:]...)
	b = append(b, sourceStateKeySep)
	b = append(b, pubkey[:]...)
	return b
}

func encodeSourceSetMarker(marker sourceSetMarker) ([]byte, error) {
	if marker.archiveID.IsZero() {
		return nil, errors.New("follow: source-set marker has an empty archive ID")
	}
	if marker.revision == 0 {
		return nil, errors.New("follow: source-set marker has revision 0")
	}
	if unknown := marker.features &^ sourceSetKnownFeatures; unknown != 0 {
		return nil, fmt.Errorf("follow: source-set marker has unknown feature bits 0x%x", unknown)
	}
	version := sourceStateEncodingV1
	capacity := 1 + 32 + 8 + 32
	if marker.features != 0 {
		version = sourceStateEncodingV2
		capacity += 8
	}
	b := make([]byte, 0, capacity)
	b = append(b, version)
	b = append(b, marker.archiveID[:]...)
	b = binary.BigEndian.AppendUint64(b, marker.revision)
	b = append(b, marker.digest[:]...)
	if version == sourceStateEncodingV2 {
		b = binary.BigEndian.AppendUint64(b, marker.features)
	}
	return b, nil
}

func decodeSourceSetMarker(b []byte) (sourceSetMarker, error) {
	if len(b) != 1+32+8+32 && len(b) != 1+32+8+32+8 {
		return sourceSetMarker{}, errors.New("follow: source-set marker has an unsupported or truncated encoding")
	}
	if b[0] == sourceStateEncodingV1 && len(b) != 1+32+8+32 ||
		b[0] == sourceStateEncodingV2 && len(b) != 1+32+8+32+8 ||
		b[0] != sourceStateEncodingV1 && b[0] != sourceStateEncodingV2 {
		return sourceSetMarker{}, errors.New("follow: source-set marker has an unsupported or truncated encoding")
	}
	var marker sourceSetMarker
	copy(marker.archiveID[:], b[1:33])
	marker.revision = binary.BigEndian.Uint64(b[33:41])
	copy(marker.digest[:], b[41:73])
	if b[0] == sourceStateEncodingV2 {
		marker.features = binary.BigEndian.Uint64(b[73:81])
		if marker.features == 0 {
			return sourceSetMarker{}, errors.New("follow: source-set marker v2 has no enabled features")
		}
	}
	if _, err := encodeSourceSetMarker(marker); err != nil {
		return sourceSetMarker{}, fmt.Errorf("follow: invalid source-set marker: %w", err)
	}
	return marker, nil
}

func (s *state) sourceSetMarker() (sourceSetMarker, bool, error) {
	v, closer, err := s.kv.Get(sourceSetMarkerKey)
	if errors.Is(err, pebble.ErrNotFound) {
		return sourceSetMarker{}, false, nil
	}
	if err != nil {
		return sourceSetMarker{}, false, fmt.Errorf("follow: reading source-set marker: %w", err)
	}
	defer closer.Close()
	marker, err := decodeSourceSetMarker(v)
	if err != nil {
		return sourceSetMarker{}, false, err
	}
	return marker, true, nil
}

func encodeSourceBinding(binding sourceBinding) ([]byte, error) {
	if err := validateSourceID(binding.sourceID); err != nil {
		return nil, err
	}
	if binding.pubkey == ([32]byte{}) {
		return nil, fmt.Errorf("follow: source %q has an empty pinned publication key", binding.sourceID)
	}
	b := []byte{sourceStateEncodingV1}
	b = append(b, binding.pubkey[:]...)
	return b, nil
}

func decodeSourceBinding(sourceID string, b []byte) (sourceBinding, error) {
	if len(b) != 1+32 || b[0] != sourceStateEncodingV1 {
		return sourceBinding{}, fmt.Errorf("follow: source %q binding has an unsupported or truncated encoding", sourceID)
	}
	binding := sourceBinding{sourceID: sourceID}
	copy(binding.pubkey[:], b[1:])
	if _, err := encodeSourceBinding(binding); err != nil {
		return sourceBinding{}, err
	}
	return binding, nil
}

func (s *state) sourceBinding(ref sourceRef) (sourceBinding, bool, error) {
	if err := validateSourceRef(ref); err != nil {
		return sourceBinding{}, false, err
	}
	v, closer, err := s.kv.Get(sourceScopedKey(keySourceBinding, ref))
	if errors.Is(err, pebble.ErrNotFound) {
		return sourceBinding{}, false, nil
	}
	if err != nil {
		return sourceBinding{}, false, fmt.Errorf("follow: reading source %q binding: %w", ref.sourceID, err)
	}
	defer closer.Close()
	binding, err := decodeSourceBinding(ref.sourceID, v)
	if err != nil {
		return sourceBinding{}, false, err
	}
	return binding, true, nil
}

func encodeSourceSignerBinding(sourceID string) ([]byte, error) {
	if err := validateSourceID(sourceID); err != nil {
		return nil, err
	}
	b := []byte{sourceStateEncodingV1, byte(len(sourceID))}
	b = append(b, sourceID...)
	return b, nil
}

func decodeSourceSignerBinding(b []byte) (string, error) {
	if len(b) < 2 || b[0] != sourceStateEncodingV1 || int(b[1]) != len(b)-2 {
		return "", errors.New("follow: source signer binding has an unsupported or truncated encoding")
	}
	sourceID := string(b[2:])
	if err := validateSourceID(sourceID); err != nil {
		return "", fmt.Errorf("follow: invalid source signer binding: %w", err)
	}
	return sourceID, nil
}

func (s *state) sourceSignerBinding(archiveID server.ArchiveID, pubkey [32]byte) (string, bool, error) {
	if archiveID.IsZero() {
		return "", false, errors.New("follow: source signer state requires a nonzero archive ID")
	}
	if pubkey == ([32]byte{}) {
		return "", false, errors.New("follow: source signer state requires a nonzero publication key")
	}
	v, closer, err := s.kv.Get(sourceSignerKey(archiveID, pubkey))
	if errors.Is(err, pebble.ErrNotFound) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("follow: reading source signer binding: %w", err)
	}
	defer closer.Close()
	sourceID, err := decodeSourceSignerBinding(v)
	if err != nil {
		return "", false, err
	}
	return sourceID, true, nil
}

// retainedSourceForSigner is the fail-closed integrity check for a missing
// reverse row. Normal existing signers take the O(1) reverse lookup; a truly new
// signer scans retained forward bindings once so a lost reverse half cannot make
// an old authority look new and reset its replay floors.
func (s *state) retainedSourceForSigner(archiveID server.ArchiveID, pubkey [32]byte) (string, bool, error) {
	lower := sourceArchivePrefix(keySourceBinding, archiveID)
	it, err := s.kv.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: prefixUpperBound(lower)})
	if err != nil {
		return "", false, fmt.Errorf("follow: scanning retained source bindings: %w", err)
	}
	defer it.Close()
	var found string
	for valid := it.First(); valid; valid = it.Next() {
		sourceID := string(it.Key()[len(lower):])
		binding, err := decodeSourceBinding(sourceID, it.Value())
		if err != nil {
			return "", false, err
		}
		if binding.pubkey != pubkey {
			continue
		}
		if found != "" && found != sourceID {
			return "", false, fmt.Errorf("follow: publication key has multiple retained forward bindings %q and %q", found, sourceID)
		}
		found = sourceID
	}
	if err := it.Error(); err != nil {
		return "", false, fmt.Errorf("follow: scanning retained source bindings: %w", err)
	}
	return found, found != "", nil
}

func validateSourceSetActivation(activation sourceSetActivation) ([]sourceBinding, error) {
	if _, err := encodeSourceSetMarker(activation.marker); err != nil {
		return nil, err
	}
	if len(activation.bindings) == 0 || len(activation.bindings) > maxSourceSetBindings {
		return nil, fmt.Errorf("follow: source set has %d bindings, want 1..%d", len(activation.bindings), maxSourceSetBindings)
	}
	bindings := append([]sourceBinding(nil), activation.bindings...)
	sort.Slice(bindings, func(i, j int) bool { return bindings[i].sourceID < bindings[j].sourceID })
	keys := make(map[[32]byte]string, len(bindings))
	for i, binding := range bindings {
		if _, err := encodeSourceBinding(binding); err != nil {
			return nil, err
		}
		if i != 0 && bindings[i-1].sourceID == binding.sourceID {
			return nil, fmt.Errorf("follow: source set repeats source ID %q", binding.sourceID)
		}
		if prior, ok := keys[binding.pubkey]; ok {
			return nil, fmt.Errorf("follow: sources %q and %q use the same pinned publication key", prior, binding.sourceID)
		}
		keys[binding.pubkey] = binding.sourceID
	}
	if migration := activation.legacyMigration; migration != nil {
		if err := validateSourceID(migration.sourceID); err != nil {
			return nil, fmt.Errorf("follow: invalid legacy migration source: %w", err)
		}
		found := false
		for _, binding := range bindings {
			if binding.sourceID == migration.sourceID {
				found = true
				break
			}
		}
		if !found {
			return nil, fmt.Errorf("follow: legacy migration names source %q, which is not in the active roster", migration.sourceID)
		}
		if migration.directIPNSName != nil {
			if _, err := checkedIPNSName(*migration.directIPNSName); err != nil {
				return nil, fmt.Errorf("follow: legacy migration has an invalid direct IPNS name: %w", err)
			}
		}
	}
	return bindings, nil
}

type sourceStateMutation struct {
	key   []byte
	value []byte
}

// activateSourceSet performs the irreversible source-set acknowledgement as one
// synchronous batch. Callers must serialize it with other follower transitions;
// normal use is once during startup, before pollers are launched.
func (s *state) activateSourceSet(activation sourceSetActivation) error {
	b := s.kv.NewBatch()
	defer b.Close()
	if err := s.stageSourceSetActivation(b, activation); err != nil {
		return err
	}
	if err := b.Commit(pebble.Sync); err != nil {
		return fmt.Errorf("follow: committing source-set activation: %w", err)
	}
	return nil
}

// stageSourceSetActivation exists so a later store-level transaction can join
// activation to other startup state. A staged batch is not visible until its
// caller commits it; activateSourceSet is the ordinary synced commit boundary.
func (s *state) stageSourceSetActivation(b *pebble.Batch, activation sourceSetActivation) error {
	if b == nil {
		return errors.New("follow: cannot stage source-set activation in a nil batch")
	}
	bindings, err := validateSourceSetActivation(activation)
	if err != nil {
		return err
	}
	marker, exists, err := s.sourceSetMarker()
	if err != nil {
		return err
	}
	if exists {
		if marker.archiveID != activation.marker.archiveID {
			return fmt.Errorf("follow: source-set archive ID cannot change from %s to %s", marker.archiveID, activation.marker.archiveID)
		}
		switch {
		case activation.marker.revision < marker.revision:
			return fmt.Errorf("follow: source-set revision rollback from %d to %d", marker.revision, activation.marker.revision)
		case activation.marker.revision == marker.revision && activation.marker.digest != marker.digest:
			return fmt.Errorf("follow: source-set revision %d has conflicting digests", marker.revision)
		case activation.marker.digest != marker.digest && activation.marker.revision <= marker.revision:
			return fmt.Errorf("follow: changed source-set digest requires a revision above %d", marker.revision)
		}
		// Source-set configuration controls roster revision and digest, not store
		// capabilities. Preserve every durable feature across an otherwise
		// ordinary roster activation so a restart cannot downgrade a latch-aware
		// store back to the v1 marker encoding.
		activation.marker.features = marker.features
	} else {
		if activation.marker.features != 0 {
			return errors.New("follow: a fresh source-set activation cannot enable store features")
		}
		orphaned, err := s.hasSourceScopedRows()
		if err != nil {
			return err
		}
		if orphaned {
			return errors.New("follow: source-scoped rows exist without a source-set marker")
		}
	}

	markerBytes, err := encodeSourceSetMarker(activation.marker)
	if err != nil {
		return err
	}
	mutations := []sourceStateMutation{{key: append([]byte(nil), sourceSetMarkerKey...), value: markerBytes}}
	for _, binding := range bindings {
		ref := sourceRef{archiveID: activation.marker.archiveID, sourceID: binding.sourceID}
		current, ok, err := s.sourceBinding(ref)
		if err != nil {
			return err
		}
		if ok && current.pubkey != binding.pubkey {
			return fmt.Errorf("follow: source ID %q is permanently bound to a different publication key", binding.sourceID)
		}
		signerSource, signerBound, err := s.sourceSignerBinding(activation.marker.archiveID, binding.pubkey)
		if err != nil {
			return err
		}
		if !signerBound {
			retainedSource, retained, err := s.retainedSourceForSigner(activation.marker.archiveID, binding.pubkey)
			if err != nil {
				return err
			}
			if retained {
				return fmt.Errorf("follow: publication key retains forward binding to source %q but its reverse binding is missing", retainedSource)
			}
		}
		if signerBound && signerSource != binding.sourceID {
			return fmt.Errorf("follow: publication key for source %q is permanently bound to source ID %q", binding.sourceID, signerSource)
		}
		if ok != signerBound {
			return fmt.Errorf("follow: source %q has only one half of its atomic forward/reverse binding", binding.sourceID)
		}
		// An unchanged digest cannot describe a newly added binding. This also
		// turns a missing row after an allegedly successful atomic activation
		// into a fail-closed corruption report rather than silently repairing it.
		if !ok && exists && activation.marker.digest == marker.digest {
			return fmt.Errorf("follow: unchanged source-set digest introduces unbound source %q", binding.sourceID)
		}
		if !ok {
			encoded, err := encodeSourceBinding(binding)
			if err != nil {
				return err
			}
			mutations = append(mutations, sourceStateMutation{key: sourceScopedKey(keySourceBinding, ref), value: encoded})
		}
		if !signerBound {
			encoded, err := encodeSourceSignerBinding(binding.sourceID)
			if err != nil {
				return err
			}
			mutations = append(mutations, sourceStateMutation{key: sourceSignerKey(activation.marker.archiveID, binding.pubkey), value: encoded})
		}
	}

	if !exists {
		legacy, err := s.readLegacySourceState()
		if err != nil {
			return err
		}
		if legacy.present() && activation.legacyMigration == nil {
			return errors.New("follow: legacy single-source state exists; source-set activation requires an explicit migration source")
		}
		if activation.legacyMigration != nil {
			migrationMutations, err := buildLegacySourceMigration(activation.marker.archiveID, bindings, *activation.legacyMigration, legacy)
			if err != nil {
				return err
			}
			mutations = append(mutations, migrationMutations...)
		}
	}

	for _, mutation := range mutations {
		if err := b.Set(mutation.key, mutation.value, nil); err != nil {
			return fmt.Errorf("follow: staging source-set activation: %w", err)
		}
	}
	return nil
}

func (s *state) hasSourceScopedRows() (bool, error) {
	// marker absence means no source-set generation has ever committed, so any
	// current or future record in the common namespace is orphaned. Scanning the
	// namespace rather than today's known keys keeps future schema additions from
	// being silently treated as an empty store by an older activation path.
	return s.hasKeyPrefix(key(keySourceNamespace))
}

func prefixUpperBound(prefix []byte) []byte {
	upper := append([]byte(nil), prefix...)
	for i := len(upper) - 1; i >= 0; i-- {
		if upper[i] != 0xff {
			upper[i]++
			return upper[:i+1]
		}
	}
	return nil
}

type legacySourceState struct {
	authority *authorityFloor
	ipns      []ipnsFloor
	unnamed   *uint64
	delegate  *delegation
	other     bool
}

func (legacy legacySourceState) present() bool {
	return legacy.authority != nil || len(legacy.ipns) != 0 || legacy.unnamed != nil || legacy.delegate != nil || legacy.other
}

func (s *state) readLegacySourceState() (legacySourceState, error) {
	legacy := legacySourceState{}
	if _, closer, err := s.kv.Get(keyUpdatedAt); err == nil {
		legacy.other = true
		if err := closer.Close(); err != nil {
			return legacy, fmt.Errorf("follow: closing legacy updated_at floor: %w", err)
		}
	} else if !errors.Is(err, pebble.ErrNotFound) {
		return legacy, fmt.Errorf("follow: reading legacy updated_at floor: %w", err)
	}
	for _, prefix := range []string{keyCheckpoint, keySyncedTo, keyManifest} {
		has, err := s.hasKeyPrefix(key(prefix))
		if err != nil {
			return legacy, err
		}
		legacy.other = legacy.other || has
	}
	lower := key(keyAuthority)
	it, err := s.kv.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: prefixUpperBound(lower)})
	if err != nil {
		return legacy, fmt.Errorf("follow: scanning legacy publication authority floors: %w", err)
	}
	for valid := it.First(); valid; valid = it.Next() {
		if len(it.Key()) != len(lower)+32 {
			_ = it.Close()
			return legacy, errors.New("follow: legacy publication authority floor has an invalid key")
		}
		if legacy.authority != nil {
			_ = it.Close()
			return legacy, errors.New("follow: multiple legacy publication authorities are ambiguous; source-set migration requires exactly one")
		}
		var authority [32]byte
		copy(authority[:], it.Key()[len(lower):])
		floor, err := decodeAuthorityFloorValue(authority, it.Value())
		if err != nil {
			_ = it.Close()
			return legacy, err
		}
		legacy.authority = &floor
	}
	if err := it.Error(); err != nil {
		_ = it.Close()
		return legacy, fmt.Errorf("follow: scanning legacy publication authority floors: %w", err)
	}
	if err := it.Close(); err != nil {
		return legacy, fmt.Errorf("follow: closing legacy publication authority scan: %w", err)
	}
	legacy.ipns, err = s.ipnsFloors()
	if err != nil {
		return legacy, err
	}
	if seq, ok, err := s.get(keyIPNSSeq); err != nil {
		return legacy, fmt.Errorf("follow: reading unnamed legacy IPNS floor: %w", err)
	} else if ok {
		legacy.unnamed = &seq
	}
	if d, ok, err := s.delegation(); err != nil {
		return legacy, err
	} else if ok {
		legacy.delegate = &d
	}
	return legacy, nil
}

func (s *state) hasKeyPrefix(prefix []byte) (bool, error) {
	it, err := s.kv.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: prefixUpperBound(prefix)})
	if err != nil {
		return false, fmt.Errorf("follow: opening legacy state scan for %q: %w", prefix, err)
	}
	has := it.First()
	iterErr := it.Error()
	closeErr := it.Close()
	if iterErr != nil {
		return false, fmt.Errorf("follow: scanning legacy state for %q: %w", prefix, iterErr)
	}
	if closeErr != nil {
		return false, fmt.Errorf("follow: closing legacy state scan for %q: %w", prefix, closeErr)
	}
	return has, nil
}

func decodeAuthorityFloorValue(authority [32]byte, b []byte) (authorityFloor, error) {
	if len(b) != 1+8+32 || b[0] != 1 {
		return authorityFloor{}, errors.New("follow: legacy publication authority floor has an unsupported or truncated encoding")
	}
	floor := authorityFloor{authority: authority, revision: binary.BigEndian.Uint64(b[1:9])}
	copy(floor.digest[:], b[9:])
	if floor.revision == 0 || floor.authority == ([32]byte{}) {
		return authorityFloor{}, errors.New("follow: legacy publication authority floor is invalid")
	}
	return floor, nil
}

func buildLegacySourceMigration(archiveID server.ArchiveID, bindings []sourceBinding, migration sourceLegacyMigration, legacy legacySourceState) ([]sourceStateMutation, error) {
	var binding sourceBinding
	found := false
	for _, candidate := range bindings {
		if candidate.sourceID == migration.sourceID {
			binding, found = candidate, true
			break
		}
	}
	if !found {
		return nil, fmt.Errorf("follow: legacy migration source %q is not bound", migration.sourceID)
	}
	ref := sourceRef{archiveID: archiveID, sourceID: migration.sourceID}
	var mutations []sourceStateMutation
	if legacy.authority != nil {
		if legacy.authority.authority != binding.pubkey {
			return nil, fmt.Errorf("follow: legacy publication authority does not match source %q's pinned key", migration.sourceID)
		}
		floor := sourcePublicationFloor{revision: legacy.authority.revision, digest: legacy.authority.digest}
		encoded, err := encodeSourcePublicationFloor(floor)
		if err != nil {
			return nil, err
		}
		mutations = append(mutations, sourceStateMutation{key: sourceScopedKey(keySourcePublication, ref), value: encoded})
	}
	if legacy.delegate != nil && !bytes.Equal(legacy.delegate.pubkey, binding.pubkey[:]) {
		return nil, fmt.Errorf("follow: legacy DNSLink delegation signer does not match source %q's pinned key", migration.sourceID)
	}
	floors := append([]ipnsFloor(nil), legacy.ipns...)
	if legacy.unnamed != nil {
		if migration.directIPNSName == nil {
			return nil, errors.New("follow: unnamed legacy IPNS floor requires an explicit direct IPNS name")
		}
		name := *migration.directIPNSName
		merged := ipnsFloor{name: name, seq: *legacy.unnamed}
		out := []ipnsFloor{merged}
		for _, floor := range floors {
			if floor.name == name {
				out[0].seq = max(out[0].seq, floor.seq)
				continue
			}
			out = append(out, floor)
		}
		floors = out
	}
	if len(floors) > maxIPNSFloorNames {
		return nil, fmt.Errorf("follow: legacy migration has %d distinct IPNS names, maximum is %d", len(floors), maxIPNSFloorNames)
	}
	if len(floors) != 0 {
		encoded, err := encodeIPNSFloors(floors)
		if err != nil {
			return nil, err
		}
		mutations = append(mutations, sourceStateMutation{key: sourceScopedKey(keySourceIPNSFloors, ref), value: encoded})
	}
	if legacy.delegate != nil {
		encoded, err := encodeDelegation(*legacy.delegate)
		if err != nil {
			return nil, err
		}
		mutations = append(mutations, sourceStateMutation{key: sourceScopedKey(keySourceDelegation, ref), value: encoded})
	}
	return mutations, nil
}

func encodeSourcePublicationFloor(floor sourcePublicationFloor) ([]byte, error) {
	if floor.revision == 0 {
		return nil, errors.New("follow: source publication floor has revision 0")
	}
	b := []byte{sourceStateEncodingV1}
	b = binary.BigEndian.AppendUint64(b, floor.revision)
	b = append(b, floor.digest[:]...)
	return b, nil
}

func decodeSourcePublicationFloor(b []byte) (sourcePublicationFloor, error) {
	if len(b) != 1+8+32 || b[0] != sourceStateEncodingV1 {
		return sourcePublicationFloor{}, errors.New("follow: source publication floor has an unsupported or truncated encoding")
	}
	floor := sourcePublicationFloor{revision: binary.BigEndian.Uint64(b[1:9])}
	copy(floor.digest[:], b[9:])
	if _, err := encodeSourcePublicationFloor(floor); err != nil {
		return sourcePublicationFloor{}, err
	}
	return floor, nil
}

func (s *state) sourcePublicationFloor(ref sourceRef) (sourcePublicationFloor, bool, error) {
	if err := validateSourceRef(ref); err != nil {
		return sourcePublicationFloor{}, false, err
	}
	v, closer, err := s.kv.Get(sourceScopedKey(keySourcePublication, ref))
	if errors.Is(err, pebble.ErrNotFound) {
		return sourcePublicationFloor{}, false, nil
	}
	if err != nil {
		return sourcePublicationFloor{}, false, fmt.Errorf("follow: reading source %q publication floor: %w", ref.sourceID, err)
	}
	defer closer.Close()
	floor, err := decodeSourcePublicationFloor(v)
	if err != nil {
		return sourcePublicationFloor{}, false, fmt.Errorf("follow: source %q: %w", ref.sourceID, err)
	}
	return floor, true, nil
}

// stageSourcePublicationFloor may be called at most once for ref in one batch:
// a plain Pebble Batch cannot read its pending writes. Callers must precompute
// one maximum admitted generation per source before calling this method.
func (s *state) stageSourcePublicationFloor(b *pebble.Batch, ref sourceRef, floor sourcePublicationFloor) error {
	if b == nil {
		return errors.New("follow: cannot stage a source publication floor in a nil batch")
	}
	if _, ok, err := s.sourceBinding(ref); err != nil {
		return err
	} else if !ok {
		return fmt.Errorf("follow: source %q has no durable binding", ref.sourceID)
	}
	current, ok, err := s.sourcePublicationFloor(ref)
	if err != nil {
		return err
	}
	if ok {
		switch {
		case floor.revision < current.revision:
			return fmt.Errorf("follow: refusing to lower source %q publication floor from %d to %d", ref.sourceID, current.revision, floor.revision)
		case floor.revision == current.revision && floor.digest != current.digest:
			return fmt.Errorf("follow: source %q has conflicting digests at publication revision %d", ref.sourceID, floor.revision)
		}
	}
	encoded, err := encodeSourcePublicationFloor(floor)
	if err != nil {
		return err
	}
	if err := b.Set(sourceScopedKey(keySourcePublication, ref), encoded, nil); err != nil {
		return fmt.Errorf("follow: staging source %q publication floor: %w", ref.sourceID, err)
	}
	return nil
}

func decodeSourceIPNSFloors(b []byte) ([]ipnsFloor, error) {
	if len(b) < 2 || b[0] != 1 {
		return nil, errors.New("follow: source IPNS sequence floors have an unsupported or truncated encoding")
	}
	count := int(b[1])
	if count > maxIPNSFloorNames {
		return nil, fmt.Errorf("follow: source IPNS sequence floors contain %d names, maximum is %d", count, maxIPNSFloorNames)
	}
	rest := b[2:]
	out := make([]ipnsFloor, 0, count)
	for i := 0; i < count; i++ {
		if len(rest) < 2 {
			return nil, fmt.Errorf("follow: source IPNS sequence floor %d is truncated before its name", i)
		}
		n := int(binary.BigEndian.Uint16(rest[:2]))
		rest = rest[2:]
		if n == 0 || len(rest) < n+8 {
			return nil, fmt.Errorf("follow: source IPNS sequence floor %d has an invalid name length %d", i, n)
		}
		name, err := ipns.NameFromString(string(rest[:n]))
		if err != nil {
			return nil, fmt.Errorf("follow: source IPNS sequence floor %d has an invalid name: %w", i, err)
		}
		out = append(out, ipnsFloor{name: name, seq: binary.BigEndian.Uint64(rest[n : n+8])})
		rest = rest[n+8:]
	}
	if len(rest) != 0 {
		return nil, fmt.Errorf("follow: source IPNS sequence floors have %d trailing bytes", len(rest))
	}
	return out, nil
}

func (s *state) sourceIPNSFloors(ref sourceRef) ([]ipnsFloor, error) {
	if err := validateSourceRef(ref); err != nil {
		return nil, err
	}
	v, closer, err := s.kv.Get(sourceScopedKey(keySourceIPNSFloors, ref))
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("follow: reading source %q IPNS floors: %w", ref.sourceID, err)
	}
	defer closer.Close()
	return decodeSourceIPNSFloors(v)
}

func (s *state) sourceIPNSSeq(ref sourceRef, name ipns.Name) (uint64, bool, error) {
	if _, err := checkedIPNSName(name); err != nil {
		return 0, false, err
	}
	floors, err := s.sourceIPNSFloors(ref)
	if err != nil {
		return 0, false, err
	}
	for _, floor := range floors {
		if floor.name == name {
			return floor.seq, true, nil
		}
	}
	return 0, false, nil
}

// stageSourceIPNSSeq may be called at most once for a source in one batch: all
// of that source's name floors share one durable MRU record. Callers must
// precompute the complete one-source MRU update and stage it once; a plain
// Pebble Batch cannot make a second call observe the first call's write.
func (s *state) stageSourceIPNSSeq(b *pebble.Batch, ref sourceRef, name ipns.Name, seq uint64) error {
	if b == nil {
		return errors.New("follow: cannot stage a source IPNS floor in a nil batch")
	}
	if _, err := checkedIPNSName(name); err != nil {
		return err
	}
	if _, ok, err := s.sourceBinding(ref); err != nil {
		return err
	} else if !ok {
		return fmt.Errorf("follow: source %q has no durable binding", ref.sourceID)
	}
	floors, err := s.sourceIPNSFloors(ref)
	if err != nil {
		return err
	}
	updated := ipnsFloor{name: name, seq: seq}
	out := []ipnsFloor{updated}
	for _, floor := range floors {
		if floor.name == name {
			out[0].seq = max(out[0].seq, floor.seq)
			continue
		}
		out = append(out, floor)
	}
	if len(out) > maxIPNSFloorNames {
		protected, hasProtected, err := s.sourceDelegation(ref)
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
	if err := b.Set(sourceScopedKey(keySourceIPNSFloors, ref), encoded, nil); err != nil {
		return fmt.Errorf("follow: staging source %q IPNS floor: %w", ref.sourceID, err)
	}
	return nil
}

func checkedIPNSName(name ipns.Name) (string, error) {
	if !name.Cid().Defined() {
		return "", errors.New("follow: invalid empty IPNS name")
	}
	raw := name.String()
	parsed, err := ipns.NameFromString(raw)
	if err != nil || parsed != name {
		return "", errors.New("follow: invalid non-canonical IPNS name")
	}
	return raw, nil
}

func decodeSourceDelegation(b []byte) (delegation, error) {
	if len(b) < 3 || b[0] != 1 {
		return delegation{}, errors.New("follow: source DNSLink delegation has an unsupported or truncated encoding")
	}
	n := int(binary.BigEndian.Uint16(b[1:3]))
	if n == 0 || len(b) != 3+n+32 {
		return delegation{}, errors.New("follow: source DNSLink delegation has an invalid name or signer length")
	}
	name, err := ipns.NameFromString(string(b[3 : 3+n]))
	if err != nil {
		return delegation{}, fmt.Errorf("follow: source DNSLink delegation has an invalid IPNS name: %w", err)
	}
	return delegation{name: name, pubkey: append([]byte(nil), b[3+n:]...)}, nil
}

func (s *state) sourceDelegation(ref sourceRef) (delegation, bool, error) {
	if err := validateSourceRef(ref); err != nil {
		return delegation{}, false, err
	}
	v, closer, err := s.kv.Get(sourceScopedKey(keySourceDelegation, ref))
	if errors.Is(err, pebble.ErrNotFound) {
		return delegation{}, false, nil
	}
	if err != nil {
		return delegation{}, false, fmt.Errorf("follow: reading source %q DNSLink delegation: %w", ref.sourceID, err)
	}
	defer closer.Close()
	d, err := decodeSourceDelegation(v)
	if err != nil {
		return delegation{}, false, fmt.Errorf("follow: source %q: %w", ref.sourceID, err)
	}
	binding, ok, err := s.sourceBinding(ref)
	if err != nil {
		return delegation{}, false, err
	}
	if !ok {
		return delegation{}, false, fmt.Errorf("follow: source %q DNSLink delegation has no durable source binding", ref.sourceID)
	}
	if !bytes.Equal(d.pubkey, binding.pubkey[:]) {
		return delegation{}, false, fmt.Errorf("follow: source %q DNSLink delegation signer differs from its durable source binding", ref.sourceID)
	}
	return d, true, nil
}

func (s *state) stageSourceDelegation(b *pebble.Batch, ref sourceRef, d delegation) error {
	if b == nil {
		return errors.New("follow: cannot stage a source DNSLink delegation in a nil batch")
	}
	binding, ok, err := s.sourceBinding(ref)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("follow: source %q has no durable binding", ref.sourceID)
	}
	if !bytes.Equal(d.pubkey, binding.pubkey[:]) {
		return fmt.Errorf("follow: source %q DNSLink delegation signer differs from its pinned publication key", ref.sourceID)
	}
	encoded, err := encodeDelegation(d)
	if err != nil {
		return err
	}
	if err := b.Set(sourceScopedKey(keySourceDelegation, ref), encoded, nil); err != nil {
		return fmt.Errorf("follow: staging source %q DNSLink delegation: %w", ref.sourceID, err)
	}
	return nil
}
