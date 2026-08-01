package follow

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/ipfs/boxo/ipns"

	"github.com/blobarchive/bloar/server"
)

func sourceSetActivationForConfig(archiveID server.ArchiveID, set *SourceSetConfig) (sourceSetActivation, error) {
	if set == nil {
		return sourceSetActivation{}, errors.New("follow: source-set activation requires configuration")
	}
	activation := sourceSetActivation{
		marker:   sourceSetMarker{archiveID: archiveID, revision: set.Revision, digest: set.Digest},
		bindings: make([]sourceBinding, 0, len(set.Sources)),
	}
	for _, source := range set.Sources {
		binding := sourceBinding{sourceID: source.ID}
		copy(binding.pubkey[:], source.PubKey)
		activation.bindings = append(activation.bindings, binding)
	}
	if set.MigrateLegacySource != "" {
		migration := &sourceLegacyMigration{sourceID: set.MigrateLegacySource}
		if set.MigrateLegacyIPNS != "" {
			name, err := ipns.NameFromString(set.MigrateLegacyIPNS)
			if err != nil {
				return sourceSetActivation{}, fmt.Errorf("follow: migration source IPNS name: %w", err)
			}
			migration.directIPNSName = &name
		}
		activation.legacyMigration = migration
	}
	if _, err := validateSourceSetActivation(activation); err != nil {
		return sourceSetActivation{}, err
	}
	return activation, nil
}

// activateSourceSet applies the irreversible roster floor only after proving
// that it does not silently transfer a durable mutable head to another signer.
// Mutable revision order is authority-local and v4 checkpoints intentionally
// bind that authority; moving ownership therefore needs a future explicit
// migration protocol rather than a roster edit which can never be rolled back.
func (f *Follower) activateSourceSet(activation sourceSetActivation) error {
	for _, name := range f.Names() {
		if f.expectedKind(name) != server.UnfinalizedMutable {
			continue
		}
		owner := f.mutableSource(name)
		if owner == nil {
			return fmt.Errorf("follow: mutable head %q has no unique configured source", name)
		}
		cp, exists, err := f.state.checkpoint(name)
		if err != nil {
			return err
		}
		if !exists {
			continue
		}
		switch cp.version {
		case checkpointVersionV4:
			if cp.sourceID != owner.cfg.ID {
				return fmt.Errorf("follow: source-set activation would transfer durable mutable head %q from source %q to %q; mutable ownership transfer requires an explicit migration protocol",
					name, cp.sourceID, owner.cfg.ID)
			}
		case checkpointVersionV2, checkpointVersionV3:
			if cp.authority != ([32]byte{}) && !bytes.Equal(cp.authority[:], owner.cfg.PubKey) {
				return fmt.Errorf("follow: source-set activation would transfer legacy mutable head %q to source %q with a different authority; mutable ownership transfer requires an explicit migration protocol",
					name, owner.cfg.ID)
			}
		}
	}
	return f.state.activateSourceSet(activation)
}
