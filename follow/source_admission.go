package follow

import (
	"bytes"
	"errors"
	"fmt"
	"time"

	"github.com/cockroachdb/pebble/v2"

	"github.com/blobarchive/bloar/server"
)

type sourceDocumentAdmission struct {
	ref         sourceRef
	updatedAt   time.Time
	publication sourcePublicationFloor
	delegation  *delegation
}

func makeSourceDocumentAdmission(source *sourceRuntime, candidate *resolved) (sourceDocumentAdmission, error) {
	if source == nil || candidate == nil || !candidate.revisioned {
		return sourceDocumentAdmission{}, errors.New("follow: source admission requires a revisioned candidate")
	}
	if candidate.authority == ([32]byte{}) || candidate.revision == 0 {
		return sourceDocumentAdmission{}, fmt.Errorf("follow: source %q candidate has invalid publication provenance", source.cfg.ID)
	}
	if !bytes.Equal(candidate.authority[:], source.cfg.PubKey) {
		return sourceDocumentAdmission{}, fmt.Errorf("follow: source %q candidate authority differs from its pinned key", source.cfg.ID)
	}
	return sourceDocumentAdmission{
		ref: source.ref, updatedAt: candidate.updatedAt,
		publication: sourcePublicationFloor{revision: candidate.revision, digest: candidate.digest},
		delegation:  candidate.delegation,
	}, nil
}

// stageSourceAdmission joins every selected v4 checkpoint to the exact
// source-local publication floor and optional DNSLink delegation which
// authorized it. Sources whose valid document did not win a head still raise
// their own replay floor in the same batch; they cannot affect another source's
// floor or serving provenance.
func (s *state) stageSourceAdmission(b *pebble.Batch, archiveID server.ArchiveID, plans []adoptPlan, admissions []sourceDocumentAdmission) error {
	if b == nil {
		return errors.New("follow: cannot stage source admission in a nil batch")
	}
	if archiveID.IsZero() {
		return errors.New("follow: cannot stage source admission for an empty archive ID")
	}
	bySource := make(map[string]sourceDocumentAdmission, len(admissions))
	for _, admission := range admissions {
		if err := validateSourceRef(admission.ref); err != nil {
			return err
		}
		if admission.ref.archiveID != archiveID {
			return fmt.Errorf("follow: source %q admission belongs to archive %s, want %s", admission.ref.sourceID, admission.ref.archiveID, archiveID)
		}
		if admission.updatedAt.Unix() < 0 || admission.publication.revision == 0 {
			return fmt.Errorf("follow: source %q admission has invalid document generation", admission.ref.sourceID)
		}
		if _, duplicate := bySource[admission.ref.sourceID]; duplicate {
			return fmt.Errorf("follow: source admission repeats source %q", admission.ref.sourceID)
		}
		bySource[admission.ref.sourceID] = admission
	}

	for _, plan := range plans {
		if !plan.writeCheckpoint {
			continue
		}
		cp := plan.cp
		if cp.version != checkpointVersionV4 {
			return fmt.Errorf("follow: source admission refuses head %q checkpoint version %d", plan.name, cp.version)
		}
		if cp.archiveID != archiveID {
			return fmt.Errorf("follow: head %q checkpoint archive %s differs from source admission archive %s", plan.name, cp.archiveID, archiveID)
		}
		admission, ok := bySource[cp.sourceID]
		if !ok {
			return fmt.Errorf("follow: head %q checkpoint names source %q without an admitted document", plan.name, cp.sourceID)
		}
		binding, bound, err := s.sourceBinding(admission.ref)
		if err != nil {
			return err
		}
		if !bound || binding.pubkey != cp.authority {
			return fmt.Errorf("follow: head %q checkpoint authority is not the durable binding for source %q", plan.name, cp.sourceID)
		}
		if cp.revision != admission.publication.revision || cp.digest != admission.publication.digest {
			return fmt.Errorf("follow: head %q checkpoint generation differs from source %q publication floor", plan.name, cp.sourceID)
		}
		if !cp.root.Defined() && cp.selected {
			return fmt.Errorf("follow: selected source checkpoint for head %q has an undefined root", plan.name)
		}
		if cp.updatedAt.Unix() < 0 {
			return fmt.Errorf("follow: source checkpoint for head %q has an invalid timestamp", plan.name)
		}
		if err := s.stageCheckpoint(b, plan.name, cp); err != nil {
			return err
		}
	}

	for _, admission := range admissions {
		if err := s.stageSourcePublicationFloor(b, admission.ref, admission.publication); err != nil {
			return err
		}
		if admission.delegation != nil {
			if err := s.stageSourceDelegation(b, admission.ref, *admission.delegation); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *state) stageSourceObservations(b *pebble.Batch, observations []sourceChannelObs) error {
	if b == nil {
		return errors.New("follow: cannot stage source channel observations in a nil batch")
	}
	seen := make(map[sourceRef]struct{}, len(observations))
	for _, obs := range observations {
		if !obs.hasIPNSSeq {
			continue
		}
		if _, duplicate := seen[obs.ref]; duplicate {
			return fmt.Errorf("follow: source channel observations repeat source %q", obs.ref.sourceID)
		}
		seen[obs.ref] = struct{}{}
		if err := s.stageSourceIPNSSeq(b, obs.ref, obs.ipnsName, obs.ipnsSeq); err != nil {
			return err
		}
	}
	return nil
}
