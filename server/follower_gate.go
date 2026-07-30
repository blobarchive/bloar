package server

import (
	"errors"
	"reflect"
)

// ValidateFollowerGate proves that follower publication and HTTP reader
// leases use the same gate instance. External archive retention depends on
// this identity: after AdoptBatch replaces a serving pointer, the follower
// drains that gate before it allows the external store to unpin the retired
// generation. Two independently constructed gates would make both halves look
// synchronized while leaving the old-reader/unpin race open.
//
// Followers pass a pointer-backed gate. The pointer requirement makes identity
// explicit and rejects the registry's value-backed noGate rather than treating
// two no-op values as shared coordination.
func (h *Heads) ValidateFollowerGate(gate Gate) error {
	if gate == nil {
		return errors.New("server: follower gate must not be nil")
	}
	configured := reflect.ValueOf(h.cfg.Gate)
	candidate := reflect.ValueOf(gate)
	if !configured.IsValid() || !candidate.IsValid() ||
		configured.Kind() != reflect.Pointer || candidate.Kind() != reflect.Pointer ||
		configured.Type() != candidate.Type() || configured.Pointer() != candidate.Pointer() {
		return errors.New("server: follower and registry reader leases do not share one gate")
	}
	return nil
}
