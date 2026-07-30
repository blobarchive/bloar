package pinning

// This file is what the follower role (spec 11.3) needs from this package and
// the writer role does not. It is one method.

import (
	"errors"
	"fmt"
	"maps"
	"slices"

	"github.com/blobarchive/bloar/archive"
	"github.com/blobarchive/bloar/schema"
)

// Registration is one desired reconciler registration in a prepared follower
// generation. Name is explicit because Head may be nil: a nil Head is a
// desired-empty withdrawal which drains every old ledger row under Name before
// the name disappears. Policy must be valid for every registration, including
// a withdrawal; its zero value is Full.
type Registration struct {
	Name   string
	Head   *archive.Head
	Policy Policy
}

type preparedRegistration struct {
	name  string
	entry entry
}

// PrepareSetBatch validates a complete registration delta and returns its
// infallible visibility half. Preparation does not mutate the reconciler. The
// returned closure patches only the named registrations under one lock,
// preserving unrelated registrations added before it runs, recomputes Names
// once, and wakes reconciliation for every changed name.
//
// A nil Registration.Head installs a desired-empty tombstone rather than
// deleting the registration immediately. Reconciliation skips head and
// manifest enumeration for that tombstone, removes its old ledger rows, and
// deletes it only after every removal succeeds. Consequently a failed drain is
// retried by the normal timer and makes a GC cut fail closed instead of
// stranding unowned pin rows.
//
// Like Set, the apply closure does not acquire Gate: follower admission calls
// it inside the encompassing checkpoint/exposure transition's Gate lease.
func (r *Reconciler) PrepareSetBatch(registrations []Registration) (apply func(), err error) {
	prepared := make([]preparedRegistration, 0, len(registrations))
	seen := make(map[string]struct{}, len(registrations))
	for i, registration := range registrations {
		name := registration.Name
		if err := checkStagingName(name); err != nil {
			return nil, fmt.Errorf("pinning: registration %d: %w", i, err)
		}
		if err := schema.ValidateHeadName(name); err != nil {
			return nil, fmt.Errorf("pinning: registration %d: %w", i, err)
		}
		if _, duplicate := seen[name]; duplicate {
			return nil, fmt.Errorf("pinning: registration %d duplicates head %q in one batch", i, name)
		}
		seen[name] = struct{}{}
		if err := registration.Policy.Validate(); err != nil {
			return nil, fmt.Errorf("pinning: registration %d for head %q: %w", i, name, err)
		}

		if registration.Head == nil {
			prepared = append(prepared, preparedRegistration{
				name: name, entry: entry{policy: registration.Policy, withdrawal: &withdrawal{}},
			})
			continue
		}
		if got := registration.Head.Params().Name; got != name {
			return nil, fmt.Errorf("pinning: registration %d names head %q but its archive head is %q", i, name, got)
		}
		prepared = append(prepared, preparedRegistration{
			name: name, entry: entry{head: registration.Head, policy: registration.Policy},
		})
	}

	return func() {
		r.mu.Lock()
		for _, registration := range prepared {
			r.heads[registration.name] = registration.entry
			r.pending[registration.name] = true
		}
		if len(prepared) != 0 {
			r.names = slices.Sorted(maps.Keys(r.heads))
		}
		r.mu.Unlock()
		if len(prepared) != 0 {
			select {
			case r.wake <- struct{}{}:
			default:
			}
		}
	}, nil
}

// Set registers a head under a policy, replacing whatever was registered under
// its name. It is Add for a caller whose head object changes.
//
// A follower's does, on every adoption. A *archive.Head is one root's reader:
// the writer's engine swaps its own state in place as it mutates, so Add's
// entry stays current forever, but a follower adopts a root by loading a fresh
// engine over it (spec 11.3) and the entry Add made is then a reader of the
// previous root. Reconciliation would go on computing that root's pins --
// pinning blocks the adopted root has orphaned, and never pinning the ones it
// added -- while GC marked from them. Hence a replace, called with the same
// head the registry adopted, in the same step.
//
// Add stays what it is: a duplicate name from a config with two heads called
// the same thing is a mistake worth failing on, and a writer never has a second
// head object to register.
func (r *Reconciler) Set(head *archive.Head, p Policy) error {
	if head == nil {
		return errors.New("pinning: Set of a nil head")
	}
	apply, err := r.PrepareSetBatch([]Registration{{Name: head.Params().Name, Head: head, Policy: p}})
	if err != nil {
		return err
	}
	apply()
	return nil
}
