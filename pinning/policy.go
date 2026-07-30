// Package pinning implements spec 9: the per-head retention policies, the
// reconciler that keeps the pin ledger equal to what those policies ask for,
// and the mark-and-sweep GC that deletes every block no pin reaches.
//
// # The ledger is the pin state
//
// There is no second pin database. A pin is a row in the pin ledger of spec 6.2
// (catalog.Ledger), reconciliation is what puts rows there, and GC marks from
// exactly those rows: "pinned" means "the ledger says so". This is why
// reconciliation's order (add, persist, remove) is crash-safe -- the add and
// the persist are the same write -- and why a crash can only ever leave extra
// rows, which cost retention rather than data.
//
// # Purposes
//
// Every pin is filed under a purpose, which is the ledger's second key
// component. A purpose is a lifetime: pins under different purposes come and go
// independently, so the reconciler can drop one kind of pin without disturbing
// another, and the diff stays readable.
//
//	root    the Head block of the current root. Recursive under full, which is
//	        the whole policy; direct otherwise.
//	index   blocks that keep the index complete without retaining blobs: every
//	        DirNode page, plus the Segments that fall outside what the policy
//	        retains. Always direct -- that is the point of the purpose.
//	window  sealed Segments inside a window policy's retention range, recursive,
//	        so the blobs they reference are retained too. A sealed segment moves
//	        from window to index as the window slides past it, which is one Add
//	        and one Remove and no change to the block.
//	open    the open Segment, which is rewritten on every apply and so churns
//	        far faster than anything else. Recursive under window, direct under
//	        none.
//	manifest the tip of the head's manifest chain (spec 10.5), recursive under
//	        every mode. It is a per-head pin like the four above, but the only one
//	        Desired does not compute from the head's structure: a Head does not
//	        link its manifest, so the tip is threaded in from outside (see
//	        Config.ManifestTip and PurposeManifest).
//
// A sixth, PurposeStaging, is not a policy's: it is filed under a reserved head
// name and belongs to the ingest path rather than to any head. See staging.go.
//
// # The GC cut
//
// At T0 the online collector takes Gate exclusively, reconciles the ledger,
// expires staging rows, snapshots the pins, and starts a protection epoch. It
// then releases Gate: mark and sweep run concurrently with application traffic.
// The closure of the T0 pin snapshot is M; successful application operations
// during the epoch add their multihashes to T; sweep retains M union T.
//
// Gate is library-level rather than daemon-level. The reconciler takes it around
// each pass, server.Heads around each publication transition,
// ingest.Ingester around each block-plus-staging transition, and the HTTP read
// API from root/tip selection through response materialization. Consequently an
// operation is wholly before T0 and represented in M, or wholly after it and
// protected through T; an old immutable snapshot selected before replacement
// also finishes reading before that replacement can be absent from a fresh M.
// Response writes happen after the read lease is released. A legacy collector
// without an epoch coordinator keeps the conservative fallback: Gate remains
// exclusive for its complete run.
//
// It used to be an HTTP middleware in cmd/bloard, which meant the exclusion was
// a property of arriving as a POST rather than of writing to the archive: an
// in-process stack -- the conformance suite's, an embedded daemon -- got none of
// it. The interfaces the two consumers declare (server.Gate, ingest.Gate) are
// satisfied by *Gate structurally, so the dependency still points this way and
// nothing above has to import this package to be safe.
//
// Lock order is gate -> the consumer's own lock (server.Heads.mu), never the
// reverse.
package pinning

import (
	"errors"
	"fmt"
	"time"

	"github.com/ipfs/go-cid"

	"github.com/blobarchive/bloar/archive"
)

// Mode is a retention policy's kind (spec 9).
type Mode int

const (
	// ModeFull retains everything: one recursive pin on the current root.
	ModeFull Mode = iota
	// ModeWindow retains the index in full and blobs for a trailing duration.
	ModeWindow
	// ModeNone retains the index in full and no blobs at all.
	ModeNone
)

func (m Mode) String() string {
	switch m {
	case ModeFull:
		return "full"
	case ModeWindow:
		return "window"
	case ModeNone:
		return "none"
	default:
		return fmt.Sprintf("Mode(%d)", int(m))
	}
}

// ParseMode parses the config spelling of a mode (spec 12).
func ParseMode(s string) (Mode, error) {
	switch s {
	case "full":
		return ModeFull, nil
	case "window":
		return ModeWindow, nil
	case "none":
		return ModeNone, nil
	default:
		return 0, fmt.Errorf("pinning: pin mode %q must be one of full, window, none", s)
	}
}

// Purposes: see the package comment for what each one means and why they are
// separate.
const (
	PurposeRoot   = "root"
	PurposeIndex  = "index"
	PurposeWindow = "window"
	PurposeOpen   = "open"
	// PurposeManifest is the tip of a head's manifest chain (spec 9, 10.5). It is
	// recursive under every retention mode -- the chain is a head's proof of what
	// it selected, negligible next to the blobs, and a mode that dropped it would
	// leave the head unverifiable -- and because each Manifest links its
	// predecessor through prev, the one pin protects the whole chain to genesis.
	//
	// Unlike the four above, this pin is not computed by Desired from the head's
	// structure: a Head object does not link its manifest (spec 10.5), so the tip
	// comes from outside the enumeration, threaded into the reconciler as a tip
	// lookup (Config.ManifestTip). The reconciler manages it like the rest -- add
	// the new tip's pin before removing the old one, so the chain stays protected
	// across an upgrade.
	PurposeManifest = "manifest"
)

// Policy is one head's retention policy.
type Policy struct {
	Mode Mode
	// Duration is the retention window. ModeWindow only.
	Duration time.Duration
	// SecondsPerSlot converts Duration into slots (spec 9's
	// dur/SECONDS_PER_SLOT). It is per-network (spec 12's
	// beacon.seconds_per_slot), not per-policy, and lives here because the
	// window arithmetic is the only thing that needs it. ModeWindow only.
	SecondsPerSlot uint64
}

// Full returns the policy that retains everything.
func Full() Policy { return Policy{Mode: ModeFull} }

// None returns the policy that retains the index and no blobs.
func None() Policy { return Policy{Mode: ModeNone} }

// Window returns the policy that retains blobs for the trailing d.
func Window(d time.Duration, secondsPerSlot uint64) Policy {
	return Policy{Mode: ModeWindow, Duration: d, SecondsPerSlot: secondsPerSlot}
}

// Validate rejects a policy that could not be evaluated.
func (p Policy) Validate() error {
	switch p.Mode {
	case ModeFull, ModeNone:
		return nil
	case ModeWindow:
		if p.Duration <= 0 {
			return fmt.Errorf("pinning: window policy has duration %s, must be positive", p.Duration)
		}
		if p.SecondsPerSlot == 0 {
			return errors.New("pinning: window policy has no seconds_per_slot; the retention window cannot be converted to slots")
		}
		return nil
	default:
		return fmt.Errorf("pinning: unknown pin mode %d", int(p.Mode))
	}
}

// WindowSlots is the policy's retention window in slots, spec 9's
// dur/SECONDS_PER_SLOT. It truncates: a duration that does not divide evenly
// retains the slots it fully covers.
func (p Policy) WindowSlots() uint64 {
	if p.Mode != ModeWindow || p.SecondsPerSlot == 0 {
		return 0
	}
	return uint64(p.Duration/time.Second) / p.SecondsPerSlot
}

// windowLow is the first slot of the retention range [synced_to - slots,
// synced_to]. A window longer than the head's history clamps to 0, which
// retains everything the head has -- and keeps retaining it as the head grows,
// which is the difference between this and a full policy.
func (p Policy) windowLow(syncedTo uint64) uint64 {
	if slots := p.WindowSlots(); slots < syncedTo {
		return syncedTo - slots
	}
	return 0
}

// Pin is one pin a policy asks for: a ledger row, once the reconciler writes it.
type Pin struct {
	Purpose   string
	CID       cid.Cid
	Recursive bool
}

// Desired computes the pin set a policy asks for over a head snapshot (spec 9).
//
// Every mode pins the whole index: an archive that cannot answer "was there a
// blob at this slot" is not an archive, and the answer for a slot whose segment
// was collected is indistinguishable from "no". Only blob retention differs,
// which is why every mode but full pins its Segments directly (the block, not
// what it points at) and only the retained ones recursively.
func Desired(p Policy, e *archive.Enumeration) ([]Pin, error) {
	if err := p.Validate(); err != nil {
		return nil, err
	}
	if e == nil || !e.Root.Defined() {
		return nil, errors.New("pinning: enumeration has no root")
	}

	if p.Mode == ModeFull {
		// One recursive pin reaches every block of the head, index and blobs
		// alike. Nothing else is needed and nothing else would be honest: a
		// second pin under a second purpose would suggest a lifetime that does
		// not exist.
		return []Pin{{Purpose: PurposeRoot, CID: e.Root, Recursive: true}}, nil
	}

	pins := make([]Pin, 0, 2+len(e.DirPages)+len(e.Sealed))
	pins = append(pins, Pin{Purpose: PurposeRoot, CID: e.Root})
	for _, page := range e.DirPages {
		pins = append(pins, Pin{Purpose: PurposeIndex, CID: page})
	}

	low := p.windowLow(e.SyncedTo)
	for _, s := range e.Sealed {
		// Intersection with [low, synced_to]: the upper test is free, because a
		// sealed window is one synced_to has passed the end of, so its first
		// slot is always at or before synced_to.
		if p.Mode == ModeWindow && s.LastSlot >= low {
			pins = append(pins, Pin{Purpose: PurposeWindow, CID: s.CID, Recursive: true})
			continue
		}
		pins = append(pins, Pin{Purpose: PurposeIndex, CID: s.CID})
	}
	if e.Open.Defined() {
		// The open window is always the most recent one, so a window policy
		// always retains it, however short the duration.
		pins = append(pins, Pin{Purpose: PurposeOpen, CID: e.Open, Recursive: p.Mode == ModeWindow})
	}
	return pins, nil
}
