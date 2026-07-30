package main

import "github.com/blobarchive/bloar/pinning"

// headPolicy renders one head's configured pin policy (spec 9, 12). The config
// has already validated the mode and that a window has a duration.
func headPolicy(cfg *Config, hc HeadConfig) (pinning.Policy, error) {
	mode, err := pinning.ParseMode(hc.Pin.Mode)
	if err != nil {
		return pinning.Policy{}, err
	}
	switch mode {
	case pinning.ModeWindow:
		// seconds_per_slot is the network's, not the policy's: spec 9 converts
		// the duration to slots with the same constant the archive's own slot
		// arithmetic uses.
		return pinning.Window(hc.Pin.Duration, cfg.Beacon.SecondsPerSlot), nil
	case pinning.ModeNone:
		return pinning.None(), nil
	default:
		return pinning.Full(), nil
	}
}

// The gcExclusion middleware that used to live here is gone. It held
// pinning's gate around every POST, which made the exclusion of spec 9 a
// property of how a mutation arrived rather than of what it did: a stack that
// did not mount this middleware -- the conformance suite's, a follower's
// embedded registry, anything importing server directly -- mutated ungated
// while GC ran.
//
// The gate is now held by server.Heads around ApplyRefs and Truncate, and by
// ingest.Ingester around PutBlobs. The online collector takes it exclusively
// only for its T0 reconcile/pin-snapshot/epoch cut; mark and sweep then run with
// mutations protected by the application blockstore's T set. The library-level
// hold keeps each mutation wholly before or after that cut for every caller. A
// legacy collector without epoch support still uses whole-run exclusion.
