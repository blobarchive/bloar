package replica

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/ipfs/go-cid"
)

const (
	stateVersion  = 1
	maxStateBytes = 256 << 10
)

var stateKey = []byte{'r', 'k', 'u', 'b', 'o', '-', 'r', 'e', 'p', 'l', 'i', 'c', 'a', ':', 'v', '1'}

// Ownership records whether the controller may later remove a generation
// anchor. Borrowed pins predated the transition and are never removed.
type Ownership string

const (
	OwnershipOwned    Ownership = "owned"
	OwnershipBorrowed Ownership = "borrowed"
)

type retainedGeneration struct {
	Generation Generation
	Anchor     cid.Cid
	Ownership  Ownership
	At         time.Time
}

type controllerState struct {
	Current *retainedGeneration
	Pending *retainedGeneration
	Cleanup []retainedGeneration
}

type diskHead struct {
	Name     string `json:"name"`
	Root     string `json:"root"`
	Manifest string `json:"manifest,omitempty"`
	SyncedTo uint64 `json:"synced_to"`
}

type diskGeneration struct {
	ReplicaID string     `json:"replica_id"`
	UpdatedAt int64      `json:"updated_at"`
	Heads     []diskHead `json:"heads"`
}

type diskRetained struct {
	Generation diskGeneration `json:"generation"`
	Anchor     string         `json:"anchor"`
	Ownership  Ownership      `json:"ownership"`
	At         int64          `json:"at"`
}

type diskState struct {
	Version int            `json:"version"`
	Current *diskRetained  `json:"current,omitempty"`
	Pending *diskRetained  `json:"pending,omitempty"`
	Cleanup []diskRetained `json:"cleanup,omitempty"`
}

func loadControllerState(kv *pebble.DB) (controllerState, error) {
	value, closer, err := kv.Get(stateKey)
	if errors.Is(err, pebble.ErrNotFound) {
		return controllerState{}, nil
	}
	if err != nil {
		return controllerState{}, fmt.Errorf("replica: reading controller state: %w", err)
	}
	defer closer.Close()
	if len(value) > maxStateBytes {
		return controllerState{}, fmt.Errorf("replica: controller state is %d bytes, over the %d-byte limit", len(value), maxStateBytes)
	}

	decoder := json.NewDecoder(io.LimitReader(bytes.NewReader(value), maxStateBytes+1))
	decoder.DisallowUnknownFields()
	var disk diskState
	if err := decoder.Decode(&disk); err != nil {
		return controllerState{}, fmt.Errorf("replica: decoding controller state: %w", err)
	}
	if decoder.Decode(&struct{}{}) != io.EOF {
		return controllerState{}, errors.New("replica: controller state has trailing JSON")
	}
	if disk.Version != stateVersion {
		return controllerState{}, fmt.Errorf("replica: controller state is version %d, this build reads version %d", disk.Version, stateVersion)
	}

	state := controllerState{}
	if disk.Current != nil {
		current, err := retainedFromDisk(*disk.Current)
		if err != nil {
			return controllerState{}, fmt.Errorf("replica: decoding current generation: %w", err)
		}
		state.Current = &current
	}
	if disk.Pending != nil {
		pending, err := retainedFromDisk(*disk.Pending)
		if err != nil {
			return controllerState{}, fmt.Errorf("replica: decoding pending generation: %w", err)
		}
		state.Pending = &pending
	}
	if len(disk.Cleanup) > maxGenerationHeads+1 {
		return controllerState{}, fmt.Errorf("replica: controller state has %d cleanup anchors, over the %d-entry limit", len(disk.Cleanup), maxGenerationHeads+1)
	}
	for i, item := range disk.Cleanup {
		decoded, err := retainedFromDisk(item)
		if err != nil {
			return controllerState{}, fmt.Errorf("replica: decoding cleanup generation %d: %w", i, err)
		}
		state.Cleanup = append(state.Cleanup, decoded)
	}
	if err := validateControllerState(state); err != nil {
		return controllerState{}, err
	}
	return state, nil
}

func validateControllerState(state controllerState) error {
	seen := make(map[string]string)
	check := func(label string, retained *retainedGeneration) error {
		if retained == nil {
			return nil
		}
		key := retained.Anchor.KeyString()
		if prior, exists := seen[key]; exists {
			return fmt.Errorf("replica: controller state repeats anchor %s in %s and %s", retained.Anchor, prior, label)
		}
		seen[key] = label
		return nil
	}
	if err := check("current", state.Current); err != nil {
		return err
	}
	if err := check("pending", state.Pending); err != nil {
		return err
	}
	for i := range state.Cleanup {
		if state.Cleanup[i].Ownership != OwnershipOwned {
			return fmt.Errorf("replica: cleanup generation %d has non-owned ownership %q", i, state.Cleanup[i].Ownership)
		}
		if err := check(fmt.Sprintf("cleanup[%d]", i), &state.Cleanup[i]); err != nil {
			return err
		}
	}
	return nil
}

func saveControllerState(kv *pebble.DB, state controllerState) error {
	if err := validateControllerState(state); err != nil {
		return err
	}
	disk := diskState{Version: stateVersion}
	if state.Current != nil {
		value, err := retainedToDisk(*state.Current)
		if err != nil {
			return err
		}
		disk.Current = &value
	}
	if state.Pending != nil {
		value, err := retainedToDisk(*state.Pending)
		if err != nil {
			return err
		}
		disk.Pending = &value
	}
	if len(state.Cleanup) > maxGenerationHeads+1 {
		return fmt.Errorf("replica: refusing to persist %d cleanup anchors, maximum is %d", len(state.Cleanup), maxGenerationHeads+1)
	}
	for _, item := range state.Cleanup {
		value, err := retainedToDisk(item)
		if err != nil {
			return err
		}
		disk.Cleanup = append(disk.Cleanup, value)
	}
	encoded, err := json.Marshal(disk)
	if err != nil {
		return fmt.Errorf("replica: encoding controller state: %w", err)
	}
	if len(encoded) > maxStateBytes {
		return fmt.Errorf("replica: encoded controller state is %d bytes, over the %d-byte limit", len(encoded), maxStateBytes)
	}
	if err := kv.Set(stateKey, encoded, pebble.Sync); err != nil {
		return fmt.Errorf("replica: persisting controller state: %w", err)
	}
	return nil
}

func retainedToDisk(value retainedGeneration) (diskRetained, error) {
	generation, err := value.Generation.Normalize()
	if err != nil {
		return diskRetained{}, err
	}
	block, err := generation.Block()
	if err != nil {
		return diskRetained{}, err
	}
	if !value.Anchor.Defined() || !value.Anchor.Equals(block.Cid()) {
		return diskRetained{}, errors.New("replica: retained generation anchor does not match its canonical generation")
	}
	if value.Ownership != OwnershipOwned && value.Ownership != OwnershipBorrowed {
		return diskRetained{}, fmt.Errorf("replica: retained generation has invalid ownership %q", value.Ownership)
	}
	if value.At.IsZero() || value.At.Unix() < 0 {
		return diskRetained{}, errors.New("replica: retained generation timestamp must be on or after the Unix epoch")
	}

	disk := diskRetained{
		Generation: diskGeneration{
			ReplicaID: generation.ReplicaID,
			UpdatedAt: generation.UpdatedAt.Unix(),
			Heads:     make([]diskHead, 0, len(generation.Heads)),
		},
		Anchor:    value.Anchor.String(),
		Ownership: value.Ownership,
		At:        value.At.UTC().Truncate(time.Second).Unix(),
	}
	for _, head := range generation.Heads {
		item := diskHead{Name: head.Name, Root: head.Root.String(), SyncedTo: head.SyncedTo}
		if head.Manifest.Defined() {
			item.Manifest = head.Manifest.String()
		}
		disk.Generation.Heads = append(disk.Generation.Heads, item)
	}
	return disk, nil
}

func retainedFromDisk(value diskRetained) (retainedGeneration, error) {
	anchor, err := cid.Parse(value.Anchor)
	if err != nil {
		return retainedGeneration{}, fmt.Errorf("invalid anchor CID: %w", err)
	}
	generation := Generation{
		ReplicaID: value.Generation.ReplicaID,
		UpdatedAt: time.Unix(value.Generation.UpdatedAt, 0).UTC(),
	}
	for i, head := range value.Generation.Heads {
		root, err := cid.Parse(head.Root)
		if err != nil {
			return retainedGeneration{}, fmt.Errorf("head %d root: %w", i, err)
		}
		manifest := cid.Undef
		if head.Manifest != "" {
			manifest, err = cid.Parse(head.Manifest)
			if err != nil {
				return retainedGeneration{}, fmt.Errorf("head %d manifest: %w", i, err)
			}
		}
		generation.Heads = append(generation.Heads, Head{
			Name: head.Name, Root: root, Manifest: manifest, SyncedTo: head.SyncedTo,
		})
	}
	normalized, err := generation.Normalize()
	if err != nil {
		return retainedGeneration{}, err
	}
	block, err := normalized.Block()
	if err != nil {
		return retainedGeneration{}, err
	}
	if !anchor.Equals(block.Cid()) {
		return retainedGeneration{}, errors.New("anchor CID does not match canonical generation")
	}
	if value.Ownership != OwnershipOwned && value.Ownership != OwnershipBorrowed {
		return retainedGeneration{}, fmt.Errorf("invalid ownership %q", value.Ownership)
	}
	if value.At < 0 {
		return retainedGeneration{}, errors.New("negative retained timestamp")
	}
	return retainedGeneration{
		Generation: normalized,
		Anchor:     anchor,
		Ownership:  value.Ownership,
		At:         time.Unix(value.At, 0).UTC(),
	}, nil
}
