package kubo

import (
	"context"
	"encoding/json"

	"github.com/ipfs/go-cid"
)

const maxStreamMessageBytes = 8 << 10

const maxRepoMetadataTextBytes = 8 << 10

// RepoStatInfo is the fixed numeric form of Kubo's repository statistics.
// Human-readable output is intentionally unavailable because it is ambiguous.
type RepoStatInfo struct {
	NumObjects uint64
	RepoSize   uint64
	StorageMax uint64
	RepoPath   string
	Version    string
}

// RepoGCResult reports the unique block CIDs removed by one fully drained GC
// stream. A non-nil error means the result is partial and must not be treated as
// proof of a complete pass.
type RepoGCResult struct {
	Removed []cid.Cid
}

// RepoVerifyResult reports a fully drained, non-destructive verification run.
// Messages include bounded Kubo corruption and completion diagnostics.
type RepoVerifyResult struct {
	BlocksProcessed int
	Messages        []string
}

// RepoStat returns bounded repository metadata without humanized numbers.
func (c *Client) RepoStat(ctx context.Context) (RepoStatInfo, error) {
	const endpoint = "repo/stat"
	query := jsonQuery()
	query.Set("size-only", "false")
	query.Set("human", "false")
	raw, err := c.post(ctx, endpoint, query, nil, "", "application/json", maxMetadataBytes)
	if err != nil {
		return RepoStatInfo{}, err
	}
	var wire struct {
		NumObjects *uint64
		RepoSize   *uint64
		StorageMax *uint64
		RepoPath   *string
		Version    *string
	}
	if err := decodeStrictJSON(raw, &wire); err != nil {
		return RepoStatInfo{}, c.protocol(endpoint, "decoding JSON: %v", err)
	}
	if wire.NumObjects == nil || wire.RepoSize == nil || wire.StorageMax == nil || wire.RepoPath == nil || wire.Version == nil {
		return RepoStatInfo{}, c.protocol(endpoint, "response is missing a required field")
	}
	if *wire.RepoPath == "" || *wire.Version == "" {
		return RepoStatInfo{}, c.protocol(endpoint, "response has an empty RepoPath or Version")
	}
	if len(*wire.RepoPath) > maxRepoMetadataTextBytes || len(*wire.Version) > maxRepoMetadataTextBytes {
		return RepoStatInfo{}, c.protocol(endpoint, "response RepoPath or Version exceeds the %d-byte limit", maxRepoMetadataTextBytes)
	}
	return RepoStatInfo{
		NumObjects: *wire.NumObjects,
		RepoSize:   *wire.RepoSize,
		StorageMax: *wire.StorageMax,
		RepoPath:   *wire.RepoPath,
		Version:    *wire.Version,
	}, nil
}

// RepoGC runs Kubo's error-streaming collector and consumes every object and
// the X-Stream-Error trailer before reporting success.
func (c *Client) RepoGC(ctx context.Context) (RepoGCResult, error) {
	const endpoint = "repo/gc"
	query := jsonQuery()
	query.Set("stream-errors", "true")
	query.Set("quiet", "false")
	query.Set("silent", "false")

	result := RepoGCResult{Removed: make([]cid.Cid, 0)}
	seen := make(map[string]struct{})
	err := c.postJSONStream(ctx, endpoint, query, c.maxStreamBytes, c.maxStreamItems, func(item int, raw json.RawMessage) error {
		var wire struct {
			Key *struct {
				CID string `json:"/"`
			}
			Error string
		}
		if err := decodeStrictJSON(raw, &wire); err != nil {
			return c.protocol(endpoint, "decoding stream item %d: %v", item, err)
		}
		hasKey := wire.Key != nil && wire.Key.CID != ""
		hasError := wire.Error != ""
		if hasKey == hasError {
			return c.protocol(endpoint, "stream item %d must contain exactly one non-empty Key or Error", item)
		}
		if hasError {
			return c.streamError(endpoint, item, wire.Error)
		}
		if len(wire.Key.CID) > maxCIDTextBytes {
			return c.protocol(endpoint, "stream item %d Key CID exceeds the %d-byte limit", item, maxCIDTextBytes)
		}
		removed, err := cid.Parse(wire.Key.CID)
		if err != nil || !removed.Defined() {
			return c.protocol(endpoint, "stream item %d has an invalid Key CID", item)
		}
		key := removed.KeyString()
		if _, duplicate := seen[key]; duplicate {
			return c.protocol(endpoint, "stream item %d repeats removed CID %s", item, removed)
		}
		seen[key] = struct{}{}
		result.Removed = append(result.Removed, removed)
		return nil
	})
	return result, err
}

// RepoVerify runs the safe read-only verifier. Destructive drop and heal flags
// are pinned false and cannot be supplied by callers.
func (c *Client) RepoVerify(ctx context.Context) (RepoVerifyResult, error) {
	const endpoint = "repo/verify"
	query := jsonQuery()
	query.Set("drop", "false")
	query.Set("heal", "false")
	query.Set("heal-timeout", "30s")

	result := RepoVerifyResult{Messages: make([]string, 0)}
	seenMessages := make(map[string]struct{})
	completionSeen := false
	nonCompletionSeen := false
	err := c.postJSONStream(ctx, endpoint, query, c.maxStreamBytes, c.maxStreamItems, func(item int, raw json.RawMessage) error {
		var wire struct {
			Msg      *string
			Progress *int
		}
		if err := decodeStrictJSON(raw, &wire); err != nil {
			return c.protocol(endpoint, "decoding stream item %d: %v", item, err)
		}
		if wire.Msg == nil || wire.Progress == nil {
			return c.protocol(endpoint, "stream item %d is missing Msg or Progress", item)
		}
		if completionSeen {
			return c.protocol(endpoint, "stream item %d appears after the completion message", item)
		}
		rawMessage := *wire.Msg
		progress := *wire.Progress
		if progress < 0 || rawMessage != "" && progress != 0 || rawMessage == "" && progress == 0 {
			return c.protocol(endpoint, "stream item %d has an invalid Msg/Progress combination", item)
		}
		if rawMessage != "" {
			if len(rawMessage) > maxStreamMessageBytes {
				return c.protocol(endpoint, "stream item %d message exceeds %d bytes", item, maxStreamMessageBytes)
			}
			if _, duplicate := seenMessages[rawMessage]; duplicate {
				return c.protocol(endpoint, "stream item %d repeats a message", item)
			}
			seenMessages[rawMessage] = struct{}{}
			if rawMessage == "verify complete, all blocks validated." {
				if nonCompletionSeen {
					return c.protocol(endpoint, "stream item %d claims success after corruption diagnostics", item)
				}
				completionSeen = true
			} else {
				// In Kubo 0.42's read-only mode, every other Msg describes a
				// corrupt block. A genuine run then fails in the stream trailer;
				// remember it so a forged success item cannot mask corruption.
				nonCompletionSeen = true
			}
			result.Messages = append(result.Messages, c.redact(rawMessage))
			return nil
		}
		if progress != result.BlocksProcessed+1 {
			return c.protocol(endpoint, "stream item %d progress is %d, want %d", item, progress, result.BlocksProcessed+1)
		}
		result.BlocksProcessed = progress
		return nil
	})
	if err != nil {
		return result, err
	}
	if !completionSeen {
		return result, c.protocol(endpoint, "stream ended without one final completion message")
	}
	return result, nil
}
