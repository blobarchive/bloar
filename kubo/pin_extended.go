package kubo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"unicode/utf8"

	"github.com/ipfs/go-cid"
)

// PinProgress is Kubo's latest recursive pin traversal snapshot. Nodes and
// Bytes are monotonic counters, not deltas.
type PinProgress struct {
	Nodes uint64
	Bytes uint64
}

// PinProgressFunc observes one validated recursive pin progress snapshot. A
// returned error stops callbacks but the response is still drained through EOF
// so framing and late stream errors are consumed before the call returns.
type PinProgressFunc func(PinProgress) error

// PinAddNamedRecursive creates or renames exactly one recursive pin. It keeps
// the ordinary Config.RequestTimeout behavior; use
// PinAddNamedRecursiveProgress for archive-scale DAGs.
func (c *Client) PinAddNamedRecursive(ctx context.Context, target cid.Cid, name string) error {
	const endpoint = "pin/add"
	targetText, err := boundedCIDArgument(endpoint, target)
	if err != nil {
		return err
	}
	if err := validateNamedPinArgument(endpoint, name); err != nil {
		return err
	}
	query := pinAddQuery(targetText, name, false)
	raw, err := c.post(ctx, endpoint, query, nil, "", "application/json", maxMetadataBytes)
	if err != nil {
		return err
	}
	return c.validatePinMutation(endpoint, target, raw)
}

// PinAddNamedRecursiveProgress performs a potentially long recursive pin and
// reports Kubo's bounded progress stream. Unlike ordinary RPCs it does not add
// Config.RequestTimeout: ctx must carry the caller's explicit deadline. The
// returned snapshot is the latest validated progress even when the operation
// fails. All fast-provide modes are forced off.
func (c *Client) PinAddNamedRecursiveProgress(
	ctx context.Context,
	target cid.Cid,
	name string,
	limits ListLimits,
	onProgress PinProgressFunc,
) (PinProgress, error) {
	const endpoint = "pin/add"
	targetText, err := boundedCIDArgument(endpoint, target)
	if err != nil {
		return PinProgress{}, err
	}
	if err := validateNamedPinArgument(endpoint, name); err != nil {
		return PinProgress{}, err
	}
	if err := requireCallerDeadline(ctx, endpoint); err != nil {
		return PinProgress{}, err
	}
	if err := c.validateListLimits(endpoint, limits); err != nil {
		return PinProgress{}, err
	}

	query := pinAddQuery(targetText, name, true)
	var snapshot PinProgress
	completed := false
	validationFailed := false
	var callbackErr error
	err = c.postJSONStreamContext(ctx, endpoint, query, limits.MaxBytes, limits.MaxItems, func(item int, raw json.RawMessage) error {
		if validationFailed {
			return nil
		}
		if !utf8.Valid(raw) {
			validationFailed = true
			return c.protocol(endpoint, "stream item %d is not valid UTF-8", item)
		}
		var wire struct {
			Pins     json.RawMessage `json:"Pins"`
			Progress *int            `json:"Progress"`
			Bytes    *uint64         `json:"Bytes"`
		}
		if err := decodeStrictJSON(raw, &wire); err != nil {
			validationFailed = true
			return c.protocolDetail(endpoint, fmt.Sprintf("decoding stream item %d", item), err)
		}
		if completed {
			validationFailed = true
			return c.protocol(endpoint, "stream item %d follows the final Pins acknowledgement", item)
		}
		if wire.Pins != nil {
			if wire.Progress != nil || wire.Bytes != nil {
				validationFailed = true
				return c.protocol(endpoint, "final stream item %d mixes Pins with progress fields", item)
			}
			if err := c.validatePinMutation(endpoint, target, raw); err != nil {
				validationFailed = true
				return err
			}
			completed = true
			return nil
		}

		next := snapshot
		if wire.Progress != nil {
			if *wire.Progress < 0 {
				validationFailed = true
				return c.protocol(endpoint, "stream item %d has negative Progress", item)
			}
			next.Nodes = uint64(*wire.Progress)
		}
		if wire.Bytes != nil {
			next.Bytes = *wire.Bytes
		}
		if next.Nodes < snapshot.Nodes || next.Bytes < snapshot.Bytes {
			validationFailed = true
			return c.protocol(endpoint, "stream item %d regresses progress from %d nodes/%d bytes to %d nodes/%d bytes", item, snapshot.Nodes, snapshot.Bytes, next.Nodes, next.Bytes)
		}
		snapshot = next
		if onProgress != nil && callbackErr == nil {
			if err := onProgress(snapshot); err != nil {
				callbackErr = fmt.Errorf("kubo: %s progress callback: %w", endpoint, err)
			}
		}
		return nil
	})
	if err != nil {
		if callbackErr != nil {
			return snapshot, errors.Join(callbackErr, err)
		}
		return snapshot, err
	}
	if !completed {
		err = c.protocol(endpoint, "progress stream ended without a final Pins acknowledgement")
		if callbackErr != nil {
			return snapshot, errors.Join(callbackErr, err)
		}
		return snapshot, err
	}
	if callbackErr != nil {
		return snapshot, callbackErr
	}
	return snapshot, nil
}

// PinUpdateAddBeforeRemove efficiently creates a recursive successor pin while
// retaining the old recursive pin. Kubo preserves the old pin's name on the
// successor. The caller removes the old pin only after separately establishing
// that the successor is durable. This potentially long call requires an
// explicit caller deadline and forces all fast-provide modes off.
func (c *Client) PinUpdateAddBeforeRemove(ctx context.Context, old, next cid.Cid) error {
	const endpoint = "pin/update"
	oldText, err := boundedCIDArgument(endpoint, old)
	if err != nil {
		return err
	}
	nextText, err := boundedCIDArgument(endpoint, next)
	if err != nil {
		return err
	}
	if old.Equals(next) {
		return errors.New("kubo: pin/update requires different old and new CIDs")
	}
	if err := requireCallerDeadline(ctx, endpoint); err != nil {
		return err
	}
	query := jsonQuery()
	query.Add("arg", oldText)
	query.Add("arg", nextText)
	query.Set("unpin", "false")
	forceFastProvideOff(query)
	raw, err := c.postContext(ctx, endpoint, query, nil, "", "application/json", maxMetadataBytes)
	if err != nil {
		return c.asNotPinned(old, err)
	}
	return c.validatePinUpdate(endpoint, old, next, raw)
}

// PinStatus returns the exact status of one direct or recursive CID without a
// repository-wide listing. Pin names are requested and validated.
func (c *Client) PinStatus(ctx context.Context, target cid.Cid, pinType PinType) (PinInfo, error) {
	const endpoint = "pin/ls"
	targetText, err := boundedCIDArgument(endpoint, target)
	if err != nil {
		return PinInfo{}, err
	}
	if pinType != PinTypeDirect && pinType != PinTypeRecursive {
		return PinInfo{}, errors.New("kubo: pin/ls exact status accepts only direct or recursive pin types")
	}
	query := jsonQuery()
	query.Set("arg", targetText)
	query.Set("type", string(pinType))
	query.Set("quiet", "false")
	query.Set("stream", "true")
	query.Set("names", "true")
	var result PinInfo
	items := 0
	maxBytes := min(maxMetadataBytes, c.maxStreamBytes)
	err = c.postJSONStream(ctx, endpoint, query, maxBytes, 1, func(item int, raw json.RawMessage) error {
		items++
		pin, err := c.decodeNamedPinItem(endpoint, item, raw, pinType, false)
		if err != nil {
			return err
		}
		if !pin.CID.Equals(target) {
			return c.protocol(endpoint, "stream item %d names CID %s, want %s", item, pin.CID, target)
		}
		result = pin
		return nil
	})
	if err != nil {
		return PinInfo{}, c.asNotPinned(target, err)
	}
	if items != 1 {
		return PinInfo{}, c.protocol(endpoint, "response contains %d pin items, want exactly 1", items)
	}
	return result, nil
}

// PinListExactName returns recursively pinned roots whose complete name equals
// name. Kubo's server-side name filter is a case-sensitive substring match, so
// every bounded result is validated and exact matching is enforced locally.
func (c *Client) PinListExactName(ctx context.Context, name string, limits ListLimits) ([]PinInfo, error) {
	const endpoint = "pin/ls"
	if err := validateNamedPinArgument(endpoint, name); err != nil {
		return nil, err
	}
	if err := c.validateListLimits(endpoint, limits); err != nil {
		return nil, err
	}
	query := jsonQuery()
	query.Set("type", string(PinTypeRecursive))
	query.Set("quiet", "false")
	query.Set("stream", "true")
	query.Set("names", "true")
	query.Set("name", name)
	result := make([]PinInfo, 0)
	seen := make(map[string]struct{})
	failed := false
	err := c.postJSONStream(ctx, endpoint, query, limits.MaxBytes, limits.MaxItems, func(item int, raw json.RawMessage) error {
		if failed {
			return nil
		}
		pin, err := c.decodeNamedPinItem(endpoint, item, raw, PinTypeRecursive, true)
		if err != nil {
			failed = true
			return err
		}
		if !strings.Contains(pin.Name, name) {
			failed = true
			return c.protocol(endpoint, "stream item %d pin name %q does not contain the requested filter", item, pin.Name)
		}
		key := string(pin.CID.Bytes())
		if _, duplicate := seen[key]; duplicate {
			failed = true
			return c.protocol(endpoint, "stream item %d duplicates CID %s", item, pin.CID)
		}
		seen[key] = struct{}{}
		if pin.Name == name {
			result = append(result, pin)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func pinAddQuery(targetText, name string, progress bool) url.Values {
	query := jsonQuery()
	query.Set("arg", targetText)
	query.Set("recursive", "true")
	query.Set("name", name)
	if progress {
		query.Set("progress", "true")
	} else {
		query.Set("progress", "false")
	}
	forceFastProvideOff(query)
	return query
}

func forceFastProvideOff(query url.Values) {
	query.Set("fast-provide-root", "false")
	query.Set("fast-provide-dag", "false")
	query.Set("fast-provide-wait", "false")
}

func validateNamedPinArgument(endpoint, name string) error {
	if name == "" {
		return fmt.Errorf("kubo: %s requires a non-empty pin name", endpoint)
	}
	if len(name) > maxPinNameBytes {
		return fmt.Errorf("kubo: %s pin name exceeds the %d-byte limit", endpoint, maxPinNameBytes)
	}
	if !utf8.ValidString(name) {
		return fmt.Errorf("kubo: %s pin name is not valid UTF-8", endpoint)
	}
	return nil
}

func requireCallerDeadline(ctx context.Context, endpoint string) error {
	if err := ctx.Err(); err != nil {
		return context.Cause(ctx)
	}
	if _, ok := ctx.Deadline(); !ok {
		return fmt.Errorf("kubo: %s long-running mutation requires a caller-supplied context deadline", endpoint)
	}
	return nil
}

func (c *Client) decodeNamedPinItem(endpoint string, item int, raw json.RawMessage, expectedType PinType, requireName bool) (PinInfo, error) {
	if !utf8.Valid(raw) {
		return PinInfo{}, c.protocol(endpoint, "stream item %d is not valid UTF-8", item)
	}
	var wire struct {
		CID  string `json:"Cid"`
		Name string `json:"Name"`
		Type string `json:"Type"`
	}
	if err := decodeStrictJSON(raw, &wire); err != nil {
		return PinInfo{}, c.protocolDetail(endpoint, fmt.Sprintf("decoding stream item %d", item), err)
	}
	if wire.Type != string(expectedType) {
		if len(wire.Type) > maxPinTypeBytes {
			return PinInfo{}, c.protocol(endpoint, "stream item %d pin type exceeds the %d-byte limit", item, maxPinTypeBytes)
		}
		return PinInfo{}, c.protocol(endpoint, "stream item %d has pin type %q, want %q", item, wire.Type, expectedType)
	}
	if wire.CID == "" {
		return PinInfo{}, c.protocol(endpoint, "stream item %d has an empty Cid", item)
	}
	if len(wire.CID) > maxCIDTextBytes {
		return PinInfo{}, c.protocol(endpoint, "stream item %d Cid exceeds the %d-byte limit", item, maxCIDTextBytes)
	}
	pinned, err := cid.Parse(wire.CID)
	if err != nil {
		return PinInfo{}, c.protocolDetail(endpoint, fmt.Sprintf("stream item %d has invalid Cid", item), err)
	}
	if len(wire.Name) > maxPinNameBytes {
		return PinInfo{}, c.protocol(endpoint, "stream item %d pin name exceeds the %d-byte limit", item, maxPinNameBytes)
	}
	if requireName && wire.Name == "" {
		return PinInfo{}, c.protocol(endpoint, "stream item %d has an empty pin name", item)
	}
	return PinInfo{CID: pinned, Type: expectedType, Name: wire.Name}, nil
}

func (c *Client) validatePinUpdate(endpoint string, old, next cid.Cid, raw []byte) error {
	if !utf8.Valid(raw) {
		return c.protocol(endpoint, "response is not valid UTF-8")
	}
	var wire struct {
		Pins []string `json:"Pins"`
	}
	if err := decodeStrictJSON(raw, &wire); err != nil {
		return c.protocolDetail(endpoint, "decoding JSON", err)
	}
	if len(wire.Pins) != 2 {
		return c.protocol(endpoint, "response contains %d Pins, want exactly 2", len(wire.Pins))
	}
	for i, expected := range []cid.Cid{old, next} {
		if wire.Pins[i] == "" || len(wire.Pins[i]) > maxCIDTextBytes {
			return c.protocol(endpoint, "Pins item %d is empty or exceeds the %d-byte limit", i, maxCIDTextBytes)
		}
		got, err := cid.Parse(wire.Pins[i])
		if err != nil {
			return c.protocolDetail(endpoint, fmt.Sprintf("invalid Pins item %d CID", i), err)
		}
		if !got.Equals(expected) {
			return c.protocol(endpoint, "Pins item %d names CID %s, want %s", i, got, expected)
		}
	}
	return nil
}
