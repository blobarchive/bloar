package kubo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/ipfs/go-cid"
)

const (
	maxPinNameBytes        = 255
	maxPinTypeBytes        = 32
	maxProtocolDetailBytes = 512
)

// ListLimits is the caller-selected resource budget for one enumeration RPC.
// Both fields are required: enumeration never silently becomes unbounded.
type ListLimits struct {
	MaxItems int
	MaxBytes int64
}

// PinType limits PinList and selects direct or recursive behavior for pin
// mutations. PinTypeAll and PinTypeIndirect are valid only for PinList.
type PinType string

const (
	PinTypeAll       PinType = "all"
	PinTypeDirect    PinType = "direct"
	PinTypeIndirect  PinType = "indirect"
	PinTypeRecursive PinType = "recursive"
)

// PinInfo is one validated entry returned by PinList.
type PinInfo struct {
	CID  cid.Cid
	Type PinType
	Name string
}

// ErrNotPinned identifies a pin/rm request for a CID that is not removable as
// the requested pin type.
var ErrNotPinned = errors.New("kubo: CID is not pinned")

// NotPinnedError preserves Kubo's bounded status response while exposing the
// stable ErrNotPinned sentinel through errors.Is.
type NotPinnedError struct {
	CID    string
	Status *StatusError
}

func (e *NotPinnedError) Error() string {
	if e == nil {
		return "<nil>"
	}
	if e.Status != nil && e.Status.Message != "" {
		return fmt.Sprintf("kubo: pin/rm: CID %s is not pinned: %s", e.CID, e.Status.Message)
	}
	return fmt.Sprintf("kubo: pin/rm: CID %s is not pinned", e.CID)
}

// Unwrap exposes both the stable sentinel and the underlying HTTP status.
func (e *NotPinnedError) Unwrap() []error {
	if e == nil || e.Status == nil {
		return []error{ErrNotPinned}
	}
	return []error{ErrNotPinned, e.Status}
}

// RefsLocal returns every locally stored block CID. The result is returned
// only after the complete response stream and its error trailer have been
// consumed and validated.
func (c *Client) RefsLocal(ctx context.Context, limits ListLimits) ([]cid.Cid, error) {
	const endpoint = "refs/local"
	if err := c.validateListLimits(endpoint, limits); err != nil {
		return nil, err
	}

	query := jsonQuery()
	refs := make([]cid.Cid, 0)
	seen := make(map[string]struct{})
	failed := false
	err := c.postJSONStream(ctx, endpoint, query, limits.MaxBytes, limits.MaxItems, func(item int, raw json.RawMessage) error {
		if failed {
			return nil
		}
		var wire struct {
			Ref string
			Err string
		}
		if err := decodeStrictJSON(raw, &wire); err != nil {
			failed = true
			return c.protocolDetail(endpoint, fmt.Sprintf("decoding stream item %d", item), err)
		}
		if wire.Err != "" {
			failed = true
			return c.streamError(endpoint, item, wire.Err)
		}
		if wire.Ref == "" {
			failed = true
			return c.protocol(endpoint, "stream item %d has an empty Ref", item)
		}
		if len(wire.Ref) > maxCIDTextBytes {
			failed = true
			return c.protocol(endpoint, "stream item %d Ref exceeds the %d-byte limit", item, maxCIDTextBytes)
		}
		ref, err := cid.Parse(wire.Ref)
		if err != nil {
			failed = true
			return c.protocolDetail(endpoint, fmt.Sprintf("stream item %d has an invalid Ref CID", item), err)
		}
		key := string(ref.Bytes())
		if _, duplicate := seen[key]; duplicate {
			failed = true
			return c.protocol(endpoint, "stream item %d duplicates CID %s", item, ref)
		}
		seen[key] = struct{}{}
		refs = append(refs, ref)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return refs, nil
}

// PinList returns local pins of the requested type. Kubo's streaming form is
// always requested so the byte and item budgets can be enforced while data is
// decoded. Pin names are deliberately not requested by this narrow API.
func (c *Client) PinList(ctx context.Context, pinType PinType, limits ListLimits) ([]PinInfo, error) {
	const endpoint = "pin/ls"
	if !validPinListType(pinType) {
		if len(pinType) > maxPinTypeBytes {
			return nil, fmt.Errorf("kubo: pin/ls pin type exceeds the %d-byte limit", maxPinTypeBytes)
		}
		return nil, fmt.Errorf("kubo: pin/ls has invalid pin type %q", pinType)
	}
	if err := c.validateListLimits(endpoint, limits); err != nil {
		return nil, err
	}

	query := jsonQuery()
	query.Set("type", string(pinType))
	query.Set("quiet", "false")
	query.Set("stream", "true")
	query.Set("names", "false")
	pins := make([]PinInfo, 0)
	seen := make(map[string]struct{})
	failed := false
	err := c.postJSONStream(ctx, endpoint, query, limits.MaxBytes, limits.MaxItems, func(item int, raw json.RawMessage) error {
		if failed {
			return nil
		}
		var wire struct {
			CID  string `json:"Cid"`
			Name string `json:"Name"`
			Type string `json:"Type"`
		}
		if err := decodeStrictJSON(raw, &wire); err != nil {
			failed = true
			return c.protocolDetail(endpoint, fmt.Sprintf("decoding stream item %d", item), err)
		}
		if wire.Name != "" {
			failed = true
			if len(wire.Name) > maxPinNameBytes {
				return c.protocol(endpoint, "stream item %d has an unsolicited pin name over %d bytes", item, maxPinNameBytes)
			}
			return c.protocol(endpoint, "stream item %d includes a pin name although names=false", item)
		}
		gotType := PinType(wire.Type)
		if gotType != PinTypeDirect && gotType != PinTypeIndirect && gotType != PinTypeRecursive {
			failed = true
			if len(wire.Type) > maxPinTypeBytes {
				return c.protocol(endpoint, "stream item %d pin type exceeds the %d-byte limit", item, maxPinTypeBytes)
			}
			return c.protocol(endpoint, "stream item %d has invalid pin type %q", item, wire.Type)
		}
		if pinType != PinTypeAll && gotType != pinType {
			failed = true
			return c.protocol(endpoint, "stream item %d has pin type %q, not requested type %q", item, gotType, pinType)
		}
		if wire.CID == "" {
			failed = true
			return c.protocol(endpoint, "stream item %d has an empty Cid", item)
		}
		if len(wire.CID) > maxCIDTextBytes {
			failed = true
			return c.protocol(endpoint, "stream item %d Cid exceeds the %d-byte limit", item, maxCIDTextBytes)
		}
		pinned, err := cid.Parse(wire.CID)
		if err != nil {
			failed = true
			return c.protocolDetail(endpoint, fmt.Sprintf("stream item %d has invalid Cid", item), err)
		}
		key := string(pinned.Bytes())
		if _, duplicate := seen[key]; duplicate {
			failed = true
			return c.protocol(endpoint, "stream item %d duplicates CID %s", item, pinned)
		}
		seen[key] = struct{}{}
		pins = append(pins, PinInfo{CID: pinned, Type: gotType})
		return nil
	})
	if err != nil {
		return nil, err
	}
	return pins, nil
}

// PinAdd creates exactly one direct or recursive local pin and verifies that
// Kubo acknowledges the requested CID. Background providing and progress
// streams are explicitly disabled.
func (c *Client) PinAdd(ctx context.Context, target cid.Cid, pinType PinType) error {
	const endpoint = "pin/add"
	targetText, err := boundedCIDArgument(endpoint, target)
	if err != nil {
		return err
	}
	recursive, err := mutationRecursive(endpoint, pinType)
	if err != nil {
		return err
	}
	query := jsonQuery()
	query.Set("arg", targetText)
	query.Set("recursive", recursive)
	query.Set("progress", "false")
	query.Set("fast-provide-root", "false")
	query.Set("fast-provide-dag", "false")
	query.Set("fast-provide-wait", "false")
	raw, err := c.post(ctx, endpoint, query, nil, "", "application/json", maxMetadataBytes)
	if err != nil {
		return err
	}
	return c.validatePinMutation(endpoint, target, raw)
}

// PinRemove removes exactly one direct or recursive local pin and verifies
// that Kubo acknowledges the requested CID.
func (c *Client) PinRemove(ctx context.Context, target cid.Cid, pinType PinType) error {
	const endpoint = "pin/rm"
	targetText, err := boundedCIDArgument(endpoint, target)
	if err != nil {
		return err
	}
	recursive, err := mutationRecursive(endpoint, pinType)
	if err != nil {
		return err
	}
	query := jsonQuery()
	query.Set("arg", targetText)
	query.Set("recursive", recursive)
	raw, err := c.post(ctx, endpoint, query, nil, "", "application/json", maxMetadataBytes)
	if err != nil {
		return c.asNotPinned(target, err)
	}
	return c.validatePinMutation(endpoint, target, raw)
}

func (c *Client) validateListLimits(endpoint string, limits ListLimits) error {
	if limits.MaxItems <= 0 || limits.MaxItems > c.maxStreamItems {
		return fmt.Errorf("kubo: %s requires MaxItems between 1 and %d", endpoint, c.maxStreamItems)
	}
	if limits.MaxBytes <= 0 || limits.MaxBytes > c.maxStreamBytes {
		return fmt.Errorf("kubo: %s requires MaxBytes between 1 and %d", endpoint, c.maxStreamBytes)
	}
	return nil
}

func validPinListType(pinType PinType) bool {
	switch pinType {
	case PinTypeAll, PinTypeDirect, PinTypeIndirect, PinTypeRecursive:
		return true
	default:
		return false
	}
}

func mutationRecursive(endpoint string, pinType PinType) (string, error) {
	switch pinType {
	case PinTypeDirect:
		return "false", nil
	case PinTypeRecursive:
		return "true", nil
	default:
		return "", fmt.Errorf("kubo: %s accepts only direct or recursive pin types", endpoint)
	}
}

func (c *Client) validatePinMutation(endpoint string, expected cid.Cid, raw []byte) error {
	if !utf8.Valid(raw) {
		return c.protocol(endpoint, "response is not valid UTF-8")
	}
	var wire struct {
		Pins []string `json:"Pins"`
	}
	if err := decodeStrictJSON(raw, &wire); err != nil {
		return c.protocolDetail(endpoint, "decoding JSON", err)
	}
	if len(wire.Pins) != 1 {
		return c.protocol(endpoint, "response contains %d Pins, want exactly 1", len(wire.Pins))
	}
	if wire.Pins[0] == "" {
		return c.protocol(endpoint, "response contains an empty Pins CID")
	}
	if len(wire.Pins[0]) > maxCIDTextBytes {
		return c.protocol(endpoint, "Pins CID exceeds the %d-byte limit", maxCIDTextBytes)
	}
	got, err := cid.Parse(wire.Pins[0])
	if err != nil {
		return c.protocolDetail(endpoint, "invalid Pins CID", err)
	}
	if !got.Equals(expected) {
		return c.protocol(endpoint, "response names CID %s, want %s", got, expected)
	}
	return nil
}

func (c *Client) asNotPinned(expected cid.Cid, err error) error {
	var status *StatusError
	if errors.As(err, &status) && !status.Truncated &&
		(status.notPinned || messageIsNotPinned(status.Message)) {
		return &NotPinnedError{CID: expected.String(), Status: status}
	}
	return err
}

func messageIsNotPinned(message string) bool {
	message = strings.ToLower(message)
	return strings.Contains(message, "not pinned") || strings.Contains(message, "not recursively pinned") || strings.Contains(message, "pinned indirectly") ||
		strings.Contains(message, "is pinned recursively")
}

func (c *Client) protocolDetail(endpoint, prefix string, err error) error {
	detail := c.redact(err.Error())
	detail = boundedText(detail, maxProtocolDetailBytes)
	return c.protocol(endpoint, "%s: %s", prefix, detail)
}
