// Package kubo implements the narrow subset of Kubo's authenticated HTTP RPC
// API that Bloar needs to identify a daemon and operate on individual blocks.
package kubo

import (
	"errors"
	"fmt"
)

// ErrNotFound identifies a block that Kubo could not find. Kubo historically
// reports missing blocks as either HTTP 404 or HTTP 500 with a command error,
// so callers should use errors.Is rather than depending on an HTTP status.
var ErrNotFound = errors.New("kubo: block not found")

// StatusError is a non-200 Kubo RPC response. Endpoint is a fixed command name,
// never a full URL, and Message is bounded and bearer-token-redacted.
type StatusError struct {
	Endpoint      string
	Status        int
	Code          int
	Type          string
	Message       string
	Truncated     bool
	blockNotFound bool
	notPinned     bool
	routingAbsent bool
}

func (e *StatusError) Error() string {
	if e == nil {
		return "<nil>"
	}
	if e.Message == "" {
		return fmt.Sprintf("kubo: %s returned HTTP %d", e.Endpoint, e.Status)
	}
	return fmt.Sprintf("kubo: %s returned HTTP %d: %s", e.Endpoint, e.Status, e.Message)
}

// NotFoundError identifies the block and preserves the bounded status response
// that established absence, when Kubo supplied one.
type NotFoundError struct {
	Endpoint string
	CID      string
	Status   *StatusError
}

func (e *NotFoundError) Error() string {
	if e == nil {
		return "<nil>"
	}
	if e.Status != nil && e.Status.Message != "" {
		return fmt.Sprintf("kubo: %s: block %s not found: %s", e.Endpoint, e.CID, e.Status.Message)
	}
	return fmt.Sprintf("kubo: %s: block %s not found", e.Endpoint, e.CID)
}

// Unwrap exposes both the stable sentinel and the underlying typed HTTP error.
func (e *NotFoundError) Unwrap() []error {
	if e == nil || e.Status == nil {
		return []error{ErrNotFound}
	}
	return []error{ErrNotFound, e.Status}
}

// ProtocolError means Kubo returned HTTP 200 but the response could not be a
// valid answer for the requested operation (for example malformed JSON, an
// oversized body, a negative size, or bytes that do not match the requested
// CID). Problem is bounded and bearer-token-redacted.
type ProtocolError struct {
	Endpoint string
	Problem  string
}

// CompatibilityError means the daemon answered /version but is outside the
// explicitly tested Kubo line. The raw Version RPC remains available for
// diagnostics; managed-backend construction must call CheckCompatibility once
// before issuing mutation or network RPCs.
type CompatibilityError struct {
	Version   string
	Supported string
}

func (e *CompatibilityError) Error() string {
	if e == nil {
		return "<nil>"
	}
	return fmt.Sprintf("kubo: unsupported daemon version %q; supported version is %s", e.Version, e.Supported)
}

// CapabilityError means a version-compatible daemon does not expose one of
// the exact commands or flags that this client relies on. This catches API
// filtering, downstream forks, and partial reverse-proxy surfaces before a
// managed backend mutates state.
type CapabilityError struct {
	Missing string
}

func (e *CapabilityError) Error() string {
	if e == nil {
		return "<nil>"
	}
	return fmt.Sprintf("kubo: daemon is missing required RPC capability %q", e.Missing)
}

// StreamError is an application error delivered after a streaming RPC has
// already returned HTTP 200, either in an item or the X-Stream-Error trailer.
// Message is bounded and bearer-token-redacted. Item is one-based for an error
// object and zero for an HTTP trailer.
type StreamError struct {
	Endpoint  string
	Item      int
	Message   string
	Truncated bool
}

func (e *StreamError) Error() string {
	if e == nil {
		return "<nil>"
	}
	if e.Item > 0 {
		return fmt.Sprintf("kubo: %s stream item %d failed: %s", e.Endpoint, e.Item, e.Message)
	}
	return fmt.Sprintf("kubo: %s stream failed: %s", e.Endpoint, e.Message)
}

func (e *ProtocolError) Error() string {
	if e == nil {
		return "<nil>"
	}
	return fmt.Sprintf("kubo: %s protocol error: %s", e.Endpoint, e.Problem)
}

// TransportError is a failure before a complete RPC response was available.
// It deliberately does not unwrap the transport's error: a custom transport is
// allowed to inspect request headers, and exposing its original error could
// undo the client's bearer-token redaction. Context cancellation and deadlines
// are returned directly instead of being wrapped in this type.
type TransportError struct {
	Endpoint string
	Problem  string
}

func (e *TransportError) Error() string {
	if e == nil {
		return "<nil>"
	}
	return fmt.Sprintf("kubo: %s transport error: %s", e.Endpoint, e.Problem)
}
