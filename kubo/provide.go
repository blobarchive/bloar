package kubo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"unicode/utf8"

	"github.com/ipfs/go-cid"
)

// MaximumProvideOnceCIDs is the absolute request and acknowledgement ceiling
// for one provide/once batch. Callers may choose a smaller ListLimits.MaxItems.
const MaximumProvideOnceCIDs = 256

// ProvideOnce queues one non-recursive provider announcement for every target.
// It does not change Kubo's periodic reprovide schedule. The potentially slow
// streamed RPC uses the caller's explicit context deadline rather than
// Config.RequestTimeout, and succeeds only after an exact, duplicate-free
// acknowledgement set has been read through EOF and late trailers.
func (c *Client) ProvideOnce(ctx context.Context, targets []cid.Cid, limits ListLimits) error {
	const endpoint = "provide/once"
	if len(targets) == 0 {
		return errors.New("kubo: provide/once requires at least one CID")
	}
	if len(targets) > MaximumProvideOnceCIDs {
		return fmt.Errorf("kubo: provide/once accepts at most %d CIDs", MaximumProvideOnceCIDs)
	}
	if err := requireCallerDeadline(ctx, endpoint); err != nil {
		return err
	}
	if err := c.validateListLimits(endpoint, limits); err != nil {
		return err
	}
	if limits.MaxItems > MaximumProvideOnceCIDs {
		return fmt.Errorf("kubo: provide/once MaxItems must not exceed %d", MaximumProvideOnceCIDs)
	}
	if len(targets) > limits.MaxItems {
		return fmt.Errorf("kubo: provide/once has %d CIDs, over the %d-item response limit", len(targets), limits.MaxItems)
	}

	query := jsonQuery()
	query.Set("recursive", "false")
	expected := make(map[string]cid.Cid, len(targets))
	for _, target := range targets {
		text, err := boundedCIDArgument(endpoint, target)
		if err != nil {
			return err
		}
		key := string(target.Bytes())
		if _, duplicate := expected[key]; duplicate {
			return fmt.Errorf("kubo: provide/once request duplicates CID %s", target)
		}
		expected[key] = target
		query.Add("arg", text)
	}

	acknowledged := make(map[string]struct{}, len(targets))
	validationFailed := false
	err := c.postJSONStreamContext(ctx, endpoint, query, limits.MaxBytes, limits.MaxItems, func(item int, raw json.RawMessage) error {
		if validationFailed {
			return nil
		}
		if !utf8.Valid(raw) {
			validationFailed = true
			return c.protocol(endpoint, "stream item %d is not valid UTF-8", item)
		}
		var wire struct {
			Queued string
		}
		if err := decodeStrictJSON(raw, &wire); err != nil {
			validationFailed = true
			return c.protocolDetail(endpoint, fmt.Sprintf("decoding stream item %d", item), err)
		}
		if wire.Queued == "" {
			validationFailed = true
			return c.protocol(endpoint, "stream item %d has an empty Queued CID", item)
		}
		if len(wire.Queued) > maxCIDTextBytes {
			validationFailed = true
			return c.protocol(endpoint, "stream item %d Queued CID exceeds the %d-byte limit", item, maxCIDTextBytes)
		}
		queued, err := cid.Parse(wire.Queued)
		if err != nil {
			validationFailed = true
			return c.protocolDetail(endpoint, fmt.Sprintf("stream item %d has an invalid Queued CID", item), err)
		}
		key := string(queued.Bytes())
		if _, requested := expected[key]; !requested {
			validationFailed = true
			return c.protocol(endpoint, "stream item %d acknowledges unrequested CID %s", item, queued)
		}
		if _, duplicate := acknowledged[key]; duplicate {
			validationFailed = true
			return c.protocol(endpoint, "stream item %d duplicates acknowledgement for CID %s", item, queued)
		}
		acknowledged[key] = struct{}{}
		return nil
	})
	if err != nil {
		return err
	}
	if len(acknowledged) != len(expected) {
		return c.protocol(endpoint, "response acknowledges %d of %d requested CIDs", len(acknowledged), len(expected))
	}
	return nil
}
