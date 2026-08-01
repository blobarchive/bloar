package kubo

import (
	"context"
	"net/url"
	"unicode/utf8"
)

const maxProvideStrategyBytes = 512

const (
	provideEnabledKey  = "Provide.Enabled"
	provideStrategyKey = "Provide.Strategy"
)

// ConfigProvideEnabled reads only Kubo's Provide.Enabled configuration key.
// It deliberately does not expose a generic config reader: config/show and
// caller-selected keys could disclose private identity or service credentials.
func (c *Client) ConfigProvideEnabled(ctx context.Context) (bool, error) {
	const endpoint = "config"
	raw, err := c.post(ctx, endpoint, configReadQuery(provideEnabledKey), nil, "", "application/json", maxMetadataBytes)
	if err != nil {
		return false, err
	}
	if !utf8.Valid(raw) {
		return false, c.protocol(endpoint, "%s response is not valid UTF-8", provideEnabledKey)
	}
	var wire struct {
		Key   *string
		Value *bool
	}
	if err := decodeStrictJSON(raw, &wire); err != nil {
		return false, c.protocol(endpoint, "decoding %s JSON: %v", provideEnabledKey, err)
	}
	if wire.Key == nil || wire.Value == nil {
		return false, c.protocol(endpoint, "%s response is missing Key or Value", provideEnabledKey)
	}
	if *wire.Key != provideEnabledKey {
		return false, c.protocol(endpoint, "%s response echoed unexpected Key %q", provideEnabledKey, *wire.Key)
	}
	return *wire.Value, nil
}

// ConfigProvideStrategy reads only Kubo's Provide.Strategy configuration key.
// The returned value is intentionally not normalized: startup policy must
// compare the exact configured strategy and fail closed on an unsafe value.
func (c *Client) ConfigProvideStrategy(ctx context.Context) (string, error) {
	const endpoint = "config"
	raw, err := c.post(ctx, endpoint, configReadQuery(provideStrategyKey), nil, "", "application/json", maxMetadataBytes)
	if err != nil {
		return "", err
	}
	if !utf8.Valid(raw) {
		return "", c.protocol(endpoint, "%s response is not valid UTF-8", provideStrategyKey)
	}
	var wire struct {
		Key   *string
		Value *string
	}
	if err := decodeStrictJSON(raw, &wire); err != nil {
		return "", c.protocol(endpoint, "decoding %s JSON: %v", provideStrategyKey, err)
	}
	if wire.Key == nil || wire.Value == nil {
		return "", c.protocol(endpoint, "%s response is missing Key or Value", provideStrategyKey)
	}
	if *wire.Key != provideStrategyKey {
		return "", c.protocol(endpoint, "%s response echoed unexpected Key %q", provideStrategyKey, *wire.Key)
	}
	if len(*wire.Value) > maxProvideStrategyBytes {
		return "", c.protocol(endpoint, "%s Value exceeds the %d-byte limit", provideStrategyKey, maxProvideStrategyBytes)
	}
	return *wire.Value, nil
}

func configReadQuery(key string) url.Values {
	query := jsonQuery()
	query.Set("arg", key)
	// Read the literal persisted value. Expanding placeholders would make a
	// startup safety check depend on an external AutoConf service.
	query.Set("expand-auto", "false")
	return query
}
