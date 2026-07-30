package kubo

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"strings"
)

const streamErrorHeader = "X-Stream-Error"
const maxJSONNestingDepth = 64
const maxJSONStreamItemBytes = 1 << 20

var errJSONStreamItemTooLarge = errors.New("JSON stream item exceeds limit")

func (c *Client) streamError(endpoint string, item int, message string) *StreamError {
	message = c.redact(strings.TrimSpace(message))
	truncated := len(message) > int(maxErrorBytes)
	message = boundedText(message, int(maxErrorBytes))
	return &StreamError{Endpoint: endpoint, Item: item, Message: message, Truncated: truncated}
}

type countingReader struct {
	r io.Reader
	n int64
}

func (r *countingReader) Read(p []byte) (int, error) {
	n, err := r.r.Read(p)
	r.n += int64(n)
	return n, err
}

// postJSONStream owns the complete response lifecycle. It decodes every JSON
// value through EOF, preserves the first item validation error while draining
// later values, and checks Kubo's late X-Stream-Error trailer only after EOF.
// It stops early only when continuing would cross an explicit resource bound
// or the JSON framing itself is no longer recoverable.
func (c *Client) postJSONStream(
	ctx context.Context,
	endpoint string,
	query url.Values,
	maxBytes int64,
	maxItems int,
	consume func(item int, raw json.RawMessage) error,
) error {
	requestCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	return c.postJSONStreamContext(requestCtx, endpoint, query, maxBytes, maxItems, consume)
}

// postJSONStreamContext is postJSONStream without an implicit request timeout.
// Callers exposing it through a long-running operation must first require an
// explicit caller deadline.
func (c *Client) postJSONStreamContext(
	ctx context.Context,
	endpoint string,
	query url.Values,
	maxBytes int64,
	maxItems int,
	consume func(item int, raw json.RawMessage) error,
) error {
	if maxBytes <= 0 || maxBytes > c.maxStreamBytes {
		return fmt.Errorf("kubo: %s stream byte limit must be between 1 and %d", endpoint, c.maxStreamBytes)
	}
	if maxItems <= 0 || maxItems > c.maxStreamItems {
		return fmt.Errorf("kubo: %s stream item limit must be between 1 and %d", endpoint, c.maxStreamItems)
	}
	u := c.base
	u.Path = strings.TrimSuffix(u.Path, "/") + "/api/v0/" + endpoint
	u.RawQuery = query.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), nil)
	if err != nil {
		return c.protocol(endpoint, "building request: %v", err)
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	req.Header.Set("Accept", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return context.Cause(ctx)
		}
		if errors.Is(err, context.DeadlineExceeded) {
			return context.DeadlineExceeded
		}
		if errors.Is(err, context.Canceled) {
			return context.Canceled
		}
		return &TransportError{Endpoint: endpoint, Problem: boundedText(c.redact(err.Error()), maxDiagnosticBytes)}
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return c.readStatusError(endpoint, resp)
	}
	if resp.ContentLength > maxBytes {
		return c.protocol(endpoint, "response declares %d bytes, over the %d-byte stream limit", resp.ContentLength, maxBytes)
	}
	mediaType, _, err := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		return c.protocol(endpoint, "response Content-Type is %q, want application/json", resp.Header.Get("Content-Type"))
	}

	reader := &countingReader{r: io.LimitReader(resp.Body, maxBytes+1)}
	lines := bufio.NewReaderSize(reader, 64<<10)
	var firstValidation error
	items := 0
	for {
		raw, readErr := readBoundedJSONLine(lines, maxJSONStreamItemBytes)
		if reader.n > maxBytes {
			return c.protocol(endpoint, "response body exceeds the %d-byte stream limit", maxBytes)
		}
		if errors.Is(readErr, errJSONStreamItemTooLarge) {
			return c.protocol(endpoint, "stream item %d exceeds the %d-byte item limit", items+1, maxJSONStreamItemBytes)
		}
		if readErr != nil && !errors.Is(readErr, io.EOF) {
			if ctx.Err() != nil {
				return context.Cause(ctx)
			}
			return c.protocol(endpoint, "reading stream item %d: %v", items+1, readErr)
		}
		raw = bytes.TrimSpace(raw)
		if len(raw) > 0 {
			if !json.Valid(raw) {
				return c.protocol(endpoint, "decoding stream item %d: invalid JSON", items+1)
			}
			items++
			if items > maxItems {
				return c.protocol(endpoint, "stream has more than %d items", maxItems)
			}
			if consume != nil {
				if err := consume(items, json.RawMessage(raw)); err != nil && firstValidation == nil {
					firstValidation = err
				}
			}
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
	}
	if resp.ContentLength >= 0 && reader.n != resp.ContentLength {
		return c.protocol(endpoint, "truncated stream: read %d of %d declared bytes", reader.n, resp.ContentLength)
	}

	trailerValues := append([]string(nil), resp.Header.Values(streamErrorHeader)...)
	trailerValues = append(trailerValues, resp.Trailer.Values(streamErrorHeader)...)
	if len(trailerValues) > 1 && firstValidation == nil {
		firstValidation = c.protocol(endpoint, "response contains duplicate %s trailers", streamErrorHeader)
	}
	if len(trailerValues) > 0 && trailerValues[0] != "" && firstValidation == nil {
		firstValidation = c.streamError(endpoint, 0, trailerValues[0])
	}
	if err := resp.Body.Close(); err != nil && firstValidation == nil {
		if ctx.Err() != nil {
			return context.Cause(ctx)
		}
		firstValidation = c.protocol(endpoint, "closing response stream: %v", err)
	}
	return firstValidation
}

// readBoundedJSONLine reads one Kubo NDJSON value without allowing the
// operation's aggregate byte budget to become a per-value allocation budget.
// ReadSlice exposes fixed-size fragments on ErrBufferFull; the accumulator is
// checked before every append and therefore never grows beyond limit.
func readBoundedJSONLine(reader *bufio.Reader, limit int) ([]byte, error) {
	var result []byte
	for {
		fragment, err := reader.ReadSlice('\n')
		if len(fragment) > limit-len(result) {
			return nil, errJSONStreamItemTooLarge
		}
		result = append(result, fragment...)
		switch {
		case err == nil:
			return result, nil
		case errors.Is(err, bufio.ErrBufferFull):
			continue
		case errors.Is(err, io.EOF):
			return result, io.EOF
		default:
			return result, err
		}
	}
}

// decodeStrictJSON rejects unknown and duplicate object fields before decoding
// one fixed-schema Kubo response value.
func decodeStrictJSON(raw []byte, dst any) error {
	if err := rejectDuplicateJSONFields(raw); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(dst); err != nil {
		return err
	}
	var extra json.RawMessage
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("multiple JSON values")
		}
		return err
	}
	return nil
}

// decodeJSONArray visits one already-bounded JSON array without first
// materializing it as a Go slice. Callers still apply decodeStrictJSON to each
// item, so malformed, duplicate, and unknown fields fail closed while the item
// limit is enforced before a large response can drive proportional object
// allocation.
func decodeJSONArray(raw []byte, maxItems int, consume func(item int, raw json.RawMessage) error) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	start, err := decoder.Token()
	if err != nil {
		return err
	}
	if start != json.Delim('[') {
		return errors.New("value is not an array")
	}
	items := 0
	for decoder.More() {
		items++
		if items > maxItems {
			return fmt.Errorf("array has more than %d items", maxItems)
		}
		var item json.RawMessage
		if err := decoder.Decode(&item); err != nil {
			return fmt.Errorf("decoding array item %d: %w", items, err)
		}
		if consume != nil {
			if err := consume(items, item); err != nil {
				return err
			}
		}
	}
	end, err := decoder.Token()
	if err != nil {
		return err
	}
	if end != json.Delim(']') {
		return errors.New("array has no closing delimiter")
	}
	var extra json.RawMessage
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("multiple JSON values")
		}
		return err
	}
	return nil
}

func rejectDuplicateJSONFields(raw []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := walkJSONValue(decoder, 0); err != nil {
		return err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("multiple JSON values")
		}
		return err
	}
	return nil
}

func walkJSONValue(decoder *json.Decoder, depth int) error {
	if depth > maxJSONNestingDepth {
		return fmt.Errorf("JSON nesting exceeds %d levels", maxJSONNestingDepth)
	}
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delim {
	case '{':
		seen := make(map[string]struct{})
		for decoder.More() {
			nameToken, err := decoder.Token()
			if err != nil {
				return err
			}
			name, ok := nameToken.(string)
			if !ok {
				return errors.New("object field name is not a string")
			}
			if !canonicalKuboFieldName(name) {
				return fmt.Errorf("noncanonical JSON field %q", name)
			}
			foldedName := strings.ToLower(name)
			if _, duplicate := seen[foldedName]; duplicate {
				return fmt.Errorf("duplicate JSON field %q", name)
			}
			seen[foldedName] = struct{}{}
			if err := walkJSONValue(decoder, depth+1); err != nil {
				return err
			}
		}
		end, err := decoder.Token()
		if err != nil {
			return err
		}
		if end != json.Delim('}') {
			return errors.New("object has no closing delimiter")
		}
	case '[':
		for decoder.More() {
			if err := walkJSONValue(decoder, depth+1); err != nil {
				return err
			}
		}
		end, err := decoder.Token()
		if err != nil {
			return err
		}
		if end != json.Delim(']') {
			return errors.New("array has no closing delimiter")
		}
	default:
		return fmt.Errorf("unexpected JSON delimiter %q", delim)
	}
	return nil
}

func canonicalKuboFieldName(name string) bool {
	// Kubo 0.42's typed JSON encoders use exported Go field names verbatim;
	// the sole symbolic field is IPLD's link key "/". encoding/json otherwise
	// accepts case-insensitive aliases, which would undermine the pinned schema
	// and duplicate detector.
	return name == "/" || len(name) > 0 && name[0] >= 'A' && name[0] <= 'Z'
}
