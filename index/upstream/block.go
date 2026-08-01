package upstream

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/blobarchive/bloar/schema"
)

// BlockClient reads the trusted beacon block feed of spec 10.1's anchored mode:
// the finalized checkpoint that bounds the walk, the per-slot header that
// decides whether a slot carried a block at all, and the block's blob
// commitments that state what blobs it must contain. It is the sole authority on
// existence and absence -- blob sources (Client) serve only bytes, which this
// feed's commitments anchor.
//
// A beacon node never prunes BLOCKS, so a header 404 is a missed slot and a
// block with no commitments is a verifiably blobless one. The one hazard is a
// node still backfilling historical blocks, which 404s a header it will later
// have -- indistinguishable from a genuine miss on its own. The indexer
// neutralizes that with parent-root continuity (index/beacon's anchored walk),
// not this client.
//
// It reuses a Client's transport, retry budget, and metrics, forced to
// beacon-node shape (no head prefix): the block endpoints are a real node's, not
// an archive's.
type BlockClient struct {
	c *Client
}

// BeaconHeader is one root-addressed, canonical beacon header. Finalized says
// what the node attested for this header; an unfinalized indexer may accept
// false, but never execution_optimistic or non-canonical data.
type BeaconHeader struct {
	Slot       uint64
	Root       [32]byte
	ParentRoot [32]byte
	Finalized  bool
}

// ExecutionOptimisticError reports an otherwise well-formed beacon response
// whose execution payload has not yet been verified. It is a temporary source
// state, not malformed ancestry: callers must retain their last proved view and
// may retry, but must never use the response as an authority.
type ExecutionOptimisticError struct {
	Path string
}

func (e *ExecutionOptimisticError) Error() string {
	return fmt.Sprintf("upstream: %s: response reports execution_optimistic:true; "+
		"an unverified execution payload cannot authorize a trusted block read", e.Path)
}

// NewBlockClient returns a BlockClient over cfg. cfg.Head is ignored -- a block
// feed is always a beacon node -- and the rest (transport, retry budget, logger,
// metrics) is shared with a Client built the same way.
func NewBlockClient(cfg Config) (*BlockClient, error) {
	cfg.Head = ""
	c, err := New(cfg)
	if err != nil {
		return nil, err
	}
	return &BlockClient{c: c}, nil
}

// FinalizedSlot is F in spec 10.1's loop for anchored mode: the finalized slot
// from GET /eth/v1/beacon/headers/finalized. ok=false means the caller must wait
// rather than read, and it is not an error:
//
//   - execution_optimistic true: the node has not verified the execution payload
//     of its own head, so its finality is not a read bound (spec 10.3). The
//     caller waits for the node to catch up.
//   - HTTP 503: prysm answers 503 SYNCING while it is still syncing. A syncing
//     node has no finality to report yet; the caller waits rather than
//     crash-looping on it.
//
// The finalized-header response carries three coverage-bearing safety booleans,
// all required by the beacon-API contract (blocks/header.yaml): top-level
// execution_optimistic and finalized, and data.canonical. Together they are what
// AUTHORIZES the returned slot as a finalized read bound that anchored mode commits
// as coverage. Each must be explicitly present -- an
// omitted or null flag fails CLOSED through the retry path (a retryable
// transportError, like a torn body), never collapsing to a permissive default the
// node did not attest. execution_optimistic true is the one "wait" (the node has
// not verified its own execution payloads, spec 10.3); finalized false or
// data.canonical false ON the finalized endpoint is the node contradicting itself
// -- a malfunction, retried, not a bound and not a wait. Only all-three-safe
// (execution_optimistic false, finalized true, canonical true) permits the bound.
func (b *BlockClient) FinalizedSlot(ctx context.Context) (slot uint64, ok bool, err error) {
	header, ok, err := b.finalizedHeader(ctx, false)
	return header.Slot, ok, err
}

// FinalizedHeader is FinalizedSlot with the checkpoint root and parent root
// retained. The optimistic tracker uses the exact root as its ancestry anchor;
// finalized-only callers may continue to use FinalizedSlot.
func (b *BlockClient) FinalizedHeader(ctx context.Context) (header BeaconHeader, ok bool, err error) {
	return b.finalizedHeader(ctx, true)
}

func (b *BlockClient) finalizedHeader(ctx context.Context, requireRoot bool) (header BeaconHeader, ok bool, err error) {
	const path = "/eth/v1/beacon/headers/finalized"
	out, err := getJSONValidated(ctx, b.c, path, metaBodyCeiling, func(out *finalizedHeaderDTO) error {
		// All three flags must be explicitly present. Missing or null is a malformed
		// answer whichever the node's state, so it is checked before the wait split.
		if err := requirePresentBool(path, "execution_optimistic", out.ExecutionOptimistic); err != nil {
			return err
		}
		if err := requirePresentBool(path, "finalized", out.Finalized); err != nil {
			return err
		}
		if err := requirePresentBool(path, "data.canonical", out.Data.Canonical); err != nil {
			return err
		}
		// An optimistic node is the "wait" case handled after the fetch; its
		// finalized/canonical values are moot because the bound is discarded anyway.
		if out.ExecutionOptimistic.Value {
			return nil
		}
		// Authorizing the bound: the node must attest this header as finalized AND
		// canonical. Either false here is the node contradicting the very endpoint it
		// answered, so it is retried, not trusted as a bound.
		if !out.Finalized.Value {
			return fmt.Errorf("upstream: %s: finalized header reports finalized:false; a node whose finalized "+
				"checkpoint is not finalized is malfunctioning, so its slot is not a read bound", path)
		}
		if !out.Data.Canonical.Value {
			return fmt.Errorf("upstream: %s: finalized header reports data.canonical:false; a non-canonical "+
				"finalized header is a contradiction, so its slot is not a read bound", path)
		}
		return nil
	})
	if err != nil {
		var httpErr *HTTPError
		if errors.As(err, &httpErr) && httpErr.Status == http.StatusServiceUnavailable {
			return BeaconHeader{}, false, nil
		}
		return BeaconHeader{}, false, err
	}
	if out.ExecutionOptimistic.Value {
		return BeaconHeader{}, false, nil
	}
	raw := out.Data.Header.Message.Slot
	slot, perr := strconv.ParseUint(raw, 10, 64)
	if perr != nil {
		return BeaconHeader{}, false, fmt.Errorf("upstream: %s: slot %q is not a number: %w", path, raw, perr)
	}
	var root, parent [32]byte
	if requireRoot {
		root, err = parseRoot(path, "root", out.Data.Root)
		if err != nil {
			return BeaconHeader{}, false, err
		}
		parent, err = parseRoot(path, "parent_root", out.Data.Header.Message.ParentRoot)
		if err != nil {
			return BeaconHeader{}, false, err
		}
	}
	return BeaconHeader{Slot: slot, Root: root, ParentRoot: parent, Finalized: true}, true, nil
}

// Head reads the node's canonical consensus head. It requires an
// execution-verified, explicitly canonical response but deliberately permits
// finalized:false: that is the provisional chain this API exists to observe.
// A syncing 503 is ok=false so a tracker waits without crash-looping.
func (b *BlockClient) Head(ctx context.Context) (BeaconHeader, bool, error) {
	return b.canonicalHeader(ctx, "head", false)
}

// HeaderByRoot reads a specific canonical header by root. Addressing by root is
// what makes a parent walk coherent while the node's head changes concurrently.
func (b *BlockClient) HeaderByRoot(ctx context.Context, root [32]byte) (BeaconHeader, error) {
	id := "0x" + hex.EncodeToString(root[:])
	header, ok, err := b.canonicalHeader(ctx, id, true)
	if err != nil {
		return BeaconHeader{}, err
	}
	if !ok {
		return BeaconHeader{}, fmt.Errorf("upstream: canonical header %s is unavailable", id)
	}
	if header.Root != root {
		return BeaconHeader{}, fmt.Errorf("upstream: header requested as %s answered with root 0x%x", id, header.Root)
	}
	return header, nil
}

// canonicalHeader implements Head and HeaderByRoot. requireExists controls the
// only 404 distinction: a moving `head` may transiently be unavailable, while a
// parent root already named by a canonical child must exist or the ancestry is
// malformed/incomplete.
func (b *BlockClient) canonicalHeader(ctx context.Context, id string, requireExists bool) (BeaconHeader, bool, error) {
	path := "/eth/v1/beacon/headers/" + id
	start := time.Now()
	out, err := getJSONValidated(ctx, b.c, path, metaBodyCeiling, func(out *slotHeaderDTO) error {
		if err := requirePresentBool(path, "execution_optimistic", out.ExecutionOptimistic); err != nil {
			return err
		}
		if out.ExecutionOptimistic.Value {
			return &ExecutionOptimisticError{Path: path}
		}
		if err := requirePresentBool(path, "finalized", out.Finalized); err != nil {
			return err
		}
		if err := requireSafeBool(path, "data.canonical", out.Data.Canonical, true); err != nil {
			return err
		}
		return nil
	})
	b.c.metrics.BlockRead(time.Since(start))
	if err != nil {
		var httpErr *HTTPError
		if errors.As(err, &httpErr) {
			switch httpErr.Status {
			case http.StatusServiceUnavailable:
				return BeaconHeader{}, false, nil
			case http.StatusNotFound:
				if !requireExists {
					return BeaconHeader{}, false, nil
				}
			}
		}
		return BeaconHeader{}, false, err
	}
	slot, err := strconv.ParseUint(out.Data.Header.Message.Slot, 10, 64)
	if err != nil {
		return BeaconHeader{}, false, fmt.Errorf("upstream: %s: slot %q is not a number: %w", path, out.Data.Header.Message.Slot, err)
	}
	root, err := parseRoot(path, "root", out.Data.Root)
	if err != nil {
		return BeaconHeader{}, false, err
	}
	parent, err := parseRoot(path, "parent_root", out.Data.Header.Message.ParentRoot)
	if err != nil {
		return BeaconHeader{}, false, err
	}
	return BeaconHeader{Slot: slot, Root: root, ParentRoot: parent, Finalized: out.Finalized.Value}, true, nil
}

// Header reads GET /eth/v1/beacon/headers/{slot}: the block's root and its
// parent's root, and whether the slot carried a block at all.
//
// present=false is a 404: a candidate missed slot. It is only a CANDIDATE
// because a node still backfilling historical blocks 404s a header it will later
// have, indistinguishable from a genuine miss here -- the anchored walk proves
// which by parent-root continuity, never this client. Every other non-200 is an
// error.
//
// A 200 is a header for a slot at or below the finalized bound F, so its
// beacon-API safety flags (blocks/header.yaml requires all three) must attest it as
// finalized, canonical, and non-optimistic: execution_optimistic false, finalized
// true, data.canonical true. A slot within F that answers otherwise -- or omits or
// nulls a flag -- is the node contradicting the bound it already gave, so the read
// fails closed inside the attempt handler and is retried, never trusted to build a
// block's expected versioned hashes on an unverified or orphaned header.
func (b *BlockClient) Header(ctx context.Context, slot uint64) (root, parentRoot [32]byte, present bool, err error) {
	path := "/eth/v1/beacon/headers/" + strconv.FormatUint(slot, 10)
	start := time.Now()
	out, err := getJSONValidated(ctx, b.c, path, metaBodyCeiling, func(out *slotHeaderDTO) error {
		return requireFinalizedRead(path, out.ExecutionOptimistic, out.Finalized, &out.Data.Canonical)
	})
	b.c.metrics.BlockRead(time.Since(start))
	if err != nil {
		var httpErr *HTTPError
		if errors.As(err, &httpErr) && httpErr.Status == http.StatusNotFound {
			return [32]byte{}, [32]byte{}, false, nil
		}
		return [32]byte{}, [32]byte{}, false, err
	}
	if root, err = parseRoot(path, "root", out.Data.Root); err != nil {
		return [32]byte{}, [32]byte{}, false, err
	}
	if parentRoot, err = parseRoot(path, "parent_root", out.Data.Header.Message.ParentRoot); err != nil {
		return [32]byte{}, [32]byte{}, false, err
	}
	return root, parentRoot, true, nil
}

// Commitments reads a present slot's blob_kzg_commitments (spec 10.1). It uses
// GET /eth/v1/beacon/blinded_blocks/{slot}, whose body carries the commitments
// without the blobs -- much smaller than the full v2 block, and all anchored
// mode needs to derive a slot's expected versioned hashes.
//
// It is called only for slots Header returned present, so a 404 here would be a
// node contradicting itself and is an error, not absence. An empty result is a
// verifiably blobless slot.
func (b *BlockClient) Commitments(ctx context.Context, slot uint64) ([][48]byte, error) {
	path := "/eth/v1/beacon/blinded_blocks/" + strconv.FormatUint(slot, 10)
	return b.commitments(ctx, path, true)
}

// CommitmentsByRoot reads a canonical provisional block's commitments by its
// immutable block root. It requires an execution-verified response but permits
// finalized:false; the root-addressed ancestry and final head recheck provide
// the consensus snapshot boundary.
func (b *BlockClient) CommitmentsByRoot(ctx context.Context, root [32]byte) ([][48]byte, error) {
	path := "/eth/v1/beacon/blinded_blocks/0x" + hex.EncodeToString(root[:])
	return b.commitments(ctx, path, false)
}

func (b *BlockClient) commitments(ctx context.Context, path string, requireFinalized bool) ([][48]byte, error) {
	start := time.Now()
	// The presence check runs INSIDE the attempt handler: a missing
	// level is a malformed answer, so it fails closed as a retryable transportError
	// and re-issues the request, exactly as a torn body does. Running it after do()
	// returned counted a malformed response as one successful request, so a
	// malformed-first/corrected-second node terminally failed on the first answer.
	// It is an error, not a blobless result -- like a 404 here (Header already proved
	// the slot present) it is the node contradicting itself, never committed as
	// coverage.
	out, err := getJSONValidated(ctx, b.c, path, metaBodyCeiling, func(out *blindedBlockDTO) error {
		// This slot is at or below F, so its safety flags (blinded_block.yaml requires
		// execution_optimistic and finalized; it has no canonical) must attest a
		// finalized, non-optimistic block. Missing, null, or an unsafe value is the
		// node contradicting the bound -- retried, never used to derive versioned hashes.
		if requireFinalized {
			if err := requireFinalizedRead(path, out.ExecutionOptimistic, out.Finalized, nil); err != nil {
				return err
			}
		} else {
			if err := requirePresentBool(path, "execution_optimistic", out.ExecutionOptimistic); err != nil {
				return err
			}
			if out.ExecutionOptimistic.Value {
				return fmt.Errorf("upstream: %s: response reports execution_optimistic:true; commitments from an unverified execution payload are not admissible", path)
			}
			if err := requirePresentBool(path, "finalized", out.Finalized); err != nil {
				return err
			}
		}
		switch {
		case out.Data == nil:
			return fmt.Errorf("upstream: %s: response has no data object", path)
		case out.Data.Message == nil:
			return fmt.Errorf("upstream: %s: response data has no message object", path)
		case out.Data.Message.Body == nil:
			return fmt.Errorf("upstream: %s: response message has no body object", path)
		case out.Data.Message.Body.BlobKZGCommitments == nil:
			return fmt.Errorf("upstream: %s: block body omits blob_kzg_commitments; a verifiably blobless slot "+
				"states it as an explicit empty array, so a missing or null member is a malformed answer, not a blobless block", path)
		}
		return nil
	})
	b.c.metrics.BlockRead(time.Since(start))
	if err != nil {
		return nil, err
	}
	raw := *out.Data.Message.Body.BlobKZGCommitments
	// Refuse a block that declares more commitments than a slot can hold (finding
	// the request safety boundary): anchored mode turns each commitment into a versioned hash
	// and then requests exactly that many blobs, so an over-ceiling array here would
	// scale the blob-body ceiling downstream without bound. A precise source error,
	// terminal rather than retried -- a block packing more than the protocol permits
	// is malformed, not a transient answer, and never a slot to derive hashes from.
	if len(raw) > schema.MaxBlobsPerSlotCeiling {
		return nil, fmt.Errorf("upstream: %s: block declares %d blob commitments, more than the %d a slot can hold; "+
			"a source packing more than the protocol permits is malformed", path, len(raw), schema.MaxBlobsPerSlotCeiling)
	}
	commits := make([][48]byte, 0, len(raw))
	for i, s := range raw {
		c, err := parseCommitment(s)
		if err != nil {
			return nil, fmt.Errorf("upstream: %s: commitment %d: %w", path, i, err)
		}
		commits = append(commits, c)
	}
	return commits, nil
}

// parseRoot decodes a 0x-prefixed 32-byte root from a header response.
func parseRoot(path, field, s string) ([32]byte, error) {
	raw, err := hex.DecodeString(strings.TrimPrefix(s, "0x"))
	if err != nil {
		return [32]byte{}, fmt.Errorf("upstream: %s: %s %q is not hex: %w", path, field, s, err)
	}
	if len(raw) != 32 {
		return [32]byte{}, fmt.Errorf("upstream: %s: %s is %d bytes, want 32", path, field, len(raw))
	}
	return [32]byte(raw), nil
}

// parseCommitment decodes a 0x-prefixed 48-byte KZG commitment.
func parseCommitment(s string) ([48]byte, error) {
	raw, err := hex.DecodeString(strings.TrimPrefix(s, "0x"))
	if err != nil {
		return [48]byte{}, fmt.Errorf("not hex: %w", err)
	}
	if len(raw) != 48 {
		return [48]byte{}, fmt.Errorf("is %d bytes, want exactly 48", len(raw))
	}
	return [48]byte(raw), nil
}

// finalizedHeaderDTO decodes GET /eth/v1/beacon/headers/finalized: the three
// coverage-bearing safety flags (blocks/header.yaml) and the finalized slot.
type finalizedHeaderDTO struct {
	ExecutionOptimistic optionalBool `json:"execution_optimistic"`
	Finalized           optionalBool `json:"finalized"`
	Data                struct {
		Root      string       `json:"root"`
		Canonical optionalBool `json:"canonical"`
		Header    struct {
			Message struct {
				Slot       string `json:"slot"`
				ParentRoot string `json:"parent_root"`
			} `json:"message"`
		} `json:"header"`
	} `json:"data"`
}

// slotHeaderDTO decodes GET /eth/v1/beacon/headers/{slot}: the same safety flags
// plus the block's root and parent_root.
type slotHeaderDTO struct {
	ExecutionOptimistic optionalBool `json:"execution_optimistic"`
	Finalized           optionalBool `json:"finalized"`
	Data                struct {
		Root      string       `json:"root"`
		Canonical optionalBool `json:"canonical"`
		Header    struct {
			Message struct {
				Slot       string `json:"slot"`
				ParentRoot string `json:"parent_root"`
			} `json:"message"`
		} `json:"header"`
	} `json:"data"`
}

// blindedBlockDTO decodes GET /eth/v1/beacon/blinded_blocks/{slot}: the two safety
// flags it carries (blinded_block.yaml has no canonical) and the commitments path.
// Every object on the path to blob_kzg_commitments is a pointer so a missing or null
// member at ANY level is distinguishable from an explicitly present empty array
// : only that empty array is a verifiably blobless slot.
type blindedBlockDTO struct {
	ExecutionOptimistic optionalBool `json:"execution_optimistic"`
	Finalized           optionalBool `json:"finalized"`
	Data                *struct {
		Message *struct {
			Body *struct {
				BlobKZGCommitments *[]string `json:"blob_kzg_commitments"`
			} `json:"body"`
		} `json:"message"`
	} `json:"data"`
}

// requirePresentBool checks that a beacon-API boolean the caller depends on is
// explicitly present -- not absent, not null. A trusted read cannot let a missing
// flag collapse to a permissive zero value, so a gap here is a malformed answer, not
// a default.
func requirePresentBool(path, field string, b optionalBool) error {
	if !b.Present || b.Null {
		return fmt.Errorf("upstream: %s: response omits %s; the beacon-API contract requires it and a trusted read "+
			"depends on its value, so a missing or null flag is a malformed answer, not a permitted default", path, field)
	}
	return nil
}

// requireSafeBool is requirePresentBool plus the value a trusted finalized read
// requires: a present flag whose value is not want is the node declaring the slot
// unverified or orphaned within a range it already called final.
func requireSafeBool(path, field string, b optionalBool, want bool) error {
	if err := requirePresentBool(path, field, b); err != nil {
		return err
	}
	if b.Value != want {
		return fmt.Errorf("upstream: %s: response reports %s:%t on a slot within the finalized bound, which a "+
			"finalized canonical block cannot; trusting it would build on an unverified or orphaned block", path, field, b.Value)
	}
	return nil
}

// requireFinalizedRead holds a per-slot trusted read -- a header or a blinded block
// for a slot at or below F -- to the safety its position implies: the node must
// attest it non-optimistic and finalized, and, for a header (which carries the
// field), canonical. canon is nil for a response type with no canonical member (the
// blinded block, per blinded_block.yaml). Any flag absent, null, or unsafe is the
// node contradicting the bound it already gave, wrapped by the caller as retryable.
func requireFinalizedRead(path string, eo, fin optionalBool, canon *optionalBool) error {
	if err := requirePresentBool(path, "execution_optimistic", eo); err != nil {
		return err
	}
	if eo.Value {
		// A node may briefly expose an optimistic execution payload even for a
		// slot below the finalized checkpoint it just reported. The response is
		// still inadmissible, but the condition is source liveness rather than
		// malformed metadata. Preserve it as a typed error through the transport
		// retry wrapper so the anchored run loop can retain its durable head and
		// retry in-process after the request budget is exhausted.
		return &ExecutionOptimisticError{Path: path}
	}
	if err := requireSafeBool(path, "finalized", fin, true); err != nil {
		return err
	}
	if canon != nil {
		if err := requireSafeBool(path, "data.canonical", *canon, true); err != nil {
			return err
		}
	}
	return nil
}

// optionalBool distinguishes a JSON boolean field's three inhabited states --
// absent, present-and-null, present-and-set -- for a coverage-bearing flag whose
// meaning turns on presence: FinalizedSlot's execution_optimistic, where absent or
// null must fail closed rather than collapse to false. encoding/json
// invokes UnmarshalJSON only when the key is present, including for an explicit
// null, so an untouched zero value is exactly "absent".
type optionalBool struct {
	Present bool
	Null    bool
	Value   bool
}

func (o *optionalBool) UnmarshalJSON(b []byte) error {
	o.Present = true
	switch string(b) {
	case "null":
		o.Null = true
	case "true":
		o.Value = true
	case "false":
		o.Value = false
	default:
		// An UnmarshalTypeError so encoding/json fills in the struct field path: the
		// message then names the actual malformed flag (finalized, data.canonical, ...)
		// rather than a shared hardcoded field name, since this one method decodes all
		// three. It fails closed either way; this is observability.
		return &json.UnmarshalTypeError{Value: string(b), Type: reflect.TypeFor[bool]()}
	}
	return nil
}
