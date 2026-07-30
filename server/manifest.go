package server

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"

	"github.com/blobarchive/bloar/schema"
)

// ErrManifestConflict reports a manifest POST whose prev does not equal the
// head's current tip (spec 7.2, 10.5). The HTTP layer maps it to 409: the same
// conflict a refs or truncate 409 reports, and for the same reason -- the writer
// raced another upgrade or is working from a stale tip. It is a compare-and-swap
// failure, not a malformed request, so the fix is to re-read the tip and rebuild.
var ErrManifestConflict = errors.New("server: manifest prev does not match the head's current tip")

// ManifestGenerationConflict reports a manifest POST whose expected_head_root
// does not equal the head's current root. The head root
// is the head's generation id: the indexer captures it during its append-only
// preflight and sends it here, so that a refs commit landing between the preflight
// and this POST -- one that advances the head's position and could make a
// formerly-legal schedule rewrite newly-covered ground -- is caught as a stale
// generation rather than published against a position that has since moved. The
// HTTP layer maps it to 409; the writer re-runs the preflight against the advanced
// head. Current is the root the head holds now.
type ManifestGenerationConflict struct {
	Head     string
	Expected cid.Cid
	Current  cid.Cid
}

func (e *ManifestGenerationConflict) Error() string {
	return fmt.Sprintf("server: manifest for head %q carries expected_head_root %s but the head's root is now %s: a refs "+
		"commit advanced the head between the append-only preflight and this POST (spec 10.5). Re-run the preflight "+
		"against the current root", e.Head, cidOrNull(e.Expected), cidOrNull(e.Current))
}

// SetManifest advances the named head's manifest chain (spec 7.2, 10.5). It is
// the writer-side compare-and-swap: the new Manifest becomes the tip iff prev
// equals the tip the head currently holds -- or both are undefined, the genesis
// bootstrap from no-tip to tip.
//
// The ordering is the crash-safe one every mutation here uses, publish-last: the
// block is made durable, then the tip is swapped (persisted and rendered), so a
// reader never sees a tip whose block is not there and a crash leaves the old tip
// intact. The recursive manifest pin is not taken here -- it lands the way a
// head-root pin does, from the reconciler this notifies (and GC reconciles before
// it marks, so the new tip is protected before any sweep can reach it). The CAS
// compares CIDs and nothing else: whether the new schedule is a legal append-only
// successor is a statement about L1 that only the indexer can make (spec 10.5).
//
// block and manifestCID are the canonical encoding of the manifest the caller
// decoded and its CID; prev is the manifest's prev link (cid.Undef for genesis).
// expectedHeadRoot is the head root the caller ran its append-only preflight
// against, compared here atomically with the prev CAS:
// the generation binding that closes the validate-then-publish race.
func (h *Heads) SetManifest(ctx context.Context, name string, block []byte, manifestCID, prev, expectedHeadRoot cid.Cid) (cid.Cid, error) {
	h.cfg.Gate.Enter()
	defer h.cfg.Gate.Leave()

	h.mu.Lock()
	defer h.mu.Unlock()

	if h.cfg.Manifests == nil || h.cfg.Blocks == nil {
		return cid.Undef, errors.New("server: this node has no manifest store or blockstore configured")
	}
	e, err := h.writable(name)
	if err != nil {
		return cid.Undef, err
	}
	if e.kind == UnfinalizedMutable {
		return cid.Undef, ErrMutableGenerationOnly
	}
	// The compare-and-swap on prev. e.manifestTip is undef when the head has no
	// tip yet, which must be matched by a null prev (genesis); otherwise prev must
	// equal the tip exactly.
	if e.manifestTip != prev {
		return cid.Undef, fmt.Errorf("%w: prev is %s but the head's tip is %s",
			ErrManifestConflict, cidOrNull(prev), cidOrNull(e.manifestTip))
	}
	// The generation compare, in the same critical section as the prev CAS so a
	// refs commit cannot slip between them: the preflight's position, and so its
	// append-only verdict, is only valid for the root it read.
	if root := e.head.Root(); expectedHeadRoot != root {
		return cid.Undef, &ManifestGenerationConflict{Head: name, Expected: expectedHeadRoot, Current: root}
	}

	// Prepare the exact prospective registry and signed document before any
	// durable selector changes. A publication revision may be burned if a later
	// block/tip write fails; a hole is harmless, while changing the ManifestStore
	// before discovering a signing failure could let reconciliation retire the
	// still-published old tip.
	pub := *e.durable
	pub.Manifest = manifestCID.String()
	ne := *e
	ne.manifestTip = manifestCID
	ne.durable = &pub
	prospective := h.reg.Load().with(&ne)
	doc, err := h.buildPublication(prospective)
	if err != nil {
		return cid.Undef, err
	}

	// Durable block first: the tip about to name it must never point at a block
	// that is not there. Put is idempotent, so a retried genesis bootstrap after a
	// crash re-stores the same bytes harmlessly.
	blk, err := blocks.NewBlockWithCid(block, manifestCID)
	if err != nil {
		return cid.Undef, fmt.Errorf("server: framing manifest block %s: %w", manifestCID, err)
	}
	if err := h.cfg.Blocks.Put(ctx, blk); err != nil {
		return cid.Undef, fmt.Errorf("server: storing manifest block %s: %w", manifestCID, err)
	}
	// Then swap the tip. After this succeeds every remaining operation is
	// infallible: one registry store, one already-rendered document store, and a
	// notification callback whose contract is nonblocking/no-error.
	if err := h.putManifestTip(ctx, name, manifestCID); err != nil {
		return cid.Undef, err
	}
	h.reg.Store(prospective)
	h.installPublication(doc)
	// Nudge the reconciler, which turns the persisted tip into the recursive
	// manifest pin and drops the previous one (spec 9). Reusing the root-swap hook:
	// it only marks the head pending, and a reconcile reads the head's current tip.
	if h.cfg.OnRoot != nil {
		h.cfg.OnRoot(name, e.head.Root())
	}
	return manifestCID, nil
}

// cidOrNull renders a CID for an error message, or "null" for the undefined one
// (the genesis prev).
func cidOrNull(c cid.Cid) string {
	if !c.Defined() {
		return "null"
	}
	return c.String()
}

// manifestRequest is the body of POST /bloar/v1/heads/{head}/manifest (spec 7.2).
type manifestRequest struct {
	Manifest *manifestReqJSON `json:"manifest"`
	// Confirm must be the head's own name. Spec 7.2 recommends it and it is
	// required here: the manifest is a rarely-run write that redefines what a head
	// means, and confirm guards against aiming it at the wrong head -- a different
	// failure from the CAS, which guards against a race, so both gates stand.
	Confirm string `json:"confirm"`
	// ExpectedHeadRoot is the head root the caller ran its append-only preflight
	// against. Required: a manifest upgrade is only legal
	// relative to the head's position, and the preflight that judged it did so
	// against exactly this root -- the server rejects a POST whose root has since
	// advanced (ManifestGenerationConflict) so the preflight's verdict cannot be
	// applied to a position it never saw.
	ExpectedHeadRoot string `json:"expected_head_root"`
}

// manifestJSON is a Manifest in the JSON shape the GET endpoint RETURNS (spec 7.2,
// 10.5). It is the encode side only: the POST body is decoded through the
// presence-aware manifestReqJSON below. The two are separate on purpose -- the
// response must OMIT a field that does not apply (an open-ended source's
// until_block, a blob-txs source's topic) to match the canonical "absent unless it
// applies" rule, which omitempty pointers/strings do; the request must instead
// DISTINGUISH absent from an explicit null, which needs presence-aware types that
// can never omit. Prev is presence-aware in both directions: the spec's
// "bafy.. | null" is an explicit null for genesis, rendered here and required on
// decode.
type manifestJSON struct {
	V       uint64               `json:"v"`
	Head    string               `json:"head"`
	Sources []manifestSourceJSON `json:"sources"`
	Prev    optionalString       `json:"prev"`
}

// manifestSourceJSON is one source in the GET RESPONSE. The type-specific fields
// are omitempty and until_block is a pointer, so the JSON mirrors the canonical
// encoding's rule (spec 10.5): a field that does not apply, or an open-ended
// source's until_block, is absent rather than null.
type manifestSourceJSON struct {
	Type       string   `json:"type"`
	Address    string   `json:"address"`
	Topic      string   `json:"topic,omitempty"`
	Senders    []string `json:"senders,omitempty"`
	FromBlock  uint64   `json:"from_block"`
	UntilBlock *uint64  `json:"until_block,omitempty"`
}

// manifestReqJSON is a Manifest in the shape the POST body carries (spec 7.2,
// 10.5): the decode-side sibling of manifestJSON. Every field whose contract turns
// on presence is presence-aware, so an absent key, an explicit null, and a real
// value are three distinct states the decoder can act on before the manifest is
// built -- what the canonical encoding's "absent unless it applies" rule requires
// and what a plain string/slice/pointer cannot express.
type manifestReqJSON struct {
	V       uint64                  `json:"v"`
	Head    string                  `json:"head"`
	Sources []manifestSourceReqJSON `json:"sources"`
	Prev    optionalString          `json:"prev"`
}

// manifestSourceReqJSON is one source of the POST body. from_block is required and
// presence-aware (an explicit 0 is a real schedule start, an absent key is a
// mistake); topic, senders, and until_block are presence-aware so the decode layer
// can reject a field PRESENT where the source's type forbids it, and an
// until_block that is present-but-null, instead of canonicalizing either into
// absence.
type manifestSourceReqJSON struct {
	Type       string          `json:"type"`
	Address    string          `json:"address"`
	Topic      optionalString  `json:"topic"`
	Senders    optionalStrings `json:"senders"`
	FromBlock  optionalUint64  `json:"from_block"`
	UntilBlock optionalUint64  `json:"until_block"`
}

// toSchema converts the request's manifest into a schema.Manifest and its prev
// link. It enforces the PRESENCE contract -- which keys must appear, which must be
// absent for a given type, and the absent-vs-null distinction on until_block and
// prev -- and leaves the STRUCTURAL contract -- byte widths, the required
// type-specific value, a non-empty allowlist, the type set itself -- to
// schema.EncodeManifest, so the two layers do not duplicate each other's rules.
func (m *manifestReqJSON) toSchema() (*schema.Manifest, cid.Cid, error) {
	out := &schema.Manifest{V: m.V, Head: m.Head}
	for i, s := range m.Sources {
		src := schema.Source{Type: s.Type}
		var err error
		if src.Address, err = decodeHexBytes(s.Address); err != nil {
			return nil, cid.Undef, fmt.Errorf("source %d address: %w", i, err)
		}
		// from_block is required and presence-aware: omitted or null is
		// a 400 -- a source with no from_block is not a schedule -- while an explicit 0
		// stays valid, since a real schedule can begin at block 0.
		if !s.FromBlock.Present || s.FromBlock.Null {
			return nil, cid.Undef, fmt.Errorf("source %d: from_block is required", i)
		}
		src.FromBlock = s.FromBlock.Value

		// Type-specific field applicability: the decode layer rejects a
		// field PRESENT where the type forbids it -- null or any other spelling -- so a
		// non-applicable key can never collapse into the absence the canonical form
		// requires (spec 10.5). The type set, byte widths, and the required value of
		// an applicable field remain schema.validate's authority: an unknown type, or
		// an inbox-events source whose topic is absent, is rejected there.
		switch s.Type {
		case schema.SourceInboxEvents:
			if s.Senders.Present {
				return nil, cid.Undef, fmt.Errorf(
					"source %d: senders is a blob-txs field and must be absent on an inbox-events source (spec 10.5)", i)
			}
			if s.Topic.Present && !s.Topic.Null {
				if src.Topic, err = decodeHexBytes(s.Topic.Value); err != nil {
					return nil, cid.Undef, fmt.Errorf("source %d topic: %w", i, err)
				}
			}
		case schema.SourceBlobTxs:
			if s.Topic.Present {
				return nil, cid.Undef, fmt.Errorf(
					"source %d: topic is an inbox-events field and must be absent on a blob-txs source (spec 10.5)", i)
			}
			if s.Senders.Present && !s.Senders.Null {
				for j, sender := range s.Senders.Value {
					b, err := decodeHexBytes(sender)
					if err != nil {
						return nil, cid.Undef, fmt.Errorf("source %d sender %d: %w", i, j, err)
					}
					src.Senders = append(src.Senders, b)
				}
			}
		default:
			// An unknown type: schema.EncodeManifest rejects it by name, which is the
			// real fault -- which keys apply is undefined until the type is valid.
		}

		// until_block is absent for an open-ended source, a value for a bounded one,
		// and NEVER an explicit null: spec 10.5 makes open-ended the ABSENT key, so a
		// present null is a malformed spelling, not open-ended.
		switch {
		case !s.UntilBlock.Present:
			src.OpenEnded = true
		case s.UntilBlock.Null:
			return nil, cid.Undef, fmt.Errorf(
				"source %d: until_block is present but null; omit it entirely for an open-ended source (spec 10.5)", i)
		default:
			src.UntilBlock = s.UntilBlock.Value
		}
		out.Sources = append(out.Sources, src)
	}

	// prev is required and three-valued: an absent key is a mistake,
	// an explicit null is the genesis manifest (cid.Undef), and a string is the tip
	// CID this manifest extends. A present but empty string is neither a CID nor the
	// genesis null, so it is its own rejection rather than a confusing CID parse.
	prev := cid.Undef
	switch {
	case !m.Prev.Present:
		return nil, cid.Undef, errors.New("prev is required; send an explicit null for the genesis manifest, " +
			"which the spec distinguishes from an omitted field (spec 10.5)")
	case m.Prev.Null:
		// genesis: prev stays cid.Undef.
	case m.Prev.Value == "":
		return nil, cid.Undef, errors.New("prev is present but an empty string; send null for the genesis manifest " +
			"or the tip CID this manifest extends")
	default:
		var err error
		if prev, err = cid.Decode(m.Prev.Value); err != nil {
			return nil, cid.Undef, fmt.Errorf("prev %q is not a CID: %w", m.Prev.Value, err)
		}
	}
	out.Prev = prev
	return out, prev, nil
}

// manifestToJSON renders a decoded Manifest as the GET response shape. It omits
// exactly what the canonical encoding treats as absent.
func manifestToJSON(m *schema.Manifest) manifestJSON {
	out := manifestJSON{V: m.V, Head: m.Head}
	for _, s := range m.Sources {
		sj := manifestSourceJSON{Type: s.Type, Address: hexBytes(s.Address), FromBlock: s.FromBlock}
		if len(s.Topic) > 0 {
			sj.Topic = hexBytes(s.Topic)
		}
		for _, sender := range s.Senders {
			sj.Senders = append(sj.Senders, hexBytes(sender))
		}
		if !s.OpenEnded {
			ub := s.UntilBlock
			sj.UntilBlock = &ub
		}
		out.Sources = append(out.Sources, sj)
	}
	// The rendered prev is always present: the spec's null for genesis, the tip CID
	// otherwise. optionalString.MarshalJSON turns each into the same bytes the old
	// *string did.
	if m.Prev.Defined() {
		out.Prev = optionalString{Present: true, Value: m.Prev.String()}
	} else {
		out.Prev = optionalString{Present: true, Null: true}
	}
	return out
}

// decodeHexBytes parses a 0x-prefixed (optional) hex byte string. Widths are the
// schema's to enforce, not this function's.
func decodeHexBytes(s string) ([]byte, error) {
	b, err := hex.DecodeString(strings.TrimPrefix(s, "0x"))
	if err != nil {
		return nil, fmt.Errorf("%q is not hex: %w", s, err)
	}
	return b, nil
}

// hexBytes renders bytes the way the API states an address or topic.
func hexBytes(b []byte) string { return "0x" + hex.EncodeToString(b) }
