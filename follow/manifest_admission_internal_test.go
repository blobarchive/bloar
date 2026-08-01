package follow

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/fluent/qp"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"

	"github.com/blobarchive/bloar/schema"
)

func manifestAdmissionCID(t *testing.T, label string) cid.Cid {
	t.Helper()
	c, err := schema.NodeCID([]byte(label))
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func manifestAdmissionRaw(t *testing.T, head string, prev cid.Cid) []byte {
	t.Helper()
	raw, _, err := schema.EncodeManifest(&schema.Manifest{
		V:    schema.ManifestVersion,
		Head: head,
		Sources: []schema.Source{{
			Type:      schema.SourceInboxEvents,
			Address:   bytes.Repeat([]byte{0x11}, schema.AddressSize),
			Topic:     bytes.Repeat([]byte{0x22}, schema.TopicSize),
			OpenEnded: true,
		}},
		Prev: prev,
	})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func branchingAdmissionRaw(t *testing.T, left, right cid.Cid) []byte {
	t.Helper()
	n, err := qp.BuildMap(basicnode.Prototype.Map, 2, func(ma datamodel.MapAssembler) {
		qp.MapEntry(ma, "left", qp.Link(cidlink.Link{Cid: left}))
		qp.MapEntry(ma, "right", qp.Link(cidlink.Link{Cid: right}))
	})
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := dagcbor.Encode(n, &out); err != nil {
		t.Fatal(err)
	}
	return out.Bytes()
}

func admissionLoader(blocks map[string][]byte) manifestBlockLoader {
	return func(_ context.Context, c cid.Cid) ([]byte, error) {
		return blocks[c.KeyString()], nil
	}
}

func smallAdmissionLimits() manifestAdmissionLimits {
	return manifestAdmissionLimits{
		maxHops:       8,
		maxBlocks:     8,
		maxBlockBytes: 1 << 20,
		maxChainBytes: 2 << 20,
		maxDuration:   time.Second,
	}
}

func TestValidateFirstManifestChainAcceptsOnlyLinearHeadBoundChain(t *testing.T) {
	genesis := manifestAdmissionCID(t, "genesis")
	tip := manifestAdmissionCID(t, "tip")
	blocks := map[string][]byte{
		genesis.KeyString(): manifestAdmissionRaw(t, "all", cid.Undef),
		tip.KeyString():     manifestAdmissionRaw(t, "all", genesis),
	}
	if err := validateFirstManifestChain(t.Context(), "all", tip, smallAdmissionLimits(), admissionLoader(blocks)); err != nil {
		t.Fatalf("valid chain refused: %v", err)
	}

	t.Run("wrong head", func(t *testing.T) {
		bad := manifestAdmissionCID(t, "wrong-head")
		err := validateFirstManifestChain(t.Context(), "all", bad, smallAdmissionLimits(), admissionLoader(map[string][]byte{
			bad.KeyString(): manifestAdmissionRaw(t, "other", cid.Undef),
		}))
		if err == nil || !strings.Contains(err.Error(), `names head "other", want "all"`) {
			t.Fatalf("error = %v, want wrong-head refusal", err)
		}
	})

	t.Run("branching arbitrary DAG", func(t *testing.T) {
		bad := manifestAdmissionCID(t, "branch")
		err := validateFirstManifestChain(t.Context(), "all", bad, smallAdmissionLimits(), admissionLoader(map[string][]byte{
			bad.KeyString(): branchingAdmissionRaw(t, genesis, tip),
		}))
		if err == nil || !strings.Contains(err.Error(), "decoding manifest") {
			t.Fatalf("error = %v, want schema refusal", err)
		}
	})

	t.Run("malformed", func(t *testing.T) {
		bad := manifestAdmissionCID(t, "malformed")
		err := validateFirstManifestChain(t.Context(), "all", bad, smallAdmissionLimits(), admissionLoader(map[string][]byte{
			bad.KeyString(): {0xff},
		}))
		if err == nil || !strings.Contains(err.Error(), "decoding manifest") {
			t.Fatalf("error = %v, want decode refusal", err)
		}
	})
}

func TestValidateFirstManifestChainEnforcesEveryBudget(t *testing.T) {
	t.Run("cycle", func(t *testing.T) {
		a := manifestAdmissionCID(t, "cycle-a")
		b := manifestAdmissionCID(t, "cycle-b")
		err := validateFirstManifestChain(t.Context(), "all", a, smallAdmissionLimits(), admissionLoader(map[string][]byte{
			a.KeyString(): manifestAdmissionRaw(t, "all", b),
			b.KeyString(): manifestAdmissionRaw(t, "all", a),
		}))
		if err == nil || !strings.Contains(err.Error(), "contains a cycle") {
			t.Fatalf("error = %v, want cycle refusal", err)
		}
	})

	t.Run("unique blocks", func(t *testing.T) {
		a := manifestAdmissionCID(t, "unique-a")
		b := manifestAdmissionCID(t, "unique-b")
		c := manifestAdmissionCID(t, "unique-c")
		limits := smallAdmissionLimits()
		limits.maxBlocks = 2
		err := validateFirstManifestChain(t.Context(), "all", a, limits, admissionLoader(map[string][]byte{
			a.KeyString(): manifestAdmissionRaw(t, "all", b),
			b.KeyString(): manifestAdmissionRaw(t, "all", c),
			c.KeyString(): manifestAdmissionRaw(t, "all", cid.Undef),
		}))
		if err == nil || !strings.Contains(err.Error(), "exceeds 2 unique blocks") {
			t.Fatalf("error = %v, want unique-block refusal", err)
		}
	})

	t.Run("hop count", func(t *testing.T) {
		a := manifestAdmissionCID(t, "hops-a")
		b := manifestAdmissionCID(t, "hops-b")
		c := manifestAdmissionCID(t, "hops-c")
		limits := smallAdmissionLimits()
		limits.maxHops = 2
		err := validateFirstManifestChain(t.Context(), "all", a, limits, admissionLoader(map[string][]byte{
			a.KeyString(): manifestAdmissionRaw(t, "all", b),
			b.KeyString(): manifestAdmissionRaw(t, "all", c),
			c.KeyString(): manifestAdmissionRaw(t, "all", cid.Undef),
		}))
		if err == nil || !strings.Contains(err.Error(), "exceeds 2 hops") {
			t.Fatalf("error = %v, want hop refusal", err)
		}
	})

	t.Run("per-block bytes", func(t *testing.T) {
		bad := manifestAdmissionCID(t, "oversized")
		limits := smallAdmissionLimits()
		limits.maxBlockBytes = 32
		err := validateFirstManifestChain(t.Context(), "all", bad, limits, admissionLoader(map[string][]byte{
			bad.KeyString(): bytes.Repeat([]byte{0}, 33),
		}))
		if err == nil || !strings.Contains(err.Error(), "exceeds the 32-byte per-block limit") {
			t.Fatalf("error = %v, want per-block byte refusal", err)
		}
	})

	t.Run("aggregate bytes", func(t *testing.T) {
		genesis := manifestAdmissionCID(t, "aggregate-genesis")
		tip := manifestAdmissionCID(t, "aggregate-tip")
		genesisRaw := manifestAdmissionRaw(t, "all", cid.Undef)
		tipRaw := manifestAdmissionRaw(t, "all", genesis)
		limits := smallAdmissionLimits()
		limits.maxChainBytes = int64(len(genesisRaw) + len(tipRaw) - 1)
		err := validateFirstManifestChain(t.Context(), "all", tip, limits, admissionLoader(map[string][]byte{
			genesis.KeyString(): genesisRaw,
			tip.KeyString():     tipRaw,
		}))
		if err == nil || !strings.Contains(err.Error(), "aggregate limit") {
			t.Fatalf("error = %v, want aggregate-byte refusal", err)
		}
	})

	t.Run("wall time", func(t *testing.T) {
		tip := manifestAdmissionCID(t, "timeout")
		limits := smallAdmissionLimits()
		limits.maxDuration = 10 * time.Millisecond
		err := validateFirstManifestChain(t.Context(), "all", tip, limits,
			func(ctx context.Context, _ cid.Cid) ([]byte, error) {
				<-ctx.Done()
				return nil, ctx.Err()
			})
		if err == nil || !strings.Contains(err.Error(), "deadline exceeded") {
			t.Fatalf("error = %v, want wall-time refusal", err)
		}
	})
}
