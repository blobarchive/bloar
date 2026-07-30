package replica

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	"github.com/ipld/go-ipld-prime/node/basicnode"
)

func TestGenerationBlockIsCanonicalAndCoversEveryHead(t *testing.T) {
	a := testCID("root-a")
	b := testCID("root-b")
	m := testCID("manifest-b")
	when := time.Unix(1_750_000_000, 999).UTC()

	left := Generation{ReplicaID: "archive-1", UpdatedAt: when, Heads: []Head{
		{Name: "zeta", Root: b, Manifest: m, SyncedTo: 20},
		{Name: "alpha", Root: a, SyncedTo: 10},
	}}
	right := Generation{ReplicaID: "archive-1", UpdatedAt: when.Truncate(time.Second), Heads: []Head{
		{Name: "alpha", Root: a, SyncedTo: 10},
		{Name: "zeta", Root: b, Manifest: m, SyncedTo: 20},
	}}
	lb, err := left.Block()
	if err != nil {
		t.Fatal(err)
	}
	rb, err := right.Block()
	if err != nil {
		t.Fatal(err)
	}
	if !lb.Cid().Equals(rb.Cid()) || !bytes.Equal(lb.RawData(), rb.RawData()) {
		t.Fatalf("canonical blocks differ: %s != %s", lb.Cid(), rb.Cid())
	}
	if !left.Equal(right) {
		t.Fatal("semantic equality rejected canonical equivalents")
	}

	builder := basicnode.Prototype.Any.NewBuilder()
	if err := (dagcbor.DecodeOptions{AllowLinks: true}).Decode(builder, bytes.NewReader(lb.RawData())); err != nil {
		t.Fatal(err)
	}
	node := builder.Build()
	heads, err := node.LookupByString("heads")
	if err != nil {
		t.Fatal(err)
	}
	if length := heads.Length(); length != 2 {
		t.Fatalf("head count = %d, want 2", length)
	}
	seen := map[string]cid.Cid{}
	iter := heads.ListIterator()
	for !iter.Done() {
		_, item, err := iter.Next()
		if err != nil {
			t.Fatal(err)
		}
		nameNode, _ := item.LookupByString("name")
		rootNode, _ := item.LookupByString("root")
		name, _ := nameNode.AsString()
		link, err := rootNode.AsLink()
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := cid.Cast([]byte(link.Binary()))
		if err != nil {
			t.Fatal(err)
		}
		seen[name] = decoded
	}
	if !seen["alpha"].Equals(a) || !seen["zeta"].Equals(b) {
		t.Fatalf("anchor roots = %#v", seen)
	}
}

func TestGenerationValidationAndIdentity(t *testing.T) {
	base := Generation{
		ReplicaID: "replica_a", UpdatedAt: time.Unix(1, 0),
		Heads: []Head{{Name: "head", Root: testCID("root"), SyncedTo: 1}},
	}
	block, err := base.Block()
	if err != nil {
		t.Fatal(err)
	}
	changed := base
	changed.UpdatedAt = time.Unix(2, 0)
	other, err := changed.Block()
	if err != nil {
		t.Fatal(err)
	}
	if block.Cid().Equals(other.Cid()) {
		t.Fatal("updated_at did not change the authority anchor")
	}

	for name, mutate := range map[string]func(*Generation){
		"bad replica":        func(g *Generation) { g.ReplicaID = "Upper Case" },
		"zero time":          func(g *Generation) { g.UpdatedAt = time.Time{} },
		"undefined root":     func(g *Generation) { g.Heads[0].Root = cid.Undef },
		"duplicate head":     func(g *Generation) { g.Heads = append(g.Heads, g.Heads[0]) },
		"oversize synced_to": func(g *Generation) { g.Heads[0].SyncedTo = uint64(^uint64(0)) },
		"too many heads": func(g *Generation) {
			g.Heads = make([]Head, maxGenerationHeads+1)
			for i := range g.Heads {
				g.Heads[i] = Head{Name: fmt.Sprintf("head-%d", i), Root: testCID("root")}
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			candidate := base
			candidate.Heads = append([]Head(nil), base.Heads...)
			mutate(&candidate)
			if _, err := candidate.Block(); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}

func TestEmptyGenerationBlockIsCanonical(t *testing.T) {
	when := time.Unix(1_750_000_000, 999).UTC()
	nilHeads := Generation{ReplicaID: "archive-1", UpdatedAt: when}
	emptyHeads := Generation{ReplicaID: "archive-1", UpdatedAt: when.Truncate(time.Second), Heads: []Head{}}

	left, err := nilHeads.Block()
	if err != nil {
		t.Fatal(err)
	}
	right, err := emptyHeads.Block()
	if err != nil {
		t.Fatal(err)
	}
	if !left.Cid().Equals(right.Cid()) || !bytes.Equal(left.RawData(), right.RawData()) {
		t.Fatalf("canonical empty blocks differ: %s != %s", left.Cid(), right.Cid())
	}
	if !nilHeads.Equal(emptyHeads) {
		t.Fatal("nil and non-nil empty head sets are not semantically equal")
	}
	normalized, err := nilHeads.Normalize()
	if err != nil {
		t.Fatal(err)
	}
	if normalized.Heads == nil || len(normalized.Heads) != 0 {
		t.Fatalf("normalized heads = %#v, want canonical non-nil empty slice", normalized.Heads)
	}

	builder := basicnode.Prototype.Any.NewBuilder()
	if err := (dagcbor.DecodeOptions{AllowLinks: true}).Decode(builder, bytes.NewReader(left.RawData())); err != nil {
		t.Fatal(err)
	}
	heads, err := builder.Build().LookupByString("heads")
	if err != nil {
		t.Fatal(err)
	}
	if heads.Length() != 0 {
		t.Fatalf("encoded empty head count = %d", heads.Length())
	}

	newer := nilHeads
	newer.UpdatedAt = when.Add(time.Second)
	newerBlock, err := newer.Block()
	if err != nil {
		t.Fatal(err)
	}
	if left.Cid().Equals(newerBlock.Cid()) {
		t.Fatal("separate empty publications collapsed to the same authority anchor")
	}
}

func testCID(body string) cid.Cid { return blocks.NewBlock([]byte(body)).Cid() }
