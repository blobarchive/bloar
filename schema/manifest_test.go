package schema

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime/codec"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/fluent/qp"
)

func decodeManifestErr(block []byte) error { _, err := DecodeManifest(block); return err }

// manifestWith builds a manifest block over the given source assemblers, so a
// test can hand-roll a source a correct encoder never would. prev is the assembler
// for the "prev" field (qp.Null for genesis).
func manifestWith(t *testing.T, prev qp.Assemble, sources ...qp.Assemble) []byte {
	t.Helper()
	return buildBlock(t, codec.MapSortMode_RFC7049, func(ma datamodel.MapAssembler) {
		qp.MapEntry(ma, "v", qp.Int(ManifestVersion))
		qp.MapEntry(ma, "head", qp.String("arbitrum-one"))
		qp.MapEntry(ma, "sources", qp.List(int64(len(sources)), func(la datamodel.ListAssembler) {
			for _, s := range sources {
				qp.ListEntry(la, s)
			}
		}))
		qp.MapEntry(ma, "prev", prev)
	})
}

// inboxSource is a valid inbox-events source assembler; overrides replaces a key,
// omits it (a nil value), or adds one a correct source would not carry.
func inboxSource(overrides map[string]qp.Assemble) qp.Assemble {
	base := map[string]qp.Assemble{
		"type":       qp.String(SourceInboxEvents),
		"address":    qp.Bytes(bytes.Repeat([]byte{0x1c}, AddressSize)),
		"topic":      qp.Bytes(bytes.Repeat([]byte{0x73}, TopicSize)),
		"from_block": qp.Int(0),
	}
	return sourceMap(base, overrides)
}

// blobTxsSource is a valid blob-txs source assembler, overridable like inboxSource.
func blobTxsSource(overrides map[string]qp.Assemble) qp.Assemble {
	base := map[string]qp.Assemble{
		"type":    qp.String(SourceBlobTxs),
		"address": qp.Bytes(bytes.Repeat([]byte{0x50}, AddressSize)),
		"senders": qp.List(1, func(la datamodel.ListAssembler) {
			qp.ListEntry(la, qp.Bytes(bytes.Repeat([]byte{0xa4}, AddressSize)))
		}),
		"from_block": qp.Int(21_000_001),
	}
	return sourceMap(base, overrides)
}

func sourceMap(base, overrides map[string]qp.Assemble) qp.Assemble {
	for k, v := range overrides {
		if v == nil {
			delete(base, k)
			continue
		}
		base[k] = v
	}
	return qp.Map(int64(len(base)), func(ma datamodel.MapAssembler) {
		// The sort mode fixes wire order; encode order here is irrelevant.
		for k, v := range base {
			qp.MapEntry(ma, k, v)
		}
	})
}

// TestManifestRoundTrip checks that the OpenEnded/bounded distinction survives a
// round trip in both directions: an open-ended source omits until_block and
// decodes back to OpenEnded, a bounded one carries it and decodes back to a value.
func TestManifestRoundTrip(t *testing.T) {
	m := &Manifest{
		V:    ManifestVersion,
		Head: "arbitrum-one",
		Sources: []Source{
			{Type: SourceInboxEvents, Address: bytes.Repeat([]byte{0x1c}, AddressSize),
				Topic: bytes.Repeat([]byte{0x73}, TopicSize), FromBlock: 0, UntilBlock: 100},
			{Type: SourceBlobTxs, Address: bytes.Repeat([]byte{0x50}, AddressSize),
				Senders: [][]byte{bytes.Repeat([]byte{0xa4}, AddressSize)}, FromBlock: 101, OpenEnded: true},
		},
	}
	block, c, err := EncodeManifest(m)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	got, err := DecodeManifest(block)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Sources[0].OpenEnded || got.Sources[0].UntilBlock != 100 {
		t.Errorf("bounded source decoded to OpenEnded=%t UntilBlock=%d, want false/100",
			got.Sources[0].OpenEnded, got.Sources[0].UntilBlock)
	}
	if !got.Sources[1].OpenEnded {
		t.Errorf("open-ended source decoded to OpenEnded=false")
	}
	// The bounded source must have put an until_block on the wire.
	if !bytes.Contains(block, []byte("until_block")) {
		t.Fatalf("bounded source should have written until_block")
	}

	reblock, rec, err := EncodeManifest(got)
	if err != nil {
		t.Fatalf("re-encode: %v", err)
	}
	if !bytes.Equal(block, reblock) || rec != c {
		t.Errorf("round trip changed the bytes or CID")
	}
}

// TestManifestRejectsUntilBlockNull is the load-bearing one-encoding-per-meaning
// check of spec 10.5: an open-ended source omits until_block, and a decoder MUST
// reject an explicit null there. Tested from both directions -- an encoder never
// writes the null, and the decoder refuses it if one is handed over.
func TestManifestRejectsUntilBlockNull(t *testing.T) {
	block := manifestWith(t, qp.Null(), inboxSource(map[string]qp.Assemble{"until_block": qp.Null()}))
	err := decodeManifestErr(block)
	if err == nil || !strings.Contains(err.Error(), "until_block") {
		t.Fatalf("until_block: null must be rejected, got %v", err)
	}

	// The other direction: an open-ended source encodes with no until_block key at
	// all, so the null spelling is unreachable through the encoder.
	block, _, err = EncodeManifest(&Manifest{
		V: ManifestVersion, Head: "arbitrum-one",
		Sources: []Source{{Type: SourceInboxEvents, Address: bytes.Repeat([]byte{0x1c}, AddressSize),
			Topic: bytes.Repeat([]byte{0x73}, TopicSize), FromBlock: 0, OpenEnded: true}},
	})
	if err != nil {
		t.Fatalf("encode open-ended: %v", err)
	}
	if bytes.Contains(block, []byte("until_block")) {
		t.Errorf("open-ended source wrote an until_block key")
	}
}

func TestManifestRejectsCrossTypeKeys(t *testing.T) {
	cases := []struct {
		name   string
		source qp.Assemble
		want   string
	}{
		{
			name: "inbox-events with senders",
			source: inboxSource(map[string]qp.Assemble{"senders": qp.List(1, func(la datamodel.ListAssembler) {
				qp.ListEntry(la, qp.Bytes(bytes.Repeat([]byte{0xa4}, AddressSize)))
			})}),
			want: "does not apply",
		},
		{
			name:   "blob-txs with topic",
			source: blobTxsSource(map[string]qp.Assemble{"topic": qp.Bytes(bytes.Repeat([]byte{0x73}, TopicSize))}),
			want:   "does not apply",
		},
		{
			name:   "inbox-events missing topic",
			source: inboxSource(map[string]qp.Assemble{"topic": nil}),
			want:   "missing field",
		},
		{
			name:   "blob-txs missing senders",
			source: blobTxsSource(map[string]qp.Assemble{"senders": nil}),
			want:   "missing field",
		},
		{
			name:   "unknown field",
			source: inboxSource(map[string]qp.Assemble{"extra": qp.Int(1)}),
			want:   "unknown field",
		},
		{
			name:   "unknown type",
			source: sourceMap(map[string]qp.Assemble{"type": qp.String("calldata"), "address": qp.Bytes(bytes.Repeat([]byte{0x1c}, AddressSize)), "from_block": qp.Int(0)}, nil),
			want:   "unknown type",
		},
		{
			name:   "empty senders allowlist",
			source: blobTxsSource(map[string]qp.Assemble{"senders": qp.List(0, func(datamodel.ListAssembler) {})}),
			want:   "empty sender allowlist",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := decodeManifestErr(manifestWith(t, qp.Null(), tc.source))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("want error containing %q, got %v", tc.want, err)
			}
		})
	}
}

func TestManifestRejectsUnknownVersion(t *testing.T) {
	block := buildBlock(t, codec.MapSortMode_RFC7049, func(ma datamodel.MapAssembler) {
		qp.MapEntry(ma, "v", qp.Int(ManifestVersion+1))
		qp.MapEntry(ma, "head", qp.String("arbitrum-one"))
		qp.MapEntry(ma, "sources", qp.List(1, func(la datamodel.ListAssembler) {
			qp.ListEntry(la, inboxSource(nil))
		}))
		qp.MapEntry(ma, "prev", qp.Null())
	})
	err := decodeManifestErr(block)
	var unknown *UnknownVersionError
	if !errors.As(err, &unknown) {
		t.Fatalf("want *UnknownVersionError, got %v", err)
	}
	if unknown.Object != "manifest" || unknown.Got != ManifestVersion+1 {
		t.Errorf("UnknownVersionError = %+v, want manifest/%d", unknown, ManifestVersion+1)
	}
}

func TestManifestRejectsUnknownTopLevelField(t *testing.T) {
	block := buildBlock(t, codec.MapSortMode_RFC7049, func(ma datamodel.MapAssembler) {
		qp.MapEntry(ma, "v", qp.Int(ManifestVersion))
		qp.MapEntry(ma, "head", qp.String("arbitrum-one"))
		qp.MapEntry(ma, "sources", qp.List(1, func(la datamodel.ListAssembler) {
			qp.ListEntry(la, inboxSource(nil))
		}))
		qp.MapEntry(ma, "prev", qp.Null())
		qp.MapEntry(ma, "extra", qp.Int(1))
	})
	if err := decodeManifestErr(block); err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("want unknown field error, got %v", err)
	}
}

// TestEncodeManifestRejectsInvalid is the encode-side half of the same rules: a
// Manifest that would not decode does not encode either.
func TestEncodeManifestRejectsInvalid(t *testing.T) {
	cases := []struct {
		name string
		m    *Manifest
		want string
	}{
		{
			name: "no sources",
			m:    &Manifest{V: ManifestVersion, Head: "arbitrum-one"},
			want: "no sources",
		},
		{
			name: "blob-txs empty senders",
			m: &Manifest{V: ManifestVersion, Head: "arbitrum-one", Sources: []Source{
				{Type: SourceBlobTxs, Address: bytes.Repeat([]byte{0x50}, AddressSize), FromBlock: 1, OpenEnded: true}}},
			want: "empty sender allowlist",
		},
		{
			name: "bad head name",
			m: &Manifest{V: ManifestVersion, Head: "Arbitrum_One", Sources: []Source{
				{Type: SourceInboxEvents, Address: bytes.Repeat([]byte{0x1c}, AddressSize),
					Topic: bytes.Repeat([]byte{0x73}, TopicSize), OpenEnded: true}}},
			want: "does not match",
		},
		{
			name: "unknown version",
			m: &Manifest{V: ManifestVersion + 1, Head: "arbitrum-one", Sources: []Source{
				{Type: SourceInboxEvents, Address: bytes.Repeat([]byte{0x1c}, AddressSize),
					Topic: bytes.Repeat([]byte{0x73}, TopicSize), OpenEnded: true}}},
			want: "unknown version",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, _, err := EncodeManifest(tc.m); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("want error containing %q, got %v", tc.want, err)
			}
		})
	}
}

// TestManifestCIDDeterminismAndHistoryBinding proves both sides of the manifest
// identity contract. Independently allocated copies of the same genesis and
// successor encode to the same CID, while the same current successor statement
// chained to a different predecessor history does not.
func TestManifestCIDDeterminismAndHistoryBinding(t *testing.T) {
	genesis := func(address byte) *Manifest {
		return &Manifest{V: ManifestVersion, Head: "arbitrum-one", Sources: []Source{
			{Type: SourceInboxEvents, Address: bytes.Repeat([]byte{address}, AddressSize),
				Topic: bytes.Repeat([]byte{0x73}, TopicSize), OpenEnded: true},
		}}
	}
	successor := func(prev cid.Cid) *Manifest {
		return &Manifest{V: ManifestVersion, Head: "arbitrum-one", Prev: prev, Sources: []Source{
			{Type: SourceInboxEvents, Address: bytes.Repeat([]byte{0x50}, AddressSize),
				Topic: bytes.Repeat([]byte{0x91}, TopicSize), FromBlock: 21_000_001, OpenEnded: true},
		}}
	}
	encode := func(what string, m *Manifest) ([]byte, cid.Cid) {
		t.Helper()
		block, c, err := EncodeManifest(m)
		if err != nil {
			t.Fatalf("encode %s: %v", what, err)
		}
		return block, c
	}

	// These are separate object graphs, including fresh address/topic byte
	// slices. Their equality therefore comes from canonical encoding, not object
	// reuse or a round trip through one encoded block.
	genesisABlock, genesisACID := encode("genesis A", genesis(0x1c))
	genesisBBlock, genesisBCID := encode("genesis B", genesis(0x1c))
	if genesisACID != genesisBCID || !bytes.Equal(genesisABlock, genesisBBlock) {
		t.Fatalf("independent identical genesis manifests differ: A=%s B=%s", genesisACID, genesisBCID)
	}

	successorABlock, successorACID := encode("successor A", successor(genesisACID))
	successorBBlock, successorBCID := encode("successor B", successor(genesisBCID))
	if successorACID != successorBCID || !bytes.Equal(successorABlock, successorBBlock) {
		t.Fatalf("independent identical successor manifests differ: A=%s B=%s", successorACID, successorBCID)
	}

	// The current successor statement is byte-for-byte the same except for Prev.
	// A different valid genesis must therefore make the successor CID differ,
	// binding the CID to its complete predecessor history rather than only its
	// current source schedule.
	_, otherGenesisCID := encode("other genesis", genesis(0x2c))
	if otherGenesisCID == genesisACID {
		t.Fatal("different genesis histories unexpectedly have the same CID")
	}
	_, otherHistoryCID := encode("successor on other history", successor(otherGenesisCID))
	if otherHistoryCID == successorACID {
		t.Fatalf("successor CID %s ignored its different predecessor history", successorACID)
	}

	decodedSuccessor, err := DecodeManifest(successorABlock)
	if err != nil {
		t.Fatalf("decode successor: %v", err)
	}
	if decodedSuccessor.Prev != genesisACID {
		t.Errorf("successor Prev = %s, want %s", decodedSuccessor.Prev, genesisACID)
	}
	decodedGenesis, err := DecodeManifest(genesisABlock)
	if err != nil {
		t.Fatalf("decode genesis: %v", err)
	}
	if decodedGenesis.Prev != cid.Undef {
		t.Errorf("genesis Prev = %s, want undefined", decodedGenesis.Prev)
	}
}
