package chain_test

import (
	"testing"

	"github.com/ethereum/go-ethereum/crypto"

	"github.com/blobarchive/bloar/index/chain"
)

// TestSequencerBatchDeliveredTopic is the reason the topic may be a pinned
// literal at all.
//
// The constant exists because deriving it at init would mean carrying an ABI,
// and the only honest source for that ABI is nitro -- which this module must
// not import (its replace directive pins a go-ethereum fork; see
// cmd/bloar-index's package comment). This test buys back what the constant
// gives up: topic0 is keccak256 of the signature string, so recomputing it from
// the string that documents it proves the pair is self-consistent, and no edit
// to one can quietly disagree with the other.
//
// What this cannot prove is that the signature string is the one nitro's
// contract actually emits. That is checked against the source, out of band:
// nitro's contracts/src/bridge/ISequencerInbox.sol declares the event, and its
// generated bindings in solgen/go/bridgegen/bridgegen.go carry this same
// literal hash. Both were read at the time this was written.
func TestSequencerBatchDeliveredTopic(t *testing.T) {
	got := crypto.Keccak256Hash([]byte(chain.SequencerBatchDeliveredSig))
	if got != chain.SequencerBatchDeliveredTopic {
		t.Errorf("keccak256(%q)\n = %s\nwant %s", chain.SequencerBatchDeliveredSig, got, chain.SequencerBatchDeliveredTopic)
	}
}

// TestSequencerBatchDeliveredSigShape guards the signature string's form rather
// than its content: topic0 is the keccak of these exact bytes, so a stray space
// or an argument name would produce a hash that is wrong in a way no amount of
// reading catches.
func TestSequencerBatchDeliveredSigShape(t *testing.T) {
	const want = "SequencerBatchDelivered(uint256,bytes32,bytes32,bytes32,uint256,(uint64,uint64,uint64,uint64),uint8)"
	if chain.SequencerBatchDeliveredSig != want {
		t.Errorf("signature = %q, want %q", chain.SequencerBatchDeliveredSig, want)
	}
}
