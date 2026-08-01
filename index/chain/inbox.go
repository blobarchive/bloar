package chain

import (
	"github.com/ethereum/go-ethereum/common"
)

// SequencerBatchDeliveredSig is the canonical signature of the event the
// SequencerInbox emits for every batch it accepts, in the exact form topic0 is
// the keccak256 of: argument types only, tuples expanded in parentheses, no
// names, no spaces.
//
// The Solidity it comes from (nitro's contracts/src/bridge/ISequencerInbox.sol):
//
//	event SequencerBatchDelivered(
//	    uint256 indexed batchSequenceNumber,
//	    bytes32 indexed beforeAcc,
//	    bytes32 indexed afterAcc,
//	    bytes32 delayedAcc,
//	    uint256 afterDelayedMessagesRead,
//	    IBridge.TimeBounds timeBounds,
//	    IBridge.BatchDataLocation dataLocation
//	);
//
// TimeBounds is a struct of four uint64s and BatchDataLocation is an enum,
// hence the (uint64,uint64,uint64,uint64) and the trailing uint8. The three
// indexed arguments are topics 1..3 and are not part of this string.
const SequencerBatchDeliveredSig = "SequencerBatchDelivered(uint256,bytes32,bytes32,bytes32,uint256,(uint64,uint64,uint64,uint64),uint8)"

// SequencerBatchDeliveredTopic is topic0 of that event.
//
// It is pinned as a literal rather than derived from an ABI at init, because
// deriving it would mean carrying an ABI JSON -- and the only honest source for
// that JSON is nitro, which this module must not import (nitro pins a
// go-ethereum fork through a replace directive, which is why conformance/ is a
// separate module). A constant plus a test that recomputes it from the
// signature string above is the same guarantee with none of the dependency:
// TestSequencerBatchDeliveredTopic keccaks the string and asserts this value,
// so the two cannot drift.
//
// Verified against nitro's generated bindings, which carry the same literal:
// solgen/go/bridgegen/bridgegen.go, in the SequencerInboxSequencerBatchDelivered
// filterer. Nitro itself derives it at init from the bundled ABI
// (arbnode/sequencer_inbox.go: sequencerBridgeABI.Events[...].ID).
var SequencerBatchDeliveredTopic = common.HexToHash("0x7394f4a19a13c7b92b5bb71033245305946ef78452f7b4986ac1390b5df4ebd7")

// dataLocation is the event's last argument: where the batch's data actually
// went. It is not decoded -- this indexer reads the transaction rather than the
// event body, and a type-3 transaction's blob hashes are the authority on what
// it posted -- but the values are worth naming, because they are the whole
// taxonomy of what a batch can be and only one of them produces rows.
//
//	0 TxInput             calldata batch
//	1 SeparateBatchEvent  AnyTrust: data in an event, not here
//	2 NoData              delayed-message-only batch
//	3 Blob                blob batch: the one this indexer records
//
// Spec 10.2: "Non-blob batches (calldata, AnyTrust) produce no rows; coverage
// still advances." The transaction type is what decides that here, for the
// reason given at skipping time in scan.
