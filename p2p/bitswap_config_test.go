package p2p

import (
	"math"
	"strings"
	"testing"

	"github.com/blobarchive/bloar/schema"
)

// TestExchangeDefaultsAreBloarOwned pins values that deliberately matched
// Boxo v0.41 when adopted. NewExchange passes every one explicitly, so a Boxo
// update cannot silently change the daemon's effective limits. Updating one of
// these values is therefore a reviewed Bloar policy change, not dependency
// drift.
func TestExchangeDefaultsAreBloarOwned(t *testing.T) {
	if DefaultBitswapMaxQueuedWantlistEntriesPerPeer != 1024 {
		t.Errorf("queued wants default = %d, want 1024", DefaultBitswapMaxQueuedWantlistEntriesPerPeer)
	}
	if DefaultBitswapMaxOutstandingBytesPerPeer != 1<<20 {
		t.Errorf("outstanding bytes default = %d, want 1 MiB", DefaultBitswapMaxOutstandingBytesPerPeer)
	}
	if DefaultBitswapTaskWorkerCount != 8 {
		t.Errorf("send worker default = %d, want 8", DefaultBitswapTaskWorkerCount)
	}
	if DefaultBitswapEngineTaskWorkerCount != 8 {
		t.Errorf("engine task worker default = %d, want 8", DefaultBitswapEngineTaskWorkerCount)
	}
	if DefaultBitswapEngineBlockstoreWorkerCount != 128 {
		t.Errorf("blockstore worker default = %d, want 128", DefaultBitswapEngineBlockstoreWorkerCount)
	}
	if DefaultBitswapMaxCIDSize != 168 {
		t.Errorf("maximum CID size default = %d, want 168", DefaultBitswapMaxCIDSize)
	}

	got, err := (ExchangeConfig{}).settings()
	if err != nil {
		t.Fatalf("zero-value settings: %v", err)
	}
	if got.maxQueuedWantlistEntriesPerPeer != 1024 ||
		got.maxOutstandingBytesPerPeer != 1<<20 ||
		got.taskWorkerCount != 8 ||
		got.engineTaskWorkerCount != 8 ||
		got.engineBlockstoreWorkerCount != 128 ||
		got.maxCIDSize != 168 {
		t.Fatalf("zero-value settings did not resolve to pinned defaults: %+v", got)
	}
}

func TestExchangeSettingsAcceptOverrides(t *testing.T) {
	got, err := (ExchangeConfig{
		MaxQueuedWantlistEntriesPerPeer: 17,
		MaxOutstandingBytesPerPeer:      18,
		TaskWorkerCount:                 19,
		EngineTaskWorkerCount:           20,
		EngineBlockstoreWorkerCount:     21,
		MaxCIDSize:                      42,
	}).settings()
	if err != nil {
		t.Fatalf("settings: %v", err)
	}
	if got.maxQueuedWantlistEntriesPerPeer != 17 ||
		got.maxOutstandingBytesPerPeer != 18 ||
		got.taskWorkerCount != 19 ||
		got.engineTaskWorkerCount != 20 ||
		got.engineBlockstoreWorkerCount != 21 ||
		got.maxCIDSize != 42 {
		t.Fatalf("settings did not preserve overrides: %+v", got)
	}
}

func TestExchangeMaxCIDSizeAdmitsBloarWireCIDs(t *testing.T) {
	blobCID, err := schema.BlobCID(make([]byte, schema.BlobSize))
	if err != nil {
		t.Fatalf("building blob CID: %v", err)
	}
	nodeCID, err := schema.NodeCID([]byte{0xa0}) // valid empty dag-cbor map
	if err != nil {
		t.Fatalf("building node CID: %v", err)
	}
	for name, size := range map[string]int{
		"blob": blobCID.ByteLen(),
		"node": nodeCID.ByteLen(),
	} {
		if size != int(MinimumBitswapMaxCIDSize) {
			t.Fatalf("%s CID length = %d, pinned minimum = %d; review the wire-format admission limit",
				name, size, MinimumBitswapMaxCIDSize)
		}
	}

	if _, err := (ExchangeConfig{MaxCIDSize: MinimumBitswapMaxCIDSize}).settings(); err != nil {
		t.Fatalf("minimum CID size was rejected: %v", err)
	}
	_, err = (ExchangeConfig{MaxCIDSize: MinimumBitswapMaxCIDSize - 1}).settings()
	if err == nil {
		t.Fatal("CID size below Bloar's wire CID length was accepted")
	}
	if !strings.Contains(err.Error(), "at least 36 bytes") {
		t.Fatalf("error = %q, want the protocol minimum", err)
	}
}

func TestExchangeSettingsKeepIndependentCapsIndependent(t *testing.T) {
	// These intentionally cross in both directions. Queue entries, response
	// bytes, and the three worker pools describe different units and pipeline
	// stages; ordering any pair would reject a safe tuning without tightening
	// an admission bound. MaxCIDSize has only the protocol minimum asserted in
	// TestExchangeMaxCIDSizeAdmitsBloarWireCIDs.
	for name, cfg := range map[string]ExchangeConfig{
		"tiny queue with large pools": {
			MaxQueuedWantlistEntriesPerPeer: 1,
			MaxOutstandingBytesPerPeer:      1,
			TaskWorkerCount:                 64,
			EngineTaskWorkerCount:           32,
			EngineBlockstoreWorkerCount:     16,
			MaxCIDSize:                      MinimumBitswapMaxCIDSize,
		},
		"large queue with inverted pools": {
			MaxQueuedWantlistEntriesPerPeer: 64,
			MaxOutstandingBytesPerPeer:      1,
			TaskWorkerCount:                 1,
			EngineTaskWorkerCount:           16,
			EngineBlockstoreWorkerCount:     2,
			MaxCIDSize:                      DefaultBitswapMaxCIDSize,
		},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := cfg.settings(); err != nil {
				t.Fatalf("independent limits were given an arbitrary ordering: %v", err)
			}
		})
	}
}

func TestBitswapCheckedConversionsRejectOverflow(t *testing.T) {
	// Exercise the production conversion branch explicitly even on amd64,
	// where no positive int64 can overflow int or uint. These are the ceilings
	// the same code receives on a 32-bit target.
	for _, tt := range []struct {
		name   string
		value  int64
		max    uint64
		target string
	}{
		{name: "int", value: int64(math.MaxInt32) + 1, max: math.MaxInt32, target: "int"},
		{name: "uint", value: int64(math.MaxUint32) + 1, max: math.MaxUint32, target: "uint"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := bitswapValue("Limit", tt.value, 1, tt.max, tt.target)
			if err == nil {
				t.Fatal("conversion accepted a value above the target width")
			}
			if !strings.Contains(err.Error(), "overflows "+tt.target) {
				t.Fatalf("error = %q, want explicit %s overflow", err, tt.target)
			}
		})
	}
}

func TestExchangeSettingsRejectNegativeValues(t *testing.T) {
	tests := []struct {
		name string
		cfg  ExchangeConfig
		want string
	}{
		{"queued wants", ExchangeConfig{MaxQueuedWantlistEntriesPerPeer: -1}, "MaxQueuedWantlistEntriesPerPeer"},
		{"outstanding bytes", ExchangeConfig{MaxOutstandingBytesPerPeer: -1}, "MaxOutstandingBytesPerPeer"},
		{"send workers", ExchangeConfig{TaskWorkerCount: -1}, "TaskWorkerCount"},
		{"engine task workers", ExchangeConfig{EngineTaskWorkerCount: -1}, "EngineTaskWorkerCount"},
		{"blockstore workers", ExchangeConfig{EngineBlockstoreWorkerCount: -1}, "EngineBlockstoreWorkerCount"},
		{"CID size", ExchangeConfig{MaxCIDSize: -1}, "MaxCIDSize"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := tt.cfg.settings()
			if err == nil {
				t.Fatal("settings accepted a negative value")
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("error = %q, want field name %q", err, tt.want)
			}
		})
	}
}
