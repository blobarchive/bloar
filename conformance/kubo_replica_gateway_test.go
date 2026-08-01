package conformance

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ipfs/go-cid"

	"github.com/blobarchive/bloar/archive"
	"github.com/blobarchive/bloar/kubo"
	"github.com/blobarchive/bloar/server"
)

// TestNitroSyncsFromKuboReplicaGateway runs Nitro's complete beacon-client
// conformance suite against a read-only server whose every archive block comes
// through Kubo's local-only RPC. The writer HTTP server is closed first. This
// is the local proof that the replica endpoint is useful on its own rather
// than a proxy which accidentally depends on the publication source.
func TestNitroSyncsFromKuboReplicaGateway(t *testing.T) {
	f := makeFixtures(t)
	served := make(map[string][]byte)
	keys, err := f.stack.store.Blocks().AllKeysChan(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	for key := range keys {
		block, err := f.stack.store.Blocks().Get(t.Context(), key)
		if err != nil {
			t.Fatal(err)
		}
		served[string(key.Hash())] = bytes.Clone(block.RawData())
	}

	var reads atomic.Int64
	kuboRPC := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/api/v0/block/get" {
			http.Error(w, "unexpected Kubo RPC", http.StatusNotFound)
			return
		}
		if got := r.URL.Query().Get("offline"); got != "true" {
			http.Error(w, "network-capable block reads are forbidden", http.StatusTeapot)
			return
		}
		target, err := cid.Decode(r.URL.Query().Get("arg"))
		if err != nil {
			http.Error(w, "invalid CID", http.StatusBadRequest)
			return
		}
		raw, ok := served[string(target.Hash())]
		if !ok {
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"Message": "block was not found locally (offline)",
				"Code":    0,
				"Type":    "error",
			})
			return
		}
		reads.Add(1)
		w.Header().Set("Content-Type", "application/vnd.ipld.raw")
		_, _ = w.Write(raw)
	}))
	defer kuboRPC.Close()

	client, err := kubo.New(kubo.Config{
		BaseURL: kuboRPC.URL, AllowUnauthenticated: true, RequestTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	localRaw, err := kubo.NewLocalBlockstore(client, kubo.BlockstoreConfig{
		Enumeration: kubo.ListLimits{MaxItems: 1, MaxBytes: 1024},
	})
	if err != nil {
		t.Fatal(err)
	}
	local, err := kubo.NewReplicaBlockstore(localRaw)
	if err != nil {
		t.Fatal(err)
	}
	root, ok, err := f.stack.roots.Get(t.Context(), testHead)
	if err != nil || !ok {
		t.Fatalf("writer root = %s present=%t err=%v", root, ok, err)
	}
	loaded, err := archive.Load(t.Context(), archive.Config{Blocks: local}, root)
	if err != nil {
		t.Fatal(err)
	}
	heads, err := server.NewHeads(server.HeadsConfig{
		Net: testNet, Roots: f.stack.roots, Blocks: local,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := heads.Add(loaded); err != nil {
		t.Fatal(err)
	}
	handler, err := server.New(server.Config{
		ReadOnly: true,
		Heads:    heads,
		Blocks:   local,
		Beacon: server.Beacon{
			GenesisTime:           genesisTime,
			SecondsPerSlot:        secondsPerSlot,
			GenesisValidatorsRoot: "0x4b363db94e286120d76eb905340fdd4e54bfe9f06bf33ff6cf5ad27f511bfe95",
			GenesisForkVersion:    "0x00000000",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	gateway := httptest.NewServer(handler)
	defer gateway.Close()

	// The publication source is deliberately gone before Nitro initializes.
	// The source's store remains open only because this fixture's RootStore is
	// the replica's local durable metadata seam; no HTTP request can reach it.
	writerURL := f.stack.url
	f.stack.http.Close()
	if response, err := http.Get(writerURL + "/bloar/v1/heads"); err == nil {
		response.Body.Close()
		t.Fatal("writer HTTP source remained reachable after close")
	}
	suite := f.at(gateway.URL)

	t.Run("initialize", func(t *testing.T) { nitroInitialize(t, suite) })
	t.Run("syncs_blobs", func(t *testing.T) { nitroSyncsBlobs(t, suite) })
	t.Run("derives_slot_from_header", func(t *testing.T) { nitroGetBlobsDerivesSlotFromHeader(t, suite) })
	t.Run("request_order_preserved", func(t *testing.T) { nitroRequestOrderPreserved(t, suite) })
	t.Run("verifies_proofs", func(t *testing.T) { nitroVerifiesProofs(t, suite) })
	t.Run("rejects_absent_blob", func(t *testing.T) { nitroRejectsAbsentBlob(t, suite) })
	t.Run("rejects_uncovered_slot", func(t *testing.T) { nitroRejectsUncoveredSlot(t, suite) })

	if reads.Load() == 0 {
		t.Fatal("Nitro conformance completed without a Kubo-local block read")
	}
}
