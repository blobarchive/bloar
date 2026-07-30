package kubo_test

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"

	"github.com/blobarchive/bloar/kubo"
)

func testBlockstores(t *testing.T, baseURL string, cfg kubo.BlockstoreConfig) (*kubo.LocalBlockstore, *kubo.FetchingBlockstore) {
	t.Helper()
	client := newClient(t, baseURL, nil)
	local, err := kubo.NewLocalBlockstore(client, cfg)
	if err != nil {
		t.Fatalf("NewLocalBlockstore: %v", err)
	}
	fetching, err := kubo.NewFetchingBlockstore(local)
	if err != nil {
		t.Fatalf("NewFetchingBlockstore: %v", err)
	}
	return local, fetching
}

func testBlockstoreConfig() kubo.BlockstoreConfig {
	return kubo.BlockstoreConfig{
		Enumeration:      kubo.ListLimits{MaxItems: 128, MaxBytes: 64 << 10},
		MaxPutManyBlocks: 8,
	}
}

func writeMissingBlock(t *testing.T, w http.ResponseWriter) {
	t.Helper()
	w.WriteHeader(http.StatusInternalServerError)
	writeJSON(t, w, map[string]any{
		"Message": "block was not found locally (offline)",
		"Code":    0,
		"Type":    "error",
	})
}

func TestKuboBlockstoresSeparateLocalAndFetch(t *testing.T) {
	target := testBlock(t, cid.Raw, "fetched through Kubo's network block service")
	var present atomic.Bool
	var localReads atomic.Int64
	var networkReads atomic.Int64
	var puts atomic.Int64

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer "+testToken {
			t.Errorf("Authorization = %q", got)
		}
		switch r.URL.Path {
		case "/api/v0/block/get":
			if got := r.URL.Query().Get("arg"); got != target.Cid().String() {
				t.Errorf("arg = %q, want %q", got, target.Cid())
			}
			switch offline := r.URL.Query().Get("offline"); offline {
			case "true":
				localReads.Add(1)
				if !present.Load() {
					writeMissingBlock(t, w)
					return
				}
			case "false":
				networkReads.Add(1)
			default:
				t.Errorf("block/get offline = %q, want explicit true or false", offline)
			}
			w.Header().Set("Content-Type", "application/vnd.ipld.raw")
			_, _ = w.Write(target.RawData())
		case "/api/v0/block/put":
			puts.Add(1)
			if err := r.ParseMultipartForm(4 << 20); err != nil {
				t.Errorf("ParseMultipartForm: %v", err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			file, _, err := r.FormFile("file")
			if err != nil {
				t.Errorf("FormFile: %v", err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			defer file.Close()
			raw, err := io.ReadAll(file)
			if err != nil {
				t.Errorf("ReadAll(block): %v", err)
			}
			if !bytes.Equal(raw, target.RawData()) {
				t.Errorf("cached bytes = %q, want %q", raw, target.RawData())
			}
			present.Store(true)
			writeJSON(t, w, map[string]any{"Key": target.Cid().String(), "Size": len(raw)})
		default:
			t.Errorf("unexpected endpoint %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	local, fetching := testBlockstores(t, server.URL, testBlockstoreConfig())

	_, err := local.Get(t.Context(), target.Cid())
	if !errors.Is(err, kubo.ErrLocalBlockAbsent) {
		t.Fatalf("local Get error = %v, want ErrLocalBlockAbsent", err)
	}
	if !ipld.IsNotFound(err) {
		t.Fatalf("local Get error = %v, want conventional ipld not-found", err)
	}
	if errors.Is(err, kubo.ErrBlockFetchFailed) {
		t.Fatalf("local Get error = %v, unexpectedly classified as a fetch failure", err)
	}
	var absent *kubo.LocalBlockAbsentError
	if !errors.As(err, &absent) || !absent.CID.Equals(target.Cid()) {
		t.Fatalf("local Get error = %#v, want LocalBlockAbsentError for %s", err, target.Cid())
	}
	if got := networkReads.Load(); got != 0 {
		t.Fatalf("local Get issued %d network-capable reads, want 0", got)
	}

	got, err := fetching.Get(t.Context(), target.Cid())
	if err != nil {
		t.Fatalf("fetching Get: %v", err)
	}
	if !got.Cid().Equals(target.Cid()) || !bytes.Equal(got.RawData(), target.RawData()) {
		t.Fatalf("fetching Get returned (%s, %q), want (%s, %q)", got.Cid(), got.RawData(), target.Cid(), target.RawData())
	}
	if got := networkReads.Load(); got != 1 {
		t.Fatalf("network-capable reads = %d, want 1", got)
	}
	if got := puts.Load(); got != 1 {
		t.Fatalf("explicit cache writes = %d, want 1", got)
	}

	// The fetched block is now an independently provable local hit. This does
	// not rely on block/get's configurable write-through behavior: the adapter
	// issued block/put before returning the first result.
	got, err = local.Get(t.Context(), target.Cid())
	if err != nil {
		t.Fatalf("local Get after fetch: %v", err)
	}
	if !bytes.Equal(got.RawData(), target.RawData()) {
		t.Fatalf("local bytes after fetch = %q, want %q", got.RawData(), target.RawData())
	}
	if got := networkReads.Load(); got != 1 {
		t.Fatalf("local verification issued another network read; total = %d", got)
	}
}

func TestFetchingBlockstoreFailureIsNotLocalAbsence(t *testing.T) {
	target := testBlock(t, cid.Raw, "unavailable from every peer")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v0/block/get" {
			t.Errorf("unexpected endpoint %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if r.URL.Query().Get("offline") == "true" {
			writeMissingBlock(t, w)
			return
		}
		w.WriteHeader(http.StatusBadGateway)
		writeJSON(t, w, map[string]any{
			"Message": "no provider returned the requested block",
			"Code":    0,
			"Type":    "error",
		})
	}))
	defer server.Close()

	_, fetching := testBlockstores(t, server.URL, testBlockstoreConfig())
	_, err := fetching.Get(t.Context(), target.Cid())
	if !errors.Is(err, kubo.ErrBlockFetchFailed) {
		t.Fatalf("fetching Get error = %v, want ErrBlockFetchFailed", err)
	}
	if errors.Is(err, kubo.ErrLocalBlockAbsent) || ipld.IsNotFound(err) {
		t.Fatalf("fetch failure = %v, must not be classified as local absence", err)
	}
	var fetch *kubo.BlockFetchError
	if !errors.As(err, &fetch) || fetch.Operation != "fetch" || !fetch.CID.Equals(target.Cid()) {
		t.Fatalf("fetching Get error = %#v, want fetch-stage BlockFetchError", err)
	}
}

func TestLocalBlockstoreDoesNotTreatCommand404AsAbsence(t *testing.T) {
	target := testBlock(t, cid.Raw, "local endpoint outage")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "command not found", http.StatusNotFound)
	}))
	defer server.Close()

	local, _ := testBlockstores(t, server.URL, testBlockstoreConfig())
	_, err := local.Get(t.Context(), target.Cid())
	var status *kubo.StatusError
	if !errors.As(err, &status) || status.Status != http.StatusNotFound {
		t.Fatalf("error = %T %v, want HTTP 404 StatusError", err, err)
	}
	if errors.Is(err, kubo.ErrLocalBlockAbsent) || ipld.IsNotFound(err) {
		t.Fatalf("command endpoint 404 classified as local absence: %v", err)
	}
}

func TestFetchingBlockstoreDoesNotHealCorruptLocalBlock(t *testing.T) {
	target := testBlock(t, cid.Raw, "honest local contents")
	var onlineCalls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v0/block/get" {
			t.Errorf("unexpected endpoint %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/vnd.ipld.raw")
		if r.URL.Query().Get("offline") == "false" {
			onlineCalls.Add(1)
			_, _ = w.Write(target.RawData())
			return
		}
		_, _ = w.Write([]byte("corrupt bytes under the requested CID"))
	}))
	defer server.Close()

	local, fetching := testBlockstores(t, server.URL, testBlockstoreConfig())
	for name, read := range map[string]func() error{
		"Get": func() error {
			_, err := fetching.Get(t.Context(), target.Cid())
			return err
		},
		"Has": func() error {
			_, err := local.Has(t.Context(), target.Cid())
			return err
		},
		"GetSize": func() error {
			_, err := local.GetSize(t.Context(), target.Cid())
			return err
		},
	} {
		t.Run(name, func(t *testing.T) {
			err := read()
			var protocol *kubo.ProtocolError
			if !errors.As(err, &protocol) {
				t.Fatalf("error = %v, want ProtocolError", err)
			}
			if errors.Is(err, kubo.ErrLocalBlockAbsent) || errors.Is(err, kubo.ErrBlockFetchFailed) {
				t.Fatalf("corruption error = %v, must not be reclassified", err)
			}
		})
	}
	if got := onlineCalls.Load(); got != 0 {
		t.Fatalf("corrupt local block triggered %d network reads, want 0", got)
	}
}

func TestFetchingBlockstoreRejectsCorruptFetchedBlock(t *testing.T) {
	target := testBlock(t, cid.Raw, "honest fetched contents")
	var puts atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v0/block/get":
			if r.URL.Query().Get("offline") == "true" {
				writeMissingBlock(t, w)
				return
			}
			w.Header().Set("Content-Type", "application/vnd.ipld.raw")
			_, _ = w.Write([]byte("dishonest peer response"))
		case "/api/v0/block/put":
			puts.Add(1)
			w.WriteHeader(http.StatusInternalServerError)
		default:
			t.Errorf("unexpected endpoint %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	_, fetching := testBlockstores(t, server.URL, testBlockstoreConfig())
	_, err := fetching.Get(t.Context(), target.Cid())
	if !errors.Is(err, kubo.ErrBlockFetchFailed) {
		t.Fatalf("fetching Get error = %v, want ErrBlockFetchFailed", err)
	}
	var protocol *kubo.ProtocolError
	if !errors.As(err, &protocol) {
		t.Fatalf("fetching Get error = %v, want wrapped ProtocolError", err)
	}
	if got := puts.Load(); got != 0 {
		t.Fatalf("corrupt fetch issued %d cache writes, want 0", got)
	}
}

func TestFetchingBlockstoreReportsDurabilityFailure(t *testing.T) {
	target := testBlock(t, cid.Raw, "network result that cannot be cached")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v0/block/get":
			if r.URL.Query().Get("offline") == "true" {
				writeMissingBlock(t, w)
				return
			}
			w.Header().Set("Content-Type", "application/vnd.ipld.raw")
			_, _ = w.Write(target.RawData())
		case "/api/v0/block/put":
			w.WriteHeader(http.StatusInsufficientStorage)
			writeJSON(t, w, map[string]any{
				"Message": "managed Kubo repo has no free space",
				"Code":    0,
				"Type":    "error",
			})
		default:
			t.Errorf("unexpected endpoint %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	_, fetching := testBlockstores(t, server.URL, testBlockstoreConfig())
	got, err := fetching.Get(t.Context(), target.Cid())
	if got != nil {
		t.Fatalf("fetching Get returned a non-durable block %s", got.Cid())
	}
	if !errors.Is(err, kubo.ErrBlockFetchFailed) {
		t.Fatalf("fetching Get error = %v, want ErrBlockFetchFailed", err)
	}
	var fetch *kubo.BlockFetchError
	if !errors.As(err, &fetch) || fetch.Operation != "cache" {
		t.Fatalf("fetching Get error = %#v, want cache-stage BlockFetchError", err)
	}
	var status *kubo.StatusError
	if !errors.As(err, &status) || status.Status != http.StatusInsufficientStorage {
		t.Fatalf("fetching Get error = %#v, want wrapped HTTP 507 StatusError", err)
	}
}

func TestLocalBlockstoreBudgets(t *testing.T) {
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()
	client := newClient(t, server.URL, nil)
	valid := testBlockstoreConfig()

	for name, cfg := range map[string]kubo.BlockstoreConfig{
		"missing enumeration": {},
		"negative batch": {
			Enumeration:      valid.Enumeration,
			MaxPutManyBlocks: -1,
		},
		"oversized batch": {
			Enumeration:      valid.Enumeration,
			MaxPutManyBlocks: kubo.DefaultMaxPutManyBlocks + 1,
		},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := kubo.NewLocalBlockstore(client, cfg); err == nil {
				t.Fatal("NewLocalBlockstore succeeded, want budget error")
			}
		})
	}
	if _, err := kubo.NewLocalBlockstore(nil, valid); err == nil {
		t.Fatal("NewLocalBlockstore(nil) succeeded")
	}
	if _, err := kubo.NewFetchingBlockstore(nil); err == nil {
		t.Fatal("NewFetchingBlockstore(nil) succeeded")
	}

	local, err := kubo.NewLocalBlockstore(client, kubo.BlockstoreConfig{
		Enumeration:      valid.Enumeration,
		MaxPutManyBlocks: 1,
	})
	if err != nil {
		t.Fatalf("NewLocalBlockstore: %v", err)
	}
	first := testBlock(t, cid.Raw, "first")
	second := testBlock(t, cid.Raw, "second")
	if err := local.PutMany(t.Context(), []blocks.Block{first, second}); err == nil {
		t.Fatal("PutMany over configured block limit succeeded")
	}
	if got := calls.Load(); got != 0 {
		t.Fatalf("over-limit PutMany issued %d RPCs, want 0", got)
	}
}
