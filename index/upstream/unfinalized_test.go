package upstream_test

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/blobarchive/bloar/index/upstream"
)

func TestRootAddressedUnfinalizedReads(t *testing.T) {
	root := [32]byte{31: 0x12}
	parent := [32]byte{31: 0x11}
	finalized := [32]byte{31: 0x10}
	finalizedParent := [32]byte{31: 0x0f}
	commitment := [48]byte{47: 0x99}

	header := func(w http.ResponseWriter, slot uint64, r, p [32]byte, fin bool) {
		fmt.Fprintf(w, `{"execution_optimistic":false,"finalized":%t,"data":{"root":"0x%x","canonical":true,"header":{"message":{"slot":"%d","parent_root":"0x%x"}}}}`,
			fin, r, slot, p)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/eth/v1/beacon/headers/head", func(w http.ResponseWriter, _ *http.Request) {
		header(w, 18, root, parent, false)
	})
	mux.HandleFunc("/eth/v1/beacon/headers/0x"+hex.EncodeToString(root[:]), func(w http.ResponseWriter, _ *http.Request) {
		header(w, 18, root, parent, false)
	})
	mux.HandleFunc("/eth/v1/beacon/headers/finalized", func(w http.ResponseWriter, _ *http.Request) {
		header(w, 16, finalized, finalizedParent, true)
	})
	mux.HandleFunc("/eth/v1/beacon/blinded_blocks/0x"+hex.EncodeToString(root[:]), func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprintf(w, `{"execution_optimistic":false,"finalized":false,"data":{"message":{"body":{"blob_kzg_commitments":["0x%x"]}}}}`, commitment)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	b, err := upstream.NewBlockClient(upstream.Config{BaseURL: srv.URL, MaxAttempts: 1, Backoff: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	head, ok, err := b.Head(context.Background())
	if err != nil || !ok {
		t.Fatalf("Head = (%+v, %t, %v)", head, ok, err)
	}
	if head.Slot != 18 || head.Root != root || head.ParentRoot != parent || head.Finalized {
		t.Fatalf("Head = %+v", head)
	}
	byRoot, err := b.HeaderByRoot(context.Background(), root)
	if err != nil || byRoot != head {
		t.Fatalf("HeaderByRoot = (%+v, %v), want %+v", byRoot, err, head)
	}
	commits, err := b.CommitmentsByRoot(context.Background(), root)
	if err != nil || len(commits) != 1 || commits[0] != commitment {
		t.Fatalf("CommitmentsByRoot = (%x, %v)", commits, err)
	}
	fin, ok, err := b.FinalizedHeader(context.Background())
	if err != nil || !ok || fin.Slot != 16 || fin.Root != finalized || fin.ParentRoot != finalizedParent || !fin.Finalized {
		t.Fatalf("FinalizedHeader = (%+v, %t, %v)", fin, ok, err)
	}
}

func TestUnfinalizedReadsFailClosedOnUnsafeFlags(t *testing.T) {
	root := [32]byte{31: 1}
	rootHex := "0x" + hex.EncodeToString(root[:])
	tests := []struct {
		name string
		body string
		want string
	}{
		{"optimistic", `{"execution_optimistic":true,"finalized":false,"data":{"root":"` + rootHex + `","canonical":true,"header":{"message":{"slot":"2","parent_root":"` + rootHex + `"}}}}`, "execution_optimistic:true"},
		{"orphan", `{"execution_optimistic":false,"finalized":false,"data":{"root":"` + rootHex + `","canonical":false,"header":{"message":{"slot":"2","parent_root":"` + rootHex + `"}}}}`, "canonical:false"},
		{"missing finalized flag", `{"execution_optimistic":false,"data":{"root":"` + rootHex + `","canonical":true,"header":{"message":{"slot":"2","parent_root":"` + rootHex + `"}}}}`, "omits finalized"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_, _ = w.Write([]byte(tt.body))
			}))
			defer srv.Close()
			b, err := upstream.NewBlockClient(upstream.Config{BaseURL: srv.URL, MaxAttempts: 1, Backoff: time.Millisecond})
			if err != nil {
				t.Fatal(err)
			}
			_, _, err = b.Head(context.Background())
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Head error = %v, want %q", err, tt.want)
			}
			var optimistic *upstream.ExecutionOptimisticError
			if got := errors.As(err, &optimistic); got != (tt.name == "optimistic") {
				t.Fatalf("ExecutionOptimisticError = %t, want %t (error %T %v)",
					got, tt.name == "optimistic", err, err)
			}
		})
	}
}
