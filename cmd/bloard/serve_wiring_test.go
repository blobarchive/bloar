package main

// The daemon's serve wiring -- cfg.Server.WriteTimeout ->
// server.Config.BlobResponseWriteTimeout -- is
// load-bearing, exercised through the WHOLE serve path (serve.go builds the handler
// and the listener). A slow reader of a real blob response is cut off ONLY because
// that assignment carries the short write budget into the handler. Delete the
// assignment and server.New falls back to its 120s default, the slow reader is not
// cut off within the test, and this fails.

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"syscall"
	"testing"
	"time"

	"github.com/blobarchive/bloar/schema"
)

func TestServeWiresBlobWriteTimeout(t *testing.T) {
	const (
		nBlobs       = 24 // ~6.3 MiB of JSON, past the pinned receive buffer
		writeTimeout = 250 * time.Millisecond
		slot         = 8626176 // the "all" head's origin slot
	)
	dir := t.TempDir()
	cfg := serveTestConfig(t, dir, false)
	cfg.Server.WriteTimeout = writeTimeout
	stop := startServe(t, cfg)
	defer stop(t)
	base := "http://" + cfg.Server.Listen

	// Ingest a full slot's worth of blobs and reference them, so the unfiltered read
	// below returns a large body -- all through the daemon's real HTTP endpoints.
	raws := make([][]byte, nBlobs)
	var body bytes.Buffer
	for i := range raws {
		raws[i] = makeBlob(uint64(3000 + i))
		body.Write(raws[i])
	}
	vhs := putBlobs(t, base, body.Bytes())
	postRefs(t, base, slot, vhs)

	// A lower bound on the full JSON body: each blob is at least 2*BlobSize of hex.
	full := nBlobs * 2 * schema.BlobSize

	// A pinned-receive-buffer slow reader, so the response overruns the socket buffer
	// and the server's write blocks -- where the wired deadline fires.
	dialer := &net.Dialer{Control: func(_, _ string, c syscall.RawConn) error {
		_ = c.Control(func(fd uintptr) {
			_ = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_RCVBUF, 2048)
		})
		return nil
	}}
	conn, err := dialer.Dial("tcp", cfg.Server.Listen)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	req := fmt.Sprintf("GET /all/eth/v1/beacon/blobs/%d HTTP/1.1\r\nHost: %s\r\nConnection: close\r\n\r\n", slot, cfg.Server.Listen)
	if _, err := io.WriteString(conn, req); err != nil {
		t.Fatalf("write request: %v", err)
	}

	if err := conn.SetReadDeadline(time.Now().Add(45 * time.Second)); err != nil {
		t.Fatalf("read deadline: %v", err)
	}
	total := 0
	buf := make([]byte, 32<<10)
	for {
		n, rerr := conn.Read(buf)
		total += n
		if rerr != nil || total >= full {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if total >= full/2 {
		t.Fatalf("slow reader received %d bytes of a >=%d-byte body; the serve write timeout is not wired into the handler", total, full)
	}
	t.Logf("slow reader received only %d bytes of a >=%d-byte body before the wired %v write timeout closed it", total, full, writeTimeout)
}

// putBlobs posts the concatenated blob bytes to the daemon and returns the
// versioned hashes it assigned, in order.
func putBlobs(t *testing.T, base string, body []byte) []string {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, base+"/bloar/v1/blobs", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("building put: %v", err)
	}
	req.Header.Set("Authorization", "Bearer test-token")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("put blobs: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(resp.Body)
		t.Fatalf("put blobs -> %d: %s", resp.StatusCode, msg)
	}
	var out struct {
		Blobs []struct {
			VersionedHash string `json:"versioned_hash"`
		} `json:"blobs"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decoding put response: %v", err)
	}
	vhs := make([]string, len(out.Blobs))
	for i, b := range out.Blobs {
		vhs[i] = b.VersionedHash
	}
	return vhs
}

// postRefs references the versioned hashes at slot on the "all" head.
func postRefs(t *testing.T, base string, slot uint64, vhs []string) {
	t.Helper()
	payload, _ := json.Marshal(map[string]any{
		"rows":      []map[string]any{{"slot": slot, "versioned_hashes": vhs}},
		"synced_to": slot + 1,
	})
	req, err := http.NewRequest(http.MethodPost, base+"/bloar/v1/heads/all/refs", bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("building refs: %v", err)
	}
	req.Header.Set("Authorization", "Bearer test-token")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("post refs: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(resp.Body)
		t.Fatalf("post refs -> %d: %s", resp.StatusCode, msg)
	}
}
