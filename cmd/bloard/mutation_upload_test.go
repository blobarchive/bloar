package main

// The valid-mutation read-deadline extension is load-bearing, and a
// declared-oversize mutation does not earn it.
//
// Both run the whole daemon over a REAL TCP connection with a short base
// ReadTimeout and a longer mutation-body extension, then upload slowly. The refs
// endpoint is used because it has no KZG cost, so the daemon answers promptly even
// under heavy parallel test load -- the load-bearing signal is the socket behavior,
// not the response latency.

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/blobarchive/bloar/schema"
)

func TestSlowAuthenticatedUploadIsExtended(t *testing.T) {
	const (
		baseRead = 1 * time.Second
		// Generous, so a dribble that stretches under a saturated scheduler still
		// finishes inside it: the point is that a VALID upload is not cut off, and the
		// exact wall-clock is not load-stable, so the extension is sized well past it.
		bodyExt    = 60 * time.Second
		uploadOver = 3 * time.Second // between baseRead and bodyExt
	)
	dir := t.TempDir()
	cfg := serveTestConfig(t, dir, false)
	cfg.Server.ReadTimeout = baseRead
	cfg.Server.MutationBodyTimeout = bodyExt
	stop := startServe(t, cfg)
	defer stop(t)
	addr := cfg.Server.Listen

	// A valid refs body padded with whitespace (which encoding/json ignores) so it is
	// large enough to dribble slowly.
	body := []byte(`{"synced_to":10,"rows":[]` + strings.Repeat(" ", 200000) + `}`)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	header := fmt.Sprintf("POST /bloar/v1/heads/all/refs HTTP/1.1\r\nHost: %s\r\n"+
		"Authorization: Bearer test-token\r\nContent-Length: %d\r\n\r\n", addr, len(body))
	if _, err := io.WriteString(conn, header); err != nil {
		t.Fatalf("writing headers: %v", err)
	}

	// The load-bearing assertion: with the extension the read side stays open for the
	// whole slow upload and every write succeeds; WITHOUT it the base ReadTimeout
	// closes the read side ~baseRead in and the writes start failing (broken pipe).
	// This turns on socket behavior, not on how fast the daemon then responds, so it
	// holds under heavy load.
	if writeErr := dribble(conn, body, uploadOver, 25); writeErr != nil {
		t.Fatalf("the slow upload was cut off mid-body (%v); a valid mutation's body read was not extended past the "+
			"%v base ReadTimeout", writeErr, baseRead)
	}

	// A response at all confirms the daemon consumed the whole body and answered
	// (refs has no KZG cost, so this is prompt). The status is beside the point --
	// the extension's effect is already proven by the completed upload.
	if err := conn.SetReadDeadline(time.Now().Add(30 * time.Second)); err != nil {
		t.Fatalf("read deadline: %v", err)
	}
	resp, err := http.ReadResponse(bufio.NewReader(conn), &http.Request{Method: http.MethodPost})
	if err != nil {
		t.Fatalf("no response to the accepted upload: %v", err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	t.Logf("a %v refs upload under a %v base read timeout completed (HTTP %d) via the %v body extension",
		uploadOver, baseRead, resp.StatusCode, bodyExt)
}

func TestDeclaredOversizeMutationGetsNoExtension(t *testing.T) {
	const (
		baseRead = 400 * time.Millisecond
		bodyExt  = 8 * time.Second
	)
	dir := t.TempDir()
	cfg := serveTestConfig(t, dir, false)
	cfg.Server.ReadTimeout = baseRead
	cfg.Server.MutationBodyTimeout = bodyExt
	// One-blob put cap, so the endpoint ceiling (128 KiB) is BELOW net/http's 256 KiB
	// post-handler drain threshold: a request declaring just over it is drained (not
	// closed outright), and the deadline that governs that drain -- base vs extension
	// -- is then observable as the close time. A 16 MiB refs declaration cannot show
	// this, because net/http closes it without draining.
	cfg.Server.MaxPutBlobs = 1
	stop := startServe(t, cfg)
	defer stop(t)
	addr := cfg.Server.Listen

	// A blob PUT declaring one byte over the one-blob ceiling, authenticated, sending
	// NO body. The declared-length framing check must 400 it BEFORE beginMutationBody,
	// so no extension is installed and net/http drains the (never-arriving) body under
	// the SHORT base timeout -- the connection closes at ~baseRead. If beginMutationBody
	// ran before that check, the extension would be installed and the drain would wait
	// ~bodyExt instead.
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	header := fmt.Sprintf("POST /bloar/v1/blobs HTTP/1.1\r\nHost: %s\r\n"+
		"Authorization: Bearer test-token\r\nContent-Length: %d\r\n\r\n", addr, schema.BlobSize+1)
	if _, err := io.WriteString(conn, header); err != nil {
		t.Fatalf("writing headers: %v", err)
	}

	if err := conn.SetReadDeadline(time.Now().Add(30 * time.Second)); err != nil {
		t.Fatalf("read deadline: %v", err)
	}
	start := time.Now()
	// The 400 is written before the drain, so it arrives at once regardless.
	resp, err := http.ReadResponse(bufio.NewReader(conn), &http.Request{Method: http.MethodPost})
	if err != nil {
		t.Fatalf("reading the declared-oversize response: %v", err)
	}
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("declared-oversize put -> %d, want 400", resp.StatusCode)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()

	// The load-bearing part: the server closes the stalled drain under the SHORT base
	// timeout, proving no long extension was installed. Correct ordering -> close at
	// ~baseRead; the reorder mutation -> close at ~bodyExt.
	closed, _ := drainUntilClose(conn)
	elapsed := time.Since(start)
	if !closed {
		t.Fatalf("server never closed the declared-oversize connection: read timed out after %v", elapsed)
	}
	if elapsed > bodyExt-3*time.Second {
		t.Fatalf("declared-oversize connection closed after %v, near the %v extension; it earned the extension it should "+
			"not have (the deadline check ran after beginMutationBody)", elapsed, bodyExt)
	}
	t.Logf("declared-oversize put was 400'd and drained-closed in %v, far under the %v extension: no extension earned", elapsed, bodyExt)
}

// dribble writes body over roughly d, in n chunks, pausing between them. It returns
// the first write error (a closed peer) rather than failing, so a caller can treat
// a mid-stream cut-off as data rather than a fatal test error.
func dribble(conn net.Conn, body []byte, d time.Duration, n int) error {
	chunk := len(body) / n
	if chunk == 0 {
		chunk = 1
	}
	pause := d / time.Duration(n)
	for off := 0; off < len(body); off += chunk {
		end := off + chunk
		if end > len(body) {
			end = len(body)
		}
		if _, err := conn.Write(body[off:end]); err != nil {
			return err
		}
		time.Sleep(pause)
	}
	return nil
}
