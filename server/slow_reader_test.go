package server_test

// Acceptance for the safety boundary's write-side bound: a public blobs response
// carries a write deadline (BlobResponseWriteTimeout), so a slow reader cannot hold
// the handler, and the admission reservation it took, open
// indefinitely.
//
// # Why it is written the way it is (load robustness)
//
// The bound fires when the server's write BLOCKS on a full socket buffer for longer
// than the deadline. Two things make that observable without flaking under a loaded
// box:
//
//   - The client caps its own receive buffer (SO_RCVBUF) to a small, FIXED size.
//     Left to the kernel, that buffer autotunes -- and grows under load -- until it
//     can swallow the whole response, at which point the server never blocks and the
//     deadline never fires. Pinning it small keeps the amount the server can push
//     before blocking small and independent of load. (An earlier version without
//     this pin passed in isolation and failed under concurrent test load, exactly
//     this way.)
//   - The client reads SLOWLY and counts bytes until the server ends the connection,
//     rather than sleeping a fixed time then reading. Reading slowly keeps the server
//     blocked so its deadline fires; stopping on the close is event-based, so nothing
//     here is tuned to a wall-clock the scheduler might not honor. A fast reader would
//     drain the buffer and let the whole body through.
//
// The invariant asserted is "the reader got far less than the full body", with a
// generous margin -- not an exact count.

import (
	"io"
	"net"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/blobarchive/bloar/schema"
)

func TestSlowBlobReaderIsCutOffByWriteDeadline(t *testing.T) {
	// With metrics enabled, instrumentRead wraps the live writer in a statusRecorder
	//: the write deadline must still reach the
	// connection through that wrapper (statusRecorder.Unwrap). The shipped writer
	// example turns metrics on, so this runs both ways -- and the metrics-on case is
	// the one that reproduced the full-body transfer before Unwrap was added.
	for _, metricsOn := range []bool{false, true} {
		name := "metrics-off"
		if metricsOn {
			name = "metrics-on"
		}
		t.Run(name, func(t *testing.T) { runSlowReaderRegression(t, metricsOn) })
	}
}

func runSlowReaderRegression(t *testing.T, metricsOn bool) {
	const (
		// The body (~6.3 MiB of hex) sits between two reference points: it is far
		// larger than the pinned receive buffer below (so with the bound the server
		// blocks and truncates at ~buffer, well under full/2), and small enough that
		// WITHOUT the bound the slow reader drains the whole thing inside the backstop
		// (so n reaches full, well over full/2). full/2 lands cleanly between the two.
		blobs        = 24
		writeTimeout = 200 * time.Millisecond
	)
	s := newStack(t, stackOpts{writeTimeout: writeTimeout, serverMetrics: metricsOn})

	// Ingest a full slot and reference it, so the unfiltered read below returns a
	// large body the write bound has to move.
	raws := make([][]byte, blobs)
	for i := range raws {
		raws[i] = makeBlob(uint64(1000 + i))
	}
	vhs := s.put(raws...)
	s.refs([]map[string]any{row(testOrigin, vhs...)}, testOrigin+1)

	// A lower bound on the full JSON body: each blob is at least 2*BlobSize of hex.
	// The real body is larger (quotes, commas, the envelope), so a read that stays
	// under half of this is unambiguously a truncated response.
	full := blobs * 2 * schema.BlobSize

	addr := strings.TrimPrefix(s.url, "http://")
	// Pin the receive buffer small so the server blocks -- and hits its write
	// deadline -- after pushing only a fixed, load-independent amount. Best-effort:
	// a kernel that clamps or ignores this still truncates a body this much larger
	// than any default buffer, just later.
	dialer := &net.Dialer{Control: func(_, _ string, c syscall.RawConn) error {
		_ = c.Control(func(fd uintptr) {
			_ = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_RCVBUF, 2048)
		})
		return nil
	}}
	conn, err := dialer.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	req := "GET /" + testHead + "/eth/v1/beacon/blobs/" + itoa(testOrigin) + " HTTP/1.1\r\n" +
		"Host: " + addr + "\r\nConnection: close\r\n\r\n"
	if _, err := io.WriteString(conn, req); err != nil {
		t.Fatalf("writing request: %v", err)
	}

	// Read steadily, pacing with a small sleep so the server stays blocked on the
	// full buffer long enough for its write deadline to fire there. Count everything
	// (headers included -- a negligible ~150 bytes) until the server ends the
	// connection, the whole body arrives, or -- only as a backstop against a total
	// hang -- the read deadline. The backstop is comfortably longer than the time to
	// drain the whole body at this pace, so WITHOUT the bound the loop reaches full,
	// not the deadline.
	if err := conn.SetReadDeadline(time.Now().Add(45 * time.Second)); err != nil {
		t.Fatalf("setting read deadline: %v", err)
	}
	total := 0
	buf := make([]byte, 32<<10)
	for {
		n, err := conn.Read(buf)
		total += n
		if err != nil {
			break
		}
		if total >= full {
			// The whole body came through (the bound is absent or ineffective); stop
			// so the failing assertion below reports promptly rather than reading on.
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	// The invariant is "far less than the full body", not an exact count: the server
	// closed at its write deadline long before the ~full-byte body was sent.
	if total >= full/2 {
		t.Fatalf("slow reader received %d bytes of a >=%d-byte body; the write deadline did not cut it off", total, full)
	}
	t.Logf("slow reader received only %d bytes of a >=%d-byte body before the %v write deadline closed it", total, full, writeTimeout)
}
