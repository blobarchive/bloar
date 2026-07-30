package main

// Acceptance for the safety boundary: the daemon's connection-lifetime bounds,
// proven over REAL TCP connections rather than httptest recorders, because the
// property under test -- net/http closing a slow or stalled body, an idle
// keep-alive, or an over-budget connection -- lives on the socket, not in the
// handler.

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"
)

// TestSlowRejectedBodiesAreClosed drives the three paths that reject a
// request WITHOUT reading its body -- an unauthenticated mutation, a mutation to
// an unknown head, and one whose framing is refused -- each followed by a body
// that never arrives. Before the fix these parked net/http's post-handler body
// drain forever (there was only a ReadHeaderTimeout, and the header phase was
// already over); the finite server-level ReadTimeout now bounds that drain, so
// the server closes each connection on its own.
//
// The declared length is kept under net/http's 256 KiB post-handler read
// tolerance on purpose: above it net/http closes immediately without reading,
// which would pass whether or not ReadTimeout existed. Under it, net/http drains
// via a blocking read, so a body that never comes is closed ONLY because
// ReadTimeout fired -- which is exactly the bound this proves.
func TestSlowRejectedBodiesAreClosed(t *testing.T) {
	const readTimeout = 750 * time.Millisecond
	dir := t.TempDir()
	cfg := serveTestConfig(t, dir, false)
	cfg.Server.ReadTimeout = readTimeout
	stop := startServe(t, cfg)
	defer stop(t)

	cases := []struct {
		name    string
		request string
	}{
		{
			// Auth-rejected: 401 before the body is touched.
			name: "auth-rejected",
			request: "POST /bloar/v1/blobs HTTP/1.1\r\n" +
				"Host: %[1]s\r\nAuthorization: Bearer wrong-token\r\nContent-Length: 200000\r\n\r\n",
		},
		{
			// Unknown-head: 404 from the head check, before the body is read.
			name: "unknown-head",
			request: "POST /bloar/v1/heads/does-not-exist/refs HTTP/1.1\r\n" +
				"Host: %[1]s\r\nAuthorization: Bearer test-token\r\nContent-Length: 200000\r\n\r\n",
		},
		{
			// Framing-rejected: 200000 is authenticated but not a whole number of
			// blobs, so a 400 before any read.
			name: "framing-rejected",
			request: "POST /bloar/v1/blobs HTTP/1.1\r\n" +
				"Host: %[1]s\r\nAuthorization: Bearer test-token\r\nContent-Length: 200000\r\n\r\n",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			conn, err := net.Dial("tcp", cfg.Server.Listen)
			if err != nil {
				t.Fatalf("dial: %v", err)
			}
			defer conn.Close()
			if _, err := io.WriteString(conn, fmt.Sprintf(tc.request, cfg.Server.Listen)); err != nil {
				t.Fatalf("writing request: %v", err)
			}

			// The read deadline is only a backstop against a total hang: the invariant
			// is that the SERVER closes the stalled connection (closed == true), not
			// that it does so within a tight wall-clock a loaded scheduler might miss.
			if err := conn.SetReadDeadline(time.Now().Add(30 * readTimeout)); err != nil {
				t.Fatalf("setting read deadline: %v", err)
			}
			start := time.Now()
			closed, data := drainUntilClose(conn)
			elapsed := time.Since(start)
			if !closed {
				t.Fatalf("server never closed the stalled-body connection: read timed out after %v; got %q", elapsed, data)
			}
			// A generous ceiling, only to prove the close was the ReadTimeout and not
			// the read-deadline backstop: 8s over a 750ms bound is ~10x, which load
			// still honors.
			if elapsed > readTimeout+8*time.Second {
				t.Fatalf("connection closed after %v, past even a generous bound on the %v ReadTimeout", elapsed, readTimeout)
			}
			// The rejection response is written before the drain, so it is normally on
			// the wire; a hard reset can drop it, so this is a soft check, not the
			// invariant (which is the close above).
			if data != "" && !strings.HasPrefix(data, "HTTP/1.1") {
				t.Errorf("expected an HTTP rejection before the close, got %q", data)
			}
			t.Logf("%s: server closed the stalled body after %v (ReadTimeout %v); rejection bytes=%d", tc.name, elapsed, readTimeout, len(data))
		})
	}
}

// TestListenerConnectionBudget proves the LimitListener caps concurrently
// served connections at server.max_conns: with a budget of one, a second
// connection is not accepted until the first closes.
func TestListenerConnectionBudget(t *testing.T) {
	cfg := &Config{}
	cfg.Server.Listen = freeAddr(t)
	cfg.Server.MaxConns = 1
	cfg.Server.ReadHeaderTimeout = 5 * time.Second

	started := make(chan struct{}, 4)
	release := make(chan struct{})
	srv := newHTTPServer(cfg, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started <- struct{}{}
		<-release
		w.WriteHeader(http.StatusOK)
	}))
	ln, err := listen(cfg)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go srv.Serve(ln)
	defer srv.Close()

	// The first connection is accepted and its handler runs, holding the sole slot.
	c1, err := net.Dial("tcp", cfg.Server.Listen)
	if err != nil {
		t.Fatalf("dial c1: %v", err)
	}
	defer c1.Close()
	if _, err := io.WriteString(c1, "GET /x HTTP/1.1\r\nHost: x\r\n\r\n"); err != nil {
		t.Fatalf("write c1: %v", err)
	}
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("first connection was never served")
	}

	// A second connection can complete its TCP handshake but must not be accepted
	// into a handler while the first holds the budget. This window cannot flake to a
	// false failure under load: LimitListener strictly blocks Accept until a slot
	// frees, so the only thing load can do is make the (correct) "not served"
	// outcome even more certain.
	c2, err := net.Dial("tcp", cfg.Server.Listen)
	if err != nil {
		t.Fatalf("dial c2: %v", err)
	}
	defer c2.Close()
	if _, err := io.WriteString(c2, "GET /x HTTP/1.1\r\nHost: x\r\n\r\n"); err != nil {
		t.Fatalf("write c2: %v", err)
	}
	select {
	case <-started:
		t.Fatal("second connection was served despite max_conns=1")
	case <-time.After(500 * time.Millisecond):
	}

	// Freeing the first (finish its handler and close the connection) releases the
	// slot, and the second is served.
	close(release)
	c1.Close()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("second connection was never served after the first freed its slot")
	}
}

// TestIdleKeepAliveIsClosed proves server.idle_timeout closes a kept-alive
// connection that has gone silent between requests.
func TestIdleKeepAliveIsClosed(t *testing.T) {
	const idle = 400 * time.Millisecond
	cfg := &Config{}
	cfg.Server.Listen = freeAddr(t)
	cfg.Server.ReadHeaderTimeout = 5 * time.Second
	cfg.Server.IdleTimeout = idle

	srv := newHTTPServer(cfg, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "2")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "ok")
	}))
	ln, err := listen(cfg)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go srv.Serve(ln)
	defer srv.Close()

	conn, err := net.Dial("tcp", cfg.Server.Listen)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	if _, err := io.WriteString(conn, "GET /x HTTP/1.1\r\nHost: x\r\n\r\n"); err != nil {
		t.Fatalf("write: %v", err)
	}
	// Read the first response, then hold the connection open and idle.
	br := bufio.NewReader(conn)
	resp, err := http.ReadResponse(br, nil)
	if err != nil {
		t.Fatalf("reading first response: %v", err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()

	// The read deadline is only a backstop against a total hang: the invariant is
	// that the SERVER closes the idle connection (drainUntilClose returns true), not
	// that it does so within a tight wall-clock the scheduler might miss under load.
	if err := conn.SetReadDeadline(time.Now().Add(30 * idle)); err != nil {
		t.Fatalf("setting read deadline: %v", err)
	}
	start := time.Now()
	closed, _ := drainUntilClose(conn)
	elapsed := time.Since(start)
	if !closed {
		t.Fatalf("server never closed the idle keep-alive connection (idle_timeout %v)", idle)
	}
	// A generous ceiling, only to prove the close was the idle timeout and not the
	// read-deadline backstop: 8s over a 400ms idle is ~20x, which a loaded scheduler
	// still honors comfortably.
	if elapsed > idle+8*time.Second {
		t.Fatalf("idle connection closed after %v, past even a generous bound on the %v idle_timeout", elapsed, idle)
	}
	t.Logf("server closed the idle keep-alive connection after %v (idle_timeout %v)", elapsed, idle)
}

// drainUntilClose reads from conn until it is closed by the peer (returns true) or
// the read deadline fires (returns false). It returns whatever it read, so a
// caller can check the rejection line the server wrote before closing.
func drainUntilClose(conn net.Conn) (closed bool, data string) {
	var b strings.Builder
	buf := make([]byte, 4096)
	for {
		n, err := conn.Read(buf)
		if n > 0 {
			b.Write(buf[:n])
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				return true, b.String()
			}
			var netErr net.Error
			if errors.As(err, &netErr) && netErr.Timeout() {
				return false, b.String()
			}
			// A reset or other close also counts as the server ending the connection.
			return true, b.String()
		}
	}
}
