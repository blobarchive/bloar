package server_test

// The read-deadline ordering of beginBoundedMutation's callers (refs, truncate,
// manifest) is pinned with a
// ResponseController deadline SPY. Only the PUT endpoint's inline path was pinned (by
// the TCP test, kept); these three share the helper, and nothing proved that a
// declared-oversize body earns NO extension, or that an accepted body's extension
// lands BEFORE the first body read. The spy makes both observable directly.

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// auditMaxRefsBody mirrors the server's unexported maxRefsBody (server/bloar.go): the
// byte ceiling a refs/truncate/manifest body is declared against. Hard-coded here
// because the test is black-box; a change to the server's constant that this no
// longer matches would show up as an unexpected accept/reject in these cases.
const auditMaxRefsBody = 16 << 20

// deadlineSpyWriter is a ResponseWriter that counts http.ResponseController's
// SetReadDeadline calls. It implements SetReadDeadline so
// http.NewResponseController(w).SetReadDeadline reaches it -- the seam the mutation
// read-deadline extension travels through.
type deadlineSpyWriter struct {
	header        http.Header
	status        int
	readDeadlines int
}

func (w *deadlineSpyWriter) Header() http.Header {
	if w.header == nil {
		w.header = make(http.Header)
	}
	return w.header
}
func (w *deadlineSpyWriter) Write(p []byte) (int, error) { return len(p), nil }
func (w *deadlineSpyWriter) WriteHeader(code int) {
	if w.status == 0 {
		w.status = code
	}
}
func (w *deadlineSpyWriter) SetReadDeadline(time.Time) error {
	w.readDeadlines++
	return nil
}

// recordingBody is a request body that snapshots, on its FIRST Read, how many read
// deadlines had been set by then -- the signal that the extension came before the
// body was read.
type recordingBody struct {
	data                 []byte
	off                  int
	firstRead            bool
	deadlinesAtFirstRead int
	deadlinesNow         func() int
}

func (b *recordingBody) Read(p []byte) (int, error) {
	if !b.firstRead {
		b.firstRead = true
		if b.deadlinesNow != nil {
			b.deadlinesAtFirstRead = b.deadlinesNow()
		}
	}
	if b.off >= len(b.data) {
		return 0, io.EOF
	}
	n := copy(p, b.data[b.off:])
	b.off += n
	return n, nil
}
func (b *recordingBody) Close() error { return nil }

func TestBoundedMutationDeadlineOrdering(t *testing.T) {
	st := newStack(t, stackOpts{})
	// The three JSON mutation handlers that share beginBoundedMutation. PUT is pinned
	// separately by the TCP test; these are the helper's other callers.
	callers := []struct {
		name string
		path string
	}{
		{"refs", "/bloar/v1/heads/" + testHead + "/refs"},
		{"truncate", "/bloar/v1/heads/" + testHead + "/truncate"},
		{"manifest", "/bloar/v1/heads/" + testHead + "/manifest"},
	}
	for _, c := range callers {
		t.Run(c.name+" declared-oversize earns no extension", func(t *testing.T) {
			spy := &deadlineSpyWriter{}
			body := &recordingBody{data: []byte("{}"), deadlinesNow: func() int { return spy.readDeadlines }}
			req := httptest.NewRequest(http.MethodPost, c.path, body)
			req.Header.Set("Authorization", "Bearer "+testToken)
			req.ContentLength = auditMaxRefsBody + 1 // declared over the ceiling

			st.handler.ServeHTTP(spy, req)

			if spy.status != http.StatusBadRequest {
				t.Fatalf("declared-oversize %s status = %d, want 400", c.name, spy.status)
			}
			if spy.readDeadlines != 0 {
				t.Errorf("a declared-oversize %s earned %d read-deadline extension(s); it must stay under the base ReadTimeout",
					c.name, spy.readDeadlines)
			}
			if body.firstRead {
				t.Errorf("a declared-oversize %s body was read; it must be rejected before any read", c.name)
			}
		})
		t.Run(c.name+" accepted body extends the deadline before the first read", func(t *testing.T) {
			spy := &deadlineSpyWriter{}
			body := &recordingBody{data: []byte("{}"), deadlinesNow: func() int { return spy.readDeadlines }}
			req := httptest.NewRequest(http.MethodPost, c.path, body)
			req.Header.Set("Authorization", "Bearer "+testToken)
			req.ContentLength = auditMaxRefsBody // exactly the accepted boundary

			st.handler.ServeHTTP(spy, req)

			if !body.firstRead {
				t.Fatalf("accepted %s body was never read; cannot prove the deadline ordering", c.name)
			}
			if body.deadlinesAtFirstRead < 1 {
				t.Errorf("accepted %s read its body before extending the read deadline (%d set at first read); a slow honest "+
					"upload would be cut off by the base bound", c.name, body.deadlinesAtFirstRead)
			}
		})
	}
}
