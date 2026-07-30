package server

import (
	"errors"
	"net/http"
	"time"
)

// Per-request deadline refinement on top of the daemon's server-level bounds
//. cmd/bloard sets a finite ReadTimeout that bounds header plus
// body wall-clock for EVERY request -- including the auth-rejected, unknown-head,
// and framing-rejected paths, where net/http's own drain-and-close of an unread
// body becomes time-bounded at the server rather than able to park a handler
// forever. These two helpers refine that base bound per endpoint, using
// http.ResponseController to move the deadline on the live connection:
//
//   - a valid mutation extends its READ deadline before the first body read, so a
//     legitimate multi-megabyte upload is not cut off by the short base bound
//     while a stalled one still is;
//   - a public blob response sets a WRITE deadline before it writes, so a slow
//     reader cannot hold the handler and its admission reservation (finding
//     the safety boundary) open indefinitely.
//
// Both tolerate http.ErrNotSupported: a ResponseWriter with no underlying
// deadline-settable connection -- httptest.ResponseRecorder in the unit tests --
// returns it, and on such a writer there is simply no deadline to set. Any other
// error is the connection reporting a real problem and is returned to be logged;
// it is never fatal to the request, which proceeds under the base bound.

// extendReadDeadline pushes the connection's read deadline out to now+d, sized to
// let a valid mutation finish uploading its bounded body. It must be called
// before the first read of that body.
func extendReadDeadline(w http.ResponseWriter, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	if err := http.NewResponseController(w).SetReadDeadline(time.Now().Add(d)); err != nil && !errors.Is(err, http.ErrNotSupported) {
		return err
	}
	return nil
}

// setWriteDeadline caps a response's total write time at now+d, so a slow reader
// of a large public body cannot retain the handler. It must be called before the
// first write.
func setWriteDeadline(w http.ResponseWriter, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	if err := http.NewResponseController(w).SetWriteDeadline(time.Now().Add(d)); err != nil && !errors.Is(err, http.ErrNotSupported) {
		return err
	}
	return nil
}
