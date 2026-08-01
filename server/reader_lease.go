package server

import (
	"net/http"
	"sync"
)

// gcReadLease keeps the next GC cut behind one HTTP read's root snapshot until
// every block needed to answer it has been materialized. Publication
// transitions take the same Gate read side, so they remain concurrent: a
// mutable generation may be replaced while an older request is walking it, but
// the exclusive T0 cut cannot make that retired generation collectable until
// the request has finished its block reads.
//
// Release is idempotent because every handler has two release paths: the first
// response write releases before touching a potentially slow client, while a
// defer covers cancellation and exits which write no response. A lease belongs
// to one handler goroutine; Once also makes the response-writer boundary robust
// to net/http calling both WriteHeader and Write.
type gcReadLease struct {
	gate Gate
	once sync.Once
}

func (h *Heads) acquireGCReadLease() *gcReadLease {
	h.cfg.Gate.Enter()
	return &gcReadLease{gate: h.cfg.Gate}
}

func (l *gcReadLease) Release() {
	l.once.Do(l.gate.Leave)
}

// releaseBeforeWriteWriter drops a reader lease before the first operation
// which may block on the network. Header mutations are local and do not release
// it; WriteHeader and Write do. Unwrap preserves http.ResponseController's
// access to the real connection if another deadline-setting layer is added
// below this wrapper later.
type releaseBeforeWriteWriter struct {
	http.ResponseWriter
	lease *gcReadLease
}

func (w *releaseBeforeWriteWriter) WriteHeader(status int) {
	w.lease.Release()
	w.ResponseWriter.WriteHeader(status)
}

func (w *releaseBeforeWriteWriter) Write(p []byte) (int, error) {
	w.lease.Release()
	return w.ResponseWriter.Write(p)
}

func (w *releaseBeforeWriteWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

// leaseResponseWriter installs the release boundary and the cancellation-safe
// fallback together so a handler cannot accidentally add one without the
// other.
func (s *Server) leaseResponseWriter(w http.ResponseWriter) (http.ResponseWriter, *gcReadLease) {
	lease := s.cfg.Heads.acquireGCReadLease()
	return &releaseBeforeWriteWriter{ResponseWriter: w, lease: lease}, lease
}
