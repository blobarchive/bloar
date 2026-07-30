package server

import (
	"crypto/subtle"
	"net/http"
	"strings"
)

// bearerScheme is the Authorization scheme of spec 7.3. RFC 6750 makes the
// scheme name case-insensitive, so it is matched as such; the token after it is
// not.
const bearerScheme = "bearer "

// auth guards the ingest and admin endpoints of spec 7.2 (7.3). Read endpoints
// do not go through it: they are public, which is the whole point of an
// archive.
func (s *Server) auth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !s.authorized(r) {
			// No detail: which of "absent", "malformed" and "wrong" a token was
			// is not something an unauthenticated caller needs told.
			w.Header().Set("WWW-Authenticate", "Bearer")
			writeError(w, http.StatusUnauthorized, "authorization required")
			return
		}
		next(w, r)
	}
}

// authorized reports whether r carries the configured bearer token.
//
// The comparison is constant-time in the token (spec 7.3). It is not
// constant-time in the token's length, which subtle.ConstantTimeCompare cannot
// be and which does not matter: the length of a token an operator chose is not
// the secret.
func (s *Server) authorized(r *http.Request) bool {
	// New rejects an empty token, so this cannot be reached with one. It is
	// re-checked anyway because the failure mode is not a bug, it is an open
	// archive: ConstantTimeCompare("", "") is 1, so an empty configured token
	// would authorize "Authorization: Bearer ".
	if s.cfg.AuthToken == "" {
		return false
	}
	h := r.Header.Get("Authorization")
	if len(h) < len(bearerScheme) || !strings.EqualFold(h[:len(bearerScheme)], bearerScheme) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(h[len(bearerScheme):]), []byte(s.cfg.AuthToken)) == 1
}
