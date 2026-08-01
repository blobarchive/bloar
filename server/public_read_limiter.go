package server

import (
	"container/list"
	"context"
	"fmt"
	"math"
	"net"
	"net/http"
	"net/netip"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// PublicReadAdmissionOutcome is the bounded result vocabulary reported by
// PublicReadLimiterConfig.Observe. It deliberately carries neither a client
// address nor a URL: an observer can turn these values into fixed-label
// counters without making a public request a source of metric cardinality.
type PublicReadAdmissionOutcome uint8

const (
	PublicReadAdmitted PublicReadAdmissionOutcome = iota
	PublicReadRejectedGlobal
	PublicReadRejectedClient
	PublicReadRejectedCanceled
)

// String returns the fixed metric-label spelling of an admission outcome.
func (o PublicReadAdmissionOutcome) String() string {
	switch o {
	case PublicReadAdmitted:
		return "admitted"
	case PublicReadRejectedGlobal:
		return "rejected_global"
	case PublicReadRejectedClient:
		return "rejected_client"
	case PublicReadRejectedCanceled:
		return "rejected_canceled"
	default:
		return "unknown"
	}
}

// PublicReadLimiterConfig bounds the public GET API with one process-wide and
// one per-client token bucket. Rates are weighted tokens per second. Bursts are
// weighted tokens and must each admit the heaviest legal request: one request
// token plus Config.MaxQueryHashes blob tokens.
//
// Client identity comes from the socket peer in Request.RemoteAddr by default.
// ForwardedHeader is honored only when the socket peer belongs to one of
// TrustedProxies. It is an IP or comma-separated IP-list header such as
// X-Forwarded-For, not RFC 7239's structured Forwarded syntax. The rightmost
// untrusted address is selected, so an untrusted client cannot choose its bucket
// by prepending a value to a correctly appended proxy chain.
//
// Observe is optional and may be called concurrently. Its outcome vocabulary is
// fixed above and cost is a value, never a label. The callback must not retain or
// mutate request state; none is passed to it.
type PublicReadLimiterConfig struct {
	GlobalRate  rate.Limit
	GlobalBurst int

	PerClientRate  rate.Limit
	PerClientBurst int

	MaxClientBuckets int
	ClientBucketTTL  time.Duration

	ForwardedHeader string
	TrustedProxies  []netip.Prefix

	Observe func(outcome PublicReadAdmissionOutcome, cost int)

	// now is a hermetic-test seam. Production callers cannot set it.
	now func() time.Time
}

// publicReadKind fixes the only two request-cost classes. Routes choose the
// class; no path or user-controlled string reaches metrics as a label.
type publicReadKind uint8

const (
	publicReadMetadata publicReadKind = iota
	publicReadBlobs
)

// publicReadCost charges one token for the request itself. A blobs query adds
// one token for every decoded array entry, including duplicates and entries in
// comma-separated values. An unfiltered query is charged for the configured
// maximum because its result cardinality is not known until after admission.
// Syntactically over-cap queries are charged at the maximum legal cost and then
// reach the handler's existing 400 response; the limiter's burst therefore
// never has to admit an unbounded URL.
func publicReadCost(r *http.Request, kind publicReadKind, maxQueryHashes int) int {
	if kind != publicReadBlobs {
		return 1
	}
	n := versionedHashQueryCount(r.URL.Query()["versioned_hashes"])
	if n == 0 || n > maxQueryHashes {
		n = maxQueryHashes
	}
	return 1 + n
}

type publicReadClientBucket struct {
	key      string
	limiter  *rate.Limiter
	lastSeen time.Time
	element  *list.Element
}

type publicReadLimiter struct {
	global         *rate.Limiter
	perClientRate  rate.Limit
	perClientBurst int
	maxClients     int
	clientTTL      time.Duration

	forwardedHeader string
	trustedProxies  []netip.Prefix

	observe func(PublicReadAdmissionOutcome, int)
	now     func() time.Time

	// mu serializes the two-bucket transaction as well as the bounded LRU. The
	// transaction matters: rate.Reservation cancellation can only restore as much
	// as later reservations permit, so a rejected client reservation must not race
	// a later global reservation and leak process-wide capacity.
	mu      sync.Mutex
	clients map[string]*publicReadClientBucket
	lru     list.List // most recently used at the front
}

// ValidatePublicReadLimiterConfig validates cfg without starting an HTTP
// server. Config adapters use this during strict file loading so an invalid
// budget fails before the daemon opens its store, listeners, or p2p host.
func ValidatePublicReadLimiterConfig(cfg PublicReadLimiterConfig, maxQueryHashes int) error {
	_, err := newPublicReadLimiter(cfg, maxQueryHashes)
	return err
}

func newPublicReadLimiter(cfg PublicReadLimiterConfig, maxQueryHashes int) (*publicReadLimiter, error) {
	maxCharge := 1 + maxQueryHashes
	if !validPublicReadRate(cfg.GlobalRate) {
		return nil, fmt.Errorf("server: Config.PublicReadLimiter.GlobalRate must be finite and positive")
	}
	if !validPublicReadRate(cfg.PerClientRate) {
		return nil, fmt.Errorf("server: Config.PublicReadLimiter.PerClientRate must be finite and positive")
	}
	if cfg.GlobalBurst < maxCharge {
		return nil, fmt.Errorf("server: Config.PublicReadLimiter.GlobalBurst is %d, must be at least maximum request charge %d", cfg.GlobalBurst, maxCharge)
	}
	if cfg.PerClientBurst < maxCharge {
		return nil, fmt.Errorf("server: Config.PublicReadLimiter.PerClientBurst is %d, must be at least maximum request charge %d", cfg.PerClientBurst, maxCharge)
	}
	if cfg.MaxClientBuckets <= 0 {
		return nil, fmt.Errorf("server: Config.PublicReadLimiter.MaxClientBuckets is %d, must be positive", cfg.MaxClientBuckets)
	}
	if cfg.ClientBucketTTL <= 0 {
		return nil, fmt.Errorf("server: Config.PublicReadLimiter.ClientBucketTTL is %s, must be positive", cfg.ClientBucketTTL)
	}
	if (cfg.ForwardedHeader == "") != (len(cfg.TrustedProxies) == 0) {
		return nil, fmt.Errorf("server: Config.PublicReadLimiter.ForwardedHeader and TrustedProxies must be configured together")
	}
	if cfg.ForwardedHeader != "" && !validHTTPFieldName(cfg.ForwardedHeader) {
		return nil, fmt.Errorf("server: Config.PublicReadLimiter.ForwardedHeader %q is not a valid HTTP field name", cfg.ForwardedHeader)
	}

	trusted := make([]netip.Prefix, len(cfg.TrustedProxies))
	for i, p := range cfg.TrustedProxies {
		if !p.IsValid() || p.Addr().Zone() != "" || p.Addr().Is4In6() {
			return nil, fmt.Errorf("server: Config.PublicReadLimiter.TrustedProxies[%d] is not a valid native IPv4 or IPv6 prefix", i)
		}
		trusted[i] = p.Masked()
	}
	now := cfg.now
	if now == nil {
		now = time.Now
	}
	return &publicReadLimiter{
		global:          rate.NewLimiter(cfg.GlobalRate, cfg.GlobalBurst),
		perClientRate:   cfg.PerClientRate,
		perClientBurst:  cfg.PerClientBurst,
		maxClients:      cfg.MaxClientBuckets,
		clientTTL:       cfg.ClientBucketTTL,
		forwardedHeader: http.CanonicalHeaderKey(cfg.ForwardedHeader),
		trustedProxies:  trusted,
		observe:         cfg.Observe,
		now:             now,
		clients:         make(map[string]*publicReadClientBucket, cfg.MaxClientBuckets),
	}, nil
}

func validPublicReadRate(v rate.Limit) bool {
	f := float64(v)
	return v != rate.Inf && f > 0 && !math.IsNaN(f) && !math.IsInf(f, 0)
}

// validHTTPFieldName is the RFC 9110 token grammar used by header names.
func validHTTPFieldName(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') {
			continue
		}
		switch c {
		case '!', '#', '$', '%', '&', '\'', '*', '+', '-', '.', '^', '_', '`', '|', '~':
			continue
		}
		return false
	}
	return true
}

type publicReadDecision struct {
	admitted   bool
	outcome    PublicReadAdmissionOutcome
	retryAfter time.Duration
}

// admit performs a non-waiting, all-or-nothing reservation against the global
// and client buckets. A rejected or canceled request consumes neither bucket.
func (l *publicReadLimiter) admit(ctx context.Context, client string, cost int) publicReadDecision {
	if err := ctx.Err(); err != nil {
		return l.decision(PublicReadRejectedCanceled, cost, 0)
	}

	l.mu.Lock()
	if err := ctx.Err(); err != nil {
		l.mu.Unlock()
		return l.decision(PublicReadRejectedCanceled, cost, 0)
	}
	// Read the clock after taking mu. Besides making tests deterministic, this
	// prevents two concurrent callers from obtaining timestamps in one order and
	// reaching rate.Limiter in the other, which would make its clock move back.
	now := l.now()
	bucket := l.bucketLocked(client, now)
	globalReservation := l.global.ReserveN(now, cost)
	clientReservation := bucket.limiter.ReserveN(now, cost)

	globalDelay := reservationDelay(globalReservation, now)
	clientDelay := reservationDelay(clientReservation, now)
	if err := ctx.Err(); err != nil {
		clientReservation.CancelAt(now)
		globalReservation.CancelAt(now)
		l.mu.Unlock()
		return l.decision(PublicReadRejectedCanceled, cost, 0)
	}
	if globalDelay > 0 || clientDelay > 0 {
		// No later admission can interleave before these cancellations because mu
		// covers the whole two-limiter transaction. Both reservations are therefore
		// restored exactly, not merely "as much as possible".
		clientReservation.CancelAt(now)
		globalReservation.CancelAt(now)
		l.mu.Unlock()

		retryAfter := globalDelay
		outcome := PublicReadRejectedGlobal
		if clientDelay > retryAfter {
			retryAfter = clientDelay
			outcome = PublicReadRejectedClient
		}
		return l.decision(outcome, cost, retryAfter)
	}
	l.mu.Unlock()
	return l.decision(PublicReadAdmitted, cost, 0)
}

func reservationDelay(r *rate.Reservation, now time.Time) time.Duration {
	if !r.OK() {
		return rate.InfDuration
	}
	return r.DelayFrom(now)
}

func (l *publicReadLimiter) decision(outcome PublicReadAdmissionOutcome, cost int, retryAfter time.Duration) publicReadDecision {
	if l.observe != nil {
		l.observe(outcome, cost)
	}
	return publicReadDecision{admitted: outcome == PublicReadAdmitted, outcome: outcome, retryAfter: retryAfter}
}

func (l *publicReadLimiter) bucketLocked(key string, now time.Time) *publicReadClientBucket {
	l.expireLocked(now)
	if bucket := l.clients[key]; bucket != nil {
		bucket.lastSeen = now
		l.lru.MoveToFront(bucket.element)
		return bucket
	}
	if len(l.clients) == l.maxClients {
		l.removeLocked(l.lru.Back())
	}
	bucket := &publicReadClientBucket{
		key:      key,
		limiter:  rate.NewLimiter(l.perClientRate, l.perClientBurst),
		lastSeen: now,
	}
	bucket.element = l.lru.PushFront(bucket)
	l.clients[key] = bucket
	return bucket
}

func (l *publicReadLimiter) expireLocked(now time.Time) {
	for element := l.lru.Back(); element != nil; element = l.lru.Back() {
		bucket := element.Value.(*publicReadClientBucket)
		if bucket.lastSeen.Add(l.clientTTL).After(now) {
			return
		}
		l.removeLocked(element)
	}
}

func (l *publicReadLimiter) removeLocked(element *list.Element) {
	if element == nil {
		return
	}
	bucket := element.Value.(*publicReadClientBucket)
	delete(l.clients, bucket.key)
	l.lru.Remove(element)
}

const unknownPublicReadClient = "_unknown"

func (l *publicReadLimiter) clientKey(r *http.Request) string {
	remote, ok := remoteIP(r.RemoteAddr)
	if !ok {
		return unknownPublicReadClient
	}
	client := remote
	if l.forwardedHeader != "" && l.trusted(remote) {
		if forwarded, ok := parseForwardedIPs(r.Header.Values(l.forwardedHeader)); ok {
			client = forwarded[0]
			for i := len(forwarded) - 1; i >= 0; i-- {
				if !l.trusted(forwarded[i]) {
					client = forwarded[i]
					break
				}
			}
		}
	}
	return publicReadClientKey(client)
}

func remoteIP(remoteAddr string) (netip.Addr, bool) {
	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		host = remoteAddr
	}
	addr, err := netip.ParseAddr(strings.TrimSpace(host))
	if err != nil {
		return netip.Addr{}, false
	}
	return addr.WithZone("").Unmap(), true
}

func parseForwardedIPs(values []string) ([]netip.Addr, bool) {
	var out []netip.Addr
	for _, value := range values {
		for _, raw := range strings.Split(value, ",") {
			addr, err := netip.ParseAddr(strings.TrimSpace(raw))
			if err != nil || addr.Zone() != "" {
				return nil, false
			}
			out = append(out, addr.Unmap())
		}
	}
	return out, len(out) > 0
}

func (l *publicReadLimiter) trusted(addr netip.Addr) bool {
	for _, prefix := range l.trustedProxies {
		if prefix.Contains(addr) {
			return true
		}
	}
	return false
}

func publicReadClientKey(addr netip.Addr) string {
	if addr.Is6() {
		return netip.PrefixFrom(addr, 64).Masked().String()
	}
	return addr.String()
}

func retryAfterInteger(delay time.Duration) string {
	seconds := int64(delay / time.Second)
	if delay%time.Second != 0 {
		seconds++
	}
	if seconds < 1 {
		seconds = 1
	}
	return strconv.FormatInt(seconds, 10)
}

// limitPublicRead wraps only the explicit public GET routes mounted in routes.
// The authenticated mutation routes and the daemon's separate metrics handler
// never pass through this function.
func (s *Server) limitPublicRead(kind publicReadKind, next http.HandlerFunc) http.HandlerFunc {
	if s.publicReadLimiter == nil {
		return next
	}
	return func(w http.ResponseWriter, r *http.Request) {
		cost := publicReadCost(r, kind, s.cfg.MaxQueryHashes)
		decision := s.publicReadLimiter.admit(r.Context(), s.publicReadLimiter.clientKey(r), cost)
		if !decision.admitted {
			w.Header().Set("Retry-After", retryAfterInteger(decision.retryAfter))
			w.Header().Set("Cache-Control", "no-store")
			writeError(w, http.StatusTooManyRequests, "public read rate limit exceeded; retry later")
			return
		}
		next(w, r)
	}
}
