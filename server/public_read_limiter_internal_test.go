package server

import (
	"context"
	"math"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/time/rate"
)

type publicReadFakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newPublicReadFakeClock() *publicReadFakeClock {
	return &publicReadFakeClock{now: time.Unix(1_700_000_000, 0)}
}

func (c *publicReadFakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *publicReadFakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

func testPublicReadLimiterConfig(clock *publicReadFakeClock) PublicReadLimiterConfig {
	return PublicReadLimiterConfig{
		GlobalRate:       10,
		GlobalBurst:      32,
		PerClientRate:    10,
		PerClientBurst:   32,
		MaxClientBuckets: 16,
		ClientBucketTTL:  time.Hour,
		now:              clock.Now,
	}
}

// These values are the default-on daemon policy shipped in both example
// configs. cmd/bloard's config tests pin the same values; keeping this helper in
// the server package lets the behavioral load trace exercise the limiter
// without importing a command package.
const (
	shippedPublicReadGlobalRate    = 4096
	shippedPublicReadGlobalBurst   = 16384
	shippedPublicReadClientRate    = 1024
	shippedPublicReadClientBurst   = 4096
	shippedPublicReadClientBuckets = 4096
)

func shippedPublicReadLimiterConfig(now func() time.Time) PublicReadLimiterConfig {
	return PublicReadLimiterConfig{
		GlobalRate:       shippedPublicReadGlobalRate,
		GlobalBurst:      shippedPublicReadGlobalBurst,
		PerClientRate:    shippedPublicReadClientRate,
		PerClientBurst:   shippedPublicReadClientBurst,
		MaxClientBuckets: shippedPublicReadClientBuckets,
		ClientBucketTTL:  15 * time.Minute,
		now:              now,
	}
}

// publicReadPace is the smallest whole-nanosecond interval that replenishes
// cost units at unitsPerSecond. Rounding upward is intentional: a trace driven
// at the documented rate must not fail because time.Duration cannot represent
// a fractional nanosecond.
func publicReadPace(cost, unitsPerSecond int) time.Duration {
	numerator := int64(time.Second)*int64(cost) + int64(unitsPerSecond) - 1
	return time.Duration(numerator / int64(unitsPerSecond))
}

func mustPublicReadLimiter(t *testing.T, cfg PublicReadLimiterConfig, maxHashes int) *publicReadLimiter {
	t.Helper()
	limiter, err := newPublicReadLimiter(cfg, maxHashes)
	if err != nil {
		t.Fatalf("newPublicReadLimiter: %v", err)
	}
	return limiter
}

func TestPublicReadCost(t *testing.T) {
	const maxHashes = 4
	tests := []struct {
		name string
		kind publicReadKind
		url  string
		want int
	}{
		{name: "metadata", kind: publicReadMetadata, url: "/bloar/v1/heads", want: 1},
		{name: "unfiltered", kind: publicReadBlobs, url: "/all/eth/v1/beacon/blobs/1", want: 1 + maxHashes},
		{name: "filtered", kind: publicReadBlobs, url: "/all/eth/v1/beacon/blobs/1?versioned_hashes=a&versioned_hashes=b", want: 3},
		{name: "comma-separated array", kind: publicReadBlobs, url: "/all/eth/v1/beacon/blobs/1?versioned_hashes=a,b", want: 3},
		{name: "mixed array encodings", kind: publicReadBlobs, url: "/all/eth/v1/beacon/blobs/1?versioned_hashes=a,b&versioned_hashes=c", want: 4},
		{name: "duplicate hashes count", kind: publicReadBlobs, url: "/all/eth/v1/beacon/blobs/1?versioned_hashes=a&versioned_hashes=a&versioned_hashes=a", want: 4},
		{name: "empty named hash is still filtered", kind: publicReadBlobs, url: "/all/eth/v1/beacon/blobs/1?versioned_hashes=", want: 2},
		{name: "over-cap request gets bounded maximum before handler rejects it", kind: publicReadBlobs, url: "/all/eth/v1/beacon/blobs/1?versioned_hashes=1&versioned_hashes=2&versioned_hashes=3&versioned_hashes=4&versioned_hashes=5", want: 1 + maxHashes},
		{name: "over-cap comma array gets bounded maximum", kind: publicReadBlobs, url: "/all/eth/v1/beacon/blobs/1?versioned_hashes=1,2,3,4,5", want: 1 + maxHashes},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, test.url, nil)
			if got := publicReadCost(req, test.kind, maxHashes); got != test.want {
				t.Fatalf("publicReadCost = %d, want %d", got, test.want)
			}
		})
	}
}

func TestPublicReadLimiterValidation(t *testing.T) {
	clock := newPublicReadFakeClock()
	valid := testPublicReadLimiterConfig(clock)
	const maxHashes = 4
	const maxCharge = 1 + maxHashes
	valid.GlobalBurst = maxCharge
	valid.PerClientBurst = maxCharge

	tests := []struct {
		name   string
		mutate func(*PublicReadLimiterConfig)
	}{
		{name: "zero global rate", mutate: func(c *PublicReadLimiterConfig) { c.GlobalRate = 0 }},
		{name: "NaN global rate", mutate: func(c *PublicReadLimiterConfig) { c.GlobalRate = rate.Limit(math.NaN()) }},
		{name: "infinite client rate", mutate: func(c *PublicReadLimiterConfig) { c.PerClientRate = rate.Inf }},
		{name: "global burst below maximum charge", mutate: func(c *PublicReadLimiterConfig) { c.GlobalBurst = maxCharge - 1 }},
		{name: "client burst below maximum charge", mutate: func(c *PublicReadLimiterConfig) { c.PerClientBurst = maxCharge - 1 }},
		{name: "no client bound", mutate: func(c *PublicReadLimiterConfig) { c.MaxClientBuckets = 0 }},
		{name: "no TTL", mutate: func(c *PublicReadLimiterConfig) { c.ClientBucketTTL = 0 }},
		{name: "header without trust", mutate: func(c *PublicReadLimiterConfig) { c.ForwardedHeader = "X-Forwarded-For" }},
		{name: "trust without header", mutate: func(c *PublicReadLimiterConfig) {
			c.TrustedProxies = []netip.Prefix{netip.MustParsePrefix("10.0.0.0/8")}
		}},
		{name: "invalid header name", mutate: func(c *PublicReadLimiterConfig) {
			c.ForwardedHeader = "X Forwarded For"
			c.TrustedProxies = []netip.Prefix{netip.MustParsePrefix("10.0.0.0/8")}
		}},
		{name: "mapped prefix", mutate: func(c *PublicReadLimiterConfig) {
			c.ForwardedHeader = "X-Forwarded-For"
			c.TrustedProxies = []netip.Prefix{netip.MustParsePrefix("::ffff:10.0.0.0/104")}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := valid
			test.mutate(&cfg)
			if _, err := newPublicReadLimiter(cfg, maxHashes); err == nil {
				t.Fatal("newPublicReadLimiter accepted invalid config")
			}
		})
	}
	if _, err := newPublicReadLimiter(valid, maxHashes); err != nil {
		t.Fatalf("maximum legal charge must fit exactly in each burst: %v", err)
	}
}

func TestPublicReadLimiterRejectedClientRestoresGlobalReservation(t *testing.T) {
	clock := newPublicReadFakeClock()
	cfg := testPublicReadLimiterConfig(clock)
	cfg.GlobalRate, cfg.GlobalBurst = 2, 2
	cfg.PerClientRate, cfg.PerClientBurst = 0.01, 2
	limiter := mustPublicReadLimiter(t, cfg, 1)

	for range 2 {
		if got := limiter.admit(context.Background(), "client-a", 1); !got.admitted {
			t.Fatalf("initial client-a admission = %+v", got)
		}
	}
	clock.Advance(time.Second) // global is full; client-a has only 0.01 token
	if got := limiter.admit(context.Background(), "client-a", 1); got.admitted || got.outcome != PublicReadRejectedClient {
		t.Fatalf("client-limited decision = %+v, want rejected_client", got)
	}

	// The rejected client reservation must have restored the successful global
	// reservation. A fresh client can therefore spend the whole two-token burst.
	if got := limiter.admit(context.Background(), "client-b", 2); !got.admitted {
		t.Fatalf("full global burst after client rejection = %+v; global reservation leaked", got)
	}
}

func TestPublicReadLimiterRejectedGlobalRestoresClientReservation(t *testing.T) {
	clock := newPublicReadFakeClock()
	cfg := testPublicReadLimiterConfig(clock)
	cfg.GlobalRate, cfg.GlobalBurst = 0.01, 2
	cfg.PerClientRate, cfg.PerClientBurst = 0.001, 2
	limiter := mustPublicReadLimiter(t, cfg, 1)

	if got := limiter.admit(context.Background(), "client-a", 2); !got.admitted {
		t.Fatalf("initial admission = %+v", got)
	}
	clock.Advance(time.Second)
	if got := limiter.admit(context.Background(), "client-b", 1); got.admitted || got.outcome != PublicReadRejectedGlobal {
		t.Fatalf("global-limited decision = %+v, want rejected_global", got)
	}

	// At t+200s the global bucket has refilled. Client-b has only 0.2 tokens of
	// natural refill, so a leaked token from the rejected request would keep this
	// two-token request out. Exact cancellation lets it through.
	clock.Advance(199 * time.Second)
	if got := limiter.admit(context.Background(), "client-b", 2); !got.admitted {
		t.Fatalf("full client burst after global rejection = %+v; client reservation leaked", got)
	}
}

func TestPublicReadLimiterCanceledRequestConsumesNothing(t *testing.T) {
	clock := newPublicReadFakeClock()
	cfg := testPublicReadLimiterConfig(clock)
	cfg.GlobalBurst, cfg.PerClientBurst = 2, 2
	limiter := mustPublicReadLimiter(t, cfg, 1)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if got := limiter.admit(ctx, "client-a", 2); got.admitted || got.outcome != PublicReadRejectedCanceled {
		t.Fatalf("canceled admission = %+v, want rejected_canceled", got)
	}
	if got := limiter.admit(context.Background(), "client-a", 2); !got.admitted {
		t.Fatalf("full burst after canceled request = %+v; canceled request consumed tokens", got)
	}
}

func TestPublicReadLimiterRetryAfterUsesGoverningBucket(t *testing.T) {
	clock := newPublicReadFakeClock()
	cfg := testPublicReadLimiterConfig(clock)
	cfg.GlobalRate, cfg.GlobalBurst = 100, 2
	cfg.PerClientRate, cfg.PerClientBurst = 0.5, 2
	limiter := mustPublicReadLimiter(t, cfg, 1)

	if got := limiter.admit(context.Background(), "client-a", 2); !got.admitted {
		t.Fatalf("initial admission = %+v", got)
	}
	got := limiter.admit(context.Background(), "client-a", 1)
	if got.admitted || got.outcome != PublicReadRejectedClient || got.retryAfter != 2*time.Second {
		t.Fatalf("limited decision = %+v, want rejected_client with two-second retry", got)
	}
	if value := retryAfterInteger(got.retryAfter); value != "2" {
		t.Fatalf("Retry-After = %q, want integer 2", value)
	}
	if value := retryAfterInteger(100*time.Millisecond + 1); value != "1" {
		t.Fatalf("subsecond Retry-After = %q, want ceiling 1", value)
	}
}

func TestPublicReadLimiterClientBucketsAreTTLandLRUBounded(t *testing.T) {
	clock := newPublicReadFakeClock()
	cfg := testPublicReadLimiterConfig(clock)
	cfg.MaxClientBuckets = 2
	cfg.ClientBucketTTL = 5 * time.Minute
	limiter := mustPublicReadLimiter(t, cfg, 1)

	for _, client := range []string{"a", "b", "a", "c"} {
		if got := limiter.admit(context.Background(), client, 1); !got.admitted {
			t.Fatalf("admitting %q: %+v", client, got)
		}
	}
	limiter.mu.Lock()
	_, hasA := limiter.clients["a"]
	_, hasB := limiter.clients["b"]
	_, hasC := limiter.clients["c"]
	count := len(limiter.clients)
	limiter.mu.Unlock()
	if count != 2 || !hasA || hasB || !hasC {
		t.Fatalf("LRU keys: len=%d a=%t b=%t c=%t, want only a and c", count, hasA, hasB, hasC)
	}

	clock.Advance(5 * time.Minute)
	if got := limiter.admit(context.Background(), "d", 1); !got.admitted {
		t.Fatalf("admitting d after TTL: %+v", got)
	}
	limiter.mu.Lock()
	count = len(limiter.clients)
	_, hasD := limiter.clients["d"]
	limiter.mu.Unlock()
	if count != 1 || !hasD {
		t.Fatalf("TTL keys: len=%d d=%t, want only d", count, hasD)
	}
}

func TestPublicReadLimiterClientKeyTrustAndIPv6Aggregation(t *testing.T) {
	clock := newPublicReadFakeClock()
	cfg := testPublicReadLimiterConfig(clock)
	cfg.ForwardedHeader = "X-Forwarded-For"
	cfg.TrustedProxies = []netip.Prefix{
		netip.MustParsePrefix("10.0.0.0/8"),
		netip.MustParsePrefix("2001:db8:ffff::/48"),
	}
	limiter := mustPublicReadLimiter(t, cfg, 1)

	req := httptest.NewRequest(http.MethodGet, "http://example.test/", nil)
	req.RemoteAddr = "203.0.113.7:4321"
	req.Header.Set("X-Forwarded-For", "198.51.100.9")
	if got := limiter.clientKey(req); got != "203.0.113.7" {
		t.Fatalf("untrusted proxy key = %q, want socket peer", got)
	}

	req.RemoteAddr = "10.2.3.4:4321"
	req.Header.Set("X-Forwarded-For", "198.51.100.99, 192.0.2.20, 10.9.8.7")
	if got := limiter.clientKey(req); got != "192.0.2.20" {
		t.Fatalf("trusted chain key = %q, want rightmost untrusted hop", got)
	}

	// A malformed forwarded chain is ignored as a whole; falling back to the
	// trusted socket peer prevents attacker-controlled malformed strings from
	// creating buckets.
	req.Header.Set("X-Forwarded-For", "198.51.100.99, definitely-not-an-ip")
	if got := limiter.clientKey(req); got != "10.2.3.4" {
		t.Fatalf("malformed chain key = %q, want socket peer fallback", got)
	}

	req.RemoteAddr = "[2001:db8:ffff::1]:4321"
	req.Header.Set("X-Forwarded-For", "2001:db8:1234:5678::1")
	first := limiter.clientKey(req)
	req.Header.Set("X-Forwarded-For", "2001:db8:1234:5678:ffff::2")
	second := limiter.clientKey(req)
	if first != "2001:db8:1234:5678::/64" || second != first {
		t.Fatalf("IPv6 keys = %q and %q, want one /64 bucket", first, second)
	}
}

func TestPublicReadLimiterConcurrentAdmissionIsRaceSafeAndExact(t *testing.T) {
	clock := newPublicReadFakeClock()
	cfg := testPublicReadLimiterConfig(clock)
	cfg.GlobalRate, cfg.GlobalBurst = 1, 16
	cfg.PerClientRate, cfg.PerClientBurst = 1, 16
	var observed atomic.Int64
	cfg.Observe = func(_ PublicReadAdmissionOutcome, _ int) { observed.Add(1) }
	limiter := mustPublicReadLimiter(t, cfg, 1)

	const requests = 128
	start := make(chan struct{})
	results := make(chan bool, requests)
	var wg sync.WaitGroup
	for range requests {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			results <- limiter.admit(context.Background(), "same-client", 1).admitted
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	allowed := 0
	for result := range results {
		if result {
			allowed++
		}
	}
	if allowed != 16 {
		t.Fatalf("concurrent admissions = %d allowed, want exact burst 16", allowed)
	}
	if got := observed.Load(); got != requests {
		t.Fatalf("observer calls = %d, want %d", got, requests)
	}
}

// TestPublicReadLimiterShippedDefaultsAdmitSerialSync is the default-policy
// acceptance trace for the public-read admission work. Nitro initializes two metadata endpoints, then
// fetches one slot at a time. The 1/3/6/9-hash cycle covers cheap historical
// reads through the current protocol maximum without pretending to be a
// mainnet distribution. Advancing the hermetic clock by each request's weighted
// service interval drives one client at exactly its shipped sustained budget.
//
// The trace spends far more than the initial 4096-unit client burst, so passing
// proves refill, weighting, and the atomic global+client transaction do not
// create unintended 429s. It does not establish that a production handler or
// network can sustain this rate; that requires a deployment-specific benchmark.
func TestPublicReadLimiterShippedDefaultsAdmitSerialSync(t *testing.T) {
	clock := newPublicReadFakeClock()
	var admitted, rejected, admittedCost int
	cfg := shippedPublicReadLimiterConfig(clock.Now)
	cfg.Observe = func(outcome PublicReadAdmissionOutcome, cost int) {
		if outcome == PublicReadAdmitted {
			admitted++
			admittedCost += cost
			return
		}
		rejected++
	}
	limiter := mustPublicReadLimiter(t, cfg, defaultMaxQueryHashes)
	s := &Server{cfg: Config{MaxQueryHashes: defaultMaxQueryHashes}, publicReadLimiter: limiter}

	handlerCalls := 0
	next := func(w http.ResponseWriter, _ *http.Request) {
		handlerCalls++
		w.WriteHeader(http.StatusNoContent)
	}
	metadata := s.limitPublicRead(publicReadMetadata, next)
	blobs := s.limitPublicRead(publicReadBlobs, next)

	for _, path := range []string{
		"http://example.test/all/eth/v1/beacon/genesis",
		"http://example.test/all/eth/v1/config/spec",
	} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		req.RemoteAddr = "192.0.2.10:40000"
		rec := httptest.NewRecorder()
		metadata(rec, req)
		if rec.Code != http.StatusNoContent {
			t.Fatalf("metadata initialization %s returned %d", path, rec.Code)
		}
	}

	const slots = 8192
	hashesPerSlot := [...]int{1, 3, 6, 9}
	expectedCost := 2 // the two metadata requests above
	previousCost := 0
	for slot := 0; slot < slots; slot++ {
		if previousCost != 0 {
			clock.Advance(publicReadPace(previousCost, shippedPublicReadClientRate))
		}
		hashes := hashesPerSlot[slot%len(hashesPerSlot)]
		cost := 1 + hashes
		expectedCost += cost
		previousCost = cost

		path := "http://example.test/all/eth/v1/beacon/blobs/1?" + repeatedHashQuery(hashes)
		req := httptest.NewRequest(http.MethodGet, path, nil)
		req.RemoteAddr = "192.0.2.10:40000"
		rec := httptest.NewRecorder()
		blobs(rec, req)
		if rec.Code != http.StatusNoContent {
			t.Fatalf("serial slot %d (%d hashes, cost %d) returned %d; body=%s",
				slot, hashes, cost, rec.Code, rec.Body.String())
		}
	}

	if rejected != 0 {
		t.Fatalf("serial trace recorded %d rejected admissions", rejected)
	}
	if admitted != slots+2 || handlerCalls != admitted {
		t.Fatalf("admitted=%d handler_calls=%d, want %d", admitted, handlerCalls, slots+2)
	}
	if admittedCost != expectedCost || admittedCost <= shippedPublicReadClientBurst {
		t.Fatalf("admitted cost=%d, want %d and greater than initial burst %d",
			admittedCost, expectedCost, shippedPublicReadClientBurst)
	}
}

func repeatedHashQuery(count int) string {
	const field = "versioned_hashes=0x0000000000000000000000000000000000000000000000000000000000000000"
	if count <= 0 {
		return ""
	}
	var query strings.Builder
	query.Grow(count*len(field) + count - 1)
	for i := 0; i < count; i++ {
		if i != 0 {
			query.WriteByte('&')
		}
		query.WriteString(field)
	}
	return query.String()
}

func TestLimitPublicReadReturns429WithoutCallingHandler(t *testing.T) {
	clock := newPublicReadFakeClock()
	cfg := testPublicReadLimiterConfig(clock)
	cfg.GlobalRate, cfg.GlobalBurst = 1, 2
	cfg.PerClientRate, cfg.PerClientBurst = 1, 2
	limiter := mustPublicReadLimiter(t, cfg, 1)
	s := &Server{cfg: Config{MaxQueryHashes: 1}, publicReadLimiter: limiter}

	called := 0
	handler := s.limitPublicRead(publicReadMetadata, func(w http.ResponseWriter, _ *http.Request) {
		called++
		w.WriteHeader(http.StatusNoContent)
	})
	for i := 0; i < 3; i++ {
		req := httptest.NewRequest(http.MethodGet, "http://example.test/bloar/v1/heads", nil)
		req.RemoteAddr = "192.0.2.1:1234"
		rec := httptest.NewRecorder()
		handler(rec, req)
		if i < 2 {
			if rec.Code != http.StatusNoContent {
				t.Fatalf("request %d status = %d, want 204", i, rec.Code)
			}
			continue
		}
		if rec.Code != http.StatusTooManyRequests {
			t.Fatalf("limited status = %d, want 429", rec.Code)
		}
		if got := rec.Header().Get("Retry-After"); got != "1" {
			t.Errorf("Retry-After = %q, want integer 1", got)
		}
		if got := rec.Header().Get("Cache-Control"); got != "no-store" {
			t.Errorf("Cache-Control = %q, want no-store", got)
		}
	}
	if called != 2 {
		t.Fatalf("handler calls = %d, want 2; rejected request reached handler", called)
	}
}

func TestRoutesDoNotLimitMutationsMetricsOrUnknownPaths(t *testing.T) {
	clock := newPublicReadFakeClock()
	cfg := testPublicReadLimiterConfig(clock)
	limiter := mustPublicReadLimiter(t, cfg, 1)
	s := &Server{
		cfg:               Config{AuthToken: "secret", MaxQueryHashes: 1},
		mux:               http.NewServeMux(),
		publicReadLimiter: limiter,
	}
	s.routes()

	requests := []*http.Request{
		httptest.NewRequest(http.MethodPost, "http://example.test/bloar/v1/blobs", nil),
		httptest.NewRequest(http.MethodGet, "http://example.test/metrics", nil),
		httptest.NewRequest(http.MethodGet, "http://example.test/not-an-api", nil),
	}
	for _, req := range requests {
		req.RemoteAddr = "192.0.2.1:1234"
		s.ServeHTTP(httptest.NewRecorder(), req)
	}
	limiter.mu.Lock()
	count := len(limiter.clients)
	limiter.mu.Unlock()
	if count != 0 {
		t.Fatalf("non-public routes allocated %d client buckets, want zero", count)
	}
}
