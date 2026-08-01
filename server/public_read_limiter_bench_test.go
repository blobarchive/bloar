package server

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

// BenchmarkPublicReadLimiterAdmitShippedDefaults measures the serialized
// two-bucket transaction itself. Its logical clock paces each case at the
// shipped per-client rate so every decision is admitted; ns/op is therefore
// limiter bookkeeping capacity, not a claim about downstream handler capacity.
func BenchmarkPublicReadLimiterAdmitShippedDefaults(b *testing.B) {
	for _, workload := range publicReadBenchmarkWorkloads() {
		b.Run(workload.name, func(b *testing.B) {
			now := time.Unix(1_700_000_000, 0)
			cfg := shippedPublicReadLimiterConfig(func() time.Time { return now })
			limiter, err := newPublicReadLimiter(cfg, defaultMaxQueryHashes)
			if err != nil {
				b.Fatal(err)
			}
			step := publicReadPace(workload.cost, shippedPublicReadClientRate)
			ctx := b.Context()
			b.ReportMetric(float64(workload.cost), "weighted-units/op")
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				now = now.Add(step)
				if decision := limiter.admit(ctx, "192.0.2.10", workload.cost); !decision.admitted {
					b.Fatalf("paced admission rejected: %+v", decision)
				}
			}
		})
	}
}

// BenchmarkPublicReadLimiterHTTPShippedDefaults includes the actual wrapper's
// URL weighting, socket-client extraction, header/status path, and downstream
// dispatch. The downstream handler is intentionally a no-op: this isolates the
// admission layer and makes the limit of the evidence explicit.
func BenchmarkPublicReadLimiterHTTPShippedDefaults(b *testing.B) {
	for _, workload := range publicReadBenchmarkWorkloads() {
		b.Run(workload.name, func(b *testing.B) {
			now := time.Unix(1_700_000_000, 0)
			cfg := shippedPublicReadLimiterConfig(func() time.Time { return now })
			limiter, err := newPublicReadLimiter(cfg, defaultMaxQueryHashes)
			if err != nil {
				b.Fatal(err)
			}
			s := &Server{cfg: Config{MaxQueryHashes: defaultMaxQueryHashes}, publicReadLimiter: limiter}
			handler := s.limitPublicRead(workload.kind, func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusNoContent)
			})
			req := httptest.NewRequest(http.MethodGet, workload.url, nil)
			req.RemoteAddr = "192.0.2.10:40000"
			writer := newPublicReadBenchmarkWriter()
			step := publicReadPace(workload.cost, shippedPublicReadClientRate)

			b.ReportMetric(float64(workload.cost), "weighted-units/op")
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				now = now.Add(step)
				writer.reset()
				handler(writer, req)
				if writer.status != http.StatusNoContent {
					b.Fatalf("paced HTTP admission returned %d", writer.status)
				}
			}
		})
	}
}

// BenchmarkPublicReadLimiterParallelShippedDefaults measures contention on the
// global transaction lock with a bounded set of active clients. now advances
// once inside that lock by the global weighted interval, so all operations are
// admitted at the shipped process-wide rate and every iteration sees the same
// steady admitted path. The atomic client selector is included in ns/op and
// makes this a conservative limiter-only figure.
func BenchmarkPublicReadLimiterParallelShippedDefaults(b *testing.B) {
	for _, workload := range []publicReadBenchmarkWorkload{
		{name: "metadata", cost: 1},
		{name: "six-blob", cost: 7},
	} {
		b.Run(workload.name, func(b *testing.B) {
			now := time.Unix(1_700_000_000, 0)
			step := publicReadPace(workload.cost, shippedPublicReadGlobalRate)
			cfg := shippedPublicReadLimiterConfig(func() time.Time {
				now = now.Add(step) // publicReadLimiter.mu serializes this callback.
				return now
			})
			limiter, err := newPublicReadLimiter(cfg, defaultMaxQueryHashes)
			if err != nil {
				b.Fatal(err)
			}
			clients := make([]string, 256)
			for i := range clients {
				clients[i] = "client-" + strconv.Itoa(i)
			}
			var sequence atomic.Uint64
			var rejected atomic.Bool
			ctx := b.Context()
			b.ReportMetric(float64(workload.cost), "weighted-units/op")
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					client := clients[sequence.Add(1)%uint64(len(clients))]
					if decision := limiter.admit(ctx, client, workload.cost); !decision.admitted {
						rejected.Store(true)
					}
				}
			})
			if rejected.Load() {
				b.Fatal("paced parallel admission rejected")
			}
		})
	}
}

type publicReadBenchmarkWorkload struct {
	name string
	kind publicReadKind
	url  string
	cost int
}

func publicReadBenchmarkWorkloads() []publicReadBenchmarkWorkload {
	return []publicReadBenchmarkWorkload{
		{name: "metadata", kind: publicReadMetadata, url: "http://example.test/all/eth/v1/beacon/genesis", cost: 1},
		{name: "one-blob", kind: publicReadBlobs, url: "http://example.test/all/eth/v1/beacon/blobs/1?" + repeatedHashQuery(1), cost: 2},
		{name: "six-blob", kind: publicReadBlobs, url: "http://example.test/all/eth/v1/beacon/blobs/1?" + repeatedHashQuery(6), cost: 7},
		{name: "nine-blob", kind: publicReadBlobs, url: "http://example.test/all/eth/v1/beacon/blobs/1?" + repeatedHashQuery(9), cost: 10},
	}
}

type publicReadBenchmarkWriter struct {
	header http.Header
	status int
}

func newPublicReadBenchmarkWriter() *publicReadBenchmarkWriter {
	return &publicReadBenchmarkWriter{header: make(http.Header)}
}

func (w *publicReadBenchmarkWriter) Header() http.Header { return w.header }

func (w *publicReadBenchmarkWriter) WriteHeader(status int) { w.status = status }

func (w *publicReadBenchmarkWriter) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return len(p), nil
}

func (w *publicReadBenchmarkWriter) reset() {
	w.status = 0
	clear(w.header)
}
