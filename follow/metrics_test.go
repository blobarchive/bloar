package follow_test

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/blobarchive/bloar/follow"
	"github.com/blobarchive/bloar/metrics"
)

// This file is the poll-outcome counter of spec 11.3. It exists because the
// thing it counts cannot be counted anywhere else: the follower's judgement of
// what a channel gave it is not visible to the HTTP client (a document that
// arrives 200 and fails its signature check is a transport success), and the
// IPNS channel has no HTTP client at all.

// pollCount scrapes one series of bloar_follow_polls_total. It goes through the
// real /metrics handler rather than reaching into the registry: the exposition
// is what an operator's Prometheus reads, and the label rendering is half of
// what this test is asserting.
func pollCount(t *testing.T, mx *metrics.Metrics, channel, outcome string) float64 {
	t.Helper()
	return scrapeSeries(t, mx, `bloar_follow_polls_total{channel="`+channel+`",outcome="`+outcome+`"}`)
}

// refusalCount scrapes one series of bloar_follow_refusals_total, the per-head
// counter that follow_polls_total's outcome does not see.
func refusalCount(t *testing.T, mx *metrics.Metrics, reason string) float64 {
	t.Helper()
	return scrapeSeries(t, mx, `bloar_follow_refusals_total{reason="`+reason+`"}`)
}

// floorLagGauge scrapes one head's bloar_follow_synced_to_floor_lag, the width
// of the synced_to-floor divergence window a truncate-and-re-sync opens.
func floorLagGauge(t *testing.T, mx *metrics.Metrics, head string) float64 {
	t.Helper()
	return scrapeSeries(t, mx, `bloar_follow_synced_to_floor_lag{head="`+head+`"}`)
}

// scrapeSeries renders mx through the real /metrics handler and returns the
// value of one series, or 0 if it has no samples yet.
func scrapeSeries(t *testing.T, mx *metrics.Metrics, series string) float64 {
	t.Helper()
	srv := httptest.NewServer(metrics.Handler(mx, nil))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/metrics")
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reading /metrics: %v", err)
	}

	for line := range strings.SplitSeq(string(body), "\n") {
		if v, ok := strings.CutPrefix(line, series+" "); ok {
			var f float64
			if _, err := fmt.Sscanf(v, "%g", &f); err != nil {
				t.Fatalf("parsing %q: %v", line, err)
			}
			return f
		}
	}
	return 0 // a series with no samples yet is a zero, not a failure.
}

// TestPollCountsTheHTTPSChannel covers the ordinary case and the one a
// transport-level counter got wrong.
func TestPollCountsTheHTTPSChannel(t *testing.T) {
	w := newWriter(t)
	docs := newDocServer(t)
	mx := metrics.New()
	f := newFollower(t, w, func(c *follow.Config) { c.URL = docs.url; c.Metrics = mx })
	f.serveHTTP(nil)

	w.ingestSlot(100, 1)
	docs.publish(t, w, time.Now())
	f.poll()

	if got := pollCount(t, mx, metrics.ChannelHTTPS, metrics.OutcomeOK); got != 1 {
		t.Errorf("https/ok after one good poll = %g, want 1", got)
	}

	// An unreachable writer.
	docs.status(http.StatusInternalServerError)
	if err := f.pollErr(); err == nil {
		t.Fatal("a poll against a broken document server reported success")
	}
	if got := pollCount(t, mx, metrics.ChannelHTTPS, metrics.OutcomeError); got != 1 {
		t.Errorf("https/error after one failed poll = %g, want 1", got)
	}
	if got := pollCount(t, mx, metrics.ChannelHTTPS, metrics.OutcomeOK); got != 1 {
		t.Errorf("https/ok after a failed poll = %g, want it to stay 1", got)
	}
}

// TestPollCountsASignatureFailureAsAnError is the regression the RoundTripper
// this replaced could not have caught: the document server answers 200, the
// bytes arrive intact, and the document is signed by a key this node does not
// follow. That is a failed poll, and counting it as a 2xx would have made a
// follower under attack look perfectly healthy on a dashboard.
func TestPollCountsASignatureFailureAsAnError(t *testing.T) {
	w := newWriter(t)
	docs := newDocServer(t)
	mx := metrics.New()
	f := newFollower(t, w, func(c *follow.Config) { c.URL = docs.url; c.Metrics = mx })
	f.serveHTTP(nil)

	// A perfectly good archive, served with a 200, signed by somebody else.
	other := newWriter(t)
	docs.set(sign(t, other.key, w.unsigned(time.Now())))

	if err := f.pollErr(); err == nil {
		t.Fatal("the follower adopted a document signed by a key it does not follow")
	}
	if got := pollCount(t, mx, metrics.ChannelHTTPS, metrics.OutcomeError); got != 1 {
		t.Errorf("https/error after a 200 carrying a badly-signed document = %g, want 1: the poll failed, and the "+
			"status code is not what decides that", got)
	}
	if got := pollCount(t, mx, metrics.ChannelHTTPS, metrics.OutcomeOK); got != 0 {
		t.Errorf("https/ok after a badly-signed document = %g, want 0", got)
	}
}

// TestMetricsAreOptionalOnAFollower is the nil-safe contract at this seam: the
// default daemon has metrics off, so every poll it makes runs these call sites
// against a nil *Metrics.
func TestMetricsAreOptionalOnAFollower(t *testing.T) {
	w := newWriter(t)
	f := newFollower(t, w) // no Metrics.
	f.serveHTTP(nil)

	w.ingestSlot(100, 1)
	f.poll()

	if _, ok := f.heads.Get(testHead); !ok {
		t.Error("a follower with no metrics configured did not adopt the head")
	}
}
