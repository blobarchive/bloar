package follow

import (
	"testing"

	"github.com/blobarchive/bloar/metrics"
)

func TestSyncWakeIsOneDirtyBit(t *testing.T) {
	mx := metrics.New()
	f := &Follower{cfg: Config{Metrics: mx}}
	wake := make(chan struct{}, 1)

	for range 100 {
		f.requestSync(wake)
	}

	if got := len(wake); got != 1 {
		t.Fatalf("pending sync wakeups = %d, want one dirty bit", got)
	}
}
