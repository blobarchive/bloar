package main

import (
	"context"
	"io"
	"log/slog"
	"os"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/blobarchive/bloar/pinning"
)

type countingGCRunner struct {
	calls atomic.Int64
	run   chan struct{}
}

func (r *countingGCRunner) Run(context.Context) (pinning.GCStats, error) {
	r.calls.Add(1)
	r.run <- struct{}{}
	return pinning.GCStats{}, nil
}

func TestRunGCTriggerRunsOnePassForUSR1(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	triggers := make(chan os.Signal, 1)
	runner := &countingGCRunner{run: make(chan struct{}, 1)}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	go runGCTrigger(ctx, runner, triggers, log)

	triggers <- syscall.SIGUSR1
	select {
	case <-runner.run:
	case <-time.After(time.Second):
		t.Fatal("SIGUSR1 did not trigger a GC run")
	}
	if got := runner.calls.Load(); got != 1 {
		t.Fatalf("GC runs = %d, want 1", got)
	}
}
