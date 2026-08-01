package follow

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ipfs/go-cid"

	"github.com/blobarchive/bloar/metrics"
)

// Poll runs one complete library cycle: resolve, verify, adopt, then
// synchronously replicate every followed head's retained closure.
//
// The synchronous contract is retained for direct callers and tests. The
// daemon uses the same admission and sync phases through RunAfterResume, but
// schedules closure work on one coalescing background worker so a long walk
// cannot delay the next publication admission.
//
// The two phases remain independent on purpose. Resolution can fail -- an
// unreachable writer, a document that does not verify -- and the fetch pass
// still runs, because unfinished backfill is useful work on the last durable
// snapshot. The reverse holds too: a head that fails to sync does not stop a
// newer authenticated document being admitted.
func (f *Follower) Poll(ctx context.Context) error {
	admissionErr := f.pollAdmission(ctx)
	syncErr := f.syncCurrent(ctx)
	return errors.Join(admissionErr, syncErr)
}

// pollAdmission runs only the authority and atomic-registry half of a follower
// cycle. Everything which makes a publication safe to expose remains inside
// this phase: structural loading, transition serialization, checkpoint
// durability, active-GC closure protection, retention preparation/commit, and
// the registry's atomic batch adoption.
func (f *Follower) pollAdmission(ctx context.Context) error {
	started := time.Now()
	var err error
	if f.cfg.SourceSet != nil {
		err = f.pollSourceSetAdmission(ctx)
	} else {
		err = f.pollSingularAdmission(ctx)
	}
	outcome := metrics.OutcomeOK
	if err != nil {
		outcome = metrics.OutcomeError
	}
	f.cfg.Metrics.FollowAdmission(outcome, time.Since(started), time.Now())
	return err
}

type headSyncSnapshot struct {
	ok                                  bool
	quarantined                         bool
	root, fetched, tip, manifestFetched cid.Cid
	completions                         uint64
}

func (s headSyncSnapshot) pending() bool {
	return s.ok && !s.quarantined && s.root.Defined() &&
		(s.root != s.fetched || s.tip.Defined() && s.tip != s.manifestFetched)
}

func (f *Follower) headSyncSnapshot(name string) headSyncSnapshot {
	f.mu.Lock()
	defer f.mu.Unlock()
	hs, ok := f.heads[name]
	if !ok || hs == nil {
		return headSyncSnapshot{}
	}
	return headSyncSnapshot{
		ok: true, quarantined: hs.quarantined,
		root: hs.adopted, fetched: hs.fetched,
		tip: hs.manifestTip, manifestFetched: hs.manifestFetched,
		completions: hs.syncCompletions,
	}
}

// syncCurrent owns the single permit shared by the daemon worker and exported
// Poll calls. Names are still processed in deterministic order, preserving the
// old bounded one-head-at-a-time I/O shape.
func (f *Follower) syncCurrent(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-f.syncPermit:
	}
	f.cfg.Metrics.FollowSyncActive(true)
	defer func() {
		f.cfg.Metrics.FollowSyncActive(false)
		f.syncPermit <- struct{}{}
	}()

	var errs []error
	for _, name := range f.Names() {
		before := f.headSyncSnapshot(name)
		started := time.Now()
		err := f.syncWithPointer(ctx, name)
		after := f.headSyncSnapshot(name)

		outcome := metrics.FollowSyncNoop
		switch {
		case err != nil:
			outcome = metrics.OutcomeError
		case after.completions != before.completions:
			outcome = metrics.FollowSyncCompleted
		case before.pending() || after.pending():
			// The pass had work, or a concurrent adoption installed work while
			// it ran, but this attempt did not stamp a generation complete.
			outcome = metrics.FollowSyncSuperseded
		}
		f.cfg.Metrics.FollowSync(name, outcome, time.Since(started), time.Now())

		if err != nil {
			if ctx.Err() != nil {
				break
			}
			errs = append(errs, fmt.Errorf("follow: syncing head %q: %w", name, err))
		}
	}
	return errors.Join(errs...)
}

// RunAfterResume polls immediately and then on every configured interval. It
// is the daemon integration seam for callers which must restore durable heads
// before opening their public listener: call Resume, establish the listener,
// then enter RunAfterResume. Most callers should use Run, which preserves the
// same ordering internally.
func (f *Follower) RunAfterResume(ctx context.Context) error {
	ticker := time.NewTicker(f.cfg.PollInterval)
	defer ticker.Stop()
	return f.runAfterResume(ctx, ticker.C)
}

// runAfterResume is split from ticker construction so deterministic tests can
// inject poll boundaries without sleeping. One buffered wake is a dirty bit:
// it says useful sync work may exist, never which historical revision to run.
func (f *Follower) runAfterResume(ctx context.Context, ticks <-chan time.Time) error {
	runCtx, cancel := context.WithCancel(ctx)
	wake := make(chan struct{}, 1)
	workerDone := make(chan struct{})
	go func() {
		defer close(workerDone)
		f.runSyncWorker(runCtx, wake)
	}()
	defer func() {
		cancel()
		<-workerDone
	}()

	// Resume may have exposed a durable but incompletely replicated generation.
	// Start that useful work without waiting for the first network resolution.
	f.requestSync(wake)

	for {
		err := f.pollAdmission(runCtx)
		if runCtx.Err() != nil {
			return nil
		}
		// Even a failed resolution should retry closure work on the last-good
		// generation. Successful admissions use the same signal.
		f.requestSync(wake)
		if err != nil {
			f.log.Error("polling the publication document", "url", f.cfg.URL, "ipns", f.cfg.IPNS, "err", err)
		}

		select {
		case <-runCtx.Done():
			return nil
		case _, ok := <-ticks:
			if !ok {
				return nil
			}
		}
	}
}

func (f *Follower) requestSync(wake chan<- struct{}) {
	select {
	case wake <- struct{}{}:
	default:
		f.cfg.Metrics.FollowSyncCoalesced()
	}
}

func (f *Follower) runSyncWorker(ctx context.Context, wake <-chan struct{}) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-wake:
		}
		if err := f.syncCurrent(ctx); err != nil {
			if ctx.Err() != nil {
				return
			}
			f.log.Error("syncing followed head closures", "err", err)
		}
	}
}
