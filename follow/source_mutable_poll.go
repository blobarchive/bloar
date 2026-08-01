package follow

import (
	"context"
	"fmt"
	"sync"

	"github.com/blobarchive/bloar/server"
)

// mutableHeadPlanResult is one single-authority mutable head's independent
// preflight. Results merge in configured name order after all workers finish.
type mutableHeadPlanResult struct {
	name     string
	plan     adoptPlan
	hasPlan  bool
	winner   *resolved
	err      error
	fatalErr error
}

func (f *Follower) preflightMutableHeads(ctx context.Context, bySource map[string]*resolved) []mutableHeadPlanResult {
	names := make([]string, 0, len(f.cfg.Heads))
	for _, name := range f.Names() {
		if f.expectedKind(name) == server.UnfinalizedMutable {
			names = append(names, name)
		}
	}
	return runMutableHeadWorkers(ctx, names, func(headCtx context.Context, name string) mutableHeadPlanResult {
		return f.preflightMutableHead(headCtx, name, bySource)
	})
}

// runMutableHeadWorkers prevents one slow mutable authority from consuming the
// proof budget before another independently authorized head starts. The source
// roster and configured head map bound worker count; indexed writes are disjoint
// and preserve deterministic merge order.
func runMutableHeadWorkers(ctx context.Context, names []string, work func(context.Context, string) mutableHeadPlanResult) []mutableHeadPlanResult {
	results := make([]mutableHeadPlanResult, len(names))
	var group sync.WaitGroup
	for index, name := range names {
		index, name := index, name
		group.Add(1)
		go func() {
			defer group.Done()
			headCtx, cancel := context.WithTimeout(ctx, docTimeout)
			defer cancel()
			results[index] = work(headCtx, name)
			results[index].name = name
		}()
	}
	group.Wait()
	return results
}

func (f *Follower) preflightMutableHead(ctx context.Context, name string, bySource map[string]*resolved) mutableHeadPlanResult {
	result := mutableHeadPlanResult{name: name}
	source := f.mutableSource(name)
	if source == nil {
		result.fatalErr = fmt.Errorf("follow: mutable head %q has no unique runtime source", name)
		return result
	}
	candidate := bySource[source.cfg.ID]
	if candidate == nil {
		return result // source outage retains the last-good mutable snapshot.
	}
	entry, published := documentHead(candidate.doc, name)
	var (
		plan adoptPlan
		err  error
	)
	if !published {
		plan, err = f.preflightWithdrawal(ctx, name, nil, candidate)
	} else {
		plan, err = f.preflightEntry(ctx, entry, candidate)
	}
	if err != nil {
		result.err = fmt.Errorf("follow: admitting single-authority mutable head %q: %w", name, err)
		return result
	}
	if planHasEffect(plan) {
		result.plan, result.hasPlan, result.winner = plan, true, candidate
	}
	return result
}
