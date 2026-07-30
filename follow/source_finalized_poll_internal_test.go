package follow

import (
	"context"
	"testing"
	"time"
)

func TestFinalizedHeadWorkersDoNotStarveLaterHead(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()

	alphaStarted := make(chan struct{})
	betaFinished := make(chan struct{})
	releaseAlpha := make(chan struct{})
	done := make(chan []finalizedHeadPlanResult, 1)
	go func() {
		done <- runFinalizedHeadWorkers(ctx, []string{"alpha", "beta"}, func(_ context.Context, name string) finalizedHeadPlanResult {
			switch name {
			case "alpha":
				close(alphaStarted)
				<-releaseAlpha
			case "beta":
				close(betaFinished)
			}
			return finalizedHeadPlanResult{plan: adoptPlan{name: "plan-" + name}, hasPlan: true}
		})
	}()

	select {
	case <-alphaStarted:
	case <-ctx.Done():
		t.Fatal("alpha worker did not start")
	}
	select {
	case <-betaFinished:
		// The later sorted head completed while alpha remained blocked.
	case <-ctx.Done():
		t.Fatal("beta was starved behind the blocked alpha proof")
	}
	close(releaseAlpha)

	var results []finalizedHeadPlanResult
	select {
	case results = <-done:
	case <-ctx.Done():
		t.Fatal("finalized-head workers did not finish")
	}
	if len(results) != 2 || results[0].name != "alpha" || results[0].plan.name != "plan-alpha" ||
		results[1].name != "beta" || results[1].plan.name != "plan-beta" {
		t.Fatalf("results = %+v, want deterministic alpha/beta merge order", results)
	}
}
