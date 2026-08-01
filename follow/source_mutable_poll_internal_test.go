package follow

import (
	"context"
	"testing"
	"time"
)

func TestMutableHeadWorkersDoNotStarveLaterHead(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()

	alphaStarted := make(chan struct{})
	betaFinished := make(chan struct{})
	releaseAlpha := make(chan struct{})
	done := make(chan []mutableHeadPlanResult, 1)
	go func() {
		done <- runMutableHeadWorkers(ctx, []string{"alpha", "beta"}, func(_ context.Context, name string) mutableHeadPlanResult {
			switch name {
			case "alpha":
				close(alphaStarted)
				<-releaseAlpha
			case "beta":
				close(betaFinished)
			}
			return mutableHeadPlanResult{plan: adoptPlan{name: "plan-" + name}, hasPlan: true}
		})
	}()

	select {
	case <-alphaStarted:
	case <-ctx.Done():
		t.Fatal("alpha worker did not start")
	}
	select {
	case <-betaFinished:
		// The later sorted mutable head completed while alpha remained blocked.
	case <-ctx.Done():
		t.Fatal("beta was starved behind the blocked alpha mutable proof")
	}
	close(releaseAlpha)

	var results []mutableHeadPlanResult
	select {
	case results = <-done:
	case <-ctx.Done():
		t.Fatal("mutable-head workers did not finish")
	}
	if len(results) != 2 || results[0].name != "alpha" || results[0].plan.name != "plan-alpha" ||
		results[1].name != "beta" || results[1].plan.name != "plan-beta" {
		t.Fatalf("results = %+v, want deterministic alpha/beta merge order", results)
	}
}
