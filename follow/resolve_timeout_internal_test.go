package follow

import (
	"testing"
	"time"
)

// TestPublicationResolutionBudgetCoversColdDHT pins the production finding
// that a healthy first Amino-DHT IPNS lookup may take longer than thirty
// seconds. Repeatedly cancelling below this floor can prevent a cold follower
// from ever reaching document discovery even though later layers are healthy.
func TestPublicationResolutionBudgetCoversColdDHT(t *testing.T) {
	if docTimeout < 2*time.Minute {
		t.Fatalf("publication resolution timeout = %s, want at least 2m for a cold public-DHT lookup", docTimeout)
	}
}
