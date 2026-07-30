package p2p

import "testing"

func TestProbeResourceBudgetCoversAcceptedAddresses(t *testing.T) {
	cfg := probeHostConfig()
	resources := cfg.ResourceManager

	if resources.OutboundConnections < MaximumProbeAddrs {
		t.Fatalf(
			"probe outbound connection limit = %d, accepts %d addresses",
			resources.OutboundConnections,
			MaximumProbeAddrs,
		)
	}
	if resources.PeerOutboundConnections < MaximumProbeAddrs {
		t.Fatalf(
			"probe per-peer outbound connection limit = %d, accepts %d addresses",
			resources.PeerOutboundConnections,
			MaximumProbeAddrs,
		)
	}
	if resources.Connections < MaximumProbeAddrs+probeConnectionHeadroom {
		t.Fatalf(
			"probe connection limit = %d, want at least %d",
			resources.Connections,
			MaximumProbeAddrs+probeConnectionHeadroom,
		)
	}
	if resources.FileDescriptors < resources.Connections {
		t.Fatalf(
			"probe file descriptor limit = %d, below connection limit %d",
			resources.FileDescriptors,
			resources.Connections,
		)
	}
	if resources.PeerFileDescriptors < MaximumProbeAddrs {
		t.Fatalf(
			"probe per-peer file descriptor limit = %d, accepts %d addresses",
			resources.PeerFileDescriptors,
			MaximumProbeAddrs,
		)
	}
	if err := ValidateResourceControls(cfg); err != nil {
		t.Fatalf("probe resource controls: %v", err)
	}
}
