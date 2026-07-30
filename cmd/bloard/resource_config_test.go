package main

import (
	"strings"
	"testing"
	"time"

	"github.com/blobarchive/bloar/p2p"
)

func TestP2PResourceYAMLMapsEveryExposedLimit(t *testing.T) {
	t.Parallel()
	cfg := loadString(t, limitsConfigBase+`p2p:
  connection_manager:
    low_watermark: 20
    high_watermark: 30
    grace_period: 45s
  resource_manager:
    memory_bytes: 67108864
    file_descriptors: 64
    connections: 40
    inbound_connections: 30
    outbound_connections: 30
    streams: 400
    inbound_streams: 300
    outbound_streams: 300
    peer_connections: 4
    peer_inbound_connections: 3
    peer_outbound_connections: 3
    peer_streams: 40
    peer_inbound_streams: 30
    peer_outbound_streams: 30
    peer_memory_bytes: 8388608
    peer_file_descriptors: 8
`)

	wantConnections := p2p.ConnectionManagerConfig{LowWatermark: 20, HighWatermark: 30, GracePeriod: 45 * time.Second}
	wantResources := p2p.ResourceManagerConfig{
		MemoryBytes: 67108864, FileDescriptors: 64,
		Connections: 40, InboundConnections: 30, OutboundConnections: 30,
		Streams: 400, InboundStreams: 300, OutboundStreams: 300,
		PeerConnections: 4, PeerInboundConnections: 3, PeerOutboundConnections: 3,
		PeerStreams: 40, PeerInboundStreams: 30, PeerOutboundStreams: 30,
		PeerMemoryBytes: 8388608, PeerFileDescriptors: 8,
	}
	got, err := cfg.P2P.hostConfig(nil, nil)
	if err != nil {
		t.Fatalf("hostConfig: %v", err)
	}
	if got.ConnectionManager != wantConnections {
		t.Fatalf("connection manager mapping = %+v, want %+v", got.ConnectionManager, wantConnections)
	}
	if got.ResourceManager != wantResources {
		t.Fatalf("resource manager mapping = %+v, want %+v", got.ResourceManager, wantResources)
	}
}

func TestP2PResourceYAMLRejectsInvalidPolicyBeforeStartup(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, block, want string
	}{
		{
			name:  "low above high",
			block: "  connection_manager: {low_watermark: 20, high_watermark: 10}\n",
			want:  "exceeds high_watermark",
		},
		{
			name:  "negative grace",
			block: "  connection_manager: {grace_period: -1s}\n",
			want:  "grace_period must be positive",
		},
		{
			name: "hard connection limit below pruning high",
			block: "  connection_manager: {low_watermark: 4, high_watermark: 8}\n" +
				"  resource_manager: {connections: 7, inbound_connections: 7, outbound_connections: 7, file_descriptors: 7, peer_connections: 1, peer_inbound_connections: 1, peer_outbound_connections: 1, peer_file_descriptors: 1}\n",
			want: "high_watermark (8) exceeds",
		},
		{
			name:  "unknown nested field",
			block: "  resource_manager: {connections: 256, peer_id_label: true}\n",
			want:  "field peer_id_label not found",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			yaml := limitsConfigBase + "p2p:\n" + tc.block
			_, err := LoadConfig(writeFile(t, "config.yaml", yaml))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("LoadConfig error = %v, want mention %q", err, tc.want)
			}
		})
	}
}
