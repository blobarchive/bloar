package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/blobarchive/bloar/census"
	"github.com/blobarchive/bloar/kubo"
	"github.com/blobarchive/bloar/p2p"
)

const censusTransportPreflightTimeout = 30 * time.Second

type kuboTransportFactory struct {
	api                  string
	bearerTokenFile      string
	allowUnauthenticated bool
	allowInsecureHTTP    bool
	requestTimeout       time.Duration
	maxProbeBytes        int64
}

func init() {
	configuredTransport = &kuboTransportFactory{}
}

func (factory *kuboTransportFactory) RegisterFlags(flags *flag.FlagSet) {
	flags.StringVar(&factory.api, "kubo-api", "http://127.0.0.1:5001", "Kubo RPC base URL")
	flags.StringVar(&factory.bearerTokenFile, "kubo-bearer-token-file", "", "file containing the Kubo RPC bearer token")
	flags.BoolVar(&factory.allowUnauthenticated, "kubo-allow-unauthenticated", false, "explicitly allow a credential-free loopback Kubo RPC")
	flags.BoolVar(&factory.allowInsecureHTTP, "kubo-allow-insecure-http", false, "allow bearer authentication over trusted non-loopback HTTP")
	flags.DurationVar(&factory.requestTimeout, "kubo-request-timeout", kubo.DefaultRequestTimeout, "deadline for Kubo preflight metadata RPCs")
	flags.Int64Var(&factory.maxProbeBytes, "max-probe-bytes", 16<<20, "maximum aggregate block bytes accepted from one challenged peer")
}

func (factory *kuboTransportFactory) Open(ctx context.Context, limits census.Limits) (transport, error) {
	if limits.MaxProviders <= 0 || limits.MaxProviders > kubo.MaximumFindProviders {
		return transport{}, fmt.Errorf("kubo transport supports -max-providers between 1 and %d, got %d", kubo.MaximumFindProviders, limits.MaxProviders)
	}
	if factory.maxProbeBytes <= 0 || factory.maxProbeBytes > p2p.MaximumProbeBytes {
		return transport{}, fmt.Errorf("-max-probe-bytes must be between 1 and %d", p2p.MaximumProbeBytes)
	}
	client, err := kubo.New(kubo.Config{
		BaseURL:              factory.api,
		BearerTokenFile:      factory.bearerTokenFile,
		AllowUnauthenticated: factory.allowUnauthenticated,
		AllowInsecureHTTP:    factory.allowInsecureHTTP,
		RequestTimeout:       factory.requestTimeout,
	})
	if err != nil {
		return transport{}, err
	}
	preflightCtx, cancel := context.WithTimeout(ctx, censusTransportPreflightTimeout)
	_, err = client.CheckCensusCompatibility(preflightCtx)
	cancel()
	if err != nil {
		return transport{}, fmt.Errorf("kubo census preflight: %w", err)
	}
	return transport{
		Finder: &kuboFinder{client: client, maxAddressBytes: min(int64(limits.MaxAddressBytes), kubo.MaximumFindProviderAddressBytes)},
		Prober: &targetProber{maxBytes: factory.maxProbeBytes, probe: p2p.ProbePeer},
	}, nil
}

type kuboFinder struct {
	client          *kubo.Client
	maxAddressBytes int64
}

func (finder *kuboFinder) FindProviders(ctx context.Context, rendezvous cid.Cid, limit int) (<-chan peer.AddrInfo, error) {
	if limit <= 0 || limit > kubo.MaximumFindProviders {
		return nil, fmt.Errorf("kubo provider discovery supports a sample of 1..%d peers, got %d", kubo.MaximumFindProviders, limit)
	}
	providers, err := finder.client.FindProviders(ctx, rendezvous, kubo.FindProvidersLimits{
		NumProviders:            limit,
		MaxEvents:               kubo.MaximumFindProviderEvents,
		MaxBytes:                kubo.MaximumFindProviderStreamBytes,
		MaxAddressesPerProvider: kubo.MaximumFindProviderAddresses,
		MaxAddressBytes:         finder.maxAddressBytes,
	})
	if err != nil {
		return nil, err
	}
	stream := make(chan peer.AddrInfo, len(providers))
	for _, provider := range providers {
		stream <- provider
	}
	close(stream)
	return stream, nil
}

type peerProbeFunc func(context.Context, peer.AddrInfo, []cid.Cid, p2p.ProbeLimits) (p2p.PeerProbe, error)

type targetProber struct {
	maxBytes int64
	probe    peerProbeFunc
}

func (prober *targetProber) Probe(ctx context.Context, provider peer.AddrInfo, challenges census.ChallengeSet) (census.ProbeResult, error) {
	targets := make([]cid.Cid, 0, 1+len(challenges.Historical))
	targets = append(targets, challenges.Current)
	targets = append(targets, challenges.Historical...)
	started := time.Now()
	observation, err := prober.probe(ctx, provider, targets, p2p.ProbeLimits{
		MaxCIDs: len(targets), MaxBytes: prober.maxBytes,
	})
	elapsed := time.Since(started)
	result := census.ProbeResult{
		Reachable:    observation.Reachable,
		Historical:   make([]bool, len(challenges.Historical)),
		Path:         censusPath(observation.Path),
		DialLatency:  observation.DialLatency,
		ProbeLatency: elapsed,
	}
	if observation.Peer != provider.ID {
		return result, fmt.Errorf("target probe attributed results to peer %s, want %s", observation.Peer, provider.ID)
	}
	if err != nil {
		return result, err
	}
	var failures []error
	if observation.Err != nil {
		failures = append(failures, observation.Err)
	}
	for index, block := range observation.Blocks {
		if index >= len(targets) {
			failures = append(failures, errors.New("target probe returned more block observations than requested"))
			break
		}
		if !block.CID.Equals(targets[index]) {
			failures = append(failures, fmt.Errorf("target probe result %d is for %s, want %s", index, block.CID, targets[index]))
			continue
		}
		if block.Success {
			if index == 0 {
				result.Current = true
			} else {
				result.Historical[index-1] = true
			}
			continue
		}
		if block.Err != nil {
			failures = append(failures, fmt.Errorf("fetching challenge %s: %w", targets[index], block.Err))
		} else {
			failures = append(failures, fmt.Errorf("challenge %s was not served", targets[index]))
		}
	}
	if len(observation.Blocks) < len(targets) {
		failures = append(failures, fmt.Errorf("target probe returned %d of %d challenge observations", len(observation.Blocks), len(targets)))
	}
	return result, errors.Join(failures...)
}

func censusPath(path string) census.ConnectionPath {
	switch path {
	case p2p.ProbePathDirect:
		return census.PathDirect
	case p2p.ProbePathRelay:
		return census.PathRelay
	default:
		return census.PathUnknown
	}
}
