package pointerhint

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
)

const (
	DefaultFindMaxResults      = 16
	DefaultFindMaxAddressBytes = 32 << 10
	DefaultFindDialConcurrency = 4
	DefaultFindDialTimeout     = 10 * time.Second
	DefaultFindTimeout         = 30 * time.Second
)

// ProviderFinder is the only DHT query capability Finder accepts. It is kept
// separate from ContentProvider and is never handed to Bitswap.
type ProviderFinder interface {
	FindProvidersAsync(context.Context, cid.Cid, int) <-chan peer.AddrInfo
}

type finderHost interface {
	ID() peer.ID
	Connectedness(peer.ID) network.Connectedness
	Connect(context.Context, peer.AddrInfo) error
}

type libp2pFinderHost struct{ host.Host }

func (h libp2pFinderHost) Connectedness(id peer.ID) network.Connectedness {
	return h.Network().Connectedness(id)
}

// FinderConfig bounds an explicit lookup for one known Pointer.
type FinderConfig struct {
	Router ProviderFinder
	Host   host.Host

	MaxResults int
	// MaxAddressBytes bounds addresses accepted for Host.Connect calls. The
	// underlying DHT implementation may already have decoded or peerstored a
	// provider record before returning it through ProviderFinder.
	MaxAddressBytes int
	// DialConcurrency bounds simultaneous Host.Connect calls. A single libp2p
	// Connect may internally race multiple addresses; its own work remains
	// governed by libp2p's resource manager and DialTimeout.
	DialConcurrency int
	DialTimeout     time.Duration
	FindTimeout     time.Duration
}

type finderSettings struct {
	maxResults      int
	maxAddressBytes int
	dialConcurrency int
	dialTimeout     time.Duration
	findTimeout     time.Duration
}

// Finder connects untrusted provider-record leads for a current pointer already
// selected by the caller's authenticated publication path. Pointer.Kind is not
// itself authentication; see Pointer. Finder does not assert that a connected
// peer still has the block, mutate publication state, or install content
// routing into Bitswap. Stale records therefore cost only bounded accepted-
// address and dial work; a later Bitswap request remains content-addressed to
// the caller-known CID. The underlying DHT may allocate or peerstore provider
// records before this adapter sees them, so MaxAddressBytes is deliberately an
// accepted-dial budget rather than a claim about total DHT intake memory.
type Finder struct {
	router ProviderFinder
	host   finderHost
	cfg    finderSettings
}

// FindResult reports bounded lead processing, not content availability.
type FindResult struct {
	Results          int
	AlreadyConnected int
	Dialed           int
	Connected        int
	DialFailed       int
}

func NewFinder(cfg FinderConfig) (*Finder, error) {
	if cfg.Router == nil {
		return nil, errors.New("pointerhint: FinderConfig.Router must not be nil")
	}
	if cfg.Host == nil {
		return nil, errors.New("pointerhint: FinderConfig.Host must not be nil")
	}
	s, err := finderConfigSettings(cfg)
	if err != nil {
		return nil, err
	}
	return &Finder{router: cfg.Router, host: libp2pFinderHost{Host: cfg.Host}, cfg: s}, nil
}

func finderConfigSettings(cfg FinderConfig) (finderSettings, error) {
	s := finderSettings{
		maxResults:      cfg.MaxResults,
		maxAddressBytes: cfg.MaxAddressBytes,
		dialConcurrency: cfg.DialConcurrency,
		dialTimeout:     cfg.DialTimeout,
		findTimeout:     cfg.FindTimeout,
	}
	if s.maxResults == 0 {
		s.maxResults = DefaultFindMaxResults
	}
	if s.maxAddressBytes == 0 {
		s.maxAddressBytes = DefaultFindMaxAddressBytes
	}
	if s.dialConcurrency == 0 {
		s.dialConcurrency = DefaultFindDialConcurrency
	}
	if s.dialTimeout == 0 {
		s.dialTimeout = DefaultFindDialTimeout
	}
	if s.findTimeout == 0 {
		s.findTimeout = DefaultFindTimeout
	}
	for name, value := range map[string]int{
		"MaxResults":      s.maxResults,
		"MaxAddressBytes": s.maxAddressBytes,
		"DialConcurrency": s.dialConcurrency,
	} {
		if value <= 0 {
			return finderSettings{}, fmt.Errorf("pointerhint: FinderConfig.%s must be positive", name)
		}
	}
	for name, value := range map[string]time.Duration{
		"DialTimeout": s.dialTimeout,
		"FindTimeout": s.findTimeout,
	} {
		if value <= 0 {
			return finderSettings{}, fmt.Errorf("pointerhint: FinderConfig.%s is %s, must be positive", name, value)
		}
	}
	return s, nil
}

// FindAndDial performs one bounded lookup. Pointer validation is what makes
// the query an explicit current root/manifest/document lookup rather than a
// generic block-provider API.
func (f *Finder) FindAndDial(ctx context.Context, pointer Pointer) (FindResult, error) {
	if err := pointer.validate(); err != nil {
		return FindResult{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, f.cfg.findTimeout)
	defer cancel()
	if err := finderContextError(ctx); err != nil {
		return FindResult{}, err
	}
	providers := f.router.FindProvidersAsync(ctx, pointer.CID, f.cfg.maxResults)
	if providers == nil {
		if err := finderContextError(ctx); err != nil {
			return FindResult{}, err
		}
		return FindResult{}, nil
	}

	result := FindResult{}
	// Track address variants rather than only the first PeerID. Provider streams
	// may first yield an addressless or over-budget record and later improve it;
	// that later usable lead must not be suppressed. Repeated identical variants
	// still consume no dial work. The empty sentinel bounds repeated addressless
	// records for test/private routers whose host peerstore already has an addr.
	seenAddresses := make(map[peer.ID]map[string]struct{})
	reportedConnected := make(map[peer.ID]struct{})
	addressBytes := 0
	dialSlots := make(chan struct{}, f.cfg.dialConcurrency)
	var wg sync.WaitGroup
	var mu sync.Mutex
	for result.Results < f.cfg.maxResults {
		select {
		case <-ctx.Done():
			wg.Wait()
			return result, finderContextError(ctx)
		case candidate, ok := <-providers:
			if !ok {
				wg.Wait()
				if err := finderContextError(ctx); err != nil {
					return result, err
				}
				return result, nil
			}
			result.Results++
			if candidate.ID == "" || candidate.ID == f.host.ID() {
				continue
			}
			candidateBytes, fits := providerAddressBytes(candidate, f.cfg.maxAddressBytes-addressBytes)
			if !fits {
				continue
			}
			if f.host.Connectedness(candidate.ID) == network.Connected {
				if _, reported := reportedConnected[candidate.ID]; !reported {
					result.AlreadyConnected++
					reportedConnected[candidate.ID] = struct{}{}
				}
				continue
			}
			fingerprints := providerAddressFingerprints(candidate)
			known := seenAddresses[candidate.ID]
			newVariant := false
			for _, fingerprint := range fingerprints {
				if _, duplicate := known[fingerprint]; !duplicate {
					newVariant = true
					break
				}
			}
			if !newVariant {
				continue
			}
			if known == nil {
				known = make(map[string]struct{}, len(fingerprints))
				seenAddresses[candidate.ID] = known
			}
			for _, fingerprint := range fingerprints {
				known[fingerprint] = struct{}{}
			}
			addressBytes += candidateBytes
			select {
			case dialSlots <- struct{}{}:
			case <-ctx.Done():
				wg.Wait()
				return result, finderContextError(ctx)
			}
			result.Dialed++
			wg.Add(1)
			go func(info peer.AddrInfo) {
				defer wg.Done()
				defer func() { <-dialSlots }()
				dialCtx, dialCancel := context.WithTimeout(ctx, f.cfg.dialTimeout)
				err := f.host.Connect(dialCtx, info)
				dialCancel()
				mu.Lock()
				defer mu.Unlock()
				if err != nil {
					result.DialFailed++
				} else {
					result.Connected++
				}
			}(candidate)
		}
	}
	wg.Wait()
	if err := finderContextError(ctx); err != nil {
		return result, err
	}
	return result, nil
}

func finderContextError(ctx context.Context) error {
	if ctx.Err() == nil {
		return nil
	}
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return context.DeadlineExceeded
	}
	return context.Cause(ctx)
}

func providerAddressBytes(info peer.AddrInfo, remaining int) (int, bool) {
	total := 0
	for _, address := range info.Addrs {
		length := len(address.Bytes())
		if length > remaining-total {
			return 0, false
		}
		total += length
	}
	return total, true
}

func providerAddressFingerprints(info peer.AddrInfo) []string {
	if len(info.Addrs) == 0 {
		return []string{""}
	}
	result := make([]string, 0, len(info.Addrs))
	for _, address := range info.Addrs {
		result = append(result, string(address.Bytes()))
	}
	return result
}
