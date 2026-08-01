package replica

import (
	"context"
	"errors"
	"fmt"
	"time"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"

	"github.com/blobarchive/bloar/kubo"
)

const (
	DefaultPinTimeout       = 24 * time.Hour
	DefaultNamedPinItems    = 128
	DefaultNamedPinBytes    = 1 << 20
	DefaultPinProgressItems = 50_000_000
	DefaultPinProgressBytes = 4 << 30
)

// KuboBackendConfig binds the retention controller to a bounded Kubo client.
// PinTimeout owns the explicit deadline required by archive-scale pin walks;
// zero selects DefaultPinTimeout.
type KuboBackendConfig struct {
	Client            *kubo.Client
	PinTimeout        time.Duration
	NamedPinLimits    kubo.ListLimits
	PinProgressLimits kubo.ListLimits
}

// KuboBackend exposes only the Kubo mutations the replica transaction needs.
// In particular it has no repo GC, block removal, key, publication, or generic
// config method.
type KuboBackend struct {
	client            *kubo.Client
	pinTimeout        time.Duration
	namedPinLimits    kubo.ListLimits
	pinProgressLimits kubo.ListLimits
}

var _ Backend = (*KuboBackend)(nil)

func NewKuboBackend(cfg KuboBackendConfig) (*KuboBackend, error) {
	if cfg.Client == nil {
		return nil, errors.New("replica: Kubo backend requires a client")
	}
	timeout := cfg.PinTimeout
	if timeout == 0 {
		timeout = DefaultPinTimeout
	}
	if timeout <= 0 {
		return nil, errors.New("replica: Kubo pin timeout must be positive")
	}
	named := cfg.NamedPinLimits
	if named == (kubo.ListLimits{}) {
		named = kubo.ListLimits{MaxItems: DefaultNamedPinItems, MaxBytes: DefaultNamedPinBytes}
	}
	progress := cfg.PinProgressLimits
	if progress == (kubo.ListLimits{}) {
		progress = kubo.ListLimits{MaxItems: DefaultPinProgressItems, MaxBytes: DefaultPinProgressBytes}
	}
	if named.MaxItems <= 0 || named.MaxBytes <= 0 || progress.MaxItems <= 0 || progress.MaxBytes <= 0 {
		return nil, errors.New("replica: Kubo pin stream limits must be positive")
	}
	return &KuboBackend{
		client: cfg.Client, pinTimeout: timeout,
		namedPinLimits: named, pinProgressLimits: progress,
	}, nil
}

func (b *KuboBackend) PutBlock(ctx context.Context, block blocks.Block) error {
	_, err := b.client.BlockPut(ctx, block)
	return err
}

func (b *KuboBackend) PinStatus(ctx context.Context, target cid.Cid) (PinStatus, bool, error) {
	info, err := b.client.PinStatus(ctx, target, kubo.PinTypeRecursive)
	if err == nil {
		return PinStatus{Recursive: true, Name: info.Name}, true, nil
	}
	if !errors.Is(err, kubo.ErrNotPinned) {
		return PinStatus{}, false, err
	}
	info, err = b.client.PinStatus(ctx, target, kubo.PinTypeDirect)
	if err == nil {
		return PinStatus{Recursive: false, Name: info.Name}, true, nil
	}
	if errors.Is(err, kubo.ErrNotPinned) {
		return PinStatus{}, false, nil
	}
	return PinStatus{}, false, err
}

func (b *KuboBackend) NamedRecursivePins(ctx context.Context, name string) ([]cid.Cid, error) {
	pins, err := b.client.PinListExactName(ctx, name, b.namedPinLimits)
	if err != nil {
		return nil, err
	}
	result := make([]cid.Cid, 0, len(pins))
	for _, pin := range pins {
		result = append(result, pin.CID)
	}
	return result, nil
}

func (b *KuboBackend) PinAddRecursive(ctx context.Context, target cid.Cid, name string, observe func(PinProgress)) error {
	callCtx, cancel := context.WithTimeout(ctx, b.pinTimeout)
	defer cancel()
	_, err := b.client.PinAddNamedRecursiveProgress(callCtx, target, name, b.pinProgressLimits, func(progress kubo.PinProgress) error {
		if observe != nil {
			observe(PinProgress{Blocks: uint64(progress.Nodes), Bytes: progress.Bytes})
		}
		return nil
	})
	return err
}

func (b *KuboBackend) PinUpdateRecursive(ctx context.Context, old, next cid.Cid, unpin bool) error {
	if unpin {
		return errors.New("replica: Kubo pin update must retain the old generation")
	}
	callCtx, cancel := context.WithTimeout(ctx, b.pinTimeout)
	defer cancel()
	return b.client.PinUpdateAddBeforeRemove(callCtx, old, next)
}

func (b *KuboBackend) PinRemoveRecursive(ctx context.Context, target cid.Cid) error {
	if err := b.client.PinRemove(ctx, target, kubo.PinTypeRecursive); err != nil {
		return fmt.Errorf("removing exact recursive pin: %w", err)
	}
	return nil
}
