package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"time"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/blobarchive/bloar/kubo"
	"github.com/blobarchive/bloar/p2p"
	"github.com/blobarchive/bloar/replica"
)

type observedRetention struct {
	controller *replica.Controller
	announce   *announcer
	metrics    *replicaMetrics

	activeMu      sync.RWMutex
	active        *replica.Generation
	activeVersion uint64
}

var errNoActiveGeneration = errors.New("replica: no follower generation is active")

func (r *observedRetention) Prepare(ctx context.Context, generation replica.Generation) error {
	err := r.controller.Prepare(ctx, generation)
	var cleanup *replica.CleanupError
	if errors.As(err, &cleanup) {
		r.metrics.recordTransition("cleanup", cleanup)
		r.announce.log.Warn("replica transition left safe cleanup debt", "operation", "prepare", "err", cleanup)
		err = nil
	}
	r.metrics.recordTransition("prepare", err)
	r.metrics.refresh()
	// A failed catch-up does not make the last served generation unsafe, and a
	// successful candidate pin does not prove what the follower currently serves.
	// Only activation and the independent audit write the retention health gate.
	return err
}

func (r *observedRetention) Commit(ctx context.Context, generation replica.Generation) error {
	err := r.controller.Commit(ctx, generation)
	var cleanup *replica.CleanupError
	if errors.As(err, &cleanup) {
		r.metrics.recordTransition("cleanup", cleanup)
		r.announce.log.Warn("replica commit left safe cleanup debt", "err", cleanup)
		err = nil
	}
	// The follower checkpoint is already durable even when controller promotion
	// returns an ordinary error. If the crash-pending generation is still an
	// exact recursive pin, make it the active audit/announcement target and leave
	// the original error visible so the next poll retries the promotion.
	activateErr := r.activate(ctx, generation)
	result := errors.Join(err, activateErr)
	r.metrics.recordTransition("commit", result)
	r.metrics.refresh()
	return result
}

func (r *observedRetention) ProtectsAll(ctx context.Context, heads []replica.Head) error {
	generation, err := r.controller.ProtectsAll(ctx, heads)
	if err != nil {
		r.metrics.recordTransition("protect", err)
		r.clearActive()
		return err
	}
	err = r.activate(ctx, generation)
	r.metrics.recordTransition("protect", err)
	return err
}

func outcome(err error) string {
	if err != nil {
		return "error"
	}
	return "ok"
}

func (r *observedRetention) activate(ctx context.Context, generation replica.Generation) error {
	if err := r.controller.AuditGeneration(ctx, generation); err != nil {
		r.clearActive()
		return err
	}
	if err := r.announce.Update(generation); err != nil {
		r.clearActive()
		return err
	}
	copy := generation
	copy.Heads = slices.Clone(generation.Heads)
	r.activeMu.Lock()
	r.active = &copy
	r.activeVersion++
	r.metrics.health.Set("kubo_replica", true)
	r.activeMu.Unlock()
	return nil
}

func (r *observedRetention) clearActive() {
	r.activeMu.Lock()
	r.active = nil
	r.activeVersion++
	r.metrics.health.Set("kubo_replica", false)
	r.activeMu.Unlock()
}

func (r *observedRetention) auditActive(ctx context.Context) error {
	r.activeMu.RLock()
	version := r.activeVersion
	var generation *replica.Generation
	if r.active != nil {
		copy := *r.active
		copy.Heads = slices.Clone(r.active.Heads)
		generation = &copy
	}
	r.activeMu.RUnlock()
	var err error
	if generation == nil {
		err = errNoActiveGeneration
	} else {
		err = r.controller.AuditGeneration(ctx, *generation)
	}
	r.activeMu.Lock()
	if version == r.activeVersion {
		r.metrics.health.Set("kubo_replica", err == nil)
	}
	r.activeMu.Unlock()
	return err
}

func runRetentionAudit(ctx context.Context, retention *observedRetention, interval time.Duration, metrics *replicaMetrics, log *slog.Logger) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	lastHealthy := false
	first := true
	for {
		timeout := min(30*time.Second, interval)
		if timeout < time.Second {
			timeout = time.Second
		}
		checkCtx, cancel := context.WithTimeout(ctx, timeout)
		err := retention.auditActive(checkCtx)
		cancel()
		healthy := err == nil
		auditOutcome := outcome(err)
		if errors.Is(err, errNoActiveGeneration) {
			auditOutcome = "empty"
		}
		if auditOutcome == "empty" {
			metrics.transitions.WithLabelValues("audit", "empty").Inc()
		} else {
			metrics.recordTransition("audit", err)
		}
		metrics.refresh()
		if !first && healthy != lastHealthy {
			if healthy {
				log.Info("replica retention audit recovered")
			} else {
				log.Error("replica retention audit failed", "err", err)
			}
		} else if first && !healthy && err != nil && !errors.Is(err, errNoActiveGeneration) {
			log.Error("replica retention audit failed", "err", err)
		}
		first = false
		lastHealthy = healthy

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

type kuboPolicyClient interface {
	providePolicyClient
	CheckReplicaCompatibility(context.Context) (kubo.VersionInfo, error)
	CheckReplicaRuntimeCompatibility(context.Context) (kubo.VersionInfo, error)
	ID(context.Context) (kubo.IDInfo, error)
}

func checkKuboCompatibility(ctx context.Context, client kuboPolicyClient, mode string) (kubo.VersionInfo, error) {
	switch mode {
	case providerPolicyCheckRuntime:
		return client.CheckReplicaCompatibility(ctx)
	case providerPolicyCheckExternal:
		return client.CheckReplicaRuntimeCompatibility(ctx)
	default:
		return kubo.VersionInfo{}, fmt.Errorf("unknown provider policy check mode %q", mode)
	}
}

func auditKuboPolicy(ctx context.Context, client kuboPolicyClient, expectedPeer peer.ID, mode string) error {
	if _, err := checkKuboCompatibility(ctx, client, mode); err != nil {
		return err
	}
	if mode == providerPolicyCheckRuntime {
		if err := checkProvidePolicy(ctx, client); err != nil {
			return err
		}
	}
	identity, err := client.ID(ctx)
	if err != nil {
		return err
	}
	if identity.ID != expectedPeer {
		return fmt.Errorf("kubo PeerID changed from %s to %s", expectedPeer, identity.ID)
	}
	return nil
}

// runKuboPolicyAudit detects live daemon/proxy drift after the startup
// preflight. Runtime mode also audits provider policy. External mode cannot:
// its least-privilege bearer deliberately lacks Kubo's path-only config RPC, so
// the native-host preflight is the operator's policy check. Both modes detect a
// Kubo restart replacing the RPC identity beneath durable pin ownership state.
func runKuboPolicyAudit(ctx context.Context, interval time.Duration, client kuboPolicyClient, expectedPeer peer.ID,
	mode string, metrics *replicaMetrics, log *slog.Logger,
) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	lastHealthy := true // startup completed the same checks before this loop.
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		timeout := min(30*time.Second, interval)
		if timeout < time.Second {
			timeout = time.Second
		}
		checkCtx, cancel := context.WithTimeout(ctx, timeout)
		err := auditKuboPolicy(checkCtx, client, expectedPeer, mode)
		cancel()
		if ctx.Err() != nil {
			return
		}
		healthy := err == nil
		metrics.health.Set(kuboRuntimeGate, healthy)
		metrics.recordTransition(runtimeAuditOperation, err)
		if healthy != lastHealthy {
			if healthy {
				log.Info("Kubo runtime audit recovered", "provider_policy_check", mode)
			} else {
				log.Error("Kubo runtime audit failed",
					"provider_policy_check", mode, "err", err)
			}
		}
		lastHealthy = healthy
	}
}

type announcementClient interface {
	ProvideOnce(context.Context, []cid.Cid, kubo.ListLimits) error
	BlockPut(context.Context, blocks.Block) (kubo.BlockStat, error)
	PinAdd(context.Context, cid.Cid, kubo.PinType) error
}

// announcer periodically re-announces only the bounded discovery roots needed
// to discover this replica and enter the retained DAG: stable per-head
// rendezvous CIDs, the generation anchor, head roots, and manifest tips. It
// never recursively announces archive blocks and never reports to a service;
// provide-once writes ordinary provider records into IPFS routing.
type announcer struct {
	client    announcementClient
	replicaID string
	network   string
	interval  time.Duration
	minGap    time.Duration
	timeout   time.Duration
	metrics   *replicaMetrics
	log       *slog.Logger

	mu      sync.RWMutex
	current []cid.Cid
	version uint64
	wake    chan struct{}
}

func newAnnouncer(client announcementClient, replicaID, network string, interval time.Duration, metrics *replicaMetrics, log *slog.Logger) *announcer {
	minGap := min(5*time.Minute, interval/4)
	if minGap < time.Second {
		minGap = time.Second
	}
	timeout := min(2*time.Minute, interval)
	if timeout < time.Second {
		timeout = time.Second
	}
	return &announcer{
		client: client, replicaID: replicaID, network: network, interval: interval, minGap: minGap, timeout: timeout,
		metrics: metrics, log: log, wake: make(chan struct{}, 1),
	}
}

// Initialize materializes and directly pins the tiny deterministic
// rendezvous blocks before any provide/once batch can include them. Kubo 0.42
// refuses to provide a CID absent from its local blockstore, unlike an embedded
// libp2p DHT which can advertise an arbitrary synthetic key. The permanent
// protocol pins are intentionally outside the rotating generation pin: they
// are stable, bounded to one tiny block per configured head, and never removed.
func (a *announcer) Initialize(ctx context.Context, heads []string) error {
	for _, head := range heads {
		block, err := p2p.RendezvousBlock(a.network, head)
		if err != nil {
			return fmt.Errorf("building rendezvous block for head %q: %w", head, err)
		}
		stored, err := a.client.BlockPut(ctx, block)
		if err != nil {
			return fmt.Errorf("storing rendezvous block for head %q: %w", head, err)
		}
		if !stored.CID.Equals(block.Cid()) || stored.Size != int64(len(block.RawData())) {
			return fmt.Errorf("storing rendezvous block for head %q returned key=%s size=%d, want key=%s size=%d",
				head, stored.CID, stored.Size, block.Cid(), len(block.RawData()))
		}
		if err := a.client.PinAdd(ctx, block.Cid(), kubo.PinTypeDirect); err != nil {
			return fmt.Errorf("pinning rendezvous block for head %q: %w", head, err)
		}
	}
	return nil
}

func (a *announcer) Update(generation replica.Generation) error {
	generation.ReplicaID = a.replicaID
	anchor, err := generation.Block()
	if err != nil {
		return fmt.Errorf("building announcement anchor: %w", err)
	}
	targets := []cid.Cid{anchor.Cid()}
	for _, head := range generation.Heads {
		rendezvous, err := p2p.RendezvousCID(a.network, head.Name)
		if err != nil {
			return fmt.Errorf("building rendezvous announcement for head %q: %w", head.Name, err)
		}
		targets = append(targets, rendezvous)
		targets = append(targets, head.Root)
		if head.Manifest.Defined() {
			targets = append(targets, head.Manifest)
		}
	}
	a.setTargets(targets)
	return nil
}

func (a *announcer) setTargets(targets []cid.Cid) {
	a.mu.Lock()
	a.current = canonicalCIDs(targets)
	a.version++
	a.mu.Unlock()
	a.signal()
}

func (a *announcer) signal() {
	select {
	case a.wake <- struct{}{}:
	default:
	}
}

func (a *announcer) Run(ctx context.Context) {
	periodic := time.NewTicker(a.interval)
	defer periodic.Stop()
	var (
		lastAttempt time.Time
		timer       *time.Timer
		timerC      <-chan time.Time
	)
	schedule := func(periodic bool) {
		delay := time.Duration(0)
		if !periodic && !lastAttempt.IsZero() {
			delay = max(0, a.minGap-time.Since(lastAttempt))
		}
		if timer == nil {
			timer = time.NewTimer(delay)
		} else {
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(delay)
		}
		timerC = timer.C
	}
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case <-a.wake:
			if timerC == nil {
				schedule(false)
			}
		case <-periodic.C:
			schedule(true)
		case <-timerC:
			timerC = nil
			targets, version := a.snapshot()
			if len(targets) == 0 {
				continue
			}
			lastAttempt = time.Now()
			callCtx, cancel := context.WithTimeout(ctx, a.timeout)
			err := a.client.ProvideOnce(callCtx, targets, kubo.ListLimits{
				MaxItems: len(targets), MaxBytes: int64(len(targets)) * 1024,
			})
			cancel()
			a.metrics.announcements.WithLabelValues(outcome(err)).Inc()
			if err != nil {
				a.log.Warn("bounded Kubo root announcement failed", "targets", len(targets), "err", err)
				schedule(false)
				continue
			}
			a.metrics.lastAnnouncement.SetToCurrentTime()
			a.log.Info("bounded Kubo roots announced", "targets", len(targets))
			if a.changedSince(version) {
				schedule(false)
			}
		}
	}
}

func (a *announcer) snapshot() ([]cid.Cid, uint64) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return slices.Clone(a.current), a.version
}

func (a *announcer) changedSince(version uint64) bool {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.version != version
}

func canonicalCIDs(values []cid.Cid) []cid.Cid {
	seen := make(map[string]cid.Cid, len(values))
	for _, value := range values {
		if value.Defined() {
			seen[value.KeyString()] = value
		}
	}
	result := make([]cid.Cid, 0, len(seen))
	for _, value := range seen {
		result = append(result, value)
	}
	slices.SortFunc(result, func(left, right cid.Cid) int { return slices.Compare(left.Bytes(), right.Bytes()) })
	return result
}
