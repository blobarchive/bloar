package pinning

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/ipfs/go-cid"

	"github.com/blobarchive/bloar/archive"
	"github.com/blobarchive/bloar/catalog"
	"github.com/blobarchive/bloar/metrics"
)

// DefaultInterval is how often the reconciler sweeps every head if the caller
// does not say. Spec 9 requires a timer but does not name a period, and spec 12
// has no key for one: the push trigger is what makes reconciliation prompt, so
// this only has to be often enough to repair a missed push.
const DefaultInterval = 5 * time.Minute

// pinLedger is the ledger the reconciler drives: catalog.Ledger, or a test's
// stand-in for it. Config takes the concrete type -- there is exactly one pin
// ledger and this package does not invite a second -- and this exists so that a
// pass can be cut in half where a crash would cut it.
type pinLedger interface {
	Add(ctx context.Context, head, purpose string, c cid.Cid, recursive bool) error
	Remove(ctx context.Context, head, purpose string, c cid.Cid) error
	ListAll(ctx context.Context, head string) ([]catalog.PinEntry, error)
}

// Config is what a Reconciler needs.
type Config struct {
	// Ledger is the pin ledger of spec 6.2. Required. It is the pin state, not
	// a record of one kept elsewhere; see the package comment.
	Ledger *catalog.Ledger
	// Gate excludes GC. Optional: nil makes one, which callers that also gate
	// their mutations should then read back with Gate().
	Gate *Gate
	// Interval is the timer of spec 9. Zero means DefaultInterval.
	Interval time.Duration
	// ManifestTip returns a head's current manifest-chain tip, or (undef, false)
	// for a head with no chain (spec 10.5). Optional; nil is a node that has no
	// manifest chains to pin. It is read on every pass rather than registered per
	// head because the tip advances out of band -- a writer's manifest POST, a
	// follower's adoption -- and the reconciler is the thing that turns that new
	// tip into a recursive PurposeManifest pin and drops the old one. It reads the
	// same durable tip a restart resumes from (server.ManifestStore), so the pin
	// survives a restart the way a head's root pin does.
	ManifestTip func(ctx context.Context, head string) (cid.Cid, bool, error)
	// Metrics instruments each pass. Optional; nil records nothing.
	Metrics *metrics.Metrics
	// Logger receives what a background pass has to say. Optional.
	Logger *slog.Logger
}

// entry is one head under a policy.
type entry struct {
	head       *archive.Head
	policy     Policy
	withdrawal *withdrawal
}

// withdrawal is the identity of one desired-empty registration. A withdrawal
// cannot delete its map entry immediately: the old ledger rows would then have
// no registered name for reconciliation or GC to drain. The unique token lets
// a completed pass remove only the tombstone it actually reconciled, without
// deleting a newer registration installed under the same name meanwhile.
type withdrawal struct{}

// Delta is what one pass changed. A steady state is the zero Delta: a head
// whose root has not moved reconciles to no writes at all.
type Delta struct {
	Added   int
	Removed int
}

func (d Delta) empty() bool { return d.Added == 0 && d.Removed == 0 }

func (d *Delta) add(o Delta) {
	d.Added += o.Added
	d.Removed += o.Removed
}

// Reconciler keeps the pin ledger equal to what each head's policy asks for
// (spec 9).
//
// # Triggers
//
// Reconciliation runs after every root swap and on a timer. Both arrive here as
// the same thing: Notify marks a head pending, the timer marks every head
// pending, and one loop drains the pending set. That is the coalescing -- a
// burst of applies to one head between two drains is one pass, because a pass
// reconciles against the head's current root rather than against whatever root
// caused the notification.
//
// # Timeliness is not correctness
//
// A pass reads the head's root, then writes the ledger; a mutation in between
// leaves the ledger describing the older root until the next pass. That is
// benign, and not because the window is small: GC is what makes an unreconciled
// ledger dangerous, and GC excludes mutation and flushes reconciliation itself
// before it marks (GC.Run). So the ledger has to be exact when GC looks at it,
// which is arranged, rather than at every instant, which is not.
type Reconciler struct {
	ledger      pinLedger
	gate        *Gate
	interval    time.Duration
	manifestTip func(ctx context.Context, head string) (cid.Cid, bool, error)
	mx          *metrics.Metrics
	log         *slog.Logger

	mu      sync.Mutex
	heads   map[string]entry
	names   []string // sorted
	pending map[string]bool

	// wake carries one bit: "the pending set is not empty". Buffered by one and
	// written non-blockingly, so a notify never waits on the loop.
	wake chan struct{}
}

// NewReconciler returns a Reconciler with no heads. Add them with Add.
func NewReconciler(cfg Config) (*Reconciler, error) {
	if cfg.Ledger == nil {
		return nil, errors.New("pinning: Config.Ledger must not be nil")
	}
	r := &Reconciler{
		ledger:      cfg.Ledger,
		gate:        cfg.Gate,
		interval:    cfg.Interval,
		manifestTip: cfg.ManifestTip,
		mx:          cfg.Metrics,
		log:         cfg.Logger,
		heads:       map[string]entry{},
		pending:     map[string]bool{},
		wake:        make(chan struct{}, 1),
	}
	if r.gate == nil {
		r.gate = NewGate()
	}
	if r.interval == 0 {
		r.interval = DefaultInterval
	}
	// Strictly positive after defaulting: Run's schedule is
	// time.NewTicker(r.interval), which panics on a non-positive value. Zero is the
	// documented default just applied, so a value at or below zero here is the
	// caller's -- rejected at construction rather than as a panic in Run.
	if r.interval <= 0 {
		return nil, fmt.Errorf("pinning: Config.Interval is %s, must be positive", r.interval)
	}
	if r.log == nil {
		r.log = slog.New(slog.DiscardHandler)
	}
	return r, nil
}

// Gate returns the exclusion GC will take. A daemon gates its mutating requests
// with this one.
func (r *Reconciler) Gate() *Gate { return r.gate }

// Add registers a head under a policy. It does not reconcile it: the caller
// decides when the first pass runs, which at startup is before serving.
func (r *Reconciler) Add(head *archive.Head, p Policy) error {
	if head == nil {
		return errors.New("pinning: Add of a nil head")
	}
	if err := p.Validate(); err != nil {
		return err
	}
	name := head.Params().Name
	if err := checkStagingName(name); err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if _, dup := r.heads[name]; dup {
		return fmt.Errorf("pinning: head %q is already registered", name)
	}
	r.heads[name] = entry{head: head, policy: p}
	r.names = slices.Sorted(maps.Keys(r.heads))
	return nil
}

// Replace retargets an already-registered name to a complete replacement
// engine while preserving its retention policy. It is the pointer handoff used
// by a mutable-generation writer after the new root is durable and before the
// server exposes it.
//
// The caller must already hold r.Gate() as part of the encompassing generation
// transition. Replace deliberately does not enter the gate itself because Gate
// is not reentrant. Reconciler.mu makes the pointer swap race-free against a
// background pass; the generation's subsequent root notification converges any
// pass which had already snapshotted the prior engine. GC takes the gate
// exclusively and runs its own reconciliation before its mark cut, so it can
// never collect against that stale snapshot.
func (r *Reconciler) Replace(head *archive.Head) error {
	if head == nil {
		return errors.New("pinning: Replace of a nil head")
	}
	params := head.Params()
	if err := checkStagingName(params.Name); err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	current, ok := r.heads[params.Name]
	if !ok {
		return fmt.Errorf("pinning: head %q is not registered", params.Name)
	}
	old := current.head.Params()
	if params.Net != old.Net || params.SegBits != old.SegBits || params.FanoutBits != old.FanoutBits {
		return fmt.Errorf("pinning: replacement head %q changes immutable parameters: net/seg_bits/fanout_bits (%q/%d/%d -> %q/%d/%d)",
			params.Name, old.Net, old.SegBits, old.FanoutBits, params.Net, params.SegBits, params.FanoutBits)
	}
	current.head = head
	r.heads[params.Name] = current
	return nil
}

// BindReplacement validates one startup registration and returns the
// infallible runtime pointer swap used by server.Heads after a mutable
// generation is durable. Reconciler registrations have no remove operation,
// and the closure preserves the registered policy, so none of the conditions
// checked here can change later.
//
// The server builds every candidate from the registered head's immutable
// parameters and routes the closure by configured head name. Keeping those
// checks at startup is important: a fallible callback after the durable
// generation commit could release the GC gate with the reconciler still
// pointing at the old root.
func (r *Reconciler) BindReplacement(name string) (func(*archive.Head), error) {
	if err := checkStagingName(name); err != nil {
		return nil, err
	}
	r.mu.Lock()
	_, ok := r.heads[name]
	r.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("pinning: head %q is not registered", name)
	}
	return func(head *archive.Head) {
		r.mu.Lock()
		current := r.heads[name]
		current.head = head
		r.heads[name] = current
		r.mu.Unlock()
	}, nil
}

// Names returns the registered head names, sorted.
func (r *Reconciler) Names() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.names)
}

// Notify marks a head for reconciliation. It never blocks and never fails: it
// is called from the mutation path, which must not wait on pinning, and an
// unknown head is a no-op rather than an error the caller has nowhere to put.
func (r *Reconciler) Notify(head string) {
	r.mu.Lock()
	r.pending[head] = true
	r.mu.Unlock()
	select {
	case r.wake <- struct{}{}:
	default: // already awake; the pending set is what carries the work.
	}
}

// markAll marks every head pending.
func (r *Reconciler) markAll() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, n := range r.names {
		r.pending[n] = true
	}
}

// takePending drains the pending set, dropping names that are not heads here.
func (r *Reconciler) takePending() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, 0, len(r.pending))
	for n := range r.pending {
		if _, ok := r.heads[n]; ok {
			out = append(out, n)
		}
	}
	clear(r.pending)
	slices.Sort(out)
	return out
}

// Run reconciles pending heads until ctx is cancelled, on which it returns nil:
// a cancelled daemon is shutting down, not failing.
//
// A pass that fails is logged and left to the timer. Retrying it here would
// spin on whatever is broken, and there is nothing to lose by waiting: an
// un-reconciled ledger over-retains, and GC will reconcile before it marks.
func (r *Reconciler) Run(ctx context.Context) error {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			r.markAll()
		case <-r.wake:
		}

		for _, name := range r.takePending() {
			delta, err := r.ReconcileHead(ctx, name)
			switch {
			case ctx.Err() != nil:
				return nil
			case err != nil:
				r.log.Error("reconciling pins", "head", name, "err", err)
			case !delta.empty():
				r.log.Info("pins reconciled", "head", name, "added", delta.Added, "removed", delta.Removed)
			}
		}
	}
}

// ReconcileAll reconciles every head, under the gate.
func (r *Reconciler) ReconcileAll(ctx context.Context) (Delta, error) {
	r.gate.Enter()
	defer r.gate.Leave()
	return r.reconcileAll(ctx)
}

// ReconcileHead reconciles one head, under the gate.
func (r *Reconciler) ReconcileHead(ctx context.Context, name string) (Delta, error) {
	r.gate.Enter()
	defer r.gate.Leave()
	return r.reconcileHead(ctx, name)
}

// reconcileAll reconciles every head. The caller holds the gate.
func (r *Reconciler) reconcileAll(ctx context.Context) (Delta, error) {
	var total Delta
	for _, name := range r.Names() {
		delta, err := r.reconcileHead(ctx, name)
		if err != nil {
			return total, err
		}
		total.add(delta)
	}
	return total, nil
}

// reconcileHead diffs one head's policy against the ledger and applies the
// difference in the order of spec 6.2: add the missing pins, which is also what
// persists them, then remove the stale ones. The caller holds the gate.
//
// The order is the crash safety. Every block the new root reaches is pinned
// before any pin the old root needed is dropped, so a crash anywhere in here
// leaves a ledger that over-retains -- extra rows naming blocks that are still
// reachable -- and never one that has unpinned something live. The next pass
// converges: the diff is computed from the head, not from what the last pass
// believed.
func (r *Reconciler) reconcileHead(ctx context.Context, name string) (delta Delta, err error) {
	start := time.Now()
	defer func() {
		if err != nil {
			r.mx.ReconcileError(name)
			return
		}
		r.mx.Reconciled(name, delta.Added, delta.Removed, time.Since(start))
	}()

	r.mu.Lock()
	e, ok := r.heads[name]
	r.mu.Unlock()
	if !ok {
		return Delta{}, fmt.Errorf("pinning: head %q is not registered", name)
	}

	var desired []Pin
	if e.withdrawal == nil {
		enum, err := e.head.Enumerate(ctx)
		if err != nil {
			return Delta{}, fmt.Errorf("pinning: enumerating head %q: %w", name, err)
		}
		desired, err = Desired(e.policy, enum)
		if err != nil {
			return Delta{}, fmt.Errorf("pinning: computing pins for head %q: %w", name, err)
		}
		// The manifest tip, if the head has a chain (spec 10.5). It is appended to the
		// desired set rather than computed by Desired because the head's structure does
		// not reach it: a Head does not link its manifest. The rest is the ordinary
		// diff -- an advanced tip is a new recursive pin added and the old one removed,
		// in that order, so the chain is protected throughout the swap.
		if r.manifestTip != nil {
			tip, ok, err := r.manifestTip(ctx, name)
			if err != nil {
				return Delta{}, fmt.Errorf("pinning: reading the manifest tip of head %q: %w", name, err)
			}
			if ok {
				desired = append(desired, Pin{Purpose: PurposeManifest, CID: tip, Recursive: true})
			}
		}
	}
	have, err := r.ledger.ListAll(ctx, name)
	if err != nil {
		return Delta{}, err
	}

	add, remove := plan(desired, have)
	for _, p := range add {
		if err := r.ledger.Add(ctx, name, p.Purpose, p.CID, p.Recursive); err != nil {
			return delta, err
		}
		delta.Added++
	}
	for _, p := range remove {
		if err := r.ledger.Remove(ctx, name, p.Purpose, p.CID); err != nil {
			return delta, err
		}
		delta.Removed++
	}
	if e.withdrawal != nil {
		// Keep a failed withdrawal registered so the timer or the next GC cut can
		// retry the rows it did not remove. A newer registration may have replaced
		// this tombstone while ledger I/O was in flight; compare the token so this
		// completed pass never deletes that newer desired state.
		r.mu.Lock()
		current, ok := r.heads[name]
		if ok && current.withdrawal == e.withdrawal {
			delete(r.heads, name)
			r.names = slices.Sorted(maps.Keys(r.heads))
		}
		r.mu.Unlock()
	}
	// From desired rather than from the ledger: the two agree by the time this
	// returns, and desired is already in hand. A second ListAll would be a read
	// of the whole head's rows on every pass, to publish a number the pass just
	// computed.
	byPurpose := map[string]int{}
	for _, p := range desired {
		byPurpose[p.Purpose]++
	}
	// Every purpose, not just the ones that appear: a head that has just moved
	// from window to full has no window pins, and the gauge has to say 0 rather
	// than keep reporting the last number it saw.
	for _, purpose := range []string{PurposeRoot, PurposeIndex, PurposeWindow, PurposeOpen, PurposeManifest} {
		r.mx.Pins(name, purpose, byPurpose[purpose])
	}
	return delta, nil
}

// pinKey identifies a ledger row. The recursive flag is not part of it: a pin
// that changes from direct to recursive is the same row rewritten, and the
// ledger's Add overwrites it (catalog.Ledger.Add).
type pinKey struct {
	purpose string
	cid     string // cid.KeyString: the CID's bytes, codec included
}

func key(purpose string, c cid.Cid) pinKey { return pinKey{purpose, c.KeyString()} }

// plan is the diff: what to add (a pin that is absent, or present with the
// wrong recursive flag) and what to remove (a row no policy asks for).
func plan(desired []Pin, have []catalog.PinEntry) (add []Pin, remove []Pin) {
	want := make(map[pinKey]Pin, len(desired))
	for _, p := range desired {
		want[key(p.Purpose, p.CID)] = p
	}
	held := make(map[pinKey]bool, len(have))
	for _, e := range have {
		k := key(e.Purpose, e.CID)
		held[k] = true
		p, ok := want[k]
		if !ok {
			remove = append(remove, Pin{Purpose: e.Purpose, CID: e.CID, Recursive: e.Recursive})
			continue
		}
		if p.Recursive != e.Recursive {
			// A direct row where a recursive one is wanted retains too little,
			// so it is rewritten rather than left alone. The other direction is
			// rewritten too: a stale recursive row would keep blobs a slid
			// window has released.
			add = append(add, p)
		}
	}
	for _, p := range desired {
		if !held[key(p.Purpose, p.CID)] {
			add = append(add, p)
		}
	}
	return add, remove
}
