package pinning

import "sync"

// Gate linearizes publication and pin-ledger transitions with spec 9's GC cut.
// The online collector holds it exclusively only while it reconciles, expires
// staging rows, snapshots pins, and activates a protection epoch. Mark and
// sweep then run concurrently with application traffic; the application
// blockstore's per-key protection supplies T. A legacy collector without epoch
// support retains the older, conservative whole-run exclusion.
//
// # Why this is a lock and not a check
//
// The cut must see each root-changing operation wholly before it or wholly
// after it. Two transitions make that important:
//
//   - A mutation writes blocks bottom-up and swaps the root last (spec 5). If
//     the mutation wins the gate, the cut reconciles its published root into M.
//     If the cut wins, every later block operation goes through the active
//     application view and enters T. Holding the gate across the whole mutation
//     prevents a cut after an unprotected early write but before its root swap.
//   - An ingested blob is unreachable until the refs that name it are applied
//     (spec 7.2 is two calls). PutBlobs stores a staging pin under the same gate:
//     the cut therefore sees that pin in M, or the post-cut write enters T. The
//     staging pin, rather than whole-run exclusion, protects the gap until refs
//     publication makes the blob reachable.
//
// A block-materializing HTTP read takes the read side before choosing its
// immutable root/tip snapshot and releases it after assembling the response but
// before writing to the client. This closes the old-reader gap: a publication
// may replace and unpin that snapshot concurrently, but the next cut cannot
// start until every descendant needed by the response has been read. A read
// admitted after a completed cut runs during the active epoch and protects each
// key through the application view. Reconciliation and publication commits
// take the gate briefly; a plain-blockstore follower also uses it for its
// conservative full-closure proof because that store has no
// collection-generation token.
//
// The zero Gate is ready to use. It is not reentrant.
type Gate struct{ mu sync.RWMutex }

// NewGate returns a Gate. The reconciler makes one for itself if it is not
// given one; a daemon that also wants to gate its mutating requests should take
// that one (Reconciler.Gate) rather than construct a second, since two gates
// exclude nothing.
func NewGate() *Gate { return &Gate{} }

// Enter registers a transition or bounded reader lease which the next GC cut
// must not split. It blocks during that short online cut (or a legacy
// collector's whole run). Every Enter must be paired with a Leave.
func (g *Gate) Enter() { g.mu.RLock() }

// Leave releases one Enter.
func (g *Gate) Leave() { g.mu.RUnlock() }

// Barrier waits for every Enter admitted before it, briefly excludes new
// entrants, and then reopens the gate. It is the publication-to-retirement
// handoff for an external store: after a serving pointer has been replaced,
// Barrier proves that no reader which could have snapshotted the retired
// pointer is still materializing it. The caller may then unpin that retired
// closure without holding the gate across a potentially slow remote RPC.
//
// Go's RWMutex blocks new readers once a writer is waiting, so a stream of new
// requests cannot starve this boundary. Gate is not reentrant; Barrier must not
// be called between Enter and Leave.
func (g *Gate) Barrier() {
	release := g.exclude()
	release()
}

// exclude blocks until every writer inside the gate has left, and returns the
// release. New Enters block from the moment this is called, so a steady stream
// of mutations delays GC by one mutation rather than forever.
func (g *Gate) exclude() func() {
	g.mu.Lock()
	return g.mu.Unlock
}
