package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"

	"github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	"github.com/ipld/go-ipld-prime/datamodel"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"

	"github.com/blobarchive/bloar/catalog"
	"github.com/blobarchive/bloar/pinning"
	"github.com/blobarchive/bloar/store"
)

// errCorruptBlocksFound and errMissingBlocksFound are what fsck's exit status
// carries when its walk found corrupt or dangling-pin blocks, so the process
// exits nonzero and a monitoring wrapper notices. They are separate: a corrupt
// block is bytes that failed validation, a missing one is a pinned block absent
// locally, and an operator triages them differently. Both are returned only after
// the full per-CID report has been printed.
var (
	errCorruptBlocksFound = errors.New("bloard: fsck found corrupt blocks")
	errMissingBlocksFound = errors.New("bloard: fsck found missing pinned blocks")
)

// fsck validates the multihash of every block reachable from the pins of the
// heads this config knows. It is offline:
// it takes the store's lock, so the daemon must be stopped. Without --repair it
// only reports; with --repair it deletes each corrupt block, turning it into a
// clean miss the operator then refills (a raw blob with `bloard put-block`, an
// index node from a backup or a peer -- see docs/operations.md).
//
// headFilter, when non-empty, scopes the walk to that one head; empty walks
// every head in the config plus the reserved staging head.
func fsck(ctx context.Context, cfg *Config, repair bool, headFilter string, out io.Writer) error {
	log := newLogger()

	// The lock check is the exclusive-ownership guard: --repair must never delete
	// blocks out from under a running daemon, and report-only reads the pin ledger
	// out of the KV, which a live daemon holds anyway. store.Locked answers without
	// opening, so the refusal is clean rather than an opaque lock error from deep
	// inside Pebble's open.
	locked, err := store.Locked(cfg.Store.Path)
	if err != nil {
		return err
	}
	if locked {
		return fmt.Errorf("bloard: the store at %s is locked; a daemon (or another tool) is holding it. fsck needs "+
			"exclusive access -- stop the daemon and retry", cfg.Store.Path)
	}

	names, err := fsckHeads(cfg, headFilter)
	if err != nil {
		return err
	}

	st, err := store.Open(cfg.Store.Path, store.WithPebbleLogger(pebbleLogger{log: log.With("component", "pebble")}))
	if err != nil {
		return err
	}
	defer func() {
		if err := st.Close(); err != nil {
			log.Error("closing store", "err", err)
		}
	}()

	report, err := runFsck(ctx, st, names, repair, out)
	return fsckExit(report, err)
}

// fsckExit is fsck's single exit-status join: it combines the runFsck error --
// which may be a fatal walk error, the pre-mutation discovery-write/flush error
// (empty report), or a post-mutation output error (full report) -- with the
// report's found-status, so that no signal masks another. errors.Join drops nils,
// so a clean run whose runErr is nil returns nil. It is a named helper, and the
// one place fsck joins, precisely so a regression can pin every signal it must
// carry -- the output error, corruption, missing pins, and each delete failure
// with its CID -- rather than have that identity live only inline where a future
// edit could quietly drop one.
func fsckExit(report fsckReport, runErr error) error {
	return errors.Join(runErr, report.exitError())
}

// fsckHeads resolves the head names fsck walks: the one in headFilter if set (and
// known), otherwise every written head, every followed head, and the reserved
// staging head, sorted and deduplicated.
func fsckHeads(cfg *Config, headFilter string) ([]string, error) {
	set := map[string]struct{}{pinning.StagingHead: {}}
	for name := range cfg.Heads {
		set[name] = struct{}{}
	}
	for name := range cfg.Follow.heads() {
		set[name] = struct{}{}
	}
	if headFilter != "" {
		if _, ok := set[headFilter]; !ok && headFilter != pinning.StagingHead {
			return nil, fmt.Errorf("bloard: head %q is not in this config's heads or follow list", headFilter)
		}
		return []string{headFilter}, nil
	}
	return slices.Sorted(maps.Keys(set)), nil
}

// deleteFailure records one corrupt block --repair tried and failed to delete.
type deleteFailure struct {
	cid cid.Cid
	err error
}

// fsckReport is what one fsck run found.
type fsckReport struct {
	// scanned is the distinct blocks the walk read and validated.
	scanned int
	// corrupt is every block whose stored bytes did not hash to its CID; missing
	// is every pinned block absent locally (a dangling pin, a different fault).
	corrupt []cid.Cid
	missing []cid.Cid
	// repaired is the corrupt blocks --repair deleted; deleteFailed is the ones it
	// tried and could not, each still present.
	repaired     []cid.Cid
	deleteFailed []deleteFailure
}

// exitError is fsck's nonzero-exit condition, joining every thing an operator
// must act on: each corrupt block --repair could not delete (named), then that
// corrupt blocks were found at all, then that missing pinned blocks were found.
// nil means a clean store. It is separate from a fatal walk error, which stops
// the run before a report exists.
func (r fsckReport) exitError() error {
	var errs []error
	for _, df := range r.deleteFailed {
		errs = append(errs, fmt.Errorf("bloard: fsck could not delete corrupt block %s: %w", df.cid, df.err))
	}
	if len(r.corrupt) > 0 {
		errs = append(errs, errCorruptBlocksFound)
	}
	if len(r.missing) > 0 {
		errs = append(errs, errMissingBlocksFound)
	}
	return errors.Join(errs...)
}

// fscker walks a store's pinned closure once, validating every block.
type fscker struct {
	bs     blockstore.Blockstore
	report fsckReport
	// validated is every CID whose bytes have been hashed, so a block shared
	// across pins or heads is read once. expanded is every dag-cbor CID whose
	// links have been walked, so a node reachable from two recursive pins is
	// traversed once. The two are deliberately distinct: a node first reached by a
	// DIRECT pin is validated but not expanded, and a later RECURSIVE pin -- in
	// this head or another -- must still expand it and check its descendants.
	// Conflating the two omitted whole subtrees. links
	// caches a validated dag-cbor node's children so that expansion reuses the one
	// read rather than hashing the block twice.
	validated map[string]struct{}
	expanded  map[string]struct{}
	links     map[string][]cid.Cid
}

// runFsck walks the pinned closure of the named heads through the store's
// validating read path, reporting (and, with repair, deleting) the corrupt
// blocks. It is the testable core taking an already-open store, so a test needs
// no config file; fsckCore underneath takes the blockstore and ledger directly so
// a test can inject a delete failure.
func runFsck(ctx context.Context, st *store.Store, headNames []string, repair bool, out io.Writer) (fsckReport, error) {
	return fsckCore(ctx, st.Blocks(), catalog.NewLedger(st.KV()), headNames, repair, out)
}

// fsckCore is runFsck's body over an explicit blockstore and ledger.
//
// # The output-transaction boundary
//
// The discovery inventory is the boundary between "nothing has changed" and "the
// store has been mutated". It is printed -- and its write checked -- BEFORE any
// deletion: a write or flush failure there is fatal, returns with an empty report,
// and attempts ZERO DeleteBlock calls, so a block is never deleted when the
// operator would not have seen what changed. Past that line the store may be
// mutated and an output failure can no longer roll a deletion back, so the mutation
// phase does the opposite: it attempts EVERY deletion, records EVERY per-CID outcome
// in the report, and ACCUMULATES output failures without aborting the loop. The
// returned error is a fatal walk error (before any report), the pre-mutation
// discovery-write error (empty report, nothing deleted), or the accumulated
// post-mutation output error (full report, repairs applied) -- the caller joins it
// with the report's exitError so found-status is never replaced by an output error.
func fsckCore(ctx context.Context, bs blockstore.Blockstore, ledger *catalog.Ledger, headNames []string, repair bool, out io.Writer) (fsckReport, error) {
	f := &fscker{
		bs:        bs,
		validated: map[string]struct{}{},
		expanded:  map[string]struct{}{},
		links:     map[string][]cid.Cid{},
	}
	for _, name := range headNames {
		pins, err := ledger.ListAll(ctx, name)
		if err != nil {
			return fsckReport{}, fmt.Errorf("bloard: fsck reading pins of head %q: %w", name, err)
		}
		if err := f.walkPins(ctx, pins); err != nil {
			return fsckReport{}, err
		}
	}

	// Pre-mutation: the discovery report must be both written AND flushed before a
	// single block is deleted. A buffered writer (bufio.Writer, and the test's) can
	// accept every Write and only fail at Flush, so checking Write alone would let a
	// deletion happen with the inventory still stuck in the buffer -- exactly what
	// this boundary forbids. Either failure is fatal and deletes nothing.
	if err := printFsckDiscovery(out, f.report); err != nil {
		return fsckReport{}, fmt.Errorf("bloard: fsck writing the discovery report (nothing was deleted): %w", err)
	}
	if err := flushWriter(out); err != nil {
		return fsckReport{}, fmt.Errorf("bloard: fsck flushing the discovery report (nothing was deleted): %w", err)
	}
	if !repair {
		return f.report, nil
	}

	// Post-mutation: attempt all deletions, retain every outcome, accumulate output
	// failures. A failed write or flush must not abort the loop or un-delete a block.
	var outErrs []error
	writef := func(format string, args ...any) {
		if _, err := fmt.Fprintf(out, format, args...); err != nil {
			outErrs = append(outErrs, err)
		}
	}
	for _, c := range f.report.corrupt {
		if err := bs.DeleteBlock(ctx, c); err != nil {
			f.report.deleteFailed = append(f.report.deleteFailed, deleteFailure{cid: c, err: err})
			writef("delete-failed\t%s\t%v\n", c, err)
			continue
		}
		f.report.repaired = append(f.report.repaired, c)
		writef("deleted\t%s\n", c)
	}
	writef("deleted: %d corrupt blocks\n", len(f.report.repaired))
	if n := len(f.report.deleteFailed); n > 0 {
		writef("delete-failed: %d\n", n)
	}
	// The final flush is post-mutation too: its failure is accumulated, not fatal.
	if err := flushWriter(out); err != nil {
		outErrs = append(outErrs, err)
	}
	if len(outErrs) > 0 {
		return f.report, fmt.Errorf("bloard: fsck writing the repair report (repairs were still applied): %w", errors.Join(outErrs...))
	}
	return f.report, nil
}

// flushWriter flushes out if it exposes Flush() error (a bufio.Writer, a network
// writer, the fsck tests' writer). A plain io.Writer has nothing buffered and
// flushes to a no-op, so the boundary is the same whether or not out buffers.
func flushWriter(out io.Writer) error {
	if f, ok := out.(interface{ Flush() error }); ok {
		return f.Flush()
	}
	return nil
}

// walkPins validates every pin and walks the closure of the recursive ones. A
// direct pin marks exactly its block; a recursive pin marks everything its links
// reach. Unlike GC's mark, which fails the run on the first unreadable block, this
// records the block and carries on -- fsck exists to report every corrupt block in
// one pass, and a corrupt dag-cbor node whose links it therefore cannot read has
// its subtree skipped (unreadable, and reported for what it is).
func (f *fscker) walkPins(ctx context.Context, pins []catalog.PinEntry) error {
	var frontier []cid.Cid
	for _, p := range pins {
		expandable, err := f.validate(ctx, p.CID)
		if err != nil {
			return err
		}
		// A recursive pin seeds the walk; a direct pin marks only its own block.
		// expandable gates on the block actually being a readable dag-cbor node, so
		// a corrupt or missing recursive pin is reported without a bogus expansion.
		if p.Recursive && expandable {
			frontier = append(frontier, p.CID)
		}
	}
	for len(frontier) > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		c := frontier[len(frontier)-1]
		frontier = frontier[:len(frontier)-1]
		if _, done := f.expanded[c.KeyString()]; done {
			continue
		}
		f.expanded[c.KeyString()] = struct{}{}
		// Its links were cached when it was validated -- every frontier CID is one
		// validate reported expandable -- so expansion reuses that read.
		for _, k := range f.links[c.KeyString()] {
			expandable, err := f.validate(ctx, k)
			if err != nil {
				return err
			}
			if expandable {
				frontier = append(frontier, k)
			}
		}
	}
	return nil
}

// validate reads and hash-checks c once, and for a valid dag-cbor block caches
// its links so a later recursive visit can expand it without a second read. It
// returns whether c is a valid, expandable dag-cbor node. A repeat visit re-derives
// that answer from the cache with no read, which is what lets a direct-pinned node
// still expand when a recursive pin reaches it later. Corruption and absence are
// findings recorded on the report, not errors; only a real I/O failure is an
// error, which stops the whole run.
func (f *fscker) validate(ctx context.Context, c cid.Cid) (expandable bool, err error) {
	if _, done := f.validated[c.KeyString()]; done {
		_, expandable = f.links[c.KeyString()]
		return expandable, nil
	}
	f.validated[c.KeyString()] = struct{}{}
	f.report.scanned++

	blk, err := f.bs.Get(ctx, c)
	switch {
	case err == nil:
		if c.Prefix().Codec != cid.DagCBOR {
			return false, nil // a raw blob leaf: nothing to expand (spec 2).
		}
		kids, derr := dagLinks(blk.RawData())
		if derr != nil {
			return false, fmt.Errorf("bloard: fsck reading links of block %s: %w", c, derr)
		}
		f.links[c.KeyString()] = kids
		return true, nil
	case errors.Is(err, store.ErrCorruptBlock):
		f.report.corrupt = append(f.report.corrupt, c)
		return false, nil
	case ipld.IsNotFound(err):
		f.report.missing = append(f.report.missing, c)
		return false, nil
	default:
		return false, fmt.Errorf("bloard: fsck reading block %s: %w", c, err)
	}
}

// dagLinks decodes a dag-cbor block and returns every CID it links to, the same
// generic traversal GC's mark uses: any link anywhere in the block is a link,
// whatever this build understands the block to mean (spec 2).
func dagLinks(data []byte) ([]cid.Cid, error) {
	nb := basicnode.Prototype.Any.NewBuilder()
	if err := dagcbor.Decode(nb, bytes.NewReader(data)); err != nil {
		return nil, err
	}
	var out []cid.Cid
	if err := appendDagLinks(nb.Build(), &out); err != nil {
		return nil, err
	}
	return out, nil
}

// appendDagLinks collects every link in the subtree rooted at n.
func appendDagLinks(n datamodel.Node, out *[]cid.Cid) error {
	switch n.Kind() {
	case datamodel.Kind_Link:
		l, err := n.AsLink()
		if err != nil {
			return err
		}
		cl, ok := l.(cidlink.Link)
		if !ok {
			return fmt.Errorf("link %s is not a CID link", l)
		}
		*out = append(*out, cl.Cid)
	case datamodel.Kind_Map:
		for it := n.MapIterator(); !it.Done(); {
			_, v, err := it.Next()
			if err != nil {
				return err
			}
			if err := appendDagLinks(v, out); err != nil {
				return err
			}
		}
	case datamodel.Kind_List:
		for it := n.ListIterator(); !it.Done(); {
			_, v, err := it.Next()
			if err != nil {
				return err
			}
			if err := appendDagLinks(v, out); err != nil {
				return err
			}
		}
	}
	return nil
}

// printFsckDiscovery writes the inventory of what the walk found, before any
// repair mutates the store: a per-CID listing of corrupt and missing blocks and
// the counts. It returns the first write error, which fsckCore treats as fatal
// before any mutation -- an operator must never be left having deleted blocks whose
// finding they could not see. The corrupt listing is the part a script greps.
func printFsckDiscovery(out io.Writer, r fsckReport) error {
	write := func(format string, args ...any) error {
		_, err := fmt.Fprintf(out, format, args...)
		return err
	}
	for _, c := range r.corrupt {
		if err := write("corrupt\t%s\n", c); err != nil {
			return err
		}
	}
	for _, c := range r.missing {
		if err := write("missing\t%s\n", c); err != nil {
			return err
		}
	}
	if err := write("scanned: %d blocks\n", r.scanned); err != nil {
		return err
	}
	if err := write("corrupt: %d\n", len(r.corrupt)); err != nil {
		return err
	}
	return write("missing: %d (dangling pins)\n", len(r.missing))
}
