package archive_test

import (
	"math/rand/v2"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/ipfs/go-cid"

	"github.com/blobarchive/bloar/archive"
)

// testSeed returns the seed for a property run, logging it either way. A run
// that fails is reproducible with BLOAR_TEST_SEED=<the logged number>.
func testSeed(t *testing.T) uint64 {
	t.Helper()
	if s := os.Getenv("BLOAR_TEST_SEED"); s != "" {
		v, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			t.Fatalf("BLOAR_TEST_SEED=%q is not a uint64: %v", s, err)
		}
		t.Logf("seed %d (from BLOAR_TEST_SEED)", v)
		return v
	}
	v := uint64(time.Now().UnixNano())
	t.Logf("seed %d (rerun with BLOAR_TEST_SEED=%d)", v, v)
	return v
}

func newRNG(t *testing.T) *rand.Rand {
	t.Helper()
	seed := testSeed(t)
	return rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
}

func testOrd(slot uint64) uint64    { return slot >> testSegBits }
func testWindowEnd(w uint64) uint64 { return ((w + 1) << testSegBits) - 1 }

// genBatch is one generated POST refs call.
type genBatch struct {
	rows     []archive.RefRow
	syncedTo uint64
}

// generateBatches produces a monotonic sequence of ref batches with random
// gaps, empty windows, multi-blob rows, and batch boundaries that sometimes
// land exactly on a window boundary -- the seal path's edge.
func generateBatches(hs *harness, rng *rand.Rand, count int) []genBatch {
	hs.t.Helper()
	batches := make([]genBatch, 0, count)
	cursor := uint64(testOrigin)
	nextVH := uint64(1)

	for range count {
		var syncedTo uint64
		switch rng.IntN(10) {
		case 0, 1, 2:
			// End exactly on a window boundary, sealing whatever it reaches.
			syncedTo = testWindowEnd(testOrd(cursor) + uint64(rng.IntN(3)))
		case 3:
			// A long jump: several windows at once, most of them empty.
			syncedTo = cursor + uint64(rng.IntN(40))
		default:
			syncedTo = cursor + uint64(rng.IntN(16))
		}
		if syncedTo < cursor {
			syncedTo = cursor
		}

		var rows []archive.RefRow
		if rng.IntN(10) > 0 { // one batch in ten only advances coverage
			for slot := cursor; slot <= syncedTo; slot++ {
				if rng.IntN(10) >= 4 {
					continue // a blobless slot: covered, but no row
				}
				ids := make([]uint64, 1+rng.IntN(3))
				for i := range ids {
					ids[i] = nextVH
					nextVH++
				}
				rows = append(rows, hs.row(slot, ids...))
			}
		}
		batches = append(batches, genBatch{rows: rows, syncedTo: syncedTo})
		cursor = syncedTo + 1
	}
	return batches
}

// model is what the head is supposed to hold: the source of truth the property
// assertions compare against.
type model struct {
	byslot   map[uint64][]uint64
	rows     []archive.RefRow
	syncedTo uint64
	covered  bool
}

func newModel() *model {
	return &model{byslot: make(map[uint64][]uint64)}
}

func (m *model) add(b genBatch, ids [][]uint64) {
	for i, r := range b.rows {
		m.byslot[r.Slot] = ids[i]
		m.rows = append(m.rows, r)
	}
	m.syncedTo, m.covered = b.syncedTo, true
}

// vhIDs recovers the blob numbers of a generated row, so the model can assert
// on them.
func vhIDs(rows []archive.RefRow) [][]uint64 {
	out := make([][]uint64, len(rows))
	for i, r := range rows {
		ids := make([]uint64, len(r.VHs))
		for j, vh := range r.VHs {
			// mkVH puts the number in the last 8 bytes, big-endian.
			var n uint64
			for _, b := range vh[24:] {
				n = n<<8 | uint64(b)
			}
			ids[j] = n
		}
		out[i] = ids
	}
	return out
}

// check asserts the head agrees with the model on every slot that exists: the
// inserted ones resolve, the covered-but-blobless ones are provably empty, and
// the uncovered ones are not yet archived.
func (m *model) check(t *testing.T, hs *harness, what string) {
	t.Helper()

	if synced, covered := hs.h.SyncedTo(); covered != m.covered || (covered && synced != m.syncedTo) {
		t.Fatalf("%s: synced_to = (%d, %t), want (%d, %t)", what, synced, covered, m.syncedTo, m.covered)
	}
	if !m.covered {
		return
	}

	for slot := uint64(testOrigin); slot <= m.syncedTo; slot++ {
		ids, inserted := m.byslot[slot]
		if inserted {
			wantBlobs(t, hs.lookup(slot), what+": inserted slot", ids...)
			// Every inserted (slot, vh) resolves to its blob, one at a time and
			// all at once.
			for _, id := range ids {
				wantBlobs(t, hs.lookupVHs(slot, id), what+": inserted (slot, vh)", id)
			}
			wantBlobs(t, hs.lookupVHs(slot, ids...), what+": inserted slot, all vhs", ids...)
			continue
		}
		// Covered and never inserted: provably carries nothing. Unfiltered that
		// is a found-but-empty row (spec 7.1 answers 200 with an empty list);
		// asking for any vh is definitively absent (404).
		wantBlobs(t, hs.lookup(slot), what+": covered blobless slot")
		wantStatus(t, hs.lookupVHs(slot, 0), archive.StatusAbsent, what+": covered blobless slot, filtered")
	}

	// A vh that exists at another slot is still absent here.
	for slot, ids := range m.byslot {
		other := slot + 1
		if _, taken := m.byslot[other]; taken || other > m.syncedTo {
			continue
		}
		wantStatus(t, hs.lookupVHs(other, ids[0]), archive.StatusAbsent, what+": a real vh at the wrong slot")
		break
	}

	wantStatus(t, hs.lookup(m.syncedTo+1), archive.StatusNotYetCovered, what+": one slot past synced_to")
	wantStatus(t, hs.lookup(m.syncedTo+100), archive.StatusNotYetCovered, what+": well past synced_to")
	wantStatus(t, hs.lookup(testOrigin-1), archive.StatusBeforeOrigin, what+": one slot below origin")

	// The directory is exactly as deep as the number of sealed segments
	// requires: every window below the open one is sealed, and depth grows only
	// at a capacity boundary (spec 5.3).
	sealed := testOrd(m.syncedTo+1) - testOrd(testOrigin)
	if got, want := hs.h.Info().DirDepth, canonicalDepth(sealed, testFanoutBits); got != want {
		t.Fatalf("%s: dir_depth = %d with %d sealed segments, want %d", what, got, sealed, want)
	}
}

// TestPropertyRandomBatches is spec 13.4: random monotonic ref batches, and
// after every one of them the whole head is checked against the model.
func TestPropertyRandomBatches(t *testing.T) {
	rng := newRNG(t)
	hs := newHarness(t, testParams())
	batches := generateBatches(hs, rng, 30)

	m := newModel()
	for i, b := range batches {
		res := hs.apply(b.rows, b.syncedTo)
		if res.NoOp {
			t.Fatalf("batch %d reported NoOp while advancing coverage to %d", i, b.syncedTo)
		}
		m.add(b, vhIDs(b.rows))
		m.check(t, hs, "after batch "+strconv.Itoa(i))
		if t.Failed() {
			t.Fatalf("stopping at batch %d; later batches would only repeat the failure", i)
		}
	}

	// The run has to have been interesting: three directory levels means the
	// growth path ran twice.
	if got := hs.h.Info().DirDepth; got < 3 {
		t.Errorf("dir_depth only reached %d; the generated run did not exercise directory growth", got)
	}
}

// independentHarness registers rows' blobs in a fresh catalog backed by a fresh
// blockstore. It is deliberately not buildFresh: that helper shares storage to
// test structural reuse, while the determinism properties need two writers with
// no storage or catalog state in common.
func independentHarness(t *testing.T, params archive.Params, rows []archive.RefRow) *harness {
	t.Helper()
	hs := newHarness(t, params)
	for _, row := range rows {
		for _, vh := range row.VHs {
			hs.cat.add(t, vh)
		}
	}
	return hs
}

func buildIndependent(t *testing.T, params archive.Params, d dataset) cid.Cid {
	t.Helper()
	hs := independentHarness(t, params, d.rows)
	hs.apply(d.rows, d.syncedTo)
	return hs.h.Root()
}

// TestPropertyDeterminism: however and wherever a head was built -- generated
// batches, one batch, or one slot at a time in independent stores -- the same
// logical rows and coverage reach the same root.
func TestPropertyDeterminism(t *testing.T) {
	rng := newRNG(t)
	incremental := newHarness(t, testParams())
	batches := generateBatches(incremental, rng, 25)

	var all []archive.RefRow
	for _, b := range batches {
		incremental.apply(b.rows, b.syncedTo)
		all = append(all, b.rows...)
	}
	want := incremental.h.Root()
	full := dataset{rows: all, syncedTo: batches[len(batches)-1].syncedTo}

	if oneShot := buildIndependent(t, testParams(), full); want != oneShot {
		t.Errorf("root after %d batches = %s, but one batch in an independent store gives %s",
			len(batches), want, oneShot)
	}

	// A third writer chooses boundaries from the rows themselves rather than
	// either the generator's coverage batches or one giant request. Group rows at
	// the same slot so this remains valid if the generator later emits more than
	// one logical row there; a final empty batch carries trailing coverage.
	rowBatches := independentHarness(t, testParams(), all)
	var coveredTo uint64
	covered := false
	for i := 0; i < len(all); {
		j := i + 1
		for j < len(all) && all[j].Slot == all[i].Slot {
			j++
		}
		rowBatches.apply(all[i:j], all[i].Slot)
		coveredTo, covered = all[i].Slot, true
		i = j
	}
	if !covered || coveredTo < full.syncedTo {
		rowBatches.apply(nil, full.syncedTo)
	}
	if got := rowBatches.h.Root(); got != want {
		t.Errorf("root from row-boundary batches in an independent store = %s, want %s", got, want)
	}
}

// TestPropertyTruncateReapply is spec 13.4's seal/truncate/re-apply roundtrip:
// truncate to a random point, and both the truncated head and the head rebuilt
// from it must match a fresh build of the same data.
func TestPropertyTruncateReapply(t *testing.T) {
	rng := newRNG(t)
	ref := newHarness(t, testParams())
	batches := generateBatches(ref, rng, 25)

	var all []archive.RefRow
	for _, b := range batches {
		ref.apply(b.rows, b.syncedTo)
		all = append(all, b.rows...)
	}
	full := dataset{rows: all, syncedTo: batches[len(batches)-1].syncedTo}
	want := ref.h.Root()

	for range 25 {
		slot := testOrigin + uint64(rng.Uint64N(full.syncedTo-testOrigin+1))

		// Rebuild and truncate in storage independent of both the reference writer
		// and the fresh-prefix oracle below. Sharing blocks can make a rebuild look
		// independent when it is only reusing the first writer's DAG.
		hs := independentHarness(t, testParams(), all)
		for _, b := range batches {
			hs.apply(b.rows, b.syncedTo)
		}
		if _, err := hs.h.Truncate(hs.ctx, slot); err != nil {
			t.Fatalf("Truncate(%d): %v", slot, err)
		}

		// Truncating to slot lands exactly where building only that far lands.
		if got, fresh := hs.h.Root(), buildIndependent(t, testParams(), full.upTo(slot)); got != fresh {
			t.Fatalf("truncate to %d: root %s != freshly built %s", slot, got, fresh)
		}

		// And re-applying what was dropped returns to the original root.
		var rest []archive.RefRow
		for _, r := range all {
			if r.Slot > slot {
				rest = append(rest, r)
			}
		}
		hs.apply(rest, full.syncedTo)
		if got := hs.h.Root(); got != want {
			t.Fatalf("truncate to %d then re-apply: root %s, want %s", slot, got, want)
		}
	}
}

// TestPropertyStructuralSharing: extending a head never rewrites what is
// already sealed. Blocks only accumulate, and every root ever published still
// serves the head it named (spec 13.3).
func TestPropertyStructuralSharing(t *testing.T) {
	rng := newRNG(t)
	hs := newHarness(t, testParams())
	batches := generateBatches(hs, rng, 15)

	type published struct {
		root     cid.Cid
		syncedTo uint64
	}
	var roots []published
	blocks := blockCIDs(t, hs.bs)

	for _, b := range batches {
		hs.apply(b.rows, b.syncedTo)

		after := blockCIDs(t, hs.bs)
		for c := range blocks {
			if !after[c] {
				t.Fatalf("block %s vanished: a commit rewrote history in place", c)
			}
		}
		blocks = after
		roots = append(roots, published{hs.h.Root(), b.syncedTo})
	}

	// Every historical root still loads and reports what it did at the time.
	for i, r := range roots {
		old := hs.reload(t, r.root)
		synced, covered := old.h.SyncedTo()
		if !covered || synced != r.syncedTo {
			t.Errorf("root %d reports synced_to (%d, %t), want (%d, true)", i, synced, covered, r.syncedTo)
		}
	}
}
