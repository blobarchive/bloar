package chain

// This audit test is the flip of the safety boundary reproducer. It once demonstrated
// that the manifest check was point-in-time -- a runtime schedule could diverge
// from the tip in the future, and a tip could change under a running indexer,
// with the process crossing the changed boundary unchecked and its refs bound to
// no tip. Each of those crossings is now closed: an illegal rewrite is rejected
// at publication rather than compared to itself after install; a future-divergent
// config is rejected at startup; and a stale process's refs are refused at commit,
// bound to the tip they were scanned under (spec 10.5).

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ipfs/go-cid"

	"github.com/blobarchive/bloar/index/archclient"
)

func TestManifestBoundaryIsBound(t *testing.T) {
	publishedA := []Source{inboxOpen(testInbox, 0)}
	upgradedB := []Source{
		inboxRange(testInbox, 0, 20),
		inboxOpen(otherAddr, 21),
	}

	// The old source has a real batch at the handoff block. Selecting A records
	// it; selecting the upgraded B schedule does not. This makes both divergence
	// directions observable as rows, not merely as different config structs.
	b := newChainBuilder(t)
	for n := uint64(0); n < 21; n++ {
		b.addBlock(n)
	}
	tx := blobTx(t, keyA, testInbox, 0, hashes(1))
	b.addBlock(21, txEntry{tx: tx, logAddr: testInbox, logTopic: testTopic})
	fc := b.chain()

	t.Run("illegal predecessor rewrite is rejected at publication, never self-compared after install", func(t *testing.T) {
		rewritten := []Source{inboxOpen(otherAddr, 0)}
		if err := ValidateUpgrade(publishedA, rewritten, 10); err == nil {
			t.Fatal("control: rewriting a covered source should be illegal against its predecessor")
		}

		// The preflight validates against the DECODED published predecessor, so the
		// rewrite is refused before any POST -- it never becomes the tip, and the
		// self-compare that once passed after install cannot arise.
		state, client := newAuditManifestArchive(t, publishedA)
		before := state.tip()
		ix := newAuditBoundaryIndexer(t, fc, client, rewritten)
		if _, err := ix.PublishManifest(context.Background(), rewritten); err == nil {
			t.Fatal("PublishManifest accepted an illegal covered-ground rewrite")
		}
		if state.tip() != before {
			t.Fatalf("the illegal rewrite was installed as the tip (was %s, now %s)", before, state.tip())
		}

		// The restart path is closed too: a process configured with the rewrite
		// refuses to run against the legitimate tip, because CheckSchedule is now
		// exact equality, not a self-compare against the current schedule.
		if err := ix.CheckSchedule(context.Background()); err == nil {
			t.Fatal("CheckSchedule accepted a rewrite config against the legitimate tip")
		}
	})

	t.Run("configured future divergence is rejected at startup", func(t *testing.T) {
		_, client := newAuditManifestArchive(t, publishedA)
		ix := newAuditBoundaryIndexer(t, fc, client, upgradedB)

		// upgradedB is a legal FUTURE successor of publishedA, but it is not the tip,
		// so exact-equality startup validation refuses it -- it can never run and
		// cross its own unpublished boundary.
		if err := ix.CheckSchedule(context.Background()); err == nil {
			t.Fatal("CheckSchedule accepted a future-divergent config that does not equal the published tip")
		}
	})

	t.Run("stale process refs after a tip upgrade are refused with no coverage advance", func(t *testing.T) {
		state, client := newAuditManifestArchive(t, publishedA)
		ix := newAuditBoundaryIndexer(t, fc, client, publishedA)

		if err := ix.CheckSchedule(context.Background()); err != nil {
			t.Fatalf("initial CheckSchedule: %v", err)
		}
		// The operator's manifest upgrade lands; the still-running process holds the
		// old tip and scans on.
		state.setManifest(upgradedB)

		advanced, err := ix.Step(context.Background())
		if err == nil {
			t.Fatal("Step advanced across a tip handoff under a stale schedule")
		}
		if advanced {
			t.Fatal("Step reported an advance despite the binding refusal")
		}
		if rows, _ := state.posted(); rows != 0 {
			t.Fatalf("stale refs recorded %d rows across the handoff, want 0", rows)
		}
		if !strings.Contains(err.Error(), "manifest tip advanced") {
			t.Errorf("refusal does not explain the tip handoff: %v", err)
		}
	})
}

// TestManifestPositionRaceReprefligths is the fourth boundary the safety boundary
// remediation closes: a refs commit landing between the append-only preflight and
// the manifest POST, which could let a formerly-legal change rewrite newly-covered
// ground. The generation binding turns it into a 409, and PublishManifest
// re-preflights against the advanced head and succeeds -- no quiescing of refs.
func TestManifestPositionRaceReprefligths(t *testing.T) {
	publishedA := []Source{inboxOpen(testInbox, 0)}
	upgradedB := []Source{inboxRange(testInbox, 0, 20), inboxOpen(otherAddr, 21)}
	fc := buildLinearChain(t, 21)

	state, client := newAuditManifestArchive(t, publishedA)
	ix := newAuditBoundaryIndexer(t, fc, client, upgradedB)
	before := state.tip()

	// Arm a one-shot head-root advance on the first manifest POST: exactly a refs
	// commit landing in the validate->publish gap.
	state.armRace()

	tip, err := ix.PublishManifest(context.Background(), upgradedB)
	if err != nil {
		t.Fatalf("PublishManifest did not recover from the position race: %v", err)
	}
	if tip == "" || tip == before {
		t.Fatalf("publish did not advance the tip: returned %q, was %q", tip, before)
	}
	if tip != state.tip() {
		t.Fatalf("returned tip %q is not the archive's current tip %q", tip, state.tip())
	}
	if n := state.manifestPosts(); n < 2 {
		t.Fatalf("expected a re-preflight (>=2 manifest POST attempts), got %d", n)
	}
}

func newAuditBoundaryIndexer(t *testing.T, fc *fakeChain, client *archclient.Client, sources []Source) *Indexer {
	t.Helper()
	ix, err := New(Config{
		Chain:          fc,
		Archive:        client,
		Head:           "audit",
		AllHead:        "all",
		Sources:        sources,
		GenesisTime:    testGenesis,
		SecondsPerSlot: testSPS,
		BlockRange:     100,
		PollInterval:   time.Millisecond,
	})
	if err != nil {
		t.Fatalf("chain.New: %v", err)
	}
	// The in-memory chain cannot interpret geth's negative finalized RPC tag.
	ix.finalized = new(big.Int).SetUint64(21)
	return ix
}

// auditManifestArchive is a hermetic stand-in for the archive that enforces the
// the safety boundary bindings the real server does: the refs expected_manifest compare and
// the manifest expected_head_root generation compare. It keeps the real
// archclient request encoding, status handling, and JSON decoding on the wire via
// httptest.ResponseRecorder, which opens no listener.
type auditManifestArchive struct {
	mu sync.Mutex

	manifest []Source // the current tip's schedule
	tipCID   string   // the current manifest tip CID
	headRoot string   // the head's current root (its generation id)

	rows         int
	lastExpected string // the expected_manifest of the most recent refs POST
	target       uint64
	postN        int  // manifest POST attempts, for the re-preflight assertion
	manifestGets int  // GET /manifest reads, for the per-poll reread sequencing
	tipSalt      int  // distinguishes a re-encoded tip carrying the same schedule
	race         bool // one-shot: advance headRoot on the next manifest POST
}

// newAuditChainlessArchive is the fake with no published manifest chain: GET
// /manifest 404s and the refs endpoint accepts a batch carrying no
// expected_manifest, exactly as a chainless head does. It is the fixture for the
// exported-boundary guard: a chain indexer that never verified would post
// unattested refs against it.
func newAuditChainlessArchive(t *testing.T) (*auditManifestArchive, *archclient.Client) {
	t.Helper()
	state, client := newAuditManifestArchive(t, nil)
	state.manifest, state.tipCID = nil, ""
	return state, client
}

// republish gives the current schedule a fresh, distinct tip CID without changing
// the schedule itself -- a manifest re-encode whose prev link moved. It is the
// Adoption fixture: the published tip changed, but its schedule still equals
// what the indexer is configured to scan, so the per-poll reread revalidates and
// adopts it.
func (a *auditManifestArchive) republish() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.tipSalt++
	a.tipCID = auditCID(fmt.Sprintf("republish:%d:%v", a.tipSalt, a.manifest))
}

func (a *auditManifestArchive) lastExpectedManifest() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.lastExpected
}

func (a *auditManifestArchive) manifestGetCount() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.manifestGets
}

func newAuditManifestArchive(t *testing.T, manifest []Source) (*auditManifestArchive, *archclient.Client) {
	t.Helper()
	state := &auditManifestArchive{
		manifest: manifest,
		tipCID:   auditTipCID(manifest),
		headRoot: auditCID("root:0"),
	}
	client, err := archclient.New(archclient.Config{
		BaseURL: "http://audit.invalid", Token: "audit",
		HTTPClient:  &http.Client{Transport: auditManifestTransport{state: state}},
		MaxAttempts: 1, Backoff: time.Millisecond,
	})
	if err != nil {
		t.Fatalf("archclient.New: %v", err)
	}
	return state, client
}

type auditManifestTransport struct{ state *auditManifestArchive }

func (tr auditManifestTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	w := httptest.NewRecorder()
	tr.state.serveHTTP(w, r)
	return w.Result(), nil
}

func (a *auditManifestArchive) setManifest(sources []Source) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.manifest, a.tipCID = sources, auditTipCID(sources)
}

func (a *auditManifestArchive) armRace() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.race = true
}

func (a *auditManifestArchive) posted() (int, uint64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.rows, a.target
}

func (a *auditManifestArchive) tip() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.tipCID
}

func (a *auditManifestArchive) manifestPosts() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.postN
}

func (a *auditManifestArchive) serveHTTP(w http.ResponseWriter, r *http.Request) {
	a.mu.Lock()
	defer a.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")

	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/bloar/v1/heads/audit/manifest":
		a.manifestGets++
		if a.tipCID == "" {
			http.Error(w, "head has no manifest chain", http.StatusNotFound)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"cid": a.tipCID,
			"manifest": map[string]any{
				"v": 1, "head": "audit", "prev": nil,
				"sources": auditSourceJSON(a.manifest),
			},
		})
	case r.Method == http.MethodGet && r.URL.Path == "/bloar/v1/heads/audit/synced_to":
		_ = json.NewEncoder(w).Encode(map[string]any{"synced_to": uint64(10)})
	case r.Method == http.MethodGet && r.URL.Path == "/bloar/v1/heads/all/synced_to":
		_ = json.NewEncoder(w).Encode(map[string]any{"synced_to": uint64(21)})
	case r.Method == http.MethodGet && r.URL.Path == "/bloar/v1/heads/audit":
		_ = json.NewEncoder(w).Encode(map[string]any{
			"name": "audit", "root": a.headRoot, "origin_slot": uint64(0), "synced_to": uint64(10),
		})
	case r.Method == http.MethodPost && r.URL.Path == "/bloar/v1/heads/audit/refs":
		a.serveRefs(w, r)
	case r.Method == http.MethodPost && r.URL.Path == "/bloar/v1/heads/audit/manifest":
		a.serveManifest(w, r)
	default:
		http.Error(w, fmt.Sprintf("unexpected %s %s", r.Method, r.URL.Path), http.StatusNotFound)
	}
}

// serveRefs mirrors ApplyRefs's commit-time binding: the batch is recorded only
// if its expected_manifest is the current tip; otherwise a 409 carrying the tip,
// and nothing recorded.
func (a *auditManifestArchive) serveRefs(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Rows             []json.RawMessage `json:"rows"`
		SyncedTo         uint64            `json:"synced_to"`
		ExpectedManifest string            `json:"expected_manifest"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	a.lastExpected = req.ExpectedManifest
	if req.ExpectedManifest != a.tipCID {
		w.WriteHeader(http.StatusConflict)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"code": http.StatusConflict, "message": "manifest tip moved", "manifest_tip": a.tipCID,
		})
		return
	}
	a.rows, a.target = len(req.Rows), req.SyncedTo
	_ = json.NewEncoder(w).Encode(map[string]any{"synced_to": req.SyncedTo, "root": a.headRoot, "noop": false})
}

// serveManifest mirrors SetManifest's prev CAS and generation binding. When armed,
// it first advances the head root -- a refs commit landing in the validate->publish
// gap -- so the incoming expected_head_root is stale and the generation compare
// refuses it, exactly as the position race requires.
func (a *auditManifestArchive) serveManifest(w http.ResponseWriter, r *http.Request) {
	a.postN++
	var req struct {
		Manifest struct {
			Sources []json.RawMessage `json:"sources"`
			Prev    *string           `json:"prev"`
		} `json:"manifest"`
		ExpectedHeadRoot string `json:"expected_head_root"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if a.race {
		a.headRoot = auditCID("root:1")
		a.race = false
	}

	prev := ""
	if req.Manifest.Prev != nil {
		prev = *req.Manifest.Prev
	}
	if prev != a.tipCID {
		a.conflict(w, "prev does not match the tip")
		return
	}
	if req.ExpectedHeadRoot != a.headRoot {
		a.conflict(w, "expected_head_root does not match the head root")
		return
	}
	// Accept: mint a fresh tip from the posted schedule.
	a.tipCID = auditTipCIDFromJSON(req.Manifest.Sources)
	_ = json.NewEncoder(w).Encode(map[string]any{"manifest": a.tipCID})
}

func (a *auditManifestArchive) conflict(w http.ResponseWriter, msg string) {
	w.WriteHeader(http.StatusConflict)
	_ = json.NewEncoder(w).Encode(map[string]any{"code": http.StatusConflict, "message": msg})
}

// auditTipCID derives a deterministic, valid tip CID from a schedule, so the fake
// gives distinct schedules distinct tips without a real DAG encode.
func auditTipCID(sources []Source) string { return auditCID("tip:" + fmt.Sprintf("%v", sources)) }

// auditTipCIDFromJSON derives the same value from the posted JSON sources, so a
// publish and a later tipOf(schedule) agree on the tip.
func auditTipCIDFromJSON(sources []json.RawMessage) string {
	parts := make([]string, len(sources))
	for i, s := range sources {
		parts[i] = string(s)
	}
	return auditCID("tipjson:" + strings.Join(parts, ","))
}

// auditCID builds a valid CIDv1 (dag-cbor, sha2-256) from a seed string, without
// importing the multihash package: 0x12 is the sha2-256 multihash code.
func auditCID(seed string) string {
	c, err := cid.Prefix{Version: 1, Codec: cid.DagCBOR, MhType: 0x12, MhLength: -1}.Sum([]byte(seed))
	if err != nil {
		panic(err)
	}
	return c.String()
}

func auditSourceJSON(sources []Source) []map[string]any {
	out := make([]map[string]any, 0, len(sources))
	for _, s := range sources {
		j := map[string]any{
			"type": s.Type, "address": auditHex(s.Address), "from_block": s.FromBlock,
		}
		switch s.Type {
		case SourceInboxEvents:
			j["topic"] = "0x" + hex.EncodeToString(s.Topic[:])
		case SourceBlobTxs:
			var senders []string
			for _, sender := range s.Senders {
				senders = append(senders, auditHex(sender))
			}
			j["senders"] = senders
		}
		if !s.OpenEnded {
			j["until_block"] = s.UntilBlock
		}
		out = append(out, j)
	}
	return out
}

func auditHex(a common.Address) string { return "0x" + hex.EncodeToString(a[:]) }
