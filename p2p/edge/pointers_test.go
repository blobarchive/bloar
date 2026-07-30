package edge

import (
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"

	"github.com/blobarchive/bloar/p2p"
	"github.com/blobarchive/bloar/p2p/pointerhint"
	"github.com/blobarchive/bloar/server"
)

func TestPointerStatePlansPureSnapshotAndCommitsOneDocument(t *testing.T) {
	root := pointerTestCID(t, cid.DagCBOR, "shared root")
	manifest := pointerTestCID(t, cid.DagCBOR, "manifest")
	synced := uint64(12)
	claim := server.Doc{Unsigned: server.Unsigned{
		Heads: []server.HeadEntry{
			{Name: "alpha", Root: root.String(), Manifest: manifest.String(), SyncedTo: &synced},
			{Name: "beta", Root: root.String(), SyncedTo: &synced},
			// A root with no coverage is not a current pointer.
			{Name: "withdrawn", Root: root.String()},
		},
	}}
	document := pointerDocumentBlock(t, claim)
	events := []string{}
	schedule := &pointerTestSchedule{events: &events}
	documents := &pointerTestDocuments{events: &events, staged: make(map[string]blocks.Block)}
	state, err := newPointerState(schedule, documents)
	if err != nil {
		t.Fatal(err)
	}

	plan, err := state.PlanAuthenticated(document, claim)
	if err != nil {
		t.Fatalf("PlanAuthenticated: %v", err)
	}
	if got, want := events, []string{"validate"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("preflight events = %v, want %v", got, want)
	}
	if len(schedule.replaceCalls) != 0 || len(documents.stageCalls) != 0 {
		t.Fatal("pure pointer preflight changed schedule or document state")
	}
	validated := schedule.validateCalls[0]
	if got := validated.heads["alpha"]; !got.Root.Equals(root) || !got.Manifest.Equals(manifest) || got.Document.Defined() {
		t.Fatalf("alpha pointers = %#v, want root+manifest and no per-head document", got)
	}
	if got := validated.heads["beta"]; !got.Root.Equals(root) || got.Manifest.Defined() || got.Document.Defined() {
		t.Fatalf("beta pointers = %#v, want shared root only", got)
	}
	if got := validated.heads["withdrawn"]; got.Root.Defined() || got.Manifest.Defined() || got.Document.Defined() {
		t.Fatalf("withdrawn pointers = %#v, want empty set", got)
	}
	if got := validated.documents; len(got) != 1 || !got[0].Equals(document.Cid()) {
		t.Fatalf("extra documents = %v, want only %s", got, document.Cid())
	}

	if err := plan.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if got, want := events, []string{"validate", "stage", "replace", "documents"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("commit events = %v, want %v", got, want)
	}
	if got := documents.active; len(got) != 1 || !got[0].Equals(document.Cid()) {
		t.Fatalf("active documents = %v, want only %s", got, document.Cid())
	}

	nextClaim := server.Doc{Unsigned: server.Unsigned{Heads: []server.HeadEntry{{
		Name: "alpha", Root: root.String(), SyncedTo: &synced,
	}}}}
	nextDocument := pointerDocumentBlock(t, nextClaim)
	next, err := state.PlanAuthenticated(nextDocument, nextClaim)
	if err != nil {
		t.Fatal(err)
	}
	if err := next.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, present := schedule.currentHeads["beta"]; present {
		t.Fatal("complete replacement retained omitted optional head beta")
	}
	if _, present := schedule.currentHeads["withdrawn"]; present {
		t.Fatal("complete replacement retained omitted withdrawn head")
	}
	if got := documents.active; len(got) != 1 || !got[0].Equals(nextDocument.Cid()) {
		t.Fatalf("rotated active documents = %v, want only %s", got, nextDocument.Cid())
	}
}

func TestPointerStatePreflightBindsBytesAndHasNoSideEffects(t *testing.T) {
	root := pointerTestCID(t, cid.DagCBOR, "root")
	synced := uint64(4)
	claim := server.Doc{Unsigned: server.Unsigned{Heads: []server.HeadEntry{{
		Name: "alpha", Root: root.String(), SyncedTo: &synced,
	}}}}
	document := pointerDocumentBlock(t, claim)

	schedule := &pointerTestSchedule{validateErr: errors.New("injected exact-snapshot rejection")}
	documents := &pointerTestDocuments{staged: make(map[string]blocks.Block)}
	state, err := newPointerState(schedule, documents)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := state.PlanAuthenticated(document, claim); err == nil ||
		!strings.Contains(err.Error(), "injected exact-snapshot rejection") {
		t.Fatalf("PlanAuthenticated error = %v", err)
	}
	if len(schedule.replaceCalls) != 0 || len(documents.stageCalls) != 0 {
		t.Fatal("rejected preflight changed schedule or document state")
	}

	changed := claim
	changed.Heads = append([]server.HeadEntry(nil), claim.Heads...)
	changed.Heads[0].Name = "other"
	schedule.validateErr = nil
	if _, err := state.PlanAuthenticated(document, changed); err == nil ||
		!strings.Contains(err.Error(), "does not match publication document bytes") {
		t.Fatalf("mismatched claim error = %v", err)
	}
	if len(schedule.validateCalls) != 1 {
		t.Fatalf("mismatched bytes reached schedule validation: %d calls", len(schedule.validateCalls))
	}
}

func TestPointerStatePostCommitFailureWithdrawsStaleSchedule(t *testing.T) {
	firstClaim := pointerClaim("alpha", pointerTestCID(t, cid.DagCBOR, "old root"))
	firstDocument := pointerDocumentBlock(t, firstClaim)
	schedule := &pointerTestSchedule{}
	documents := &pointerTestDocuments{staged: make(map[string]blocks.Block)}
	state, err := newPointerState(schedule, documents)
	if err != nil {
		t.Fatal(err)
	}
	first, err := state.PlanAuthenticated(firstDocument, firstClaim)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Commit(); err != nil {
		t.Fatal(err)
	}

	nextClaim := pointerClaim("alpha", pointerTestCID(t, cid.DagCBOR, "new root"))
	nextDocument := pointerDocumentBlock(t, nextClaim)
	next, err := state.PlanAuthenticated(nextDocument, nextClaim)
	if err != nil {
		t.Fatal(err)
	}
	schedule.replaceErrors = []error{errors.New("injected replacement failure"), nil}
	if err := next.Commit(); err == nil || !strings.Contains(err.Error(), "injected replacement failure") {
		t.Fatalf("Commit error = %v", err)
	}
	if schedule.currentHeads != nil || schedule.currentDocuments != nil {
		t.Fatalf("failed post-commit replacement retained stale schedule: heads=%v documents=%v",
			schedule.currentHeads, schedule.currentDocuments)
	}
	if documents.active != nil {
		t.Fatalf("failed post-commit replacement retained current documents: %v", documents.active)
	}
	if state.current != nil {
		t.Fatal("failed post-commit replacement retained a local current snapshot")
	}
	last := schedule.replaceCalls[len(schedule.replaceCalls)-1]
	if last.heads != nil || last.documents != nil {
		t.Fatalf("final replacement was not fail-closed withdrawal: %#v", last)
	}
}

func TestPointerStateDocumentCommitFailureWithdrawsInstalledSchedule(t *testing.T) {
	claim := pointerClaim("alpha", pointerTestCID(t, cid.DagCBOR, "root"))
	document := pointerDocumentBlock(t, claim)
	schedule := &pointerTestSchedule{}
	documents := &pointerTestDocuments{
		staged:        make(map[string]blocks.Block),
		replaceErrors: []error{errors.New("injected document commit failure"), nil},
	}
	state, err := newPointerState(schedule, documents)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := state.PlanAuthenticated(document, claim)
	if err != nil {
		t.Fatal(err)
	}
	if err := plan.Commit(); err == nil || !strings.Contains(err.Error(), "injected document commit failure") {
		t.Fatalf("Commit error = %v", err)
	}
	if schedule.currentHeads != nil || schedule.currentDocuments != nil || documents.active != nil {
		t.Fatalf("document commit failure did not withdraw: schedule=%v/%v documents=%v",
			schedule.currentHeads, schedule.currentDocuments, documents.active)
	}
	if got := len(schedule.replaceCalls); got != 2 {
		t.Fatalf("schedule replacements = %d, want install then withdrawal", got)
	}
}

func TestPointerStateStageFailureWithdrawsPreviousSchedule(t *testing.T) {
	firstClaim := pointerClaim("alpha", pointerTestCID(t, cid.DagCBOR, "old root"))
	firstDocument := pointerDocumentBlock(t, firstClaim)
	schedule := &pointerTestSchedule{}
	documents := &pointerTestDocuments{staged: make(map[string]blocks.Block)}
	state, err := newPointerState(schedule, documents)
	if err != nil {
		t.Fatal(err)
	}
	first, err := state.PlanAuthenticated(firstDocument, firstClaim)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Commit(); err != nil {
		t.Fatal(err)
	}

	nextClaim := pointerClaim("alpha", pointerTestCID(t, cid.DagCBOR, "new root"))
	next, err := state.PlanAuthenticated(pointerDocumentBlock(t, nextClaim), nextClaim)
	if err != nil {
		t.Fatal(err)
	}
	documents.stageErrors = []error{errors.New("injected stage failure")}
	if err := next.Commit(); err == nil || !strings.Contains(err.Error(), "injected stage failure") {
		t.Fatalf("Commit error = %v", err)
	}
	if schedule.currentHeads != nil || schedule.currentDocuments != nil || documents.active != nil || state.current != nil {
		t.Fatalf("stage failure retained stale state: schedule=%v/%v documents=%v current=%v",
			schedule.currentHeads, schedule.currentDocuments, documents.active, state.current)
	}
}

func TestPointerStateReportsFailedFailClosedWithdrawal(t *testing.T) {
	claim := pointerClaim("alpha", pointerTestCID(t, cid.DagCBOR, "root"))
	document := pointerDocumentBlock(t, claim)
	schedule := &pointerTestSchedule{
		replaceErrors: []error{
			errors.New("injected replacement failure"),
			errors.New("injected withdrawal failure"),
		},
	}
	documents := &pointerTestDocuments{staged: make(map[string]blocks.Block)}
	state, err := newPointerState(schedule, documents)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := state.PlanAuthenticated(document, claim)
	if err != nil {
		t.Fatal(err)
	}
	err = plan.Commit()
	for _, want := range []string{"injected replacement failure", "injected withdrawal failure"} {
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Fatalf("Commit error = %v, want %q", err, want)
		}
	}
	if state.current != nil {
		t.Fatal("failed withdrawal retained a claimed local current snapshot")
	}
}

func pointerClaim(name string, root cid.Cid) server.Doc {
	synced := uint64(1)
	return server.Doc{Unsigned: server.Unsigned{Heads: []server.HeadEntry{{
		Name: name, Root: root.String(), SyncedTo: &synced,
	}}}}
}

func pointerDocumentBlock(t *testing.T, claim server.Doc) blocks.Block {
	t.Helper()
	raw, err := json.Marshal(claim)
	if err != nil {
		t.Fatal(err)
	}
	block, err := p2p.NewDocumentBlock(raw)
	if err != nil {
		t.Fatal(err)
	}
	return block
}

func pointerTestCID(t *testing.T, codec uint64, value string) cid.Cid {
	t.Helper()
	hash, err := multihash.Sum([]byte(value), multihash.SHA2_256, 32)
	if err != nil {
		t.Fatal(err)
	}
	return cid.NewCidV1(codec, hash)
}

type pointerScheduleCall struct {
	heads     map[string]pointerhint.Set
	documents []cid.Cid
}

type pointerTestSchedule struct {
	mu               sync.Mutex
	events           *[]string
	validateErr      error
	replaceErrors    []error
	validateCalls    []pointerScheduleCall
	replaceCalls     []pointerScheduleCall
	currentHeads     map[string]pointerhint.Set
	currentDocuments []cid.Cid
}

func (s *pointerTestSchedule) ValidateAllWithDocuments(heads map[string]pointerhint.Set, documents []cid.Cid) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.event("validate")
	s.validateCalls = append(s.validateCalls, clonePointerCall(heads, documents))
	return s.validateErr
}

func (s *pointerTestSchedule) ReplaceAllWithDocuments(heads map[string]pointerhint.Set, documents []cid.Cid) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.event("replace")
	call := clonePointerCall(heads, documents)
	s.replaceCalls = append(s.replaceCalls, call)
	if len(s.replaceErrors) != 0 {
		err := s.replaceErrors[0]
		s.replaceErrors = s.replaceErrors[1:]
		if err != nil {
			return err
		}
	}
	s.currentHeads = call.heads
	s.currentDocuments = call.documents
	return nil
}

func (s *pointerTestSchedule) event(name string) {
	if s.events != nil {
		*s.events = append(*s.events, name)
	}
}

func clonePointerCall(heads map[string]pointerhint.Set, documents []cid.Cid) pointerScheduleCall {
	return pointerScheduleCall{heads: clonePointerHeads(heads), documents: append([]cid.Cid(nil), documents...)}
}

type pointerTestDocuments struct {
	mu            sync.Mutex
	events        *[]string
	stageErrors   []error
	replaceErrors []error
	stageCalls    [][]blocks.Block
	replaceCalls  [][]cid.Cid
	staged        map[string]blocks.Block
	active        []cid.Cid
}

func (s *pointerTestDocuments) StageCurrentAfterVerification(documents []blocks.Block) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.event("stage")
	s.stageCalls = append(s.stageCalls, append([]blocks.Block(nil), documents...))
	if len(s.stageErrors) != 0 {
		err := s.stageErrors[0]
		s.stageErrors = s.stageErrors[1:]
		if err != nil {
			return err
		}
	}
	for _, document := range documents {
		s.staged[document.Cid().KeyString()] = document
	}
	return nil
}

func (s *pointerTestDocuments) ReplaceCurrentDocuments(documents []cid.Cid) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.event("documents")
	s.replaceCalls = append(s.replaceCalls, append([]cid.Cid(nil), documents...))
	if len(s.replaceErrors) != 0 {
		err := s.replaceErrors[0]
		s.replaceErrors = s.replaceErrors[1:]
		if err != nil {
			return err
		}
	}
	for _, document := range documents {
		if _, ok := s.staged[document.KeyString()]; !ok {
			return errors.New("document was not staged")
		}
	}
	s.active = append([]cid.Cid(nil), documents...)
	return nil
}

func (s *pointerTestDocuments) event(name string) {
	if s.events != nil {
		*s.events = append(*s.events, name)
	}
}
