package follow_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"

	"github.com/blobarchive/bloar/follow"
	"github.com/blobarchive/bloar/p2p"
	"github.com/blobarchive/bloar/p2p/pointerhint"
	"github.com/blobarchive/bloar/replica"
	"github.com/blobarchive/bloar/server"
)

func TestAdmittedDocumentCallbackGetsExactHTTPSBlockAfterDurabilityAndRetries(t *testing.T) {
	w := newWriter(t)
	w.ingestSlot(testOrigin, 1)
	raw := bytes.Clone(w.heads.Doc())

	var (
		f        *follower
		attempts int
		got      blocks.Block
		gotDoc   server.Doc
	)
	f = newFollower(t, w, func(cfg *follow.Config) {
		cfg.OnAdmittedDocument = func(blk blocks.Block, doc server.Doc) error {
			attempts++
			// The hook is not a pre-commit notification: the complete serving
			// snapshot and root mirror must already be visible and durable.
			head, ok := f.heads.Get(testHead)
			if !ok || !head.Root().Equals(w.head.Root()) {
				return errors.New("callback ran before the adopted head was visible")
			}
			root, ok, err := f.roots.Get(t.Context(), testHead)
			if err != nil || !ok || !root.Equals(w.head.Root()) {
				return errors.New("callback ran before the root mirror was durable")
			}
			if attempts == 1 {
				return errors.New("injected verified-document retention failure")
			}
			got, gotDoc = blk, doc
			return nil
		}
	})

	err := f.pollErr()
	if err == nil || !strings.Contains(err.Error(), "injected verified-document retention failure") {
		t.Fatalf("first Poll error = %v, want callback failure", err)
	}
	if _, ok := f.heads.Get(testHead); !ok {
		t.Fatal("callback failure undid an already-durable adoption")
	}

	// The publication is now the current generation, so this is a no-op
	// admission. It must still retry the callback which failed after durability.
	if err := f.pollErr(); err != nil {
		t.Fatalf("retrying current document: %v", err)
	}
	if attempts != 2 {
		t.Fatalf("callback attempts = %d, want failure plus current-document retry", attempts)
	}
	if !bytes.Equal(got.RawData(), raw) {
		t.Fatalf("callback raw bytes differ from HTTPS response\n got: %s\nwant: %s", got.RawData(), raw)
	}
	want, err := p2p.NewDocumentBlock(raw)
	if err != nil {
		t.Fatal(err)
	}
	if !got.Cid().Equals(want.Cid()) || got.Cid().Prefix().Codec != cid.Raw {
		t.Fatalf("callback document CID = %s, want raw/sha2-256 %s", got.Cid(), want.Cid())
	}
	var wantDoc server.Doc
	if err := json.Unmarshal(raw, &wantDoc); err != nil {
		t.Fatal(err)
	}
	if gotDoc.Signature == "" || gotDoc.Signature != wantDoc.Signature {
		t.Fatal("callback did not carry the verified document decoded from the exact HTTPS bytes")
	}

	// Once the exact CID has completed successfully, ordinary repolls do not
	// repeatedly notify the ephemeral serving layer.
	if err := f.pollErr(); err != nil {
		t.Fatalf("repolling admitted document: %v", err)
	}
	if attempts != 2 {
		t.Fatalf("successful document callback repeated: attempts = %d, want 2", attempts)
	}
}

func TestAdmittedDocumentCallbackRunsAfterExternalRetentionCommit(t *testing.T) {
	w := newWriter(t)
	w.ingestSlot(testOrigin, 1)
	retention := &callbackRetention{failCommit: true}
	callbackCalls := 0
	f := newFollower(t, w,
		externalRetentionOption(t, retention),
		func(cfg *follow.Config) {
			cfg.OnAdmittedDocument = func(blocks.Block, server.Doc) error {
				callbackCalls++
				if len(retention.events) == 0 || retention.events[len(retention.events)-1] != "commit:ok" {
					return errors.New("document callback ran before external retention Commit")
				}
				return nil
			}
		},
	)

	if err := f.pollErr(); err == nil || !strings.Contains(err.Error(), "injected external commit failure") {
		t.Fatalf("commit-failed Poll error = %v", err)
	}
	if callbackCalls != 0 {
		t.Fatalf("callback ran %d times before external retention committed", callbackCalls)
	}
	retention.failCommit = false
	// The checkpoint is already durable after the failed external commit. The
	// exact current/no-op document must repeat retention Commit and only then
	// reach the verified-document callback.
	if err := f.pollErr(); err != nil {
		t.Fatalf("retrying after external commit failure: %v", err)
	}
	if callbackCalls != 1 {
		t.Fatalf("callback calls = %d, want 1", callbackCalls)
	}
}

func TestAdmittedDocumentCallbackExcludesFailedAndRefusedCandidates(t *testing.T) {
	w := newWriter(t)
	w.ingestSlot(testOrigin, 1)
	docs := newDocServer(t)
	base := time.Now().UTC().Truncate(time.Second)
	docs.publish(t, w, base)

	calls := 0
	f := newFollower(t, w, func(cfg *follow.Config) {
		cfg.URL = docs.url
		cfg.OnAdmittedDocument = func(blocks.Block, server.Doc) error {
			calls++
			return nil
		}
	})

	// A staged document whose atomic checkpoint batch fails never reaches the
	// post-durability callback.
	follow.SetBeforeAdmissionCommitHook(func() error { return errors.New("injected admission commit failure") })
	t.Cleanup(func() { follow.SetBeforeAdmissionCommitHook(nil) })
	if err := f.pollErr(); err == nil || !strings.Contains(err.Error(), "injected admission commit failure") {
		t.Fatalf("commit-failed Poll error = %v", err)
	}
	if calls != 0 {
		t.Fatalf("callback ran %d times for a commit-failed document", calls)
	}
	follow.SetBeforeAdmissionCommitHook(nil)
	if err := f.pollErr(); err != nil {
		t.Fatalf("admitting document after commit recovers: %v", err)
	}
	if calls != 1 {
		t.Fatalf("callback calls after successful recovery = %d, want 1", calls)
	}

	// A correctly signed but malformed timestamp is not a candidate.
	malformed := w.unsigned(base.Add(time.Minute))
	malformed.UpdatedAt = "not-a-time"
	docs.set(sign(t, w.key, malformed))
	if err := f.pollErr(); err == nil || !strings.Contains(err.Error(), "unparseable updated_at") {
		t.Fatalf("malformed Poll error = %v", err)
	}
	if calls != 1 {
		t.Fatalf("callback ran for a malformed document: calls = %d", calls)
	}

	// A validly signed replay below the admitted timestamp floor is refused.
	docs.publish(t, w, base.Add(-time.Hour))
	if err := f.pollErr(); err == nil || !strings.Contains(err.Error(), "before the accepted floor") {
		t.Fatalf("stale Poll error = %v", err)
	}
	if calls != 1 {
		t.Fatalf("callback ran for a stale document: calls = %d", calls)
	}

	// A fresh, signed document whose claimed coverage disagrees with its root
	// reaches whole-document preflight but is refused before any commit.
	refused := w.unsigned(base.Add(time.Hour))
	claimed := uint64(testOrigin + 100)
	refused.Heads[0].SyncedTo = &claimed
	docs.set(sign(t, w.key, refused))
	if err := f.pollErr(); err == nil || !strings.Contains(err.Error(), "claims synced_to") {
		t.Fatalf("coverage-refused Poll error = %v", err)
	}
	if calls != 1 {
		t.Fatalf("callback ran for a whole-document refusal: calls = %d", calls)
	}
}

func TestIPNSAdmittedDocumentRequiresBytesMatchingRecordCID(t *testing.T) {
	w := newIPNSWriter(t)
	w.ingestSlot(testOrigin, 1)
	firstRaw := bytes.Clone(w.heads.Doc())
	firstCID, _ := w.publish(t, firstRaw)

	corrupt := false
	calls := 0
	var got blocks.Block
	f := ipnsFollower(t, w, func(cfg *follow.Config) {
		cfg.DocumentBlock = func(ctx context.Context, c cid.Cid) (blocks.Block, error) {
			blk, err := w.docs.Get(ctx, c)
			if err != nil || !corrupt {
				return blk, err
			}
			// Report the requested CID while returning unrelated bytes. This
			// models an untrusted external DocumentBlock implementation; the
			// follower must independently hash the bytes.
			return mismatchedBlock{c: c, raw: []byte("not the IPNS-named document")}, nil
		}
		cfg.OnAdmittedDocument = func(blk blocks.Block, _ server.Doc) error {
			calls++
			got = blk
			return nil
		}
	})
	if err := f.pollErr(); err != nil {
		t.Fatalf("admitting matching IPNS document: %v", err)
	}
	if calls != 1 || !got.Cid().Equals(firstCID) || !bytes.Equal(got.RawData(), firstRaw) {
		t.Fatalf("IPNS callback = calls:%d cid:%s raw:%q, want calls:1 cid:%s exact named bytes",
			calls, got.Cid(), got.RawData(), firstCID)
	}

	w.ingestSlot(testOrigin+1, 2)
	w.publish(t, w.heads.Doc())
	corrupt = true
	err := f.pollErr()
	if err == nil || !strings.Contains(err.Error(), "document bytes hash to") {
		t.Fatalf("mismatched IPNS document Poll error = %v", err)
	}
	if calls != 1 {
		t.Fatalf("callback ran for IPNS bytes which did not match the record CID: calls = %d", calls)
	}
}

func TestIPNSDocumentFetchMissFindsExactPointerAndRetries(t *testing.T) {
	w := newIPNSWriter(t)
	w.ingestSlot(testOrigin, 1)
	documentCID, _ := w.publish(t, w.heads.Doc())

	found := false
	findCalls := 0
	f := ipnsFollower(t, w, func(cfg *follow.Config) {
		cfg.FetchTimeout = 50 * time.Millisecond
		cfg.DocumentBlock = func(ctx context.Context, c cid.Cid) (blocks.Block, error) {
			if !found {
				return nil, errors.New("no connected document provider")
			}
			return w.docs.Get(ctx, c)
		}
		cfg.FindPointer = func(_ context.Context, pointer pointerhint.Pointer) error {
			findCalls++
			if pointer.Kind != pointerhint.Document || !pointer.CID.Equals(documentCID) {
				return fmt.Errorf("finder received %s %s, want document %s", pointer.Kind, pointer.CID, documentCID)
			}
			found = true
			return nil
		}
	})
	if err := f.pollErr(); err != nil {
		t.Fatalf("Poll after exact document discovery: %v", err)
	}
	if findCalls != 1 {
		t.Fatalf("exact document finder calls = %d, want 1", findCalls)
	}
}

type mismatchedBlock struct {
	c   cid.Cid
	raw []byte
}

func (b mismatchedBlock) RawData() []byte          { return b.raw }
func (b mismatchedBlock) Cid() cid.Cid             { return b.c }
func (b mismatchedBlock) String() string           { return b.c.String() }
func (b mismatchedBlock) Loggable() map[string]any { return map[string]any{"cid": b.c.String()} }

type callbackRetention struct {
	events     []string
	failCommit bool
}

func (r *callbackRetention) Prepare(context.Context, replica.Generation) error {
	r.events = append(r.events, "prepare")
	return nil
}

func (r *callbackRetention) Commit(context.Context, replica.Generation) error {
	if r.failCommit {
		r.events = append(r.events, "commit:failed")
		return errors.New("injected external commit failure")
	}
	r.events = append(r.events, "commit:ok")
	return nil
}

func (*callbackRetention) ProtectsAll(context.Context, []replica.Head) error { return nil }

var _ follow.Retention = (*callbackRetention)(nil)
