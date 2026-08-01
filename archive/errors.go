package archive

import (
	"context"
	"fmt"
	"strings"

	"github.com/ipfs/go-cid"

	"github.com/blobarchive/bloar/schema"
)

// BlobResolver maps a versioned hash to the CID of its blob block. It is the
// read side of the blob catalog (spec 6.1), which the ingest path writes.
//
// ok is false when the vh is not in the catalog. Catalog entries may outlive
// their blocks (GC does not update the catalog), so a resolved CID is not proof
// the block exists; ApplyRefs checks the blockstore separately (spec 5.1 step
// 4).
type BlobResolver interface {
	ResolveBlob(ctx context.Context, vh schema.VersionedHash) (cid.Cid, bool, error)
}

// ConflictError reports a mutation refused because it conflicts with the head's
// current state or is malformed: every spec 5.1 validation failure, and a
// truncation past synced_to. The server maps it to HTTP 409 (spec 7.2).
type ConflictError struct {
	Reason string
	Err    error
}

func (e *ConflictError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("archive: %s: %v", e.Reason, e.Err)
	}
	return "archive: " + e.Reason
}

func (e *ConflictError) Unwrap() error { return e.Err }

func conflictf(format string, args ...any) *ConflictError {
	return &ConflictError{Reason: fmt.Sprintf(format, args...)}
}

// MissingBlobsError lists the versioned hashes a batch referenced that either
// are not in the catalog or whose blocks are not in the blockstore. It arrives
// wrapped in a ConflictError, so a server can test for 409 and for the
// missing_blobs response field independently:
//
//	var conflict *archive.ConflictError
//	var missing *archive.MissingBlobsError
//	errors.As(err, &conflict)   // -> 409
//	errors.As(err, &missing)    // -> missing_blobs: [...]
type MissingBlobsError struct {
	VHs []schema.VersionedHash
}

func (e *MissingBlobsError) Error() string {
	hs := make([]string, 0, len(e.VHs))
	for _, vh := range e.VHs {
		hs = append(hs, fmt.Sprintf("0x%x", vh[:]))
	}
	return fmt.Sprintf("%d referenced blob(s) unavailable: %s", len(e.VHs), strings.Join(hs, ", "))
}
