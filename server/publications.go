package server

import (
	"context"
	"crypto/ed25519"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sync"

	"github.com/cockroachdb/pebble/v2"
)

// prefixPublicationRevision is keyed by the signing public key, because the
// document revision is an authority-local order rather than a node or URL
// counter.
const prefixPublicationRevision byte = 'r'

// ErrPublicationRevisionOverflow fails closed instead of allowing a signer to
// wrap back to a replayable revision.
var ErrPublicationRevisionOverflow = errors.New("server: publication revision counter exhausted")

// PublicationRevisions is the persisted allocator consumed by Heads.
type PublicationRevisions interface {
	Current(ctx context.Context, signer ed25519.PublicKey) (revision uint64, active bool, err error)
	Next(ctx context.Context, signer ed25519.PublicKey) (uint64, error)
}

// PublicationStore persists signer-local publication revision floors.
type PublicationStore struct {
	kv *pebble.DB
	mu sync.Mutex
}

// NewPublicationStore returns a revision allocator over kv.
func NewPublicationStore(kv *pebble.DB) *PublicationStore {
	return &PublicationStore{kv: kv}
}

func publicationRevisionKey(signer ed25519.PublicKey) []byte {
	k := make([]byte, 1+len(signer))
	k[0] = prefixPublicationRevision
	copy(k[1:], signer)
	return k
}

// Current reports the durable revision floor for signer. An absent floor means
// this authority has never entered revisioned publication mode.
func (s *PublicationStore) Current(ctx context.Context, signer ed25519.PublicKey) (uint64, bool, error) {
	if err := ctx.Err(); err != nil {
		return 0, false, err
	}
	if len(signer) != ed25519.PublicKeySize {
		return 0, false, fmt.Errorf("server: publication signer is %d bytes, want %d", len(signer), ed25519.PublicKeySize)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.currentLocked(signer)
}

func (s *PublicationStore) currentLocked(signer ed25519.PublicKey) (uint64, bool, error) {
	v, closer, err := s.kv.Get(publicationRevisionKey(signer))
	switch {
	case errors.Is(err, pebble.ErrNotFound):
		return 0, false, nil
	case err != nil:
		return 0, false, fmt.Errorf("server: reading publication revision: %w", err)
	default:
		defer closer.Close()
		if len(v) != 8 {
			return 0, false, fmt.Errorf("server: publication revision is %d bytes, want 8", len(v))
		}
		current := binary.BigEndian.Uint64(v)
		if current == 0 {
			return 0, false, errors.New("server: publication revision floor is zero")
		}
		return current, true, nil
	}
}

// Next durably burns and returns the next revision. Allocation precedes signing
// and publication, so failures may leave gaps; readers explicitly permit them.
func (s *PublicationStore) Next(ctx context.Context, signer ed25519.PublicKey) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if len(signer) != ed25519.PublicKeySize {
		return 0, fmt.Errorf("server: publication signer is %d bytes, want %d", len(signer), ed25519.PublicKeySize)
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	current, active, err := s.currentLocked(signer)
	if err != nil {
		return 0, err
	}
	if !active {
		current = 0
	}
	if current == math.MaxUint64 {
		return 0, ErrPublicationRevisionOverflow
	}
	next := current + 1
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], next)
	if err := s.kv.Set(publicationRevisionKey(signer), encoded[:], syncWrite); err != nil {
		return 0, fmt.Errorf("server: writing publication revision %d: %w", next, err)
	}
	return next, nil
}
