package idempotency

import (
	"context"
	"errors"
	"sync"
)

var ErrPayloadMismatch = errors.New("idempotency key reused with a different payload")

type Result struct {
	Status int
	Body   []byte
}

type entry struct {
	fingerprint string
	done        chan struct{}
	result      Result
	err         error
}

// Store is an in-memory demonstration store.
// It is not suitable for multi-instance production deployments.
type Store struct {
	mu      sync.Mutex
	entries map[string]*entry
}

func NewStore() *Store {
	return &Store{entries: make(map[string]*entry)}
}

// Do runs create once per key while the entry is retained.
// Concurrent callers with the same key and payload wait for the first result.
// Reusing a key with a different payload returns ErrPayloadMismatch.
func (s *Store) Do(
	ctx context.Context,
	key string,
	fingerprint string,
	create func() (Result, error),
) (result Result, replayed bool, err error) {
	s.mu.Lock()

	if existing, ok := s.entries[key]; ok {
		if existing.fingerprint != fingerprint {
			s.mu.Unlock()
			return Result{}, false, ErrPayloadMismatch
		}

		done := existing.done
		s.mu.Unlock()

		select {
		case <-done:
			return existing.result, true, existing.err
		case <-ctx.Done():
			return Result{}, true, ctx.Err()
		}
	}

	current := &entry{
		fingerprint: fingerprint,
		done:        make(chan struct{}),
	}
	s.entries[key] = current
	s.mu.Unlock()

	res, createErr := create()

	s.mu.Lock()
	current.result = res
	current.err = createErr
	close(current.done)

	// Demo policy: remove failed operations so a later request can retry.
	// Production systems need a deliberate policy for failures and recovery.
	if createErr != nil {
		delete(s.entries, key)
	}
	s.mu.Unlock()

	return res, false, createErr
}
