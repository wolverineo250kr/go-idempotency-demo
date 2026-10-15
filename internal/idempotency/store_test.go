package idempotency

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestDoReplaysResult(t *testing.T) {
	store := NewStore()
	var calls atomic.Int32

	create := func() (Result, error) {
		calls.Add(1)
		return Result{Status: 201, Body: []byte(`{"order_id":1}`)}, nil
	}

	first, replayed, err := store.Do(context.Background(), "key-1", `{"product_id":42}`, create)
	if err != nil {
		t.Fatal(err)
	}
	if replayed {
		t.Fatal("first call must not be marked as replayed")
	}

	second, replayed, err := store.Do(context.Background(), "key-1", `{"product_id":42}`, create)
	if err != nil {
		t.Fatal(err)
	}
	if !replayed {
		t.Fatal("second call must be marked as replayed")
	}
	if first.Status != second.Status || string(first.Body) != string(second.Body) {
		t.Fatalf("results differ: first=%+v second=%+v", first, second)
	}
	if calls.Load() != 1 {
		t.Fatalf("create called %d times; want 1", calls.Load())
	}
}

func TestDoRejectsDifferentPayload(t *testing.T) {
	store := NewStore()
	create := func() (Result, error) {
		return Result{Status: 201, Body: []byte(`{}`)}, nil
	}

	_, _, err := store.Do(context.Background(), "same-key", `{"quantity":1}`, create)
	if err != nil {
		t.Fatal(err)
	}

	_, _, err = store.Do(context.Background(), "same-key", `{"quantity":2}`, create)
	if err != ErrPayloadMismatch {
		t.Fatalf("got %v; want %v", err, ErrPayloadMismatch)
	}
}

func TestConcurrentCallsCreateOnce(t *testing.T) {
	store := NewStore()
	var calls atomic.Int32

	create := func() (Result, error) {
		calls.Add(1)
		time.Sleep(30 * time.Millisecond)
		return Result{Status: 201, Body: []byte(`{"order_id":1}`)}, nil
	}

	const workers = 20
	var wg sync.WaitGroup
	wg.Add(workers)

	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			_, _, err := store.Do(context.Background(), "shared-key", `{"product_id":42}`, create)
			if err != nil {
				t.Errorf("Do returned error: %v", err)
			}
		}()
	}
	wg.Wait()

	if calls.Load() != 1 {
		t.Fatalf("create called %d times; want 1", calls.Load())
	}
}
