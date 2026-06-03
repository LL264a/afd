package api

import (
	"sync"
	"testing"
	"time"
)

// Regression: Close used to close the done channel unconditionally,
// so the second call panicked with "close of closed channel".
// After the sync.Once wrap, repeated and concurrent Close calls
// must be safe.
func TestWebSocketHubCloseIdempotent(t *testing.T) {
	h := NewWebSocketHub()
	h.Close()
	h.Close()
	h.Close()
}

func TestWebSocketHubCloseConcurrent(t *testing.T) {
	h := NewWebSocketHub()
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			h.Close()
		}()
	}
	wg.Wait()

	select {
	case <-h.done:
	default:
		t.Error("done should be closed after Close")
	}
}

// Regression: BroadcastTaskUpdate used to write to h.broadcast
// unconditionally, which blocked forever once Run() had returned
// (no consumer).  After tryBroadcast, the call must return promptly
// even when the hub has been closed.
func TestWebSocketHubBroadcastAfterCloseDoesNotBlock(t *testing.T) {
	h := NewWebSocketHub()
	h.Close() // no Run() ever started → no consumer

	done := make(chan struct{})
	go func() {
		h.BroadcastTaskUpdate(nil)
		close(done)
	}()

	select {
	case <-done:
		// good: returned without blocking
	case <-time.After(2 * time.Second):
		t.Fatal("BroadcastTaskUpdate blocked after hub Close")
	}
}
