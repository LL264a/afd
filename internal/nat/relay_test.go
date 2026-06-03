package nat

import (
	"sync"
	"testing"
)

// Regression: Stop used to close stopCh unconditionally, so a second
// call panicked with "close of closed channel".  After the sync.Once
// wrap, repeated calls must be safe.
func TestRelayServerStopIdempotent(t *testing.T) {
	r := NewRelayServer("127.0.0.1:0")

	// We never call Start, so r.conn is nil.  Stop must not panic
	// regardless, and a second call must remain a no-op.
	r.Stop()
	r.Stop()
	r.Stop()
}

func TestRelayClientStopIdempotent(t *testing.T) {
	c := NewRelayClient("127.0.0.1:0", "client-1")
	// Same situation: no Start, no conn.  Stop should be safe to
	// call multiple times.
	c.Stop()
	c.Stop()
}

func TestRelayStopIdempotent(t *testing.T) {
	r := NewRelay()
	r.Stop()
	r.Stop()
}

// Concurrent Stop calls (e.g. from signal handler and deferred
// shutdown) must not panic and must close stopCh exactly once.
func TestRelayStopConcurrent(t *testing.T) {
	r := NewRelayServer("127.0.0.1:0")
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r.Stop()
		}()
	}
	wg.Wait()

	// Verify stopCh is closed (read should not block).
	select {
	case <-r.stopCh:
	default:
		t.Error("stopCh should be closed")
	}
}
