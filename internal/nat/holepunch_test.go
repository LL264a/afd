package nat

import (
	"sync"
	"testing"
)

// Regression: Stop used to close stopCh unconditionally, so a second
// call panicked with "close of closed channel".  After the sync.Once
// wrap, repeated and concurrent calls must be safe.
func TestHolePuncherStopIdempotent(t *testing.T) {
	h := NewHolePuncher()
	// Never Started; conn is nil.  Stop must be safe to call multiple
	// times and must close stopCh exactly once.
	h.Stop()
	h.Stop()
	h.Stop()
}

func TestHolePuncherStopConcurrent(t *testing.T) {
	h := NewHolePuncher()
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			h.Stop()
		}()
	}
	wg.Wait()

	select {
	case <-h.stopCh:
	default:
		t.Error("stopCh should be closed")
	}
}
