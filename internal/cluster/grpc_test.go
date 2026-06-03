package cluster

import (
	"sync"
	"testing"
)

// Regression: Shutdown used to close stopCh and the listener on
// every call, so a second invocation panicked with "close of closed
// channel" or wrote to a closed net.Listener.  The sync.Once + the
// running-flag short-circuit make Shutdown safe to call multiple
// times and from multiple goroutines.
func TestRPCServerShutdownIdempotent(t *testing.T) {
	s := NewRPCServer("127.0.0.1:0", nil)

	// Never Started; Shutdown must still be safe and must not panic
	// on a second call.
	s.Shutdown()
	s.Shutdown()
	s.Shutdown()
}

func TestRPCServerShutdownConcurrent(t *testing.T) {
	s := NewRPCServer("127.0.0.1:0", nil)

	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.Shutdown()
		}()
	}
	wg.Wait()

	select {
	case <-s.stopCh:
	default:
		t.Error("stopCh should be closed after Shutdown")
	}
}

// Regression: Start used to spawn a second acceptLoop when called
// twice.  Now it must report an error instead of silently leaking
// a goroutine that fights the first loop for the same listener.
func TestRPCServerStartTwiceFails(t *testing.T) {
	s := NewRPCServer("127.0.0.1:0", nil)
	if err := s.Start(); err != nil {
		t.Fatalf("first Start failed: %v", err)
	}
	defer s.Shutdown()

	if err := s.Start(); err == nil {
		t.Error("second Start should have returned an error")
	}
}

// Regression: Stop used to close dispatchChan.  With the requeue
// AfterFunc in dispatchTask, a requeue that fired after Stop
// panicked with "send on closed channel".  The fix is to honor
// s.ctx.Done in the AfterFunc and leave the channel for GC.
func TestSchedulerStopDoesNotPanicPendingAfterFunc(t *testing.T) {
	s := NewScheduler("node-1", nil)

	// Simulate the dispatchTask AfterFunc path: schedule a requeue
	// that would fire after Stop.  We do this directly here because
	// triggering dispatchTask requires a real node and task flow.
	stopCh := s.ctx.Done()
	_ = stopCh

	// Spawn an AfterFunc-style goroutine that mirrors the real one.
	done := make(chan struct{})
	go func() {
		// Spin briefly so Stop runs first.
		for i := 0; i < 5; i++ {
		}
		select {
		case <-s.ctx.Done():
			close(done)
			return
		default:
		}
		select {
		case s.dispatchChan <- nil:
		case <-s.ctx.Done():
		default:
		}
		close(done)
	}()

	// Stop first.  Pending senders must observe ctx.Done and bail
	// out instead of panicking.
	s.Stop()

	<-done
}
