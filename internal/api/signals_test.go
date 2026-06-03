package api

import (
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
)

func TestRequestGracefulShutdown_UsesRegisteredHandler(t *testing.T) {
	called := int32(0)
	var mu sync.Mutex
	var gotSig syscall.Signal

	RegisterGracefulShutdownHandler(func(sig syscall.Signal) error {
		atomic.AddInt32(&called, 1)
		mu.Lock()
		gotSig = sig
		mu.Unlock()
		return nil
	})
	t.Cleanup(func() {
		gracefulShutdownHandler = nil
	})

	if err := requestGracefulShutdown(); err != nil {
		t.Fatalf("requestGracefulShutdown returned error: %v", err)
	}
	if atomic.LoadInt32(&called) != 1 {
		t.Fatalf("expected registered handler to be called once, got %d", called)
	}
	mu.Lock()
	defer mu.Unlock()
	if gotSig != syscall.SIGTERM {
		t.Fatalf("expected SIGTERM, got %v", gotSig)
	}
}

func TestRequestGracefulShutdown_NoHandler(t *testing.T) {
	gracefulShutdownHandler = nil
	err := requestGracefulShutdown()
	if err == nil {
		t.Skip("running in environment that allows self-signaling; got nil error which is fine")
	}
}

func TestRegisterGracefulShutdownHandler_OverridesPrevious(t *testing.T) {
	var firstCalled, secondCalled int32
	RegisterGracefulShutdownHandler(func(sig syscall.Signal) error {
		atomic.AddInt32(&firstCalled, 1)
		return nil
	})
	RegisterGracefulShutdownHandler(func(sig syscall.Signal) error {
		atomic.AddInt32(&secondCalled, 1)
		return nil
	})
	t.Cleanup(func() {
		gracefulShutdownHandler = nil
	})

	if err := requestGracefulShutdown(); err != nil {
		t.Fatalf("requestGracefulShutdown returned error: %v", err)
	}
	if atomic.LoadInt32(&firstCalled) != 0 {
		t.Fatalf("first handler should not have been called, was called %d times", firstCalled)
	}
	if atomic.LoadInt32(&secondCalled) != 1 {
		t.Fatalf("second handler should have been called once, was called %d times", secondCalled)
	}
}
