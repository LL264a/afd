package plugin

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"

	"github.com/nexus-dl/afd/pkg/logger"
)

// TestMain 初始化 logger，因为 hooks.go 中直接使用 logger.Log（无 nil 检查）。
func TestMain(m *testing.M) {
	_ = logger.Init("info", "")
	os.Exit(m.Run())
}

func TestHookSystemRegister(t *testing.T) {
	hs := NewHookSystem()
	handler := func(ctx context.Context, data interface{}) error {
		return nil
	}
	hs.Register(PreDownloadHook, handler)

	hooks := hs.ListHooks(PreDownloadHook)
	if len(hooks) != 1 {
		t.Errorf("expected 1 hook, got %d", len(hooks))
	}
}

func TestHookSystemExecute(t *testing.T) {
	hs := NewHookSystem()
	called := false
	hs.Register(PreDownloadHook, func(ctx context.Context, data interface{}) error {
		called = true
		return nil
	})

	task := &DownloadTask{URL: "http://example.com/file.zip"}
	err := hs.Execute(context.Background(), PreDownloadHook, task)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !called {
		t.Error("hook was not called")
	}
}

func TestHookSystemExecuteMultiple(t *testing.T) {
	hs := NewHookSystem()
	order := []string{}
	var mu sync.Mutex

	hs.Register(PreDownloadHook, func(ctx context.Context, data interface{}) error {
		mu.Lock()
		order = append(order, "first")
		mu.Unlock()
		return nil
	})
	hs.Register(PreDownloadHook, func(ctx context.Context, data interface{}) error {
		mu.Lock()
		order = append(order, "second")
		mu.Unlock()
		return nil
	})

	task := &DownloadTask{URL: "http://example.com/file.zip"}
	err := hs.Execute(context.Background(), PreDownloadHook, task)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(order) != 2 {
		t.Errorf("expected 2 hooks called, got %d", len(order))
	}
}

func TestHookSystemExecuteError(t *testing.T) {
	hs := NewHookSystem()
	expectedErr := errors.New("hook failed")
	hs.Register(PreDownloadHook, func(ctx context.Context, data interface{}) error {
		return expectedErr
	})

	task := &DownloadTask{URL: "http://example.com/file.zip"}
	err := hs.Execute(context.Background(), PreDownloadHook, task)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestHookSystemUnregister(t *testing.T) {
	hs := NewHookSystem()
	handler := func(ctx context.Context, data interface{}) error {
		return nil
	}
	hs.Register(PreDownloadHook, handler)
	hs.Unregister(PreDownloadHook, handler)

	hooks := hs.ListHooks(PreDownloadHook)
	if len(hooks) != 0 {
		t.Errorf("expected 0 hooks after unregister, got %d", len(hooks))
	}
}

func TestHookSystemClear(t *testing.T) {
	hs := NewHookSystem()
	hs.Register(PreDownloadHook, func(ctx context.Context, data interface{}) error { return nil })
	hs.Register(PostDownloadHook, func(ctx context.Context, data interface{}) error { return nil })

	hs.Clear(PreDownloadHook)
	if len(hs.ListHooks(PreDownloadHook)) != 0 {
		t.Error("PreDownloadHook not cleared")
	}
	if len(hs.ListHooks(PostDownloadHook)) != 1 {
		t.Error("PostDownloadHook should not be cleared")
	}
}

func TestHookSystemConcurrentAccess(t *testing.T) {
	hs := NewHookSystem()
	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			hs.Register(PreDownloadHook, func(ctx context.Context, data interface{}) error {
				return nil
			})
		}()
	}

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			hs.ListHooks(PreDownloadHook)
		}()
	}

	wg.Wait()
	if len(hs.ListHooks(PreDownloadHook)) != 100 {
		t.Errorf("expected 100 hooks, got %d", len(hs.ListHooks(PreDownloadHook)))
	}
}
