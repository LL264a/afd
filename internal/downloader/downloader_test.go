package downloader

import (
	"os"
	"testing"
)

func TestNewRateLimiter(t *testing.T) {
	rl := NewRateLimiter(1024*1024, 1024)
	if rl == nil {
		t.Fatal("expected non-nil rate limiter")
	}
}

func TestRateLimiter_SetRate(t *testing.T) {
	rl := NewRateLimiter(0, 1024)
	rl.SetRate(1024 * 1024)
	if rl.GetRate() != 1024*1024 {
		t.Fatalf("expected rate %d, got %d", 1024*1024, rl.GetRate())
	}
}

func TestRateLimiter_GetRate(t *testing.T) {
	rl := NewRateLimiter(2048, 1024)
	if rl.GetRate() != 2048 {
		t.Fatalf("expected rate 2048, got %d", rl.GetRate())
	}
}

func TestNewGlobalRateLimiter(t *testing.T) {
	grl := GetGlobalRateLimiter()
	if grl == nil {
		t.Fatal("expected non-nil global rate limiter")
	}
}

func TestGlobalRateLimiter_TaskLimit(t *testing.T) {
	grl := GetGlobalRateLimiter()
	rl := NewRateLimiter(1024, 1024)
	grl.mu.Lock()
	grl.taskLimit["task-1"] = rl
	delete(grl.taskLimit, "task-1")
	grl.mu.Unlock()
}

func TestServerConnectionLimiter(t *testing.T) {
	limiter := NewServerConnectionLimiter(2)
	if limiter == nil {
		t.Fatal("expected non-nil limiter")
	}

	limiter.Acquire("server-1")
	limiter.Acquire("server-1")

	go func() {
		limiter.Release("server-1")
	}()

	limiter.Acquire("server-1")
	limiter.Release("server-1")
	limiter.Release("server-1")
}

func TestPreallocateFile(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := tmpDir + "/test_prealloc"
	
	file, err := createTestFile(filePath)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	err = preallocateFile(file, 1024*1024, false)
	if err != nil {
		t.Fatalf("preallocateFile failed: %v", err)
	}

	stat, err := file.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if stat.Size() != 1024*1024 {
		t.Fatalf("expected size %d, got %d", 1024*1024, stat.Size())
	}
}

func TestPreallocateFileSparse(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := tmpDir + "/test_sparse"
	
	file, err := createTestFile(filePath)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	err = preallocateFile(file, 10*1024*1024, true)
	if err != nil {
		t.Fatalf("preallocateFile sparse failed: %v", err)
	}

	stat, err := file.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if stat.Size() != 10*1024*1024 {
		t.Fatalf("expected size %d, got %d", 10*1024*1024, stat.Size())
	}
}

func createTestFile(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
}
