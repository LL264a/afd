package downloader

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestNewRateLimiterBurstDefaultsToRate(t *testing.T) {
	r := NewRateLimiter(1024, 0)
	if r.burst != 1024 {
		t.Errorf("burst = %d, want 1024 when burst <= 0 falls back to rate", r.burst)
	}
}

func TestNewRateLimiterBurstDefaultsTo1MB(t *testing.T) {
	r := NewRateLimiter(0, 0)
	if r.burst != 1024*1024 {
		t.Errorf("burst = %d, want 1MB when both rate and burst <= 0", r.burst)
	}
}

func TestRateLimiterAllowConsumesTokens(t *testing.T) {
	r := NewRateLimiter(1000, 1000)
	if !r.Allow(500) {
		t.Error("Allow(500) on fresh limiter should succeed")
	}
	if !r.Allow(500) {
		t.Error("Allow(500) should still succeed (drains last tokens)")
	}
	if r.Allow(1) {
		t.Error("Allow(1) should fail after tokens drained")
	}
}

func TestRateLimiterSetRateZeroResetsTokens(t *testing.T) {
	r := NewRateLimiter(1000, 1000)
	if !r.Allow(1000) {
		t.Fatal("Allow should succeed with full burst")
	}
	r.SetRate(0)
	if r.tokens != r.burst {
		t.Errorf("After SetRate(0), tokens = %d, want %d (burst)", r.tokens, r.burst)
	}
	if r.GetRate() != 0 {
		t.Errorf("GetRate = %d, want 0", r.GetRate())
	}
}

func TestRateLimiterSetRateRespectsPositiveValue(t *testing.T) {
	r := NewRateLimiter(1000, 1000)
	r.SetRate(2048)
	if r.GetRate() != 2048 {
		t.Errorf("GetRate = %d, want 2048", r.GetRate())
	}
}

func TestRateLimiterRefillOverTime(t *testing.T) {
	r := NewRateLimiter(1000, 1000)
	if !r.Allow(1000) {
		t.Fatal("initial burst should be available")
	}
	time.Sleep(150 * time.Millisecond)
	allowed := false
	for i := 0; i < 200; i++ {
		if r.Allow(1) {
			allowed = true
			break
		}
	}
	if !allowed {
		t.Error("After 150ms at 1000 B/s, at least 1 token should be refilled")
	}
}

func TestRateLimiterWaitImmediateForZeroRate(t *testing.T) {
	r := NewRateLimiter(0, 1024)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	if err := r.Wait(ctx, 1024); err != nil {
		t.Errorf("Wait on zero rate should return nil, got %v", err)
	}
	if elapsed := time.Since(start); elapsed > 20*time.Millisecond {
		t.Errorf("Wait should be near-instant for zero rate, took %v", elapsed)
	}
}

func TestRateLimiterWaitContextCancel(t *testing.T) {
	r := NewRateLimiter(100, 100)
	if !r.Allow(100) {
		t.Fatal("expected to drain burst")
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(30 * time.Millisecond)
		cancel()
	}()
	start := time.Now()
	err := r.Wait(ctx, 200)
	if err == nil {
		t.Error("Wait should return error after context cancel")
	}
	if elapsed := time.Since(start); elapsed > 200*time.Millisecond {
		t.Errorf("Wait should return soon after cancel, took %v", elapsed)
	}
}

func TestRateLimiterConcurrentAllow(t *testing.T) {
	r := NewRateLimiter(10000, 1000)
	var allowed int64
	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if r.Allow(10) {
				atomic.AddInt64(&allowed, 10)
			}
		}()
	}
	wg.Wait()
	if allowed > 1000+int64(float64(10000)*0.5) {
		t.Errorf("Concurrent Allow allowed too much: %d (should be near burst)", allowed)
	}
}

func TestGlobalRateLimiterSetAndGetGlobal(t *testing.T) {
	g := &GlobalRateLimiter{
		limiter:   NewRateLimiter(0, 1024),
		taskLimit: make(map[string]*RateLimiter),
	}
	g.SetGlobalRate(2048)
	if got := g.GetGlobalRate(); got != 2048 {
		t.Errorf("GetGlobalRate = %d, want 2048", got)
	}
}

func TestGlobalRateLimiterTaskRateAddAndQuery(t *testing.T) {
	g := &GlobalRateLimiter{
		limiter:   NewRateLimiter(0, 1024),
		taskLimit: make(map[string]*RateLimiter),
	}
	g.SetTaskRate("task-1", 4096)
	if got := g.GetTaskRate("task-1"); got != 4096 {
		t.Errorf("GetTaskRate = %d, want 4096", got)
	}
	if got := g.GetTaskRate("nonexistent"); got != 0 {
		t.Errorf("GetTaskRate on missing = %d, want 0", got)
	}
}

func TestGlobalRateLimiterSetTaskRateZeroRemoves(t *testing.T) {
	g := &GlobalRateLimiter{
		limiter:   NewRateLimiter(0, 1024),
		taskLimit: make(map[string]*RateLimiter),
	}
	g.SetTaskRate("task-1", 4096)
	g.SetTaskRate("task-1", 0)
	if got := g.GetTaskRate("task-1"); got != 0 {
		t.Errorf("After SetTaskRate(0), GetTaskRate = %d, want 0", got)
	}
}

func TestGlobalRateLimiterRemoveTask(t *testing.T) {
	g := &GlobalRateLimiter{
		limiter:   NewRateLimiter(0, 1024),
		taskLimit: make(map[string]*RateLimiter),
	}
	g.SetTaskRate("task-1", 4096)
	g.RemoveTask("task-1")
	if got := g.GetTaskRate("task-1"); got != 0 {
		t.Errorf("After RemoveTask, GetTaskRate = %d, want 0", got)
	}
}

func TestGlobalRateLimiterWaitTaskOnly(t *testing.T) {
	g := &GlobalRateLimiter{
		limiter:   NewRateLimiter(0, 1024),
		taskLimit: make(map[string]*RateLimiter),
	}
	g.SetTaskRate("task-1", 0)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	if err := g.Wait(ctx, "task-1", 1024); err != nil {
		t.Errorf("Wait on task with rate=0 should return nil, got %v", err)
	}
	if elapsed := time.Since(start); elapsed > 20*time.Millisecond {
		t.Errorf("Wait should be near-instant for zero rate, took %v", elapsed)
	}
}

func TestGlobalRateLimiterWaitGlobalWithZeroRate(t *testing.T) {
	g := &GlobalRateLimiter{
		limiter:   NewRateLimiter(0, 1024),
		taskLimit: make(map[string]*RateLimiter),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	if err := g.WaitGlobal(ctx, 1024); err != nil {
		t.Errorf("WaitGlobal on rate=0 should return nil, got %v", err)
	}
	if elapsed := time.Since(start); elapsed > 20*time.Millisecond {
		t.Errorf("WaitGlobal should be near-instant for zero rate, took %v", elapsed)
	}
}

func TestGlobalRateLimiterWaitCombinesTaskAndGlobal(t *testing.T) {
	g := &GlobalRateLimiter{
		limiter:   NewRateLimiter(0, 1024),
		taskLimit: make(map[string]*RateLimiter),
	}
	g.SetTaskRate("task-1", 0)
	g.SetGlobalRate(0)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	if err := g.Wait(ctx, "task-1", 1024); err != nil {
		t.Errorf("combined Wait should return nil when both rates are 0, got %v", err)
	}
	if elapsed := time.Since(start); elapsed > 20*time.Millisecond {
		t.Errorf("combined Wait should be near-instant when both rates are 0, took %v", elapsed)
	}
}

func TestGlobalRateLimiterSetScheduleLimitsEmpty(t *testing.T) {
	g := &GlobalRateLimiter{
		limiter:   NewRateLimiter(0, 1024),
		taskLimit: make(map[string]*RateLimiter),
	}
	g.SetScheduleLimits(nil, 1024)
	if g.scheduleActive {
		t.Error("scheduleActive should remain false when no limits provided")
	}
}
