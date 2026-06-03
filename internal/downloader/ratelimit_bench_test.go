package downloader

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
)

func BenchmarkRateLimiterAllow(b *testing.B) {
	r := NewRateLimiter(1<<30, 1<<20)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			r.Allow(1024)
		}
	})
}

func BenchmarkRateLimiterSetRate(b *testing.B) {
	r := NewRateLimiter(1024, 1024)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.SetRate(int64(1024 + i%1000))
	}
}

func BenchmarkGlobalRateLimiterWaitZero(b *testing.B) {
	g := &GlobalRateLimiter{
		limiter:   NewRateLimiter(0, 1024),
		taskLimit: make(map[string]*RateLimiter),
	}
	g.SetTaskRate("bench", 0)
	g.SetGlobalRate(0)
	ctx := newBenchCtx(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := g.Wait(ctx, "bench", 1); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGlobalRateLimiterConcurrentSetGet(b *testing.B) {
	g := &GlobalRateLimiter{
		limiter:   NewRateLimiter(0, 1024),
		taskLimit: make(map[string]*RateLimiter),
	}
	var wg sync.WaitGroup
	var counter int64
	keys := []string{"a", "b", "c", "d", "e"}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			g.SetTaskRate(keys[int(atomic.AddInt64(&counter, 1))%len(keys)], 1024)
		}()
		go func() {
			defer wg.Done()
			g.GetTaskRate(keys[int(atomic.AddInt64(&counter, 1))%len(keys)])
		}()
	}
	wg.Wait()
}

func BenchmarkTokenBucketRefill(b *testing.B) {
	r := NewRateLimiter(1<<20, 1<<20)
	for i := 0; i < b.N; i++ {
		r.refill()
	}
}

func newBenchCtx(b *testing.B) context.Context {
	b.Helper()
	return context.Background()
}
