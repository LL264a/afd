package downloader

import (
	"context"
	"sync"
	"time"

	"github.com/nexus-dl/afd/pkg/config"
)

type RateLimiter struct {
	rate      int64
	burst     int64
	tokens    int64
	lastUpdate time.Time
	mu        sync.Mutex
}

func NewRateLimiter(rate int64, burst int64) *RateLimiter {
	if burst <= 0 {
		burst = rate
	}
	if burst <= 0 {
		burst = 1024 * 1024
	}

	return &RateLimiter{
		rate:       rate,
		burst:      burst,
		tokens:     burst,
		lastUpdate: time.Now(),
	}
}

func (r *RateLimiter) Allow(n int64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.refill()

	if r.tokens >= n {
		r.tokens -= n
		return true
	}

	return false
}

func (r *RateLimiter) Wait(ctx context.Context, n int64) error {
	if r.rate <= 0 {
		return nil
	}

	for {
		r.mu.Lock()
		r.refill()

		if r.tokens >= n {
			r.tokens -= n
			r.mu.Unlock()
			return nil
		}

		needed := n - r.tokens
		waitTime := time.Duration(float64(needed) / float64(r.rate) * float64(time.Second))
		r.mu.Unlock()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(waitTime):
		}
	}
}

func (r *RateLimiter) SetRate(rate int64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.rate = rate
	if rate <= 0 {
		r.tokens = r.burst
	}
}

func (r *RateLimiter) GetRate() int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.rate
}

func (r *RateLimiter) refill() {
	now := time.Since(r.lastUpdate)
	if now <= 0 {
		return
	}

	elapsedSeconds := now.Seconds()
	addTokens := float64(r.rate) * elapsedSeconds

	r.tokens += int64(addTokens)
	if r.tokens > r.burst {
		r.tokens = r.burst
	}

	r.lastUpdate = time.Now()
}

type GlobalRateLimiter struct {
	limiter         *RateLimiter
	mu              sync.RWMutex
	taskLimit       map[string]*RateLimiter
	scheduleLimits  []config.ScheduleSpeedLimit
	defaultRate     int64
	scheduleActive  bool
	scheduleCtx     context.Context
	scheduleCancel  context.CancelFunc
}

var (
	globalInstance     *GlobalRateLimiter
	globalInstanceOnce sync.Once
)

func GetGlobalRateLimiter() *GlobalRateLimiter {
	globalInstanceOnce.Do(func() {
		globalInstance = &GlobalRateLimiter{
			limiter:   NewRateLimiter(0, 1024*1024),
			taskLimit: make(map[string]*RateLimiter),
		}
	})
	return globalInstance
}

func (g *GlobalRateLimiter) SetScheduleLimits(limits []config.ScheduleSpeedLimit, defaultRate int64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	
	g.scheduleLimits = limits
	g.defaultRate = defaultRate
	
	// 停止旧的调度器
	if g.scheduleCancel != nil {
		g.scheduleCancel()
	}
	
	if len(limits) == 0 {
		return
	}
	
	// 启动新的调度器
	g.scheduleCtx, g.scheduleCancel = context.WithCancel(context.Background())
	g.scheduleActive = true
	
	go g.runSchedule(g.scheduleCtx)
}

func (g *GlobalRateLimiter) runSchedule(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()
	
	// 初始检查
	g.checkSchedule()
	
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			g.checkSchedule()
		}
	}
}

func (g *GlobalRateLimiter) checkSchedule() {
	g.mu.Lock()
	defer g.mu.Unlock()
	
	now := time.Now()
	currentWeekday := int(now.Weekday())
	currentTime := now.Format("15:04")
	
	for _, limit := range g.scheduleLimits {
		// 检查是否在指定星期
		if limit.Weekday != nil && *limit.Weekday != currentWeekday {
			continue
		}
		
		// 检查时间范围
		if currentTime >= limit.StartTime && currentTime < limit.EndTime {
			if g.limiter.GetRate() != limit.Limit {
				g.limiter.SetRate(limit.Limit)
			}
			return
		}
	}
	
	// 没有匹配的计划，使用默认速率
	if g.limiter.GetRate() != g.defaultRate {
		g.limiter.SetRate(g.defaultRate)
	}
}

func (g *GlobalRateLimiter) StopSchedule() {
	g.mu.Lock()
	defer g.mu.Unlock()
	
	if g.scheduleCancel != nil {
		g.scheduleCancel()
		g.scheduleCancel = nil
		g.scheduleActive = false
	}
}

func (g *GlobalRateLimiter) SetGlobalRate(rate int64) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.limiter == nil {
		g.limiter = NewRateLimiter(rate, rate)
	} else {
		g.limiter.SetRate(rate)
	}
}

func (g *GlobalRateLimiter) GetGlobalRate() int64 {
	g.mu.RLock()
	defer g.mu.RUnlock()

	if g.limiter == nil {
		return 0
	}
	return g.limiter.GetRate()
}

func (g *GlobalRateLimiter) WaitGlobal(ctx context.Context, n int64) error {
	g.mu.RLock()
	limiter := g.limiter
	g.mu.RUnlock()

	if limiter == nil {
		return nil
	}

	return limiter.Wait(ctx, n)
}

func (g *GlobalRateLimiter) SetTaskRate(taskID string, rate int64) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if rate <= 0 {
		delete(g.taskLimit, taskID)
		return
	}

	if _, exists := g.taskLimit[taskID]; !exists {
		g.taskLimit[taskID] = NewRateLimiter(rate, rate)
	} else {
		g.taskLimit[taskID].SetRate(rate)
	}
}

func (g *GlobalRateLimiter) GetTaskRate(taskID string) int64 {
	g.mu.RLock()
	defer g.mu.RUnlock()

	limiter, exists := g.taskLimit[taskID]
	if !exists || limiter == nil {
		return 0
	}
	return limiter.GetRate()
}

func (g *GlobalRateLimiter) WaitTask(ctx context.Context, taskID string, n int64) error {
	g.mu.RLock()
	limiter, exists := g.taskLimit[taskID]
	g.mu.RUnlock()

	if !exists || limiter == nil {
		return nil
	}

	return limiter.Wait(ctx, n)
}

func (g *GlobalRateLimiter) RemoveTask(taskID string) {
	g.mu.Lock()
	defer g.mu.Unlock()

	delete(g.taskLimit, taskID)
}

func (g *GlobalRateLimiter) Wait(ctx context.Context, taskID string, n int64) error {
	if taskID != "" {
		if err := g.WaitTask(ctx, taskID, n); err != nil {
			return err
		}
	}

	return g.WaitGlobal(ctx, n)
}