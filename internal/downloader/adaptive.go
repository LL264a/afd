package downloader

import (
	"sync"
	"sync/atomic"
	"time"
)

type speedSample struct {
	timestamp time.Time
	bytes     int64
}

type adaptiveController struct {
	mu         sync.Mutex
	samples    []speedSample
	windowSize int
	head       int
	count      int

	activeThreads int32
	maxThreads    int32
	minThreads    int32

	totalBytes     int64
	lastCheckBytes int64
	lastCheckTime  time.Time

	speedThresholdLow int64
	speedImproveRatio float64
	adjustInterval    time.Duration
	lastAdjustTime    time.Time
}

func newAdaptiveController(maxThreads, minThreads int) *adaptiveController {
	if minThreads < 1 {
		minThreads = 1
	}
	if maxThreads < minThreads {
		maxThreads = minThreads
	}

	return &adaptiveController{
		samples:           make([]speedSample, 20),
		windowSize:        20,
		maxThreads:        int32(maxThreads),
		minThreads:        int32(minThreads),
		speedThresholdLow: 50 * 1024, // 50 KB/s
		speedImproveRatio: 1.2,
		adjustInterval:    3 * time.Second,
	}
}

func (ac *adaptiveController) addSample(bytes int64) {
	ac.mu.Lock()
	ac.samples[ac.head] = speedSample{
		timestamp: time.Now(),
		bytes:     bytes,
	}
	ac.head = (ac.head + 1) % ac.windowSize
	if ac.count < ac.windowSize {
		ac.count++
	}
	ac.mu.Unlock()
	atomic.AddInt64(&ac.totalBytes, bytes)
}

func (ac *adaptiveController) currentSpeed() int64 {
	ac.mu.Lock()
	defer ac.mu.Unlock()

	if ac.count == 0 {
		return 0
	}

	var totalBytes int64
	oldest := (ac.head - ac.count + ac.windowSize) % ac.windowSize

	for i := 0; i < ac.count; i++ {
		idx := (oldest + i) % ac.windowSize
		totalBytes += ac.samples[idx].bytes
	}

	duration := time.Since(ac.samples[oldest].timestamp)
	if duration <= 0 {
		return 0
	}

	return int64(float64(totalBytes) / duration.Seconds())
}

func (ac *adaptiveController) threadCount() int32 {
	return atomic.LoadInt32(&ac.activeThreads)
}

func (ac *adaptiveController) incrementThreads() int32 {
	for {
		current := atomic.LoadInt32(&ac.activeThreads)
		if current >= ac.maxThreads {
			return current
		}
		if atomic.CompareAndSwapInt32(&ac.activeThreads, current, current+1) {
			return current + 1
		}
	}
}

func (ac *adaptiveController) decrementThreads() int32 {
	for {
		current := atomic.LoadInt32(&ac.activeThreads)
		if current <= ac.minThreads {
			return current
		}
		if atomic.CompareAndSwapInt32(&ac.activeThreads, current, current-1) {
			return current - 1
		}
	}
}

func (ac *adaptiveController) setThreadCount(n int32) {
	atomic.StoreInt32(&ac.activeThreads, n)
}

func (ac *adaptiveController) shouldAdjust() (adjusted bool, newThreads int32) {
	ac.mu.Lock()
	if time.Since(ac.lastAdjustTime) < ac.adjustInterval {
		ac.mu.Unlock()
		return false, 0
	}

	currentBytes := atomic.LoadInt64(&ac.totalBytes)

	elapsed := time.Since(ac.lastCheckTime).Seconds()
	if elapsed <= 0 {
		elapsed = 0.001
	}

	intervalSpeed := int64(float64(currentBytes-ac.lastCheckBytes) / elapsed)

	ac.lastCheckBytes = currentBytes
	ac.lastCheckTime = time.Now()
	ac.lastAdjustTime = time.Now()
	ac.mu.Unlock()

	current := atomic.LoadInt32(&ac.activeThreads)

	// 速度低于阈值且未达最大线程 → 加线程
	if intervalSpeed < ac.speedThresholdLow && current < ac.maxThreads {
		newT := ac.incrementThreads()
		return true, newT
	}

	// 速度足够高时，评估减少线程是否不影响速度
	if current > ac.minThreads && intervalSpeed > ac.speedThresholdLow {
		// 估算单线程速度
		perThreadSpeed := intervalSpeed / int64(current)
		// 预测减少一个线程后的速度
		predictedSpeed := perThreadSpeed * int64(current-1)
		// 如果预测速度仍然高于阈值的 90%，可以减少线程
		if predictedSpeed > int64(float64(intervalSpeed)*0.9) && current > 2 {
			newT := ac.decrementThreads()
			return true, newT
		}
	}

	return false, current
}

func (ac *adaptiveController) reset() {
	ac.mu.Lock()
	defer ac.mu.Unlock()
	ac.head = 0
	ac.count = 0
	ac.lastCheckBytes = 0
	ac.lastCheckTime = time.Now()
	ac.lastAdjustTime = time.Time{}
}
