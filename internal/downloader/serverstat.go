package downloader

import (
	"net/url"
	"sort"
	"sync"
	"time"
)

// ServerStat 记录单个服务器的速度统计
type ServerStat struct {
	URL          string
	Speed        int64 // 最近平均速度 (bytes/s)
	Connections  int   // 当前连接数
	FailCount    int   // 连续失败次数
	LastUpdate   time.Time
	speedSamples []int64 // 速度采样窗口
	sampleMu     sync.Mutex
}

// ServerStatMan 管理所有服务器的速度统计
type ServerStatMan struct {
	stats map[string]*ServerStat // key: server URL (scheme://host)
	mu    sync.RWMutex
}

func NewServerStatMan() *ServerStatMan {
	return &ServerStatMan{stats: make(map[string]*ServerStat)}
}

// RecordSpeed 记录服务器速度
func (m *ServerStatMan) RecordSpeed(rawURL string, speed int64) {
	server := extractServer(rawURL)
	m.mu.Lock()
	defer m.mu.Unlock()

	stat, ok := m.stats[server]
	if !ok {
		stat = &ServerStat{URL: server}
		m.stats[server] = stat
	}

	stat.sampleMu.Lock()
	stat.speedSamples = append(stat.speedSamples, speed)
	if len(stat.speedSamples) > 10 {
		stat.speedSamples = stat.speedSamples[1:]
	}
	// 计算平均速度
	var sum int64
	for _, s := range stat.speedSamples {
		sum += s
	}
	stat.Speed = sum / int64(len(stat.speedSamples))
	stat.sampleMu.Unlock()

	stat.LastUpdate = time.Now()
	stat.FailCount = 0 // 成功记录速度，重置失败计数
}

// RecordFailure 记录服务器失败
func (m *ServerStatMan) RecordFailure(rawURL string) {
	server := extractServer(rawURL)
	m.mu.Lock()
	defer m.mu.Unlock()

	stat, ok := m.stats[server]
	if !ok {
		stat = &ServerStat{URL: server}
		m.stats[server] = stat
	}
	stat.FailCount++
}

// GetSpeed 获取服务器速度
func (m *ServerStatMan) GetSpeed(rawURL string) int64 {
	server := extractServer(rawURL)
	m.mu.RLock()
	defer m.mu.RUnlock()

	if stat, ok := m.stats[server]; ok {
		stat.sampleMu.Lock()
		defer stat.sampleMu.Unlock()
		return stat.Speed
	}
	return 0
}

// GetFailCount 获取服务器连续失败次数
func (m *ServerStatMan) GetFailCount(rawURL string) int {
	server := extractServer(rawURL)
	m.mu.RLock()
	defer m.mu.RUnlock()
	if stat, ok := m.stats[server]; ok {
		return stat.FailCount
	}
	return 0
}

// SortURLsBySelector 根据选择器策略排序 URL 列表
func (m *ServerStatMan) SortURLsBySelector(urls []string, selector string) []string {
	if len(urls) <= 1 {
		return urls
	}

	switch selector {
	case "inorder", "":
		return urls // 保持原序

	case "feedback":
		// 按速度降序排列（快的优先）
		sorted := make([]string, len(urls))
		copy(sorted, urls)
		sort.Slice(sorted, func(i, j int) bool {
			si := m.GetSpeed(sorted[i])
			sj := m.GetSpeed(sorted[j])
			if si != sj {
				return si > sj // 速度高的在前
			}
			// 速度相同，失败次数少的优先
			return m.GetFailCount(sorted[i]) < m.GetFailCount(sorted[j])
		})
		return sorted

	case "adaptive":
		// adaptive: 在 feedback 基础上，对无速度数据的 URL 给予初始高优先级（探索）
		sorted := make([]string, len(urls))
		copy(sorted, urls)
		sort.Slice(sorted, func(i, j int) bool {
			si := m.GetSpeed(sorted[i])
			sj := m.GetSpeed(sorted[j])
			// 无速度数据的 URL 排在前面（探索新源）
			if si == 0 && sj > 0 {
				return true
			}
			if si > 0 && sj == 0 {
				return false
			}
			if si != sj {
				return si > sj
			}
			return m.GetFailCount(sorted[i]) < m.GetFailCount(sorted[j])
		})
		return sorted

	default:
		return urls
	}
}

// extractServer 提取 scheme://host:port 部分
func extractServer(rawURL string) string {
	if u, err := url.Parse(rawURL); err == nil {
		return u.Scheme + "://" + u.Host
	}
	return rawURL
}
