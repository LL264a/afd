package task

import (
	"context"
	"crypto/rand"
	"fmt"
	mrand "math/rand"
	"sync"
	"time"
)

type TaskStatus string

const (
	StatusPending     TaskStatus = "pending"
	StatusDownloading TaskStatus = "downloading"
	StatusPaused      TaskStatus = "paused"
	StatusDone        TaskStatus = "done"
	StatusFailed      TaskStatus = "failed"
	StatusCancelled   TaskStatus = "cancelled"
)

func GenerateID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		// crypto/rand should never fail on a healthy system. Fall back
		// to a time-based pseudo-random so the process does not panic
		// (and we still get collision-resistant IDs for in-process
		// uniqueness within the same nanosecond).
		ts := time.Now().UnixNano()
		for i := 0; i < 8 && i < len(b); i++ {
			b[i] = byte(ts >> (8 * i))
		}
		// 用 math/rand 填充剩余字节，补充熵源
		for i := 8; i < len(b); i++ {
			b[i] = byte(mrand.Intn(256))
		}
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

type ChunkInfo struct {
	Index      int        `json:"index"`
	Start      int64      `json:"start"`
	End        int64      `json:"end"`
	Downloaded int64      `json:"downloaded"`
	Status     TaskStatus `json:"status"`
}

type Task struct {
	mu             sync.RWMutex
	ID             string            `json:"id"`
	URL            string            `json:"url"`
	OutputPath     string            `json:"output_path"`
	Status         TaskStatus        `json:"status"`
	TotalSize      int64             `json:"total_size"`
	DownloadedSize int64             `json:"downloaded_size"`
	Speed          int64             `json:"speed"`
	CreatedAt      time.Time         `json:"created_at"`
	UpdatedAt      time.Time         `json:"updated_at"`
	TargetNode     string            `json:"target_node"`
	Protocol       string            `json:"protocol"`
	Metadata       map[string]string `json:"metadata"`
	Chunks         []ChunkInfo       `json:"chunks"`
	Error          string            `json:"error"`
	Priority       int               `json:"priority"`
	ChecksumType   string            `json:"checksum_type"`
	ChecksumValue  string            `json:"checksum_value"`
	ctx            context.Context
	cancel         context.CancelFunc
}

func NewTask(url, outputPath string) *Task {
	now := time.Now()
	return &Task{
		ID:         GenerateID(),
		URL:        url,
		OutputPath: outputPath,
		Status:     StatusPending,
		CreatedAt:  now,
		UpdatedAt:  now,
		Metadata:   make(map[string]string),
	}
}

func (t *Task) ProgressPercent() float64 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.TotalSize == 0 {
		return 0
	}
	return float64(t.DownloadedSize) / float64(t.TotalSize) * 100
}

func (t *Task) IsCompleted() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.Status == StatusDone || t.Status == StatusFailed || t.Status == StatusCancelled
}

func (t *Task) IsActive() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.Status == StatusPending || t.Status == StatusDownloading
}

func (t *Task) SetStatus(status TaskStatus) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.Status = status
	t.UpdatedAt = time.Now()
}

func (t *Task) SetTargetNode(nodeID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.TargetNode = nodeID
	t.UpdatedAt = time.Now()
}

func (t *Task) SetError(err string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.Error = err
	t.Status = StatusFailed
	t.UpdatedAt = time.Now()
}

func (t *Task) SetPriority(priority int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.Priority = priority
	t.UpdatedAt = time.Now()
}

func (t *Task) SetMetadata(metadata map[string]string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.Metadata = metadata
	t.UpdatedAt = time.Now()
}

func (t *Task) UpdateProgress(downloaded, total, speed int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if downloaded < 0 {
		downloaded = 0
	}
	if total < 0 {
		total = 0
	}
	t.DownloadedSize = downloaded
	t.TotalSize = total
	t.Speed = speed
	t.UpdatedAt = time.Now()
}

func (t *Task) AddChunk(index int, start, end int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.Chunks = append(t.Chunks, ChunkInfo{
		Index:  index,
		Start:  start,
		End:    end,
		Status: StatusPending,
	})
}

func (t *Task) GetStatus() TaskStatus {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.Status
}

func (t *Task) GetSafe() Task {
	t.mu.RLock()
	defer t.mu.RUnlock()
	metadataCopy := make(map[string]string, len(t.Metadata))
	for k, v := range t.Metadata {
		metadataCopy[k] = v
	}
	chunksCopy := make([]ChunkInfo, len(t.Chunks))
	copy(chunksCopy, t.Chunks)
	return Task{
		ID:             t.ID,
		URL:            t.URL,
		OutputPath:     t.OutputPath,
		Status:         t.Status,
		TotalSize:      t.TotalSize,
		DownloadedSize: t.DownloadedSize,
		Speed:          t.Speed,
		CreatedAt:      t.CreatedAt,
		UpdatedAt:      t.UpdatedAt,
		TargetNode:     t.TargetNode,
		Protocol:       t.Protocol,
		Metadata:       metadataCopy,
		Chunks:         chunksCopy,
		Error:          t.Error,
		Priority:       t.Priority,
		ChecksumType:   t.ChecksumType,
		ChecksumValue:  t.ChecksumValue,
	}
}

func (t *Task) SetContext(ctx context.Context) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.ctx = ctx
}

func (t *Task) GetContext() context.Context {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.ctx
}

func (t *Task) SetCancel(cancel context.CancelFunc) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.cancel = cancel
}

func (t *Task) Cancel() {
	t.mu.Lock()
	cancel := t.cancel
	t.Status = StatusCancelled
	t.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}
