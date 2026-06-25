package downloader

import (
	"context"
	"fmt"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/nexus-dl/afd/internal"
	"github.com/nexus-dl/afd/internal/api"
	"github.com/nexus-dl/afd/internal/task"
	"github.com/nexus-dl/afd/pkg/config"
	"github.com/nexus-dl/afd/pkg/logger"
)

type activeDownload struct {
	task          *task.Task
	cancel        context.CancelFunc
	done          chan struct{}
	lowSpeedSince time.Time
	startTime     time.Time
}

type DownloadManager struct {
	mu            sync.RWMutex
	taskQueue     *task.TaskQueue
	taskStore     *task.TaskStore
	hub           *api.WebSocketHub
	downloadCfg   *config.DownloadConfig
	active        map[string]*activeDownload
	stopCh        chan struct{}
	stopOnce      sync.Once
	wg            sync.WaitGroup
	eventEmitter  *internal.EventEmitter
	postProcessor *internal.PostProcessor
}

func NewDownloadManager(taskQueue *task.TaskQueue, taskStore *task.TaskStore, hub *api.WebSocketHub, downloadCfg *config.DownloadConfig, eventEmitter *internal.EventEmitter, postProcessor *internal.PostProcessor) *DownloadManager {
	return &DownloadManager{
		taskQueue:     taskQueue,
		taskStore:     taskStore,
		hub:           hub,
		downloadCfg:   downloadCfg,
		active:        make(map[string]*activeDownload),
		stopCh:        make(chan struct{}),
		eventEmitter:  eventEmitter,
		postProcessor: postProcessor,
	}
}

// failTask 统一处理下载失败时的事件发射、WebSocket 通知和任务队列状态更新
func (m *DownloadManager) failTask(taskID string, errMsg string) {
	if m.eventEmitter != nil {
		m.eventEmitter.EmitTaskFailed(taskID, map[string]interface{}{"error": errMsg})
	}
	if m.hub != nil {
		m.hub.BroadcastNotification("aria2.onDownloadError", taskID)
	}
	m.taskQueue.FailTask(taskID, errMsg)
}

func (m *DownloadManager) Start() {
	logger.Log.Info("Download manager started")

	m.wg.Add(1)
	go func() {
		defer m.wg.Done()

		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-m.stopCh:
				logger.Log.Info("Download manager stopping")
				return
			case <-ticker.C:
				m.updateProgress()
			}
		}
	}()
}

func (m *DownloadManager) Stop() {
	logger.Log.Info("Stopping download manager...")
	m.stopOnce.Do(func() {
		close(m.stopCh)
	})

	m.mu.Lock()
	for id, dl := range m.active {
		if dl.cancel != nil {
			dl.cancel()
		}
		logger.Log.Infow("Stopping download", "task_id", id)
	}
	m.mu.Unlock()

	m.wg.Wait()

	logger.Log.Info("Download manager stopped")
}

func (m *DownloadManager) StartDownload(t *task.Task) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 检查是否正在关闭
	select {
	case <-m.stopCh:
		logger.Log.Warnw("DownloadManager is stopping, rejecting new download", "task_id", t.ID)
		return
	default:
	}

	if _, exists := m.active[t.ID]; exists {
		logger.Log.Warnw("Download already active", "task_id", t.ID)
		return
	}

	ctx, cancel := context.WithCancel(context.Background())

	outputPath := t.OutputPath
	if outputPath == "" {
		// 清理 URL 中的查询参数和片段
		u, err := url.Parse(t.URL)
		if err == nil {
			outputPath = filepath.Join("data", "downloads", filepath.Base(u.Path))
		} else {
			outputPath = filepath.Join("data", "downloads", filepath.Base(t.URL))
		}
	}

	dl := &activeDownload{
		task:      t,
		cancel:    cancel,
		done:      make(chan struct{}),
		startTime: time.Now(),
	}
	m.active[t.ID] = dl

	// 发射开始事件
	if m.eventEmitter != nil {
		m.eventEmitter.EmitTaskStarted(t.ID, map[string]interface{}{
			"url":         t.URL,
			"output_path": outputPath,
		})
	}

	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		defer cancel()
		defer close(dl.done)
		defer func() {
			m.mu.Lock()
			delete(m.active, t.ID)
			m.mu.Unlock()
		}()

		d, err := NewDownloaderFromURL(t.URL, outputPath, m.downloadCfg, logger.Log.Named("downloader"))
		if err != nil {
			t.SetError(fmt.Sprintf("failed to create downloader: %v", err))
			m.failTask(t.ID, err.Error())
			return
		}

		t.SetContext(ctx)

		// 推送 aria2 兼容的下载开始通知
		if m.hub != nil {
			m.hub.BroadcastNotification("aria2.onDownloadStart", t.ID)
		}

		// 启动进度监控 goroutine
		progressCtx, progressCancel := context.WithCancel(ctx)
		defer progressCancel()
		m.wg.Add(1)
		go func() {
			defer m.wg.Done()
			m.monitorDownloadProgress(progressCtx, t, d)
		}()

		err = d.Download(ctx)
		if err != nil {
			if ctx.Err() == context.Canceled {
				logger.Log.Infow("Download cancelled", "task_id", t.ID)
				if m.eventEmitter != nil {
					m.eventEmitter.EmitTaskPaused(t.ID, map[string]interface{}{"reason": "cancelled"})
				}
				if m.hub != nil {
					m.hub.BroadcastNotification("aria2.onDownloadStop", t.ID)
				}
				return
			}
			t.SetError(err.Error())
			m.failTask(t.ID, err.Error())
			return
		}

		// 校验完整性（仅在启用 CheckIntegrity 时执行）
		if m.downloadCfg.CheckIntegrity {
			valid, err := t.VerifyDownload()
			if err != nil {
				t.SetError(fmt.Sprintf("checksum verification failed: %v", err))
				if m.eventEmitter != nil {
					m.eventEmitter.EmitTaskFailed(t.ID, map[string]interface{}{"error": err.Error()})
				}
				if m.hub != nil {
					m.hub.BroadcastNotification("aria2.onDownloadError", t.ID)
				}
				m.taskQueue.FailTask(t.ID, fmt.Sprintf("checksum verification failed: %v", err))
				return
			}

			if !valid {
				t.SetError("checksum mismatch")
				m.failTask(t.ID, "checksum mismatch")
				return
			}
		}

		// 后处理
		if m.postProcessor != nil {
			// 检查是否是压缩文件
			ext := filepath.Ext(outputPath)
			isArchive := ext == ".zip" || ext == ".rar" || ext == ".7z" || ext == ".tar" ||
				strings.HasSuffix(outputPath, ".tar.gz") || strings.HasSuffix(outputPath, ".tgz") ||
				strings.HasSuffix(outputPath, ".tar.bz2") || strings.HasSuffix(outputPath, ".tbz2") ||
				strings.HasSuffix(outputPath, ".tar.xz") || strings.HasSuffix(outputPath, ".txz")

			if isArchive && m.downloadCfg.PostProcess != nil && m.downloadCfg.PostProcess.Extract.Enabled {
				logger.Log.Infow("Extracting archive", "path", outputPath)
				if err := m.postProcessor.ProcessWithExtract(outputPath); err != nil {
					logger.Log.Warnw("Failed to extract archive", "error", err)
				}
			} else if m.downloadCfg.PostProcess != nil {
				if err := m.postProcessor.Process(outputPath); err != nil {
					logger.Log.Warnw("Failed to process file", "error", err)
				}
			}
		}

		// 发射完成事件
		if m.eventEmitter != nil {
			m.eventEmitter.EmitTaskCompleted(t.ID, map[string]interface{}{
				"output_path": outputPath,
			})
		}

		// 推送 aria2 兼容的下载完成通知（BT 下载使用 onBtDownloadComplete）
		if m.hub != nil {
			if IsMagnetLink(t.URL) || IsTorrentFile(t.URL) {
				m.hub.BroadcastNotification("aria2.onBtDownloadComplete", t.ID)
			} else {
				m.hub.BroadcastNotification("aria2.onDownloadComplete", t.ID)
			}
		}

		m.taskQueue.CompleteTask(t.ID)
	}()
}

func (m *DownloadManager) updateProgress() {
	m.mu.RLock()
	tasks := make([]*task.Task, 0, len(m.active))
	for _, dl := range m.active {
		tasks = append(tasks, dl.task)
	}
	m.mu.RUnlock()
	for _, t := range tasks {
		m.hub.BroadcastTaskUpdate(t)
	}
}

// PauseDownload 取消下载 context 并更新任务队列状态
func (m *DownloadManager) PauseDownload(taskID string) error {
	m.mu.Lock()
	dl, ok := m.active[taskID]
	m.mu.Unlock()
	if !ok {
		return fmt.Errorf("task %s not active", taskID)
	}
	// 推送 aria2 兼容的下载暂停通知
	if m.hub != nil {
		m.hub.BroadcastNotification("aria2.onDownloadPause", taskID)
	}
	dl.cancel()
	// 等待下载 goroutine 退出
	<-dl.done
	// 从 active 中移除
	m.mu.Lock()
	delete(m.active, taskID)
	m.mu.Unlock()
	return nil
}

func (m *DownloadManager) GetActiveDownloads() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	ids := make([]string, 0, len(m.active))
	for id := range m.active {
		ids = append(ids, id)
	}
	return ids
}

func (m *DownloadManager) monitorDownloadProgress(ctx context.Context, t *task.Task, d DownloaderInterface) {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			progress := d.Progress()
			speed := d.Speed()
			downloaded := d.TotalDownloaded()

			if m.eventEmitter != nil {
				m.eventEmitter.EmitTaskProgress(t.ID, map[string]interface{}{
					"progress":   progress,
					"speed":      speed,
					"downloaded": downloaded,
				})
				m.eventEmitter.EmitDownloadSpeedChanged(t.ID, map[string]interface{}{
					"speed": speed,
				})
			}

			// 检查最低速度
			if m.downloadCfg.MinSpeed > 0 {
				m.mu.Lock()
				activeDl, exists := m.active[t.ID]
				var shouldCancel bool
				if exists {
					// 下载开始后 30 秒内不检测低速
					if time.Since(activeDl.startTime) < 30*time.Second {
						m.mu.Unlock()
						continue
					}
					if speed < m.downloadCfg.MinSpeed {
						if activeDl.lowSpeedSince.IsZero() {
							activeDl.lowSpeedSince = time.Now()
						} else if time.Since(activeDl.lowSpeedSince) >= m.downloadCfg.MinSpeedTimeout {
							shouldCancel = true
						}
					} else {
						activeDl.lowSpeedSince = time.Time{}
					}
				}
				m.mu.Unlock()

				if shouldCancel {
					logger.Log.Infow("Download speed too low, failing task", "task_id", t.ID, "speed", speed)
					activeDl.cancel()
					<-activeDl.done
					m.mu.Lock()
					delete(m.active, t.ID)
					m.mu.Unlock()
					m.taskQueue.FailTask(t.ID, "download speed too low")
					continue
				}
			}
		}
	}
}
