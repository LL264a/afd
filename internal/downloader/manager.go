package downloader

import (
	"context"
	"fmt"
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
	task         *task.Task
	cancel       context.CancelFunc
	done         chan struct{}
	lowSpeedSince time.Time
}

type DownloadManager struct {
	mu            sync.RWMutex
	taskQueue     *task.TaskQueue
	taskStore     *task.TaskStore
	hub           *api.WebSocketHub
	downloadCfg   *config.DownloadConfig
	active        map[string]*activeDownload
	stopCh        chan struct{}
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
	close(m.stopCh)
	
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
	
	if _, exists := m.active[t.ID]; exists {
		logger.Log.Warnw("Download already active", "task_id", t.ID)
		return
	}
	
	ctx, cancel := context.WithCancel(context.Background())
	
	outputPath := t.OutputPath
	if outputPath == "" {
		outputPath = filepath.Join("data", "downloads", filepath.Base(t.URL))
	}
	
	dl := &activeDownload{
		task:   t,
		cancel: cancel,
		done:   make(chan struct{}),
	}
	m.active[t.ID] = dl
	
	// 发射开始事件
	if m.eventEmitter != nil {
		m.eventEmitter.EmitTaskStarted(t.ID, map[string]interface{}{
			"url":          t.URL,
			"output_path": outputPath,
		})
	}
	
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		defer func() {
			m.mu.Lock()
			delete(m.active, t.ID)
			m.mu.Unlock()
		}()
		
		d, err := NewDownloaderFromURL(t.URL, outputPath, m.downloadCfg, logger.Log.Named("downloader"))
		if err != nil {
			t.SetError(fmt.Sprintf("failed to create downloader: %v", err))
			if m.eventEmitter != nil {
				m.eventEmitter.EmitTaskFailed(t.ID, map[string]interface{}{"error": err.Error()})
			}
			m.taskQueue.FailTask(t.ID, err.Error())
			return
		}
		
		t.SetContext(ctx)
		
		// 启动进度监控 goroutine
		progressCtx, progressCancel := context.WithCancel(ctx)
		defer progressCancel()
		go m.monitorDownloadProgress(progressCtx, t, d)
		
		err = d.Download(ctx)
		if err != nil {
			if ctx.Err() == context.Canceled {
				logger.Log.Infow("Download cancelled", "task_id", t.ID)
				if m.eventEmitter != nil {
					m.eventEmitter.EmitTaskPaused(t.ID, map[string]interface{}{"reason": "cancelled"})
				}
				return
			}
			t.SetError(err.Error())
			if m.eventEmitter != nil {
				m.eventEmitter.EmitTaskFailed(t.ID, map[string]interface{}{"error": err.Error()})
			}
			m.taskQueue.FailTask(t.ID, err.Error())
			return
		}
		
		valid, err := t.VerifyDownload()
		if err != nil {
			t.SetError(fmt.Sprintf("checksum verification failed: %v", err))
			if m.eventEmitter != nil {
				m.eventEmitter.EmitTaskFailed(t.ID, map[string]interface{}{"error": err.Error()})
			}
			m.taskQueue.FailTask(t.ID, fmt.Sprintf("checksum verification failed: %v", err))
			return
		}
		
		if !valid {
			t.SetError("checksum mismatch")
			if m.eventEmitter != nil {
				m.eventEmitter.EmitTaskFailed(t.ID, map[string]interface{}{"error": "checksum mismatch"})
			}
			m.taskQueue.FailTask(t.ID, "checksum mismatch")
			return
		}
		
		// 后处理
		if m.postProcessor != nil {
			// 检查是否是压缩文件
			ext := filepath.Ext(outputPath)
			isArchive := ext == ".zip" || ext == ".rar" || ext == ".7z" || 
				strings.HasSuffix(ext, ".tar.gz") || strings.HasSuffix(ext, ".tgz") ||
				strings.HasSuffix(ext, ".tar.bz2") || strings.HasSuffix(ext, ".tbz2") ||
				strings.HasSuffix(ext, ".tar.xz") || strings.HasSuffix(ext, ".txz") ||
				ext == ".tar"
			
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
		
		m.taskQueue.CompleteTask(t.ID)
	}()
}

func (m *DownloadManager) updateProgress() {
	m.mu.RLock()
	for _, dl := range m.active {
		m.hub.BroadcastTaskUpdate(dl.task)
	}
	m.mu.RUnlock()
}

func (m *DownloadManager) PauseDownload(id string) error {
	m.mu.Lock()
	dl, exists := m.active[id]
	if !exists {
		m.mu.Unlock()
		return fmt.Errorf("download %s not active", id)
	}
	cancelFn := dl.cancel
	m.mu.Unlock()

	if cancelFn != nil {
		cancelFn()
	}

	logger.Log.Infow("Download paused", "task_id", id)
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
					"progress":    progress,
					"speed":       speed,
					"downloaded":  downloaded,
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
					logger.Log.Infow("Download speed too low, pausing", "task_id", t.ID, "speed", speed)
					m.PauseDownload(t.ID)
				}
			}
		}
	}
}