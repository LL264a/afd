package downloader

import (
	"bufio"
	"context"
	"crypto/sha1"
	"encoding/gob"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nexus-dl/afd/internal/task"
	"github.com/nexus-dl/afd/pkg/config"
	"go.uber.org/zap"
	"golang.org/x/net/publicsuffix"
)

type Downloader struct {
	cfg              *config.DownloadConfig
	retryConfig      RetryConfig
	client           *http.Client
	logger           *zap.SugaredLogger
	url              string
	altURLs          []string
	outputPath       string
	controlFile      *task.ControlFile
	controlFilePath  string
	rateLimiter      *RateLimiter
	proxy            *config.ProxyConfig
	torrentDownloader *TorrentDownloader
	cookieJar        *cookiejar.Jar
	cookieFile       string

	speedWindow []speedSample
	swHead      int
	swCount     int
	swMu        sync.Mutex

	chunkMu sync.Mutex

	totalDownloaded int64
	fileSize        int64
	startTime       time.Time

	adaptive *adaptiveController

	lastSaveTime    time.Time
	sinceLastSave   int64
	saveInterval    time.Duration
	progressChan    chan struct{}
	done            chan struct{}

	diskCache       *DiskCache
}

var globalTorrentDownloader *TorrentDownloader
var torrentDownloaderOnce sync.Once
var torrentDownloaderErr error

func getGlobalTorrentDownloader(cfg *config.DownloadConfig, logger *zap.SugaredLogger) (*TorrentDownloader, error) {
	torrentDownloaderOnce.Do(func() {
		if cfg.BT == nil || !cfg.BT.Enabled {
			torrentDownloaderErr = nil
			return
		}
		btCfg := &BTConfig{
			Enabled:            cfg.BT.Enabled,
			DownloadSpeedLimit: cfg.BT.DownloadSpeedLimit,
			UploadSpeedLimit:   cfg.BT.UploadSpeedLimit,
			Port:               cfg.BT.Port,
			DHTEnabled:         cfg.BT.DHTEnabled,
		}
		globalTorrentDownloader, torrentDownloaderErr = NewBTDownloader(btCfg, "", ""), nil
	})
	return globalTorrentDownloader, torrentDownloaderErr
}

func NewDownloader(cfg *config.DownloadConfig, logger *zap.SugaredLogger) *Downloader {
	if cfg == nil {
		cfg = config.DefaultDownloadConfig()
	}

	jar, _ := cookiejar.New(&cookiejar.Options{PublicSuffixList: publicsuffix.List})

	maxConnsPerHost := 10
	if cfg.MaxPerServerConn > 0 {
		maxConnsPerHost = cfg.MaxPerServerConn
	}

	transport := &http.Transport{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: maxConnsPerHost,
		MaxConnsPerHost:     maxConnsPerHost,
		IdleConnTimeout:     90 * time.Second,
		TLSHandshakeTimeout: 10 * time.Second,
		ResponseHeaderTimeout: 30 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:   true,
		DisableCompression:  false,
		DisableKeepAlives:   false,
		ReadBufferSize:      32 * 1024,
		WriteBufferSize:     32 * 1024,
		MaxResponseHeaderBytes: 256 * 1024,
	}

	client := &http.Client{
		Timeout:   cfg.Timeout,
		Jar:       jar,
		Transport: transport,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 10 {
				return fmt.Errorf("stopped after 10 redirects")
			}
			return nil
		},
	}

	var proxyCfg *config.ProxyConfig
	if cfg.Proxy != nil && cfg.Proxy.IsValid() {
		proxyCfg = cfg.Proxy
		newClient, err := CreateProxyClient(proxyCfg, cfg.Timeout, proxyCfg.UseDigest, proxyCfg.ExcludeList)
		if err != nil {
			logger.Warnw("failed to create proxy client, using default",
				"error", err,
				"proxy_type", proxyCfg.Type,
			)
		} else {
			client = newClient
		}
	}

	var rateLimiter *RateLimiter
	if cfg.SpeedLimit > 0 {
		rateLimiter = NewRateLimiter(cfg.SpeedLimit, cfg.SpeedLimit)
	}

	retryConfig := DefaultRetryConfig()
	if cfg.RetryCount > 0 {
		retryConfig.MaxRetries = cfg.RetryCount
	}

	d := &Downloader{
		cfg:           cfg,
		retryConfig:   retryConfig,
		client:        client,
		logger:        logger,
		proxy:         proxyCfg,
		cookieJar:     jar,
		speedWindow:   make([]speedSample, 20),
		adaptive:      newAdaptiveController(cfg.MaxConnections, 1),
		saveInterval:  5 * time.Second,
		rateLimiter:   rateLimiter,
		diskCache:     NewDiskCache(),
		done:          make(chan struct{}),
	}

	d.loadCookies()

	return d
}

type DownloaderInterface interface {
	SetURL(url string)
	SetOutputPath(path string)
	SetControlFilePath(path string)
	SetControlFile(cf interface{})
	URL() string
	OutputPath() string
	Download(ctx context.Context) error
	Speed() int64
	Progress() float64
	TotalDownloaded() int64
	ActiveThreads() int32
	SetRateLimit(rate int64)
	GetRateLimit() int64
	SetRetryConfig(config RetryConfig)
	GetRetryConfig() RetryConfig
	LoadProgress(ctx context.Context) error
	SaveProgress() error
}

func NewDownloaderFromURL(url, outputPath string, cfg *config.DownloadConfig, logger *zap.SugaredLogger) (DownloaderInterface, error) {
	if IsMetalinkFile(url) {
		return NewMetalinkDownloader(url, outputPath), nil
	}
	if IsFTPURL(url) {
		return NewFTPDownloader(url, outputPath, cfg, logger)
	}
	if IsS3URL(url) {
		return NewS3Downloader(url, outputPath, cfg, logger)
	}
	if IsWebDAVURL(url) {
		return NewWebDAVDownloader(url, outputPath, cfg, logger)
	}
	if IsTorrentFile(url) || IsMagnetLink(url) {
		// 使用完整的 BT 配置
		btCfg := &BTConfig{
			Enabled:            true,
			DownloadSpeedLimit: 0,
			UploadSpeedLimit:   0,
			Port:               6881,
			DataDir:            "./bt-data",
			MaxConnections:     100,
			MaxPeers:           100,
			TrackerList:        []string{},
			DHTEnabled:         true,
			DisableTCP:         false,
			DisableUTP:         false,
			SequentialDownload: false,
		}
		if cfg != nil && cfg.BT != nil {
			btCfg.Enabled = cfg.BT.Enabled
			btCfg.DownloadSpeedLimit = cfg.BT.DownloadSpeedLimit
			btCfg.UploadSpeedLimit = cfg.BT.UploadSpeedLimit
			btCfg.Port = cfg.BT.Port
			btCfg.DataDir = cfg.BT.DataDir
			btCfg.MaxConnections = cfg.BT.MaxConnections
			btCfg.MaxPeers = cfg.BT.MaxPeers
			btCfg.DHTEnabled = cfg.BT.DHTEnabled
			btCfg.DisableTCP = cfg.BT.DisableTCP
			btCfg.DisableUTP = cfg.BT.DisableUTP
			btCfg.SequentialDownload = cfg.BT.SequentialDownload
		}
		return NewBTDownloader(btCfg, url, outputPath), nil
	}
	d := NewDownloader(cfg, logger)
	d.SetURL(url)
	d.SetOutputPath(outputPath)
	return d, nil
}

func (d *Downloader) SetURL(url string) {
	d.url = url
}

func (d *Downloader) SetOutputPath(path string) {
	d.outputPath = path
}

func (d *Downloader) SetControlFilePath(path string) {
	d.controlFilePath = path
}

func (d *Downloader) SetControlFile(cf interface{}) {
	if cf == nil {
		d.controlFile = nil
		return
	}
	if v, ok := cf.(*task.ControlFile); ok {
		d.controlFile = v
		return
	}
	d.logger.Warnw("SetControlFile: unexpected type, ignoring", "type", fmt.Sprintf("%T", cf))
}

func (d *Downloader) LoadProgress(ctx context.Context) error {
	if d.controlFilePath == "" {
		return nil
	}

	store := task.NewControlFileStore(filepath.Dir(d.controlFilePath))
	taskID := strings.TrimSuffix(filepath.Base(d.controlFilePath), filepath.Ext(d.controlFilePath))

	cf, err := store.Load(taskID)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			return nil
		}
		return fmt.Errorf("load control file: %w", err)
	}

	d.controlFile = cf
	d.logger.Infow("loaded progress from control file",
		"completed_length", cf.CompletedLength,
		"total_length", cf.TotalLength,
	)

	return nil
}

func (d *Downloader) SaveProgress() error {
	if d.controlFilePath == "" {
		return nil
	}

	if d.controlFile == nil {
		d.controlFile = &task.ControlFile{
			Status: "downloading",
		}
	}

	d.controlFile.CompletedLength = atomic.LoadInt64(&d.totalDownloaded)
	d.controlFile.TotalLength = d.fileSize
	d.controlFile.UpdatedAt = time.Now()

	store := task.NewControlFileStore(filepath.Dir(d.controlFilePath))
	taskID := strings.TrimSuffix(filepath.Base(d.controlFilePath), filepath.Ext(d.controlFilePath))

	if err := store.Save(taskID, d.controlFile); err != nil {
		return fmt.Errorf("save progress: %w", err)
	}

	d.lastSaveTime = time.Now()
	d.sinceLastSave = 0

	return nil
}

func (d *Downloader) shouldSaveProgress(bytes int64) bool {
	d.swMu.Lock()
	defer d.swMu.Unlock()

	d.sinceLastSave += bytes
	now := time.Now()

	if d.sinceLastSave >= 1024*1024 {
		return true
	}

	if !d.lastSaveTime.IsZero() && now.Sub(d.lastSaveTime) >= d.saveInterval {
		return true
	}

	return false
}

func (d *Downloader) URL() string {
	return d.url
}

func (d *Downloader) OutputPath() string {
	return d.outputPath
}

func (d *Downloader) Download(ctx context.Context) error {
	d.startTime = time.Now()
	d.lastSaveTime = time.Now()

	return DoWithRetryWithLogger(ctx, d.retryConfig, d.logger, func() error {
		return d.doDownload(ctx)
	})
}

func (d *Downloader) doDownload(ctx context.Context) error {
	if err := d.LoadProgress(ctx); err != nil {
		d.logger.Warnw("failed to load progress", "error", err)
	}

	fileSize, supportsRange, err := d.headRequest(ctx)
	if err != nil {
		return fmt.Errorf("head request: %w", err)
	}
	d.fileSize = fileSize

	d.logger.Infow("starting download",
		"file_size", fileSize,
		"supports_range", supportsRange,
		"url", d.url,
	)

	if d.controlFile != nil && d.controlFile.CompletedLength > 0 {
		localFileSize := d.controlFile.CompletedLength

		stat, err := os.Stat(d.outputPath)
		if err == nil {
			localFileSize = stat.Size()
		}

		if localFileSize == fileSize && fileSize > 0 {
			d.logger.Infow("file already fully downloaded, skipping")
			atomic.StoreInt64(&d.totalDownloaded, fileSize)
			if d.controlFile != nil {
				d.controlFile.Status = "completed"
				d.SaveProgress()
			}
			return nil
		}

		if localFileSize > 0 && localFileSize < fileSize {
			d.logger.Infow("resuming download",
				"local_size", localFileSize,
				"server_size", fileSize,
			)
		}
	}

	if !supportsRange || fileSize <= 0 {
		return d.singleThreadDownload(ctx)
	}

	chunks := d.prepareChunks(fileSize)
	d.logger.Infow("chunks prepared", "total_chunks", len(chunks))

	activeChunks := int32(d.cfg.MaxConnections)
	d.adaptive.setThreadCount(activeChunks)

	sem := make(chan struct{}, d.cfg.MaxConnections)
	var wg sync.WaitGroup
	var errOnce sync.Once
	var downloadErr error

	filePerm := os.FileMode(0644)
	if d.cfg.FileMode != 0 {
		filePerm = d.cfg.FileMode
	}

	file, err := os.OpenFile(d.outputPath, os.O_CREATE|os.O_RDWR, filePerm)
	if err != nil {
		return fmt.Errorf("open output file: %w", err)
	}
	defer file.Close()

	existingSize := int64(0)
	if stat, err := os.Stat(d.outputPath); err == nil {
		existingSize = stat.Size()
	}

	if existingSize < fileSize {
		if d.cfg.PreallocateSpace {
			// 预分配磁盘空间
			if err := preallocateFile(file, fileSize, d.cfg.SparseFile); err != nil {
				d.logger.Warnw("failed to preallocate space, falling back to truncate", "error", err)
				if err := file.Truncate(fileSize); err != nil {
					return fmt.Errorf("truncate output file: %w", err)
				}
			}
		} else {
			if err := file.Truncate(fileSize); err != nil {
				return fmt.Errorf("truncate output file: %w", err)
			}
		}
	}

	// 确保文件权限正确
	if d.cfg.FileMode != 0 {
		if err := os.Chmod(d.outputPath, d.cfg.FileMode); err != nil {
			d.logger.Warnw("failed to set file permissions", "error", err)
		}
	}

	go d.periodicSaveProgress(ctx)

	defer close(d.done)

	for i := 0; i < d.cfg.MaxConnections; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					d.SaveProgress()
					return
				default:
				}

				d.chunkMu.Lock()
				var chunk *Chunk
				for idx := range chunks {
					if chunks[idx].Status == ChunkPending {
						chunks[idx].Status = ChunkDownloading
						chunk = chunks[idx]
						break
					}
				}
				d.chunkMu.Unlock()
				if chunk == nil {
					return
				}

				sem <- struct{}{}

				err := d.downloadChunk(ctx, file, chunk)
				<-sem

				if err != nil {
					chunk.Status = ChunkFailed
					errOnce.Do(func() {
						downloadErr = err
					})
					d.logger.Errorw("chunk download failed",
						"start", chunk.Start,
						"end", chunk.End,
						"error", err,
					)
					d.SaveProgress()
					return
				}

				chunk.Status = ChunkDone
			}
		}()
	}

	wg.Wait()

	if downloadErr != nil {
		d.SaveProgress()
		return downloadErr
	}

	d.SaveProgress()

	if d.controlFile != nil {
		d.controlFile.Status = "completed"
		d.SaveProgress()
	}

	d.logger.Infow("download completed",
		"total_bytes", atomic.LoadInt64(&d.totalDownloaded),
		"duration", time.Since(d.startTime),
	)

	return nil
}

func (d *Downloader) periodicSaveProgress(ctx context.Context) {
	ticker := time.NewTicker(d.saveInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			d.SaveProgress()
			return
		case <-d.done:
			return
		case <-ticker.C:
			d.SaveProgress()
		}
	}
}

func (d *Downloader) headRequest(ctx context.Context) (int64, bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, d.url, nil)
	if err != nil {
		return 0, false, fmt.Errorf("create head request: %w", err)
	}

	resp, err := d.client.Do(req)
	if err != nil {
		return 0, false, fmt.Errorf("head request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, false, fmt.Errorf("head request returned status: %d", resp.StatusCode)
	}

	supportsRange := resp.Header.Get("Accept-Ranges") == "bytes"
	fileSize := resp.ContentLength

	return fileSize, supportsRange, nil
}

func (d *Downloader) prepareChunks(fileSize int64) []*Chunk {
	chunks := SplitFileIntoChunks(fileSize, d.cfg)

	stat, err := os.Stat(d.outputPath)
	if err == nil && stat.Size() > 0 {
		for _, chunk := range chunks {
			if stat.Size() > chunk.End {
				chunk.Status = ChunkDone
				chunk.Downloaded = chunk.Size()
				atomic.AddInt64(&d.totalDownloaded, chunk.Size())
			} else if stat.Size() > chunk.Start {
				// Bytes between the original chunk.Start and stat.Size()
				// are already on disk.  Record them as progress but
				// reset chunk.Downloaded to 0 because downloadChunkOnce
				// uses writeOffset = chunk.Start + chunk.Downloaded —
				// leaving the old value would write at the wrong offset
				// and corrupt the output file.
				alreadyDownloaded := stat.Size() - chunk.Start
				atomic.AddInt64(&d.totalDownloaded, alreadyDownloaded)
				chunk.Start = stat.Size()
				chunk.Downloaded = 0
			}
		}
	}

	return chunks
}

func (d *Downloader) downloadChunk(ctx context.Context, file *os.File, chunk *Chunk) error {
	urls := []string{d.url}
	urls = append(urls, d.altURLs...)

	var lastErr error
	for _, downloadURL := range urls {
		for retry := 0; retry <= d.cfg.RetryCount; retry++ {
			if retry > 0 {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(time.Duration(retry) * time.Second):
				}
			}

			err := d.downloadChunkOnceFromURL(ctx, file, chunk, downloadURL)
			if err == nil {
				return nil
			}

			if ctx.Err() != nil {
				return ctx.Err()
			}

			lastErr = err
			d.logger.Warnw("retrying chunk",
				"retry", retry,
				"start", chunk.Start,
				"end", chunk.End,
				"url", downloadURL,
				"error", err,
			)
		}
		d.logger.Warnw("source failed, trying next", "url", downloadURL, "error", lastErr)
	}

	return fmt.Errorf("chunk download failed from all sources: %w", lastErr)
}

func (d *Downloader) downloadChunkOnce(ctx context.Context, file *os.File, chunk *Chunk) error {
	return d.downloadChunkOnceFromURL(ctx, file, chunk, d.url)
}

func (d *Downloader) downloadChunkOnceFromURL(ctx context.Context, file *os.File, chunk *Chunk, downloadURL string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, downloadURL, nil)
	if err != nil {
		return err
	}

	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", chunk.Start, chunk.End))

	resp, err := d.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusRequestedRangeNotSatisfiable {
		return fmt.Errorf("server does not support range requests (416)")
	}

	if resp.StatusCode != http.StatusPartialContent && resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	buf := make([]byte, d.cfg.BufferSize)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		n, readErr := resp.Body.Read(buf)
		if n > 0 {
			if d.rateLimiter != nil {
				if err := d.rateLimiter.Wait(ctx, int64(n)); err != nil {
					return err
				}
			}

			writeOffset := chunk.Start + chunk.Downloaded
			_, writeErr := file.WriteAt(buf[:n], writeOffset)
			if writeErr != nil {
				return fmt.Errorf("write chunk: %w", writeErr)
			}

			chunk.Downloaded += int64(n)
			atomic.AddInt64(&d.totalDownloaded, int64(n))
			d.recordSpeed(int64(n))

			if d.shouldSaveProgress(int64(n)) {
				d.SaveProgress()
			}
		}

		if readErr != nil {
			if readErr == io.EOF {
				break
			}
			return fmt.Errorf("read chunk: %w", readErr)
		}
	}

	return nil
}

func (d *Downloader) recordSpeed(bytes int64) {
	d.swMu.Lock()
	defer d.swMu.Unlock()

	d.speedWindow[d.swHead] = speedSample{
		timestamp: time.Now(),
		bytes:     bytes,
	}
	d.swHead = (d.swHead + 1) % len(d.speedWindow)
	if d.swCount < len(d.speedWindow) {
		d.swCount++
	}

	d.adaptive.addSample(bytes)
	d.adaptive.shouldAdjust()
}

func (d *Downloader) Speed() int64 {
	d.swMu.Lock()
	defer d.swMu.Unlock()

	if d.swCount == 0 {
		return 0
	}

	var totalBytes int64
	oldest := (d.swHead - d.swCount + len(d.speedWindow)) % len(d.speedWindow)

	for i := 0; i < d.swCount; i++ {
		idx := (oldest + i) % len(d.speedWindow)
		totalBytes += d.speedWindow[idx].bytes
	}

	duration := time.Since(d.speedWindow[oldest].timestamp)
	if duration <= 0 {
		return 0
	}

	return int64(float64(totalBytes) / duration.Seconds())
}

func (d *Downloader) Progress() float64 {
	if d.fileSize <= 0 {
		return 0
	}
	downloaded := atomic.LoadInt64(&d.totalDownloaded)
	return float64(downloaded) / float64(d.fileSize) * 100
}

func (d *Downloader) TotalDownloaded() int64 {
	return atomic.LoadInt64(&d.totalDownloaded)
}

func (d *Downloader) ActiveThreads() int32 {
	return d.adaptive.threadCount()
}

func (d *Downloader) singleThreadDownload(ctx context.Context) error {
	stat, err := os.Stat(d.outputPath)
	existingSize := int64(0)
	if err == nil {
		existingSize = stat.Size()
	}

	if existingSize > 0 && existingSize < d.fileSize {
		d.logger.Infow("resuming single-thread download",
			"existing_size", existingSize,
			"total_size", d.fileSize,
		)
		return d.singleThreadResume(ctx, existingSize)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, d.url, nil)
	if err != nil {
		return err
	}

	resp, err := d.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	file, err := os.Create(d.outputPath)
	if err != nil {
		return err
	}
	defer file.Close()

	buf := make([]byte, d.cfg.BufferSize)
	downloaded := int64(0)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		n, readErr := resp.Body.Read(buf)
		if n > 0 {
			if d.rateLimiter != nil {
				if err := d.rateLimiter.Wait(ctx, int64(n)); err != nil {
					return err
				}
			}

			_, writeErr := file.Write(buf[:n])
			if writeErr != nil {
				return fmt.Errorf("write: %w", writeErr)
			}

			downloaded += int64(n)
			atomic.AddInt64(&d.totalDownloaded, int64(n))
			d.recordSpeed(int64(n))

			if d.shouldSaveProgress(int64(n)) {
				d.SaveProgress()
			}
		}

		if readErr != nil {
			if readErr == io.EOF {
				break
			}
			return fmt.Errorf("read: %w", readErr)
		}
	}

	if resp.ContentLength > 0 {
		d.fileSize = existingSize + resp.ContentLength
	}
	// totalDownloaded was incremented per-read above; for a fresh
	// download existingSize is 0, so the value is already correct.
	d.SaveProgress()
	return nil
}

func (d *Downloader) singleThreadResume(ctx context.Context, existingSize int64) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, d.url, nil)
	if err != nil {
		return err
	}

	req.Header.Set("Range", fmt.Sprintf("bytes=%d-", existingSize))

	resp, err := d.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusRequestedRangeNotSatisfiable {
		d.logger.Warnw("server does not support range requests, restarting from beginning")
		if err := os.Remove(d.outputPath); err != nil {
			return fmt.Errorf("remove partial file: %w", err)
		}
		return d.singleThreadDownload(ctx)
	}

	if resp.StatusCode != http.StatusPartialContent && resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	file, err := os.OpenFile(d.outputPath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	if _, err := file.Seek(existingSize, io.SeekStart); err != nil {
		return fmt.Errorf("seek to position: %w", err)
	}

	atomic.StoreInt64(&d.totalDownloaded, existingSize)

	buf := make([]byte, d.cfg.BufferSize)
	var downloaded int64

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		n, readErr := resp.Body.Read(buf)
		if n > 0 {
			if d.rateLimiter != nil {
				if err := d.rateLimiter.Wait(ctx, int64(n)); err != nil {
					return err
				}
			}

			_, writeErr := file.Write(buf[:n])
			if writeErr != nil {
				return fmt.Errorf("write: %w", writeErr)
			}

			downloaded += int64(n)
			atomic.AddInt64(&d.totalDownloaded, int64(n))
			d.recordSpeed(int64(n))

			if d.shouldSaveProgress(int64(n)) {
				d.SaveProgress()
			}
		}

		if readErr != nil {
			if readErr == io.EOF {
				break
			}
			return fmt.Errorf("read: %w", readErr)
		}
	}

	d.SaveProgress()
	return nil
}

func (d *Downloader) SetRateLimit(rate int64) {
	if d.rateLimiter == nil && rate > 0 {
		d.rateLimiter = NewRateLimiter(rate, rate)
		return
	}

	if d.rateLimiter != nil {
		d.rateLimiter.SetRate(rate)
	}
}

func (d *Downloader) GetRateLimit() int64 {
	if d.rateLimiter == nil {
		return 0
	}
	return d.rateLimiter.GetRate()
}

func (d *Downloader) SetRetryConfig(config RetryConfig) {
	d.retryConfig = config
}

func (d *Downloader) GetRetryConfig() RetryConfig {
	return d.retryConfig
}

func (d *Downloader) getCookieFilePath() string {
	if d.cookieFile != "" {
		return d.cookieFile
	}
	hash := sha1.Sum([]byte(d.url))
	return filepath.Join(os.TempDir(), fmt.Sprintf("nexus-dl-cookies-%x.gob", hash))
}

func (d *Downloader) loadCookies() error {
	if d.cookieJar == nil {
		return nil
	}

	path := d.getCookieFilePath()
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer file.Close()

	var cookies []*http.Cookie
	decoder := gob.NewDecoder(file)
	if err := decoder.Decode(&cookies); err != nil {
		return err
	}

	if u, err := url.Parse(d.url); err == nil {
		d.cookieJar.SetCookies(u, cookies)
		d.logger.Debugw("Loaded cookies", "count", len(cookies))
	}

	return nil
}

func (d *Downloader) saveCookies() error {
	if d.cookieJar == nil {
		return nil
	}

	path := d.getCookieFilePath()
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	if u, err := url.Parse(d.url); err == nil {
		cookies := d.cookieJar.Cookies(u)
		encoder := gob.NewEncoder(file)
		if err := encoder.Encode(cookies); err != nil {
			return err
		}
		d.logger.Debugw("Saved cookies", "count", len(cookies))
	}

	return nil
}

func (d *Downloader) SetCookieFile(path string) {
	d.cookieFile = path
}

func (d *Downloader) SetAltURLs(urls []string) {
	d.altURLs = urls
}

func (d *Downloader) GetAltURLs() []string {
	return d.altURLs
}

type DiskCache struct {
	cacheDir   string
	maxSize    int64
	curSize    int64
	cacheMap   map[string]*cacheItem
	mu         sync.Mutex
}

// ServerConnectionLimiter 每个服务器连接数限制器
type ServerConnectionLimiter struct {
	mu         sync.Mutex
	connCounts map[string]int
	maxConns   int
	cond       *sync.Cond
}

func NewServerConnectionLimiter(maxConns int) *ServerConnectionLimiter {
	limiter := &ServerConnectionLimiter{
		connCounts: make(map[string]int),
		maxConns:   maxConns,
	}
	limiter.cond = sync.NewCond(&limiter.mu)
	return limiter
}

func (s *ServerConnectionLimiter) Acquire(server string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for s.connCounts[server] >= s.maxConns {
		s.cond.Wait()
	}
	s.connCounts[server]++
}

func (s *ServerConnectionLimiter) Release(server string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.connCounts[server]--
	if s.connCounts[server] < 0 {
		s.connCounts[server] = 0
	}
	s.cond.Broadcast()
}

type cacheItem struct {
	path     string
	size     int64
	lastUsed time.Time
}

func NewDiskCache() *DiskCache {
	return &DiskCache{
		cacheDir: filepath.Join(os.TempDir(), "nexus-dl-cache"),
		maxSize:  1024 * 1024 * 1024,
		cacheMap: make(map[string]*cacheItem),
	}
}

func (c *DiskCache) ensureCacheDir() error {
	return os.MkdirAll(c.cacheDir, 0755)
}

func (c *DiskCache) getCachePath(key string) string {
	hash := sha1.Sum([]byte(key))
	return filepath.Join(c.cacheDir, fmt.Sprintf("%x", hash))
}

func (c *DiskCache) Get(key string) (io.ReadCloser, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	item, ok := c.cacheMap[key]
	if !ok {
		return nil, os.ErrNotExist
	}

	item.lastUsed = time.Now()
	return os.Open(item.path)
}

func (c *DiskCache) Put(key string, data io.Reader) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.ensureCacheDir(); err != nil {
		return err
	}

	path := c.getCachePath(key)
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	size, err := io.Copy(writer, data)
	if err != nil {
		return err
	}
	writer.Flush()

	c.cacheMap[key] = &cacheItem{
		path:     path,
		size:     size,
		lastUsed: time.Now(),
	}
	c.curSize += size

	c.evictIfNeeded()
	return nil
}

func (c *DiskCache) evictIfNeeded() {
	if c.curSize <= c.maxSize {
		return
	}

	keys := make([]string, 0, len(c.cacheMap))
	for k := range c.cacheMap {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		return c.cacheMap[keys[i]].lastUsed.Before(c.cacheMap[keys[j]].lastUsed)
	})

	for _, key := range keys {
		item := c.cacheMap[key]
		os.Remove(item.path)
		c.curSize -= item.size
		delete(c.cacheMap, key)
		if c.curSize <= c.maxSize*3/4 {
			break
		}
	}
}

func (c *DiskCache) Clear() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, item := range c.cacheMap {
		os.Remove(item.path)
	}
	c.cacheMap = make(map[string]*cacheItem)
	c.curSize = 0
	return os.RemoveAll(c.cacheDir)
}

func preallocateFile(file *os.File, size int64, sparse bool) error {
	// 尝试使用不同的预分配策略
	stat, err := file.Stat()
	if err != nil {
		return err
	}
	if stat.Size() >= size {
		return nil
	}
	
	if sparse {
		// 稀疏文件：只设置文件大小，不预先分配磁盘块
		if err := file.Truncate(size); err != nil {
			return err
		}
		return nil
	}
	
	// 先尝试通过写入最后一个字节来强制分配
	if _, err := file.Seek(size-1, 0); err != nil {
		return err
	}
	if _, err := file.Write([]byte{0}); err != nil {
		return err
	}
	
	// 现在 truncate 到正确大小
	return file.Truncate(size)
}