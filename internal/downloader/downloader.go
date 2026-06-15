package downloader

import (
	"bufio"
	"context"
	"crypto/sha1"
	"crypto/tls"
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
	"strconv"
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
	cfg               *config.DownloadConfig
	retryConfig       RetryConfig
	client            *http.Client
	logger            *zap.SugaredLogger
	url               string
	altURLs           []string
	outputPath        string
	controlFile       *task.ControlFile
	controlFilePath   string
	rateLimiter       *RateLimiter
	proxy             *config.ProxyConfig
	torrentDownloader *TorrentDownloader
	cookieJar         *cookiejar.Jar
	cookieFile        string

	speedWindow []speedSample
	swHead      int
	swCount     int
	swMu        sync.Mutex

	chunkMu sync.Mutex

	totalDownloaded int64
	fileSize        int64
	startTime       time.Time

	pieceMgr *PieceManager

	done     chan struct{}
	doneOnce sync.Once


	adaptive *adaptiveController

	lastSaveTime  time.Time
	sinceLastSave int64
	saveInterval  time.Duration
	progressChan  chan struct{}

	diskCache *DiskCache
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
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   maxConnsPerHost,
		MaxConnsPerHost:       maxConnsPerHost,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: 60 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:      false,
		DisableCompression:     false,
		DisableKeepAlives:      false,
		ReadBufferSize:         32 * 1024,
		WriteBufferSize:        32 * 1024,
		MaxResponseHeaderBytes: 256 * 1024,
	}

	// 跳过 TLS 证书验证
	if cfg.Insecure {
		transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	}

	client := &http.Client{
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
		cfg:          cfg,
		retryConfig:  retryConfig,
		client:       client,
		logger:       logger,
		proxy:        proxyCfg,
		cookieJar:    jar,
		speedWindow:  make([]speedSample, 20),
		adaptive:     newAdaptiveController(cfg.MaxConnections, 1),
		saveInterval: 5 * time.Second,
		rateLimiter:  rateLimiter,
		diskCache:    NewDiskCache(),
		done:         make(chan struct{}),
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
	FileSize() int64
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

	// CompletedLength 用 totalDownloaded（实际写入字节数），pieceBitfields 用于精确续传
	d.controlFile.CompletedLength = atomic.LoadInt64(&d.totalDownloaded)
	d.controlFile.TotalLength = d.fileSize
	d.controlFile.UpdatedAt = time.Now()

	// 序列化 Piece 级 Block 位图（用于精确续传）
	if d.pieceMgr != nil {
		entries := d.pieceMgr.SerializePieceBitfields()
		if len(entries) > 0 {
			d.controlFile.PieceBitfields = entries
			d.controlFile.NumPieces = len(d.pieceMgr.pieces)
		}
	}

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

func (d *Downloader) FileSize() int64 {
	return d.fileSize
}

func (d *Downloader) Download(ctx context.Context) error {
	d.startTime = time.Now()
	d.lastSaveTime = time.Now()

	return DoWithRetryWithLogger(ctx, d.retryConfig, d.logger, func() error {
		return d.doDownload(ctx)
	})
}

func (d *Downloader) doDownload(ctx context.Context) error {
	// 每次重试时重新初始化 done channel
	d.done = make(chan struct{})
	d.doneOnce = sync.Once{}

	// 确保 done channel 在所有退出路径上都被关闭，通知 periodicSaveProgress 退出
	defer d.doneOnce.Do(func() { close(d.done) })

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
		"adaptive", d.cfg.Adaptive,
		"insecure", d.cfg.Insecure,
		"url", d.url,
	)

	// 使用 controlFile.CompletedLength 判断续传进度（不用 os.Stat，因为预分配文件会导致文件大小等于完整大小）
	resumeCompleted := int64(0)
	if d.controlFile != nil && d.controlFile.CompletedLength > 0 {
		resumeCompleted = d.controlFile.CompletedLength

		if resumeCompleted >= fileSize && fileSize > 0 {
			// 验证本地文件确实存在且大小匹配
			stat, err := os.Stat(d.outputPath)
			if err == nil && stat.Size() == fileSize {
				d.logger.Infow("file already fully downloaded, skipping")
				atomic.StoreInt64(&d.totalDownloaded, fileSize)
				d.controlFile.Status = "completed"
				d.SaveProgress()
				return nil
			}
			// controlFile 说下完了但文件不对，重置进度
			d.logger.Warnw("control file says completed but local file mismatch, re-downloading",
				"control_completed", resumeCompleted,
				"file_size_on_disk", func() int64 {
					if s, e := os.Stat(d.outputPath); e == nil {
						return s.Size()
					}
					return -1
				}(),
			)
			resumeCompleted = 0
		}

		if resumeCompleted > 0 && resumeCompleted < fileSize {
			d.logger.Infow("resuming download",
				"completed_length", resumeCompleted,
				"total_size", fileSize,
			)
		}
	} else {
		// 没有 controlFile，检查本地文件是否存在且完整
		stat, err := os.Stat(d.outputPath)
		if err == nil && stat.Size() == fileSize && fileSize > 0 {
			d.logger.Infow("file already fully downloaded, skipping")
			atomic.StoreInt64(&d.totalDownloaded, fileSize)
			return nil
		}
	}

	if !supportsRange || fileSize <= 0 {
		return d.singleThreadDownload(ctx)
	}

	// 使用 Piece+Block 模型
	pieces := SplitFileIntoPieces(fileSize, d.cfg)
	pm := NewPieceManager(pieces, fileSize)
	d.pieceMgr = pm

	d.logger.Infow("pieces prepared", "total_pieces", len(pieces))

	// 恢复已下载的进度
	if resumeCompleted > 0 {
		// 优先使用 Block 级位图恢复（精确到每个 Block）
		if d.controlFile != nil && len(d.controlFile.PieceBitfields) > 0 {
			pm.RestorePieceBitfields(d.controlFile.PieceBitfields)
			// 统计恢复的已完成 Piece 数
			completedPieces := 0
			for _, p := range pieces {
				if p.IsComplete() {
					completedPieces++
				}
			}
			atomic.StoreInt64(&d.totalDownloaded, pm.TotalCompletedLength())
			d.logger.Infow("resumed progress from block-level bitfields",
				"completed_length", pm.TotalCompletedLength(),
				"total_pieces", len(pieces),
				"completed_pieces", completedPieces,
			)
		} else {
			// 回退到 CompletedLength 粗略恢复
			for _, p := range pieces {
				pieceEnd := p.Start + p.Length
				if resumeCompleted >= pieceEnd {
					numBlocks := p.blocks.NumBlocks()
					for i := 0; i < numBlocks; i++ {
						p.CompleteBlock(i)
					}
					pm.CompletePiece(p.Index)
					atomic.AddInt64(&d.totalDownloaded, p.Length)
				} else if resumeCompleted > p.Start {
					completedInPiece := resumeCompleted - p.Start
					if completedInPiece > p.Length {
						completedInPiece = p.Length
					}
					completedBlocks := int(completedInPiece / DefaultBlockLength)
					for i := 0; i < completedBlocks && i < p.blocks.NumBlocks(); i++ {
						p.CompleteBlock(i)
					}
					atomic.AddInt64(&d.totalDownloaded, completedInPiece)
					if p.IsComplete() {
						pm.CompletePiece(p.Index)
					}
				}
			}
			d.logger.Infow("resumed progress from completed length",
				"completed_length", resumeCompleted,
				"total_pieces", len(pieces),
			)
		}
	}

	initialThreads := int32(d.cfg.MaxConnections)
	if d.cfg.Adaptive {
		initialThreads = 1
	}
	d.adaptive.setThreadCount(initialThreads)

	var wg sync.WaitGroup
	var errOnce sync.Once
	var downloadErr error

	filePerm := os.FileMode(0644)
	if d.cfg.FileMode != 0 {
		filePerm = d.cfg.FileMode
	}

	outputDir := filepath.Dir(d.outputPath)
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return fmt.Errorf("create output directory: %w", err)
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
			if err := preallocateFile(file, fileSize, d.cfg.SparseFile); err != nil {
				d.logger.Warnw("failed to preallocate space, falling back to truncate", "error", err)
				if err := file.Truncate(fileSize); err != nil {
					return fmt.Errorf("truncate output file: %w", err)
				}
			}
		}
		// 不预分配时，文件按需增长（通过 Seek+Write），避免 os.Stat 误判续传进度
	}

	if d.cfg.FileMode != 0 {
		if err := os.Chmod(d.outputPath, d.cfg.FileMode); err != nil {
			d.logger.Warnw("failed to set file permissions", "error", err)
		}
	}

	go d.periodicSaveProgress(ctx)

	// 使用动态信号量实现自适应线程控制
	maxSem := make(chan struct{}, d.cfg.MaxConnections)
	adaptiveSem := make(chan struct{}, d.cfg.MaxConnections)

	minStealSize := d.cfg.MinChunkSize

	for i := 0; i < d.cfg.MaxConnections; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			cuid := pm.NextCUID()

			for {
				select {
				case <-ctx.Done():
					d.SaveProgress()
					return
				default:
				}

				// 尝试获取一个空闲 Piece
				piece := pm.GetPieceForDownload(cuid)

				// 如果没有空闲 Piece，尝试 segment stealing
				if piece == nil {
					stealStart, stealEnd, srcPiece := pm.TryStealPiece(cuid, minStealSize)
					if stealStart >= 0 && srcPiece != nil {
						// 自适应模式：检查当前允许的线程数
						if d.cfg.Adaptive {
							maxThreads := d.adaptive.threadCount()
							select {
							case maxSem <- struct{}{}:
								currentActive := len(maxSem)
								if currentActive > int(maxThreads) {
									<-maxSem
									time.Sleep(500 * time.Millisecond)
									continue
								}
							case <-ctx.Done():
								d.SaveProgress()
								return
							}
						} else {
							select {
							case adaptiveSem <- struct{}{}:
							case <-ctx.Done():
								d.SaveProgress()
								return
							}
						}

						err := d.downloadRange(ctx, file, stealStart, stealEnd, srcPiece)

						if d.cfg.Adaptive {
							<-maxSem
						} else {
							<-adaptiveSem
						}

						if err != nil {
							errOnce.Do(func() {
								downloadErr = err
							})
							d.logger.Errorw("range download failed",
								"start", stealStart,
								"end", stealEnd,
								"error", err,
							)
							d.SaveProgress()
							return
						}
						continue
					}

					// 没有可偷的范围，也没有空闲 Piece，下载完成
					return
				}

				// 自适应模式：检查当前允许的线程数
				if d.cfg.Adaptive {
					maxThreads := d.adaptive.threadCount()
					select {
					case maxSem <- struct{}{}:
						currentActive := len(maxSem)
						if currentActive > int(maxThreads) {
							<-maxSem
							// 把 piece 放回 idle
							piece.SetStatus(PieceIdle)
							piece.SetOwner(0)
							time.Sleep(500 * time.Millisecond)
							continue
						}
					case <-ctx.Done():
						piece.SetStatus(PieceIdle)
						piece.SetOwner(0)
						d.SaveProgress()
						return
					}
				} else {
					select {
					case adaptiveSem <- struct{}{}:
					case <-ctx.Done():
						piece.SetStatus(PieceIdle)
						piece.SetOwner(0)
						d.SaveProgress()
						return
					}
				}

				// 获取动态 end offset（aria2 风格：如果下一个 piece 空闲，扩展到文件末尾）
				endOffset := pm.GetEndOffset(piece.Index)

				err := d.downloadPiece(ctx, file, piece, endOffset, pm)

				if d.cfg.Adaptive {
					<-maxSem
				} else {
					<-adaptiveSem
				}

				if err != nil {
					errOnce.Do(func() {
						downloadErr = err
					})
					d.logger.Errorw("piece download failed",
						"piece_index", piece.Index,
						"start", piece.Start,
						"length", piece.Length,
						"error", err,
					)
					d.SaveProgress()
					return
				}
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

	// 下载完成后删除控制文件
	if d.controlFilePath != "" {
		store := task.NewControlFileStore(filepath.Dir(d.controlFilePath))
		taskID := strings.TrimSuffix(filepath.Base(d.controlFilePath), filepath.Ext(d.controlFilePath))
		if err := store.Delete(taskID); err != nil && !strings.Contains(err.Error(), "not found") {
			d.logger.Warnw("failed to remove control file", "path", d.controlFilePath, "error", err)
		}
	}

	d.logger.Infow("download completed",
		"total_bytes", d.fileSize,
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
	req.Header.Set("User-Agent", "AFD/0.3")

	resp, err := d.client.Do(req)
	if err != nil {
		d.logger.Warnw("HEAD request failed, falling back to GET", "error", err)
		return d.getSizeViaGet(ctx)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		d.logger.Warnw("HEAD request returned non-200, falling back to GET", "status", resp.StatusCode)
		return d.getSizeViaGet(ctx)
	}

	supportsRange := resp.Header.Get("Accept-Ranges") == "bytes"
	fileSize := resp.ContentLength

	return fileSize, supportsRange, nil
}

func (d *Downloader) getSizeViaGet(ctx context.Context) (int64, bool, error) {
	req, err := d.newGetRequest(ctx, d.url)
	if err != nil {
		return 0, false, fmt.Errorf("create get request: %w", err)
	}
	req.Header.Set("Range", "bytes=0-0")

	resp, err := d.client.Do(req)
	if err != nil {
		return 0, false, fmt.Errorf("get request failed: %w", err)
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)

	if resp.StatusCode == http.StatusPartialContent {
		contentRange := resp.Header.Get("Content-Range")
		if contentRange != "" {
			parts := strings.Split(contentRange, "/")
			if len(parts) == 2 {
				if total, err := strconv.ParseInt(parts[1], 10, 64); err == nil {
					return total, true, nil
				}
			}
		}
		return resp.ContentLength + 1, true, nil
	}

	if resp.StatusCode == http.StatusOK {
		return resp.ContentLength, false, nil
	}

	return 0, false, fmt.Errorf("get request returned status: %d", resp.StatusCode)
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

	backoff := time.Second
	maxBackoff := 30 * time.Second

	var lastErr error
	for _, downloadURL := range urls {
		for retry := 0; retry <= d.cfg.RetryCount; retry++ {
			if retry > 0 {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(backoff):
				}
				// 指数退避
				backoff = time.Duration(float64(backoff) * 1.5)
				if backoff > maxBackoff {
					backoff = maxBackoff
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

			// 416/404 等永久错误不重试
			if IsPermanentError(err) {
				d.logger.Warnw("permanent error, skipping retry",
					"start", chunk.Start, "end", chunk.End, "error", err)
				break
			}

			d.logger.Warnw("retrying chunk",
				"retry", retry,
				"start", chunk.Start,
				"end", chunk.End,
				"downloaded", chunk.Downloaded,
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
	req, err := d.newGetRequest(ctx, downloadURL)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}

	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", chunk.Start+chunk.Downloaded, chunk.End))

	resp, err := d.client.Do(req)
	if err != nil {
		return fmt.Errorf("request: %w", err)
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
		n, readErr := resp.Body.Read(buf)
		if n > 0 {
			if d.rateLimiter != nil {
				if err := d.rateLimiter.Wait(ctx, int64(n)); err != nil {
					return fmt.Errorf("rate limit: %w", err)
				}
			}

			writeOffset := chunk.Start + chunk.Downloaded
			if _, writeErr := file.WriteAt(buf[:n], writeOffset); writeErr != nil {
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
				return nil
			}
			// context 取消导致的读错误直接传播
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("read chunk: %w", readErr)
		}
	}
}

// downloadPiece 下载一个 Piece，使用 block 级别追踪完成状态
// endOffset 是动态 Range 结束位置（aria2 风格：如果下一个 piece 空闲，可扩展到文件末尾）
func (d *Downloader) downloadPiece(ctx context.Context, file *os.File, piece *Piece, endOffset int64, pm *PieceManager) error {
	urls := []string{d.url}
	urls = append(urls, d.altURLs...)

	backoff := time.Second
	maxBackoff := 30 * time.Second

	// 低速检测参数
	minSpeed := d.cfg.MinSpeed
	minSpeedTimeout := d.cfg.MinSpeedTimeout
	if minSpeedTimeout <= 0 {
		minSpeedTimeout = 30 * time.Second
	}

	var lastErr error
	for _, downloadURL := range urls {
		for retry := 0; retry <= d.cfg.RetryCount; retry++ {
			if retry > 0 {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(backoff):
				}
				backoff = time.Duration(float64(backoff) * 1.5)
				if backoff > maxBackoff {
					backoff = maxBackoff
				}
			}

			err := d.downloadPieceOnceFromURL(ctx, file, piece, endOffset, pm, downloadURL, minSpeed, minSpeedTimeout)
			if err == nil {
				return nil
			}

			if ctx.Err() != nil {
				return ctx.Err()
			}

			lastErr = err

			// 416/404 等永久错误不重试
			if IsPermanentError(err) {
				d.logger.Warnw("permanent error, skipping retry",
					"piece_index", piece.Index, "error", err)
				break
			}

			d.logger.Warnw("retrying piece",
				"retry", retry,
				"piece_index", piece.Index,
				"start", piece.Start,
				"length", piece.Length,
				"error", err,
			)
		}
		d.logger.Warnw("source failed, trying next", "url", downloadURL, "error", lastErr)
	}

	return fmt.Errorf("piece download failed from all sources: %w", lastErr)
}

// downloadPieceOnceFromURL 从指定 URL 下载一个 Piece 的一次尝试
func (d *Downloader) downloadPieceOnceFromURL(ctx context.Context, file *os.File, piece *Piece, endOffset int64, pm *PieceManager, downloadURL string, minSpeed int64, minSpeedTimeout time.Duration) error {
	// 使用 block 级别下载
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// 检查 piece 是否已完成
		if piece.IsComplete() {
			pm.CompletePiece(piece.Index)
			return nil
		}

		// 获取下一个未下载的 block
		blockOffset, blockLength := piece.NextUnusedBlock()
		if blockOffset < 0 {
			// 没有更多 block 可用，等待或完成
			if piece.IsComplete() {
				pm.CompletePiece(piece.Index)
				return nil
			}
			// 所有 block 都被占用但未完成，短暂等待后重试
			time.Sleep(100 * time.Millisecond)
			continue
		}

		// 计算 Range 请求的范围
		rangeEnd := blockOffset + blockLength - 1
		// 使用动态 endOffset：如果 endOffset 超过当前 block 末尾，可以一次请求更多数据
		if endOffset > rangeEnd {
			rangeEnd = endOffset
		}
		// 不超过文件大小
		if rangeEnd > d.fileSize-1 {
			rangeEnd = d.fileSize - 1
		}

		req, err := d.newGetRequest(ctx, downloadURL)
		if err != nil {
			piece.CancelBlock(piece.BlockIndexForOffset(blockOffset))
			return fmt.Errorf("create request: %w", err)
		}

		req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", blockOffset, rangeEnd))

		resp, err := d.client.Do(req)
		if err != nil {
			piece.CancelBlock(piece.BlockIndexForOffset(blockOffset))
			return fmt.Errorf("request: %w", err)
		}

		if resp.StatusCode == http.StatusRequestedRangeNotSatisfiable {
			resp.Body.Close()
			piece.CancelBlock(piece.BlockIndexForOffset(blockOffset))
			return fmt.Errorf("server does not support range requests (416)")
		}

		if resp.StatusCode != http.StatusPartialContent && resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			piece.CancelBlock(piece.BlockIndexForOffset(blockOffset))
			return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
		}

		// 低速检测状态
		var lowSpeedBytes int64
		lowSpeedStart := time.Now()
		lowSpeedDetected := false

		buf := make([]byte, d.cfg.BufferSize)
		currentOffset := blockOffset
		currentBlockIdx := piece.BlockIndexForOffset(blockOffset)
		remainingInBlock := blockLength

		for {
			n, readErr := resp.Body.Read(buf)
			if n > 0 {
				if d.rateLimiter != nil {
					if err := d.rateLimiter.Wait(ctx, int64(n)); err != nil {
						resp.Body.Close()
						piece.CancelBlock(currentBlockIdx)
						return fmt.Errorf("rate limit: %w", err)
					}
				}

				if _, writeErr := file.WriteAt(buf[:n], currentOffset); writeErr != nil {
					resp.Body.Close()
					piece.CancelBlock(currentBlockIdx)
					return fmt.Errorf("write piece: %w", writeErr)
				}

				written := int64(n)
				atomic.AddInt64(&d.totalDownloaded, written)
				d.recordSpeed(written)

				currentOffset += written
				remainingInBlock -= written

				// 低速检测
				if minSpeed > 0 {
					lowSpeedBytes += written
					elapsed := time.Since(lowSpeedStart)
					if elapsed >= time.Second {
						currentSpeed := lowSpeedBytes / int64(elapsed.Seconds())
						if currentSpeed < minSpeed {
							if time.Since(lowSpeedStart) >= minSpeedTimeout {
								d.logger.Warnw("low speed detected, closing connection to retry",
									"speed", currentSpeed,
									"min_speed", minSpeed,
									"piece_index", piece.Index,
								)
								lowSpeedDetected = true
								resp.Body.Close()
								// 取消当前 block 占用，让重试时重新获取
								piece.CancelBlock(currentBlockIdx)
								break
							}
						} else {
							// 速度恢复，重置检测窗口
							lowSpeedBytes = 0
							lowSpeedStart = time.Now()
						}
					}
				}

				// 当当前 block 下载完成时，标记完成并移到下一个 block
				if remainingInBlock <= 0 {
					piece.CompleteBlock(currentBlockIdx)

					if d.shouldSaveProgress(written) {
						d.SaveProgress()
					}

					if piece.IsComplete() {
						resp.Body.Close()
						pm.CompletePiece(piece.Index)
						return nil
					}

					// 获取下一个 block
					nextOffset, nextLength := piece.NextUnusedBlock()
					if nextOffset < 0 {
						// 没有更多 block 可获取，但 piece 未完成（其他 goroutine 在下载剩余 block）
						resp.Body.Close()
						return nil
					}

					currentBlockIdx = piece.BlockIndexForOffset(nextOffset)
					remainingInBlock = nextLength
					// 注意：我们继续读取同一 HTTP 响应的数据，写入下一个 block 的位置
					// 因为 Range 请求可能返回超出当前 block 的数据（动态 endOffset）
				}
			}

			if readErr != nil {
				resp.Body.Close()
				if readErr == io.EOF {
					// EOF 时标记当前 block 完成（数据已写入文件）
					piece.CompleteBlock(currentBlockIdx)
					if piece.IsComplete() {
						pm.CompletePiece(piece.Index)
					}
					return nil
				}
				if ctx.Err() != nil {
					piece.CancelBlock(currentBlockIdx)
					return ctx.Err()
				}
				piece.CancelBlock(currentBlockIdx)
				return fmt.Errorf("read piece: %w", readErr)
			}
		}

		if lowSpeedDetected {
			return fmt.Errorf("low speed detected, connection closed for retry")
		}
	}
}

// downloadRange 下载一个偷来的范围（segment stealing），并将数据写入文件
func (d *Downloader) downloadRange(ctx context.Context, file *os.File, start, end int64, piece *Piece) error {
	urls := []string{d.url}
	urls = append(urls, d.altURLs...)

	backoff := time.Second
	maxBackoff := 30 * time.Second

	var lastErr error
	for _, downloadURL := range urls {
		for retry := 0; retry <= d.cfg.RetryCount; retry++ {
			if retry > 0 {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(backoff):
				}
				backoff = time.Duration(float64(backoff) * 1.5)
				if backoff > maxBackoff {
					backoff = maxBackoff
				}
			}

			err := d.downloadRangeOnceFromURL(ctx, file, start, end, piece, downloadURL)
			if err == nil {
				return nil
			}

			if ctx.Err() != nil {
				return ctx.Err()
			}

			lastErr = err

			if IsPermanentError(err) {
				d.logger.Warnw("permanent error in range download, skipping retry",
					"start", start, "end", end, "error", err)
				break
			}

			d.logger.Warnw("retrying range download",
				"retry", retry,
				"start", start,
				"end", end,
				"error", err,
			)
		}
		d.logger.Warnw("source failed, trying next", "url", downloadURL, "error", lastErr)
	}

	return fmt.Errorf("range download failed from all sources: %w", lastErr)
}

// downloadRangeOnceFromURL 从指定 URL 下载一个范围的一次尝试
func (d *Downloader) downloadRangeOnceFromURL(ctx context.Context, file *os.File, start, end int64, piece *Piece, downloadURL string) error {
	req, err := d.newGetRequest(ctx, downloadURL)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}

	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))

	resp, err := d.client.Do(req)
	if err != nil {
		return fmt.Errorf("request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusRequestedRangeNotSatisfiable {
		return fmt.Errorf("server does not support range requests (416)")
	}

	if resp.StatusCode != http.StatusPartialContent && resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	buf := make([]byte, d.cfg.BufferSize)
	currentOffset := start

	for {
		n, readErr := resp.Body.Read(buf)
		if n > 0 {
			if d.rateLimiter != nil {
				if err := d.rateLimiter.Wait(ctx, int64(n)); err != nil {
					return fmt.Errorf("rate limit: %w", err)
				}
			}

			if _, writeErr := file.WriteAt(buf[:n], currentOffset); writeErr != nil {
				return fmt.Errorf("write range: %w", writeErr)
			}

			written := int64(n)
			currentOffset += written
			atomic.AddInt64(&d.totalDownloaded, written)
			d.recordSpeed(written)

			if d.shouldSaveProgress(written) {
				d.SaveProgress()
			}

			// 标记对应 block 为完成
			blockIdx := piece.BlockIndexForOffset(currentOffset - written)
			blockStart := piece.Start + int64(blockIdx)*DefaultBlockLength
			blockEnd := blockStart + piece.blocks.BlockLength(blockIdx)
			if currentOffset >= blockEnd {
				piece.CompleteBlock(blockIdx)
			}
		}

		if readErr != nil {
			if readErr == io.EOF {
				// 标记所有已下载的 block 为完成
				blockIdx := piece.BlockIndexForOffset(start)
				endBlockIdx := piece.BlockIndexForOffset(end)
				for i := blockIdx; i <= endBlockIdx && i < piece.blocks.NumBlocks(); i++ {
					piece.CompleteBlock(i)
				}
				if piece.IsComplete() {
					if d.pieceMgr != nil {
						d.pieceMgr.CompletePiece(piece.Index)
					}
				}
				return nil
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("read range: %w", readErr)
		}
	}
}

// newGetRequest 创建 GET 请求，保留原始 URL 路径字符不被二次编码
func (d *Downloader) newGetRequest(ctx context.Context, rawURL string) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, err
	}
	// 保留原始 URL 中的特殊字符（如 [ ] 中文等），避免 Go http 包二次编码
	if req.URL != nil {
		req.URL.RawPath = ""
	}
	req.Header.Set("User-Agent", "AFD/0.3")
	return req, nil
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

	if d.cfg.Adaptive {
		d.adaptive.addSample(bytes)
		d.adaptive.shouldAdjust()
	}
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
	// 优先使用 PieceManager 的精确进度
	if d.pieceMgr != nil {
		completed := d.pieceMgr.TotalCompletedLength()
		return float64(completed) / float64(d.fileSize) * 100
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
	// 使用 controlFile 判断续传进度
	existingSize := int64(0)
	if d.controlFile != nil && d.controlFile.CompletedLength > 0 {
		existingSize = d.controlFile.CompletedLength
		if existingSize >= d.fileSize && d.fileSize > 0 {
			stat, err := os.Stat(d.outputPath)
			if err == nil && stat.Size() == d.fileSize {
				d.logger.Infow("file already fully downloaded, skipping")
				atomic.StoreInt64(&d.totalDownloaded, d.fileSize)
				return nil
			}
			existingSize = 0
		}
		if existingSize > 0 && existingSize < d.fileSize {
			d.logger.Infow("resuming single-thread download",
				"completed_length", existingSize,
				"total_size", d.fileSize,
			)
			return d.singleThreadResume(ctx, existingSize)
		}
	} else {
		// 没有 controlFile，检查本地文件
		stat, err := os.Stat(d.outputPath)
		if err == nil && stat.Size() > 0 && stat.Size() < d.fileSize {
			existingSize = stat.Size()
			d.logger.Infow("resuming single-thread download from local file",
				"existing_size", existingSize,
				"total_size", d.fileSize,
			)
			return d.singleThreadResume(ctx, existingSize)
		}
	}

	req, err := d.newGetRequest(ctx, d.url)
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

	outputDir := filepath.Dir(d.outputPath)
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return fmt.Errorf("create output directory: %w", err)
	}

	file, err := os.Create(d.outputPath)
	if err != nil {
		return err
	}
	defer file.Close()

	go d.periodicSaveProgress(ctx)

	buf := make([]byte, d.cfg.BufferSize)

	for {
		select {
		case <-ctx.Done():
			d.SaveProgress()
			return ctx.Err()
		default:
		}

		n, readErr := resp.Body.Read(buf)
		if n > 0 {
			if d.rateLimiter != nil {
				if err := d.rateLimiter.Wait(ctx, int64(n)); err != nil {
					d.SaveProgress()
					return err
				}
			}

			_, writeErr := file.Write(buf[:n])
			if writeErr != nil {
				return fmt.Errorf("write: %w", writeErr)
			}

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
			d.SaveProgress()
			return fmt.Errorf("read: %w", readErr)
		}
	}

	d.SaveProgress()

	// 下载完成后删除控制文件
	if d.controlFilePath != "" {
		store := task.NewControlFileStore(filepath.Dir(d.controlFilePath))
		taskID := strings.TrimSuffix(filepath.Base(d.controlFilePath), filepath.Ext(d.controlFilePath))
		if err := store.Delete(taskID); err != nil && !strings.Contains(err.Error(), "not found") {
			d.logger.Warnw("failed to remove control file", "path", d.controlFilePath, "error", err)
		}
	}

	return nil
}

func (d *Downloader) singleThreadResume(ctx context.Context, existingSize int64) error {
	req, err := d.newGetRequest(ctx, d.url)
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

	go d.periodicSaveProgress(ctx)

	buf := make([]byte, d.cfg.BufferSize)

	for {
		select {
		case <-ctx.Done():
			d.SaveProgress()
			return ctx.Err()
		default:
		}

		n, readErr := resp.Body.Read(buf)
		if n > 0 {
			if d.rateLimiter != nil {
				if err := d.rateLimiter.Wait(ctx, int64(n)); err != nil {
					d.SaveProgress()
					return err
				}
			}

			_, writeErr := file.Write(buf[:n])
			if writeErr != nil {
				return fmt.Errorf("write: %w", writeErr)
			}

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
			d.SaveProgress()
			return fmt.Errorf("read: %w", readErr)
		}
	}

	d.SaveProgress()

	// 下载完成后删除控制文件
	if d.controlFilePath != "" {
		store := task.NewControlFileStore(filepath.Dir(d.controlFilePath))
		taskID := strings.TrimSuffix(filepath.Base(d.controlFilePath), filepath.Ext(d.controlFilePath))
		if err := store.Delete(taskID); err != nil && !strings.Contains(err.Error(), "not found") {
			d.logger.Warnw("failed to remove control file", "path", d.controlFilePath, "error", err)
		}
	}

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

func (d *Downloader) SetInsecure(insecure bool) {
	d.cfg.Insecure = insecure
	if insecure {
		if transport, ok := d.client.Transport.(*http.Transport); ok {
			transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
		}
	}
}

type DiskCache struct {
	cacheDir string
	maxSize  int64
	curSize  int64
	cacheMap map[string]*cacheItem
	mu       sync.Mutex
}

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
	stat, err := file.Stat()
	if err != nil {
		return err
	}
	if stat.Size() >= size {
		return nil
	}

	if sparse {
		if err := file.Truncate(size); err != nil {
			return err
		}
		return nil
	}

	if _, err := file.Seek(size-1, 0); err != nil {
		return err
	}
	if _, err := file.Write([]byte{0}); err != nil {
		return err
	}

	return file.Truncate(size)
}
