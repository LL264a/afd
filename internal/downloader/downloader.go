package downloader

import (
	"context"
	"crypto/sha1"
	"crypto/tls"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"os"
	"path/filepath"
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

// errNotModified 表示服务器返回 304 Not Modified，文件未修改
var errNotModified = errors.New("resource not modified")

const (
	defaultFileMode os.FileMode = 0644
	defaultDirMode  os.FileMode = 0755
	userAgent       = "AFD/0.3"
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
	netrc             *Netrc

	conditionalGet bool   // 是否启用条件下载
	lastModified   string // 上次下载的 Last-Modified
	etag           string // 上次下载的 ETag

	speedWindow []speedSample
	swHead      int
	swCount     int
	swMu        sync.Mutex

	totalDownloaded int64
	fileSize        int64
	startTime       time.Time

	pieceMgr *PieceManager

	saveMu     sync.Mutex
	cfMu       sync.Mutex
	rateMu     sync.Mutex
	pieceMgrMu sync.RWMutex
	altURLsMu  sync.Mutex
	retryMu    sync.Mutex

	adaptive *adaptiveController

	lastSaveTime  time.Time
	sinceLastSave int64
	saveInterval  time.Duration

	serverStatMan *ServerStatMan
	uriSelector   string
}

func NewDownloader(cfg *config.DownloadConfig, logger *zap.SugaredLogger) *Downloader {
	if cfg == nil {
		cfg = config.DefaultDownloadConfig()
	}

	if cfg.BufferSize <= 0 {
		cfg.BufferSize = 32 * 1024 // 默认 32KB
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

	saveInterval := 5 * time.Second
	if cfg.AutoSaveInterval > 0 {
		saveInterval = time.Duration(cfg.AutoSaveInterval) * time.Second
	}

	d := &Downloader{
		cfg:            cfg,
		retryConfig:    retryConfig,
		client:         client,
		logger:         logger,
		proxy:          proxyCfg,
		cookieJar:      jar,
		speedWindow:    make([]speedSample, 20),
		adaptive:       newAdaptiveController(cfg.MaxConnections, 1),
		saveInterval:   saveInterval,
		rateLimiter:    rateLimiter,
		conditionalGet: cfg.ConditionalGet,
		serverStatMan:  NewServerStatMan(),
		uriSelector:    cfg.UriSelector,
	}

	// 加载 netrc（除非显式禁用）
	if !cfg.NoNetrc {
		if netrc, err := LoadNetrc(""); err == nil {
			d.netrc = netrc
		} else if !os.IsNotExist(err) {
			logger.Debugw("failed to load netrc", "error", err)
		}
	}

	return d
}

type DownloaderInterface interface {
	SetURL(url string)
	SetOutputPath(path string)
	SetControlFilePath(path string)
	SetControlFile(cf any)
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
	if IsSFTPURL(url) {
		return NewSFTPDownloader(url, outputPath, nil), nil
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

func (d *Downloader) SetURL(rawURL string) {
	d.url = rawURL
	if err := d.loadCookies(); err != nil {
		d.logger.Debugw("load cookies failed", "error", err)
	}
	// 从 netrc 获取凭证（仅当未显式配置 HTTP 凭证时）
	if d.netrc != nil && d.cfg.HTTPUsername == "" {
		if u, err := url.Parse(d.url); err == nil && u != nil {
			if user, pass := d.netrc.GetCredentials(u.Hostname()); user != "" {
				d.cfg.HTTPUsername = user
				d.cfg.HTTPPassword = pass
			}
		}
	}
}

func (d *Downloader) SetOutputPath(path string) {
	d.outputPath = path
}

func (d *Downloader) SetControlFilePath(path string) {
	d.controlFilePath = path
}

func (d *Downloader) SetControlFile(cf any) {
	d.cfMu.Lock()
	defer d.cfMu.Unlock()
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
		if errors.Is(err, task.ErrControlFileNotFound) {
			return nil
		}
		return fmt.Errorf("load control file: %w", err)
	}

	d.cfMu.Lock()
	d.controlFile = cf
	d.lastModified = cf.LastModified
	d.etag = cf.ETag
	d.cfMu.Unlock()
	d.logger.Infow("loaded progress from control file",
		"completed_length", cf.CompletedLength,
		"total_length", cf.TotalLength,
		"has_last_modified", cf.LastModified != "",
		"has_etag", cf.ETag != "",
	)

	return nil
}

func (d *Downloader) SaveProgress() error {
	d.saveMu.Lock()
	defer d.saveMu.Unlock()

	if d.controlFilePath == "" {
		return nil
	}

	d.cfMu.Lock()
	if d.controlFile == nil {
		d.controlFile = &task.ControlFile{
			Status: "downloading",
		}
	}

	// CompletedLength 用 totalDownloaded（实际写入字节数），pieceBitfields 用于精确续传
	d.controlFile.CompletedLength = atomic.LoadInt64(&d.totalDownloaded)
	d.controlFile.TotalLength = atomic.LoadInt64(&d.fileSize)
	d.controlFile.UpdatedAt = time.Now()
	// 持久化条件下载所需的 Last-Modified / ETag
	if d.conditionalGet {
		d.controlFile.LastModified = d.lastModified
		d.controlFile.ETag = d.etag
	}

	// 序列化 Piece 级 Block 位图（用于精确续传）
	if d.pieceMgr != nil {
		entries := d.pieceMgr.SerializePieceBitfields()
		if len(entries) > 0 {
			d.controlFile.PieceBitfields = entries
			d.controlFile.NumPieces = len(d.pieceMgr.pieces)
		}
	}
	cf := *d.controlFile // 拷贝用于存储
	d.cfMu.Unlock()

	// 存储操作在锁外执行
	store := task.NewControlFileStore(filepath.Dir(d.controlFilePath))
	taskID := strings.TrimSuffix(filepath.Base(d.controlFilePath), filepath.Ext(d.controlFilePath))

	if err := store.Save(taskID, &cf); err != nil {
		return fmt.Errorf("save progress: %w", err)
	}

	d.swMu.Lock()
	d.lastSaveTime = time.Now()
	d.sinceLastSave = 0
	d.swMu.Unlock()

	return nil
}

// saveProgressOrLog 调用 SaveProgress 并在失败时记录日志，消除重复的错误处理样板代码
func (d *Downloader) saveProgressOrLog() {
	if err := d.SaveProgress(); err != nil {
		d.logger.Errorw("failed to save progress", "error", err)
	}
}

// recordSpeedAndCheckSave 在单次锁内完成速度采样和保存进度判断，避免热路径双重锁
func (d *Downloader) recordSpeedAndCheckSave(bytes int64) bool {
	d.swMu.Lock()
	defer d.swMu.Unlock()

	// recordSpeed 逻辑
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

	// shouldSaveProgress 逻辑
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
	return atomic.LoadInt64(&d.fileSize)
}

func (d *Downloader) Download(ctx context.Context) error {
	if d.cfg.DryRun {
		d.logger.Infow("dry run, skipping download", "url", d.url)
		return nil
	}
	d.startTime = time.Now()
	d.swMu.Lock()
	d.lastSaveTime = time.Now()
	d.swMu.Unlock()

	err := DoWithRetryWithLogger(ctx, d.GetRetryConfig(), d.logger, func() error {
		return d.doDownload(ctx)
	})
	if err != nil {
		return err
	}
	if saveErr := d.saveCookies(); saveErr != nil {
		d.logger.Warnw("failed to save cookies", "error", saveErr)
	}
	return nil
}

func (d *Downloader) doDownload(ctx context.Context) error {
	// 使用局部 done channel，确保 periodicSaveProgress goroutine 在所有退出路径上退出
	done := make(chan struct{})
	var doneOnce sync.Once
	defer doneOnce.Do(func() { close(done) })

	if d.cfg.MaxConnections <= 0 {
		return fmt.Errorf("MaxConnections must be positive, got %d", d.cfg.MaxConnections)
	}

	if err := d.LoadProgress(ctx); err != nil {
		d.logger.Warnw("failed to load progress", "error", err)
	}

	fileSize, supportsRange, err := d.headRequest(ctx)
	if err != nil {
		if errors.Is(err, errNotModified) {
			// 文件未修改：检查本地文件是否存在
			if stat, e := os.Stat(d.outputPath); e == nil && stat.Size() > 0 {
				d.logger.Infow("file not modified since last download, skipping")
				atomic.StoreInt64(&d.totalDownloaded, stat.Size())
				atomic.StoreInt64(&d.fileSize, stat.Size())
				d.cfMu.Lock()
				if d.controlFile != nil {
					d.controlFile.Status = "completed"
				}
				d.cfMu.Unlock()
				d.saveProgressOrLog()
				return nil
			}
			// 本地文件不存在但服务器返回 304，清除条件头重新探测
			d.logger.Warnw("server returned 304 but local file missing, re-probing without conditional headers")
			d.lastModified = ""
			d.etag = ""
			fileSize, supportsRange, err = d.headRequest(ctx)
			if err != nil {
				return fmt.Errorf("head request: %w", err)
			}
		} else {
			return fmt.Errorf("head request: %w", err)
		}
	}
	atomic.StoreInt64(&d.fileSize, fileSize)

	d.logger.Infow("starting download",
		"file_size", fileSize,
		"supports_range", supportsRange,
		"adaptive", d.cfg.Adaptive,
		"insecure", d.cfg.Insecure,
		"url", d.url,
	)

	// 使用 controlFile.CompletedLength 判断续传进度（不用 os.Stat，因为预分配文件会导致文件大小等于完整大小）
	resumeCompleted := int64(0)
	d.cfMu.Lock()
	cfExists := d.controlFile != nil
	if cfExists {
		resumeCompleted = d.controlFile.CompletedLength
	}
	d.cfMu.Unlock()

	if cfExists && resumeCompleted > 0 {
		if resumeCompleted >= fileSize && fileSize > 0 {
			// 验证本地文件确实存在且大小匹配
			stat, err := os.Stat(d.outputPath)
			if err == nil && stat.Size() == fileSize {
				d.logger.Infow("file already fully downloaded, skipping")
				atomic.StoreInt64(&d.totalDownloaded, fileSize)
				d.cfMu.Lock()
				d.controlFile.Status = "completed"
				d.cfMu.Unlock()
				d.saveProgressOrLog()
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
	pm := NewPieceManager(pieces, fileSize, d.cfg.StreamPieceSelector)
	d.pieceMgr = pm

	d.logger.Infow("pieces prepared", "total_pieces", len(pieces))

	// 恢复已下载的进度
	if resumeCompleted > 0 {
		// 优先使用 Block 级位图恢复（精确到每个 Block）
		d.cfMu.Lock()
		var pieceBitfields []task.PieceBitfieldEntry
		if d.controlFile != nil && len(d.controlFile.PieceBitfields) > 0 {
			pieceBitfields = d.controlFile.PieceBitfields
		}
		d.cfMu.Unlock()

		if len(pieceBitfields) > 0 {
			pm.RestorePieceBitfields(pieceBitfields)
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
			// 没有 Block 级位图，只恢复完整 piece，部分 piece 从头下载
			// （多线程下载不保证顺序，不能假设前 N 个 block 已完成）
			for _, p := range pieces {
				pieceEnd := p.Start + p.Length
				if resumeCompleted >= pieceEnd {
					numBlocks := p.blocks.NumBlocks()
					for i := 0; i < numBlocks; i++ {
						p.CompleteBlock(i)
					}
					pm.CompletePiece(p.Index)
					atomic.AddInt64(&d.totalDownloaded, p.Length)
				}
				// 部分 piece 不标记任何 block，从头下载
			}
			d.logger.Infow("resumed progress from completed length (conservative mode, partial pieces re-downloaded)",
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
			allocation := d.cfg.FileAllocation
			if allocation == "" {
				allocation = "trunc"
			}
			if d.cfg.SparseFile && allocation == "trunc" {
				// 向后兼容：SparseFile 语义等同于 trunc（在大多数文件系统上创建稀疏文件）
				allocation = "trunc"
			}
			if err := preallocateFile(file, fileSize, allocation); err != nil {
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

	go d.periodicSaveProgress(ctx, done)

	// 使用动态信号量实现自适应线程控制
	maxSem := make(chan struct{}, d.cfg.MaxConnections)
	adaptiveSem := make(chan struct{}, d.cfg.MaxConnections)

	minStealSize := d.cfg.MinChunkSize

	// 可取消的 context：某个 worker 失败时取消其他 worker
	workerCtx, workerCancel := context.WithCancel(ctx)
	defer workerCancel()

	for i := 0; i < d.cfg.MaxConnections; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			cuid := pm.NextCUID()

			// 使用 NewTimer 替代 time.After，避免在 for-select 长循环中
			// 累积未触发的定时器导致内存泄漏。每次复用前 Stop+Reset。
			throttleTimer := time.NewTimer(500 * time.Millisecond)
			defer throttleTimer.Stop()
			// 初始定时器不需要立即触发，先排空使其进入待 Reset 状态。
			if !throttleTimer.Stop() {
				<-throttleTimer.C
			}

			for {
				select {
				case <-workerCtx.Done():
					d.saveProgressOrLog()
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
									throttleTimer.Reset(500 * time.Millisecond)
									select {
									case <-workerCtx.Done():
										d.saveProgressOrLog()
										return
									case <-throttleTimer.C:
									}
									continue
								}
							case <-workerCtx.Done():
								d.saveProgressOrLog()
								return
							}
						} else {
							select {
							case adaptiveSem <- struct{}{}:
							case <-workerCtx.Done():
								d.saveProgressOrLog()
								return
							}
						}

						err := d.downloadRange(workerCtx, file, stealStart, stealEnd, srcPiece)

						if d.cfg.Adaptive {
							<-maxSem
						} else {
							<-adaptiveSem
						}

						if err != nil {
							errOnce.Do(func() {
								downloadErr = err
								workerCancel()
							})
							d.logger.Errorw("range download failed",
								"start", stealStart,
								"end", stealEnd,
								"error", err,
							)
							d.saveProgressOrLog()
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
							throttleTimer.Reset(500 * time.Millisecond)
							select {
							case <-workerCtx.Done():
								d.saveProgressOrLog()
								return
							case <-throttleTimer.C:
							}
							continue
						}
					case <-workerCtx.Done():
						piece.SetStatus(PieceIdle)
						piece.SetOwner(0)
						d.saveProgressOrLog()
						return
					}
				} else {
					select {
					case adaptiveSem <- struct{}{}:
					case <-workerCtx.Done():
						piece.SetStatus(PieceIdle)
						piece.SetOwner(0)
						d.saveProgressOrLog()
						return
					}
				}

				// 获取动态 end offset（aria2 风格：如果下一个 piece 空闲，扩展到文件末尾）
				endOffset := pm.GetEndOffset(piece.Index)

				err := d.downloadPiece(workerCtx, file, piece, endOffset, pm)

				if d.cfg.Adaptive {
					<-maxSem
				} else {
					<-adaptiveSem
				}

				if err != nil {
					errOnce.Do(func() {
						downloadErr = err
						workerCancel()
					})
					d.logger.Errorw("piece download failed",
						"piece_index", piece.Index,
						"start", piece.Start,
						"length", piece.Length,
						"error", err,
					)
					// 重置 piece 状态，允许后续重试
					if d.pieceMgr != nil {
						piece.SetStatus(PieceIdle)
						piece.SetOwner(0)
					}
					d.saveProgressOrLog()
					return
				}
			}
		}()
	}

	wg.Wait()

	if downloadErr != nil {
		d.saveProgressOrLog()
		return downloadErr
	}

	d.saveProgressOrLog()

	d.cfMu.Lock()
	if d.controlFile != nil {
		d.controlFile.Status = "completed"
	}
	d.cfMu.Unlock()
	d.saveProgressOrLog()

	// 下载完成后删除控制文件（启用条件下载时保留，以便下次使用 Last-Modified/ETag）
	if d.controlFilePath != "" && !d.conditionalGet {
		store := task.NewControlFileStore(filepath.Dir(d.controlFilePath))
		taskID := strings.TrimSuffix(filepath.Base(d.controlFilePath), filepath.Ext(d.controlFilePath))
		if err := store.Delete(taskID); err != nil && !errors.Is(err, task.ErrControlFileNotFound) {
			d.logger.Warnw("failed to remove control file", "path", d.controlFilePath, "error", err)
		}
	}

	d.logger.Infow("download completed",
		"total_bytes", atomic.LoadInt64(&d.fileSize),
		"duration", time.Since(d.startTime),
	)

	d.applyRemoteTime()

	return nil
}

func (d *Downloader) periodicSaveProgress(ctx context.Context, done chan struct{}) {
	ticker := time.NewTicker(d.saveInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			d.saveProgressOrLog()
			return
		case <-done:
			return
		case <-ticker.C:
			d.saveProgressOrLog()
		}
	}
}

func (d *Downloader) headRequest(ctx context.Context) (int64, bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, d.url, nil)
	if err != nil {
		return 0, false, fmt.Errorf("create head request: %w", err)
	}
	d.applyCustomHeaders(req)

	// 条件下载：添加 If-Modified-Since / If-None-Match
	if d.conditionalGet {
		if d.lastModified != "" {
			req.Header.Set("If-Modified-Since", d.lastModified)
		}
		if d.etag != "" {
			req.Header.Set("If-None-Match", d.etag)
		}
	}

	resp, err := d.client.Do(req)
	if err != nil {
		d.logger.Warnw("HEAD request failed, falling back to GET", "error", err)
		return d.getSizeViaGet(ctx)
	}
	defer resp.Body.Close()

	// 条件下载：304 表示文件未修改
	if d.conditionalGet && resp.StatusCode == http.StatusNotModified {
		d.logger.Infow("server returned 304 Not Modified, file unchanged")
		return 0, false, errNotModified
	}

	if resp.StatusCode != http.StatusOK {
		d.logger.Warnw("HEAD request returned non-200, falling back to GET", "status", resp.StatusCode)
		return d.getSizeViaGet(ctx)
	}

	// 捕获 Last-Modified 用于 remote-time 和条件下载
	if lm := resp.Header.Get("Last-Modified"); lm != "" {
		d.lastModified = lm
	}
	// ETag 仅在条件下载时捕获
	if d.conditionalGet {
		if et := resp.Header.Get("ETag"); et != "" {
			d.etag = et
		}
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

	// 捕获 Last-Modified 用于 remote-time
	if lm := resp.Header.Get("Last-Modified"); lm != "" {
		d.lastModified = lm
	}

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
		if resp.ContentLength < 0 {
			return 0, false, fmt.Errorf("unknown content length")
		}
		return resp.ContentLength + 1, true, nil
	}

	if resp.StatusCode == http.StatusOK {
		if resp.ContentLength < 0 {
			return 0, false, fmt.Errorf("unknown content length")
		}
		return resp.ContentLength, false, nil
	}

	return 0, false, fmt.Errorf("get request returned status: %d", resp.StatusCode)
}

// downloadPiece 下载一个 Piece，使用 block 级别追踪完成状态
// endOffset 是动态 Range 结束位置（aria2 风格：如果下一个 piece 空闲，可扩展到文件末尾）
func (d *Downloader) downloadPiece(ctx context.Context, file *os.File, piece *Piece, endOffset int64, pm *PieceManager) error {
	urls := []string{d.url}
	urls = append(urls, d.getAltURLs()...)
	if d.serverStatMan != nil {
		urls = d.serverStatMan.SortURLsBySelector(urls, d.uriSelector)
	}

	backoff := time.Second
	maxBackoff := 30 * time.Second

	// 低速检测参数
	minSpeed := d.cfg.MinSpeed
	minSpeedTimeout := d.cfg.MinSpeedTimeout
	if minSpeedTimeout <= 0 {
		minSpeedTimeout = 30 * time.Second
	}

	// 使用 NewTimer 替代 time.After，避免重试循环中累积未触发的定时器。
	backoffTimer := time.NewTimer(backoff)
	defer backoffTimer.Stop()
	// 初始不需要立即触发，排空使其进入待 Reset 状态。
	if !backoffTimer.Stop() {
		<-backoffTimer.C
	}

	var lastErr error
	for _, downloadURL := range urls {
		for retry := 0; retry <= d.retryConfig.MaxRetries; retry++ {
			if retry > 0 {
				backoffTimer.Reset(backoff)
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-backoffTimer.C:
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
func (d *Downloader) downloadPieceOnceFromURL(ctx context.Context, file *os.File, piece *Piece, endOffset int64, pm *PieceManager, downloadURL string, minSpeed int64, minSpeedTimeout time.Duration) (err error) {
	startTime := time.Now()
	var totalBytes int64
	defer func() {
		if d.serverStatMan == nil {
			return
		}
		if err != nil {
			// 不记录 context 取消导致的失败
			if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				d.serverStatMan.RecordFailure(downloadURL)
			}
			return
		}
		if totalBytes > 0 {
			elapsed := time.Since(startTime)
			if elapsed > 0 {
				d.serverStatMan.RecordSpeed(downloadURL, int64(float64(totalBytes)/elapsed.Seconds()))
			}
		}
	}()

	buf := make([]byte, d.cfg.BufferSize)
	// 使用 NewTimer 替代 time.After，避免 block 等待循环中累积未触发的定时器。
	blockWaitTimer := time.NewTimer(100 * time.Millisecond)
	defer blockWaitTimer.Stop()
	// 初始不需要立即触发，排空使其进入待 Reset 状态。
	if !blockWaitTimer.Stop() {
		<-blockWaitTimer.C
	}
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
			blockWaitTimer.Reset(100 * time.Millisecond)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-blockWaitTimer.C:
			}
			continue
		}

		// 计算 Range 请求的范围
		rangeEnd := blockOffset + blockLength - 1
		// 使用动态 endOffset：如果 endOffset 超过当前 block 末尾，可以一次请求更多数据
		if endOffset > rangeEnd {
			rangeEnd = endOffset
		}
		// 不超过文件大小
		fileSize := atomic.LoadInt64(&d.fileSize)
		if rangeEnd > fileSize-1 {
			rangeEnd = fileSize - 1
		}

		req, err := d.newGetRequest(ctx, downloadURL)
		if err != nil {
			piece.CancelBlock(piece.BlockIndexForOffset(blockOffset))
			return fmt.Errorf("create request: %w", err)
		}

		req.Header.Set("Range", "bytes="+strconv.FormatInt(blockOffset, 10)+"-"+strconv.FormatInt(rangeEnd, 10))

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

		currentOffset := blockOffset
		currentBlockIdx := piece.BlockIndexForOffset(blockOffset)
		remainingInBlock := blockLength

		for {
			n, readErr := resp.Body.Read(buf)
			if n > 0 {
				if rl := d.getRateLimiter(); rl != nil {
					if err := rl.Wait(ctx, int64(n)); err != nil {
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
				totalBytes += written

				if d.recordSpeedAndCheckSave(written) {
					d.saveProgressOrLog()
				}

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
						break // 跳出内层循环，外层循环检查 piece 是否完成
					}

					// 如果下一个 block 的偏移量与当前写入位置不连续，不能继续用同一 HTTP 响应
					// 否则数据会写错位置（P0 数据损坏）
					if nextOffset != currentOffset {
						resp.Body.Close()
						break // 跳出内层循环，外层循环重新获取 block
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
					if remainingInBlock > 0 {
						// block 未完整下载，不标记完成
						return fmt.Errorf("short read: block %d incomplete, %d bytes remaining", currentBlockIdx, remainingInBlock)
					}
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
	urls = append(urls, d.getAltURLs()...)
	if d.serverStatMan != nil {
		urls = d.serverStatMan.SortURLsBySelector(urls, d.uriSelector)
	}

	backoff := time.Second
	maxBackoff := 30 * time.Second

	// 使用 NewTimer 替代 time.After，避免重试循环中累积未触发的定时器。
	rangeBackoffTimer := time.NewTimer(backoff)
	defer rangeBackoffTimer.Stop()
	// 初始不需要立即触发，排空使其进入待 Reset 状态。
	if !rangeBackoffTimer.Stop() {
		<-rangeBackoffTimer.C
	}

	var lastErr error
	for _, downloadURL := range urls {
		for retry := 0; retry <= d.retryConfig.MaxRetries; retry++ {
			if retry > 0 {
				rangeBackoffTimer.Reset(backoff)
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-rangeBackoffTimer.C:
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
func (d *Downloader) downloadRangeOnceFromURL(ctx context.Context, file *os.File, start, end int64, piece *Piece, downloadURL string) (err error) {
	startTime := time.Now()
	var totalBytes int64
	defer func() {
		if d.serverStatMan == nil {
			return
		}
		if err != nil {
			if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				d.serverStatMan.RecordFailure(downloadURL)
			}
			return
		}
		if totalBytes > 0 {
			elapsed := time.Since(startTime)
			if elapsed > 0 {
				d.serverStatMan.RecordSpeed(downloadURL, int64(float64(totalBytes)/elapsed.Seconds()))
			}
		}
	}()

	req, err := d.newGetRequest(ctx, downloadURL)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}

	req.Header.Set("Range", "bytes="+strconv.FormatInt(start, 10)+"-"+strconv.FormatInt(end, 10))

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
			if rl := d.getRateLimiter(); rl != nil {
				if err := rl.Wait(ctx, int64(n)); err != nil {
					return fmt.Errorf("rate limit: %w", err)
				}
			}

			if _, writeErr := file.WriteAt(buf[:n], currentOffset); writeErr != nil {
				return fmt.Errorf("write range: %w", writeErr)
			}

			written := int64(n)
			currentOffset += written
			atomic.AddInt64(&d.totalDownloaded, written)
			totalBytes += written

			if d.recordSpeedAndCheckSave(written) {
				d.saveProgressOrLog()
			}
		}

		if readErr != nil {
			if readErr == io.EOF {
				// 只标记实际已写入数据的 block
				lastWrittenOffset := currentOffset - 1
				if lastWrittenOffset >= start {
					lastBlock := piece.BlockIndexForOffset(lastWrittenOffset)
					firstBlock := piece.BlockIndexForOffset(start)
					for i := firstBlock; i <= lastBlock; i++ {
						piece.CompleteBlock(i)
					}
				}
				// 检查是否真的完成了所有 block
				if !piece.IsComplete() {
					return fmt.Errorf("unexpected EOF, piece %d incomplete", piece.Index)
				}
				if d.pieceMgr != nil {
					d.pieceMgr.CompletePiece(piece.Index)
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

// applyCustomHeaders 应用自定义 HTTP 头、Referer、HTTP 认证和 gzip 设置
func (d *Downloader) applyCustomHeaders(req *http.Request) {
	if d.cfg.UserAgent != "" {
		req.Header.Set("User-Agent", d.cfg.UserAgent)
	} else {
		req.Header.Set("User-Agent", "AFD/0.3")
	}
	if d.cfg.Referer != "" {
		req.Header.Set("Referer", d.cfg.Referer)
	}
	for k, v := range d.cfg.CustomHeaders {
		req.Header.Set(k, v)
	}
	if d.cfg.HTTPUsername != "" {
		req.SetBasicAuth(d.cfg.HTTPUsername, d.cfg.HTTPPassword)
	} else if d.netrc != nil && req.URL != nil {
		// HTTPUsername 为空时，尝试从 netrc 按请求主机获取凭证
		if user, pass := d.netrc.GetCredentials(req.URL.Hostname()); user != "" {
			req.SetBasicAuth(user, pass)
		}
	}
	if d.cfg.AcceptGzip {
		req.Header.Set("Accept-Encoding", "gzip")
	}
}

// applyRemoteTime 如果启用 remote-time，根据 HTTP 响应的 Last-Modified 设置本地文件时间
func (d *Downloader) applyRemoteTime() {
	if d.cfg.RemoteTime && d.lastModified != "" {
		if t, err := http.ParseTime(d.lastModified); err == nil {
			if err := os.Chtimes(d.outputPath, t, t); err != nil {
				d.logger.Warnw("failed to set file time from remote", "error", err)
			}
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
	d.applyCustomHeaders(req)
	return req, nil
}

func (d *Downloader) Speed() int64 {
	d.swMu.Lock()
	defer d.swMu.Unlock()

	if d.swCount == 0 {
		return 0
	}

	if d.swCount < 2 {
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
	fileSize := atomic.LoadInt64(&d.fileSize)
	if fileSize <= 0 {
		return 0
	}
	// 优先使用 PieceManager 的精确进度
	d.pieceMgrMu.RLock()
	pm := d.pieceMgr
	d.pieceMgrMu.RUnlock()
	if pm != nil {
		completed := pm.TotalCompletedLength()
		return float64(completed) / float64(fileSize) * 100
	}
	downloaded := atomic.LoadInt64(&d.totalDownloaded)
	return float64(downloaded) / float64(fileSize) * 100
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
	d.cfMu.Lock()
	if d.controlFile != nil && d.controlFile.CompletedLength > 0 {
		existingSize = d.controlFile.CompletedLength
	}
	d.cfMu.Unlock()
	fileSize := atomic.LoadInt64(&d.fileSize)
	if existingSize > 0 {
		if existingSize >= fileSize && fileSize > 0 {
			stat, err := os.Stat(d.outputPath)
			if err == nil && stat.Size() == fileSize {
				d.logger.Infow("file already fully downloaded, skipping")
				atomic.StoreInt64(&d.totalDownloaded, fileSize)
				return nil
			}
			existingSize = 0
		}
		if existingSize > 0 && existingSize < fileSize {
			d.logger.Infow("resuming single-thread download",
				"completed_length", existingSize,
				"total_size", fileSize,
			)
			return d.singleThreadResume(ctx, existingSize)
		}
	} else {
		// 没有 controlFile，检查本地文件
		stat, err := os.Stat(d.outputPath)
		if err == nil && stat.Size() > 0 && stat.Size() < fileSize {
			existingSize = stat.Size()
			d.logger.Infow("resuming single-thread download from local file",
				"existing_size", existingSize,
				"total_size", fileSize,
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

	done := make(chan struct{})
	var doneOnce sync.Once
	defer doneOnce.Do(func() { close(done) })
	go d.periodicSaveProgress(ctx, done)

	buf := make([]byte, d.cfg.BufferSize)

	for {
		select {
		case <-ctx.Done():
			d.saveProgressOrLog()
			return ctx.Err()
		default:
		}

		n, readErr := resp.Body.Read(buf)
		if n > 0 {
			if rl := d.getRateLimiter(); rl != nil {
				if err := rl.Wait(ctx, int64(n)); err != nil {
					d.saveProgressOrLog()
					return err
				}
			}

			_, writeErr := file.Write(buf[:n])
			if writeErr != nil {
				return fmt.Errorf("write: %w", writeErr)
			}

			atomic.AddInt64(&d.totalDownloaded, int64(n))

			if d.recordSpeedAndCheckSave(int64(n)) {
				d.saveProgressOrLog()
			}
		}

		if readErr != nil {
			if readErr == io.EOF {
				break
			}
			d.saveProgressOrLog()
			return fmt.Errorf("read: %w", readErr)
		}
	}

	d.saveProgressOrLog()

	// 下载完成后删除控制文件（启用条件下载时保留，以便下次使用 Last-Modified/ETag）
	if d.controlFilePath != "" && !d.conditionalGet {
		store := task.NewControlFileStore(filepath.Dir(d.controlFilePath))
		taskID := strings.TrimSuffix(filepath.Base(d.controlFilePath), filepath.Ext(d.controlFilePath))
		if err := store.Delete(taskID); err != nil && !errors.Is(err, task.ErrControlFileNotFound) {
			d.logger.Warnw("failed to remove control file", "path", d.controlFilePath, "error", err)
		}
	}

	if saveErr := d.saveCookies(); saveErr != nil {
		d.logger.Warnw("failed to save cookies", "error", saveErr)
	}

	d.applyRemoteTime()

	return nil
}

func (d *Downloader) singleThreadResume(ctx context.Context, existingSize int64) error {
	req, err := d.newGetRequest(ctx, d.url)
	if err != nil {
		return err
	}

	req.Header.Set("Range", "bytes="+strconv.FormatInt(existingSize, 10)+"-")

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
		// 重置进度，避免无限递归
		d.cfMu.Lock()
		if d.controlFile != nil {
			d.controlFile.CompletedLength = 0
			d.controlFile.PieceBitfields = nil
		}
		d.cfMu.Unlock()
		atomic.StoreInt64(&d.totalDownloaded, 0)
		return d.singleThreadDownload(ctx)
	}

	if resp.StatusCode != http.StatusPartialContent && resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	file, err := os.OpenFile(d.outputPath, os.O_CREATE|os.O_RDWR, defaultFileMode)
	if err != nil {
		return err
	}
	defer file.Close()

	if _, err := file.Seek(existingSize, io.SeekStart); err != nil {
		return fmt.Errorf("seek to position: %w", err)
	}

	atomic.StoreInt64(&d.totalDownloaded, existingSize)

	done := make(chan struct{})
	var doneOnce sync.Once
	defer doneOnce.Do(func() { close(done) })
	go d.periodicSaveProgress(ctx, done)

	buf := make([]byte, d.cfg.BufferSize)

	for {
		select {
		case <-ctx.Done():
			d.saveProgressOrLog()
			return ctx.Err()
		default:
		}

		n, readErr := resp.Body.Read(buf)
		if n > 0 {
			if rl := d.getRateLimiter(); rl != nil {
				if err := rl.Wait(ctx, int64(n)); err != nil {
					d.saveProgressOrLog()
					return err
				}
			}

			_, writeErr := file.Write(buf[:n])
			if writeErr != nil {
				return fmt.Errorf("write: %w", writeErr)
			}

			atomic.AddInt64(&d.totalDownloaded, int64(n))

			if d.recordSpeedAndCheckSave(int64(n)) {
				d.saveProgressOrLog()
			}
		}

		if readErr != nil {
			if readErr == io.EOF {
				break
			}
			d.saveProgressOrLog()
			return fmt.Errorf("read: %w", readErr)
		}
	}

	d.saveProgressOrLog()

	// 下载完成后删除控制文件（启用条件下载时保留，以便下次使用 Last-Modified/ETag）
	if d.controlFilePath != "" && !d.conditionalGet {
		store := task.NewControlFileStore(filepath.Dir(d.controlFilePath))
		taskID := strings.TrimSuffix(filepath.Base(d.controlFilePath), filepath.Ext(d.controlFilePath))
		if err := store.Delete(taskID); err != nil && !errors.Is(err, task.ErrControlFileNotFound) {
			d.logger.Warnw("failed to remove control file", "path", d.controlFilePath, "error", err)
		}
	}

	if saveErr := d.saveCookies(); saveErr != nil {
		d.logger.Warnw("failed to save cookies", "error", saveErr)
	}

	d.applyRemoteTime()

	return nil
}

func (d *Downloader) getRateLimiter() *RateLimiter {
	d.rateMu.Lock()
	defer d.rateMu.Unlock()
	return d.rateLimiter
}

func (d *Downloader) SetRateLimit(rate int64) {
	d.rateMu.Lock()
	defer d.rateMu.Unlock()
	if d.rateLimiter == nil && rate > 0 {
		d.rateLimiter = NewRateLimiter(rate, rate)
		return
	}

	if d.rateLimiter != nil {
		d.rateLimiter.SetRate(rate)
	}
}

func (d *Downloader) GetRateLimit() int64 {
	rl := d.getRateLimiter()
	if rl == nil {
		return 0
	}
	return rl.GetRate()
}

func (d *Downloader) SetRetryConfig(config RetryConfig) {
	d.retryMu.Lock()
	defer d.retryMu.Unlock()
	d.retryConfig = config
}

func (d *Downloader) GetRetryConfig() RetryConfig {
	d.retryMu.Lock()
	defer d.retryMu.Unlock()
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

	u, err := url.Parse(d.url)
	if err != nil {
		return err
	}

	cookies := d.cookieJar.Cookies(u)
	if len(cookies) == 0 {
		return nil
	}

	path := d.getCookieFilePath()
	tmpPath := path + ".tmp"

	file, err := os.Create(tmpPath)
	if err != nil {
		return err
	}

	if err := gob.NewEncoder(file).Encode(cookies); err != nil {
		file.Close()
		os.Remove(tmpPath)
		return err
	}
	file.Close()

	if err := os.Rename(tmpPath, path); err != nil {
		os.Remove(tmpPath)
		return err
	}

	d.logger.Debugw("Saved cookies", "count", len(cookies))
	return nil
}

func (d *Downloader) SetCookieFile(path string) {
	d.cookieFile = path
}

func (d *Downloader) SetAltURLs(urls []string) {
	d.altURLsMu.Lock()
	defer d.altURLsMu.Unlock()
	d.altURLs = urls
}

func (d *Downloader) GetAltURLs() []string {
	d.altURLsMu.Lock()
	defer d.altURLsMu.Unlock()
	return d.altURLs
}

func (d *Downloader) getAltURLs() []string {
	d.altURLsMu.Lock()
	defer d.altURLsMu.Unlock()
	return d.altURLs
}

func (d *Downloader) SetInsecure(insecure bool) {
	d.cfg.Insecure = insecure
	if insecure {
		if transport, ok := d.client.Transport.(*http.Transport); ok {
			transport.TLSClientConfig = &tls.Config{
				InsecureSkipVerify: true,
				MinVersion:         tls.VersionTLS12,
			}
		}
	}
}

func preallocateFile(file *os.File, size int64, allocation string) error {
	stat, err := file.Stat()
	if err != nil {
		return err
	}
	if stat.Size() >= size {
		return nil
	}

	switch allocation {
	case "none":
		return nil
	case "falloc":
		// 尝试 syscall.Fallocate（Linux），失败则回退到 trunc
		if err := syscallFallocate(file, size); err == nil {
			return nil
		}
		return file.Truncate(size)
	case "trunc", "":
		return file.Truncate(size)
	case "prealloc":
		// 简化：prealloc 等同于 trunc
		return file.Truncate(size)
	default:
		return file.Truncate(size)
	}
}
