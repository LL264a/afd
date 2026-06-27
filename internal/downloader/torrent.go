package downloader

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/anacrolix/dht/v2"
	"github.com/anacrolix/torrent"
	"github.com/anacrolix/torrent/metainfo"
	"github.com/anacrolix/torrent/mse"
	"github.com/nexus-dl/afd/pkg/config"
	"github.com/nexus-dl/afd/pkg/logger"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

type BTConfig struct {
	Enabled            bool
	DownloadSpeedLimit int64
	UploadSpeedLimit   int64
	Port               int
	DataDir            string
	TorrentFilesDir    string
	MaxConnections     int
	MaxPeers           int
	SeedRatio          float64
	SeedTime           time.Duration
	TrackerList        []string
	DHTEnabled         bool
	DHTNodes           []string
	DisableTCP         bool
	DisableUTP         bool
	PieceLength        int64
	SequentialDownload bool
	FirstPiecePriority bool
	UPNPEnabled        bool
	LocalPeerDiscovery bool
	EnableSeeding      bool
	SelectFiles        []string // 选择性下载的文件路径列表
	RequireEncryption  bool     // 强制加密（MSE）
	MinCryptoLevel     string   // plain/arc4
	WebSeeds           []string // WebSeed URL 列表
}

type TorrentDownloader struct {
	cfg           *BTConfig
	url           string
	outputPath    string
	logger        *zap.SugaredLogger
	downloaded    int64
	speed         int64
	activeThreads int32
	rateLimit     int64
	retryConfig   RetryConfig
	client        *torrent.Client
	torrent       *torrent.Torrent
}

func NewBTDownloader(cfg *BTConfig, url, outputPath string) *TorrentDownloader {
	return &TorrentDownloader{
		cfg:        cfg,
		url:        url,
		outputPath: outputPath,
		logger:     logger.Log.Named("bt-downloader"),
	}
}

func (d *TorrentDownloader) SetURL(url string) {
	d.url = url
}

func (d *TorrentDownloader) SetOutputPath(path string) {
	d.outputPath = path
}

func (d *TorrentDownloader) SetControlFilePath(path string) {
	// BT 下载通过 anacrolix/torrent 引擎管理进度，不使用控制文件
}

func (d *TorrentDownloader) SetControlFile(cf interface{}) {
	// BT 下载通过 anacrolix/torrent 引擎管理进度，不使用控制文件
}

func (d *TorrentDownloader) URL() string {
	return d.url
}

func (d *TorrentDownloader) OutputPath() string {
	return d.outputPath
}

func (d *TorrentDownloader) FileSize() int64 {
	if d.torrent != nil {
		if info := d.torrent.Info(); info != nil {
			return info.TotalLength()
		}
	}
	return 0
}

func (d *TorrentDownloader) Speed() int64 {
	return atomic.LoadInt64(&d.speed)
}

func (d *TorrentDownloader) Progress() float64 {
	if d.torrent == nil {
		return 0
	}
	info := d.torrent.Info()
	if info == nil {
		return 0
	}
	bytesCompleted := d.torrent.BytesCompleted()
	totalLength := info.TotalLength()
	if totalLength == 0 {
		return 0
	}
	return float64(bytesCompleted) / float64(totalLength) * 100
}

func (d *TorrentDownloader) TotalDownloaded() int64 {
	if d.torrent == nil {
		return atomic.LoadInt64(&d.downloaded)
	}
	return d.torrent.BytesCompleted()
}

func (d *TorrentDownloader) ActiveThreads() int32 {
	if d.torrent == nil {
		return 0
	}
	stats := d.torrent.Stats()
	return int32(stats.ActivePeers)
}

func (d *TorrentDownloader) SetRateLimit(rate int64) {
	atomic.StoreInt64(&d.rateLimit, rate)
}

func (d *TorrentDownloader) GetRateLimit() int64 {
	return atomic.LoadInt64(&d.rateLimit)
}

func (d *TorrentDownloader) SetRetryConfig(config RetryConfig) {
	d.retryConfig = config
}

func (d *TorrentDownloader) GetRetryConfig() RetryConfig {
	return d.retryConfig
}

func (d *TorrentDownloader) LoadProgress(ctx context.Context) error {
	// BT 下载由 torrent 引擎自动管理分片进度，无需显式加载
	return nil
}

func (d *TorrentDownloader) SaveProgress() error {
	// BT 下载由 torrent 引擎自动管理分片进度，无需显式保存
	return nil
}

func createTorrentClient(cfg *BTConfig) (*torrent.Client, error) {
	cfgDir := cfg.DataDir
	if cfgDir == "" {
		cfgDir = "./bt-data"
	}
	if err := os.MkdirAll(cfgDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create bt data dir: %w", err)
	}

	clientCfg := torrent.NewDefaultClientConfig()
	clientCfg.DataDir = cfgDir
	clientCfg.DisableTCP = cfg.DisableTCP
	clientCfg.DisableUTP = cfg.DisableUTP
	clientCfg.NoDefaultPortForwarding = !cfg.UPNPEnabled // UPnP 启用时不禁止默认端口转发
	clientCfg.NoDHT = !cfg.DHTEnabled
	clientCfg.Debug = false

	if cfg.Port > 0 {
		clientCfg.ListenPort = cfg.Port
	}

	// DHT 引导节点注入：anacrolix/torrent 的 ClientConfig 没有 DHTNodes 字段，
	// 而是通过 ClientDhtConfig.DhtStartingNodes 提供 StartingNodesGetter 回调。
	// dht.ResolveHostPorts 将 "host:port" 字符串解析为 dht.Addr。
	if cfg.DHTEnabled && len(cfg.DHTNodes) > 0 {
		nodes := cfg.DHTNodes
		clientCfg.DhtStartingNodes = func(network string) dht.StartingNodesGetter {
			return func() ([]dht.Addr, error) {
				return dht.ResolveHostPorts(nodes)
			}
		}
	}

	// TODO: Local Peer Discovery - anacrolix/torrent v1.58.1 的 ClientConfig
	// 没有 DisableLocalPeerDiscovery 字段，暂无法通过配置控制本地对等节点发现。
	// clientCfg.DisableLocalPeerDiscovery = !cfg.LocalPeerDiscovery

	// 限速配置：每个 token 代表一个字节，burst 取限速值以容纳一个块（通常 16KiB）。
	if cfg.DownloadSpeedLimit > 0 {
		clientCfg.DownloadRateLimiter = rate.NewLimiter(rate.Limit(cfg.DownloadSpeedLimit), int(cfg.DownloadSpeedLimit))
	}
	if cfg.UploadSpeedLimit > 0 {
		clientCfg.UploadRateLimiter = rate.NewLimiter(rate.Limit(cfg.UploadSpeedLimit), int(cfg.UploadSpeedLimit))
	}

	if cfg.DownloadSpeedLimit > 0 || cfg.UploadSpeedLimit > 0 {
		clientCfg.EstablishedConnsPerTorrent = 50
	}

	// 配置 MSE（Message Stream Encryption）头部混淆与加密方法。
	// anacrolix/torrent 的 HeaderObfuscationPolicy 字段为 RequirePreferred 与 Preferred：
	//   - RequirePreferred=true 表示 Preferred 是强制要求（拒绝不符合的连接）
	//   - Preferred=true 表示偏好头部混淆
	if cfg.RequireEncryption {
		clientCfg.HeaderObfuscationPolicy = torrent.HeaderObfuscationPolicy{
			RequirePreferred: true,
			Preferred:        true,
		}
		// 强制加密时仅提供 RC4
		clientCfg.CryptoProvides = mse.CryptoMethodRC4
	} else {
		// 默认：偏好加密但不强制
		clientCfg.HeaderObfuscationPolicy = torrent.HeaderObfuscationPolicy{
			RequirePreferred: false,
			Preferred:        true,
		}
		clientCfg.CryptoProvides = mse.CryptoMethodPlaintext | mse.CryptoMethodRC4
	}

	// 根据最低加密级别配置接收连接时的 CryptoSelector。
	// mse.DefaultCryptoSelector 默认偏好 plaintext，若要求 arc4 则强制选择 RC4。
	minLevel := strings.ToLower(strings.TrimSpace(cfg.MinCryptoLevel))
	if cfg.RequireEncryption && minLevel == "" {
		minLevel = "arc4"
	}
	switch minLevel {
	case "arc4":
		clientCfg.CryptoSelector = func(provided mse.CryptoMethod) mse.CryptoMethod {
			if provided&mse.CryptoMethodRC4 != 0 {
				return mse.CryptoMethodRC4
			}
			return 0 // 拒绝不支持 RC4 的连接
		}
	default:
		// plain 或未设置：使用默认选择器（偏好 plaintext）
		clientCfg.CryptoSelector = mse.DefaultCryptoSelector
	}

	// WebSeed 支持：确保未禁用，并设置自定义 HTTP RoundTripper。
	// 不使用 http.DefaultTransport 以避免全局共享状态，并为 WebSeed
	// HTTP 请求设置合理的拨号、TLS 与连接超时。
	clientCfg.DisableWebseeds = false
	if clientCfg.WebTransport == nil {
		clientCfg.WebTransport = &http.Transport{
			DialContext: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			MaxIdleConns:          100,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
		}
	}

	client, err := torrent.NewClient(clientCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create torrent client: %w", err)
	}

	return client, nil
}

func (d *TorrentDownloader) addTorrent(ctx context.Context) (*torrent.Torrent, error) {
	var t *torrent.Torrent
	if IsMagnetLink(d.url) {
		d.logger.Infow("Adding magnet link", "url", d.url)
		var err error
		t, err = d.client.AddMagnet(d.url)
		if err != nil {
			return nil, fmt.Errorf("failed to add magnet link: %w", err)
		}
	} else if IsTorrentFile(d.url) {
		d.logger.Infow("Adding torrent file", "path", d.url)
		mi, err := metainfo.LoadFromFile(d.url)
		if err != nil {
			return nil, fmt.Errorf("failed to load torrent file: %w", err)
		}
		// TorrentSpecFromMetaInfo 会从 .torrent 文件的 "url-list" 字段自动填充 Webseeds。
		spec := torrent.TorrentSpecFromMetaInfo(mi)
		t, _, err = d.client.AddTorrentSpec(spec)
		if err != nil {
			return nil, fmt.Errorf("failed to add torrent: %w", err)
		}
	} else {
		return nil, fmt.Errorf("unsupported torrent source: %s", d.url)
	}

	// 追加配置中指定的 WebSeed URL（对 magnet link 尤其有用，因为 magnet 不携带 url-list）。
	// AddWebSeeds 签名：func (t *Torrent) AddWebSeeds(urls []string, opts ...AddWebSeedsOpt)
	if len(d.cfg.WebSeeds) > 0 {
		d.logger.Infow("Adding web seeds", "count", len(d.cfg.WebSeeds))
		t.AddWebSeeds(d.cfg.WebSeeds)
	}

	return t, nil
}

func (d *TorrentDownloader) Download(ctx context.Context) error {
	d.logger.Infow("Starting BitTorrent download",
		"url", d.url,
		"output", d.outputPath,
	)

	var err error
	d.client, err = createTorrentClient(d.cfg)
	if err != nil {
		return err
	}
	defer d.client.Close()

	d.torrent, err = d.addTorrent(ctx)
	if err != nil {
		return err
	}

	d.logger.Infow("Waiting for torrent info...")
	select {
	case <-d.torrent.GotInfo():
		d.logger.Infow("Torrent info received",
			"name", d.torrent.Name(),
			"files", len(d.torrent.Files()),
			"size", d.torrent.Info().TotalLength(),
		)
	case <-ctx.Done():
		return ctx.Err()
	}

	// 选择性文件下载
	if len(d.cfg.SelectFiles) > 0 {
		// 先禁用所有文件
		for _, f := range d.torrent.Files() {
			f.SetPriority(torrent.PiecePriorityNone)
		}
		// 启用选择的文件
		for _, f := range d.torrent.Files() {
			filePath := f.Path()
			for _, selectedPath := range d.cfg.SelectFiles {
				// 支持通配符匹配或完全匹配
				if matchFilePath(filePath, selectedPath) {
					d.logger.Infow("Selecting file for download", "path", filePath)
					f.SetPriority(torrent.PiecePriorityNormal)
					break
				}
			}
		}
	} else if d.cfg.SequentialDownload {
		// 顺序下载：给靠前的 piece 设置更高优先级，使其优先下载。
		// anacrolix/torrent 的 PiecePriority 是枚举类型（None/Normal/High/Readahead/Next/Now），
		// 不支持按 index 设置递增数值优先级，这里通过分段设置优先级来近似顺序下载。
		numPieces := d.torrent.NumPieces()
		highWatermark := numPieces / 10
		if highWatermark < 1 {
			highWatermark = 1
		}
		for i := 0; i < numPieces; i++ {
			prio := torrent.PiecePriorityNormal
			if i < highWatermark {
				prio = torrent.PiecePriorityHigh
			}
			d.torrent.Piece(i).SetPriority(prio)
		}
		d.torrent.DownloadAll()
	} else {
		for _, f := range d.torrent.Files() {
			f.SetPriority(torrent.PiecePriorityNormal)
		}
	}

	// FirstPiecePriority：优先下载第一个 piece（用于快速预览）
	if d.cfg.FirstPiecePriority && d.torrent.NumPieces() > 0 {
		d.torrent.Piece(0).SetPriority(torrent.PiecePriorityHigh)
	}

	// Derived context for the monitor goroutine.  Cancelled in defer
	// so it always exits when Download returns, even if the caller's
	// ctx is never cancelled (e.g. on a successful download).  Without
	// this, monitorProgress would leak for the lifetime of the process.
	monitorCtx, cancelMonitor := context.WithCancel(ctx)
	defer cancelMonitor()

	go d.monitorProgress(monitorCtx)

	// 启动下载并等待完成
	if len(d.cfg.SelectFiles) == 0 {
		d.torrent.DownloadAll()
	}

	// 使用循环检查进度直到完成。
	// 使用 NewTimer 替代 time.After，避免在 for-select 长循环中累积未触发的定时器。
	progressTimer := time.NewTimer(1 * time.Second)
	defer progressTimer.Stop()
	// 初始不需要立即触发，排空使其进入待 Reset 状态。
	if !progressTimer.Stop() {
		<-progressTimer.C
	}
	for {
		progressTimer.Reset(1 * time.Second)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-progressTimer.C:
			info := d.torrent.Info()
			if info != nil {
				// 检查是否所有选择的文件都已完成
				if d.isDownloadComplete() {
					d.logger.Infow("Torrent download complete", "name", d.torrent.Name())
					goto DownloadComplete
				}
			}
		}
	}

DownloadComplete:
	d.logger.Infow("Torrent download complete", "name", d.torrent.Name())

	if d.outputPath != "" && d.outputPath != d.cfg.DataDir {
		if err := d.moveFiles(d.outputPath); err != nil {
			d.logger.Errorw("Failed to move files", "error", err)
		}
	}

	// 做种功能
	if d.cfg.EnableSeeding {
		d.logger.Infow("Starting seeding", "ratio", d.cfg.SeedRatio, "time", d.cfg.SeedTime)
		if err := d.seed(ctx); err != nil {
			d.logger.Warnw("Seeding stopped with error", "error", err)
		}
	}

	return nil
}

func (d *TorrentDownloader) isDownloadComplete() bool {
	// 检查选择的文件是否完成
	if len(d.cfg.SelectFiles) > 0 {
		for _, f := range d.torrent.Files() {
			filePath := f.Path()
			for _, selectedPath := range d.cfg.SelectFiles {
				if matchFilePath(filePath, selectedPath) {
					if f.BytesCompleted() < f.Length() {
						return false
					}
					break
				}
			}
		}
		return true
	}

	// 否则检查所有文件
	info := d.torrent.Info()
	if info == nil {
		return false
	}
	return d.torrent.BytesCompleted() >= info.TotalLength()
}

func matchFilePath(filePath, pattern string) bool {
	// 简单的匹配逻辑：完全匹配、后缀匹配或前缀匹配
	if filePath == pattern {
		return true
	}
	if strings.HasSuffix(filePath, pattern) {
		return true
	}
	if strings.HasPrefix(filePath, pattern) {
		return true
	}
	// 支持通配符 *
	if strings.Contains(pattern, "*") {
		matched, _ := filepath.Match(pattern, filePath)
		return matched
	}
	return false
}

func (d *TorrentDownloader) seed(ctx context.Context) error {
	seedStartTime := time.Now()
	seedTargetRatio := d.cfg.SeedRatio
	seedTargetTime := d.cfg.SeedTime

	// 计算总下载量作为做种基准
	var totalDownloaded int64
	for _, f := range d.torrent.Files() {
		totalDownloaded += f.Length()
	}

	if totalDownloaded == 0 {
		return fmt.Errorf("no data to seed")
	}

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			d.logger.Infow("Seeding stopped by context")
			return nil
		case <-ticker.C:
			stats := d.torrent.Stats()
			// 获取上传的字节数 - 使用 Int64() 方法或访问字段
			uploadedBytes := stats.BytesWrittenData.Int64()
			currentRatio := float64(uploadedBytes) / float64(totalDownloaded)
			elapsedTime := time.Since(seedStartTime)

			d.logger.Infow("Seeding progress",
				"ratio", fmt.Sprintf("%.2f", currentRatio),
				"target_ratio", seedTargetRatio,
				"elapsed_time", elapsedTime,
				"target_time", seedTargetTime,
				"peers", stats.ActivePeers,
				"uploaded", uploadedBytes,
			)

			// 检查是否达到做种目标
			if seedTargetRatio > 0 && currentRatio >= seedTargetRatio {
				d.logger.Infow("Seeding complete, ratio reached", "ratio", currentRatio)
				return nil
			}
			if seedTargetTime > 0 && elapsedTime >= seedTargetTime {
				d.logger.Infow("Seeding complete, time reached", "elapsed", elapsedTime)
				return nil
			}
		}
	}
}

func (d *TorrentDownloader) monitorProgress(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	var lastBytes int64
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if d.torrent == nil {
				continue
			}
			currentBytes := d.torrent.BytesCompleted()
			atomic.StoreInt64(&d.speed, currentBytes-lastBytes)
			lastBytes = currentBytes
			atomic.StoreInt64(&d.downloaded, currentBytes)

			stats := d.torrent.Stats()
			d.logger.Debugw("Download progress",
				"progress", fmt.Sprintf("%.2f%%", d.Progress()),
				"downloaded", currentBytes,
				"speed", d.Speed(),
				"peers", stats.ActivePeers,
				"seeders", stats.ConnectedSeeders,
			)
		}
	}
}

func (d *TorrentDownloader) moveFiles(targetDir string) error {
	if err := os.MkdirAll(targetDir, 0755); err != nil {
		return err
	}

	for _, f := range d.torrent.Files() {
		srcPath := filepath.Join(d.cfg.DataDir, f.Path())
		dstPath := filepath.Join(targetDir, f.Path())

		if err := os.MkdirAll(filepath.Dir(dstPath), 0755); err != nil {
			return err
		}

		if err := os.Rename(srcPath, dstPath); err != nil {
			return fmt.Errorf("failed to move %s: %w", f.Path(), err)
		}
	}

	return nil
}

func ParseMagnetLink(magnet string) (infoHash string, displayName string, err error) {
	magnet = string(magnet)
	if !strings.HasPrefix(magnet, "magnet:?") {
		return "", "", fmt.Errorf("invalid magnet link")
	}

	u, err := url.Parse(magnet)
	if err != nil {
		return "", "", err
	}

	xt := u.Query().Get("xt")
	infoHash = strings.TrimPrefix(xt, "urn:btih:")
	displayName = u.Query().Get("dn")

	return infoHash, displayName, nil
}

func IsTorrentFile(path string) bool {
	return strings.HasSuffix(strings.ToLower(path), ".torrent")
}

func IsMagnetLink(input string) bool {
	return strings.HasPrefix(strings.ToLower(input), "magnet:?")
}

type BTProtocolHandler struct {
	cfg *BTConfig
}

func NewBTProtocolHandler(cfg *BTConfig) *BTProtocolHandler {
	return &BTProtocolHandler{cfg: cfg}
}

func (h *BTProtocolHandler) CanHandle(input string) bool {
	return IsMagnetLink(input) || IsTorrentFile(input)
}

func (h *BTProtocolHandler) NewDownloader(url, outputPath string) interface {
	Download(context.Context) error
} {
	return NewBTDownloader(h.cfg, url, outputPath)
}

func NewBTDownloaderFromURL(cfg *config.DownloadConfig, url, outputPath string) (interface {
	Download(context.Context) error
}, error) {
	btCfg := &BTConfig{
		Enabled:            true,
		DHTEnabled:         true,
		Port:               6881,
		DataDir:            "./bt-data",
		MaxConnections:     100,
		MaxPeers:           100,
		TrackerList:        []string{},
		DisableTCP:         false,
		DisableUTP:         false,
		SequentialDownload: false,
		FirstPiecePriority: false,
		UPNPEnabled:        true,
		LocalPeerDiscovery: true,
		EnableSeeding:      true,
	}
	if cfg != nil && cfg.BT != nil {
		btCfg.Enabled = cfg.BT.Enabled
		btCfg.DownloadSpeedLimit = cfg.BT.DownloadSpeedLimit
		btCfg.UploadSpeedLimit = cfg.BT.UploadSpeedLimit
		btCfg.Port = cfg.BT.Port
		btCfg.DataDir = cfg.BT.DataDir
		btCfg.TorrentFilesDir = cfg.BT.TorrentFilesDir
		btCfg.MaxConnections = cfg.BT.MaxConnections
		btCfg.MaxPeers = cfg.BT.MaxPeers
		btCfg.SeedRatio = cfg.BT.SeedRatio
		btCfg.SeedTime = cfg.BT.SeedTime
		btCfg.TrackerList = cfg.BT.TrackerList
		btCfg.DHTEnabled = cfg.BT.DHTEnabled
		btCfg.DHTNodes = cfg.BT.DHTNodes
		btCfg.DisableTCP = cfg.BT.DisableTCP
		btCfg.DisableUTP = cfg.BT.DisableUTP
		btCfg.PieceLength = cfg.BT.PieceLength
		btCfg.SequentialDownload = cfg.BT.SequentialDownload
		btCfg.FirstPiecePriority = cfg.BT.FirstPiecePriority
		btCfg.UPNPEnabled = cfg.BT.UPNPEnabled
		btCfg.LocalPeerDiscovery = cfg.BT.LocalPeerDiscovery
		btCfg.EnableSeeding = cfg.BT.EnableSeeding
		btCfg.RequireEncryption = cfg.BT.RequireEncryption
		btCfg.MinCryptoLevel = cfg.BT.MinCryptoLevel
		btCfg.WebSeeds = cfg.BT.WebSeeds
	}
	return NewBTDownloader(btCfg, url, outputPath), nil
}

// CreateTorrent 从文件或目录创建 .torrent 文件 (暂时占位)
func CreateTorrent(sourcePath, outputPath string, trackers []string, pieceLength int64, name string) error {
	// 暂时返回不支持的错误
	return fmt.Errorf("CreateTorrent function temporarily unavailable")
}

// CreateMagnetFromTorrent 从 .torrent 文件创建 magnet 链接
func CreateMagnetFromTorrent(torrentPath string) (string, error) {
	mi, err := metainfo.LoadFromFile(torrentPath)
	if err != nil {
		return "", fmt.Errorf("failed to load torrent file: %w", err)
	}

	magnet := mi.Magnet(nil, nil)
	return magnet.String(), nil
}
