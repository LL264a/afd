package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/nexus-dl/afd/internal"
	"github.com/nexus-dl/afd/internal/api"
	"github.com/nexus-dl/afd/internal/cluster"
	"github.com/nexus-dl/afd/internal/downloader"
	"github.com/nexus-dl/afd/internal/nat"
	"github.com/nexus-dl/afd/internal/plugin"
	"github.com/nexus-dl/afd/internal/task"
	"github.com/nexus-dl/afd/pkg/config"
	"github.com/nexus-dl/afd/pkg/logger"
	"github.com/spf13/cobra"
)

var (
	Version   = "dev"
	Commit    = "unknown"
	BuildTime = "unknown"
	cfgFile   string
	rpcAddr   string
	rpcToken  string
)

var rootCmd = &cobra.Command{
	Use:   "afd",
	Short: "AFD - Auto Download Tool",
	Long: `AFD is a distributed, cluster-aware download system.
It supports HTTP, FTP, BitTorrent, S3, WebDAV, and more.

快速使用:
  afd http://example.com/file.zip           # 直接下载
  afd -o file.zip http://example.com/file   # 指定输出
  afd -s 4 http://example.com/file          # 4线程
  afd -i urls.txt                            # 批量下载`,
	Version: fmt.Sprintf("%s (commit: %s, built: %s)", Version, Commit, BuildTime),
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		if cfgFile != "" {
			if _, err := config.Load(cfgFile); err != nil {
				// 配置加载失败不阻断，CLI 参数优先
				if logger.Log != nil {
					logger.Log.Debugw("config load skipped", "error", err)
				}
			}
		}
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		// 没有子命令时，直接当作下载处理
		if len(args) > 0 {
			return doDownload(args[0], output, explicitOutput)
		}
		return cmd.Help()
	},
}

var serveCmd = &cobra.Command{
	Use:   "serve",
	Short: "启动 AFD 服务",
	RunE: func(cmd *cobra.Command, args []string) error {
		return runServe()
	},
}

var addCmd = &cobra.Command{
	Use:   "add <url>",
	Short: "添加下载任务 (需要先启动服务)",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return runAdd(args[0])
	},
}

var listCmd = &cobra.Command{
	Use:   "list",
	Short: "列出所有任务 (需要先启动服务)",
	RunE: func(cmd *cobra.Command, args []string) error {
		return runList()
	},
}

var pauseCmd = &cobra.Command{
	Use:   "pause <task-id>",
	Short: "暂停任务 (需要先启动服务)",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return runPause(args[0])
	},
}

var resumeCmd = &cobra.Command{
	Use:   "resume <task-id>",
	Short: "恢复任务 (需要先启动服务)",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return runResume(args[0])
	},
}

var removeCmd = &cobra.Command{
	Use:   "remove <task-id>",
	Short: "删除任务 (需要先启动服务)",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return runRemove(args[0])
	},
}

var statusCmd = &cobra.Command{
	Use:   "status",
	Short: "查看集群状态 (需要先启动服务)",
	RunE: func(cmd *cobra.Command, args []string) error {
		return runStatus()
	},
}

var (
	parallel            int
	output              string
	explicitOutput      bool // -o 是否被显式指定
	speedLimit          string
	timeout             int
	inputFile           string
	dir                 string
	adaptive            bool
	insecure            bool
	quiet               bool
	noNetrc             bool
	daemon              bool
	streamPieceSelector string
	uriSelector         string

	// wget/curl 兼容 flag
	userAgent            string
	headers              []string
	referer              string
	httpUser             string
	httpPassword         string
	spider               bool
	serverResponse       bool
	noContentDisposition bool
	remoteTime           bool
	maxTime              int
)

// errInterrupted 表示用户通过 SIGINT/SIGTERM 中断下载。
// main() 将其映射为 exit code 130（128+SIGINT），与 wget 行为一致。
var errInterrupted = errors.New("download interrupted by user")

var downloadCmd = &cobra.Command{
	Use:   "dl <url>",
	Short: "下载文件 (download 的别名)",
	Long: `直接下载文件，无需启动服务。

示例:
  afd dl http://example.com/file.zip           # 直接下载
  afd dl -o /tmp/file.zip http://example.com/file.zip  # 指定输出
  afd dl -s 4 http://example.com/file.zip      # 4线程
  afd dl -i urls.txt                           # 批量下载
  afd dl -U "MyApp/1.0" URL                    # 自定义 User-Agent
  afd dl -H "Authorization: Bearer token" URL  # 自定义请求头 (可多次)
  afd dl -e https://ref.example.com URL        # 设置 Referer
  afd dl --http-user user --http-password pass URL  # HTTP 认证
  afd dl --spider URL                          # 只检查不下载
  afd dl -S URL                                # 打印服务器响应头
  afd dl -m 60 URL                             # 60秒总超时
  afd dl --remote-time URL                     # 用服务器时间设置文件时间`,
	Aliases: []string{"download"},
	Args:    cobra.RangeArgs(0, 2),
	RunE: func(cmd *cobra.Command, args []string) error {
		// 批量下载模式
		if inputFile != "" {
			return doBatchDownload(inputFile)
		}

		// 单文件下载模式
		if len(args) == 0 {
			return cmd.Help()
		}

		rawURL := args[0]
		// 自动补全 URL scheme（对标 wget/curl：无 scheme 时默认 http://）
		if !strings.Contains(rawURL, "://") {
			// 跳过已经是 scheme: 形式的（如 file:, data:, magnet:）
			if !strings.Contains(rawURL, ":") || looksLikeHostPort(rawURL) {
				rawURL = "http://" + rawURL
			}
		}

		outPath := output
		explicit := explicitOutput

		if outPath == "" && len(args) > 1 {
			outPath = args[1]
			explicit = true
		}

		if outPath == "" {
			// 从 URL 路径推断文件名（剥离查询参数）
			if name, ok := inferFilename(rawURL); ok {
				if dir != "" {
					outPath = filepath.Join(dir, name)
				} else {
					outPath = name
				}
			}
		}

		if outPath == "" || strings.HasPrefix(outPath, "-") {
			return fmt.Errorf("请指定输出文件路径: -o <path>")
		}

		return doDownload(rawURL, outPath, explicit)
	},
}

func printBanner() {
	fmt.Println()
	fmt.Println("  _   _      _ _         ___        _   ")
	fmt.Println(" | \\ | |    | | |       |__ \\      | |  ")
	fmt.Println(" |  \\| | ___| | |_ ___     ) |___  | |_ ")
	fmt.Println(" | . ` |/ _ \\ | __/ _ \\   / /___ \\ | __|")
	fmt.Println(" | |\\  |  __/ | || (_) | |____| || | | ")
	fmt.Println("  \\_| \\_/\\___|_|\\__\\___/       |_||_| ")
	fmt.Println()
	fmt.Printf("  Version: %s\n", Version)
	fmt.Printf("  Commit:  %s\n", Commit)
	fmt.Printf("  Built:   %s\n", BuildTime)
	fmt.Printf("  Website: https://github.com/nexus-dl/afd\n")
	fmt.Println(strings.Repeat("=", 50))
}

func doDownload(url, outputPath string, explicit bool) error {
	cfg := config.DefaultDownloadConfig()

	if speedLimit != "" {
		var limit int64
		var suffix string
		n, err := fmt.Sscanf(speedLimit, "%d%s", &limit, &suffix)
		if err != nil && n < 1 {
			return fmt.Errorf("invalid speed limit: %s", speedLimit)
		}
		suffix = strings.ToLower(suffix)
		switch suffix {
		case "", "b":
			// bytes per second
		case "k":
			limit *= 1024
		case "m":
			limit *= 1024 * 1024
		case "g":
			limit *= 1024 * 1024 * 1024
		default:
			return fmt.Errorf("invalid speed limit suffix: %s", suffix)
		}
		cfg.SpeedLimit = limit
	}

	if timeout > 0 {
		cfg.Timeout = time.Duration(timeout) * time.Second
	}

	if parallel > 0 {
		cfg.MaxConnections = parallel
	}

	if adaptive {
		cfg.Adaptive = true
	}

	if insecure {
		cfg.Insecure = true
	}

	if noNetrc {
		cfg.NoNetrc = true
	}

	if streamPieceSelector != "" {
		cfg.StreamPieceSelector = streamPieceSelector
	}

	if uriSelector != "" {
		cfg.UriSelector = uriSelector
	}

	// wget/curl 兼容选项
	if userAgent != "" {
		cfg.UserAgent = userAgent
	}
	if referer != "" {
		cfg.Referer = referer
	}
	if httpUser != "" {
		cfg.HTTPUsername = httpUser
		cfg.HTTPPassword = httpPassword
	}
	if len(headers) > 0 {
		cfg.CustomHeaders = make(map[string]string, len(headers))
		for _, h := range headers {
			// 解析 "Key: Value" 格式（对标 curl -H）
			idx := strings.Index(h, ":")
			if idx > 0 {
				key := strings.TrimSpace(h[:idx])
				val := strings.TrimSpace(h[idx+1:])
				if key != "" {
					cfg.CustomHeaders[key] = val
				}
			} else {
				return fmt.Errorf("invalid header format (expected 'Key: Value'): %s", h)
			}
		}
	}
	if remoteTime {
		cfg.RemoteTime = true
	}

	cfg.Quiet = quiet

	logLevel := "info"
	if quiet {
		logLevel = "error"
	}
	logger.Init(logLevel, "")
	defer logger.Log.Sync()

	log := logger.Log.Named("download")

	// --spider 模式：只检查不下载（对标 wget --spider）
	if spider {
		return doSpider(url, cfg, serverResponse)
	}

	log.Infow("starting download", "url", url, "output", outputPath,
		"speed_limit", speedLimit, "parallel", parallel, "adaptive", adaptive, "insecure", insecure)

	// --server-response: 下载前打印响应头（对标 wget -S，调试用途）
	if serverResponse {
		if err := probeAndPrintHeaders(url, cfg); err != nil {
			log.Warnw("server-response probe failed", "error", err)
		}
	}

	d, err := downloader.NewDownloaderFromURL(url, outputPath, cfg, log)
	if err != nil {
		return fmt.Errorf("创建下载器失败: %w", err)
	}

	// 设置控制文件路径，支持断点续传
	d.SetControlFilePath(outputPath + ".ctl")

	ctx, cancel := context.WithCancel(context.Background())
	// --max-time: 用 context 超时实现总时长限制（不用 http.Client.Timeout，避免杀死大文件）
	if maxTime > 0 {
		ctx, cancel = context.WithTimeout(ctx, time.Duration(maxTime)*time.Second)
	}
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		log.Infow("received signal, stopping download", "signal", sig)
		cancel()
	}()

	startTime := time.Now()

	// TTY 下显示动态进度条（200ms 刷新），非 TTY 回退到结构化日志（2s 间隔）。
	tty := isTerminal(os.Stderr) && !quiet
	interval := 2 * time.Second
	var bar *progressBar
	if tty {
		interval = 200 * time.Millisecond
		bar = &progressBar{w: os.Stderr, width: 30}
	}
	ticker := time.NewTicker(interval)
	go func() {
		defer ticker.Stop()
		var lastLoggedPct int
		for {
			select {
			case <-ctx.Done():
				if bar != nil {
					fmt.Fprint(os.Stderr, "\n")
				}
				return
			case <-ticker.C:
				progress := d.Progress()
				speed := d.Speed()
				downloaded := d.TotalDownloaded()
				fileSize := d.FileSize()
				pct := int(progress)

				if bar != nil {
					bar.render(pct, downloaded, fileSize, speed)
				} else if pct != lastLoggedPct || speed > 0 {
					lastLoggedPct = pct
					if fileSize > 0 {
						log.Infow("progress", "pct", pct, "downloaded", formatBytes(downloaded),
							"total", formatBytes(fileSize), "speed", fmt.Sprintf("%s/s", formatBytes(speed)))
					} else {
						log.Infow("progress", "pct", pct, "downloaded", formatBytes(downloaded),
							"speed", fmt.Sprintf("%s/s", formatBytes(speed)))
					}
				}
			}
		}
	}()

	err = d.Download(ctx)
	if err != nil {
		if ctx.Err() != nil {
			log.Infow("download cancelled")
			signal.Stop(sigCh)
			cancel()
			// 用户中断返回 130（128+SIGINT），让 shell 脚本能区分
			return errInterrupted
		}
		signal.Stop(sigCh)
		cancel()
		return fmt.Errorf("下载失败: %w", err)
	}
	signal.Stop(sigCh)
	cancel()

	// Content-Disposition 重命名：若 -o 未显式指定且未禁用，用服务器建议的文件名
	if !explicit && !noContentDisposition {
		if cd := d.ContentDisposition(); cd != "" {
			if name := downloader.ParseContentDispositionFilename(cd); name != "" {
				name = sanitizeFilename(name)
				dir := filepath.Dir(outputPath)
				newPath := filepath.Join(dir, name)
				if newPath != outputPath {
					if _, e := os.Stat(newPath); os.IsNotExist(e) {
						if e := os.Rename(outputPath, newPath); e == nil {
							log.Infow("renamed output per Content-Disposition", "from", outputPath, "to", newPath)
							outputPath = newPath
						}
					}
				}
			}
		}
	}

	elapsed := time.Since(startTime)
	fileSize := d.FileSize()
	downloaded := d.TotalDownloaded()
	// 未知长度（fileSize==0）时用实际下载字节数计算平均速度
	speedBase := fileSize
	if speedBase <= 0 {
		speedBase = downloaded
	}
	var avgSpeed int64
	if elapsed.Seconds() > 0 {
		avgSpeed = int64(float64(speedBase) / elapsed.Seconds())
	}
	log.Infow("download finished",
		"elapsed", elapsed.Round(time.Second).String(),
		"file_size", formatBytes(fileSize),
		"downloaded", formatBytes(downloaded),
		"avg_speed", fmt.Sprintf("%s/s", formatBytes(avgSpeed)))

	return nil
}

func doBatchDownload(inputFile string) error {
	data, err := os.ReadFile(inputFile)
	if err != nil {
		return fmt.Errorf("读取文件失败: %w", err)
	}

	lines := strings.Split(string(data), "\n")
	urls := []string{}
	outNames := []string{}
	currentDir := dir
	var pendingOut string

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		// 处理 aria2 风格的选项
		if strings.HasPrefix(line, "dir=") {
			currentDir = strings.TrimPrefix(line, "dir=")
			continue
		}
		if strings.HasPrefix(line, "out=") {
			pendingOut = strings.TrimPrefix(line, "out=")
			continue
		}
		if strings.HasPrefix(line, "http://") ||
			strings.HasPrefix(line, "https://") ||
			strings.HasPrefix(line, "ftp://") ||
			strings.HasPrefix(line, "magnet:") ||
			strings.HasPrefix(line, "file://") {
			urls = append(urls, line)
			outNames = append(outNames, pendingOut)
			pendingOut = ""
		}
	}

	if len(urls) == 0 {
		return fmt.Errorf("文件中没有找到有效的下载链接")
	}

	fmt.Printf("批量下载: 找到 %d 个任务\n", len(urls))
	if currentDir != "" {
		fmt.Printf("保存目录: %s\n", currentDir)
	}

	success := 0
	failed := 0

	for i, url := range urls {
		fmt.Printf("\n[%d/%d] %s\n", i+1, len(urls), url)

		outPath := filepath.Base(url)
		if i < len(outNames) && outNames[i] != "" {
			outPath = outNames[i]
		}
		if currentDir != "" {
			outPath = filepath.Join(currentDir, outPath)
		}

		if err := doDownload(url, outPath, true); err != nil {
			fmt.Printf("下载失败: %v\n", err)
			failed++
		} else {
			success++
		}
	}

	fmt.Printf("\n========== 下载完成 ==========\n")
	fmt.Printf("成功: %d, 失败: %d\n", success, failed)

	return nil
}

func formatBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for n >= div*unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(n)/float64(div), "KMGTPE"[exp])
}

// looksLikeHostPort 判断 "host:port" 形式（避免误判为 scheme:）。
func looksLikeHostPort(s string) bool {
	idx := strings.LastIndex(s, ":")
	if idx < 0 {
		return false
	}
	// 冒号后全是数字 → host:port
	tail := s[idx+1:]
	for _, r := range tail {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

// inferFilename 从 URL 路径推断输出文件名。
// 尾斜杠 URL（如 http://example.com/）返回 ("index.html", true)。
// 无法推断时返回 ("", false)。
func inferFilename(rawURL string) (string, bool) {
	u, err := url.Parse(rawURL)
	if err != nil {
		// 回退到 filepath.Base
		name := filepath.Base(rawURL)
		if name == "." || name == "/" || name == "" || name == "\\" {
			return "index.html", true
		}
		return sanitizeFilename(name), true
	}
	// URL 路径始终使用 '/' 分隔符，用 path.Base（非 filepath.Base）
	// 避免在 Windows 上把 '/' 误判为分隔符返回 '\'
	name := path.Base(u.Path)
	if name == "." || name == "/" || name == "" {
		return "index.html", true
	}
	return sanitizeFilename(name), true
}

// sanitizeFilename 替换文件系统非法字符。
func sanitizeFilename(name string) string {
	// 替换 Windows 和 Unix 非法字符
	repl := strings.NewReplacer(
		"\\", "_", "/", "_", ":", "_",
		"*", "_", "?", "_", "\"", "_",
		"<", "_", ">", "_", "|", "_",
	)
	return repl.Replace(name)
}

// applyCfgHeaders 将 cfg 中的自定义头、Referer、User-Agent 和 HTTP 认证应用到请求。
// 用于 --spider/--server-response 的独立探测请求（不依赖 downloader 内部状态）。
func applyCfgHeaders(req *http.Request, cfg *config.DownloadConfig) {
	if cfg.UserAgent != "" {
		req.Header.Set("User-Agent", cfg.UserAgent)
	} else {
		req.Header.Set("User-Agent", "AFD/0.3")
	}
	if cfg.Referer != "" {
		req.Header.Set("Referer", cfg.Referer)
	}
	for k, v := range cfg.CustomHeaders {
		req.Header.Set(k, v)
	}
	if cfg.HTTPUsername != "" {
		req.SetBasicAuth(cfg.HTTPUsername, cfg.HTTPPassword)
	}
}

// buildProbeClient 构建用于 --spider/--server-response 的轻量 HTTP 客户端。
// 不设置 http.Client.Timeout（避免限制大文件），仅用 Transport 级超时。
func buildProbeClient(cfg *config.DownloadConfig) *http.Client {
	transport := &http.Transport{
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: 60 * time.Second,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2: true,
	}
	if cfg.Insecure {
		transport.TLSClientConfig = &tls.Config{
			InsecureSkipVerify: true,
			MinVersion:         tls.VersionTLS12,
		}
	}
	return &http.Client{Transport: transport}
}

// doSpider 执行 --spider 模式：发 HEAD 请求，打印状态和元数据，不下载文件。
func doSpider(rawURL string, cfg *config.DownloadConfig, printHeaders bool) error {
	client := buildProbeClient(cfg)
	req, err := http.NewRequest("HEAD", rawURL, nil)
	if err != nil {
		return fmt.Errorf("创建请求失败: %w", err)
	}
	applyCfgHeaders(req, cfg)

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("请求失败: %w", err)
	}
	defer resp.Body.Close()

	fmt.Fprintf(os.Stderr, "HTTP %s\n", resp.Status)
	if printHeaders {
		resp.Header.Write(os.Stderr)
		fmt.Fprintln(os.Stderr)
	}

	size := resp.ContentLength
	ct := resp.Header.Get("Content-Type")
	fmt.Fprintf(os.Stderr, "URL: %s\n", rawURL)
	if size >= 0 {
		fmt.Fprintf(os.Stderr, "Size: %d (%s)\n", size, formatBytes(size))
	} else {
		fmt.Fprintln(os.Stderr, "Size: unknown")
	}
	if ct != "" {
		fmt.Fprintf(os.Stderr, "Content-Type: %s\n", ct)
	}
	fmt.Fprintf(os.Stderr, "Supports Range: %v\n", resp.Header.Get("Accept-Ranges") == "bytes")

	if resp.StatusCode >= 400 {
		return fmt.Errorf("HTTP %s", resp.Status)
	}
	return nil
}

// probeAndPrintHeaders 为 --server-response 打印响应头（非 spider 模式）。
// 会多发一次 HEAD 请求，调试场景可接受（wget -S 也打印每次请求的响应头）。
func probeAndPrintHeaders(rawURL string, cfg *config.DownloadConfig) error {
	client := buildProbeClient(cfg)
	req, err := http.NewRequest("HEAD", rawURL, nil)
	if err != nil {
		return err
	}
	applyCfgHeaders(req, cfg)

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	fmt.Fprintf(os.Stderr, "HTTP %s\n", resp.Status)
	resp.Header.Write(os.Stderr)
	fmt.Fprintln(os.Stderr)
	return nil
}

// isTerminal 检测文件描述符是否连接到终端（非 TTY 环境如 journald 回退到日志输出）。
func isTerminal(f *os.File) bool {
	fi, err := f.Stat()
	if err != nil {
		return false
	}
	return (fi.Mode() & os.ModeCharDevice) != 0
}

// progressBar 在 TTY 下渲染动态单行进度条。
// 格式: [========>           ] 45% | 5.2 MiB/s | 1.2 GiB / 2.4 GiB | ETA 3m45s
type progressBar struct {
	w     *os.File
	width int
}

func (p *progressBar) render(pct int, downloaded, fileSize, speed int64) {
	if pct < 0 {
		pct = 0
	} else if pct > 100 {
		pct = 100
	}
	filled := p.width * pct / 100
	bar := strings.Repeat("=", filled)
	if filled < p.width {
		bar += ">"
		bar += strings.Repeat(" ", p.width-filled-1)
	} else {
		bar = strings.Repeat("=", p.width)
	}

	total := formatBytes(fileSize)
	if fileSize <= 0 {
		total = "?"
	}

	var eta string
	if speed > 0 && fileSize > 0 && downloaded < fileSize {
		remaining := fileSize - downloaded
		secs := remaining / speed
		if secs < 60 {
			eta = fmt.Sprintf("%ds", secs)
		} else {
			eta = fmt.Sprintf("%dm%ds", secs/60, secs%60)
		}
	} else {
		eta = "--"
	}

	fmt.Fprintf(p.w, "\r[%s] %3d%% | %8s/s | %s / %s | ETA %s   ",
		bar, pct, formatBytes(speed), formatBytes(downloaded), total, eta)
}

// --- RPC client and command implementations ---

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type rpcResponse struct {
	Result json.RawMessage `json:"result,omitempty"`
	Error  *rpcError       `json:"error,omitempty"`
}

type rpcClient struct {
	addr  string
	token string
	http  *http.Client
}

func newRPCClient(addr, token string) *rpcClient {
	if addr == "" {
		addr = "http://localhost:6800/jsonrpc"
	}
	return &rpcClient{
		addr:  addr,
		token: token,
		http:  &http.Client{Timeout: 30 * time.Second},
	}
}

func (c *rpcClient) call(method string, params []any) (json.RawMessage, error) {
	reqBody := map[string]any{
		"jsonrpc": "2.0",
		"method":  method,
		"params":  params,
		"id":      "1",
	}
	bodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("编码请求失败: %w", err)
	}
	req, err := http.NewRequest(http.MethodPost, c.addr, bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("创建请求失败: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("连接 RPC 服务器失败: %w", err)
	}
	defer resp.Body.Close()
	var rpcResp rpcResponse
	if err := json.NewDecoder(resp.Body).Decode(&rpcResp); err != nil {
		return nil, fmt.Errorf("解析响应失败: %w", err)
	}
	if rpcResp.Error != nil {
		return nil, fmt.Errorf("RPC 错误 %d: %s", rpcResp.Error.Code, rpcResp.Error.Message)
	}
	return rpcResp.Result, nil
}

// daemonize 以守护进程方式重新启动自身。
// Windows 不支持真正的守护进程，提示用户使用 Start-Process 或服务。
// Unix 上通过重新执行自身、分离标准输入/输出并调用 setsid 创建新会话实现，
// 确保终端关闭时子进程不会收到 SIGHUP 被杀死。
func daemonize() error {
	if runtime.GOOS == "windows" {
		fmt.Fprintln(os.Stderr, "Daemon mode is not supported on Windows. Use 'Start-Process' or install as a service.")
		return nil
	}

	// 已经是守护进程子进程，正常继续执行
	if os.Getenv("AFD_DAEMONIZED") == "1" {
		return nil
	}

	cmd := exec.Command(os.Args[0], os.Args[1:]...)
	cmd.Stdin = nil
	cmd.Env = append(os.Environ(), "AFD_DAEMONIZED=1")

	// 重定向 daemon 子进程的标准输出/错误到日志文件，避免日志全部丢失。
	// 子进程通过 FD 继承持有日志文件描述符，父进程退出后子进程仍可写入。
	logDir := "/var/log/afd"
	if runtime.GOOS == "windows" {
		logDir = filepath.Join(os.Getenv("APPDATA"), "afd", "logs")
	}
	if err := os.MkdirAll(logDir, 0755); err == nil {
		if logFile, err := os.OpenFile(filepath.Join(logDir, "afd.log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644); err == nil {
			cmd.Stdout = logFile
			cmd.Stderr = logFile
		}
	}

	// Unix: 创建新会话脱离控制终端；Windows: no-op
	setSysProcAttr(cmd)

	if err := cmd.Start(); err != nil {
		return err
	}

	fmt.Println("Daemon started with PID:", cmd.Process.Pid)
	os.Exit(0)
	return nil
}

func runServe() error {
	if daemon {
		if err := daemonize(); err != nil {
			return fmt.Errorf("启动守护进程失败: %w", err)
		}
	}

	printBanner()

	cfg, err := config.Load(cfgFile)
	if err != nil {
		return fmt.Errorf("加载配置失败: %w", err)
	}

	if quiet {
		cfg.Node.LogLevel = "error"
	}
	if err := logger.Init(cfg.Node.LogLevel, ""); err != nil {
		return fmt.Errorf("初始化日志失败: %w", err)
	}
	defer logger.Log.Sync()

	taskQueue := task.NewTaskQueue(cfg.Download.MaxConnections)
	taskStore := task.NewTaskStore(cfg.Node.DataDir)
	localNode := cluster.NewLocalNode(cfg.Node.ID, cfg.Node.Name, 0, cfg.API.Port, nil)
	membership := cluster.NewMembership(cfg.Node.ID)
	hub := api.NewWebSocketHub()
	hub.SetTaskQueue(taskQueue)
	hub.SetMembership(membership)
	hub.SetLocalNode(localNode)
	go hub.Run()

	// 事件系统
	eventEmitter := internal.NewEventEmitter(true, 4)

	// 注册事件处理器
	// 如果配置了 webhook URL，注册 HTTPHandler
	if cfg.Events.WebhookURL != "" {
		eventEmitter.Subscribe(internal.NewHTTPHandler(cfg.Events.WebhookURL, cfg.Events.WebhookHeaders))
	}
	// 如果配置了事件命令，注册 CommandHandler
	if cfg.Events.OnCompleteCmd != "" {
		eventEmitter.Subscribe(internal.NewCommandHandler(cfg.Events.OnCompleteCmd, cfg.Events.OnCompleteArgs))
	}

	// 后处理器
	postProcessor := internal.NewPostProcessor(cfg.Download.PostProcess)

	// 下载管理器
	downloadMgr := downloader.NewDownloadManager(
		taskQueue,
		taskStore,
		hub,
		&cfg.Download,
		eventEmitter,
		postProcessor,
	)

	// 接线：任务队列回调 → 下载管理器
	taskQueue.OnTaskStart = func(t *task.Task) {
		downloadMgr.StartDownload(t)
	}

	// 启动下载管理器
	downloadMgr.Start()

	srv := api.NewServer(cfg, taskQueue, taskStore, membership, localNode, hub, Version)

	// 集群组件
	scheduler := cluster.NewScheduler(cfg.Node.ID, cfg)
	failover := cluster.NewFailover(cfg, scheduler)
	stateSync := cluster.NewStateSync(cfg.Node.ID, cfg)

	// 节点发现（gossip）
	discCfg := cluster.DiscoveryConfig{
		BindAddr:   cfg.API.Host,
		BindPort:   cfg.Cluster.DiscoveryPort,
		Seeds:      cfg.Cluster.JoinPeers,
		GossipPort: cfg.Cluster.DiscoveryPort,
	}
	discovery := cluster.NewDiscovery(discCfg, localNode, membership)

	// RPC 服务器（集群内部通信）
	rpcSrvAddr := fmt.Sprintf("%s:%d", cfg.API.Host, cfg.Cluster.GRPCPort)
	clusterAuth, err := cluster.NewClusterAuth("", "", "")
	if err != nil {
		logger.Log.Warnw("failed to create cluster auth", "error", err)
	}
	rpcServer := cluster.NewRPCServer(rpcSrvAddr, clusterAuth)
	scheduler.SetClusterAuth(clusterAuth)

	// Failover 回调：节点故障时重新分配任务
	failover.SetTaskReassignFn(func(taskID, newNodeID string) error {
		logger.Log.Infow("reassigning task", "taskID", taskID, "newNode", newNodeID)
		// TODO: 通过 gRPC 投递任务到新节点
		return nil
	})

	// NAT 穿透
	natManager := nat.NewNATManager(config.NATConfig{})

	// 插件系统
	pluginMgr := plugin.NewPluginManager()
	_ = pluginMgr

	api.RegisterGracefulShutdownHandler(func(sig syscall.Signal) error {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return srv.Shutdown(ctx)
	})

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	go func() {
		for sig := range sigCh {
			switch sig {
			case syscall.SIGHUP:
				// 热重载配置，不退出进程
				logger.Log.Info("received SIGHUP, reloading configuration")
				if newCfg, err := config.Load(cfgFile); err != nil {
					logger.Log.Errorw("failed to reload config", "error", err)
				} else {
					// 更新日志级别（最常见的热重载需求）
					if err := logger.SetLevel(newCfg.Node.LogLevel); err == nil {
						logger.Log.Info("log level updated", "level", newCfg.Node.LogLevel)
					}
					cfg = newCfg
					logger.Log.Info("configuration reloaded (log level applied; other changes require restart)")
				}
			case syscall.SIGINT, syscall.SIGTERM:
				if logger.Log != nil {
					logger.Log.Infow("收到信号，正在关闭", "signal", sig)
				}
				// 先停止 server 接收新请求，再停止下载管理器和事件系统
				_ = srv.Stop()
				downloadMgr.Stop()
				_ = eventEmitter.Close()
				// 停止集群组件
				rpcServer.Shutdown()
				discovery.Shutdown()
				stateSync.Stop()
				failover.Stop()
				scheduler.Stop()
				_ = natManager.Close()
				return
			}
		}
	}()

	logger.Log.Infow("启动 API 服务器", "addr", srv.Addr)
	fmt.Printf("RPC  地址: http://%s/jsonrpc\n", srv.Addr)
	fmt.Printf("XML-RPC 地址: http://%s/xmlrpc\n", srv.Addr)
	fmt.Println(strings.Repeat("=", 50))

	// 启动集群组件
	scheduler.Start()
	failover.Start()
	stateSync.Start()
	if err := discovery.Start(); err != nil {
		logger.Log.Warnw("failed to start discovery", "error", err)
	}
	if err := rpcServer.Start(); err != nil {
		logger.Log.Warnw("failed to start RPC server", "error", err)
	}

	if err := srv.Start(); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("服务器错误: %w", err)
	}

	return nil
}

func runAdd(url string) error {
	client := newRPCClient(rpcAddr, rpcToken)
	result, err := client.call("aria2.addUri", []any{[]string{url}})
	if err != nil {
		return err
	}
	var gids []string
	if err := json.Unmarshal(result, &gids); err != nil {
		var gid string
		if err2 := json.Unmarshal(result, &gid); err2 != nil {
			return fmt.Errorf("意外的响应: %s", string(result))
		}
		fmt.Printf("已添加任务: %s\n", gid)
		return nil
	}
	for _, gid := range gids {
		fmt.Printf("已添加任务: %s\n", gid)
	}
	return nil
}

func runList() error {
	client := newRPCClient(rpcAddr, rpcToken)
	fmt.Printf("%-36s %-12s %-20s %s\n", "GID", "状态", "进度", "速度")
	fmt.Println(strings.Repeat("-", 80))

	for _, method := range []string{"aria2.tellActive", "aria2.tellWaiting", "aria2.tellStopped"} {
		var params []any
		if method == "aria2.tellWaiting" || method == "aria2.tellStopped" {
			params = []any{0, 1000}
		}
		result, err := client.call(method, params)
		if err != nil {
			return err
		}
		var tasks []map[string]any
		if err := json.Unmarshal(result, &tasks); err != nil {
			continue
		}
		for _, t := range tasks {
			gid, _ := t["gid"].(string)
			status, _ := t["status"].(string)
			completed, _ := t["completedLength"].(string)
			total, _ := t["totalLength"].(string)
			speed, _ := t["downloadSpeed"].(string)
			progress := completed + "/" + total
			fmt.Printf("%-36s %-12s %-20s %s B/s\n", gid, status, progress, speed)
		}
	}
	return nil
}

func runPause(gid string) error {
	client := newRPCClient(rpcAddr, rpcToken)
	if _, err := client.call("aria2.pause", []any{gid}); err != nil {
		return err
	}
	fmt.Printf("已暂停任务: %s\n", gid)
	return nil
}

func runResume(gid string) error {
	client := newRPCClient(rpcAddr, rpcToken)
	if _, err := client.call("aria2.unpause", []any{gid}); err != nil {
		return err
	}
	fmt.Printf("已恢复任务: %s\n", gid)
	return nil
}

func runRemove(gid string) error {
	client := newRPCClient(rpcAddr, rpcToken)
	if _, err := client.call("aria2.remove", []any{gid}); err != nil {
		return err
	}
	fmt.Printf("已删除任务: %s\n", gid)
	return nil
}

func runStatus() error {
	client := newRPCClient(rpcAddr, rpcToken)
	result, err := client.call("aria2.getGlobalStat", []any{})
	if err != nil {
		return err
	}
	var stat map[string]any
	if err := json.Unmarshal(result, &stat); err != nil {
		return fmt.Errorf("意外的响应: %s", string(result))
	}
	fmt.Println("=== 全局状态 ===")
	fmt.Printf("下载速度: %s B/s\n", toString(stat["downloadSpeed"]))
	fmt.Printf("上传速度: %s B/s\n", toString(stat["uploadSpeed"]))
	fmt.Printf("活动任务: %s\n", toString(stat["numActive"]))
	fmt.Printf("等待任务: %s\n", toString(stat["numWaiting"]))
	fmt.Printf("已完成:   %s\n", toString(stat["numStopped"]))
	return nil
}

func toString(v any) string {
	if v == nil {
		return ""
	}
	switch x := v.(type) {
	case string:
		return x
	case float64:
		return fmt.Sprintf("%d", int64(x))
	default:
		return fmt.Sprintf("%v", v)
	}
}

func init() {
	rootCmd.PersistentFlags().StringVarP(&cfgFile, "config", "c", "", "配置文件路径")
	rootCmd.PersistentFlags().StringVar(&rpcAddr, "addr", "http://localhost:6800/jsonrpc", "RPC 服务器地址")
	rootCmd.PersistentFlags().StringVar(&rpcToken, "token", "", "RPC 认证 token")

	rootCmd.AddCommand(serveCmd)
	rootCmd.AddCommand(addCmd)
	rootCmd.AddCommand(listCmd)
	rootCmd.AddCommand(pauseCmd)
	rootCmd.AddCommand(resumeCmd)
	rootCmd.AddCommand(removeCmd)
	rootCmd.AddCommand(statusCmd)
	rootCmd.AddCommand(downloadCmd)

	downloadCmd.Flags().IntVarP(&parallel, "split", "s", 0, "下载线程数")
	downloadCmd.Flags().StringVarP(&output, "output", "o", "", "输出文件路径")
	// 标记 -o 是否被显式指定（用于 Content-Disposition 重命名决策）
	downloadCmd.Flags().Lookup("output").NoOptDefVal = "" // 确保 -o 需要参数
	downloadCmd.PreRun = func(cmd *cobra.Command, args []string) {
		explicitOutput = cmd.Flags().Changed("output")
	}
	downloadCmd.Flags().StringVar(&speedLimit, "speed-limit", "", "速度限制 (例如: 1M, 500K)")
	downloadCmd.Flags().IntVar(&timeout, "timeout", 0, "超时时间 (秒)")
	downloadCmd.Flags().StringVarP(&inputFile, "input-file", "i", "", "批量下载文件 (每行一个URL)")
	downloadCmd.Flags().StringVarP(&dir, "dir", "d", "", "下载保存目录")
	downloadCmd.Flags().BoolVar(&adaptive, "adaptive", false, "自适应线程数 (根据网络状况自动调整)")
	downloadCmd.Flags().BoolVarP(&insecure, "insecure", "k", false, "跳过TLS证书验证")
	downloadCmd.Flags().BoolVarP(&noNetrc, "no-netrc", "n", false, "禁用 netrc 凭证读取")
	downloadCmd.Flags().StringVar(&streamPieceSelector, "stream-piece-selector", "", "分片选择策略: inorder|geom|random (默认 inorder)")
	downloadCmd.Flags().StringVar(&uriSelector, "uri-selector", "", "URI 选择器: inorder|feedback|adaptive (默认 feedback)")

	// wget/curl 兼容 flag
	downloadCmd.Flags().StringVarP(&userAgent, "user-agent", "U", "", "自定义 User-Agent (对标 wget -U / curl -A)")
	downloadCmd.Flags().StringArrayVarP(&headers, "header", "H", nil, "自定义请求头 (格式: 'Key: Value'，可多次使用，对标 curl -H)")
	downloadCmd.Flags().StringVarP(&referer, "referer", "e", "", "设置 Referer 头 (对标 curl -e / wget --referer)")
	downloadCmd.Flags().StringVar(&httpUser, "http-user", "", "HTTP 认证用户名 (对标 wget --http-user / curl -u)")
	downloadCmd.Flags().StringVar(&httpPassword, "http-password", "", "HTTP 认证密码 (对标 wget --http-password)")
	downloadCmd.Flags().BoolVar(&spider, "spider", false, "只检查不下载 (对标 wget --spider)")
	downloadCmd.Flags().BoolVarP(&serverResponse, "server-response", "S", false, "打印服务器响应头 (对标 wget -S)")
	downloadCmd.Flags().BoolVar(&noContentDisposition, "no-content-disposition", false, "禁用 Content-Disposition 自动重命名")
	downloadCmd.Flags().BoolVar(&remoteTime, "remote-time", false, "使用服务器时间设置本地文件时间 (对标 wget --remote-time)")
	downloadCmd.Flags().IntVarP(&maxTime, "max-time", "m", 0, "最大下载时长 (秒)，超时取消 (对标 curl -m)")

	serveCmd.Flags().BoolVarP(&daemon, "daemon", "D", false, "以守护进程方式运行 (仅 Unix)")

	rootCmd.PersistentFlags().BoolVarP(&quiet, "quiet", "q", false, "安静模式 (仅输出错误日志)")
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		// 用户中断（SIGINT/SIGTERM）返回 130（128+SIGINT），与 wget 一致
		if errors.Is(err, errInterrupted) {
			os.Exit(130)
		}
		os.Exit(1)
	}
}
