package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/nexus-dl/afd/internal/api"
	"github.com/nexus-dl/afd/internal/cluster"
	"github.com/nexus-dl/afd/internal/downloader"
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
			return doDownload(args[0], output)
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
	parallel   int
	output     string
	speedLimit string
	timeout    int
	inputFile  string
	dir        string
	adaptive   bool
	insecure   bool
)

var downloadCmd = &cobra.Command{
	Use:   "dl <url>",
	Short: "下载文件 (download 的别名)",
	Long: `直接下载文件，无需启动服务。

示例:
  afd dl http://example.com/file.zip
  afd dl -o /tmp/file.zip http://example.com/file.zip
  afd dl -s 4 http://example.com/file.zip
  afd dl -i urls.txt                    # 批量下载`,
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

		url := args[0]
		outPath := output

		if outPath == "" && len(args) > 1 {
			outPath = args[1]
		}

		if outPath == "" && dir != "" {
			outPath = filepath.Join(dir, filepath.Base(url))
		}

		if outPath == "" {
			outPath = filepath.Base(url)
		}

		if outPath == "" || strings.HasPrefix(outPath, "-") {
			return fmt.Errorf("请指定输出文件路径: -o <path>")
		}

		return doDownload(url, outPath)
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

func doDownload(url, outputPath string) error {
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

	logger.Init("info", "")
	defer logger.Log.Sync()

	log := logger.Log.Named("download")

	log.Infow("starting download", "url", url, "output", outputPath,
		"speed_limit", speedLimit, "parallel", parallel, "adaptive", adaptive, "insecure", insecure)

	d, err := downloader.NewDownloaderFromURL(url, outputPath, cfg, log)
	if err != nil {
		return fmt.Errorf("创建下载器失败: %w", err)
	}

	// 设置控制文件路径，支持断点续传
	d.SetControlFilePath(outputPath + ".ctl")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		log.Infow("received signal, stopping download", "signal", sig)
		cancel()
	}()

	startTime := time.Now()

	ticker := time.NewTicker(2 * time.Second)
	go func() {
		defer ticker.Stop()
		var lastLoggedPct int
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				progress := d.Progress()
				speed := d.Speed()
				downloaded := d.TotalDownloaded()
				fileSize := d.FileSize()
				pct := int(progress)

				// 每 10% 或速度变化时输出一次
				if pct != lastLoggedPct || speed > 0 {
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
			return nil
		}
		signal.Stop(sigCh)
		cancel()
		return fmt.Errorf("下载失败: %w", err)
	}
	signal.Stop(sigCh)
	cancel()

	elapsed := time.Since(startTime)
	fileSize := d.FileSize()
	var avgSpeed int64
	if elapsed.Seconds() > 0 {
		avgSpeed = int64(float64(fileSize) / elapsed.Seconds())
	}
	log.Infow("download finished",
		"elapsed", elapsed.Round(time.Second).String(),
		"file_size", formatBytes(fileSize),
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
	currentDir := dir

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
			continue // TODO: 处理输出文件名
		}
		if strings.HasPrefix(line, "http://") ||
			strings.HasPrefix(line, "https://") ||
			strings.HasPrefix(line, "ftp://") ||
			strings.HasPrefix(line, "magnet:") ||
			strings.HasPrefix(line, "file://") {
			urls = append(urls, line)
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
		if currentDir != "" {
			outPath = filepath.Join(currentDir, outPath)
		}

		if err := doDownload(url, outPath); err != nil {
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

// --- RPC client and command implementations ---

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type rpcResponse struct {
	Result json.RawMessage `json:"result"`
	Error  *rpcError       `json:"error"`
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

func (c *rpcClient) call(method string, params []interface{}) (json.RawMessage, error) {
	reqBody := map[string]interface{}{
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

func runServe() error {
	printBanner()

	cfg, err := config.Load(cfgFile)
	if err != nil {
		return fmt.Errorf("加载配置失败: %w", err)
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

	srv := api.NewServer(cfg, taskQueue, taskStore, membership, localNode, hub, Version)

	api.RegisterGracefulShutdownHandler(func(sig syscall.Signal) error {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return srv.Shutdown(ctx)
	})

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		if logger.Log != nil {
			logger.Log.Infow("收到信号，正在关闭", "signal", sig)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	}()

	logger.Log.Infow("启动 API 服务器", "addr", srv.Addr)
	fmt.Printf("RPC  地址: http://%s/jsonrpc\n", srv.Addr)
	fmt.Printf("XML-RPC 地址: http://%s/xmlrpc\n", srv.Addr)
	fmt.Println(strings.Repeat("=", 50))

	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("服务器错误: %w", err)
	}

	return nil
}

func runAdd(url string) error {
	client := newRPCClient(rpcAddr, rpcToken)
	result, err := client.call("aria2.addUri", []interface{}{[]string{url}})
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
		var params []interface{}
		if method == "aria2.tellWaiting" || method == "aria2.tellStopped" {
			params = []interface{}{0, 1000}
		}
		result, err := client.call(method, params)
		if err != nil {
			return err
		}
		var tasks []map[string]interface{}
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
	if _, err := client.call("aria2.pause", []interface{}{gid}); err != nil {
		return err
	}
	fmt.Printf("已暂停任务: %s\n", gid)
	return nil
}

func runResume(gid string) error {
	client := newRPCClient(rpcAddr, rpcToken)
	if _, err := client.call("aria2.unpause", []interface{}{gid}); err != nil {
		return err
	}
	fmt.Printf("已恢复任务: %s\n", gid)
	return nil
}

func runRemove(gid string) error {
	client := newRPCClient(rpcAddr, rpcToken)
	if _, err := client.call("aria2.remove", []interface{}{gid}); err != nil {
		return err
	}
	fmt.Printf("已删除任务: %s\n", gid)
	return nil
}

func runStatus() error {
	client := newRPCClient(rpcAddr, rpcToken)
	result, err := client.call("aria2.getGlobalStat", []interface{}{})
	if err != nil {
		return err
	}
	var stat map[string]interface{}
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

func toString(v interface{}) string {
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
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "配置文件路径")
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
	downloadCmd.Flags().StringVar(&speedLimit, "speed-limit", "", "速度限制 (例如: 1M, 500K)")
	downloadCmd.Flags().IntVar(&timeout, "timeout", 0, "超时时间 (秒)")
	downloadCmd.Flags().StringVarP(&inputFile, "input-file", "i", "", "批量下载文件 (每行一个URL)")
	downloadCmd.Flags().StringVarP(&dir, "dir", "d", "", "下载保存目录")
	downloadCmd.Flags().BoolVar(&adaptive, "adaptive", false, "自适应线程数 (根据网络状况自动调整)")
	downloadCmd.Flags().BoolVarP(&insecure, "insecure", "k", false, "跳过TLS证书验证")
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
