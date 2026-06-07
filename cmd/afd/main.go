package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/nexus-dl/afd/internal/downloader"
	"github.com/nexus-dl/afd/pkg/config"
	"github.com/nexus-dl/afd/pkg/logger"
	"github.com/spf13/cobra"
)

var (
	Version   = "dev"
	Commit    = "unknown"
	BuildTime = "unknown"
	cfgFile   string
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
			config.Load(cfgFile)
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
		printBanner()
		fmt.Println("服务功能开发中，请使用 download 命令直接下载")
		return nil
	},
}

var addCmd = &cobra.Command{
	Use:   "add <url>",
	Short: "添加下载任务 (需要先启动服务)",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		fmt.Println("请先使用 'afd serve' 启动服务，然后通过 API 添加任务")
		return nil
	},
}

var listCmd = &cobra.Command{
	Use:   "list",
	Short: "列出所有任务 (需要先启动服务)",
	RunE: func(cmd *cobra.Command, args []string) error {
		fmt.Println("请先使用 'afd serve' 启动服务")
		return nil
	},
}

var pauseCmd = &cobra.Command{
	Use:   "pause <task-id>",
	Short: "暂停任务 (需要先启动服务)",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		fmt.Println("请先使用 'afd serve' 启动服务")
		return nil
	},
}

var resumeCmd = &cobra.Command{
	Use:   "resume <task-id>",
	Short: "恢复任务 (需要先启动服务)",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		fmt.Println("请先使用 'afd serve' 启动服务")
		return nil
	},
}

var removeCmd = &cobra.Command{
	Use:   "remove <task-id>",
	Short: "删除任务 (需要先启动服务)",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		fmt.Println("请先使用 'afd serve' 启动服务")
		return nil
	},
}

var statusCmd = &cobra.Command{
	Use:   "status",
	Short: "查看集群状态 (需要先启动服务)",
	RunE: func(cmd *cobra.Command, args []string) error {
		fmt.Println("请先使用 'afd serve' 启动服务")
		return nil
	},
}

var (
	parallel    int
	output      string
	speedLimit  string
	timeout     int
	inputFile   string
	dir         string
	allTorrrent bool
	adaptive    bool
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
		fmt.Sscanf(speedLimit, "%d", &limit)
		if strings.HasSuffix(speedLimit, "M") {
			limit *= 1024 * 1024
		} else if strings.HasSuffix(speedLimit, "K") {
			limit *= 1024
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

	logger.Init("info", "")
	defer logger.Log.Sync()

	log := logger.Log.Named("download")

	fmt.Printf("开始下载: %s\n", url)
	fmt.Printf("保存到: %s\n", outputPath)
	if speedLimit != "" {
		fmt.Printf("速度限制: %s\n", speedLimit)
	}
	if parallel > 0 {
		fmt.Printf("连接数: %d\n", parallel)
	}
	if adaptive {
		fmt.Printf("自适应模式: 启用\n")
	}

	d, err := downloader.NewDownloaderFromURL(url, outputPath, cfg, log)
	if err != nil {
		return fmt.Errorf("创建下载器失败: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		fmt.Println("\n正在停止下载...")
		cancel()
	}()

	startTime := time.Now()

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(500 * time.Millisecond):
				progress := d.Progress()
				speed := d.Speed()
				downloaded := d.TotalDownloaded()

				if speed > 0 {
					remaining := float64(downloaded) / float64(speed)
					eta := time.Duration(remaining) * time.Second
					fmt.Printf("\r进度: %.1f%% | 速度: %s/s | 已下载: %s | 预计剩余: %s",
						progress, formatBytes(speed), formatBytes(downloaded), eta.Round(time.Second))
				} else {
					fmt.Printf("\r进度: %.1f%% | 已下载: %s", progress, formatBytes(downloaded))
				}
			}
		}
	}()

	err = d.Download(ctx)

	fmt.Println()
	if err != nil {
		if ctx.Err() != nil {
			fmt.Println("下载已取消")
			return nil
		}
		return fmt.Errorf("下载失败: %w", err)
	}

	elapsed := time.Since(startTime)
	fmt.Printf("下载完成! 耗时: %s, 平均速度: %s/s\n",
		elapsed.Round(time.Second),
		formatBytes(int64(float64(d.TotalDownloaded())/elapsed.Seconds())))

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
	for n >= unit*1024 {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(n)/float64(div), "KMGTPE"[exp])
}

func init() {
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "配置文件路径")

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
	downloadCmd.Flags().BoolVar(&allTorrrent, "all", false, "下载所有torrent文件中的内容")
	downloadCmd.Flags().BoolVar(&adaptive, "adaptive", false, "自适应线程数 (根据网络状况自动调整)")
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
