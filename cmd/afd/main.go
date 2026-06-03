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

Quick usage:
  afd http://example.com/file.zip           # Direct download
  afd -o file.zip http://example.com/file   # Specify output
  afd -s 4 http://example.com/file          # 4 threads
  afd -i urls.txt                            # Batch download`,
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		if cfgFile != "" {
			config.Load(cfgFile)
		}
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) > 0 {
			return doDownload(args[0], output)
		}
		return cmd.Help()
	},
}

var serveCmd = &cobra.Command{
	Use:   "serve",
	Short: "Start AFD service",
	RunE: func(cmd *cobra.Command, args []string) error {
		printBanner()
		fmt.Println("Service mode under development, use direct download")
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
)

var downloadCmd = &cobra.Command{
	Use:   "dl <url>",
	Short: "Download file",
	Aliases: []string{"download"},
	Args:    cobra.RangeArgs(0, 2),
	RunE: func(cmd *cobra.Command, args []string) error {
		if inputFile != "" {
			return doBatchDownload(inputFile)
		}
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
			return fmt.Errorf("please specify output path: -o <path>")
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

	logger.Init("info", "")
	defer logger.Log.Sync()

	log := logger.Log.Named("download")

	fmt.Printf("Starting download: %s\n", url)
	fmt.Printf("Save to: %s\n", outputPath)
	if speedLimit != "" {
		fmt.Printf("Speed limit: %s\n", speedLimit)
	}
	if parallel > 0 {
		fmt.Printf("Connections: %d\n", parallel)
	}

	d, err := downloader.NewDownloaderFromURL(url, outputPath, cfg, log)
	if err != nil {
		return fmt.Errorf("failed to create downloader: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		fmt.Println("\nStopping download...")
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
					fmt.Printf("\rProgress: %.1f%% | Speed: %s/s | Downloaded: %s | ETA: %s",
						progress, formatBytes(speed), formatBytes(downloaded), eta.Round(time.Second))
				} else {
					fmt.Printf("\rProgress: %.1f%% | Downloaded: %s", progress, formatBytes(downloaded))
				}
			}
		}
	}()

	err = d.Download(ctx)

	fmt.Println()
	if err != nil {
		if ctx.Err() != nil {
			fmt.Println("Download cancelled")
			return nil
		}
		return fmt.Errorf("download failed: %w", err)
	}

	elapsed := time.Since(startTime)
	fmt.Printf("Download complete! Time: %s, Avg speed: %s/s\n",
		elapsed.Round(time.Second),
		formatBytes(int64(float64(d.TotalDownloaded())/elapsed.Seconds())))

	return nil
}

func doBatchDownload(inputFile string) error {
	data, err := os.ReadFile(inputFile)
	if err != nil {
		return fmt.Errorf("failed to read file: %w", err)
	}

	lines := strings.Split(string(data), "\n")
	urls := []string{}
	currentDir := dir

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		if strings.HasPrefix(line, "dir=") {
			currentDir = strings.TrimPrefix(line, "dir=")
			continue
		}
		if strings.HasPrefix(line, "out=") {
			continue
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
		return fmt.Errorf("no valid URLs found in file")
	}

	fmt.Printf("Batch download: found %d tasks\n", len(urls))
	if currentDir != "" {
		fmt.Printf("Save directory: %s\n", currentDir)
	}

	success, failed := 0, 0

	for i, url := range urls {
		fmt.Printf("\n[%d/%d] %s\n", i+1, len(urls), url)

		outPath := filepath.Base(url)
		if currentDir != "" {
			outPath = filepath.Join(currentDir, outPath)
		}

		if err := doDownload(url, outPath); err != nil {
			fmt.Printf("Failed: %v\n", err)
			failed++
		} else {
			success++
		}
	}

	fmt.Printf("\n========== Complete ==========\n")
	fmt.Printf("Success: %d, Failed: %d\n", success, failed)

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
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "Config file path")

	rootCmd.AddCommand(serveCmd)
	rootCmd.AddCommand(downloadCmd)

	downloadCmd.Flags().IntVarP(¶llel, "split", "s", 0, "Download threads")
	downloadCmd.Flags().StringVarP(&output, "output", "o", "", "Output file path")
	downloadCmd.Flags().StringVar(&speedLimit, "speed-limit", "", "Speed limit (e.g. 1M, 500K)")
	downloadCmd.Flags().IntVar(&timeout, "timeout", 0, "Timeout (seconds)")
	downloadCmd.Flags().StringVarP(&inputFile, "input-file", "i", "", "Batch download file")
	downloadCmd.Flags().StringVarP(&dir, "dir", "d", "", "Download directory")
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}