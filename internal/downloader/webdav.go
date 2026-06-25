package downloader

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/nexus-dl/afd/pkg/config"
	"go.uber.org/zap"
	"golang.org/x/net/webdav"
)

type WebDAVDownloader struct {
	client     *http.Client
	url        string
	outputPath string
	cfg        *config.DownloadConfig
	logger     *zap.SugaredLogger

	host string
	path string

	user     string
	password string

	fileSize        int64
	totalDownloaded int64
	startTime       time.Time

	controlFilePath string
	controlFile     interface{}
}

func NewWebDAVDownloader(urlStr, outputPath string, cfg *config.DownloadConfig, logger *zap.SugaredLogger) (*WebDAVDownloader, error) {
	if cfg == nil {
		cfg = config.DefaultDownloadConfig()
	}

	host, path, err := ParseWebDAVURL(urlStr)
	if err != nil {
		return nil, fmt.Errorf("parse WebDAV URL: %w", err)
	}

	transport := &http.Transport{
		MaxIdleConns:        10,
		IdleConnTimeout:     30 * time.Second,
		TLSHandshakeTimeout: 10 * time.Second,
	}

	client := &http.Client{
		Transport: transport,
		Timeout:   cfg.Timeout,
	}

	parsedURL, _ := url.Parse(urlStr)
	user := ""
	password := ""

	if parsedURL != nil {
		user = parsedURL.User.Username()
		if pw, ok := parsedURL.User.Password(); ok {
			password = pw
		}
	}

	return &WebDAVDownloader{
		client:     client,
		url:        urlStr,
		outputPath: outputPath,
		cfg:        cfg,
		logger:     logger,
		host:       host,
		path:       path,
		user:       user,
		password:   password,
		startTime:  time.Now(),
	}, nil
}

func ParseWebDAVURL(urlStr string) (host, path string, err error) {
	if !strings.HasPrefix(urlStr, "webdav://") && !strings.HasPrefix(urlStr, "webdavs://") {
		return "", "", fmt.Errorf("not a WebDAV URL: %s", urlStr)
	}

	scheme := "http"
	if strings.HasPrefix(urlStr, "webdavs://") {
		scheme = "https"
		urlStr = strings.TrimPrefix(urlStr, "webdavs://")
	} else {
		urlStr = strings.TrimPrefix(urlStr, "webdav://")
	}

	slashIdx := strings.Index(urlStr, "/")
	if slashIdx == -1 {
		host = urlStr
		path = "/"
	} else {
		host = urlStr[:slashIdx]
		path = urlStr[slashIdx:]
	}

	if !strings.Contains(host, ":") {
		if scheme == "https" {
			host = host + ":443"
		} else {
			host = host + ":80"
		}
	}

	if path == "" {
		path = "/"
	}

	return scheme + "://" + host + path, path, nil
}

func IsWebDAVURL(url string) bool {
	return strings.HasPrefix(url, "webdav://") || strings.HasPrefix(url, "webdavs://")
}

func (d *WebDAVDownloader) SetURL(urlStr string) {
	d.url = urlStr
	host, path, _ := ParseWebDAVURL(urlStr)
	d.host = host
	d.path = path

	parsedURL, _ := url.Parse(urlStr)
	if parsedURL != nil {
		d.user = parsedURL.User.Username()
		if pw, ok := parsedURL.User.Password(); ok {
			d.password = pw
		}
	}
}

func (d *WebDAVDownloader) SetOutputPath(path string) {
	d.outputPath = path
}

func (d *WebDAVDownloader) SetControlFilePath(path string) {
	d.controlFilePath = path
}

func (d *WebDAVDownloader) SetControlFile(cf interface{}) {
	d.controlFile = cf
}

func (d *WebDAVDownloader) URL() string {
	return d.url
}

func (d *WebDAVDownloader) OutputPath() string {
	return d.outputPath
}

func (d *WebDAVDownloader) FileSize() int64 {
	return d.fileSize
}

func (d *WebDAVDownloader) GetFileSize(ctx context.Context) (int64, error) {
	return GetWebDAVFileSize(ctx, d.client, d.url, d.user, d.password)
}

func GetWebDAVFileSize(ctx context.Context, client *http.Client, urlStr, user, password string) (int64, error) {
	host, _, err := ParseWebDAVURL(urlStr)
	if err != nil {
		return 0, fmt.Errorf("parse WebDAV URL: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "PROPFIND", host, nil)
	if err != nil {
		return 0, fmt.Errorf("create PROPFIND request: %w", err)
	}

	req.Header.Set("Depth", "0")
	req.Header.Set("Content-Type", "application/xml")

	if user != "" {
		setWebDAVAuth(req, user, password)
	}

	resp, err := client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("PROPFIND request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusUnauthorized {
		if user == "" || password == "" {
			return 0, fmt.Errorf("WebDAV server requires authentication")
		}
		return 0, fmt.Errorf("WebDAV authentication failed")
	}

	if resp.StatusCode != http.StatusMultiStatus && resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("PROPFIND returned status: %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, fmt.Errorf("read response body: %w", err)
	}

	size, err := parseWebDAVPropfindResponse(string(body))
	if err != nil {
		return 0, fmt.Errorf("parse PROPFIND response: %w", err)
	}

	return size, nil
}

func setWebDAVAuth(req *http.Request, user, password string) {
	if user == "" {
		return
	}

	auth := base64.StdEncoding.EncodeToString([]byte(user + ":" + password))
	req.Header.Set("Authorization", "Basic "+auth)
}

func parseWebDAVPropfindResponse(body string) (int64, error) {
	start := strings.Index(body, "<D:getcontentlength>")
	if start == -1 {
		start = strings.Index(body, "<d:getcontentlength>")
	}

	if start == -1 {
		return 0, fmt.Errorf("content length not found in response")
	}

	start += len("<D:getcontentlength>")
	end := strings.Index(body[start:], "<")
	if end == -1 {
		return 0, fmt.Errorf("invalid XML response")
	}

	sizeStr := strings.TrimSpace(body[start : start+end])

	var size int64
	for _, c := range sizeStr {
		if c < '0' || c > '9' {
			return 0, fmt.Errorf("invalid size value: %s", sizeStr)
		}
		size = size*10 + int64(c-'0')
	}

	return size, nil
}

func (d *WebDAVDownloader) Download(ctx context.Context) error {
	d.startTime = time.Now()

	fileSize, err := d.GetFileSize(ctx)
	if err != nil {
		return fmt.Errorf("get file size: %w", err)
	}
	d.fileSize = fileSize

	d.logger.Infow("starting WebDAV download",
		"host", d.host,
		"path", d.path,
		"file_size", fileSize,
		"url", d.url,
	)

	existingSize := int64(0)
	if stat, err := os.Stat(d.outputPath); err == nil {
		existingSize = stat.Size()
	}

	if existingSize == fileSize && fileSize > 0 {
		d.logger.Infow("file already fully downloaded, skipping")
		atomic.StoreInt64(&d.totalDownloaded, fileSize)
		return nil
	}

	if err := os.MkdirAll(filepath.Dir(d.outputPath), 0755); err != nil {
		return fmt.Errorf("create output directory: %w", err)
	}

	var file *os.File
	if existingSize > 0 {
		file, err = os.OpenFile(d.outputPath, os.O_CREATE|os.O_RDWR, 0644)
		if err != nil {
			return fmt.Errorf("open output file: %w", err)
		}
	} else {
		file, err = os.Create(d.outputPath)
		if err != nil {
			return fmt.Errorf("create output file: %w", err)
		}
	}
	defer file.Close()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, d.host, nil)
	if err != nil {
		return fmt.Errorf("create GET request: %w", err)
	}

	if d.user != "" {
		setWebDAVAuth(req, d.user, d.password)
	}

	if existingSize > 0 && existingSize < fileSize {
		d.logger.Infow("resuming WebDAV download",
			"existing_size", existingSize,
			"total_size", fileSize,
		)
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-", existingSize))
		atomic.StoreInt64(&d.totalDownloaded, existingSize)
	}

	resp, err := d.client.Do(req)
	if err != nil {
		return fmt.Errorf("GET request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusUnauthorized {
		return fmt.Errorf("WebDAV authentication failed")
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		return fmt.Errorf("GET returned status: %d", resp.StatusCode)
	}

	if existingSize > 0 && resp.StatusCode == http.StatusOK {
		if err := file.Truncate(0); err != nil {
			return fmt.Errorf("truncate file: %w", err)
		}
		existingSize = 0
		atomic.StoreInt64(&d.totalDownloaded, 0)
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
			_, writeErr := file.Write(buf[:n])
			if writeErr != nil {
				return fmt.Errorf("write file: %w", writeErr)
			}

			atomic.AddInt64(&d.totalDownloaded, int64(n))
		}

		if readErr != nil {
			if readErr == io.EOF {
				break
			}
			return fmt.Errorf("read response: %w", readErr)
		}
	}

	d.logger.Infow("WebDAV download completed",
		"host", d.host,
		"path", d.path,
		"total_bytes", atomic.LoadInt64(&d.totalDownloaded),
		"duration", time.Since(d.startTime),
	)

	return nil
}

func (d *WebDAVDownloader) Speed() int64 {
	if d.startTime.IsZero() {
		return 0
	}

	elapsed := time.Since(d.startTime).Seconds()
	if elapsed <= 0 {
		return 0
	}

	downloaded := atomic.LoadInt64(&d.totalDownloaded)
	return int64(float64(downloaded) / elapsed)
}

func (d *WebDAVDownloader) Progress() float64 {
	if d.fileSize <= 0 {
		return 0
	}
	downloaded := atomic.LoadInt64(&d.totalDownloaded)
	return float64(downloaded) / float64(d.fileSize) * 100
}

func (d *WebDAVDownloader) TotalDownloaded() int64 {
	return atomic.LoadInt64(&d.totalDownloaded)
}

func (d *WebDAVDownloader) ActiveThreads() int32 {
	return 1
}

func (d *WebDAVDownloader) SetRateLimit(rate int64) {
}

func (d *WebDAVDownloader) GetRateLimit() int64 {
	return 0
}

func (d *WebDAVDownloader) SetRetryConfig(cfg RetryConfig) {
}

func (d *WebDAVDownloader) GetRetryConfig() RetryConfig {
	return DefaultRetryConfig()
}

func (d *WebDAVDownloader) LoadProgress(ctx context.Context) error {
	return nil
}

func (d *WebDAVDownloader) SaveProgress() error {
	return nil
}

type WebDAVProtocolHandler struct {
	logger *zap.SugaredLogger
	cfg    *config.DownloadConfig
}

func NewWebDAVProtocolHandler(logger *zap.SugaredLogger, cfg *config.DownloadConfig) *WebDAVProtocolHandler {
	if cfg == nil {
		cfg = config.DefaultDownloadConfig()
	}
	return &WebDAVProtocolHandler{
		logger: logger,
		cfg:    cfg,
	}
}

func (h *WebDAVProtocolHandler) CanHandle(url string) bool {
	return IsWebDAVURL(url)
}

func (h *WebDAVProtocolHandler) NewDownloader(url, outputPath string) (DownloaderInterface, error) {
	return NewWebDAVDownloader(url, outputPath, h.cfg, h.logger)
}

func (h *WebDAVProtocolHandler) GetFileInfo(urlStr string) (int64, error) {
	transport := &http.Transport{
		MaxIdleConns:        10,
		IdleConnTimeout:     30 * time.Second,
		TLSHandshakeTimeout: 10 * time.Second,
	}

	client := &http.Client{
		Transport: transport,
		Timeout:   h.cfg.Timeout,
	}

	parsedURL, _ := url.Parse(urlStr)
	user := ""
	password := ""

	if parsedURL != nil {
		user = parsedURL.User.Username()
		if pw, ok := parsedURL.User.Password(); ok {
			password = pw
		}
	}

	// GetFileInfo 没有 context 参数，这里为 PROPFIND 网络请求添加超时，
	// 避免在异常 WebDAV 端点上长时间阻塞。
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return GetWebDAVFileSize(ctx, client, urlStr, user, password)
}

func NewWebDAVFileServer() *webdav.FileSystem {
	return nil
}
