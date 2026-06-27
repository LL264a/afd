package downloader

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/nexus-dl/afd/pkg/config"
	"go.uber.org/zap"
)

type S3Downloader struct {
	client     *s3.Client
	url        string
	outputPath string
	cfg        *config.DownloadConfig
	logger     *zap.SugaredLogger

	bucket string
	key    string

	fileSize        int64
	totalDownloaded int64
	speed           int64
	lastSpeedCheck  time.Time
	lastSpeedBytes  int64

	controlFilePath string
	controlFile     interface{}
}

func NewS3Downloader(url, outputPath string, cfg *config.DownloadConfig, logger *zap.SugaredLogger) (*S3Downloader, error) {
	if cfg == nil {
		cfg = config.DefaultDownloadConfig()
	}

	bucket, key, err := ParseS3URL(url)
	if err != nil {
		return nil, fmt.Errorf("parse S3 URL: %w", err)
	}

	awsCfg, err := loadAWSConfig()
	if err != nil {
		return nil, fmt.Errorf("load AWS config: %w", err)
	}

	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.UsePathStyle = true
	})

	return &S3Downloader{
		client:         client,
		url:            url,
		outputPath:     outputPath,
		cfg:            cfg,
		logger:         logger,
		bucket:         bucket,
		key:            key,
		lastSpeedCheck: time.Now(),
	}, nil
}

func loadAWSConfig() (aws.Config, error) {
	region := os.Getenv("AWS_REGION")
	if region == "" {
		region = "us-east-1"
	}

	accessKeyID := os.Getenv("AWS_ACCESS_KEY_ID")
	secretAccessKey := os.Getenv("AWS_SECRET_ACCESS_KEY")

	opts := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRegion(region),
	}

	if accessKeyID != "" && secretAccessKey != "" {
		opts = append(opts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(accessKeyID, secretAccessKey, ""),
		))
	}

	// LoadDefaultConfig 可能触发 IMDS 元数据请求（网络调用），添加超时以避免
	// 在无凭证环境或网络异常时长时间阻塞。
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return awsconfig.LoadDefaultConfig(ctx, opts...)
}

func ParseS3URL(url string) (bucket, key string, err error) {
	if !strings.HasPrefix(url, "s3://") {
		return "", "", fmt.Errorf("not an S3 URL: %s", url)
	}

	url = strings.TrimPrefix(url, "s3://")

	slashIdx := strings.Index(url, "/")
	if slashIdx == -1 {
		return "", "", fmt.Errorf("invalid S3 URL format: %s", url)
	}

	bucket = url[:slashIdx]
	key = url[slashIdx+1:]

	if bucket == "" {
		return "", "", fmt.Errorf("bucket cannot be empty")
	}

	if key == "" {
		return "", "", fmt.Errorf("key cannot be empty")
	}

	return bucket, key, nil
}

func IsS3URL(url string) bool {
	return strings.HasPrefix(url, "s3://")
}

func (d *S3Downloader) SetURL(url string) {
	d.url = url
	bucket, key, _ := ParseS3URL(url)
	d.bucket = bucket
	d.key = key
}

func (d *S3Downloader) SetOutputPath(path string) {
	d.outputPath = path
}

func (d *S3Downloader) SetControlFilePath(path string) {
	d.controlFilePath = path
}

func (d *S3Downloader) SetControlFile(cf interface{}) {
	d.controlFile = cf
}

func (d *S3Downloader) URL() string {
	return d.url
}

func (d *S3Downloader) OutputPath() string {
	return d.outputPath
}

func (d *S3Downloader) FileSize() int64 {
	return d.fileSize
}

func (d *S3Downloader) Download(ctx context.Context) error {
	d.lastSpeedCheck = time.Now()
	d.lastSpeedBytes = 0

	headInput := &s3.HeadObjectInput{
		Bucket: aws.String(d.bucket),
		Key:    aws.String(d.key),
	}

	headResult, err := d.client.HeadObject(ctx, headInput)
	if err != nil {
		return fmt.Errorf("head object: %w", err)
	}

	if headResult.ContentLength != nil {
		d.fileSize = *headResult.ContentLength
	}

	d.logger.Infow("starting S3 download",
		"bucket", d.bucket,
		"key", d.key,
		"file_size", d.fileSize,
		"url", d.url,
	)

	existingSize := int64(0)
	if stat, err := os.Stat(d.outputPath); err == nil {
		existingSize = stat.Size()
	}

	if existingSize == d.fileSize && d.fileSize > 0 {
		d.logger.Infow("file already fully downloaded, skipping")
		atomic.StoreInt64(&d.totalDownloaded, d.fileSize)
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
		if _, err := file.Seek(0, io.SeekEnd); err != nil {
			file.Close()
			return fmt.Errorf("seek to end of file: %w", err)
		}
	} else {
		file, err = os.OpenFile(d.outputPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
		if err != nil {
			return fmt.Errorf("create output file: %w", err)
		}
	}
	defer file.Close()

	getInput := &s3.GetObjectInput{
		Bucket: aws.String(d.bucket),
		Key:    aws.String(d.key),
	}

	if existingSize > 0 && existingSize < d.fileSize {
		d.logger.Infow("resuming S3 download",
			"existing_size", existingSize,
			"total_size", d.fileSize,
		)
		getInput.Range = aws.String(fmt.Sprintf("bytes=%d-", existingSize))
		atomic.StoreInt64(&d.totalDownloaded, existingSize)
	}

	getResult, err := d.client.GetObject(ctx, getInput)
	if err != nil {
		return fmt.Errorf("get object: %w", err)
	}
	defer getResult.Body.Close()

	if getResult.ContentRange != nil {
		rangeStr := *getResult.ContentRange
		if strings.HasPrefix(rangeStr, "bytes ") {
			parts := strings.Split(rangeStr, "/")
			if len(parts) == 2 && parts[1] != "*" {
				if totalSize, parseErr := parseContentRangeTotal(parts[1]); parseErr == nil {
					if existingSize == totalSize && totalSize > 0 {
						d.logger.Infow("file already fully downloaded, skipping")
						atomic.StoreInt64(&d.totalDownloaded, totalSize)
						return nil
					}
				}
			}
		}
	}

	buf := make([]byte, d.cfg.BufferSize)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		n, readErr := getResult.Body.Read(buf)
		if n > 0 {
			_, writeErr := file.Write(buf[:n])
			if writeErr != nil {
				return fmt.Errorf("write file: %w", writeErr)
			}

			atomic.AddInt64(&d.totalDownloaded, int64(n))

			now := time.Now()
			if now.Sub(d.lastSpeedCheck) >= time.Second {
				elapsed := now.Sub(d.lastSpeedCheck).Seconds()
				currentTotal := atomic.LoadInt64(&d.totalDownloaded)
				if elapsed > 0 {
					atomic.StoreInt64(&d.speed, int64(float64(currentTotal-d.lastSpeedBytes)/elapsed))
				}
				d.lastSpeedBytes = currentTotal
				d.lastSpeedCheck = now
			}
		}

		if readErr != nil {
			if readErr == io.EOF {
				break
			}
			return fmt.Errorf("read object: %w", readErr)
		}
	}

	d.logger.Infow("S3 download completed",
		"bucket", d.bucket,
		"key", d.key,
		"total_bytes", atomic.LoadInt64(&d.totalDownloaded),
	)

	return nil
}

func parseContentRangeTotal(s string) (int64, error) {
	if s == "" || s == "*" {
		return 0, fmt.Errorf("unknown total size")
	}

	var total int64
	for _, c := range s {
		if c < '0' || c > '9' {
			return 0, fmt.Errorf("invalid character in content range total")
		}
		total = total*10 + int64(c-'0')
	}

	return total, nil
}

func (d *S3Downloader) Speed() int64 {
	return atomic.LoadInt64(&d.speed)
}

func (d *S3Downloader) Progress() float64 {
	if d.fileSize <= 0 {
		return 0
	}
	downloaded := atomic.LoadInt64(&d.totalDownloaded)
	return float64(downloaded) / float64(d.fileSize) * 100
}

func (d *S3Downloader) TotalDownloaded() int64 {
	return atomic.LoadInt64(&d.totalDownloaded)
}

func (d *S3Downloader) ActiveThreads() int32 {
	return 1
}

func (d *S3Downloader) SetRateLimit(rate int64) {
}

func (d *S3Downloader) GetRateLimit() int64 {
	return 0
}

func (d *S3Downloader) SetRetryConfig(cfg RetryConfig) {
}

func (d *S3Downloader) GetRetryConfig() RetryConfig {
	return DefaultRetryConfig()
}

func (d *S3Downloader) LoadProgress(ctx context.Context) error {
	return nil
}

func (d *S3Downloader) SaveProgress() error {
	return nil
}

type S3ProtocolHandler struct {
	logger *zap.SugaredLogger
	cfg    *config.DownloadConfig
}

func NewS3ProtocolHandler(logger *zap.SugaredLogger, cfg *config.DownloadConfig) *S3ProtocolHandler {
	if cfg == nil {
		cfg = config.DefaultDownloadConfig()
	}
	return &S3ProtocolHandler{
		logger: logger,
		cfg:    cfg,
	}
}

func (h *S3ProtocolHandler) CanHandle(url string) bool {
	return IsS3URL(url)
}

func (h *S3ProtocolHandler) NewDownloader(url, outputPath string) (DownloaderInterface, error) {
	return NewS3Downloader(url, outputPath, h.cfg, h.logger)
}

func (h *S3ProtocolHandler) GetFileInfo(url string) (int64, error) {
	bucket, key, err := ParseS3URL(url)
	if err != nil {
		return 0, fmt.Errorf("parse S3 URL: %w", err)
	}

	awsCfg, err := loadAWSConfig()
	if err != nil {
		return 0, fmt.Errorf("load AWS config: %w", err)
	}

	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.UsePathStyle = true
	})

	headInput := &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}

	// GetFileInfo 没有 context 参数，这里为 HeadObject 网络请求添加超时，
	// 避免在异常 S3 端点上长时间阻塞。
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	headResult, err := client.HeadObject(ctx, headInput)
	if err != nil {
		return 0, fmt.Errorf("head object: %w", err)
	}

	if headResult.ContentLength == nil {
		return 0, fmt.Errorf("content length not available")
	}

	return *headResult.ContentLength, nil
}
