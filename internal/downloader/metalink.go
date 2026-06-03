package downloader

import (
	"context"
	"encoding/xml"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/nexus-dl/afd/internal/task"
	"github.com/nexus-dl/afd/pkg/config"
	"github.com/nexus-dl/afd/pkg/logger"
	"go.uber.org/zap"
)

const (
	MetalinkVersion3 = "3.0"
	MetalinkVersion4 = "4.0"
)

type MetalinkFile struct {
	XMLName xml.Name `xml:"file"`
	Name    string   `xml:"name,attr"`
	Size    int64    `xml:"size"`
	Hash    []struct {
		Type string `xml:"type,attr"`
		Hash string `xml:",innerxml"`
	} `xml:"hash"`
	URLs []struct {
		Priority int    `xml:"priority,attr"`
		Type     string `xml:"type,attr"`
		Location string `xml:"location,attr"`
		URL      string `xml:",innerxml"`
	} `xml:"url"`
}

type Metalink3 struct {
	XMLName xml.Name       `xml:"metalink"`
	Version string         `xml:"version,attr"`
	Files   []MetalinkFile `xml:"file"`
}

type Metalink4 struct {
	XMLName   xml.Name       `xml:"metalink"`
	XMLNS     string         `xml:"xmlns,attr"`
	Origin    *string        `xml:"origin,omitempty"`
	Generator *string        `xml:"generator,omitempty"`
	Files     []MetalinkFile `xml:"file"`
}

type MetalinkDownloader struct {
	mu           sync.Mutex
	metalinkPath string
	outputDir    string
	files        []MetalinkFile
	currentIndex int32
	logger       *zap.SugaredLogger
}

func NewMetalinkDownloader(metalinkPath, outputDir string) *MetalinkDownloader {
	return &MetalinkDownloader{
		metalinkPath: metalinkPath,
		outputDir:    outputDir,
		logger:       logger.Log.Named("metalink"),
	}
}

func (m *MetalinkDownloader) SetURL(url string)             { m.metalinkPath = url }
func (m *MetalinkDownloader) SetOutputPath(p string)        { m.outputDir = p }
func (m *MetalinkDownloader) SetControlFilePath(p string)   {}
func (m *MetalinkDownloader) SetControlFile(cf interface{}) {}
func (m *MetalinkDownloader) URL() string                   { return m.metalinkPath }
func (m *MetalinkDownloader) OutputPath() string            { return m.outputDir }
func (m *MetalinkDownloader) Speed() int64                  { return 0 }
func (m *MetalinkDownloader) Progress() float64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.files) == 0 {
		return 0.0
	}
	return float64(m.currentIndex+1) / float64(len(m.files))
}
func (m *MetalinkDownloader) TotalDownloaded() int64                 { return 0 }
func (m *MetalinkDownloader) ActiveThreads() int32                   { return 0 }
func (m *MetalinkDownloader) SetRateLimit(rate int64)                {}
func (m *MetalinkDownloader) GetRateLimit() int64                    { return 0 }
func (m *MetalinkDownloader) SetRetryConfig(cfg RetryConfig)         {}
func (m *MetalinkDownloader) GetRetryConfig() RetryConfig            { return RetryConfig{} }
func (m *MetalinkDownloader) LoadProgress(ctx context.Context) error { return nil }
func (m *MetalinkDownloader) SaveProgress() error                    { return nil }

func (m *MetalinkDownloader) parse() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.logger.Infow("Parsing metalink file", "path", m.metalinkPath)

	bytes, err := os.ReadFile(m.metalinkPath)
	if err != nil {
		return fmt.Errorf("failed to read metalink file: %w", err)
	}

	var ml4 Metalink4
	if err = xml.Unmarshal(bytes, &ml4); err == nil && ml4.XMLNS != "" {
		m.files = ml4.Files
		m.logger.Infow("Parsed as Metalink 4.0", "files", len(m.files))
		return nil
	}

	var ml3 Metalink3
	if err = xml.Unmarshal(bytes, &ml3); err == nil && ml3.Version != "" {
		m.files = ml3.Files
		m.logger.Infow("Parsed as Metalink 3.0", "files", len(m.files))
		return nil
	}

	return fmt.Errorf("failed to parse metalink file as either 3.0 or 4.0 format")
}

func (m *MetalinkDownloader) getBestURL(file MetalinkFile) string {
	bestPriority := 9999
	bestURL := ""

	for _, u := range file.URLs {
		if u.Priority == 0 {
			u.Priority = 100
		}
		if u.Priority < bestPriority {
			bestPriority = u.Priority
			bestURL = u.URL
		}
	}

	if bestURL == "" && len(file.URLs) > 0 {
		bestURL = file.URLs[0].URL
	}
	return strings.TrimSpace(bestURL)
}

func (m *MetalinkDownloader) Download(ctx context.Context) error {
	if err := m.parse(); err != nil {
		return err
	}

	if len(m.files) == 0 {
		return fmt.Errorf("no files found in metalink")
	}

	if err := os.MkdirAll(m.outputDir, 0755); err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}

	for i := range m.files {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		atomic.StoreInt32(&m.currentIndex, int32(i))
		file := m.files[i]
		outputPath := filepath.Join(m.outputDir, file.Name)
		url := m.getBestURL(file)

		if url == "" {
			m.logger.Warnw("No URL found for file, skipping", "file", file.Name)
			continue
		}

		m.logger.Infow("Downloading file from metalink",
			"file", file.Name,
			"url", url,
			"total_files", len(m.files),
			"current", i+1)

		cfg := config.DefaultDownloadConfig()
		dl := NewDownloader(cfg, m.logger)
		dl.SetURL(url)
		dl.SetOutputPath(outputPath)
		if err := dl.Download(ctx); err != nil {
			return err
		}

		if len(file.Hash) > 0 {
			for _, hash := range file.Hash {
				valid, err := task.VerifyChecksum(outputPath, hash.Type, hash.Hash)
				if err == nil && !valid {
					return fmt.Errorf("checksum verification failed for %s (type: %s)", file.Name, hash.Type)
				} else if err == nil {
					m.logger.Infow("Checksum verification passed", "file", file.Name, "type", hash.Type)
					break
				}
			}
		}
	}

	m.logger.Infow("Metalink download completed", "files", len(m.files))
	return nil
}

func IsMetalinkFile(path string) bool {
	ext := strings.ToLower(filepath.Ext(path))
	return ext == ".metalink" || ext == ".meta4"
}

type MetalinkProtocolHandler struct{}

func NewMetalinkProtocolHandler() *MetalinkProtocolHandler {
	return &MetalinkProtocolHandler{}
}

func (h *MetalinkProtocolHandler) CanHandle(input string) bool {
	return IsMetalinkFile(input)
}

func (h *MetalinkProtocolHandler) NewDownloader(input, outputDir string) interface {
	Download(context.Context) error
} {
	return NewMetalinkDownloader(input, outputDir)
}
