package internal

import (
	"archive/tar"
	"compress/bzip2"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/nexus-dl/afd/pkg/config"
	"github.com/nexus-dl/afd/pkg/logger"
	yeka "github.com/yeka/zip"
)

type PostProcessor struct {
	config   *config.PostProcessConfig
	mu       sync.RWMutex
	registry map[string]PostProcessorFunc
}

type PostProcessorFunc func(src, dst string) error

func NewPostProcessor(cfg *config.PostProcessConfig) *PostProcessor {
	if cfg == nil {
		cfg = config.DefaultPostProcessConfig()
	}
	return &PostProcessor{
		config:   cfg,
		registry: make(map[string]PostProcessorFunc),
	}
}

func (p *PostProcessor) RegisterProcessor(name string, fn PostProcessorFunc) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.registry[name] = fn
}

func (p *PostProcessor) GetProcessor(name string) (PostProcessorFunc, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	fn, ok := p.registry[name]
	return fn, ok
}

// ExtractArchive 解压归档文件到 destDir，根据扩展名自动检测压缩格式。
// 支持 zip、tar.gz/tgz、tar.bz2/tbz2、tar 格式。
func (p *PostProcessor) ExtractArchive(archivePath, destDir string) error {
	lowerPath := strings.ToLower(archivePath)
	switch {
	case strings.HasSuffix(lowerPath, ".zip"):
		return p.extractZip(archivePath, destDir)
	case strings.HasSuffix(lowerPath, ".tar.gz") || strings.HasSuffix(lowerPath, ".tgz"):
		return p.extractTar(archivePath, destDir, "gz")
	case strings.HasSuffix(lowerPath, ".tar.bz2") || strings.HasSuffix(lowerPath, ".tbz2"):
		return p.extractTar(archivePath, destDir, "bz2")
	case strings.HasSuffix(lowerPath, ".gz"):
		return p.extractTar(archivePath, destDir, "gz")
	case strings.HasSuffix(lowerPath, ".bz2"):
		return p.extractTar(archivePath, destDir, "bz2")
	case strings.HasSuffix(lowerPath, ".tar"):
		return p.extractTar(archivePath, destDir, "")
	default:
		return fmt.Errorf("unsupported archive format: %s", filepath.Ext(archivePath))
	}
}

// extractZip 解压 zip 归档，支持 StripComponents、Overwrite 与加密条目解密。
// 加密条目通过 ExtractConfig.Password 解密（支持 ZipCrypto 与 WinZip AES）。
func (p *PostProcessor) extractZip(archivePath, destDir string) error {
	r, err := yeka.OpenReader(archivePath)
	if err != nil {
		return fmt.Errorf("open zip: %w", err)
	}
	defer r.Close()

	if err := os.MkdirAll(destDir, 0755); err != nil {
		return fmt.Errorf("create dest dir: %w", err)
	}

	strip := p.config.Extract.StripComponents
	overwrite := p.config.Extract.Overwrite
	password := p.config.Extract.Password

	for _, f := range r.File {
		name := applyStripComponents(f.Name, strip)
		if name == "" {
			continue
		}

		path := filepath.Join(destDir, name)
		if !isSafeExtractPath(path, destDir) {
			if logger.Log != nil {
				logger.Log.Warnw("skipping unsafe zip entry path", "entry", f.Name)
			}
			continue
		}

		if f.FileInfo().IsDir() {
			os.MkdirAll(path, f.Mode())
			continue
		}

		if !overwrite {
			if _, err := os.Stat(path); err == nil {
				continue
			}
		}

		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			continue
		}

		if f.IsEncrypted() {
			if password != "" {
				f.SetPassword(password)
			} else {
				if logger.Log != nil {
					logger.Log.Warnw("encrypted zip entry without password", "file", f.Name)
				}
				continue
			}
		}

		rc, err := f.Open()
		if err != nil {
			// 加密条目等无法打开的情况，跳过
			if logger.Log != nil {
				logger.Log.Warnw("skip zip entry", "entry", f.Name, "error", err)
			}
			continue
		}

		w, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, f.Mode())
		if err != nil {
			rc.Close()
			continue
		}

		_, copyErr := io.Copy(w, rc)
		w.Close()
		rc.Close()
		if copyErr != nil {
			if logger.Log != nil {
				logger.Log.Warnw("copy zip entry failed", "entry", f.Name, "error", copyErr)
			}
		}
	}
	return nil
}

// extractTar 解压 tar 归档，compression 支持 "gz"、"bz2" 或 ""（无压缩）。
func (p *PostProcessor) extractTar(archivePath, destDir string, compression string) error {
	f, err := os.Open(archivePath)
	if err != nil {
		return fmt.Errorf("open archive: %w", err)
	}
	defer f.Close()

	var reader io.Reader = f
	switch compression {
	case "gz":
		gzr, err := gzip.NewReader(f)
		if err != nil {
			return fmt.Errorf("gzip reader: %w", err)
		}
		defer gzr.Close()
		reader = gzr
	case "bz2":
		reader = bzip2.NewReader(f)
	case "":
		reader = f
	default:
		return fmt.Errorf("unsupported compression: %s", compression)
	}

	if err := os.MkdirAll(destDir, 0755); err != nil {
		return fmt.Errorf("create dest dir: %w", err)
	}

	strip := p.config.Extract.StripComponents
	overwrite := p.config.Extract.Overwrite

	tr := tar.NewReader(reader)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("tar read: %w", err)
		}

		name := applyStripComponents(hdr.Name, strip)
		if name == "" {
			continue
		}

		path := filepath.Join(destDir, name)
		if !isSafeExtractPath(path, destDir) {
			if logger.Log != nil {
				logger.Log.Warnw("skipping unsafe tar entry path", "entry", hdr.Name)
			}
			continue
		}

		switch hdr.Typeflag {
		case tar.TypeDir:
			os.MkdirAll(path, os.FileMode(hdr.Mode))
		case tar.TypeReg, tar.TypeRegA:
			if !overwrite {
				if _, err := os.Stat(path); err == nil {
					continue
				}
			}
			if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
				continue
			}
			w, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, os.FileMode(hdr.Mode))
			if err != nil {
				continue
			}
			_, copyErr := io.Copy(w, tr)
			w.Close()
			if copyErr != nil {
				if logger.Log != nil {
					logger.Log.Warnw("copy tar entry failed", "entry", hdr.Name, "error", copyErr)
				}
			}
		case tar.TypeSymlink:
			// 出于安全考虑，跳过符号链接
			continue
		}
	}
	return nil
}

// Process 按 Extract → Move → Cleanup 顺序执行后处理，每步检查 Enabled 配置。
func (p *PostProcessor) Process(filePath string) error {
	// 1. Extract
	if p.config.Extract.Enabled {
		destDir := p.config.Extract.OutputDir
		if destDir == "" {
			destDir = filepath.Dir(filePath)
		}
		if err := p.ExtractArchive(filePath, destDir); err != nil {
			if logger.Log != nil {
				logger.Log.Warnw("extract failed", "file", filePath, "error", err)
			}
		}
		if p.config.Extract.DeleteAfterExtract {
			os.Remove(filePath)
		}
	}

	// 2. Move
	p.moveFile(filePath)

	// 3. Cleanup
	p.cleanupFiles(filePath)

	return nil
}

// ProcessWithExtract 强制执行解压，随后执行 Move 和 Cleanup。
func (p *PostProcessor) ProcessWithExtract(filePath string) error {
	// 1. Extract（强制）
	destDir := p.config.Extract.OutputDir
	if destDir == "" {
		destDir = filepath.Dir(filePath)
	}
	if err := p.ExtractArchive(filePath, destDir); err != nil {
		if logger.Log != nil {
			logger.Log.Warnw("extract failed", "file", filePath, "error", err)
		}
	}
	if p.config.Extract.DeleteAfterExtract {
		os.Remove(filePath)
	}

	// 2. Move
	p.moveFile(filePath)

	// 3. Cleanup
	p.cleanupFiles(filePath)

	return nil
}

// moveFile 按 Move 配置移动文件，跨设备时回退到复制。
func (p *PostProcessor) moveFile(filePath string) {
	if !p.config.Move.Enabled {
		return
	}
	dest := filepath.Join(p.config.Move.Destination, filepath.Base(filePath))
	if p.config.Move.Overwrite {
		os.Remove(dest)
	}
	if err := os.Rename(filePath, dest); err != nil {
		// 跨设备时复制
		if copyErr := copyFile(filePath, dest); copyErr != nil {
			if logger.Log != nil {
				logger.Log.Warnw("move failed", "file", filePath, "error", copyErr)
			}
		} else if p.config.Move.DeleteSource {
			os.Remove(filePath)
		}
	}
}

// cleanupFiles 按 Cleanup 配置删除匹配文件。
func (p *PostProcessor) cleanupFiles(filePath string) {
	if !p.config.Cleanup.Enabled {
		return
	}
	dir := filepath.Dir(filePath)
	for _, pattern := range p.config.Cleanup.Patterns {
		matches, _ := filepath.Glob(filepath.Join(dir, pattern))
		for _, m := range matches {
			if err := os.Remove(m); err != nil {
				if logger.Log != nil {
					logger.Log.Warnw("cleanup failed", "file", m, "error", err)
				}
			}
		}
	}
	if p.config.Cleanup.DeleteTempFiles {
		// 删除下载过程中产生的临时/控制文件（如未完成的分段、断点续传控制文件）
		tempPatterns := []string{"*.tmp", "*.part", "*.ctl", "*.aria2"}
		for _, pattern := range tempPatterns {
			matches, _ := filepath.Glob(filepath.Join(dir, pattern))
			for _, m := range matches {
				if err := os.Remove(m); err != nil {
					if logger.Log != nil {
						logger.Log.Debugw("failed to remove temp file", "file", m, "error", err)
					}
				}
			}
		}
	}
	if p.config.Cleanup.DeleteSourceFile {
		os.Remove(filePath)
	}
}

func (p *PostProcessor) SetConfig(cfg *config.PostProcessConfig) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.config = cfg
}

func (p *PostProcessor) GetConfig() *config.PostProcessConfig {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.config
}

// applyStripComponents 去除路径前 n 层组件，返回剩余相对路径；全部被去除则返回空串。
func applyStripComponents(name string, n int) string {
	if n <= 0 {
		return name
	}
	name = filepath.ToSlash(name)
	parts := strings.Split(name, "/")
	if len(parts) <= n {
		return ""
	}
	result := strings.Join(parts[n:], "/")
	if result == "" || result == "." {
		return ""
	}
	return result
}

// isSafeExtractPath 检查解压目标路径是否位于 destDir 内，防止路径遍历攻击。
func isSafeExtractPath(path, destDir string) bool {
	absPath, err := filepath.Abs(path)
	if err != nil {
		return false
	}
	absDest, err := filepath.Abs(destDir)
	if err != nil {
		return false
	}
	rel, err := filepath.Rel(absDest, absPath)
	if err != nil {
		return false
	}
	return rel != "." && !strings.HasPrefix(rel, "..") && !strings.HasPrefix(filepath.ToSlash(rel), "../")
}

// copyFile 复制文件内容，用于跨设备移动场景。
func copyFile(src, dst string) error {
	srcFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer srcFile.Close()

	if err := os.MkdirAll(filepath.Dir(dst), 0755); err != nil {
		return err
	}

	tmpDst := dst + ".tmp"
	dstFile, err := os.OpenFile(tmpDst, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}

	if _, err := io.Copy(dstFile, srcFile); err != nil {
		dstFile.Close()
		os.Remove(tmpDst)
		return err
	}
	dstFile.Close()

	return os.Rename(tmpDst, dst)
}
