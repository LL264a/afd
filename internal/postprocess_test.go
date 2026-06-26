package internal

import (
	"archive/zip"
	"os"
	"path/filepath"
	"testing"

	"github.com/nexus-dl/afd/pkg/config"
)

func TestIsSafeExtractPath(t *testing.T) {
	tests := []struct {
		name     string
		path     string
		destDir  string
		expected bool
	}{
		{"normal file", "/tmp/dest/file.txt", "/tmp/dest", true},
		{"subdirectory", "/tmp/dest/sub/file.txt", "/tmp/dest", true},
		{"path traversal", "/tmp/dest/../../../etc/passwd", "/tmp/dest", false},
		// path == destDir 时 rel == "."，isSafeExtractPath 返回 false
		{"exact dest dir", "/tmp/dest", "/tmp/dest", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isSafeExtractPath(tt.path, tt.destDir)
			if result != tt.expected {
				t.Errorf("isSafeExtractPath(%q, %q) = %v, want %v", tt.path, tt.destDir, result, tt.expected)
			}
		})
	}
}

func TestApplyStripComponents(t *testing.T) {
	tests := []struct {
		name     string
		path     string
		strip    int
		expected string
	}{
		{"no strip", "a/b/c.txt", 0, "a/b/c.txt"},
		{"strip 1", "a/b/c.txt", 1, "b/c.txt"},
		{"strip 2", "a/b/c.txt", 2, "c.txt"},
		{"strip all", "a/b/c.txt", 3, ""},
		{"strip more", "a/b/c.txt", 4, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := applyStripComponents(tt.path, tt.strip)
			if result != tt.expected {
				t.Errorf("applyStripComponents(%q, %d) = %q, want %q", tt.path, tt.strip, result, tt.expected)
			}
		})
	}
}

func TestExtractZip(t *testing.T) {
	// 创建临时目录
	tmpDir := t.TempDir()

	// 创建 zip 文件
	zipPath := filepath.Join(tmpDir, "test.zip")
	destDir := filepath.Join(tmpDir, "extracted")
	os.MkdirAll(destDir, 0755)

	// 创建 zip 文件内容
	zipFile, err := os.Create(zipPath)
	if err != nil {
		t.Fatal(err)
	}

	zw := zip.NewWriter(zipFile)

	// 添加文件
	w, err := zw.Create("hello.txt")
	if err != nil {
		t.Fatal(err)
	}
	w.Write([]byte("hello world"))

	// 添加目录中的文件
	w2, err := zw.Create("subdir/file2.txt")
	if err != nil {
		t.Fatal(err)
	}
	w2.Write([]byte("nested file"))

	zw.Close()
	zipFile.Close()

	// 创建 PostProcessor 并解压
	pp := NewPostProcessor(nil)
	err = pp.ExtractArchive(zipPath, destDir)
	if err != nil {
		t.Fatalf("ExtractArchive failed: %v", err)
	}

	// 验证解压结果
	content, err := os.ReadFile(filepath.Join(destDir, "hello.txt"))
	if err != nil {
		t.Fatalf("failed to read extracted file: %v", err)
	}
	if string(content) != "hello world" {
		t.Errorf("expected 'hello world', got '%s'", string(content))
	}

	content2, err := os.ReadFile(filepath.Join(destDir, "subdir", "file2.txt"))
	if err != nil {
		t.Fatalf("failed to read nested file: %v", err)
	}
	if string(content2) != "nested file" {
		t.Errorf("expected 'nested file', got '%s'", string(content2))
	}
}

func TestExtractZipPathTraversal(t *testing.T) {
	tmpDir := t.TempDir()

	zipPath := filepath.Join(tmpDir, "evil.zip")
	destDir := filepath.Join(tmpDir, "extracted")
	os.MkdirAll(destDir, 0755)

	// 创建包含路径遍历的 zip
	zipFile, _ := os.Create(zipPath)
	zw := zip.NewWriter(zipFile)

	w, _ := zw.Create("../../../etc/passwd")
	w.Write([]byte("evil"))

	zw.Close()
	zipFile.Close()

	pp := NewPostProcessor(nil)
	_ = pp.ExtractArchive(zipPath, destDir)

	// 验证路径遍历被阻止
	_, err := os.ReadFile(filepath.Join(tmpDir, "..", "..", "..", "etc", "passwd"))
	if err == nil {
		t.Error("path traversal was not blocked")
	}
}

func TestMoveFile(t *testing.T) {
	tmpDir := t.TempDir()

	src := filepath.Join(tmpDir, "src.txt")
	dstDir := filepath.Join(tmpDir, "moved")
	os.MkdirAll(dstDir, 0755)
	dst := filepath.Join(dstDir, "src.txt")

	os.WriteFile(src, []byte("test content"), 0644)

	// moveFile 是单参数方法，目标通过 config.Move.Destination 确定
	cfg := config.DefaultPostProcessConfig()
	cfg.Move.Enabled = true
	cfg.Move.Destination = dstDir
	pp := NewPostProcessor(cfg)
	pp.moveFile(src)

	// 验证源文件已删除（os.Rename 移动）
	if _, err := os.Stat(src); !os.IsNotExist(err) {
		t.Error("source file should be deleted")
	}

	// 验证目标文件存在
	content, err := os.ReadFile(dst)
	if err != nil {
		t.Fatalf("failed to read moved file: %v", err)
	}
	if string(content) != "test content" {
		t.Errorf("expected 'test content', got '%s'", string(content))
	}
}

func TestCleanupFiles(t *testing.T) {
	tmpDir := t.TempDir()

	// 创建测试文件
	f1 := filepath.Join(tmpDir, "temp.tmp")
	f2 := filepath.Join(tmpDir, "data.bak")
	f3 := filepath.Join(tmpDir, "keep.txt")

	os.WriteFile(f1, []byte("temp"), 0644)
	os.WriteFile(f2, []byte("backup"), 0644)
	os.WriteFile(f3, []byte("keep"), 0644)

	// cleanupFiles 是单参数方法，模式通过 config.Cleanup.Patterns 确定
	cfg := config.DefaultPostProcessConfig()
	cfg.Cleanup.Enabled = true
	cfg.Cleanup.Patterns = []string{"*.tmp", "*.bak"}
	pp := NewPostProcessor(cfg)
	// cleanupFiles 基于 filepath.Dir(filePath) 进行清理
	pp.cleanupFiles(f3)

	// 验证临时文件已删除
	if _, err := os.Stat(f1); !os.IsNotExist(err) {
		t.Error("temp file should be deleted")
	}
	if _, err := os.Stat(f2); !os.IsNotExist(err) {
		t.Error("bak file should be deleted")
	}
	// 验证 keep.txt 保留
	if _, err := os.Stat(f3); os.IsNotExist(err) {
		t.Error("keep.txt should not be deleted")
	}
}
