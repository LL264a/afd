package task

import (
	"os"
	"path/filepath"
	"testing"
)

func TestComputeChecksumMD5(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "test.txt")
	content := []byte("hello world")
	if err := os.WriteFile(filePath, content, 0644); err != nil {
		t.Fatal(err)
	}

	result, err := ComputeChecksum(filePath, ChecksumMD5)
	if err != nil {
		t.Fatal(err)
	}
	if result == "" {
		t.Error("expected non-empty checksum")
	}
}

func TestComputeChecksumSHA256(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "test.txt")
	content := []byte("hello world")
	if err := os.WriteFile(filePath, content, 0644); err != nil {
		t.Fatal(err)
	}

	result, err := ComputeChecksum(filePath, ChecksumSHA256)
	if err != nil {
		t.Fatal(err)
	}
	if result == "" {
		t.Error("expected non-empty checksum")
	}
}

func TestComputeChecksumCRC32(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "test.txt")
	content := []byte("hello world")
	if err := os.WriteFile(filePath, content, 0644); err != nil {
		t.Fatal(err)
	}

	result, err := ComputeChecksum(filePath, ChecksumCRC32)
	if err != nil {
		t.Fatal(err)
	}
	if result == "" {
		t.Error("expected non-empty checksum")
	}
}

func TestVerifyChecksum(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "test.txt")
	content := []byte("hello world")
	if err := os.WriteFile(filePath, content, 0644); err != nil {
		t.Fatal(err)
	}

	checksum, err := ComputeChecksum(filePath, ChecksumMD5)
	if err != nil {
		t.Fatal(err)
	}

	ok, err := VerifyChecksum(filePath, ChecksumMD5, checksum)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Error("expected checksum verification to pass")
	}

	ok, err = VerifyChecksum(filePath, ChecksumMD5, "invalid")
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Error("expected checksum verification to fail")
	}
}

func TestComputeChecksumInvalidType(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "test.txt")
	if err := os.WriteFile(filePath, []byte("test"), 0644); err != nil {
		t.Fatal(err)
	}

	_, err := ComputeChecksum(filePath, "invalid")
	if err == nil {
		t.Error("expected error for invalid checksum type")
	}
}
