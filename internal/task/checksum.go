package task

import (
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"os"
	"strings"
	"time"
)

const (
	ChecksumMD5    = "md5"
	ChecksumSHA1   = "sha1"
	ChecksumSHA256 = "sha256"
	ChecksumCRC32  = "crc32"
)

func ComputeChecksum(filePath, checksumType string) (string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", fmt.Errorf("open file: %w", err)
	}
	defer file.Close()

	var h hash.Hash
	switch strings.ToLower(checksumType) {
	case ChecksumMD5:
		h = md5.New()
	case ChecksumSHA1:
		h = sha1.New()
	case ChecksumSHA256:
		h = sha256.New()
	case ChecksumCRC32:
		h = crc32.NewIEEE()
	default:
		return "", fmt.Errorf("unsupported checksum type: %s", checksumType)
	}

	if _, err := io.Copy(h, file); err != nil {
		return "", fmt.Errorf("compute checksum: %w", err)
	}

	return hex.EncodeToString(h.Sum(nil)), nil
}

func VerifyChecksum(filePath, checksumType, expectedChecksum string) (bool, error) {
	actual, err := ComputeChecksum(filePath, checksumType)
	if err != nil {
		return false, err
	}
	return strings.EqualFold(actual, expectedChecksum), nil
}

func (t *Task) VerifyDownload() (bool, error) {
	t.mu.RLock()
	checksumType := t.ChecksumType
	checksumValue := t.ChecksumValue
	outputPath := t.OutputPath
	t.mu.RUnlock()

	if checksumType == "" || checksumValue == "" {
		return true, nil
	}

	return VerifyChecksum(outputPath, checksumType, checksumValue)
}

func (t *Task) SetChecksum(checksumType, checksumValue string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.ChecksumType = strings.ToLower(checksumType)
	t.ChecksumValue = checksumValue
	t.UpdatedAt = time.Now()
}
