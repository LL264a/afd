package task

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/nexus-dl/afd/pkg/logger"
)

type ControlFile struct {
	InfoHash        string                   `json:"infohash"`
	TotalLength     int64                    `json:"totalLength"`
	CompletedLength int64                    `json:"completedLength"`
	PieceLength     int64                    `json:"pieceLength"`
	Pieces          string                   `json:"pieces"`
	Bitfield        string                   `json:"bitfield"`
	NumPieces       int                      `json:"numPieces"`
	PieceBitfields  []PieceBitfieldEntry     `json:"pieceBitfields,omitempty"`
	CreatedAt       time.Time                `json:"createdAt"`
	UpdatedAt       time.Time                `json:"updatedAt"`
	Status          string                   `json:"status"`
	LastModified    string                   `json:"lastModified,omitempty"`
	ETag            string                   `json:"etag,omitempty"`
}

// PieceBitfieldEntry 保存单个 Piece 的 Block 级完成位图
type PieceBitfieldEntry struct {
	Index    int    `json:"index"`
	Bitfield string `json:"bitfield"` // base64 编码的位图
}

func (cf *ControlFile) MarshalJSON() ([]byte, error) {
	type Alias ControlFile
	return json.Marshal(&struct {
		*Alias
	}{
		Alias: (*Alias)(cf),
	})
}

func (cf *ControlFile) UnmarshalJSON(data []byte) error {
	type Alias ControlFile
	aux := &struct {
		*Alias
	}{
		Alias: (*Alias)(cf),
	}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	if cf.CreatedAt.IsZero() {
		cf.CreatedAt = time.Now()
	}
	if cf.UpdatedAt.IsZero() {
		cf.UpdatedAt = time.Now()
	}
	return nil
}

type ControlFileStore struct {
	mu      sync.RWMutex
	dataDir string
}

func NewControlFileStore(dataDir string) *ControlFileStore {
	return &ControlFileStore{
		dataDir: filepath.Join(dataDir, "afd"),
	}
}

func (s *ControlFileStore) dir() (string, error) {
	if err := os.MkdirAll(s.dataDir, 0755); err != nil {
		return "", fmt.Errorf("failed to create control file directory: %w", err)
	}
	return s.dataDir, nil
}

func (s *ControlFileStore) filePath(taskID string) string {
	return filepath.Join(s.dataDir, taskID+".ctl")
}

func (s *ControlFileStore) Save(taskID string, cf *ControlFile) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	dir, err := s.dir()
	if err != nil {
		return err
	}

	cf.UpdatedAt = time.Now()
	if cf.CreatedAt.IsZero() {
		cf.CreatedAt = cf.UpdatedAt
	}

	data, err := json.MarshalIndent(cf, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal control file for task %s: %w", taskID, err)
	}

	dst := s.filePath(taskID)
	tmp, err := os.CreateTemp(dir, filepath.Base(dst)+".*.tmp")
	if err != nil {
		return fmt.Errorf("failed to create temp file for task %s: %w", taskID, err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName) // 清理未 rename 的 temp 文件

	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return fmt.Errorf("failed to write temp file for task %s: %w", taskID, err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("failed to fsync temp file for task %s: %w", taskID, err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("failed to close temp file for task %s: %w", taskID, err)
	}
	if err := os.Rename(tmpName, dst); err != nil {
		return fmt.Errorf("failed to rename temp file for task %s: %w", taskID, err)
	}

	// fsync 目录确保 rename 持久化（Windows 上可能失败，非致命）
	if d, err := os.Open(dir); err == nil {
		if err := d.Sync(); err != nil {
			d.Close()
			// Windows 上目录 fsync 可能返回 "Access is denied"，不阻断写入
			if logger.Log != nil {
				logger.Log.Debugw("fsync directory failed (non-fatal)", "dir", dir, "error", err)
			}
		} else {
			d.Close()
		}
	}

	return nil
}

func (s *ControlFileStore) Load(taskID string) (*ControlFile, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	path := s.filePath(taskID)
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("control file for task %s: %w", taskID, ErrControlFileNotFound)
		}
		return nil, fmt.Errorf("failed to read control file for task %s: %w", taskID, err)
	}

	var cf ControlFile
	if err := json.Unmarshal(data, &cf); err != nil {
		return nil, fmt.Errorf("failed to unmarshal control file for task %s: %w", taskID, err)
	}

	return &cf, nil
}

func (s *ControlFileStore) Delete(taskID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	path := s.filePath(taskID)
	if err := os.Remove(path); err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("control file for task %s: %w", taskID, ErrControlFileNotFound)
		}
		return fmt.Errorf("failed to delete control file for task %s: %w", taskID, err)
	}

	return nil
}

func (s *ControlFileStore) List() ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if _, err := s.dir(); err != nil {
		return nil, err
	}

	entries, err := os.ReadDir(s.dataDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to read control file directory: %w", err)
	}

	var taskIDs []string
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".ctl" {
			continue
		}
		taskID := entry.Name()[:len(entry.Name())-4]
		taskIDs = append(taskIDs, taskID)
	}

	return taskIDs, nil
}
