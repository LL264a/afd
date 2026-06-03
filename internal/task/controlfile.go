package task

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type ControlFile struct {
	InfoHash         string    `json:"infohash"`
	TotalLength      int64     `json:"totalLength"`
	CompletedLength  int64     `json:"completedLength"`
	PieceLength      int64     `json:"pieceLength"`
	Pieces           string    `json:"pieces"`
	Bitfield         string    `json:"bitfield"`
	NumPieces        int       `json:"numPieces"`
	CreatedAt        time.Time `json:"createdAt"`
	UpdatedAt        time.Time `json:"updatedAt"`
	Status           string    `json:"status"`
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
		dataDir: filepath.Join(dataDir, "nexus-dl"),
	}
}

func (s *ControlFileStore) dir() (string, error) {
	if err := os.MkdirAll(s.dataDir, 0755); err != nil {
		return "", fmt.Errorf("failed to create control file directory: %w", err)
	}
	return s.dataDir, nil
}

func (s *ControlFileStore) filePath(taskID string) string {
	return filepath.Join(s.dataDir, taskID+".aria2")
}

func (s *ControlFileStore) Save(taskID string, cf *ControlFile) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, err := s.dir(); err != nil {
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

	return os.WriteFile(s.filePath(taskID), data, 0644)
}

func (s *ControlFileStore) Load(taskID string) (*ControlFile, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	path := s.filePath(taskID)
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("control file for task %s not found", taskID)
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
			return fmt.Errorf("control file for task %s not found", taskID)
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
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".aria2" {
			continue
		}
		taskID := entry.Name()[:len(entry.Name())-6]
		taskIDs = append(taskIDs, taskID)
	}

	return taskIDs, nil
}