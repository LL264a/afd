package task

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

type TaskStore struct {
	mu      sync.Mutex
	dataDir string
}

func NewTaskStore(dataDir string) *TaskStore {
	return &TaskStore{
		dataDir: filepath.Join(dataDir, "tasks"),
	}
}

func (s *TaskStore) dir() (string, error) {
	if err := os.MkdirAll(s.dataDir, 0755); err != nil {
		return "", fmt.Errorf("failed to create task store directory: %w", err)
	}
	return s.dataDir, nil
}

func (s *TaskStore) taskPath(id string) string {
	return filepath.Join(s.dataDir, id+".json")
}

func (s *TaskStore) Save(task *Task) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	dir, err := s.dir()
	if err != nil {
		return err
	}

	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create task store directory: %w", err)
	}

	data, err := json.MarshalIndent(task.GetSafe(), "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal task %s: %w", task.ID, err)
	}

	// Atomic write: write to a temp file in the same directory, then
	// rename.  os.WriteFile truncates first, so a crash mid-write
	// would leave a zero-length file and the task would be lost on
	// the next LoadAll.
	dst := s.taskPath(task.ID)
	tmp, err := os.CreateTemp(dir, task.ID+".*.tmp")
	if err != nil {
		return fmt.Errorf("failed to create temp file for task %s: %w", task.ID, err)
	}
	tmpName := tmp.Name()

	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return fmt.Errorf("failed to write task %s: %w", task.ID, err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return fmt.Errorf("failed to fsync task %s: %w", task.ID, err)
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("failed to close temp file for task %s: %w", task.ID, err)
	}
	if err := os.Rename(tmpName, dst); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("failed to rename task %s: %w", task.ID, err)
	}

	return nil
}

func (s *TaskStore) Load(id string) (*Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	path := s.taskPath(id)
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("task %s not found", id)
		}
		return nil, fmt.Errorf("failed to read task %s: %w", id, err)
	}

	var task Task
	if err := json.Unmarshal(data, &task); err != nil {
		return nil, fmt.Errorf("failed to unmarshal task %s: %w", id, err)
	}

	if task.Metadata == nil {
		task.Metadata = make(map[string]string)
	}
	return &task, nil
}

func (s *TaskStore) LoadAll() ([]*Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, err := s.dir(); err != nil {
		return nil, err
	}

	entries, err := os.ReadDir(s.dataDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to read task store directory: %w", err)
	}

	var tasks []*Task
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}

		path := filepath.Join(s.dataDir, entry.Name())
		data, err := os.ReadFile(path)
		if err != nil {
			continue
		}

		var task Task
		if err := json.Unmarshal(data, &task); err != nil {
			continue
		}

		if task.Metadata == nil {
			task.Metadata = make(map[string]string)
		}
		tasks = append(tasks, &task)
	}

	return tasks, nil
}

func (s *TaskStore) Delete(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	path := s.taskPath(id)
	if err := os.Remove(path); err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("task %s not found", id)
		}
		return fmt.Errorf("failed to delete task %s: %w", id, err)
	}

	return nil
}
