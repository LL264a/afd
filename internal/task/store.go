package task

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/nexus-dl/afd/pkg/logger"
)

// 哨兵错误：用于 errors.Is 精确匹配，避免依赖字符串比较。
var (
	ErrTaskNotFound        = errors.New("task not found")
	ErrControlFileNotFound = errors.New("control file not found")
)

type TaskStore struct {
	mu      sync.RWMutex
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
	// 跨进程排他锁：使用独立的 .lock 文件，避免锁住目标文件导致 Windows 上 rename 失败
	lockPath := dst + ".lock"
	lockFile, err := os.OpenFile(lockPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return fmt.Errorf("failed to open lock file for task %s: %w", task.ID, err)
	}
	defer lockFile.Close()
	defer os.Remove(lockPath)
	if err := flockFile(lockFile); err != nil {
		return fmt.Errorf("failed to lock task %s: %w", task.ID, err)
	}
	defer unflockFile(lockFile)

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

	// fsync 目录确保 rename 持久化（Windows 上可能失败，非致命）
	if d, err := os.Open(s.dataDir); err == nil {
		if err := d.Sync(); err != nil {
			d.Close()
			// Windows 上目录 fsync 可能返回 "Access is denied"，不阻断写入
			if logger.Log != nil {
				logger.Log.Debugw("fsync directory failed (non-fatal)", "dir", s.dataDir, "error", err)
			}
		} else {
			d.Close()
		}
	}

	return nil
}

func (s *TaskStore) Load(id string) (*Task, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	path := s.taskPath(id)
	f, err := os.OpenFile(path, os.O_RDONLY, 0644)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("task %s: %w", id, ErrTaskNotFound)
		}
		return nil, fmt.Errorf("failed to read task %s: %w", id, err)
	}
	defer f.Close()
	if err := flockFile(f); err != nil {
		return nil, fmt.Errorf("failed to lock task file for task %s: %w", id, err)
	}
	defer unflockFile(f)

	data, err := io.ReadAll(f)
	if err != nil {
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
	// 使用 Lock 而非 RLock，因为本函数会执行 os.Remove 清理 temp 文件（写操作）
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
		if entry.IsDir() {
			continue
		}
		// 清理残留的 temp 文件
		if filepath.Ext(entry.Name()) == ".tmp" {
			tmpPath := filepath.Join(s.dataDir, entry.Name())
			if err := os.Remove(tmpPath); err != nil {
				if logger.Log != nil {
					logger.Log.Warnw("failed to remove stale temp file", "path", tmpPath, "error", err)
				}
			}
			continue
		}
		if filepath.Ext(entry.Name()) != ".json" {
			continue
		}

		path := filepath.Join(s.dataDir, entry.Name())
		data, err := os.ReadFile(path)
		if err != nil {
			if logger.Log != nil {
				logger.Log.Warnw("failed to read task file", "path", path, "error", err)
			}
			continue
		}

		var task Task
		if err := json.Unmarshal(data, &task); err != nil {
			if logger.Log != nil {
				logger.Log.Warnw("failed to parse task file", "path", path, "error", err)
			}
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
			return fmt.Errorf("task %s: %w", id, ErrTaskNotFound)
		}
		return fmt.Errorf("failed to delete task %s: %w", id, err)
	}

	return nil
}
