package task

import (
	"os"
	"path/filepath"
	"testing"
)

func newTestStore(t *testing.T) *TaskStore {
	t.Helper()
	dir := t.TempDir()
	return NewTaskStore(dir)
}

func TestTaskStoreSaveAndLoad(t *testing.T) {
	store := newTestStore(t)
	task := newTestTask(1)
	task.Metadata["source"] = "test"

	if err := store.Save(task); err != nil {
		t.Fatalf("Save returned error: %v", err)
	}

	loaded, err := store.Load(task.ID)
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if loaded.ID != task.ID {
		t.Errorf("ID = %s, want %s", loaded.ID, task.ID)
	}
	if loaded.URL != task.URL {
		t.Errorf("URL = %s, want %s", loaded.URL, task.URL)
	}
	if loaded.Metadata["source"] != "test" {
		t.Errorf("Metadata lost, got %v", loaded.Metadata)
	}
}

func TestTaskStoreLoadMissing(t *testing.T) {
	store := newTestStore(t)
	_, err := store.Load("nonexistent")
	if err == nil {
		t.Error("Load missing should return error")
	}
}

func TestTaskStoreLoadAll(t *testing.T) {
	store := newTestStore(t)
	for i := 0; i < 5; i++ {
		task := newTestTask(i)
		if err := store.Save(task); err != nil {
			t.Fatalf("Save %d error: %v", i, err)
		}
	}

	tasks, err := store.LoadAll()
	if err != nil {
		t.Fatalf("LoadAll error: %v", err)
	}
	if len(tasks) != 5 {
		t.Errorf("len(tasks) = %d, want 5", len(tasks))
	}
}

func TestTaskStoreLoadAllEmpty(t *testing.T) {
	store := newTestStore(t)
	tasks, err := store.LoadAll()
	if err != nil {
		t.Fatalf("LoadAll error: %v", err)
	}
	if len(tasks) != 0 {
		t.Errorf("len(tasks) = %d, want 0", len(tasks))
	}
}

func TestTaskStoreDelete(t *testing.T) {
	store := newTestStore(t)
	task := newTestTask(1)
	_ = store.Save(task)

	if err := store.Delete(task.ID); err != nil {
		t.Fatalf("Delete error: %v", err)
	}

	_, err := store.Load(task.ID)
	if err == nil {
		t.Error("Load after Delete should return error")
	}
}

func TestTaskStoreDeleteMissing(t *testing.T) {
	store := newTestStore(t)
	err := store.Delete("nonexistent")
	if err == nil {
		t.Error("Delete missing should return error")
	}
}

func TestTaskStoreLoadAllSkipsCorrupted(t *testing.T) {
	store := newTestStore(t)

	task := newTestTask(1)
	_ = store.Save(task)

	corruptedPath := filepath.Join(store.dataDir, "corrupted.json")
	if err := os.WriteFile(corruptedPath, []byte("not-json{"), 0644); err != nil {
		t.Fatalf("write corrupted: %v", err)
	}

	tasks, err := store.LoadAll()
	if err != nil {
		t.Fatalf("LoadAll error: %v", err)
	}
	if len(tasks) != 1 {
		t.Errorf("len(tasks) = %d, want 1 (skipped corrupted)", len(tasks))
	}
}

// Regression: Save used os.WriteFile which truncates before writing.
// A crash mid-write would leave a zero-length file; LoadAll would
// then skip it (unmarshal fails) and the task would be silently
// lost.  After switching to write-to-temp + rename, the destination
// file is either the old content or the new content — never partial.
func TestTaskStoreAtomicWrite(t *testing.T) {
	store := newTestStore(t)
	task := newTestTask(42)
	task.Metadata["key"] = "value"

	if err := store.Save(task); err != nil {
		t.Fatalf("Save: %v", err)
	}

	// Verify the file exists and is valid JSON (not zero-length).
	path := store.taskPath(task.ID)
	stat, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if stat.Size() == 0 {
		t.Error("task file is empty after Save")
	}

	// No leftover temp files.
	entries, _ := os.ReadDir(store.dataDir)
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".tmp" {
			t.Errorf("leftover temp file: %s", e.Name())
		}
	}

	// Overwrite should still be atomic.
	task.Metadata["key"] = "updated"
	if err := store.Save(task); err != nil {
		t.Fatalf("Save (update): %v", err)
	}
	loaded, err := store.Load(task.ID)
	if err != nil {
		t.Fatalf("Load after update: %v", err)
	}
	if loaded.Metadata["key"] != "updated" {
		t.Errorf("Metadata[key] = %q, want %q", loaded.Metadata["key"], "updated")
	}
}
