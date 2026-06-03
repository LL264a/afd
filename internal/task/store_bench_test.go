package task

import (
	"os"
	"path/filepath"
	"testing"
)

func BenchmarkTaskStoreSave(b *testing.B) {
	dir := b.TempDir()
	store := NewTaskStore(dir)
	tasks := make([]*Task, b.N)
	for i := range tasks {
		tasks[i] = NewTask("https://example.com/file.bin", "/tmp/file.bin")
		tasks[i].Priority = i % 10
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = store.Save(tasks[i])
	}
}

func BenchmarkTaskStoreLoad(b *testing.B) {
	dir := b.TempDir()
	store := NewTaskStore(dir)
	t1 := NewTask("https://example.com/file.bin", "/tmp/file.bin")
	_ = store.Save(t1)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = store.Load(t1.ID)
	}
}

func BenchmarkTaskWriteAndRead(b *testing.B) {
	dir := b.TempDir()
	data := make([]byte, 1<<20)
	for i := range data {
		data[i] = byte(i % 251)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p := filepath.Join(dir, "x.bin")
		if err := os.WriteFile(p, data, 0o644); err != nil {
			b.Fatal(err)
		}
		_, _ = os.ReadFile(p)
	}
}
