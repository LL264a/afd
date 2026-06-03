package task

import (
	"strconv"
	"sync"
	"testing"
)

func BenchmarkTaskQueueAdd(b *testing.B) {
	q := NewTaskQueue(1024)
	for i := 0; i < b.N; i++ {
		t1 := NewTask("https://example.com/"+strconv.Itoa(i), "/tmp/"+strconv.Itoa(i))
		_ = q.Add(t1)
	}
}

func BenchmarkTaskQueueGet(b *testing.B) {
	q := NewTaskQueue(1024)
	tasks := make([]*Task, 1024)
	for i := range tasks {
		tasks[i] = NewTask("https://example.com/"+strconv.Itoa(i), "/tmp/"+strconv.Itoa(i))
		_ = q.Add(tasks[i])
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = q.Get(tasks[i%len(tasks)].ID)
	}
}

func BenchmarkTaskQueueList(b *testing.B) {
	q := NewTaskQueue(1024)
	for i := 0; i < 1024; i++ {
		_ = q.Add(NewTask("https://example.com/"+strconv.Itoa(i), "/tmp/"+strconv.Itoa(i)))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = q.List()
	}
}

func BenchmarkTaskQueuePriorityAdd(b *testing.B) {
	q := NewTaskQueue(1024)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		t1 := NewTask("https://example.com/"+strconv.Itoa(i), "/tmp/"+strconv.Itoa(i))
		t1.Priority = i % 10
		_ = q.Add(t1)
	}
}

func BenchmarkTaskQueueConcurrentAddGet(b *testing.B) {
	q := NewTaskQueue(1024)
	for i := 0; i < 1024; i++ {
		_ = q.Add(NewTask("https://example.com/"+strconv.Itoa(i), "/tmp/"+strconv.Itoa(i)))
	}
	var wg sync.WaitGroup
	var ops int64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			wg.Add(1)
			go func() {
				defer wg.Done()
				id := strconv.FormatInt(atomicAdd(&ops), 10)
				_ = q.Add(NewTask("https://example.com/"+id, "/tmp/"+id))
			}()
		}
	})
	wg.Wait()
}

func atomicAdd(p *int64) int64 {
	mut.Lock()
	defer mut.Unlock()
	*p++
	return *p
}

var mut sync.Mutex
