package task

import (
	"sync"
	"testing"
	"time"
)

func newTestTask(priority int) *Task {
	t := NewTask("http://example.com/file.bin", "/tmp/out.bin")
	t.Priority = priority
	return t
}

func TestTaskQueueAddAndGet(t *testing.T) {
	q := NewTaskQueue(3)
	task := newTestTask(5)

	if err := q.Add(task); err != nil {
		t.Fatalf("Add returned error: %v", err)
	}

	got, err := q.Get(task.ID)
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if got.ID != task.ID {
		t.Errorf("Get returned wrong task: %s", got.ID)
	}
}

func TestTaskQueueAddDuplicate(t *testing.T) {
	q := NewTaskQueue(3)
	task := newTestTask(5)

	_ = q.Add(task)
	err := q.Add(task)
	if err == nil {
		t.Error("Add duplicate should return error")
	}
}

func TestTaskQueueGetMissing(t *testing.T) {
	q := NewTaskQueue(3)
	_, err := q.Get("nonexistent")
	if err == nil {
		t.Error("Get missing should return error")
	}
}

func TestTaskQueueMaxConcurrent(t *testing.T) {
	q := NewTaskQueue(2)
	t1 := newTestTask(1)
	t2 := newTestTask(2)
	t3 := newTestTask(3)

	_ = q.Add(t1)
	_ = q.Add(t2)
	_ = q.Add(t3)

	if q.ActiveCount() != 2 {
		t.Errorf("ActiveCount = %d, want 2", q.ActiveCount())
	}
	if q.TotalCount() != 3 {
		t.Errorf("TotalCount = %d, want 3", q.TotalCount())
	}
	if t3.GetStatus() != StatusPending {
		t.Errorf("t3 status = %s, want pending", t3.GetStatus())
	}
}

func TestTaskQueuePriorityOrder(t *testing.T) {
	q := NewTaskQueue(1)
	t1 := newTestTask(10)
	t2 := newTestTask(1)
	t3 := newTestTask(5)

	_ = q.Add(t1)
	_ = q.Add(t2)
	_ = q.Add(t3)

	q.CompleteTask(t1.ID)

	if t2.GetStatus() != StatusDownloading {
		t.Errorf("Expected lowest priority t2 to run, got %s", t2.GetStatus())
	}
}

func TestTaskQueuePause(t *testing.T) {
	q := NewTaskQueue(2)
	t1 := newTestTask(1)
	t2 := newTestTask(2)
	_ = q.Add(t1)
	_ = q.Add(t2)

	if err := q.Pause(t1.ID); err != nil {
		t.Fatalf("Pause returned error: %v", err)
	}
	if t1.GetStatus() != StatusPaused {
		t.Errorf("t1 status = %s, want paused", t1.GetStatus())
	}
	if q.ActiveCount() != 1 {
		t.Errorf("ActiveCount = %d, want 1", q.ActiveCount())
	}
}

func TestTaskQueuePauseNotDownloading(t *testing.T) {
	q := NewTaskQueue(1)
	t1 := newTestTask(1)
	t2 := newTestTask(1)
	_ = q.Add(t1)
	_ = q.Add(t2)

	err := q.Pause(t2.ID)
	if err == nil {
		t.Error("Pause on pending task should return error")
	}
}

func TestTaskQueueResume(t *testing.T) {
	q := NewTaskQueue(2)
	t1 := newTestTask(1)
	t2 := newTestTask(2)
	_ = q.Add(t1)
	_ = q.Add(t2)
	_ = q.Pause(t1.ID)

	if err := q.Resume(t1.ID); err != nil {
		t.Fatalf("Resume returned error: %v", err)
	}
	if t1.GetStatus() != StatusDownloading {
		t.Errorf("t1 status = %s, want downloading", t1.GetStatus())
	}
}

func TestTaskQueueResumeNotPaused(t *testing.T) {
	q := NewTaskQueue(2)
	t1 := newTestTask(1)
	_ = q.Add(t1)

	err := q.Resume(t1.ID)
	if err == nil {
		t.Error("Resume on downloading task should return error")
	}
}

// Regression: when Resume is called with no free slot, the task must
// be re-enqueued in the heap.  Otherwise it silently sits in
// StatusPending forever and never reaches StatusDownloading.
func TestTaskQueueResumeReenqueuesWhenFull(t *testing.T) {
	q := NewTaskQueue(1)
	t1 := newTestTask(1)
	t2 := newTestTask(2)
	_ = q.Add(t1) // starts immediately
	_ = q.Add(t2) // pending in heap

	if err := q.Pause(t1.ID); err != nil {
		t.Fatalf("Pause t1: %v", err)
	}
	if err := q.Resume(t1.ID); err != nil {
		t.Fatalf("Resume t1: %v", err)
	}

	// t1 is now pending (no free slot because t2 took over).
	// After t2 completes, t1 must be picked up.
	q.CompleteTask(t2.ID)
	if t1.GetStatus() != StatusDownloading {
		t.Errorf("After t2 complete, t1 status = %s, want downloading", t1.GetStatus())
	}
}

func TestTaskQueueRemove(t *testing.T) {
	q := NewTaskQueue(2)
	t1 := newTestTask(1)
	t2 := newTestTask(2)
	_ = q.Add(t1)
	_ = q.Add(t2)

	if err := q.Remove(t1.ID); err != nil {
		t.Fatalf("Remove returned error: %v", err)
	}
	if q.TotalCount() != 1 {
		t.Errorf("TotalCount = %d, want 1", q.TotalCount())
	}
	if t1.GetStatus() != StatusCancelled {
		t.Errorf("t1 status = %s, want cancelled", t1.GetStatus())
	}
}

func TestTaskQueueRemoveDecrementsActive(t *testing.T) {
	q := NewTaskQueue(2)
	t1 := newTestTask(1)
	_ = q.Add(t1)

	if q.ActiveCount() != 1 {
		t.Fatalf("ActiveCount = %d, want 1", q.ActiveCount())
	}

	_ = q.Remove(t1.ID)

	if q.ActiveCount() != 0 {
		t.Errorf("After Remove, ActiveCount = %d, want 0", q.ActiveCount())
	}
}

// Regression test: Remove must look up the heap index via
// priorityItem.index (the value assigned by container/heap), not the
// slice index from a for-range loop.  Using the slice index corrupts
// the heap invariant; subsequent Pop operations may panic with
// "index out of range" or pop the wrong task.
func TestTaskQueueRemovePreservesHeapInvariant(t *testing.T) {
	q := NewTaskQueue(1)

	// With maxConcurrent=1, only t1 will start; t2..t5 stay pending
	// in the heap.
	tasks := make([]*Task, 0, 5)
	for i := 1; i <= 5; i++ {
		tk := newTestTask(i)
		tasks = append(tasks, tk)
		if err := q.Add(tk); err != nil {
			t.Fatalf("Add t%d: %v", i, err)
		}
	}

	// Remove t3 (priority 3) — it lives somewhere in the middle of the
	// heap, not at the top.  This is the path that exposed the bug.
	if err := q.Remove(tasks[2].ID); err != nil {
		t.Fatalf("Remove t3: %v", err)
	}

	// Complete the running task; the next Pop must yield t2 (the
	// remaining lowest-priority task) without panicking and without
	// losing or duplicating any entries.
	q.CompleteTask(tasks[0].ID)

	if tasks[1].GetStatus() != StatusDownloading {
		t.Errorf("After remove+complete, t2 status = %s, want downloading",
			tasks[1].GetStatus())
	}

	// Drain the rest.  Order should be t2 (prio 2), t4 (prio 4), t5 (prio 5).
	q.CompleteTask(tasks[1].ID)
	if tasks[3].GetStatus() != StatusDownloading {
		t.Errorf("Expected t4 to be next, got status %s",
			tasks[3].GetStatus())
	}
	q.CompleteTask(tasks[3].ID)
	if tasks[4].GetStatus() != StatusDownloading {
		t.Errorf("Expected t5 to be next, got status %s",
			tasks[4].GetStatus())
	}
}

func TestTaskQueueRemoveLastPending(t *testing.T) {
	q := NewTaskQueue(1)
	t1 := newTestTask(1)
	t2 := newTestTask(2)
	_ = q.Add(t1) // starts
	_ = q.Add(t2) // pending in heap

	// Remove the only pending task; the heap should now be empty.
	if err := q.Remove(t2.ID); err != nil {
		t.Fatalf("Remove t2: %v", err)
	}
	if q.TotalCount() != 1 {
		t.Errorf("TotalCount = %d, want 1", q.TotalCount())
	}
	if q.ActiveCount() != 1 {
		t.Errorf("ActiveCount = %d, want 1", q.ActiveCount())
	}
}

func TestTaskQueueCompleteTriggersNext(t *testing.T) {
	q := NewTaskQueue(1)
	q.OnTaskStart = func(t *Task) {}

	t1 := newTestTask(1)
	t2 := newTestTask(1)
	_ = q.Add(t1)
	_ = q.Add(t2)

	q.CompleteTask(t1.ID)

	if t2.GetStatus() != StatusDownloading {
		t.Errorf("After complete, t2 status = %s, want downloading", t2.GetStatus())
	}
}

func TestTaskQueueFailTask(t *testing.T) {
	q := NewTaskQueue(1)
	t1 := newTestTask(1)
	_ = q.Add(t1)

	q.FailTask(t1.ID, "network error")

	if t1.GetStatus() != StatusFailed {
		t.Errorf("t1 status = %s, want failed", t1.GetStatus())
	}
	if q.ActiveCount() != 0 {
		t.Errorf("ActiveCount = %d, want 0", q.ActiveCount())
	}
}

func TestTaskQueueSetMaxConcurrent(t *testing.T) {
	q := NewTaskQueue(1)
	t1 := newTestTask(1)
	t2 := newTestTask(1)
	_ = q.Add(t1)
	_ = q.Add(t2)

	q.SetMaxConcurrent(2)
	_ = q.Pause(t1.ID)

	if t2.GetStatus() != StatusDownloading {
		t.Errorf("t2 status = %s, want downloading", t2.GetStatus())
	}
	if q.ActiveCount() != 1 {
		t.Errorf("ActiveCount = %d, want 1", q.ActiveCount())
	}
}

func TestTaskQueueConcurrentAccess(t *testing.T) {
	q := NewTaskQueue(10)
	var wg sync.WaitGroup

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			task := newTestTask(i)
			_ = q.Add(task)
			_, _ = q.Get(task.ID)
			q.List()
		}(i)
	}

	wg.Wait()

	if q.TotalCount() != 50 {
		t.Errorf("TotalCount = %d, want 50", q.TotalCount())
	}
}

func TestTaskQueueCallbacks(t *testing.T) {
	q := NewTaskQueue(1)
	var started, completed []*Task
	var mu sync.Mutex

	q.OnTaskStart = func(t *Task) {
		mu.Lock()
		started = append(started, t)
		mu.Unlock()
	}
	q.OnTaskComplete = func(t *Task) {
		mu.Lock()
		completed = append(completed, t)
		mu.Unlock()
	}

	t1 := newTestTask(1)
	_ = q.Add(t1)
	time.Sleep(10 * time.Millisecond)
	q.CompleteTask(t1.ID)
	time.Sleep(10 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if len(started) != 1 {
		t.Errorf("OnTaskStart called %d times, want 1", len(started))
	}
	if len(completed) != 1 {
		t.Errorf("OnTaskComplete called %d times, want 1", len(completed))
	}
}
