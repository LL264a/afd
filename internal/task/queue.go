package task

import (
	"container/heap"
	"fmt"
	"sync"
)

type TaskCallback func(t *Task)

type priorityItem struct {
	task     *Task
	priority int
	index    int
}

type priorityQueue []*priorityItem

func (pq priorityQueue) Len() int { return len(pq) }

func (pq priorityQueue) Less(i, j int) bool {
	return pq[i].priority < pq[j].priority
}

func (pq priorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *priorityQueue) Push(x any) {
	n := len(*pq)
	item := x.(*priorityItem)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *priorityQueue) Pop() any {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = -1
	*pq = old[0 : n-1]
	return item
}

type TaskQueue struct {
	mu             sync.RWMutex
	tasks          map[string]*Task
	pq             priorityQueue
	maxConcurrent  int
	activeCount    int
	OnTaskStart    TaskCallback
	OnTaskProgress TaskCallback
	OnTaskComplete TaskCallback
	OnTaskError    TaskCallback
}

func NewTaskQueue(maxConcurrent int) *TaskQueue {
	q := &TaskQueue{
		tasks:         make(map[string]*Task),
		pq:            make(priorityQueue, 0),
		maxConcurrent: maxConcurrent,
	}
	heap.Init(&q.pq)
	return q
}

func (q *TaskQueue) Add(task *Task) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if _, exists := q.tasks[task.ID]; exists {
		return fmt.Errorf("task %s already exists", task.ID)
	}

	q.tasks[task.ID] = task

	// Only enqueue when we cannot start immediately.  Otherwise the
	// heap would carry stale entries for already-running tasks that
	// tryStartNext has to pop and discard on every iteration.
	if q.activeCount < q.maxConcurrent && task.GetStatus() == StatusPending {
		q.startTask(task)
	} else {
		heap.Push(&q.pq, &priorityItem{
			task:     task,
			priority: task.Priority,
		})
	}

	return nil
}

func (q *TaskQueue) Remove(id string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	task, exists := q.tasks[id]
	if !exists {
		return fmt.Errorf("task %s not found", id)
	}

	wasActive := task.GetStatus() == StatusDownloading
	task.SetStatus(StatusCancelled)
	delete(q.tasks, id)

	// Find the matching heap entry and remove it.  We must pass
	// priorityItem.index (assigned by container/heap) to heap.Remove,
	// not the slice index from the for-range loop — otherwise the
	// heap's internal invariant is corrupted and a subsequent Pop
	// can panic with index out of range.
	if idx := findPQIndex(q.pq, id); idx >= 0 {
		heap.Remove(&q.pq, idx)
	}

	if wasActive {
		q.activeCount--
	}

	return nil
}

func findPQIndex(pq []*priorityItem, id string) int {
	for _, item := range pq {
		if item.task.ID == id {
			return item.index
		}
	}
	return -1
}

func (q *TaskQueue) Pause(id string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	task, exists := q.tasks[id]
	if !exists {
		return fmt.Errorf("task %s not found", id)
	}

	if task.GetStatus() != StatusDownloading {
		return fmt.Errorf("task %s is not downloading", id)
	}

	task.SetStatus(StatusPaused)
	q.activeCount--
	q.tryStartNext()
	return nil
}

func (q *TaskQueue) Resume(id string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	task, exists := q.tasks[id]
	if !exists {
		return fmt.Errorf("task %s not found", id)
	}

	if task.GetStatus() != StatusPaused {
		return fmt.Errorf("task %s is not paused", id)
	}

	task.SetStatus(StatusPending)

	if q.activeCount < q.maxConcurrent {
		q.startTask(task)
	} else {
		// No free slot: enqueue so tryStartNext can pick it up
		// when a slot opens.  Without this the task sits in
		// StatusPending forever.
		heap.Push(&q.pq, &priorityItem{
			task:     task,
			priority: task.Priority,
		})
	}

	return nil
}

func (q *TaskQueue) Get(id string) (*Task, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()

	task, exists := q.tasks[id]
	if !exists {
		return nil, fmt.Errorf("task %s not found", id)
	}

	return task, nil
}

func (q *TaskQueue) List() []*Task {
	q.mu.RLock()
	defer q.mu.RUnlock()

	list := make([]*Task, 0, len(q.tasks))
	for _, t := range q.tasks {
		list = append(list, t)
	}
	return list
}

func (q *TaskQueue) ActiveCount() int {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.activeCount
}

func (q *TaskQueue) TotalCount() int {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return len(q.tasks)
}

func (q *TaskQueue) SetMaxConcurrent(max int) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.maxConcurrent = max
}

func (q *TaskQueue) tryStartNext() {
	for q.activeCount < q.maxConcurrent && q.pq.Len() > 0 {
		item := heap.Pop(&q.pq).(*priorityItem)
		task := item.task

		// Defensive: with the Add/Resume fixes the heap only carries
		// Pending tasks, but a concurrent Remove could have flipped
		// the status between Pop and the lock-held dispatch.
		if task.GetStatus() != StatusPending {
			continue
		}

		q.activeCount++
		task.SetStatus(StatusDownloading)
		if q.OnTaskStart != nil {
			q.OnTaskStart(task)
		}
	}
}

func (q *TaskQueue) startTask(task *Task) {
	q.activeCount++
	task.SetStatus(StatusDownloading)
	if q.OnTaskStart != nil {
		q.OnTaskStart(task)
	}
}

func (q *TaskQueue) CompleteTask(id string) {
	q.mu.Lock()
	defer q.mu.Unlock()

	task, exists := q.tasks[id]
	if !exists {
		return
	}

	wasActive := task.GetStatus() == StatusDownloading
	task.SetStatus(StatusDone)

	if wasActive {
		q.activeCount--
	}

	if q.OnTaskComplete != nil {
		q.OnTaskComplete(task)
	}

	q.tryStartNext()
}

func (q *TaskQueue) FailTask(id string, errMsg string) {
	q.mu.Lock()
	defer q.mu.Unlock()

	task, exists := q.tasks[id]
	if !exists {
		return
	}

	wasActive := task.GetStatus() == StatusDownloading
	task.SetError(errMsg)

	if wasActive {
		q.activeCount--
	}

	if q.OnTaskError != nil {
		q.OnTaskError(task)
	}

	q.tryStartNext()
}

func (q *TaskQueue) NotifyProgress(task *Task) {
	if q.OnTaskProgress != nil {
		q.OnTaskProgress(task)
	}
}
