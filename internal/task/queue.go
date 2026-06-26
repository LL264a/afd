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
	if _, exists := q.tasks[task.ID]; exists {
		q.mu.Unlock()
		return fmt.Errorf("task %s already exists", task.ID)
	}

	q.tasks[task.ID] = task

	// Only enqueue when we cannot start immediately.  Otherwise the
	// heap would carry stale entries for already-running tasks that
	// tryStartNext has to pop and discard on every iteration.
	shouldStart := q.activeCount < q.maxConcurrent && task.GetStatus() == StatusPending
	if !shouldStart {
		heap.Push(&q.pq, &priorityItem{
			task:     task,
			priority: task.Priority,
		})
	}
	q.mu.Unlock()

	if shouldStart {
		q.startTask(task)
	}

	return nil
}

func (q *TaskQueue) Remove(id string) error {
	q.mu.Lock()
	task, exists := q.tasks[id]
	if !exists {
		q.mu.Unlock()
		return fmt.Errorf("task %s: %w", id, ErrTaskNotFound)
	}

	wasActive := task.GetStatus() == StatusDownloading
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
	q.mu.Unlock()

	task.Cancel() // 取消任务 context
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
	task, exists := q.tasks[id]
	if !exists {
		q.mu.Unlock()
		return fmt.Errorf("task %s: %w", id, ErrTaskNotFound)
	}

	if task.GetStatus() != StatusDownloading {
		q.mu.Unlock()
		return fmt.Errorf("task %s is not downloading", id)
	}

	task.SetStatus(StatusPaused)
	q.activeCount--
	q.mu.Unlock()

	q.tryStartNext()
	return nil
}

func (q *TaskQueue) Resume(id string) error {
	q.mu.Lock()
	task, exists := q.tasks[id]
	if !exists {
		q.mu.Unlock()
		return fmt.Errorf("task %s: %w", id, ErrTaskNotFound)
	}

	if task.GetStatus() != StatusPaused {
		q.mu.Unlock()
		return fmt.Errorf("task %s is not paused", id)
	}

	task.SetStatus(StatusPending)

	shouldStart := q.activeCount < q.maxConcurrent
	if !shouldStart {
		// No free slot: enqueue so tryStartNext can pick it up
		// when a slot opens.  Without this the task sits in
		// StatusPending forever.
		heap.Push(&q.pq, &priorityItem{
			task:     task,
			priority: task.Priority,
		})
	}
	q.mu.Unlock()

	if shouldStart {
		q.startTask(task)
	}

	return nil
}

func (q *TaskQueue) Get(id string) (*Task, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()

	task, exists := q.tasks[id]
	if !exists {
		return nil, fmt.Errorf("task %s: %w", id, ErrTaskNotFound)
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
	q.maxConcurrent = max
	q.mu.Unlock()
	q.tryStartNext()
}

func (q *TaskQueue) tryStartNext() {
	tasksToStart := []*Task{}
	q.mu.Lock()
	for q.activeCount < q.maxConcurrent && q.pq.Len() > 0 {
		item := heap.Pop(&q.pq).(*priorityItem)
		task := item.task

		// Defensive: with the Add/Resume fixes the heap only carries
		// Pending tasks, but a concurrent Remove could have flipped
		// the status between Pop and the lock-held dispatch.
		if task.GetStatus() != StatusPending {
			continue
		}

		task.SetStatus(StatusDownloading)
		q.activeCount++
		tasksToStart = append(tasksToStart, task)
	}
	q.mu.Unlock()

	// 释放锁后执行回调
	for _, t := range tasksToStart {
		if q.OnTaskStart != nil {
			q.OnTaskStart(t)
		}
	}
}

func (q *TaskQueue) startTask(task *Task) {
	q.mu.Lock()
	task.SetStatus(StatusDownloading)
	q.activeCount++
	q.mu.Unlock()

	if q.OnTaskStart != nil {
		q.OnTaskStart(task)
	}
}

func (q *TaskQueue) CompleteTask(id string) {
	q.mu.Lock()
	task, exists := q.tasks[id]
	if !exists {
		q.mu.Unlock()
		return
	}
	// 只有下载中的任务才能完成，防止重复调用导致 activeCount 重复递减
	if task.GetStatus() != StatusDownloading {
		q.mu.Unlock()
		return
	}
	task.SetStatus(StatusDone)
	q.activeCount--
	if q.activeCount < 0 {
		q.activeCount = 0
	}
	delete(q.tasks, id)
	q.mu.Unlock()

	if q.OnTaskComplete != nil {
		q.OnTaskComplete(task)
	}

	q.tryStartNext()
}

func (q *TaskQueue) FailTask(id string, errMsg string) {
	q.mu.Lock()
	task, exists := q.tasks[id]
	if !exists {
		q.mu.Unlock()
		return
	}
	// 只有下载中的任务才能失败，防止重复调用导致 activeCount 重复递减
	if task.GetStatus() != StatusDownloading {
		q.mu.Unlock()
		return
	}
	task.SetError(errMsg)
	q.activeCount--
	if q.activeCount < 0 {
		q.activeCount = 0
	}
	delete(q.tasks, id)
	q.mu.Unlock()

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

// PurgeStopped removes all tasks with Done/Failed/Cancelled status from
// the queue and returns the number of removed tasks.
func (q *TaskQueue) PurgeStopped() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	count := 0
	for id, t := range q.tasks {
		st := t.GetStatus()
		if st == StatusDone || st == StatusFailed || st == StatusCancelled {
			delete(q.tasks, id)
			if idx := findPQIndex(q.pq, id); idx >= 0 {
				heap.Remove(&q.pq, idx)
			}
			count++
		}
	}
	return count
}

// RemoveStopped removes a single stopped (Done/Failed/Cancelled) task from
// the queue. Returns an error if the task is not found or still active.
func (q *TaskQueue) RemoveStopped(id string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	task, exists := q.tasks[id]
	if !exists {
		return fmt.Errorf("task %s: %w", id, ErrTaskNotFound)
	}
	st := task.GetStatus()
	if st == StatusDownloading || st == StatusPending {
		return fmt.Errorf("task %s is still active", id)
	}
	if st == StatusPaused {
		return fmt.Errorf("task %s is paused, cannot remove as download result", id)
	}
	delete(q.tasks, id)
	if idx := findPQIndex(q.pq, id); idx >= 0 {
		heap.Remove(&q.pq, idx)
	}
	return nil
}

// MaxConcurrent returns the current maximum concurrent download limit.
func (q *TaskQueue) MaxConcurrent() int {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.maxConcurrent
}

// ChangePosition adjusts the position of a waiting task in the queue.
// how can be "POS_SET" (absolute), "POS_CUR" (relative to current), or
// "POS_END" (relative to end). Returns the new position.
//
// The priority queue is a heap ordered by priority, so a true reorder
// requires draining the heap, moving the target item to its new slot,
// reassigning priorities to match the new order, and rebuilding the heap.
func (q *TaskQueue) ChangePosition(id string, pos int, how string) (int, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	task, exists := q.tasks[id]
	if !exists {
		return 0, fmt.Errorf("task %s: %w", id, ErrTaskNotFound)
	}
	if task.GetStatus() != StatusPending {
		return 0, fmt.Errorf("task %s is not in waiting queue", id)
	}

	// Drain the heap into a slice ordered by current priority (pop order).
	items := make([]*priorityItem, 0, q.pq.Len())
	for q.pq.Len() > 0 {
		items = append(items, heap.Pop(&q.pq).(*priorityItem))
	}

	// Locate the target item's current position in priority order.
	curPos := -1
	for i, item := range items {
		if item.task.ID == id {
			curPos = i
			break
		}
	}
	if curPos < 0 {
		// Defensive: task is Pending but not in the heap. Restore the heap
		// before reporting the error so the queue stays usable.
		for _, item := range items {
			heap.Push(&q.pq, item)
		}
		return 0, fmt.Errorf("task %s not found in waiting queue", id)
	}

	newPos := pos
	switch how {
	case "POS_SET":
		newPos = pos
	case "POS_CUR":
		newPos = curPos + pos
	case "POS_END":
		newPos = len(items) - 1 + pos
	default:
		for _, item := range items {
			heap.Push(&q.pq, item)
		}
		return 0, fmt.Errorf("invalid how parameter: %s", how)
	}

	if newPos < 0 {
		newPos = 0
	}
	if newPos >= len(items) {
		newPos = len(items) - 1
	}

	// Shift elements to move the item from curPos to newPos.
	moved := items[curPos]
	if newPos > curPos {
		copy(items[curPos:newPos], items[curPos+1:newPos+1])
	} else {
		copy(items[newPos+1:curPos+1], items[newPos:curPos])
	}
	items[newPos] = moved

	// Reassign priorities to match the new linear order and rebuild the heap.
	// Using the slice index as the priority guarantees the heap pop order
	// reflects the requested position.
	for i, item := range items {
		item.priority = i
		item.task.Priority = i
		heap.Push(&q.pq, item)
	}

	return newPos, nil
}
