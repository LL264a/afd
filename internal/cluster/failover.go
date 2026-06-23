package cluster

import (
	"context"
	"sync"
	"time"

	"github.com/nexus-dl/afd/pkg/config"
	"github.com/nexus-dl/afd/pkg/logger"
	"go.uber.org/zap"
)

type FailoverConfig struct {
	HeartbeatInterval time.Duration
	HeartbeatTimeout  time.Duration
	MaxRetries        int
	ReassignDelay     time.Duration
}

type FailedNode struct {
	NodeID            string
	FailedAt          time.Time
	RetryCount        int
	Tasks             []string
	reassignScheduled bool
}

type Failover struct {
	mu           sync.RWMutex
	config       *FailoverConfig
	scheduler    *Scheduler
	logger       *zap.SugaredLogger
	failedNodes  map[string]*FailedNode
	heartbeat    map[string]time.Time
	wg           sync.WaitGroup
	ctx          context.Context
	cancel       context.CancelFunc
	taskReassign func(taskID, newNodeID string) error
}

func NewFailover(cfg *config.Config, scheduler *Scheduler) *Failover {
	ctx, cancel := context.WithCancel(context.Background())
	return &Failover{
		config: &FailoverConfig{
			HeartbeatInterval: 5 * time.Second,
			HeartbeatTimeout:  15 * time.Second,
			MaxRetries:        3,
			ReassignDelay:     10 * time.Second,
		},
		scheduler:   scheduler,
		logger:      logger.Log,
		failedNodes: make(map[string]*FailedNode),
		heartbeat:   make(map[string]time.Time),
		ctx:         ctx,
		cancel:      cancel,
	}
}

func (f *Failover) SetTaskReassignFn(fn func(taskID, newNodeID string) error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.taskReassign = fn
}

func (f *Failover) Start() {
	f.wg.Add(1)
	go f.monitorLoop()
	f.logger.Infof("Failover monitor started")
}

func (f *Failover) Stop() {
	f.cancel()
	f.wg.Wait()
	f.logger.Infof("Failover monitor stopped")
}

func (f *Failover) RecordHeartbeat(nodeID string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.heartbeat[nodeID] = time.Now()
}

func (f *Failover) IsNodeHealthy(nodeID string) bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	lastHeartbeat, ok := f.heartbeat[nodeID]
	if !ok {
		return false
	}
	return time.Since(lastHeartbeat) < f.config.HeartbeatTimeout
}

func (f *Failover) monitorLoop() {
	defer f.wg.Done()
	ticker := time.NewTicker(f.config.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-f.ctx.Done():
			return
		case <-ticker.C:
			f.checkNodeHealth()
		}
	}
}

func (f *Failover) checkNodeHealth() {
	f.mu.Lock()
	nodes := f.scheduler.GetAllNodes()
	var pendingReassign [][]string
	for _, node := range nodes {
		if node.ID == "" {
			continue
		}

		lastHeartbeat, hasHeartbeat := f.heartbeat[node.ID]
		if !hasHeartbeat {
			continue
		}

		timeSinceHeartbeat := time.Since(lastHeartbeat)
		if timeSinceHeartbeat > f.config.HeartbeatTimeout {
			if tasks := f.handleNodeFailure(node.ID); len(tasks) > 0 {
				pendingReassign = append(pendingReassign, tasks)
			}
		}
	}
	f.mu.Unlock()

	// 释放锁后再执行任务重分配，避免在持有 f.mu 时调用外部回调
	for _, tasks := range pendingReassign {
		f.reassignTasks(tasks)
	}
}

func (f *Failover) handleNodeFailure(nodeID string) []string {
	f.logger.Warnf("Node %s failed, initiating failover", nodeID)

	failed, exists := f.failedNodes[nodeID]
	if !exists {
		f.failedNodes[nodeID] = &FailedNode{
			NodeID:   nodeID,
			FailedAt: time.Now(),
			Tasks:    f.getTasksForNode(nodeID),
		}
		failed = f.failedNodes[nodeID]
	}

	failed.RetryCount++

	if failed.RetryCount >= f.config.MaxRetries {
		f.logger.Errorf("Node %s failed after %d retries, removing permanently", nodeID, f.config.MaxRetries)
		f.scheduler.RemoveNode(nodeID)
		// 先保存任务列表
		tasksToReassign := failed.Tasks
		delete(f.failedNodes, nodeID)
		return tasksToReassign
	}

	if !failed.reassignScheduled {
		failed.reassignScheduled = true
		f.logger.Infof("Scheduling task reassignment for node %s in %v", nodeID, f.config.ReassignDelay)
		tasksToReassign := failed.Tasks
		time.AfterFunc(f.config.ReassignDelay, func() {
			// Bail out if the failover has been stopped; otherwise we
			// do wasted work and may touch a torn-down scheduler.
			select {
			case <-f.ctx.Done():
				return
			default:
			}
			f.reassignTasks(tasksToReassign)
		})
	} else {
		f.logger.Debugf("Node %s still failing (retry %d), AfterFunc already scheduled", nodeID, failed.RetryCount)
	}
	return nil
}

func (f *Failover) getTasksForNode(nodeID string) []string {
	return f.scheduler.GetTasksForNode(nodeID)
}

func (f *Failover) reassignTasks(taskIDs []string) {
	onlineNodes := f.scheduler.GetOnlineNodes()
	if len(onlineNodes) == 0 {
		f.logger.Warnw("No online nodes available for task reassignment")
		return
	}

	f.mu.RLock()
	reassign := f.taskReassign
	f.mu.RUnlock()

	type reassignItem struct {
		taskID string
		nodeID string
	}
	items := make([]reassignItem, 0, len(taskIDs))

	for i, taskID := range taskIDs {
		targetNode := onlineNodes[i%len(onlineNodes)]
		t, ok := f.scheduler.GetTask(taskID)
		if ok {
			t.SetTargetNode(targetNode.ID)
			items = append(items, reassignItem{taskID: taskID, nodeID: targetNode.ID})
		}
	}

	// 释放锁后执行外部回调（reassignTasks 自身不持有 f.mu）
	for _, item := range items {
		if reassign != nil {
			if err := reassign(item.taskID, item.nodeID); err != nil {
				f.logger.Errorf("Failed to reassign task %s to node %s: %v", item.taskID, item.nodeID, err)
			}
		}
	}
}

func (f *Failover) GetFailedNodes() []*FailedNode {
	f.mu.RLock()
	defer f.mu.RUnlock()

	failedList := make([]*FailedNode, 0, len(f.failedNodes))
	for _, fn := range f.failedNodes {
		failedList = append(failedList, fn)
	}
	return failedList
}

func (f *Failover) ClearFailedNode(nodeID string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.failedNodes, nodeID)
}

func (f *Failover) MarkNodeRecovered(nodeID string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if _, exists := f.failedNodes[nodeID]; exists {
		f.logger.Infof("Node %s recovered, clearing failure state", nodeID)
		delete(f.failedNodes, nodeID)
	}
	f.heartbeat[nodeID] = time.Now()
}
