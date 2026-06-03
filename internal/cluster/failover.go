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
	NodeID      string
	FailedAt    time.Time
	RetryCount  int
	Tasks       []string
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
	defer f.mu.Unlock()

	nodes := f.scheduler.GetAllNodes()
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
			f.handleNodeFailure(node.ID)
		}
	}
}

func (f *Failover) handleNodeFailure(nodeID string) {
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
		delete(f.failedNodes, nodeID)
		return
	}

	f.logger.Infof("Scheduling task reassignment for node %s in %v", nodeID, f.config.ReassignDelay)
	time.AfterFunc(f.config.ReassignDelay, func() {
		// Bail out if the failover has been stopped; otherwise we
		// do wasted work and may touch a torn-down scheduler.
		select {
		case <-f.ctx.Done():
			return
		default:
		}
		f.reassignTasks(nodeID)
	})
}

func (f *Failover) getTasksForNode(nodeID string) []string {
	return f.scheduler.GetTasksForNode(nodeID)
}

func (f *Failover) reassignTasks(nodeID string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	failed, exists := f.failedNodes[nodeID]
	if !exists {
		return
	}

	onlineNodes := f.scheduler.GetOnlineNodes()
	if len(onlineNodes) == 0 {
		f.logger.Warnf("No online nodes available for task reassignment")
		return
	}

	nodeIdx := 0
	for i, taskID := range failed.Tasks {
		targetNode := onlineNodes[(nodeIdx+i)%len(onlineNodes)]
		f.logger.Infof("Reassigning task %s from node %s to node %s", taskID, nodeID, targetNode.ID)

		if f.taskReassign != nil {
			if err := f.taskReassign(taskID, targetNode.ID); err != nil {
				f.logger.Errorf("Failed to reassign task %s: %v", taskID, err)
				continue
			}
		}

		t, ok := f.scheduler.GetTask(taskID)
		if ok {
			t.TargetNode = targetNode.ID
		}
		nodeIdx = (nodeIdx + 1) % len(onlineNodes)
	}

	delete(f.failedNodes, nodeID)
	f.logger.Infof("Task reassignment completed for failed node %s", nodeID)
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