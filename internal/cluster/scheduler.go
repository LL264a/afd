package cluster

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/nexus-dl/afd/internal/task"
	"github.com/nexus-dl/afd/pkg/config"
	"github.com/nexus-dl/afd/pkg/logger"
	"go.uber.org/zap"
)

type Scheduler struct {
	mu           sync.RWMutex
	nodes        map[string]*Node
	taskQueue    map[string]*task.Task
	localNodeID  string
	config       *config.Config
	logger       *zap.SugaredLogger
	dispatchChan chan *task.Task
	wg           sync.WaitGroup
	ctx          context.Context
	cancel       context.CancelFunc
}

func NewScheduler(localNodeID string, cfg *config.Config) *Scheduler {
	ctx, cancel := context.WithCancel(context.Background())
	return &Scheduler{
		nodes:        make(map[string]*Node),
		taskQueue:    make(map[string]*task.Task),
		localNodeID:  localNodeID,
		config:       cfg,
		logger:       logger.Log,
		dispatchChan: make(chan *task.Task, 1024),
		ctx:          ctx,
		cancel:       cancel,
	}
}

func (s *Scheduler) Start() {
	s.wg.Add(1)
	go s.dispatchLoop()
	s.logger.Infof("Scheduler started for node %s", s.localNodeID)
}

func (s *Scheduler) Stop() {
	s.cancel()
	s.wg.Wait()
	// 不关闭 dispatchChan，避免 AfterFunc 回调向已关闭 channel 发送导致 panic
	s.logger.Infof("Scheduler stopped for node %s", s.localNodeID)
}

func (s *Scheduler) AddNode(node *Node) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nodes[node.ID] = node
	s.logger.Debugf("Node added to scheduler: %s", node.ID)
}

func (s *Scheduler) RemoveNode(nodeID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.nodes, nodeID)
	s.logger.Debugf("Node removed from scheduler: %s", nodeID)
}

func (s *Scheduler) UpdateNode(node *Node) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nodes[node.ID] = node
}

func (s *Scheduler) GetNode(nodeID string) (*Node, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	node, ok := s.nodes[nodeID]
	return node, ok
}

func (s *Scheduler) GetAllNodes() []*Node {
	s.mu.RLock()
	defer s.mu.RUnlock()
	nodes := make([]*Node, 0, len(s.nodes))
	for _, node := range s.nodes {
		nodes = append(nodes, node)
	}
	return nodes
}

func (s *Scheduler) GetOnlineNodes() []*Node {
	s.mu.RLock()
	defer s.mu.RUnlock()
	nodes := make([]*Node, 0)
	for _, node := range s.nodes {
		if node.IsOnline() {
			nodes = append(nodes, node)
		}
	}
	return nodes
}

func (s *Scheduler) SubmitTask(t *task.Task) error {
	s.mu.Lock()
	s.taskQueue[t.ID] = t
	s.mu.Unlock()

	select {
	case s.dispatchChan <- t:
	case <-s.ctx.Done():
		s.mu.Lock()
		delete(s.taskQueue, t.ID)
		s.mu.Unlock()
		return fmt.Errorf("scheduler is stopped")
	default:
		s.logger.Warnf("Dispatch channel full, task %s will be picked up later", t.ID)
	}
	s.logger.Infof("Task %s submitted to scheduler", t.ID)
	return nil
}

func (s *Scheduler) RemoveTask(taskID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.taskQueue, taskID)
}

func (s *Scheduler) GetTask(taskID string) (*task.Task, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	t, ok := s.taskQueue[taskID]
	return t, ok
}

func (s *Scheduler) dispatchLoop() {
	defer s.wg.Done()
	for {
		select {
		case <-s.ctx.Done():
			return
		case t := <-s.dispatchChan:
			s.dispatchTask(t)
		}
	}
}

func (s *Scheduler) dispatchTask(t *task.Task) {
	targetNode := s.selectNode()
	if targetNode == nil {
		s.logger.Warnf("No suitable node found for task %s, requeuing", t.ID)
		time.AfterFunc(5*time.Second, func() {
			// Bail out if the scheduler has been stopped.  Sending
			// to dispatchChan after Stop would either block (buffer
			// full) or panic (channel closed, but we no longer
			// close it — still, guard the send to be safe under
			// any future change to Stop).
			select {
			case <-s.ctx.Done():
				return
			default:
			}
			select {
			case s.dispatchChan <- t:
			case <-s.ctx.Done():
			default:
				s.logger.Warnf("Failed to requeue task %s", t.ID)
			}
		})
		return
	}

	s.logger.Infof("Dispatching task %s to node %s", t.ID, targetNode.ID)
	t.SetTargetNode(targetNode.ID)
}

func (s *Scheduler) selectNode() *Node {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var onlineNodes []*Node
	for _, node := range s.nodes {
		if node.IsOnline() && node.Load < 80 {
			onlineNodes = append(onlineNodes, node)
		}
	}
	// 不排除本地节点，单节点部署时也能调度
	if len(onlineNodes) == 0 {
		return nil
	}
	// 选择负载最低的节点
	sort.Slice(onlineNodes, func(i, j int) bool {
		return onlineNodes[i].Load < onlineNodes[j].Load
	})
	return onlineNodes[0]
}

func (s *Scheduler) GetTaskCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.taskQueue)
}

func (s *Scheduler) Rebalance() {
	s.mu.Lock()
	defer s.mu.Unlock()

	onlineNodes := make([]*Node, 0)
	for _, node := range s.nodes {
		if node.IsOnline() {
			onlineNodes = append(onlineNodes, node)
		}
	}
	if len(onlineNodes) == 0 {
		return
	}

	totalTasks := len(s.taskQueue)
	avgTasks := totalTasks / len(onlineNodes)

	for _, node := range onlineNodes {
		currentTasks := s.countTasksForNode(node.ID)
		if currentTasks > avgTasks+2 {
			excess := currentTasks - avgTasks
			s.reassignTasks(node.ID, excess, onlineNodes)
		}
	}
}

func (s *Scheduler) countTasksForNode(nodeID string) int {
	count := 0
	for _, t := range s.taskQueue {
		if t.TargetNode == nodeID {
			count++
		}
	}
	return count
}

func (s *Scheduler) GetTasksForNode(nodeID string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	taskIDs := make([]string, 0)
	for id, t := range s.taskQueue {
		if t.TargetNode == nodeID {
			taskIDs = append(taskIDs, id)
		}
	}
	return taskIDs
}

func (s *Scheduler) reassignTasks(fromNodeID string, count int, nodes []*Node) {
	reassigned := 0
	nodeIdx := 0
	for _, t := range s.taskQueue {
		if reassigned >= count {
			break
		}
		if t.TargetNode == fromNodeID {
			// Round-robin across online nodes instead of dumping
			// everything onto the first available one.
			for i := 0; i < len(nodes); i++ {
				candidate := nodes[(nodeIdx+i)%len(nodes)]
				if candidate.ID != fromNodeID && candidate.IsOnline() {
					t.TargetNode = candidate.ID
					nodeIdx = (nodeIdx + i + 1) % len(nodes)
					reassigned++
					break
				}
			}
		}
	}
}
