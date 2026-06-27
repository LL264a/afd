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
	startOnce    sync.Once

	auth       *ClusterAuth
	clientsMu  sync.Mutex
	rpcClients map[string]*RPCClient
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

// SetClusterAuth 注入集群鉴权配置，dispatchTask 通过它构造 RPCClient。
// 必须在 Start 之前调用。
func (s *Scheduler) SetClusterAuth(auth *ClusterAuth) {
	s.auth = auth
}

// getOrCreateClient 按 addr 复用 RPCClient。同一地址变更时会自动新建连接。
func (s *Scheduler) getOrCreateClient(node *Node) *RPCClient {
	addr := node.FullAddr()
	s.clientsMu.Lock()
	defer s.clientsMu.Unlock()
	if s.rpcClients == nil {
		s.rpcClients = make(map[string]*RPCClient)
	}
	if c, ok := s.rpcClients[addr]; ok {
		return c
	}
	c := NewRPCClient(addr, s.auth)
	s.rpcClients[addr] = c
	return c
}

func (s *Scheduler) closeClients() {
	s.clientsMu.Lock()
	defer s.clientsMu.Unlock()
	for _, c := range s.rpcClients {
		c.Close()
	}
	s.rpcClients = nil
}

func (s *Scheduler) Start() {
	s.wg.Add(1)
	go s.dispatchLoop()
	s.logger.Infof("Scheduler started for node %s", s.localNodeID)
}

func (s *Scheduler) Stop() {
	s.cancel()
	s.wg.Wait()
	s.closeClients()
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

	// 本地节点：无需 RPC 投递，由本地下载管理器处理
	if targetNode.ID == s.localNodeID {
		s.logger.Debugw("task assigned to local node", "taskID", t.ID, "nodeID", targetNode.ID)
		return
	}

	// 通过 gRPC 投递任务到远端节点
	client := s.getOrCreateClient(targetNode)
	req := SubmitTaskRequest{
		URL:        t.URL,
		OutputPath: t.OutputPath,
		NodeID:     targetNode.ID,
	}
	var resp SubmitTaskResponse
	if err := client.Call("SubmitTask", req, &resp); err != nil {
		s.logger.Errorw("failed to dispatch task to remote node",
			"taskID", t.ID, "nodeID", targetNode.ID, "addr", targetNode.FullAddr(), "error", err)
		return
	}
	s.logger.Infow("task dispatched to remote node",
		"taskID", t.ID, "remoteTaskID", resp.TaskID, "nodeID", targetNode.ID)
}

func (s *Scheduler) selectNode() *Node {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var onlineNodes []*Node
	for _, node := range s.nodes {
		if node.IsOnline() && node.GetLoad() < 80 {
			onlineNodes = append(onlineNodes, node)
		}
	}
	// 不排除本地节点，单节点部署时也能调度
	if len(onlineNodes) == 0 {
		return nil
	}
	// 选择负载最低的节点
	sort.Slice(onlineNodes, func(i, j int) bool {
		return onlineNodes[i].GetLoad() < onlineNodes[j].GetLoad()
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
	avgTasks := float64(totalTasks) / float64(len(onlineNodes))

	for _, node := range onlineNodes {
		currentTasks := s.countTasksForNode(node.ID)
		if currentTasks > int(avgTasks)+2 {
			excess := currentTasks - int(avgTasks)
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
		if t.GetTargetNode() == nodeID {
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
		if t.GetTargetNode() == fromNodeID {
			// Round-robin across online nodes instead of dumping
			// everything onto the first available one.
			for i := 0; i < len(nodes); i++ {
				candidate := nodes[(nodeIdx+i)%len(nodes)]
				if candidate.ID != fromNodeID && candidate.IsOnline() {
					t.SetTargetNode(candidate.ID)
					nodeIdx = (nodeIdx + i + 1) % len(nodes)
					reassigned++
					break
				}
			}
		}
	}
}
