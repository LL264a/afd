package cluster

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/nexus-dl/afd/pkg/config"
	"github.com/nexus-dl/afd/pkg/logger"
	"go.uber.org/zap"
)

type NodeState struct {
	NodeID      string            `json:"node_id"`
	TaskCount   int               `json:"task_count"`
	Load        int               `json:"load"`
	Status      NodeStatus        `json:"status"`
	LastSync    time.Time         `json:"last_sync"`
	ActiveTasks map[string]string `json:"active_tasks"`
}

type ClusterState struct {
	Nodes    map[string]*NodeState `json:"nodes"`
	Tasks    map[string]string     `json:"tasks"`
	Version  int64                 `json:"version"`
	Updated  time.Time             `json:"updated"`
}

type SyncConfig struct {
	SyncInterval time.Duration
	MaxRetries   int
	RetryDelay   time.Duration
}

type StateSync struct {
	mu           sync.RWMutex
	config       *SyncConfig
	localNodeID  string
	logger       *zap.SugaredLogger
	clusterState *ClusterState
	stateChan    chan *ClusterState
	wg           sync.WaitGroup
	ctx          context.Context
	cancel       context.CancelFunc
	syncPeers    func(state *ClusterState) error
}

func NewStateSync(localNodeID string, cfg *config.Config) *StateSync {
	ctx, cancel := context.WithCancel(context.Background())
	return &StateSync{
		config: &SyncConfig{
			SyncInterval: 10 * time.Second,
			MaxRetries:   3,
			RetryDelay:   2 * time.Second,
		},
		localNodeID:  localNodeID,
		logger:       logger.Log,
		clusterState: &ClusterState{
			Nodes:    make(map[string]*NodeState),
			Tasks:    make(map[string]string),
			Version:  0,
			Updated:  time.Now(),
		},
		stateChan: make(chan *ClusterState, 10),
		ctx:       ctx,
		cancel:    cancel,
	}
}

func (s *StateSync) SetSyncPeersFn(fn func(state *ClusterState) error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.syncPeers = fn
}

func (s *StateSync) Start() {
	s.wg.Add(1)
	go s.syncLoop()
	s.logger.Infof("State sync started for node %s", s.localNodeID)
}

func (s *StateSync) Stop() {
	s.cancel()
	s.wg.Wait()
	close(s.stateChan)
	s.logger.Infof("State sync stopped for node %s", s.localNodeID)
}

func (s *StateSync) GetClusterState() *ClusterState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	state := *s.clusterState
	state.Nodes = make(map[string]*NodeState)
	for k, v := range s.clusterState.Nodes {
		nodeState := *v
		state.Nodes[k] = &nodeState
	}
	return &state
}

func (s *StateSync) UpdateLocalState(taskCount, load int, activeTasks map[string]string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.clusterState.Nodes[s.localNodeID] = &NodeState{
		NodeID:      s.localNodeID,
		TaskCount:   taskCount,
		Load:        load,
		Status:      StatusOnline,
		LastSync:    time.Now(),
		ActiveTasks: activeTasks,
	}
	s.clusterState.Version++
	s.clusterState.Updated = time.Now()
}

func (s *StateSync) UpdateTaskMapping(taskID, nodeID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.clusterState.Tasks[taskID] = nodeID
	s.clusterState.Version++
	s.clusterState.Updated = time.Now()
}

func (s *StateSync) RemoveTaskMapping(taskID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.clusterState.Tasks, taskID)
	s.clusterState.Version++
	s.clusterState.Updated = time.Now()
}

func (s *StateSync) GetTaskNode(taskID string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	nodeID, ok := s.clusterState.Tasks[taskID]
	return nodeID, ok
}

func (s *StateSync) syncLoop() {
	defer s.wg.Done()
	ticker := time.NewTicker(s.config.SyncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case state := <-s.stateChan:
			s.broadcastState(state)
		case <-ticker.C:
			s.performSync()
		}
	}
}

func (s *StateSync) performSync() {
	s.mu.RLock()
	state := s.clusterState
	s.mu.RUnlock()

	if s.syncPeers != nil {
		err := s.syncWithRetry(state)
		if err != nil {
			s.logger.Warnf("Failed to sync state with peers: %v", err)
			return
		}
	}

	s.logger.Debugf("State sync completed, version: %d", state.Version)
}

func (s *StateSync) syncWithRetry(state *ClusterState) error {
	var lastErr error
	for i := 0; i < s.config.MaxRetries; i++ {
		if err := s.syncPeers(state); err != nil {
			lastErr = err
			s.logger.Warnf("Sync attempt %d failed: %v", i+1, err)
			time.Sleep(s.config.RetryDelay)
			continue
		}
		return nil
	}
	return lastErr
}

func (s *StateSync) broadcastState(state *ClusterState) {
	if s.syncPeers != nil {
		go func() {
			if err := s.syncPeers(state); err != nil {
				s.logger.Warnf("Failed to broadcast state: %v", err)
			}
		}()
	}
}

func (s *StateSync) MergeState(remoteState *ClusterState) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if remoteState.Version <= s.clusterState.Version {
		return
	}

	for nodeID, remoteNode := range remoteState.Nodes {
		localNode, exists := s.clusterState.Nodes[nodeID]
		if !exists || remoteNode.LastSync.After(localNode.LastSync) {
			s.clusterState.Nodes[nodeID] = remoteNode
		}
	}

	for taskID, nodeID := range remoteState.Tasks {
		if _, exists := s.clusterState.Tasks[taskID]; !exists {
			s.clusterState.Tasks[taskID] = nodeID
		}
	}

	s.clusterState.Version = remoteState.Version
	s.clusterState.Updated = time.Now()

	s.logger.Debugf("Merged remote state, new version: %d", s.clusterState.Version)
}

func (s *StateSync) SerializeState() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return json.Marshal(s.clusterState)
}

func (s *StateSync) DeserializeState(data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return json.Unmarshal(data, &s.clusterState)
}

func (s *StateSync) GetNodeState(nodeID string) (*NodeState, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	state, ok := s.clusterState.Nodes[nodeID]
	return state, ok
}

func (s *StateSync) GetAllNodeStates() []*NodeState {
	s.mu.RLock()
	defer s.mu.RUnlock()

	states := make([]*NodeState, 0, len(s.clusterState.Nodes))
	for _, state := range s.clusterState.Nodes {
		states = append(states, state)
	}
	return states
}

func (s *StateSync) ResolveTaskConflict(taskID, preferredNodeID string) string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	currentNode, exists := s.clusterState.Tasks[taskID]
	if !exists {
		return preferredNodeID
	}

	nodeState, nodeExists := s.clusterState.Nodes[currentNode]
	if !nodeExists || nodeState.Status != StatusOnline {
		return preferredNodeID
	}

	return currentNode
}