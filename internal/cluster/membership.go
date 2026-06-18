package cluster

import (
	"fmt"
	"sync"
	"time"

	"github.com/nexus-dl/afd/pkg/logger"
)

type ClusterEventType string

const (
	EventNodeJoin   ClusterEventType = "node_join"
	EventNodeLeave  ClusterEventType = "node_leave"
	EventNodeUpdate ClusterEventType = "node_update"
)

type ClusterEvent struct {
	Type      ClusterEventType
	Node      Node
	Timestamp time.Time
}

type EventHandler func(event ClusterEvent)

type Membership struct {
	mu       sync.RWMutex
	members  map[string]*Node
	events   chan ClusterEvent
	handlers []EventHandler
	localID  string
	stopCh   chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup
}

func NewMembership(localID string) *Membership {
	m := &Membership{
		members: make(map[string]*Node),
		events:  make(chan ClusterEvent, 256),
		localID: localID,
		stopCh:  make(chan struct{}),
	}
	m.wg.Add(1)
	go m.eventLoop()
	return m
}

func (m *Membership) eventLoop() {
	defer m.wg.Done()
	for event := range m.events {
		m.mu.RLock()
		handlers := make([]EventHandler, len(m.handlers))
		copy(handlers, m.handlers)
		m.mu.RUnlock()

		for _, handler := range handlers {
			func() {
				defer func() {
					if r := recover(); r != nil {
						logger.Log.Errorw("cluster event handler panic",
							"error", fmt.Sprintf("%v", r),
							"event_type", event.Type,
						)
					}
				}()
				handler(event)
			}()
		}
	}
}

func (m *Membership) AddMember(node Node) {
	m.mu.Lock()
	existing, ok := m.members[node.ID]
	if ok {
		existing.Addr = node.Addr
		existing.GRPCPort = node.GRPCPort
		existing.APIPort = node.APIPort
		existing.Load = node.Load
		existing.Tags = node.Tags
		existing.UpdateSeen()
		m.mu.Unlock()
		m.emitEvent(ClusterEvent{
			Type:      EventNodeUpdate,
			Node:      *existing,
			Timestamp: time.Now(),
		})
		return
	}
	node.UpdateSeen()
	m.members[node.ID] = &node
	m.mu.Unlock()

	logger.Log.Infow("Node joined cluster",
		"node_id", node.ID,
		"name", node.Name,
		"addr", node.FullAddr(),
	)

	m.emitEvent(ClusterEvent{
		Type:      EventNodeJoin,
		Node:      node,
		Timestamp: time.Now(),
	})
}

func (m *Membership) RemoveMember(nodeID string) {
	m.mu.Lock()
	node, ok := m.members[nodeID]
	if !ok {
		m.mu.Unlock()
		return
	}
	node.MarkOffline()
	delete(m.members, nodeID)
	m.mu.Unlock()

	logger.Log.Infow("Node left cluster", "node_id", nodeID)

	m.emitEvent(ClusterEvent{
		Type:      EventNodeLeave,
		Node:      *node,
		Timestamp: time.Now(),
	})
}

func (m *Membership) GetMember(nodeID string) (Node, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	node, ok := m.members[nodeID]
	if !ok {
		return Node{}, false
	}
	return *node, true // 返回值拷贝，避免外部修改导致数据竞争
}

func (m *Membership) Members() []Node {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]Node, 0, len(m.members))
	for _, node := range m.members {
		result = append(result, *node)
	}
	return result
}

func (m *Membership) MemberCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.members)
}

func (m *Membership) OnlineMembers() []Node {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]Node, 0, len(m.members))
	for _, node := range m.members {
		if node.IsOnline() {
			result = append(result, *node)
		}
	}
	return result
}

func (m *Membership) OnlineCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	count := 0
	for _, node := range m.members {
		if node.IsOnline() {
			count++
		}
	}
	return count
}

func (m *Membership) MergeMembers(nodes []Node) {
	for _, node := range nodes {
		if node.ID == m.localID {
			continue
		}
		m.AddMember(node)
	}
}

func (m *Membership) GetSnapshot() []Node {
	return m.Members()
}

func (m *Membership) CheckTimeouts(timeout time.Duration) []string {
	m.mu.Lock()
	defer m.mu.Unlock()

	var expired []string
	now := time.Now()
	for id, node := range m.members {
		if node.IsOnline() && now.Sub(node.LastSeen) > timeout {
			node.MarkOffline()
			expired = append(expired, id)
		}
	}

	for _, id := range expired {
		node := *m.members[id]
		delete(m.members, id)
		m.emitEvent(ClusterEvent{
			Type:      EventNodeLeave,
			Node:      node,
			Timestamp: now,
		})
	}

	return expired
}

func (m *Membership) OnEvent(handler EventHandler) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.handlers = append(m.handlers, handler)
}

func (m *Membership) emitEvent(event ClusterEvent) {
	defer func() {
		// 防止 Shutdown 关闭 events channel 后发送导致 panic
		recover()
	}()
	select {
	case m.events <- event:
	default:
		logger.Log.Warnw("Cluster event channel full, dropping event",
			"type", event.Type,
			"node_id", event.Node.ID,
		)
	}
}

func (m *Membership) Shutdown() {
	m.stopOnce.Do(func() {
		close(m.events)
		m.wg.Wait()
	})
}
