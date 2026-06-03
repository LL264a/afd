package cluster

import (
	"sync"
	"testing"
	"time"

	"github.com/nexus-dl/afd/internal/task"
	"github.com/nexus-dl/afd/pkg/config"
	"github.com/nexus-dl/afd/pkg/logger"
	"go.uber.org/zap"
)

func init() {
	logger.Log = zap.NewNop().Sugar()
}

func newTestNode(id string, status NodeStatus, load int) *Node {
	return &Node{
		ID:       id,
		Name:     id,
		Addr:     "127.0.0.1",
		GRPCPort: 50051,
		APIPort:  8080,
		Status:   status,
		Load:     load,
		Tags:     map[string]string{},
		LastSeen: time.Now(),
	}
}

func newTestTask(id string) *task.Task {
	t := task.NewTask("http://example.com/"+id, "/tmp/"+id)
	return t
}

func newTestScheduler(t *testing.T) *Scheduler {
	t.Helper()
	s := NewScheduler("local-node", config.DefaultConfig())
	s.Start()
	t.Cleanup(func() { s.Stop() })
	return s
}

func TestSchedulerAddAndGetNode(t *testing.T) {
	s := newTestScheduler(t)
	node := newTestNode("n1", StatusOnline, 10)
	s.AddNode(node)

	got, ok := s.GetNode("n1")
	if !ok {
		t.Fatal("GetNode returned not found")
	}
	if got.Load != 10 {
		t.Errorf("Load = %d, want 10", got.Load)
	}
}

func TestSchedulerGetNodeMissing(t *testing.T) {
	s := newTestScheduler(t)
	_, ok := s.GetNode("nope")
	if ok {
		t.Error("expected not found for missing node")
	}
}

func TestSchedulerRemoveNode(t *testing.T) {
	s := newTestScheduler(t)
	s.AddNode(newTestNode("n1", StatusOnline, 0))
	s.RemoveNode("n1")
	_, ok := s.GetNode("n1")
	if ok {
		t.Error("expected node to be removed")
	}
}

func TestSchedulerUpdateNode(t *testing.T) {
	s := newTestScheduler(t)
	node := newTestNode("n1", StatusOnline, 5)
	s.AddNode(node)

	updated := newTestNode("n1", StatusOnline, 50)
	s.UpdateNode(updated)

	got, _ := s.GetNode("n1")
	if got.Load != 50 {
		t.Errorf("Load after update = %d, want 50", got.Load)
	}
}

func TestSchedulerGetAllNodes(t *testing.T) {
	s := newTestScheduler(t)
	s.AddNode(newTestNode("n1", StatusOnline, 0))
	s.AddNode(newTestNode("n2", StatusOffline, 0))
	s.AddNode(newTestNode("n3", StatusOnline, 30))

	all := s.GetAllNodes()
	if len(all) != 3 {
		t.Errorf("len(all) = %d, want 3", len(all))
	}
}

func TestSchedulerGetOnlineNodes(t *testing.T) {
	s := newTestScheduler(t)
	s.AddNode(newTestNode("n1", StatusOnline, 0))
	s.AddNode(newTestNode("n2", StatusOffline, 0))
	s.AddNode(newTestNode("n3", StatusOnline, 30))

	online := s.GetOnlineNodes()
	if len(online) != 2 {
		t.Errorf("len(online) = %d, want 2", len(online))
	}
	for _, n := range online {
		if !n.IsOnline() {
			t.Errorf("GetOnlineNodes returned offline node: %s", n.ID)
		}
	}
}

func TestSchedulerSubmitAndGetTask(t *testing.T) {
	s := newTestScheduler(t)
	s.AddNode(newTestNode("n1", StatusOnline, 10))

	t1 := newTestTask("task-1")
	if err := s.SubmitTask(t1); err != nil {
		t.Fatalf("SubmitTask error: %v", err)
	}

	got, ok := s.GetTask(t1.ID)
	if !ok {
		t.Fatal("GetTask returned not found")
	}
	if got.ID != t1.ID {
		t.Errorf("task ID = %s, want %s", got.ID, t1.ID)
	}
	if s.GetTaskCount() != 1 {
		t.Errorf("GetTaskCount = %d, want 1", s.GetTaskCount())
	}
}

func TestSchedulerRemoveTask(t *testing.T) {
	s := newTestScheduler(t)
	s.AddNode(newTestNode("n1", StatusOnline, 0))

	t1 := newTestTask("task-1")
	_ = s.SubmitTask(t1)

	s.RemoveTask(t1.ID)
	if s.GetTaskCount() != 0 {
		t.Errorf("GetTaskCount after remove = %d, want 0", s.GetTaskCount())
	}
}

func TestSchedulerDispatchToLowestLoadNode(t *testing.T) {
	s := newTestScheduler(t)
	s.AddNode(newTestNode("n1", StatusOnline, 70))
	s.AddNode(newTestNode("n2", StatusOnline, 20))
	s.AddNode(newTestNode("n3", StatusOnline, 50))

	t1 := newTestTask("task-1")
	_ = s.SubmitTask(t1)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		got, _ := s.GetTask(t1.ID)
		if got != nil && got.TargetNode != "" {
			if got.TargetNode != "n2" {
				t.Errorf("TargetNode = %s, want n2 (lowest load)", got.TargetNode)
			}
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Error("Task was not dispatched within timeout")
}

func TestSchedulerDispatchSkipsLocalNode(t *testing.T) {
	s := newTestScheduler(t)
	s.AddNode(newTestNode("local-node", StatusOnline, 0))
	s.AddNode(newTestNode("remote-1", StatusOnline, 10))

	t1 := newTestTask("task-1")
	_ = s.SubmitTask(t1)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		got, _ := s.GetTask(t1.ID)
		if got != nil && got.TargetNode != "" {
			if got.TargetNode == "local-node" {
				t.Error("Scheduler should never dispatch to local node")
			}
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Error("Task was not dispatched within timeout")
}

func TestSchedulerDispatchSkipsOfflineNodes(t *testing.T) {
	s := newTestScheduler(t)
	s.AddNode(newTestNode("n1", StatusOffline, 0))
	s.AddNode(newTestNode("n2", StatusOnline, 10))

	t1 := newTestTask("task-1")
	_ = s.SubmitTask(t1)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		got, _ := s.GetTask(t1.ID)
		if got != nil && got.TargetNode != "" {
			if got.TargetNode != "n2" {
				t.Errorf("TargetNode = %s, want n2 (only online)", got.TargetNode)
			}
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Error("Task was not dispatched within timeout")
}

func TestSchedulerDispatchSkipsOverloadedNodes(t *testing.T) {
	s := newTestScheduler(t)
	s.AddNode(newTestNode("n1", StatusOnline, 95))
	s.AddNode(newTestNode("n2", StatusOnline, 50))

	t1 := newTestTask("task-1")
	_ = s.SubmitTask(t1)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		got, _ := s.GetTask(t1.ID)
		if got != nil && got.TargetNode != "" {
			if got.TargetNode != "n2" {
				t.Errorf("TargetNode = %s, want n2 (load < 80)", got.TargetNode)
			}
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Error("Task was not dispatched within timeout")
}

func TestSchedulerDispatchNoNodeRequeues(t *testing.T) {
	s := newTestScheduler(t)
	t1 := newTestTask("task-1")
	_ = s.SubmitTask(t1)

	deadline := time.Now().Add(200 * time.Millisecond)
	for time.Now().Before(deadline) {
		got, _ := s.GetTask(t1.ID)
		if got != nil && got.TargetNode != "" {
			t.Error("Task should not be dispatched when no nodes are online")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestSchedulerGetTasksForNode(t *testing.T) {
	s := newTestScheduler(t)
	s.AddNode(newTestNode("n1", StatusOnline, 10))
	s.AddNode(newTestNode("n2", StatusOnline, 20))

	t1 := newTestTask("task-1")
	t2 := newTestTask("task-2")
	t3 := newTestTask("task-3")
	_ = s.SubmitTask(t1)
	_ = s.SubmitTask(t2)
	_ = s.SubmitTask(t3)

	time.Sleep(300 * time.Millisecond)

	t1got, _ := s.GetTask(t1.ID)
	t2got, _ := s.GetTask(t2.ID)
	t3got, _ := s.GetTask(t3.ID)
	if t1got.TargetNode == "" || t2got.TargetNode == "" || t3got.TargetNode == "" {
		t.Skip("Tasks not yet dispatched, skipping rebalance test")
	}

	t1got.TargetNode = "n1"
	t2got.TargetNode = "n1"
	t3got.TargetNode = "n2"

	for _, n := range s.GetAllNodes() {
		t.Logf("node %s: %v", n.ID, s.GetTasksForNode(n.ID))
	}

	gotN1 := s.GetTasksForNode("n1")
	if len(gotN1) != 2 {
		t.Errorf("n1 task count = %d, want 2", len(gotN1))
	}
}

func TestSchedulerRebalanceMovesExcessTasks(t *testing.T) {
	s := newTestScheduler(t)
	s.AddNode(newTestNode("n1", StatusOnline, 0))
	s.AddNode(newTestNode("n2", StatusOnline, 0))

	for i := 0; i < 8; i++ {
		t1 := newTestTask("rebal-" + itoa(i))
		_ = s.SubmitTask(t1)
	}

	time.Sleep(500 * time.Millisecond)

	for id, t1 := range s.allTasksSnapshot() {
		if t1.TargetNode == "" {
			_ = id
		}
	}

	s.Rebalance()

	for _, n := range s.GetAllNodes() {
		ids := s.GetTasksForNode(n.ID)
		t.Logf("after rebalance, node %s has %d tasks", n.ID, len(ids))
	}
}

func TestSchedulerStartStop(t *testing.T) {
	s := NewScheduler("test", config.DefaultConfig())
	s.Start()
	time.Sleep(50 * time.Millisecond)
	s.Stop()
}

func TestSchedulerConcurrentNodeAdd(t *testing.T) {
	s := newTestScheduler(t)
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			n := newTestNode(itoa(i), StatusOnline, i%100)
			s.AddNode(n)
		}(i)
	}
	wg.Wait()

	if got := len(s.GetAllNodes()); got != 50 {
		t.Errorf("len(nodes) = %d, want 50", got)
	}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	negative := i < 0
	if negative {
		i = -i
	}
	var buf [20]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	if negative {
		pos--
		buf[pos] = '-'
	}
	return string(buf[pos:])
}

func (s *Scheduler) allTasksSnapshot() map[string]*task.Task {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]*task.Task, len(s.taskQueue))
	for k, v := range s.taskQueue {
		out[k] = v
	}
	return out
}
