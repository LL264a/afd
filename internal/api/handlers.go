package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"path/filepath"
	"time"

	"github.com/nexus-dl/afd/internal/cluster"
	"github.com/nexus-dl/afd/internal/task"
	"github.com/nexus-dl/afd/pkg/config"
	"github.com/nexus-dl/afd/pkg/logger"
)

type TaskHandler struct {
	taskQueue *task.TaskQueue
	taskStore *task.TaskStore
	config    *config.Config
	hub       *WebSocketHub
}

type NodeHandler struct {
	membership *cluster.Membership
	localNode  *cluster.LocalNode
}

type StatusHandler struct {
	membership *cluster.Membership
	localNode  *cluster.LocalNode
	taskQueue  *task.TaskQueue
}

func NewTaskHandler(taskQueue *task.TaskQueue, taskStore *task.TaskStore, cfg *config.Config, hub *WebSocketHub) *TaskHandler {
	return &TaskHandler{
		taskQueue: taskQueue,
		taskStore: taskStore,
		config:    cfg,
		hub:       hub,
	}
}

func NewNodeHandler(membership *cluster.Membership, localNode *cluster.LocalNode) *NodeHandler {
	return &NodeHandler{
		membership: membership,
		localNode:  localNode,
	}
}

func NewStatusHandler(membership *cluster.Membership, localNode *cluster.LocalNode, taskQueue *task.TaskQueue) *StatusHandler {
	return &StatusHandler{
		membership: membership,
		localNode:  localNode,
		taskQueue:  taskQueue,
	}
}

func (h *TaskHandler) ListTasks(w http.ResponseWriter, r *http.Request) {
	tasks := h.taskQueue.List()

	response := TaskListResponse{
		Tasks:  make([]*task.Task, len(tasks)),
		Total:  len(tasks),
		Active: h.taskQueue.ActiveCount(),
	}

	for i, t := range tasks {
		taskCopy := t.GetSafe()
		tasks[i] = &taskCopy
	}
	response.Tasks = tasks

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func (h *TaskHandler) CreateTask(w http.ResponseWriter, r *http.Request) {
	const maxBodySize = 1 << 20 // 1 MiB
	r.Body = http.MaxBytesReader(w, r.Body, maxBodySize)

	var req CreateTaskRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		sendError(w, http.StatusBadRequest, "Invalid request body", err.Error())
		return
	}

	if req.URL == "" {
		sendError(w, http.StatusBadRequest, "URL is required", "")
		return
	}

	if req.OutputPath == "" {
		req.OutputPath = filepath.Join(h.config.Node.DataDir, "downloads")
	}

	newTask := task.NewTask(req.URL, req.OutputPath)
	newTask.Priority = req.Priority
	if req.Metadata != nil {
		newTask.Metadata = req.Metadata
	}

	if err := h.taskQueue.Add(newTask); err != nil {
		sendError(w, http.StatusInternalServerError, "Failed to add task", err.Error())
		return
	}

	if err := h.taskStore.Save(newTask); err != nil {
		logger.Log.Warnw("Failed to save task to store", "error", err)
	}

	h.hub.BroadcastTaskUpdate(newTask)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	taskCopy := newTask.GetSafe()
	json.NewEncoder(w).Encode(TaskResponse{Task: &taskCopy})
}

func (h *TaskHandler) GetTask(w http.ResponseWriter, r *http.Request, id string) {
	t, err := h.taskQueue.Get(id)
	if err != nil {
		sendError(w, http.StatusNotFound, err.Error(), "")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	taskCopy := t.GetSafe()
	json.NewEncoder(w).Encode(TaskResponse{Task: &taskCopy})
}

func (h *TaskHandler) UpdateTask(w http.ResponseWriter, r *http.Request, id string) {
	const maxBodySize = 1 << 20 // 1 MiB
	r.Body = http.MaxBytesReader(w, r.Body, maxBodySize)

	var req UpdateTaskRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		sendError(w, http.StatusBadRequest, "Invalid request body", err.Error())
		return
	}

	t, err := h.taskQueue.Get(id)
	if err != nil {
		sendError(w, http.StatusNotFound, err.Error(), "")
		return
	}

	if req.Priority != nil {
		p := *req.Priority
		if p < 0 || p > 10 {
			sendError(w, http.StatusBadRequest, "Priority must be between 0 and 10", "")
			return
		}
		t.SetPriority(p)
	}

	if req.Metadata != nil {
		t.SetMetadata(req.Metadata)
	}

	h.hub.BroadcastTaskUpdate(t)

	w.Header().Set("Content-Type", "application/json")
	taskCopy := t.GetSafe()
	json.NewEncoder(w).Encode(TaskResponse{Task: &taskCopy})
}

func (h *TaskHandler) DeleteTask(w http.ResponseWriter, r *http.Request, id string) {
	if err := h.taskQueue.Remove(id); err != nil {
		sendError(w, http.StatusNotFound, err.Error(), "")
		return
	}

	h.taskStore.Delete(id)
	// 广播删除事件（使用一个包含 ID 的最小 Task 对象）
	deletedTask := &task.Task{ID: id}
	h.hub.BroadcastTaskUpdate(deletedTask)

	w.WriteHeader(http.StatusNoContent)
}

func (h *TaskHandler) PauseTask(w http.ResponseWriter, r *http.Request, id string) {
	if err := h.taskQueue.Pause(id); err != nil {
		sendError(w, http.StatusBadRequest, err.Error(), "")
		return
	}

	t, err := h.taskQueue.Get(id)
	if err != nil {
		sendError(w, http.StatusInternalServerError, "Failed to get task after pause", err.Error())
		return
	}
	h.hub.BroadcastTaskUpdate(t)

	w.Header().Set("Content-Type", "application/json")
	taskCopy := t.GetSafe()
	json.NewEncoder(w).Encode(TaskResponse{Task: &taskCopy})
}

func (h *TaskHandler) ResumeTask(w http.ResponseWriter, r *http.Request, id string) {
	if err := h.taskQueue.Resume(id); err != nil {
		sendError(w, http.StatusBadRequest, err.Error(), "")
		return
	}

	t, err := h.taskQueue.Get(id)
	if err != nil {
		sendError(w, http.StatusInternalServerError, "Failed to get task after resume", err.Error())
		return
	}
	h.hub.BroadcastTaskUpdate(t)

	w.Header().Set("Content-Type", "application/json")
	taskCopy := t.GetSafe()
	json.NewEncoder(w).Encode(TaskResponse{Task: &taskCopy})
}

func (h *NodeHandler) ListNodes(w http.ResponseWriter, r *http.Request) {
	nodes := h.membership.Members()

	localNodeInfo := h.localNode.NodeInfo()
	nodes = append(nodes, localNodeInfo.Node)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(NodeResponse{Nodes: nodes})
}

func (h *StatusHandler) GetStatus(w http.ResponseWriter, r *http.Request) {
	localNodeInfo := h.localNode.NodeInfo()

	response := ClusterStatusResponse{
		Status:      "healthy",
		NodeCount:   h.membership.MemberCount() + 1,
		OnlineCount: h.membership.OnlineCount() + 1,
		TaskCount:   h.taskQueue.TotalCount(),
		ActiveTasks: h.taskQueue.ActiveCount(),
		LocalNode:   localNodeInfo.Node,
		Version:     localNodeInfo.Version,
		Uptime:      time.Since(localNodeInfo.StartedAt),
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func extractTaskID(path string) string {
	return path[len("/api/tasks/"):]
}

func isTaskPath(path string) bool {
	return len(path) > len("/api/tasks/") && path[:len("/api/tasks/")] == "/api/tasks/"
}

func isPauseAction(path string) bool {
	return len(path) > len("/api/tasks/")+len("/pause") && path[len(path)-len("/pause"):] == "/pause"
}

func isResumeAction(path string) bool {
	return len(path) > len("/api/tasks/")+len("/resume") && path[len(path)-len("/resume"):] == "/resume"
}

func (h *TaskHandler) HandleTaskAction(w http.ResponseWriter, r *http.Request, id string, action string) {
	switch action {
	case "pause":
		h.PauseTask(w, r, id)
	case "resume":
		h.ResumeTask(w, r, id)
	default:
		sendError(w, http.StatusBadRequest, "Unknown action", action)
	}
}

func ParseTaskPath(urlPath string) (isTask bool, id string, action string) {
	if !isTaskPath(urlPath) {
		return false, "", ""
	}

	id = extractTaskID(urlPath)

	if isPauseAction(urlPath) {
		action = "pause"
		id = urlPath[len("/api/tasks/") : len(urlPath)-len("/pause")]
	} else if isResumeAction(urlPath) {
		action = "resume"
		id = urlPath[len("/api/tasks/") : len(urlPath)-len("/resume")]
	}

	return true, id, action
}

type TaskStatsResponse struct {
	Total   int `json:"total"`
	Active  int `json:"active"`
	Pending int `json:"pending"`
	Paused  int `json:"paused"`
	Done    int `json:"done"`
	Failed  int `json:"failed"`
}

func (h *TaskHandler) GetTaskStats(w http.ResponseWriter, r *http.Request) {
	tasks := h.taskQueue.List()

	stats := TaskStatsResponse{
		Total: len(tasks),
	}

	for _, t := range tasks {
		switch t.GetStatus() {
		case task.StatusDownloading:
			stats.Active++
		case task.StatusPending:
			stats.Pending++
		case task.StatusPaused:
			stats.Paused++
		case task.StatusDone:
			stats.Done++
		case task.StatusFailed:
			stats.Failed++
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(stats)
}

type NodeStatsResponse struct {
	TotalNodes   int `json:"total_nodes"`
	OnlineNodes  int `json:"online_nodes"`
	OfflineNodes int `json:"offline_nodes"`
}

func (h *NodeHandler) GetNodeStats(w http.ResponseWriter, r *http.Request) {
	stats := NodeStatsResponse{
		TotalNodes:   h.membership.MemberCount() + 1,
		OnlineNodes:  h.membership.OnlineCount() + 1,
		OfflineNodes: h.membership.MemberCount() - h.membership.OnlineCount(),
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(stats)
}

type HealthResponse struct {
	Status   string `json:"status"`
	Services map[string]struct {
		Status  string `json:"status"`
		Message string `json:"message,omitempty"`
	} `json:"services"`
}

func (h *StatusHandler) GetHealth(w http.ResponseWriter, r *http.Request) {
	localNodeInfo := h.localNode.NodeInfo()

	response := HealthResponse{
		Status: "healthy",
		Services: map[string]struct {
			Status  string `json:"status"`
			Message string `json:"message,omitempty"`
		}{
			"api": {
				Status:  "healthy",
				Message: "API server is running",
			},
			"cluster": {
				Status:  "healthy",
				Message: fmt.Sprintf("Connected to %d nodes", h.membership.MemberCount()),
			},
			"task_queue": {
				Status:  "healthy",
				Message: fmt.Sprintf("%d tasks in queue", h.taskQueue.TotalCount()),
			},
			"node": {
				Status:  "healthy",
				Message: fmt.Sprintf("Node %s is online", localNodeInfo.Node.ID),
			},
		},
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}
