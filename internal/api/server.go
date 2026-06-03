package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/nexus-dl/afd/internal/cluster"
	"github.com/nexus-dl/afd/internal/task"
	"github.com/nexus-dl/afd/pkg/config"
	"github.com/nexus-dl/afd/pkg/logger"
)

type Server struct {
	http.Server
	taskQueue     *task.TaskQueue
	taskStore     *task.TaskStore
	membership    *cluster.Membership
	localNode     *cluster.LocalNode
	config        *config.Config
	hub           *WebSocketHub
	mu            sync.RWMutex
	startedAt     time.Time
	version       string
	rateLimitStop func()
}

func (s *Server) Handler() http.Handler {
	return s.Server.Handler
}

type CreateTaskRequest struct {
	URL        string            `json:"url"`
	OutputPath string            `json:"output_path"`
	Priority   int               `json:"priority"`
	Metadata   map[string]string `json:"metadata"`
}

type TaskResponse struct {
	Task *task.Task `json:"task"`
}

type TaskListResponse struct {
	Tasks  []*task.Task `json:"tasks"`
	Total  int          `json:"total"`
	Active int          `json:"active"`
}

type NodeResponse struct {
	Nodes []cluster.Node `json:"nodes"`
}

type ClusterStatusResponse struct {
	Status      string        `json:"status"`
	NodeCount   int           `json:"node_count"`
	OnlineCount int           `json:"online_count"`
	TaskCount   int           `json:"task_count"`
	ActiveTasks int           `json:"active_tasks"`
	LocalNode   cluster.Node  `json:"local_node"`
	Version     string        `json:"version"`
	Uptime      time.Duration `json:"uptime"`
}

type ErrorResponse struct {
	Error   string `json:"error"`
	Code    int    `json:"code"`
	Details string `json:"details,omitempty"`
}

func sendError(w http.ResponseWriter, code int, message string, details string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(ErrorResponse{
		Error:   message,
		Code:    code,
		Details: details,
	})
}

func isValidURL(rawURL string) bool {
	if rawURL == "" {
		return false
	}
	if strings.HasPrefix(rawURL, "magnet:") {
		return strings.HasPrefix(rawURL, "magnet:?xt=urn:")
	}
	u, err := url.Parse(rawURL)
	if err != nil {
		return false
	}
	switch u.Scheme {
	case "http", "https", "ftp", "ftps", "sftp", "s3", "webdav", "webdavs":
		return u.Host != ""
	default:
		return false
	}
}

func isSafePath(path string) bool {
	cleaned := filepath.Clean(path)
	if strings.Contains(cleaned, "..") {
		return false
	}
	return true
}

func NewServer(cfg *config.Config, taskQueue *task.TaskQueue, taskStore *task.TaskStore, membership *cluster.Membership, localNode *cluster.LocalNode, hub *WebSocketHub, ver string) *Server {
	mux := http.NewServeMux()
	apiMux := http.NewServeMux()
	server := &Server{
		taskQueue:  taskQueue,
		taskStore:  taskStore,
		membership: membership,
		localNode:  localNode,
		config:     cfg,
		hub:        hub,
		startedAt:  time.Now(),
		version:    ver,
	}

	// WebSocket endpoint - no middleware needed
	mux.HandleFunc("/ws", server.handleWebSocket)

	// JSON-RPC endpoint
	jsonRPCServer := NewJSONRPCServer(taskQueue, taskStore, cfg, ver)
	apiMux.Handle("/jsonrpc", jsonRPCServer)
	apiMux.Handle("/rpc", jsonRPCServer)

	// XML-RPC endpoint
	xmlRPCServer := NewXMLRPCServer(taskQueue)
	xmlRPCServer.RegisterRoutes(apiMux)

	// API endpoints
	apiMux.HandleFunc("/api/tasks", server.handleTasks)
	apiMux.HandleFunc("/api/tasks/", server.handleTaskByID)
	apiMux.HandleFunc("/api/tasks/pause-all", server.handlePauseAll)
	apiMux.HandleFunc("/api/tasks/resume-all", server.handleResumeAll)
	apiMux.HandleFunc("/api/config/reload", server.handleReloadConfig)
	apiMux.HandleFunc("/api/log-level", server.handleLogLevel)
	apiMux.HandleFunc("/api/nodes", server.handleNodes)
	apiMux.HandleFunc("/api/status", server.handleStatus)
	apiMux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status":    "healthy",
			"version":   server.version,
			"uptime":    time.Since(server.startedAt).String(),
			"tasks":     server.taskQueue.TotalCount(),
			"active":    server.taskQueue.ActiveCount(),
			"timestamp": time.Now().UTC().Format(time.RFC3339),
		})
	})
	apiMux.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{
			"status": "ready",
		})
	})
	apiMux.Handle("/metrics", MetricsHandler())

	// Apply middleware to API routes only
	middlewares := []Middleware{
		Recovery(),
		Logging(),
		server.rateLimiterMiddleware(cfg.API.RateLimit, time.Minute),
		CORS(cfg.API.EnableCORS),
		Auth(cfg.API.AuthToken),
	}
	mux.Handle("/api/v1/", Chain(apiMux, middlewares...))
	mux.Handle("/api/", Chain(apiMux, append([]Middleware{Deprecation()}, middlewares...)...))
	mux.Handle("/jsonrpc", Chain(apiMux, middlewares...))
	mux.Handle("/xmlrpc", Chain(apiMux, middlewares...))
	mux.Handle("/rpc", Chain(apiMux, middlewares...))

	probes := []Middleware{Recovery(), Logging(), CORS(cfg.API.EnableCORS, cfg.API.CORSAllowedOrigins...)}
	mux.Handle("/health", Chain(apiMux, probes...))
	mux.Handle("/ready", Chain(apiMux, probes...))
	mux.Handle("/metrics", Chain(apiMux, probes...))

	// Core is a pure JSON API now; the web UI lives in a separate
	// repository (./ui) and is served by a dedicated nginx / CDN
	// deployment.  We do not serve any static assets from this
	// process so the binary can be deployed independently of the UI
	// release cadence.
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-NexusDL-Service", "core")
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(ErrorResponse{
			Error: "NexusDL Core is a pure JSON/WebSocket API. Please deploy the UI separately from the ui/ repository.",
			Code:  404,
		})
	})

	server.Server = http.Server{
		Addr:              fmt.Sprintf("%s:%d", cfg.API.Host, cfg.API.Port),
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      0, // streaming responses; rely on IdleTimeout
		IdleTimeout:       120 * time.Second,
		MaxHeaderBytes:    1 << 20, // 1 MiB
	}

	return server
}

func findWebPath() string {
	// Retained as a no-op so older callers / plugins that import this
	// symbol keep compiling.  The web UI is no longer shipped with
	// the core binary; deploy it separately from the ui repository
	// and proxy it through nginx / CDN.
	return ""
}

func (s *Server) handleTasks(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		s.listTasks(w, r)
	case http.MethodPost:
		s.createTask(w, r)
	default:
		sendError(w, http.StatusMethodNotAllowed, "Method not allowed", "")
	}
}

func (s *Server) listTasks(w http.ResponseWriter, r *http.Request) {
	tasks := s.taskQueue.List()

	response := TaskListResponse{
		Tasks:  make([]*task.Task, len(tasks)),
		Total:  len(tasks),
		Active: s.taskQueue.ActiveCount(),
	}

	for i, t := range tasks {
		taskCopy := t.GetSafe()
		tasks[i] = &taskCopy
	}
	response.Tasks = tasks

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func (s *Server) createTask(w http.ResponseWriter, r *http.Request) {
	var req CreateTaskRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		sendError(w, http.StatusBadRequest, "Invalid request body", err.Error())
		return
	}

	if req.URL == "" {
		sendError(w, http.StatusBadRequest, "URL is required", "")
		return
	}

	if !isValidURL(req.URL) {
		sendError(w, http.StatusBadRequest, "Invalid URL format", req.URL)
		return
	}

	if req.OutputPath != "" && !isSafePath(req.OutputPath) {
		sendError(w, http.StatusBadRequest, "Invalid output path", "path traversal detected")
		return
	}

	if req.OutputPath == "" {
		req.OutputPath = filepath.Join(s.config.Node.DataDir, "downloads")
	}

	if req.Priority < 0 || req.Priority > 10 {
		req.Priority = 5
	}

	newTask := task.NewTask(req.URL, req.OutputPath)
	newTask.Priority = req.Priority
	if req.Metadata != nil {
		newTask.Metadata = req.Metadata
	}

	if err := s.taskQueue.Add(newTask); err != nil {
		sendError(w, http.StatusInternalServerError, "Failed to add task", err.Error())
		return
	}

	if err := s.taskStore.Save(newTask); err != nil {
		logger.Log.Warnw("Failed to save task to store", "error", err)
	}

	s.hub.BroadcastTaskUpdate(newTask)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	taskCopy := newTask.GetSafe()
	json.NewEncoder(w).Encode(TaskResponse{Task: &taskCopy})
}

func (s *Server) handleTaskByID(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	id := path[len("/api/tasks/"):]

	if strings.HasSuffix(id, "/pause") {
		id = strings.TrimSuffix(id, "/pause")
		if r.Method == http.MethodPost {
			s.pauseTask(w, r, id)
			return
		}
	}

	if strings.HasSuffix(id, "/resume") {
		id = strings.TrimSuffix(id, "/resume")
		if r.Method == http.MethodPost {
			s.resumeTask(w, r, id)
			return
		}
	}

	if id == "" {
		sendError(w, http.StatusBadRequest, "Task ID required", "")
		return
	}

	switch r.Method {
	case http.MethodGet:
		s.getTask(w, r, id)
	case http.MethodDelete:
		s.deleteTask(w, r, id)
	default:
		sendError(w, http.StatusMethodNotAllowed, "Method not allowed", "")
	}
}

func (s *Server) getTask(w http.ResponseWriter, r *http.Request, id string) {
	t, err := s.taskQueue.Get(id)
	if err != nil {
		sendError(w, http.StatusNotFound, "Task not found", err.Error())
		return
	}

	w.Header().Set("Content-Type", "application/json")
	taskCopy := t.GetSafe()
	json.NewEncoder(w).Encode(TaskResponse{Task: &taskCopy})
}

func (s *Server) deleteTask(w http.ResponseWriter, r *http.Request, id string) {
	if err := s.taskQueue.Remove(id); err != nil {
		sendError(w, http.StatusNotFound, "Task not found", err.Error())
		return
	}

	s.taskStore.Delete(id)
	s.hub.BroadcastTaskUpdate(nil)

	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) pauseTask(w http.ResponseWriter, r *http.Request, id string) {
	if err := s.taskQueue.Pause(id); err != nil {
		sendError(w, http.StatusBadRequest, "Failed to pause task", err.Error())
		return
	}

	t, err := s.taskQueue.Get(id)
	if err != nil {
		sendError(w, http.StatusInternalServerError, "Failed to get task after pause", err.Error())
		return
	}
	s.hub.BroadcastTaskUpdate(t)

	w.Header().Set("Content-Type", "application/json")
	taskCopy := t.GetSafe()
	json.NewEncoder(w).Encode(TaskResponse{Task: &taskCopy})
}

func (s *Server) resumeTask(w http.ResponseWriter, r *http.Request, id string) {
	if err := s.taskQueue.Resume(id); err != nil {
		sendError(w, http.StatusBadRequest, "Failed to resume task", err.Error())
		return
	}

	t, err := s.taskQueue.Get(id)
	if err != nil {
		sendError(w, http.StatusInternalServerError, "Failed to get task after resume", err.Error())
		return
	}
	s.hub.BroadcastTaskUpdate(t)

	w.Header().Set("Content-Type", "application/json")
	taskCopy := t.GetSafe()
	json.NewEncoder(w).Encode(TaskResponse{Task: &taskCopy})
}

func (s *Server) handleNodes(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		sendError(w, http.StatusMethodNotAllowed, "Method not allowed", "")
		return
	}

	nodes := s.membership.Members()

	localNodeInfo := s.localNode.NodeInfo()
	nodes = append(nodes, localNodeInfo.Node)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(NodeResponse{Nodes: nodes})
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		sendError(w, http.StatusMethodNotAllowed, "Method not allowed", "")
		return
	}

	localNodeInfo := s.localNode.NodeInfo()

	response := ClusterStatusResponse{
		Status:      "healthy",
		NodeCount:   s.membership.MemberCount() + 1,
		OnlineCount: s.membership.OnlineCount() + 1,
		TaskCount:   s.taskQueue.TotalCount(),
		ActiveTasks: s.taskQueue.ActiveCount(),
		LocalNode:   localNodeInfo.Node,
		Version:     localNodeInfo.Version,
		Uptime:      time.Since(s.startedAt),
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func (s *Server) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	s.hub.ServeWS(w, r)
}

func (s *Server) Start() error {
	logger.Log.Infow("Starting API server",
		"addr", s.Addr,
	)
	return s.ListenAndServe()
}

func (s *Server) Stop() error {
	logger.Log.Info("Stopping API server")
	if s.rateLimitStop != nil {
		s.rateLimitStop()
		s.rateLimitStop = nil
	}
	return s.Shutdown(context.Background())
}

func (s *Server) handlePauseAll(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		sendError(w, http.StatusMethodNotAllowed, "Method not allowed", "")
		return
	}

	tasks := s.taskQueue.List()
	for _, t := range tasks {
		if t.Status == task.StatusDownloading || t.Status == task.StatusPending {
			s.taskQueue.Pause(t.ID)
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"status": "ok", "message": "All tasks paused"}`))
}

func (s *Server) handleResumeAll(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		sendError(w, http.StatusMethodNotAllowed, "Method not allowed", "")
		return
	}

	tasks := s.taskQueue.List()
	for _, t := range tasks {
		if t.Status == task.StatusPaused {
			s.taskQueue.Resume(t.ID)
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"status": "ok", "message": "All tasks resumed"}`))
}

type LogLevelRequest struct {
	Level string `json:"level"`
}

func (s *Server) handleLogLevel(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{
			"level": s.config.Node.LogLevel,
		})
	case http.MethodPost:
		var req LogLevelRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			sendError(w, http.StatusBadRequest, "Invalid request body", err.Error())
			return
		}

		if err := logger.Init(req.Level, ""); err != nil {
			sendError(w, http.StatusInternalServerError, "Failed to update log level", err.Error())
			return
		}

		s.config.Node.LogLevel = req.Level
		logger.Log.Infow("Log level updated", "level", req.Level)

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{
			"status": "ok",
			"level":  req.Level,
		})
	default:
		sendError(w, http.StatusMethodNotAllowed, "Method not allowed", "")
	}
}

func (s *Server) handleReloadConfig(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		sendError(w, http.StatusMethodNotAllowed, "Method not allowed", "")
		return
	}

	newConfig, err := config.Load("")
	if err != nil {
		sendError(w, http.StatusInternalServerError, "Failed to reload config", err.Error())
		return
	}

	// Validate before mutating so a bad reload does not corrupt the
	// in-memory config that the rest of the process is reading.
	if err := newConfig.Validate(); err != nil {
		sendError(w, http.StatusBadRequest, "Invalid reloaded config", err.Error())
		return
	}

	s.mu.Lock()
	oldConfig := *s.config
	*s.config = *newConfig
	s.mu.Unlock()

	logger.Log.Infow("Config reloaded",
		"port", newConfig.API.Port,
		"data_dir", newConfig.Node.DataDir,
		"log_level", newConfig.Node.LogLevel,
	)

	// If log level changed, update it.  On failure we roll the
	// in-memory config back so the next reload attempt sees the
	// pre-reload value of Node.LogLevel again.
	if oldConfig.Node.LogLevel != newConfig.Node.LogLevel {
		if err := logger.Init(newConfig.Node.LogLevel, ""); err != nil {
			logger.Log.Warnw("Failed to update log level after config reload", "error", err)
			s.mu.Lock()
			*s.config = oldConfig
			s.mu.Unlock()
			sendError(w, http.StatusInternalServerError, "Failed to apply new log level", err.Error())
			return
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status":  "ok",
		"message": "Config reloaded successfully. Note: listener address, TLS settings, auth token, rate-limit, CORS and middleware chain require a process restart to take effect.",
	})
}
