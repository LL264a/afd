package api

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/pprof"
	"net/url"
	"path/filepath"
	"strconv"
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
	stopCh        chan struct{}
	stopOnce      sync.Once
	wg            sync.WaitGroup
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

type UpdateTaskRequest struct {
	Priority *int              `json:"priority,omitempty"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

type TaskResponse struct {
	Task *task.Task `json:"task"`
}

type TaskListResponse struct {
	Tasks  []*task.Task `json:"tasks"`
	Total  int          `json:"total"`
	Active int          `json:"active"`
	Limit  int          `json:"limit,omitempty"`
	Offset int          `json:"offset"`
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
	writeJSON(w, code, ErrorResponse{
		Error:   message,
		Code:    code,
		Details: details,
	})
}

// writeJSON writes a JSON response with the given status code. It sets
// the Content-Type header and writes the status code before encoding,
// and logs (instead of silently dropping) any encoding/write error.
func writeJSON(w http.ResponseWriter, code int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		logger.Log.Warnw("failed to write JSON response", "error", err)
	}
}

func isPrivateIP(host string) bool {
	ips, err := net.LookupIP(host)
	if err != nil {
		return false
	}
	for _, ip := range ips {
		if ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsUnspecified() {
			return true
		}
	}
	return false
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
		if u.Host == "" {
			return false
		}
		// SSRF 防护：阻止内网地址
		if isPrivateIP(u.Hostname()) {
			return false
		}
		return true
	default:
		return false
	}
}

func isSafePath(path string) bool {
	cleaned := filepath.Clean(path)
	// 检查路径穿越：按分隔符分段检查 ".."，避免误拦 file..txt 等合法文件名
	parts := strings.Split(cleaned, string(filepath.Separator))
	for _, part := range parts {
		if part == ".." {
			return false
		}
	}
	// 阻止绝对路径
	if filepath.IsAbs(cleaned) {
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
		stopCh:     make(chan struct{}),
	}

	// WebSocket endpoint - no middleware needed
	mux.HandleFunc("/ws", server.handleWebSocket)

	// JSON-RPC endpoint
	jsonRPCServer := NewJSONRPCServer(taskQueue, taskStore, cfg, ver)
	apiMux.Handle("/jsonrpc", jsonRPCServer)
	apiMux.Handle("/rpc", jsonRPCServer)

	// XML-RPC endpoint
	xmlRPCServer := NewXMLRPCServer(taskQueue, jsonRPCServer)
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
		writeJSON(w, http.StatusOK, map[string]string{"status": "healthy"})
	})
	apiMux.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{
			"status": "ready",
		})
	})
	apiMux.Handle("/metrics", MetricsHandler())

	// pprof 调试端点。默认启用；若配置了 AuthToken 则同样需要认证。
	// 可通过 api.enable_pprof: false 显式关闭。
	pprofEnabled := cfg.API.EnablePprof == nil || *cfg.API.EnablePprof
	if pprofEnabled {
		pprofMux := http.NewServeMux()
		pprofMux.HandleFunc("/debug/pprof/", pprof.Index)
		pprofMux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		pprofMux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		pprofMux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		pprofMux.HandleFunc("/debug/pprof/trace", pprof.Trace)
		// pprof 仅套用 Recovery + Auth，避免 Logging 中间件把高频的
		// profile 采样请求当成业务请求记录到访问日志中。
		pprofMW := []Middleware{
			Recovery(),
			Auth(cfg.API.AuthToken),
		}
		mux.Handle("/debug/pprof/", Chain(pprofMux, pprofMW...))
		mux.Handle("/debug/pprof/cmdline", Chain(pprofMux, pprofMW...))
		mux.Handle("/debug/pprof/profile", Chain(pprofMux, pprofMW...))
		mux.Handle("/debug/pprof/symbol", Chain(pprofMux, pprofMW...))
		mux.Handle("/debug/pprof/trace", Chain(pprofMux, pprofMW...))
	}

	// Apply middleware to API routes only
	middlewares := []Middleware{
		Recovery(),
		PrometheusMiddleware,
		Logging(),
		server.rateLimiterMiddleware(cfg.API.RateLimit, time.Minute),
		CORS(cfg.API.EnableCORS, cfg.API.CORSAllowedOrigins...),
		Auth(cfg.API.AuthToken),
		RequestValidation(),
	}
	// apiMux 内部路由以 /api/ 开头，挂载到 /api/v1/ 下时需要将
	// /api/v1 前缀剥离并改写为 /api，使内部路由模式能够匹配。
	v1Handler := http.StripPrefix("/api/v1", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.URL.Path = "/api" + r.URL.Path
		if r.URL.RawPath != "" {
			r.URL.RawPath = "/api" + r.URL.RawPath
		}
		apiMux.ServeHTTP(w, r)
	}))
	mux.Handle("/api/v1/", Chain(v1Handler, middlewares...))
	mux.Handle("/api/", Chain(apiMux, append([]Middleware{Deprecation()}, middlewares...)...))
	mux.Handle("/jsonrpc", Chain(apiMux, middlewares...))
	mux.Handle("/xmlrpc", Chain(apiMux, middlewares...))
	mux.Handle("/rpc", Chain(apiMux, middlewares...))

	probes := []Middleware{Recovery(), Logging(), CORS(cfg.API.EnableCORS, cfg.API.CORSAllowedOrigins...)}
	mux.Handle("/health", Chain(apiMux, probes...))
	mux.Handle("/ready", Chain(apiMux, probes...))
	mux.Handle("/metrics", Chain(apiMux, Recovery(), Logging(), CORS(cfg.API.EnableCORS, cfg.API.CORSAllowedOrigins...), Auth(cfg.API.AuthToken)))

	// Core is a pure JSON API now; the web UI lives in a separate
	// repository (./ui) and is served by a dedicated nginx / CDN
	// deployment.  We do not serve any static assets from this
	// process so the binary can be deployed independently of the UI
	// release cadence.
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-AFD-Service", "core")
		writeJSON(w, http.StatusNotFound, ErrorResponse{
			Error: "AFD Core is a pure JSON/WebSocket API. Please deploy the UI separately from the ui/ repository.",
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

	if cfg.API.AuthToken == "" {
		logger.Log.Warnw("API authentication is disabled - all endpoints are open")
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

	// 解析分页参数
	limit := 0 // 0 表示全部
	offset := 0
	if l := r.URL.Query().Get("limit"); l != "" {
		if v, err := strconv.Atoi(l); err == nil && v > 0 {
			limit = v
		}
	}
	if o := r.URL.Query().Get("offset"); o != "" {
		if v, err := strconv.Atoi(o); err == nil && v >= 0 {
			offset = v
		}
	}

	total := len(tasks)
	if offset > total {
		offset = total
	}
	end := total
	if limit > 0 && offset+limit < end {
		end = offset + limit
	}
	paged := tasks[offset:end]

	response := TaskListResponse{
		Tasks:  make([]*task.Task, len(paged)),
		Total:  total,
		Active: s.taskQueue.ActiveCount(),
		Offset: offset,
	}
	if limit > 0 {
		response.Limit = limit
	}
	for i, t := range paged {
		taskCopy := t.GetSafe()
		response.Tasks[i] = &taskCopy
	}

	writeJSON(w, http.StatusOK, response)
}

func (s *Server) createTask(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20) // 1MB
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
		sendError(w, http.StatusBadRequest, "Priority must be between 0 and 10", "")
		return
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

	taskCopy := newTask.GetSafe()
	writeJSON(w, http.StatusCreated, TaskResponse{Task: &taskCopy})
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

	taskCopy := t.GetSafe()
	writeJSON(w, http.StatusOK, TaskResponse{Task: &taskCopy})
}

func (s *Server) deleteTask(w http.ResponseWriter, r *http.Request, id string) {
	if err := s.taskQueue.Remove(id); err != nil {
		sendError(w, http.StatusNotFound, "Task not found", err.Error())
		return
	}

	s.taskStore.Delete(id)
	// 广播删除事件（使用一个包含 ID 的最小 Task 对象）
	deletedTask := &task.Task{ID: id}
	s.hub.BroadcastTaskUpdate(deletedTask)

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

	taskCopy := t.GetSafe()
	writeJSON(w, http.StatusOK, TaskResponse{Task: &taskCopy})
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

	taskCopy := t.GetSafe()
	writeJSON(w, http.StatusOK, TaskResponse{Task: &taskCopy})
}

func (s *Server) handleNodes(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		sendError(w, http.StatusMethodNotAllowed, "Method not allowed", "")
		return
	}

	nodes := s.membership.Members()

	localNodeInfo := s.localNode.NodeInfo()
	nodes = append(nodes, localNodeInfo.Node)

	writeJSON(w, http.StatusOK, NodeResponse{Nodes: nodes})
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

	writeJSON(w, http.StatusOK, response)
}

func (s *Server) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	// 认证检查：WebSocket 无法使用标准中间件，需在握手前校验 token
	if s.config.API.AuthToken != "" {
		token := r.URL.Query().Get("token")
		if token == "" {
			token = r.Header.Get("Authorization")
			if strings.HasPrefix(token, "Bearer ") {
				token = token[7:]
			}
		}
		if !secureCompare(token, s.config.API.AuthToken) {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
	}
	s.hub.ServeWS(w, r)
}

// saveSession 将所有任务持久化到 taskStore，用于会话保存和自动保存。
func (s *Server) saveSession() {
	if s.taskStore == nil || s.taskQueue == nil {
		return
	}
	tasks := s.taskQueue.List()
	for _, t := range tasks {
		if err := s.taskStore.Save(t); err != nil {
			logger.Log.Warnw("saveSession persist failed", "gid", t.ID, "err", err)
		}
	}
}

func (s *Server) Start() error {
	// 启动定时自动保存会话
	if s.config != nil && s.config.AutoSaveInterval > 0 {
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.autoSaveLoop(s.config.AutoSaveInterval)
		}()
	}

	tlsEnabled := s.config != nil && s.config.API.TLSEnabled &&
		s.config.API.TLSCertFile != "" && s.config.API.TLSKeyFile != ""

	if tlsEnabled {
		// 强制 TLS 1.2+，并启用 HTTP/2 协商。
		s.TLSConfig = &tls.Config{
			MinVersion: tls.VersionTLS12,
			NextProtos: []string{"h2", "http/1.1"},
		}
		logger.Log.Infow("Starting API server (TLS)",
			"addr", s.Addr,
			"cert", s.config.API.TLSCertFile,
		)
		return s.ListenAndServeTLS(s.config.API.TLSCertFile, s.config.API.TLSKeyFile)
	}

	logger.Log.Infow("Starting API server",
		"addr", s.Addr,
	)
	return s.ListenAndServe()
}

// autoSaveLoop 定期保存会话，直到 stopCh 关闭。
func (s *Server) autoSaveLoop(intervalSec int) {
	interval := time.Duration(intervalSec) * time.Second
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.saveSession()
		case <-s.stopCh:
			return
		}
	}
}

func (s *Server) Stop() error {
	logger.Log.Info("Stopping API server")
	s.stopOnce.Do(func() {
		close(s.stopCh)
		if s.rateLimitStop != nil {
			s.rateLimitStop()
		}
	})
	s.wg.Wait()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return s.Shutdown(ctx)
}

func (s *Server) handlePauseAll(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		sendError(w, http.StatusMethodNotAllowed, "Method not allowed", "")
		return
	}

	var failed []string
	tasks := s.taskQueue.List()
	for _, t := range tasks {
		status := t.GetStatus()
		if status == task.StatusDownloading || status == task.StatusPending {
			if err := s.taskQueue.Pause(t.ID); err != nil {
				failed = append(failed, t.ID)
				logger.Log.Warnw("failed to pause task", "id", t.ID, "error", err)
			}
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

	var failed []string
	tasks := s.taskQueue.List()
	for _, t := range tasks {
		status := t.GetStatus()
		if status == task.StatusPaused {
			if err := s.taskQueue.Resume(t.ID); err != nil {
				failed = append(failed, t.ID)
				logger.Log.Warnw("failed to resume task", "id", t.ID, "error", err)
			}
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
		writeJSON(w, http.StatusOK, map[string]string{
			"level": s.config.Node.LogLevel,
		})
	case http.MethodPost:
		r.Body = http.MaxBytesReader(w, r.Body, 4<<10) // 4KB
		var req LogLevelRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			sendError(w, http.StatusBadRequest, "Invalid request body", err.Error())
			return
		}

		allowed := map[string]bool{"debug": true, "info": true, "warn": true, "error": true}
		if !allowed[req.Level] {
			sendError(w, http.StatusBadRequest, "Invalid log level", req.Level)
			return
		}

		if err := logger.Init(req.Level, ""); err != nil {
			sendError(w, http.StatusInternalServerError, "Failed to update log level", err.Error())
			return
		}

		s.config.Node.LogLevel = req.Level
		logger.Log.Infow("Log level updated", "level", req.Level)

		writeJSON(w, http.StatusOK, map[string]string{
			"status": "ok",
			"level":  req.Level,
		})
	default:
		sendError(w, http.StatusMethodNotAllowed, "Method not allowed", "")
	}
}

// cloneConfig 通过序列化/反序列化实现 config 的深拷贝，确保回滚时
// 指针字段和切片字段不会与新配置共享底层数据。
func cloneConfig(c *config.Config) (*config.Config, error) {
	data, err := json.Marshal(c)
	if err != nil {
		return nil, err
	}
	var clone config.Config
	if err := json.Unmarshal(data, &clone); err != nil {
		return nil, err
	}
	return &clone, nil
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

	// 深拷贝当前配置作为备份，避免浅拷贝导致回滚不完整
	oldConfig, err := cloneConfig(s.config)
	if err != nil {
		sendError(w, http.StatusInternalServerError, "failed to backup config", "")
		return
	}

	s.mu.Lock()
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
			*s.config = *oldConfig
			s.mu.Unlock()
			sendError(w, http.StatusInternalServerError, "Failed to apply new log level", err.Error())
			return
		}
	}

	writeJSON(w, http.StatusOK, map[string]string{
		"status":  "ok",
		"message": "Config reloaded successfully. Note: listener address, TLS settings, auth token, rate-limit, CORS and middleware chain require a process restart to take effect.",
	})
}
