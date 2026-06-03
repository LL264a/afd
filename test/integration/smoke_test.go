// Package integration contains end-to-end smoke tests for NexusDL.
//
// These tests start an in-process API server backed by a real on-disk task
// store and a temp data directory, then exercise the most common flows:
// health probes, task lifecycle, persistence across restarts, and basic
// authentication.
//
// They are NOT part of the default `go test ./...` run because they bind
// sockets and write to disk. Run explicitly:
//
//   go test -tags=integration ./test/integration/...
//
//go:build integration
// +build integration

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/nexus-dl/afd/internal/api"
	"github.com/nexus-dl/afd/internal/cluster"
	"github.com/nexus-dl/afd/internal/task"
	"github.com/nexus-dl/afd/pkg/config"
	"github.com/nexus-dl/afd/pkg/logger"
	"go.uber.org/zap"
)

func init() {
	if logger.Log == nil {
		logger.Log = zap.NewNop().Sugar()
	}
}

type testHarness struct {
	t      *testing.T
	cfg    *config.Config
	dataDir string
	server *httptest.Server
	cancel context.CancelFunc
}

func newHarness(t *testing.T, opts func(*config.Config)) *testHarness {
	t.Helper()

	dir := t.TempDir()
	cfg := config.DefaultConfig()
	cfg.Node.DataDir = dir
	cfg.API.Port = 18080
	cfg.API.RateLimit = 0
	cfg.API.AuthToken = "test-token-123"
	if opts != nil {
		opts(cfg)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("config invalid: %v", err)
	}

	taskQueue := task.NewTaskQueue(cfg.Download.MaxConnections)
	taskStore := task.NewTaskStore(cfg.Node.DataDir)
	localNode := cluster.NewLocalNode(cfg.Node.ID, cfg.Node.Name, 0, 0, nil)
	membership := cluster.NewMembership(cfg.Node.ID)
	hub := api.NewWebSocketHub()
	hub.SetTaskQueue(taskQueue)
	hub.SetMembership(membership)
	hub.SetLocalNode(localNode)
	go hub.Run()

	srv := api.NewServer(cfg, taskQueue, taskStore, membership, localNode, hub, "test")
	ts := httptest.NewServer(srv.Handler())

	_, cancel := context.WithCancel(context.Background())
	return &testHarness{
		t:      t,
		cfg:    cfg,
		dataDir: dir,
		server: ts,
		cancel: cancel,
	}
}

func (h *testHarness) Close() {
	h.server.Close()
	h.cancel()
}

func (h *testHarness) do(t *testing.T, method, path string, body any) (*http.Response, []byte) {
	t.Helper()
	var buf io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal body: %v", err)
		}
		buf = bytes.NewReader(raw)
	}
	req, err := http.NewRequest(method, h.server.URL+path, buf)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+h.cfg.API.AuthToken)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	return resp, data
}

var _loggerOnce sync.Once
var _logger *zap.SugaredLogger

func TestMain(m *testing.M) {
	_loggerOnce.Do(func() { _logger = zap.NewNop().Sugar() })
	os.Exit(m.Run())
}

func TestHealthAndReady(t *testing.T) {
	h := newHarness(t, nil)
	defer h.Close()

	for _, path := range []string{"/health", "/ready"} {
		resp, err := http.Get(h.server.URL + path)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Errorf("%s: status = %d, want 200", path, resp.StatusCode)
		}
		resp.Body.Close()
	}
}

func TestAuthRequired(t *testing.T) {
	h := newHarness(t, nil)
	defer h.Close()

	resp, err := http.Get(h.server.URL + "/api/tasks")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("no auth: status = %d, want 401", resp.StatusCode)
	}
}

func TestAddListShowRemoveTask(t *testing.T) {
	h := newHarness(t, nil)
	defer h.Close()

	addBody := map[string]any{
		"url":      "https://example.com/file.bin",
		"priority": 5,
	}
	resp, data := h.do(t, http.MethodPost, "/api/tasks", addBody)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("add task: status = %d, body = %s", resp.StatusCode, data)
	}
	var created struct {
		Task *task.Task `json:"task"`
	}
	if err := json.Unmarshal(data, &created); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if created.Task == nil || created.Task.ID == "" {
		t.Fatal("created task has no id")
	}

	resp, data = h.do(t, http.MethodGet, "/api/tasks", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("list: status = %d, body = %s", resp.StatusCode, data)
	}
	var list struct {
		Tasks  []*task.Task `json:"tasks"`
		Total  int          `json:"total"`
		Active int          `json:"active"`
	}
	_ = json.Unmarshal(data, &list)
	if list.Total < 1 {
		t.Errorf("list total = %d, want >= 1", list.Total)
	}

	resp, data = h.do(t, http.MethodGet, "/api/tasks/"+created.Task.ID, nil)
	if resp.StatusCode != http.StatusOK {
		t.Errorf("show: status = %d, body = %s", resp.StatusCode, data)
	}

	resp, _ = h.do(t, http.MethodDelete, "/api/tasks/"+created.Task.ID, nil)
	if resp.StatusCode != http.StatusNoContent {
		t.Errorf("remove: status = %d, want 204", resp.StatusCode)
	}
}

func TestTaskPersistsAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	cfg := config.DefaultConfig()
	cfg.Node.DataDir = dir
	cfg.API.RateLimit = 0
	cfg.API.AuthToken = "test-token"
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}

	store := task.NewTaskStore(cfg.Node.DataDir)
	tk := task.NewTask("https://example.com/persist.bin", "/tmp/persist.bin")
	tk.Priority = 3
	if err := store.Save(tk); err != nil {
		t.Fatalf("save: %v", err)
	}

	store2 := task.NewTaskStore(cfg.Node.DataDir)
	loaded, err := store2.Load(tk.ID)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if loaded.URL != tk.URL {
		t.Errorf("URL = %q, want %q", loaded.URL, tk.URL)
	}
	if loaded.Priority != 3 {
		t.Errorf("Priority = %d, want 3", loaded.Priority)
	}
}

func TestRateLimiterRejectsExcess(t *testing.T) {
	h := newHarness(t, func(c *config.Config) { c.API.RateLimit = 2 })
	defer h.Close()

	got429 := 0
	for i := 0; i < 20; i++ {
		req, _ := http.NewRequest(http.MethodGet, h.server.URL+"/api/tasks", nil)
		req.Header.Set("Authorization", "Bearer "+h.cfg.API.AuthToken)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("GET: %v", err)
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			got429++
		}
		resp.Body.Close()
	}
	if got429 == 0 {
		t.Error("expected at least one 429 from rate limiter, got none")
	}
}

func TestLogLevelSet(t *testing.T) {
	h := newHarness(t, nil)
	defer h.Close()

	resp, _ := h.do(t, http.MethodPost, "/api/log-level", map[string]string{"level": "debug"})
	if resp.StatusCode != http.StatusOK {
		t.Errorf("set log level: status = %d, want 200", resp.StatusCode)
	}

	resp, data := h.do(t, http.MethodGet, "/api/log-level", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("get log level: status = %d", resp.StatusCode)
	}
	var got struct {
		Level string `json:"level"`
	}
	_ = json.Unmarshal(data, &got)
	if got.Level == "" {
		t.Errorf("level is empty, body = %s", data)
	}
}

func TestNodeListAndStatus(t *testing.T) {
	h := newHarness(t, nil)
	defer h.Close()

	resp, data := h.do(t, http.MethodGet, "/api/nodes", nil)
	if resp.StatusCode != http.StatusOK {
		t.Errorf("nodes: status = %d, body = %s", resp.StatusCode, data)
	}

	resp, data = h.do(t, http.MethodGet, "/api/status", nil)
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status: status = %d, body = %s", resp.StatusCode, data)
	}
}

func TestDataDirLayout(t *testing.T) {
	h := newHarness(t, func(c *config.Config) { c.API.AuthToken = "" })
	defer h.Close()
	resp, _ := h.do(t, http.MethodPost, "/api/tasks", map[string]any{"url": "https://example.com/x.bin"})
	if resp.StatusCode >= 400 {
		t.Fatalf("add failed: %d", resp.StatusCode)
	}
	_, err := os.Stat(filepath.Join(h.dataDir, "tasks"))
	if err != nil {
		t.Errorf("expected tasks/ in data dir: %v", err)
	}
	_ = fmt.Sprintf("harness data dir = %s", h.dataDir)
}
