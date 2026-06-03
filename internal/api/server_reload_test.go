package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/nexus-dl/afd/pkg/config"
	"github.com/nexus-dl/afd/pkg/logger"
)

func writeConfig(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	f := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(f, []byte(content), 0600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return f
}

const validConfigYAML = `
node:
  name: "test-node"
  data_dir: "./data"
  log_level: "info"
api:
  host: "127.0.0.1"
  port: 18080
  auth_token: "secret"
cluster:
  enabled: false
download:
  default_concurrency: 4
  max_concurrent_tasks: 100
`

func TestHandleReloadConfig_AppliesNewValues(t *testing.T) {
	if err := logger.Init("info", ""); err != nil {
		t.Fatalf("logger: %v", err)
	}
	path := writeConfig(t, validConfigYAML)
	t.Setenv("NEXUS_CONFIG_FILE", path)

	cfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}

	s := &Server{config: cfg}

	body := strings.NewReader("")
	req := httptest.NewRequest("POST", "/api/config/reload", body)
	rr := httptest.NewRecorder()
	s.handleReloadConfig(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	var resp map[string]string
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp["status"] != "ok" {
		t.Fatalf("resp: %+v", resp)
	}
	if !strings.Contains(resp["message"], "restart") {
		t.Fatalf("response should mention restart caveats, got %q", resp["message"])
	}
}

func TestHandleReloadConfig_RejectsBadConfig(t *testing.T) {
	if err := logger.Init("info", ""); err != nil {
		t.Fatalf("logger: %v", err)
	}
	path := writeConfig(t, validConfigYAML)
	t.Setenv("NEXUS_CONFIG_FILE", path)

	cfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}
	originalDataDir := cfg.Node.DataDir
	s := &Server{config: cfg}

	badYAML := `
node:
  data_dir: "../escape"
  log_level: "info"
  name: ""
api:
  port: 70000
`
	badPath := writeConfig(t, badYAML)
	t.Setenv("NEXUS_CONFIG_FILE", badPath)

	body := bytes.NewReader(nil)
	req := httptest.NewRequest("POST", "/api/config/reload", body)
	rr := httptest.NewRecorder()
	s.handleReloadConfig(rr, req)

	if rr.Code == http.StatusOK {
		t.Fatalf("expected non-2xx for bad config, got %d body=%s", rr.Code, rr.Body.String())
	}
	if s.config.Node.DataDir != originalDataDir {
		t.Fatalf("in-memory config was mutated on rejected reload: %q", s.config.Node.DataDir)
	}
}

func TestHandleReloadConfig_RejectsWrongMethod(t *testing.T) {
	cfg := &config.Config{}
	s := &Server{config: cfg}

	req := httptest.NewRequest("GET", "/api/config/reload", nil)
	rr := httptest.NewRecorder()
	s.handleReloadConfig(rr, req)
	if rr.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status=%d, want 405", rr.Code)
	}
}
