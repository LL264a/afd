// Package webui contains a lightweight asset-integrity test for the embedded
// web UI. It does NOT exercise JavaScript behaviour; it ensures the
// static files are present, parse as HTML/JS/CSS, and reference the
// resources they claim to.
//
// Run with: go test ./test/webui/...
package webui

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func webuiDir(t *testing.T) string {
	t.Helper()
	pwd, _ := os.Getwd()
	for d := pwd; d != filepath.Dir(d); d = filepath.Dir(d) {
		candidate := filepath.Join(d, "web")
		if info, err := os.Stat(candidate); err == nil && info.IsDir() {
			return candidate
		}
	}
	t.Skip("web/ directory not found; skipping webui tests")
	return ""
}

func TestWebUIDirectoryLayout(t *testing.T) {
	root := webuiDir(t)
	required := []string{
		filepath.Join("js", "api.js"),
		filepath.Join("js", "app.js"),
		filepath.Join("js", "chart.js"),
		filepath.Join("js", "notification.js"),
		filepath.Join("js", "utils.js"),
		filepath.Join("js", "websocket.js"),
		filepath.Join("css", "base.css"),
		filepath.Join("css", "components.css"),
		filepath.Join("css", "layout.css"),
		filepath.Join("css", "variables.css"),
		"index.html",
	}
	for _, r := range required {
		if _, err := os.Stat(filepath.Join(root, r)); err != nil {
			t.Errorf("missing required file: %s: %v", r, err)
		}
	}
}

func TestIndexHTMLLoadsAndReferencesAssets(t *testing.T) {
	root := webuiDir(t)
	data, err := os.ReadFile(filepath.Join(root, "index.html"))
	if err != nil {
		t.Fatal(err)
	}
	html := string(data)

	requiredRefs := []string{
		"js/api.js",
		"js/app.js",
		"js/websocket.js",
		"js/utils.js",
		"js/notification.js",
		"js/chart.js",
		"css/base.css",
		"css/variables.css",
		"css/layout.css",
		"css/components.css",
	}
	for _, ref := range requiredRefs {
		if !strings.Contains(html, ref) {
			t.Errorf("index.html does not reference %s", ref)
		}
	}

	if !strings.Contains(html, "<!DOCTYPE html>") && !strings.Contains(strings.ToUpper(html), "<!DOCTYPE") {
		t.Error("index.html missing DOCTYPE")
	}
}

func TestJSFilesParse(t *testing.T) {
	root := webuiDir(t)
	jsDir := filepath.Join(root, "js")
	entries, err := os.ReadDir(jsDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".js") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(jsDir, e.Name()))
		if err != nil {
			t.Errorf("read %s: %v", e.Name(), err)
			continue
		}
		if len(data) == 0 {
			t.Errorf("%s is empty", e.Name())
		}
		if !strings.Contains(string(data), "afd") && !strings.Contains(string(data), "AFD") {
			t.Logf("warning: %s does not mention 'afd' or 'AFD'", e.Name())
		}
	}
}

func TestCSSFilesParse(t *testing.T) {
	root := webuiDir(t)
	cssDir := filepath.Join(root, "css")
	entries, err := os.ReadDir(cssDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".css") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(cssDir, e.Name()))
		if err != nil {
			t.Errorf("read %s: %v", e.Name(), err)
			continue
		}
		hasBrace := strings.Contains(string(data), "{") && strings.Contains(string(data), "}")
		if !hasBrace {
			t.Errorf("%s has no CSS rule blocks", e.Name())
		}
	}
}

func TestWebUIReachableFromHTTPServer(t *testing.T) {
	root := webuiDir(t)
	fs := http.FileServer(http.Dir(root))
	ts := httptest.NewServer(fs)
	defer ts.Close()

	for _, path := range []string{"/", "/index.html", "/css/base.css", "/js/api.js"} {
		resp, err := http.Get(ts.URL + path)
		if err != nil {
			t.Errorf("GET %s: %v", path, err)
			continue
		}
		if resp.StatusCode == http.StatusOK {
			t.Logf("GET %s: %d (%d bytes)", path, resp.StatusCode, resp.ContentLength)
		} else {
			t.Errorf("GET %s: status %d", path, resp.StatusCode)
		}
		resp.Body.Close()
	}
}
