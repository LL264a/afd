package api

import (
	"fmt"
	"strings"
	"testing"
)

func TestNormalizeEndpoint_CollapsesUUIDs(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"/api/tasks/12345678-1234-1234-1234-123456789abc/pause", "/api/tasks/{id}/pause"},
		{"/api/tasks/abcdef0123456789abcdef0123456789", "/api/tasks/{id}"},
		{"/api/tasks/short", "/api/tasks/short"},
		{"/api/nodes", "/api/nodes"},
		{"/api/tasks", "/api/tasks"},
		{"", "<empty>"},
		{"/", "/"},
		{"/health", "/health"},
	}
	for _, c := range cases {
		got := NormalizeEndpoint(c.in)
		if got != c.want {
			t.Errorf("NormalizeEndpoint(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestIsLikelyID(t *testing.T) {
	yes := []string{
		"12345678-1234-1234-1234-123456789abc",
		"abcdef0123456789abcdef0123456789",
		"ABCDEF1234567890ABCDEF1234567890",
	}
	no := []string{
		"short",
		"a-b-c",
		"still_too_short",
		"1234567",
		"path-with-mixed-words-and-stuff",
	}
	for _, s := range yes {
		if !isLikelyID(s) {
			t.Errorf("isLikelyID(%q) = false, want true", s)
		}
	}
	for _, s := range no {
		if isLikelyID(s) {
			t.Errorf("isLikelyID(%q) = true, want false", s)
		}
	}
}

func TestPerTaskLRU_CapAndEviction(t *testing.T) {
	perTaskMu.Lock()
	perTaskLRU.Init()
	for k := range perTaskEntry {
		delete(perTaskEntry, k)
	}
	downloadSpeedByTask.Reset()
	downloadProgressByTask.Reset()
	perTaskMu.Unlock()

	for i := 0; i < maxPerTaskLabels+10; i++ {
		id := fmt.Sprintf("task-%d", i)
		RecordDownloadSpeed(id, int64(i*100))
	}

	perTaskMu.Lock()
	if perTaskLRU.Len() != maxPerTaskLabels {
		perTaskMu.Unlock()
		t.Fatalf("LRU size = %d, want %d", perTaskLRU.Len(), maxPerTaskLabels)
	}
	perTaskMu.Unlock()

	ForgetPerTask("task-99999")
	perTaskMu.Lock()
	if _, ok := perTaskEntry["task-99999"]; ok {
		perTaskMu.Unlock()
		t.Fatalf("task-99999 should be forgotten")
	}
	perTaskMu.Unlock()
}

func TestSplitPath(t *testing.T) {
	cases := []struct {
		in   string
		want []string
	}{
		{"a/b/c", []string{"a", "b", "c"}},
		{"/a/b/c/", []string{"a", "b", "c"}},
		{"a", []string{"a"}},
		{"/", []string{}},
		{"", []string{}},
	}
	for _, c := range cases {
		got := splitPath(c.in)
		if strings.Join(got, "/") != strings.Join(c.want, "/") {
			t.Errorf("splitPath(%q) = %v, want %v", c.in, got, c.want)
		}
	}
}
