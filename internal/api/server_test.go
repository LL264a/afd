package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestIsValidURL(t *testing.T) {
	tests := []struct {
		url  string
		want bool
	}{
		{"https://example.com/file.zip", true},
		{"http://example.com/file.zip", true},
		{"ftp://example.com/file.zip", true},
		{"s3://bucket/key", true},
		{"webdav://example.com/file", true},
		{"magnet:?xt=urn:btih:abc123", true},
		{"", false},
		{"not-a-url", false},
		{"magnet:invalid", false},
		{"file:///etc/passwd", false},
	}

	for _, tt := range tests {
		got := isValidURL(tt.url)
		if got != tt.want {
			t.Errorf("isValidURL(%q) = %v, want %v", tt.url, got, tt.want)
		}
	}
}

func TestIsSafePath(t *testing.T) {
	tests := []struct {
		path string
		want bool
	}{
		{"/tmp/downloads", true},
		{"downloads/file.zip", true},
		{"../etc/passwd", false},
		{"../../secret", false},
		{"./safe/path", true},
	}

	for _, tt := range tests {
		got := isSafePath(tt.path)
		if got != tt.want {
			t.Errorf("isSafePath(%q) = %v, want %v", tt.path, got, tt.want)
		}
	}
}

func TestSendError(t *testing.T) {
	rec := httptest.NewRecorder()
	sendError(rec, http.StatusBadRequest, "test error", "details")

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d", http.StatusBadRequest, rec.Code)
	}

	contentType := rec.Header().Get("Content-Type")
	if contentType != "application/json" {
		t.Fatalf("expected Content-Type application/json, got %s", contentType)
	}

	var resp ErrorResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	if resp.Error != "test error" {
		t.Errorf("expected error 'test error', got %q", resp.Error)
	}
	if resp.Code != http.StatusBadRequest {
		t.Errorf("expected code %d, got %d", http.StatusBadRequest, resp.Code)
	}
	if resp.Details != "details" {
		t.Errorf("expected details 'details', got %q", resp.Details)
	}
}
