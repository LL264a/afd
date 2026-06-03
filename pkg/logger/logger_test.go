package logger

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestInit_ReinitClosesPreviousFile(t *testing.T) {
	dir := t.TempDir()
	f1 := filepath.Join(dir, "a.log")
	f2 := filepath.Join(dir, "b.log")

	if err := Init("info", f1); err != nil {
		t.Fatalf("Init f1: %v", err)
	}
	Log.Info("first")

	if err := Init("debug", f2); err != nil {
		t.Fatalf("Init f2: %v", err)
	}
	Log.Info("second")
	// Drain everything zap buffered and release the file handles
	// before t.TempDir cleanup runs, otherwise Windows reports
	// "file is being used by another process".
	Close()

	data1, err := os.ReadFile(f1)
	if err != nil {
		t.Fatalf("read f1: %v", err)
	}
	data2, err := os.ReadFile(f2)
	if err != nil {
		t.Fatalf("read f2: %v", err)
	}
	if !strings.Contains(string(data1), "first") {
		t.Fatalf("f1 should contain 'first', got %q", data1)
	}
	if !strings.Contains(string(data2), "second") {
		t.Fatalf("f2 should contain 'second', got %q", data2)
	}
}

func TestInit_FileMode(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX permissions not applicable")
	}
	dir := t.TempDir()
	f := filepath.Join(dir, "perm.log")
	if err := Init("info", f); err != nil {
		t.Fatalf("Init: %v", err)
	}
	info, err := os.Stat(f)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	mode := info.Mode().Perm()
	if mode != 0600 {
		t.Fatalf("log file mode = %o, want 0600", mode)
	}
}

func TestInit_StdoutOnly(t *testing.T) {
	if err := Init("info", ""); err != nil {
		t.Fatalf("Init without file: %v", err)
	}
	if Log == nil {
		t.Fatalf("Log should be non-nil")
	}
}

func TestInit_InvalidLevelDefaultsToInfo(t *testing.T) {
	if err := Init("not-a-level", ""); err != nil {
		t.Fatalf("Init: %v", err)
	}
	Log.Debug("should not appear")
	Log.Info("should appear via console")
}

func TestClose_Idempotent(t *testing.T) {
	dir := t.TempDir()
	f := filepath.Join(dir, "x.log")
	if err := Init("info", f); err != nil {
		t.Fatalf("Init: %v", err)
	}
	Close()
	Close()
	if closer != nil {
		t.Fatalf("closer should be nil after Close")
	}
}

func TestSync_AfterClose_NoPanic(t *testing.T) {
	dir := t.TempDir()
	f := filepath.Join(dir, "y.log")
	if err := Init("info", f); err != nil {
		t.Fatalf("Init: %v", err)
	}
	Close()
	Sync()
}
