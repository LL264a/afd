package config

import (
	"os"
	"testing"
	"time"
)

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	if cfg.Node.ID != "nexus-node-1" {
		t.Errorf("Expected node ID 'nexus-node-1', got '%s'", cfg.Node.ID)
	}

	if cfg.API.Port != 8080 {
		t.Errorf("Expected API port 8080, got %d", cfg.API.Port)
	}

	if cfg.Cluster.GRPCPort != 50051 {
		t.Errorf("Expected gRPC port 50051, got %d", cfg.Cluster.GRPCPort)
	}

	if cfg.Download.MaxConnections != 8 {
		t.Errorf("Expected max connections 8, got %d", cfg.Download.MaxConnections)
	}
}

func TestConfigValidation(t *testing.T) {
	cfg := DefaultConfig()

	err := cfg.Validate()
	if err != nil {
		t.Errorf("Expected valid config, got error: %v", err)
	}

	cfg.Node.ID = ""
	err = cfg.Validate()
	if err == nil {
		t.Error("Expected validation error for empty node ID")
	}

	cfg.Node.ID = "test"
	cfg.API.Port = 70000
	err = cfg.Validate()
	if err == nil {
		t.Error("Expected validation error for invalid port")
	}
}

func TestEnvOverrides(t *testing.T) {
	os.Setenv("AFD_NODE_ID", "env-test-node")
	os.Setenv("AFD_API_PORT", "9090")
	os.Setenv("AFD_DOWNLOAD_MAX_CONNECTIONS", "16")

	defer func() {
		os.Unsetenv("AFD_NODE_ID")
		os.Unsetenv("AFD_API_PORT")
		os.Unsetenv("AFD_DOWNLOAD_MAX_CONNECTIONS")
	}()

	cfg, err := Load("")
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	if cfg.Node.ID != "env-test-node" {
		t.Errorf("Expected node ID from env 'env-test-node', got '%s'", cfg.Node.ID)
	}

	if cfg.API.Port != 9090 {
		t.Errorf("Expected API port from env 9090, got %d", cfg.API.Port)
	}

	if cfg.Download.MaxConnections != 16 {
		t.Errorf("Expected max connections from env 16, got %d", cfg.Download.MaxConnections)
	}
}

func TestBTConfigValidate(t *testing.T) {
	btCfg := DefaultBTConfig()

	err := btCfg.Validate()
	if err != nil {
		t.Errorf("Expected valid BT config, got error: %v", err)
	}

	btCfg.Port = 70000
	err = btCfg.Validate()
	if err == nil {
		t.Error("Expected validation error for invalid BT port")
	}

	btCfg.Port = 6881
	btCfg.MaxConnections = 2000
	err = btCfg.Validate()
	if err == nil {
		t.Error("Expected validation error for invalid max connections")
	}
}

func TestDownloadConfigValidate(t *testing.T) {
	dlCfg := DefaultDownloadConfig()

	err := dlCfg.Validate()
	if err != nil {
		t.Errorf("Expected valid download config, got error: %v", err)
	}

	dlCfg.MaxConnections = 0
	err = dlCfg.Validate()
	if err == nil {
		t.Error("Expected validation error for invalid max connections")
	}

	dlCfg.MaxConnections = 8
	dlCfg.Timeout = 500 * time.Millisecond
	err = dlCfg.Validate()
	if err == nil {
		t.Error("Expected validation error for invalid timeout")
	}
}
