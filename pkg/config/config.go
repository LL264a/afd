package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

type ProxyConfig struct {
	Type        string   `json:"type" yaml:"type"`
	Host        string   `json:"host" yaml:"host"`
	Port        int      `json:"port" yaml:"port"`
	Username    string   `json:"username,omitempty" yaml:"username,omitempty"`
	Password    string   `json:"password,omitempty" yaml:"password,omitempty"`
	ExcludeList []string `json:"exclude_list,omitempty" yaml:"exclude_list,omitempty"`
	UseDigest   bool     `json:"use_digest,omitempty" yaml:"use_digest,omitempty"`
}

func (p *ProxyConfig) IsValid() bool {
	if p == nil {
		return false
	}
	if p.Host == "" || p.Port <= 0 || p.Port > 65535 {
		return false
	}
	if p.Type != "http" && p.Type != "https" && p.Type != "socks5" && p.Type != "socks4" && p.Type != "socks4a" {
		return false
	}
	return true
}

func (p *ProxyConfig) ShouldExclude(host string) bool {
	if p == nil || len(p.ExcludeList) == 0 {
		return false
	}
	for _, pattern := range p.ExcludeList {
		if strings.Contains(host, pattern) {
			return true
		}
	}
	return false
}

type BTConfig struct {
	Enabled            bool          `json:"enabled" yaml:"enabled"`
	DownloadSpeedLimit int64         `json:"download_speed_limit" yaml:"download_speed_limit"`
	UploadSpeedLimit   int64         `json:"upload_speed_limit" yaml:"upload_speed_limit"`
	Port               int           `json:"port" yaml:"port"`
	DataDir            string        `json:"data_dir" yaml:"data_dir"`
	TorrentFilesDir    string        `json:"torrent_files_dir" yaml:"torrent_files_dir"`
	MaxConnections     int           `json:"max_connections" yaml:"max_connections"`
	MaxPeers           int           `json:"max_peers" yaml:"max_peers"`
	SeedRatio          float64       `json:"seed_ratio" yaml:"seed_ratio"`
	SeedTime           time.Duration `json:"seed_time" yaml:"seed_time"`
	TrackerList        []string      `json:"tracker_list" yaml:"tracker_list"`
	DHTEnabled         bool          `json:"dht_enabled" yaml:"dht_enabled"`
	DHTNodes           []string      `json:"dht_nodes" yaml:"dht_nodes"`
	DisableTCP         bool          `json:"disable_tcp" yaml:"disable_tcp"`
	DisableUTP         bool          `json:"disable_utp" yaml:"disable_utp"`
	PieceLength        int64         `json:"piece_length" yaml:"piece_length"`
	SequentialDownload bool          `json:"sequential_download" yaml:"sequential_download"`
	FirstPiecePriority bool          `json:"first_piece_priority" yaml:"first_piece_priority"`
	UPNPEnabled        bool          `json:"upnp_enabled" yaml:"upnp_enabled"`
	LocalPeerDiscovery bool          `json:"local_peer_discovery" yaml:"local_peer_discovery"`
	EnableSeeding      bool          `json:"enable_seeding" yaml:"enable_seeding"`
}

type ScheduleSpeedLimit struct {
	StartTime string `json:"start_time" yaml:"start_time"`
	EndTime   string `json:"end_time" yaml:"end_time"`
	Limit     int64  `json:"limit" yaml:"limit"`
	Weekday   *int   `json:"weekday,omitempty" yaml:"weekday,omitempty"`
}

func (c *BTConfig) Validate() error {
	if c.Port < 0 || c.Port > 65535 {
		return fmt.Errorf("bt port must be between 0 and 65535")
	}
	if c.DownloadSpeedLimit < 0 || c.UploadSpeedLimit < 0 {
		return fmt.Errorf("bt speed limits must be non-negative")
	}
	if c.MaxConnections < 0 || c.MaxConnections > 1000 {
		return fmt.Errorf("bt max_connections must be between 0 and 1000")
	}
	if c.MaxPeers < 0 || c.MaxPeers > 1000 {
		return fmt.Errorf("bt max_peers must be between 0 and 1000")
	}
	if c.SeedRatio < 0 || c.SeedRatio > 100 {
		return fmt.Errorf("seed_ratio must be between 0 and 100")
	}
	return nil
}

type DownloadConfig struct {
	URL                 string               `json:"url,omitempty" yaml:"url,omitempty"`
	OutputPath          string               `json:"output_path,omitempty" yaml:"output_path,omitempty"`
	MaxConnections      int                  `json:"max_connections" yaml:"max_connections"`
	MinChunkSize        int64                `json:"min_chunk_size" yaml:"min_chunk_size"`
	MaxChunkSize        int64                `json:"max_chunk_size" yaml:"max_chunk_size"`
	DefaultChunkSize    int64                `json:"default_chunk_size" yaml:"default_chunk_size"`
	BufferSize          int                  `json:"buffer_size" yaml:"buffer_size"`
	Timeout             time.Duration        `json:"timeout" yaml:"timeout"`
	RetryCount          int                  `json:"retry_count" yaml:"retry_count"`
	SpeedLimit          int64                `json:"speed_limit" yaml:"speed_limit"`
	Proxy               *ProxyConfig         `json:"proxy,omitempty" yaml:"proxy,omitempty"`
	BT                  *BTConfig            `json:"bt,omitempty" yaml:"bt,omitempty"`
	PostProcess         *PostProcessConfig   `json:"post_process,omitempty" yaml:"post_process,omitempty"`
	MinSpeed            int64                `json:"min_speed" yaml:"min_speed"`
	MinSpeedTimeout     time.Duration        `json:"min_speed_timeout" yaml:"min_speed_timeout"`
	PreallocateSpace    bool                 `json:"preallocate_space" yaml:"preallocate_space"`
	SparseFile          bool                 `json:"sparse_file" yaml:"sparse_file"`
	FileMode            os.FileMode          `json:"file_mode" yaml:"file_mode"`
	IncludeConfig       []string             `json:"include_config,omitempty" yaml:"include_config,omitempty"`
	MaxPerServerConn    int                  `json:"max_per_server_conn" yaml:"max_per_server_conn"`
	ScheduleSpeedLimits []ScheduleSpeedLimit `json:"schedule_speed_limits,omitempty" yaml:"schedule_speed_limits,omitempty"`
	UseDigestAuth       bool                 `json:"use_digest_auth" yaml:"use_digest_auth"`
	Adaptive            bool                 `json:"adaptive" yaml:"adaptive"`
}

func (c *DownloadConfig) Validate() error {
	if c.MaxConnections < 1 || c.MaxConnections > 64 {
		return fmt.Errorf("max_connections must be between 1 and 64")
	}
	if c.MinChunkSize < 0 {
		return fmt.Errorf("min_chunk_size must be non-negative")
	}
	if c.MaxChunkSize < c.MinChunkSize {
		return fmt.Errorf("max_chunk_size must be >= min_chunk_size")
	}
	if c.DefaultChunkSize < c.MinChunkSize || c.DefaultChunkSize > c.MaxChunkSize {
		return fmt.Errorf("default_chunk_size must be between min_chunk_size and max_chunk_size")
	}
	if c.BufferSize < 1024 {
		return fmt.Errorf("buffer_size must be at least 1024 bytes")
	}
	if c.Timeout < time.Second {
		return fmt.Errorf("timeout must be at least 1 second")
	}
	if c.RetryCount < 0 || c.RetryCount > 100 {
		return fmt.Errorf("retry_count must be between 0 and 100")
	}
	if c.BT != nil {
		if err := c.BT.Validate(); err != nil {
			return fmt.Errorf("bt config: %w", err)
		}
	}
	return nil
}

type NodeConfig struct {
	ID       string      `json:"id" yaml:"id"`
	Name     string      `json:"name" yaml:"name"`
	LogLevel string      `json:"log_level" yaml:"log_level"`
	DataDir  string      `json:"data_dir" yaml:"data_dir"`
	Proxy    ProxyConfig `json:"proxy,omitempty" yaml:"proxy,omitempty"`
}

func (c *NodeConfig) Validate() error {
	if c.ID == "" {
		return fmt.Errorf("node id cannot be empty")
	}
	if c.Name == "" {
		return fmt.Errorf("node name cannot be empty")
	}
	if c.DataDir == "" {
		return fmt.Errorf("data_dir cannot be empty")
	}
	return nil
}

type APIConfig struct {
	Port               int      `json:"port" yaml:"port"`
	Host               string   `json:"host" yaml:"host"`
	AuthToken          string   `json:"auth_token,omitempty" yaml:"auth_token,omitempty"`
	RateLimit          int      `json:"rate_limit" yaml:"rate_limit"`
	EnableCORS         bool     `json:"enable_cors" yaml:"enable_cors"`
	CORSAllowedOrigins []string `json:"cors_allowed_origins,omitempty" yaml:"cors_allowed_origins,omitempty"`
}

func (c *APIConfig) Validate() error {
	if c.Port < 1 || c.Port > 65535 {
		return fmt.Errorf("port must be between 1 and 65535")
	}
	if c.RateLimit < 0 {
		return fmt.Errorf("rate_limit must be non-negative")
	}
	return nil
}

type ClusterConfig struct {
	GRPCPort      int           `json:"grpc_port" yaml:"grpc_port"`
	DiscoveryPort int           `json:"discovery_port" yaml:"discovery_port"`
	JoinPeers     []string      `json:"join_peers,omitempty" yaml:"join_peers,omitempty"`
	NodeTimeout   time.Duration `json:"node_timeout" yaml:"node_timeout"`
}

func (c *ClusterConfig) Validate() error {
	if c.GRPCPort < 1 || c.GRPCPort > 65535 {
		return fmt.Errorf("grpc_port must be between 1 and 65535")
	}
	if c.DiscoveryPort < 1 || c.DiscoveryPort > 65535 {
		return fmt.Errorf("discovery_port must be between 1 and 65535")
	}
	if c.NodeTimeout < time.Second {
		return fmt.Errorf("node_timeout must be at least 1 second")
	}
	return nil
}

type NATConfig struct {
	Enabled         bool     `json:"enabled" yaml:"enabled"`
	STUNServer      string   `json:"stun_server,omitempty" yaml:"stun_server,omitempty"`
	SignalingServer string   `json:"signaling_server,omitempty" yaml:"signaling_server,omitempty"`
	RelayServer     string   `json:"relay_server,omitempty" yaml:"relay_server,omitempty"`
	STUNServers     []string `json:"stun_servers,omitempty" yaml:"stun_servers,omitempty"`
}

func (c *NATConfig) Validate() error {
	return nil
}

type Config struct {
	Node     NodeConfig     `json:"node" yaml:"node"`
	API      APIConfig      `json:"api" yaml:"api"`
	Cluster  ClusterConfig  `json:"cluster" yaml:"cluster"`
	Download DownloadConfig `json:"download" yaml:"download"`
}

func (c *Config) Validate() error {
	if err := c.Node.Validate(); err != nil {
		return fmt.Errorf("node config: %w", err)
	}
	if err := c.API.Validate(); err != nil {
		return fmt.Errorf("api config: %w", err)
	}
	if err := c.Cluster.Validate(); err != nil {
		return fmt.Errorf("cluster config: %w", err)
	}
	if err := c.Download.Validate(); err != nil {
		return fmt.Errorf("download config: %w", err)
	}
	return nil
}

func DefaultBTConfig() *BTConfig {
	return &BTConfig{
		Enabled:            true,
		DownloadSpeedLimit: 0,
		UploadSpeedLimit:   0,
		Port:               6881,
		DataDir:            "./bt-data",
		TorrentFilesDir:    "./torrents",
		MaxConnections:     100,
		MaxPeers:           100,
		SeedRatio:          1.0,
		SeedTime:           24 * time.Hour,
		TrackerList:        []string{},
		DHTEnabled:         true,
		DHTNodes: []string{
			"router.bittorrent.com:6881",
			"dht.transmissionbt.com:6881",
			"router.utorrent.com:6881",
		},
		DisableTCP:         false,
		DisableUTP:         false,
		PieceLength:        0,
		SequentialDownload: false,
		FirstPiecePriority: false,
		UPNPEnabled:        true,
		LocalPeerDiscovery: true,
		EnableSeeding:      true,
	}
}

func DefaultDownloadConfig() *DownloadConfig {
	return &DownloadConfig{
		MaxConnections:      8,
		MinChunkSize:        1024 * 1024,      // 1 MB
		MaxChunkSize:        50 * 1024 * 1024, // 50 MB
		DefaultChunkSize:    10 * 1024 * 1024, // 10 MB
		BufferSize:          32 * 1024,        // 32 KB
		Timeout:             30 * time.Second,
		RetryCount:          3,
		SpeedLimit:          0,
		BT:                  DefaultBTConfig(),
		PostProcess:         DefaultPostProcessConfig(),
		MinSpeed:            0,
		MinSpeedTimeout:     30 * time.Second,
		PreallocateSpace:    false,
		SparseFile:          false,
		FileMode:            0644,
		IncludeConfig:       []string{},
		MaxPerServerConn:    0,
		ScheduleSpeedLimits: []ScheduleSpeedLimit{},
		UseDigestAuth:       false,
		Adaptive:            false,
	}
}

func DefaultConfig() *Config {
	return &Config{
		Node: NodeConfig{
			ID:       "nexus-node-1",
			Name:     "nexus-node",
			LogLevel: "info",
			DataDir:  "./data",
		},
		API: APIConfig{
			Port:       8080,
			Host:       "0.0.0.0",
			AuthToken:  "",
			RateLimit:  100,
			EnableCORS: true,
			// Allow the standalone UI's typical dev / prod origins by
			// default; override in production via NEXUS_API_CORS_ALLOWED_ORIGINS
			// or the config file.  An empty / unset value still allows
			// the wildcard for backward compatibility, but the moment
			// a non-empty list is supplied the wildcard is dropped
			// (see internal/api/middleware.go CORS).
			CORSAllowedOrigins: []string{
				"http://localhost:5173",
				"http://127.0.0.1:5173",
			},
		},
		Cluster: ClusterConfig{
			GRPCPort:      50051,
			DiscoveryPort: 50052,
			JoinPeers:     []string{},
			NodeTimeout:   30 * time.Second,
		},
		Download: *DefaultDownloadConfig(),
	}
}

func Load(path string) (*Config, error) {
	cfg := DefaultConfig()
	loadedPaths := make(map[string]bool) // 防止循环 include

	// When no explicit path is supplied, try NEXUS_CONFIG_FILE,
	// then the conventional locations, before giving up.  This
	// mirrors the precedence documented in README and keeps
	// config-reload working in production deployments where the
	// config path was supplied through the environment.
	if path == "" {
		path = os.Getenv("NEXUS_CONFIG_FILE")
	}
	if path == "" {
		for _, candidate := range []string{"/etc/nexus-dl/config.yaml", "./config.yaml", "./config.yml"} {
			if _, err := os.Stat(candidate); err == nil {
				path = candidate
				break
			}
		}
	}
	if path != "" {
		if err := loadConfigFile(path, cfg, loadedPaths); err != nil {
			return nil, err
		}
	}

	applyEnvOverrides(cfg)

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	return cfg, nil
}

func loadConfigFile(path string, cfg *Config, loadedPaths map[string]bool) error {
	absPath, err := filepath.Abs(path)
	if err != nil {
		return fmt.Errorf("invalid config path: %w", err)
	}

	// 防止循环 include
	if loadedPaths[absPath] {
		return fmt.Errorf("circular include detected: %s", absPath)
	}
	loadedPaths[absPath] = true

	data, err := os.ReadFile(absPath)
	if err != nil {
		return fmt.Errorf("failed to read config file %s: %w", absPath, err)
	}

	// 先解析到临时配置，获取 include 列表
	tempCfg := DefaultConfig()
	if err := parseConfig(data, filepath.Ext(absPath), tempCfg); err != nil {
		return fmt.Errorf("failed to parse config file %s: %w", absPath, err)
	}

	// 先加载 include 的配置（优先级更低）
	baseDir := filepath.Dir(absPath)
	for _, includePath := range tempCfg.Download.IncludeConfig {
		// 相对路径转换为绝对路径
		includeAbsPath := includePath
		if !filepath.IsAbs(includePath) {
			includeAbsPath = filepath.Join(baseDir, includePath)
		}
		if err := loadConfigFile(includeAbsPath, cfg, loadedPaths); err != nil {
			return err
		}
	}

	// 再合并当前配置（优先级更高）
	if err := parseConfig(data, filepath.Ext(absPath), cfg); err != nil {
		return fmt.Errorf("failed to parse config file %s: %w", absPath, err)
	}

	return nil
}

func parseConfig(data []byte, ext string, cfg *Config) error {
	switch strings.ToLower(ext) {
	case ".yaml", ".yml":
		return yaml.Unmarshal(data, cfg)
	case ".json":
		return json.Unmarshal(data, cfg)
	default:
		if err := yaml.Unmarshal(data, cfg); err != nil {
			if jsonErr := json.Unmarshal(data, cfg); jsonErr != nil {
				return fmt.Errorf("unsupported config format: %s", ext)
			}
		}
	}
	return nil
}

func applyEnvOverrides(cfg *Config) {
	if v := os.Getenv("NEXUS_NODE_ID"); v != "" {
		cfg.Node.ID = v
	}
	if v := os.Getenv("NEXUS_NODE_NAME"); v != "" {
		cfg.Node.Name = v
	}
	if v := os.Getenv("NEXUS_NODE_LOG_LEVEL"); v != "" {
		cfg.Node.LogLevel = v
	}
	if v := os.Getenv("NEXUS_NODE_DATA_DIR"); v != "" {
		cfg.Node.DataDir = v
	}
	if v := os.Getenv("NEXUS_API_HOST"); v != "" {
		cfg.API.Host = v
	}
	if v := os.Getenv("NEXUS_API_PORT"); v != "" {
		if port, err := strconv.Atoi(v); err == nil {
			cfg.API.Port = port
		}
	}
	if v := os.Getenv("NEXUS_API_AUTH_TOKEN"); v != "" {
		cfg.API.AuthToken = v
	}
	if v := os.Getenv("NEXUS_API_RATE_LIMIT"); v != "" {
		if limit, err := strconv.Atoi(v); err == nil {
			cfg.API.RateLimit = limit
		}
	}
	if v := os.Getenv("NEXUS_CLUSTER_GRPC_PORT"); v != "" {
		if port, err := strconv.Atoi(v); err == nil {
			cfg.Cluster.GRPCPort = port
		}
	}
	if v := os.Getenv("NEXUS_CLUSTER_DISCOVERY_PORT"); v != "" {
		if port, err := strconv.Atoi(v); err == nil {
			cfg.Cluster.DiscoveryPort = port
		}
	}
	if v := os.Getenv("NEXUS_DOWNLOAD_MAX_CONNECTIONS"); v != "" {
		if conn, err := strconv.Atoi(v); err == nil {
			cfg.Download.MaxConnections = conn
		}
	}
	if v := os.Getenv("NEXUS_DOWNLOAD_TIMEOUT"); v != "" {
		if timeout, err := strconv.Atoi(v); err == nil {
			cfg.Download.Timeout = time.Duration(timeout) * time.Second
		}
	}
	if v := os.Getenv("NEXUS_DOWNLOAD_RETRY_COUNT"); v != "" {
		if count, err := strconv.Atoi(v); err == nil {
			cfg.Download.RetryCount = count
		}
	}
	if v := os.Getenv("NEXUS_DOWNLOAD_SPEED_LIMIT"); v != "" {
		if limit, err := strconv.ParseInt(v, 10, 64); err == nil {
			cfg.Download.SpeedLimit = limit
		}
	}
}
