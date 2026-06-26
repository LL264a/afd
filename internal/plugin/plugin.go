package plugin

import (
	"context"
	"fmt"
	"plugin"
	"sync"
	"time"

	"github.com/nexus-dl/afd/pkg/logger"
)

const pluginInitTimeout = 30 * time.Second

// Plugin defines the interface every plugin must implement, exposing its
// metadata plus initialization and download entry points.
type Plugin interface {
	Name() string
	Version() string
	Init(ctx context.Context) error
	Download(ctx context.Context, taskID string) error
}

// DownloadTask describes a download job passed between the manager and plugins,
// tracking its identifier, source URL, local path, status and progress.
type DownloadTask struct {
	ID       string
	URL      string
	FilePath string
	Status   string
	Progress int
}

// PluginLoader loads, stores and manages the lifecycle of Plugin instances.
type PluginLoader struct {
	plugins map[string]Plugin
	mu      sync.RWMutex
}

// NewPluginLoader creates and returns a new PluginLoader instance.
func NewPluginLoader() *PluginLoader {
	return &PluginLoader{
		plugins: make(map[string]Plugin),
	}
}

// LoadFromFile opens the shared library at path, looks up its Plugin symbol,
// initializes it with a timeout, and registers it under its name.
func (pl *PluginLoader) LoadFromFile(path string) (Plugin, error) {
	plug, err := plugin.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open plugin file: %w", err)
	}

	symPlugin, err := plug.Lookup("Plugin")
	if err != nil {
		return nil, fmt.Errorf("failed to find Plugin symbol: %w", err)
	}

	p, ok := symPlugin.(Plugin)
	if !ok {
		return nil, fmt.Errorf("plugin does not implement Plugin interface")
	}

	// 为插件初始化添加超时，避免恶意或异常插件在 Init 中无限阻塞。
	initCtx, cancel := context.WithTimeout(context.Background(), pluginInitTimeout)
	defer cancel()
	if err := p.Init(initCtx); err != nil {
		return nil, fmt.Errorf("failed to initialize plugin: %w", err)
	}

	pl.mu.Lock()
	pl.plugins[p.Name()] = p
	pl.mu.Unlock()

	logger.Log.Infof("Loaded plugin: %s v%s", p.Name(), p.Version())
	return p, nil
}

// Load initializes the given Plugin with a timeout and registers it under its name.
func (pl *PluginLoader) Load(p Plugin) error {
	// 为插件初始化添加超时，避免恶意或异常插件在 Init 中无限阻塞。
	initCtx, cancel := context.WithTimeout(context.Background(), pluginInitTimeout)
	defer cancel()
	if err := p.Init(initCtx); err != nil {
		return fmt.Errorf("failed to initialize plugin: %w", err)
	}

	pl.mu.Lock()
	pl.plugins[p.Name()] = p
	pl.mu.Unlock()

	logger.Log.Infof("Loaded plugin: %s v%s", p.Name(), p.Version())
	return nil
}

// Get returns the registered plugin with the given name and whether it was found.
func (pl *PluginLoader) Get(name string) (Plugin, bool) {
	pl.mu.RLock()
	defer pl.mu.RUnlock()
	p, ok := pl.plugins[name]
	return p, ok
}

// List returns a slice of all currently registered plugins.
func (pl *PluginLoader) List() []Plugin {
	pl.mu.RLock()
	defer pl.mu.RUnlock()

	result := make([]Plugin, 0, len(pl.plugins))
	for _, p := range pl.plugins {
		result = append(result, p)
	}
	return result
}

// Unload removes the plugin with the given name, returning an error if it is not loaded.
func (pl *PluginLoader) Unload(name string) error {
	pl.mu.Lock()
	defer pl.mu.Unlock()
	if _, ok := pl.plugins[name]; !ok {
		return fmt.Errorf("plugin not found: %s", name)
	}
	delete(pl.plugins, name)
	logger.Log.Infof("Unloaded plugin: %s", name)
	return nil
}

// PluginManager combines a PluginLoader with a HookSystem, providing a single
// entry point for plugin lifecycle management and hook execution.
type PluginManager struct {
	loader     *PluginLoader
	hookSystem *HookSystem
}

// NewPluginManager creates and returns a new PluginManager with fresh loader
// and hook system instances.
func NewPluginManager() *PluginManager {
	return &PluginManager{
		loader:     NewPluginLoader(),
		hookSystem: NewHookSystem(),
	}
}

// Loader returns the manager's underlying PluginLoader.
func (pm *PluginManager) Loader() *PluginLoader {
	return pm.loader
}

// Hooks returns the manager's underlying HookSystem.
func (pm *PluginManager) Hooks() *HookSystem {
	return pm.hookSystem
}

// RegisterHook registers a hook handler for the given event on the manager's hook system.
func (pm *PluginManager) RegisterHook(event string, handler HookHandler) {
	pm.hookSystem.Register(event, handler)
}

// ExecutePreDownloadHooks runs all hooks registered for the PreDownloadHook event
// against the given download task.
func (pm *PluginManager) ExecutePreDownloadHooks(ctx context.Context, task *DownloadTask) error {
	return pm.hookSystem.Execute(ctx, PreDownloadHook, task)
}

// ExecutePostDownloadHooks runs all hooks registered for the PostDownloadHook event
// against the given download task.
func (pm *PluginManager) ExecutePostDownloadHooks(ctx context.Context, task *DownloadTask) error {
	return pm.hookSystem.Execute(ctx, PostDownloadHook, task)
}
