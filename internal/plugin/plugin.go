package plugin

import (
	"context"
	"fmt"
	"plugin"
	"sync"

	"github.com/nexus-dl/afd/pkg/logger"
)

type Plugin interface {
	Name() string
	Version() string
	Init(ctx context.Context) error
	Download(ctx context.Context, taskID string) error
}

type DownloadTask struct {
	ID       string
	URL      string
	FilePath string
	Status   string
	Progress int
}

type PluginLoader struct {
	plugins map[string]Plugin
	mu      sync.RWMutex
}

func NewPluginLoader() *PluginLoader {
	return &PluginLoader{
		plugins: make(map[string]Plugin),
	}
}

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

	if err := p.Init(context.Background()); err != nil {
		return nil, fmt.Errorf("failed to initialize plugin: %w", err)
	}

	pl.mu.Lock()
	pl.plugins[p.Name()] = p
	pl.mu.Unlock()

	logger.Log.Infof("Loaded plugin: %s v%s", p.Name(), p.Version())
	return p, nil
}

func (pl *PluginLoader) Load(p Plugin) error {
	if err := p.Init(context.Background()); err != nil {
		return fmt.Errorf("failed to initialize plugin: %w", err)
	}

	pl.mu.Lock()
	pl.plugins[p.Name()] = p
	pl.mu.Unlock()

	logger.Log.Infof("Loaded plugin: %s v%s", p.Name(), p.Version())
	return nil
}

func (pl *PluginLoader) Get(name string) (Plugin, bool) {
	pl.mu.RLock()
	defer pl.mu.RUnlock()
	p, ok := pl.plugins[name]
	return p, ok
}

func (pl *PluginLoader) List() []Plugin {
	pl.mu.RLock()
	defer pl.mu.RUnlock()

	result := make([]Plugin, 0, len(pl.plugins))
	for _, p := range pl.plugins {
		result = append(result, p)
	}
	return result
}

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

type PluginManager struct {
	loader     *PluginLoader
	hookSystem *HookSystem
}

func NewPluginManager() *PluginManager {
	return &PluginManager{
		loader:     NewPluginLoader(),
		hookSystem: NewHookSystem(),
	}
}

func (pm *PluginManager) Loader() *PluginLoader {
	return pm.loader
}

func (pm *PluginManager) Hooks() *HookSystem {
	return pm.hookSystem
}

func (pm *PluginManager) RegisterHook(event string, handler HookHandler) {
	pm.hookSystem.Register(event, handler)
}

func (pm *PluginManager) ExecutePreDownloadHooks(ctx context.Context, task *DownloadTask) error {
	return pm.hookSystem.Execute(ctx, PreDownloadHook, task)
}

func (pm *PluginManager) ExecutePostDownloadHooks(ctx context.Context, task *DownloadTask) error {
	return pm.hookSystem.Execute(ctx, PostDownloadHook, task)
}