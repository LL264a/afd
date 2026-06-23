package plugin

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"plugin"
	"sync"

	"github.com/nexus-dl/afd/pkg/logger"
)

type Registry struct {
	mu         sync.RWMutex
	builtin    map[string]PluginFactory
	discovered map[string]Plugin
}

type PluginFactory func() Plugin

func NewRegistry() *Registry {
	return &Registry{
		builtin:    make(map[string]PluginFactory),
		discovered: make(map[string]Plugin),
	}
}

func (r *Registry) RegisterBuiltin(name string, factory PluginFactory) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.builtin[name] = factory
	logger.Log.Infof("Registered builtin plugin factory: %s", name)
}

func (r *Registry) GetBuiltin(name string) (PluginFactory, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	factory, ok := r.builtin[name]
	return factory, ok
}

func (r *Registry) ListBuiltin() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	names := make([]string, 0, len(r.builtin))
	for name := range r.builtin {
		names = append(names, name)
	}
	return names
}

// LoadFromFile 从共享库文件加载插件
func (r *Registry) LoadFromFile(path string) (Plugin, error) {
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

	return p, nil
}

func (r *Registry) DiscoverFromDir(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			logger.Log.Infof("Plugin directory does not exist: %s", dir)
			return nil
		}
		return fmt.Errorf("failed to read plugin directory: %w", err)
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		ext := filepath.Ext(entry.Name())
		if ext != ".so" && ext != ".dll" && ext != ".dylib" {
			continue
		}

		path := filepath.Join(dir, entry.Name())
		p, err := r.LoadFromFile(path)
		if err != nil {
			logger.Log.Warnw("failed to load discovered plugin", "path", path, "error", err)
			continue
		}
		r.mu.Lock()
		r.discovered[p.Name()] = p
		r.mu.Unlock()
		logger.Log.Infof("Discovered and loaded plugin: %s", path)
	}

	return nil
}

func (r *Registry) DiscoverFromFS(fsys fs.FS, pattern string) error {
	readDirFS, ok := fsys.(fs.ReadDirFS)
	if !ok {
		return fmt.Errorf("filesystem does not support ReadDirFS")
	}

	entries, err := readDirFS.ReadDir(".")
	if err != nil {
		return fmt.Errorf("failed to read filesystem: %w", err)
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		name := entry.Name()
		matched, err := filepath.Match(pattern, name)
		if err != nil {
			continue
		}

		if matched {
			logger.Log.Infof("Discovered plugin in FS: %s", name)
		}
	}

	return nil
}

func (r *Registry) LoadBuiltin(name string) (Plugin, error) {
	r.mu.RLock()
	factory, ok := r.builtin[name]
	r.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("builtin plugin not found: %s", name)
	}

	plugin := factory()
	if err := plugin.Init(context.Background()); err != nil {
		return nil, fmt.Errorf("failed to initialize plugin: %w", err)
	}

	r.mu.Lock()
	r.discovered[name] = plugin
	r.mu.Unlock()

	logger.Log.Infof("Loaded builtin plugin: %s v%s", plugin.Name(), plugin.Version())
	return plugin, nil
}

func (r *Registry) GetDiscovered(name string) (Plugin, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	p, ok := r.discovered[name]
	return p, ok
}

func (r *Registry) ListDiscovered() []Plugin {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result := make([]Plugin, 0, len(r.discovered))
	for _, p := range r.discovered {
		result = append(result, p)
	}
	return result
}

func (r *Registry) Unload(name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	p, ok := r.discovered[name]
	if !ok {
		return fmt.Errorf("plugin not loaded: %s", name)
	}

	if closer, ok := p.(interface{ Close() error }); ok {
		if err := closer.Close(); err != nil {
			logger.Log.Warnf("Error closing plugin %s: %v", name, err)
		}
	}

	delete(r.discovered, name)
	logger.Log.Infof("Unloaded plugin: %s", name)
	return nil
}

var defaultRegistry = NewRegistry()

func DefaultRegistry() *Registry {
	return defaultRegistry
}

func RegisterBuiltinPlugin(name string, factory PluginFactory) {
	defaultRegistry.RegisterBuiltin(name, factory)
}

func LoadBuiltinPlugin(name string) (Plugin, error) {
	return defaultRegistry.LoadBuiltin(name)
}

func DiscoverPlugins(dir string) error {
	return defaultRegistry.DiscoverFromDir(dir)
}
