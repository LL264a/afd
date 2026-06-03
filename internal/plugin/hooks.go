package plugin

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"sync"

	"github.com/nexus-dl/afd/pkg/logger"
)

const (
	PreDownloadHook  = "pre_download"
	PostDownloadHook = "post_download"
	PreUploadHook    = "pre_upload"
	PostUploadHook   = "post_upload"
)

type HookHandler func(ctx context.Context, data interface{}) error

type Hook struct {
	Name     string
	Priority int
	Handler  HookHandler
}

type HookSystem struct {
	hooks map[string][]Hook
	mu    sync.RWMutex
}

func NewHookSystem() *HookSystem {
	return &HookSystem{
		hooks: make(map[string][]Hook),
	}
}

func (hs *HookSystem) Register(event string, handler HookHandler) {
	hs.RegisterWithPriority(event, handler, 0)
}

func (hs *HookSystem) RegisterWithPriority(event string, handler HookHandler, priority int) {
	hs.mu.Lock()
	defer hs.mu.Unlock()

	hook := Hook{
		Name:     event,
		Priority: priority,
		Handler:  handler,
	}

	hooks := hs.hooks[event]
	hooks = append(hooks, hook)

	sort.Slice(hooks, func(i, j int) bool {
		return hooks[i].Priority < hooks[j].Priority
	})

	hs.hooks[event] = hooks
	logger.Log.Debugf("Registered hook: %s with priority %d", event, priority)
}

func (hs *HookSystem) Unregister(event string, handler HookHandler) {
	hs.mu.Lock()
	defer hs.mu.Unlock()

	hooks := hs.hooks[event]
	for i, h := range hooks {
		if reflect.ValueOf(h.Handler) == reflect.ValueOf(handler) {
			hs.hooks[event] = append(hooks[:i], hooks[i+1:]...)
			logger.Log.Debugf("Unregistered hook: %s", event)
			return
		}
	}
}

func (hs *HookSystem) Execute(ctx context.Context, event string, data interface{}) error {
	hs.mu.RLock()
	hooks := make([]Hook, len(hs.hooks[event]))
	copy(hooks, hs.hooks[event])
	hs.mu.RUnlock()

	if len(hooks) == 0 {
		return nil
	}

	var errs []error
	for _, hook := range hooks {
		if err := hook.Handler(ctx, data); err != nil {
			errs = append(errs, fmt.Errorf("hook %s: %w", event, err))
			logger.Log.Errorf("Hook %s failed: %v", event, err)
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("hook execution failed: %v", errs)
	}

	return nil
}

func (hs *HookSystem) ListHooks(event string) []Hook {
	hs.mu.RLock()
	defer hs.mu.RUnlock()
	hooks := make([]Hook, len(hs.hooks[event]))
	copy(hooks, hs.hooks[event])
	return hooks
}

func (hs *HookSystem) Clear(event string) {
	hs.mu.Lock()
	defer hs.mu.Unlock()
	delete(hs.hooks, event)
	logger.Log.Debugf("Cleared all hooks for event: %s", event)
}

func (hs *HookSystem) ClearAll() {
	hs.mu.Lock()
	defer hs.mu.Unlock()
	hs.hooks = make(map[string][]Hook)
	logger.Log.Debugf("Cleared all hooks")
}
