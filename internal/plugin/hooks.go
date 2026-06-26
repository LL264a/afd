package plugin

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"sync"

	"github.com/nexus-dl/afd/pkg/logger"
)

// PreDownloadHook, PostDownloadHook, PreUploadHook and PostUploadHook define the
// supported hook event types used by the HookSystem.
const (
	PreDownloadHook  = "pre_download"
	PostDownloadHook = "post_download"
	PreUploadHook    = "pre_upload"
	PostUploadHook   = "post_upload"
)

// HookHandler is the function signature invoked when a hook event is executed.
// It receives the calling context and event-specific data, returning any error.
type HookHandler func(ctx context.Context, data interface{}) error

// Hook represents a registered hook entry with its name, priority and handler.
type Hook struct {
	Name     string
	Priority int
	Handler  HookHandler
}

// HookSystem manages the registration and execution of hooks grouped by event.
type HookSystem struct {
	hooks map[string][]Hook
	mu    sync.RWMutex
}

// NewHookSystem creates and returns a new HookSystem instance.
func NewHookSystem() *HookSystem {
	return &HookSystem{
		hooks: make(map[string][]Hook),
	}
}

// Register registers a hook handler for the given event with default priority 0.
func (hs *HookSystem) Register(event string, handler HookHandler) {
	hs.RegisterWithPriority(event, handler, 0)
}

// RegisterWithPriority registers a hook handler for the given event with the
// specified priority. Hooks are kept sorted by priority in ascending order.
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

// Unregister removes the first hook handler matching the given event and handler.
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

// Execute runs all hooks registered for the given event in priority order,
// collecting any errors returned by the handlers.
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
		return fmt.Errorf("hook execution failed: %w", errors.Join(errs...))
	}

	return nil
}

// ListHooks returns a copy of the hooks registered for the given event.
func (hs *HookSystem) ListHooks(event string) []Hook {
	hs.mu.RLock()
	defer hs.mu.RUnlock()
	hooks := make([]Hook, len(hs.hooks[event]))
	copy(hooks, hs.hooks[event])
	return hooks
}

// Clear removes all hooks registered for the given event.
func (hs *HookSystem) Clear(event string) {
	hs.mu.Lock()
	defer hs.mu.Unlock()
	delete(hs.hooks, event)
	logger.Log.Debugf("Cleared all hooks for event: %s", event)
}

// ClearAll removes all hooks registered for every event.
func (hs *HookSystem) ClearAll() {
	hs.mu.Lock()
	defer hs.mu.Unlock()
	hs.hooks = make(map[string][]Hook)
	logger.Log.Debugf("Cleared all hooks")
}
