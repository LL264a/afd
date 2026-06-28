package internal

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os/exec"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/nexus-dl/afd/pkg/logger"
)

type EventType string

const (
	EventTaskStarted          EventType = "task_started"
	EventTaskPaused           EventType = "task_paused"
	EventTaskResumed          EventType = "task_resumed"
	EventTaskCompleted        EventType = "task_completed"
	EventTaskFailed           EventType = "task_failed"
	EventTaskProgress         EventType = "task_progress"
	EventDownloadSpeedChanged EventType = "download_speed_changed"
)

type Event struct {
	Type      EventType      `json:"type"`
	TaskID    string         `json:"task_id"`
	Data      map[string]any `json:"data"`
	Timestamp time.Time      `json:"timestamp"`
}

type EventHandler interface {
	HandleEvent(event *Event) error
	Close() error
}

type HTTPHandler struct {
	URL     string
	Client  *http.Client
	Headers map[string]string
	mu      sync.Mutex
}

func NewHTTPHandler(url string, headers map[string]string) *HTTPHandler {
	return &HTTPHandler{
		URL:     url,
		Client:  &http.Client{Timeout: 10 * time.Second},
		Headers: headers,
	}
}

func (h *HTTPHandler) HandleEvent(event *Event) error {
	// 持锁拷贝 headers 和 URL，避免持锁执行网络请求阻塞所有事件
	h.mu.Lock()
	headers := make(map[string]string, len(h.Headers))
	for k, v := range h.Headers {
		headers[k] = v
	}
	url := h.URL
	h.mu.Unlock()

	// 锁外执行网络请求
	body, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	req, err := http.NewRequest(http.MethodPost, url, bytes.NewBuffer(body))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	resp, err := h.Client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	return nil
}

func (h *HTTPHandler) Close() error {
	return nil
}

type WebSocketHandler struct {
	URL       string
	Conn      *websocket.Conn
	SendQueue chan []byte
	done      chan struct{}
	mu        sync.Mutex
}

func NewWebSocketHandler(url string) (*WebSocketHandler, error) {
	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to dial websocket: %w", err)
	}

	handler := &WebSocketHandler{
		URL:       url,
		Conn:      conn,
		SendQueue: make(chan []byte, 100),
		done:      make(chan struct{}),
	}

	go handler.writePump()

	return handler, nil
}

func (h *WebSocketHandler) writePump() {
	for {
		select {
		case message, ok := <-h.SendQueue:
			if !ok {
				h.Conn.Close()
				return
			}

			h.mu.Lock()
			err := h.Conn.WriteMessage(websocket.TextMessage, message)
			h.mu.Unlock()

			if err != nil {
				return
			}
		case <-h.done:
			return
		}
	}
}

func (h *WebSocketHandler) HandleEvent(event *Event) error {
	body, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	select {
	case h.SendQueue <- body:
		return nil
	default:
		return fmt.Errorf("send queue is full")
	}
}

func (h *WebSocketHandler) Close() error {
	close(h.done)
	return h.Conn.Close()
}

type CommandHandler struct {
	Command string
	Args    []string
}

func NewCommandHandler(command string, args []string) *CommandHandler {
	return &CommandHandler{
		Command: command,
		Args:    args,
	}
}

func (h *CommandHandler) HandleEvent(event *Event) error {
	eventJSON, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, h.Command, h.Args...)
	cmd.Stdin = bytes.NewReader(eventJSON)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("command failed: %w, output: %s", err, string(output))
	}

	return nil
}

func (h *CommandHandler) Close() error {
	return nil
}

type EventEmitter struct {
	handlers    []EventHandler
	async       bool
	eventQueue  chan *Event
	workerCount int
	done        chan struct{}
	closeOnce   sync.Once
	wg          sync.WaitGroup
	mu          sync.RWMutex
}

func NewEventEmitter(async bool, workerCount int) *EventEmitter {
	emitter := &EventEmitter{
		handlers:    make([]EventHandler, 0),
		async:       async,
		eventQueue:  make(chan *Event, 1000),
		workerCount: workerCount,
		done:        make(chan struct{}),
	}

	if async {
		emitter.startWorkers()
	}

	return emitter
}

func (e *EventEmitter) startWorkers() {
	for i := 0; i < e.workerCount; i++ {
		e.wg.Add(1)
		go func() {
			defer e.wg.Done()
			for {
				select {
				case <-e.done:
					return
				case event := <-e.eventQueue:
					e.processEvent(event)
				}
			}
		}()
	}
}

func (e *EventEmitter) processEvent(event *Event) {
	e.mu.RLock()
	handlers := make([]EventHandler, len(e.handlers))
	copy(handlers, e.handlers)
	e.mu.RUnlock()

	for _, handler := range handlers {
		if err := handler.HandleEvent(event); err != nil {
			logger.Log.Errorw("event handler error",
				"error", err,
				"event_type", event.Type,
				"task_id", event.TaskID,
			)
		}
	}
}

func (e *EventEmitter) Subscribe(handler EventHandler) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.handlers = append(e.handlers, handler)
}

func (e *EventEmitter) Unsubscribe(handler EventHandler) {
	e.mu.Lock()
	defer e.mu.Unlock()

	for i, h := range e.handlers {
		if h == handler {
			e.handlers = append(e.handlers[:i], e.handlers[i+1:]...)
			break
		}
	}
}

func (e *EventEmitter) Emit(event *Event) {
	if e.async {
		select {
		case <-e.done:
			return // 已关闭，直接返回
		case e.eventQueue <- event:
		default:
			logger.Log.Warnw("event queue full, dropping event",
				"event_type", event.Type,
				"task_id", event.TaskID,
			)
		}
	} else {
		// 同步模式
		select {
		case <-e.done:
			return
		default:
		}
		e.processEvent(event)
	}
}

func (e *EventEmitter) EmitTaskStarted(taskID string, data map[string]any) {
	e.Emit(&Event{
		Type:      EventTaskStarted,
		TaskID:    taskID,
		Data:      data,
		Timestamp: time.Now(),
	})
}

func (e *EventEmitter) EmitTaskPaused(taskID string, data map[string]any) {
	e.Emit(&Event{
		Type:      EventTaskPaused,
		TaskID:    taskID,
		Data:      data,
		Timestamp: time.Now(),
	})
}

func (e *EventEmitter) EmitTaskResumed(taskID string, data map[string]any) {
	e.Emit(&Event{
		Type:      EventTaskResumed,
		TaskID:    taskID,
		Data:      data,
		Timestamp: time.Now(),
	})
}

func (e *EventEmitter) EmitTaskCompleted(taskID string, data map[string]any) {
	e.Emit(&Event{
		Type:      EventTaskCompleted,
		TaskID:    taskID,
		Data:      data,
		Timestamp: time.Now(),
	})
}

func (e *EventEmitter) EmitTaskFailed(taskID string, data map[string]any) {
	e.Emit(&Event{
		Type:      EventTaskFailed,
		TaskID:    taskID,
		Data:      data,
		Timestamp: time.Now(),
	})
}

func (e *EventEmitter) EmitTaskProgress(taskID string, data map[string]any) {
	e.Emit(&Event{
		Type:      EventTaskProgress,
		TaskID:    taskID,
		Data:      data,
		Timestamp: time.Now(),
	})
}

func (e *EventEmitter) EmitDownloadSpeedChanged(taskID string, data map[string]any) {
	e.Emit(&Event{
		Type:      EventDownloadSpeedChanged,
		TaskID:    taskID,
		Data:      data,
		Timestamp: time.Now(),
	})
}

func (e *EventEmitter) Close() error {
	e.closeOnce.Do(func() {
		close(e.done)
	})
	e.wg.Wait()

	e.mu.Lock()
	defer e.mu.Unlock()

	for _, handler := range e.handlers {
		handler.Close()
	}

	return nil
}
