package api

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"github.com/nexus-dl/afd/internal/cluster"
	"github.com/nexus-dl/afd/internal/task"
	"github.com/nexus-dl/afd/pkg/logger"
	"nhooyr.io/websocket"
)

type WSMessageType string

const (
	WSTaskUpdate   WSMessageType = "task_update"
	WSNodeUpdate   WSMessageType = "node_update"
	WSClusterEvent WSMessageType = "cluster_event"
	WSHeartbeat    WSMessageType = "heartbeat"
)

type WSMessage struct {
	Type    WSMessageType   `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

type WSTaskPayload struct {
	Task *task.Task `json:"task,omitempty"`
}

type WSNodePayload struct {
	Nodes []cluster.Node `json:"nodes"`
}

type WSClusterEventPayload struct {
	EventType string       `json:"event_type"`
	Node      cluster.Node `json:"node"`
	Timestamp time.Time    `json:"timestamp"`
}

type WSClient struct {
	conn     *websocket.Conn
	send     chan []byte
	hub      *WebSocketHub
	mu       sync.Mutex
	isClosed bool
}

type WebSocketHub struct {
	clients    map[*WSClient]bool
	broadcast  chan []byte
	register   chan *WSClient
	unregister chan *WSClient
	done       chan struct{}
	closeOnce  sync.Once
	mu         sync.RWMutex
	taskQueue  *task.TaskQueue
	membership *cluster.Membership
	localNode  *cluster.LocalNode
}

func NewWebSocketHub() *WebSocketHub {
	return &WebSocketHub{
		clients:    make(map[*WSClient]bool),
		broadcast:  make(chan []byte, 256),
		register:   make(chan *WSClient),
		unregister: make(chan *WSClient),
		done:       make(chan struct{}),
	}
}

func (h *WebSocketHub) SetTaskQueue(q *task.TaskQueue) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.taskQueue = q
}

func (h *WebSocketHub) SetMembership(m *cluster.Membership) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.membership = m
}

func (h *WebSocketHub) SetLocalNode(n *cluster.LocalNode) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.localNode = n
}

func (h *WebSocketHub) Run() {
	for {
		select {
		case <-h.done:
			h.mu.Lock()
			for client := range h.clients {
				close(client.send)
			}
			h.clients = make(map[*WSClient]bool)
			h.mu.Unlock()
			return

		case client := <-h.register:
			h.mu.Lock()
			h.clients[client] = true
			h.mu.Unlock()
			logger.Log.Debugw("WebSocket client connected")

		case client := <-h.unregister:
			h.mu.Lock()
			if _, ok := h.clients[client]; ok {
				delete(h.clients, client)
				close(client.send)
			}
			h.mu.Unlock()
			logger.Log.Debugw("WebSocket client disconnected")

		case message := <-h.broadcast:
			h.mu.Lock()
			for client := range h.clients {
				select {
				case client.send <- message:
				default:
					close(client.send)
					delete(h.clients, client)
				}
			}
			h.mu.Unlock()
		}
	}
}

func (h *WebSocketHub) ServeWS(w http.ResponseWriter, r *http.Request) {
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		CompressionMode: websocket.CompressionContextTakeover,
	})

	if err != nil {
		logger.Log.Errorw("Failed to accept WebSocket connection",
			"error", err,
		)
		return
	}

	client := &WSClient{
		conn: conn,
		send: make(chan []byte, 256),
		hub:  h,
	}

	h.register <- client

	go client.writePump()
	go client.readPump()
}

func (h *WebSocketHub) Close() {
	h.closeOnce.Do(func() {
		close(h.done)
	})
}

func (h *WebSocketHub) BroadcastTaskUpdate(t *task.Task) {
	var payload WSTaskPayload
	if t != nil {
		taskCopy := t.GetSafe()
		payload.Task = &taskCopy
	}

	data, err := json.Marshal(WSMessage{
		Type:    WSTaskUpdate,
		Payload: mustMarshal(payload),
	})
	if err != nil {
		logger.Log.Errorw("Failed to marshal task update",
			"error", err,
		)
		return
	}

	h.tryBroadcast(data)
}

func (h *WebSocketHub) BroadcastNodeUpdate() {
	h.mu.RLock()
	membership := h.membership
	localNode := h.localNode
	h.mu.RUnlock()

	var nodes []cluster.Node
	if membership != nil {
		nodes = membership.Members()
	}

	if localNode != nil {
		nodes = append(nodes, localNode.Node())
	}

	payload := WSNodePayload{Nodes: nodes}
	data, err := json.Marshal(WSMessage{
		Type:    WSNodeUpdate,
		Payload: mustMarshal(payload),
	})
	if err != nil {
		logger.Log.Errorw("Failed to marshal node update",
			"error", err,
		)
		return
	}

	h.tryBroadcast(data)
}

func (h *WebSocketHub) BroadcastClusterEvent(event cluster.ClusterEvent) {
	payload := WSClusterEventPayload{
		EventType: string(event.Type),
		Node:      event.Node,
		Timestamp: event.Timestamp,
	}

	data, err := json.Marshal(WSMessage{
		Type:    WSClusterEvent,
		Payload: mustMarshal(payload),
	})
	if err != nil {
		logger.Log.Errorw("Failed to marshal cluster event",
			"error", err,
		)
		return
	}

	h.tryBroadcast(data)
}

// tryBroadcast delivers data to the hub's broadcast channel without
// blocking once the hub is closed or its buffer is saturated.  After
// Close, Run() has already returned and no one is consuming; blocking
// on the buffered channel would freeze any caller that publishes a
// progress event during shutdown.
func (h *WebSocketHub) tryBroadcast(data []byte) {
	select {
	case <-h.done:
		// Hub already shut down; drop the message.
		return
	default:
	}

	select {
	case h.broadcast <- data:
	case <-h.done:
		// Hub shut down between the first select and the send.
	default:
		// Buffer is full; drop rather than block.  Progress events
		// are best-effort and a slower consumer can catch up on the
		// next snapshot.
		logger.Log.Warnw("WebSocket broadcast buffer full, dropping message",
			"len", len(data),
		)
	}
}

func (c *WSClient) readPump() {
	defer func() {
		select {
		case c.hub.unregister <- c:
		case <-c.hub.done:
		}
		c.conn.Close(websocket.StatusNormalClosure, "")
	}()

	for {
		ctx, cancel := context.WithTimeout(c.ctx(), 60*time.Second)
		_, message, err := c.conn.Read(ctx)
		cancel()
		if err != nil {
			if websocket.CloseStatus(err) != websocket.StatusNormalClosure {
				logger.Log.Warnw("WebSocket read error",
					"error", err,
				)
			}
			break
		}

		var msg WSMessage
		if err := json.Unmarshal(message, &msg); err != nil {
			logger.Log.Warnw("Invalid WebSocket message",
				"error", err,
			)
			continue
		}

		c.handleMessage(msg)
	}
}

func (c *WSClient) writePump() {
	ticker := time.NewTicker(30 * time.Second)
	defer func() {
		ticker.Stop()
		c.conn.Close(websocket.StatusNormalClosure, "")
	}()

	for {
		select {
		case message, ok := <-c.send:
			if !ok {
				c.conn.Close(websocket.StatusNormalClosure, "")
				return
			}

			ctx, cancel := context.WithTimeout(c.ctx(), 10*time.Second)
			err := c.conn.Write(ctx, websocket.MessageText, message)
			cancel()

			if err != nil {
				logger.Log.Warnw("WebSocket write error",
					"error", err,
				)
				return
			}

		case <-ticker.C:
			ctx, cancel := context.WithTimeout(c.ctx(), 10*time.Second)
			heartbeat, _ := json.Marshal(WSMessage{
				Type: WSHeartbeat,
			})
			err := c.conn.Write(ctx, websocket.MessageText, heartbeat)
			cancel()

			if err != nil {
				return
			}
		}
	}
}

func (c *WSClient) handleMessage(msg WSMessage) {
	switch msg.Type {
	case "ping":
		pong, _ := json.Marshal(WSMessage{
			Type: "pong",
		})
		select {
		case c.send <- pong:
		default:
			// 客户端消费过慢，丢弃 pong
		}
	}
}

func (c *WSClient) ctx() context.Context {
	return context.Background()
}

func mustMarshal(v interface{}) json.RawMessage {
	data, err := json.Marshal(v)
	if err != nil {
		logger.Log.Warnw("failed to marshal websocket message", "error", err)
		return nil
	}
	return data
}
