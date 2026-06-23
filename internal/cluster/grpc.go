package cluster

import (
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/nexus-dl/afd/pkg/logger"
)

const (
	MaxMessageSize    = 16 * 1024 * 1024
	RPCDefaultTimeout = 30 * time.Second
	FrameHeaderSize   = 4
)

type TaskStatus string

const (
	TaskPending   TaskStatus = "pending"
	TaskRunning   TaskStatus = "running"
	TaskPaused    TaskStatus = "paused"
	TaskCompleted TaskStatus = "completed"
	TaskFailed    TaskStatus = "failed"
	TaskCanceled  TaskStatus = "canceled"
)

type TaskInfo struct {
	ID         string     `json:"id"`
	URL        string     `json:"url"`
	OutputPath string     `json:"output_path"`
	Status     TaskStatus `json:"status"`
	Progress   float64    `json:"progress"`
	Size       int64      `json:"size"`
	Downloaded int64      `json:"downloaded"`
	Speed      int64      `json:"speed"`
	CreatedAt  time.Time  `json:"created_at"`
	UpdatedAt  time.Time  `json:"updated_at"`
	NodeID     string     `json:"node_id"`
	Error      string     `json:"error,omitempty"`
}

type SubmitTaskRequest struct {
	URL        string `json:"url"`
	OutputPath string `json:"output_path"`
	NodeID     string `json:"node_id,omitempty"`
}

type SubmitTaskResponse struct {
	TaskID string `json:"task_id"`
}

type CancelTaskRequest struct {
	TaskID string `json:"task_id"`
}

type CancelTaskResponse struct {
	Success bool `json:"success"`
}

type PauseTaskRequest struct {
	TaskID string `json:"task_id"`
}

type PauseTaskResponse struct {
	Success bool `json:"success"`
}

type ResumeTaskRequest struct {
	TaskID string `json:"task_id"`
}

type ResumeTaskResponse struct {
	Success bool `json:"success"`
}

type GetTaskStatusRequest struct {
	TaskID string `json:"task_id"`
}

type GetTaskStatusResponse struct {
	Task TaskInfo `json:"task"`
}

type ListTasksRequest struct {
	NodeID string     `json:"node_id,omitempty"`
	Status TaskStatus `json:"status,omitempty"`
	Limit  int        `json:"limit"`
	Offset int        `json:"offset"`
}

type ListTasksResponse struct {
	Tasks []TaskInfo `json:"tasks"`
	Total int        `json:"total"`
}

type GetNodeInfoResponse struct {
	Node   NodeInfo `json:"node"`
	Tasks  int      `json:"active_tasks"`
	Uptime string   `json:"uptime"`
}

type StreamTaskProgressRequest struct {
	TaskID string `json:"task_id"`
}

type StreamTaskProgressResponse struct {
	TaskID    string  `json:"task_id"`
	Progress  float64 `json:"progress"`
	Speed     int64   `json:"speed"`
	Status    string  `json:"status"`
	Timestamp int64   `json:"timestamp"`
}

type RPCRequest struct {
	Method    string
	RequestID uint64
	Payload   []byte
	Token     string
}

type RPCResponse struct {
	RequestID uint64
	Payload   []byte
	Error     string
}

type RPCServer struct {
	addr      string
	listener  net.Listener
	auth      *ClusterAuth
	handler   RPCHandler
	stopCh    chan struct{}
	stopOnce  sync.Once
	wg        sync.WaitGroup
	requestID uint64
	mu        sync.Mutex
	running   bool
	conns     map[net.Conn]struct{}
}

type RPCHandler interface {
	HandleRPC(method string, payload []byte) ([]byte, error)
}

type NexusServiceHandler interface {
	SubmitTask(req SubmitTaskRequest) (*SubmitTaskResponse, error)
	CancelTask(req CancelTaskRequest) (*CancelTaskResponse, error)
	PauseTask(req PauseTaskRequest) (*PauseTaskResponse, error)
	ResumeTask(req ResumeTaskRequest) (*ResumeTaskResponse, error)
	GetTaskStatus(req GetTaskStatusRequest) (*GetTaskStatusResponse, error)
	ListTasks(req ListTasksRequest) (*ListTasksResponse, error)
	GetNodeInfo() (*GetNodeInfoResponse, error)
	StreamTaskProgress(req StreamTaskProgressRequest, send func(StreamTaskProgressResponse) error) error
}

func NewRPCServer(addr string, auth *ClusterAuth) *RPCServer {
	return &RPCServer{
		addr:   addr,
		auth:   auth,
		stopCh: make(chan struct{}),
	}
}

func (s *RPCServer) SetHandler(handler RPCHandler) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.handler = handler
}

func (s *RPCServer) Start() error {
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return fmt.Errorf("RPC server already running")
	}
	s.mu.Unlock()

	gob.Register(RPCRequest{})
	gob.Register(RPCResponse{})

	var listener net.Listener
	var err error
	if s.auth != nil && s.auth.IsTLSEnabled() {
		listener, err = s.auth.ListenTLS("tcp", s.addr)
	} else {
		listener, err = net.Listen("tcp", s.addr)
	}
	if err != nil {
		return fmt.Errorf("failed to start RPC server: %w", err)
	}

	s.mu.Lock()
	// Re-check under the lock; another goroutine may have raced us.
	if s.running {
		s.mu.Unlock()
		listener.Close()
		return fmt.Errorf("RPC server already running")
	}
	s.listener = listener
	s.running = true
	s.mu.Unlock()

	s.wg.Add(1)
	go s.acceptLoop()

	logger.Log.Infow("RPC server started", "addr", s.addr, "tls", s.auth != nil && s.auth.IsTLSEnabled())

	return nil
}

func (s *RPCServer) acceptLoop() {
	defer s.wg.Done()

	for {
		select {
		case <-s.stopCh:
			return
		default:
		}

		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.stopCh:
				return
			default:
			}
			// Listener permanently dead: stop accepting.
			if isClosedConnError(err) {
				return
			}
			logger.Log.Errorw("RPC accept error", "error", err)
			return
		}

		s.wg.Add(1)
		go s.handleConn(conn)
	}
}

func (s *RPCServer) handleConn(conn net.Conn) {
	defer s.wg.Done()

	// 注册连接，以便 Shutdown 时能主动关闭
	s.mu.Lock()
	if s.conns == nil {
		s.conns = make(map[net.Conn]struct{})
	}
	s.conns[conn] = struct{}{}
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		delete(s.conns, conn)
		s.mu.Unlock()
		conn.Close()
	}()

	// Per-connection read deadline; refreshed on each successful read.
	// Prevents Slowloris-style attacks where a client connects and
	// never finishes sending a frame.
	if dl, ok := s.rpcReadTimeout(); ok {
		conn.SetReadDeadline(time.Now().Add(dl))
	}

	for {
		req, err := s.readMessage(conn)
		if err != nil {
			if err != io.EOF {
				logger.Log.Debugw("RPC read error", "error", err)
			}
			return
		}

		// Refresh the deadline after a complete message arrived.
		if dl, ok := s.rpcReadTimeout(); ok {
			conn.SetReadDeadline(time.Now().Add(dl))
		}

		if s.auth != nil && !s.auth.ValidateToken(req.Token) {
			resp := &RPCResponse{
				RequestID: req.RequestID,
				Error:     "unauthorized: invalid token",
			}
			s.writeMessage(conn, resp)
			continue
		}

		if s.handler == nil {
			resp := &RPCResponse{
				RequestID: req.RequestID,
				Error:     "no handler registered",
			}
			s.writeMessage(conn, resp)
			continue
		}

		payload, err := s.handler.HandleRPC(req.Method, req.Payload)
		resp := &RPCResponse{
			RequestID: req.RequestID,
			Payload:   payload,
		}
		if err != nil {
			resp.Error = err.Error()
		}

		if wErr := s.writeMessage(conn, resp); wErr != nil {
			logger.Log.Debugw("RPC write error", "error", wErr)
			return
		}
	}
}

// rpcReadTimeout returns the configured per-message read deadline.
// If zero, no timeout is enforced.
func (s *RPCServer) rpcReadTimeout() (time.Duration, bool) {
	// 30s default; matches RPCDefaultTimeout to keep the contract
	// consistent with the client side.
	return 30 * time.Second, true
}

// isClosedConnError reports whether err indicates that the listener
// has been closed and the accept loop should exit.
func isClosedConnError(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, net.ErrClosed)
}

func (s *RPCServer) readMessage(conn net.Conn) (*RPCRequest, error) {
	header := make([]byte, FrameHeaderSize)
	if _, err := io.ReadFull(conn, header); err != nil {
		return nil, err
	}

	length := binary.BigEndian.Uint32(header)
	if length == 0 || length > MaxMessageSize {
		return nil, fmt.Errorf("invalid message length: %d", length)
	}

	body := make([]byte, length)
	if _, err := io.ReadFull(conn, body); err != nil {
		return nil, err
	}

	var req RPCRequest
	dec := gob.NewDecoder(&gobReader{buf: body})
	if err := dec.Decode(&req); err != nil {
		return nil, fmt.Errorf("failed to decode request: %w", err)
	}

	return &req, nil
}

func (s *RPCServer) writeMessage(conn net.Conn, resp *RPCResponse) error {
	var buf bytesBuffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(resp); err != nil {
		return err
	}

	data := buf.Bytes()
	header := make([]byte, FrameHeaderSize)
	binary.BigEndian.PutUint32(header, uint32(len(data)))

	if _, err := conn.Write(header); err != nil {
		return err
	}
	if _, err := conn.Write(data); err != nil {
		return err
	}

	return nil
}

func (s *RPCServer) Shutdown() {
	s.stopOnce.Do(func() {
		s.mu.Lock()
		s.running = false
		s.mu.Unlock()
		close(s.stopCh)
		if s.listener != nil {
			s.listener.Close()
		}
		// 关闭所有活跃连接
		s.mu.Lock()
		for conn := range s.conns {
			conn.Close()
		}
		s.conns = nil
		s.mu.Unlock()
	})
	s.wg.Wait()
	logger.Log.Info("RPC server stopped")
}

type RPCClient struct {
	addr    string
	auth    *ClusterAuth
	conn    net.Conn
	mu      sync.Mutex
	reqID   uint64
	timeout time.Duration
}

func NewRPCClient(addr string, auth *ClusterAuth) *RPCClient {
	return &RPCClient{
		addr:    addr,
		auth:    auth,
		timeout: RPCDefaultTimeout,
	}
}

// connectLocked dials the server if no connection exists. The caller
// MUST hold c.mu.
func (c *RPCClient) connectLocked() error {
	if c.conn != nil {
		return nil
	}

	var conn net.Conn
	var err error

	if c.auth != nil && c.auth.IsTLSEnabled() {
		conn, err = c.auth.DialTLS("tcp", c.addr)
	} else {
		conn, err = net.DialTimeout("tcp", c.addr, c.timeout)
	}
	if err != nil {
		return fmt.Errorf("failed to connect to %s: %w", c.addr, err)
	}

	c.conn = conn
	return nil
}

func (c *RPCClient) Call(method string, req interface{}, resp interface{}) error {
	// c.mu serializes both the request/response pairing on c.conn
	// and the reqID counter.  Without holding it across the read,
	// two concurrent callers would race on c.conn.Read and could
	// receive each other's response.
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.connectLocked(); err != nil {
		return err
	}

	var buf bytesBuffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(req); err != nil {
		return fmt.Errorf("failed to encode request: %w", err)
	}

	c.reqID++
	reqID := c.reqID
	rpcReq := &RPCRequest{
		Method:    method,
		RequestID: reqID,
		Payload:   buf.Bytes(),
	}
	if c.auth != nil {
		rpcReq.Token = c.auth.Token()
	}

	var frameBuf bytesBuffer
	enc2 := gob.NewEncoder(&frameBuf)
	if err := enc2.Encode(rpcReq); err != nil {
		return fmt.Errorf("failed to encode RPC request: %w", err)
	}

	data := frameBuf.Bytes()
	header := make([]byte, FrameHeaderSize)
	binary.BigEndian.PutUint32(header, uint32(len(data)))

	// 设置读写超时，防止挂死
	if err := c.conn.SetDeadline(time.Now().Add(c.timeout)); err != nil {
		return err
	}

	if _, err := c.conn.Write(header); err != nil {
		c.conn.Close()
		c.conn = nil
		return fmt.Errorf("failed to write header: %w", err)
	}
	if _, err := c.conn.Write(data); err != nil {
		c.conn.Close()
		c.conn = nil
		return fmt.Errorf("failed to write body: %w", err)
	}

	respHeader := make([]byte, FrameHeaderSize)
	if _, err := io.ReadFull(c.conn, respHeader); err != nil {
		c.conn.Close()
		c.conn = nil
		return fmt.Errorf("failed to read response header: %w", err)
	}

	respLen := binary.BigEndian.Uint32(respHeader)
	if respLen == 0 || respLen > MaxMessageSize {
		c.conn.Close()
		c.conn = nil
		return fmt.Errorf("invalid response length: %d", respLen)
	}

	respBody := make([]byte, respLen)
	if _, err := io.ReadFull(c.conn, respBody); err != nil {
		c.conn.Close()
		c.conn = nil
		return fmt.Errorf("failed to read response body: %w", err)
	}

	var rpcResp RPCResponse
	dec := gob.NewDecoder(&gobReader{buf: respBody})
	if err := dec.Decode(&rpcResp); err != nil {
		c.conn.Close()
		c.conn = nil
		return fmt.Errorf("failed to decode response: %w", err)
	}

	if rpcResp.Error != "" {
		return fmt.Errorf("RPC error: %s", rpcResp.Error)
	}

	if resp != nil && len(rpcResp.Payload) > 0 {
		dec2 := gob.NewDecoder(&gobReader{buf: rpcResp.Payload})
		if err := dec2.Decode(resp); err != nil {
			return fmt.Errorf("failed to decode response payload: %w", err)
		}
	}

	return nil
}

func (c *RPCClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn != nil {
		err := c.conn.Close()
		c.conn = nil
		return err
	}
	return nil
}

type NexusRPCAdapter struct {
	service NexusServiceHandler
}

func NewNexusRPCAdapter(service NexusServiceHandler) *NexusRPCAdapter {
	return &NexusRPCAdapter{service: service}
}

func (a *NexusRPCAdapter) HandleRPC(method string, payload []byte) ([]byte, error) {
	gobRegisterTypes()

	var buf bytesBuffer
	enc := gob.NewEncoder(&buf)

	switch method {
	case "SubmitTask":
		var req SubmitTaskRequest
		if err := gobDecode(payload, &req); err != nil {
			return nil, fmt.Errorf("decode SubmitTaskRequest: %w", err)
		}
		resp, err := a.service.SubmitTask(req)
		if err != nil {
			return nil, err
		}
		if err := enc.Encode(resp); err != nil {
			return nil, err
		}

	case "CancelTask":
		var req CancelTaskRequest
		if err := gobDecode(payload, &req); err != nil {
			return nil, err
		}
		resp, err := a.service.CancelTask(req)
		if err != nil {
			return nil, err
		}
		if err := enc.Encode(resp); err != nil {
			return nil, err
		}

	case "PauseTask":
		var req PauseTaskRequest
		if err := gobDecode(payload, &req); err != nil {
			return nil, err
		}
		resp, err := a.service.PauseTask(req)
		if err != nil {
			return nil, err
		}
		if err := enc.Encode(resp); err != nil {
			return nil, err
		}

	case "ResumeTask":
		var req ResumeTaskRequest
		if err := gobDecode(payload, &req); err != nil {
			return nil, err
		}
		resp, err := a.service.ResumeTask(req)
		if err != nil {
			return nil, err
		}
		if err := enc.Encode(resp); err != nil {
			return nil, err
		}

	case "GetTaskStatus":
		var req GetTaskStatusRequest
		if err := gobDecode(payload, &req); err != nil {
			return nil, err
		}
		resp, err := a.service.GetTaskStatus(req)
		if err != nil {
			return nil, err
		}
		if err := enc.Encode(resp); err != nil {
			return nil, err
		}

	case "ListTasks":
		var req ListTasksRequest
		if err := gobDecode(payload, &req); err != nil {
			return nil, err
		}
		resp, err := a.service.ListTasks(req)
		if err != nil {
			return nil, err
		}
		if err := enc.Encode(resp); err != nil {
			return nil, err
		}

	case "GetNodeInfo":
		resp, err := a.service.GetNodeInfo()
		if err != nil {
			return nil, err
		}
		if err := enc.Encode(resp); err != nil {
			return nil, err
		}

	case "StreamTaskProgress":
		return nil, fmt.Errorf("StreamTaskProgress must be called via streaming client")

	default:
		return nil, fmt.Errorf("unknown method: %s", method)
	}

	return buf.Bytes(), nil
}

func gobRegisterTypes() {
	gob.Register(SubmitTaskRequest{})
	gob.Register(SubmitTaskResponse{})
	gob.Register(CancelTaskRequest{})
	gob.Register(CancelTaskResponse{})
	gob.Register(PauseTaskRequest{})
	gob.Register(PauseTaskResponse{})
	gob.Register(ResumeTaskRequest{})
	gob.Register(ResumeTaskResponse{})
	gob.Register(GetTaskStatusRequest{})
	gob.Register(GetTaskStatusResponse{})
	gob.Register(ListTasksRequest{})
	gob.Register(ListTasksResponse{})
	gob.Register(GetNodeInfoResponse{})
	gob.Register(StreamTaskProgressRequest{})
	gob.Register(StreamTaskProgressResponse{})
	gob.Register(TaskInfo{})
	gob.Register(NodeInfo{})
}

func gobDecode(data []byte, v interface{}) error {
	dec := gob.NewDecoder(&gobReader{buf: data})
	return dec.Decode(v)
}

func init() {
	gobRegisterTypes()
}

type StreamingClient struct {
	*RPCClient
}

func NewStreamingClient(addr string, auth *ClusterAuth) *StreamingClient {
	return &StreamingClient{
		RPCClient: NewRPCClient(addr, auth),
	}
}

func (sc *StreamingClient) StreamTaskProgress(req StreamTaskProgressRequest, callback func(StreamTaskProgressResponse) error) error {
	// Serialize against any other Call/Stream on the same underlying
	// connection: the request write and the read loop must both be
	// guarded, otherwise concurrent callers would interleave on
	// sc.conn.Read.
	sc.mu.Lock()
	defer sc.mu.Unlock()

	if err := sc.connectLocked(); err != nil {
		return err
	}

	sc.reqID++
	reqID := sc.reqID
	rpcReq := &RPCRequest{
		Method:    "StreamTaskProgress",
		RequestID: reqID,
	}
	if sc.auth != nil {
		rpcReq.Token = sc.auth.Token()
	}

	var payloadBuf bytesBuffer
	enc := gob.NewEncoder(&payloadBuf)
	if err := enc.Encode(req); err != nil {
		return err
	}
	rpcReq.Payload = payloadBuf.Bytes()

	var frameBuf bytesBuffer
	enc2 := gob.NewEncoder(&frameBuf)
	if err := enc2.Encode(rpcReq); err != nil {
		return err
	}

	data := frameBuf.Bytes()
	header := make([]byte, FrameHeaderSize)
	binary.BigEndian.PutUint32(header, uint32(len(data)))

	if _, err := sc.conn.Write(header); err != nil {
		sc.conn.Close()
		sc.conn = nil
		return err
	}
	if _, err := sc.conn.Write(data); err != nil {
		sc.conn.Close()
		sc.conn = nil
		return err
	}

	for {
		respHeader := make([]byte, FrameHeaderSize)
		if _, err := io.ReadFull(sc.conn, respHeader); err != nil {
			if err == io.EOF {
				return nil
			}
			sc.conn.Close()
			sc.conn = nil
			return err
		}

		respLen := binary.BigEndian.Uint32(respHeader)
		if respLen == 0 || respLen > MaxMessageSize {
			return fmt.Errorf("invalid response length: %d", respLen)
		}

		respBody := make([]byte, respLen)
		if _, err := io.ReadFull(sc.conn, respBody); err != nil {
			sc.conn.Close()
			sc.conn = nil
			return err
		}

		var rpcResp RPCResponse
		dec := gob.NewDecoder(&gobReader{buf: respBody})
		if err := dec.Decode(&rpcResp); err != nil {
			return err
		}

		if rpcResp.Error == "EOF" {
			return nil
		}
		if rpcResp.Error != "" {
			return fmt.Errorf("stream error: %s", rpcResp.Error)
		}

		var streamResp StreamTaskProgressResponse
		if len(rpcResp.Payload) > 0 {
			dec2 := gob.NewDecoder(&gobReader{buf: rpcResp.Payload})
			if err := dec2.Decode(&streamResp); err != nil {
				return err
			}
		}

		if err := callback(streamResp); err != nil {
			return err
		}
	}
}
