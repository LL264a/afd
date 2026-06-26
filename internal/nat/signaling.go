package nat

import (
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/nexus-dl/afd/pkg/logger"
)

type SignalingMessage struct {
	Type       string `json:"type"`
	PeerID     string `json:"peer_id"`
	LocalAddr  string `json:"local_addr"`
	RemoteAddr string `json:"remote_addr"`
	NatType    string `json:"nat_type"`
	PublicIP   string `json:"public_ip"`
	PublicPort uint16 `json:"public_port"`
	Data       string `json:"data,omitempty"` // 承载 offer/answer 等 SDP 数据
}

type SignalingServer struct {
	addr     string
	peers    map[string]*PeerInfo
	mu       sync.RWMutex
	conn     *net.UDPConn
	stopCh   chan struct{}
	stopOnce sync.Once
}

type PeerInfo struct {
	ID         string
	LocalAddr  string
	RemoteAddr string
	NatType    string
	LastSeen   time.Time
}

type SignalingClient struct {
	serverAddr  string
	peerID      string
	conn        *net.UDPConn
	localAddr   string
	stopCh      chan struct{}
	queryRespCh chan *SignalingMessage
}

func NewSignalingServer(addr string) *SignalingServer {
	return &SignalingServer{
		addr:   addr,
		peers:  make(map[string]*PeerInfo),
		stopCh: make(chan struct{}),
	}
}

func (s *SignalingServer) Start() error {
	addr, err := net.ResolveUDPAddr("udp", s.addr)
	if err != nil {
		logger.Log.Errorf("Failed to resolve UDP address: %v", err)
		return err
	}

	s.conn, err = net.ListenUDP("udp", addr)
	if err != nil {
		logger.Log.Errorf("Failed to listen on UDP: %v", err)
		return err
	}

	logger.Log.Infof("Signaling server started on %s", s.addr)

	go s.handleMessages()
	return nil
}

func (s *SignalingServer) handleMessages() {
	buf := make([]byte, 4096)
	for {
		select {
		case <-s.stopCh:
			return
		default:
		}

		s.conn.SetReadDeadline(time.Now().Add(defaultReadTimeout))
		n, clientAddr, err := s.conn.ReadFromUDP(buf)
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				continue
			}
			logger.Log.Errorf("Error reading from UDP: %v", err)
			continue
		}

		var msg SignalingMessage
		if err := json.Unmarshal(buf[:n], &msg); err != nil {
			logger.Log.Errorf("Failed to unmarshal message: %v", err)
			continue
		}

		s.handleMessage(&msg, clientAddr)
	}
}

func (s *SignalingServer) handleMessage(msg *SignalingMessage, clientAddr *net.UDPAddr) {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch msg.Type {
	case "register":
		s.peers[msg.PeerID] = &PeerInfo{
			ID:         msg.PeerID,
			LocalAddr:  msg.LocalAddr,
			RemoteAddr: clientAddr.String(),
			NatType:    msg.NatType,
			LastSeen:   time.Now(),
		}
		logger.Log.Infof("Peer registered: %s from %s", msg.PeerID, clientAddr)

	case "query":
		if peer, ok := s.peers[msg.PeerID]; ok {
			response := SignalingMessage{
				Type:       "response",
				PeerID:     peer.ID,
				LocalAddr:  peer.LocalAddr,
				RemoteAddr: peer.RemoteAddr,
				NatType:    peer.NatType,
			}
			s.sendResponse(response, clientAddr)
		}

	case "offer", "answer", "ice":
		if peer, ok := s.peers[msg.PeerID]; ok {
			peerAddr, err := net.ResolveUDPAddr("udp", peer.RemoteAddr)
			if err == nil {
				s.forwardMessage(msg, peerAddr)
			}
		}
	}
}

func (s *SignalingServer) sendResponse(msg SignalingMessage, addr *net.UDPAddr) {
	data, err := json.Marshal(msg)
	if err != nil {
		logger.Log.Errorf("Failed to marshal response: %v", err)
		return
	}
	s.conn.WriteToUDP(data, addr)
}

func (s *SignalingServer) forwardMessage(msg *SignalingMessage, addr *net.UDPAddr) {
	data, err := json.Marshal(msg)
	if err != nil {
		logger.Log.Errorf("Failed to marshal message: %v", err)
		return
	}
	s.conn.WriteToUDP(data, addr)
}

func (s *SignalingServer) Stop() {
	s.stopOnce.Do(func() {
		close(s.stopCh)
		if s.conn != nil {
			s.conn.Close()
		}
	})
}

func NewSignalingClient(serverAddr, peerID string) *SignalingClient {
	return &SignalingClient{
		serverAddr:  serverAddr,
		peerID:      peerID,
		stopCh:      make(chan struct{}),
		queryRespCh: make(chan *SignalingMessage, 1),
	}
}

func (c *SignalingClient) Start() error {
	addr, err := net.ResolveUDPAddr("udp", ":0")
	if err != nil {
		return err
	}

	c.conn, err = net.ListenUDP("udp", addr)
	if err != nil {
		return err
	}

	c.localAddr = c.conn.LocalAddr().String()
	logger.Log.Infof("Signaling client started on %s", c.localAddr)

	go c.handleMessages()
	return nil
}

func (c *SignalingClient) handleMessages() {
	buf := make([]byte, 4096)
	for {
		select {
		case <-c.stopCh:
			return
		default:
		}

		c.conn.SetReadDeadline(time.Now().Add(defaultReadTimeout))
		n, _, err := c.conn.ReadFromUDP(buf)
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				continue
			}
			logger.Log.Errorf("Error reading from UDP: %v", err)
			continue
		}

		var msg SignalingMessage
		if err := json.Unmarshal(buf[:n], &msg); err != nil {
			logger.Log.Errorf("Failed to unmarshal message: %v", err)
			continue
		}

		logger.Log.Debugf("Received message type: %s", msg.Type)

		if msg.Type == "response" {
			select {
			case c.queryRespCh <- &msg:
			default:
			}
		}
	}
}

func (c *SignalingClient) Register(natType string) error {
	msg := SignalingMessage{
		Type:      "register",
		PeerID:    c.peerID,
		LocalAddr: c.localAddr,
		NatType:   natType,
	}
	return c.sendMessage(msg)
}

func (c *SignalingClient) QueryPeer(peerID string) (*SignalingMessage, error) {
	// 清空可能残留的旧响应，避免读到上一次查询的结果
	select {
	case <-c.queryRespCh:
	default:
	}

	msg := SignalingMessage{
		Type:   "query",
		PeerID: peerID,
	}
	if err := c.sendMessage(msg); err != nil {
		return nil, err
	}

	// 等待 handleMessages 路由过来的 "response" 消息（带超时）
	select {
	case resp := <-c.queryRespCh:
		return resp, nil
	case <-time.After(5 * time.Second):
		return nil, fmt.Errorf("query response timeout")
	case <-c.stopCh:
		return nil, fmt.Errorf("signaling client stopped")
	}
}

func (c *SignalingClient) SendOffer(peerID string, offer string) error {
	msg := SignalingMessage{
		Type:   "offer",
		PeerID: peerID,
		Data:   offer,
	}
	return c.sendMessage(msg)
}

func (c *SignalingClient) SendAnswer(peerID string, answer string) error {
	msg := SignalingMessage{
		Type:   "answer",
		PeerID: peerID,
		Data:   answer,
	}
	return c.sendMessage(msg)
}

func (c *SignalingClient) sendMessage(msg SignalingMessage) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	addr, err := net.ResolveUDPAddr("udp", c.serverAddr)
	if err != nil {
		return err
	}

	_, err = c.conn.WriteToUDP(data, addr)
	return err
}

func (c *SignalingClient) Stop() {
	close(c.stopCh)
	if c.conn != nil {
		c.conn.Close()
	}
}

func (c *SignalingClient) LocalAddr() string {
	return c.localAddr
}
