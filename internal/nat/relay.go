package nat

import (
	"encoding/binary"
	"net"
	"sync"
	"time"

	"github.com/nexus-dl/afd/pkg/logger"
)

const (
	RelayConnect    = 0x0001
	RelayConnectAck = 0x0002
	RelayData       = 0x0003
	RelayClose      = 0x0004
)

type RelayServer struct {
	addr     string
	conn     *net.UDPConn
	clients  map[string]*RelayClient
	mu       sync.RWMutex
	stopCh   chan struct{}
	stopOnce sync.Once
}

type RelayClient struct {
	id         string
	localAddr  string
	conn       *net.UDPConn
	serverAddr string
	stopCh     chan struct{}
	stopOnce   sync.Once
	mu         sync.RWMutex
	connected  bool
	relayAddr  string
}

type RelayMessage struct {
	Type     uint16
	ClientID string
	Length   uint16
	Data     []byte
}

type Relay struct {
	localConn  *net.UDPConn
	localAddr  string
	remoteAddr string
	stopCh     chan struct{}
	stopOnce   sync.Once
	mu         sync.RWMutex
	active     bool
}

func NewRelayServer(addr string) *RelayServer {
	return &RelayServer{
		addr:    addr,
		clients: make(map[string]*RelayClient),
		stopCh:  make(chan struct{}),
	}
}

func (r *RelayServer) Start() error {
	udpAddr, err := net.ResolveUDPAddr("udp", r.addr)
	if err != nil {
		logger.Log.Errorf("Failed to resolve UDP address: %v", err)
		return err
	}

	r.conn, err = net.ListenUDP("udp", udpAddr)
	if err != nil {
		logger.Log.Errorf("Failed to listen on UDP: %v", err)
		return err
	}

	logger.Log.Infof("Relay server started on %s", r.addr)

	go r.handleMessages()
	return nil
}

func (r *RelayServer) handleMessages() {
	buf := make([]byte, 4096)
	for {
		select {
		case <-r.stopCh:
			return
		default:
		}

		r.conn.SetReadDeadline(time.Now().Add(defaultReadTimeout))
		n, clientAddr, err := r.conn.ReadFromUDP(buf)
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				continue
			}
			logger.Log.Errorf("Error reading from UDP: %v", err)
			continue
		}

		var msg RelayMessage
		if err := decodeRelayMessage(buf[:n], &msg); err != nil {
			logger.Log.Errorf("Failed to decode relay message: %v", err)
			continue
		}

		r.handleMessage(&msg, clientAddr)
	}
}

func (r *RelayServer) handleMessage(msg *RelayMessage, addr *net.UDPAddr) {
	switch msg.Type {
	case RelayConnect:
		r.handleConnect(msg.ClientID, addr)

	case RelayData:
		r.handleRelayData(msg.ClientID, msg.Data)

	case RelayClose:
		r.handleClose(msg.ClientID)
	}
}

func (r *RelayServer) handleConnect(clientID string, addr *net.UDPAddr) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.clients[clientID] = &RelayClient{
		id:        clientID,
		localAddr: addr.String(),
		connected: true,
	}

	response := RelayMessage{
		Type:     RelayConnectAck,
		ClientID: clientID,
	}

	data := encodeRelayMessage(response)
	r.conn.WriteToUDP(data, addr)

	logger.Log.Infof("Client connected to relay: %s from %s", clientID, addr)
}

func (r *RelayServer) handleRelayData(clientID string, data []byte) {
	r.mu.RLock()
	client, ok := r.clients[clientID]
	r.mu.RUnlock()

	// Presence in the map is the sole "connected" signal: handleConnect
	// adds the entry, handleClose deletes it. Reading client.connected
	// outside the lock would race with handleClose's write.
	if !ok {
		logger.Log.Warnf("Client not found for relay data: %s", clientID)
		return
	}

	addr, err := net.ResolveUDPAddr("udp", client.localAddr)
	if err != nil {
		logger.Log.Errorf("Failed to resolve client address: %v", err)
		return
	}

	r.conn.WriteToUDP(data, addr)
}

func (r *RelayServer) handleClose(clientID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if client, ok := r.clients[clientID]; ok {
		client.connected = false
		delete(r.clients, clientID)
		logger.Log.Infof("Client disconnected from relay: %s", clientID)
	}
}

func (r *RelayServer) Stop() {
	r.stopOnce.Do(func() {
		close(r.stopCh)
	})
	if r.conn != nil {
		r.conn.Close()
	}
}

func NewRelayClient(serverAddr, clientID string) *RelayClient {
	return &RelayClient{
		serverAddr: serverAddr,
		id:         clientID,
		stopCh:     make(chan struct{}),
	}
}

func (c *RelayClient) Start() error {
	addr, err := net.ResolveUDPAddr("udp", ":0")
	if err != nil {
		return err
	}

	c.conn, err = net.ListenUDP("udp", addr)
	if err != nil {
		return err
	}

	c.localAddr = c.conn.LocalAddr().String()
	logger.Log.Infof("Relay client started on %s", c.localAddr)

	if err := c.connect(); err != nil {
		return err
	}

	go c.handleMessages()
	return nil
}

func (c *RelayClient) connect() error {
	msg := RelayMessage{
		Type:     RelayConnect,
		ClientID: c.id,
	}

	data := encodeRelayMessage(msg)

	serverAddr, err := net.ResolveUDPAddr("udp", c.serverAddr)
	if err != nil {
		return err
	}

	_, err = c.conn.WriteToUDP(data, serverAddr)
	if err != nil {
		return err
	}

	c.conn.SetReadDeadline(time.Now().Add(5 * time.Second))

	buf := make([]byte, 1024)
	n, _, err := c.conn.ReadFromUDP(buf)
	if err != nil {
		return err
	}

	var response RelayMessage
	if err := decodeRelayMessage(buf[:n], &response); err != nil {
		return err
	}

	if response.Type == RelayConnectAck {
		c.mu.Lock()
		c.connected = true
		c.relayAddr = c.serverAddr
		c.mu.Unlock()
		logger.Log.Infof("Connected to relay server")
	}

	return nil
}

func (c *RelayClient) handleMessages() {
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

		var msg RelayMessage
		if err := decodeRelayMessage(buf[:n], &msg); err != nil {
			logger.Log.Errorf("Failed to decode relay message: %v", err)
			continue
		}

		if msg.Type == RelayData {
			logger.Log.Debugf("Received relay data: %d bytes", len(msg.Data))
		}
	}
}

func (c *RelayClient) Send(data []byte) error {
	c.mu.RLock()
	connected := c.connected
	c.mu.RUnlock()
	if !connected {
		return ErrNotConnected
	}

	msg := RelayMessage{
		Type:     RelayData,
		ClientID: c.id,
		Length:   uint16(len(data)),
		Data:     data,
	}

	dataBytes := encodeRelayMessage(msg)

	serverAddr, err := net.ResolveUDPAddr("udp", c.serverAddr)
	if err != nil {
		return err
	}

	_, err = c.conn.WriteToUDP(dataBytes, serverAddr)
	return err
}

func (c *RelayClient) IsConnected() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.connected
}

func (c *RelayClient) Stop() {
	c.stopOnce.Do(func() {
		c.mu.RLock()
		connected := c.connected
		c.mu.RUnlock()
		if connected {
			msg := RelayMessage{
				Type:     RelayClose,
				ClientID: c.id,
			}
			data := encodeRelayMessage(msg)
			serverAddr, _ := net.ResolveUDPAddr("udp", c.serverAddr)
			c.conn.WriteToUDP(data, serverAddr)
		}

		close(c.stopCh)
		if c.conn != nil {
			c.conn.Close()
		}
	})
}

func NewRelay() *Relay {
	return &Relay{
		stopCh: make(chan struct{}),
	}
}

func (r *Relay) Start(localAddr, remoteAddr string) error {
	addr, err := net.ResolveUDPAddr("udp", ":0")
	if err != nil {
		return err
	}

	r.localConn, err = net.ListenUDP("udp", addr)
	if err != nil {
		return err
	}

	r.localAddr = r.localConn.LocalAddr().String()
	r.remoteAddr = remoteAddr
	r.active = true

	logger.Log.Infof("Relay started: local=%s, remote=%s", r.localAddr, remoteAddr)

	go r.relayLoop()
	return nil
}

func (r *Relay) relayLoop() {
	buf := make([]byte, 4096)

	for {
		select {
		case <-r.stopCh:
			return
		default:
		}

		r.localConn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
		n, _, err := r.localConn.ReadFromUDP(buf)
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				continue
			}
			logger.Log.Errorf("Error reading from UDP: %v", err)
			continue
		}

		if n > 0 {
			r.forwardToRemote(buf[:n])
		}
	}
}

func (r *Relay) forwardToRemote(data []byte) {
	remoteUDPAddr, err := net.ResolveUDPAddr("udp", r.remoteAddr)
	if err != nil {
		logger.Log.Errorf("Failed to resolve remote address: %v", err)
		return
	}

	r.localConn.WriteToUDP(data, remoteUDPAddr)
}

func (r *Relay) Stop() {
	r.stopOnce.Do(func() {
		r.mu.Lock()
		r.active = false
		r.mu.Unlock()
		close(r.stopCh)
		if r.localConn != nil {
			r.localConn.Close()
		}
	})
}

func (r *Relay) LocalAddr() string {
	return r.localAddr
}

func (r *Relay) IsActive() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.active
}

func encodeRelayMessage(msg RelayMessage) []byte {
	headerLen := 6 + len(msg.ClientID)
	dataLen := len(msg.Data)
	totalLen := headerLen + dataLen

	data := make([]byte, totalLen)
	binary.BigEndian.PutUint16(data[0:2], msg.Type)

	clientIDLen := len(msg.ClientID)
	binary.BigEndian.PutUint16(data[2:4], uint16(clientIDLen))
	copy(data[4:4+clientIDLen], []byte(msg.ClientID))

	offset := 4 + clientIDLen
	binary.BigEndian.PutUint16(data[offset:offset+2], msg.Length)

	if dataLen > 0 {
		copy(data[offset+2:], msg.Data)
	}

	return data
}

func decodeRelayMessage(data []byte, msg *RelayMessage) error {
	if len(data) < 6 {
		return ErrInvalidRelayMessage
	}

	msg.Type = binary.BigEndian.Uint16(data[0:2])
	clientIDLen := int(binary.BigEndian.Uint16(data[2:4]))

	if len(data) < 4+clientIDLen {
		return ErrInvalidRelayMessage
	}

	msg.ClientID = string(data[4 : 4+clientIDLen])

	offset := 4 + clientIDLen
	if len(data) >= offset+2 {
		msg.Length = binary.BigEndian.Uint16(data[offset : offset+2])
	}

	if msg.Length > 0 && len(data) > offset+2 {
		msg.Data = data[offset+2 : offset+2+int(msg.Length)]
	}

	return nil
}

var (
	ErrNotConnected        = &RelayError{"Not connected to relay server"}
	ErrInvalidRelayMessage = &RelayError{"Invalid relay message"}
)

type RelayError struct {
	msg string
}

func (e *RelayError) Error() string {
	return e.msg
}
