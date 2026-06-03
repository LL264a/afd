package nat

import (
	"encoding/binary"
	"net"
	"sync"
	"time"

	"github.com/nexus-dl/afd/pkg/logger"
)

const (
	HolePunchRequest  = 0x0001
	HolePunchResponse = 0x0002
	HolePunchAck      = 0x0003
	HolePunchSync     = 0x0004
)

type HolePuncher struct {
	localAddr  string
	conn       *net.UDPConn
	peerConn   *net.UDPConn
	mu         sync.RWMutex
	stopCh     chan struct{}
	stopOnce   sync.Once
	connected  bool
	remoteAddr string
}

type HolePunchMessage struct {
	Type       uint16
	Seq        uint32
	LocalIP    string
	LocalPort  uint16
	RemoteIP   string
	RemotePort uint16
}

func NewHolePuncher() *HolePuncher {
	return &HolePuncher{
		stopCh: make(chan struct{}),
	}
}

func (h *HolePuncher) Start() error {
	addr, err := net.ResolveUDPAddr("udp", ":0")
	if err != nil {
		return err
	}

	h.conn, err = net.ListenUDP("udp", addr)
	if err != nil {
		return err
	}

	h.localAddr = h.conn.LocalAddr().String()
	logger.Log.Infof("Hole puncher started on %s", h.localAddr)

	go h.handleMessages()
	return nil
}

func (h *HolePuncher) handleMessages() {
	buf := make([]byte, 4096)
	for {
		select {
		case <-h.stopCh:
			return
		default:
		}

		h.conn.SetReadDeadline(time.Now().Add(1 * time.Second))
		n, addr, err := h.conn.ReadFromUDP(buf)
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				continue
			}
			logger.Log.Errorf("Error reading from UDP: %v", err)
			continue
		}

		var msg HolePunchMessage
		if err := decodeHolePunchMessage(buf[:n], &msg); err != nil {
			logger.Log.Errorf("Failed to decode hole punch message: %v", err)
			continue
		}

		h.handleMessage(&msg, addr)
	}
}

func (h *HolePuncher) handleMessage(msg *HolePunchMessage, addr *net.UDPAddr) {
	switch msg.Type {
	case HolePunchRequest:
		logger.Log.Debugf("Received hole punch request from %s", addr)
		h.sendResponse(addr)

	case HolePunchResponse:
		logger.Log.Debugf("Received hole punch response from %s", addr)
		h.establishConnection(addr)

	case HolePunchAck:
		logger.Log.Debugf("Received hole punch ack from %s", addr)
		h.mu.Lock()
		h.connected = true
		h.remoteAddr = addr.String()
		h.mu.Unlock()
	}
}

func (h *HolePuncher) sendResponse(addr *net.UDPAddr) {
	msg := HolePunchMessage{
		Type:      HolePunchResponse,
		Seq:       0,
		LocalIP:   h.conn.LocalAddr().(*net.UDPAddr).IP.String(),
		LocalPort: uint16(h.conn.LocalAddr().(*net.UDPAddr).Port),
	}

	data := encodeHolePunchMessage(msg)
	h.conn.WriteToUDP(data, addr)

	h.sendAck(addr)
}

func (h *HolePuncher) sendAck(addr *net.UDPAddr) {
	msg := HolePunchMessage{
		Type: HolePunchAck,
		Seq:  0,
	}

	data := encodeHolePunchMessage(msg)
	h.conn.WriteToUDP(data, addr)
}

func (h *HolePuncher) establishConnection(addr *net.UDPAddr) {
	h.mu.Lock()
	h.connected = true
	h.remoteAddr = addr.String()
	h.mu.Unlock()

	logger.Log.Infof("Hole punch successful, connected to %s", addr)
}

func (h *HolePuncher) Punch(peerAddr string, localPublicIP string, localPublicPort uint16, remotePublicIP string, remotePublicPort uint16) error {
	addr, err := net.ResolveUDPAddr("udp", peerAddr)
	if err != nil {
		return err
	}

	msg := HolePunchMessage{
		Type:       HolePunchRequest,
		Seq:        0,
		LocalIP:    localPublicIP,
		LocalPort:  localPublicPort,
		RemoteIP:   remotePublicIP,
		RemotePort: remotePublicPort,
	}

	data := encodeHolePunchMessage(msg)

	for i := 0; i < 10; i++ {
		_, err := h.conn.WriteToUDP(data, addr)
		if err != nil {
			logger.Log.Warnf("Failed to send hole punch request: %v", err)
			continue
		}

		logger.Log.Debugf("Sent hole punch request %d to %s", i+1, peerAddr)

		time.Sleep(500 * time.Millisecond)

		h.mu.RLock()
		connected := h.connected
		h.mu.RUnlock()

		if connected {
			logger.Log.Infof("Hole punch successful after %d attempts", i+1)
			return nil
		}
	}

	return ErrHolePunchTimeout
}

func (h *HolePuncher) PunchWithSync(peerAddr string, localPublicIP string, localPublicPort uint16) error {
	addr, err := net.ResolveUDPAddr("udp", peerAddr)
	if err != nil {
		return err
	}

	_ = h.conn.LocalAddr().(*net.UDPAddr)

	msg := HolePunchMessage{
		Type:      HolePunchSync,
		Seq:       0,
		LocalIP:   localPublicIP,
		LocalPort: localPublicPort,
	}

	data := encodeHolePunchMessage(msg)

	for i := 0; i < 5; i++ {
		h.conn.WriteToUDP(data, addr)
		time.Sleep(200 * time.Millisecond)
	}

	go h.listenForPeer()

	time.Sleep(2 * time.Second)

	h.mu.RLock()
	connected := h.connected
	h.mu.RUnlock()

	if !connected {
		return ErrHolePunchTimeout
	}

	return nil
}

func (h *HolePuncher) listenForPeer() {
	buf := make([]byte, 4096)
	for {
		select {
		case <-h.stopCh:
			return
		default:
		}

		h.conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		n, addr, err := h.conn.ReadFromUDP(buf)
		if err != nil {
			continue
		}

		if n > 0 {
			h.mu.Lock()
			if !h.connected {
				h.connected = true
				h.remoteAddr = addr.String()
				logger.Log.Infof("Received peer connection from %s", addr)
			}
			h.mu.Unlock()
		}
	}
}

func (h *HolePuncher) IsConnected() bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.connected
}

func (h *HolePuncher) RemoteAddr() string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.remoteAddr
}

func (h *HolePuncher) LocalAddr() string {
	return h.localAddr
}

func (h *HolePuncher) GetConn() *net.UDPConn {
	return h.conn
}

func (h *HolePuncher) Stop() {
	close(h.stopCh)
	if h.conn != nil {
		h.conn.Close()
	}
}

func encodeHolePunchMessage(msg HolePunchMessage) []byte {
	data := make([]byte, 24)
	binary.BigEndian.PutUint16(data[0:2], msg.Type)
	binary.BigEndian.PutUint32(data[4:8], msg.Seq)

	ip := net.ParseIP(msg.LocalIP).To4()
	if ip != nil {
		copy(data[8:12], ip)
	}
	binary.BigEndian.PutUint16(data[12:14], msg.LocalPort)

	remoteIP := net.ParseIP(msg.RemoteIP).To4()
	if remoteIP != nil {
		copy(data[14:18], remoteIP)
	}
	binary.BigEndian.PutUint16(data[18:20], msg.RemotePort)

	return data
}

func decodeHolePunchMessage(data []byte, msg *HolePunchMessage) error {
	if len(data) < 20 {
		return ErrInvalidHolePunchMessage
	}

	msg.Type = binary.BigEndian.Uint16(data[0:2])
	msg.Seq = binary.BigEndian.Uint32(data[4:8])

	msg.LocalIP = net.IP(data[8:12]).String()
	msg.LocalPort = binary.BigEndian.Uint16(data[12:14])

	msg.RemoteIP = net.IP(data[14:18]).String()
	msg.RemotePort = binary.BigEndian.Uint16(data[18:20])

	return nil
}

var (
	ErrHolePunchTimeout        = &HolePunchError{"Hole punch timeout"}
	ErrInvalidHolePunchMessage = &HolePunchError{"Invalid hole punch message"}
)

type HolePunchError struct {
	msg string
}

func (e *HolePunchError) Error() string {
	return e.msg
}
