package nat

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/nexus-dl/afd/pkg/config"
	"github.com/nexus-dl/afd/pkg/logger"
)

// defaultReadTimeout 是 NAT 连接读取/接受操作的超时时间，
// 用于在 select 循环中周期性地检查停止信号。
const defaultReadTimeout = 1 * time.Second

type ConnectionType int

const (
	ConnectionDirect ConnectionType = iota
	ConnectionHolePunch
	ConnectionRelay
)

type NATManager struct {
	config          config.NATConfig
	signalingServer string
	stunServer      string
	relayServer     string
	listener        *net.TCPListener
	connections     map[string]*NATConnection
	holePuncher     *HolePuncher
	mu              sync.RWMutex
	ctx             context.Context
	cancel          context.CancelFunc
}

type NATConnection struct {
	PeerID     string
	Conn       net.Conn
	ConnType   ConnectionType
	LocalAddr  net.Addr
	RemoteAddr net.Addr
	CreatedAt  time.Time
}

func NewNATManager(cfg config.NATConfig) *NATManager {
	ctx, cancel := context.WithCancel(context.Background())
	nm := &NATManager{
		config:          cfg,
		signalingServer: cfg.SignalingServer,
		stunServer:      cfg.STUNServer,
		relayServer:     cfg.RelayServer,
		connections:     make(map[string]*NATConnection),
		ctx:             ctx,
		cancel:          cancel,
	}
	nm.holePuncher = NewHolePuncher()
	return nm
}

func (nm *NATManager) Connect(peerID, peerAddr string) (*NATConnection, error) {
	logger.Log.Infof("NAT: connecting to peer %s at %s", peerID, peerAddr)

	// 1. 尝试直连
	conn, err := nm.tryDirectConnect(peerID, peerAddr)
	if err == nil {
		nm.mu.Lock()
		nm.connections[peerID] = conn
		nm.mu.Unlock()
		logger.Log.Infof("NAT: connected to peer %s via %s", peerID, conn.ConnType)
		return conn, nil
	}
	logger.Log.Warnf("NAT: direct connect failed for peer %s: %v", peerID, err)

	// 2. 尝试打洞
	if nm.holePuncher != nil {
		conn, err = nm.tryHolePunch(peerID, peerAddr)
		if err == nil {
			nm.mu.Lock()
			nm.connections[peerID] = conn
			nm.mu.Unlock()
			logger.Log.Infof("NAT: connected to peer %s via %s", peerID, conn.ConnType)
			return conn, nil
		}
		logger.Log.Warnf("NAT: hole punch failed for peer %s: %v", peerID, err)
	}

	// 3. 回退到中继
	conn, err = nm.useRelay(peerID, peerAddr)
	if err != nil {
		return nil, fmt.Errorf("NAT: direct, hole punch and relay all failed: %w", err)
	}

	nm.mu.Lock()
	nm.connections[peerID] = conn
	nm.mu.Unlock()
	logger.Log.Infof("NAT: connected to peer %s via %s", peerID, conn.ConnType)
	return conn, nil
}

func (nm *NATManager) tryDirectConnect(peerID, peerAddr string) (*NATConnection, error) {
	conn, err := net.Dial("tcp", peerAddr)
	if err != nil {
		return nil, fmt.Errorf("direct dial failed: %w", err)
	}

	localAddr := conn.LocalAddr()
	remoteAddr := conn.RemoteAddr()

	return &NATConnection{
		PeerID:     peerID,
		Conn:       conn,
		ConnType:   ConnectionDirect,
		LocalAddr:  localAddr,
		RemoteAddr: remoteAddr,
		CreatedAt:  time.Now(),
	}, nil
}

func (nm *NATManager) useRelay(peerID, peerAddr string) (*NATConnection, error) {
	if nm.relayServer == "" {
		return nil, fmt.Errorf("no relay server configured")
	}

	conn, err := net.Dial("tcp", nm.relayServer)
	if err != nil {
		return nil, fmt.Errorf("relay connection failed: %w", err)
	}

	localAddr := conn.LocalAddr()
	remoteAddr := conn.RemoteAddr()

	return &NATConnection{
		PeerID:     peerID,
		Conn:       conn,
		ConnType:   ConnectionRelay,
		LocalAddr:  localAddr,
		RemoteAddr: remoteAddr,
		CreatedAt:  time.Now(),
	}, nil
}

func (nm *NATManager) tryHolePunch(peerID, peerAddr string) (*NATConnection, error) {
	// 每次打洞使用新建的 HolePuncher，避免 stopCh/conn 复用导致的生命周期问题。
	hp := NewHolePuncher()
	if err := hp.Start(); err != nil {
		return nil, fmt.Errorf("hole puncher start failed: %w", err)
	}

	host, portStr, err := net.SplitHostPort(peerAddr)
	if err != nil {
		hp.Stop()
		return nil, fmt.Errorf("invalid peer address: %w", err)
	}
	remotePort, err := strconv.ParseUint(portStr, 10, 16)
	if err != nil {
		hp.Stop()
		return nil, fmt.Errorf("invalid peer port: %w", err)
	}

	localHost, localPortStr, _ := net.SplitHostPort(hp.LocalAddr())
	localPort, _ := strconv.ParseUint(localPortStr, 10, 16)

	if err := hp.Punch(peerAddr, localHost, uint16(localPort), host, uint16(remotePort)); err != nil {
		hp.Stop()
		return nil, fmt.Errorf("hole punch failed: %w", err)
	}

	nm.holePuncher = hp
	conn := hp.GetConn()

	var remoteAddr net.Addr
	if ra := hp.RemoteAddr(); ra != "" {
		remoteAddr, _ = net.ResolveUDPAddr("udp", ra)
	}
	if remoteAddr == nil {
		remoteAddr = conn.RemoteAddr()
	}

	return &NATConnection{
		PeerID:     peerID,
		Conn:       conn,
		ConnType:   ConnectionHolePunch,
		LocalAddr:  conn.LocalAddr(),
		RemoteAddr: remoteAddr,
		CreatedAt:  time.Now(),
	}, nil
}

func (nm *NATManager) Listen() (net.Listener, error) {
	if !nm.config.Enabled {
		return nil, fmt.Errorf("NAT traversal is disabled")
	}

	addr, err := net.ResolveTCPAddr("tcp", "0.0.0.0:0")
	if err != nil {
		return nil, fmt.Errorf("failed to resolve address: %w", err)
	}

	ln, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("failed to listen: %w", err)
	}

	nm.listener = ln
	logger.Log.Infof("NAT: listening on %s", ln.Addr().String())

	go nm.acceptConnections()
	return ln, nil
}

func (nm *NATManager) acceptConnections() {
	for {
		select {
		case <-nm.ctx.Done():
			return
		default:
		}

		nm.listener.SetDeadline(time.Now().Add(defaultReadTimeout))
		conn, err := nm.listener.Accept()
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				continue
			}
			if nm.ctx.Err() != nil {
				return
			}
			logger.Log.Errorf("NAT: accept error: %v", err)
			continue
		}

		go nm.handleConnection(conn)
	}
}

func (nm *NATManager) handleConnection(conn net.Conn) {
	peerID := conn.RemoteAddr().String()
	nconn := &NATConnection{
		PeerID:     peerID,
		Conn:       conn,
		ConnType:   ConnectionDirect,
		LocalAddr:  conn.LocalAddr(),
		RemoteAddr: conn.RemoteAddr(),
		CreatedAt:  time.Now(),
	}

	nm.mu.Lock()
	nm.connections[peerID] = nconn
	nm.mu.Unlock()

	logger.Log.Infof("NAT: accepted connection from %s", peerID)
}

func (nm *NATManager) GetConnection(peerID string) (*NATConnection, bool) {
	nm.mu.RLock()
	defer nm.mu.RUnlock()
	conn, ok := nm.connections[peerID]
	return conn, ok
}

func (nm *NATManager) RemoveConnection(peerID string) {
	nm.mu.Lock()
	defer nm.mu.Unlock()
	if conn, ok := nm.connections[peerID]; ok {
		conn.Conn.Close()
		delete(nm.connections, peerID)
		logger.Log.Infof("NAT: removed connection for peer %s", peerID)
	}
}

func (nm *NATManager) Close() error {
	nm.cancel()

	nm.mu.Lock()
	defer nm.mu.Unlock()

	for _, conn := range nm.connections {
		conn.Conn.Close()
	}
	nm.connections = make(map[string]*NATConnection)

	if nm.listener != nil {
		return nm.listener.Close()
	}
	return nil
}

func (nm *NATManager) GetPublicAddr() (net.Addr, error) {
	servers := nm.config.STUNServers
	if len(servers) == 0 {
		servers = DefaultSTUNServers
	}
	client := NewSTUNClient(servers)
	result, err := client.Discover()
	if err != nil {
		return nil, err
	}
	return &net.UDPAddr{
		IP:   net.ParseIP(result.PublicIP),
		Port: int(result.PublicPort),
	}, nil
}

func (nm *NATManager) DetectNATType() (NATType, net.Addr, error) {
	servers := nm.config.STUNServers
	if len(servers) == 0 {
		servers = DefaultSTUNServers
	}
	client := NewSTUNClient(servers)
	result, err := client.Discover()
	if err != nil {
		return NATTypeUnknown, nil, err
	}
	addr := &net.UDPAddr{
		IP:   net.ParseIP(result.PublicIP),
		Port: int(result.PublicPort),
	}
	return result.NATType, addr, nil
}

func (nm *NATManager) IsEnabled() bool {
	return nm.config.Enabled
}

func (c *NATConnection) String() string {
	return fmt.Sprintf("NATConnection{peer=%s, type=%d, local=%s, remote=%s}",
		c.PeerID, c.ConnType, c.LocalAddr, c.RemoteAddr)
}
