package cluster

import (
	"encoding/gob"
	"fmt"
	"io"
	"math/rand"
	"net"
	"sync"
	"time"

	"github.com/nexus-dl/afd/pkg/logger"
)

const (
	DefaultGossipPort   = 9998
	GossipInterval      = 2 * time.Second
	FailureTimeout      = 8 * time.Second
	CleanupInterval     = 10 * time.Second
	MaxGossipPacketSize = 8192
	GossipFanout        = 3
)

type GossipMessageType byte

const (
	MsgHeartbeat GossipMessageType = iota + 1
	MsgMembershipList
	MsgJoin
	MsgLeave
)

type GossipMessage struct {
	Type      GossipMessageType
	Sender    Node
	Members   []Node
	Timestamp int64
}

type DiscoveryConfig struct {
	BindAddr   string
	BindPort   int
	Seeds      []string
	GossipPort int
}

type Discovery struct {
	config     DiscoveryConfig
	localNode  *LocalNode
	membership *Membership
	peers      map[string]*net.UDPAddr
	conn       *net.UDPConn
	stopCh     chan struct{}
	stopOnce   sync.Once
	wg         sync.WaitGroup
	mu         sync.RWMutex
	running    bool

	onJoinHandlers   []func(Node)
	onLeaveHandlers  []func(Node)
	onUpdateHandlers []func(Node)
}

func NewDiscovery(config DiscoveryConfig, localNode *LocalNode, membership *Membership) *Discovery {
	if config.GossipPort == 0 {
		config.GossipPort = DefaultGossipPort
	}

	d := &Discovery{
		config:     config,
		localNode:  localNode,
		membership: membership,
		peers:      make(map[string]*net.UDPAddr),
		stopCh:     make(chan struct{}),
	}

	for _, seed := range config.Seeds {
		d.peers[seed] = nil
	}

	return d
}

func (d *Discovery) Start() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	gob.Register(GossipMessage{})

	addr := &net.UDPAddr{
		IP:   net.ParseIP(d.config.BindAddr),
		Port: d.config.GossipPort,
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return fmt.Errorf("failed to start UDP gossip listener: %w", err)
	}
	d.conn = conn
	d.running = true

	for _, seed := range d.config.Seeds {
		if seedAddr, err := net.ResolveUDPAddr("udp", seed); err == nil {
			d.peers[seed] = seedAddr
		}
	}

	d.wg.Add(2)
	go d.listenLoop()
	go d.gossipLoop()

	logger.Log.Infow("Gossip discovery started",
		"bind_addr", d.config.BindAddr,
		"port", d.config.GossipPort,
		"seeds", len(d.config.Seeds),
	)

	return nil
}

func (d *Discovery) listenLoop() {
	defer d.wg.Done()

	buf := make([]byte, MaxGossipPacketSize)
	for {
		select {
		case <-d.stopCh:
			return
		default:
		}

		d.conn.SetReadDeadline(time.Now().Add(1 * time.Second))
		n, remoteAddr, err := d.conn.ReadFromUDP(buf)
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				continue
			}
			// After Shutdown sets running=false and closes the
			// connection, ReadFromUDP returns an error.  Check
			// stopCh to decide whether to log or exit.
			select {
			case <-d.stopCh:
				return
			default:
				logger.Log.Errorw("UDP read error", "error", err)
			}
			continue
		}

		dec := gob.NewDecoder(&gobReader{buf: buf[:n]})
		var msg GossipMessage
		if err := dec.Decode(&msg); err != nil {
			logger.Log.Warnw("Failed to decode gossip message", "error", err)
			continue
		}

		d.handleMessage(&msg, remoteAddr)
	}
}

func (d *Discovery) gossipLoop() {
	defer d.wg.Done()

	ticker := time.NewTicker(GossipInterval)
	defer ticker.Stop()

	cleanupTicker := time.NewTicker(CleanupInterval)
	defer cleanupTicker.Stop()

	for {
		select {
		case <-d.stopCh:
			return
		case <-ticker.C:
			d.sendHeartbeat()
		case <-cleanupTicker.C:
			d.cleanupFailedNodes()
		}
	}
}

func (d *Discovery) sendHeartbeat() {
	localNode := d.localNode.NodeInfo()

	msg := GossipMessage{
		Type:      MsgHeartbeat,
		Sender:    localNode.Node,
		Members:   d.membership.GetSnapshot(),
		Timestamp: time.Now().UnixNano(),
	}

	d.broadcast(msg)
}

func (d *Discovery) broadcast(msg GossipMessage) {
	d.mu.RLock()
	conn := d.conn
	peerAddrs := make([]string, 0, len(d.peers))
	for addr, udpAddr := range d.peers {
		if udpAddr != nil {
			peerAddrs = append(peerAddrs, addr)
		}
	}
	d.mu.RUnlock()

	if conn == nil || len(peerAddrs) == 0 {
		return
	}

	rand.Shuffle(len(peerAddrs), func(i, j int) {
		peerAddrs[i], peerAddrs[j] = peerAddrs[j], peerAddrs[i]
	})

	fanout := GossipFanout
	if fanout > len(peerAddrs) {
		fanout = len(peerAddrs)
	}

	var buf bytesBuffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(msg); err != nil {
		return
	}
	data := buf.Bytes()

	for i := 0; i < fanout; i++ {
		udpAddr, err := net.ResolveUDPAddr("udp", peerAddrs[i])
		if err != nil {
			continue
		}
		conn.WriteToUDP(data, udpAddr)
	}
}

func (d *Discovery) handleMessage(msg *GossipMessage, remoteAddr *net.UDPAddr) {
	if msg.Sender.ID == d.localNode.Node().ID {
		return
	}

	d.mu.Lock()
	peerKey := remoteAddr.String()
	d.peers[peerKey] = remoteAddr
	d.mu.Unlock()

	switch msg.Type {
	case MsgHeartbeat:
		d.membership.AddMember(msg.Sender)
		d.membership.MergeMembers(msg.Members)
		d.triggerOnUpdate(msg.Sender)

	case MsgMembershipList:
		d.membership.AddMember(msg.Sender)
		d.membership.MergeMembers(msg.Members)

	case MsgJoin:
		d.membership.AddMember(msg.Sender)
		d.triggerOnJoin(msg.Sender)

		response := GossipMessage{
			Type:      MsgMembershipList,
			Sender:    d.localNode.NodeInfo().Node,
			Members:   d.membership.GetSnapshot(),
			Timestamp: time.Now().UnixNano(),
		}
		d.sendTo(response, remoteAddr)

	case MsgLeave:
		d.membership.RemoveMember(msg.Sender.ID)
		d.triggerOnLeave(msg.Sender)
	}
}

func (d *Discovery) sendTo(msg GossipMessage, addr *net.UDPAddr) {
	var buf bytesBuffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(msg); err != nil {
		return
	}
	d.conn.WriteToUDP(buf.Bytes(), addr)
}

func (d *Discovery) Join(seeds []string) error {
	localNode := d.localNode.NodeInfo()

	for _, seed := range seeds {
		addr, err := net.ResolveUDPAddr("udp", seed)
		if err != nil {
			logger.Log.Warnw("Failed to resolve seed address", "seed", seed, "error", err)
			continue
		}

		d.mu.Lock()
		d.peers[seed] = addr
		d.mu.Unlock()

		msg := GossipMessage{
			Type:      MsgJoin,
			Sender:    localNode.Node,
			Timestamp: time.Now().UnixNano(),
		}
		d.sendTo(msg, addr)

		logger.Log.Infow("Joining cluster via seed", "seed", seed)
	}

	return nil
}

func (d *Discovery) Leave() {
	localNode := d.localNode.NodeInfo()

	msg := GossipMessage{
		Type:      MsgLeave,
		Sender:    localNode.Node,
		Timestamp: time.Now().UnixNano(),
	}

	d.broadcast(msg)
}

func (d *Discovery) Members() []Node {
	return d.membership.Members()
}

func (d *Discovery) LocalNode() Node {
	return d.localNode.Node()
}

func (d *Discovery) OnJoin(handler func(Node)) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.onJoinHandlers = append(d.onJoinHandlers, handler)
}

func (d *Discovery) OnLeave(handler func(Node)) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.onLeaveHandlers = append(d.onLeaveHandlers, handler)
}

func (d *Discovery) OnUpdate(handler func(Node)) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.onUpdateHandlers = append(d.onUpdateHandlers, handler)
}

func (d *Discovery) triggerOnJoin(node Node) {
	d.mu.RLock()
	handlers := make([]func(Node), len(d.onJoinHandlers))
	copy(handlers, d.onJoinHandlers)
	d.mu.RUnlock()
	for _, h := range handlers {
		h(node)
	}
}

func (d *Discovery) triggerOnLeave(node Node) {
	d.mu.RLock()
	handlers := make([]func(Node), len(d.onLeaveHandlers))
	copy(handlers, d.onLeaveHandlers)
	d.mu.RUnlock()
	for _, h := range handlers {
		h(node)
	}
}

func (d *Discovery) triggerOnUpdate(node Node) {
	d.mu.RLock()
	handlers := make([]func(Node), len(d.onUpdateHandlers))
	copy(handlers, d.onUpdateHandlers)
	d.mu.RUnlock()
	for _, h := range handlers {
		h(node)
	}
}

func (d *Discovery) cleanupFailedNodes() {
	expired := d.membership.CheckTimeouts(FailureTimeout)
	for _, id := range expired {
		logger.Log.Infow("Node failed health check, removing", "node_id", id)
	}
}

func (d *Discovery) Shutdown() {
	d.stopOnce.Do(func() {
		d.mu.Lock()
		d.running = false
		d.mu.Unlock()

		d.Leave()

		close(d.stopCh)
		d.wg.Wait()

		if d.conn != nil {
			d.conn.Close()
		}

		logger.Log.Info("Gossip discovery stopped")
	})
}

type gobReader struct {
	buf []byte
	pos int
}

func (r *gobReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.buf) {
		return 0, io.EOF
	}
	n := copy(p, r.buf[r.pos:])
	r.pos += n
	return n, nil
}

type bytesBuffer struct {
	buf []byte
}

func (b *bytesBuffer) Write(p []byte) (int, error) {
	b.buf = append(b.buf, p...)
	return len(p), nil
}

func (b *bytesBuffer) Bytes() []byte {
	return b.buf
}
