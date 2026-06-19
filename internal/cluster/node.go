package cluster

import (
	"fmt"
	"net"
	"sync"
	"time"
)

type NodeStatus string

const (
	StatusOnline  NodeStatus = "online"
	StatusOffline NodeStatus = "offline"
)

type Node struct {
	ID       string            `json:"id"`
	Name     string            `json:"name"`
	Addr     string            `json:"addr"`
	GRPCPort int               `json:"grpc_port"`
	APIPort  int               `json:"api_port"`
	Status   NodeStatus        `json:"status"`
	Load     int               `json:"load"`
	Tags     map[string]string `json:"tags"`
	LastSeen time.Time         `json:"last_seen"`
	mu       sync.RWMutex      `json:"-"`
}

type NodeInfo struct {
	Node      Node      `json:"node"`
	StartedAt time.Time `json:"started_at"`
	Version   string    `json:"version"`
}

func (n *Node) IsOnline() bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.Status == StatusOnline
}

func (n *Node) FullAddr() string {
	return fmt.Sprintf("%s:%d", n.Addr, n.GRPCPort)
}

func (n *Node) UpdateSeen() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.LastSeen = time.Now()
	n.Status = StatusOnline
}

func (n *Node) MarkOffline() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.Status = StatusOffline
}

func (n *Node) GetLoad() int {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.Load
}

func (n *Node) SetLoad(load int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.Load = load
}

func (n *Node) GetStatus() NodeStatus {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.Status
}

type LocalNode struct {
	mu   sync.RWMutex
	node Node
	info NodeInfo

	resolvedAddr string
	localIP      string
}

func NewLocalNode(id, name string, grpcPort, apiPort int, tags map[string]string) *LocalNode {
	ln := &LocalNode{
		node: Node{
			ID:       id,
			Name:     name,
			GRPCPort: grpcPort,
			APIPort:  apiPort,
			Status:   StatusOnline,
			Load:     0,
			Tags:     tags,
			LastSeen: time.Now(),
		},
		info: NodeInfo{
			StartedAt: time.Now(),
			Version:   "0.1.0",
		},
	}
	ln.resolveAddr()
	ln.info.Node = ln.node
	return ln
}

func (ln *LocalNode) resolveAddr() {
	ln.resolvedAddr = "0.0.0.0"

	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return
	}
	for _, addr := range addrs {
		ipNet, ok := addr.(*net.IPNet)
		if !ok {
			continue
		}
		ip := ipNet.IP
		if ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() {
			continue
		}
		if ip4 := ip.To4(); ip4 != nil {
			ln.resolvedAddr = ip4.String()
			ln.localIP = ip4.String()
			ln.node.Addr = ip4.String()
			return
		}
	}
}

func (ln *LocalNode) Node() Node {
	ln.mu.RLock()
	defer ln.mu.RUnlock()
	return ln.node
}

func (ln *LocalNode) NodeInfo() NodeInfo {
	ln.mu.RLock()
	defer ln.mu.RUnlock()
	ni := ln.info
	ni.Node = ln.node
	return ni
}

func (ln *LocalNode) SetLoad(load int) {
	ln.mu.Lock()
	defer ln.mu.Unlock()
	if load < 0 {
		load = 0
	}
	if load > 100 {
		load = 100
	}
	ln.node.Load = load
}

func (ln *LocalNode) SetTag(key, value string) {
	ln.mu.Lock()
	defer ln.mu.Unlock()
	if ln.node.Tags == nil {
		ln.node.Tags = make(map[string]string)
	}
	ln.node.Tags[key] = value
}

func (ln *LocalNode) SetAddr(addr string) {
	ln.mu.Lock()
	defer ln.mu.Unlock()
	ln.node.Addr = addr
}

func (ln *LocalNode) UpdateLastSeen() {
	ln.mu.Lock()
	defer ln.mu.Unlock()
	ln.node.LastSeen = time.Now()
}

func (ln *LocalNode) BindAddr() string {
	return ln.resolvedAddr
}

func (ln *LocalNode) LocalIP() string {
	return ln.localIP
}
