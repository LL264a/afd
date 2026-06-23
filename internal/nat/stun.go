package nat

import (
	"encoding/binary"
	"net"
	"time"

	"github.com/nexus-dl/afd/pkg/logger"
)

const (
	STUNBindingRequest   = 0x0001
	STUNBindingResponse  = 0x0101
	STUNBindingError     = 0x0111
	STUNMappedAddress    = 0x0001
	STUNResponseAddress  = 0x0002
	STUNChangeRequest    = 0x0003
	STUNSourceAddress    = 0x0004
	STUNChangedAddress   = 0x0005
	STUNXorMappedAddress = 0x0020
	stunMagicCookie      = 0x2112A442
)

var DefaultSTUNServers = []string{
	"stun.l.google.com:19302",
	"stun1.l.google.com:19302",
	"stun2.l.google.com:19302",
}

type NATType int

const (
	NATTypeUnknown NATType = iota
	NATTypeOpen
	NATTypeFullCone
	NATTypeRestrictedCone
	NATTypePortRestrictedCone
	NATTypeSymmetric
)

func (n NATType) String() string {
	switch n {
	case NATTypeOpen:
		return "Open"
	case NATTypeFullCone:
		return "Full Cone"
	case NATTypeRestrictedCone:
		return "Restricted Cone"
	case NATTypePortRestrictedCone:
		return "Port Restricted Cone"
	case NATTypeSymmetric:
		return "Symmetric"
	default:
		return "Unknown"
	}
}

type STUNResult struct {
	PublicIP   string
	PublicPort uint16
	NATType    NATType
}

type STUNClient struct {
	servers   []string
	conn      *net.UDPConn
	localAddr string
}

func NewSTUNClient(servers []string) *STUNClient {
	if len(servers) == 0 {
		servers = DefaultSTUNServers
	}
	return &STUNClient{
		servers: servers,
	}
}

func (c *STUNClient) Discover() (*STUNResult, error) {
	addr, err := net.ResolveUDPAddr("udp", ":0")
	if err != nil {
		return nil, err
	}

	c.conn, err = net.ListenUDP("udp", addr)
	if err != nil {
		return nil, err
	}
	defer c.conn.Close()

	c.localAddr = c.conn.LocalAddr().String()

	result := &STUNResult{}

	for _, server := range c.servers {
		stunAddr, err := net.ResolveUDPAddr("udp", server)
		if err != nil {
			logger.Log.Warnf("Failed to resolve STUN server %s: %v", server, err)
			continue
		}

		publicIP, publicPort, err := c.queryBinding(stunAddr)
		if err != nil {
			logger.Log.Warnf("STUN query failed for %s: %v", server, err)
			continue
		}

		result.PublicIP = publicIP
		result.PublicPort = publicPort
		break
	}

	if result.PublicIP == "" {
		return nil, ErrSTUNFailed
	}

	nATType, err := c.detectNATType(result.PublicIP, result.PublicPort)
	if err != nil {
		logger.Log.Warnf("NAT type detection failed: %v", err)
		result.NATType = NATTypeUnknown
	} else {
		result.NATType = nATType
	}

	logger.Log.Infof("STUN discovery result: %s:%d, NAT Type: %s",
		result.PublicIP, result.PublicPort, result.NATType)

	return result, nil
}

func (c *STUNClient) queryBinding(stunAddr *net.UDPAddr) (string, uint16, error) {
	req := c.createBindingRequest()

	_, err := c.conn.WriteToUDP(req, stunAddr)
	if err != nil {
		return "", 0, err
	}

	c.conn.SetReadDeadline(time.Now().Add(3 * time.Second))

	buf := make([]byte, 1024)
	n, _, err := c.conn.ReadFromUDP(buf)
	if err != nil {
		return "", 0, err
	}

	return c.parseBindingResponse(buf[:n])
}

func (c *STUNClient) createBindingRequest() []byte {
	req := make([]byte, 20)
	binary.BigEndian.PutUint16(req[0:2], 0x0001)
	binary.BigEndian.PutUint16(req[2:4], 0x0000)
	binary.BigEndian.PutUint16(req[4:6], 0x0000)
	binary.BigEndian.PutUint16(req[6:8], 0x0000)

	transactionID := []byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b}
	copy(req[8:20], transactionID)

	return req
}

func (c *STUNClient) parseBindingResponse(data []byte) (string, uint16, error) {
	if len(data) < 20 {
		return "", 0, ErrInvalidResponse
	}

	msgType := binary.BigEndian.Uint16(data[0:2])
	if msgType != STUNBindingResponse {
		return "", 0, ErrInvalidResponse
	}

	ip := ""
	port := uint16(0)

	i := 20
	for i < len(data)-4 {
		attrType := binary.BigEndian.Uint16(data[i : i+2])
		attrLen := binary.BigEndian.Uint16(data[i+2 : i+4])
		i += 4

		if int(i+int(attrLen)) > len(data) {
			break
		}

		if attrType == STUNMappedAddress || attrType == STUNXorMappedAddress {
			if attrLen >= 8 {
				// STUN MAPPED-ADDRESS layout (RFC 5389 §15.2):
				//   byte 0:   reserved (0x00)
				//   byte 1:   family (0x01 = IPv4, 0x02 = IPv6)
				//   bytes 2-3: port
				//   bytes 4+:  IP address (4 bytes for IPv4)
				family := data[i+1]
				port = binary.BigEndian.Uint16(data[i+2 : i+4])
				if attrType == STUNXorMappedAddress {
					port ^= 0x2112
				}

				if family == 0x01 && int(i+4+4) <= len(data) {
					// IPv4
					if attrType == STUNXorMappedAddress {
						xorIP := make([]byte, 4)
						for j := 0; j < 4; j++ {
							xorIP[j] = data[i+4+j] ^ transactionIDXOR[j]
						}
						ip = net.IP(xorIP).String()
					} else {
						ip = net.IP(data[i+4 : i+4+4]).String()
					}
				}
			}
			break
		}

		i += int(attrLen)
	}

	if ip == "" || port == 0 {
		return "", 0, ErrNoMappedAddress
	}

	return ip, port, nil
}

var transactionIDXOR = []byte{0x21, 0x12, 0xa4, 0x42, 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07}

func (c *STUNClient) detectNATType(publicIP string, publicPort uint16) (NATType, error) {
	localIP := c.conn.LocalAddr().(*net.UDPAddr).IP.String()

	if localIP == publicIP && c.localAddr == "" {
		return NATTypeOpen, nil
	}

	changedAddr, err := c.testChangeRequest()
	if err != nil {
		return NATTypeSymmetric, err
	}

	if changedAddr == nil || changedAddr.String() == "" {
		return NATTypeSymmetric, nil
	}

	portRestricted, err := c.testPortRestricted(publicIP, publicPort)
	if err != nil {
		return NATTypeRestrictedCone, err
	}

	if !portRestricted {
		return NATTypePortRestrictedCone, nil
	}

	fullCone, err := c.testFullCone(publicIP, publicPort)
	if err != nil {
		return NATTypeRestrictedCone, err
	}

	if fullCone {
		return NATTypeFullCone, nil
	}

	return NATTypeRestrictedCone, nil
}

func (c *STUNClient) testChangeRequest() (net.IP, error) {
	req := make([]byte, 28) // 足够大以容纳 CHANGE-REQUEST attribute
	copy(req, c.createBindingRequest())

	attrOffset := 20
	binary.BigEndian.PutUint16(req[attrOffset:attrOffset+2], STUNChangeRequest)
	binary.BigEndian.PutUint16(req[attrOffset+2:attrOffset+4], 0x0004)
	binary.BigEndian.PutUint32(req[attrOffset+4:attrOffset+8], 0x00000002)

	stunAddr, err := net.ResolveUDPAddr("udp", "stun.l.google.com:19302")
	if err != nil {
		return nil, err
	}

	_, err = c.conn.WriteToUDP(req, stunAddr)
	if err != nil {
		return nil, err
	}

	c.conn.SetReadDeadline(time.Now().Add(2 * time.Second))

	buf := make([]byte, 1024)
	n, _, err := c.conn.ReadFromUDP(buf)
	if err != nil {
		return nil, err
	}

	return c.parseChangedAddress(buf[:n])
}

func (c *STUNClient) parseChangedAddress(data []byte) (net.IP, error) {
	i := 20
	for i < len(data)-4 {
		attrType := binary.BigEndian.Uint16(data[i : i+2])
		attrLen := binary.BigEndian.Uint16(data[i+2 : i+4])
		i += 4

		if int(i+int(attrLen)) > len(data) {
			break
		}

		if attrType == STUNChangedAddress && attrLen >= 8 {
			family := data[i+1]
			if family == 0x01 && int(i+4+4) <= len(data) {
				return net.IP(data[i+4 : i+4+4]), nil
			}
		}

		i += int(attrLen)
	}

	return nil, ErrNoChangedAddress
}

func (c *STUNClient) testPortRestricted(publicIP string, publicPort uint16) (bool, error) {
	return false, nil
}

func (c *STUNClient) testFullCone(publicIP string, publicPort uint16) (bool, error) {
	return true, nil
}

var (
	ErrSTUNFailed       = &STUNError{"STUN discovery failed"}
	ErrInvalidResponse  = &STUNError{"Invalid STUN response"}
	ErrNoMappedAddress  = &STUNError{"No mapped address in response"}
	ErrNoChangedAddress = &STUNError{"No changed address in response"}
)

type STUNError struct {
	msg string
}

func (e *STUNError) Error() string {
	return e.msg
}
