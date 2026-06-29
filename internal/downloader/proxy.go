package downloader

import (
	"context"
	"crypto/md5"
	"encoding/base64"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/nexus-dl/afd/pkg/config"
	"golang.org/x/net/proxy"
)

type ProxyAuthTransport struct {
	Transport   http.RoundTripper
	Username    string
	Password    string
	UseDigest   bool
	ExcludeList []string
	digestNonce string
	digestRealm string
	digestQOP   string
}

func (t *ProxyAuthTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	// 检查是否在排除列表中
	if t.shouldExclude(req.URL) {
		return t.Transport.RoundTrip(req)
	}

	if t.Username != "" && t.Password != "" {
		if t.UseDigest && t.digestNonce != "" {
			// 使用 Digest 认证
			auth := t.createDigestAuth(req.Method, req.URL.RequestURI())
			req.Header.Set("Proxy-Authorization", auth)
		} else {
			// 使用 Basic 认证
			auth := base64.StdEncoding.EncodeToString([]byte(t.Username + ":" + t.Password))
			req.Header.Set("Proxy-Authorization", "Basic "+auth)
		}
	}

	resp, err := t.Transport.RoundTrip(req)
	if err != nil {
		return resp, err
	}

	// 如果收到 407 且需要 Digest 认证，获取 nonce 并重试
	if resp.StatusCode == http.StatusProxyAuthRequired && t.UseDigest {
		authHeader := resp.Header.Get("Proxy-Authenticate")
		if authHeader != "" {
			t.parseDigestChallenge(authHeader)
			resp.Body.Close()

			// 重新发起请求
			if t.digestNonce != "" {
				newReq := cloneRequest(req)
				auth := t.createDigestAuth(newReq.Method, newReq.URL.RequestURI())
				newReq.Header.Set("Proxy-Authorization", auth)
				return t.Transport.RoundTrip(newReq)
			}
		}
	}

	return resp, err
}

func (t *ProxyAuthTransport) shouldExclude(u *url.URL) bool {
	if u == nil || len(t.ExcludeList) == 0 {
		return false
	}
	host := u.Hostname()
	for _, excluded := range t.ExcludeList {
		if strings.HasSuffix(host, excluded) || host == excluded {
			return true
		}
	}
	return false
}

func (t *ProxyAuthTransport) parseDigestChallenge(challenge string) {
	// 解析类似: Digest realm="example.com", nonce="abc123", qop="auth"
	if !strings.HasPrefix(challenge, "Digest ") {
		return
	}
	params := parseChallengeParams(challenge[7:])
	t.digestRealm = params["realm"]
	t.digestNonce = params["nonce"]
	t.digestQOP = params["qop"]
}

func parseChallengeParams(challenge string) map[string]string {
	params := make(map[string]string)
	for _, param := range strings.Split(challenge, ",") {
		kv := strings.SplitN(strings.TrimSpace(param), "=", 2)
		if len(kv) == 2 {
			key := strings.TrimSpace(kv[0])
			value := strings.Trim(strings.TrimSpace(kv[1]), "\"")
			params[key] = value
		}
	}
	return params
}

func (t *ProxyAuthTransport) createDigestAuth(method, uri string) string {
	ha1 := fmt.Sprintf("%x", md5.Sum([]byte(t.Username+":"+t.digestRealm+":"+t.Password)))
	ha2 := fmt.Sprintf("%x", md5.Sum([]byte(method+":"+uri)))
	nc := "00000001"
	cnonce := generateCNonce()
	response := fmt.Sprintf("%x", md5.Sum([]byte(ha1+":"+t.digestNonce+":"+nc+":"+cnonce+":"+t.digestQOP+":"+ha2)))

	return fmt.Sprintf(`Digest username="%s", realm="%s", nonce="%s", uri="%s", qop=%s, nc=%s, cnonce="%s", response="%s"`,
		t.Username, t.digestRealm, t.digestNonce, uri, t.digestQOP, nc, cnonce, response)
}

func generateCNonce() string {
	return fmt.Sprintf("%x", md5.Sum([]byte(time.Now().String())))[:8]
}

func cloneRequest(req *http.Request) *http.Request {
	newReq := &http.Request{
		Method:        req.Method,
		URL:           req.URL,
		Header:        make(http.Header),
		Body:          req.Body,
		GetBody:       req.GetBody,
		ContentLength: req.ContentLength,
		Host:          req.Host,
		Proto:         req.Proto,
		ProtoMajor:    req.ProtoMajor,
		ProtoMinor:    req.ProtoMinor,
	}
	for k, v := range req.Header {
		newReq.Header[k] = v
	}
	return newReq
}

func NewProxyAuthTransport(transport http.RoundTripper, username, password string, useDigest bool, excludeList []string) *ProxyAuthTransport {
	if transport == nil {
		transport = http.DefaultTransport
	}
	return &ProxyAuthTransport{
		Transport:   transport,
		Username:    username,
		Password:    password,
		UseDigest:   useDigest,
		ExcludeList: excludeList,
	}
}

func NewHTTPProxyClient(proxyCfg *config.ProxyConfig, timeout time.Duration, useDigest bool, excludeList []string) (*http.Client, error) {
	if !proxyCfg.IsValid() {
		return nil, fmt.Errorf("invalid proxy config")
	}

	proxyURL := &url.URL{
		Scheme: proxyCfg.Type,
		Host:   fmt.Sprintf("%s:%d", proxyCfg.Host, proxyCfg.Port),
	}

	if proxyCfg.Username != "" && proxyCfg.Password != "" {
		proxyURL.User = url.UserPassword(proxyCfg.Username, proxyCfg.Password)
	}

	var transport http.RoundTripper = &http.Transport{
		Proxy: http.ProxyURL(proxyURL),
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   10,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		ResponseHeaderTimeout: 60 * time.Second,
	}

	if proxyCfg.Username != "" && proxyCfg.Password != "" {
		transport = NewProxyAuthTransport(transport, proxyCfg.Username, proxyCfg.Password, useDigest, excludeList)
	}

	return &http.Client{
		Transport: transport,
	}, nil
}

func CreateSOCKS5ProxyClient(proxyCfg *config.ProxyConfig, timeout time.Duration) (*http.Client, error) {
	if !proxyCfg.IsValid() {
		return nil, fmt.Errorf("invalid proxy config")
	}

	socksAddr := fmt.Sprintf("%s:%d", proxyCfg.Host, proxyCfg.Port)

	var auth *proxy.Auth
	if proxyCfg.Username != "" && proxyCfg.Password != "" {
		auth = &proxy.Auth{
			User:     proxyCfg.Username,
			Password: proxyCfg.Password,
		}
	}

	dialer, err := proxy.SOCKS5("tcp", socksAddr, auth, proxy.Direct)
	if err != nil {
		return nil, fmt.Errorf("create socks5 proxy dialer: %w", err)
	}

	transport := &http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			return dialer.Dial(network, addr)
		},
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   10,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		ResponseHeaderTimeout: 60 * time.Second,
	}

	return &http.Client{
		Transport: transport,
	}, nil
}

func CreateSOCKS4ProxyClient(proxyCfg *config.ProxyConfig, timeout time.Duration, is4a bool) (*http.Client, error) {
	if !proxyCfg.IsValid() {
		return nil, fmt.Errorf("invalid proxy config")
	}

	socksAddr := fmt.Sprintf("%s:%d", proxyCfg.Host, proxyCfg.Port)

	// 简单的 SOCKS4/4a 实现
	dialer := &socks4Dialer{
		addr:     socksAddr,
		username: proxyCfg.Username,
		is4a:     is4a,
	}

	transport := &http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			return dialer.Dial(network, addr)
		},
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   10,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		ResponseHeaderTimeout: 60 * time.Second,
	}

	return &http.Client{
		Transport: transport,
	}, nil
}

type socks4Dialer struct {
	addr     string
	username string
	is4a     bool
}

func (d *socks4Dialer) Dial(network, addr string) (net.Conn, error) {
	conn, err := net.Dial("tcp", d.addr)
	if err != nil {
		return nil, err
	}

	// 解析目标地址
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		conn.Close()
		return nil, err
	}
	port := 0
	fmt.Sscanf(portStr, "%d", &port)

	// SOCKS4 请求
	req := make([]byte, 0, 9+len(d.username)+1+len(host)+1)
	req = append(req, 4)                         // SOCKS version
	req = append(req, 1)                         // CONNECT command
	req = append(req, byte(port>>8), byte(port)) // port (network byte order)

	if d.is4a {
		// SOCKS4a: 发送 0.0.0.x 来表示域名
		req = append(req, 0, 0, 0, 1)
	} else {
		// SOCKS4: 解析 IP
		ip := net.ParseIP(host)
		if ip == nil {
			ip = net.IPv4(127, 0, 0, 1)
		}
		req = append(req, ip.To4()...)
	}
	req = append(req, []byte(d.username)...)
	req = append(req, 0)

	if d.is4a {
		// SOCKS4a: 附加域名
		req = append(req, []byte(host)...)
		req = append(req, 0)
	}

	// 发送请求
	if _, err := conn.Write(req); err != nil {
		conn.Close()
		return nil, err
	}

	// 读取响应
	resp := make([]byte, 8)
	if _, err := io.ReadFull(conn, resp); err != nil {
		conn.Close()
		return nil, err
	}

	if resp[1] != 90 {
		conn.Close()
		return nil, fmt.Errorf("socks4 request rejected, code: %d", resp[1])
	}

	return conn, nil
}

func CreateProxyClient(proxyCfg *config.ProxyConfig, timeout time.Duration, useDigest bool, excludeList []string) (*http.Client, error) {
	if proxyCfg == nil || !proxyCfg.IsValid() {
		return &http.Client{}, nil
	}

	switch proxyCfg.Type {
	case "http", "https":
		return NewHTTPProxyClient(proxyCfg, timeout, useDigest, excludeList)
	case "socks5":
		return CreateSOCKS5ProxyClient(proxyCfg, timeout)
	case "socks4":
		return CreateSOCKS4ProxyClient(proxyCfg, timeout, false)
	case "socks4a":
		return CreateSOCKS4ProxyClient(proxyCfg, timeout, true)
	default:
		return nil, fmt.Errorf("unsupported proxy type: %s", proxyCfg.Type)
	}
}

type ProxyManager struct {
	mu          sync.RWMutex
	globalProxy *config.ProxyConfig
	taskProxies map[string]*config.ProxyConfig
}

func NewProxyManager() *ProxyManager {
	return &ProxyManager{
		taskProxies: make(map[string]*config.ProxyConfig),
	}
}

func (pm *ProxyManager) SetGlobalProxy(proxyCfg *config.ProxyConfig) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	pm.globalProxy = proxyCfg
}

func (pm *ProxyManager) GetGlobalProxy() *config.ProxyConfig {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	return pm.globalProxy
}

func (pm *ProxyManager) SetTaskProxy(taskID string, proxyCfg *config.ProxyConfig) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	if proxyCfg == nil {
		delete(pm.taskProxies, taskID)
	} else {
		pm.taskProxies[taskID] = proxyCfg
	}
}

func (pm *ProxyManager) GetTaskProxy(taskID string) *config.ProxyConfig {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	if p, ok := pm.taskProxies[taskID]; ok {
		return p
	}
	return pm.globalProxy
}

func (pm *ProxyManager) RemoveTaskProxy(taskID string) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	delete(pm.taskProxies, taskID)
}

func (pm *ProxyManager) ClearGlobalProxy() {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	pm.globalProxy = nil
}

func (pm *ProxyManager) GetProxyForTask(taskID string) *config.ProxyConfig {
	return pm.GetTaskProxy(taskID)
}

func (d *Downloader) SetProxy(proxyCfg *config.ProxyConfig) {
	d.proxy = proxyCfg
	d.client = d.createClientWithProxy(proxyCfg)
}

func (d *Downloader) GetProxy() *config.ProxyConfig {
	return d.proxy
}

func (d *Downloader) createClientWithProxy(proxyCfg *config.ProxyConfig) *http.Client {
	if proxyCfg == nil || !proxyCfg.IsValid() {
		// 不设置 http.Client.Timeout：它会限制整个下载时长，大文件会被杀死。
		// 仅依赖 Transport 级的连接/读超时（见 NewDownloader）。
		return &http.Client{}
	}

	client, err := CreateProxyClient(proxyCfg, d.cfg.Timeout, proxyCfg.UseDigest, proxyCfg.ExcludeList)
	if err != nil {
		d.logger.Warnw("failed to create proxy client, using default",
			"error", err,
			"proxy_type", proxyCfg.Type,
			"proxy_host", proxyCfg.Host,
			"proxy_port", proxyCfg.Port,
		)
		return &http.Client{}
	}

	return client
}
