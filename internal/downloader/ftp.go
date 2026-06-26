package downloader

import (
	"bufio"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/textproto"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/nexus-dl/afd/pkg/config"
	"go.uber.org/zap"
)

type FTPClient struct {
	host     string
	port     string
	user     string
	password string
	useTLS   bool
	passive  bool
	conn     net.Conn
	tlsConn  *tls.Conn
	logger   *zap.SugaredLogger

	dataConn     net.Conn
	dataTlsConn  *tls.Conn
	transferType string
}

type ftpReader struct {
	reader   io.Reader
	client   *FTPClient
	progress func(int64)
}

func (r *ftpReader) Read(p []byte) (n int, err error) {
	n, err = r.reader.Read(p)
	if n > 0 && r.progress != nil {
		r.progress(int64(n))
	}
	return n, err
}

func (r *ftpReader) Close() error {
	if rc, ok := r.reader.(io.Closer); ok {
		return rc.Close()
	}
	return nil
}

func NewFTPClient(host, port, user, password string, useTLS, passive bool, logger *zap.SugaredLogger) *FTPClient {
	if port == "" {
		if useTLS {
			port = "990"
		} else {
			port = "21"
		}
	}
	if user == "" {
		user = "anonymous"
	}
	if password == "" {
		password = "anonymous@"
	}
	return &FTPClient{
		host:         host,
		port:         port,
		user:         user,
		password:     password,
		useTLS:       useTLS,
		passive:      passive,
		logger:       logger,
		transferType: "binary",
	}
}

func (c *FTPClient) Connect() error {
	addr := net.JoinHostPort(c.host, c.port)

	var err error
	if c.useTLS {
		tlsConfig := &tls.Config{
			ServerName:         c.host,
			InsecureSkipVerify: false,
		}
		c.tlsConn, err = tls.Dial("tcp", addr, tlsConfig)
		if err != nil {
			return fmt.Errorf("TLS connection failed: %w", err)
		}
		c.conn = c.tlsConn
	} else {
		c.conn, err = net.Dial("tcp", addr)
		if err != nil {
			return fmt.Errorf("TCP connection failed: %w", err)
		}
	}

	if _, _, err := c.readResponse(); err != nil {
		return fmt.Errorf("connection failed: %w", err)
	}

	return nil
}

func (c *FTPClient) readResponse() (int, string, error) {
	reader := textproto.NewReader(bufio.NewReader(c.conn))
	line, err := reader.ReadLine()
	if err != nil {
		return 0, "", err
	}

	if len(line) < 3 {
		return 0, "", fmt.Errorf("invalid response: %s", line)
	}

	code, err := strconv.Atoi(line[:3])
	if err != nil {
		return 0, "", fmt.Errorf("invalid response code: %s", line[:3])
	}

	message := line[4:]
	if len(line) > 4 && line[3] == '-' {
		for {
			l, err := reader.ReadLine()
			if err != nil {
				return code, message, err
			}
			if len(l) >= 4 && l[:3] == line[:3] && l[3] == ' ' {
				message = l[4:]
				break
			}
			message += "\n" + l
		}
	}

	return code, message, nil
}

func (c *FTPClient) sendCommand(format string, args ...any) (int, string, error) {
	cmd := fmt.Sprintf(format, args...)
	if c.logger != nil {
		c.logger.Debugw("FTP command", "cmd", cmd)
	}

	_, err := fmt.Fprintf(c.conn, "%s\r\n", cmd)
	if err != nil {
		return 0, "", err
	}

	return c.readResponse()
}

func (c *FTPClient) Login(user, password string) error {
	if user == "" {
		user = c.user
	}
	if password == "" {
		password = c.password
	}

	code, _, err := c.sendCommand("USER %s", user)
	if err != nil {
		return fmt.Errorf("USER command failed: %w", err)
	}

	if code == 331 {
		code, _, err = c.sendCommand("PASS %s", password)
		if err != nil {
			return fmt.Errorf("PASS command failed: %w", err)
		}
	}

	if code != 230 && code != 202 {
		return fmt.Errorf("login failed with code: %d", code)
	}

	code, _, err = c.sendCommand("TYPE %s", c.transferType)
	if err != nil {
		return fmt.Errorf("TYPE command failed: %w", err)
	}

	return nil
}

func (c *FTPClient) Size(path string) (int64, error) {
	code, message, err := c.sendCommand("SIZE %s", path)
	if err != nil {
		return 0, fmt.Errorf("SIZE command failed: %w", err)
	}

	if code != 213 {
		return 0, fmt.Errorf("SIZE command returned: %d %s", code, message)
	}

	size, err := strconv.ParseInt(message, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse size failed: %w", err)
	}

	return size, nil
}

func (c *FTPClient) FileSize(path string) (int64, error) {
	return c.Size(path)
}

func (c *FTPClient) RETR(path string, offset int64) (io.ReadCloser, error) {
	var dataConn net.Conn
	var err error

	if c.passive {
		dataConn, err = c.retrPassive(path, offset)
	} else {
		dataConn, err = c.retrActive(path, offset)
	}

	if err != nil {
		return nil, err
	}

	reader := &ftpReader{
		reader: dataConn,
		client: c,
	}

	return reader, nil
}

func (c *FTPClient) retrPassive(path string, offset int64) (net.Conn, error) {
	code, message, err := c.sendCommand("PASV")
	if err != nil {
		return nil, fmt.Errorf("PASV command failed: %w", err)
	}

	if code != 227 {
		return nil, fmt.Errorf("PASV command returned: %d %s", code, message)
	}

	host, port, err := c.parsePasvResponse(message)
	if err != nil {
		return nil, fmt.Errorf("parse PASV response: %w", err)
	}

	var dataConn net.Conn
	if c.useTLS {
		tlsConfig := &tls.Config{
			ServerName:         host,
			InsecureSkipVerify: false,
		}
		dataConn, err = tls.Dial("tcp", fmt.Sprintf("%s:%d", host, port), tlsConfig)
		if err != nil {
			return nil, fmt.Errorf("data TLS connection failed: %w", err)
		}
		c.dataTlsConn = dataConn.(*tls.Conn)

		if c.tlsConn != nil {
			if err := c.tlsConn.Handshake(); err != nil {
				return nil, fmt.Errorf("data TLS handshake failed: %w", err)
			}
		}
	} else {
		dataConn, err = net.Dial("tcp", net.JoinHostPort(host, fmt.Sprintf("%d", port)))
		if err != nil {
			return nil, fmt.Errorf("data connection failed: %w", err)
		}
	}
	c.dataConn = dataConn

	if offset > 0 {
		code, _, err = c.sendCommand("REST %d", offset)
		if err != nil {
			return nil, fmt.Errorf("REST command failed: %w", err)
		}
		if code != 350 {
			return nil, fmt.Errorf("REST command returned: %d", code)
		}
	}

	code, _, err = c.sendCommand("RETR %s", path)
	if err != nil {
		return nil, fmt.Errorf("RETR command failed: %w", err)
	}

	if code != 125 && code != 150 {
		return nil, fmt.Errorf("RETR command returned: %d", code)
	}

	return dataConn, nil
}

func (c *FTPClient) retrActive(path string, offset int64) (net.Conn, error) {
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		return nil, fmt.Errorf("listen for data connection: %w", err)
	}
	defer listener.Close()

	addr := listener.Addr().(*net.TCPAddr)
	host := strings.ReplaceAll(addr.IP.String(), ".", ",")
	port1 := addr.Port / 256
	port2 := addr.Port % 256

	code, _, err := c.sendCommand("PORT %s,%d,%d", host, port1, port2)
	if err != nil {
		return nil, fmt.Errorf("PORT command failed: %w", err)
	}

	if code != 200 {
		return nil, fmt.Errorf("PORT command returned: %d", code)
	}

	if offset > 0 {
		code, _, err = c.sendCommand("REST %d", offset)
		if err != nil {
			return nil, fmt.Errorf("REST command failed: %w", err)
		}
		if code != 350 {
			return nil, fmt.Errorf("REST command returned: %d", code)
		}
	}

	code, _, err = c.sendCommand("RETR %s", path)
	if err != nil {
		return nil, fmt.Errorf("RETR command failed: %w", err)
	}

	if code != 125 && code != 150 {
		return nil, fmt.Errorf("RETR command returned: %d", code)
	}

	dataConn, err := listener.Accept()
	if err != nil {
		return nil, fmt.Errorf("accept data connection: %w", err)
	}

	if c.useTLS {
		tlsConfig := &tls.Config{
			ServerName:         c.host,
			InsecureSkipVerify: false,
		}
		dataConn = tls.Client(dataConn, tlsConfig)
		c.dataTlsConn = dataConn.(*tls.Conn)
	}

	c.dataConn = dataConn
	return dataConn, nil
}

func (c *FTPClient) parsePasvResponse(response string) (string, int, error) {
	start := strings.Index(response, "(")
	end := strings.Index(response, ")")
	if start == -1 || end == -1 {
		return "", 0, fmt.Errorf("invalid PASV response: %s", response)
	}

	parts := strings.Split(response[start+1:end], ",")
	if len(parts) != 6 {
		return "", 0, fmt.Errorf("invalid PASV response: %s", response)
	}

	h1, _ := strconv.Atoi(parts[0])
	h2, _ := strconv.Atoi(parts[1])
	h3, _ := strconv.Atoi(parts[2])
	h4, _ := strconv.Atoi(parts[3])
	p1, _ := strconv.Atoi(parts[4])
	p2, _ := strconv.Atoi(parts[5])

	host := fmt.Sprintf("%d.%d.%d.%d", h1, h2, h3, h4)
	port := p1*256 + p2

	return host, port, nil
}

func (c *FTPClient) Quit() error {
	if c.conn == nil {
		return nil
	}

	c.sendCommand("QUIT")

	if c.dataConn != nil {
		c.dataConn.Close()
		c.dataConn = nil
	}
	if c.dataTlsConn != nil {
		c.dataTlsConn.Close()
		c.dataTlsConn = nil
	}
	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
	}
	if c.tlsConn != nil {
		c.tlsConn = nil
	}

	return nil
}

func (c *FTPClient) IsConnected() bool {
	return c.conn != nil
}

func (c *FTPClient) DownloadFile(remotePath, localPath string, resume bool) error {
	fileSize, err := c.FileSize(remotePath)
	if err != nil {
		return fmt.Errorf("get file size: %w", err)
	}

	existingSize := int64(0)
	if resume {
		if stat, err := os.Stat(localPath); err == nil {
			existingSize = stat.Size()
		}
	}

	if resume && existingSize > 0 && existingSize < fileSize {
		return c.DownloadFileRange(remotePath, localPath, existingSize)
	}

	if err := os.Remove(localPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove existing file: %w", err)
	}

	reader, err := c.RETR(remotePath, 0)
	if err != nil {
		return fmt.Errorf("RETR command failed: %w", err)
	}
	defer reader.Close()

	file, err := os.Create(localPath)
	if err != nil {
		return fmt.Errorf("create local file: %w", err)
	}
	defer file.Close()

	buf := make([]byte, 32*1024)
	for {
		n, readErr := reader.Read(buf)
		if n > 0 {
			_, writeErr := file.Write(buf[:n])
			if writeErr != nil {
				return fmt.Errorf("write file: %w", writeErr)
			}
		}

		if readErr != nil {
			if readErr == io.EOF {
				break
			}
			return fmt.Errorf("read data: %w", readErr)
		}
	}

	return nil
}

func (c *FTPClient) DownloadFileRange(remotePath, localPath string, start int64) error {
	fileSize, err := c.FileSize(remotePath)
	if err != nil {
		return fmt.Errorf("get file size: %w", err)
	}

	if start >= fileSize {
		return fmt.Errorf("start position >= file size")
	}

	reader, err := c.RETR(remotePath, start)
	if err != nil {
		return fmt.Errorf("RETR command failed: %w", err)
	}
	defer reader.Close()

	var file *os.File
	if start > 0 {
		file, err = os.OpenFile(localPath, os.O_CREATE|os.O_RDWR, 0644)
		if err != nil {
			return fmt.Errorf("open local file: %w", err)
		}
		defer file.Close()

		if _, err := file.Seek(start, io.SeekStart); err != nil {
			return fmt.Errorf("seek file: %w", err)
		}
	} else {
		if err := os.Remove(localPath); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("remove existing file: %w", err)
		}
		file, err = os.Create(localPath)
		if err != nil {
			return fmt.Errorf("create local file: %w", err)
		}
		defer file.Close()
	}

	buf := make([]byte, 32*1024)
	for {
		n, readErr := reader.Read(buf)
		if n > 0 {
			_, writeErr := file.Write(buf[:n])
			if writeErr != nil {
				return fmt.Errorf("write file: %w", writeErr)
			}
		}

		if readErr != nil {
			if readErr == io.EOF {
				break
			}
			return fmt.Errorf("read data: %w", readErr)
		}
	}

	return nil
}

type FTPDownloader struct {
	client     *FTPClient
	url        string
	outputPath string
	cfg        *config.DownloadConfig
	logger     *zap.SugaredLogger

	fileSize        int64
	totalDownloaded int64
	startTime       time.Time

	controlFilePath      string
	controlFile          any
	controlFileCompleted int64
	controlFileTotal     int64
	controlFileStatus    string
}

func NewFTPDownloader(url, outputPath string, cfg *config.DownloadConfig, logger *zap.SugaredLogger) (*FTPDownloader, error) {
	if cfg == nil {
		cfg = config.DefaultDownloadConfig()
	}

	parsedURL, err := parseFTPURL(url)
	if err != nil {
		return nil, fmt.Errorf("parse FTP URL: %w", err)
	}

	passive := true
	if parsedURL.passive != "" {
		passive = parsedURL.passive == "passive"
	}

	client := NewFTPClient(
		parsedURL.host,
		parsedURL.port,
		parsedURL.user,
		parsedURL.password,
		parsedURL.useTLS,
		passive,
		logger,
	)

	return &FTPDownloader{
		client:     client,
		url:        url,
		outputPath: outputPath,
		cfg:        cfg,
		logger:     logger,
		startTime:  time.Now(),
	}, nil
}

type parsedFTPURL struct {
	scheme   string
	host     string
	port     string
	user     string
	password string
	path     string
	useTLS   bool
	passive  string
}

func parseFTPURL(urlStr string) (*parsedFTPURL, error) {
	result := &parsedFTPURL{}

	if strings.HasPrefix(urlStr, "ftps://") {
		result.scheme = "ftps"
		result.useTLS = true
		urlStr = strings.TrimPrefix(urlStr, "ftps://")
	} else if strings.HasPrefix(urlStr, "ftp://") {
		result.scheme = "ftp"
		result.useTLS = false
		urlStr = strings.TrimPrefix(urlStr, "ftp://")
	} else {
		return nil, fmt.Errorf("not an FTP URL: %s", urlStr)
	}

	if idx := strings.Index(urlStr, "@"); idx != -1 {
		credentials := urlStr[:idx]
		urlStr = urlStr[idx+1:]

		if colonIdx := strings.Index(credentials, ":"); colonIdx != -1 {
			result.user = credentials[:colonIdx]
			result.password = credentials[colonIdx+1:]
		} else {
			result.user = credentials
		}
	}

	pathIdx := strings.Index(urlStr, "/")
	var hostPort string
	if pathIdx == -1 {
		hostPort = urlStr
		result.path = "/"
	} else {
		hostPort = urlStr[:pathIdx]
		result.path = urlStr[pathIdx:]
	}

	if strings.HasPrefix(hostPort, "[") {
		endBracket := strings.Index(hostPort, "]")
		if endBracket == -1 {
			return nil, fmt.Errorf("invalid IPv6 address format")
		}
		result.host = hostPort[1:endBracket]
		if endBracket+1 < len(hostPort) {
			rest := hostPort[endBracket+1:]
			if strings.HasPrefix(rest, ":") {
				result.port = rest[1:]
			}
		}
	} else {
		if colonIdx := strings.Index(hostPort, ":"); colonIdx != -1 {
			result.host = hostPort[:colonIdx]
			result.port = hostPort[colonIdx+1:]
		} else {
			result.host = hostPort
		}
	}

	if result.port == "" {
		if result.useTLS {
			result.port = "990"
		} else {
			result.port = "21"
		}
	}

	if strings.Contains(result.path, "?") {
		queryPart := strings.Split(result.path, "?")[1]
		params := strings.Split(queryPart, "&")
		for _, param := range params {
			kv := strings.Split(param, "=")
			if len(kv) == 2 {
				switch kv[0] {
				case "passive":
					result.passive = kv[1]
				case "tls":
					if kv[1] == "true" {
						result.useTLS = true
					}
				}
			}
		}
	}

	return result, nil
}

func IsFTPURL(url string) bool {
	return strings.HasPrefix(url, "ftp://") || strings.HasPrefix(url, "ftps://")
}

func (d *FTPDownloader) SetURL(url string) {
	d.url = url
}

func (d *FTPDownloader) SetOutputPath(path string) {
	d.outputPath = path
}

func (d *FTPDownloader) SetControlFilePath(path string) {
	d.controlFilePath = path
}

func (d *FTPDownloader) SetControlFile(cf any) {
	d.controlFile = cf
}

func (d *FTPDownloader) URL() string {
	return d.url
}

func (d *FTPDownloader) OutputPath() string {
	return d.outputPath
}

func (d *FTPDownloader) FileSize() int64 {
	return d.fileSize
}

func (d *FTPDownloader) Download(ctx context.Context) error {
	d.startTime = time.Now()

	if err := d.client.Connect(); err != nil {
		return fmt.Errorf("FTP connect failed: %w", err)
	}
	// Single, guaranteed Quit on every exit path.  Idempotent.
	defer d.client.Quit()

	if err := d.client.Login(d.client.user, d.client.password); err != nil {
		return fmt.Errorf("FTP login failed: %w", err)
	}

	parsedURL, err := parseFTPURL(d.url)
	if err != nil {
		return fmt.Errorf("parse URL: %w", err)
	}

	fileSize, err := d.client.Size(parsedURL.path)
	if err != nil {
		return fmt.Errorf("get file size: %w", err)
	}
	d.fileSize = fileSize

	d.logger.Infow("starting FTP download",
		"file_size", fileSize,
		"url", d.url,
		"output", d.outputPath,
	)

	existingSize := int64(0)
	if stat, err := os.Stat(d.outputPath); err == nil {
		existingSize = stat.Size()
	}

	if existingSize == fileSize && fileSize > 0 {
		d.logger.Infow("file already fully downloaded, skipping")
		atomic.StoreInt64(&d.totalDownloaded, fileSize)
		return nil
	}

	var file *os.File
	if existingSize > 0 {
		file, err = os.OpenFile(d.outputPath, os.O_CREATE|os.O_RDWR, 0644)
		if err != nil {
			return fmt.Errorf("open output file: %w", err)
		}
	} else {
		file, err = os.Create(d.outputPath)
		if err != nil {
			return fmt.Errorf("create output file: %w", err)
		}
	}
	defer file.Close()

	if existingSize > 0 {
		if _, err := file.Seek(existingSize, io.SeekStart); err != nil {
			return fmt.Errorf("seek file: %w", err)
		}
		atomic.StoreInt64(&d.totalDownloaded, existingSize)
	}

	reader, err := d.client.RETR(parsedURL.path, existingSize)
	if err != nil {
		return fmt.Errorf("RETR command failed: %w", err)
	}
	defer reader.Close()

	buf := make([]byte, d.cfg.BufferSize)
	var downloaded int64

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		n, readErr := reader.Read(buf)
		if n > 0 {
			if _, writeErr := file.Write(buf[:n]); writeErr != nil {
				return fmt.Errorf("write file: %w", writeErr)
			}

			downloaded += int64(n)
			atomic.AddInt64(&d.totalDownloaded, int64(n))
		}

		if readErr != nil {
			if readErr == io.EOF {
				break
			}
			return fmt.Errorf("read data: %w", readErr)
		}
	}

	d.logger.Infow("FTP download completed",
		"total_bytes", atomic.LoadInt64(&d.totalDownloaded),
		"duration", time.Since(d.startTime),
	)

	return nil
}

func (d *FTPDownloader) Speed() int64 {
	return 0
}

func (d *FTPDownloader) Progress() float64 {
	if d.fileSize <= 0 {
		return 0
	}
	downloaded := atomic.LoadInt64(&d.totalDownloaded)
	return float64(downloaded) / float64(d.fileSize) * 100
}

func (d *FTPDownloader) TotalDownloaded() int64 {
	return atomic.LoadInt64(&d.totalDownloaded)
}

func (d *FTPDownloader) ActiveThreads() int32 {
	return 1
}

func (d *FTPDownloader) SetRateLimit(rate int64) {
}

func (d *FTPDownloader) GetRateLimit() int64 {
	return 0
}

func (d *FTPDownloader) SetRetryConfig(config RetryConfig) {
}

func (d *FTPDownloader) GetRetryConfig() RetryConfig {
	return DefaultRetryConfig()
}

func (d *FTPDownloader) LoadProgress(ctx context.Context) error {
	return nil
}

func (d *FTPDownloader) SaveProgress() error {
	return nil
}

type FTPProtocolHandler struct {
	logger *zap.SugaredLogger
	cfg    *config.DownloadConfig
}

func NewFTPProtocolHandler(logger *zap.SugaredLogger, cfg *config.DownloadConfig) *FTPProtocolHandler {
	if cfg == nil {
		cfg = config.DefaultDownloadConfig()
	}
	return &FTPProtocolHandler{
		logger: logger,
		cfg:    cfg,
	}
}

func (h *FTPProtocolHandler) CanHandle(url string) bool {
	return IsFTPURL(url)
}

func (h *FTPProtocolHandler) NewDownloader(url, outputPath string) (DownloaderInterface, error) {
	return NewFTPDownloader(url, outputPath, h.cfg, h.logger)
}

func (h *FTPProtocolHandler) GetFileInfo(url string) (int64, error) {
	parsedURL, err := parseFTPURL(url)
	if err != nil {
		return 0, fmt.Errorf("parse FTP URL: %w", err)
	}

	passive := true
	if parsedURL.passive != "" {
		passive = parsedURL.passive == "passive"
	}

	client := NewFTPClient(
		parsedURL.host,
		parsedURL.port,
		parsedURL.user,
		parsedURL.password,
		parsedURL.useTLS,
		passive,
		h.logger,
	)

	if err := client.Connect(); err != nil {
		return 0, fmt.Errorf("FTP connect failed: %w", err)
	}
	defer client.Quit()

	if err := client.Login(parsedURL.user, parsedURL.password); err != nil {
		return 0, fmt.Errorf("FTP login failed: %w", err)
	}

	return client.FileSize(parsedURL.path)
}
