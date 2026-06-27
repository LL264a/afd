package downloader

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/nexus-dl/afd/pkg/logger"
	"github.com/pkg/sftp"
	"go.uber.org/zap"
	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/agent"
	"golang.org/x/crypto/ssh/knownhosts"
)

type SFTPDownloader struct {
	url           string
	outputPath    string
	client        *sftp.Client
	sshClient     *ssh.Client
	sshAgent      net.Conn // SSH agent 连接，随 Close 一起释放
	logger        *zap.SugaredLogger
	downloaded    int64
	speed         int64
	activeThreads int32
	rateLimit     int64
	retryConfig   RetryConfig
	config        *SFTPConfig
}

type SFTPConfig struct {
	Host           string
	Port           int
	Username       string
	Password       string
	PrivateKey     string
	Passphrase     string
	Timeout        time.Duration
	KnownHostsFile string
}

func IsSFTPURL(input string) bool {
	return strings.HasPrefix(input, "sftp://")
}

func ParseSFTPURL(input string) (*SFTPConfig, string, error) {
	u, err := url.Parse(input)
	if err != nil {
		return nil, "", err
	}

	if u.Scheme != "sftp" {
		return nil, "", fmt.Errorf("not an sftp url")
	}

	host := u.Hostname()
	port := 22
	if p := u.Port(); p != "" {
		fmt.Sscanf(p, "%d", &port)
	}

	username := u.User.Username()
	password, _ := u.User.Password()

	return &SFTPConfig{
		Host:     host,
		Port:     port,
		Username: username,
		Password: password,
		Timeout:  30 * time.Second,
	}, u.Path, nil
}

func NewSFTPDownloader(url, outputPath string, cfg *SFTPConfig) *SFTPDownloader {
	if cfg == nil {
		cfg = &SFTPConfig{}
	}
	return &SFTPDownloader{
		url:        url,
		outputPath: outputPath,
		logger:     logger.Log.Named("sftp-downloader"),
		config:     cfg,
	}
}

func (d *SFTPDownloader) connect() error {
	cfg, _, err := ParseSFTPURL(d.url)
	if err != nil {
		return err
	}

	if d.config.Host == "" {
		d.config = cfg
	}

	authMethods := []ssh.AuthMethod{}

	if d.config.Password != "" {
		authMethods = append(authMethods, ssh.Password(d.config.Password))
	}

	if d.config.PrivateKey != "" {
		signer, err := d.getSigner()
		if err != nil {
			d.logger.Warnw("Failed to load private key", "error", err)
		} else {
			authMethods = append(authMethods, ssh.PublicKeys(signer))
		}
	}

	if sshAgent, err := net.Dial("unix", os.Getenv("SSH_AUTH_SOCK")); err == nil {
		d.sshAgent = sshAgent // 保存到结构体，随 Close 一起释放
		agent := agent.NewClient(sshAgent)
		authMethods = append(authMethods, ssh.PublicKeysCallback(agent.Signers))
	}

	hostKeyCallback, err := d.getHostKeyCallback()
	if err != nil {
		d.logger.Warnw("Failed to load known_hosts, using insecure mode", "error", err)
		hostKeyCallback = ssh.InsecureIgnoreHostKey()
	}

	sshConfig := &ssh.ClientConfig{
		User:            d.config.Username,
		Auth:            authMethods,
		HostKeyCallback: hostKeyCallback,
		Timeout:         d.config.Timeout,
	}

	addr := fmt.Sprintf("%s:%d", d.config.Host, d.config.Port)
	sshClient, err := ssh.Dial("tcp", addr, sshConfig)
	if err != nil {
		return fmt.Errorf("failed to dial ssh: %w", err)
	}
	d.sshClient = sshClient

	sftpClient, err := sftp.NewClient(sshClient)
	if err != nil {
		sshClient.Close()
		return fmt.Errorf("failed to create sftp client: %w", err)
	}
	d.client = sftpClient

	return nil
}

func (d *SFTPDownloader) getSigner() (ssh.Signer, error) {
	keyBytes, err := os.ReadFile(d.config.PrivateKey)
	if err != nil {
		return nil, err
	}

	var signer ssh.Signer
	if d.config.Passphrase != "" {
		signer, err = ssh.ParsePrivateKeyWithPassphrase(keyBytes, []byte(d.config.Passphrase))
	} else {
		signer, err = ssh.ParsePrivateKey(keyBytes)
	}
	if err != nil {
		return nil, err
	}
	return signer, nil
}

func (d *SFTPDownloader) getHostKeyCallback() (ssh.HostKeyCallback, error) {
	knownHosts := d.config.KnownHostsFile
	if knownHosts == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return nil, fmt.Errorf("cannot determine home directory: %w", err)
		}
		knownHosts = filepath.Join(home, ".ssh", "known_hosts")
	}

	if _, err := os.Stat(knownHosts); err != nil {
		return nil, fmt.Errorf("known_hosts file not found: %w", err)
	}

	return knownhosts.New(knownHosts)
}

func (d *SFTPDownloader) SetURL(url string)                      { d.url = url }
func (d *SFTPDownloader) SetOutputPath(path string)              { d.outputPath = path }
func (d *SFTPDownloader) SetControlFilePath(path string)         {}
func (d *SFTPDownloader) SetControlFile(cf interface{})          {}
func (d *SFTPDownloader) URL() string                            { return d.url }
func (d *SFTPDownloader) OutputPath() string                     { return d.outputPath }
func (d *SFTPDownloader) FileSize() int64                         { return 0 }
func (d *SFTPDownloader) Speed() int64                           { return atomic.LoadInt64(&d.speed) }
func (d *SFTPDownloader) Progress() float64                      { return 0 }
func (d *SFTPDownloader) TotalDownloaded() int64                 { return atomic.LoadInt64(&d.downloaded) }
func (d *SFTPDownloader) ActiveThreads() int32                   { return atomic.LoadInt32(&d.activeThreads) }
func (d *SFTPDownloader) SetRateLimit(rate int64)                { atomic.StoreInt64(&d.rateLimit, rate) }
func (d *SFTPDownloader) GetRateLimit() int64                    { return atomic.LoadInt64(&d.rateLimit) }
func (d *SFTPDownloader) SetRetryConfig(config RetryConfig)      { d.retryConfig = config }
func (d *SFTPDownloader) GetRetryConfig() RetryConfig            { return d.retryConfig }
func (d *SFTPDownloader) LoadProgress(ctx context.Context) error { return nil }
func (d *SFTPDownloader) SaveProgress() error                    { return nil }

// Close 释放 SFTPDownloader 持有的底层资源，包括 SSH agent 连接。
func (d *SFTPDownloader) Close() error {
	if d.sshAgent != nil {
		d.sshAgent.Close()
		d.sshAgent = nil
	}
	return nil
}

func (d *SFTPDownloader) Download(ctx context.Context) error {
	defer d.Close() // 确保 sshAgent 在所有退出路径上被释放
	if err := d.connect(); err != nil {
		return err
	}
	defer d.client.Close()
	defer d.sshClient.Close()

	_, remotePath, err := ParseSFTPURL(d.url)
	if err != nil {
		return err
	}

	remoteFile, err := d.client.Open(remotePath)
	if err != nil {
		return fmt.Errorf("failed to open remote file: %w", err)
	}
	defer remoteFile.Close()

	_, err = remoteFile.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat remote file: %w", err)
	}

	localPath := d.outputPath
	if localPath == "" {
		localPath = filepath.Base(remotePath)
	}

	localFile, err := os.OpenFile(localPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("failed to create local file: %w", err)
	}
	defer localFile.Close()

	buf := make([]byte, 32*1024)
	var totalDownloaded int64
	var lastProgress time.Time
	var lastBytes int64

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		n, err := remoteFile.Read(buf)
		if n > 0 {
			if _, writeErr := localFile.Write(buf[:n]); writeErr != nil {
				return fmt.Errorf("failed to write to local file: %w", writeErr)
			}
			totalDownloaded += int64(n)
			atomic.StoreInt64(&d.downloaded, totalDownloaded)

			now := time.Now()
			if now.Sub(lastProgress) >= time.Second {
				elapsed := now.Sub(lastProgress).Seconds()
				if elapsed > 0 {
					atomic.StoreInt64(&d.speed, int64(float64(totalDownloaded-lastBytes)/elapsed))
				}
				lastBytes = totalDownloaded
				lastProgress = now
			}
		}

		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("failed to read remote file: %w", err)
		}
	}

	d.logger.Infow("SFTP download completed", "path", localPath, "size", totalDownloaded)
	return nil
}

type SFTPProtocolHandler struct {
	cfg *SFTPConfig
}

func NewSFTPProtocolHandler(cfg *SFTPConfig) *SFTPProtocolHandler {
	return &SFTPProtocolHandler{cfg: cfg}
}

func (h *SFTPProtocolHandler) CanHandle(input string) bool {
	return IsSFTPURL(input)
}

func (h *SFTPProtocolHandler) NewDownloader(url, outputPath string) interface {
	Download(context.Context) error
} {
	return NewSFTPDownloader(url, outputPath, h.cfg)
}
