package cluster

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/nexus-dl/afd/pkg/logger"
)

const NodeIDLength = 16

func GenerateNodeID() (string, error) {
	b := make([]byte, NodeIDLength)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("failed to generate node ID: %w", err)
	}
	return hex.EncodeToString(b), nil
}

type ClusterAuth struct {
	token      string
	tlsConfig  *tls.Config
	caCertPool *x509.CertPool
	certFile   string
	keyFile    string
	tlsEnabled bool
}

func NewClusterAuth(token, certFile, keyFile string) (*ClusterAuth, error) {
	ca := &ClusterAuth{
		token:    token,
		certFile: certFile,
		keyFile:  keyFile,
	}

	if certFile != "" && keyFile != "" {
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load TLS certificate: %w", err)
		}

		caCertPool := x509.NewCertPool()
		certData, err := os.ReadFile(certFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read certificate file: %w", err)
		}
		if !caCertPool.AppendCertsFromPEM(certData) {
			return nil, fmt.Errorf("failed to append certificate to pool")
		}
		ca.caCertPool = caCertPool

		ca.tlsConfig = &tls.Config{
			Certificates: []tls.Certificate{cert},
			ClientAuth:   tls.RequireAndVerifyClientCert,
			ClientCAs:    caCertPool,
			MinVersion:   tls.VersionTLS12,
		}
		ca.tlsEnabled = true
		logger.Log.Info("TLS enabled for cluster communication")
	}

	return ca, nil
}

func (ca *ClusterAuth) TLSConfig() *tls.Config {
	if ca.tlsConfig != nil {
		return ca.tlsConfig.Clone()
	}
	return nil
}

func (ca *ClusterAuth) ClientTLSConfig() *tls.Config {
	if ca.tlsConfig != nil {
		clientCfg := ca.tlsConfig.Clone()
		clientCfg.ClientAuth = tls.NoClientCert
		clientCfg.ServerName = ""
		if ca.caCertPool != nil {
			clientCfg.RootCAs = ca.caCertPool
		}
		return clientCfg
	}
	return nil
}

func (ca *ClusterAuth) IsTLSEnabled() bool {
	return ca.tlsEnabled
}

func (ca *ClusterAuth) ValidateToken(token string) bool {
	if ca.token == "" {
		return true
	}
	return subtle.ConstantTimeCompare([]byte(token), []byte(ca.token)) == 1
}

func (ca *ClusterAuth) Token() string {
	return ca.token
}

func (ca *ClusterAuth) DialTLS(network, addr string) (net.Conn, error) {
	if ca.tlsEnabled {
		return tls.Dial(network, addr, ca.ClientTLSConfig())
	}
	return net.Dial(network, addr)
}

func (ca *ClusterAuth) ListenTLS(network, addr string) (net.Listener, error) {
	if ca.tlsEnabled {
		return tls.Listen(network, addr, ca.tlsConfig)
	}
	return net.Listen(network, addr)
}

func GenerateSelfSignedCert(organization, host string) (certFile, keyFile string, err error) {
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return "", "", fmt.Errorf("failed to generate private key: %w", err)
	}

	serialNumber, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return "", "", fmt.Errorf("failed to generate serial number: %w", err)
	}

	template := x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			Organization: []string{organization},
			CommonName:   host,
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(365 * 24 * time.Hour),
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	if ip := net.ParseIP(host); ip != nil {
		template.IPAddresses = []net.IP{ip}
	} else {
		template.DNSNames = []string{host}
	}

	template.IPAddresses = append(template.IPAddresses, net.ParseIP("127.0.0.1"))
	template.DNSNames = append(template.DNSNames, "localhost")

	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	if err != nil {
		return "", "", fmt.Errorf("failed to create certificate: %w", err)
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	keyPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(priv),
	})

	certFile = safeCertName(host) + ".crt"
	keyFile = safeCertName(host) + ".key"

	if err := os.WriteFile(certFile, certPEM, 0600); err != nil {
		return "", "", fmt.Errorf("failed to write certificate file: %w", err)
	}
	if err := os.WriteFile(keyFile, keyPEM, 0600); err != nil {
		return "", "", fmt.Errorf("failed to write key file: %w", err)
	}

	return certFile, keyFile, nil
}

// safeCertName sanitizes a user-supplied hostname into a filename-safe
// token so GenerateSelfSignedCert cannot escape the working directory
// via path separators or traversal segments.
func safeCertName(host string) string {
	host = strings.TrimSpace(host)
	if host == "" {
		return "afd-node"
	}
	host = filepath.Base(host)
	host = strings.ReplaceAll(host, "..", "_")
	host = strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z',
			r >= 'A' && r <= 'Z',
			r >= '0' && r <= '9',
			r == '-', r == '_', r == '.':
			return r
		}
		return '_'
	}, host)
	if host == "" || host == "." || host == ".." {
		return "afd-node"
	}
	return host
}

func NewClientTLSConfig(caCertFile, certFile, keyFile string) (*tls.Config, error) {
	caCert, err := os.ReadFile(caCertFile)
	if err != nil {
		return nil, fmt.Errorf("failed to read CA certificate: %w", err)
	}

	caCertPool := x509.NewCertPool()
	if !caCertPool.AppendCertsFromPEM(caCert) {
		return nil, fmt.Errorf("failed to parse CA certificate")
	}

	var certs []tls.Certificate
	if certFile != "" && keyFile != "" {
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load client certificate: %w", err)
		}
		certs = append(certs, cert)
	}

	return &tls.Config{
		Certificates: certs,
		RootCAs:      caCertPool,
		MinVersion:   tls.VersionTLS12,
	}, nil
}
