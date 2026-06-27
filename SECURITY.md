# Security Policy

## Supported Versions

| Version | Supported          |
| ------- | ------------------ |
| 0.1.x   | :white_check_mark: |
| < 0.1   | :x:                |

## Reporting a Vulnerability

**Please do not file public GitHub issues for security vulnerabilities.**

Report privately by email to **security@afd.example.com** (or open a
private security advisory on GitHub: <https://github.com/nexus-dl/afd/security/advisories/new>).

A maintainer will acknowledge receipt within **48 hours** and provide a triage
plan within **5 business days**.

When reporting, please include:

- A clear description of the issue and its impact
- Reproduction steps (proof-of-concept preferred)
- Affected commit / version
- Any known mitigations

We follow [coordinated disclosure](https://en.wikipedia.org/wiki/Coordinated_vulnerability_disclosure).
Please give us a reasonable window (typically 90 days) before any public
disclosure so we can ship a fix and CVE if applicable.

## Security Design Notes

AFD is hardened against the most common attack surfaces:

- API authentication with constant-time token comparison
  ([`subtle.ConstantTimeCompare`](https://pkg.go.dev/crypto/subtle))
- Per-IP request rate limiting (token bucket, configurable)
- Request body size limited to 10 MB on RPC endpoints
- URL scheme allow-list on input (HTTP/HTTPS/FTP/FTPS/S3/WebDAV/SFTP/Magnet)
- Output path validation rejects traversal patterns
- Cluster token comparison hardened against timing attacks
- TLS for S3, FTPS, HTTPS, SFTP supported natively
- All API errors are returned as JSON (no information leakage in HTML stack traces)
- The `/metrics`, `/health`, `/ready` endpoints are exempt from auth
  to allow Prometheus and Kubernetes probes

## Best Practices for Deployment

- **Always set `AFD_API_AUTH_TOKEN`** to a random ≥ 32 byte secret:
  `openssl rand -hex 32`
- Run behind a reverse proxy (nginx, Caddy, Traefik) with TLS termination
- Restrict the cluster gRPC port (`50051/tcp`) and UDP discovery (`50052/udp`)
  to a trusted network or VPN
- Place the data directory on a filesystem with a quota
- Enable `download.sparse_file` only on filesystems that support it
- Set `node.data_dir` on a separate partition to avoid filling the root FS
- Keep the binary up to date; subscribe to releases
  (GitHub Releases / Atom feed)

## Acknowledgements

We thank the security researchers and community members who report
vulnerabilities responsibly. Reporters are credited in the release notes
unless they prefer to remain anonymous.
