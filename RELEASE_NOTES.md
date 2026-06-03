## AFD v1.0.0 Release

### Downloads

| Platform | Architecture | File |
|----------|--------------|------|
| Linux | 386 | afd-linux-386 |
| Linux | amd64 | afd-linux-amd64 |
| Linux | arm | afd-linux-arm |
| Linux | arm64 | afd-linux-arm64 |
| macOS | amd64 | afd-darwin-amd64 |
| macOS | arm64 | afd-darwin-arm64 |
| Windows | 386 | afd-windows-386.exe |
| Windows | amd64 | afd-windows-amd64.exe |
| Windows | arm64 | afd-windows-arm64.exe |
| FreeBSD | 386 | afd-freebsd-386 |
| FreeBSD | amd64 | afd-freebsd-amd64 |
| FreeBSD | arm | afd-freebsd-arm |
| FreeBSD | arm64 | afd-freebsd-arm64 |
| OpenBSD | 386 | afd-openbsd-386 |
| OpenBSD | amd64 | afd-openbsd-amd64 |
| OpenBSD | arm | afd-openbsd-arm |
| OpenBSD | arm64 | afd-openbsd-arm64 |
| NetBSD | 386 | afd-netbsd-386 |
| NetBSD | amd64 | afd-netbsd-amd64 |
| NetBSD | arm | afd-netbsd-arm |
| NetBSD | arm64 | afd-netbsd-arm64 |
| DragonFly | amd64 | afd-dragonfly-amd64 |
| Solaris | amd64 | afd-solaris-amd64 |

### Quick Install

```bash
# Linux/macOS
curl -sL https://github.com/nexus-dl/afd/releases/download/v1.0.0/afd-linux-amd64 -o afd
chmod +x afd

# Windows
# Download afd-windows-amd64.exe from releases
```

### Features
- Direct download (like aria2c)
- Multi-threaded download
- Batch download support
- Speed limit
- Multiple protocols (HTTP, FTP, BT, S3, WebDAV, SFTP)