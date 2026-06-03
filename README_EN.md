# AFD - Auto Download Tool

[中文](./README.md) | English

AFD is a distributed, cluster-aware download system, designed as a modern alternative to aria2.

## Features

- **Multiple Protocols**: HTTP, HTTPS, FTP, BitTorrent, S3, WebDAV, SFTP
- **High Performance**: Multi-threaded downloading, adaptive chunking
- **Cluster Support**: Distributed download across multiple nodes
- **Simple CLI**: aria2-like command line interface

## Quick Start

```bash
# Direct download
afd http://example.com/file.zip

# Specify output file
afd -o file.zip http://example.com/file.zip

# Multi-threaded download
afd -s 4 http://example.com/file.zip

# Speed limit
afd --speed-limit 1M http://example.com/file.zip

# Batch download
afd -i urls.txt

# Download to directory
afd -d /downloads -i urls.txt
```

## Installation

### One-line install (Linux/macOS)
```bash
curl -sL https://raw.githubusercontent.com/nexus-dl/afd/main/install.sh | bash
```

### Manual install
```bash
# Install Go 1.25+
wget https://go.dev/dl/go1.25.0.linux-amd64.tar.gz
sudo tar -C /usr/local -xzf go1.25.0.linux-amd64.tar.gz

# Build
git clone https://github.com/nexus-dl/afd.git
cd afd
go build -o afd ./cmd/afd

# Install
sudo mv afd /usr/local/bin/
```

## Configuration

Default config location: `~/.afd/config.yaml`

```yaml
node:
  id: "node-1"
  name: "AFD Node"
  data_dir: "~/.afd/downloads"
  log_level: "info"

api:
  host: "0.0.0.0"
  port: 8080

cluster:
  enabled: true
  grpc_port: 9999

download:
  max_connections: 16
  buffer_size: 1048576
  timeout: 300
```

## Batch Download Format

Create a text file with URLs (one per line):

```
http://example.com/file1.zip
http://example.com/file2.zip
dir=/downloads
out=custom.zip
http://example.com/file3.zip
```

## CLI Options

| Option | Description |
|--------|-------------|
| `-o, --output` | Output file path |
| `-s, --split` | Number of download threads |
| `-d, --dir` | Download directory |
| `-i, --input-file` | Batch download file |
| `--speed-limit` | Speed limit (e.g. 1M, 500K) |
| `--timeout` | Timeout in seconds |

## License

MIT License