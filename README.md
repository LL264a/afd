# AFD - 自动下载工具

[English](./README_EN.md) | 中文

AFD 是一个分布式、集群感知的下载系统，可作为 aria2 的现代替代品。

## 特性

- **多协议支持**: HTTP, HTTPS, FTP, BitTorrent, S3, WebDAV, SFTP
- **高性能**: 多线程下载，自适应分块
- **集群支持**: 多节点分布式下载
- **简洁 CLI**: 类似 aria2 的命令行界面

## 快速开始

```bash
# 直接下载
afd http://example.com/file.zip

# 指定输出文件
afd -o file.zip http://example.com/file.zip

# 多线程下载
afd -s 4 http://example.com/file.zip

# 速度限制
afd --speed-limit 1M http://example.com/file.zip

# 批量下载
afd -i urls.txt

# 下载到目录
afd -d /downloads -i urls.txt
```

## 安装

### 一键安装 (Linux/macOS)
```bash
curl -sL https://raw.githubusercontent.com/nexus-dl/afd/main/install.sh | bash
```

### 手动安装
```bash
# 安装 Go 1.25+
wget https://go.dev/dl/go1.25.0.linux-amd64.tar.gz
sudo tar -C /usr/local -xzf go1.25.0.linux-amd64.tar.gz

# 编译
git clone https://github.com/nexus-dl/afd.git
cd afd
go build -o afd ./cmd/afd

# 安装
sudo mv afd /usr/local/bin/
```

## 配置

默认配置位置: `~/.afd/config.yaml`

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

## 批量下载格式

创建包含 URL 的文本文件（每行一个）:

```
http://example.com/file1.zip
http://example.com/file2.zip
dir=/downloads
out=custom.zip
http://example.com/file3.zip
```

## 命令行选项

| 选项 | 说明 |
|------|------|
| `-o, --output` | 输出文件路径 |
| `-s, --split` | 下载线程数 |
| `-d, --dir` | 下载保存目录 |
| `-i, --input-file` | 批量下载文件 |
| `--speed-limit` | 速度限制 (如 1M, 500K) |
| `--timeout` | 超时时间 (秒) |

## 许可证

MIT License