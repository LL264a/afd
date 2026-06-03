# NexusDL

> A distributed, cluster-aware multi-protocol download system with an aria2-compatible RPC interface.

[![Go Version](https://img.shields.io/badge/Go-1.20+-00ADD8)](https://go.dev/)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Tests](https://img.shields.io/badge/tests-passing-brightgreen)](https://github.com/nexus-dl/nexus-dl)
[![codecov](https://codecov.io/gh/nexus-dl/nexus-dl/branch/main/graph/badge.svg)](https://codecov.io/gh/nexus-dl/nexus-dl)
[![Go Report Card](https://goreportcard.com/badge/github.com/nexus-dl/nexus-dl)](https://goreportcard.com/report/github.com/nexus-dl/nexus-dl)

## Features

- **Multi-protocol**: HTTP/HTTPS, FTP/FTPS, BitTorrent, S3, WebDAV, SFTP, Metalink
- **Resumable**: Automatic resume of interrupted downloads (HTTP Range, FTP REST, S3 multipart)
- **Cluster-aware**: Multi-node deployment with gRPC and UDP-based discovery
- **Load balancing**: Tasks dispatch to the least-loaded online node
- **Failover**: Automatic task re-assignment when a node goes offline
- **Rate limiting**: Global, per-task, and time-scheduled speed limits
- **Proxy support**: HTTP/HTTPS/SOCKS5/SOCKS4(a) with auth and per-host exclude list
- **Retry**: Exponential backoff for transient failures
- **Real-time events**: WebSocket push for task progress, node updates, and cluster events
- **Post-processing**: Automatic archive extraction (zip, tar, tar.gz, tar.bz2)
- **Hooks / Plugins**: Pre-download and post-completion hooks for scripting
- **NAT traversal**: STUN, UDP/TCP hole punching, relay fallback (optional)
- **Aria2-compatible RPC**: `addUri`, `addTorrent`, `tellStatus`, `multicall`, ... over JSON-RPC and XML-RPC
- **Standalone web UI**: ships separately from [`ui/`](../ui) (Vite + React + TypeScript); deployable as an independent nginx / CDN image
- **Prometheus metrics**: `/metrics` endpoint for monitoring
- **CLI**: `add` / `pause` / `resume` / `remove` / `show` / `list` / `status` / `nodes` / `config` / `log-level`
- **Auth**: `Authorization: Bearer` or `X-API-Key` header, constant-time comparison
- **Rate-limit aware**: Per-IP request rate limiting
- **Graceful shutdown**: SIGINT/SIGTERM with in-flight request drain

## Quick Start

### Run from source

```bash
git clone https://github.com/nexus-dl/nexus-dl.git
cd nexus-dl
go build -o nexus-dl ./cmd/nexus-dl
./nexus-dl serve                       # http://localhost:8080
```

### Run with Docker

```bash
docker compose up -d
```

### Add a download

```bash
curl -X POST http://localhost:8080/api/tasks \
  -H "Content-Type: application/json" \
  -d '{"url":"https://example.com/file.zip","output_path":"./downloads/file.zip"}'
```

Web UI: deploy [`ui/`](../ui) and proxy `/api` and `/ws` to this service.  See [the top-level README](../README.md) for the recommended Docker Compose layout.

> **Note**: this repository ships a pure JSON/WebSocket API.  The single-page web UI that used to live under `web/` has been moved to the standalone [`ui/`](../ui) repository so the two components can be versioned and deployed independently.

## Configuration

NexusDL reads configuration in this order (later overrides earlier):

1. Built-in defaults
2. Config file (YAML or JSON)
3. Environment variables (`NEXUS_*`)
4. CLI flags

### `config.yaml`

```yaml
node:
  id: nexus-node-1
  name: nexus-node
  log_level: info          # debug | info | warn | error
  data_dir: ./data

api:
  host: 0.0.0.0
  port: 8080
  auth_token: ""            # empty disables auth
  rate_limit: 100           # requests per second per IP, 0 disables
  enable_cors: true

cluster:
  grpc_port: 50051
  discovery_port: 50052
  node_timeout: 30s
  join_peers:               # seed nodes to bootstrap cluster
    - 192.168.1.10:50052

download:
  max_connections: 8
  min_chunk_size: 1048576           # 1 MB
  max_chunk_size: 52428800          # 50 MB
  default_chunk_size: 10485760      # 10 MB
  buffer_size: 32768                # 32 KB
  timeout: 30s
  retry_count: 3
  speed_limit: 0                    # bytes/sec, 0 = unlimited
  use_digest_auth: false
  preallocate_space: false
  sparse_file: false
  file_mode: 0o644
  min_speed: 0                      # abort if below for min_speed_timeout
  min_speed_timeout: 30s
  schedule_speed_limits:            # time-of-day based limits
    - start_time: "20:00"
      end_time:   "23:00"
      limit:      1048576           # 1 MB/s
      weekday:    1                 # 1=Monday ... 7=Sunday
  proxy:                            # optional
    type: http                      # http | https | socks5 | socks4 | socks4a
    host: proxy.example.com
    port: 8080
    username: ""
    password: ""
    exclude_list: ["localhost", "127.0.0.1"]
  bt:                               # BitTorrent (when enabled)
    enabled: true
    port: 6881
    max_peers: 100
    seed_ratio: 1.0
    dht_enabled: true
    upnp_enabled: true
  post_process:
    enabled: true
    extract_archives: true
    delete_archive_after: true
    run_commands: []                # e.g. ["echo done: {filepath}"]
```

### Environment variables

| Variable | Maps to |
| --- | --- |
| `NEXUS_NODE_ID` | `node.id` |
| `NEXUS_NODE_NAME` | `node.name` |
| `NEXUS_NODE_LOG_LEVEL` | `node.log_level` |
| `NEXUS_NODE_DATA_DIR` | `node.data_dir` |
| `NEXUS_API_HOST` | `api.host` |
| `NEXUS_API_PORT` | `api.port` |
| `NEXUS_API_AUTH_TOKEN` | `api.auth_token` |
| `NEXUS_API_RATE_LIMIT` | `api.rate_limit` |
| `NEXUS_CLUSTER_GRPC_PORT` | `cluster.grpc_port` |
| `NEXUS_CLUSTER_DISCOVERY_PORT` | `cluster.discovery_port` |
| `NEXUS_DOWNLOAD_MAX_CONNECTIONS` | `download.max_connections` |
| `NEXUS_DOWNLOAD_TIMEOUT` | `download.timeout` (seconds) |
| `NEXUS_DOWNLOAD_RETRY_COUNT` | `download.retry_count` |
| `NEXUS_DOWNLOAD_SPEED_LIMIT` | `download.speed_limit` |

## CLI

```
nexus-dl [global flags] <command> [command flags]
```

Global flags: `-c, --config <path>` (config file), `--api-host <url>` (default `http://localhost:8080`)

| Command | Description |
| --- | --- |
| `serve` | Start the node (API, cluster, download engine) |
| `add <url>` | Add a download (`-o output`, `-n node`, `-p priority`, `-i file`) |
| `metalink <file>` | Add a Metalink download |
| `pause <id>` | Pause a task |
| `resume <id>` | Resume a paused task |
| `pause-all` | Pause everything |
| `resume-all` | Resume everything |
| `remove <id>` | Remove a task (`-f` to skip confirm) |
| `show <id>` | Show task details |
| `list` | List all tasks (tabular) |
| `status [id]` | Cluster status or task status |
| `nodes` | List cluster nodes |
| `config <key> [value]` | Get or set config (printed, requires restart) |
| `log-level [level]` | Get or set log level at runtime |
| `version` | Print version, commit, build time |

### Examples

```bash
# Add a torrent
./nexus-dl add https://archive.org/download/file.torrent -p 5

# Batch add from a file (URLs one per line, # comments)
./nexus-dl add -i urls.txt

# Raise global speed limit at runtime
./nexus-dl log-level debug
```

## API

Base URL: `http://<host>:<port>`

### REST

| Method | Path | Description |
| --- | --- | --- |
| `GET` | `/api/tasks` | List tasks (returns `{tasks, total, active}`) |
| `POST` | `/api/tasks` | Add a task (`url`, `output_path`, `target_node`, `priority`) |
| `GET` | `/api/tasks/{id}` | Get task details |
| `DELETE` | `/api/tasks/{id}` | Remove a task |
| `POST` | `/api/tasks/{id}/pause` | Pause a task |
| `POST` | `/api/tasks/{id}/resume` | Resume a task |
| `POST` | `/api/tasks/pause-all` | Pause all |
| `POST` | `/api/tasks/resume-all` | Resume all |
| `GET` | `/api/nodes` | List cluster nodes |
| `GET` | `/api/status` | Cluster status + version |
| `GET` | `/api/log-level` | Get runtime log level |
| `POST` | `/api/log-level` | Set runtime log level (`{"level":"debug"}`) |
| `POST` | `/api/config/reload` | Reload config from disk |
| `GET` | `/health` | Liveness probe |
| `GET` | `/ready` | Readiness probe |
| `GET` | `/metrics` | Prometheus metrics |
| `WS` | `/ws` | WebSocket event stream (bypasses auth middleware) |

All `/api/*` routes (except `/health`, `/ready`, `/metrics`) are protected by auth and rate limiting.

### Authentication

If `api.auth_token` is set, every request must carry one of:

```bash
curl -H "Authorization: Bearer YOUR_TOKEN" http://localhost:8080/api/tasks
curl -H "X-API-Key: YOUR_TOKEN" http://localhost:8080/api/tasks
```

Tokens are compared with constant-time equality to prevent timing attacks.

### Aria2-compatible RPC

Aria2 methods exposed at `/jsonrpc` (JSON-RPC 2.0), `/xmlrpc`, and `/rpc`:

- `addUri`, `addTorrent`, `addMetalink`
- `removeDownloadResult`, `purgeDownloadResult`
- `pause`, `unpause`, `pauseAll`, `unpauseAll`
- `tellStatus`, `tellActive`, `tellWaiting`, `tellStopped`
- `getFiles`, `getPeers`, `getServers`
- `changeGlobalOption`, `changeOption`
- `getGlobalOption`, `getOption`
- `getVersion`, `getSessionInfo`, `shutdown`
- `multicall` (system + download methods)
- `changePosition`, `changeUri`, `saveSession`

`POST /jsonrpc` body example:

```json
{
  "jsonrpc": "2.0", "id": "1", "method": "addUri",
  "params": [["https://example.com/file.zip"], {"dir": "/downloads"}]
}
```

## WebSocket

Connect to `ws://<host>:<port>/ws` to receive:

```json
{"type":"task_update","task":{...}}
{"type":"node_update","nodes":[...]}
{"type":"cluster_event","event":{...}}
```

## Metrics

Prometheus format at `/metrics`:

- `nexus_tasks_total`, `nexus_tasks_active`
- `nexus_download_speed_bytes` (aggregate)
- `nexus_download_progress` (aggregate)
- `nexus_download_speed_bytes_by_task` (per task, capped)
- `nexus_cluster_nodes_online`
- `nexus_http_requests_total{path,method,status}`

## Cluster

NexusDL forms a peer-to-peer cluster with:

- **Discovery**: UDP multicast + unicast seed peers (`cluster.join_peers`)
- **Membership**: SWIM-style health checks with `node_timeout`
- **gRPC**: Inter-node task dispatch, status sync, file transfer
- **Scheduler**: Picks the lowest-load online node (`Load < 80`); skips local node
- **Failover**: When a node disappears, its tasks are re-queued to peers
- **Rebalance**: `Rebalance()` evens out per-node task counts

Bring up a 3-node cluster with `docker compose`:

```bash
NODE_ID=node-1 NEXUS_API_PORT=8080 NEXUS_CLUSTER_GRPC_PORT=50051 NEXUS_CLUSTER_DISCOVERY_PORT=50052 docker compose up -d
NODE_ID=node-2 NEXUS_API_PORT=8081 NEXUS_CLUSTER_GRPC_PORT=50061 NEXUS_CLUSTER_DISCOVERY_PORT=50062 JOIN_PEERS=node-1:50052 docker compose up -d
```

## Architecture

```
                        Browser / aria2 / curl / Python SDK
                                     │
                            HTTP · WS · JSON-RPC / XML-RPC
                                     │
                                     ▼
              ╔══════════════════════════════════════════╗
              ║           NexusDL Node (:8080)           ║
              ║──────────────────────────────────────────║
              ║  API layer  (auth, CORS, rate-limit)     ║
              ║      │                                   ║
              ║      ▼                                   ║
              ║  TaskQueue  ◄──callbacks──► WS Hub       ║
              ║      │                       │           ║
              ║      ▼                       ▼           ║
              ║  DownloadManager ──► EventEmitter        ║
              ║   │  │  │  │  │  │                       ║
              ║   ▼  ▼  ▼  ▼  ▼  ▼                       ║
              ║  HTTP FTP S3 WebDAV SFTP BT Metalink     ║
              ║                                          ║
              ║  disk: data/downloads + data/tasks/*.json║
              ╚════╤═══════════════════════╤═════════════╝
                   │                       │
              gRPC :50051              UDP :50052
              (task / status)         (discovery)
                   │                       │
                   ▼                       ▼
            ╔═════════════╗         ╔═════════════╗
            ║   Node 2    ║ ◄─────► ║   Node 3    ║
            ║   (peer)    ║   gRPC  ║   (peer)    ║
            ╚═════════════╝         ╚═════════════╝
```

**Data flow for a new task:**

1. Client `POST /api/tasks` → API handler validates URL scheme and path
2. `TaskStore.Save` writes the task to `data/tasks/<uuid>.json` (durable)
3. `TaskQueue.Add` enqueues with priority; dispatcher picks the lowest-load online peer
4. `DownloadManager.StartDownload` selects the protocol-specific downloader
5. Chunks stream to disk via `DownloadCache` (LRU-evicted)
6. `OnTaskProgress` broadcasts the updated task to the WebSocket Hub
7. `OnTaskComplete` / `OnTaskError` triggers post-processing (extract, hooks) and persists final state

**Failure modes:**

- Local download fails → exponential-backoff retry → `failed` after N tries
- Target node disappears → `failover` re-queues the task to a live peer
- Process restart → tasks in `pending` / `downloading` are reloaded from disk
- Network partition → SWIM membership marks node offline after `node_timeout`

## Security

- API authentication via `Authorization: Bearer` or `X-API-Key` (constant-time compare)
- Per-IP rate limiting (`api.rate_limit` requests/sec, configurable)
- Request body size limit (10 MB) on RPC endpoints
- URL scheme allow-list (HTTP/HTTPS/FTP/FTPS/S3/WebDAV/SFTP/Magnet)
- Output path validation (rejects traversal patterns)
- Cluster token verified with constant-time compare
- All API errors returned as JSON (XML-RPC endpoints use XML)
- TLS for S3, FTPS, HTTPS, SFTP supported natively

## Building

```bash
# Local
go build -o nexus-dl ./cmd/nexus-dl

# With version injection
go build -ldflags "-X main.Version=1.0.0 -X main.Commit=$(git rev-parse HEAD) -X main.BuildTime=$(date -u +%FT%TZ)" \
  -o nexus-dl ./cmd/nexus-dl

# Cross-compile
GOOS=linux   GOARCH=amd64 go build -o nexus-dl-linux   ./cmd/nexus-dl
GOOS=darwin  GOARCH=amd64 go build -o nexus-dl-darwin  ./cmd/nexus-dl
GOOS=windows GOARCH=amd64 go build -o nexus-dl.exe     ./cmd/nexus-dl
```

`build.ps1` is provided for Windows.

## Docker

```bash
docker build -t nexus-dl:latest .
docker run -d --name nexus-dl \
  -p 8080:8080 \
  -p 50051:50051 \
  -p 50052:50052/udp \
  -v $(pwd)/data:/data \
  -e NEXUS_API_AUTH_TOKEN=$(openssl rand -hex 32) \
  nexus-dl:latest
```

Or use `docker compose`:

```bash
docker compose up -d
```

## Testing

```bash
go test ./...                              # all packages
go test -race ./...                        # race detector
go test -cover ./...                       # coverage
go test ./internal/task/... -v             # one package, verbose
```

Coverage today: `internal/task`, `internal/downloader/ratelimit`, `internal/cluster/scheduler`, `internal/api`, `pkg/config`, `pkg/logger`.

## Project Layout

```
nexus-dl/
├── cmd/nexus-dl/main.go        # CLI entry
├── internal/
│   ├── api/                    # HTTP / JSON-RPC / XML-RPC / WS
│   ├── cluster/                # membership, scheduler, gRPC, NAT
│   ├── downloader/             # HTTP, FTP, S3, WebDAV, SFTP, BT, Metalink
│   ├── task/                   # queue, store, model, checksum
│   ├── plugin/                 # hooks
│   ├── events.go               # event emitter
│   └── postprocess.go          # archive extraction
├── pkg/
│   ├── config/                 # YAML/JSON + env
│   └── logger/                 # zap-based structured logger
├── web/                        # static SPA
├── proto/                      # gRPC proto
├── config.yaml                 # default config
├── Dockerfile
├── docker-compose.yml
└── README.md
```

## Roadmap

- DASH/HLS streaming
- Browser extension (capture downloads from Chrome/Firefox)
- WebDAV server (mount downloads as a filesystem)
- TUS resumable upload protocol
- Prometheus alerting rules
- Helm chart for Kubernetes

## License

MIT — see [LICENSE](LICENSE).

## Contributing

Issues and PRs welcome. Please run `go test ./...` and `go vet ./...` before submitting.
