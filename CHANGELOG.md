# Changelog

All notable changes to NexusDL are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed (BREAKING)

- **Web UI extracted to standalone repository** (`ui/`, Vite + React + TypeScript).  The `web/` static assets, `findWebPath()` runtime search, and FileServer handler have been removed from this repository.  The Core service is now a pure JSON / WebSocket API: requests to `/` receive a JSON 404 with `X-NexusDL-Service: core` instead of a web shell.  Deploy the UI from the `ui/` repository and proxy `/api`, `/ws`, `/rpc`, `/xmlrpc`, and `/metrics` to Core via nginx.  `core/Dockerfile` no longer `COPY web/`.  `CORSAllowedOrigins` is now configurable (default: `http://localhost:5173`, `http://127.0.0.1:5173`); when the list is non-empty, the wildcard origin is dropped and unknown origins are rejected with 403.

### Added

- **Go fuzz tests** for `JSONRPCServer.ServeHTTP`, `JSONRPCServer.multicall` params, and `XMLRPCServer.ServeHTTP` (19 seed inputs each, run in CI for 30s)
- **Go benchmarks** for `task.TaskQueue` (Add / Get / List / PriorityAdd / ConcurrentAddGet), `task.TaskStore` (Save / Load / WriteAndRead), and `downloader.RateLimiter` (Allow / SetRate / Wait / Concurrent / Refill)
- **API v1 prefix** with soft deprecation: `/api/v1/...` is canonical, `/api/...` still works and emits `Deprecation: true` + `Link: </api/v1/>; rel="successor-version"`
- **Helm chart** at `deploy/helm/` (Deployment, Service, PVC, ConfigMap, Secret, ServiceAccount, ServiceMonitor) with documented `values.yaml`
- **Release Please** workflow (`.github/workflows/release-please.yml`) for automatic version bumps and CHANGELOG generation from Conventional Commits
- **Coverage report** job in CI uploading to Codecov; merged unit + integration coverage
- **`golangci-lint` configuration** (`.golangci.yml`) with 18 linters enabled
- **`deploy/helm/README.md`** with quick start, cluster mode, configuration, security notes, linting
- **`deploy/README.md`** decision tree and production checklist
- **Web UI asset tests** in `test/webui/` (5 tests: layout, HTML references, JS parse, CSS parse, HTTP server simulation)

### Changed

- CI workflow expanded from 3 jobs to **8 jobs**: lint, unit-test (3 OS), integration-test, webui-test, fuzz-test (3 targets), benchmark, coverage, build, docker
- `internal/api/middleware.go` exposes a new `Deprecation()` middleware

## [0.1.0] - 2026-06-02

First public beta. Suitable for internal dogfood and evaluation; not yet recommended for production.

### Added

- **Multi-protocol downloaders**: HTTP/HTTPS, FTP/FTPS, BitTorrent, S3, WebDAV, SFTP, Metalink
- **Resumable downloads**: HTTP Range, FTP REST, S3 multipart resume on interruption
- **Cluster mode**: gRPC inter-node communication, UDP-based discovery, SWIM membership
- **Task scheduler**: load-aware dispatch (skips local node, prefers `Load < 80` lowest)
- **Failover & rebalance**: automatic task reassignment on node failure, periodic rebalance
- **Rate limiting**: global, per-task, and time-of-day scheduled limits (token bucket)
- **Proxy support**: HTTP/HTTPS/SOCKS5/SOCKS4(a) with auth and host exclude list
- **Retry**: exponential backoff for transient network errors
- **WebSocket event stream** for real-time task and cluster updates
- **Post-processing**: automatic extraction of zip / tar / tar.gz / tar.bz2
- **Plugin hooks**: pre-download and post-completion script invocation
- **NAT traversal**: STUN, UDP/TCP hole punching, relay fallback
- **Aria2-compatible RPC**: `addUri` / `addTorrent` / `addMetalink` / `tellStatus` / `tellActive` / `tellWaiting` / `tellStopped` / `getFiles` / `getPeers` / `getServers` / `changeGlobalOption` / `changeOption` / `getGlobalOption` / `getOption` / `getVersion` / `getSessionInfo` / `shutdown` / `multicall` / `changePosition` / `changeUri` / `saveSession` over JSON-RPC and XML-RPC
- **REST API**: `/api/tasks`, `/api/nodes`, `/api/status`, `/api/log-level`, `/api/config/reload`, `/health`, `/ready`, `/metrics`
- **Web UI**: SPA at `/` with task list, live progress, charts, dark mode
- **CLI**: `serve`, `add`, `metalink`, `pause`, `resume`, `pause-all`, `resume-all`, `remove`, `show`, `list`, `status`, `nodes`, `config`, `log-level`, `version`
- **Configuration**: YAML / JSON files with include support, env-var overrides (`NEXUS_*`), CLI flags
- **Structured JSON logging** (zap) with runtime level switch
- **Prometheus metrics** at `/metrics` (aggregate + per-task gauges)
- **API authentication** via `Authorization: Bearer` and `X-API-Key` headers, constant-time token comparison
- **Per-IP request rate limiting** (configurable, 0 disables)
- **Graceful shutdown**: SIGINT/SIGTERM with in-flight request drain, context-based cancellation
- **Version injection** at build time via `ldflags` (`-X main.Version / Commit / BuildTime`)
- **Docker image** and `docker-compose.yml` for single-node and 3-node cluster bring-up
- **Unit tests** for: `internal/task` (queue + store), `internal/downloader/ratelimit`, `internal/cluster/scheduler`, `internal/api`, `pkg/config`

### Security

- All API errors returned as JSON via `sendError()` (XML-RPC endpoints use XML)
- Constant-time token comparison (`subtle.ConstantTimeCompare`) for API and cluster auth
- Request body size limited to 10 MB on RPC endpoints
- URL scheme allow-list on input
- Output path validation rejects traversal patterns (`..`, absolute paths outside data dir)
- Cluster token comparison hardened

### Fixed

- **Chunk status race condition** in `downloader.go` (concurrent map access)
- **singleThreadDownload** progress tracking now updates correctly
- **periodicSaveProgress** goroutine leak on downloader stop
- **TaskQueue.Remove** no longer leaks `activeCount`
- **Scheduler.Rebalance** deadlock under heavy concurrent task movement
- **pauseTask / resumeTask** no longer panic on missing task (nil check)
- **server.Stop** uses `context.Background()` for `http.Server.Shutdown`
- **JSON-RPC multicall** validates `params[0]` to prevent out-of-bounds panic
- **FTP** write-error path returns the original error, not a stale one
- **WebDAV** URL parsing uses correct `TrimPrefix` order
- **WebSocket Hub** now has `Close()` and `done` channel; no goroutine leak
- **Default global hub** removed in favor of explicit `NewWebSocketHub()`
- **TaskStore.Load** preserves existing `Metadata` when reloading
- **RateLimiter.cleanup** goroutine leak fixed via `done` + `stop()`
- **SFTP** speed calculation uses a sliding window, not a per-tick snapshot
- **S3** seek on partial objects now uses correct range header
- **Downloader manager** no longer deadlocks on stop when downloads are in flight
- **Torrent** null-pointer on missing metadata
- **Adaptive concurrency** no longer holds lock while doing IO
- **Metalink** concurrency-safe map access
- **SFTP HostKeyCallback** wired up for strict host verification
- **S3 sliding-window speed** matches HTTP behaviour
- **LRU eviction** in disk cache uses real `lastUsed` sort
- **Metadata deep copy** in `Task.GetSafe()` to prevent external mutation
- **Logger file handle** tracked and closed via `logger.Close()`

### Changed

- Prometheus metrics split: aggregate `nexus_download_speed_bytes` (Gauge) plus capped `nexus_download_speed_bytes_by_task` (GaugeVec) to prevent label cardinality blow-up
- CLI command `add` accepts `-i <file>` for batch URL input

### Known Limitations

- Web UI does not yet support editing tasks after creation
- `multicall` does not yet support nested responses for `system.*` methods
- IPv6 multicast discovery not yet enabled by default
- No DASH/HLS or browser extension yet
- No Helm chart yet
- Some advanced `aria2` options (`checksum`, `index-out`, `select-file`, `split`) not yet implemented

## [0.0.1] - 2025-12-01

Initial private prototype. Not released publicly.

[Unreleased]: https://github.com/nexus-dl/nexus-dl/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/nexus-dl/nexus-dl/releases/tag/v0.1.0
[0.0.1]: https://github.com/nexus-dl/nexus-dl/releases/tag/v0.0.1
