# Changelog

All notable changes to NexusDL are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.3.0-beta](https://github.com/LL264a/afd/compare/v0.2.3-beta...v0.3.0-beta) (2026-06-29)


### Features

* add --insecure/-k flag to skip TLS certificate verification ([2f44ace](https://github.com/LL264a/afd/commit/2f44acec924afa968156b8a24a65440ced0b68d9))
* add --insecure/-k flag to skip TLS certificate verification ([8f083c4](https://github.com/LL264a/afd/commit/8f083c4c5f2256f53d9351f60e356874b98310b2))
* add insecure TLS support, disable HTTP/2, improve chunk download logging ([a169c35](https://github.com/LL264a/afd/commit/a169c3554ad96d3b16f16d47bc1ad21de968f2c7))
* aria2 alignment - SFTP/Cookie/RPC methods/XML-RPC/CLI commands ([7f7b681](https://github.com/LL264a/afd/commit/7f7b681a8e00c32fefa7e6c453f14022d7ab01a4))
* BT enhancements + conditional download + postprocess implementation ([6fd564d](https://github.com/LL264a/afd/commit/6fd564df471adc22b707ae91aa9c2a9e8bf028dc))
* HTTP headers/auth/gzip/dry-run/remote-time/quiet/check-integrity/file-allocation + RPC notifications + auto-save ([0e77f36](https://github.com/LL264a/afd/commit/0e77f364daa30b116686aa9823b5f7479866d85c))
* netrc support + daemon mode ([dc65af1](https://github.com/LL264a/afd/commit/dc65af19a143daff88e17c5008ed5145dafe0ecf))
* piece selector (inorder/geom/random) + URI selector (inorder/feedback/adaptive) + BT MSE encryption + WebSeed support ([39901a6](https://github.com/LL264a/afd/commit/39901a6c20d52fe90cb08cb9096bf1b99aa2bd09))
* Piece+Block two-level chunking, segment stealing, block-level progress persistence ([7f64be9](https://github.com/LL264a/afd/commit/7f64be9907474256a723fef43ddf624cf1c2c376))
* TTY progress bar for CLI download command ([95147c9](https://github.com/LL264a/afd/commit/95147c9a762389386c20cda36a5dd8390c3df01a))
* unify naming to afd + fix Ubuntu/Linux deploy (Dockerfile/Makefile/systemd/SIGHUP/setsid/flock/perms) ([38e75f2](https://github.com/LL264a/afd/commit/38e75f2580ce6b3e7a10ae62cf9fad848d738b31))
* wget/curl-compatible CLI flags for drop-in replacement ([5cc05aa](https://github.com/LL264a/afd/commit/5cc05aa2d9ef69b66f179fb47c734104d7577d08))
* wire all subsystems in runServe (downloadMgr/events/postProcess/cluster/nat/plugin) + fix NAT holepunch/STUN/signaling + implement changePosition/changeUri + add pprof/log rotation/TLS ([9856ed3](https://github.com/LL264a/afd/commit/9856ed326e352f84cebcdb22963b47454ff7c315))
* wire event handlers + schedule speed limits + encrypted zip password + delete temp files + BT filesize + scheduler gRPC dispatch ([4e11a7b](https://github.com/LL264a/afd/commit/4e11a7b1ae6fa448ded2b596aa72079cdf94abc4))


### Bug Fixes

* 18 issues - compile errors, races, perf optimization, NAT stubs, dead code ([8b99ee2](https://github.com/LL264a/afd/commit/8b99ee2a7c38b95faf9904a7a20277d963c4f6a3))
* gofmt + Dockerfile go1.24 + smoke test error logging ([a51acf3](https://github.com/LL264a/afd/commit/a51acf326a5e264a69549fc8a74c6ea563250e81))
* Linux CI failures - inverted argv test logic + lazy log file + Docker health retry ([e132356](https://github.com/LL264a/afd/commit/e132356c3f40e132fb13935499597a66214dea43))
* register -c shorthand for --config flag ([bb09994](https://github.com/LL264a/afd/commit/bb099947c6695163b5701cd5ec2c23fc39bbe48b))
* remove Node mutex to resolve go vet lock-copy warnings ([d3a3ced](https://github.com/LL264a/afd/commit/d3a3ced49483a2ce6654a432a8fe7a906af04a94))
* resolve 30+ commercial-blocking issues across all modules ([ce2702a](https://github.com/LL264a/afd/commit/ce2702a31fd28f159c7c6f26a0fcdf1dbf997138))
* resolve 50+ P0/P1 issues - data corruption, security, races, panics ([7404e85](https://github.com/LL264a/afd/commit/7404e85339503d116d4d45c3e7ac657a0fb60cbd))
* resolve remaining 16 commercial-blocking issues ([d609c07](https://github.com/LL264a/afd/commit/d609c071abad22e248f410bac4c59cabdddd8780))
* severe bugs for wget/curl replacement ([9726b6b](https://github.com/LL264a/afd/commit/9726b6b8f37f9b256eb164238807eea0a6111971))
* store URL in control file to prevent cross-URL resume corruption ([38e2881](https://github.com/LL264a/afd/commit/38e2881dee39aedce81dcbc39be844fa54b915ef))
* TestExtractZipPathTraversal uses sentinel name not /etc/passwd ([cec8d36](https://github.com/LL264a/afd/commit/cec8d36fba7ae0d5a44851b9a8f431a798af20e6))
* TestIsSafePath Linux absolute path + CI failure reporting + smoke test PID fix ([702bcad](https://github.com/LL264a/afd/commit/702bcad25b1b820afe53b2d625d487ae85587b83))
* Ubuntu/Linux deploy readiness (Dockerfile go1.24 + BT persist + install.sh systemd + SIGHUP loglevel + journald colors + daemon logs + release BuildTime) ([a0a552c](https://github.com/LL264a/afd/commit/a0a552c9683c55d986d8a49cc90dd3634397c198))
* Windows filename inference + unknown-size download stats ([4715ab4](https://github.com/LL264a/afd/commit/4715ab4efadb12169fa7c74ba5797e611fc028e1))

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
