# Upgrading

This document covers how to upgrade between NexusDL versions and the
breaking changes to be aware of.

## Upgrade Procedure (General)

1. Read the **CHANGELOG.md** entry for the version you are upgrading to.
   Pay attention to the **BREAKING CHANGE** and **Known Limitations**
   sections.
2. Back up your data directory (`node.data_dir`), especially the `tasks/`
   subdirectory, before upgrading.
3. Stop the running node:
   ```bash
   sudo systemctl stop nexus-dl
   # or
   docker compose down
   ```
4. Replace the binary (or pull the new image).
5. Start the node:
   ```bash
   sudo systemctl start nexus-dl
   # or
   docker compose up -d
   ```
6. Tail the logs for a minute to confirm no migration errors:
   ```bash
   journalctl -u nexus-dl -f
   ```
7. Run `nexus-dl version` to confirm the new build is in place.

## Compatibility Matrix

| From | To | Path | Notes |
| --- | --- | --- | --- |
| 0.0.x | 0.1.0 | Direct | No format change; wipe `data/tasks/` if schema is unclear |
| 0.1.x | 0.2.x | Direct | Will add `/api/v1/` prefix; old paths will keep working with a `Deprecation` header for one minor version |
| 0.1.x | 1.0.0 | Migration guide | Schema freeze; v1.0 is the first GA release |

## API Stability

| Surface | Stability | Deprecation policy |
| --- | --- | --- |
| `Authorization: Bearer` / `X-API-Key` | Stable | No plan to remove |
| Unversioned `/api/tasks` etc. | Deprecated in 0.2.0 | Will keep working for 1 minor version with `Deprecation: true` and `Sunset:` headers |
| `/api/v1/tasks` etc. | Stable from 0.2.0 | First-class; backed by the OpenAPI spec |
| JSON-RPC `/jsonrpc` (aria2) | Stable | Backwards-compat with aria2 1.36+ |
| XML-RPC `/xmlrpc` (aria2) | Stable | Backwards-compat with aria2 1.36+ |
| WebSocket `/ws` | Stable | Wire format may add optional fields |
| Prometheus `/metrics` | Stable | Labels may be added; existing labels won't change |
| `Authorization: Basic` | Removed in 0.1.0 | Use `Bearer` or `X-API-Key` |

## v1 API Migration (planned for 0.2.0)

When v1 lands, the following changes apply:

### Path changes

| Old (still works) | New (preferred) |
| --- | --- |
| `POST /api/tasks` | `POST /api/v1/tasks` |
| `GET /api/tasks/{id}` | `GET /api/v1/tasks/{id}` |
| `POST /api/tasks/{id}/pause` | `POST /api/v1/tasks/{id}/pause` |
| `GET /api/nodes` | `GET /api/v1/nodes` |
| `GET /api/status` | `GET /api/v1/status` |
| `GET /api/log-level` | `GET /api/v1/log-level` |
| `POST /api/log-level` | `POST /api/v1/log-level` |
| `POST /api/config/reload` | `POST /api/v1/config/reload` |

### Response changes

- All v1 responses use the `{ "data": ... }` envelope for success and
  `{ "error": { "code": "...", "message": "..." } }` for failure.
  (The current unversioned endpoints use a mix of bare and wrapped
  shapes, which is part of why we're versioning.)
- All v1 timestamps are RFC 3339 with timezone (e.g.
  `2026-06-02T14:30:00Z`).
- All v1 IDs are UUID v4.

### Client migration

```python
# 0.1.x
import requests
r = requests.post("http://host:8080/api/tasks", json={"url": "..."}, headers=h)
task = r.json()["task"]

# 0.2.x (preferred)
r = requests.post("http://host:8080/api/v1/tasks", json={"url": "..."}, headers=h)
task = r.json()["data"]
```

The old paths will continue to work for the 0.2.x lifetime and return
a `Deprecation: true` and `Sunset: 2026-12-31T00:00:00Z` header. Use
the new paths going forward.

## Config Compatibility

Config files are forward-compatible at the file level: new fields are
optional with safe defaults. Removed fields log a warning at startup.

To migrate a 0.0.x `config.yaml` to 0.1.x:

```diff
- nodes:                  # removed; use cluster.join_peers
-   - host: peer:50052
+ cluster:
+   join_peers:
+     - "peer:50052"
```

## Cluster Compatibility

- A 0.1.0 node can talk to a 0.1.0 node.
- 0.1.0 nodes with mixed 0.0.x peers are **not** supported; upgrade all
  nodes in lockstep.
- The cluster gRPC protocol is versioned; the handshake includes a
  `protocol_version` field and mismatches are rejected.

## Data Directory

The on-disk format of `data/tasks/<id>.json` is documented in
`docs/on-disk-format.md` (planned for 0.2.0). Once a 1.0.0 release
freezes the format, older directories are readable by newer binaries
forever, but newer features require newer directories.

To migrate, stop the node, copy the old `tasks/` to the new
`data/tasks/`, and start the new binary. No conversion is required.

## Docker Tagging

`ghcr.io/nexus-dl/nexus-dl` is tagged:

| Tag | Meaning |
| --- | --- |
| `latest` | Most recent stable release |
| `0.1.0`, `0.1`, `0` | Specific major / minor / patch |
| `<sha>` | Specific commit |
| `nightly` | Daily build of `main` (best effort) |

Pin to a specific version in production.
