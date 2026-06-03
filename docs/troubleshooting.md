# Troubleshooting

Common errors and how to fix them.

## "address already in use" on `serve`

Another process is bound to `api.port` (default `8080`) or `cluster.grpc_port`
(default `50051`).

```bash
# Linux/macOS — find the offending PID
sudo lsof -i :8080
sudo lsof -i :50051

# Windows
netstat -ano | findstr :8080
taskkill /PID <pid> /F
```

Or change the port in `config.yaml`.

## "no suitable node found for task, requeuing" forever

The scheduler cannot find an online peer. Most common causes:

- `cluster.node_timeout` is too low for your network latency
- UDP discovery port `50052` is blocked by a firewall
- The peer never started (check `journalctl -u nexus-dl` or the container log)

Run `nexus-dl nodes` to inspect the cluster.

## Tasks are stuck at `pending`

Two possibilities:

1. The scheduler is full and `download.max_connections` is too low. Raise it.
2. The target node is offline. Run `nexus-dl status` and `nexus-dl nodes`.

## "disk space insufficient" mid-download

`download.preallocate_space: true` reserves the full file at the start. On
filesystems without sparse-file support this fails before any bytes are
written. Either:

- Set `download.preallocate_space: false` (default)
- Or set `download.sparse_file: true` on a filesystem that supports it
  (ext4, xfs, btrfs, NTFS — not FAT32/exFAT)

## WebSocket disconnects every minute

`nginx` example config sets `proxy_read_timeout 1h`, but a tighter timeout
in your environment is closing idle sockets. Increase the timeout on the
proxy upstream and ensure `Connection: upgrade` is set.

## BitTorrent says "no usable sources"

- DHT is off and no peers are on the tracker. Try `bt.dht_enabled: true`.
- The torrent has no seeds (private tracker with all peers gone).
- Your firewall blocks outbound UDP (DHT) or TCP (peers).

## Speed limit is ignored

`download.speed_limit` is the global cap. Per-task limits are set at
runtime via `api/tasks/{id}` options (JSON-RPC `changeOption`) or
`download.schedule_speed_limits` for time-of-day. A value of `0` means
unlimited — make sure you didn't set it to `0` by accident.

## Authentication: 401 even with the right token

- Check `api.auth_token` is set in `config.yaml` (or `NEXUS_API_AUTH_TOKEN` env)
- Make sure the server has been restarted after changing the token
- Watch for trailing whitespace in the token; YAML keeps it
- `nexus-dl config api.auth_token` should print the value (omitted in the
  table for clarity, use `--api-host http://localhost:8080` if not local)

## S3: "AccessDenied" on download

The credentials are valid but the IAM policy does not allow
`s3:GetObject` on the bucket/prefix. Check the policy in the AWS console.

## "context deadline exceeded" on first run

`config.yaml` has `timeout: 30s` and your first connection took longer.
Either raise `download.timeout` or warm up the connection.

## High memory usage with many tasks

The default in-memory WebSocket hub keeps the last event per client. If
you have thousands of WebSocket clients, switch the broadcast to lazy
fan-out. There is no per-client queue for missed events in 0.1.x — that
is a 0.2 feature.

## Service won't start under systemd: "permission denied"

`/var/lib/nexus-dl` must be owned by the `nexus` user. Create it and
adjust ownership:

```bash
sudo useradd -r -s /usr/sbin/nologin nexus
sudo mkdir -p /var/lib/nexus-dl /etc/nexus-dl
sudo chown -R nexus:nexus /var/lib/nexus-dl
```

## How to enable debug logging

```bash
./nexus-dl log-level debug
```

Or in `config.yaml`:

```yaml
node:
  log_level: debug
```

And restart.

## How to wipe state and start over

```bash
sudo systemctl stop nexus-dl
sudo rm -rf /var/lib/nexus-dl/tasks/*
sudo systemctl start nexus-dl
```

This removes **all** persisted task state. Downloaded files in
`/var/lib/nexus-dl/downloads` are untouched.
